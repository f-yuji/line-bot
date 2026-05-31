"""H5 Live Limited Sort Variant Analysis.

Research-only script. Does not modify DB, case definitions, or any live code.

Tests multiple entry_sort variants for the Live Limited selection and compares
performance to identify which sort produces the best outcomes vs. Research baseline.

Variants tested:
  current_bug_ev_desc  : expected_value_desc (the fallback that was actually applied)
  intended_original    : [signal_probability_desc, overheat_score_asc, volume_ratio_desc]
  no_volume            : [signal_probability_desc, overheat_score_asc]
  low_volume           : [volume_ratio_asc, signal_probability_desc, overheat_score_asc]
  moderate_volume      : [volume_ratio_moderate, signal_probability_desc, overheat_score_asc]
  regime_modvol        : [market_regime_priority, volume_ratio_moderate, signal_probability_desc, overheat_score_asc]
  random_seed0/42/99   : random baselines

Usage:
    python scripts/analyze_h5_live_sort_variants.py
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from services.h5_primary import H5_LIVE_LIMITED_RULES, h5_overheat_score
from services.trade_case_tester import (
    _build_supabase,
    _load_candidates_v2,
    _sort_candidates,
    _to_float,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MAX_HOLD = 10
EST12_STOP = -0.12


# ──────────────────────────────────────────────
# Sort variants definition
# ──────────────────────────────────────────────

SORT_VARIANTS: list[dict] = [
    {
        "name": "current_bug_ev_desc",
        "keys": ["expected_value_desc"],
        "description": "Fallback EV sort — what was actually applied due to list→str conversion bug",
    },
    {
        "name": "intended_original",
        "keys": ["signal_probability_desc", "overheat_score_asc", "volume_ratio_desc"],
        "description": "Original intended sort (now fixed: list handled correctly)",
    },
    {
        "name": "no_volume",
        "keys": ["signal_probability_desc", "overheat_score_asc"],
        "description": "signal_prob desc + overheat asc only (volume removed)",
    },
    {
        "name": "low_volume",
        "keys": ["volume_ratio_asc", "signal_probability_desc", "overheat_score_asc"],
        "description": "Low volume first, then prob desc (volume_ratio ascending)",
    },
    {
        "name": "moderate_volume",
        "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
        "description": "Volume closest to 1.3 first, then prob desc",
    },
    {
        "name": "regime_modvol",
        "keys": ["market_regime_priority", "volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
        "description": "panic_rebound > risk_on > weak > normal, then moderate volume, then prob",
    },
    {
        "name": "random_seed0",
        "keys": ["random_seed0"],
        "description": "Random baseline (seed=0)",
    },
    {
        "name": "random_seed42",
        "keys": ["random_seed42"],
        "description": "Random baseline (seed=42)",
    },
    {
        "name": "random_seed99",
        "keys": ["random_seed99"],
        "description": "Random baseline (seed=99)",
    },
]


# ──────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────

def _d(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _round(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        f = float(value)
        return round(f, digits) if math.isfinite(f) else None
    except Exception:
        return value


def _avg(vals: list[float]) -> float | None:
    return sum(vals) / len(vals) if vals else None


def _pf(vals: list[float]) -> float | None:
    w = sum(v for v in vals if v > 0)
    l = abs(sum(v for v in vals if v <= 0))
    if l <= 0:
        return None if w <= 0 else 999.0
    return w / l


def _wr(vals: list[float]) -> float | None:
    return sum(1 for v in vals if v > 0) / len(vals) * 100 if vals else None


def _max_dd(vals: list[float]) -> float:
    eq = pk = dd = 0.0
    for v in vals:
        eq += v
        pk = max(pk, eq)
        dd = min(dd, eq - pk)
    return dd


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for k in row:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for row in rows:
            w.writerow({k: _round(v) for k, v in row.items()})


def _passes_h5_entry(row: dict) -> bool:
    prob = _to_float(row.get("signal_probability"), None)
    stage = str(row.get("signal_stage") or "")
    drop20 = _to_float(row.get("drop_from_20d_high_pct"), None)
    margin = _to_float(row.get("margin_ratio"), None)
    regime = str(row.get("market_regime") or "")
    if prob is None or prob < 0.65:
        return False
    if stage not in {"confirmed", "strong_confirmed"}:
        return False
    if drop20 is None or drop20 > -8.0:
        return False
    if regime == "panic_selloff":
        return False
    if h5_overheat_score(row) > 1:
        return False
    if margin is not None and (margin < 3 or margin > 30):
        return False
    return True


def _raw_ret(row: dict, entry: float, hold: int) -> float | None:
    c = _to_float(row.get(f"future_close_{hold}d"), None)
    return (c / entry - 1.0) * 100.0 if c is not None and entry > 0 else None


def _est12(row: dict, entry: float, hold: int) -> dict:
    stop = entry * (1.0 + EST12_STOP)
    last_c = None
    last_d = 0
    for d in range(1, hold + 1):
        low = _to_float(row.get(f"future_low_{d}d"), None)
        close = _to_float(row.get(f"future_close_{d}d"), None)
        if close is not None:
            last_c = close
            last_d = d
        if low is not None and low <= stop:
            return {"ret": EST12_STOP * 100.0, "exit_day": d, "reason": "emergency_stop"}
    if last_c is None:
        return {"ret": None, "exit_day": None, "reason": "no_data"}
    return {"ret": (last_c / entry - 1.0) * 100.0, "exit_day": last_d, "reason": "time_stop"}


def _vol_bucket(v: float | None) -> str:
    if v is None:
        return "null"
    if v < 0.7:
        return "lt0.7"
    if v < 1.0:
        return "0.7_1.0"
    if v < 1.5:
        return "1.0_1.5"
    if v < 2.0:
        return "1.5_2.0"
    if v < 3.0:
        return "2.0_3.0"
    return "gte3.0"


# ──────────────────────────────────────────────
# Dataset build
# ──────────────────────────────────────────────

def _build_dataset(candidates: list[dict]) -> list[dict]:
    dataset = []
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry or entry <= 0:
            continue

        vol = _to_float(row.get("volume_ratio_20d"), None)
        rec: dict = {
            "entry_date": str(row.get("trade_date") or ""),
            "code": str(row.get("code") or ""),
            "name": row.get("name"),
            "sector": str(row.get("sector") or ""),
            "market_regime": str(row.get("market_regime") or ""),
            "entry_price": entry,
            "signal_probability": _to_float(row.get("signal_probability"), None),
            "signal_stage": row.get("signal_stage"),
            "overheat_score": h5_overheat_score(row),
            "volume_ratio": vol,
            "volume_ratio_bucket": _vol_bucket(vol),
            "drop_from_20d_high_pct": _to_float(row.get("drop_from_20d_high_pct"), None),
            "margin_ratio": _to_float(row.get("margin_ratio"), None),
            "rule_score": _to_float(row.get("rule_score"), None),
            "bad_news_score": _to_float(row.get("bad_news_score"), None),
        }
        # Future returns
        for hold in [1, 2, 3, 5, 7, 10]:
            rec[f"hd{hold}_ret_raw"] = _round(_raw_ret(row, entry, hold))
        est3 = _est12(row, entry, 3)
        rec["hd3_ret_est12"] = _round(est3.get("ret"))
        rec["hd3_exit_reason"] = est3.get("reason")
        rec["emergency_stop"] = est3.get("reason") == "emergency_stop"

        # Keep original row fields for _sort_candidates
        for k in ["volume_ratio_20d", "signal_probability", "rule_score", "bad_news_score",
                  "ma5_gap_pct", "entry_gap_pct", "drop_from_20d_high_pct", "trade_date"]:
            if k not in rec:
                rec[k] = row.get(k)

        dataset.append(rec)
    return dataset


# ──────────────────────────────────────────────
# Live Limited simulation
# ──────────────────────────────────────────────

def _simulate(
    rows: list[dict],
    sort_keys: list[str],
    max_daily: int,
    max_open: int,
    max_sector: int,
    rank_limit: int,
) -> list[dict]:
    """Simulate Live Limited selection using the fixed _sort_candidates with given sort_keys."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("entry_date") or "")].append(row)

    # Assign in-day rank
    for dt, day_rows in by_date.items():
        sorted_day = _sort_candidates(day_rows, sort_keys, H5_LIVE_LIMITED_RULES)
        for i, r in enumerate(sorted_day):
            r["_rank"] = i + 1
            r["_in_limit"] = (i + 1) <= rank_limit

    open_positions: list[dict] = []
    result: list[dict] = []

    for dt in sorted(by_date.keys()):
        today = _d(dt)
        open_positions = [p for p in open_positions if _d(p.get("_expiry")) >= today]

        day_rows = sorted(by_date[dt], key=lambda r: r.get("_rank", 9999))
        top = [r for r in day_rows if r.get("_in_limit")]
        below = [r for r in day_rows if not r.get("_in_limit")]

        daily = 0
        for row in top:
            skip = None
            if daily >= max_daily:
                skip = "daily_limit"
            elif len(open_positions) >= max_open:
                skip = "open_position_limit"
            else:
                sec = str(row.get("sector") or "unknown")
                if sum(1 for p in open_positions if (p.get("sector") or "unknown") == sec) >= max_sector:
                    skip = "sector_limit"

            row["_selected"] = skip is None
            row["_skip_reason"] = skip or ""
            if skip is None:
                daily += 1
                row["_expiry"] = (today + timedelta(days=5)).isoformat()
                open_positions.append(row)
            result.append(row)

        for row in below:
            row["_selected"] = False
            row["_skip_reason"] = "rank_limit"
            result.append(row)

    return result


# ──────────────────────────────────────────────
# Performance helper
# ──────────────────────────────────────────────

def _perf(rows: list[dict], label: str, period: str) -> dict:
    n = len(rows)
    out: dict = {"variant": label, "period": period, "n": n}
    if n == 0:
        return out
    for hold in [1, 2, 3, 5, 7, 10]:
        col = f"hd{hold}_ret_raw"
        vals = [_to_float(r.get(col), None) for r in rows]
        vals = [v for v in vals if v is not None]
        out[f"hd{hold}_avg"] = _round(_avg(vals))
        out[f"hd{hold}_wr"] = _round(_wr(vals))
        out[f"hd{hold}_pf"] = _round(_pf(vals))
    hd3e = [_to_float(r.get("hd3_ret_est12"), None) for r in rows]
    hd3e = [v for v in hd3e if v is not None]
    out["hd3_est12_avg"] = _round(_avg(hd3e))
    out["emergency_stop_rate"] = _round(sum(1 for r in rows if r.get("emergency_stop")) / n * 100)
    out["avg_signal_prob"] = _round(_avg([_to_float(r.get("signal_probability"), None) for r in rows if r.get("signal_probability") is not None]))
    out["avg_overheat"] = _round(_avg([float(r.get("overheat_score", 0)) for r in rows]))
    out["avg_volume_ratio"] = _round(_avg([_to_float(r.get("volume_ratio"), None) for r in rows if r.get("volume_ratio") is not None]))
    out["avg_drop20"] = _round(_avg([_to_float(r.get("drop_from_20d_high_pct"), None) for r in rows if r.get("drop_from_20d_high_pct") is not None]))
    return out


def _split(rows: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    train = [r for r in rows if _d(r["entry_date"]) <= train_end]
    test = [r for r in rows if _d(r["entry_date"]) > train_end]
    return train, test


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    train_start = _d(args.train_start)
    train_end = _d(args.train_end)
    test_end = _d(args.test_end)
    start = _d(args.train_start)
    end = test_end
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    max_daily = args.max_daily_entries
    max_open = args.max_open_positions
    max_sector = args.max_sector_positions
    rank_limit = args.entry_rank_limit

    sb = _build_supabase()
    logger.info("[sort_variants] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[sort_variants] raw candidates=%d", len(candidates))

    dataset = _build_dataset(candidates)
    logger.info("[sort_variants] research dataset rows=%d", len(dataset))

    research_train, research_test = _split(dataset, train_end)

    # Research baseline performance
    research_baseline = {
        "train": _perf(research_train, "research_all", "train"),
        "test": _perf(research_test, "research_all", "test"),
        "all": _perf(dataset, "research_all", "all"),
    }

    # ── Run all sort variants ──
    comparison_train: list[dict] = [research_baseline["train"]]
    comparison_test: list[dict] = [research_baseline["test"]]
    comparison_all: list[dict] = [research_baseline["all"]]

    regime_breakdown: list[dict] = []
    volume_breakdown: list[dict] = []
    selected_trades: list[dict] = []
    rank_bucket_out: list[dict] = []
    skip_reason_out: list[dict] = []

    random_hd3_all: list[float] = []

    for variant in SORT_VARIANTS:
        vname = variant["name"]
        vkeys = variant["keys"]
        logger.info("[sort_variants] running variant: %s  keys=%s", vname, vkeys)

        sim = _simulate(
            [dict(r) for r in dataset],
            vkeys, max_daily, max_open, max_sector, rank_limit,
        )
        selected = [r for r in sim if r.get("_selected")]

        sel_train, sel_test = _split(selected, train_end)

        p_train = _perf(sel_train, vname, "train")
        p_test = _perf(sel_test, vname, "test")
        p_all = _perf(selected, vname, "all")

        # Compute diff vs research
        for p, base in [(p_train, research_baseline["train"]), (p_test, research_baseline["test"]), (p_all, research_baseline["all"])]:
            for hold in [3, 5, 7]:
                col = f"hd{hold}_avg"
                if p.get(col) is not None and base.get(col) is not None:
                    p[f"hd{hold}_vs_research"] = _round(p[col] - base[col])

        comparison_train.append(p_train)
        comparison_test.append(p_test)
        comparison_all.append(p_all)

        if vname.startswith("random_"):
            if p_all.get("hd3_avg") is not None:
                random_hd3_all.append(p_all["hd3_avg"])

        # ── Regime breakdown ──
        by_regime: dict[str, list] = defaultdict(list)
        for r in selected:
            by_regime[str(r.get("market_regime") or "unknown")].append(r)
        for regime, group in sorted(by_regime.items()):
            p = _perf(group, vname, "all")
            p["market_regime"] = regime
            regime_breakdown.append(p)

        # ── Volume bucket breakdown ──
        by_vol: dict[str, list] = defaultdict(list)
        for r in selected:
            by_vol[str(r.get("volume_ratio_bucket") or "null")].append(r)
        for vb, group in sorted(by_vol.items()):
            p = _perf(group, vname, "all")
            p["volume_bucket"] = vb
            volume_breakdown.append(p)

        # ── Skip reason ──
        skip_counts: dict[str, list] = defaultdict(list)
        for r in sim:
            skip_counts[str(r.get("_skip_reason") or "selected")].append(r)
        for sr, group in sorted(skip_counts.items()):
            p = _perf(group, vname, "all")
            p["skip_reason"] = sr
            skip_reason_out.append(p)

        # ── Rank bucket breakdown ──
        def _rb(rank) -> str:
            if rank is None:
                return "unranked"
            r = int(rank)
            if r == 1:
                return "rank_1"
            if r == 2:
                return "rank_2"
            if r <= 5:
                return "rank_3_5"
            if r <= 10:
                return "rank_6_10"
            if r <= 20:
                return "rank_11_20"
            return "rank_21_plus"

        by_rb: dict[str, list] = defaultdict(list)
        for r in sim:
            by_rb[_rb(r.get("_rank"))].append(r)
        for b, group in sorted(by_rb.items()):
            p = _perf(group, vname, "all")
            p["rank_bucket"] = b
            rank_bucket_out.append(p)

        # ── Selected trades ──
        for r in selected:
            trade_rec = {
                "variant": vname,
                "entry_date": r.get("entry_date"),
                "code": r.get("code"),
                "name": r.get("name"),
                "sector": r.get("sector"),
                "market_regime": r.get("market_regime"),
                "rank": r.get("_rank"),
                "signal_probability": r.get("signal_probability"),
                "overheat_score": r.get("overheat_score"),
                "volume_ratio": r.get("volume_ratio"),
                "hd3_ret_raw": r.get("hd3_ret_raw"),
                "hd5_ret_raw": r.get("hd5_ret_raw"),
                "hd7_ret_raw": r.get("hd7_ret_raw"),
                "hd3_ret_est12": r.get("hd3_ret_est12"),
                "emergency_stop": r.get("emergency_stop"),
            }
            selected_trades.append(trade_rec)

        logger.info(
            "[sort_variants] %s: train=%d  test=%d  all=%d  HD3_all=%.4f  WR=%.1f%%",
            vname,
            p_train.get("n", 0),
            p_test.get("n", 0),
            p_all.get("n", 0),
            p_all.get("hd3_avg") or 0,
            p_all.get("hd3_wr") or 0,
        )

    _write_csv(out_dir / "01_sort_variant_comparison_train.csv", comparison_train)
    _write_csv(out_dir / "02_sort_variant_comparison_test.csv", comparison_test)
    _write_csv(out_dir / "03_sort_variant_comparison_all.csv", comparison_all)
    _write_csv(out_dir / "04_sort_variant_regime_breakdown.csv", regime_breakdown)
    _write_csv(out_dir / "05_sort_variant_volume_breakdown.csv", volume_breakdown)
    _write_csv(out_dir / "06_sort_variant_selected_trades.csv", selected_trades)
    _write_csv(out_dir / "07_sort_variant_rank_bucket.csv", rank_bucket_out)
    _write_csv(out_dir / "08_sort_variant_skip_reason.csv", skip_reason_out)
    logger.info("[sort_variants] 01-08 done")

    # ── File 09: Bug fix verification ──
    entry_sort_raw = H5_LIVE_LIMITED_RULES.get("entry_sort")
    entry_sort_str = str(entry_sort_raw or "")
    old_matched = entry_sort_str == "signal_probability_desc"
    fix_lines = [
        "H5 Live Limited entry_sort List Bug Fix Verification",
        "=" * 60,
        "",
        "BUG DESCRIPTION:",
        "  H5_LIVE_LIMITED_RULES['entry_sort'] was stored as a list:",
        f"  {entry_sort_raw!r}",
        "",
        "  _sort_candidates() (old code) compared:",
        "    if sort_key == 'signal_probability_desc':",
        "  But sort_key = str(list) = " + repr(entry_sort_str),
        f"  Match: {old_matched}  → {'BUG: fell through to expected_value_desc' if not old_matched else 'OK'}",
        "",
        "FIX APPLIED (services/trade_case_tester.py):",
        "  _sort_candidates() now accepts str OR list[str].",
        "  - str input: normalised to [single_key] list",
        "  - list input: each element applied as priority sort key",
        "  - Unknown keys: logged as warning, skipped",
        "  - Empty key list: fallback to expected_value_desc",
        "",
        "SORT KEYS NOW SUPPORTED:",
        "  signal_probability_desc / asc",
        "  overheat_score_asc / desc",
        "  volume_ratio_desc / asc",
        "  volume_ratio_moderate  (bucket: 0.7-2.0 preferred, score=abs(v-1.3))",
        "  entry_gap_pct_asc / desc",
        "  drop_from_20d_high_asc / desc",
        "  market_regime_priority  (panic_rebound=0, risk_on=1, weak=2, normal=3)",
        "  expected_value_desc",
        "  random_seedN  (deterministic per code+date+seed)",
        "",
        "FALLBACK CONDITION:",
        "  Only when ALL keys in the list are unknown (no valid key found).",
        "  In normal operation with recognized keys, no fallback occurs.",
        "",
        "IMPACT ON EXISTING PRIMARY:",
        f"  H5_LIVE_LIMITED_RULES entry_sort = {entry_sort_raw!r}",
        "  Before fix: str(list) failed comparison → expected_value_desc applied",
        "  After fix:  list is processed correctly → intended multi-key sort applied",
        "  → The sort behavior of the existing Primary CHANGES after this fix.",
        "  → Until DB rules are updated or case_key is changed, the Primary",
        "    will now use [signal_probability_desc, overheat_score_asc, volume_ratio_desc]",
        "    instead of expected_value_desc.",
        "  → Performance impact should be evaluated via this script before deploying.",
        "",
        "VERIFICATION:",
        "  Variant 'current_bug_ev_desc' reproduces the old fallback behavior.",
        "  Variant 'intended_original' shows what the fix produces for the original list.",
        "  Compare these two variants to quantify the fix's effect.",
    ]
    (out_dir / "09_sort_bug_fix_verification.txt").write_text("\n".join(fix_lines), encoding="utf-8")
    logger.info("[sort_variants] 09 done")

    # ── File 10: Final report ──
    def _row(variant_name: str, period: str) -> dict:
        rows_list = {"train": comparison_train, "test": comparison_test, "all": comparison_all}[period]
        return next((r for r in rows_list if r.get("variant") == variant_name), {})

    def _fmt(variant_name: str, period: str) -> str:
        r = _row(variant_name, period)
        if not r:
            return "N/A"
        return (
            f"n={r.get('n'):4d}"
            f"  HD3={r.get('hd3_avg'):7.4f}"
            f"  WR={r.get('hd3_wr'):5.1f}%"
            f"  HD5={r.get('hd5_avg'):7.4f}"
            f"  HD7={r.get('hd7_avg'):7.4f}"
            f"  vs_research_HD3={r.get('hd3_vs_research', 'N/A')}"
        )

    avg_random_hd3 = _round(_avg(random_hd3_all)) if random_hd3_all else None

    # Determine best candidate
    test_perf = {r["variant"]: r for r in comparison_test if r.get("variant") != "research_all"}
    train_perf = {r["variant"]: r for r in comparison_train if r.get("variant") != "research_all"}
    all_perf = {r["variant"]: r for r in comparison_all if r.get("variant") != "research_all"}

    # Rank by test HD3 avg (higher = better)
    non_random = [v for v in SORT_VARIANTS if not v["name"].startswith("random_")]
    ranked = sorted(
        non_random,
        key=lambda v: (test_perf.get(v["name"], {}).get("hd3_avg") or -9999),
        reverse=True,
    )
    best = ranked[0]["name"] if ranked else "N/A"
    second_best = ranked[1]["name"] if len(ranked) > 1 else "N/A"

    report = [
        "H5 Live Limited Sort Variant Report",
        "=" * 70,
        f"Generated: {date.today()}",
        f"Research: n={len(dataset)} (train={len(research_train)}, test={len(research_test)})",
        f"Position limits: max_open={max_open}  max_daily={max_daily}  max_sector={max_sector}  rank_limit={rank_limit}",
        "",
        "=" * 70,
        "ENTRY_SORT BUG SUMMARY",
        "=" * 70,
        "  H5_LIVE_LIMITED_RULES['entry_sort'] stored as LIST →",
        "  old _sort_candidates compared str(list) vs 'signal_probability_desc'",
        "  → MISMATCH → expected_value_desc (EV fallback) always applied",
        "  Fix: _sort_candidates now handles list[str] correctly",
        "  Impact: Primary sort behavior changes after fix deployment",
        "",
        "=" * 70,
        "VARIANT PERFORMANCE (ALL period)",
        "=" * 70,
    ]
    report.append(f"{'Research ALL':30s}: {_fmt('research_all', 'all')}")
    for v in SORT_VARIANTS:
        report.append(f"  {v['name']:28s}: {_fmt(v['name'], 'all')}")

    report += [
        "",
        "=" * 70,
        "VARIANT PERFORMANCE (TRAIN period)",
        "=" * 70,
    ]
    report.append(f"{'Research ALL':30s}: {_fmt('research_all', 'train')}")
    for v in SORT_VARIANTS:
        report.append(f"  {v['name']:28s}: {_fmt(v['name'], 'train')}")

    report += [
        "",
        "=" * 70,
        "VARIANT PERFORMANCE (TEST period)",
        "=" * 70,
    ]
    report.append(f"{'Research ALL':30s}: {_fmt('research_all', 'test')}")
    for v in SORT_VARIANTS:
        report.append(f"  {v['name']:28s}: {_fmt(v['name'], 'test')}")

    report += [
        "",
        f"Random baseline avg HD3 (all period): {avg_random_hd3}",
        "",
        "=" * 70,
        "REGIME BREAKDOWN: panic_rebound vs normal (ALL period, selected)",
        "=" * 70,
    ]
    for v in SORT_VARIANTS:
        panic = next((r for r in regime_breakdown if r.get("variant") == v["name"] and r.get("market_regime") == "panic_rebound"), {})
        norm = next((r for r in regime_breakdown if r.get("variant") == v["name"] and r.get("market_regime") == "normal"), {})
        report.append(
            f"  {v['name']:28s}: "
            f"panic_rebound n={panic.get('n', 0):3d} HD3={panic.get('hd3_avg','N/A')}  "
            f"normal n={norm.get('n', 0):4d} HD3={norm.get('hd3_avg','N/A')}"
        )

    report += [
        "",
        "=" * 70,
        "ADOPTION JUDGMENT",
        "=" * 70,
        f"  Best (by test HD3): {best}",
        f"  Second best:        {second_best}",
        f"  Random avg (all):   {avg_random_hd3}",
        "",
    ]

    # Judgment per variant
    research_hd3_all = _row("research_all", "all").get("hd3_avg") or 0
    for v in SORT_VARIANTS:
        if v["name"].startswith("random_"):
            continue
        r_test = test_perf.get(v["name"], {})
        r_all = all_perf.get(v["name"], {})
        r_train = train_perf.get(v["name"], {})
        hd3_test = r_test.get("hd3_avg")
        hd3_train = r_train.get("hd3_avg")
        hd3_all = r_all.get("hd3_avg")
        better_than_random = (hd3_all is not None and avg_random_hd3 is not None and hd3_all > avg_random_hd3)
        better_than_research = (hd3_all is not None and hd3_all >= research_hd3_all)

        verdict = "PASS" if (better_than_random and hd3_test is not None and hd3_test >= 0) else "FAIL"
        report.append(
            f"  {v['name']:28s}: "
            f"train={hd3_train}  test={hd3_test}  all={hd3_all}  "
            f"vs_random={'>' if better_than_random else '<'}  "
            f"vs_research={'≥' if better_than_research else '<'}  "
            f"→ {verdict}"
        )

    report += [
        "",
        "=" * 70,
        "RECOMMENDATIONS",
        "=" * 70,
        f"  Best sort candidate for next DB case: {best}",
        f"  Description: {next((v['description'] for v in SORT_VARIANTS if v['name'] == best), 'N/A')}",
        "",
        "  Primary (h5_ai65_hd3_est12_cm_range330_live_limited) NOT changed.",
        "  Next step: DB-register best candidate as comparison case, run forward-test.",
        "",
        "  IMPORTANT: _sort_candidates fix changes Primary sort behavior.",
        "  Current Primary entry_sort is a list → now correctly applies intended sort.",
        "  Evaluate 'intended_original' performance before or alongside fix deployment.",
        "",
        "Output files:",
        "  01_sort_variant_comparison_train.csv",
        "  02_sort_variant_comparison_test.csv",
        "  03_sort_variant_comparison_all.csv",
        "  04_sort_variant_regime_breakdown.csv",
        "  05_sort_variant_volume_breakdown.csv",
        "  06_sort_variant_selected_trades.csv",
        "  07_sort_variant_rank_bucket.csv",
        "  08_sort_variant_skip_reason.csv",
        "  09_sort_bug_fix_verification.txt",
        "  10_sort_variant_report.txt",
    ]

    (out_dir / "10_sort_variant_report.txt").write_text("\n".join(report), encoding="utf-8")
    logger.info("[sort_variants] 10 done")
    logger.info("[sort_variants] ALL DONE. Output: %s", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="H5 Live Sort Variant Analysis")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="2026-05-28")
    parser.add_argument("--output-dir", default="outputs/h5_live_sort_variants")
    parser.add_argument("--max-open-positions", type=int, default=2)
    parser.add_argument("--max-daily-entries", type=int, default=2)
    parser.add_argument("--max-sector-positions", type=int, default=2)
    parser.add_argument("--entry-rank-limit", type=int, default=10)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
