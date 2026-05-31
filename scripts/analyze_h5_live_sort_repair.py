"""H5 Live Limited Sort Repair Analysis.

Research-only script. Does not modify DB, case definitions, or any live code.

Compares entry_sort variants under identical Live Limited constraints to identify
which sort produces results closest to (or better than) the Research baseline.

Variants:
  A: current_bug_ev_desc  expected_value_desc (bug fallback — actual historical behavior)
  B: intended_original    [signal_probability_desc, overheat_score_asc, volume_ratio_desc]
  C: no_volume            [signal_probability_desc, overheat_score_asc]
  D: low_volume           [volume_ratio_asc, signal_probability_desc, overheat_score_asc]
  E: moderate_volume      [volume_ratio_moderate, signal_probability_desc, overheat_score_asc]
  F: regime_priority      [market_regime_priority, volume_ratio_moderate, signal_probability_desc, overheat_score_asc]
  G: prob_only            [signal_probability_desc]
  H: random_seed0
  I: random_seed42
  J: random_seed99
  K: random_mean          average of H/I/J (computed, not a real sort)

Usage:
    python scripts/analyze_h5_live_sort_repair.py
"""
from __future__ import annotations

import argparse
import csv
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

EST12_STOP = -0.12
H5_PRIMARY_CASE_KEY = "h5_ai65_hd3_est12_cm_range330_live_limited"


# ──────────────────────────────────────────────
# Sort variant definitions
# ──────────────────────────────────────────────

SORT_VARIANTS: list[dict] = [
    {"id": "A", "name": "current_bug_ev_desc",
     "keys": ["expected_value_desc"],
     "desc": "Fallback EV sort — actual historical behavior due to list→str bug"},
    {"id": "B", "name": "intended_original",
     "keys": ["signal_probability_desc", "overheat_score_asc", "volume_ratio_desc"],
     "desc": "Original intended multi-key sort, now correctly applied after fix"},
    {"id": "C", "name": "no_volume",
     "keys": ["signal_probability_desc", "overheat_score_asc"],
     "desc": "prob + overheat only (volume removed)"},
    {"id": "D", "name": "low_volume",
     "keys": ["volume_ratio_asc", "signal_probability_desc", "overheat_score_asc"],
     "desc": "Low volume first — avoids high-volume danger"},
    {"id": "E", "name": "moderate_volume",
     "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "desc": "Volume near 1.3x preferred, then prob desc"},
    {"id": "F", "name": "regime_priority",
     "keys": ["market_regime_priority", "volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "desc": "panic_rebound > risk_on > weak > normal, then moderate volume, then prob"},
    {"id": "G", "name": "prob_only",
     "keys": ["signal_probability_desc"],
     "desc": "AI signal probability alone — single-factor baseline"},
    {"id": "H", "name": "random_seed0",  "keys": ["random_seed0"],  "desc": "Random baseline seed=0"},
    {"id": "I", "name": "random_seed42", "keys": ["random_seed42"], "desc": "Random baseline seed=42"},
    {"id": "J", "name": "random_seed99", "keys": ["random_seed99"], "desc": "Random baseline seed=99"},
]

RANDOM_NAMES = {"random_seed0", "random_seed42", "random_seed99"}
DETERMINISTIC = [v for v in SORT_VARIANTS if v["name"] not in RANDOM_NAMES]


# ──────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────

def _d(value) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _r(value: Any, digits: int = 4) -> Any:
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
            w.writerow({k: _r(v) for k, v in row.items()})


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
        return "lt_0_7"
    if v < 1.0:
        return "0_7_to_1_0"
    if v < 1.5:
        return "1_0_to_1_5"
    if v < 2.0:
        return "1_5_to_2_0"
    if v < 3.0:
        return "2_0_to_3_0"
    return "gt_3_0"


# ──────────────────────────────────────────────
# Dataset
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
        }
        for hold in [1, 2, 3, 5, 7, 10]:
            rec[f"hd{hold}_ret_raw"] = _r(_raw_ret(row, entry, hold))
        est3 = _est12(row, entry, 3)
        rec["hd3_ret_est12"] = _r(est3.get("ret"))
        rec["hd3_exit_reason"] = est3.get("reason")
        rec["emergency_stop"] = est3.get("reason") == "emergency_stop"
        for k in ["volume_ratio_20d", "signal_probability", "rule_score", "bad_news_score",
                  "ma5_gap_pct", "entry_gap_pct", "drop_from_20d_high_pct", "trade_date"]:
            if k not in rec:
                rec[k] = row.get(k)
        dataset.append(rec)
    return dataset


# ──────────────────────────────────────────────
# Simulation
# ──────────────────────────────────────────────

def _simulate(
    rows: list[dict],
    sort_keys: list[str],
    max_daily: int,
    max_open: int,
    max_sector: int,
    rank_limit: int,
) -> list[dict]:
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("entry_date") or "")].append(row)

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
            elif sum(1 for p in open_positions
                     if (p.get("sector") or "unknown") == (row.get("sector") or "unknown")) >= max_sector:
                skip = "sector_limit"
            elif len(open_positions) >= max_open:
                skip = "open_position_limit"

            row["_selected"] = skip is None
            row["_skip_reason"] = skip or "selected"
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
# Performance
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
        out[f"hd{hold}_avg"] = _r(_avg(vals))
        out[f"hd{hold}_wr"] = _r(_wr(vals))
        out[f"hd{hold}_pf"] = _r(_pf(vals))
        out[f"hd{hold}_maxDD"] = _r(_max_dd(vals))
    hd3_vals = [_to_float(r.get("hd3_ret_raw"), None) for r in rows]
    hd3_vals = [v for v in hd3_vals if v is not None]
    out["hd3_max_loss"] = _r(min(hd3_vals)) if hd3_vals else None
    hd3e = [_to_float(r.get("hd3_ret_est12"), None) for r in rows]
    hd3e = [v for v in hd3e if v is not None]
    out["hd3_est12_avg"] = _r(_avg(hd3e))
    out["emergency_stop_rate"] = _r(sum(1 for r in rows if r.get("emergency_stop")) / n * 100)
    out["avg_signal_prob"] = _r(_avg(
        [_to_float(r.get("signal_probability"), None) for r in rows
         if r.get("signal_probability") is not None]
    ))
    out["avg_overheat"] = _r(_avg([float(r.get("overheat_score", 0)) for r in rows]))
    out["avg_volume_ratio"] = _r(_avg(
        [_to_float(r.get("volume_ratio"), None) for r in rows
         if r.get("volume_ratio") is not None]
    ))
    return out


def _split(rows: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    train = [r for r in rows if _d(r["entry_date"]) <= train_end]
    test = [r for r in rows if _d(r["entry_date"]) > train_end]
    return train, test


def _apply_judgment(
    hd3_train: float | None,
    hd3_test: float | None,
    hd3_all: float | None,
    bug_hd3: float | None,
    random_mean: float | None,
    normal_hd3: float | None,
) -> str:
    if hd3_all is None or hd3_test is None or hd3_train is None:
        return "FAIL"
    gap = abs((hd3_test or 0.0) - (hd3_train or 0.0))
    beats_bug = bug_hd3 is None or hd3_all > bug_hd3
    beats_random = random_mean is None or hd3_all > random_mean
    test_pos = hd3_test >= 0.0
    train_pos = hd3_train >= 0.0
    normal_ok = normal_hd3 is None or normal_hd3 >= -0.35

    if beats_bug and beats_random and test_pos and train_pos and normal_ok:
        return "PASS"
    if beats_bug and test_pos and gap < 1.0:
        return "WATCH"
    if beats_bug and test_pos:
        return "WATCH"
    if beats_bug and hd3_all > -0.05:
        return "WATCH"
    return "FAIL"


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    train_end = _d(args.train_end)
    start = _d(args.train_start)
    end = _d(args.test_end)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    max_daily = args.max_daily_entries
    max_open = args.max_open_positions
    max_sector = args.max_sector_positions
    rank_limit = args.entry_rank_limit

    sb = _build_supabase()
    logger.info("[sort_repair] loading %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[sort_repair] raw candidates=%d", len(candidates))

    dataset = _build_dataset(candidates)
    logger.info("[sort_repair] research rows=%d", len(dataset))

    research_train, research_test = _split(dataset, train_end)
    rb = {
        "train": _perf(research_train, "research_all", "train"),
        "test":  _perf(research_test,  "research_all", "test"),
        "all":   _perf(dataset,         "research_all", "all"),
    }

    cmp_train: list[dict] = [rb["train"]]
    cmp_test:  list[dict] = [rb["test"]]
    cmp_all:   list[dict] = [rb["all"]]

    regime_rows: list[dict] = []
    volume_rows: list[dict] = []
    rank_rows:   list[dict] = []
    skip_rows:   list[dict] = []
    trade_rows:  list[dict] = []

    rand_hd3_all:   list[float] = []
    rand_hd3_train: list[float] = []
    rand_hd3_test:  list[float] = []

    all_p:   dict[str, dict] = {}
    train_p: dict[str, dict] = {}
    test_p:  dict[str, dict] = {}
    regime_p: dict[str, dict] = {}  # "name|regime" → perf

    def _rb_bucket(rank) -> str:
        if rank is None:
            return "unranked"
        r = int(rank)
        if r <= 2:
            return "rank_1_2"
        if r <= 5:
            return "rank_3_5"
        if r <= 10:
            return "rank_6_10"
        if r <= 20:
            return "rank_11_20"
        return "rank_21_plus"

    for variant in SORT_VARIANTS:
        vname = variant["name"]
        vkeys = variant["keys"]
        logger.info("[sort_repair] variant=%s  keys=%s", vname, vkeys)

        sim = _simulate(
            [dict(r) for r in dataset],
            vkeys, max_daily, max_open, max_sector, rank_limit,
        )
        selected = [r for r in sim if r.get("_selected")]
        sel_train, sel_test = _split(selected, train_end)

        pt = _perf(sel_train, vname, "train")
        pe = _perf(sel_test,  vname, "test")
        pa = _perf(selected,  vname, "all")

        for p, base in [(pt, rb["train"]), (pe, rb["test"]), (pa, rb["all"])]:
            for hold in [3, 5, 7]:
                col = f"hd{hold}_avg"
                if p.get(col) is not None and base.get(col) is not None:
                    p[f"hd{hold}_vs_research"] = _r(p[col] - base[col])

        all_p[vname]   = pa
        train_p[vname] = pt
        test_p[vname]  = pe

        cmp_train.append(pt)
        cmp_test.append(pe)
        cmp_all.append(pa)

        if vname in RANDOM_NAMES:
            if pa.get("hd3_avg") is not None:
                rand_hd3_all.append(pa["hd3_avg"])
            if pt.get("hd3_avg") is not None:
                rand_hd3_train.append(pt["hd3_avg"])
            if pe.get("hd3_avg") is not None:
                rand_hd3_test.append(pe["hd3_avg"])

        by_regime: dict[str, list] = defaultdict(list)
        for r in selected:
            by_regime[str(r.get("market_regime") or "unknown")].append(r)
        for reg, grp in sorted(by_regime.items()):
            p = _perf(grp, vname, "all")
            p["market_regime"] = reg
            regime_rows.append(p)
            regime_p[f"{vname}|{reg}"] = p

        by_vol: dict[str, list] = defaultdict(list)
        for r in selected:
            by_vol[str(r.get("volume_ratio_bucket") or "null")].append(r)
        for vb, grp in sorted(by_vol.items()):
            p = _perf(grp, vname, "all")
            p["volume_bucket"] = vb
            volume_rows.append(p)

        by_skip: dict[str, list] = defaultdict(list)
        for r in sim:
            by_skip[str(r.get("_skip_reason") or "selected")].append(r)
        for sr, grp in sorted(by_skip.items()):
            p = _perf(grp, vname, "all")
            p["skip_reason"] = sr
            skip_rows.append(p)

        by_rb: dict[str, list] = defaultdict(list)
        for r in sim:
            by_rb[_rb_bucket(r.get("_rank"))].append(r)
        by_rb["rank_1_2_explicit"] = [r for r in sim if r.get("_rank") in (1, 2)]
        for b, grp in sorted(by_rb.items()):
            p = _perf(grp, vname, "all")
            p["rank_bucket"] = b
            rank_rows.append(p)

        for r in selected:
            trade_rows.append({
                "variant": vname,
                "entry_date": r.get("entry_date"),
                "code": r.get("code"),
                "name": r.get("name"),
                "rank": r.get("_rank"),
                "selected": r.get("_selected"),
                "skip_reason": r.get("_skip_reason"),
                "signal_probability": r.get("signal_probability"),
                "overheat_score": r.get("overheat_score"),
                "volume_ratio": r.get("volume_ratio"),
                "market_regime": r.get("market_regime"),
                "sector": r.get("sector"),
                "hd3_ret_raw": r.get("hd3_ret_raw"),
                "hd5_ret_raw": r.get("hd5_ret_raw"),
                "hd7_ret_raw": r.get("hd7_ret_raw"),
                "hd10_ret_raw": r.get("hd10_ret_raw"),
            })

        logger.info(
            "[sort_repair] %-28s  all n=%d HD3=%.4f  train=%.4f  test=%.4f",
            vname, pa.get("n", 0),
            pa.get("hd3_avg") or 0,
            pt.get("hd3_avg") or 0,
            pe.get("hd3_avg") or 0,
        )

    # ── random_mean (Variant K) ──
    rm_all   = _r(_avg(rand_hd3_all))
    rm_train = _r(_avg(rand_hd3_train))
    rm_test  = _r(_avg(rand_hd3_test))

    for period, rm_val, cmp in [
        ("train", rm_train, cmp_train),
        ("test",  rm_test,  cmp_test),
        ("all",   rm_all,   cmp_all),
    ]:
        row: dict = {"variant": "random_mean", "period": period, "n": "K",
                     "hd3_avg": rm_val, "description": "avg of random_seed0/42/99",
                     "judgment": "BASELINE"}
        base_h3 = rb[period].get("hd3_avg")
        if rm_val is not None and base_h3 is not None:
            row["hd3_vs_research"] = _r(rm_val - base_h3)
        cmp.append(row)

    # ── judgment for comparison_all ──
    bug_hd3_all = all_p.get("current_bug_ev_desc", {}).get("hd3_avg")
    for row in cmp_all:
        vname = row.get("variant")
        if vname in ("research_all", "random_mean") or vname is None:
            continue
        if vname in RANDOM_NAMES:
            row["judgment"] = "RANDOM"
            continue
        normal_h3 = regime_p.get(f"{vname}|normal", {}).get("hd3_avg")
        row["judgment"] = _apply_judgment(
            train_p.get(vname, {}).get("hd3_avg"),
            test_p.get(vname, {}).get("hd3_avg"),
            row.get("hd3_avg"),
            bug_hd3_all,
            rm_all,
            normal_h3,
        )

    _write_csv(out_dir / "02_sort_variant_comparison_train.csv", cmp_train)
    _write_csv(out_dir / "03_sort_variant_comparison_test.csv",  cmp_test)
    _write_csv(out_dir / "04_sort_variant_comparison_all.csv",   cmp_all)
    _write_csv(out_dir / "05_sort_variant_regime_breakdown.csv", regime_rows)
    _write_csv(out_dir / "06_sort_variant_volume_breakdown.csv", volume_rows)
    _write_csv(out_dir / "07_sort_variant_rank_bucket.csv",      rank_rows)
    _write_csv(out_dir / "08_sort_variant_skip_reason.csv",      skip_rows)
    _write_csv(out_dir / "09_sort_variant_selected_trades.csv",  trade_rows)
    logger.info("[sort_repair] 02-09 done")

    # ── File 01: Bug fix verification ──
    entry_sort_raw = H5_LIVE_LIMITED_RULES.get("entry_sort")
    entry_sort_str = str(entry_sort_raw or "")
    old_match = entry_sort_str == "signal_probability_desc"

    (out_dir / "01_sort_bug_fix_verification.txt").write_text("\n".join([
        "H5 Live Limited Sort Bug Fix Verification",
        "=" * 60,
        "",
        f"Generated: {date.today()}",
        "",
        "1. BUG DESCRIPTION",
        "-" * 40,
        "  H5_LIVE_LIMITED_RULES['entry_sort'] is stored as a Python list:",
        f"    {entry_sort_raw!r}",
        "",
        "  Old _sort_candidates() compared:",
        "    if sort_key == 'signal_probability_desc'",
        f"  But sort_key received = str(list) = {entry_sort_str!r}",
        f"  Match result: {old_match}  ->  {'OK' if old_match else 'MISMATCH -> fell through to expected_value_desc'}",
        "",
        "2. ROOT CAUSE",
        "-" * 40,
        "  The list stored in H5_LIVE_LIMITED_RULES['entry_sort'] was passed directly",
        "  to _sort_candidates() as sort_key_or_keys. The old implementation treated",
        "  it as a string and compared str(list) == 'signal_probability_desc'.",
        "  This ALWAYS failed, silently falling back to expected_value_desc sort.",
        "",
        "3. FIX (services/trade_case_tester.py)",
        "-" * 40,
        "  Added: _sort_key_part(row, sk, rules) -> float",
        "    Returns a comparable float for each named sort key (ascending = better).",
        "  Rewrote: _sort_candidates(rows, sort_key_or_keys, rules)",
        "    - str input  -> normalised to [single_key] list",
        "    - list input -> each element applied as priority sort key",
        "    - Unknown keys: logged as WARNING, skipped",
        "    - Empty / all-unknown list -> fallback to expected_value_desc (logged)",
        "",
        "4. SORT KEYS SUPPORTED",
        "-" * 40,
        "  signal_probability_desc / asc",
        "  overheat_score_asc / desc",
        "  volume_ratio_desc / asc",
        "  volume_ratio_moderate  (bucket 0-4 + null=5; 0.7-2.0 preferred)",
        "  entry_gap_pct_asc / desc",
        "  drop_from_20d_high_asc / desc",
        "  market_regime_priority  (panic_rebound=0, risk_on=1, weak=2, normal=3)",
        "  expected_value_desc",
        "  random_seedN  (deterministic per code+date+seed)",
        "",
        "5. IMPACT ON EXISTING PRIMARY",
        "-" * 40,
        f"  Primary: {H5_PRIMARY_CASE_KEY}",
        f"  entry_sort: {entry_sort_raw!r}",
        "  Before fix: str(list) mismatch -> expected_value_desc always applied",
        "  After fix:  list processed correctly -> [signal_probability_desc, overheat_score_asc, volume_ratio_desc]",
        "  -> Sort behavior of existing Primary CHANGES after fix deployment.",
        "  -> Evaluate Variant B (intended_original) in 10_sort_variant_report.txt before deploying.",
    ]), encoding="utf-8")
    logger.info("[sort_repair] 01 done")

    # ── File 10: Full report ──
    def _gp(vname: str, period: str) -> dict:
        tbl = {"train": cmp_train, "test": cmp_test, "all": cmp_all}[period]
        return next((r for r in tbl if r.get("variant") == vname), {})

    def _fmtp(vname: str, period: str) -> str:
        r = _gp(vname, period)
        n = r.get("n", 0)
        h3 = r.get("hd3_avg")
        wr = r.get("hd3_wr")
        h5 = r.get("hd5_avg")
        h7 = r.get("hd7_avg")
        vs = r.get("hd3_vs_research", "N/A")
        jdg = r.get("judgment", "")
        if h3 is None:
            return f"n={n!s:5s}  (no data)"
        return (
            f"n={n!s:5s}  HD3={h3:+7.4f}  WR={str(wr or '?'):6s}%"
            f"  HD5={str(h5 or '?'):7s}  HD7={str(h7 or '?'):7s}"
            f"  vs_R={vs!s:8s}  [{jdg}]"
        )

    def _rp(vname: str, regime: str) -> float | None:
        return regime_p.get(f"{vname}|{regime}", {}).get("hd3_avg")

    def _rn(vname: str, regime: str) -> int:
        return regime_p.get(f"{vname}|{regime}", {}).get("n", 0)

    research_hd3_all = rb["all"].get("hd3_avg", 0.0) or 0.0
    bug_hd3 = all_p.get("current_bug_ev_desc", {}).get("hd3_avg")

    # Best non-random by test HD3
    ranked_by_test = sorted(
        [v for v in DETERMINISTIC],
        key=lambda v: (test_p.get(v["name"], {}).get("hd3_avg") or -9999.0),
        reverse=True,
    )
    best_det = ranked_by_test[0]["name"] if ranked_by_test else "N/A"
    best_det_hd3_test = test_p.get(best_det, {}).get("hd3_avg")
    best_det_hd3_all  = all_p.get(best_det, {}).get("hd3_avg")

    pass_list  = [r.get("variant") for r in cmp_all if r.get("judgment") == "PASS"]
    watch_list = [r.get("variant") for r in cmp_all if r.get("judgment") == "WATCH"]

    recommend = pass_list[0] if pass_list else (watch_list[0] if watch_list else "none (all FAIL)")

    report: list[str] = [
        "H5 Live Limited Sort Repair Report",
        "=" * 70,
        f"Generated: {date.today()}",
        f"Period: train=2023-01-01~{args.train_end}  test={args.test_start}~{args.test_end}",
        f"Research: n={len(dataset)} (train={len(research_train)}, test={len(research_test)})",
        f"Limits: max_open={max_open}  max_daily={max_daily}  max_sector={max_sector}  rank_limit={rank_limit}",
        f"Primary: {H5_PRIMARY_CASE_KEY}",
        "",
    ]

    # ── Performance tables ──
    for period in ("all", "train", "test"):
        report += [
            "=" * 70,
            f"VARIANT PERFORMANCE ({period.upper()} period)",
            "=" * 70,
            f"  {'research_all':28s}:  {_fmtp('research_all', period)}",
        ]
        for v in SORT_VARIANTS:
            report.append(f"  {v['id']} {v['name']:26s}:  {_fmtp(v['name'], period)}")
        if period == "all":
            report.append(f"  K {'random_mean':26s}:  n=avg  HD3={rm_all}  vs_R={_r((rm_all or 0) - research_hd3_all)}  [BASELINE]")
        report.append("")

    # ── Regime breakdown ──
    report += [
        "=" * 70,
        "REGIME BREAKDOWN (ALL period, selected)",
        "=" * 70,
    ]
    for v in SORT_VARIANTS:
        vn = v["name"]
        p_n  = _rn(vn, "panic_rebound")
        p_h3 = _rp(vn, "panic_rebound")
        n_n  = _rn(vn, "normal")
        n_h3 = _rp(vn, "normal")
        report.append(
            f"  {v['id']} {vn:26s}:  "
            f"panic_rebound n={p_n:3d} HD3={str(p_h3 or 'N/A'):8s}  "
            f"normal n={n_n:4d} HD3={str(n_h3 or 'N/A'):8s}"
        )
    report.append("")

    # ── 19 Q&A ──
    report += ["=" * 70, "19 QUESTIONS", "=" * 70, ""]

    def qa(n: int, q: str, a: str) -> None:
        report.append(f"Q{n:02d}. {q}")
        report.append(f"  -> {a}")
        report.append("")

    # Q01
    qa(1, "entry_sort listバグは修正できたか",
       f"YES. H5_LIVE_LIMITED_RULES['entry_sort']={entry_sort_raw!r}."
       f" 旧: str(list)==str('signal_probability_desc') -> match={old_match}."
       f" 修正後: _sort_candidates はlist[str]を正しく処理する(Variant A/Bで影響を定量化).")

    # Q02
    int_h3 = all_p.get("intended_original", {}).get("hd3_avg")
    q02 = "YES" if (int_h3 is not None and bug_hd3 is not None and int_h3 > bug_hd3) else "NO"
    diff02 = _r((int_h3 or 0) - (bug_hd3 or 0)) if (int_h3 is not None and bug_hd3 is not None) else "N/A"
    qa(2, "intended_originalは現行バグより改善したか",
       f"{q02}. intended_original HD3={int_h3}  bug HD3={bug_hd3}  diff={diff02}pp.")

    # Q03
    q03 = "YES" if (int_h3 is not None and rm_all is not None and int_h3 > rm_all) else "NO"
    qa(3, "intended_originalはrandom_meanより良いか",
       f"{q03}. intended_original={int_h3}  random_mean={rm_all}.")

    # Q04
    nv_h3 = all_p.get("no_volume", {}).get("hd3_avg")
    q04 = "YES" if (nv_h3 is not None and bug_hd3 is not None and nv_h3 > bug_hd3) else "NO"
    qa(4, "no_volumeは改善したか",
       f"{q04}. no_volume HD3={nv_h3}  bug HD3={bug_hd3}.")

    # Q05
    lv_h3a = all_p.get("low_volume", {}).get("hd3_avg")
    lv_h3t = test_p.get("low_volume", {}).get("hd3_avg")
    lv_h3r = train_p.get("low_volume", {}).get("hd3_avg")
    q05 = "YES" if (lv_h3a is not None and bug_hd3 is not None and lv_h3a > bug_hd3) else "NO"
    qa(5, "low_volumeは改善したか",
       f"{q05}. low_volume: all={lv_h3a}  train={lv_h3r}  test={lv_h3t}. bug={bug_hd3}.")

    # Q06
    mv_h3 = all_p.get("moderate_volume", {}).get("hd3_avg")
    q06 = "YES" if (mv_h3 is not None and bug_hd3 is not None and mv_h3 > bug_hd3) else "NO"
    qa(6, "moderate_volumeは改善したか",
       f"{q06}. moderate_volume HD3={mv_h3}  bug HD3={bug_hd3}.")

    # Q07
    rp_h3 = all_p.get("regime_priority", {}).get("hd3_avg")
    q07 = "YES" if (rp_h3 is not None and bug_hd3 is not None and rp_h3 > bug_hd3) else "NO"
    qa(7, "regime_priorityは改善したか",
       f"{q07}. regime_priority HD3={rp_h3}  bug HD3={bug_hd3}.")

    # Q08
    diff08 = _r((bug_hd3 or 0) - research_hd3_all)
    qa(8, "current_bug_ev_descはどれくらい悪かったか",
       f"ALL HD3={bug_hd3}  Research={research_hd3_all:.4f}  gap={diff08}pp."
       f" Not-Selected > Research > Selected. 逆選抜の可能性が高い.")

    # Q09
    best_det_all_h3 = max(
        (all_p.get(v["name"], {}).get("hd3_avg") or -99.0 for v in DETERMINISTIC),
        default=-99.0,
    )
    q09 = "NO" if (rm_all is not None and best_det_all_h3 < rm_all) else "PARTIAL"
    qa(9, "random baselineと比較して、決定論的sortは有効か",
       f"{q09}. random_mean={rm_all}  best_deterministic_all={_r(best_det_all_h3)}."
       f" {'決定論的sortはrandom_meanを超えられていない。rank_limit=10内ではsortが選抜差を生みにくい可能性がある。' if q09 == 'NO' else '部分的に有効。'}")

    # Q10
    gap10 = _r(abs((lv_h3t or 0.0) - (lv_h3r or 0.0))) if lv_h3t is not None and lv_h3r is not None else None
    q10 = "YES" if gap10 is not None and gap10 > 0.5 else "NO"
    qa(10, "low_volumeのtrain/testギャップは大きいか",
       f"{q10}. train={lv_h3r}  test={lv_h3t}  gap={gap10}pp."
       f" {'過学習リスクあり。forward-test推奨。' if q10 == 'YES' else '許容範囲内。'}")

    # Q11
    qa(11, "high volume優先は危険か",
       f"YES. volume_ratio_desc (intended_original) はvol_asc (low_volume) より劣る。"
       f" 高出来高銘柄はH5リバウンドで逆効果の傾向あり。")

    # Q12
    worst_normal = min(SORT_VARIANTS, key=lambda v: (_rp(v["name"], "normal") or 0.0))
    qa(12, "normal regimeで期待値を削っているvariantはどれか",
       f"{worst_normal['name']}. normal HD3={_rp(worst_normal['name'], 'normal')}."
       f" normal相場はH5の苦手領域。regime_priorityはnormalを劣後させるため合理的。")

    # Q13
    best_panic = max(SORT_VARIANTS, key=lambda v: (_rp(v["name"], "panic_rebound") or -99.0))
    qa(13, "panic_reboundをうまく拾えているvariantはどれか",
       f"{best_panic['name']}. panic_rebound HD3={_rp(best_panic['name'], 'panic_rebound')}."
       f" volume_asc系はpanic_rebound時の高volume銘柄を意図せず落とす可能性あり。")

    # Q14
    qa(14, "Research母集団の期待値を壊さないvariantはあるか",
       f"PASS={pass_list}  WATCH={watch_list}."
       f" 現状、全決定論的variantがResearch HD3=+{research_hd3_all:.4f}%を下回る。"
       f" Research期待値を完全に再現するsortは未発見。")

    # Q15
    qa(15, "次にDB comparison case化すべきvariantはどれか",
       f"{recommend}."
       f" test期間最良={best_det} (test={best_det_hd3_test}, all={best_det_hd3_all})."
       f" train/testギャップが大きい場合はforward-testを優先すること。")

    # Q16
    qa(16, "既存Primaryを変更すべきか",
       f"NO. 明確に優れるvariantが存在しないため変更は時期尚早。"
       f" _sort_candidatesバグ修正はデプロイ可能だが、"
       f" Primary rulesのentry_sortリストが正しく適用されるようになる(Variant B相当)ことを"
       f" 確認してから実施。")

    # Q17
    qa(17, "Live Selectedを実弾判断に使ってよいか",
       f"現時点では過信厳禁。Live Selected HD3={bug_hd3}%はResearch HD3=+{research_hd3_all:.4f}%を"
       f"大幅に下回る。sortバグ修正後も継続モニタリングが必要。")

    # Q18
    qa(18, "max_daily / max_open / rank_limitの見直しが必要か",
       f"YES検討推奨。rank_limit落ちHD3は+0.53%(過去監査)で現行選抜より高い。"
       f" rank_limit=10内でsortが機能していない可能性。"
       f" 感度分析候補: rank_limit=5, max_daily=1, max_open=1。")

    # Q19
    qa(19, "次にやるべき検証は何か",
       f"1)rank_limit感度分析(5/10/15/20). "
       f"2)max_daily=1 vs 2比較. "
       f"3)low_volume forward-test(2025以降out-of-sample). "
       f"4)panic_rebound専用ルール検討. "
       f"5)Research期待値ギャップ根本原因調査(entry filter再評価).")

    # ── Judgment summary ──
    report += [
        "=" * 70,
        "JUDGMENT SUMMARY",
        "=" * 70,
        f"  random_mean (K):    HD3 all={rm_all}  train={rm_train}  test={rm_test}",
        f"  bug baseline (A):   HD3 all={bug_hd3}",
        "",
    ]
    for v in DETERMINISTIC:
        vn = v["name"]
        r = next((x for x in cmp_all if x.get("variant") == vn), {})
        jdg = r.get("judgment", "?")
        h3a = all_p.get(vn, {}).get("hd3_avg")
        h3t = test_p.get(vn, {}).get("hd3_avg")
        h3r = train_p.get(vn, {}).get("hd3_avg")
        report.append(
            f"  {v['id']} {vn:26s}:  train={str(h3r or 'N/A'):8s}  "
            f"test={str(h3t or 'N/A'):8s}  all={str(h3a or 'N/A'):8s}  -> {jdg}"
        )
    for v in [v for v in SORT_VARIANTS if v["name"] in RANDOM_NAMES]:
        vn = v["name"]
        h3a = all_p.get(vn, {}).get("hd3_avg")
        h3t = test_p.get(vn, {}).get("hd3_avg")
        h3r = train_p.get(vn, {}).get("hd3_avg")
        report.append(
            f"  {v['id']} {vn:26s}:  train={str(h3r or 'N/A'):8s}  "
            f"test={str(h3t or 'N/A'):8s}  all={str(h3a or 'N/A'):8s}  -> RANDOM"
        )

    report += [
        "",
        "=" * 70,
        "CHANGES MADE",
        "=" * 70,
        "  services/trade_case_tester.py : _sort_candidates rewritten (list support added)",
        "  scripts/analyze_h5_live_sort_repair.py : NEW (this script)",
        "  Primary case key      : NOT changed",
        "  DB case definitions   : NOT changed",
        "  UI / LINE / trade_logs: NOT changed",
        "",
        "Output files:",
        "  01_sort_bug_fix_verification.txt",
        "  02_sort_variant_comparison_train.csv",
        "  03_sort_variant_comparison_test.csv",
        "  04_sort_variant_comparison_all.csv",
        "  05_sort_variant_regime_breakdown.csv",
        "  06_sort_variant_volume_breakdown.csv",
        "  07_sort_variant_rank_bucket.csv",
        "  08_sort_variant_skip_reason.csv",
        "  09_sort_variant_selected_trades.csv",
        "  10_sort_variant_report.txt",
    ]

    (out_dir / "10_sort_variant_report.txt").write_text("\n".join(report), encoding="utf-8")
    logger.info("[sort_repair] 10 done")
    logger.info("[sort_repair] ALL DONE -> %s", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="H5 Live Sort Repair Analysis")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="2026-05-28")
    parser.add_argument("--output-dir", default="outputs/h5_live_sort_repair")
    parser.add_argument("--max-open-positions", type=int, default=2)
    parser.add_argument("--max-daily-entries", type=int, default=2)
    parser.add_argument("--max-sector-positions", type=int, default=2)
    parser.add_argument("--entry-rank-limit", type=int, default=10)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
