"""H5 Live Limited Selection Audit.

Research-only script. Does not modify DB, case definitions, or any live code.

Audits the current Live Limited selection logic vs. Research full population:
  - Fetches actual DB rules and compares with code constants
  - Reproduces rank logic and labels each candidate
  - Compares selected vs. non-selected performance
  - Tests entry_sort variants, position limit sensitivity, regime breakdown

Usage:
    python scripts/analyze_h5_live_selection_audit.py
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from services.h5_primary import (
    H5_LIVE_LIMITED_CASE_KEY,
    H5_LIVE_LIMITED_RULES,
    H5_PRIMARY_CASE_KEY,
    H5_RESEARCH_CASE_KEY,
    H5_RESEARCH_RULES,
    h5_overheat_score,
)
from services.trade_case_tester import _build_supabase, _fetch_all, _load_candidates_v2, _to_float

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MAX_HOLD = 10
EST12_STOP = -0.12


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
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, digits)
    except Exception:
        return value


def _avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def _pf(values: list[float]) -> float | None:
    wins = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v <= 0))
    if losses <= 0:
        return None if wins <= 0 else 999.0
    return wins / losses


def _wr(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(1 for v in values if v > 0) / len(values) * 100.0


def _max_dd(values: list[float]) -> float:
    equity = peak = dd = 0.0
    for v in values:
        equity += v
        peak = max(peak, equity)
        dd = min(dd, equity - peak)
    return dd


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _round(v) for k, v in row.items()})


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


# ──────────────────────────────────────────────
# Return computation (using future_close_Nd labels)
# ──────────────────────────────────────────────

def _raw_ret(row: dict, entry: float, hold: int) -> float | None:
    c = _to_float(row.get(f"future_close_{hold}d"), None)
    if c is None or entry <= 0:
        return None
    return (c / entry - 1.0) * 100.0


def _est12_ret(row: dict, entry: float, hold: int) -> dict:
    stop_price = entry * (1.0 + EST12_STOP)
    last_close = None
    last_day = 0
    for d in range(1, hold + 1):
        low = _to_float(row.get(f"future_low_{d}d"), None)
        close = _to_float(row.get(f"future_close_{d}d"), None)
        if close is not None:
            last_close = close
            last_day = d
        if low is not None and low <= stop_price:
            return {"ret": EST12_STOP * 100.0, "exit_day": d, "exit_reason": "emergency_stop"}
    if last_close is None:
        return {"ret": None, "exit_day": None, "exit_reason": "no_data"}
    return {"ret": (last_close / entry - 1.0) * 100.0, "exit_day": last_day, "exit_reason": "time_stop"}


# ──────────────────────────────────────────────
# Expected Value computation (mirrors trade_case_tester._expected_value_for_rules)
# ──────────────────────────────────────────────

def _ev(row: dict) -> float:
    """Reproduce expected_value_desc sort key used by _sort_candidates fallback."""
    ai = _to_float(row.get("signal_probability"), 0.0) or 0.0
    rule = _to_float(row.get("rule_score"), 50.0) or 50.0
    bad = _to_float(row.get("bad_news_score"), 0.0) or 0.0
    # Live limited rules have no tp_pct/sl_pct, defaults: 0.06 / -0.04
    tp_pct = 6.0
    sl_pct = -4.0
    rule_adjust = (rule - 50.0) * 0.035
    bad_adjust = min(1.5, bad * 0.20)
    return round((ai * tp_pct) + ((1.0 - ai) * sl_pct) + rule_adjust - bad_adjust, 3)


# ──────────────────────────────────────────────
# Sort functions
# ──────────────────────────────────────────────

def _sort_ev_desc(rows: list[dict]) -> list[dict]:
    """Current actual sort: expected_value_desc (what the code actually applies)."""
    return sorted(rows, key=lambda r: (_ev(r), _to_float(r.get("signal_probability"), 0) or 0), reverse=True)


def _sort_prob_desc(rows: list[dict]) -> list[dict]:
    """signal_probability desc (intended by entry_sort list but not actually applied)."""
    return sorted(rows, key=lambda r: (_to_float(r.get("signal_probability"), 0) or 0, _ev(r)), reverse=True)


def _sort_low_volume(rows: list[dict]) -> list[dict]:
    """volume_ratio asc (volume_ratio desc reversed)."""
    return sorted(rows, key=lambda r: _to_float(r.get("volume_ratio_20d"), 1.0) or 1.0)


def _sort_moderate_volume(rows: list[dict]) -> list[dict]:
    """Prefer volume_ratio closest to 1.2 (moderate)."""
    return sorted(rows, key=lambda r: abs((_to_float(r.get("volume_ratio_20d"), 1.2) or 1.2) - 1.2))


def _sort_no_volume(rows: list[dict]) -> list[dict]:
    """signal_probability desc + overheat_score asc only (no volume)."""
    return sorted(
        rows,
        key=lambda r: (
            _to_float(r.get("signal_probability"), 0) or 0,
            -(h5_overheat_score(r)),
        ),
        reverse=True,
    )


def _sort_drop_deep(rows: list[dict]) -> list[dict]:
    """drop_from_20d_high_pct asc (more negative = deeper drop = higher priority)."""
    return sorted(rows, key=lambda r: _to_float(r.get("drop_from_20d_high_pct"), 0) or 0)


def _sort_random(rows: list[dict], seed: int) -> list[dict]:
    shuffled = list(rows)
    random.Random(seed).shuffle(shuffled)
    return shuffled


# ──────────────────────────────────────────────
# Live Limited simulation
# ──────────────────────────────────────────────

def _simulate_selection(
    rows: list[dict],
    sort_fn: Callable[[list[dict]], list[dict]],
    max_daily: int = 2,
    max_open: int = 2,
    max_sector: int = 2,
    rank_limit: int = 10,
) -> list[dict]:
    """Simulate Live Limited selection. Returns rows with selection metadata attached."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("entry_date") or row.get("trade_date") or "")].append(row)

    # Assign in-day rank per date
    for dt, day_rows in by_date.items():
        sorted_day = sort_fn(day_rows)
        for i, r in enumerate(sorted_day):
            r["_rank"] = i + 1
            r["_within_rank_limit"] = (i + 1) <= rank_limit

    open_positions: list[dict] = []
    selected_all: list[dict] = []

    for dt in sorted(by_date.keys()):
        today = _d(dt)
        # Remove expired positions (HD3 = 3 trading days ≈ 5 calendar days)
        open_positions = [p for p in open_positions if _d(p.get("_expiry_date")) >= today]

        day_rows = sorted(by_date[dt], key=lambda r: r.get("_rank", 9999))
        top_rows = [r for r in day_rows if r.get("_within_rank_limit")]
        below_rank = [r for r in day_rows if not r.get("_within_rank_limit")]

        daily_entries = 0
        for row in top_rows:
            skip_reason = None
            if daily_entries >= max_daily:
                skip_reason = "daily_limit"
            elif len(open_positions) >= max_open:
                skip_reason = "open_position_limit"
            else:
                sector = str(row.get("sector") or "unknown")
                sector_count = sum(1 for p in open_positions if (p.get("sector") or "unknown") == sector)
                if sector_count >= max_sector:
                    skip_reason = "sector_limit"

            row["_selected"] = skip_reason is None
            row["_skip_reason"] = skip_reason or ""
            if skip_reason is None:
                daily_entries += 1
                expiry = today + timedelta(days=5)
                row["_expiry_date"] = expiry.isoformat()
                open_positions.append(row)
            selected_all.append(row)

        for row in below_rank:
            row["_selected"] = False
            row["_skip_reason"] = "rank_limit"
            selected_all.append(row)

    return selected_all


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
            "volume_ratio": _to_float(row.get("volume_ratio_20d"), None),
            "drop_from_20d_high_pct": _to_float(row.get("drop_from_20d_high_pct"), None),
            "margin_ratio": _to_float(row.get("margin_ratio"), None),
            "rule_score": _to_float(row.get("rule_score"), None),
            "bad_news_score": _to_float(row.get("bad_news_score"), None),
            "ev_score": _ev(row),
            "entry_gap_pct": _to_float(row.get("ma5_gap_pct"), None),  # proxy for entry_gap
        }

        # Returns
        for hold in [1, 2, 3, 5, 7, 10]:
            rec[f"hd{hold}_ret_raw"] = _round(_raw_ret(row, entry, hold))
        est3 = _est12_ret(row, entry, 3)
        rec["hd3_ret_est12"] = _round(est3.get("ret"))
        rec["hd3_exit_reason"] = est3.get("exit_reason")
        rec["emergency_stop_hit"] = est3.get("exit_reason") == "emergency_stop"

        dataset.append(rec)
    return dataset


# ──────────────────────────────────────────────
# Performance summary helper
# ──────────────────────────────────────────────

def _perf(rows: list[dict], label: str, period: str) -> dict:
    n = len(rows)
    if n == 0:
        return {"label": label, "period": period, "n": 0}
    hd3 = [_to_float(r.get("hd3_ret_raw"), None) for r in rows]
    hd3 = [v for v in hd3 if v is not None]
    hd5 = [_to_float(r.get("hd5_ret_raw"), None) for r in rows]
    hd5 = [v for v in hd5 if v is not None]
    hd7 = [_to_float(r.get("hd7_ret_raw"), None) for r in rows]
    hd7 = [v for v in hd7 if v is not None]
    hd10 = [_to_float(r.get("hd10_ret_raw"), None) for r in rows]
    hd10 = [v for v in hd10 if v is not None]
    hd3e = [_to_float(r.get("hd3_ret_est12"), None) for r in rows]
    hd3e = [v for v in hd3e if v is not None]
    stop_rate = _round(sum(1 for r in rows if r.get("emergency_stop_hit")) / n * 100)
    return {
        "label": label,
        "period": period,
        "n": n,
        "hd3_raw_avg": _round(_avg(hd3)),
        "hd3_raw_wr": _round(_wr(hd3)),
        "hd3_raw_pf": _round(_pf(hd3)),
        "hd3_est12_avg": _round(_avg(hd3e)),
        "hd5_raw_avg": _round(_avg(hd5)),
        "hd5_raw_wr": _round(_wr(hd5)),
        "hd7_raw_avg": _round(_avg(hd7)),
        "hd7_raw_wr": _round(_wr(hd7)),
        "hd10_raw_avg": _round(_avg(hd10)),
        "hd10_raw_wr": _round(_wr(hd10)),
        "emergency_stop_rate": stop_rate,
        "avg_signal_prob": _round(_avg([_to_float(r.get("signal_probability"), None) for r in rows if r.get("signal_probability") is not None])),
        "avg_overheat": _round(_avg([_to_float(r.get("overheat_score"), None) for r in rows if r.get("overheat_score") is not None])),
        "avg_volume_ratio": _round(_avg([_to_float(r.get("volume_ratio"), None) for r in rows if r.get("volume_ratio") is not None])),
        "avg_ev_score": _round(_avg([_to_float(r.get("ev_score"), None) for r in rows if r.get("ev_score") is not None])),
        "avg_drop_from_20d": _round(_avg([_to_float(r.get("drop_from_20d_high_pct"), None) for r in rows if r.get("drop_from_20d_high_pct") is not None])),
    }


# ──────────────────────────────────────────────
# Period split
# ──────────────────────────────────────────────

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
    test_start = _d(args.test_start)
    test_end = _d(args.test_end)
    start = min(train_start, test_start)
    end = max(train_end, test_end)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sb = _build_supabase()

    # ── File 01: DB rules ──
    logger.info("[audit] fetching DB rules for %s", H5_LIVE_LIMITED_CASE_KEY)
    try:
        db_result = (
            sb.table("trade_case_definitions")
            .select("case_key,is_enabled,rules")
            .eq("case_key", H5_LIVE_LIMITED_CASE_KEY)
            .execute()
        )
        db_rows = db_result.data if db_result else []
        db_rules_raw = db_rows[0] if db_rows else {}
    except Exception as exc:
        logger.warning("[audit] DB query failed: %s", exc)
        db_rules_raw = {"error": str(exc)}

    db_rules_json = json.dumps(db_rules_raw, ensure_ascii=False, indent=2, default=str)
    (out_dir / "01_live_limited_db_rules.json").write_text(db_rules_json, encoding="utf-8")
    logger.info("[audit] 01 done")

    # ── File 02: Code constants ──
    code_const_lines = [
        "H5 Primary Code Constants",
        "=" * 60,
        f"H5_PRIMARY_CASE_KEY         = {H5_PRIMARY_CASE_KEY}",
        f"H5_LIVE_LIMITED_CASE_KEY    = {H5_LIVE_LIMITED_CASE_KEY}",
        f"H5_RESEARCH_CASE_KEY        = {H5_RESEARCH_CASE_KEY}",
        "",
        "H5_LIVE_LIMITED_RULES (services/h5_primary.py):",
        json.dumps(H5_LIVE_LIMITED_RULES, ensure_ascii=False, indent=2, default=str),
        "",
        "H5_RESEARCH_RULES (services/h5_primary.py):",
        json.dumps(H5_RESEARCH_RULES, ensure_ascii=False, indent=2, default=str),
    ]
    (out_dir / "02_h5_primary_constants.txt").write_text("\n".join(code_const_lines), encoding="utf-8")
    logger.info("[audit] 02 done")

    # ── File 03: Selection flow report ──
    # Analyze _sort_candidates behavior: entry_sort is a list,
    # str(list) != "signal_probability_desc" → falls through to expected_value_desc
    entry_sort_raw = H5_LIVE_LIMITED_RULES.get("entry_sort")
    entry_sort_str = str(entry_sort_raw or "")
    actual_sort_applied = (
        "signal_probability_desc"
        if entry_sort_str == "signal_probability_desc"
        else "expected_value_desc (FALLBACK)"
    )
    flow_lines = [
        "H5 Live Limited Selection Flow",
        "=" * 60,
        "",
        "Confirmed from code (services/trade_case_tester.py, ~line 1369-1431):",
        "",
        "STEP 1: Filter candidates by entry conditions",
        "  - signal_probability >= min_ai_score (0.65)",
        "  - signal_stage in {confirmed, strong_confirmed}",
        "  - drop_from_20d_high_pct <= -8.0",
        "  - market_regime != panic_selloff",
        "  - overheat_score <= max_overheat_score (1)",
        "  - margin_ratio in [3, 30] (if use_margin_filter and margin data present)",
        "",
        "STEP 2: Group by entry_date",
        "",
        "STEP 3: Apply regime_adjust (regime-specific multiplier on entry_rank_limit)",
        "  - _adjusted_rules() reads rules.regime_adjust[market_regime]",
        "  - H5_LIVE_LIMITED_RULES has no regime_adjust → no adjustment",
        "",
        "STEP 4: Sort within date by entry_sort",
        f"  rules['entry_sort'] value (type={type(entry_sort_raw).__name__}): {entry_sort_raw!r}",
        f"  str(entry_sort) = {entry_sort_str!r}",
        f"  _sort_candidates checks: str(sort_key) == 'signal_probability_desc'",
        f"  → match? {entry_sort_str == 'signal_probability_desc'}",
        f"  → ACTUAL SORT APPLIED: {actual_sort_applied}",
        "",
        "  CRITICAL: entry_sort is stored as a list, but _sort_candidates expects a string.",
        "  str(['signal_probability_desc', ...]) != 'signal_probability_desc'",
        "  → The code falls through to expected_value_desc (EV-based) sort.",
        "  → overheat_score_asc and volume_ratio_desc are NEVER applied.",
        "  → entry_gap_pct_asc is NOT in H5_LIVE_LIMITED_RULES at all.",
        "",
        "  expected_value_desc formula:",
        "    ai * tp_pct + (1-ai) * sl_pct + (rule-50)*0.035 - min(1.5, bad*0.20)",
        "    With tp_pct=6.0, sl_pct=-4.0 (Live Limited has no tp/sl overrides)",
        "    ≈ 10*ai - 4 + (rule-50)*0.035 - bad_adjust",
        "",
        "STEP 5: Apply entry_rank_limit (10) → take top-10 per day",
        "",
        "STEP 6: Apply position limits (iterate through top-10 in rank order):",
        "  a. if daily_entries >= max_daily_entries (2) → break",
        "  b. if open_positions >= max_open_positions (2) → break",
        "  c. if sector_count >= max_sector_positions (2) → continue (skip this row, try next)",
        "",
        "  Note: daily_limit and open_position_limit break the loop entirely.",
        "  sector_limit continues to the next candidate.",
        "",
        "STEP 7: Create simulation / virtual trade",
        "  No live_candidate_rank or live_skip_reason stored in current production code.",
        "  These fields are not written to the database by the simulation loop.",
        "",
        "Summary of discrepancies vs. expected spec:",
        "  1. entry_sort list → str() mismatch → EV sort used instead of prob+overheat+vol",
        "  2. entry_gap_pct_asc not in H5_LIVE_LIMITED_RULES (not implemented anywhere)",
        "  3. live_skip_reason not persisted in DB (only exists transiently during simulation)",
    ]
    (out_dir / "03_selection_flow_report.txt").write_text("\n".join(flow_lines), encoding="utf-8")
    logger.info("[audit] 03 done")

    # ── Load candidates ──
    logger.info("[audit] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[audit] raw candidates=%d", len(candidates))

    dataset = _build_dataset(candidates)
    logger.info("[audit] research dataset rows=%d", len(dataset))

    research_train, research_test = _split(dataset, train_end)

    # ── Simulate current actual sort (EV desc = what the code does) ──
    simulated_all = _simulate_selection(
        [dict(r) for r in dataset],
        _sort_ev_desc,
        max_daily=2, max_open=2, max_sector=2, rank_limit=10,
    )

    for row in simulated_all:
        row["selected_by_live_limited"] = row.pop("_selected", False)
        row["live_candidate_rank"] = row.pop("_rank", None)
        row["within_rank_limit"] = row.pop("_within_rank_limit", False)
        row["live_skip_reason"] = row.pop("_skip_reason", "")
        row.pop("_expiry_date", None)

    # Rank bucket
    def _rank_bucket(row: dict) -> str:
        rank = row.get("live_candidate_rank")
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

    for row in simulated_all:
        row["rank_bucket"] = _rank_bucket(row)

    sim_train, sim_test = _split(simulated_all, train_end)

    # ── File 04: Full dataset CSV ──
    _write_csv(out_dir / "04_live_selection_dataset.csv", simulated_all)
    logger.info("[audit] 04 done")

    # ── File 05: Selected vs Not Selected ──
    sel_rows: list[dict] = []
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        selected = [r for r in rows if r.get("selected_by_live_limited")]
        not_selected = [r for r in rows if not r.get("selected_by_live_limited")]
        rank_limited = [r for r in not_selected if r.get("live_skip_reason") in {"daily_limit", "open_position_limit", "sector_limit"}]
        rank_below = [r for r in not_selected if r.get("live_skip_reason") == "rank_limit"]
        sel_rows.append(_perf(rows, "research_all", period))
        sel_rows.append(_perf(selected, "live_selected", period))
        sel_rows.append(_perf(not_selected, "not_selected", period))
        sel_rows.append(_perf(rank_limited, "not_selected_position_limited", period))
        sel_rows.append(_perf(rank_below, "not_selected_rank_below_10", period))
    _write_csv(out_dir / "05_selected_vs_not_selected.csv", sel_rows)
    logger.info("[audit] 05 done")

    # ── File 06: Rank bucket performance ──
    rank_buckets_out: list[dict] = []
    bucket_order = ["rank_1", "rank_2", "rank_3_5", "rank_6_10", "rank_11_20", "rank_21_plus", "unranked"]
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        by_bucket: dict[str, list] = defaultdict(list)
        for r in rows:
            by_bucket[r.get("rank_bucket", "unranked")].append(r)
        for b in bucket_order:
            group = by_bucket.get(b, [])
            if group:
                rank_buckets_out.append(_perf(group, b, period))
    _write_csv(out_dir / "06_rank_bucket_performance.csv", rank_buckets_out)
    logger.info("[audit] 06 done")

    # ── File 07: entry_sort factor buckets ──
    factor_rows: list[dict] = []

    def _factor_bucket(rows: list[dict], factor: str, buckets: list[tuple]) -> list[dict]:
        out = []
        for period, period_rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
            by_b: dict[str, list] = defaultdict(list)
            for r in period_rows:
                val = _to_float(r.get(factor), None)
                matched = "null"
                if val is not None:
                    for label, lo, hi in buckets:
                        if lo <= val < hi:
                            matched = label
                            break
                by_b[matched].append(r)
            for b_label, group in sorted(by_b.items()):
                p = _perf(group, f"{factor}={b_label}", period)
                p[factor + "_bucket"] = b_label
                out.append(p)
        return out

    factor_rows += _factor_bucket(
        simulated_all, "signal_probability",
        [("0.65_0.70", 0.65, 0.70), ("0.70_0.75", 0.70, 0.75),
         ("0.75_0.80", 0.75, 0.80), ("0.80_0.85", 0.80, 0.85), ("0.85_1.0", 0.85, 1.1)],
    )
    factor_rows += _factor_bucket(
        simulated_all, "volume_ratio",
        [("lt0.7", 0.0, 0.7), ("0.7_1.0", 0.7, 1.0), ("1.0_1.5", 1.0, 1.5),
         ("1.5_2.0", 1.5, 2.0), ("2.0_3.0", 2.0, 3.0), ("3.0_plus", 3.0, 9999)],
    )
    factor_rows += _factor_bucket(
        simulated_all, "drop_from_20d_high_pct",
        [("lt-15", -999, -15), ("-15_-12", -15, -12), ("-12_-10", -12, -10),
         ("-10_-8", -10, -8)],
    )
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        by_oh: dict[int, list] = defaultdict(list)
        by_oh_null: list = []
        for r in rows:
            s = r.get("overheat_score")
            if s is None:
                by_oh_null.append(r)
            else:
                by_oh[int(s)].append(r)
        for oh_val, group in sorted(by_oh.items()):
            p = _perf(group, f"overheat={oh_val}", period)
            p["overheat_score_bucket"] = str(oh_val)
            factor_rows.append(p)
        if by_oh_null:
            p = _perf(by_oh_null, "overheat=null", period)
            p["overheat_score_bucket"] = "null"
            factor_rows.append(p)

    _write_csv(out_dir / "07_entry_sort_factor_buckets.csv", factor_rows)
    logger.info("[audit] 07 done")

    # ── File 08: Sort variant comparison ──
    sort_variants: list[tuple[str, Callable]] = [
        ("current_ev_desc", _sort_ev_desc),
        ("prob_desc", _sort_prob_desc),
        ("low_volume_asc", _sort_low_volume),
        ("moderate_volume", _sort_moderate_volume),
        ("no_volume", _sort_no_volume),
        ("drop_deep", _sort_drop_deep),
        ("random_seed42", lambda rows: _sort_random(rows, 42)),
        ("random_seed0", lambda rows: _sort_random(rows, 0)),
        ("random_seed99", lambda rows: _sort_random(rows, 99)),
    ]
    sort_rows: list[dict] = []
    for variant_name, sort_fn in sort_variants:
        sim = _simulate_selection(
            [dict(r) for r in dataset],
            sort_fn,
            max_daily=2, max_open=2, max_sector=2, rank_limit=10,
        )
        for r in sim:
            r["entry_date"] = r.get("entry_date") or r.get("trade_date") or ""
        sim_t = [r for r in sim if r.get("_selected") and _d(r["entry_date"]) <= train_end]
        sim_te = [r for r in sim if r.get("_selected") and _d(r["entry_date"]) > train_end]
        sim_a = [r for r in sim if r.get("_selected")]
        for period, sel in [("train", sim_t), ("test", sim_te), ("all", sim_a)]:
            p = _perf(sel, variant_name, period)
            sort_rows.append(p)
    _write_csv(out_dir / "08_sort_variant_comparison.csv", sort_rows)
    logger.info("[audit] 08 done")

    # ── File 09: Position limit sensitivity ──
    limit_rows: list[dict] = []
    for param, values, fixed in [
        ("max_open", [1, 2, 3, 5, 999], {"max_daily": 2, "max_sector": 2, "rank_limit": 10}),
        ("max_daily", [1, 2, 3, 5, 999], {"max_open": 2, "max_sector": 2, "rank_limit": 10}),
        ("max_sector", [1, 2, 3, 999], {"max_open": 2, "max_daily": 2, "rank_limit": 10}),
        ("rank_limit", [2, 5, 10, 20, 999], {"max_open": 2, "max_daily": 2, "max_sector": 2}),
    ]:
        for v in values:
            kwargs = dict(fixed)
            kwargs[param] = v
            sim = _simulate_selection(
                [dict(r) for r in dataset],
                _sort_ev_desc,
                **kwargs,
            )
            sel = [r for r in sim if r.get("_selected")]
            for r in sel:
                r["entry_date"] = r.get("entry_date") or ""
            sel_t = [r for r in sel if _d(r["entry_date"]) <= train_end]
            sel_te = [r for r in sel if _d(r["entry_date"]) > train_end]
            sel_a = sel
            for period, rows in [("train", sel_t), ("test", sel_te), ("all", sel_a)]:
                p = _perf(rows, f"{param}={v}", period)
                p["varied_param"] = param
                p["varied_value"] = v
                limit_rows.append(p)
    _write_csv(out_dir / "09_position_limit_sensitivity.csv", limit_rows)
    logger.info("[audit] 09 done")

    # ── File 10: Skip reason performance ──
    skip_rows: list[dict] = []
    skip_reasons = ["", "daily_limit", "open_position_limit", "sector_limit", "rank_limit"]
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        for sr in skip_reasons:
            group = [r for r in rows if r.get("live_skip_reason") == sr]
            label = "selected" if sr == "" else sr
            p = _perf(group, label, period)
            p["skip_reason"] = label
            skip_rows.append(p)
    _write_csv(out_dir / "10_skip_reason_performance.csv", skip_rows)
    logger.info("[audit] 10 done")

    # ── File 11: Regime selection performance ──
    regime_rows: list[dict] = []
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        by_regime: dict[str, list] = defaultdict(list)
        for r in rows:
            by_regime[str(r.get("market_regime") or "unknown")].append(r)
        for regime, group in sorted(by_regime.items()):
            selected = [r for r in group if r.get("selected_by_live_limited")]
            not_sel = [r for r in group if not r.get("selected_by_live_limited")]
            p_all = _perf(group, f"research_{regime}", period)
            p_sel = _perf(selected, f"selected_{regime}", period)
            p_not = _perf(not_sel, f"not_selected_{regime}", period)
            p_all["market_regime"] = regime
            p_sel["market_regime"] = regime
            p_not["market_regime"] = regime
            regime_rows += [p_all, p_sel, p_not]
    _write_csv(out_dir / "11_regime_selection_performance.csv", regime_rows)
    logger.info("[audit] 11 done")

    # ── File 12: Sector selection performance ──
    sector_rows: list[dict] = []
    for period, rows in [("train", sim_train), ("test", sim_test), ("all", simulated_all)]:
        by_sector: dict[str, list] = defaultdict(list)
        for r in rows:
            by_sector[str(r.get("sector") or "unknown")].append(r)
        for sector, group in sorted(by_sector.items()):
            selected = [r for r in group if r.get("selected_by_live_limited")]
            not_sel = [r for r in group if not r.get("selected_by_live_limited")]
            n = len(group)
            p = _perf(group, f"research_{sector}", period)
            p["sector"] = sector
            p["selected_count"] = len(selected)
            p["selected_rate"] = _round(len(selected) / n * 100 if n else None)
            p["selected_hd3_avg"] = _round(_avg([_to_float(r.get("hd3_ret_raw"), None) for r in selected if r.get("hd3_ret_raw") is not None]))
            p["not_selected_hd3_avg"] = _round(_avg([_to_float(r.get("hd3_ret_raw"), None) for r in not_sel if r.get("hd3_ret_raw") is not None]))
            sector_rows.append(p)
    _write_csv(out_dir / "12_sector_selection_performance.csv", sector_rows)
    logger.info("[audit] 12 done")

    # ── File 13: Final audit report ──
    # Compute key metrics for the report
    def _stats(rows: list[dict], col: str) -> str:
        vals = [_to_float(r.get(col), None) for r in rows if r.get(col) is not None]
        if not vals:
            return "N/A"
        return f"avg={_round(_avg(vals))}  WR={_round(_wr(vals))}%  PF={_round(_pf(vals))}"

    research_all_rows = simulated_all
    selected_all = [r for r in simulated_all if r.get("selected_by_live_limited")]
    not_selected_all = [r for r in simulated_all if not r.get("selected_by_live_limited")]
    pos_limited_all = [r for r in not_selected_all if r.get("live_skip_reason") in {"daily_limit", "open_position_limit", "sector_limit"}]
    rank_below_all = [r for r in not_selected_all if r.get("live_skip_reason") == "rank_limit"]

    # Sort variant results (all period, selected)
    def _variant_hd3(name: str) -> str:
        rows = [r for r in sort_rows if r.get("label") == name and r.get("period") == "all"]
        if not rows:
            return "N/A"
        r = rows[0]
        return f"n={r['n']}  HD3={r.get('hd3_raw_avg')}  WR={r.get('hd3_raw_wr')}%"

    # DB vs code comparison
    db_rules_parsed = {}
    if isinstance(db_rules_raw.get("rules"), dict):
        db_rules_parsed = db_rules_raw["rules"]
    elif isinstance(db_rules_raw.get("rules"), str):
        try:
            db_rules_parsed = json.loads(db_rules_raw["rules"])
        except Exception:
            db_rules_parsed = {}

    def _compare(key: str) -> str:
        code_val = H5_LIVE_LIMITED_RULES.get(key)
        db_val = db_rules_parsed.get(key)
        match = "✓ match" if str(code_val) == str(db_val) else f"✗ MISMATCH  code={code_val}  db={db_val}"
        return f"  {key}: {match}"

    check_keys = [
        "min_ai_score", "max_holding_days", "initial_sl_pct", "max_overheat_score",
        "min_margin_ratio", "max_margin_ratio", "position_limit_mode",
        "max_open_positions", "max_daily_entries", "max_sector_positions",
        "entry_rank_limit", "entry_sort", "is_primary_h5", "is_h5_live_limited",
    ]

    # Regime performance highlights
    regime_sel_perf: dict[str, dict] = {}
    for r in regime_rows:
        k = (str(r.get("market_regime") or ""), str(r.get("period") or ""))
        label = str(r.get("label") or "")
        if "selected_" in label:
            regime_sel_perf[k] = r

    report = [
        "H5 Live Limited Selection Audit Report",
        "=" * 70,
        f"Generated: {date.today()}",
        f"Period: {train_start} ~ {test_end}",
        "",
        "=" * 70,
        "1. Current Live Limited Rules (code: H5_LIVE_LIMITED_RULES)",
        "=" * 70,
        f"  case_key              : {H5_LIVE_LIMITED_CASE_KEY}",
        f"  min_ai_score          : {H5_LIVE_LIMITED_RULES.get('min_ai_score')}",
        f"  max_overheat_score    : {H5_LIVE_LIMITED_RULES.get('max_overheat_score')}",
        f"  min_margin_ratio      : {H5_LIVE_LIMITED_RULES.get('min_margin_ratio')}",
        f"  max_margin_ratio      : {H5_LIVE_LIMITED_RULES.get('max_margin_ratio')}",
        f"  max_holding_days      : {H5_LIVE_LIMITED_RULES.get('max_holding_days')}",
        f"  initial_sl_pct        : {H5_LIVE_LIMITED_RULES.get('initial_sl_pct')}",
        f"  max_open_positions    : {H5_LIVE_LIMITED_RULES.get('max_open_positions')}",
        f"  max_daily_entries     : {H5_LIVE_LIMITED_RULES.get('max_daily_entries')}",
        f"  max_sector_positions  : {H5_LIVE_LIMITED_RULES.get('max_sector_positions')}",
        f"  entry_rank_limit      : {H5_LIVE_LIMITED_RULES.get('entry_rank_limit')}",
        f"  entry_sort (raw)      : {H5_LIVE_LIMITED_RULES.get('entry_sort')!r}",
        "",
        "2. DB rules vs. Code constants",
        "-" * 40,
        f"  DB is_enabled: {db_rules_raw.get('is_enabled')}",
    ]
    for k in check_keys:
        report.append(_compare(k))

    report += [
        "",
        "3. CRITICAL: entry_sort discrepancy",
        "-" * 40,
        "  entry_sort in H5_LIVE_LIMITED_RULES is a LIST:",
        f"  {H5_LIVE_LIMITED_RULES.get('entry_sort')!r}",
        "  _sort_candidates() does: str(sort_key) == 'signal_probability_desc'",
        f"  str(list) = '{entry_sort_str}'  → comparison FAILS",
        "  → Actual sort applied: expected_value_desc (EV-based fallback)",
        "  → overheat_score_asc and volume_ratio_desc are NEVER applied",
        "  → entry_gap_pct_asc does NOT exist in H5_LIVE_LIMITED_RULES",
        "",
        "4. Research vs. Live Selected performance (ALL period)",
        "-" * 40,
        f"  Research (n={len(research_all_rows)}):   HD3 {_stats(research_all_rows, 'hd3_ret_raw')}",
        f"  Live selected (n={len(selected_all)}): HD3 {_stats(selected_all, 'hd3_ret_raw')}",
        f"  Not selected (n={len(not_selected_all)}): HD3 {_stats(not_selected_all, 'hd3_ret_raw')}",
        f"  Position-limited (n={len(pos_limited_all)}): HD3 {_stats(pos_limited_all, 'hd3_ret_raw')}",
        f"  Rank<10 but not selected (n={len(rank_below_all)}): (should be 0 if rank_limit=10 used)",
        "",
        f"  Live selected HD5: {_stats(selected_all, 'hd5_ret_raw')}",
        f"  Live selected HD7: {_stats(selected_all, 'hd7_ret_raw')}",
        f"  Research HD5:      {_stats(research_all_rows, 'hd5_ret_raw')}",
        f"  Research HD7:      {_stats(research_all_rows, 'hd7_ret_raw')}",
        "",
        "5. Live Selected vs. Not Selected",
        "-" * 40,
        f"  Live > Research? HD3: {(_avg([_to_float(r.get('hd3_ret_raw'), None) for r in selected_all if r.get('hd3_ret_raw') is not None]) or 0) >= (_avg([_to_float(r.get('hd3_ret_raw'), None) for r in research_all_rows if r.get('hd3_ret_raw') is not None]) or 0)}",
        f"  Live > NotSelected? HD3: {(_avg([_to_float(r.get('hd3_ret_raw'), None) for r in selected_all if r.get('hd3_ret_raw') is not None]) or 0) >= (_avg([_to_float(r.get('hd3_ret_raw'), None) for r in not_selected_all if r.get('hd3_ret_raw') is not None]) or 0)}",
        "",
        "6. Sort variant comparison (ALL period, Live selected subset)",
        "-" * 40,
        f"  current_ev_desc     : {_variant_hd3('current_ev_desc')}",
        f"  prob_desc           : {_variant_hd3('prob_desc')}",
        f"  low_volume_asc      : {_variant_hd3('low_volume_asc')}",
        f"  moderate_volume     : {_variant_hd3('moderate_volume')}",
        f"  no_volume           : {_variant_hd3('no_volume')}",
        f"  drop_deep           : {_variant_hd3('drop_deep')}",
        f"  random_seed42       : {_variant_hd3('random_seed42')}",
        f"  random_seed0        : {_variant_hd3('random_seed0')}",
        f"  random_seed99       : {_variant_hd3('random_seed99')}",
        "",
        "7. Market regime: panic_rebound coverage",
        "-" * 40,
    ]
    # Find panic_rebound rows
    panic_all = [r for r in simulated_all if r.get("market_regime") == "panic_rebound"]
    panic_sel = [r for r in panic_all if r.get("selected_by_live_limited")]
    normal_all = [r for r in simulated_all if r.get("market_regime") == "normal"]
    normal_sel = [r for r in normal_all if r.get("selected_by_live_limited")]
    report += [
        f"  panic_rebound total: {len(panic_all)}  selected: {len(panic_sel)} ({len(panic_sel)/max(1,len(panic_all))*100:.1f}%)",
        f"    Research HD3: {_stats(panic_all, 'hd3_ret_raw')}",
        f"    Selected HD3: {_stats(panic_sel, 'hd3_ret_raw')}",
        f"  normal total: {len(normal_all)}  selected: {len(normal_sel)} ({len(normal_sel)/max(1,len(normal_all))*100:.1f}%)",
        f"    Research HD3: {_stats(normal_all, 'hd3_ret_raw')}",
        f"    Selected HD3: {_stats(normal_sel, 'hd3_ret_raw')}",
        "",
        "8. Skip reason breakdown (ALL)",
        "-" * 40,
    ]
    for sr in ["", "daily_limit", "open_position_limit", "sector_limit", "rank_limit"]:
        group = [r for r in simulated_all if r.get("live_skip_reason") == sr]
        label = "selected" if sr == "" else sr
        report.append(f"  {label:30s}: n={len(group):4d}  HD3 {_stats(group, 'hd3_ret_raw')}")

    report += [
        "",
        "9. Q&A Summary (see spec sections 16)",
        "-" * 40,
        "  Q1.  Current Live Limited rules: see section 1 above.",
        f"  Q2.  DB vs code: see section 2. entry_sort MISMATCH (list vs string).",
        "  Q3.  entry_sort actual: expected_value_desc (EV fallback) — NOT signal_probability_desc.",
        "  Q4.  Position limit apply order: rank_limit → daily_limit → open_limit → sector_limit.",
        "  Q5-7. See section 4 (Research/Live/NotSelected comparison).",
        "  Q8-10. See 06_rank_bucket_performance.csv.",
        "  Q11-14. See 07_entry_sort_factor_buckets.csv.",
        "  Q15-18. See section 6 (sort variant comparison) and 08_sort_variant_comparison.csv.",
        "  Q19-22. See 09_position_limit_sensitivity.csv.",
        "  Q23. See section 8 (skip reason).",
        "  Q24-25. See section 7 (regime).",
        "  Q26. See 12_sector_selection_performance.csv.",
        "  Q27-30. See conclusion below.",
        "",
        "10. Conclusion",
        "-" * 40,
        "  CRITICAL: entry_sort is a list stored in H5_LIVE_LIMITED_RULES, but",
        "  _sort_candidates() compares str(list) != 'signal_probability_desc',",
        "  causing the EV fallback sort to always be applied. This is the primary",
        "  implementation discrepancy to investigate.",
        "",
        "  Whether EV sort vs prob_desc sort produces better or worse results",
        "  is quantified in 08_sort_variant_comparison.csv.",
        "",
        "  Primary case (h5_ai65_hd3_est12_cm_range330_live_limited) NOT changed.",
        "  Live Limited rules NOT changed. DB definitions NOT changed.",
        "  This script is research/audit only.",
        "",
        "Output files:",
        "  01_live_limited_db_rules.json",
        "  02_h5_primary_constants.txt",
        "  03_selection_flow_report.txt",
        "  04_live_selection_dataset.csv",
        "  05_selected_vs_not_selected.csv",
        "  06_rank_bucket_performance.csv",
        "  07_entry_sort_factor_buckets.csv",
        "  08_sort_variant_comparison.csv",
        "  09_position_limit_sensitivity.csv",
        "  10_skip_reason_performance.csv",
        "  11_regime_selection_performance.csv",
        "  12_sector_selection_performance.csv",
        "  13_live_selection_audit_report.txt",
    ]

    (out_dir / "13_live_selection_audit_report.txt").write_text("\n".join(report), encoding="utf-8")
    logger.info("[audit] 13 done")
    logger.info("[audit] ALL DONE. Output: %s", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="H5 Live Limited Selection Audit")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="2026-05-28")
    parser.add_argument("--output-dir", default="outputs/h5_live_selection_audit")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
