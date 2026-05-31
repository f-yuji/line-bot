"""H5 HD3 Edge Anatomy.

Research-only script. Does not modify DB, case definitions, or any live code.

Analyzes WHY the 3-trading-day (HD3) exit is strong for H5 Primary entries by
studying rebound lifecycle across day1–day10 return paths.

Populations:
  Research   : all candidates passing H5 entry conditions
  Live Limited: rank-filtered (max 2 entries/day, max 2 open positions)

Usage:
    python scripts/analyze_h5_hd3_edge_anatomy.py
    python scripts/analyze_h5_hd3_edge_anatomy.py --train-end 2024-12-31 --test-start 2025-01-01
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

from services.h5_primary import h5_overheat_score
from services.trade_case_tester import _build_supabase, _fetch_all, _load_candidates_v2, _to_float

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MAX_HOLD = 10
EST12_STOP = -0.12  # -12%

# Live Limited position rules
LIVE_MAX_DAILY = 2
LIVE_MAX_OPEN = 2


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
    if len(vals) % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


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


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


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
# Price path computation
# ──────────────────────────────────────────────

def _build_day_path(row: dict, entry: float) -> list[dict]:
    """Build day1..MAX_HOLD close/high/low from future_*_Nd labels."""
    path = []
    for day in range(1, MAX_HOLD + 1):
        close = _to_float(row.get(f"future_close_{day}d"), None)
        high = _to_float(row.get(f"future_high_{day}d"), None)
        low = _to_float(row.get(f"future_low_{day}d"), None)
        prev = entry if day == 1 else (_to_float(row.get(f"future_close_{day - 1}d"), None))
        path.append({
            "day": day,
            "close": close,
            "high": high,
            "low": low,
            "open": prev,
        })
    return path


def _raw_ret(path: list[dict], entry: float, hold: int) -> float | None:
    """Return from entry to close of hold_day (no stop)."""
    if hold < 1 or hold > len(path):
        return None
    c = path[hold - 1].get("close")
    if c is None:
        return None
    return (c / entry - 1.0) * 100.0


def _est12_ret(path: list[dict], entry: float, hold: int) -> dict:
    """EST12-applied return up to hold_day. Emergency stop at -12%."""
    stop_price = entry * (1.0 + EST12_STOP)
    last_close = None
    last_day = 0
    for i, d in enumerate(path[:hold], start=1):
        low = d.get("low")
        close = d.get("close")
        if close is not None:
            last_close = close
            last_day = i
        if low is not None and low <= stop_price:
            return {"ret": EST12_STOP * 100.0, "exit_day": i, "exit_reason": "emergency_stop"}
    if last_close is None:
        return {"ret": None, "exit_day": None, "exit_reason": "no_data"}
    return {"ret": (last_close / entry - 1.0) * 100.0, "exit_day": last_day, "exit_reason": "time_stop"}


# ──────────────────────────────────────────────
# Build dataset
# ──────────────────────────────────────────────

def _bucket_day1(d1: float | None) -> str:
    if d1 is None:
        return "null"
    if d1 >= -1.0:
        return "gte-1"
    if d1 >= -2.0:
        return "-2_-1"
    if d1 >= -3.0:
        return "-3_-2"
    if d1 >= -4.0:
        return "-4_-3"
    if d1 >= -5.0:
        return "-5_-4"
    return "lt-5"


def _bucket_day3(d3: float | None) -> str:
    if d3 is None:
        return "null"
    if d3 >= 3.0:
        return "gte+3"
    if d3 >= 0.0:
        return "0_+3"
    if d3 >= -1.0:
        return "-1_0"
    if d3 >= -3.0:
        return "-3_-1"
    if d3 >= -5.0:
        return "-5_-3"
    return "lt-5"


def _bucket_vol(v: float | None) -> str:
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
    return "gte2.0"


def _lifecycle_class(path: list[dict], entry: float, hd3_ret: float | None) -> str:
    """Classify rebound lifecycle based on day3/day5/day10 returns (Raw)."""
    if hd3_ret is None:
        return "unknown"
    # Check if returned to entry by day3
    hd3_positive = hd3_ret >= 0.0
    # day5 raw
    d5r = _raw_ret(path, entry, 5)
    # day10 raw
    d10r = _raw_ret(path, entry, 10)

    if hd3_positive:
        return "early_finished"  # recovered by day3
    # day3 still negative
    if d5r is not None and d5r >= 0.0:
        return "delayed"  # recovered by day5
    if d10r is not None and d10r >= 0.0:
        return "trend_reversal"  # recovered by day10
    # Check for dead cat: day3 down, brief recovery then more drop
    if d5r is not None and d10r is not None and d5r > hd3_ret and d10r < hd3_ret:
        return "dead_cat"
    return "failed"  # still down at day10


def _build_dataset(candidates: list[dict]) -> list[dict]:
    dataset = []
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry or entry <= 0:
            continue

        path = _build_day_path(row, entry)

        # Check HD3 and HD5 have data (for extension-related analysis)
        hd3_result = _est12_ret(path, entry, 3)
        hd3_ret = hd3_result["ret"]
        if hd3_ret is None:
            continue

        hd1r = _raw_ret(path, entry, 1)
        hd3r_raw = _raw_ret(path, entry, 3)
        day1_daily = hd1r  # cumulative = daily for day1

        day1_vol = _to_float(row.get("volume_ratio_20d"), None)
        entry_rsi = _to_float(row.get("rsi14"), None)

        # Raw returns day1..day10
        raw_rets = {}
        for d in range(1, MAX_HOLD + 1):
            raw_rets[d] = _raw_ret(path, entry, d)

        # EST12 applied returns day1..day10
        est_rets = {}
        for d in range(1, MAX_HOLD + 1):
            r = _est12_ret(path, entry, d)
            est_rets[d] = r["ret"]

        # Peak day: day with highest high over day1..day10
        highs = [(d["day"], d.get("high")) for d in path if d.get("high") is not None]
        if highs:
            peak_day = max(highs, key=lambda x: x[1])[0]
            peak_high = max(h for _, h in highs)
            peak_ret = _pct(peak_high, entry)
        else:
            peak_day = None
            peak_high = None
            peak_ret = None

        # Lifecycle
        lifecycle = _lifecycle_class(path, entry, hd3r_raw)

        # Extension-enabled check
        is_ext_enabled = hd3r_raw is not None and hd3r_raw <= -1.0
        # benefit 3→5, 3→7, 3→10
        benefit_3_5 = (
            (raw_rets.get(5) - hd3r_raw)
            if (raw_rets.get(5) is not None and hd3r_raw is not None)
            else None
        )
        benefit_3_7 = (
            (raw_rets.get(7) - hd3r_raw)
            if (raw_rets.get(7) is not None and hd3r_raw is not None)
            else None
        )
        benefit_3_10 = (
            (raw_rets.get(10) - hd3r_raw)
            if (raw_rets.get(10) is not None and hd3r_raw is not None)
            else None
        )

        # EST12 results for HD1,2,3,4,5,7,10
        est_results = {}
        for hold in [1, 2, 3, 4, 5, 7, 10]:
            r = _est12_ret(path, entry, hold)
            est_results[hold] = r

        market_regime = str(row.get("market_regime") or "")
        sector = str(row.get("sector") or "")
        signal_prob = _to_float(row.get("signal_probability"), None)

        rec: dict = {
            "entry_date": str(row.get("trade_date") or ""),
            "code": str(row.get("code") or ""),
            "name": row.get("name"),
            "sector": sector,
            "market_regime": market_regime,
            "entry_price": entry,
            "signal_probability": signal_prob,
            "signal_stage": row.get("signal_stage"),
            "entry_rsi": entry_rsi,
            "margin_ratio": _to_float(row.get("margin_ratio"), None),
            "entry_volume_ratio": day1_vol,
            # Day1 return (cumulative from entry)
            "day1_ret_raw": hd1r,
            "day1_ret_bucket": _bucket_day1(hd1r),
            # HD3
            "hd3_ret_raw": hd3r_raw,
            "hd3_ret_est12": hd3_ret,
            "hd3_est12_exit_day": hd3_result.get("exit_day"),
            "hd3_est12_exit_reason": hd3_result.get("exit_reason"),
            "day3_ret_bucket": _bucket_day3(hd3r_raw),
            # Lifecycle
            "lifecycle": lifecycle,
            "is_ext_enabled": is_ext_enabled,
            # Peak
            "peak_day": peak_day,
            "peak_ret_raw": _round(peak_ret),
            # Day3-to-future benefits (raw)
            "benefit_3_to_5": _round(benefit_3_5),
            "benefit_3_to_7": _round(benefit_3_7),
            "benefit_3_to_10": _round(benefit_3_10),
            # Volume (entry)
            "entry_volume_ratio_bucket": _bucket_vol(day1_vol),
        }

        # Raw return path columns
        for d in range(1, MAX_HOLD + 1):
            rec[f"raw_ret_d{d}"] = _round(raw_rets.get(d))

        # EST12 applied path columns
        for d in range(1, MAX_HOLD + 1):
            rec[f"est12_ret_d{d}"] = _round(est_rets.get(d))

        # Individual EST12 exit info (useful for HD analysis)
        for hold in [1, 2, 3, 4, 5, 7, 10]:
            r = est_results[hold]
            rec[f"hd{hold}_ret_raw"] = _round(raw_rets.get(hold))
            rec[f"hd{hold}_ret_est12"] = _round(r.get("ret"))
            rec[f"hd{hold}_exit_day"] = r.get("exit_day")
            rec[f"hd{hold}_exit_reason"] = r.get("exit_reason")

        dataset.append(rec)
    return dataset


# ──────────────────────────────────────────────
# Live Limited filter (rank-based position limit)
# ──────────────────────────────────────────────

def _apply_live_limited(rows: list[dict]) -> list[dict]:
    """Apply max_daily_entries=2, max_open_positions=2 rank filter.

    Within each entry_date, sort by signal_probability descending.
    Respect open position limit: a position is "open" from entry_date
    through HD3 exit day (entry + 3 trading days). Since we don't have
    actual trading-day calendars here, we approximate by counting each
    entry as open for 3 calendar days.
    """
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_date[row["entry_date"]].append(row)

    open_positions: list[date] = []  # list of expiry dates
    selected: list[dict] = []

    for dt_str in sorted(by_date.keys()):
        today = _d(dt_str)
        # Remove expired positions (expiry < today)
        open_positions = [exp for exp in open_positions if exp >= today]

        day_candidates = sorted(
            by_date[dt_str],
            key=lambda r: _to_float(r.get("signal_probability"), 0.0) or 0.0,
            reverse=True,
        )
        entries_today = 0
        for cand in day_candidates:
            if entries_today >= LIVE_MAX_DAILY:
                break
            if len(open_positions) >= LIVE_MAX_OPEN:
                break
            selected.append(cand)
            entries_today += 1
            # Position expires after 3 trading days ≈ 5 calendar days
            open_positions.append(today + timedelta(days=5))

    return selected


# ──────────────────────────────────────────────
# Period split
# ──────────────────────────────────────────────

def _split(rows: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    train = [r for r in rows if _d(r["entry_date"]) <= train_end]
    test = [r for r in rows if _d(r["entry_date"]) > train_end]
    return train, test


# ──────────────────────────────────────────────
# Return curve statistics
# ──────────────────────────────────────────────

def _return_curve(rows: list[dict], col_prefix: str, label: str) -> list[dict]:
    """Compute avg/median/WR/PF per day for raw or EST12 path."""
    out = []
    for d in range(1, MAX_HOLD + 1):
        col = f"{col_prefix}_d{d}"
        vals = [_to_float(r.get(col), None) for r in rows]
        vals = [v for v in vals if v is not None]
        out.append({
            "label": label,
            "day": d,
            "n": len(vals),
            "avg_ret": _round(_avg(vals)),
            "median_ret": _round(_median(vals)),
            "win_rate": _round(_wr(vals)),
            "pf": _round(_pf(vals)),
            "pct_above_hd3": None,  # filled below
        })
    # Fill pct_above_hd3
    hd3_col = f"{col_prefix}_d3"
    for rec in out:
        d = rec["day"]
        if d <= 3:
            rec["pct_above_hd3"] = None
            continue
        col = f"{col_prefix}_d{d}"
        paired = [
            (r.get(col), r.get(hd3_col))
            for r in rows
            if r.get(col) is not None and r.get(hd3_col) is not None
        ]
        if paired:
            rec["pct_above_hd3"] = _round(sum(1 for c, h in paired if c > h) / len(paired) * 100)
    return out


# ──────────────────────────────────────────────
# Fixed holding day comparison
# ──────────────────────────────────────────────

def _holding_day_comparison(rows: list[dict], period: str) -> list[dict]:
    out = []
    for hold in [1, 2, 3, 4, 5, 7, 10]:
        raw_vals = [_to_float(r.get(f"hd{hold}_ret_raw"), None) for r in rows]
        raw_vals = [v for v in raw_vals if v is not None]
        est_vals = [_to_float(r.get(f"hd{hold}_ret_est12"), None) for r in rows]
        est_vals = [v for v in est_vals if v is not None]
        est_stop_rate = None
        if rows:
            n_stop = sum(1 for r in rows if r.get(f"hd{hold}_exit_reason") == "emergency_stop")
            est_stop_rate = _round(n_stop / len(rows) * 100)
        out.append({
            "period": period,
            "hold_days": hold,
            "n_raw": len(raw_vals),
            "raw_avg_ret": _round(_avg(raw_vals)),
            "raw_median_ret": _round(_median(raw_vals)),
            "raw_win_rate": _round(_wr(raw_vals)),
            "raw_pf": _round(_pf(raw_vals)),
            "n_est12": len(est_vals),
            "est12_avg_ret": _round(_avg(est_vals)),
            "est12_median_ret": _round(_median(est_vals)),
            "est12_win_rate": _round(_wr(est_vals)),
            "est12_pf": _round(_pf(est_vals)),
            "est12_stop_rate": est_stop_rate,
        })
    return out


# ──────────────────────────────────────────────
# Bucket analysis
# ──────────────────────────────────────────────

def _bucket_analysis(
    rows: list[dict],
    bucket_key: str,
    period: str,
    val_cols: list[str] | None = None,
) -> list[dict]:
    """For each bucket value, compute HD1/HD3/HD5/HD10 raw stats + lifecycle dist."""
    if val_cols is None:
        val_cols = ["hd1_ret_raw", "hd3_ret_raw", "hd5_ret_raw", "hd10_ret_raw"]

    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        b = str(r.get(bucket_key) or "null")
        buckets[b].append(r)

    out = []
    for b_val, group in sorted(buckets.items()):
        rec: dict = {"period": period, bucket_key: b_val, "n": len(group)}
        for col in val_cols:
            vals = [_to_float(r.get(col), None) for r in group]
            vals = [v for v in vals if v is not None]
            rec[f"{col}_avg"] = _round(_avg(vals))
            rec[f"{col}_wr"] = _round(_wr(vals))

        # Lifecycle distribution
        lifecycle_counts: dict[str, int] = defaultdict(int)
        for r in group:
            lifecycle_counts[r.get("lifecycle") or "unknown"] += 1
        total = len(group)
        for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed", "unknown"]:
            rec[f"lifecycle_{lc}_pct"] = _round(lifecycle_counts.get(lc, 0) / total * 100 if total else None)

        out.append(rec)
    return out


# ──────────────────────────────────────────────
# Peak day distribution
# ──────────────────────────────────────────────

def _peak_day_dist(rows: list[dict], period: str) -> list[dict]:
    counts: dict[int | str, int] = defaultdict(int)
    total = 0
    for r in rows:
        pd_ = r.get("peak_day")
        if pd_ is not None:
            counts[int(pd_)] += 1
            total += 1
        else:
            counts["null"] += 1
    out = []
    for k in sorted([k for k in counts if k != "null"]) + (["null"] if "null" in counts else []):
        out.append({
            "period": period,
            "peak_day": k,
            "count": counts[k],
            "rate": _round(counts[k] / (total or 1) * 100),
        })
    return out


# ──────────────────────────────────────────────
# Day3-bucket analysis (benefit_3_to_X by day3 state)
# ──────────────────────────────────────────────

def _day3_bucket_benefit(rows: list[dict], period: str) -> list[dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        b = str(r.get("day3_ret_bucket") or "null")
        buckets[b].append(r)

    bucket_order = ["gte+3", "0_+3", "-1_0", "-3_-1", "-5_-3", "lt-5", "null"]
    out = []
    for b_val in bucket_order:
        group = buckets.get(b_val, [])
        n = len(group)
        if n == 0:
            continue
        b35 = [_to_float(r.get("benefit_3_to_5"), None) for r in group]
        b37 = [_to_float(r.get("benefit_3_to_7"), None) for r in group]
        b310 = [_to_float(r.get("benefit_3_to_10"), None) for r in group]
        b35 = [v for v in b35 if v is not None]
        b37 = [v for v in b37 if v is not None]
        b310 = [v for v in b310 if v is not None]

        lifecycle_counts: dict[str, int] = defaultdict(int)
        for r in group:
            lifecycle_counts[r.get("lifecycle") or "unknown"] += 1

        out.append({
            "period": period,
            "day3_ret_bucket": b_val,
            "n": n,
            "benefit_3_to_5_avg": _round(_avg(b35)),
            "benefit_3_to_5_wr": _round(_wr(b35)),
            "benefit_3_to_7_avg": _round(_avg(b37)),
            "benefit_3_to_7_wr": _round(_wr(b37)),
            "benefit_3_to_10_avg": _round(_avg(b310)),
            "benefit_3_to_10_wr": _round(_wr(b310)),
            **{
                f"lifecycle_{lc}_pct": _round(lifecycle_counts.get(lc, 0) / n * 100)
                for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed"]
            },
        })
    return out


# ──────────────────────────────────────────────
# Lifecycle analysis by dimension
# ──────────────────────────────────────────────

def _lifecycle_by_dim(rows: list[dict], dim: str, period: str) -> list[dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        b = str(r.get(dim) or "null")
        buckets[b].append(r)

    out = []
    for b_val, group in sorted(buckets.items()):
        n = len(group)
        lc_counts: dict[str, int] = defaultdict(int)
        for r in group:
            lc_counts[r.get("lifecycle") or "unknown"] += 1
        hd3 = [_to_float(r.get("hd3_ret_raw"), None) for r in group]
        hd3 = [v for v in hd3 if v is not None]
        hd5 = [_to_float(r.get("hd5_ret_raw"), None) for r in group]
        hd5 = [v for v in hd5 if v is not None]
        hd3est = [_to_float(r.get("hd3_ret_est12"), None) for r in group]
        hd3est = [v for v in hd3est if v is not None]

        rec: dict = {
            "period": period,
            dim: b_val,
            "n": n,
            "hd3_raw_avg": _round(_avg(hd3)),
            "hd3_raw_wr": _round(_wr(hd3)),
            "hd5_raw_avg": _round(_avg(hd5)),
            "hd5_raw_wr": _round(_wr(hd5)),
            "hd3_est12_avg": _round(_avg(hd3est)),
            "hd3_est12_wr": _round(_wr(hd3est)),
        }
        total = n
        for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed", "unknown"]:
            rec[f"lifecycle_{lc}_pct"] = _round(lc_counts.get(lc, 0) / total * 100 if total else None)
        out.append(rec)
    return out


# ──────────────────────────────────────────────
# Summary stats helper
# ──────────────────────────────────────────────

def _summary_stats(rows: list[dict], label: str) -> dict:
    hd3 = [_to_float(r.get("hd3_ret_raw"), None) for r in rows]
    hd3 = [v for v in hd3 if v is not None]
    hd3est = [_to_float(r.get("hd3_ret_est12"), None) for r in rows]
    hd3est = [v for v in hd3est if v is not None]
    hd5 = [_to_float(r.get("hd5_ret_raw"), None) for r in rows]
    hd5 = [v for v in hd5 if v is not None]
    hd10 = [_to_float(r.get("hd10_ret_raw"), None) for r in rows]
    hd10 = [v for v in hd10 if v is not None]

    lc_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        lc_counts[r.get("lifecycle") or "unknown"] += 1
    n = len(rows)

    return {
        "label": label,
        "n": n,
        "hd3_raw_avg": _round(_avg(hd3)),
        "hd3_raw_wr": _round(_wr(hd3)),
        "hd3_raw_pf": _round(_pf(hd3)),
        "hd3_est12_avg": _round(_avg(hd3est)),
        "hd3_est12_wr": _round(_wr(hd3est)),
        "hd3_est12_pf": _round(_pf(hd3est)),
        "hd5_raw_avg": _round(_avg(hd5)),
        "hd5_raw_wr": _round(_wr(hd5)),
        "hd10_raw_avg": _round(_avg(hd10)),
        "hd10_raw_wr": _round(_wr(hd10)),
        **{
            f"lifecycle_{lc}_pct": _round(lc_counts.get(lc, 0) / n * 100 if n else None)
            for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed", "unknown"]
        },
    }


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
    logger.info("[hd3_anatomy] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[hd3_anatomy] raw candidates=%d", len(candidates))

    # ── Build Research population ──
    research_all = _build_dataset(candidates)
    logger.info("[hd3_anatomy] research dataset rows=%d", len(research_all))

    # ── Build Live Limited population ──
    live_all = _apply_live_limited(research_all)
    logger.info("[hd3_anatomy] live_limited dataset rows=%d", len(live_all))

    # Period splits
    research_train, research_test = _split(research_all, train_end)
    live_train, live_test = _split(live_all, train_end)

    date_tag = f"Train: {train_start} ~ {train_end}  Test: {test_start} ~ {test_end}"

    # ── File 01: Dataset summary ──
    summary_rows = []
    for label, rows in [
        ("research_train", research_train),
        ("research_test", research_test),
        ("research_all", research_all),
        ("live_train", live_train),
        ("live_test", live_test),
        ("live_all", live_all),
    ]:
        summary_rows.append(_summary_stats(rows, label))
    _write_csv(out_dir / "01_dataset_summary.csv", summary_rows)
    logger.info("[hd3_anatomy] 01 done")

    # ── File 02: Raw return curve (Research) ──
    curve_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        for rec in _return_curve(rows, "raw_ret", f"research_{period}"):
            rec["period"] = period
            curve_rows.append(rec)
    _write_csv(out_dir / "02_raw_return_curve_research.csv", curve_rows)
    logger.info("[hd3_anatomy] 02 done")

    # ── File 03: EST12 return curve (Research) ──
    est_curve_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        for rec in _return_curve(rows, "est12_ret", f"research_{period}"):
            rec["period"] = period
            est_curve_rows.append(rec)
    _write_csv(out_dir / "03_est12_return_curve_research.csv", est_curve_rows)
    logger.info("[hd3_anatomy] 03 done")

    # ── File 04: Raw return curve (Live Limited) ──
    live_curve_rows: list[dict] = []
    for period, rows in [("train", live_train), ("test", live_test), ("all", live_all)]:
        for rec in _return_curve(rows, "raw_ret", f"live_{period}"):
            rec["period"] = period
            live_curve_rows.append(rec)
    _write_csv(out_dir / "04_raw_return_curve_live.csv", live_curve_rows)
    logger.info("[hd3_anatomy] 04 done")

    # ── File 05: Peak day distribution ──
    peak_rows: list[dict] = []
    for period, rows in [
        ("research_train", research_train),
        ("research_test", research_test),
        ("research_all", research_all),
        ("live_train", live_train),
        ("live_test", live_test),
        ("live_all", live_all),
    ]:
        peak_rows.extend(_peak_day_dist(rows, period))
    _write_csv(out_dir / "05_peak_day_distribution.csv", peak_rows)
    logger.info("[hd3_anatomy] 05 done")

    # ── File 06: Day3-to-future benefit analysis ──
    d3b_rows: list[dict] = []
    for period, rows in [
        ("research_train", research_train),
        ("research_test", research_test),
        ("research_all", research_all),
    ]:
        d3b_rows.extend(_day3_bucket_benefit(rows, period))
    _write_csv(out_dir / "06_day3_bucket_benefit.csv", d3b_rows)
    logger.info("[hd3_anatomy] 06 done")

    # ── File 07: Fixed holding day comparison (Research) ──
    hold_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        hold_rows.extend(_holding_day_comparison(rows, f"research_{period}"))
    _write_csv(out_dir / "07_holding_day_comparison_research.csv", hold_rows)
    logger.info("[hd3_anatomy] 07 done")

    # ── File 08: Fixed holding day comparison (Live Limited) ──
    live_hold_rows: list[dict] = []
    for period, rows in [("train", live_train), ("test", live_test), ("all", live_all)]:
        live_hold_rows.extend(_holding_day_comparison(rows, f"live_{period}"))
    _write_csv(out_dir / "08_holding_day_comparison_live.csv", live_hold_rows)
    logger.info("[hd3_anatomy] 08 done")

    # ── File 09: Market regime lifecycle ──
    regime_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        regime_rows.extend(_lifecycle_by_dim(rows, "market_regime", period))
    _write_csv(out_dir / "09_market_regime_lifecycle.csv", regime_rows)
    logger.info("[hd3_anatomy] 09 done")

    # ── File 10: Sector lifecycle ──
    sector_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        sector_rows.extend(_lifecycle_by_dim(rows, "sector", period))
    _write_csv(out_dir / "10_sector_lifecycle.csv", sector_rows)
    logger.info("[hd3_anatomy] 10 done")

    # ── File 11: Volume bucket lifecycle ──
    vol_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        vol_rows.extend(_bucket_analysis(
            rows,
            "entry_volume_ratio_bucket",
            period,
            val_cols=["hd1_ret_raw", "hd3_ret_raw", "hd5_ret_raw", "hd10_ret_raw"],
        ))
    _write_csv(out_dir / "11_volume_bucket_lifecycle.csv", vol_rows)
    logger.info("[hd3_anatomy] 11 done")

    # ── File 12: Day1_return bucket lifecycle ──
    d1_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        d1_rows.extend(_bucket_analysis(
            rows,
            "day1_ret_bucket",
            period,
            val_cols=["hd1_ret_raw", "hd3_ret_raw", "hd5_ret_raw", "hd10_ret_raw"],
        ))
    _write_csv(out_dir / "12_day1_bucket_lifecycle.csv", d1_rows)
    logger.info("[hd3_anatomy] 12 done")

    # ── File 13: Day3 bucket lifecycle ──
    d3lc_rows: list[dict] = []
    for period, rows in [("train", research_train), ("test", research_test), ("all", research_all)]:
        d3lc_rows.extend(_bucket_analysis(
            rows,
            "day3_ret_bucket",
            period,
            val_cols=["hd3_ret_raw", "hd5_ret_raw", "hd7_ret_raw", "hd10_ret_raw"],
        ))
    _write_csv(out_dir / "13_day3_bucket_lifecycle.csv", d3lc_rows)
    logger.info("[hd3_anatomy] 13 done")

    # ── File 14: Extension-enabled subgroup analysis ──
    ext_rows_r = [r for r in research_all if r.get("is_ext_enabled")]
    ext_rows_l = [r for r in live_all if r.get("is_ext_enabled")]
    ext_out: list[dict] = []
    for label, rows in [
        ("research_train_ext", [r for r in research_train if r.get("is_ext_enabled")]),
        ("research_test_ext", [r for r in research_test if r.get("is_ext_enabled")]),
        ("research_all_ext", ext_rows_r),
        ("live_train_ext", [r for r in live_train if r.get("is_ext_enabled")]),
        ("live_test_ext", [r for r in live_test if r.get("is_ext_enabled")]),
        ("live_all_ext", ext_rows_l),
    ]:
        ext_out.append(_summary_stats(rows, label))
    _write_csv(out_dir / "14_extension_enabled_subgroup.csv", ext_out)
    logger.info("[hd3_anatomy] 14 done")

    # ── File 15: All individual records (research) ──
    _write_csv(out_dir / "15_research_all_records.csv", research_all)
    logger.info("[hd3_anatomy] 15 done")

    # ── File 16: Summary report (TXT) ──
    def _curve_line(curve: list[dict], day: int) -> str:
        rec = next((r for r in curve if r.get("day") == day), None)
        if not rec:
            return "  N/A"
        return (
            f"  HD{day}: n={rec.get('n')}"
            f"  avg={rec.get('avg_ret')}"
            f"  median={rec.get('median_ret')}"
            f"  WR={rec.get('win_rate')}%"
            f"  PF={rec.get('pf')}"
        )

    def _hold_line(hold_rows_list: list[dict], hold: int, period_prefix: str) -> str:
        recs = [r for r in hold_rows_list if r.get("hold_days") == hold and str(r.get("period") or "").startswith(period_prefix)]
        if not recs:
            return f"  HD{hold}: N/A"
        rec = recs[0]
        return (
            f"  HD{hold}: raw_avg={rec.get('raw_avg_ret')}"
            f"  raw_WR={rec.get('raw_win_rate')}%"
            f"  est12_avg={rec.get('est12_avg_ret')}"
            f"  est12_WR={rec.get('est12_win_rate')}%"
            f"  stop_rate={rec.get('est12_stop_rate')}%"
        )

    # Rebuild curves for report
    research_all_curve_raw = _return_curve(research_all, "raw_ret", "research_all")
    research_all_curve_est = _return_curve(research_all, "est12_ret", "research_all")
    live_all_curve_raw = _return_curve(live_all, "raw_ret", "live_all")

    lifecycle_dist_research: dict[str, int] = defaultdict(int)
    for r in research_all:
        lifecycle_dist_research[r.get("lifecycle") or "unknown"] += 1
    lifecycle_dist_live: dict[str, int] = defaultdict(int)
    for r in live_all:
        lifecycle_dist_live[r.get("lifecycle") or "unknown"] += 1

    def _lc_pct(d: dict[str, int], key: str) -> str:
        total = sum(d.values())
        n = d.get(key, 0)
        return f"{key}: {n} ({n/total*100:.1f}%)" if total else f"{key}: 0"

    report_lines = [
        "H5 HD3 Edge Anatomy Report",
        "=" * 60,
        f"Generated: {date.today()}",
        date_tag,
        "",
        "1. Dataset Size",
        f"   Research: train={len(research_train)}  test={len(research_test)}  all={len(research_all)}",
        f"   Live Limited: train={len(live_train)}  test={len(live_test)}  all={len(live_all)}",
        f"   Extension-enabled (research_all): {len(ext_rows_r)} / {len(research_all)}",
        f"   Extension-enabled (live_all): {len(ext_rows_l)} / {len(live_all)}",
        "",
        "2. Raw Return Curve (Research ALL)",
    ]
    for d in [1, 2, 3, 4, 5, 7, 10]:
        report_lines.append(_curve_line(research_all_curve_raw, d))

    report_lines += [
        "",
        "3. EST12 Return Curve (Research ALL)",
    ]
    for d in [1, 2, 3, 4, 5, 7, 10]:
        report_lines.append(_curve_line(research_all_curve_est, d))

    report_lines += [
        "",
        "4. Raw Return Curve (Live Limited ALL)",
    ]
    for d in [1, 2, 3, 4, 5, 7, 10]:
        report_lines.append(_curve_line(live_all_curve_raw, d))

    # Peak day
    peak_dist_all = _peak_day_dist(research_all, "research_all")
    peak_dist_live = _peak_day_dist(live_all, "live_all")
    report_lines += [
        "",
        "5. Peak Day Distribution (Research ALL)",
    ]
    for rec in peak_dist_all:
        report_lines.append(f"  Day {rec['peak_day']}: {rec['count']} ({rec['rate']}%)")

    report_lines += [
        "",
        "6. Lifecycle Distribution (Research ALL)",
    ]
    for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed", "unknown"]:
        report_lines.append(f"  {_lc_pct(lifecycle_dist_research, lc)}")

    report_lines += [
        "",
        "7. Lifecycle Distribution (Live Limited ALL)",
    ]
    for lc in ["early_finished", "delayed", "trend_reversal", "dead_cat", "failed", "unknown"]:
        report_lines.append(f"  {_lc_pct(lifecycle_dist_live, lc)}")

    report_lines += [
        "",
        "8. Holding Day Comparison (Research ALL)",
    ]
    hold_all = _holding_day_comparison(research_all, "research_all")
    for hold in [1, 2, 3, 4, 5, 7, 10]:
        report_lines.append(_hold_line(hold_all, hold, "research_all"))

    report_lines += [
        "",
        "9. Holding Day Comparison (Live Limited ALL)",
    ]
    hold_live_all = _holding_day_comparison(live_all, "live_all")
    for hold in [1, 2, 3, 4, 5, 7, 10]:
        report_lines.append(_hold_line(hold_live_all, hold, "live_all"))

    # Day3-to-future benefit (ext-enabled only)
    ext_d3b = _day3_bucket_benefit([r for r in research_all if r.get("is_ext_enabled")], "ext_enabled_all")
    report_lines += [
        "",
        "10. Day3-to-Future Benefit (Extension-enabled, Research ALL)",
        "  Bucket | n | benefit_3_to_5 avg/WR | benefit_3_to_7 avg/WR",
    ]
    for rec in ext_d3b:
        report_lines.append(
            f"  {rec.get('day3_ret_bucket','?'):10s} | {rec['n']:4d}"
            f" | {rec.get('benefit_3_to_5_avg'):6}% ({rec.get('benefit_3_to_5_wr')}%WR)"
            f" | {rec.get('benefit_3_to_7_avg'):6}% ({rec.get('benefit_3_to_7_wr')}%WR)"
        )

    report_lines += [
        "",
        "11. Market Regime Breakdown (Research ALL)",
    ]
    for rec in sorted(_lifecycle_by_dim(research_all, "market_regime", "all"), key=lambda r: -r["n"]):
        report_lines.append(
            f"  {str(rec.get('market_regime')):25s}  n={rec['n']:4d}"
            f"  HD3_raw_avg={rec.get('hd3_raw_avg')}"
            f"  HD3_est12_avg={rec.get('hd3_est12_avg')}"
        )

    report_lines += [
        "",
        "12. Conclusion",
        "   This report analyzes the H5 HD3 edge from a lifecycle perspective.",
        "   Primary case key: h5_ai65_hd3_est12_cm_range330_live_limited",
        "   No changes to DB, case definitions, or live code.",
        "",
        "Output files:",
        "  01_dataset_summary.csv",
        "  02_raw_return_curve_research.csv",
        "  03_est12_return_curve_research.csv",
        "  04_raw_return_curve_live.csv",
        "  05_peak_day_distribution.csv",
        "  06_day3_bucket_benefit.csv",
        "  07_holding_day_comparison_research.csv",
        "  08_holding_day_comparison_live.csv",
        "  09_market_regime_lifecycle.csv",
        "  10_sector_lifecycle.csv",
        "  11_volume_bucket_lifecycle.csv",
        "  12_day1_bucket_lifecycle.csv",
        "  13_day3_bucket_lifecycle.csv",
        "  14_extension_enabled_subgroup.csv",
        "  15_research_all_records.csv",
        "  16_hd3_edge_anatomy_report.txt",
    ]

    (out_dir / "16_hd3_edge_anatomy_report.txt").write_text(
        "\n".join(report_lines), encoding="utf-8"
    )
    logger.info("[hd3_anatomy] 16 done")

    logger.info("[hd3_anatomy] ALL DONE. Output: %s", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="H5 HD3 Edge Anatomy")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="2026-05-28")
    parser.add_argument("--output-dir", default="outputs/h5_hd3_edge_anatomy")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
