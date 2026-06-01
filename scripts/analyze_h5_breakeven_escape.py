"""Analyze breakeven escape chances for H5 trades that are losing at HD3.

Research-only script. It writes CSV/TXT files under outputs/ and does not
modify DB rows, case definitions, UI, notifications, or actual_trade_logs.
"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from services.h5_primary import h5_overheat_score
from services.trade_case_tester import _build_supabase, _load_candidates_v2, _to_float

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MAX_HOLD = 10
EST12_STOP_PCT = -12.0
EST12_STOP_MULT = 0.88
HOLD_DAYS = (3, 5, 7, 10)


def _d(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _round(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        if isinstance(value, str):
            return value
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
    return (vals[mid - 1] + vals[mid]) / 2.0


def _pct(part: int | float, total: int | float) -> float | None:
    if not total:
        return None
    return float(part) / float(total) * 100.0


def _pf(values: list[float]) -> float | None:
    wins = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v < 0))
    if losses <= 0:
        return 999.0 if wins > 0 else None
    return wins / losses


def _wr(values: list[float]) -> float | None:
    if not values:
        return None
    return _pct(sum(1 for v in values if v > 0), len(values))


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _round(row.get(k)) for k in keys})


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def _entry_price(row: dict) -> float | None:
    return (
        _to_float(row.get("entry_price"), None)
        or _to_float(row.get("close"), None)
        or _to_float(row.get("signal_price"), None)
    )


def _ret(price: float | None, entry: float | None) -> float | None:
    if price is None or entry is None or entry <= 0:
        return None
    return (price / entry - 1.0) * 100.0


def _build_path(row: dict) -> list[dict]:
    path: list[dict] = []
    for day in range(1, MAX_HOLD + 1):
        path.append({
            "day": day,
            "high": _to_float(row.get(f"future_high_{day}d"), None),
            "low": _to_float(row.get(f"future_low_{day}d"), None),
            "close": _to_float(row.get(f"future_close_{day}d"), None),
        })
    return path


def _first_est12_day(path: list[dict], entry: float, start_day: int = 1, end_day: int = MAX_HOLD) -> int | None:
    stop = entry * EST12_STOP_MULT
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        low = item.get("low")
        if low is not None and low <= stop:
            return day
    return None


def _first_recovery_day(path: list[dict], entry: float, *, basis: str, start_day: int = 4, end_day: int = MAX_HOLD) -> int | None:
    key = "high" if basis == "high" else "close"
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        price = item.get(key)
        if price is not None and price >= entry:
            return day
    return None


def _close_ret(path: list[dict], entry: float, day: int) -> float | None:
    if day < 1 or day > len(path):
        return None
    return _ret(path[day - 1].get("close"), entry)


def _est12_hold_return(path: list[dict], entry: float, hold_day: int) -> dict:
    est_day = _first_est12_day(path, entry, 1, hold_day)
    if est_day is not None:
        return {"return_pct": EST12_STOP_PCT, "holding_days": est_day, "exit_reason": "emergency_stop"}
    ret = _close_ret(path, entry, hold_day)
    if ret is None:
        return {"return_pct": None, "holding_days": None, "exit_reason": "no_data"}
    return {"return_pct": ret, "holding_days": hold_day, "exit_reason": "time_stop"}


def _additional_drawdown_after_hd3(path: list[dict], entry: float, hd3_close: float | None, end_day: int = MAX_HOLD) -> float | None:
    if hd3_close is None or hd3_close <= 0:
        return None
    lows = [item.get("low") for item in path if 4 <= int(item["day"]) <= end_day and item.get("low") is not None]
    if not lows:
        return None
    return (min(lows) / hd3_close - 1.0) * 100.0


def _max_adverse_to_day(path: list[dict], entry: float, end_day: int = MAX_HOLD) -> float | None:
    lows = [item.get("low") for item in path if 1 <= int(item["day"]) <= end_day and item.get("low") is not None]
    if not lows:
        return None
    return (min(lows) / entry - 1.0) * 100.0


def _max_favorable_after_hd3(path: list[dict], entry: float, end_day: int = MAX_HOLD) -> float | None:
    highs = [item.get("high") for item in path if 4 <= int(item["day"]) <= end_day and item.get("high") is not None]
    if not highs:
        return None
    return (max(highs) / entry - 1.0) * 100.0


def _passes_ai_plus_drop(row: dict) -> bool:
    prob = _to_float(row.get("signal_probability"), None)
    drop = _to_float(row.get("drop_from_20d_high_pct"), None)
    return prob is not None and prob >= 0.65 and drop is not None and drop <= -8.0


def _passes_h5_full(row: dict) -> bool:
    if not _passes_ai_plus_drop(row):
        return False
    if str(row.get("signal_stage") or "") not in {"confirmed", "strong_confirmed"}:
        return False
    if str(row.get("market_regime") or "") == "panic_selloff":
        return False
    if h5_overheat_score(row) > 1:
        return False
    margin = _to_float(row.get("margin_ratio"), None)
    if margin is not None and (margin < 3 or margin > 30):
        return False
    return True


def _groups_for_row(row: dict) -> list[str]:
    groups: list[str] = []
    if _passes_ai_plus_drop(row):
        groups.append("AI_plus_drop")
    if _passes_h5_full(row):
        groups.extend(["H5_full", "Research_ALL", "Primary_equivalent"])
        if str(row.get("market_regime") or "") not in {"normal", "euphoria"}:
            groups.append("K_no_normal")
    return groups


def _bucket_hd3_loss(v: float | None) -> str:
    if v is None:
        return "null"
    if v >= 0:
        return "not_loser"
    if v >= -1:
        return "0_to_-1"
    if v >= -2:
        return "-1_to_-2"
    if v >= -3:
        return "-2_to_-3"
    if v >= -5:
        return "-3_to_-5"
    return "lte_-5"


def _bucket_probability(v: float | None) -> str:
    if v is None:
        return "null"
    if v < 0.65:
        return "lt_065"
    if v < 0.70:
        return "065_070"
    if v < 0.75:
        return "070_075"
    return "gte_075"


def _bucket_margin(v: float | None) -> str:
    if v is None:
        return "missing"
    if v < 3:
        return "lt_3"
    if v < 10:
        return "3_10"
    if v <= 30:
        return "10_30"
    return "gt_30"


def _bucket_drop(v: float | None) -> str:
    if v is None:
        return "null"
    if v > -10:
        return "-8_-10"
    if v > -15:
        return "-10_-15"
    return "lte_-15"


def _bucket_volume(v: float | None) -> str:
    if v is None:
        return "null"
    if v < 1.0:
        return "low"
    if v < 2.0:
        return "mid"
    return "high"


def _period_for(trade_date: date, train_end: date, test_start: date) -> str | None:
    if trade_date <= train_end:
        return "train"
    if trade_date >= test_start:
        return "test"
    return None


def _build_dataset(candidates: list[dict], train_end: date, test_start: date) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    skipped: list[dict] = []
    for row in candidates:
        groups = _groups_for_row(row)
        if not groups:
            continue
        trade_date_raw = row.get("trade_date") or row.get("label_trade_date")
        if not trade_date_raw:
            skipped.append({"reason": "missing_trade_date", "count": 1})
            continue
        trade_date = _d(trade_date_raw)
        period = _period_for(trade_date, train_end, test_start)
        if not period:
            continue
        entry = _entry_price(row)
        if entry is None or entry <= 0:
            skipped.append({"reason": "missing_entry_price", "count": 1})
            continue
        path = _build_path(row)
        if _close_ret(path, entry, 3) is None:
            skipped.append({"reason": "missing_future_close_3d", "count": 1})
            continue
        if any(item.get("high") is None for item in path[:5]):
            skipped.append({"reason": "missing_future_high_5d", "count": 1})
        if any(item.get("low") is None for item in path[:10]):
            skipped.append({"reason": "missing_future_low_10d", "count": 1})

        hd3_close = path[2].get("close")
        hd3_ret_raw = _close_ret(path, entry, 3)
        current_hd3 = _est12_hold_return(path, entry, 3)
        est12_day = _first_est12_day(path, entry, 1, MAX_HOLD)
        rec_base = {
            "code": str(row.get("code") or ""),
            "name": row.get("name"),
            "trade_date": trade_date.isoformat(),
            "period": period,
            "entry_price": entry,
            "hd3_close": hd3_close,
            "hd3_return_pct": hd3_ret_raw,
            "min_low_to_hd3_pct": _max_adverse_to_day(path, entry, 3),
            "est12_trigger_day": est12_day,
            "recover_day_high": _first_recovery_day(path, entry, basis="high", start_day=4, end_day=MAX_HOLD),
            "recover_day_close": _first_recovery_day(path, entry, basis="close", start_day=4, end_day=MAX_HOLD),
            "return_if_hd3_exit_pct": current_hd3.get("return_pct"),
            "additional_drawdown_after_hd3_pct": _additional_drawdown_after_hd3(path, entry, hd3_close, MAX_HOLD),
            "max_adverse_excursion_to_hd10_pct": _max_adverse_to_day(path, entry, MAX_HOLD),
            "max_favorable_excursion_after_hd3_pct": _max_favorable_after_hd3(path, entry, MAX_HOLD),
            "signal_probability": _to_float(row.get("signal_probability"), None),
            "signal_stage": row.get("signal_stage"),
            "drop_from_20d_high_pct": _to_float(row.get("drop_from_20d_high_pct"), None),
            "market_regime": row.get("market_regime"),
            "overheat_score": h5_overheat_score(row),
            "margin_ratio": _to_float(row.get("margin_ratio"), None),
            "volume_ratio": _to_float(row.get("volume_ratio_20d"), None),
            "sector": row.get("sector"),
            "_path": path,
        }
        for hold in (5, 7, 10):
            rec_base[f"recover_by_hd{hold}_high"] = (
                rec_base["recover_day_high"] is not None and rec_base["recover_day_high"] <= hold
            )
            rec_base[f"recover_by_hd{hold}_close"] = (
                rec_base["recover_day_close"] is not None and rec_base["recover_day_close"] <= hold
            )
            rec_base[f"return_if_breakeven_wait_hd{hold}_pct"] = None
        for group in groups:
            rec = dict(rec_base)
            rec["group"] = group
            rows.append(rec)
    return rows, skipped


def _simulate_policy(row: dict, policy: str) -> dict:
    path = row["_path"]
    entry = float(row["entry_price"])
    hd3_raw = row.get("hd3_return_pct")
    current = _est12_hold_return(path, entry, 3)

    def current_result() -> dict:
        return {
            "return_pct": current.get("return_pct"),
            "holding_days": current.get("holding_days"),
            "exit_reason": current.get("exit_reason"),
            "breakeven_exit": False,
            "hd3_exit": True,
            "final_time_exit": current.get("exit_reason") == "time_stop",
            "est12": current.get("exit_reason") == "emergency_stop",
        }

    if policy == "current_hd3":
        return current_result()
    if current.get("exit_reason") == "emergency_stop" or hd3_raw is None or hd3_raw >= 0:
        return current_result()

    max_day = 7
    threshold: float | None = None
    if policy == "hd3_loser_wait_be_hd5":
        max_day = 5
    elif policy == "hd3_loser_wait_be_hd7":
        max_day = 7
    elif policy == "hd3_loser_wait_be_hd10":
        max_day = 10
    elif policy == "hd3_loser_within_2pct_wait_be_hd7":
        max_day, threshold = 7, -2.0
    elif policy == "hd3_loser_within_3pct_wait_be_hd7":
        max_day, threshold = 7, -3.0
    elif policy == "hd3_loser_within_5pct_wait_be_hd7":
        max_day, threshold = 7, -5.0
    else:
        return {"return_pct": None, "holding_days": None, "exit_reason": "skipped"}

    if threshold is not None and hd3_raw < threshold:
        return current_result()

    stop_price = entry * EST12_STOP_MULT
    for item in path:
        day = int(item["day"])
        if day < 4 or day > max_day:
            continue
        low = item.get("low")
        high = item.get("high")
        if low is not None and low <= stop_price:
            return {
                "return_pct": EST12_STOP_PCT,
                "holding_days": day,
                "exit_reason": "emergency_stop",
                "breakeven_exit": False,
                "hd3_exit": False,
                "final_time_exit": False,
                "est12": True,
            }
        if high is not None and high >= entry:
            return {
                "return_pct": 0.0,
                "holding_days": day,
                "exit_reason": "breakeven_escape",
                "breakeven_exit": True,
                "hd3_exit": False,
                "final_time_exit": False,
                "est12": False,
            }
    final_ret = _close_ret(path, entry, max_day)
    return {
        "return_pct": final_ret,
        "holding_days": max_day if final_ret is not None else None,
        "exit_reason": "final_time_exit" if final_ret is not None else "no_data",
        "breakeven_exit": False,
        "hd3_exit": False,
        "final_time_exit": final_ret is not None,
        "est12": False,
    }


def _summarize_losers(rows: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        group_rows = [r for r in rows if r["group"] == group]
        for period in periods:
            subset = group_rows if period == "all" else [r for r in group_rows if r["period"] == period]
            losers = [r for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
            est_before = []
            for r in losers:
                rec_day = r.get("recover_day_high")
                est_day = r.get("est12_trigger_day")
                if est_day is not None and (rec_day is None or est_day <= rec_day):
                    est_before.append(r)
            out.append({
                "group": group,
                "period": period,
                "total_n": len(subset),
                "hd3_loser_n": len(losers),
                "hd3_loser_rate": _pct(len(losers), len(subset)),
                "hd3_loser_avg_return": _avg([r["hd3_return_pct"] for r in losers if r.get("hd3_return_pct") is not None]),
                "recover_by_hd5_high_n": sum(1 for r in losers if r.get("recover_by_hd5_high")),
                "recover_by_hd5_high_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd5_high")), len(losers)),
                "recover_by_hd7_high_n": sum(1 for r in losers if r.get("recover_by_hd7_high")),
                "recover_by_hd7_high_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd7_high")), len(losers)),
                "recover_by_hd10_high_n": sum(1 for r in losers if r.get("recover_by_hd10_high")),
                "recover_by_hd10_high_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd10_high")), len(losers)),
                "recover_by_hd5_close_n": sum(1 for r in losers if r.get("recover_by_hd5_close")),
                "recover_by_hd5_close_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd5_close")), len(losers)),
                "recover_by_hd7_close_n": sum(1 for r in losers if r.get("recover_by_hd7_close")),
                "recover_by_hd7_close_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd7_close")), len(losers)),
                "recover_by_hd10_close_n": sum(1 for r in losers if r.get("recover_by_hd10_close")),
                "recover_by_hd10_close_rate": _pct(sum(1 for r in losers if r.get("recover_by_hd10_close")), len(losers)),
                "never_recover_hd10_n": sum(1 for r in losers if not r.get("recover_by_hd10_high")),
                "never_recover_hd10_rate": _pct(sum(1 for r in losers if not r.get("recover_by_hd10_high")), len(losers)),
                "est12_before_recover_n": len(est_before),
                "est12_before_recover_rate": _pct(len(est_before), len(losers)),
                "avg_days_to_recover_high": _avg([r["recover_day_high"] for r in losers if r.get("recover_day_high") is not None]),
                "median_days_to_recover_high": _median([r["recover_day_high"] for r in losers if r.get("recover_day_high") is not None]),
            })
    return out


def _policy_comparison(rows: list[dict], periods: list[str]) -> list[dict]:
    policies = [
        "current_hd3",
        "hd3_loser_wait_be_hd5",
        "hd3_loser_wait_be_hd7",
        "hd3_loser_wait_be_hd10",
        "hd3_loser_within_2pct_wait_be_hd7",
        "hd3_loser_within_3pct_wait_be_hd7",
        "hd3_loser_within_5pct_wait_be_hd7",
    ]
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        group_rows = [r for r in rows if r["group"] == group]
        for period in periods:
            subset = group_rows if period == "all" else [r for r in group_rows if r["period"] == period]
            current_vals = [
                _simulate_policy(r, "current_hd3").get("return_pct")
                for r in subset
            ]
            current_vals = [v for v in current_vals if v is not None]
            current_avg = _avg(current_vals)
            current_pf = _pf(current_vals)
            current_est = sum(1 for r in subset if _simulate_policy(r, "current_hd3").get("est12"))
            for policy in policies:
                sims = [_simulate_policy(r, policy) for r in subset]
                vals = [s["return_pct"] for s in sims if s.get("return_pct") is not None]
                avg_ret = _avg(vals)
                pf = _pf(vals)
                est_count = sum(1 for s in sims if s.get("est12"))
                delta_avg = (avg_ret - current_avg) if (avg_ret is not None and current_avg is not None) else None
                delta_pf = (pf - current_pf) if (pf is not None and current_pf is not None) else None
                est_delta = est_count - current_est
                judgment = "WATCH"
                if policy == "current_hd3":
                    judgment = "BASE"
                elif delta_avg is not None and delta_avg > 0 and (delta_pf is None or delta_pf >= -0.02) and est_delta <= max(3, len(subset) * 0.01):
                    judgment = "PASS"
                elif delta_avg is not None and delta_avg < 0:
                    judgment = "FAIL"
                out.append({
                    "policy": policy,
                    "period": period,
                    "group": group,
                    "n": len(subset),
                    "avg_return_pct": avg_ret,
                    "median_return_pct": _median(vals),
                    "win_rate": _wr(vals),
                    "profit_factor": pf,
                    "total_return_sum": sum(vals) if vals else None,
                    "avg_holding_days": _avg([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                    "median_holding_days": _median([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                    "est12_count": est_count,
                    "est12_rate": _pct(est_count, len(subset)),
                    "breakeven_exit_count": sum(1 for s in sims if s.get("breakeven_exit")),
                    "breakeven_exit_rate": _pct(sum(1 for s in sims if s.get("breakeven_exit")), len(subset)),
                    "hd3_exit_count": sum(1 for s in sims if s.get("hd3_exit")),
                    "final_time_exit_count": sum(1 for s in sims if s.get("final_time_exit")),
                    "delta_avg_vs_current_hd3": delta_avg,
                    "delta_pf_vs_current_hd3": delta_pf,
                    "delta_est12_vs_current_hd3": est_delta,
                    "judgment": judgment,
                })
    return out


def _recovery_distribution(rows: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = [r for r in rows if r["group"] == group and (period == "all" or r["period"] == period)]
            losers = [r for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
            buckets = defaultdict(list)
            for r in losers:
                rec_day = r.get("recover_day_high")
                est_day = r.get("est12_trigger_day")
                if est_day is not None and (rec_day is None or est_day <= rec_day):
                    key = "est12_before_recovery"
                elif rec_day is None:
                    key = "no_recovery"
                else:
                    key = f"day{rec_day}"
                buckets[key].append(r)
            for key in [f"day{d}" for d in range(4, 11)] + ["no_recovery", "est12_before_recovery"]:
                bucket_rows = buckets.get(key, [])
                out.append({
                    "group": group,
                    "period": period,
                    "recovery_day": key,
                    "n": len(bucket_rows),
                    "pct_of_hd3_losers": _pct(len(bucket_rows), len(losers)),
                    "avg_hd3_return_pct": _avg([r["hd3_return_pct"] for r in bucket_rows if r.get("hd3_return_pct") is not None]),
                    "avg_final_hd10_return_pct": _avg([_close_ret(r["_path"], r["entry_price"], 10) for r in bucket_rows if _close_ret(r["_path"], r["entry_price"], 10) is not None]),
                })
    return out


def _extra_risk_summary(rows: list[dict], policies: list[dict]) -> list[dict]:
    out: list[dict] = []
    policy_rows = [r for r in policies if r["policy"] != "current_hd3"]
    by_key = {(r["policy"], r["group"], r["period"]): r for r in policy_rows}
    for key, pol in by_key.items():
        policy, group, period = key
        subset = [r for r in rows if r["group"] == group and (period == "all" or r["period"] == period)]
        losers = [r for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
        current = next((r for r in policies if r["policy"] == "current_hd3" and r["group"] == group and r["period"] == period), {})
        avg_added = None
        if pol.get("avg_holding_days") is not None and current.get("avg_holding_days") is not None:
            avg_added = pol["avg_holding_days"] - current["avg_holding_days"]
        improvement = pol.get("delta_avg_vs_current_hd3")
        out.append({
            "policy": policy,
            "group": group,
            "period": period,
            "hd3_loser_n": len(losers),
            "avg_additional_drawdown_after_hd3_pct": _avg([r["additional_drawdown_after_hd3_pct"] for r in losers if r.get("additional_drawdown_after_hd3_pct") is not None]),
            "median_additional_drawdown_after_hd3_pct": _median([r["additional_drawdown_after_hd3_pct"] for r in losers if r.get("additional_drawdown_after_hd3_pct") is not None]),
            "worst_additional_drawdown_after_hd3_pct": min([r["additional_drawdown_after_hd3_pct"] for r in losers if r.get("additional_drawdown_after_hd3_pct") is not None], default=None),
            "avg_holding_days_added": avg_added,
            "avg_return_improvement_vs_hd3": improvement,
            "improvement_per_extra_day": (improvement / avg_added) if improvement is not None and avg_added and avg_added > 0 else None,
            "est12_rate": pol.get("est12_rate"),
            "notes": "",
        })
    return out


def _feature_breakdown(rows: list[dict], periods: list[str]) -> list[dict]:
    specs = [
        ("hd3_return_bucket", lambda r: _bucket_hd3_loss(r.get("hd3_return_pct"))),
        ("signal_probability_bucket", lambda r: _bucket_probability(r.get("signal_probability"))),
        ("market_regime", lambda r: str(r.get("market_regime") or "null")),
        ("overheat_score", lambda r: str(r.get("overheat_score") if r.get("overheat_score") is not None else "null")),
        ("margin_ratio_bucket", lambda r: _bucket_margin(r.get("margin_ratio"))),
        ("drop20_bucket", lambda r: _bucket_drop(r.get("drop_from_20d_high_pct"))),
        ("volume_ratio_bucket", lambda r: _bucket_volume(r.get("volume_ratio"))),
    ]
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            base = [r for r in rows if r["group"] == group and (period == "all" or r["period"] == period)]
            losers = [r for r in base if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
            for feature, fn in specs:
                buckets: dict[str, list[dict]] = defaultdict(list)
                for r in losers:
                    buckets[fn(r)].append(r)
                for bucket, bucket_rows in sorted(buckets.items()):
                    hd7_vals = [_simulate_policy(r, "hd3_loser_wait_be_hd7")["return_pct"] for r in bucket_rows]
                    hd7_vals = [v for v in hd7_vals if v is not None]
                    hd3_vals = [_simulate_policy(r, "current_hd3")["return_pct"] for r in bucket_rows]
                    hd3_vals = [v for v in hd3_vals if v is not None]
                    avg_wait = _avg(hd7_vals)
                    avg_hd3 = _avg(hd3_vals)
                    delta = avg_wait - avg_hd3 if avg_wait is not None and avg_hd3 is not None else None
                    out.append({
                        "feature": feature,
                        "bucket": bucket,
                        "group": group,
                        "period": period,
                        "hd3_loser_n": len(bucket_rows),
                        "recover_by_hd7_rate": _pct(sum(1 for r in bucket_rows if r.get("recover_by_hd7_high")), len(bucket_rows)),
                        "recover_by_hd10_rate": _pct(sum(1 for r in bucket_rows if r.get("recover_by_hd10_high")), len(bucket_rows)),
                        "avg_return_if_wait_hd7": avg_wait,
                        "avg_return_if_hd3_exit": avg_hd3,
                        "delta": delta,
                        "est12_rate": _pct(sum(1 for r in bucket_rows if _simulate_policy(r, "hd3_loser_wait_be_hd7").get("est12")), len(bucket_rows)),
                        "judgment": "PASS" if delta is not None and delta > 0 else ("FAIL" if delta is not None and delta < 0 else "WATCH"),
                    })
    return out


def _monthly_stability(rows: list[dict], periods: list[str]) -> list[dict]:
    policies = ["current_hd3", "hd3_loser_wait_be_hd5", "hd3_loser_wait_be_hd7", "hd3_loser_wait_be_hd10"]
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        group_rows = [r for r in rows if r["group"] == group]
        months = sorted({r["trade_date"][:7] for r in group_rows})
        for month in months:
            subset = [r for r in group_rows if r["trade_date"].startswith(month)]
            for policy in policies:
                sims = [_simulate_policy(r, policy) for r in subset]
                vals = [s["return_pct"] for s in sims if s.get("return_pct") is not None]
                out.append({
                    "policy": policy,
                    "group": group,
                    "month": month,
                    "n": len(subset),
                    "avg_return_pct": _avg(vals),
                    "win_rate": _wr(vals),
                    "pf": _pf(vals),
                    "breakeven_exit_count": sum(1 for s in sims if s.get("breakeven_exit")),
                    "est12_count": sum(1 for s in sims if s.get("est12")),
                    "avg_holding_days": _avg([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                })
    return out


def _group_comparison(rows: list[dict], policy_rows: list[dict]) -> list[dict]:
    out = []
    for group in sorted({r["group"] for r in rows}):
        subset = [r for r in rows if r["group"] == group]
        all_policies = [r for r in policy_rows if r["group"] == group and r["period"] == "all"]
        current = next((r for r in all_policies if r["policy"] == "current_hd3"), {})
        best = max(
            [r for r in all_policies if r["policy"] != "current_hd3" and r.get("avg_return_pct") is not None],
            key=lambda r: r["avg_return_pct"],
            default={},
        )
        out.append({
            "group": group,
            "n": len(subset),
            "hd3_avg": current.get("avg_return_pct"),
            "hd3_loser_rate": _pct(sum(1 for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0), len(subset)),
            "recover_by_hd7_rate": _pct(sum(1 for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0 and r.get("recover_by_hd7_high")), sum(1 for r in subset if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0)),
            "best_policy": best.get("policy"),
            "best_policy_avg_return": best.get("avg_return_pct"),
            "current_hd3_avg_return": current.get("avg_return_pct"),
            "delta": best.get("delta_avg_vs_current_hd3"),
            "judgment": best.get("judgment"),
        })
    return out


def _detail_rows(rows: list[dict]) -> list[dict]:
    details = []
    for r in rows:
        if r.get("hd3_return_pct") is None or r["hd3_return_pct"] >= 0:
            continue
        row = {k: v for k, v in r.items() if not k.startswith("_")}
        for hold in (5, 7, 10):
            row[f"return_if_breakeven_wait_hd{hold}_pct"] = _simulate_policy(r, f"hd3_loser_wait_be_hd{hold}")["return_pct"]
        details.append(row)
    return details


def _skipped_summary(skipped: list[dict]) -> list[dict]:
    counts = defaultdict(int)
    for row in skipped:
        counts[row["reason"]] += int(row.get("count") or 1)
    return [{"reason": reason, "count": count, "notes": ""} for reason, count in sorted(counts.items())]


def _generate_report(
    *,
    rows: list[dict],
    summary: list[dict],
    policies: list[dict],
    group_comp: list[dict],
    score_source: str,
    start: date,
    end: date,
) -> str:
    def find_summary(group: str, period: str = "all") -> dict:
        return next((r for r in summary if r["group"] == group and r["period"] == period), {})

    def best_policy(group: str, period: str = "all") -> dict:
        candidates = [
            r for r in policies
            if r["group"] == group and r["period"] == period and r["policy"] != "current_hd3"
            and r.get("avg_return_pct") is not None
        ]
        return max(candidates, key=lambda r: r["avg_return_pct"], default={})

    main = find_summary("Research_ALL", "all") or (summary[0] if summary else {})
    best = best_policy("Research_ALL", "all")
    current = next((r for r in policies if r["group"] == "Research_ALL" and r["period"] == "all" and r["policy"] == "current_hd3"), {})
    train_best = best_policy("Research_ALL", "train")
    test_best = best_policy("Research_ALL", "test")

    lines = [
        "H5 Breakeven Escape Analysis",
        "=" * 36,
        "",
        f"期間: {start.isoformat()} .. {end.isoformat()}",
        f"score_source: {score_source}",
        "注意: highベースの建値回復は、実運用では建値売指値を置いていた場合の逃げ場です。closeベースとは分けて見てください。",
        "",
        "1. HD3負け銘柄の概要",
        f"- 対象件数(Research_ALL): {main.get('total_n')}",
        f"- HD3負け銘柄数/割合: {main.get('hd3_loser_n')} / {_round(main.get('hd3_loser_rate'), 2)}%",
        f"- HD3負け銘柄の平均損失: {_round(main.get('hd3_loser_avg_return'), 3)}%",
        "",
        "2. 建値回復チャンス",
        f"- HD5 high回復率: {_round(main.get('recover_by_hd5_high_rate'), 2)}%",
        f"- HD7 high回復率: {_round(main.get('recover_by_hd7_high_rate'), 2)}%",
        f"- HD10 high回復率: {_round(main.get('recover_by_hd10_high_rate'), 2)}%",
        f"- HD5 close回復率: {_round(main.get('recover_by_hd5_close_rate'), 2)}%",
        f"- HD7 close回復率: {_round(main.get('recover_by_hd7_close_rate'), 2)}%",
        f"- HD10 close回復率: {_round(main.get('recover_by_hd10_close_rate'), 2)}%",
        f"- 平均回復日(high): {_round(main.get('avg_days_to_recover_high'), 2)}",
        "",
        "3. Policy比較",
        f"- current_hd3 avg: {_round(current.get('avg_return_pct'), 4)}% PF={_round(current.get('profit_factor'), 4)}",
        f"- 最良policy(all): {best.get('policy')} avg={_round(best.get('avg_return_pct'), 4)}% PF={_round(best.get('profit_factor'), 4)} delta={_round(best.get('delta_avg_vs_current_hd3'), 4)} judgment={best.get('judgment')}",
        f"- train最良: {train_best.get('policy')} avg={_round(train_best.get('avg_return_pct'), 4)}% judgment={train_best.get('judgment')}",
        f"- test最良: {test_best.get('policy')} avg={_round(test_best.get('avg_return_pct'), 4)}% judgment={test_best.get('judgment')}",
        "",
        "4. 暫定判断",
    ]
    if best.get("judgment") == "PASS" and train_best.get("judgment") == "PASS" and test_best.get("judgment") in {"PASS", "WATCH"}:
        lines.append("- 建値逃げ待ちは採用候補として追加検証する価値があります。ただしPrimary変更はまだ不要です。")
    else:
        lines.append("- 現時点ではPrimary変更は不要です。建値逃げは研究候補として、資金拘束とhighベース実行可能性を追加確認してください。")
    lines.extend([
        "- HD3負け全件を長く粘るより、-2%/-3%以内など損失幅を絞ったpolicyが安定するかを重視してください。",
        "- highベースだけ良く、closeベースが弱い場合は、建値売指値を置く運用が前提になります。",
        "",
        "5. group比較",
    ])
    for row in group_comp:
        lines.append(
            f"- {row.get('group')}: n={row.get('n')} hd3_avg={_round(row.get('hd3_avg'), 4)} "
            f"best={row.get('best_policy')} delta={_round(row.get('delta'), 4)} judgment={row.get('judgment')}"
        )
    lines.extend([
        "",
        "次に見ること:",
        "- 建値回復highが実際に指値で刺さるか、日中高値到達の現実性を確認する",
        "- stored_predictions forward-testでも同じ建値逃げログを蓄積する",
        "- 採用する場合も比較caseから始め、Primaryにはすぐ入れない",
    ])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze H5 HD3 loser breakeven escape chances")
    parser.add_argument("--output-dir", default="outputs/h5_breakeven_escape")
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--score-source", default="active_model", choices=["active_model", "stored_predictions", "stored_or_active_fallback"])
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--allow-score-fallback", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    start = _d(args.start_date)
    end = _d(args.end_date)
    train_end = _d(args.train_end)
    test_start = _d(args.test_start)
    periods = ["train", "test", "all"]

    sb = _build_supabase()
    candidates = _load_candidates_v2(
        sb,
        start,
        end,
        score_source=args.score_source,
        model_key=args.model_key,
        model_version=args.model_version,
        allow_score_fallback=args.allow_score_fallback,
    )
    rows, skipped = _build_dataset(candidates, train_end, test_start)
    detail = _detail_rows(rows)
    summary = _summarize_losers(rows, periods)
    policies = _policy_comparison(rows, periods)
    recovery = _recovery_distribution(rows, periods)
    extra = _extra_risk_summary(rows, policies)
    features = _feature_breakdown(rows, periods)
    monthly = _monthly_stability(rows, periods)
    group_comp = _group_comparison(rows, policies)
    skipped_rows = _skipped_summary(skipped)

    _write_text(output_dir / "00_input_dataset_summary.txt", f"""
Input dataset summary
=====================
start_date: {start.isoformat()}
end_date: {end.isoformat()}
train_end: {train_end.isoformat()}
test_start: {test_start.isoformat()}
score_source: {args.score_source}
model_key: {args.model_key}
model_version: {args.model_version or ""}
loaded_candidates: {len(candidates)}
analysis_rows_with_group_membership: {len(rows)}
hd3_loser_detail_rows: {len(detail)}

This is analysis-only. It does not modify Primary, DB case definitions, UI,
LINE notifications, actual_trade_logs, Watchlist, or Intraday H5.
""")
    _write_csv(output_dir / "01_hd3_loser_breakeven_summary.csv", summary)
    _write_csv(output_dir / "02_hd3_loser_detail.csv", detail)
    _write_csv(output_dir / "03_breakeven_exit_policy_comparison.csv", policies)
    _write_csv(output_dir / "04_recovery_day_distribution.csv", recovery)
    _write_csv(output_dir / "05_extra_risk_summary.csv", extra)
    _write_csv(output_dir / "06_breakeven_recovery_feature_breakdown.csv", features)
    _write_csv(output_dir / "07_policy_monthly_stability.csv", monthly)
    _write_csv(output_dir / "08_group_comparison.csv", group_comp)
    _write_csv(output_dir / "09_skipped_rows_summary.csv", skipped_rows)
    _write_text(
        output_dir / "10_breakeven_escape_report.txt",
        _generate_report(
            rows=rows,
            summary=summary,
            policies=policies,
            group_comp=group_comp,
            score_source=args.score_source,
            start=start,
            end=end,
        ),
    )

    main_summary = next((r for r in summary if r["group"] == "Research_ALL" and r["period"] == "all"), {})
    best = max(
        [
            r for r in policies
            if r["group"] == "Research_ALL"
            and r["period"] == "all"
            and r["policy"] != "current_hd3"
            and r.get("avg_return_pct") is not None
        ],
        key=lambda r: r["avg_return_pct"],
        default={},
    )
    print(f"loaded_candidates={len(candidates)}")
    print(f"analysis_rows={len(rows)}")
    print(f"hd3_loser_n={main_summary.get('hd3_loser_n')}")
    print(f"recover_by_hd7_high_rate={_round(main_summary.get('recover_by_hd7_high_rate'), 2)}")
    print(f"best_policy={best.get('policy')} avg={_round(best.get('avg_return_pct'), 4)} judgment={best.get('judgment')}")
    print(f"output_dir={output_dir}")


if __name__ == "__main__":
    main()
