"""Analyze what happens after H5 HD3 losers recover to breakeven.

Research-only script. It writes CSV/TXT files under outputs/ and does not
modify Primary, DB case definitions, UI, notifications, actual_trade_logs,
Watchlist, or Intraday H5.
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
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from dotenv import load_dotenv

from analyze_h5_breakeven_escape import (  # noqa: E402
    EST12_STOP_MULT,
    EST12_STOP_PCT,
    MAX_HOLD,
    _avg,
    _bucket_drop,
    _bucket_hd3_loss,
    _bucket_margin,
    _bucket_probability,
    _bucket_volume,
    _build_dataset,
    _build_supabase,
    _close_ret,
    _d,
    _first_est12_day,
    _load_candidates_v2,
    _median,
    _pct,
    _period_for,
    _pf,
    _ret,
    _round,
    _to_float,
    _wr,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


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


def _price_on(path: list[dict], day: int, key: str) -> float | None:
    if day < 1 or day > len(path):
        return None
    return path[day - 1].get(key)


def _max_high_after(path: list[dict], entry: float, start_day: int, end_day: int = MAX_HOLD) -> tuple[float | None, int | None]:
    best_price = None
    best_day = None
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        high = item.get("high")
        if high is None:
            continue
        if best_price is None or high > best_price:
            best_price = high
            best_day = day
    return _ret(best_price, entry), best_day


def _min_low_after(path: list[dict], entry: float, start_day: int, end_day: int = MAX_HOLD) -> tuple[float | None, int | None]:
    worst_price = None
    worst_day = None
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        low = item.get("low")
        if low is None:
            continue
        if worst_price is None or low < worst_price:
            worst_price = low
            worst_day = day
    return _ret(worst_price, entry), worst_day


def _min_close_after(path: list[dict], entry: float, start_day: int, end_day: int = MAX_HOLD) -> tuple[float | None, int | None]:
    worst_price = None
    worst_day = None
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        close = item.get("close")
        if close is None:
            continue
        if worst_price is None or close < worst_price:
            worst_price = close
            worst_day = day
    return _ret(worst_price, entry), worst_day


def _first_recovery_price(path: list[dict], entry: float, basis: str, start_day: int = 4, end_day: int = MAX_HOLD) -> tuple[int | None, float | None]:
    key = "high" if basis == "high" else "close"
    for item in path:
        day = int(item["day"])
        if day < start_day or day > end_day:
            continue
        price = item.get(key)
        if price is not None and price >= entry:
            return day, price
    return None, None


def _est12_after_recovery(path: list[dict], entry: float, recover_day: int, end_day: int = MAX_HOLD) -> int | None:
    # Start from the next day because same-day OHLC order is unknowable.
    return _first_est12_day(path, entry, recover_day + 1, end_day)


def _build_after_recovery_dataset(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    details: list[dict] = []
    skipped: list[dict] = []
    for r in rows:
        hd3 = r.get("hd3_return_pct")
        if hd3 is None or hd3 >= 0:
            skipped.append({"reason": "no_hd3_loss", "count": 1})
            continue
        path = r.get("_path")
        entry = _to_float(r.get("entry_price"), None)
        if not path or entry is None or entry <= 0:
            skipped.append({"reason": "missing_entry_price", "count": 1})
            continue
        rec_day, rec_price = _first_recovery_price(path, entry, "high", 4, MAX_HOLD)
        rec_close_day, rec_close_price = _first_recovery_price(path, entry, "close", 4, MAX_HOLD)
        if rec_day is None:
            skipped.append({"reason": "no_breakeven_recovery", "count": 1})
            continue
        est_before = _first_est12_day(path, entry, 1, rec_day)
        if est_before is not None:
            skipped.append({"reason": "est12_before_or_same_day_recovery", "count": 1})
            continue

        max_gain, max_gain_day = _max_high_after(path, entry, rec_day, MAX_HOLD)
        min_low, min_low_day = _min_low_after(path, entry, rec_day, MAX_HOLD)
        min_close, min_close_day = _min_close_after(path, entry, rec_day, MAX_HOLD)
        est_after = _est12_after_recovery(path, entry, rec_day, MAX_HOLD)
        hd5_ret = _close_ret(path, entry, 5)
        hd7_ret = _close_ret(path, entry, 7)
        hd10_ret = _close_ret(path, entry, 10)
        detail = {
            "code": r.get("code"),
            "name": r.get("name"),
            "trade_date": r.get("trade_date"),
            "period": r.get("period"),
            "group": r.get("group"),
            "entry_price": entry,
            "hd3_close": r.get("hd3_close"),
            "hd3_return_pct": hd3,
            "recover_day_high": rec_day,
            "recover_price_high": rec_price,
            "recover_day_close": rec_close_day,
            "recover_price_close": rec_close_price,
            "est12_trigger_day_before_recover": est_before,
            "est12_trigger_day_after_recover": est_after,
            "after_recovery_high_max_pct": max_gain,
            "after_recovery_high_max_day": max_gain_day,
            "after_recovery_low_min_pct": min_low,
            "after_recovery_low_min_day": min_low_day,
            "after_recovery_close_min_pct": min_close,
            "after_recovery_close_min_day": min_close_day,
            "after_recovery_plus1": max_gain is not None and max_gain >= 1.0,
            "after_recovery_plus2": max_gain is not None and max_gain >= 2.0,
            "after_recovery_plus3": max_gain is not None and max_gain >= 3.0,
            "after_recovery_plus5": max_gain is not None and max_gain >= 5.0,
            "after_recovery_rebreak_minus1": min_low is not None and min_low <= -1.0,
            "after_recovery_rebreak_minus2": min_low is not None and min_low <= -2.0,
            "after_recovery_rebreak_minus3": min_low is not None and min_low <= -3.0,
            "after_recovery_close_rebreak_minus1": min_close is not None and min_close <= -1.0,
            "after_recovery_close_rebreak_minus2": min_close is not None and min_close <= -2.0,
            "after_recovery_close_rebreak_minus3": min_close is not None and min_close <= -3.0,
            "hd5_close_return_pct": hd5_ret,
            "hd7_close_return_pct": hd7_ret,
            "hd10_close_return_pct": hd10_ret,
            "return_if_sell_at_breakeven_pct": 0.0,
            "return_if_hold_to_hd7_pct": hd7_ret if rec_day <= 7 else None,
            "return_if_hold_to_hd10_pct": hd10_ret,
            "signal_probability": r.get("signal_probability"),
            "signal_stage": r.get("signal_stage"),
            "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
            "market_regime": r.get("market_regime"),
            "overheat_score": r.get("overheat_score"),
            "margin_ratio": r.get("margin_ratio"),
            "volume_ratio": r.get("volume_ratio"),
            "sector": r.get("sector"),
            "_path": path,
            "_entry_price": entry,
        }
        details.append(detail)
    return details, skipped


def _subset(rows: list[dict], group: str, period: str) -> list[dict]:
    return [r for r in rows if r["group"] == group and (period == "all" or r["period"] == period)]


def _summary(rows: list[dict], recovered: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    groups = sorted({r["group"] for r in rows})
    for group in groups:
        for period in periods:
            base = _subset(rows, group, period)
            losers = [r for r in base if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
            rec = _subset(recovered, group, period)
            rec5 = [r for r in rec if r["recover_day_high"] <= 5]
            rec7 = [r for r in rec if r["recover_day_high"] <= 7]
            rec10 = rec
            out.append({
                "group": group,
                "period": period,
                "hd3_loser_n": len(losers),
                "recovered_by_hd5_n": len(rec5),
                "recovered_by_hd7_n": len(rec7),
                "recovered_by_hd10_n": len(rec10),
                "recovered_by_hd5_rate": _pct(len(rec5), len(losers)),
                "recovered_by_hd7_rate": _pct(len(rec7), len(losers)),
                "recovered_by_hd10_rate": _pct(len(rec10), len(losers)),
                "after_recovery_plus1_n": sum(1 for r in rec if r["after_recovery_plus1"]),
                "after_recovery_plus1_rate": _pct(sum(1 for r in rec if r["after_recovery_plus1"]), len(rec)),
                "after_recovery_plus2_n": sum(1 for r in rec if r["after_recovery_plus2"]),
                "after_recovery_plus2_rate": _pct(sum(1 for r in rec if r["after_recovery_plus2"]), len(rec)),
                "after_recovery_plus3_n": sum(1 for r in rec if r["after_recovery_plus3"]),
                "after_recovery_plus3_rate": _pct(sum(1 for r in rec if r["after_recovery_plus3"]), len(rec)),
                "after_recovery_plus5_n": sum(1 for r in rec if r["after_recovery_plus5"]),
                "after_recovery_plus5_rate": _pct(sum(1 for r in rec if r["after_recovery_plus5"]), len(rec)),
                "after_recovery_rebreak_minus1_n": sum(1 for r in rec if r["after_recovery_rebreak_minus1"]),
                "after_recovery_rebreak_minus1_rate": _pct(sum(1 for r in rec if r["after_recovery_rebreak_minus1"]), len(rec)),
                "after_recovery_rebreak_minus2_n": sum(1 for r in rec if r["after_recovery_rebreak_minus2"]),
                "after_recovery_rebreak_minus2_rate": _pct(sum(1 for r in rec if r["after_recovery_rebreak_minus2"]), len(rec)),
                "after_recovery_rebreak_minus3_n": sum(1 for r in rec if r["after_recovery_rebreak_minus3"]),
                "after_recovery_rebreak_minus3_rate": _pct(sum(1 for r in rec if r["after_recovery_rebreak_minus3"]), len(rec)),
                "hd7_close_positive_n": sum(1 for r in rec if r.get("hd7_close_return_pct") is not None and r["hd7_close_return_pct"] > 0),
                "hd7_close_positive_rate": _pct(sum(1 for r in rec if r.get("hd7_close_return_pct") is not None and r["hd7_close_return_pct"] > 0), len(rec)),
                "hd10_close_positive_n": sum(1 for r in rec if r.get("hd10_close_return_pct") is not None and r["hd10_close_return_pct"] > 0),
                "hd10_close_positive_rate": _pct(sum(1 for r in rec if r.get("hd10_close_return_pct") is not None and r["hd10_close_return_pct"] > 0), len(rec)),
                "avg_after_recovery_max_gain_pct": _avg([r["after_recovery_high_max_pct"] for r in rec if r.get("after_recovery_high_max_pct") is not None]),
                "median_after_recovery_max_gain_pct": _median([r["after_recovery_high_max_pct"] for r in rec if r.get("after_recovery_high_max_pct") is not None]),
                "avg_hd7_close_return_after_recovery": _avg([r["hd7_close_return_pct"] for r in rec if r.get("hd7_close_return_pct") is not None]),
                "avg_hd10_close_return_after_recovery": _avg([r["hd10_close_return_pct"] for r in rec if r.get("hd10_close_return_pct") is not None]),
            })
    return out


def _simulate_after_policy(row: dict, policy: str) -> dict:
    path = row["_path"]
    entry = float(row["_entry_price"])
    rec_day = int(row["recover_day_high"])

    def sell_be() -> dict:
        return {
            "return_pct": 0.0,
            "holding_days": rec_day,
            "exit_reason": "sell_at_breakeven",
            "target_hit": False,
            "breakeven_stop": False,
            "time_exit": False,
            "est12": False,
        }

    if policy == "sell_at_breakeven":
        return sell_be()

    max_day = None
    target_pct = None
    trailing_pct = None
    if policy == "hold_to_hd7_after_recovery":
        max_day = 7
    elif policy == "hold_to_hd10_after_recovery":
        max_day = 10
    elif policy.startswith("target_plus") and policy.endswith("_hd7"):
        max_day = 7
        target_pct = float(policy.split("_")[1].replace("plus", ""))
    elif policy.startswith("target_plus") and policy.endswith("_hd10"):
        max_day = 10
        target_pct = float(policy.split("_")[1].replace("plus", ""))
    elif policy == "trailing_after_breakeven_2pct_hd7":
        max_day = 7
        trailing_pct = 2.0
    else:
        return {"return_pct": None, "holding_days": None, "exit_reason": "skipped"}

    if rec_day > max_day:
        return {"return_pct": None, "holding_days": None, "exit_reason": "not_recovered_by_policy_horizon"}

    stop_price = entry * EST12_STOP_MULT
    if policy in {"hold_to_hd7_after_recovery", "hold_to_hd10_after_recovery"}:
        for day in range(rec_day + 1, max_day + 1):
            low = _price_on(path, day, "low")
            if low is not None and low <= stop_price:
                return {
                    "return_pct": EST12_STOP_PCT,
                    "holding_days": day,
                    "exit_reason": "emergency_stop",
                    "target_hit": False,
                    "breakeven_stop": False,
                    "time_exit": False,
                    "est12": True,
                }
        close_ret = _close_ret(path, entry, max_day)
        return {
            "return_pct": close_ret,
            "holding_days": max_day if close_ret is not None else None,
            "exit_reason": "time_exit" if close_ret is not None else "no_data",
            "target_hit": False,
            "breakeven_stop": False,
            "time_exit": close_ret is not None,
            "est12": False,
        }

    target_price = entry * (1 + target_pct / 100.0) if target_pct is not None else None
    peak = entry
    for day in range(rec_day, max_day + 1):
        high = _price_on(path, day, "high")
        low = _price_on(path, day, "low")
        if high is not None:
            peak = max(peak, high)

        # Recovery-day high can satisfy a target, but same-day low order is unknowable.
        if target_price is not None and high is not None and high >= target_price:
            return {
                "return_pct": target_pct,
                "holding_days": day,
                "exit_reason": f"target_plus{target_pct:g}",
                "target_hit": True,
                "breakeven_stop": False,
                "time_exit": False,
                "est12": False,
            }

        if day > rec_day:
            if low is not None and low <= stop_price:
                return {
                    "return_pct": EST12_STOP_PCT,
                    "holding_days": day,
                    "exit_reason": "emergency_stop",
                    "target_hit": False,
                    "breakeven_stop": False,
                    "time_exit": False,
                    "est12": True,
                }
            if low is not None and low <= entry:
                return {
                    "return_pct": 0.0,
                    "holding_days": day,
                    "exit_reason": "breakeven_stop",
                    "target_hit": False,
                    "breakeven_stop": True,
                    "time_exit": False,
                    "est12": False,
                }
            if trailing_pct is not None and peak > entry:
                trail_stop = peak * (1 - trailing_pct / 100.0)
                if low is not None and low <= max(entry, trail_stop):
                    exit_price = max(entry, trail_stop)
                    return {
                        "return_pct": _ret(exit_price, entry),
                        "holding_days": day,
                        "exit_reason": "trailing_stop",
                        "target_hit": False,
                        "breakeven_stop": exit_price <= entry,
                        "time_exit": False,
                        "est12": False,
                    }

    close_ret = _close_ret(path, entry, max_day)
    return {
        "return_pct": close_ret,
        "holding_days": max_day if close_ret is not None else None,
        "exit_reason": "time_exit" if close_ret is not None else "no_data",
        "target_hit": False,
        "breakeven_stop": False,
        "time_exit": close_ret is not None,
        "est12": False,
    }


def _policy_comparison(recovered: list[dict], periods: list[str]) -> list[dict]:
    policies = [
        "sell_at_breakeven",
        "hold_to_hd7_after_recovery",
        "hold_to_hd10_after_recovery",
        "target_plus1_or_breakeven_stop_hd7",
        "target_plus2_or_breakeven_stop_hd7",
        "target_plus3_or_breakeven_stop_hd7",
        "target_plus1_or_breakeven_stop_hd10",
        "target_plus2_or_breakeven_stop_hd10",
        "target_plus3_or_breakeven_stop_hd10",
        "trailing_after_breakeven_2pct_hd7",
    ]
    out: list[dict] = []
    groups = sorted({r["group"] for r in recovered})
    for group in groups:
        for period in periods:
            base = _subset(recovered, group, period)
            for policy in policies:
                sims = [_simulate_after_policy(r, policy) for r in base]
                sims = [s for s in sims if s.get("return_pct") is not None]
                vals = [s["return_pct"] for s in sims]
                avg_ret = _avg(vals)
                pf = _pf(vals)
                judgment = "WATCH"
                if policy == "sell_at_breakeven":
                    judgment = "BASE"
                elif avg_ret is not None and avg_ret > 0 and (pf is None or pf >= 1.0) and sum(1 for s in sims if s.get("est12")) <= max(2, len(sims) * 0.01):
                    judgment = "PASS"
                elif avg_ret is not None and avg_ret < 0:
                    judgment = "FAIL"
                out.append({
                    "policy": policy,
                    "group": group,
                    "period": period,
                    "n": len(sims),
                    "avg_return_pct": avg_ret,
                    "median_return_pct": _median(vals),
                    "win_rate": _wr(vals),
                    "profit_factor": pf,
                    "total_return_sum": sum(vals) if vals else None,
                    "avg_holding_days": _avg([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                    "median_holding_days": _median([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                    "target_hit_count": sum(1 for s in sims if s.get("target_hit")),
                    "target_hit_rate": _pct(sum(1 for s in sims if s.get("target_hit")), len(sims)),
                    "breakeven_stop_count": sum(1 for s in sims if s.get("breakeven_stop")),
                    "breakeven_stop_rate": _pct(sum(1 for s in sims if s.get("breakeven_stop")), len(sims)),
                    "time_exit_count": sum(1 for s in sims if s.get("time_exit")),
                    "time_exit_rate": _pct(sum(1 for s in sims if s.get("time_exit")), len(sims)),
                    "est12_count": sum(1 for s in sims if s.get("est12")),
                    "est12_rate": _pct(sum(1 for s in sims if s.get("est12")), len(sims)),
                    "avg_return_vs_sell_at_breakeven": avg_ret,
                    "delta_pf_vs_sell_at_breakeven": pf,
                    "judgment": judgment,
                })
    return out


def _rebreak_summary(recovered: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in recovered}):
        for period in periods:
            rows = _subset(recovered, group, period)
            out.append({
                "group": group,
                "period": period,
                "recovered_n": len(rows),
                "rebreak_minus1_low_n": sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -1),
                "rebreak_minus1_low_rate": _pct(sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -1), len(rows)),
                "rebreak_minus2_low_n": sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -2),
                "rebreak_minus2_low_rate": _pct(sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -2), len(rows)),
                "rebreak_minus3_low_n": sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -3),
                "rebreak_minus3_low_rate": _pct(sum(1 for r in rows if r["after_recovery_low_min_pct"] is not None and r["after_recovery_low_min_pct"] <= -3), len(rows)),
                "rebreak_minus1_close_n": sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -1),
                "rebreak_minus1_close_rate": _pct(sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -1), len(rows)),
                "rebreak_minus2_close_n": sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -2),
                "rebreak_minus2_close_rate": _pct(sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -2), len(rows)),
                "rebreak_minus3_close_n": sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -3),
                "rebreak_minus3_close_rate": _pct(sum(1 for r in rows if r["after_recovery_close_min_pct"] is not None and r["after_recovery_close_min_pct"] <= -3), len(rows)),
                "avg_min_low_after_recovery_pct": _avg([r["after_recovery_low_min_pct"] for r in rows if r.get("after_recovery_low_min_pct") is not None]),
                "avg_min_close_after_recovery_pct": _avg([r["after_recovery_close_min_pct"] for r in rows if r.get("after_recovery_close_min_pct") is not None]),
                "notes": "low metrics include recovery day; OHLC order is unknown",
            })
    return out


def _gain_bucket(v: float | None) -> str:
    if v is None:
        return "null"
    if v < 1:
        return "0_to_1"
    if v < 2:
        return "1_to_2"
    if v < 3:
        return "2_to_3"
    if v < 5:
        return "3_to_5"
    return "gte_5"


def _gain_distribution(recovered: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in recovered}):
        for period in periods:
            rows = _subset(recovered, group, period)
            buckets: dict[str, list[dict]] = defaultdict(list)
            for r in rows:
                buckets[_gain_bucket(r.get("after_recovery_high_max_pct"))].append(r)
            for bucket in ["0_to_1", "1_to_2", "2_to_3", "3_to_5", "gte_5", "null"]:
                bucket_rows = buckets.get(bucket, [])
                out.append({
                    "group": group,
                    "period": period,
                    "gain_bucket": bucket,
                    "n": len(bucket_rows),
                    "pct": _pct(len(bucket_rows), len(rows)),
                    "avg_hd3_return_pct": _avg([r["hd3_return_pct"] for r in bucket_rows if r.get("hd3_return_pct") is not None]),
                    "avg_recover_day": _avg([r["recover_day_high"] for r in bucket_rows if r.get("recover_day_high") is not None]),
                    "avg_hd10_close_return_pct": _avg([r["hd10_close_return_pct"] for r in bucket_rows if r.get("hd10_close_return_pct") is not None]),
                })
    return out


def _recovery_day_behavior(recovered: list[dict], periods: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in recovered}):
        for period in periods:
            rows = _subset(recovered, group, period)
            buckets: dict[str, list[dict]] = defaultdict(list)
            for r in rows:
                buckets[f"day{r['recover_day_high']}"].append(r)
            for day in range(4, 11):
                bucket_rows = buckets.get(f"day{day}", [])
                out.append({
                    "group": group,
                    "period": period,
                    "recover_day": f"day{day}",
                    "recovered_n": len(bucket_rows),
                    "plus1_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus1"]), len(bucket_rows)),
                    "plus2_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus2"]), len(bucket_rows)),
                    "plus3_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus3"]), len(bucket_rows)),
                    "rebreak_minus1_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_rebreak_minus1"]), len(bucket_rows)),
                    "hd7_close_positive_rate": _pct(sum(1 for r in bucket_rows if r.get("hd7_close_return_pct") is not None and r["hd7_close_return_pct"] > 0), len(bucket_rows)),
                    "hd10_close_positive_rate": _pct(sum(1 for r in bucket_rows if r.get("hd10_close_return_pct") is not None and r["hd10_close_return_pct"] > 0), len(bucket_rows)),
                    "avg_after_recovery_max_gain_pct": _avg([r["after_recovery_high_max_pct"] for r in bucket_rows if r.get("after_recovery_high_max_pct") is not None]),
                    "avg_hd10_close_return_pct": _avg([r["hd10_close_return_pct"] for r in bucket_rows if r.get("hd10_close_return_pct") is not None]),
                })
    return out


def _recover_day_bucket(day: int | None) -> str:
    if day is None:
        return "null"
    if day == 4:
        return "day4"
    if day == 5:
        return "day5"
    if day <= 7:
        return "day6_7"
    return "day8_10"


def _feature_breakdown(recovered: list[dict], periods: list[str], policy_rows: list[dict]) -> list[dict]:
    specs = [
        ("recover_day_bucket", lambda r: _recover_day_bucket(r.get("recover_day_high"))),
        ("hd3_return_bucket", lambda r: _bucket_hd3_loss(r.get("hd3_return_pct"))),
        ("signal_probability_bucket", lambda r: _bucket_probability(r.get("signal_probability"))),
        ("market_regime", lambda r: str(r.get("market_regime") or "null")),
        ("overheat_score", lambda r: str(r.get("overheat_score") if r.get("overheat_score") is not None else "null")),
        ("margin_ratio_bucket", lambda r: _bucket_margin(r.get("margin_ratio"))),
        ("drop20_bucket", lambda r: _bucket_drop(r.get("drop_from_20d_high_pct"))),
        ("volume_ratio_bucket", lambda r: _bucket_volume(r.get("volume_ratio"))),
    ]
    out: list[dict] = []
    for group in sorted({r["group"] for r in recovered}):
        for period in periods:
            rows = _subset(recovered, group, period)
            for feature, fn in specs:
                buckets: dict[str, list[dict]] = defaultdict(list)
                for r in rows:
                    buckets[fn(r)].append(r)
                for bucket, bucket_rows in sorted(buckets.items()):
                    if not bucket_rows:
                        continue
                    policy_avgs = {}
                    for policy in [
                        "sell_at_breakeven",
                        "target_plus1_or_breakeven_stop_hd7",
                        "target_plus2_or_breakeven_stop_hd7",
                        "hold_to_hd7_after_recovery",
                        "hold_to_hd10_after_recovery",
                    ]:
                        sims = [_simulate_after_policy(r, policy) for r in bucket_rows]
                        vals = [s["return_pct"] for s in sims if s.get("return_pct") is not None]
                        policy_avgs[policy] = _avg(vals)
                    best_policy = max(policy_avgs, key=lambda k: policy_avgs[k] if policy_avgs[k] is not None else -999)
                    out.append({
                        "feature": feature,
                        "bucket": bucket,
                        "group": group,
                        "period": period,
                        "recovered_n": len(bucket_rows),
                        "plus1_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus1"]), len(bucket_rows)),
                        "plus2_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus2"]), len(bucket_rows)),
                        "plus3_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_plus3"]), len(bucket_rows)),
                        "rebreak_minus1_rate": _pct(sum(1 for r in bucket_rows if r["after_recovery_rebreak_minus1"]), len(bucket_rows)),
                        "avg_hold_to_hd7_return": policy_avgs.get("hold_to_hd7_after_recovery"),
                        "avg_hold_to_hd10_return": policy_avgs.get("hold_to_hd10_after_recovery"),
                        "avg_sell_at_be_return": 0.0,
                        "best_policy": best_policy,
                        "judgment": "PASS" if (policy_avgs.get(best_policy) or 0) > 0 else "WATCH",
                    })
    return out


def _monthly_stability(recovered: list[dict]) -> list[dict]:
    policies = [
        "sell_at_breakeven",
        "target_plus1_or_breakeven_stop_hd7",
        "target_plus2_or_breakeven_stop_hd7",
        "hold_to_hd7_after_recovery",
        "hold_to_hd10_after_recovery",
    ]
    out: list[dict] = []
    for group in sorted({r["group"] for r in recovered}):
        group_rows = [r for r in recovered if r["group"] == group]
        months = sorted({r["trade_date"][:7] for r in group_rows})
        for month in months:
            rows = [r for r in group_rows if r["trade_date"].startswith(month)]
            for policy in policies:
                sims = [_simulate_after_policy(r, policy) for r in rows]
                sims = [s for s in sims if s.get("return_pct") is not None]
                vals = [s["return_pct"] for s in sims]
                out.append({
                    "policy": policy,
                    "group": group,
                    "month": month,
                    "n": len(sims),
                    "avg_return_pct": _avg(vals),
                    "win_rate": _wr(vals),
                    "pf": _pf(vals),
                    "target_hit_rate": _pct(sum(1 for s in sims if s.get("target_hit")), len(sims)),
                    "rebreak_rate": _pct(sum(1 for s in sims if s.get("breakeven_stop")), len(sims)),
                    "est12_rate": _pct(sum(1 for s in sims if s.get("est12")), len(sims)),
                    "avg_holding_days": _avg([s["holding_days"] for s in sims if s.get("holding_days") is not None]),
                })
    return out


def _group_comparison(rows: list[dict], recovered: list[dict], policy_rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        base = [r for r in rows if r["group"] == group]
        losers = [r for r in base if r.get("hd3_return_pct") is not None and r["hd3_return_pct"] < 0]
        rec = [r for r in recovered if r["group"] == group]
        policies = [
            r for r in policy_rows
            if r["group"] == group and r["period"] == "all" and r["policy"] != "sell_at_breakeven"
            and r.get("avg_return_pct") is not None
        ]
        best = max(policies, key=lambda r: r["avg_return_pct"], default={})
        out.append({
            "group": group,
            "hd3_loser_n": len(losers),
            "recovered_n": len(rec),
            "recovered_rate": _pct(len(rec), len(losers)),
            "plus1_rate": _pct(sum(1 for r in rec if r["after_recovery_plus1"]), len(rec)),
            "plus2_rate": _pct(sum(1 for r in rec if r["after_recovery_plus2"]), len(rec)),
            "plus3_rate": _pct(sum(1 for r in rec if r["after_recovery_plus3"]), len(rec)),
            "rebreak_minus1_rate": _pct(sum(1 for r in rec if r["after_recovery_rebreak_minus1"]), len(rec)),
            "best_policy": best.get("policy"),
            "best_policy_avg_return": best.get("avg_return_pct"),
            "sell_at_breakeven_avg_return": 0.0,
            "delta": best.get("avg_return_pct"),
            "judgment": best.get("judgment"),
        })
    return out


def _skipped_summary(*groups: list[dict]) -> list[dict]:
    counts = defaultdict(int)
    for rows in groups:
        for row in rows:
            counts[row["reason"]] += int(row.get("count") or 1)
    return [{"reason": reason, "count": count, "notes": ""} for reason, count in sorted(counts.items())]


def _report(
    *,
    summary: list[dict],
    policies: list[dict],
    rebreak: list[dict],
    group_comp: list[dict],
    score_source: str,
    start: date,
    end: date,
) -> str:
    main = next((r for r in summary if r["group"] == "Research_ALL" and r["period"] == "all"), {})
    main_rebreak = next((r for r in rebreak if r["group"] == "Research_ALL" and r["period"] == "all"), {})
    policy_all = [r for r in policies if r["group"] == "Research_ALL" and r["period"] == "all"]
    policy_train = [r for r in policies if r["group"] == "Research_ALL" and r["period"] == "train"]
    policy_test = [r for r in policies if r["group"] == "Research_ALL" and r["period"] == "test"]
    best = max(
        [r for r in policy_all if r["policy"] != "sell_at_breakeven" and r.get("avg_return_pct") is not None],
        key=lambda r: r["avg_return_pct"],
        default={},
    )
    best_train = max(
        [r for r in policy_train if r["policy"] != "sell_at_breakeven" and r.get("avg_return_pct") is not None],
        key=lambda r: r["avg_return_pct"],
        default={},
    )
    best_test = max(
        [r for r in policy_test if r["policy"] != "sell_at_breakeven" and r.get("avg_return_pct") is not None],
        key=lambda r: r["avg_return_pct"],
        default={},
    )

    lines = [
        "H5 Breakeven After-Recovery Analysis",
        "=" * 44,
        "",
        f"period: {start.isoformat()} .. {end.isoformat()}",
        f"score_source: {score_source}",
        "note: high-based recovery/targets assume resting limit orders. This is not a Primary change.",
        "",
        "1. Research_ALL recovered population",
        f"- hd3_loser_n: {main.get('hd3_loser_n')}",
        f"- recovered_by_hd10_n: {main.get('recovered_by_hd10_n')}",
        f"- recovered_by_hd10_rate: {_round(main.get('recovered_by_hd10_rate'), 2)}%",
        "",
        "2. After-recovery upside",
        f"- plus1_rate: {_round(main.get('after_recovery_plus1_rate'), 2)}%",
        f"- plus2_rate: {_round(main.get('after_recovery_plus2_rate'), 2)}%",
        f"- plus3_rate: {_round(main.get('after_recovery_plus3_rate'), 2)}%",
        f"- plus5_rate: {_round(main.get('after_recovery_plus5_rate'), 2)}%",
        f"- avg_max_gain_after_recovery: {_round(main.get('avg_after_recovery_max_gain_pct'), 3)}%",
        "",
        "3. Rebreak risk",
        f"- low rebreak -1%: {_round(main_rebreak.get('rebreak_minus1_low_rate'), 2)}%",
        f"- low rebreak -2%: {_round(main_rebreak.get('rebreak_minus2_low_rate'), 2)}%",
        f"- low rebreak -3%: {_round(main_rebreak.get('rebreak_minus3_low_rate'), 2)}%",
        f"- close rebreak -1%: {_round(main_rebreak.get('rebreak_minus1_close_rate'), 2)}%",
        "",
        "4. Close-positive persistence",
        f"- hd7_close_positive_rate: {_round(main.get('hd7_close_positive_rate'), 2)}%",
        f"- hd10_close_positive_rate: {_round(main.get('hd10_close_positive_rate'), 2)}%",
        f"- avg_hd7_close_return_after_recovery: {_round(main.get('avg_hd7_close_return_after_recovery'), 3)}%",
        f"- avg_hd10_close_return_after_recovery: {_round(main.get('avg_hd10_close_return_after_recovery'), 3)}%",
        "",
        "5. Policy comparison vs sell_at_breakeven",
        "- sell_at_breakeven baseline return is 0%.",
        f"- best_all: {best.get('policy')} avg={_round(best.get('avg_return_pct'), 4)}% PF={_round(best.get('profit_factor'), 4)} judgment={best.get('judgment')}",
        f"- best_train: {best_train.get('policy')} avg={_round(best_train.get('avg_return_pct'), 4)}% judgment={best_train.get('judgment')}",
        f"- best_test: {best_test.get('policy')} avg={_round(best_test.get('avg_return_pct'), 4)}% judgment={best_test.get('judgment')}",
        "",
        "6. Interpretation",
    ]
    if best.get("avg_return_pct") is not None and best["avg_return_pct"] > 0:
        lines.append("- Some after-recovery upside exists; immediate breakeven exit is conservative but may leave money on the table.")
    else:
        lines.append("- After-recovery holding did not beat immediate breakeven exit on average.")
    lines.extend([
        "- Because intraday high/low order is unknown, target and stop policies should be treated as research approximations.",
        "- Primary should not be changed from this analysis alone.",
        "- If used operationally, start as a comparison rule with a resting breakeven/target order design.",
        "",
        "7. Group comparison",
    ])
    for row in group_comp:
        lines.append(
            f"- {row.get('group')}: losers={row.get('hd3_loser_n')} recovered={row.get('recovered_n')} "
            f"plus1={_round(row.get('plus1_rate'), 2)} best={row.get('best_policy')} "
            f"avg={_round(row.get('best_policy_avg_return'), 4)} judgment={row.get('judgment')}"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze H5 behavior after breakeven recovery")
    parser.add_argument("--output-dir", default="outputs/h5_after_breakeven_recovery")
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
    rows, skipped_base = _build_dataset(candidates, train_end, test_start)
    recovered, skipped_recovered = _build_after_recovery_dataset(rows)

    summary = _summary(rows, recovered, periods)
    policies = _policy_comparison(recovered, periods)
    rebreak = _rebreak_summary(recovered, periods)
    gain_dist = _gain_distribution(recovered, periods)
    day_behavior = _recovery_day_behavior(recovered, periods)
    features = _feature_breakdown(recovered, periods, policies)
    monthly = _monthly_stability(recovered)
    group_comp = _group_comparison(rows, recovered, policies)
    skipped = _skipped_summary(skipped_base, skipped_recovered)

    public_detail = [{k: v for k, v in r.items() if not k.startswith("_")} for r in recovered]
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
after_recovery_detail_rows: {len(recovered)}

This is analysis-only. It does not modify Primary, DB case definitions, UI,
LINE notifications, actual_trade_logs, Watchlist, or Intraday H5.
""")
    _write_csv(output_dir / "01_after_recovery_summary.csv", summary)
    _write_csv(output_dir / "02_after_recovery_detail.csv", public_detail)
    _write_csv(output_dir / "03_after_recovery_policy_comparison.csv", policies)
    _write_csv(output_dir / "04_after_recovery_rebreak_summary.csv", rebreak)
    _write_csv(output_dir / "05_after_recovery_gain_distribution.csv", gain_dist)
    _write_csv(output_dir / "06_recovery_day_after_behavior.csv", day_behavior)
    _write_csv(output_dir / "07_after_recovery_feature_breakdown.csv", features)
    _write_csv(output_dir / "08_after_recovery_monthly_stability.csv", monthly)
    _write_csv(output_dir / "09_group_comparison.csv", group_comp)
    _write_csv(output_dir / "10_skipped_rows_summary.csv", skipped)
    _write_text(
        output_dir / "11_after_recovery_report.txt",
        _report(
            summary=summary,
            policies=policies,
            rebreak=rebreak,
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
            and r["policy"] != "sell_at_breakeven"
            and r.get("avg_return_pct") is not None
        ],
        key=lambda r: r["avg_return_pct"],
        default={},
    )
    print(f"loaded_candidates={len(candidates)}")
    print(f"analysis_rows={len(rows)}")
    print(f"after_recovery_rows={len(recovered)}")
    print(f"research_all_recovered={main_summary.get('recovered_by_hd10_n')}")
    print(f"plus1_rate={_round(main_summary.get('after_recovery_plus1_rate'), 2)}")
    print(f"best_policy={best.get('policy')} avg={_round(best.get('avg_return_pct'), 4)} judgment={best.get('judgment')}")
    print(f"output_dir={output_dir}")


if __name__ == "__main__":
    main()
