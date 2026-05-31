"""H5 Extension Survival Analysis.

Analyze only trades where the research extension would trigger:
day3_return <= -1%, no peak pullback, EST12 emergency stop, and HD5 extension.

This script is research-only. It writes CSV/TXT files under
outputs/h5_extension_survival and does not modify DB state.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

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


Condition = tuple[str, Callable[[dict], bool]]


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


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


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


def _month(value: Any) -> str:
    text = str(value or "")
    return text[:7] if len(text) >= 7 else "unknown"


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


def _day(row: dict, day: int) -> dict:
    close = _to_float(row.get(f"future_close_{day}d"), None)
    high = _to_float(row.get(f"future_high_{day}d"), None)
    low = _to_float(row.get(f"future_low_{day}d"), None)
    prev_close = (
        _to_float(row.get("entry_price"), None)
        or _to_float(row.get("close"), None)
        if day == 1
        else _to_float(row.get(f"future_close_{day - 1}d"), None)
    )
    return {"day": day, "close": close, "high": high, "low": low, "open_proxy": prev_close}


def _ret(price: float | None, entry: float | None) -> float | None:
    return _pct(price, entry)


def _simulate_hd(row: dict, hold_days: int, stop_pct: float = -0.12) -> dict:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    if not entry:
        return {"ret": None, "exit_reason": "invalid_entry", "holding_days": None}
    stop_price = entry * (1.0 + stop_pct)
    last_close = None
    for day in range(1, hold_days + 1):
        d = _day(row, day)
        low = d["low"]
        close = d["close"]
        if close is not None:
            last_close = close
        if low is not None and low <= stop_price:
            return {
                "ret": stop_pct * 100.0,
                "exit_price": stop_price,
                "exit_reason": "emergency_stop",
                "holding_days": day,
            }
    if last_close is None:
        return {"ret": None, "exit_reason": "no_data", "holding_days": None}
    return {
        "ret": (last_close / entry - 1.0) * 100.0,
        "exit_price": last_close,
        "exit_reason": "time_stop",
        "holding_days": hold_days,
    }


def _extension_class(benefit: float | None, hd5_exit_reason: str) -> str:
    if hd5_exit_reason == "emergency_stop":
        return "emergency_died"
    if benefit is None:
        return "unknown"
    if benefit >= 1.0:
        return "strong_recovered"
    if benefit > 0.3:
        return "recovered"
    if benefit >= -0.3:
        return "flat"
    if benefit <= -1.0:
        return "strong_died"
    return "died"


def _entry_rsi_bucket(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 20:
        return "lt20"
    if value < 30:
        return "20_30"
    if value < 40:
        return "30_40"
    if value < 50:
        return "40_50"
    return "gte50"


def _margin_bucket(value: float | None) -> str | None:
    if value is None:
        return "missing"
    if value < 3:
        return "lt3"
    if value <= 5:
        return "3_5"
    if value <= 10:
        return "5_10"
    if value <= 20:
        return "10_20"
    if value <= 30:
        return "20_30"
    return "gt30"


def _day3_features(row: dict, hd3_ret: float, hd5_ret: float, hd5_reason: str) -> dict:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    d1, d2, d3, d4, d5 = [_day(row, i) for i in range(1, 6)]
    c1, c2, c3 = d1["close"], d2["close"], d3["close"]
    h3, l3, o3 = d3["high"], d3["low"], d3["open_proxy"]
    range3 = (h3 - l3) if h3 is not None and l3 is not None else None
    body_pct = (abs(c3 - o3) / o3 * 100.0) if c3 is not None and o3 else None
    upper_shadow = ((h3 - max(c3, o3)) / c3 * 100.0) if h3 is not None and c3 and o3 else None
    lower_shadow = ((min(c3, o3) - l3) / c3 * 100.0) if l3 is not None and c3 and o3 else None
    close_pos = ((c3 - l3) / range3) if c3 is not None and l3 is not None and range3 and range3 > 0 else None
    daily3 = _pct(c3, c2)
    day1_ret = _ret(c1, entry)
    day2_ret = _ret(c2, entry)
    day1_daily = _pct(c1, entry)
    day2_daily = _pct(c2, c1)
    lows_12 = [v for v in [d1["low"], d2["low"]] if v is not None]
    lows_123 = [v for v in [d1["low"], d2["low"], d3["low"]] if v is not None]
    highs_45 = [v for v in [d4["high"], d5["high"]] if v is not None]
    lows_45 = [v for v in [d4["low"], d5["low"]] if v is not None]
    recent_low_5d = min(lows_12, default=None)
    close_below_recent_low = c3 is not None and recent_low_5d is not None and c3 < recent_low_5d
    low_below_recent_low = l3 is not None and recent_low_5d is not None and l3 < recent_low_5d
    close_recovered_recent_low = low_below_recent_low and c3 is not None and recent_low_5d is not None and c3 >= recent_low_5d
    margin = _to_float(row.get("margin_ratio"), None)
    volume_ratio = _to_float(row.get("volume_ratio_20d"), None)
    atr_pct = _to_float(row.get("atr_pct"), None)
    day3_range_pct = ((h3 - l3) / c3 * 100.0) if h3 is not None and l3 is not None and c3 else None
    max_adv = _pct(min(lows_45, default=None), c3)
    max_fav = _pct(max(highs_45, default=None), c3)
    benefit = hd5_ret - hd3_ret
    ext_class = _extension_class(benefit, hd5_reason)
    recovered = benefit > 0
    died = benefit < 0
    return {
        "day1_return": day1_ret,
        "day2_return": day2_ret,
        "day3_return": hd3_ret,
        "day3_return_daily": daily3,
        "day1_to_day3_return": hd3_ret,
        "day1_day2_day3_slope": ((hd3_ret - (day1_ret or 0)) / 2.0) if day1_ret is not None else None,
        "down_acceleration_flag": day1_daily is not None and day2_daily is not None and daily3 is not None and day1_daily < 0 and day2_daily < 0 and daily3 < 0 and daily3 < day2_daily,
        "consecutive_down_days": sum(1 for v in [day1_daily, day2_daily, daily3] if v is not None and v < 0),
        "day3_new_low_since_entry": l3 is not None and lows_123 and l3 <= min(lows_123),
        "day3_breaks_day1_low": l3 is not None and d1["low"] is not None and l3 < d1["low"],
        "day3_breaks_day2_low": l3 is not None and d2["low"] is not None and l3 < d2["low"],
        "day3_close": c3,
        "day3_high": h3,
        "day3_low": l3,
        "day3_open": o3,
        "day3_close_vs_entry_pct": hd3_ret,
        "day3_high_vs_entry_pct": _ret(h3, entry),
        "day3_low_vs_entry_pct": _ret(l3, entry),
        "day3_candle_body_pct": body_pct,
        "day3_upper_shadow_pct": upper_shadow,
        "day3_lower_shadow_pct": lower_shadow,
        "day3_close_position_in_range": close_pos,
        "day3_is_bullish": c3 is not None and o3 is not None and c3 >= o3,
        "day3_is_bearish": c3 is not None and o3 is not None and c3 < o3,
        "day3_is_doji_like": body_pct is not None and body_pct <= 0.3,
        "day3_long_lower_shadow": lower_shadow is not None and lower_shadow >= 1.0,
        "day3_long_upper_shadow": upper_shadow is not None and upper_shadow >= 1.0,
        "day3_close_near_high": close_pos is not None and close_pos >= 0.7,
        "day3_close_near_low": close_pos is not None and close_pos <= 0.3,
        "day3_ma5": row.get("ma5"),
        "day3_ma25": row.get("ma25"),
        "day3_ma75": row.get("ma75"),
        "day3_close_vs_ma5_pct": _pct(c3, _to_float(row.get("ma5"), None)),
        "day3_close_vs_ma25_pct": _pct(c3, _to_float(row.get("ma25"), None)),
        "day3_close_vs_ma75_pct": _pct(c3, _to_float(row.get("ma75"), None)),
        "day3_close_above_ma5": c3 is not None and _to_float(row.get("ma5"), None) is not None and c3 >= _to_float(row.get("ma5"), None),
        "day3_close_above_ma25": c3 is not None and _to_float(row.get("ma25"), None) is not None and c3 >= _to_float(row.get("ma25"), None),
        "day3_ma5_slope": None,
        "day3_ma25_slope": None,
        "day3_rsi": row.get("rsi14"),
        "day3_rsi_bucket": _entry_rsi_bucket(_to_float(row.get("rsi14"), None)),
        "rsi_change_entry_to_day3": None,
        "day3_volume": None,
        "day3_volume_ratio": volume_ratio,
        "day1_volume_ratio": volume_ratio,
        "day2_volume_ratio": volume_ratio,
        "volume_change_day1_to_day3": None,
        "day3_volume_spike": volume_ratio is not None and volume_ratio >= 1.5,
        "day3_volume_dry_up": volume_ratio is not None and volume_ratio <= 0.7,
        "recent_low_5d": recent_low_5d,
        "recent_low_10d": recent_low_5d,
        "day3_close_below_recent_low_5d": close_below_recent_low,
        "day3_close_below_recent_low_10d": close_below_recent_low,
        "day3_low_below_recent_low_5d": low_below_recent_low,
        "day3_low_below_recent_low_10d": low_below_recent_low,
        "day3_low_below_recent_low_5d_close_recovered": close_recovered_recent_low,
        "distance_to_recent_low_pct": _pct(c3, recent_low_5d),
        "support_break_flag": close_below_recent_low,
        "day3_market_regime": row.get("market_regime"),
        "day3_index_return": row.get("market_nikkei_pct"),
        "day1_to_day3_index_return": row.get("market_nikkei_pct"),
        "day3_sector_return": row.get("sector_return_pct"),
        "day1_to_day3_sector_return": row.get("sector_return_pct"),
        "sector_strength_bucket": row.get("sector_strength_bucket"),
        "market_regime_change_from_entry": None,
        "margin_ratio": margin,
        "margin_ratio_bucket": _margin_bucket(margin),
        "credit_bucket": _margin_bucket(margin),
        "liquidity": row.get("turnover_value") or row.get("liquidity"),
        "turnover": row.get("turnover_value") or row.get("liquidity"),
        "volume_value": row.get("turnover_value") or row.get("liquidity"),
        "market_cap": row.get("market_cap"),
        "atr_pct": atr_pct,
        "day3_atr_pct": atr_pct,
        "volatility_5d": None,
        "volatility_20d": None,
        "day3_range_pct": day3_range_pct,
        "day3_range_vs_atr": (day3_range_pct / atr_pct) if day3_range_pct is not None and atr_pct else None,
        "max_adverse_after_day3": max_adv,
        "max_favorable_after_day3": max_fav,
        "extension_benefit": benefit,
        "extension_class": ext_class,
        "recovered": recovered,
        "strong_recovered": benefit >= 1.0,
        "flat": -0.3 <= benefit <= 0.3,
        "died": died,
        "strong_died": benefit <= -1.0,
        "emergency_died": hd5_reason == "emergency_stop",
    }


def _build_extension_enabled_rows(candidates: list[dict]) -> list[dict]:
    rows = []
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry:
            continue
        hd3 = _simulate_hd(row, 3)
        if hd3.get("ret") is None or hd3.get("exit_reason") == "emergency_stop":
            continue
        if float(hd3["ret"]) > -1.0:
            continue
        hd5 = _simulate_hd(row, 5)
        if hd5.get("ret") is None:
            continue
        d3 = _day(row, 3)
        d5 = _day(row, 5)
        features = _day3_features(row, float(hd3["ret"]), float(hd5["ret"]), str(hd5.get("exit_reason") or ""))
        rows.append({
            "trade_date": str(row.get("trade_date")),
            "code": row.get("code"),
            "name": row.get("name"),
            "sector": row.get("sector"),
            "entry_date": str(row.get("trade_date")),
            "entry_price": entry,
            "day3_date": None,
            "day3_close": d3["close"],
            "day3_return": hd3["ret"],
            "hd5_date": None,
            "hd5_close": d5["close"],
            "hd5_return": hd5["ret"],
            "extension_benefit": float(hd5["ret"]) - float(hd3["ret"]),
            "extension_class": features["extension_class"],
            "exit_reason": hd5.get("exit_reason"),
            "emergency_stop_hit": hd5.get("exit_reason") == "emergency_stop",
            "signal_probability": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "drop_from_20d_high_pct": row.get("drop_from_20d_high_pct"),
            "entry_market_regime": row.get("market_regime"),
            **features,
        })
    return rows


def _summary(rows: list[dict]) -> dict:
    n = len(rows)
    benefits = [_to_float(r.get("extension_benefit"), 0.0) or 0.0 for r in rows]
    hd3 = [_to_float(r.get("day3_return"), 0.0) or 0.0 for r in rows]
    hd5 = [_to_float(r.get("hd5_return"), 0.0) or 0.0 for r in rows]
    return {
        "selected_count": n,
        "recovered_count": sum(1 for r in rows if r.get("recovered")),
        "died_count": sum(1 for r in rows if r.get("died")),
        "strong_recovered_count": sum(1 for r in rows if r.get("strong_recovered")),
        "strong_died_count": sum(1 for r in rows if r.get("strong_died")),
        "emergency_died_count": sum(1 for r in rows if r.get("emergency_died")),
        "recovered_rate": _round(sum(1 for r in rows if r.get("recovered")) / n * 100, 3) if n else None,
        "died_rate": _round(sum(1 for r in rows if r.get("died")) / n * 100, 3) if n else None,
        "avg_extension_benefit": _round(_avg(benefits), 4),
        "median_extension_benefit": _round(_median(benefits), 4),
        "avg_hd3_return": _round(_avg(hd3), 4),
        "avg_hd5_return": _round(_avg(hd5), 4),
        "PF_extension": _round(_pf(hd5), 4),
        "max_loss": _round(min(hd5), 4) if hd5 else None,
        "worst_case": _round(min(benefits), 4) if benefits else None,
        "best_case": _round(max(benefits), 4) if benefits else None,
    }


def _conditions() -> list[Condition]:
    return [
        ("day3_return_le_minus_1", lambda r: _to_float(r.get("day3_return"), 99) <= -1),
        ("day3_return_le_minus_2", lambda r: _to_float(r.get("day3_return"), 99) <= -2),
        ("day3_return_le_minus_3", lambda r: _to_float(r.get("day3_return"), 99) <= -3),
        ("day3_return_between_minus3_minus1", lambda r: -3 <= _to_float(r.get("day3_return"), 99) <= -1),
        ("day3_return_lt_minus5", lambda r: _to_float(r.get("day3_return"), 99) < -5),
        ("day3_close_position_ge_0_7", lambda r: _to_float(r.get("day3_close_position_in_range"), -1) >= 0.7),
        ("day3_close_position_ge_0_6", lambda r: _to_float(r.get("day3_close_position_in_range"), -1) >= 0.6),
        ("day3_close_position_le_0_3", lambda r: _to_float(r.get("day3_close_position_in_range"), 99) <= 0.3),
        ("day3_lower_shadow_ge_1", lambda r: _to_float(r.get("day3_lower_shadow_pct"), -1) >= 1),
        ("day3_lower_shadow_ge_2", lambda r: _to_float(r.get("day3_lower_shadow_pct"), -1) >= 2),
        ("day3_upper_shadow_ge_1", lambda r: _to_float(r.get("day3_upper_shadow_pct"), -1) >= 1),
        ("day3_is_bullish", lambda r: bool(r.get("day3_is_bullish"))),
        ("day3_is_bearish", lambda r: bool(r.get("day3_is_bearish"))),
        ("day3_close_near_low", lambda r: bool(r.get("day3_close_near_low"))),
        ("day3_close_near_high", lambda r: bool(r.get("day3_close_near_high"))),
        ("day3_close_above_ma5", lambda r: bool(r.get("day3_close_above_ma5"))),
        ("day3_close_vs_ma5_gt_minus1", lambda r: _to_float(r.get("day3_close_vs_ma5_pct"), -99) > -1),
        ("day3_close_vs_ma5_gt_minus2", lambda r: _to_float(r.get("day3_close_vs_ma5_pct"), -99) > -2),
        ("day3_close_vs_ma5_lt_minus3", lambda r: _to_float(r.get("day3_close_vs_ma5_pct"), 99) < -3),
        ("day3_close_above_ma25", lambda r: bool(r.get("day3_close_above_ma25"))),
        ("day3_close_vs_ma25_lt_minus5", lambda r: _to_float(r.get("day3_close_vs_ma25_pct"), 99) < -5),
        ("day3_rsi_lt_20", lambda r: _to_float(r.get("day3_rsi"), 99) < 20),
        ("day3_rsi_lt_30", lambda r: _to_float(r.get("day3_rsi"), 99) < 30),
        ("day3_rsi_20_35", lambda r: 20 <= _to_float(r.get("day3_rsi"), -1) <= 35),
        ("day3_rsi_gte_40", lambda r: _to_float(r.get("day3_rsi"), -1) >= 40),
        ("day3_volume_ratio_ge_1_2", lambda r: _to_float(r.get("day3_volume_ratio"), -1) >= 1.2),
        ("day3_volume_ratio_ge_1_5", lambda r: _to_float(r.get("day3_volume_ratio"), -1) >= 1.5),
        ("day3_volume_ratio_ge_2_0", lambda r: _to_float(r.get("day3_volume_ratio"), -1) >= 2.0),
        ("day3_volume_spike", lambda r: bool(r.get("day3_volume_spike"))),
        ("day3_volume_dry_up", lambda r: bool(r.get("day3_volume_dry_up"))),
        ("support_break_flag", lambda r: bool(r.get("support_break_flag"))),
        ("day3_close_below_recent_low_5d", lambda r: bool(r.get("day3_close_below_recent_low_5d"))),
        ("day3_low_below_recent_low_5d", lambda r: bool(r.get("day3_low_below_recent_low_5d"))),
        ("day3_low_below_recent_low_5d_close_recovered", lambda r: bool(r.get("day3_low_below_recent_low_5d_close_recovered"))),
        ("day3_regime_normal_or_risk_on", lambda r: str(r.get("day3_market_regime") or "") in {"normal", "risk_on", "strong_risk_on"}),
        ("day3_index_return_gt_0", lambda r: _to_float(r.get("day3_index_return"), -99) > 0),
        ("day3_index_return_lt_minus1", lambda r: _to_float(r.get("day3_index_return"), 99) < -1),
        ("day3_sector_return_gt_0", lambda r: _to_float(r.get("day3_sector_return"), -99) > 0),
        ("day3_sector_return_lt_minus1", lambda r: _to_float(r.get("day3_sector_return"), 99) < -1),
        ("margin_ratio_lte_10", lambda r: r.get("margin_ratio") is not None and _to_float(r.get("margin_ratio"), 999) <= 10),
        ("margin_ratio_3_30", lambda r: r.get("margin_ratio") is None or 3 <= _to_float(r.get("margin_ratio"), 0) <= 30),
        ("margin_ratio_gt_30", lambda r: r.get("margin_ratio") is not None and _to_float(r.get("margin_ratio"), 0) > 30),
        ("day3_range_pct_ge_atr", lambda r: r.get("day3_range_vs_atr") is not None and _to_float(r.get("day3_range_vs_atr"), 0) >= 1),
        ("day3_range_pct_ge_1_5atr", lambda r: r.get("day3_range_vs_atr") is not None and _to_float(r.get("day3_range_vs_atr"), 0) >= 1.5),
    ]


def _condition_result(rows: list[dict], selected: list[dict], condition: str, period: str, condition_count: int) -> dict:
    rest = [r for r in rows if id(r) not in {id(x) for x in selected}]
    sm = _summary(selected)
    rest_sm = _summary(rest)
    return {
        "period": period,
        "condition": condition,
        "condition_count": condition_count,
        "total_count": len(rows),
        "selected_rate": _round(len(selected) / len(rows) * 100, 3) if rows else None,
        **sm,
        "nonselected_count": len(rest),
        "nonselected_avg_extension_benefit": rest_sm["avg_extension_benefit"],
        "avg_benefit_diff_vs_nonselected": _round((sm["avg_extension_benefit"] or 0) - (rest_sm["avg_extension_benefit"] or 0), 4),
        "recovered_rate_diff_vs_nonselected": _round((sm["recovered_rate"] or 0) - (rest_sm["recovered_rate"] or 0), 4),
        "died_rate_diff_vs_nonselected": _round((sm["died_rate"] or 0) - (rest_sm["died_rate"] or 0), 4),
    }


def _condition_results(rows: list[dict], conds: list[Condition], period: str, min_count: int, max_combo: int = 1) -> list[dict]:
    out: list[dict] = []
    for size in range(1, max_combo + 1):
        for combo in itertools.combinations(conds, size):
            names = [c[0] for c in combo]
            funcs = [c[1] for c in combo]
            selected = [r for r in rows if all(fn(r) for fn in funcs)]
            if len(selected) < min_count:
                continue
            out.append(_condition_result(rows, selected, " AND ".join(names), period, size))
    return out


def _merge_train_test(train_rows: list[dict], test_rows: list[dict]) -> list[dict]:
    train_by_cond = {r["condition"]: r for r in train_rows}
    out = []
    for test in test_rows:
        train = train_by_cond.get(test["condition"])
        if not train:
            continue
        out.append({
            "condition": test["condition"],
            "condition_count": test["condition_count"],
            "train_total_count": train["total_count"],
            "test_total_count": test["total_count"],
            "train_selected_count": train["selected_count"],
            "test_selected_count": test["selected_count"],
            "train_avg_extension_benefit": train["avg_extension_benefit"],
            "test_avg_extension_benefit": test["avg_extension_benefit"],
            "train_recovered_rate": train["recovered_rate"],
            "test_recovered_rate": test["recovered_rate"],
            "train_died_rate": train["died_rate"],
            "test_died_rate": test["died_rate"],
            "train_emergency_died_count": train["emergency_died_count"],
            "test_emergency_died_count": test["emergency_died_count"],
            "train_benefit_diff_vs_nonselected": train["avg_benefit_diff_vs_nonselected"],
            "test_benefit_diff_vs_nonselected": test["avg_benefit_diff_vs_nonselected"],
            "train_died_rate_diff_vs_nonselected": train["died_rate_diff_vs_nonselected"],
            "test_died_rate_diff_vs_nonselected": test["died_rate_diff_vs_nonselected"],
        })
    return out


def _top_recovery_rules(train_rows: list[dict], test_rows: list[dict]) -> list[dict]:
    merged = _merge_train_test(train_rows, test_rows)
    for row in merged:
        is_subgroup = (
            _to_float(row.get("train_selected_count"), 0) < _to_float(row.get("train_total_count"), 0) * 0.9
            and _to_float(row.get("test_selected_count"), 0) < _to_float(row.get("test_total_count"), 0) * 0.9
        )
        row["recovery_score"] = _round(
            (_to_float(row.get("train_benefit_diff_vs_nonselected"), 0) + _to_float(row.get("test_benefit_diff_vs_nonselected"), 0)) * 10
            + (_to_float(row.get("train_recovered_rate"), 0) + _to_float(row.get("test_recovered_rate"), 0)) / 5
            - (_to_float(row.get("train_died_rate"), 0) + _to_float(row.get("test_died_rate"), 0)) / 8
            + min(_to_float(row.get("test_selected_count"), 0), 100) / 10,
            4,
        )
        row["candidate"] = (
            is_subgroup
            and
            _to_float(row.get("train_benefit_diff_vs_nonselected"), 0) > 0
            and _to_float(row.get("test_benefit_diff_vs_nonselected"), 0) > 0
            and _to_float(row.get("test_selected_count"), 0) >= 10
        )
    return sorted(merged, key=lambda r: (_to_float(r.get("candidate"), 0), _to_float(r.get("recovery_score"), 0)), reverse=True)


def _top_death_rules(train_rows: list[dict], test_rows: list[dict]) -> list[dict]:
    merged = _merge_train_test(train_rows, test_rows)
    for row in merged:
        is_subgroup = (
            _to_float(row.get("train_selected_count"), 0) < _to_float(row.get("train_total_count"), 0) * 0.9
            and _to_float(row.get("test_selected_count"), 0) < _to_float(row.get("test_total_count"), 0) * 0.9
        )
        row["death_score"] = _round(
            (_to_float(row.get("train_died_rate_diff_vs_nonselected"), 0) + _to_float(row.get("test_died_rate_diff_vs_nonselected"), 0))
            - (_to_float(row.get("train_benefit_diff_vs_nonselected"), 0) + _to_float(row.get("test_benefit_diff_vs_nonselected"), 0)) * 5
            + min(_to_float(row.get("test_selected_count"), 0), 100) / 20,
            4,
        )
        row["candidate"] = (
            is_subgroup
            and
            _to_float(row.get("train_died_rate_diff_vs_nonselected"), 0) > 0
            and _to_float(row.get("test_died_rate_diff_vs_nonselected"), 0) > 0
            and _to_float(row.get("test_selected_count"), 0) >= 10
        )
    return sorted(merged, key=lambda r: (_to_float(r.get("candidate"), 0), _to_float(r.get("death_score"), 0)), reverse=True)


def _monthly(rows: list[dict]) -> list[dict]:
    by_month: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_month[_month(row.get("trade_date"))].append(row)
    out = []
    for month, group in sorted(by_month.items()):
        out.append({"month": month, **_summary(group)})
    return out


def _sample(rows: list[dict], reverse: bool, n: int = 100) -> list[dict]:
    return sorted(rows, key=lambda r: _to_float(r.get("extension_benefit"), 0), reverse=reverse)[:n]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows([{k: _round(v) for k, v in row.items()} for row in rows])


def _report(path: Path, all_rows: list[dict], train_rows: list[dict], test_rows: list[dict], recovery: list[dict], death: list[dict]) -> None:
    all_sm = _summary(all_rows)
    test_sm = _summary(test_rows)
    rec = [r for r in all_rows if r.get("recovered")]
    died = [r for r in all_rows if r.get("died")]
    top_rec = recovery[0] if recovery else {}
    top_death = death[0] if death else {}
    lines = [
        "H5 Extension Survival Analysis Report",
        "",
        "Scope: extension-enabled trades only (day3_return <= -1%). Primary remains HD3 + EST12.",
        "Note: future labels contain high/low/close. Day3 open uses previous close as a proxy; future volume/RSI/MA use entry-time proxies where unavailable.",
        "",
        "[Overall]",
        f"extension_enabled_all={len(all_rows)}",
        f"recovered={sum(1 for r in all_rows if r.get('recovered'))}",
        f"died={sum(1 for r in all_rows if r.get('died'))}",
        f"strong_recovered={sum(1 for r in all_rows if r.get('strong_recovered'))}",
        f"strong_died={sum(1 for r in all_rows if r.get('strong_died'))}",
        f"emergency_died={sum(1 for r in all_rows if r.get('emergency_died'))}",
        f"avg_extension_benefit={all_sm.get('avg_extension_benefit')}",
        "",
        "[Test]",
        f"test_count={len(test_rows)} recovered_rate={test_sm.get('recovered_rate')} died_rate={test_sm.get('died_rate')} avg_benefit={test_sm.get('avg_extension_benefit')}",
        "",
        "[Recovery characteristics]",
        f"avg_day3_return_recovered={_round(_avg([_to_float(r.get('day3_return'), 0) for r in rec]), 4)}",
        f"avg_close_position_recovered={_round(_avg([_to_float(r.get('day3_close_position_in_range'), 0) for r in rec if r.get('day3_close_position_in_range') is not None]), 4)}",
        f"top_recovery_rule={top_rec.get('condition')} test_benefit_diff={top_rec.get('test_benefit_diff_vs_nonselected')} candidate={top_rec.get('candidate')}",
        "",
        "[Death characteristics]",
        f"avg_day3_return_died={_round(_avg([_to_float(r.get('day3_return'), 0) for r in died]), 4)}",
        f"avg_close_position_died={_round(_avg([_to_float(r.get('day3_close_position_in_range'), 0) for r in died if r.get('day3_close_position_in_range') is not None]), 4)}",
        f"top_death_rule={top_death.get('condition')} test_died_rate_diff={top_death.get('test_died_rate_diff_vs_nonselected')} candidate={top_death.get('candidate')}",
        "",
        "[Answers]",
        "1-4. Counts are listed above and in 02_recovered_vs_died_summary.csv.",
        "5-6. See top recovery/death rules and day3 feature CSV.",
        "7. RSI is evaluated via entry-time rsi14 proxy as day3_rsi.",
        "8. MA5/MA25 are evaluated via entry-time MA proxies against day3 close.",
        "9. Volume is evaluated via entry-day volume_ratio proxy; true future volume is unavailable.",
        "10. Low-close is evaluated by day3_close_position_in_range <= 0.3.",
        "11. Support break is evaluated by day3 close below day1/day2 lows.",
        "12. Regime/sector fields are included when available.",
        "13. Margin buckets are included.",
        "14-15. Candidate rules are in 10/11 CSV.",
        "16. Keep raw extension as research-only until a death filter is forward-tested.",
        "17. If a stable death rule exists, create a new comparison case with an extension-ban filter.",
        "18. Primary promotion remains rejected for now.",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = _d(args.start_date)
    end = _d(args.end_date)
    train_start = _d(args.train_start)
    train_end = _d(args.train_end)
    test_start = _d(args.test_start)
    test_end = _d(args.test_end)

    sb = _build_supabase()
    logger.info("loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    rows = _build_extension_enabled_rows(candidates)
    logger.info("extension enabled rows=%d", len(rows))

    train = [r for r in rows if train_start <= _d(r["trade_date"]) <= train_end]
    test = [r for r in rows if test_start <= _d(r["trade_date"]) <= test_end]
    conds = _conditions()
    min_train = max(5, int(len(train) * 0.04))
    min_test = max(5, int(len(test) * 0.04))
    single_train = _condition_results(train, conds, "train", min_train, max_combo=1)
    single_test = _condition_results(test, conds, "test", min_test, max_combo=1)
    combo_conds = conds[:34]
    combo_train = _condition_results(train, combo_conds, "train", min_train, max_combo=3)
    combo_test = _condition_results(test, combo_conds, "test", min_test, max_combo=3)
    recovery = _top_recovery_rules(single_train + combo_train, single_test + combo_test)
    death = _top_death_rules(single_train + combo_train, single_test + combo_test)
    ban = [r for r in death if r.get("candidate")][:100]
    allow = [r for r in recovery if r.get("candidate")][:100]

    summary_rows = [
        {"scope": "all", **_summary(rows)},
        {"scope": "train", **_summary(train)},
        {"scope": "test", **_summary(test)},
    ]
    _write_csv(out_dir / "01_extension_enabled_all_trades.csv", rows)
    _write_csv(out_dir / "02_recovered_vs_died_summary.csv", summary_rows)
    _write_csv(out_dir / "03_day3_features_extension_enabled.csv", rows)
    _write_csv(out_dir / "04_single_condition_survival_train.csv", single_train)
    _write_csv(out_dir / "05_single_condition_survival_test.csv", single_test)
    _write_csv(out_dir / "06_combo_condition_survival_train.csv", combo_train)
    _write_csv(out_dir / "07_combo_condition_survival_test.csv", combo_test)
    _write_csv(out_dir / "08_top_recovery_rules.csv", recovery[:200])
    _write_csv(out_dir / "09_top_death_rules.csv", death[:200])
    _write_csv(out_dir / "10_extension_ban_rule_candidates.csv", ban)
    _write_csv(out_dir / "11_extension_allow_rule_candidates.csv", allow)
    _write_csv(out_dir / "12_monthly_stability_survival.csv", _monthly(rows))
    _write_csv(out_dir / "13_case_samples_recovered.csv", _sample([r for r in rows if r.get("recovered")], True))
    _write_csv(out_dir / "14_case_samples_died.csv", _sample([r for r in rows if r.get("died")], False))
    _report(out_dir / "15_survival_recommendation_report.txt", rows, train, test, recovery, death)
    logger.info("wrote outputs to %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", default="2023-01-01")
    p.add_argument("--end-date", default="2026-05-28")
    p.add_argument("--train-start", default="2023-01-01")
    p.add_argument("--train-end", default="2024-12-31")
    p.add_argument("--test-start", default="2025-01-01")
    p.add_argument("--test-end", default="2026-05-28")
    p.add_argument("--output-dir", default="outputs/h5_extension_survival")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
