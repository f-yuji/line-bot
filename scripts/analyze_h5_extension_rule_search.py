"""H5 Extension Rule Search.

Research-only script. It does not modify DB/case definitions.

The dataset is the H5 extension-enabled population:
H5 entry conditions pass and HD3 return <= -1%. The script then builds as many
real day-3 features as possible from stock_feature_snapshots, labels HD5/HD7
extension benefit, brute-forces allow/ban rules, and emits ML helper outputs.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import logging
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

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


@dataclass(frozen=True)
class Condition:
    name: str
    func: Callable[[dict], bool]
    feature_names: tuple[str, ...]


SNAPSHOT_COLS = [
    "trade_date", "code", "name", "market", "sector",
    "open", "high", "low", "close", "volume", "turnover_value",
    "prev_close", "day_change_pct",
    "ma5", "ma25", "ma75", "ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct",
    "rsi14", "volume_avg_20d", "volume_ratio_20d", "atr14", "volatility_20d",
    "nikkei_change_pct", "topix_change_pct", "sector_change_pct", "sector_gap_pct",
    "margin_ratio", "is_tradeable",
]


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


def _max_dd(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        dd = min(dd, equity - peak)
    return dd


def _month(value: Any) -> str:
    text = str(value or "")
    return text[:7] if len(text) >= 7 else "unknown"


def _year(value: Any) -> str:
    text = str(value or "")
    return text[:4] if len(text) >= 4 else "unknown"


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


def _future_day(row: dict, day: int) -> dict:
    close = _to_float(row.get(f"future_close_{day}d"), None)
    high = _to_float(row.get(f"future_high_{day}d"), None)
    low = _to_float(row.get(f"future_low_{day}d"), None)
    prev = (
        _to_float(row.get("entry_price"), None)
        or _to_float(row.get("close"), None)
        if day == 1
        else _to_float(row.get(f"future_close_{day - 1}d"), None)
    )
    return {"close": close, "high": high, "low": low, "open": prev, "source": "future_label_proxy"}


def _simulate_from_days(entry: float | None, days: list[dict], hold_days: int, stop_pct: float = -0.12) -> dict:
    if not entry:
        return {"ret": None, "exit_reason": "invalid_entry", "holding_days": None}
    stop_price = entry * (1.0 + stop_pct)
    last_close = None
    last_day = None
    for i, day in enumerate(days[:hold_days], start=1):
        low = _to_float(day.get("low"), None)
        close = _to_float(day.get("close"), None)
        if close is not None:
            last_close = close
            last_day = i
        if low is not None and low <= stop_price:
            return {"ret": stop_pct * 100.0, "exit_reason": "emergency_stop", "holding_days": i, "exit_price": stop_price}
    if last_close is None:
        return {"ret": None, "exit_reason": "no_data", "holding_days": None}
    return {
        "ret": (last_close / entry - 1.0) * 100.0,
        "exit_reason": "time_stop",
        "holding_days": last_day or min(hold_days, len(days)),
        "exit_price": last_close,
    }


def _fetch_snapshot_rows(sb, codes: list[str], start: date, end: date, chunk_size: int = 50) -> dict[str, list[dict]]:
    by_code: dict[str, list[dict]] = defaultdict(list)
    cols = ",".join(SNAPSHOT_COLS)
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i + chunk_size]
        logger.info("[rule_search] loading snapshots chunk=%d/%d codes=%d", i // chunk_size + 1, math.ceil(len(codes) / chunk_size), len(chunk))

        def query(last_id: int):
            q = (
                sb.table("stock_feature_snapshots")
                .select("id," + cols)
                .in_("code", chunk)
                .gte("trade_date", start.isoformat())
                .lte("trade_date", end.isoformat())
                .order("id")
            )
            return q.gt("id", last_id) if last_id else q

        for row in _fetch_all(query, label="day3_snapshots"):
            by_code[str(row.get("code"))].append(row)
    for rows in by_code.values():
        rows.sort(key=lambda r: str(r.get("trade_date") or ""))
    return by_code


def _index_by_date(rows: list[dict]) -> dict[str, int]:
    return {str(r.get("trade_date")): i for i, r in enumerate(rows) if r.get("trade_date")}


def _row_at(rows: list[dict], idx: int) -> dict | None:
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


def _bucket_rsi(value: float | None) -> str | None:
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
    if value < 60:
        return "50_60"
    return "gte60"


def _bucket_margin(value: float | None) -> str:
    if value is None:
        return "null"
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


def _build_dataset(candidates: list[dict], snapshots: dict[str, list[dict]]) -> list[dict]:
    dataset: list[dict] = []
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        code = str(row.get("code"))
        entry_date = str(row.get("trade_date"))
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry:
            continue

        code_rows = snapshots.get(code, [])
        idx_map = _index_by_date(code_rows)
        idx = idx_map.get(entry_date)
        real_days: list[dict] = []
        day_rows: dict[int, dict | None] = {}
        if idx is not None:
            for day in range(1, 8):
                snap = _row_at(code_rows, idx + day)
                day_rows[day] = snap
                if snap:
                    real_days.append(snap)

        future_days = [_future_day(row, day) for day in range(1, 8)]
        sim_days = []
        for day in range(1, 8):
            snap = day_rows.get(day)
            if snap:
                sim_days.append(snap)
            else:
                sim_days.append(future_days[day - 1])

        hd3 = _simulate_from_days(entry, sim_days, 3)
        hd5 = _simulate_from_days(entry, sim_days, 5)
        hd7 = _simulate_from_days(entry, sim_days, 7)
        hd3_return = _to_float(hd3.get("ret"), None)
        hd5_return = _to_float(hd5.get("ret"), None)
        hd7_return = _to_float(hd7.get("ret"), None)
        if hd3.get("exit_reason") == "emergency_stop" or hd3_return is None or hd5_return is None:
            continue
        if hd3_return > -1.0:
            continue

        d1, d2, d3 = sim_days[0], sim_days[1], sim_days[2]
        s1, s2, s3 = day_rows.get(1), day_rows.get(2), day_rows.get(3)
        c1, c2, c3 = [_to_float(d.get("close"), None) for d in [d1, d2, d3]]
        h3, l3 = _to_float(d3.get("high"), None), _to_float(d3.get("low"), None)
        o3 = _to_float(d3.get("open"), None)
        open_proxy = False
        if o3 is None:
            o3 = _to_float(d3.get("prev_close"), None) or c2
            open_proxy = True
        volume3 = _to_float(d3.get("volume"), None)
        rsi3 = _to_float(d3.get("rsi14"), None)
        rsi_proxy = False
        if rsi3 is None:
            rsi3 = _to_float(row.get("rsi14"), None)
            rsi_proxy = True
        ma_proxy = False
        ma5 = _to_float(d3.get("ma5"), None)
        ma25 = _to_float(d3.get("ma25"), None)
        ma75 = _to_float(d3.get("ma75"), None)
        if ma5 is None:
            ma5 = _to_float(row.get("ma5"), None)
            ma_proxy = True
        if ma25 is None:
            ma25 = _to_float(row.get("ma25"), None)
            ma_proxy = True
        if ma75 is None:
            ma75 = _to_float(row.get("ma75"), None)
            ma_proxy = True
        volume_ratio3 = _to_float(d3.get("volume_ratio_20d"), None)
        volume_ratio_proxy = False
        if volume_ratio3 is None:
            volume_ratio3 = _to_float(row.get("volume_ratio_20d"), None)
            volume_ratio_proxy = True
        atr3 = _to_float(d3.get("atr14"), None)
        atr_proxy = False
        if atr3 is None:
            atr3 = _to_float(row.get("atr14"), None)
            atr_proxy = True
        volatility_20d = _to_float(d3.get("volatility_20d"), None) or _to_float(row.get("volatility_20d"), None)

        range3 = (h3 - l3) if h3 is not None and l3 is not None else None
        body = abs(c3 - o3) if c3 is not None and o3 is not None else None
        upper = (h3 - max(o3, c3)) if h3 is not None and o3 is not None and c3 is not None else None
        lower = (min(o3, c3) - l3) if l3 is not None and o3 is not None and c3 is not None else None
        close_pos = ((c3 - l3) / range3) if c3 is not None and l3 is not None and range3 and range3 > 0 else None

        recent_lows = [_to_float(d.get("low"), None) for d in sim_days[:2]]
        recent_lows = [v for v in recent_lows if v is not None]
        recent_highs = [_to_float(d.get("high"), None) for d in sim_days[:2]]
        recent_highs = [v for v in recent_highs if v is not None]
        recent_low_5d = min(recent_lows, default=None)
        recent_high_5d = max(recent_highs, default=None)

        benefit5 = hd5_return - hd3_return
        benefit7 = (hd7_return - hd3_return) if hd7_return is not None else None
        entry_rsi = _to_float(row.get("rsi14"), None)
        day1_daily = _pct(c1, entry)
        day2_daily = _pct(c2, c1)
        day3_daily = _pct(c3, c2)
        margin = _to_float(row.get("margin_ratio"), None)
        turnover = _to_float(d3.get("turnover_value"), None) or _to_float(row.get("turnover_value"), None)
        volume_value = turnover
        day3_range_pct = (range3 / c3 * 100.0) if range3 is not None and c3 else None
        atr_pct = (atr3 / c3 * 100.0) if atr3 is not None and c3 else _to_float(row.get("atr_pct"), None)
        rec = {
            "entry_date": entry_date,
            "code": code,
            "name": row.get("name"),
            "sector": row.get("sector") or d3.get("sector"),
            "entry_price": entry,
            "signal_probability": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "entry_market_regime": row.get("market_regime"),
            "entry_rsi": entry_rsi,
            "entry_overheat_score": h5_overheat_score(row),
            "entry_close_vs_ma5_pct": row.get("ma5_gap_pct"),
            "entry_close_vs_ma25_pct": row.get("ma25_gap_pct"),
            "margin_ratio": margin,
            "margin_ratio_bucket": _bucket_margin(margin),
            "credit_bucket": _bucket_margin(margin),
            "liquidity": turnover,
            "turnover": turnover,
            "volume_value": volume_value,
            "hd3_return": hd3_return,
            "hd5_return": hd5_return,
            "hd7_return": hd7_return,
            "extension_benefit_5": benefit5,
            "extension_benefit_7": benefit7,
            "y_extend_better_5": int(benefit5 > 0),
            "y_strong_extend_better_5": int(benefit5 >= 1.0),
            "y_strong_extend_worse_5": int(benefit5 <= -1.0),
            "flat_5": int(-0.3 <= benefit5 <= 0.3),
            "extend_better_7": int(benefit7 is not None and benefit7 > 0),
            "day3_date": str(d3.get("trade_date") or ""),
            "day3_open": o3,
            "day3_high": h3,
            "day3_low": l3,
            "day3_close": c3,
            "day3_volume": volume3,
            "day3_rsi": rsi3,
            "day3_ma5": ma5,
            "day3_ma25": ma25,
            "day3_ma75": ma75,
            "day3_volume_ratio": volume_ratio3,
            "day3_atr": atr3,
            "day3_atr_pct": atr_pct,
            "day3_open_is_proxy": open_proxy or s3 is None,
            "day3_rsi_is_proxy": rsi_proxy,
            "day3_ma_is_proxy": ma_proxy,
            "day3_volume_ratio_is_proxy": volume_ratio_proxy,
            "day3_atr_is_proxy": atr_proxy,
            "day1_return": _pct(c1, entry),
            "day2_return": _pct(c2, entry),
            "day3_return": hd3_return,
            "day3_close_vs_entry_pct": hd3_return,
            "day3_high_vs_entry_pct": _pct(h3, entry),
            "day3_low_vs_entry_pct": _pct(l3, entry),
            "day1_to_day3_return": hd3_return,
            "day1_day2_day3_slope": (hd3_return - (_pct(c1, entry) or 0)) / 2.0 if c1 else None,
            "return_acceleration": (day3_daily - day2_daily) if day2_daily is not None and day3_daily is not None else None,
            "consecutive_down_days": sum(1 for v in [day1_daily, day2_daily, day3_daily] if v is not None and v < 0),
            "consecutive_up_days": sum(1 for v in [day1_daily, day2_daily, day3_daily] if v is not None and v > 0),
            "day3_range_pct": day3_range_pct,
            "day3_body_pct": (body / c3 * 100.0) if body is not None and c3 else None,
            "day3_upper_shadow_pct": (max(0.0, upper) / c3 * 100.0) if upper is not None and c3 else None,
            "day3_lower_shadow_pct": (max(0.0, lower) / c3 * 100.0) if lower is not None and c3 else None,
            "day3_close_position_in_range": close_pos,
            "day3_is_bullish": c3 is not None and o3 is not None and c3 > o3,
            "day3_is_bearish": c3 is not None and o3 is not None and c3 < o3,
            "day3_is_doji_like": body is not None and range3 and range3 > 0 and body / range3 <= 0.15,
            "day3_close_near_high": close_pos is not None and close_pos >= 0.7,
            "day3_close_near_low": close_pos is not None and close_pos <= 0.3,
            "day3_long_upper_shadow": upper is not None and c3 and upper / c3 * 100.0 >= 1.0,
            "day3_long_lower_shadow": lower is not None and c3 and lower / c3 * 100.0 >= 1.0,
            "day3_close_vs_ma5_pct": _pct(c3, ma5),
            "day3_close_vs_ma25_pct": _pct(c3, ma25),
            "day3_close_vs_ma75_pct": _pct(c3, ma75),
            "day3_close_above_ma5": c3 is not None and ma5 is not None and c3 >= ma5,
            "day3_close_above_ma25": c3 is not None and ma25 is not None and c3 >= ma25,
            "day3_close_above_ma75": c3 is not None and ma75 is not None and c3 >= ma75,
            "day3_ma5_slope": _pct(ma5, _to_float(row.get("ma5"), None)),
            "day3_ma25_slope": _pct(ma25, _to_float(row.get("ma25"), None)),
            "day3_rsi_bucket": _bucket_rsi(rsi3),
            "rsi_change_entry_to_day3": (rsi3 - entry_rsi) if rsi3 is not None and entry_rsi is not None else None,
            "day3_overheat_score": h5_overheat_score({**row, "rsi14": rsi3, "ma5_gap_pct": _pct(c3, ma5), "volume_ratio_20d": volume_ratio3}),
            "day1_volume_ratio": _to_float(d1.get("volume_ratio_20d"), None) or _to_float(row.get("volume_ratio_20d"), None),
            "day2_volume_ratio": _to_float(d2.get("volume_ratio_20d"), None) or _to_float(row.get("volume_ratio_20d"), None),
            "volume_change_day1_to_day3": _pct(volume3, _to_float(d1.get("volume"), None)),
            "day3_volume_spike": volume_ratio3 is not None and volume_ratio3 >= 1.5,
            "day3_volume_dry_up": volume_ratio3 is not None and volume_ratio3 <= 0.7,
            "recent_low_5d": recent_low_5d,
            "recent_low_10d": recent_low_5d,
            "recent_low_20d": recent_low_5d,
            "recent_high_5d": recent_high_5d,
            "recent_high_10d": recent_high_5d,
            "day3_low_below_recent_low_5d": l3 is not None and recent_low_5d is not None and l3 < recent_low_5d,
            "day3_close_below_recent_low_5d": c3 is not None and recent_low_5d is not None and c3 < recent_low_5d,
            "day3_low_below_recent_low_10d": l3 is not None and recent_low_5d is not None and l3 < recent_low_5d,
            "day3_close_below_recent_low_10d": c3 is not None and recent_low_5d is not None and c3 < recent_low_5d,
            "support_break_flag_5d": c3 is not None and recent_low_5d is not None and c3 < recent_low_5d,
            "support_break_flag_10d": c3 is not None and recent_low_5d is not None and c3 < recent_low_5d,
            "low_break_recovered_flag": l3 is not None and recent_low_5d is not None and l3 < recent_low_5d and c3 is not None and c3 >= recent_low_5d,
            "distance_to_recent_low_5d_pct": _pct(c3, recent_low_5d),
            "distance_to_recent_low_10d_pct": _pct(c3, recent_low_5d),
            "day3_new_low_since_entry": l3 is not None and all(v is None or l3 <= v for v in [_to_float(d1.get("low"), None), _to_float(d2.get("low"), None)]),
            "day3_new_high_since_entry": h3 is not None and all(v is None or h3 >= v for v in [_to_float(d1.get("high"), None), _to_float(d2.get("high"), None)]),
            "day3_market_regime": row.get("market_regime"),
            "market_regime_change": "same_or_unknown",
            "day3_index_return": _to_float(d3.get("nikkei_change_pct"), None) or _to_float(row.get("nikkei_change_pct"), None),
            "day1_to_day3_index_return": None,
            "day3_sector_return": _to_float(d3.get("sector_change_pct"), None) or _to_float(row.get("sector_change_pct"), None),
            "day1_to_day3_sector_return": None,
            "sector_strength_bucket": None,
            "sector_relative_strength": _to_float(d3.get("sector_gap_pct"), None) or _to_float(row.get("sector_gap_pct"), None),
            "atr_pct": _to_float(row.get("atr_pct"), None) or ((_to_float(row.get("atr14"), None) / entry * 100.0) if _to_float(row.get("atr14"), None) else None),
            "volatility_5d": None,
            "volatility_20d": volatility_20d,
            "day3_range_vs_atr": (day3_range_pct / atr_pct) if day3_range_pct is not None and atr_pct else None,
            "day3_body_vs_atr": ((body / c3 * 100.0) / atr_pct) if body is not None and c3 and atr_pct else None,
        }
        dataset.append(rec)
    return dataset


def _numeric_values(rows: list[dict], feature: str) -> list[float]:
    out = []
    for row in rows:
        value = row.get(feature)
        if isinstance(value, bool):
            continue
        number = _to_float(value, None)
        if number is not None and math.isfinite(number):
            out.append(number)
    return out


def _quantiles(values: list[float]) -> list[float]:
    if not values:
        return []
    vals = sorted(values)
    qs = []
    for q in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * q))))
        qs.append(vals[idx])
    return qs


def _generate_conditions(train_rows: list[dict]) -> list[Condition]:
    numeric_fixed = {
        "day3_return": [-1, -2, -3, -4, -5],
        "day3_rsi": [20, 25, 30, 35, 40, 45, 50, 60],
        "day3_close_position_in_range": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        "day3_upper_shadow_pct": [0.5, 1.0, 1.5, 2.0, 3.0],
        "day3_lower_shadow_pct": [0.5, 1.0, 1.5, 2.0, 3.0],
        "day3_volume_ratio": [0.7, 1.0, 1.2, 1.5, 2.0, 3.0],
        "day3_close_vs_ma5_pct": [-10, -7, -5, -3, -2, -1, 0, 1, 2],
        "day3_close_vs_ma25_pct": [-10, -7, -5, -3, -2, -1, 0, 1, 2],
        "day3_range_pct": [1, 2, 3, 4, 5, 7, 10],
        "day3_body_pct": [0.5, 1, 1.5, 2, 3, 5],
        "volume_change_day1_to_day3": [-50, -25, 0, 25, 50, 100],
    }
    numeric_features = sorted(set(numeric_fixed) | {
        "day1_return", "day2_return", "day3_return_daily", "return_acceleration",
        "day3_high_vs_entry_pct", "day3_low_vs_entry_pct",
        "day3_close_vs_ma75_pct", "rsi_change_entry_to_day3",
        "distance_to_recent_low_5d_pct", "day3_index_return", "day3_sector_return",
        "margin_ratio", "atr_pct", "day3_atr_pct", "day3_range_vs_atr",
        "day3_body_vs_atr", "volatility_20d",
    })
    boolean_features = [
        "day3_is_bullish", "day3_is_bearish", "day3_is_doji_like",
        "day3_close_near_high", "day3_close_near_low",
        "day3_long_upper_shadow", "day3_long_lower_shadow",
        "day3_close_above_ma5", "day3_close_above_ma25", "day3_close_above_ma75",
        "day3_volume_spike", "day3_volume_dry_up",
        "day3_low_below_recent_low_5d", "day3_close_below_recent_low_5d",
        "support_break_flag_5d", "low_break_recovered_flag",
        "day3_new_low_since_entry", "day3_new_high_since_entry",
    ]
    category_features = ["day3_rsi_bucket", "margin_ratio_bucket", "credit_bucket", "entry_market_regime"]

    conditions: dict[str, Condition] = {}

    def add(cond: Condition) -> None:
        conditions.setdefault(cond.name, cond)

    for feature in numeric_features:
        values = _numeric_values(train_rows, feature)
        thresholds = list(numeric_fixed.get(feature, [])) + _quantiles(values)
        seen_thresholds = []
        for t in thresholds:
            if t is None:
                continue
            rounded = round(float(t), 4)
            if rounded in seen_thresholds:
                continue
            seen_thresholds.append(rounded)
            add(Condition(
                f"{feature}_lte_{rounded:g}",
                lambda r, f=feature, x=rounded: (_to_float(r.get(f), None) is not None and _to_float(r.get(f), None) <= x),
                (feature,),
            ))
            add(Condition(
                f"{feature}_gte_{rounded:g}",
                lambda r, f=feature, x=rounded: (_to_float(r.get(f), None) is not None and _to_float(r.get(f), None) >= x),
                (feature,),
            ))

    ranges = [
        ("day3_rsi", 20, 35), ("day3_rsi", 30, 50), ("day3_rsi", 35, 60),
        ("day3_close_position_in_range", 0.3, 0.7),
        ("day3_close_position_in_range", 0.4, 0.8),
        ("day3_volume_ratio", 1.0, 2.0),
        ("margin_ratio", 3, 10), ("margin_ratio", 10, 30),
    ]
    for feature, lo, hi in ranges:
        add(Condition(
            f"{lo:g}_lte_{feature}_lte_{hi:g}",
            lambda r, f=feature, a=lo, b=hi: (_to_float(r.get(f), None) is not None and a <= _to_float(r.get(f), None) <= b),
            (feature,),
        ))

    for feature in boolean_features:
        add(Condition(f"{feature}_true", lambda r, f=feature: bool(r.get(f)) is True, (feature,)))
        add(Condition(f"{feature}_false", lambda r, f=feature: bool(r.get(f)) is False, (feature,)))

    for feature in category_features:
        cats = sorted({str(r.get(feature)) for r in train_rows if r.get(feature) not in (None, "")})
        for cat in cats:
            add(Condition(f"{feature}_eq_{cat}", lambda r, f=feature, c=cat: str(r.get(f)) == c, (feature,)))

    return list(conditions.values())


def _period_rows(rows: list[dict], period: str, train_end: date) -> list[dict]:
    if period == "all":
        return rows
    if period == "train":
        return [r for r in rows if _d(r["entry_date"]) <= train_end]
    if period == "test":
        return [r for r in rows if _d(r["entry_date"]) > train_end]
    raise ValueError(period)


def _returns_for_mode(rows: list[dict], condition: Condition, mode: str) -> tuple[list[float], list[dict]]:
    rets = []
    selected = []
    for row in rows:
        hit = condition.func(row)
        if hit:
            selected.append(row)
        if mode == "allow":
            ret = row["hd5_return"] if hit else row["hd3_return"]
        elif mode == "ban":
            ret = row["hd3_return"] if hit else row["hd5_return"]
        else:
            raise ValueError(mode)
        if ret is not None:
            rets.append(float(ret))
    return rets, selected


def _eval_condition(rows: list[dict], condition: Condition, mode: str, period: str, baseline: str) -> dict:
    rets, selected = _returns_for_mode(rows, condition, mode)
    selected_benefits = [_to_float(r.get("extension_benefit_5"), 0.0) or 0.0 for r in selected]
    selected_count = len(selected)
    n = len(rows)
    raw_extension = [float(r["hd5_return"]) for r in rows if r.get("hd5_return") is not None]
    primary = [float(r["hd3_return"]) for r in rows if r.get("hd3_return") is not None]
    base = raw_extension if baseline == "extension" else primary
    return {
        "condition": condition.name,
        "mode": mode,
        "period": period,
        "selected_count": selected_count,
        "selected_rate": round(selected_count / n * 100, 3) if n else None,
        "trade_count": len(rets),
        "avg_ret": _round(_avg(rets)),
        "median_ret": _round(_median(rets)),
        "pf": _round(_pf(rets)),
        "win_rate": _round(sum(1 for v in rets if v > 0) / len(rets) * 100 if rets else None),
        "max_loss": _round(min(rets) if rets else None),
        "max_dd": _round(_max_dd(rets)),
        "baseline_avg_ret": _round(_avg(base)),
        "baseline_pf": _round(_pf(base)),
        "avg_ret_diff": _round((_avg(rets) or 0) - (_avg(base) or 0)),
        "pf_diff": _round((_pf(rets) or 0) - (_pf(base) or 0)),
        "selected_avg_extension_benefit": _round(_avg(selected_benefits)),
        "selected_median_extension_benefit": _round(_median(selected_benefits)),
        "selected_recovered_rate": _round(sum(1 for v in selected_benefits if v > 0) / len(selected_benefits) * 100 if selected_benefits else None),
        "selected_died_rate": _round(sum(1 for v in selected_benefits if v <= 0) / len(selected_benefits) * 100 if selected_benefits else None),
        "selected_strong_recovered_rate": _round(sum(1 for v in selected_benefits if v >= 1.0) / len(selected_benefits) * 100 if selected_benefits else None),
        "selected_strong_died_rate": _round(sum(1 for v in selected_benefits if v <= -1.0) / len(selected_benefits) * 100 if selected_benefits else None),
    }


def _combined_condition(conds: tuple[Condition, ...]) -> Condition:
    name = " AND ".join(c.name for c in conds)
    features = tuple(sorted({f for c in conds for f in c.feature_names}))
    return Condition(name, lambda r, cs=conds: all(c.func(r) for c in cs), features)


def _score_rank(row: dict, min_count: int) -> float:
    count = int(row.get("selected_count") or 0)
    if count < min_count:
        return -9999
    return (
        float(row.get("avg_ret_diff") or 0) * 2.0
        + float(row.get("pf_diff") or 0)
        + float(row.get("selected_avg_extension_benefit") or 0) * 0.5
        - max(0.0, -float(row.get("max_dd") or 0)) * 0.02
    )


def _evaluate_conditions(rows: list[dict], conditions: list[Condition], train_end: date, mode: str, baseline: str) -> list[dict]:
    out: list[dict] = []
    for cond in conditions:
        train_row = _eval_condition(_period_rows(rows, "train", train_end), cond, mode, "train", baseline)
        test_row = _eval_condition(_period_rows(rows, "test", train_end), cond, mode, "test", baseline)
        all_row = _eval_condition(rows, cond, mode, "all", baseline)
        merged = {
            "condition": cond.name,
            "mode": mode,
            "features": ",".join(cond.feature_names),
        }
        for prefix, row in [("train", train_row), ("test", test_row), ("all", all_row)]:
            for key, value in row.items():
                if key not in {"condition", "mode"}:
                    merged[f"{prefix}_{key}"] = value
        merged["rank_score"] = _round(
            _score_rank(test_row, 10) + _score_rank(train_row, 30) * 0.5
        )
        out.append(merged)
    return sorted(out, key=lambda r: float(r.get("rank_score") or -9999), reverse=True)


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


def _proxy_usage(rows: list[dict]) -> list[dict]:
    fields = [
        "day3_open_is_proxy", "day3_rsi_is_proxy", "day3_ma_is_proxy",
        "day3_volume_ratio_is_proxy", "day3_atr_is_proxy",
    ]
    out = []
    n = len(rows)
    for field in fields:
        count = sum(1 for r in rows if r.get(field))
        out.append({"field": field, "proxy_count": count, "total": n, "proxy_rate": round(count / n * 100, 3) if n else None})
    return out


def _stability(rows: list[dict], key_func: Callable[[dict], str]) -> list[dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[key_func(row)].append(row)
    out = []
    for key, group in sorted(buckets.items()):
        benefits = [float(r["extension_benefit_5"]) for r in group if r.get("extension_benefit_5") is not None]
        out.append({
            "bucket": key,
            "count": len(group),
            "recovered_rate": _round(sum(1 for v in benefits if v > 0) / len(benefits) * 100 if benefits else None),
            "avg_extension_benefit": _round(_avg(benefits)),
            "median_extension_benefit": _round(_median(benefits)),
            "avg_hd3_return": _round(_avg([float(r["hd3_return"]) for r in group if r.get("hd3_return") is not None])),
            "avg_hd5_return": _round(_avg([float(r["hd5_return"]) for r in group if r.get("hd5_return") is not None])),
        })
    return out


def _run_ml(rows: list[dict], out_dir: Path) -> tuple[list[dict], str]:
    numeric_features = [
        key for key, value in rows[0].items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
        and key not in {"y_extend_better_5", "y_strong_extend_better_5", "y_strong_extend_worse_5"}
    ] if rows else []
    numeric_features = [f for f in numeric_features if f not in {"hd3_return", "hd5_return", "hd7_return", "extension_benefit_5", "extension_benefit_7"}]
    try:
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.inspection import permutation_importance
        from sklearn.pipeline import make_pipeline
        from sklearn.tree import DecisionTreeClassifier, export_text
    except Exception as exc:
        (out_dir / "14_decision_tree_rules.txt").write_text(f"ML skipped: {exc}", encoding="utf-8")
        return [{"status": "skipped", "reason": str(exc)}], f"ML skipped: {exc}"

    if len(rows) < 50 or not numeric_features:
        msg = "ML skipped: insufficient rows/features"
        (out_dir / "14_decision_tree_rules.txt").write_text(msg, encoding="utf-8")
        return [{"status": "skipped", "reason": msg}], msg

    X = [[_to_float(row.get(f), math.nan) for f in numeric_features] for row in rows]
    y = [int(row.get("y_extend_better_5") or 0) for row in rows]
    importances: list[dict] = []
    rf = make_pipeline(SimpleImputer(strategy="median"), RandomForestClassifier(n_estimators=200, min_samples_leaf=8, random_state=42))
    rf.fit(X, y)
    forest = rf.named_steps["randomforestclassifier"]
    for feature, imp in sorted(zip(numeric_features, forest.feature_importances_), key=lambda x: x[1], reverse=True)[:80]:
        importances.append({"model": "random_forest", "feature": feature, "importance": round(float(imp), 6)})
    try:
        perm = permutation_importance(rf, X, y, n_repeats=5, random_state=42, scoring="roc_auc")
        for feature, imp in sorted(zip(numeric_features, perm.importances_mean), key=lambda x: x[1], reverse=True)[:50]:
            importances.append({"model": "permutation_rf", "feature": feature, "importance": round(float(imp), 6)})
    except Exception:
        pass
    tree_pipe = make_pipeline(SimpleImputer(strategy="median"), DecisionTreeClassifier(max_depth=4, min_samples_leaf=15, random_state=42))
    tree_pipe.fit(X, y)
    tree = tree_pipe.named_steps["decisiontreeclassifier"]
    tree_text = export_text(tree, feature_names=numeric_features, decimals=3)
    (out_dir / "14_decision_tree_rules.txt").write_text(tree_text, encoding="utf-8")
    return importances, "ML completed with RandomForest/DecisionTree"


def _sample_rows(rows: list[dict], condition_name: str, conditions: dict[str, Condition], hit: bool) -> list[dict]:
    cond = conditions.get(condition_name)
    if not cond:
        return []
    filtered = [r for r in rows if cond.func(r) is hit]
    filtered.sort(key=lambda r: float(r.get("extension_benefit_5") or 0), reverse=hit)
    return filtered[:100]


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
    logger.info("[rule_search] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    seed_rows = [r for r in candidates if _passes_h5_entry(r)]
    codes = sorted({str(r.get("code")) for r in seed_rows if r.get("code")})
    snap_start = start - timedelta(days=10)
    snap_end = end + timedelta(days=30)
    snapshots = _fetch_snapshot_rows(sb, codes, snap_start, snap_end)
    rows = _build_dataset(candidates, snapshots)
    logger.info("[rule_search] extension-enabled dataset rows=%d", len(rows))

    train_rows = _period_rows(rows, "train", train_end)
    test_rows = _period_rows(rows, "test", train_end)
    conditions = _generate_conditions(train_rows)
    condition_map = {c.name: c for c in conditions}
    logger.info("[rule_search] generated single conditions=%d", len(conditions))

    single_ban = _evaluate_conditions(rows, conditions, train_end, "ban", "extension")
    single_allow = _evaluate_conditions(rows, conditions, train_end, "allow", "primary")
    singles_all = sorted(single_ban + single_allow, key=lambda r: float(r.get("rank_score") or -9999), reverse=True)

    top_for_ban = [condition_map[r["condition"]] for r in single_ban if r["condition"] in condition_map and (r.get("train_selected_count") or 0) >= args.min_train_count][: min(60, len(single_ban))]
    top_for_allow = [condition_map[r["condition"]] for r in single_allow if r["condition"] in condition_map and (r.get("train_selected_count") or 0) >= args.min_train_count][: min(60, len(single_allow))]

    def build_combos(base: list[Condition], depth: int) -> list[Condition]:
        combos = []
        for combo in itertools.combinations(base, depth):
            features = [f for c in combo for f in c.feature_names]
            if len(set(features)) < len(features):
                continue
            combos.append(_combined_condition(combo))
            if len(combos) >= args.max_combos_per_depth:
                break
        return combos

    combo2_ban = _evaluate_conditions(rows, build_combos(top_for_ban, 2), train_end, "ban", "extension")
    combo2_allow = _evaluate_conditions(rows, build_combos(top_for_allow, 2), train_end, "allow", "primary")
    combo3_ban = _evaluate_conditions(rows, build_combos(top_for_ban[:35], 3), train_end, "ban", "extension") if args.max_combo_depth >= 3 else []
    combo3_allow = _evaluate_conditions(rows, build_combos(top_for_allow[:35], 3), train_end, "allow", "primary") if args.max_combo_depth >= 3 else []

    candidates_ranked = []
    for group, label in [(single_ban[:30], "single_ban"), (single_allow[:30], "single_allow"), (combo2_ban[:30], "combo2_ban"), (combo2_allow[:30], "combo2_allow"), (combo3_ban[:30], "combo3_ban"), (combo3_allow[:30], "combo3_allow")]:
        for row in group:
            item = dict(row)
            item["rule_group"] = label
            candidates_ranked.append(item)
    candidates_ranked.sort(key=lambda r: float(r.get("rank_score") or -9999), reverse=True)

    ml_importance, ml_status = _run_ml(rows, out_dir) if args.run_ml else ([{"status": "skipped", "reason": "disabled"}], "ML disabled")

    _write_csv(out_dir / "01_extension_dataset.csv", rows)
    _write_csv(out_dir / "02_feature_proxy_usage.csv", _proxy_usage(rows))
    _write_csv(out_dir / "03_single_conditions_all.csv", singles_all)
    _write_csv(out_dir / "04_single_conditions_ban_ranked.csv", single_ban)
    _write_csv(out_dir / "05_single_conditions_allow_ranked.csv", single_allow)
    _write_csv(out_dir / "06_combo2_conditions_ban_ranked.csv", combo2_ban)
    _write_csv(out_dir / "07_combo2_conditions_allow_ranked.csv", combo2_allow)
    _write_csv(out_dir / "08_combo3_conditions_ban_ranked.csv", combo3_ban)
    _write_csv(out_dir / "09_combo3_conditions_allow_ranked.csv", combo3_allow)
    _write_csv(out_dir / "10_top_candidate_rules.csv", candidates_ranked)
    _write_csv(out_dir / "11_yearly_stability.csv", _stability(rows, lambda r: _year(r.get("entry_date"))))
    _write_csv(out_dir / "12_monthly_stability.csv", _stability(rows, lambda r: _month(r.get("entry_date"))))
    _write_csv(out_dir / "13_ml_feature_importance.csv", ml_importance)
    top_condition = candidates_ranked[0]["condition"] if candidates_ranked else ""
    _write_csv(out_dir / "15_case_samples_rule_hits.csv", _sample_rows(rows, top_condition, condition_map, True))
    _write_csv(out_dir / "16_case_samples_rule_misses.csv", _sample_rows(rows, top_condition, condition_map, False))

    benefits = [float(r["extension_benefit_5"]) for r in rows if r.get("extension_benefit_5") is not None]
    report = [
        "H5 Extension Rule Search Report",
        "",
        f"Dataset rows(extension enabled): {len(rows)}",
        f"Train rows: {len(train_rows)}",
        f"Test rows: {len(test_rows)}",
        f"HD5 improved count: {sum(1 for v in benefits if v > 0)}",
        f"HD5 worsened count: {sum(1 for v in benefits if v <= 0)}",
        f"Average extension benefit: {_round(_avg(benefits))}",
        "",
        "Proxy usage:",
        json.dumps(_proxy_usage(rows), ensure_ascii=False),
        "",
        "Top ban rules:",
        *[json.dumps(r, ensure_ascii=False) for r in single_ban[:5]],
        "",
        "Top allow rules:",
        *[json.dumps(r, ensure_ascii=False) for r in single_allow[:5]],
        "",
        "Top combo candidates:",
        *[json.dumps(r, ensure_ascii=False) for r in candidates_ranked[:10]],
        "",
        "ML status:",
        ml_status,
        "",
        "Interpretation checklist:",
        "1. Primary is unchanged: HD3 + EST12.",
        "2. Extension remains research-only.",
        "3. Prefer rules that improve both train/test and have enough count.",
        "4. This script creates no DB rows and registers no cases.",
    ]
    (out_dir / "17_extension_rule_search_report.txt").write_text("\n".join(report), encoding="utf-8")
    logger.info("[rule_search] wrote outputs to %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--train-start", default="2023-01-01")
    p.add_argument("--train-end", default="2024-12-31")
    p.add_argument("--test-start", default="2025-01-01")
    p.add_argument("--test-end", default="2026-05-28")
    p.add_argument("--output-dir", default="outputs/h5_extension_rule_search")
    p.add_argument("--max-combo-depth", type=int, default=3)
    p.add_argument("--min-train-count", type=int, default=30)
    p.add_argument("--min-test-count", type=int, default=10)
    p.add_argument("--max-combos-per-depth", type=int, default=5000)
    p.add_argument("--run-ml", default="true")
    args = p.parse_args()
    args.run_ml = str(args.run_ml).lower() in {"1", "true", "yes", "y"}
    return args


if __name__ == "__main__":
    run(_parse_args())
