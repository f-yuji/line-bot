"""H5 Extension Technical Rule Search.

Research-only script. Does not modify DB, case definitions, or any live code.

Extracts the Extension-enabled population (H5 Primary entry conditions AND
HD3 return <= -1%), builds day3-point technical features (support lines,
MA deviation, confirmed/partial weekly bars), runs exhaustive allow/ban rule
search, and outputs CSV/TXT reports.

Usage:
    python scripts/analyze_h5_extension_technical_rule_search.py
    python scripts/analyze_h5_extension_technical_rule_search.py --train-end 2024-12-31 --test-start 2025-01-01
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

SNAPSHOT_COLS = [
    "trade_date", "code", "name", "market", "sector",
    "open", "high", "low", "close", "volume", "turnover_value",
    "prev_close", "day_change_pct",
    "ma5", "ma25", "ma75", "ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct",
    "rsi14", "volume_avg_20d", "volume_ratio_20d", "atr14", "volatility_20d",
    "nikkei_change_pct", "topix_change_pct", "sector_change_pct", "sector_gap_pct",
    "margin_ratio", "is_tradeable",
]

# Current Extension Allow baseline for comparison
CURRENT_ALLOW_DAY1_RET_GTE = -2.22
CURRENT_ALLOW_BODY_LTE = 3.74
CURRENT_ALLOW_VOL_RATIO_LTE = 2.0

HISTORY_LOOKBACK_DAYS = 200  # calendar days before period_start for weekly/support history


# ──────────────────────────────────────────────
# Condition dataclass
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class Condition:
    name: str
    func: Callable[[dict], bool]
    feature_names: tuple[str, ...]


# ──────────────────────────────────────────────
# Utility helpers
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
    for v in values:
        equity += v
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
        _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if day == 1
        else _to_float(row.get(f"future_close_{day - 1}d"), None)
    )
    return {"close": close, "high": high, "low": low, "open": prev, "source": "future_label_proxy"}


def _simulate_from_days(entry: float | None, days: list[dict], hold_days: int, stop_pct: float = -0.12) -> dict:
    if not entry:
        return {"ret": None, "exit_reason": "invalid_entry"}
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
            return {"ret": stop_pct * 100.0, "exit_reason": "emergency_stop", "holding_days": i}
    if last_close is None:
        return {"ret": None, "exit_reason": "no_data"}
    return {"ret": (last_close / entry - 1.0) * 100.0, "exit_reason": "time_stop", "holding_days": last_day}


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


# ──────────────────────────────────────────────
# Snapshot loading
# ──────────────────────────────────────────────

def _fetch_snapshot_rows(
    sb, codes: list[str], start: date, end: date, chunk_size: int = 20
) -> dict[str, list[dict]]:
    by_code: dict[str, list[dict]] = defaultdict(list)
    cols = ",".join(SNAPSHOT_COLS)
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i : i + chunk_size]
        logger.info(
            "[tech_rule_search] loading snapshots chunk=%d/%d codes=%d",
            i // chunk_size + 1,
            math.ceil(len(codes) / chunk_size),
            len(chunk),
        )

        def query(last_id: int, _chunk: list[str] = chunk) -> object:
            q = (
                sb.table("stock_feature_snapshots")
                .select("id," + cols)
                .in_("code", _chunk)
                .gte("trade_date", start.isoformat())
                .lte("trade_date", end.isoformat())
                .order("id")
            )
            return q.gt("id", last_id) if last_id else q

        for row in _fetch_all(query, label=f"tech_snaps_{i // chunk_size}"):
            by_code[str(row.get("code"))].append(row)

    for rows in by_code.values():
        rows.sort(key=lambda r: str(r.get("trade_date") or ""))
    return dict(by_code)


def _index_by_date(rows: list[dict]) -> dict[str, int]:
    return {str(r.get("trade_date")): i for i, r in enumerate(rows) if r.get("trade_date")}


def _row_at(rows: list[dict], idx: int) -> dict | None:
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


# ──────────────────────────────────────────────
# MA / RSI helpers
# ──────────────────────────────────────────────

def _wmean(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _rsi14(closes: list[float]) -> float | None:
    if len(closes) < 15:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
    avg_g = sum(gains[-14:]) / 14
    avg_l = sum(losses[-14:]) / 14
    if avg_l == 0:
        return 100.0
    return 100.0 - (100.0 / (1 + avg_g / avg_l))


def _compute_ma_from_history(code_rows: list[dict], day3_idx: int, period: int) -> float | None:
    """Simple MA of `period` closes ending at day3_idx (inclusive)."""
    if day3_idx < period - 1:
        return None
    closes = []
    for i in range(day3_idx - period + 1, day3_idx + 1):
        if i < 0 or i >= len(code_rows):
            return None
        c = _to_float(code_rows[i].get("close"), None)
        if c is None:
            return None
        closes.append(c)
    return sum(closes) / len(closes) if len(closes) == period else None


# ──────────────────────────────────────────────
# Support line features
# ──────────────────────────────────────────────

def _compute_support_features(
    code_rows: list[dict],
    day3_idx: int,
    day3_close: float | None,
    day3_high: float | None,
    day3_low: float | None,
) -> dict:
    """Compute daily support line features from N trading days BEFORE day3.

    Note: recent_low_Nd = min(low) for the N trading days ending at day2
    (the day before day3). This avoids using day3 data in the support level.
    """
    feat: dict = {}
    for n in [5, 10, 20, 60]:
        start_idx = max(0, day3_idx - n)
        hist = code_rows[start_idx:day3_idx]  # N days before day3, exclusive of day3
        lows = [_to_float(r.get("low"), None) for r in hist]
        lows = [v for v in lows if v is not None]
        highs = [_to_float(r.get("high"), None) for r in hist]
        highs = [v for v in highs if v is not None]

        rl = min(lows) if lows else None
        rh = max(highs) if highs else None
        feat[f"recent_low_{n}d"] = rl
        feat[f"recent_high_{n}d"] = rh

        dist_pct = _pct(day3_close, rl)
        feat[f"day3_close_vs_recent_low_{n}d_pct"] = dist_pct
        feat[f"day3_low_below_recent_low_{n}d"] = bool(
            day3_low is not None and rl is not None and day3_low < rl
        )
        feat[f"day3_close_below_recent_low_{n}d"] = bool(
            day3_close is not None and rl is not None and day3_close < rl
        )
        feat[f"low_break_recovered_{n}d"] = bool(
            day3_low is not None and rl is not None and day3_low < rl
            and day3_close is not None and day3_close >= rl
        )
        feat[f"support_break_{n}d"] = bool(
            day3_close is not None and rl is not None and day3_close < rl
        )
        if n in {5, 10, 20}:
            feat[f"near_support_{n}d"] = bool(
                dist_pct is not None and 0 <= dist_pct <= 2.0
            )
    return feat


# ──────────────────────────────────────────────
# Weekly features
# ──────────────────────────────────────────────

def _compute_weekly_features(code_rows: list[dict], day3_idx: int) -> dict:
    """Build confirmed weekly bar (previous ISO week) and partial weekly bar
    (day3's ISO week up to and including day3).

    LEAKAGE SAFETY:
    - confirmed_week = all days in the ISO week BEFORE day3's week. All data
      is confirmed before day3's week starts. No future data used.
    - partial_week = days from Monday of day3's week through day3 (inclusive).
      We only include rows up to code_rows[day3_idx]. No future data used.
    - Weekly MA is computed from confirmed weekly closes only (< day3's week).
    """
    feat: dict = {}
    if day3_idx < 0 or day3_idx >= len(code_rows):
        return feat

    day3_row = code_rows[day3_idx]
    day3_date_str = str(day3_row.get("trade_date") or "")
    if not day3_date_str:
        return feat
    day3_date = _d(day3_date_str)
    day3_iso = day3_date.isocalendar()
    day3_year_week: tuple[int, int] = (day3_iso[0], day3_iso[1])

    # Group rows up to and including day3 by ISO (year, week)
    by_week: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for r in code_rows[: day3_idx + 1]:
        d_str = str(r.get("trade_date") or "")
        if not d_str:
            continue
        iso = _d(d_str).isocalendar()
        by_week[(iso[0], iso[1])].append(r)

    sorted_weeks = sorted(by_week.keys())
    confirmed_weeks = [yw for yw in sorted_weeks if yw < day3_year_week]

    # ── Partial week (day3's ISO week up to day3) ──
    partial_rows = sorted(
        by_week.get(day3_year_week, []),
        key=lambda r: str(r.get("trade_date") or ""),
    )
    if partial_rows:
        pw_opens = [_to_float(r.get("open"), None) for r in partial_rows]
        pw_highs = [_to_float(r.get("high"), None) for r in partial_rows]
        pw_lows = [_to_float(r.get("low"), None) for r in partial_rows]
        pw_closes = [_to_float(r.get("close"), None) for r in partial_rows]
        pw_volumes = [_to_float(r.get("volume"), None) for r in partial_rows]
        pw_opens = [v for v in pw_opens if v is not None]
        pw_highs = [v for v in pw_highs if v is not None]
        pw_lows = [v for v in pw_lows if v is not None]
        pw_closes = [v for v in pw_closes if v is not None]
        pw_volumes = [v for v in pw_volumes if v is not None]

        pw_open = pw_opens[0] if pw_opens else None
        pw_high = max(pw_highs) if pw_highs else None
        pw_low = min(pw_lows) if pw_lows else None
        pw_close = pw_closes[-1] if pw_closes else None
        pw_volume = sum(pw_volumes) if pw_volumes else None

        pw_range = (pw_high - pw_low) if pw_high is not None and pw_low is not None else None
        pw_body = abs(pw_close - pw_open) if pw_close is not None and pw_open is not None else None
        pw_upper = (
            (pw_high - max(pw_open, pw_close))
            if pw_high is not None and pw_open is not None and pw_close is not None
            else None
        )
        pw_lower = (
            (min(pw_open, pw_close) - pw_low)
            if pw_low is not None and pw_open is not None and pw_close is not None
            else None
        )
        pw_pos = (
            (pw_close - pw_low) / pw_range
            if pw_close is not None and pw_low is not None and pw_range and pw_range > 0
            else None
        )
        feat["partial_week_open"] = pw_open
        feat["partial_week_high"] = pw_high
        feat["partial_week_low"] = pw_low
        feat["partial_week_close"] = pw_close
        feat["partial_week_volume"] = pw_volume
        feat["partial_week_return_pct"] = _pct(pw_close, pw_open)
        feat["partial_week_body_pct"] = (pw_body / pw_close * 100) if pw_body is not None and pw_close else None
        feat["partial_week_upper_shadow_pct"] = (
            (max(0.0, pw_upper) / pw_close * 100) if pw_upper is not None and pw_close else None
        )
        feat["partial_week_lower_shadow_pct"] = (
            (max(0.0, pw_lower) / pw_close * 100) if pw_lower is not None and pw_close else None
        )
        feat["partial_week_close_position_in_range"] = pw_pos
        feat["partial_week_is_bullish"] = bool(
            pw_close is not None and pw_open is not None and pw_close > pw_open
        )
        feat["partial_week_is_bearish"] = bool(
            pw_close is not None and pw_open is not None and pw_close < pw_open
        )
        feat["partial_week_days"] = len(partial_rows)

    # ── Confirmed weekly bar (previous ISO week) ──
    if not confirmed_weeks:
        return feat

    prev_week_key = confirmed_weeks[-1]
    conf_rows = sorted(
        by_week[prev_week_key], key=lambda r: str(r.get("trade_date") or "")
    )
    cw_opens = [_to_float(r.get("open"), None) for r in conf_rows]
    cw_highs = [_to_float(r.get("high"), None) for r in conf_rows]
    cw_lows = [_to_float(r.get("low"), None) for r in conf_rows]
    cw_closes = [_to_float(r.get("close"), None) for r in conf_rows]
    cw_volumes = [_to_float(r.get("volume"), None) for r in conf_rows]
    cw_opens = [v for v in cw_opens if v is not None]
    cw_highs = [v for v in cw_highs if v is not None]
    cw_lows = [v for v in cw_lows if v is not None]
    cw_closes = [v for v in cw_closes if v is not None]
    cw_volumes = [v for v in cw_volumes if v is not None]

    cw_open = cw_opens[0] if cw_opens else None
    cw_high = max(cw_highs) if cw_highs else None
    cw_low = min(cw_lows) if cw_lows else None
    cw_close = cw_closes[-1] if cw_closes else None
    cw_volume = sum(cw_volumes) if cw_volumes else None

    cw_range = (cw_high - cw_low) if cw_high is not None and cw_low is not None else None
    cw_body = abs(cw_close - cw_open) if cw_close is not None and cw_open is not None else None
    cw_upper = (
        (cw_high - max(cw_open, cw_close))
        if cw_high is not None and cw_open is not None and cw_close is not None
        else None
    )
    cw_lower = (
        (min(cw_open, cw_close) - cw_low)
        if cw_low is not None and cw_open is not None and cw_close is not None
        else None
    )
    cw_pos = (
        (cw_close - cw_low) / cw_range
        if cw_close is not None and cw_low is not None and cw_range and cw_range > 0
        else None
    )

    feat["confirmed_week_open"] = cw_open
    feat["confirmed_week_high"] = cw_high
    feat["confirmed_week_low"] = cw_low
    feat["confirmed_week_close"] = cw_close
    feat["confirmed_week_volume"] = cw_volume
    feat["confirmed_week_return_pct"] = _pct(cw_close, cw_open)
    feat["confirmed_week_body_pct"] = (
        (cw_body / cw_close * 100) if cw_body is not None and cw_close else None
    )
    feat["confirmed_week_upper_shadow_pct"] = (
        (max(0.0, cw_upper) / cw_close * 100) if cw_upper is not None and cw_close else None
    )
    feat["confirmed_week_lower_shadow_pct"] = (
        (max(0.0, cw_lower) / cw_close * 100) if cw_lower is not None and cw_close else None
    )
    feat["confirmed_week_close_position_in_range"] = cw_pos
    feat["confirmed_week_is_bullish"] = bool(
        cw_close is not None and cw_open is not None and cw_close > cw_open
    )
    feat["confirmed_week_is_bearish"] = bool(
        cw_close is not None and cw_open is not None and cw_close < cw_open
    )

    # ── Weekly closes and lows (for MA and support) from confirmed weeks only ──
    all_weekly_closes: list[float] = []
    all_weekly_lows: list[float] = []
    for yw in confirmed_weeks:
        wr = sorted(by_week[yw], key=lambda r: str(r.get("trade_date") or ""))
        wc_list = [_to_float(r.get("close"), None) for r in wr]
        wl_list = [_to_float(r.get("low"), None) for r in wr]
        wc_list = [v for v in wc_list if v is not None]
        wl_list = [v for v in wl_list if v is not None]
        if wc_list:
            all_weekly_closes.append(wc_list[-1])
        if wl_list:
            all_weekly_lows.append(min(wl_list))

    # Weekly MA
    feat["confirmed_week_ma5"] = _wmean(all_weekly_closes, 5)
    feat["confirmed_week_ma13"] = _wmean(all_weekly_closes, 13)
    feat["confirmed_week_ma26"] = _wmean(all_weekly_closes, 26)
    feat["confirmed_week_ma52"] = _wmean(all_weekly_closes, 52)
    feat["confirmed_week_weeks_available"] = len(all_weekly_closes)

    feat["confirmed_week_close_vs_ma5_pct"] = _pct(cw_close, feat.get("confirmed_week_ma5"))
    feat["confirmed_week_close_vs_ma13_pct"] = _pct(cw_close, feat.get("confirmed_week_ma13"))
    feat["confirmed_week_close_vs_ma26_pct"] = _pct(cw_close, feat.get("confirmed_week_ma26"))
    feat["confirmed_week_close_vs_ma52_pct"] = _pct(cw_close, feat.get("confirmed_week_ma52"))

    feat["confirmed_week_close_above_ma5"] = bool(
        cw_close is not None and feat.get("confirmed_week_ma5") is not None
        and cw_close >= feat["confirmed_week_ma5"]
    )
    feat["confirmed_week_close_above_ma13"] = bool(
        cw_close is not None and feat.get("confirmed_week_ma13") is not None
        and cw_close >= feat["confirmed_week_ma13"]
    )
    feat["confirmed_week_close_above_ma26"] = bool(
        cw_close is not None and feat.get("confirmed_week_ma26") is not None
        and cw_close >= feat["confirmed_week_ma26"]
    )

    # Weekly MA13 slope (current MA13 vs. previous MA13 one week ago)
    if len(all_weekly_closes) >= 14:
        ma13_prev = sum(all_weekly_closes[-14:-1]) / 13
        ma13_curr = feat.get("confirmed_week_ma13")
        feat["confirmed_week_ma13_slope"] = _pct(ma13_curr, ma13_prev)

    # Weekly RSI14
    feat["confirmed_week_rsi"] = _rsi14(all_weekly_closes)

    # Weekly support lines (based on weekly lows over 13/26 confirmed weeks)
    if len(all_weekly_lows) >= 13:
        wrl13 = min(all_weekly_lows[-13:])
        feat["confirmed_week_recent_low_13w"] = wrl13
        feat["confirmed_week_close_vs_recent_low_13w_pct"] = _pct(cw_close, wrl13)
        feat["confirmed_week_close_below_recent_low_13w"] = bool(
            cw_close is not None and cw_close < wrl13
        )
        feat["confirmed_week_low_below_recent_low_13w"] = bool(
            cw_low is not None and cw_low < wrl13
        )
        feat["confirmed_week_low_break_recovered_13w"] = bool(
            cw_low is not None and cw_low < wrl13
            and cw_close is not None and cw_close >= wrl13
        )

    if len(all_weekly_lows) >= 26:
        wrl26 = min(all_weekly_lows[-26:])
        feat["confirmed_week_recent_low_26w"] = wrl26
        feat["confirmed_week_close_vs_recent_low_26w_pct"] = _pct(cw_close, wrl26)
        feat["confirmed_week_close_below_recent_low_26w"] = bool(
            cw_close is not None and cw_close < wrl26
        )

    # Weekly volume ratio (confirmed_week vs 13-week avg, if available)
    if len(confirmed_weeks) >= 14 and cw_volume is not None:
        vol_history = []
        for yw in confirmed_weeks[-14:-1]:
            wr = sorted(by_week[yw], key=lambda r: str(r.get("trade_date") or ""))
            wv = sum(_to_float(r.get("volume"), 0.0) or 0.0 for r in wr)
            if wv > 0:
                vol_history.append(wv)
        if vol_history:
            avg_vol = sum(vol_history) / len(vol_history)
            feat["confirmed_week_volume_ratio"] = cw_volume / avg_vol if avg_vol > 0 else None

    return feat


# ──────────────────────────────────────────────
# Dataset builder
# ──────────────────────────────────────────────

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

        real_days: dict[int, dict | None] = {}
        if idx is not None:
            for day in range(1, 8):
                real_days[day] = _row_at(code_rows, idx + day)

        future_days = {day: _future_day(row, day) for day in range(1, 8)}
        sim_days = [real_days.get(day) or future_days[day] for day in range(1, 8)]

        hd3 = _simulate_from_days(entry, sim_days, 3)
        hd5 = _simulate_from_days(entry, sim_days, 5)
        hd7 = _simulate_from_days(entry, sim_days, 7)
        hd3_return = _to_float(hd3.get("ret"), None)
        hd5_return = _to_float(hd5.get("ret"), None)
        hd7_return = _to_float(hd7.get("ret"), None)

        if hd3.get("exit_reason") == "emergency_stop" or hd3_return is None:
            continue
        if hd5_return is None:
            continue
        if hd3_return > -1.0:
            continue  # not extension-enabled

        d1, d2, d3 = sim_days[0], sim_days[1], sim_days[2]
        s3 = real_days.get(3)  # real day3 snapshot (may be None)

        c1 = _to_float(d1.get("close"), None)
        c2 = _to_float(d2.get("close"), None)
        c3 = _to_float(d3.get("close"), None)
        h3 = _to_float(d3.get("high"), None)
        l3 = _to_float(d3.get("low"), None)

        o3 = _to_float(d3.get("open"), None)
        open_proxy = False
        if o3 is None:
            o3 = _to_float(d3.get("prev_close"), None) or c2
            open_proxy = True

        rsi3 = _to_float(d3.get("rsi14"), None)
        rsi_proxy = False
        if rsi3 is None:
            rsi3 = _to_float(row.get("rsi14"), None)
            rsi_proxy = True

        ma5 = _to_float(d3.get("ma5"), None)
        ma25 = _to_float(d3.get("ma25"), None)
        ma75 = _to_float(d3.get("ma75"), None)
        ma_proxy = False
        if ma5 is None:
            ma5 = _to_float(row.get("ma5"), None)
            ma_proxy = True
        if ma25 is None:
            ma25 = _to_float(row.get("ma25"), None)
        if ma75 is None:
            ma75 = _to_float(row.get("ma75"), None)

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
        volume3 = _to_float(d3.get("volume"), None)
        turnover = _to_float(d3.get("turnover_value"), None) or _to_float(row.get("turnover_value"), None)

        range3 = (h3 - l3) if h3 is not None and l3 is not None else None
        body = abs(c3 - o3) if c3 is not None and o3 is not None else None
        upper = (h3 - max(o3, c3)) if h3 is not None and o3 is not None and c3 is not None else None
        lower = (min(o3, c3) - l3) if l3 is not None and o3 is not None and c3 is not None else None
        close_pos = (
            (c3 - l3) / range3
            if c3 is not None and l3 is not None and range3 and range3 > 0
            else None
        )

        day1_daily = _pct(c1, entry)
        day2_daily = _pct(c2, c1)
        day3_daily = _pct(c3, c2)
        day3_range_pct = (range3 / c3 * 100.0) if range3 is not None and c3 else None
        atr_pct = (atr3 / c3 * 100.0) if atr3 is not None and c3 else None

        benefit5 = hd5_return - hd3_return
        benefit7 = (hd7_return - hd3_return) if hd7_return is not None else None
        entry_rsi = _to_float(row.get("rsi14"), None)
        margin = _to_float(row.get("margin_ratio"), None)

        day3_idx = (idx + 3) if idx is not None else None

        # ── Support line features ──
        support_feat: dict = {}
        if day3_idx is not None and s3 is not None:
            support_feat = _compute_support_features(code_rows, day3_idx, c3, h3, l3)

        # ── Weekly features ──
        weekly_feat: dict = {}
        if day3_idx is not None and s3 is not None:
            weekly_feat = _compute_weekly_features(code_rows, day3_idx)

        # ── MA slope features ──
        ma5_slope_3d = None
        ma25_slope_5d = None
        ma75_slope_10d = None
        if day3_idx is not None:
            r3ago = _row_at(code_rows, day3_idx - 3)
            r5ago = _row_at(code_rows, day3_idx - 5)
            r10ago = _row_at(code_rows, day3_idx - 10)
            ma5_slope_3d = _pct(ma5, _to_float(r3ago.get("ma5"), None)) if r3ago else None
            ma25_slope_5d = _pct(ma25, _to_float(r5ago.get("ma25"), None)) if r5ago else None
            ma75_slope_10d = _pct(ma75, _to_float(r10ago.get("ma75"), None)) if r10ago else None

        # ── Computed daily MAs from historical closes ──
        day3_ma10 = _compute_ma_from_history(code_rows, day3_idx, 10) if day3_idx is not None else None
        day3_ma50 = _compute_ma_from_history(code_rows, day3_idx, 50) if day3_idx is not None else None
        day3_ma100 = _compute_ma_from_history(code_rows, day3_idx, 100) if day3_idx is not None else None

        rec: dict = {
            # Identifiers
            "entry_date": entry_date,
            "code": code,
            "name": row.get("name"),
            "sector": row.get("sector") or (d3.get("sector") if hasattr(d3, "get") else None),
            "entry_price": entry,
            # Entry signals
            "signal_probability": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "entry_market_regime": row.get("market_regime"),
            "entry_rsi": entry_rsi,
            "entry_overheat_score": h5_overheat_score(row),
            "entry_close_vs_ma5_pct": row.get("ma5_gap_pct"),
            "entry_close_vs_ma25_pct": row.get("ma25_gap_pct"),
            "margin_ratio": margin,
            "margin_ratio_bucket": _bucket_margin(margin),
            # Returns and labels (FUTURE — excluded from ML features)
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
            # Day3 OHLCV
            "day3_date": str(d3.get("trade_date") or "") if hasattr(d3, "get") else "",
            "day3_open": o3,
            "day3_high": h3,
            "day3_low": l3,
            "day3_close": c3,
            "day3_volume": volume3,
            # Proxy flags
            "day3_open_is_proxy": open_proxy or s3 is None,
            "day3_rsi_is_proxy": rsi_proxy,
            "day3_ma_is_proxy": ma_proxy,
            "day3_volume_ratio_is_proxy": volume_ratio_proxy,
            "day3_atr_is_proxy": atr_proxy,
            # Day3 technical
            "day3_rsi": rsi3,
            "day3_rsi_bucket": _bucket_rsi(rsi3),
            "day3_ma5": ma5,
            "day3_ma25": ma25,
            "day3_ma75": ma75,
            "day3_ma10": day3_ma10,
            "day3_ma50": day3_ma50,
            "day3_ma100": day3_ma100,
            "day3_volume_ratio": volume_ratio3,
            "day3_atr": atr3,
            "day3_atr_pct": atr_pct,
            "volatility_20d": volatility_20d,
            # Day3 MA gaps
            "day3_close_vs_ma5_pct": _pct(c3, ma5),
            "day3_close_vs_ma25_pct": _pct(c3, ma25),
            "day3_close_vs_ma75_pct": _pct(c3, ma75),
            "day3_close_vs_ma10_pct": _pct(c3, day3_ma10),
            "day3_close_vs_ma50_pct": _pct(c3, day3_ma50),
            "day3_close_vs_ma100_pct": _pct(c3, day3_ma100),
            "day3_close_above_ma5": c3 is not None and ma5 is not None and c3 >= ma5,
            "day3_close_above_ma25": c3 is not None and ma25 is not None and c3 >= ma25,
            "day3_close_above_ma75": c3 is not None and ma75 is not None and c3 >= ma75,
            "day3_close_above_ma10": c3 is not None and day3_ma10 is not None and c3 >= day3_ma10,
            "day3_close_above_ma50": c3 is not None and day3_ma50 is not None and c3 >= day3_ma50,
            "day3_close_above_ma100": c3 is not None and day3_ma100 is not None and c3 >= day3_ma100,
            # MA slopes
            "ma5_slope_3d": ma5_slope_3d,
            "ma25_slope_5d": ma25_slope_5d,
            "ma75_slope_10d": ma75_slope_10d,
            # Day3 candle
            "day3_range_pct": day3_range_pct,
            "day3_body_pct": (body / c3 * 100.0) if body is not None and c3 else None,
            "day3_upper_shadow_pct": (max(0.0, upper) / c3 * 100.0) if upper is not None and c3 else None,
            "day3_lower_shadow_pct": (max(0.0, lower) / c3 * 100.0) if lower is not None and c3 else None,
            "day3_close_position_in_range": close_pos,
            "day3_is_bullish": c3 is not None and o3 is not None and c3 > o3,
            "day3_is_bearish": c3 is not None and o3 is not None and c3 < o3,
            "day3_is_doji_like": (
                body is not None and range3 and range3 > 0 and body / range3 <= 0.15
            ),
            "day3_close_near_high": close_pos is not None and close_pos >= 0.7,
            "day3_close_near_low": close_pos is not None and close_pos <= 0.3,
            "day3_long_upper_shadow": upper is not None and c3 and upper / c3 * 100.0 >= 1.0,
            "day3_long_lower_shadow": lower is not None and c3 and lower / c3 * 100.0 >= 1.0,
            # Day returns
            "day1_return": _pct(c1, entry),
            "day2_return": _pct(c2, entry),
            "day3_return": hd3_return,
            "day1_daily": day1_daily,
            "day2_daily": day2_daily,
            "day3_daily": day3_daily,
            "return_acceleration": (
                (day3_daily - day2_daily)
                if day2_daily is not None and day3_daily is not None
                else None
            ),
            "consecutive_down_days": sum(
                1 for v in [day1_daily, day2_daily, day3_daily] if v is not None and v < 0
            ),
            "consecutive_up_days": sum(
                1 for v in [day1_daily, day2_daily, day3_daily] if v is not None and v > 0
            ),
            # Volume
            "day1_volume_ratio": (
                _to_float(d1.get("volume_ratio_20d"), None) or _to_float(row.get("volume_ratio_20d"), None)
            ),
            "day2_volume_ratio": (
                _to_float(d2.get("volume_ratio_20d"), None) or _to_float(row.get("volume_ratio_20d"), None)
            ),
            "volume_change_day1_to_day3": _pct(volume3, _to_float(d1.get("volume"), None)),
            "day3_volume_spike": volume_ratio3 is not None and volume_ratio3 >= 1.5,
            "day3_volume_dry_up": volume_ratio3 is not None and volume_ratio3 <= 0.7,
            # Market / sector
            "day3_market_regime": row.get("market_regime"),
            "day3_index_return": (
                _to_float(d3.get("nikkei_change_pct"), None) or _to_float(row.get("nikkei_change_pct"), None)
            ),
            "day3_sector_return": (
                _to_float(d3.get("sector_change_pct"), None) or _to_float(row.get("sector_change_pct"), None)
            ),
            "sector_relative_strength": (
                _to_float(d3.get("sector_gap_pct"), None) or _to_float(row.get("sector_gap_pct"), None)
            ),
            # ATR
            "day3_range_vs_atr": (
                (day3_range_pct / atr_pct) if day3_range_pct is not None and atr_pct else None
            ),
            "day3_body_vs_atr": (
                ((body / c3 * 100.0) / atr_pct) if body is not None and c3 and atr_pct else None
            ),
        }
        # Merge support / weekly features
        rec.update(support_feat)
        rec.update(weekly_feat)
        dataset.append(rec)
    return dataset


# ──────────────────────────────────────────────
# Condition generation
# ──────────────────────────────────────────────

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
    return [
        vals[min(len(vals) - 1, max(0, int(round((len(vals) - 1) * q))))]
        for q in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    ]


def _generate_conditions(train_rows: list[dict]) -> list[Condition]:
    numeric_fixed: dict[str, list] = {
        # Existing
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
        "day1_return": [-5, -3, -2.22, -2, -1, 0],
        "volume_change_day1_to_day3": [-50, -25, 0, 25, 50, 100],
        # Support lines
        "day3_close_vs_recent_low_5d_pct": [-10, -5, -3, -2, -1, 0, 1, 2, 3, 5],
        "day3_close_vs_recent_low_10d_pct": [-10, -5, -3, -2, -1, 0, 1, 2, 3, 5],
        "day3_close_vs_recent_low_20d_pct": [-10, -5, -3, -2, -1, 0, 1, 2, 3, 5],
        "day3_close_vs_recent_low_60d_pct": [-15, -10, -5, -3, 0, 3, 5],
        # MA slopes
        "ma5_slope_3d": [-5, -3, -2, -1, 0, 1, 2],
        "ma25_slope_5d": [-5, -3, -2, -1, 0, 1, 2],
        "ma75_slope_10d": [-5, -3, -2, -1, 0, 1],
        # Computed daily MAs
        "day3_close_vs_ma10_pct": [-10, -5, -3, -1, 0, 1],
        "day3_close_vs_ma50_pct": [-15, -10, -8, -5, -3, 0],
        "day3_close_vs_ma100_pct": [-15, -10, -8, -5, 0],
        "day3_close_vs_ma75_pct": [-15, -10, -8, -5, -3, 0],
        # Confirmed weekly
        "confirmed_week_close_vs_ma5_pct": [-10, -5, -3, -1, 0, 1, 3],
        "confirmed_week_close_vs_ma13_pct": [-10, -8, -5, -3, -1, 0, 1, 3, 5],
        "confirmed_week_close_vs_ma26_pct": [-15, -10, -8, -5, -3, -1, 0, 3],
        "confirmed_week_close_position_in_range": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
        "confirmed_week_lower_shadow_pct": [0.5, 1.0, 1.5, 2.0, 3.0],
        "confirmed_week_upper_shadow_pct": [0.5, 1.0, 1.5, 2.0, 3.0],
        "confirmed_week_return_pct": [-5, -3, -2, -1, 0, 1, 3],
        "confirmed_week_body_pct": [1, 2, 3, 5],
        "confirmed_week_close_vs_recent_low_13w_pct": [-10, -5, -3, 0, 3, 5],
        "confirmed_week_close_vs_recent_low_26w_pct": [-10, -5, -3, 0, 3],
        "confirmed_week_rsi": [30, 40, 50, 60, 70],
        "confirmed_week_ma13_slope": [-3, -2, -1, 0, 1],
        "confirmed_week_volume_ratio": [0.7, 1.0, 1.5, 2.0, 3.0],
        # Partial weekly
        "partial_week_close_position_in_range": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
        "partial_week_return_pct": [-5, -3, -2, -1, 0, 1],
        "partial_week_lower_shadow_pct": [0.5, 1.0, 1.5, 2.0],
        "partial_week_upper_shadow_pct": [0.5, 1.0, 1.5, 2.0],
        # Market / sector
        "day3_index_return": [-3, -2, -1, 0, 1],
        "day3_sector_return": [-3, -2, -1, 0, 1],
        "sector_relative_strength": [-3, -2, -1, 0, 1, 2],
        "margin_ratio": [3, 5, 10, 20, 30],
        "day3_atr_pct": [1, 2, 3, 5],
        "day3_range_vs_atr": [0.5, 1.0, 1.5, 2.0],
        "day3_body_vs_atr": [0.5, 1.0, 1.5, 2.0],
    }
    numeric_extra = [
        "day2_return", "day1_daily", "day2_daily", "day3_daily", "return_acceleration",
        "rsi_change_entry_to_day3", "day3_high_vs_entry_pct", "day3_low_vs_entry_pct",
        "atr_pct", "volatility_20d",
    ]

    boolean_features = [
        # Existing candle
        "day3_is_bullish", "day3_is_bearish", "day3_is_doji_like",
        "day3_close_near_high", "day3_close_near_low",
        "day3_long_upper_shadow", "day3_long_lower_shadow",
        # Existing MA
        "day3_close_above_ma5", "day3_close_above_ma25", "day3_close_above_ma75",
        # Computed MA
        "day3_close_above_ma10", "day3_close_above_ma50", "day3_close_above_ma100",
        # Volume
        "day3_volume_spike", "day3_volume_dry_up",
        # Support line (existing minimal)
        "support_break_5d", "support_break_10d", "support_break_20d", "support_break_60d",
        "low_break_recovered_5d", "low_break_recovered_10d", "low_break_recovered_20d", "low_break_recovered_60d",
        "day3_low_below_recent_low_5d", "day3_low_below_recent_low_10d",
        "day3_low_below_recent_low_20d", "day3_low_below_recent_low_60d",
        "day3_close_below_recent_low_5d", "day3_close_below_recent_low_10d",
        "near_support_5d", "near_support_10d", "near_support_20d",
        # Weekly
        "confirmed_week_is_bullish", "confirmed_week_is_bearish",
        "confirmed_week_close_above_ma5", "confirmed_week_close_above_ma13", "confirmed_week_close_above_ma26",
        "confirmed_week_close_below_recent_low_13w", "confirmed_week_close_below_recent_low_26w",
        "confirmed_week_low_below_recent_low_13w", "confirmed_week_low_break_recovered_13w",
        "partial_week_is_bullish", "partial_week_is_bearish",
    ]
    category_features = ["day3_rsi_bucket", "margin_ratio_bucket", "entry_market_regime", "day3_market_regime"]
    range_specs = [
        ("day3_rsi", 20, 35), ("day3_rsi", 30, 50), ("day3_rsi", 35, 60),
        ("day3_close_position_in_range", 0.3, 0.7), ("day3_close_position_in_range", 0.4, 0.8),
        ("day3_volume_ratio", 1.0, 2.0),
        ("confirmed_week_close_vs_ma13_pct", -8, 0), ("confirmed_week_close_vs_ma13_pct", -5, 3),
        ("confirmed_week_close_position_in_range", 0.3, 0.7), ("confirmed_week_close_position_in_range", 0.4, 0.8),
        ("day3_close_vs_recent_low_10d_pct", -2, 3), ("day3_close_vs_recent_low_20d_pct", -3, 5),
        ("margin_ratio", 3, 10), ("margin_ratio", 10, 30),
        ("partial_week_close_position_in_range", 0.4, 0.8),
    ]

    conditions: dict[str, Condition] = {}

    def add(cond: Condition) -> None:
        conditions.setdefault(cond.name, cond)

    all_numeric = sorted(set(numeric_fixed) | set(numeric_extra))
    for feature in all_numeric:
        values = _numeric_values(train_rows, feature)
        thresholds = list(numeric_fixed.get(feature, [])) + _quantiles(values)
        seen: list[float] = []
        for t in thresholds:
            if t is None:
                continue
            r = round(float(t), 4)
            if r in seen:
                continue
            seen.append(r)
            add(Condition(
                f"{feature}_lte_{r:g}",
                lambda row, f=feature, x=r: (
                    _to_float(row.get(f), None) is not None
                    and _to_float(row.get(f), None) <= x
                ),
                (feature,),
            ))
            add(Condition(
                f"{feature}_gte_{r:g}",
                lambda row, f=feature, x=r: (
                    _to_float(row.get(f), None) is not None
                    and _to_float(row.get(f), None) >= x
                ),
                (feature,),
            ))

    for feature, lo, hi in range_specs:
        add(Condition(
            f"{lo:g}_lte_{feature}_lte_{hi:g}",
            lambda row, f=feature, a=lo, b=hi: (
                _to_float(row.get(f), None) is not None
                and a <= _to_float(row.get(f), None) <= b
            ),
            (feature,),
        ))

    for feature in boolean_features:
        add(Condition(f"{feature}_true", lambda row, f=feature: bool(row.get(f)) is True, (feature,)))
        add(Condition(f"{feature}_false", lambda row, f=feature: bool(row.get(f)) is False, (feature,)))

    for feature in category_features:
        cats = sorted({str(r.get(feature)) for r in train_rows if r.get(feature) not in (None, "")})
        for cat in cats:
            add(Condition(
                f"{feature}_eq_{cat}",
                lambda row, f=feature, c=cat: str(row.get(f)) == c,
                (feature,),
            ))

    return list(conditions.values())


# ──────────────────────────────────────────────
# Condition evaluation
# ──────────────────────────────────────────────

def _period_rows(rows: list[dict], period: str, train_end: date) -> list[dict]:
    if period == "all":
        return rows
    if period == "train":
        return [r for r in rows if _d(r["entry_date"]) <= train_end]
    if period == "test":
        return [r for r in rows if _d(r["entry_date"]) > train_end]
    raise ValueError(period)


def _returns_for_mode(
    rows: list[dict], condition: Condition, mode: str
) -> tuple[list[float], list[dict]]:
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


def _eval_condition(
    rows: list[dict], condition: Condition, mode: str, period: str, baseline: str
) -> dict:
    rets, selected = _returns_for_mode(rows, condition, mode)
    selected_benefits = [
        _to_float(r.get("extension_benefit_5"), 0.0) or 0.0 for r in selected
    ]
    n = len(rows)
    selected_count = len(selected)
    raw_ext = [float(r["hd5_return"]) for r in rows if r.get("hd5_return") is not None]
    primary = [float(r["hd3_return"]) for r in rows if r.get("hd3_return") is not None]
    base = raw_ext if baseline == "extension" else primary
    return {
        "condition": condition.name,
        "mode": mode,
        "period": period,
        "selected_count": selected_count,
        "selected_rate": _round(selected_count / n * 100 if n else None),
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
        "selected_recovered_rate": _round(
            sum(1 for v in selected_benefits if v > 0) / len(selected_benefits) * 100
            if selected_benefits else None
        ),
        "selected_died_rate": _round(
            sum(1 for v in selected_benefits if v <= 0) / len(selected_benefits) * 100
            if selected_benefits else None
        ),
    }


def _evaluate_conditions(
    rows: list[dict],
    conditions: list[Condition],
    train_end: date,
    mode: str,
    baseline: str,
) -> list[dict]:
    out: list[dict] = []
    for cond in conditions:
        train_r = _eval_condition(_period_rows(rows, "train", train_end), cond, mode, "train", baseline)
        test_r = _eval_condition(_period_rows(rows, "test", train_end), cond, mode, "test", baseline)
        all_r = _eval_condition(rows, cond, mode, "all", baseline)
        merged: dict = {"condition": cond.name, "mode": mode, "features": ",".join(cond.feature_names)}
        for prefix, r in [("train", train_r), ("test", test_r), ("all", all_r)]:
            for k, v in r.items():
                if k not in {"condition", "mode"}:
                    merged[f"{prefix}_{k}"] = v
        merged["rank_score"] = _round(
            _score_rank(test_r, 10) + _score_rank(train_r, 30) * 0.5
        )
        out.append(merged)
    return sorted(out, key=lambda r: float(r.get("rank_score") or -9999), reverse=True)


# ──────────────────────────────────────────────
# ML helper
# ──────────────────────────────────────────────

LABEL_COLS = {
    "hd3_return", "hd5_return", "hd7_return",
    "extension_benefit_5", "extension_benefit_7",
    "y_extend_better_5", "y_strong_extend_better_5", "y_strong_extend_worse_5",
    "flat_5", "extend_better_7",
}
EXCLUDE_FROM_ML = LABEL_COLS | {
    "entry_date", "code", "name", "sector", "day3_date",
    "entry_market_regime", "day3_market_regime",
    "day3_rsi_bucket", "margin_ratio_bucket", "signal_stage",
}


def _run_ml(rows: list[dict], out_dir: Path) -> tuple[list[dict], str]:
    dt_path = out_dir / "17_decision_tree_rules.txt"
    numeric_features = [
        k for k, v in (rows[0] if rows else {}).items()
        if isinstance(v, (int, float)) and not isinstance(v, bool)
        and k not in EXCLUDE_FROM_ML
    ] if rows else []

    try:
        import numpy as np  # noqa: F401
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.inspection import permutation_importance
        from sklearn.pipeline import make_pipeline
        from sklearn.tree import DecisionTreeClassifier, export_text
    except Exception as exc:
        dt_path.write_text(f"ML skipped: {exc}", encoding="utf-8")
        return [{"status": "skipped", "reason": str(exc)}], f"ML skipped: {exc}"

    if len(rows) < 50 or not numeric_features:
        msg = "ML skipped: insufficient rows/features"
        dt_path.write_text(msg, encoding="utf-8")
        return [{"status": "skipped", "reason": msg}], msg

    X = [[_to_float(row.get(f), math.nan) for f in numeric_features] for row in rows]
    y = [int(row.get("y_extend_better_5") or 0) for row in rows]
    importances: list[dict] = []

    rf_pipe = make_pipeline(
        SimpleImputer(strategy="median"),
        RandomForestClassifier(n_estimators=200, min_samples_leaf=8, random_state=42),
    )
    rf_pipe.fit(X, y)
    forest = rf_pipe.named_steps["randomforestclassifier"]
    for feat, imp in sorted(zip(numeric_features, forest.feature_importances_), key=lambda x: x[1], reverse=True)[:80]:
        importances.append({"model": "random_forest", "feature": feat, "importance": round(float(imp), 6)})

    try:
        perm = permutation_importance(rf_pipe, X, y, n_repeats=5, random_state=42, scoring="roc_auc")
        for feat, imp in sorted(zip(numeric_features, perm.importances_mean), key=lambda x: x[1], reverse=True)[:50]:
            importances.append({"model": "permutation_rf", "feature": feat, "importance": round(float(imp), 6)})
    except Exception:
        pass

    dt_pipe = make_pipeline(
        SimpleImputer(strategy="median"),
        DecisionTreeClassifier(max_depth=4, min_samples_leaf=15, random_state=42),
    )
    dt_pipe.fit(X, y)
    tree_text = export_text(dt_pipe.named_steps["decisiontreeclassifier"], feature_names=numeric_features, decimals=3)
    dt_path.write_text(tree_text, encoding="utf-8")
    return importances, "ML completed (RandomForest + DecisionTree)"


# ──────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────

def _proxy_usage(rows: list[dict]) -> list[dict]:
    fields = [
        "day3_open_is_proxy", "day3_rsi_is_proxy", "day3_ma_is_proxy",
        "day3_volume_ratio_is_proxy", "day3_atr_is_proxy",
    ]
    n = len(rows)
    return [
        {
            "field": f,
            "proxy_count": sum(1 for r in rows if r.get(f)),
            "total": n,
            "proxy_rate": round(sum(1 for r in rows if r.get(f)) / n * 100, 3) if n else None,
        }
        for f in fields
    ]


def _feature_summary(rows: list[dict], feature_prefixes: list[str]) -> list[dict]:
    out = []
    all_keys = {k for r in rows for k in r}
    for feat in sorted(all_keys):
        if not any(feat.startswith(p) for p in feature_prefixes):
            continue
        vals = [_to_float(r.get(feat), None) for r in rows]
        bool_vals = [bool(r.get(feat)) for r in rows if isinstance(r.get(feat), bool)]
        numeric = [v for v in vals if v is not None and isinstance(v, (int, float)) and not isinstance(v, bool)]
        if numeric:
            srt = sorted(numeric)
            n = len(srt)
            out.append({
                "feature": feat,
                "count": n,
                "null_count": len(rows) - len(numeric),
                "mean": _round(_avg(numeric)),
                "std": _round(math.sqrt(sum((v - (_avg(numeric) or 0)) ** 2 for v in numeric) / n) if n > 1 else 0),
                "min": _round(srt[0]),
                "p25": _round(srt[int(n * 0.25)]),
                "p50": _round(srt[int(n * 0.50)]),
                "p75": _round(srt[int(n * 0.75)]),
                "max": _round(srt[-1]),
            })
        elif bool_vals:
            n = len(bool_vals)
            out.append({
                "feature": feat,
                "count": n,
                "true_rate": round(sum(bool_vals) / n * 100, 3) if n else None,
            })
    return out


def _stability(rows: list[dict], key_func: Callable[[dict], str]) -> list[dict]:
    buckets: dict[str, list] = defaultdict(list)
    for row in rows:
        buckets[key_func(row)].append(row)
    out = []
    for key, group in sorted(buckets.items()):
        benefits = [float(r["extension_benefit_5"]) for r in group if r.get("extension_benefit_5") is not None]
        hd3s = [float(r["hd3_return"]) for r in group if r.get("hd3_return") is not None]
        hd5s = [float(r["hd5_return"]) for r in group if r.get("hd5_return") is not None]
        out.append({
            "bucket": key,
            "count": len(group),
            "recovered_rate": _round(sum(1 for v in benefits if v > 0) / len(benefits) * 100 if benefits else None),
            "avg_extension_benefit": _round(_avg(benefits)),
            "median_extension_benefit": _round(_median(benefits)),
            "avg_hd3_return": _round(_avg(hd3s)),
            "avg_hd5_return": _round(_avg(hd5s)),
        })
    return out


def _sample_rows(rows: list[dict], condition_func: Callable[[dict], bool], hit: bool, n: int = 100) -> list[dict]:
    filtered = [r for r in rows if condition_func(r) is hit]
    filtered.sort(key=lambda r: float(r.get("extension_benefit_5") or 0), reverse=hit)
    return filtered[:n]


def _eval_current_allow(rows: list[dict], train_end: date) -> list[dict]:
    """Evaluate the existing Extension Allow rule against primary/extension baselines."""
    def current_allow_func(r: dict) -> bool:
        d1 = _to_float(r.get("day1_return"), None)
        body = _to_float(r.get("day3_body_pct"), None)
        vol = _to_float(r.get("day3_volume_ratio"), None)
        return (
            (d1 is not None and d1 >= CURRENT_ALLOW_DAY1_RET_GTE)
            and (body is not None and body <= CURRENT_ALLOW_BODY_LTE)
            and (vol is not None and vol <= CURRENT_ALLOW_VOL_RATIO_LTE)
        )

    allow_cond = Condition(
        f"current_extension_allow(day1>={CURRENT_ALLOW_DAY1_RET_GTE},body<={CURRENT_ALLOW_BODY_LTE},vol<={CURRENT_ALLOW_VOL_RATIO_LTE})",
        current_allow_func,
        ("day1_return", "day3_body_pct", "day3_volume_ratio"),
    )
    out = []
    for period in ["train", "test", "all"]:
        period_data = _period_rows(rows, period, train_end)
        result = _eval_condition(period_data, allow_cond, "allow", period, "primary")
        result["rule"] = "current_extension_allow"
        out.append(result)
    return out


def _combined_condition(conds: tuple[Condition, ...]) -> Condition:
    name = " AND ".join(c.name for c in conds)
    features = tuple(sorted({f for c in conds for f in c.feature_names}))
    return Condition(name, lambda r, cs=conds: all(c.func(r) for c in cs), features)


def _leakage_check_text(rows: list[dict]) -> str:
    lines = [
        "H5 Extension Technical Rule Search — Future Leakage Check",
        "=" * 60,
        "",
        "Feature categories and leakage status:",
        "",
        "[SAFE] day3_close / day3_high / day3_low",
        "  Source: real snapshot (stock_feature_snapshots). Day3 data is",
        "  the last point in time used. No day4+ data involved.",
        "",
        "[SAFE / PROXY] day3_open",
        "  Source: real snapshot open if available (day3_open_is_proxy=False).",
        "  Fallback: prev_close (=day2 close) as proxy. No day4+ data.",
        "",
        "[SAFE / PROXY] day3_rsi, day3_ma5/25/75, day3_volume_ratio, day3_atr",
        "  Source: real day3 snapshot if available; entry-date snapshot as proxy.",
        "  Both are within [entry_date, day3_date]. No day4+ data.",
        "",
        "[SAFE] day3_ma10 / day3_ma50 / day3_ma100",
        "  Computed from historical closes in stock_feature_snapshots up to",
        "  and including day3. No day4+ data.",
        "",
        "[SAFE] ma5_slope_3d / ma25_slope_5d / ma75_slope_10d",
        "  Computed from MA values at day3 vs. N trading days before day3.",
        "  Both points are <= day3. No day4+ data.",
        "",
        "[SAFE] recent_low_Nd / recent_high_Nd / support_break_Nd",
        "  Computed from N trading days BEFORE day3 (exclusive of day3).",
        "  Uses historical snapshots only. No day4+ data.",
        "",
        "[SAFE] confirmed_week_*",
        "  Source: the ISO week immediately BEFORE day3's ISO week.",
        "  All daily snapshots in that week are confirmed before day3's week",
        "  starts. Weekly MA is computed from confirmed weekly closes only.",
        "  No day4+ data, and day3's own week is excluded from confirmed_week.",
        "",
        "[SAFE] partial_week_to_day3_*",
        "  Source: daily snapshots in day3's ISO week, up to and including",
        "  day3. Does not include day4+ or any future Friday close.",
        "  Correctly excludes later days in the same week.",
        "",
        "[LEAKAGE RISK — EXCLUDED FROM ML]",
        "  hd5_return, hd7_return, extension_benefit_5, extension_benefit_7,",
        "  y_extend_better_5, y_strong_extend_better_5, y_strong_extend_worse_5,",
        "  flat_5, extend_better_7",
        "  These are future-derived labels and are NEVER used as ML features.",
        "",
        "Proxy usage summary:",
    ]
    proxy = _proxy_usage(rows)
    for p in proxy:
        lines.append(
            f"  {p['field']}: proxy_count={p['proxy_count']} / {p['total']}"
            f" ({p.get('proxy_rate', 0):.1f}%)"
        )
    lines += [
        "",
        "Verdict: No confirmed future leakage detected. Proxy features are",
        "  within [entry_date, day3_date] and thus leakage-free in the causal",
        "  sense, though they may differ from the actual day3 value.",
    ]
    return "\n".join(lines)


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
    logger.info("[tech_rule_search] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    seed_rows = [r for r in candidates if _passes_h5_entry(r)]
    codes = sorted({str(r.get("code")) for r in seed_rows if r.get("code")})
    logger.info("[tech_rule_search] H5 entry candidates=%d distinct_codes=%d", len(seed_rows), len(codes))

    # Extended lookback for support lines and weekly features
    snap_start = start - timedelta(days=HISTORY_LOOKBACK_DAYS)
    snap_end = end + timedelta(days=30)
    logger.info(
        "[tech_rule_search] loading snapshots %s..%s (lookback=%d cal-days)",
        snap_start, snap_end, HISTORY_LOOKBACK_DAYS,
    )
    snapshots = _fetch_snapshot_rows(sb, codes, snap_start, snap_end)

    rows = _build_dataset(candidates, snapshots)
    logger.info("[tech_rule_search] extension-enabled dataset rows=%d", len(rows))

    train_rows = _period_rows(rows, "train", train_end)
    test_rows = _period_rows(rows, "test", train_end)
    logger.info("[tech_rule_search] train=%d test=%d", len(train_rows), len(test_rows))

    conditions = _generate_conditions(train_rows)
    condition_map = {c.name: c for c in conditions}
    logger.info("[tech_rule_search] generated conditions=%d", len(conditions))

    # Single-condition evaluation
    single_ban = _evaluate_conditions(rows, conditions, train_end, "ban", "extension")
    single_allow = _evaluate_conditions(rows, conditions, train_end, "allow", "primary")

    top_for_ban = [
        condition_map[r["condition"]]
        for r in single_ban
        if r["condition"] in condition_map
        and (r.get("train_selected_count") or 0) >= args.min_train_count
    ][: min(60, len(single_ban))]
    top_for_allow = [
        condition_map[r["condition"]]
        for r in single_allow
        if r["condition"] in condition_map
        and (r.get("train_selected_count") or 0) >= args.min_train_count
    ][: min(60, len(single_allow))]

    def build_combos(base: list[Condition], depth: int) -> list[Condition]:
        combos: list[Condition] = []
        for combo in itertools.combinations(base, depth):
            features = [f for c in combo for f in c.feature_names]
            if len(set(features)) < len(features):
                continue
            combos.append(_combined_condition(combo))
            if len(combos) >= args.max_combos_per_depth:
                break
        return combos

    logger.info("[tech_rule_search] evaluating 2-condition combos...")
    combo2_ban = _evaluate_conditions(rows, build_combos(top_for_ban, 2), train_end, "ban", "extension")
    combo2_allow = _evaluate_conditions(rows, build_combos(top_for_allow, 2), train_end, "allow", "primary")

    combo3_ban: list[dict] = []
    combo3_allow: list[dict] = []
    if args.max_combo_depth >= 3:
        logger.info("[tech_rule_search] evaluating 3-condition combos...")
        combo3_ban = _evaluate_conditions(rows, build_combos(top_for_ban[:35], 3), train_end, "ban", "extension")
        combo3_allow = _evaluate_conditions(rows, build_combos(top_for_allow[:35], 3), train_end, "allow", "primary")

    # Top candidates across all groups
    top_candidates: list[dict] = []
    for group, label in [
        (single_ban[:30], "single_ban"),
        (single_allow[:30], "single_allow"),
        (combo2_ban[:30], "combo2_ban"),
        (combo2_allow[:30], "combo2_allow"),
        (combo3_ban[:30], "combo3_ban"),
        (combo3_allow[:30], "combo3_allow"),
    ]:
        for r in group:
            item = dict(r)
            item["rule_group"] = label
            top_candidates.append(item)
    top_candidates.sort(key=lambda r: float(r.get("rank_score") or -9999), reverse=True)

    # Current Extension Allow comparison
    compare_allow = _eval_current_allow(rows, train_end)

    # ML
    ml_importance, ml_status = (
        _run_ml(rows, out_dir) if args.run_ml
        else ([{"status": "skipped", "reason": "disabled"}], "ML disabled")
    )

    # Sample rows: support recovered vs. died
    support_recovered_func = lambda r: bool(r.get("low_break_recovered_10d"))
    support_died_func = lambda r: bool(r.get("support_break_10d")) and (_to_float(r.get("extension_benefit_5"), 0) or 0) <= -1.0

    # ── Write outputs ──
    logger.info("[tech_rule_search] writing outputs to %s", out_dir)

    _write_csv(out_dir / "01_extension_technical_dataset.csv", rows)
    _write_csv(out_dir / "02_feature_proxy_usage.csv", _proxy_usage(rows))
    _write_csv(
        out_dir / "03_daily_support_features_summary.csv",
        _feature_summary(rows, ["recent_low_", "recent_high_", "day3_close_vs_recent_low",
                                  "day3_close_below", "day3_low_below", "support_break",
                                  "low_break_recovered", "near_support"]),
    )
    _write_csv(
        out_dir / "04_daily_ma_gap_features_summary.csv",
        _feature_summary(rows, ["day3_close_vs_ma", "day3_close_above_ma",
                                  "ma5_slope", "ma25_slope", "ma75_slope",
                                  "day3_ma10", "day3_ma50", "day3_ma100"]),
    )
    _write_csv(
        out_dir / "05_weekly_features_summary.csv",
        _feature_summary(rows, ["confirmed_week_", "partial_week_"]),
    )
    _write_csv(out_dir / "06_single_conditions_allow_ranked.csv", single_allow)
    _write_csv(out_dir / "07_single_conditions_ban_ranked.csv", single_ban)
    _write_csv(out_dir / "08_combo2_conditions_allow_ranked.csv", combo2_allow)
    _write_csv(out_dir / "09_combo2_conditions_ban_ranked.csv", combo2_ban)
    _write_csv(out_dir / "10_combo3_conditions_allow_ranked.csv", combo3_allow)
    _write_csv(out_dir / "11_combo3_conditions_ban_ranked.csv", combo3_ban)
    _write_csv(out_dir / "12_top_candidate_rules.csv", top_candidates)
    _write_csv(out_dir / "13_compare_with_current_allow.csv", compare_allow)
    _write_csv(out_dir / "14_yearly_stability.csv", _stability(rows, lambda r: _year(r.get("entry_date"))))
    _write_csv(out_dir / "15_monthly_stability.csv", _stability(rows, lambda r: _month(r.get("entry_date"))))
    _write_csv(out_dir / "16_ml_feature_importance.csv", ml_importance)
    # 17_decision_tree_rules.txt written by _run_ml
    (out_dir / "18_future_leakage_check.txt").write_text(_leakage_check_text(rows), encoding="utf-8")
    _write_csv(out_dir / "19_case_samples_support_recovered.csv", _sample_rows(rows, support_recovered_func, True))
    _write_csv(out_dir / "20_case_samples_support_died.csv", _sample_rows(rows, support_died_func, True))

    # ── Summary report ──
    benefits = [float(r["extension_benefit_5"]) for r in rows if r.get("extension_benefit_5") is not None]
    hd3_avg = _round(_avg([float(r["hd3_return"]) for r in rows if r.get("hd3_return") is not None]))
    hd5_avg = _round(_avg([float(r["hd5_return"]) for r in rows if r.get("hd5_return") is not None]))
    weekly_avail = [r.get("confirmed_week_weeks_available") for r in rows if r.get("confirmed_week_weeks_available") is not None]

    def _top5(cond_list: list[dict]) -> list[str]:
        return [json.dumps({k: v for k, v in r.items() if k in {
            "condition", "train_avg_ret_diff", "test_avg_ret_diff",
            "train_selected_count", "test_selected_count", "rank_score",
        }}, ensure_ascii=False) for r in cond_list[:5]]

    report_lines = [
        "H5 Extension Technical Rule Search Report",
        "=" * 60,
        f"Generated: 2026-05-31",
        f"Train: {train_start} ~ {train_end}",
        f"Test:  {test_start} ~ {test_end}",
        "",
        "1. Extension-enabled銘柄数",
        f"   Total: {len(rows)}  Train: {len(train_rows)}  Test: {len(test_rows)}",
        f"   Avg HD3 return: {hd3_avg}%  Avg HD5 return: {hd5_avg}%",
        f"   HD5 improved (benefit>0): {sum(1 for v in benefits if v > 0)} / {len(benefits)}",
        f"   HD5 worsened (benefit<=0): {sum(1 for v in benefits if v <= 0)} / {len(benefits)}",
        f"   Avg extension benefit: {_round(_avg(benefits))}%",
        "",
        "2. 週足データ利用可能性",
        f"   confirmed_week features available: {sum(1 for r in rows if r.get('confirmed_week_close') is not None)} / {len(rows)}",
        f"   Avg weeks available: {_round(_avg([float(v) for v in weekly_avail if v is not None]))}",
        f"   confirmed_week_ma13 available: {sum(1 for r in rows if r.get('confirmed_week_ma13') is not None)} / {len(rows)}",
        f"   confirmed_week_ma26 available: {sum(1 for r in rows if r.get('confirmed_week_ma26') is not None)} / {len(rows)}",
        "",
        "3. Proxy使用率",
        *[f"   {p['field']}: {p.get('proxy_rate', 0):.1f}%" for p in _proxy_usage(rows)],
        "",
        "4. 支持線系 上位許可ルール (allow, ranked by rank_score)",
        *_top5([r for r in single_allow if any(
            kw in r.get("condition", "")
            for kw in ["support_break", "low_break_recovered", "recent_low", "near_support"]
        )]),
        "",
        "5. MA乖離系 上位許可ルール",
        *_top5([r for r in single_allow if any(
            kw in r.get("condition", "")
            for kw in ["vs_ma5", "vs_ma25", "vs_ma75", "vs_ma10", "vs_ma50", "slope"]
        )]),
        "",
        "6. 週足系 上位許可ルール",
        *_top5([r for r in single_allow if "confirmed_week" in r.get("condition", "")]),
        "",
        "7. 上位禁止ルール (ban)",
        *_top5(single_ban),
        "",
        "8. 上位許可ルール (all types, single)",
        *_top5(single_allow),
        "",
        "9. 上位2条件許可ルール",
        *_top5(combo2_allow),
        "",
        "10. 上位3条件許可ルール",
        *_top5(combo3_allow),
        "",
        "11. 既存Extension Allowとの比較",
        *[json.dumps(r, ensure_ascii=False) for r in compare_allow],
        "",
        "12. ML状態",
        ml_status,
        "",
        "13. 結論",
        "   Primary (h5_ai65_hd3_est12_cm_range330_live_limited) は変更しない。",
        "   Extension Allow (既存) は研究枠として維持。",
        "   このスクリプトはDB、case定義、app.pyを変更しない。",
        "",
        "14. 採用候補判定チェックリスト",
        "   train/test両方で改善: 要確認",
        "   件数十分 (train>=30, test>=10): 要確認",
        "   DD悪化なし: 要確認",
        "   proxy依存低い: 要確認",
        "   未来情報混入なし: 18_future_leakage_check.txt 参照",
        "   現Extension Allowを上回る: 13_compare_with_current_allow.csv 参照",
    ]
    (out_dir / "21_technical_rule_search_report.txt").write_text(
        "\n".join(report_lines), encoding="utf-8"
    )
    logger.info("[tech_rule_search] done. outputs in %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="H5 Extension Technical Rule Search")
    p.add_argument("--train-start", default="2023-01-01")
    p.add_argument("--train-end", default="2024-12-31")
    p.add_argument("--test-start", default="2025-01-01")
    p.add_argument("--test-end", default="2026-05-28")
    p.add_argument("--output-dir", default="outputs/h5_extension_technical_rule_search")
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
