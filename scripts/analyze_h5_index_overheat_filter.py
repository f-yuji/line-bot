#!/usr/bin/env python3
"""
analyze_h5_index_overheat_filter.py
H5 Index Overheat Entry Filter Analysis

指数過熱日にH5へ入ると弱いのか、反発初動で強いのかを検証する。
今回は分析のみ。Primary / DB / UI / LINE 変更なし。
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

from services.h5_primary import h5_overheat_score
from services.trade_case_tester import (
    _build_supabase,
    _load_candidates_v2,
    _to_float,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
logger = logging.getLogger("idx_overheat")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("peewee").setLevel(logging.WARNING)

EST12_STOP = -0.12
H5_PRIMARY_KEY = "h5_ai65_hd3_est12_cm_range330_live_limited"


# ──────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────

def _d(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
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


def _avg(vals: list) -> float | None:
    v = [x for x in vals if x is not None]
    return sum(v) / len(v) if v else None


def _pf(vals: list) -> float | None:
    v = [x for x in vals if x is not None]
    w = sum(x for x in v if x > 0)
    l = abs(sum(x for x in v if x <= 0))
    if l <= 0:
        return None if w <= 0 else 999.0
    return w / l


def _wr(vals: list) -> float | None:
    v = [x for x in vals if x is not None]
    return sum(1 for x in v if x > 0) / len(v) * 100 if v else None


def _max_dd(vals: list) -> float:
    v = [x for x in vals if x is not None]
    eq = pk = dd = 0.0
    for x in v:
        eq += x
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
            w.writerow({k: _r(v) if isinstance(v, float) else v for k, v in row.items()})


def _month_key(entry_date_str: str) -> str:
    return str(entry_date_str)[:7]  # "YYYY-MM"


def _split(rows: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    train = [r for r in rows if _d(r["entry_date"]) <= train_end]
    test = [r for r in rows if _d(r["entry_date"]) > train_end]
    return train, test


# ──────────────────────────────────────────────
# H5 Entry Filter
# ──────────────────────────────────────────────

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


def _est12_result(row: dict, entry: float, hold: int) -> dict:
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


# ──────────────────────────────────────────────
# Index Data Loading
# ──────────────────────────────────────────────

def _compute_rsi14(closes: list[float]) -> list[float | None]:
    """Wilder RSI-14. Returns list same length as closes."""
    n = len(closes)
    rsi: list[float | None] = [None] * n
    if n < 16:
        return rsi
    rets = [(closes[i] - closes[i-1]) / closes[i-1] * 100.0 for i in range(1, n)]
    gains = [max(0.0, r) for r in rets]
    losses = [max(0.0, -r) for r in rets]
    # First 14 returns: indices 0..13 in rets = closes[1..14]
    avg_g = sum(gains[:14]) / 14
    avg_l = sum(losses[:14]) / 14
    rs = avg_g / avg_l if avg_l > 0 else float('inf')
    rsi[14] = 100 - 100 / (1 + rs)
    for i in range(15, n):
        avg_g = (avg_g * 13 + gains[i - 1]) / 14
        avg_l = (avg_l * 13 + losses[i - 1]) / 14
        rs = avg_g / avg_l if avg_l > 0 else float('inf')
        rsi[i] = 100 - 100 / (1 + rs)
    return rsi


def _load_index_data(analysis_start: date, analysis_end: date) -> dict[str, dict]:
    """
    Fetch Nikkei225 OHLC via yfinance.
    Returns dict keyed by date_str with all computed features.
    Features are computed from trade_date and earlier only (no future leak for filters).
    next_return_* are included but labeled for evaluation only.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed. Run: pip install yfinance")
        return {}

    warmup_start = analysis_start - timedelta(days=200)  # ~150 trading days for MA75
    fetch_end = analysis_end + timedelta(days=10)

    logger.info("[idx] fetching Nikkei225 (^N225) %s to %s", warmup_start, fetch_end)
    try:
        hist = yf.Ticker("^N225").history(
            start=warmup_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
    except Exception as e:
        logger.error("[idx] yfinance fetch failed: %s", e)
        return {}

    if hist.empty:
        logger.error("[idx] yfinance returned empty dataframe for ^N225")
        return {}

    records: list[dict] = []
    for ts, row in hist.iterrows():
        d = ts.date()
        records.append({
            "date": d,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
        })
    records.sort(key=lambda r: r["date"])
    n = len(records)
    logger.info("[idx] raw OHLC records=%d (%s to %s)", n, records[0]["date"], records[-1]["date"])

    closes = [r["close"] for r in records]
    opens_ = [r["open"] for r in records]

    # 1d return
    for i in range(n):
        records[i]["return_1d"] = (closes[i] / closes[i-1] - 1) * 100 if i >= 1 else None

    # Multi-day returns
    for days in [2, 3, 5]:
        for i in range(n):
            records[i][f"return_{days}d"] = (closes[i] / closes[i-days] - 1) * 100 if i >= days else None

    # MA & MA gap
    for window in [5, 25, 75]:
        for i in range(n):
            if i >= window - 1:
                ma = sum(closes[i-window+1:i+1]) / window
                records[i][f"ma{window}"] = ma
                records[i][f"close_vs_ma{window}_pct"] = (closes[i] / ma - 1) * 100
            else:
                records[i][f"ma{window}"] = None
                records[i][f"close_vs_ma{window}_pct"] = None

    # RSI14
    rsi_vals = _compute_rsi14(closes)
    for i in range(n):
        records[i]["rsi14"] = rsi_vals[i]

    # Gap (open vs prev close) — entry_date当日の情報なので使用可能
    for i in range(n):
        records[i]["gap_pct"] = (opens_[i] / closes[i-1] - 1) * 100 if i >= 1 else None

    # Intraday position
    for r in records:
        h, l, c = r["high"], r["low"], r["close"]
        r["intraday_position"] = (c - l) / (h - l) if h > l else 0.5

    # Recent high/low
    for window in [5, 20]:
        for i in range(n):
            if i >= window - 1:
                hi = max(closes[i-window+1:i+1])
                lo = min(closes[i-window+1:i+1])
                records[i][f"close_vs_recent_high_{window}d_pct"] = (closes[i] / hi - 1) * 100
                records[i][f"close_vs_recent_low_{window}d_pct"] = (closes[i] / lo - 1) * 100
            else:
                records[i][f"close_vs_recent_high_{window}d_pct"] = None
                records[i][f"close_vs_recent_low_{window}d_pct"] = None

    # ATR (5d average of daily range %)
    for i in range(n):
        if i >= 4:
            ranges = [abs(records[j]["high"] - records[j]["low"]) / closes[j] * 100
                      for j in range(i-4, i+1)]
            records[i]["atr_pct"] = sum(ranges) / 5
        else:
            records[i]["atr_pct"] = None

    # Daily range
    for r in records:
        r["range_pct"] = (r["high"] - r["low"]) / r["close"] * 100

    # Overheat score (7 components)
    def _overheat(r: dict) -> int:
        score = 0
        r1d = r.get("return_1d")
        r2d = r.get("return_2d")
        ma5g = r.get("close_vs_ma5_pct")
        ma25g = r.get("close_vs_ma25_pct")
        rsi = r.get("rsi14")
        gap = r.get("gap_pct")
        ipos = r.get("intraday_position")
        if r1d is not None and r1d >= 1.5:
            score += 1
        if r2d is not None and r2d >= 2.5:
            score += 1
        if ma5g is not None and ma5g >= 1.5:
            score += 1
        if ma25g is not None and ma25g >= 3.0:
            score += 1
        if rsi is not None and rsi >= 65:
            score += 1
        if gap is not None and gap >= 0.5:
            score += 1
        if ipos is not None and ipos >= 0.8:
            score += 1
        return score

    for r in records:
        r["overheat_score"] = _overheat(r)

    # Rebound flags
    for r in records:
        r1d = r.get("return_1d")
        r3d = r.get("return_3d")
        r5d = r.get("return_5d")
        ma5g = r.get("close_vs_ma5_pct")
        oh = r.get("overheat_score", 0)

        recent_down = (r3d is not None and r3d <= -2.0) or (r5d is not None and r5d <= -3.0)
        rebound_today = (r1d is not None and r1d >= 0.3) and (r3d is not None and r3d <= 0)
        down_then_rebound = (r5d is not None and r5d <= -2.0) and (r1d is not None and r1d >= 0.5)
        not_overheated = oh <= 1
        rebound_setup = down_then_rebound and not_overheated
        weak_stabilizing = (
            r5d is not None and r5d < 0 and
            r1d is not None and -0.3 <= r1d <= 1.0 and
            ma5g is not None and -2.0 <= ma5g <= 1.0
        )
        r["recent_down"] = recent_down
        r["rebound_today"] = rebound_today
        r["down_then_rebound"] = down_then_rebound
        r["not_overheated"] = not_overheated
        r["rebound_setup"] = rebound_setup
        r["weak_stabilizing"] = weak_stabilizing

    # next_return (evaluation only — NOT for filter conditions)
    for i in range(n):
        for days in [1, 2, 3]:
            future_i = i + days
            records[i][f"next_return_{days}d"] = (
                (closes[future_i] / closes[i] - 1) * 100 if future_i < n else None
            )

    # Build output dict keyed by date_str
    result: dict[str, dict] = {}
    for r in records:
        ds = r["date"].isoformat()
        result[ds] = r

    logger.info("[idx] index features computed for %d dates", len(result))
    return result


# ──────────────────────────────────────────────
# Dataset Assembly
# ──────────────────────────────────────────────

def _build_dataset(candidates: list[dict], index_data: dict[str, dict]) -> list[dict]:
    dataset: list[dict] = []
    missing_idx = 0
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry or entry <= 0:
            continue

        td = str(row.get("trade_date") or "")
        idx = index_data.get(td, {})
        if not idx:
            missing_idx += 1

        # Base fields
        rec: dict = {
            "entry_date": td,
            "code": str(row.get("code") or ""),
            "name": row.get("name"),
            "sector": str(row.get("sector") or ""),
            "market_regime": str(row.get("market_regime") or ""),
            "entry_price": entry,
            "signal_probability": _to_float(row.get("signal_probability"), None),
            "signal_stage": row.get("signal_stage"),
            "h5_overheat_score": h5_overheat_score(row),
            "volume_ratio": _to_float(row.get("volume_ratio_20d"), None),
            "drop_from_20d_high_pct": _to_float(row.get("drop_from_20d_high_pct"), None),
            "margin_ratio": _to_float(row.get("margin_ratio"), None),
        }

        # Returns
        for hold in [1, 2, 3, 5, 7, 10]:
            rec[f"hd{hold}_ret_raw"] = _r(_raw_ret(row, entry, hold))
        est3 = _est12_result(row, entry, 3)
        rec["hd3_ret_est12"] = _r(est3.get("ret"))
        rec["hd3_exit_reason"] = est3.get("reason")
        rec["emergency_stop"] = est3.get("reason") == "emergency_stop"

        # Index features (entry_date時点で利用可能)
        rec["index_return_1d"] = _r(idx.get("return_1d"))
        rec["index_return_2d"] = _r(idx.get("return_2d"))
        rec["index_return_3d"] = _r(idx.get("return_3d"))
        rec["index_return_5d"] = _r(idx.get("return_5d"))
        rec["index_close_vs_ma5_pct"] = _r(idx.get("close_vs_ma5_pct"))
        rec["index_close_vs_ma25_pct"] = _r(idx.get("close_vs_ma25_pct"))
        rec["index_close_vs_ma75_pct"] = _r(idx.get("close_vs_ma75_pct"))
        rec["index_rsi14"] = _r(idx.get("rsi14"))
        rec["index_gap_pct"] = _r(idx.get("gap_pct"))
        rec["index_intraday_position"] = _r(idx.get("intraday_position"))
        rec["index_overheat_score"] = idx.get("overheat_score")
        rec["index_rebound_setup"] = idx.get("rebound_setup")
        rec["index_weak_stabilizing"] = idx.get("weak_stabilizing")
        rec["index_recent_down"] = idx.get("recent_down")
        rec["index_down_then_rebound"] = idx.get("down_then_rebound")
        rec["index_close_vs_recent_high_5d_pct"] = _r(idx.get("close_vs_recent_high_5d_pct"))
        rec["index_close_vs_recent_high_20d_pct"] = _r(idx.get("close_vs_recent_high_20d_pct"))
        rec["index_close_vs_recent_low_5d_pct"] = _r(idx.get("close_vs_recent_low_5d_pct"))
        rec["index_close_vs_recent_low_20d_pct"] = _r(idx.get("close_vs_recent_low_20d_pct"))

        # next_index_return — 評価用のみ (filterには使わない)
        rec["next_index_return_1d"] = _r(idx.get("next_return_1d"))
        rec["next_index_return_2d"] = _r(idx.get("next_return_2d"))
        rec["next_index_return_3d"] = _r(idx.get("next_return_3d"))

        rec["has_index_data"] = bool(idx)
        dataset.append(rec)

    if missing_idx > 0:
        logger.warning("[dataset] %d rows had no index data (holiday/weekend mismatch)", missing_idx)
    return dataset


# ──────────────────────────────────────────────
# Performance Stats
# ──────────────────────────────────────────────

def _perf(rows: list[dict], label: str, period: str) -> dict:
    n = len(rows)
    out: dict = {"group": label, "period": period, "n": n}
    if n == 0:
        return out
    for hold in [1, 2, 3, 5, 7, 10]:
        col = f"hd{hold}_ret_raw"
        vals = [_to_float(r.get(col), None) for r in rows if r.get(col) is not None]
        out[f"HD{hold}_avg"] = _r(_avg(vals))
        if hold == 3:
            out["HD3_WR"] = _r(_wr(vals))
            out["PF_HD3"] = _r(_pf(vals))
            out["maxDD_HD3"] = _r(_max_dd(vals))
            out["max_loss"] = _r(min(vals)) if vals else None
    est = [_to_float(r.get("hd3_ret_est12"), None) for r in rows if r.get("hd3_ret_est12") is not None]
    out["HD3_est12_avg"] = _r(_avg(est))
    out["emergency_stop_rate"] = _r(sum(1 for r in rows if r.get("emergency_stop")) / n * 100)
    idx_vals = [r.get("index_overheat_score") for r in rows if r.get("index_overheat_score") is not None]
    out["avg_index_overheat_score"] = _r(_avg(idx_vals))
    r1d_vals = [r.get("index_return_1d") for r in rows if r.get("index_return_1d") is not None]
    out["avg_index_return_1d"] = _r(_avg(r1d_vals))
    return out


# ──────────────────────────────────────────────
# Filter Rules
# ──────────────────────────────────────────────

def _make_filter(name: str) -> dict:
    return {"name": name}


FILTER_RULES = [
    {"name": "L_H5_full",              "fn": lambda r: True},
    {"name": "A_no_index_1d_big_up",   "fn": lambda r: r.get("index_return_1d") is None or r["index_return_1d"] < 1.5},
    {"name": "B_no_index_2d_big_up",   "fn": lambda r: r.get("index_return_2d") is None or r["index_return_2d"] < 2.5},
    {"name": "C_no_index_ma5_overheat","fn": lambda r: r.get("index_close_vs_ma5_pct") is None or r["index_close_vs_ma5_pct"] < 2.0},
    {"name": "D_no_index_rsi_overheat","fn": lambda r: r.get("index_rsi14") is None or r["index_rsi14"] < 65},
    {"name": "E_no_index_gap_up",      "fn": lambda r: r.get("index_gap_pct") is None or r["index_gap_pct"] < 0.8},
    {"name": "F_no_index_overheat_score_2","fn": lambda r: (r.get("index_overheat_score") or 0) <= 1},
    {"name": "G_no_index_overheat_score_3","fn": lambda r: (r.get("index_overheat_score") or 0) <= 2},
    {"name": "H_rebound_setup_only",   "fn": lambda r: r.get("index_rebound_setup") is True},
    {"name": "I_weak_stabilizing_only","fn": lambda r: r.get("index_weak_stabilizing") is True},
    {"name": "J_K_no_normal",          "fn": lambda r: str(r.get("market_regime") or "") not in {"normal", "euphoria"}},
    {"name": "K_no_normal_plus_no_overheat",
     "fn": lambda r: str(r.get("market_regime") or "") not in {"normal", "euphoria"} and (r.get("index_overheat_score") or 0) <= 1},
]

MONTHLY_RULES = [
    "L_H5_full",
    "J_K_no_normal",
    "F_no_index_overheat_score_2",
    "A_no_index_1d_big_up",
    "C_no_index_ma5_overheat",
    "H_rebound_setup_only",
    "K_no_normal_plus_no_overheat",
]


# ──────────────────────────────────────────────
# Bucket Definitions
# ──────────────────────────────────────────────

def _bucket_return_1d(v: float | None) -> str:
    if v is None:
        return "null"
    if v <= -2.0:   return "<=-2.0"
    if v <= -1.0:   return "-2.0_to_-1.0"
    if v <= -0.5:   return "-1.0_to_-0.5"
    if v <= 0.0:    return "-0.5_to_0"
    if v <= 0.5:    return "0_to_+0.5"
    if v <= 1.0:    return "+0.5_to_+1.0"
    if v <= 1.5:    return "+1.0_to_+1.5"
    if v <= 2.0:    return "+1.5_to_+2.0"
    return ">+2.0"


def _bucket_return_nd(v: float | None, break1: float, break2: float, break3: float, break4: float) -> str:
    if v is None:   return "null"
    if v <= -break4: return f"<=-{break4}"
    if v <= -break3: return f"-{break4}_to_-{break3}"
    if v <= -break2: return f"-{break3}_to_-{break2}"
    if v <= -break1: return f"-{break2}_to_-{break1}"
    if v <= 0:       return f"-{break1}_to_0"
    if v <= break1:  return f"0_to_+{break1}"
    if v <= break2:  return f"+{break1}_to_+{break2}"
    if v <= break3:  return f"+{break2}_to_+{break3}"
    return f">+{break3}"


def _bucket_ma_gap(v: float | None) -> str:
    if v is None:  return "null"
    if v <= -3.0:  return "<=-3"
    if v <= -2.0:  return "-3_to_-2"
    if v <= -1.0:  return "-2_to_-1"
    if v <= 0.0:   return "-1_to_0"
    if v <= 1.0:   return "0_to_+1"
    if v <= 2.0:   return "+1_to_+2"
    if v <= 3.0:   return "+2_to_+3"
    return ">+3"


def _bucket_rsi(v: float | None) -> str:
    if v is None:  return "null"
    if v < 30:     return "<30"
    if v < 40:     return "30-40"
    if v < 50:     return "40-50"
    if v < 60:     return "50-60"
    if v < 65:     return "60-65"
    if v < 70:     return "65-70"
    return ">=70"


def _bucket_gap(v: float | None) -> str:
    if v is None:  return "null"
    if v <= -1.0:  return "<=-1.0"
    if v <= -0.5:  return "-1.0_to_-0.5"
    if v <= 0.0:   return "-0.5_to_0"
    if v <= 0.5:   return "0_to_+0.5"
    if v <= 1.0:   return "+0.5_to_+1.0"
    return ">+1.0"


def _bucket_ipos(v: float | None) -> str:
    if v is None:  return "null"
    if v <= 0.2:   return "<=0.2"
    if v <= 0.4:   return "0.2-0.4"
    if v <= 0.6:   return "0.4-0.6"
    if v <= 0.8:   return "0.6-0.8"
    return ">0.8"


def _bucket_overheat(v) -> str:
    if v is None:  return "null"
    iv = int(v)
    if iv >= 4:    return "4+"
    return str(iv)


BUCKET_SPECS: list[dict] = [
    {"feature": "index_return_1d",       "fn": _bucket_return_1d},
    {"feature": "index_return_2d",       "fn": lambda v: _bucket_return_nd(v, 1.0, 2.0, 3.0, 4.0)},
    {"feature": "index_return_3d",       "fn": lambda v: _bucket_return_nd(v, 1.5, 3.0, 4.0, 5.0)},
    {"feature": "index_close_vs_ma5_pct","fn": _bucket_ma_gap},
    {"feature": "index_close_vs_ma25_pct","fn": _bucket_ma_gap},
    {"feature": "index_rsi14",           "fn": _bucket_rsi},
    {"feature": "index_gap_pct",         "fn": _bucket_gap},
    {"feature": "index_intraday_position","fn": _bucket_ipos},
    {"feature": "index_overheat_score",  "fn": _bucket_overheat},
]


# ──────────────────────────────────────────────
# Analysis Functions
# ──────────────────────────────────────────────

def _bucket_analysis(dataset: list[dict], train_end: date) -> list[dict]:
    rows_out: list[dict] = []
    train, test = _split(dataset, train_end)
    splits = [("all", dataset), ("train", train), ("test", test)]

    for spec in BUCKET_SPECS:
        feat = spec["feature"]
        fn = spec["fn"]
        groups: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"all": [], "train": [], "test": []})
        for period_name, period_rows in splits:
            for r in period_rows:
                b = fn(r.get(feat))
                groups[b][period_name].append(r)

        for bucket, period_dict in sorted(groups.items()):
            for period_name, period_rows in [("all", period_dict["all"]),
                                              ("train", period_dict["train"]),
                                              ("test", period_dict["test"])]:
                p = _perf(period_rows, f"{feat}|{bucket}", period_name)
                row = {"feature": feat, "bucket": bucket, "period": period_name,
                       "n": p["n"],
                       "HD1_avg": p.get("HD1_avg"),
                       "HD3_avg": p.get("HD3_avg"),
                       "HD5_avg": p.get("HD5_avg"),
                       "HD7_avg": p.get("HD7_avg"),
                       "HD10_avg": p.get("HD10_avg"),
                       "HD3_WR": p.get("HD3_WR"),
                       "PF_HD3": p.get("PF_HD3"),
                       "emergency_stop_rate": p.get("emergency_stop_rate"),
                       "avg_index_overheat_score": p.get("avg_index_overheat_score"),
                       }
                rows_out.append(row)
    return rows_out


def _filter_rule_comparison(dataset: list[dict], train_end: date) -> list[dict]:
    rows_out: list[dict] = []
    train, test = _split(dataset, train_end)
    total_n = len(dataset)

    for rule in FILTER_RULES:
        rname = rule["name"]
        fn = rule["fn"]
        try:
            selected = [r for r in dataset if fn(r)]
            sel_train = [r for r in train if fn(r)]
            sel_test = [r for r in test if fn(r)]
        except Exception as e:
            logger.warning("[filter] rule=%s error: %s", rname, e)
            continue

        for period_name, period_rows, total in [
            ("all",   selected,  total_n),
            ("train", sel_train, len(train)),
            ("test",  sel_test,  len(test)),
        ]:
            p = _perf(period_rows, rname, period_name)
            r1d_vals = [r.get("index_return_1d") for r in period_rows if r.get("index_return_1d") is not None]
            oh_vals  = [r.get("index_overheat_score") for r in period_rows if r.get("index_overheat_score") is not None]
            row = {
                "rule_name": rname,
                "period": period_name,
                "n": p["n"],
                "HD1_avg": p.get("HD1_avg"),
                "HD3_avg": p.get("HD3_avg"),
                "HD5_avg": p.get("HD5_avg"),
                "HD7_avg": p.get("HD7_avg"),
                "HD10_avg": p.get("HD10_avg"),
                "HD3_WR": p.get("HD3_WR"),
                "PF_HD3": p.get("PF_HD3"),
                "maxDD_HD3": p.get("maxDD_HD3"),
                "max_loss": p.get("max_loss"),
                "emergency_stop_rate": p.get("emergency_stop_rate"),
                "selected_rate": _r(p["n"] / total * 100) if total > 0 else None,
                "avg_index_return_1d": _r(_avg(r1d_vals)),
                "avg_index_overheat_score": _r(_avg(oh_vals)),
            }
            rows_out.append(row)
    return rows_out


def _train_test_stability(filter_rows: list[dict], full_hd3_all: float | None) -> list[dict]:
    rows_out: list[dict] = []
    # Pivot filter_rule_comparison by rule
    by_rule: dict[str, dict] = defaultdict(dict)
    for r in filter_rows:
        by_rule[r["rule_name"]][r["period"]] = r

    for rname, periods in by_rule.items():
        all_p = periods.get("all", {})
        train_p = periods.get("train", {})
        test_p = periods.get("test", {})
        hd3_all = all_p.get("HD3_avg")
        hd3_train = train_p.get("HD3_avg")
        hd3_test = test_p.get("HD3_avg")
        pf_train = train_p.get("PF_HD3")
        pf_test = test_p.get("PF_HD3")
        gap = abs((hd3_test or 0) - (hd3_train or 0)) if hd3_test is not None and hd3_train is not None else None

        # Judgment
        if hd3_all is not None and hd3_train is not None and hd3_test is not None:
            full = full_hd3_all or 0.0
            if hd3_all > full and hd3_train >= -0.1 and hd3_test >= 0.0 and (gap or 99) < 1.0:
                judgment = "PASS"
            elif hd3_test >= 0.0 and hd3_all > full:
                judgment = "WATCH"
            elif hd3_test >= 0.0:
                judgment = "WATCH"
            else:
                judgment = "FAIL"
        else:
            judgment = "FAIL"

        rows_out.append({
            "rule_name": rname,
            "n_train": train_p.get("n"),
            "HD3_train": hd3_train,
            "PF_train": pf_train,
            "n_test": test_p.get("n"),
            "HD3_test": hd3_test,
            "PF_test": pf_test,
            "n_all": all_p.get("n"),
            "HD3_all": hd3_all,
            "PF_all": all_p.get("PF_HD3"),
            "train_test_gap": _r(gap),
            "judgment": judgment,
        })
    return rows_out


def _k_no_normal_decomposition(dataset: list[dict], train_end: date) -> list[dict]:
    rows_out: list[dict] = []
    train, test = _split(dataset, train_end)

    GROUPS = [
        ("H5_full",                    lambda r: True),
        ("J_K_no_normal",              lambda r: str(r.get("market_regime") or "") not in {"normal", "euphoria"}),
        ("normal_only",                lambda r: str(r.get("market_regime") or "") == "normal"),
        ("euphoria_only",              lambda r: str(r.get("market_regime") or "") == "euphoria"),
        ("non_normal_non_euphoria",    lambda r: str(r.get("market_regime") or "") not in {"normal", "euphoria"}),
        ("index_overheated",           lambda r: (r.get("index_overheat_score") or 0) >= 2),
        ("index_not_overheated",       lambda r: (r.get("index_overheat_score") or 0) <= 1),
        ("normal_but_not_idx_overheat",lambda r: str(r.get("market_regime") or "") == "normal" and (r.get("index_overheat_score") or 0) <= 1),
        ("non_normal_but_idx_overheat",lambda r: str(r.get("market_regime") or "") not in {"normal", "euphoria"} and (r.get("index_overheat_score") or 0) >= 2),
    ]

    for gname, fn in GROUPS:
        for period_name, period_rows in [("all", dataset), ("train", train), ("test", test)]:
            try:
                grp = [r for r in period_rows if fn(r)]
            except Exception:
                grp = []
            p = _perf(grp, gname, period_name)
            r1d_vals = [r.get("index_return_1d") for r in grp if r.get("index_return_1d") is not None]
            ma5_vals = [r.get("index_close_vs_ma5_pct") for r in grp if r.get("index_close_vs_ma5_pct") is not None]
            rsi_vals = [r.get("index_rsi14") for r in grp if r.get("index_rsi14") is not None]
            oh_vals  = [r.get("index_overheat_score") for r in grp if r.get("index_overheat_score") is not None]
            rows_out.append({
                "group": gname,
                "period": period_name,
                "n": p["n"],
                "HD3_avg": p.get("HD3_avg"),
                "HD5_avg": p.get("HD5_avg"),
                "HD7_avg": p.get("HD7_avg"),
                "HD3_WR": p.get("HD3_WR"),
                "PF_HD3": p.get("PF_HD3"),
                "avg_index_return_1d": _r(_avg(r1d_vals)),
                "avg_index_close_vs_ma5_pct": _r(_avg(ma5_vals)),
                "avg_index_rsi": _r(_avg(rsi_vals)),
                "avg_index_overheat_score": _r(_avg(oh_vals)),
            })
    return rows_out


def _next_index_return_impact(dataset: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    """File 05: next_index_return_1d bucket → H5 performance.
       File 06: index_overheat_score bucket → avg next_index_return + H5 HD3."""
    def _bucket_next(v: float | None) -> str:
        if v is None:    return "null"
        if v <= -2.0:    return "<=-2.0"
        if v <= -1.0:    return "-2.0_to_-1.0"
        if v <= -0.5:    return "-1.0_to_-0.5"
        if v <= 0.0:     return "-0.5_to_0"
        if v <= 0.5:     return "0_to_+0.5"
        if v <= 1.0:     return "+0.5_to_+1.0"
        return ">+1.0"

    train, test = _split(dataset, train_end)

    # File 05
    rows_05: list[dict] = []
    for period_name, period_rows in [("all", dataset), ("train", train), ("test", test)]:
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in period_rows:
            b = _bucket_next(r.get("next_index_return_1d"))
            groups[b].append(r)
        for bucket, grp in sorted(groups.items()):
            p = _perf(grp, bucket, period_name)
            nxt_vals = [r.get("next_index_return_1d") for r in grp if r.get("next_index_return_1d") is not None]
            rows_05.append({
                "next_index_bucket": bucket,
                "period": period_name,
                "n": p["n"],
                "H5_HD3_avg": p.get("HD3_avg"),
                "H5_HD3_WR": p.get("HD3_WR"),
                "avg_next_index_return": _r(_avg(nxt_vals)),
            })

    # File 06
    rows_06: list[dict] = []
    for period_name, period_rows in [("all", dataset), ("train", train), ("test", test)]:
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in period_rows:
            b = _bucket_overheat(r.get("index_overheat_score"))
            groups[b].append(r)
        for bucket, grp in sorted(groups.items()):
            p = _perf(grp, bucket, period_name)
            nxt1 = [r.get("next_index_return_1d") for r in grp if r.get("next_index_return_1d") is not None]
            nxt2 = [r.get("next_index_return_2d") for r in grp if r.get("next_index_return_2d") is not None]
            rows_06.append({
                "index_overheat_bucket": bucket,
                "period": period_name,
                "n": p["n"],
                "avg_next_index_return_1d": _r(_avg(nxt1)),
                "avg_next_index_return_2d": _r(_avg(nxt2)),
                "avg_H5_HD3_return": p.get("HD3_avg"),
                "H5_HD3_WR": p.get("HD3_WR"),
            })

    return rows_05, rows_06


def _monthly_stability(dataset: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    rows_07: list[dict] = []
    rows_08: list[dict] = []

    rule_fns = {r["name"]: r["fn"] for r in FILTER_RULES}

    for rname in MONTHLY_RULES:
        fn = rule_fns.get(rname)
        if fn is None:
            continue
        try:
            selected = [r for r in dataset if fn(r)]
        except Exception:
            selected = dataset

        monthly: dict[str, list[dict]] = defaultdict(list)
        for r in selected:
            monthly[_month_key(r["entry_date"])].append(r)

        month_hd3_list: list[float] = []
        for mkey in sorted(monthly.keys()):
            grp = monthly[mkey]
            vals = [_to_float(r.get("hd3_ret_raw"), None) for r in grp if r.get("hd3_ret_raw") is not None]
            hd3_avg = _avg(vals)
            hd3_sum = sum(vals) if vals else None
            hd3_wr  = _wr(vals)
            pf      = _pf(vals)
            estop   = sum(1 for r in grp if r.get("emergency_stop"))
            rows_07.append({
                "rule_name": rname,
                "month": mkey,
                "n": len(grp),
                "HD3_avg": _r(hd3_avg),
                "HD3_total_return_sum": _r(hd3_sum),
                "HD3_WR": _r(hd3_wr),
                "PF_HD3": _r(pf),
                "emergency_stop_count": estop,
            })
            if hd3_sum is not None:
                month_hd3_list.append(hd3_sum)

        pos_months = sum(1 for v in month_hd3_list if v > 0)
        total_months = len(month_hd3_list)
        monthly_wr = pos_months / total_months * 100 if total_months > 0 else None
        avg_sum = _avg(month_hd3_list)
        worst = min(month_hd3_list) if month_hd3_list else None
        best  = max(month_hd3_list) if month_hd3_list else None
        std   = None
        if len(month_hd3_list) >= 2:
            mean = avg_sum or 0
            std = (sum((v - mean) ** 2 for v in month_hd3_list) / len(month_hd3_list)) ** 0.5
        rows_08.append({
            "rule_name": rname,
            "monthly_count": total_months,
            "positive_month_count": pos_months,
            "monthly_win_rate": _r(monthly_wr),
            "avg_monthly_sum": _r(avg_sum),
            "worst_month_sum": _r(worst),
            "best_month_sum": _r(best),
            "monthly_std": _r(std),
        })

    return rows_07, rows_08


# ──────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────

def _answer(rows_filter: list[dict], rule: str, period: str, field: str) -> float | None:
    for r in rows_filter:
        if r.get("rule_name") == rule and r.get("period") == period:
            return r.get(field)
    return None


def _generate_report(
    dataset: list[dict],
    train_end: date,
    rows_filter: list[dict],
    rows_stable: list[dict],
    rows_decomp: list[dict],
    out_dir: Path,
) -> None:
    report: list[str] = []

    def ln(s: str = "") -> None:
        report.append(s)

    def qa(n: int, q: str, a: str) -> None:
        report.append(f"Q{n:02d}. {q}")
        report.append(f"  -> {a}")
        report.append("")

    def _get_filter(rname: str, period: str, field: str) -> float | None:
        return _answer(rows_filter, rname, period, field)

    def _get_decomp(gname: str, period: str, field: str) -> float | None:
        for r in rows_decomp:
            if r.get("group") == gname and r.get("period") == period:
                return r.get(field)
        return None

    def _get_stable(rname: str, field: str) -> Any:
        for r in rows_stable:
            if r.get("rule_name") == rname:
                return r.get(field)
        return None

    def _fmt(v, suffix="") -> str:
        if v is None:
            return "N/A"
        return f"{v:+.4f}{suffix}"

    train_data, test_data = _split(dataset, train_end)

    # H5_full reference
    full_all   = _get_filter("L_H5_full", "all",   "HD3_avg")
    full_train = _get_filter("L_H5_full", "train", "HD3_avg")
    full_test  = _get_filter("L_H5_full", "test",  "HD3_avg")
    full_n     = _get_filter("L_H5_full", "all",   "n")
    k_all      = _get_filter("J_K_no_normal", "all",   "HD3_avg")
    k_train    = _get_filter("J_K_no_normal", "train", "HD3_avg")
    k_test     = _get_filter("J_K_no_normal", "test",  "HD3_avg")

    ln("H5 Index Overheat Entry Filter Report")
    ln("=" * 70)
    ln(f"Generated: {date.today().isoformat()}")
    ln(f"Period: train=2023-01-01~2024-12-31  test=2025-01-01~{test_data[-1]['entry_date'] if test_data else 'N/A'}")
    ln(f"Research: n={len(dataset)} (train={len(train_data)}, test={len(test_data)})")
    ln(f"Index: Nikkei225 (^N225) via yfinance")
    ln(f"Primary: {H5_PRIMARY_KEY}")
    ln()

    ln("=" * 70)
    ln("REFERENCE PERFORMANCE")
    ln("=" * 70)
    ln(f"  H5_full  (L): all={_fmt(full_all)}%  train={_fmt(full_train)}%  test={_fmt(full_test)}%  n={full_n}")
    ln(f"  K_no_normal: all={_fmt(k_all)}%  train={_fmt(k_train)}%  test={_fmt(k_test)}%")
    ln()

    ln("=" * 70)
    ln("FILTER RULE COMPARISON (ALL period)")
    ln("=" * 70)
    ln(f"  {'rule_name':<40s}  {'HD3_all':>8s}  {'HD3_train':>9s}  {'HD3_test':>8s}  {'n':>5s}  {'judgment'}")
    for st in rows_stable:
        rn = st.get("rule_name", "")
        ln(f"  {rn:<40s}  {_fmt(st.get('HD3_all')):>8s}  {_fmt(st.get('HD3_train')):>9s}  {_fmt(st.get('HD3_test')):>8s}  {st.get('n_all') or '':>5}  {st.get('judgment','')}")
    ln()

    ln("=" * 70)
    ln("19 QUESTIONS")
    ln("=" * 70)
    ln()

    # Q01: 指数1日大幅上昇後
    a_all = _get_filter("A_no_index_1d_big_up", "all", "HD3_avg")
    a_n   = _get_filter("A_no_index_1d_big_up", "all", "n")
    qa(1, "指数が大きく上げた日のH5は弱いか",
       f"A_no_index_1d_big_up (index_return_1d<1.5%): all={_fmt(a_all)}%  n={a_n}  vs full={_fmt(full_all)}%. "
       + ("過熱日除外で改善" if a_all is not None and full_all is not None and a_all > full_all else "改善なし/データ確認要"))

    # Q02: 2d/3d累計上昇
    b_all = _get_filter("B_no_index_2d_big_up", "all", "HD3_avg")
    qa(2, "指数2日/3日累計上昇後のH5は弱いか",
       f"B_no_index_2d_big_up (return_2d<2.5%): all={_fmt(b_all)}%  vs full={_fmt(full_all)}%. "
       + ("除外効果あり" if b_all is not None and full_all is not None and b_all > full_all else "効果限定的"))

    # Q03: MA5乖離
    c_all = _get_filter("C_no_index_ma5_overheat", "all", "HD3_avg")
    qa(3, "指数MA5乖離が大きい日のH5は弱いか",
       f"C_no_index_ma5_overheat (ma5_gap<2.0%): all={_fmt(c_all)}%  vs full={_fmt(full_all)}%. "
       + ("改善" if c_all is not None and full_all is not None and c_all > full_all else "改善なし"))

    # Q04: MA25乖離
    qa(4, "指数MA25乖離が大きい日のH5は弱いか",
       "bucket分析 01_index_feature_bucket_performance.csv の index_close_vs_ma25_pct 参照. "
       + f"K_no_normal HD3: all={_fmt(k_all)}% は normal除外で MA25乖離の高い相場を避けている可能性あり.")

    # Q05: RSI
    d_all = _get_filter("D_no_index_rsi_overheat", "all", "HD3_avg")
    qa(5, "指数RSIが高い日のH5は弱いか",
       f"D_no_index_rsi_overheat (rsi<65): all={_fmt(d_all)}%  vs full={_fmt(full_all)}%. "
       + ("RSI過熱除外で改善" if d_all is not None and full_all is not None and d_all > full_all else "RSI単独の効果は限定的"))

    # Q06: ギャップアップ
    e_all = _get_filter("E_no_index_gap_up", "all", "HD3_avg")
    qa(6, "指数ギャップアップ日のH5は弱いか",
       f"E_no_index_gap_up (gap<0.8%): all={_fmt(e_all)}%  vs full={_fmt(full_all)}%. "
       + ("ギャップアップ除外で改善" if e_all is not None and full_all is not None and e_all > full_all else "効果限定的"))

    # Q07: 過熱スコア
    f_all = _get_filter("F_no_index_overheat_score_2", "all", "HD3_avg")
    g_all = _get_filter("G_no_index_overheat_score_3", "all", "HD3_avg")
    qa(7, "指数過熱スコアが高いほどH5は悪化するか",
       f"F_overheat_score<=1: {_fmt(f_all)}%  G_score<=2: {_fmt(g_all)}%  full: {_fmt(full_all)}%. "
       + "01_bucket_performance.csv の index_overheat_score bucketで単調性を確認.")

    # Q08: 反発初動
    h_all = _get_filter("H_rebound_setup_only", "all", "HD3_avg")
    h_n   = _get_filter("H_rebound_setup_only", "all", "n")
    i_all = _get_filter("I_weak_stabilizing_only", "all", "HD3_avg")
    qa(8, "指数反発初動ではH5は強いか",
       f"H_rebound_setup: {_fmt(h_all)}%  n={h_n}  I_weak_stabilizing: {_fmt(i_all)}%. "
       + ("反発初動で改善" if h_all is not None and full_all is not None and h_all > full_all else "件数が少ない場合は過学習リスクに注意"))

    # Q09: K_no_normalを指数過熱で説明できるか
    k_plus_all = _get_filter("K_no_normal_plus_no_overheat", "all", "HD3_avg")
    qa(9, "K_no_normalの優位性は指数過熱回避で説明できるか",
       f"K_no_normal={_fmt(k_all)}%  K+no_overheat={_fmt(k_plus_all)}%  "
       + "04_k_no_normal_decomposition.csv の normal_but_not_idx_overheat vs index_overheated を参照.")

    # Q10: normalでも過熱していなければ有効か
    norm_notoh = _get_decomp("normal_but_not_idx_overheat", "all", "HD3_avg")
    norm_notoh_n = _get_decomp("normal_but_not_idx_overheat", "all", "n")
    qa(10, "normalでも指数過熱していなければH5は有効か",
       f"normal_but_not_idx_overheat: HD3={_fmt(norm_notoh)}%  n={norm_notoh_n}  "
       + f"vs normal_only: HD3={_fmt(_get_decomp('normal_only','all','HD3_avg'))}%")

    # Q11: non-normalでも過熱なら弱いか
    nonnorm_oh = _get_decomp("non_normal_but_idx_overheat", "all", "HD3_avg")
    qa(11, "non_normalでも指数過熱していたらH5は弱いか",
       f"non_normal_but_idx_overheat: HD3={_fmt(nonnorm_oh)}%  "
       + f"vs K_no_normal: {_fmt(k_all)}%")

    # Q12: 翌日指数反落との連動
    qa(12, "翌日指数反落とH5損益は連動するか",
       "05_next_index_return_impact.csv 参照. "
       "翌日指数<=−1%のbucketでH5 HD3が低下していれば連動あり.")

    # Q13: ユーザー体感の確認
    qa(13, "ユーザーの体感「指数が大きく上げた日に入ると翌日連れ安する」は正しいか",
       "06_index_overheat_to_next_index_return.csv で overheat_score別の翌日指数リターンを確認. "
       "overheat_scoreが高いほど avg_next_index_return_1dが低下していれば体感と一致.")

    # Q14: market_regime vs index_overheat
    qa(14, "market_regimeとindex_overheat_scoreのどちらが実運用フィルターとして有効か",
       f"K_no_normal(regime基準): all={_fmt(k_all)}%  F_overheat_score<=1(数値基準): all={_fmt(f_all)}%. "
       + "04_decomposition と 03_stability を比較. 数値基準の方が定量的で実装しやすい.")

    # Q15: 次にcase化すべきルール
    best_pass = None
    best_hd3 = full_all or -99.0
    for st in rows_stable:
        if st.get("judgment") == "PASS" and (st.get("HD3_all") or -99) > best_hd3:
            best_hd3 = st["HD3_all"]
            best_pass = st["rule_name"]
    if best_pass is None:
        for st in rows_stable:
            if st.get("judgment") == "WATCH" and (st.get("HD3_all") or -99) > (full_all or -99):
                best_pass = st["rule_name"]
                break
    qa(15, "次にcomparison case化すべきルールは何か",
       f"推奨: {best_pass or 'なし'}. 判定基準: PASS優先、次点WATCH。"
       "今回は分析のみ、case登録は別セッションで行う.")

    # Q16: Primary変更要否
    qa(16, "Primary変更は必要か",
       "NO. 今回は分析のみ。Primary (h5_ai65_hd3_est12_cm_range330_live_limited) は変更しない.")

    # Q17: Live Limitedへのフィルター追加
    qa(17, "Live Limited選抜にこのフィルターを入れるべきか",
       "現時点では判断保留。train/test両方でPASSするルールが確認できてからforward-testを経て判断する.")

    # Q18: Research表示に残すべきか
    qa(18, "Research表示には残すべきか",
       "YES. 今回は研究段階。Research母集団に指数フィルターを追加するとH5の期待値が変わる可能性がある。 "
       "まずDB comparison caseとして登録し、forward-testで確認する.")

    ln("=" * 70)
    ln("JUDGMENT SUMMARY")
    ln("=" * 70)
    for st in rows_stable:
        ln(f"  {st.get('rule_name',''):<40s}: train={_fmt(st.get('HD3_train'))}  test={_fmt(st.get('HD3_test'))}  all={_fmt(st.get('HD3_all'))}  -> {st.get('judgment','')}")
    ln()

    ln("=" * 70)
    ln("INVARIANTS (UNCHANGED)")
    ln("=" * 70)
    ln("  Primary case key:       NOT changed")
    ln("  DB case definitions:    NOT changed")
    ln("  UI / LINE / trade_logs: NOT changed")
    ln("  Watchlist / Intraday:   NOT changed")
    ln()

    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "10_index_overheat_filter_report.txt"
    p.write_text("\n".join(report), encoding="utf-8")
    logger.info("[report] written: %s", p)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    train_end = _d(args.train_end)
    start = _d(args.train_start)
    end_date_str = args.test_end
    if end_date_str in ("latest", "today"):
        end = date.today()
    else:
        end = _d(end_date_str)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sb = _build_supabase()
    logger.info("[overheat] loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[overheat] raw candidates=%d", len(candidates))

    # Load index data (with 200-day warmup for MA75)
    index_data = _load_index_data(start, end)

    dataset = _build_dataset(candidates, index_data)
    logger.info("[overheat] research rows=%d", len(dataset))

    train_data, test_data = _split(dataset, train_end)
    logger.info("[overheat] train=%d test=%d", len(train_data), len(test_data))

    # 01 — Bucket analysis
    logger.info("[overheat] 01 bucket analysis...")
    rows_01 = _bucket_analysis(dataset, train_end)
    _write_csv(out_dir / "01_index_feature_bucket_performance.csv", rows_01)
    logger.info("[overheat] 01 done (%d rows)", len(rows_01))

    # 02 — Filter rule comparison
    logger.info("[overheat] 02 filter rule comparison...")
    rows_02 = _filter_rule_comparison(dataset, train_end)
    _write_csv(out_dir / "02_index_filter_rule_comparison.csv", rows_02)
    logger.info("[overheat] 02 done")

    # 03 — Train/test stability
    logger.info("[overheat] 03 train/test stability...")
    full_all_hd3 = _answer(rows_02, "L_H5_full", "all", "HD3_avg")
    rows_03 = _train_test_stability(rows_02, full_all_hd3)
    _write_csv(out_dir / "03_rule_train_test_stability.csv", rows_03)
    logger.info("[overheat] 03 done")

    # 04 — K_no_normal decomposition
    logger.info("[overheat] 04 K_no_normal decomposition...")
    rows_04 = _k_no_normal_decomposition(dataset, train_end)
    _write_csv(out_dir / "04_k_no_normal_decomposition.csv", rows_04)
    logger.info("[overheat] 04 done")

    # 05, 06 — Next index return impact
    logger.info("[overheat] 05-06 next index return impact...")
    rows_05, rows_06 = _next_index_return_impact(dataset, train_end)
    _write_csv(out_dir / "05_next_index_return_impact.csv", rows_05)
    _write_csv(out_dir / "06_index_overheat_to_next_index_return.csv", rows_06)
    logger.info("[overheat] 05-06 done")

    # 07, 08 — Monthly stability
    logger.info("[overheat] 07-08 monthly stability...")
    rows_07, rows_08 = _monthly_stability(dataset, train_end)
    _write_csv(out_dir / "07_monthly_stability.csv", rows_07)
    _write_csv(out_dir / "08_monthly_stability_summary.csv", rows_08)
    logger.info("[overheat] 07-08 done")

    # 09 — H5 candidates with index features
    logger.info("[overheat] 09 h5 with index features...")
    rows_09: list[dict] = []
    for r in dataset:
        rows_09.append({
            "code": r.get("code"),
            "name": r.get("name"),
            "entry_date": r.get("entry_date"),
            "market_regime": r.get("market_regime"),
            "signal_probability": r.get("signal_probability"),
            "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
            "HD3_return": r.get("hd3_ret_raw"),
            "HD5_return": r.get("hd5_ret_raw"),
            "HD7_return": r.get("hd7_ret_raw"),
            "index_return_1d": r.get("index_return_1d"),
            "index_return_2d": r.get("index_return_2d"),
            "index_return_3d": r.get("index_return_3d"),
            "index_close_vs_ma5_pct": r.get("index_close_vs_ma5_pct"),
            "index_close_vs_ma25_pct": r.get("index_close_vs_ma25_pct"),
            "index_rsi14": r.get("index_rsi14"),
            "index_gap_pct": r.get("index_gap_pct"),
            "index_intraday_position": r.get("index_intraday_position"),
            "index_overheat_score": r.get("index_overheat_score"),
            "index_rebound_setup": r.get("index_rebound_setup"),
            "index_weak_stabilizing": r.get("index_weak_stabilizing"),
            "next_index_return_1d": r.get("next_index_return_1d"),
            "next_index_return_3d": r.get("next_index_return_3d"),
        })
    _write_csv(out_dir / "09_h5_with_index_features.csv", rows_09)
    logger.info("[overheat] 09 done (%d rows)", len(rows_09))

    # 10 — Report
    logger.info("[overheat] 10 report...")
    _generate_report(dataset, train_end, rows_02, rows_03, rows_04, out_dir)
    logger.info("[overheat] ALL DONE -> %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="H5 Index Overheat Entry Filter Analysis")
    p.add_argument("--train-start",  default="2023-01-01")
    p.add_argument("--train-end",    default="2024-12-31")
    p.add_argument("--test-start",   default="2025-01-01")
    p.add_argument("--test-end",     default="latest")
    p.add_argument("--output-dir",   default="outputs/h5_index_overheat_filter")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args)
