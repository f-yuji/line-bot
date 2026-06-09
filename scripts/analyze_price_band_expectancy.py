#!/usr/bin/env python3
"""Analyze price-band to target-band mean reversion expectancy.

Research only. This script reads historical feature snapshots, builds
forward high/low/close windows, and writes CSV/report outputs. It does not
change Primary/H5 production logic, LINE notifications, actual_trade_logs,
virtual trade state, or auto-trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "price_band_expectancy"
ENV_ROWS = ROOT / "outputs" / "h5_environment_meter" / "environment_daily_rows.csv"

HORIZON_DAYS = 30
POSITION_AMOUNT = 300_000.0
COST_BPS = 10.0
PERIODS = {
    "1y": 365,
    "2y": 365 * 2,
    "3y": 365 * 3,
    "5y": 365 * 5,
}


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def fnum(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "nan", "NaN"):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except Exception:
        return default


def norm_code(value: Any) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    try:
        return str(int(float(txt)))
    except Exception:
        return txt.split(".", 1)[0]


def dtext(value: Any) -> str:
    return str(value or "")[:10]


def pct(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator * 100.0


def pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses == 0:
        return 999.0 if gains > 0 else None
    return gains / losses


def max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * q))))
    return vals[idx]


def fetch_snapshots(
    client: Any,
    start: str,
    end: str | None,
    max_rows: int,
    max_symbols: int,
    symbol_offset: int = 0,
    page_size: int = 1000,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    columns = (
        "id,trade_date,code,name,sector,open,high,low,close,ma5,ma25,ma75,"
        "rsi14,atr14,volume_ratio_20d,drop_pct,drop_from_20d_high_pct,"
        "ma5_gap_pct,ma25_gap_pct,ma75_gap_pct,is_tradeable,is_drop_candidate"
    )
    diagnostics: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    latest_q = (
        client.table("stock_feature_snapshots")
        .select("trade_date")
        .gte("trade_date", start)
        .order("trade_date", desc=True)
        .limit(1)
    )
    if end:
        latest_q = latest_q.lte("trade_date", end)
    latest_data = latest_q.execute().data or []
    latest_date = dtext(latest_data[0].get("trade_date")) if latest_data else end
    if not latest_date:
        return pd.DataFrame(), [{"step": "fetch_latest_date", "status": "empty"}]

    universe_q = (
        client.table("stock_feature_snapshots")
        .select("code,name,close,is_tradeable")
        .eq("trade_date", latest_date)
        .order("code", desc=False)
        .limit(max((symbol_offset + max_symbols) * 3, max_symbols * 3))
    )
    universe = universe_q.execute().data or []
    codes = []
    for row in universe:
        if row.get("is_tradeable") is False:
            continue
        close = fnum(row.get("close"))
        code = norm_code(row.get("code"))
        if code and close and close >= 50:
            codes.append(code)
    all_codes = codes
    codes = codes[symbol_offset:symbol_offset + max_symbols]
    diagnostics.append({
        "step": "fetch_latest_universe",
        "status": "ok",
        "latest_date": latest_date,
        "codes": len(codes),
        "eligible_codes_before_offset": len(all_codes),
        "symbol_offset": symbol_offset,
        "max_symbols": max_symbols,
    })

    chunk_size = 80
    for chunk_start in range(0, len(codes), chunk_size):
        chunk = codes[chunk_start:chunk_start + chunk_size]
        offset = 0
        while len(rows) < max_rows:
            q = (
                client.table("stock_feature_snapshots")
                .select(columns)
                .gte("trade_date", start)
                .in_("code", chunk)
                .order("trade_date", desc=False)
                .range(offset, offset + page_size - 1)
            )
            if end:
                q = q.lte("trade_date", end)
            data = q.execute().data or []
            if not data:
                break
            rows.extend(data)
            if len(data) < page_size:
                break
            offset += page_size
        if len(rows) >= max_rows:
            break
    df = pd.DataFrame(rows[:max_rows])
    diagnostics.append({
        "step": "fetch_snapshots",
        "status": "ok",
        "rows": len(df),
        "start": start,
        "end": end or "",
        "max_rows": max_rows,
        "symbols_requested": len(codes),
    })
    return df, diagnostics


def prepare_snapshots(df: pd.DataFrame, max_symbols: int) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    proxy: list[dict[str, Any]] = []
    if df.empty:
        return df, diagnostics, proxy

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["code"] = df["code"].map(norm_code)
    for col in ["open", "high", "low", "close", "ma5", "ma25", "ma75", "rsi14", "atr14", "volume_ratio_20d", "drop_pct", "drop_from_20d_high_pct", "ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["trade_date", "code", "close"])
    df = df[df["close"] > 0]
    if "is_tradeable" in df.columns:
        df = df[df["is_tradeable"].fillna(True).astype(bool)]
    df = df[df["close"] >= 50]
    diagnostics.append({
        "step": "basic_filter",
        "input_rows": before,
        "output_rows": len(df),
        "reason": "valid date/code/close, tradeable if available, close>=50",
    })

    if df.empty:
        return df, diagnostics, proxy

    counts = df.groupby("code").size().sort_values(ascending=False)
    keep_codes = set(counts.head(max_symbols).index.tolist())
    before_symbol = len(df)
    df = df[df["code"].isin(keep_codes)].copy()
    diagnostics.append({
        "step": "symbol_cap",
        "input_rows": before_symbol,
        "output_rows": len(df),
        "symbols": len(keep_codes),
        "max_symbols": max_symbols,
        "reason": "most observed symbols used as TOPIX500/Nikkei225/liquid universe proxy",
    })
    proxy.append({
        "item": "TOPIX500/Nikkei225/liquid universe",
        "proxy_used": "most observed tradeable stock_feature_snapshots symbols with close>=50",
        "reason": "constituent master was not required for this read-only run",
    })

    df = df.sort_values(["code", "trade_date"]).reset_index(drop=True)
    g = df.groupby("code", group_keys=False)
    for window in [5, 20, 60, 252]:
        df[f"roll_high_{window}"] = g["high"].rolling(window, min_periods=max(5, min(window, 20))).max().reset_index(level=0, drop=True)
        df[f"roll_low_{window}"] = g["low"].rolling(window, min_periods=max(5, min(window, 20))).min().reset_index(level=0, drop=True)

    for window in [20, 60]:
        rng = df[f"roll_high_{window}"] - df[f"roll_low_{window}"]
        df[f"range_position_{window}"] = ((df["close"] - df[f"roll_low_{window}"]) / rng * 100).where(rng > 0)

    df["drop5"] = (df["close"] / df["roll_high_5"] - 1.0) * 100.0
    if df["drop_from_20d_high_pct"].isna().all():
        df["drop20"] = (df["close"] / df["roll_high_20"] - 1.0) * 100.0
        proxy.append({
            "item": "drop20",
            "proxy_used": "close vs rolling 20 day high",
            "reason": "drop_from_20d_high_pct was unavailable or empty",
        })
    else:
        df["drop20"] = df["drop_from_20d_high_pct"].fillna((df["close"] / df["roll_high_20"] - 1.0) * 100.0)

    for ma in ["ma5", "ma25", "ma75"]:
        gap_col = f"{ma}_gap_pct"
        if gap_col not in df.columns or df[gap_col].isna().all():
            df[gap_col] = (df["close"] / df[ma] - 1.0) * 100.0
            proxy.append({
                "item": gap_col,
                "proxy_used": f"close/{ma}-1",
                "reason": f"{gap_col} was unavailable or empty",
            })

    df["atr_pct"] = (df["atr14"] / df["close"] * 100.0).where(df["atr14"].notna())
    df["h5_like"] = (df["drop20"] <= -8.0) & (df["rsi14"].fillna(99) <= 65.0)
    proxy.append({
        "item": "H5-like price-band condition",
        "proxy_used": "drop20<=-8 and RSI14<=65",
        "reason": "price-band script intentionally avoids production H5 entry/stage logic",
    })

    for i in range(1, HORIZON_DAYS + 1):
        df[f"future_open_{i}d"] = g["open"].shift(-i)
        df[f"future_high_{i}d"] = g["high"].shift(-i)
        df[f"future_low_{i}d"] = g["low"].shift(-i)
        df[f"future_close_{i}d"] = g["close"].shift(-i)
    df["has_20d_forward"] = df[f"future_close_{20}d"].notna()
    before_forward = len(df)
    df = df[df["has_20d_forward"]].copy()
    diagnostics.append({
        "step": "forward_window_filter",
        "input_rows": before_forward,
        "output_rows": len(df),
        "reason": "requires 20 future trading-day closes within the same symbol",
    })
    return df, diagnostics, proxy


def buy_zone_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "range20_le_10": df["range_position_20"] <= 10,
        "range20_le_15": df["range_position_20"] <= 15,
        "range20_le_20": df["range_position_20"] <= 20,
        "range60_le_10": df["range_position_60"] <= 10,
        "rsi_le_30": df["rsi14"] <= 30,
        "rsi_le_25": df["rsi14"] <= 25,
        "ma25_gap_le_m5": df["ma25_gap_pct"] <= -5,
        "ma25_gap_le_m8": df["ma25_gap_pct"] <= -8,
        "ma25_gap_le_m10": df["ma25_gap_pct"] <= -10,
        "drop5_le_m3": df["drop5"] <= -3,
        "drop20_le_m8": df["drop20"] <= -8,
        "range20_le_10_rsi_le_30": (df["range_position_20"] <= 10) & (df["rsi14"] <= 30),
        "h5_like_price_zone": df["h5_like"],
    }


def target_price(row: pd.Series | dict[str, Any], sell_zone: str) -> float | None:
    entry = fnum(row.get("close"))
    if entry is None or entry <= 0:
        return None
    if sell_zone.startswith("tp"):
        rate = float(sell_zone.replace("tp", "").replace("pct", ""))
        return entry * (1.0 + rate / 100.0)
    if sell_zone == "ma25_revert":
        val = fnum(row.get("ma25"))
        return val if val and val > entry else None
    if sell_zone == "ma75_revert":
        val = fnum(row.get("ma75"))
        return val if val and val > entry else None
    if sell_zone.startswith("range20_ge_"):
        level = float(sell_zone.rsplit("_", 1)[-1])
        low = fnum(row.get("roll_low_20"))
        high = fnum(row.get("roll_high_20"))
        if low is None or high is None or high <= low:
            return None
        price = low + (high - low) * level / 100.0
        return price if price > entry else None
    if sell_zone == "box60_high_touch":
        val = fnum(row.get("roll_high_60"))
        return val if val and val > entry else None
    return None


def evaluate_events(df: pd.DataFrame, buy_zone: str, sell_zone: str, period_label: str, period_start: pd.Timestamp) -> list[dict[str, Any]]:
    rows = df[df["trade_date"] >= period_start]
    if rows.empty:
        return []
    mask = buy_zone_masks(rows).get(buy_zone)
    if mask is None:
        return []
    rows = rows[mask.fillna(False)]
    if rows.empty:
        return []

    out: list[dict[str, Any]] = []
    time_exit = sell_zone in {"time5", "time10", "time20"}
    time_days = int(sell_zone.replace("time", "")) if time_exit else HORIZON_DAYS
    for row in rows.to_dict("records"):
        entry = fnum(row.get("close"))
        if entry is None or entry <= 0:
            continue
        target = None if time_exit else target_price(row, sell_zone)
        if not time_exit and not target:
            continue
        highs = [fnum(row.get(f"future_high_{i}d")) for i in range(1, HORIZON_DAYS + 1)]
        lows = [fnum(row.get(f"future_low_{i}d")) for i in range(1, HORIZON_DAYS + 1)]
        closes = [fnum(row.get(f"future_close_{i}d")) for i in range(1, HORIZON_DAYS + 1)]
        hit_day: int | None = None
        if time_exit:
            exit_day = min(time_days, HORIZON_DAYS)
            exit_price = closes[exit_day - 1]
            hit = exit_price is not None and exit_price > entry
            hit_day = exit_day if hit else None
        else:
            hit = False
            exit_day = HORIZON_DAYS
            exit_price = closes[-1]
            for i, high in enumerate(highs, start=1):
                if high is not None and target is not None and high >= target:
                    hit = True
                    hit_day = i
                    exit_day = i
                    exit_price = target
                    break
        if exit_price is None:
            continue
        lows_until_exit = [v for v in lows[:exit_day] if v is not None]
        max_adverse_pct = ((min(lows_until_exit) / entry) - 1.0) * 100.0 if lows_until_exit else None
        ret = (exit_price / entry - 1.0) * 100.0
        pnl_before = POSITION_AMOUNT * ret / 100.0
        pnl_after = POSITION_AMOUNT * (ret / 100.0 - COST_BPS / 10000.0)
        out.append({
            "period": period_label,
            "buy_zone": buy_zone,
            "sell_zone": sell_zone,
            "trade_date": row["trade_date"].date().isoformat() if hasattr(row.get("trade_date"), "date") else dtext(row.get("trade_date")),
            "code": row.get("code"),
            "name": row.get("name"),
            "sector": row.get("sector"),
            "entry_price": entry,
            "target_price": target if target is not None else "",
            "hit": bool(hit),
            "days_to_hit": hit_day if hit_day is not None else "",
            "exit_day": exit_day,
            "exit_price": exit_price,
            "return_pct": ret,
            "pnl_after_cost": pnl_after,
            "max_adverse_pct": max_adverse_pct,
            "range_position_20": fnum(row.get("range_position_20")),
            "range_position_60": fnum(row.get("range_position_60")),
            "rsi14": fnum(row.get("rsi14")),
            "ma25_gap_pct": fnum(row.get("ma25_gap_pct")),
            "drop5": fnum(row.get("drop5")),
            "drop20": fnum(row.get("drop20")),
            "h5_like": bool(row.get("h5_like")),
        })
    return out


def summarize_events(events: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    extra = extra or {}
    returns = [fnum(r.get("return_pct"), 0.0) or 0.0 for r in events]
    pnls = [fnum(r.get("pnl_after_cost"), 0.0) or 0.0 for r in events]
    adverse = [fnum(r.get("max_adverse_pct")) for r in events if fnum(r.get("max_adverse_pct")) is not None]
    days = [int(r["days_to_hit"]) for r in events if str(r.get("days_to_hit") or "").isdigit()]
    out = {
        **extra,
        "events": len(events),
        "symbols": len({r.get("code") for r in events}),
        "active_days": len({r.get("trade_date") for r in events}),
        "hit_rate": pct(sum(1 for r in events if r.get("hit")), len(events)) if events else None,
        "avg_days_to_hit": mean(days) if days else None,
        "median_days_to_hit": median(days) if days else None,
        "avg_return_pct": mean(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "win_rate": pct(sum(1 for v in returns if v > 0), len(returns)) if returns else None,
        "PF": pf(returns),
        "pnl_after_cost": sum(pnls),
        "max_dd_after_cost": max_drawdown(pnls),
        "median_max_adverse_pct": median(adverse) if adverse else None,
        "p95_max_adverse_pct": quantile(adverse, 0.05) if adverse else None,
    }
    return out


def build_environment_map() -> dict[str, dict[str, Any]]:
    env = {}
    for row in read_csv(ENV_ROWS):
        day = dtext(row.get("date"))
        if not day:
            continue
        env[day] = row
    return env


def environment_bucket(row: dict[str, Any], env: dict[str, dict[str, Any]]) -> str:
    e = env.get(dtext(row.get("trade_date")), {})
    tags = str(e.get("environment_tags") or "").lower()
    status = str(e.get("environment_status") or "").lower()
    score = fnum(e.get("environment_score"))
    if "darasage" in tags or "darasage" in status:
        return "darasage"
    if "crash" in tags or "rebound" in tags or "crash" in status:
        return "crash_rebound"
    if "sox" in tags:
        return "sox_shock"
    if score is not None and score >= 60:
        return "high_vol_or_favorable"
    if score is not None and score < 30:
        return "low_vol_or_warning"
    return "unknown_or_neutral"


def rank_symbol_types(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    names: dict[str, str] = {}
    for row in events:
        code = str(row.get("code") or "")
        if not code:
            continue
        groups[code].append(row)
        names[code] = str(row.get("name") or "")
    ranking = []
    types = []
    for code, rows in groups.items():
        if len(rows) < 10:
            continue
        s = summarize_events(rows, {"code": code, "name": names.get(code, "")})
        ranking.append(s)
        hit_rate = fnum(s.get("hit_rate"), 0.0) or 0.0
        pfv = fnum(s.get("PF"), 0.0) or 0.0
        adverse = fnum(s.get("median_max_adverse_pct"), 0.0) or 0.0
        avg_ret = fnum(s.get("avg_return_pct"), 0.0) or 0.0
        if hit_rate >= 65 and pfv >= 1.5:
            kind = "mean_reversion_strong"
        elif hit_rate < 45 or pfv < 0.9:
            kind = "trend_break_danger"
        elif adverse <= -5 and pfv >= 1.2:
            kind = "volatile_rebound"
        elif avg_ret < 0 and hit_rate < 55:
            kind = "darasage_stock"
        else:
            kind = "neutral_reversion"
        types.append({
            "code": code,
            "name": names.get(code, ""),
            "symbol_type": kind,
            **{k: v for k, v in s.items() if k not in {"code", "name"}},
        })
    ranking.sort(key=lambda r: (fnum(r.get("PF"), 0.0) or 0.0, fnum(r.get("hit_rate"), 0.0) or 0.0, r.get("events", 0)), reverse=True)
    types.sort(key=lambda r: (str(r.get("symbol_type")), -(fnum(r.get("PF"), 0.0) or 0.0)))
    return ranking, types


def train_test_stability(summary_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in summary_events:
        grouped[(str(row.get("buy_zone")), str(row.get("sell_zone")))].append(row)
    for (buy_zone, sell_zone), rows in grouped.items():
        if len(rows) < 30:
            continue
        rows = sorted(rows, key=lambda r: str(r.get("trade_date")))
        split = int(len(rows) * 0.7)
        train = summarize_events(rows[:split])
        test = summarize_events(rows[split:])
        train_pf = fnum(train.get("PF"))
        test_pf = fnum(test.get("PF"))
        out.append({
            "buy_zone": buy_zone,
            "sell_zone": sell_zone,
            "train_events": train.get("events"),
            "test_events": test.get("events"),
            "train_hit_rate": train.get("hit_rate"),
            "test_hit_rate": test.get("hit_rate"),
            "train_PF": train_pf,
            "test_PF": test_pf,
            "train_avg_return_pct": train.get("avg_return_pct"),
            "test_avg_return_pct": test.get("avg_return_pct"),
            "stable_flag": bool((train_pf or 0) >= 1.1 and (test_pf or 0) >= 1.1 and (test.get("events") or 0) >= 10),
        })
    out.sort(key=lambda r: (bool(r.get("stable_flag")), fnum(r.get("test_PF"), 0.0) or 0.0, r.get("test_events", 0)), reverse=True)
    return out


def monthly_yearly_summary(events: list[dict[str, Any]], min_events: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    monthly_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    yearly_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        day = dtext(row.get("trade_date"))
        if not day:
            continue
        ym = day[:7]
        yy = day[:4]
        key = (str(row.get("buy_zone")), str(row.get("sell_zone")), str(row.get("period")), ym)
        monthly_groups[key].append(row)
        yearly_groups[(key[0], key[1], key[2], yy)].append(row)

    monthly = []
    for (buy_zone, sell_zone, period, ym), rows in monthly_groups.items():
        if len(rows) >= min_events:
            monthly.append(summarize_events(rows, {
                "year_month": ym,
                "period": period,
                "buy_zone": buy_zone,
                "sell_zone": sell_zone,
            }))
    yearly = []
    for (buy_zone, sell_zone, period, yy), rows in yearly_groups.items():
        if len(rows) >= min_events:
            yearly.append(summarize_events(rows, {
                "year": yy,
                "period": period,
                "buy_zone": buy_zone,
                "sell_zone": sell_zone,
            }))
    monthly.sort(key=lambda r: (str(r.get("period")), str(r.get("buy_zone")), str(r.get("sell_zone")), str(r.get("year_month"))))
    yearly.sort(key=lambda r: (str(r.get("period")), str(r.get("buy_zone")), str(r.get("sell_zone")), str(r.get("year"))))
    return monthly, yearly


def outlier_sensitivity(events: list[dict[str, Any]], min_events: int) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        groups[(str(row.get("period")), str(row.get("buy_zone")), str(row.get("sell_zone")))].append(row)
    out = []
    for (period, buy_zone, sell_zone), rows in groups.items():
        if len(rows) < min_events:
            continue
        ordered = sorted(rows, key=lambda r: fnum(r.get("return_pct"), 0.0) or 0.0)
        trim = max(1, int(len(ordered) * 0.01))
        variants = {
            "raw": ordered,
            "drop_top_1pct": ordered[:-trim],
            "drop_bottom_1pct": ordered[trim:],
            "drop_both_1pct": ordered[trim:-trim] if len(ordered) > trim * 2 else [],
        }
        raw_pf = fnum(summarize_events(ordered).get("PF"))
        for label, variant in variants.items():
            if len(variant) < min_events:
                continue
            s = summarize_events(variant, {
                "period": period,
                "buy_zone": buy_zone,
                "sell_zone": sell_zone,
                "variant": label,
            })
            s["raw_PF"] = raw_pf
            s["PF_delta_vs_raw"] = (fnum(s.get("PF"), 0.0) or 0.0) - (raw_pf or 0.0)
            out.append(s)
    return out


def current_price_expectancy_rows(df: pd.DataFrame, summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = df.sort_values(["code", "trade_date"]).groupby("code").tail(1)
    best_by_buy: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        if str(row.get("period")) != "3y":
            continue
        if (row.get("events") or 0) < 30:
            continue
        buy = str(row.get("buy_zone"))
        cur = best_by_buy.get(buy)
        if cur is None or (fnum(row.get("PF"), 0.0) or 0.0) > (fnum(cur.get("PF"), 0.0) or 0.0):
            best_by_buy[buy] = row
    out = []
    masks = buy_zone_masks(latest)
    for buy_zone, mask in masks.items():
        best = best_by_buy.get(buy_zone)
        if not best:
            continue
        matched = latest[mask.fillna(False)]
        for _, row in matched.iterrows():
            out.append({
                "trade_date": row["trade_date"].date().isoformat(),
                "code": row.get("code"),
                "name": row.get("name"),
                "current_price": row.get("close"),
                "matched_buy_zone": buy_zone,
                "suggested_sell_zone": best.get("sell_zone"),
                "historical_events": best.get("events"),
                "historical_hit_rate": best.get("hit_rate"),
                "historical_PF": best.get("PF"),
                "historical_avg_return_pct": best.get("avg_return_pct"),
                "historical_median_max_adverse_pct": best.get("median_max_adverse_pct"),
                "range_position_60": row.get("range_position_60"),
                "rsi14": row.get("rsi14"),
                "ma25_gap_pct": row.get("ma25_gap_pct"),
                "drop20": row.get("drop20"),
            })
    out.sort(key=lambda r: (fnum(r.get("historical_PF"), 0.0) or 0.0, fnum(r.get("historical_hit_rate"), 0.0) or 0.0), reverse=True)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--universe", default="topix500", choices=["nikkei225", "topix500", "prime", "all"])
    parser.add_argument("--period", default="3y", choices=["1y", "2y", "3y", "5y", "all"])
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--max-rows", type=int, default=250_000)
    parser.add_argument("--max-symbols", type=int, default=800)
    parser.add_argument("--symbol-offset", type=int, default=0)
    parser.add_argument("--min-events", type=int, default=30)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--light", action="store_true")
    parser.add_argument("--full", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(ROOT / ".env")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    end_dt = datetime.fromisoformat(args.end).date() if args.end else date.today()
    if args.start:
        start_dt = datetime.fromisoformat(args.start).date()
    elif args.period == "all":
        start_dt = date(2008, 1, 1)
    else:
        start_dt = end_dt - timedelta(days=PERIODS.get(args.period, 365 * 3) + 300)

    client = build_supabase()
    raw, diagnostics = fetch_snapshots(
        client,
        start_dt.isoformat(),
        args.end or None,
        args.max_rows,
        args.max_symbols,
        args.symbol_offset,
    )
    df, prep_diag, proxy_usage = prepare_snapshots(raw, args.max_symbols)
    diagnostics.extend(prep_diag)

    if df.empty:
        write_csv(out_dir / "join_diagnostics.csv", diagnostics)
        write_csv(out_dir / "proxy_usage.csv", proxy_usage)
        write_text(out_dir / "report.txt", "No usable stock_feature_snapshots rows were available.\n")
        return

    latest_date = df["trade_date"].max().date()
    period_starts = {
        label: pd.Timestamp(latest_date - timedelta(days=days))
        for label, days in PERIODS.items()
        if args.period in {"all", label} or (args.full and label in PERIODS)
    }
    if args.period == "all" or args.full:
        period_starts["all_available"] = df["trade_date"].min()
    buy_zones = list(buy_zone_masks(df).keys())
    sell_zones = [
        "tp3pct",
        "tp5pct",
        "tp8pct",
        "tp10pct",
        "ma25_revert",
        "ma75_revert",
        "range20_ge_70",
        "range20_ge_80",
        "range20_ge_90",
        "box60_high_touch",
        "time5",
        "time10",
        "time20",
    ]

    all_events: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    for period_label, period_start in period_starts.items():
        for buy_zone in buy_zones:
            for sell_zone in sell_zones:
                events = evaluate_events(df, buy_zone, sell_zone, period_label, period_start)
                if not events:
                    continue
                all_events.extend(events)
                row = summarize_events(events, {
                    "period": period_label,
                    "buy_zone": buy_zone,
                    "sell_zone": sell_zone,
                })
                summary_rows.append(row)
                if row["events"] >= args.min_events:
                    matrix_rows.append(row)

    summary_rows.sort(key=lambda r: (str(r.get("period")), fnum(r.get("PF"), 0.0) or 0.0, fnum(r.get("hit_rate"), 0.0) or 0.0), reverse=True)
    matrix_rows.sort(key=lambda r: (fnum(r.get("PF"), 0.0) or 0.0, fnum(r.get("hit_rate"), 0.0) or 0.0, r.get("events", 0)), reverse=True)

    default_events = [
        r for r in all_events
        if r.get("period") == "3y" and r.get("sell_zone") == "tp5pct"
    ]
    symbol_rank, symbol_types = rank_symbol_types(default_events)

    h5_rows = []
    for period in PERIODS:
        rows_period = [r for r in all_events if r.get("period") == period and r.get("sell_zone") in {"tp3pct", "tp5pct", "ma25_revert"}]
        h5_events = [r for r in rows_period if r.get("h5_like")]
        normal_events = [r for r in rows_period if not r.get("h5_like")]
        for label, rows in [("h5_like", h5_events), ("normal_or_other", normal_events)]:
            if rows:
                h5_rows.append(summarize_events(rows, {"period": period, "bucket": label}))

    env_map = build_environment_map()
    env_rows = []
    if env_map:
        grouped_env: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in all_events:
            if row.get("period") != "3y" or row.get("sell_zone") not in {"tp3pct", "tp5pct", "ma25_revert"}:
                continue
            grouped_env[(str(row.get("buy_zone")), str(row.get("sell_zone")), environment_bucket(row, env_map))].append(row)
        for (buy_zone, sell_zone, bucket), rows in grouped_env.items():
            if len(rows) >= 10:
                env_rows.append(summarize_events(rows, {
                    "buy_zone": buy_zone,
                    "sell_zone": sell_zone,
                    "environment_bucket": bucket,
                }))
    else:
        proxy_usage.append({
            "item": "environment split",
            "proxy_used": "not available",
            "reason": f"{ENV_ROWS} not found or empty",
        })

    stable_rows = train_test_stability([r for r in all_events if r.get("period") == "3y"])
    monthly_rows, yearly_rows = monthly_yearly_summary(all_events, args.min_events)
    outlier_rows = outlier_sensitivity(all_events, args.min_events)
    current_rows = current_price_expectancy_rows(df, summary_rows)
    top_cases = [r for r in matrix_rows if (r.get("events") or 0) >= args.min_events][:100]
    worst_cases = sorted(
        [r for r in matrix_rows if (r.get("events") or 0) >= args.min_events],
        key=lambda r: (fnum(r.get("avg_return_pct"), 0.0) or 0.0, fnum(r.get("PF"), 999.0) or 999.0),
    )[:100]

    write_csv(out_dir / "price_band_expectancy_summary.csv", summary_rows)
    write_csv(out_dir / "symbol_expectancy_ranking.csv", symbol_rank)
    write_csv(out_dir / "buy_zone_sell_zone_matrix.csv", matrix_rows)
    write_csv(out_dir / "h5_vs_normal_reversion.csv", h5_rows)
    write_csv(out_dir / "environment_reversion_summary.csv", env_rows)
    write_csv(out_dir / "mean_reversion_symbol_types.csv", symbol_types)
    write_csv(out_dir / "top_reversion_cases.csv", top_cases)
    write_csv(out_dir / "worst_breakdown_cases.csv", worst_cases)
    write_csv(out_dir / "train_test_stability.csv", stable_rows)
    write_csv(out_dir / "monthly_summary.csv", monthly_rows)
    write_csv(out_dir / "yearly_summary.csv", yearly_rows)
    write_csv(out_dir / "outlier_sensitivity.csv", outlier_rows)
    write_csv(out_dir / "current_price_expectancy.csv", current_rows)
    write_csv(out_dir / "data_availability_report.csv", [{
        "universe": args.universe,
        "period_arg": args.period,
        "symbol_offset": args.symbol_offset,
        "max_symbols": args.max_symbols,
        "snapshot_rows_used": len(df),
        "symbols_used": df["code"].nunique(),
        "min_trade_date": df["trade_date"].min().date().isoformat(),
        "max_trade_date_for_20d_forward": latest_date.isoformat(),
        "note": "latest 20 trading days are excluded because 20d forward outcomes are required",
    }])
    overfit_rows = []
    robust_rows = []
    stable_keys = {(r.get("buy_zone"), r.get("sell_zone")): r for r in stable_rows}
    outlier_pf = {
        (r.get("period"), r.get("buy_zone"), r.get("sell_zone"), r.get("variant")): r
        for r in outlier_rows
    }
    for row in matrix_rows:
        stable = stable_keys.get((row.get("buy_zone"), row.get("sell_zone")), {})
        both = outlier_pf.get((row.get("period"), row.get("buy_zone"), row.get("sell_zone"), "drop_both_1pct"), {})
        warnings = []
        if (row.get("events") or 0) < 50:
            warnings.append("low_events")
        if stable and (fnum(stable.get("test_PF"), 0.0) or 0.0) < 1.0:
            warnings.append("weak_test")
        if both and (fnum(both.get("PF"), 0.0) or 0.0) < (fnum(row.get("PF"), 0.0) or 0.0) * 0.7:
            warnings.append("outlier_dependent")
        scored = dict(row)
        scored["test_PF"] = stable.get("test_PF")
        scored["stable_flag"] = stable.get("stable_flag")
        scored["outlier_drop_both_PF"] = both.get("PF")
        scored["overfit_warning"] = ";".join(warnings)
        score = (
            (fnum(row.get("PF"), 0.0) or 0.0)
            + (fnum(stable.get("test_PF"), 0.0) or 0.0)
            + (fnum(row.get("hit_rate"), 0.0) or 0.0) / 100.0
            - abs(fnum(row.get("max_dd_after_cost"), 0.0) or 0.0) / 1_000_000.0
        )
        scored["robust_score"] = score
        if warnings:
            overfit_rows.append(scored)
        else:
            robust_rows.append(scored)
    robust_rows.sort(key=lambda r: fnum(r.get("robust_score"), 0.0) or 0.0, reverse=True)
    write_csv(out_dir / "price_band_robust_best_cases.csv", robust_rows[:500])
    write_csv(out_dir / "price_band_overfit_warning_cases.csv", overfit_rows[:1000])
    diagnostics.append({
        "step": "analysis_output",
        "status": "ok",
        "latest_date": latest_date.isoformat(),
        "snapshot_rows_used": len(df),
        "symbols_used": df["code"].nunique(),
        "event_rows_generated": len(all_events),
        "summary_rows": len(summary_rows),
        "monthly_rows": len(monthly_rows),
        "yearly_rows": len(yearly_rows),
        "current_price_expectancy_rows": len(current_rows),
    })
    write_csv(out_dir / "join_diagnostics.csv", diagnostics)
    write_csv(out_dir / "proxy_usage.csv", proxy_usage)

    best = top_cases[0] if top_cases else {}
    best_stable = robust_rows[0] if robust_rows else next((r for r in stable_rows if r.get("stable_flag")), stable_rows[0] if stable_rows else {})
    h5_like = next((r for r in h5_rows if r.get("period") == "3y" and r.get("bucket") == "h5_like"), {})
    normal = next((r for r in h5_rows if r.get("period") == "3y" and r.get("bucket") == "normal_or_other"), {})
    high_vol = [r for r in env_rows if r.get("environment_bucket") in {"high_vol_or_favorable", "crash_rebound"}]
    darasage = [r for r in env_rows if r.get("environment_bucket") == "darasage"]
    type_counts = Counter(str(r.get("symbol_type")) for r in symbol_types)
    ntt = [r for r in symbol_rank if str(r.get("code")) == "9432"]

    report = [
        "# Price Band Expectancy Report",
        "",
        "Research-only script. Production H5/Primary/LINE/actual_trade_logs/auto-trading were not changed.",
        "",
        f"- snapshot_rows_used: {len(df):,}",
        f"- symbols_used: {df['code'].nunique():,}",
        f"- period: {df['trade_date'].min().date()} to {latest_date}",
        f"- generated_events: {len(all_events):,}",
        f"- total_cases: {len(summary_rows):,}",
        f"- valid_cases_min_events: {len(matrix_rows):,}",
        f"- robust_cases: {len(robust_rows):,}",
        f"- overfit_warning_cases: {len(overfit_rows):,}",
        "",
        "## Best Overall Price-Band Case",
        json.dumps(best, ensure_ascii=False, indent=2, default=str) if best else "No case met the minimum event threshold.",
        "",
        "## Best Train/Test Stable Case",
        json.dumps(best_stable, ensure_ascii=False, indent=2, default=str) if best_stable else "No stable case was found.",
        "",
        "## H5-Like vs Normal Reversion",
        f"- H5-like 3y: events={h5_like.get('events')}, hit_rate={h5_like.get('hit_rate')}, PF={h5_like.get('PF')}, avg_return={h5_like.get('avg_return_pct')}",
        f"- Normal/other 3y: events={normal.get('events')}, hit_rate={normal.get('hit_rate')}, PF={normal.get('PF')}, avg_return={normal.get('avg_return_pct')}",
        "",
        "## Environment",
        f"- high_vol_or_crash rows: {len(high_vol)} summary rows",
        f"- darasage rows: {len(darasage)} summary rows",
        "See environment_reversion_summary.csv for high-vol, crash-rebound, and darasage splits.",
        "",
        "## Symbol Types",
        *(f"- {k}: {v}" for k, v in sorted(type_counts.items())),
        "",
        "## NTT-Type Stable Reversion Check",
        json.dumps(ntt[:3], ensure_ascii=False, indent=2, default=str) if ntt else "9432 was not in the ranked output or did not meet the event threshold.",
        "",
        "## Notes",
        "- Box lines are not used as discretionary lines; they are converted into target prices such as range80%, MA25, MA75, and 60-day high touch.",
        "- PTS is ignored. Entry uses the snapshot close and forward regular-session OHLC.",
        "- TOPIX500/Nikkei225/liquid universe is approximated from available tradeable snapshots unless a constituent master is later added.",
        "- UI display of current-price expectancy was not added in this run; this is analysis output only.",
    ]
    write_text(out_dir / "report.txt", "\n".join(report) + "\n")


if __name__ == "__main__":
    main()
