#!/usr/bin/env python3
"""Backtest for the box_pullback strategy.

Read-only: fetches data from Supabase, writes CSV to outputs/box_backtest/.
Never writes to any DB table.

Exit strategies (configured via --exit-case):
  box_upper_exit  : tp=box_high, sl=box_low*0.97
  pullback_exit   : trailing 2% from peak, hard floor=entry*0.96
  ma25_stop_box_tp: tp=box_high, sl=close<ma25 OR entry*0.96
"""

from __future__ import annotations

import argparse
import bisect
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.box_signal_logic import (
    DEFAULTS,
    _box_metrics,
    _derived,
    _score,
    _signal_rejects,
    _to_float,
    _watch_rejects,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

OUTPUT_DIR = Path(__file__).parent.parent / "outputs" / "box_backtest"

HIST_COLUMNS = (
    "trade_date,code,high,low,close,volume,turnover_value,"
    "ma75,ma25,ma5,rsi14,volume_ratio_20d,atr14,per,pbr,is_deficit,roe"
)

EXIT_CASES = ("box_upper_exit", "pullback_exit", "ma25_stop_box_tp")


# ── Supabase helpers ─────────────────────────────────────────────────────────

def _build_supabase():
    mode = os.getenv("SUPABASE_MODE") or os.getenv("ENV") or ""
    mode_upper = mode.upper()
    url = (os.getenv(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or os.getenv("SUPABASE_URL")
    key = (os.getenv(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _fetch_all(build_query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        res = build_query().range(offset, offset + page_size - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        offset += page_size


def _date_chunks(start_date: str, end_date: str, days: int) -> list[tuple[str, str]]:
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    chunks: list[tuple[str, str]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max(1, days) - 1), end)
        chunks.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _latest_snapshot_date(sb, end_date: str) -> str:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .lte("trade_date", end_date)
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    if not rows:
        raise RuntimeError(f"stock_feature_snapshots has no rows before {end_date}")
    return str(rows[0]["trade_date"])


# ── Data loading ─────────────────────────────────────────────────────────────

def _load_settings(sb) -> dict:
    cfg = dict(DEFAULTS)
    try:
        rows = (
            sb.table("box_settings")
            .select("*")
            .eq("user_id", "global")
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            row = rows[0]
            for key in ("min_price", "min_turnover_value", "gu_skip_pct", "gd_skip_pct",
                        "min_equity_ratio", "max_per", "max_pbr", "signal_box_position_pct", "max_pending_days"):
                v = _to_float(row.get(key))
                if v is not None:
                    cfg[key] = v
                    if key == "signal_box_position_pct":
                        cfg["signal_box_position_max_pct"] = v
            ideal = _to_float(row.get("box_width_pct"))
            if ideal and ideal > 0:
                cfg["ideal_box_width_pct"] = ideal
                cfg["watch_box_width_min_pct"] = max(3.0, ideal * 0.6)
                cfg["watch_box_width_max_pct"] = max(12.0, ideal * 2.5)
            atr_max = _to_float(row.get("atr_max_pct"))
            if atr_max is not None:
                cfg["signal_atr_max_pct"] = atr_max
                cfg["watch_atr_max_pct"] = atr_max * 1.2
    except Exception as e:
        logger.warning("[backtest] box_settings unavailable; using defaults: %s", e)
    return cfg


def _load_history(
    sb,
    start_date: str,
    end_date: str,
    chunk_size: int = 25,
    date_chunk_days: int = 120,
    page_size: int = 1000,
) -> dict[str, list[dict]]:
    """Load OHLCV + indicator history for all prime stocks in the date range.

    Fetches in code chunks to avoid Supabase statement timeout.
    """
    logger.info("[backtest] loading history %s ~ %s ...", start_date, end_date)

    # Step 1: get code list from the latest available snapshot in range.
    # Avoid ordering all rows <= end_date, which can hit Supabase statement timeout.
    latest_date = _latest_snapshot_date(sb, end_date)
    logger.info("[backtest] latest code universe date=%s", latest_date)
    code_rows = _fetch_all(
        lambda: (
            sb.table("stock_feature_snapshots")
            .select("code")
            .eq("trade_date", latest_date)
            .eq("market", "prime")
            .order("code")
        )
    )
    codes = sorted({str(r["code"]) for r in code_rows if r.get("code")})
    date_ranges = _date_chunks(start_date, end_date, date_chunk_days)
    logger.info(
        "[backtest] prime codes=%d; fetching code_chunks=%d date_chunks=%d ...",
        len(codes),
        chunk_size,
        len(date_ranges),
    )

    # Step 2: fetch by both code chunk and date chunk. Keeping each SQL small is
    # slower than one broad query, but much more reliable for multi-year tests.
    by_code: dict[str, list[dict]] = defaultdict(list)
    total_rows = 0
    total_chunks = -(-len(codes) // chunk_size)
    for i in range(0, len(codes), chunk_size):
        code_chunk = codes[i : i + chunk_size]
        chunk_total = 0
        for range_start, range_end in date_ranges:
            chunk_rows = _fetch_all(
                lambda code_chunk=code_chunk, range_start=range_start, range_end=range_end: (
                    sb.table("stock_feature_snapshots")
                    .select(HIST_COLUMNS)
                    .in_("code", code_chunk)
                    .gte("trade_date", range_start)
                    .lte("trade_date", range_end)
                    .order("trade_date")
                    .order("code")
                ),
                page_size=page_size,
            )
            for r in chunk_rows:
                code = str(r.get("code") or "")
                if code:
                    by_code[code].append(r)
            total_rows += len(chunk_rows)
            chunk_total += len(chunk_rows)
        logger.info(
            "[backtest] chunk %d/%d done (+%d rows, total=%d)",
            i // chunk_size + 1,
            total_chunks,
            chunk_total,
            total_rows,
        )

    for rows in by_code.values():
        rows.sort(key=lambda r: str(r.get("trade_date") or ""))
    logger.info("[backtest] history total rows=%d codes=%d", total_rows, len(by_code))
    return dict(by_code)


def _load_margin_history(sb, start_date: str, end_date: str) -> dict[str, list[dict]]:
    """Load margin data keyed by code, sorted by date asc."""
    try:
        rows = _fetch_all(
            lambda: (
                sb.table("stock_weekly_margin_interest")
                .select("code,date,margin_ratio,long_margin_outstanding,short_margin_outstanding")
                .gte("date", start_date)
                .lte("date", end_date)
                .order("date")
            )
        )
    except Exception as e:
        logger.warning("[backtest] margin history unavailable: %s", e)
        return {}
    by_code: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        code = str(r.get("code") or "")
        if code:
            by_code[code].append(r)
    return dict(by_code)


def _load_regime_history(sb, start_date: str, end_date: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return {trade_date: regime} for short and long-term market regimes."""
    short: dict[str, str] = {}
    long_: dict[str, str] = {}
    try:
        rows = _fetch_all(
            lambda: (
                sb.table("market_regime")
                .select("trade_date,mode")
                .gte("trade_date", start_date)
                .lte("trade_date", end_date)
                .order("trade_date")
            )
        )
        short = {str(r["trade_date"]): str(r.get("mode") or "") for r in rows}
    except Exception:
        pass
    try:
        rows = _fetch_all(
            lambda: (
                sb.table("long_term_market_regime")
                .select("trade_date,regime")
                .gte("trade_date", start_date)
                .lte("trade_date", end_date)
                .order("trade_date")
            )
        )
        long_ = {str(r["trade_date"]): str(r.get("regime") or "") for r in rows}
    except Exception:
        pass
    return short, long_


# ── Point-in-time lookups ─────────────────────────────────────────────────────

def _pit_margin(margin_rows: list[dict], as_of_date: str) -> dict | None:
    """Latest margin record on or before as_of_date."""
    dates = [str(r["date"]) for r in margin_rows]
    idx = bisect.bisect_right(dates, as_of_date) - 1
    if idx < 0:
        return None
    return margin_rows[idx]


def _pit_regime(regime_map: dict[str, str], as_of_date: str) -> str | None:
    if not regime_map:
        return None
    dates = sorted(regime_map)
    idx = bisect.bisect_right(dates, as_of_date) - 1
    if idx < 0:
        return None
    return regime_map[dates[idx]]


# ── GU / GD approximation ────────────────────────────────────────────────────

def _is_gu(row: dict, prev_close: float, gu_pct: float) -> bool:
    day_open = _to_float(row.get("high"))  # use day_high as proxy when open unavailable
    if day_open is None or prev_close <= 0:
        return False
    return (day_open - prev_close) / prev_close * 100.0 >= gu_pct


def _is_gd(row: dict, prev_close: float, gd_pct: float) -> bool:
    day_low = _to_float(row.get("low"))
    if day_low is None or prev_close <= 0:
        return False
    return (prev_close - day_low) / prev_close * 100.0 >= gd_pct


# ── Signal generation (point-in-time) ────────────────────────────────────────

def _generate_signals(
    trade_date: str,
    history_slice: list[dict],  # rows up to and including trade_date
    current_row: dict,
    margin_rows: list[dict],
    cfg: dict,
    window: int,
) -> dict | None:
    """Return signal dict if entry criteria met on trade_date, else None."""
    metrics = _box_metrics(history_slice, window)
    if not metrics:
        return None

    margin_rec = _pit_margin(margin_rows, trade_date)
    if margin_rec:
        current_row = dict(current_row)
        current_row["margin_ratio"] = margin_rec.get("margin_ratio")
        current_row["margin_date"] = margin_rec.get("date")
        current_row["margin_buy_balance"] = margin_rec.get("long_margin_outstanding")
        current_row["margin_sell_balance"] = margin_rec.get("short_margin_outstanding")

    sig_reasons = _signal_rejects(current_row, metrics, cfg)
    if sig_reasons:
        return None

    score, reasons, warnings = _score(current_row, metrics, cfg, signal=True)
    box_low = metrics["box_low"]
    box_high = metrics["box_high"]

    return {
        "trade_date": trade_date,
        "code": str(current_row.get("code")),
        "name": current_row.get("name", ""),
        "close": _to_float(current_row.get("close")),
        "box_low": box_low,
        "box_high": box_high,
        "box_width_pct": metrics["box_width_pct"],
        "box_position_pct": metrics["box_position_pct"],
        "bounce_count": metrics["bounce_count"],
        "rsi14": _to_float(current_row.get("rsi14")),
        "atr_pct": _derived(current_row)["atr_pct"],
        "margin_ratio": _to_float(current_row.get("margin_ratio")),
        "score": round(score, 1),
        "entry_price_target": round(box_low * 1.01, 2),
        "entry_price_min": round(box_low, 2),
        "entry_price_max": round(box_low * 1.02, 2),
        "stop_loss": round(box_low * 0.97, 2),
        "take_profit": round(box_high, 2),
        "signal_reason": "・".join(reasons),
        "warnings": " / ".join(warnings) if warnings else "",
    }


# ── Entry fill simulation ─────────────────────────────────────────────────────

def _try_fill(signal: dict, future_rows: list[dict], cfg: dict, max_pending_days: int) -> dict | None:
    """
    Try to fill entry in the next `max_pending_days` trading days.
    Returns fill info dict or None if not filled.
    """
    entry_min = signal["entry_price_min"]
    entry_max = signal["entry_price_max"]
    signal_close = signal["close"] or 0.0
    gu_pct = cfg["gu_skip_pct"]
    gd_pct = cfg["gd_skip_pct"]

    for i, row in enumerate(future_rows[:max_pending_days]):
        day_high = _to_float(row.get("high"))
        day_low = _to_float(row.get("low"))
        prev_close = future_rows[i - 1].get("close") if i > 0 else signal_close
        prev_close_f = _to_float(prev_close) or signal_close

        if day_high is None or day_low is None:
            continue

        # GU skip: if day opens gap-up above entry zone, skip this day
        if _is_gu(row, prev_close_f, gu_pct):
            continue
        # GD skip: if day gaps down badly, skip (wait for stabilization)
        if _is_gd(row, prev_close_f, gd_pct):
            continue

        # Check if price touched entry zone intraday
        if day_low <= entry_max and day_high >= entry_min:
            fill_price = max(entry_min, min(entry_max, day_low))
            return {
                "fill_date": str(row["trade_date"]),
                "fill_price": round(fill_price, 2),
                "pending_days": i + 1,
            }

    return None


# ── Exit simulation ───────────────────────────────────────────────────────────

def _simulate_exit(
    fill: dict,
    signal: dict,
    future_rows: list[dict],
    exit_case: str,
    max_holding_days: int,
) -> dict:
    entry_price = fill["fill_price"]
    box_high = signal["box_high"]
    box_low = signal["box_low"]

    if exit_case == "box_upper_exit":
        tp = box_high
        sl = round(box_low * 0.97, 2)
    elif exit_case == "pullback_exit":
        tp = None  # trailing exit
        sl = round(entry_price * 0.96, 2)
    else:  # ma25_stop_box_tp
        tp = box_high
        sl = round(entry_price * 0.96, 2)

    highest = entry_price
    for i, row in enumerate(future_rows[:max_holding_days]):
        day_high = _to_float(row.get("high"))
        day_low = _to_float(row.get("low"))
        day_close = _to_float(row.get("close"))
        ma25 = _to_float(row.get("ma25"))
        if day_high is None or day_low is None or day_close is None:
            continue

        if exit_case == "pullback_exit":
            if day_high > highest:
                highest = day_high
            trailing_sl = round(highest * 0.98, 2)
            effective_sl = max(sl, trailing_sl)
        else:
            effective_sl = sl

        # Check stop loss hit intraday
        if day_low <= effective_sl:
            exit_reason = "stop_loss"
            exit_price = effective_sl
            pnl_pct = (exit_price - entry_price) / entry_price * 100.0
            holding_days = i + 1
            return _exit_record(fill, signal, row, exit_reason, exit_price, pnl_pct, holding_days, exit_case)

        # Check take profit (not applicable for pullback_exit)
        if tp is not None and day_high >= tp:
            exit_reason = "take_profit"
            exit_price = tp
            pnl_pct = (exit_price - entry_price) / entry_price * 100.0
            holding_days = i + 1
            return _exit_record(fill, signal, row, exit_reason, exit_price, pnl_pct, holding_days, exit_case)

        # ma25_stop_box_tp: exit if close falls below MA25
        if exit_case == "ma25_stop_box_tp" and ma25 is not None and day_close < ma25:
            exit_reason = "ma25_stop"
            exit_price = day_close
            pnl_pct = (exit_price - entry_price) / entry_price * 100.0
            holding_days = i + 1
            return _exit_record(fill, signal, row, exit_reason, exit_price, pnl_pct, holding_days, exit_case)

    # Max holding days reached — exit at last available close
    last = future_rows[min(max_holding_days - 1, len(future_rows) - 1)]
    exit_price = _to_float(last.get("close")) or entry_price
    pnl_pct = (exit_price - entry_price) / entry_price * 100.0
    return _exit_record(fill, signal, last, "max_holding", exit_price, pnl_pct, min(max_holding_days, len(future_rows)), exit_case)


def _exit_record(fill: dict, signal: dict, exit_row: dict, reason: str, exit_price: float, pnl_pct: float, holding_days: int, exit_case: str) -> dict:
    return {
        "signal_date": signal["trade_date"],
        "code": signal["code"],
        "fill_date": fill["fill_date"],
        "fill_price": fill["fill_price"],
        "pending_days": fill["pending_days"],
        "exit_date": str(exit_row["trade_date"]),
        "exit_price": round(exit_price, 2),
        "exit_reason": reason,
        "exit_case": exit_case,
        "pnl_pct": round(pnl_pct, 3),
        "holding_days": holding_days,
        "box_low": signal["box_low"],
        "box_high": signal["box_high"],
        "box_width_pct": round(signal["box_width_pct"], 2),
        "box_position_pct": round(signal["box_position_pct"], 2),
        "bounce_count": signal["bounce_count"],
        "rsi14": signal["rsi14"],
        "atr_pct": round(signal["atr_pct"], 3) if signal["atr_pct"] else None,
        "margin_ratio": signal["margin_ratio"],
        "score": signal["score"],
    }


# ── Bucket helpers ────────────────────────────────────────────────────────────

def _bucket_box_position(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v <= 10:
        return "0-10"
    if v <= 20:
        return "10-20"
    if v <= 35:
        return "20-35"
    return "35+"


def _bucket_box_width(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v <= 10:
        return "<=10"
    if v <= 15:
        return "10-15"
    if v <= 20:
        return "15-20"
    if v <= 25:
        return "20-25"
    return "25+"


def _bucket_bounce(v: int | None) -> str:
    if v is None:
        return "unknown"
    if v <= 2:
        return "2"
    if v <= 4:
        return "3-4"
    if v <= 7:
        return "5-7"
    return "8+"


def _bucket_margin(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v <= 1:
        return "<=1"
    if v <= 5:
        return "1-5"
    if v <= 15:
        return "5-15"
    if v <= 30:
        return "15-30"
    return "30+"


def _bucket_rsi(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v < 35:
        return "<35"
    if v <= 45:
        return "35-45"
    if v <= 55:
        return "45-55"
    if v <= 65:
        return "55-65"
    return "65+"


def _bucket_atr(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v <= 2:
        return "<=2"
    if v <= 3.5:
        return "2-3.5"
    if v <= 5:
        return "3.5-5"
    return "5+"


# ── Summary computation ───────────────────────────────────────────────────────

def _stats(pnl_list: list[float]) -> dict:
    if not pnl_list:
        return {"count": 0, "win_rate": None, "avg_pnl": None, "median_pnl": None,
                "avg_win": None, "avg_loss": None, "profit_factor": None, "max_dd": None}
    wins = [p for p in pnl_list if p > 0]
    losses = [p for p in pnl_list if p <= 0]
    sorted_pnl = sorted(pnl_list)
    median = sorted_pnl[len(sorted_pnl) // 2]
    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else None
    # max drawdown over cumulative P&L series
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnl_list:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    return {
        "count": len(pnl_list),
        "win_rate": round(len(wins) / len(pnl_list) * 100, 1),
        "avg_pnl": round(mean(pnl_list), 3),
        "median_pnl": round(median, 3),
        "avg_win": round(mean(wins), 3) if wins else None,
        "avg_loss": round(mean(losses), 3) if losses else None,
        "profit_factor": round(pf, 3) if pf is not None else None,
        "max_dd_pct": round(max_dd, 3),
    }


def _bucket_summary(trades: list[dict], dim: str, key_fn) -> list[dict]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        bucket = key_fn(t.get(dim))
        buckets[bucket].append(t["pnl_pct"])
    rows = []
    for bucket, pnls in sorted(buckets.items()):
        s = _stats(pnls)
        rows.append({"dimension": dim, "bucket": bucket, **s})
    return rows


# ── CSV writing ───────────────────────────────────────────────────────────────

def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        logger.info("[backtest] %s: 0 rows, skipping", path.name)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("[backtest] wrote %s (%d rows)", path, len(rows))


# ── Main backtest loop ────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    cfg = _load_settings(sb)
    if args.signal_box_position_max_pct is not None:
        cfg["signal_box_position_pct"] = float(args.signal_box_position_max_pct)
        cfg["signal_box_position_max_pct"] = float(args.signal_box_position_max_pct)
    logger.info(
        "[backtest] settings: signal_box_position=%.1f max_pending_days=%s",
        cfg["signal_box_position_pct"],
        args.max_pending_days,
    )

    start_date = args.start
    end_date = args.end
    window = args.window
    lookback_days = args.lookback_days
    max_holding_days = args.max_holding_days
    max_pending_days = args.max_pending_days
    exit_cases = args.exit_case if args.exit_case else list(EXIT_CASES)

    # Extend fetch start to cover the lookback window for box_metrics
    fetch_start = (
        datetime.fromisoformat(start_date).date() - timedelta(days=lookback_days)
    ).isoformat()

    history = _load_history(sb, fetch_start, end_date)
    margin_hist = _load_margin_history(sb, fetch_start, end_date)
    short_regime_hist, long_regime_hist = _load_regime_history(sb, fetch_start, end_date)

    # All unique trade dates in range (sorted)
    all_dates: list[str] = sorted(
        {str(r["trade_date"]) for rows in history.values() for r in rows
         if start_date <= str(r["trade_date"]) <= end_date}
    )
    logger.info("[backtest] signal scan dates=%d codes=%d exit_cases=%s",
                len(all_dates), len(history), exit_cases)

    signal_rows: list[dict] = []
    trade_rows: list[dict] = []

    for code, code_rows in history.items():
        code_dates = [str(r["trade_date"]) for r in code_rows]
        margin_rows_code = margin_hist.get(code, [])

        for d_idx, trade_date in enumerate(all_dates):
            if trade_date not in code_dates:
                continue
            row_idx = code_dates.index(trade_date)
            current_row = code_rows[row_idx]

            # Build history slice up to trade_date (point-in-time)
            history_slice = code_rows[:row_idx + 1]

            sig = _generate_signals(
                trade_date, history_slice, current_row,
                margin_rows_code, cfg, window
            )
            if sig is None:
                continue

            # Attach regime
            sig["short_regime"] = _pit_regime(short_regime_hist, trade_date) or ""
            sig["long_regime"] = _pit_regime(long_regime_hist, trade_date) or ""
            signal_rows.append(sig)

            # Future rows for entry fill + exit simulation
            future_rows = code_rows[row_idx + 1:]
            if not future_rows:
                continue

            fill = _try_fill(sig, future_rows, cfg, max_pending_days)
            if fill is None:
                sig["fill_status"] = "no_fill"
                continue
            sig["fill_status"] = "filled"

            # Rows after fill for exit simulation
            fill_date = fill["fill_date"]
            fill_idx_in_future = next(
                (i for i, r in enumerate(future_rows) if str(r["trade_date"]) == fill_date), None
            )
            if fill_idx_in_future is None:
                continue
            exit_rows = future_rows[fill_idx_in_future + 1:]

            for exit_case in exit_cases:
                if not exit_rows:
                    break
                trade = _simulate_exit(fill, sig, exit_rows, exit_case, max_holding_days)
                trade["short_regime"] = sig["short_regime"]
                trade["long_regime"] = sig["long_regime"]
                trade_rows.append(trade)

    logger.info("[backtest] signals=%d trades=%d", len(signal_rows), len(trade_rows))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _write_csv(OUTPUT_DIR / f"box_backtest_signals_{ts}.csv", signal_rows)
    _write_csv(OUTPUT_DIR / f"box_backtest_trades_{ts}.csv", trade_rows)

    # Per-exit-case summary
    for exit_case in exit_cases:
        case_trades = [t for t in trade_rows if t["exit_case"] == exit_case]
        if not case_trades:
            continue
        pnls = [t["pnl_pct"] for t in case_trades]
        s = _stats(pnls)
        logger.info(
            "[backtest][%s] trades=%d win_rate=%.1f%% avg_pnl=%.2f%% pf=%s",
            exit_case, s["count"], s["win_rate"] or 0, s["avg_pnl"] or 0,
            ("%.2f" % s["profit_factor"]) if s["profit_factor"] else "—",
        )

    # Overall summary CSV
    summary_rows: list[dict] = []
    for exit_case in exit_cases:
        case_trades = [t for t in trade_rows if t["exit_case"] == exit_case]
        s = _stats([t["pnl_pct"] for t in case_trades])
        exit_counts: dict[str, int] = defaultdict(int)
        for t in case_trades:
            exit_counts[t["exit_reason"]] += 1
        summary_rows.append({
            "exit_case": exit_case,
            **s,
            "exit_by_tp": exit_counts.get("take_profit", 0),
            "exit_by_sl": exit_counts.get("stop_loss", 0),
            "exit_by_ma25": exit_counts.get("ma25_stop", 0),
            "exit_by_maxhold": exit_counts.get("max_holding", 0),
            "signals_total": len([sig for sig in signal_rows]),
            "fills_total": len([sig for sig in signal_rows if sig.get("fill_status") == "filled"]),
        })
    _write_csv(OUTPUT_DIR / f"box_backtest_summary_{ts}.csv", summary_rows)

    # Bucket breakdown (only for primary exit case or first)
    primary_exit = exit_cases[0]
    primary_trades = [t for t in trade_rows if t["exit_case"] == primary_exit]
    if primary_trades:
        bucket_rows: list[dict] = []
        bucket_rows += _bucket_summary(primary_trades, "box_position_pct", _bucket_box_position)
        bucket_rows += _bucket_summary(primary_trades, "box_width_pct", _bucket_box_width)
        bucket_rows += _bucket_summary(primary_trades, "bounce_count", _bucket_bounce)
        bucket_rows += _bucket_summary(primary_trades, "margin_ratio", _bucket_margin)
        bucket_rows += _bucket_summary(primary_trades, "rsi14", _bucket_rsi)
        bucket_rows += _bucket_summary(primary_trades, "atr_pct", _bucket_atr)
        bucket_rows += _bucket_summary(primary_trades, "short_regime", lambda v: v or "unknown")
        bucket_rows += _bucket_summary(primary_trades, "long_regime", lambda v: v or "unknown")
        _write_csv(OUTPUT_DIR / f"box_backtest_bucket_{ts}.csv", bucket_rows)

    # Per-symbol summary
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for t in primary_trades:
        by_symbol[t["code"]].append(t["pnl_pct"])
    symbol_rows = []
    for code, pnls in sorted(by_symbol.items()):
        s = _stats(pnls)
        symbol_rows.append({"code": code, "exit_case": primary_exit, **s})
    symbol_rows.sort(key=lambda r: (r["avg_pnl"] or -999), reverse=True)
    _write_csv(OUTPUT_DIR / f"box_backtest_by_symbol_{ts}.csv", symbol_rows)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest box_pullback strategy")
    parser.add_argument("--start", required=True, help="Signal scan start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Signal scan end date YYYY-MM-DD")
    parser.add_argument("--window", type=int, default=120, help="Box window (trading days)")
    parser.add_argument("--lookback-days", type=int, default=220, help="Calendar days for history fetch pre-start")
    parser.add_argument("--max-holding-days", type=int, default=20, help="Max holding days per trade")
    parser.add_argument("--max-pending-days", type=int, default=5, help="Max days waiting for entry fill")
    parser.add_argument(
        "--signal-box-position-max-pct",
        type=float,
        default=45.0,
        help="Override box_position_pct max for signal generation. Default: 45.",
    )
    parser.add_argument(
        "--exit-case",
        nargs="+",
        choices=EXIT_CASES,
        default=["ma25_stop_box_tp"],
        help="Exit cases to simulate (default: ma25_stop_box_tp)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
