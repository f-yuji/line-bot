#!/usr/bin/env python3
"""Generate box_lab watchlist and pullback entry candidates.

box_lab is intentionally separate from rebound_lab. This script reads shared
market snapshots, writes only box_watchlist / box_signals, and never creates
virtual trades.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.box_signal_logic import (
    DEFAULTS,
    _box_metrics,
    _derived,
    _equity_ratio,
    _quality_warnings,
    _score,
    _signal_rejects,
    _to_bool,
    _to_float,
    _watch_rejects,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


SNAPSHOT_COLUMNS = (
    "id,trade_date,code,name,market,sector,high,low,close,volume,turnover_value,"
    "ma75,ma75_gap_pct,ma25,ma25_gap_pct,ma5,ma5_gap_pct,rsi14,volume_ratio_20d,"
    "atr14,per,pbr,is_deficit,roe,dividend_yield_pct"
)


def _opt(name: str) -> str:
    return os.getenv(name) or ""


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper() if mode else ""
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _latest_trade_date(sb, trade_date: str | None) -> str:
    if trade_date:
        return trade_date
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    if not rows:
        raise RuntimeError("stock_feature_snapshots is empty")
    return str(rows[0]["trade_date"])


def _fetch_all(build_query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        query = build_query()
        res = query.range(offset, offset + page_size - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        offset += page_size


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
            entry_mode = str(row.get("entry_mode") or cfg["entry_mode"])
            cfg["entry_mode"] = entry_mode
            for key in (
                "min_price",
                "min_turnover_value",
                "gu_skip_pct",
                "gd_skip_pct",
                "min_equity_ratio",
                "max_per",
                "max_pbr",
            ):
                value = _to_float(row.get(key))
                if value is not None:
                    cfg[key] = value
            ideal_box_width = _to_float(row.get("box_width_pct"))
            if ideal_box_width is not None and ideal_box_width > 0:
                cfg["ideal_box_width_pct"] = ideal_box_width
                cfg["watch_box_width_min_pct"] = max(3.0, ideal_box_width * 0.6)
                cfg["watch_box_width_max_pct"] = max(12.0, ideal_box_width * 2.5)
            atr_max = _to_float(row.get("atr_max_pct"))
            if atr_max is not None:
                cfg["signal_atr_max_pct"] = atr_max
                cfg["watch_atr_max_pct"] = atr_max * 1.2
    except Exception as e:
        logger.warning("[box_lab] box_settings unavailable; defaults used: %s", e)
    return cfg


def _load_market_context(sb) -> tuple[str | None, str | None]:
    short_regime = None
    long_regime = None
    try:
        rows = (
            sb.table("market_regime")
            .select("mode")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            short_regime = rows[0].get("mode")
    except Exception:
        pass
    try:
        rows = (
            sb.table("long_term_market_regime")
            .select("regime")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            long_regime = rows[0].get("regime")
    except Exception:
        pass
    return short_regime, long_regime


def _load_snapshots(sb, trade_date: str, lookback_days: int) -> tuple[list[dict], dict[str, list[dict]]]:
    latest_rows = _fetch_all(
        lambda: (
            sb.table("stock_feature_snapshots")
            .select(SNAPSHOT_COLUMNS)
            .eq("trade_date", trade_date)
            .eq("market", "prime")
            .eq("is_tradeable", True)
            .order("code")
        )
    )
    start_date = (datetime.fromisoformat(trade_date[:10]).date() - timedelta(days=lookback_days)).isoformat()
    codes = sorted({str(r.get("code")) for r in latest_rows if r.get("code")})
    by_code: dict[str, list[dict]] = defaultdict(list)
    for i in range(0, len(codes), 100):
        chunk = codes[i:i + 100]
        hist_rows = _fetch_all(
            lambda chunk=chunk: (
                sb.table("stock_feature_snapshots")
                .select("trade_date,code,high,low,close")
                .in_("code", chunk)
                .gte("trade_date", start_date)
                .lte("trade_date", trade_date)
                .order("trade_date")
            )
        )
        for row in hist_rows:
            code = str(row.get("code") or "")
            if code:
                by_code[code].append(row)
    return latest_rows, by_code


def _load_margin_data(sb, trade_date: str, lookback_days: int = 60) -> dict[str, dict]:
    cutoff = (datetime.fromisoformat(trade_date[:10]).date() - timedelta(days=lookback_days)).isoformat()
    try:
        rows = _fetch_all(
            lambda: (
                sb.table("stock_weekly_margin_interest")
                .select("code,date,margin_ratio,long_margin_outstanding,short_margin_outstanding")
                .gte("date", cutoff)
                .lte("date", trade_date[:10])
                .order("date", desc=True)
            )
        )
    except Exception as e:
        logger.warning("[box_lab] margin data unavailable: %s", e)
        return {}
    by_code: dict[str, dict] = {}
    for row in rows:
        code = str(row.get("code") or "")
        if code and code not in by_code:
            by_code[code] = {
                "margin_ratio": row.get("margin_ratio"),
                "margin_date": row.get("date"),
                "margin_buy_balance": row.get("long_margin_outstanding"),
                "margin_sell_balance": row.get("short_margin_outstanding"),
            }
    return by_code


def _base_payload(row: dict, metrics: dict, cfg: dict, short_regime: str | None, long_regime: str | None) -> dict:
    d = _derived(row)
    equity_ratio = _equity_ratio(row)
    return {
        "trade_date": row.get("trade_date"),
        "code": str(row.get("code")),
        "name": row.get("name"),
        "sector": row.get("sector"),
        "strategy_type": "box_pullback",
        "close": d["close"],
        "box_high": metrics["box_high"],
        "box_low": metrics["box_low"],
        "box_width_pct": metrics["box_width_pct"],
        "box_position_pct": metrics["box_position_pct"],
        "box_days": metrics["box_days"],
        "bounce_count": metrics["bounce_count"],
        "atr_pct": round(d["atr_pct"], 4) if d["atr_pct"] is not None else None,
        "ma5_gap_pct": row.get("ma5_gap_pct"),
        "ma25_gap_pct": row.get("ma25_gap_pct"),
        "ma75_gap_pct": row.get("ma75_gap_pct"),
        "rsi14": row.get("rsi14"),
        "volume_ratio_20d": row.get("volume_ratio_20d"),
        "turnover_value": d["turnover_value"],
        "per": row.get("per"),
        "pbr": row.get("pbr"),
        "equity_ratio": equity_ratio,
        "margin_ratio": row.get("margin_ratio"),
        "margin_date": str(row.get("margin_date")) if row.get("margin_date") else None,
        "margin_buy_balance": row.get("margin_buy_balance"),
        "margin_sell_balance": row.get("margin_sell_balance"),
        "raw": {
            "snapshot_id": row.get("id"),
            "short_market_regime": short_regime,
            "long_market_regime": long_regime,
            "ma75": row.get("ma75"),
            "dividend_yield_pct": row.get("dividend_yield_pct"),
            "roe": row.get("roe"),
            "filter": "large_cap_box_range_v1",
            "ideal_box_width_pct": cfg["ideal_box_width_pct"],
            "box_width_min_pct": cfg["watch_box_width_min_pct"],
            "box_width_max_pct": cfg["watch_box_width_max_pct"],
            "market_support_status": None,
            "nikkei_trend_warning": None,
            "relative_strength_vs_nikkei": None,
            "margin_ratio": row.get("margin_ratio"),
            "margin_date": str(row.get("margin_date")) if row.get("margin_date") else None,
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _watch_payload(row: dict, metrics: dict, cfg: dict, short_regime: str | None, long_regime: str | None) -> dict:
    score, reasons, warnings = _score(row, metrics, cfg, signal=False)
    payload = _base_payload(row, metrics, cfg, short_regime, long_regime)
    payload.update(
        {
            "status": "watching",
            "signal_status": "watching",
            "watch_score": round(score, 2),
            "watch_reason": "・".join(reasons),
            "warnings": " / ".join(warnings) if warnings else None,
        }
    )
    return payload


def _signal_payload(row: dict, metrics: dict, cfg: dict, short_regime: str | None, long_regime: str | None) -> dict:
    score, reasons, warnings = _score(row, metrics, cfg, signal=True)
    box_low = metrics["box_low"]
    box_high = metrics["box_high"]
    payload = _base_payload(row, metrics, cfg, short_regime, long_regime)
    payload.update(
        {
            "status": "signal_generated",
            "entry_status": "entry_pending",
            "entry_pending_at": datetime.now(timezone.utc).isoformat(),
            "box_upper": box_high,
            "box_lower": box_low,
            "box_score": round(score, 2),
            "entry_target_price": round(box_low * 1.01, 4),
            "entry_price_min": round(box_low, 4),
            "entry_price_max": round(box_low * 1.02, 4),
            "stop_loss_price": round(box_low * 0.97, 4),
            "take_profit_price": round(box_high, 4),
            "entry_skip_gu_pct": cfg["gu_skip_pct"],
            "entry_skip_gd_pct": cfg["gd_skip_pct"],
            "entry_reason": "・".join(reasons),
            "signal_reason": "・".join(reasons),
            "entry_mode": cfg["entry_mode"],
            "short_market_regime": short_regime,
            "long_market_regime": long_regime,
            "warnings": " / ".join(warnings) if warnings else None,
        }
    )
    return payload


def _upsert_optional(sb, table: str, rows: list[dict], dry_run: bool, on_conflict: str = "trade_date,code") -> None:
    if dry_run or not rows:
        return
    remaining = [dict(r) for r in rows]
    for _ in range(20):
        try:
            sb.table(table).upsert(remaining, on_conflict=on_conflict).execute()
            return
        except Exception as e:
            msg = str(e)
            if "Could not find the table" in msg or "PGRST205" in msg:
                logger.warning("[%s] table missing; run db/box_lab.sql in Supabase SQL Editor", table)
                return
            marker = "Could not find the '"
            missing = None
            if marker in msg:
                missing = msg.split(marker, 1)[1].split("'", 1)[0]
            if missing:
                logger.warning("[%s] column missing; skip field for this run: %s", table, missing)
                for row in remaining:
                    row.pop(missing, None)
                    raw = row.get("raw")
                    if isinstance(raw, dict):
                        raw.pop(missing, None)
                continue
            raise


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    trade_date = _latest_trade_date(sb, args.trade_date)
    cfg = _load_settings(sb)
    short_regime, long_regime = _load_market_context(sb)
    latest_rows, history_by_code = _load_snapshots(sb, trade_date, int(args.lookback_days))
    margin_by_code = _load_margin_data(sb, trade_date)
    for row in latest_rows:
        code = str(row.get("code") or "")
        m = margin_by_code.get(code, {})
        row["margin_ratio"] = m.get("margin_ratio")
        row["margin_date"] = m.get("margin_date")
        row["margin_buy_balance"] = m.get("margin_buy_balance")
        row["margin_sell_balance"] = m.get("margin_sell_balance")
    margin_missing = sum(1 for r in latest_rows if r.get("margin_ratio") is None)
    margin_high = sum(1 for r in latest_rows if (_to_float(r.get("margin_ratio")) or 0) > cfg["margin_ratio_warning"])
    margin_overheated = sum(1 for r in latest_rows if (_to_float(r.get("margin_ratio")) or 0) > 50)
    logger.info("[box_lab] margin rows loaded=%d latest<=%s", len(margin_by_code), trade_date)
    logger.info("[box_lab] margin warnings: missing=%d high=%d overheated=%d", margin_missing, margin_high, margin_overheated)
    logger.info("[box_lab] trade_date=%s latest_rows=%d dry_run=%s", trade_date, len(latest_rows), args.dry_run)
    logger.info(
        "[box_lab] settings: entry_mode=%s ideal_box_width=%.1f box_width_range=%.1f-%.1f min_equity_ratio=%.1f atr_max_pct=%.1f",
        cfg["entry_mode"],
        cfg["ideal_box_width_pct"],
        cfg["watch_box_width_min_pct"],
        cfg["watch_box_width_max_pct"],
        cfg["min_equity_ratio"],
        cfg["signal_atr_max_pct"],
    )
    signals_paused = cfg["entry_mode"] == "paused"
    if signals_paused:
        logger.info("[box_lab] entry_mode=paused のため signal生成停止。box_watchlist のみ生成します")

    watch_rows: list[dict] = []
    signal_rows: list[dict] = []
    watch_rejects: dict[str, int] = defaultdict(int)
    signal_rejects: dict[str, int] = defaultdict(int)

    for row in latest_rows:
        code = str(row.get("code") or "")
        metrics = _box_metrics(history_by_code.get(code, []), window=int(args.window))
        if not metrics:
            watch_rejects["insufficient_box_history"] += 1
            continue
        watch_reasons = _watch_rejects(row, metrics, cfg)
        if watch_reasons:
            for reason in watch_reasons:
                watch_rejects[reason] += 1
            continue
        watch_rows.append(_watch_payload(row, metrics, cfg, short_regime, long_regime))

        if not signals_paused:
            sig_reasons = _signal_rejects(row, metrics, cfg)
            if sig_reasons:
                for reason in sig_reasons:
                    signal_rejects[reason] += 1
                continue
            signal_rows.append(_signal_payload(row, metrics, cfg, short_regime, long_regime))

    watch_rows.sort(key=lambda r: (r.get("watch_score") or 0, -(r.get("box_position_pct") or 999)), reverse=True)
    signal_rows.sort(key=lambda r: (r.get("box_score") or 0, -(r.get("box_position_pct") or 999)), reverse=True)
    if args.watch_limit:
        watch_rows = watch_rows[: int(args.watch_limit)]
    if args.signal_limit:
        signal_rows = signal_rows[: int(args.signal_limit)]

    logger.info("[box_watchlist] candidates=%d rejects=%s", len(watch_rows), dict(sorted(watch_rejects.items())))
    for row in watch_rows[:20]:
        logger.info(
            "[box_watchlist] code=%s name=%s score=%.1f pos=%.1f width=%.1f bounce=%s",
            row.get("code"),
            row.get("name"),
            row.get("watch_score") or 0,
            row.get("box_position_pct") or 0,
            row.get("box_width_pct") or 0,
            row.get("bounce_count") or 0,
        )

    logger.info("[box_signals] entry_pending=%d rejects=%s", len(signal_rows), dict(sorted(signal_rejects.items())))
    margin_too_high_count = sum(1 for r in watch_rejects.items() if r[0] == "margin_ratio_too_high" for _ in range(r[1]))
    if margin_too_high_count:
        logger.info("[box_lab] reject margin_ratio_too_high=%d", watch_rejects.get("margin_ratio_too_high", 0))
    for row in signal_rows[:20]:
        mr = row.get("margin_ratio")
        margin_str = "%.1f" % mr if mr is not None else "—"
        logger.info(
            "[box_signals] code=%s name=%s score=%.1f pos=%.1f entry=%.0f-%.0f margin=%s",
            row.get("code"),
            row.get("name"),
            row.get("box_score") or 0,
            row.get("box_position_pct") or 0,
            row.get("entry_price_min") or 0,
            row.get("entry_price_max") or 0,
            margin_str,
        )

    _upsert_optional(sb, "box_watchlist", watch_rows, bool(args.dry_run))
    _upsert_optional(sb, "box_signals", signal_rows, bool(args.dry_run))
    if not args.dry_run:
        logger.info("[box_lab] saved watchlist=%d signals=%d", len(watch_rows), len(signal_rows))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate box_lab watchlist and pullback signals")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--lookback-days", type=int, default=220)
    parser.add_argument("--window", type=int, default=120)
    parser.add_argument("--watch-limit", type=int, default=150)
    parser.add_argument("--signal-limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
