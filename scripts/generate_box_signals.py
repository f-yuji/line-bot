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
from statistics import mean
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


DEFAULTS = {
    "min_price": 1000.0,
    "min_turnover_value": 1_000_000_000.0,
    "watch_box_width_min_pct": 5.0,
    "watch_box_width_max_pct": 25.0,
    "watch_rsi_min": 30.0,
    "watch_rsi_hard_max": 70.0,
    "watch_atr_max_pct": 6.0,
    "watch_volume_ratio_min": 0.5,
    "watch_min_bounce_count": 2,
    "signal_box_position_pct": 35.0,
    "signal_strong_position_pct": 20.0,
    "signal_rsi_min": 35.0,
    "signal_rsi_cool_max": 55.0,
    "signal_rsi_hard_max": 70.0,
    "signal_atr_max_pct": 5.0,
    "signal_volume_ratio_min": 0.7,
    "volume_ratio_warning_max": 3.0,
    "max_per": 40.0,
    "max_pbr": 5.0,
    "gu_skip_pct": 3.0,
    "gd_skip_pct": 5.0,
}

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


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1", "yes"}


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
            for key in (
                "min_price",
                "min_turnover_value",
                "gu_skip_pct",
                "gd_skip_pct",
                "max_per",
                "max_pbr",
            ):
                value = _to_float(row.get(key))
                if value is not None:
                    cfg[key] = value
            atr_max = _to_float(row.get("atr_max_pct"))
            if atr_max is not None:
                cfg["signal_atr_max_pct"] = atr_max
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


def _box_metrics(history: list[dict], window: int) -> dict | None:
    rows = [r for r in history if _to_float(r.get("high")) is not None and _to_float(r.get("low")) is not None]
    if len(rows) < min(80, window):
        return None
    rows = rows[-window:]
    highs = [_to_float(r.get("high")) for r in rows]
    lows = [_to_float(r.get("low")) for r in rows]
    closes = [_to_float(r.get("close")) for r in rows if _to_float(r.get("close")) is not None]
    if not highs or not lows or not closes:
        return None
    box_high = max(v for v in highs if v is not None)
    box_low = min(v for v in lows if v is not None)
    if not box_low or box_high <= box_low:
        return None
    width_pct = (box_high - box_low) / box_low * 100.0
    close = closes[-1]
    position_pct = (close - box_low) / (box_high - box_low) * 100.0
    lower_band = box_low + (box_high - box_low) * 0.30
    bounce_count = 0
    was_near_low = False
    for row in rows:
        low = _to_float(row.get("low"))
        close_i = _to_float(row.get("close"))
        if low is None or close_i is None:
            continue
        near_low = low <= lower_band
        if was_near_low and not near_low and close_i > lower_band:
            bounce_count += 1
        was_near_low = near_low
    return {
        "box_high": box_high,
        "box_low": box_low,
        "box_width_pct": width_pct,
        "box_position_pct": position_pct,
        "box_days": len(rows),
        "bounce_count": bounce_count,
        "avg_close": mean(closes),
    }


def _derived(row: dict) -> dict:
    close = _to_float(row.get("close"))
    atr14 = _to_float(row.get("atr14"))
    turnover = _to_float(row.get("turnover_value"))
    volume = _to_float(row.get("volume"))
    if turnover is None and close is not None and volume is not None:
        turnover = close * volume
    return {
        "close": close,
        "turnover_value": turnover,
        "atr_pct": atr14 / close * 100.0 if atr14 is not None and close else None,
    }


def _watch_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons: list[str] = []
    d = _derived(row)
    close = d["close"]
    if close is None or close < cfg["min_price"]:
        reasons.append("price_below_min")
    if d["turnover_value"] is None or d["turnover_value"] < cfg["min_turnover_value"]:
        reasons.append("turnover_below_min")
    ma75 = _to_float(row.get("ma75"))
    if ma75 is None or close is None or close <= ma75:
        reasons.append("below_ma75")
    width = metrics["box_width_pct"]
    if not (cfg["watch_box_width_min_pct"] <= width <= cfg["watch_box_width_max_pct"]):
        reasons.append("watch_box_width_out_of_range")
    if metrics["bounce_count"] < cfg["watch_min_bounce_count"]:
        reasons.append("bounce_count_low")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["watch_rsi_min"] or rsi >= cfg["watch_rsi_hard_max"]:
        reasons.append("watch_rsi_out_of_range")
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["watch_atr_max_pct"]:
        reasons.append("watch_atr_too_high")
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is None or volume_ratio < cfg["watch_volume_ratio_min"]:
        reasons.append("watch_volume_too_low")
    if _to_bool(row.get("is_deficit")):
        reasons.append("deficit")
    per = _to_float(row.get("per"))
    if per is not None and (per <= 0 or per > cfg["max_per"]):
        reasons.append("per_outlier")
    pbr = _to_float(row.get("pbr"))
    if pbr is not None and (pbr <= 0 or pbr > cfg["max_pbr"]):
        reasons.append("pbr_outlier")
    return reasons


def _signal_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons = _watch_rejects(row, metrics, cfg)
    pos = metrics["box_position_pct"]
    if not (0 <= pos <= cfg["signal_box_position_pct"]):
        reasons.append("not_near_box_low")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["signal_rsi_min"] or rsi >= cfg["signal_rsi_hard_max"]:
        reasons.append("signal_rsi_out_of_range")
    d = _derived(row)
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["signal_atr_max_pct"]:
        reasons.append("signal_atr_too_high")
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is None or volume_ratio < cfg["signal_volume_ratio_min"]:
        reasons.append("signal_volume_too_low")
    return reasons


def _score(row: dict, metrics: dict, cfg: dict, *, signal: bool) -> tuple[float, list[str], list[str]]:
    reasons = ["長期上昇中", "6か月レンジ継続"]
    warnings: list[str] = []
    score = 0.0
    close = _to_float(row.get("close"), 0.0) or 0.0
    ma75 = _to_float(row.get("ma75"))
    if ma75 and close > ma75:
        score += 25
    if metrics["bounce_count"] >= 3:
        score += 15
        reasons.append("複数回反発")
    elif metrics["bounce_count"] >= 2:
        score += 10
    width = metrics["box_width_pct"]
    if 8 <= width <= 18:
        score += 15
    elif cfg["watch_box_width_min_pct"] <= width <= cfg["watch_box_width_max_pct"]:
        score += 10

    pos = metrics["box_position_pct"]
    if signal:
        if 0 <= pos <= cfg["signal_strong_position_pct"]:
            score += 30
            reasons.append("box下限強接近")
        elif 0 <= pos <= cfg["signal_box_position_pct"]:
            score += 22
            reasons.append("box下限接近")
    else:
        if 0 <= pos <= 35:
            score += 20
            reasons.append("下限圏")
        elif 35 < pos <= 70:
            score += 12
            reasons.append("レンジ中央")
        else:
            score += 6
            warnings.append("現在は上限寄り")

    rsi = _to_float(row.get("rsi14"))
    if rsi is not None:
        if 40 <= rsi <= 50:
            score += 15
            reasons.append("RSI冷却")
        elif cfg["signal_rsi_min"] <= rsi <= cfg["signal_rsi_cool_max"]:
            score += 10
            reasons.append("RSI冷却")
        elif cfg["signal_rsi_cool_max"] < rsi < cfg["signal_rsi_hard_max"]:
            score += 4
            warnings.append("RSIやや高め")

    turnover = _derived(row)["turnover_value"]
    if turnover and turnover >= cfg["min_turnover_value"] * 2:
        score += 15
    elif turnover and turnover >= cfg["min_turnover_value"]:
        score += 11
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is not None and volume_ratio > cfg["volume_ratio_warning_max"]:
        warnings.append("出来高急増")

    fund = 0.0
    if not _to_bool(row.get("is_deficit")):
        fund += 5
    per = _to_float(row.get("per"))
    pbr = _to_float(row.get("pbr"))
    if per is not None and 0 < per <= cfg["max_per"]:
        fund += 5
    elif per is None:
        warnings.append("PER未取得")
    if pbr is not None and 0 < pbr <= cfg["max_pbr"]:
        fund += 5
    elif pbr is None:
        warnings.append("PBR未取得")
    score += fund
    return min(score, 100.0), reasons, warnings


def _base_payload(row: dict, metrics: dict, cfg: dict, short_regime: str | None, long_regime: str | None) -> dict:
    d = _derived(row)
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
        "raw": {
            "snapshot_id": row.get("id"),
            "short_market_regime": short_regime,
            "long_market_regime": long_regime,
            "ma75": row.get("ma75"),
            "dividend_yield_pct": row.get("dividend_yield_pct"),
            "roe": row.get("roe"),
            "filter": "large_cap_box_range_v1",
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
            "entry_mode": "box_pullback",
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
    logger.info("[box_lab] trade_date=%s latest_rows=%d dry_run=%s", trade_date, len(latest_rows), args.dry_run)

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
    for row in signal_rows[:20]:
        logger.info(
            "[box_signals] code=%s name=%s score=%.1f pos=%.1f entry=%.0f-%.0f",
            row.get("code"),
            row.get("name"),
            row.get("box_score") or 0,
            row.get("box_position_pct") or 0,
            row.get("entry_price_min") or 0,
            row.get("entry_price_max") or 0,
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
