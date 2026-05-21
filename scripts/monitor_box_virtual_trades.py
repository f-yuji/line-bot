#!/usr/bin/env python3
"""Monitor open box_lab virtual trades and close by ma25_stop_box_tp."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.box_signal_logic import _to_float

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MISSING_COLUMN_RE = re.compile(r"Could not find the '([^']+)' column")


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper() if mode else ""
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
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


def _chunked(values: list[str], size: int = 80):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _latest_trade_date(sb, trade_date: str | None) -> str:
    if trade_date:
        return trade_date
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        raise RuntimeError("stock_feature_snapshots is empty")
    return str(rows[0]["trade_date"])


def _load_latest_snapshots(sb, trade_date: str, codes: list[str]) -> dict[str, dict]:
    by_code: dict[str, dict] = {}
    for chunk in _chunked(codes):
        rows = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .eq("trade_date", trade_date)
            .in_("code", chunk)
            .execute()
            .data
            or []
        )
        for row in rows:
            by_code[str(row.get("code"))] = row
    return by_code


def _load_trade_dates(sb, start_date: str, end_date: str) -> list[str]:
    rows = _fetch_all(
        lambda: (
            sb.table("stock_feature_snapshots")
            .select("trade_date")
            .gte("trade_date", start_date)
            .lte("trade_date", end_date)
            .order("trade_date")
        )
    )
    return sorted({str(r["trade_date"]) for r in rows if r.get("trade_date")})


def _remove_missing_column(payload: dict, exc: Exception) -> bool:
    match = MISSING_COLUMN_RE.search(str(exc))
    if not match:
        return False
    col = match.group(1)
    if col not in payload:
        return False
    payload.pop(col, None)
    logger.warning("[box_monitor] optional column missing; omitted column=%s", col)
    return True


def _update_optional(sb, table: str, payload: dict, *, row_id: str, dry_run: bool) -> None:
    if dry_run:
        return
    remaining = dict(payload)
    for _ in range(40):
        try:
            sb.table(table).update(remaining).eq("id", row_id).execute()
            return
        except Exception as e:
            if _remove_missing_column(remaining, e):
                continue
            raise
    raise RuntimeError(f"too many optional column retries for {table}")


def _parse_date(value) -> date | None:
    if not value:
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _holding_days(trade: dict, trade_dates: list[str], target_date: str) -> int:
    buy = str(trade.get("buy_date") or "")[:10]
    if buy:
        dates = [d for d in trade_dates if buy <= d <= target_date]
        if dates:
            return len(dates)
    buy_date = _parse_date(trade.get("buy_date"))
    target = _parse_date(target_date)
    if buy_date and target:
        return max((target - buy_date).days + 1, 1)
    return 1


def _evaluate_exit(trade: dict, snap: dict, holding_days: int, max_holding_days: int) -> tuple[str | None, float | None, str]:
    day_high = _to_float(snap.get("high"))
    day_low = _to_float(snap.get("low"))
    close = _to_float(snap.get("close"))
    ma25 = _to_float(snap.get("ma25"))
    take_profit = _to_float(trade.get("take_profit_price") or trade.get("box_high") or trade.get("box_upper"))
    stop_loss = _to_float(trade.get("stop_loss_price"))

    # Conservative priority: loss-side exits before take profit when both hit.
    if stop_loss is not None and day_low is not None and day_low <= stop_loss:
        return "stop_loss", stop_loss, "day_low <= stop_loss_price"
    if ma25 is not None and close is not None and close < ma25:
        return "ma25_break", close, "close < ma25"
    if take_profit is not None and day_high is not None and day_high >= take_profit:
        return "box_take_profit", take_profit, "day_high >= take_profit_price"
    if holding_days > max_holding_days and close is not None:
        return "max_holding_days", close, "holding_days > max_holding_days"
    return None, None, "hold"


def _pnl(trade: dict, price: float) -> tuple[float, float]:
    buy = _to_float(trade.get("buy_price")) or 0.0
    qty = int(float(trade.get("quantity") or 100))
    pnl = (price - buy) * qty
    pct = (price / buy - 1.0) * 100.0 if buy > 0 else 0.0
    return pnl, pct


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    target_date = _latest_trade_date(sb, args.trade_date)
    rows = (
        sb.table("box_virtual_trades")
        .select("*")
        .eq("status", "open")
        .order("buy_date")
        .limit(args.limit)
        .execute()
        .data
        or []
    )
    codes = sorted({str(r.get("code")) for r in rows if r.get("code")})
    snapshots = _load_latest_snapshots(sb, target_date, codes) if codes else {}
    min_buy = min((str(r.get("buy_date") or "")[:10] for r in rows if r.get("buy_date")), default=target_date)
    trade_dates = _load_trade_dates(sb, min_buy, target_date) if rows else []

    logger.info(
        "[box_monitor] target_date=%s open=%d max_holding_days=%d",
        target_date,
        len(rows),
        args.max_holding_days,
    )

    updated = closed = skipped = 0
    exit_counts: Counter[str] = Counter()
    total_unrealized = 0.0
    total_realized = 0.0
    now_iso = datetime.now(timezone.utc).isoformat()

    for trade in rows:
        tid = str(trade.get("id"))
        code = str(trade.get("code") or "")
        snap = snapshots.get(code)
        if not snap:
            skipped += 1
            logger.info("[box_monitor] skip code=%s reason=snapshot_missing", code)
            continue

        close = _to_float(snap.get("close"))
        if close is None:
            skipped += 1
            logger.info("[box_monitor] skip code=%s reason=close_missing", code)
            continue

        hold_days = _holding_days(trade, trade_dates, target_date)
        unrealized_pnl, unrealized_pct = _pnl(trade, close)
        total_unrealized += unrealized_pnl
        update = {
            "current_price": close,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pct,
            "holding_days": hold_days,
            "updated_at": now_iso,
        }

        reason, exit_price, detail = _evaluate_exit(trade, snap, hold_days, args.max_holding_days)
        if reason and exit_price is not None:
            pnl, pnl_pct = _pnl(trade, float(exit_price))
            total_realized += pnl
            exit_counts[reason] += 1
            closed += 1
            update.update(
                {
                    "status": "closed",
                    "sell_date": target_date,
                    "exit_date": target_date,
                    "sell_price": float(exit_price),
                    "exit_price": float(exit_price),
                    "exit_reason": reason,
                    "profit_loss": pnl,
                    "profit_loss_pct": pnl_pct,
                    "profit_pct": pnl_pct,
                    "unrealized_pnl": 0,
                    "unrealized_pnl_pct": 0,
                }
            )
            logger.info(
                "[box_exit] code=%s reason=%s price=%.2f pnl_pct=%.2f detail=%s",
                code,
                reason,
                float(exit_price),
                pnl_pct,
                detail,
            )
        else:
            updated += 1
            logger.info(
                "[box_monitor] hold code=%s current=%.2f unrealized_pct=%.2f holding_days=%d",
                code,
                close,
                unrealized_pct,
                hold_days,
            )

        _update_optional(sb, "box_virtual_trades", update, row_id=tid, dry_run=args.dry_run)

    logger.info(
        "[box_monitor] complete dry_run=%s updated=%d closed=%d skipped=%d exit_reasons=%s total_unrealized=%.0f total_realized=%.0f",
        args.dry_run,
        updated,
        closed,
        skipped,
        dict(sorted(exit_counts.items())),
        total_unrealized,
        total_realized,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor box_lab virtual trades")
    parser.add_argument("--trade-date", default=None, help="Monitoring trade_date. Defaults to latest snapshot date.")
    parser.add_argument("--max-holding-days", type=int, default=20)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
