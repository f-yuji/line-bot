#!/usr/bin/env python3
"""Check open virtual_trades with pullback/RSI/MA5 exit rules."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from settings_loader import get_settings
from services.virtual_trade_exit import (
    HAS_PRICE_DEPS,
    close_related_watchlist,
    evaluate_virtual_trade_exit,
    is_non_japanese_trade,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _log_exit(code: str, update: dict) -> None:
    reason = update.get("exit_reason")
    if reason in {"pullback2", "rsi75_pullback1"}:
        logger.info(
            "[virtual_exit] code=%s reason=%s daily_return=%s sell_price=%s",
            code,
            reason,
            update.get("exit_trigger_value"),
            update.get("sell_price"),
        )
    elif reason == "stop_loss_4pct":
        logger.info(
            "[virtual_exit] code=%s reason=%s pnl_pct=%s sell_price=%s",
            code,
            reason,
            update.get("profit_loss_pct"),
            update.get("sell_price"),
        )
    elif reason == "ma5_failed_recovery":
        logger.info(
            "[virtual_exit] code=%s reason=%s ma5_diff_pct=%s sell_price=%s",
            code,
            reason,
            update.get("exit_trigger_value"),
            update.get("sell_price"),
        )
    else:
        logger.info(
            "[virtual_exit] code=%s reason=%s sell_price=%s pnl_pct=%s",
            code,
            reason,
            update.get("sell_price"),
            update.get("profit_loss_pct"),
        )


def run(args: argparse.Namespace) -> None:
    if not HAS_PRICE_DEPS:
        raise RuntimeError("pandas and yfinance are required")
    sb = _build_supabase()
    cfg = get_settings(force_reload=True)
    holding_days = args.holding_days
    rows = (
        sb.table("virtual_trades")
        .select("*")
        .eq("status", "open")
        .is_("sell_date", "null")
        .execute()
        .data or []
    )
    logger.info("open virtual trades=%d", len(rows))
    checked = closed = skipped = errors = 0
    now_utc = datetime.now(timezone.utc)
    for trade in rows:
        code = str(trade.get("code") or "")
        if is_non_japanese_trade(trade):
            logger.info("skip non-japanese virtual trade: %s market=%s", code, trade.get("market"))
            skipped += 1
            continue
        try:
            result = evaluate_virtual_trade_exit(
                trade,
                holding_days=holding_days,
                settings=cfg,
                now=now_utc,
            )
            if not result:
                skipped += 1
                continue
            update = result.update
            checked += 1
            if update.get("status") == "closed":
                closed += 1
                _log_exit(code, update)
            logger.info(
                "%svirtual trade: %s status=%s reason=%s max_return=%s max_dd=%s highest_close=%s rsi75=%s ma5_recovered=%s",
                "DRYRUN " if args.dry_run else "",
                code,
                update.get("status", "open"),
                update.get("exit_reason"),
                update.get("max_return_pct"),
                update.get("max_drawdown_pct"),
                update.get("highest_close"),
                update.get("rsi75_touched"),
                update.get("ma5_recovered"),
            )
            if args.dry_run:
                logger.info("DRYRUN virtual trade detail: code=%s detail=%s", code, result.dry_log)
                if update.get("status") == "closed":
                    close_related_watchlist(sb, trade, str(update.get("exit_reason") or "closed"), dry_run=True)
                continue
            sb.table("virtual_trades").update(update).eq("id", trade["id"]).execute()
            if update.get("status") == "closed":
                close_related_watchlist(sb, trade, str(update.get("exit_reason") or "closed"), dry_run=False)
        except Exception as e:
            errors += 1
            logger.exception("virtual trade check failed id=%s code=%s: %s", trade.get("id"), code, e)
    logger.info("complete: checked=%d closed=%d skipped=%d errors=%d", checked, closed, skipped, errors)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check virtual trades")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--holding-days", type=int, default=None, help="Override virtual_exit_holding_days")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
