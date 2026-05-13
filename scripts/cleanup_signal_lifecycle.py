#!/usr/bin/env python3
"""Repair stock_drop_watchlist lifecycle status from virtual_trades/current rows.

Default is dry-run. Use --apply to update DB.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ACTIVE_STAGES = {"early", "confirmed", "strong_confirmed"}


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


def _fetch_all(q, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        data = q.range(start, start + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            return rows
        start += page_size


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update(sb, row: dict, update: dict, *, apply: bool, reason: str) -> bool:
    if not row.get("id"):
        return False
    logger.info(
        "%s[signal_lifecycle] code=%s watchlist_id=%s status %s -> %s reason=%s",
        "" if apply else "DRYRUN ",
        row.get("code"),
        row.get("id"),
        row.get("status"),
        update.get("status"),
        reason,
    )
    if apply:
        sb.table("stock_drop_watchlist").update(update).eq("id", row["id"]).execute()
    return True


def _find_watchlist_for_trade(sb, trade: dict) -> dict | None:
    base_select = "id,code,status,signal_stage,is_excluded,feature_snapshot_id,virtual_trade_id,updated_at"
    if trade.get("watchlist_id"):
        rows = (
            sb.table("stock_drop_watchlist")
            .select(base_select)
            .eq("id", trade.get("watchlist_id"))
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return rows[0]
    if trade.get("feature_snapshot_id"):
        rows = (
            sb.table("stock_drop_watchlist")
            .select(base_select)
            .eq("feature_snapshot_id", trade.get("feature_snapshot_id"))
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return rows[0]
    if trade.get("code"):
        rows = (
            sb.table("stock_drop_watchlist")
            .select(base_select)
            .eq("code", trade.get("code"))
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return rows[0]
    return None


def _repair_from_virtual_trades(sb, args: argparse.Namespace) -> int:
    q = sb.table("virtual_trades").select("*").order("created_at", desc=True)
    if args.code:
        q = q.eq("code", args.code)
    if args.limit:
        q = q.limit(args.limit)
    trades = _fetch_all(q)
    changed = 0
    now = _now()
    for trade in trades:
        wl = _find_watchlist_for_trade(sb, trade)
        if not wl:
            continue
        if trade.get("status") == "open" and not trade.get("sell_date"):
            if wl.get("status") in {"rebound_signal", "rebound_candidate", "watching"}:
                changed += _update(sb, wl, {
                    "status": "entered",
                    "entered_at": now,
                    "virtual_trade_id": str(trade.get("id")) if trade.get("id") else None,
                    "signal_status_reason": "virtual_trade_open_repair",
                    "updated_at": now,
                }, apply=args.apply, reason="virtual_trade_open_repair")
        elif trade.get("status") == "closed" or trade.get("sell_date"):
            if wl.get("status") in {"rebound_signal", "rebound_candidate", "entered", "watching"}:
                reason = trade.get("exit_reason") or trade.get("sell_reason") or "closed"
                changed += _update(sb, wl, {
                    "status": "closed",
                    "closed_at": now,
                    "close_reason": reason,
                    "signal_status_reason": f"virtual_trade_closed:{reason}",
                    "virtual_trade_id": str(trade.get("id")) if trade.get("id") else wl.get("virtual_trade_id"),
                    "updated_at": now,
                }, apply=args.apply, reason=f"virtual_trade_closed:{reason}")
    return changed


def _repair_watchlist_rows(sb, args: argparse.Namespace) -> int:
    q = (
        sb.table("stock_drop_watchlist")
        .select("id,code,status,signal_stage,is_excluded,drop_detected_at,updated_at")
        .order("updated_at", desc=True)
    )
    if args.code:
        q = q.eq("code", args.code)
    if args.limit:
        q = q.limit(args.limit)
    rows = _fetch_all(q)
    changed = 0
    now = _now()
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(args.days))
    for row in rows:
        status = row.get("status")
        stage = row.get("signal_stage") or "none"
        if row.get("is_excluded") and status != "excluded":
            changed += _update(sb, row, {
                "status": "excluded",
                "closed_at": now,
                "close_reason": "excluded",
                "signal_status_reason": "excluded_repair",
                "updated_at": now,
            }, apply=args.apply, reason="excluded_repair")
            continue
        if stage not in ACTIVE_STAGES and status in {"watching", "rebound_candidate", "rebound_signal"}:
            changed += _update(sb, row, {
                "status": "ai_dropped",
                "closed_at": now,
                "close_reason": "ai_score_below_threshold",
                "signal_status_reason": "ai_score_below_threshold",
                "updated_at": now,
            }, apply=args.apply, reason="ai_score_below_threshold")
            continue
        if stage == "early" and status == "rebound_signal":
            changed += _update(sb, row, {
                "status": "rebound_candidate",
                "signal_status_reason": "early_candidate_repair",
                "updated_at": now,
            }, apply=args.apply, reason="early_candidate_repair")
            continue
        if stage in {"confirmed", "strong_confirmed"} and status == "rebound_candidate":
            changed += _update(sb, row, {
                "status": "rebound_signal",
                "signal_status_reason": "confirmed_signal_repair",
                "updated_at": now,
            }, apply=args.apply, reason="confirmed_signal_repair")
            continue
        if status in {"watching", "rebound_candidate", "rebound_signal", "signal_skipped"}:
            raw_date = row.get("drop_detected_at") or row.get("updated_at")
            try:
                dt = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    changed += _update(sb, row, {
                        "status": "expired",
                        "closed_at": now,
                        "close_reason": "stale_signal",
                        "signal_status_reason": "stale_signal_cleanup",
                        "updated_at": now,
                    }, apply=args.apply, reason="stale_signal_cleanup")
            except Exception:
                continue
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair signal lifecycle statuses")
    parser.add_argument("--apply", action="store_true", help="actually update DB")
    parser.add_argument("--dry-run", action="store_true", help="dry-run alias; default")
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--code")
    args = parser.parse_args()
    if args.dry_run:
        args.apply = False

    sb = _build_supabase()
    changed = _repair_from_virtual_trades(sb, args)
    changed += _repair_watchlist_rows(sb, args)
    logger.info("complete: mode=%s changes=%d", "apply" if args.apply else "dry-run", changed)


if __name__ == "__main__":
    main()
