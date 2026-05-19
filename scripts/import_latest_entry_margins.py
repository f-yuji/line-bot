#!/usr/bin/env python3
"""Import recent weekly margin data for the latest rebound entry candidates.

This is intentionally narrower than a full historical import. It refreshes the
credit balance data needed by the live entry filter before predict_rebound runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from jquants_client import get_weekly_margin_interest, normalize_code
from scripts.import_jquants_margin import _build_supabase, _fetch_with_retry, _upsert, _weekly_row

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def _latest_feature_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return str(rows[0].get("trade_date")) if rows else None


def _candidate_codes(sb, trade_date: str, limit: int) -> list[str]:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("code")
        .eq("trade_date", trade_date)
        .eq("is_drop_candidate", True)
        .eq("is_tradeable", True)
        .order("drop_pct")
        .limit(limit)
        .execute()
        .data or []
    )
    return sorted({normalize_code(r.get("code")) for r in rows if r.get("code")})


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    trade_date = args.trade_date or _latest_feature_date(sb)
    if not trade_date:
        raise RuntimeError("latest stock_feature_snapshots trade_date not found")

    codes = _candidate_codes(sb, trade_date, int(args.limit))
    end_date = datetime.fromisoformat(str(trade_date)[:10]).date()
    fallback_start = end_date - timedelta(days=int(args.lookback_days))

    logger.info(
        "[entry_margin_import] trade_date=%s codes=%d start=%s end=%s dry_run=%s",
        trade_date,
        len(codes),
        fallback_start,
        end_date,
        args.dry_run,
    )

    code_set = set(codes)
    saved = 0
    fetched = 0
    current = fallback_start
    day_count = 0
    while current <= end_date:
        day_count += 1
        rows = _fetch_with_retry(
            "entry_margin_import",
            lambda current=current: get_weekly_margin_interest(date=current),
            retries=int(args.retries),
            base_sleep=float(args.retry_wait_seconds),
        )
        seen_keys: set[tuple] = set()
        mapped = []
        for mapped_row in (_weekly_row(row) for row in rows):
            if not mapped_row or mapped_row.get("code") not in code_set:
                continue
            key = (mapped_row.get("code"), mapped_row.get("date"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            mapped.append(mapped_row)
        fetched += len(mapped)
        if args.dry_run:
            if mapped:
                logger.info("[entry_margin_import] DRYRUN date=%s rows=%d sample=%s", current, len(mapped), mapped[:1])
        elif mapped:
            saved += _upsert(sb, "stock_weekly_margin_interest", mapped, "code,date", int(args.batch_size))
        if day_count % max(1, int(args.progress_every)) == 0:
            logger.info("[entry_margin_import] progress days=%d date=%s fetched=%d saved=%d", day_count, current, fetched, saved)
        current += timedelta(days=1)
        time.sleep(float(args.sleep_sec or 0))

    logger.info("[entry_margin_import] complete codes=%d days=%d fetched=%d saved=%d dry_run=%s", len(codes), day_count, fetched, saved, args.dry_run)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import recent weekly margin data for latest entry candidates")
    parser.add_argument("--trade-date")
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--sleep-sec", type=float, default=float(os.getenv("JQUANTS_SLEEP_SEC", "0.1") or 0.1))
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--retry-wait-seconds", type=float, default=20.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
