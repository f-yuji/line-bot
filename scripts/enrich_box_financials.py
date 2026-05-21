#!/usr/bin/env python3
"""Backfill display-only valuation fields into box_lab rows from nikkei_financials."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


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


def _pick(row: dict, *keys: str):
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _financial_payload(fin: dict) -> dict:
    return {
        "per": _pick(fin, "per", "PER"),
        "pbr": _pick(fin, "pbr", "PBR"),
        "equity_ratio": _pick(fin, "equity_ratio", "equity_ratio_pct", "self_capital_ratio"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _update_table(sb, table: str, financials: dict[str, dict], *, dry_run: bool, limit: int | None) -> int:
    rows = _fetch_all(lambda: sb.table(table).select("id,code,per,pbr,equity_ratio").order("trade_date", desc=True))
    updated = 0
    for row in rows:
        if limit and updated >= limit:
            break
        code = str(row.get("code") or "")
        fin = financials.get(code)
        if not fin:
            continue
        payload = _financial_payload(fin)
        payload = {k: v for k, v in payload.items() if v not in (None, "")}
        if not any(k in payload for k in ("per", "pbr", "equity_ratio")):
            continue
        needs_update = any(row.get(k) in (None, "") and payload.get(k) not in (None, "") for k in ("per", "pbr", "equity_ratio"))
        if not needs_update:
            continue
        updated += 1
        logger.info("[%s] %s per=%s pbr=%s equity=%s", table, code, payload.get("per"), payload.get("pbr"), payload.get("equity_ratio"))
        if not dry_run:
            sb.table(table).update(payload).eq("id", row["id"]).execute()
    return updated


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    fin_rows = _fetch_all(lambda: sb.table("nikkei_financials").select("*"))
    financials = {str(row.get("code")): row for row in fin_rows if row.get("code")}
    logger.info("financial cache rows=%d dry_run=%s", len(financials), args.dry_run)
    for table in ("box_watchlist", "box_signals"):
        try:
            count = _update_table(sb, table, financials, dry_run=bool(args.dry_run), limit=args.limit)
            logger.info("[%s] rows_to_update=%d", table, count)
        except Exception as e:
            logger.warning("[%s] enrich failed: %s", table, e)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich box_lab rows with PER/PBR/equity ratio from nikkei_financials")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
