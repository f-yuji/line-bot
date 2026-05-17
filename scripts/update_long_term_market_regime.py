#!/usr/bin/env python3
"""Update long_term_market_regime from index structure and market breadth."""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.long_term_market_regime import upsert_long_term_market_regime

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


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    row = upsert_long_term_market_regime(sb, trade_date=args.date, dry_run=args.dry_run)
    logger.info(
        "complete long_term_market_regime date=%s regime=%s score=%s dry_run=%s",
        row.get("trade_date"),
        row.get("regime"),
        row.get("score"),
        args.dry_run,
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update long-term market regime")
    p.add_argument("--date", help="YYYY-MM-DD. Defaults to latest stock_feature_snapshots date.")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
