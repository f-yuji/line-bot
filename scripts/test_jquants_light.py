#!/usr/bin/env python3
"""Smoke test J-Quants Light access without writing to DB."""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from jquants_client import get_daily_quotes, get_id_token, get_listed_info, normalize_code
from prime_stocks import fetch_prime_from_jquants

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def main() -> int:
    token = get_id_token()
    logger.info("id token ok len=%d", len(token))

    listed = get_listed_info()
    logger.info("listed info rows=%d", len(listed))

    prime = fetch_prime_from_jquants()
    logger.info("prime stocks rows=%d sample=%s", len(prime), prime[:3])

    end = datetime.now(JST).date()
    start = end - timedelta(days=365)
    quotes = get_daily_quotes(code="7203", from_date=start, to_date=end)
    logger.info("daily quotes 7203 rows=%d sample_code=%s", len(quotes), normalize_code((quotes[0] if quotes else {}).get("Code")))

    logger.info("pagination ok")
    logger.info("complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
