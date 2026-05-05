#!/usr/bin/env python3
"""Import basic J-Quants statements into the existing nikkei_financials table."""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from jquants_client import get_statements, normalize_code

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


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


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _first(row: dict, keys: list[str]) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _build_financial_row(statement: dict) -> dict | None:
    code = normalize_code(statement.get("LocalCode") or statement.get("Code"))
    if not code:
        return None
    profit = _to_float(_first(statement, [
        "Profit", "ProfitLoss", "ProfitLossAttributableToOwnersOfParent", "NP",
        "NetIncome", "NetIncomeLoss",
    ]))
    operating_profit = _to_float(_first(statement, ["OperatingProfit", "OperatingIncome", "OP"]))
    dividend = _to_float(_first(statement, ["DividendPerShare", "AnnualDividendPerShare", "ResultDividendPerShareAnnual", "DPS", "DEPS"]))
    return {
        "code": code,
        "is_deficit": profit is not None and profit < 0,
        "dividend_per_share": dividend,
        "operating_profit": operating_profit,
        "net_income": profit,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _existing_columns(sb) -> set[str]:
    try:
        rows = sb.table("nikkei_financials").select("*").limit(1).execute().data or []
        if rows:
            return set(rows[0].keys())
    except Exception as e:
        logger.warning("nikkei_financials column probe failed: %s", e)
    return {"code", "is_deficit", "dividend_per_share", "updated_at"}


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    end = args.end or datetime.now(JST).date().isoformat()
    start = args.start or (datetime.fromisoformat(end).date() - timedelta(days=365)).isoformat()
    try:
        rows = get_statements(code=args.code, from_date=None if args.code else start, to_date=None if args.code else end)
    except Exception as e:
        logger.warning("J-Quants statements fetch failed; no DB changes: %s", e)
        rows = []
    logger.info("J-Quants statements rows=%d", len(rows))
    latest: dict[str, dict] = {}
    for st in rows:
        row = _build_financial_row(st)
        if row:
            latest[row["code"]] = row
    out = list(latest.values())
    if args.limit:
        out = out[: int(args.limit)]
    logger.info("financial rows prepared=%d", len(out))
    if args.dry_run:
        logger.info("DRYRUN sample=%s", out[:3])
        return
    cols = _existing_columns(sb)
    filtered = [{k: v for k, v in row.items() if k in cols} for row in out]
    for i in range(0, len(filtered), int(args.batch_size)):
        sb.table("nikkei_financials").upsert(filtered[i:i + int(args.batch_size)], on_conflict="code").execute()
    logger.info("upsert nikkei_financials rows=%d", len(filtered))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import J-Quants statements")
    p.add_argument("--code")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch-size", type=int, default=200)
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
