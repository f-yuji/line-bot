"""TSE Prime stock list management with J-Quants first, cache/fallback second."""
import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from jquants_client import get_listed_info, normalize_code

load_dotenv()
logger = logging.getLogger(__name__)
CACHE_TTL_DAYS = 7

EXCLUDE_PRODUCT_WORDS = [
    "ETF", "ETN", "REIT", "リート", "投資法人", "インフラファンド", "外国", "優先",
    "出資証券", "受益証券",
]


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


def _is_prime(row: dict[str, Any]) -> bool:
    text = " ".join(str(row.get(k) or "") for k in ("MarketCodeName", "MarketName", "MarketSegment", "MktNm", "Mkt"))
    return "プライム" in text or "Prime" in text


def _is_excluded_product(row: dict[str, Any]) -> bool:
    text = " ".join(str(row.get(k) or "") for k in (
        "CompanyName", "IssueName", "CoName", "Sector17CodeName", "Sector33CodeName", "S17Nm", "S33Nm", "MarketCodeName", "MktNm",
    ))
    return any(word in text for word in EXCLUDE_PRODUCT_WORDS)


def _row_to_stock(row: dict[str, Any]) -> dict | None:
    if not _is_prime(row) or _is_excluded_product(row):
        return None
    code = normalize_code(row.get("Code"))
    if not code or not code[:4].isdigit() or len(code) != 4:
        return None
    return {
        "code": code,
        "name": row.get("CompanyName") or row.get("IssueName") or row.get("CoName") or "",
        "sector": row.get("Sector17CodeName") or row.get("Sector33CodeName") or row.get("S17Nm") or row.get("S33Nm") or "",
        "sector17": row.get("Sector17CodeName") or row.get("S17Nm") or "",
        "sector33": row.get("Sector33CodeName") or row.get("S33Nm") or "",
        "market": "prime",
    }


def fetch_prime_from_jquants(target_date: str | None = None) -> list[dict]:
    rows = get_listed_info(date=target_date)
    logger.info("fetched listed/info rows=%d", len(rows))
    stocks: dict[str, dict] = {}
    skipped = 0
    for row in rows:
        stock = _row_to_stock(row)
        if not stock:
            skipped += 1
            continue
        if stock["code"] not in stocks:
            stocks[stock["code"]] = stock
    result = sorted(stocks.values(), key=lambda r: r["code"])
    logger.info("prime stocks rows=%d normalized codes=%d skipped etf/reit/etc=%d", len(result), len(result), skipped)
    return result


def _save_to_supabase(supabase, records: list[dict], *, dry_run: bool = False) -> None:
    if not records:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {"code": r["code"], "name": r["name"], "sector": r.get("sector", ""), "updated_at": now}
        for r in records
        if r.get("code")
    ]
    if dry_run:
        logger.info("DRYRUN upsert prime_stocks_cache rows=%d", len(rows))
        return
    for i in range(0, len(rows), 1000):
        supabase.table("prime_stocks_cache").upsert(rows[i:i + 1000], on_conflict="code").execute()
    logger.info("upsert prime_stocks_cache rows=%d", len(rows))


def _load_from_supabase(supabase, *, allow_stale: bool = False) -> list[dict] | None:
    try:
        rows = []
        offset = 0
        while True:
            data = (
                supabase.table("prime_stocks_cache")
                .select("code, name, sector, updated_at")
                .order("code")
                .range(offset, offset + 999)
                .execute()
                .data or []
            )
            rows.extend(data)
            if len(data) < 1000:
                break
            offset += 1000
        if not rows:
            return None
        latest = max(r.get("updated_at", "") for r in rows)
        records = [{"code": r["code"], "name": r.get("name", ""), "sector": r.get("sector", "")} for r in rows]
        if allow_stale:
            logger.warning("prime_stocks_cache stale fallback rows=%d latest=%s", len(records), latest)
            return records
        if latest:
            dt = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - dt < timedelta(days=CACHE_TTL_DAYS):
                return records
    except Exception as e:
        logger.warning("prime_stocks_cache load failed: %s", e)
    return None


def get_prime_tickers(supabase=None, *, force_refresh: bool = False) -> list[dict]:
    if supabase and not force_refresh:
        cached = _load_from_supabase(supabase)
        if cached:
            logger.info("prime_stocks_cache rows=%d", len(cached))
            return cached
    try:
        stocks = fetch_prime_from_jquants()
        if stocks:
            if supabase:
                _save_to_supabase(supabase, stocks)
            return stocks
    except Exception as e:
        logger.warning("J-Quants listed/info failed; fallback used: %s", e)
        if supabase:
            cached = _load_from_supabase(supabase, allow_stale=True)
            if cached:
                return cached

    logger.warning("prime stock fetch failed; Nikkei225 fallback used")
    from nikkei_alert import NIKKEI225
    return [{"code": k, "name": v, "sector": ""} for k, v in NIKKEI225.items()]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Refresh TSE Prime stock cache")
    p.add_argument("--refresh-jquants", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--date")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _parse_args()
    if not args.refresh_jquants:
        print("usage: python prime_stocks.py --refresh-jquants [--dry-run]")
        return
    sb = _build_supabase()
    stocks = fetch_prime_from_jquants(args.date)
    _save_to_supabase(sb, stocks, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
