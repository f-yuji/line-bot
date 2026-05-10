#!/usr/bin/env python3
"""Import J-Quants Standard margin and short-selling data.

This stores source data for later analysis. It does not change predictions,
virtual_trades, signal stages, or active models.
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from jquants_client import (
    get_daily_margin_interest,
    get_short_selling,
    get_weekly_margin_interest,
    normalize_code,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("postgrest").setLevel(logging.WARNING)

JST = timezone(timedelta(hours=9))
DEFAULT_BATCH_SIZE = 500


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


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(args: argparse.Namespace) -> tuple[date, date]:
    end = _parse_date(args.end) or datetime.now(JST).date()
    start = _parse_date(args.start) if args.start else end - timedelta(days=365 * int(args.years or 1))
    if args.start_after_date:
        after = _parse_date(args.start_after_date)
        if after and after >= start:
            start = after + timedelta(days=1)
    return start, end


def _to_num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in {"", "-", "*"}:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_date(value: Any) -> str | None:
    if not value:
        return None
    try:
        if hasattr(value, "date") and not isinstance(value, (str, date)):
            return value.date().isoformat()
        return datetime.fromisoformat(str(value).replace("/", "-")).date().isoformat()
    except Exception:
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _first(row: dict, keys: list[str]) -> Any:
    for key in keys:
        if row.get(key) is not None:
            return row.get(key)
    return None


def _load_codes(sb, args: argparse.Namespace) -> list[str]:
    if args.code:
        return [normalize_code(args.code)]
    rows = (
        sb.table("prime_stocks_cache")
        .select("code")
        .order("code")
        .execute()
        .data or []
    )
    codes = sorted({normalize_code(r.get("code")) for r in rows if r.get("code")})
    if args.code_from:
        codes = [c for c in codes if c >= normalize_code(args.code_from)]
    if args.code_to:
        codes = [c for c in codes if c <= normalize_code(args.code_to)]
    if args.start_after_code:
        codes = [c for c in codes if c > normalize_code(args.start_after_code)]
    if args.limit:
        codes = codes[: int(args.limit)]
    return codes


def _upsert(sb, table: str, rows: list[dict], conflict: str, batch_size: int) -> int:
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        sb.table(table).upsert(batch, on_conflict=conflict).execute()
        total += len(batch)
    return total


def _iter_dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _fetch_with_retry(label: str, fn, *, retries: int = 6, base_sleep: float = 10.0):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            if "429" not in msg and "Max retries exceeded" not in msg and "too many" not in msg:
                raise
            wait = base_sleep * attempt
            logger.warning("[%s] rate limited attempt=%d/%d sleep=%.1fs error=%s", label, attempt, retries, wait, msg[:180])
            time.sleep(wait)
    return fn()


def _weekly_row(row: dict) -> dict | None:
    code = normalize_code(row.get("Code") or row.get("code"))
    d = _to_date(row.get("Date") or row.get("date"))
    if not code or not d:
        return None
    short_out = _to_num(_first(row, ["ShortMarginOutstanding", "ShortMarginBalance", "ShortOutstanding", "ShrtVol"]))
    long_out = _to_num(_first(row, ["LongMarginOutstanding", "LongMarginBalance", "LongOutstanding", "LongVol"]))
    ratio = (long_out / short_out) if short_out and long_out is not None else None
    return {
        "code": code,
        "date": d,
        "published_date": _to_date(row.get("PublishedDate") or row.get("published_date")),
        "short_margin_outstanding": short_out,
        "long_margin_outstanding": long_out,
        "margin_ratio": ratio,
        "short_margin_change": _to_num(_first(row, ["ChangeShortMarginOutstanding", "WeeklyChangeShortMarginOutstanding", "DailyChangeShortMarginOutstanding"])),
        "long_margin_change": _to_num(_first(row, ["ChangeLongMarginOutstanding", "WeeklyChangeLongMarginOutstanding", "DailyChangeLongMarginOutstanding"])),
        "short_margin_listed_share_ratio": _to_num(_first(row, ["ShortMarginOutstandingListedShareRatio"])),
        "long_margin_listed_share_ratio": _to_num(_first(row, ["LongMarginOutstandingListedShareRatio"])),
        "raw": _json_safe(row),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _daily_margin_row(row: dict) -> dict | None:
    code = normalize_code(row.get("Code") or row.get("code"))
    app_date = _to_date(row.get("ApplicationDate") or row.get("AppDate") or row.get("Date") or row.get("date"))
    if not code or not app_date:
        return None
    short_out = _to_num(_first(row, ["ShortMarginOutstanding", "ShortMarginBalance", "ShortOutstanding", "ShrtOut"]))
    long_out = _to_num(_first(row, ["LongMarginOutstanding", "LongMarginBalance", "LongOutstanding", "LongOut"]))
    ratio = _to_num(_first(row, ["ShortLongRatio", "SLRatio"]))
    if ratio is None:
        ratio = (short_out / long_out) if long_out and short_out is not None else None
    return {
        "code": code,
        "application_date": app_date,
        "published_date": _to_date(row.get("PublishedDate") or row.get("PubDate")),
        "short_margin_outstanding": short_out,
        "long_margin_outstanding": long_out,
        "margin_ratio": ratio,
        "short_margin_change": _to_num(_first(row, ["DailyChangeShortMarginOutstanding", "ShrtOutChg"])),
        "long_margin_change": _to_num(_first(row, ["DailyChangeLongMarginOutstanding", "LongOutChg"])),
        "short_margin_listed_share_ratio": _to_num(_first(row, ["ShortMarginOutstandingListedShareRatio", "ShrtOutRatio"])),
        "long_margin_listed_share_ratio": _to_num(_first(row, ["LongMarginOutstandingListedShareRatio", "LongOutRatio"])),
        "publish_reason": _json_safe(row.get("PublishReason") or row.get("PubReason")),
        "raw": _json_safe(row),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _short_selling_row(row: dict) -> dict | None:
    d = _to_date(row.get("Date") or row.get("date"))
    sector = str(row.get("Sector33Code") or row.get("S33") or row.get("sector33code") or "").strip()
    if not d or not sector:
        return None
    long_value = _to_num(_first(row, ["SellingExcludingShortSellingTurnoverValue", "SellExShortVa"])) or 0
    restricted = _to_num(_first(row, ["ShortSellingWithRestrictionsTurnoverValue", "ShrtWithResVa"])) or 0
    unrestricted = _to_num(_first(row, ["ShortSellingWithoutRestrictionsTurnoverValue", "ShrtNoResVa"])) or 0
    short_total = restricted + unrestricted
    total = long_value + short_total
    ratio = (short_total / total * 100.0) if total else None
    return {
        "date": d,
        "sector33_code": sector,
        "selling_excluding_short_value": long_value,
        "short_selling_with_restrictions_value": restricted,
        "short_selling_without_restrictions_value": unrestricted,
        "total_selling_value": total,
        "total_short_selling_value": short_total,
        "short_selling_ratio": ratio,
        "raw": _json_safe(row),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def import_weekly_margin(sb, args: argparse.Namespace) -> int:
    start, end = _date_range(args)
    codes = _load_codes(sb, args)
    logger.info("[margin_weekly] codes=%d start=%s end=%s dry_run=%s", len(codes), start, end, args.dry_run)
    saved = 0
    for idx, code in enumerate(codes, 1):
        rows = get_weekly_margin_interest(code=code, from_date=start, to_date=end)
        mapped = [r for r in (_weekly_row(row) for row in rows) if r]
        if args.dry_run:
            logger.info("[margin_weekly] DRYRUN code=%s rows=%d sample=%s", code, len(mapped), mapped[:1])
        elif mapped:
            saved += _upsert(sb, "stock_weekly_margin_interest", mapped, "code,date", int(args.batch_size))
        if idx % max(1, int(args.progress_every)) == 0:
            logger.info("[margin_weekly] progress codes=%d/%d saved=%d last=%s", idx, len(codes), saved, code)
        time.sleep(float(args.sleep_sec or 0))
    return saved


def import_daily_margin(sb, args: argparse.Namespace) -> int:
    start, end = _date_range(args)
    logger.info("[margin_daily] start=%s end=%s dry_run=%s", start, end, args.dry_run)
    saved = 0
    total_rows = 0
    for idx, d in enumerate(_iter_dates(start, end), 1):
        rows = _fetch_with_retry("margin_daily", lambda: get_daily_margin_interest(date=d))
        mapped = [r for r in (_daily_margin_row(row) for row in rows) if r]
        total_rows += len(mapped)
        if args.dry_run and mapped:
            logger.info("[margin_daily] DRYRUN date=%s rows=%d sample=%s", d, len(mapped), mapped[:1])
        elif mapped:
            saved += _upsert(sb, "stock_daily_margin_interest", mapped, "code,application_date,published_date", int(args.batch_size))
        if idx % max(1, int(args.progress_every)) == 0:
            logger.info("[margin_daily] progress days=%d date=%s rows=%d saved=%d", idx, d, total_rows, saved)
        time.sleep(float(args.sleep_sec or 0))
    return saved


def import_short_selling(sb, args: argparse.Namespace) -> int:
    start, end = _date_range(args)
    logger.info("[short_selling] start=%s end=%s dry_run=%s", start, end, args.dry_run)
    saved = 0
    total_rows = 0
    for idx, d in enumerate(_iter_dates(start, end), 1):
        rows = _fetch_with_retry("short_selling", lambda: get_short_selling(date=d))
        mapped = [r for r in (_short_selling_row(row) for row in rows) if r]
        total_rows += len(mapped)
        if args.dry_run and mapped:
            logger.info("[short_selling] DRYRUN date=%s rows=%d sample=%s", d, len(mapped), mapped[:1])
        elif mapped:
            saved += _upsert(sb, "sector_short_selling", mapped, "date,sector33_code", int(args.batch_size))
        if idx % max(1, int(args.progress_every)) == 0:
            logger.info("[short_selling] progress days=%d date=%s rows=%d saved=%d", idx, d, total_rows, saved)
        time.sleep(float(args.sleep_sec or 0))
    return saved


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    total = 0
    if args.kind in {"weekly", "all"}:
        total += import_weekly_margin(sb, args)
    if args.kind in {"daily", "all"}:
        total += import_daily_margin(sb, args)
    if args.kind in {"short", "all"}:
        total += import_short_selling(sb, args)
    logger.info("[jquants_standard] complete saved=%d dry_run=%s", total, args.dry_run)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import J-Quants Standard margin/short-selling data")
    parser.add_argument("--kind", choices=["weekly", "daily", "short", "all"], default="weekly")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--years", type=int, default=3)
    parser.add_argument("--code")
    parser.add_argument("--code-from")
    parser.add_argument("--code-to")
    parser.add_argument("--start-after-code")
    parser.add_argument("--start-after-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--sleep-sec", type=float, default=float(os.getenv("JQUANTS_SLEEP_SEC", "0.2") or 0.2))
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
