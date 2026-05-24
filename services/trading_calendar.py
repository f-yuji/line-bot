"""Trading-day helpers for cron and display code.

The app treats dates present in stock_feature_snapshots as the source of truth
for exchange business days. For preflight checks where DB data is not enough,
weekends are considered non-trading days.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

JST = timezone(timedelta(hours=9))


def today_jst() -> date:
    return datetime.now(JST).date()


def is_weekend(day: date | str | None = None) -> bool:
    d = parse_date(day) if day is not None else today_jst()
    return d.weekday() >= 5


def parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.astimezone(JST).date() if value.tzinfo else value.date()
    text = str(value or "").strip()
    if text.lower() == "today":
        return today_jst()
    return date.fromisoformat(text[:10])


def latest_feature_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return str(rows[0].get("trade_date")) if rows else None


def is_latest_feature_today(sb) -> bool:
    latest = latest_feature_date(sb)
    return bool(latest and latest == today_jst().isoformat())


def latest_feature_matches_today(sb) -> tuple[bool, str | None, str]:
    latest = latest_feature_date(sb)
    today = today_jst().isoformat()
    return bool(latest and latest == today), latest, today


def trading_dates_between(sb, start_date: str, end_date: str) -> list[str]:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .gte("trade_date", start_date)
        .lte("trade_date", end_date)
        .order("trade_date")
        .execute()
        .data
        or []
    )
    return sorted({str(r["trade_date"]) for r in rows if r.get("trade_date")})


def trading_day_distance(sb, start_date: str, end_date: str) -> int | None:
    dates = trading_dates_between(sb, start_date, end_date)
    if start_date not in dates or end_date not in dates:
        return None
    return max(len(dates) - 1, 0)


def should_skip_today_cron() -> tuple[bool, str]:
    today = today_jst()
    if is_weekend(today):
        return True, f"non_trading_day_weekend:{today.isoformat()}"
    return False, ""
