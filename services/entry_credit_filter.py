"""Entry-side credit balance filters for rebound virtual trades."""

from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CreditFilterResult:
    passed: bool
    reason: str | None = None
    margin_ratio: float | None = None
    margin_date: str | None = None


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


def _ref_date(row: dict) -> str:
    for key in ("trade_date", "buy_date", "drop_detected_at", "created_at", "updated_at"):
        value = row.get(key)
        if value:
            text = str(value)
            if "T" in text:
                return text.split("T", 1)[0]
            return text[:10]
    return date.today().isoformat()


def _load_latest_margin(sb, code: str, ref_date: str) -> tuple[float | None, str | None]:
    try:
        rows = (
            sb.table("stock_weekly_margin_interest")
            .select("date,margin_ratio")
            .eq("code", str(code))
            .lte("date", ref_date)
            .order("date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if not rows:
            return None, None
        row = rows[0]
        return _to_float(row.get("margin_ratio")), row.get("date")
    except Exception as e:
        logger.warning("[entry_margin_filter] load failed code=%s ref_date=%s error=%s", code, ref_date, e)
        return None, None


def attach_entry_margin_data(sb, rows: list[dict]) -> None:
    refs: list[date] = []
    codes = sorted({str(r.get("code") or "") for r in rows if r.get("code")})
    if not rows or not codes:
        return
    for row in rows:
        try:
            refs.append(date.fromisoformat(_ref_date(row)))
        except Exception:
            pass
    if not refs:
        return
    start = (min(refs) - timedelta(days=120)).isoformat()
    end = max(refs).isoformat()
    margin_rows: list[dict] = []
    try:
        for i in range(0, len(codes), 100):
            chunk = codes[i : i + 100]
            offset = 0
            while True:
                data = (
                    sb.table("stock_weekly_margin_interest")
                    .select("code,date,margin_ratio")
                    .in_("code", chunk)
                    .gte("date", start)
                    .lte("date", end)
                    .order("date")
                    .range(offset, offset + 999)
                    .execute()
                    .data or []
                )
                margin_rows.extend(data)
                if len(data) < 1000:
                    break
                offset += 1000
    except Exception as e:
        logger.warning("[entry_margin_filter] bulk load failed: %s", e)
        return

    by_code: dict[str, list[tuple[date, dict]]] = {}
    for row in margin_rows:
        code = str(row.get("code") or "")
        try:
            d = date.fromisoformat(str(row.get("date")))
        except Exception:
            continue
        by_code.setdefault(code, []).append((d, row))
    index: dict[str, tuple[list[date], list[dict]]] = {}
    for code, items in by_code.items():
        items.sort(key=lambda x: x[0])
        index[code] = ([d for d, _ in items], [r for _, r in items])

    for row in rows:
        if _to_float(row.get("margin_ratio")) is not None:
            continue
        code = str(row.get("code") or "")
        dates, items = index.get(code, ([], []))
        if not dates:
            continue
        try:
            ref = date.fromisoformat(_ref_date(row))
        except Exception:
            continue
        pos = bisect_right(dates, ref) - 1
        if pos < 0:
            continue
        margin = items[pos]
        row["margin_ratio"] = _to_float(margin.get("margin_ratio"))
        row["margin_date"] = margin.get("date")


def evaluate_entry_credit_filter(sb, row: dict, cfg: dict) -> CreditFilterResult:
    if not _to_bool(cfg.get("entry_margin_filter_enabled", True)):
        return CreditFilterResult(True)

    max_ratio = _to_float(cfg.get("entry_max_margin_ratio"), 5.0)
    require_data = _to_bool(cfg.get("entry_margin_require_data", True))
    if max_ratio is None or max_ratio <= 0:
        return CreditFilterResult(True)

    ratio = _to_float(row.get("margin_ratio"))
    margin_date = row.get("margin_date")
    if ratio is None:
        code = str(row.get("code") or "")
        ratio, margin_date = _load_latest_margin(sb, code, _ref_date(row))

    if ratio is None:
        if require_data:
            return CreditFilterResult(False, "margin_ratio_missing")
        return CreditFilterResult(True, margin_ratio=None, margin_date=margin_date)

    if ratio > max_ratio:
        return CreditFilterResult(False, "margin_ratio_over_limit", ratio, str(margin_date) if margin_date else None)
    return CreditFilterResult(True, margin_ratio=ratio, margin_date=str(margin_date) if margin_date else None)
