"""Display-only current-price support for H5 purchase decisions.

The values returned here must not be used to create, reprice, or close virtual
trades. They are delayed/best-effort quotes for a human execution check only.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)

PRICE_CACHE_TTL = timedelta(minutes=5)
_PRICE_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}
_PRICE_CACHE_LOCK = Lock()


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            return None
        return number
    except (TypeError, ValueError):
        return None


def _ticker(code: str) -> str:
    clean = str(code or "").replace(".T", "").strip()
    return f"{clean}.T"


def _last_close(frame: Any) -> float | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        return _number(frame["Close"].dropna().iloc[-1])
    except Exception:
        return None


def _fetch_yfinance_uncached(code: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    result: dict[str, Any] = {
        "code": str(code),
        "ticker": _ticker(code),
        "current_price": None,
        "fetched_at": now.isoformat(),
        "source": "yfinance",
        "status": "failed",
        "error": None,
    }
    try:
        import yfinance as yf

        ticker = yf.Ticker(result["ticker"])
        price = None
        try:
            price = _number(getattr(ticker.fast_info, "last_price", None))
        except Exception:
            price = None
        if price is None:
            price = _last_close(ticker.history(period="1d", interval="1m", auto_adjust=False))
        if price is None:
            price = _last_close(ticker.history(period="5d", interval="1d", auto_adjust=False))
        if price is None:
            result["error"] = "price_not_available"
            return result
        result["current_price"] = price
        result["status"] = "ok"
        return result
    except Exception as exc:
        logger.warning("[h5_price_assist] yfinance fetch failed code=%s error=%s", code, exc)
        result["error"] = str(exc)
        return result


def get_yfinance_current_price(code: str, *, force: bool = False) -> dict[str, Any]:
    """Get a best-effort quote, cached in-process for five minutes."""
    clean = str(code or "").replace(".T", "").strip()
    if not clean:
        return {
            "code": clean,
            "ticker": "",
            "current_price": None,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": "yfinance",
            "status": "failed",
            "error": "code_missing",
        }
    now = datetime.now(timezone.utc)
    if not force:
        with _PRICE_CACHE_LOCK:
            cached = _PRICE_CACHE.get(clean)
            if cached and now - cached[0] < PRICE_CACHE_TTL:
                return dict(cached[1])
    result = _fetch_yfinance_uncached(clean)
    with _PRICE_CACHE_LOCK:
        _PRICE_CACHE[clean] = (now, dict(result))
    return result


def judge_h5_entry_status(entry_gap_pct: float | None) -> tuple[str, str]:
    """Classify execution distance from the signal/virtual-entry price."""
    if entry_gap_pct is None:
        return "price_fetch_failed", "現在値取得失敗。証券口座の現在値・板で確認。"
    if abs(entry_gap_pct) > 30:
        return "price_fetch_suspect", "現在値が異常値の可能性。証券口座で確認。"
    if entry_gap_pct <= 0:
        return "entry_favorable", "シグナル価格以下。価格面は有利。H5候補維持。"
    if entry_gap_pct <= 1:
        return "entry_ok", "エントリー可。シグナル価格からの乖離は小さい。"
    if entry_gap_pct <= 2:
        return "entry_caution", "許容範囲内だが慎重。H5期待値はやや低下。"
    if entry_gap_pct <= 3:
        return "gap_chase_warning", "+2%超。飛びつき警戒。小ロットまたは見送り検討。"
    return "entry_ng", "+3%超。原則見送り。"


def signal_price_from_row(row: dict[str, Any]) -> float | None:
    """Return the H5 signal/virtual-entry price from known field names."""
    return (
        _number(row.get("signal_price"))
        or _number(row.get("virtual_entry_price"))
        or _number(row.get("entry_price"))
        or _number(row.get("entry_price_at_signal"))
        or _number(row.get("buy_price"))
    )


def build_h5_price_assist_fields(
    row: dict[str, Any],
    quote: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build display-only H5 execution price fields for a candidate/trade."""
    signal_price = signal_price_from_row(row)
    result: dict[str, Any] = {
        "signal_price": signal_price,
        "entry_limit_2pct": signal_price * 1.02 if signal_price else None,
        "entry_limit_3pct": signal_price * 1.03 if signal_price else None,
        "price_source": (quote or {}).get("source") or row.get("price_source") or "yfinance",
        "current_price_yf": row.get("current_price_yf"),
        "current_price_fetched_at": row.get("current_price_fetched_at"),
        "entry_gap_pct": row.get("entry_gap_pct"),
        "entry_status": row.get("entry_status"),
        "entry_status_label": row.get("entry_status_label"),
        "price_fetch_error": row.get("price_fetch_error"),
    }
    if signal_price is None:
        result["entry_status"] = "price_fetch_failed"
        result["entry_status_label"] = "シグナル価格を取得できません。証券口座で確認。"
        result["price_fetch_error"] = result["price_fetch_error"] or "signal_price_missing"
        return result

    if quote is not None:
        result["current_price_yf"] = quote.get("current_price")
        result["current_price_fetched_at"] = quote.get("fetched_at")
        result["price_fetch_error"] = quote.get("error")
        if quote.get("status") == "ok" and _number(quote.get("current_price")) is not None:
            result["entry_gap_pct"] = round((float(quote["current_price"]) / signal_price - 1.0) * 100.0, 6)
        else:
            result["entry_gap_pct"] = None

    try:
        gap = float(result["entry_gap_pct"]) if result["entry_gap_pct"] is not None else None
    except Exception:
        gap = None
    result["entry_status"], result["entry_status_label"] = judge_h5_entry_status(gap)
    return result


def decorate_h5_price_assist_cards(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach display fields only to H5 Live Limited cards.

    This function intentionally does not fetch yfinance quotes. Quotes are
    updated by explicit manual refresh actions and then displayed here.
    """
    for row in rows:
        if not row.get("h5_primary_match") or not row.get("is_live_candidate"):
            continue
        row.update(build_h5_price_assist_fields(row))
    return rows


H5_ENTRY_STATUS_PRIORITY = {
    "entry_favorable": 5,
    "entry_ok": 4,
    "entry_caution": 3,
    "gap_chase_warning": 2,
    "price_fetch_failed": 1,
    "price_fetch_suspect": 1,
    "entry_ng": 0,
}
