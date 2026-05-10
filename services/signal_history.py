"""Append-only-ish rebound signal history.

This keeps event history separate from stock_drop_watchlist/current state and
virtual_trades/forward verification.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.signal_stage import SIGNAL_STAGES


logger = logging.getLogger(__name__)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _date_only(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    return text[:10] if len(text) >= 10 else text


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _lookup_prime_stock(sb, code: str) -> dict:
    try:
        rows = (
            sb.table("prime_stocks_cache")
            .select("code,name,sector")
            .eq("code", code)
            .limit(1)
            .execute()
            .data or []
        )
        return rows[0] if rows else {}
    except Exception as e:
        logger.debug("signal_history prime stock lookup failed code=%s error=%s", code, e)
        return {}


def _signal_key(source: str, code: str, feature_snapshot_id: Any, signal_date: Any, stage: str) -> str:
    if feature_snapshot_id:
        anchor = f"fs:{feature_snapshot_id}"
    else:
        anchor = f"date:{_date_only(signal_date) or 'unknown'}"
    return f"{source}:{code}:{anchor}:{stage}"


def record_rebound_signal(
    sb,
    *,
    source: str,
    snapshot: dict | None = None,
    watchlist: dict | None = None,
    result: dict | None = None,
    extra: dict | None = None,
    dry_run: bool = False,
) -> None:
    """Record an active signal event without touching production trade state."""

    snapshot = snapshot or {}
    watchlist = watchlist or {}
    result = result or {}
    extra = extra or {}

    stage = (
        result.get("signal_stage")
        or watchlist.get("signal_stage")
        or snapshot.get("signal_stage")
        or "none"
    )
    if stage not in SIGNAL_STAGES:
        return

    code = str(snapshot.get("code") or watchlist.get("code") or "").strip()
    if not code:
        return

    master = {}
    raw_sector = _clean_text(snapshot.get("sector")) or _clean_text(watchlist.get("sector"))
    raw_name = _clean_text(snapshot.get("name")) or _clean_text(watchlist.get("name"))
    if not raw_sector or not raw_name:
        master = _lookup_prime_stock(sb, code)
    sector = raw_sector or _clean_text(master.get("sector"))
    name = raw_name or _clean_text(master.get("name"))

    now = datetime.now(timezone.utc).isoformat()
    feature_snapshot_id = snapshot.get("id") or watchlist.get("feature_snapshot_id")
    signal_date = (
        snapshot.get("trade_date")
        or _date_only(watchlist.get("drop_detected_at"))
        or _date_only(watchlist.get("last_signal_at"))
    )
    key = _signal_key(source, code, feature_snapshot_id, signal_date, stage)
    probability = (
        result.get("probability")
        if result.get("probability") is not None
        else result.get("signal_probability")
        if result.get("signal_probability") is not None
        else watchlist.get("signal_probability")
    )
    rule_score = (
        result.get("signal_score")
        if result.get("signal_score") is not None
        else watchlist.get("signal_score")
        if watchlist.get("signal_score") is not None
        else watchlist.get("score")
    )
    expected_value = (
        result.get("expected_value")
        if result.get("expected_value") is not None
        else watchlist.get("expected_value")
    )
    payload = {
        "snapshot": {
            "id": snapshot.get("id"),
            "trade_date": snapshot.get("trade_date"),
            "close": snapshot.get("close"),
            "drop_pct": snapshot.get("drop_pct") or snapshot.get("day_change_pct"),
            "rsi14": snapshot.get("rsi14"),
            "volume_ratio_20d": snapshot.get("volume_ratio_20d"),
        },
        "watchlist": {
            "id": watchlist.get("id"),
            "status": watchlist.get("status"),
            "signal_count": watchlist.get("signal_count"),
        },
        "result": {
            "signal_stage": stage,
            "probability": probability,
            "expected_value": expected_value,
            "signal_score": rule_score,
        },
        **extra,
    }
    price_at_signal = snapshot.get("close") or watchlist.get("price_at_drop")
    current_price = extra.get("current_price") or watchlist.get("current_price") or price_at_signal

    row = {
        "signal_key": key,
        "source": source,
        "source_run_id": extra.get("source_run_id"),
        "watchlist_id": watchlist.get("id"),
        "feature_snapshot_id": feature_snapshot_id,
        "code": code,
        "name": name,
        "market": snapshot.get("market") or watchlist.get("market") or "prime",
        "sector": sector,
        "signal_date": _date_only(signal_date),
        "detected_at": now,
        "last_seen_at": now,
        "signal_stage": stage,
        "signal_probability": probability,
        "expected_value": expected_value,
        "rule_score": rule_score,
        "current_price": current_price,
        "price_at_signal": price_at_signal,
        "drop_pct": snapshot.get("drop_pct") or snapshot.get("day_change_pct") or watchlist.get("drop_pct"),
        "rsi14": snapshot.get("rsi14"),
        "volume_ratio": snapshot.get("volume_ratio_20d") or extra.get("volume_ratio"),
        "status_at_signal": watchlist.get("status"),
        "bad_news_score": snapshot.get("bad_news_score") or watchlist.get("bad_news_score") or 0,
        "is_excluded": bool(result.get("is_excluded") or watchlist.get("is_excluded")),
        "exclude_reason": result.get("exclude_reason") or watchlist.get("exclude_reason"),
        "market_regime": result.get("market_regime") or watchlist.get("market_regime"),
        "market_regime_label": result.get("market_regime_label") or watchlist.get("market_regime_label"),
        "market_threshold_adjust": result.get("market_threshold_adjust") or watchlist.get("market_threshold_adjust") or 0,
        "market_regime_reason": result.get("market_regime_reason") or watchlist.get("market_regime_reason"),
        "market_nikkei_pct": result.get("market_nikkei_pct") or watchlist.get("market_nikkei_pct"),
        "market_topix_pct": result.get("market_topix_pct") or watchlist.get("market_topix_pct"),
        "market_nikkei_change_yen": result.get("market_nikkei_change_yen") or watchlist.get("market_nikkei_change_yen"),
        "payload": payload,
        "updated_at": now,
    }

    if dry_run:
        logger.info("DRYRUN signal_history upsert: %s", row)
        return

    try:
        existing = (
            sb.table("rebound_signal_history")
            .select("id,occurrence_count")
            .eq("signal_key", key)
            .limit(1)
            .execute()
            .data or []
        )
        if existing:
            count = int(existing[0].get("occurrence_count") or 1) + 1
            update = dict(row)
            update.pop("detected_at", None)
            update["occurrence_count"] = count
            sb.table("rebound_signal_history").update(update).eq("id", existing[0]["id"]).execute()
        else:
            sb.table("rebound_signal_history").insert(row).execute()
    except Exception as e:
        logger.warning("signal_history record failed code=%s source=%s error=%s", code, source, e)
