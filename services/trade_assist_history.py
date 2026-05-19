"""Snapshot and load trade-assist candidates.

The history table is display/research support only. It does not affect
virtual_trades, watchlist lifecycle, or entry logic.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from services.entry_mode import ENTRY_MODE_LABELS, classify_entry_case, ma_gap_pct

logger = logging.getLogger(__name__)


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _fetch_latest_feature_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return str(rows[0].get("trade_date")) if rows else None


def _fetch_by_ids(sb, table: str, ids: list[Any], select: str = "*") -> dict[str, dict]:
    clean_ids = [str(x) for x in ids if x]
    if not clean_ids:
        return {}
    rows = sb.table(table).select(select).in_("id", clean_ids).execute().data or []
    return {str(r.get("id")): r for r in rows if r.get("id")}


def _latest_margin_by_code(sb, codes: set[str], trade_date: str | None) -> dict[str, dict]:
    if not codes:
        return {}
    start_date = None
    if trade_date:
        try:
            start_date = (datetime.fromisoformat(str(trade_date)[:10]) - timedelta(days=60)).date().isoformat()
        except Exception:
            start_date = None
    q = (
        sb.table("stock_weekly_margin_interest")
        .select("code,date,margin_ratio")
        .in_("code", list(codes))
        .order("date", desc=True)
        .limit(1000)
    )
    if trade_date:
        q = q.lte("date", trade_date)
    if start_date:
        q = q.gte("date", start_date)
    rows = q.execute().data or []
    out: dict[str, dict] = {}
    for row in rows:
        code = str(row.get("code") or "")
        if code and code not in out:
            out[code] = row
    return out


def _merge_sources(*sources: dict | None) -> dict:
    merged: dict = {}
    for source in sources:
        if not source:
            continue
        for key, value in source.items():
            if value is not None and merged.get(key) is None:
                merged[key] = value
    return merged


def _card_payload(row: dict, *, trade_date: str, source_kind: str, margin_by_code: dict[str, dict], stop_loss_pct: float) -> dict:
    code = str(row.get("code") or "")
    entry_price = _to_float(row.get("buy_price"), None)
    if entry_price is None:
        entry_price = _to_float(row.get("price_at_drop"), _to_float(row.get("close"), None))
    stop_loss_price = entry_price * (1 - stop_loss_pct / 100) if entry_price is not None else None
    risk_100 = (entry_price - stop_loss_price) * 100 if entry_price is not None and stop_loss_price is not None else None
    margin = margin_by_code.get(code) or {}
    probability = _to_float(row.get("signal_probability"), _to_float(row.get("entry_probability"), None))
    stage = row.get("signal_stage")
    payload = {
        "trade_date": trade_date,
        "code": code,
        "name": row.get("name"),
        "sector": row.get("sector"),
        "source_kind": source_kind,
        "signal_stage": stage,
        "display_status": "強本命" if stage == "strong_confirmed" else "翌日購入候補",
        "entry_price": entry_price,
        "stop_loss_price": stop_loss_price,
        "risk_100": risk_100,
        "ai_score": probability * 100 if probability is not None else None,
        "signal_probability": probability,
        "expected_value": row.get("expected_value"),
        "drop_pct": row.get("drop_pct") or row.get("day_change_pct"),
        "rsi14": row.get("rsi14"),
        "volume_ratio_20d": row.get("volume_ratio_20d"),
        "margin_ratio": margin.get("margin_ratio"),
        "margin_date": margin.get("date"),
        "entry_case": row.get("entry_case") or classify_entry_case(row),
        "entry_mode_used": row.get("entry_mode_used"),
        "recommended_entry_mode": row.get("recommended_entry_mode"),
        "entry_ma5_gap_pct": row.get("entry_ma5_gap_pct") if row.get("entry_ma5_gap_pct") is not None else ma_gap_pct(row, "ma5"),
        "entry_ma25_gap_pct": row.get("entry_ma25_gap_pct") if row.get("entry_ma25_gap_pct") is not None else ma_gap_pct(row, "ma25"),
        "entry_ma75_gap_pct": row.get("entry_ma75_gap_pct") if row.get("entry_ma75_gap_pct") is not None else ma_gap_pct(row, "ma75"),
        "feature_snapshot_id": str(row.get("feature_snapshot_id") or row.get("id") or "") or None,
        "watchlist_id": str(row.get("watchlist_id") or "") or None,
        "virtual_trade_id": str(row.get("virtual_trade_id") or row.get("trade_id") or "") or None,
        "payload": {
            "market_regime": row.get("market_regime"),
            "market_regime_label": row.get("market_regime_label"),
            "market_regime_reason": row.get("market_regime_reason"),
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    return payload


def collect_trade_assist_candidates(sb, *, trade_date: str | None = None, stop_loss_pct: float = 4.0, limit: int = 300) -> list[dict]:
    trade_date = trade_date or _fetch_latest_feature_date(sb)
    if not trade_date:
        return []

    trades = (
        sb.table("virtual_trades")
        .select("*")
        .gte("buy_date", f"{trade_date}T00:00:00+09:00")
        .lt("buy_date", f"{trade_date}T23:59:59+09:00")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )
    snapshot_ids = [t.get("feature_snapshot_id") for t in trades if t.get("feature_snapshot_id")]
    watchlist_ids = [t.get("watchlist_id") for t in trades if t.get("watchlist_id")]
    snapshots = _fetch_by_ids(sb, "stock_feature_snapshots", snapshot_ids)
    watchlists = _fetch_by_ids(sb, "stock_drop_watchlist", watchlist_ids)

    signal_rows = (
        sb.table("stock_drop_watchlist")
        .select("*")
        .eq("status", "rebound_signal")
        .in_("signal_stage", ["confirmed", "strong_confirmed"])
        .order("last_signal_at", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )

    all_codes = {str(t.get("code") or "") for t in trades if t.get("code")} | {str(r.get("code") or "") for r in signal_rows if r.get("code")}
    margin_by_code = _latest_margin_by_code(sb, all_codes, trade_date)

    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for trade in trades:
        base = _merge_sources(
            watchlists.get(str(trade.get("watchlist_id"))),
            snapshots.get(str(trade.get("feature_snapshot_id"))),
            trade,
            {
                "virtual_trade_id": trade.get("id"),
                "buy_price": trade.get("buy_price"),
                "entry_probability": trade.get("entry_probability"),
                "signal_probability": trade.get("entry_probability"),
            },
        )
        if base.get("signal_stage") not in {"confirmed", "strong_confirmed"}:
            continue
        key = (str(base.get("code") or ""), "entered")
        if not key[0] or key in seen:
            continue
        rows.append(_card_payload(base, trade_date=trade_date, source_kind="entered", margin_by_code=margin_by_code, stop_loss_pct=stop_loss_pct))
        seen.add(key)

    for signal in signal_rows:
        if signal.get("is_excluded") or signal.get("virtual_trade_id"):
            continue
        key = (str(signal.get("code") or ""), "signal")
        if not key[0] or key in seen:
            continue
        rows.append(_card_payload(signal, trade_date=trade_date, source_kind="signal", margin_by_code=margin_by_code, stop_loss_pct=stop_loss_pct))
        seen.add(key)

    rows.sort(key=lambda r: (1 if r.get("signal_stage") == "strong_confirmed" else 0, _to_float(r.get("signal_probability"), 0) or 0), reverse=True)
    return rows[:limit]


def save_trade_assist_candidate_history(sb, *, trade_date: str | None = None, stop_loss_pct: float = 4.0, dry_run: bool = False) -> dict:
    rows = collect_trade_assist_candidates(sb, trade_date=trade_date, stop_loss_pct=stop_loss_pct)
    if dry_run:
        logger.info("[trade_assist_history] dry-run rows=%d", len(rows))
        return {"trade_date": trade_date or _fetch_latest_feature_date(sb), "rows": len(rows)}
    if not rows:
        return {"trade_date": trade_date or _fetch_latest_feature_date(sb), "rows": 0}
    try:
        sb.table("trade_assist_candidate_history").upsert(rows, on_conflict="trade_date,code,source_kind").execute()
        logger.info("[trade_assist_history] saved rows=%d date=%s", len(rows), rows[0].get("trade_date"))
    except Exception as e:
        msg = str(e)
        if "trade_assist_candidate_history" in msg or "Could not find" in msg or "relation" in msg:
            logger.warning("[trade_assist_history] table missing; run db/trade_assist_candidate_history.sql: %s", e)
        else:
            raise
    return {"trade_date": rows[0].get("trade_date"), "rows": len(rows)}


def decorate_history_rows(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        r = dict(row)
        r["display_status"] = "履歴"
        r["entry_mode_label"] = ENTRY_MODE_LABELS.get(str(r.get("entry_mode_used") or ""), r.get("entry_mode_used") or "-")
        r["recommended_entry_mode_label"] = ENTRY_MODE_LABELS.get(
            str(r.get("recommended_entry_mode") or ""),
            r.get("recommended_entry_mode") or "-",
        )
        out.append(r)
    return out
