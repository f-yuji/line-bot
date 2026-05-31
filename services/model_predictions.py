"""Helpers for point-in-time model prediction storage.

This module never updates existing prediction rows. A prediction is immutable
for (code, trade_date, model_key, model_version); reruns with the same key are
reported as skipped.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any


def _date_str(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return datetime.fromisoformat(str(value)[:10]).date().isoformat()


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _prediction_key(row: dict, model_key: str, model_version: str) -> tuple[str, str, str, str]:
    return (
        str(row.get("code") or ""),
        _date_str(row.get("trade_date")),
        str(model_key),
        str(model_version),
    )


def _fetch_existing_keys(sb, rows: list[dict], model_key: str, model_version: str, chunk_size: int) -> set[tuple[str, str, str, str]]:
    existing: set[tuple[str, str, str, str]] = set()
    by_date: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        code = str(row.get("code") or "")
        if not code:
            continue
        by_date[_date_str(row.get("trade_date"))].add(code)

    for trade_date, codes in by_date.items():
        for code_chunk in _chunks(sorted(codes), chunk_size):
            data = (
                sb.table("model_predictions")
                .select("code,trade_date,model_key,model_version")
                .eq("trade_date", trade_date)
                .eq("model_key", model_key)
                .eq("model_version", model_version)
                .in_("code", code_chunk)
                .execute()
                .data or []
            )
            for item in data:
                existing.add((
                    str(item.get("code") or ""),
                    _date_str(item.get("trade_date")),
                    str(item.get("model_key") or ""),
                    str(item.get("model_version") or ""),
                ))
    return existing


def save_model_predictions(
    sb,
    rows: list[dict],
    *,
    model_key: str,
    model_version: str,
    source: str = "daily_prediction",
    metadata: dict | None = None,
    chunk_size: int = 500,
) -> dict:
    """Insert immutable prediction rows, skipping rows that already exist."""

    result = {"inserted": 0, "skipped": 0, "errors": 0, "error_details": []}
    metadata = dict(metadata or {})
    clean_rows: list[dict] = []
    seen: set[tuple[str, str, str, str]] = set()

    for row in rows:
        try:
            code = str(row.get("code") or "").strip()
            trade_date = _date_str(row.get("trade_date"))
            probability = _float(row.get("signal_probability"))
            if not code or probability is None:
                raise ValueError("code/trade_date/signal_probability is required")
            key = (code, trade_date, str(model_key), str(model_version))
            if key in seen:
                result["skipped"] += 1
                continue
            seen.add(key)
            row_meta = dict(metadata)
            if isinstance(row.get("metadata"), dict):
                row_meta.update(row.get("metadata") or {})
            clean_rows.append({
                "code": code,
                "trade_date": trade_date,
                "model_key": model_key,
                "model_version": model_version,
                "prediction_date": row.get("prediction_date") or datetime.now(timezone.utc).isoformat(),
                "signal_probability": probability,
                "signal_stage": row.get("signal_stage"),
                "prediction_label": row.get("prediction_label"),
                "feature_snapshot_trade_date": row.get("feature_snapshot_trade_date") or trade_date,
                "feature_snapshot_id": row.get("feature_snapshot_id"),
                "feature_hash": row.get("feature_hash"),
                "feature_version": row.get("feature_version"),
                "source": source,
                "metadata": row_meta,
                "is_active": row.get("is_active", True),
            })
        except Exception as exc:
            result["errors"] += 1
            result["error_details"].append({"row": row, "error": str(exc)})

    if not clean_rows:
        return result

    existing = _fetch_existing_keys(sb, clean_rows, model_key, model_version, chunk_size)
    to_insert = []
    for row in clean_rows:
        key = _prediction_key(row, model_key, model_version)
        if key in existing:
            result["skipped"] += 1
        else:
            to_insert.append(row)

    for chunk in _chunks(to_insert, chunk_size):
        try:
            sb.table("model_predictions").insert(chunk).execute()
            result["inserted"] += len(chunk)
        except Exception as exc:
            result["errors"] += len(chunk)
            result["error_details"].append({"chunk_size": len(chunk), "error": str(exc)})
    return result


def get_latest_model_version_from_predictions(sb, model_key: str) -> str | None:
    rows = (
        sb.table("model_predictions")
        .select("model_version,prediction_date")
        .eq("model_key", model_key)
        .eq("is_active", True)
        .order("prediction_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return str(rows[0].get("model_version")) if rows else None


def load_model_predictions(
    sb,
    *,
    model_key: str,
    model_version: str | None = None,
    trade_date_from: str | date | None = None,
    trade_date_to: str | date | None = None,
    source: str | None = None,
    active_only: bool = True,
) -> list[dict]:
    if model_version in {None, "", "latest"}:
        model_version = get_latest_model_version_from_predictions(sb, model_key)
    if not model_version:
        return []

    def query(last_id: int):
        q = (
            sb.table("model_predictions")
            .select("*")
            .gt("id", last_id)
            .eq("model_key", model_key)
            .eq("model_version", model_version)
            .order("id")
        )
        if active_only:
            q = q.eq("is_active", True)
        if trade_date_from:
            q = q.gte("trade_date", _date_str(trade_date_from))
        if trade_date_to:
            q = q.lte("trade_date", _date_str(trade_date_to))
        if source:
            q = q.eq("source", source)
        return q

    rows: list[dict] = []
    last_id = 0
    while True:
        data = query(last_id).limit(1000).execute().data or []
        rows.extend(data)
        if len(data) < 1000:
            break
        last_id = max(int(r.get("id") or last_id) for r in data)
    return rows


def prediction_exists(sb, code: str, trade_date: str | date, model_key: str, model_version: str) -> bool:
    rows = (
        sb.table("model_predictions")
        .select("id")
        .eq("code", str(code))
        .eq("trade_date", _date_str(trade_date))
        .eq("model_key", model_key)
        .eq("model_version", model_version)
        .limit(1)
        .execute()
        .data or []
    )
    return bool(rows)


def join_predictions_to_candidates(
    candidates: list[dict],
    predictions: list[dict],
    *,
    on: tuple[str, str] = ("code", "trade_date"),
) -> dict:
    """Attach saved predictions to candidate rows in-place."""

    pred_by_key = {
        (str(p.get(on[0]) or ""), _date_str(p.get(on[1]))): p
        for p in predictions
    }
    matched = 0
    missing = 0
    for row in candidates:
        trade_date = row.get("trade_date") or row.get("label_trade_date")
        key = (str(row.get("code") or ""), _date_str(trade_date))
        pred = pred_by_key.get(key)
        if not pred:
            row["score_missing"] = True
            missing += 1
            continue
        row["signal_probability"] = _float(pred.get("signal_probability"))
        row["signal_stage"] = pred.get("signal_stage")
        row["score_source"] = "stored_predictions"
        row["model_key"] = pred.get("model_key")
        row["model_version"] = pred.get("model_version")
        row["prediction_date"] = pred.get("prediction_date")
        row["prediction_created_at"] = pred.get("created_at")
        row["prediction_source"] = pred.get("source")
        row["score_missing"] = False
        row["score_fallback_used"] = False
        matched += 1
    return {"matched": matched, "missing": missing}
