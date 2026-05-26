"""Display-only diagnostics for rebound signal and virtual entry decisions.

This module deliberately does not write to the database or change eligibility.
It explains the values that the current ``predict_rebound`` path already used.
"""

from __future__ import annotations

from typing import Any

from services.signal_stage import evaluate_signal_stage


SNAPSHOT_FIELDS = (
    "id,trade_date,close,prev_close,day_change_pct,drop_pct,"
    "drop_from_20d_high_pct,rsi14,rsi_min_5d,rsi_recover_flag,"
    "volume_ratio_20d,ma5,ma25,ma75"
)


def _number(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_number(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _load_snapshots(sb, rows: list[dict]) -> dict[str, dict]:
    ids = sorted({str(row.get("feature_snapshot_id")) for row in rows if row.get("feature_snapshot_id")})
    found: dict[str, dict] = {}
    for offset in range(0, len(ids), 100):
        batch = ids[offset:offset + 100]
        data = sb.table("stock_feature_snapshots").select(SNAPSHOT_FIELDS).in_("id", batch).execute().data or []
        found.update({str(item.get("id")): item for item in data if item.get("id")})
    return found


def _market_context(row: dict, fallback: dict | None) -> dict:
    fallback = fallback or {}
    adjust = _first_number(row, "market_threshold_adjust")
    return {
        "regime": row.get("market_regime") or fallback.get("regime"),
        "label": row.get("market_regime_label") or fallback.get("label"),
        "ai_threshold_adjust": adjust if adjust is not None else float(fallback.get("ai_threshold_adjust") or 0.0),
    }


def decorate_rebound_diagnostics(
    sb,
    rows: list[dict],
    settings: dict,
    market_adjustment: dict | None = None,
) -> list[dict]:
    """Attach decision-explanation fields to rows for UI rendering only."""
    if not rows:
        return rows
    snapshots = _load_snapshots(sb, rows)
    for row in rows:
        snapshot = snapshots.get(str(row.get("feature_snapshot_id"))) or {}
        for key, value in snapshot.items():
            if row.get(key) is None:
                row[key] = value

        probability = _first_number(row, "signal_probability", "entry_probability", "ai_probability")
        rule_score = _first_number(row, "signal_score", "entry_score", "buy_score", "score") or 0.0
        expected_value = _first_number(row, "expected_value") or 0.0
        decision = evaluate_signal_stage(
            probability,
            rule_score,
            expected_value,
            settings,
            _market_context(row, market_adjustment),
        )
        thresholds = decision.get("thresholds") or {}
        entered = (
            row.get("buy_price") is not None
            or row.get("virtual_trade_id") is not None
            or str(row.get("status") or "") in {"entered", "open", "closed"}
        )
        entry_price = _first_number(row, "buy_price", "entry_price", "price_at_drop", "close")
        ai_text = f"{probability * 100:.0f}%" if probability is not None else "-"
        confirmed = float(thresholds.get("confirmed") or 0) * 100
        strong = float(thresholds.get("strong") or 0) * 100

        row.update({
            "diagnostic_engine": "predict_rebound_ai_snapshot",
            "diagnostic_engine_label": "引け後AIモデル判定",
            "diagnostic_stage_reason": decision.get("reason"),
            "diagnostic_threshold_text": f"AI {ai_text} / 本命 {confirmed:.0f}% / 強本命 {strong:.0f}% + rule 60",
            "diagnostic_day_change_pct": _first_number(row, "day_change_pct"),
            "diagnostic_drop_pct": _first_number(row, "drop_pct", "drop_from_20d_high_pct"),
            "diagnostic_close": _first_number(row, "close", "price_at_drop"),
            "diagnostic_rsi14": _first_number(row, "rsi14"),
            "diagnostic_rsi_min_5d": _first_number(row, "rsi_min_5d"),
            "diagnostic_rsi_recover_flag": row.get("rsi_recover_flag"),
            "diagnostic_volume_ratio_20d": _first_number(row, "volume_ratio_20d"),
            "diagnostic_entry_timing": "signal_close" if entered else "not_entered",
            "diagnostic_entry_timing_label": (
                "シグナル発生日の終値で仮想購入" if entered else "シグナル判定のみ（未購入）"
            ),
            "diagnostic_entry_price_text": (
                f"{entry_price:,.0f}円（シグナル日終値）" if entered and entry_price is not None else "-"
            ),
            "diagnostic_rule_note": (
                "反発率・RSI回復ラインは monitor_rebound 用で、"
                "日次AIエントリーの必須条件ではありません。"
            ),
            "diagnostic_settings_note": "AI閾値は現在設定と判定時の地合い補正で照合表示しています。",
        })
    return rows
