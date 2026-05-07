"""Shared signal_stage evaluation for rebound AI.

The database column names still use "probability" for compatibility, but the
UI treats this value as an AI score rather than a calibrated win probability.
"""

from __future__ import annotations

from typing import Any


SIGNAL_STAGES = {"early", "confirmed", "strong_confirmed"}
STAGE_LABELS = {
    "strong_confirmed": "強本命",
    "confirmed": "本命",
    "early": "初動",
    "none": "シグナルなし",
}
STAGE_RANK = {
    "strong_confirmed": 3,
    "confirmed": 2,
    "early": 1,
    "none": 0,
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _threshold(settings: dict | None, key: str, default: float) -> float:
    if not settings:
        return default
    value = _to_float(settings.get(key), None)
    return default if value is None else value


def _normalize_ai_score(ai_score: Any) -> float | None:
    value = _to_float(ai_score, None)
    if value is None:
        return None
    if value > 1.0:
        return value / 100.0
    return value


def evaluate_signal_stage(
    ai_score: Any,
    rule_score: Any,
    expected_value: Any = None,
    settings: dict | None = None,
    market_regime: dict | None = None,
) -> dict:
    """Return the single canonical signal_stage decision.

    `ai_score` accepts either 0..1 or 0..100. `rule_score` is a 0..100 helper
    score. `expected_value` is currently returned for callers' context but does
    not gate the stage; the stage is intentionally AI-score-led.
    """

    ai = _normalize_ai_score(ai_score)
    rule = _to_float(rule_score, None)
    early = _threshold(settings, "ai_probability_early", 0.35)
    confirmed = _threshold(settings, "ai_probability_confirmed", 0.50)
    strong = _threshold(settings, "ai_probability_strong", 0.65)
    adjust = _to_float((market_regime or {}).get("ai_threshold_adjust"), 0.0) or 0.0
    early = min(0.90, early + adjust)
    confirmed = min(0.90, confirmed + adjust)
    strong = min(0.90, strong + adjust)
    regime_label = (market_regime or {}).get("label")

    if ai is None:
        stage = "none"
        reason = "AIスコアなし"
    elif ai >= strong and rule is not None and rule >= 60:
        stage = "strong_confirmed"
        reason = f"AIスコア{strong * 100:.0f}以上かつルールスコア60以上"
    elif ai >= confirmed:
        stage = "confirmed"
        reason = f"AIスコア{confirmed * 100:.0f}以上"
    elif ai >= early:
        stage = "early"
        reason = f"AIスコア{early * 100:.0f}以上"
    else:
        stage = "none"
        reason = f"AIスコア{early * 100:.0f}未満"

    return {
        "stage": stage,
        "stage_label": STAGE_LABELS[stage],
        "stage_rank": STAGE_RANK[stage],
        "reason": f"{reason}（{regime_label}補正）" if regime_label and adjust else reason,
        "ai_score": ai,
        "rule_score": rule,
        "expected_value": _to_float(expected_value, None),
        "thresholds": {
            "early": early,
            "confirmed": confirmed,
            "strong": strong,
        },
    }
