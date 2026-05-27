"""Shared definition and entry qualification for the rebound H5 Primary."""

from __future__ import annotations

import math
from typing import Any

H5_PRIMARY_CASE_KEY = "h5_ai65_pb20_hd3_est12_cm_range330"
H5_PRIMARY_LABEL = "H5 Primary: AI65 / PB2 / HD3 / EST12 / Credit 3-30"
H5_PRIMARY_DISPLAY_NAME = "H5 Primary: AI65 / PB2 / HD3 / EST12 / 信用3-30"
H5_ENTRY_EXECUTION_NOTE = (
    "同日終値付近のentry前提。翌日寄りは期待値が低下するため、"
    "+2%超GUは飛びつき警戒。"
)

H5_PRIMARY_RULES: dict[str, Any] = {
    "min_ai_score": 0.65,
    "allowed_stages": ["confirmed", "strong_confirmed"],
    "min_drop_from_20d_high": -8.0,
    "excluded_regimes": ["panic_selloff"],
    "exit_type": "peak_pullback_exit",
    "peak_pullback_pct": -0.02,
    "initial_sl_pct": -0.12,
    "max_holding_days": 3,
    "max_overheat_score": 1,
    "use_margin_filter": True,
    "require_margin_data": False,
    "min_margin_ratio": 3.0,
    "max_margin_ratio": 30.0,
    "is_primary_h5": True,
    "entry_execution_note": H5_ENTRY_EXECUTION_NOTE,
}


def _float(value: Any) -> float | None:
    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def h5_overheat_score(row: dict[str, Any]) -> int:
    """Use the same cool/mild score definition as the H5 backtest."""
    score = 0
    rsi = _float(row.get("rsi14"))
    ma5_gap = _float(row.get("ma5_gap_pct"))
    if ma5_gap is None:
        ma5_gap = _float(row.get("entry_ma5_gap_pct"))
    ret5 = _float(row.get("return_5d_pct"))
    volume = _float(row.get("volume_ratio_20d"))
    if rsi is not None and rsi >= 65:
        score += 1
    if ma5_gap is not None and ma5_gap >= 5:
        score += 1
    if ret5 is not None and ret5 >= 8:
        score += 1
    if volume is not None and volume >= 3:
        score += 1
    return score


def evaluate_h5_primary_entry(row: dict[str, Any]) -> tuple[bool, list[str], dict[str, Any]]:
    """Return whether a signal qualifies for H5 Primary and stored metadata."""
    rules = H5_PRIMARY_RULES
    probability = _float(row.get("signal_probability"))
    if probability is None:
        probability = _float(row.get("probability"))
    drop20 = _float(row.get("drop_from_20d_high_pct"))
    margin = _float(row.get("margin_ratio"))
    stage = str(row.get("signal_stage") or "")
    regime = str(row.get("market_regime") or "")
    overheat = h5_overheat_score(row)
    reasons: list[str] = []

    if probability is None or probability < rules["min_ai_score"]:
        reasons.append("h5_ai_score_below_065")
    if stage not in rules["allowed_stages"]:
        reasons.append("h5_stage_not_confirmed")
    if drop20 is None:
        reasons.append("h5_drop20_missing")
    elif drop20 > rules["min_drop_from_20d_high"]:
        reasons.append("h5_drop20_not_deep_enough")
    if regime in rules["excluded_regimes"]:
        reasons.append("h5_panic_selloff")
    if overheat > rules["max_overheat_score"]:
        reasons.append("h5_overheat")
    if margin is not None:
        if margin < rules["min_margin_ratio"]:
            reasons.append("h5_margin_below_3")
        if margin > rules["max_margin_ratio"]:
            reasons.append("h5_margin_above_30")

    meta = {
        "case_key": H5_PRIMARY_CASE_KEY,
        "case_label": H5_PRIMARY_DISPLAY_NAME,
        "is_primary_h5": True,
        "exit_rule": rules["exit_type"],
        "peak_pullback_pct": rules["peak_pullback_pct"],
        "initial_sl_pct": rules["initial_sl_pct"],
        "max_holding_days": rules["max_holding_days"],
        "entry_drop_from_20d_high_pct": drop20,
        "entry_overheat_score": overheat,
        "margin_ratio": margin,
        "virtual_entry_model": "close_entry",
        "entry_execution_note": H5_ENTRY_EXECUTION_NOTE,
        "h5_skip_reason": ",".join(reasons) if reasons else None,
    }
    return not reasons, reasons, meta

