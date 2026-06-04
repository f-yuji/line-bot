"""Shared definition and entry qualification for rebound H5."""

from __future__ import annotations

import math
from typing import Any

# New HD3+EST12 primary keys (no peak pullback)
H5_LIVE_LIMITED_CASE_KEY = "h5_ai65_hd3_est12_cm_range330_live_limited"
H5_RESEARCH_CASE_KEY = "h5_ai65_hd3_est12_cm_range330_research"

# Legacy PB20 keys kept for comparison / backward compatibility
H5_OLD_PB20_LIVE_LIMITED_CASE_KEY = "h5_ai65_pb20_hd3_est12_cm_range330_live_limited"
H5_OLD_PB20_RESEARCH_CASE_KEY = "h5_ai65_pb20_hd3_est12_cm_range330_research"
H5_LEGACY_PRIMARY_CASE_KEY = "h5_ai65_pb20_hd3_est12_cm_range330"

# Extension research keys. These are comparison/forward-test cases only.
H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY = "h5_ai65_hd5_ext_d3ret_m1_est12_cm_range330_live_limited"
H5_EXTENSION_D3RET_M1_RESEARCH_CASE_KEY = "h5_ai65_hd5_ext_d3ret_m1_est12_cm_range330_research"
H5_EXTENSION_D3RET_M1_CASE_KEY = H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY

H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY = "h5_ai65_hd5_ext_m1_ban_uwrsi_est12_range330_live_limited"
H5_EXTENSION_BAN_RESEARCH_CASE_KEY = "h5_ai65_hd5_ext_m1_ban_uwrsi_est12_range330_research"
H5_EXTENSION_BAN_CASE_KEY = H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY

H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY = "h5_ai65_hd5_ext_m1_allow_d1bodyvol_est12_range330_live_limited"
H5_EXTENSION_ALLOW_RESEARCH_CASE_KEY = "h5_ai65_hd5_ext_m1_allow_d1bodyvol_est12_range330_research"
H5_EXTENSION_ALLOW_CASE_KEY = H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY

H5_PRIMARY_CASE_KEY = H5_LIVE_LIMITED_CASE_KEY
H5_ACTIVE_CASE_KEYS = {
    H5_LIVE_LIMITED_CASE_KEY,
    H5_RESEARCH_CASE_KEY,
    H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
    H5_OLD_PB20_RESEARCH_CASE_KEY,
    H5_LEGACY_PRIMARY_CASE_KEY,
}

H5_PRIMARY_LABEL = "H5 Live Limited: AI65 / HD3 / EST12 / Credit 3-30"
H5_RESEARCH_LABEL = "H5 Research: AI65 / HD3 / EST12 / Credit 3-30"
H5_PRIMARY_DISPLAY_NAME = "H5 Live Limited: AI65 / HD3 / EST12 / 信用3-30"
H5_RESEARCH_DISPLAY_NAME = "H5 Research: AI65 / HD3 / EST12 / 信用3-30 / 制限なし"
H5_ENTRY_EXECUTION_NOTE = (
    "同日終値付近のentry前提。翌日寄りは期待値が低下するため、"
    "+2%超GUは飛びつき警戒。"
)

H5_BASE_RULES: dict[str, Any] = {
    "min_ai_score": 0.65,
    "allowed_stages": ["confirmed", "strong_confirmed"],
    "min_drop_from_20d_high": -8.0,
    "excluded_regimes": ["panic_selloff"],
    "exit_type": "time_stop",          # HD3 time stop + EST12 emergency stop. No peak pullback.
    "initial_sl_pct": -0.12,
    "max_holding_days": 3,
    "max_overheat_score": 1,
    "use_margin_filter": True,
    "require_margin_data": False,
    "min_margin_ratio": 3.0,
    "max_margin_ratio": 30.0,
    "entry_execution_note": H5_ENTRY_EXECUTION_NOTE,
    "h5_exit_model": "hd3_est12_no_pullback",
}

# PB20 rules preserved for comparison / legacy trades. Not used for new entries.
H5_PB20_COMPARISON_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "exit_type": "peak_pullback_exit",
    "peak_pullback_pct": -0.02,
    "h5_exit_model": "pb20_hd3_est12_comparison",
}

H5_RESEARCH_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "position_limit_mode": "research",
    "ignore_global_position_limits": True,
    "is_primary_h5": False,
    "is_h5_research": True,
    "is_h5_live_limited": False,
    "is_live_candidate": False,
}

H5_LIVE_LIMITED_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "position_limit_mode": "balanced_cases",
    "live_allocation_mode": "balanced_cases",
    "max_daily_live_candidates": 10,
    "live_allocation_buckets": {
        "current_h5_core": 4,
        "short_pullback": 3,
        "trend_support": 3,
    },
    "max_sector_positions": 2,
    "entry_sort": ["existing_order"],
    "is_primary_h5": True,
    "is_h5_research": False,
    "is_h5_live_limited": True,
    "is_live_candidate": True,
}

H5_EXTENSION_D3RET_M1_BASE_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "exit_type": "conditional_extension",
    "base_holding_days": 3,
    "extension_holding_days": 5,
    "extension_rule": "day3_return_lte",
    "extension_day": 3,
    "extension_return_threshold_pct": -1.0,
    "max_holding_days": 5,
    "is_primary_h5": False,
    "is_live_candidate": False,
    "is_extension_research": True,
    "h5_exit_model": "hd5_extension_d3ret_lte_m1_no_pullback",
}

H5_EXTENSION_D3RET_M1_RESEARCH_RULES: dict[str, Any] = {
    **H5_EXTENSION_D3RET_M1_BASE_RULES,
    "position_limit_mode": "research",
    "ignore_global_position_limits": True,
    "is_h5_research": True,
    "is_h5_live_limited": False,
}

H5_EXTENSION_D3RET_M1_LIVE_LIMITED_RULES: dict[str, Any] = {
    **H5_EXTENSION_D3RET_M1_BASE_RULES,
    "position_limit_mode": "live_limited",
    "max_open_positions": 2,
    "max_daily_entries": 2,
    "max_sector_positions": 2,
    "entry_rank_limit": 10,
    "entry_sort": [
        "signal_probability_desc",
        "overheat_score_asc",
        "volume_ratio_desc",
    ],
    "is_h5_research": False,
    "is_h5_live_limited": True,
}

H5_EXTENSION_BAN_BASE_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "exit_type": "conditional_extension_with_ban",
    "base_holding_days": 3,
    "extension_holding_days": 5,
    "extension_rule": "day3_return_lte",
    "extension_day": 3,
    "extension_return_threshold_pct": -1.0,
    "extension_ban_rule": "deep_loss_upper_shadow_rsi_20_35",
    "ban_day3_return_lte_pct": -3.0,
    "ban_day3_upper_shadow_gte_pct": 1.0,
    "ban_day3_rsi_min": 20.0,
    "ban_day3_rsi_max": 35.0,
    "max_holding_days": 5,
    "is_primary_h5": False,
    "is_live_candidate": False,
    "is_extension_research": True,
    "h5_exit_model": "hd5_extension_d3ret_lte_m1_ban_deep_upper_rsi_no_pullback",
}

H5_EXTENSION_BAN_RESEARCH_RULES: dict[str, Any] = {
    **H5_EXTENSION_BAN_BASE_RULES,
    "position_limit_mode": "research",
    "ignore_global_position_limits": True,
    "is_h5_research": True,
    "is_h5_live_limited": False,
}

H5_EXTENSION_BAN_LIVE_LIMITED_RULES: dict[str, Any] = {
    **H5_EXTENSION_BAN_BASE_RULES,
    "position_limit_mode": "live_limited",
    "max_open_positions": 2,
    "max_daily_entries": 2,
    "max_sector_positions": 2,
    "entry_rank_limit": 10,
    "entry_sort": [
        "signal_probability_desc",
        "overheat_score_asc",
        "volume_ratio_desc",
    ],
    "is_h5_research": False,
    "is_h5_live_limited": True,
}

H5_EXTENSION_ALLOW_BASE_RULES: dict[str, Any] = {
    **H5_BASE_RULES,
    "exit_type": "conditional_extension_allow",
    "base_holding_days": 3,
    "extension_holding_days": 5,
    "extension_rule": "day3_return_lte",
    "extension_day": 3,
    "extension_return_threshold_pct": -1.0,
    "extension_allow_rule": "stable_day1_small_day3_body_low_volume",
    "allow_day1_return_gte_pct": -2.22,
    "allow_day3_body_lte_pct": 3.74,
    "allow_day3_volume_ratio_lte": 2.0,
    "max_holding_days": 5,
    "is_primary_h5": False,
    "is_live_candidate": False,
    "is_extension_research": True,
    "h5_exit_model": "hd5_extension_m1_allow_d1bodyvol_no_pullback",
}

H5_EXTENSION_ALLOW_RESEARCH_RULES: dict[str, Any] = {
    **H5_EXTENSION_ALLOW_BASE_RULES,
    "position_limit_mode": "research",
    "ignore_global_position_limits": True,
    "is_h5_research": True,
    "is_h5_live_limited": False,
}

H5_EXTENSION_ALLOW_LIVE_LIMITED_RULES: dict[str, Any] = {
    **H5_EXTENSION_ALLOW_BASE_RULES,
    "position_limit_mode": "live_limited",
    "max_open_positions": 2,
    "max_daily_entries": 2,
    "max_sector_positions": 2,
    "entry_rank_limit": 10,
    "entry_sort": [
        "signal_probability_desc",
        "overheat_score_asc",
        "volume_ratio_desc",
    ],
    "is_h5_research": False,
    "is_h5_live_limited": True,
}

# Backwards-compatible name used by existing code paths.
H5_PRIMARY_RULES = H5_LIVE_LIMITED_RULES


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


def evaluate_h5_primary_entry(
    row: dict[str, Any],
    *,
    case_key: str = H5_LIVE_LIMITED_CASE_KEY,
    case_label: str = H5_PRIMARY_DISPLAY_NAME,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Return whether a signal qualifies for H5 and stored metadata."""
    rules = H5_BASE_RULES
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

    is_live_limited = case_key == H5_LIVE_LIMITED_CASE_KEY
    meta = {
        "case_key": case_key,
        "case_label": case_label,
        "is_primary_h5": is_live_limited,
        "position_limit_mode": "live_limited" if is_live_limited else "research",
        "is_h5_research": case_key == H5_RESEARCH_CASE_KEY,
        "is_h5_live_limited": is_live_limited,
        "is_live_candidate": is_live_limited,
        "exit_rule": rules["exit_type"],
        "peak_pullback_pct": rules.get("peak_pullback_pct"),
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
