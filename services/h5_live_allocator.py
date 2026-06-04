"""Balanced LIVE allocation for H5 comparison candidates.

This module only decides which already-scored candidates receive the LIVE
display flag. It does not write actual trades or auto orders.
"""

from __future__ import annotations

import math
from typing import Any

from services.h5_primary import (
    H5_BASE_RULES,
    H5_LIVE_LIMITED_CASE_KEY,
    H5_RESEARCH_CASE_KEY,
    h5_overheat_score,
)

CURRENT_H5_CORE_BUCKET = "current_h5_core"
SHORT_PULLBACK_BUCKET = "short_pullback"
TREND_SUPPORT_BUCKET = "trend_support"

SHORT_PULLBACK_CASE_KEY = "H5_short_pullback_drop5_m3"
TREND_SUPPORT_CASE_KEY = "trend_support_best"

LIVE_ALLOCATION_MODE = "balanced_cases"
LIVE_ALLOCATION_BUCKETS: list[tuple[str, int]] = [
    (CURRENT_H5_CORE_BUCKET, 4),
    (SHORT_PULLBACK_BUCKET, 3),
    (TREND_SUPPORT_BUCKET, 3),
]
LIVE_MAX_DAILY_CANDIDATES = sum(limit for _bucket, limit in LIVE_ALLOCATION_BUCKETS)

TREND_SUPPORT_PRIORITY_CASES = [
    "tf_166745_trend_rs_mom_market",
    "tf_22709_trend_ma25_mom_market",
    "tf_51545_trend_ma25_ma75_mom_market",
]


def _float(value: Any) -> float | None:
    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _score(row: dict[str, Any]) -> float | None:
    return _float(
        row.get("signal_probability")
        if row.get("signal_probability") is not None
        else row.get("probability")
        if row.get("probability") is not None
        else row.get("entry_probability")
    )


def _stage_ok(row: dict[str, Any]) -> bool:
    return str(row.get("signal_stage") or "") in set(H5_BASE_RULES["allowed_stages"])


def _not_panic(row: dict[str, Any]) -> bool:
    return str(row.get("market_regime") or "") != "panic_selloff"


def _margin_ok(row: dict[str, Any]) -> bool:
    margin = _float(row.get("margin_ratio"))
    if margin is None:
        return True
    return float(H5_BASE_RULES["min_margin_ratio"]) <= margin <= float(H5_BASE_RULES["max_margin_ratio"])


def _gap_ok(row: dict[str, Any], limit: float = 3.0) -> bool:
    gap = _float(
        row.get("gap")
        if row.get("gap") is not None
        else row.get("gap_pct")
        if row.get("gap_pct") is not None
        else row.get("entry_gap_pct")
        if row.get("entry_gap_pct") is not None
        else row.get("open_gap_pct")
    )
    return gap is None or gap <= limit


def _overheat(row: dict[str, Any]) -> int:
    raw = _float(row.get("entry_overheat_score") if row.get("entry_overheat_score") is not None else row.get("overheat_score"))
    if raw is not None:
        return int(raw)
    return h5_overheat_score(row)


def _drop(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float(row.get(key))
        if value is not None:
            return value
    return None


def _base_ai_stage_filters(row: dict[str, Any], min_score: float = 0.65) -> list[str]:
    reasons: list[str] = []
    score = _score(row)
    if score is None or score < min_score:
        reasons.append("ai_score_below_threshold")
    if not _stage_ok(row):
        reasons.append("stage_not_confirmed")
    if not _not_panic(row):
        reasons.append("panic_selloff")
    if not _margin_ok(row):
        reasons.append("margin_filter")
    if not _gap_ok(row):
        reasons.append("gap_gt_3")
    return reasons


def current_h5_core_reasons(row: dict[str, Any]) -> list[str]:
    reasons = _base_ai_stage_filters(row, 0.65)
    drop20 = _drop(row, "drop20", "drop_from_20d_high_pct", "entry_drop_from_20d_high_pct")
    if drop20 is None or drop20 > -8.0:
        reasons.append("drop20_gt_m8")
    if _overheat(row) > 1:
        reasons.append("overheat_gt_1")
    return reasons


def short_pullback_reasons(row: dict[str, Any]) -> list[str]:
    reasons = _base_ai_stage_filters(row, 0.65)
    drop5 = _drop(row, "drop5", "drop_from_5d_high_pct")
    if drop5 is None or drop5 > -3.0:
        reasons.append("drop5_gt_m3")
    if _overheat(row) > 1:
        reasons.append("overheat_gt_1")
    return reasons


def _flag_true(row: dict[str, Any], key: str) -> bool | None:
    if key not in row or row.get(key) is None:
        return None
    value = row.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _gt(row: dict[str, Any], a: str, b: str) -> bool | None:
    av = _float(row.get(a))
    bv = _float(row.get(b))
    if av is None or bv is None:
        return None
    return av > bv


def _positive(row: dict[str, Any], key: str) -> bool | None:
    value = _float(row.get(key))
    if value is None:
        return None
    return value > 0


def _rsi_not_too_hot(row: dict[str, Any], limit: float = 75.0) -> bool | None:
    rsi = _float(row.get("rsi14"))
    if rsi is None:
        return None
    return rsi <= limit


def _market_positive(row: dict[str, Any]) -> bool | None:
    nikkei = _positive(row, "market_nikkei_pct")
    topix = _positive(row, "market_topix_pct")
    if nikkei is None and topix is None:
        return None
    if nikkei is None or topix is None:
        return None
    return bool(nikkei and topix)


def _return5_not_extreme(row: dict[str, Any], limit: float = 8.0) -> bool | None:
    value = _float(row.get("return_5d_pct"))
    if value is None:
        return None
    return value < limit


def _relative_strength_top(row: dict[str, Any]) -> bool | None:
    flag = _flag_true(row, "relative_strength_top")
    if flag is not None:
        return flag
    percentile = _float(row.get("return5_percentile") or row.get("momentum_percentile"))
    if percentile is not None:
        return percentile >= 70.0
    return None


def trend_support_reasons(row: dict[str, Any]) -> tuple[str | None, list[str]]:
    """Return the first matching fixed trend case and reasons if none match."""
    shared: list[str] = []
    score = _score(row)
    if score is None or score < 0.55:
        shared.append("ai_score_below_055")
    if not _stage_ok(row):
        shared.append("stage_not_confirmed")
    if not _not_panic(row):
        shared.append("panic_selloff")
    if not _gap_ok(row):
        shared.append("gap_gt_3")
    if _overheat(row) > 3:
        shared.append("overheat_gt_3")
    if not _margin_ok(row):
        shared.append("margin_filter")
    if shared:
        return None, shared

    close_gt_ma25 = _flag_true(row, "close_gt_ma25")
    if close_gt_ma25 is None:
        close_gt_ma25 = _gt(row, "close", "ma25")
    close_gt_ma75 = _flag_true(row, "close_gt_ma75")
    if close_gt_ma75 is None:
        close_gt_ma75 = _gt(row, "close", "ma75")
    return20_positive = _positive(row, "return_20d_pct")
    if return20_positive is None:
        return20_positive = _positive(row, "return20")
    return10_positive = _positive(row, "return_10d_pct")
    market_positive = _market_positive(row)
    rsi_ok = _rsi_not_too_hot(row)
    return5_ok = _return5_not_extreme(row)
    rs_top = _relative_strength_top(row)

    if score is not None and score >= 0.65 and all(
        v is True for v in [rs_top, close_gt_ma25, market_positive, rsi_ok, return5_ok]
    ):
        return "tf_166745_trend_rs_mom_market", []
    if score is not None and score >= 0.55 and all(
        v is True for v in [close_gt_ma25, market_positive, rsi_ok, return5_ok]
    ):
        return "tf_22709_trend_ma25_mom_market", []
    if score is not None and score >= 0.65 and all(
        v is True for v in [close_gt_ma25, close_gt_ma75, market_positive, rsi_ok, return5_ok]
    ):
        return "tf_51545_trend_ma25_ma75_mom_market", []

    reasons: list[str] = []
    if rs_top is False:
        reasons.append("relative_strength_not_top")
    if close_gt_ma25 is False:
        reasons.append("close_not_gt_ma25")
    elif close_gt_ma25 is None:
        reasons.append("close_gt_ma25_missing")
    if close_gt_ma75 is False:
        reasons.append("close_not_gt_ma75")
    if market_positive is False:
        reasons.append("market_not_positive")
    elif market_positive is None:
        reasons.append("market_trend_missing")
    if rsi_ok is False:
        reasons.append("rsi_too_hot")
    elif rsi_ok is None:
        reasons.append("rsi_missing")
    if return5_ok is False:
        reasons.append("return5_extreme")
    elif return5_ok is None:
        reasons.append("return5_missing")
    if return20_positive is False and return10_positive is False:
        reasons.append("momentum_not_positive")
    return None, reasons or ["trend_definition_missing_or_not_matched"]


def bucket_case_key(bucket: str, trend_case_key: str | None = None) -> str:
    if bucket == CURRENT_H5_CORE_BUCKET:
        return H5_LIVE_LIMITED_CASE_KEY
    if bucket == SHORT_PULLBACK_BUCKET:
        return SHORT_PULLBACK_CASE_KEY
    if bucket == TREND_SUPPORT_BUCKET:
        return trend_case_key or TREND_SUPPORT_CASE_KEY
    return H5_RESEARCH_CASE_KEY


def bucket_case_label(bucket: str, trend_case_key: str | None = None) -> str:
    if bucket == CURRENT_H5_CORE_BUCKET:
        return "LIVE / current_h5_core"
    if bucket == SHORT_PULLBACK_BUCKET:
        return "LIVE / H5_short_pullback_drop5_m3"
    if bucket == TREND_SUPPORT_BUCKET:
        return f"LIVE / {trend_case_key or TREND_SUPPORT_CASE_KEY}"
    return "research"


def allocate_balanced_live_candidates(
    entries: list[dict[str, Any]],
    *,
    sector_counts: dict[str, int] | None = None,
    max_sector_positions: int = 2,
) -> list[dict[str, Any]]:
    """Mutate entry metadata and return entries in original order.

    Each entry must have:
      data: merged feature/result row for filtering
      meta: dict to receive LIVE metadata
      code: optional code override
      sector: optional sector override
    """
    sector_counts = dict(sector_counts or {})
    selected_keys: set[tuple[str, str]] = set()
    selected_count = 0

    for entry in entries:
        meta = entry.setdefault("meta", {})
        data = dict(entry.get("data") or {})
        trend_case, trend_reasons = trend_support_reasons(data)
        bucket_reasons = {
            CURRENT_H5_CORE_BUCKET: current_h5_core_reasons(data),
            SHORT_PULLBACK_BUCKET: short_pullback_reasons(data),
            TREND_SUPPORT_BUCKET: trend_reasons,
        }
        entry["_bucket_reasons"] = bucket_reasons
        entry["_trend_case_key"] = trend_case
        meta.update(
            {
                "position_limit_mode": "research",
                "is_h5_research": True,
                "is_h5_live_limited": False,
                "is_live_candidate": False,
                "is_primary_h5": False,
                "live_allocation_mode": LIVE_ALLOCATION_MODE,
                "live_allocation_bucket": None,
                "allocation_rank": None,
                "selected_rank": None,
                "live_skip_reason": "bucket_not_matched",
                "case_key": H5_RESEARCH_CASE_KEY,
            }
        )

    for bucket, limit in LIVE_ALLOCATION_BUCKETS:
        bucket_selected = 0
        bucket_rank = 0
        for entry in entries:
            if bucket_selected >= limit or selected_count >= LIVE_MAX_DAILY_CANDIDATES:
                break
            data = dict(entry.get("data") or {})
            meta = entry.setdefault("meta", {})
            code = str(entry.get("code") or data.get("code") or "")
            signal_date = str(data.get("trade_date") or data.get("signal_date") or data.get("drop_detected_at") or "")
            if not code:
                continue
            if (signal_date, code) in selected_keys:
                if meta.get("live_skip_reason") == "bucket_not_matched":
                    meta["live_skip_reason"] = "duplicate_across_buckets"
                continue
            reasons = entry.get("_bucket_reasons", {}).get(bucket, ["bucket_not_matched"])
            if bucket == TREND_SUPPORT_BUCKET and not entry.get("_trend_case_key"):
                reasons = reasons or ["definition_missing"]
            if reasons:
                if meta.get("live_skip_reason") == "bucket_not_matched":
                    meta["live_skip_reason"] = ",".join(reasons)
                continue
            sector = str(entry.get("sector") or data.get("sector") or "unknown")
            if max_sector_positions and sector_counts.get(sector, 0) >= max_sector_positions:
                if meta.get("live_skip_reason") == "bucket_not_matched":
                    meta["live_skip_reason"] = "sector_limit"
                continue
            bucket_rank += 1
            bucket_selected += 1
            selected_count += 1
            selected_keys.add((signal_date, code))
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            case_key = bucket_case_key(bucket, entry.get("_trend_case_key"))
            meta.update(
                {
                    "position_limit_mode": LIVE_ALLOCATION_MODE,
                    "is_h5_research": False,
                    "is_h5_live_limited": True,
                    "is_live_candidate": True,
                    "is_primary_h5": bucket == CURRENT_H5_CORE_BUCKET,
                    "live_allocation_bucket": bucket,
                    "allocation_rank": bucket_rank,
                    "selected_rank": selected_count,
                    "live_skip_reason": None,
                    "case_key": case_key,
                    "case_label": bucket_case_label(bucket, entry.get("_trend_case_key")),
                    "live_case_key": case_key,
                    "source_case": case_key,
                    "allocation_bucket": bucket,
                }
            )

    for entry in entries:
        entry.setdefault("meta", {})["h5_selected_count"] = selected_count
        entry.setdefault("meta", {})["h5_candidate_count"] = len(entries)
    return entries
