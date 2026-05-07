"""Market environment adjustment for rebound signals."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_NIKKEI_PCT_KEYS = (
    "nikkei_pct", "nikkei_change_pct", "nikkei_return_pct",
    "nikkei_return", "nikkei_daily_return",
)
_TOPIX_PCT_KEYS = (
    "topix_pct", "topix_change_pct", "topix_return_pct",
    "topix_return", "topix_daily_return",
)
_NIKKEI_YEN_KEYS = (
    "nikkei_change_yen", "nikkei_change_value", "nikkei_change",
    "nikkei_diff", "nikkei_delta",
)


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _pick_raw(data: dict, keys: tuple[str, ...]) -> Any:
    """Return the first non-None raw value among keys (no conversion)."""
    for key in keys:
        v = data.get(key)
        if v is not None:
            return v
    return None


def _normalize_pct(value: float, is_decimal: bool) -> float:
    """Convert value to percent. is_decimal=True: multiply by 100. is_decimal=False: use as-is."""
    result = value * 100.0 if is_decimal else float(value)
    logger.info("[normalize_pct] value=%s is_decimal=%s result=%s", value, is_decimal, result)
    return result


def _pick_pct(data: dict, keys: tuple[str, ...], *, is_decimal: bool = False) -> float | None:
    """Return first non-None value among keys, converting via _normalize_pct."""
    for key in keys:
        v = _to_float(data.get(key), None)
        if v is not None:
            return _normalize_pct(v, is_decimal)
    return None


def _pick_yen(data: dict, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        v = _to_float(data.get(key), None)
        if v is not None:
            return v
    return None


def _used(nikkei_pct: float | None, topix_pct: float | None, nikkei_change_yen: float | None) -> dict:
    return {
        "nikkei_pct_used": nikkei_pct,
        "topix_pct_used": topix_pct,
        "nikkei_change_yen_used": nikkei_change_yen,
    }


def evaluate_market_regime(market_data: dict | None) -> dict:
    data = market_data or {}

    nikkei_raw = _pick_raw(data, _NIKKEI_PCT_KEYS)
    topix_raw = _pick_raw(data, _TOPIX_PCT_KEYS)
    nikkei_yen_raw = _pick_raw(data, _NIKKEI_YEN_KEYS)
    logger.info(
        "[market_regime_input_raw] nikkei_raw=%s topix_raw=%s nikkei_change_yen_raw=%s source=%s",
        nikkei_raw, topix_raw, nikkei_yen_raw, data,
    )

    nikkei_pct = _pick_pct(data, _NIKKEI_PCT_KEYS)
    topix_pct = _pick_pct(data, _TOPIX_PCT_KEYS)
    nikkei_change_yen = _pick_yen(data, _NIKKEI_YEN_KEYS)
    decliners_ratio = _to_float(data.get("decliners_ratio"), None)
    logger.info(
        "[market_regime_input_norm] nikkei_pct=%s topix_pct=%s nikkei_change_yen=%s",
        nikkei_pct, topix_pct, nikkei_change_yen,
    )

    reasons: list[str] = []
    if nikkei_pct is not None:
        reasons.append(f"日経平均 {nikkei_pct:+.1f}%")
    if nikkei_change_yen is not None:
        reasons.append(f"日経平均 {nikkei_change_yen:+.0f}円")
    if topix_pct is not None:
        reasons.append(f"TOPIX {topix_pct:+.1f}%")
    if decliners_ratio is not None:
        reasons.append(f"値下がり比率 {decliners_ratio:.0%}")

    if (
        (nikkei_pct is not None and nikkei_pct <= -5.0)
        or (nikkei_change_yen is not None and nikkei_change_yen <= -2000)
        or (topix_pct is not None and topix_pct <= -4.0)
    ):
        r = {
            "regime": "panic_selloff",
            "label": "パニック下落",
            "ai_threshold_adjust": 0.10,
            "entry_size_multiplier": 0.0,
            "reason": "、".join(reasons) or "急落条件に該当",
            **_used(nikkei_pct, topix_pct, nikkei_change_yen),
        }
        logger.info("[market_regime] regime=%s adjust=%.2f reason=%s", r["regime"], r["ai_threshold_adjust"], r["reason"])
        return r

    if (
        (nikkei_pct is not None and nikkei_pct >= 5.0)
        or (nikkei_change_yen is not None and nikkei_change_yen >= 2000)
        or (topix_pct is not None and topix_pct >= 4.0)
    ):
        r = {
            "regime": "panic_rebound",
            "label": "異常急反発",
            "ai_threshold_adjust": 0.10,
            "entry_size_multiplier": 0.5,
            "reason": "、".join(reasons) or "急反発条件に該当",
            **_used(nikkei_pct, topix_pct, nikkei_change_yen),
        }
        logger.info("[market_regime] regime=%s adjust=%.2f reason=%s", r["regime"], r["ai_threshold_adjust"], r["reason"])
        return r

    if (
        (nikkei_pct is not None and nikkei_pct <= -2.0)
        or (topix_pct is not None and topix_pct <= -1.5)
    ):
        r = {
            "regime": "risk_off",
            "label": "弱地合い",
            "ai_threshold_adjust": 0.05,
            "entry_size_multiplier": 1.0,
            "reason": "、".join(reasons) or "弱地合い条件に該当",
            **_used(nikkei_pct, topix_pct, nikkei_change_yen),
        }
        logger.info("[market_regime] regime=%s adjust=%.2f reason=%s", r["regime"], r["ai_threshold_adjust"], r["reason"])
        return r

    if (
        (nikkei_pct is not None and nikkei_pct >= 2.0)
        or (topix_pct is not None and topix_pct >= 1.5)
    ):
        r = {
            "regime": "strong_risk_on",
            "label": "強リスクオン",
            "ai_threshold_adjust": 0.05,
            "entry_size_multiplier": 1.0,
            "reason": "、".join(reasons) or "強リスクオン条件に該当",
            **_used(nikkei_pct, topix_pct, nikkei_change_yen),
        }
        logger.info("[market_regime] regime=%s adjust=%.2f reason=%s", r["regime"], r["ai_threshold_adjust"], r["reason"])
        return r

    r = {
        "regime": "normal",
        "label": "通常",
        "ai_threshold_adjust": 0.0,
        "entry_size_multiplier": 1.0,
        "reason": "、".join(reasons) or "通常地合い",
        **_used(nikkei_pct, topix_pct, nikkei_change_yen),
    }
    logger.info("[market_regime] regime=%s adjust=%.2f reason=%s", r["regime"], r["ai_threshold_adjust"], r["reason"])
    return r
