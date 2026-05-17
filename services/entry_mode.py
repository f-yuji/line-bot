"""Entry-mode helpers for rebound virtual entries.

This module is read-only and side-effect free. It only classifies the current
market context and entry candidates so callers can decide whether to create a
virtual trade.
"""

from __future__ import annotations

from typing import Any

ENTRY_MODES = {"auto", "normal", "risk_on_pullback", "panic_deep_rebound", "paused"}

ENTRY_MODE_LABELS = {
    "auto": "自動判定",
    "normal": "normal標準型",
    "risk_on_pullback": "risk_on押し目型",
    "panic_deep_rebound": "panic深リバ型",
    "paused": "新規停止",
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def ma_gap_pct(row: dict, ma_key: str = "ma5") -> float | None:
    direct = _to_float(row.get(f"{ma_key}_gap_pct"))
    if direct is not None:
        return direct
    close = _to_float(row.get("close"))
    ma = _to_float(row.get(ma_key))
    if close is None or ma is None or ma == 0:
        return None
    return (close - ma) / ma * 100.0


def classify_entry_case(row: dict) -> str:
    ma5_gap = ma_gap_pct(row, "ma5")
    drop = _to_float(row.get("drop_pct") if row.get("drop_pct") is not None else row.get("day_change_pct"))
    if ma5_gap is None or drop is None:
        return "unknown"
    if ma5_gap >= 0 and -5.0 <= drop <= -3.0:
        return "ma5_upper_shallow_pullback"
    if ma5_gap >= 0 and drop < -5.0:
        return "ma5_upper_deep_drop"
    if ma5_gap < 0 and drop <= -8.0:
        return "ma5_lower_deep_rebound"
    if ma5_gap < 0 and drop > -8.0:
        return "ma5_lower_shallow_weak"
    return "unknown"


def recommended_entry_mode(market_regime: str | None) -> str:
    regime = str(market_regime or "normal")
    if regime in {"strong_risk_on", "risk_on"}:
        return "risk_on_pullback"
    if regime in {"panic_rebound", "panic_selloff"}:
        return "panic_deep_rebound"
    if regime == "risk_off":
        return "paused"
    return "normal"


def resolve_entry_mode(settings: dict | None, market_adjustment: dict | None) -> dict:
    configured = str((settings or {}).get("entry_mode") or "normal")
    if configured not in ENTRY_MODES:
        configured = "normal"
    regime = str((market_adjustment or {}).get("regime") or "normal")
    recommended = recommended_entry_mode(regime)
    if recommended == "normal":
        nikkei = _to_float((market_adjustment or {}).get("nikkei_pct"))
        topix = _to_float((market_adjustment or {}).get("topix_pct"))
        if (nikkei is not None and nikkei >= 0.75) or (topix is not None and topix >= 0.5):
            recommended = "risk_on_pullback"
    effective = recommended if configured == "auto" else configured
    return {
        "configured": configured,
        "effective": effective,
        "recommended": recommended,
        "regime": regime,
        "configured_label": ENTRY_MODE_LABELS.get(configured, configured),
        "effective_label": ENTRY_MODE_LABELS.get(effective, effective),
        "recommended_label": ENTRY_MODE_LABELS.get(recommended, recommended),
    }


def entry_mode_filter(row: dict, effective_mode: str) -> tuple[bool, str | None, dict]:
    ma5_gap = ma_gap_pct(row, "ma5")
    ma25_gap = ma_gap_pct(row, "ma25")
    ma75_gap = ma_gap_pct(row, "ma75")
    drop = _to_float(row.get("drop_pct") if row.get("drop_pct") is not None else row.get("day_change_pct"))
    entry_case = classify_entry_case(row)
    meta = {
        "entry_ma5_gap_pct": round(ma5_gap, 4) if ma5_gap is not None else None,
        "entry_ma25_gap_pct": round(ma25_gap, 4) if ma25_gap is not None else None,
        "entry_ma75_gap_pct": round(ma75_gap, 4) if ma75_gap is not None else None,
        "entry_case": entry_case,
    }

    if effective_mode in {"normal", "auto"}:
        return True, None, meta
    if effective_mode == "paused":
        return False, "entry_mode_paused", meta
    if ma5_gap is None or drop is None:
        return False, f"entry_mode_{effective_mode}_filter", meta
    if effective_mode == "risk_on_pullback":
        ok = ma5_gap >= 0 and -5.0 <= drop <= -3.0
        return ok, None if ok else "entry_mode_risk_on_pullback_filter", meta
    if effective_mode == "panic_deep_rebound":
        ok = ma5_gap < 0 and drop <= -8.0
        return ok, None if ok else "entry_mode_panic_deep_rebound_filter", meta
    return True, None, meta


def regime_scores(market_adjustment: dict | None) -> dict:
    ctx = market_adjustment or {}
    regime = str(ctx.get("regime") or "normal")
    nikkei = _to_float(ctx.get("nikkei_pct"))
    topix = _to_float(ctx.get("topix_pct"))
    pct = max([v for v in [nikkei, topix] if v is not None], default=0.0)
    neg = min([v for v in [nikkei, topix] if v is not None], default=0.0)

    scores = {"risk_on": 25, "normal": 55, "risk_off": 20, "panic": 10}
    if regime == "strong_risk_on":
        scores.update({"risk_on": 85, "normal": 40, "risk_off": 10, "panic": 5})
    elif regime == "risk_on":
        scores.update({"risk_on": 72, "normal": 48, "risk_off": 15, "panic": 8})
    elif regime == "risk_off":
        scores.update({"risk_on": 15, "normal": 35, "risk_off": 72, "panic": 30})
    elif regime == "panic_selloff":
        scores.update({"risk_on": 5, "normal": 15, "risk_off": 80, "panic": 92})
    elif regime == "panic_rebound":
        scores.update({"risk_on": 60, "normal": 25, "risk_off": 35, "panic": 78})
    else:
        if pct >= 0.75:
            scores["risk_on"] = min(70, 45 + int(pct * 15))
        if neg <= -0.75:
            scores["risk_off"] = min(70, 35 + int(abs(neg) * 18))
    return scores
