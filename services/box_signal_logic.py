"""Shared box_lab signal logic.

Used by both generate_box_signals.py (live) and backtest_box_pullback.py (backtest).
All functions are stateless — no DB, no logging side effects.
"""

from __future__ import annotations

from statistics import mean
from typing import Any

DEFAULTS: dict[str, Any] = {
    "entry_mode": "normal",
    "min_price": 1000.0,
    "min_turnover_value": 1_000_000_000.0,
    "ideal_box_width_pct": 12.0,
    "watch_box_width_min_pct": 5.0,
    "watch_box_width_max_pct": 25.0,
    "watch_rsi_min": 30.0,
    "watch_rsi_hard_max": 70.0,
    "watch_atr_max_pct": 6.0,
    "watch_volume_ratio_min": 0.5,
    "watch_min_bounce_count": 2,
    "signal_box_position_pct": 45.0,
    "signal_box_position_max_pct": 45.0,
    "max_pending_days": 5,
    "signal_strong_position_pct": 20.0,
    "signal_rsi_min": 35.0,
    "signal_rsi_cool_max": 55.0,
    "signal_rsi_hard_max": 70.0,
    "signal_atr_max_pct": 5.0,
    "signal_volume_ratio_min": 0.7,
    "volume_ratio_warning_max": 3.0,
    "min_equity_ratio": 30.0,
    "max_per": 40.0,
    "max_pbr": 5.0,
    "gu_skip_pct": 3.0,
    "gd_skip_pct": 5.0,
    "max_margin_ratio_hard": 100.0,
    "margin_ratio_warning": 30.0,
    "support_lookback_days": 120,
    "support_zone_low_pct": -1.5,
    "support_zone_high_pct": 2.5,
    "support_rebound_days": 5,
    "support_rebound_pct": 3.0,
    "support_min_touch_count": 3,
    "support_max_break_count": 1,
    "support_watch_distance_pct": 12.0,
    "support_signal_distance_pct": 6.0,
    "support_watch_rsi_min": 35.0,
    "support_watch_rsi_max": 70.0,
    "support_signal_rsi_min": 40.0,
    "support_signal_rsi_max": 65.0,
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1", "yes"}


def _derived(row: dict) -> dict:
    close = _to_float(row.get("close"))
    atr14 = _to_float(row.get("atr14"))
    turnover = _to_float(row.get("turnover_value"))
    volume = _to_float(row.get("volume"))
    if turnover is None and close is not None and volume is not None:
        turnover = close * volume
    return {
        "close": close,
        "turnover_value": turnover,
        "atr_pct": atr14 / close * 100.0 if atr14 is not None and close else None,
    }


def _equity_ratio(row: dict) -> float | None:
    for key in ("equity_ratio", "equity_ratio_pct", "self_capital_ratio", "capital_adequacy_ratio"):
        value = _to_float(row.get(key))
        if value is not None:
            return value
    return None


def _box_metrics(history: list[dict], window: int) -> dict | None:
    rows = [r for r in history if _to_float(r.get("high")) is not None and _to_float(r.get("low")) is not None]
    if len(rows) < min(80, window):
        return None
    rows = rows[-window:]
    highs = [_to_float(r.get("high")) for r in rows]
    lows = [_to_float(r.get("low")) for r in rows]
    closes = [_to_float(r.get("close")) for r in rows if _to_float(r.get("close")) is not None]
    if not highs or not lows or not closes:
        return None
    box_high = max(v for v in highs if v is not None)
    box_low = min(v for v in lows if v is not None)
    if not box_low or box_high <= box_low:
        return None
    width_pct = (box_high - box_low) / box_low * 100.0
    close = closes[-1]
    position_pct = (close - box_low) / (box_high - box_low) * 100.0
    lower_band = box_low + (box_high - box_low) * 0.30
    bounce_count = 0
    was_near_low = False
    for row in rows:
        low = _to_float(row.get("low"))
        close_i = _to_float(row.get("close"))
        if low is None or close_i is None:
            continue
        near_low = low <= lower_band
        if was_near_low and not near_low and close_i > lower_band:
            bounce_count += 1
        was_near_low = near_low
    return {
        "box_high": box_high,
        "box_low": box_low,
        "box_width_pct": width_pct,
        "box_position_pct": position_pct,
        "box_days": len(rows),
        "bounce_count": bounce_count,
        "avg_close": mean(closes),
    }


def _support_events(rows: list[dict], support_line: float, cfg: dict) -> tuple[list[dict], int]:
    zone_high = support_line * (1.0 + float(cfg.get("support_zone_high_pct", 2.5)) / 100.0)
    rebound_days = int(cfg.get("support_rebound_days", 5) or 5)
    rebound_pct = float(cfg.get("support_rebound_pct", 3.0) or 3.0)
    events: list[dict] = []
    break_count = 0
    last_event_i = -999
    in_zone_cluster = False

    for i, row in enumerate(rows):
        low = _to_float(row.get("low"))
        close = _to_float(row.get("close"))
        if low is None or close is None:
            continue
        if close < support_line * 0.97:
            break_count += 1

        near_support = low <= zone_high and close >= support_line
        if not near_support:
            in_zone_cluster = False
            continue
        if in_zone_cluster:
            continue
        in_zone_cluster = True
        if i - last_event_i < rebound_days:
            continue

        future = rows[i + 1 : i + 1 + rebound_days]
        future_closes = [_to_float(r.get("close")) for r in future]
        future_closes = [v for v in future_closes if v is not None]
        if not future_closes:
            continue
        future_max_close = max(future_closes)
        if future_max_close < support_line * (1.0 + rebound_pct / 100.0):
            continue

        base = close if close > 0 else support_line
        events.append(
            {
                "date": row.get("trade_date"),
                "price": low,
                "rebound_pct": (future_max_close - base) / base * 100.0 if base else None,
            }
        )
        last_event_i = i
    return events, break_count


def _support_candidate_lines(rows: list[dict]) -> list[tuple[str, float]]:
    lines: list[tuple[str, float]] = []
    lows = [_to_float(r.get("low")) for r in rows]
    lows = [v for v in lows if v is not None and v > 0]
    if lows:
        sorted_lows = sorted(lows)
        idx = max(0, min(len(sorted_lows) - 1, int(len(sorted_lows) * 0.15)))
        lines.append(("major_low_120d", sorted_lows[idx]))
    ma25 = _to_float(rows[-1].get("ma25")) if rows else None
    ma75 = _to_float(rows[-1].get("ma75")) if rows else None
    if ma25 and ma25 > 0:
        lines.append(("ma25", ma25))
    if ma75 and ma75 > 0:
        lines.append(("ma75", ma75))
    return lines


def _support_metrics(history: list[dict], cfg: dict) -> dict | None:
    rows = [
        r
        for r in history
        if _to_float(r.get("low")) is not None and _to_float(r.get("close")) is not None
    ]
    lookback = int(cfg.get("support_lookback_days", 120) or 120)
    if len(rows) < min(80, lookback):
        return None
    rows = rows[-lookback:]
    close = _to_float(rows[-1].get("close"))
    ma75 = _to_float(rows[-1].get("ma75"))
    ma75_past = _to_float(rows[-21].get("ma75")) if len(rows) >= 21 else None
    if close is None or ma75 is None or close <= ma75:
        return None
    if ma75_past is not None and ma75 <= ma75_past:
        return None

    best: dict | None = None
    for source, support_line in _support_candidate_lines(rows):
        if not support_line or support_line <= 0:
            continue
        events, break_count = _support_events(rows, support_line, cfg)
        distance_pct = (close - support_line) / support_line * 100.0
        avg_bounce = mean([e["rebound_pct"] for e in events if e.get("rebound_pct") is not None]) if events else None
        recent_high_60d = max(
            v
            for v in (_to_float(r.get("high")) for r in rows[-60:])
            if v is not None
        )
        score_key = (
            len(events) * 10
            - break_count * 8
            - max(distance_pct, 0) * 0.7
            + (5 if source in {"ma25", "ma75"} else 0)
        )
        candidate = {
            "support_source": source,
            "support_line": support_line,
            "support_zone_low": support_line * 0.985,
            "support_zone_high": support_line * 1.025,
            "support_touch_count": len(events),
            "support_break_count": break_count,
            "support_distance_pct": distance_pct,
            "avg_bounce_return_pct": avg_bounce,
            "support_points": events[-10:],
            "recent_high_60d": recent_high_60d,
            "ma75_slope_positive": ma75_past is None or ma75 > ma75_past,
            "_score_key": score_key,
        }
        if best is None or score_key > best.get("_score_key", -999999):
            best = candidate
    if not best:
        return None
    best.pop("_score_key", None)
    return best


def _quality_warnings(row: dict, metrics: dict, cfg: dict) -> list[str]:
    warnings: list[str] = []
    d = _derived(row)
    if d["atr_pct"] is None:
        warnings.append("ATR未取得")
    if _equity_ratio(row) is None:
        warnings.append("自己資本比率未取得")
    if metrics["box_position_pct"] <= cfg["signal_strong_position_pct"] and metrics["bounce_count"] < 2:
        warnings.append("下限反発回数が少なく、ただの下落途中の可能性があります")
    return warnings


def _watch_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons: list[str] = []
    d = _derived(row)
    close = d["close"]
    if close is None or close < cfg["min_price"]:
        reasons.append("price_below_min")
    if d["turnover_value"] is None or d["turnover_value"] < cfg["min_turnover_value"]:
        reasons.append("turnover_below_min")
    ma75 = _to_float(row.get("ma75"))
    if ma75 is None or close is None or close <= ma75:
        reasons.append("below_ma75")
    width = metrics["box_width_pct"]
    if not (cfg["watch_box_width_min_pct"] <= width <= cfg["watch_box_width_max_pct"]):
        reasons.append("watch_box_width_out_of_range")
    if metrics["bounce_count"] < cfg["watch_min_bounce_count"]:
        reasons.append("bounce_count_low")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["watch_rsi_min"] or rsi >= cfg["watch_rsi_hard_max"]:
        reasons.append("watch_rsi_out_of_range")
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["watch_atr_max_pct"]:
        reasons.append("watch_atr_too_high")
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is None or volume_ratio < cfg["watch_volume_ratio_min"]:
        reasons.append("watch_volume_too_low")
    if _to_bool(row.get("is_deficit")):
        reasons.append("deficit")
    equity_ratio = _equity_ratio(row)
    if equity_ratio is not None and equity_ratio < cfg["min_equity_ratio"]:
        reasons.append("equity_ratio_low")
    per = _to_float(row.get("per"))
    if per is not None and (per <= 0 or per > cfg["max_per"]):
        reasons.append("per_outlier")
    pbr = _to_float(row.get("pbr"))
    if pbr is not None and (pbr <= 0 or pbr > cfg["max_pbr"]):
        reasons.append("pbr_outlier")
    margin_ratio = _to_float(row.get("margin_ratio"))
    if margin_ratio is not None and margin_ratio > cfg["max_margin_ratio_hard"]:
        reasons.append("margin_ratio_too_high")
    return reasons


def _signal_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons = _watch_rejects(row, metrics, cfg)
    pos = metrics["box_position_pct"]
    signal_position_max = cfg.get("signal_box_position_pct", cfg.get("signal_box_position_max_pct", 45.0))
    if not (0 <= pos <= signal_position_max):
        reasons.append("not_near_box_low")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["signal_rsi_min"] or rsi >= cfg["signal_rsi_hard_max"]:
        reasons.append("signal_rsi_out_of_range")
    d = _derived(row)
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["signal_atr_max_pct"]:
        reasons.append("signal_atr_too_high")
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is None or volume_ratio < cfg["signal_volume_ratio_min"]:
        reasons.append("signal_volume_too_low")
    return reasons


def _score(row: dict, metrics: dict, cfg: dict, *, signal: bool) -> tuple[float, list[str], list[str]]:
    # Score breakdown (max 100):
    # 長期トレンド:18  bounce:14  box下限接近:28  RSI:10  流動性:10  ファンダ:10  信用倍率:10
    reasons = ["長期上昇中", "6か月レンジ継続"]
    warnings: list[str] = _quality_warnings(row, metrics, cfg)
    score = 0.0

    close = _to_float(row.get("close"), 0.0) or 0.0
    ma75 = _to_float(row.get("ma75"))
    if ma75 and close > ma75:
        score += 18

    if metrics["bounce_count"] >= 3:
        score += 14
        reasons.append("下限意識強め")
    elif metrics["bounce_count"] >= 2:
        score += 9
        reasons.append("下限候補")

    pos = metrics["box_position_pct"]
    if signal:
        if 0 <= pos <= cfg["signal_strong_position_pct"]:
            score += 28
            reasons.append("box下限強接近")
        elif 0 <= pos <= cfg.get("signal_box_position_pct", cfg.get("signal_box_position_max_pct", 45.0)):
            score += 20
            reasons.append("box下限接近")
    else:
        if 0 <= pos <= 35:
            score += 28
            reasons.append("下限圏")
        elif 35 < pos <= 70:
            score += 14
            reasons.append("レンジ中央")
        else:
            score += 5
            warnings.append("現在は上限寄り")

    rsi = _to_float(row.get("rsi14"))
    if rsi is not None:
        if 40 <= rsi <= 50:
            score += 10
            reasons.append("RSI冷却")
        elif cfg["signal_rsi_min"] <= rsi <= cfg["signal_rsi_cool_max"]:
            score += 7
            reasons.append("RSI冷却")
        elif cfg["signal_rsi_cool_max"] < rsi < cfg["signal_rsi_hard_max"]:
            score += 3
            warnings.append("RSIやや高め")

    turnover = _derived(row)["turnover_value"]
    if turnover and turnover >= cfg["min_turnover_value"] * 2:
        score += 10
    elif turnover and turnover >= cfg["min_turnover_value"]:
        score += 7
    volume_ratio = _to_float(row.get("volume_ratio_20d"))
    if volume_ratio is not None and volume_ratio > cfg["volume_ratio_warning_max"]:
        warnings.append("出来高急増")

    fund = 0.0
    if not _to_bool(row.get("is_deficit")):
        fund += 5
    per = _to_float(row.get("per"))
    pbr = _to_float(row.get("pbr"))
    if per is not None and 0 < per <= cfg["max_per"]:
        fund += 3
    elif per is None:
        warnings.append("PER未取得")
    if pbr is not None and 0 < pbr <= cfg["max_pbr"]:
        fund += 3
    elif pbr is None:
        warnings.append("PBR未取得")
    equity_ratio = _equity_ratio(row)
    if equity_ratio is not None and equity_ratio >= cfg["min_equity_ratio"]:
        fund += 4
    score += min(fund, 10.0)

    margin_ratio = _to_float(row.get("margin_ratio"))
    if margin_ratio is None:
        score += 4
        warnings.append("信用倍率未取得")
    elif margin_ratio <= 0:
        warnings.append("信用倍率異常値")
    elif margin_ratio <= 1:
        score += 8
        reasons.append("信用倍率低位")
    elif margin_ratio <= 5:
        score += 10
        reasons.append("信用倍率良好")
    elif margin_ratio <= 15:
        score += 8
        reasons.append("信用倍率許容")
    elif margin_ratio <= cfg["margin_ratio_warning"]:
        score += 4
        warnings.append("信用倍率やや高め")
    elif margin_ratio <= 50:
        score += 1
        warnings.append("信用倍率高め")
    else:
        warnings.append("信用倍率過熱")

    return min(score, 100.0), reasons, warnings


def _support_watch_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons: list[str] = []
    d = _derived(row)
    close = d["close"]
    if close is None or close < cfg["min_price"]:
        reasons.append("support_price_below_min")
    if d["turnover_value"] is None or d["turnover_value"] < cfg["min_turnover_value"]:
        reasons.append("support_turnover_below_min")
    if metrics["support_touch_count"] < cfg["support_min_touch_count"]:
        reasons.append("support_touch_count_low")
    if metrics["support_break_count"] > cfg["support_max_break_count"]:
        reasons.append("support_break_count_high")
    distance = metrics["support_distance_pct"]
    if not (0 <= distance <= cfg["support_watch_distance_pct"]):
        reasons.append("support_distance_too_far")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["support_watch_rsi_min"] or rsi >= cfg["support_watch_rsi_max"]:
        reasons.append("support_rsi_out_of_range")
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["watch_atr_max_pct"]:
        reasons.append("support_atr_too_high")
    if _to_bool(row.get("is_deficit")):
        reasons.append("support_deficit")
    equity_ratio = _equity_ratio(row)
    if equity_ratio is not None and equity_ratio < cfg["min_equity_ratio"]:
        reasons.append("support_equity_ratio_low")
    per = _to_float(row.get("per"))
    if per is not None and (per <= 0 or per > cfg["max_per"]):
        reasons.append("support_per_outlier")
    pbr = _to_float(row.get("pbr"))
    if pbr is not None and (pbr <= 0 or pbr > cfg["max_pbr"]):
        reasons.append("support_pbr_outlier")
    margin_ratio = _to_float(row.get("margin_ratio"))
    if margin_ratio is not None and margin_ratio > cfg["max_margin_ratio_hard"]:
        reasons.append("support_margin_ratio_too_high")
    return reasons


def _support_signal_rejects(row: dict, metrics: dict, cfg: dict) -> list[str]:
    reasons = _support_watch_rejects(row, metrics, cfg)
    distance = metrics["support_distance_pct"]
    if not (0 <= distance <= cfg["support_signal_distance_pct"]):
        reasons.append("support_signal_distance_too_far")
    rsi = _to_float(row.get("rsi14"))
    if rsi is None or rsi < cfg["support_signal_rsi_min"] or rsi >= cfg["support_signal_rsi_max"]:
        reasons.append("support_signal_rsi_out_of_range")
    d = _derived(row)
    if d["atr_pct"] is not None and d["atr_pct"] > cfg["signal_atr_max_pct"]:
        reasons.append("support_signal_atr_too_high")
    return reasons


def _support_score(row: dict, metrics: dict, cfg: dict, *, signal: bool) -> tuple[float, list[str], list[str]]:
    reasons = ["支持線反発", "長期上昇中"]
    warnings = _quality_warnings(row, {"box_position_pct": 999, "bounce_count": metrics["support_touch_count"]}, cfg)
    score = 0.0
    close = _to_float(row.get("close"), 0.0) or 0.0
    ma75 = _to_float(row.get("ma75"))
    if ma75 and close > ma75 and metrics.get("ma75_slope_positive"):
        score += 20
        reasons.append("MA75上昇")

    touches = int(metrics["support_touch_count"] or 0)
    if touches >= 5:
        score += 25
        reasons.append("支持線反発5回以上")
    elif touches == 4:
        score += 20
        reasons.append("支持線反発4回")
    elif touches >= 3:
        score += 15
        reasons.append("支持線反発3回")

    breaks = int(metrics["support_break_count"] or 0)
    if breaks == 0:
        score += 15
        reasons.append("支持線割れなし")
    elif breaks == 1:
        score += 7
        warnings.append("支持線割れ1回")

    distance = metrics["support_distance_pct"]
    if 0 <= distance <= 2:
        score += 20
        reasons.append("支持線至近")
    elif 2 < distance <= 4:
        score += 15
        reasons.append("支持線近辺")
    elif 4 < distance <= cfg["support_signal_distance_pct" if signal else "support_watch_distance_pct"]:
        score += 10
        reasons.append("支持線圏")

    rsi = _to_float(row.get("rsi14"))
    if rsi is not None:
        if 45 <= rsi <= 58:
            score += 10
            reasons.append("RSI適温")
        elif cfg["support_signal_rsi_min"] <= rsi < cfg["support_signal_rsi_max"]:
            score += 6
        elif cfg["support_watch_rsi_min"] <= rsi < cfg["support_watch_rsi_max"]:
            score += 3

    turnover = _derived(row)["turnover_value"]
    if turnover and turnover >= cfg["min_turnover_value"]:
        score += 5

    margin_ratio = _to_float(row.get("margin_ratio"))
    if margin_ratio is None:
        score += 2
        warnings.append("信用倍率未取得")
    elif 0 < margin_ratio <= 5:
        score += 5
        reasons.append("信用倍率良好")
    elif margin_ratio <= cfg["margin_ratio_warning"]:
        score += 3
    else:
        warnings.append("信用倍率高め")

    return min(score, 100.0), reasons, warnings
