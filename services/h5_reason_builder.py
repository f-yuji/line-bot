"""Rule-based H5/AI/risk reason builder for trade assist cards.

This module intentionally does not load model artifacts or recompute scores.
It explains the values already present on a trade-assist candidate row.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CACHE_PATH = Path("outputs/trade_assist_reasons/h5_ai_reasons.json")
SHAP_NOTICE = "これはSHAPによる厳密な寄与分解ではなく、候補特徴量に基づく近似説明です。"


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
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _fmt_pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}%"


def _fmt_score(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}"


def _score(row: dict[str, Any]) -> float | None:
    score = _to_float(
        _first(
            row,
            "ai_score",
            "display_probability",
            "signal_probability",
            "entry_probability",
            "ai_probability",
        )
    )
    if score is not None and 1.0 < score <= 100.0:
        return score / 100.0
    return score


def _drop20(row: dict[str, Any]) -> float | None:
    return _to_float(_first(row, "drop_from_20d_high_pct", "drop20", "drop20_pct"))


def _volume_ratio(row: dict[str, Any]) -> float | None:
    return _to_float(_first(row, "volume_ratio_20d", "volume_ratio", "diagnostic_volume_ratio_20d"))


def _overheat(row: dict[str, Any]) -> float | None:
    return _to_float(_first(row, "h5_overheat_score", "overheat_score", "entry_overheat_score"))


def _is_h5_match(row: dict[str, Any]) -> bool:
    explicit = _first(row, "h5_primary_match", "H5_full")
    if explicit is not None:
        return _to_bool(explicit)

    score = _score(row)
    drop20 = _drop20(row)
    stage = str(_first(row, "signal_stage") or "")
    regime = str(_first(row, "market_regime") or "")
    overheat = _overheat(row)
    margin = _to_float(_first(row, "margin_ratio"))
    fallback = _to_bool(_first(row, "score_fallback_used", "fallback_used"))
    return (
        score is not None
        and score >= 0.65
        and drop20 is not None
        and drop20 <= -8.0
        and stage in {"confirmed", "strong_confirmed"}
        and regime != "panic_selloff"
        and (overheat is None or overheat <= 1)
        and (margin is None or 3 <= margin <= 30)
        and not fallback
    )


def build_h5_reason(row: dict[str, Any]) -> dict[str, str]:
    score = _score(row)
    drop20 = _drop20(row)
    stage = str(_first(row, "signal_stage") or "")
    regime = str(_first(row, "market_regime") or "")
    overheat = _overheat(row)
    margin = _to_float(_first(row, "margin_ratio"))
    score_source = str(_first(row, "score_source", "prediction_source") or "")
    model_version = str(_first(row, "model_version") or "")
    fallback = _to_bool(_first(row, "score_fallback_used", "fallback_used"))
    skip_reason = str(_first(row, "h5_skip_reason", "live_skip_reason") or "").strip()

    parts: list[str] = []
    if _is_h5_match(row):
        parts.append("H5_full条件を満たしています。")
    else:
        parts.append("H5_full条件は満たしていません。参考候補として扱います。")
        if skip_reason:
            parts.append(f"対象外理由: {skip_reason}")

    if score is not None:
        if score >= 0.65:
            parts.append(f"AIスコアは{_fmt_score(score)}で、H5基準の0.650以上です。")
        else:
            parts.append(f"AIスコアは{_fmt_score(score)}で、H5基準の0.650未満です。")

    if drop20 is not None:
        if drop20 <= -8.0:
            parts.append(f"20日高値比は{_fmt_pct(drop20)}で、急落条件を満たしています。")
        else:
            parts.append(f"20日高値比は{_fmt_pct(drop20)}で、H5の急落条件には届いていません。")

    if stage:
        if stage in {"confirmed", "strong_confirmed"}:
            parts.append(f"signal_stageは{stage}で、confirmed系の候補です。")
        else:
            parts.append(f"signal_stageは{stage}で、H5_fullでは注意対象です。")

    if regime:
        if regime == "panic_selloff":
            parts.append("market_regimeがpanic_selloffのため、H5条件では除外対象です。")
        else:
            parts.append(f"market_regimeは{regime}で、panic_selloff除外には該当しません。")

    if overheat is not None:
        if overheat <= 1:
            parts.append(f"過熱スコアは{overheat:.0f}で、許容範囲内です。")
        else:
            parts.append(f"過熱スコアは{overheat:.0f}で、H5条件では注意対象です。")

    if margin is not None:
        if 3 <= margin <= 30:
            parts.append(f"信用倍率は{margin:.1f}倍で、3〜30倍のH5許容レンジ内です。")
        else:
            parts.append(f"信用倍率は{margin:.1f}倍で、H5許容レンジ外です。")

    if score_source == "stored_predictions":
        suffix = f" model_version={model_version}" if model_version else ""
        parts.append(f"AIスコアは保存済み予測を使用しています。active_model再スコアではありません。{suffix}".strip())
    elif score_source:
        parts.append(f"score_sourceは{score_source}です。保存済み予測かどうかを確認してください。")

    if fallback:
        parts.append("fallbackが使われているため、実弾判断には使わないでください。")

    parts.append("これは買い推奨ではなく、手動確認前提の候補説明です。")
    return {"comment": "\n".join(parts), "source": "rule_based_h5_reason"}


def build_ai_score_reason(row: dict[str, Any]) -> dict[str, str]:
    score = _score(row)
    drop20 = _drop20(row)
    drop_pct = _to_float(_first(row, "drop_pct", "day_change_pct"))
    rsi14 = _to_float(_first(row, "rsi14", "diagnostic_rsi14"))
    volume = _volume_ratio(row)
    regime = str(_first(row, "market_regime") or "")
    stage = str(_first(row, "signal_stage") or "")
    margin = _to_float(_first(row, "margin_ratio"))

    parts: list[str] = []
    if score is not None:
        parts.append(f"AIスコアは{_fmt_score(score)}です。")
        if score >= 0.65:
            parts.append("H5基準を上回るため、過去類似パターンでは短期反発候補として評価されています。")
        else:
            parts.append("H5基準には届いていません。")
    else:
        parts.append("AIスコアは候補データ上で確認できません。")

    if drop20 is not None and drop20 <= -10:
        parts.append(f"20日高値から{_fmt_pct(drop20)}下落しており、平均回帰候補として評価されやすい状態です。")
    elif drop20 is not None and drop20 <= -8:
        parts.append(f"20日高値から{_fmt_pct(drop20)}下落しており、H5の急落条件に入っています。")
    elif drop_pct is not None:
        parts.append(f"当日急落率は{_fmt_pct(drop_pct)}です。")

    if rsi14 is not None:
        if rsi14 <= 30:
            parts.append(f"RSIは{rsi14:.0f}で売られすぎ水準に近く、短期反発候補として評価されやすい状態です。")
        elif rsi14 >= 70:
            parts.append(f"RSIは{rsi14:.0f}で高めのため、短期過熱には注意が必要です。")
        else:
            parts.append(f"RSIは{rsi14:.0f}で、極端な過熱・売られすぎではありません。")

    if volume is not None:
        if volume >= 1.5:
            parts.append(f"出来高倍率は{volume:.1f}倍で、需給イベントが発生している可能性があります。")
        elif volume < 0.8:
            parts.append(f"出来高倍率は{volume:.1f}倍で、反発時の流動性には注意が必要です。")
        else:
            parts.append(f"出来高倍率は{volume:.1f}倍で、極端な過熱ではありません。")

    if stage:
        parts.append(f"signal_stageは{stage}です。")
    if regime:
        parts.append(f"地合い判定は{regime}です。")
    if margin is not None:
        parts.append(f"信用倍率は{margin:.1f}倍です。")

    parts.append(SHAP_NOTICE)
    return {"comment": "\n".join(parts), "source": "rule_based_ai_score_reason"}


def build_risk_reason(row: dict[str, Any]) -> dict[str, str]:
    rsi14 = _to_float(_first(row, "rsi14", "diagnostic_rsi14"))
    volume = _volume_ratio(row)
    margin = _to_float(_first(row, "margin_ratio"))
    regime = str(_first(row, "market_regime") or "")
    entry_gap = _to_float(_first(row, "entry_gap_pct"))
    ma25_gap = _to_float(_first(row, "entry_ma25_gap_pct", "ma25_gap_pct", "close_vs_ma25_pct"))
    ma75_gap = _to_float(_first(row, "entry_ma75_gap_pct", "ma75_gap_pct", "close_vs_ma75_pct"))
    overheat = _overheat(row)
    fallback = _to_bool(_first(row, "score_fallback_used", "fallback_used"))
    score_source = str(_first(row, "score_source", "prediction_source") or "")

    parts: list[str] = []
    if ma75_gap is not None and ma75_gap < -10:
        parts.append("MA75から大きく下に乖離しており、中長期では下落基調の可能性があります。")
    elif ma25_gap is not None and ma25_gap < -5:
        parts.append("MA25から下に乖離しており、短期リバウンド狙いに限定すべき形です。")

    if rsi14 is not None and rsi14 <= 25:
        parts.append("RSIは強い売られすぎですが、落ちナイフ化するリスクもあります。")

    if entry_gap is not None:
        if entry_gap > 3:
            parts.append("シグナル価格から+3%超乖離しており、飛びつきリスクが高い状態です。")
        elif entry_gap > 2:
            parts.append("シグナル価格から+2%超乖離しており、検証想定より期待値が低下する可能性があります。")

    if margin is not None:
        if margin > 30:
            parts.append("信用倍率が30倍を超えており、戻り売りや需給悪化に注意が必要です。")
        elif margin < 3:
            parts.append("信用倍率が3倍未満で、H5想定レンジとは異なる可能性があります。")

    if volume is not None and volume < 0.8:
        parts.append("出来高が少なく、約定・撤退時の滑りに注意が必要です。")
    elif volume is not None and volume >= 2.0:
        parts.append("出来高が大きく増えており、投げ売り継続や材料確認漏れに注意が必要です。")

    if regime in {"risk_off", "panic_selloff"}:
        parts.append(f"地合いは{regime}で、個別反発より市場全体の売りに巻き込まれるリスクがあります。")

    if overheat is not None and overheat > 1:
        parts.append("過熱スコアが高く、H5条件では慎重に扱うべき状態です。")

    if score_source and score_source != "stored_predictions":
        parts.append("保存済みAIスコアではない可能性があります。point-in-timeのスコアか確認してください。")
    if fallback:
        parts.append("fallbackスコアが使われているため、実弾判断からは除外してください。")

    if not parts:
        parts.append("明確な個別リスクは検出していませんが、寄付き、板、ニュース、決算予定は必ず確認してください。")

    parts.append("翌日寄りが前日終値比+2%を超える場合は、検証想定よりentry価格が悪化するため飛びつき注意です。")
    parts.append("H5は右裾込みの母集団型であり、単発勝率を保証するものではありません。")
    return {"comment": "\n".join(parts), "source": "rule_based_risk_reason"}


def build_h5_ai_reasons(row: dict[str, Any]) -> dict[str, str]:
    h5 = build_h5_reason(row)
    ai = build_ai_score_reason(row)
    risk = build_risk_reason(row)
    now = datetime.now(timezone.utc).isoformat()
    return {
        "h5_reason_comment": h5["comment"],
        "h5_reason_source": h5["source"],
        "h5_reason_generated_at": now,
        "ai_score_reason_comment": ai["comment"],
        "ai_score_reason_source": ai["source"],
        "ai_score_reason_generated_at": now,
        "risk_reason_comment": risk["comment"],
        "risk_reason_source": risk["source"],
        "risk_reason_generated_at": now,
    }


def reason_cache_key(row: dict[str, Any]) -> str:
    code = str(row.get("code") or "").strip()
    trade_date = str(_first(row, "trade_date", "feature_snapshot_trade_date", "last_signal_at") or "").strip()
    if "T" in trade_date:
        trade_date = trade_date.split("T", 1)[0]
    trade_date = trade_date[:10]
    return f"{trade_date}:{code}" if trade_date and code else code


def load_reason_cache(path: Path = DEFAULT_CACHE_PATH) -> dict[str, dict[str, Any]]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_reason_cache(cache: dict[str, dict[str, Any]], path: Path = DEFAULT_CACHE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def get_cached_reasons(row: dict[str, Any], cache: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    key = reason_cache_key(row)
    if key and key in cache:
        return cache[key]
    code = str(row.get("code") or "").strip()
    if not code:
        return None
    matches = [
        value
        for cached_key, value in cache.items()
        if cached_key == code or cached_key.endswith(f":{code}")
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda r: str(r.get("updated_at") or r.get("h5_reason_generated_at") or ""), reverse=True)[0]


def upsert_cached_reasons(row: dict[str, Any], reasons: dict[str, Any], path: Path = DEFAULT_CACHE_PATH) -> dict[str, Any]:
    cache = load_reason_cache(path)
    record = {
        "code": str(row.get("code") or "").strip(),
        "name": str(row.get("name") or "").strip(),
        "trade_date": str(_first(row, "trade_date", "feature_snapshot_trade_date") or "").strip()[:10],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **reasons,
    }
    cache[reason_cache_key(record)] = record
    save_reason_cache(cache, path)
    return record
