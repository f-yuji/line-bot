"""Build Japanese SHAP reason text for H5 trade-assist UI."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _feature_names(items: list[dict[str, Any]], limit: int = 3) -> str:
    names = [str(item.get("label") or item.get("feature") or "") for item in items[:limit]]
    names = [name for name in names if name]
    return "、".join(names) if names else ""


def _fmt_value(value: Any) -> str:
    try:
        f = float(value)
        if abs(f) >= 100:
            return f"{f:.0f}"
        if abs(f) >= 10:
            return f"{f:.1f}"
        return f"{f:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


def _list_summary(items: list[dict[str, Any]], *, empty_text: str) -> str:
    if not items:
        return empty_text
    lines = []
    for i, item in enumerate(items[:5], start=1):
        label = item.get("label") or item.get("feature") or "-"
        feature = item.get("feature") or "-"
        value = _fmt_value(item.get("value"))
        shap_value = _fmt_value(item.get("shap_value"))
        lines.append(f"{i}. {label} ({feature}) = {value} / SHAP {shap_value}")
    return "\n".join(lines)


def build_shap_reason_comment(shap_result: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    if not shap_result.get("ok"):
        reason = shap_result.get("reason") or "unknown"
        warnings = list(shap_result.get("warnings") or [])
        return {
            "shap_reason_comment": f"SHAP理由生成不可: {reason}",
            "shap_reason_source": "shap_unavailable",
            "shap_positive_summary": "",
            "shap_negative_summary": "",
            "shap_generated_at": now,
            "warnings": warnings,
        }

    positives = list(shap_result.get("positive_contributions") or [])
    negatives = list(shap_result.get("negative_contributions") or [])
    pos_names = _feature_names(positives)
    neg_names = _feature_names(negatives)
    parts = ["SHAP AIスコア理由:"]
    if pos_names:
        parts.append(f"この候補では、{pos_names}がAIスコアを主に押し上げています。")
    else:
        parts.append("この候補では、押し上げ要因は取得できませんでした。")
    if neg_names:
        parts.append(f"一方で、{neg_names}は押し下げ要因です。")
    else:
        parts.append("押し下げ要因は取得できませんでした。")
    parts.append("SHAP値はモデル内部の寄与方向を示す参考値であり、確率への単純加算値ではありません。これは買い推奨ではありません。")
    warnings = list(shap_result.get("warnings") or [])
    if warnings:
        parts.append("警告: " + " / ".join(str(w) for w in warnings[:5]))

    return {
        "shap_reason_comment": "\n".join(parts),
        "shap_reason_source": shap_result.get("source") or "shap_tree_explainer",
        "shap_positive_summary": _list_summary(positives, empty_text="押し上げ要因は取得できませんでした。"),
        "shap_negative_summary": _list_summary(negatives, empty_text="押し下げ要因は取得できませんでした。"),
        "shap_generated_at": shap_result.get("generated_at") or now,
        "warnings": warnings,
    }


def merge_shap_reason(shap_result: dict[str, Any]) -> dict[str, Any]:
    reason = build_shap_reason_comment(shap_result)
    merged = dict(shap_result)
    merged.update(reason)
    return merged
