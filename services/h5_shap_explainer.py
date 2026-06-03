"""SHAP explanations for H5 trade-assist candidates.

SHAP is used only for explanation. This module never updates
model_predictions and never changes candidate eligibility.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from services.model_storage import download_model_artifact

try:
    import shap  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    shap = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_KEY = "rebound_lgbm_5d"
DEFAULT_CACHE_ROOT = ROOT / "outputs" / "h5_shap_explanations"


FEATURE_EXPLANATIONS: dict[str, dict[str, str]] = {
    "drop_from_20d_high_pct": {
        "label": "20日高値比下落率",
        "positive": "20日高値から大きく下落しており、短期リバウンド候補としてスコアを押し上げています。",
        "negative": "下落率の形状が今回のモデルではスコアを押し下げています。",
    },
    "rsi14": {
        "label": "RSI14",
        "positive": "RSIが売られすぎ水準に近く、反発候補として評価されています。",
        "negative": "RSI水準がモデル上は反発優位に働いていません。",
    },
    "volume_ratio_20d": {
        "label": "出来高倍率",
        "positive": "出来高が通常より増えており、需給イベントとして評価されています。",
        "negative": "出来高の状態がモデル上は押し下げ要因です。",
    },
    "margin_ratio": {
        "label": "信用倍率",
        "positive": "信用倍率が過去の反発候補に近い範囲として評価されています。",
        "negative": "信用倍率が需給リスクとして評価されています。",
    },
    "market_regime": {
        "label": "市場レジーム",
        "positive": "地合い判定が反発候補に有利に働いています。",
        "negative": "地合い判定が反発候補としては不利に働いています。",
    },
    "overheat_score": {
        "label": "指数過熱スコア",
        "positive": "指数過熱が強くなく、反発余地の評価につながっています。",
        "negative": "指数過熱や地合い状態がリスクとして評価されています。",
    },
    "ma5_gap_pct": {
        "label": "MA5乖離",
        "positive": "短期移動平均との乖離が反発候補として評価されています。",
        "negative": "短期移動平均との乖離がリスクとして評価されています。",
    },
    "ma25_gap_pct": {
        "label": "MA25乖離",
        "positive": "中期移動平均との乖離が反発余地として評価されています。",
        "negative": "中期トレンドとの乖離がリスクとして評価されています。",
    },
    "ma75_gap_pct": {
        "label": "MA75乖離",
        "positive": "長期移動平均との乖離が反発余地として評価されています。",
        "negative": "長期下落基調がスコアを押し下げています。",
    },
    "operating_profit_growth_pct": {
        "label": "営業利益成長率",
        "positive": "業績成長率がスコアを押し上げています。",
        "negative": "業績成長率がスコアを押し下げています。",
    },
    "signal_stage": {
        "label": "シグナル段階",
        "positive": "signal_stageがconfirmed系で、反発候補として評価されています。",
        "negative": "signal_stageが反発候補として弱く評価されています。",
    },
}


def is_shap_available() -> bool:
    return shap is not None


def _get_optional_env(name: str) -> str:
    return os.getenv(name, "").strip()


def _get_mode_env(base_name: str, mode: str) -> str:
    normalized = (mode or "").strip().upper()
    for candidate in ([f"{base_name}_{normalized}"] if normalized else []) + [base_name]:
        value = _get_optional_env(candidate)
        if value:
            return value
    return ""


def build_supabase():
    load_dotenv()
    mode = _get_optional_env("SUPABASE_MODE") or _get_optional_env("ENV")
    url = _get_mode_env("SUPABASE_URL", mode)
    key = _get_mode_env("SUPABASE_KEY", mode)
    if not url or not key:
        raise RuntimeError("Supabase credentials are missing")
    return create_client(url, key)


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _safe_path_part(value: Any) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "unknown"))


def _feature_meta(feature: str, shap_value: float) -> tuple[str, str]:
    base = feature
    if "_" in feature:
        for known in FEATURE_EXPLANATIONS:
            if feature == known or feature.startswith(f"{known}_"):
                base = known
                break
    meta = FEATURE_EXPLANATIONS.get(base)
    if not meta:
        return feature, "この特徴量がAIスコアに影響しています。"
    return meta["label"], meta["positive"] if shap_value >= 0 else meta["negative"]


def load_model_bundle_for_shap(
    model_key: str,
    model_version: str | None,
    *,
    allow_active_fallback: bool = False,
    sb=None,
) -> dict[str, Any]:
    warnings: list[str] = []
    if sb is None:
        sb = build_supabase()
    try:
        q = sb.table("ml_models").select("*").eq("model_name", model_key)
        if model_version and model_version != "latest":
            q = q.eq("model_version", model_version)
        else:
            if allow_active_fallback:
                q = q.eq("is_active", True)
            else:
                return {
                    "ok": False,
                    "model": None,
                    "feature_columns": [],
                    "model_key": model_key,
                    "model_version": model_version,
                    "warnings": ["model_version_required_without_active_fallback"],
                    "reason": "model_version_required",
                }
        rows = q.order("created_at", desc=True).limit(1).execute().data or []
        if not rows and allow_active_fallback:
            warnings.append("specified_model_not_found_active_fallback_used")
            rows = (
                sb.table("ml_models")
                .select("*")
                .eq("model_name", model_key)
                .eq("is_active", True)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
                .data
                or []
            )
    except Exception as exc:
        return {
            "ok": False,
            "model": None,
            "feature_columns": [],
            "model_key": model_key,
            "model_version": model_version,
            "warnings": warnings + [f"model_lookup_error:{exc}"],
            "reason": "model_lookup_error",
        }
    if not rows:
        return {
            "ok": False,
            "model": None,
            "feature_columns": [],
            "model_key": model_key,
            "model_version": model_version,
            "warnings": warnings,
            "reason": "model_not_found",
        }
    model_row = rows[0]
    path = ROOT / str(model_row.get("model_path") or "")
    if not path.exists():
        storage_path = str(model_row.get("storage_path") or model_row.get("model_path") or "")
        if storage_path:
            try:
                download_model_artifact(sb, storage_path, path)
                warnings.append("model_artifact_downloaded")
            except Exception as exc:
                warnings.append(f"model_download_failed:{exc}")
    if not path.exists():
        return {
            "ok": False,
            "model": None,
            "feature_columns": [],
            "model_key": model_key,
            "model_version": model_row.get("model_version"),
            "warnings": warnings,
            "reason": "model_file_not_found",
        }
    try:
        bundle = joblib.load(path)
    except Exception as exc:
        return {
            "ok": False,
            "model": None,
            "feature_columns": [],
            "model_key": model_key,
            "model_version": model_row.get("model_version"),
            "warnings": warnings + [f"model_load_error:{exc}"],
            "reason": "model_load_error",
        }
    feature_columns = list(bundle.get("feature_columns") or model_row.get("features") or [])
    if not feature_columns:
        return {
            "ok": False,
            "model": None,
            "feature_columns": [],
            "model_key": model_key,
            "model_version": model_row.get("model_version"),
            "warnings": warnings,
            "reason": "feature_columns_not_found",
        }
    return {
        "ok": True,
        "model": bundle.get("model"),
        "bundle": bundle,
        "feature_columns": feature_columns,
        "model_key": model_key,
        "model_version": model_row.get("model_version"),
        "model_row": model_row,
        "warnings": warnings,
        "reason": None,
    }


def _model_frame(rows: list[dict[str, Any]], bundle: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    df = pd.DataFrame(rows)
    numeric_cols = list(bundle.get("numeric_columns") or [])
    categorical_cols = list(bundle.get("categorical_columns") or [])
    fill_values = dict(bundle.get("fill_values") or {})
    feature_columns = list(bundle.get("feature_columns") or [])
    missing_raw = [col for col in numeric_cols + categorical_cols if col not in df.columns]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")
    x_num = df[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(fill_values) if numeric_cols else pd.DataFrame(index=df.index)

    for col in categorical_cols:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = pd.get_dummies(df[categorical_cols], prefix=categorical_cols, dummy_na=False) if categorical_cols else pd.DataFrame(index=df.index)
    x = pd.concat([x_num, x_cat], axis=1).reindex(columns=feature_columns, fill_value=0)
    return x, missing_raw


def get_feature_row_for_shap(
    code: str,
    trade_date: str,
    feature_columns: list[str],
    *,
    bundle: dict[str, Any] | None = None,
    sb=None,
) -> dict[str, Any]:
    warnings: list[str] = []
    if sb is None:
        sb = build_supabase()
    code = str(code).replace(".T", "").strip()
    try:
        exact = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .eq("code", code)
            .eq("trade_date", trade_date)
            .limit(1)
            .execute()
            .data
            or []
        )
        rows = exact
        if not rows:
            rows = (
                sb.table("stock_feature_snapshots")
                .select("*")
                .eq("code", code)
                .lte("trade_date", trade_date)
                .order("trade_date", desc=True)
                .limit(1)
                .execute()
                .data
                or []
            )
            if rows:
                warnings.append(f"feature_snapshot_date_fallback:{rows[0].get('trade_date')}")
    except Exception as exc:
        return {
            "ok": False,
            "features": {},
            "feature_vector": [],
            "feature_frame": None,
            "missing_features": [],
            "extra_features": [],
            "warnings": warnings + [f"feature_lookup_error:{exc}"],
            "reason": "feature_lookup_error",
        }
    if not rows:
        return {
            "ok": False,
            "features": {},
            "feature_vector": [],
            "feature_frame": None,
            "missing_features": [],
            "extra_features": [],
            "warnings": warnings,
            "reason": "feature_row_not_found",
        }
    features = rows[0]
    if bundle is None:
        missing = [f for f in feature_columns if f not in features]
        vector = [_to_float(features.get(f), 0.0) or 0.0 for f in feature_columns]
        return {
            "ok": not missing,
            "features": features,
            "feature_vector": vector,
            "feature_frame": pd.DataFrame([vector], columns=feature_columns) if not missing else None,
            "missing_features": missing,
            "extra_features": sorted(set(features) - set(feature_columns)),
            "warnings": warnings,
            "reason": "feature_mismatch" if missing else None,
        }
    frame, missing_raw = _model_frame([features], bundle)
    if missing_raw:
        warnings.append("missing_raw_features:" + ",".join(missing_raw[:20]))
    return {
        "ok": True,
        "features": features,
        "feature_vector": [float(v) for v in frame.iloc[0].tolist()],
        "feature_frame": frame,
        "missing_features": missing_raw,
        "extra_features": sorted(set(features) - set(bundle.get("numeric_columns") or []) - set(bundle.get("categorical_columns") or [])),
        "warnings": warnings,
        "reason": None,
    }


def normalize_shap_values(raw_shap_values: Any, raw_expected_value: Any) -> dict[str, Any]:
    warnings: list[str] = []
    values = raw_shap_values
    expected = raw_expected_value
    if isinstance(values, list):
        values = values[1] if len(values) > 1 else values[0]
        warnings.append("shap_values_list_class1_used")
    if isinstance(expected, list) or isinstance(expected, np.ndarray):
        expected = expected[1] if len(expected) > 1 else expected[0]
    try:
        arr = np.asarray(values)
        if arr.ndim == 2:
            arr = arr[0]
        return {
            "ok": True,
            "shap_values": [float(v) for v in arr.tolist()],
            "expected_value": None if expected is None else float(expected),
            "warnings": warnings,
            "reason": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "shap_values": [],
            "expected_value": None,
            "warnings": warnings + [f"normalize_error:{exc}"],
            "reason": "normalize_error",
        }


def get_shap_cache_path(
    code: str,
    trade_date: str,
    model_key: str,
    model_version: str | None,
    *,
    cache_root: Path = DEFAULT_CACHE_ROOT,
) -> Path:
    day = str(trade_date or "unknown")[:10]
    filename = "_".join([
        _safe_path_part(code),
        _safe_path_part(day),
        _safe_path_part(model_key),
        _safe_path_part(model_version or "unknown"),
    ]) + ".json"
    return cache_root / day / filename


def load_shap_cache(
    code: str,
    trade_date: str,
    model_key: str,
    model_version: str | None,
    *,
    cache_root: Path = DEFAULT_CACHE_ROOT,
) -> dict[str, Any] | None:
    path = get_shap_cache_path(code, trade_date, model_key, model_version, cache_root=cache_root)
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_shap_cache(result: dict[str, Any], *, cache_root: Path = DEFAULT_CACHE_ROOT) -> Path | None:
    try:
        path = get_shap_cache_path(
            result.get("code"),
            result.get("trade_date"),
            result.get("model_key"),
            result.get("model_version"),
            cache_root=cache_root,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return path
    except Exception:
        return None


def _contribution_rows(
    feature_columns: list[str],
    values: list[float],
    shap_values: list[float],
    *,
    positive: bool,
    top_n: int,
) -> list[dict[str, Any]]:
    rows = []
    for feature, value, shap_value in zip(feature_columns, values, shap_values):
        if positive and shap_value <= 0:
            continue
        if not positive and shap_value >= 0:
            continue
        label, description = _feature_meta(feature, shap_value)
        rows.append({
            "feature": feature,
            "value": value,
            "shap_value": float(shap_value),
            "label": label,
            "description": description,
        })
    rows.sort(key=lambda r: abs(float(r["shap_value"])), reverse=True)
    return rows[:top_n]


def compute_shap_for_candidate(
    row: dict[str, Any],
    *,
    top_n: int = 8,
    allow_active_fallback: bool = False,
    force: bool = False,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    sb=None,
) -> dict[str, Any]:
    code = str(row.get("code") or "").replace(".T", "").strip()
    trade_date = str(row.get("trade_date") or row.get("feature_snapshot_trade_date") or "")[:10]
    model_key = str(row.get("model_key") or DEFAULT_MODEL_KEY)
    model_version = str(row.get("model_version") or "")
    signal_probability = _to_float(row.get("signal_probability") or row.get("display_probability") or row.get("entry_probability"))
    if not code or not trade_date:
        return {
            "ok": False,
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": model_version,
            "signal_probability": signal_probability,
            "reason": "missing_code_or_trade_date",
            "warnings": [],
        }
    if model_version in {"", "latest", "None"} and not allow_active_fallback:
        return {
            "ok": False,
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": model_version,
            "signal_probability": signal_probability,
            "reason": "model_version_required",
            "warnings": ["model_version_required_without_active_fallback"],
        }

    cached = None if force else load_shap_cache(code, trade_date, model_key, model_version, cache_root=cache_root)
    if cached:
        cached["cache_hit"] = True
        return cached

    if not is_shap_available():
        return {
            "ok": False,
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": model_version,
            "signal_probability": signal_probability,
            "reason": "shap_not_installed",
            "warnings": ["shap_not_installed"],
        }
    if sb is None:
        sb = build_supabase()
    bundle_result = load_model_bundle_for_shap(
        model_key,
        model_version,
        allow_active_fallback=allow_active_fallback,
        sb=sb,
    )
    if not bundle_result.get("ok"):
        return {**bundle_result, "code": code, "name": row.get("name"), "trade_date": trade_date, "signal_probability": signal_probability}
    feature_result = get_feature_row_for_shap(
        code,
        trade_date,
        bundle_result["feature_columns"],
        bundle=bundle_result.get("bundle"),
        sb=sb,
    )
    if not feature_result.get("ok"):
        return {**feature_result, "code": code, "name": row.get("name"), "trade_date": trade_date, "model_key": model_key, "model_version": bundle_result.get("model_version"), "signal_probability": signal_probability}
    try:
        frame = feature_result["feature_frame"]
        model = bundle_result["model"]
        explainer = shap.TreeExplainer(model)
        raw_values = explainer.shap_values(frame)
        normalized = normalize_shap_values(raw_values, explainer.expected_value)
        if not normalized.get("ok"):
            return {**normalized, "code": code, "name": row.get("name"), "trade_date": trade_date, "model_key": model_key, "model_version": bundle_result.get("model_version"), "signal_probability": signal_probability}
        computed_probability = None
        try:
            computed_probability = float(model.predict_proba(frame)[:, 1][0])
        except Exception as exc:
            bundle_result["warnings"].append(f"predict_proba_for_check_failed:{exc}")
        warnings = list(bundle_result.get("warnings") or []) + list(feature_result.get("warnings") or []) + list(normalized.get("warnings") or [])
        if signal_probability is not None and computed_probability is not None and abs(computed_probability - signal_probability) > 0.02:
            warnings.append(f"probability_mismatch:stored={signal_probability:.6f},computed={computed_probability:.6f}")
        feature_columns = list(bundle_result["feature_columns"])
        vector = list(feature_result["feature_vector"])
        shap_values = list(normalized["shap_values"])
        result = {
            "ok": True,
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": bundle_result.get("model_version"),
            "signal_probability": signal_probability,
            "computed_probability": computed_probability,
            "expected_value": normalized.get("expected_value"),
            "positive_contributions": _contribution_rows(feature_columns, vector, shap_values, positive=True, top_n=top_n),
            "negative_contributions": _contribution_rows(feature_columns, vector, shap_values, positive=False, top_n=top_n),
            "warnings": warnings,
            "source": "shap_tree_explainer",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_hit": False,
        }
        path = save_shap_cache(result, cache_root=cache_root)
        result["cache_path"] = str(path) if path else ""
        return result
    except Exception as exc:
        return {
            "ok": False,
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": bundle_result.get("model_version"),
            "signal_probability": signal_probability,
            "reason": "compute_error",
            "warnings": [str(exc)],
        }
