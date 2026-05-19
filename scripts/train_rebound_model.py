#!/usr/bin/env python3
"""Train a LightGBM rebound classifier from feature snapshots and labels."""
import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

try:
    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.metrics import accuracy_score, f1_score, log_loss, precision_score, recall_score, roc_auc_score

    HAS_BASE_DEPS = True
except ImportError:
    HAS_BASE_DEPS = False

try:
    import lightgbm as lgb

    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False

from supabase import create_client
from services.model_storage import upload_model_artifacts

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "models"

NUMERIC_FEATURES = [
    "day_change_pct", "drop_pct", "drop_from_5d_high_pct", "drop_from_20d_high_pct",
    "drop_from_52w_high_pct", "return_1d_pct", "return_3d_pct", "return_5d_pct", "return_10d_pct",
    "ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct", "rsi14", "rsi_min_5d",
    "volume_ratio_20d", "atr14", "volatility_20d",
    "nikkei_change_pct", "topix_change_pct", "sector_change_pct", "index_gap_pct", "sector_gap_pct",
    "decliners_ratio", "advancers_ratio", "vix_value", "vix_change_pct", "nikkei_vi_value",
    "nikkei_vi_change_pct", "per", "pbr", "dividend_yield_pct", "roe",
    "operating_profit_growth_pct", "net_income_growth_pct",
    "margin_ratio", "margin_buy_change_pct", "short_selling_ratio",
    "market_shock_score", "sector_risk_score", "bad_news_score", "fx_yen_score",
    "energy_naphtha_score", "interest_rate_score",
]
BOOL_FEATURES = [
    "rsi_recover_flag", "volume_spike_flag", "is_deficit", "earnings_soon_flag",
    "earnings_within_5d_flag", "earnings_recent_flag",
]
CATEGORICAL_FEATURES = ["sector", "market"]
TARGET_LABELS = {
    "5d": {
        "label_col": "label_5d_success",
        "compat_col": "label_success",
        "return_col": "label_5d_max_return",
        "drawdown_col": "label_5d_max_drawdown",
        "model_name": "rebound_lgbm_5d",
        "target_name": "label_5d_success",
    },
    "10d": {
        "label_col": "label_10d_success",
        "compat_col": "label_10d_success",
        "return_col": "label_10d_max_return",
        "drawdown_col": "label_10d_max_drawdown",
        "model_name": "rebound_lgbm_10d",
        "target_name": "label_10d_success",
    },
}


def _target_spec(args: argparse.Namespace) -> dict:
    return TARGET_LABELS.get(str(getattr(args, "target_label", "5d") or "5d"), TARGET_LABELS["5d"])


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    start_date = datetime.fromisoformat(str(start)[:10]).date()
    end_date = datetime.fromisoformat(str(end)[:10]).date()
    chunk_days = max(1, int(chunk_days or 31))
    chunks: list[tuple[str, str]] = []
    cur = start_date
    while cur <= end_date:
        chunk_end = min(end_date, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _fetch_all(query_factory, *, page_size: int = 500, label: str = "rows") -> list[dict]:
    rows: list[dict] = []
    last_id = 0
    while True:
        builder = query_factory(last_id)
        res = builder.limit(page_size).execute()
        data = res.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        try:
            last_id = max(int(r.get("id") or last_id) for r in data)
        except Exception:
            break
        if len(rows) % 10000 == 0:
            logger.info("load %s progress: rows=%d", label, len(rows))
    return rows


def _fetch_all_chunked(query_factory, *, start: str, end: str, chunk_days: int = 31, page_size: int = 500, label: str = "rows") -> list[dict]:
    rows: list[dict] = []
    seen_ids: set[Any] = set()
    chunks = _date_chunks(start, end, chunk_days)
    for chunk_start, chunk_end in chunks:
        chunk_rows = _fetch_all(
            lambda last_id, cs=chunk_start, ce=chunk_end: query_factory(last_id, cs, ce),
            page_size=page_size,
            label=f"{label} {chunk_start}..{chunk_end}",
        )
        added = 0
        for row in chunk_rows:
            row_id = row.get("id")
            if row_id is not None and row_id in seen_ids:
                continue
            if row_id is not None:
                seen_ids.add(row_id)
            rows.append(row)
            added += 1
        logger.info(
            "load %s chunk: %s..%s rows=%d total=%d",
            label,
            chunk_start,
            chunk_end,
            added,
            len(rows),
        )
    return rows


def _date_range(args: argparse.Namespace) -> tuple[str, str]:
    end = args.end or datetime.now(JST).date().isoformat()
    if args.start:
        start = args.start
    else:
        start = (datetime.fromisoformat(end).date() - timedelta(days=365 * int(args.years or 1))).isoformat()
    return start, end


def _load_training_rows(sb, args: argparse.Namespace) -> "pd.DataFrame":
    start, end = _date_range(args)
    target = _target_spec(args)
    label_col = target["label_col"]
    fallback_col = target["compat_col"]
    snap_cols = sorted(set(
        ["id", "trade_date", "code", "name", "is_drop_candidate", "is_tradeable"]
        + NUMERIC_FEATURES + BOOL_FEATURES + CATEGORICAL_FEATURES
    ))
    label_cols = [
        "id", "feature_snapshot_id", "trade_date", "code", "label_success", "is_valid_label",
        "label_5d_success", "label_5d_max_return", "label_5d_max_drawdown",
        "label_10d_success", "label_10d_max_return", "label_10d_max_drawdown",
        "max_return_5d_pct", "max_drawdown_5d_pct",
    ]
    def snapshot_query(last_id: int = 0, chunk_start: str = start, chunk_end: str = end):
        q = (
            sb.table("stock_feature_snapshots")
            .select(",".join(snap_cols))
            .eq("is_drop_candidate", True)
            .eq("is_tradeable", True)
            .gte("trade_date", chunk_start)
            .lte("trade_date", chunk_end)
            .order("id")
        )
        if last_id:
            q = q.gt("id", last_id)
        return q

    def label_query(last_id: int = 0, chunk_start: str = start, chunk_end: str = end):
        q = (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .eq("is_valid_label", True)
            .gte("trade_date", chunk_start)
            .lte("trade_date", chunk_end)
            .order("id")
        )
        try:
            q = q.not_.is_(label_col, "null")
        except Exception:
            q = q.not_.is_(fallback_col, "null")
        if last_id:
            q = q.gt("id", last_id)
        return q

    snapshots = _fetch_all_chunked(
        snapshot_query,
        start=start,
        end=end,
        chunk_days=args.fetch_chunk_days,
        page_size=args.fetch_page_size,
        label="snapshots",
    )
    labels = _fetch_all_chunked(
        label_query,
        start=start,
        end=end,
        chunk_days=args.fetch_chunk_days,
        page_size=args.fetch_page_size,
        label="labels",
    )
    logger.info("loaded training source rows: snapshots=%d labels=%d", len(snapshots), len(labels))
    if not snapshots or not labels:
        return pd.DataFrame()
    s = pd.DataFrame(snapshots)
    ldf = pd.DataFrame(labels).rename(columns={"trade_date": "label_trade_date", "code": "label_code"})
    merged = s.merge(ldf, left_on="id", right_on="feature_snapshot_id", how="inner")
    if merged.empty:
        ldf2 = pd.DataFrame(labels)
        merged = s.merge(ldf2, on=["code", "trade_date"], how="inner")
    if label_col not in merged.columns or merged[label_col].isna().all():
        label_col = fallback_col
    merged = merged[merged[label_col].notna()].copy()
    merged["target_success"] = merged[label_col].astype(bool)
    merged["trade_date"] = pd.to_datetime(merged["trade_date"])
    merged = merged.sort_values("trade_date")
    return merged


def _feature_frame(df: "pd.DataFrame") -> tuple["pd.DataFrame", list[str], dict[str, float], list[str], list[str]]:
    work = df.copy()
    numeric = [c for c in NUMERIC_FEATURES + BOOL_FEATURES if c in work.columns]
    categorical = [c for c in CATEGORICAL_FEATURES if c in work.columns]

    for col in BOOL_FEATURES:
        if col in work.columns:
            work[col] = work[col].fillna(False).astype(bool).astype(int)
    for col in numeric:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    fill_values = {col: 0.0 for col in numeric}
    x_num = work[numeric].replace([np.inf, -np.inf], np.nan).fillna(fill_values) if numeric else pd.DataFrame(index=work.index)

    for col in categorical:
        work[col] = work[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = pd.get_dummies(work[categorical], prefix=categorical, dummy_na=False) if categorical else pd.DataFrame(index=work.index)

    x = pd.concat([x_num, x_cat], axis=1)
    x = x.reindex(sorted(x.columns), axis=1)
    return x, list(x.columns), fill_values, numeric, categorical


def _split_time(df: "pd.DataFrame", args: argparse.Namespace) -> tuple["pd.DataFrame", "pd.DataFrame"]:
    if args.valid_months:
        valid_start = df["trade_date"].max() - pd.DateOffset(months=int(args.valid_months))
        train = df[df["trade_date"] < valid_start].copy()
        valid = df[df["trade_date"] >= valid_start].copy()
        if len(train) and len(valid):
            return train, valid
    cut = max(1, int(len(df) * 0.8))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


def _success_rate(mask: "pd.Series", y: "pd.Series") -> float | None:
    total = int(mask.sum())
    if total == 0:
        return None
    return float(y[mask].mean())


def _metrics(y_valid: "pd.Series", prob: "np.ndarray", valid_df: "pd.DataFrame", args: argparse.Namespace) -> dict:
    pred = (prob >= 0.5).astype(int)
    out: dict[str, Any] = {
        "valid_samples": int(len(y_valid)),
        "valid_success_rate": float(y_valid.mean()) if len(y_valid) else None,
        "accuracy": float(accuracy_score(y_valid, pred)),
        "precision": float(precision_score(y_valid, pred, zero_division=0)),
        "recall": float(recall_score(y_valid, pred, zero_division=0)),
        "f1": float(f1_score(y_valid, pred, zero_division=0)),
    }
    try:
        out["roc_auc"] = float(roc_auc_score(y_valid, prob))
    except Exception:
        out["roc_auc"] = None
    try:
        out["log_loss"] = float(log_loss(y_valid, prob, labels=[0, 1]))
    except Exception:
        out["log_loss"] = None

    p = pd.Series(prob, index=y_valid.index)
    out["top_10pct_success_rate"] = _success_rate(p >= p.quantile(0.90), y_valid)
    out["top_20pct_success_rate"] = _success_rate(p >= p.quantile(0.80), y_valid)
    out["prob_55_success_rate"] = _success_rate(p >= 0.55, y_valid)
    out["prob_65_success_rate"] = _success_rate(p >= 0.65, y_valid)
    out["prob_72_success_rate"] = _success_rate(p >= 0.72, y_valid)
    top20 = p >= p.quantile(0.80)
    target = _target_spec(args)
    return_col = target["return_col"] if target["return_col"] in valid_df.columns else "max_return_5d_pct"
    drawdown_col = target["drawdown_col"] if target["drawdown_col"] in valid_df.columns else "max_drawdown_5d_pct"
    out["avg_max_return_top_20pct"] = float(pd.to_numeric(valid_df.loc[top20, return_col], errors="coerce").mean()) if top20.any() else None
    out["avg_max_drawdown_top_20pct"] = float(pd.to_numeric(valid_df.loc[top20, drawdown_col], errors="coerce").mean()) if top20.any() else None
    return out


def _clean_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_json(v) for v in value]
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _register_model(sb, row: dict, activate: bool) -> None:
    model_name = row["model_name"]
    if activate:
        try:
            sb.table("ml_models").update({
                "is_active": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("model_name", model_name).eq("is_active", True).execute()
        except Exception as e:
            logger.warning("active model cleanup failed: %s", e)
        row["is_active"] = True
    sb.table("ml_models").upsert(row, on_conflict="model_name,model_version").execute()


def run(args: argparse.Namespace) -> None:
    if not HAS_BASE_DEPS:
        raise RuntimeError("pandas, numpy, scikit-learn and joblib are required")
    if not HAS_LIGHTGBM and not args.dry_run:
        raise RuntimeError("lightgbm is required for training")

    sb = _build_supabase()
    df = _load_training_rows(sb, args)
    logger.info("training rows loaded: %d", len(df))
    if df.empty:
        return
    x_all, feature_cols, fill_values, numeric_cols, categorical_cols = _feature_frame(df)
    train_df, valid_df = _split_time(df, args)
    train_idx = train_df.index
    valid_idx = valid_df.index
    logger.info(
        "features=%d train=%d valid=%d train_period=%s..%s valid_period=%s..%s",
        len(feature_cols), len(train_df), len(valid_df),
        train_df["trade_date"].min().date() if len(train_df) else None,
        train_df["trade_date"].max().date() if len(train_df) else None,
        valid_df["trade_date"].min().date() if len(valid_df) else None,
        valid_df["trade_date"].max().date() if len(valid_df) else None,
    )
    logger.info("feature columns: %s", feature_cols)

    if len(df) < int(args.min_samples):
        logger.warning("not enough samples: %d < min_samples=%d", len(df), int(args.min_samples))
        return
    if args.dry_run:
        logger.info("DRYRUN complete: no model training or DB save")
        return

    target = _target_spec(args)
    y_train = train_df["target_success"].astype(bool).astype(int)
    y_valid = valid_df["target_success"].astype(bool).astype(int)
    x_train = x_all.loc[train_idx, feature_cols]
    x_valid = x_all.loc[valid_idx, feature_cols]

    params = {
        "objective": "binary",
        "learning_rate": 0.05,
        "n_estimators": 300,
        "num_leaves": 31,
        "min_child_samples": 20,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "random_state": 42,
        "class_weight": "balanced",
    }
    model = lgb.LGBMClassifier(**params)
    model.fit(x_train, y_train, eval_set=[(x_valid, y_valid)], eval_metric="binary_logloss")
    prob = model.predict_proba(x_valid)[:, 1]
    metrics = _metrics(y_valid, prob, valid_df, args)
    logger.info(
        "metrics: valid_success_rate=%.1f%% top_20pct_success_rate=%s roc_auc=%s",
        (metrics.get("valid_success_rate") or 0) * 100,
        None if metrics.get("top_20pct_success_rate") is None else round(metrics["top_20pct_success_rate"] * 100, 1),
        metrics.get("roc_auc"),
    )

    MODEL_DIR.mkdir(exist_ok=True)
    version = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    model_name = args.model_name or target["model_name"]
    stem = f"{model_name}_{version}"
    model_path = MODEL_DIR / f"{stem}.pkl"
    feature_path = MODEL_DIR / f"{stem}_features.json"
    importance_path = MODEL_DIR / f"{stem}_importance.csv"
    bundle = {
        "model": model,
        "feature_columns": feature_cols,
        "fill_values": fill_values,
        "categorical_columns": categorical_cols,
        "numeric_columns": numeric_cols,
        "train_config": vars(args),
        "metrics": metrics,
    }
    joblib.dump(bundle, model_path)
    feature_path.write_text(json.dumps({
        "feature_columns": feature_cols,
        "numeric_columns": numeric_cols,
        "categorical_columns": categorical_cols,
        "fill_values": fill_values,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False).to_csv(importance_path, index=False)

    row = {
        "model_name": model_name,
        "model_version": version,
        "target_name": target["target_name"],
        "train_start": train_df["trade_date"].min().date().isoformat(),
        "train_end": train_df["trade_date"].max().date().isoformat(),
        "valid_start": valid_df["trade_date"].min().date().isoformat(),
        "valid_end": valid_df["trade_date"].max().date().isoformat(),
        "features": feature_cols,
        "params": _clean_json(params),
        "metrics": _clean_json(metrics),
        "model_path": str(model_path.relative_to(ROOT)),
        "feature_path": str(feature_path.relative_to(ROOT)),
        "importance_path": str(importance_path.relative_to(ROOT)),
        "is_active": bool(args.activate),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if not args.no_upload_storage:
        uploaded = upload_model_artifacts(sb, [model_path, feature_path, importance_path], root=ROOT)
        logger.info("model artifacts uploaded to storage: files=%d", uploaded)
    _register_model(sb, row, bool(args.activate))
    logger.info("model saved: %s active=%s", model_path, bool(args.activate))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train rebound LightGBM model")
    p.add_argument("--years", type=int, default=1)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--valid-months", type=int, default=6)
    p.add_argument("--min-samples", type=int, default=200)
    p.add_argument("--activate", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--target-label", choices=["5d", "10d"], default="5d")
    p.add_argument("--model-name")
    p.add_argument("--force", action="store_true")
    p.add_argument("--no-upload-storage", action="store_true")
    p.add_argument("--fetch-chunk-days", type=int, default=31, help="Split Supabase training fetches into date chunks to avoid statement timeouts.")
    p.add_argument("--fetch-page-size", type=int, default=500, help="Rows per Supabase page while loading training data.")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
