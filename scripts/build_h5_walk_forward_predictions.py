"""Build H5 walk-forward prediction CSVs.

Research-only script. It does not update models, DB case definitions, Primary,
UI, notifications, Watchlist, Intraday H5, or actual trade logs.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import lightgbm as lgb
    import numpy as np
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("lightgbm, numpy, and pandas are required") from exc

from services.signal_stage import evaluate_signal_stage
from services.trade_case_tester import (
    _attach_future_day_features,
    _attach_market_regime,
    _attach_weekly_margin,
    _build_supabase,
    _expected_value_for_rules,
    _fetch_all,
    _fetch_snapshots_by_ids,
    _load_future_day_snapshots,
    _load_market_regime_rows,
    _load_weekly_margin_rows,
    _proxy_rule_score,
)
from scripts.train_rebound_model import BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

JST = timezone(timedelta(hours=9))
MAX_FUTURE_DAYS = 20
RISK_TOKENS_HIGH = (
    "future", "label", "target", "y_true", "hd1", "hd3", "hd5", "hd7",
    "hd10", "exit", "realized", "next", "forward",
)
RISK_TOKENS_MEDIUM = ("profit", "loss", "return")


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:10]).date()


def month_end(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def add_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def round_value(value: Any, digits: int = 6) -> Any:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, digits)
    except Exception:
        return value


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: round_value(row.get(key)) for key in headers})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def feature_risk(name: str) -> tuple[str, str]:
    lower = name.lower()
    high_hits = [tok for tok in RISK_TOKENS_HIGH if tok in lower]
    if high_hits:
        return "high", "name contains " + ",".join(high_hits)
    medium_hits = [tok for tok in RISK_TOKENS_MEDIUM if tok in lower]
    if medium_hits:
        return "medium", "name contains " + ",".join(medium_hits)
    return "low", "entry-time feature name"


def feature_columns_rows() -> list[dict]:
    rows = []
    for feature in list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES):
        risk, notes = feature_risk(feature)
        rows.append({
            "feature_name": feature,
            "source_table": "stock_feature_snapshots",
            "leakage_risk": risk,
            "notes": notes,
        })
    return rows


def load_unscored_candidate_rows(sb, period_start: date, period_end: date) -> list[dict]:
    snap_cols = sorted(set(
        [
            "id", "trade_date", "code", "name", "market", "sector", "close",
            "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
            "volume_ratio_20d", "bad_news_score", "market_shock_score",
        ]
        + list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES)
    ))
    future_cols: list[str] = []
    for day in range(1, MAX_FUTURE_DAYS + 1):
        future_cols += [f"future_high_{day}d", f"future_low_{day}d", f"future_close_{day}d"]
    label_cols = [
        "id", "feature_snapshot_id", "trade_date", "code", "entry_price",
        "label_5d_success", "label_5d_max_return", "label_5d_max_drawdown",
    ] + future_cols
    start_s = period_start.isoformat()
    end_s = period_end.isoformat()

    def label_query(last_id: int):
        return (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .gt("id", last_id)
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .not_.is_("label_5d_success", "null")
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("id")
        )

    labels = _fetch_all(label_query, label="wf_labels")
    logger.info("[wf] labels loaded rows=%d", len(labels))
    snap_ids = [int(r["feature_snapshot_id"]) for r in labels if r.get("feature_snapshot_id")]
    snapshots = _fetch_snapshots_by_ids(sb, snap_ids, snap_cols)
    logger.info("[wf] snapshots loaded rows=%d", len(snapshots))
    snap_by_id = {
        str(s["id"]): s
        for s in snapshots
        if s.get("is_drop_candidate") and s.get("is_tradeable")
    }
    rows: list[dict] = []
    for label in labels:
        snap = snap_by_id.get(str(label.get("feature_snapshot_id")))
        if not snap:
            continue
        merged = dict(snap)
        for key, value in label.items():
            if key in {"id", "code", "trade_date"}:
                merged[f"label_{key}"] = value
            else:
                merged[key] = value
        rows.append(merged)
    _attach_weekly_margin(rows, _load_weekly_margin_rows(sb, period_start, period_end))
    _attach_market_regime(rows, _load_market_regime_rows(sb, period_start, period_end))
    codes = list({str(r.get("code") or "") for r in rows if r.get("code")})
    if codes:
        future_snaps = _load_future_day_snapshots(sb, codes, period_start, period_end)
        _attach_future_day_features(rows, future_snaps)
    logger.info("[wf] unscored candidate rows=%d", len(rows))
    return rows


def build_feature_matrix(df: "pd.DataFrame") -> tuple["pd.DataFrame", list[str], dict[str, float], list[str], list[str]]:
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
    x = pd.concat([x_num, x_cat], axis=1).reindex(sorted(list(x_num.columns) + list(x_cat.columns)), axis=1)
    return x, list(x.columns), fill_values, numeric, categorical


def apply_feature_matrix(df: "pd.DataFrame", feature_cols: list[str], fill_values: dict[str, float]) -> "pd.DataFrame":
    x, _, _, _, _ = build_feature_matrix(df)
    x = x.reindex(columns=feature_cols, fill_value=0)
    for col, value in fill_values.items():
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").fillna(value)
    return x.replace([np.inf, -np.inf], np.nan).fillna(0)


def make_runs(initial_train_start: date, initial_train_end: date, predict_start: date, predict_end: date) -> list[dict]:
    runs = []
    cur = predict_start
    while cur <= predict_end:
        pe = min(month_end(cur), predict_end)
        train_end = cur - timedelta(days=1)
        train_start = initial_train_start
        run_id = f"wf_{cur:%Y%m}_train{train_start:%Y%m%d}_{train_end:%Y%m%d}"
        runs.append({
            "walk_run_id": run_id,
            "train_start": train_start,
            "train_end": train_end,
            "predict_start": cur,
            "predict_end": pe,
        })
        cur = add_month(cur)
    if runs and initial_train_end and runs[0]["train_end"] != initial_train_end:
        logger.warning("initial_train_end adjusted by predict_start: requested=%s actual=%s", initial_train_end, runs[0]["train_end"])
    return runs


def as_date_series(series: "pd.Series") -> "pd.Series":
    return pd.to_datetime(series).dt.date


def add_signal_fields(row: dict, prob: float) -> dict:
    row["signal_probability"] = round(float(prob), 6)
    row["rule_score"] = _proxy_rule_score(row)
    row["expected_value"] = round(_expected_value_for_rules(row, {"tp_pct": 0.06, "sl_pct": -0.04}), 3)
    stage = evaluate_signal_stage(row["signal_probability"], row["rule_score"], row["expected_value"])
    row["signal_stage"] = stage["stage"]
    return row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_walk_forward_predictions")
    parser.add_argument("--initial-train-start", default="2023-05-08")
    parser.add_argument("--initial-train-end", default="2024-12-31")
    parser.add_argument("--predict-start", default="2025-01-01")
    parser.add_argument("--predict-end", default="latest")
    parser.add_argument("--allow-risky-features", action="store_true")
    parser.add_argument("--min-train-rows", type=int, default=500)
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    initial_train_start = parse_date(args.initial_train_start)
    initial_train_end = parse_date(args.initial_train_end)
    predict_start = parse_date(args.predict_start)
    predict_end = date.today() if args.predict_end == "latest" else parse_date(args.predict_end)

    feature_rows = feature_columns_rows()
    write_csv(output_dir / "00_feature_columns_used.csv", feature_rows)
    high_risk = [r for r in feature_rows if r["leakage_risk"] == "high"]
    if high_risk and not args.allow_risky_features:
        raise RuntimeError(f"High-risk feature columns found: {[r['feature_name'] for r in high_risk]}")

    sb = _build_supabase()
    rows = load_unscored_candidate_rows(sb, initial_train_start, predict_end)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No candidate rows loaded")
    df["trade_date_dt"] = as_date_series(df["trade_date"])
    df["target_success"] = df["label_5d_success"].astype(bool).astype(int)

    runs = make_runs(initial_train_start, initial_train_end, predict_start, predict_end)
    prediction_rows: list[dict] = []
    run_rows: list[dict] = []
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
        "verbose": -1,
    }

    for run in runs:
        train_mask = (df["trade_date_dt"] >= run["train_start"]) & (df["trade_date_dt"] <= run["train_end"])
        predict_mask = (df["trade_date_dt"] >= run["predict_start"]) & (df["trade_date_dt"] <= run["predict_end"])
        train_df = df.loc[train_mask].copy()
        predict_df = df.loc[predict_mask].copy()
        model_version = f"{run['walk_run_id']}"
        run_log = {
            "walk_run_id": run["walk_run_id"],
            "model_key": "rebound_lgbm_5d",
            "model_version": model_version,
            "train_start": run["train_start"].isoformat(),
            "train_end": run["train_end"].isoformat(),
            "predict_start": run["predict_start"].isoformat(),
            "predict_end": run["predict_end"].isoformat(),
            "train_rows": len(train_df),
            "predict_rows": len(predict_df),
            "positive_rate_train": float(train_df["target_success"].mean()) if len(train_df) else None,
            "positive_rate_predict": float(predict_df["target_success"].mean()) if len(predict_df) else None,
            "feature_count": None,
            "model_params_json": json.dumps(params, sort_keys=True),
            "fit_status": "pending",
            "predict_status": "pending",
            "error_message": "",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if len(train_df) < args.min_train_rows or predict_df.empty:
                raise RuntimeError("insufficient train or predict rows")
            x_train, feature_cols, fill_values, numeric_cols, categorical_cols = build_feature_matrix(train_df)
            x_predict = apply_feature_matrix(predict_df, feature_cols, fill_values)
            y_train = train_df["target_success"].astype(int)
            model = lgb.LGBMClassifier(**params)
            model.fit(x_train, y_train)
            probs = model.predict_proba(x_predict)[:, 1]
            run_log["feature_count"] = len(feature_cols)
            run_log["fit_status"] = "ok"
            run_log["predict_status"] = "ok"

            for (_, src_row), prob in zip(predict_df.iterrows(), probs):
                raw = src_row.to_dict()
                add_signal_fields(raw, float(prob))
                trade_date = parse_date(raw["trade_date"])
                prediction_rows.append({
                    "code": raw.get("code"),
                    "name": raw.get("name"),
                    "trade_date": trade_date.isoformat(),
                    "model_key": "rebound_lgbm_5d",
                    "model_version": model_version,
                    "walk_run_id": run["walk_run_id"],
                    "train_start": run["train_start"].isoformat(),
                    "train_end": run["train_end"].isoformat(),
                    "predict_start": run["predict_start"].isoformat(),
                    "predict_end": run["predict_end"].isoformat(),
                    "prediction_date": datetime.now(JST).isoformat(),
                    "signal_probability": raw["signal_probability"],
                    "signal_stage": raw["signal_stage"],
                    "source": "walk_forward",
                    "feature_version": "train_rebound_model_features_v1",
                    "feature_hash": "",
                    "prediction_note": "CSV walk-forward prediction; generated now with OOS train_end structure",
                    "rule_score": raw.get("rule_score"),
                    "expected_value": raw.get("expected_value"),
                    "entry_price": raw.get("entry_price"),
                    "close": raw.get("close"),
                    "drop_from_20d_high_pct": raw.get("drop_from_20d_high_pct"),
                    "market_regime": raw.get("market_regime"),
                    "overheat_score": raw.get("overheat_score"),
                    "margin_ratio": raw.get("margin_ratio"),
                    "volume_ratio_20d": raw.get("volume_ratio_20d"),
                    "sector": raw.get("sector"),
                    "market": raw.get("market"),
                    "label_5d_success": raw.get("label_5d_success"),
                    **{f"future_high_{d}d": raw.get(f"future_high_{d}d") for d in range(1, 11)},
                    **{f"future_low_{d}d": raw.get(f"future_low_{d}d") for d in range(1, 11)},
                    **{f"future_close_{d}d": raw.get(f"future_close_{d}d") for d in range(1, 11)},
                })
        except Exception as exc:
            run_log["fit_status"] = "failed"
            run_log["predict_status"] = "failed"
            run_log["error_message"] = str(exc)
            logger.exception("[wf] run failed: %s", run["walk_run_id"])
        run_rows.append(run_log)
        logger.info("[wf] run=%s train=%d predict=%d status=%s", run["walk_run_id"], len(train_df), len(predict_df), run_log["predict_status"])

    write_csv(output_dir / "01_walk_forward_predictions.csv", prediction_rows)
    write_csv(output_dir / "02_walk_forward_model_runs.csv", run_rows)
    ok_runs = [r for r in run_rows if r.get("predict_status") == "ok"]
    write_text(output_dir / "03_walk_forward_prediction_summary.txt", f"""
# H5 Walk-forward Prediction Summary

prediction_rows: {len(prediction_rows)}
run_count: {len(run_rows)}
ok_run_count: {len(ok_runs)}
failed_run_count: {len(run_rows) - len(ok_runs)}
initial_train_start: {initial_train_start}
predict_start: {predict_start}
predict_end: {predict_end}
model_key: rebound_lgbm_5d
feature_count_last_ok: {ok_runs[-1].get('feature_count') if ok_runs else None}

Important:
Predictions are generated now into CSV, but each run trains only on rows with
trade_date <= train_end and predicts the next month. No active model bundle is
used and no DB model state is changed.
""")
    logger.info("[wf] wrote predictions=%d runs=%d to %s", len(prediction_rows), len(run_rows), output_dir)


if __name__ == "__main__":
    main()
