#!/usr/bin/env python3
"""Evaluate TP/SL grids on historical rows ranked by the active rebound model.

This script is read-only. It does not update Supabase or write model outputs.
"""
import argparse
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

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from supabase import create_client

from scripts.train_rebound_model import BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]

TARGET_MODELS = {
    "5d": ["rebound_lgbm_5d", "rebound_lgbm"],
    "10d": ["rebound_lgbm_10d"],
}


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


def _fetch_all(query_factory, *, page_size: int = 1000, label: str = "rows") -> list[dict]:
    rows: list[dict] = []
    last_id = 0
    while True:
        q = query_factory(last_id).limit(page_size)
        data = q.execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        last_id = max(int(r.get("id") or last_id) for r in data)
        if len(rows) % 10000 == 0:
            logger.info("load %s progress: rows=%d", label, len(rows))
    return rows


def _date_range(args: argparse.Namespace) -> tuple[str, str]:
    end = args.end or datetime.now(JST).date().isoformat()
    if args.start:
        start = args.start
    else:
        start = (datetime.fromisoformat(end).date() - timedelta(days=365 * int(args.years or 1))).isoformat()
    return start, end


def _active_model_row(sb, args: argparse.Namespace) -> dict | None:
    names = [args.model_name] if args.model_name else TARGET_MODELS[args.target_label]
    for name in names:
        rows = (
            sb.table("ml_models")
            .select("*")
            .eq("model_name", name)
            .eq("is_active", True)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return rows[0]
    return None


def _load_model(sb, args: argparse.Namespace) -> tuple[dict, dict]:
    row = _active_model_row(sb, args)
    if not row:
        raise RuntimeError(f"active model not found for target={args.target_label}")
    path = ROOT / str(row["model_path"])
    bundle = joblib.load(path)
    logger.info("active model loaded: name=%s version=%s path=%s", row["model_name"], row["model_version"], path)
    return row, bundle


def _load_rows(sb, args: argparse.Namespace) -> "pd.DataFrame":
    start, end = _date_range(args)
    snap_cols = sorted(set(
        ["id", "trade_date", "code", "name", "is_drop_candidate", "is_tradeable"]
        + NUMERIC_FEATURES + BOOL_FEATURES + CATEGORICAL_FEATURES
    ))
    label_cols = [
        "id", "feature_snapshot_id", "trade_date", "code", "entry_price",
        "future_high_1d", "future_high_2d", "future_high_3d", "future_high_4d", "future_high_5d",
        "future_low_1d", "future_low_2d", "future_low_3d", "future_low_4d", "future_low_5d",
    ]

    def snapshot_query(last_id: int):
        q = (
            sb.table("stock_feature_snapshots")
            .select(",".join(snap_cols))
            .eq("is_drop_candidate", True)
            .eq("is_tradeable", True)
            .gte("trade_date", start)
            .lte("trade_date", end)
            .order("id")
        )
        if last_id:
            q = q.gt("id", last_id)
        return q

    def label_query(last_id: int):
        q = (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .gte("trade_date", start)
            .lte("trade_date", end)
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("id")
        )
        if last_id:
            q = q.gt("id", last_id)
        return q

    snapshots = _fetch_all(snapshot_query, label="snapshots")
    labels = _fetch_all(label_query, label="labels")
    if not snapshots or not labels:
        return pd.DataFrame()
    s = pd.DataFrame(snapshots)
    l = pd.DataFrame(labels).rename(columns={"trade_date": "label_trade_date", "code": "label_code", "id": "label_id"})
    df = s.merge(l, left_on="id", right_on="feature_snapshot_id", how="inner")
    if args.limit:
        df = df.head(int(args.limit)).copy()
    logger.info("loaded rows for grid: %d", len(df))
    return df


def _model_frame(df: "pd.DataFrame", bundle: dict) -> "pd.DataFrame":
    numeric_cols = list(bundle.get("numeric_columns") or [])
    categorical_cols = list(bundle.get("categorical_columns") or [])
    fill_values = dict(bundle.get("fill_values") or {})
    feature_columns = list(bundle.get("feature_columns") or [])
    work = df.copy()

    for col in numeric_cols:
        if col not in work.columns:
            work[col] = 0
        work[col] = pd.to_numeric(work[col], errors="coerce")
    x_num = work[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(fill_values) if numeric_cols else pd.DataFrame(index=work.index)

    for col in categorical_cols:
        if col not in work.columns:
            work[col] = "unknown"
        work[col] = work[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = pd.get_dummies(work[categorical_cols], prefix=categorical_cols, dummy_na=False) if categorical_cols else pd.DataFrame(index=work.index)
    x = pd.concat([x_num, x_cat], axis=1)
    return x.reindex(columns=feature_columns, fill_value=0)


def _grid_params(args: argparse.Namespace) -> list[tuple[float, float]]:
    if args.grid:
        pairs = []
        for item in args.grid.split(","):
            tp, sl = item.split("/")
            pairs.append((float(tp), float(sl)))
        return pairs
    return [
        (3.0, -2.0), (3.0, -3.0), (3.0, -4.0),
        (4.0, -2.0), (4.0, -3.0), (4.0, -4.0),
        (5.0, -2.0), (5.0, -3.0), (5.0, -4.0),
        (6.0, -3.0), (6.0, -4.0),
        (7.0, -3.0), (7.0, -4.0),
    ]


def _first_hit(values: list[float], threshold: float, *, direction: str) -> int | None:
    for i, value in enumerate(values, start=1):
        if direction == "up" and value >= threshold:
            return i
        if direction == "down" and value <= threshold:
            return i
    return None


def _evaluate_grid(df: "pd.DataFrame", args: argparse.Namespace) -> "pd.DataFrame":
    rows = []
    top_quantile = 1.0 - float(args.top_pct) / 100.0
    cutoff = float(df["probability"].quantile(top_quantile))
    target = df[df["probability"] >= cutoff].copy()
    logger.info("top selection: top_pct=%.1f cutoff=%.4f rows=%d/%d", args.top_pct, cutoff, len(target), len(df))

    for tp_pct, sl_pct in _grid_params(args):
        realized = []
        wins = 0
        for _, r in target.iterrows():
            entry = float(r["entry_price"])
            highs = [float(r[f"future_high_{i}d"]) for i in range(1, 6)]
            lows = [float(r[f"future_low_{i}d"]) for i in range(1, 6)]
            tp_day = _first_hit(highs, entry * (1 + tp_pct / 100.0), direction="up")
            sl_day = _first_hit(lows, entry * (1 + sl_pct / 100.0), direction="down")
            win = tp_day is not None and (sl_day is None or tp_day < sl_day)
            wins += int(win)
            if win:
                realized.append(tp_pct)
            elif sl_day is not None:
                realized.append(sl_pct)
            else:
                realized.append(0.0)
        total = len(realized)
        losses = [v for v in realized if v <= 0]
        win_values = [v for v in realized if v > 0]
        rows.append({
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "samples": total,
            "win_rate": round(wins / total * 100.0, 1) if total else 0.0,
            "expected_return_pct": round(sum(realized) / total, 3) if total else 0.0,
            "avg_win_pct": round(sum(win_values) / len(win_values), 3) if win_values else None,
            "avg_loss_pct": round(sum(losses) / len(losses), 3) if losses else None,
        })
    return pd.DataFrame(rows).sort_values("expected_return_pct", ascending=False)


def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas, numpy and joblib are required")
    if args.target_label != "5d":
        raise RuntimeError("exact TP/SL grid currently requires stored future_high/low_1d..5d; use --target-label 5d")
    sb = _build_supabase()
    _, bundle = _load_model(sb, args)
    df = _load_rows(sb, args)
    if df.empty:
        logger.warning("no rows")
        return
    x = _model_frame(df, bundle)
    df["probability"] = bundle["model"].predict_proba(x)[:, 1]
    result = _evaluate_grid(df, args)
    print(result.to_string(index=False))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate TP/SL grid on model-ranked rows")
    p.add_argument("--years", type=int, default=3)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--target-label", choices=["5d", "10d"], default="5d")
    p.add_argument("--model-name")
    p.add_argument("--top-pct", type=float, default=20.0)
    p.add_argument("--grid", help="comma-separated TP/SL pairs, e.g. 3/-4,4/-4,5/-3")
    p.add_argument("--limit", type=int)
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
