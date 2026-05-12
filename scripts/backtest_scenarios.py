#!/usr/bin/env python3
"""Evaluate the active rebound model across multiple historical market scenarios.

Read-only: does not write to DB, retrain models, or change is_active flags.
Outputs a per-scenario summary table to stdout and optionally to CSV.

Usage examples
--------------
  python scripts/backtest_scenarios.py
  python scripts/backtest_scenarios.py --horizon 20
  python scripts/backtest_scenarios.py --min-prob 0.55 --top-pct 20 --output results.csv
  python scripts/backtest_scenarios.py --tp-pct 7.0 --sl-pct -4.0
"""
import argparse
import csv
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

SCENARIOS = [
    {"name": "custom_recent",       "type": "custom",  "start": "2026-02-09", "end": "2026-05-10"},
    {"name": "2025_ai_bubble",      "type": "bull",    "start": "2025-01-01", "end": "2025-12-31"},
    {"name": "2024_ai_bubble",      "type": "bull",    "start": "2024-01-01", "end": "2024-12-31"},
    {"name": "2023_rebound",        "type": "rebound", "start": "2023-01-01", "end": "2023-12-31"},
    {"name": "2022_rate_hike_bear", "type": "bear",    "start": "2022-01-01", "end": "2022-12-31"},
    {"name": "2020_covid_crash",    "type": "panic",   "start": "2020-02-20", "end": "2020-04-30"},
]

# TP/SL defaults per horizon
HORIZON_DEFAULTS = {
    5:  {"tp_pct": 5.0,  "sl_pct": -3.0},
    20: {"tp_pct": 10.0, "sl_pct": -5.0},
}

# Active model names to try in order (same as predict_rebound.py)
TARGET_MODEL_NAMES = {
    5:  ["rebound_lgbm_5d", "rebound_lgbm"],
    20: ["rebound_lgbm_5d", "rebound_lgbm"],  # reuse 5d model for 20d window
}

SNAP_COLS = sorted(set(
    ["id", "trade_date", "code", "name"]
    + NUMERIC_FEATURES + BOOL_FEATURES + CATEGORICAL_FEATURES
))

OUTPUT_COLS = [
    "scenario", "type", "period", "total", "samples",
    "win_rate", "avg_return", "expected_return", "avg_prob",
    "top_pct_win_rate", "prob65_win_rate",
    "avg_max_drawdown", "avg_holding_days",
]


# ── Supabase ──────────────────────────────────────────────────────────────────

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


def _fetch_all_by_id(query_factory, *, page_size: int = 1000, label: str = "rows") -> list[dict]:
    """Cursor pagination using id. Works well for recent/sequential data."""
    rows: list[dict] = []
    last_id = 0
    while True:
        q = query_factory(last_id).limit(page_size)
        data = q.execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        try:
            last_id = max(int(r.get("id") or last_id) for r in data)
        except Exception:
            break
        if len(rows) % 10000 == 0:
            logger.info("load %s: %d rows", label, len(rows))
    return rows


def _fetch_all_by_offset(query_factory, *, page_size: int = 1000, label: str = "rows") -> list[dict]:
    """Offset pagination ordered by trade_date. Reliable for any date range (uses date index)."""
    rows: list[dict] = []
    offset = 0
    while True:
        data = query_factory().range(offset, offset + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        offset += page_size
        if len(rows) % 10000 == 0:
            logger.info("load %s: %d rows", label, len(rows))
    return rows


# ── Model ─────────────────────────────────────────────────────────────────────

def _load_active_model(sb, horizon: int, model_name: str | None = None) -> tuple[dict, dict]:
    names = [model_name] if model_name else TARGET_MODEL_NAMES[horizon]
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
            row = rows[0]
            path = ROOT / str(row["model_path"])
            bundle = joblib.load(path)
            logger.info(
                "active model loaded: name=%s version=%s path=%s",
                row["model_name"], row.get("model_version"), path,
            )
            return row, bundle
    raise RuntimeError(f"active model not found for horizon={horizon} names={names}")


def _predict(df: "pd.DataFrame", bundle: dict) -> "np.ndarray":
    numeric_cols = list(bundle.get("numeric_columns") or [])
    categorical_cols = list(bundle.get("categorical_columns") or [])
    fill_values = dict(bundle.get("fill_values") or {})
    feature_columns = list(bundle.get("feature_columns") or [])

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    x_num = (
        df[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(fill_values)
        if numeric_cols else pd.DataFrame(index=df.index)
    )

    for col in categorical_cols:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = (
        pd.get_dummies(df[categorical_cols], prefix=categorical_cols, dummy_na=False)
        if categorical_cols else pd.DataFrame(index=df.index)
    )

    x = pd.concat([x_num, x_cat], axis=1).reindex(columns=feature_columns, fill_value=0)
    return bundle["model"].predict_proba(x)[:, 1]


# ── Data loading ──────────────────────────────────────────────────────────────

def _label_cols(horizon: int) -> list[str]:
    days = horizon
    future_cols = []
    for i in range(1, days + 1):
        future_cols += [f"future_high_{i}d", f"future_low_{i}d"]
    base = [
        "id", "feature_snapshot_id", "trade_date", "code",
        "entry_price",
        "label_5d_max_return", "label_5d_max_drawdown",
        "label_5d_days_to_tp", "label_5d_days_to_sl",
    ]
    return base + future_cols


def _fetch_snapshots_by_ids(sb, ids: list[int], *, batch_size: int = 500) -> list[dict]:
    """Load snapshots for a specific set of IDs, bypassing slow composite-filter queries."""
    rows: list[dict] = []
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        data = (
            sb.table("stock_feature_snapshots")
            .select(",".join(SNAP_COLS))
            .in_("id", batch)
            .execute()
            .data or []
        )
        rows.extend(data)
        if len(rows) % 5000 == 0 and len(rows) > 0:
            logger.info("snapshots loaded: %d/%d", len(rows), len(ids))
    return rows


def _load_scenario_df(sb, sc: dict, horizon: int) -> "pd.DataFrame":
    start, end = sc["start"], sc["end"]
    lcols = _label_cols(horizon)
    last_future_col = f"future_high_{horizon}d"

    # 1. Load labels first — offset pagination ordered by trade_date (uses date index)
    def label_query():
        return (
            sb.table("stock_rebound_labels")
            .select(",".join(lcols))
            .gte("trade_date", start)
            .lte("trade_date", end)
            .not_.is_(last_future_col, "null")
            .order("trade_date")
        )

    labels = _fetch_all_by_offset(label_query, label=f"{sc['name']}/labels")
    if not labels:
        logger.warning("no labels for scenario=%s", sc["name"])
        return pd.DataFrame()

    # 2. Extract snapshot IDs referenced by labels and batch-load snapshots
    snap_ids = [int(r["feature_snapshot_id"]) for r in labels if r.get("feature_snapshot_id")]
    if not snap_ids:
        logger.warning("no feature_snapshot_ids in labels for scenario=%s", sc["name"])
        return pd.DataFrame()

    logger.info("scenario=%s loading %d snapshots by ID", sc["name"], len(snap_ids))
    snaps = _fetch_snapshots_by_ids(sb, snap_ids)
    if not snaps:
        logger.warning("no snapshots loaded for scenario=%s", sc["name"])
        return pd.DataFrame()

    s = pd.DataFrame(snaps)
    lbl = pd.DataFrame(labels).rename(columns={"id": "label_id", "trade_date": "lbl_trade_date", "code": "lbl_code"})
    df = s.merge(lbl, left_on="id", right_on="feature_snapshot_id", how="inner")
    logger.info("scenario=%s joined rows=%d", sc["name"], len(df))
    return df


# ── Outcome computation ───────────────────────────────────────────────────────

def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _first_hit(values: list[float | None], threshold: float, *, direction: str) -> int | None:
    for i, v in enumerate(values, start=1):
        if v is None:
            continue
        if direction == "up" and v >= threshold:
            return i
        if direction == "down" and v <= threshold:
            return i
    return None


def _compute_outcomes(df: "pd.DataFrame", tp_pct: float, sl_pct: float, horizon: int) -> "pd.DataFrame":
    results = []
    for _, row in df.iterrows():
        entry = _to_float(row.get("entry_price"))
        if entry is None or entry <= 0:
            results.append({"outcome": None, "realized_return": None, "holding_days_actual": None, "max_drawdown_computed": None})
            continue

        highs = [_to_float(row.get(f"future_high_{i}d")) for i in range(1, horizon + 1)]
        lows = [_to_float(row.get(f"future_low_{i}d")) for i in range(1, horizon + 1)]

        if any(v is None for v in highs + lows):
            results.append({"outcome": None, "realized_return": None, "holding_days_actual": None, "max_drawdown_computed": None})
            continue

        tp_price = entry * (1 + tp_pct / 100.0)
        sl_price = entry * (1 + sl_pct / 100.0)

        tp_day = _first_hit(highs, tp_price, direction="up")
        sl_day = _first_hit(lows, sl_price, direction="down")

        if tp_day is not None and (sl_day is None or tp_day < sl_day):
            outcome = True
            realized = tp_pct
            hold = tp_day
        elif sl_day is not None and (tp_day is None or sl_day <= tp_day):
            outcome = False
            realized = sl_pct
            hold = sl_day
        else:
            outcome = False
            realized = 0.0
            hold = horizon

        valid_lows = [v for v in lows if v is not None]
        max_dd = (min(valid_lows) / entry - 1.0) * 100.0 if valid_lows else None

        results.append({
            "outcome": outcome,
            "realized_return": realized,
            "holding_days_actual": hold,
            "max_drawdown_computed": max_dd,
        })

    out = pd.DataFrame(results, index=df.index)
    return pd.concat([df, out], axis=1)


# ── Metrics ───────────────────────────────────────────────────────────────────

def _win_rate(sub: "pd.DataFrame") -> float | None:
    valid = sub["outcome"].dropna()
    if valid.empty:
        return None
    return round(valid.mean() * 100.0, 1)


def _scenario_metrics(df: "pd.DataFrame", sc: dict, tp_pct: float, sl_pct: float,
                      horizon: int, min_prob: float, top_pct: float, signal_prob: float) -> dict:
    total = len(df)
    if total == 0:
        return _empty_metrics(sc)

    df = df.copy()
    df = df[df["outcome"].notna()]  # drop rows without future data

    # apply min_prob filter
    filtered = df[df["probability"] >= min_prob] if min_prob > 0 else df
    samples = len(filtered)

    if samples == 0:
        return _empty_metrics(sc)

    win_rate = _win_rate(filtered)
    avg_return = round(filtered["realized_return"].mean(), 3) if samples else None
    avg_prob = round(filtered["probability"].mean(), 4) if samples else None
    expected_return = round(
        (filtered["probability"] * tp_pct - (1 - filtered["probability"]) * abs(sl_pct)).mean(), 3
    ) if samples else None
    avg_max_dd = round(filtered["max_drawdown_computed"].mean(), 3) if samples else None
    avg_hold = round(filtered["holding_days_actual"].mean(), 1) if samples else None

    # top-N% by probability
    cutoff = float(filtered["probability"].quantile(1.0 - top_pct / 100.0))
    top_df = filtered[filtered["probability"] >= cutoff]
    top_win = _win_rate(top_df)

    # prob >= signal_prob
    sig_df = filtered[filtered["probability"] >= signal_prob]
    sig_win = _win_rate(sig_df)

    return {
        "scenario": sc["name"],
        "type": sc["type"],
        "period": f"{sc['start']}~{sc['end']}",
        "total": total,
        "samples": samples,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "expected_return": expected_return,
        "avg_prob": avg_prob,
        "top_pct_win_rate": top_win,
        "prob65_win_rate": sig_win,
        "avg_max_drawdown": avg_max_dd,
        "avg_holding_days": avg_hold,
    }


def _empty_metrics(sc: dict) -> dict:
    return {
        "scenario": sc["name"],
        "type": sc["type"],
        "period": f"{sc['start']}~{sc['end']}",
        "total": 0,
        "samples": 0,
        **{k: None for k in ["win_rate", "avg_return", "expected_return", "avg_prob",
                              "top_pct_win_rate", "prob65_win_rate", "avg_max_drawdown", "avg_holding_days"]},
    }


# ── Output ────────────────────────────────────────────────────────────────────

def _fmt(v: Any, decimals: int = 1, suffix: str = "") -> str:
    if v is None:
        return "-"
    try:
        return f"{float(v):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return str(v)


def _print_table(rows: list[dict], tp_pct: float, sl_pct: float, horizon: int,
                 top_pct: float, signal_prob: float) -> None:
    widths = [24, 8, 26, 7, 8, 9, 10, 14, 9, 14, 13, 13, 13]
    headers = [
        "scenario", "type", "period", "total", "samples",
        "win_rate", "avg_ret", "expected_ret", "avg_prob",
        f"top{int(top_pct)}%_wr", f"p{int(signal_prob*100)}+_wr",
        "avg_max_dd", "avg_hold_d",
    ]

    def hr():
        return "+-" + "-+-".join("-" * w for w in widths) + "-+"

    def row_str(cells):
        parts = [str(c if c is not None else "-").ljust(widths[i])[:widths[i]] for i, c in enumerate(cells)]
        return "| " + " | ".join(parts) + " |"

    print()
    print(f"=== Backtest Results  horizon={horizon}d  TP={tp_pct:+.1f}%  SL={sl_pct:.1f}%  "
          f"top_pct={top_pct}%  signal_prob>={signal_prob} ===")
    print(f"Run at: {datetime.now(JST).strftime('%Y-%m-%d %H:%M JST')}")
    print(hr())
    print(row_str(headers))
    print(hr())
    for r in rows:
        print(row_str([
            r["scenario"],
            r["type"],
            r["period"],
            r["total"],
            r["samples"],
            _fmt(r["win_rate"], 1, "%"),
            _fmt(r["avg_return"], 2, "%"),
            _fmt(r["expected_return"], 3, "%"),
            _fmt(r["avg_prob"], 3),
            _fmt(r["top_pct_win_rate"], 1, "%"),
            _fmt(r["prob65_win_rate"], 1, "%"),
            _fmt(r["avg_max_drawdown"], 2, "%"),
            _fmt(r["avg_holding_days"], 1, "d"),
        ]))
    print(hr())
    print()


def _write_csv(rows: list[dict], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV saved: %s", path)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas, numpy and joblib are required")

    horizon = int(args.horizon)
    if horizon not in HORIZON_DEFAULTS:
        raise ValueError(f"--horizon must be 5 or 20, got {horizon}")

    defaults = HORIZON_DEFAULTS[horizon]
    tp_pct = float(args.tp_pct) if args.tp_pct is not None else defaults["tp_pct"]
    sl_pct = float(args.sl_pct) if args.sl_pct is not None else defaults["sl_pct"]
    min_prob = float(args.min_prob)
    top_pct = float(args.top_pct)
    signal_prob = float(args.signal_prob)

    sb = _build_supabase()
    _model_row, bundle = _load_active_model(sb, horizon, args.model_name)

    results = []
    for sc in SCENARIOS:
        logger.info("--- scenario: %s (%s ~ %s) ---", sc["name"], sc["start"], sc["end"])
        df = _load_scenario_df(sb, sc, horizon)
        if df.empty:
            results.append(_empty_metrics(sc))
            continue

        df["probability"] = _predict(df, bundle)
        df = _compute_outcomes(df, tp_pct, sl_pct, horizon)
        metrics = _scenario_metrics(df, sc, tp_pct, sl_pct, horizon, min_prob, top_pct, signal_prob)
        results.append(metrics)
        logger.info(
            "  scenario=%s samples=%d win_rate=%s avg_ret=%s expected_ret=%s avg_prob=%s",
            metrics["scenario"],
            metrics["samples"],
            _fmt(metrics["win_rate"], 1, "%"),
            _fmt(metrics["avg_return"], 2, "%"),
            _fmt(metrics["expected_return"], 3, "%"),
            _fmt(metrics["avg_prob"], 3),
        )

    _print_table(results, tp_pct, sl_pct, horizon, top_pct, signal_prob)

    if args.output:
        _write_csv(results, args.output)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest active model across scenario periods (read-only)")
    p.add_argument("--horizon", choices=["5", "20"], default="5",
                   help="Evaluation horizon in trading days (default: 5)")
    p.add_argument("--tp-pct", type=float, default=None,
                   help="Take-profit %% (default: 5.0 for 5d, 10.0 for 20d)")
    p.add_argument("--sl-pct", type=float, default=None,
                   help="Stop-loss %% as negative (default: -3.0 for 5d, -5.0 for 20d)")
    p.add_argument("--min-prob", type=float, default=0.0,
                   help="Minimum model probability to include in evaluation (default: 0.0 = all)")
    p.add_argument("--top-pct", type=float, default=20.0,
                   help="Top-N%% by probability for top_pct_win_rate column (default: 20.0)")
    p.add_argument("--signal-prob", type=float, default=0.65,
                   help="Probability threshold for prob65_win_rate column (default: 0.65)")
    p.add_argument("--model-name", default=None,
                   help="Override active model name (default: auto-detect from ml_models)")
    p.add_argument("--output", default=None,
                   help="Save results to CSV at this path")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
