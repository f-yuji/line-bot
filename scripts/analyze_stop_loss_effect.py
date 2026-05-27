#!/usr/bin/env python3
"""Analyze whether stop-loss actually helps in rebound strategies.

For each candidate, simulates:
1. Was stop loss triggered within N days?
2. What did the price do AFTER the stop was hit?
3. Compare: "with stop" vs "without stop" return

Outputs 6 CSV files to outputs/rebound_next_analysis/.

Usage:
    python scripts/analyze_stop_loss_effect.py
"""

from __future__ import annotations

import argparse
import logging
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = ROOT / "outputs" / "rebound_grid_search" / "cands_2020_2026.pkl"
OUT_DIR = ROOT / "outputs" / "rebound_next_analysis"

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"


def _load_df(cache_path: Path) -> pd.DataFrame:
    with open(cache_path, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    df["trade_date"] = df["trade_date"].astype(str)
    logger.info("Loaded %d candidates", len(df))
    return df


def _get_float(row: dict, key: str) -> Optional[float]:
    v = row.get(key)
    if v is None:
        return None
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _find_stop_day(row: dict, max_hold: int, stop_pct: float) -> Optional[int]:
    """Return the first day when stop is triggered, or None."""
    entry = _get_float(row, "entry_price")
    if entry is None or entry <= 0:
        return None
    stop_price = entry * (1 + stop_pct / 100)
    for day in range(1, max_hold + 1):
        low = _get_float(row, f"future_low_{day}d")
        if low is None:
            return None
        if low <= stop_price:
            return day
    return None


def _return_without_stop(row: dict, hold: int) -> Optional[float]:
    """Return if holding to day `hold` (no stop)."""
    entry = _get_float(row, "entry_price")
    close = _get_float(row, f"future_close_{hold}d")
    if entry is None or close is None or entry <= 0:
        return None
    return (close - entry) / entry * 100


def _return_after_stop(row: dict, stop_day: int, extra_days: int) -> Optional[float]:
    """Return from stop_day price to stop_day + extra_days close."""
    # We approximate the stop execution price as entry * (1 + stop_pct/100)
    # But we don't have a "close at stop day" easily. Use stop_day close as proxy.
    stop_close = _get_float(row, f"future_close_{stop_day}d")
    future_day = stop_day + extra_days
    if future_day > 20:
        return None
    future_close = _get_float(row, f"future_close_{future_day}d")
    if stop_close is None or future_close is None or stop_close <= 0:
        return None
    return (future_close - stop_close) / stop_close * 100


def build_stop_triggered_df(df: pd.DataFrame, stop_pct: float, max_hold: int) -> pd.DataFrame:
    """For each row, determine if stop was triggered and compute outcomes."""
    records = df.to_dict("records")
    rows = []
    for row in records:
        entry = _get_float(row, "entry_price")
        if entry is None:
            continue
        stop_day = _find_stop_day(row, max_hold, stop_pct)
        triggered = stop_day is not None

        # Return WITH stop loss
        ret_with_stop = stop_pct if triggered else _return_without_stop(row, max_hold)
        # Return WITHOUT stop loss (hold to max_hold regardless)
        ret_no_stop = _return_without_stop(row, max_hold)
        # Post-stop recovery: what happened after stop triggered?
        post_3d = _return_after_stop(row, stop_day, 3) if triggered else None
        post_5d = _return_after_stop(row, stop_day, 5) if triggered else None
        post_10d = _return_after_stop(row, stop_day, 10) if triggered else None

        rows.append({
            "trade_date": row.get("trade_date"),
            "market_regime": row.get("market_regime"),
            "signal_probability": row.get("signal_probability"),
            "stop_triggered": triggered,
            "stop_day": stop_day,
            "ret_with_stop": ret_with_stop,
            "ret_no_stop": ret_no_stop,
            "stop_benefit": (ret_with_stop - ret_no_stop) if (ret_with_stop is not None and ret_no_stop is not None) else None,
            "post_stop_3d": post_3d,
            "post_stop_5d": post_5d,
            "post_stop_10d": post_10d,
        })
    return pd.DataFrame(rows)


def analyze_stop_trigger_rate(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("SL01: Stop trigger rate by stop_pct and hold period...")
    rows = []
    for stop_pct in [-3.0, -4.0, -5.0, -6.0, -8.0, -10.0]:
        for hold in [3, 5, 10]:
            result = build_stop_triggered_df(df, stop_pct, hold)
            triggered = result[result["stop_triggered"]]
            not_triggered = result[~result["stop_triggered"]]
            rows.append({
                "stop_pct": stop_pct,
                "max_hold": hold,
                "total_trades": len(result),
                "stop_trigger_count": len(triggered),
                "stop_trigger_rate": len(triggered) / len(result) * 100 if len(result) > 0 else 0,
                "avg_ret_with_stop": result["ret_with_stop"].mean(),
                "avg_ret_no_stop": result["ret_no_stop"].mean(),
                "avg_stop_benefit": result["stop_benefit"].mean(),
                "triggered_avg_ret_without": triggered["ret_no_stop"].mean() if len(triggered) > 0 else np.nan,
                "triggered_avg_post3d": triggered["post_stop_3d"].mean() if len(triggered) > 0 else np.nan,
                "triggered_avg_post5d": triggered["post_stop_5d"].mean() if len(triggered) > 0 else np.nan,
            })
    pd.DataFrame(rows).to_csv(out_dir / "SL01_stop_trigger_rate.csv", index=False)
    logger.info("  → SL01_stop_trigger_rate.csv (%d rows)", len(rows))


def analyze_post_stop_recovery(df: pd.DataFrame, out_dir: Path) -> None:
    """SL02: After stop triggered at -5%, where does price go next?"""
    logger.info("SL02: Post-stop price recovery analysis (stop=-5%, hold=3)...")
    result = build_stop_triggered_df(df, -5.0, 3)
    triggered = result[result["stop_triggered"]].copy()
    triggered["split"] = np.where(triggered["trade_date"] <= TRAIN_END, "train", "test")

    # Classify post-stop behavior
    triggered["recovered_3d"] = triggered["post_stop_3d"] > 0
    triggered["recovered_5d"] = triggered["post_stop_5d"] > 0

    rows = []
    for split in ["train", "test", "all"]:
        grp = triggered if split == "all" else triggered[triggered["split"] == split]
        if len(grp) == 0:
            continue
        rows.append({
            "split": split,
            "stop_triggered_count": len(grp),
            "pct_recovered_3d": grp["recovered_3d"].mean() * 100,
            "pct_recovered_5d": grp["recovered_5d"].mean() * 100,
            "avg_post_stop_3d": grp["post_stop_3d"].mean(),
            "avg_post_stop_5d": grp["post_stop_5d"].mean(),
            "avg_post_stop_10d": grp["post_stop_10d"].mean(),
            "avg_no_stop_return": grp["ret_no_stop"].mean(),
            "avg_stop_benefit": grp["stop_benefit"].mean(),
            "stop_hurt_count": (grp["stop_benefit"] < 0).sum(),
            "stop_helped_count": (grp["stop_benefit"] > 0).sum(),
        })
    pd.DataFrame(rows).to_csv(out_dir / "SL02_post_stop_recovery.csv", index=False)
    logger.info("  → SL02_post_stop_recovery.csv (%d rows)", len(rows))


def analyze_stop_by_regime(df: pd.DataFrame, out_dir: Path) -> None:
    """SL03: Stop-loss effectiveness by market regime."""
    logger.info("SL03: Stop-loss effect by regime (stop=-5%, hold=3)...")
    result = build_stop_triggered_df(df, -5.0, 3)
    result["split"] = np.where(result["trade_date"] <= TRAIN_END, "train", "test")
    rows = []
    for regime in result["market_regime"].dropna().unique():
        for split in ["train", "test", "all"]:
            grp = result[result["market_regime"] == regime]
            if split != "all":
                grp = grp[grp["split"] == split]
            if len(grp) == 0:
                continue
            triggered = grp[grp["stop_triggered"]]
            rows.append({
                "regime": regime,
                "split": split,
                "total": len(grp),
                "trigger_rate": len(triggered) / len(grp) * 100,
                "avg_ret_with_stop": grp["ret_with_stop"].mean(),
                "avg_ret_no_stop": grp["ret_no_stop"].mean(),
                "avg_stop_benefit": grp["stop_benefit"].mean(),
                "avg_post3d_after_stop": triggered["post_stop_3d"].mean() if len(triggered) > 0 else np.nan,
            })
    pd.DataFrame(rows).to_csv(out_dir / "SL03_stop_by_regime.csv", index=False)
    logger.info("  → SL03_stop_by_regime.csv (%d rows)", len(rows))


def analyze_stop_vs_no_stop_distribution(df: pd.DataFrame, out_dir: Path) -> None:
    """SL04: Distribution comparison — with stop vs. without stop."""
    logger.info("SL04: Return distribution with vs without stop (-5%, hold=3)...")
    result = build_stop_triggered_df(df, -5.0, 3)
    result["split"] = np.where(result["trade_date"] <= TRAIN_END, "train", "test")

    rows = []
    for split in ["train", "test", "all"]:
        grp = result if split == "all" else result[result["split"] == split]
        for col, label in [("ret_with_stop", "with_stop_5pct"), ("ret_no_stop", "no_stop")]:
            valid = grp[col].dropna()
            if len(valid) == 0:
                continue
            wins = valid[valid > 0]
            losses = valid[valid < 0]
            pf = wins.sum() / abs(losses.sum()) if losses.sum() < 0 else 99.0
            rows.append({
                "split": split,
                "scenario": label,
                "n": len(valid),
                "win_rate": len(wins) / len(valid) * 100,
                "avg_ret": valid.mean(),
                "median_ret": valid.median(),
                "pf": min(pf, 99.0),
                "max_loss": valid.min(),
                "pct_loss_gt5": (valid < -5).mean() * 100,
                "pct_loss_gt10": (valid < -10).mean() * 100,
            })
    pd.DataFrame(rows).to_csv(out_dir / "SL04_stop_distribution.csv", index=False)
    logger.info("  → SL04_stop_distribution.csv (%d rows)", len(rows))


def analyze_stop_by_ai_score(df: pd.DataFrame, out_dir: Path) -> None:
    """SL05: Does high AI score reduce stop-loss benefit?"""
    logger.info("SL05: Stop effect by AI score group (stop=-5%, hold=3)...")
    result = build_stop_triggered_df(df, -5.0, 3)
    result["signal_probability"] = df["signal_probability"].values[:len(result)]
    result["split"] = np.where(result["trade_date"] <= TRAIN_END, "train", "test")

    bins = [0.0, 0.40, 0.50, 0.60, 1.0]
    labels = ["<0.40", "0.40-0.50", "0.50-0.60", ">=0.60"]
    result["ai_bucket"] = pd.cut(result["signal_probability"], bins=bins, labels=labels, right=False)

    rows = []
    for ai_b in labels:
        for split in ["train", "test", "all"]:
            grp = result[result["ai_bucket"] == ai_b]
            if split != "all":
                grp = grp[grp["split"] == split]
            if len(grp) == 0:
                continue
            triggered = grp[grp["stop_triggered"]]
            rows.append({
                "ai_bucket": ai_b,
                "split": split,
                "n": len(grp),
                "trigger_rate": len(triggered) / len(grp) * 100,
                "avg_ret_with_stop": grp["ret_with_stop"].mean(),
                "avg_ret_no_stop": grp["ret_no_stop"].mean(),
                "avg_stop_benefit": grp["stop_benefit"].mean(),
            })
    pd.DataFrame(rows).to_csv(out_dir / "SL05_stop_by_ai_score.csv", index=False)
    logger.info("  → SL05_stop_by_ai_score.csv (%d rows)", len(rows))


def build_stop_summary(out_dir: Path) -> None:
    """SL06: Summary of stop-loss findings."""
    logger.info("SL06: Building stop-loss summary...")
    try:
        rate = pd.read_csv(out_dir / "SL01_stop_trigger_rate.csv")
        recovery = pd.read_csv(out_dir / "SL02_post_stop_recovery.csv")
    except Exception as e:
        logger.warning("  Cannot build summary: %s", e)
        return

    # Key finding: is avg_stop_benefit positive (stop helps) or negative (stop hurts)?
    rate["stop_helps"] = rate["avg_stop_benefit"] > 0
    summary_rows = []
    for _, row in rate.iterrows():
        summary_rows.append({
            "stop_pct": row["stop_pct"],
            "max_hold": row["max_hold"],
            "trigger_rate": row["stop_trigger_rate"],
            "avg_benefit": row["avg_stop_benefit"],
            "verdict": "HELPS" if row["avg_stop_benefit"] > 0 else "HURTS",
        })
    pd.DataFrame(summary_rows).to_csv(out_dir / "SL06_stop_loss_summary.csv", index=False)
    logger.info("  → SL06_stop_loss_summary.csv (%d rows)", len(summary_rows))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--cache-path", default=str(DEFAULT_CACHE))
    p.add_argument("--out-dir", default=str(OUT_DIR))
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_df(Path(args.cache_path))

    analyze_stop_trigger_rate(df, out_dir)
    analyze_post_stop_recovery(df, out_dir)
    analyze_stop_by_regime(df, out_dir)
    analyze_stop_vs_no_stop_distribution(df, out_dir)
    analyze_stop_by_ai_score(df, out_dir)
    build_stop_summary(out_dir)

    logger.info("Done. Stop-loss analysis complete.")


if __name__ == "__main__":
    main()
