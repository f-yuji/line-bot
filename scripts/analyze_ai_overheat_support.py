#!/usr/bin/env python3
"""Analyze AI high-score × overheat condition × support gap intersection.

Key questions:
1. Do high AI-score stocks tend to be "overheated" (high RSI, far from MA)?
2. Does overheat status affect forward returns even for high-AI stocks?
3. Are "high AI + near support + not overheated" the best entries?
4. How does the triple filter compare to individual filters?

Outputs 4 CSV files to outputs/rebound_next_analysis/.

Usage:
    python scripts/analyze_ai_overheat_support.py
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

HOLD = 3
PULLBACK = 1.0
STOP = None


def _load_df(cache_path: Path) -> pd.DataFrame:
    with open(cache_path, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    df["trade_date"] = df["trade_date"].astype(str)
    logger.info("Loaded %d candidates", len(df))
    return df


def simulate_return(row: dict, hold: int = HOLD) -> Optional[float]:
    """Simple hold-N-days return with pullback exit."""
    entry = row.get("entry_price")
    if not entry or entry <= 0:
        return None
    peak = float(entry)
    for day in range(1, hold + 1):
        high = row.get(f"future_high_{day}d")
        low = row.get(f"future_low_{day}d")
        close = row.get(f"future_close_{day}d")
        if high is None or low is None or close is None:
            break
        try:
            high, close = float(high), float(close)
        except (TypeError, ValueError):
            break
        if np.isnan(high) or np.isnan(close):
            break
        peak = max(peak, high)
        if PULLBACK is not None and peak > entry * 1.005:
            if close <= peak * (1 - PULLBACK / 100):
                return (close - entry) / entry * 100
    close_f = row.get(f"future_close_{hold}d")
    if close_f is None:
        return None
    try:
        close_f = float(close_f)
    except (TypeError, ValueError):
        return None
    if np.isnan(close_f):
        return None
    return (close_f - entry) / entry * 100


def compute_metrics(valid: np.ndarray, label: str = "", split: str = "") -> dict:
    valid = np.array([v for v in valid if v is not None and not np.isnan(float(v))], dtype=float)
    n = len(valid)
    if n == 0:
        return {"label": label, "split": split, "n": 0,
                "win_rate": np.nan, "avg_ret": np.nan, "pf": np.nan, "max_loss": np.nan}
    wins = valid[valid > 0]
    losses = valid[valid < 0]
    pf = wins.sum() / abs(losses.sum()) if len(losses) > 0 and losses.sum() < 0 else 99.0
    return {
        "label": label,
        "split": split,
        "n": int(n),
        "win_rate": float(len(wins) / n * 100),
        "avg_ret": float(valid.mean()),
        "median_ret": float(np.median(valid)),
        "pf": float(min(pf, 99.0)),
        "max_loss": float(valid.min()),
    }


def compute_split_metrics(df: pd.DataFrame, rets: np.ndarray, label: str) -> list[dict]:
    rows = []
    for split, mask in [
        ("train", df["trade_date"].values <= TRAIN_END),
        ("test",  df["trade_date"].values >= TEST_START),
        ("all",   np.ones(len(df), dtype=bool)),
    ]:
        rows.append(compute_metrics(rets[mask], label=label, split=split))
    return rows


def _classify_overheat(df: pd.DataFrame) -> pd.DataFrame:
    """Add overheat flags based on RSI, MA5 gap, and recent return."""
    df = df.copy()
    # Individual overheat signals
    df["oh_rsi"] = df["rsi14"] >= 65
    df["oh_ma5"] = df["ma5_gap_pct"] >= 5
    df["oh_return5d"] = df["return_5d_pct"] >= 8
    df["oh_volume"] = df["volume_ratio_20d"] >= 3.0
    # Combined: any 2 or more overheat signals
    oh_score = (df["oh_rsi"].astype(int) + df["oh_ma5"].astype(int) +
                df["oh_return5d"].astype(int) + df["oh_volume"].astype(int))
    df["overheat_score"] = oh_score
    df["is_overheated"] = oh_score >= 2
    df["is_mild_overheat"] = oh_score == 1
    df["is_cool"] = oh_score == 0
    return df


def _classify_support(df: pd.DataFrame) -> pd.DataFrame:
    """Add support proximity flags."""
    df = df.copy()
    # Near MA25 support: within -7%
    df["near_ma25"] = df["ma25_gap_pct"] <= -3
    df["deep_ma25"] = df["ma25_gap_pct"] <= -7
    # Near MA75 support
    df["near_ma75"] = df["ma75_gap_pct"] <= -5
    # Significant drop from 20d high
    df["big_drop_20d"] = df["drop_from_20d_high_pct"] <= -7
    df["moderate_drop_20d"] = (df["drop_from_20d_high_pct"] <= -3) & (df["drop_from_20d_high_pct"] > -7)
    # "Good support setup": near MA25 and big drop
    df["good_support"] = df["near_ma25"] & df["big_drop_20d"]
    return df


def analyze_overheat_vs_return(df: pd.DataFrame, out_dir: Path) -> None:
    """OVH01: Returns by overheat score vs. non-overheated."""
    logger.info("OVH01: Returns by overheat level...")
    df = _classify_overheat(df)
    records = df.to_dict("records")
    rets = np.array([simulate_return(r) for r in records], dtype=object)

    rows = []
    for oh_label, mask in [
        ("cool (0 signals)",       df["is_cool"].values),
        ("mild (1 signal)",        df["is_mild_overheat"].values),
        ("overheated (2+ signals)",df["is_overheated"].values),
    ]:
        for m in compute_split_metrics(df[mask], rets[mask], oh_label):
            rows.append(m)

    # Also by individual overheat signal
    for sig_name, col in [("high_rsi>=65", "oh_rsi"), ("ma5_gap>=5%", "oh_ma5"),
                           ("return5d>=8%", "oh_return5d"), ("volume>=3x", "oh_volume")]:
        for has in [True, False]:
            label = f"{sig_name}={'yes' if has else 'no'}"
            mask = df[col].values == has
            for m in compute_split_metrics(df[mask], rets[mask], label):
                m["signal"] = sig_name
                m["has_signal"] = has
                rows.append(m)

    pd.DataFrame(rows).to_csv(out_dir / "OVH01_overheat_vs_return.csv", index=False)
    logger.info("  → OVH01_overheat_vs_return.csv (%d rows)", len(rows))


def analyze_ai_overheat_cross(df: pd.DataFrame, out_dir: Path) -> None:
    """OVH02: AI score group × overheat status cross-table."""
    logger.info("OVH02: AI score × overheat cross-analysis...")
    df = _classify_overheat(df)
    records = df.to_dict("records")
    rets = np.array([simulate_return(r) for r in records], dtype=object)

    ai_bins = [0.0, 0.40, 0.50, 0.55, 0.60, 1.0]
    ai_labels = ["<0.40", "0.40-0.50", "0.50-0.55", "0.55-0.60", ">=0.60"]
    df["ai_bucket"] = pd.cut(df["signal_probability"], bins=ai_bins, labels=ai_labels, right=False)

    rows = []
    for ai_b in ai_labels:
        for oh_b, oh_mask_name in [
            ("cool",     "is_cool"),
            ("mild",     "is_mild_overheat"),
            ("hot",      "is_overheated"),
        ]:
            mask = (df["ai_bucket"] == ai_b) & (df[oh_mask_name].values)
            if mask.sum() == 0:
                continue
            label = f"ai={ai_b}_oh={oh_b}"
            for m in compute_split_metrics(df[mask], rets[mask], label):
                m["ai_bucket"] = ai_b
                m["overheat"] = oh_b
                rows.append(m)

    pd.DataFrame(rows).to_csv(out_dir / "OVH02_ai_overheat_cross.csv", index=False)
    logger.info("  → OVH02_ai_overheat_cross.csv (%d rows)", len(rows))


def analyze_triple_filter(df: pd.DataFrame, out_dir: Path) -> None:
    """OVH03: Triple filter: high AI × near support × not overheated."""
    logger.info("OVH03: Triple filter analysis...")
    df = _classify_overheat(df)
    df = _classify_support(df)
    records = df.to_dict("records")
    rets = np.array([simulate_return(r) for r in records], dtype=object)

    configs = [
        ("all",                             {}),
        ("ai>=0.50",                        {"ai": 0.50}),
        ("ai>=0.55",                        {"ai": 0.55}),
        ("near_support",                    {"support": True}),
        ("not_overheated",                  {"cool": True}),
        ("ai>=0.50+support",                {"ai": 0.50, "support": True}),
        ("ai>=0.50+cool",                   {"ai": 0.50, "cool": True}),
        ("ai>=0.50+support+cool",           {"ai": 0.50, "support": True, "cool": True}),
        ("ai>=0.55+support+cool",           {"ai": 0.55, "support": True, "cool": True}),
        ("ai>=0.55+deep_ma25+cool",         {"ai": 0.55, "deep_ma25": True, "cool": True}),
        ("ai>=0.60+support+cool",           {"ai": 0.60, "support": True, "cool": True}),
    ]

    rows = []
    for label, cfg in configs:
        mask = np.ones(len(df), dtype=bool)
        if "ai" in cfg:
            mask &= df["signal_probability"].values >= cfg["ai"]
        if cfg.get("support"):
            mask &= df["good_support"].values
        if cfg.get("deep_ma25"):
            mask &= df["deep_ma25"].values
        if cfg.get("cool"):
            mask &= df["is_cool"].values

        for m in compute_split_metrics(df[mask], rets[mask], label):
            m.update({k: v for k, v in cfg.items()})
            rows.append(m)

    pd.DataFrame(rows).to_csv(out_dir / "OVH03_triple_filter.csv", index=False)
    logger.info("  → OVH03_triple_filter.csv (%d rows)", len(rows))


def analyze_entry_quality_score(df: pd.DataFrame, out_dir: Path) -> None:
    """OVH04: Composite entry quality score (AI + support + cool)."""
    logger.info("OVH04: Entry quality score breakdown...")
    df = _classify_overheat(df)
    df = _classify_support(df)
    records = df.to_dict("records")
    rets = np.array([simulate_return(r) for r in records], dtype=object)

    # Quality score: 0–4
    # +1 for signal_probability >= 0.50
    # +1 for signal_probability >= 0.55
    # +1 for good_support (near MA25 + big drop)
    # +1 for not overheated
    # -1 for overheated
    df["quality_score"] = (
        (df["signal_probability"] >= 0.50).astype(int) +
        (df["signal_probability"] >= 0.55).astype(int) +
        df["good_support"].astype(int) +
        df["is_cool"].astype(int) -
        df["is_overheated"].astype(int)
    )

    rows = []
    for score in sorted(df["quality_score"].unique()):
        mask = df["quality_score"].values == score
        label = f"quality={score}"
        for m in compute_split_metrics(df[mask], rets[mask], label):
            m["quality_score"] = score
            rows.append(m)

    # Also cumulative: quality >= N
    for min_score in [0, 1, 2, 3]:
        mask = df["quality_score"].values >= min_score
        label = f"quality>={min_score}"
        for m in compute_split_metrics(df[mask], rets[mask], label):
            m["min_quality"] = min_score
            rows.append(m)

    pd.DataFrame(rows).to_csv(out_dir / "OVH04_entry_quality_score.csv", index=False)
    logger.info("  → OVH04_entry_quality_score.csv (%d rows)", len(rows))


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

    analyze_overheat_vs_return(df, out_dir)
    analyze_ai_overheat_cross(df, out_dir)
    analyze_triple_filter(df, out_dir)
    analyze_entry_quality_score(df, out_dir)

    logger.info("Done. AI/overheat/support analysis complete.")


if __name__ == "__main__":
    main()
