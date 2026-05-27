#!/usr/bin/env python3
"""Comprehensive rebound rule analysis using pre-cached candidates.

Sweeps over AI score thresholds, support gap filters, overheat conditions,
stop-loss models, pullback exit widths, holding periods, and regime filters.

Outputs 18 CSV files to outputs/rebound_next_analysis/.

Usage:
    python scripts/analyze_next_rebound_rules.py
    python scripts/analyze_next_rebound_rules.py --cache-path outputs/rebound_grid_search/cands_2020_2026.pkl
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

# Baseline simulation params (derived from grid search winning patterns)
BASELINE_HOLD = 3
BASELINE_PULLBACK = 1.0   # 1% trailing pullback exit
BASELINE_STOP = None      # no fixed stop loss
BASELINE_REGIME = "all"   # no regime filter (OOS-passing pattern)


# ── Data loading ─────────────────────────────────────────────────────────────

def _load_df(cache_path: Path) -> pd.DataFrame:
    logger.info("Loading candidates from %s", cache_path)
    with open(cache_path, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    logger.info("Loaded %d candidates", len(df))
    # Ensure trade_date is string for comparison
    df["trade_date"] = df["trade_date"].astype(str)
    return df


# ── Trade simulation ──────────────────────────────────────────────────────────

def simulate_trade(row: dict, max_hold: int,
                   stop_pct: Optional[float] = None,
                   pullback_pct: Optional[float] = None) -> Optional[float]:
    """Simulate a single trade. Returns return_pct or None if no data."""
    entry = row.get("entry_price")
    if not entry or entry <= 0:
        return None

    stop_price = entry * (1 + stop_pct / 100) if stop_pct is not None else None
    peak = float(entry)

    for day in range(1, max_hold + 1):
        high = row.get(f"future_high_{day}d")
        low = row.get(f"future_low_{day}d")
        close = row.get(f"future_close_{day}d")

        if high is None or low is None or close is None:
            break
        try:
            high, low, close = float(high), float(low), float(close)
        except (TypeError, ValueError):
            break
        if np.isnan(high) or np.isnan(low) or np.isnan(close):
            break

        peak = max(peak, high)

        # Stop loss: intraday low touches stop price
        if stop_price is not None and low <= stop_price:
            return stop_pct

        # Pullback exit: close drops pullback_pct% from running peak
        if pullback_pct is not None and peak > entry * 1.005:
            if close <= peak * (1 - pullback_pct / 100):
                return (close - entry) / entry * 100

    close_final = row.get(f"future_close_{max_hold}d")
    if close_final is None:
        return None
    try:
        close_final = float(close_final)
    except (TypeError, ValueError):
        return None
    if np.isnan(close_final):
        return None
    return (close_final - entry) / entry * 100


def simulate_batch(df: pd.DataFrame, max_hold: int = 3,
                   stop_pct: Optional[float] = None,
                   pullback_pct: Optional[float] = None) -> np.ndarray:
    records = df.to_dict("records")
    return np.array([simulate_trade(r, max_hold, stop_pct, pullback_pct) for r in records],
                    dtype=object)


# ── Metrics ──────────────────────────────────────────────────────────────────

def metrics(rets: np.ndarray, label: str = "") -> dict:
    valid = np.array([r for r in rets if r is not None and not np.isnan(float(r))], dtype=float)
    n = len(valid)
    if n == 0:
        return {k: (label if k == "label" else (n if k == "n" else np.nan))
                for k in ["label", "n", "win_rate", "avg_ret", "median_ret", "pf",
                          "max_loss", "best_ret", "avg_win", "avg_loss"]}
    wins = valid[valid > 0]
    losses = valid[valid < 0]
    sum_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    pf = wins.sum() / sum_loss if sum_loss > 0 else (99.0 if wins.sum() > 0 else 1.0)
    return {
        "label": label,
        "n": int(n),
        "win_rate": float(len(wins) / n * 100),
        "avg_ret": float(valid.mean()),
        "median_ret": float(np.median(valid)),
        "pf": float(min(pf, 99.0)),
        "max_loss": float(valid.min()),
        "best_ret": float(valid.max()),
        "avg_win": float(wins.mean()) if len(wins) > 0 else 0.0,
        "avg_loss": float(losses.mean()) if len(losses) > 0 else 0.0,
    }


def split_metrics(df: pd.DataFrame, rets: np.ndarray, label: str = "") -> list[dict]:
    """Compute metrics for train, test, and all splits."""
    rows = []
    for split, mask in [
        ("train", df["trade_date"].values <= TRAIN_END),
        ("test",  df["trade_date"].values >= TEST_START),
        ("all",   np.ones(len(df), dtype=bool)),
    ]:
        m = metrics(rets[mask], label=label)
        m["split"] = split
        rows.append(m)
    return rows


# ── Regime filter ─────────────────────────────────────────────────────────────

def apply_regime(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "all":
        return df
    if mode == "no_panic_selloff":
        return df[df["market_regime"] != "panic_selloff"]
    if mode == "no_euphoria":
        return df[df["market_regime"] != "euphoria"]
    if mode == "no_risk_off":
        return df[df["market_regime"] != "risk_off"]
    if mode == "normal_only":
        return df[df["market_regime"] == "normal"]
    if mode == "panic_only":
        return df[df["market_regime"] == "panic_rebound"]
    if mode == "strong_risk_on":
        return df[df["market_regime"] == "strong_risk_on"]
    return df


# ── Analysis functions ────────────────────────────────────────────────────────

def analyze_ai_score_threshold(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("01: AI score threshold sweep...")
    rows = []
    for thresh in [None, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65]:
        label = f"prob>={thresh}" if thresh is not None else "all"
        sub = df[df["signal_probability"] >= thresh] if thresh is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["ai_threshold"] = thresh
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "01_ai_score_threshold.csv", index=False)
    logger.info("  → 01_ai_score_threshold.csv (%d rows)", len(rows))


def analyze_rule_score_threshold(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("02: Rule score threshold sweep...")
    rows = []
    for thresh in [None, 50, 55, 60, 65, 70, 75]:
        label = f"rule>={thresh}" if thresh is not None else "all"
        sub = df[df["rule_score"] >= thresh] if thresh is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["rule_threshold"] = thresh
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "02_rule_score_threshold.csv", index=False)
    logger.info("  → 02_rule_score_threshold.csv (%d rows)", len(rows))


def analyze_support_gap_ma25(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("03: Support gap MA25 filter...")
    rows = []
    # ma25_gap_pct < 0 means below MA25 (good for rebound entry)
    for max_gap in [None, -2, -3, -5, -7, -10]:
        label = f"ma25<={max_gap}%" if max_gap is not None else "all"
        sub = df[df["ma25_gap_pct"] <= max_gap] if max_gap is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["ma25_gap_max"] = max_gap
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "03_support_gap_ma25.csv", index=False)
    logger.info("  → 03_support_gap_ma25.csv (%d rows)", len(rows))


def analyze_support_gap_ma75(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("04: Support gap MA75 filter...")
    rows = []
    for max_gap in [None, -2, -5, -10, -15, -20]:
        label = f"ma75<={max_gap}%" if max_gap is not None else "all"
        sub = df[df["ma75_gap_pct"] <= max_gap] if max_gap is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["ma75_gap_max"] = max_gap
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "04_support_gap_ma75.csv", index=False)
    logger.info("  → 04_support_gap_ma75.csv (%d rows)", len(rows))


def analyze_support_gap_20d(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("05: Support gap 20d-low (drop from 20d high) filter...")
    rows = []
    for max_drop in [None, -3, -5, -7, -10, -15]:
        label = f"drop20d<={max_drop}%" if max_drop is not None else "all"
        sub = df[df["drop_from_20d_high_pct"] <= max_drop] if max_drop is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["drop_20d_max"] = max_drop
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "05_support_gap_20d_low.csv", index=False)
    logger.info("  → 05_support_gap_20d_low.csv (%d rows)", len(rows))


def analyze_overheat_rsi(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("06: Overheat RSI filter...")
    rows = []
    for max_rsi in [None, 80, 70, 65, 60, 55, 50]:
        label = f"rsi<={max_rsi}" if max_rsi is not None else "all"
        sub = df[df["rsi14"] <= max_rsi] if max_rsi is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["rsi_max"] = max_rsi
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "06_overheat_rsi.csv", index=False)
    logger.info("  → 06_overheat_rsi.csv (%d rows)", len(rows))


def analyze_overheat_ma5(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("07: Overheat MA5 gap filter (exclude if too far above MA5)...")
    rows = []
    for max_ma5 in [None, 15, 10, 8, 5, 3]:
        label = f"ma5_gap<={max_ma5}%" if max_ma5 is not None else "all"
        sub = df[df["ma5_gap_pct"] <= max_ma5] if max_ma5 is not None else df
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, label):
            m["ma5_gap_max"] = max_ma5
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "07_overheat_ma5.csv", index=False)
    logger.info("  → 07_overheat_ma5.csv (%d rows)", len(rows))


def analyze_stop_loss_models(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("08: Stop-loss model comparison...")
    rows = []
    for stop, label in [
        (None,  "no_stop"),
        (-3.0,  "stop_-3%"),
        (-4.0,  "stop_-4%"),
        (-5.0,  "stop_-5%"),
        (-6.0,  "stop_-6%"),
        (-8.0,  "stop_-8%"),
        (-10.0, "stop_-10%"),
    ]:
        rets = simulate_batch(df, BASELINE_HOLD, stop, BASELINE_PULLBACK)
        for m in split_metrics(df, rets, label):
            m["stop_pct"] = stop
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "08_stop_loss_model.csv", index=False)
    logger.info("  → 08_stop_loss_model.csv (%d rows)", len(rows))


def analyze_pullback_width(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("09: Pullback exit width sweep...")
    rows = []
    for pb, label in [
        (None, "no_pullback"),
        (0.5,  "pullback_0.5%"),
        (1.0,  "pullback_1.0%"),
        (1.25, "pullback_1.25%"),
        (1.5,  "pullback_1.5%"),
        (1.75, "pullback_1.75%"),
        (2.0,  "pullback_2.0%"),
    ]:
        rets = simulate_batch(df, BASELINE_HOLD, BASELINE_STOP, pb)
        for m in split_metrics(df, rets, label):
            m["pullback_pct"] = pb
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "09_pullback_exit_width.csv", index=False)
    logger.info("  → 09_pullback_exit_width.csv (%d rows)", len(rows))


def analyze_holding_days(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("10: Holding period sweep...")
    rows = []
    for hold in [2, 3, 4, 5, 7, 10]:
        label = f"hold_{hold}d"
        rets = simulate_batch(df, hold, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(df, rets, label):
            m["max_hold"] = hold
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "10_holding_days.csv", index=False)
    logger.info("  → 10_holding_days.csv (%d rows)", len(rows))


def analyze_regime_filter(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("11: Regime filter comparison...")
    rows = []
    for mode in ["all", "no_panic_selloff", "no_euphoria", "no_risk_off",
                 "normal_only", "strong_risk_on", "panic_only"]:
        sub = apply_regime(df, mode)
        rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
        for m in split_metrics(sub, rets, mode):
            m["regime_filter"] = mode
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "11_regime_filter.csv", index=False)
    logger.info("  → 11_regime_filter.csv (%d rows)", len(rows))


def analyze_combined_ai_support(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("12: Combined AI score × support gap (MA25) grid...")
    rows = []
    ai_thresholds = [None, 0.45, 0.50, 0.55, 0.60]
    support_gaps = [None, -3, -5, -7]
    for ai_t in ai_thresholds:
        for gap in support_gaps:
            sub = df.copy()
            if ai_t is not None:
                sub = sub[sub["signal_probability"] >= ai_t]
            if gap is not None:
                sub = sub[sub["ma25_gap_pct"] <= gap]
            label = f"ai>={ai_t}_ma25<={gap}"
            rets = simulate_batch(sub, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
            for m in split_metrics(sub, rets, label):
                m["ai_threshold"] = ai_t
                m["ma25_gap_max"] = gap
                rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "12_combined_ai_support.csv", index=False)
    logger.info("  → 12_combined_ai_support.csv (%d rows)", len(rows))


def analyze_combined_hold_pullback(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("13: Combined holding days × pullback width grid...")
    rows = []
    for hold in [2, 3, 4, 5]:
        for pb in [None, 0.5, 1.0, 1.5, 2.0]:
            label = f"hold{hold}_pb{pb}"
            rets = simulate_batch(df, hold, BASELINE_STOP, pb)
            for m in split_metrics(df, rets, label):
                m["max_hold"] = hold
                m["pullback_pct"] = pb
                rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "13_combined_hold_pullback.csv", index=False)
    logger.info("  → 13_combined_hold_pullback.csv (%d rows)", len(rows))


def analyze_hypothesis_validation(df: pd.DataFrame, out_dir: Path) -> None:
    """14: Main hypothesis: score>=0.55 × drop_20d<=-8% × pullback1.5 × hold=3 × no_panic_selloff."""
    logger.info("14: Hypothesis validation...")
    rows = []
    configs = [
        ("baseline_all", dict(ai=None, drop20=None, hold=3, pb=1.0, stop=None, regime="all")),
        ("h1_ai55",      dict(ai=0.55, drop20=None, hold=3, pb=1.0, stop=None, regime="all")),
        ("h2_ai55_drop8",dict(ai=0.55, drop20=-8,   hold=3, pb=1.0, stop=None, regime="all")),
        ("h3_ai55_drop8_pb15",   dict(ai=0.55, drop20=-8, hold=3, pb=1.5,  stop=None,  regime="all")),
        ("h4_ai55_drop8_pb15_np",dict(ai=0.55, drop20=-8, hold=3, pb=1.5,  stop=None,  regime="no_panic_selloff")),
        ("h5_ai60_drop8_pb15_np",dict(ai=0.60, drop20=-8, hold=3, pb=1.5,  stop=None,  regime="no_panic_selloff")),
        ("h6_ai55_drop5_hold3",  dict(ai=0.55, drop20=-5, hold=3, pb=1.0,  stop=None,  regime="no_panic_selloff")),
        ("h7_ai55_drop8_hold5",  dict(ai=0.55, drop20=-8, hold=5, pb=1.0,  stop=None,  regime="no_panic_selloff")),
        ("h8_ai55_drop8_stop6",  dict(ai=0.55, drop20=-8, hold=3, pb=1.0,  stop=-6.0,  regime="no_panic_selloff")),
        ("h9_ai55_drop8_em10",   dict(ai=0.55, drop20=-8, hold=3, pb=1.0,  stop=-10.0, regime="no_panic_selloff")),
    ]
    for label, cfg in configs:
        sub = apply_regime(df, cfg["regime"])
        if cfg["ai"] is not None:
            sub = sub[sub["signal_probability"] >= cfg["ai"]]
        if cfg["drop20"] is not None:
            sub = sub[sub["drop_from_20d_high_pct"] <= cfg["drop20"]]
        rets = simulate_batch(sub, cfg["hold"], cfg["stop"], cfg["pb"])
        for m in split_metrics(sub, rets, label):
            m.update({k: v for k, v in cfg.items()})
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "14_hypothesis_validation.csv", index=False)
    logger.info("  → 14_hypothesis_validation.csv (%d rows)", len(rows))


def analyze_monthly_returns(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("15: Monthly returns (baseline)...")
    rets = simulate_batch(df, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
    df2 = df.copy()
    df2["_ret"] = rets
    df2["_month"] = df2["trade_date"].str[:7]
    rows = []
    for month, grp in df2.groupby("_month"):
        m = metrics(grp["_ret"].values, label=month)
        m["month"] = month
        m["split"] = "train" if month <= "2024-12" else "test"
        rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "15_monthly_returns.csv", index=False)
    logger.info("  → 15_monthly_returns.csv (%d rows)", len(rows))


def analyze_year_breakdown(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("16: Year breakdown (baseline)...")
    rets = simulate_batch(df, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
    df2 = df.copy()
    df2["_ret"] = rets
    df2["_year"] = df2["trade_date"].str[:4]
    rows = []
    for year, grp in df2.groupby("_year"):
        m = metrics(grp["_ret"].values, label=year)
        m["year"] = int(year)
        m["split"] = "train" if year <= "2024" else "test"
        rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "16_year_breakdown.csv", index=False)
    logger.info("  → 16_year_breakdown.csv (%d rows)", len(rows))


def analyze_regime_return_matrix(df: pd.DataFrame, out_dir: Path) -> None:
    logger.info("17: Regime × return matrix...")
    rets = simulate_batch(df, BASELINE_HOLD, BASELINE_STOP, BASELINE_PULLBACK)
    df2 = df.copy()
    df2["_ret"] = rets
    rows = []
    for regime, grp in df2.groupby("market_regime"):
        for split, sgrp in [
            ("train", grp[grp["trade_date"] <= TRAIN_END]),
            ("test",  grp[grp["trade_date"] >= TEST_START]),
            ("all",   grp),
        ]:
            m = metrics(sgrp["_ret"].values, label=f"{regime}_{split}")
            m["regime"] = regime
            m["split"] = split
            rows.append(m)
    pd.DataFrame(rows).to_csv(out_dir / "17_regime_return_matrix.csv", index=False)
    logger.info("  → 17_regime_return_matrix.csv (%d rows)", len(rows))


def analyze_h5_monthly(df: pd.DataFrame, out_dir: Path) -> None:
    """19: H5条件の月次トレード件数・成績."""
    logger.info("19: H5 monthly trade count and performance...")
    # H5 filter: ai>=0.60, drop_20d<=-8%, no_panic_selloff
    sub = apply_regime(df, "no_panic_selloff")
    sub = sub[sub["signal_probability"] >= 0.60]
    sub = sub[sub["drop_from_20d_high_pct"] <= -8]
    rets = simulate_batch(sub, max_hold=3, stop_pct=None, pullback_pct=1.5)
    sub2 = sub.copy()
    sub2["_ret"] = rets
    sub2["_month"] = sub2["trade_date"].str[:7]
    rows = []
    for month, grp in sub2.groupby("_month"):
        valid = grp["_ret"].dropna()
        n = len(valid)
        wins = valid[valid > 0]
        losses = valid[valid < 0]
        pf = wins.sum() / abs(losses.sum()) if len(losses) > 0 and losses.sum() < 0 else (99.0 if wins.sum() > 0 else 1.0)
        rows.append({
            "month": month,
            "split": "train" if month <= "2024-12" else "test",
            "trade_count": n,
            "win_rate": float(len(wins) / n * 100) if n > 0 else None,
            "avg_ret": float(valid.mean()) if n > 0 else None,
            "pf": float(min(pf, 99.0)) if n > 0 else None,
            "max_loss": float(valid.min()) if n > 0 else None,
        })
    pd.DataFrame(rows).to_csv(out_dir / "19_h5_monthly.csv", index=False)
    logger.info("  → 19_h5_monthly.csv (%d months)", len(rows))


def build_final_summary(out_dir: Path) -> None:
    logger.info("18: Building final summary...")
    summary = []

    for fname, key_col in [
        ("01_ai_score_threshold.csv",   "ai_threshold"),
        ("02_rule_score_threshold.csv",  "rule_threshold"),
        ("08_stop_loss_model.csv",       "stop_pct"),
        ("09_pullback_exit_width.csv",   "pullback_pct"),
        ("10_holding_days.csv",          "max_hold"),
        ("11_regime_filter.csv",         "regime_filter"),
    ]:
        try:
            tmp = pd.read_csv(out_dir / fname)
            best = tmp[tmp["split"] == "train"].sort_values("avg_ret", ascending=False).head(1)
            if len(best):
                row = best.iloc[0].to_dict()
                row["source_file"] = fname
                summary.append(row)
        except Exception as e:
            logger.warning("  skip %s: %s", fname, e)

    pd.DataFrame(summary).to_csv(out_dir / "18_final_summary.csv", index=False)
    logger.info("  → 18_final_summary.csv (%d rows)", len(summary))


# ── Main ─────────────────────────────────────────────────────────────────────

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

    analyze_ai_score_threshold(df, out_dir)
    analyze_rule_score_threshold(df, out_dir)
    analyze_support_gap_ma25(df, out_dir)
    analyze_support_gap_ma75(df, out_dir)
    analyze_support_gap_20d(df, out_dir)
    analyze_overheat_rsi(df, out_dir)
    analyze_overheat_ma5(df, out_dir)
    analyze_stop_loss_models(df, out_dir)
    analyze_pullback_width(df, out_dir)
    analyze_holding_days(df, out_dir)
    analyze_regime_filter(df, out_dir)
    analyze_combined_ai_support(df, out_dir)
    analyze_combined_hold_pullback(df, out_dir)
    analyze_hypothesis_validation(df, out_dir)
    analyze_monthly_returns(df, out_dir)
    analyze_year_breakdown(df, out_dir)
    analyze_regime_return_matrix(df, out_dir)
    analyze_h5_monthly(df, out_dir)
    build_final_summary(out_dir)

    logger.info("Done. All outputs in %s", out_dir)


if __name__ == "__main__":
    main()
