#!/usr/bin/env python3
"""H5-focused grid: 108 strategy combinations around H5 hypothesis.

Fixed: signal_prob >= threshold, drop20d <= -8%, no_panic_selloff
Variable: AI threshold × pullback × hold × emergency stop × overheat mode

Usage:
    python scripts/analyze_h5_focused_grid.py
    python scripts/analyze_h5_focused_grid.py --ai-thresholds 0.60,0.65 --pullbacks 1.5,1.75,2.0
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
from datetime import datetime, timezone, timedelta
from itertools import product
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = ROOT / "outputs" / "rebound_grid_search" / "cands_2020_2026.pkl"
OUT_DIR = ROOT / "outputs" / "rebound_next_analysis" / "h5_focused"

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"

FIXED_DROP20D = -8.0
FIXED_REGIME = "no_panic_selloff"

# Default grid
DEFAULT_AI = [0.60, 0.65]
DEFAULT_PB  = [1.5, 1.75, 2.0]
DEFAULT_HD  = [3, 4, 5]
DEFAULT_STOP = [None, -8.0, -10.0]   # None=no stop, -8/-10=emergency
DEFAULT_OH  = ["cool_mild_only", "include_hot"]


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_df(cache_path: Path) -> pd.DataFrame:
    logger.info("Loading candidates from %s", cache_path)
    with open(cache_path, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    df["trade_date"] = df["trade_date"].astype(str)
    logger.info("Loaded %d candidates", len(df))
    return df


# ── Overheat classification ───────────────────────────────────────────────────

def _classify_overheat(df: pd.DataFrame) -> pd.DataFrame:
    """Add overheat_score and overheat_bucket columns."""
    df = df.copy()
    sc = (
        (df["rsi14"].fillna(0) >= 65).astype(int) +
        (df["ma5_gap_pct"].fillna(0) >= 5).astype(int) +
        (df["return_5d_pct"].fillna(0) >= 8).astype(int) +
        (df["volume_ratio_20d"].fillna(0) >= 3.0).astype(int)
    )
    df["overheat_score"] = sc

    # extreme: 3+ signals
    # hot: 2 signals
    # mild: 1 signal
    # cool: 0 signals
    def _bucket(s):
        if s >= 3:
            return "extreme"
        if s == 2:
            return "hot"
        if s == 1:
            return "mild"
        return "cool"

    df["overheat_bucket"] = sc.map(_bucket)
    return df


def _apply_overheat_filter(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "cool_mild_only":
        return df[df["overheat_bucket"].isin(["cool", "mild"])]
    # include_hot: allow cool/mild/hot, exclude only extreme
    return df[df["overheat_bucket"] != "extreme"]


# ── Trade simulation ──────────────────────────────────────────────────────────

def _simulate_one(row: dict, max_hold: int,
                  emergency_stop_pct: Optional[float],
                  pullback_pct: float) -> tuple[float | None, int, str]:
    """Simulate one trade. Priority: emergency_stop > pullback > time_stop."""
    entry = row.get("entry_price")
    if not entry or entry <= 0:
        return None, 0, "no_entry"

    em_price = entry * (1 + emergency_stop_pct / 100) if emergency_stop_pct is not None else None
    peak = float(entry)

    for day in range(1, max_hold + 1):
        high = row.get(f"future_high_{day}d")
        low  = row.get(f"future_low_{day}d")
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

        # 1. Emergency stop
        if em_price is not None and low <= em_price:
            return emergency_stop_pct, day, "emergency_stop"

        # 2. Pullback exit (only after gaining 0.5%)
        if peak > entry * 1.005 and close <= peak * (1 - pullback_pct / 100):
            return (close - entry) / entry * 100, day, "pullback"

    close_f = row.get(f"future_close_{max_hold}d")
    if close_f is None:
        return None, max_hold, "no_data"
    try:
        close_f = float(close_f)
    except (TypeError, ValueError):
        return None, max_hold, "no_data"
    if np.isnan(close_f):
        return None, max_hold, "no_data"
    return (close_f - entry) / entry * 100, max_hold, "time_stop"


def _precompute(df: pd.DataFrame,
                holds: list, pullbacks: list, stops: list) -> dict:
    """Pre-simulate all (hold, pullback, stop) combos for every row.

    Returns dict: (hold, pb, stop) → list of (ret, exit_type, exit_day) per row
    """
    logger.info("Pre-computing simulations (%d combos × %d candidates)...",
                len(holds) * len(pullbacks) * len(stops), len(df))
    records = df.to_dict("records")
    cache: dict = {}
    combos = list(product(holds, pullbacks, stops))
    for hold, pb, stop in combos:
        rets, types, days = [], [], []
        for row in records:
            r, d, t = _simulate_one(row, hold, stop, pb)
            rets.append(r)
            types.append(t)
            days.append(d)
        cache[(hold, pb, stop)] = (np.array(rets, dtype=object),
                                   np.array(types, dtype=object),
                                   np.array(days, dtype=object))
    logger.info("Pre-computation done.")
    return cache


# ── Metrics ───────────────────────────────────────────────────────────────────

def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _compute_metrics(rets, types, days, dates: pd.Series) -> dict:
    """Compute all metrics from arrays of returns, exit types, days, and dates."""
    mask = np.array([_safe_float(r) is not None for r in rets])
    valid_rets = np.array([_safe_float(r) for r in rets[mask]], dtype=float)
    valid_types = types[mask]
    valid_days = np.array([d for d in days[mask] if d is not None], dtype=float)
    valid_dates = dates.values[mask]

    n = len(valid_rets)
    if n == 0:
        return {k: None for k in [
            "trade_count", "win_rate", "avg_ret", "median_ret", "pf",
            "max_loss", "max_gain", "max_dd", "sharpe",
            "monthly_consistency", "avg_holding_days",
            "emergency_stop_rate", "pullback_exit_rate", "time_stop_rate",
            "profit_conc_month", "profit_conc_year"]}

    wins = valid_rets[valid_rets > 0]
    losses = valid_rets[valid_rets < 0]
    sum_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    pf = wins.sum() / sum_loss if sum_loss > 0 else (99.0 if wins.sum() > 0 else 1.0)

    # Max drawdown via equity curve
    sorted_idx = np.argsort(valid_dates)
    cum = np.cumsum(valid_rets[sorted_idx])
    peak_so_far = np.maximum.accumulate(cum)
    dd_series = cum - peak_so_far
    max_dd = float(dd_series.min())

    # Sharpe (annualized by holding period)
    avg_hd = float(valid_days.mean()) if len(valid_days) > 0 else 3.0
    ann_factor = np.sqrt(252.0 / max(avg_hd, 1.0))
    sharpe = float(valid_rets.mean() / valid_rets.std() * ann_factor) if valid_rets.std() > 0 else 0.0

    # Monthly consistency
    s_dates = pd.Series(valid_dates)
    months = s_dates.str[:7]
    monthly_totals = pd.Series(valid_rets).groupby(months.values).sum()
    monthly_consistency = float((monthly_totals > 0).mean() * 100) if len(monthly_totals) > 0 else 0.0

    # Profit concentration
    monthly_abs = monthly_totals.abs()
    profit_conc_month = float(monthly_abs.max() / monthly_abs.sum() * 100) if monthly_abs.sum() > 0 else 0.0
    yearly = s_dates.str[:4]
    yearly_totals = pd.Series(valid_rets).groupby(yearly.values).sum()
    yearly_abs = yearly_totals.abs()
    profit_conc_year = float(yearly_abs.max() / yearly_abs.sum() * 100) if yearly_abs.sum() > 0 else 0.0

    # Exit type rates
    em_rate = float((valid_types == "emergency_stop").mean() * 100)
    pb_rate = float((valid_types == "pullback").mean() * 100)
    ts_rate = float((valid_types == "time_stop").mean() * 100)

    return {
        "trade_count": int(n),
        "win_rate": float(len(wins) / n * 100),
        "avg_ret": float(valid_rets.mean()),
        "median_ret": float(np.median(valid_rets)),
        "pf": float(min(pf, 99.0)),
        "max_loss": float(valid_rets.min()),
        "max_gain": float(valid_rets.max()),
        "max_dd": max_dd,
        "sharpe": sharpe,
        "monthly_consistency": monthly_consistency,
        "avg_holding_days": float(valid_days.mean()) if len(valid_days) > 0 else None,
        "emergency_stop_rate": em_rate,
        "pullback_exit_rate": pb_rate,
        "time_stop_rate": ts_rate,
        "profit_conc_month": profit_conc_month,
        "profit_conc_year": profit_conc_year,
    }


def _compute_deploy_score(test: dict, train: dict) -> tuple[float, list[str]]:
    """Compute deploy_score and warning flags."""
    if test.get("trade_count") is None or test["trade_count"] == 0:
        return -999.0, ["NO_TEST_TRADES"]

    score = (
        (test["avg_ret"] or 0) * 100 +
        min(test["pf"] or 1, 20) * 10 +
        (test["win_rate"] or 0) * 20 +
        (test["monthly_consistency"] or 0) * 20 -
        abs(test["max_dd"] or 0) * 2 -
        abs(test["max_loss"] or 0) * 1.5
    )

    warnings = []

    # Train/test gap penalty
    gap_penalty = 0.0
    if train and train.get("avg_ret") and test.get("avg_ret"):
        if test["avg_ret"] > (train["avg_ret"] or 0) * 5 and (train["avg_ret"] or 0) > 0:
            gap_penalty += 30
            warnings.append("LARGE_TEST_ADVANTAGE")
        if test["avg_ret"] < 0 and (train["avg_ret"] or 0) > 0:
            gap_penalty += 20
            warnings.append("TEST_NEGATIVE")
    score -= gap_penalty

    # Low trade penalty
    tc = test["trade_count"]
    if tc < 50:
        score -= 50
        warnings.append("LOW_TEST_TRADES<50")
    elif tc < 100:
        score -= 20
        warnings.append("LOW_TEST_TRADES<100")

    # Extreme PF penalty
    pf = test["pf"] or 0
    if pf > 20:
        score -= 20
        warnings.append("EXTREME_PF>20")
    elif pf > 10:
        score -= 5
        warnings.append("HIGH_PF>10")

    # Profit concentration penalty
    if (test["profit_conc_month"] or 0) > 40:
        score -= 15
        warnings.append("MONTH_CONC>40%")
    if (test["profit_conc_year"] or 0) > 50:
        score -= 10
        warnings.append("YEAR_CONC>50%")

    return score, warnings


def _make_strategy_id(ai: float, pb: float, hold: int,
                       stop: Optional[float], overheat: str) -> str:
    ai_s = f"AI{int(ai * 100)}"
    pb_s = f"PB{str(pb).replace('.', '')}"
    hd_s = f"HD{hold}"
    stop_s = "NOSTOP" if stop is None else f"EST{int(abs(stop))}"
    oh_s = "CM" if overheat == "cool_mild_only" else "HOT"
    return f"H5_{ai_s}_{pb_s}_{hd_s}_{stop_s}_{oh_s}"


# ── Grid run ──────────────────────────────────────────────────────────────────

def _run_grid(df: pd.DataFrame, sim_cache: dict,
              ai_list, pb_list, hd_list, stop_list, oh_list) -> pd.DataFrame:
    # Fixed H5 base filter
    base = df[
        (df["drop_from_20d_high_pct"] <= FIXED_DROP20D) &
        (df["market_regime"] != "panic_selloff")
    ].copy()

    rows = []
    combos = list(product(ai_list, pb_list, hd_list, stop_list, oh_list))
    logger.info("Running %d strategy combinations on %d base candidates...",
                len(combos), len(base))

    for ai, pb, hold, stop, oh in combos:
        sid = _make_strategy_id(ai, pb, hold, stop, oh)

        # Apply variable filters on base
        sub = base[base["signal_probability"] >= ai]
        sub = _apply_overheat_filter(sub, oh)
        idx = sub.index

        # Retrieve pre-computed simulation results
        rets, types, days = sim_cache[(hold, pb, stop)]
        # Map global indices to cache positions (cache is indexed as df row order)
        # We need position in original df, not sub
        pos = df.index.get_indexer(idx)
        valid_pos = pos[pos >= 0]

        rets_s = rets[valid_pos]
        types_s = types[valid_pos]
        days_s  = days[valid_pos]
        dates_s = sub.loc[sub.index[pos >= 0], "trade_date"].reset_index(drop=True)

        # Split train / test
        train_mask = dates_s.values <= TRAIN_END
        test_mask  = dates_s.values >= TEST_START

        train_m = _compute_metrics(rets_s[train_mask], types_s[train_mask],
                                   days_s[train_mask], dates_s[train_mask])
        test_m  = _compute_metrics(rets_s[test_mask],  types_s[test_mask],
                                   days_s[test_mask],  dates_s[test_mask])

        deploy_score, warn_flags = _compute_deploy_score(test_m, train_m)

        row: dict = {"strategy_id": sid,
                     "ai_threshold": ai,
                     "pullback_pct": pb,
                     "max_holding_days": hold,
                     "stop_model": "none" if stop is None else f"emergency{int(abs(stop))}",
                     "emergency_stop_pct": stop,
                     "overheat_mode": oh,
                     "score_deploy": round(deploy_score, 1),
                     "warning_flags": "|".join(warn_flags)}

        for split, m in [("train", train_m), ("test", test_m)]:
            for k, v in m.items():
                row[f"{split}_{k}"] = round(v, 4) if isinstance(v, float) else v

        rows.append(row)

    return pd.DataFrame(rows)


# ── Output helpers ────────────────────────────────────────────────────────────

def _equity_curve(df_base: pd.DataFrame, sim_cache: dict,
                  ai: float, pb: float, hold: int,
                  stop: Optional[float], oh: str) -> tuple[np.ndarray, np.ndarray]:
    """Return sorted dates and cumulative returns for one strategy."""
    base = df_base[
        (df_base["drop_from_20d_high_pct"] <= FIXED_DROP20D) &
        (df_base["market_regime"] != "panic_selloff") &
        (df_base["signal_probability"] >= ai)
    ]
    base = _apply_overheat_filter(base, oh)
    pos = df_base.index.get_indexer(base.index)
    valid_pos = pos[pos >= 0]
    rets, _, _ = sim_cache[(hold, pb, stop)]
    r = np.array([_safe_float(x) for x in rets[valid_pos]], dtype=float)
    dates = base.loc[base.index[pos >= 0], "trade_date"].values
    valid = ~np.isnan(r)
    r, dates = r[valid], dates[valid]
    sort_idx = np.argsort(dates)
    return dates[sort_idx], np.cumsum(r[sort_idx])


def _plot_equity_curves(df_base: pd.DataFrame, sim_cache: dict,
                        top_rows: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 7))
    colors = plt.cm.tab10(np.linspace(0, 1, min(len(top_rows), 10)))
    for i, (_, row) in enumerate(top_rows.iterrows()):
        dates, cum = _equity_curve(
            df_base, sim_cache,
            row["ai_threshold"], row["pullback_pct"],
            int(row["max_holding_days"]), _stop_key(row["emergency_stop_pct"]),
            row["overheat_mode"])
        if len(dates) == 0:
            continue
        train_mask = dates <= TRAIN_END
        test_mask  = dates >= TEST_START
        ax.plot(range(train_mask.sum()),
                cum[train_mask], color=colors[i], linewidth=1.5,
                label=row["strategy_id"], alpha=0.85)
        offset = train_mask.sum()
        ax.plot(range(offset, offset + test_mask.sum()),
                cum[test_mask], color=colors[i], linewidth=2.5,
                linestyle="--", alpha=0.9)

    # Vertical line for train/test split
    train_counts = []
    for _, row in top_rows.iterrows():
        dates, _ = _equity_curve(
            df_base, sim_cache,
            row["ai_threshold"], row["pullback_pct"],
            int(row["max_holding_days"]), _stop_key(row["emergency_stop_pct"]),
            row["overheat_mode"])
        train_counts.append((dates <= TRAIN_END).sum())
    if train_counts:
        avg_split = int(np.mean(train_counts))
        ax.axvline(x=avg_split, color="gray", linestyle=":", linewidth=1, label="train|test")

    ax.set_title("H5 Focused Grid - Top 10 Equity Curves (solid=train, dashed=test)")
    ax.set_xlabel("Trade number")
    ax.set_ylabel("Cumulative return (%)")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120)
    plt.close()
    logger.info("  → %s", out_path.name)


def _stop_key(v) -> Optional[float]:
    """Convert DataFrame NaN back to None for sim_cache lookup."""
    if v is None:
        return None
    try:
        return None if np.isnan(float(v)) else float(v)
    except (TypeError, ValueError):
        return None


def _build_monthly(df_base: pd.DataFrame, sim_cache: dict,
                   top_rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in top_rows.iterrows():
        base = df_base[
            (df_base["drop_from_20d_high_pct"] <= FIXED_DROP20D) &
            (df_base["market_regime"] != "panic_selloff") &
            (df_base["signal_probability"] >= row["ai_threshold"])
        ]
        base = _apply_overheat_filter(base, row["overheat_mode"])
        pos = df_base.index.get_indexer(base.index)
        valid_pos = pos[pos >= 0]
        rets, _, _ = sim_cache[(int(row["max_holding_days"]), float(row["pullback_pct"]),
                                _stop_key(row["emergency_stop_pct"]))]
        r = rets[valid_pos]
        dates = base.loc[base.index[pos >= 0], "trade_date"].values
        regimes = base.loc[base.index[pos >= 0], "market_regime"].values

        tmp = pd.DataFrame({"ret": [_safe_float(x) for x in r],
                            "date": dates, "regime": regimes}).dropna(subset=["ret"])
        tmp["month"] = tmp["date"].str[:7]
        tmp["split"] = np.where(tmp["date"] <= TRAIN_END, "train", "test")

        for month, grp in tmp.groupby("month"):
            wins = grp["ret"][grp["ret"] > 0]
            losses = grp["ret"][grp["ret"] < 0]
            sl = abs(losses.sum()) if len(losses) > 0 else 0
            pf = wins.sum() / sl if sl > 0 else (99.0 if wins.sum() > 0 else 1.0)
            regime_mode = grp["regime"].mode().iloc[0] if len(grp) > 0 else ""
            records.append({
                "strategy_id": row["strategy_id"],
                "month": month,
                "split": grp["split"].iloc[0],
                "trade_count": len(grp),
                "win_rate": round(len(wins) / len(grp) * 100, 1),
                "avg_ret": round(grp["ret"].mean(), 2),
                "total_ret": round(grp["ret"].sum(), 2),
                "pf": round(min(pf, 99.0), 2),
                "max_loss": round(grp["ret"].min(), 2),
                "max_gain": round(grp["ret"].max(), 2),
                "market_regime_majority": regime_mode,
            })
    return pd.DataFrame(records)


def _build_yearly(df_base: pd.DataFrame, sim_cache: dict,
                  top_rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in top_rows.iterrows():
        base = df_base[
            (df_base["drop_from_20d_high_pct"] <= FIXED_DROP20D) &
            (df_base["market_regime"] != "panic_selloff") &
            (df_base["signal_probability"] >= row["ai_threshold"])
        ]
        base = _apply_overheat_filter(base, row["overheat_mode"])
        pos = df_base.index.get_indexer(base.index)
        valid_pos = pos[pos >= 0]
        rets, _, _ = sim_cache[(int(row["max_holding_days"]), float(row["pullback_pct"]),
                                _stop_key(row["emergency_stop_pct"]))]
        r = rets[valid_pos]
        dates = base.loc[base.index[pos >= 0], "trade_date"].values

        tmp = pd.DataFrame({"ret": [_safe_float(x) for x in r], "date": dates}).dropna()
        tmp["year"] = tmp["date"].str[:4]
        tmp["split"] = np.where(tmp["date"] <= TRAIN_END, "train", "test")

        for year, grp in tmp.groupby("year"):
            wins = grp["ret"][grp["ret"] > 0]
            losses = grp["ret"][grp["ret"] < 0]
            sl = abs(losses.sum()) if len(losses) > 0 else 0
            pf = wins.sum() / sl if sl > 0 else (99.0 if wins.sum() > 0 else 1.0)
            records.append({
                "strategy_id": row["strategy_id"],
                "year": int(year),
                "split": grp["split"].iloc[0],
                "trade_count": len(grp),
                "win_rate": round(len(wins) / len(grp) * 100, 1),
                "avg_ret": round(grp["ret"].mean(), 2),
                "total_ret": round(grp["ret"].sum(), 2),
                "pf": round(min(pf, 99.0), 2),
            })
    return pd.DataFrame(records)


def _build_risk(grid: pd.DataFrame) -> pd.DataFrame:
    cols = ["strategy_id", "ai_threshold", "pullback_pct", "max_holding_days",
            "stop_model", "overheat_mode",
            "train_max_loss", "test_max_loss",
            "train_max_dd", "test_max_dd",
            "train_emergency_stop_rate", "test_emergency_stop_rate",
            "train_avg_ret", "test_avg_ret", "score_deploy", "warning_flags"]
    risk = grid[[c for c in cols if c in grid.columns]].copy()
    risk["risk_score"] = (
        abs(risk.get("test_max_loss", pd.Series(0)).fillna(0)) * 1.5 +
        abs(risk.get("test_max_dd", pd.Series(0)).fillna(0)) * 2 +
        risk.get("test_emergency_stop_rate", pd.Series(0)).fillna(0) * 0.5
    )
    return risk.sort_values("risk_score")


def _build_trade_samples(df_base: pd.DataFrame, sim_cache: dict,
                         best_row: pd.Series, n: int = 20) -> pd.DataFrame:
    """Top N best and worst trades for the best strategy."""
    base = df_base[
        (df_base["drop_from_20d_high_pct"] <= FIXED_DROP20D) &
        (df_base["market_regime"] != "panic_selloff") &
        (df_base["signal_probability"] >= best_row["ai_threshold"])
    ]
    base = _apply_overheat_filter(base, best_row["overheat_mode"])
    pos = df_base.index.get_indexer(base.index)
    valid_pos = pos[pos >= 0]
    rets, types, days_arr = sim_cache[(int(best_row["max_holding_days"]),
                                       float(best_row["pullback_pct"]),
                                       _stop_key(best_row["emergency_stop_pct"]))]
    r = np.array([_safe_float(x) for x in rets[valid_pos]], dtype=float)
    actual_base = base.loc[base.index[pos >= 0]].copy().reset_index(drop=True)
    actual_base["ret"] = r
    actual_base["exit_type"] = types[valid_pos]
    actual_base["exit_day"] = days_arr[valid_pos]
    actual_base = actual_base.dropna(subset=["ret"])

    top = actual_base.nlargest(n // 2, "ret")[
        ["trade_date", "code", "market_regime", "signal_probability",
         "entry_price", "ret", "exit_type", "exit_day"]]
    top["sample_type"] = "best"
    bot = actual_base.nsmallest(n // 2, "ret")[
        ["trade_date", "code", "market_regime", "signal_probability",
         "entry_price", "ret", "exit_type", "exit_day"]]
    bot["sample_type"] = "worst"
    return pd.concat([top, bot]).reset_index(drop=True)


# ── Report generation ─────────────────────────────────────────────────────────

def _generate_report(grid: pd.DataFrame, top20: pd.DataFrame,
                     config: dict, out_path: Path) -> None:
    lines = []
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M JST")

    lines += [
        "=" * 70,
        "H5 FOCUSED GRID REPORT",
        f"Generated: {now}",
        "=" * 70,
        "",
        "[Run Summary]",
        f"  Fixed conditions : signal_prob >= threshold, drop20d <= -8%, no_panic_selloff",
        f"  Period           : {config['start']} to {config['end']}",
        f"  Train / Test     : ~ {TRAIN_END} / {TEST_START} ~",
        f"  Total combos     : {len(grid)}",
        f"  AI thresholds    : {config['ai_thresholds']}",
        f"  Pullbacks        : {config['pullbacks']}",
        f"  Holding days     : {config['holding_days']}",
        f"  Stop models      : {config['stop_models']}",
        f"  Overheat modes   : {config['overheat_modes']}",
        "",
    ]

    # Top 20
    lines.append("[Top 20 by deploy_score]")
    lines.append(f"  {'strategy_id':<40} {'tc':>5} {'wr':>6} {'avgR':>6} {'PF':>5} {'mxL':>6} {'mxDD':>7} {'score':>7}")
    lines.append("  " + "-" * 80)
    for _, r in top20.iterrows():
        tc   = r.get("test_trade_count") or 0
        wr   = r.get("test_win_rate") or 0
        avg  = r.get("test_avg_ret") or 0
        pf   = r.get("test_pf") or 0
        ml   = r.get("test_max_loss") or 0
        mdd  = r.get("test_max_dd") or 0
        sc   = r.get("score_deploy") or 0
        warn = r.get("warning_flags") or ""
        flag = " ⚠" if warn else ""
        lines.append(f"  {r['strategy_id']:<40} {tc:>5} {wr:>5.1f}% {avg:>+5.2f}% {pf:>5.2f} {ml:>+5.2f}% {mdd:>+6.2f}% {sc:>7.1f}{flag}")
    lines.append("")

    # AI threshold conclusion
    def _axis_summary(axis_col: str, axis_name: str) -> list[str]:
        out = [f"[{axis_name} breakdown (test, top-score per value)]"]
        grp = grid.groupby(axis_col).apply(
            lambda g: g.nlargest(1, "score_deploy").iloc[0]
        ).reset_index(drop=True)
        for _, r in grp.iterrows():
            out.append(f"  {axis_col}={r[axis_col]:>6}: "
                       f"tc={r.get('test_trade_count',0):>4}  "
                       f"avg={r.get('test_avg_ret',0):>+5.2f}%  "
                       f"pf={r.get('test_pf',0):>5.2f}  "
                       f"wr={r.get('test_win_rate',0):>5.1f}%  "
                       f"score={r.get('score_deploy',0):>7.1f}")
        out.append("")
        return out

    for col, name in [("ai_threshold", "AI threshold"),
                      ("pullback_pct", "Pullback width"),
                      ("max_holding_days", "Holding days"),
                      ("stop_model", "Stop model"),
                      ("overheat_mode", "Overheat mode")]:
        lines += _axis_summary(col, name)

    # Final candidates
    lines += ["[Forward-test candidates]",
              "  Primary   : " + (top20.iloc[0]["strategy_id"] if len(top20) > 0 else "-"),
              "  Secondary : " + (top20.iloc[1]["strategy_id"] if len(top20) > 1 else "-"),
              "  Aggressive: " + (top20.iloc[2]["strategy_id"] if len(top20) > 2 else "-"),
              "",
              "=" * 70]

    out_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("  → %s", out_path.name)


# ── Main ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--cache-path", default=str(DEFAULT_CACHE))
    p.add_argument("--output-dir", default=str(OUT_DIR))
    p.add_argument("--start", default="2020-01-01")
    p.add_argument("--end", default="2026-05-26")
    p.add_argument("--train-end", default=TRAIN_END)
    p.add_argument("--ai-thresholds", default=",".join(str(x) for x in DEFAULT_AI))
    p.add_argument("--pullbacks", default=",".join(str(x) for x in DEFAULT_PB))
    p.add_argument("--holding-days", default=",".join(str(x) for x in DEFAULT_HD))
    p.add_argument("--stop-models", default="none,emergency8,emergency10")
    p.add_argument("--overheat-modes", default=",".join(DEFAULT_OH))
    return p.parse_args()


def _parse_stops(s: str) -> list:
    result = []
    for tok in s.split(","):
        tok = tok.strip()
        if tok == "none":
            result.append(None)
        elif tok.startswith("emergency"):
            result.append(-float(tok.replace("emergency", "")))
        else:
            result.append(float(tok))
    return result


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ai_list   = [float(x) for x in args.ai_thresholds.split(",")]
    pb_list   = [float(x) for x in args.pullbacks.split(",")]
    hd_list   = [int(x)   for x in args.holding_days.split(",")]
    stop_list = _parse_stops(args.stop_models)
    oh_list   = args.overheat_modes.split(",")

    config = {
        "start": args.start, "end": args.end, "train_end": args.train_end,
        "ai_thresholds": ai_list, "pullbacks": pb_list,
        "holding_days": hd_list, "stop_models": [str(s) for s in stop_list],
        "overheat_modes": oh_list,
    }
    (out_dir / "h5_focused_config.json").write_text(
        json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")

    df = _load_df(Path(args.cache_path))
    df = _classify_overheat(df)
    df = df[(df["trade_date"] >= args.start) & (df["trade_date"] <= args.end)].copy()
    df = df.reset_index(drop=True)
    logger.info("After date filter: %d candidates", len(df))

    # Pre-compute simulations
    sim_cache = _precompute(df, hd_list, pb_list, stop_list)

    # Run grid
    grid = _run_grid(df, sim_cache, ai_list, pb_list, hd_list, stop_list, oh_list)
    grid.to_csv(out_dir / "h5_focused_grid.csv", index=False)
    logger.info("→ h5_focused_grid.csv (%d rows)", len(grid))

    # Top 20 by deploy_score
    top20 = grid.nlargest(20, "score_deploy")
    top20.to_csv(out_dir / "h5_focused_top20.csv", index=False)
    logger.info("→ h5_focused_top20.csv")

    # Axis summaries
    for col, fname in [
        ("ai_threshold",    "h5_focused_by_ai_threshold.csv"),
        ("pullback_pct",    "h5_focused_by_pullback.csv"),
        ("max_holding_days","h5_focused_by_hold.csv"),
        ("stop_model",      "h5_focused_by_stop.csv"),
        ("overheat_mode",   "h5_focused_by_overheat.csv"),
    ]:
        agg = grid.groupby(col).agg(
            best_deploy=("score_deploy", "max"),
            avg_deploy=("score_deploy", "mean"),
            avg_test_trade_count=("test_trade_count", "mean"),
            avg_test_avg_ret=("test_avg_ret", "mean"),
            avg_test_pf=("test_pf", "mean"),
            avg_test_win_rate=("test_win_rate", "mean"),
            avg_test_max_loss=("test_max_loss", "mean"),
            avg_test_max_dd=("test_max_dd", "mean"),
            avg_train_avg_ret=("train_avg_ret", "mean"),
            avg_train_pf=("train_pf", "mean"),
        ).round(3).reset_index()
        agg.to_csv(out_dir / fname, index=False)
        logger.info("→ %s", fname)

    # Monthly / yearly (top 10)
    top10 = grid.nlargest(10, "score_deploy")
    monthly = _build_monthly(df, sim_cache, top10)
    monthly.to_csv(out_dir / "h5_focused_monthly.csv", index=False)
    logger.info("→ h5_focused_monthly.csv (%d rows)", len(monthly))

    yearly = _build_yearly(df, sim_cache, top10)
    yearly.to_csv(out_dir / "h5_focused_yearly.csv", index=False)
    logger.info("→ h5_focused_yearly.csv (%d rows)", len(yearly))

    # Risk
    risk = _build_risk(grid)
    risk.to_csv(out_dir / "h5_focused_risk.csv", index=False)
    logger.info("→ h5_focused_risk.csv")

    # Trade samples (best strategy)
    best = top20.iloc[0]
    samples = _build_trade_samples(df, sim_cache, best)
    samples.to_csv(out_dir / "h5_focused_trade_samples.csv", index=False)
    logger.info("→ h5_focused_trade_samples.csv")

    # Equity curves (top 10)
    _plot_equity_curves(df, sim_cache, top10, out_dir / "h5_focused_equity_top10.png")

    # Report
    _generate_report(grid, top20, config, out_dir / "h5_focused_report.txt")

    logger.info("All done. Output: %s", out_dir)


if __name__ == "__main__":
    main()
