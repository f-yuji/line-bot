#!/usr/bin/env python3
"""Analyze WHY max_holding_days=3 is optimal for H5.

Breaks down exit reasons, tracks what happens after HD3 time_stop,
computes per-trade HD3 vs HD4/HD5 diffs, and tests conditional extension.

Usage:
    python scripts/analyze_h5_holding_days_reason.py
    python scripts/analyze_h5_holding_days_reason.py --holding-days 3,4,5,7
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import pickle
from datetime import datetime, timezone, timedelta
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
OUT_DIR = ROOT / "outputs" / "rebound_next_analysis" / "h5_holding_reason"

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"

# Fixed H5 best conditions
AI_THRESHOLD  = 0.65
DROP_20D_MAX  = -8.0
EM_STOP_PCT   = -8.0
PULLBACK_PCT  = 2.0
OVERHEAT_MODE = "cool_mild_only"
DEFAULT_HOLDS = [3, 4, 5, 7]


# ── Load & filter ─────────────────────────────────────────────────────────────

def _load_df(cache_path: Path) -> pd.DataFrame:
    logger.info("Loading candidates from %s", cache_path)
    with open(cache_path, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    df["trade_date"] = df["trade_date"].astype(str)
    return df


def _classify_overheat(df: pd.DataFrame) -> pd.DataFrame:
    sc = (
        (df["rsi14"].fillna(0) >= 65).astype(int) +
        (df["ma5_gap_pct"].fillna(0) >= 5).astype(int) +
        (df["return_5d_pct"].fillna(0) >= 8).astype(int) +
        (df["volume_ratio_20d"].fillna(0) >= 3.0).astype(int)
    )
    df = df.copy()
    df["overheat_score"] = sc
    df["overheat_bucket"] = sc.map({0: "cool", 1: "mild", 2: "hot", 3: "extreme"})
    df["overheat_bucket"] = df["overheat_bucket"].fillna("extreme")
    return df


def _apply_h5_filters(df: pd.DataFrame, ai: float, drop20d: float,
                       overheat_mode: str) -> pd.DataFrame:
    df = df[
        (df["signal_probability"] >= ai) &
        (df["drop_from_20d_high_pct"] <= drop20d) &
        (df["market_regime"] != "panic_selloff")
    ]
    if overheat_mode == "cool_mild_only":
        df = df[df["overheat_bucket"].isin(["cool", "mild"])]
    else:
        df = df[df["overheat_bucket"] != "extreme"]
    return df.reset_index(drop=True)


# ── Simulation ────────────────────────────────────────────────────────────────

def _get_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def simulate_all_holds(row: dict, holds: list[int],
                        em_stop_pct: Optional[float],
                        pb_pct: float) -> dict[int, tuple]:
    """Simulate for all holds in one pass. Returns {hold: (ret, exit_type, exit_day)}."""
    entry = _get_float(row.get("entry_price"))
    if entry is None or entry <= 0:
        return {h: (None, "no_entry", 0) for h in holds}

    em_price = entry * (1 + em_stop_pct / 100) if em_stop_pct is not None else None
    peak = entry
    max_h = max(holds)

    # Find the first triggering event (emergency or pullback)
    trigger_day: Optional[int] = None
    trigger_ret: Optional[float] = None
    trigger_type: str = ""

    for day in range(1, max_h + 1):
        high  = _get_float(row.get(f"future_high_{day}d"))
        low   = _get_float(row.get(f"future_low_{day}d"))
        close = _get_float(row.get(f"future_close_{day}d"))

        if high is None or low is None or close is None:
            # Mark all remaining holds as no_data
            result = {}
            for h in holds:
                if h < day:
                    pass  # already handled below
                else:
                    result[h] = (None, "no_data", day)
            # Fill holds that already would have time_stopped
            for h in holds:
                if h < day:
                    c = _get_float(row.get(f"future_close_{h}d"))
                    result[h] = (
                        ((c - entry) / entry * 100, "time_stop", h)
                        if c is not None else (None, "no_data", h)
                    )
            return result

        peak = max(peak, high)

        # Priority 1: emergency stop
        if em_price is not None and low <= em_price:
            trigger_day = day
            trigger_ret = em_stop_pct
            trigger_type = "emergency_stop"
            break

        # Priority 2: pullback exit
        if peak > entry * 1.005 and close <= peak * (1 - pb_pct / 100):
            trigger_day = day
            trigger_ret = (close - entry) / entry * 100
            trigger_type = "pullback_exit"
            break

    results: dict[int, tuple] = {}
    for h in holds:
        if trigger_day is not None and trigger_day <= h:
            results[h] = (trigger_ret, trigger_type, trigger_day)
        else:
            c = _get_float(row.get(f"future_close_{h}d"))
            results[h] = (
                ((c - entry) / entry * 100, "time_stop", h)
                if c is not None else (None, "no_data", h)
            )
    return results


def build_trade_df(df: pd.DataFrame, holds: list[int],
                   em_stop_pct: float, pb_pct: float) -> pd.DataFrame:
    """Build per-trade DataFrame with results for every hold."""
    records = df.to_dict("records")
    rows = []
    for row in records:
        sim = simulate_all_holds(row, holds, em_stop_pct, pb_pct)
        r = {
            "trade_date":       row.get("trade_date"),
            "code":             row.get("code"),
            "name":             row.get("name", ""),
            "market_regime":    row.get("market_regime"),
            "signal_probability": row.get("signal_probability"),
            "drop_from_20d_high_pct": row.get("drop_from_20d_high_pct"),
            "overheat_bucket":  row.get("overheat_bucket"),
            "rsi14":            row.get("rsi14"),
            "ma5_gap_pct":      row.get("ma5_gap_pct"),
            "volume_ratio_20d": row.get("volume_ratio_20d"),
            "entry_price":      row.get("entry_price"),
            "future_close_3d":  _get_float(row.get("future_close_3d")),
        }
        for h in holds:
            ret, etype, eday = sim[h]
            r[f"ret_{h}"]       = ret
            r[f"exit_type_{h}"] = etype
            r[f"exit_day_{h}"]  = eday
        rows.append(r)
    return pd.DataFrame(rows)


# ── Metrics helpers ───────────────────────────────────────────────────────────

def _metrics(rets: pd.Series) -> dict:
    valid = rets.dropna().values.astype(float)
    n = len(valid)
    if n == 0:
        return {k: None for k in ["n", "win_rate", "avg_ret", "median_ret",
                                   "pf", "max_loss", "max_gain", "max_dd", "sharpe",
                                   "monthly_consistency"]}
    wins   = valid[valid > 0]
    losses = valid[valid < 0]
    sl     = abs(losses.sum()) if len(losses) > 0 else 0.0
    pf     = wins.sum() / sl if sl > 0 else (99.0 if wins.sum() > 0 else 1.0)
    cum    = np.cumsum(valid)
    peak   = np.maximum.accumulate(cum)
    max_dd = float((cum - peak).min())
    sharpe = float(valid.mean() / valid.std() * np.sqrt(252 / 3)) if valid.std() > 0 else 0.0
    return {
        "n": int(n),
        "win_rate": float(len(wins) / n * 100),
        "avg_ret": float(valid.mean()),
        "median_ret": float(np.median(valid)),
        "pf": float(min(pf, 99.0)),
        "max_loss": float(valid.min()),
        "max_gain": float(valid.max()),
        "max_dd": max_dd,
        "sharpe": sharpe,
    }


def _exit_rate(df_split: pd.DataFrame, h: int, etype: str) -> float:
    col = f"exit_type_{h}"
    if col not in df_split.columns or len(df_split) == 0:
        return 0.0
    return float((df_split[col] == etype).mean() * 100)


def _deploy_score(m: dict) -> float:
    if not m.get("n"):
        return -999.0
    return (
        (m.get("avg_ret") or 0) * 100 +
        min(m.get("pf") or 1, 20) * 10 +
        (m.get("win_rate") or 0) * 20 -
        abs(m.get("max_dd") or 0) * 2 -
        abs(m.get("max_loss") or 0) * 1.5
    )


def _split_mask(df: pd.DataFrame, period: str) -> pd.Series:
    if period == "train":
        return df["trade_date"] <= TRAIN_END
    if period == "test":
        return df["trade_date"] >= TEST_START
    return pd.Series(True, index=df.index)


def _max_consecutive(bools: np.ndarray) -> int:
    max_run = cur = 0
    for b in bools:
        cur = cur + 1 if b else 0
        max_run = max(max_run, cur)
    return max_run


# ── Analysis functions ────────────────────────────────────────────────────────

def holding_days_summary(tdf: pd.DataFrame, holds: list[int]) -> pd.DataFrame:
    rows = []
    for h in holds:
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)]
            m = _metrics(sub[f"ret_{h}"])
            if not m["n"]:
                continue
            avg_hd = sub[f"exit_day_{h}"].mean()
            em_r  = _exit_rate(sub, h, "emergency_stop")
            pb_r  = _exit_rate(sub, h, "pullback_exit")
            ts_r  = _exit_rate(sub, h, "time_stop")
            rows.append({
                "max_holding_days": h, "period": period,
                **m,
                "avg_holding_days": round(avg_hd, 2) if avg_hd else None,
                "emergency_stop_rate": round(em_r, 1),
                "pullback_exit_rate":  round(pb_r, 1),
                "time_stop_rate":      round(ts_r, 1),
                "deploy_score":        round(_deploy_score(m), 1),
            })
    return pd.DataFrame(rows)


def exit_reason_breakdown(tdf: pd.DataFrame, holds: list[int]) -> pd.DataFrame:
    rows = []
    for h in holds:
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)].dropna(subset=[f"ret_{h}"])
            for etype in ["emergency_stop", "pullback_exit", "time_stop", "no_data"]:
                grp = sub[sub[f"exit_type_{h}"] == etype]
                if len(grp) == 0:
                    continue
                m = _metrics(grp[f"ret_{h}"])
                rows.append({
                    "max_holding_days": h, "period": period, "exit_reason": etype,
                    "count": len(grp),
                    "ratio": round(len(grp) / len(sub) * 100, 1),
                    **{k: round(v, 3) if isinstance(v, float) else v
                       for k, v in m.items()},
                    "avg_exit_day": round(grp[f"exit_day_{h}"].mean(), 2),
                })
    return pd.DataFrame(rows)


def exit_day_distribution(tdf: pd.DataFrame, holds: list[int]) -> pd.DataFrame:
    rows = []
    for h in holds:
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)].dropna(subset=[f"ret_{h}"])
            total = len(sub)
            for day in range(1, h + 1):
                for etype in ["emergency_stop", "pullback_exit", "time_stop"]:
                    grp = sub[
                        (sub[f"exit_day_{h}"] == day) &
                        (sub[f"exit_type_{h}"] == etype)
                    ]
                    if len(grp) == 0:
                        continue
                    valid = grp[f"ret_{h}"].dropna()
                    wins  = valid[valid > 0]
                    rows.append({
                        "max_holding_days": h, "period": period,
                        "exit_day": day, "exit_reason": etype,
                        "count": len(grp),
                        "ratio": round(len(grp) / total * 100, 1) if total > 0 else 0,
                        "avg_ret": round(float(valid.mean()), 3) if len(valid) > 0 else None,
                        "win_rate": round(len(wins) / len(valid) * 100, 1) if len(valid) > 0 else None,
                    })
    return pd.DataFrame(rows)


def time_stop_after_analysis(tdf: pd.DataFrame) -> pd.DataFrame:
    """For HD3 time_stop trades: track what happens on day4/5/7."""
    rows = []
    for period in ["train", "test", "all"]:
        sub = tdf[_split_mask(tdf, period)]
        ts3 = sub[sub["exit_type_3"] == "time_stop"].copy()
        n = len(ts3)
        if n == 0:
            continue

        r3 = ts3["ret_3"].dropna()
        for col_h, label in [("ret_4", "day4"), ("ret_5", "day5"), ("ret_7", "day7")]:
            if col_h not in ts3.columns:
                continue
            r_h = ts3[col_h]
            diff = r_h - ts3["ret_3"]
            valid = diff.dropna()
            n_valid = len(valid)
            if n_valid == 0:
                continue
            rows.append({
                "period": period,
                "trade_count": n_valid,
                "hd3_exit_avg_ret":  round(float(r3.mean()), 3),
                "extend_to":        label,
                "extend_avg_ret":   round(float(r_h.dropna().mean()), 3),
                "better_rate":      round(float((r_h > ts3["ret_3"]).mean() * 100), 1),
                "worse_rate":       round(float((r_h < ts3["ret_3"]).mean() * 100), 1),
                "neutral_rate":     round(float((r_h == ts3["ret_3"]).mean() * 100), 1),
                "avg_diff":         round(float(valid.mean()), 3),
                "median_diff":      round(float(valid.median()), 3),
                "max_favorable":    round(float(valid.max()), 3) if len(valid) > 0 else None,
                "max_adverse":      round(float(valid.min()), 3) if len(valid) > 0 else None,
                "profit_after_loss_rate": round(
                    float(((r_h > 0) & (ts3["ret_3"] < 0)).mean() * 100), 1),
                "loss_after_profit_rate": round(
                    float(((r_h < 0) & (ts3["ret_3"] > 0)).mean() * 100), 1),
            })
    return pd.DataFrame(rows)


def trade_diff(tdf: pd.DataFrame, h_ext: int) -> pd.DataFrame:
    """Per-trade comparison of HD3 vs HD{h_ext}."""
    col_ret  = f"ret_{h_ext}"
    col_type = f"exit_type_{h_ext}"
    col_day  = f"exit_day_{h_ext}"

    out = tdf[["trade_date", "code", "name", "market_regime",
               "signal_probability", "drop_from_20d_high_pct",
               "overheat_bucket", "rsi14", "ma5_gap_pct", "entry_price",
               "ret_3", "exit_type_3", "exit_day_3",
               col_ret, col_type, col_day]].copy()
    out = out.dropna(subset=["ret_3", col_ret])
    out[f"diff_hd{h_ext}_minus_hd3"] = out[col_ret] - out["ret_3"]
    out[f"hd{h_ext}_helped"] = out[f"diff_hd{h_ext}_minus_hd3"] > 0
    out[f"hd{h_ext}_hurt"]   = out[f"diff_hd{h_ext}_minus_hd3"] < 0
    out["hd3_avoided_loss"]  = (out["ret_3"] > out[col_ret]) & (out[col_ret] < 0)
    out["hd3_missed_profit"] = (out[col_ret] > out["ret_3"]) & (out[col_ret] > 0)
    return out.round(4)


def missed_avoided_analysis(diff_df: pd.DataFrame, h_ext: int) -> pd.DataFrame:
    rows = []
    diff_col = f"diff_hd{h_ext}_minus_hd3"
    missed_mask  = diff_df["hd3_missed_profit"]
    avoided_mask = diff_df["hd3_avoided_loss"]

    for label, mask, sign in [
        ("missed_profit", missed_mask, 1),
        ("avoided_loss",  avoided_mask, -1),
    ]:
        for period, pmask in [
            ("train", diff_df["trade_date"] <= TRAIN_END),
            ("test",  diff_df["trade_date"] >= TEST_START),
            ("all",   pd.Series(True, index=diff_df.index)),
        ]:
            grp = diff_df[mask & pmask]
            base = diff_df[pmask]
            if len(grp) == 0:
                continue
            d = grp[diff_col] * sign
            rows.append({
                "analysis": label,
                "extension": f"HD{h_ext}_vs_HD3",
                "period": period,
                "count": len(grp),
                "ratio": round(len(grp) / len(base) * 100, 1) if len(base) > 0 else 0,
                "avg": round(float(d.mean()), 3),
                "median": round(float(d.median()), 3),
                "max": round(float(d.max()), 3),
                "total": round(float(d.sum()), 3),
                "as_pct_of_all_total": round(
                    float(d.sum() / abs(diff_df[pmask][diff_col].sum()) * 100), 1)
                    if diff_df[pmask][diff_col].sum() != 0 else None,
                "common_exit_hd3": grp["exit_type_3"].mode().iloc[0] if len(grp) > 0 else "",
                "common_regime":   grp["market_regime"].mode().iloc[0] if len(grp) > 0 else "",
            })
    return pd.DataFrame(rows)


def holding_extension_impact(tdf: pd.DataFrame, holds: list[int]) -> pd.DataFrame:
    rows = []
    base_h = holds[0]
    for h in holds[1:]:
        col = f"ret_{h}"
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)].dropna(subset=[f"ret_{base_h}", col])
            diff = sub[col] - sub[f"ret_{base_h}"]
            n = len(diff)
            if n == 0:
                continue
            helped = (diff > 0).sum()
            hurt   = (diff < 0).sum()
            rows.append({
                "extension": f"HD{h}_minus_HD{base_h}",
                "period": period,
                "trade_count": n,
                "helped_count": int(helped),
                "hurt_count":   int(hurt),
                "helped_rate":  round(helped / n * 100, 1),
                "hurt_rate":    round(hurt / n * 100, 1),
                "avg_diff":     round(float(diff.mean()), 3),
                "median_diff":  round(float(diff.median()), 3),
                "total_diff":   round(float(diff.sum()), 3),
                "avg_positive_diff": round(float(diff[diff > 0].mean()), 3) if helped > 0 else 0,
                "avg_negative_diff": round(float(diff[diff < 0].mean()), 3) if hurt > 0 else 0,
                "max_positive_diff": round(float(diff.max()), 3),
                "max_negative_diff": round(float(diff.min()), 3),
                "net_benefit":  round(float(diff.sum()), 3),
            })
    return pd.DataFrame(rows)


def by_axis(tdf: pd.DataFrame, holds: list[int], axis_col: str) -> pd.DataFrame:
    rows = []
    for h in holds:
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)].dropna(subset=[f"ret_{h}"])
            for val, grp in sub.groupby(axis_col):
                m = _metrics(grp[f"ret_{h}"])
                if not m["n"]:
                    continue
                rows.append({
                    "max_holding_days": h, "period": period, axis_col: val,
                    **{k: round(v, 3) if isinstance(v, float) else v for k, v in m.items()},
                    "pullback_exit_rate": round(_exit_rate(grp, h, "pullback_exit"), 1),
                    "time_stop_rate":     round(_exit_rate(grp, h, "time_stop"), 1),
                })
    return pd.DataFrame(rows)


def risk_deep_dive(tdf: pd.DataFrame, holds: list[int]) -> pd.DataFrame:
    rows = []
    for h in holds:
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)].sort_values("trade_date")
            valid = sub[f"ret_{h}"].dropna().values.astype(float)
            if len(valid) == 0:
                continue
            sub_valid = sub.dropna(subset=[f"ret_{h}"])
            bools = valid < 0
            max_consec_loss = _max_consecutive(bools)
            max_consec_win  = _max_consecutive(~bools)
            cum  = np.cumsum(valid)
            peak = np.maximum.accumulate(cum)
            dd_series = cum - peak
            max_dd = float(dd_series.min())
            # DD duration
            dd_dur = 0
            cur = 0
            for d in dd_series:
                cur = cur + 1 if d < 0 else 0
                dd_dur = max(dd_dur, cur)

            sub_valid = sub_valid.copy()
            sub_valid["_ret"] = valid
            sub_valid["_month"] = sub_valid["trade_date"].str[:7]
            sub_valid["_year"]  = sub_valid["trade_date"].str[:4]
            monthly = sub_valid.groupby("_month")["_ret"].sum()
            yearly  = sub_valid.groupby("_year")["_ret"].sum()
            worst_m  = monthly.idxmin() if len(monthly) > 0 else ""
            best_m   = monthly.idxmax() if len(monthly) > 0 else ""
            conc_m   = float(monthly.abs().max() / monthly.abs().sum() * 100) if monthly.abs().sum() > 0 else 0
            conc_y   = float(yearly.abs().max() / yearly.abs().sum() * 100) if yearly.abs().sum() > 0 else 0
            loss_m   = monthly[monthly < 0]
            conc_loss_m = float(loss_m.abs().max() / loss_m.abs().sum() * 100) if len(loss_m) > 0 and loss_m.abs().sum() > 0 else 0

            rows.append({
                "max_holding_days": h, "period": period,
                "max_single_loss": round(float(valid.min()), 3),
                "max_single_gain": round(float(valid.max()), 3),
                "max_consecutive_losses": max_consec_loss,
                "max_consecutive_wins":  max_consec_win,
                "max_drawdown": round(max_dd, 3),
                "max_dd_duration_trades": dd_dur,
                "worst_month": worst_m,
                "worst_month_ret": round(float(monthly.min()), 3) if len(monthly) > 0 else None,
                "best_month": best_m,
                "best_month_ret": round(float(monthly.max()), 3) if len(monthly) > 0 else None,
                "profit_conc_month": round(conc_m, 1),
                "profit_conc_year":  round(conc_y, 1),
                "loss_conc_month":   round(conc_loss_m, 1),
            })
    return pd.DataFrame(rows)


# ── Conditional hold ──────────────────────────────────────────────────────────

def conditional_hold_sim(tdf: pd.DataFrame, condition: str) -> pd.Series:
    """Apply conditional HD3→HD4 extension. Returns Series of returns."""
    def _apply(row):
        ret3   = row.get("ret_3")
        type3  = row.get("exit_type_3")
        ret4   = row.get("ret_4")
        # Only consider extending if HD3 was time_stop and ret4 is available
        if type3 != "time_stop" or ret4 is None or math.isnan(float(ret4)):
            return ret3
        entry = row.get("entry_price") or 0
        close3 = row.get("future_close_3d")

        extend = False
        if condition == "always":
            extend = True
        elif condition == "profit_at_d3":
            extend = (close3 is not None and not math.isnan(float(close3)) and
                      float(close3) >= entry)
        elif condition == "regime":
            extend = row.get("market_regime") in ["panic_rebound", "strong_risk_on"]
        elif condition == "profit_and_regime":
            extend = (
                (close3 is not None and not math.isnan(float(close3)) and float(close3) >= entry)
                and row.get("market_regime") in ["panic_rebound", "strong_risk_on"]
            )
        elif condition == "low_rsi":
            extend = (row.get("rsi14") or 100) < 80
        return ret4 if extend else ret3

    return tdf.apply(_apply, axis=1)


def conditional_hold_comparison(tdf: pd.DataFrame) -> pd.DataFrame:
    conditions = [
        ("HD3_fixed",           "fixed HD3 (baseline)"),
        ("HD4_fixed",           "fixed HD4"),
        ("HD5_fixed",           "fixed HD5"),
        ("cond_always",         "always extend to HD4"),
        ("cond_profit_at_d3",   "extend if profit at day3"),
        ("cond_regime",         "extend if panic_rebound/strong_risk_on"),
        ("cond_profit_regime",  "extend if profit AND regime"),
        ("cond_low_rsi",        "extend if RSI<80"),
    ]

    # Compute returns for each condition
    rets_dict = {
        "HD3_fixed":          tdf["ret_3"],
        "HD4_fixed":          tdf["ret_4"],
        "HD5_fixed":          tdf["ret_5"],
        "cond_always":        conditional_hold_sim(tdf, "always"),
        "cond_profit_at_d3":  conditional_hold_sim(tdf, "profit_at_d3"),
        "cond_regime":        conditional_hold_sim(tdf, "regime"),
        "cond_profit_regime": conditional_hold_sim(tdf, "profit_and_regime"),
        "cond_low_rsi":       conditional_hold_sim(tdf, "low_rsi"),
    }

    rows = []
    for model_id, desc in conditions:
        rets = rets_dict.get(model_id)
        if rets is None:
            continue
        for period in ["train", "test", "all"]:
            sub = tdf[_split_mask(tdf, period)]
            r   = rets[sub.index]
            m   = _metrics(r)
            if not m["n"]:
                continue
            # Extension rate (how often HD4 was used instead of HD3)
            if "cond_" in model_id:
                hd3_ts = sub["exit_type_3"] == "time_stop"
                cond_rets = rets[sub.index]
                hd4_rets  = sub["ret_4"]
                used_hd4  = hd3_ts & (cond_rets == hd4_rets)
                ext_rate  = float(used_hd4.sum() / len(sub) * 100)
            elif model_id == "HD4_fixed":
                ext_rate = 100.0
            else:
                ext_rate = 0.0

            rows.append({
                "exit_model": model_id,
                "description": desc,
                "period": period,
                **{k: round(v, 3) if isinstance(v, float) else v for k, v in m.items()},
                "avg_holding_days": None,
                "extension_rate": round(ext_rate, 1),
                "deploy_score": round(_deploy_score(m), 1),
            })
    return pd.DataFrame(rows)


# ── Equity curve ──────────────────────────────────────────────────────────────

def plot_equity_curves(tdf: pd.DataFrame, holds: list[int], out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 7))
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    for i, h in enumerate(holds):
        sub = tdf.sort_values("trade_date")
        for period_label, mask, lw, ls in [
            ("train", sub["trade_date"] <= TRAIN_END, 1.5, "-"),
            ("test",  sub["trade_date"] >= TEST_START, 2.5, "--"),
        ]:
            s = sub[mask][f"ret_{h}"].dropna()
            if len(s) == 0:
                continue
            cum = np.cumsum(s.values)
            ax.plot(range(len(cum)), cum,
                    color=colors[i % len(colors)], linewidth=lw, linestyle=ls,
                    label=f"HD{h} ({period_label})", alpha=0.85)

    train_n = (tdf["trade_date"] <= TRAIN_END).sum()
    ax.axvline(x=train_n, color="gray", linestyle=":", linewidth=1, label="train|test split")
    ax.set_title(f"H5 Holding Period Comparison (AI={AI_THRESHOLD}, PB={PULLBACK_PCT}, EST={abs(EM_STOP_PCT)}%)")
    ax.set_xlabel("Trade number")
    ax.set_ylabel("Cumulative return (%)")
    ax.legend(fontsize=8, ncol=4)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120)
    plt.close()


# ── Report ────────────────────────────────────────────────────────────────────

def generate_report(tdf: pd.DataFrame, holds: list[int],
                    summary: pd.DataFrame, exit_bd: pd.DataFrame,
                    ts_after: pd.DataFrame, ext_impact: pd.DataFrame,
                    cond_cmp: pd.DataFrame, out_path: Path) -> None:
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M JST")
    lines = [
        "=" * 70,
        "H5 HOLDING DAYS REASON REPORT",
        f"Generated: {now}",
        "=" * 70,
        "",
        "[Fixed Conditions]",
        f"  signal_prob >= {AI_THRESHOLD}",
        f"  drop20d <= {DROP_20D_MAX}%",
        f"  no_panic_selloff",
        f"  pullback = {PULLBACK_PCT}%",
        f"  emergency_stop = {abs(EM_STOP_PCT)}%",
        f"  overheat_mode = {OVERHEAT_MODE}",
        f"  Total candidates: {len(tdf)}",
        "",
    ]

    # Summary table
    lines.append("[Holding Days Summary (test)]")
    lines.append(f"  {'HD':<5} {'n':>5} {'wr':>6} {'avg':>6} {'PF':>5} {'mxL':>6} {'mxDD':>7} {'pb%':>5} {'ts%':>5} {'score':>7}")
    lines.append("  " + "-" * 70)
    test_sum = summary[summary["period"] == "test"]
    for _, r in test_sum.iterrows():
        lines.append(
            f"  HD{int(r['max_holding_days']):<4} "
            f"{int(r.get('n', 0) or 0):>5} "
            f"{r.get('win_rate', 0) or 0:>5.1f}% "
            f"{r.get('avg_ret', 0) or 0:>+5.2f}% "
            f"{r.get('pf', 0) or 0:>5.2f} "
            f"{r.get('max_loss', 0) or 0:>+5.2f}% "
            f"{r.get('max_dd', 0) or 0:>+6.2f}% "
            f"{r.get('pullback_exit_rate', 0) or 0:>5.1f} "
            f"{r.get('time_stop_rate', 0) or 0:>5.1f} "
            f"{r.get('deploy_score', 0) or 0:>7.1f}")
    lines.append("")

    # Exit reason breakdown
    lines.append("[Exit Reason Breakdown (test)]")
    for h in holds:
        bd_h = exit_bd[(exit_bd["max_holding_days"] == h) & (exit_bd["period"] == "test")]
        if len(bd_h) == 0:
            continue
        lines.append(f"  HD{h}:")
        for _, r in bd_h.iterrows():
            lines.append(f"    {r['exit_reason']:<18}: {r['ratio']:>5.1f}%  avg_ret={r.get('avg_ret', 0):>+5.2f}%")
    lines.append("")

    # Time stop after analysis
    lines.append("[HD3 Time-Stop: What Happens Next? (test)]")
    ts_test = ts_after[ts_after["period"] == "test"]
    for _, r in ts_test.iterrows():
        ext = r.get("extend_to", "")
        better = r.get("better_rate", 0)
        worse  = r.get("worse_rate", 0)
        avg_d  = r.get("avg_diff", 0)
        lines.append(
            f"  {ext}: better={better:.1f}%  worse={worse:.1f}%  avg_diff={avg_d:+.3f}%"
            f"  (max_fav={r.get('max_favorable', 0):+.2f}%  max_adv={r.get('max_adverse', 0):+.2f}%)")
    lines.append("")

    # Extension impact
    lines.append("[Holding Extension Net Impact (test)]")
    ei_test = ext_impact[ext_impact["period"] == "test"]
    for _, r in ei_test.iterrows():
        verdict = "BENEFIT" if (r.get("net_benefit") or 0) > 0 else "LOSS"
        lines.append(
            f"  {r['extension']}: helped={r.get('helped_rate', 0):.1f}%  "
            f"hurt={r.get('hurt_rate', 0):.1f}%  "
            f"avg_diff={r.get('avg_diff', 0):+.3f}%  "
            f"net={r.get('net_benefit', 0):+.2f}  [{verdict}]")
    lines.append("")

    # Conditional hold
    lines.append("[Conditional Hold Comparison (test)]")
    lines.append(f"  {'model':<24} {'n':>5} {'wr':>6} {'avg':>6} {'PF':>5} {'ext%':>5} {'score':>7}")
    cond_test = cond_cmp[cond_cmp["period"] == "test"]
    for _, r in cond_test.iterrows():
        lines.append(
            f"  {r['exit_model']:<24} "
            f"{int(r.get('n', 0) or 0):>5} "
            f"{r.get('win_rate', 0) or 0:>5.1f}% "
            f"{r.get('avg_ret', 0) or 0:>+5.2f}% "
            f"{r.get('pf', 0) or 0:>5.2f} "
            f"{r.get('extension_rate', 0) or 0:>5.1f} "
            f"{r.get('deploy_score', 0) or 0:>7.1f}")
    lines.append("")

    # Conclusion
    # Determine best hold
    if len(test_sum) > 0:
        best_h = test_sum.loc[test_sum["deploy_score"].idxmax(), "max_holding_days"]
        best_cond = cond_test.loc[cond_test["deploy_score"].idxmax(), "exit_model"] if len(cond_test) > 0 else "HD3_fixed"
        ts_day4_better = ts_test[ts_test["extend_to"] == "day4"].iloc[0].get("better_rate", 0) if len(ts_test[ts_test["extend_to"] == "day4"]) > 0 else 0
        lines += [
            "[Conclusion]",
            f"  Best fixed hold (deploy_score): HD{int(best_h)}",
            f"  Best conditional model: {best_cond}",
            f"  HD3 time_stop → day4 better rate: {ts_day4_better:.1f}%",
            "",
        ]
        if best_h == 3 and ts_day4_better < 50:
            lines.append("  → A: HD3採用でよい。延長するとDDや損失が拡大する。")
        elif ts_day4_better > 55 and (ext_impact[(ext_impact["extension"] == "HD4_minus_HD3") & (ext_impact["period"] == "test")]["net_benefit"].values[0] if len(ext_impact[(ext_impact["extension"] == "HD4_minus_HD3") & (ext_impact["period"] == "test")]) > 0 else -1) > 0:
            lines.append("  → B: HD4への延長に価値あり。")
        else:
            lines.append("  → D: 条件付き延長を検討。")

    lines += ["", "=" * 70]
    out_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("→ %s", out_path.name)


# ── Main ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--cache-path",   default=str(DEFAULT_CACHE))
    p.add_argument("--output-dir",   default=str(OUT_DIR))
    p.add_argument("--start",        default="2020-01-01")
    p.add_argument("--end",          default="2026-05-26")
    p.add_argument("--train-end",    default=TRAIN_END)
    p.add_argument("--ai-threshold", type=float, default=AI_THRESHOLD)
    p.add_argument("--pullback",     type=float, default=PULLBACK_PCT)
    p.add_argument("--stop-model",   default="emergency8")
    p.add_argument("--overheat-mode",default=OVERHEAT_MODE)
    p.add_argument("--holding-days", default="3,4,5,7")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    holds = [int(x) for x in args.holding_days.split(",")]
    em_stop = -float(args.stop_model.replace("emergency", "")) if "emergency" in args.stop_model else None
    pb_pct  = args.pullback

    config = {
        "start": args.start, "end": args.end, "train_end": args.train_end,
        "ai_threshold": args.ai_threshold, "pullback_pct": pb_pct,
        "stop_model": args.stop_model, "emergency_stop_pct": em_stop,
        "overheat_mode": args.overheat_mode, "holding_days": holds,
    }
    (out_dir / "holding_reason_config.json").write_text(
        json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")

    # Load & filter
    df = _load_df(Path(args.cache_path))
    df = _classify_overheat(df)
    df = df[(df["trade_date"] >= args.start) & (df["trade_date"] <= args.end)]
    df = _apply_h5_filters(df, args.ai_threshold, DROP_20D_MAX, args.overheat_mode)
    logger.info("H5 filtered candidates: %d", len(df))

    # Build per-trade simulation
    logger.info("Simulating HD %s...", holds)
    tdf = build_trade_df(df, holds, em_stop, pb_pct)
    logger.info("Trade DataFrame built: %d rows", len(tdf))

    # 1. Summary
    summary = holding_days_summary(tdf, holds)
    summary.to_csv(out_dir / "holding_days_summary.csv", index=False)
    logger.info("→ holding_days_summary.csv")

    # 2. Exit reason breakdown
    exit_bd = exit_reason_breakdown(tdf, holds)
    exit_bd.to_csv(out_dir / "exit_reason_breakdown.csv", index=False)
    logger.info("→ exit_reason_breakdown.csv")

    # 3. Exit day distribution
    exit_dd = exit_day_distribution(tdf, holds)
    exit_dd.to_csv(out_dir / "exit_day_distribution.csv", index=False)
    logger.info("→ exit_day_distribution.csv")

    # 4. Time stop after analysis
    ts_after = time_stop_after_analysis(tdf)
    ts_after.to_csv(out_dir / "time_stop_after_analysis.csv", index=False)
    logger.info("→ time_stop_after_analysis.csv")

    # 5-6. Per-trade diffs
    for h_ext in [h for h in holds if h > holds[0]]:
        diff = trade_diff(tdf, h_ext)
        diff.to_csv(out_dir / f"hd3_vs_hd{h_ext}_trade_diff.csv", index=False)
        logger.info("→ hd3_vs_hd%d_trade_diff.csv (%d rows)", h_ext, len(diff))

    # 7-8. Missed profit / avoided loss
    missed_avoided_rows = []
    for h_ext in [h for h in holds if h > holds[0]][:2]:  # HD4 and HD5
        diff_df = trade_diff(tdf, h_ext)
        ma = missed_avoided_analysis(diff_df, h_ext)
        missed_avoided_rows.append(ma)
    if missed_avoided_rows:
        missed = pd.concat([r[r["analysis"] == "missed_profit"] for r in missed_avoided_rows], ignore_index=True)
        avoided = pd.concat([r[r["analysis"] == "avoided_loss"] for r in missed_avoided_rows], ignore_index=True)
        missed.to_csv(out_dir / "missed_profit_analysis.csv", index=False)
        avoided.to_csv(out_dir / "avoided_loss_analysis.csv", index=False)
        logger.info("→ missed_profit_analysis.csv / avoided_loss_analysis.csv")

    # 9. Extension impact
    ext_impact = holding_extension_impact(tdf, holds)
    ext_impact.to_csv(out_dir / "holding_extension_impact.csv", index=False)
    logger.info("→ holding_extension_impact.csv")

    # 10. By regime
    reg = by_axis(tdf, holds, "market_regime")
    reg.to_csv(out_dir / "holding_by_regime.csv", index=False)
    logger.info("→ holding_by_regime.csv")

    # 11. By month
    tdf["_month"] = tdf["trade_date"].str[:7]
    mon = by_axis(tdf, holds, "_month")
    mon = mon.rename(columns={"_month": "month"})
    mon.to_csv(out_dir / "holding_by_month.csv", index=False)
    logger.info("→ holding_by_month.csv")

    # 12. By year
    tdf["_year"] = tdf["trade_date"].str[:4]
    yr = by_axis(tdf, holds, "_year")
    yr = yr.rename(columns={"_year": "year"})
    yr.to_csv(out_dir / "holding_by_year.csv", index=False)
    logger.info("→ holding_by_year.csv")

    # 13. Risk deep dive
    risk = risk_deep_dive(tdf, holds)
    risk.to_csv(out_dir / "holding_risk_deep_dive.csv", index=False)
    logger.info("→ holding_risk_deep_dive.csv")

    # 14. Conditional hold comparison
    cond_cmp = conditional_hold_comparison(tdf)
    cond_cmp.to_csv(out_dir / "conditional_hold_comparison.csv", index=False)
    logger.info("→ conditional_hold_comparison.csv")

    # 15. Equity curve
    plot_equity_curves(tdf, holds, out_dir / "equity_curve_holding_compare.png")
    logger.info("→ equity_curve_holding_compare.png")

    # 16. Report
    generate_report(tdf, holds, summary, exit_bd, ts_after,
                    ext_impact, cond_cmp, out_dir / "holding_reason_report.txt")

    logger.info("All done. Output: %s", out_dir)


if __name__ == "__main__":
    main()
