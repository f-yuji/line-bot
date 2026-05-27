#!/usr/bin/env python3
"""Filter and re-rank grid search results for real deployment candidates.

Reads existing grid_search_results.csv and related files, applies strict
filters, computes deploy_score, and outputs analysis-ready files.

Usage:
    python scripts/filter_grid_search_results.py \\
        --run-name overnight_2020_2026_v1 \\
        --input-dir outputs/rebound_grid_search/overnight_2020_2026_v1 \\
        --output-dir outputs/rebound_grid_search/overnight_2020_2026_v1_filtered \\
        --min-train-trades 100 --min-test-trades 20 \\
        --max-train-dd -35 --max-test-dd -35 --top-n 300
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

PARAM_COLS = [
    "p_entry_mode", "p_exit_mode", "p_stop_loss_pct", "p_max_holding_days",
    "p_max_margin_ratio", "p_max_positions", "p_max_daily_entries",
    "p_sector_limit", "p_panic_guard", "p_regime_filter",
    "p_nikkei_ma25_gap_limit", "p_signal_rsi_max", "p_signal_rsi_min", "p_ma5_gap_max",
]

METRIC_COLS = [
    "train_trade_count", "train_win_rate", "train_pf", "train_cagr", "train_max_dd",
    "train_sharpe", "train_monthly_consistency", "train_yearly_consistency",
    "train_exposure_ratio", "train_year_concentration", "train_month_concentration",
    "train_total_pnl",
    "test_trade_count", "test_win_rate", "test_pf", "test_cagr", "test_max_dd",
    "test_sharpe", "test_monthly_consistency", "test_yearly_consistency",
    "test_exposure_ratio", "test_year_concentration", "test_month_concentration",
    "test_total_pnl",
    "oos_pass", "oos_penalty", "low_trade_penalty",
    "year_concentration_penalty", "month_concentration_penalty",
    "regime_fragility_penalty", "total_penalty",
    "balanced_score", "conservative_score", "aggressive_score",
]

USECOLS = ["strategy_id", "params_hash"] + PARAM_COLS + METRIC_COLS


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-name", default="overnight_2020_2026_v1")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--min-train-trades", type=int, default=100)
    p.add_argument("--min-test-trades", type=int, default=20)
    p.add_argument("--max-train-dd", type=float, default=-35.0)
    p.add_argument("--max-test-dd", type=float, default=-35.0)
    p.add_argument("--exclude-panic-only", type=lambda x: x.lower() != "false", default=True)
    p.add_argument("--exclude-oos-fail", type=lambda x: x.lower() != "false", default=True)
    p.add_argument("--max-pf-for-low-trades", type=float, default=10.0)
    p.add_argument("--low-trade-threshold", type=int, default=100)
    p.add_argument("--top-n", type=int, default=300)
    return p.parse_args()


def _load_main(input_dir: Path) -> pd.DataFrame:
    path = input_dir / "grid_search_results.csv"
    logger.info("loading %s (%.0fMB)...", path, path.stat().st_size / 1e6)
    cols_in_file = pd.read_csv(path, nrows=0, encoding="utf-8-sig").columns.tolist()
    use = [c for c in USECOLS if c in cols_in_file]
    df = pd.read_csv(path, usecols=use, encoding="utf-8-sig", low_memory=False)
    logger.info("loaded %d rows, %d cols", len(df), len(df.columns))
    df["oos_pass"] = df["oos_pass"].astype(str).str.lower().isin(["true", "1", "yes"])
    for col in ["train_pf", "test_pf", "train_cagr", "test_cagr",
                "train_max_dd", "test_max_dd", "train_sharpe", "test_sharpe",
                "train_monthly_consistency", "balanced_score", "conservative_score", "aggressive_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df = df.sort_values("balanced_score", ascending=False)
    df = df.drop_duplicates(subset="strategy_id", keep="first")
    return df, before - len(df)


def _compute_deploy_score(df: pd.DataFrame, low_trade_thresh: int) -> pd.DataFrame:
    tc = df.get("train_cagr", 0).fillna(0)
    ec = df.get("test_cagr", 0).fillna(0)
    tp = df.get("train_pf", 1).fillna(1).clip(upper=50)
    ep = df.get("test_pf", 1).fillna(1).clip(upper=50)
    ts = df.get("train_sharpe", 0).fillna(0)
    es = df.get("test_sharpe", 0).fillna(0)
    mc = df.get("train_monthly_consistency", 0).fillna(0)
    tdd = df.get("train_max_dd", 0).fillna(0)
    edd = df.get("test_max_dd", 0).fillna(0)
    ttc = df.get("train_trade_count", 0).fillna(0)
    etc = df.get("test_trade_count", 0).fillna(0)

    # oos gap penalty
    cagr_gap = (tc - ec).abs()
    oos_gap_pen = (cagr_gap > 30).astype(float) * 20.0
    pf_ratio = ep / tp.replace(0, 1)
    oos_gap_pen += (pf_ratio > 5).astype(float) * 15.0
    cagr_ratio = ec / tc.replace(0, 0.01).abs()
    oos_gap_pen += ((tc > 0) & (cagr_ratio > 5)).astype(float) * 15.0
    oos_gap_pen = oos_gap_pen.clip(upper=40)

    # pf extreme penalty
    pf_ext_pen = (ep > 20).astype(float) * 10.0 + (ep > 10).astype(float) * 5.0

    # low trade penalty
    total_tc = ttc + etc
    lt_pen = ((low_trade_thresh - total_tc).clip(lower=0) / low_trade_thresh * 15.0)

    base = (
        tc * 1.0
        + ec * 1.5
        + tp * 8.0
        + ep * 10.0
        + ts * 5.0
        + es * 6.0
        + mc * 20.0
        - tdd.abs() * 2.0
        - edd.abs() * 2.5
        - oos_gap_pen
        - pf_ext_pen
        - lt_pen
    )

    df = df.copy()
    df["oos_gap_penalty"] = oos_gap_pen.round(2)
    df["pf_extreme_penalty"] = pf_ext_pen.round(2)
    df["low_trade_penalty_new"] = lt_pen.round(2)
    df["deploy_score"] = base.round(3)
    return df


def _apply_filters(
    df: pd.DataFrame,
    args: argparse.Namespace,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats: dict[str, int] = {"input": len(df)}

    # OOS fail
    oos_mask = df["oos_pass"] == True  # noqa: E712
    stats["excluded_oos_fail"] = (~oos_mask).sum()
    if args.exclude_oos_fail:
        df = df[oos_mask]

    # panic_only separation
    panic_mask = df.get("p_regime_filter", pd.Series(dtype=str)) == "panic_only"
    panic_df = df[panic_mask].copy()
    stats["excluded_panic_only"] = int(panic_mask.sum())
    if args.exclude_panic_only:
        df = df[~panic_mask]

    # trade count
    ttc_mask = df["train_trade_count"].fillna(0) >= args.min_train_trades
    stats["excluded_train_trades"] = (~ttc_mask).sum()
    df = df[ttc_mask]

    etc_mask = df["test_trade_count"].fillna(0) >= args.min_test_trades
    stats["excluded_test_trades"] = (~etc_mask).sum()
    df = df[etc_mask]

    # DD
    train_dd_mask = df["train_max_dd"].fillna(-9999) >= args.max_train_dd
    stats["excluded_train_dd"] = (~train_dd_mask).sum()
    df = df[train_dd_mask]

    test_dd_mask = df["test_max_dd"].fillna(-9999) >= args.max_test_dd
    stats["excluded_test_dd"] = (~test_dd_mask).sum()
    df = df[test_dd_mask]

    # PF anomaly
    total_tc = df["train_trade_count"].fillna(0) + df["test_trade_count"].fillna(0)
    pf_anom = (total_tc < args.low_trade_threshold) & (df["train_pf"].fillna(0) > args.max_pf_for_low_trades)
    stats["excluded_pf_anomaly"] = int(pf_anom.sum())
    df = df[~pf_anom]

    stats["passed"] = len(df)
    return df, panic_df, stats


def _build_panic_candidates(panic_df: pd.DataFrame, min_train: int = 30, min_test: int = 5) -> pd.DataFrame:
    if panic_df.empty:
        return panic_df
    mask = (
        (panic_df["oos_pass"] == True)  # noqa: E712
        & (panic_df["train_trade_count"].fillna(0) >= min_train)
        & (panic_df["test_trade_count"].fillna(0) >= min_test)
    )
    return panic_df[mask].sort_values("balanced_score", ascending=False)


def _build_strong_short_candidates(df: pd.DataFrame) -> pd.DataFrame:
    em_col = "entry_mode" if "entry_mode" in df.columns else "p_entry_mode"
    ex_col = "exit_mode" if "exit_mode" in df.columns else "p_exit_mode"
    hd_col = "max_holding_days" if "max_holding_days" in df.columns else "p_max_holding_days"
    rf_col = "regime_filter" if "regime_filter" in df.columns else "p_regime_filter"
    mask = (
        df.get(em_col, pd.Series(dtype=str)).fillna("") == "ai_close_entry_strong_only"
    ) & (
        df.get(hd_col, pd.Series(dtype=object)).astype(str) == "3"
    ) & (
        df.get(ex_col, pd.Series(dtype=str)).fillna("").isin(["pullback1", "pullback2", "trailing_3"])
    ) & (
        df.get(rf_col, pd.Series(dtype=str)).fillna("").isin(["no_panic", "no_euphoria", "all"])
    )
    return df[mask].sort_values("deploy_score", ascending=False)


def _rename_param_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "p_entry_mode": "entry_mode",
        "p_exit_mode": "exit_mode",
        "p_stop_loss_pct": "stop_loss_pct",
        "p_max_holding_days": "max_holding_days",
        "p_max_margin_ratio": "max_margin_ratio",
        "p_max_positions": "max_positions",
        "p_max_daily_entries": "max_daily_entries",
        "p_sector_limit": "sector_limit",
        "p_panic_guard": "panic_guard",
        "p_regime_filter": "regime_filter",
        "p_nikkei_ma25_gap_limit": "nikkei_ma25_gap_limit",
        "p_signal_rsi_max": "signal_rsi_max",
        "p_signal_rsi_min": "signal_rsi_min",
        "p_ma5_gap_max": "ma5_gap_max",
    }
    return df.rename(columns={k: v for k, v in rename.items() if k in df.columns})


def _deploy_output_cols(df: pd.DataFrame) -> pd.DataFrame:
    want = [
        "strategy_id", "deploy_score", "balanced_score", "conservative_score", "aggressive_score",
        "entry_mode", "exit_mode", "stop_loss_pct", "max_holding_days", "max_margin_ratio",
        "max_positions", "max_daily_entries", "sector_limit", "panic_guard", "regime_filter",
        "nikkei_ma25_gap_limit", "signal_rsi_max", "signal_rsi_min", "ma5_gap_max",
        "trade_count", "train_trade_count", "test_trade_count",
        "train_cagr", "test_cagr", "train_pf", "test_pf",
        "train_sharpe", "test_sharpe", "train_max_dd", "test_max_dd",
        "train_win_rate", "train_monthly_consistency",
        "train_exposure_ratio", "oos_pass",
        "oos_gap_penalty", "pf_extreme_penalty", "low_trade_penalty_new",
    ]
    for col in want:
        if col not in df.columns:
            df[col] = None
    return df[[c for c in want if c in df.columns]]


def _build_parameter_importance(df: pd.DataFrame) -> pd.DataFrame:
    params = [c for c in [
        "entry_mode", "exit_mode", "stop_loss_pct", "max_holding_days",
        "max_margin_ratio", "max_positions", "max_daily_entries", "sector_limit",
        "panic_guard", "regime_filter", "nikkei_ma25_gap_limit",
        "signal_rsi_max", "signal_rsi_min", "ma5_gap_max",
    ] if c in df.columns]

    rows = []
    for param in params:
        for val, grp in df.groupby(param, dropna=False):
            rows.append({
                "parameter_name": param,
                "parameter_value": str(val),
                "count": len(grp),
                "avg_deploy_score": grp["deploy_score"].mean() if "deploy_score" in grp else None,
                "median_deploy_score": grp["deploy_score"].median() if "deploy_score" in grp else None,
                "avg_balanced_score": grp["balanced_score"].mean() if "balanced_score" in grp else None,
                "avg_train_cagr": grp["train_cagr"].mean() if "train_cagr" in grp else None,
                "avg_test_cagr": grp["test_cagr"].mean() if "test_cagr" in grp else None,
                "avg_train_pf": grp["train_pf"].mean() if "train_pf" in grp else None,
                "avg_test_pf": grp["test_pf"].mean() if "test_pf" in grp else None,
                "avg_train_max_dd": grp["train_max_dd"].mean() if "train_max_dd" in grp else None,
                "avg_test_max_dd": grp["test_max_dd"].mean() if "test_max_dd" in grp else None,
                "avg_trade_count": grp["trade_count"].mean() if "trade_count" in grp else None,
                "oos_pass_rate": grp["oos_pass"].mean() if "oos_pass" in grp else None,
            })
    return pd.DataFrame(rows)


def _build_condition_effects(df: pd.DataFrame) -> pd.DataFrame:
    params = [c for c in [
        "entry_mode", "exit_mode", "stop_loss_pct", "max_holding_days",
        "max_margin_ratio", "max_positions", "panic_guard", "regime_filter",
    ] if c in df.columns]
    rows = []
    for param in params:
        for val, grp in df.groupby(param, dropna=False):
            rows.append({
                "parameter": param, "value": str(val), "count": len(grp),
                "avg_deploy_score": round(grp["deploy_score"].mean(), 2) if "deploy_score" in grp else None,
                "avg_test_cagr": round(grp["test_cagr"].mean(), 2) if "test_cagr" in grp else None,
                "avg_test_pf": round(grp["test_pf"].mean(), 3) if "test_pf" in grp else None,
                "avg_test_dd": round(grp["test_max_dd"].mean(), 2) if "test_max_dd" in grp else None,
                "avg_test_sharpe": round(grp["test_sharpe"].mean(), 3) if "test_sharpe" in grp else None,
            })
    return pd.DataFrame(rows)


def _build_compare_core_variants(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    # anchor: strong entry, holding=3, no_panic or no_euphoria
    anchor_mask = (
        df.get("entry_mode", pd.Series(dtype=str)) == "ai_close_entry_strong_only"
    ) & (
        df.get("max_holding_days", pd.Series(dtype=object)).astype(str) == "3"
    ) & (
        df.get("regime_filter", pd.Series(dtype=str)).isin(["no_panic", "no_euphoria", "all"])
    )
    base = df[anchor_mask].copy()
    if base.empty:
        # fall back to best entry
        base = df.copy()

    records = []
    compare_axes = {
        "exit_mode": ["pullback1", "pullback2", "trailing_3", "ma5_break"],
        "regime_filter": ["all", "no_panic", "no_euphoria"],
        "stop_loss_pct": [None, -6.0, -5.0, -4.0],
        "max_margin_ratio": [None, 5.0, 10.0, 20.0, 30.0],
    }
    for axis, values in compare_axes.items():
        for val in values:
            if axis not in base.columns:
                continue
            col = base[axis].astype(str)
            val_s = str(val) if val is not None else "None"
            grp = base[col == val_s]
            if grp.empty:
                grp = base[col.isin([val_s, str(val)])]
            if grp.empty:
                continue
            records.append({
                "axis": axis,
                "value": val_s,
                "count": len(grp),
                "avg_deploy_score": round(grp["deploy_score"].mean(), 2) if "deploy_score" in grp else None,
                "avg_train_cagr": round(grp["train_cagr"].mean(), 2) if "train_cagr" in grp else None,
                "avg_test_cagr": round(grp["test_cagr"].mean(), 2) if "test_cagr" in grp else None,
                "avg_train_pf": round(grp["train_pf"].mean(), 3) if "train_pf" in grp else None,
                "avg_test_pf": round(grp["test_pf"].mean(), 3) if "test_pf" in grp else None,
                "avg_train_dd": round(grp["train_max_dd"].mean(), 2) if "train_max_dd" in grp else None,
                "avg_test_dd": round(grp["test_max_dd"].mean(), 2) if "test_max_dd" in grp else None,
                "avg_train_sharpe": round(grp["train_sharpe"].mean(), 3) if "train_sharpe" in grp else None,
                "avg_test_sharpe": round(grp["test_sharpe"].mean(), 3) if "test_sharpe" in grp else None,
            })
    return pd.DataFrame(records)


def _load_returns(input_dir: Path, kind: str) -> pd.DataFrame:
    path = input_dir / f"{kind}_returns.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _filter_returns(returns: pd.DataFrame, top_ids: list[str]) -> pd.DataFrame:
    if returns.empty or not top_ids:
        return returns
    return returns[returns["strategy_id"].isin(top_ids)]


def _build_equity_curve(df_top: pd.DataFrame, monthly_returns: pd.DataFrame, out_path: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import numpy as np
    except ImportError:
        logger.warning("matplotlib not installed, skipping equity curve")
        return

    if df_top.empty or monthly_returns.empty:
        return

    top10_ids = df_top.head(10)["strategy_id"].tolist()
    mr = monthly_returns[monthly_returns["strategy_id"].isin(top10_ids)].copy()
    if mr.empty:
        return

    mr["month"] = pd.to_datetime(mr["month"].astype(str), format="%Y-%m", errors="coerce")
    mr = mr.dropna(subset=["month"])

    all_months = pd.date_range("2020-01", "2026-06", freq="MS")

    fig, ax = plt.subplots(figsize=(14, 7))
    for sid in top10_ids:
        grp = mr[mr["strategy_id"] == sid].set_index("month")["return_pct"].reindex(all_months, fill_value=0)
        cumret = (1 + grp / 100).cumprod() * 100 - 100
        label = sid[:40] + ("..." if len(sid) > 40 else "")
        ax.plot(all_months, cumret, linewidth=1.2, label=label)

    ax.axhline(0, color="black", linewidth=0.5, linestyle="--")
    ax.axvline(pd.Timestamp("2025-01-01"), color="gray", linewidth=1, linestyle=":", label="OOS start")
    ax.set_title("Filtered Top 10 (deploy_score) — Cumulative Return")
    ax.set_xlabel("Month")
    ax.set_ylabel("Cumulative Return (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.xticks(rotation=45)
    ax.legend(fontsize=6, loc="upper left")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    logger.info("equity curve saved: %s", out_path)


def _find_hypothesis_candidates(df: pd.DataFrame) -> pd.DataFrame:
    target_conds = [
        {"entry_mode": "ai_close_entry_strong_only", "exit_mode": "pullback2", "max_holding_days": "3", "regime_filter": "no_euphoria"},
        {"entry_mode": "ai_close_entry_strong_only", "exit_mode": "pullback2", "max_holding_days": "3", "regime_filter": "no_panic"},
        {"entry_mode": "ai_close_entry_strong_only", "exit_mode": "pullback1", "max_holding_days": "3", "regime_filter": "no_panic"},
    ]
    results = []
    for cond in target_conds:
        mask = pd.Series([True] * len(df), index=df.index)
        for col, val in cond.items():
            if col in df.columns:
                mask &= df[col].astype(str) == str(val)
        found = df[mask].sort_values("deploy_score", ascending=False).head(3)
        if not found.empty:
            found["_hypothesis"] = str(cond)
            results.append(found)
    if results:
        return pd.concat(results).drop_duplicates(subset="strategy_id")
    return pd.DataFrame()


def _build_report(
    args: argparse.Namespace,
    stats: dict,
    df_filtered: pd.DataFrame,
    df_top_deploy: pd.DataFrame,
    param_imp: pd.DataFrame,
    hypo_df: pd.DataFrame,
    dup_count: int,
) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("FILTERED GRID SEARCH REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append("[実行概要]")
    lines.append(f"  run_name           : {args.run_name}")
    lines.append(f"  入力件数           : {stats.get('input', 0):,}")
    lines.append(f"  重複削除           : {dup_count:,}")
    lines.append(f"  OOS除外            : {stats.get('excluded_oos_fail', 0):,}")
    lines.append(f"  panic_only除外     : {stats.get('excluded_panic_only', 0):,}")
    lines.append(f"  train trade不足    : {stats.get('excluded_train_trades', 0):,}")
    lines.append(f"  test trade不足     : {stats.get('excluded_test_trades', 0):,}")
    lines.append(f"  train DD超過       : {stats.get('excluded_train_dd', 0):,}")
    lines.append(f"  test DD超過        : {stats.get('excluded_test_dd', 0):,}")
    lines.append(f"  PF異常値除外       : {stats.get('excluded_pf_anomaly', 0):,}")
    lines.append(f"  フィルター通過     : {stats.get('passed', 0):,}")
    lines.append("")

    lines.append("[Top deploy strategies]")
    if df_top_deploy.empty:
        lines.append("  (なし)")
    else:
        for i, row in df_top_deploy.head(10).iterrows():
            lines.append(f"  #{list(df_top_deploy.index).index(i)+1:02d}: {row.get('strategy_id','')}")
            lines.append(f"       deploy={row.get('deploy_score',0):.1f}  balanced={row.get('balanced_score',0):.1f}")
            lines.append(f"       entry={row.get('entry_mode','')}  exit={row.get('exit_mode','')}  hold={row.get('max_holding_days','')}  regime={row.get('regime_filter','')}")
            lines.append(f"       train CAGR={row.get('train_cagr',0):.1f}%  PF={row.get('train_pf',0):.3f}  DD={row.get('train_max_dd',0):.1f}%  trades={row.get('train_trade_count',0):.0f}")
            lines.append(f"       test  CAGR={row.get('test_cagr',0):.1f}%  PF={row.get('test_pf',0):.3f}  DD={row.get('test_max_dd',0):.1f}%  trades={row.get('test_trade_count',0):.0f}")
    lines.append("")

    lines.append("[有力な条件傾向]")
    if not param_imp.empty and "avg_deploy_score" in param_imp.columns:
        for param in ["entry_mode", "exit_mode", "max_holding_days", "regime_filter", "stop_loss_pct"]:
            sub = param_imp[param_imp["parameter_name"] == param].sort_values("avg_deploy_score", ascending=False)
            if sub.empty:
                continue
            top_val = sub.iloc[0]
            lines.append(f"  {param}: {top_val['parameter_value']} が優勢 (avg_deploy={top_val['avg_deploy_score']:.1f}, count={top_val['count']})")
    lines.append("")

    lines.append("[暫定仮説確認]")
    lines.append("  仮説A: entry=ai_close_entry_strong_only が強い")
    lines.append("  仮説B: exit=pullback1/pullback2/trailing_3 が強い")
    lines.append("  仮説C: max_holding_days=3 が強い")
    lines.append("  仮説D: regime=no_panic/no_euphoria/all が候補")
    lines.append("  仮説E: panic_only は特殊局面専用として分離管理")
    lines.append("  仮説F: stop_loss=none が強く出るが実運用ではハードストップ別途検討")
    lines.append("")

    lines.append("[暫定本命候補]")
    if hypo_df.empty:
        lines.append("  仮説条件に合致する戦略なし。strong_short_candidates.csv を参照")
    else:
        for _, row in hypo_df.head(5).iterrows():
            lines.append(f"  {row.get('strategy_id','')}  deploy={row.get('deploy_score',0):.1f}")
            lines.append(f"    train: CAGR={row.get('train_cagr',0):.1f}% PF={row.get('train_pf',0):.3f} DD={row.get('train_max_dd',0):.1f}%")
            lines.append(f"    test : CAGR={row.get('test_cagr',0):.1f}% PF={row.get('test_pf',0):.3f} DD={row.get('test_max_dd',0):.1f}%")
    lines.append("")

    lines.append("[注意点]")
    lines.append("  - stop_loss=none が強いが、日足検証は場中急落・寄りGDを完全に表現できない")
    lines.append("  - test_PFが高すぎるものは期間ハマりの可能性あり")
    lines.append("  - deploy候補はforward-test前提。いきなり実弾投入しない")
    lines.append("")

    lines.append("[次のアクション]")
    lines.append("  1. top_filtered_deploy.csv の上位10件をforward-test候補に登録")
    lines.append("  2. strong_short_candidates.csv を重点確認")
    lines.append("  3. stop_loss=none と -6/-5 の比較を個別検証")
    lines.append("  4. pullback1/pullback2/trailing_3 を同条件で比較 → compare_core_variants.csv")
    lines.append("  5. no_panic/no_euphoria/all を同条件で比較 → compare_core_variants.csv")
    lines.append("")
    lines.append("=" * 70)
    return "\n".join(lines)


def run(args: argparse.Namespace) -> None:
    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load
    df = _load_main(input_dir)

    # Dedup by strategy_id (highest balanced_score wins)
    df, dup_count = _dedup(df)
    logger.info("after dedup: %d rows (removed %d duplicates)", len(df), dup_count)

    # Add total trade count
    df["trade_count"] = df["train_trade_count"].fillna(0) + df["test_trade_count"].fillna(0)

    # Filter
    df, panic_df, stats = _apply_filters(df, args)
    logger.info("filter stats: %s", stats)

    # Compute deploy score
    df = _compute_deploy_score(df, args.low_trade_threshold)
    if not panic_df.empty:
        panic_df = _compute_deploy_score(panic_df, args.low_trade_threshold)

    # Rename p_ cols
    df = _rename_param_cols(df)
    if not panic_df.empty:
        panic_df = _rename_param_cols(panic_df)

    # Sort by deploy_score
    df = df.sort_values("deploy_score", ascending=False).reset_index(drop=True)

    # Load returns
    monthly_returns = _load_returns(input_dir, "monthly")
    yearly_returns = _load_returns(input_dir, "yearly")

    top_ids = df.head(args.top_n)["strategy_id"].tolist()

    # --- Output files ---

    # 1. filtered_results.csv
    df.to_csv(out_dir / "filtered_results.csv", index=False, encoding="utf-8-sig")
    logger.info("saved filtered_results.csv (%d rows)", len(df))

    # 2. top_filtered_deploy.csv
    deploy_cols = _deploy_output_cols(df.head(args.top_n).copy())
    deploy_cols.to_csv(out_dir / "top_filtered_deploy.csv", index=False, encoding="utf-8-sig")

    # 3. top_filtered_balanced.csv
    df.sort_values("balanced_score", ascending=False).head(args.top_n).to_csv(
        out_dir / "top_filtered_balanced.csv", index=False, encoding="utf-8-sig")

    # 4. top_filtered_conservative.csv
    if "conservative_score" in df.columns:
        df.sort_values("conservative_score", ascending=False).head(args.top_n).to_csv(
            out_dir / "top_filtered_conservative.csv", index=False, encoding="utf-8-sig")

    # 5. top_filtered_aggressive.csv
    if "aggressive_score" in df.columns:
        df.sort_values("aggressive_score", ascending=False).head(args.top_n).to_csv(
            out_dir / "top_filtered_aggressive.csv", index=False, encoding="utf-8-sig")

    # 6. panic_only_candidates.csv
    panic_cands = _build_panic_candidates(panic_df)
    if not panic_cands.empty:
        _rename_param_cols(panic_cands).sort_values("deploy_score", ascending=False).head(args.top_n).to_csv(
            out_dir / "panic_only_candidates.csv", index=False, encoding="utf-8-sig")
        logger.info("panic_only_candidates: %d rows", len(panic_cands))

    # 7. strong_short_candidates.csv
    ssc = _build_strong_short_candidates(df)
    ssc.to_csv(out_dir / "strong_short_candidates.csv", index=False, encoding="utf-8-sig")
    logger.info("strong_short_candidates: %d rows", len(ssc))

    # 8. filtered_parameter_importance.csv
    param_imp = _build_parameter_importance(df)
    param_imp.to_csv(out_dir / "filtered_parameter_importance.csv", index=False, encoding="utf-8-sig")

    # 9. filtered_condition_effects.csv
    cond_eff = _build_condition_effects(df)
    cond_eff.to_csv(out_dir / "filtered_condition_effects.csv", index=False, encoding="utf-8-sig")

    # 10. filtered_monthly_returns.csv
    if not monthly_returns.empty:
        _filter_returns(monthly_returns, top_ids).to_csv(
            out_dir / "filtered_monthly_returns.csv", index=False, encoding="utf-8-sig")

    # 11. filtered_yearly_returns.csv
    if not yearly_returns.empty:
        _filter_returns(yearly_returns, top_ids).to_csv(
            out_dir / "filtered_yearly_returns.csv", index=False, encoding="utf-8-sig")

    # 12. equity_curve_filtered_top10.png
    df_top_deploy = df.sort_values("deploy_score", ascending=False).head(args.top_n)
    _build_equity_curve(df_top_deploy, monthly_returns, out_dir / "equity_curve_filtered_top10.png")

    # 13. compare_core_variants.csv
    compare = _build_compare_core_variants(df)
    compare.to_csv(out_dir / "compare_core_variants.csv", index=False, encoding="utf-8-sig")

    # 14. filtered_auto_report.txt
    hypo_df = _find_hypothesis_candidates(df)
    param_imp_named = param_imp.rename(columns={"parameter_name": "parameter_name"})
    report = _build_report(args, stats, df, df_top_deploy.head(10), param_imp_named, hypo_df, dup_count)
    (out_dir / "filtered_auto_report.txt").write_text(report, encoding="utf-8")
    logger.info("report saved")

    # 15. filter_config.json
    cfg = {
        "run_name": args.run_name,
        "min_train_trades": args.min_train_trades,
        "min_test_trades": args.min_test_trades,
        "max_train_dd": args.max_train_dd,
        "max_test_dd": args.max_test_dd,
        "exclude_panic_only": args.exclude_panic_only,
        "exclude_oos_fail": args.exclude_oos_fail,
        "max_pf_for_low_trades": args.max_pf_for_low_trades,
        "low_trade_threshold": args.low_trade_threshold,
        "top_n": args.top_n,
        "_timestamp": datetime.now(JST).isoformat(),
    }
    (out_dir / "filter_config.json").write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n" + report)
    print(f"\nAll outputs saved to: {out_dir}")
    print(f"Filtered results: {stats.get('passed', 0):,} rows")
    print(f"strong_short_candidates: {len(ssc)} rows")

    # Preview top_filtered_deploy
    if not deploy_cols.empty:
        print("\n[top_filtered_deploy.csv preview (top 10)]")
        preview_cols = ["strategy_id", "deploy_score", "entry_mode", "exit_mode",
                        "max_holding_days", "regime_filter", "train_cagr", "test_cagr",
                        "train_max_dd", "test_max_dd", "train_trade_count", "test_trade_count"]
        available = [c for c in preview_cols if c in deploy_cols.columns]
        print(deploy_cols[available].head(10).to_string(index=False))


if __name__ == "__main__":
    run(_parse_args())
