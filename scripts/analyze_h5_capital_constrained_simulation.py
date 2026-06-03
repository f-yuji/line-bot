#!/usr/bin/env python3
"""Capital-constrained simulation for walk-forward H5_full candidates.

Analysis only. This script does not update Primary, H5 rules, DB case
definitions, UI, LINE, actual_trade_logs, or any trading table.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import (  # noqa: E402
    add_sizing_columns,
    bucket_score,
    business_exit,
    date_text,
    load_walk_forward_h5_full_rows,
    parse_date,
    standardize,
    to_bool,
    to_float,
    write_csv,
    write_text,
)


DEFAULT_OUTPUT_DIR = "outputs/h5_capital_constrained_simulation"
CAPITAL_GRID = [1_000_000, 2_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000]
NOTIONAL_GRID = [100_000, 200_000, 300_000, 500_000]
SELECTION_METHODS = ["first", "ai_score_desc", "unit_amount_asc", "entry_gap_asc", "volume_desc", "random"]


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def pf_from_pnls(pnls: list[float]) -> float | None:
    gross_profit = sum(v for v in pnls if v > 0)
    gross_loss = -sum(v for v in pnls if v < 0)
    if gross_loss == 0:
        return None if gross_profit == 0 else float("inf")
    return gross_profit / gross_loss


def max_streak(values: list[float], want_loss: bool) -> int:
    best = cur = 0
    for value in values:
        ok = value < 0 if want_loss else value > 0
        if ok:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def pctile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    idx = max(0, min(len(vals) - 1, math.ceil(len(vals) * pct) - 1))
    return vals[idx]


def week_start(dt: date) -> date:
    return dt - timedelta(days=dt.weekday())


def trade_sort_key(method: str, rng: random.Random | None = None):
    if method == "first":
        return lambda r: (int(r.get("_row_index") or 0),)
    if method == "ai_score_desc":
        return lambda r: (-(to_float(r.get("signal_probability"), -1) or -1), int(r.get("_row_index") or 0))
    if method == "unit_amount_asc":
        return lambda r: (to_float(r.get("unit_amount"), math.inf) or math.inf, int(r.get("_row_index") or 0))
    if method == "entry_gap_asc":
        return lambda r: (to_float(r.get("entry_gap_pct"), math.inf) or math.inf, int(r.get("_row_index") or 0))
    if method == "volume_desc":
        return lambda r: (-(to_float(r.get("volume_ratio_20d"), None) or to_float(r.get("volume_ratio"), -1) or -1), int(r.get("_row_index") or 0))
    if method == "random":
        return None
    return lambda r: (int(r.get("_row_index") or 0),)


def order_candidates(rows: list[dict], method: str, rng: random.Random) -> list[dict]:
    items = list(rows)
    if method == "random":
        rng.shuffle(items)
        return items
    key = trade_sort_key(method, rng)
    return sorted(items, key=key)


def prepare_trade(row: dict, notional_per_trade: float) -> dict:
    entry = to_float(row.get("entry_price"))
    exitp = to_float(row.get("exit_price"))
    out = dict(row)
    if not entry or entry <= 0:
        out["_sim_skip_reason"] = "missing_entry_price"
        out["_sim_shares"] = 0
        out["_sim_notional"] = 0.0
        out["_sim_pnl"] = 0.0
        return out
    shares = math.floor(notional_per_trade / entry)
    out["_sim_shares"] = shares
    out["_sim_notional"] = shares * entry if shares > 0 else 0.0
    out["_sim_pnl"] = shares * (exitp - entry) if shares > 0 and exitp is not None else 0.0
    out["_sim_skip_reason"] = "too_expensive" if shares <= 0 else ""
    return out


def summarize_group(rows: list[dict], pnl_key: str = "_sim_pnl") -> dict:
    pnls = [to_float(r.get(pnl_key), 0) or 0 for r in rows]
    returns = [to_float(r.get("return_pct"), 0) or 0 for r in rows]
    wins = [v > 0 for v in returns]
    gross_profit = sum(v for v in pnls if v > 0)
    gross_loss = -sum(v for v in pnls if v < 0)
    return {
        "count": len(rows),
        "avg_return_pct": sum(returns) / len(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "win_rate": sum(wins) / len(wins) * 100 if wins else None,
        "total_pnl": sum(pnls),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": pf_from_pnls(pnls),
        "avg_pnl": sum(pnls) / len(pnls) if pnls else None,
        "max_win": max(pnls) if pnls else None,
        "max_loss": min(pnls) if pnls else None,
    }


def drawdown_from_curve(curve: list[dict]) -> dict:
    peak = 0.0
    max_dd = 0.0
    max_start = ""
    max_end = ""
    peak_date = ""
    for row in curve:
        equity = to_float(row.get("equity"), 0) or 0
        if equity > peak:
            peak = equity
            peak_date = row.get("date") or ""
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            max_start = peak_date
            max_end = row.get("date") or ""
    return {"max_dd_yen": max_dd, "max_dd_start": max_start, "max_dd_end": max_end}


def scenario_id(params: dict) -> str:
    parts = [
        f"cap{int(params['capital'])}",
        f"not{int(params['notional_per_trade'])}",
        str(params.get("selection_method") or "first"),
    ]
    if params.get("daily_entry_cap") is not None:
        parts.append(f"dcap{params['daily_entry_cap']}")
    if params.get("max_open_positions") is not None:
        parts.append(f"maxopen{params['max_open_positions']}")
    if params.get("cluster_threshold") is not None:
        parts.append(f"cl{params['cluster_threshold']}_{params.get('cluster_daily_cap')}")
    if params.get("seed") is not None:
        parts.append(f"seed{params['seed']}")
    return "_".join(parts)


def simulate(rows: list[dict], params: dict) -> dict:
    capital = float(params["capital"])
    notional = float(params["notional_per_trade"])
    method = str(params.get("selection_method") or "first")
    daily_cap = params.get("daily_entry_cap")
    max_open_positions = params.get("max_open_positions")
    cluster_threshold = params.get("cluster_threshold")
    cluster_daily_cap = params.get("cluster_daily_cap")
    cash_release = str(params.get("cash_release") or "same_day")
    rng = random.Random(int(params.get("seed") or 0))

    prepared = [prepare_trade(r, notional) for r in rows]
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in prepared:
        by_date[row["entry_date"]].append(row)

    open_positions: list[dict] = []
    executed: list[dict] = []
    skipped: list[dict] = []
    curve: list[dict] = []
    realized_by_date: Counter = Counter()
    equity = 0.0
    max_cash_used = 0.0
    all_dates = sorted(set(by_date.keys()) | {r.get("exit_date") for r in prepared if r.get("exit_date")})

    for current in all_dates:
        if not current:
            continue
        releasable: list[dict] = []
        still_open: list[dict] = []
        for pos in open_positions:
            exit_date = str(pos.get("exit_date") or "")
            release_now = exit_date <= current if cash_release == "same_day" else exit_date < current
            if release_now:
                releasable.append(pos)
            else:
                still_open.append(pos)
        open_positions = still_open
        daily_realized = 0.0
        for pos in releasable:
            pnl = to_float(pos.get("_sim_pnl"), 0) or 0
            daily_realized += pnl
            equity += pnl
            realized_by_date[current] += pnl

        candidates = order_candidates(by_date.get(current, []), method, rng)
        if cluster_threshold is not None and len(candidates) > int(cluster_threshold) and cluster_daily_cap is not None:
            selected_keys = {id(r) for r in candidates[: int(cluster_daily_cap)]}
            for row in candidates[int(cluster_daily_cap):]:
                nr = dict(row)
                nr["_skip_reason"] = "cluster_daily_cap"
                skipped.append(nr)
            candidates = [r for r in candidates if id(r) in selected_keys]
        if daily_cap is not None:
            for row in candidates[int(daily_cap):]:
                nr = dict(row)
                nr["_skip_reason"] = "daily_cap"
                skipped.append(nr)
            candidates = candidates[: int(daily_cap)]

        for row in candidates:
            if row.get("_sim_skip_reason") == "too_expensive":
                nr = dict(row)
                nr["_skip_reason"] = "too_expensive"
                skipped.append(nr)
                continue
            cash_used = sum(to_float(p.get("_sim_notional"), 0) or 0 for p in open_positions)
            if max_open_positions is not None and len(open_positions) >= int(max_open_positions):
                nr = dict(row)
                nr["_skip_reason"] = "max_open_positions"
                skipped.append(nr)
                continue
            actual_notional = to_float(row.get("_sim_notional"), 0) or 0
            if cash_used + actual_notional <= capital:
                nr = dict(row)
                nr["_entry_status"] = "executed"
                open_positions.append(nr)
                executed.append(nr)
                max_cash_used = max(max_cash_used, cash_used + actual_notional)
            else:
                nr = dict(row)
                nr["_skip_reason"] = "capital_limit"
                skipped.append(nr)

        cash_used = sum(to_float(p.get("_sim_notional"), 0) or 0 for p in open_positions)
        curve.append({
            "date": current,
            "equity": equity,
            "daily_realized_pnl": daily_realized,
            "open_positions": len(open_positions),
            "cash_used": cash_used,
            "available_cash": capital - cash_used,
            "capital_utilization": cash_used / capital * 100 if capital else None,
        })

    # Close remaining positions after the last candidate date for full PnL accounting.
    for pos in sorted(open_positions, key=lambda r: r.get("exit_date") or ""):
        pnl = to_float(pos.get("_sim_pnl"), 0) or 0
        equity += pnl
        current = str(pos.get("exit_date") or all_dates[-1])
        realized_by_date[current] += pnl
        curve.append({
            "date": current,
            "equity": equity,
            "daily_realized_pnl": pnl,
            "open_positions": 0,
            "cash_used": 0,
            "available_cash": capital,
            "capital_utilization": 0,
        })

    # Add drawdown fields.
    peak = 0.0
    for row in curve:
        eq = to_float(row.get("equity"), 0) or 0
        peak = max(peak, eq)
        dd = peak - eq
        row["drawdown"] = dd
        row["drawdown_pct"] = dd / capital * 100 if capital else None

    dd = drawdown_from_curve(curve)
    exec_summary = summarize_group(executed)
    skipped_summary = summarize_group(skipped)
    pnls = [to_float(r.get("_sim_pnl"), 0) or 0 for r in executed]
    top_count = max(1, math.ceil(len(rows) * 0.05))
    top_keys = {
        (r.get("entry_date"), r.get("code"))
        for r in sorted(rows, key=lambda r: to_float(r.get("return_pct"), -999) or -999, reverse=True)[:top_count]
    }
    executed_top = {(r.get("entry_date"), r.get("code")) for r in executed}
    summary = {
        "scenario_id": scenario_id(params),
        "capital": capital,
        "notional_per_trade": notional,
        "selection_method": method,
        "daily_entry_cap": daily_cap,
        "max_open_positions": max_open_positions,
        "cluster_threshold": cluster_threshold,
        "cluster_daily_cap": cluster_daily_cap,
        "trades_total": len(rows),
        "executed_count": len(executed),
        "skipped_count": len(skipped),
        "skipped_capital_limit_count": sum(1 for r in skipped if r.get("_skip_reason") == "capital_limit"),
        "skipped_too_expensive_count": sum(1 for r in skipped if r.get("_skip_reason") == "too_expensive"),
        "coverage_pct": len(executed) / len(rows) * 100 if rows else None,
        "total_pnl": exec_summary["total_pnl"],
        "gross_profit": exec_summary["gross_profit"],
        "gross_loss": exec_summary["gross_loss"],
        "profit_factor": exec_summary["profit_factor"],
        "avg_return_pct": exec_summary["avg_return_pct"],
        "median_return_pct": exec_summary["median_return_pct"],
        "win_rate": exec_summary["win_rate"],
        "avg_pnl_per_trade": exec_summary["avg_pnl"],
        "max_win": exec_summary["max_win"],
        "max_loss": exec_summary["max_loss"],
        "max_dd_yen": dd["max_dd_yen"],
        "max_dd_pct_of_capital": dd["max_dd_yen"] / capital * 100 if capital else None,
        "max_consecutive_losses": max_streak(pnls, True),
        "max_consecutive_wins": max_streak(pnls, False),
        "max_open_positions_used": max((int(r.get("open_positions") or 0) for r in curve), default=0),
        "avg_open_positions": sum(int(r.get("open_positions") or 0) for r in curve) / len(curve) if curve else 0,
        "max_cash_used": max_cash_used,
        "avg_cash_used": sum(to_float(r.get("cash_used"), 0) or 0 for r in curve) / len(curve) if curve else 0,
        "capital_utilization_avg": sum(to_float(r.get("capital_utilization"), 0) or 0 for r in curve) / len(curve) if curve else 0,
        "capital_utilization_max": max((to_float(r.get("capital_utilization"), 0) or 0 for r in curve), default=0),
        "ending_equity": equity,
        "skipped_avg_return_pct": skipped_summary["avg_return_pct"],
        "skipped_total_theoretical_pnl": skipped_summary["total_pnl"],
        "skipped_profit_factor": skipped_summary["profit_factor"],
        "skipped_top_winner_return": max((to_float(r.get("return_pct"), -999) or -999 for r in skipped), default=None),
        "right_tail_capture_rate": len(top_keys & executed_top) / len(top_keys) * 100 if top_keys else None,
    }
    return {"summary": summary, "executed": executed, "skipped": skipped, "curve": curve}


def aggregate_random(rows: list[dict], base_params: dict, seeds: list[int]) -> list[dict]:
    summaries = []
    for seed in seeds:
        params = dict(base_params)
        params["selection_method"] = "random"
        params["seed"] = seed
        summaries.append(simulate(rows, params)["summary"])
    out = []
    for label, reducer in (
        ("random_mean", lambda vals: sum(vals) / len(vals) if vals else None),
        ("random_p10", lambda vals: pctile(vals, 0.10)),
        ("random_p90", lambda vals: pctile(vals, 0.90)),
    ):
        row = dict(base_params)
        row["selection_method"] = label
        row["scenario_id"] = scenario_id(row)
        for key in summaries[0].keys() if summaries else []:
            if key in {"scenario_id", "selection_method"}:
                continue
            vals = [to_float(s.get(key), None) for s in summaries if to_float(s.get(key), None) is not None]
            row[key] = reducer(vals) if vals else summaries[0].get(key) if summaries else None
        out.append(row)
    return out


def top_rows(rows: list[dict], reverse: bool = True, n: int = 50) -> list[dict]:
    return sorted(rows, key=lambda r: to_float(r.get("_sim_pnl"), 0) or 0, reverse=reverse)[:n]


def weekly_result_summary(executed: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in executed:
        dt = parse_date(row.get("entry_date"))
        if dt:
            groups[week_start(dt).isoformat()].append(row)
    out = []
    for ws, items in sorted(groups.items()):
        s = summarize_group(items)
        out.append({"week_start": ws, **s})
    return out


def matrix_rows(summaries: list[dict], value_key: str) -> list[dict]:
    out = []
    for row in summaries:
        if row.get("selection_method") != "first":
            continue
        out.append({
            "capital": row.get("capital"),
            "notional_per_trade": row.get("notional_per_trade"),
            value_key: row.get(value_key),
        })
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate capital-constrained H5 walk-forward operation")
    parser.add_argument("--input", default="")
    parser.add_argument("--capital", type=float, default=5_000_000)
    parser.add_argument("--notional-per-trade", type=float, default=300_000)
    parser.add_argument("--holding-days", type=int, default=3)
    parser.add_argument("--historical-source", default="walk-forward-h5-full")
    parser.add_argument("--selection-method", default="first")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--include-open", default="false")
    parser.add_argument("--cash-release", choices=["same_day", "next_day"], default="same_day")
    parser.add_argument("--daily-entry-cap", type=int, default=None)
    parser.add_argument("--max-open-positions", type=int, default=None)
    parser.add_argument("--cluster-threshold", type=int, default=None)
    parser.add_argument("--cluster-daily-cap", type=int, default=None)
    parser.add_argument("--grid", default="false")
    parser.add_argument("--cluster-grid", default="false")
    parser.add_argument("--random-seeds", default="0,1,2,3,4,5,10,42,99,123")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    outdir = ROOT / args.output_dir
    loader_args = argparse.Namespace(candidate_log=args.input, start_date="", end_date="")
    rows, skipped, sources = load_walk_forward_h5_full_rows(loader_args)
    standardized = [standardize(r) for r in rows]
    for i, row in enumerate(standardized):
        row["_row_index"] = i
    add_sizing_columns(standardized)
    write_csv(outdir / "01_normalized_h5_full_dataset.csv", standardized)

    daily_counts = []
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in standardized:
        by_day[row["entry_date"]].append(row)
    for dt, items in sorted(by_day.items()):
        daily_counts.append({"date": dt, "h5_full_count": len(items)})
    write_csv(outdir / "02_daily_signal_counts.csv", daily_counts)

    seeds = [int(x.strip()) for x in str(args.random_seeds).split(",") if x.strip()]
    base_params = {
        "capital": args.capital,
        "notional_per_trade": args.notional_per_trade,
        "selection_method": args.selection_method,
        "daily_entry_cap": args.daily_entry_cap,
        "max_open_positions": args.max_open_positions,
        "cluster_threshold": args.cluster_threshold,
        "cluster_daily_cap": args.cluster_daily_cap,
        "cash_release": args.cash_release,
        "seed": 0,
    }
    main_result = simulate(standardized, base_params)
    write_csv(outdir / "03_simulation_summary.csv", [main_result["summary"]])
    write_csv(outdir / "05_executed_trades.csv", main_result["executed"])
    write_csv(outdir / "06_skipped_trades.csv", main_result["skipped"])
    write_csv(outdir / "07_open_positions_timeline.csv", main_result["curve"])
    write_csv(outdir / "08_equity_curve.csv", main_result["curve"])
    dd = drawdown_from_curve(main_result["curve"])
    write_csv(outdir / "09_drawdown_summary.csv", [{**main_result["summary"], **dd}])

    exec_s = summarize_group(main_result["executed"])
    skip_s = summarize_group(main_result["skipped"])
    write_csv(outdir / "10_skipped_vs_executed_summary.csv", [
        {"group": "executed", **exec_s},
        {"group": "skipped", **skip_s},
        {
            "group": "delta_executed_minus_skipped",
            "avg_return_pct": (exec_s.get("avg_return_pct") or 0) - (skip_s.get("avg_return_pct") or 0),
            "total_pnl": (exec_s.get("total_pnl") or 0) - (skip_s.get("total_pnl") or 0),
        },
    ])

    grid_summaries: list[dict] = []
    if to_bool(args.grid):
        for capital in CAPITAL_GRID:
            for notional in NOTIONAL_GRID:
                for method in ("first", "ai_score_desc", "unit_amount_asc"):
                    params = dict(base_params, capital=capital, notional_per_trade=notional, selection_method=method, seed=0)
                    grid_summaries.append(simulate(standardized, params)["summary"])
                rand_base = dict(base_params, capital=capital, notional_per_trade=notional, selection_method="random")
                grid_summaries.extend(aggregate_random(standardized, rand_base, seeds))
    else:
        for method in ("first", "ai_score_desc", "unit_amount_asc", "entry_gap_asc", "volume_desc"):
            params = dict(base_params, selection_method=method, seed=0)
            grid_summaries.append(simulate(standardized, params)["summary"])
        grid_summaries.extend(aggregate_random(standardized, dict(base_params, selection_method="random"), seeds))
    write_csv(outdir / "04_grid_summary.csv", grid_summaries)
    write_csv(outdir / "11_selection_method_comparison.csv", grid_summaries)

    daily_cap_rows = []
    for cap in (None, 1, 2, 3, 5, 10):
        for method in ("first", "random", "ai_score_desc"):
            if method == "random":
                rows_agg = aggregate_random(standardized, dict(base_params, daily_entry_cap=cap, selection_method="random"), seeds)
                daily_cap_rows.extend(rows_agg)
            else:
                daily_cap_rows.append(simulate(standardized, dict(base_params, daily_entry_cap=cap, selection_method=method, seed=0))["summary"])
    write_csv(outdir / "12_daily_cap_comparison.csv", daily_cap_rows)

    cluster_rows = []
    for threshold in (5, 10, 20, 50):
        for cap in (3, 5, 10):
            for method in ("first", "random", "ai_score_desc", "unit_amount_asc"):
                if method == "random":
                    cluster_rows.extend(aggregate_random(standardized, dict(base_params, cluster_threshold=threshold, cluster_daily_cap=cap, selection_method="random"), seeds))
                else:
                    cluster_rows.append(simulate(standardized, dict(base_params, cluster_threshold=threshold, cluster_daily_cap=cap, selection_method=method, seed=0))["summary"])
    write_csv(outdir / "13_cluster_rule_comparison.csv", cluster_rows)

    matrix = grid_summaries if to_bool(args.grid) else []
    write_csv(outdir / "14_capital_notional_matrix.csv", matrix)
    write_csv(outdir / "14a_matrix_total_pnl.csv", matrix_rows(matrix, "total_pnl"))
    write_csv(outdir / "14b_matrix_pf.csv", matrix_rows(matrix, "profit_factor"))
    write_csv(outdir / "14c_matrix_max_dd_pct.csv", matrix_rows(matrix, "max_dd_pct_of_capital"))
    write_csv(outdir / "14d_matrix_coverage_pct.csv", matrix_rows(matrix, "coverage_pct"))

    write_csv(outdir / "15_top_executed_winners.csv", top_rows(main_result["executed"], True))
    write_csv(outdir / "16_top_executed_losers.csv", top_rows(main_result["executed"], False))
    write_csv(outdir / "17_top_skipped_winners.csv", top_rows(main_result["skipped"], True))
    write_csv(outdir / "18_top_skipped_losers.csv", top_rows(main_result["skipped"], False))
    write_csv(outdir / "19_weekly_result_summary.csv", weekly_result_summary(main_result["executed"]))
    write_csv(outdir / "00_input_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in skipped.most_common()])

    s = main_result["summary"]
    def best_by_pnl(rows):
        if not rows:
            return {}
        return max(rows, key=lambda r: to_float(r.get("total_pnl"), -10**18))

    def fmt_row(row):
        if not row:
            return "unavailable"
        bits = [
            f"scenario_id={row.get('scenario_id')}",
            f"executed={row.get('executed_count')}",
            f"coverage={row.get('coverage_pct')}",
            f"total_pnl={row.get('total_pnl')}",
            f"PF={row.get('profit_factor')}",
            f"max_dd={row.get('max_dd_yen')}",
            f"right_tail_capture={row.get('right_tail_capture_rate')}",
        ]
        return ", ".join(bits)

    selection_best = best_by_pnl([
        row for row in grid_summaries
        if row.get("capital") == float(args.capital)
        and row.get("notional_per_trade") == float(args.notional_per_trade)
        and row.get("daily_entry_cap") is None
        and row.get("cluster_threshold") is None
    ])
    daily_best = best_by_pnl(daily_cap_rows)
    cluster_best = best_by_pnl(cluster_rows)
    grid_best = best_by_pnl(grid_summaries)
    report = f"""
H5 capital-constrained simulation

Input:
{chr(10).join('- ' + src for src in sources) or '- none'}
walk_forward_h5_full_rows: {len(standardized)}
period_start: {min((r.get('entry_date') for r in standardized), default='')}
period_end: {max((r.get('entry_date') for r in standardized), default='')}

Main scenario:
capital: {args.capital:,.0f}
notional_per_trade: {args.notional_per_trade:,.0f}
selection_method: {args.selection_method}
holding_days: {args.holding_days}
cash_release: {args.cash_release}

Result:
executed_count: {s.get('executed_count')}
skipped_count: {s.get('skipped_count')}
coverage_pct: {s.get('coverage_pct')}
total_pnl: {s.get('total_pnl')}
profit_factor: {s.get('profit_factor')}
avg_return_pct: {s.get('avg_return_pct')}
win_rate: {s.get('win_rate')}
max_dd_yen: {s.get('max_dd_yen')}
max_dd_pct_of_capital: {s.get('max_dd_pct_of_capital')}
max_open_positions_used: {s.get('max_open_positions_used')}
max_cash_used: {s.get('max_cash_used')}
capital_utilization_max: {s.get('capital_utilization_max')}

Skipped side:
skipped_avg_return_pct: {s.get('skipped_avg_return_pct')}
skipped_total_theoretical_pnl: {s.get('skipped_total_theoretical_pnl')}
skipped_profit_factor: {s.get('skipped_profit_factor')}
skipped_top_winner_return: {s.get('skipped_top_winner_return')}
right_tail_capture_rate: {s.get('right_tail_capture_rate')}

Comparison highlights:
best_selection_same_capital_notional:
{fmt_row(selection_best)}

best_daily_cap_scenario:
{fmt_row(daily_best)}

best_cluster_rule_scenario:
{fmt_row(cluster_best)}

best_grid_scenario:
{fmt_row(grid_best)}

Interpretation:
This is not an all-entry test. It only enters new H5_full candidates when capital is available.
Skipped trades are retained with theoretical S-share PnL so missed right-tail risk can be reviewed.
If skipped_avg_return_pct or skipped_profit_factor is higher than executed, the capital rule is
leaving some right-tail candidates outside the book. In that case, compare lower notional,
higher capital, daily-cap, and cluster-rule outputs before treating the first-come rule as final.
See 11_selection_method_comparison.csv, 12_daily_cap_comparison.csv, and
13_cluster_rule_comparison.csv for capital-constrained alternatives.

Analysis only. Primary, H5 rules, DB case definitions, UI, LINE, actual_trade_logs,
and auto-trading were not changed.
"""
    write_text(outdir / "00_input_summary.txt", report)
    write_text(outdir / "20_report.txt", report)
    print(report.strip())


if __name__ == "__main__":
    main()
