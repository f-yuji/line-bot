#!/usr/bin/env python3
"""Realistic S-share operation analysis for walk-forward H5_full.

Includes next-open entry, S-share sizing, capital limit, daily cap, GU filters,
round-trip cost bps, and profit tax. Analysis only; no production state is
updated.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import (  # noqa: E402
    load_walk_forward_h5_full_rows,
    standardize,
    to_float,
    write_csv,
    write_text,
)
from analyze_h5_s_share_execution_timing import (  # noqa: E402
    gap_bucket,
    load_all_wf_dates,
    load_next_open_rows,
    make_execution_rows,
    next_date_map,
)


DEFAULT_INPUT = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
DEFAULT_OUT = "outputs/h5_s_share_realistic_operation"


def parse_list(raw: str, *, as_float: bool = False) -> list[Any]:
    out = []
    for part in str(raw or "").split(","):
        item = part.strip()
        if not item:
            continue
        if item.lower() in {"none", "null", "na"}:
            out.append(None)
        elif as_float:
            out.append(float(item))
        else:
            out.append(int(float(item)))
    return out


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def pf(pnls: list[float]) -> float | None:
    gp = sum(v for v in pnls if v > 0)
    gl = -sum(v for v in pnls if v < 0)
    if gl == 0:
        return None if gp == 0 else float("inf")
    return gp / gl


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


def parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def scenario_id(params: dict) -> str:
    cap = "none" if params.get("daily_cap") is None else str(params.get("daily_cap"))
    gap = "none" if params.get("gap_limit") is None else str(params.get("gap_limit")).replace(".", "_")
    tax = "tax" if params.get("apply_tax") else "pretax"
    return (
        f"cap{int(params['capital'])}_not{int(params['notional'])}_"
        f"dcap{cap}_gap{gap}_cost{int(params['cost_bps'])}_{tax}"
    )


def trade_sort_key(row: dict) -> tuple:
    return (str(row.get("entry_date") or ""), int(to_float(row.get("_row_index"), 0) or 0))


def prepare_trade(row: dict, notional: float, cost_bps: float, tax_rate: float) -> dict:
    out = dict(row)
    entry = to_float(out.get("entry_price"))
    exitp = to_float(out.get("exit_price"))
    if not entry or entry <= 0 or exitp is None:
        out["_realistic_skip_reason"] = "invalid_price"
        out["_shares"] = 0
        out["_actual_notional"] = 0.0
        return out
    shares = math.floor(notional / entry)
    if shares <= 0:
        out["_realistic_skip_reason"] = "too_expensive"
        out["_shares"] = 0
        out["_actual_notional"] = 0.0
        return out
    actual = shares * entry
    before = shares * (exitp - entry)
    cost = actual * cost_bps / 10_000.0
    after_cost = before - cost
    tax = max(after_cost, 0.0) * tax_rate
    after_tax = after_cost - tax
    out["_shares"] = shares
    out["_actual_notional"] = actual
    out["_pnl_before_cost_tax"] = before
    out["_round_trip_cost"] = cost
    out["_pnl_after_cost"] = after_cost
    out["_tax"] = tax
    out["_pnl_after_tax"] = after_tax
    out["_realistic_skip_reason"] = ""
    return out


def pnl_metrics(rows: list[dict], pnl_key: str) -> dict:
    pnls = [to_float(r.get(pnl_key), 0) or 0 for r in rows]
    rets = [to_float(r.get("return_pct"), 0) or 0 for r in rows]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v < 0]
    return {
        "count": len(rows),
        "total_pnl": sum(pnls),
        "gross_profit": sum(wins),
        "gross_loss": -sum(losses),
        "profit_factor": pf(pnls),
        "avg_return_pct": sum(rets) / len(rets) if rets else None,
        "median_return_pct": median(rets) if rets else None,
        "win_rate": len(wins) / len(pnls) * 100 if pnls else None,
        "avg_pnl": sum(pnls) / len(pnls) if pnls else None,
        "max_win": max(pnls) if pnls else None,
        "max_loss": min(pnls) if pnls else None,
    }


def summarize_skips(rows: list[dict], executed: list[dict]) -> list[dict]:
    out = []
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[str(row.get("_entry_status") or "unknown")].append(row)
    groups["executed"] = executed
    for name, items in sorted(groups.items()):
        rec = {"group": name}
        rec.update(pnl_metrics(items, "_pnl_after_tax"))
        rec["top_winner_return_pct"] = max([to_float(r.get("return_pct"), -999) or -999 for r in items], default=None)
        out.append(rec)
    return out


def top_tail_capture(all_rows: list[dict], executed: list[dict]) -> float | None:
    if not all_rows:
        return None
    top_n = max(1, math.ceil(len(all_rows) * 0.05))
    top = sorted(all_rows, key=lambda r: to_float(r.get("return_pct"), -999) or -999, reverse=True)[:top_n]
    top_keys = {(str(r.get("signal_date") or r.get("entry_date")), str(r.get("code"))) for r in top}
    exe_keys = {(str(r.get("signal_date") or r.get("entry_date")), str(r.get("code"))) for r in executed}
    return len(top_keys & exe_keys) / len(top_keys) * 100 if top_keys else None


def simulate_realistic(rows: list[dict], params: dict) -> dict:
    prepared = [prepare_trade(r, params["notional"], params["cost_bps"], params["tax_rate"]) for r in rows]
    prepared = [r for r in prepared if not r.get("_realistic_skip_reason")]
    prepared.sort(key=trade_sort_key)
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in prepared:
        by_date[str(row.get("entry_date") or "")].append(row)
    dates = sorted(set(by_date) | {str(r.get("exit_date") or "") for r in prepared if r.get("exit_date")})
    open_positions: list[dict] = []
    executed: list[dict] = []
    skipped: list[dict] = []
    curve = []
    realized_before = 0.0
    realized_after = 0.0
    peak_after = 0.0
    max_dd = 0.0
    cash_used = 0.0
    daily_cap = params.get("daily_cap")
    gap_limit = params.get("gap_limit")
    capital = float(params["capital"])

    for cur in dates:
        exiting = [p for p in open_positions if str(p.get("exit_date") or "") <= cur]
        remaining = [p for p in open_positions if str(p.get("exit_date") or "") > cur]
        daily_before = sum(to_float(p.get("_pnl_before_cost_tax"), 0) or 0 for p in exiting)
        daily_after = sum(to_float(p.get("_pnl_after_tax"), 0) or 0 for p in exiting)
        realized_before += daily_before
        realized_after += daily_after
        cash_used = sum(to_float(p.get("_actual_notional"), 0) or 0 for p in remaining)
        open_positions = remaining

        day_entries = 0
        for row in by_date.get(cur, []):
            candidate = dict(row)
            gap = to_float(candidate.get("entry_gap_pct"))
            if gap_limit is not None and gap is not None and gap > float(gap_limit):
                candidate["_entry_status"] = "skip_gap_limit"
                skipped.append(candidate)
                continue
            if daily_cap is not None and day_entries >= int(daily_cap):
                candidate["_entry_status"] = "skip_daily_cap"
                skipped.append(candidate)
                continue
            actual = to_float(candidate.get("_actual_notional"), 0) or 0
            if cash_used + actual > capital:
                candidate["_entry_status"] = "skip_capital_limit"
                skipped.append(candidate)
                continue
            candidate["_entry_status"] = "executed"
            executed.append(candidate)
            open_positions.append(candidate)
            cash_used += actual
            day_entries += 1

        peak_after = max(peak_after, realized_after)
        dd = realized_after - peak_after
        max_dd = min(max_dd, dd)
        curve.append({
            "scenario_id": params["scenario_id"],
            "date": cur,
            "equity_before_tax": round(realized_before, 4),
            "equity_after_tax": round(realized_after, 4),
            "daily_realized_pnl": round(daily_after, 4),
            "daily_cost": round(sum(to_float(p.get("_round_trip_cost"), 0) or 0 for p in exiting), 4),
            "daily_tax": round(sum(to_float(p.get("_tax"), 0) or 0 for p in exiting), 4),
            "open_positions": len(open_positions),
            "cash_used": round(cash_used, 4),
            "available_cash": round(capital - cash_used, 4),
            "capital_utilization": round(cash_used / capital * 100, 6) if capital else None,
            "drawdown_after_tax": round(dd, 4),
            "drawdown_pct_after_tax": round(abs(dd) / capital * 100, 6) if capital else None,
        })

    # Realize any remaining positions at their planned exit for summary consistency.
    for p in open_positions:
        realized_before += to_float(p.get("_pnl_before_cost_tax"), 0) or 0
        realized_after += to_float(p.get("_pnl_after_tax"), 0) or 0

    before = pnl_metrics(executed, "_pnl_before_cost_tax")
    after_cost = pnl_metrics(executed, "_pnl_after_cost")
    after_tax = pnl_metrics(executed, "_pnl_after_tax")
    skipped_capital = [r for r in skipped if r.get("_entry_status") == "skip_capital_limit"]
    summary = {
        "scenario_id": params["scenario_id"],
        "capital": params["capital"],
        "notional_per_trade": params["notional"],
        "entry_mode": params["entry_mode"],
        "daily_cap": params.get("daily_cap"),
        "gap_limit": params.get("gap_limit"),
        "tax_rate": params["tax_rate"],
        "round_trip_cost_bps": params["cost_bps"],
        "executed_count": len(executed),
        "skipped_count": len(skipped),
        "coverage_pct": len(executed) / len(prepared) * 100 if prepared else None,
        "total_pnl_before_tax": before["total_pnl"],
        "total_cost": sum(to_float(r.get("_round_trip_cost"), 0) or 0 for r in executed),
        "total_tax": sum(to_float(r.get("_tax"), 0) or 0 for r in executed),
        "total_pnl_after_cost": after_cost["total_pnl"],
        "total_pnl_after_tax": after_tax["total_pnl"],
        "gross_profit_after_tax": after_tax["gross_profit"],
        "gross_loss_after_tax": after_tax["gross_loss"],
        "PF_after_tax": after_tax["profit_factor"],
        "avg_return_pct": after_tax["avg_return_pct"],
        "median_return_pct": after_tax["median_return_pct"],
        "win_rate": after_tax["win_rate"],
        "max_dd_after_tax": abs(max_dd),
        "max_dd_pct_of_capital": abs(max_dd) / capital * 100 if capital else None,
        "max_consecutive_losses": max_streak([to_float(r.get("_pnl_after_tax"), 0) or 0 for r in executed], True),
        "max_consecutive_wins": max_streak([to_float(r.get("_pnl_after_tax"), 0) or 0 for r in executed], False),
        "max_open_positions": max([int(r["open_positions"]) for r in curve], default=0),
        "max_cash_used": max([to_float(r.get("cash_used"), 0) or 0 for r in curve], default=0),
        "avg_cash_used": sum(to_float(r.get("cash_used"), 0) or 0 for r in curve) / len(curve) if curve else None,
        "capital_utilization_max": max([to_float(r.get("capital_utilization"), 0) or 0 for r in curve], default=0),
        "capital_utilization_avg": sum(to_float(r.get("capital_utilization"), 0) or 0 for r in curve) / len(curve) if curve else None,
        "skipped_avg_return": pnl_metrics(skipped, "_pnl_after_tax")["avg_return_pct"],
        "skipped_capital_avg_return": pnl_metrics(skipped_capital, "_pnl_after_tax")["avg_return_pct"],
        "skipped_max_winner": max([to_float(r.get("return_pct"), -999) or -999 for r in skipped], default=None),
        "right_tail_capture_rate": top_tail_capture(prepared, executed),
    }
    return {"summary": summary, "executed": executed, "skipped": skipped, "curve": curve}


def annualize(summary: dict, start: str, end: str) -> None:
    s = parse_date(start)
    e = parse_date(end)
    days = (e - s).days if s and e and e > s else None
    capital = to_float(summary.get("capital"))
    pnl = to_float(summary.get("total_pnl_after_tax"), 0) or 0
    if not days or not capital:
        summary["annualized_simple_return"] = None
        summary["annualized_compound_return"] = None
        return
    period_return = pnl / capital
    summary["annualized_simple_return"] = period_return / (days / 365.0) * 100
    summary["annualized_compound_return"] = ((1 + period_return) ** (365.0 / days) - 1) * 100 if period_return > -1 else None


def load_datasets(args: argparse.Namespace) -> tuple[list[dict], list[dict], list[str], Counter]:
    loader_args = argparse.Namespace(candidate_log=args.input, start_date="", end_date="")
    raw, skipped, sources = load_walk_forward_h5_full_rows(loader_args)
    all_dates = load_all_wf_dates(Path(args.input))
    date_by_signal = next_date_map(all_dates)
    rows = [standardize(r) for r in raw]
    for i, row in enumerate(rows):
        row["_row_index"] = i
        row["population"] = "walk_forward_h5_full"
    # Reuse the execution-timing cache so this analysis does not re-query
    # Supabase when the previous next-open audit has already populated it.
    timing_cache = ROOT / "outputs" / "h5_s_share_execution_timing" / "next_open_cache.json"
    cache_path = timing_cache if timing_cache.exists() else ROOT / args.output_dir / "next_open_cache.json"
    open_cache, open_stats = load_next_open_rows(rows, date_by_signal, cache_path)
    # Use a light Namespace because make_execution_rows only needs these fields.
    exec_args = argparse.Namespace(holding_days=args.holding_days, stop_pct=-12.0)
    close_rows, next_open_rows, exec_skipped = make_execution_rows(rows, open_cache, date_by_signal, exec_args)
    return close_rows, next_open_rows, sources, skipped + open_stats + exec_skipped


def timing_comparison(close_rows: list[dict], open_rows: list[dict]) -> list[dict]:
    out = []
    for label, rows in [("close_entry", close_rows), ("next_open_entry", open_rows)]:
        rec = {"entry_mode": label, "trades": len(rows)}
        for notional in (200_000, 300_000, 500_000):
            key = f"fractional_pnl_{notional//1000}k"
            m = pnl_metrics(rows, key)
            rec[f"s_share_{notional//1000}k_pnl"] = m["total_pnl"]
        m = pnl_metrics(rows, "fractional_pnl_300k")
        rec.update({
            "avg_return_pct": m["avg_return_pct"],
            "median_return_pct": m["median_return_pct"],
            "win_rate": m["win_rate"],
            "PF": m["profit_factor"],
        })
        out.append(rec)
    if len(out) == 2:
        out.append({
            "entry_mode": "delta_next_open_minus_close",
            "trades": None,
            "avg_return_pct": (out[1]["avg_return_pct"] or 0) - (out[0]["avg_return_pct"] or 0),
            "median_return_pct": (out[1]["median_return_pct"] or 0) - (out[0]["median_return_pct"] or 0),
            "win_rate": (out[1]["win_rate"] or 0) - (out[0]["win_rate"] or 0),
            "PF": None,
            "s_share_200k_pnl": (out[1]["s_share_200k_pnl"] or 0) - (out[0]["s_share_200k_pnl"] or 0),
            "s_share_300k_pnl": (out[1]["s_share_300k_pnl"] or 0) - (out[0]["s_share_300k_pnl"] or 0),
            "s_share_500k_pnl": (out[1]["s_share_500k_pnl"] or 0) - (out[0]["s_share_500k_pnl"] or 0),
        })
    return out


def gap_bucket_summary(rows: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[gap_bucket(row.get("entry_gap_pct"))].append(row)
    out = []
    for bucket, items in sorted(groups.items()):
        rec = {"gap_bucket": bucket}
        rec.update(pnl_metrics(items, "fractional_pnl_300k"))
        out.append(rec)
    return out


def monthly_summary(executed: list[dict], scenario: str) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in executed:
        d = str(row.get("entry_date") or "")[:7]
        groups[d].append(row)
    out = []
    for month, items in sorted(groups.items()):
        rec = {"month": month, "scenario_id": scenario}
        rec.update(pnl_metrics(items, "_pnl_after_tax"))
        out.append(rec)
    return out


def build_params(args: argparse.Namespace, *, notional: int, daily_cap: int | None, gap_limit: float | None, cost_bps: float, apply_tax: bool) -> dict:
    params = {
        "capital": float(args.capital),
        "notional": float(notional),
        "entry_mode": args.entry_mode,
        "daily_cap": daily_cap,
        "gap_limit": gap_limit,
        "tax_rate": float(args.tax_rate) if apply_tax else 0.0,
        "cost_bps": float(cost_bps),
        "apply_tax": apply_tax,
    }
    params["scenario_id"] = scenario_id(params)
    return params


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=DEFAULT_INPUT)
    p.add_argument("--capital", type=float, default=5_000_000)
    p.add_argument("--holding-days", type=int, default=3)
    p.add_argument("--notional-list", default="200000,300000")
    p.add_argument("--daily-cap-list", default="none,3,5,10")
    p.add_argument("--gap-limit-list", default="none,1,2,3,5")
    p.add_argument("--entry-mode", default="next_open")
    p.add_argument("--tax-rate", type=float, default=0.20315)
    p.add_argument("--round-trip-cost-bps-list", default="0,5,10,20,30")
    p.add_argument("--output-dir", default=DEFAULT_OUT)
    p.add_argument("--grid", default="true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outdir = ROOT / args.output_dir
    outdir.mkdir(parents=True, exist_ok=True)

    close_rows, next_open_rows, sources, skipped = load_datasets(args)
    target_rows = next_open_rows if args.entry_mode in {"next_open", "next_open_entry"} else close_rows
    start = min((str(r.get("entry_date") or "") for r in target_rows), default="")
    end = max((str(r.get("exit_date") or r.get("entry_date") or "") for r in target_rows), default="")

    write_csv(outdir / "01_normalized_dataset.csv", target_rows)
    write_csv(outdir / "02_entry_timing_comparison.csv", timing_comparison(close_rows, next_open_rows))
    write_csv(outdir / "16_gap_bucket_summary.csv", gap_bucket_summary(next_open_rows))
    write_csv(outdir / "19_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in skipped.most_common()])

    notional_list = parse_list(args.notional_list)
    daily_caps = parse_list(args.daily_cap_list)
    gap_limits = parse_list(args.gap_limit_list, as_float=True)
    cost_bps_list = parse_list(args.round_trip_cost_bps_list, as_float=True)

    main_scenarios = [
        (200_000, None, None, 0, False),
        (200_000, 10, None, 0, False),
        (200_000, 10, 3.0, 0, False),
        (200_000, 10, 3.0, 10, True),
        (300_000, None, None, 0, False),
        (300_000, 10, None, 0, False),
        (300_000, 10, 3.0, 0, False),
        (300_000, 10, 3.0, 10, True),
    ]

    results: dict[str, dict] = {}
    main_rows = []
    equity_rows = []
    executed_rows = []
    skipped_rows = []
    for notional, daily_cap, gap_limit, cost_bps, apply_tax in main_scenarios:
        params = build_params(args, notional=notional, daily_cap=daily_cap, gap_limit=gap_limit, cost_bps=cost_bps, apply_tax=apply_tax)
        sim = simulate_realistic(target_rows, params)
        annualize(sim["summary"], start, end)
        results[params["scenario_id"]] = sim
        main_rows.append(sim["summary"])
        for r in sim["curve"]:
            equity_rows.append(r)
        for r in sim["executed"]:
            executed_rows.append({"scenario_id": params["scenario_id"], **r})
        for r in sim["skipped"]:
            skipped_rows.append({"scenario_id": params["scenario_id"], **r})

    write_csv(outdir / "03_realistic_operation_summary.csv", main_rows)
    write_csv(outdir / "08_equity_curve.csv", equity_rows)
    write_csv(outdir / "10_executed_trades.csv", executed_rows)
    write_csv(outdir / "11_skipped_trades.csv", skipped_rows)

    grid_rows = []
    for notional in notional_list:
        for daily_cap in daily_caps:
            for gap_limit in gap_limits:
                for cost_bps in cost_bps_list:
                    for apply_tax in (False, True):
                        params = build_params(args, notional=notional, daily_cap=daily_cap, gap_limit=gap_limit, cost_bps=cost_bps, apply_tax=apply_tax)
                        sim = simulate_realistic(target_rows, params)
                        annualize(sim["summary"], start, end)
                        grid_rows.append(sim["summary"])
    write_csv(outdir / "13_grid_summary.csv", grid_rows)

    write_csv(outdir / "04_notional_comparison.csv", [
        r for r in grid_rows if r.get("notional_per_trade") in {100_000, 200_000, 300_000, 500_000}
    ])
    write_csv(outdir / "05_gap_filter_comparison.csv", [
        r for r in grid_rows if r.get("daily_cap") == 10 or r.get("daily_cap") is None
    ])
    write_csv(outdir / "06_daily_cap_comparison.csv", grid_rows)
    write_csv(outdir / "07_cost_tax_comparison.csv", grid_rows)

    drawdown_rows = []
    for row in main_rows:
        sid = row["scenario_id"]
        drawdown_rows.append({
            "scenario_id": sid,
            "max_dd_yen": row.get("max_dd_after_tax"),
            "max_dd_pct_of_capital": row.get("max_dd_pct_of_capital"),
            "max_consecutive_losses": row.get("max_consecutive_losses"),
            "max_consecutive_wins": row.get("max_consecutive_wins"),
        })
    write_csv(outdir / "09_drawdown_summary.csv", drawdown_rows)

    skipped_summary = []
    for sid, sim in results.items():
        for row in summarize_skips(sim["skipped"], sim["executed"]):
            skipped_summary.append({"scenario_id": sid, **row})
    write_csv(outdir / "12_skipped_vs_executed_summary.csv", skipped_summary)

    recommended_defs = [
        ("RULE_A", 200_000, 10, None, 10, True),
        ("RULE_B", 200_000, 10, 3.0, 10, True),
        ("RULE_C", 300_000, 10, None, 10, True),
        ("RULE_D", 300_000, 10, 3.0, 10, True),
        ("RULE_E", 200_000, 5, 3.0, 10, True),
        ("RULE_F", 300_000, 5, 3.0, 10, True),
    ]
    rec_rows = []
    for name, notional, daily_cap, gap_limit, cost_bps, apply_tax in recommended_defs:
        params = build_params(args, notional=notional, daily_cap=daily_cap, gap_limit=gap_limit, cost_bps=cost_bps, apply_tax=apply_tax)
        sim = simulate_realistic(target_rows, params)
        annualize(sim["summary"], start, end)
        rec_rows.append({"rule": name, **sim["summary"]})
    rec_rows.sort(key=lambda r: (
        -(to_float(r.get("PF_after_tax"), 0) or 0),
        to_float(r.get("max_dd_pct_of_capital"), 999) or 999,
        -(to_float(r.get("total_pnl_after_tax"), 0) or 0),
    ))
    for i, row in enumerate(rec_rows, 1):
        row["recommendation_rank"] = i
    write_csv(outdir / "14_recommended_rule_comparison.csv", rec_rows)

    tops = []
    for sid, sim in results.items():
        for label, rows, reverse in [
            ("winner", sim["executed"], True),
            ("loser", sim["executed"], False),
        ]:
            ordered = sorted(rows, key=lambda r: to_float(r.get("_pnl_after_tax"), 0) or 0, reverse=reverse)[:20]
            for i, row in enumerate(ordered, 1):
                tops.append({"scenario_id": sid, "side": label, "rank": i, **row})
    write_csv(outdir / "15_top_winners_losers.csv", tops)

    monthly_rows = []
    for sid, sim in results.items():
        monthly_rows.extend(monthly_summary(sim["executed"], sid))
    write_csv(outdir / "17_monthly_summary.csv", monthly_rows)

    capital_rows = [{
        "scenario_id": r["scenario_id"],
        "max_cash_used": r.get("max_cash_used"),
        "avg_cash_used": r.get("avg_cash_used"),
        "capital_utilization_max": r.get("capital_utilization_max"),
        "capital_utilization_avg": r.get("capital_utilization_avg"),
        "max_open_positions": r.get("max_open_positions"),
    } for r in main_rows]
    write_csv(outdir / "18_capital_usage_summary.csv", capital_rows)

    best = rec_rows[0] if rec_rows else {}
    timing = timing_comparison(close_rows, next_open_rows)
    report = f"""H5 S-share realistic operation analysis

Input:
{chr(10).join('- ' + s for s in sources) or '- none'}
H5_full rows: {len(close_rows)}
next_open rows: {len(next_open_rows)}
period: {start} to {end}

Entry timing:
close avg_return_pct: {timing[0].get('avg_return_pct') if timing else None}
next_open avg_return_pct: {timing[1].get('avg_return_pct') if len(timing) > 1 else None}
delta avg_return_pt: {timing[2].get('avg_return_pct') if len(timing) > 2 else None}

Best recommended rule:
rule: {best.get('rule')}
scenario_id: {best.get('scenario_id')}
total_pnl_after_tax: {best.get('total_pnl_after_tax')}
PF_after_tax: {best.get('PF_after_tax')}
max_dd_pct_of_capital: {best.get('max_dd_pct_of_capital')}
annualized_simple_return: {best.get('annualized_simple_return')}
coverage_pct: {best.get('coverage_pct')}
right_tail_capture_rate: {best.get('right_tail_capture_rate')}

Notes:
- Tax applies only to profitable trades after cost.
- round_trip_cost_bps approximates S-share spread/execution friction.
- Annualized values are walk-forward analysis estimates, not guarantees.
- This is analysis only. Primary, H5 rules, DB case definitions, UI, LINE,
  actual_trade_logs, and auto-trading were not changed.
"""
    write_text(outdir / "00_input_summary.txt", report)
    write_text(outdir / "20_report.txt", report)
    print(report.strip())


if __name__ == "__main__":
    main()
