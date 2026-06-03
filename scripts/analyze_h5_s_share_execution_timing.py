#!/usr/bin/env python3
"""S-share execution timing analysis for walk-forward H5_full.

Compares signal-close entry with next-session-open S-share entry under the
same capital-constrained framework. Analysis only; no production state is
updated.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import pickle
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_capital_constrained_simulation import simulate  # noqa: E402
from analyze_h5_primary_fractional_sizing import (  # noqa: E402
    add_sizing_columns,
    load_walk_forward_h5_full_rows,
    standardize,
    to_float,
    write_csv,
    write_text,
)


DEFAULT_OUT = "outputs/h5_s_share_execution_timing"
DEFAULT_WF = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
LEGACY_OPEN_CACHE = ROOT / "outputs" / "rebound_next_analysis" / "h5_entry_lag" / "_next_open_cache.pkl"


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


def metrics(rows: list[dict], pnl_key: str = "fractional_pnl_300k", ret_key: str = "return_pct") -> dict:
    pnls = [to_float(r.get(pnl_key), 0) or 0 for r in rows]
    rets = [to_float(r.get(ret_key), 0) or 0 for r in rows]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r < 0]
    return {
        "count": len(rows),
        "avg_return_pct": sum(rets) / len(rets) if rets else None,
        "median_return_pct": median(rets) if rets else None,
        "win_rate": len(wins) / len(rets) * 100 if rets else None,
        "total_pnl": sum(pnls),
        "profit_factor": pf(pnls),
        "gross_profit": sum(v for v in pnls if v > 0),
        "gross_loss": -sum(v for v in pnls if v < 0),
        "max_win": max(pnls) if pnls else None,
        "max_loss": min(pnls) if pnls else None,
        "est12_count": sum(1 for r in rows if str(r.get("exit_reason") or "") == "emergency_stop"),
        "est12_rate": (
            sum(1 for r in rows if str(r.get("exit_reason") or "") == "emergency_stop") / len(rows) * 100
            if rows else None
        ),
    }


def load_all_wf_dates(path: Path) -> list[str]:
    rows = read_csv(path)
    return sorted({str(r.get("trade_date") or "") for r in rows if r.get("trade_date")})


def next_date_map(dates: list[str]) -> dict[str, str]:
    return {dates[i]: dates[i + 1] for i in range(len(dates) - 1)}


def load_pickle_cache(path: Path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            loaded = pickle.load(f)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def load_json_cache(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_cache(path: Path, rows: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def cache_key(trade_date: str, code: str) -> str:
    return f"{trade_date}|{code}"


def build_supabase():
    from services.trade_case_tester import _build_supabase

    return _build_supabase()


def load_next_open_rows(selected: list[dict], date_by_signal: dict[str, str], cache_path: Path) -> tuple[dict[str, dict], Counter]:
    stats: Counter = Counter()
    out: dict[str, dict] = {}

    legacy = load_pickle_cache(LEGACY_OPEN_CACHE)
    json_cache = load_json_cache(cache_path)

    needed: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        signal_date = str(row.get("trade_date") or row.get("entry_date") or "")
        next_date = date_by_signal.get(signal_date)
        code = str(row.get("code") or "")
        if not next_date or not code:
            stats["missing_next_trade_date"] += 1
            continue
        key = cache_key(next_date, code)
        if key in json_cache:
            out[key] = json_cache[key]
            stats["json_cache_hit"] += 1
            continue
        legacy_row = legacy.get((next_date, code))
        if legacy_row:
            out[key] = dict(legacy_row)
            stats["legacy_cache_hit"] += 1
            continue
        needed[next_date].add(code)

    if needed:
        sb = build_supabase()
        for trade_date, codes in sorted(needed.items()):
            code_list = sorted(codes)
            for i in range(0, len(code_list), 60):
                chunk = code_list[i:i + 60]
                rows = (
                    sb.table("stock_feature_snapshots")
                    .select("trade_date,code,open,high,low,close")
                    .eq("trade_date", trade_date)
                    .in_("code", chunk)
                    .execute()
                    .data
                    or []
                )
                found = {str(r.get("code") or ""): r for r in rows}
                for code in chunk:
                    key = cache_key(trade_date, code)
                    row = found.get(code) or {}
                    out[key] = row
                    json_cache[key] = row
                    stats["db_loaded" if row else "db_missing"] += 1
        save_json_cache(cache_path, json_cache)

    return out, stats


def est12_or_close_return(row: dict, entry_price: float, *, first_day: int, holding_days: int, stop_pct: float) -> tuple[float | None, float | None, str]:
    if entry_price <= 0:
        return None, None, "invalid_entry_price"
    stop_price = entry_price * (1 + stop_pct / 100.0)
    last_day = first_day + holding_days - 1
    for day in range(first_day, last_day + 1):
        low = to_float(row.get(f"future_low_{day}d"))
        if low is not None and low <= stop_price:
            return stop_pct, entry_price * (1 + stop_pct / 100.0), "emergency_stop"
    exit_price = to_float(row.get(f"future_close_{last_day}d"))
    if exit_price is None:
        return None, None, "missing_exit_price"
    return (exit_price / entry_price - 1) * 100.0, exit_price, "time_stop"


def make_execution_rows(rows: list[dict], open_rows: dict[str, dict], date_by_signal: dict[str, str], args: argparse.Namespace) -> tuple[list[dict], list[dict], Counter]:
    close_rows: list[dict] = []
    open_rows_out: list[dict] = []
    skipped: Counter = Counter()

    for source in rows:
        signal_date = str(source.get("trade_date") or source.get("entry_date") or "")
        next_date = date_by_signal.get(signal_date)
        code = str(source.get("code") or "")
        signal_close = to_float(source.get("entry_price"))
        if not signal_close or signal_close <= 0:
            skipped["missing_signal_close"] += 1
            continue

        close_row = dict(source)
        close_row["execution_model"] = "signal_close"
        close_row["signal_close"] = signal_close
        close_row["entry_date"] = signal_date
        close_row["entry_price"] = signal_close
        close_ret, close_exit, close_reason = est12_or_close_return(
            close_row, signal_close, first_day=1, holding_days=args.holding_days, stop_pct=args.stop_pct
        )
        if close_ret is None or close_exit is None:
            skipped[f"close_{close_reason}"] += 1
        else:
            close_row["return_pct"] = close_ret
            close_row["exit_price"] = close_exit
            close_row["exit_reason"] = close_reason
            close_row["entry_gap_pct"] = 0.0
            close_rows.append(close_row)

        key = cache_key(next_date or "", code)
        next_snap = open_rows.get(key) or {}
        next_open = to_float(next_snap.get("open"))
        if not next_date:
            skipped["missing_next_trade_date"] += 1
            continue
        if not next_open or next_open <= 0:
            skipped["missing_next_open"] += 1
            continue
        gap_pct = (next_open / signal_close - 1.0) * 100.0

        open_row = dict(source)
        open_row["execution_model"] = "next_open"
        open_row["signal_close"] = signal_close
        open_row["next_trade_date"] = next_date
        open_row["entry_date"] = next_date
        open_row["trade_date"] = next_date
        open_row["signal_date"] = signal_date
        open_row["entry_price"] = next_open
        open_row["entry_gap_pct"] = gap_pct
        open_row["next_open"] = next_open
        ret, exit_price, reason = est12_or_close_return(
            open_row, next_open, first_day=1, holding_days=args.holding_days, stop_pct=args.stop_pct
        )
        if ret is None or exit_price is None:
            skipped[f"open_{reason}"] += 1
            continue
        open_row["return_pct"] = ret
        open_row["exit_price"] = exit_price
        open_row["exit_reason"] = reason
        open_rows_out.append(open_row)

    add_sizing_columns(close_rows)
    add_sizing_columns(open_rows_out)
    return close_rows, open_rows_out, skipped


def gap_bucket(value: Any) -> str:
    v = to_float(value)
    if v is None:
        return "unknown"
    if v <= -3:
        return "le_-3"
    if v <= -1:
        return "-3_-1"
    if v <= 0:
        return "-1_0"
    if v <= 1:
        return "0_1"
    if v <= 2:
        return "1_2"
    if v <= 3:
        return "2_3"
    return "gt_3"


def grouped(rows: list[dict], key_name: str, key_fn) -> list[dict]:
    out = []
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    for key, items in sorted(groups.items()):
        rec = {key_name: key}
        rec.update(metrics(items))
        out.append(rec)
    return out


def gap_limit_summary(rows: list[dict], base_params: dict) -> list[dict]:
    out = []
    for limit in (None, 0.0, 1.0, 2.0, 3.0, 5.0):
        if limit is None:
            subset = list(rows)
            label = "no_gap_limit"
        else:
            subset = [r for r in rows if (to_float(r.get("entry_gap_pct"), 999) or 999) <= limit]
            label = f"gap_lte_{str(limit).replace('.', '_')}"
        m = metrics(subset)
        sim = simulate(subset, base_params)["summary"] if subset else {}
        out.append({
            "gap_policy": label,
            "gap_limit_pct": limit,
            "candidate_count": len(subset),
            "candidate_coverage_pct": len(subset) / len(rows) * 100 if rows else None,
            "avg_return_pct": m.get("avg_return_pct"),
            "median_return_pct": m.get("median_return_pct"),
            "win_rate": m.get("win_rate"),
            "s_share_300k_total_pnl": m.get("total_pnl"),
            "pf_all_candidates": m.get("profit_factor"),
            "capital_executed_count": sim.get("executed_count"),
            "capital_coverage_pct": sim.get("coverage_pct"),
            "capital_total_pnl": sim.get("total_pnl"),
            "capital_pf": sim.get("profit_factor"),
            "capital_max_dd_yen": sim.get("max_dd_yen"),
            "capital_right_tail_capture_rate": sim.get("right_tail_capture_rate"),
        })
    return out


def execution_summary(close_rows: list[dict], open_rows: list[dict]) -> list[dict]:
    rows = []
    close_m = metrics(close_rows)
    open_m = metrics(open_rows)
    rows.append({"execution_model": "signal_close", **close_m})
    rows.append({"execution_model": "next_open", **open_m})
    rows.append({
        "execution_model": "delta_next_open_minus_close",
        "count": None,
        "avg_return_pct": (open_m.get("avg_return_pct") or 0) - (close_m.get("avg_return_pct") or 0),
        "median_return_pct": (open_m.get("median_return_pct") or 0) - (close_m.get("median_return_pct") or 0),
        "win_rate": (open_m.get("win_rate") or 0) - (close_m.get("win_rate") or 0),
        "total_pnl": (open_m.get("total_pnl") or 0) - (close_m.get("total_pnl") or 0),
        "profit_factor": None,
        "gross_profit": None,
        "gross_loss": None,
        "max_win": None,
        "max_loss": None,
        "est12_count": None,
        "est12_rate": None,
    })
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=DEFAULT_WF)
    p.add_argument("--output-dir", default=DEFAULT_OUT)
    p.add_argument("--capital", type=float, default=5_000_000)
    p.add_argument("--notional-per-trade", type=float, default=300_000)
    p.add_argument("--holding-days", type=int, default=3)
    p.add_argument("--selection-method", default="first")
    p.add_argument("--stop-pct", type=float, default=-12.0)
    p.add_argument("--cache", default=str(ROOT / DEFAULT_OUT / "next_open_cache.json"))
    p.add_argument("--grid", default="false")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outdir = ROOT / args.output_dir
    outdir.mkdir(parents=True, exist_ok=True)

    loader_args = argparse.Namespace(candidate_log=args.input, start_date="", end_date="")
    raw_rows, filter_skipped, sources = load_walk_forward_h5_full_rows(loader_args)
    all_dates = load_all_wf_dates(Path(args.input))
    date_by_signal = next_date_map(all_dates)
    standardized = [standardize(r) for r in raw_rows]
    for row in standardized:
        row["population"] = "walk_forward_h5_full"
        row["_input_source"] = sources[0] if sources else args.input
    for i, row in enumerate(standardized):
        row["_row_index"] = i

    open_cache, open_stats = load_next_open_rows(standardized, date_by_signal, ROOT / args.cache)
    close_rows, next_open_rows, exec_skipped = make_execution_rows(standardized, open_cache, date_by_signal, args)

    write_csv(outdir / "01_signal_close_dataset.csv", close_rows)
    write_csv(outdir / "02_next_open_dataset.csv", next_open_rows)
    write_csv(outdir / "03_execution_timing_summary.csv", execution_summary(close_rows, next_open_rows))
    write_csv(outdir / "04_entry_gap_summary.csv", grouped(next_open_rows, "entry_gap_bucket", lambda r: gap_bucket(r.get("entry_gap_pct"))))

    base_params = {
        "capital": args.capital,
        "notional_per_trade": args.notional_per_trade,
        "holding_days": args.holding_days,
        "selection_method": args.selection_method,
        "cash_release": "same_day",
        "daily_entry_cap": None,
        "max_open_positions": None,
        "cluster_threshold": None,
        "cluster_daily_cap": None,
        "seed": 0,
    }
    close_sim = simulate(close_rows, base_params)
    open_sim = simulate(next_open_rows, base_params)
    sim_rows = [
        {"execution_model": "signal_close", **close_sim["summary"]},
        {"execution_model": "next_open", **open_sim["summary"]},
    ]
    write_csv(outdir / "05_capital_constrained_summary.csv", sim_rows)
    write_csv(outdir / "06_next_open_executed_trades.csv", open_sim["executed"])
    write_csv(outdir / "07_next_open_skipped_trades.csv", open_sim["skipped"])
    write_csv(outdir / "08_next_open_equity_curve.csv", open_sim["curve"])

    grid_rows = []
    if str(args.grid).lower() in {"1", "true", "yes"}:
        for capital in (3_000_000, 5_000_000, 10_000_000):
            for notional in (200_000, 300_000, 500_000):
                for method in ("first", "ai_score_desc", "unit_amount_asc"):
                    params = dict(base_params, capital=capital, notional_per_trade=notional, selection_method=method)
                    grid_rows.append({"execution_model": "next_open", **simulate(next_open_rows, params)["summary"]})
    write_csv(outdir / "09_next_open_grid_summary.csv", grid_rows)

    write_csv(outdir / "10_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in (filter_skipped + exec_skipped + open_stats).most_common()])
    write_csv(outdir / "12_gap_limit_policy_summary.csv", gap_limit_summary(next_open_rows, base_params))

    close_m = metrics(close_rows)
    open_m = metrics(next_open_rows)
    close_s = close_sim["summary"]
    open_s = open_sim["summary"]
    report = f"""H5 S-share execution timing analysis

Input:
{chr(10).join('- ' + s for s in sources) or '- none'}
walk_forward_h5_full_rows: {len(standardized)}
signal_close_evaluable: {len(close_rows)}
next_open_evaluable: {len(next_open_rows)}

Entry timing, all H5_full:
signal_close avg_return_pct: {close_m.get('avg_return_pct')}
next_open avg_return_pct: {open_m.get('avg_return_pct')}
delta next_open - close: {(open_m.get('avg_return_pct') or 0) - (close_m.get('avg_return_pct') or 0)}
signal_close S-share 300k PnL: {close_m.get('total_pnl')}
next_open S-share 300k PnL: {open_m.get('total_pnl')}
delta PnL: {(open_m.get('total_pnl') or 0) - (close_m.get('total_pnl') or 0)}
next_open avg_gap_pct: {sum((to_float(r.get('entry_gap_pct'), 0) or 0) for r in next_open_rows) / len(next_open_rows) if next_open_rows else None}

Capital constrained:
capital: {args.capital:,.0f}
notional_per_trade: {args.notional_per_trade:,.0f}
selection_method: {args.selection_method}
signal_close executed: {close_s.get('executed_count')} total_pnl: {close_s.get('total_pnl')} PF: {close_s.get('profit_factor')} maxDD: {close_s.get('max_dd_yen')}
next_open executed: {open_s.get('executed_count')} total_pnl: {open_s.get('total_pnl')} PF: {open_s.get('profit_factor')} maxDD: {open_s.get('max_dd_yen')}
delta constrained PnL: {(open_s.get('total_pnl') or 0) - (close_s.get('total_pnl') or 0)}

Interpretation:
This compares the research close-entry assumption with practical S-share next-open entry.
The next-open signal_probability is not recomputed and model_predictions are not overwritten.
Primary, H5 rules, DB case definitions, UI, LINE, actual_trade_logs, and auto-trading were not changed.
"""
    write_text(outdir / "00_input_summary.txt", report)
    write_text(outdir / "11_execution_timing_report.txt", report)
    print(report.strip())


if __name__ == "__main__":
    main()
