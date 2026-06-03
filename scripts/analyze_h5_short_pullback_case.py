#!/usr/bin/env python3
"""Dedicated comparison for H5_short_pullback_drop5_m3.

Analysis only. This script does not modify Primary, H5 rules, DB case
definitions, UI, LINE, actual_trade_logs, Watchlist, Intraday H5, or any
auto-trading path.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import standardize, to_float, write_csv, write_text  # noqa: E402
from analyze_h5_pullback_relaxation import (  # noqa: E402
    TAX_RATE,
    common_pass,
    distribution,
    enrich_rows,
    fetch_feature_rows,
    latest_relaxed_candidates,
    market_environment_split,
    normalize_code,
    prefetch_common_pass,
    variant_pass,
)
from analyze_h5_s_share_execution_timing import (  # noqa: E402
    load_all_wf_dates,
    load_next_open_rows,
    make_execution_rows,
    next_date_map,
)
from analyze_h5_s_share_realistic_operation import annualize, pf, simulate_realistic  # noqa: E402


DEFAULT_INPUT = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
DEFAULT_OUT = "outputs/h5_short_pullback_case"


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_feature_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def num(value: Any, default: float | None = None) -> float | None:
    out = to_float(value)
    if out is None or math.isnan(out):
        return default
    return out


def key_of(row: dict[str, Any]) -> tuple[str, str]:
    signal_date = str(row.get("signal_date") or row.get("trade_date") or row.get("entry_date") or "")
    return signal_date, normalize_code(row.get("code"))


def sim_params(scenario_id: str, *, daily_cap: int | None = 10) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "capital": 5_000_000.0,
        "notional": 300_000.0,
        "daily_cap": daily_cap,
        "gap_limit": 3.0,
        "tax_rate": 0.0,
        "cost_bps": 10.0,
        "apply_tax": False,
        "entry_mode": "next_open",
    }


def aggregate_tax_summary(sim: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    s = dict(sim["summary"])
    after_cost = num(s.get("total_pnl_after_tax"), 0.0) or 0.0
    tax = max(after_cost, 0.0) * TAX_RATE
    s["aggregate_tax"] = tax
    s["total_pnl_after_aggregate_tax"] = after_cost - tax
    s["PF_after_cost"] = s.get("PF_after_tax")
    s["max_dd_after_cost"] = s.get("max_dd_after_tax")
    curve = sim.get("curve") or []
    daily = [num(r.get("daily_realized_pnl"), 0.0) or 0.0 for r in curve]
    s["sharpe_like_daily"] = (
        mean(daily) / pstdev(daily) * (252 ** 0.5)
        if len(daily) > 2 and pstdev(daily)
        else None
    )
    temp = dict(s)
    temp["total_pnl_after_tax"] = s["total_pnl_after_aggregate_tax"]
    annualize(temp, start, end)
    s["annualized_simple_return_aggregate_tax"] = temp.get("annualized_simple_return")
    s["annualized_compound_return_aggregate_tax"] = temp.get("annualized_compound_return")
    return s


def summary_of_rows(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    returns = [num(r.get("return_pct")) for r in rows if num(r.get("return_pct")) is not None]
    pnls = [num(r.get("fractional_pnl_300k"), 0.0) or 0.0 for r in rows]
    return {
        "group": label,
        "n": len(rows),
        "avg_return_pct": mean(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "win_rate": sum(v > 0 for v in returns) / len(returns) * 100 if returns else None,
        "PF": pf(pnls),
        "total_pnl_before_tax_cost_adjusted_input": sum(pnls),
        "big_win_ge5_rate": sum(v >= 5 for v in returns) / len(returns) * 100 if returns else None,
        "big_loss_le_minus5_rate": sum(v <= -5 for v in returns) / len(returns) * 100 if returns else None,
        "max_return_pct": max(returns) if returns else None,
        "min_return_pct": min(returns) if returns else None,
    }


def top_bottom(rows: list[dict[str, Any]], group: str, n: int = 20) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for side, ordered in [
        ("top", sorted(rows, key=lambda r: num(r.get("return_pct"), -999) or -999, reverse=True)[:n]),
        ("bottom", sorted(rows, key=lambda r: num(r.get("return_pct"), 999) or 999)[:n]),
    ]:
        for rank, row in enumerate(ordered, 1):
            out.append({
                "group": group,
                "side": side,
                "rank": rank,
                "code": row.get("code"),
                "name": row.get("name"),
                "signal_date": row.get("signal_date") or row.get("trade_date"),
                "entry_date": row.get("entry_date"),
                "return_pct": row.get("return_pct"),
                "signal_probability": row.get("signal_probability"),
                "drop5": row.get("drop_from_5d_high_pct"),
                "drop10": row.get("drop_from_10d_high_pct"),
                "drop20": row.get("drop_from_20d_high_pct"),
                "entry_gap_pct": row.get("entry_gap_pct"),
                "market_regime": row.get("market_regime"),
                "sector": row.get("sector"),
            })
    return out


def reorder_for_priority(rows: list[dict[str, Any]], method: str) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[str(row.get("entry_date") or "")].append(row)
    ordered: list[dict[str, Any]] = []
    seq = 0
    for day in sorted(by_day):
        items = by_day[day]
        if method == "current_priority":
            items = sorted(items, key=lambda r: (not bool(r.get("is_current_h5")), int(r.get("_source_row_index") or 0)))
        elif method == "short_priority":
            items = sorted(items, key=lambda r: (not bool(r.get("is_short_pullback")), int(r.get("_source_row_index") or 0)))
        elif method == "ai_score_desc":
            items = sorted(items, key=lambda r: (-(num(r.get("signal_probability"), -1) or -1), int(r.get("_source_row_index") or 0)))
        elif method == "gap_asc":
            items = sorted(items, key=lambda r: (num(r.get("entry_gap_pct"), 999) or 999, int(r.get("_source_row_index") or 0)))
        elif method == "overheat_asc":
            items = sorted(items, key=lambda r: (num(r.get("overheat_score"), 999) or 999, int(r.get("_source_row_index") or 0)))
        elif method == "drop5_deep":
            items = sorted(items, key=lambda r: (num(r.get("drop_from_5d_high_pct"), 999) or 999, int(r.get("_source_row_index") or 0)))
        elif method == "current7_short3":
            current = [r for r in items if r.get("is_current_h5")]
            short_only = [r for r in items if r.get("is_short_pullback") and not r.get("is_current_h5")]
            rest = [r for r in items if r not in current and r not in short_only]
            items = current[:7] + short_only[:3] + current[7:] + short_only[3:] + rest
        elif method == "current5_short5":
            current = [r for r in items if r.get("is_current_h5")]
            short_only = [r for r in items if r.get("is_short_pullback") and not r.get("is_current_h5")]
            rest = [r for r in items if r not in current and r not in short_only]
            items = current[:5] + short_only[:5] + current[5:] + short_only[5:] + rest
        else:
            items = sorted(items, key=lambda r: int(r.get("_source_row_index") or 0))
        for item in items:
            nr = dict(item)
            nr["_row_index"] = seq
            nr["priority_method"] = method
            seq += 1
            ordered.append(nr)
    return ordered


def run_case(rows: list[dict[str, Any]], scenario_id: str, start: str, end: str, *, daily_cap: int | None = 10) -> tuple[dict[str, Any], dict[str, Any]]:
    sim = simulate_realistic(rows, sim_params(scenario_id, daily_cap=daily_cap))
    summary = aggregate_tax_summary(sim, start, end)
    summary["scenario_id"] = scenario_id
    summary["active_days"] = len({r.get("entry_date") for r in sim.get("executed", [])})
    return summary, sim


def today_filter_counts(features: dict[str, dict[str, Any]], out_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    today_path = ROOT / "outputs" / "h5_tax_priority_today_audit" / "07_today_h5_evaluation_rows.csv"
    if not today_path.exists():
        return [], [{"filter": "today_audit_missing", "remaining": 0, "dropped": 0}]
    rows = read_csv(today_path)
    for row in rows:
        row["code"] = normalize_code(row.get("code"))
    # Ensure latest rows have drop5 if the cache does not already contain it.
    latest_features, _ = fetch_feature_rows(rows, out_dir, compute_drop10=True)
    merged_features = dict(features)
    merged_features.update(latest_features)
    rows = enrich_rows(rows, merged_features)
    stages = {"confirmed", "strong_confirmed"}
    steps = [
        ("all_predictions", lambda r: True),
        ("AI>=0.65", lambda r: (num(r.get("signal_probability") or r.get("score"), -1) or -1) >= 0.65),
        ("confirmed_stage", lambda r: str(r.get("signal_stage") or "") in stages),
        ("not_panic_selloff", lambda r: str(r.get("market_regime") or "") != "panic_selloff"),
        ("overheat<=1", lambda r: (num(r.get("overheat_score")) is not None and (num(r.get("overheat_score")) or 99) <= 1)),
        ("margin_3_30_if_present", lambda r: (num(r.get("margin_ratio")) is None or 3 <= (num(r.get("margin_ratio")) or 0) <= 30)),
        ("gap<=3_if_present", lambda r: (num(r.get("entry_gap_pct")) is None or (num(r.get("entry_gap_pct")) or 0) <= 3)),
        ("drop5<=-3", lambda r: (num(r.get("drop_from_5d_high_pct")) is not None and (num(r.get("drop_from_5d_high_pct")) or 999) <= -3)),
    ]
    current = list(rows)
    counts = []
    prev = len(current)
    for name, fn in steps:
        current = [r for r in current if fn(r)]
        counts.append({"filter": name, "remaining": len(current), "dropped": prev - len(current)})
        prev = len(current)
    candidates = []
    for r in current:
        candidates.append({
            "code": r.get("code"),
            "name": r.get("name"),
            "trade_date": r.get("trade_date"),
            "score": r.get("signal_probability") or r.get("score"),
            "signal_stage": r.get("signal_stage"),
            "drop5": r.get("drop_from_5d_high_pct"),
            "drop10": r.get("drop_from_10d_high_pct"),
            "drop20": r.get("drop_from_20d_high_pct"),
            "gap": r.get("entry_gap_pct"),
            "rsi14": r.get("rsi14"),
            "overheat_score": r.get("overheat_score"),
            "sector": r.get("sector"),
            "adoption_note": "short_pullback_candidate",
        })
    return candidates, counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUT)
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = read_csv(ROOT / args.input)
    rows = [standardize(r) for r in raw]
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["_row_index"] = i
        row["code"] = normalize_code(row.get("code"))
        row["score_source"] = row.get("source") or "walk_forward"

    prefetch_rows = [r for r in rows if prefetch_common_pass(r)]
    shared_cache = ROOT / "outputs" / "h5_pullback_relaxation" / "feature_cache.json"
    features = load_feature_cache(shared_cache)
    feature_stats = Counter({"shared_feature_cache_rows": len(features)})
    if not features:
        features, feature_stats = fetch_feature_rows(prefetch_rows, out_dir, compute_drop10=True)
    rows = enrich_rows(rows, features)
    for row in rows:
        row["is_current_h5"] = variant_pass(row, "drop20", -8.0)
        row["is_short_pullback"] = variant_pass(row, "drop5", -3.0)

    selected_raw = {
        "current_h5_drop20_m8": [r for r in rows if r.get("is_current_h5")],
        "H5_short_pullback_drop5_m3": [r for r in rows if r.get("is_short_pullback")],
        "combined_union": [r for r in rows if r.get("is_current_h5") or r.get("is_short_pullback")],
    }

    all_dates = load_all_wf_dates(ROOT / args.input)
    date_by_signal = next_date_map(all_dates)
    cache_path = ROOT / "outputs" / "h5_s_share_execution_timing" / "next_open_cache.json"
    open_cache, open_stats = load_next_open_rows(selected_raw["combined_union"], date_by_signal, cache_path)
    exec_args = argparse.Namespace(holding_days=3, stop_pct=-12.0)

    selected_exec: dict[str, list[dict[str, Any]]] = {}
    skipped_exec: Counter = Counter()
    for name, subset in selected_raw.items():
        _, next_rows, skipped = make_execution_rows(subset, open_cache, date_by_signal, exec_args)
        selected_exec[name] = next_rows
        skipped_exec += skipped

    start = min((str(r.get("entry_date") or r.get("trade_date") or "") for r in selected_exec["combined_union"] if r.get("entry_date") or r.get("trade_date")), default="")
    end = max((str(r.get("exit_date") or r.get("entry_date") or "") for r in selected_exec["combined_union"] if r.get("exit_date") or r.get("entry_date")), default="")

    summaries = []
    sims: dict[str, dict[str, Any]] = {}
    for name in ["current_h5_drop20_m8", "H5_short_pullback_drop5_m3", "combined_union"]:
        summary, sim = run_case(selected_exec[name], name, start, end)
        summaries.append(summary)
        sims[name] = sim

    current_keys = {key_of(r) for r in selected_exec["current_h5_drop20_m8"]}
    short_keys = {key_of(r) for r in selected_exec["H5_short_pullback_drop5_m3"]}
    overlap_groups = {
        "current_only": [r for r in selected_exec["current_h5_drop20_m8"] if key_of(r) not in short_keys],
        "short_only": [r for r in selected_exec["H5_short_pullback_drop5_m3"] if key_of(r) not in current_keys],
        "both": [r for r in selected_exec["combined_union"] if key_of(r) in current_keys and key_of(r) in short_keys],
    }
    overlap_summary = [summary_of_rows(v, k) for k, v in overlap_groups.items()]
    add_only_rows = overlap_groups["short_only"]
    add_only_summary = [summary_of_rows(add_only_rows, "short_pullback_only_not_current")]

    priority_methods = [
        "current_priority",
        "short_priority",
        "ai_score_desc",
        "gap_asc",
        "overheat_asc",
        "drop5_deep",
        "current7_short3",
        "current5_short5",
    ]
    priority_summaries = []
    priority_skips = []
    for method in priority_methods:
        ordered = reorder_for_priority(selected_exec["combined_union"], method)
        summary, sim = run_case(ordered, f"combined_{method}", start, end)
        skipped = sim.get("skipped", [])
        summary["priority_method"] = method
        summary["skipped_theoretical_pnl_after_cost"] = sum(num(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in skipped)
        summary["skipped_avg_return"] = mean([num(r.get("return_pct"), 0.0) or 0.0 for r in skipped]) if skipped else None
        summary["skipped_ge5_count"] = sum((num(r.get("return_pct"), -999) or -999) >= 5 for r in skipped)
        priority_summaries.append(summary)
        for r in skipped:
            priority_skips.append({"priority_method": method, **r})

    env_rows = market_environment_split({
        "current_h5_drop20_m8": selected_exec["current_h5_drop20_m8"],
        "H5_short_pullback_drop5_m3": selected_exec["H5_short_pullback_drop5_m3"],
        "combined_union": selected_exec["combined_union"],
    })
    attr_rows = []
    for name in ["current_h5_drop20_m8", "H5_short_pullback_drop5_m3"]:
        attr_rows.extend(distribution(selected_exec[name], name))

    latest_candidates, latest_counts = today_filter_counts(features, out_dir)

    top_rows = []
    for name, rows2 in selected_exec.items():
        top_rows.extend(top_bottom(rows2, name, 10))
    top_rows.extend(top_bottom(add_only_rows, "short_only_not_current", 20))

    write_text(out_dir / "00_input_summary.txt", f"""H5_short_pullback_drop5_m3 analysis
input: {ROOT / args.input}
rows_loaded: {len(raw)}
feature_enriched_rows: {len(features)}
feature_stats: {dict(feature_stats)}
open_stats: {dict(open_stats)}
execution_skips: {dict(skipped_exec)}
operation: next_open, HD3, capital 5M, S-share 300k, daily cap10, gap<=3, cost10bps, aggregate tax
""")
    write_csv(out_dir / "01_case_comparison.csv", summaries)
    write_csv(out_dir / "02_short_only_summary.csv", add_only_summary)
    write_csv(out_dir / "03_overlap_group_summary.csv", overlap_summary)
    write_csv(out_dir / "04_priority_method_comparison.csv", priority_summaries)
    write_csv(out_dir / "05_market_environment_split.csv", env_rows)
    write_csv(out_dir / "06_attribute_distribution.csv", attr_rows)
    write_csv(out_dir / "07_latest_short_pullback_candidates.csv", latest_candidates)
    write_csv(out_dir / "08_latest_filter_counts.csv", latest_counts)
    write_csv(out_dir / "09_top_bottom.csv", top_rows)
    write_csv(out_dir / "10_priority_skipped_rows.csv", priority_skips)
    write_csv(out_dir / "11_short_only_rows.csv", add_only_rows)
    write_csv(out_dir / "12_case_definition_proposal.csv", [{
        "case_key": "H5_short_pullback_drop5_m3",
        "score_source": "stored_predictions_or_walk_forward",
        "conditions": "AI>=0.65; stage confirmed/strong_confirmed; drop5<=-3; gap<=3; overheat<=1; market_regime!=panic_selloff; margin 3-30 if present",
        "entry": "next_open",
        "exit": "HD3_EST12",
        "capital_assumption": "5M",
        "notional_per_trade": "300k S-share",
        "daily_cap": 10,
        "primary_change": "no",
        "line_notification": "no",
        "auto_trade": "no",
        "recommended_next_step": "stored forward-test comparison case only",
    }])

    def fmt(v: Any, digits: int = 2) -> str:
        x = num(v)
        if x is None:
            return "n/a"
        return f"{x:,.{digits}f}"

    current = next((s for s in summaries if s["scenario_id"] == "current_h5_drop20_m8"), {})
    short = next((s for s in summaries if s["scenario_id"] == "H5_short_pullback_drop5_m3"), {})
    combined = next((s for s in summaries if s["scenario_id"] == "combined_union"), {})
    best_priority = max(priority_summaries, key=lambda s: num(s.get("total_pnl_after_aggregate_tax"), -10**18) or -10**18)
    report = f"""H5_short_pullback_drop5_m3 report

Case comparison:
- current H5: rows={current.get('executed_count')}, PnL={fmt(current.get('total_pnl_after_aggregate_tax'), 0)}円, PF={fmt(current.get('PF_after_cost'), 3)}, DD={fmt(current.get('max_dd_after_cost'), 0)}円
- short pullback: rows={short.get('executed_count')}, PnL={fmt(short.get('total_pnl_after_aggregate_tax'), 0)}円, PF={fmt(short.get('PF_after_cost'), 3)}, DD={fmt(short.get('max_dd_after_cost'), 0)}円
- combined union: rows={combined.get('executed_count')}, PnL={fmt(combined.get('total_pnl_after_aggregate_tax'), 0)}円, PF={fmt(combined.get('PF_after_cost'), 3)}, DD={fmt(combined.get('max_dd_after_cost'), 0)}円

Short-only additional rows:
- n={add_only_summary[0].get('n')}
- avg={fmt(add_only_summary[0].get('avg_return_pct'))}%
- PF={fmt(add_only_summary[0].get('PF'), 3)}
- big win >=5%={fmt(add_only_summary[0].get('big_win_ge5_rate'))}%
- big loss <=-5%={fmt(add_only_summary[0].get('big_loss_le_minus5_rate'))}%

Best daily-cap priority method:
- method={best_priority.get('priority_method')}
- PnL={fmt(best_priority.get('total_pnl_after_aggregate_tax'), 0)}円
- PF={fmt(best_priority.get('PF_after_cost'), 3)}
- DD={fmt(best_priority.get('max_dd_after_cost'), 0)}円
- skipped theoretical after-cost PnL={fmt(best_priority.get('skipped_theoretical_pnl_after_cost'), 0)}円

Latest stored day:
- short_pullback candidates={len(latest_candidates)}
- see 08_latest_filter_counts.csv when zero.

Interpretation:
- short_pullback is not merely a looser drop20 copy; the short-only group is reported separately in 02 and 11.
- If combined priority degrades, use it as a separate comparison case rather than merging into Primary.
- No Primary, UI, LINE, actual_trade_logs, or auto-trading logic was changed.
"""
    write_text(out_dir / "13_report.txt", report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
