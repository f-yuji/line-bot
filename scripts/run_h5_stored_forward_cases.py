#!/usr/bin/env python3
"""Build research-only stored forward-test comparison cases for H5.

This script writes comparison reports only. It does not modify Primary/H5
production rules, LINE notification logic, actual_trade_logs, or auto trading.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import standardize, to_float, write_csv, write_text  # noqa: E402
from analyze_h5_pullback_relaxation import (  # noqa: E402
    TAX_RATE,
    cache_key,
    common_pass,
    enrich_rows,
    fetch_feature_rows,
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
from services.h5_primary import h5_overheat_score  # noqa: E402


DEFAULT_INPUT = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
DEFAULT_OUTPUT = "outputs/h5_stored_forward_cases"
LATEST_STORED = ROOT / "outputs/h5_stored_forward_test/latest_h5_candidates.csv"
TODAY_AUDIT = ROOT / "outputs/h5_tax_priority_today_audit/07_today_h5_evaluation_rows.csv"
SHARED_FEATURE_CACHE = ROOT / "outputs/h5_pullback_relaxation/feature_cache.json"

CURRENT_CASE = "current_h5"
SHORT_CASE = "H5_short_pullback_drop5_m3"
MIX_CASE = "H5_current7_short3"
WATCH_CASE = "H5_overheat_reject_watch"

CASE_DAILY_COLUMNS = [
    "signal_date", "case_key", "code", "name", "score", "signal_stage",
    "drop5", "drop10", "drop20", "gap", "overheat_score",
    "entry_date", "entry_price", "exit_date", "exit_price", "return_pct",
    "pnl_after_cost", "cumulative_pnl", "採用理由", "除外理由",
]
WATCH_COLUMNS = [
    "signal_date", "code", "name", "score", "signal_stage", "drop5", "drop10",
    "drop20", "RSI14", "ma5_gap_pct", "return_5d_pct", "volume_ratio_20d",
    "overheat_score", "除外理由", "watch_case",
]


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_csv_with_headers(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        extra = []
        seen = set(headers)
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    extra.append(key)
        headers = headers + extra
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def num(value: Any, default: float | None = None) -> float | None:
    out = to_float(value)
    if out is None or math.isnan(out):
        return default
    return out


def score_of(row: dict[str, Any]) -> float | None:
    return num(row.get("signal_probability") or row.get("score"))


def stage_ok(row: dict[str, Any]) -> bool:
    return str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}


def margin_ok(row: dict[str, Any]) -> bool:
    margin = num(row.get("margin_ratio"))
    return margin is None or 3.0 <= margin <= 30.0


def gap_ok(row: dict[str, Any]) -> bool:
    gap = num(row.get("entry_gap_pct"))
    return gap is None or gap <= 3.0


def overheat_value(row: dict[str, Any]) -> int | None:
    value = num(row.get("overheat_score"))
    if value is not None:
        return int(value)
    try:
        return int(h5_overheat_score(row))
    except Exception:
        return None


def short_pass(row: dict[str, Any]) -> bool:
    if not common_pass(row):
        return False
    drop5 = num(row.get("drop_from_5d_high_pct"))
    return drop5 is not None and drop5 <= -3.0


def current_pass(row: dict[str, Any]) -> bool:
    return variant_pass(row, "drop20", -8.0)


def case_key(row: dict[str, Any]) -> tuple[str, str]:
    signal_date = str(row.get("signal_date") or row.get("trade_date") or row.get("entry_date") or "")
    return signal_date, normalize_code(row.get("code"))


def load_rows(input_path: Path, out_dir: Path) -> tuple[list[dict[str, Any]], Counter]:
    raw = read_csv(input_path)
    rows = [standardize(r) for r in raw]
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["_row_index"] = i
        row["code"] = normalize_code(row.get("code"))
        row["score_source"] = row.get("source") or "walk_forward"

    prefetch_rows = [r for r in rows if prefetch_common_pass(r)]
    features = read_json(SHARED_FEATURE_CACHE)
    stats = Counter({"shared_feature_cache_rows": len(features)})
    if not features:
        features, stats = fetch_feature_rows(prefetch_rows, out_dir, compute_drop10=True)
    rows = enrich_rows(rows, features)
    for row in rows:
        row["overheat_score"] = overheat_value(row)
        row["is_current_h5"] = current_pass(row)
        row["is_short_pullback"] = short_pass(row)
    return rows, stats


def make_exec(rows: list[dict[str, Any]], input_path: Path) -> tuple[list[dict[str, Any]], Counter]:
    all_dates = load_all_wf_dates(input_path)
    date_by_signal = next_date_map(all_dates)
    cache_path = ROOT / "outputs/h5_s_share_execution_timing/next_open_cache.json"
    open_cache, open_stats = load_next_open_rows(rows, date_by_signal, cache_path)
    exec_args = argparse.Namespace(holding_days=3, stop_pct=-12.0)
    _, next_rows, skipped = make_execution_rows(rows, open_cache, date_by_signal, exec_args)
    return next_rows, Counter(open_stats) + Counter(skipped)


def mixed_current7_short3(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[str(row.get("entry_date") or "")].append(row)

    selected: list[dict[str, Any]] = []
    for day in sorted(by_day):
        items = sorted(by_day[day], key=lambda r: int(num(r.get("_source_row_index"), 0) or 0))
        day_selected: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for row in [r for r in items if r.get("is_current_h5")]:
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["adoption_reason"] = "current_h5 slot"
            day_selected.append(nr)
            seen.add(key)
            if len(day_selected) >= 7:
                break

        short_count = 0
        for row in [r for r in items if r.get("is_short_pullback")]:
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["adoption_reason"] = "short_pullback support slot"
            day_selected.append(nr)
            seen.add(key)
            short_count += 1
            if short_count >= 3:
                break

        selected.extend(day_selected[:10])
    return selected


def sim_params(scenario_id: str) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "capital": 5_000_000.0,
        "notional": 300_000.0,
        "daily_cap": 10,
        "gap_limit": 3.0,
        "tax_rate": 0.0,
        "cost_bps": 10.0,
        "apply_tax": False,
        "entry_mode": "next_open",
    }


def summarize_sim(case: str, sim: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    s = dict(sim["summary"])
    after_cost = num(s.get("total_pnl_after_tax"), 0.0) or 0.0
    aggregate_tax = max(after_cost, 0.0) * TAX_RATE
    temp = dict(s)
    temp["total_pnl_after_tax"] = after_cost - aggregate_tax
    annualize(temp, start, end)
    s.update({
        "case_key": case,
        "count": s.get("executed_count"),
        "active_days": len({r.get("entry_date") for r in sim.get("executed", [])}),
        "PF": s.get("PF_after_tax"),
        "pretax_pnl": s.get("total_pnl_before_tax"),
        "pnl_after_cost": after_cost,
        "aggregate_tax": aggregate_tax,
        "pnl_after_aggregate_tax": after_cost - aggregate_tax,
        "CAGR": temp.get("annualized_compound_return"),
        "max_dd": s.get("max_dd_after_tax"),
        "max_loss_streak": s.get("max_consecutive_losses"),
    })
    return s


def run_case(case: str, rows: list[dict[str, Any]], start: str, end: str) -> tuple[dict[str, Any], dict[str, Any]]:
    sim = simulate_realistic(rows, sim_params(case))
    return summarize_sim(case, sim, start, end), sim


def output_case_rows(case: str, sim: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cumulative = 0.0
    rows = [dict(r) for r in sim.get("executed", [])]
    rows.sort(key=lambda r: (str(r.get("exit_date") or ""), str(r.get("entry_date") or ""), int(num(r.get("_source_row_index"), 0) or 0)))
    for row in rows:
        pnl = num(row.get("_pnl_after_cost"), 0.0) or 0.0
        cumulative += pnl
        out.append({
            "signal_date": row.get("signal_date") or row.get("trade_date"),
            "case_key": case,
            "code": row.get("code"),
            "name": row.get("name"),
            "score": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "drop5": row.get("drop_from_5d_high_pct"),
            "drop10": row.get("drop_from_10d_high_pct"),
            "drop20": row.get("drop_from_20d_high_pct"),
            "gap": row.get("entry_gap_pct"),
            "overheat_score": row.get("overheat_score"),
            "entry_date": row.get("entry_date"),
            "entry_price": row.get("entry_price"),
            "exit_date": row.get("exit_date"),
            "exit_price": row.get("exit_price"),
            "return_pct": row.get("return_pct"),
            "pnl_after_cost": pnl,
            "cumulative_pnl": cumulative,
            "採用理由": row.get("adoption_reason") or case,
            "除外理由": "",
        })
    for row in sim.get("skipped", []):
        out.append({
            "signal_date": row.get("signal_date") or row.get("trade_date"),
            "case_key": case,
            "code": row.get("code"),
            "name": row.get("name"),
            "score": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "drop5": row.get("drop_from_5d_high_pct"),
            "drop10": row.get("drop_from_10d_high_pct"),
            "drop20": row.get("drop_from_20d_high_pct"),
            "gap": row.get("entry_gap_pct"),
            "overheat_score": row.get("overheat_score"),
            "entry_date": row.get("entry_date"),
            "entry_price": row.get("entry_price"),
            "exit_date": row.get("exit_date"),
            "exit_price": row.get("exit_price"),
            "return_pct": row.get("return_pct"),
            "pnl_after_cost": row.get("_pnl_after_cost"),
            "cumulative_pnl": "",
            "採用理由": "",
            "除外理由": row.get("_entry_status") or row.get("_realistic_skip_reason") or "not_executed",
        })
    return out


def top_bottom_rows(case: str, rows: list[dict[str, Any]], n: int = 10) -> list[dict[str, Any]]:
    ordered_top = sorted(rows, key=lambda r: num(r.get("return_pct"), -999) or -999, reverse=True)[:n]
    ordered_bottom = sorted(rows, key=lambda r: num(r.get("return_pct"), 999) or 999)[:n]
    out = []
    for side, items in [("top", ordered_top), ("bottom", ordered_bottom)]:
        for rank, row in enumerate(items, 1):
            out.append({
                "case_key": case,
                "side": side,
                "rank": rank,
                "signal_date": row.get("signal_date") or row.get("trade_date"),
                "code": row.get("code"),
                "name": row.get("name"),
                "return_pct": row.get("return_pct"),
                "pnl_after_cost": row.get("_pnl_after_cost"),
                "score": row.get("signal_probability"),
                "drop5": row.get("drop_from_5d_high_pct"),
                "drop20": row.get("drop_from_20d_high_pct"),
            })
    return out


def latest_source_rows() -> list[dict[str, Any]]:
    audit_rows = [standardize(r) for r in read_csv(TODAY_AUDIT)]
    stored_rows = [standardize(r) for r in read_csv(LATEST_STORED)]
    audit_date = max((str(r.get("trade_date") or "")[:10] for r in audit_rows), default="")
    stored_date = max((str(r.get("trade_date") or "")[:10] for r in stored_rows), default="")
    if stored_rows and stored_date >= audit_date:
        return stored_rows
    return audit_rows


def latest_enriched_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows = latest_source_rows()
    if not rows:
        return []
    for row in rows:
        row["code"] = normalize_code(row.get("code"))
        row["_source_row_index"] = int(num(row.get("_source_row_index"), 0) or 0)
    features = read_json(SHARED_FEATURE_CACHE)
    rows = enrich_rows(rows, features)
    missing_drop5 = [r for r in rows if r.get("trade_date") and r.get("code") and r.get("drop_from_5d_high_pct") in (None, "")]
    if missing_drop5:
        fetched, _ = fetch_feature_rows(rows, out_dir, compute_drop10=True)
        merged = dict(features)
        merged.update(fetched)
        rows = enrich_rows(rows, merged)
    for row in rows:
        row["overheat_score"] = overheat_value(row)
        row["is_current_h5"] = current_pass(row)
        row["is_short_pullback"] = short_pass(row)
    return rows


def latest_filter_counts(rows: list[dict[str, Any]], case: str) -> list[dict[str, Any]]:
    total = len(rows)
    filters = [
        ("AI>=0.65", lambda r: (score_of(r) or -1) >= 0.65),
        ("confirmed系通過", stage_ok),
        ("drop20<=-8 通過", lambda r: (num(r.get("drop_from_20d_high_pct")) is not None and (num(r.get("drop_from_20d_high_pct")) or 999) <= -8)),
        ("drop5<=-3 通過", lambda r: (num(r.get("drop_from_5d_high_pct")) is not None and (num(r.get("drop_from_5d_high_pct")) or 999) <= -3)),
        ("overheat<=1 通過", lambda r: (overheat_value(r) is not None and (overheat_value(r) or 99) <= 1)),
        ("gap<=3 通過", gap_ok),
        ("panic_selloff除外後", lambda r: str(r.get("market_regime") or "") != "panic_selloff"),
        ("margin現行H5同等通過", margin_ok),
    ]
    out = []
    out.append({"case_key": case, "filter": "all_predictions", "remaining": total, "dropped": 0, "scope": "all"})
    for name, fn in filters:
        count = sum(1 for r in rows if fn(r))
        out.append({"case_key": case, "filter": name, "remaining": count, "dropped": total - count, "scope": "independent"})
    if case == CURRENT_CASE:
        final_count = sum(1 for r in rows if current_pass(r) and gap_ok(r))
    elif case == SHORT_CASE:
        final_count = sum(1 for r in rows if short_pass(r) and gap_ok(r))
    else:
        final_count = len(mixed_current7_short3([r for r in rows if (current_pass(r) or short_pass(r)) and gap_ok(r)]))
    out.append({"case_key": case, "filter": "最終採用数", "remaining": final_count, "dropped": total - final_count, "scope": "case_final"})
    return out


def latest_candidate_row(row: dict[str, Any], case: str, reason: str) -> dict[str, Any]:
    return {
        "signal_date": row.get("trade_date") or row.get("signal_date"),
        "case_key": case,
        "code": row.get("code"),
        "name": row.get("name"),
        "score": row.get("signal_probability") or row.get("score"),
        "signal_stage": row.get("signal_stage"),
        "drop5": row.get("drop_from_5d_high_pct"),
        "drop10": row.get("drop_from_10d_high_pct"),
        "drop20": row.get("drop_from_20d_high_pct"),
        "gap": row.get("entry_gap_pct"),
        "overheat_score": row.get("overheat_score"),
        "entry_date": "",
        "entry_price": "",
        "exit_date": "",
        "exit_price": "",
        "return_pct": "",
        "pnl_after_cost": "",
        "cumulative_pnl": "",
        "採用理由": reason,
        "除外理由": "",
    }


def latest_outputs(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    current = [r for r in rows if current_pass(r)]
    short = [r for r in rows if short_pass(r)]
    mixed = mixed_current7_short3([r for r in rows if current_pass(r) or short_pass(r)])

    candidates = (
        [latest_candidate_row(r, CURRENT_CASE, "current_h5") for r in current]
        + [latest_candidate_row(r, SHORT_CASE, "short_pullback") for r in short]
        + [latest_candidate_row(r, MIX_CASE, r.get("adoption_reason") or "current7_short3") for r in mixed]
    )
    counts = []
    counts.extend(latest_filter_counts(rows, CURRENT_CASE))
    counts.extend(latest_filter_counts(rows, SHORT_CASE))
    counts.extend(latest_filter_counts(rows, MIX_CASE))

    rejects = []
    for row in rows:
        score = score_of(row)
        drop20_ok = num(row.get("drop_from_20d_high_pct")) is not None and (num(row.get("drop_from_20d_high_pct")) or 999) <= -8
        drop5_ok = num(row.get("drop_from_5d_high_pct")) is not None and (num(row.get("drop_from_5d_high_pct")) or 999) <= -3
        hot = overheat_value(row)
        base_ok = (
            score is not None and score >= 0.65
            and stage_ok(row)
            and str(row.get("market_regime") or "") != "panic_selloff"
            and margin_ok(row)
            and (drop20_ok or drop5_ok)
        )
        if base_ok and hot is not None and hot > 1:
            rejects.append({
                "signal_date": row.get("trade_date") or row.get("signal_date"),
                "code": row.get("code"),
                "name": row.get("name"),
                "score": row.get("signal_probability") or row.get("score"),
                "signal_stage": row.get("signal_stage"),
                "drop5": row.get("drop_from_5d_high_pct"),
                "drop10": row.get("drop_from_10d_high_pct"),
                "drop20": row.get("drop_from_20d_high_pct"),
                "RSI14": row.get("rsi14"),
                "ma5_gap_pct": row.get("ma5_gap_pct"),
                "return_5d_pct": row.get("return_5d_pct"),
                "volume_ratio_20d": row.get("volume_ratio_20d") or row.get("volume_ratio"),
                "overheat_score": hot,
                "除外理由": "overheat_score > 1",
                "watch_case": WATCH_CASE,
            })
    return candidates, counts, rejects


def report_text(summary: list[dict[str, Any]], latest: list[dict[str, Any]], rejects: list[dict[str, Any]], stats: Counter) -> str:
    latest_counts = Counter(str(r.get("case_key")) for r in latest)
    lines = [
        "H5 stored forward comparison cases",
        "",
        "Production impact:",
        "- Primary/H5 production rules changed: no",
        "- LINE main notification changed: no",
        "- actual_trade_logs changed: no",
        "- auto trading changed: no",
        "",
        "Cases:",
    ]
    for row in summary:
        lines.append(
            f"- {row.get('case_key')}: n={row.get('count')} avg={num(row.get('avg_return_pct'), 0):.2f}% "
            f"PF={num(row.get('PF'), 0):.3f} after_cost={num(row.get('pnl_after_cost'), 0):,.0f} "
            f"after_aggregate_tax={num(row.get('pnl_after_aggregate_tax'), 0):,.0f} maxDD={num(row.get('max_dd'), 0):,.0f}"
        )
    lines.extend([
        "",
        "Latest candidates:",
        f"- {CURRENT_CASE}: {latest_counts[CURRENT_CASE]}",
        f"- {SHORT_CASE}: {latest_counts[SHORT_CASE]}",
        f"- {MIX_CASE}: {latest_counts[MIX_CASE]}",
        f"- {WATCH_CASE}: {len(rejects)}",
        "",
        f"feature_stats: {dict(stats)}",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    input_path = ROOT / args.input

    rows, feature_stats = load_rows(input_path, out_dir)
    selected_raw = {
        CURRENT_CASE: [r for r in rows if r.get("is_current_h5")],
        SHORT_CASE: [r for r in rows if r.get("is_short_pullback")],
    }
    union_raw = [r for r in rows if r.get("is_current_h5") or r.get("is_short_pullback")]
    selected_raw[MIX_CASE] = mixed_current7_short3(union_raw)

    summaries: list[dict[str, Any]] = []
    all_case_rows: list[dict[str, Any]] = []
    all_top_bottom: list[dict[str, Any]] = []
    sims: dict[str, dict[str, Any]] = {}
    all_exec_input = selected_raw[MIX_CASE] or union_raw
    exec_all, exec_stats = make_exec(all_exec_input, input_path)
    start = min((str(r.get("entry_date") or "") for r in exec_all if r.get("entry_date")), default="")
    end = max((str(r.get("exit_date") or r.get("entry_date") or "") for r in exec_all if r.get("exit_date") or r.get("entry_date")), default="")

    for case, raw_case_rows in selected_raw.items():
        exec_rows, stats = make_exec(raw_case_rows, input_path)
        feature_stats += stats
        summary, sim = run_case(case, exec_rows, start, end)
        summaries.append(summary)
        sims[case] = sim
        all_case_rows.extend(output_case_rows(case, sim))
        all_top_bottom.extend(top_bottom_rows(case, sim.get("executed", []), 10))

    latest_rows = latest_enriched_rows(out_dir)
    latest_candidates, latest_counts, overheat_rejects = latest_outputs(latest_rows)

    write_csv_with_headers(out_dir / "case_daily_rows.csv", all_case_rows, CASE_DAILY_COLUMNS)
    write_csv(out_dir / "case_summary.csv", summaries)
    write_csv(out_dir / "case_top_bottom.csv", all_top_bottom)
    write_csv_with_headers(out_dir / "latest_candidates.csv", latest_candidates, CASE_DAILY_COLUMNS)
    write_csv(out_dir / "latest_filter_counts.csv", latest_counts)
    write_csv_with_headers(out_dir / "overheat_reject_watch.csv", overheat_rejects, WATCH_COLUMNS)
    write_text(out_dir / "report.txt", report_text(summaries, latest_candidates, overheat_rejects, feature_stats + exec_stats))

    print(f"output_dir={out_dir}")
    for case in [CURRENT_CASE, SHORT_CASE, MIX_CASE]:
        print(f"latest_{case}_count={sum(1 for r in latest_candidates if r.get('case_key') == case)}")
    print(f"overheat_reject_watch_count={len(overheat_rejects)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
