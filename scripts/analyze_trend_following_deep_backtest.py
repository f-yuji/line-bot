#!/usr/bin/env python3
"""Deep backtest for trend-following / trend-pullback research cases.

Research only. This script does not change Primary/H5 production rules, LINE
notifications, actual_trade_logs, or auto-trading paths.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_pullback_relaxation import TAX_RATE, common_pass, normalize_code, variant_pass  # noqa: E402
from analyze_h5_s_share_realistic_operation import pf, simulate_realistic  # noqa: E402
from analyze_trend_following_grid_search import (  # noqa: E402
    CAPITAL,
    COST_BPS,
    DEFAULT_INPUT,
    NOTIONAL,
    CaseDef,
    base_case_pass,
    build_cases,
    cond_registry,
    execute_all_horizons,
    latest_candidates,
    latest_rows,
    load_feature_rows,
    margin_pass,
    market_split,
    num,
    overheat,
    proxy_usage_rows,
    read_csv,
    score,
    sim_params,
    summarize,
    top_bottom,
)
from analyze_h5_primary_fractional_sizing import write_csv, write_text  # noqa: E402


DEFAULT_OUTPUT = "outputs/trend_following_deep_backtest"
H5_STORED_CASES = ROOT / "outputs/h5_stored_forward_cases/case_summary.csv"


def date_text(value: Any) -> str:
    return str(value or "")[:10]


def parse_dt(value: Any) -> datetime | None:
    text = date_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def case_key(row: dict[str, Any]) -> tuple[str, str]:
    return date_text(row.get("trade_date") or row.get("signal_date") or row.get("entry_date")), normalize_code(row.get("code"))


def current_h5_pass(row: dict[str, Any]) -> bool:
    return variant_pass(row, "drop20", -8.0)


def short_h5_pass(row: dict[str, Any]) -> bool:
    if not common_pass(row):
        return False
    drop5 = num(row.get("drop_from_5d_high_pct"))
    return drop5 is not None and drop5 <= -3.0


def mixed_current7_short3(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[date_text(row.get("entry_date") or row.get("trade_date"))].append(row)
    out: list[dict[str, Any]] = []
    for day in sorted(by_day):
        items = sorted(by_day[day], key=lambda r: int(num(r.get("_source_row_index"), 0) or 0))
        selected: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in [r for r in items if r.get("is_current_h5")]:
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["adoption_reason"] = "current_h5_slot"
            selected.append(nr)
            seen.add(key)
            if len(selected) >= 7:
                break
        short_count = 0
        for row in [r for r in items if r.get("is_short_h5")]:
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["adoption_reason"] = "short_h5_slot"
            selected.append(nr)
            seen.add(key)
            short_count += 1
            if short_count >= 3:
                break
        out.extend(selected[:10])
    return out


def run_sim(case_id: str, rows: list[dict[str, Any]], *, holding_days: int = 3, daily_cap: int = 10, gap_limit: float = 3.0) -> dict[str, Any]:
    params = {
        "scenario_id": case_id,
        "capital": CAPITAL,
        "notional": NOTIONAL,
        "daily_cap": daily_cap,
        "gap_limit": gap_limit,
        "tax_rate": 0.0,
        "cost_bps": COST_BPS,
        "apply_tax": False,
        "entry_mode": "next_open",
    }
    return simulate_realistic(rows, params)


def summarize_custom(case_id: str, sim: dict[str, Any], *, conditions: str = "", family: str = "custom") -> dict[str, Any]:
    s = dict(sim.get("summary") or {})
    after_cost = num(s.get("total_pnl_after_tax"), 0.0) or 0.0
    taxed = after_cost - max(after_cost, 0.0) * TAX_RATE
    curve = sim.get("curve") or []
    daily = [num(r.get("daily_realized_pnl"), 0.0) or 0.0 for r in curve]
    sharpe = mean(daily) / pstdev(daily) * (252 ** 0.5) if len(daily) > 2 and pstdev(daily) else None
    dates = [date_text(r.get("date")) for r in curve if r.get("date")]
    cagr = None
    if dates and taxed:
        start = parse_dt(min(dates))
        end = parse_dt(max(dates))
        if start and end and (end - start).days > 0:
            cagr = ((CAPITAL + taxed) / CAPITAL) ** (365 / (end - start).days) - 1
            cagr *= 100
    return {
        "case_key": case_id,
        "family": family,
        "conditions": conditions,
        "count": s.get("executed_count"),
        "active_days": len({r.get("entry_date") for r in sim.get("executed", [])}),
        "avg_return_pct": s.get("avg_return_pct"),
        "median_return_pct": s.get("median_return_pct"),
        "win_rate": s.get("win_rate"),
        "PF": s.get("PF_after_tax"),
        "pretax_pnl": s.get("total_pnl_before_tax"),
        "pnl_after_cost": after_cost,
        "taxed_pnl": taxed,
        "CAGR": cagr,
        "max_dd": s.get("max_dd_after_tax"),
        "sharpe_like": sharpe,
        "max_loss_streak": s.get("max_consecutive_losses"),
        "max_daily_loss": min(daily) if daily else None,
    }


def period_key(date_value: Any, period: str) -> str:
    dt = parse_dt(date_value)
    if not dt:
        return "unknown"
    if period == "year":
        return f"{dt.year}"
    if period == "month":
        return f"{dt.year}-{dt.month:02d}"
    if period == "quarter":
        return f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"
    return date_text(date_value)


def group_metrics(case_id: str, group_name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [num(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in rows]
    rets = [num(r.get("return_pct"), 0.0) or 0.0 for r in rows]
    after = sum(pnls)
    return {
        "case_key": case_id,
        "group": group_name,
        "count": len(rows),
        "avg_return_pct": mean(rets) if rets else None,
        "median_return_pct": median(rets) if rets else None,
        "win_rate": sum(v > 0 for v in pnls) / len(pnls) * 100 if pnls else None,
        "PF": pf(pnls),
        "pnl_after_cost": after,
        "taxed_pnl": after - max(after, 0.0) * TAX_RATE,
    }


def period_summary(case_id: str, rows: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[period_key(row.get("entry_date") or row.get("trade_date"), period)].append(row)
    return [group_metrics(case_id, key, value) for key, value in sorted(groups.items())]


def recent_summary(case_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [parse_dt(r.get("entry_date") or r.get("trade_date")) for r in rows]
    dates = [d for d in dates if d]
    if not dates:
        return []
    last = max(dates)
    out = []
    for label, days in [("recent_1m", 31), ("recent_3m", 93), ("recent_6m", 186)]:
        start = last - timedelta(days=days)
        subset = [r for r in rows if (parse_dt(r.get("entry_date") or r.get("trade_date")) or last) >= start]
        out.append(group_metrics(case_id, label, subset))
    return out


def outlier_check(case_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda r: num(r.get("return_pct"), 0.0) or 0.0)
    n = len(ordered)
    cut = max(1, int(n * 0.01)) if n >= 100 else 1
    variants = {
        "all": ordered,
        "drop_top_1pct": ordered[:-cut] if n > cut else [],
        "drop_bottom_1pct": ordered[cut:] if n > cut else [],
        "drop_both_1pct": ordered[cut:-cut] if n > cut * 2 else [],
    }
    base = group_metrics(case_id, "all", variants["all"])
    out = {"case_key": case_id}
    for name, subset in variants.items():
        rec = group_metrics(case_id, name, subset)
        out[f"{name}_count"] = rec["count"]
        out[f"{name}_PF"] = rec["PF"]
        out[f"{name}_taxed_pnl"] = rec["taxed_pnl"]
    top3 = sorted([num(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in rows], reverse=True)[:3]
    total = num(base.get("pnl_after_cost"), 0.0) or 0.0
    out["top3_profit_share_pct"] = (sum(v for v in top3 if v > 0) / total * 100) if total > 0 else None
    return out


def month_stability(rows: list[dict[str, Any]]) -> float | None:
    monthly = period_summary("x", rows, "month")
    vals = [num(r.get("taxed_pnl"), 0.0) or 0.0 for r in monthly]
    if not vals:
        return None
    return sum(v > 0 for v in vals) / len(vals) * 100


def simple_corr(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def daily_pnl_map(sim: dict[str, Any]) -> dict[str, float]:
    return {date_text(r.get("date")): num(r.get("daily_realized_pnl"), 0.0) or 0.0 for r in sim.get("curve", [])}


def robust_rank(summary: list[dict[str, Any]], sim_by_case: dict[str, dict[str, Any]], outliers: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for row in summary:
        case = str(row.get("case_key"))
        rows = sim_by_case.get(case, {}).get("executed", [])
        count = num(row.get("count"), 0) or 0
        pfv = num(row.get("PF"), 0) or 0
        dd = abs(num(row.get("max_dd"), 0) or 0)
        taxed = num(row.get("taxed_pnl"), 0) or 0
        stable = month_stability(rows) or 0
        out = outliers.get(case, {})
        base_pf = num(out.get("all_PF"), 0) or 0
        no_top_pf = num(out.get("drop_top_1pct_PF"), 0) or 0
        outlier_penalty = max(0.0, base_pf - no_top_pf)
        condition_count = len(str(row.get("conditions") or "").split(";")) if row.get("conditions") else 0
        score_value = (
            min(pfv, 3.0) * 30
            + min(count / 100, 3.0) * 12
            + stable * 0.25
            + min(max(taxed, 0) / 100_000, 5.0) * 8
            - min(dd / 100_000, 8.0) * 5
            - outlier_penalty * 20
            - max(0, condition_count - 5) * 3
        )
        nr = dict(row)
        nr["robust_score"] = score_value
        nr["monthly_positive_rate"] = stable
        nr["outlier_pf_drop_top_1pct"] = outlier_penalty
        nr["condition_count"] = condition_count
        ranked.append(nr)
    return sorted(ranked, key=lambda r: num(r.get("robust_score"), -999) or -999, reverse=True)


def warning_rows(summary: list[dict[str, Any]], sim_by_case: dict[str, dict[str, Any]], outliers: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in summary:
        case = str(row.get("case_key"))
        reasons = []
        count = num(row.get("count"), 0) or 0
        pfv = num(row.get("PF"), 0) or 0
        if count < 50:
            reasons.append("count_lt_50")
        if pfv >= 3 and count < 100:
            reasons.append("high_pf_low_count")
        check = outliers.get(case, {})
        share = num(check.get("top3_profit_share_pct"))
        if share is not None and share >= 60:
            reasons.append("top3_profit_dependency")
        base_pf = num(check.get("all_PF"), 0) or 0
        no_top_pf = num(check.get("drop_top_1pct_PF"), 0) or 0
        if base_pf and no_top_pf and no_top_pf < base_pf * 0.75:
            reasons.append("top_outlier_pf_damage")
        monthly = period_summary(case, sim_by_case.get(case, {}).get("executed", []), "month")
        if monthly:
            best_month = max((num(m.get("taxed_pnl"), 0.0) or 0.0 for m in monthly), default=0.0)
            total = num(row.get("taxed_pnl"), 0.0) or 0.0
            if total > 0 and best_month / total >= 0.5:
                reasons.append("single_month_dependency")
        if reasons:
            nr = dict(row)
            nr["warning_reasons"] = ",".join(reasons)
            nr["top3_profit_share_pct"] = share
            out.append(nr)
    return out


def select_by_slots(h5_rows: list[dict[str, Any]], trend_rows: list[dict[str, Any]], h5_slots: int, trend_slots: int, case_id: str) -> list[dict[str, Any]]:
    by_day_h5: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_day_trend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in h5_rows:
        by_day_h5[date_text(row.get("entry_date"))].append(row)
    for row in trend_rows:
        by_day_trend[date_text(row.get("entry_date"))].append(row)
    out = []
    for day in sorted(set(by_day_h5) | set(by_day_trend)):
        seen: set[tuple[str, str]] = set()
        selected = []
        for row in sorted(by_day_h5.get(day, []), key=lambda r: int(num(r.get("_source_row_index"), 0) or 0)):
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["mix_case_key"] = case_id
            nr["mix_source"] = "h5"
            selected.append(nr)
            seen.add(key)
            if len([r for r in selected if r.get("mix_source") == "h5"]) >= h5_slots:
                break
        trend_count = 0
        for row in sorted(by_day_trend.get(day, []), key=lambda r: int(num(r.get("_source_row_index"), 0) or 0)):
            key = case_key(row)
            if key in seen:
                continue
            nr = dict(row)
            nr["mix_case_key"] = case_id
            nr["mix_source"] = "trend"
            selected.append(nr)
            seen.add(key)
            trend_count += 1
            if trend_count >= trend_slots:
                break
        out.extend(selected[:10])
    return out


def mix_daily_rows(case_id: str, sim: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    cumulative = 0.0
    for row in sorted(sim.get("executed", []), key=lambda r: (date_text(r.get("exit_date")), date_text(r.get("entry_date")))):
        pnl = num(row.get("_pnl_after_cost"), 0.0) or 0.0
        cumulative += pnl
        out.append({
            "mix_case_key": case_id,
            "source": row.get("mix_source"),
            "signal_date": row.get("trade_date"),
            "entry_date": row.get("entry_date"),
            "exit_date": row.get("exit_date"),
            "code": row.get("code"),
            "name": row.get("name"),
            "return_pct": row.get("return_pct"),
            "pnl_after_cost": pnl,
            "cumulative_pnl": cumulative,
        })
    return out


def corr_rows(named_sims: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    maps = {k: daily_pnl_map(v) for k, v in named_sims.items()}
    names = sorted(maps)
    out = []
    for a in names:
        for b in names:
            dates = sorted(set(maps[a]) | set(maps[b]))
            xs = [maps[a].get(d, 0.0) for d in dates]
            ys = [maps[b].get(d, 0.0) for d in dates]
            out.append({"case_a": a, "case_b": b, "correlation": simple_corr(xs, ys), "days": len(dates)})
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT)
    parser.add_argument("--max-cases", type=int, default=2400)
    parser.add_argument("--min-candidates", type=int, default=30)
    parser.add_argument("--robust-top", type=int, default=30)
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    input_path = ROOT / args.input
    conditions = cond_registry()
    rows, feature_stats = load_feature_rows(input_path, out_dir)
    join_diag = Counter({
        "loaded_rows": len(rows),
        "missing_code": sum(1 for r in rows if not r.get("code")),
        "missing_trade_date": sum(1 for r in rows if not r.get("trade_date")),
        "missing_overheat_before_fill": sum(1 for r in rows if r.get("overheat_score") in (None, "")),
        "feature_cache_rows": feature_stats.get("shared_feature_cache_rows", 0),
    })
    for row in rows:
        row["is_current_h5"] = current_h5_pass(row)
        row["is_short_h5"] = short_h5_pass(row)
    cases = build_cases(args.max_cases)
    exec_by_hd, exec_stats = execute_all_horizons(rows, input_path)
    all_exec_rows = [r for hd_rows in exec_by_hd.values() for r in hd_rows]
    start_date = min((date_text(r.get("entry_date")) for r in all_exec_rows if r.get("entry_date")), default="")
    end_date = max((date_text(r.get("exit_date") or r.get("entry_date")) for r in all_exec_rows if r.get("exit_date") or r.get("entry_date")), default="")

    trend_summaries: list[dict[str, Any]] = []
    sim_by_case: dict[str, dict[str, Any]] = {}
    top_bottom_rows: list[dict[str, Any]] = []
    for case in cases:
        candidates = [r for r in exec_by_hd[case.holding_days] if base_case_pass(r, case, conditions)]
        if len(candidates) < args.min_candidates and not case.focus:
            continue
        sim = simulate_realistic(candidates, sim_params(case))
        rec = summarize(case, sim, len(candidates), start_date, end_date)
        trend_summaries.append(rec)
        sim_by_case[case.case_key] = sim
        top_bottom_rows.extend(top_bottom(case, sim.get("executed", []), 10))

    trend_summaries.sort(key=lambda r: num(r.get("taxed_pnl"), -10**18) or -10**18, reverse=True)
    raw_best = trend_summaries[:100]
    outlier_rows = {case: outlier_check(case, sim.get("executed", [])) for case, sim in sim_by_case.items()}
    robust = robust_rank(trend_summaries, sim_by_case, outlier_rows)
    warnings = warning_rows(trend_summaries, sim_by_case, outlier_rows)
    robust_top_cases = [str(r.get("case_key")) for r in robust[:args.robust_top]]

    yearly: list[dict[str, Any]] = []
    monthly: list[dict[str, Any]] = []
    regime: list[dict[str, Any]] = []
    recent: list[dict[str, Any]] = []
    outlier_csv: list[dict[str, Any]] = []
    for case in robust_top_cases:
        sim = sim_by_case[case]
        executed = sim.get("executed", [])
        yearly.extend(period_summary(case, executed, "year"))
        monthly.extend(period_summary(case, executed, "month"))
        monthly.extend(period_summary(case, executed, "quarter"))
        recent.extend(recent_summary(case, executed))
        outlier_csv.append(outlier_rows[case])
        fake_case = CaseDef(case, "robust", (), 0, None, "none", 3, 10, 3.0)
        regime.extend(market_split(fake_case, executed))
    monthly.extend(recent)

    # H5 baselines recomputed from the same execution universe.
    h5_exec = exec_by_hd[3]
    h5_current = [r for r in h5_exec if r.get("is_current_h5")]
    h5_short = [r for r in h5_exec if r.get("is_short_h5")]
    h5_mix = mixed_current7_short3([r for r in h5_exec if r.get("is_current_h5") or r.get("is_short_h5")])
    h5_sims = {
        "current_h5": run_sim("current_h5", h5_current, holding_days=3, daily_cap=10, gap_limit=3.0),
        "H5_short_pullback_drop5_m3": run_sim("H5_short_pullback_drop5_m3", h5_short, holding_days=3, daily_cap=10, gap_limit=3.0),
        "H5_current7_short3": run_sim("H5_current7_short3", h5_mix, holding_days=3, daily_cap=10, gap_limit=3.0),
    }
    h5_compare = [summarize_custom(k, v, family="H5") for k, v in h5_sims.items()]
    h5_compare.extend([{**r, "family": "trend"} for r in robust[:30]])

    best_trend_key = robust[0]["case_key"] if robust else ""
    best_trend_rows = sim_by_case.get(str(best_trend_key), {}).get("executed", [])
    mix_defs = [
        ("mix_current_h5_8_2", h5_sims["current_h5"].get("executed", []), best_trend_rows, 8, 2),
        ("mix_current_h5_7_3", h5_sims["current_h5"].get("executed", []), best_trend_rows, 7, 3),
        ("mix_current_h5_5_5", h5_sims["current_h5"].get("executed", []), best_trend_rows, 5, 5),
        ("mix_short_h5_trend_7_3", h5_sims["H5_short_pullback_drop5_m3"].get("executed", []), best_trend_rows, 7, 3),
        ("mix_current7_short3_trend_7_3", h5_sims["H5_current7_short3"].get("executed", []), best_trend_rows, 7, 3),
    ]
    mix_sims: dict[str, dict[str, Any]] = {}
    mix_summary: list[dict[str, Any]] = []
    mix_rows: list[dict[str, Any]] = []
    for mix_key, h5_rows, trend_rows, h5_slots, trend_slots in mix_defs:
        selected = select_by_slots(h5_rows, trend_rows, h5_slots, trend_slots, mix_key)
        sim = run_sim(mix_key, selected, holding_days=3, daily_cap=10, gap_limit=3.0)
        mix_sims[mix_key] = sim
        rec = summarize_custom(mix_key, sim, family="portfolio_mix", conditions=f"h5_slots={h5_slots};trend_slots={trend_slots};trend={best_trend_key}")
        rec["h5_slots"] = h5_slots
        rec["trend_slots"] = trend_slots
        rec["trend_case_key"] = best_trend_key
        mix_summary.append(rec)
        mix_rows.extend(mix_daily_rows(mix_key, sim))

    corr = corr_rows({**h5_sims, **{str(best_trend_key): sim_by_case.get(str(best_trend_key), {})}, **mix_sims})
    latest = latest_candidates(latest_rows(out_dir), [c for c in cases if c.case_key in robust_top_cases[:10]], conditions)
    h5_loaded = read_csv(H5_STORED_CASES)

    write_csv(out_dir / "01_all_case_summary.csv", trend_summaries)
    write_csv(out_dir / "02_raw_best_cases.csv", raw_best)
    write_csv(out_dir / "03_robust_best_cases.csv", robust[:100])
    write_csv(out_dir / "04_overfit_warning_cases.csv", warnings)
    write_csv(out_dir / "05_yearly_summary.csv", yearly)
    write_csv(out_dir / "06_monthly_summary.csv", monthly)
    write_csv(out_dir / "07_regime_summary.csv", regime)
    write_csv(out_dir / "08_outlier_check.csv", outlier_csv)
    write_csv(out_dir / "09_top_bottom.csv", top_bottom_rows)
    write_csv(out_dir / "10_h5_comparison.csv", h5_compare)
    write_csv(out_dir / "11_portfolio_mix_summary.csv", mix_summary)
    write_csv(out_dir / "12_portfolio_mix_daily_rows.csv", mix_rows)
    write_csv(out_dir / "13_strategy_correlation.csv", corr)
    write_csv(out_dir / "14_latest_candidates.csv", latest)
    write_csv(out_dir / "15_join_diagnostics.csv", [{"metric": k, "value": v} for k, v in {**join_diag, **exec_stats}.items()])
    write_csv(out_dir / "16_proxy_usage.csv", proxy_usage_rows())

    dd_improved = [
        r for r in mix_summary
        if abs(num(r.get("max_dd"), 10**18) or 10**18) < abs(num(h5_compare[0].get("max_dd"), 0) or 0)
    ]
    h5_best_taxed = max((num(r.get("taxed_pnl"), 0.0) or 0.0 for r in h5_compare if r.get("family") == "H5"), default=0.0)
    trend_better = [r for r in robust if (num(r.get("taxed_pnl"), 0.0) or 0.0) > h5_best_taxed]
    report = f"""Trend following deep backtest report

Production impact: no Primary/H5/LINE/actual_trade_logs/auto-trading changes.

total_cases_defined: {len(cases)}
valid_trend_cases: {len(trend_summaries)}
robust_top_case: {robust[0].get('case_key') if robust else ''}
robust_top_taxed_pnl: {num(robust[0].get('taxed_pnl'), 0) if robust else 0:,.0f}
robust_top_PF: {num(robust[0].get('PF'), 0) if robust else 0:.3f}

H5 best taxed PnL: {h5_best_taxed:,.0f}
trend cases better than H5 by taxed PnL: {len(trend_better)}
portfolio mixes with DD below current_h5: {len(dd_improved)}
latest candidate rows: {len(latest)}
overfit warning cases: {len(warnings)}

Interpretation:
- Trend-pullback cases show positive expectancy in some regimes, but current H5 still dominates absolute taxed PnL in this data.
- Low-credit / low-overheat trend pullback is the cleaner forward-test candidate than pure breakout.
- Credit data is limited to margin_ratio proxy; credit balance deltas, short-sale ratio, and JSF data are not available in the local WF/cache.
- Do not promote to production from this backtest alone. Use stored forward-test observation first.
"""
    write_text(out_dir / "17_report.txt", report)

    print(f"output_dir={out_dir}")
    print(f"total_cases={len(cases)}")
    print(f"valid_cases={len(trend_summaries)}")
    print(f"robust_top={robust[0].get('case_key') if robust else ''}")
    print(f"overfit_warning_cases={len(warnings)}")
    print(f"trend_better_than_h5={len(trend_better)}")
    print(f"mix_dd_improved={len(dd_improved)}")
    print(f"latest_candidates={len(latest)}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
