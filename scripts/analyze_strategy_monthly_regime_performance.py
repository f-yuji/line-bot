#!/usr/bin/env python3
"""Monthly/regime performance audit for H5, short-pullback, trend, and mixes.

Research only. This script reads existing backtest/forward-test CSVs and writes
reports under outputs/. It does not update Primary/H5 production rules, LINE,
actual_trade_logs, virtual_trades, or auto-trading paths.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import write_csv, write_text  # noqa: E402
from analyze_h5_pullback_relaxation import TAX_RATE, common_pass, normalize_code, variant_pass  # noqa: E402
from analyze_h5_s_share_realistic_operation import pf, simulate_realistic  # noqa: E402
from analyze_trend_following_deep_backtest import (  # noqa: E402
    CAPITAL,
    COST_BPS,
    DEFAULT_INPUT,
    NOTIONAL,
    mixed_current7_short3,
    num,
    run_sim,
)
from analyze_trend_following_grid_search import (  # noqa: E402
    base_case_pass,
    build_cases,
    cond_registry,
    execute_all_horizons,
    load_feature_rows,
    sim_params,
)


DEFAULT_OUTPUT = "outputs/strategy_monthly_regime_performance"
H5_CASE_ROWS = ROOT / "outputs/h5_stored_forward_cases/case_daily_rows.csv"
TREND_ROBUST = ROOT / "outputs/trend_following_deep_backtest/03_robust_best_cases.csv"
MIX_ROWS = ROOT / "outputs/trend_following_deep_backtest/12_portfolio_mix_daily_rows.csv"
BOX_ROWS = ROOT / "outputs/box_portfolio/portfolio_trades.csv"
SUPPORT_ROWS = ROOT / "outputs/box_backtest/support_trades.csv"

CURRENT_CASE = "current_h5_core"
STORED_CURRENT_CASE = "current_h5"
SHORT_CASE = "H5_short_pullback_drop5_m3"
CURRENT7_SHORT3_CASE = "H5_current7_short3"
TREND_ALIAS = "trend_support_best"
PREFERRED_TREND_PREFIX = "tf_166745_trend_rs_mom_market"
MIX_TREND_CASE = "mix_current7_short3_trend_7_3"


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def date_text(value: Any) -> str:
    return str(value or "").split("T", 1)[0][:10]


def parse_date(value: Any) -> date | None:
    text = date_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def year_month(value: Any) -> str:
    dt = parse_date(value)
    return f"{dt.year}-{dt.month:02d}" if dt else "unknown"


def to_float(value: Any, default: float | None = None) -> float | None:
    out = num(value, default)
    if out is None or (isinstance(out, float) and math.isnan(out)):
        return default
    return out


def pct_return_pnl(row: dict[str, Any]) -> float:
    return_pct = to_float(row.get("return_pct"), 0.0) or 0.0
    return NOTIONAL * return_pct / 100.0


def after_cost_pnl(row: dict[str, Any]) -> float:
    for key in ("pnl_after_cost", "_pnl_after_cost", "tax_adjusted_pnl"):
        value = to_float(row.get(key))
        if value is not None:
            return value
    return pct_return_pnl(row) - NOTIONAL * COST_BPS / 10000.0


def max_drawdown(values: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        dd = min(dd, equity - peak)
    return abs(dd)


def max_loss_streak(values: list[float]) -> int:
    best = cur = 0
    for value in values:
        if value < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def top_bottom_names(rows: list[dict[str, Any]], top: bool) -> str:
    ordered = sorted(rows, key=lambda r: to_float(r.get("return_pct"), 0.0) or 0.0, reverse=top)[:5]
    labels = []
    for row in ordered:
        code = normalize_code(row.get("code"))
        name = str(row.get("name") or "").strip()
        ret = to_float(row.get("return_pct"), 0.0) or 0.0
        labels.append(f"{code} {name} {ret:.2f}%".strip())
    return " / ".join(labels)


def normalize_trade_row(row: dict[str, Any], case_key: str, source: str) -> dict[str, Any]:
    entry = date_text(row.get("entry_date") or row.get("signal_date") or row.get("trade_date"))
    signal = date_text(row.get("signal_date") or row.get("trade_date") or entry)
    out = dict(row)
    out["case_key"] = case_key
    out["source_file"] = source
    out["signal_date"] = signal
    out["entry_date"] = entry
    out["exit_date"] = date_text(row.get("exit_date"))
    out["code"] = normalize_code(row.get("code"))
    out["return_pct"] = to_float(row.get("return_pct"), 0.0) or 0.0
    out["_pretax_pnl"] = pct_return_pnl(out)
    out["_pnl_after_cost"] = after_cost_pnl(out)
    out["_year_month"] = year_month(entry)
    dt = parse_date(entry)
    out["_year"] = dt.year if dt else "unknown"
    return out


def metric_row(case_key: str, group_key: str, rows: list[dict[str, Any]], *, kind: str) -> dict[str, Any]:
    rows = sorted(rows, key=lambda r: (date_text(r.get("entry_date")), normalize_code(r.get("code"))))
    pnls = [to_float(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in rows]
    pretax = [to_float(r.get("_pretax_pnl"), 0.0) or 0.0 for r in rows]
    rets = [to_float(r.get("return_pct"), 0.0) or 0.0 for r in rows]
    by_day: dict[str, float] = defaultdict(float)
    for row, pnl in zip(rows, pnls):
        by_day[date_text(row.get("entry_date"))] += pnl
    daily_pnls = [by_day[d] for d in sorted(by_day)]
    after = sum(pnls)
    taxed = after - max(after, 0.0) * TAX_RATE
    base = {
        "case_key": case_key,
        "count": len(rows),
        "active_days": len({date_text(r.get("entry_date")) for r in rows if date_text(r.get("entry_date"))}),
        "avg_return_pct": mean(rets) if rets else None,
        "median_return_pct": median(rets) if rets else None,
        "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
        "PF": pf(pnls),
        "pretax_pnl": sum(pretax),
        "pnl_after_cost": after,
        "taxed_pnl": taxed,
        "max_dd": max_drawdown(daily_pnls),
        "max_loss_streak": max_loss_streak(pnls),
        "max_daily_loss": min(daily_pnls) if daily_pnls else None,
        "top_names": top_bottom_names(rows, True),
        "bottom_names": top_bottom_names(rows, False),
    }
    if kind == "month":
        ym = group_key
        y, m = ("unknown", "unknown")
        if "-" in ym:
            y, m = ym.split("-", 1)
        return {"year": y, "month": m, "year_month": ym, **base}
    if kind == "year":
        month_groups = {r.get("_year_month") for r in rows if r.get("_year_month") != "unknown"}
        monthly_values = []
        by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_month[str(row.get("_year_month"))].append(row)
        for _, month_rows in by_month.items():
            monthly_values.append(sum(to_float(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in month_rows))
        worst_month = min(by_month, key=lambda k: sum(to_float(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in by_month[k]), default="")
        best_month = max(by_month, key=lambda k: sum(to_float(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in by_month[k]), default="")
        return {
            "year": group_key,
            "case_key": case_key,
            "count": base["count"],
            "active_months": len(month_groups),
            "avg_return_pct": base["avg_return_pct"],
            "win_rate": base["win_rate"],
            "PF": base["PF"],
            "taxed_pnl": base["taxed_pnl"],
            "max_dd": base["max_dd"],
            "monthly_win_rate": sum(1 for v in monthly_values if v > 0) / len(monthly_values) * 100 if monthly_values else None,
            "worst_month": worst_month,
            "best_month": best_month,
        }
    return {"group": group_key, **base}


def load_h5_cases() -> list[dict[str, Any]]:
    rows = []
    for row in read_csv(H5_CASE_ROWS):
        source_case = str(row.get("case_key") or "")
        if source_case == STORED_CURRENT_CASE:
            case = CURRENT_CASE
        elif source_case in {SHORT_CASE, CURRENT7_SHORT3_CASE}:
            case = source_case
        else:
            continue
        rows.append(normalize_trade_row(row, case, str(H5_CASE_ROWS.relative_to(ROOT))))
    return rows


def current_h5_pass(row: dict[str, Any]) -> bool:
    return variant_pass(row, "drop20", -8.0)


def short_h5_pass(row: dict[str, Any]) -> bool:
    if not common_pass(row):
        return False
    drop5 = to_float(row.get("drop_from_5d_high_pct"))
    return drop5 is not None and drop5 <= -3.0


def select_trend_case(preferred: str) -> tuple[str, str]:
    robust = read_csv(TREND_ROBUST)
    for row in robust:
        key = str(row.get("case_key") or "")
        if key.startswith(preferred):
            return key, "preferred_tf_166745"
    if robust:
        return str(robust[0].get("case_key") or ""), "robust_top_fallback"
    return "", "not_available"


def build_trend_rows(input_path: Path, out_dir: Path, trend_case_key: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    if not trend_case_key:
        return [], {}, [], {}
    conditions = cond_registry()
    feature_rows, feature_stats = load_feature_rows(input_path, out_dir)
    # Use the full generated universe here. The deep-backtest robust IDs keep
    # their original generated index (for example tf_166745...), so a sampled
    # build_cases() call can skip the exact case even when it is in the robust
    # output.
    cases = {c.case_key: c for c in build_cases(200000)}
    case = cases.get(trend_case_key)
    if case is None:
        return [], {"trend_case_missing": trend_case_key, **feature_stats}, feature_rows, {}
    exec_by_hd, exec_stats = execute_all_horizons(feature_rows, input_path)
    selected = [r for r in exec_by_hd.get(case.holding_days, []) if base_case_pass(r, case, conditions)]
    sim = simulate_realistic(selected, sim_params(case))
    out = []
    cumulative = 0.0
    for row in sorted(sim.get("executed", []), key=lambda r: (date_text(r.get("entry_date")), normalize_code(r.get("code")))):
        pnl = to_float(row.get("_pnl_after_cost"), 0.0) or 0.0
        cumulative += pnl
        nr = dict(row)
        nr["signal_date"] = row.get("trade_date") or row.get("signal_date")
        nr["pnl_after_cost"] = pnl
        nr["cumulative_pnl"] = cumulative
        out.append(normalize_trade_row(nr, TREND_ALIAS, f"recomputed:{trend_case_key}"))
    stats = {"trend_case_key": trend_case_key, **feature_stats, **exec_stats}
    return out, stats, feature_rows, exec_by_hd


def load_mix_rows() -> list[dict[str, Any]]:
    rows = []
    for row in read_csv(MIX_ROWS):
        if str(row.get("mix_case_key") or "") == MIX_TREND_CASE:
            rows.append(normalize_trade_row(row, MIX_TREND_CASE, str(MIX_ROWS.relative_to(ROOT))))
    return rows


def load_optional_case(path: Path, case_key: str, source_name: str) -> tuple[list[dict[str, Any]], str]:
    raw = read_csv(path)
    if not raw:
        return [], "not_available"
    rows = []
    for row in raw:
        if not (row.get("return_pct") or row.get("pnl_after_cost")):
            continue
        rows.append(normalize_trade_row(row, case_key, source_name))
    return rows, "available" if rows else "no_compatible_trade_rows"


def monthly_summary(all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        groups[(str(row.get("case_key")), str(row.get("_year_month")))].append(row)
    return [metric_row(case, ym, rows, kind="month") for (case, ym), rows in sorted(groups.items())]


def yearly_summary(all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        groups[(str(row.get("case_key")), str(row.get("_year")))].append(row)
    return [metric_row(case, year, rows, kind="year") for (case, year), rows in sorted(groups.items())]


def market_months_from_rows(all_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        by_month[str(row.get("_year_month"))].append(row)
    out = []
    proxy = [
        {"feature": "nikkei_month_return", "status": "proxy", "implementation": "sum/mean of nikkei_change_pct on available signal rows"},
        {"feature": "TOPIX_month_return", "status": "proxy", "implementation": "sum/mean of topix_change_pct on available signal rows when present"},
        {"feature": "NASDAQ/SOX/USDJPY/VIX", "status": "not_available", "implementation": "no local monthly market series in existing outputs"},
        {"feature": "nikkei/topix > 25MA ratio", "status": "not_available", "implementation": "not in loaded trade rows"},
        {"feature": "darasage", "status": "proxy", "implementation": "negative monthly proxy return with few <= -2% daily proxy moves"},
        {"feature": "crash_rebound", "status": "proxy", "implementation": "month has <= -3% daily proxy move and positive monthly proxy return"},
    ]
    for ym, rows in sorted(by_month.items()):
        nikkei = [to_float(r.get("nikkei_change_pct")) for r in rows]
        nikkei = [v for v in nikkei if v is not None]
        topix = [to_float(r.get("topix_change_pct")) for r in rows]
        topix = [v for v in topix if v is not None]
        proxy_returns = nikkei or [mean([to_float(r.get("return_pct"), 0.0) or 0.0 for r in rows])]
        nikkei_month = sum(nikkei) if nikkei else None
        topix_month = sum(topix) if topix else None
        down_days = sum(1 for v in proxy_returns if v < 0)
        crash_days = sum(1 for v in proxy_returns if v <= -2.0)
        big_crash_days = sum(1 for v in proxy_returns if v <= -3.0)
        proxy_month = nikkei_month if nikkei_month is not None else sum(proxy_returns)
        is_bull = (nikkei_month or 0) > 0 and (topix_month is None or topix_month > 0)
        is_weak = (nikkei_month or 0) < 0 and (topix_month is None or topix_month < 0)
        darasage = proxy_month < 0 and crash_days <= max(1, len(proxy_returns) // 10)
        crash_rebound = big_crash_days > 0 and proxy_month > 0
        value_rotation = nikkei_month is not None and topix_month is not None and topix_month > nikkei_month + 1.0
        out.append({
            "year_month": ym,
            "nikkei_month_return": nikkei_month,
            "topix_month_return": topix_month,
            "nasdaq_month_return": "",
            "sox_month_return": "",
            "usdjpy_month_return": "",
            "vix_avg": "",
            "nikkei_above_ma25_ratio": "",
            "topix_above_ma25_ratio": "",
            "nikkei_down_days": down_days if nikkei else "",
            "nikkei_crash_days": crash_days if nikkei else "",
            "sox_down": "",
            "nasdaq_down": "",
            "bullish_index": is_bull,
            "weak_index": is_weak,
            "value_rotation": value_rotation,
            "growth_riskoff": "",
            "darasage": darasage,
            "crash_rebound": crash_rebound,
            "classification_note": "market proxy from available trade rows; blanks mean unavailable local series",
        })
    return out, proxy


def market_by_month(market_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(r.get("year_month")): r for r in market_rows}


def add_h5_stress_proxy_regimes(market_rows: list[dict[str, Any]], monthly_rows: list[dict[str, Any]], proxy_rows: list[dict[str, Any]]) -> None:
    """Fill weak/darasage proxies when external monthly index series is absent.

    The preferred regime source is market data. In this repo's stored outputs,
    NASDAQ/SOX/VIX are not present and Nikkei/TOPIX fields are only available on
    some recomputed trend rows. To keep the warning-month view useful, mark
    current_h5_core negative/PF<1 months as a fallback stress proxy. A very large
    one-day loss is treated as crash-like; otherwise it is tagged as darasage.
    """
    by_month = market_by_month(market_rows)
    current = [r for r in monthly_rows if r.get("case_key") == CURRENT_CASE]
    applied = 0
    for row in current:
        ym = str(row.get("year_month"))
        market = by_month.get(ym)
        if not market:
            continue
        h5_negative = (to_float(row.get("taxed_pnl"), 0.0) or 0.0) < 0
        h5_pf_bad = (to_float(row.get("PF"), 0.0) or 0.0) < 1.0
        max_daily_loss = to_float(row.get("max_daily_loss"), 0.0) or 0.0
        if h5_negative and h5_pf_bad:
            market["weak_index"] = True if market.get("weak_index") in ("", None, False, "False") else market.get("weak_index")
            if max_daily_loss <= -200_000:
                market["crash_rebound"] = True
            else:
                market["darasage"] = True
            market["classification_note"] = f"{market.get('classification_note')}; H5 stress fallback proxy applied"
            applied += 1
    proxy_rows.append({
        "feature": "H5 stress fallback regime",
        "status": "proxy" if applied else "not_used",
        "implementation": "current_h5_core taxed_pnl<0 and PF<1; large one-day loss => crash-like, otherwise darasage",
    })


def h5_bad_months(monthly_rows: list[dict[str, Any]], market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_case_month = {(r["case_key"], r["year_month"]): r for r in monthly_rows}
    market = market_by_month(market_rows)
    current_rows = [r for r in monthly_rows if r.get("case_key") == CURRENT_CASE]
    dd_values = sorted([to_float(r.get("max_dd"), 0.0) or 0.0 for r in current_rows], reverse=True)
    dd_cut = dd_values[max(0, min(len(dd_values) - 1, int(len(dd_values) * 0.25)))] if dd_values else 0.0
    out = []
    for row in current_rows:
        ym = str(row.get("year_month"))
        m = market.get(ym, {})
        reasons = []
        if (to_float(row.get("taxed_pnl"), 0.0) or 0.0) < 0:
            reasons.append("taxed_pnl_negative")
        if (to_float(row.get("PF"), 0.0) or 0.0) < 1.0:
            reasons.append("PF_below_1")
        if (to_float(row.get("max_dd"), 0.0) or 0.0) >= dd_cut and dd_cut > 0:
            reasons.append("large_dd_top_quartile")
        if (to_float(row.get("win_rate"), 0.0) or 0.0) < 45:
            reasons.append("win_rate_below_45")
        if str(m.get("darasage")).lower() == "true":
            reasons.append("darasage_month")
        if str(m.get("sox_down")).lower() == "true":
            reasons.append("sox_down")
        if str(m.get("bullish_index")).lower() == "true" and (to_float(row.get("taxed_pnl"), 0.0) or 0.0) < 0:
            reasons.append("strong_index_but_h5_weak")
        if not reasons:
            continue
        short = by_case_month.get((SHORT_CASE, ym), {})
        trend = by_case_month.get((TREND_ALIAS, ym), {})
        mix = by_case_month.get((MIX_TREND_CASE, ym), by_case_month.get((CURRENT7_SHORT3_CASE, ym), {}))
        h5_pnl = to_float(row.get("taxed_pnl"), 0.0) or 0.0
        out.append({
            **row,
            "bad_reasons": ",".join(reasons),
            "market_flags": ",".join(k for k in ["bullish_index", "weak_index", "value_rotation", "darasage", "crash_rebound"] if str(m.get(k)).lower() == "true"),
            "short_taxed_pnl": short.get("taxed_pnl"),
            "trend_taxed_pnl": trend.get("taxed_pnl"),
            "mix_taxed_pnl": mix.get("taxed_pnl"),
            "short_improved": (to_float(short.get("taxed_pnl"), -10**18) or -10**18) > h5_pnl,
            "trend_improved": (to_float(trend.get("taxed_pnl"), -10**18) or -10**18) > h5_pnl,
            "mix_improved": (to_float(mix.get("taxed_pnl"), -10**18) or -10**18) > h5_pnl,
            "mix_dd_improved": (to_float(mix.get("max_dd"), 10**18) or 10**18) < (to_float(row.get("max_dd"), 0.0) or 0.0),
        })
    return out


def complement_effect(monthly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_case_month = {(r["case_key"], r["year_month"]): r for r in monthly_rows}
    months = sorted({r["year_month"] for r in monthly_rows})
    out = []
    for ym in months:
        h5 = by_case_month.get((CURRENT_CASE, ym), {})
        if not h5:
            continue
        h5_pnl = to_float(h5.get("taxed_pnl"), 0.0) or 0.0
        h5_dd = to_float(h5.get("max_dd"), 0.0) or 0.0
        for case in [SHORT_CASE, CURRENT7_SHORT3_CASE, TREND_ALIAS, MIX_TREND_CASE, "BOX", "SUPPORT"]:
            other = by_case_month.get((case, ym), {})
            if not other:
                continue
            other_pnl = to_float(other.get("taxed_pnl"), 0.0) or 0.0
            other_dd = to_float(other.get("max_dd"), 0.0) or 0.0
            out.append({
                "year_month": ym,
                "case_key": case,
                "h5_taxed_pnl": h5_pnl,
                "case_taxed_pnl": other_pnl,
                "h5_max_dd": h5_dd,
                "case_max_dd": other_dd,
                "h5_negative_case_positive": h5_pnl < 0 < other_pnl,
                "case_improved_pnl": other_pnl > h5_pnl,
                "case_improved_dd": other_dd < h5_dd,
                "case_strong_month": other_pnl > 0 and (to_float(other.get("PF"), 0.0) or 0.0) >= 1.5,
            })
    return out


def recent_period_summary(all_rows: list[dict[str, Any]], monthly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [parse_date(r.get("entry_date")) for r in all_rows]
    dates = [d for d in dates if d]
    if not dates:
        return []
    last = max(dates)
    h5_by_period: dict[str, float] = {}
    out = []
    for label, days in [("recent_1m", 31), ("recent_3m", 93), ("recent_6m", 186)]:
        start = last - timedelta(days=days)
        by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in all_rows:
            dt = parse_date(row.get("entry_date"))
            if dt and dt >= start:
                by_case[str(row.get("case_key"))].append(row)
        h5_value = None
        for case, rows in sorted(by_case.items()):
            rec = metric_row(case, label, rows, kind="group")
            rec["period"] = label
            if case == CURRENT_CASE:
                h5_value = to_float(rec.get("taxed_pnl"), 0.0) or 0.0
                h5_by_period[label] = h5_value
            out.append(rec)
        for rec in out:
            if rec.get("period") == label:
                rec["diff_vs_h5"] = (to_float(rec.get("taxed_pnl"), 0.0) or 0.0) - h5_by_period.get(label, 0.0)
    return out


def top_bottom_by_month(all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        groups[(str(row.get("case_key")), str(row.get("_year_month")))].append(row)
    out = []
    for (case, ym), rows in sorted(groups.items()):
        for side, ordered in [
            ("top", sorted(rows, key=lambda r: to_float(r.get("return_pct"), 0.0) or 0.0, reverse=True)[:10]),
            ("bottom", sorted(rows, key=lambda r: to_float(r.get("return_pct"), 0.0) or 0.0)[:10]),
        ]:
            for rank, row in enumerate(ordered, 1):
                out.append({
                    "year_month": ym,
                    "case_key": case,
                    "side": side,
                    "rank": rank,
                    "signal_date": row.get("signal_date"),
                    "entry_date": row.get("entry_date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "return_pct": row.get("return_pct"),
                    "pnl_after_cost": row.get("_pnl_after_cost"),
                })
    return out


def hd3_extension_rows(
    feature_rows: list[dict[str, Any]],
    exec_by_hd: dict[int, list[dict[str, Any]]],
    trend_case_key: str,
) -> list[dict[str, Any]]:
    if not exec_by_hd.get(3) or not exec_by_hd.get(5):
        return []
    conditions = cond_registry()
    cases = {c.case_key: c for c in build_cases(200000)}
    trend_case = cases.get(trend_case_key)
    hd3 = exec_by_hd[3]
    hd5_by_key = {
        (date_text(r.get("trade_date") or r.get("signal_date")), normalize_code(r.get("code"))): r
        for r in exec_by_hd[5]
    }
    for row in hd3:
        row["is_current_h5"] = current_h5_pass(row)
        row["is_short_h5"] = short_h5_pass(row)
    case_rows = {
        CURRENT_CASE: [r for r in hd3 if r.get("is_current_h5")],
        SHORT_CASE: [r for r in hd3 if r.get("is_short_h5")],
        CURRENT7_SHORT3_CASE: mixed_current7_short3([r for r in hd3 if r.get("is_current_h5") or r.get("is_short_h5")]),
    }
    if trend_case:
        case_rows[TREND_ALIAS] = [r for r in hd3 if base_case_pass(r, trend_case, conditions)]
    out = []
    for case, rows in case_rows.items():
        groups: dict[str, list[tuple[dict[str, Any], dict[str, Any] | None]]] = defaultdict(list)
        for row in rows:
            key = (date_text(row.get("trade_date") or row.get("signal_date")), normalize_code(row.get("code")))
            groups[year_month(row.get("entry_date") or row.get("trade_date"))].append((row, hd5_by_key.get(key)))
        for ym, pairs in sorted(groups.items()):
            profit = flat = loss = 0
            flat_improved = loss_breakeven = loss_improved = 0
            hd3_pnl = 0.0
            extension_pnl = 0.0
            for r3, r5 in pairs:
                ret3 = to_float(r3.get("return_pct"), 0.0) or 0.0
                pnl3 = after_cost_pnl(r3)
                pnl5 = after_cost_pnl(r5 or r3)
                hd3_pnl += pnl3
                if ret3 > 1.0:
                    profit += 1
                    extension_pnl += pnl3
                elif ret3 >= -1.0:
                    flat += 1
                    extension_pnl += pnl5
                    if pnl5 > pnl3:
                        flat_improved += 1
                else:
                    loss += 1
                    extension_pnl += pnl5
                    if (to_float((r5 or {}).get("return_pct"), ret3) or ret3) >= 0:
                        loss_breakeven += 1
                    if pnl5 > pnl3:
                        loss_improved += 1
            out.append({
                "year_month": ym,
                "case_key": case,
                "hd3_profit_count": profit,
                "hd3_flat_count": flat,
                "hd3_loss_count": loss,
                "flat_to_hd5_improve_rate": flat_improved / flat * 100 if flat else None,
                "loss_to_breakeven_rate": loss_breakeven / loss * 100 if loss else None,
                "loss_to_hd5_improve_rate": loss_improved / loss * 100 if loss else None,
                "hd3_fixed_pnl": hd3_pnl,
                "hd3_state_extension_pnl": extension_pnl,
                "extension_delta": extension_pnl - hd3_pnl,
            })
    return out


def darasage_summary(monthly_rows: list[dict[str, Any]], market_rows: list[dict[str, Any]], hd3_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    darasage_months = {r["year_month"] for r in market_rows if str(r.get("darasage")).lower() == "true"}
    ext_by_key = {(r["case_key"], r["year_month"]): r for r in hd3_rows}
    out = []
    for row in monthly_rows:
        if row.get("year_month") not in darasage_months:
            continue
        ext = ext_by_key.get((row.get("case_key"), row.get("year_month")), {})
        out.append({
            "year_month": row.get("year_month"),
            "case_key": row.get("case_key"),
            "count": row.get("count"),
            "avg_return_pct": row.get("avg_return_pct"),
            "PF": row.get("PF"),
            "taxed_pnl": row.get("taxed_pnl"),
            "max_dd": row.get("max_dd"),
            "hd3_extension_delta": ext.get("extension_delta"),
            "hd3_extension_helped": (to_float(ext.get("extension_delta"), 0.0) or 0.0) > 0,
        })
    return out


def monthly_vs_index_summary(monthly_rows: list[dict[str, Any]], market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    market = market_by_month(market_rows)
    out = []
    for row in sorted(monthly_rows, key=lambda r: (str(r.get("year_month")), str(r.get("case_key")))):
        ym = str(row.get("year_month") or "")
        m = market.get(ym, {})
        pnl_after_cost = to_float(row.get("pnl_after_cost"), 0.0) or 0.0
        taxed_pnl = to_float(row.get("taxed_pnl"), 0.0) or 0.0
        strategy_after_cost_return = pnl_after_cost / CAPITAL * 100.0
        strategy_taxed_return = taxed_pnl / CAPITAL * 100.0
        nikkei_ret = to_float(m.get("nikkei_month_return"))
        topix_ret = to_float(m.get("topix_month_return"))
        out.append({
            "year": row.get("year"),
            "month": row.get("month"),
            "year_month": ym,
            "case_key": row.get("case_key"),
            "count": row.get("count"),
            "active_days": row.get("active_days"),
            "avg_trade_return_pct": row.get("avg_return_pct"),
            "strategy_after_cost_return_pct": strategy_after_cost_return,
            "strategy_taxed_return_pct": strategy_taxed_return,
            "nikkei_month_return_pct": nikkei_ret,
            "topix_month_return_pct": topix_ret,
            "alpha_after_tax_vs_nikkei_pct": strategy_taxed_return - nikkei_ret if nikkei_ret is not None else "",
            "alpha_after_tax_vs_topix_pct": strategy_taxed_return - topix_ret if topix_ret is not None else "",
            "taxed_pnl": taxed_pnl,
            "max_dd": row.get("max_dd"),
            "PF": row.get("PF"),
            "win_rate": row.get("win_rate"),
            "bullish_index": m.get("bullish_index"),
            "weak_index": m.get("weak_index"),
            "value_rotation": m.get("value_rotation"),
            "darasage": m.get("darasage"),
            "crash_rebound": m.get("crash_rebound"),
            "index_data_note": m.get("classification_note"),
        })
    return out


def report_text(
    monthly_rows: list[dict[str, Any]],
    yearly_rows: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
    vs_index_rows: list[dict[str, Any]],
    bad_rows: list[dict[str, Any]],
    comp_rows: list[dict[str, Any]],
    darasage_rows: list[dict[str, Any]],
    recent_rows: list[dict[str, Any]],
    hd3_rows: list[dict[str, Any]],
    trend_case_key: str,
    target_cases: list[str],
    diagnostics: Counter,
) -> str:
    current = [r for r in monthly_rows if r.get("case_key") == CURRENT_CASE]
    strong = sorted(current, key=lambda r: to_float(r.get("taxed_pnl"), -10**18) or -10**18, reverse=True)[:5]
    weak = sorted(current, key=lambda r: to_float(r.get("taxed_pnl"), 10**18) or 10**18)[:5]
    short_comp = [r for r in comp_rows if r.get("case_key") == SHORT_CASE and str(r.get("h5_negative_case_positive")).lower() == "true"]
    trend_comp = [r for r in comp_rows if r.get("case_key") == TREND_ALIAS and str(r.get("h5_negative_case_positive")).lower() == "true"]
    mix_dd = [r for r in comp_rows if r.get("case_key") == MIX_TREND_CASE and str(r.get("case_improved_dd")).lower() == "true"]
    sox_rows = [r for r in market_rows if str(r.get("sox_down")).lower() == "true"]
    darasage_pnl = sum(to_float(r.get("taxed_pnl"), 0.0) or 0.0 for r in darasage_rows if r.get("case_key") == CURRENT_CASE)
    extension_help = [r for r in hd3_rows if (to_float(r.get("extension_delta"), 0.0) or 0.0) > 0]
    recent_h5 = [r for r in recent_rows if r.get("case_key") == CURRENT_CASE]
    h5_vs_index = [r for r in vs_index_rows if r.get("case_key") == CURRENT_CASE]
    h5_alpha_positive = [r for r in h5_vs_index if (to_float(r.get("alpha_after_tax_vs_nikkei_pct"), -10**18) or -10**18) > 0]
    h5_alpha_negative = [r for r in h5_vs_index if (to_float(r.get("alpha_after_tax_vs_nikkei_pct"), 10**18) or 10**18) < 0]
    lines = [
        "Strategy monthly/regime performance report",
        "",
        "Production impact: none. Read-only CSV analysis; no DB writes, LINE changes, actual_trade_logs changes, or auto-trading changes.",
        f"target_cases: {len(target_cases)} ({', '.join(target_cases)})",
        f"trend_support_best_source: {trend_case_key or 'not_available'}",
        f"period: {min((r.get('year_month') for r in monthly_rows), default='')} to {max((r.get('year_month') for r in monthly_rows), default='')}",
        "",
        "Regime classification logic:",
        "- bullish_index: available Nikkei monthly proxy > 0 and TOPIX proxy is blank or > 0.",
        "- weak_index: available Nikkei monthly proxy < 0 and TOPIX proxy is blank or < 0.",
        "- value_rotation: TOPIX proxy exceeds Nikkei proxy by more than 1 point.",
        "- darasage: monthly proxy is negative with few <= -2% daily proxy moves.",
        "- crash_rebound: month has a <= -3% daily proxy move and positive monthly proxy return.",
        "- NASDAQ/SOX/VIX/USDJPY are blank unless local source columns exist; see proxy_usage.csv.",
        "",
        "current_h5_core strong months:",
        *[f"- {r.get('year_month')}: taxed_pnl={to_float(r.get('taxed_pnl'), 0):,.0f}, PF={to_float(r.get('PF'), 0):.3f}, win={to_float(r.get('win_rate'), 0):.1f}%" for r in strong],
        "",
        "current_h5_core weak months:",
        *[f"- {r.get('year_month')}: taxed_pnl={to_float(r.get('taxed_pnl'), 0):,.0f}, PF={to_float(r.get('PF'), 0):.3f}, win={to_float(r.get('win_rate'), 0):.1f}%" for r in weak],
        "",
        f"H5 bad months flagged: {len(bad_rows)}",
        f"short_pullback complemented H5-negative months: {len(short_comp)}",
        f"trend_support complemented H5-negative months: {len(trend_comp)}",
        f"mix months with lower DD than H5: {len(mix_dd)}",
        f"darasage current_h5_core taxed_pnl total: {darasage_pnl:,.0f}",
        f"SOX down months available: {len(sox_rows)} (blank means SOX local series unavailable)",
        f"HD3 state extension helped rows/months: {len(extension_help)}",
        f"H5 months beating Nikkei proxy after tax: {len(h5_alpha_positive)} / {len(h5_vs_index)}",
        f"H5 months lagging Nikkei proxy after tax: {len(h5_alpha_negative)} / {len(h5_vs_index)}",
        "",
        "Recent H5 summary:",
        *[f"- {r.get('period')}: count={r.get('count')}, taxed_pnl={to_float(r.get('taxed_pnl'), 0):,.0f}, PF={to_float(r.get('PF'), 0):.3f}, DD={to_float(r.get('max_dd'), 0):,.0f}" for r in recent_h5],
        "",
        "Operational notes:",
        "- H5 weakness should be watched when monthly PF falls below 1, win rate falls below 45%, or darasage proxy is true.",
        "- Short/trend/mix are still comparison cases; use monthly forward-test stability before production promotion.",
        "- For SOX-shock Mondays, local SOX data is not available here, so treat semiconductor/growth weakness as an external manual warning until a SOX series is added.",
        "- Rule-change candidates: monitor H5 monthly stop conditions, and test HD3-to-HD5 extension only for flat/loss states, not blanket extension.",
        "",
        "Diagnostics:",
        *[f"- {k}: {v}" for k, v in sorted(diagnostics.items())],
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT)
    parser.add_argument("--trend-case", default="")
    parser.add_argument("--skip-trend-rebuild", action="store_true")
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    diagnostics: Counter = Counter()
    proxy_rows: list[dict[str, Any]] = []

    h5_rows = load_h5_cases()
    diagnostics["h5_case_rows"] = len(h5_rows)
    diagnostics["h5_case_source_exists"] = int(H5_CASE_ROWS.exists())

    selected_trend, trend_source = (args.trend_case, "cli") if args.trend_case else select_trend_case(PREFERRED_TREND_PREFIX)
    trend_rows: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    exec_by_hd: dict[int, list[dict[str, Any]]] = {}
    trend_stats: dict[str, Any] = {}
    if not args.skip_trend_rebuild and selected_trend:
        trend_rows, trend_stats, feature_rows, exec_by_hd = build_trend_rows(ROOT / args.input, out_dir, selected_trend)
    diagnostics["trend_rows"] = len(trend_rows)
    diagnostics["trend_source_mode"] = trend_source

    mix_rows = load_mix_rows()
    diagnostics["mix_rows"] = len(mix_rows)
    diagnostics["mix_source_exists"] = int(MIX_ROWS.exists())

    box_rows, box_status = load_optional_case(BOX_ROWS, "BOX", str(BOX_ROWS.relative_to(ROOT)))
    support_rows, support_status = load_optional_case(SUPPORT_ROWS, "SUPPORT", str(SUPPORT_ROWS.relative_to(ROOT)))
    diagnostics["box_rows"] = len(box_rows)
    diagnostics["support_rows"] = len(support_rows)

    all_rows = h5_rows + trend_rows + mix_rows + box_rows + support_rows
    all_rows = [r for r in all_rows if r.get("entry_date") and r.get("code")]
    diagnostics["all_trade_rows"] = len(all_rows)
    diagnostics["join_failed_missing_code_or_date"] = len(h5_rows + trend_rows + mix_rows + box_rows + support_rows) - len(all_rows)

    monthly_rows = monthly_summary(all_rows)
    yearly_rows = yearly_summary(all_rows)
    market_rows, market_proxy = market_months_from_rows(all_rows)
    proxy_rows.extend(market_proxy)
    add_h5_stress_proxy_regimes(market_rows, monthly_rows, proxy_rows)
    vs_index_rows = monthly_vs_index_summary(monthly_rows, market_rows)
    proxy_rows.extend([
        {"feature": "BOX", "status": box_status, "implementation": str(BOX_ROWS.relative_to(ROOT))},
        {"feature": "SUPPORT", "status": support_status, "implementation": str(SUPPORT_ROWS.relative_to(ROOT))},
        {"feature": "trend_support_best", "status": "available" if trend_rows else "not_available", "implementation": selected_trend},
        {"feature": "overheat_score fill", "status": "inherited", "implementation": "existing stored/deep backtest outputs; no DB recomputation"},
    ])

    bad_rows = h5_bad_months(monthly_rows, market_rows)
    comp_rows = complement_effect(monthly_rows)
    hd3_rows = hd3_extension_rows(feature_rows, exec_by_hd, selected_trend) if exec_by_hd else []
    darasage_rows = darasage_summary(monthly_rows, market_rows, hd3_rows)
    recent_rows = recent_period_summary(all_rows, monthly_rows)
    top_bottom_rows = top_bottom_by_month(all_rows)

    for k, v in trend_stats.items():
        diagnostics[str(k)] = v if isinstance(v, int) else str(v)
    target_cases = sorted({str(r.get("case_key")) for r in all_rows})

    write_csv(out_dir / "monthly_case_summary.csv", monthly_rows)
    write_csv(out_dir / "yearly_case_summary.csv", yearly_rows)
    write_csv(out_dir / "monthly_market_regime.csv", market_rows)
    write_csv(out_dir / "monthly_vs_index_summary.csv", vs_index_rows)
    write_csv(out_dir / "h5_bad_months.csv", bad_rows)
    write_csv(out_dir / "complement_effect_summary.csv", comp_rows)
    write_csv(out_dir / "hd3_extension_monthly.csv", hd3_rows)
    write_csv(out_dir / "darasage_regime_summary.csv", darasage_rows)
    write_csv(out_dir / "recent_period_summary.csv", recent_rows)
    write_csv(out_dir / "top_bottom_by_month.csv", top_bottom_rows)
    write_csv(out_dir / "join_diagnostics.csv", [{"metric": k, "value": v} for k, v in sorted(diagnostics.items())])
    write_csv(out_dir / "proxy_usage.csv", proxy_rows)
    write_text(
        out_dir / "report.txt",
        report_text(monthly_rows, yearly_rows, market_rows, vs_index_rows, bad_rows, comp_rows, darasage_rows, recent_rows, hd3_rows, selected_trend, target_cases, diagnostics),
    )

    print(f"output_dir={out_dir}")
    print(f"target_cases={len(target_cases)}")
    print(f"period={min((r.get('year_month') for r in monthly_rows), default='')}..{max((r.get('year_month') for r in monthly_rows), default='')}")
    print(f"h5_bad_months={len(bad_rows)}")
    print(f"trend_case={selected_trend}")
    print(f"darasage_rows={len(darasage_rows)}")
    print(f"hd3_extension_rows={len(hd3_rows)}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
