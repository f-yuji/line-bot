#!/usr/bin/env python3
"""Grid search trend-following and trend-pullback cases.

Research only. This script does not modify Primary/H5 production rules, LINE
notifications, actual_trade_logs, or auto-trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_h5_primary_fractional_sizing import standardize, to_float, write_csv, write_text  # noqa: E402
from analyze_h5_pullback_relaxation import (  # noqa: E402
    TAX_RATE,
    cache_key,
    enrich_rows,
    fetch_feature_rows,
    normalize_code,
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
DEFAULT_OUTPUT = "outputs/trend_following_grid_search"
FEATURE_CACHE = ROOT / "outputs/h5_pullback_relaxation/feature_cache.json"
LATEST_AUDIT = ROOT / "outputs/h5_tax_priority_today_audit/07_today_h5_evaluation_rows.csv"
LATEST_STORED = ROOT / "outputs/h5_stored_forward_test/latest_h5_candidates.csv"
H5_SUMMARY = ROOT / "outputs/h5_stored_forward_cases/case_summary.csv"

CAPITAL = 5_000_000.0
NOTIONAL = 300_000.0
COST_BPS = 10.0


@dataclass(frozen=True)
class Condition:
    key: str
    label: str
    fn: Callable[[dict[str, Any]], bool]


@dataclass(frozen=True)
class CaseDef:
    case_key: str
    family: str
    conditions: tuple[str, ...]
    score_min: float
    overheat_max: int | None
    margin_rule: str
    holding_days: int
    daily_cap: int
    gap_limit: float
    focus: bool = False


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def read_json(path: Path) -> dict[str, Any]:
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


def score(row: dict[str, Any]) -> float:
    return num(row.get("signal_probability") or row.get("score"), -1.0) or -1.0


def overheat(row: dict[str, Any]) -> int | None:
    value = num(row.get("overheat_score"))
    if value is not None:
        return int(value)
    try:
        return int(h5_overheat_score(row))
    except Exception:
        return None


def margin_ratio(row: dict[str, Any]) -> float | None:
    return num(row.get("margin_ratio"))


def margin_pass(row: dict[str, Any], rule: str) -> bool:
    m = margin_ratio(row)
    if rule == "none" or m is None:
        return True
    if rule == "lt3":
        return m < 3.0
    if rule == "lt5":
        return m < 5.0
    if rule == "gt10":
        return m > 10.0
    if rule == "range3_30":
        return 3.0 <= m <= 30.0
    return True


def load_feature_rows(input_path: Path, out_dir: Path) -> tuple[list[dict[str, Any]], Counter]:
    raw = read_csv(input_path)
    rows = [standardize(r) for r in raw]
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["_row_index"] = i
        row["code"] = normalize_code(row.get("code"))
        row["score_source"] = row.get("source") or "walk_forward"

    features = read_json(FEATURE_CACHE)
    stats = Counter({"shared_feature_cache_rows": len(features)})
    if not features:
        wanted = [r for r in rows if r.get("trade_date") and r.get("code")]
        features, stats = fetch_feature_rows(wanted, out_dir, compute_drop10=True)
    rows = enrich_rows(rows, features)
    add_daily_percentiles(rows)
    for row in rows:
        row["overheat_score"] = overheat(row)
    return rows, stats


def add_daily_percentiles(rows: list[dict[str, Any]]) -> None:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[str(row.get("trade_date") or "")].append(row)
    for items in by_day.values():
        valid = sorted(
            [r for r in items if num(r.get("return_5d_pct")) is not None],
            key=lambda r: num(r.get("return_5d_pct"), -999) or -999,
        )
        denom = max(1, len(valid) - 1)
        for rank, row in enumerate(valid):
            row["return5_percentile"] = rank / denom * 100.0
        valid_score = sorted(items, key=lambda r: score(r))
        denom_score = max(1, len(valid_score) - 1)
        for rank, row in enumerate(valid_score):
            row["score_percentile"] = rank / denom_score * 100.0


def cond_registry() -> dict[str, Condition]:
    return {
        "close_gt_ma25": Condition("close_gt_ma25", "close > MA25", lambda r: (num(r.get("ma25_gap_pct")) or -999) > 0),
        "close_gt_ma75": Condition("close_gt_ma75", "close > MA75", lambda r: (num(r.get("ma75_gap_pct")) or -999) > 0),
        "ma25_gt_ma75": Condition(
            "ma25_gt_ma75",
            "MA25 > MA75 proxy",
            lambda r: num(r.get("ma25_gap_pct")) is not None
            and num(r.get("ma75_gap_pct")) is not None
            and (num(r.get("ma25_gap_pct")) or 999) < (num(r.get("ma75_gap_pct")) or -999),
        ),
        "near_20d_high": Condition("near_20d_high", "20d high proximity proxy", lambda r: (num(r.get("drop_from_20d_high_pct")) or -999) >= -3),
        "high_update_20d": Condition("high_update_20d", "20d high update proxy", lambda r: (num(r.get("drop_from_20d_high_pct")) or -999) >= -0.5),
        "relative_strength_top": Condition("relative_strength_top", "return5 percentile >= 70", lambda r: (num(r.get("return5_percentile")) or -1) >= 70),
        "return_5d_pos": Condition("return_5d_pos", "return_5d > 0", lambda r: (num(r.get("return_5d_pct")) or -999) > 0),
        "return_20d_proxy_pos": Condition("return_20d_proxy_pos", "score/rule momentum proxy", lambda r: score(r) >= 0.55),
        "sector_strength_pos": Condition("sector_strength_pos", "sector strength positive", lambda r: (num(r.get("sector_change_pct")) or -999) > 0),
        "nikkei_up": Condition("nikkei_up", "Nikkei up day", lambda r: (num(r.get("nikkei_change_pct")) or -999) > 0),
        "topix_up": Condition("topix_up", "TOPIX up day", lambda r: (num(r.get("topix_change_pct")) or -999) > 0),
        "risk_on": Condition("risk_on", "risk-on regime", lambda r: str(r.get("market_regime") or "") in {"risk_on", "strong_risk_on", "normal"}),
        "drop5_lte_m3": Condition("drop5_lte_m3", "drop5 <= -3", lambda r: num(r.get("drop_from_5d_high_pct")) is not None and (num(r.get("drop_from_5d_high_pct")) or 999) <= -3),
        "drop10_lte_m5": Condition("drop10_lte_m5", "drop10 <= -5", lambda r: num(r.get("drop_from_10d_high_pct")) is not None and (num(r.get("drop_from_10d_high_pct")) or 999) <= -5),
        "light_pullback": Condition(
            "light_pullback",
            "light pullback -5 <= drop5 <= -1",
            lambda r: num(r.get("drop_from_5d_high_pct")) is not None
            and -5 <= (num(r.get("drop_from_5d_high_pct")) or 999) <= -1,
        ),
        "ma5_gap_negative": Condition("ma5_gap_negative", "ma5 gap negative", lambda r: (num(r.get("ma5_gap_pct")) or 999) < 0),
        "volume_high": Condition("volume_high", "volume ratio >= 1.5", lambda r: (num(r.get("volume_ratio_20d") or r.get("volume_ratio")) or -1) >= 1.5),
        "rsi_below_75": Condition("rsi_below_75", "RSI < 75", lambda r: num(r.get("rsi14")) is None or (num(r.get("rsi14")) or 999) < 75),
        "rsi_45_75": Condition("rsi_45_75", "45 <= RSI < 75", lambda r: num(r.get("rsi14")) is not None and 45 <= (num(r.get("rsi14")) or -1) < 75),
        "return5_not_extreme": Condition("return5_not_extreme", "return5 <= 20", lambda r: num(r.get("return_5d_pct")) is None or (num(r.get("return_5d_pct")) or 999) <= 20),
        "buying_not_extreme": Condition("buying_not_extreme", "margin ratio <= 10 if present", lambda r: margin_ratio(r) is None or (margin_ratio(r) or 999) <= 10),
    }


def base_case_pass(row: dict[str, Any], case: CaseDef, conditions: dict[str, Condition]) -> bool:
    if score(row) < case.score_min:
        return False
    hot = overheat(row)
    if case.overheat_max is not None and (hot is None or hot > case.overheat_max):
        return False
    if not margin_pass(row, case.margin_rule):
        return False
    return all(conditions[key].fn(row) for key in case.conditions)


def build_cases(max_cases: int) -> list[CaseDef]:
    focus_cases: list[CaseDef] = [
        CaseDef(
            "trend_pullback_v1", "focus_pullback",
            ("close_gt_ma25", "close_gt_ma75", "ma25_gt_ma75", "return_20d_proxy_pos", "drop5_lte_m3", "rsi_below_75"),
            0.55, 2, "lt3", 3, 10, 3.0, True,
        ),
        CaseDef(
            "trend_breakout_v1", "focus_breakout",
            ("near_20d_high", "return_5d_pos", "volume_high", "rsi_below_75", "buying_not_extreme"),
            0.55, 3, "none", 3, 10, 3.0, True,
        ),
        CaseDef(
            "trend_low_credit_v1", "focus_credit",
            ("close_gt_ma25", "close_gt_ma75", "ma25_gt_ma75", "relative_strength_top", "rsi_45_75"),
            0.55, 3, "lt3", 3, 10, 3.0, True,
        ),
    ]

    trend_sets = [
        ("trend_ma25", ("close_gt_ma25",)),
        ("trend_ma25_ma75", ("close_gt_ma25", "close_gt_ma75")),
        ("trend_stack", ("close_gt_ma25", "close_gt_ma75", "ma25_gt_ma75")),
        ("trend_high_near", ("near_20d_high", "return_5d_pos")),
        ("trend_breakout", ("high_update_20d", "volume_high")),
        ("trend_rs", ("relative_strength_top", "close_gt_ma25")),
    ]
    momentum_sets = [
        ("mom_none", ()),
        ("mom_ret5", ("return_5d_pos",)),
        ("mom_sector", ("sector_strength_pos",)),
        ("mom_market", ("nikkei_up", "topix_up")),
    ]
    pullback_sets = [
        ("pb_none", ()),
        ("pb_light", ("light_pullback",)),
        ("pb_drop5", ("drop5_lte_m3",)),
        ("pb_drop10", ("drop10_lte_m5",)),
        ("pb_ma5neg", ("ma5_gap_negative",)),
    ]
    heat_rules = [1, 2, 3, None]
    margin_rules = ["none", "lt3", "lt5", "gt10", "range3_30"]
    score_mins = [0.50, 0.55, 0.60, 0.65]
    horizons = [1, 3, 5]
    daily_caps = [5, 10]
    gap_limits = [1.0, 3.0, 5.0]

    generated: list[CaseDef] = []
    idx = 1
    for trend_name, trend in trend_sets:
        for mom_name, mom in momentum_sets:
            for pb_name, pb in pullback_sets:
                for heat in heat_rules:
                    for margin in margin_rules:
                        for score_min in score_mins:
                            for hd in horizons:
                                for cap in daily_caps:
                                    for gap in gap_limits:
                                        conds = tuple(dict.fromkeys(trend + mom + pb + ("rsi_below_75", "return5_not_extreme")))
                                        heat_key = "none" if heat is None else str(heat)
                                        key = f"tf_{idx:04d}_{trend_name}_{mom_name}_{pb_name}_oh{heat_key}_m{margin}_s{int(score_min*100)}_hd{hd}_dc{cap}_g{int(gap)}"
                                        generated.append(CaseDef(key, f"{trend_name}/{mom_name}/{pb_name}", conds, score_min, heat, margin, hd, cap, gap))
                                        idx += 1
    room = max(0, max_cases - len(focus_cases))
    if room and len(generated) > room:
        step = len(generated) / room
        sampled = [generated[min(len(generated) - 1, int(i * step))] for i in range(room)]
    else:
        sampled = generated[:room] if room else []
    return focus_cases + sampled


def execute_all_horizons(rows: list[dict[str, Any]], input_path: Path) -> tuple[dict[int, list[dict[str, Any]]], Counter]:
    all_dates = load_all_wf_dates(input_path)
    date_by_signal = next_date_map(all_dates)
    cache_path = ROOT / "outputs/h5_s_share_execution_timing/next_open_cache.json"
    open_cache, open_stats = load_next_open_rows(rows, date_by_signal, cache_path)
    out: dict[int, list[dict[str, Any]]] = {}
    stats = Counter(open_stats)
    for hd in [1, 3, 5]:
        args = argparse.Namespace(holding_days=hd, stop_pct=-12.0)
        _, next_rows, skipped = make_execution_rows(rows, open_cache, date_by_signal, args)
        for row in next_rows:
            row["holding_days"] = hd
        out[hd] = next_rows
        stats += Counter(skipped)
    return out, stats


def sim_params(case: CaseDef) -> dict[str, Any]:
    return {
        "scenario_id": case.case_key,
        "capital": CAPITAL,
        "notional": NOTIONAL,
        "daily_cap": case.daily_cap,
        "gap_limit": case.gap_limit,
        "tax_rate": 0.0,
        "cost_bps": COST_BPS,
        "apply_tax": False,
        "entry_mode": "next_open",
    }


def summarize(case: CaseDef, sim: dict[str, Any], selected_count: int, start: str, end: str) -> dict[str, Any]:
    s = dict(sim["summary"])
    after_cost = num(s.get("total_pnl_after_tax"), 0.0) or 0.0
    aggregate_tax = max(after_cost, 0.0) * TAX_RATE
    temp = dict(s)
    temp["total_pnl_after_tax"] = after_cost - aggregate_tax
    annualize(temp, start, end)
    daily = [num(r.get("daily_realized_pnl"), 0.0) or 0.0 for r in sim.get("curve", [])]
    sharpe = mean(daily) / pstdev(daily) * (252 ** 0.5) if len(daily) > 2 and pstdev(daily) else None
    return {
        "case_key": case.case_key,
        "family": case.family,
        "conditions": ";".join(case.conditions),
        "score_min": case.score_min,
        "overheat_max": "none" if case.overheat_max is None else case.overheat_max,
        "margin_rule": case.margin_rule,
        "holding_days": case.holding_days,
        "daily_cap": case.daily_cap,
        "gap_limit": case.gap_limit,
        "candidate_count": selected_count,
        "count": s.get("executed_count"),
        "active_days": len({r.get("entry_date") for r in sim.get("executed", [])}),
        "avg_return_pct": s.get("avg_return_pct"),
        "median_return_pct": s.get("median_return_pct"),
        "win_rate": s.get("win_rate"),
        "PF": s.get("PF_after_tax"),
        "pretax_pnl": s.get("total_pnl_before_tax"),
        "pnl_after_cost": after_cost,
        "aggregate_tax": aggregate_tax,
        "taxed_pnl": after_cost - aggregate_tax,
        "CAGR": temp.get("annualized_compound_return"),
        "max_dd": s.get("max_dd_after_tax"),
        "sharpe_like": sharpe,
        "max_loss_streak": s.get("max_consecutive_losses"),
        "focus": case.focus,
    }


def top_bottom(case: CaseDef, rows: list[dict[str, Any]], n: int = 10) -> list[dict[str, Any]]:
    out = []
    for side, items in [
        ("top", sorted(rows, key=lambda r: num(r.get("return_pct"), -999) or -999, reverse=True)[:n]),
        ("bottom", sorted(rows, key=lambda r: num(r.get("return_pct"), 999) or 999)[:n]),
    ]:
        for rank, row in enumerate(items, 1):
            out.append({
                "case_key": case.case_key,
                "side": side,
                "rank": rank,
                "signal_date": row.get("trade_date") or row.get("signal_date"),
                "entry_date": row.get("entry_date"),
                "code": row.get("code"),
                "name": row.get("name"),
                "score": row.get("signal_probability"),
                "return_pct": row.get("return_pct"),
                "pnl_after_cost": row.get("_pnl_after_cost"),
                "sector": row.get("sector"),
                "market_regime": row.get("market_regime"),
            })
    return out


def market_split(case: CaseDef, executed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in executed:
        regime = str(row.get("market_regime") or "unknown")
        groups[f"regime:{regime}"].append(row)
        nikkei = num(row.get("nikkei_change_pct"))
        if nikkei is not None:
            groups["nikkei_up" if nikkei > 0 else "nikkei_down"].append(row)
        sector = str(row.get("sector") or "")
        if any(word in sector for word in ["電機", "精密", "情報", "通信"]):
            groups["ai_semiconductor_proxy_sector"].append(row)
    out = []
    for name, rows in groups.items():
        pnls = [num(r.get("_pnl_after_cost"), 0.0) or 0.0 for r in rows]
        returns = [num(r.get("return_pct"), 0.0) or 0.0 for r in rows]
        out.append({
            "case_key": case.case_key,
            "segment": name,
            "count": len(rows),
            "avg_return_pct": mean(returns) if returns else None,
            "win_rate": sum(v > 0 for v in pnls) / len(pnls) * 100 if pnls else None,
            "PF": pf(pnls),
            "pnl_after_cost": sum(pnls),
        })
    return out


def latest_rows(out_dir: Path) -> list[dict[str, Any]]:
    audit_rows = [standardize(r) for r in read_csv(LATEST_AUDIT)]
    stored_rows = [standardize(r) for r in read_csv(LATEST_STORED)]
    audit_date = max((str(r.get("trade_date") or "")[:10] for r in audit_rows), default="")
    stored_date = max((str(r.get("trade_date") or "")[:10] for r in stored_rows), default="")
    rows = stored_rows if stored_rows and stored_date >= audit_date else audit_rows
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["code"] = normalize_code(row.get("code"))
    features = read_json(FEATURE_CACHE)
    rows = enrich_rows(rows, features)
    missing = [r for r in rows if r.get("trade_date") and r.get("code") and r.get("drop_from_5d_high_pct") in (None, "")]
    if missing:
        try:
            fetched, _ = fetch_feature_rows(rows, out_dir, compute_drop10=True)
            merged = dict(features)
            merged.update(fetched)
            rows = enrich_rows(rows, merged)
        except Exception:
            pass
    add_daily_percentiles(rows)
    for row in rows:
        row["overheat_score"] = overheat(row)
    return rows


def first_failed_condition(row: dict[str, Any], case: CaseDef, conditions: dict[str, Condition]) -> str:
    if score(row) < case.score_min:
        return f"score<{case.score_min}"
    hot = overheat(row)
    if case.overheat_max is not None and (hot is None or hot > case.overheat_max):
        return f"overheat>{case.overheat_max}"
    if not margin_pass(row, case.margin_rule):
        return f"margin_rule:{case.margin_rule}"
    for key in case.conditions:
        if not conditions[key].fn(row):
            return key
    return ""


def latest_candidates(rows: list[dict[str, Any]], cases: list[CaseDef], conditions: dict[str, Condition]) -> list[dict[str, Any]]:
    out = []
    target_cases: list[CaseDef] = []
    seen_cases: set[str] = set()
    for case in [c for c in cases if c.focus] + [c for c in cases if not c.focus][:7]:
        if case.case_key in seen_cases:
            continue
        seen_cases.add(case.case_key)
        target_cases.append(case)
    for case in target_cases:
        passed = [r for r in rows if base_case_pass(r, case, conditions)]
        passed = sorted(passed, key=lambda r: int(num(r.get("_source_row_index"), 0) or 0))[:case.daily_cap]
        if passed:
            for row in passed:
                out.append({
                    "case_key": case.case_key,
                    "signal_date": row.get("trade_date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "score": row.get("signal_probability") or row.get("score"),
                    "trend_conditions": ";".join([k for k in case.conditions if k.startswith(("close_", "ma", "near_", "high_", "relative_"))]),
                    "momentum_conditions": ";".join([k for k in case.conditions if k.startswith(("return_", "sector_", "nikkei", "topix", "risk"))]),
                    "drop_conditions": ";".join([k for k in case.conditions if k.startswith(("drop", "light_", "ma5"))]),
                    "overheat": row.get("overheat_score"),
                    "credit": row.get("margin_ratio"),
                    "sector": row.get("sector"),
                    "adoption_reason": "trend grid candidate",
                    "exclusion_reason": "",
                })
        else:
            near = sorted(rows, key=lambda r: (first_failed_condition(r, case, conditions), -score(r)))[:5]
            for row in near:
                out.append({
                    "case_key": case.case_key,
                    "signal_date": row.get("trade_date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "score": row.get("signal_probability") or row.get("score"),
                    "trend_conditions": ";".join([k for k in case.conditions if k.startswith(("close_", "ma", "near_", "high_", "relative_"))]),
                    "momentum_conditions": ";".join([k for k in case.conditions if k.startswith(("return_", "sector_", "nikkei", "topix", "risk"))]),
                    "drop_conditions": ";".join([k for k in case.conditions if k.startswith(("drop", "light_", "ma5"))]),
                    "overheat": row.get("overheat_score"),
                    "credit": row.get("margin_ratio"),
                    "sector": row.get("sector"),
                    "adoption_reason": "",
                    "exclusion_reason": first_failed_condition(row, case, conditions),
                })
    return out


def proxy_usage_rows() -> list[dict[str, Any]]:
    return [
        {"requested_feature": "close > MA25/MA75", "implementation": "ma25_gap_pct/ma75_gap_pct", "status": "available"},
        {"requested_feature": "MA25 > MA75", "implementation": "derived from close-to-MA gaps", "status": "proxy"},
        {"requested_feature": "MA200 / MA75 > MA200", "implementation": "not in current WF/cache", "status": "not_available"},
        {"requested_feature": "52-week high proximity / breakout", "implementation": "20-day high proximity proxy", "status": "proxy"},
        {"requested_feature": "relative strength", "implementation": "daily return_5d_pct percentile", "status": "proxy"},
        {"requested_feature": "return_10d / return_20d", "implementation": "score/rule momentum proxy where needed", "status": "proxy"},
        {"requested_feature": "credit ratio", "implementation": "margin_ratio", "status": "available"},
        {"requested_feature": "credit balance delta / short sale ratio / JSF", "implementation": "not in current WF/cache", "status": "not_available"},
        {"requested_feature": "AI/semiconductor leadership day", "implementation": "electric/precision/info sector proxy", "status": "proxy"},
    ]


def report(summary: list[dict[str, Any]], h5_rows: list[dict[str, Any]], latest_count: int) -> str:
    ranked = sorted(summary, key=lambda r: (num(r.get("taxed_pnl"), -10**18) or -10**18, num(r.get("PF"), 0) or 0), reverse=True)
    focus = [r for r in summary if str(r.get("focus")).lower() == "true" or r.get("focus") is True]
    lines = [
        "Trend following grid search report",
        "",
        "Production impact: no Primary/H5/LINE/actual_trade_logs/auto-trading changes.",
        "",
        "Top cases by aggregate-taxed PnL:",
    ]
    for row in ranked[:10]:
        lines.append(
            f"- {row.get('case_key')}: n={row.get('count')} PF={num(row.get('PF'), 0):.3f} "
            f"avg={num(row.get('avg_return_pct'), 0):.2f}% taxed_pnl={num(row.get('taxed_pnl'), 0):,.0f} "
            f"DD={num(row.get('max_dd'), 0):,.0f} hd={row.get('holding_days')} cap={row.get('daily_cap')} gap={row.get('gap_limit')}"
        )
    lines.append("")
    lines.append("Focus cases:")
    for row in focus:
        lines.append(
            f"- {row.get('case_key')}: n={row.get('count')} PF={num(row.get('PF'), 0):.3f} "
            f"taxed_pnl={num(row.get('taxed_pnl'), 0):,.0f} DD={num(row.get('max_dd'), 0):,.0f}"
        )
    if h5_rows:
        lines.append("")
        lines.append("H5 comparison loaded from outputs/h5_stored_forward_cases/case_summary.csv.")
    lines.extend([
        "",
        f"latest trend candidate/near-miss rows: {latest_count}",
        "",
        "Caution: MA200, 52-week high, credit deltas, short-sale ratio, and true AI/semiconductor leadership are proxies or unavailable in the current local feature set.",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT)
    parser.add_argument("--max-cases", type=int, default=1200)
    parser.add_argument("--min-candidates", type=int, default=30)
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    input_path = ROOT / args.input
    conditions = cond_registry()
    rows, feature_stats = load_feature_rows(input_path, out_dir)
    cases = build_cases(args.max_cases)
    exec_by_hd, exec_stats = execute_all_horizons(rows, input_path)
    all_exec = [r for rows2 in exec_by_hd.values() for r in rows2]
    start = min((str(r.get("entry_date") or "") for r in all_exec if r.get("entry_date")), default="")
    end = max((str(r.get("exit_date") or r.get("entry_date") or "") for r in all_exec if r.get("exit_date") or r.get("entry_date")), default="")

    summary_rows = []
    top_rows = []
    market_rows = []
    detail_rows = []
    sims: dict[str, dict[str, Any]] = {}
    for case in cases:
        candidates = [r for r in exec_by_hd[case.holding_days] if base_case_pass(r, case, conditions)]
        if len(candidates) < args.min_candidates and not case.focus:
            continue
        sim = simulate_realistic(candidates, sim_params(case))
        sims[case.case_key] = sim
        rec = summarize(case, sim, len(candidates), start, end)
        summary_rows.append(rec)
        top_rows.extend(top_bottom(case, sim.get("executed", []), 10))
        if case.focus:
            market_rows.extend(market_split(case, sim.get("executed", [])))
            for row in sim.get("executed", [])[:500]:
                detail_rows.append({
                    "case_key": case.case_key,
                    "signal_date": row.get("trade_date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "entry_date": row.get("entry_date"),
                    "exit_date": row.get("exit_date"),
                    "return_pct": row.get("return_pct"),
                    "pnl_after_cost": row.get("_pnl_after_cost"),
                    "score": row.get("signal_probability"),
                    "sector": row.get("sector"),
                    "market_regime": row.get("market_regime"),
                })

    summary_rows = sorted(summary_rows, key=lambda r: num(r.get("taxed_pnl"), -10**18) or -10**18, reverse=True)
    h5_rows = read_csv(H5_SUMMARY)
    h5_compare = []
    for row in h5_rows:
        h5_compare.append({
            "group": "H5",
            "case_key": row.get("case_key"),
            "count": row.get("count"),
            "avg_return_pct": row.get("avg_return_pct"),
            "PF": row.get("PF"),
            "taxed_pnl": row.get("pnl_after_aggregate_tax"),
            "max_dd": row.get("max_dd"),
            "CAGR": row.get("CAGR"),
        })
    for row in summary_rows[:30]:
        h5_compare.append({
            "group": "trend",
            "case_key": row.get("case_key"),
            "count": row.get("count"),
            "avg_return_pct": row.get("avg_return_pct"),
            "PF": row.get("PF"),
            "taxed_pnl": row.get("taxed_pnl"),
            "max_dd": row.get("max_dd"),
            "CAGR": row.get("CAGR"),
        })

    latest = latest_candidates(latest_rows(out_dir), [c for c in cases if c.focus] + cases[:7], conditions)
    write_text(out_dir / "00_input_summary.txt", f"""trend following grid search
input: {input_path}
rows_loaded: {len(rows)}
cases_defined: {len(cases)}
cases_reported: {len(summary_rows)}
min_candidates: {args.min_candidates}
feature_stats: {dict(feature_stats)}
execution_stats: {dict(exec_stats)}
entry: next_open
holding_days: HD1/HD3/HD5
capital: 5M
notional: 300k S-share
cost_bps: 10
aggregate_tax_rate: {TAX_RATE}
production_changes: none
""")
    write_csv(out_dir / "01_grid_summary.csv", summary_rows)
    write_csv(out_dir / "02_top_bottom.csv", top_rows)
    write_csv(out_dir / "03_focus_case_daily_rows.csv", detail_rows)
    write_csv(out_dir / "04_focus_market_split.csv", market_rows)
    write_csv(out_dir / "05_h5_vs_trend_comparison.csv", h5_compare)
    write_csv(out_dir / "06_latest_candidates.csv", latest)
    write_csv(out_dir / "07_proxy_usage.csv", proxy_usage_rows())
    write_text(out_dir / "08_report.txt", report(summary_rows, h5_rows, len(latest)))

    print(f"output_dir={out_dir}")
    print(f"cases_reported={len(summary_rows)}")
    if summary_rows:
        print(f"best_case={summary_rows[0].get('case_key')}")
        print(f"best_taxed_pnl={summary_rows[0].get('taxed_pnl')}")
    print(f"latest_rows={len(latest)}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
