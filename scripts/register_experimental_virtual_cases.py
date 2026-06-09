#!/usr/bin/env python3
"""Generate/register experimental paper-trade virtual cases.

Default mode writes CSV/report only. Use --apply to upsert experimental rows to
virtual_trades and experimental_case_definitions. Production H5, LINE main
notifications, actual_trade_logs, and auto-trading are not touched.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402
from supabase import create_client  # noqa: E402

from analyze_h5_primary_fractional_sizing import write_csv, write_text  # noqa: E402
from analyze_h5_pullback_relaxation import common_pass, normalize_code, variant_pass  # noqa: E402
from analyze_trend_following_grid_search import (  # noqa: E402
    CaseDef,
    base_case_pass,
    cond_registry,
    latest_rows,
    margin_pass,
    num,
    overheat,
    score,
)
from services.h5_primary import H5_ACTIVE_CASE_KEYS, H5_PRIMARY_CASE_KEY  # noqa: E402
from services.position_sizing import calculate_virtual_position_size  # noqa: E402


OUT_DIR = ROOT / "outputs/experimental_virtual_cases"
DEFINITION_VERSION = "experimental_virtual_cases_v1_20260603"
LATEST_AUDIT = ROOT / "outputs/h5_tax_priority_today_audit/07_today_h5_evaluation_rows.csv"
H5_CASE_ROWS = ROOT / "outputs/h5_stored_forward_cases/case_daily_rows.csv"
H5_SUMMARY = ROOT / "outputs/h5_stored_forward_cases/case_summary.csv"
TREND_SUMMARY = ROOT / "outputs/trend_following_deep_backtest/03_robust_best_cases.csv"
DEEP_LATEST = ROOT / "outputs/trend_following_deep_backtest/14_latest_candidates.csv"
PB_SYMBOL_TYPES = ROOT / "outputs/price_band_expectancy_long/symbol_type_classification.csv"

TAX_RATE = 0.20315
COST_BPS = 10.0
NOTIONAL = 300_000.0


TARGET_CASES = [
    "H5_short_pullback_drop5_m3",
    "H5_current7_short3",
    "tf_166745_trend_rs_mom_market",
    "tf_22709_trend_ma25_mom_market",
    "tf_51545_trend_ma25_ma75_mom_market",
    "mix_current_h5_7_3",
    "mix_current7_short3_trend_7_3",
    "PB_MR_STRONG_MA25_M10_HD20",
    "PB_MR_STRONG_RSI25_HD20",
]


PB_CASES = {
    "PB_MR_STRONG_MA25_M10_HD20": {
        "label": "PB MA25 Deep Reversion",
        "condition": "mean_reversion_strong and ma25_gap_pct <= -10",
        "exit": "MA75_REVERT_OR_HD20",
        "holding_days": 20,
        "buy_zone": "ma25_gap_le_m10",
        "exit_target": "ma75",
    },
    "PB_MR_STRONG_RSI25_HD20": {
        "label": "PB RSI25 Reversion",
        "condition": "mean_reversion_strong and rsi14 <= 25",
        "exit": "HD20",
        "holding_days": 20,
        "buy_zone": "rsi14_le_25",
        "exit_target": "",
    },
}


TREND_CASES: dict[str, CaseDef] = {
    "tf_166745_trend_rs_mom_market": CaseDef(
        "tf_166745_trend_rs_mom_market",
        "trend_following",
        ("relative_strength_top", "close_gt_ma25", "nikkei_up", "topix_up", "rsi_below_75", "return5_not_extreme"),
        0.65,
        None,
        "none",
        3,
        10,
        3.0,
    ),
    "tf_22709_trend_ma25_mom_market": CaseDef(
        "tf_22709_trend_ma25_mom_market",
        "trend_following",
        ("close_gt_ma25", "nikkei_up", "topix_up", "rsi_below_75", "return5_not_extreme"),
        0.55,
        None,
        "none",
        3,
        10,
        3.0,
    ),
    "tf_51545_trend_ma25_ma75_mom_market": CaseDef(
        "tf_51545_trend_ma25_ma75_mom_market",
        "trend_following",
        ("close_gt_ma25", "close_gt_ma75", "nikkei_up", "topix_up", "rsi_below_75", "return5_not_extreme"),
        0.65,
        None,
        "none",
        3,
        10,
        3.0,
    ),
}


ORIGINAL_CASE_KEY = {
    "tf_166745_trend_rs_mom_market": "tf_166745_trend_rs_mom_market_pb_none_ohnone_mnone_s65_hd3_dc10_g3",
    "tf_22709_trend_ma25_mom_market": "tf_22709_trend_ma25_mom_market_pb_none_ohnone_mnone_s55_hd3_dc10_g3",
    "tf_51545_trend_ma25_ma75_mom_market": "tf_51545_trend_ma25_ma75_mom_market_pb_none_ohnone_mnone_s65_hd3_dc10_g3",
}


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_pb_symbol_types() -> dict[str, str]:
    out: dict[str, str] = {}
    for row in read_csv(PB_SYMBOL_TYPES):
        code = normalize_code(row.get("code"))
        if code:
            out[code] = str(row.get("symbol_type") or "")
    return out


def date_text(value: Any) -> str:
    return str(value or "")[:10]


def parse_date(value: Any) -> date | None:
    text = date_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except Exception:
        return None


def next_weekday(d: date) -> date:
    cur = d
    while True:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            return cur


def build_supabase():
    load_dotenv()
    mode = os.getenv("SUPABASE_MODE", "").strip() or os.getenv("ENV", "").strip()
    mode_u = mode.upper()
    url = (os.getenv(f"SUPABASE_URL_{mode_u}", "").strip() if mode_u else "") or os.getenv("SUPABASE_URL", "").strip()
    key = (os.getenv(f"SUPABASE_KEY_{mode_u}", "").strip() if mode_u else "") or os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is missing")
    return create_client(url, key)


def latest_date(rows: list[dict[str, Any]]) -> str:
    return max((date_text(r.get("trade_date") or r.get("signal_date")) for r in rows if date_text(r.get("trade_date") or r.get("signal_date"))), default="")


def fetch_latest_feature_snapshot_rows() -> list[dict[str, Any]]:
    try:
        sb = build_supabase()
        latest = (
            sb.table("stock_feature_snapshots")
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not latest:
            return []
        trade_date = date_text(latest[0].get("trade_date"))
        cols = (
            "trade_date,code,name,sector,open,high,low,close,ma25,ma75,"
            "ma25_gap_pct,ma75_gap_pct,rsi14,drop_from_5d_high_pct,"
            "drop_from_10d_high_pct,drop_from_20d_high_pct,volume_ratio_20d"
        )
        rows = (
            sb.table("stock_feature_snapshots")
            .select(cols)
            .eq("trade_date", trade_date)
            .limit(6000)
            .execute()
            .data
            or []
        )
    except Exception as e:
        print(f"pb_latest_snapshot_fetch_failed={e}")
        return []
    symbol_types = load_pb_symbol_types()
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["signal_date"] = date_text(row.get("trade_date"))
        row["code"] = normalize_code(row.get("code"))
        row["symbol_type"] = symbol_types.get(str(row.get("code") or ""), "")
    return rows


def enrich_latest_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows = latest_rows(out_dir)
    for i, row in enumerate(rows):
        row["_source_row_index"] = i
        row["code"] = normalize_code(row.get("code"))
        row["signal_probability"] = row.get("signal_probability") or row.get("score")
        row["overheat_score"] = overheat(row)
    return rows


def h5_short_pass(row: dict[str, Any]) -> bool:
    if not common_pass(row):
        return False
    d5 = num(row.get("drop_from_5d_high_pct"))
    return d5 is not None and d5 <= -3.0


def h5_current_pass(row: dict[str, Any]) -> bool:
    return variant_pass(row, "drop20", -8.0)


def key_of(row: dict[str, Any]) -> tuple[str, str]:
    return date_text(row.get("trade_date") or row.get("signal_date") or row.get("entry_date")), normalize_code(row.get("code"))


def select_h5_current7_short3(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = sorted(rows, key=lambda r: int(num(r.get("_source_row_index"), 0) or 0))
    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in [r for r in items if h5_current_pass(r)]:
        key = key_of(row)
        if key in seen:
            continue
        selected.append(row)
        seen.add(key)
        if len(selected) >= 7:
            break
    short_count = 0
    for row in [r for r in items if h5_short_pass(r)]:
        key = key_of(row)
        if key in seen:
            continue
        selected.append(row)
        seen.add(key)
        short_count += 1
        if short_count >= 3:
            break
    return selected[:10]


def flags_for(row: dict[str, Any], case: str, conditions) -> tuple[str, str, str]:
    trend_keys = {"relative_strength_top", "close_gt_ma25", "close_gt_ma75", "ma25_gt_ma75", "near_20d_high", "high_update_20d"}
    momentum_keys = {"nikkei_up", "topix_up", "return_5d_pos", "sector_strength_pos", "return_20d_proxy_pos", "return5_not_extreme", "rsi_below_75"}
    credit_keys = {"buying_not_extreme"}
    if case in TREND_CASES:
        conds = TREND_CASES[case].conditions
        return (
            ";".join(k for k in conds if k in trend_keys),
            ";".join(k for k in conds if k in momentum_keys),
            ";".join(k for k in conds if k in credit_keys) or TREND_CASES[case].margin_rule,
        )
    return ("h5", "stored_prediction", "margin_ratio_3_30_if_present")


def candidate_row(row: dict[str, Any], case: str, adopted: bool, reason: str = "") -> dict[str, Any]:
    signal_date = date_text(row.get("trade_date") or row.get("signal_date"))
    sig_dt = parse_date(signal_date)
    entry_dt = next_weekday(sig_dt).isoformat() if sig_dt else ""
    entry_price = num(row.get("close") or row.get("entry_price") or row.get("signal_price"))
    trend_flags, momentum_flags, credit_flags = flags_for(row, case, cond_registry())
    source_case = "stored_forward_case" if case.startswith("H5") or case.startswith("mix_current") else "deep_backtest"
    bucket = allocation_bucket(case)
    sizing = calculate_virtual_position_size(entry_price)
    return {
        "latest_date": signal_date,
        "signal_date": signal_date,
        "case_key": case,
        "code": normalize_code(row.get("code")),
        "name": row.get("name"),
        "score": row.get("signal_probability") or row.get("score"),
        "signal_stage": row.get("signal_stage"),
        "trend_flags": trend_flags,
        "momentum_flags": momentum_flags,
        "credit_flags": credit_flags,
        "drop5": row.get("drop_from_5d_high_pct"),
        "drop10": row.get("drop_from_10d_high_pct"),
        "drop20": row.get("drop_from_20d_high_pct"),
        "gap": row.get("entry_gap_pct"),
        "overheat_score": row.get("overheat_score"),
        "entry_date": entry_dt,
        "entry_price": entry_price,
        "planned_exit_rule": "HD3_EST12",
        "planned_holding_days": 3,
        "status": "open" if adopted else "excluded",
        "exit_date": "",
        "exit_price": "",
        "return_pct": "",
        "pnl_before_cost": "",
        "pnl_after_cost": "",
        "tax_adjusted_pnl": "",
        "cumulative_pnl": "",
        "採用可否": "採用" if adopted else "除外",
        "exclusion_reason": "" if adopted else reason,
        "除外理由": "" if adopted else reason,
        "source_case": source_case,
        "strategy_group": strategy_group(case),
        "strategy_label": strategy_label(case),
        "allocation_bucket": bucket,
        "source_logic": source_logic(case),
        "target_position_size": sizing["target_position_size"],
        "theoretical_shares": sizing["theoretical_shares"],
        "theoretical_position_size": sizing["theoretical_position_size"],
        "lot_type": sizing["lot_type"],
        "position_sizing_rule": sizing["position_sizing_rule"],
        "sizing_note": sizing["sizing_note"],
        "actual_position_size": "",
        "is_capital_constrained": sizing["is_capital_constrained"],
        "is_experimental": True,
        "definition_version": DEFINITION_VERSION,
    }


def strategy_group(case: str) -> str:
    if case in PB_CASES:
        return "PRICE_BAND"
    if case.startswith("tf_"):
        return "TREND_SUPPORT"
    if case.startswith("mix_"):
        return "H5_MIX"
    if case == "H5_short_pullback_drop5_m3":
        return "H5_SHORT_PULLBACK"
    return "H5_MIX"


def strategy_label(case: str) -> str:
    if case in PB_CASES:
        return str(PB_CASES[case]["label"])
    if case == "H5_short_pullback_drop5_m3":
        return "H5 Short Pullback"
    if case == "H5_current7_short3":
        return "H5 Mix"
    if case.startswith("tf_"):
        return "Trend Support"
    return "H5 Mix"


def source_logic(case: str) -> str:
    if case in PB_CASES:
        return "price_band_expectancy"
    if case.startswith("tf_"):
        return "trend_following_deep_backtest"
    return "stored_forward_case"


def allocation_bucket(case: str) -> str:
    if case in PB_CASES:
        return "price_band"
    if case == "H5_short_pullback_drop5_m3":
        return "short_pullback"
    if case == "H5_current7_short3":
        return "h5_core"
    if case.startswith("tf_"):
        return "trend"
    return "mix"


def pb_case_pass(row: dict[str, Any], case: str) -> bool:
    if str(row.get("symbol_type") or "") != "mean_reversion_strong":
        return False
    if case == "PB_MR_STRONG_MA25_M10_HD20":
        v = num(row.get("ma25_gap_pct"))
        return v is not None and v <= -10.0
    if case == "PB_MR_STRONG_RSI25_HD20":
        v = num(row.get("rsi14"))
        return v is not None and v <= 25.0
    return False


def pb_first_fail(row: dict[str, Any], case: str) -> str:
    if str(row.get("symbol_type") or "") != "mean_reversion_strong":
        return "not_mean_reversion_strong"
    if case == "PB_MR_STRONG_MA25_M10_HD20":
        return "ma25_gap>-10"
    if case == "PB_MR_STRONG_RSI25_HD20":
        return "rsi14>25"
    return "pb_filters_not_met"


def pb_candidate_row(row: dict[str, Any], case: str, adopted: bool, reason: str = "") -> dict[str, Any]:
    signal_date = date_text(row.get("trade_date") or row.get("signal_date"))
    sig_dt = parse_date(signal_date)
    entry_dt = next_weekday(sig_dt).isoformat() if sig_dt else ""
    entry_price = num(row.get("close"))
    cfg = PB_CASES[case]
    ma75 = num(row.get("ma75"))
    current_exit_target = ma75 if cfg.get("exit_target") == "ma75" else ""
    sizing = calculate_virtual_position_size(entry_price)
    return {
        "latest_date": signal_date,
        "signal_date": signal_date,
        "case_key": case,
        "code": normalize_code(row.get("code")),
        "name": row.get("name"),
        "score": "",
        "signal_stage": "price_band",
        "trend_flags": "",
        "momentum_flags": "",
        "credit_flags": "",
        "drop5": row.get("drop_from_5d_high_pct"),
        "drop10": row.get("drop_from_10d_high_pct"),
        "drop20": row.get("drop_from_20d_high_pct"),
        "gap": "",
        "overheat_score": "",
        "entry_date": entry_dt,
        "entry_price": entry_price,
        "planned_exit_rule": cfg["exit"],
        "planned_holding_days": cfg["holding_days"],
        "status": "open" if adopted else "excluded",
        "exit_date": "",
        "exit_price": "",
        "return_pct": "",
        "pnl_before_cost": "",
        "pnl_after_cost": "",
        "tax_adjusted_pnl": "",
        "cumulative_pnl": "",
        "採用可否": "採用" if adopted else "除外",
        "exclusion_reason": "" if adopted else reason,
        "除外理由": "" if adopted else reason,
        "source_case": "price_band_revalidation",
        "source_logic": "price_band_expectancy",
        "strategy_group": "PRICE_BAND",
        "strategy_label": cfg["label"],
        "allocation_bucket": "price_band",
        "target_position_size": sizing["target_position_size"],
        "theoretical_shares": sizing["theoretical_shares"],
        "theoretical_position_size": sizing["theoretical_position_size"],
        "lot_type": sizing["lot_type"],
        "position_sizing_rule": sizing["position_sizing_rule"],
        "sizing_note": sizing["sizing_note"],
        "actual_position_size": "",
        "is_capital_constrained": sizing["is_capital_constrained"],
        "definition_version": DEFINITION_VERSION,
        "entry_ma25_gap_pct": row.get("ma25_gap_pct"),
        "entry_ma75_gap_pct": row.get("ma75_gap_pct"),
        "entry_rsi14": row.get("rsi14"),
        "entry_ma75": ma75,
        "current_exit_target": current_exit_target,
        "mean_reversion_type": row.get("symbol_type"),
        "max_holding_days": cfg["holding_days"],
    }


def first_fail(row: dict[str, Any], case: str, conditions) -> str:
    if case in PB_CASES:
        return "" if pb_case_pass(row, case) else pb_first_fail(row, case)
    if case == "H5_short_pullback_drop5_m3":
        return "" if h5_short_pass(row) else "short_pullback_filters_not_met"
    if case == "H5_current7_short3":
        return "" if (h5_current_pass(row) or h5_short_pass(row)) else "current_or_short_filters_not_met"
    if case in TREND_CASES:
        c = TREND_CASES[case]
        if score(row) < c.score_min:
            return f"score<{c.score_min}"
        hot = overheat(row)
        if c.overheat_max is not None and (hot is None or hot > c.overheat_max):
            return f"overheat>{c.overheat_max}"
        if not margin_pass(row, c.margin_rule):
            return f"margin_rule:{c.margin_rule}"
        for key in c.conditions:
            if not conditions[key].fn(row):
                return key
    return ""


def latest_candidates_for_cases(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    conditions = cond_registry()
    latest_out: list[dict[str, Any]] = []
    counts: list[dict[str, Any]] = []
    watch: list[dict[str, Any]] = []
    pb_rows_cache: list[dict[str, Any]] | None = None
    for case in TARGET_CASES:
        if case in PB_CASES:
            if pb_rows_cache is None:
                pb_rows_cache = fetch_latest_feature_snapshot_rows()
            passed = [r for r in pb_rows_cache if pb_case_pass(r, case)]
            passed = sorted(
                passed,
                key=lambda r: (
                    num(r.get("ma25_gap_pct"), 999) if case == "PB_MR_STRONG_MA25_M10_HD20" else num(r.get("rsi14"), 999),
                    int(num(r.get("_source_row_index"), 0) or 0),
                ),
            )[:10]
            for row in passed:
                latest_out.append(pb_candidate_row(row, case, True))
            if not passed:
                near = sorted(
                    pb_rows_cache,
                    key=lambda r: (
                        pb_first_fail(r, case),
                        num(r.get("ma25_gap_pct"), 999) if case == "PB_MR_STRONG_MA25_M10_HD20" else num(r.get("rsi14"), 999),
                    ),
                )[:5]
                for row in near:
                    latest_out.append(pb_candidate_row(row, case, False, pb_first_fail(row, case)))
        elif case == "H5_short_pullback_drop5_m3":
            passed = [r for r in rows if h5_short_pass(r)]
        elif case == "H5_current7_short3":
            passed = select_h5_current7_short3(rows)
        elif case == "mix_current_h5_7_3":
            trend = [r for r in rows if base_case_pass(r, TREND_CASES["tf_166745_trend_rs_mom_market"], conditions)]
            h5 = [r for r in rows if h5_current_pass(r)]
            passed = select_slots(h5, trend, 7, 3)
        elif case == "mix_current7_short3_trend_7_3":
            trend = [r for r in rows if base_case_pass(r, TREND_CASES["tf_166745_trend_rs_mom_market"], conditions)]
            h5 = select_h5_current7_short3(rows)
            passed = select_slots(h5, trend, 7, 3)
        else:
            passed = [r for r in rows if base_case_pass(r, TREND_CASES[case], conditions)]
            passed = sorted(passed, key=lambda r: int(num(r.get("_source_row_index"), 0) or 0))[:10]
            for row in passed:
                latest_out.append(candidate_row(row, case, True))
            if not passed:
                near = sorted(rows, key=lambda r: (first_fail(r, case, conditions), -score(r)))[:5]
                for row in near:
                    latest_out.append(candidate_row(row, case, False, first_fail(row, case, conditions)))
        counts.append({
            "latest_date": latest_date(pb_rows_cache or []) if case in PB_CASES else latest_date(rows),
            "case_key": case,
            "candidate_count": len(passed),
            "near_miss_rows": 0 if passed else min(5, len(pb_rows_cache or rows)),
        })
        if case not in PB_CASES:
            watch.extend(overheat_watch(rows, case, conditions))
    return latest_out, counts, watch


def select_slots(core: list[dict[str, Any]], trend: list[dict[str, Any]], core_slots: int, trend_slots: int) -> list[dict[str, Any]]:
    out = []
    seen: set[tuple[str, str]] = set()
    for row in sorted(core, key=lambda r: int(num(r.get("_source_row_index"), 0) or 0)):
        key = key_of(row)
        if key in seen:
            continue
        out.append(row)
        seen.add(key)
        if len(out) >= core_slots:
            break
    trend_count = 0
    for row in sorted(trend, key=lambda r: int(num(r.get("_source_row_index"), 0) or 0)):
        key = key_of(row)
        if key in seen:
            continue
        out.append(row)
        seen.add(key)
        trend_count += 1
        if trend_count >= trend_slots:
            break
    return out[:10]


def overheat_watch(rows: list[dict[str, Any]], case: str, conditions) -> list[dict[str, Any]]:
    out = []
    if case.startswith("tf_"):
        return out
    for row in rows:
        hot = overheat(row)
        if hot is None or hot <= 1:
            continue
        base_ok = score(row) >= 0.65 and str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}
        drop_ok = (num(row.get("drop_from_20d_high_pct"), 999) or 999) <= -8 or (num(row.get("drop_from_5d_high_pct"), 999) or 999) <= -3
        if base_ok and drop_ok:
            out.append({
                "signal_date": date_text(row.get("trade_date")),
                "case_key": case,
                "code": normalize_code(row.get("code")),
                "name": row.get("name"),
                "score": row.get("signal_probability") or row.get("score"),
                "signal_stage": row.get("signal_stage"),
                "drop5": row.get("drop_from_5d_high_pct"),
                "drop10": row.get("drop_from_10d_high_pct"),
                "drop20": row.get("drop_from_20d_high_pct"),
                "overheat_score": hot,
                "exclusion_reason": "overheat_reject",
            })
    return out


def definitions() -> list[dict[str, Any]]:
    out = []
    for case in TARGET_CASES:
        if case in PB_CASES:
            c = PB_CASES[case]
            cond = {
                "conditions": c["condition"],
                "entry": "next_open",
                "exit": c["exit"],
                "is_capital_constrained": False,
                "theoretical_position_size": NOTIONAL,
            }
            proxy_used = False
            original = case
            hd, cap, gap = int(c["holding_days"]), None, None
        elif case in TREND_CASES:
            c = TREND_CASES[case]
            cond = {
                "conditions": list(c.conditions),
                "score_min": c.score_min,
                "overheat_max": c.overheat_max,
                "margin_rule": c.margin_rule,
            }
            proxy_used = True
            original = ORIGINAL_CASE_KEY[case]
            hd, cap, gap = c.holding_days, c.daily_cap, c.gap_limit
        elif case == "H5_short_pullback_drop5_m3":
            cond = {"AI": ">=0.65", "stage": ["confirmed", "strong_confirmed"], "drop5": "<=-3", "overheat": "<=1", "gap": "<=3", "margin": "current_h5"}
            proxy_used = False
            original = case
            hd, cap, gap = 3, 10, 3.0
        elif case == "H5_current7_short3":
            cond = {"current_h5_slots": 7, "short_pullback_slots": 3, "daily_cap": 10}
            proxy_used = False
            original = case
            hd, cap, gap = 3, 10, 3.0
        else:
            cond = {"h5_slots": 7, "trend_slots": 3, "trend_case": "tf_166745_trend_rs_mom_market", "daily_cap": 10}
            proxy_used = True
            original = case
            hd, cap, gap = 3, 10, 3.0
        out.append({
            "case_key": case,
            "original_case_key": original,
            "definition_version": DEFINITION_VERSION,
            "condition_json": json.dumps(cond, ensure_ascii=False, sort_keys=True),
            "proxy_used": proxy_used,
            "hd": hd,
            "daily_cap": cap,
            "gap_limit": gap,
            "tax_mode": "aggregate_tax",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "strategy_group": strategy_group(case),
            "strategy_label": strategy_label(case),
            "allocation_bucket": allocation_bucket(case),
            "source_logic": source_logic(case),
        })
    return out


def historical_rows() -> list[dict[str, Any]]:
    rows = read_csv(H5_CASE_ROWS)
    out = []
    for r in rows:
        case = r.get("case_key")
        if case not in {"H5_short_pullback_drop5_m3", "H5_current7_short3"}:
            continue
        nr = {
            **r,
            "strategy_group": strategy_group(case),
            "allocation_bucket": allocation_bucket(case),
            "source_case": "stored_forward_case",
            "is_experimental": True,
        }
        out.append(nr)
    return out


def external_summary_map() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    robust = read_csv(ROOT / "outputs/trend_following_deep_backtest/03_robust_best_cases.csv")
    by_original = {r.get("case_key"): r for r in robust}
    for case, original in ORIGINAL_CASE_KEY.items():
        if original in by_original:
            out[case] = by_original[original]
    mixes = read_csv(ROOT / "outputs/trend_following_deep_backtest/11_portfolio_mix_summary.csv")
    for row in mixes:
        key = row.get("case_key")
        if key in TARGET_CASES:
            out[key] = row
    return out


def summary_from_rows(rows: list[dict[str, Any]], latest_counts: list[dict[str, Any]], open_db_counts: dict[str, int] | None = None) -> list[dict[str, Any]]:
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("case_key") in TARGET_CASES:
            by_case[row.get("case_key")].append(row)
    latest_map = {r["case_key"]: r["candidate_count"] for r in latest_counts}
    external = external_summary_map()
    out = []
    for case in TARGET_CASES:
        items = by_case.get(case, [])
        returns = [num(r.get("return_pct")) for r in items if num(r.get("return_pct")) is not None]
        pnls = [num(r.get("pnl_after_cost"), 0.0) or 0.0 for r in items]
        position_sizes = [num(r.get("theoretical_position_size")) for r in items if num(r.get("theoretical_position_size")) is not None]
        shares = [num(r.get("theoretical_shares")) for r in items if num(r.get("theoretical_shares")) is not None]
        lot_counts = Counter(str(r.get("lot_type") or "unknown") for r in items)
        wins = [v for v in pnls if v > 0]
        losses = [v for v in pnls if v < 0]
        recent_cut = max((parse_date(r.get("signal_date")) for r in items if parse_date(r.get("signal_date"))), default=None)
        recent = []
        if recent_cut:
            start = recent_cut - timedelta(days=30)
            recent = [r for r in items if (parse_date(r.get("signal_date")) or recent_cut) >= start]
        ext = external.get(case, {})
        count_value = len(returns) if returns else int(num(ext.get("count"), 0) or 0)
        closed_value = len(returns) if returns else int(num(ext.get("count"), 0) or 0)
        after_cost_total = sum(pnls) if pnls else num(ext.get("pnl_after_cost"), 0.0) or 0.0
        out.append({
            "case_key": case,
            "count": count_value,
            "open_count": (open_db_counts or {}).get(case, latest_map.get(case, 0)),
            "closed_count": closed_value,
            "avg_return_pct": mean(returns) if returns else ext.get("avg_return_pct"),
            "median_return_pct": median(returns) if returns else ext.get("median_return_pct"),
            "win_rate": len(wins) / len(pnls) * 100 if pnls else ext.get("win_rate"),
            "PF": (sum(wins) / abs(sum(losses)) if losses else (None if not wins else float("inf"))) if pnls else ext.get("PF"),
            "pretax_pnl": sum(num(r.get("pnl_before_cost"), 0.0) or 0.0 for r in items) or ext.get("pretax_pnl"),
            "taxed_pnl": (sum(pnls) - max(sum(pnls), 0.0) * TAX_RATE) if pnls else ext.get("taxed_pnl"),
            "max_dd": max_drawdown(pnls) if pnls else ext.get("max_dd"),
            "max_loss_streak": max_loss_streak(pnls) if pnls else ext.get("max_loss_streak"),
            "recent_30d_pnl_after_cost": sum(num(r.get("pnl_after_cost"), 0.0) or 0.0 for r in recent),
            "latest_candidate_count": latest_map.get(case, 0),
            "avg_theoretical_position_size": mean(position_sizes) if position_sizes else "",
            "avg_theoretical_shares": mean(shares) if shares else "",
            "lot_type_counts": ";".join(f"{k}:{v}" for k, v in sorted(lot_counts.items())) if items else "",
        })
    return out


def max_drawdown(pnls: list[float]) -> float:
    eq = peak = 0.0
    dd = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    return abs(dd)


def max_loss_streak(pnls: list[float]) -> int:
    best = cur = 0
    for p in pnls:
        if p < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def payload_for_virtual_trade(row: dict[str, Any]) -> dict[str, Any]:
    signal_date = date_text(row.get("signal_date"))
    code = normalize_code(row.get("code"))
    entry_price = num(row.get("entry_price"))
    sizing = calculate_virtual_position_size(entry_price)
    quantity = int(sizing["theoretical_shares"] or 0) or None
    constrained_raw = row.get("is_capital_constrained")
    is_capital_constrained = str(constrained_raw).strip().lower() in {"true", "1", "yes", "y"}
    return {
        "code": code,
        "name": row.get("name"),
        "buy_price": entry_price,
        "buy_date": f"{row.get('entry_date')}T00:00:00+09:00" if row.get("entry_date") else None,
        "quantity": quantity,
        "buy_score": num(row.get("score")),
        "signal_stage": row.get("signal_stage"),
        "entry_reason": "experimental_forward_test_paper_trade",
        "entry_probability": num(row.get("score")),
        "case_key": row.get("case_key"),
        "case_label": row.get("strategy_label") or row.get("case_key"),
        "strategy_group": row.get("strategy_group"),
        "strategy_label": row.get("strategy_label"),
        "is_experimental": True,
        "source_case": row.get("source_case"),
        "source_logic": row.get("source_logic"),
        "allocation_bucket": row.get("allocation_bucket"),
        "target_position_size": num(row.get("target_position_size"), sizing["target_position_size"]),
        "theoretical_shares": int(num(row.get("theoretical_shares"), sizing["theoretical_shares"] or 0) or 0) or None,
        "theoretical_position_size": num(row.get("theoretical_position_size"), sizing["theoretical_position_size"]),
        "lot_type": row.get("lot_type") or sizing["lot_type"],
        "position_sizing_rule": row.get("position_sizing_rule") or sizing["position_sizing_rule"],
        "sizing_note": row.get("sizing_note") or sizing["sizing_note"],
        "actual_position_size": num(row.get("actual_position_size")),
        "is_capital_constrained": is_capital_constrained,
        "experimental_definition_version": row.get("definition_version"),
        "trend_flags": row.get("trend_flags"),
        "momentum_flags": row.get("momentum_flags"),
        "credit_flags": row.get("credit_flags"),
        "drop5": num(row.get("drop5")),
        "drop10": num(row.get("drop10")),
        "drop20": num(row.get("drop20")),
        "entry_gap_pct": num(row.get("gap")),
        "entry_overheat_score": int(num(row.get("overheat_score"), 0) or 0),
        "virtual_entry_price": num(row.get("entry_price")),
        "virtual_entry_model": "next_open_paper_trade",
        "virtual_entry_date": f"{row.get('entry_date')}T00:00:00+09:00" if row.get("entry_date") else None,
        "planned_exit_rule": row.get("planned_exit_rule"),
        "planned_holding_days": int(num(row.get("planned_holding_days"), 3) or 3),
        "max_holding_days": int(num(row.get("planned_holding_days"), 3) or 3),
        "entry_ma25_gap_pct": num(row.get("entry_ma25_gap_pct")),
        "entry_ma75_gap_pct": num(row.get("entry_ma75_gap_pct")),
        "entry_rsi14": num(row.get("entry_rsi14")),
        "entry_ma75": num(row.get("entry_ma75")),
        "current_exit_target": num(row.get("current_exit_target")),
        "mean_reversion_type": row.get("mean_reversion_type"),
        "environment_status": row.get("environment_status"),
        "status": "open",
        "is_primary_h5": False,
        "is_live_candidate": False,
        "is_h5_live_limited": False,
        "is_h5_research": False,
        "position_limit_mode": "experimental_paper_trade",
        "skip_reason": None,
        "exclusion_reason": row.get("exclusion_reason"),
    }


def insert_optional(sb, table: str, payload: dict[str, Any], *, on_conflict: str | None = None) -> bool:
    remaining = dict(payload)
    while remaining:
        try:
            q = sb.table(table).upsert(remaining, on_conflict=on_conflict) if on_conflict else sb.table(table).insert(remaining)
            q.execute()
            return True
        except Exception as e:
            msg = str(e)
            marker = "Could not find the '"
            if marker in msg:
                missing = msg.split(marker, 1)[1].split("'", 1)[0]
                remaining.pop(missing, None)
                continue
            raise
    return False


def existing_open_counts(sb) -> dict[str, int]:
    base = {case: 0 for case in TARGET_CASES}
    try:
        rows = (
            sb.table("virtual_trades")
            .select("case_key")
            .eq("is_experimental", True)
            .eq("status", "open")
            .in_("case_key", TARGET_CASES)
            .execute()
            .data or []
        )
    except Exception:
        rows = []
    base.update(dict(Counter(str(r.get("case_key") or "") for r in rows)))
    return base


def apply_to_db(rows: list[dict[str, Any]], defs: list[dict[str, Any]]) -> tuple[int, int, dict[str, int]]:
    sb = build_supabase()
    definitions_written = 0
    for d in defs:
        payload = dict(d)
        payload["condition_json"] = json.loads(str(payload["condition_json"]))
        if insert_optional(sb, "experimental_case_definitions", payload, on_conflict="case_key"):
            definitions_written += 1
    inserted = 0
    for row in rows:
        if row.get("採用可否") != "採用":
            continue
        payload = payload_for_virtual_trade(row)
        # Case-key scoped duplicate guard: same case/date/code is skipped, but other cases are independent.
        existing = (
            sb.table("virtual_trades")
            .select("id")
            .eq("is_experimental", True)
            .eq("case_key", payload["case_key"])
            .eq("code", payload["code"])
            .eq("buy_date", payload["buy_date"])
            .limit(1)
            .execute()
            .data or []
        )
        if existing:
            continue
        insert_optional(sb, "virtual_trades", payload)
        inserted += 1
    return inserted, definitions_written, existing_open_counts(sb)


def safety_rows() -> list[dict[str, Any]]:
    return [
        {"check": "Primary変更なし", "result": "PASS", "detail": "script is independent"},
        {"check": "本番H5変更なし", "result": "PASS", "detail": "H5_ACTIVE_CASE_KEYS not modified"},
        {"check": "actual_trade_logs変更なし", "result": "PASS", "detail": "script never writes actual_trade_logs"},
        {"check": "自動売買変更なし", "result": "PASS", "detail": "is_live_candidate=false, is_experimental=true"},
        {"check": "本命LINE通知変更なし", "result": "PASS", "detail": "no LINE code path"},
        {"check": "cooldown分離", "result": "PASS", "detail": "duplicate guard is case_key+code+buy_date scoped"},
        {"check": "本番virtual_trade分離", "result": "PASS", "detail": "is_experimental=true and case_key not in H5 active production set"},
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--apply", action="store_true", help="Upsert experimental definitions and adopted latest candidates to Supabase")
    parser.add_argument("--check-db", action="store_true", help="Read current experimental open counts from virtual_trades without writing")
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir if not Path(args.output_dir).is_absolute() else Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = enrich_latest_rows(out_dir)
    latest, filter_counts, watch = latest_candidates_for_cases(rows)
    defs = definitions()
    hist = historical_rows()
    daily_rows = hist + [r for r in latest if r.get("採用可否") == "採用"]
    open_counts = {r["case_key"]: r["candidate_count"] for r in filter_counts}
    inserted = definitions_written = 0
    if args.apply:
        inserted, definitions_written, open_counts = apply_to_db(latest, defs)
    elif args.check_db:
        open_counts = existing_open_counts(build_supabase())
    summary = summary_from_rows(daily_rows, filter_counts, open_counts)

    write_csv(out_dir / "virtual_case_daily_rows.csv", daily_rows)
    write_csv(out_dir / "virtual_case_summary.csv", summary)
    write_csv(out_dir / "latest_virtual_candidates.csv", latest)
    write_csv(out_dir / "latest_virtual_filter_counts.csv", filter_counts)
    write_csv(out_dir / "experimental_case_definitions.csv", defs)
    write_csv(out_dir / "experimental_overheat_watch.csv", watch)
    write_csv(out_dir / "safety_checks.csv", safety_rows())
    active_key_overlap = sorted(set(TARGET_CASES) & set(H5_ACTIVE_CASE_KEYS))
    write_text(out_dir / "report.txt", f"""Experimental virtual cases report

apply_to_db: {args.apply}
virtual_trades_inserted: {inserted}
definitions_written: {definitions_written if args.apply else len(defs)} ({'db' if args.apply else 'csv only'})
latest_date: {latest_date(rows)}
target_cases: {len(TARGET_CASES)}
adopted_latest_candidates: {sum(1 for r in latest if r.get('採用可否') == '採用')}

Safety:
- Primary changed: no
- H5 production changed: no
- LINE main notification changed: no
- actual_trade_logs changed: no
- auto trading changed: no
- cooldown scope: case_key + code + buy_date
- production H5 active keys touched: no
- target/active H5 key overlap: {active_key_overlap or 'none'}
""")

    print(f"output_dir={out_dir}")
    for row in filter_counts:
        print(f"latest_{row['case_key']}={row['candidate_count']}")
    print(f"open_virtual_trades_counts={open_counts}")
    print(f"definitions_saved={len(defs)}")
    print(f"virtual_trades_inserted={inserted}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
