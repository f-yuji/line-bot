#!/usr/bin/env python3
"""
Backtest all enabled trade_case_definitions across 6 historical market regimes.
Read-only: no DB writes. Output: stdout table + optional CSV.

Usage:
    venv/Scripts/python.exe scripts/backtest_regimes.py
    venv/Scripts/python.exe scripts/backtest_regimes.py --output results.csv
    venv/Scripts/python.exe scripts/backtest_regimes.py --scenario 2025_ai_bubble,custom_recent
    venv/Scripts/python.exe scripts/backtest_regimes.py --case-keys current_fixed6,current_trailing3
"""
import argparse
import csv
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SCENARIOS = [
    {"name": "custom_recent",       "type": "custom",  "start": "2026-02-09", "end": "2026-05-10"},
    {"name": "2025_ai_bubble",      "type": "bull",    "start": "2025-01-01", "end": "2025-12-31"},
    {"name": "2024_ai_bubble",      "type": "bull",    "start": "2024-01-01", "end": "2024-12-31"},
    {"name": "2023_rebound",        "type": "rebound", "start": "2023-01-01", "end": "2023-12-31"},
    {"name": "2022_rate_hike_bear", "type": "bear",    "start": "2022-01-01", "end": "2022-12-31"},
    {"name": "2020_covid_crash",    "type": "panic",   "start": "2020-02-20", "end": "2020-04-30"},
]

OUTPUT_COLS = [
    "scenario", "scenario_type", "case_key", "case_name",
    "trades", "win_rate", "expected_return", "total_return_pct",
    "max_drawdown_pct", "avg_holding_days",
    "tp_count", "sl_count", "timeout_count",
    "top20_win_rate", "prob65_win_rate",
    "avg_peak_profit_pct", "avg_trade_drawdown_pct",
]

_ENTRY_LABELS = {
    "current": "現行入口",
    "ai_top10": "AI上位10件",
    "ev_top10": "期待値上位10件",
    "position_limited": "保有数制限",
    "sector_limited": "セクター制限",
    "regime_strict": "地合い厳格化",
}
_EXIT_LABELS = {
    "fixed6": "固定6%",
    "fixed7": "固定7%",
    "fixed10": "固定10%",
    "trailing3": "トレーリング3%",
    "trailing5": "トレーリング5%",
    "pullback2": "反落-2%",
    "ma5": "5日MA割れ",
    "rsi70": "RSI70反落",
    "atr15": "ATR 1.5倍",
}
_CREDIT_LABELS = {
    "no_margin": "",
    "margin_le20": "信用倍率20倍以下",
    "margin_le10": "信用倍率10倍以下",
    "margin_le5": "信用倍率5倍以下",
    "short_pressure": "売り残比率10%以上",
}


def _case_display_name(case: dict) -> str:
    rules = case.get("rules") or {}
    if not isinstance(rules, dict):
        rules = {}
    parts = []
    entry = str(rules.get("entry_profile") or "")
    if entry:
        parts.append(_ENTRY_LABELS.get(entry, entry))
    exit_p = str(rules.get("exit_profile") or "")
    if exit_p:
        parts.append(_EXIT_LABELS.get(exit_p, exit_p))
    credit = str(rules.get("credit_profile") or "")
    credit_label = _CREDIT_LABELS.get(credit, credit)
    if credit_label:
        parts.append(credit_label)
    if parts:
        return " x ".join(parts)
    return str(case.get("case_name") or case.get("case_key") or "")


def _to_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _top_pct_win_rate(sims: list[dict], pct: float = 0.20) -> float | None:
    """Win rate for trades with signal_probability in top pct%."""
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    if not closed:
        return None
    probs = sorted(
        [_to_float(s.get("signal_probability"), 0) or 0 for s in closed],
        reverse=True,
    )
    threshold_idx = max(0, int(len(probs) * pct) - 1)
    threshold = probs[threshold_idx] if probs else 0.0
    top = [s for s in closed if (_to_float(s.get("signal_probability"), 0) or 0) >= threshold]
    if not top:
        return None
    wins = [s for s in top if (_to_float(s.get("profit_pct"), 0) or 0) > 0]
    return round(len(wins) / len(top) * 100, 1)


def _prob_threshold_win_rate(sims: list[dict], threshold: float = 0.65) -> float | None:
    """Win rate for trades with signal_probability >= threshold."""
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    subset = [s for s in closed if (_to_float(s.get("signal_probability"), 0) or 0) >= threshold]
    if not subset:
        return None
    wins = [s for s in subset if (_to_float(s.get("profit_pct"), 0) or 0) > 0]
    return round(len(wins) / len(subset) * 100, 1)


def _build_row(scenario: dict, case: dict, result: dict, sims: list[dict]) -> dict:
    case_key = str(case.get("case_key") or case.get("id"))
    return {
        "scenario": scenario["name"],
        "scenario_type": scenario["type"],
        "case_key": case_key,
        "case_name": _case_display_name(case),
        "trades": result.get("entry_count") or 0,
        "win_rate": result.get("win_rate"),
        "expected_return": result.get("expected_value_pct"),
        "total_return_pct": result.get("total_profit_pct"),
        "max_drawdown_pct": result.get("max_drawdown_pct"),
        "avg_holding_days": result.get("avg_holding_days"),
        "tp_count": result.get("tp_count") or 0,
        "sl_count": result.get("sl_count") or 0,
        "timeout_count": result.get("timeout_count") or 0,
        "top20_win_rate": _top_pct_win_rate(sims, pct=0.20),
        "prob65_win_rate": _prob_threshold_win_rate(sims, threshold=0.65),
        "avg_peak_profit_pct": result.get("avg_peak_profit_pct"),
        "avg_trade_drawdown_pct": result.get("avg_trade_drawdown_pct"),
    }


def _fmt(v, fmt=".1f", width=7) -> str:
    if v is None:
        return "-".rjust(width)
    return f"{v:{fmt}}".rjust(width)


def _print_table(all_rows: list[dict], scenarios: list[dict]) -> None:
    for sc in scenarios:
        sc_rows = [r for r in all_rows if r["scenario"] == sc["name"]]
        if not sc_rows:
            print(f"\n  {sc['name']}: no results")
            continue
        print(f"\n=== {sc['name']} ({sc['type']}) ===")
        hdr = f"{'case_key':<32} {'trades':>6} {'win%':>6} {'EV%':>6} {'total%':>8} {'maxDD%':>7} {'hold':>5} {'top20%':>7} {'p65%':>6}"
        print(hdr)
        print("-" * len(hdr))
        for r in sorted(sc_rows, key=lambda x: -(x.get("expected_return") or -999)):
            print(
                f"{r['case_key']:<32}"
                f"{r['trades']:>6}"
                f"{_fmt(r['win_rate'], '.1f', 6)}"
                f"{_fmt(r['expected_return'], '.2f', 6)}"
                f"{_fmt(r['total_return_pct'], '.1f', 8)}"
                f"{_fmt(r['max_drawdown_pct'], '.1f', 7)}"
                f"{_fmt(r['avg_holding_days'], '.1f', 5)}"
                f"{_fmt(r['top20_win_rate'], '.1f', 7)}"
                f"{_fmt(r['prob65_win_rate'], '.1f', 6)}"
            )


def run(args: argparse.Namespace) -> None:
    from services.trade_case_tester import run_trade_case_test_readonly

    case_keys = [k.strip() for k in args.case_keys.split(",")] if args.case_keys else None
    scenarios = SCENARIOS
    if args.scenario:
        names = {s.strip() for s in args.scenario.split(",")}
        scenarios = [s for s in SCENARIOS if s["name"] in names]

    all_rows: list[dict] = []
    for sc in scenarios:
        logger.info("=== scenario: %s (%s) ===", sc["name"], sc["type"])
        try:
            cases, sims_by_case, results_by_case = run_trade_case_test_readonly(
                sc["start"], sc["end"],
                case_keys=case_keys,
            )
            for case in cases:
                case_key = str(case.get("case_key") or case.get("id"))
                sims = sims_by_case.get(case_key, [])
                result = results_by_case.get(case_key, {})
                all_rows.append(_build_row(sc, case, result, sims))
        except Exception:
            logger.exception("scenario %s failed", sc["name"])

    _print_table(all_rows, scenarios)

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLS)
            writer.writeheader()
            writer.writerows(all_rows)
        logger.info("CSV saved: %s (%d rows)", args.output, len(all_rows))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest trade cases across 6 market regimes (read-only)")
    parser.add_argument("--scenario", help="comma-separated scenario names (default: all)")
    parser.add_argument("--case-keys", help="comma-separated case_key values (default: all enabled)")
    parser.add_argument("--output", help="CSV output path")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
