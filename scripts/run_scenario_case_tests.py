#!/usr/bin/env python3
"""シナリオ別ケーステストをCSV出力する（読み取り専用・DB書き込みなし）。

Usage:
    python scripts/run_scenario_case_tests.py
    python scripts/run_scenario_case_tests.py --scenario 2020_covid_crash
    python scripts/run_scenario_case_tests.py --output outputs/case_mix/scenario_case_results.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "outputs" / "case_mix" / "scenario_case_results.csv"

SCENARIOS = {
    "2020_covid_crash":    (date(2020, 2, 20), date(2020, 4, 30)),
    "2022_rate_hike_bear": (date(2022, 1,  1), date(2022, 12, 31)),
    "2023_rebound":        (date(2023, 1,  1), date(2023, 12, 31)),
    "2024_ai_bubble":      (date(2024, 1,  1), date(2024, 12, 31)),
    "2025_ai_bubble":      (date(2025, 1,  1), date(2025, 12, 31)),
    "custom_recent":       (date(2026, 2,  9), date(2026, 5,  10)),
}

COLS = [
    "scenario",
    "case_key",
    "entry_count",
    "win_rate",
    "expected_value_pct",
    "total_profit_pct",
    "total_profit_yen",
    "max_drawdown_pct",
    "avg_peak_profit_pct",
    "avg_trade_drawdown_pct",
    "avg_holding_days",
    "max_open_positions",
    "tp_count",
    "sl_count",
    "open_count",
]


def run(args: argparse.Namespace) -> None:
    from services.trade_case_tester import run_trade_case_test_readonly

    selected = (
        {args.scenario: SCENARIOS[args.scenario]}
        if args.scenario
        else SCENARIOS
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []

    for scenario_name, (start, end) in selected.items():
        logger.info("[scenario_test] scenario=%s %s..%s", scenario_name, start, end)
        try:
            cases, _sims_by_case, results_by_case = run_trade_case_test_readonly(start, end)
        except Exception as e:
            logger.error("[scenario_test] failed scenario=%s: %s", scenario_name, e)
            continue

        for case in cases:
            case_key = str(case.get("case_key") or case.get("id"))
            r = results_by_case.get(case_key, {})
            all_rows.append({
                "scenario":              scenario_name,
                "case_key":              case_key,
                "entry_count":           r.get("entry_count", 0),
                "win_rate":              r.get("win_rate"),
                "expected_value_pct":    r.get("expected_value_pct"),
                "total_profit_pct":      r.get("total_profit_pct"),
                "total_profit_yen":      r.get("total_profit_yen"),
                "max_drawdown_pct":      r.get("max_drawdown_pct"),
                "avg_peak_profit_pct":   r.get("avg_peak_profit_pct"),
                "avg_trade_drawdown_pct":r.get("avg_trade_drawdown_pct"),
                "avg_holding_days":      r.get("avg_holding_days"),
                "max_open_positions":    r.get("max_open_positions"),
                "tp_count":              r.get("tp_count", 0),
                "sl_count":              r.get("sl_count", 0),
                "open_count":            r.get("open_count", 0),
            })
        logger.info("[scenario_test] scenario=%s cases=%d", scenario_name, len(cases))

    with output.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLS)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("[scenario_test] saved %s rows=%d", output, len(all_rows))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="シナリオ別ケーステストCSV出力（読み取り専用）")
    p.add_argument("--scenario", choices=list(SCENARIOS), default=None,
                   help="シナリオ名（省略時は全シナリオ）")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT),
                   help=f"出力CSVパス（デフォルト: {DEFAULT_OUTPUT}）")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
