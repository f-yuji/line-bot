#!/usr/bin/env python3
"""Backtest fixed-weight mixes of trade cases.

Research-only / read-only script:
- Does not write to Supabase.
- Does not touch virtual_trades.
- Does not change ACTIVE models.
- Reuses services.trade_case_tester.run_trade_case_test_readonly() for
  individual trade simulation logic.

Daily equity is accumulated with simple returns from an initial equity of 100.
That is intentionally not compounding for the first version, so each day adds
daily_return_pct directly to equity.
"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "outputs" / "case_mix"

SCENARIOS = {
    "2020_covid_crash": {"start": "2020-02-20", "end": "2020-04-30"},
    "2022_rate_hike_bear": {"start": "2022-01-01", "end": "2022-12-31"},
    "2023_rebound": {"start": "2023-01-01", "end": "2023-12-31"},
    "2024_ai_bubble": {"start": "2024-01-01", "end": "2024-12-31"},
    "2025_ai_bubble": {"start": "2025-01-01", "end": "2025-12-31"},
    "custom_recent": {"start": "2026-02-09", "end": "2026-05-10"},
}

CASE_KEYS = [
    "combo_current__pullback2__margin_le20",
    "combo_current__ma5__margin_le20",
    "combo_current__rsi70__margin_le5",
    "combo_current__fixed10",
]

MIXES = {
    "core_mix": {
        "combo_current__pullback2__margin_le20": 0.50,
        "combo_current__ma5__margin_le20": 0.25,
        "combo_current__rsi70__margin_le5": 0.15,
        "combo_current__fixed10": 0.10,
    },
    "pullback2_only": {
        "combo_current__pullback2__margin_le20": 1.00,
    },
    "defensive_mix": {
        "combo_current__pullback2__margin_le20": 0.40,
        "combo_current__rsi70__margin_le5": 0.30,
        "combo_current__fixed10": 0.20,
        "combo_current__ma5__margin_le20": 0.10,
    },
    "bull_mix": {
        "combo_current__ma5__margin_le20": 0.40,
        "combo_current__pullback2__margin_le20": 0.40,
        "combo_current__rsi70__margin_le5": 0.10,
        "combo_current__fixed10": 0.10,
    },
}

EQUITY_COLS = [
    "scenario",
    "mix_name",
    "date",
    "daily_return_pct",
    "equity",
    "drawdown_pct",
    "combo_current__pullback2__margin_le20_return_pct",
    "combo_current__ma5__margin_le20_return_pct",
    "combo_current__rsi70__margin_le5_return_pct",
    "combo_current__fixed10_return_pct",
]

SUMMARY_COLS = [
    "scenario",
    "mix_name",
    "total_return_pct",
    "max_drawdown_pct",
    "win_rate_days",
    "avg_daily_return_pct",
    "best_day_pct",
    "worst_day_pct",
    "profit_factor",
    "active_days",
    "total_trades",
    "notes",
]

CONTRIBUTION_COLS = [
    "scenario",
    "mix_name",
    "case_key",
    "weight",
    "total_return_contribution_pct",
    "trades",
    "avg_trade_return_pct",
    "win_rate",
    "max_drawdown_pct",
]


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _date_range(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    if math.isinf(value):
        return value
    return round(value, digits)


def _case_capital_base(sims: list[dict]) -> float:
    entry_costs = [
        (_to_float(s.get("entry_price"), 0.0) or 0.0) * 100.0
        for s in sims
        if (_to_float(s.get("entry_price"), 0.0) or 0.0) > 0
    ]
    if not entry_costs:
        return 0.0

    events: list[tuple[date, int]] = []
    for sim in sims:
        entry_date = _to_date(sim.get("entry_date"))
        exit_s = sim.get("exit_date") or sim.get("entry_date")
        exit_date = _to_date(exit_s)
        events.append((entry_date, 1))
        events.append((exit_date + timedelta(days=1), -1))

    open_count = 0
    max_open = 0
    for _day, delta in sorted(events, key=lambda x: (x[0], -x[1])):
        open_count += delta
        max_open = max(max_open, open_count)

    avg_position_cost = sum(entry_costs) / len(entry_costs)
    return avg_position_cost * max(1, max_open)


def _case_daily_returns(sims: list[dict], start: date, end: date) -> dict[str, float]:
    """Return realized daily P/L percentage normalized by the case fund capital."""
    capital_base = _case_capital_base(sims)
    if capital_base <= 0:
        return {}

    daily_profit_yen: dict[str, float] = defaultdict(float)
    for sim in sims:
        if sim.get("status") != "closed":
            continue
        exit_s = sim.get("exit_date")
        if not exit_s:
            continue
        exit_date = _to_date(exit_s)
        if exit_date < start or exit_date > end:
            continue
        daily_profit_yen[exit_date.isoformat()] += _to_float(sim.get("profit_yen"), 0.0) or 0.0

    return {
        day: (profit / capital_base * 100.0)
        for day, profit in daily_profit_yen.items()
    }


def _equity_rows_for_mix(
    scenario_name: str,
    mix_name: str,
    weights: dict[str, float],
    daily_by_case: dict[str, dict[str, float]],
    start: date,
    end: date,
    notes: str = "",
) -> tuple[list[dict], dict]:
    equity = 100.0
    peak = 100.0
    rows: list[dict] = []
    daily_values: list[float] = []
    positive_sum = 0.0
    negative_sum = 0.0

    for day in _date_range(start, end):
        day_s = day.isoformat()
        case_returns = {
            case_key: daily_by_case.get(case_key, {}).get(day_s, 0.0)
            for case_key in CASE_KEYS
        }
        daily_return = sum(case_returns.get(case_key, 0.0) * weight for case_key, weight in weights.items())

        # Simple accumulation: equity starts at 100 and adds daily_return_pct.
        equity += daily_return
        peak = max(peak, equity)
        drawdown = (equity - peak) / peak * 100.0 if peak else 0.0
        daily_values.append(daily_return)
        if daily_return > 0:
            positive_sum += daily_return
        elif daily_return < 0:
            negative_sum += daily_return

        rows.append({
            "scenario": scenario_name,
            "mix_name": mix_name,
            "date": day_s,
            "daily_return_pct": _round(daily_return),
            "equity": _round(equity),
            "drawdown_pct": _round(drawdown),
            "combo_current__pullback2__margin_le20_return_pct": _round(case_returns["combo_current__pullback2__margin_le20"]),
            "combo_current__ma5__margin_le20_return_pct": _round(case_returns["combo_current__ma5__margin_le20"]),
            "combo_current__rsi70__margin_le5_return_pct": _round(case_returns["combo_current__rsi70__margin_le5"]),
            "combo_current__fixed10_return_pct": _round(case_returns["combo_current__fixed10"]),
        })

    active_days = len([v for v in daily_values if abs(v) > 1e-12])
    summary = {
        "scenario": scenario_name,
        "mix_name": mix_name,
        "total_return_pct": _round(equity - 100.0),
        "max_drawdown_pct": _round(min((_to_float(r["drawdown_pct"], 0.0) or 0.0) for r in rows), 4) if rows else None,
        "win_rate_days": _round(
            len([v for v in daily_values if v > 0]) / active_days * 100.0 if active_days else None,
            2,
        ),
        "avg_daily_return_pct": _round(sum(daily_values) / len(daily_values) if daily_values else None),
        "best_day_pct": _round(max(daily_values) if daily_values else None),
        "worst_day_pct": _round(min(daily_values) if daily_values else None),
        "profit_factor": _round((positive_sum / abs(negative_sum)) if negative_sum < 0 else None),
        "active_days": active_days,
        "total_trades": None,
        "notes": notes,
    }
    return rows, summary


def _case_contribution_row(
    scenario_name: str,
    mix_name: str,
    case_key: str,
    weight: float,
    sims: list[dict],
    daily_returns: dict[str, float],
) -> dict:
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    trade_returns = [_to_float(s.get("profit_pct"), 0.0) or 0.0 for s in closed]
    wins = [v for v in trade_returns if v > 0]

    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    for day_s in sorted(daily_returns):
        equity += daily_returns[day_s]
        peak = max(peak, equity)
        dd = (equity - peak) / peak * 100.0 if peak else 0.0
        max_dd = min(max_dd, dd)

    return {
        "scenario": scenario_name,
        "mix_name": mix_name,
        "case_key": case_key,
        "weight": weight,
        "total_return_contribution_pct": _round(sum(daily_returns.values()) * weight),
        "trades": len(sims),
        "avg_trade_return_pct": _round(sum(trade_returns) / len(trade_returns) if trade_returns else None),
        "win_rate": _round(len(wins) / len(trade_returns) * 100.0 if trade_returns else None, 2),
        "max_drawdown_pct": _round(max_dd),
    }


def _select_scenarios(value: str) -> list[tuple[str, dict]]:
    if value == "all":
        return list(SCENARIOS.items())
    names = [v.strip() for v in value.split(",") if v.strip()]
    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        raise ValueError(f"unknown scenario: {', '.join(unknown)}")
    return [(name, SCENARIOS[name]) for name in names]


def _select_mixes(value: str) -> list[tuple[str, dict[str, float]]]:
    if value == "all":
        return list(MIXES.items())
    names = [v.strip() for v in value.split(",") if v.strip()]
    unknown = [name for name in names if name not in MIXES]
    if unknown:
        raise ValueError(f"unknown mix: {', '.join(unknown)}")
    return [(name, MIXES[name]) for name in names]


def _write_csv(path: Path, cols: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> None:
    from services.trade_case_tester import run_trade_case_test_readonly

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    scenarios = _select_scenarios(args.scenario)
    mixes = _select_mixes(args.mix)
    requested_case_keys = sorted({case_key for _mix_name, weights in mixes for case_key in weights})

    all_equity_rows: list[dict] = []
    all_summary_rows: list[dict] = []
    all_contribution_rows: list[dict] = []

    for scenario_name, scenario in scenarios:
        start = _to_date(scenario["start"])
        end = _to_date(scenario["end"])
        try:
            cases, sims_by_case, _results_by_case = run_trade_case_test_readonly(
                start,
                end,
                case_keys=requested_case_keys,
            )
        except Exception as exc:
            logger.exception("[case_mix] scenario=%s failed", scenario_name)
            for mix_name, _weights in mixes:
                all_summary_rows.append({
                    "scenario": scenario_name,
                    "mix_name": mix_name,
                    "total_return_pct": None,
                    "max_drawdown_pct": None,
                    "win_rate_days": None,
                    "avg_daily_return_pct": None,
                    "best_day_pct": None,
                    "worst_day_pct": None,
                    "profit_factor": None,
                    "active_days": 0,
                    "total_trades": 0,
                    "notes": f"scenario failed: {exc}",
                })
            continue

        found_case_keys = {str(case.get("case_key") or case.get("id")) for case in cases}
        daily_by_case = {
            case_key: _case_daily_returns(sims_by_case.get(case_key, []), start, end)
            for case_key in requested_case_keys
        }

        for case_key in requested_case_keys:
            logger.info(
                "[case_mix] loaded case=%s trades=%d",
                case_key,
                len(sims_by_case.get(case_key, [])),
            )

        for mix_name, weights in mixes:
            logger.info(
                "[case_mix] scenario=%s mix=%s start=%s end=%s",
                scenario_name,
                mix_name,
                start.isoformat(),
                end.isoformat(),
            )

            missing = [case_key for case_key in weights if case_key not in found_case_keys]
            notes_parts: list[str] = []
            if missing:
                all_summary_rows.append({
                    "scenario": scenario_name,
                    "mix_name": mix_name,
                    "total_return_pct": None,
                    "max_drawdown_pct": None,
                    "win_rate_days": None,
                    "avg_daily_return_pct": None,
                    "best_day_pct": None,
                    "worst_day_pct": None,
                    "profit_factor": None,
                    "active_days": 0,
                    "total_trades": 0,
                    "notes": f"missing case_key: {', '.join(missing)}",
                })
                continue

            zero_trade_cases = [
                case_key
                for case_key in weights
                if not sims_by_case.get(case_key)
            ]
            if zero_trade_cases:
                notes_parts.append(f"zero trades: {', '.join(zero_trade_cases)}")
            notes = " | ".join(notes_parts)

            equity_rows, summary = _equity_rows_for_mix(
                scenario_name,
                mix_name,
                weights,
                daily_by_case,
                start,
                end,
                notes=notes,
            )
            total_trades = sum(len(sims_by_case.get(case_key, [])) for case_key in weights)
            summary["total_trades"] = total_trades
            all_equity_rows.extend(equity_rows)
            all_summary_rows.append(summary)

            for case_key, weight in weights.items():
                all_contribution_rows.append(_case_contribution_row(
                    scenario_name,
                    mix_name,
                    case_key,
                    weight,
                    sims_by_case.get(case_key, []),
                    daily_by_case.get(case_key, {}),
                ))

            logger.info("[case_mix] saved equity rows=%d", len(equity_rows))

    _write_csv(OUTPUT_DIR / "case_mix_equity.csv", EQUITY_COLS, all_equity_rows)
    _write_csv(OUTPUT_DIR / "case_mix_summary.csv", SUMMARY_COLS, all_summary_rows)
    _write_csv(OUTPUT_DIR / "case_mix_contribution.csv", CONTRIBUTION_COLS, all_contribution_rows)
    logger.info("[case_mix] saved summary rows=%d", len(all_summary_rows))
    logger.info("[case_mix] output_dir=%s", OUTPUT_DIR)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest fixed-weight trade case mixes (read-only)")
    parser.add_argument("--scenario", default="all", help="scenario name, comma-separated names, or all")
    parser.add_argument("--mix", default="core_mix", help="mix name, comma-separated names, or all")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
