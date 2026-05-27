#!/usr/bin/env python3
"""Grid search / parameter optimization engine for the rebound trading bot.

Overnight-run tool. Evaluates millions of parameter combinations across
train/test splits with OOS validation, staged search, and resume support.

Usage:
    # Staged search (default) with 1M combos:
    python scripts/grid_search_rebound.py --start 2020-01-01 --end 2026-04-28 \\
        --train-end 2024-12-31 --run-name run_20260526 --workers 7

    # Resume interrupted run:
    python scripts/grid_search_rebound.py --resume --run-name run_20260526

    # Random sample:
    python scripts/grid_search_rebound.py --search-mode random --sample-n 500000

    # Full grid (warning: extremely slow):
    python scripts/grid_search_rebound.py --search-mode full
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import pickle
import random
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from itertools import product
from multiprocessing import Pool
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from services.research_database import build_supabase
from services.trade_case_tester import (
    _active_model_bundle,
    _attach_weekly_margin,
    _exit_for_candidate,
    _expected_value_for_rules,
    _fetch_snapshots_by_ids,
    _load_weekly_margin_rows,
    _score_candidates,
    _sort_candidates,
    _to_date,
    _to_float,
    _to_int,
    MAX_FUTURE_DAYS,
)
try:
    from scripts.train_rebound_model import BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES
except Exception:
    BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES = [], [], []

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
OUT_BASE = ROOT / "outputs" / "rebound_grid_search"
JST = timezone(timedelta(hours=9))

logger = logging.getLogger(__name__)

# ─── Parameter space ──────────────────────────────────────────────────────────

PARAM_SPACE: dict[str, list] = {
    "entry_mode": [
        "ai_close_entry",
        "ai_close_entry_confirmed_only",
        "ai_close_entry_strong_only",
        "ai_top_score",
        "ai_top_expected",
        "ai_with_sector_limit",
    ],
    "exit_mode": [
        "pullback1",
        "pullback2",
        "ma5_break",
        "rsi70",
        "fixed_5",
        "fixed_8",
        "fixed_10",
        "trailing_3",
        "trailing_5",
    ],
    "stop_loss_pct": [-3.0, -4.0, -5.0, -6.0, None],
    "max_holding_days": [3, 5, 10, 15, 20],
    "max_margin_ratio": [5.0, 10.0, 20.0, 30.0, None],
    "max_positions": [3, 5, 10],
    "max_daily_entries": [1, 2, 3, 5],
    "sector_limit": ["off", 2, 3],
    "panic_guard": ["off", "weak", "strong"],
    "regime_filter": [
        "all",
        "no_euphoria",
        "no_panic",
        "panic_only",
        "normal_only",
        "no_risk_off",
        "risk_on_only",
    ],
    "nikkei_ma25_gap_limit": [None, 5.0, 8.0, 10.0],
    "signal_rsi_max": [None, 70.0, 75.0, 80.0],
    "signal_rsi_min": [None, 20.0, 25.0, 30.0],
    "ma5_gap_max": [None, 8.0, 12.0, 15.0],
}

# ─── Entry mode config ────────────────────────────────────────────────────────

_ALLOWED_STAGES: dict[str, set[str]] = {
    "ai_close_entry": {"confirmed", "strong_confirmed"},
    "ai_close_entry_confirmed_only": {"confirmed"},
    "ai_close_entry_strong_only": {"strong_confirmed"},
    "ai_top_score": {"confirmed", "strong_confirmed"},
    "ai_top_expected": {"confirmed", "strong_confirmed"},
    "ai_with_sector_limit": {"confirmed", "strong_confirmed"},
}

_ENTRY_RANK_LIMIT: dict[str, int] = {
    "ai_top_score": 5,
    "ai_top_expected": 5,
}

# ─── Strategy ID short names ──────────────────────────────────────────────────

_SHORT: dict[str, str] = {
    "ai_close_entry": "AICLOSE",
    "ai_close_entry_confirmed_only": "AICONF",
    "ai_close_entry_strong_only": "AISTRONG",
    "ai_top_score": "AITOP",
    "ai_top_expected": "AIEV",
    "ai_with_sector_limit": "AISEC",
    "pullback1": "PB1",
    "pullback2": "PB2",
    "ma5_break": "MA5",
    "rsi70": "RSI70",
    "fixed_5": "FX5",
    "fixed_8": "FX8",
    "fixed_10": "FX10",
    "trailing_3": "TR3",
    "trailing_5": "TR5",
}

# ─── Regime filter ────────────────────────────────────────────────────────────

PANIC_MODES: set[str] = {"panic_selloff", "panic_rebound", "panic", "shock"}
RISK_OFF_MODES: set[str] = {"risk_off"} | PANIC_MODES
RISK_ON_MODES: set[str] = {"strong_risk_on", "euphoria", "normal"}


def _passes_regime(regime: str, regime_filter: str, nikkei_gap: float | None) -> bool:
    if regime_filter == "all":
        return True
    if regime_filter == "no_euphoria":
        if nikkei_gap is not None and nikkei_gap > 8.0:
            return False
        return regime != "euphoria"
    if regime_filter == "no_panic":
        return regime not in PANIC_MODES
    if regime_filter == "panic_only":
        return regime in PANIC_MODES
    if regime_filter == "normal_only":
        return regime == "normal"
    if regime_filter == "no_risk_off":
        return regime not in RISK_OFF_MODES
    if regime_filter == "risk_on_only":
        return regime in RISK_ON_MODES
    return True


# ─── Exit rules builder ───────────────────────────────────────────────────────

def _build_exit_rules(params: dict) -> dict:
    exit_mode: str = params["exit_mode"]
    sl: float | None = params["stop_loss_pct"]
    max_days: int = params["max_holding_days"]

    sl_frac = float(sl) / 100.0 if sl is not None else -0.99

    rules: dict = {
        "initial_sl_pct": sl_frac,
        "sl_pct": sl_frac,
        "tp_pct": 0.06,
        "max_holding_days": max_days,
    }

    if exit_mode == "pullback1":
        rules.update({"exit_type": "pullback_exit", "pullback_day_pct": -0.01})
    elif exit_mode == "pullback2":
        rules.update({"exit_type": "pullback_exit", "pullback_day_pct": -0.02})
    elif exit_mode == "ma5_break":
        rules.update({"exit_type": "ma_break_exit", "ma_period": 5})
    elif exit_mode == "rsi70":
        rules.update({"exit_type": "rsi_reversal_exit", "overbought_rsi": 70})
    elif exit_mode == "fixed_5":
        rules.update({"exit_type": "fixed_tp_sl", "tp_pct": 0.99, "max_holding_days": 5})
    elif exit_mode == "fixed_8":
        rules.update({"exit_type": "fixed_tp_sl", "tp_pct": 0.99, "max_holding_days": 8})
    elif exit_mode == "fixed_10":
        rules.update({"exit_type": "fixed_tp_sl", "tp_pct": 0.99, "max_holding_days": 10})
    elif exit_mode == "trailing_3":
        rules.update({"exit_type": "trailing_stop", "trailing_drop_pct": 0.03})
    elif exit_mode == "trailing_5":
        rules.update({"exit_type": "trailing_stop", "trailing_drop_pct": 0.05})
    else:
        rules["exit_type"] = "pullback_exit"

    return rules


# ─── Strategy ID and hash ─────────────────────────────────────────────────────

def _strategy_id(params: dict) -> str:
    entry = _SHORT.get(str(params.get("entry_mode", "")), "E?")
    exit_ = _SHORT.get(str(params.get("exit_mode", "")), "X?")
    sl = params.get("stop_loss_pct")
    sl_str = f"SL{abs(int(sl))}" if sl is not None else "NOSL"
    hold = f"HD{params.get('max_holding_days', 5)}"
    mr = params.get("max_margin_ratio")
    mr_str = f"MR{int(mr)}" if mr is not None else "NOMR"
    regime = str(params.get("regime_filter", "all")).upper()[:8]
    return f"RB_{entry}_{exit_}_{sl_str}_{hold}_{mr_str}_{regime}"


def _params_hash(params: dict) -> str:
    s = json.dumps({str(k): str(v) for k, v in sorted(params.items())})
    return hashlib.md5(s.encode()).hexdigest()


# ─── Core simulation ──────────────────────────────────────────────────────────

def _run_combo(
    params: dict,
    candidates: list[dict],
    nikkei_gaps: dict[str, float],
) -> dict | None:
    entry_mode = params["entry_mode"]
    regime_filter = params["regime_filter"]
    max_margin = params.get("max_margin_ratio")
    rsi_max = params.get("signal_rsi_max")
    rsi_min = params.get("signal_rsi_min")
    ma5_gap_max_val = params.get("ma5_gap_max")
    nikkei_gap_limit = params.get("nikkei_ma25_gap_limit")
    max_positions = int(params.get("max_positions", 5))
    max_daily = int(params.get("max_daily_entries", 3))
    sector_limit_val = params.get("sector_limit", "off")
    panic_guard = params.get("panic_guard", "off")

    exit_rules = _build_exit_rules(params)
    allowed_stages = _ALLOWED_STAGES.get(entry_mode, {"confirmed", "strong_confirmed"})
    sort_key = "signal_probability_desc" if entry_mode == "ai_top_score" else "expected_value_desc"
    rank_limit = _ENTRY_RANK_LIMIT.get(entry_mode, 0)
    max_sector = sector_limit_val if sector_limit_val != "off" else 999

    # Filter candidates into per-date buckets
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in candidates:
        regime = str(row.get("market_regime") or "normal")
        d_str = str(row.get("trade_date") or "")
        nikkei_gap = nikkei_gaps.get(d_str) if nikkei_gaps else None

        if not _passes_regime(regime, regime_filter, nikkei_gap):
            continue
        if nikkei_gap_limit is not None and nikkei_gaps:
            if nikkei_gap is not None and nikkei_gap > nikkei_gap_limit:
                continue
        if str(row.get("signal_stage") or "") not in allowed_stages:
            continue
        if max_margin is not None:
            mr = _to_float(row.get("margin_ratio"), None)
            if mr is not None and mr > max_margin:
                continue
        rsi_val = _to_float(row.get("rsi14"), None)
        if rsi_max is not None and rsi_val is not None and rsi_val > rsi_max:
            continue
        if rsi_min is not None and rsi_val is not None and rsi_val < rsi_min:
            continue
        if ma5_gap_max_val is not None:
            ma5_gap = _to_float(row.get("ma5_gap_pct"), None)
            if ma5_gap is not None and ma5_gap > ma5_gap_max_val:
                continue

        by_date[d_str].append(row)

    if not by_date:
        return None

    # Portfolio simulation
    simulations: list[dict] = []
    open_positions: list[dict] = []

    for trade_date in sorted(by_date):
        today = _to_date(trade_date)

        # Expire closed positions
        open_positions = [
            p for p in open_positions
            if not p.get("exit_date") or _to_date(p["exit_date"]) >= today
        ]

        if len(open_positions) >= max_positions:
            continue

        daily_rows = _sort_candidates(by_date[trade_date], sort_key, exit_rules)
        if rank_limit > 0:
            daily_rows = daily_rows[:rank_limit]

        first_row = by_date[trade_date][0]
        regime = str(first_row.get("market_regime") or "normal")
        daily_count = 0
        sectors: Counter[str] = Counter(
            str(p.get("sector") or "unknown") for p in open_positions
        )

        for row in daily_rows:
            if len(open_positions) >= max_positions:
                break
            if daily_count >= max_daily:
                break

            # Panic guard
            if panic_guard == "strong" and regime in PANIC_MODES:
                break
            if panic_guard == "weak" and regime in PANIC_MODES and daily_count >= 1:
                break

            sector = str(row.get("sector") or "unknown")
            if sectors[sector] >= max_sector:
                continue

            exit_data = _exit_for_candidate(row, exit_rules)
            sim: dict = {
                "entry_date": trade_date,
                "exit_date": exit_data.get("exit_date"),
                "profit_pct": exit_data.get("profit_pct"),
                "exit_reason": exit_data.get("exit_reason"),
                "holding_days": exit_data.get("holding_days"),
                "entry_price": _to_float(row.get("entry_price")) or _to_float(row.get("close")),
                "code": str(row.get("code") or ""),
                "sector": sector,
                "market_regime": regime,
            }
            simulations.append(sim)
            open_positions.append(sim)
            sectors[sector] += 1
            daily_count += 1

    if len(simulations) < 5:
        return None

    metrics = _calculate_metrics(simulations)
    if metrics is None:
        return None

    return metrics


# ─── Metrics ─────────────────────────────────────────────────────────────────

def _calculate_metrics(simulations: list[dict]) -> dict | None:
    closed = [s for s in simulations if s.get("profit_pct") is not None]
    if not closed:
        return None

    pcts = [float(s["profit_pct"]) for s in closed]
    wins = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p <= 0]
    trade_count = len(closed)

    win_rate = len(wins) / trade_count * 100.0 if trade_count else 0.0
    avg_pnl = sum(pcts) / trade_count if trade_count else 0.0
    median_pnl = statistics.median(pcts) if pcts else 0.0
    pf: float | None = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else None

    # Monthly and yearly PnL grouped by entry month
    monthly: dict[str, list[float]] = defaultdict(list)
    yearly: dict[str, list[float]] = defaultdict(list)
    for s in closed:
        month = str(s.get("entry_date") or "")[:7]
        year = str(s.get("entry_date") or "")[:4]
        if month:
            monthly[month].append(float(s["profit_pct"]))
        if year:
            yearly[year].append(float(s["profit_pct"]))

    monthly_returns: dict[str, float] = {m: sum(v) for m, v in sorted(monthly.items())}
    yearly_returns: dict[str, float] = {y: sum(v) for y, v in sorted(yearly.items())}

    # Equity curve sorted by exit date → cumsum → maxDD
    sorted_closed = sorted(closed, key=lambda s: s.get("exit_date") or s.get("entry_date") or "")
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for s in sorted_closed:
        equity += float(s["profit_pct"])
        if equity > peak:
            peak = equity
        dd = equity - peak
        if dd < max_dd:
            max_dd = dd

    # Date span for CAGR
    first_date = min(str(s.get("entry_date") or "") for s in closed)
    last_date = max(str(s.get("exit_date") or s.get("entry_date") or "") for s in closed)
    cagr = equity
    try:
        years = (
            datetime.fromisoformat(last_date[:10]) - datetime.fromisoformat(first_date[:10])
        ).days / 365.25
        if years > 0.1 and equity > -100:
            cagr = ((1.0 + equity / 100.0) ** (1.0 / years) - 1.0) * 100.0
    except Exception:
        pass

    # Sharpe from monthly returns (annualised), require >= 3 months
    m_vals = list(monthly_returns.values())
    sharpe = 0.0
    if len(m_vals) >= 3:
        m_mean = statistics.mean(m_vals)
        m_std = statistics.stdev(m_vals) if len(m_vals) > 1 else 0.0
        sharpe = (m_mean / m_std * math.sqrt(12)) if m_std > 0 else 0.0

    positive_months = sum(1 for v in monthly_returns.values() if v > 0)
    total_months = len(monthly_returns)
    monthly_consistency = positive_months / total_months * 100.0 if total_months else 0.0

    positive_years = sum(1 for v in yearly_returns.values() if v > 0)
    total_years = len(yearly_returns)
    yearly_consistency = positive_years / total_years * 100.0 if total_years else 0.0

    worst_month = min(monthly_returns.values()) if monthly_returns else 0.0
    best_month = max(monthly_returns.values()) if monthly_returns else 0.0
    worst_year = min(yearly_returns.values()) if yearly_returns else 0.0
    best_year = max(yearly_returns.values()) if yearly_returns else 0.0

    # Average holding days
    holding_list = [float(s["holding_days"]) for s in closed if s.get("holding_days") is not None]
    avg_holding_days = statistics.mean(holding_list) if holding_list else 0.0

    # Exposure ratio: sum(holding_days) / total_calendar_days
    total_holding = sum(holding_list)
    try:
        span_days = (
            datetime.fromisoformat(last_date[:10]) - datetime.fromisoformat(first_date[:10])
        ).days or 1
    except Exception:
        span_days = 1
    exposure_ratio = total_holding / span_days if span_days > 0 else 0.0

    # Concentration metrics (overfit detection)
    total_profit = sum(pcts)
    year_profits = list(yearly_returns.values())
    month_profits = list(monthly_returns.values())
    year_concentration: float | None = None
    month_concentration: float | None = None
    if total_profit != 0 and year_profits:
        year_concentration = max(abs(p) for p in year_profits) / abs(total_profit)
    if total_profit != 0 and month_profits:
        month_concentration = max(abs(p) for p in month_profits) / abs(total_profit)

    # Regime stats
    regime_groups: dict[str, list[float]] = defaultdict(list)
    for s in closed:
        r = str(s.get("market_regime") or "normal")
        regime_groups[r].append(float(s["profit_pct"]))
    regime_stats: dict[str, dict] = {}
    for r, vals in regime_groups.items():
        wins_r = [v for v in vals if v > 0]
        regime_stats[r] = {
            "trade_count": len(vals),
            "win_rate": round(len(wins_r) / len(vals) * 100.0, 1) if vals else 0.0,
            "avg_pnl": round(sum(vals) / len(vals), 3) if vals else 0.0,
        }

    return {
        "trade_count": trade_count,
        "win_rate": round(win_rate, 1),
        "avg_pnl": round(avg_pnl, 3),
        "median_pnl": round(median_pnl, 3),
        "pf": round(pf, 3) if pf is not None else None,
        "cagr": round(cagr, 2),
        "max_dd": round(max_dd, 2),
        "sharpe": round(sharpe, 3),
        "monthly_consistency": round(monthly_consistency, 1),
        "yearly_consistency": round(yearly_consistency, 1),
        "worst_month": round(worst_month, 2),
        "best_month": round(best_month, 2),
        "worst_year": round(worst_year, 2),
        "best_year": round(best_year, 2),
        "avg_holding_days": round(avg_holding_days, 2),
        "exposure_ratio": round(exposure_ratio, 4),
        "year_concentration": round(year_concentration, 4) if year_concentration is not None else None,
        "month_concentration": round(month_concentration, 4) if month_concentration is not None else None,
        "total_pnl": round(equity, 2),
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "regime_stats": regime_stats,
    }


# ─── Scoring functions ────────────────────────────────────────────────────────

def _penalties(m: dict) -> dict:
    trade_count = m.get("trade_count", 0)
    year_conc = m.get("year_concentration") or 0
    month_conc = m.get("month_concentration") or 0

    if trade_count < 10:
        low_trade_penalty = 100
    elif trade_count < 20:
        low_trade_penalty = 30
    elif trade_count < 30:
        low_trade_penalty = 10
    else:
        low_trade_penalty = 0

    year_conc_penalty = 20 if year_conc > 0.5 else (10 if year_conc > 0.35 else 0)
    month_conc_penalty = 15 if month_conc > 0.4 else 0

    regime_stats = m.get("regime_stats") or {}
    active_regimes = [r for r, s in regime_stats.items() if s.get("trade_count", 0) >= 5]
    regime_fragility_penalty = 10 if len(active_regimes) <= 1 and len(regime_stats) > 1 else 0

    total = low_trade_penalty + year_conc_penalty + month_conc_penalty + regime_fragility_penalty
    return {
        "low_trade_penalty": low_trade_penalty,
        "year_concentration_penalty": year_conc_penalty,
        "month_concentration_penalty": month_conc_penalty,
        "regime_fragility_penalty": regime_fragility_penalty,
        "total_penalty": total,
    }


def _balanced_score(m: dict, penalties: dict, oos_penalty: float = 0) -> float:
    cagr = m.get("cagr", 0) or 0
    pf = m.get("pf", 0) or 0
    sharpe = m.get("sharpe", 0) or 0
    win_rate = m.get("win_rate", 0) or 0
    max_dd = abs(m.get("max_dd", 0) or 0)
    mc = m.get("monthly_consistency", 0) or 0
    exp = m.get("exposure_ratio", 0) or 0
    s = cagr * 2 + pf * 10 + sharpe * 5 + win_rate * 0.2 + mc * 20 + exp * 5 - max_dd * 1.5
    return s - penalties.get("total_penalty", 0) - oos_penalty


def _conservative_score(m: dict, penalties: dict, oos_penalty: float = 0) -> float:
    cagr = m.get("cagr", 0) or 0
    pf = m.get("pf", 0) or 0
    sharpe = m.get("sharpe", 0) or 0
    max_dd = abs(m.get("max_dd", 0) or 0)
    mc = m.get("monthly_consistency", 0) or 0
    worst_m = abs(m.get("worst_month", 0) or 0)
    s = cagr * 1 + pf * 12 + sharpe * 8 + mc * 25 - max_dd * 3 - worst_m * 2
    return s - penalties.get("total_penalty", 0) - oos_penalty


def _aggressive_score(m: dict, penalties: dict, oos_penalty: float = 0) -> float:
    cagr = m.get("cagr", 0) or 0
    pf = m.get("pf", 0) or 0
    sharpe = m.get("sharpe", 0) or 0
    win_rate = m.get("win_rate", 0) or 0
    max_dd = abs(m.get("max_dd", 0) or 0)
    s = cagr * 3 + pf * 7 + sharpe * 3 + win_rate * 0.1 - max_dd * 1.0
    return s - penalties.get("total_penalty", 0) - oos_penalty


# ─── OOS combo runner ─────────────────────────────────────────────────────────

def _run_combo_with_oos(
    params: dict,
    train_candidates: list[dict],
    test_candidates: list[dict],
    nikkei_gaps: dict[str, float],
) -> dict | None:
    train_r = _run_combo(params, train_candidates, nikkei_gaps)
    if train_r is None:
        return None

    test_r = _run_combo(params, test_candidates, nikkei_gaps)

    test_trade_count = (test_r or {}).get("trade_count", 0)
    test_pf = (test_r or {}).get("pf") or 0
    test_max_dd = abs((test_r or {}).get("max_dd", 0) or 0)
    train_pf = train_r.get("pf") or 0

    oos_pass = (
        test_trade_count >= 10
        and test_pf >= 1.1
        and test_max_dd <= 50
        and (train_pf == 0 or test_pf / train_pf >= 0.6)
    )

    oos_penalty: float = 0.0
    if test_r is None or test_trade_count < 5:
        oos_penalty = 30.0
    elif not oos_pass:
        oos_penalty = 15.0

    penalties = _penalties(train_r)
    params_prefixed = {f"p_{k}": v for k, v in params.items()}

    return {
        "strategy_id": _strategy_id(params),
        "params_hash": _params_hash(params),
        **params_prefixed,
        **{f"train_{k}": v for k, v in train_r.items()
           if k not in {"monthly_returns", "yearly_returns", "regime_stats"}},
        **{f"test_{k}": v for k, v in (test_r or {}).items()
           if k not in {"monthly_returns", "yearly_returns", "regime_stats"}},
        "oos_pass": oos_pass,
        "oos_penalty": oos_penalty,
        **penalties,
        "balanced_score": round(_balanced_score(train_r, penalties, oos_penalty), 3),
        "conservative_score": round(_conservative_score(train_r, penalties, oos_penalty), 3),
        "aggressive_score": round(_aggressive_score(train_r, penalties, oos_penalty), 3),
        "train_monthly_returns": train_r.get("monthly_returns") or {},
        "train_yearly_returns": train_r.get("yearly_returns") or {},
        "train_regime_stats": train_r.get("regime_stats") or {},
    }


# ─── Worker globals (multiprocessing initializer) ─────────────────────────────

_g_train_candidates: list[dict] = []
_g_test_candidates: list[dict] = []
_g_nikkei_gaps: dict[str, float] = {}


def _worker_init(train_pkl: bytes, test_pkl: bytes, nikkei_pkl: bytes) -> None:
    global _g_train_candidates, _g_test_candidates, _g_nikkei_gaps
    _g_train_candidates = pickle.loads(train_pkl)
    _g_test_candidates = pickle.loads(test_pkl)
    _g_nikkei_gaps = pickle.loads(nikkei_pkl)


def _worker_run(params: dict) -> dict:
    try:
        result = _run_combo_with_oos(
            params, _g_train_candidates, _g_test_candidates, _g_nikkei_gaps
        )
        if result is None:
            return {"_skip": True, "params_hash": _params_hash(params)}
        return result
    except Exception as e:
        return {
            "_error": str(e),
            "strategy_id": _strategy_id(params),
            "params_hash": _params_hash(params),
        }


# ─── Combo generation ─────────────────────────────────────────────────────────

def _all_combos(space: dict[str, list]) -> list[dict]:
    keys = list(space.keys())
    return [dict(zip(keys, combo)) for combo in product(*[space[k] for k in keys])]


def _random_combos(space: dict[str, list], n: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    keys = list(space.keys())
    return [{k: rng.choice(space[k]) for k in keys} for _ in range(n)]


def _weighted_combos(
    space: dict[str, list],
    n: int,
    good_values: dict[str, Any],
    good_prob: float,
    seed: int = 42,
) -> list[dict]:
    """Sample combos, using good_values with good_prob probability per param."""
    rng = random.Random(seed)
    keys = list(space.keys())
    result = []
    for _ in range(n):
        combo = {}
        for k in keys:
            if k in good_values and rng.random() < good_prob:
                combo[k] = good_values[k]
            else:
                combo[k] = rng.choice(space[k])
        result.append(combo)
    return result


def _tight_combos(
    space: dict[str, list],
    best_params: dict,
    n: int,
    best_prob: float = 0.80,
    seed: int = 99,
) -> list[dict]:
    """Generate combos tightly around best_params."""
    rng = random.Random(seed)
    keys = list(space.keys())
    result = []
    for _ in range(n):
        combo = {}
        for k in keys:
            if rng.random() < best_prob and k in best_params:
                combo[k] = best_params[k]
            else:
                combo[k] = rng.choice(space[k])
        result.append(combo)
    return result


def _staged_combos(sample_n: int, seed: int = 42) -> tuple[list[dict], str]:
    """Generate combos in stages, returning (combos, description)."""
    n1 = int(sample_n * 0.5)
    n3 = int(sample_n * 0.4)
    n4 = sample_n - n1 - n3

    stage1 = _random_combos(PARAM_SPACE, n1, seed=seed)

    # Run stage1 in-process to find good values (light; just return stage1 now,
    # caller will handle staged execution via staged run logic)
    # Return all staged combos concatenated; staged refinement happens post-batch
    stage3 = _random_combos(PARAM_SPACE, n3, seed=seed + 1)
    stage4 = _random_combos(PARAM_SPACE, n4, seed=seed + 2)

    all_combos = stage1 + stage3 + stage4
    desc = f"staged: stage1={n1} stage3={n3} stage4={n4}"
    return all_combos, desc


def _refine_staged_combos(
    stage1_results: list[dict],
    sample_n: int,
    seed: int = 42,
) -> list[dict]:
    """Given stage1 results, generate refined stage3 and stage4 combos."""
    n3 = int(sample_n * 0.4)
    n4 = sample_n - int(sample_n * 0.5) - n3

    # Find top 5% by balanced_score
    if not stage1_results:
        return _random_combos(PARAM_SPACE, n3 + n4, seed=seed + 10)

    sorted_r = sorted(stage1_results, key=lambda r: r.get("balanced_score", 0), reverse=True)
    top5pct = sorted_r[: max(1, len(sorted_r) // 20)]

    # Extract most common param value among top 5%
    keys = list(PARAM_SPACE.keys())
    good_values: dict[str, Any] = {}
    for k in keys:
        counts: Counter = Counter(str(r.get(f"p_{k}")) for r in top5pct if f"p_{k}" in r)
        if counts:
            best_str = counts.most_common(1)[0][0]
            # Find actual typed value in PARAM_SPACE
            for v in PARAM_SPACE[k]:
                if str(v) == best_str:
                    good_values[k] = v
                    break
            else:
                good_values[k] = PARAM_SPACE[k][0]

    stage3 = _weighted_combos(PARAM_SPACE, n3, good_values, good_prob=0.70, seed=seed + 3)

    # Stage4: tight around absolute best
    best_params = {k[2:]: v for k, v in sorted_r[0].items() if k.startswith("p_")}
    stage4 = _tight_combos(PARAM_SPACE, best_params, n4, best_prob=0.80, seed=seed + 4)

    return stage3 + stage4


# ─── Market regime attachment and validation ──────────────────────────────────

def _attach_and_validate_regime(
    candidates: list[dict],
    sb: Any,
    allow_missing: bool = False,
) -> None:
    if not candidates:
        return
    dates_in = sorted({str(r.get("trade_date")) for r in candidates if r.get("trade_date")})
    if not dates_in:
        return
    try:
        rows: list[dict] = []
        page_size = 1000
        offset = 0
        while True:
            page = (
                sb.table("market_regime")
                .select("trade_date,mode,nikkei_change_pct,nikkei_ma25_gap")
                .gte("trade_date", dates_in[0])
                .lte("trade_date", dates_in[-1])
                .order("trade_date")
                .range(offset, offset + page_size - 1)
                .execute()
                .data or []
            )
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        logger.info("[grid_search] market_regime fetched total=%d", len(rows))
    except Exception as e:
        logger.warning("[grid_search] market_regime load failed: %s", e)
        for row in candidates:
            row.setdefault("market_regime", "normal")
        return

    regime_map = {str(r["trade_date"]): r for r in rows}
    missing_count = 0
    for row in candidates:
        entry = regime_map.get(str(row.get("trade_date") or ""), {})
        if entry:
            row["market_regime"] = entry.get("mode") or "normal"
            row["nikkei_change_pct"] = entry.get("nikkei_change_pct")
            row["nikkei_ma25_gap"] = entry.get("nikkei_ma25_gap")
        else:
            row.setdefault("market_regime", "normal")
            missing_count += 1

    missing_ratio = missing_count / len(candidates) if candidates else 0.0
    logger.info(
        "[grid_search] market_regime attached rows=%d regimes=%d missing=%d (%.1f%%)",
        len(candidates), len(regime_map), missing_count, missing_ratio * 100,
    )
    if missing_ratio > 0.01 and not allow_missing:
        raise RuntimeError(
            f"market_regime missing for {missing_ratio:.1%} of candidates. "
            "Run: python scripts/backfill_market_regime.py --start 2020-01-01 --end 2026-05-26 --force"
        )
    elif missing_ratio > 0:
        logger.warning("[grid_search] market_regime missing_ratio=%.1f%%", missing_ratio * 100)


def _load_nikkei_gaps_from_regime(candidates: list[dict]) -> dict[str, float]:
    """Build nikkei_ma25_gap dict from already-attached regime data on candidates."""
    gaps: dict[str, float] = {}
    for row in candidates:
        d = str(row.get("trade_date") or "")
        val = row.get("nikkei_ma25_gap")
        if d and val is not None:
            try:
                gaps[d] = float(val)
            except Exception:
                pass
    return gaps


# ─── CSV helpers ─────────────────────────────────────────────────────────────

_SKIP_COLS = {"monthly_returns", "yearly_returns", "regime_stats",
              "train_monthly_returns", "train_yearly_returns", "train_regime_stats"}


def _flatten_row(row: dict, skip: set[str] | None = None) -> dict:
    skip = skip or _SKIP_COLS
    return {k: v for k, v in row.items() if k not in skip}


def _write_csv(path: Path, rows: list[dict], skip: set[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    skip = skip or _SKIP_COLS
    fields: list[str] = []
    for row in rows:
        for k in row:
            if k not in skip and k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _append_csv(path: Path, rows: list[dict], skip: set[str] | None = None) -> None:
    """Append rows to CSV; write header only if file doesn't exist yet."""
    if not rows:
        return
    skip = skip or _SKIP_COLS
    flat = [_flatten_row(r, skip) for r in rows]
    fields: list[str] = []
    for row in flat:
        for k in row:
            if k not in fields:
                fields.append(k)

    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(flat)


# ─── Parameter importance and condition effects ───────────────────────────────

def _build_parameter_importance(results: list[dict]) -> list[dict]:
    if not results:
        return []
    keys = list(PARAM_SPACE.keys())
    rows = []
    for k in keys:
        param_key = f"p_{k}"
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in results:
            val = str(r.get(param_key, ""))
            groups[val].append(r)
        for val, group in sorted(groups.items()):
            bs = [r.get("balanced_score", 0) or 0 for r in group]
            cagr_list = [r.get("train_cagr", 0) or 0 for r in group]
            pf_list = [r.get("train_pf", 0) or 0 for r in group]
            sharpe_list = [r.get("train_sharpe", 0) or 0 for r in group]
            dd_list = [abs(r.get("train_max_dd", 0) or 0) for r in group]
            tc_list = [r.get("train_trade_count", 0) or 0 for r in group]
            oos_list = [1 if r.get("oos_pass") else 0 for r in group]
            rows.append({
                "parameter_name": k,
                "parameter_value": val,
                "result_count": len(group),
                "avg_balanced_score": round(statistics.mean(bs), 3) if bs else 0,
                "median_balanced_score": round(statistics.median(bs), 3) if bs else 0,
                "avg_train_cagr": round(statistics.mean(cagr_list), 3) if cagr_list else 0,
                "avg_train_pf": round(statistics.mean(pf_list), 3) if pf_list else 0,
                "avg_train_sharpe": round(statistics.mean(sharpe_list), 3) if sharpe_list else 0,
                "avg_train_max_dd": round(statistics.mean(dd_list), 3) if dd_list else 0,
                "avg_train_trade_count": round(statistics.mean(tc_list), 1) if tc_list else 0,
                "oos_pass_rate": round(statistics.mean(oos_list), 3) if oos_list else 0,
            })
    return rows


BASELINES: dict[str, str] = {
    "exit_mode": "pullback2",
    "entry_mode": "ai_close_entry",
    "regime_filter": "all",
    "stop_loss_pct": "-4.0",
    "max_margin_ratio": "None",
    "panic_guard": "off",
    "sector_limit": "off",
    "signal_rsi_max": "None",
    "ma5_gap_max": "None",
    "nikkei_ma25_gap_limit": "None",
}


def _build_condition_effects(results: list[dict]) -> list[dict]:
    if not results:
        return []
    rows = []
    for k, baseline_val in BASELINES.items():
        param_key = f"p_{k}"
        baseline_rows = [r for r in results if str(r.get(param_key, "")) == baseline_val]
        if not baseline_rows:
            continue
        base_bs = statistics.mean([r.get("balanced_score", 0) or 0 for r in baseline_rows])
        base_cagr = statistics.mean([r.get("train_cagr", 0) or 0 for r in baseline_rows])
        base_pf = statistics.mean([r.get("train_pf", 0) or 0 for r in baseline_rows])
        base_sharpe = statistics.mean([r.get("train_sharpe", 0) or 0 for r in baseline_rows])
        base_dd = statistics.mean([abs(r.get("train_max_dd", 0) or 0) for r in baseline_rows])
        base_tc = statistics.mean([r.get("train_trade_count", 0) or 0 for r in baseline_rows])

        for val in PARAM_SPACE[k]:
            val_str = str(val)
            if val_str == baseline_val:
                continue
            group = [r for r in results if str(r.get(param_key, "")) == val_str]
            if not group:
                continue
            g_bs = statistics.mean([r.get("balanced_score", 0) or 0 for r in group])
            g_cagr = statistics.mean([r.get("train_cagr", 0) or 0 for r in group])
            g_pf = statistics.mean([r.get("train_pf", 0) or 0 for r in group])
            g_sharpe = statistics.mean([r.get("train_sharpe", 0) or 0 for r in group])
            g_dd = statistics.mean([abs(r.get("train_max_dd", 0) or 0) for r in group])
            g_tc = statistics.mean([r.get("train_trade_count", 0) or 0 for r in group])
            rows.append({
                "parameter": k,
                "value": val_str,
                "baseline_value": baseline_val,
                "result_count": len(group),
                "delta_balanced_score": round(g_bs - base_bs, 3),
                "delta_cagr": round(g_cagr - base_cagr, 3),
                "delta_pf": round(g_pf - base_pf, 3),
                "delta_sharpe": round(g_sharpe - base_sharpe, 3),
                "delta_max_dd": round(g_dd - base_dd, 3),
                "delta_trade_count": round(g_tc - base_tc, 1),
            })
    return rows


# ─── Equity curve plot ────────────────────────────────────────────────────────

def _plot_equity_curves(top_results: list[dict], path: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(14, 6))
        for result in top_results[:10]:
            monthly = result.get("train_monthly_returns") or {}
            if not monthly:
                continue
            months = sorted(monthly)
            cum = 0.0
            xs, ys = [], []
            for m in months:
                cum += monthly[m]
                xs.append(m)
                ys.append(cum)
            step = max(1, len(xs) // 60)
            ax.plot(xs[::step], ys[::step],
                    label=result.get("strategy_id", "")[:40], alpha=0.8)

        ax.set_title("Top 10 Strategy Equity Curves (monthly cumulative % PnL)")
        ax.set_xlabel("Month")
        ax.set_ylabel("Cumulative PnL (%)")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=30, fontsize=7)
        plt.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(str(path), dpi=120)
        plt.close()
        logger.info("[grid_search] equity_curve saved: %s", path)
    except ImportError:
        logger.warning("[grid_search] matplotlib not available; equity curve skipped")
    except Exception as e:
        logger.warning("[grid_search] equity_curve plot failed: %s", e)


# ─── Regime breakdown ─────────────────────────────────────────────────────────

def _build_regime_breakdown(
    top_results: list[dict],
    train_candidates: list[dict],
    nikkei_gaps: dict[str, float],
) -> list[dict]:
    rows = []
    regimes = sorted({
        str(c.get("market_regime") or "normal")
        for c in train_candidates if c.get("market_regime")
    })
    for result in top_results[:5]:
        params = {k[2:]: v for k, v in result.items() if k.startswith("p_")}
        for regime in regimes:
            regime_candidates = [
                c for c in train_candidates
                if str(c.get("market_regime") or "normal") == regime
            ]
            if not regime_candidates:
                continue
            override = dict(params)
            override["regime_filter"] = "all"
            mini = _run_combo(override, regime_candidates, nikkei_gaps)
            rows.append({
                "strategy_id": result.get("strategy_id", ""),
                "regime": regime,
                "trade_count": mini.get("trade_count", 0) if mini else 0,
                "win_rate": mini.get("win_rate") if mini else None,
                "avg_pnl": mini.get("avg_pnl") if mini else None,
                "pf": mini.get("pf") if mini else None,
                "cagr": mini.get("cagr") if mini else None,
                "max_dd": mini.get("max_dd") if mini else None,
                "sharpe": mini.get("sharpe") if mini else None,
                "balanced_score": result.get("balanced_score"),
            })
    return rows


# ─── Auto report ─────────────────────────────────────────────────────────────

def _auto_report(
    results: list[dict],
    top_n: int,
    elapsed: float,
    total_combos: int,
    start: date,
    end: date,
    train_end: date | None,
) -> list[str]:
    lines = ["=" * 70, "AUTO REPORT - Grid Search Rebound Strategy Optimizer", "=" * 70]

    # 1. Run summary
    valid = len(results)
    oos_pass_count = sum(1 for r in results if r.get("oos_pass"))
    lines.append(f"\n[Run Summary]")
    lines.append(f"  Period        : {start} to {end}")
    lines.append(f"  Train end     : {train_end or 'N/A (no OOS split)'}")
    lines.append(f"  Total combos  : {total_combos:,}")
    lines.append(f"  Valid results : {valid:,}")
    lines.append(f"  OOS-passing   : {oos_pass_count:,}")
    lines.append(f"  Elapsed       : {elapsed:.0f}s ({elapsed/60:.1f}min)")

    if not results:
        lines.append("\nNo valid results found.")
        return lines

    top_by_balanced = sorted(results, key=lambda r: r.get("balanced_score", 0), reverse=True)
    top_by_cons = sorted(results, key=lambda r: r.get("conservative_score", 0), reverse=True)
    top_by_agg = sorted(results, key=lambda r: r.get("aggressive_score", 0), reverse=True)

    def _fmt(r: dict) -> str:
        return (
            f"  {r.get('strategy_id','')[:50]}"
            f"  bal={r.get('balanced_score', 0):.1f}"
            f"  CAGR={r.get('train_cagr', 0):.1f}%"
            f"  DD={r.get('train_max_dd', 0):.1f}%"
            f"  PF={r.get('train_pf')}"
            f"  Sharpe={r.get('train_sharpe', 0):.2f}"
            f"  mc={r.get('train_monthly_consistency', 0):.0f}%"
            f"  trades={r.get('train_trade_count', 0)}"
            f"  OOS={'Y' if r.get('oos_pass') else 'N'}"
        )

    # 2. Top 10 balanced
    lines.append(f"\n[Top 10 Balanced Score]")
    for i, r in enumerate(top_by_balanced[:10], 1):
        lines.append(f"  #{i}: " + _fmt(r))

    # 3. Top 10 conservative
    lines.append(f"\n[Top 10 Conservative Score]")
    for i, r in enumerate(top_by_cons[:10], 1):
        lines.append(f"  #{i}: " + _fmt(r))

    # 4. Top 10 aggressive
    lines.append(f"\n[Top 10 Aggressive Score]")
    for i, r in enumerate(top_by_agg[:10], 1):
        lines.append(f"  #{i}: " + _fmt(r))

    # 5. OOS-passing strategies
    oos_results = [r for r in results if r.get("oos_pass")]
    lines.append(f"\n[OOS-Passing Strategies: {len(oos_results)}]")
    oos_sorted = sorted(oos_results, key=lambda r: r.get("balanced_score", 0), reverse=True)
    for i, r in enumerate(oos_sorted[:5], 1):
        lines.append(f"  #{i}: " + _fmt(r))

    # 6. Parameter tendencies
    def _freq(param_key: str, label: str, top_list: list[dict]) -> None:
        c: Counter = Counter(str(r.get(param_key, "")) for r in top_list[:min(50, top_n)])
        lines.append(f"\n[{label} (上位{min(50, top_n)}件中)]")
        for val, cnt in c.most_common():
            lines.append(f"  {val}: {cnt}件 ({cnt / len(top_list[:min(50,top_n)]) * 100:.0f}%)")

    _freq("p_exit_mode", "Exit mode tendency", top_by_balanced)
    _freq("p_entry_mode", "Entry mode tendency", top_by_balanced)
    _freq("p_regime_filter", "Regime filter tendency", top_by_balanced)
    _freq("p_stop_loss_pct", "Stop loss tendency", top_by_balanced)
    _freq("p_panic_guard", "Panic guard tendency", top_by_balanced)
    _freq("p_max_holding_days", "Max holding days tendency", top_by_balanced)
    _freq("p_max_positions", "Max positions tendency", top_by_balanced)
    _freq("p_sector_limit", "Sector limit tendency", top_by_balanced)

    # 7. DD-reducing conditions
    lines.append(f"\n[DD-Reducing Conditions (top 20 by max_dd)]")
    low_dd = sorted(
        [r for r in results if r.get("train_max_dd") is not None],
        key=lambda r: abs(r.get("train_max_dd", 0))
    )[:20]
    if low_dd:
        _freq("p_exit_mode", "  Exit mode (low DD)", low_dd)
        _freq("p_panic_guard", "  Panic guard (low DD)", low_dd)
        _freq("p_regime_filter", "  Regime filter (low DD)", low_dd)

    # 8. PF-increasing conditions
    lines.append(f"\n[PF-Increasing Conditions (top 20 by PF)]")
    high_pf = sorted(
        [r for r in results if r.get("train_pf") is not None],
        key=lambda r: r.get("train_pf", 0),
        reverse=True
    )[:20]
    if high_pf:
        _freq("p_entry_mode", "  Entry mode (high PF)", high_pf)
        _freq("p_exit_mode", "  Exit mode (high PF)", high_pf)

    # 9. Overfit suspects
    suspects = [
        r for r in results
        if (r.get("train_trade_count", 0) < 20)
        or (r.get("year_concentration_penalty", 0) > 0)
        or (r.get("month_concentration_penalty", 0) > 0)
    ]
    lines.append(f"\n[Overfit Suspects: {len(suspects)} results]")
    lines.append(f"  (low trade count, concentrated profits by year/month)")

    # 10. Recommended next steps
    lines.append(f"\n[Recommended Next Steps]")
    if oos_results:
        best_oos = oos_sorted[0]
        lines.append(f"  1. Forward-test: {best_oos.get('strategy_id', '')}")
        lines.append(f"     bal_score={best_oos.get('balanced_score', 0):.1f}")
        lines.append(f"     train_CAGR={best_oos.get('train_cagr', 0):.1f}% / test_CAGR={best_oos.get('test_cagr', 'N/A')}")
    lines.append(f"  2. Investigate exit_mode and regime_filter combinations")
    lines.append(f"  3. Run with larger sample_n for more reliable signal")

    lines.append("\n" + "=" * 70)
    return lines


# ─── Monthly / yearly output ──────────────────────────────────────────────────

def _build_monthly_returns_csv(results: list[dict], n: int = 20) -> list[dict]:
    rows = []
    for r in results[:n]:
        sid = r.get("strategy_id", "")
        for month, ret in sorted((r.get("train_monthly_returns") or {}).items()):
            rows.append({"strategy_id": sid, "month": month, "return_pct": round(ret, 3)})
    return rows


def _build_yearly_returns_csv(results: list[dict], n: int = 20) -> list[dict]:
    rows = []
    for r in results[:n]:
        sid = r.get("strategy_id", "")
        for year, ret in sorted((r.get("train_yearly_returns") or {}).items()):
            rows.append({"strategy_id": sid, "year": year, "return_pct": round(ret, 3)})
    return rows


# ─── Data loading ─────────────────────────────────────────────────────────────

def _load_labels_chunked(sb, start_s: str, end_s: str, label_cols: list[str], chunk_days: int = 60) -> list[dict]:
    """Load stock_rebound_labels by date chunks to avoid OFFSET pagination timeout."""
    start_d = date.fromisoformat(start_s)
    end_d = date.fromisoformat(end_s)
    all_labels: list[dict] = []
    current = start_d
    while current <= end_d:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_d)
        attempt = 0
        while attempt < 4:
            try:
                data = (
                    sb.table("stock_rebound_labels")
                    .select(",".join(label_cols))
                    .gte("trade_date", current.isoformat())
                    .lte("trade_date", chunk_end.isoformat())
                    .not_.is_("future_high_5d", "null")
                    .not_.is_("future_low_5d", "null")
                    .order("trade_date")
                    .execute()
                    .data or []
                )
                all_labels.extend(data)
                logger.info("[grid_search] labels chunk %s..%s => %d rows (total %d)", current, chunk_end, len(data), len(all_labels))
                break
            except Exception as e:
                attempt += 1
                wait = 5 * attempt
                logger.warning("[grid_search] labels chunk %s..%s attempt %d failed: %s — retry in %ds", current, chunk_end, attempt, e, wait)
                time.sleep(wait)
                if attempt >= 4:
                    raise
        current = chunk_end + timedelta(days=1)
    return all_labels


def _load_candidates_for_grid(sb, period_start: date, period_end: date) -> list[dict]:
    """Timeout-safe candidate loader using date-chunked label loading (no OFFSET)."""
    snap_cols = sorted(set(
        ["id", "trade_date", "code", "name", "market", "sector", "close",
         "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
         "volume_ratio_20d", "bad_news_score", "market_shock_score"]
        + list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES)
    ))
    future_cols: list[str] = []
    for day in range(1, MAX_FUTURE_DAYS + 1):
        future_cols += [f"future_high_{day}d", f"future_low_{day}d", f"future_close_{day}d"]
    label_cols = ["id", "feature_snapshot_id", "trade_date", "code", "entry_price"] + future_cols

    labels = _load_labels_chunked(sb, period_start.isoformat(), period_end.isoformat(), label_cols)
    logger.info("[grid_search] labels loaded rows=%d", len(labels))

    snap_ids = [int(r["feature_snapshot_id"]) for r in labels if r.get("feature_snapshot_id")]
    if not snap_ids:
        logger.warning("[grid_search] no labels for period %s..%s", period_start, period_end)
        return []

    snapshots = _fetch_snapshots_by_ids(sb, snap_ids, snap_cols)
    logger.info("[grid_search] snapshots loaded rows=%d", len(snapshots))

    snap_by_id = {
        str(s["id"]): s
        for s in snapshots
        if s.get("is_drop_candidate") and s.get("is_tradeable")
    }

    rows: list[dict] = []
    for label in labels:
        snap = snap_by_id.get(str(label.get("feature_snapshot_id")))
        if not snap:
            continue
        merged = dict(snap)
        for key, value in label.items():
            if key in {"id", "code", "trade_date"}:
                merged[f"label_{key}"] = value
            else:
                merged[key] = value
        rows.append(merged)

    logger.info("[grid_search] merged candidate rows=%d", len(rows))
    _attach_weekly_margin(rows, _load_weekly_margin_rows(sb, period_start, period_end))
    return _score_candidates(rows, _active_model_bundle(sb))


def _load_data(
    args: argparse.Namespace,
    sb: Any,
    start: date,
    end: date,
) -> tuple[list[dict], dict[str, float]]:
    cache_path = Path(args.cache_file) if args.cache_file else None

    if cache_path and cache_path.exists():
        logger.info("[grid_search] loading from cache: %s", cache_path)
        with cache_path.open("rb") as f:
            data = pickle.load(f)
        candidates = data.get("candidates", [])
        nikkei_gaps = data.get("nikkei_gaps", {})
        logger.info("[grid_search] cache loaded candidates=%d nikkei_gaps=%d", len(candidates), len(nikkei_gaps))
        return candidates, nikkei_gaps

    t0 = time.time()
    logger.info("[grid_search] loading candidates from DB %s..%s", start, end)
    candidates = _load_candidates_for_grid(sb, start, end)
    logger.info("[grid_search] candidates=%d elapsed=%.1fs", len(candidates), time.time() - t0)

    _attach_and_validate_regime(candidates, sb, allow_missing=args.allow_missing_regime)
    nikkei_gaps = _load_nikkei_gaps_from_regime(candidates)
    logger.info("[grid_search] nikkei_gaps loaded from regime data entries=%d", len(nikkei_gaps))

    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("wb") as f:
            pickle.dump({"candidates": candidates, "nikkei_gaps": nikkei_gaps}, f)
        logger.info("[grid_search] cache saved: %s", cache_path)

    return candidates, nikkei_gaps


# ─── Progress / resume ────────────────────────────────────────────────────────

def _load_processed_hashes(run_dir: Path) -> set[str]:
    path = run_dir / "processed_hashes.txt"
    if not path.exists():
        return set()
    return set(path.read_text(encoding="utf-8").splitlines())


def _append_processed_hashes(run_dir: Path, hashes: list[str]) -> None:
    path = run_dir / "processed_hashes.txt"
    with path.open("a", encoding="utf-8") as f:
        for h in hashes:
            f.write(h + "\n")


def _save_run_config(run_dir: Path, args: argparse.Namespace) -> None:
    cfg = {k: str(v) for k, v in vars(args).items()}
    cfg["_timestamp"] = datetime.now(JST).isoformat()
    (run_dir / "run_config.json").write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _load_run_config(run_dir: Path) -> dict:
    path = run_dir / "run_config.json"
    if not path.exists():
        raise FileNotFoundError(f"run_config.json not found in {run_dir}")
    return json.loads(path.read_text(encoding="utf-8"))


# ─── Batch runner ─────────────────────────────────────────────────────────────

def _run_batches(
    combos: list[dict],
    train_candidates: list[dict],
    test_candidates: list[dict],
    nikkei_gaps: dict[str, float],
    n_workers: int,
    run_dir: Path,
    processed_hashes: set[str],
    progress_every: int = 1000,
) -> list[dict]:
    all_results: list[dict] = []
    error_rows: list[dict] = []
    new_valid: list[dict] = []
    new_hashes: list[str] = []
    t_start = time.time()
    done_count = 0
    valid_since_save = 0

    chunksize = max(1, min(500, len(combos) // max(1, n_workers * 4)))

    if n_workers > 1:
        train_pkl = pickle.dumps(train_candidates)
        test_pkl = pickle.dumps(test_candidates)
        nk_pkl = pickle.dumps(nikkei_gaps)
        pool_kwargs = {
            "processes": n_workers,
            "initializer": _worker_init,
            "initargs": (train_pkl, test_pkl, nk_pkl),
        }
        with Pool(**pool_kwargs) as pool:
            for raw in pool.imap_unordered(_worker_run, combos, chunksize=chunksize):
                done_count += 1
                _process_raw(
                    raw, all_results, new_valid, new_hashes, error_rows,
                    processed_hashes
                )
                valid_since_save += 1 if (raw and not raw.get("_skip") and not raw.get("_error")) else 0

                if valid_since_save >= progress_every:
                    _flush_progress(run_dir, new_valid, new_hashes, error_rows)
                    new_valid, new_hashes, error_rows = [], [], []
                    valid_since_save = 0
                    _log_progress(done_count, len(combos), len(all_results), t_start)
    else:
        for params in combos:
            done_count += 1
            raw = _worker_run(params)
            _process_raw(
                raw, all_results, new_valid, new_hashes, error_rows,
                processed_hashes
            )
            valid_since_save += 1 if (raw and not raw.get("_skip") and not raw.get("_error")) else 0

            if valid_since_save >= progress_every:
                _flush_progress(run_dir, new_valid, new_hashes, error_rows)
                new_valid, new_hashes, error_rows = [], [], []
                valid_since_save = 0
                _log_progress(done_count, len(combos), len(all_results), t_start)

    # Final flush
    if new_valid or new_hashes or error_rows:
        _flush_progress(run_dir, new_valid, new_hashes, error_rows)

    return all_results


def _process_raw(
    raw: dict | None,
    all_results: list[dict],
    new_valid: list[dict],
    new_hashes: list[str],
    error_rows: list[dict],
    processed_hashes: set[str],
) -> None:
    if raw is None:
        return
    h = raw.get("params_hash", "")
    if raw.get("_skip"):
        if h:
            processed_hashes.add(h)
            new_hashes.append(h)
        return
    if raw.get("_error"):
        error_rows.append({"params_hash": h, "error": raw["_error"],
                           "strategy_id": raw.get("strategy_id", "")})
        if h:
            processed_hashes.add(h)
            new_hashes.append(h)
        return
    all_results.append(raw)
    new_valid.append(raw)
    if h:
        processed_hashes.add(h)
        new_hashes.append(h)


def _flush_progress(
    run_dir: Path,
    new_valid: list[dict],
    new_hashes: list[str],
    error_rows: list[dict],
) -> None:
    if new_valid:
        _append_csv(run_dir / "progress.csv", new_valid)
    if new_hashes:
        _append_processed_hashes(run_dir, new_hashes)
    if error_rows:
        _append_csv(run_dir / "errors.csv", error_rows, skip=set())


def _log_progress(done: int, total: int, valid: int, t_start: float) -> None:
    elapsed = time.time() - t_start
    rate = done / elapsed if elapsed > 0 else 1
    remaining = (total - done) / rate if rate > 0 else 0
    pct = done / total * 100 if total else 0
    logger.info(
        "[grid_search] progress %.1f%% done=%d/%d valid=%d elapsed=%.0fs ETA=%.0fs(%.1fmin)",
        pct, done, total, valid, elapsed, remaining, remaining / 60,
    )


# ─── Main run ─────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    # ── Setup logging ─────────────────────────────────────────────────────
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")

    # ── Run directory setup ───────────────────────────────────────────────
    run_name = args.run_name or f"run_{timestamp}"
    run_dir = OUT_BASE / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    # Add file log handler
    if args.log_file:
        fh = logging.FileHandler(run_dir / "run.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(fh)

    # ── Resume or fresh start ─────────────────────────────────────────────
    if args.resume:
        try:
            cfg = _load_run_config(run_dir)
            logger.info("[grid_search] resuming run: %s", run_name)
            # Override key params from saved config
            args.start = cfg.get("start", args.start)
            args.end = cfg.get("end", args.end)
            args.train_end = cfg.get("train_end", args.train_end) or None
            if cfg.get("seed"):
                try:
                    args.seed = int(cfg["seed"])
                except Exception:
                    pass
            if cfg.get("sample_n"):
                try:
                    args.sample_n = int(cfg["sample_n"])
                except Exception:
                    pass
        except FileNotFoundError:
            logger.warning("[grid_search] no run_config.json found for %s; starting fresh", run_name)
            args.resume = False
    else:
        _save_run_config(run_dir, args)

    processed_hashes = _load_processed_hashes(run_dir)
    if processed_hashes:
        logger.info("[grid_search] resume: loaded %d processed hashes", len(processed_hashes))

    sb = build_supabase()
    start = _to_date(args.start)
    end = _to_date(args.end)
    train_end = _to_date(args.train_end) if args.train_end else None
    top_n = args.top_n
    n_workers = min(args.workers, os.cpu_count() or 1)

    logger.info("[grid_search] start=%s end=%s train_end=%s workers=%d run_name=%s",
                start, end, train_end, n_workers, run_name)

    # ── Data loading ──────────────────────────────────────────────────────
    t0 = time.time()
    all_candidates, nikkei_gaps = _load_data(args, sb, start, end)
    if not all_candidates:
        logger.error("[grid_search] no candidates loaded, aborting")
        return

    # ── Train / test split ────────────────────────────────────────────────
    if train_end is not None:
        train_candidates = [
            c for c in all_candidates
            if _to_date(str(c.get("trade_date") or "2000-01-01")) <= train_end
        ]
        test_candidates = [
            c for c in all_candidates
            if _to_date(str(c.get("trade_date") or "2000-01-01")) > train_end
        ]
        logger.info("[grid_search] train=%d test=%d (split at %s)",
                    len(train_candidates), len(test_candidates), train_end)
    else:
        train_candidates = all_candidates
        test_candidates = []
        logger.info("[grid_search] no OOS split; all %d candidates used for train", len(train_candidates))

    # ── Combo generation ──────────────────────────────────────────────────
    search_mode = args.search_mode
    sample_n = args.sample_n

    total_possible = 1
    for v in PARAM_SPACE.values():
        total_possible *= len(v)
    logger.info("[grid_search] total_possible_combos=%d", total_possible)

    if search_mode == "full":
        combos = _all_combos(PARAM_SPACE)
        logger.warning(
            "[grid_search] FULL GRID: %d combos — this will take very long!", len(combos)
        )
    elif search_mode == "random":
        combos = _random_combos(PARAM_SPACE, sample_n, seed=args.seed)
        logger.info("[grid_search] random sample: %d combos", len(combos))
    else:
        # staged: generate stage1 randomly, will refine after stage1 completes
        combos, desc = _staged_combos(sample_n, seed=args.seed)
        logger.info("[grid_search] staged search: %s total=%d combos", desc, len(combos))

    # Filter out already-processed hashes (resume)
    if processed_hashes:
        combos = [c for c in combos if _params_hash(c) not in processed_hashes]
        logger.info("[grid_search] after resume filter: %d combos remaining", len(combos))

    # For staged mode: handle stage refinement
    if search_mode == "staged" and not args.resume:
        n1 = int(sample_n * 0.5)
        stage1_combos = combos[:n1]
        remaining_combos = combos[n1:]

        logger.info("[grid_search] staged: running stage1 (%d combos)", len(stage1_combos))
        stage1_results = _run_batches(
            stage1_combos, train_candidates, test_candidates, nikkei_gaps,
            n_workers, run_dir, processed_hashes, args.progress_every,
        )
        logger.info("[grid_search] staged: stage1 done valid=%d", len(stage1_results))

        # Generate refined stage3+4 based on stage1 results
        refined = _refine_staged_combos(stage1_results, sample_n, seed=args.seed)
        # Filter already processed
        refined = [c for c in refined if _params_hash(c) not in processed_hashes]
        logger.info("[grid_search] staged: refined stage3+4 (%d combos)", len(refined))

        logger.info("[grid_search] staged: running remaining random combos (%d)", len(remaining_combos))
        more_results = _run_batches(
            remaining_combos + refined, train_candidates, test_candidates, nikkei_gaps,
            n_workers, run_dir, processed_hashes, args.progress_every,
        )
        all_run_results = stage1_results + more_results
    else:
        all_run_results = _run_batches(
            combos, train_candidates, test_candidates, nikkei_gaps,
            n_workers, run_dir, processed_hashes, args.progress_every,
        )

    elapsed = time.time() - t0
    logger.info(
        "[grid_search] simulation done valid=%d elapsed=%.1fs (%.1fmin)",
        len(all_run_results), elapsed, elapsed / 60,
    )

    if not all_run_results:
        logger.error("[grid_search] no valid results, aborting output")
        return

    # ── Deduplicate by params_hash ────────────────────────────────────────
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_run_results:
        h = r.get("params_hash", "")
        if h not in seen:
            seen.add(h)
            deduped.append(r)
    logger.info("[grid_search] after dedup: %d results", len(deduped))

    # Sort by balanced_score
    deduped.sort(key=lambda r: r.get("balanced_score", 0), reverse=True)
    top_balanced = deduped[:top_n]
    top_conservative = sorted(deduped, key=lambda r: r.get("conservative_score", 0), reverse=True)[:top_n]
    top_aggressive = sorted(deduped, key=lambda r: r.get("aggressive_score", 0), reverse=True)[:top_n]
    oos_pass_results = [r for r in deduped if r.get("oos_pass")]

    # ── Write output files ────────────────────────────────────────────────
    logger.info("[grid_search] writing output files to %s", run_dir)

    _write_csv(run_dir / "grid_search_results.csv", deduped)
    logger.info("[grid_search] grid_search_results.csv: %d rows", len(deduped))

    _write_csv(run_dir / "top_300_balanced.csv", top_balanced)
    _write_csv(run_dir / "top_300_conservative.csv", top_conservative)
    _write_csv(run_dir / "top_300_aggressive.csv", top_aggressive)
    logger.info("[grid_search] top_%d CSVs written", top_n)

    _write_csv(run_dir / "oos_validation.csv", oos_pass_results)
    logger.info("[grid_search] oos_validation.csv: %d rows", len(oos_pass_results))

    # Regime breakdown for top 5
    regime_rows = _build_regime_breakdown(deduped[:5], train_candidates, nikkei_gaps)
    if regime_rows:
        _write_csv(run_dir / "regime_breakdown.csv", regime_rows, skip=set())

    # Monthly / yearly returns for top 20
    monthly_rows = _build_monthly_returns_csv(deduped, n=20)
    if monthly_rows:
        _write_csv(run_dir / "monthly_returns.csv", monthly_rows, skip=set())

    yearly_rows = _build_yearly_returns_csv(deduped, n=20)
    if yearly_rows:
        _write_csv(run_dir / "yearly_returns.csv", yearly_rows, skip=set())

    # Equity curve
    _plot_equity_curves(deduped[:10], run_dir / "equity_curve_top10.png")

    # Parameter importance
    importance_rows = _build_parameter_importance(deduped)
    if importance_rows:
        _write_csv(run_dir / "parameter_importance.csv", importance_rows, skip=set())

    # Condition effects
    effect_rows = _build_condition_effects(deduped)
    if effect_rows:
        _write_csv(run_dir / "condition_effects.csv", effect_rows, skip=set())

    # Auto report
    total_combos_run = len(all_run_results) + len(processed_hashes)
    report_lines = _auto_report(
        deduped, top_n, elapsed, total_combos_run, start, end, train_end
    )
    (run_dir / "auto_report.txt").write_text("\n".join(report_lines), encoding="utf-8")
    for line in report_lines:
        logger.info(line)

    logger.info(
        "[grid_search] complete run_name=%s valid=%d oos_pass=%d elapsed=%.1fs",
        run_name, len(deduped), len(oos_pass_results), elapsed,
    )


# ─── CLI ─────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Grid search rebound strategy optimizer (overnight-run tool)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--start", default="2020-01-01",
                   help="Backtest start date (YYYY-MM-DD)")
    p.add_argument("--end", default="2026-04-28",
                   help="Backtest end date (YYYY-MM-DD)")
    p.add_argument("--train-end", default=None,
                   help="Train period end for OOS split (YYYY-MM-DD); test = after this date")
    p.add_argument("--search-mode", choices=["staged", "random", "full"],
                   default="staged",
                   help="staged=multi-phase refinement, random=pure random, full=brute force")
    p.add_argument("--sample-n", type=int, default=1_000_000,
                   help="Total combo sample count for staged/random modes")
    p.add_argument("--top-n", type=int, default=300,
                   help="Save top-N results per score type")
    p.add_argument("--workers", type=int,
                   default=max(1, (os.cpu_count() or 1) - 1),
                   help="Parallel worker count")
    p.add_argument("--seed", type=int, default=42,
                   help="Random seed for reproducible sampling")
    p.add_argument("--cache-file", default=None,
                   help="Pickle cache path for candidates (auto-saved on first run)")
    p.add_argument("--run-name", default=None,
                   help="Run directory name under outputs/rebound_grid_search/ (default: run_TIMESTAMP)")
    p.add_argument("--resume", action="store_true",
                   help="Resume an existing run by --run-name, skipping already-processed combos")
    p.add_argument("--progress-every", type=int, default=1000,
                   help="Flush progress.csv and processed_hashes.txt every N valid results")
    p.add_argument("--allow-missing-regime", action="store_true",
                   help="Don't abort if market_regime data is missing for >1% of candidates")
    p.add_argument("--log-file", action="store_true",
                   help="Also write logs to {run_dir}/run.log")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
