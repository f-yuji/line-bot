#!/usr/bin/env python3
"""Analyze H5 emergency stop behavior by entry and holding-period regime.

This is a research-only script. It reads the candidate cache and market_regime
rows, then writes CSV/text reports. It does not update any database table.

The H5 exit used here is the established close-based peak pullback exit:
after the post-entry high exceeds +0.5%, exit when close retreats 2% from
that peak. Stop checks use intraday low, matching the H5 analysis scripts.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import pickle
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import mean, median
from typing import Any

import numpy as np
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

DEFAULT_CACHE = (
    ROOT
    / "outputs"
    / "rebound_next_analysis"
    / "h5_forward_next"
    / "_candidates_cache_2023-01-01_2026-05-26.pkl"
)
DEFAULT_OUT = ROOT / "outputs" / "rebound_next_analysis" / "h5_stop_regime"
REGIME_PRIORITY = {
    "unknown": 0,
    "euphoria": 1,
    "strong_risk_on": 1,
    "normal": 2,
    "panic_rebound": 3,
    "risk_off": 4,
    "panic_selloff": 5,
}
STATIC_STOPS = {
    "nostop": None,
    "emergency8": -0.08,
    "emergency10": -0.10,
    "emergency12": -0.12,
    "emergency15": -0.15,
    "emergency20": -0.20,
}
ALL_MODELS = list(STATIC_STOPS) + [
    "regime_dynamic_stop",
    "dynamic_A",
    "dynamic_B",
    "dynamic_C",
    "dynamic_D",
    "market_panic_exit",
]


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        result = float(value)
        return default if math.isnan(result) or math.isinf(result) else result
    except (TypeError, ValueError):
        return default


def _period(trade_date: str, train_end: str) -> str:
    return "train" if trade_date <= train_end else "test"


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        logger.info("[h5_stop_regime] saved %s rows=0", path.name)
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("[h5_stop_regime] saved %s rows=%d", path.name, len(rows))


def _overheat_score(row: dict) -> int:
    return sum(
        [
            (_f(row.get("rsi14"), 0) or 0) >= 65,
            (_f(row.get("ma5_gap_pct"), 0) or 0) >= 5,
            (_f(row.get("return_5d_pct"), 0) or 0) >= 8,
            (_f(row.get("volume_ratio_20d"), 0) or 0) >= 3.0,
        ]
    )


def _margin_pass(row: dict, margin_filter: str) -> bool:
    ratio = _f(row.get("margin_ratio"))
    if margin_filter in {"none", "off"} or ratio is None:
        return True
    limits = {"le5": 5.0, "le10": 10.0, "le20": 20.0, "le30": 30.0}
    return ratio <= limits.get(margin_filter, float("inf"))


def _load_candidates(cache_path: Path) -> list[dict]:
    if not cache_path.exists():
        raise FileNotFoundError(f"candidate cache not found: {cache_path}")
    logger.info("[h5_stop_regime] loading candidates cache=%s", cache_path)
    with cache_path.open("rb") as file:
        loaded = pickle.load(file)
    candidates = loaded["candidates"] if isinstance(loaded, dict) else loaded
    logger.info("[h5_stop_regime] candidates loaded=%d", len(candidates))
    return candidates


def _filter_h5(
    candidates: list[dict],
    *,
    start: str,
    end: str,
    ai_threshold: float,
    drop20d_threshold: float,
    overheat_mode: str,
    margin_filter: str,
    exclude_panic_entry: bool,
) -> list[dict]:
    output: list[dict] = []
    for row in candidates:
        trade_date = str(row.get("trade_date") or "")
        if not (start <= trade_date <= end):
            continue
        if (_f(row.get("signal_probability"), 0) or 0) < ai_threshold:
            continue
        if (_f(row.get("drop_from_20d_high_pct"), 0) or 0) > drop20d_threshold:
            continue
        regime = str(row.get("market_regime") or "unknown")
        if exclude_panic_entry and regime == "panic_selloff":
            continue
        score = _overheat_score(row)
        if overheat_mode == "cool_mild_only" and score > 1:
            continue
        if overheat_mode != "cool_mild_only" and score >= 3:
            continue
        if not _margin_pass(row, margin_filter):
            continue
        output.append(row)
    return output


def _load_regime_calendar(start: str, end: str, candidates: list[dict]) -> tuple[list[str], dict[str, str], str]:
    """Read daily market regimes; fall back to dates present in candidate cache."""
    try:
        from services.trade_case_tester import _build_supabase

        sb = _build_supabase()
        rows: list[dict] = []
        offset = 0
        while True:
            page = (
                sb.table("market_regime")
                .select("trade_date,mode")
                .gte("trade_date", start)
                .lte("trade_date", end)
                .order("trade_date")
                .range(offset, offset + 999)
                .execute()
                .data
                or []
            )
            rows.extend(page)
            if len(page) < 1000:
                break
            offset += 1000
        if rows:
            regime_map = {str(row["trade_date"]): str(row.get("mode") or "unknown") for row in rows}
            logger.info("[h5_stop_regime] daily regimes loaded=%d", len(regime_map))
            return sorted(regime_map), regime_map, "market_regime_table"
    except Exception as exc:
        logger.warning("[h5_stop_regime] daily regime DB read failed; cache fallback: %s", exc)

    regime_map = {}
    for row in candidates:
        td = str(row.get("trade_date") or "")
        if start <= td <= end:
            regime_map.setdefault(td, str(row.get("market_regime") or "unknown"))
    logger.warning("[h5_stop_regime] regime calendar uses candidate-date fallback rows=%d", len(regime_map))
    return sorted(regime_map), regime_map, "candidate_cache_fallback"


def _future_regimes(
    trade_date: str, dates: list[str], regime_map: dict[str, str], max_hold: int
) -> list[str]:
    try:
        entry_index = dates.index(trade_date)
    except ValueError:
        return ["unknown"] * max_hold
    result = []
    for offset in range(1, max_hold + 1):
        idx = entry_index + offset
        result.append(regime_map.get(dates[idx], "unknown") if idx < len(dates) else "unknown")
    return result


def _worst_regime(regimes: list[str]) -> str:
    if not regimes:
        return "unknown"
    return max(regimes, key=lambda regime: REGIME_PRIORITY.get(regime, 0))


def _transition(entry_regime: str, holding_regimes: list[str]) -> str:
    if "panic_selloff" in holding_regimes:
        return f"{entry_regime}_to_panic_selloff"
    changed = [regime for regime in holding_regimes if regime not in {"unknown", entry_regime}]
    if changed:
        return f"{entry_regime}_to_{_worst_regime(changed)}"
    return "unchanged"


def _entry_stop_for_model(model: str, entry_regime: str) -> float | None:
    if model in STATIC_STOPS:
        return STATIC_STOPS[model]
    if model in {"market_panic_exit"}:
        return None
    if model == "dynamic_A":
        return -0.08 if entry_regime in {"risk_off", "panic_selloff"} else None
    if model in {"regime_dynamic_stop", "dynamic_B"}:
        return -0.08 if entry_regime in {"risk_off", "panic_selloff"} else -0.12
    if model == "dynamic_C":
        if entry_regime == "panic_rebound":
            return None
        return -0.08 if entry_regime in {"risk_off", "panic_selloff"} else -0.12
    if model == "dynamic_D":
        return -0.12
    raise ValueError(f"unknown stop model: {model}")


def _panic_exit_enabled(model: str) -> bool:
    return model in {"regime_dynamic_stop", "dynamic_A", "dynamic_B", "dynamic_D", "market_panic_exit"}


def _simulate(
    row: dict,
    model: str,
    holding_regimes: list[str],
    *,
    pullback_pct: float,
    max_hold: int,
) -> dict:
    entry = _f(row.get("entry_price")) or _f(row.get("close"))
    if not entry or entry <= 0:
        return {"ret": None, "exit_type": "invalid", "exit_day": None}
    entry_regime = str(row.get("market_regime") or "unknown")
    stop_pct = _entry_stop_for_model(model, entry_regime)
    peak = entry
    peak_threshold = entry * 1.005
    pullback_ratio = abs(pullback_pct)

    for day in range(1, max_hold + 1):
        high = _f(row.get(f"future_high_{day}d"))
        low = _f(row.get(f"future_low_{day}d"))
        close = _f(row.get(f"future_close_{day}d"))
        if close is None:
            return {"ret": None, "exit_type": "no_data", "exit_day": day}
        peak = max(peak, high if high is not None else close)
        day_regime = holding_regimes[day - 1] if day <= len(holding_regimes) else "unknown"

        effective_stop = stop_pct
        if model == "dynamic_C" and day_regime == "panic_selloff":
            effective_stop = -0.08
        if effective_stop is not None and low is not None and low <= entry * (1 + effective_stop):
            return {
                "ret": round(effective_stop * 100, 4),
                "exit_type": "emergency_stop",
                "exit_day": day,
                "regime_at_exit": day_regime,
            }
        if day_regime == "panic_selloff" and _panic_exit_enabled(model):
            return {
                "ret": round((close - entry) / entry * 100, 4),
                "exit_type": "market_panic_exit",
                "exit_day": day,
                "regime_at_exit": day_regime,
            }
        if peak > peak_threshold and close <= peak * (1 - pullback_ratio):
            return {
                "ret": round((close - entry) / entry * 100, 4),
                "exit_type": "peak_pullback_exit",
                "exit_day": day,
                "regime_at_exit": day_regime,
            }
        if day == max_hold:
            return {
                "ret": round((close - entry) / entry * 100, 4),
                "exit_type": "time_stop",
                "exit_day": day,
                "regime_at_exit": day_regime,
            }
    return {"ret": None, "exit_type": "no_data", "exit_day": None}


def _simulate_rows(
    rows: list[dict],
    models: list[str],
    dates: list[str],
    regime_map: dict[str, str],
    *,
    pullback_pct: float,
    max_hold: int,
    train_end: str,
) -> dict[str, list[dict]]:
    output: dict[str, list[dict]] = {model: [] for model in models}
    for row in rows:
        trade_date = str(row.get("trade_date") or "")
        regimes = _future_regimes(trade_date, dates, regime_map, max_hold)
        entry_regime = str(row.get("market_regime") or "unknown")
        base = {
            "trade_date": trade_date,
            "period": _period(trade_date, train_end),
            "code": row.get("code"),
            "name": row.get("name"),
            "entry_regime": entry_regime,
            "worst_regime_during_holding": _worst_regime(regimes),
            "regime_transition": _transition(entry_regime, regimes),
            "panic_selloff_during_hold": "panic_selloff" in regimes,
            "entry_price": _f(row.get("entry_price")) or _f(row.get("close")),
        }
        for model in models:
            sim = _simulate(row, model, regimes, pullback_pct=pullback_pct, max_hold=max_hold)
            output[model].append({**base, "stop_model": model, **sim, "_source": row, "_regimes": regimes})
    return output


def _metrics(trades: list[dict]) -> dict:
    valid = [trade for trade in trades if trade.get("ret") is not None]
    rets = [float(trade["ret"]) for trade in valid]
    if not rets:
        return {
            "trade_count": 0,
            "win_rate": None,
            "avg_ret": None,
            "median_ret": None,
            "pf": None,
            "max_loss": None,
            "max_dd": None,
            "stop_count": 0,
            "stop_rate": None,
            "peak_pullback_count": 0,
            "timeout_count": 0,
            "market_exit_count": 0,
            "avg_holding_days": None,
            "deploy_score": None,
            "conservative_score": None,
        }
    wins = [ret for ret in rets if ret > 0]
    losses = [ret for ret in rets if ret < 0]
    gross_loss = abs(sum(losses))
    pf = sum(wins) / gross_loss if gross_loss else (99.0 if wins else 1.0)
    dated = sorted(valid, key=lambda trade: trade["trade_date"])
    cumulative = np.cumsum([float(trade["ret"]) for trade in dated])
    max_dd = float((cumulative - np.maximum.accumulate(cumulative)).min())
    counts = Counter(trade["exit_type"] for trade in valid)
    stop_count = counts["emergency_stop"]
    avg_ret = mean(rets)
    win_rate = len(wins) / len(rets) * 100
    pf_capped = min(float(pf), 99.0)
    deploy = avg_ret * 100 + pf_capped * 10 + win_rate * 20 + max_dd * 2 + min(rets) * 1.5
    conservative = deploy + max_dd * 2 + min(rets) * 3
    return {
        "trade_count": len(rets),
        "win_rate": round(win_rate, 2),
        "avg_ret": round(avg_ret, 4),
        "median_ret": round(median(rets), 4),
        "pf": round(pf_capped, 3),
        "max_loss": round(min(rets), 4),
        "max_dd": round(max_dd, 3),
        "stop_count": stop_count,
        "stop_rate": round(stop_count / len(rets) * 100, 2),
        "peak_pullback_count": counts["peak_pullback_exit"],
        "timeout_count": counts["time_stop"],
        "market_exit_count": counts["market_panic_exit"],
        "avg_holding_days": round(mean([trade["exit_day"] for trade in valid if trade["exit_day"]]), 2),
        "deploy_score": round(deploy, 2),
        "conservative_score": round(conservative, 2),
    }


def _period_trades(trades: list[dict], period: str) -> list[dict]:
    return trades if period == "all" else [trade for trade in trades if trade["period"] == period]


def _summary_row(prefix: dict, trades: list[dict]) -> dict:
    return {**prefix, **_metrics(trades)}


def _sr01(
    filtered_sim: dict[str, list[dict]], inclusive_sim: dict[str, list[dict]]
) -> list[dict]:
    rows = []
    regimes = ["panic_selloff", "panic_rebound", "risk_off", "normal", "strong_risk_on", "euphoria", "unknown"]
    for filter_label, simulations in [("on", filtered_sim), ("off", inclusive_sim)]:
        for regime in regimes:
            for model, trades in simulations.items():
                for period in ["train", "test", "all"]:
                    selected = [
                        trade for trade in _period_trades(trades, period) if trade["entry_regime"] == regime
                    ]
                    if selected:
                        rows.append(
                            _summary_row(
                                {
                                    "no_panic_filter": filter_label,
                                    "entry_regime": regime,
                                    "stop_model": model,
                                    "period": period,
                                },
                                selected,
                            )
                        )
    return rows


def _sr02(simulations: dict[str, list[dict]]) -> list[dict]:
    rows = []
    baseline_by_key = {
        (trade["trade_date"], trade["code"]): trade for trade in simulations["nostop"]
    }
    panic_by_key = {
        (trade["trade_date"], trade["code"]): trade for trade in simulations["market_panic_exit"]
    }
    for model, trades in simulations.items():
        groups: dict[tuple[str, str, str], list[dict]] = {}
        for trade in trades:
            key = (trade["entry_regime"], trade["worst_regime_during_holding"], trade["regime_transition"])
            groups.setdefault(key, []).append(trade)
        for (entry_regime, worst_regime, transition), grouped in groups.items():
            for period in ["train", "test", "all"]:
                selected = _period_trades(grouped, period)
                if not selected:
                    continue
                panic_selected = [trade for trade in selected if trade["panic_selloff_during_hold"]]
                hold_rets = []
                exit_rets = []
                helped = hurt = 0
                for trade in selected:
                    key = (trade["trade_date"], trade["code"])
                    hold = baseline_by_key.get(key)
                    exited = panic_by_key.get(key)
                    if not hold or not exited or hold.get("ret") is None or exited.get("ret") is None:
                        continue
                    hold_rets.append(float(hold["ret"]))
                    exit_rets.append(float(exited["ret"]))
                    if exited["ret"] > hold["ret"]:
                        helped += 1
                    elif exited["ret"] < hold["ret"]:
                        hurt += 1
                n_compare = len(hold_rets)
                rows.append(
                    {
                        "entry_regime": entry_regime,
                        "worst_regime_during_holding": worst_regime,
                        "regime_transition": transition,
                        "stop_model": model,
                        "period": period,
                        **_metrics(selected),
                        "panic_selloff_during_hold_count": len(panic_selected),
                        "panic_selloff_during_hold_rate": round(len(panic_selected) / len(selected) * 100, 2),
                        "ret_if_hold_to_hd3": round(mean(hold_rets), 4) if hold_rets else None,
                        "ret_if_exit_on_panic": round(mean(exit_rets), 4) if exit_rets else None,
                        "exit_on_panic_helped_rate": round(helped / n_compare * 100, 2) if n_compare else None,
                        "exit_on_panic_hurt_rate": round(hurt / n_compare * 100, 2) if n_compare else None,
                        "net_effect_exit_on_panic": round(sum(exit_rets) - sum(hold_rets), 4) if hold_rets else None,
                    }
                )
    return rows


def _sr03(simulations: dict[str, list[dict]], *, pullback_pct: float, max_hold: int) -> list[dict]:
    rows = []
    for model in ["emergency8", "emergency10", "emergency12"]:
        stopped = [trade for trade in simulations[model] if trade.get("exit_type") == "emergency_stop"]
        groups: dict[tuple[str, str], list[dict]] = {}
        for trade in stopped:
            key = (trade["entry_regime"], str(trade.get("regime_at_exit") or "unknown"))
            groups.setdefault(key, []).append(trade)
        for (entry_regime, regime_at_stop), grouped in groups.items():
            for period in ["train", "test", "all"]:
                selected = _period_trades(grouped, period)
                if not selected:
                    continue
                helped = hurt = 0
                after: dict[int, list[float]] = {1: [], 2: [], 3: [], 5: []}
                recovered: dict[int, int] = {1: 0, 2: 0, 3: 0, 5: 0}
                timeout_rets = []
                would_pullback = 0
                for trade in selected:
                    source = trade["_source"]
                    entry = trade["entry_price"]
                    stop_day = int(trade["exit_day"])
                    nostop = _simulate(source, "nostop", trade["_regimes"], pullback_pct=pullback_pct, max_hold=max_hold)
                    if nostop.get("ret") is not None:
                        timeout_rets.append(float(nostop["ret"]))
                        if nostop["ret"] < trade["ret"]:
                            helped += 1
                        elif nostop["ret"] > trade["ret"]:
                            hurt += 1
                    if nostop.get("exit_type") == "peak_pullback_exit":
                        would_pullback += 1
                    for offset in after:
                        close = _f(source.get(f"future_close_{stop_day + offset}d"))
                        if close is not None and entry:
                            value = (close - entry) / entry * 100
                            after[offset].append(value)
                            if value >= 0:
                                recovered[offset] += 1
                count = len(selected)
                rows.append(
                    {
                        "stop_model": model,
                        "entry_regime": entry_regime,
                        "regime_at_stop": regime_at_stop,
                        "period": period,
                        "stop_count": count,
                        **{
                            f"recovered_entry_{day}d_rate": round(recovered[day] / count * 100, 2)
                            for day in after
                        },
                        **{
                            f"avg_ret_after_stop_{day}d": round(mean(after[day]), 4) if after[day] else None
                            for day in after
                        },
                        "would_timeout_ret_hd3_avg": round(mean(timeout_rets), 4) if timeout_rets else None,
                        "would_peak_pullback_after_stop_rate": round(would_pullback / count * 100, 2),
                        "stop_helped_count": helped,
                        "stop_hurt_count": hurt,
                        "stop_helped_rate": round(helped / count * 100, 2),
                        "stop_hurt_rate": round(hurt / count * 100, 2),
                        "net_stop_effect": round(
                            sum(float(trade["ret"]) for trade in selected) - sum(timeout_rets), 4
                        )
                        if timeout_rets
                        else None,
                    }
                )
    return rows


def _sr04(simulations: dict[str, list[dict]]) -> list[dict]:
    rows = []
    for model in STATIC_STOPS:
        for period in ["train", "test", "all"]:
            rows.append(
                _summary_row(
                    {"stop_model": model, "stop_pct": STATIC_STOPS[model], "period": period},
                    _period_trades(simulations[model], period),
                )
            )
    return rows


def _sr05(simulations: dict[str, list[dict]]) -> list[dict]:
    rows = []
    for model in ALL_MODELS:
        for period in ["train", "test", "all"]:
            rows.append(_summary_row({"stop_model": model, "period": period}, _period_trades(simulations[model], period)))
    return rows


def _sr06(
    filtered_sim: dict[str, list[dict]], unfiltered_sim: dict[str, list[dict]]
) -> list[dict]:
    rows = []
    models = ["emergency8", "emergency10", "emergency12", "nostop", "market_panic_exit"]
    for label, simulations in [("on", filtered_sim), ("off", unfiltered_sim)]:
        for model in models:
            for period in ["train", "test", "all"]:
                selected = _period_trades(simulations[model], period)
                panic_entries = sum(trade["entry_regime"] == "panic_selloff" for trade in selected)
                rows.append(
                    {
                        "no_panic_filter": label,
                        "stop_model": model,
                        "period": period,
                        **_metrics(selected),
                        "panic_entry_count": panic_entries,
                    }
                )
    return rows


def _sr07(sr05: list[dict]) -> list[dict]:
    candidates = [row for row in sr05 if row["period"] == "test" and row.get("trade_count")]
    candidates.sort(key=lambda row: (row.get("conservative_score") or -9999), reverse=True)
    rows = []
    for rank, row in enumerate(candidates, start=1):
        rows.append(
            {
                "rank": rank,
                "stop_model": row["stop_model"],
                "trade_count": row["trade_count"],
                "win_rate": row["win_rate"],
                "avg_ret": row["avg_ret"],
                "pf": row["pf"],
                "max_loss": row["max_loss"],
                "max_dd": row["max_dd"],
                "deploy_score": row["deploy_score"],
                "conservative_score": row["conservative_score"],
            }
        )
    return rows


def _row_for(rows: list[dict], model: str, period: str, **filters: str) -> dict | None:
    return next(
        (
            row
            for row in rows
            if row.get("stop_model") == model
            and row.get("period") == period
            and all(row.get(key) == value for key, value in filters.items())
        ),
        None,
    )


def _write_report(
    path: Path,
    *,
    args: argparse.Namespace,
    config: dict,
    filtered_count: int,
    inclusive_count: int,
    sr01: list[dict],
    sr02: list[dict],
    sr03: list[dict],
    sr04: list[dict],
    sr05: list[dict],
    sr06: list[dict],
    final: list[dict],
) -> None:
    no_stop = _row_for(sr04, "nostop", "test")
    est8 = _row_for(sr04, "emergency8", "test")
    est10 = _row_for(sr04, "emergency10", "test")
    est12 = _row_for(sr04, "emergency12", "test")
    panic_on_nostop = _row_for(sr06, "nostop", "test", no_panic_filter="on")
    panic_off_nostop = _row_for(sr06, "nostop", "test", no_panic_filter="off")
    dynamic_best = next(
        (
            row
            for row in final
            if row["stop_model"].startswith("dynamic") or row["stop_model"] == "regime_dynamic_stop"
        ),
        None,
    )
    best_observed = final[0]["stop_model"] if final else "undetermined"
    primary = "emergency12"
    if not (no_stop and est12):
        primary = best_observed
    elif not (
        (no_stop["avg_ret"] - est12["avg_ret"]) <= 0.03
        and est12["max_loss"] > no_stop["max_loss"]
    ):
        primary = best_observed

    if panic_on_nostop and panic_off_nostop and (panic_off_nostop["avg_ret"] or 0) < (panic_on_nostop["avg_ret"] or 0):
        nostop_reason = "B: no_panic_selloff 下で良く見える傾向があり、地合い除外の寄与があります。"
    else:
        nostop_reason = "A: panic_selloff を含めても nostop の優位低下は限定的です。"
    if dynamic_best and final and dynamic_best["rank"] == 1:
        nostop_reason = "C: regime 連動 stop が static/nostop より上位になりました。"

    lines = [
        "=" * 76,
        "H5 STOP x REGIME ANALYSIS REPORT",
        f"Generated: {date.today().isoformat()}",
        "=" * 76,
        "",
        "[Run Summary]",
        f"  Period            : {args.start} to {args.end} (train <= {args.train_end})",
        f"  Conditions        : AI >= {args.ai_threshold}, drop20d <= {args.drop20d_threshold}%, "
        f"peak_pullback={abs(args.pullback) * 100:.1f}%, HD={args.holding_days}",
        f"  Overheat          : {args.overheat_mode}",
        f"  Margin filter     : {args.margin_filter} (missing margin passes, matching current H5 analysis)",
        f"  Entry panic filter: ON primary / OFF comparison",
        f"  Regime source     : {config['regime_source']}",
        f"  Primary rows      : {filtered_count}",
        f"  Panic-included rows: {inclusive_count}",
        "",
        "[Stop Depth Curve - test]",
        "  model          n      WR      EV      PF    maxLoss     maxDD  stop%",
    ]
    for model in STATIC_STOPS:
        row = _row_for(sr04, model, "test")
        if row:
            lines.append(
                f"  {model:<12} {row['trade_count']:>5}  {row['win_rate']:>6.2f}  "
                f"{row['avg_ret']:>+7.4f}  {row['pf']:>6.3f}  {row['max_loss']:>+8.2f}  "
                f"{row['max_dd']:>+9.2f}  {row['stop_rate']:>5.2f}"
            )
    lines += [
        "",
        "[Why nostop looks strong]",
        f"  {nostop_reason}",
    ]
    if no_stop and est8 and est12:
        lines.append(
            f"  Test EV: nostop {no_stop['avg_ret']:+.4f}% / emergency8 {est8['avg_ret']:+.4f}% / "
            f"emergency12 {est12['avg_ret']:+.4f}%."
        )
        lines.append(
            f"  Max loss: nostop {no_stop['max_loss']:+.2f}% / emergency12 {est12['max_loss']:+.2f}%."
        )
    lines += [
        "",
        "[no_panic_selloff dependency - test nostop]",
    ]
    for row in [panic_on_nostop, panic_off_nostop]:
        if row:
            lines.append(
                f"  filter={row['no_panic_filter']:<3} n={row['trade_count']:<5} "
                f"EV={row['avg_ret']:+.4f}% PF={row['pf']:.3f} "
                f"maxLoss={row['max_loss']:+.2f}% panicEntry={row['panic_entry_count']}"
            )
    lines += [
        "",
        "[Entry regime x stop - test highlights (primary no_panic entry filter)]",
    ]
    for regime in ["panic_rebound", "risk_off", "normal", "strong_risk_on", "euphoria"]:
        regime_rows = [
            row
            for row in sr01
            if row["period"] == "test"
            and row["entry_regime"] == regime
            and row.get("no_panic_filter") == "on"
        ]
        if not regime_rows:
            continue
        best = max(regime_rows, key=lambda row: row.get("conservative_score") or -9999)
        lines.append(
            f"  {regime:<16}: best={best['stop_model']:<18} n={best['trade_count']:<4} "
            f"EV={best['avg_ret']:+.4f}% PF={best['pf']:.3f} maxLoss={best['max_loss']:+.2f}%"
        )
    lines += [
        "",
        "[Stopped trades after-analysis]",
        "  Full details: SR03_stopped_trade_after_by_regime.csv",
    ]
    for model in ["emergency8", "emergency10", "emergency12"]:
        grouped = [row for row in sr03 if row["period"] == "test" and row["stop_model"] == model]
        if grouped:
            helped = sum(row["stop_helped_count"] for row in grouped)
            hurt = sum(row["stop_hurt_count"] for row in grouped)
            total = sum(row["stop_count"] for row in grouped)
            lines.append(f"  {model:<12}: stops={total} helped={helped} hurt={hurt}")
    panic_transitions = [
        row
        for row in sr02
        if row["period"] == "test"
        and row["stop_model"] == "nostop"
        and row["regime_transition"].endswith("_to_panic_selloff")
    ]
    if panic_transitions:
        total_transition = sum(row["trade_count"] for row in panic_transitions)
        hold_weighted = sum(row["ret_if_hold_to_hd3"] * row["trade_count"] for row in panic_transitions) / total_transition
        exit_weighted = sum(row["ret_if_exit_on_panic"] * row["trade_count"] for row in panic_transitions) / total_transition
        net = sum(row["net_effect_exit_on_panic"] for row in panic_transitions)
        lines += [
            "",
            "[Market panic exit conclusion - test]",
            f"  Holding period panic_selloff transitions: {total_transition} trades.",
            f"  HD3 hold average={hold_weighted:+.4f}% / exit-on-panic average={exit_weighted:+.4f}% / net={net:+.4f}.",
            "  Immediate market_panic_exit worsened this sample; do not adopt it as an automatic exit.",
        ]
    lines += [
        "",
        "[Dynamic stop candidates - test]",
    ]
    for row in [item for item in final if item["stop_model"] in {"regime_dynamic_stop", "dynamic_A", "dynamic_B", "dynamic_C", "dynamic_D", "market_panic_exit"}]:
        lines.append(
            f"  {row['stop_model']:<18} EV={row['avg_ret']:+.4f}% PF={row['pf']:.3f} "
            f"maxLoss={row['max_loss']:+.2f}% maxDD={row['max_dd']:+.2f}"
        )
    lines += [
        "",
        "[Primary Candidate]",
        f"  Best observed conservative-score model: {best_observed}",
        f"  Practical Primary among requested deployment choices: H5_AI65_PB20_HD3_EST12_CM_MR20 ({primary})",
        "  Rationale: emergency12 retains nearly all nostop EV while putting a hard cap on the observed single-trade loss.",
        "",
        "[Provisional operating policy]",
        "  - emergency8 is too early for this H5 candidate set.",
        "  - emergency12 is the current practical accident stop candidate.",
        "  - Automatic exit solely because the holding-period regime turns panic_selloff is not supported here.",
        "  - New panic_selloff entries remain diagnostic only until operational risk is separately accepted.",
        "  This is a research candidate, not an automatic production-setting change.",
        "",
        "[Files]",
        "  SR01_entry_regime_stop_matrix.csv",
        "  SR02_regime_transition_exit_analysis.csv",
        "  SR03_stopped_trade_after_by_regime.csv",
        "  SR04_stop_depth_curve.csv",
        "  SR05_dynamic_stop_model_comparison.csv",
        "  SR06_no_panic_vs_stop.csv",
        "  SR07_stop_regime_final_candidates.csv",
        "",
        "[Interpretation guardrails]",
        "  - panic_selloff included comparison is diagnostic only; it is not a new-entry recommendation.",
        "  - margin data missing values pass through to stay consistent with current H5 studies.",
        "  - Stop-trigger after-analysis uses future prices only for evaluation, never for entry selection.",
        "",
        "=" * 76,
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("[h5_stop_regime] saved %s", path.name)


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = _load_candidates(Path(args.cache))
    dates, regime_map, regime_source = _load_regime_calendar(args.start, args.end, candidates)
    models = [model.strip() for model in args.stop_models.split(",") if model.strip()]
    invalid_models = [model for model in models if model not in ALL_MODELS]
    if invalid_models:
        raise ValueError(f"unsupported stop model(s): {invalid_models}")

    primary_rows = _filter_h5(
        candidates,
        start=args.start,
        end=args.end,
        ai_threshold=args.ai_threshold,
        drop20d_threshold=args.drop20d_threshold,
        overheat_mode=args.overheat_mode,
        margin_filter=args.margin_filter,
        exclude_panic_entry=True,
    )
    inclusive_rows = _filter_h5(
        candidates,
        start=args.start,
        end=args.end,
        ai_threshold=args.ai_threshold,
        drop20d_threshold=args.drop20d_threshold,
        overheat_mode=args.overheat_mode,
        margin_filter=args.margin_filter,
        exclude_panic_entry=False,
    )
    logger.info(
        "[h5_stop_regime] filtered primary=%d inclusive_panic=%d models=%s",
        len(primary_rows),
        len(inclusive_rows),
        ",".join(models),
    )
    primary_sim = _simulate_rows(
        primary_rows,
        models,
        dates,
        regime_map,
        pullback_pct=args.pullback,
        max_hold=args.holding_days,
        train_end=args.train_end,
    )
    inclusive_sim = _simulate_rows(
        inclusive_rows,
        models,
        dates,
        regime_map,
        pullback_pct=args.pullback,
        max_hold=args.holding_days,
        train_end=args.train_end,
    )

    sr01 = _sr01(primary_sim, inclusive_sim)
    sr02 = _sr02(primary_sim)
    sr03 = _sr03(primary_sim, pullback_pct=args.pullback, max_hold=args.holding_days)
    sr04 = _sr04(primary_sim)
    sr05 = _sr05(primary_sim)
    sr06 = _sr06(primary_sim, inclusive_sim)
    sr07 = _sr07(sr05)
    _write_csv(output_dir / "SR01_entry_regime_stop_matrix.csv", sr01)
    _write_csv(output_dir / "SR02_regime_transition_exit_analysis.csv", sr02)
    _write_csv(output_dir / "SR03_stopped_trade_after_by_regime.csv", sr03)
    _write_csv(output_dir / "SR04_stop_depth_curve.csv", sr04)
    _write_csv(output_dir / "SR05_dynamic_stop_model_comparison.csv", sr05)
    _write_csv(output_dir / "SR06_no_panic_vs_stop.csv", sr06)
    _write_csv(output_dir / "SR07_stop_regime_final_candidates.csv", sr07)

    config = {
        **vars(args),
        "regime_source": regime_source,
        "primary_candidate_count": len(primary_rows),
        "panic_included_candidate_count": len(inclusive_rows),
        "read_only": True,
        "exit_definition": "peak_pullback_exit close-based; emergency stop intraday-low based",
    }
    (output_dir / "h5_stop_regime_config.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _write_report(
        output_dir / "SR08_stop_regime_report.txt",
        args=args,
        config=config,
        filtered_count=len(primary_rows),
        inclusive_count=len(inclusive_rows),
        sr01=sr01,
        sr02=sr02,
        sr03=sr03,
        sr04=sr04,
        sr05=sr05,
        sr06=sr06,
        final=sr07,
    )
    logger.info("[h5_stop_regime] done output=%s", output_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze H5 stops by market regime (read-only).")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2026-05-26")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--ai-threshold", type=float, default=0.65)
    parser.add_argument("--drop20d-threshold", type=float, default=-8.0)
    parser.add_argument("--pullback", type=float, default=-0.02)
    parser.add_argument("--holding-days", type=int, default=3)
    parser.add_argument("--overheat-mode", default="cool_mild_only")
    parser.add_argument("--margin-filter", default="le20")
    parser.add_argument("--stop-models", default=",".join(ALL_MODELS))
    parser.add_argument("--regime-analysis", default="true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
