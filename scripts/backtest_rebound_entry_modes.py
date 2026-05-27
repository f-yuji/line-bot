#!/usr/bin/env python3
"""Compare rebound_lab entry timing while keeping the current virtual exit rules.

Research-only / read-only:
- Loads historical AI-scored drop candidates through the existing trade-case loader.
- Applies the current confirmed threshold and entry credit filter.
- Compares signal-date close entry with rebound-confirmed close entries.
- Reuses services.virtual_trade_exit.evaluate_virtual_trade_exit() for exits.
- Writes CSV output only; it never updates Supabase tables or virtual_trades.

The comparison is intentionally trade-level rather than a capital allocation
simulation. It answers whether requiring visible rebound evidence improves
entries selected by the AI, before changing the production entry flow.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

import services.trade_case_tester as case_tester
from services.research_database import build_supabase
from services.signal_stage import evaluate_signal_stage
from services.trade_case_tester import (
    _build_current_settings_rules,
    _load_strategy_settings,
    _passes_credit_rules,
    _sort_candidates,
)
from services.virtual_trade_exit import evaluate_virtual_trade_exit

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
# Exit evaluation is reused hundreds of times here. Its per-trade operational
# lifecycle logs are useful in cron, but make a research comparison unreadable.
logging.getLogger("services.virtual_trade_exit").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "rebound_entry_compare"
JST = timezone(timedelta(hours=9))

SCENARIOS = {
    "2020_covid_crash": ("2020-02-20", "2020-04-30"),
    "2022_rate_hike_bear": ("2022-01-01", "2022-12-31"),
    "2023_rebound": ("2023-01-01", "2023-12-31"),
    "2024_ai_bubble": ("2024-01-01", "2024-12-31"),
    "2025_ai_bubble": ("2025-01-01", "2025-12-31"),
    "custom_recent": ("2026-02-09", "2026-05-10"),
}

ENTRY_MODES = {
    "ai_signal_close": "AI判定日終値",
    "rebound_confirm_1": "反発条件1つ確認後終値",
    "rebound_confirm_2": "反発条件2つ確認後終値",
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:10]).date()


def _fetch_paged(query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        data = query.range(offset, offset + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            return rows
        offset += page_size


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(closes[-period - 1 : -1], closes[-period:]):
        move = current - previous
        gains.append(max(move, 0.0))
        losses.append(max(-move, 0.0))
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    if average_loss == 0:
        return 100.0
    relative_strength = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))


def _profit_factor(values: list[float]) -> float | None:
    profits = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    return profits / losses if losses else None


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _scenario_names(value: str) -> list[str]:
    if value == "all":
        return list(SCENARIOS)
    if value not in SCENARIOS:
        raise SystemExit(f"unknown scenario: {value}")
    return [value]


def _load_candidates_without_margin(sb, period_start: date, period_end: date) -> list[dict]:
    """Load and score the standard candidate population without weekly margin joins.

    Historical weekly-margin rows contain large repeated imports for some
    years. Omitting the common credit filter lets the entry-timing experiment
    finish without changing the relative comparison between timing modes.
    """

    snap_cols = sorted(set(
        [
            "id", "trade_date", "code", "name", "market", "sector", "close",
            "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
            "volume_ratio_20d", "bad_news_score", "market_shock_score",
        ]
        + list(case_tester.NUMERIC_FEATURES)
        + list(case_tester.BOOL_FEATURES)
        + list(case_tester.CATEGORICAL_FEATURES)
    ))
    future_cols: list[str] = []
    for day_index in range(1, case_tester.MAX_FUTURE_DAYS + 1):
        future_cols.extend([
            f"future_high_{day_index}d",
            f"future_low_{day_index}d",
            f"future_close_{day_index}d",
        ])
    label_cols = ["id", "feature_snapshot_id", "trade_date", "code", "entry_price", *future_cols]

    def label_query():
        return (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .gte("trade_date", period_start.isoformat())
            .lte("trade_date", period_end.isoformat())
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("trade_date")
        )

    labels = case_tester._fetch_all_by_offset(label_query, label="labels")
    logger.info("[entry_compare] labels loaded rows=%d credit_filter=off", len(labels))
    snapshot_ids = [int(row["feature_snapshot_id"]) for row in labels if row.get("feature_snapshot_id")]
    snapshots = case_tester._fetch_snapshots_by_ids(sb, snapshot_ids, snap_cols)
    by_id = {
        str(row["id"]): row
        for row in snapshots
        if row.get("is_drop_candidate") and row.get("is_tradeable")
    }
    merged: list[dict] = []
    for label in labels:
        snapshot = by_id.get(str(label.get("feature_snapshot_id")))
        if not snapshot:
            continue
        row = dict(snapshot)
        for key, value in label.items():
            if key in {"id", "code", "trade_date"}:
                row[f"label_{key}"] = value
            else:
                row[key] = value
        merged.append(row)
    logger.info("[entry_compare] candidate snapshots merged rows=%d credit_filter=off", len(merged))
    return case_tester._score_candidates(merged, case_tester._active_model_bundle(sb))


def _load_candidates(sb, start: date, end: date, credit_filter: str) -> list[dict]:
    if credit_filter == "current":
        logger.info("[entry_compare] credit_filter=current (historical margin join may be slow)")
        return case_tester._load_candidates_v2(sb, start, end)
    return _load_candidates_without_margin(sb, start, end)


def _select_ai_signals(candidates: list[dict], cfg: dict, credit_filter: str) -> list[dict]:
    """Build a common signal population before applying alternative entry timing.

    This keeps the current confirmed threshold, credit rule, daily ranking,
    daily entry count and sector cap. It intentionally does not model open
    portfolio-slot occupation because entry modes have different entry dates;
    that is a separate portfolio simulation question.
    """

    rules = _build_current_settings_rules(cfg)
    eligible_by_date: dict[str, list[dict]] = defaultdict(list)
    excluded = Counter()
    for row in candidates:
        bad_news_score = _to_float(row.get("bad_news_score"), 0.0) or 0.0
        if bad_news_score >= 80:
            excluded["bad_news"] += 1
            continue
        evaluated = evaluate_signal_stage(
            row.get("signal_probability"),
            row.get("rule_score"),
            row.get("expected_value"),
            cfg,
        )
        row["signal_stage"] = evaluated["stage"]
        if row["signal_stage"] not in {"confirmed", "strong_confirmed"}:
            excluded["not_confirmed"] += 1
            continue
        if credit_filter == "current" and not _passes_credit_rules(row, rules):
            excluded["credit_filter"] += 1
            continue
        eligible_by_date[str(row.get("trade_date"))].append(row)

    rank_limit = int(rules.get("entry_rank_limit") or 10)
    max_daily = int(rules.get("max_daily_entries") or 5)
    max_sector = int(rules.get("max_sector_positions") or 99)
    selected: list[dict] = []
    for trade_date in sorted(eligible_by_date):
        ranked = _sort_candidates(eligible_by_date[trade_date], str(rules.get("entry_sort")), rules)
        ranked = ranked[:rank_limit] if rank_limit > 0 else ranked
        sectors: Counter[str] = Counter()
        daily_count = 0
        for row in ranked:
            if max_daily and daily_count >= max_daily:
                excluded["max_daily_entries"] += 1
                continue
            sector = str(row.get("sector") or "unknown")
            if max_sector and sectors[sector] >= max_sector:
                excluded["max_sector_positions"] += 1
                continue
            selected.append(row)
            sectors[sector] += 1
            daily_count += 1
    logger.info("[entry_compare] selected_ai_signals=%d excluded=%s", len(selected), dict(excluded))
    return selected


def _load_history(sb, codes: list[str], start: date, end: date) -> dict[str, list[dict]]:
    cols = "code,trade_date,open,high,low,close,volume,rsi14,ma5"
    by_code: dict[str, list[dict]] = defaultdict(list)
    for index in range(0, len(codes), 25):
        chunk = codes[index : index + 25]
        query = (
            sb.table("stock_feature_snapshots")
            .select(cols)
            .in_("code", chunk)
            .gte("trade_date", start.isoformat())
            .lte("trade_date", end.isoformat())
            .order("trade_date")
        )
        rows = _fetch_paged(query)
        for row in rows:
            close = _to_float(row.get("close"))
            if close is None or close <= 0:
                continue
            by_code[str(row.get("code"))].append({
                "date": str(row.get("trade_date")),
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": close,
                "volume": _to_float(row.get("volume")),
                "rsi14": _to_float(row.get("rsi14")),
                "ma5": _to_float(row.get("ma5")),
            })
        logger.info(
            "[entry_compare] history progress codes=%d/%d rows=%d",
            min(index + len(chunk), len(codes)),
            len(codes),
            sum(len(values) for values in by_code.values()),
        )
    for values in by_code.values():
        values.sort(key=lambda row: row["date"])
    return by_code


def _confirmation_reasons(
    history: list[dict],
    index: int,
    signal_close: float,
    cfg: dict,
) -> tuple[list[str], dict]:
    if index <= 0:
        return [], {}
    row = history[index]
    previous = history[index - 1]
    close = _to_float(row.get("close"))
    prev_close = _to_float(previous.get("close"))
    if close is None or prev_close is None or prev_close <= 0:
        return [], {}

    reasons: list[str] = []
    metrics: dict[str, float | None] = {}
    daily_return_pct = (close / prev_close - 1.0) * 100.0
    from_signal_pct = (close / signal_close - 1.0) * 100.0 if signal_close > 0 else None
    metrics["confirm_daily_return_pct"] = daily_return_pct
    metrics["confirm_from_signal_pct"] = from_signal_pct
    if daily_return_pct >= float(cfg.get("daily_rebound_threshold", 4.0)):
        reasons.append("daily_rebound")
    if from_signal_pct is not None and from_signal_pct >= float(cfg.get("drop_rebound_threshold", 8.0)):
        reasons.append("from_drop_rebound")

    previous_volumes = [
        _to_float(item.get("volume"))
        for item in history[max(0, index - 20) : index]
        if _to_float(item.get("volume")) is not None
    ]
    current_volume = _to_float(row.get("volume"))
    volume_ratio = None
    if len(previous_volumes) >= 10 and current_volume is not None:
        average_volume = sum(previous_volumes) / len(previous_volumes)
        if average_volume > 0:
            volume_ratio = current_volume / average_volume
            if volume_ratio >= float(cfg.get("volume_ratio_threshold", 2.0)):
                reasons.append("volume_surge")
    metrics["confirm_volume_ratio"] = volume_ratio

    closes = [_to_float(item.get("close")) for item in history[: index + 1]]
    clean_closes = [value for value in closes if value is not None]
    rsi_now = _to_float(row.get("rsi14"), _rsi(clean_closes))
    previous_rsi_values = [
        _to_float(item.get("rsi14"))
        for item in history[max(0, index - 5) : index]
        if _to_float(item.get("rsi14")) is not None
    ]
    metrics["confirm_rsi14"] = rsi_now
    if (
        rsi_now is not None
        and rsi_now >= float(cfg.get("rsi_recover_threshold", 40.0))
        and any(value < float(cfg.get("rsi_low_threshold", 25.0)) for value in previous_rsi_values)
    ):
        reasons.append("rsi_recovery")

    if cfg.get("ma5_cross_enabled", False) and len(clean_closes) >= 5:
        ma5 = sum(clean_closes[-5:]) / 5.0
        if prev_close <= ma5 < close:
            reasons.append("ma5_cross")
        metrics["confirm_ma5"] = ma5
    return reasons, metrics


def _find_entry(
    signal: dict,
    mode: str,
    history: list[dict],
    cfg: dict,
    max_wait_days: int,
) -> dict | None:
    signal_date = str(signal.get("trade_date"))
    signal_close = _to_float(signal.get("entry_price"), _to_float(signal.get("close")))
    if signal_close is None or signal_close <= 0:
        return None
    signal_index = next((idx for idx, row in enumerate(history) if row["date"] == signal_date), None)
    if signal_index is None:
        return None
    if mode == "ai_signal_close":
        return {
            "entry_date": signal_date,
            "entry_price": signal_close,
            "confirmation_count": 0,
            "confirmation_reasons": "ai_signal",
            "wait_days": 0,
        }
    needed = 1 if mode == "rebound_confirm_1" else 2
    end_index = min(len(history), signal_index + max_wait_days + 1)
    for idx in range(signal_index + 1, end_index):
        reasons, metrics = _confirmation_reasons(history, idx, signal_close, cfg)
        if len(reasons) >= needed:
            return {
                "entry_date": history[idx]["date"],
                "entry_price": history[idx]["close"],
                "confirmation_count": len(reasons),
                "confirmation_reasons": ",".join(reasons),
                "wait_days": idx - signal_index,
                **metrics,
            }
    return None


def _evaluate_entry(
    scenario: str,
    signal: dict,
    mode: str,
    entry: dict,
    history: list[dict],
    cfg: dict,
) -> dict:
    fake_trade = {
        "code": signal.get("code"),
        "name": signal.get("name"),
        "buy_date": entry["entry_date"],
        "buy_price": entry["entry_price"],
        "quantity": 100,
        "status": "open",
    }
    evaluated = evaluate_virtual_trade_exit(
        fake_trade,
        price_rows=history,
        settings=cfg,
        now=datetime.now(timezone.utc),
    )
    update = evaluated.update if evaluated else {}
    return {
        "scenario": scenario,
        "entry_mode": mode,
        "entry_mode_label": ENTRY_MODES[mode],
        "code": signal.get("code"),
        "name": signal.get("name"),
        "sector": signal.get("sector"),
        "signal_date": signal.get("trade_date"),
        "signal_close": signal.get("entry_price") or signal.get("close"),
        "signal_stage": signal.get("signal_stage"),
        "signal_probability": signal.get("signal_probability"),
        "rule_score": signal.get("rule_score"),
        "margin_ratio": signal.get("margin_ratio"),
        **entry,
        "status": update.get("status", "open"),
        "exit_date": update.get("sell_date"),
        "exit_price": update.get("sell_price"),
        "exit_reason": update.get("exit_reason"),
        "profit_pct": update.get("profit_loss_pct"),
        "profit_yen": update.get("profit_loss"),
        "max_return_pct": update.get("max_return_pct"),
        "max_drawdown_pct": update.get("max_drawdown_pct"),
        "rsi75_touched": update.get("rsi75_touched"),
        "ma5_recovered": update.get("ma5_recovered"),
    }


def _summary(scenario: str, mode: str, signals: int, rows: list[dict], credit_filter: str) -> dict:
    closed = [row for row in rows if row.get("status") == "closed" and row.get("profit_pct") is not None]
    pcts = [_to_float(row.get("profit_pct"), 0.0) or 0.0 for row in closed]
    yen = [_to_float(row.get("profit_yen"), 0.0) or 0.0 for row in closed]
    holds = [
        (_to_date(row["exit_date"]) - _to_date(row["entry_date"])).days
        for row in closed
        if row.get("exit_date") and row.get("entry_date")
    ]
    reasons = Counter(str(row.get("exit_reason") or "open") for row in rows)
    profit_factor = _profit_factor(pcts)
    return {
        "scenario": scenario,
        "credit_filter": credit_filter,
        "entry_mode": mode,
        "entry_mode_label": ENTRY_MODES[mode],
        "signals": signals,
        "entries": len(rows),
        "entry_rate_pct": round(len(rows) / signals * 100.0, 1) if signals else None,
        "closed_trades": len(closed),
        "win_rate": round(sum(value > 0 for value in pcts) / len(pcts) * 100.0, 1) if pcts else None,
        "avg_profit_pct": round(mean(pcts), 3) if pcts else None,
        "median_profit_pct": round(median(pcts), 3) if pcts else None,
        "total_profit_yen_100shares": round(sum(yen), 0),
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "best_trade_pct": round(max(pcts), 3) if pcts else None,
        "worst_trade_pct": round(min(pcts), 3) if pcts else None,
        "avg_trade_max_drawdown_pct": round(
            mean(_to_float(row.get("max_drawdown_pct"), 0.0) or 0.0 for row in closed), 3
        ) if closed else None,
        "avg_wait_days": round(mean(float(row.get("wait_days") or 0) for row in rows), 2) if rows else None,
        "avg_holding_calendar_days": round(mean(holds), 2) if holds else None,
        "exit_reason_counts": "; ".join(f"{key}={value}" for key, value in sorted(reasons.items())),
        "notes": "現行virtual_trade_exitを利用。資金配分・同時保有限度は未反映。"
        + ("信用倍率フィルタ込み。" if credit_filter == "current" else "入口時点の信用倍率フィルタは共通で除外。"),
    }


def run(args: argparse.Namespace) -> None:
    sb = build_supabase()
    cfg = _load_strategy_settings(sb)
    scenarios = _scenario_names(args.scenario)
    detail_rows: list[dict] = []
    summary_rows: list[dict] = []
    logger.info(
        "[entry_compare] current_exit pullback=%s rsi=%s rsi_pullback=%s stop=%s ma5_failure=%s holding=%s extend=%s",
        cfg.get("virtual_exit_pullback_pct"),
        cfg.get("virtual_exit_rsi_level"),
        cfg.get("virtual_exit_rsi_pullback_pct"),
        cfg.get("virtual_exit_stop_loss_pct"),
        cfg.get("virtual_exit_ma5_failure_pct"),
        cfg.get("virtual_exit_holding_days"),
        cfg.get("virtual_exit_extend_high_update_days"),
    )
    logger.info(
        "[entry_compare] confirmation thresholds daily=%s from_drop=%s volume=%s rsi_low=%s rsi_recover=%s ma5_cross=%s max_wait_days=%s",
        cfg.get("daily_rebound_threshold"),
        cfg.get("drop_rebound_threshold"),
        cfg.get("volume_ratio_threshold"),
        cfg.get("rsi_low_threshold"),
        cfg.get("rsi_recover_threshold"),
        cfg.get("ma5_cross_enabled"),
        args.max_wait_days,
    )
    for scenario in scenarios:
        start_text, end_text = SCENARIOS[scenario]
        start = _to_date(start_text)
        end = _to_date(end_text)
        logger.info("[entry_compare] scenario=%s start=%s end=%s", scenario, start, end)
        candidates = _load_candidates(sb, start, end, args.credit_filter)
        signals = _select_ai_signals(candidates, cfg, args.credit_filter)
        codes = sorted({str(row.get("code")) for row in signals if row.get("code")})
        history = _load_history(
            sb,
            codes,
            start - timedelta(days=45),
            end + timedelta(days=args.forward_calendar_days),
        )
        by_mode: dict[str, list[dict]] = defaultdict(list)
        for signal in signals:
            code_history = history.get(str(signal.get("code")), [])
            for mode in ENTRY_MODES:
                entry = _find_entry(signal, mode, code_history, cfg, args.max_wait_days)
                if entry is None:
                    continue
                result = _evaluate_entry(scenario, signal, mode, entry, code_history, cfg)
                result["credit_filter"] = args.credit_filter
                by_mode[mode].append(result)
                detail_rows.append(result)
        for mode in ENTRY_MODES:
            row = _summary(scenario, mode, len(signals), by_mode.get(mode, []), args.credit_filter)
            summary_rows.append(row)
            logger.info(
                "[entry_compare] scenario=%s mode=%s signals=%d entries=%d win_rate=%s avg=%s pf=%s",
                scenario,
                mode,
                len(signals),
                row["entries"],
                row["win_rate"],
                row["avg_profit_pct"],
                row["profit_factor"],
            )

    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    summary_path = OUT_DIR / f"rebound_entry_mode_summary_{timestamp}.csv"
    detail_path = OUT_DIR / f"rebound_entry_mode_trades_{timestamp}.csv"
    _write_csv(summary_path, summary_rows)
    _write_csv(detail_path, detail_rows)
    logger.info("[entry_compare] saved summary=%s rows=%d", summary_path, len(summary_rows))
    logger.info("[entry_compare] saved trades=%s rows=%d", detail_path, len(detail_rows))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare rebound entry modes using current virtual exit settings")
    parser.add_argument("--scenario", default="custom_recent", help="all or a scenario name")
    parser.add_argument(
        "--max-wait-days",
        type=int,
        default=5,
        help="Maximum subsequent trading days allowed for rebound confirmation entry",
    )
    parser.add_argument(
        "--forward-calendar-days",
        type=int,
        default=45,
        help="Additional calendar days loaded after scenario end for exit evaluation",
    )
    parser.add_argument(
        "--credit-filter",
        choices=["off", "current"],
        default="off",
        help="off compares pure entry timing quickly; current also applies current margin filter and can be slow historically",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
