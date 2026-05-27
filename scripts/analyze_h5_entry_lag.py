#!/usr/bin/env python3
"""Analyze executable entry timing for the H5 rebound candidate.

This is a research-only script. It reads the existing H5 candidate cache and
daily snapshot opens, then writes CSV/text results only. It never updates DB
tables or production signal/trade state.

The default evaluation candidate reflects the current practical H5 hypothesis:
AI65, drop20d <= -8%, no entry during panic_selloff, cool/mild overheat,
margin <= 20 when available, peak-pullback 2%, HD3, emergency stop -12%.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import pickle
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

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
DEFAULT_OUT = ROOT / "outputs" / "rebound_next_analysis" / "h5_entry_lag"
ENTRY_MODELS = [
    "close_entry",
    "next_open_entry",
    "next_open_gap_limit_1",
    "next_open_gap_limit_2",
    "next_open_gap_limit_3",
    "next_open_gap_limit_5",
    "next_open_no_gap_chase",
    "limit_prev_close",
    "limit_prev_close_minus_1",
    "next_close_entry",
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


def _overheat_score(row: dict) -> int:
    return sum(
        [
            (_f(row.get("rsi14"), 0) or 0) >= 65,
            (_f(row.get("ma5_gap_pct"), 0) or 0) >= 5,
            (_f(row.get("return_5d_pct"), 0) or 0) >= 8,
            (_f(row.get("volume_ratio_20d"), 0) or 0) >= 3.0,
        ]
    )


def _load_candidates(path: Path) -> list[dict]:
    with path.open("rb") as file:
        loaded = pickle.load(file)
    rows = loaded["candidates"] if isinstance(loaded, dict) else loaded
    logger.info("[h5_entry_lag] candidate cache rows=%d", len(rows))
    return rows


def _filter_h5(rows: list[dict], args: argparse.Namespace) -> list[dict]:
    selected: list[dict] = []
    margin_limit = {"le5": 5.0, "le10": 10.0, "le20": 20.0, "le30": 30.0}.get(
        args.margin_filter
    )
    for row in rows:
        trade_date = str(row.get("trade_date") or "")
        if not (args.start <= trade_date <= args.end):
            continue
        if (_f(row.get("signal_probability"), 0) or 0) < args.ai_threshold:
            continue
        if (_f(row.get("drop_from_20d_high_pct"), 0) or 0) > args.drop20d_threshold:
            continue
        if str(row.get("market_regime") or "unknown") == "panic_selloff":
            continue
        if args.overheat_mode == "cool_mild_only" and _overheat_score(row) > 1:
            continue
        margin = _f(row.get("margin_ratio"))
        if margin_limit is not None and margin is not None and margin > margin_limit:
            continue
        selected.append(row)
    logger.info("[h5_entry_lag] H5 selected rows=%d margin_filter=%s", len(selected), args.margin_filter)
    return selected


def _next_dates(dates: list[str]) -> dict[str, str]:
    return {dates[index]: dates[index + 1] for index in range(len(dates) - 1)}


def _load_open_cache(path: Path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    with path.open("rb") as file:
        loaded = pickle.load(file)
    return loaded if isinstance(loaded, dict) else {}


def _save_open_cache(path: Path, values: dict[tuple[str, str], dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        pickle.dump(values, file)


def _load_trading_dates(
    start: str, end: str, candidate_rows: list[dict]
) -> tuple[list[str], Any | None]:
    """Use the regime calendar as the trading calendar and return its DB client."""
    try:
        from services.trade_case_tester import _build_supabase

        sb = _build_supabase()
        dates: list[str] = []
        offset = 0
        while True:
            page = (
                sb.table("market_regime")
                .select("trade_date")
                .gte("trade_date", start)
                .lte("trade_date", end)
                .order("trade_date")
                .range(offset, offset + 999)
                .execute()
                .data
                or []
            )
            dates.extend(str(row["trade_date"]) for row in page if row.get("trade_date"))
            if len(page) < 1000:
                break
            offset += 1000
        unique_dates = sorted(set(dates))
        if unique_dates:
            logger.info("[h5_entry_lag] trading calendar source=market_regime dates=%d", len(unique_dates))
            return unique_dates, sb
    except Exception as exc:
        logger.warning("[h5_entry_lag] market calendar unavailable; candidate fallback: %s", exc)
    fallback = sorted({str(row.get("trade_date")) for row in candidate_rows if row.get("trade_date")})
    logger.warning("[h5_entry_lag] trading calendar source=candidate_cache dates=%d", len(fallback))
    return fallback, None


def _load_next_open_rows(
    selected: list[dict],
    trading_dates: list[str],
    cache_path: Path,
    sb: Any | None = None,
) -> dict[tuple[str, str], dict]:
    """Read only the next-session snapshot rows required for selected signals."""
    next_date_by_signal = _next_dates(trading_dates)
    requested: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        signal_date = str(row.get("trade_date") or "")
        next_date = next_date_by_signal.get(signal_date)
        if next_date:
            requested[next_date].add(str(row.get("code") or ""))

    cached = _load_open_cache(cache_path)
    missing: dict[str, list[str]] = {}
    for next_date, codes in requested.items():
        missing_codes = [code for code in sorted(codes) if (next_date, code) not in cached]
        if missing_codes:
            missing[next_date] = missing_codes
    if not missing:
        logger.info("[h5_entry_lag] next-open cache hit rows=%d", len(cached))
        return cached

    if sb is None:
        from services.trade_case_tester import _build_supabase

        sb = _build_supabase()
    completed_dates = 0
    logger.info(
        "[h5_entry_lag] loading actual next opens dates=%d missing_pairs=%d",
        len(missing),
        sum(len(codes) for codes in missing.values()),
    )
    try:
        for next_date in sorted(missing):
            codes = missing[next_date]
            for offset in range(0, len(codes), 60):
                chunk = codes[offset : offset + 60]
                rows = (
                    sb.table("stock_feature_snapshots")
                    .select("trade_date,code,open,high,low,close")
                    .eq("trade_date", next_date)
                    .in_("code", chunk)
                    .execute()
                    .data
                    or []
                )
                found = {str(row.get("code")): row for row in rows}
                for code in chunk:
                    cached[(next_date, code)] = found.get(code, {})
            completed_dates += 1
            if completed_dates % 50 == 0:
                _save_open_cache(cache_path, cached)
                logger.info(
                    "[h5_entry_lag] open progress dates=%d/%d cache_rows=%d",
                    completed_dates,
                    len(missing),
                    len(cached),
                )
    finally:
        _save_open_cache(cache_path, cached)
    logger.info("[h5_entry_lag] actual next opens loaded cache_rows=%d", len(cached))
    return cached


def _attach_next_session(
    selected: list[dict], trading_dates: list[str], open_rows: dict[tuple[str, str], dict]
) -> list[dict]:
    next_date_by_signal = _next_dates(trading_dates)
    attached = []
    missing_open = 0
    close_mismatch = 0
    for source in selected:
        row = dict(source)
        next_date = next_date_by_signal.get(str(row.get("trade_date") or ""))
        next_row = open_rows.get((next_date, str(row.get("code") or "")), {}) if next_date else {}
        row["next_trade_date"] = next_date
        row["future_open_1d"] = _f(next_row.get("open"))
        observed_close = _f(next_row.get("close"))
        labeled_close = _f(row.get("future_close_1d"))
        if row["future_open_1d"] is None:
            missing_open += 1
        if observed_close is not None and labeled_close is not None and abs(observed_close - labeled_close) > 0.001:
            close_mismatch += 1
        attached.append(row)
    logger.info(
        "[h5_entry_lag] attached next-open missing=%d close_mismatch=%d",
        missing_open,
        close_mismatch,
    )
    return attached


def _simulate_exit(
    row: dict,
    *,
    entry_price: float,
    first_future_day: int,
    stop_pct: float | None,
    pullback_pct: float,
    holding_days: int,
    skip_first_peak_exit: bool = False,
) -> dict:
    if entry_price <= 0:
        return {"ret": None, "exit_type": "invalid"}
    peak = entry_price
    peak_threshold = entry_price * 1.005
    for held_day in range(holding_days):
        source_day = first_future_day + held_day
        high = _f(row.get(f"future_high_{source_day}d"))
        low = _f(row.get(f"future_low_{source_day}d"))
        close = _f(row.get(f"future_close_{source_day}d"))
        if close is None:
            return {"ret": None, "exit_type": "missing_future_price"}
        if stop_pct is not None and low is not None and low <= entry_price * (1 + stop_pct):
            return {"ret": round(stop_pct * 100, 4), "exit_type": "emergency_stop"}
        if not (skip_first_peak_exit and held_day == 0):
            peak = max(peak, high if high is not None else close)
        if peak > peak_threshold and close <= peak * (1 - abs(pullback_pct)):
            return {
                "ret": round((close - entry_price) / entry_price * 100, 4),
                "exit_type": "peak_pullback_exit",
            }
        if held_day == holding_days - 1:
            return {
                "ret": round((close - entry_price) / entry_price * 100, 4),
                "exit_type": "time_stop",
            }
    return {"ret": None, "exit_type": "missing_future_price"}


def _entry_plan(row: dict, model: str) -> tuple[float | None, int, bool, float | None, str]:
    """Return entry price, first evaluated future day, filled, gap, note."""
    prev_close = _f(row.get("entry_price")) or _f(row.get("close"))
    next_open = _f(row.get("future_open_1d"))
    next_low = _f(row.get("future_low_1d"))
    if prev_close is None or prev_close <= 0:
        return None, 1, False, None, "missing_signal_close"
    if model == "close_entry":
        return prev_close, 1, True, None, "signal_close_fill"
    if model == "next_close_entry":
        next_close = _f(row.get("future_close_1d"))
        return next_close, 2, next_close is not None, None, "next_close_fill"
    if next_open is None:
        return None, 1, False, None, "missing_next_open"
    gap_pct = (next_open / prev_close - 1) * 100
    if model == "next_open_entry":
        return next_open, 1, True, gap_pct, "next_open_fill"
    gap_limits = {
        "next_open_gap_limit_1": 1.0,
        "next_open_gap_limit_2": 2.0,
        "next_open_gap_limit_3": 3.0,
        "next_open_gap_limit_5": 5.0,
        "next_open_no_gap_chase": 0.0,
    }
    if model in gap_limits:
        if gap_pct > gap_limits[model]:
            return next_open, 1, False, gap_pct, "gap_too_high"
        return next_open, 1, True, gap_pct, "next_open_fill"
    limit_pct = {"limit_prev_close": 0.0, "limit_prev_close_minus_1": -1.0}.get(model)
    if limit_pct is not None:
        limit_price = prev_close * (1 + limit_pct / 100)
        if next_low is None or next_low > limit_price:
            return next_open, 1, False, gap_pct, "limit_not_filled"
        fill_price = min(next_open, limit_price)
        if next_open <= limit_price:
            return fill_price, 1, True, gap_pct, "limit_filled_at_open"
        # The order crossed intraday. Its same-day stop is observable, but a
        # same-day peak pullback is not because high/entry ordering is unknown.
        return fill_price, 1, True, gap_pct, "limit_filled_intraday"
    raise ValueError(f"unknown entry model: {model}")


def _metrics(rets: list[float]) -> dict:
    if not rets:
        return {
            "win_rate": None,
            "avg_ret": None,
            "median_ret": None,
            "pf": None,
            "max_loss": None,
            "max_dd": None,
            "total_ret": None,
        }
    wins = [value for value in rets if value > 0]
    losses = [value for value in rets if value < 0]
    gross_loss = abs(sum(losses))
    pf = sum(wins) / gross_loss if gross_loss else (99.0 if wins else None)
    equity = peak = drawdown = 0.0
    for value in rets:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return {
        "win_rate": round(len(wins) / len(rets) * 100, 2),
        "avg_ret": round(mean(rets), 4),
        "median_ret": round(median(rets), 4),
        "pf": round(pf, 3) if pf is not None else None,
        "max_loss": round(min(rets), 4),
        "max_dd": round(drawdown, 3),
        "total_ret": round(sum(rets), 4),
    }


def _analyze(rows: list[dict], args: argparse.Namespace) -> list[dict]:
    output = []
    for period in ["train", "test", "all"]:
        period_rows = rows if period == "all" else [
            row for row in rows if _period(str(row.get("trade_date") or ""), args.train_end) == period
        ]
        for model in ENTRY_MODELS:
            returns: list[float] = []
            gaps: list[float] = []
            skipped = 0
            missed_profit = saved_loss = 0.0
            skip_reasons: dict[str, int] = defaultdict(int)
            for row in sorted(period_rows, key=lambda value: str(value.get("trade_date") or "")):
                entry_price, first_day, filled, gap, note = _entry_plan(row, model)
                if filled and entry_price is not None:
                    sim = _simulate_exit(
                        row,
                        entry_price=entry_price,
                        first_future_day=first_day,
                        stop_pct=args.stop_pct,
                        pullback_pct=args.pullback,
                        holding_days=args.holding_days,
                        skip_first_peak_exit=note == "limit_filled_intraday",
                    )
                    if sim["ret"] is None:
                        skipped += 1
                        skip_reasons["missing_exit_data"] += 1
                        continue
                    returns.append(float(sim["ret"]))
                    if gap is not None:
                        gaps.append(gap)
                    continue
                skipped += 1
                skip_reasons[note] += 1
                if model.startswith("next_open_") and entry_price is not None:
                    counterfactual = _simulate_exit(
                        row,
                        entry_price=entry_price,
                        first_future_day=1,
                        stop_pct=args.stop_pct,
                        pullback_pct=args.pullback,
                        holding_days=args.holding_days,
                    ).get("ret")
                elif model.startswith("limit_"):
                    next_open = _f(row.get("future_open_1d"))
                    counterfactual = (
                        _simulate_exit(
                            row,
                            entry_price=next_open,
                            first_future_day=1,
                            stop_pct=args.stop_pct,
                            pullback_pct=args.pullback,
                            holding_days=args.holding_days,
                        ).get("ret")
                        if next_open is not None
                        else None
                    )
                else:
                    counterfactual = None
                if counterfactual is not None:
                    if counterfactual > 0:
                        missed_profit += counterfactual
                    elif counterfactual < 0:
                        saved_loss += abs(counterfactual)
            output.append(
                {
                    "entry_model": model,
                    "period": period,
                    "signal_count": len(period_rows),
                    "filled_count": len(returns),
                    "fill_rate": round(len(returns) / len(period_rows) * 100, 2) if period_rows else None,
                    "skipped_count": skipped,
                    "avg_gap_pct": round(mean(gaps), 4) if gaps else None,
                    **_metrics(returns),
                    "missed_profit_after_skip": round(missed_profit, 4),
                    "saved_loss_after_skip": round(saved_loss, 4),
                    "skip_reasons": json.dumps(skip_reasons, ensure_ascii=False, sort_keys=True),
                }
            )
    return output


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info("[h5_entry_lag] saved %s rows=%d", path.name, len(rows))


def _write_report(path: Path, rows: list[dict], args: argparse.Namespace) -> None:
    test = {row["entry_model"]: row for row in rows if row["period"] == "test"}
    close = test.get("close_entry", {})
    next_open = test.get("next_open_entry", {})
    lines = [
        "H5 ENTRY LAG / EXECUTION ANALYSIS",
        "=" * 60,
        "",
        f"Period: {args.start} to {args.end} (train <= {args.train_end})",
        "Conditions: AI65 / drop20d <= -8% / no panic_selloff / cool_mild_only / margin <= 20",
        f"Exit: peak_pullback=2.0% / HD={args.holding_days} / emergency stop={args.stop_pct * 100:.0f}%",
        "Open source: stock_feature_snapshots.open (actual daily open, not close proxy)",
        "",
        "TEST RESULTS",
        "model                         fill%      WR        EV        PF        maxLoss",
    ]
    for model in ENTRY_MODELS:
        row = test.get(model)
        if not row:
            continue
        lines.append(
            f"{model:<29} {str(row['fill_rate']):>6}  {str(row['win_rate']):>7}  "
            f"{str(row['avg_ret']):>8}  {str(row['pf']):>8}  {str(row['max_loss']):>9}"
        )
    if close and next_open:
        lines += [
            "",
            "CLOSE VS NEXT OPEN",
            f"close_entry EV:     {close.get('avg_ret')}%",
            f"next_open_entry EV: {next_open.get('avg_ret')}%",
            f"difference:         {round((next_open.get('avg_ret') or 0) - (close.get('avg_ret') or 0), 4)} pt",
        ]
    lines += [
        "",
        "Notes:",
        "- Gap-limited models skip only positive gap opens above the named percentage.",
        "- Limit-order models apply same-day stops after a fill. For intraday fills,",
        "  same-day peak-pullback exits are omitted because OHLC cannot prove ordering.",
        "- missed/saved values are counterfactual return-point sums for skipped signals.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("[h5_entry_lag] saved %s", path.name)


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    all_candidates = _load_candidates(Path(args.cache))
    selected = _filter_h5(all_candidates, args)
    trading_dates, sb = _load_trading_dates(args.start, args.end, all_candidates)
    open_rows = _load_next_open_rows(selected, trading_dates, out_dir / "_next_open_cache.pkl", sb)
    rows = _attach_next_session(selected, trading_dates, open_rows)
    results = _analyze(rows, args)
    _write_csv(out_dir / "LIVE03_entry_lag_open_analysis.csv", results)
    _write_report(out_dir / "LIVE03_entry_lag_open_report.txt", results, args)
    (out_dir / "h5_entry_lag_config.json").write_text(
        json.dumps(vars(args), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )
    logger.info("[h5_entry_lag] done")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2026-05-26")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    parser.add_argument("--ai-threshold", type=float, default=0.65)
    parser.add_argument("--drop20d-threshold", type=float, default=-8.0)
    parser.add_argument("--pullback", type=float, default=-0.02)
    parser.add_argument("--holding-days", type=int, default=3)
    parser.add_argument("--overheat-mode", default="cool_mild_only")
    parser.add_argument("--margin-filter", default="le20")
    parser.add_argument("--stop-pct", type=float, default=-0.12)
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
