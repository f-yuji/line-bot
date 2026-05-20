#!/usr/bin/env python3
"""Portfolio simulation for box_lab backtest trades.

Reads box_backtest_trades_*.csv and simulates practical capital allocation:
cash, max positions, skipped entries, daily equity, drawdown, and monthly returns.

Read-only with respect to the database. Writes CSV/PNG under outputs/box_portfolio/.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


OUTPUT_DIR = Path(__file__).parent.parent / "outputs" / "box_portfolio"
DEFAULT_EXIT_CASE = "ma25_stop_box_tp"


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value)[:10]).date()


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except Exception:
        return default


def _daterange_weekdays(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


@dataclass
class OpenPosition:
    trade: dict
    allocated_amount: float


def _load_trades(path: Path, exit_case: str, start: date | None, end: date | None) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            if exit_case != "all" and (raw.get("exit_case") or "") != exit_case:
                continue
            entry_date = _parse_date(raw.get("fill_date") or raw.get("entry_date"))
            exit_date = _parse_date(raw.get("exit_date"))
            pnl_pct = _to_float(raw.get("pnl_pct") or raw.get("profit_pct"))
            if entry_date is None or exit_date is None or pnl_pct is None:
                continue
            if start and entry_date < start:
                continue
            if end and entry_date > end:
                continue
            row = dict(raw)
            row["entry_date"] = entry_date.isoformat()
            row["exit_date"] = exit_date.isoformat()
            row["profit_pct"] = pnl_pct
            row["box_score"] = _to_float(raw.get("score") or raw.get("box_score"), 0.0) or 0.0
            row["box_position_pct"] = _to_float(raw.get("box_position_pct"), 999.0) or 999.0
            row["margin_ratio"] = _to_float(raw.get("margin_ratio"))
            row["bounce_count"] = _to_int(raw.get("bounce_count"), 0)
            row["holding_days"] = _to_int(raw.get("holding_days"), 0)
            rows.append(row)
    return rows


def _priority_key(trade: dict) -> tuple:
    margin = trade.get("margin_ratio")
    margin_key = margin if margin is not None else 9999.0
    return (
        -float(trade.get("box_score") or 0.0),
        float(trade.get("box_position_pct") or 999.0),
        margin_key,
        -int(trade.get("bounce_count") or 0),
        str(trade.get("code") or ""),
    )


def _calc_position_amount(args: argparse.Namespace, total_equity: float, cash: float) -> float:
    if args.position_size_mode == "fixed_amount":
        base = args.max_position_size if args.max_position_size > 0 else args.initial_capital * args.position_size_pct
    else:
        base = total_equity * args.position_size_pct
        if args.max_position_size > 0:
            base = min(base, args.max_position_size)
    return min(base, cash)


def _profit_factor(pnls: list[float]) -> float | None:
    gains = sum(v for v in pnls if v > 0)
    losses = abs(sum(v for v in pnls if v < 0))
    if losses == 0:
        return None if gains == 0 else math.inf
    return gains / losses


def _stats_from_trades(adopted: list[dict]) -> dict:
    if not adopted:
        return {
            "win_rate": None,
            "average_profit_pct": None,
            "profit_factor": None,
            "average_holding_days": None,
        }
    pcts = [_to_float(t.get("profit_pct"), 0.0) or 0.0 for t in adopted]
    wins = [v for v in pcts if v > 0]
    return {
        "win_rate": round(len(wins) / len(pcts) * 100.0, 3),
        "average_profit_pct": round(mean(pcts), 3),
        "profit_factor": None if _profit_factor(pcts) is None else round(_profit_factor(pcts), 3),
        "average_holding_days": round(mean([_to_int(t.get("holding_days"), 0) for t in adopted]), 3),
    }


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _render_chart(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    try:
        os.environ.setdefault("MPLCONFIGDIR", str((OUTPUT_DIR / ".mplconfig").resolve()))
        import matplotlib.pyplot as plt
    except Exception:
        return
    dates = [datetime.fromisoformat(r["trade_date"]).date() for r in rows]
    equity = [float(r["total_equity"]) for r in rows]
    peak: list[float] = []
    cur_peak = 0.0
    for v in equity:
        cur_peak = max(cur_peak, v)
        peak.append(cur_peak)
    dd = [float(r["drawdown_pct"]) for r in rows]

    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(dates, equity, label="equity")
    ax1.plot(dates, peak, label="peak", linestyle="--")
    ax1.set_ylabel("equity")
    ax1.legend(loc="upper left")
    ax2 = ax1.twinx()
    ax2.fill_between(dates, dd, 0, alpha=0.18, label="drawdown")
    ax2.set_ylabel("drawdown %")
    ax2.legend(loc="lower left")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _output_path(output_dir: Path, tag: str, name: str) -> Path:
    if tag:
        return output_dir / f"{tag}_{name}"
    return output_dir / name


def run(args: argparse.Namespace) -> None:
    trades_path = Path(args.trades)
    output_dir = Path(args.output_dir)
    tag = str(args.tag or "").strip()
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    trades = _load_trades(trades_path, args.exit_case, start, end)
    if not trades:
        raise SystemExit(f"No trades found for exit_case={args.exit_case}: {trades_path}")

    first_entry = min(_parse_date(t["entry_date"]) for t in trades if _parse_date(t["entry_date"]))
    last_exit = max(_parse_date(t["exit_date"]) for t in trades if _parse_date(t["exit_date"]))
    sim_start = start or first_entry
    sim_end = end or last_exit
    if sim_start is None or sim_end is None:
        raise SystemExit("Could not determine simulation date range")

    by_entry: dict[str, list[dict]] = defaultdict(list)
    for trade in trades:
        by_entry[trade["entry_date"]].append(trade)
    for day_trades in by_entry.values():
        day_trades.sort(key=_priority_key)

    cash = float(args.initial_capital)
    open_positions: list[OpenPosition] = []
    peak_equity = cash
    prev_equity = cash

    daily_rows: list[dict] = []
    adopted_rows: list[dict] = []
    skipped_rows: list[dict] = []
    monthly_trades: dict[str, int] = defaultdict(int)
    regime_pnls: dict[str, list[float]] = defaultdict(list)

    for cur in _daterange_weekdays(sim_start, sim_end):
        cur_s = cur.isoformat()

        # Exit first, then allow same-day new entries to reuse released cash.
        still_open: list[OpenPosition] = []
        for pos in open_positions:
            if pos.trade["exit_date"] <= cur_s:
                pnl_pct = float(pos.trade["profit_pct"])
                pnl_amount = pos.allocated_amount * pnl_pct / 100.0
                cash += pos.allocated_amount + pnl_amount
                equity_after = cash + sum(p.allocated_amount for p in still_open)
                adopted_rows.append(
                    {
                        "entry_date": pos.trade["entry_date"],
                        "exit_date": pos.trade["exit_date"],
                        "code": pos.trade.get("code", ""),
                        "name": pos.trade.get("name", ""),
                        "allocated_amount": round(pos.allocated_amount, 2),
                        "profit_pct": round(pnl_pct, 4),
                        "holding_days": pos.trade.get("holding_days", ""),
                        "pnl_amount": round(pnl_amount, 2),
                        "equity_after": round(equity_after, 2),
                        "skip_reason": "",
                        "exit_case": pos.trade.get("exit_case", ""),
                        "short_regime": pos.trade.get("short_regime", ""),
                        "long_regime": pos.trade.get("long_regime", ""),
                    }
                )
                monthly_trades[_month_key(cur)] += 1
                regime = pos.trade.get("short_regime") or "unknown"
                regime_pnls[regime].append(pnl_pct)
            else:
                still_open.append(pos)
        open_positions = still_open

        total_equity_before_entry = cash + sum(p.allocated_amount for p in open_positions)
        open_codes = {str(p.trade.get("code") or "") for p in open_positions}
        for trade in by_entry.get(cur_s, []):
            code = str(trade.get("code") or "")
            skip_reason = ""
            if len(open_positions) >= args.max_positions:
                skip_reason = "max_positions_reached"
            elif code in open_codes:
                skip_reason = "duplicate_open_position"
            else:
                alloc = _calc_position_amount(args, total_equity_before_entry, cash)
                if alloc <= 0 or cash < alloc:
                    skip_reason = "insufficient_cash"
                else:
                    cash -= alloc
                    open_positions.append(OpenPosition(trade=trade, allocated_amount=alloc))
                    open_codes.add(code)
                    continue
            skipped_rows.append(
                {
                    "entry_date": trade["entry_date"],
                    "exit_date": trade["exit_date"],
                    "code": code,
                    "name": trade.get("name", ""),
                    "allocated_amount": 0,
                    "profit_pct": trade.get("profit_pct"),
                    "holding_days": trade.get("holding_days", ""),
                    "pnl_amount": 0,
                    "equity_after": round(cash + sum(p.allocated_amount for p in open_positions), 2),
                    "skip_reason": skip_reason,
                    "exit_case": trade.get("exit_case", ""),
                    "short_regime": trade.get("short_regime", ""),
                    "long_regime": trade.get("long_regime", ""),
                }
            )

        invested = sum(p.allocated_amount for p in open_positions)
        total_equity = cash + invested
        peak_equity = max(peak_equity, total_equity)
        daily_return = (total_equity / prev_equity - 1.0) * 100.0 if prev_equity else 0.0
        drawdown = (total_equity - peak_equity) / peak_equity * 100.0 if peak_equity else 0.0
        daily_rows.append(
            {
                "trade_date": cur_s,
                "cash": round(cash, 2),
                "invested": round(invested, 2),
                "total_equity": round(total_equity, 2),
                "open_positions": len(open_positions),
                "daily_return_pct": round(daily_return, 6),
                "drawdown_pct": round(drawdown, 6),
            }
        )
        prev_equity = total_equity

    final_equity = float(daily_rows[-1]["total_equity"]) if daily_rows else float(args.initial_capital)
    elapsed_days = max(1, (sim_end - sim_start).days)
    total_return_pct = (final_equity / args.initial_capital - 1.0) * 100.0
    cagr = ((final_equity / args.initial_capital) ** (365.0 / elapsed_days) - 1.0) * 100.0
    daily_returns = [float(r["daily_return_pct"]) for r in daily_rows]
    stdev = pstdev(daily_returns) if len(daily_returns) > 1 else 0.0
    sharpe = (mean(daily_returns) / stdev * math.sqrt(252)) if stdev > 0 else None
    cash_ratios = [
        (float(r["cash"]) / float(r["total_equity"])) if float(r["total_equity"]) else 0.0
        for r in daily_rows
    ]
    usage = [float(r["open_positions"]) / args.max_positions for r in daily_rows] if args.max_positions else [0.0]
    adopted_stats = _stats_from_trades(adopted_rows)
    skipped_cash = len([r for r in skipped_rows if r["skip_reason"] == "insufficient_cash"])
    skipped_max = len([r for r in skipped_rows if r["skip_reason"] == "max_positions_reached"])

    summary_rows = [
        {
            "source_trades": str(trades_path),
            "exit_case": args.exit_case,
            "initial_capital": round(args.initial_capital, 2),
            "final_equity": round(final_equity, 2),
            "total_return_pct": round(total_return_pct, 3),
            "CAGR": round(cagr, 3),
            "max_drawdown_pct": round(min(float(r["drawdown_pct"]) for r in daily_rows), 3) if daily_rows else 0.0,
            "sharpe_ratio": round(sharpe, 3) if sharpe is not None else None,
            "win_rate": adopted_stats["win_rate"],
            "average_profit_pct": adopted_stats["average_profit_pct"],
            "profit_factor": adopted_stats["profit_factor"],
            "average_holding_days": adopted_stats["average_holding_days"],
            "total_trades": len(adopted_rows),
            "skipped_cash": skipped_cash,
            "skipped_max_positions": skipped_max,
            "skipped_duplicate": len([r for r in skipped_rows if r["skip_reason"] == "duplicate_open_position"]),
            "average_cash_ratio": round(mean(cash_ratios) * 100.0, 3) if cash_ratios else None,
            "average_position_usage": round(mean(usage) * 100.0, 3) if usage else None,
        }
    ]

    monthly_rows: list[dict] = []
    by_month: dict[str, list[dict]] = defaultdict(list)
    for row in daily_rows:
        by_month[_month_key(_parse_date(row["trade_date"]))].append(row)
    for month, rows in sorted(by_month.items()):
        start_equity = float(rows[0]["total_equity"])
        end_equity = float(rows[-1]["total_equity"])
        peak = start_equity
        max_dd = 0.0
        for row in rows:
            eq = float(row["total_equity"])
            peak = max(peak, eq)
            max_dd = min(max_dd, (eq - peak) / peak * 100.0 if peak else 0.0)
        monthly_rows.append(
            {
                "month": month,
                "return_pct": round((end_equity / start_equity - 1.0) * 100.0 if start_equity else 0.0, 3),
                "max_drawdown_pct": round(max_dd, 3),
                "trades": monthly_trades.get(month, 0),
            }
        )

    regime_rows: list[dict] = []
    for regime, pnls in sorted(regime_pnls.items()):
        s = _stats_from_trades([{"profit_pct": p, "holding_days": 0} for p in pnls])
        regime_rows.append({"market_regime": regime, "trades": len(pnls), **s})

    portfolio_rows = adopted_rows + skipped_rows
    portfolio_rows.sort(key=lambda r: (r["entry_date"], r["code"], r["skip_reason"]))

    _write_csv(_output_path(output_dir, tag, "portfolio_daily_equity.csv"), daily_rows)
    _write_csv(_output_path(output_dir, tag, "portfolio_trades.csv"), portfolio_rows)
    _write_csv(_output_path(output_dir, tag, "portfolio_summary.csv"), summary_rows)
    _write_csv(_output_path(output_dir, tag, "portfolio_monthly_returns.csv"), monthly_rows)
    _write_csv(_output_path(output_dir, tag, "portfolio_regime_summary.csv"), regime_rows)
    _render_chart(daily_rows, _output_path(output_dir, tag, "equity_curve.png"))

    summary = summary_rows[0]
    print(
        "[box_portfolio] "
        f"exit_case={args.exit_case} trades={summary['total_trades']} "
        f"return={summary['total_return_pct']}% CAGR={summary['CAGR']}% "
        f"maxDD={summary['max_drawdown_pct']}% sharpe={summary['sharpe_ratio']} "
        f"cash={summary['average_cash_ratio']}% usage={summary['average_position_usage']}%"
    )
    print(f"[box_portfolio] saved outputs to {output_dir}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Portfolio simulation for box_lab trades CSV")
    parser.add_argument("--trades", required=True, help="outputs/box_backtest/box_backtest_trades_*.csv")
    parser.add_argument("--initial-capital", type=float, default=5_000_000)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--position-size-mode", choices=("fixed_pct", "fixed_amount"), default="fixed_pct")
    parser.add_argument("--position-size-pct", type=float, default=0.2)
    parser.add_argument("--max-position-size", type=float, default=0.0)
    parser.add_argument("--rebalance", action="store_true", help="Reserved for future use")
    parser.add_argument("--allow-sector-overlap", action="store_true", default=True)
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--exit-case", default=DEFAULT_EXIT_CASE, help="Exit case to simulate, or all")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--tag", default="", help="Prefix output filenames for comparison runs")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
