#!/usr/bin/env python3
"""Capital-constrained backtest for price-band strategies and H5 mixes.

Research only. Reads existing CSV outputs and writes analysis CSV/report files.
It does not write to DB and does not change production H5/Primary/LINE,
actual_trade_logs, or auto-trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "price_band_capital_constrained"
PB_CHUNKS = ROOT / "outputs" / "price_band_revalidation" / "chunks"
H5_ROWS = ROOT / "outputs" / "h5_stored_forward_cases" / "case_daily_rows.csv"
ENV_ROWS = ROOT / "outputs" / "h5_environment_meter" / "environment_daily_rows.csv"

INITIAL_CAPITAL = 5_000_000.0
LOT_AMOUNT = 300_000.0
DAILY_CAP = 10
TAX_RATE = 0.20315

PB_CASES = {
    "PB_MR_STRONG_MA25_M10_HD20": "case_F_mr_strong_ma25_gap_le_m10_time20_nogap",
    "PB_MR_STRONG_MA25_M10_HD20_GAP3": "case_F_mr_strong_ma25_gap_le_m10_time20_gap3",
    "PB_MR_STRONG_RSI25_HD20": "case_F_mr_strong_rsi_le_25_time20_nogap",
    "PB_RANGE60_LOW10_HD20": "case_F_mr_strong_range60_le_10_time20_nogap",
}


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "nan", "NaN"):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except Exception:
        return default


def dtext(value: Any) -> str:
    return str(value or "")[:10]


def parse_day(value: Any) -> date | None:
    txt = dtext(value)
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt).date()
    except Exception:
        return None


def add_bdays(day: date, n: int) -> date:
    return (pd.Timestamp(day) + pd.offsets.BDay(n)).date()


def pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses == 0:
        return 999.0 if gains > 0 else None
    return gains / losses


def max_dd(equity: list[float]) -> float:
    peak = equity[0] if equity else 0.0
    worst = 0.0
    for val in equity:
        peak = max(peak, val)
        worst = min(worst, val - peak)
    return worst


def tax_adjust(pnl: float) -> float:
    return pnl * (1.0 - TAX_RATE) if pnl > 0 else pnl


def load_env() -> dict[str, dict[str, Any]]:
    return {dtext(r.get("date")): r for r in read_csv(ENV_ROWS) if dtext(r.get("date"))}


def env_bucket(day: str, env: dict[str, dict[str, Any]]) -> str:
    row = env.get(day, {})
    tags = str(row.get("environment_tags") or "").lower()
    status = str(row.get("environment_status") or "").lower()
    score = fnum(row.get("environment_score"), -1)
    if "darasage" in tags or "darasage" in status:
        return "darasage"
    if "crash" in tags or "rebound" in tags:
        return "crash_rebound"
    if "sox" in tags:
        return "SOX_shock"
    if score >= 60:
        return "H5_favorable"
    if 0 <= score < 30:
        return "H5_warning"
    return "normal"


def load_pb_candidates() -> list[dict[str, Any]]:
    rows = []
    if not PB_CHUNKS.exists():
        return rows
    wanted = {v: k for k, v in PB_CASES.items()}
    for path in PB_CHUNKS.glob("chunk_*/events.csv"):
        for row in read_csv(path):
            strategy = wanted.get(str(row.get("case_key")))
            if not strategy:
                continue
            entry = parse_day(row.get("entry_date")) or (add_bdays(parse_day(row.get("signal_date")), 1) if parse_day(row.get("signal_date")) else None)
            if not entry:
                continue
            hold = int(fnum(row.get("exit_day"), fnum(row.get("max_hold"), 20)))
            rows.append({
                "strategy": strategy,
                "source": "PB",
                "signal_date": dtext(row.get("signal_date")),
                "entry_date": entry.isoformat(),
                "exit_date": add_bdays(entry, max(1, hold)).isoformat(),
                "code": row.get("code"),
                "name": row.get("name"),
                "return_pct": fnum(row.get("return_pct")),
                "pnl_after_cost": fnum(row.get("pnl_after_cost")),
                "rank_score": fnum(row.get("ma25_gap_pct")) * -1 + fnum(row.get("rsi14")) * -0.01,
                "exit_day": hold,
            })
    return rows


def load_h5_candidates() -> list[dict[str, Any]]:
    out = []
    for row in read_csv(H5_ROWS):
        case = str(row.get("case_key") or "")
        if case not in {"H5_current7_short3", "current_h5", "current_h5_core"}:
            continue
        signal = parse_day(row.get("signal_date"))
        entry = parse_day(row.get("entry_date")) or (add_bdays(signal, 1) if signal else None)
        if not entry:
            continue
        exit_day = parse_day(row.get("exit_date")) or add_bdays(entry, 3)
        strategy = "H5_current7_short3" if case == "H5_current7_short3" else "current_h5_core"
        out.append({
            "strategy": strategy,
            "source": "H5",
            "signal_date": dtext(row.get("signal_date")),
            "entry_date": entry.isoformat(),
            "exit_date": exit_day.isoformat(),
            "code": row.get("code"),
            "name": row.get("name"),
            "return_pct": fnum(row.get("return_pct")),
            "pnl_after_cost": fnum(row.get("pnl_after_cost")),
            "rank_score": fnum(row.get("score")),
            "exit_day": 3,
        })
    return out


def strategy_candidates(strategy: str, pb: list[dict[str, Any]], h5: list[dict[str, Any]], env: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if strategy in PB_CASES:
        return [r for r in pb if r["strategy"] == strategy]
    if strategy == "H5_current7_short3":
        rows = [r for r in h5 if r["strategy"] == "H5_current7_short3"]
        return rows or [r for r in h5 if r["strategy"] == "current_h5_core"]
    if strategy == "MIX_H5_5_PB_5":
        return mix_candidates(pb, h5, env, mode="fixed")
    if strategy == "MIX_ADAPTIVE_ENV":
        return mix_candidates(pb, h5, env, mode="adaptive")
    return []


def mix_candidates(pb: list[dict[str, Any]], h5: list[dict[str, Any]], env: dict[str, dict[str, Any]], mode: str) -> list[dict[str, Any]]:
    pb_main = [r for r in pb if r["strategy"] == "PB_MR_STRONG_MA25_M10_HD20"]
    h5_main = [r for r in h5 if r["strategy"] == "H5_current7_short3"] or [r for r in h5 if r["strategy"] == "current_h5_core"]
    by_day_h5: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_day_pb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in h5_main:
        by_day_h5[r["entry_date"]].append(r)
    for r in pb_main:
        by_day_pb[r["entry_date"]].append(r)
    out = []
    for day in sorted(set(by_day_h5) | set(by_day_pb)):
        bucket = env_bucket(day, env)
        if mode == "adaptive":
            if bucket in {"H5_favorable", "crash_rebound", "SOX_shock"}:
                h5_cap, pb_cap = 7, 3
            elif bucket == "darasage":
                h5_cap, pb_cap = 2, 8
            else:
                h5_cap, pb_cap = 5, 5
        else:
            h5_cap, pb_cap = 5, 5
        hrows = sorted(by_day_h5.get(day, []), key=lambda r: r["rank_score"], reverse=True)[:h5_cap]
        used = {(r["entry_date"], r["code"]) for r in hrows}
        prows = []
        for r in sorted(by_day_pb.get(day, []), key=lambda r: r["rank_score"], reverse=True):
            key = (r["entry_date"], r["code"])
            if key in used:
                continue
            prows.append(r)
            if len(prows) >= pb_cap:
                break
        for r in hrows + prows:
            nr = dict(r)
            nr["strategy"] = "MIX_ADAPTIVE_ENV" if mode == "adaptive" else "MIX_H5_5_PB_5"
            out.append(nr)
    return out


def simulate(strategy: str, candidates: list[dict[str, Any]], env: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_entry: dict[date, list[dict[str, Any]]] = defaultdict(list)
    dates: set[date] = set()
    for row in candidates:
        e = parse_day(row.get("entry_date"))
        x = parse_day(row.get("exit_date"))
        if not e or not x:
            continue
        by_entry[e].append(row)
        dates.add(e)
        dates.add(x)
    if not dates:
        return [], []
    all_days = pd.bdate_range(min(dates), max(dates)).date
    cash = INITIAL_CAPITAL
    realized = 0.0
    open_pos: list[dict[str, Any]] = []
    curve = []
    trades = []
    peak = INITIAL_CAPITAL
    for day in all_days:
        # Exit first.
        remaining = []
        for pos in open_pos:
            if parse_day(pos["exit_date"]) <= day:
                cash += LOT_AMOUNT + fnum(pos["pnl_after_cost"])
                realized += fnum(pos["pnl_after_cost"])
                pos["actual_exit_date"] = day.isoformat()
                trades.append(pos)
            else:
                remaining.append(pos)
        open_pos = remaining

        accepted_today = 0
        used_codes = {p["code"] for p in open_pos}
        for row in sorted(by_entry.get(day, []), key=lambda r: r.get("rank_score", 0), reverse=True):
            if accepted_today >= DAILY_CAP:
                break
            if cash < LOT_AMOUNT:
                continue
            if row.get("code") in used_codes:
                continue
            pos = dict(row)
            pos["capital"] = LOT_AMOUNT
            cash -= LOT_AMOUNT
            open_pos.append(pos)
            used_codes.add(pos["code"])
            accepted_today += 1

        unrealized = 0.0
        for pos in open_pos:
            entry = parse_day(pos["entry_date"])
            exit_d = parse_day(pos["exit_date"])
            if entry and exit_d and exit_d > entry:
                progress = max(0.0, min(1.0, (day - entry).days / max(1, (exit_d - entry).days)))
            else:
                progress = 0.0
            unrealized += fnum(pos["pnl_after_cost"]) * progress
        total = cash + len(open_pos) * LOT_AMOUNT + unrealized
        peak = max(peak, total)
        curve.append({
            "strategy": strategy,
            "date": day.isoformat(),
            "cash": cash,
            "unrealized": unrealized,
            "realized": realized,
            "positions_value": len(open_pos) * LOT_AMOUNT,
            "total_equity": total,
            "drawdown": total - peak,
            "positions_count": len(open_pos),
            "env": env_bucket(day.isoformat(), env),
        })
    return curve, trades


def summarize_strategy(strategy: str, curve: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [fnum(t.get("pnl_after_cost")) for t in trades]
    rets = [fnum(t.get("return_pct")) for t in trades]
    final = fnum(curve[-1].get("total_equity"), INITIAL_CAPITAL) if curve else INITIAL_CAPITAL
    days = len(curve)
    years = days / 252 if days else 0
    cagr = ((final / INITIAL_CAPITAL) ** (1 / years) - 1) * 100 if years > 0 and final > 0 else None
    return {
        "strategy": strategy,
        "trades": len(trades),
        "active_days": len({t.get("entry_date") for t in trades}),
        "final_equity": final,
        "pnl_after_cost": final - INITIAL_CAPITAL,
        "tax_adjusted_pnl": tax_adjust(final - INITIAL_CAPITAL),
        "avg_return_pct": mean(rets) if rets else None,
        "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
        "PF": pf(rets),
        "max_dd": min((fnum(r.get("drawdown")) for r in curve), default=0.0),
        "CAGR": cagr,
        "avg_positions": mean([fnum(r.get("positions_count")) for r in curve]) if curve else None,
        "max_positions": max([int(fnum(r.get("positions_count"))) for r in curve], default=0),
        "avg_cash": mean([fnum(r.get("cash")) for r in curve]) if curve else None,
        "capital_usage_pct": mean([
            (fnum(r.get("positions_value")) / fnum(r.get("total_equity"), INITIAL_CAPITAL) * 100)
            for r in curve
            if fnum(r.get("total_equity"), 0.0) > 0
        ]) if curve else None,
    }


def grouped_summary(trades: list[dict[str, Any]], curve: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        day = dtext(t.get("entry_date"))
        key = day[:7] if period == "month" else day[:4]
        groups[(str(t.get("strategy")), key)].append(t)
    dd_by_key: dict[tuple[str, str], float] = defaultdict(float)
    for r in curve:
        key = dtext(r.get("date"))[:7] if period == "month" else dtext(r.get("date"))[:4]
        dd_by_key[(str(r.get("strategy")), key)] = min(dd_by_key[(str(r.get("strategy")), key)], fnum(r.get("drawdown")))
    out = []
    for (strategy, key), rows in groups.items():
        pnls = [fnum(r.get("pnl_after_cost")) for r in rows]
        rets = [fnum(r.get("return_pct")) for r in rows]
        out.append({
            "strategy": strategy,
            "year_month" if period == "month" else "year": key,
            "trades": len(rows),
            "pnl_after_cost": sum(pnls),
            "tax_adjusted_pnl": tax_adjust(sum(pnls)),
            "PF": pf(rets),
            "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
            "avg_return_pct": mean(rets) if rets else None,
            "max_dd": dd_by_key[(strategy, key)],
        })
    return out


def environment_performance(trades: list[dict[str, Any]], env: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        groups[(str(t.get("strategy")), env_bucket(dtext(t.get("entry_date")), env))].append(t)
    out = []
    for (strategy, bucket), rows in groups.items():
        pnls = [fnum(r.get("pnl_after_cost")) for r in rows]
        rets = [fnum(r.get("return_pct")) for r in rows]
        out.append({
            "strategy": strategy,
            "environment": bucket,
            "trades": len(rows),
            "pnl_after_cost": sum(pnls),
            "PF": pf(rets),
            "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
            "avg_return_pct": mean(rets) if rets else None,
        })
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default=str(OUT_DIR))
    p.add_argument("--resume", action="store_true")
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--chunk-size", type=int, default=50_000)
    p.add_argument("--full", action="store_true")
    return p.parse_args()


def main() -> None:
    started = time.time()
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env = load_env()
    pb = load_pb_candidates()
    h5 = load_h5_candidates()
    diagnostics = [
        {"step": "load_pb_candidates", "rows": len(pb), "source": str(PB_CHUNKS)},
        {"step": "load_h5_candidates", "rows": len(h5), "source": str(H5_ROWS)},
    ]
    strategies = [
        "H5_current7_short3",
        "PB_MR_STRONG_MA25_M10_HD20",
        "PB_MR_STRONG_MA25_M10_HD20_GAP3",
        "PB_MR_STRONG_RSI25_HD20",
        "PB_RANGE60_LOW10_HD20",
        "MIX_H5_5_PB_5",
        "MIX_ADAPTIVE_ENV",
    ]
    all_curve: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    summaries = []
    for strategy in strategies:
        print(f"[simulate] {strategy}", flush=True)
        candidates = strategy_candidates(strategy, pb, h5, env)
        curve, trades = simulate(strategy, candidates, env)
        all_curve.extend(curve)
        all_trades.extend(trades)
        summaries.append(summarize_strategy(strategy, curve, trades))

    monthly = grouped_summary(all_trades, all_curve, "month")
    yearly = grouped_summary(all_trades, all_curve, "year")
    env_perf = environment_performance(all_trades, env)

    write_csv(out_dir / "strategy_summary.csv", summaries)
    write_csv(out_dir / "monthly_summary.csv", monthly)
    write_csv(out_dir / "yearly_summary.csv", yearly)
    write_csv(out_dir / "equity_curve.csv", all_curve)
    write_csv(out_dir / "environment_performance.csv", env_perf)
    write_csv(out_dir / "join_diagnostics.csv", diagnostics)
    write_csv(out_dir / "h5_vs_pb_monthly.csv", monthly)
    write_csv(out_dir / "mix_portfolio_summary.csv", [r for r in summaries if r["strategy"].startswith("MIX")])
    write_csv(out_dir / "mix_vs_single_comparison.csv", summaries)
    write_csv(out_dir / "position_duration_summary.csv", [{
        "strategy": s,
        "avg_holding_days": mean([fnum(t.get("exit_day")) for t in all_trades if t.get("strategy") == s]) if any(t.get("strategy") == s for t in all_trades) else None,
        "max_holding_days": max([fnum(t.get("exit_day")) for t in all_trades if t.get("strategy") == s], default=0),
    } for s in strategies])
    write_csv(out_dir / "capital_usage_summary.csv", [{
        "strategy": r["strategy"],
        "avg_positions": r.get("avg_positions"),
        "max_positions": r.get("max_positions"),
        "avg_cash": r.get("avg_cash"),
        "capital_usage_pct": r.get("capital_usage_pct"),
    } for r in summaries])
    write_csv(out_dir / "drawdown_analysis.csv", sorted([
        r for r in all_curve
    ], key=lambda r: fnum(r.get("drawdown")))[:200])
    write_csv(out_dir / "worst_months.csv", sorted(monthly, key=lambda r: fnum(r.get("pnl_after_cost")))[:100])
    write_csv(out_dir / "best_months.csv", sorted(monthly, key=lambda r: fnum(r.get("pnl_after_cost")), reverse=True)[:100])

    elapsed = time.time() - started
    best = sorted(summaries, key=lambda r: fnum(r.get("tax_adjusted_pnl")), reverse=True)[0] if summaries else {}
    best_mix = sorted([r for r in summaries if r["strategy"].startswith("MIX")], key=lambda r: fnum(r.get("tax_adjusted_pnl")), reverse=True)
    pb_main = next((r for r in summaries if r["strategy"] == "PB_MR_STRONG_MA25_M10_HD20"), {})
    h5_main = next((r for r in summaries if r["strategy"] == "H5_current7_short3"), {})
    report = [
        "# Price Band Capital-Constrained Backtest",
        "",
        "Research-only. Production H5/Primary/LINE/actual_trade_logs/auto-trading were not changed.",
        "",
        f"- elapsed_sec: {elapsed:.1f}",
        "- status: complete",
        f"- output_dir: {out_dir}",
        f"- PB candidates loaded: {len(pb):,}",
        f"- H5 candidates loaded: {len(h5):,}",
        "",
        "## Best Strategy",
        json.dumps(best, ensure_ascii=False, indent=2, default=str),
        "",
        "## Best Mix",
        json.dumps(best_mix[0], ensure_ascii=False, indent=2, default=str) if best_mix else "No mix result.",
        "",
        "## H5 vs PB",
        f"- H5_current7_short3: pnl={h5_main.get('tax_adjusted_pnl')}, PF={h5_main.get('PF')}, DD={h5_main.get('max_dd')}, trades={h5_main.get('trades')}",
        f"- PB_MR_STRONG_MA25_M10_HD20: pnl={pb_main.get('tax_adjusted_pnl')}, PF={pb_main.get('PF')}, DD={pb_main.get('max_dd')}, trades={pb_main.get('trades')}",
        "",
        "## Notes",
        "- PB is capital constrained with 300k per symbol and 5M initial cash.",
        "- Unrealized PnL is linearly accrued between entry and planned exit for equity-curve visualization.",
        "- PB exit variants beyond stored event cases require regenerating price-path events; this pass uses the validated HD20 fixed cases.",
        "- Next forward-test candidate: PB_MR_STRONG_MA25_M10_HD20 and MIX_H5_5_PB_5.",
    ]
    write_text(out_dir / "report.txt", "\n".join(report) + "\n")
    print(f"[done] {out_dir}", flush=True)


if __name__ == "__main__":
    main()
