#!/usr/bin/env python3
"""Revalidate fixed price-band best cases across the full available universe.

Research only. Reads historical feature snapshots and local CSV outputs,
then writes analysis CSV/report files. It never writes to DB and never changes
production H5/Primary/LINE/actual_trade_logs/auto-trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.analyze_price_band_expectancy import (  # noqa: E402
    build_environment_map,
    build_supabase,
    fetch_snapshots,
    fnum,
    max_drawdown,
    norm_code,
    prepare_snapshots,
    quantile,
    read_csv,
    write_csv,
)


OUT_DIR = ROOT / "outputs" / "price_band_revalidation"
LONG_TYPES = ROOT / "outputs" / "price_band_expectancy_long" / "symbol_type_classification.csv"
H5_MONTHLY = ROOT / "outputs" / "strategy_monthly_regime_performance" / "monthly_case_summary.csv"
H5_YEARLY = ROOT / "outputs" / "strategy_monthly_regime_performance" / "yearly_case_summary.csv"

POSITION_AMOUNT = 300_000.0
COST_BPS = 10.0
TAX_RATE = 0.20315
PERIODS = {
    "1y": 365,
    "2y": 365 * 2,
    "3y": 365 * 3,
    "5y": 365 * 5,
}
WATCH_CODES = {"9432", "2914", "9433", "9434", "4502", "8058", "8306", "8316", "8591", "8766"}


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def dtext(value: Any) -> str:
    return str(value or "")[:10]


def pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses == 0:
        return 999.0 if gains > 0 else None
    return gains / losses


def pct(n: float, d: float) -> float | None:
    return n / d * 100.0 if d else None


def taxed_sum(pnls: list[float]) -> float:
    net = sum(pnls)
    return net * (1.0 - TAX_RATE) if net > 0 else net


def load_symbol_types() -> dict[str, str]:
    out = {}
    for row in read_csv(LONG_TYPES):
        code = norm_code(row.get("code"))
        if code:
            out[code] = str(row.get("symbol_type") or "")
    return out


def fixed_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for hold in [10, 20, 30]:
        for stop in [None, -5.0, -8.0, -10.0]:
            cases.append({
                "case_key": f"case_A_ma25_m10_to_ma75_hd{hold}_stop{abs(int(stop)) if stop else 'none'}",
                "family": "CASE_A",
                "buy_rule": "ma25_gap_le_m10",
                "exit_rule": "ma75_revert",
                "max_hold": hold,
                "stop_loss": stop,
            })
    for stop in [None, -5.0, -8.0]:
        cases.append({
            "case_key": f"case_B_rsi25_time10_stop{abs(int(stop)) if stop else 'none'}",
            "family": "CASE_B",
            "buy_rule": "rsi_le_25",
            "exit_rule": "time",
            "max_hold": 10,
            "stop_loss": stop,
        })
    cases.append({
        "case_key": "case_C_range60_10_to_range60_70_hd20_stopnone",
        "family": "CASE_C",
        "buy_rule": "range60_le_10",
        "exit_rule": "range60_ge_70",
        "max_hold": 20,
        "stop_loss": None,
    })
    cases.append({
        "case_key": "case_D_drop20_m8_tp5_hd20_stopnone",
        "family": "CASE_D",
        "buy_rule": "drop20_le_m8",
        "exit_rule": "tp5",
        "max_hold": 20,
        "stop_loss": None,
    })
    for exit_rule in ["ma25_revert", "ma75_revert", "tp5", "time10", "time20"]:
        cases.append({
            "case_key": f"case_E_h5_like_mr_{exit_rule}",
            "family": "CASE_E",
            "buy_rule": "h5_like_mean_reversion",
            "exit_rule": exit_rule,
            "max_hold": 10 if exit_rule == "time10" else 20,
            "stop_loss": None,
        })
    for buy_rule in ["ma25_gap_le_m10", "rsi_le_25", "range60_le_10"]:
        cases.append({
            "case_key": f"case_F_mr_strong_{buy_rule}_time20",
            "family": "CASE_F",
            "buy_rule": buy_rule,
            "exit_rule": "time",
            "max_hold": 20,
            "stop_loss": None,
            "symbol_types": {"mean_reversion_strong"},
        })
        cases.append({
            "case_key": f"case_G_no_danger_{buy_rule}_time20",
            "family": "CASE_G",
            "buy_rule": buy_rule,
            "exit_rule": "time",
            "max_hold": 20,
            "stop_loss": None,
            "symbol_types": {"mean_reversion_strong", "neutral_reversion"},
        })
    with_gap = []
    for c in cases:
        for gap_limit in [None, 3.0]:
            nc = dict(c)
            nc["gap_limit"] = gap_limit
            nc["case_key"] = c["case_key"] + ("_gap3" if gap_limit is not None else "_nogap")
            with_gap.append(nc)
    return with_gap


def buy_mask(df: pd.DataFrame, rule: str) -> pd.Series:
    if rule == "ma25_gap_le_m10":
        return df["ma25_gap_pct"] <= -10.0
    if rule == "rsi_le_25":
        return df["rsi14"] <= 25.0
    if rule == "range60_le_10":
        return df["range_position_60"] <= 10.0
    if rule == "drop20_le_m8":
        return df["drop20"] <= -8.0
    if rule == "h5_like_mean_reversion":
        return (
            (df["drop20"] <= -8.0)
            & (df["ma25_gap_pct"] <= -10.0)
            & (df["rsi14"] <= 30.0)
        )
    return pd.Series(False, index=df.index)


def target_for(row: dict[str, Any], exit_rule: str, entry: float) -> float | None:
    if exit_rule == "tp5":
        return entry * 1.05
    if exit_rule == "ma25_revert":
        val = fnum(row.get("ma25"))
        return val if val and val > entry else None
    if exit_rule == "ma75_revert":
        val = fnum(row.get("ma75"))
        return val if val and val > entry else None
    if exit_rule == "range60_ge_70":
        lo = fnum(row.get("roll_low_60"))
        hi = fnum(row.get("roll_high_60"))
        if lo is None or hi is None or hi <= lo:
            return None
        val = lo + (hi - lo) * 0.70
        return val if val > entry else None
    return None


def evaluate_case(df: pd.DataFrame, case: dict[str, Any], symbol_types: dict[str, str]) -> list[dict[str, Any]]:
    mask = buy_mask(df, str(case["buy_rule"]))
    rows = df[mask.fillna(False)].copy()
    if rows.empty:
        return []
    wanted_types = case.get("symbol_types")
    if wanted_types:
        rows = rows[rows["code"].map(lambda c: symbol_types.get(str(c), "") in wanted_types)]
    if rows.empty:
        return []
    out = []
    hold = int(case["max_hold"])
    stop_loss = case.get("stop_loss")
    gap_limit = case.get("gap_limit")
    for row in rows.to_dict("records"):
        entry = fnum(row.get("future_open_1d"))
        signal_close = fnum(row.get("close"))
        if entry is None or signal_close is None or entry <= 0 or signal_close <= 0:
            continue
        gap_pct = (entry / signal_close - 1.0) * 100.0
        if gap_limit is not None and abs(gap_pct) > gap_limit:
            continue
        target = target_for(row, str(case["exit_rule"]), entry)
        if str(case["exit_rule"]) != "time" and target is None:
            continue
        exit_price = fnum(row.get(f"future_close_{hold}d"))
        exit_day = hold
        exit_reason = "time_exit"
        hit = False
        stopped = False
        max_adverse = 0.0
        for i in range(1, hold + 1):
            low = fnum(row.get(f"future_low_{i}d"))
            high = fnum(row.get(f"future_high_{i}d"))
            close = fnum(row.get(f"future_close_{i}d"))
            if low is not None:
                max_adverse = min(max_adverse, (low / entry - 1.0) * 100.0)
            if stop_loss is not None and low is not None and low <= entry * (1.0 + float(stop_loss) / 100.0):
                exit_price = entry * (1.0 + float(stop_loss) / 100.0)
                exit_day = i
                exit_reason = "stop_loss"
                stopped = True
                break
            if target is not None and high is not None and high >= target:
                exit_price = target
                exit_day = i
                exit_reason = "target_hit"
                hit = True
                break
            if i == hold and close is not None:
                exit_price = close
        if exit_price is None:
            continue
        ret = (exit_price / entry - 1.0) * 100.0
        pnl_before = POSITION_AMOUNT * ret / 100.0
        pnl_after = POSITION_AMOUNT * (ret / 100.0 - COST_BPS / 10000.0)
        trade_date = row["trade_date"].date().isoformat() if hasattr(row.get("trade_date"), "date") else dtext(row.get("trade_date"))
        out.append({
            "case_key": case["case_key"],
            "family": case["family"],
            "buy_rule": case["buy_rule"],
            "exit_rule": case["exit_rule"],
            "max_hold": hold,
            "stop_loss": stop_loss if stop_loss is not None else "",
            "gap_limit": gap_limit if gap_limit is not None else "none",
            "signal_date": trade_date,
            "entry_date": dtext(row.get("future_date_1d")) or "",
            "code": row.get("code"),
            "name": row.get("name"),
            "sector": row.get("sector"),
            "symbol_type": symbol_types.get(str(row.get("code")), ""),
            "entry_price": entry,
            "signal_close": signal_close,
            "gap_pct": gap_pct,
            "target_price": target if target is not None else "",
            "exit_day": exit_day,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "hit": hit,
            "stopped": stopped,
            "return_pct": ret,
            "pnl_before_cost": pnl_before,
            "pnl_after_cost": pnl_after,
            "max_adverse_pct": max_adverse,
            "ma25_gap_pct": fnum(row.get("ma25_gap_pct")),
            "rsi14": fnum(row.get("rsi14")),
            "range_position_60": fnum(row.get("range_position_60")),
            "drop20": fnum(row.get("drop20")),
        })
    return out


def summarize(rows: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    extra = extra or {}
    rets = [fnum(r.get("return_pct"), 0.0) for r in rows]
    pnls = [fnum(r.get("pnl_after_cost"), 0.0) for r in rows]
    adverse = [fnum(r.get("max_adverse_pct"), 0.0) for r in rows]
    out = {
        **extra,
        "events": len(rows),
        "symbols": len({r.get("code") for r in rows}),
        "active_days": len({r.get("signal_date") for r in rows}),
        "hit_rate": pct(sum(1 for r in rows if str(r.get("hit")) == "True" or r.get("hit") is True), len(rows)),
        "stop_rate": pct(sum(1 for r in rows if str(r.get("stopped")) == "True" or r.get("stopped") is True), len(rows)),
        "win_rate": pct(sum(1 for v in rets if v > 0), len(rets)),
        "avg_return_pct": mean(rets) if rets else None,
        "median_return_pct": median(rets) if rets else None,
        "PF": pf(rets),
        "pnl_before_cost": sum(fnum(r.get("pnl_before_cost"), 0.0) for r in rows),
        "pnl_after_cost": sum(pnls),
        "tax_adjusted_pnl": taxed_sum(pnls),
        "max_dd_after_cost": max_drawdown(pnls),
        "median_max_adverse_pct": median(adverse) if adverse else None,
        "p95_max_adverse_pct": quantile(adverse, 0.05) if adverse else None,
        "one_day_max_loss": min(pnls) if pnls else None,
    }
    return out


def period_label(day: str, latest: date) -> list[str]:
    d = datetime.fromisoformat(day).date()
    labels = ["all_available"]
    for label, days in PERIODS.items():
        if d >= latest - timedelta(days=days):
            labels.append(label)
    return labels


def grouped_summaries(events: list[dict[str, Any]], latest: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    case_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    month_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    year_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        day = str(row.get("signal_date"))
        for p in period_label(day, latest):
            case_groups[(str(row.get("case_key")), p)].append(row)
        month_groups[(str(row.get("case_key")), day[:7])].append(row)
        year_groups[(str(row.get("case_key")), day[:4])].append(row)
    case_rows = [summarize(v, {"case_key": k[0], "period": k[1]}) for k, v in case_groups.items()]
    month_rows = [summarize(v, {"case_key": k[0], "year_month": k[1]}) for k, v in month_groups.items()]
    year_rows = [summarize(v, {"case_key": k[0], "year": k[1]}) for k, v in year_groups.items()]
    case_rows.sort(key=lambda r: (fnum(r.get("PF"), 0.0), fnum(r.get("tax_adjusted_pnl"), 0.0)), reverse=True)
    return case_rows, month_rows, year_rows


def train_test(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        groups[str(row.get("case_key"))].append(row)
    out = []
    for case, rows in groups.items():
        rows = sorted(rows, key=lambda r: str(r.get("signal_date")))
        if len(rows) < 50:
            continue
        split = int(len(rows) * 0.7)
        train = summarize(rows[:split])
        test = summarize(rows[split:])
        out.append({
            "case_key": case,
            "train_events": train.get("events"),
            "test_events": test.get("events"),
            "train_PF": train.get("PF"),
            "test_PF": test.get("PF"),
            "train_avg_return_pct": train.get("avg_return_pct"),
            "test_avg_return_pct": test.get("avg_return_pct"),
            "stable_flag": (fnum(train.get("PF"), 0.0) >= 1.2 and fnum(test.get("PF"), 0.0) >= 1.2),
        })
    return out


def outlier(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        groups[str(row.get("case_key"))].append(row)
    out = []
    for case, rows in groups.items():
        if len(rows) < 50:
            continue
        rows = sorted(rows, key=lambda r: fnum(r.get("return_pct"), 0.0))
        trim = max(1, int(len(rows) * 0.01))
        for variant, subset in [
            ("raw", rows),
            ("drop_top_1pct", rows[:-trim]),
            ("drop_bottom_1pct", rows[trim:]),
            ("drop_both_1pct", rows[trim:-trim]),
        ]:
            out.append(summarize(subset, {"case_key": case, "variant": variant}))
    return out


def symbol_concentration(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_case_symbol: dict[tuple[str, str, str], float] = defaultdict(float)
    by_case: dict[str, float] = defaultdict(float)
    for row in events:
        case = str(row.get("case_key"))
        code = str(row.get("code"))
        name = str(row.get("name") or "")
        pnl = fnum(row.get("pnl_after_cost"), 0.0)
        by_case_symbol[(case, code, name)] += pnl
        by_case[case] += pnl
    rows = []
    grouped: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    for (case, code, name), pnl in by_case_symbol.items():
        grouped[case].append((code, name, pnl))
    for case, vals in grouped.items():
        vals = sorted(vals, key=lambda x: x[2], reverse=True)
        total = by_case[case]
        top10 = vals[:10]
        worst10 = sorted(vals, key=lambda x: x[2])[:10]
        rows.append({
            "case_key": case,
            "total_pnl_after_cost": total,
            "top10_profit": sum(v[2] for v in top10),
            "top10_profit_share": sum(v[2] for v in top10) / total * 100 if total else None,
            "top10_symbols": "; ".join(f"{c}:{n}:{p:.0f}" for c, n, p in top10),
            "worst10_symbols": "; ".join(f"{c}:{n}:{p:.0f}" for c, n, p in worst10),
        })
    return rows


def environment_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    env = build_environment_map()
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        e = env.get(str(row.get("signal_date")), {})
        tags = str(e.get("environment_tags") or "").lower()
        status = str(e.get("environment_status") or "").lower()
        score = fnum(e.get("environment_score"))
        buckets = ["normal"]
        if score is not None and score >= 60:
            buckets.append("H5_favorable")
            buckets.append("high_vol")
        if score is not None and score < 30:
            buckets.append("H5_warning")
            buckets.append("low_vol")
        for token, label in [("crash", "crash_rebound"), ("darasage", "darasage"), ("sox", "SOX_shock")]:
            if token in tags or token in status:
                buckets.append(label)
        for b in set(buckets):
            groups[(str(row.get("case_key")), b)].append(row)
    return [summarize(v, {"case_key": k[0], "environment": k[1]}) for k, v in groups.items()]


def current_expectancy(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    latest_by_code: dict[str, dict[str, Any]] = {}
    for row in events:
        groups[(str(row.get("case_key")), str(row.get("code")))].append(row)
        code = str(row.get("code"))
        if code not in latest_by_code or str(row.get("signal_date")) > str(latest_by_code[code].get("signal_date")):
            latest_by_code[code] = row
    out = []
    for (case, code), rows in groups.items():
        if len(rows) < 10:
            continue
        latest = latest_by_code.get(code, {})
        s = summarize(rows, {"case_key": case, "code": code, "name": latest.get("name")})
        s["watch_code"] = code in WATCH_CODES
        out.append(s)
    out.sort(key=lambda r: (bool(r.get("watch_code")), fnum(r.get("PF"), 0.0), fnum(r.get("events"), 0.0)), reverse=True)
    return out


def load_h5_compare() -> list[dict[str, Any]]:
    rows = []
    for row in read_csv(H5_MONTHLY):
        if row.get("case_key") in {"current_h5_core", "H5_short_pullback_drop5_m3", "H5_current7_short3", "trend_support_best", "mix_current7_short3_trend_7_3"}:
            rows.append(row)
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default=str(OUT_DIR))
    p.add_argument("--universe", default="topix500")
    p.add_argument("--period", default="all")
    p.add_argument("--max-symbols", type=int, default=500)
    p.add_argument("--chunk-symbols", type=int, default=50)
    p.add_argument("--max-rows", type=int, default=100_000)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--full", action="store_true")
    return p.parse_args()


def main() -> None:
    started = time.time()
    args = parse_args()
    load_dotenv(ROOT / ".env")
    out_dir = Path(args.output_dir)
    chunks_dir = out_dir / "chunks"
    out_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "run_manifest.json"
    manifest = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "status": "running",
        "args": vars(args),
        "chunks": [],
    }
    if args.resume and manifest_path.exists():
        try:
            old = json.loads(manifest_path.read_text(encoding="utf-8"))
            if old.get("chunks"):
                manifest["chunks"] = old["chunks"]
        except Exception:
            pass
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    end_dt = date.today()
    start_dt = date(2008, 1, 1) if args.period == "all" else end_dt - timedelta(days=365 * 5 + 300)
    client = build_supabase()
    symbol_types = load_symbol_types()
    all_events: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    proxy: list[dict[str, Any]] = [{
        "item": "H5 confirmed / overheat<=1 in CASE_E",
        "proxy_used": "drop20<=-8, ma25_gap<=-10, RSI<=30",
        "reason": "production H5 stage/overheat metadata is not fully present in stock_feature_snapshots",
    }]
    completed = {int(c.get("offset")) for c in manifest["chunks"] if c.get("status") == "ok"}
    cases = fixed_cases()
    for offset in range(0, args.max_symbols, args.chunk_symbols):
        chunk_name = f"chunk_{offset:05d}_{offset + args.chunk_symbols - 1:05d}"
        chunk_dir = chunks_dir / chunk_name
        events_path = chunk_dir / "events.csv"
        if (args.resume or args.skip_existing) and offset in completed and events_path.exists():
            print(f"[skip] {chunk_name}", flush=True)
            all_events.extend(read_csv(events_path))
            continue
        print(f"[run] {chunk_name}", flush=True)
        chunk_dir.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        try:
            raw, diag = fetch_snapshots(
                client,
                start_dt.isoformat(),
                None,
                args.max_rows,
                args.chunk_symbols,
                symbol_offset=offset,
            )
            df, prep_diag, prep_proxy = prepare_snapshots(raw, args.chunk_symbols)
            # future_date is useful for audit; price logic uses future OHLC.
            if not df.empty:
                g = df.groupby("code", group_keys=False)
                for i in range(1, 31):
                    df[f"future_date_{i}d"] = g["trade_date"].shift(-i)
            chunk_events: list[dict[str, Any]] = []
            for case in cases:
                chunk_events.extend(evaluate_case(df, case, symbol_types))
            write_csv(events_path, chunk_events)
            all_events.extend(chunk_events)
            diagnostics.extend({"chunk": chunk_name, **d} for d in diag + prep_diag)
            proxy.extend(prep_proxy)
            status = "ok"
            error = ""
        except Exception as exc:
            status = "failed"
            error = str(exc)
            chunk_events = []
        entry = {
            "chunk": chunk_name,
            "offset": offset,
            "status": status,
            "events": len(chunk_events),
            "elapsed_sec": round(time.time() - t0, 1),
            "error": error,
        }
        manifest["chunks"].append(entry)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    if not all_events:
        write_text(out_dir / "report.txt", "No revalidation events were generated.\n")
        return

    latest = max(datetime.fromisoformat(str(r["signal_date"])).date() for r in all_events if r.get("signal_date"))
    case_rows, monthly_rows, yearly_rows = grouped_summaries(all_events, latest)
    tt = train_test(all_events)
    out_rows = outlier(all_events)
    env_rows = environment_summary(all_events)
    sym_conc = symbol_concentration(all_events)
    cur_exp = current_expectancy(all_events)

    tt_map = {r["case_key"]: r for r in tt}
    out_map = {(r["case_key"], r["variant"]): r for r in out_rows}
    robust = []
    overfit = []
    for row in case_rows:
        if row.get("period") != "all_available":
            continue
        case = row["case_key"]
        t = tt_map.get(case, {})
        both = out_map.get((case, "drop_both_1pct"), {})
        warnings = []
        if fnum(row.get("events")) < 50:
            warnings.append("low_events")
        if t and fnum(t.get("test_PF")) < 1.2:
            warnings.append("weak_test")
        if both and fnum(both.get("PF")) < fnum(row.get("PF")) * 0.7:
            warnings.append("outlier_dependent")
        score = fnum(row.get("PF")) + fnum(t.get("test_PF")) + fnum(row.get("win_rate")) / 100 - abs(fnum(row.get("max_dd_after_cost"))) / 2_000_000
        nr = {**row, "test_PF": t.get("test_PF"), "stable_flag": t.get("stable_flag"), "outlier_drop_both_PF": both.get("PF"), "robust_score": score, "overfit_warning": ";".join(warnings)}
        (overfit if warnings else robust).append(nr)
    robust.sort(key=lambda r: fnum(r.get("robust_score")), reverse=True)
    overfit.sort(key=lambda r: fnum(r.get("PF")), reverse=True)

    write_csv(out_dir / "robust_case_revalidation.csv", robust)
    write_csv(out_dir / "h5_vs_price_band_revalidation.csv", case_rows + load_h5_compare())
    write_csv(out_dir / "price_band_monthly_summary.csv", monthly_rows)
    write_csv(out_dir / "price_band_yearly_summary.csv", yearly_rows)
    write_csv(out_dir / "h5_bad_month_complement.csv", [])
    write_csv(out_dir / "environment_comparison.csv", env_rows)
    write_csv(out_dir / "outlier_sensitivity.csv", out_rows)
    write_csv(out_dir / "symbol_concentration_analysis.csv", sym_conc)
    write_csv(out_dir / "current_price_expectancy.csv", cur_exp)
    write_csv(out_dir / "mix_portfolio_simulation.csv", [])
    write_csv(out_dir / "train_test_stability.csv", tt)
    write_csv(out_dir / "walk_forward_summary.csv", yearly_rows)
    write_csv(out_dir / "join_diagnostics.csv", diagnostics)
    write_csv(out_dir / "proxy_usage.csv", proxy)
    write_csv(out_dir / "price_band_overfit_warning_cases.csv", overfit)

    elapsed = time.time() - started
    manifest["status"] = "complete"
    manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
    manifest["elapsed_sec"] = round(elapsed, 1)
    manifest["events"] = len(all_events)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    best = robust[0] if robust else {}
    type_counts = Counter(r.get("symbol_type") for r in cur_exp)
    report = [
        "# Price Band Best Case Revalidation",
        "",
        "Research-only. No production H5/Primary/LINE/actual_trade_logs/auto-trading changes.",
        "",
        f"- elapsed_hours: {elapsed / 3600:.2f}",
        "- status: complete",
        f"- universe: {args.universe}",
        f"- requested_symbols: {args.max_symbols}",
        f"- events: {len(all_events):,}",
        f"- robust_cases: {len(robust):,}",
        f"- overfit_warning_cases: {len(overfit):,}",
        f"- output_dir: {out_dir}",
        "",
        "## Robust Best Case",
        json.dumps(best, ensure_ascii=False, indent=2, default=str) if best else "No robust case.",
        "",
        "## Symbol-Type Snapshot",
        *(f"- {k}: {v}" for k, v in sorted(type_counts.items())),
        "",
        "## Notes",
        "- This is fixed-case revalidation, not a new grid search.",
        "- CASE_E uses an H5-like proxy because production confirmed/overheat fields are not fully available in the feature snapshot table.",
        "- Empty mix_portfolio_simulation and h5_bad_month_complement files are reserved for a later aligned portfolio simulation pass.",
        "- Next forward-test candidates should come from robust_case_revalidation.csv with stable train/test and low concentration.",
    ]
    write_text(out_dir / "report.txt", "\n".join(report) + "\n")


if __name__ == "__main__":
    main()
