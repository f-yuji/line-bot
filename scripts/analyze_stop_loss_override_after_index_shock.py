#!/usr/bin/env python3
"""Analyze whether H5 stop-loss breaches should be delayed after index shocks.

Read-only research script. It writes CSV/text outputs only and never changes
Primary/H5 production logic, LINE, actual_trade_logs, virtual_trades, or
automation state.
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "stop_loss_override_after_index_shock"

H5_CASE_ROWS = ROOT / "outputs" / "h5_stored_forward_cases" / "case_daily_rows.csv"
EXPERIMENTAL_ROWS = ROOT / "outputs" / "experimental_virtual_cases" / "virtual_case_daily_rows.csv"
MIX_ROWS = ROOT / "outputs" / "trend_following_deep_backtest" / "12_portfolio_mix_daily_rows.csv"
ENV_ROWS = ROOT / "outputs" / "h5_environment_meter" / "environment_daily_rows.csv"
MARKET_DAILY = ROOT / "outputs" / "market_data" / "daily_market_indices.csv"

TARGET_CASES = {
    "current_h5": "current_h5_core",
    "current_h5_core": "current_h5_core",
    "H5_short_pullback_drop5_m3": "H5_short_pullback_drop5_m3",
    "H5_current7_short3": "H5_current7_short3",
    "mix_current7_short3_trend_7_3": "mix_current7_short3_trend_7_3",
}
STOP_LEVELS = [-4.0, -5.0, -6.0, -8.0]
POSITION_AMOUNT = 300_000.0
COST_BPS = 10.0


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def fnum(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        v = float(value)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def dtext(value: Any) -> str:
    if value is None:
        return ""
    return str(value)[:10]


def parse_date(value: Any) -> date | None:
    txt = dtext(value)
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt).date()
    except Exception:
        return None


def norm_code(value: Any) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    try:
        return str(int(float(txt)))
    except Exception:
        return txt


def pct_from_prices(price: float | None, entry: float | None) -> float | None:
    if price is None or entry is None or entry <= 0:
        return None
    return (price / entry - 1.0) * 100.0


def pnl_from_return(return_pct: float | None) -> float:
    if return_pct is None:
        return 0.0
    return POSITION_AMOUNT * (return_pct / 100.0) - POSITION_AMOUNT * (COST_BPS / 10000.0)


def pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses == 0:
        return None if gains == 0 else 999.0
    return gains / losses


def mean(values: list[float]) -> float | None:
    clean = [v for v in values if v is not None]
    return sum(clean) / len(clean) if clean else None


@dataclass
class MarketLookup:
    env_by_date: dict[str, dict[str, Any]]
    market_by_ticker: dict[str, dict[str, dict[str, Any]]]
    dates_by_ticker: dict[str, list[str]]

    def market_return(self, ticker: str, day: str, *, before: bool = False) -> float | None:
        dates = self.dates_by_ticker.get(ticker) or []
        target = ""
        if before:
            for d in dates:
                if d < day:
                    target = d
                elif d >= day:
                    break
        else:
            target = day if day in self.market_by_ticker.get(ticker, {}) else ""
            if not target:
                for d in dates:
                    if d <= day:
                        target = d
                    else:
                        break
        row = self.market_by_ticker.get(ticker, {}).get(target)
        return fnum((row or {}).get("return_pct"))


def load_market_lookup() -> MarketLookup:
    env = {dtext(r.get("date")): r for r in read_csv(ENV_ROWS) if dtext(r.get("date"))}
    by_ticker: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in read_csv(MARKET_DAILY):
        ticker = str(row.get("ticker") or "")
        day = dtext(row.get("date"))
        if ticker and day:
            by_ticker[ticker][day] = row
    dates_by = {ticker: sorted(rows.keys()) for ticker, rows in by_ticker.items()}
    return MarketLookup(env_by_date=env, market_by_ticker=dict(by_ticker), dates_by_ticker=dates_by)


def load_trade_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    def add(row: dict[str, Any], case_key: str, source: str) -> None:
        code = norm_code(row.get("code"))
        entry_date = dtext(row.get("entry_date") or row.get("signal_date"))
        entry_price = fnum(row.get("entry_price"))
        if not code or not entry_date or entry_price is None or entry_price <= 0:
            diagnostics.append({"source": source, "reason": "missing_code_entry_date_or_price", "code": code})
            return
        rows.append({
            "case_key": case_key,
            "source_file": source,
            "signal_date": dtext(row.get("signal_date")),
            "entry_date": entry_date,
            "exit_date": dtext(row.get("exit_date")),
            "code": code,
            "name": row.get("name"),
            "score": fnum(row.get("score")),
            "signal_stage": row.get("signal_stage"),
            "entry_price": entry_price,
            "base_return_pct": fnum(row.get("return_pct")),
            "base_pnl_after_cost": fnum(row.get("pnl_after_cost")),
            "gap": fnum(row.get("gap")),
            "drop5": fnum(row.get("drop5")),
            "drop10": fnum(row.get("drop10")),
            "drop20": fnum(row.get("drop20")),
            "overheat_score": fnum(row.get("overheat_score")),
        })

    for row in read_csv(H5_CASE_ROWS):
        raw_case = str(row.get("case_key") or "")
        case = TARGET_CASES.get(raw_case)
        if case in {"current_h5_core", "H5_short_pullback_drop5_m3", "H5_current7_short3"}:
            add(row, case, str(H5_CASE_ROWS.relative_to(ROOT)))

    for row in read_csv(EXPERIMENTAL_ROWS):
        raw_case = str(row.get("case_key") or "")
        case = TARGET_CASES.get(raw_case)
        if case in {"H5_short_pullback_drop5_m3", "H5_current7_short3"}:
            add(row, case, str(EXPERIMENTAL_ROWS.relative_to(ROOT)))

    for row in read_csv(MIX_ROWS):
        if str(row.get("mix_case_key") or "") == "mix_current7_short3_trend_7_3":
            add(row, "mix_current7_short3_trend_7_3", str(MIX_ROWS.relative_to(ROOT)))

    seen = set()
    unique = []
    for row in rows:
        key = (row["case_key"], row["entry_date"], row["code"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    diagnostics.append({"source": "trade_rows", "reason": "loaded_unique", "rows": len(unique)})
    return unique, diagnostics


def fetch_price_history(sb, trades: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    by_code_dates: dict[str, tuple[date, date]] = {}
    for row in trades:
        code = row["code"]
        entry = parse_date(row["entry_date"])
        if not entry:
            continue
        start = entry
        end = entry + timedelta(days=12)
        old = by_code_dates.get(code)
        if old:
            start = min(start, old[0])
            end = max(end, old[1])
        by_code_dates[code] = (start, end)

    out: dict[str, list[dict[str, Any]]] = {}
    diagnostics = []
    select_cols = (
        "trade_date,code,name,open,high,low,close,day_change_pct,return_1d_pct,"
        "nikkei_change_pct,topix_change_pct,sector_change_pct,volume_ratio_20d,"
        "bad_news_score,sector_risk_score,market_shock_score,rsi14,ma5_gap_pct"
    )
    for idx, (code, (start, end)) in enumerate(sorted(by_code_dates.items()), 1):
        try:
            rows = (
                sb.table("stock_feature_snapshots")
                .select(select_cols)
                .eq("code", code)
                .gte("trade_date", start.isoformat())
                .lte("trade_date", end.isoformat())
                .order("trade_date", desc=False)
                .execute()
                .data or []
            )
            out[code] = rows
            if not rows:
                diagnostics.append({"source": "stock_feature_snapshots", "code": code, "reason": "no_price_rows"})
        except Exception as exc:
            diagnostics.append({"source": "stock_feature_snapshots", "code": code, "reason": "fetch_failed", "detail": str(exc)[:200]})
        if idx % 100 == 0:
            print(f"[stop_loss_override] price history progress {idx}/{len(by_code_dates)}", flush=True)
    diagnostics.append({"source": "stock_feature_snapshots", "reason": "codes_requested", "rows": len(by_code_dates)})
    return out, diagnostics


def find_window(history: list[dict[str, Any]], entry_date: str, max_days: int = 5) -> list[dict[str, Any]]:
    rows = [r for r in history if dtext(r.get("trade_date")) >= entry_date]
    return rows[: max_days + 1]


def classify_loss(stop_row: dict[str, Any], market: MarketLookup) -> tuple[str, str, dict[str, Any]]:
    day = dtext(stop_row.get("trade_date"))
    env = market.env_by_date.get(day) or {}
    nikkei = fnum(stop_row.get("nikkei_change_pct"))
    topix = fnum(stop_row.get("topix_change_pct"))
    sox_prev = market.market_return("^SOX", day, before=True)
    nasdaq_prev = market.market_return("^IXIC", day, before=True)
    status = str(env.get("environment_status") or "")
    tags = str(env.get("environment_tags") or "")
    darasage = fnum(env.get("darasage_score"), 0.0) or 0.0
    crash = fnum(env.get("crash_rebound_score"), 0.0) or 0.0
    bad_news = fnum(stop_row.get("bad_news_score"), 0.0) or 0.0
    sector = fnum(stop_row.get("sector_change_pct"))
    volume_ratio = fnum(stop_row.get("volume_ratio_20d"), 0.0) or 0.0
    stock_day = fnum(stop_row.get("day_change_pct"), fnum(stop_row.get("return_1d_pct")))

    index_shock = (
        (nikkei is not None and nikkei <= -1.5)
        or (topix is not None and topix <= -1.5)
        or (sox_prev is not None and sox_prev <= -3.0)
        or (nasdaq_prev is not None and nasdaq_prev <= -2.0)
        or status == "H5 favorable"
        or "crash rebound" in tags
    )
    individual = (
        not index_shock
        and bad_news > 0
        or (
            not index_shock
            and stock_day is not None
            and stock_day <= -3.0
            and (nikkei is None or nikkei > -1.0)
            and (topix is None or topix > -1.0)
            and (sector is None or sector > -1.5)
            and volume_ratio >= 1.5
        )
    )
    darasage_loss = (
        not index_shock
        and (status in {"H5 warning", "darasage risk"} or darasage >= 3)
        and crash <= 1
    )
    if index_shock and bad_news <= 0:
        loss_type = "index_shock_loss"
    elif individual:
        loss_type = "individual_weakness_loss"
    elif darasage_loss:
        loss_type = "darasage_loss"
    else:
        loss_type = "individual_weakness_loss" if bad_news > 0 else "darasage_loss"
    reason = (
        f"nikkei={nikkei} topix={topix} sox_prev={sox_prev} nasdaq_prev={nasdaq_prev} "
        f"env={status} tags={tags} bad_news={bad_news} sector={sector} vol_ratio={volume_ratio}"
    )
    metrics = {
        "nikkei_change_pct": nikkei,
        "topix_change_pct": topix,
        "sox_prev_return_pct": sox_prev,
        "nasdaq_prev_return_pct": nasdaq_prev,
        "environment_status": status,
        "environment_score": fnum(env.get("environment_score")),
        "environment_tags": tags,
        "darasage_score": darasage,
        "crash_rebound_score": crash,
        "bad_news_score": bad_news,
        "sector_change_pct": sector,
        "volume_ratio_20d": volume_ratio,
        "stock_day_change_pct": stock_day,
    }
    return loss_type, reason, metrics


def first_close_at_or_above(rows: list[dict[str, Any]], threshold: float) -> dict[str, Any] | None:
    for row in rows:
        close = fnum(row.get("close"))
        if close is not None and close >= threshold:
            return row
    return None


def make_events(trades: list[dict[str, Any]], histories: dict[str, list[dict[str, Any]]], market: MarketLookup) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for trade in trades:
        window = find_window(histories.get(trade["code"], []), trade["entry_date"], max_days=5)
        if len(window) < 2:
            continue
        entry_price = fnum(trade.get("entry_price"))
        if entry_price is None or entry_price <= 0:
            continue
        for level in STOP_LEVELS:
            for basis, price_col in [("close", "close"), ("intraday_low", "low")]:
                stop_row = None
                stop_index = None
                for i, row in enumerate(window):
                    price = fnum(row.get(price_col))
                    ret = pct_from_prices(price, entry_price)
                    if ret is not None and ret <= level:
                        stop_row = row
                        stop_index = i
                        break
                if stop_row is None or stop_index is None:
                    continue
                after = window[stop_index + 1 :]
                through_hd5 = window[stop_index:]
                stop_close = fnum(stop_row.get("close"))
                stop_price_for_normal = stop_close if basis == "close" else entry_price * (1 + level / 100.0)
                normal_return = pct_from_prices(stop_price_for_normal, entry_price)
                hd5_row = window[-1]
                hd5_return = pct_from_prices(fnum(hd5_row.get("close")), entry_price)
                breakeven_row = first_close_at_or_above(after, entry_price)
                within1_row = first_close_at_or_above(after, entry_price * 0.99)
                best_close_ret = max([pct_from_prices(fnum(r.get("close")), entry_price) for r in through_hd5 if pct_from_prices(fnum(r.get("close")), entry_price) is not None] or [None])
                worst_low_ret = min([pct_from_prices(fnum(r.get("low")), entry_price) for r in through_hd5 if pct_from_prices(fnum(r.get("low")), entry_price) is not None] or [None])
                loss_type, reason, metrics = classify_loss(stop_row, market)
                event = {
                    **trade,
                    **metrics,
                    "stop_level": level,
                    "trigger_basis": basis,
                    "stop_date": dtext(stop_row.get("trade_date")),
                    "holding_day_at_stop": stop_index,
                    "stop_close": stop_close,
                    "stop_low": fnum(stop_row.get("low")),
                    "stop_return_pct": pct_from_prices(fnum(stop_row.get(price_col)), entry_price),
                    "loss_type": loss_type,
                    "classification_reason": reason,
                    "normal_exit_date": dtext(stop_row.get("trade_date")),
                    "normal_return_pct": normal_return,
                    "normal_pnl": pnl_from_return(normal_return),
                    "hd5_exit_date": dtext(hd5_row.get("trade_date")),
                    "hd5_return_pct": hd5_return,
                    "hd5_pnl": pnl_from_return(hd5_return),
                    "breakeven_recovered_next_day": bool(after[:1] and (fnum(after[0].get("close")) or -1) >= entry_price),
                    "breakeven_recovered_2d": bool(first_close_at_or_above(after[:2], entry_price)),
                    "breakeven_recovered_3d": bool(first_close_at_or_above(after[:3], entry_price)),
                    "breakeven_recovered_hd5": bool(breakeven_row),
                    "within1_recovered_hd5": bool(within1_row),
                    "breakeven_recovery_date": dtext((breakeven_row or {}).get("trade_date")),
                    "within1_recovery_date": dtext((within1_row or {}).get("trade_date")),
                    "max_rebound_return_pct": best_close_ret,
                    "worst_low_return_pct": worst_low_ret,
                    "further_dug": bool(worst_low_ret is not None and worst_low_ret <= level - 2.0),
                    "max_dd_worsening_pct": (worst_low_ret - level) if worst_low_ret is not None else None,
                }
                events.append(event)
    return events


def rule_exit(event: dict[str, Any], rule: str) -> tuple[str, float | None, str]:
    loss_type = str(event.get("loss_type") or "")
    env_status = str(event.get("environment_status") or "")
    tags = str(event.get("environment_tags") or "")
    normal = fnum(event.get("normal_return_pct"))
    hd5 = fnum(event.get("hd5_return_pct"))
    if rule == "A_normal_stop":
        return str(event.get("normal_exit_date")), normal, "normal_stop"
    if rule == "B_index_shock_extend_hd5":
        if loss_type == "index_shock_loss":
            if event.get("breakeven_recovered_hd5"):
                return str(event.get("breakeven_recovery_date")), 0.0, "breakeven_recovery"
            return str(event.get("hd5_exit_date")), hd5, "hd5_forced"
        return str(event.get("normal_exit_date")), normal, "normal_stop"
    if rule == "C_index_shock_next_day_confirm":
        if loss_type != "index_shock_loss":
            return str(event.get("normal_exit_date")), normal, "normal_stop"
        # Approximate next-day confirmation with close-to-close improvement.
        if event.get("within1_recovered_hd5") or event.get("breakeven_recovered_2d"):
            if event.get("breakeven_recovered_hd5"):
                return str(event.get("breakeven_recovery_date")), 0.0, "confirmed_then_breakeven"
            return str(event.get("within1_recovery_date")), -1.0, "confirmed_within1"
        return str(event.get("normal_exit_date")), normal, "no_rebound_next_day"
    if rule == "D_favorable_only_extend":
        favorable = env_status == "H5 favorable" or "crash rebound" in tags
        if favorable:
            if event.get("breakeven_recovered_hd5"):
                return str(event.get("breakeven_recovery_date")), 0.0, "favorable_breakeven"
            return str(event.get("hd5_exit_date")), hd5, "favorable_hd5"
        return str(event.get("normal_exit_date")), normal, "normal_stop"
    if rule == "E_darasage_immediate_index_extend":
        if loss_type == "darasage_loss":
            return str(event.get("normal_exit_date")), normal, "darasage_immediate"
        if loss_type == "index_shock_loss":
            if event.get("breakeven_recovered_hd5"):
                return str(event.get("breakeven_recovery_date")), 0.0, "index_breakeven"
            return str(event.get("hd5_exit_date")), hd5, "index_hd5"
        return str(event.get("normal_exit_date")), normal, "normal_stop"
    return str(event.get("normal_exit_date")), normal, "normal_stop"


def build_rule_comparison(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rules = [
        "A_normal_stop",
        "B_index_shock_extend_hd5",
        "C_index_shock_next_day_confirm",
        "D_favorable_only_extend",
        "E_darasage_immediate_index_extend",
    ]
    out = []
    for ev in events:
        normal_pnl = fnum(ev.get("normal_pnl"), 0.0) or 0.0
        for rule in rules:
            exit_date, ret, reason = rule_exit(ev, rule)
            pnl = pnl_from_return(ret)
            out.append({
                "case_key": ev.get("case_key"),
                "code": ev.get("code"),
                "name": ev.get("name"),
                "entry_date": ev.get("entry_date"),
                "stop_date": ev.get("stop_date"),
                "stop_level": ev.get("stop_level"),
                "trigger_basis": ev.get("trigger_basis"),
                "loss_type": ev.get("loss_type"),
                "rule": rule,
                "exit_date": exit_date,
                "exit_reason": reason,
                "return_pct": ret,
                "pnl_after_cost": pnl,
                "delta_vs_normal_pnl": pnl - normal_pnl,
            })
    return out


def summarize_events(events: list[dict[str, Any]], group_cols: list[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for ev in events:
        groups[tuple(ev.get(c) for c in group_cols)].append(ev)
    out = []
    for key, items in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        row = {col: val for col, val in zip(group_cols, key)}
        normal = [fnum(x.get("normal_pnl"), 0.0) or 0.0 for x in items]
        hd5 = [fnum(x.get("hd5_pnl"), 0.0) or 0.0 for x in items]
        row.update({
            "events": len(items),
            "next_day_breakeven_rate": sum(1 for x in items if x.get("breakeven_recovered_next_day")) / len(items) * 100,
            "two_day_breakeven_rate": sum(1 for x in items if x.get("breakeven_recovered_2d")) / len(items) * 100,
            "three_day_breakeven_rate": sum(1 for x in items if x.get("breakeven_recovered_3d")) / len(items) * 100,
            "hd5_breakeven_rate": sum(1 for x in items if x.get("breakeven_recovered_hd5")) / len(items) * 100,
            "hd5_within1_rate": sum(1 for x in items if x.get("within1_recovered_hd5")) / len(items) * 100,
            "further_dug_rate": sum(1 for x in items if x.get("further_dug")) / len(items) * 100,
            "avg_max_rebound_return_pct": mean([fnum(x.get("max_rebound_return_pct")) for x in items if fnum(x.get("max_rebound_return_pct")) is not None]),
            "avg_worst_low_return_pct": mean([fnum(x.get("worst_low_return_pct")) for x in items if fnum(x.get("worst_low_return_pct")) is not None]),
            "normal_pnl_total": sum(normal),
            "hd5_pnl_total": sum(hd5),
            "hd5_delta_vs_normal": sum(hd5) - sum(normal),
            "avg_hd5_delta_vs_normal": (sum(hd5) - sum(normal)) / len(items),
            "worst_extension_delta": min([(h - n) for h, n in zip(hd5, normal)] or [0]),
            "PF_hd5": pf(hd5),
            "win_rate_hd5": sum(1 for v in hd5 if v > 0) / len(hd5) * 100 if hd5 else None,
        })
        out.append(row)
    return out


def summarize_rules(rule_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rule_rows:
        groups[(row.get("case_key"), row.get("stop_level"), row.get("trigger_basis"), row.get("loss_type"), row.get("rule"))].append(row)
    out = []
    for key, rows in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        pnls = [fnum(r.get("pnl_after_cost"), 0.0) or 0.0 for r in rows]
        deltas = [fnum(r.get("delta_vs_normal_pnl"), 0.0) or 0.0 for r in rows]
        out.append({
            "case_key": key[0],
            "stop_level": key[1],
            "trigger_basis": key[2],
            "loss_type": key[3],
            "rule": key[4],
            "events": len(rows),
            "pnl_after_cost": sum(pnls),
            "PF": pf(pnls),
            "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
            "delta_vs_normal_pnl": sum(deltas),
            "avg_delta_vs_normal_pnl": mean(deltas),
            "worst_delta_vs_normal_pnl": min(deltas) if deltas else None,
        })
    return out


def current_position_like(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for ev in events:
        if (
            str(ev.get("trigger_basis")) == "close"
            and fnum(ev.get("stop_level")) in {-5.0, -6.0}
            and fnum(ev.get("holding_day_at_stop"), 99) in {2, 3, 4}
            and fnum(ev.get("stop_return_pct"), 0.0) <= -5.0
            and str(ev.get("loss_type")) == "index_shock_loss"
            and (str(ev.get("environment_status")) == "H5 favorable" or fnum(ev.get("crash_rebound_score"), 0.0) >= 2)
        ):
            out.append(ev)
    return out


def report_text(events: list[dict[str, Any]], summaries: dict[str, list[dict[str, Any]]], rule_summary: list[dict[str, Any]]) -> str:
    def pick_loss(loss_type: str, stop: float = -5.0) -> dict[str, Any]:
        rows = [r for r in summaries["by_loss_type"] if r.get("loss_type") == loss_type and fnum(r.get("stop_level")) == stop and r.get("trigger_basis") == "close"]
        return rows[0] if rows else {}

    def best_rule(loss_type: str, stop: float = -5.0) -> dict[str, Any]:
        rows = [
            r for r in rule_summary
            if r.get("loss_type") == loss_type
            and fnum(r.get("stop_level")) == stop
            and r.get("trigger_basis") == "close"
            and r.get("rule") != "A_normal_stop"
        ]
        return max(rows, key=lambda r: fnum(r.get("delta_vs_normal_pnl"), -10**18) or -10**18) if rows else {}

    index5 = pick_loss("index_shock_loss")
    indiv5 = pick_loss("individual_weakness_loss")
    dara5 = pick_loss("darasage_loss")
    best_index = best_rule("index_shock_loss")
    best_dara = best_rule("darasage_loss")
    current_like = summaries["current_like"]
    current_like_rate = (
        sum(1 for r in current_like if r.get("breakeven_recovered_hd5")) / len(current_like) * 100
        if current_like else None
    )
    lines = [
        "Stop-loss override after index shock analysis",
        "",
        "Safety:",
        "- Read-only research script.",
        "- No Primary/H5 production entry or exit logic changed.",
        "- No LINE, actual_trade_logs writes, or auto-trading changes.",
        "",
        "Classification logic:",
        "- index_shock_loss: Nikkei <= -1.5%, TOPIX <= -1.5%, previous SOX <= -3%, previous NASDAQ <= -2%, or H5 ENV favorable/crash rebound, with no bad-news flag.",
        "- individual_weakness_loss: index is not shocked and the stock falls alone, bad-news flag exists, or stock/volume weakness is isolated.",
        "- darasage_loss: H5 warning/darasage risk with weak crash rebound score and no index shock.",
        "",
        "Key -5% close-trigger findings:",
        f"- index_shock_loss events: {index5.get('events', 0)}, HD5 breakeven rate: {fnum(index5.get('hd5_breakeven_rate'), 0):.1f}%, HD5 delta vs normal: {fnum(index5.get('hd5_delta_vs_normal'), 0):,.0f}",
        f"- individual_weakness_loss events: {indiv5.get('events', 0)}, HD5 breakeven rate: {fnum(indiv5.get('hd5_breakeven_rate'), 0):.1f}%, HD5 delta vs normal: {fnum(indiv5.get('hd5_delta_vs_normal'), 0):,.0f}",
        f"- darasage_loss events: {dara5.get('events', 0)}, HD5 breakeven rate: {fnum(dara5.get('hd5_breakeven_rate'), 0):.1f}%, HD5 delta vs normal: {fnum(dara5.get('hd5_delta_vs_normal'), 0):,.0f}",
        "",
        "Best override candidates:",
        f"- index_shock_loss best rule: {best_index.get('rule')} delta={fnum(best_index.get('delta_vs_normal_pnl'), 0):,.0f} worst_delta={fnum(best_index.get('worst_delta_vs_normal_pnl'), 0):,.0f}",
        f"- darasage_loss best rule: {best_dara.get('rule')} delta={fnum(best_dara.get('delta_vs_normal_pnl'), 0):,.0f} worst_delta={fnum(best_dara.get('worst_delta_vs_normal_pnl'), 0):,.0f}",
        "",
        "Current-position-like cases:",
        f"- rows: {len(current_like)}",
        f"- HD5 breakeven recovery rate: {fnum(current_like_rate, 0):.1f}%",
        "",
        "Proposed operating rules to forward-test, not implement yet:",
        "- Rule idea 1: normal stop around -5%; if H5 favorable + index_shock_loss only, wait until HD5 or breakeven.",
        "- Rule idea 2: do not override individual_weakness_loss; cut normally because isolated weakness is less likely to mean-revert.",
        "- Rule idea 3: darasage_loss should generally remain immediate stop unless the report shows strong positive delta in your preferred case.",
        "- Rule idea 4: use close-trigger evidence as the main operational reference; intraday-low triggers are noisier with daily data.",
    ]
    return "\n".join(lines) + "\n"


def load_actual_trade_context(sb) -> list[dict[str, Any]]:
    try:
        return (
            sb.table("actual_trade_logs")
            .select("trade_date,code,name,case_key,actual_entry_price,actual_exit_price,skip_reason,note")
            .order("created_at", desc=True)
            .limit(100)
            .execute()
            .data or []
        )
    except Exception as exc:
        return [{"error": str(exc)[:200]}]


def main() -> int:
    load_dotenv()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("[stop_loss_override] loading trades...", flush=True)
    trades, diag = load_trade_rows()
    market = load_market_lookup()
    sb = build_supabase()
    actual_context = load_actual_trade_context(sb)
    print(f"[stop_loss_override] loaded trades={len(trades)}", flush=True)
    histories, price_diag = fetch_price_history(sb, trades)
    diag.extend(price_diag)
    print("[stop_loss_override] building stop events...", flush=True)
    events = make_events(trades, histories, market)
    rule_rows = build_rule_comparison(events)
    rule_summary = summarize_rules(rule_rows)
    by_loss_type = summarize_events(events, ["case_key", "stop_level", "trigger_basis", "loss_type"])
    by_stop = summarize_events(events, ["case_key", "stop_level", "trigger_basis"])
    env_summary = summarize_events(events, ["case_key", "stop_level", "trigger_basis", "environment_status"])
    current_like = current_position_like(events)
    top_recovery = sorted(events, key=lambda r: fnum(r.get("max_rebound_return_pct"), -999) or -999, reverse=True)[:50]
    worst_extension = sorted(events, key=lambda r: (fnum(r.get("hd5_pnl"), 0) or 0) - (fnum(r.get("normal_pnl"), 0) or 0))[:50]

    diag.append({"source": "environment_daily_rows", "reason": "loaded_rows", "rows": len(market.env_by_date)})
    diag.append({"source": "daily_market_indices", "reason": "loaded_tickers", "rows": len(market.market_by_ticker)})
    diag.append({"source": "actual_trade_logs", "reason": "read_only_context_rows", "rows": len(actual_context)})
    diag.append({"source": "stop_loss_events", "reason": "generated_rows", "rows": len(events)})

    write_csv(OUT_DIR / "stop_loss_events.csv", events)
    write_csv(OUT_DIR / "stop_loss_rule_comparison.csv", rule_summary)
    write_csv(OUT_DIR / "recovery_rate_by_loss_type.csv", by_loss_type)
    write_csv(OUT_DIR / "recovery_rate_by_stop_level.csv", by_stop)
    write_csv(OUT_DIR / "environment_stop_loss_summary.csv", env_summary)
    write_csv(OUT_DIR / "current_position_like_cases.csv", current_like)
    write_csv(OUT_DIR / "top_recovery_cases.csv", top_recovery)
    write_csv(OUT_DIR / "worst_extension_cases.csv", worst_extension)
    write_csv(OUT_DIR / "join_diagnostics.csv", diag)
    write_csv(OUT_DIR / "actual_trade_logs_context.csv", actual_context)
    summaries = {
        "by_loss_type": by_loss_type,
        "by_stop": by_stop,
        "env": env_summary,
        "current_like": current_like,
    }
    write_text(OUT_DIR / "report.txt", report_text(events, summaries, rule_summary))
    print(f"[stop_loss_override] events={len(events)} out={OUT_DIR}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
