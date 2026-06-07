#!/usr/bin/env python3
"""Fetch market index/volatility data for research reports.

Research only. Writes CSV files under outputs/market_data and does not touch
production trading logic, DB tables, LINE notifications, or actual_trade_logs.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "outputs/market_data"
DEFAULT_START = "2024-12-01"
DEFAULT_END = ""

TICKERS = {
    "^VIX": "VIX",
    "^N225": "nikkei225",
    "1306.T": "topix_etf_proxy",
    "^IXIC": "nasdaq",
    "^SOX": "sox",
    "USDJPY=X": "usdjpy",
    "^TNX": "us10y_yield",
}


def date_text(value: Any) -> str:
    return str(value or "").split("T", 1)[0][:10]


def parse_date(value: Any) -> date | None:
    text = date_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def year_month(value: Any) -> str:
    dt = parse_date(value)
    return f"{dt.year}-{dt.month:02d}" if dt else "unknown"


def fnum(value: Any) -> float | None:
    try:
        if value in (None, "", "nan", "NaN"):
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def longest_streak(values: list[float], predicate) -> int:
    best = cur = 0
    for value in values:
        if predicate(value):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def max_drawdown_pct(closes: list[float]) -> float | None:
    peak: float | None = None
    max_dd = 0.0
    for close in closes:
        if peak is None or close > peak:
            peak = close
        if peak:
            max_dd = min(max_dd, (close / peak - 1.0) * 100.0)
    return max_dd


def fetch_one(ticker: str, name: str, start: str, end: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    kwargs = {"start": start, "progress": False, "auto_adjust": False}
    if end:
        kwargs["end"] = end
    df = yf.download(ticker, **kwargs)
    if df is None or df.empty:
        return [], {"ticker": ticker, "name": name, "status": "empty"}
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [str(c[0]) for c in df.columns]
    rows: list[dict[str, Any]] = []
    prev_close: float | None = None
    for idx, rec in df.iterrows():
        close = fnum(rec.get("Close"))
        if close is None:
            continue
        ret = (close / prev_close - 1.0) * 100.0 if prev_close else None
        prev_close = close
        rows.append({
            "date": date_text(idx),
            "ticker": ticker,
            "name": name,
            "open": fnum(rec.get("Open")),
            "high": fnum(rec.get("High")),
            "low": fnum(rec.get("Low")),
            "close": close,
            "adj_close": fnum(rec.get("Adj Close")),
            "volume": fnum(rec.get("Volume")),
            "return_pct": ret,
        })
    return rows, {"ticker": ticker, "name": name, "status": "ok", "rows": len(rows)}


def monthly_rows(daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        groups[(str(row["ticker"]), year_month(row["date"]))].append(row)
    out: list[dict[str, Any]] = []
    for (ticker, ym), rows in sorted(groups.items()):
        rows = sorted(rows, key=lambda r: r["date"])
        closes = [fnum(r.get("close")) for r in rows]
        closes = [v for v in closes if v is not None]
        rets = [fnum(r.get("return_pct")) for r in rows]
        rets = [v for v in rets if v is not None]
        if not closes:
            continue
        monthly_ret = (closes[-1] / closes[0] - 1.0) * 100.0 if closes[0] else None
        out.append({
            "year_month": ym,
            "ticker": ticker,
            "name": rows[0].get("name"),
            "first_date": rows[0].get("date"),
            "last_date": rows[-1].get("date"),
            "trading_days": len(rows),
            "monthly_return_pct": monthly_ret,
            "avg_close": mean(closes),
            "max_close": max(closes),
            "end_close": closes[-1],
            "prev_day_return_std": pstdev(rets) if len(rets) > 1 else None,
            "daily_return_mean": mean(rets) if rets else None,
            "max_daily_gain_pct": max(rets) if rets else None,
            "max_daily_drop_pct": min(rets) if rets else None,
            "down_1pct_days": sum(1 for v in rets if v <= -1.0),
            "down_2pct_days": sum(1 for v in rets if v <= -2.0),
            "down_3pct_days": sum(1 for v in rets if v <= -3.0),
            "up_2pct_days": sum(1 for v in rets if v >= 2.0),
            "up_3pct_days": sum(1 for v in rets if v >= 3.0),
            "longest_down_streak": longest_streak(rets, lambda v: v < 0),
            "longest_up_streak": longest_streak(rets, lambda v: v > 0),
            "max_drawdown_pct": max_drawdown_pct(closes),
        })
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    all_daily: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for ticker, name in TICKERS.items():
        rows, diag = fetch_one(ticker, name, args.start, args.end)
        diagnostics.append(diag)
        all_daily.extend(rows)

    monthly = monthly_rows(all_daily)
    write_csv(out_dir / "daily_market_indices.csv", all_daily)
    write_csv(out_dir / "monthly_market_volatility.csv", monthly)
    write_csv(out_dir / "fetch_diagnostics.csv", diagnostics)

    print(f"output_dir={out_dir}")
    print(f"daily_rows={len(all_daily)}")
    print(f"monthly_rows={len(monthly)}")
    for diag in diagnostics:
        print(f"{diag.get('ticker')} {diag.get('status')} rows={diag.get('rows', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
