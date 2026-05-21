#!/usr/bin/env python3
"""Import basic J-Quants statements into the existing nikkei_financials table."""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from jquants_client import get_statements, normalize_code

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _first(row: dict, keys: list[str]) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _statement_date(statement: dict) -> str:
    value = _first(statement, [
        "DisclosedDate", "CurrentPeriodEndDate", "CurrentFiscalYearEndDate",
        "LocalDate", "Date", "UpdatedDate",
    ])
    return str(value or "")


def _calc_ratio(numerator: float | None, denominator: float | None, multiplier: float = 1.0) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    try:
        return numerator / denominator * multiplier
    except Exception:
        return None


def _build_financial_row(statement: dict, close_by_code: dict[str, float] | None = None) -> dict | None:
    code = normalize_code(statement.get("LocalCode") or statement.get("Code"))
    if not code:
        return None
    close = (close_by_code or {}).get(code)
    profit = _to_float(_first(statement, [
        "Profit", "ProfitLoss", "ProfitLossAttributableToOwnersOfParent", "NP",
        "NetIncome", "NetIncomeLoss",
    ]))
    operating_profit = _to_float(_first(statement, ["OperatingProfit", "OperatingIncome", "OP"]))
    dividend = _to_float(_first(statement, ["DividendPerShare", "AnnualDividendPerShare", "ResultDividendPerShareAnnual", "DPS", "DEPS"]))
    eps = _to_float(_first(statement, [
        "EarningsPerShare", "ForecastEarningsPerShare", "ResultEarningsPerShare",
        "BasicEarningsPerShare", "EPS",
    ]))
    bps = _to_float(_first(statement, [
        "BookValuePerShare", "ResultBookValuePerShare", "BPS",
    ]))
    per = _to_float(_first(statement, ["PER", "PriceEarningsRatio"]))
    pbr = _to_float(_first(statement, ["PBR", "PriceBookValueRatio"]))
    if per is None and close is not None and eps not in (None, 0):
        per = close / eps
    if pbr is None and close is not None and bps not in (None, 0):
        pbr = close / bps

    equity_ratio = _to_float(_first(statement, [
        "EquityToAssetRatio", "EquityRatio", "CapitalAdequacyRatio",
        "ShareholdersEquityRatio", "SelfCapitalRatio",
    ]))
    net_assets = _to_float(_first(statement, ["NetAssets", "Equity", "ShareholdersEquity"]))
    total_assets = _to_float(_first(statement, ["TotalAssets", "Assets"]))
    if equity_ratio is None:
        equity_ratio = _calc_ratio(net_assets, total_assets, 100.0)
    if equity_ratio is not None and 0 < equity_ratio <= 1.5:
        equity_ratio *= 100.0

    roe = _to_float(_first(statement, ["ROE", "ReturnOnEquity"]))
    if roe is None:
        roe = _calc_ratio(profit, net_assets, 100.0)
    dividend_yield_pct = _to_float(_first(statement, ["DividendYield", "DividendYieldPct", "DividendYieldPercent"]))
    if dividend_yield_pct is None and dividend is not None and close and close > 0:
        dividend_yield_pct = dividend / close * 100.0
    operating_cf = _to_float(_first(statement, [
        "CashFlowsFromOperatingActivities", "OperatingCashFlow", "NetCashProvidedByUsedInOperatingActivities",
    ]))
    return {
        "code": code,
        "is_deficit": profit is not None and profit < 0,
        "dividend_per_share": dividend,
        "dividend_yield_pct": dividend_yield_pct,
        "operating_profit": operating_profit,
        "operating_cf": operating_cf,
        "net_income": profit,
        "per": per,
        "pbr": pbr,
        "eps": eps,
        "bps": bps,
        "equity_ratio": equity_ratio,
        "roe": roe,
        "statement_date": _statement_date(statement) or None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _existing_columns(sb) -> set[str]:
    try:
        rows = sb.table("nikkei_financials").select("*").limit(1).execute().data or []
        if rows:
            return set(rows[0].keys())
    except Exception as e:
        logger.warning("nikkei_financials column probe failed: %s", e)
    return {"code", "is_deficit", "dividend_per_share", "updated_at"}


def _fetch_all(build_query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        res = build_query().range(offset, offset + page_size - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        offset += page_size


def _load_latest_closes(sb) -> dict[str, float]:
    try:
        latest = (
            sb.table("stock_feature_snapshots")
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if not latest:
            return {}
        trade_date = latest[0].get("trade_date")
        rows = _fetch_all(
            lambda: (
                sb.table("stock_feature_snapshots")
                .select("code,close")
                .eq("trade_date", trade_date)
            )
        )
        closes = {}
        for row in rows:
            code = normalize_code(row.get("code"))
            close = _to_float(row.get("close"))
            if code and close:
                closes[code] = close
        logger.info("latest closes loaded: trade_date=%s rows=%d", trade_date, len(closes))
        return closes
    except Exception as e:
        logger.warning("latest close load failed; PER/PBR direct fields only: %s", e)
        return {}


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    end = args.end or datetime.now(JST).date().isoformat()
    start = args.start or (datetime.fromisoformat(end).date() - timedelta(days=365)).isoformat()
    try:
        rows = get_statements(code=args.code, from_date=None if args.code else start, to_date=None if args.code else end)
    except Exception as e:
        logger.warning("J-Quants statements fetch failed; no DB changes: %s", e)
        rows = []
    logger.info("J-Quants statements rows=%d", len(rows))
    close_by_code = _load_latest_closes(sb)
    latest: dict[str, dict] = {}
    latest_key: dict[str, str] = {}
    for st in rows:
        row = _build_financial_row(st, close_by_code)
        if row:
            key = str(row.get("statement_date") or "")
            if row["code"] in latest and key < latest_key.get(row["code"], ""):
                continue
            latest[row["code"]] = row
            latest_key[row["code"]] = key
    out = list(latest.values())
    if args.limit:
        out = out[: int(args.limit)]
    logger.info("financial rows prepared=%d", len(out))
    if args.dry_run:
        logger.info("DRYRUN sample=%s", out[:3])
        return
    cols = _existing_columns(sb)
    filtered = [{k: v for k, v in row.items() if k in cols} for row in out]
    for i in range(0, len(filtered), int(args.batch_size)):
        sb.table("nikkei_financials").upsert(filtered[i:i + int(args.batch_size)], on_conflict="code").execute()
    logger.info("upsert nikkei_financials rows=%d", len(filtered))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import J-Quants statements")
    p.add_argument("--code")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch-size", type=int, default=200)
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
