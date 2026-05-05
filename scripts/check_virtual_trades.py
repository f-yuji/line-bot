#!/usr/bin/env python3
"""
Check open virtual_trades and close take-profit / stop-loss / expired trades.
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

try:
    import pandas as pd
    import yfinance as yf

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


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
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _biz_days(from_dt: datetime, to_dt: datetime) -> int:
    days, cur = 0, from_dt.date()
    end = to_dt.date()
    while cur < end:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def _is_non_japanese(row: dict) -> bool:
    code = str(row.get("code") or "").strip()
    market = str(row.get("market") or "").strip().lower()
    return (bool(code) and code.isalpha()) or market in {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}


def _fetch_since_entry(code: str, buy_date: str, holding_days: int) -> list[dict]:
    start = datetime.fromisoformat(str(buy_date).replace("Z", "+00:00")).date()
    end = datetime.now(timezone.utc).date() + timedelta(days=1)
    hist = yf.Ticker(f"{code}.T").history(
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=False,
    )
    if hist is None or hist.empty:
        return []
    rows = []
    for idx, r in hist.iterrows():
        d = pd.Timestamp(idx).tz_localize(None).date().isoformat()
        rows.append({"date": d, "high": r.get("High"), "low": r.get("Low"), "close": r.get("Close")})
    return rows[: holding_days + 3]


def evaluate_trade(trade: dict, *, take_profit: float, stop_loss: float, holding_days: int) -> dict | None:
    buy = _to_float(trade.get("buy_price"))
    if buy is None or buy <= 0 or not trade.get("buy_date"):
        return None
    rows = _fetch_since_entry(str(trade.get("code")), str(trade.get("buy_date")), holding_days)
    if not rows:
        return None

    tp_price = buy * (1 + take_profit / 100.0)
    sl_price = buy * (1 + stop_loss / 100.0)
    max_return = max((_to_float(r["high"]) or buy) / buy - 1 for r in rows) * 100.0
    max_drawdown = min((_to_float(r["low"]) or buy) / buy - 1 for r in rows) * 100.0

    exit_reason = None
    exit_price = _to_float(rows[-1].get("close"))
    exit_date = rows[-1]["date"]
    for r in rows[1:]:
        high = _to_float(r.get("high"))
        close = _to_float(r.get("close"))
        if close is not None and close <= sl_price:
            exit_reason = "stop_loss"
            exit_price = close
            exit_date = r["date"]
            break
        if high is not None and high >= tp_price:
            exit_reason = "take_profit"
            exit_price = tp_price
            exit_date = r["date"]
            break

    now_utc = datetime.now(timezone.utc)
    try:
        buy_dt = datetime.fromisoformat(str(trade.get("buy_date")).replace("Z", "+00:00"))
    except Exception:
        buy_dt = now_utc
    if exit_reason is None and _biz_days(buy_dt, now_utc) >= holding_days:
        exit_reason = "expired"

    qty = int(trade.get("quantity") or 100)
    pnl_pct = (exit_price / buy - 1.0) * 100.0 if exit_price else None
    pnl = (exit_price - buy) * qty if exit_price else None
    update = {
        "max_return_pct": round(max_return, 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "exit_checked_at": now_utc.isoformat(),
    }
    if exit_reason:
        update.update({
            "sell_price": exit_price,
            "sell_date": exit_date,
            "sell_reason": exit_reason,
            "exit_reason": exit_reason,
            "profit_loss": round(pnl, 0) if pnl is not None else None,
            "profit_loss_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
            "status": "closed",
        })
    return update


def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas and yfinance are required")
    sb = _build_supabase()
    rows = sb.table("virtual_trades").select("*").eq("status", "open").execute().data or []
    logger.info("open virtual trades=%d", len(rows))
    checked = closed = skipped = errors = 0
    for trade in rows:
        if _is_non_japanese(trade):
            logger.info("skip non-japanese virtual trade: %s market=%s", trade.get("code"), trade.get("market"))
            skipped += 1
            continue
        try:
            update = evaluate_trade(
                trade,
                take_profit=float(args.take_profit),
                stop_loss=float(args.stop_loss),
                holding_days=int(args.holding_days),
            )
            if not update:
                skipped += 1
                continue
            checked += 1
            if update.get("status") == "closed":
                closed += 1
            logger.info(
                "%svirtual trade: %s update status=%s reason=%s max_return=%s max_dd=%s",
                "DRYRUN " if args.dry_run else "",
                trade.get("code"),
                update.get("status", "open"),
                update.get("exit_reason"),
                update.get("max_return_pct"),
                update.get("max_drawdown_pct"),
            )
            if not args.dry_run:
                sb.table("virtual_trades").update(update).eq("id", trade["id"]).execute()
        except Exception as e:
            errors += 1
            logger.exception("virtual trade check failed id=%s code=%s: %s", trade.get("id"), trade.get("code"), e)
    logger.info("complete: checked=%d closed=%d skipped=%d errors=%d", checked, closed, skipped, errors)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check virtual trades")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--take-profit", type=float, default=5.0)
    parser.add_argument("--stop-loss", type=float, default=-4.0)
    parser.add_argument("--holding-days", type=int, default=5)
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
