"""Shared close rules for virtual_trades.

This module only evaluates and updates virtual trade exits. It does not create
entries or change signal-stage decisions.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from services.h5_primary import H5_PRIMARY_CASE_KEY, H5_PRIMARY_RULES

logger = logging.getLogger(__name__)

# exit_reason values added after the DB sell_reason CHECK constraint was frozen.
# sell_reason must use the legacy value that the constraint allows.
_LEGACY_SELL_REASON: dict[str, str] = {
    "close_stop_loss_4pct": "stop_loss_4pct",
    "gap_down_stop_loss": "stop_loss_4pct",
}

DEFAULT_EXIT_SETTINGS = {
    "virtual_exit_pullback_pct": 2.0,
    "virtual_exit_rsi_level": 75.0,
    "virtual_exit_rsi_pullback_pct": 1.0,
    "virtual_exit_stop_loss_pct": 4.0,
    "virtual_exit_ma5_failure_pct": 2.0,
    "virtual_exit_holding_days": 5,
    "virtual_exit_extend_high_update_days": 2,
}

try:
    import pandas as pd
    import yfinance as yf

    HAS_PRICE_DEPS = True
    _YF_CACHE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".yfinance_cache")
    os.makedirs(_YF_CACHE, exist_ok=True)
    yf.set_tz_cache_location(_YF_CACHE)
except Exception:  # pragma: no cover
    pd = None
    yf = None
    HAS_PRICE_DEPS = False


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _exit_settings(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = dict(DEFAULT_EXIT_SETTINGS)
    if settings is None:
        try:
            from settings_loader import get_settings

            settings = get_settings(force_reload=True)
        except Exception as e:
            logger.warning("virtual exit settings load failed; using defaults: %s", e)
            settings = {}
    for key, default in DEFAULT_EXIT_SETTINGS.items():
        value = settings.get(key) if isinstance(settings, dict) else None
        if key.endswith("_days"):
            cfg[key] = max(0, int(_to_float(value, default) or default))
        else:
            cfg[key] = max(0.0, float(_to_float(value, default) or default))
    return cfg


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _ticker(code: str, market: str | None = None) -> str:
    code = str(code or "").strip()
    market_l = str(market or "").strip().lower()
    if code.isalpha() or market_l in {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}:
        return code
    return f"{code}.T"


def is_non_japanese_trade(row: dict) -> bool:
    code = str(row.get("code") or "").strip()
    market = str(row.get("market") or "").strip().lower()
    return (bool(code) and code.isalpha()) or market in {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}


def fetch_price_rows_since_entry(trade: dict, *, pre_days: int = 35) -> list[dict]:
    if not HAS_PRICE_DEPS:
        raise RuntimeError("pandas and yfinance are required")
    buy_dt = _parse_dt(trade.get("buy_date"))
    if not buy_dt:
        return []
    start = (buy_dt.date() - timedelta(days=pre_days)).isoformat()
    end = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
    hist = yf.Ticker(_ticker(str(trade.get("code") or ""), trade.get("market"))).history(
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,
    )
    if hist is None or hist.empty:
        return []
    rows: list[dict] = []
    for idx, r in hist.iterrows():
        d = pd.Timestamp(idx).tz_localize(None).date()
        rows.append({
            "date": d.isoformat(),
            "open": _to_float(r.get("Open")),
            "high": _to_float(r.get("High")),
            "low": _to_float(r.get("Low")),
            "close": _to_float(r.get("Close")),
            "volume": _to_float(r.get("Volume")),
        })
    return rows


def fetch_snapshot_price_rows_since_entry(sb, trade: dict, *, pre_days: int = 35) -> list[dict]:
    """Fetch daily close rows from stock_feature_snapshots.

    This is the preferred source for Japan-market virtual exits because the
    rebound pipeline already imports the official close into this table. It
    also avoids yfinance occasionally returning today's row with Close=null.
    """
    buy_dt = _parse_dt(trade.get("buy_date"))
    code = str(trade.get("code") or "").replace(".T", "").strip()
    if not buy_dt or not code:
        return []
    start = (buy_dt.date() - timedelta(days=pre_days)).isoformat()
    try:
        rows = (
            sb.table("stock_feature_snapshots")
            .select("trade_date,open,high,low,close,volume")
            .eq("code", code)
            .gte("trade_date", start)
            .order("trade_date")
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.warning("snapshot price rows fetch failed code=%s: %s", code, e)
        return []
    out: list[dict] = []
    for row in rows:
        close = _to_float(row.get("close"))
        if close is None or close <= 0:
            continue
        out.append({
            "date": str(row.get("trade_date")),
            "open": _to_float(row.get("open")),
            "high": _to_float(row.get("high")),
            "low": _to_float(row.get("low")),
            "close": close,
            "volume": _to_float(row.get("volume")),
        })
    return out


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in zip(closes[-period - 1 : -1], closes[-period:]):
        diff = cur - prev
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _biz_days_between(rows: list[dict], buy_date: str, current_date: str) -> int:
    dates = [str(r.get("date")) for r in rows if r.get("date")]
    return sum(1 for d in dates if buy_date < d <= current_date)


def _trading_days_since(rows: list[dict], start_date: str, current_date: str) -> int | None:
    dates = [str(r.get("date")) for r in rows if r.get("date")]
    if start_date not in dates or current_date not in dates:
        return None
    return dates.index(current_date) - dates.index(start_date)


@dataclass
class ExitEvaluation:
    update: dict[str, Any]
    exit_reason: str | None = None
    dry_log: dict[str, Any] | None = None


def _is_h5_primary_trade(trade: dict) -> bool:
    return bool(trade.get("is_primary_h5")) or str(trade.get("case_key") or "") == H5_PRIMARY_CASE_KEY


def evaluate_h5_primary_exit(
    trade: dict,
    price_rows: list[dict],
    *,
    now: datetime | None = None,
) -> ExitEvaluation | None:
    """Apply the deployed H5 Primary exit without changing legacy open trades."""
    buy = _to_float(trade.get("buy_price"))
    buy_dt = _parse_dt(trade.get("buy_date"))
    if buy is None or buy <= 0 or not buy_dt or not price_rows:
        return None
    now_utc = now or datetime.now(timezone.utc)
    buy_date = buy_dt.date().isoformat()
    future_rows = [
        row for row in price_rows
        if str(row.get("date") or "") > buy_date and _to_float(row.get("close")) is not None
    ]
    if not future_rows:
        return ExitEvaluation(
            update={"exit_checked_at": now_utc.isoformat(), "updated_at": now_utc.isoformat()},
            dry_log={"case_key": H5_PRIMARY_CASE_KEY, "message": "no post-entry close yet"},
        )

    peak_pullback_pct = _to_float(trade.get("peak_pullback_pct"), H5_PRIMARY_RULES["peak_pullback_pct"])
    initial_sl_pct = _to_float(trade.get("initial_sl_pct"), H5_PRIMARY_RULES["initial_sl_pct"])
    max_holding_days = int(_to_float(trade.get("max_holding_days"), H5_PRIMARY_RULES["max_holding_days"]) or 3)
    use_sl = initial_sl_pct is not None and initial_sl_pct > -0.49
    stop_price = buy * (1.0 + initial_sl_pct) if use_sl else None
    min_peak_ratio = 1.005
    peak_price = _to_float(trade.get("peak_price"), buy) or buy
    peak_price_at = trade.get("peak_price_at") or buy_dt.isoformat()
    qty = int(trade.get("quantity") or 100)
    max_return_pct = _to_float(trade.get("max_return_pct"), None)
    max_drawdown_pct = _to_float(trade.get("max_drawdown_pct"), None)
    exit_reason = None
    exit_mode = None
    exit_price = None
    exit_date = None
    exit_trigger_value = None

    for day_number, row in enumerate(future_rows, start=1):
        date = str(row.get("date"))
        close = _to_float(row.get("close"))
        high = _to_float(row.get("high"), close)
        low = _to_float(row.get("low"), close)
        if close is None:
            continue
        close_ret = (close / buy - 1.0) * 100.0
        max_return_pct = close_ret if max_return_pct is None else max(max_return_pct, close_ret)
        max_drawdown_pct = close_ret if max_drawdown_pct is None else min(max_drawdown_pct, close_ret)
        if high is not None and high > peak_price:
            peak_price = high
            peak_price_at = date

        if use_sl and stop_price is not None and low is not None and low <= stop_price:
            exit_reason = "emergency_stop_12pct"
            exit_mode = "h5_emergency_stop"
            exit_price = stop_price
            exit_trigger_value = initial_sl_pct * 100.0 if initial_sl_pct is not None else None
        elif peak_price > buy * min_peak_ratio and close <= peak_price * (1.0 + float(peak_pullback_pct or -0.02)):
            exit_reason = "peak_pullback_exit"
            exit_mode = "h5_peak_pullback"
            exit_price = close
            exit_trigger_value = (close / peak_price - 1.0) * 100.0
        elif day_number >= max_holding_days:
            exit_reason = "h5_timeout"
            exit_mode = "h5_timeout"
            exit_price = close
            exit_trigger_value = float(day_number)

        if exit_reason:
            exit_date = date
            break

    latest_close = _to_float(future_rows[-1].get("close"))
    update: dict[str, Any] = {
        "peak_price": round(peak_price, 4),
        "peak_price_at": peak_price_at,
        "highest_close": round(peak_price, 4),
        "highest_close_at": peak_price_at,
        "last_high_update_at": peak_price_at,
        "current_price": latest_close,
        "max_return_pct": round(max_return_pct or 0.0, 2),
        "max_drawdown_pct": round(max_drawdown_pct or 0.0, 2),
        "exit_checked_at": now_utc.isoformat(),
        "updated_at": now_utc.isoformat(),
    }
    if latest_close is not None:
        update["unrealized_pnl"] = round((latest_close - buy) * qty, 0)
        update["unrealized_pnl_pct"] = round((latest_close / buy - 1.0) * 100.0, 2)
    dry_log = {
        "case_key": H5_PRIMARY_CASE_KEY,
        "peak_price": round(peak_price, 4),
        "peak_pullback_pct": peak_pullback_pct,
        "initial_sl_pct": initial_sl_pct,
        "stop_price": round(stop_price, 4) if stop_price is not None else None,
        "max_holding_days": max_holding_days,
    }
    if exit_reason and exit_price is not None and exit_date:
        pnl_pct = (exit_price / buy - 1.0) * 100.0
        pnl = (exit_price - buy) * qty
        sell_reason = {
            "peak_pullback_exit": "take_profit",
            "emergency_stop_12pct": "stop_loss",
            "h5_timeout": "expired",
        }[exit_reason]
        update.update({
            "sell_price": round(exit_price, 4),
            "sell_date": exit_date,
            "sell_reason": sell_reason,
            "exit_reason": exit_reason,
            "exit_mode": exit_mode,
            "exit_trigger_value": round(exit_trigger_value, 4) if exit_trigger_value is not None else None,
            "profit_loss": round(pnl, 0),
            "profit_loss_pct": round(pnl_pct, 2),
            "virtual_exit_price": round(exit_price, 4),
            "virtual_pnl_pct": round(pnl_pct, 2),
            "status": "closed",
        })
        dry_log.update({"exit_reason": exit_reason, "sell_price": round(exit_price, 4), "profit_loss_pct": round(pnl_pct, 2)})
    return ExitEvaluation(update=update, exit_reason=exit_reason, dry_log=dry_log)


def evaluate_virtual_trade_exit(
    trade: dict,
    price_rows: list[dict] | None = None,
    *,
    holding_days: int | None = None,
    settings: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> ExitEvaluation | None:
    buy = _to_float(trade.get("buy_price"))
    buy_dt = _parse_dt(trade.get("buy_date"))
    if buy is None or buy <= 0 or not buy_dt:
        return None
    rows = price_rows if price_rows is not None else fetch_price_rows_since_entry(trade)
    if not rows:
        return None
    if _is_h5_primary_trade(trade):
        return evaluate_h5_primary_exit(trade, rows, now=now)

    exit_cfg = _exit_settings(settings)
    pullback_pct = float(exit_cfg["virtual_exit_pullback_pct"])
    rsi_level = float(exit_cfg["virtual_exit_rsi_level"])
    rsi_pullback_pct = float(exit_cfg["virtual_exit_rsi_pullback_pct"])
    stop_loss_pct = float(exit_cfg["virtual_exit_stop_loss_pct"])
    ma5_failure_pct = float(exit_cfg["virtual_exit_ma5_failure_pct"])
    effective_holding_days = int(holding_days if holding_days is not None else exit_cfg["virtual_exit_holding_days"])
    extend_high_update_days = int(exit_cfg["virtual_exit_extend_high_update_days"])

    now_utc = now or datetime.now(timezone.utc)
    buy_date = buy_dt.date().isoformat()
    pre_rows = [r for r in rows if str(r.get("date")) < buy_date and _to_float(r.get("close")) is not None]
    eval_rows = [r for r in rows if str(r.get("date")) >= buy_date and _to_float(r.get("close")) is not None]
    if not eval_rows:
        return None

    qty = int(trade.get("quantity") or 100)
    highest_close = _to_float(trade.get("highest_close"), buy) or buy
    highest_close_at = str(trade.get("highest_close_at") or buy_dt.isoformat())
    last_high_update_at = str(trade.get("last_high_update_at") or highest_close_at)
    rsi75_touched = _to_bool(trade.get("rsi75_touched"))
    rsi75_touched_at = trade.get("rsi75_touched_at")
    ma5_recovered = _to_bool(trade.get("ma5_recovered"))
    ma5_recovered_at = trade.get("ma5_recovered_at")

    closes_so_far: list[float] = [_to_float(r.get("close")) or 0.0 for r in pre_rows]
    prev_close: float | None = None
    exit_reason: str | None = None
    exit_mode: str | None = None
    exit_trigger_value: float | None = None
    exit_price: float | None = None
    exit_date: str | None = None
    last_ma5_diff: float | None = None
    last_daily_return: float | None = None
    last_open_price: float | None = None
    stop_loss_price = buy * (1.0 - stop_loss_pct / 100.0)

    max_return_pct = _to_float(trade.get("max_return_pct"), None)
    max_drawdown_pct = _to_float(trade.get("max_drawdown_pct"), None)

    for row in eval_rows:
        open_price = _to_float(row.get("open"))
        close = _to_float(row.get("close"))
        if close is None or close <= 0:
            continue
        d = str(row.get("date"))
        last_open_price = open_price
        closes_so_far.append(close)

        pnl_pct = (close / buy - 1.0) * 100.0
        max_return_pct = pnl_pct if max_return_pct is None else max(max_return_pct, pnl_pct)
        max_drawdown_pct = pnl_pct if max_drawdown_pct is None else min(max_drawdown_pct, pnl_pct)

        if close > highest_close:
            highest_close = close
            highest_close_at = d
            last_high_update_at = d

        rsi = _rsi(closes_so_far)
        if not rsi75_touched and rsi is not None and rsi >= rsi_level:
            rsi75_touched = True
            rsi75_touched_at = d

        ma5_diff_pct: float | None = None
        if len(closes_so_far) >= 5:
            ma5 = sum(closes_so_far[-5:]) / 5.0
            if ma5:
                ma5_diff_pct = (close - ma5) / ma5 * 100.0
                last_ma5_diff = ma5_diff_pct
                if not ma5_recovered and ma5_diff_pct >= 0:
                    ma5_recovered = True
                    ma5_recovered_at = d
                    logger.info("[ma5_recovery] code=%s ma5_diff_pct=%.2f", trade.get("code"), ma5_diff_pct)

        daily_return_pct: float | None = None
        if prev_close and prev_close > 0:
            daily_return_pct = (close - prev_close) / prev_close * 100.0
            last_daily_return = daily_return_pct

        if d == buy_date:
            prev_close = close
            continue

        if open_price is not None and open_price > 0 and open_price <= stop_loss_price:
            # Separate gap-down stops from close-based stops so real-order
            # slippage can be analyzed instead of hiding it in one bucket.
            exit_reason = "gap_down_stop_loss"
            exit_mode = "gap_down_stop_loss"
            exit_trigger_value = (open_price / buy - 1.0) * 100.0
            exit_price = open_price
        elif pnl_pct <= -stop_loss_pct:
            exit_reason = "close_stop_loss_4pct"
            exit_mode = "close_stop_loss"
            exit_trigger_value = pnl_pct
        elif ma5_recovered and close < buy and ma5_diff_pct is not None and ma5_diff_pct <= -ma5_failure_pct:
            exit_reason = "ma5_failed_recovery"
            exit_mode = "ma5_failure"
            exit_trigger_value = ma5_diff_pct
        elif rsi75_touched and close > buy and daily_return_pct is not None and daily_return_pct <= -rsi_pullback_pct:
            exit_reason = "rsi75_pullback1"
            exit_mode = "rsi75_pullback1"
            exit_trigger_value = daily_return_pct
        elif (not rsi75_touched) and close > buy and daily_return_pct is not None and daily_return_pct <= -pullback_pct:
            exit_reason = "pullback2"
            exit_mode = "pullback2"
            exit_trigger_value = daily_return_pct
        else:
            biz_days = _biz_days_between(eval_rows, buy_date, d)
            if biz_days >= effective_holding_days:
                high_days_ago = _trading_days_since(eval_rows, str(last_high_update_at)[:10], d)
                extend = close > buy and high_days_ago is not None and high_days_ago <= extend_high_update_days
                if extend:
                    logger.info(
                        "[holding_extended] code=%s holding_days=%d last_high_update_at=%s highest_close=%.4f",
                        trade.get("code"),
                        biz_days,
                        str(last_high_update_at)[:10],
                        highest_close,
                    )
                else:
                    exit_reason = "holding_timeout"
                    exit_mode = "timeout"

        if exit_reason:
            if exit_price is None:
                exit_price = close
            exit_date = d
            break
        prev_close = close

    latest_close = _to_float(eval_rows[-1].get("close"))
    update: dict[str, Any] = {
        "highest_close": round(highest_close, 4),
        "highest_close_at": highest_close_at,
        "last_high_update_at": last_high_update_at,
        "rsi75_touched": rsi75_touched,
        "rsi75_touched_at": rsi75_touched_at,
        "ma5_recovered": ma5_recovered,
        "ma5_recovered_at": ma5_recovered_at,
        "max_return_pct": round(max_return_pct or 0.0, 2),
        "max_drawdown_pct": round(max_drawdown_pct or 0.0, 2),
        "current_price": latest_close,
        "exit_checked_at": now_utc.isoformat(),
        "updated_at": now_utc.isoformat(),
    }
    if latest_close:
        update["unrealized_pnl"] = round((latest_close - buy) * qty, 0)
        update["unrealized_pnl_pct"] = round((latest_close / buy - 1.0) * 100.0, 2)

    dry_log = {
        "rsi75_touched": rsi75_touched,
        "ma5_recovered": ma5_recovered,
        "highest_close": round(highest_close, 4),
        "daily_return_pct": round(last_daily_return, 2) if last_daily_return is not None else None,
        "ma5_diff_pct": round(last_ma5_diff, 2) if last_ma5_diff is not None else None,
        "open_price": round(last_open_price, 4) if last_open_price is not None else None,
        "stop_loss_price": round(stop_loss_price, 4),
        "exit_settings": {
            "pullback_pct": pullback_pct,
            "rsi_level": rsi_level,
            "rsi_pullback_pct": rsi_pullback_pct,
            "stop_loss_pct": stop_loss_pct,
            "ma5_failure_pct": ma5_failure_pct,
            "holding_days": effective_holding_days,
            "extend_high_update_days": extend_high_update_days,
        },
    }

    if exit_reason and exit_price is not None and exit_date:
        pnl_pct = (exit_price / buy - 1.0) * 100.0
        pnl = (exit_price - buy) * qty
        update.update({
            "sell_price": round(exit_price, 4),
            "sell_date": exit_date,
            "sell_reason": _LEGACY_SELL_REASON.get(exit_reason, exit_reason),
            "exit_reason": exit_reason,
            "exit_mode": exit_mode,
            "exit_trigger_value": round(exit_trigger_value, 4) if exit_trigger_value is not None else None,
            "profit_loss": round(pnl, 0),
            "profit_loss_pct": round(pnl_pct, 2),
            "status": "closed",
        })
        dry_log.update({
            "sell_price": round(exit_price, 4),
            "profit_loss_pct": round(pnl_pct, 2),
            "exit_trigger_value": round(exit_trigger_value, 4) if exit_trigger_value is not None else None,
        })
    return ExitEvaluation(update=update, exit_reason=exit_reason, dry_log=dry_log)


def close_related_watchlist(sb, trade: dict, exit_reason: str, *, dry_run: bool) -> None:
    now = datetime.now(timezone.utc).isoformat()
    update = {
        "status": "closed",
        "closed_at": now,
        "close_reason": exit_reason,
        "signal_status_reason": f"virtual_trade_closed:{exit_reason}",
        "updated_at": now,
    }

    def _fetch_by_query(q):
        try:
            rows = q.limit(1).execute().data or []
            return rows[0] if rows else None
        except Exception as e:
            logger.warning("watchlist lookup failed trade=%s: %s", trade.get("id"), e)
            return None

    row = None
    if trade.get("watchlist_id"):
        row = _fetch_by_query(
            sb.table("stock_drop_watchlist")
            .select("id,code,status")
            .eq("id", trade.get("watchlist_id"))
        )
    if not row and trade.get("feature_snapshot_id"):
        row = _fetch_by_query(
            sb.table("stock_drop_watchlist")
            .select("id,code,status")
            .eq("feature_snapshot_id", trade.get("feature_snapshot_id"))
            .in_("status", ["rebound_signal", "entered", "watching", "rebound_candidate", "signal_skipped"])
            .order("updated_at", desc=True)
        )
    if not row and trade.get("code"):
        row = _fetch_by_query(
            sb.table("stock_drop_watchlist")
            .select("id,code,status")
            .eq("code", trade.get("code"))
            .in_("status", ["rebound_signal", "entered", "watching", "rebound_candidate", "signal_skipped"])
            .order("updated_at", desc=True)
        )

    if not row:
        logger.info("watchlist close skipped: no related row trade_id=%s code=%s", trade.get("id"), trade.get("code"))
        return
    if dry_run:
        logger.info("DRYRUN watchlist close: id=%s update=%s", row.get("id"), update)
        return
    sb.table("stock_drop_watchlist").update(update).eq("id", row["id"]).execute()
    logger.info(
        "[signal_lifecycle] code=%s watchlist_id=%s status %s -> closed reason=%s trade_id=%s",
        row.get("code") or trade.get("code"),
        row.get("id"),
        row.get("status"),
        exit_reason,
        trade.get("id"),
    )
