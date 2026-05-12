"""Virtual trade performance aggregation (read-only).

Aggregates virtual_trades by daily / weekly / monthly period based on sell_date.
Never writes to or modifies virtual_trades.

Column mapping (actual virtual_trades schema):
  exit date : sell_date  (exit_date does not exist)
  entry date: buy_date
  pnl yen   : profit_loss
  pnl pct   : profit_loss_pct
  ai score  : entry_probability / buy_score
  holding   : computed from buy_date + sell_date (no stored column)
  cleanup   : exit_reason in {'cleanup_position_limit', 'cleanup_duplicate_open'}
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
CLEANUP_REASONS = {"cleanup_position_limit", "cleanup_duplicate_open"}


def _to_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _to_jst_date(v: Any) -> date | None:
    """Parse ISO timestamp (UTC or offset-aware) and return JST date."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST).date()
    except Exception:
        return None


def _is_open(row: dict) -> bool:
    return row.get("status") == "open" and not (row.get("sell_date") or row.get("exit_date"))


def _is_closed(row: dict) -> bool:
    return row.get("status") == "closed" or bool(row.get("sell_date") or row.get("exit_date"))


def _exit_date(row: dict) -> date | None:
    return _to_jst_date(row.get("sell_date") or row.get("exit_date"))


def _entry_date(row: dict) -> date | None:
    return _to_jst_date(row.get("buy_date") or row.get("entry_date"))


def _holding_days(row: dict) -> int | None:
    """Compute holding days from buy_date and sell_date."""
    stored = row.get("holding_days")
    if stored is not None:
        try:
            return int(stored)
        except Exception:
            pass
    ed = _exit_date(row)
    nd = _entry_date(row)
    if ed and nd:
        return max(0, (ed - nd).days)
    return None


def _exit_reason(row: dict) -> str | None:
    return row.get("exit_reason")


def _ai_score(row: dict) -> float | None:
    for col in ("entry_probability", "buy_score", "entry_score", "signal_probability", "ai_score"):
        v = _to_float(row.get(col), None)
        if v is not None:
            return v
    return None


def _period_key(d: date, period: str) -> tuple[date, date, str]:
    if period == "daily":
        return d, d, d.strftime("%Y-%m-%d")
    if period == "monthly":
        start = d.replace(day=1)
        if d.month == 12:
            end = d.replace(day=31)
        else:
            end = d.replace(month=d.month + 1, day=1) - timedelta(days=1)
        return start, end, d.strftime("%Y年%m月")
    # weekly (Mon-Sun)
    start = d - timedelta(days=d.weekday())
    end = start + timedelta(days=6)
    return start, end, f"{start.strftime('%m/%d')}〜{end.strftime('%m/%d')}"


def aggregate(all_rows: list[dict], period: str = "weekly") -> list[dict]:
    """Aggregate closed trades by period (sell_date/JST based).

    entries_count uses buy_date. All other metrics use sell_date.
    cleanup trades (position_limit, duplicate_open) are excluded.
    """
    closed = [
        r for r in all_rows
        if _is_closed(r) and _exit_reason(r) not in CLEANUP_REASONS
    ]

    buckets: dict[date, dict] = {}
    for row in closed:
        ed = _exit_date(row)
        if ed is None:
            continue
        ps, pe, label = _period_key(ed, period)
        if ps not in buckets:
            buckets[ps] = {"period_start": ps, "period_end": pe, "period_label": label, "_exits": []}
        buckets[ps]["_exits"].append(row)

    # entries_count: entry_date (buy_date) based, all trades including open
    entry_counts: dict[date, int] = {}
    for row in all_rows:
        nd = _entry_date(row)
        if nd is None:
            continue
        ps, _, _ = _period_key(nd, period)
        entry_counts[ps] = entry_counts.get(ps, 0) + 1

    results = []
    for ps in sorted(buckets):
        b = buckets[ps]
        exits = b["_exits"]
        profits = [p for p in (_to_float(r.get("profit_loss_pct"), None) for r in exits) if p is not None]
        pnl_yens = [_to_float(r.get("profit_loss"), 0) or 0 for r in exits]
        wins = [p for p in profits if p > 0]
        losses = [p for p in profits if p <= 0]
        holding = [h for h in (_holding_days(r) for r in exits) if h is not None]

        results.append({
            "period_label": b["period_label"],
            "period_start": b["period_start"].isoformat(),
            "period_end": b["period_end"].isoformat(),
            "entries_count": entry_counts.get(ps, 0),
            "exits_count": len(exits),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": round(len(wins) / len(profits) * 100, 1) if profits else None,
            "realized_pnl_yen": round(sum(pnl_yens), 0),
            "realized_pnl_pct_avg": round(sum(profits) / len(profits), 2) if profits else None,
            "avg_profit_pct": round(sum(wins) / len(wins), 2) if wins else None,
            "avg_loss_pct": round(sum(losses) / len(losses), 2) if losses else None,
            "max_profit_pct": round(max(profits), 2) if profits else None,
            "max_loss_pct": round(min(profits), 2) if profits else None,
            "avg_holding_days": round(sum(holding) / len(holding), 1) if holding else None,
            "open_positions_count_end": None,
            "open_unrealized_pnl_yen_end": None,
            "open_unrealized_pnl_pct_end": None,
        })

    logger.info("[virtual_trade_performance] period=%s rows=%d", period, len(results))
    return results


def open_summary(all_rows: list[dict]) -> dict:
    """Current open position summary for the top cards."""
    open_trades = [r for r in all_rows if _is_open(r)]
    cost_total = 0.0
    pnl_total = 0.0
    value_total = 0.0
    for r in open_trades:
        buy = _to_float(r.get("buy_price"), 0) or 0
        qty = int(r.get("quantity") or 100)
        cost = buy * qty
        cost_total += cost
        current = _to_float(r.get("current_price"), None)
        if current is not None and buy > 0:
            value = current * qty
            value_total += value
            pnl_total += value - cost
        else:
            # current_price未取得の場合はunrealized_pnlを使う
            stored_pnl = _to_float(r.get("unrealized_pnl"), None)
            if stored_pnl is not None:
                pnl_total += stored_pnl
            value_total += cost
    pnl_pct = (pnl_total / cost_total * 100) if cost_total > 0 else None
    return {
        "count": len(open_trades),
        "cost_total": cost_total,
        "value_total": value_total,
        "pnl_yen": round(pnl_total, 0),
        "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
    }


def top_card_summary(all_rows: list[dict]) -> dict:
    """Realized PnL for today / this week / this month (JST)."""
    today = datetime.now(JST).date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    closed = [
        r for r in all_rows
        if _is_closed(r) and _exit_reason(r) not in CLEANUP_REASONS
    ]

    def _sum(rows, since: date) -> float:
        return sum(
            _to_float(r.get("profit_loss"), 0) or 0
            for r in rows
            if (_exit_date(r) or date.min) >= since
        )

    return {
        "today_pnl": round(_sum(closed, today), 0),
        "week_pnl": round(_sum(closed, week_start), 0),
        "month_pnl": round(_sum(closed, month_start), 0),
    }


def detail_trades(all_rows: list[dict], period_start: str, period_end: str) -> list[dict]:
    """Trades closed within [period_start, period_end] for the detail page.

    Attaches computed holding_days and ai_score to each row.
    """
    ps = _to_jst_date(period_start)
    pe = _to_jst_date(period_end)
    if ps is None or pe is None:
        return []
    rows = []
    for r in all_rows:
        if not _is_closed(r):
            continue
        if _exit_reason(r) in CLEANUP_REASONS:
            continue
        ed = _exit_date(r)
        if ed is None or not (ps <= ed <= pe):
            continue
        row = dict(r)
        row["_holding_days"] = _holding_days(r)
        row["_ai_score"] = _ai_score(r)
        row["_exit_reason"] = _exit_reason(r)
        rows.append(row)
    rows.sort(key=lambda r: (_exit_date(r) or date.min), reverse=True)
    logger.info(
        "[virtual_trade_performance_detail] period_start=%s period_end=%s rows=%d",
        period_start, period_end, len(rows),
    )
    return rows
