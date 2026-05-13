#!/usr/bin/env python3
"""Write a rebound AI cron summary to research_import_logs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
JST = timezone(timedelta(hours=9))

try:
    from openai import OpenAI

    HAS_OPENAI = True
except Exception:
    OpenAI = None
    HAS_OPENAI = False


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


def _count(sb, table: str, **filters: Any) -> int:
    q = sb.table(table).select("id", count="exact").limit(1)
    for key, value in filters.items():
        if value is True:
            q = q.eq(key, True)
        elif value is False:
            q = q.eq(key, False)
        elif value is None:
            q = q.is_(key, "null")
        else:
            q = q.eq(key, value)
    return int(q.execute().count or 0)


def _latest_feature_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return str(rows[0].get("trade_date")) if rows else None


def _latest_market_regime(sb, latest_date: str | None) -> dict[str, Any] | None:
    q = (
        sb.table("market_regime")
        .select("trade_date,mode,shock_score,reason,nikkei_change_pct,topix_change_pct,decliners_ratio")
        .order("trade_date", desc=True)
        .limit(1)
    )
    if latest_date:
        q = q.lte("trade_date", latest_date)
    rows = q.execute().data or []
    return rows[0] if rows else None


def _fmt_yen(value: Any) -> str:
    try:
        amount = float(value or 0)
    except Exception:
        amount = 0.0
    sign = "+" if amount > 0 else ""
    return f"{sign}{amount:,.0f}円"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "-"
    try:
        amount = float(value)
    except Exception:
        return "-"
    sign = "+" if amount > 0 else ""
    return f"{sign}{amount:.2f}%"


def _trade_label(trade: dict[str, Any]) -> str:
    code = trade.get("code") or "-"
    name = trade.get("name") or ""
    return f"{name}（{code}）" if name else str(code)


def _day_bounds_utc() -> tuple[str, str, str]:
    target = datetime.now(JST).date()
    start = datetime(target.year, target.month, target.day, tzinfo=JST)
    end = start + timedelta(days=1)
    return target.isoformat(), start.astimezone(timezone.utc).isoformat(), end.astimezone(timezone.utc).isoformat()


def _sum_number(rows: list[dict[str, Any]], key: str) -> float:
    total = 0.0
    for row in rows:
        try:
            total += float(row.get(key) or 0)
        except Exception:
            continue
    return total


def _trade_activity(sb) -> dict[str, Any]:
    activity_date, start_utc, end_utc = _day_bounds_utc()
    cols = (
        "id,code,name,buy_price,buy_date,sell_price,sell_date,quantity,status,"
        "profit_loss,profit_loss_pct,unrealized_pnl,unrealized_pnl_pct,"
        "current_price,exit_reason,sell_reason,signal_stage,created_at,updated_at"
    )
    entries = (
        sb.table("virtual_trades")
        .select(cols)
        .gte("buy_date", start_utc)
        .lt("buy_date", end_utc)
        .order("buy_date", desc=True)
        .limit(30)
        .execute()
        .data
        or []
    )
    exits = (
        sb.table("virtual_trades")
        .select(cols)
        .gte("sell_date", start_utc)
        .lt("sell_date", end_utc)
        .order("sell_date", desc=True)
        .limit(30)
        .execute()
        .data
        or []
    )
    open_positions = (
        sb.table("virtual_trades")
        .select(cols)
        .eq("status", "open")
        .is_("sell_date", "null")
        .order("buy_date", desc=True)
        .limit(100)
        .execute()
        .data
        or []
    )
    return {
        "activity_date": activity_date,
        "entries": entries,
        "exits": exits,
        "open_positions": open_positions,
        "entry_count": len(entries),
        "exit_count": len(exits),
        "realized_pnl": _sum_number(exits, "profit_loss"),
        "unrealized_pnl": _sum_number(open_positions, "unrealized_pnl"),
    }


def _compact_trade(trade: dict[str, Any], *, side: str) -> dict[str, Any]:
    item = {
        "code": trade.get("code"),
        "name": trade.get("name"),
        "quantity": trade.get("quantity"),
        "status": trade.get("status"),
    }
    if side == "entry":
        item.update({
            "buy_price": trade.get("buy_price"),
            "buy_date": trade.get("buy_date"),
            "signal_stage": trade.get("signal_stage"),
        })
    elif side == "exit":
        item.update({
            "buy_price": trade.get("buy_price"),
            "sell_price": trade.get("sell_price"),
            "sell_date": trade.get("sell_date"),
            "profit_loss": trade.get("profit_loss"),
            "profit_loss_pct": trade.get("profit_loss_pct"),
            "exit_reason": trade.get("exit_reason") or trade.get("sell_reason"),
        })
    else:
        item.update({
            "buy_price": trade.get("buy_price"),
            "current_price": trade.get("current_price"),
            "unrealized_pnl": trade.get("unrealized_pnl"),
            "unrealized_pnl_pct": trade.get("unrealized_pnl_pct"),
        })
    return item


def _fallback_ai_summary(summary: dict[str, Any]) -> str:
    market = summary.get("market_regime") or {}
    activity = summary.get("trade_activity") or {}
    entries = activity.get("entries") or []
    exits = activity.get("exits") or []
    open_positions = activity.get("open_positions") or []
    entry_text = "、".join(_trade_label(t) for t in entries[:8]) or "なし"
    if exits:
        exit_text = "、".join(
            f"{_trade_label(t)} {_fmt_yen(t.get('profit_loss'))}（{_fmt_pct(t.get('profit_loss_pct'))}）"
            for t in exits[:8]
        )
    else:
        exit_text = "なし"
    return (
        f"{summary.get('latest_feature_date')}の市場は"
        f"地合い{market.get('mode') or '不明'}（{market.get('reason') or '詳細なし'}）。"
        f"{activity.get('activity_date')}の仮想売買は、買い{len(entries)}件: {entry_text}。"
        f"売り{len(exits)}件: {exit_text}。"
        f"本日の確定損益は{_fmt_yen(activity.get('realized_pnl'))}、"
        f"保有中{len(open_positions)}件の含み損益は{_fmt_yen(activity.get('unrealized_pnl'))}。"
    )


def _build_ai_summary(summary: dict[str, Any]) -> str:
    api_key = _opt("OPENAI_API_KEY")
    if not HAS_OPENAI or not api_key:
        return _fallback_ai_summary(summary)

    market = summary.get("market_regime") or {}
    trades = summary.get("recent_virtual_trades") or []
    activity = summary.get("trade_activity") or {}
    trade_lines = [
        {
            "code": t.get("code"),
            "name": t.get("name"),
            "stage": t.get("signal_stage"),
            "probability": t.get("entry_probability"),
            "expected_value": t.get("expected_value"),
        }
        for t in trades[:10]
    ]
    payload = {
        "latest_feature_date": summary.get("latest_feature_date"),
        "trade_activity_date": activity.get("activity_date"),
        "feature_snapshots": summary.get("feature_snapshots"),
        "drop_tradeable_candidates": summary.get("drop_tradeable_candidates"),
        "open_virtual_trades": summary.get("open_virtual_trades"),
        "market_regime": market,
        "today_entries": [_compact_trade(t, side="entry") for t in (activity.get("entries") or [])[:12]],
        "today_exits": [_compact_trade(t, side="exit") for t in (activity.get("exits") or [])[:12]],
        "today_realized_pnl": activity.get("realized_pnl"),
        "open_positions_unrealized_pnl": activity.get("unrealized_pnl"),
        "open_positions": [_compact_trade(t, side="open") for t in (activity.get("open_positions") or [])[:12]],
        "recent_virtual_trades": trade_lines,
    }
    prompt = (
        "急落リバウンドAIの1日運用ログを日本語で短くまとめてください。"
        "投資助言ではなく、研究・フォワード検証ログとして書くこと。"
        "必ず、買った銘柄、売った銘柄、確定損益、保有中の含み損益を含めること。"
        "売買がない項目は「なし」と書くこと。明日見るべき点を最後に1文だけ添え、全体は5文以内。"
    )
    try:
        client = OpenAI(api_key=api_key)
        res = client.chat.completions.create(
            model=os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
            ],
            temperature=0.2,
            max_tokens=360,
        )
        text = (res.choices[0].message.content or "").strip()
        return text or _fallback_ai_summary(summary)
    except Exception as e:
        summary["ai_summary_error"] = str(e)[:300]
        return _fallback_ai_summary(summary)


def build_summary(sb) -> dict[str, Any]:
    latest_date = _latest_feature_date(sb)
    snapshot_total = 0
    drop_tradeable = 0
    if latest_date:
        snapshot_total = _count(sb, "stock_feature_snapshots", trade_date=latest_date)
        drop_tradeable = (
            sb.table("stock_feature_snapshots")
            .select("id", count="exact")
            .eq("trade_date", latest_date)
            .eq("is_drop_candidate", True)
            .eq("is_tradeable", True)
            .limit(1)
            .execute()
            .count
            or 0
        )
    open_trades = (
        sb.table("virtual_trades")
        .select("id", count="exact")
        .eq("status", "open")
        .is_("sell_date", "null")
        .limit(1)
        .execute()
        .count
        or 0
    )
    recent_trades = (
        sb.table("virtual_trades")
        .select("code,name,signal_stage,entry_probability,expected_value,created_at")
        .order("created_at", desc=True)
        .limit(10)
        .execute()
        .data
        or []
    )
    summary = {
        "latest_feature_date": latest_date,
        "feature_snapshots": int(snapshot_total),
        "drop_tradeable_candidates": int(drop_tradeable),
        "open_virtual_trades": int(open_trades),
        "recent_virtual_trades": recent_trades,
        "trade_activity": _trade_activity(sb),
        "market_regime": _latest_market_regime(sb, latest_date),
    }
    summary["ai_summary"] = _build_ai_summary(summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Log rebound AI daily cron summary")
    parser.add_argument("--status", default="completed")
    parser.add_argument("--error-message")
    args = parser.parse_args()

    sb = _build_supabase()
    summary = build_summary(sb)
    status = args.status or "completed"
    if args.error_message and status == "completed":
        status = "failed"
    row = {
        "dataset_key": f"rebound_ai_daily:{summary.get('latest_feature_date') or 'unknown'}",
        "job_type": "rebound_ai_daily",
        "status": status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "rows_inserted": summary["open_virtual_trades"],
        "rows_updated": summary["feature_snapshots"],
        "rows_skipped": summary["drop_tradeable_candidates"],
        "error_message": args.error_message,
        "params": summary,
    }
    sb.table("research_import_logs").insert(row).execute()
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
