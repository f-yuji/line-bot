#!/usr/bin/env python3
"""Write a compact rebound AI cron summary to research_import_logs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

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


def _fallback_ai_summary(summary: dict[str, Any]) -> str:
    market = summary.get("market_regime") or {}
    trades = summary.get("recent_virtual_trades") or []
    trade_text = "、".join(str(t.get("code")) for t in trades[:5] if t.get("code")) or "なし"
    return (
        f"{summary.get('latest_feature_date')}は"
        f"特徴量{summary.get('feature_snapshots')}件、急落候補{summary.get('drop_tradeable_candidates')}件。"
        f"地合いは{market.get('mode') or '不明'}（{market.get('reason') or '詳細なし'}）。"
        f"直近の仮想売買候補は{trade_text}。"
    )


def _build_ai_summary(summary: dict[str, Any]) -> str:
    api_key = _opt("OPENAI_API_KEY")
    if not HAS_OPENAI or not api_key:
        return _fallback_ai_summary(summary)

    market = summary.get("market_regime") or {}
    trades = summary.get("recent_virtual_trades") or []
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
        "feature_snapshots": summary.get("feature_snapshots"),
        "drop_tradeable_candidates": summary.get("drop_tradeable_candidates"),
        "open_virtual_trades": summary.get("open_virtual_trades"),
        "market_regime": market,
        "recent_virtual_trades": trade_lines,
    }
    prompt = (
        "急落リバウンドAIの1日運用ログを日本語で短くまとめてください。"
        "投資助言ではなく、研究・フォワード検証ログとして書くこと。"
        "市場で何があったか、仮想売買で何をしたか、明日見るべき点を3文以内で。"
        "銘柄コードは必要に応じて列挙してください。"
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
            max_tokens=260,
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
