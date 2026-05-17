#!/usr/bin/env python3
"""Analyze closed virtual trades by entry-time MA gap buckets.

Read-only research script. It joins closed virtual_trades to the entry
stock_feature_snapshots and writes CSV files under outputs/entry_ma_buckets.
It never inserts, updates, or deletes database rows.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from services.research_database import build_supabase

load_dotenv()

JST = timezone(timedelta(hours=9))
OUT_DIR = Path("outputs/entry_ma_buckets")
CLEANUP_REASONS = {"cleanup_position_limit", "cleanup_duplicate_open"}
MA_COLUMNS = ("ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct")


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


def _to_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST).date()
    except Exception:
        try:
            return date.fromisoformat(str(value)[:10])
        except Exception:
            return None


def _fetch_all(query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        chunk = query.range(offset, offset + page_size - 1).execute().data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


def _load_closed_trades(sb, start: str | None, end: str | None) -> list[dict]:
    query = (
        sb.table("virtual_trades")
        .select("*")
        .eq("status", "closed")
        .order("sell_date", desc=False)
    )
    if start:
        query = query.gte("sell_date", start)
    if end:
        query = query.lte("sell_date", end)
    rows = _fetch_all(query)
    return [
        r for r in rows
        if (r.get("exit_reason") or r.get("sell_reason")) not in CLEANUP_REASONS
        and _to_float(r.get("profit_loss_pct")) is not None
    ]


def _load_snapshots_by_id(sb, ids: list[Any]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    clean_ids = sorted({int(x) for x in ids if str(x or "").isdigit()})
    for i in range(0, len(clean_ids), 200):
        batch = clean_ids[i : i + 200]
        rows = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .in_("id", batch)
            .execute()
            .data
            or []
        )
        for r in rows:
            out[str(r.get("id"))] = r
    return out


def _load_snapshot_fallback(sb, code: str, entry_date: date) -> dict | None:
    try:
        rows = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .eq("code", code)
            .lte("trade_date", entry_date.isoformat())
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception:
        return None


def _gap_bucket(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v < -15:
        return "<-15%"
    if v < -10:
        return "-15%..-10%"
    if v < -5:
        return "-10%..-5%"
    if v < 0:
        return "-5%..0%"
    if v < 5:
        return "0%..+5%"
    if v < 10:
        return "+5%..+10%"
    return ">=+10%"


def _drop_bucket(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v <= -12:
        return "<=-12%"
    if v <= -8:
        return "-12%..-8%"
    if v <= -5:
        return "-8%..-5%"
    if v <= -3:
        return "-5%..-3%"
    return ">-3%"


def _entry_case(row: dict) -> str:
    ma5 = _to_float(row.get("ma5_gap_pct"))
    drop = _to_float(row.get("drop_pct"))
    if ma5 is None:
        return "unknown_ma5"
    if ma5 >= 0:
        if drop is not None and -5 <= drop <= -3:
            return "ma5_upper_shallow_pullback"
        if drop is not None and drop < -5:
            return "ma5_upper_deep_drop"
        return "ma5_upper_other"
    if drop is not None and drop <= -8:
        return "ma5_lower_deep_rebound"
    if drop is not None and drop > -8:
        return "ma5_lower_shallow_weak"
    return "ma5_lower_other"


def _holding_days(trade: dict) -> int | None:
    b = _to_date(trade.get("buy_date"))
    s = _to_date(trade.get("sell_date") or trade.get("exit_date"))
    if b and s:
        return max(0, (s - b).days)
    return None


def _profit_factor(pcts: list[float]) -> float | None:
    wins = sum(p for p in pcts if p > 0)
    losses = sum(p for p in pcts if p < 0)
    if losses == 0:
        return None
    return wins / abs(losses)


def _summary_note(rows: list[dict], group_name: str) -> str:
    pcts = [_to_float(r.get("profit_loss_pct"), 0.0) or 0.0 for r in rows]
    n = len(rows)
    win_rate = len([p for p in pcts if p > 0]) / n * 100 if n else 0
    avg = sum(pcts) / n if n else 0
    reasons = defaultdict(int)
    for r in rows:
        reasons[str(r.get("exit_reason") or r.get("sell_reason") or "unknown")] += 1
    top_reason = max(reasons.items(), key=lambda x: x[1])[0] if reasons else "unknown"
    if n < 5:
        return f"{group_name}: sample small; top_exit={top_reason}"
    if avg > 0 and win_rate >= 50:
        return f"{group_name}: strong/steady; top_exit={top_reason}"
    if avg > 0:
        return f"{group_name}: positive expectancy but uneven; top_exit={top_reason}"
    if win_rate < 35:
        return f"{group_name}: weak win rate; top_exit={top_reason}"
    return f"{group_name}: low expectancy; top_exit={top_reason}"


def _summarize(rows: list[dict], group_fields: list[str]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        key = tuple(r.get(f) for f in group_fields)
        groups[key].append(r)

    out: list[dict] = []
    for key, items in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        pcts = [_to_float(r.get("profit_loss_pct"), 0.0) or 0.0 for r in items]
        pnl_yen = [_to_float(r.get("profit_loss"), 0.0) or 0.0 for r in items]
        holds = [h for h in (_holding_days(r) for r in items) if h is not None]
        drawdowns = [
            _to_float(r.get("max_drawdown_pct"), None)
            for r in items
            if _to_float(r.get("max_drawdown_pct"), None) is not None
        ]
        wins = [p for p in pcts if p > 0]
        row = {field: value for field, value in zip(group_fields, key)}
        n = len(items)
        row.update({
            "trades": n,
            "win_rate": round(len(wins) / n * 100, 1) if n else None,
            "avg_profit_loss_pct": round(sum(pcts) / n, 2) if n else None,
            "total_profit_loss_yen": round(sum(pnl_yen), 0),
            "profit_factor": round(_profit_factor(pcts), 2) if _profit_factor(pcts) is not None else None,
            "avg_holding_days": round(sum(holds) / len(holds), 1) if holds else None,
            "max_drawdown_pct": round(min(drawdowns), 2) if drawdowns else round(min(pcts), 2) if pcts else None,
            "expectancy_pct": round(sum(pcts) / n, 2) if n else None,
            "best_trade_pct": round(max(pcts), 2) if pcts else None,
            "worst_trade_pct": round(min(pcts), 2) if pcts else None,
            "notes": _summary_note(items, " / ".join(str(x) for x in key)),
        })
        out.append(row)
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fields: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> None:
    print("[entry_ma_buckets] loading closed virtual_trades...")
    sb = build_supabase()
    trades = _load_closed_trades(sb, args.start, args.end)
    print(f"[entry_ma_buckets] closed trades={len(trades)}")

    snapshots = _load_snapshots_by_id(sb, [t.get("feature_snapshot_id") for t in trades])
    enriched: list[dict] = []
    missing = 0
    for t in trades:
        snap = snapshots.get(str(t.get("feature_snapshot_id")))
        entry_date = _to_date(t.get("buy_date"))
        if not snap and entry_date and t.get("code"):
            snap = _load_snapshot_fallback(sb, str(t.get("code")), entry_date)
        if not snap:
            missing += 1
            continue
        row = dict(t)
        for col in (
            "trade_date", "drop_pct", "day_change_pct", "ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct",
            "rsi14", "volume_ratio_20d", "margin_ratio", "nikkei_change_pct", "topix_change_pct",
            "market_regime", "market_regime_reason", "sector", "close", "ma5", "ma25", "ma75",
        ):
            row[f"entry_{col}"] = snap.get(col)
        row["entry_case"] = _entry_case(snap)
        row["entry_drop_bucket"] = _drop_bucket(_to_float(snap.get("drop_pct")))
        for ma_col in MA_COLUMNS:
            row[f"entry_{ma_col}_bucket"] = _gap_bucket(_to_float(snap.get(ma_col)))
            row[f"entry_{ma_col}_side"] = "upper" if (_to_float(snap.get(ma_col), -999) or -999) >= 0 else "lower"
        enriched.append(row)

    detail_rows = []
    for r in enriched:
        detail_rows.append({
            "trade_id": r.get("id"),
            "code": r.get("code"),
            "name": r.get("name"),
            "buy_date": r.get("buy_date"),
            "sell_date": r.get("sell_date") or r.get("exit_date"),
            "entry_trade_date": r.get("entry_trade_date"),
            "entry_case": r.get("entry_case"),
            "entry_drop_pct": r.get("entry_drop_pct"),
            "entry_drop_bucket": r.get("entry_drop_bucket"),
            "entry_ma5_gap_pct": r.get("entry_ma5_gap_pct"),
            "entry_ma5_gap_bucket": r.get("entry_ma5_gap_pct_bucket"),
            "entry_ma25_gap_pct": r.get("entry_ma25_gap_pct"),
            "entry_ma25_gap_bucket": r.get("entry_ma25_gap_pct_bucket"),
            "entry_ma75_gap_pct": r.get("entry_ma75_gap_pct"),
            "entry_ma75_gap_bucket": r.get("entry_ma75_gap_pct_bucket"),
            "entry_rsi14": r.get("entry_rsi14"),
            "entry_volume_ratio_20d": r.get("entry_volume_ratio_20d"),
            "entry_margin_ratio": r.get("entry_margin_ratio"),
            "buy_price": r.get("buy_price"),
            "sell_price": r.get("sell_price"),
            "profit_loss": r.get("profit_loss"),
            "profit_loss_pct": r.get("profit_loss_pct"),
            "max_drawdown_pct": r.get("max_drawdown_pct"),
            "exit_reason": r.get("exit_reason") or r.get("sell_reason"),
            "holding_days": _holding_days(r),
        })

    bucket_rows: list[dict] = []
    for ma_col in MA_COLUMNS:
        metric = ma_col.replace("_gap_pct", "")
        temp = []
        for r in enriched:
            rr = dict(r)
            rr["ma_metric"] = metric
            rr["ma_side"] = r.get(f"entry_{ma_col}_side")
            rr["ma_gap_bucket"] = r.get(f"entry_{ma_col}_bucket")
            temp.append(rr)
        bucket_rows.extend(_summarize(temp, ["ma_metric", "ma_side", "ma_gap_bucket"]))

    case_rows = _summarize(enriched, ["entry_case", "entry_drop_bucket"])
    exit_rows = _summarize(enriched, ["entry_case", "exit_reason"])

    _write_csv(OUT_DIR / "entry_ma_trade_detail.csv", detail_rows)
    _write_csv(OUT_DIR / "entry_ma_bucket_summary.csv", bucket_rows)
    _write_csv(OUT_DIR / "entry_case_summary.csv", case_rows)
    _write_csv(OUT_DIR / "entry_case_exit_reason_summary.csv", exit_rows)

    print(f"[entry_ma_buckets] enriched={len(enriched)} missing_snapshot={missing}")
    print(f"[entry_ma_buckets] saved {OUT_DIR / 'entry_ma_trade_detail.csv'}")
    print(f"[entry_ma_buckets] saved {OUT_DIR / 'entry_ma_bucket_summary.csv'}")
    print(f"[entry_ma_buckets] saved {OUT_DIR / 'entry_case_summary.csv'}")
    print(f"[entry_ma_buckets] saved {OUT_DIR / 'entry_case_exit_reason_summary.csv'}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze closed virtual trades by entry MA buckets")
    p.add_argument("--start", help="Filter sell_date >= YYYY-MM-DD")
    p.add_argument("--end", help="Filter sell_date <= YYYY-MM-DD")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
