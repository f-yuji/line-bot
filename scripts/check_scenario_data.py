#!/usr/bin/env python3
"""Check DB data coverage for each backtest scenario period.

Queries stock_feature_snapshots and stock_rebound_labels for each scenario
and reports counts, label coverage, and future data availability.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SCENARIOS = [
    {"name": "custom_recent",      "type": "custom",  "start": "2026-02-09", "end": "2026-05-10"},
    {"name": "2025_ai_bubble",     "type": "bull",    "start": "2025-01-01", "end": "2025-12-31"},
    {"name": "2024_ai_bubble",     "type": "bull",    "start": "2024-01-01", "end": "2024-12-31"},
    {"name": "2023_rebound",       "type": "rebound", "start": "2023-01-01", "end": "2023-12-31"},
    {"name": "2022_rate_hike_bear","type": "bear",    "start": "2022-01-01", "end": "2022-12-31"},
    {"name": "2020_covid_crash",   "type": "panic",   "start": "2020-02-20", "end": "2020-04-30"},
]

COL_W = {
    "name":        24,
    "period":      26,
    "snap":         8,
    "labels":       8,
    "lbl_5d":      10,
    "lbl_20d":     10,
    "lbl_miss":    10,
    "snap_drop":   10,
    "snap_trade":  11,
}


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


def _count(sb, table: str, start: str, end: str, extra_filters: list | None = None, *, exact: bool = False) -> int:
    count_mode = "exact" if exact else "planned"
    q = (
        sb.table(table)
        .select("id", count=count_mode)
        .gte("trade_date", start)
        .lte("trade_date", end)
    )
    for col, op, val in (extra_filters or []):
        if op == "eq":
            q = q.eq(col, val)
        elif op == "not_is_null":
            q = q.not_.is_(col, "null")
    res = q.execute()
    return res.count or 0


def _exists(sb, table: str, start: str, end: str, extra_filters: list | None = None) -> bool:
    q = (
        sb.table(table)
        .select("id")
        .gte("trade_date", start)
        .lte("trade_date", end)
        .limit(1)
    )
    for col, op, val in (extra_filters or []):
        if op == "eq":
            q = q.eq(col, val)
        elif op == "not_is_null":
            q = q.not_.is_(col, "null")
    return bool((q.execute().data or []))


def _date_range_actual(sb, table: str, start: str, end: str) -> tuple[str | None, str | None, list[str]]:
    """Return (min_date, max_date, list_of_distinct_dates_sampled) within the period."""
    rows_min = (
        sb.table(table)
        .select("trade_date")
        .gte("trade_date", start)
        .lte("trade_date", end)
        .order("trade_date", desc=False)
        .limit(1)
        .execute()
        .data or []
    )
    rows_max = (
        sb.table(table)
        .select("trade_date")
        .gte("trade_date", start)
        .lte("trade_date", end)
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    min_d = str(rows_min[0]["trade_date"]) if rows_min else None
    max_d = str(rows_max[0]["trade_date"]) if rows_max else None
    return min_d, max_d


def _check_scenario(sb, sc: dict) -> dict:
    name = sc["name"]
    start = sc["start"]
    end = sc["end"]

    # snapshots: planned estimate only (trade_date index only — filtered counts timeout)
    snap_total = _count(sb, "stock_feature_snapshots", start, end)
    snap_min, snap_max = _date_range_actual(sb, "stock_feature_snapshots", start, end)[:2]

    # labels: exact count — labels table is much smaller
    lbl_total = _count(sb, "stock_rebound_labels", start, end, exact=True)
    lbl_5d = _count(sb, "stock_rebound_labels", start, end,
                    [("future_high_5d", "not_is_null", None), ("future_low_5d", "not_is_null", None)],
                    exact=True)
    lbl_20d = _count(sb, "stock_rebound_labels", start, end,
                     [("future_high_20d", "not_is_null", None), ("future_low_20d", "not_is_null", None)],
                     exact=True)
    lbl_min, lbl_max = _date_range_actual(sb, "stock_rebound_labels", start, end)[:2]

    lbl_miss_rate = (
        round((lbl_total - lbl_5d) / lbl_total * 100, 1) if lbl_total > 0 else None
    )

    return {
        "name": name,
        "start": start,
        "end": end,
        "snap_total": snap_total,
        "snap_min": snap_min,
        "snap_max": snap_max,
        "lbl_total": lbl_total,
        "lbl_5d": lbl_5d,
        "lbl_20d": lbl_20d,
        "lbl_miss_rate": lbl_miss_rate,
        "lbl_min": lbl_min,
        "lbl_max": lbl_max,
    }


def _status(r: dict) -> str:
    if r["snap_total"] == 0:
        return "NO_SNAP"
    if r["lbl_total"] == 0:
        return "NO_LABELS"
    if r["lbl_5d"] == 0:
        return "NO_FUTURE_5D"
    if r["lbl_miss_rate"] is not None and r["lbl_miss_rate"] > 50:
        return f"SPARSE_5D({r['lbl_miss_rate']}%miss)"
    return "OK"


def _table_row(cells: list, widths: list[int]) -> str:
    parts = [str(c if c is not None else "-").ljust(w)[:w] for c, w in zip(cells, widths)]
    return "| " + " | ".join(parts) + " |"


def _table_hr(widths: list[int]) -> str:
    return "+-" + "-+-".join("-" * w for w in widths) + "-+"


def run() -> None:
    sb = _build_supabase()

    print()
    print("=== Scenario Data Coverage Check ===")
    print(f"Run at: {datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M JST')}")
    print("(snapshot counts are PostgreSQL planner estimates; label counts are exact)")
    print()

    results = []
    for sc in SCENARIOS:
        print(f"  checking {sc['name']} ...", flush=True)
        r = _check_scenario(sb, sc)
        results.append(r)

    # ── stock_feature_snapshots ─────────────────────────────────────────────
    print()
    print("## stock_feature_snapshots")
    sw = [24, 26, 10, 12, 12]
    print(_table_hr(sw))
    print(_table_row(["scenario", "期間(指定)", "総件数(est)", "実データ最小日", "実データ最大日"], sw))
    print(_table_hr(sw))
    for r in results:
        print(_table_row([
            r["name"],
            f"{r['start']}~{r['end']}",
            r["snap_total"],
            r["snap_min"] or "---",
            r["snap_max"] or "---",
        ], sw))
    print(_table_hr(sw))

    # ── stock_rebound_labels ─────────────────────────────────────────────────
    print()
    print("## stock_rebound_labels")
    lw = [24, 8, 8, 8, 12, 12, 8, 20]
    print(_table_hr(lw))
    print(_table_row(["scenario", "総件数", "5d完備", "20d完備", "最小日", "最大日", "5d欠損率", "ステータス"], lw))
    print(_table_hr(lw))
    for r in results:
        miss = f"{r['lbl_miss_rate']}%" if r["lbl_miss_rate"] is not None else "-"
        print(_table_row([
            r["name"],
            r["lbl_total"],
            r["lbl_5d"],
            r["lbl_20d"],
            r["lbl_min"] or "---",
            r["lbl_max"] or "---",
            miss,
            _status(r),
        ], lw))
    print(_table_hr(lw))

    # ── サマリー ──────────────────────────────────────────────────────────────
    print()
    print("## Summary")
    ok = [r for r in results if _status(r) == "OK"]
    ng = [r for r in results if _status(r) != "OK"]
    print(f"  OK ({len(ok)}): {', '.join(r['name'] for r in ok) or 'none'}")
    print(f"  NG ({len(ng)}): {', '.join(r['name'] for r in ng) or 'none'}")
    print()
    if ng:
        print("## 対応が必要なシナリオ")
        for r in ng:
            st = _status(r)
            if st == "NO_SNAP":
                print(f"  {r['name']}: stock_feature_snapshots なし")
                print(f"    → python scripts/generate_feature_snapshots.py --start {r['start']} --end {r['end']}")
            elif st == "NO_LABELS":
                print(f"  {r['name']}: stock_rebound_labels なし")
                print(f"    → python scripts/generate_rebound_labels.py --start {r['start']} --end {r['end']}")
            elif st == "NO_FUTURE_5D":
                print(f"  {r['name']}: future_high/low_5d が0件")
                print(f"    → python scripts/generate_rebound_labels.py --start {r['start']} --end {r['end']} --force")
            else:
                print(f"  {r['name']}: {st} — ラベル再生成を検討")
                print(f"    → python scripts/generate_rebound_labels.py --start {r['start']} --end {r['end']} --force")
        print()


if __name__ == "__main__":
    run()
