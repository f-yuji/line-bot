#!/usr/bin/env python3
"""Audit weekly margin gaps needed by case-mix scenarios.

Read-only by default. With --import-missing, fetches only missing code/date
ranges from J-Quants and upserts stock_weekly_margin_interest.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from scripts.import_jquants_margin import _upsert, _weekly_row
from jquants_client import get_weekly_margin_interest

load_dotenv()

SCENARIOS = {
    "2020_covid_crash": (date(2020, 2, 20), date(2020, 4, 30)),
    "2022_rate_hike_bear": (date(2022, 1, 1), date(2022, 12, 31)),
}
OUTPUT_DIR = Path("outputs/case_mix")


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


def _fetch_all_by_offset(query_factory, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        data = query_factory().range(offset, offset + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            return rows
        offset += page_size


def _fetch_snapshots_by_ids(sb, ids: list[int], *, batch_size: int = 500) -> list[dict]:
    rows: list[dict] = []
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        data = (
            sb.table("stock_feature_snapshots")
            .select("id,code,trade_date,is_drop_candidate,is_tradeable")
            .in_("id", batch)
            .execute()
            .data or []
        )
        rows.extend(data)
    return rows


def _candidate_code_dates(sb, start: date, end: date) -> dict[str, list[date]]:
    start_s = start.isoformat()
    end_s = end.isoformat()

    def label_query():
        return (
            sb.table("stock_rebound_labels")
            .select("feature_snapshot_id,trade_date,code,future_high_5d,future_low_5d")
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("trade_date")
        )

    labels = _fetch_all_by_offset(label_query)
    ids = [int(r["feature_snapshot_id"]) for r in labels if r.get("feature_snapshot_id")]
    snapshots = _fetch_snapshots_by_ids(sb, ids)
    valid_ids = {
        str(r.get("id"))
        for r in snapshots
        if r.get("is_drop_candidate") and r.get("is_tradeable")
    }
    by_code: dict[str, set[date]] = defaultdict(set)
    for row in labels:
        if str(row.get("feature_snapshot_id")) not in valid_ids:
            continue
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        try:
            by_code[code].add(datetime.fromisoformat(str(row.get("trade_date"))).date())
        except Exception:
            continue
    return {code: sorted(days) for code, days in by_code.items()}


def _margin_dates_for_codes(sb, codes: list[str], start: date, end: date, *, batch_size: int = 50, page_size: int = 1000) -> dict[str, list[date]]:
    by_code: dict[str, list[date]] = defaultdict(list)
    start_s, end_s = start.isoformat(), end.isoformat()
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        offset = 0
        while True:
            rows = (
                sb.table("stock_weekly_margin_interest")
                .select("code,date")
                .in_("code", batch)
                .gte("date", start_s)
                .lte("date", end_s)
                .order("date")
                .range(offset, offset + page_size - 1)
                .execute()
                .data or []
            )
            for row in rows:
                try:
                    by_code[str(row.get("code"))].append(datetime.fromisoformat(str(row.get("date"))).date())
                except Exception:
                    continue
            if len(rows) < page_size:
                break
            offset += page_size
    return by_code


def _has_prior_margin(margin_dates: list[date], trade_date: date, lookback_days: int) -> bool:
    cutoff = trade_date - timedelta(days=lookback_days)
    return any(cutoff <= d <= trade_date for d in margin_dates)


def _audit_scenario(sb, scenario: str, start: date, end: date, lookback_days: int) -> list[dict[str, Any]]:
    code_dates = _candidate_code_dates(sb, start, end)
    if not code_dates:
        return []
    margin_by_code = _margin_dates_for_codes(sb, sorted(code_dates), start - timedelta(days=lookback_days), end)
    rows: list[dict[str, Any]] = []
    for code, trade_dates in sorted(code_dates.items()):
        margin_dates = margin_by_code.get(code, [])
        missing_dates = [d for d in trade_dates if not _has_prior_margin(margin_dates, d, lookback_days)]
        if not missing_dates:
            continue
        rows.append({
            "scenario": scenario,
            "code": code,
            "candidate_trade_dates": len(trade_dates),
            "missing_trade_dates": len(missing_dates),
            "first_missing_trade_date": missing_dates[0].isoformat(),
            "last_missing_trade_date": missing_dates[-1].isoformat(),
            "fetch_start": max(start - timedelta(days=lookback_days), missing_dates[0] - timedelta(days=lookback_days)).isoformat(),
            "fetch_end": missing_dates[-1].isoformat(),
            "reason": "no weekly margin row within lookback window",
        })
    return rows


def _import_rows(sb, missing_rows: list[dict[str, Any]], *, sleep_sec: float, batch_size: int) -> int:
    saved = 0
    for idx, row in enumerate(missing_rows, 1):
        code = str(row["code"])
        start = datetime.fromisoformat(str(row["fetch_start"])).date()
        end = datetime.fromisoformat(str(row["fetch_end"])).date()
        raw_rows = get_weekly_margin_interest(code=code, from_date=start, to_date=end)
        mapped = [r for r in (_weekly_row(raw) for raw in raw_rows) if r]
        if mapped:
            saved += _upsert(sb, "stock_weekly_margin_interest", mapped, "code,date", batch_size)
        print(f"[missing_margin] import {idx}/{len(missing_rows)} code={code} rows={len(mapped)} saved={saved}")
        time.sleep(sleep_sec)
    return saved


def _dedupe_margin_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("code") or ""), str(row.get("date") or ""))
        if key[0] and key[1]:
            deduped[key] = row
    return list(deduped.values())


def _fridays(start: date, end: date) -> list[date]:
    current = start
    while current.weekday() != 4:
        current += timedelta(days=1)
    days: list[date] = []
    while current <= end:
        days.append(current)
        current += timedelta(days=7)
    return days


def _fetch_weekly_date_with_retry(target: date, *, retries: int = 6) -> list[dict]:
    for attempt in range(1, retries + 1):
        try:
            return get_weekly_margin_interest(date=target)
        except Exception as e:
            msg = str(e)
            if "429" not in msg and "Max retries exceeded" not in msg and "too many" not in msg:
                raise
            wait = min(180.0, 20.0 * attempt)
            print(f"[missing_margin] rate limited date={target} attempt={attempt}/{retries} sleep={wait:.0f}s")
            time.sleep(wait)
    return get_weekly_margin_interest(date=target)


def _import_by_weekly_dates(
    sb,
    selected: dict[str, tuple[date, date]],
    missing_rows: list[dict[str, Any]],
    *,
    lookback_days: int,
    sleep_sec: float,
    batch_size: int,
) -> int:
    dates: set[date] = set()
    if missing_rows:
        for row in missing_rows:
            try:
                start = datetime.fromisoformat(str(row["fetch_start"])).date()
                end = datetime.fromisoformat(str(row["fetch_end"])).date()
            except Exception:
                continue
            dates.update(_fridays(start, end))
    else:
        for start, end in selected.values():
            dates.update(_fridays(start - timedelta(days=lookback_days), end))
    saved = 0
    for idx, target in enumerate(sorted(dates), 1):
        raw_rows = _fetch_weekly_date_with_retry(target)
        mapped = _dedupe_margin_rows([r for r in (_weekly_row(raw) for raw in raw_rows) if r])
        if mapped:
            saved += _upsert(sb, "stock_weekly_margin_interest", mapped, "code,date", batch_size)
        print(f"[missing_margin] import-date {idx}/{len(dates)} date={target} rows={len(mapped)} saved={saved}")
        time.sleep(sleep_sec)
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit/import missing weekly margin rows for case mix")
    parser.add_argument("--scenario", choices=[*SCENARIOS.keys(), "all"], default="all")
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--import-missing", action="store_true")
    parser.add_argument("--import-by-date", action="store_true", help="fetch all weekly margin rows by Friday dates in scenario windows")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--sleep-sec", type=float, default=0.2)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    sb = _build_supabase()
    selected = SCENARIOS if args.scenario == "all" else {args.scenario: SCENARIOS[args.scenario]}
    missing: list[dict[str, Any]] = []
    for scenario, (start, end) in selected.items():
        print(f"[missing_margin] audit scenario={scenario} start={start} end={end}")
        missing.extend(_audit_scenario(sb, scenario, start, end, int(args.lookback_days)))
    if args.limit:
        missing = missing[: int(args.limit)]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "missing_weekly_margin_for_case_mix.csv"
    cols = [
        "scenario",
        "code",
        "candidate_trade_dates",
        "missing_trade_dates",
        "first_missing_trade_date",
        "last_missing_trade_date",
        "fetch_start",
        "fetch_end",
        "reason",
    ]
    with out.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(missing)
    print(f"[missing_margin] saved {out} rows={len(missing)}")

    if args.import_by_date:
        saved = _import_by_weekly_dates(
            sb,
            selected,
            missing,
            lookback_days=int(args.lookback_days),
            sleep_sec=float(args.sleep_sec),
            batch_size=int(args.batch_size),
        )
        print(f"[missing_margin] import-by-date complete saved={saved}")
    elif args.import_missing and missing:
        saved = _import_rows(sb, missing, sleep_sec=float(args.sleep_sec), batch_size=int(args.batch_size))
        print(f"[missing_margin] import complete saved={saved}")


if __name__ == "__main__":
    main()
