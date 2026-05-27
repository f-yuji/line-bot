#!/usr/bin/env python3
"""Verify market_regime table coverage and classification for key historical periods.

Outputs:
  outputs/rebound_grid_search/regime_coverage_report.csv
  outputs/rebound_grid_search/regime_coverage_summary.txt
"""
import argparse
import csv
import logging
import os
import sys
from collections import Counter
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PERIODS = [
    {"name": "2020_covid_crash",    "start": "2020-02-01", "end": "2020-04-30", "expected_panic": True},
    {"name": "2022_rate_hike_bear", "start": "2022-01-01", "end": "2022-12-31", "expected_panic": False},
    {"name": "2023_rebound",        "start": "2023-01-01", "end": "2023-12-31", "expected_panic": False},
    {"name": "2024_ai_bubble",      "start": "2024-01-01", "end": "2024-12-31", "expected_panic": False},
    {"name": "2025_shock",          "start": "2025-01-01", "end": "2025-12-31", "expected_panic": True},
    {"name": "full_range",          "start": "2020-01-01", "end": "2026-05-26", "expected_panic": True},
]

PANIC_MODES = {"panic_selloff", "panic", "shock"}

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "outputs", "rebound_grid_search"
)


def _build_supabase():
    mode = os.getenv("SUPABASE_MODE", "").strip() or os.getenv("ENV", "").strip()
    mode_upper = mode.upper() if mode else ""
    url = (os.getenv(f"SUPABASE_URL_{mode_upper}", "").strip() if mode_upper else "") or os.getenv("SUPABASE_URL", "").strip()
    key = (os.getenv(f"SUPABASE_KEY_{mode_upper}", "").strip() if mode_upper else "") or os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY not set")
    return create_client(url, key)


def _fetch_all(sb, start_s: str, end_s: str) -> list:
    rows, offset = [], 0
    while True:
        data = (
            sb.table("market_regime")
            .select("trade_date,mode,nikkei_change_pct,topix_change_pct,nikkei_ma25_gap")
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .order("trade_date")
            .range(offset, offset + 999)
            .execute()
            .data or []
        )
        rows.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return rows


def _calendar_days(start_s: str, end_s: str) -> int:
    d0 = date.fromisoformat(start_s)
    d1 = date.fromisoformat(end_s)
    return max((d1 - d0).days + 1, 1)


def _safe_mean(values: list) -> float | None:
    valid = [v for v in values if v is not None]
    return round(sum(valid) / len(valid), 4) if valid else None


def _fmt_pct(val) -> str:
    return f"{val:.1f}%" if val is not None else "N/A"


def analyze_period(period: dict, rows: list) -> dict:
    total_days = len(rows)
    cal_days = _calendar_days(period["start"], period["end"])
    expected_trading = int(cal_days * 0.7)
    missing_days = max(expected_trading - total_days, 0)

    mode_counts = Counter(r["mode"] for r in rows)
    mode_pct = {m: round(c / total_days * 100, 1) if total_days else 0.0 for m, c in mode_counts.items()}

    nikkei_vals = [r["nikkei_change_pct"] for r in rows]
    ma25_vals   = [r["nikkei_ma25_gap"]    for r in rows]
    avg_nikkei_pct    = _safe_mean(nikkei_vals)
    avg_nikkei_ma25_gap = _safe_mean(ma25_vals)

    valid_nikkei = [(r["trade_date"], r["nikkei_change_pct"]) for r in rows if r["nikkei_change_pct"] is not None]
    best_day  = max(valid_nikkei, key=lambda x: x[1])[0] if valid_nikkei else None
    worst_day = min(valid_nikkei, key=lambda x: x[1])[0] if valid_nikkei else None

    panic_days = sum(1 for r in rows if r["mode"] in PANIC_MODES)
    has_panic  = panic_days > 0
    coverage_ok = (panic_days > 0) if period["expected_panic"] else True

    return {
        "name":                 period["name"],
        "start":                period["start"],
        "end":                  period["end"],
        "expected_panic":       period["expected_panic"],
        "total_days":           total_days,
        "expected_trading":     expected_trading,
        "missing_days":         missing_days,
        "mode_counts":          dict(mode_counts),
        "mode_pct":             mode_pct,
        "avg_nikkei_pct":       avg_nikkei_pct,
        "avg_nikkei_ma25_gap":  avg_nikkei_ma25_gap,
        "best_day":             best_day,
        "worst_day":            worst_day,
        "panic_days":           panic_days,
        "has_panic":            has_panic,
        "coverage_ok":          coverage_ok,
    }


def write_csv(results: list, path: str) -> None:
    fieldnames = [
        "name", "start", "end", "expected_panic",
        "total_days", "expected_trading", "missing_days",
        "panic_days", "has_panic", "coverage_ok",
        "avg_nikkei_pct", "avg_nikkei_ma25_gap",
        "best_day", "worst_day", "mode_counts", "mode_pct",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in results:
            row = dict(r)
            row["mode_counts"] = str(r["mode_counts"])
            row["mode_pct"]    = str(r["mode_pct"])
            w.writerow(row)


def write_summary(results: list, path: str, cli_start: str, cli_end: str) -> str:
    failed = [r for r in results if not r["coverage_ok"]]
    if not failed:
        overall = "OK"
    elif any(r["name"] in ("2020_covid_crash", "full_range") for r in failed):
        overall = "CRITICAL"
    else:
        overall = "WARNING"

    covid = next((r for r in results if r["name"] == "2020_covid_crash"), None)
    covid_ok = covid["has_panic"] if covid else False

    lines = [
        "=" * 64,
        f"Market Regime Coverage Report  ({cli_start} ~ {cli_end})",
        "=" * 64,
        f"Overall status : {overall}",
        f"2020 COVID panic classified : {'YES' if covid_ok else 'NO -- CHECK REQUIRED'}",
        "",
        f"{'Period':<24} {'Days':>5} {'Missing':>7} {'Panic':>6} {'CovOK':>6}  Mode breakdown",
        "-" * 80,
    ]
    for r in results:
        mode_str = "  ".join(f"{m}:{c}" for m, c in sorted(r["mode_counts"].items(), key=lambda x: -x[1]))
        lines.append(
            f"{r['name']:<24} {r['total_days']:>5} {r['missing_days']:>7} "
            f"{r['panic_days']:>6} {'OK' if r['coverage_ok'] else 'NG':>6}  {mode_str}"
        )

    lines += [
        "",
        "Recommendation:",
    ]
    if overall == "OK":
        lines.append("  Ready for grid_search")
    else:
        lines.append("  Run backfill_market_regime.py first")

    text = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


def main():
    parser = argparse.ArgumentParser(description="Check market_regime coverage")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end",   default="2026-05-26")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sb = _build_supabase()

    results = []
    for period in PERIODS:
        logger.info("Fetching period: %s (%s ~ %s)", period["name"], period["start"], period["end"])
        rows = _fetch_all(sb, period["start"], period["end"])
        result = analyze_period(period, rows)
        results.append(result)
        logger.info(
            "  %s: %d days, %d panic, coverage_ok=%s",
            result["name"], result["total_days"], result["panic_days"], result["coverage_ok"]
        )

    csv_path = os.path.join(OUTPUT_DIR, "regime_coverage_report.csv")
    txt_path = os.path.join(OUTPUT_DIR, "regime_coverage_summary.txt")

    write_csv(results, csv_path)
    summary = write_summary(results, txt_path, args.start, args.end)

    logger.info("\n%s", summary)
    logger.info("CSV  -> %s", csv_path)
    logger.info("TXT  -> %s", txt_path)


if __name__ == "__main__":
    main()
