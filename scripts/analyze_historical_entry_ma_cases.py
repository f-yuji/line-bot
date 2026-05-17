#!/usr/bin/env python3
"""Analyze historical trade-case entries by entry-time MA gap buckets.

Research-only / read-only:
- Reuses services.trade_case_tester.run_trade_case_test_readonly().
- Reads stock_feature_snapshots only for entry-time features.
- Writes CSV files under outputs/historical_entry_ma_cases.
- Never writes to Supabase, virtual_trades, or trade_case_* tables.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from services.research_database import build_supabase
from services.trade_case_tester import run_trade_case_test_readonly

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "historical_entry_ma_cases"

SCENARIOS = {
    "2020_covid_crash": ("2020-02-20", "2020-04-30"),
    "2022_rate_hike_bear": ("2022-01-01", "2022-12-31"),
    "2023_rebound": ("2023-01-01", "2023-12-31"),
    "2024_ai_bubble": ("2024-01-01", "2024-12-31"),
    "2025_ai_bubble": ("2025-01-01", "2025-12-31"),
    "custom_recent": ("2026-02-09", "2026-05-10"),
}

DEFAULT_CASE_KEYS = [
    "combo_current__pullback2__margin_le20",
    "combo_current__ma5__margin_le20",
    "combo_current__rsi70__margin_le5",
    "combo_current__fixed10",
]

MA_COLUMNS = ("ma5_gap_pct", "ma25_gap_pct", "ma75_gap_pct")


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:10]).date()


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


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


def _snapshot_select_cols() -> list[str]:
    return [
        "id", "trade_date", "code", "name", "sector", "close", "drop_pct",
        "day_change_pct", "ma5", "ma25", "ma75", "ma5_gap_pct",
        "ma25_gap_pct", "ma75_gap_pct", "rsi14", "volume_ratio_20d",
        "margin_ratio", "nikkei_change_pct", "topix_change_pct",
    ]


def _snapshot_map_for_entries(sb, sims_by_case: dict[str, list[dict]]) -> dict[tuple[str, str], dict]:
    cols = [
        * _snapshot_select_cols(),
    ]
    by_date: dict[str, set[str]] = defaultdict(set)
    for sims in sims_by_case.values():
        for sim in sims:
            if sim.get("code") and sim.get("entry_date"):
                by_date[str(sim.get("entry_date"))].add(str(sim.get("code")))

    out: dict[tuple[str, str], dict] = {}
    for trade_date, codes in sorted(by_date.items()):
        code_list = sorted(codes)
        for i in range(0, len(code_list), 200):
            batch = code_list[i : i + 200]
            rows = (
                sb.table("stock_feature_snapshots")
                .select(",".join(cols))
                .eq("trade_date", trade_date)
                .in_("code", batch)
                .execute()
                .data
                or []
            )
            for r in rows:
                out[(str(r.get("code")), str(r.get("trade_date")))] = r
    return out


def _regime_map(sb, start: str, end: str) -> dict[str, str]:
    """Load market_regime.mode by trade_date.

    Older snapshot rows do not always carry regime fields, so this script uses
    the dedicated market_regime table as the source of truth when available.
    """
    try:
        rows = (
            sb.table("market_regime")
            .select("trade_date,mode")
            .gte("trade_date", start)
            .lte("trade_date", end)
            .order("trade_date")
            .execute()
            .data
            or []
        )
    except Exception as e:
        logging.getLogger(__name__).warning("[historical_entry_ma] market_regime load failed: %s", e)
        return {}
    return {str(r.get("trade_date")): str(r.get("mode") or "unknown") for r in rows if r.get("trade_date")}


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


def _entry_case(snap: dict) -> str:
    ma5 = _to_float(snap.get("ma5_gap_pct"))
    drop = _to_float(snap.get("drop_pct"))
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


def _derive_regime_from_snapshot(snap: dict) -> str:
    """Fallback regime classification for historical rows without market_regime.

    This is analysis-only. It mirrors the broad market-regime thresholds and
    adds a mild risk_on bucket so the CSV can separate normal from supportive
    markets even when the market_regime table has no historical row.
    """
    nikkei = _to_float(snap.get("nikkei_change_pct"))
    topix = _to_float(snap.get("topix_change_pct"))
    if nikkei is None and topix is None:
        return "unknown"
    if (nikkei is not None and nikkei <= -5.0) or (topix is not None and topix <= -4.0):
        return "panic_selloff"
    if (nikkei is not None and nikkei >= 5.0) or (topix is not None and topix >= 4.0):
        return "panic_rebound"
    if (nikkei is not None and nikkei <= -2.0) or (topix is not None and topix <= -1.5):
        return "risk_off"
    if (nikkei is not None and nikkei >= 2.0) or (topix is not None and topix >= 1.5):
        return "strong_risk_on"
    if (nikkei is not None and nikkei >= 0.75) or (topix is not None and topix >= 0.5):
        return "risk_on"
    return "normal"


def _profit_factor(pcts: list[float]) -> float | None:
    wins = sum(p for p in pcts if p > 0)
    losses = sum(p for p in pcts if p < 0)
    if losses == 0:
        return None
    return wins / abs(losses)


def _summary_note(rows: list[dict], label: str) -> str:
    n = len(rows)
    pcts = [_to_float(r.get("profit_pct"), 0.0) or 0.0 for r in rows]
    avg = sum(pcts) / n if n else 0
    win_rate = len([p for p in pcts if p > 0]) / n * 100 if n else 0
    reasons: dict[str, int] = defaultdict(int)
    for r in rows:
        reasons[str(r.get("exit_reason") or "unknown")] += 1
    top_reason = max(reasons.items(), key=lambda x: x[1])[0] if reasons else "unknown"
    if n < 20:
        return f"{label}: sample small; top_exit={top_reason}"
    if avg > 0 and win_rate >= 50:
        return f"{label}: strong/steady; top_exit={top_reason}"
    if avg > 0:
        return f"{label}: positive expectancy but uneven; top_exit={top_reason}"
    if win_rate < 35:
        return f"{label}: weak win rate; top_exit={top_reason}"
    return f"{label}: low expectancy; top_exit={top_reason}"


def _summarize(rows: list[dict], group_fields: list[str]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[tuple(r.get(f) for f in group_fields)].append(r)

    out: list[dict] = []
    for key, items in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        pcts = [_to_float(r.get("profit_pct"), 0.0) or 0.0 for r in items if r.get("status") == "closed"]
        yens = [_to_float(r.get("profit_yen"), 0.0) or 0.0 for r in items if r.get("status") == "closed"]
        holds = [
            int(r.get("holding_days"))
            for r in items
            if r.get("holding_days") is not None and str(r.get("holding_days")).lstrip("-").isdigit()
        ]
        wins = [p for p in pcts if p > 0]
        row = {field: value for field, value in zip(group_fields, key)}
        n = len(pcts)
        pf = _profit_factor(pcts)
        row.update({
            "trades": len(items),
            "closed_trades": n,
            "win_rate": round(len(wins) / n * 100, 1) if n else None,
            "avg_profit_pct": round(sum(pcts) / n, 3) if n else None,
            "median_profit_pct": round(median(pcts), 3) if pcts else None,
            "expectancy_pct": round(sum(pcts) / n, 3) if n else None,
            "profit_factor": round(pf, 3) if pf is not None else None,
            "total_profit_yen": round(sum(yens), 0),
            "avg_holding_days": round(sum(holds) / len(holds), 1) if holds else None,
            "best_trade_pct": round(max(pcts), 3) if pcts else None,
            "worst_trade_pct": round(min(pcts), 3) if pcts else None,
            "max_drawdown_pct": round(min(pcts), 3) if pcts else None,
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
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _scenario_names(value: str) -> list[str]:
    if value == "all":
        return list(SCENARIOS)
    if value not in SCENARIOS:
        raise SystemExit(f"unknown scenario: {value}")
    return [value]


def _case_keys(value: str) -> list[str] | None:
    if value == "all":
        return None
    if value == "default":
        return list(DEFAULT_CASE_KEYS)
    return [v.strip() for v in value.split(",") if v.strip()]


def run(args: argparse.Namespace) -> None:
    sb = build_supabase()
    scenarios = _scenario_names(args.scenario)
    case_keys = _case_keys(args.case)
    detail_rows: list[dict] = []

    for scenario in scenarios:
        start, end = SCENARIOS[scenario]
        print(f"[historical_entry_ma] scenario={scenario} start={start} end={end} case={args.case}")
        regimes = _regime_map(sb, start, end)
        cases, sims_by_case, _results = run_trade_case_test_readonly(start, end, case_keys=case_keys, sb=sb)
        snapshots = _snapshot_map_for_entries(sb, sims_by_case)
        case_name = {str(c.get("case_key") or c.get("id")): c.get("case_name") for c in cases}

        for case_key, sims in sims_by_case.items():
            print(f"[historical_entry_ma] loaded case={case_key} trades={len(sims)}")
            for sim in sims:
                snap = snapshots.get((str(sim.get("code")), str(sim.get("entry_date"))))
                if not snap:
                    continue
                entry_date = str(sim.get("entry_date"))
                market_regime = sim.get("market_regime") or regimes.get(entry_date) or _derive_regime_from_snapshot(snap)
                row = {
                    "scenario": scenario,
                    "case_key": case_key,
                    "case_name": case_name.get(case_key),
                    "code": sim.get("code"),
                    "name": sim.get("name"),
                    "entry_date": entry_date,
                    "exit_date": sim.get("exit_date"),
                    "status": sim.get("status"),
                    "exit_reason": sim.get("exit_reason"),
                    "entry_price": sim.get("entry_price"),
                    "exit_price": sim.get("exit_price"),
                    "profit_pct": sim.get("profit_pct"),
                    "profit_yen": sim.get("profit_yen"),
                    "holding_days": sim.get("holding_days"),
                    "signal_stage": sim.get("signal_stage"),
                    "signal_probability": sim.get("signal_probability"),
                    "expected_value": sim.get("expected_value"),
                    "entry_case": _entry_case(snap),
                    "entry_drop_pct": snap.get("drop_pct"),
                    "entry_drop_bucket": _drop_bucket(_to_float(snap.get("drop_pct"))),
                    "entry_rsi14": snap.get("rsi14"),
                    "entry_volume_ratio_20d": snap.get("volume_ratio_20d"),
                    "entry_margin_ratio": sim.get("margin_ratio") or snap.get("margin_ratio"),
                    "entry_market_regime": market_regime,
                    "entry_nikkei_change_pct": snap.get("nikkei_change_pct"),
                    "entry_topix_change_pct": snap.get("topix_change_pct"),
                }
                for ma_col in MA_COLUMNS:
                    metric = ma_col.replace("_gap_pct", "")
                    value = _to_float(snap.get(ma_col))
                    row[f"entry_{ma_col}"] = snap.get(ma_col)
                    row[f"{metric}_side"] = "upper" if value is not None and value >= 0 else "lower" if value is not None else "unknown"
                    row[f"{metric}_bucket"] = _gap_bucket(value)
                detail_rows.append(row)

    case_summary = _summarize(detail_rows, ["scenario", "case_key", "entry_case", "entry_drop_bucket"])
    ma_summary: list[dict] = []
    for metric in ("ma5", "ma25", "ma75"):
        temp = []
        for row in detail_rows:
            r = dict(row)
            r["ma_metric"] = metric
            r["ma_side"] = row.get(f"{metric}_side")
            r["ma_bucket"] = row.get(f"{metric}_bucket")
            temp.append(r)
        ma_summary.extend(_summarize(temp, ["scenario", "case_key", "ma_metric", "ma_side", "ma_bucket"]))
    exit_summary = _summarize(detail_rows, ["scenario", "case_key", "entry_case", "exit_reason"])
    regime_case_summary = _summarize(
        detail_rows,
        ["scenario", "case_key", "entry_market_regime", "entry_case"],
    )
    regime_ma_summary: list[dict] = []
    for metric in ("ma5", "ma25", "ma75"):
        temp = []
        for row in detail_rows:
            r = dict(row)
            r["ma_metric"] = metric
            r["ma_side"] = row.get(f"{metric}_side")
            r["ma_bucket"] = row.get(f"{metric}_bucket")
            temp.append(r)
        regime_ma_summary.extend(
            _summarize(temp, ["scenario", "case_key", "entry_market_regime", "ma_metric", "ma_side", "ma_bucket"])
        )

    _write_csv(OUT_DIR / "historical_entry_ma_trade_detail.csv", detail_rows)
    _write_csv(OUT_DIR / "historical_entry_case_summary.csv", case_summary)
    _write_csv(OUT_DIR / "historical_entry_ma_bucket_summary.csv", ma_summary)
    _write_csv(OUT_DIR / "historical_entry_case_exit_summary.csv", exit_summary)
    _write_csv(OUT_DIR / "historical_entry_case_regime_summary.csv", regime_case_summary)
    _write_csv(OUT_DIR / "historical_ma_bucket_regime_summary.csv", regime_ma_summary)

    print(f"[historical_entry_ma] saved detail rows={len(detail_rows)}")
    print(f"[historical_entry_ma] output={OUT_DIR}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Historical case entry MA bucket analysis")
    p.add_argument("--scenario", default="all", help="all or scenario name")
    p.add_argument("--case", default="default", help="default, all, or comma-separated case_key list")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
