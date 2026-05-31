"""Compare H5 Primary against the day3-return conditional HD5 extension case.

This is a research/report script only. It does not write to Supabase.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

from services.h5_primary import (
    H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_D3RET_M1_LIVE_LIMITED_RULES,
    H5_LIVE_LIMITED_CASE_KEY,
    H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
)
from services.trade_case_tester import run_trade_case_test_readonly

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

PERIODS = {
    "train": (date(2023, 1, 1), date(2024, 12, 31)),
    "test": (date(2025, 1, 1), date(2026, 5, 28)),
}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _profit_factor(sims: list[dict]) -> float | None:
    wins = sum(_f(s.get("profit_pct")) for s in sims if _f(s.get("profit_pct")) > 0)
    losses = abs(sum(_f(s.get("profit_pct")) for s in sims if _f(s.get("profit_pct")) <= 0))
    if losses <= 0:
        return None
    return wins / losses


def _summary(case_key: str, period: str, sims: list[dict]) -> dict:
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    n = len(closed)
    wins = [s for s in closed if _f(s.get("profit_pct")) > 0]
    ext_enabled = [s for s in closed if str(s.get("exit_indicator") or "").startswith("extension_")]
    ext_time = [s for s in closed if s.get("exit_reason") == "extension_time_stop"]
    sl = [s for s in closed if s.get("exit_reason") == "sl"]
    pb = [s for s in closed if s.get("exit_reason") == "peak_pullback_exit"]
    timeouts = [s for s in closed if s.get("exit_reason") in {"timeout", "extension_time_stop"}]
    return {
        "period": period,
        "case_key": case_key,
        "trade_count": n,
        "win_rate": round(len(wins) / n * 100, 3) if n else None,
        "avg_ret": round(sum(_f(s.get("profit_pct")) for s in closed) / n, 4) if n else None,
        "median_ret": _median([_f(s.get("profit_pct")) for s in closed]),
        "pf": round(_profit_factor(closed), 4) if _profit_factor(closed) is not None else None,
        "max_loss": round(min((_f(s.get("profit_pct")) for s in closed), default=0.0), 4),
        "emergency_stop_count": len(sl),
        "time_stop_count": len(timeouts),
        "peak_pullback_count": len(pb),
        "extension_enabled_count": len(ext_enabled),
        "extension_enabled_rate": round(len(ext_enabled) / n * 100, 3) if n else None,
        "extension_time_stop_count": len(ext_time),
        "avg_day3_return_extended": _avg([
            _f(s.get("exit_signal_value"))
            for s in ext_enabled
            if s.get("exit_signal_value") is not None
        ]),
    }


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    mid = len(vals) // 2
    if len(vals) % 2:
        return round(vals[mid], 4)
    return round((vals[mid - 1] + vals[mid]) / 2, 4)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _extension_rows(sims: list[dict], enabled: bool) -> list[dict]:
    rows = []
    for s in sims:
        is_ext = str(s.get("exit_indicator") or "").startswith("extension_")
        if is_ext != enabled:
            continue
        rows.append({
            "entry_date": s.get("entry_date"),
            "exit_date": s.get("exit_date"),
            "code": s.get("code"),
            "name": s.get("name"),
            "entry_price": s.get("entry_price"),
            "exit_price": s.get("exit_price"),
            "profit_pct": s.get("profit_pct"),
            "exit_reason": s.get("exit_reason"),
            "exit_indicator": s.get("exit_indicator"),
            "day3_return_pct": s.get("exit_signal_value"),
            "holding_days": s.get("holding_days"),
            "signal_probability": s.get("signal_probability"),
            "margin_ratio": s.get("margin_ratio"),
            "market_regime": s.get("market_regime"),
        })
    return rows


def _monthly(sims: list[dict]) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for s in sims:
        month = str(s.get("entry_date") or "")[:7]
        if month:
            buckets.setdefault(month, []).append(s)
    return [_summary("extension", month, rows) for month, rows in sorted(buckets.items())]


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    cases = [
        H5_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
        H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
    ]
    _write_json(out_dir / "01_extension_case_rules.json", {
        "case_key": H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
        "rules": H5_EXTENSION_D3RET_M1_LIVE_LIMITED_RULES,
    })

    all_summaries: list[dict] = []
    extension_sims_by_period: dict[str, list[dict]] = {}
    for period, (start, end) in PERIODS.items():
        logger.info("[h5_extension_case] period=%s %s..%s", period, start, end)
        _cases, sims_by_case, _results = run_trade_case_test_readonly(start, end, case_keys=cases)
        summaries = [_summary(ck, period, sims_by_case.get(ck, [])) for ck in cases]
        all_summaries.extend(summaries)
        extension_sims_by_period[period] = sims_by_case.get(H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY, [])
        if period == "train":
            _write_csv(out_dir / "02_primary_vs_extension_train.csv", summaries)
        else:
            _write_csv(out_dir / "03_primary_vs_extension_test.csv", summaries)

    exit_rows = []
    for period, sims in extension_sims_by_period.items():
        for reason in sorted({str(s.get("exit_reason") or "") for s in sims}):
            subset = [s for s in sims if str(s.get("exit_reason") or "") == reason]
            exit_rows.append({
                "period": period,
                "exit_reason": reason,
                "count": len(subset),
                "avg_ret": _avg([_f(s.get("profit_pct")) for s in subset]),
            })
    _write_csv(out_dir / "04_extension_exit_breakdown.csv", exit_rows)
    _write_csv(out_dir / "05_extension_enabled_trades.csv", _extension_rows(extension_sims_by_period.get("test", []), True))
    _write_csv(out_dir / "06_extension_not_enabled_trades.csv", _extension_rows(extension_sims_by_period.get("test", []), False))
    _write_csv(out_dir / "07_extension_monthly_stability.csv", _monthly(extension_sims_by_period.get("test", [])))

    train = {r["case_key"]: r for r in all_summaries if r["period"] == "train"}
    test = {r["case_key"]: r for r in all_summaries if r["period"] == "test"}
    primary = H5_LIVE_LIMITED_CASE_KEY
    ext = H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY
    lines = [
        "H5 Extension Case Forward-Test Report",
        "",
        f"Primary: {primary}",
        f"Extension: {ext}",
        "Rule: day3_return <= -1.0% extends holding from HD3 to HD5. EST12 remains active. PB is not used.",
        "",
        "[Train]",
        json.dumps(train.get(primary, {}), ensure_ascii=False),
        json.dumps(train.get(ext, {}), ensure_ascii=False),
        "",
        "[Test]",
        json.dumps(test.get(primary, {}), ensure_ascii=False),
        json.dumps(test.get(ext, {}), ensure_ascii=False),
        "",
        "Conclusion: research/forward-test only. Do not promote to Primary from this script.",
    ]
    (out_dir / "08_extension_forward_test_report.txt").write_text("\n".join(lines), encoding="utf-8")
    logger.info("[h5_extension_case] wrote %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/h5_extension_case")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
