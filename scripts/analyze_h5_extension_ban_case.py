"""Compare H5 Primary, raw Extension, Extension Ban, and old PB20.

Research/report script only. It does not write to Supabase.
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
    H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_BAN_LIVE_LIMITED_RULES,
    H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
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


def _profit_factor(sims: list[dict]) -> float | None:
    wins = sum(_f(s.get("profit_pct")) for s in sims if _f(s.get("profit_pct")) > 0)
    losses = abs(sum(_f(s.get("profit_pct")) for s in sims if _f(s.get("profit_pct")) <= 0))
    if losses <= 0:
        return None
    return wins / losses


def _max_dd(sims: list[dict]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for sim in sorted(sims, key=lambda r: (str(r.get("exit_date") or ""), str(r.get("entry_date") or ""))):
        equity += _f(sim.get("profit_pct"))
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(max_dd, 4)


def _is_ext_enabled(sim: dict) -> bool:
    return str(sim.get("exit_indicator") or "") in {"extension_time_stop", "extension_initial_sl"}


def _is_ext_banned(sim: dict) -> bool:
    return str(sim.get("exit_indicator") or "") == "extension_banned_deep_upper_rsi"


def _is_ext_candidate(sim: dict) -> bool:
    return _is_ext_enabled(sim) or _is_ext_banned(sim)


def _summary(case_key: str, period: str, sims: list[dict]) -> dict:
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    n = len(closed)
    wins = [s for s in closed if _f(s.get("profit_pct")) > 0]
    enabled = [s for s in closed if _is_ext_enabled(s)]
    banned = [s for s in closed if _is_ext_banned(s)]
    candidates = [s for s in closed if _is_ext_candidate(s)]
    ext_time = [s for s in closed if s.get("exit_reason") == "extension_time_stop"]
    sl = [s for s in closed if s.get("exit_reason") == "sl"]
    pb = [s for s in closed if s.get("exit_reason") == "peak_pullback_exit"]
    return {
        "period": period,
        "case_key": case_key,
        "trade_count": n,
        "win_rate": round(len(wins) / n * 100, 3) if n else None,
        "avg_ret": _avg([_f(s.get("profit_pct")) for s in closed]),
        "median_ret": _median([_f(s.get("profit_pct")) for s in closed]),
        "pf": round(_profit_factor(closed), 4) if _profit_factor(closed) is not None else None,
        "max_loss": round(min((_f(s.get("profit_pct")) for s in closed), default=0.0), 4),
        "max_dd": _max_dd(closed),
        "emergency_stop_count": len(sl),
        "peak_pullback_count": len(pb),
        "extension_candidate_count": len(candidates),
        "extension_enabled_count": len(enabled),
        "extension_enabled_rate": round(len(enabled) / n * 100, 3) if n else None,
        "extension_banned_count": len(banned),
        "extension_ban_rate": round(len(banned) / len(candidates) * 100, 3) if candidates else None,
        "extension_time_stop_count": len(ext_time),
        "avg_day3_return_extended": _avg([
            _f(s.get("exit_signal_value"))
            for s in enabled
            if s.get("exit_signal_value") is not None
        ]),
        "avg_day3_return_banned": _avg([
            _f(s.get("exit_signal_value"))
            for s in banned
            if s.get("exit_signal_value") is not None
        ]),
        "avg_ret_banned_actual_hd3": _avg([_f(s.get("profit_pct")) for s in banned]),
        "proxy_open_rate": 100.0 if case_key == H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY and n else None,
        "proxy_rsi_rate": 100.0 if case_key == H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY and n else None,
    }


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


def _trade_rows(sims: list[dict], predicate) -> list[dict]:
    rows = []
    for s in sims:
        if not predicate(s):
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


def _exit_breakdown(period: str, case_key: str, sims: list[dict]) -> list[dict]:
    rows = []
    reasons = sorted({str(s.get("exit_reason") or "") for s in sims})
    for reason in reasons:
        subset = [s for s in sims if str(s.get("exit_reason") or "") == reason]
        rows.append({
            "period": period,
            "case_key": case_key,
            "exit_reason": reason,
            "count": len(subset),
            "avg_ret": _avg([_f(s.get("profit_pct")) for s in subset]),
        })
    for indicator in sorted({str(s.get("exit_indicator") or "") for s in sims if s.get("exit_indicator")}):
        subset = [s for s in sims if str(s.get("exit_indicator") or "") == indicator]
        rows.append({
            "period": period,
            "case_key": case_key,
            "exit_indicator": indicator,
            "count": len(subset),
            "avg_ret": _avg([_f(s.get("profit_pct")) for s in subset]),
            "avg_day3_return": _avg([
                _f(s.get("exit_signal_value"))
                for s in subset
                if s.get("exit_signal_value") is not None
            ]),
        })
    return rows


def _monthly(case_key: str, sims: list[dict]) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for sim in sims:
        month = str(sim.get("entry_date") or "")[:7]
        if month:
            buckets.setdefault(month, []).append(sim)
    return [_summary(case_key, month, rows) for month, rows in sorted(buckets.items())]


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    cases = [
        H5_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
        H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
    ]
    _write_json(out_dir / "01_extension_ban_case_rules.json", {
        "case_key": H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
        "rules": H5_EXTENSION_BAN_LIVE_LIMITED_RULES,
        "note": "Open uses previous close proxy and day3_rsi uses entry snapshot rsi14 in trade_case_tester.",
    })

    all_summaries: list[dict] = []
    sims_by_period_case: dict[tuple[str, str], list[dict]] = {}
    for period, (start, end) in PERIODS.items():
        logger.info("[h5_extension_ban_case] period=%s %s..%s", period, start, end)
        _cases, sims_by_case, _results = run_trade_case_test_readonly(start, end, case_keys=cases)
        summaries = [_summary(ck, period, sims_by_case.get(ck, [])) for ck in cases]
        all_summaries.extend(summaries)
        for ck in cases:
            sims_by_period_case[(period, ck)] = sims_by_case.get(ck, [])
        if period == "train":
            _write_csv(out_dir / "02_primary_vs_extension_vs_ban_train.csv", summaries)
        else:
            _write_csv(out_dir / "03_primary_vs_extension_vs_ban_test.csv", summaries)

    breakdown_rows: list[dict] = []
    for period in PERIODS:
        for ck in cases:
            breakdown_rows.extend(_exit_breakdown(period, ck, sims_by_period_case.get((period, ck), [])))
    _write_csv(out_dir / "04_extension_ban_exit_breakdown.csv", breakdown_rows)

    test_ban_sims = sims_by_period_case.get(("test", H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY), [])
    _write_csv(out_dir / "05_extension_banned_trades.csv", _trade_rows(test_ban_sims, _is_ext_banned))
    _write_csv(out_dir / "06_extension_enabled_after_ban_trades.csv", _trade_rows(test_ban_sims, _is_ext_enabled))
    _write_csv(out_dir / "07_extension_ban_monthly_stability.csv", _monthly(H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY, test_ban_sims))
    _write_csv(out_dir / "08_extension_ban_proxy_usage.csv", [
        {
            "field": "day3_open",
            "source": "previous_close_proxy",
            "proxy_rate": 100.0,
            "reason": "stock_rebound_labels does not provide future open.",
        },
        {
            "field": "day3_rsi",
            "source": "entry_snapshot_rsi14_proxy",
            "proxy_rate": 100.0,
            "reason": "stock_rebound_labels does not provide future RSI.",
        },
    ])

    train = {r["case_key"]: r for r in all_summaries if r["period"] == "train"}
    test = {r["case_key"]: r for r in all_summaries if r["period"] == "test"}
    primary = H5_LIVE_LIMITED_CASE_KEY
    ext = H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY
    ban = H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY
    old_pb = H5_OLD_PB20_LIVE_LIMITED_CASE_KEY
    lines = [
        "H5 Extension Ban Case Report",
        "",
        f"Primary: {primary}",
        f"Raw Extension: {ext}",
        f"Extension Ban: {ban}",
        f"Old PB20: {old_pb}",
        "",
        "Rule:",
        "  day3_return <= -1.0% extends to HD5.",
        "  Ban extension and exit at HD3 when day3_return <= -3.0%, day3 upper shadow >= 1.0%, and entry-rsi14 proxy is 20-35.",
        "  EST12 remains active. PB is not used.",
        "",
        "Proxy note:",
        "  Day3 open uses previous close proxy. Day3 RSI uses entry snapshot rsi14 proxy. Treat this as research-only.",
        "",
        "[Train summaries]",
        json.dumps(train.get(primary, {}), ensure_ascii=False),
        json.dumps(train.get(ext, {}), ensure_ascii=False),
        json.dumps(train.get(ban, {}), ensure_ascii=False),
        json.dumps(train.get(old_pb, {}), ensure_ascii=False),
        "",
        "[Test summaries]",
        json.dumps(test.get(primary, {}), ensure_ascii=False),
        json.dumps(test.get(ext, {}), ensure_ascii=False),
        json.dumps(test.get(ban, {}), ensure_ascii=False),
        json.dumps(test.get(old_pb, {}), ensure_ascii=False),
        "",
        "Judgement:",
        "  This script does not promote Extension Ban to Primary. Compare maxDD, PF, avg_ret, banned_count, and monthly stability before adding any follow-up case.",
    ]
    (out_dir / "09_extension_ban_report.txt").write_text("\n".join(lines), encoding="utf-8")
    logger.info("[h5_extension_ban_case] wrote outputs to %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/h5_extension_ban_case")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
