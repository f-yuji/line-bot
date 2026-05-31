"""Compare H5 Primary, raw Extension, Extension Ban, and Extension Allow.

Research/report script only. It does not write to Supabase.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

from services.h5_primary import (
    H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_ALLOW_LIVE_LIMITED_RULES,
    H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
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
        if value is None or value == "":
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


def _profit_factor(values: list[float]) -> float | None:
    wins = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v <= 0))
    if losses <= 0:
        return None if wins <= 0 else 999.0
    return wins / losses


def _max_dd(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(max_dd, 4)


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
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _is_allow_enabled(sim: dict) -> bool:
    return str(sim.get("exit_indicator") or "") in {"extension_allowed_time_stop", "extension_allowed_initial_sl"}


def _is_allow_rejected(sim: dict) -> bool:
    return str(sim.get("exit_indicator") or "").startswith("extension_rejected:")


def _is_extension_enabled(sim: dict) -> bool:
    return str(sim.get("exit_indicator") or "") in {
        "extension_time_stop",
        "extension_initial_sl",
        "extension_allowed_time_stop",
        "extension_allowed_initial_sl",
    }


def _summary(case_key: str, period: str, sims: list[dict]) -> dict:
    closed = [s for s in sims if s.get("status") == "closed" and s.get("profit_pct") is not None]
    returns = [_f(s.get("profit_pct")) for s in closed]
    n = len(closed)
    wins = [v for v in returns if v > 0]
    allowed = [s for s in closed if _is_allow_enabled(s)]
    rejected = [s for s in closed if _is_allow_rejected(s)]
    ext_enabled = [s for s in closed if _is_extension_enabled(s)]
    pb = [s for s in closed if s.get("exit_reason") == "peak_pullback_exit"]
    sl = [s for s in closed if s.get("exit_reason") == "sl"]
    return {
        "period": period,
        "case_key": case_key,
        "trade_count": n,
        "win_rate": round(len(wins) / n * 100, 3) if n else None,
        "avg_ret": _avg(returns),
        "median_ret": _median(returns),
        "pf": round(_profit_factor(returns), 4) if _profit_factor(returns) is not None else None,
        "max_loss": round(min(returns), 4) if returns else None,
        "max_dd": _max_dd(returns),
        "emergency_stop_count": len(sl),
        "peak_pullback_count": len(pb),
        "extension_enabled_count": len(ext_enabled),
        "extension_allowed_count": len(allowed),
        "extension_allowed_rate": round(len(allowed) / n * 100, 3) if n else None,
        "extension_rejected_count": len(rejected),
        "extension_rejected_rate": round(len(rejected) / n * 100, 3) if n else None,
        "extension_time_stop_count": len([s for s in closed if s.get("exit_reason") == "extension_time_stop"]),
        "reject_day1_weak": len([s for s in rejected if "day1_weak" in str(s.get("exit_indicator") or "")]),
        "reject_body_large": len([s for s in rejected if "day3_body_large" in str(s.get("exit_indicator") or "")]),
        "reject_volume_hot": len([s for s in rejected if "day3_volume_hot" in str(s.get("exit_indicator") or "")]),
        "avg_day3_return_allowed": _avg([
            _f(s.get("exit_signal_value"))
            for s in allowed
            if s.get("exit_signal_value") is not None
        ]),
    }


def _trade_rows(sims: list[dict], predicate) -> list[dict]:
    rows: list[dict] = []
    for sim in sims:
        if not predicate(sim):
            continue
        rows.append({
            "entry_date": sim.get("entry_date"),
            "exit_date": sim.get("exit_date"),
            "code": sim.get("code"),
            "name": sim.get("name"),
            "entry_price": sim.get("entry_price"),
            "exit_price": sim.get("exit_price"),
            "profit_pct": sim.get("profit_pct"),
            "exit_reason": sim.get("exit_reason"),
            "exit_indicator": sim.get("exit_indicator"),
            "day3_return_pct": sim.get("exit_signal_value"),
            "holding_days": sim.get("holding_days"),
            "signal_probability": sim.get("signal_probability"),
            "margin_ratio": sim.get("margin_ratio"),
            "market_regime": sim.get("market_regime"),
        })
    return rows


def _exit_breakdown(period: str, case_key: str, sims: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for reason in sorted({str(s.get("exit_reason") or "") for s in sims}):
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


def _load_rule_search_dataset(path: Path) -> list[dict]:
    if not path.exists():
        logger.warning("[h5_extension_allow_case] real-feature dataset missing: %s", path)
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _real_allow_rule(row: dict) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    day3_return = _f(row.get("day3_return"), 999.0)
    day1_return = _f(row.get("day1_return"), -999.0)
    body = _f(row.get("day3_body_pct"), 999.0)
    volume = _f(row.get("day3_volume_ratio"), 999.0)
    if day3_return > -1.0:
        reasons.append("day3_return_not_lte_threshold")
    if day1_return < -2.22:
        reasons.append("day1_weak")
    if body > 3.74:
        reasons.append("day3_body_large")
    if volume > 2.0:
        reasons.append("day3_volume_hot")
    return not reasons, reasons


def _real_feature_rows(dataset: list[dict]) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    allowed_rows: list[dict] = []
    rejected_rows: list[dict] = []
    reason_rows: list[dict] = []
    summary_rows: list[dict] = []
    for row in dataset:
        allowed, reasons = _real_allow_rule(row)
        hd3 = _f(row.get("hd3_return"))
        hd5 = _f(row.get("hd5_return"))
        result = hd5 if allowed else hd3
        out = {
            **row,
            "extension_allowed": allowed,
            "extension_rejected_reason": ",".join(reasons),
            "allow_case_return": result,
            "raw_extension_benefit": _f(row.get("extension_benefit_5")),
        }
        if allowed:
            allowed_rows.append(out)
        else:
            rejected_rows.append(out)
            for reason in reasons:
                reason_rows.append({"reason": reason, "code": row.get("code"), "entry_date": row.get("entry_date")})

    for name, rows in [("allowed", allowed_rows), ("rejected", rejected_rows), ("all", allowed_rows + rejected_rows)]:
        returns = [_f(r.get("allow_case_return")) for r in rows]
        benefits = [_f(r.get("raw_extension_benefit")) for r in rows]
        summary_rows.append({
            "group": name,
            "count": len(rows),
            "avg_case_return": _avg(returns),
            "median_case_return": _median(returns),
            "pf_case_return": round(_profit_factor(returns), 4) if _profit_factor(returns) is not None else None,
            "max_dd_case_return": _max_dd(returns),
            "avg_extension_benefit": _avg(benefits),
            "recovered_rate": round(len([b for b in benefits if b > 0]) / len(benefits) * 100, 3) if benefits else None,
            "died_rate": round(len([b for b in benefits if b <= 0]) / len(benefits) * 100, 3) if benefits else None,
        })
    return allowed_rows, rejected_rows, reason_rows, summary_rows


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    cases = [
        H5_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
        H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY,
        H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
    ]
    _write_json(out_dir / "01_extension_allow_case_rules.json", {
        "case_key": H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY,
        "rules": H5_EXTENSION_ALLOW_LIVE_LIMITED_RULES,
        "note": "Case-test engine uses future close labels, previous-close open proxy, and entry volume_ratio_20d proxy. Real-feature validation uses outputs/h5_extension_rule_search/01_extension_dataset.csv.",
    })

    all_summaries: list[dict] = []
    sims_by_period_case: dict[tuple[str, str], list[dict]] = {}
    for period, (start, end) in PERIODS.items():
        logger.info("[h5_extension_allow_case] period=%s %s..%s", period, start, end)
        _cases, sims_by_case, _results = run_trade_case_test_readonly(start, end, case_keys=cases)
        summaries = [_summary(ck, period, sims_by_case.get(ck, [])) for ck in cases]
        all_summaries.extend(summaries)
        for ck in cases:
            sims_by_period_case[(period, ck)] = sims_by_case.get(ck, [])
        if period == "train":
            _write_csv(out_dir / "02_primary_vs_extension_vs_ban_vs_allow_train.csv", summaries)
        else:
            _write_csv(out_dir / "03_primary_vs_extension_vs_ban_vs_allow_test.csv", summaries)

    breakdown: list[dict] = []
    for period in PERIODS:
        for ck in cases:
            breakdown.extend(_exit_breakdown(period, ck, sims_by_period_case.get((period, ck), [])))
    _write_csv(out_dir / "04_extension_allow_exit_breakdown.csv", breakdown)

    test_allow_sims = sims_by_period_case.get(("test", H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY), [])
    _write_csv(out_dir / "05_extension_allowed_trades.csv", _trade_rows(test_allow_sims, _is_allow_enabled))
    _write_csv(out_dir / "06_extension_rejected_trades.csv", _trade_rows(test_allow_sims, _is_allow_rejected))
    _write_csv(out_dir / "07_extension_allow_monthly_stability.csv", _monthly(H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY, test_allow_sims))
    _write_csv(out_dir / "08_extension_allow_proxy_usage.csv", [
        {
            "field": "day3_open",
            "case_test_source": "previous_close_proxy",
            "case_test_proxy_rate": 100.0,
            "real_feature_proxy_rate": 0.0,
        },
        {
            "field": "day3_volume_ratio",
            "case_test_source": "entry_snapshot_volume_ratio_20d_proxy",
            "case_test_proxy_rate": 100.0,
            "real_feature_proxy_rate": 0.0,
        },
    ])

    reason_counts: dict[str, int] = {}
    for sim in test_allow_sims:
        indicator = str(sim.get("exit_indicator") or "")
        if not indicator.startswith("extension_rejected:"):
            continue
        for reason in indicator.split(":", 1)[1].split(","):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    _write_csv(out_dir / "09_extension_allow_reject_reasons.csv", [
        {"reason": reason, "count": count}
        for reason, count in sorted(reason_counts.items(), key=lambda x: (-x[1], x[0]))
    ])

    dataset = _load_rule_search_dataset(Path(args.rule_search_dataset))
    real_allowed, real_rejected, real_reason_hits, real_summary = _real_feature_rows(dataset)
    _write_csv(out_dir / "11_real_feature_allowed_trades.csv", real_allowed)
    _write_csv(out_dir / "12_real_feature_rejected_trades.csv", real_rejected)
    _write_csv(out_dir / "13_real_feature_allow_summary.csv", real_summary)
    real_reason_counts: dict[str, int] = {}
    for item in real_reason_hits:
        reason = str(item.get("reason") or "")
        real_reason_counts[reason] = real_reason_counts.get(reason, 0) + 1
    _write_csv(out_dir / "14_real_feature_reject_reasons.csv", [
        {"reason": reason, "count": count}
        for reason, count in sorted(real_reason_counts.items(), key=lambda x: (-x[1], x[0]))
    ])

    train = {r["case_key"]: r for r in all_summaries if r["period"] == "train"}
    test = {r["case_key"]: r for r in all_summaries if r["period"] == "test"}
    lines = [
        "H5 Extension Allow Case Report",
        "",
        f"Primary: {H5_LIVE_LIMITED_CASE_KEY}",
        f"Raw Extension: {H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY}",
        f"Extension Ban: {H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY}",
        f"Extension Allow: {H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY}",
        f"Old PB20: {H5_OLD_PB20_LIVE_LIMITED_CASE_KEY}",
        "",
        "Rule:",
        "  day3_return <= -1.0%",
        "  AND day1_return >= -2.22%",
        "  AND day3_body_pct <= 3.74%",
        "  AND day3_volume_ratio <= 2.0",
        "  then extend to HD5. Otherwise exit at HD3. EST12 remains active. PB is not used.",
        "",
        "Proxy note:",
        "  trade_case_tester cannot see real future open/volume-ratio, so case-test output uses proxies.",
        "  real-feature allow/reject outputs are generated from outputs/h5_extension_rule_search/01_extension_dataset.csv.",
        "",
        "[Train summaries]",
        json.dumps(train, ensure_ascii=False),
        "",
        "[Test summaries]",
        json.dumps(test, ensure_ascii=False),
        "",
        "[Real-feature extension-enabled summary]",
        json.dumps(real_summary, ensure_ascii=False),
        "",
        "Judgement:",
        "  This script does not promote Extension Allow to Primary. Use this as a research/forward-test comparison only.",
    ]
    (out_dir / "10_extension_allow_report.txt").write_text("\n".join(lines), encoding="utf-8")
    logger.info("[h5_extension_allow_case] wrote outputs to %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/h5_extension_allow_case")
    p.add_argument("--rule-search-dataset", default="outputs/h5_extension_rule_search/01_extension_dataset.csv")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
