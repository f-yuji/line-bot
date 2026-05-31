"""H5 Extension Research.

Research-only analysis for whether the new H5 Primary
HD3 + EST12 / no pullback exit should ever be extended to HD5 or HD7.

This script writes CSV/TXT files under outputs/h5_extension_research and does
not modify DB state.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from services.h5_primary import h5_overheat_score
from services.trade_case_tester import _build_supabase, _load_candidates_v2, _to_float

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _d(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


def _round(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, digits)
    except Exception:
        return value


def _month(value: Any) -> str:
    text = str(value or "")
    return text[:7] if len(text) >= 7 else "unknown"


def _pf(returns: list[float]) -> float | None:
    wins = sum(r for r in returns if r > 0)
    losses = abs(sum(r for r in returns if r <= 0))
    if losses == 0:
        return None if wins == 0 else 999.0
    return wins / losses


def _max_dd(returns: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for ret in returns:
        equity += ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _summary(rows: list[dict], ret_key: str = "ret") -> dict:
    vals = [float(r[ret_key]) for r in rows if r.get(ret_key) is not None]
    n = len(vals)
    if not n:
        return {
            "trade_count": 0,
            "win_rate": None,
            "avg_ret": None,
            "median_ret": None,
            "pf": None,
            "total_ret": 0.0,
            "max_loss": None,
            "max_dd": 0.0,
        }
    ordered = sorted(vals)
    median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
    return {
        "trade_count": n,
        "win_rate": round(sum(1 for v in vals if v > 0) / n * 100, 2),
        "avg_ret": round(sum(vals) / n, 4),
        "median_ret": round(median, 4),
        "pf": _round(_pf(vals), 4),
        "total_ret": round(sum(vals), 4),
        "max_loss": round(min(vals), 4),
        "max_dd": round(_max_dd(vals), 4),
    }


def _simulate_hd(row: dict, hold_days: int, stop_pct: float = -0.12) -> dict:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    if not entry:
        return {"ret": None, "exit_reason": "invalid_entry", "holding_days": None}
    stop_price = entry * (1.0 + stop_pct)
    last_close = None
    for day in range(1, hold_days + 1):
        low = _to_float(row.get(f"future_low_{day}d"), None)
        close = _to_float(row.get(f"future_close_{day}d"), None)
        if close is not None:
            last_close = close
        if low is not None and low <= stop_price:
            return {
                "ret": stop_pct * 100.0,
                "exit_price": stop_price,
                "exit_reason": "emergency_stop",
                "holding_days": day,
            }
    if last_close is None:
        return {"ret": None, "exit_reason": "no_data", "holding_days": None}
    return {
        "ret": (last_close / entry - 1.0) * 100.0,
        "exit_price": last_close,
        "exit_reason": "time_stop",
        "holding_days": hold_days,
    }


def _passes_h5_entry(row: dict) -> bool:
    prob = _to_float(row.get("signal_probability"), None)
    stage = str(row.get("signal_stage") or "")
    drop20 = _to_float(row.get("drop_from_20d_high_pct"), None)
    margin = _to_float(row.get("margin_ratio"), None)
    regime = str(row.get("market_regime") or "")
    if prob is None or prob < 0.65:
        return False
    if stage not in {"confirmed", "strong_confirmed"}:
        return False
    if drop20 is None or drop20 > -8.0:
        return False
    if regime == "panic_selloff":
        return False
    if h5_overheat_score(row) > 1:
        return False
    if margin is not None and (margin < 3 or margin > 30):
        return False
    return True


def _day3_features(row: dict) -> dict:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    c3 = _to_float(row.get("future_close_3d"), None)
    h3 = _to_float(row.get("future_high_3d"), None)
    l3 = _to_float(row.get("future_low_3d"), None)
    highs = [_to_float(row.get(f"future_high_{d}d"), None) for d in range(1, 4)]
    lows = [_to_float(row.get(f"future_low_{d}d"), None) for d in range(1, 4)]
    peak = max([v for v in highs if v is not None], default=None)
    trough = min([v for v in lows if v is not None], default=None)
    day_range = (h3 - l3) if h3 is not None and l3 is not None else None
    ma5 = _to_float(row.get("ma5"), None)
    ma25 = _to_float(row.get("ma25"), None)
    ma75 = _to_float(row.get("ma75"), None)
    volume_ratio = _to_float(row.get("volume_ratio_20d"), None)
    body = None
    upper = None
    lower = None
    close_pos = None
    if c3 is not None and h3 is not None and l3 is not None and day_range and day_range > 0:
        close_pos = (c3 - l3) / day_range
        upper = (h3 - c3) / c3 * 100.0 if c3 else None
        lower = (c3 - l3) / c3 * 100.0 if c3 else None
    return {
        "day3_close": c3,
        "day3_high": h3,
        "day3_low": l3,
        "day3_close_vs_entry_pct": _pct(c3, entry),
        "day3_high_vs_entry_pct": _pct(h3, entry),
        "day3_low_vs_entry_pct": _pct(l3, entry),
        "day3_ma5": ma5,
        "day3_ma25": ma25,
        "day3_ma75": ma75,
        "day3_close_vs_ma5_pct": _pct(c3, ma5),
        "day3_close_vs_ma25_pct": _pct(c3, ma25),
        "day3_close_vs_ma75_pct": _pct(c3, ma75),
        "day3_close_above_ma5": c3 is not None and ma5 is not None and c3 >= ma5,
        "day3_close_above_ma25": c3 is not None and ma25 is not None and c3 >= ma25,
        "day3_rsi": _to_float(row.get("rsi14"), None),
        "day3_overheat_score": h5_overheat_score({**row, "close": c3 or row.get("close")}),
        "day3_overheat_bucket": "hot" if h5_overheat_score(row) >= 2 else ("mild" if h5_overheat_score(row) == 1 else "cool"),
        "day3_volume_ratio": volume_ratio,
        "day3_volume_maintained_flag": volume_ratio is not None and volume_ratio >= 1.0,
        "day3_candle_body_pct": body,
        "day3_upper_shadow_pct": upper,
        "day3_lower_shadow_pct": lower,
        "day3_is_bullish": c3 is not None and entry is not None and c3 >= entry,
        "day3_is_bearish": c3 is not None and entry is not None and c3 < entry,
        "day3_close_position_in_range": close_pos,
        "day3_peak_since_entry": peak,
        "day3_trough_since_entry": trough,
        "day3_peak_gap_pct": _pct(c3, peak),
        "day3_new_high_since_entry": peak is not None and entry is not None and peak > entry,
        "day3_breaks_recent_low": trough is not None and entry is not None and trough < entry * 0.97,
        "day3_close_near_high": close_pos is not None and close_pos >= 0.7,
        "day3_close_near_low": close_pos is not None and close_pos <= 0.3,
        "day3_market_regime": row.get("market_regime"),
        "margin_ratio": row.get("margin_ratio"),
        "liquidity": row.get("turnover_value") or row.get("liquidity"),
        "atr_pct": row.get("atr_pct"),
        "day3_atr_pct": row.get("atr_pct"),
    }


Condition = tuple[str, Callable[[dict], bool]]


def _conditions() -> list[Condition]:
    return [
        ("day3_return_ge_0", lambda r: (r.get("day3_close_vs_entry_pct") is not None and r["day3_close_vs_entry_pct"] >= 0)),
        ("day3_return_ge_1", lambda r: (r.get("day3_close_vs_entry_pct") is not None and r["day3_close_vs_entry_pct"] >= 1)),
        ("day3_return_ge_2", lambda r: (r.get("day3_close_vs_entry_pct") is not None and r["day3_close_vs_entry_pct"] >= 2)),
        ("day3_return_le_minus_1", lambda r: (r.get("day3_close_vs_entry_pct") is not None and r["day3_close_vs_entry_pct"] <= -1)),
        ("day3_close_above_ma5", lambda r: bool(r.get("day3_close_above_ma5"))),
        ("day3_close_above_ma25", lambda r: bool(r.get("day3_close_above_ma25"))),
        ("day3_rsi_lt_70", lambda r: (r.get("day3_rsi") is not None and r["day3_rsi"] < 70)),
        ("day3_rsi_40_65", lambda r: (r.get("day3_rsi") is not None and 40 <= r["day3_rsi"] <= 65)),
        ("day3_rsi_lt_50", lambda r: (r.get("day3_rsi") is not None and r["day3_rsi"] < 50)),
        ("day3_volume_ratio_ge_1_2", lambda r: (r.get("day3_volume_ratio") is not None and r["day3_volume_ratio"] >= 1.2)),
        ("day3_volume_ratio_ge_1_5", lambda r: (r.get("day3_volume_ratio") is not None and r["day3_volume_ratio"] >= 1.5)),
        ("day3_peak_gap_gt_minus_1", lambda r: (r.get("day3_peak_gap_pct") is not None and r["day3_peak_gap_pct"] > -1)),
        ("day3_peak_gap_gt_minus_2", lambda r: (r.get("day3_peak_gap_pct") is not None and r["day3_peak_gap_pct"] > -2)),
        ("day3_close_position_ge_0_6", lambda r: (r.get("day3_close_position_in_range") is not None and r["day3_close_position_in_range"] >= 0.6)),
        ("day3_upper_shadow_lt_1_5", lambda r: (r.get("day3_upper_shadow_pct") is not None and r["day3_upper_shadow_pct"] < 1.5)),
        ("day3_lower_shadow_gt_1_0", lambda r: (r.get("day3_lower_shadow_pct") is not None and r["day3_lower_shadow_pct"] > 1.0)),
        ("day3_regime_normal_or_risk_on", lambda r: str(r.get("day3_market_regime") or "") in {"normal", "risk_on", "strong_risk_on"}),
        ("margin_3_30", lambda r: (r.get("margin_ratio") is None or 3 <= float(r["margin_ratio"]) <= 30)),
    ]


def _classify(diff: float | None) -> str:
    if diff is None:
        return "unknown"
    if diff >= 1.0:
        return "strong_better"
    if diff > 0.3:
        return "better"
    if diff >= -0.3:
        return "flat"
    return "worse"


def _build_trade_rows(candidates: list[dict]) -> list[dict]:
    out = []
    for row in candidates:
        if not _passes_h5_entry(row):
            continue
        hd3 = _simulate_hd(row, 3)
        hd5 = _simulate_hd(row, 5)
        hd7 = _simulate_hd(row, 7)
        if hd3.get("ret") is None:
            continue
        benefit5 = (hd5.get("ret") - hd3.get("ret")) if hd5.get("ret") is not None else None
        benefit7 = (hd7.get("ret") - hd3.get("ret")) if hd7.get("ret") is not None else None
        feat = _day3_features(row)
        out.append({
            "trade_date": str(row.get("trade_date")),
            "code": row.get("code"),
            "name": row.get("name"),
            "sector": row.get("sector"),
            "entry_price": row.get("entry_price") or row.get("close"),
            "signal_probability": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "drop_from_20d_high_pct": row.get("drop_from_20d_high_pct"),
            "market_regime": row.get("market_regime"),
            "margin_ratio": row.get("margin_ratio"),
            "hd3_return": hd3.get("ret"),
            "hd5_return": hd5.get("ret"),
            "hd7_return": hd7.get("ret"),
            "hd3_exit_reason": hd3.get("exit_reason"),
            "hd5_exit_reason": hd5.get("exit_reason"),
            "hd7_exit_reason": hd7.get("exit_reason"),
            "extension_benefit_5": benefit5,
            "extension_benefit_7": benefit7,
            "extension_class_5": _classify(benefit5),
            "extension_class_7": _classify(benefit7),
            **feat,
        })
    return out


def _fixed_comparison(rows: list[dict], period: str) -> list[dict]:
    result = []
    for label, key in [("HD3_FIXED", "hd3_return"), ("HD5_FIXED", "hd5_return"), ("HD7_FIXED", "hd7_return")]:
        vals = [{"ret": r.get(key)} for r in rows if r.get(key) is not None]
        result.append({"period": period, "exit_model": label, **_summary(vals)})
    return result


def _condition_results(rows: list[dict], conds: list[Condition], period: str, max_combo: int = 1, min_combo: int = 1) -> list[dict]:
    base = _summary([{"ret": r["hd3_return"]} for r in rows if r.get("hd3_return") is not None])
    out = []
    for size in range(min_combo, max_combo + 1):
        for combo in itertools.combinations(conds, size):
            names = [c[0] for c in combo]
            funcs = [c[1] for c in combo]
            selected = [r for r in rows if all(fn(r) for fn in funcs)]
            if not selected:
                continue
            for target, ret_key in [("HD5", "hd5_return"), ("HD7", "hd7_return")]:
                model_rows = []
                selected_ids = {id(r) for r in selected}
                for r in rows:
                    use_ext = id(r) in selected_ids and r.get(ret_key) is not None
                    model_rows.append({"ret": r.get(ret_key) if use_ext else r.get("hd3_return")})
                sm = _summary(model_rows)
                selected_sm = _summary([{"ret": r.get(ret_key)} for r in selected if r.get(ret_key) is not None])
                out.append({
                    "period": period,
                    "target_extension": target,
                    "condition_count": size,
                    "condition": " AND ".join(names),
                    "trade_count": sm["trade_count"],
                    "selected_count": len(selected),
                    "selected_rate": round(len(selected) / len(rows) * 100, 2) if rows else 0,
                    "win_rate": sm["win_rate"],
                    "avg_ret": sm["avg_ret"],
                    "median_ret": sm["median_ret"],
                    "pf": sm["pf"],
                    "total_ret": sm["total_ret"],
                    "max_loss": sm["max_loss"],
                    "maxDD": sm["max_dd"],
                    "avg_ret_diff": _round((sm["avg_ret"] or 0) - (base["avg_ret"] or 0)),
                    "PF_diff": _round((sm["pf"] or 0) - (base["pf"] or 0)),
                    "win_rate_diff": _round((sm["win_rate"] or 0) - (base["win_rate"] or 0)),
                    "maxDD_diff": _round((sm["max_dd"] or 0) - (base["max_dd"] or 0)),
                    "total_ret_diff": _round((sm["total_ret"] or 0) - (base["total_ret"] or 0)),
                    "selected_avg_ret": selected_sm["avg_ret"],
                    "selected_pf": selected_sm["pf"],
                })
    return out


def _merge_top(train: list[dict], test: list[dict]) -> list[dict]:
    train_by_key = {(r["target_extension"], r["condition"]): r for r in train}
    out = []
    for t in test:
        key = (t["target_extension"], t["condition"])
        tr = train_by_key.get(key)
        if not tr:
            continue
        selected_count = min(int(t.get("selected_count") or 0), int(tr.get("selected_count") or 0))
        stability = (
            (float(t.get("avg_ret_diff") or 0) + float(tr.get("avg_ret_diff") or 0)) * 10
            + (float(t.get("PF_diff") or 0) + float(tr.get("PF_diff") or 0)) * 5
            + min(selected_count, 300) / 30
        )
        out.append({
            "target_extension": key[0],
            "condition": key[1],
            "train_selected_count": tr.get("selected_count"),
            "test_selected_count": t.get("selected_count"),
            "train_avg_ret_diff": tr.get("avg_ret_diff"),
            "test_avg_ret_diff": t.get("avg_ret_diff"),
            "train_pf_diff": tr.get("PF_diff"),
            "test_pf_diff": t.get("PF_diff"),
            "train_maxDD_diff": tr.get("maxDD_diff"),
            "test_maxDD_diff": t.get("maxDD_diff"),
            "stability_score": round(stability, 4),
            "candidate": (
                (tr.get("avg_ret_diff") or 0) > 0
                and (t.get("avg_ret_diff") or 0) > 0
                and (tr.get("PF_diff") or 0) >= 0
                and (t.get("PF_diff") or 0) >= 0
                and selected_count >= 30
            ),
        })
    out.sort(key=lambda r: (r["candidate"], r["stability_score"]), reverse=True)
    return out


def _monthly(rows: list[dict]) -> list[dict]:
    by_month: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_month[_month(r.get("trade_date"))].append(r)
    out = []
    for month, group in sorted(by_month.items()):
        row = {"month": month}
        for label, key in [("hd3", "hd3_return"), ("hd5", "hd5_return"), ("hd7", "hd7_return")]:
            sm = _summary([{"ret": r.get(key)} for r in group if r.get(key) is not None])
            row[f"{label}_trade_count"] = sm["trade_count"]
            row[f"{label}_avg_ret"] = sm["avg_ret"]
            row[f"{label}_pf"] = sm["pf"]
            row[f"{label}_total_ret"] = sm["total_ret"]
        out.append(row)
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _sample(rows: list[dict], key: str, reverse: bool, n: int = 100) -> list[dict]:
    return sorted(
        [r for r in rows if r.get(key) is not None],
        key=lambda r: float(r.get(key) or 0),
        reverse=reverse,
    )[:n]


def _report(path: Path, fixed_train: list[dict], fixed_test: list[dict], top: list[dict]) -> None:
    def fixed_line(rows: list[dict], model: str) -> str:
        row = next((r for r in rows if r.get("exit_model") == model), {})
        return (
            f"{model}: n={row.get('trade_count')} WR={row.get('win_rate')}% "
            f"avg={row.get('avg_ret')} PF={row.get('pf')} maxDD={row.get('max_dd')}"
        )

    candidates = [r for r in top if r.get("candidate")]
    lines = [
        "H5 Extension Research Report",
        "",
        "Primary premise: H5 Primary remains HD3 + EST12 / no pullback.",
        "Extension rules are research-only and are not applied to production.",
        "",
        "[Train fixed exits]",
        fixed_line(fixed_train, "HD3_FIXED"),
        fixed_line(fixed_train, "HD5_FIXED"),
        fixed_line(fixed_train, "HD7_FIXED"),
        "",
        "[Test fixed exits]",
        fixed_line(fixed_test, "HD3_FIXED"),
        fixed_line(fixed_test, "HD5_FIXED"),
        fixed_line(fixed_test, "HD7_FIXED"),
        "",
        "[Top extension candidates]",
    ]
    for row in top[:15]:
        lines.append(
            f"{row.get('target_extension')} {row.get('condition')} "
            f"train_diff={row.get('train_avg_ret_diff')} test_diff={row.get('test_avg_ret_diff')} "
            f"train_pf_diff={row.get('train_pf_diff')} test_pf_diff={row.get('test_pf_diff')} "
            f"candidate={row.get('candidate')}"
        )
    lines += [
        "",
        "[Conclusion]",
        (
            "Adoption candidate exists, but keep as comparison/forward-test first."
            if candidates else
            "No sufficiently stable simple extension rule found. Keep HD3 fixed as Primary."
        ),
        "",
        "Questions answered:",
        "1. HD3/HD5/HD7 fixed comparison: see 01/02 CSV.",
        "2. Extension groups: see 05-09 CSV.",
        "3. Day3 features: see 04 CSV.",
        "4. RSI/MA/volume/regime effects: represented in condition result CSVs.",
        "5. Production reflection: not applied; research only.",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = _d(args.start)
    end = _d(args.end)
    train_end = _d(args.train_end)

    sb = _build_supabase()
    logger.info("loading candidates %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    trades = _build_trade_rows(candidates)
    if args.max_trades and len(trades) > args.max_trades:
        trades = trades[: args.max_trades]
    logger.info("H5 trades=%d", len(trades))

    train = [r for r in trades if _d(r["trade_date"]) <= train_end]
    test = [r for r in trades if _d(r["trade_date"]) > train_end]
    conds = _conditions()

    fixed_train = _fixed_comparison(train, "train")
    fixed_test = _fixed_comparison(test, "test")
    single_train = _condition_results(train, conds, "train", max_combo=1)
    single_test = _condition_results(test, conds, "test", max_combo=1)
    combo_train = _condition_results(train, conds[:14], "train", max_combo=3, min_combo=2)
    combo_test = _condition_results(test, conds[:14], "test", max_combo=3, min_combo=2)
    top = _merge_top(single_train + combo_train, single_test + combo_test)
    bad = sorted(top, key=lambda r: (r.get("test_avg_ret_diff") or 0))[:100]

    labels = [{
        k: _round(v) for k, v in row.items()
        if k in {
            "trade_date", "code", "name", "sector", "entry_price", "signal_probability",
            "hd3_return", "hd5_return", "hd7_return", "hd3_exit_reason", "hd5_exit_reason", "hd7_exit_reason",
            "extension_benefit_5", "extension_benefit_7", "extension_class_5", "extension_class_7",
        }
    } for row in trades]
    features = [{k: _round(v) for k, v in row.items()} for row in trades]

    _write_csv(out_dir / "01_base_hd3_hd5_hd7_comparison_train.csv", fixed_train)
    _write_csv(out_dir / "02_base_hd3_hd5_hd7_comparison_test.csv", fixed_test)
    _write_csv(out_dir / "03_extension_labels_all_trades.csv", labels)
    _write_csv(out_dir / "04_day3_features_all_trades.csv", features)
    _write_csv(out_dir / "05_single_condition_extension_results_train.csv", single_train)
    _write_csv(out_dir / "06_single_condition_extension_results_test.csv", single_test)
    _write_csv(out_dir / "07_combo_condition_extension_results_train.csv", combo_train)
    _write_csv(out_dir / "08_combo_condition_extension_results_test.csv", combo_test)
    _write_csv(out_dir / "09_top_extension_rules.csv", top[:200])
    _write_csv(out_dir / "10_bad_extension_rules.csv", bad)
    _write_csv(out_dir / "11_monthly_stability_extension.csv", _monthly(trades))
    _write_csv(out_dir / "12_case_samples_extension_better.csv", _sample(trades, "extension_benefit_5", True))
    _write_csv(out_dir / "13_case_samples_hd3_better.csv", _sample(trades, "extension_benefit_5", False))
    _report(out_dir / "14_extension_recommendation_report.txt", fixed_train, fixed_test, top)

    logger.info("wrote outputs to %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2023-01-01")
    p.add_argument("--train-end", default="2024-12-31")
    p.add_argument("--end", default="2026-05-28")
    p.add_argument("--output-dir", default="outputs/h5_extension_research")
    p.add_argument("--max-trades", type=int, default=0, help="debug only; 0 = no limit")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
