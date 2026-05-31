"""Analyze H5 baselines using walk-forward prediction CSV only.

Research-only script. It does not use the active model and does not modify DB,
Primary, UI, notifications, Watchlist, Intraday H5, or actual trade logs.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_primary import h5_overheat_score


EST12_STOP_RATE = -0.12
EST12_STOP_PCT = -12.0
HOLDING_DAYS = (1, 2, 3, 5, 7, 10)
RANDOM_SEEDS = (0, 1, 2, 3, 4, 5, 10, 42, 99, 123)


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:10]).date()


def to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        number = float(value)
        if not math.isfinite(number):
            return default
        return number
    except Exception:
        return default


def round_value(value: Any, digits: int = 6) -> Any:
    number = to_float(value, None)
    if number is None:
        return value
    return round(number, digits)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: round_value(row.get(key)) for key in headers})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def win_rate(values: list[float]) -> float | None:
    return sum(1 for v in values if v > 0) / len(values) * 100.0 if values else None


def profit_factor(values: list[float]) -> float | None:
    wins = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v <= 0))
    if losses <= 0:
        return 999.0 if wins > 0 else None
    return wins / losses


def max_drawdown_sum(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def entry_price(row: dict) -> float | None:
    return to_float(row.get("entry_price"), None) or to_float(row.get("close"), None)


def hd_return(row: dict, hold: int, path_type: str = "est12") -> tuple[float | None, str]:
    entry = entry_price(row)
    if entry is None or entry <= 0:
        return None, "invalid_entry"
    if path_type == "raw":
        close = to_float(row.get(f"future_close_{hold}d"), None)
        if close is None:
            return None, "no_data"
        return (close / entry - 1.0) * 100.0, "time_stop"
    stop = entry * (1.0 + EST12_STOP_RATE)
    last_close = None
    for day in range(1, hold + 1):
        close = to_float(row.get(f"future_close_{day}d"), None)
        low = to_float(row.get(f"future_low_{day}d"), None)
        if close is not None:
            last_close = close
        if low is not None and low <= stop:
            return EST12_STOP_PCT, "emergency_stop"
    if last_close is None:
        return None, "no_data"
    return (last_close / entry - 1.0) * 100.0, "time_stop"


def attach_returns(rows: list[dict]) -> None:
    for row in rows:
        row["_trade_date"] = parse_date(row["trade_date"])
        row["_month"] = row["_trade_date"].strftime("%Y-%m")
        for hold in HOLDING_DAYS:
            ret, reason = hd_return(row, hold, "est12")
            row[f"_hd{hold}"] = ret
            row[f"_reason_hd{hold}"] = reason


def passes_ai(row: dict) -> bool:
    p = to_float(row.get("signal_probability"), None)
    return p is not None and p >= 0.65


def passes_drop(row: dict) -> bool:
    d = to_float(row.get("drop_from_20d_high_pct"), None)
    return d is not None and d <= -8.0


def passes_stage(row: dict) -> bool:
    return str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}


def passes_no_panic(row: dict) -> bool:
    return str(row.get("market_regime") or "") != "panic_selloff"


def passes_overheat(row: dict) -> bool:
    return h5_overheat_score(row) <= 1


def passes_margin(row: dict) -> bool:
    margin = to_float(row.get("margin_ratio"), None)
    return margin is None or 3.0 <= margin <= 30.0


def passes_k_no_normal(row: dict) -> bool:
    return passes_h5_full(row) and str(row.get("market_regime") or "") not in {"normal", "euphoria"}


def passes_h5_full(row: dict) -> bool:
    return passes_ai(row) and passes_drop(row) and passes_stage(row) and passes_no_panic(row) and passes_overheat(row) and passes_margin(row)


STRATEGIES: list[tuple[str, Callable[[dict], bool]]] = [
    ("filter_zero_all", lambda r: True),
    ("AI_only", passes_ai),
    ("drop_only", passes_drop),
    ("AI_plus_drop", lambda r: passes_ai(r) and passes_drop(r)),
    ("AI_plus_drop_stage", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r)),
    ("H5_full_no_margin", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r)),
    ("H5_full", passes_h5_full),
    ("K_no_normal", passes_k_no_normal),
]


def summarize(rows: list[dict], strategy: str, period: str, path_type: str = "est12") -> dict:
    out = {"strategy": strategy, "period": period, "path_type": path_type}
    vals3 = [row["_hd3"] for row in rows if row.get("_hd3") is not None]
    out["n"] = len(vals3)
    for hold in HOLDING_DAYS:
        vals = [row[f"_hd{hold}"] for row in rows if row.get(f"_hd{hold}") is not None]
        out[f"HD{hold}_avg"] = avg(vals)
        if hold in (3, 5, 7):
            out[f"HD{hold}_WR"] = win_rate(vals)
    out["PF_HD3"] = profit_factor(vals3)
    out["maxDD_HD3"] = max_drawdown_sum(vals3)
    out["max_loss"] = min(vals3) if vals3 else None
    out["emergency_stop_count"] = sum(1 for row in rows if row.get("_reason_hd3") == "emergency_stop")
    out["notes"] = ""
    return out


def period_rows(rows: list[dict], period: str) -> list[dict]:
    if period == "all":
        return rows
    if period == "test":
        return rows
    return rows


def rows_for_strategy(rows: list[dict], pred: Callable[[dict], bool]) -> list[dict]:
    return [row for row in rows if pred(row)]


def random_same_day(rows: list[dict], h5_rows: list[dict], exclude_h5: bool = True) -> tuple[list[dict], list[dict]]:
    by_date: dict[date, list[dict]] = defaultdict(list)
    h5_by_date: dict[date, list[dict]] = defaultdict(list)
    h5_keys = {(row.get("code"), row["_trade_date"]) for row in h5_rows}
    for row in rows:
        if exclude_h5 and (row.get("code"), row["_trade_date"]) in h5_keys:
            continue
        by_date[row["_trade_date"]].append(row)
    for row in h5_rows:
        h5_by_date[row["_trade_date"]].append(row)
    seed_results = []
    all_draws = []
    for seed in RANDOM_SEEDS:
        rng = random.Random(seed)
        draws = []
        skipped = 0
        for d, hs in h5_by_date.items():
            pool = list(by_date.get(d, []))
            n = len(hs)
            if len(pool) < n:
                skipped += n
                continue
            draws.extend(rng.sample(pool, n))
        seed_results.append({**summarize(draws, "market_random_same_day_exclude_h5", "test"), "seed": seed, "skipped": skipped})
        all_draws.extend(draws)
    return seed_results, all_draws


def group_random(rows: list[dict], h5_rows: list[dict], group_fn: Callable[[dict], str], name: str) -> list[dict]:
    by_key: dict[tuple[date, str], list[dict]] = defaultdict(list)
    h5_keys = {(row.get("code"), row["_trade_date"]) for row in h5_rows}
    for row in rows:
        if (row.get("code"), row["_trade_date"]) in h5_keys:
            continue
        by_key[(row["_trade_date"], group_fn(row))].append(row)
    seed_summaries = []
    for seed in RANDOM_SEEDS:
        rng = random.Random(seed)
        draws = []
        skipped = 0
        for h5 in h5_rows:
            pool = by_key.get((h5["_trade_date"], group_fn(h5)), [])
            if not pool:
                skipped += 1
                continue
            draws.append(rng.choice(pool))
        seed_summaries.append({**summarize(draws, name, "test"), "seed": seed, "skipped": skipped})
    return seed_summaries


def volume_bucket(row: dict) -> str:
    v = to_float(row.get("volume_ratio_20d"), None)
    if v is None:
        return "null"
    if v < 0.7:
        return "lt_0_7"
    if v < 1.0:
        return "0_7_to_1_0"
    if v < 1.5:
        return "1_0_to_1_5"
    if v < 2.0:
        return "1_5_to_2_0"
    if v < 3.0:
        return "2_0_to_3_0"
    return "gt_3_0"


def drop_bucket(row: dict) -> str:
    d = to_float(row.get("drop_from_20d_high_pct"), None)
    if d is None:
        return "null"
    if d > -8:
        return "minus5_to_minus8_or_less"
    if d > -10:
        return "minus8_to_minus10"
    if d > -15:
        return "minus10_to_minus15"
    if d > -20:
        return "minus15_to_minus20"
    return "minus20_or_more"


def score_bucket(row: dict) -> str:
    p = to_float(row.get("signal_probability"), None)
    if p is None:
        return "null"
    if p < 0.50:
        return "lt_0_50"
    if p < 0.55:
        return "0_50_to_0_55"
    if p < 0.60:
        return "0_55_to_0_60"
    if p < 0.65:
        return "0_60_to_0_65"
    if p < 0.70:
        return "0_65_to_0_70"
    if p < 0.75:
        return "0_70_to_0_75"
    if p < 0.80:
        return "0_75_to_0_80"
    if p < 0.85:
        return "0_80_to_0_85"
    if p < 0.90:
        return "0_85_to_0_90"
    return "gte_0_90"


def mean_seed_summary(seed_rows: list[dict], strategy: str) -> dict:
    if not seed_rows:
        return {"strategy": strategy, "period": "test", "path_type": "est12", "n": 0}
    keys = [k for k in seed_rows[0] if k not in {"strategy", "period", "path_type", "seed", "notes"}]
    out = {"strategy": strategy, "period": "test", "path_type": "est12"}
    for key in keys:
        vals = [to_float(row.get(key), None) for row in seed_rows if to_float(row.get(key), None) is not None]
        out[key] = avg(vals)
    out["notes"] = "mean across random seeds"
    return out


def load_active_baseline(path: Path) -> list[dict]:
    rows = read_csv(path)
    wanted = {"AI_only", "AI_plus_drop", "H5_full", "market_random_same_day_exclude_h5_mean"}
    return [row for row in rows if row.get("strategy") in wanted and row.get("period") in {"test", "all"} and row.get("path_type") == "est12"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions", default="outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv")
    parser.add_argument("--output-dir", default="outputs/h5_walk_forward_baseline")
    parser.add_argument("--active-baseline", default="outputs/h5_market_random_baseline/01_strategy_performance_matrix.csv")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    rows = read_csv(ROOT / args.predictions)
    if not rows:
        raise RuntimeError("walk-forward predictions CSV is missing or empty")
    attach_returns(rows)
    rows = [row for row in rows if row.get("_hd3") is not None]
    h5_rows = rows_for_strategy(rows, passes_h5_full)

    perf_rows = []
    for name, pred in STRATEGIES:
        perf_rows.append(summarize(rows_for_strategy(rows, pred), name, "test"))

    seed_rows, _ = random_same_day(rows, h5_rows, exclude_h5=True)
    sector_seed = group_random(rows, h5_rows, lambda r: str(r.get("sector") or "unknown"), "same_sector_random")
    vol_seed = group_random(rows, h5_rows, volume_bucket, "same_volume_bucket_random")
    drop_seed = group_random(rows, h5_rows, drop_bucket, "same_drop_bucket_random")
    perf_rows.extend([
        mean_seed_summary(seed_rows, "market_random_same_day_exclude_h5_mean"),
        mean_seed_summary(sector_seed, "same_sector_random_mean"),
        mean_seed_summary(vol_seed, "same_volume_bucket_random_mean"),
        mean_seed_summary(drop_seed, "same_drop_bucket_random_mean"),
    ])
    write_csv(output_dir / "01_strategy_performance_matrix.csv", perf_rows)

    ablation_rows = []
    previous = None
    ai_drop = None
    for name, pred in STRATEGIES[:7]:
        summary = summarize(rows_for_strategy(rows, pred), name, "test")
        cur = to_float(summary.get("HD3_avg"), None)
        prev = to_float(previous.get("HD3_avg"), None) if previous else None
        summary["delta_vs_previous"] = cur - prev if cur is not None and prev is not None else None
        if name == "AI_plus_drop":
            ai_drop = summary
        ai_drop_v = to_float(ai_drop.get("HD3_avg"), None) if ai_drop else None
        summary["delta_vs_AI_plus_drop"] = cur - ai_drop_v if cur is not None and ai_drop_v is not None else None
        ablation_rows.append(summary)
        previous = summary
    write_csv(output_dir / "02_filter_ablation.csv", ablation_rows)
    write_csv(output_dir / "03_market_random_comparison.csv", seed_rows + sector_seed + vol_seed + drop_seed)
    write_csv(output_dir / "04_k_no_normal_comparison.csv", [summarize(rows_for_strategy(rows, passes_k_no_normal), "K_no_normal", "test")])

    active_rows = load_active_baseline(ROOT / args.active_baseline)
    active_by = {(r.get("strategy"), r.get("period")): r for r in active_rows}
    wf_by = {(r.get("strategy"), r.get("period")): r for r in perf_rows}
    compare = []
    for strategy in ("AI_only", "AI_plus_drop", "H5_full", "K_no_normal", "market_random_same_day_exclude_h5_mean"):
        active = active_by.get((strategy, "test")) or active_by.get((strategy, "all"))
        wf = wf_by.get((strategy, "test"))
        if active:
            compare.append({**active, "version": "active_model_rescore", "delta_walk_forward_minus_active": None})
        if wf:
            active_avg = to_float(active.get("HD3_avg"), None) if active else None
            wf_avg = to_float(wf.get("HD3_avg"), None)
            row = dict(wf)
            row["version"] = "walk_forward"
            row["delta_walk_forward_minus_active"] = wf_avg - active_avg if wf_avg is not None and active_avg is not None else None
            compare.append(row)
    write_csv(output_dir / "05_active_vs_walk_forward_comparison.csv", compare)

    monthly_rows = []
    by_month: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for name, pred in STRATEGIES:
        for row in rows_for_strategy(rows, pred):
            by_month[(name, row["_month"])].append(row)
    for (name, month), group in sorted(by_month.items()):
        s = summarize(group, name, "test")
        s["month"] = month
        vals = [row["_hd3"] for row in group if row.get("_hd3") is not None]
        s["HD3_total_return_sum"] = sum(vals)
        monthly_rows.append(s)
    write_csv(output_dir / "06_monthly_stability.csv", monthly_rows)

    regime_rows = []
    for name, pred in STRATEGIES:
        groups: dict[str, list[dict]] = defaultdict(list)
        for row in rows_for_strategy(rows, pred):
            groups[str(row.get("market_regime") or "unknown")].append(row)
        for regime, group in sorted(groups.items()):
            s = summarize(group, name, "test")
            s["regime"] = regime
            regime_rows.append(s)
    write_csv(output_dir / "07_regime_breakdown.csv", regime_rows)

    bucket_rows = []
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[score_bucket(row)].append(row)
    for bucket, group in sorted(groups.items()):
        s = summarize(group, "score_bucket", "test")
        s["bucket"] = bucket
        bucket_rows.append(s)
    write_csv(output_dir / "08_ai_score_bucket_performance.csv", bucket_rows)

    skipped = []
    for reason, count in [
        ("missing_or_invalid_hd3_eval", len(read_csv(ROOT / args.predictions)) - len(rows)),
        ("h5_full_candidate_count", len(h5_rows)),
    ]:
        skipped.append({"reason": reason, "count": count, "period": "test", "notes": ""})
    write_csv(output_dir / "09_missing_or_skipped_summary.csv", skipped)

    perf_by_name = {r["strategy"]: r for r in perf_rows}
    ai = perf_by_name.get("AI_only", {})
    aipd = perf_by_name.get("AI_plus_drop", {})
    h5 = perf_by_name.get("H5_full", {})
    rand = perf_by_name.get("market_random_same_day_exclude_h5_mean", {})
    kn = perf_by_name.get("K_no_normal", {})
    h5_minus_rand = (to_float(h5.get("HD3_avg"), None) or 0) - (to_float(rand.get("HD3_avg"), None) or 0)
    write_text(output_dir / "10_walk_forward_baseline_report.txt", f"""
# H5 Walk-forward Baseline Report

## Executive Summary

Walk-forward predictions were analyzed from CSV only. The active model bundle
is not loaded by this baseline script.

AI_only HD3_avg: {ai.get('HD3_avg')} n={ai.get('n')} PF={ai.get('PF_HD3')}
AI_plus_drop HD3_avg: {aipd.get('HD3_avg')} n={aipd.get('n')} PF={aipd.get('PF_HD3')}
H5_full HD3_avg: {h5.get('HD3_avg')} n={h5.get('n')} PF={h5.get('PF_HD3')}
K_no_normal HD3_avg: {kn.get('HD3_avg')} n={kn.get('n')} PF={kn.get('PF_HD3')}
same-day random exclude H5 HD3_avg: {rand.get('HD3_avg')} PF={rand.get('PF_HD3')}
H5_full_minus_same_day_random: {h5_minus_rand}

## Answers

1. walk-forward predictions created:
yes, if 01_walk_forward_predictions.csv exists and this report was generated.

2. future labels in training:
Each prediction row comes from a run where train_end is before predict_start.
The build script trains only on rows with trade_date <= train_end.

3. high risk feature columns:
See outputs/h5_walk_forward_predictions/00_feature_columns_used.csv.
By default the build script refuses high-risk feature names.

4. active model delta:
See 05_active_vs_walk_forward_comparison.csv.

5. H5_full vs same-day random:
H5_full_minus_same_day_random={h5_minus_rand}.

6. K_no_normal:
See 04_k_no_normal_comparison.csv.

7. AI score bucket:
See 08_ai_score_bucket_performance.csv.

8. Primary change:
No. This is research-only. Primary should not change from this script alone.

9. PB20:
No reason to restore PB20 from this analysis.

10. Next:
If walk-forward AI retains edge, create a DB-backed model_predictions table
and store daily predictions without overwriting historical scores.
""")


if __name__ == "__main__":
    main()
