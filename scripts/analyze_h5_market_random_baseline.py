"""H5 Market Random Baseline Audit.

Research-only script. It does not modify DB rows, case definitions, UI, LINE
notifications, actual trade logs, Watchlist, or Intraday H5.

The audit compares H5_full Research entries with same-day market random
baselines and stepwise filter ablations using the same entry price, future
labels, holding days, and EST12 stop logic.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import random
import statistics
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

EST12_STOP_PCT = -12.0
EST12_STOP_RATE = -0.12
DEFAULT_HOLDING_DAYS = [1, 2, 3, 5, 7, 10]
DEFAULT_SEEDS = [0, 1, 2, 3, 4, 5, 10, 42, 99, 123]


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def round_value(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, digits)
    except Exception:
        return value


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def win_rate(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value > 0) / len(values) * 100.0


def profit_factor(values: list[float]) -> float | None:
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value <= 0))
    if losses <= 0:
        return 999.0 if wins > 0 else None
    return wins / losses


def max_drawdown(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


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


def get_entry_date(row: dict) -> date | None:
    raw = row.get("trade_date") or row.get("label_trade_date")
    if not raw:
        return None
    return parse_date(str(raw))


def get_code(row: dict) -> str:
    return str(row.get("code") or row.get("label_code") or "")


def get_entry_price(row: dict) -> float | None:
    return _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)


def raw_return(row: dict, hold: int) -> float | None:
    entry = get_entry_price(row)
    close = _to_float(row.get(f"future_close_{hold}d"), None)
    if entry is None or entry <= 0 or close is None:
        return None
    return (close / entry - 1.0) * 100.0


def est12_return(row: dict, hold: int) -> tuple[float | None, str]:
    entry = get_entry_price(row)
    if entry is None or entry <= 0:
        return None, "invalid_entry"
    stop_price = entry * (1.0 + EST12_STOP_RATE)
    last_close = None
    for day in range(1, hold + 1):
        low = _to_float(row.get(f"future_low_{day}d"), None)
        close = _to_float(row.get(f"future_close_{day}d"), None)
        if close is not None:
            last_close = close
        if low is not None and low <= stop_price:
            return EST12_STOP_PCT, "emergency_stop"
    if last_close is None:
        return None, "no_data"
    return (last_close / entry - 1.0) * 100.0, "time_stop"


def attach_returns(rows: list[dict], holding_days: list[int]) -> None:
    for row in rows:
        row["_entry_date"] = get_entry_date(row)
        row["_code"] = get_code(row)
        row["_entry_price"] = get_entry_price(row)
        for hold in holding_days:
            row[f"_raw_hd{hold}"] = raw_return(row, hold)
            ret, reason = est12_return(row, hold)
            row[f"_est12_hd{hold}"] = ret
            row[f"_est12_reason_hd{hold}"] = reason


def has_hd3(row: dict) -> bool:
    return row.get("_entry_date") is not None and row.get("_entry_price") and row.get("_raw_hd3") is not None


def volume_bucket(row: dict) -> str:
    value = _to_float(row.get("volume_ratio_20d"), None)
    if value is None:
        return "null"
    if value < 0.7:
        return "lt_0_7"
    if value < 1.0:
        return "0_7_to_1_0"
    if value < 1.5:
        return "1_0_to_1_5"
    if value < 2.0:
        return "1_5_to_2_0"
    if value < 3.0:
        return "2_0_to_3_0"
    return "gt_3_0"


def drop_bucket(row: dict) -> str:
    value = _to_float(row.get("drop_from_20d_high_pct"), None)
    if value is None:
        return "null"
    if value > -8:
        return "-5_to_-8"
    if value > -10:
        return "-8_to_-10"
    if value > -15:
        return "-10_to_-15"
    if value > -20:
        return "-15_to_-20"
    return "lte_-20"


def sector_bucket(row: dict) -> str:
    return str(row.get("sector") or "unknown")


def period_of(d: date, train_end: date, test_start: date) -> str | None:
    if d <= train_end:
        return "train"
    if d >= test_start:
        return "test"
    return None


def filter_period(rows: list[dict], period: str, train_end: date, test_start: date) -> list[dict]:
    if period == "all":
        return rows
    return [
        row for row in rows
        if row.get("_entry_date") and period_of(row["_entry_date"], train_end, test_start) == period
    ]


def passes_ai(row: dict) -> bool:
    prob = _to_float(row.get("signal_probability"), None)
    return prob is not None and prob >= 0.65


def passes_drop(row: dict) -> bool:
    drop = _to_float(row.get("drop_from_20d_high_pct"), None)
    return drop is not None and drop <= -8.0


def passes_stage(row: dict) -> bool:
    return str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}


def passes_no_panic(row: dict) -> bool:
    return str(row.get("market_regime") or "") != "panic_selloff"


def passes_overheat(row: dict) -> bool:
    return h5_overheat_score(row) <= 1


def passes_margin(row: dict) -> bool:
    margin = _to_float(row.get("margin_ratio"), None)
    if margin is None:
        return True
    return 3.0 <= margin <= 30.0


FILTERS: dict[str, Callable[[dict], bool]] = {
    "filter_zero_all": lambda r: True,
    "AI_only": passes_ai,
    "drop_only": passes_drop,
    "AI_plus_drop": lambda r: passes_ai(r) and passes_drop(r),
    "AI_plus_drop_stage": lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r),
    "AI_plus_drop_stage_no_panic": lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r),
    "AI_plus_drop_stage_no_panic_overheat": lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r),
    "H5_full_no_margin": lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r),
    "H5_full": lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r) and passes_margin(r),
}


def summarize_returns(rows: list[dict], strategy: str, period: str, path_type: str, holding_days: list[int], notes: str = "") -> dict:
    result: dict[str, Any] = {
        "strategy": strategy,
        "period": period,
        "path_type": path_type,
        "n": 0,
        "max_loss": None,
        "emergency_stop_rate": None,
        "notes": notes,
    }
    returns_by_hold: dict[int, list[float]] = {}
    for hold in holding_days:
        key = f"_{path_type}_hd{hold}"
        returns = [float(row[key]) for row in rows if row.get(key) is not None]
        returns_by_hold[hold] = returns
        result[f"HD{hold}_avg"] = avg(returns)
    hd3 = returns_by_hold.get(3, [])
    hd5 = returns_by_hold.get(5, [])
    hd7 = returns_by_hold.get(7, [])
    result["n"] = len(hd3)
    result["HD3_WR"] = win_rate(hd3)
    result["HD5_WR"] = win_rate(hd5)
    result["HD7_WR"] = win_rate(hd7)
    result["PF_HD3"] = profit_factor(hd3)
    result["maxDD_HD3"] = max_drawdown(hd3)
    result["max_loss"] = min(hd3) if hd3 else None
    if path_type == "est12" and rows:
        stop_count = sum(1 for row in rows if row.get("_est12_reason_hd3") == "emergency_stop")
        valid_count = sum(1 for row in rows if row.get("_est12_hd3") is not None)
        result["emergency_stop_rate"] = stop_count / valid_count * 100.0 if valid_count else None
    return result


def rows_for_strategy(rows: list[dict], strategy: str) -> list[dict]:
    predicate = FILTERS[strategy]
    return [row for row in rows if predicate(row)]


def build_strategy_matrix(rows: list[dict], holding_days: list[int], train_end: date, test_start: date) -> list[dict]:
    matrix: list[dict] = []
    for strategy in FILTERS:
        strategy_rows = rows_for_strategy(rows, strategy)
        for period in ("train", "test", "all"):
            period_rows = filter_period(strategy_rows, period, train_end, test_start)
            for path_type in ("raw", "est12"):
                matrix.append(summarize_returns(period_rows, strategy, period, path_type, holding_days, "filter ablation"))
    return matrix


def group_by_date(rows: list[dict]) -> dict[date, list[dict]]:
    grouped: dict[date, list[dict]] = defaultdict(list)
    for row in rows:
        d = row.get("_entry_date")
        if d:
            grouped[d].append(row)
    return grouped


def sample_same_day(
    universe_by_date: dict[date, list[dict]],
    h5_by_date: dict[date, list[dict]],
    seed: int,
    *,
    exclude_h5: bool,
) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    sampled: list[dict] = []
    skipped: list[dict] = []
    for d, h5_rows in h5_by_date.items():
        h5_codes = {row["_code"] for row in h5_rows}
        pool = list(universe_by_date.get(d, []))
        if exclude_h5:
            pool = [row for row in pool if row["_code"] not in h5_codes]
        n = len(h5_rows)
        if len(pool) < n:
            skipped.append({"entry_date": d.isoformat(), "h5_count": n, "pool_count": len(pool), "random_type": "same_day_exclude_h5" if exclude_h5 else "same_day"})
            continue
        sampled.extend(rng.sample(pool, n))
    return sampled, skipped


def sample_matched_bucket(
    universe: list[dict],
    h5_rows: list[dict],
    seed: int,
    bucket_fn: Callable[[dict], str],
    random_type: str,
) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    grouped: dict[tuple[date, str], list[dict]] = defaultdict(list)
    for item in universe:
        d = item.get("_entry_date")
        if d:
            grouped[(d, bucket_fn(item))].append(item)

    h5_grouped: dict[tuple[date, str], list[dict]] = defaultdict(list)
    for item in h5_rows:
        d = item.get("_entry_date")
        if d:
            h5_grouped[(d, bucket_fn(item))].append(item)

    sampled: list[dict] = []
    skipped: list[dict] = []
    for key, h5_group in h5_grouped.items():
        h5_codes = {item["_code"] for item in h5_group}
        pool = [item for item in grouped.get(key, []) if item["_code"] not in h5_codes]
        n = len(h5_group)
        if len(pool) < n:
            skipped.append({
                "entry_date": key[0].isoformat(),
                "bucket": key[1],
                "h5_count": n,
                "pool_count": len(pool),
                "random_type": random_type,
            })
            continue
        sampled.extend(rng.sample(pool, n))
    return sampled, skipped


def random_seed_rows(
    universe: list[dict],
    h5_rows: list[dict],
    seeds: list[int],
    holding_days: list[int],
    train_end: date,
    test_start: date,
) -> tuple[list[dict], dict[str, dict[str, list[dict]]], list[dict]]:
    universe_by_date = group_by_date(universe)
    h5_by_date = group_by_date(h5_rows)
    seed_results: list[dict] = []
    samples_by_type: dict[str, dict[str, list[dict]]] = defaultdict(dict)
    skipped_all: list[dict] = []

    samplers = [
        ("market_random_same_day", lambda seed: sample_same_day(universe_by_date, h5_by_date, seed, exclude_h5=False)),
        ("market_random_same_day_exclude_h5", lambda seed: sample_same_day(universe_by_date, h5_by_date, seed, exclude_h5=True)),
        ("same_sector_random", lambda seed: sample_matched_bucket(universe, h5_rows, seed, sector_bucket, "same_sector_random")),
        ("same_volume_bucket_random", lambda seed: sample_matched_bucket(universe, h5_rows, seed, volume_bucket, "same_volume_bucket_random")),
        ("same_drop_bucket_random", lambda seed: sample_matched_bucket(universe, h5_rows, seed, drop_bucket, "same_drop_bucket_random")),
    ]

    for random_type, sampler in samplers:
        samples_by_type[random_type] = {}
        for seed in seeds:
            sampled, skipped = sampler(seed)
            skipped_all.extend({"seed": seed, **item} for item in skipped)
            samples_by_type[random_type][str(seed)] = sampled
            for period in ("train", "test", "all"):
                period_rows = filter_period(sampled, period, train_end, test_start)
                for path_type in ("raw", "est12"):
                    summary = summarize_returns(period_rows, random_type, period, path_type, holding_days)
                    seed_results.append({
                        "random_type": random_type,
                        "seed": seed,
                        "period": period,
                        "path_type": path_type,
                        "n": summary["n"],
                        "HD3_avg": summary.get("HD3_avg"),
                        "HD5_avg": summary.get("HD5_avg"),
                        "HD7_avg": summary.get("HD7_avg"),
                        "HD3_WR": summary.get("HD3_WR"),
                        "PF_HD3": summary.get("PF_HD3"),
                        "maxDD_HD3": summary.get("maxDD_HD3"),
                    })
    return seed_results, samples_by_type, skipped_all


def mean_random_strategy_rows(seed_results: list[dict], holding_days: list[int]) -> list[dict]:
    out: list[dict] = []
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for item in seed_results:
        grouped[(item["random_type"], item["period"], item["path_type"])].append(item)
    for (random_type, period, path_type), records in grouped.items():
        row = {
            "strategy": f"{random_type}_mean",
            "period": period,
            "path_type": path_type,
            "n": avg([_to_float(r.get("n"), 0.0) or 0.0 for r in records]),
            "HD3_WR": avg([_to_float(r.get("HD3_WR"), None) for r in records if r.get("HD3_WR") is not None]),
            "PF_HD3": avg([_to_float(r.get("PF_HD3"), None) for r in records if r.get("PF_HD3") is not None]),
            "maxDD_HD3": avg([_to_float(r.get("maxDD_HD3"), None) for r in records if r.get("maxDD_HD3") is not None]),
            "max_loss": "",
            "emergency_stop_rate": "",
            "notes": "mean across random seeds",
        }
        for hold in holding_days:
            row[f"HD{hold}_avg"] = avg([_to_float(r.get(f"HD{hold}_avg"), None) for r in records if r.get(f"HD{hold}_avg") is not None])
        out.append(row)
    return out


def filter_ablation_rows(matrix: list[dict]) -> list[dict]:
    order = [
        "filter_zero_all",
        "AI_only",
        "drop_only",
        "AI_plus_drop",
        "AI_plus_drop_stage",
        "AI_plus_drop_stage_no_panic",
        "AI_plus_drop_stage_no_panic_overheat",
        "H5_full_no_margin",
        "H5_full",
    ]
    out: list[dict] = []
    for period in ("train", "test", "all"):
        rows = {
            item["strategy"]: item
            for item in matrix
            if item["period"] == period and item["path_type"] == "est12" and item["strategy"] in order
        }
        zero = rows.get("filter_zero_all")
        previous = None
        same_day = next(
            (item for item in matrix if item["strategy"] == "market_random_same_day_exclude_h5_mean" and item["period"] == period and item["path_type"] == "est12"),
            None,
        )
        for step in order:
            item = rows.get(step)
            if not item:
                continue
            hd3 = _to_float(item.get("HD3_avg"), None)
            prev_hd3 = _to_float(previous.get("HD3_avg"), None) if previous else None
            zero_hd3 = _to_float(zero.get("HD3_avg"), None) if zero else None
            random_hd3 = _to_float(same_day.get("HD3_avg"), None) if same_day else None
            out.append({
                "filter_step": step,
                "period": period,
                "n": item.get("n"),
                "HD3_avg": hd3,
                "HD3_WR": item.get("HD3_WR"),
                "PF_HD3": item.get("PF_HD3"),
                "delta_vs_previous": hd3 - prev_hd3 if hd3 is not None and prev_hd3 is not None else None,
                "delta_vs_filter_zero": hd3 - zero_hd3 if hd3 is not None and zero_hd3 is not None else None,
                "delta_vs_same_day_random": hd3 - random_hd3 if hd3 is not None and random_hd3 is not None else None,
            })
            previous = item
    return out


def same_day_comparison(h5_rows: list[dict], samples_by_type: dict[str, dict[str, list[dict]]], path_type: str = "est12") -> list[dict]:
    h5_by_date = group_by_date(h5_rows)
    sample_grouped: dict[int, dict[date, list[dict]]] = {}
    for seed, sampled in samples_by_type.get("market_random_same_day_exclude_h5", {}).items():
        sample_grouped[int(seed)] = group_by_date(sampled)

    out: list[dict] = []
    for d, day_h5 in sorted(h5_by_date.items()):
        h5_returns = [row[f"_{path_type}_hd3"] for row in day_h5 if row.get(f"_{path_type}_hd3") is not None]
        if not h5_returns:
            continue
        random_avgs = []
        for grouped in sample_grouped.values():
            rs = [row[f"_{path_type}_hd3"] for row in grouped.get(d, []) if row.get(f"_{path_type}_hd3") is not None]
            if rs:
                random_avgs.append(avg(rs))
        random_mean = avg(random_avgs)
        random_std = statistics.pstdev(random_avgs) if len(random_avgs) > 1 else None
        h5_avg = avg(h5_returns)
        out.append({
            "entry_date": d.isoformat(),
            "h5_count": len(day_h5),
            "h5_HD3_avg": h5_avg,
            "random_HD3_avg_mean": random_mean,
            "random_HD3_avg_std": random_std,
            "h5_minus_random": h5_avg - random_mean if h5_avg is not None and random_mean is not None else None,
            "h5_win_vs_random": h5_avg > random_mean if h5_avg is not None and random_mean is not None else None,
            "market_regime": day_h5[0].get("market_regime"),
            "index_return_if_available": "",
        })
    return out


def breakdown(rows_by_strategy: dict[str, list[dict]], bucket_name: str, bucket_fn: Callable[[dict], str], train_end: date, test_start: date) -> list[dict]:
    out: list[dict] = []
    for strategy, rows in rows_by_strategy.items():
        for period in ("train", "test", "all"):
            period_rows = filter_period(rows, period, train_end, test_start)
            grouped: dict[str, list[dict]] = defaultdict(list)
            for item in period_rows:
                grouped[bucket_fn(item)].append(item)
            for bucket, bucket_rows in sorted(grouped.items()):
                returns = [item["_est12_hd3"] for item in bucket_rows if item.get("_est12_hd3") is not None]
                out.append({
                    "strategy": strategy,
                    bucket_name: bucket,
                    "period": period,
                    "n": len(returns),
                    "HD3_avg": avg(returns),
                    "HD5_avg": avg([item["_est12_hd5"] for item in bucket_rows if item.get("_est12_hd5") is not None]),
                    "HD7_avg": avg([item["_est12_hd7"] for item in bucket_rows if item.get("_est12_hd7") is not None]),
                    "HD3_WR": win_rate(returns),
                    "PF_HD3": profit_factor(returns),
                })
    return out


def monthly_comparison(h5_rows: list[dict], random_same_day_rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for strategy, rows in (("H5_full", h5_rows), ("market_random_same_day_exclude_h5_mean_sample_seed0", random_same_day_rows)):
        for item in rows:
            d = item.get("_entry_date")
            if d:
                grouped[(strategy, d.strftime("%Y-%m"))].append(item)
    random_month_avg = {}
    for (strategy, month), rows in grouped.items():
        if strategy.startswith("market_random"):
            vals = [item["_est12_hd3"] for item in rows if item.get("_est12_hd3") is not None]
            random_month_avg[month] = avg(vals)
    for (strategy, month), rows in sorted(grouped.items()):
        vals = [item["_est12_hd3"] for item in rows if item.get("_est12_hd3") is not None]
        random_avg = random_month_avg.get(month)
        h5_minus = avg(vals) - random_avg if strategy == "H5_full" and avg(vals) is not None and random_avg is not None else None
        out.append({
            "strategy": strategy,
            "month": month,
            "n": len(vals),
            "HD3_avg": avg(vals),
            "HD3_total_return_sum": sum(vals) if vals else None,
            "HD3_WR": win_rate(vals),
            "random_baseline_HD3_avg": random_avg if strategy == "H5_full" else "",
            "h5_minus_random": h5_minus,
        })
    return out


def edge_decomposition(ablation: list[dict]) -> list[dict]:
    out: list[dict] = []
    components = [
        ("AI_only_vs_filter_zero", "AI_only", "filter_zero_all", "AI score alone"),
        ("drop_only_vs_filter_zero", "drop_only", "filter_zero_all", "20d drop alone"),
        ("AI_plus_drop_vs_drop_only", "AI_plus_drop", "drop_only", "AI added to drop"),
        ("stage_added", "AI_plus_drop_stage", "AI_plus_drop", "confirmed/strong_confirmed added"),
        ("panic_removed", "AI_plus_drop_stage_no_panic", "AI_plus_drop_stage", "panic_selloff removed"),
        ("overheat_added", "AI_plus_drop_stage_no_panic_overheat", "AI_plus_drop_stage_no_panic", "cool/mild overheat filter added"),
        ("margin_added", "H5_full", "H5_full_no_margin", "credit margin 3-30 added"),
        ("H5_full_vs_same_day_random", "H5_full", "market_random_same_day_exclude_h5_mean", "H5 full over same-day random"),
    ]
    for period in ("train", "test", "all"):
        rows = {item["filter_step"]: item for item in ablation if item["period"] == period}
        matrix_rows = {item["strategy"]: item for item in getattr(edge_decomposition, "_matrix", []) if item["period"] == period and item["path_type"] == "est12"}
        for component, left, right, interpretation in components:
            left_row = rows.get(left) or matrix_rows.get(left)
            right_row = rows.get(right) or matrix_rows.get(right)
            left_avg = _to_float(left_row.get("HD3_avg"), None) if left_row else None
            right_avg = _to_float(right_row.get("HD3_avg"), None) if right_row else None
            out.append({
                "component": component,
                "period": period,
                "delta_HD3_avg": left_avg - right_avg if left_avg is not None and right_avg is not None else None,
                "interpretation": interpretation,
            })
    return out


def get_strategy_row(matrix: list[dict], strategy: str, period: str = "all", path_type: str = "est12") -> dict | None:
    for item in matrix:
        if item["strategy"] == strategy and item["period"] == period and item["path_type"] == path_type:
            return item
    return None


def generate_report(matrix: list[dict], ablation: list[dict], same_day: list[dict], skipped: list[dict], entry_price_note: str) -> str:
    h5_all = get_strategy_row(matrix, "H5_full")
    zero_all = get_strategy_row(matrix, "filter_zero_all")
    rnd_all = get_strategy_row(matrix, "market_random_same_day_exclude_h5_mean")
    sector_all = get_strategy_row(matrix, "same_sector_random_mean")
    vol_all = get_strategy_row(matrix, "same_volume_bucket_random_mean")
    drop_all = get_strategy_row(matrix, "same_drop_bucket_random_mean")
    ai_all = get_strategy_row(matrix, "AI_only")
    drop_only_all = get_strategy_row(matrix, "drop_only")
    ai_drop_all = get_strategy_row(matrix, "AI_plus_drop")
    no_margin_all = get_strategy_row(matrix, "H5_full_no_margin")

    h5_minus_random = None
    if h5_all and rnd_all and h5_all.get("HD3_avg") is not None and rnd_all.get("HD3_avg") is not None:
        h5_minus_random = h5_all["HD3_avg"] - rnd_all["HD3_avg"]
    same_day_wins = [item for item in same_day if item.get("h5_win_vs_random") is True]
    same_day_count = len([item for item in same_day if item.get("random_HD3_avg_mean") is not None])
    same_day_win_rate = len(same_day_wins) / same_day_count * 100.0 if same_day_count else None

    def line(name: str, item: dict | None) -> str:
        if not item:
            return f"- {name}: no data"
        return (
            f"- {name}: n={round_value(item.get('n'))}, HD3_avg={round_value(item.get('HD3_avg'))}%, "
            f"WR={round_value(item.get('HD3_WR'))}%, PF={round_value(item.get('PF_HD3'))}"
        )

    return f"""
# H5 Market Random Baseline Audit Report

## Scope

This is an analysis-only report. It does not modify Primary, DB case definitions, UI, LINE notification, actual_trade_logs, Watchlist, or Intraday H5.

Universe note:
The random universe uses the same screening/candidate loader as existing H5 research: stock_rebound_labels joined to stock_feature_snapshots via _load_candidates_v2, restricted to tradeable drop-candidate snapshots with available future labels. This is the H5 screening universe, not a raw all-listed-stock universe.

Entry price:
{entry_price_note}

## Main EST12 HD3 comparison, all period

{line('filter_zero_all', zero_all)}
{line('market_random_same_day_exclude_h5_mean', rnd_all)}
{line('same_sector_random_mean', sector_all)}
{line('same_volume_bucket_random_mean', vol_all)}
{line('same_drop_bucket_random_mean', drop_all)}
{line('AI_only', ai_all)}
{line('drop_only', drop_only_all)}
{line('AI_plus_drop', ai_drop_all)}
{line('H5_full_no_margin', no_margin_all)}
{line('H5_full', h5_all)}

## Required answers

1. H5_full vs filter_zero_all:
H5_full is better if its HD3_avg/PF exceed filter_zero_all. See 01_strategy_performance_matrix.csv.

2. H5_full vs same-day random:
All-period H5_full minus same-day exclude-H5 random HD3_avg = {round_value(h5_minus_random)} percentage points.

3. H5_full vs same-day exclude-H5 random:
This is the primary random baseline. Same-day date win rate against seed-mean random = {round_value(same_day_win_rate)}%.

4. H5_full vs same-sector random:
See same_sector_random_mean in the matrix and 07_sector_breakdown.csv.

5. H5_full vs same-volume bucket random:
See same_volume_bucket_random_mean in the matrix and 06_volume_bucket_breakdown.csv.

6. H5_full vs same-drop bucket random:
See same_drop_bucket_random_mean in the matrix.

7. AI_only effect:
AI_only is separated in 03_filter_ablation.csv.

8. drop_only effect:
drop_only is separated in 03_filter_ablation.csv.

9. AI_plus_drop effect:
AI_plus_drop is compared against AI_only and drop_only in 03_filter_ablation.csv and 09_h5_edge_decomposition.csv.

10. stage / no_panic / overheat / margin:
Each is represented as a step in 03_filter_ablation.csv.

11. Is H5 only market regime?
If H5_full remains above same-day random, same-sector random, and same-volume/drop bucket random, the edge is not fully explained by date/regime alone. If not, the edge is likely market-date dependent.

12. Are H5 signal dates broadly rebound-friendly?
04_same_day_random_comparison.csv shows whether random stocks on the same H5 dates also rebound.

13. Train/test consistency:
Use period=train and period=test rows in 01_strategy_performance_matrix.csv and 03_filter_ablation.csv.

14. Monthly dependence:
08_monthly_comparison.csv shows whether excess return is concentrated in a few months.

## Data quality

Random skipped group count: {len(skipped)}
Skipped rows are mostly caused by insufficient same-day same-bucket universe after excluding H5 codes.

## Next checks

- If H5_full beats same-day random in train and test: keep trusting H5 entry and return to Live Limited selector repair.
- If H5_full is close to same-day random: treat H5 edge as market-date dependent and prioritize regime/date filters.
- If drop_only explains most of the edge: simplify H5 entry research around drawdown buckets.
- If AI_plus_drop materially beats drop_only: AI signal adds real information.
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_market_random_baseline")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="latest")
    parser.add_argument("--random-seeds", default=",".join(str(s) for s in DEFAULT_SEEDS))
    parser.add_argument("--holding-days", default=",".join(str(d) for d in DEFAULT_HOLDING_DAYS))
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    test_start = parse_date(args.test_start)
    seeds = [int(s.strip()) for s in args.random_seeds.split(",") if s.strip()]
    holding_days = [int(s.strip()) for s in args.holding_days.split(",") if s.strip()]
    max_hold = max(holding_days)

    sb = _build_supabase()
    load_end = date.today() if args.test_end == "latest" else parse_date(args.test_end)
    logger.info("Loading candidate universe %s..%s", train_start, load_end)
    rows = _load_candidates_v2(sb, train_start, load_end)
    attach_returns(rows, holding_days)
    universe = [row for row in rows if has_hd3(row)]
    if not universe:
        raise RuntimeError("No evaluable universe rows loaded.")
    latest_date = max(row["_entry_date"] for row in universe if row.get("_entry_date"))
    logger.info("Universe rows=%d latest_date=%s", len(universe), latest_date)

    h5_rows = rows_for_strategy(universe, "H5_full")
    logger.info("H5_full rows=%d", len(h5_rows))
    seed_results, samples_by_type, skipped = random_seed_rows(universe, h5_rows, seeds, holding_days, train_end, test_start)

    strategy_matrix = build_strategy_matrix(universe, holding_days, train_end, test_start)
    random_mean_rows = mean_random_strategy_rows(seed_results, holding_days)
    strategy_matrix.extend(random_mean_rows)

    write_csv(output_dir / "01_strategy_performance_matrix.csv", strategy_matrix)
    write_csv(output_dir / "02_random_seed_results.csv", seed_results)

    ablation = filter_ablation_rows(strategy_matrix)
    write_csv(output_dir / "03_filter_ablation.csv", ablation)

    same_day_rows = same_day_comparison(h5_rows, samples_by_type, "est12")
    write_csv(output_dir / "04_same_day_random_comparison.csv", same_day_rows)

    rows_by_strategy = {
        "filter_zero_all": universe,
        "H5_full": h5_rows,
        "market_random_same_day_exclude_h5_seed0": samples_by_type.get("market_random_same_day_exclude_h5", {}).get(str(seeds[0]), []),
    }
    write_csv(output_dir / "05_regime_breakdown.csv", breakdown(rows_by_strategy, "regime", lambda r: str(r.get("market_regime") or "unknown"), train_end, test_start))
    write_csv(output_dir / "06_volume_bucket_breakdown.csv", breakdown(rows_by_strategy, "volume_bucket", volume_bucket, train_end, test_start))
    write_csv(output_dir / "07_sector_breakdown.csv", breakdown(rows_by_strategy, "sector", sector_bucket, train_end, test_start))
    write_csv(output_dir / "08_monthly_comparison.csv", monthly_comparison(h5_rows, rows_by_strategy["market_random_same_day_exclude_h5_seed0"]))

    edge_decomposition._matrix = strategy_matrix  # type: ignore[attr-defined]
    write_csv(output_dir / "09_h5_edge_decomposition.csv", edge_decomposition(ablation))
    write_csv(output_dir / "11_random_skip_log.csv", skipped)

    entry_price_note = "entry_price uses stock_rebound_labels.entry_price when available, falling back to snapshot close. Random rows use the same definition."
    write_text(output_dir / "10_market_random_baseline_report.txt", generate_report(strategy_matrix, ablation, same_day_rows, skipped, entry_price_note))

    logger.info("Wrote outputs to %s", output_dir)


if __name__ == "__main__":
    main()
