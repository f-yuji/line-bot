"""Analyze H5 return distribution and right-tail contribution.

Research-only script. It writes CSV/TXT files under outputs/ and does not
modify Primary, DB case definitions, UI, notifications, actual_trade_logs,
Watchlist, or Intraday H5.
"""
from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from dotenv import load_dotenv

from analyze_h5_breakeven_escape import (  # noqa: E402
    EST12_STOP_MULT,
    EST12_STOP_PCT,
    MAX_HOLD,
    _avg,
    _build_dataset,
    _build_supabase,
    _close_ret,
    _d,
    _first_est12_day,
    _load_candidates_v2,
    _median,
    _pct,
    _pf,
    _ret,
    _round,
    _to_float,
    _wr,
)

load_dotenv()

HORIZON_DAYS = (3, 5, 7, 10)
RANDOM_SEEDS = (0, 1, 2, 3, 4, 5, 10, 42, 99, 123)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _round(row.get(k)) for k in keys})


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _norm_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    s = str(value or "").strip().lower()
    return s in {"1", "1.0", "true", "yes", "y"}


def _live_selected_keys(path: Path) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if not _norm_bool(row.get("selected_by_live_limited")):
                continue
            code = _norm_code(row.get("code"))
            trade_date = str(row.get("entry_date") or row.get("trade_date") or "").strip()
            if code and trade_date:
                keys.add((code, trade_date))
    return keys


def _max_high_return(path: list[dict], entry: float, hold: int) -> float | None:
    highs = [item.get("high") for item in path if 1 <= int(item["day"]) <= hold and item.get("high") is not None]
    return _ret(max(highs), entry) if highs else None


def _min_low_return(path: list[dict], entry: float, hold: int) -> float | None:
    lows = [item.get("low") for item in path if 1 <= int(item["day"]) <= hold and item.get("low") is not None]
    return _ret(min(lows), entry) if lows else None


def _est12_adjusted_return(path: list[dict], entry: float, hold: int) -> tuple[float | None, int | None]:
    est_day = _first_est12_day(path, entry, 1, hold)
    if est_day is not None:
        return EST12_STOP_PCT, est_day
    return _close_ret(path, entry, hold), None


def _std(values: list[float]) -> float | None:
    return pstdev(values) if len(values) >= 2 else None


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    pos = (len(vals) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return vals[int(pos)]
    return vals[lo] * (hi - pos) + vals[hi] * (pos - lo)


def _skewness(values: list[float]) -> float | None:
    if len(values) < 3:
        return None
    mean = _avg(values)
    sd = _std(values)
    if mean is None or not sd:
        return None
    return sum(((v - mean) / sd) ** 3 for v in values) / len(values)


def _kurtosis(values: list[float]) -> float | None:
    if len(values) < 4:
        return None
    mean = _avg(values)
    sd = _std(values)
    if mean is None or not sd:
        return None
    return sum(((v - mean) / sd) ** 4 for v in values) / len(values) - 3.0


def _build_return_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    out: list[dict] = []
    skipped: list[dict] = []
    for r in rows:
        path = r.get("_path")
        entry = _to_float(r.get("entry_price"), None)
        if not path or entry is None or entry <= 0:
            skipped.append({"reason": "missing_entry_price", "count": 1})
            continue
        code = _norm_code(r.get("code"))
        trade_date = str(r.get("trade_date") or "")
        group_names = [r["group"]]
        if r["group"] == "H5_full":
            group_names.extend(["Research_ALL", "Primary_equivalent"])
        # Keep one Research_ALL/Primary row only even though imported rows already include them.
        if r["group"] in {"Research_ALL", "Primary_equivalent"}:
            continue
        for group in dict.fromkeys(group_names):
            base = {
                "group": group,
                "period": r.get("period"),
                "code": code,
                "name": r.get("name"),
                "trade_date": trade_date,
                "entry_price": entry,
                "signal_probability": r.get("signal_probability"),
                "signal_stage": r.get("signal_stage"),
                "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
                "market_regime": r.get("market_regime"),
                "overheat_score": r.get("overheat_score"),
                "margin_ratio": r.get("margin_ratio"),
                "volume_ratio": r.get("volume_ratio"),
                "sector": r.get("sector"),
                "is_live_limited": False,
                "is_actual_entry": False,
                "is_skipped": False,
                "is_missed": False,
            }
            for hold in HORIZON_DAYS:
                raw = _close_ret(path, entry, hold)
                est, est_day = _est12_adjusted_return(path, entry, hold)
                max_high = _max_high_return(path, entry, hold)
                min_low = _min_low_return(path, entry, hold)
                if raw is None:
                    skipped.append({"reason": f"missing_future_close_{hold}d", "count": 1})
                base[f"HD{hold}"] = raw
                base[f"HD{hold}_EST12"] = est
                base[f"MAX_HIGH_{hold}D"] = max_high
                base[f"MIN_LOW_{hold}D"] = min_low
                base[f"EST12_DAY_{hold}D"] = est_day
            out.append(base)
    return out, skipped


def _build_audit_return_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for src in csv.DictReader(f):
            code = _norm_code(src.get("code"))
            trade_date = str(src.get("entry_date") or "").strip()
            period = str(src.get("period") or "").strip()
            if not period:
                # The audit dataset itself does not store period per row. Infer from date.
                try:
                    period = "train" if _d(trade_date) <= _d("2024-12-31") else "test"
                except Exception:
                    period = "unknown"
            selected = _norm_bool(src.get("selected_by_live_limited"))
            common = {
                "period": period,
                "code": code,
                "name": src.get("name"),
                "trade_date": trade_date,
                "entry_price": _to_float(src.get("entry_price"), None),
                "signal_probability": _to_float(src.get("signal_probability"), None),
                "signal_stage": src.get("signal_stage"),
                "drop_from_20d_high_pct": _to_float(src.get("drop_from_20d_high_pct"), None),
                "market_regime": src.get("market_regime"),
                "overheat_score": _to_float(src.get("overheat_score"), None),
                "margin_ratio": _to_float(src.get("margin_ratio"), None),
                "volume_ratio": _to_float(src.get("volume_ratio"), None),
                "sector": src.get("sector"),
                "is_live_limited": selected,
                "is_actual_entry": False,
                "is_skipped": False,
                "is_missed": False,
            }
            for hold in HORIZON_DAYS:
                common[f"HD{hold}"] = _to_float(src.get(f"hd{hold}_ret_raw"), None)
                common[f"HD{hold}_EST12"] = _to_float(src.get(f"hd{hold}_ret_est12"), None) if hold == 3 else _to_float(src.get(f"hd{hold}_ret_raw"), None)
                common[f"MAX_HIGH_{hold}D"] = None
                common[f"MIN_LOW_{hold}D"] = None
                common[f"EST12_DAY_{hold}D"] = None
            groups = ["Research_Audit", "Live_Limited" if selected else "Not_Selected_Audit"]
            for group in groups:
                row = dict(common)
                row["group"] = group
                out.append(row)
    return out


def _values(rows: list[dict], horizon: str) -> list[float]:
    vals: list[float] = []
    for r in rows:
        v = _to_float(r.get(horizon), None)
        if v is not None and math.isfinite(v):
            vals.append(v)
    return vals


def _period_subset(rows: list[dict], group: str, period: str) -> list[dict]:
    return [r for r in rows if r["group"] == group and (period == "all" or r["period"] == period)]


def _distribution_summary(rows: list[dict], periods: list[str], horizons: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                vals = _values(subset, horizon)
                out.append({
                    "group": group,
                    "period": period,
                    "horizon": horizon,
                    "n": len(vals),
                    "avg_return_pct": _avg(vals),
                    "median_return_pct": _median(vals),
                    "win_rate": _wr(vals),
                    "profit_factor": _pf(vals),
                    "std_return_pct": _std(vals),
                    "skewness": _skewness(vals),
                    "kurtosis": _kurtosis(vals),
                    "min_return_pct": min(vals, default=None),
                    "max_return_pct": max(vals, default=None),
                    "p01": _quantile(vals, 0.01),
                    "p05": _quantile(vals, 0.05),
                    "p10": _quantile(vals, 0.10),
                    "p25": _quantile(vals, 0.25),
                    "p50": _quantile(vals, 0.50),
                    "p75": _quantile(vals, 0.75),
                    "p90": _quantile(vals, 0.90),
                    "p95": _quantile(vals, 0.95),
                    "p99": _quantile(vals, 0.99),
                    "positive_n": sum(1 for v in vals if v > 0),
                    "negative_n": sum(1 for v in vals if v < 0),
                    "zero_n": sum(1 for v in vals if v == 0),
                    "est12_count": sum(1 for r in subset if horizon.startswith("HD") and not horizon.endswith("EST12") and r.get(f"EST12_DAY_{horizon[2:]}D")),
                    "est12_rate": _pct(sum(1 for r in subset if horizon.startswith("HD") and not horizon.endswith("EST12") and r.get(f"EST12_DAY_{horizon[2:]}D")), len(vals)),
                })
    return out


def _top_n_count(n: int, pct: float) -> int:
    return max(1, math.ceil(n * pct / 100.0)) if n else 0


def _tail_contribution(rows: list[dict], periods: list[str], horizons: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                vals = sorted(_values(subset, horizon), reverse=True)
                if not vals:
                    continue
                positives = [v for v in vals if v > 0]
                losses = [v for v in vals if v < 0]
                total_profit = sum(positives)
                total_loss = sum(losses)
                net = sum(vals)
                row = {
                    "group": group,
                    "period": period,
                    "horizon": horizon,
                    "n": len(vals),
                    "total_profit_sum": total_profit,
                    "total_loss_sum": total_loss,
                    "net_return_sum": net,
                }
                for pct in (1, 5, 10, 20):
                    k = _top_n_count(len(vals), pct)
                    top_sum = sum(vals[:k])
                    row[f"top_{pct}pct_n"] = k
                    row[f"top_{pct}pct_profit_sum"] = top_sum
                    row[f"top_{pct}pct_share_of_positive_profit"] = (top_sum / total_profit * 100.0) if total_profit else None
                    row[f"top_{pct}pct_share_of_net_return"] = (top_sum / net * 100.0) if net else None
                asc = sorted(vals)
                abs_losses = abs(total_loss)
                for pct in (1, 5, 10):
                    k = _top_n_count(len(vals), pct)
                    loss_sum = sum(v for v in asc[:k] if v < 0)
                    row[f"bottom_{pct}pct_loss_sum"] = loss_sum
                    row[f"bottom_{pct}pct_loss_share"] = (abs(loss_sum) / abs_losses * 100.0) if abs_losses else None
                top5_net = row.get("top_5pct_share_of_net_return")
                top5_pos = row.get("top_5pct_share_of_positive_profit")
                if (top5_net is not None and top5_net >= 50) or (top5_pos is not None and top5_pos >= 40):
                    judgment = "RIGHT_TAIL_HEAVY"
                elif top5_net is not None and top5_net >= 25:
                    judgment = "MODERATE_TAIL"
                else:
                    judgment = "BROAD_EDGE"
                row["right_tail_dependency_judgment"] = judgment
                out.append(row)
    return out


def _rank_rows(rows: list[dict], periods: list[str], horizons: list[str], reverse: bool, limit: int = 50) -> list[dict]:
    out: list[dict] = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                valid = [r for r in subset if _to_float(r.get(horizon), None) is not None]
                valid = sorted(valid, key=lambda r: _to_float(r.get(horizon), 0.0) or 0.0, reverse=reverse)
                for idx, r in enumerate(valid[:limit], 1):
                    hold = "".join(ch for ch in horizon if ch.isdigit())
                    hold_day = int(hold) if hold else 10
                    out.append({
                        "rank": idx,
                        "group": group,
                        "period": period,
                        "horizon": horizon,
                        "code": r.get("code"),
                        "name": r.get("name"),
                        "trade_date": r.get("trade_date"),
                        "entry_price": r.get("entry_price"),
                        "exit_price": None,
                        "return_pct": r.get(horizon),
                        "max_high_return_pct": r.get(f"MAX_HIGH_{hold_day}D"),
                        "min_low_return_pct": r.get(f"MIN_LOW_{hold_day}D"),
                        "signal_probability": r.get("signal_probability"),
                        "signal_stage": r.get("signal_stage"),
                        "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
                        "market_regime": r.get("market_regime"),
                        "overheat_score": r.get("overheat_score"),
                        "margin_ratio": r.get("margin_ratio"),
                        "volume_ratio": r.get("volume_ratio"),
                        "sector": r.get("sector"),
                        "is_live_limited": r.get("is_live_limited"),
                        "is_actual_entry": r.get("is_actual_entry"),
                        "is_skipped": r.get("is_skipped"),
                        "is_missed": r.get("is_missed"),
                        "est12_trigger_day": r.get(f"EST12_DAY_{hold_day}D"),
                        "worst_drawdown_pct": r.get(f"MIN_LOW_{hold_day}D"),
                        "notes": "",
                    })
    return out


def _tail_removed(rows: list[dict], periods: list[str], horizons: list[str]) -> list[dict]:
    out: list[dict] = []
    scenarios = [("full_distribution", 0), ("remove_top_1pct", 1), ("remove_top_3pct", 3), ("remove_top_5pct", 5), ("remove_top_10pct", 10)]
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                valid = [r for r in subset if _to_float(r.get(horizon), None) is not None]
                valid_sorted = sorted(valid, key=lambda r: _to_float(r.get(horizon), 0.0) or 0.0, reverse=True)
                full_vals = [_to_float(r.get(horizon), 0.0) or 0.0 for r in valid_sorted]
                full_avg = _avg(full_vals)
                full_net = sum(full_vals)
                for scenario, pct in scenarios:
                    k = _top_n_count(len(valid_sorted), pct) if pct else 0
                    remaining = valid_sorted[k:]
                    vals = [_to_float(r.get(horizon), 0.0) or 0.0 for r in remaining]
                    avg = _avg(vals)
                    net = sum(vals) if vals else None
                    out.append({
                        "scenario": scenario,
                        "group": group,
                        "period": period,
                        "horizon": horizon,
                        "n_remaining": len(vals),
                        "avg_return_pct": avg,
                        "median_return_pct": _median(vals),
                        "win_rate": _wr(vals),
                        "profit_factor": _pf(vals),
                        "net_return_sum": net,
                        "delta_avg_vs_full": (avg - full_avg) if avg is not None and full_avg is not None else None,
                        "delta_net_vs_full": (net - full_net) if net is not None else None,
                        "judgment": "COLLAPSE" if avg is not None and avg <= 0 and scenario != "full_distribution" else "OK",
                    })
                if valid_sorted:
                    seed_avgs = []
                    remove_n = max(1, math.ceil(len(valid_sorted) * 0.05))
                    for seed in RANDOM_SEEDS:
                        rng = random.Random(seed)
                        idxs = set(rng.sample(range(len(valid_sorted)), min(remove_n, len(valid_sorted))))
                        vals = [_to_float(r.get(horizon), 0.0) or 0.0 for i, r in enumerate(valid_sorted) if i not in idxs]
                        seed_avgs.append(_avg(vals) or 0.0)
                    avg = _avg(seed_avgs)
                    out.append({
                        "scenario": "remove_random_5pct_mean",
                        "group": group,
                        "period": period,
                        "horizon": horizon,
                        "n_remaining": len(valid_sorted) - remove_n,
                        "avg_return_pct": avg,
                        "median_return_pct": None,
                        "win_rate": None,
                        "profit_factor": None,
                        "net_return_sum": None,
                        "delta_avg_vs_full": (avg - full_avg) if avg is not None and full_avg is not None else None,
                        "delta_net_vs_full": None,
                        "judgment": "REFERENCE",
                    })
    return out


def _threshold_distribution(rows: list[dict], periods: list[str], horizons: list[str]) -> list[dict]:
    out = []
    pos_thresholds = (1, 2, 3, 5, 10, 15, 20)
    neg_thresholds = (-1, -2, -3, -5, -10, -12)
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                vals = _values(subset, horizon)
                row = {"group": group, "period": period, "horizon": horizon, "n": len(vals)}
                for t in pos_thresholds:
                    c = sum(1 for v in vals if v > t)
                    row[f"gt_{t}pct_n"] = c
                    row[f"gt_{t}pct_rate"] = _pct(c, len(vals))
                for t in neg_thresholds:
                    c = sum(1 for v in vals if v < t)
                    row[f"lt_minus{abs(t)}pct_n"] = c
                    row[f"lt_minus{abs(t)}pct_rate"] = _pct(c, len(vals))
                out.append(row)
    return out


def _quantile_distribution(rows: list[dict], periods: list[str], horizons: list[str]) -> list[dict]:
    out = []
    for group in sorted({r["group"] for r in rows}):
        for period in periods:
            subset = _period_subset(rows, group, period)
            for horizon in horizons:
                valid = [r for r in subset if _to_float(r.get(horizon), None) is not None]
                valid = sorted(valid, key=lambda r: _to_float(r.get(horizon), 0.0) or 0.0)
                vals_all = [_to_float(r.get(horizon), 0.0) or 0.0 for r in valid]
                net = sum(vals_all)
                positive = sum(v for v in vals_all if v > 0)
                if not valid:
                    continue
                bucket_count = 20
                for idx in range(bucket_count):
                    start = math.floor(len(valid) * idx / bucket_count)
                    end = math.floor(len(valid) * (idx + 1) / bucket_count)
                    part = valid[start:end]
                    vals = [_to_float(r.get(horizon), 0.0) or 0.0 for r in part]
                    codes = ",".join(str(r.get("code")) for r in sorted(part, key=lambda r: _to_float(r.get(horizon), 0.0) or 0.0, reverse=True)[:5])
                    out.append({
                        "group": group,
                        "period": period,
                        "horizon": horizon,
                        "quantile_bucket": f"Q{idx + 1:02d}_{idx * 5}_{(idx + 1) * 5}pct",
                        "n": len(vals),
                        "avg_return_pct": _avg(vals),
                        "median_return_pct": _median(vals),
                        "min_return_pct": min(vals, default=None),
                        "max_return_pct": max(vals, default=None),
                        "return_sum": sum(vals) if vals else None,
                        "share_of_total_net_return": (sum(vals) / net * 100.0) if net else None,
                        "share_of_positive_profit": (sum(v for v in vals if v > 0) / positive * 100.0) if positive else None,
                        "representative_codes": codes,
                    })
    return out


def _group_comparison(dist: list[dict], tail: list[dict], thresh: list[dict]) -> list[dict]:
    tail_map = {(r["group"], r["period"], r["horizon"]): r for r in tail}
    thresh_map = {(r["group"], r["period"], r["horizon"]): r for r in thresh}
    out = []
    for row in dist:
        if row["horizon"].startswith("MAX") or row["horizon"].startswith("MIN"):
            continue
        key = (row["group"], row["period"], row["horizon"])
        t = tail_map.get(key, {})
        th = thresh_map.get(key, {})
        out.append({
            "group": row["group"],
            "period": row["period"],
            "horizon": row["horizon"],
            "n": row["n"],
            "avg_return_pct": row["avg_return_pct"],
            "median_return_pct": row["median_return_pct"],
            "win_rate": row["win_rate"],
            "pf": row["profit_factor"],
            "top_5pct_share_of_net_return": t.get("top_5pct_share_of_net_return"),
            "top_10pct_share_of_net_return": t.get("top_10pct_share_of_net_return"),
            "gt_5pct_rate": th.get("gt_5pct_rate"),
            "gt_10pct_rate": th.get("gt_10pct_rate"),
            "lt_minus5pct_rate": th.get("lt_minus5pct_rate"),
            "lt_minus10pct_rate": th.get("lt_minus10pct_rate"),
            "max_return_pct": row["max_return_pct"],
            "min_return_pct": row["min_return_pct"],
            "right_tail_judgment": t.get("right_tail_dependency_judgment"),
            "operational_judgment": "wide_small_lot" if t.get("right_tail_dependency_judgment") == "RIGHT_TAIL_HEAVY" else "selection_possible",
        })
    return out


def _live_vs_research(rows: list[dict], horizons: list[str]) -> list[dict]:
    out = []
    for period in ("train", "test", "all"):
        research = _period_subset(rows, "Research_Audit", period) or _period_subset(rows, "Research_ALL", period)
        live = _period_subset(rows, "Live_Limited", period)
        for horizon in horizons:
            rvals = _values(research, horizon)
            lvals = _values(live, horizon)
            research_valid = [r for r in research if _to_float(r.get(horizon), None) is not None]
            top_k = _top_n_count(len(research_valid), 5)
            top_research = sorted(research_valid, key=lambda r: _to_float(r.get(horizon), 0.0) or 0.0, reverse=True)[:top_k]
            top_keys = {(r["code"], r["trade_date"]) for r in top_research}
            live_keys = {(r["code"], r["trade_date"]) for r in live}
            captured = top_keys & live_keys
            missed = top_keys - live_keys
            out.append({
                "period": period,
                "horizon": horizon,
                "research_n": len(rvals),
                "live_n": len(lvals),
                "research_avg": _avg(rvals),
                "live_avg": _avg(lvals),
                "research_median": _median(rvals),
                "live_median": _median(lvals),
                "research_gt5_rate": _pct(sum(1 for v in rvals if v > 5), len(rvals)),
                "live_gt5_rate": _pct(sum(1 for v in lvals if v > 5), len(lvals)),
                "research_gt10_rate": _pct(sum(1 for v in rvals if v > 10), len(rvals)),
                "live_gt10_rate": _pct(sum(1 for v in lvals if v > 10), len(lvals)),
                "research_top5pct_max": max([_to_float(r.get(horizon), 0.0) or 0.0 for r in top_research], default=None),
                "live_top5pct_max": max(lvals, default=None),
                "live_captured_top_winners_n": len(captured),
                "live_missed_top_winners_n": len(missed),
                "live_missed_top_winners_codes": ",".join(sorted(code for code, _ in missed)[:50]),
                "live_tail_capture_rate": _pct(len(captured), len(top_keys)),
                "judgment": "LIVE_MISSES_RIGHT_TAIL" if top_keys and _pct(len(captured), len(top_keys)) < 25 else "LIVE_CAPTURES_TAIL",
            })
    return out


def _monthly_tail(rows: list[dict], horizons: list[str]) -> list[dict]:
    out = []
    for group in sorted({r["group"] for r in rows}):
        group_rows = [r for r in rows if r["group"] == group]
        months = sorted({r["trade_date"][:7] for r in group_rows})
        for month in months:
            subset = [r for r in group_rows if r["trade_date"].startswith(month)]
            for horizon in horizons:
                vals = _values(subset, horizon)
                if not vals:
                    continue
                sorted_vals = sorted(vals, reverse=True)
                k = _top_n_count(len(sorted_vals), 10)
                net = sum(vals)
                top_sum = sum(sorted_vals[:k])
                out.append({
                    "month": month,
                    "group": group,
                    "horizon": horizon,
                    "n": len(vals),
                    "avg_return_pct": _avg(vals),
                    "median_return_pct": _median(vals),
                    "win_rate": _wr(vals),
                    "pf": _pf(vals),
                    "max_return_pct": max(vals),
                    "gt5_count": sum(1 for v in vals if v > 5),
                    "gt10_count": sum(1 for v in vals if v > 10),
                    "top_10pct_share_of_net_return": (top_sum / net * 100.0) if net else None,
                    "right_tail_present": any(v > 10 for v in vals),
                    "notes": "",
                })
    return out


def _left_tail(dist: list[dict], thresh: list[dict], losers: list[dict]) -> list[dict]:
    th_map = {(r["group"], r["period"], r["horizon"]): r for r in thresh}
    out = []
    for row in dist:
        if row["horizon"].startswith("MAX") or row["horizon"].startswith("MIN"):
            continue
        key = (row["group"], row["period"], row["horizon"])
        th = th_map.get(key, {})
        worst = next((r for r in losers if r["group"] == row["group"] and r["period"] == row["period"] and r["horizon"] == row["horizon"] and str(r.get("rank")) == "1"), {})
        out.append({
            "group": row["group"],
            "period": row["period"],
            "horizon": row["horizon"],
            "n": row["n"],
            "lt_minus3_rate": th.get("lt_minus3pct_rate"),
            "lt_minus5_rate": th.get("lt_minus5pct_rate"),
            "lt_minus10_rate": th.get("lt_minus10pct_rate"),
            "est12_rate": row.get("est12_rate"),
            "bottom_5pct_avg": row.get("p05"),
            "worst_code": worst.get("code"),
            "worst_return": worst.get("return_pct"),
            "left_tail_judgment": "HEAVY_LEFT_TAIL" if (th.get("lt_minus10pct_rate") or 0) > 5 else "ACCEPTABLE",
        })
    return out


def _actual_vs_skipped() -> list[dict]:
    return [
        {
            "group": "actual_entry",
            "period": "all",
            "horizon": "HD3",
            "n": 0,
            "avg_return_pct": None,
            "median_return_pct": None,
            "win_rate": None,
            "max_return_pct": None,
            "gt5_rate": None,
            "gt10_rate": None,
            "tail_contribution": None,
            "codes_top_winners": "",
            "judgment": "sample insufficient: actual/skipped H5 linkage not available in this script",
        },
        {
            "group": "skipped",
            "period": "all",
            "horizon": "HD3",
            "n": 0,
            "avg_return_pct": None,
            "median_return_pct": None,
            "win_rate": None,
            "max_return_pct": None,
            "gt5_rate": None,
            "gt10_rate": None,
            "tail_contribution": None,
            "codes_top_winners": "",
            "judgment": "sample insufficient: start logging missed H5 candidates",
        },
    ]


def _report(
    *,
    dist: list[dict],
    tail: list[dict],
    removed: list[dict],
    thresh: list[dict],
    live: list[dict],
    actual: list[dict],
    start: date,
    end: date,
    score_source: str,
) -> str:
    def find(rows: list[dict], **kwargs) -> dict:
        return next((r for r in rows if all(r.get(k) == v for k, v in kwargs.items())), {})

    d3 = find(dist, group="Research_ALL", period="all", horizon="HD3")
    d5 = find(dist, group="Research_ALL", period="all", horizon="HD5")
    d7 = find(dist, group="Research_ALL", period="all", horizon="HD7")
    d10 = find(dist, group="Research_ALL", period="all", horizon="HD10")
    tail3 = find(tail, group="Research_ALL", period="all", horizon="HD3")
    rem5 = find(removed, scenario="remove_top_5pct", group="Research_ALL", period="all", horizon="HD3")
    rem10 = find(removed, scenario="remove_top_10pct", group="Research_ALL", period="all", horizon="HD3")
    th3 = find(thresh, group="Research_ALL", period="all", horizon="HD3")
    live3 = find(live, period="all", horizon="HD3")

    avg = d3.get("avg_return_pct")
    med = d3.get("median_return_pct")
    gap = (avg - med) if avg is not None and med is not None else None
    lines = [
        "H5 Return Distribution / Tail Contribution Analysis",
        "=" * 54,
        "",
        f"period: {start.isoformat()} .. {end.isoformat()}",
        f"score_source: {score_source}",
        "analysis-only: Primary / DB / UI / LINE / actual_trade_logs were not modified.",
        "",
        "1. Research_ALL return curve",
        f"- HD3 avg={_round(d3.get('avg_return_pct'), 4)} median={_round(d3.get('median_return_pct'), 4)} gap={_round(gap, 4)} WR={_round(d3.get('win_rate'), 2)}%",
        f"- HD5 avg={_round(d5.get('avg_return_pct'), 4)} median={_round(d5.get('median_return_pct'), 4)}",
        f"- HD7 avg={_round(d7.get('avg_return_pct'), 4)} median={_round(d7.get('median_return_pct'), 4)}",
        f"- HD10 avg={_round(d10.get('avg_return_pct'), 4)} median={_round(d10.get('median_return_pct'), 4)}",
        "",
        "2. Right-tail contribution, Research_ALL HD3",
        f"- top 1% share of net: {_round(tail3.get('top_1pct_share_of_net_return'), 2)}%",
        f"- top 5% share of net: {_round(tail3.get('top_5pct_share_of_net_return'), 2)}%",
        f"- top 10% share of net: {_round(tail3.get('top_10pct_share_of_net_return'), 2)}%",
        f"- judgment: {tail3.get('right_tail_dependency_judgment')}",
        "",
        "3. Tail removal",
        f"- remove top 5% HD3 avg: {_round(rem5.get('avg_return_pct'), 4)}% delta={_round(rem5.get('delta_avg_vs_full'), 4)}",
        f"- remove top 10% HD3 avg: {_round(rem10.get('avg_return_pct'), 4)}% delta={_round(rem10.get('delta_avg_vs_full'), 4)}",
        "",
        "4. Threshold rates, Research_ALL HD3",
        f"- > +5%: {_round(th3.get('gt_5pct_rate'), 2)}%",
        f"- > +10%: {_round(th3.get('gt_10pct_rate'), 2)}%",
        f"- < -5%: {_round(th3.get('lt_minus5pct_rate'), 2)}%",
        f"- < -10%: {_round(th3.get('lt_minus10pct_rate'), 2)}%",
        "",
        "5. Live Limited tail capture",
        f"- live_n={live3.get('live_n')} research_n={live3.get('research_n')}",
        f"- live_tail_capture_rate: {_round(live3.get('live_tail_capture_rate'), 2)}%",
        f"- judgment: {live3.get('judgment')}",
        "",
        "6. Actual / skipped",
        "- actual/skipped sample insufficient in this script. Start logging H5 candidate entry/skipped reason with strategy_group, score_source, model_version, and virtual HD3/HD5/HD7/HD10 outcomes.",
        "",
        "7. Operating view",
    ]
    if tail3.get("right_tail_dependency_judgment") == "RIGHT_TAIL_HEAVY":
        lines.extend([
            "- H5 behaves like a right-tail / population strategy rather than a high-confidence single-name snipe.",
            "- Missing a few large winners can damage total expectancy.",
            "- Favor broad small-lot coverage and avoid subjective exclusions unless they are objective accident filters.",
        ])
    else:
        lines.extend([
            "- H5 has some right-tail contribution, but distribution is not conclusively dominated by top names in this run.",
            "- Selection may still be possible, but tail capture must be measured explicitly.",
        ])
    lines.extend([
        "- Acceptable exclusions: fallback scores, active_model rescore rows, large GU, clear bad news, liquidity/data defects, earnings collision, accident-stop conditions.",
        "- Avoid subjective exclusions: scary chart, vague index anxiety, not understanding the material, lack of confidence, or dislike after prior loss.",
        "- Primary should not be changed from this analysis alone.",
    ])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze H5 return distribution and tail contribution")
    parser.add_argument("--output-dir", default="outputs/h5_return_distribution")
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--score-source", default="active_model", choices=["active_model", "stored_predictions", "stored_or_active_fallback"])
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--allow-score-fallback", action="store_true")
    parser.add_argument("--live-selection-dataset", default="outputs/h5_live_selection_audit/04_live_selection_dataset.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    start = _d(args.start_date)
    end = _d(args.end_date)
    train_end = _d(args.train_end)
    test_start = _d(args.test_start)
    periods = ["train", "test", "all"]
    horizons = [f"HD{d}" for d in HORIZON_DAYS] + [f"HD{d}_EST12" for d in HORIZON_DAYS] + [f"MAX_HIGH_{d}D" for d in HORIZON_DAYS]
    primary_horizons = [f"HD{d}" for d in HORIZON_DAYS]

    sb = _build_supabase()
    candidates = _load_candidates_v2(
        sb,
        start,
        end,
        score_source=args.score_source,
        model_key=args.model_key,
        model_version=args.model_version,
        allow_score_fallback=args.allow_score_fallback,
    )
    rows, skipped_base = _build_dataset(candidates, train_end, test_start)
    audit_path = ROOT / args.live_selection_dataset
    live_keys = _live_selected_keys(audit_path)
    return_rows, skipped_returns = _build_return_rows(rows)
    audit_rows = _build_audit_return_rows(audit_path)
    return_rows.extend(audit_rows)

    dist = _distribution_summary(return_rows, periods, horizons)
    tail = _tail_contribution(return_rows, periods, horizons)
    winners = _rank_rows(return_rows, periods, primary_horizons, True)
    losers = _rank_rows(return_rows, periods, primary_horizons, False)
    removed = _tail_removed(return_rows, periods, primary_horizons)
    thresholds = _threshold_distribution(return_rows, periods, primary_horizons)
    quantiles = _quantile_distribution(return_rows, periods, primary_horizons)
    group_comp = _group_comparison(dist, tail, thresholds)
    live_comp = _live_vs_research(return_rows, primary_horizons)
    actual_comp = _actual_vs_skipped()
    monthly = _monthly_tail(return_rows, primary_horizons)
    right_tail_cases = [r for r in winners if (_to_float(r.get("return_pct"), 0.0) or 0.0) >= 10 or int(r.get("rank") or 999) <= 10]
    left_tail = _left_tail(dist, thresholds, losers)
    skipped = defaultdict(int)
    for item in skipped_base + skipped_returns:
        skipped[item["reason"]] += int(item.get("count") or 1)
    skipped_rows = [{"reason": k, "count": v, "notes": ""} for k, v in sorted(skipped.items())]

    group_counts = defaultdict(int)
    period_counts = defaultdict(int)
    for r in return_rows:
        group_counts[r["group"]] += 1
        period_counts[r["period"]] += 1
    calc_counts = {h: len(_values(return_rows, h)) for h in primary_horizons}

    _write_text(output_dir / "00_input_dataset_summary.txt", f"""
Input dataset summary
=====================
start_date: {start.isoformat()}
end_date: {end.isoformat()}
train_end: {train_end.isoformat()}
test_start: {test_start.isoformat()}
score_source: {args.score_source}
model_key: {args.model_key}
model_version: {args.model_version or ""}
loaded_candidates: {len(candidates)}
analysis_rows_with_group_membership: {len(rows)}
return_distribution_rows: {len(return_rows)}
group_counts: {dict(sorted(group_counts.items()))}
period_counts: {dict(sorted(period_counts.items()))}
return_calculable_counts: {calc_counts}
actual_trade_logs_rows: 0
skipped_or_missed_rows: 0
live_selection_keys_loaded: {len(live_keys)}
audit_distribution_rows_loaded: {len(audit_rows)}

This is analysis-only. It does not modify Primary, DB case definitions, UI,
LINE notifications, actual_trade_logs, Watchlist, or Intraday H5.
""")
    _write_csv(output_dir / "01_return_distribution_summary.csv", dist)
    _write_csv(output_dir / "02_tail_contribution_summary.csv", tail)
    _write_csv(output_dir / "03_top_winners.csv", winners)
    _write_csv(output_dir / "04_top_losers.csv", losers)
    _write_csv(output_dir / "05_tail_removed_simulation.csv", removed)
    _write_csv(output_dir / "06_threshold_distribution.csv", thresholds)
    _write_csv(output_dir / "07_quantile_distribution.csv", quantiles)
    _write_csv(output_dir / "08_group_comparison.csv", group_comp)
    _write_csv(output_dir / "09_live_vs_research_tail_comparison.csv", live_comp)
    _write_csv(output_dir / "10_actual_vs_skipped_comparison.csv", actual_comp)
    _write_csv(output_dir / "11_monthly_tail_stability.csv", monthly)
    _write_csv(output_dir / "12_right_tail_case_studies.csv", right_tail_cases)
    _write_csv(output_dir / "13_left_tail_risk_summary.csv", left_tail)
    _write_csv(output_dir / "14_skipped_rows_summary.csv", skipped_rows)
    _write_text(output_dir / "15_return_distribution_report.txt", _report(
        dist=dist,
        tail=tail,
        removed=removed,
        thresh=thresholds,
        live=live_comp,
        actual=actual_comp,
        start=start,
        end=end,
        score_source=args.score_source,
    ))

    d3 = next((r for r in dist if r["group"] == "Research_ALL" and r["period"] == "all" and r["horizon"] == "HD3"), {})
    t3 = next((r for r in tail if r["group"] == "Research_ALL" and r["period"] == "all" and r["horizon"] == "HD3"), {})
    print(f"loaded_candidates={len(candidates)}")
    print(f"return_rows={len(return_rows)}")
    print(f"research_all_hd3_avg={_round(d3.get('avg_return_pct'), 4)}")
    print(f"research_all_hd3_median={_round(d3.get('median_return_pct'), 4)}")
    print(f"top5_net_share={_round(t3.get('top_5pct_share_of_net_return'), 2)}")
    print(f"judgment={t3.get('right_tail_dependency_judgment')}")
    print(f"output_dir={output_dir}")


if __name__ == "__main__":
    main()
