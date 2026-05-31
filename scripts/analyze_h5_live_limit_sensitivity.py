"""H5 Live Limited Sensitivity Audit.

Analyzes rank_limit / max_daily / max_open / sector_limit sensitivity to
determine whether H5 is a concentrated-selection or distributed strategy.

Research-only — no DB / case / live-code changes.

Usage:
    python scripts/analyze_h5_live_limit_sensitivity.py
"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from services.h5_primary import H5_LIVE_LIMITED_RULES, h5_overheat_score
from services.trade_case_tester import (
    _build_supabase, _load_candidates_v2, _sort_candidates, _to_float,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

EST12_STOP = -0.12
H5_PRIMARY_CASE_KEY = "h5_ai65_hd3_est12_cm_range330_live_limited"
UNLIMITED = 999999

# ── Sort definitions ──────────────────────────────────────────────────────────

SENS_SORTS: dict[str, list[str]] = {
    "intended_original": ["signal_probability_desc", "overheat_score_asc", "volume_ratio_desc"],
    "low_volume":        ["volume_ratio_asc", "signal_probability_desc", "overheat_score_asc"],
    "moderate_volume":   ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
    "regime_priority":   ["market_regime_priority", "volume_ratio_moderate",
                          "signal_probability_desc", "overheat_score_asc"],
}
RANDOM_SEEDS = [0, 42, 99]

# ── Sensitivity ranges ────────────────────────────────────────────────────────

RANK_LIMITS  = [2, 5, 10, 15, 20, 30, 50, 100, UNLIMITED]
MAX_DAILYS   = [1, 2, 3, 5, 10, UNLIMITED]
MAX_OPENS    = [1, 2, 3, 5, 10, UNLIMITED]
MAX_SECTORS  = [1, 2, 3, UNLIMITED]

BASE_RANK   = 10
BASE_DAILY  = 2
BASE_OPEN   = 2
BASE_SECTOR = 2

# ── Combined policy definitions ───────────────────────────────────────────────

POLICIES: list[dict] = [
    {"name": "A_current_live",
     "keys": ["expected_value_desc"],
     "rank": 10, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": False,
     "note": "Current bug behavior (ev fallback sort)"},
    {"name": "B_intended_fixed",
     "keys": ["signal_probability_desc", "overheat_score_asc", "volume_ratio_desc"],
     "rank": 10, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": False,
     "note": "Fixed intended sort (list bug corrected)"},
    {"name": "C_low_volume_wide",
     "keys": ["volume_ratio_asc", "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": False,
     "note": "Low volume sort, rank limit=50"},
    {"name": "D_moderate_volume_wide",
     "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": False,
     "note": "Moderate volume sort, rank limit=50"},
    {"name": "E_regime_modvol_wide",
     "keys": ["market_regime_priority", "volume_ratio_moderate",
              "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": False,
     "note": "Regime priority + moderate volume, rank=50"},
    {"name": "F_diversified_daily5",
     "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 5, "open": 10, "sector": 3,
     "filter": None, "random": False,
     "note": "Moderate volume, daily=5, open=10, sector=3"},
    {"name": "G_broad_research",
     "keys": ["signal_probability_desc"],
     "rank": UNLIMITED, "daily": UNLIMITED, "open": UNLIMITED, "sector": UNLIMITED,
     "filter": None, "random": False,
     "note": "No position limits (Research ALL equivalent)"},
    {"name": "H_random_daily2",
     "keys": [],
     "rank": UNLIMITED, "daily": 2, "open": 2, "sector": 2,
     "filter": None, "random": True,
     "note": "Random sort, unlimited rank, daily=2"},
    {"name": "I_random_daily5",
     "keys": [],
     "rank": UNLIMITED, "daily": 5, "open": 10, "sector": 3,
     "filter": None, "random": True,
     "note": "Random sort, unlimited rank, daily=5, open=10"},
    {"name": "J_panic_rebound",
     "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 2, "open": 5, "sector": 2,
     "filter": "panic_rebound", "random": False,
     "note": "panic_rebound regime only, moderate volume"},
    {"name": "K_no_normal",
     "keys": ["volume_ratio_moderate", "signal_probability_desc", "overheat_score_asc"],
     "rank": 50, "daily": 3, "open": 5, "sector": 2,
     "filter": "no_normal", "random": False,
     "note": "Exclude normal regime, moderate volume"},
]


# ── Utilities ─────────────────────────────────────────────────────────────────

def _d(value) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()

def _lbl(v: int) -> str:
    return "unlimited" if v >= UNLIMITED else str(v)

def _r(v, digits: int = 4):
    try:
        if v is None:
            return None
        f = float(v)
        return round(f, digits) if math.isfinite(f) else None
    except Exception:
        return v

def _avg(vals: list) -> float | None:
    clean = [float(v) for v in vals if v is not None
             and not (isinstance(v, float) and math.isnan(v))]
    return sum(clean) / len(clean) if clean else None

def _pf(vals: list) -> float | None:
    w = sum(v for v in vals if v > 0)
    l = abs(sum(v for v in vals if v <= 0))
    if l <= 0:
        return None if w <= 0 else 999.0
    return w / l

def _wr(vals: list) -> float | None:
    return sum(1 for v in vals if v > 0) / len(vals) * 100 if vals else None

def _max_dd(vals: list) -> float:
    eq = pk = dd = 0.0
    for v in vals:
        eq += v
        pk = max(pk, eq)
        dd = min(dd, eq - pk)
    return dd

def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for k in row:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for row in rows:
            w.writerow({k: (_r(v) if isinstance(v, float) else v) for k, v in row.items()})

def _passes_h5(row: dict) -> bool:
    prob  = _to_float(row.get("signal_probability"), None)
    stage = str(row.get("signal_stage") or "")
    drop  = _to_float(row.get("drop_from_20d_high_pct"), None)
    marg  = _to_float(row.get("margin_ratio"), None)
    reg   = str(row.get("market_regime") or "")
    if prob is None or prob < 0.65:                         return False
    if stage not in {"confirmed", "strong_confirmed"}:      return False
    if drop is None or drop > -8.0:                        return False
    if reg == "panic_selloff":                              return False
    if h5_overheat_score(row) > 1:                         return False
    if marg is not None and (marg < 3 or marg > 30):       return False
    return True

def _raw_ret(row: dict, entry: float, hold: int) -> float | None:
    c = _to_float(row.get(f"future_close_{hold}d"), None)
    return (c / entry - 1.0) * 100.0 if c and entry > 0 else None

def _est12_result(row: dict, entry: float, hold: int) -> dict:
    stop   = entry * (1.0 + EST12_STOP)
    last_c = None
    last_d = 0
    for d in range(1, hold + 1):
        low   = _to_float(row.get(f"future_low_{d}d"),   None)
        close = _to_float(row.get(f"future_close_{d}d"), None)
        if close is not None:
            last_c, last_d = close, d
        if low is not None and low <= stop:
            return {"ret": EST12_STOP * 100.0, "exit_day": d, "reason": "emergency_stop"}
    if last_c is None:
        return {"ret": None, "exit_day": None, "reason": "no_data"}
    return {"ret": (last_c / entry - 1.0) * 100.0, "exit_day": last_d, "reason": "time_stop"}

def _vol_bucket(v: float | None) -> str:
    if v is None:      return "null"
    if v < 0.7:        return "lt_0_7"
    if v < 1.0:        return "0_7_to_1_0"
    if v < 1.5:        return "1_0_to_1_5"
    if v < 2.0:        return "1_5_to_2_0"
    if v < 3.0:        return "2_0_to_3_0"
    return "gt_3_0"

def _month_key(date_str: str) -> str:
    return str(date_str)[:7]

def _split(rows: list[dict], train_end: date) -> tuple[list[dict], list[dict]]:
    train = [r for r in rows if _d(r["entry_date"]) <= train_end]
    test  = [r for r in rows if _d(r["entry_date"]) > train_end]
    return train, test


# ── Dataset build ─────────────────────────────────────────────────────────────

def _build_dataset(candidates: list[dict]) -> list[dict]:
    dataset: list[dict] = []
    for row in candidates:
        if not _passes_h5(row):
            continue
        entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
        if not entry or entry <= 0:
            continue
        vol  = _to_float(row.get("volume_ratio_20d"), None)
        est3 = _est12_result(row, entry, 3)
        rec: dict = {
            "entry_date":          str(row.get("trade_date") or ""),
            "code":                str(row.get("code") or ""),
            "name":                row.get("name"),
            "sector":              str(row.get("sector") or ""),
            "market_regime":       str(row.get("market_regime") or ""),
            "entry_price":         entry,
            "signal_probability":  _to_float(row.get("signal_probability"), None),
            "overheat_score":      h5_overheat_score(row),
            "volume_ratio":        vol,
            "volume_ratio_bucket": _vol_bucket(vol),
            "hd3_ret_raw":         _r(_raw_ret(row, entry, 3)),
            "hd5_ret_raw":         _r(_raw_ret(row, entry, 5)),
            "hd7_ret_raw":         _r(_raw_ret(row, entry, 7)),
            "hd10_ret_raw":        _r(_raw_ret(row, entry, 10)),
            "hd3_ret_est12":       _r(est3.get("ret")),
            "hd3_exit_day":        est3.get("exit_day") or 3,
            "hd3_exit_reason":     est3.get("reason"),
            "emergency_stop":      est3.get("reason") == "emergency_stop",
        }
        for k in ["volume_ratio_20d", "signal_probability", "rule_score",
                  "bad_news_score", "ma5_gap_pct", "entry_gap_pct",
                  "drop_from_20d_high_pct", "trade_date"]:
            if k not in rec:
                rec[k] = row.get(k)
        dataset.append(rec)
    return dataset


# ── Simulation ────────────────────────────────────────────────────────────────

def _simulate(rows: list[dict], sort_keys: list[str],
              max_daily: int, max_open: int, max_sector: int,
              rank_limit: int) -> list[dict]:
    by_date: dict[str, list] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("entry_date") or "")].append(row)

    for dt, day_rows in by_date.items():
        sorted_day = _sort_candidates(day_rows, sort_keys, H5_LIVE_LIMITED_RULES)
        for i, r in enumerate(sorted_day):
            r["_rank"]     = i + 1
            r["_in_limit"] = (i + 1) <= rank_limit

    open_pos: list[dict] = []
    result:   list[dict] = []

    for dt in sorted(by_date.keys()):
        today    = _d(dt)
        open_pos = [p for p in open_pos if _d(p.get("_expiry")) >= today]

        day_rows = sorted(by_date[dt], key=lambda r: r.get("_rank", UNLIMITED))
        top      = [r for r in day_rows if r.get("_in_limit")]
        below    = [r for r in day_rows if not r.get("_in_limit")]

        daily = 0
        for row in top:
            sec_row = row.get("sector") or "unknown"
            skip: str | None = None
            if daily >= max_daily:
                skip = "daily_limit"
            elif sum(1 for p in open_pos if (p.get("sector") or "unknown") == sec_row) >= max_sector:
                skip = "sector_limit"
            elif len(open_pos) >= max_open:
                skip = "open_position_limit"

            row["_selected"]    = skip is None
            row["_skip_reason"] = skip or "selected"
            if skip is None:
                daily += 1
                row["_expiry"] = (today + timedelta(days=5)).isoformat()
                open_pos.append(row)
            result.append(row)

        for row in below:
            row["_selected"]    = False
            row["_skip_reason"] = "rank_limit"
            result.append(row)

    return result


# ── Performance ───────────────────────────────────────────────────────────────

def _perf(rows: list[dict], label: str = "", period: str = "all") -> dict:
    n   = len(rows)
    out: dict = {"variant": label, "period": period, "n": n}
    if n == 0:
        return out
    for h in [3, 5, 7, 10]:
        col  = f"hd{h}_ret_raw"
        vals = [_to_float(r.get(col), None) for r in rows]
        vals = [v for v in vals if v is not None]
        out[f"hd{h}_avg"]   = _r(_avg(vals))
        out[f"hd{h}_wr"]    = _r(_wr(vals))
        out[f"hd{h}_pf"]    = _r(_pf(vals))
        out[f"hd{h}_maxDD"] = _r(_max_dd(vals))
    hd3_vals = [_to_float(r.get("hd3_ret_raw"), None) for r in rows]
    hd3_vals = [v for v in hd3_vals if v is not None]
    out["hd3_max_loss"]        = _r(min(hd3_vals)) if hd3_vals else None
    out["emergency_stop_rate"] = _r(sum(1 for r in rows if r.get("emergency_stop")) / n * 100)
    out["avg_signal_prob"]     = _r(_avg(
        [_to_float(r.get("signal_probability"), None) for r in rows if r.get("signal_probability") is not None]
    ))
    out["avg_volume_ratio"]    = _r(_avg(
        [_to_float(r.get("volume_ratio"), None) for r in rows if r.get("volume_ratio") is not None]
    ))
    return out

def _avg_perfs(perfs: list[dict], label: str, period: str) -> dict:
    result: dict = {"variant": label, "period": period}
    all_keys: set[str] = set()
    for p in perfs:
        all_keys.update(p.keys())
    for k in all_keys:
        if k in ("variant", "period"):
            continue
        vals = [p[k] for p in perfs if k in p and p[k] is not None
                and isinstance(p[k], (int, float))]
        if vals:
            result[k] = _r(sum(vals) / len(vals))
    return result


# ── Simulation helper ─────────────────────────────────────────────────────────

def _run(dataset: list[dict], sort_keys: list[str],
         rank: int, daily: int, open_: int, sector: int,
         train_end: date) -> dict:
    sim      = _simulate([dict(r) for r in dataset], sort_keys, daily, open_, sector, rank)
    selected = [r for r in sim if r.get("_selected")]
    s_tr, s_te = _split(selected, train_end)
    return {
        "sim":     sim,
        "all":     selected,
        "train":   s_tr,
        "test":    s_te,
        "p_all":   _perf(selected, "", "all"),
        "p_train": _perf(s_tr,     "", "train"),
        "p_test":  _perf(s_te,     "", "test"),
    }

def _random_mean_run(dataset: list[dict], rank: int, daily: int, open_: int,
                     sector: int, train_end: date) -> dict:
    pa, ptr, pte = [], [], []
    for seed in RANDOM_SEEDS:
        r = _run(dataset, [f"random_seed{seed}"], rank, daily, open_, sector, train_end)
        pa.append(r["p_all"]); ptr.append(r["p_train"]); pte.append(r["p_test"])
    return {
        "p_all":   _avg_perfs(pa,  "random_mean", "all"),
        "p_train": _avg_perfs(ptr, "random_mean", "train"),
        "p_test":  _avg_perfs(pte, "random_mean", "test"),
    }


# ── Capital & monthly helpers ─────────────────────────────────────────────────

def _capital_stats(selected: list[dict], all_dates: list[str]) -> dict:
    n = len(selected)
    if n == 0:
        return {"n": 0}
    entry_dates = sorted(set(r.get("entry_date", "") for r in selected))
    n_days      = len(entry_dates)
    cap_days    = sum(r.get("hd3_exit_day", 3) for r in selected)
    avg_hold    = cap_days / n
    avg_per_day = n / n_days if n_days else 0
    avg_open    = cap_days / n_days if n_days else 0
    hd3_vals    = [_to_float(r.get("hd3_ret_raw"), None) for r in selected]
    hd3_vals    = [v for v in hd3_vals if v is not None]
    total_ev    = sum(hd3_vals)
    years       = len(all_dates) / 252 if all_dates else 1
    return {
        "n":                     n,
        "entry_days":            n_days,
        "avg_entries_per_day":   _r(avg_per_day),
        "avg_holding_days":      _r(avg_hold),
        "avg_open_positions":    _r(avg_open),
        "total_capital_days":    cap_days,
        "total_ev_pct":          _r(total_ev),
        "ev_per_capital_day":    _r(total_ev / cap_days) if cap_days > 0 else None,
        "annual_trade_count":    _r(n / years) if years > 0 else None,
        "lot500k_capital_est":   _r(avg_open * 500_000),
        "lot1m_capital_est":     _r(avg_open * 1_000_000),
        "lot500k_annual_ev_est": _r(n / years * (_avg(hd3_vals) or 0) / 100 * 500_000) if years > 0 else None,
        "lot1m_annual_ev_est":   _r(n / years * (_avg(hd3_vals) or 0) / 100 * 1_000_000) if years > 0 else None,
    }

def _monthly_stats(selected: list[dict], policy_name: str) -> list[dict]:
    by_month: dict[str, list] = defaultdict(list)
    for r in selected:
        by_month[_month_key(r.get("entry_date", ""))].append(r)
    rows = []
    for m in sorted(by_month.keys()):
        grp = by_month[m]
        p   = _perf(grp, policy_name, "month")
        p["month"]       = m
        p["policy_name"] = policy_name
        p["total_return_sum"] = _r(sum(_to_float(r.get("hd3_ret_raw"), 0) or 0 for r in grp))
        p["emergency_stop_count"] = sum(1 for r in grp if r.get("emergency_stop"))
        rows.append(p)
    return rows

def _monthly_summary(monthly_rows: list[dict], policy_name: str) -> dict:
    mn  = len(monthly_rows)
    if mn == 0:
        return {"policy_name": policy_name, "monthly_count": 0}
    pos_months  = sum(1 for r in monthly_rows if (r.get("total_return_sum") or 0) > 0)
    sums        = [r.get("total_return_sum") or 0 for r in monthly_rows]
    return {
        "policy_name":           policy_name,
        "monthly_count":         mn,
        "positive_month_count":  pos_months,
        "monthly_win_rate":      _r(pos_months / mn * 100),
        "avg_monthly_return_sum":_r(_avg(sums)),
        "worst_month_return_sum":_r(min(sums)) if sums else None,
        "best_month_return_sum": _r(max(sums)) if sums else None,
        "monthly_return_std":    _r(
            math.sqrt(sum((s - (sum(sums)/mn))**2 for s in sums) / mn) if mn > 1 else 0
        ),
    }


# ── Sensitivity analysis ──────────────────────────────────────────────────────

def _run_sensitivity(
    dataset: list[dict], train_end: date, research_hd3: dict,
    dim_key: str, dim_values: list[int],
    fixed_rank: int, fixed_daily: int, fixed_open: int, fixed_sector: int,
) -> list[dict]:
    rows: list[dict] = []

    def _make_row(dim_val: int, sort_name: str, period: str,
                  p: dict, sim: list[dict]) -> dict:
        selected = [r for r in sim if r.get("_selected")]
        d_rank   = [r for r in sim if r.get("_skip_reason") == "rank_limit"]
        d_daily  = [r for r in sim if r.get("_skip_reason") == "daily_limit"]
        d_open   = [r for r in sim if r.get("_skip_reason") == "open_position_limit"]
        d_sector = [r for r in sim if r.get("_skip_reason") == "sector_limit"]
        def _dh3(grp: list) -> float | None:
            vals = [_to_float(r.get("hd3_ret_raw"), None) for r in grp]
            return _r(_avg([v for v in vals if v is not None]))
        rh = research_hd3.get(period) or 0.0
        return {
            dim_key:               _lbl(dim_val),
            "sort_variant":        sort_name,
            "period":              period,
            "n":                   p.get("n", 0),
            "hd3_avg":             p.get("hd3_avg"),
            "hd5_avg":             p.get("hd5_avg"),
            "hd7_avg":             p.get("hd7_avg"),
            "hd10_avg":            p.get("hd10_avg"),
            "hd3_wr":              p.get("hd3_wr"),
            "hd3_pf":              p.get("hd3_pf"),
            "hd3_maxDD":           p.get("hd3_maxDD"),
            "emergency_stop_rate": p.get("emergency_stop_rate"),
            "selected_count":      len(selected),
            "dropped_rank_count":  len(d_rank),
            "dropped_rank_hd3":    _dh3(d_rank),
            "dropped_daily_count": len(d_daily),
            "dropped_daily_hd3":   _dh3(d_daily),
            "dropped_open_count":  len(d_open),
            "dropped_sector_count":len(d_sector),
            "selected_vs_research":_r((p.get("hd3_avg") or 0.0) - rh),
        }

    for val in dim_values:
        rank   = val if dim_key == "entry_rank_limit"      else fixed_rank
        daily  = val if dim_key == "max_daily_entries"     else fixed_daily
        open_  = val if dim_key == "max_open_positions"    else fixed_open
        sector = val if dim_key == "max_sector_positions"  else fixed_sector
        v_lbl  = _lbl(val)

        for sort_name, sort_keys in SENS_SORTS.items():
            logger.info("[sens] %s=%s sort=%s", dim_key, v_lbl, sort_name)
            r   = _run(dataset, sort_keys, rank, daily, open_, sector, train_end)
            sim = r["sim"]
            for period in ["all", "train", "test"]:
                p    = r[f"p_{period}"]
                rows.append(_make_row(val, sort_name, period, p, sim))

        # random_mean
        logger.info("[sens] %s=%s sort=random_mean", dim_key, v_lbl)
        rm = _random_mean_run(dataset, rank, daily, open_, sector, train_end)
        # For random_mean we don't have a single sim, so use totals from all_period only
        for period in ["all", "train", "test"]:
            p = rm[f"p_{period}"]
            rh = research_hd3.get(period) or 0.0
            rows.append({
                dim_key:               v_lbl,
                "sort_variant":        "random_mean",
                "period":              period,
                "n":                   p.get("n"),
                "hd3_avg":             p.get("hd3_avg"),
                "hd5_avg":             p.get("hd5_avg"),
                "hd7_avg":             p.get("hd7_avg"),
                "hd10_avg":            p.get("hd10_avg"),
                "hd3_wr":              p.get("hd3_wr"),
                "hd3_pf":              p.get("hd3_pf"),
                "hd3_maxDD":           p.get("hd3_maxDD"),
                "emergency_stop_rate": p.get("emergency_stop_rate"),
                "selected_vs_research":_r((p.get("hd3_avg") or 0.0) - rh),
            })

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    train_end  = _d(args.train_end)
    start      = _d(args.train_start)
    end        = _d(args.test_end)
    out_dir    = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    sb = _build_supabase()
    logger.info("[limit_sens] loading %s..%s", start, end)
    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[limit_sens] raw candidates=%d", len(candidates))

    dataset = _build_dataset(candidates)
    logger.info("[limit_sens] research rows=%d", len(dataset))

    research_train, research_test = _split(dataset, train_end)
    rb = {
        "all":   _perf(dataset,         "research_all", "all"),
        "train": _perf(research_train,  "research_all", "train"),
        "test":  _perf(research_test,   "research_all", "test"),
    }
    research_hd3 = {p: rb[p].get("hd3_avg") for p in ("all", "train", "test")}
    all_dates    = sorted(set(r.get("entry_date", "") for r in dataset))

    logger.info("[limit_sens] Research: n=%d  HD3_all=%.4f  HD3_train=%.4f  HD3_test=%.4f",
                len(dataset),
                research_hd3["all"] or 0,
                research_hd3["train"] or 0,
                research_hd3["test"] or 0)

    # ── Files 01-04: Sensitivity analyses ────────────────────────────────────

    logger.info("[limit_sens] running rank_limit sensitivity (%d values x %d sorts)…",
                len(RANK_LIMITS), len(SENS_SORTS) + 1)
    rows01 = _run_sensitivity(
        dataset, train_end, research_hd3,
        "entry_rank_limit", RANK_LIMITS,
        BASE_RANK, BASE_DAILY, BASE_OPEN, BASE_SECTOR,
    )
    _write_csv(out_dir / "01_rank_limit_sensitivity.csv", rows01)
    logger.info("[limit_sens] 01 done")

    logger.info("[limit_sens] running max_daily sensitivity…")
    rows02 = _run_sensitivity(
        dataset, train_end, research_hd3,
        "max_daily_entries", MAX_DAILYS,
        BASE_RANK, BASE_DAILY, BASE_OPEN, BASE_SECTOR,
    )
    _write_csv(out_dir / "02_max_daily_sensitivity.csv", rows02)
    logger.info("[limit_sens] 02 done")

    logger.info("[limit_sens] running max_open sensitivity…")
    rows03 = _run_sensitivity(
        dataset, train_end, research_hd3,
        "max_open_positions", MAX_OPENS,
        BASE_RANK, BASE_DAILY, BASE_OPEN, BASE_SECTOR,
    )
    _write_csv(out_dir / "03_max_open_sensitivity.csv", rows03)
    logger.info("[limit_sens] 03 done")

    logger.info("[limit_sens] running sector sensitivity…")
    rows04 = _run_sensitivity(
        dataset, train_end, research_hd3,
        "max_sector_positions", MAX_SECTORS,
        BASE_RANK, BASE_DAILY, BASE_OPEN, BASE_SECTOR,
    )
    _write_csv(out_dir / "04_sector_limit_sensitivity.csv", rows04)
    logger.info("[limit_sens] 04 done")

    # ── File 05: Combined policy comparison ───────────────────────────────────

    logger.info("[limit_sens] running combined policies…")
    policy_results: dict[str, dict] = {}  # name -> {p_all, p_train, p_test, selected_all, monthly}

    rows05:  list[dict] = []
    rows06:  list[dict] = []
    rows07:  list[dict] = []
    rows08:  list[dict] = []
    rows09:  list[dict] = []
    rows10:  list[dict] = []
    rows11:  list[dict] = []

    for pol in POLICIES:
        pname = pol["name"]
        logger.info("[limit_sens] policy=%s", pname)

        # Optionally filter dataset by regime
        filt = pol.get("filter")
        if filt == "panic_rebound":
            ds = [r for r in dataset if r.get("market_regime") == "panic_rebound"]
        elif filt == "no_normal":
            ds = [r for r in dataset if r.get("market_regime") != "normal"]
        else:
            ds = dataset

        rank, daily, open_, sector = pol["rank"], pol["daily"], pol["open"], pol["sector"]

        if pol.get("random"):
            rm   = _random_mean_run(ds, rank, daily, open_, sector, train_end)
            sim_all   = None
            p_all     = rm["p_all"]
            p_train   = rm["p_train"]
            p_test    = rm["p_test"]
            # For selected trades, use seed=0 as representative
            r0   = _run(ds, ["random_seed0"], rank, daily, open_, sector, train_end)
            sel  = r0["all"]
            sim_ = r0["sim"]
        else:
            r    = _run(ds, pol["keys"], rank, daily, open_, sector, train_end)
            p_all   = r["p_all"]
            p_train = r["p_train"]
            p_test  = r["p_test"]
            sel     = r["all"]
            sim_    = r["sim"]

        policy_results[pname] = {
            "p_all": p_all, "p_train": p_train, "p_test": p_test,
            "selected": sel, "sim": sim_,
        }

        # Monthly
        monthly = _monthly_stats(sel, pname)
        rows07.extend(monthly)
        rows08.append(_monthly_summary(monthly, pname))

        # Capital stats
        cap = _capital_stats(sel, all_dates)

        # Monthly win rate
        pos_m = sum(1 for m in monthly if (m.get("total_return_sum") or 0) > 0)
        mwr   = _r(pos_m / len(monthly) * 100) if monthly else None
        worst_m = _r(min(m.get("total_return_sum") or 0 for m in monthly)) if monthly else None

        # Build file 05 rows
        for period in ["all", "train", "test"]:
            p  = {"all": p_all, "train": p_train, "test": p_test}[period]
            rh = research_hd3.get(period) or 0.0
            bug_h3 = None  # filled later
            row05: dict = {
                "policy_name":          pname,
                "period":               period,
                "n":                    p.get("n", 0),
                "hd3_avg":              p.get("hd3_avg"),
                "hd5_avg":              p.get("hd5_avg"),
                "hd7_avg":              p.get("hd7_avg"),
                "hd10_avg":             p.get("hd10_avg"),
                "hd3_wr":               p.get("hd3_wr"),
                "hd3_pf":               p.get("hd3_pf"),
                "hd3_maxDD":            p.get("hd3_maxDD"),
                "emergency_stop_rate":  p.get("emergency_stop_rate"),
                "monthly_win_rate":     mwr if period == "all" else None,
                "worst_month":          worst_m if period == "all" else None,
                "avg_entries_per_day":  cap.get("avg_entries_per_day") if period == "all" else None,
                "avg_open_positions":   cap.get("avg_open_positions") if period == "all" else None,
                "lot500k_capital_est":  cap.get("lot500k_capital_est") if period == "all" else None,
                "lot1m_capital_est":    cap.get("lot1m_capital_est") if period == "all" else None,
                "annual_trade_count":   cap.get("annual_trade_count") if period == "all" else None,
                "selected_vs_research": _r((p.get("hd3_avg") or 0.0) - rh),
                "note":                 pol.get("note", ""),
            }
            rows05.append(row05)

        # File 06: Capital efficiency
        cap_row = {"policy_name": pname, **cap,
                   "regime_filter": filt or "none",
                   "sort":          str(pol.get("keys", "random")),
                   "rank":          _lbl(rank), "daily": _lbl(daily),
                   "open": _lbl(open_), "sector": _lbl(sector)}
        rows06.append(cap_row)

        # File 09: Regime breakdown
        by_reg: dict[str, list] = defaultdict(list)
        for r2 in sel:
            by_reg[r2.get("market_regime") or "unknown"].append(r2)
        for reg, grp in sorted(by_reg.items()):
            p2 = _perf(grp, pname, "all")
            p2["policy_name"]  = pname
            p2["market_regime"]= reg
            p2["selected_rate"]= _r(len(grp) / len(ds) * 100) if ds else None
            rows09.append(p2)

        # File 10: Volume breakdown
        by_vol: dict[str, list] = defaultdict(list)
        for r2 in sel:
            by_vol[r2.get("volume_ratio_bucket") or "null"].append(r2)
        for vb, grp in sorted(by_vol.items()):
            p2 = _perf(grp, pname, "all")
            p2["policy_name"]   = pname
            p2["volume_bucket"] = vb
            rows10.append(p2)

        # File 11: Selected vs dropped
        if sim_ is not None:
            drop_groups = {
                "selected":              [r2 for r2 in sim_ if r2.get("_selected")],
                "dropped_rank":          [r2 for r2 in sim_ if r2.get("_skip_reason") == "rank_limit"],
                "dropped_daily":         [r2 for r2 in sim_ if r2.get("_skip_reason") == "daily_limit"],
                "dropped_open":          [r2 for r2 in sim_ if r2.get("_skip_reason") == "open_position_limit"],
                "dropped_sector":        [r2 for r2 in sim_ if r2.get("_skip_reason") == "sector_limit"],
                "not_selected_all":      [r2 for r2 in sim_ if not r2.get("_selected")],
            }
            for grp_name, grp_rows in drop_groups.items():
                p2 = _perf(grp_rows, pname, "all")
                p2["policy_name"] = pname
                p2["group"]       = grp_name
                rows11.append(p2)

    _write_csv(out_dir / "05_combined_policy_comparison.csv", rows05)
    _write_csv(out_dir / "06_capital_efficiency.csv",         rows06)
    _write_csv(out_dir / "07_monthly_stability.csv",          rows07)
    _write_csv(out_dir / "08_monthly_stability_summary.csv",  rows08)
    _write_csv(out_dir / "09_regime_breakdown.csv",           rows09)
    _write_csv(out_dir / "10_volume_breakdown.csv",           rows10)
    _write_csv(out_dir / "11_selected_vs_dropped.csv",        rows11)
    logger.info("[limit_sens] 05-11 done")

    # ── Derive key insights for reports ──────────────────────────────────────

    def _pol_hd3(name: str, period: str = "all") -> float | None:
        pr = policy_results.get(name, {})
        return pr.get(f"p_{period}", {}).get("hd3_avg")

    bug_hd3  = _pol_hd3("A_current_live")
    research = research_hd3["all"] or 0.0

    # Best rank_limit for low_volume sort (all period)
    rl_lv_all = {row["entry_rank_limit"]: row["hd3_avg"]
                 for row in rows01
                 if row.get("sort_variant") == "low_volume" and row.get("period") == "all"}
    best_rl_lv   = max(rl_lv_all, key=lambda k: rl_lv_all[k] or -99)
    best_rl_h3   = rl_lv_all.get(best_rl_lv)

    # Best max_daily for moderate_volume (all period)
    md_mv_all = {row["max_daily_entries"]: row["hd3_avg"]
                 for row in rows02
                 if row.get("sort_variant") == "moderate_volume" and row.get("period") == "all"}
    best_md_mv  = max(md_mv_all, key=lambda k: md_mv_all[k] or -99)
    best_md_h3  = md_mv_all.get(best_md_mv)

    # Best max_open for moderate_volume (all period)
    mo_mv_all = {row["max_open_positions"]: row["hd3_avg"]
                 for row in rows03
                 if row.get("sort_variant") == "moderate_volume" and row.get("period") == "all"}
    best_mo_mv  = max(mo_mv_all, key=lambda k: mo_mv_all[k] or -99)
    best_mo_h3  = mo_mv_all.get(best_mo_mv)

    # Does unlimited rank_limit improve performance?
    rl10_lv  = rl_lv_all.get("10",        -99.0) or -99.0
    rl_unlim = rl_lv_all.get("unlimited", -99.0) or -99.0
    rank_limit_widens_help = rl_unlim > rl10_lv

    # Does broad_research come close to Research?
    broad_h3 = _pol_hd3("G_broad_research")

    # PASS/FAIL policies
    def _judge(name: str) -> str:
        h3a = _pol_hd3(name, "all")
        h3t = _pol_hd3(name, "test")
        h3r = _pol_hd3(name, "train")
        if h3a is None or h3t is None or h3r is None:
            return "?"
        beats_bug    = bug_hd3 is None or h3a > (bug_hd3 or -99)
        test_ok      = h3t >= 0.0
        train_ok     = h3r >= 0.0
        close_to_res = h3a >= research * 0.5
        if beats_bug and test_ok and train_ok and close_to_res:
            return "PASS"
        if beats_bug and test_ok:
            return "WATCH"
        return "FAIL"

    judged = {pol["name"]: _judge(pol["name"]) for pol in POLICIES}

    # H5 type: if G (broad) >> A (current) → distributed
    h5_type = "DISTRIBUTED" if (broad_h3 or 0) > research * 0.9 else "MIXED"

    # ── File 12: Policy recommendation ───────────────────────────────────────

    rec_lines = [
        "H5 Live Limited — Policy Recommendation",
        "=" * 60,
        "",
        f"Generated: {date.today()}",
        f"Research ALL HD3:    +{research:.4f}%",
        f"Current live (A) HD3: {bug_hd3:.4f}%" if bug_hd3 is not None else "Current live (A): N/A",
        "",
    ]

    def qr(n: int, q: str, a: str) -> None:
        rec_lines.append(f"Q{n:02d}. {q}")
        rec_lines.append(f"  -> {a}")
        rec_lines.append("")

    qr(1, "H5は少数選抜型か、分散取得型か",
       f"{h5_type}. broad_research (G, 制限なし) HD3={broad_h3}% vs Research={research:.4f}%."
       f" rank_limit拡大でHD3が{'改善' if rank_limit_widens_help else '改善せず'}(low_volume: rl10={rl10_lv:.4f} vs unlimited={rl_unlim:.4f})."
       f" 制限緩和が改善するなら分散型の特性あり。")

    qr(2, "entry_rank_limit=10は妥当か",
       f"low_volume sortでの最良rank_limit={best_rl_lv} (HD3={best_rl_h3})."
       f" 現行10が {'最適' if best_rl_lv == '10' else '最適でない可能性あり — ' + str(best_rl_lv) + 'が良い'}.")

    qr(3, "max_daily_entries=2は妥当か",
       f"moderate_volume sortでの最良max_daily={best_md_mv} (HD3={best_md_h3})."
       f" 現行2が {'最適' if best_md_mv == '2' else '2より' + str(best_md_mv) + 'が良い可能性'}.")

    qr(4, "max_open_positions=2は妥当か",
       f"moderate_volume sortでの最良max_open={best_mo_mv} (HD3={best_mo_h3})."
       f" 現行2が {'最適' if best_mo_mv == '2' else '2より' + str(best_mo_mv) + 'が良い可能性'}.")

    qr(5, "sector制限は妥当か",
       f"sector sensitivity 04ファイルを参照。sector=unlimited vs sector=2の差が小さければ制限は合理的。")

    qr(6, "sortより制限緩和の方が効くか",
       f"{'YES' if rank_limit_widens_help else 'UNCLEAR'}."
       f" rank_limit拡大でlow_volume HD3が{rl10_lv:.4f}->{rl_unlim:.4f}変化。"
       f" sort改良より制限緩和の方が影響が大きいなら分散型戦略を支持。")

    pass_pols  = [n for n, j in judged.items() if j == "PASS"]
    watch_pols = [n for n, j in judged.items() if j == "WATCH"]
    qr(7, "selectedがdroppedより良いpolicyはあるか",
       f"file 11 (selected_vs_dropped) を参照。selected > dropped になるpolicyが実用候補。"
       f" PASS={pass_pols}  WATCH={watch_pols}.")

    best_pol  = max(POLICIES, key=lambda p: (_pol_hd3(p["name"], "all") or -99.0))
    best_name = best_pol["name"]
    best_h3   = _pol_hd3(best_name, "all")
    qr(8, "Research平均に近づくpolicyはあるか",
       f"最良policy={best_name} (HD3={best_h3}). Research={research:.4f}%."
       f" gap={_r((best_h3 or 0) - research):.4f}pp.")

    # Capital-feasible: avg_open < 5 and good performance
    cap_feasible = [p["name"] for p in POLICIES
                    if (_pol_hd3(p["name"], "all") or -99) > -0.1
                    and p["open"] <= 5]
    qr(9, "資金拘束込みで現実的なpolicyはどれか",
       f"max_open<=5かつHD3>-0.1: {cap_feasible}."
       f" 詳細は06_capital_efficiency.csvを参照。lot500k=avg_open*50万円。")

    db_candidate = watch_pols[0] if watch_pols else (pass_pols[0] if pass_pols else "none")
    qr(10, "次にDB comparison case化すべきpolicyはどれか",
       f"{db_candidate}."
       f" 理由: test期間でResearch比での改善が見られる場合、forward-testとして登録する価値がある。")

    qr(11, "Primaryは変更すべきか",
       f"NO. 現時点で明確に優れるpolicyなし。本分析はresearch段階。"
       f" Primary ({H5_PRIMARY_CASE_KEY}) は維持。")

    qr(12, "Live Selectedをどう扱うべきか",
       f"過信禁止。Live Selected HD3={bug_hd3}%はResearch HD3=+{research:.4f}%を大幅に下回る。"
       f" sortバグ修正後も継続モニタリングが必要。実弾判断の参考にはなるが、盲信しない。")

    (out_dir / "12_policy_recommendation.txt").write_text("\n".join(rec_lines), encoding="utf-8")
    logger.info("[limit_sens] 12 done")

    # ── File 13: Full report ──────────────────────────────────────────────────

    rep: list[str] = [
        "H5 Live Limited Sensitivity Audit Report",
        "=" * 70,
        f"Generated: {date.today()}",
        f"Period: {args.train_start}~{args.train_end} (train) / {args.test_start}~{args.test_end} (test)",
        f"Research: n={len(dataset)} (train={len(research_train)}, test={len(research_test)})",
        f"Primary: {H5_PRIMARY_CASE_KEY}",
        "",
        "=" * 70,
        "1. EXECUTIVE SUMMARY",
        "=" * 70,
        f"  Research ALL HD3=+{research:.4f}%  WR={rb['all'].get('hd3_wr'):.1f}%",
        f"  Current live (A) HD3={bug_hd3}%",
        f"  Sort repair best (low_volume ALL) HD3=+0.1783%",
        f"  broad_research (G, no limits) HD3={broad_h3}%",
        f"  H5 type assessment: {h5_type}",
        "",
        "  Key question: Does loosening limits improve performance?",
        f"  rank_limit effect (low_volume, 10->unlimited): {rl10_lv:.4f}% -> {rl_unlim:.4f}%",
        f"  Conclusion: {'制限緩和が有効 → 分散型戦略' if rank_limit_widens_help else '制限緩和の効果不明確 → 選抜型 or シグナル自体の課題'}",
        "",
        "=" * 70,
        "2. BACKGROUND",
        "=" * 70,
        "  - H5 Research has positive edge: HD3=+0.3541%, WR=53.2%",
        "  - PB20 abolished (justified)",
        "  - Live Selected HD3=-0.1353% (worse than Research)",
        "  - entry_sort list bug fixed (_sort_candidates handles list[str])",
        "  - Sort repair: low_volume best deterministic (ALL=+0.178%) but < random_mean(+0.069%)",
        "  - None of deterministic sorts matched Research baseline",
        "  - Hypothesis: problem is not just sort but position limit configuration",
        "",
        "=" * 70,
        "3. RANK LIMIT ANALYSIS (low_volume sort, file 01)",
        "=" * 70,
    ]
    # Add rank limit table for low_volume sort
    rep.append(f"  {'rank_limit':12s}  {'HD3_all':10s}  {'HD3_test':10s}  {'n_all':6s}")
    for rl in RANK_LIMITS:
        lbl_rl = _lbl(rl)
        h3a = rl_lv_all.get(lbl_rl)
        h3t = next((r["hd3_avg"] for r in rows01
                    if r.get("sort_variant") == "low_volume"
                    and r.get("period") == "test"
                    and r.get("entry_rank_limit") == lbl_rl), None)
        n_all = next((r["n"] for r in rows01
                      if r.get("sort_variant") == "low_volume"
                      and r.get("period") == "all"
                      and r.get("entry_rank_limit") == lbl_rl), None)
        rep.append(f"  {lbl_rl:12s}  {str(h3a or 'N/A'):10s}  {str(h3t or 'N/A'):10s}  {str(n_all or '?'):6s}")
    rep.append("")

    rep += [
        "=" * 70,
        "4. DAILY ENTRY LIMIT ANALYSIS (moderate_volume sort, file 02)",
        "=" * 70,
    ]
    rep.append(f"  {'max_daily':12s}  {'HD3_all':10s}  {'HD3_test':10s}  {'n_all':6s}")
    for md in MAX_DAILYS:
        lbl_md = _lbl(md)
        h3a = md_mv_all.get(lbl_md)
        h3t = next((r["hd3_avg"] for r in rows02
                    if r.get("sort_variant") == "moderate_volume"
                    and r.get("period") == "test"
                    and r.get("max_daily_entries") == lbl_md), None)
        n_all = next((r["n"] for r in rows02
                      if r.get("sort_variant") == "moderate_volume"
                      and r.get("period") == "all"
                      and r.get("max_daily_entries") == lbl_md), None)
        rep.append(f"  {lbl_md:12s}  {str(h3a or 'N/A'):10s}  {str(h3t or 'N/A'):10s}  {str(n_all or '?'):6s}")
    rep.append("")

    rep += [
        "=" * 70,
        "5. OPEN POSITION LIMIT ANALYSIS (moderate_volume sort, file 03)",
        "=" * 70,
    ]
    rep.append(f"  {'max_open':12s}  {'HD3_all':10s}  {'HD3_test':10s}  {'n_all':6s}")
    for mo in MAX_OPENS:
        lbl_mo = _lbl(mo)
        h3a = mo_mv_all.get(lbl_mo)
        h3t = next((r["hd3_avg"] for r in rows03
                    if r.get("sort_variant") == "moderate_volume"
                    and r.get("period") == "test"
                    and r.get("max_open_positions") == lbl_mo), None)
        n_all = next((r["n"] for r in rows03
                      if r.get("sort_variant") == "moderate_volume"
                      and r.get("period") == "all"
                      and r.get("max_open_positions") == lbl_mo), None)
        rep.append(f"  {lbl_mo:12s}  {str(h3a or 'N/A'):10s}  {str(h3t or 'N/A'):10s}  {str(n_all or '?'):6s}")
    rep.append("")

    rep += [
        "=" * 70,
        "6. COMBINED POLICY COMPARISON (file 05)",
        "=" * 70,
        f"  {'policy':30s}  {'HD3_all':10s}  {'HD3_train':10s}  {'HD3_test':10s}  {'n':6s}  judgment",
    ]
    for pol in POLICIES:
        pn = pol["name"]
        h3a = _pol_hd3(pn, "all")
        h3t = _pol_hd3(pn, "test")
        h3r = _pol_hd3(pn, "train")
        n_all = policy_results.get(pn, {}).get("p_all", {}).get("n", 0)
        jdg = judged.get(pn, "?")
        rep.append(
            f"  {pn:30s}  {str(h3a or 'N/A'):10s}  {str(h3r or 'N/A'):10s}"
            f"  {str(h3t or 'N/A'):10s}  {str(n_all):6s}  {jdg}"
        )
    rep.append("")

    rep += [
        "=" * 70,
        "7. MONTHLY STABILITY (file 08 summary)",
        "=" * 70,
        f"  {'policy':30s}  {'monthly_wr':12s}  {'avg_monthly_sum':15s}  {'worst_month':12s}",
    ]
    for row08 in rows08:
        rep.append(
            f"  {row08.get('policy_name', '?'):30s}  "
            f"{str(row08.get('monthly_win_rate') or 'N/A'):12s}  "
            f"{str(row08.get('avg_monthly_return_sum') or 'N/A'):15s}  "
            f"{str(row08.get('worst_month_return_sum') or 'N/A'):12s}"
        )
    rep.append("")

    rep += [
        "=" * 70,
        "8. CAPITAL EFFICIENCY (file 06, selected policies)",
        "=" * 70,
        f"  {'policy':30s}  {'avg_open':10s}  {'ev_cap_day':10s}  {'lot500k_cap':12s}  {'ann_trades':10s}",
    ]
    for row06 in rows06:
        rep.append(
            f"  {row06.get('policy_name', '?'):30s}  "
            f"{str(row06.get('avg_open_positions') or 'N/A'):10s}  "
            f"{str(row06.get('ev_per_capital_day') or 'N/A'):10s}  "
            f"{str(row06.get('lot500k_capital_est') or 'N/A'):12s}  "
            f"{str(row06.get('annual_trade_count') or 'N/A'):10s}"
        )
    rep.append("")

    rep += [
        "=" * 70,
        "9. RECOMMENDATION",
        "=" * 70,
        f"  H5 type: {h5_type}",
        f"  PASS policies:  {pass_pols}",
        f"  WATCH policies: {watch_pols}",
        f"  Best all-period: {best_name} HD3={best_h3}%",
        f"  DB case candidate: {db_candidate}",
        "",
        "  DO NOT change yet:",
        f"  - Primary ({H5_PRIMARY_CASE_KEY}): NOT changed",
        "  - PB20: NOT revived",
        "  - Extension Allow: NOT promoted",
        "  - DB case definitions: NOT changed",
        "  - UI / LINE / trade_logs: NOT changed",
        "",
        "Output files:",
        "  01_rank_limit_sensitivity.csv",
        "  02_max_daily_sensitivity.csv",
        "  03_max_open_sensitivity.csv",
        "  04_sector_limit_sensitivity.csv",
        "  05_combined_policy_comparison.csv",
        "  06_capital_efficiency.csv",
        "  07_monthly_stability.csv",
        "  08_monthly_stability_summary.csv",
        "  09_regime_breakdown.csv",
        "  10_volume_breakdown.csv",
        "  11_selected_vs_dropped.csv",
        "  12_policy_recommendation.txt",
        "  13_live_limit_sensitivity_report.txt",
    ]

    (out_dir / "13_live_limit_sensitivity_report.txt").write_text("\n".join(rep), encoding="utf-8")
    logger.info("[limit_sens] 13 done")
    logger.info("[limit_sens] ALL DONE -> %s", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="H5 Live Limited Sensitivity Audit")
    parser.add_argument("--train-start",          default="2023-01-01")
    parser.add_argument("--train-end",            default="2024-12-31")
    parser.add_argument("--test-start",           default="2025-01-01")
    parser.add_argument("--test-end",             default="2026-05-28")
    parser.add_argument("--output-dir",           default="outputs/h5_live_limit_sensitivity")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
