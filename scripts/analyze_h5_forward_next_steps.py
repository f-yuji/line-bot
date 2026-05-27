"""H5 forward-test 追加検証 — 損切り・信用倍率・実運用制約の3点を詰める。

テーマ:
  A (SL01/02)  : SL発火トレードの事後分析
  B (SL03)     : emergency_stop 深さ比較
  C (MR01/02)  : 信用倍率フィルター再比較
  D (LIVE01)   : 実運用制約シミュレーション
  E (LIVE02)   : entry lag 分析
  F (FINAL01)  : 最終候補比較
    (FINAL02)  : 最終レポート

Usage:
    python scripts/analyze_h5_forward_next_steps.py \
      --start 2023-01-01 --end 2026-05-26 --train-end 2024-12-31 \
      --output-dir outputs/rebound_next_analysis/h5_forward_next
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import pickle
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"
H5_MAX_HOLD = 3
PB_PCT = 2.0       # peak pullback %
MIN_PEAK_RATIO = 1.005


# ─── helpers ──────────────────────────────────────────────────────────────────

def _f(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None or v == "":
            return default
        out = float(v)
        return default if (math.isnan(out) or math.isinf(out)) else out
    except (TypeError, ValueError):
        return default


def _period_label(td: str, train_end: str) -> str:
    return "train" if str(td) <= train_end else "test"


def _pf(wins: list[float], losses: list[float]) -> float | None:
    gross_profit = sum(w for w in wins if w > 0)
    gross_loss = abs(sum(l for l in losses if l < 0))
    if gross_loss == 0:
        return None if gross_profit == 0 else 99.0
    return round(gross_profit / gross_loss, 3)


def _max_dd(rets: list[float]) -> float:
    eq, peak, dd = 0.0, 0.0, 0.0
    for r in rets:
        eq += r
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    return round(dd, 3)


def _score(wr: float, avg: float, pf: float | None, max_loss: float) -> float:
    pf_v = min(pf or 1.0, 10.0)
    ml_pen = max(0.0, 1 + max_loss / 20)
    return round(wr / 100 * pf_v * avg * ml_pen, 4)


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("(no data)\n", encoding="utf-8-sig")
        return
    keys = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    logger.info("wrote %s (%d rows)", path.name, len(rows))


def _overheat_score(row: dict) -> int:
    score = 0
    if (_f(row.get("rsi14"), 0) or 0) >= 65:
        score += 1
    if (_f(row.get("ma5_gap_pct"), 0) or 0) >= 5:
        score += 1
    if (_f(row.get("return_5d_pct"), 0) or 0) >= 8:
        score += 1
    if (_f(row.get("volume_ratio_20d"), 0) or 0) >= 3.0:
        score += 1
    return score


# ─── core H5 simulation ───────────────────────────────────────────────────────

def _sim_h5(
    row: dict,
    stop_pct: float | None = -0.08,
    pullback_pct: float = PB_PCT,
    max_hold: int = H5_MAX_HOLD,
    entry_override: float | None = None,
    day_offset: int = 0,
) -> dict:
    """Simulate one H5 trade.

    day_offset: shift future data by N days (for entry lag analysis).
    Returns: ret, exit_type, exit_day, peak_ret_pct, sl_day, peak_at_sl_pct
    """
    entry = _f(entry_override) or _f(row.get("entry_price")) or _f(row.get("close"))
    if not entry or entry <= 0:
        return {"ret": None, "exit_type": "invalid", "exit_day": None,
                "peak_ret_pct": None, "sl_day": None, "peak_at_sl_pct": None}

    sl_price = entry * (1 + stop_pct) if (stop_pct is not None and stop_pct > -0.49) else None
    trigger_pct = pullback_pct / 100.0
    peak = entry
    sl_day = None
    peak_at_sl = None
    sim_ret = None
    exit_type = "open"

    for day in range(1, max_hold + 1):
        actual_day = day + day_offset
        high = _f(row.get(f"future_high_{actual_day}d"))
        low  = _f(row.get(f"future_low_{actual_day}d"))
        close = _f(row.get(f"future_close_{actual_day}d"))
        if close is None:
            break
        if high is not None:
            peak = max(peak, high)
        # SL check (intraday low)
        if sl_price is not None and low is not None and low <= sl_price:
            sl_day = day
            peak_at_sl = (peak - entry) / entry * 100
            sim_ret = (sl_price - entry) / entry * 100
            exit_type = "sl"
            break
        # Peak pullback check (close-based)
        if close is not None and peak > entry * MIN_PEAK_RATIO:
            if close <= peak * (1 - trigger_pct):
                sim_ret = (close - entry) / entry * 100
                exit_type = "peak_pullback"
                break
        # Time stop
        if day == max_hold:
            sim_ret = (close - entry) / entry * 100
            exit_type = "timeout"
            break

    peak_ret = (peak - entry) / entry * 100

    return {
        "ret": round(sim_ret, 4) if sim_ret is not None else None,
        "exit_type": exit_type,
        "exit_day": day if exit_type != "open" else None,
        "peak_ret_pct": round(peak_ret, 4),
        "sl_day": sl_day,
        "peak_at_sl_pct": round(peak_at_sl, 4) if peak_at_sl is not None else None,
    }


def _sim_nostop(row, pullback_pct=PB_PCT, max_hold=H5_MAX_HOLD, day_offset=0):
    """Simulate without SL (for post-SL analysis)."""
    return _sim_h5(row, stop_pct=None, pullback_pct=pullback_pct,
                   max_hold=max_hold, day_offset=day_offset)


def _summary_stats(rets: list[float], counts: dict | None = None) -> dict:
    if not rets:
        return {"n": 0, "win_rate": None, "avg_ret": None, "median_ret": None,
                "pf": None, "max_loss": None, "max_dd": None, "score": None}
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    wr = len(wins) / len(rets) * 100
    avg = mean(rets)
    med = median(rets)
    pf = _pf(wins, losses)
    ml = min(rets)
    dd = _max_dd(rets)
    sc = _score(wr, avg, pf, ml)
    out = {
        "n": len(rets),
        "win_rate": round(wr, 2),
        "avg_ret": round(avg, 4),
        "median_ret": round(med, 4),
        "pf": pf,
        "max_loss": round(ml, 4),
        "max_dd": round(dd, 3),
        "score": sc,
    }
    if counts:
        out.update(counts)
    return out


# ─── data loading ─────────────────────────────────────────────────────────────

def _load_candidates_cached(start: date, end: date, cache_path: Path) -> list[dict]:
    if cache_path.exists():
        logger.info("loading candidates from cache %s", cache_path.name)
        with cache_path.open("rb") as f:
            return pickle.load(f)

    logger.info("loading candidates from DB %s..%s", start, end)
    from services.trade_case_tester import (
        _build_supabase, _load_candidates_v2,
    )
    sb = _build_supabase()
    cands = _load_candidates_v2(sb, start, end)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("wb") as f:
        pickle.dump(cands, f)
    logger.info("cached %d candidates to %s", len(cands), cache_path.name)
    return cands


def _h5_base_filter(cands: list[dict]) -> list[dict]:
    """Apply H5 base conditions (without margin filter)."""
    out = []
    for r in cands:
        if (_f(r.get("signal_probability"), 0) or 0) < 0.65:
            continue
        drop = _f(r.get("drop_from_20d_high_pct"), 0) or 0
        if drop > -8.0:
            continue
        if str(r.get("market_regime") or "") == "panic_selloff":
            continue
        if _overheat_score(r) > 1:
            continue
        out.append(r)
    return out


def _apply_margin_filter(rows: list[dict], margin_filter: str) -> list[dict]:
    """Apply a named margin filter. require_margin_data=False (no data = pass)."""
    if margin_filter == "none":
        return rows

    def _mr(r):
        return _f(r.get("margin_ratio"))

    if margin_filter == "le5":
        return [r for r in rows if _mr(r) is None or _mr(r) <= 5]
    if margin_filter == "le10":
        return [r for r in rows if _mr(r) is None or _mr(r) <= 10]
    if margin_filter == "le20":
        return [r for r in rows if _mr(r) is None or _mr(r) <= 20]
    if margin_filter == "le30":
        return [r for r in rows if _mr(r) is None or _mr(r) <= 30]
    if margin_filter == "exclude_gt30":
        return [r for r in rows if _mr(r) is None or _mr(r) <= 30]
    if margin_filter == "range_3_30":
        return [r for r in rows if _mr(r) is None or (3 <= _mr(r) <= 30)]
    if margin_filter == "range_5_30":
        return [r for r in rows if _mr(r) is None or (5 <= _mr(r) <= 30)]
    if margin_filter == "range_10_30":
        return [r for r in rows if _mr(r) is None or (10 <= _mr(r) <= 30)]
    if margin_filter == "range_5_20":
        return [r for r in rows if _mr(r) is None or (5 <= _mr(r) <= 20)]
    if margin_filter == "range_10_20":
        return [r for r in rows if _mr(r) is None or (10 <= _mr(r) <= 20)]
    return rows


# ─── Theme A: SL発火トレードの事後分析 ──────────────────────────────────────────

def theme_a_sl_analysis(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    logger.info("[ThemeA] SL analysis start")
    stop_pct = -0.08
    sl01_rows: list[dict] = []

    for r in h5_rows:
        sim = _sim_h5(r, stop_pct=stop_pct)
        if sim["exit_type"] != "sl":
            continue
        sl_day = sim["sl_day"]
        entry = _f(r.get("entry_price")) or _f(r.get("close"))
        # post-SL data (relative to entry date, using future_close columns)
        def _c(d): return _f(r.get(f"future_close_{sl_day + d}d"))
        def _ret(d):
            c = _c(d)
            return round((c - entry) / entry * 100, 4) if c is not None else None

        ret1 = _ret(1); ret2 = _ret(2); ret3 = _ret(3); ret5 = _ret(5)

        # Would-be outcomes without SL
        nostop = _sim_nostop(r)
        w3 = _f(r.get(f"future_close_{min(3, 20)}d"))
        w4 = _f(r.get(f"future_close_{min(4, 20)}d"))
        w5 = _f(r.get(f"future_close_{min(5, 20)}d"))
        wt3 = round((w3 - entry) / entry * 100, 4) if w3 else None
        wt4 = round((w4 - entry) / entry * 100, 4) if w4 else None
        wt5 = round((w5 - entry) / entry * 100, 4) if w5 else None

        rec1 = (ret1 is not None and ret1 >= 0)
        rec2 = (ret2 is not None and ret2 >= 0)
        rec3 = (ret3 is not None and ret3 >= 0)
        rec5 = (ret5 is not None and ret5 >= 0)

        sl_ret = sim["ret"]
        # stop_helped: without SL, HD3 would be worse
        # stop_hurt: without SL, HD3 would be better or entry recovered
        if wt3 is not None and sl_ret is not None:
            if wt3 < sl_ret:
                label = "helped"
            elif wt3 > 0 or rec3:
                label = "hurt"
            elif abs(wt3 - sl_ret) < 0.5:
                label = "neutral"
            else:
                label = "hurt"
        else:
            label = "unknown"

        sl01_rows.append({
            "period": _period_label(str(r.get("trade_date") or ""), train_end),
            "trade_date": r.get("trade_date"),
            "code": r.get("code"),
            "name": r.get("name"),
            "entry_price": entry,
            "sl_day": sl_day,
            "sl_ret": sl_ret,
            "signal_probability": _f(r.get("signal_probability")),
            "drop_from_20d_high_pct": _f(r.get("drop_from_20d_high_pct")),
            "market_regime": r.get("market_regime"),
            "margin_ratio": _f(r.get("margin_ratio")),
            "overheat_score": _overheat_score(r),
            "peak_before_sl_pct": sim["peak_at_sl_pct"],
            "ret_after_sl_1d": ret1,
            "ret_after_sl_2d": ret2,
            "ret_after_sl_3d": ret3,
            "ret_after_sl_5d": ret5,
            "recovered_entry_1d": rec1,
            "recovered_entry_2d": rec2,
            "recovered_entry_3d": rec3,
            "recovered_entry_5d": rec5,
            "would_hit_peak_pullback": (nostop["exit_type"] == "peak_pullback"),
            "would_timeout_ret_hd3": wt3,
            "would_timeout_ret_hd4": wt4,
            "would_timeout_ret_hd5": wt5,
            "stop_helped": (label == "helped"),
            "stop_hurt": (label == "hurt"),
            "stop_result_label": label,
        })

    _write_csv(out_dir / "SL01_stopped_trades_after.csv", sl01_rows)

    # SL02: summary
    sl02_rows = []
    for period in ["train", "test", "all"]:
        subset = [r for r in sl01_rows if period == "all" or r["period"] == period]
        if not subset:
            continue
        n = len(subset)
        helped = sum(1 for r in subset if r["stop_helped"])
        hurt = sum(1 for r in subset if r["stop_hurt"])
        neutral = n - helped - hurt

        def _avg(key):
            vals = [r[key] for r in subset if r[key] is not None]
            return round(mean(vals), 4) if vals else None

        def _rate(key):
            return round(sum(1 for r in subset if r[key]) / n * 100, 2)

        saved = sum(abs(r["sl_ret"]) - abs(r["would_timeout_ret_hd3"])
                    for r in subset
                    if r["stop_helped"] and r["would_timeout_ret_hd3"] is not None and r["sl_ret"] is not None)
        lost = sum(abs(r["would_timeout_ret_hd3"]) - abs(r["sl_ret"])
                   for r in subset
                   if r["stop_hurt"] and r["would_timeout_ret_hd3"] is not None and r["sl_ret"] is not None)

        sl02_rows.append({
            "period": period,
            "stop_count": n,
            "helped_count": helped,
            "hurt_count": hurt,
            "neutral_count": neutral,
            "helped_rate": round(helped / n * 100, 2),
            "hurt_rate": round(hurt / n * 100, 2),
            "recovered_entry_1d_rate": _rate("recovered_entry_1d"),
            "recovered_entry_2d_rate": _rate("recovered_entry_2d"),
            "recovered_entry_3d_rate": _rate("recovered_entry_3d"),
            "recovered_entry_5d_rate": _rate("recovered_entry_5d"),
            "avg_ret_after_sl_1d": _avg("ret_after_sl_1d"),
            "avg_ret_after_sl_2d": _avg("ret_after_sl_2d"),
            "avg_ret_after_sl_3d": _avg("ret_after_sl_3d"),
            "avg_ret_after_sl_5d": _avg("ret_after_sl_5d"),
            "avg_would_timeout_ret_hd3": _avg("would_timeout_ret_hd3"),
            "avg_would_timeout_ret_hd4": _avg("would_timeout_ret_hd4"),
            "avg_would_timeout_ret_hd5": _avg("would_timeout_ret_hd5"),
            "total_saved_by_stop": round(saved, 4),
            "total_lost_by_stop": round(lost, 4),
            "net_stop_effect": round(saved - lost, 4),
        })

    _write_csv(out_dir / "SL02_stop_effect_summary.csv", sl02_rows)
    logger.info("[ThemeA] done: SL trades=%d", len(sl01_rows))


# ─── Theme B: emergency_stop 深さ比較 ────────────────────────────────────────

def theme_b_stop_depth(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    logger.info("[ThemeB] stop depth comparison")
    stop_models = [
        ("emergency8",  -0.08),
        ("emergency10", -0.10),
        ("emergency12", -0.12),
        ("emergency15", -0.15),
        ("nostop",      None),
    ]
    sl03_rows = []

    for period in ["train", "test"]:
        rows = [r for r in h5_rows
                if _period_label(str(r.get("trade_date") or ""), train_end) == period]
        if not rows:
            continue
        for model_name, stop_pct in stop_models:
            rets, types = [], []
            for r in rows:
                s = _sim_h5(r, stop_pct=stop_pct)
                if s["ret"] is None:
                    continue
                rets.append(s["ret"])
                types.append(s["exit_type"])

            if not rets:
                continue
            wins = [r for r in rets if r > 0]
            losses = [r for r in rets if r <= 0]
            sl_cnt = types.count("sl")
            pb_cnt = types.count("peak_pullback")
            to_cnt = types.count("timeout")

            st = _summary_stats(rets)
            sl03_rows.append({
                "stop_model": model_name,
                "period": period,
                "trade_count": st["n"],
                "win_rate": st["win_rate"],
                "avg_ret": st["avg_ret"],
                "median_ret": st["median_ret"],
                "pf": st["pf"],
                "max_loss": st["max_loss"],
                "max_dd": st["max_dd"],
                "stop_count": sl_cnt,
                "stop_rate": round(sl_cnt / st["n"] * 100, 2) if st["n"] else None,
                "peak_pullback_count": pb_cnt,
                "timeout_count": to_cnt,
                "avg_holding_days": round(mean([
                    s["exit_day"] for r in rows
                    if (s := _sim_h5(r, stop_pct=stop_pct)) and s["exit_day"]
                ]), 2) if rows else None,
                "score": st["score"],
            })

    _write_csv(out_dir / "SL03_emergency_depth_comparison.csv", sl03_rows)
    logger.info("[ThemeB] done")


# ─── Theme C: 信用倍率フィルター再比較 ──────────────────────────────────────────

def theme_c_margin_filter(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    logger.info("[ThemeC] margin filter comparison")
    margin_filters = [
        "none", "le5", "le10", "le20", "le30",
        "exclude_gt30", "range_3_30", "range_5_30", "range_10_30",
        "range_5_20", "range_10_20",
    ]
    stop_pct = -0.08

    mr01_rows = []
    for period in ["train", "test"]:
        period_rows = [r for r in h5_rows
                       if _period_label(str(r.get("trade_date") or ""), train_end) == period]
        if not period_rows:
            continue

        has_mr = sum(1 for r in period_rows if _f(r.get("margin_ratio")) is not None)
        coverage = round(has_mr / len(period_rows) * 100, 1) if period_rows else 0

        for mf in margin_filters:
            filtered = _apply_margin_filter(period_rows, mf)
            rets = []
            for r in filtered:
                s = _sim_h5(r, stop_pct=stop_pct)
                if s["ret"] is not None:
                    rets.append(s["ret"])
            st = _summary_stats(rets)
            mr01_rows.append({
                "margin_filter": mf,
                "period": period,
                "margin_data_coverage_pct": coverage,
                **{k: st[k] for k in ["n","win_rate","avg_ret","median_ret","pf","max_loss","max_dd","score"]},
            })

    _write_csv(out_dir / "MR01_margin_filter_comparison.csv", mr01_rows)

    # MR02: bucket breakdown
    buckets = [
        ("0-3",   0,  3),
        ("3-5",   3,  5),
        ("5-10",  5, 10),
        ("10-20",10, 20),
        ("20-30",20, 30),
        ("30+",  30, 9999),
        ("no_data", None, None),
    ]
    mr02_rows = []
    for period in ["train", "test"]:
        period_rows = [r for r in h5_rows
                       if _period_label(str(r.get("trade_date") or ""), train_end) == period]
        for bname, lo, hi in buckets:
            if lo is None:
                bucket_rows = [r for r in period_rows if _f(r.get("margin_ratio")) is None]
            else:
                bucket_rows = [r for r in period_rows
                               if _f(r.get("margin_ratio")) is not None
                               and lo <= _f(r["margin_ratio"]) < hi]
            rets = []
            for r in bucket_rows:
                s = _sim_h5(r, stop_pct=stop_pct)
                if s["ret"] is not None:
                    rets.append(s["ret"])
            st = _summary_stats(rets)
            mr02_rows.append({"bucket": bname, "period": period,
                               **{k: st[k] for k in ["n","win_rate","avg_ret","pf","max_loss","max_dd","score"]}})

    _write_csv(out_dir / "MR02_margin_bucket_detail.csv", mr02_rows)
    logger.info("[ThemeC] done")


# ─── Theme D: 実運用制約シミュレーション ────────────────────────────────────────

def theme_d_position_constraint(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    logger.info("[ThemeD] position constraint simulation")
    stop_pct = -0.08
    margin_filter = "le20"

    combos = [(mp, md) for mp in [2, 3, 5] for md in [1, 2, 3, 5]]
    live01_rows = []

    for period in ["train", "test"]:
        period_rows = _apply_margin_filter(
            [r for r in h5_rows
             if _period_label(str(r.get("trade_date") or ""), train_end) == period],
            margin_filter,
        )
        if not period_rows:
            continue

        # Build date index (sorted trading days)
        trade_dates = sorted(set(str(r.get("trade_date") or "") for r in period_rows))
        date_idx = {d: i for i, d in enumerate(trade_dates)}
        by_date: dict[str, list] = defaultdict(list)
        for r in period_rows:
            by_date[str(r.get("trade_date") or "")].append(r)

        for max_pos, max_de in combos:
            open_trades: list[int] = []  # list of exit_idx (date index)
            selected_rets: list[float] = []
            positions_used_per_day: list[int] = []
            idle_days = 0

            for d in trade_dates:
                di = date_idx[d]
                # expire closed trades
                open_trades = [t for t in open_trades if t > di]
                avail_slots = max_pos - len(open_trades)

                today = sorted(
                    by_date[d],
                    key=lambda r: -(_f(r.get("signal_probability"), 0) or 0),
                )
                take = min(avail_slots, max_de, len(today))
                if take <= 0:
                    idle_days += 1
                    positions_used_per_day.append(len(open_trades))
                    continue

                day_taken = 0
                for row in today[:take]:
                    s = _sim_h5(row, stop_pct=stop_pct)
                    if s["ret"] is None:
                        continue
                    selected_rets.append(s["ret"])
                    exit_day = s["exit_day"] or H5_MAX_HOLD
                    open_trades.append(di + exit_day)
                    day_taken += 1

                positions_used_per_day.append(len(open_trades))

            st = _summary_stats(selected_rets)
            total_days = len(trade_dates)
            live01_rows.append({
                "strategy_id": f"mp{max_pos}_md{max_de}",
                "max_positions": max_pos,
                "max_daily_entries": max_de,
                "period": period,
                "candidate_count": len(period_rows),
                "selected_trade_count": st["n"],
                "selection_rate": round(st["n"] / len(period_rows) * 100, 2) if period_rows else None,
                "win_rate": st["win_rate"],
                "avg_ret": st["avg_ret"],
                "pf": st["pf"],
                "total_ret": round(sum(selected_rets), 4) if selected_rets else None,
                "max_dd": st["max_dd"],
                "max_loss": st["max_loss"],
                "avg_positions_used": round(mean(positions_used_per_day), 2) if positions_used_per_day else None,
                "idle_days": idle_days,
                "capital_efficiency": round(st["n"] / total_days, 4) if total_days else None,
                "score": st["score"],
            })

    _write_csv(out_dir / "LIVE01_position_constraint_simulation.csv", live01_rows)
    logger.info("[ThemeD] done")


# ─── Theme E: entry lag 分析 ─────────────────────────────────────────────────

def theme_e_entry_lag(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    """entry lag 分析。future_open は DB にないため close ベースで近似。"""
    logger.info("[ThemeE] entry lag analysis")
    stop_pct = -0.08
    entry_models = [
        ("close_entry",           0,   None),    # baseline
        ("next_close_entry",      1,   None),    # 翌日終値
        ("next_close_gu_limit2",  1,   0.02),    # 翌日終値 GU2%超スキップ
        ("next_close_gu_limit3",  1,   0.03),    # 翌日終値 GU3%超スキップ
        ("next_close_gu_limit5",  1,   0.05),    # 翌日終値 GU5%超スキップ
    ]
    # Note: future_open_Nd は DB に存在しない。
    # GU判定は (future_close_1d - entry_close) / entry_close をGU proxyとして使用。

    live02_rows = []
    for period in ["train", "test"]:
        period_rows = [r for r in h5_rows
                       if _period_label(str(r.get("trade_date") or ""), train_end) == period]
        if not period_rows:
            continue

        # baseline stats for reference
        for model_name, day_offset, gu_limit in entry_models:
            filled, skipped = 0, 0
            rets: list[float] = []
            gaps: list[float] = []
            missed_profit = 0.0   # profit of skipped trades (would have been positive)
            saved_loss = 0.0      # loss saved by skipping (would have been negative)

            for r in period_rows:
                entry_close = _f(r.get("entry_price")) or _f(r.get("close"))
                if entry_close is None:
                    skipped += 1
                    continue

                if day_offset > 0:
                    new_entry = _f(r.get(f"future_close_{day_offset}d"))
                    if new_entry is None:
                        skipped += 1
                        continue
                    # GU proxy filter (close_t+1 vs close_t)
                    if gu_limit is not None:
                        gap = (new_entry - entry_close) / entry_close
                        gaps.append(gap)
                        if gap > gu_limit:
                            # Would-be return on skipped trade (using shifted sim)
                            s_ref = _sim_h5(r, stop_pct=stop_pct, day_offset=day_offset,
                                            entry_override=new_entry)
                            if s_ref["ret"] is not None:
                                if s_ref["ret"] > 0:
                                    missed_profit += s_ref["ret"]
                                else:
                                    saved_loss += abs(s_ref["ret"])
                            skipped += 1
                            continue
                    s = _sim_h5(r, stop_pct=stop_pct, day_offset=day_offset,
                                entry_override=new_entry)
                else:
                    s = _sim_h5(r, stop_pct=stop_pct)

                if s["ret"] is None:
                    skipped += 1
                    continue
                filled += 1
                rets.append(s["ret"])

            st = _summary_stats(rets)
            avg_gap = round(mean(gaps) * 100, 4) if gaps else None
            live02_rows.append({
                "entry_model": model_name,
                "period": period,
                "signal_count": len(period_rows),
                "filled_count": filled,
                "fill_rate": round(filled / len(period_rows) * 100, 2) if period_rows else None,
                "skipped_count": skipped,
                "avg_gap_pct": avg_gap,
                "win_rate": st["win_rate"],
                "avg_ret": st["avg_ret"],
                "median_ret": st["median_ret"],
                "pf": st["pf"],
                "max_loss": st["max_loss"],
                "max_dd": st["max_dd"],
                "total_ret": round(sum(rets), 4) if rets else None,
                "missed_profit_after_skip": round(missed_profit, 4),
                "saved_loss_after_skip": round(saved_loss, 4),
                "score": st["score"],
                "note": "GU_proxy=close_1d/close (future_open_Nd unavailable)",
            })

    _write_csv(out_dir / "LIVE02_entry_lag_analysis.csv", live02_rows)
    logger.info("[ThemeE] done")


# ─── Theme F: 最終候補比較 ────────────────────────────────────────────────────

def theme_f_final_candidates(h5_rows: list[dict], train_end: str, out_dir: Path) -> None:
    logger.info("[ThemeF] final candidate comparison")

    candidates = [
        ("H5_AI65_EST8_MR20",           -0.08, "le20"),
        ("H5_AI65_NOSTOP_MR20",          None,  "le20"),
        ("H5_AI65_EST10_MR20",          -0.10, "le20"),
        ("H5_AI65_EST12_MR20",          -0.12, "le20"),
        ("H5_AI65_EST8_EXCLUDE_GT30",   -0.08, "exclude_gt30"),
        ("H5_AI65_EST8_no_MR",          -0.08, "none"),
        ("H5_AI65_EST8_MR20_nextclose", -0.08, "le20"),  # entry lag variant, handled below
    ]

    final01_rows = []
    train_stats: dict[str, dict] = {}

    for period in ["train", "test"]:
        period_rows = [r for r in h5_rows
                       if _period_label(str(r.get("trade_date") or ""), train_end) == period]

        for cname, stop_pct, mf in candidates:
            filtered = _apply_margin_filter(period_rows, mf)
            is_nextclose = "nextclose" in cname
            rets, types = [], []
            for r in filtered:
                if is_nextclose:
                    s = _sim_h5(r, stop_pct=stop_pct, day_offset=1,
                                entry_override=_f(r.get("future_close_1d")))
                else:
                    s = _sim_h5(r, stop_pct=stop_pct)
                if s["ret"] is None:
                    continue
                rets.append(s["ret"])
                types.append(s["exit_type"])

            st = _summary_stats(rets)
            sl_cnt = types.count("sl")
            pb_cnt = types.count("peak_pullback")
            to_cnt = types.count("timeout")
            n = st["n"] or 1

            row_out = {
                "candidate_name": cname,
                "period": period,
                "trade_count": st["n"],
                "win_rate": st["win_rate"],
                "avg_ret": st["avg_ret"],
                "median_ret": st["median_ret"],
                "pf": st["pf"],
                "max_loss": st["max_loss"],
                "max_dd": st["max_dd"],
                "stop_count": sl_cnt,
                "stop_rate": round(sl_cnt / n * 100, 2),
                "peak_pullback_rate": round(pb_cnt / n * 100, 2),
                "timeout_rate": round(to_cnt / n * 100, 2),
                "train_test_gap": None,
                "score": st["score"],
            }

            if period == "train":
                train_stats[cname] = {"avg_ret": st["avg_ret"], "win_rate": st["win_rate"]}
            else:
                tr = train_stats.get(cname, {})
                gap_wr = round((st["win_rate"] or 0) - (tr.get("win_rate") or 0), 2)
                gap_ev = round((st["avg_ret"] or 0) - (tr.get("avg_ret") or 0), 4)
                row_out["train_test_gap"] = f"WR {gap_wr:+.1f}pp / EV {gap_ev:+.4f}"

            final01_rows.append(row_out)

    _write_csv(out_dir / "FINAL01_candidate_comparison.csv", final01_rows)
    logger.info("[ThemeF] done")


# ─── Final Report ─────────────────────────────────────────────────────────────

def write_final_report(
    out_dir: Path,
    start: str, end: str, train_end: str,
    h5_rows: list[dict],
    config: dict,
) -> None:
    logger.info("[Report] generating FINAL02")

    def _load_csv(fname: str) -> list[dict]:
        p = out_dir / fname
        if not p.exists():
            return []
        with p.open("r", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))

    sl02 = _load_csv("SL02_stop_effect_summary.csv")
    sl03 = _load_csv("SL03_emergency_depth_comparison.csv")
    live01 = _load_csv("LIVE01_position_constraint_simulation.csv")
    live02 = _load_csv("LIVE02_entry_lag_analysis.csv")
    mr01 = _load_csv("MR01_margin_filter_comparison.csv")
    final01 = _load_csv("FINAL01_candidate_comparison.csv")

    def _best(rows, period, sort_key, reverse=True):
        sub = [r for r in rows if r.get("period") == period and r.get(sort_key)]
        if not sub:
            return None
        return sorted(sub, key=lambda r: float(r[sort_key] or 0), reverse=reverse)[0]

    lines = [
        "=" * 68,
        "  H5 FORWARD-TEST 追加検証レポート",
        f"  生成日時: 2026-05-27",
        "=" * 68,
        "",
        "1. 実行概要",
        f"   期間: {start} 〜 {end}  (train: 〜{train_end} / test: {TEST_START}〜)",
        f"   H5ベース候補数: train={sum(1 for r in h5_rows if _period_label(str(r.get('trade_date') or ''), train_end)=='train')}"
        f" / test={sum(1 for r in h5_rows if _period_label(str(r.get('trade_date') or ''), train_end)=='test')}",
        "",
        "2. SL分析の結論 (Theme A/B)",
        "   ── SL事後効果 (SL02) ──",
    ]

    for r in sl02:
        lines.append(
            f"   [{r.get('period')}]  SL={r.get('stop_count')}件  "
            f"helped={r.get('helped_count')}({r.get('helped_rate')}%)  "
            f"hurt={r.get('hurt_count')}({r.get('hurt_rate')}%)  "
            f"net={r.get('net_stop_effect')}"
        )

    lines += ["", "   ── stop深さ比較 (SL03 / test期間) ──"]
    for r in [x for x in sl03 if x.get("period") == "test"]:
        lines.append(
            f"   {r.get('stop_model'):<15}"
            f"  n={r.get('trade_count'):<5}"
            f"  WR={r.get('win_rate'):<6}"
            f"  EV={r.get('avg_ret'):<7}"
            f"  mxDD={r.get('max_dd'):<7}"
            f"  SL%={r.get('stop_rate')}"
        )

    best_stop = _best([x for x in sl03 if x.get("period") == "test"], "test", "score")
    if best_stop:
        lines += [f"   → best stop_model by score: {best_stop.get('stop_model')}"]

    lines += [
        "",
        "3. 信用倍率分析の結論 (Theme C / MR01 test期間)",
    ]
    for r in [x for x in mr01 if x.get("period") == "test"]:
        lines.append(
            f"   {r.get('margin_filter'):<18}"
            f"  n={r.get('n'):<5}"
            f"  WR={r.get('win_rate'):<6}"
            f"  EV={r.get('avg_ret'):<7}"
            f"  score={r.get('score')}"
        )

    lines += [
        "",
        "4. 実運用制約の結論 (Theme D / LIVE01 test期間)",
    ]
    for r in [x for x in live01 if x.get("period") == "test"]:
        lines.append(
            f"   mp={r.get('max_positions')} md={r.get('max_daily_entries')}  "
            f"n={r.get('selected_trade_count'):<5}"
            f"  WR={r.get('win_rate'):<6}"
            f"  EV={r.get('avg_ret'):<7}"
            f"  cap_eff={r.get('capital_efficiency')}"
        )

    lines += [
        "",
        "5. entry lag の結論 (Theme E / LIVE02 test期間)",
        "   ※ future_open_Nd は DB 未収録。GU判定は future_close_1d をproxy使用。",
    ]
    for r in [x for x in live02 if x.get("period") == "test"]:
        lines.append(
            f"   {r.get('entry_model'):<26}"
            f"  fill={r.get('fill_rate'):<6}"
            f"  WR={r.get('win_rate'):<6}"
            f"  EV={r.get('avg_ret'):<7}"
            f"  score={r.get('score')}"
        )

    lines += [
        "",
        "6. 最終候補比較 (FINAL01 test期間)",
    ]
    for r in [x for x in final01 if x.get("period") == "test"]:
        lines.append(
            f"   {r.get('candidate_name'):<32}"
            f"  n={r.get('trade_count'):<5}"
            f"  WR={r.get('win_rate'):<6}"
            f"  EV={r.get('avg_ret'):<7}"
            f"  gap={r.get('train_test_gap')}"
        )

    best_final = _best([x for x in final01 if x.get("period") == "test"], "test", "score")
    lines += [
        "",
        f"   → best candidate by score: {best_final.get('candidate_name') if best_final else 'N/A'}",
        "",
        "7. 小ロット実弾ルール案 (暫定)",
        "   ・Primary候補のみ実弾対象",
        "   ・1銘柄 5〜10万円",
        "   ・最大同時 max_positions 件 (上記 LIVE01 参照)",
        "   ・1日最大 max_daily_entries 件",
        "   ・翌日close乖離 > 3% は見送り (LIVE02 参照)",
        "   ・panic_selloff 期間は新規停止",
        "   ・20〜30 トレード後に再評価",
        "",
        "=" * 68,
    ]

    report_path = out_dir / "FINAL02_h5_forward_next_report.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("wrote %s", report_path.name)


# ─── main ─────────────────────────────────────────────────────────────────────

def main(args: argparse.Namespace) -> None:
    start = args.start
    end = args.end
    train_end = args.train_end
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_path = out_dir / f"_candidates_cache_{start}_{end}.pkl"
    cands = _load_candidates_cached(
        date.fromisoformat(start), date.fromisoformat(end), cache_path
    )
    logger.info("total candidates: %d", len(cands))

    h5_rows = _h5_base_filter(cands)
    logger.info("H5 base filtered: %d", len(h5_rows))

    # save config
    config = vars(args)
    (out_dir / "h5_forward_next_config.json").write_text(
        json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    theme_a_sl_analysis(h5_rows, train_end, out_dir)
    theme_b_stop_depth(h5_rows, train_end, out_dir)
    theme_c_margin_filter(h5_rows, train_end, out_dir)
    theme_d_position_constraint(h5_rows, train_end, out_dir)
    theme_e_entry_lag(h5_rows, train_end, out_dir)
    theme_f_final_candidates(h5_rows, train_end, out_dir)
    write_final_report(out_dir, start, end, train_end, h5_rows, config)

    logger.info("All themes done. Output: %s", out_dir)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start",       default="2023-01-01")
    p.add_argument("--end",         default="2026-05-26")
    p.add_argument("--train-end",   default=TRAIN_END)
    p.add_argument("--output-dir",  default="outputs/rebound_next_analysis/h5_forward_next")
    return p.parse_args()


if __name__ == "__main__":
    main(_parse_args())
