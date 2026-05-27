#!/usr/bin/env python3
"""H5 × 信用倍率フィルター分析

Strategy: H5_AI65_PB20_HD3_EST8_CM (固定)
Filter:   margin_ratio <= [none, 5, 10, 20, 30]

J-Quants API から週次信用残データ (date 指定で全銘柄一括) を取得し、
published lag 7日で lookahead-free に結合して比較する。

信用倍率 = LongVol / ShrtVol  (信用買い残 / 信用売り残)

Usage:
    python scripts/analyze_h5_margin_filter.py
    python scripts/analyze_h5_margin_filter.py --use-db  # Supabase fallback
"""

from __future__ import annotations

import argparse
import bisect
import logging
import os
import pickle
import sys
import time
from datetime import date as date_type, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("postgrest").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = ROOT / "outputs" / "rebound_grid_search" / "cands_2020_2026.pkl"
OUT_DIR = ROOT / "outputs" / "rebound_next_analysis" / "h5_margin"
MARGIN_CACHE_PATH = OUT_DIR / "_margin_jquants_cache.pkl"

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"

# H5_AI65_PB20_HD3_EST8_CM 固定パラメータ
AI_THRESH = 0.65
DROP20D   = -8.0
PB_PCT    = 2.0
HD        = 3
EM_STOP   = -8.0

MARGIN_THRESHOLDS = [None, 5.0, 10.0, 20.0, 30.0]

# Publication lag: weekly margin measured on Friday, published ~7 days later
PUBLISH_LAG_DAYS = 7


# ── Date helpers ─────────────────────────────────────────────────────────────

def _prev_friday(d: date_type) -> date_type:
    """Most recent Friday on or before d."""
    offset = (d.weekday() - 4) % 7
    return d - timedelta(days=offset)


def _relevant_fridays(trade_dates: list[str]) -> list[str]:
    """For each trade_date, find the most recent Friday available.

    'Available' = Friday + LAG_DAYS <= trade_date
    i.e., Friday <= trade_date - LAG_DAYS
    """
    fridays = set()
    for td_str in trade_dates:
        td = date_type.fromisoformat(td_str[:10])
        cutoff = td - timedelta(days=PUBLISH_LAG_DAYS)
        fri = _prev_friday(cutoff)
        # Also include 1-2 weeks before for safety
        for w in range(0, 3):
            fridays.add((fri - timedelta(weeks=w)).isoformat())
    return sorted(fridays)


# ── J-Quants margin fetch ─────────────────────────────────────────────────────

def _fetch_jquants_for_date(jq_date: str) -> list[dict]:
    """Fetch weekly margin for ALL codes on a given date from J-Quants."""
    from jquants_client import get_weekly_margin_interest
    try:
        rows = get_weekly_margin_interest(date=jq_date)
        return rows
    except Exception as e:
        logger.warning("J-Quants fetch failed for %s: %s", jq_date, e)
        return []


def _build_margin_df(trade_dates: list[str], use_cache: bool = True) -> pd.DataFrame:
    """Fetch margin data for all relevant Fridays and return as DataFrame.

    Returns columns: code, date (measurement), margin_ratio
    """
    if use_cache and MARGIN_CACHE_PATH.exists():
        logger.info("Loading margin cache from %s", MARGIN_CACHE_PATH)
        with open(MARGIN_CACHE_PATH, "rb") as f:
            return pickle.load(f)

    fridays = _relevant_fridays(trade_dates)
    logger.info("Fetching J-Quants weekly margin for %d Fridays...", len(fridays))

    all_rows = []
    for i, fri in enumerate(fridays):
        rows = _fetch_jquants_for_date(fri)
        for r in rows:
            code = str(r.get("Code", "")).replace("0", "", 1).zfill(4) if r.get("Code") else None
            # Normalize: remove trailing zero from 5-digit code → 4-digit
            raw_code = str(r.get("Code", ""))
            if raw_code.endswith("0") and len(raw_code) == 5:
                code = raw_code[:-1]
            else:
                code = raw_code
            long_vol = r.get("LongVol") or r.get("long_margin_outstanding") or 0
            shrt_vol = r.get("ShrtVol") or r.get("short_margin_outstanding") or 0
            try:
                long_vol = float(long_vol)
                shrt_vol = float(shrt_vol)
            except (TypeError, ValueError):
                continue
            if shrt_vol > 0:
                mr = long_vol / shrt_vol
            elif long_vol > 0:
                mr = 99.0
            else:
                continue
            meas_date = str(r.get("Date", fri))[:10]
            all_rows.append({"code": code, "date": meas_date, "margin_ratio": mr})

        if (i + 1) % 10 == 0 or (i + 1) == len(fridays):
            logger.info("  %d/%d Fridays done, %d rows", i + 1, len(fridays), len(all_rows))
        time.sleep(0.2)

    mdf = pd.DataFrame(all_rows)
    if mdf.empty:
        return mdf
    mdf = mdf.sort_values(["code", "date"]).reset_index(drop=True)
    logger.info("Total margin rows: %d  unique codes: %d", len(mdf), mdf["code"].nunique())

    # Save cache
    MARGIN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MARGIN_CACHE_PATH, "wb") as f:
        pickle.dump(mdf, f)
    logger.info("Margin cache saved: %s", MARGIN_CACHE_PATH)
    return mdf


# ── Join ──────────────────────────────────────────────────────────────────────

def _join_margin(df: pd.DataFrame, mdf: pd.DataFrame) -> pd.DataFrame:
    """Lookahead-free join: for each (code, trade_date), use the latest margin
    where measurement_date + PUBLISH_LAG_DAYS <= trade_date.
    """
    df = df.copy()
    if mdf.empty:
        df["margin_ratio_joined"] = np.nan
        return df

    logger.info("Building margin lookup (publication lag=%dd)...", PUBLISH_LAG_DAYS)
    # available_date = measurement_date + lag
    mdf_work = mdf.copy()
    mdf_work["avail_date"] = (
        pd.to_datetime(mdf_work["date"]) + timedelta(days=PUBLISH_LAG_DAYS)
    ).dt.strftime("%Y-%m-%d")

    # Per-code sorted list of (avail_date, margin_ratio)
    code_lookup: dict[str, list[tuple[str, float]]] = {}
    for _, row in mdf_work.iterrows():
        code_lookup.setdefault(str(row["code"]), []).append(
            (str(row["avail_date"]), float(row["margin_ratio"]))
        )
    for code in code_lookup:
        code_lookup[code].sort(key=lambda x: x[0])

    def _lookup(code: str, trade_date: str):
        entries = code_lookup.get(str(code))
        if not entries:
            return np.nan
        dates = [e[0] for e in entries]
        idx = bisect.bisect_right(dates, trade_date) - 1
        return entries[idx][1] if idx >= 0 else np.nan

    df["margin_ratio_joined"] = [
        _lookup(r["code"], r["trade_date"]) for _, r in df.iterrows()
    ]

    null_count = int(df["margin_ratio_joined"].isna().sum())
    total = len(df)
    logger.info("Join done: %d/%d have margin_ratio (%.1f%% coverage)",
                total - null_count, total, (total - null_count) / total * 100)
    return df


# ── Overheat classification ───────────────────────────────────────────────────

def _classify_overheat(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    sc = (
        (df["rsi14"].fillna(0) >= 65).astype(int) +
        (df["ma5_gap_pct"].fillna(0) >= 5).astype(int) +
        (df["return_5d_pct"].fillna(0) >= 8).astype(int) +
        (df["volume_ratio_20d"].fillna(0) >= 3.0).astype(int)
    )
    df["overheat_score"] = sc
    df["overheat_bucket"] = sc.map(
        lambda s: "extreme" if s >= 3 else "hot" if s == 2 else "mild" if s == 1 else "cool"
    )
    return df


# ── Simulation ───────────────────────────────────────────────────────────────

def _simulate_one(row: dict) -> tuple[float | None, str]:
    entry = row.get("entry_price")
    if not entry or entry <= 0:
        return None, "no_entry"
    em_price = entry * (1 + EM_STOP / 100)
    peak = float(entry)
    for day in range(1, HD + 1):
        high  = row.get(f"future_high_{day}d")
        low   = row.get(f"future_low_{day}d")
        close = row.get(f"future_close_{day}d")
        if high is None or low is None or close is None:
            break
        try:
            high, low, close = float(high), float(low), float(close)
        except (TypeError, ValueError):
            break
        if np.isnan(high) or np.isnan(low) or np.isnan(close):
            break
        peak = max(peak, high)
        if low <= em_price:
            return EM_STOP, "emergency_stop"
        if peak > entry * 1.005 and close <= peak * (1 - PB_PCT / 100):
            return (close - entry) / entry * 100, "pullback"
    c = row.get(f"future_close_{HD}d")
    if c is None:
        return None, "no_data"
    try:
        c = float(c)
    except (TypeError, ValueError):
        return None, "no_data"
    if np.isnan(c):
        return None, "no_data"
    return (c - entry) / entry * 100, "time_stop"


# ── Metrics ───────────────────────────────────────────────────────────────────

def _metrics(rets_arr: np.ndarray, label: str, period: str) -> dict:
    valid = np.array([v for v in rets_arr if v is not None and not np.isnan(float(v))], dtype=float)
    n = len(valid)
    base = {"label": label, "period": period, "tc": n}
    if n == 0:
        return {**base, "wr": np.nan, "avg": np.nan, "pf": np.nan,
                "max_loss": np.nan, "max_gain": np.nan, "max_dd": np.nan, "score": np.nan}
    wins   = valid[valid > 0]
    losses = valid[valid < 0]
    pf = float(wins.sum() / abs(losses.sum())) if len(losses) > 0 and losses.sum() < 0 else 99.0
    pf = min(pf, 99.0)
    cum = np.cumsum(valid)
    max_dd = float((cum - np.maximum.accumulate(cum)).min())
    score = (
        float(valid.mean()) * 100
        + pf * 10
        + (len(wins) / n * 100) * 20
        + max_dd * 2
        + float(valid.min()) * 1.5
    )
    return {
        **base,
        "wr":       float(len(wins) / n * 100),
        "avg":      float(valid.mean()),
        "median":   float(np.median(valid)),
        "pf":       float(pf),
        "max_loss": float(valid.min()),
        "max_gain": float(valid.max()),
        "max_dd":   float(max_dd),
        "score":    float(score),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh-cache", action="store_true", help="Re-fetch margin data from J-Quants")
    args = ap.parse_args()

    out_dir = OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load candidates
    logger.info("Loading candidates from %s", DEFAULT_CACHE)
    with open(DEFAULT_CACHE, "rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data["candidates"])
    df["trade_date"] = df["trade_date"].astype(str)
    logger.info("Loaded %d candidates", len(df))

    # 2. H5 base filters (cool_mild_only = EST8_CM)
    df = _classify_overheat(df)
    h5 = df[
        (df["signal_probability"] >= AI_THRESH) &
        (df["drop_from_20d_high_pct"] <= DROP20D) &
        (df["market_regime"] != "panic_selloff") &
        (df["overheat_bucket"].isin(["cool", "mild"]))
    ].copy()
    logger.info("H5 base candidates: %d", len(h5))

    # 3. Fetch margin data from J-Quants
    use_cache = not args.refresh_cache
    trade_dates = h5["trade_date"].unique().tolist()
    mdf = _build_margin_df(trade_dates, use_cache=use_cache)

    # 4. Join margin to candidates
    h5 = _join_margin(h5, mdf)

    # 5. Simulate all trades
    logger.info("Simulating %d trades...", len(h5))
    records = h5.to_dict("records")
    rets_all   = np.array([_simulate_one(r)[0] for r in records], dtype=object)
    margin_arr = h5["margin_ratio_joined"].values

    # 6. Compare margin thresholds
    rows = []
    for thresh in MARGIN_THRESHOLDS:
        label = "margin_none" if thresh is None else f"margin_le{int(thresh)}"
        if thresh is None:
            mask = np.ones(len(h5), dtype=bool)
        else:
            # unknown margin → include (conservative, don't penalize missing data)
            known  = ~pd.isna(margin_arr)
            passed = np.where(known, np.array(margin_arr, dtype=float) <= thresh, True)
            mask   = passed.astype(bool)

        for period, date_mask in [
            ("train", h5["trade_date"].values <= TRAIN_END),
            ("test",  h5["trade_date"].values >= TEST_START),
            ("all",   np.ones(len(h5), dtype=bool)),
        ]:
            combined = mask & date_mask
            m = _metrics(rets_all[combined], label, period)
            m["margin_threshold"] = thresh
            m["n_filtered_out"] = int((~mask & date_mask).sum())
            rows.append(m)

    result_df = pd.DataFrame(rows)
    result_df.to_csv(out_dir / "margin_filter_comparison.csv", index=False)
    logger.info("→ margin_filter_comparison.csv")

    # 7. Strict filter (exclude unknown margin too)
    rows_strict = []
    for thresh in MARGIN_THRESHOLDS[1:]:  # skip None
        label = f"margin_le{int(thresh)}_strict"
        known  = ~pd.isna(margin_arr)
        passed = known & (np.array(pd.to_numeric(margin_arr, errors="coerce"), dtype=float) <= thresh)
        mask   = passed
        for period, date_mask in [
            ("train", h5["trade_date"].values <= TRAIN_END),
            ("test",  h5["trade_date"].values >= TEST_START),
        ]:
            combined = mask & date_mask
            m = _metrics(rets_all[combined], label, period)
            m["margin_threshold"] = thresh
            m["strict"] = True
            rows_strict.append(m)
    pd.DataFrame(rows_strict).to_csv(out_dir / "margin_filter_strict.csv", index=False)
    logger.info("→ margin_filter_strict.csv")

    # 8. Margin distribution
    mr_vals = pd.to_numeric(h5["margin_ratio_joined"], errors="coerce").dropna()
    dist_rows = []
    for thresh in [3, 5, 10, 20, 30, 50, 100]:
        n_le = int((mr_vals <= thresh).sum())
        dist_rows.append({
            "threshold": thresh,
            "n_le": n_le,
            "pct_le": float(n_le / len(h5) * 100),
            "n_gt": int((mr_vals > thresh).sum()),
        })
    pd.DataFrame(dist_rows).to_csv(out_dir / "margin_distribution.csv", index=False)
    logger.info("→ margin_distribution.csv")

    # 9. Margin bucket breakdown
    bucket_rows = []
    bins   = [0, 3, 5, 10, 20, 30, float("inf")]
    labels = ["0-3", "3-5", "5-10", "10-20", "20-30", "30+"]
    h5_copy = h5.copy()
    h5_copy["_mr"] = pd.to_numeric(h5_copy["margin_ratio_joined"], errors="coerce")
    h5_copy["margin_bucket"] = pd.cut(h5_copy["_mr"], bins=bins, labels=labels, right=False)
    for bkt in labels + ["unknown"]:
        if bkt == "unknown":
            mask = h5_copy["_mr"].isna().values
        else:
            mask = (h5_copy["margin_bucket"] == bkt).values
        for period, date_mask in [
            ("train", h5_copy["trade_date"].values <= TRAIN_END),
            ("test",  h5_copy["trade_date"].values >= TEST_START),
        ]:
            combined = mask & date_mask
            m = _metrics(rets_all[combined], f"bucket_{bkt}", period)
            m["bucket"] = bkt
            bucket_rows.append(m)
    pd.DataFrame(bucket_rows).to_csv(out_dir / "margin_bucket_breakdown.csv", index=False)
    logger.info("→ margin_bucket_breakdown.csv")

    # 10. Text report
    _write_report(out_dir, result_df, h5, mr_vals)
    logger.info("All done. Output: %s", out_dir)


def _write_report(out_dir: Path, df: pd.DataFrame, h5: pd.DataFrame,
                  mr_vals: pd.Series) -> None:
    from datetime import datetime, timezone, timedelta as td
    JST = timezone(td(hours=9))
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")

    cover = len(mr_vals) / len(h5) * 100 if len(h5) > 0 else 0

    lines = [
        "=" * 70,
        "H5 × 信用倍率フィルター分析レポート",
        f"Generated: {now}",
        "=" * 70,
        "",
        "[Fixed Strategy: H5_AI65_PB20_HD3_EST8_CM]",
        f"  AI >= {AI_THRESH}  drop20d <= {DROP20D}%  no_panic_selloff",
        f"  overheat: cool_mild_only  PB={PB_PCT}%  HD={HD}  em_stop={EM_STOP}%",
        f"  H5 base candidates: {len(h5)}",
        "",
        "[Margin Data Coverage]",
        f"  Candidates with margin_ratio: {len(mr_vals)} / {len(h5)} ({cover:.1f}%)",
    ]
    if len(mr_vals) > 0:
        lines += [
            f"  min={mr_vals.min():.1f}x  median={mr_vals.median():.1f}x  "
            f"mean={mr_vals.mean():.1f}x  max={mr_vals.max():.1f}x",
            f"  <= 5x : {(mr_vals<=5).sum()} ({(mr_vals<=5).mean()*100:.1f}%)",
            f"  <= 10x: {(mr_vals<=10).sum()} ({(mr_vals<=10).mean()*100:.1f}%)",
            f"  <= 20x: {(mr_vals<=20).sum()} ({(mr_vals<=20).mean()*100:.1f}%)",
            f"  <= 30x: {(mr_vals<=30).sum()} ({(mr_vals<=30).mean()*100:.1f}%)",
        ]
    lines.append("")

    for period_label, period_key in [("Test (2025-)", "test"), ("Train (2020-2024)", "train")]:
        sub = df[df["period"] == period_key]
        lines += [
            f"[{period_label}]",
            f"  {'filter':<20} {'tc':>5}  {'wr':>6}  {'avg':>7}  {'PF':>5}  "
            f"{'mxL':>7}  {'mxDD':>8}  {'score':>8}",
            "  " + "-" * 70,
        ]
        for _, row in sub.iterrows():
            if pd.isna(row.get("tc")) or row["tc"] == 0:
                lines.append(f"  {row['label']:<20}  (no trades)")
                continue
            lines.append(
                f"  {row['label']:<20} {int(row['tc']):>5}  {row['wr']:>5.1f}%  "
                f"{row['avg']:>+6.2f}%  {row['pf']:>5.2f}  "
                f"{row['max_loss']:>+6.2f}%  {row['max_dd']:>+7.2f}%  {row['score']:>8.1f}"
            )
        lines.append("")

    # Conclusion
    test_sub = df[(df["period"] == "test") & (df["tc"] >= 20)]
    lines.append("[Conclusion]")
    if not test_sub.empty:
        none_row = test_sub[test_sub["label"] == "margin_none"]
        none_tc  = int(none_row.iloc[0]["tc"]) if not none_row.empty else 0
        none_score = float(none_row.iloc[0]["score"]) if not none_row.empty else 0
        for thresh in [5, 10, 20, 30]:
            lbl = f"margin_le{thresh}"
            r = test_sub[test_sub["label"] == lbl]
            if r.empty:
                continue
            tc = int(r.iloc[0]["tc"])
            sc = float(r.iloc[0]["score"])
            diff = sc - none_score
            sign = "+" if diff >= 0 else ""
            lines.append(
                f"  le{thresh}x: tc={tc} ({tc/none_tc*100:.0f}% of none)  "
                f"avg={r.iloc[0]['avg']:+.2f}%  score={sc:.1f} ({sign}{diff:.1f} vs none)"
            )
    lines += ["", "=" * 70]

    (out_dir / "margin_filter_report.txt").write_text("\n".join(lines), encoding="utf-8")
    logger.info("-> margin_filter_report.txt")


if __name__ == "__main__":
    main()
