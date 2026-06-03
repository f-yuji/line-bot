"""Analyze H5 overheat filter sensitivity.

Research-only script. Does NOT modify production logic, DB, active model state,
virtual_trades, watchlist, notifications, or actual_trade_logs.

Compares overheat thresholds:
  A: <=1  (current production)
  B: <=2
  C: <=3
  D: no limit  (AI+drop+stage+no_panic+margin only)

Data source:
  - Walk-forward predictions CSV: signal_probability, signal_stage, future prices,
    drop_from_20d_high_pct, margin_ratio, volume_ratio_20d, market_regime
  - DB stock_rebound_labels: entry_price, future_close_Nd, future_low_Nd
  - DB stock_feature_snapshots (via label IDs): rsi14, ma5_gap_pct, return_5d_pct
  - DB market_regime: market_regime by date

Output: outputs/overheat_sensitivity/
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from supabase import create_client

from services.h5_primary import h5_overheat_score
from services.trade_case_tester import _fetch_all, _fetch_snapshots_by_ids

load_dotenv()

# ── Constants ───────────────────────────────────────────────────────────────
EST12_STOP_RATE = -0.12
EST12_STOP_PCT = -12.0
INITIAL_CAPITAL = 5_000_000
PER_POSITION_CAP = 300_000
DAILY_CAP = 10
COST_BPS = 10.0
TAX_RATE = 0.20315
DEFAULT_PREDICTIONS_CSV = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
OUTPUT_DIR_DEFAULT = "outputs/overheat_sensitivity"
BULLISH_REGIMES = {"normal", "euphoria"}

# ── Utilities ───────────────────────────────────────────────────────────────
def to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        n = float(value)
        return None if not math.isfinite(n) else n
    except Exception:
        return default


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    m = len(s) // 2
    return (s[m - 1] + s[m]) / 2.0 if len(s) % 2 == 0 else s[m]


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
    equity = peak = 0.0
    max_dd = 0.0
    for v in values:
        equity += v
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def parse_date(v: str | date) -> date:
    return v if isinstance(v, date) else datetime.fromisoformat(str(v)[:10]).date()


def normalize_code(raw: Any) -> str:
    """Remove '.0' float suffix if present (walk-forward CSV uses float codes)."""
    return str(raw or "").split(".")[0].strip()


def fmt_f(v: float | None, digits: int = 4) -> str:
    return f"{v:.{digits}f}" if v is not None else "N/A"


def fmt_jpy(v: int | float | None) -> str:
    return f"{int(v):,}円" if v is not None else "N/A"


def fmt_pct(v: float | None, digits: int = 3) -> str:
    return f"{v:.{digits}f}%" if v is not None else "N/A"


# ── Return helpers ──────────────────────────────────────────────────────────
def entry_price_val(row: dict) -> float | None:
    return to_float(row.get("entry_price")) or to_float(row.get("close"))


def hd_return(row: dict, hold: int = 3) -> tuple[float | None, str]:
    ep = entry_price_val(row)
    if ep is None or ep <= 0:
        return None, "invalid_entry"
    stop = ep * (1.0 + EST12_STOP_RATE)
    last_close = None
    for day in range(1, hold + 1):
        close = to_float(row.get(f"future_close_{day}d"))
        low = to_float(row.get(f"future_low_{day}d"))
        if close is not None:
            last_close = close
        if low is not None and low <= stop:
            return EST12_STOP_PCT, "emergency_stop"
    if last_close is None:
        return None, "no_data"
    return (last_close / ep - 1.0) * 100.0, "time_stop"


def attach_returns(rows: list[dict]) -> None:
    for row in rows:
        row["_trade_date"] = parse_date(row["trade_date"])
        ret, reason = hd_return(row, 3)
        row["_hd3"] = ret
        row["_reason_hd3"] = reason
        row["_overheat_score"] = h5_overheat_score(row)


# ── H5 filter functions ─────────────────────────────────────────────────────
def passes_ai(row: dict) -> bool:
    p = to_float(row.get("signal_probability"))
    return p is not None and p >= 0.65


def passes_drop(row: dict) -> bool:
    d = to_float(row.get("drop_from_20d_high_pct"))
    return d is not None and d <= -8.0


def passes_stage(row: dict) -> bool:
    return str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}


def passes_no_panic(row: dict) -> bool:
    return str(row.get("market_regime") or "") != "panic_selloff"


def passes_margin(row: dict) -> bool:
    margin = to_float(row.get("margin_ratio"))
    return margin is None or 3.0 <= margin <= 30.0


def passes_h5_base(row: dict) -> bool:
    return (
        passes_ai(row)
        and passes_drop(row)
        and passes_stage(row)
        and passes_no_panic(row)
        and passes_margin(row)
    )


# _overheat_score must be pre-computed via attach_returns
VARIANTS: list[tuple[str, str, Callable[[dict], bool]]] = [
    ("A", "overheat<=1 (current)",    lambda r: passes_h5_base(r) and r["_overheat_score"] <= 1),
    ("B", "overheat<=2",              lambda r: passes_h5_base(r) and r["_overheat_score"] <= 2),
    ("C", "overheat<=3",              lambda r: passes_h5_base(r) and r["_overheat_score"] <= 3),
    ("D", "no_overheat_limit",        passes_h5_base),
]


# ── Stats ───────────────────────────────────────────────────────────────────
def compute_stats(rows: list[dict], label: str) -> dict:
    vals = [r["_hd3"] for r in rows if r.get("_hd3") is not None]
    n = len(vals)
    em = sum(1 for r in rows if r.get("_reason_hd3") == "emergency_stop")
    active_days = len({r["_trade_date"] for r in rows if r.get("_hd3") is not None})
    return {
        "variant": label,
        "n": n,
        "active_days": active_days,
        "avg_hd3": round(avg(vals), 4) if avg(vals) is not None else None,
        "median_hd3": round(median(vals), 4) if median(vals) is not None else None,
        "win_rate_pct": round(win_rate(vals), 2) if win_rate(vals) is not None else None,
        "profit_factor": round(profit_factor(vals), 4) if profit_factor(vals) is not None else None,
        "max_dd_sum_pct": round(max_drawdown_sum(vals), 4) if max_drawdown_sum(vals) is not None else None,
        "emergency_stop_count": em,
        "emergency_stop_pct": round(em / n * 100, 2) if n > 0 else None,
    }


def top_bottom_stocks(rows: list[dict], n: int = 10) -> tuple[list[dict], list[dict]]:
    valid = sorted([r for r in rows if r.get("_hd3") is not None], key=lambda r: r["_hd3"], reverse=True)
    def _r(r: dict, t: str) -> dict:
        return {
            "type": t, "code": r.get("code"), "name": r.get("name"),
            "trade_date": str(r["_trade_date"]), "hd3_pct": r["_hd3"],
            "overheat_score": r["_overheat_score"],
            "signal_probability": to_float(r.get("signal_probability")),
        }
    return [_r(r, "top") for r in valid[:n]], [_r(r, "bottom") for r in reversed(valid[-n:])]


# ── Portfolio simulation ────────────────────────────────────────────────────
def portfolio_sim(
    rows: list[dict],
    *,
    initial_capital: float = INITIAL_CAPITAL,
    per_position_cap: float = PER_POSITION_CAP,
    daily_cap: int = DAILY_CAP,
    cost_bps: float = COST_BPS,
    tax_rate: float = TAX_RATE,
) -> dict:
    """Simplified portfolio simulation.
    position_size = min(per_position_cap, initial_capital/10)
    Sort each day by signal_probability desc, take top daily_cap.
    Round-trip cost = 2 * cost_bps per trade.
    Tax applied on net profit (通算課税).
    Note: gap<=3% filter NOT applied (翌日オープン価格未取得).
    """
    cost_rate = cost_bps / 10_000
    pos_size = min(per_position_cap, initial_capital / 10)

    by_date: dict[date, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("_hd3") is not None:
            by_date[r["_trade_date"]].append(r)

    if not by_date:
        return {
            "total_trades": 0, "active_days": 0, "pos_size_jpy": round(pos_size),
            "gross_pnl": 0, "tax": 0, "net_pnl": 0, "final_equity": round(initial_capital),
            "cagr_pct": None, "sharpe": None, "max_dd_pct": None, "n_years": None,
        }

    trade_dates = sorted(by_date.keys())
    daily_results: list[tuple[date, float, int]] = []

    for d in trade_dates:
        day_rows = sorted(by_date[d], key=lambda r: -(to_float(r.get("signal_probability")) or 0))[:daily_cap]
        day_pnl = sum(r["_hd3"] / 100.0 * pos_size - 2.0 * cost_rate * pos_size for r in day_rows)
        daily_results.append((d, day_pnl, len(day_rows)))

    equity = initial_capital
    peak = initial_capital
    max_dd_jpy = 0.0
    for _, pnl, _ in daily_results:
        equity += pnl
        peak = max(peak, equity)
        max_dd_jpy = min(max_dd_jpy, equity - peak)

    gross_pnl = equity - initial_capital
    tax = max(0.0, gross_pnl * tax_rate)
    net_pnl = gross_pnl - tax
    final_equity = initial_capital + net_pnl

    n_cal_days = (trade_dates[-1] - trade_dates[0]).days + 1 if len(trade_dates) > 1 else 365
    n_years = n_cal_days / 365.25
    cagr = (final_equity / initial_capital) ** (1.0 / n_years) - 1.0 if n_years > 0 else None

    daily_rets = [pnl / initial_capital for _, pnl, _ in daily_results]
    if len(daily_rets) >= 2:
        m = sum(daily_rets) / len(daily_rets)
        std = (sum((r - m) ** 2 for r in daily_rets) / (len(daily_rets) - 1)) ** 0.5
        sharpe = m / std * (252 ** 0.5) if std > 0 else None
    else:
        sharpe = None

    return {
        "total_trades": sum(n for _, _, n in daily_results),
        "active_days": len(trade_dates),
        "pos_size_jpy": round(pos_size),
        "gross_pnl": round(gross_pnl),
        "tax": round(tax),
        "net_pnl": round(net_pnl),
        "final_equity": round(final_equity),
        "cagr_pct": round(cagr * 100, 3) if cagr is not None else None,
        "sharpe": round(sharpe, 3) if sharpe is not None else None,
        "max_dd_pct": round(max_dd_jpy / initial_capital * 100, 3),
        "n_years": round(n_years, 2),
    }


# ── CSV / text writers ──────────────────────────────────────────────────────
def read_csv_file(path: Path) -> list[dict]:
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
        for k in row:
            if k not in headers:
                headers.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in headers})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


# ── DB connection ───────────────────────────────────────────────────────────
def build_supabase():
    mode = (os.getenv("SUPABASE_MODE") or os.getenv("ENV") or "").upper()
    url = (os.getenv(f"SUPABASE_URL_{mode}") if mode else "") or os.getenv("SUPABASE_URL", "")
    key = (os.getenv(f"SUPABASE_KEY_{mode}") if mode else "") or os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY not set")
    return create_client(url, key)


# ── Custom data loader ──────────────────────────────────────────────────────
def load_data_for_overheat(
    sb,
    wf_csv_path: Path,
    period_start: date,
    period_end: date,
) -> list[dict]:
    """
    Merge walk-forward CSV with DB feature snapshots to get full overheat data.

    Walk-forward CSV provides: signal_probability, signal_stage, future_close/low Nd,
    drop_from_20d_high_pct, margin_ratio, volume_ratio_20d, market_regime, entry_price.

    DB stock_feature_snapshots provides: rsi14, ma5_gap_pct, return_5d_pct
    (needed for h5_overheat_score).

    Process:
    1. Read walk-forward CSV, filter to period, build (code, date) lookup.
    2. Load matching labels from DB to get feature_snapshot_ids.
    3. Batch-load snapshot rsi14/ma5_gap_pct/return_5d_pct by those IDs.
    4. Load market_regime from DB.
    5. Merge all data.
    """
    # ── 1. Walk-forward CSV ──────────────────────────────────────────────
    print(f"[overheat] Reading walk-forward CSV: {wf_csv_path.name}")
    wf_raw = read_csv_file(wf_csv_path)
    wf_rows = []
    for r in wf_raw:
        td = str(r.get("trade_date") or "")[:10]
        if not td:
            continue
        try:
            d = parse_date(td)
        except ValueError:
            continue
        if d < period_start or d > period_end:
            continue
        r["code"] = normalize_code(r.get("code"))
        r["trade_date"] = td
        wf_rows.append(r)

    wf_by_key: dict[tuple[str, str], dict] = {
        (r["code"], r["trade_date"]): r for r in wf_rows
    }
    print(f"[overheat] Walk-forward rows in period: {len(wf_rows)} / unique (code,date) keys: {len(wf_by_key)}")

    # ── 2. Load labels from DB ───────────────────────────────────────────
    start_s = period_start.isoformat()
    end_s = period_end.isoformat()
    label_select = (
        "id,feature_snapshot_id,trade_date,code,entry_price,"
        "future_close_1d,future_close_2d,future_close_3d,"
        "future_low_1d,future_low_2d,future_low_3d"
    )

    def label_query(last_id: int):
        return (
            sb.table("stock_rebound_labels")
            .select(label_select)
            .gt("id", last_id)
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .not_.is_("future_low_3d", "null")
            .order("id")
        )

    print(f"[overheat] Loading labels from DB ({start_s} to {end_s})...")
    all_labels = _fetch_all(label_query, label="labels")
    print(f"[overheat] Labels loaded: {len(all_labels)}")

    # Keep only labels that have a matching walk-forward entry
    matched_labels = [
        lb for lb in all_labels
        if (normalize_code(lb.get("code")), str(lb.get("trade_date") or "")[:10]) in wf_by_key
    ]
    print(f"[overheat] Labels matched to walk-forward: {len(matched_labels)}")

    # ── 3. Batch-load snapshot features ─────────────────────────────────
    snap_ids = [int(lb["feature_snapshot_id"]) for lb in matched_labels if lb.get("feature_snapshot_id")]
    overheat_snap_cols = ["id", "rsi14", "ma5_gap_pct", "return_5d_pct"]
    print(f"[overheat] Loading {len(snap_ids)} snapshots for overheat features...")
    snapshots = _fetch_snapshots_by_ids(sb, snap_ids, overheat_snap_cols)
    snap_by_id = {str(s["id"]): s for s in snapshots}
    print(f"[overheat] Snapshots loaded: {len(snapshots)}")

    # ── 4. Market regime ────────────────────────────────────────────────
    regime_data = (
        sb.table("market_regime")
        .select("trade_date,mode")
        .gte("trade_date", start_s)
        .lte("trade_date", end_s)
        .execute()
        .data or []
    )
    regime_by_date = {str(r["trade_date"]): str(r.get("mode") or "normal") for r in regime_data}

    # ── 5. Merge ─────────────────────────────────────────────────────────
    merged: list[dict] = []
    for lb in matched_labels:
        code = normalize_code(lb.get("code"))
        td = str(lb.get("trade_date") or "")[:10]
        wf = wf_by_key.get((code, td))
        if not wf:
            continue
        snap = snap_by_id.get(str(lb.get("feature_snapshot_id")))

        row: dict = dict(wf)  # start with walk-forward data

        # Override future prices with label data (source of truth for returns)
        for k in ("future_close_1d", "future_close_2d", "future_close_3d",
                  "future_low_1d", "future_low_2d", "future_low_3d"):
            v = to_float(lb.get(k))
            if v is not None:
                row[k] = v

        row["entry_price"] = (
            to_float(lb.get("entry_price")) or to_float(wf.get("entry_price")) or to_float(wf.get("close"))
        )
        row["market_regime"] = regime_by_date.get(td) or wf.get("market_regime")

        # Overheat-specific features from DB snapshot
        if snap:
            row["rsi14"] = snap.get("rsi14")
            row["ma5_gap_pct"] = snap.get("ma5_gap_pct")
            row["return_5d_pct"] = snap.get("return_5d_pct")

        merged.append(row)

    print(f"[overheat] Merged rows: {len(merged)}")
    return merged


# ── Main ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="H5 Overheat Filter Sensitivity Analysis")
    parser.add_argument("--predictions", default=DEFAULT_PREDICTIONS_CSV,
                        help="Walk-forward predictions CSV path")
    parser.add_argument("--start", default=None, help="Analysis start (YYYY-MM-DD), default=CSV min date")
    parser.add_argument("--end", default=None, help="Analysis end (YYYY-MM-DD), default=CSV max date")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT)
    args = parser.parse_args()

    wf_csv_path = ROOT / args.predictions
    if not wf_csv_path.exists():
        print(f"[overheat] ERROR: Walk-forward CSV not found: {wf_csv_path}")
        sys.exit(1)

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine date range from CSV if not specified
    if args.start or args.end:
        period_start = parse_date(args.start) if args.start else date(2020, 1, 1)
        period_end = parse_date(args.end) if args.end else date.today()
    else:
        # Auto-detect from CSV
        raw = read_csv_file(wf_csv_path)
        dates = [str(r.get("trade_date") or "")[:10] for r in raw if r.get("trade_date")]
        period_start = parse_date(min(dates))
        period_end = parse_date(max(dates))
        print(f"[overheat] Auto-detected date range from CSV: {period_start} → {period_end}")

    print(f"[overheat] Connecting to Supabase...")
    sb = build_supabase()

    print(f"[overheat] Loading data for period {period_start} → {period_end}...")
    rows = load_data_for_overheat(sb, wf_csv_path, period_start, period_end)

    if not rows:
        print("[overheat] ERROR: No rows loaded. Check CSV and DB connection.")
        sys.exit(1)

    print(f"[overheat] Attaching HD3 returns and overheat scores...")
    attach_returns(rows)

    valid_rows = [r for r in rows if r.get("_hd3") is not None]
    base_rows = [r for r in valid_rows if passes_h5_base(r)]
    all_rows = valid_rows  # for overheat group analysis (includes non-H5-base)

    # Overheat score distribution in base candidates
    oh_dist = defaultdict(int)
    for r in base_rows:
        oh_dist[r["_overheat_score"]] += 1
    print(f"[overheat] Valid rows (HD3 not null): {len(valid_rows)}")
    print(f"[overheat] H5-base candidates: {len(base_rows)}")
    print(f"[overheat] Overheat score dist (H5-base): {dict(sorted(oh_dist.items()))}")

    # ── 検証1+2: Threshold variants ────────────────────────────────────
    print("[overheat] Computing threshold variants A/B/C/D...")
    stats_rows: list[dict] = []
    sim_rows: list[dict] = []
    top_bottom_rows: list[dict] = []

    for key, label, pred in VARIANTS:
        var_rows = [r for r in base_rows if pred(r)]
        st = compute_stats(var_rows, f"{key}: {label}")
        st["variant_key"] = key
        stats_rows.append(st)

        sim = portfolio_sim(var_rows)
        sim["variant_key"] = key
        sim["variant_label"] = label
        sim_rows.append(sim)

        top, bottom = top_bottom_stocks(var_rows, 10)
        for item in top + bottom:
            item["variant_key"] = key
        top_bottom_rows.extend(top + bottom)

    write_csv(output_dir / "01_threshold_comparison.csv", stats_rows)
    write_csv(output_dir / "02_portfolio_sim.csv", sim_rows)
    write_csv(output_dir / "03_top_bottom_stocks.csv", top_bottom_rows)

    # ── 検証3: overheat_score group analysis ───────────────────────────
    print("[overheat] overheat_score group analysis (0/1/2/3/4+)...")
    score_group_rows: list[dict] = []
    score_groups: dict[str, list[dict]] = defaultdict(list)
    for r in base_rows:
        grp = str(r["_overheat_score"]) if r["_overheat_score"] < 4 else "4+"
        score_groups[grp].append(r)

    for grp in ("0", "1", "2", "3", "4+"):
        group = score_groups.get(grp, [])
        st = compute_stats(group, f"overheat_score={grp}")
        st["overheat_score_group"] = grp
        sim = portfolio_sim(group)
        for k, v in sim.items():
            st[f"sim_{k}"] = v
        score_group_rows.append(st)

    write_csv(output_dir / "04_overheat_score_groups.csv", score_group_rows)

    # ── 検証4: Market regime split ─────────────────────────────────────
    print("[overheat] Market regime split...")
    regime_out: list[dict] = []

    for key, label, pred in VARIANTS:
        var_rows = [r for r in base_rows if pred(r)]
        # Raw regime groups
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in var_rows:
            groups[str(r.get("market_regime") or "unknown")].append(r)
        for regime in sorted(groups):
            st = compute_stats(groups[regime], f"{key}: {label}")
            st["variant_key"] = key
            st["market_regime"] = regime
            st["nikkei_trend_proxy"] = "bullish" if regime in BULLISH_REGIMES else "correction_or_bear"
            regime_out.append(st)
        # Aggregated 2-way split
        for tag, pool in [
            ("bullish(日経>25MA proxy)", [r for r in var_rows if str(r.get("market_regime") or "") in BULLISH_REGIMES]),
            ("correction_or_bear(日経<25MA proxy)", [r for r in var_rows if str(r.get("market_regime") or "") not in BULLISH_REGIMES]),
        ]:
            st = compute_stats(pool, f"{key}: {label}")
            st["variant_key"] = key
            st["market_regime"] = tag
            st["nikkei_trend_proxy"] = tag
            regime_out.append(st)

    write_csv(output_dir / "05_regime_split.csv", regime_out)

    # ── 検証5: 6981 村田製作所 history ─────────────────────────────────
    print("[overheat] 6981 case history...")
    hist_6981: list[dict] = []
    for r in sorted((r for r in rows if normalize_code(r.get("code")) == "6981"), key=lambda x: x["_trade_date"]):
        ep = entry_price_val(r)
        hist_6981.append({
            "trade_date": str(r["_trade_date"]),
            "code": r.get("code"),
            "name": r.get("name"),
            "signal_probability": to_float(r.get("signal_probability")),
            "drop_from_20d_high_pct": to_float(r.get("drop_from_20d_high_pct")),
            "rsi14": to_float(r.get("rsi14")),
            "ma5_gap_pct": to_float(r.get("ma5_gap_pct")),
            "return_5d_pct": to_float(r.get("return_5d_pct")),
            "volume_ratio_20d": to_float(r.get("volume_ratio_20d")),
            "overheat_score": r["_overheat_score"],
            "signal_stage": r.get("signal_stage"),
            "market_regime": r.get("market_regime"),
            "entry_price_ref": ep,
            "passes_h5_base": passes_h5_base(r),
            "passes_A_leq1": passes_h5_base(r) and r["_overheat_score"] <= 1,
            "passes_B_leq2": passes_h5_base(r) and r["_overheat_score"] <= 2,
            "passes_C_leq3": passes_h5_base(r) and r["_overheat_score"] <= 3,
            "passes_D_no_limit": passes_h5_base(r),
            "hd3_est12_pct": r.get("_hd3"),
            "exit_reason": r.get("_reason_hd3"),
            "future_close_1d": to_float(r.get("future_close_1d")),
            "future_close_3d": to_float(r.get("future_close_3d")),
        })
    write_csv(output_dir / "06_6981_history.csv", hist_6981)

    # ── 検証6: near-H5 candidates (overheat-only failures) ────────────
    # Use the last 30 days of available data in the loaded rows
    print("[overheat] Near-H5 candidates (overheat-only failures, from loaded data)...")
    cutoff = period_end - timedelta(days=30)
    near_h5: list[dict] = []
    for r in base_rows:
        if r["_trade_date"] < cutoff:
            continue
        os_ = r["_overheat_score"]
        if os_ <= 1:
            continue
        near_h5.append({
            "trade_date": str(r["_trade_date"]),
            "code": r.get("code"),
            "name": r.get("name"),
            "overheat_score": os_,
            "signal_probability": to_float(r.get("signal_probability")),
            "drop_from_20d_high_pct": to_float(r.get("drop_from_20d_high_pct")),
            "rsi14": to_float(r.get("rsi14")),
            "ma5_gap_pct": to_float(r.get("ma5_gap_pct")),
            "return_5d_pct": to_float(r.get("return_5d_pct")),
            "volume_ratio_20d": to_float(r.get("volume_ratio_20d")),
            "signal_stage": r.get("signal_stage"),
            "market_regime": r.get("market_regime"),
            "hd3_est12_pct": r.get("_hd3"),
            "would_pass_B": os_ <= 2,
            "would_pass_C": os_ <= 3,
            "would_pass_D": True,
        })
    near_h5 = sorted(near_h5, key=lambda r: (r["trade_date"], -(r.get("signal_probability") or 0)))
    write_csv(output_dir / "07_near_h5_candidates.csv", near_h5)

    # ── 検証7: cooldown gap text ────────────────────────────────────────
    cooldown_text = f"""# 検証7: 本番virtual_trades cooldown vs バックテスト差分

## バックテスト（このスクリプト）
- cooldown処理なし
- 同銘柄が複数日でH5シグナルを出しても全てカウント対象
- データソース: ウォークフォワード予測CSV ({wf_csv_path.name})
- 期間: {period_start} ～ {period_end}
- signal_probability: ウォークフォワード予測値（point-in-time valid）
- rsi14/ma5_gap_pct/return_5d_pct: DB feature_snapshots から補完

## 本番（predict_rebound.py / monitor_rebound.py）
- VIRTUAL_REENTRY_COOLDOWN_DAYS = 10
- 同銘柄で直近10日以内にvirtual_tradeがclose済みの場合はシグナル非発信
- 実装: _recent_closed_trade(sb, code, cooldown_days=10)
- _same_signal_trade_exists(sb, snapshot, watch) で重複シグナルも除外

## actual_trade_logs（実弾）への影響
- cooldownはvirtual_trades生成のみに適用
- actual_trade_logsには直接影響しない
- 実弾entryはスクショ補助/手動記録で行うため、cooldown制約を受けない

## バックテスト vs 本番の乖離要因
1. cooldownなし → バックテストは同銘柄の短期連続シグナルも計上
2. 銘柄集中が高い場合（6981等）で乖離が顕在化する可能性あり
3. H5-base候補が少ない時期では影響軽微と推定

## 結論
- 感度比較（A/B/C/D）は全て同条件（cooldownなし）のため相対評価は有効
- 本番ではcooldownにより実質的シグナル数が少なくなる点を認識した上で活用すること
- gap<=3%フィルタ（翌日寄りGU対策）も未適用のためやや楽観的な数値になりうる
"""
    write_text(output_dir / "08_cooldown_gap.txt", cooldown_text)

    # ── Final text report ──────────────────────────────────────────────
    def get_st(key: str) -> dict:
        return next((s for s in stats_rows if s.get("variant_key") == key), {})

    def get_sim(key: str) -> dict:
        return next((s for s in sim_rows if s.get("variant_key") == key), {})

    lines = [
        "# H5 Overheat Filter Sensitivity Analysis Report",
        f"期間: {period_start} ～ {period_end}",
        f"分析日: {date.today()}",
        f"データ: {wf_csv_path.name} + DB feature_snapshots",
        f"",
        f"## シミュレーション設定",
        f"  元本: {INITIAL_CAPITAL:,}円 / S株上限: {PER_POSITION_CAP:,}円 / 日次上限: {DAILY_CAP}件",
        f"  コスト: {COST_BPS}bps往復 / 税率: {TAX_RATE * 100:.3f}% (通算課税)",
        f"  exitモデル: HD3 + EST12 (-12%緊急ストップ)",
        f"  ※gap<=3%フィルタ未適用 / ※cooldown未適用（バックテスト）",
        f"",
        f"## データサマリー",
        f"  valid rows (HD3あり): {len(valid_rows)}",
        f"  H5-base候補: {len(base_rows)}",
        f"  overheat分布 (H5-base): {dict(sorted(oh_dist.items()))}",
        f"",
    ]

    for key in ("A", "B", "C", "D"):
        st = get_st(key)
        sim = get_sim(key)
        label = st.get("variant", key)
        lines += [
            f"## {key}: {label}",
            f"  n={st.get('n')} / 稼働日={st.get('active_days')}",
            f"  平均HD3={fmt_f(st.get('avg_hd3'))}% / 中央値={fmt_f(st.get('median_hd3'))}%",
            f"  勝率={fmt_pct(st.get('win_rate_pct'), 1)} / PF={fmt_f(st.get('profit_factor'), 3)}",
            f"  maxDD_sum={fmt_f(st.get('max_dd_sum_pct'), 2)}% / EM_stop={st.get('emergency_stop_count')}件({fmt_pct(st.get('emergency_stop_pct'), 1)})",
            f"  CAGR={fmt_pct(sim.get('cagr_pct'))} / Sharpe={fmt_f(sim.get('sharpe'), 3)}",
            f"  maxDD(sim)={fmt_pct(sim.get('max_dd_pct'))} / 税後PnL={fmt_jpy(sim.get('net_pnl'))}",
            f"",
        ]

    lines += [
        "## 検証3: overheat_score別期待値 (H5-base条件内)",
        "  score | n   | avg_HD3% | WR%  | PF    | CAGR%",
    ]
    for sg in score_group_rows:
        grp = sg.get("overheat_score_group", "?")
        lines.append(
            f"  {grp:>5} | {sg.get('n', 0):>3} | "
            f"{fmt_f(sg.get('avg_hd3'), 3):>8} | "
            f"{fmt_f(sg.get('win_rate_pct'), 1):>4} | "
            f"{fmt_f(sg.get('profit_factor'), 3):>5} | "
            f"{fmt_f(sg.get('sim_cagr_pct'), 2):>6}"
        )
    lines.append("")

    lines += [
        "## 検証4: 地合い別 (日経>25MA proxy: bullish=normal/euphoria)",
    ]
    agg_regime = [r for r in regime_out if "proxy)" in str(r.get("market_regime", ""))]
    for r in agg_regime:
        lines.append(
            f"  {r.get('variant_key')} / {r.get('market_regime', '?')}: "
            f"n={r.get('n')} avg={fmt_f(r.get('avg_hd3'), 3)}% WR={fmt_f(r.get('win_rate_pct'), 1)}% PF={fmt_f(r.get('profit_factor'), 3)}"
        )
    lines.append("")

    if hist_6981:
        lines += [f"## 検証5: 6981 村田製作所 (n={len(hist_6981)}件)"]
        for h in hist_6981:
            lines.append(
                f"  {h['trade_date']} OH={h['overheat_score']} "
                f"AI={fmt_f(h.get('signal_probability'), 2)} "
                f"RSI={fmt_f(h.get('rsi14'), 1)} "
                f"drop={fmt_f(h.get('drop_from_20d_high_pct'), 1)}% "
                f"HD3={fmt_f(h.get('hd3_est12_pct'), 2)}% "
                f"passA={h.get('passes_A_leq1')} passB={h.get('passes_B_leq2')}"
            )
        lines.append("")

    if near_h5:
        lines += [
            f"## 検証6: 準H5候補 (overheat>1のみ落選, 直近30日, n={len(near_h5)})",
            f"  ※ データ期間末尾 {cutoff} ～ {period_end} の範囲",
        ]
        for c in near_h5[:20]:
            lines.append(
                f"  {c.get('trade_date')} {c.get('code')} {str(c.get('name') or '')[:12]:12s} "
                f"OH={c.get('overheat_score')} AI={fmt_f(c.get('signal_probability'), 2)} "
                f"HD3={fmt_f(c.get('hd3_est12_pct'), 2)}% passB={c.get('would_pass_B')}"
            )
        lines.append("")

    lines += [
        "## 出力ファイル",
        "  01_threshold_comparison.csv   - A/B/C/D統計",
        "  02_portfolio_sim.csv          - ポートフォリオシミュレーション",
        "  03_top_bottom_stocks.csv      - 上位・下位10銘柄",
        "  04_overheat_score_groups.csv  - スコア別期待値",
        "  05_regime_split.csv           - 地合い別成績",
        "  06_6981_history.csv           - 村田製作所履歴",
        "  07_near_h5_candidates.csv     - 準H5候補（overheatのみ落選）",
        "  08_cooldown_gap.txt           - cooldown差分解説",
        "  09_report.txt                 - このレポート",
    ]

    report_text = "\n".join(lines)
    write_text(output_dir / "09_report.txt", report_text)

    # Console summary
    print(f"\n{'=' * 60}")
    print("H5 OVERHEAT SENSITIVITY SUMMARY")
    print(f"{'=' * 60}")
    for key in ("A", "B", "C", "D"):
        st = get_st(key)
        sim = get_sim(key)
        print(f"\n  {key}: {st.get('variant', key)}")
        print(f"    n={st.get('n')}  avg={fmt_f(st.get('avg_hd3'))}%  WR={fmt_pct(st.get('win_rate_pct'), 1)}  PF={fmt_f(st.get('profit_factor'), 3)}")
        print(f"    CAGR={fmt_pct(sim.get('cagr_pct'))}  Sharpe={fmt_f(sim.get('sharpe'), 3)}  maxDD={fmt_pct(sim.get('max_dd_pct'))}")
        print(f"    税後PnL={fmt_jpy(sim.get('net_pnl'))}")

    print(f"\n[overheat] Done. Output: {output_dir}")
    print(f"  Outputs: 09 files written")


if __name__ == "__main__":
    main()
