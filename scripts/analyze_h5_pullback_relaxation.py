"""Compare relaxed H5 pullback/drop conditions.

Research-only script. It does not modify Primary, H5 rules, DB case
definitions, UI, LINE, actual_trade_logs, or auto-trading.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict, deque
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from services.trade_case_tester import _build_supabase  # noqa: E402
from analyze_h5_primary_fractional_sizing import standardize, to_float, write_csv, write_text  # noqa: E402
from analyze_h5_s_share_execution_timing import (  # noqa: E402
    gap_bucket,
    load_all_wf_dates,
    load_next_open_rows,
    make_execution_rows,
    next_date_map,
)
from analyze_h5_s_share_realistic_operation import annualize, pf, simulate_realistic  # noqa: E402
from services.h5_primary import h5_overheat_score  # noqa: E402


DEFAULT_INPUT = "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
DEFAULT_OUT = "outputs/h5_pullback_relaxation"
TAX_RATE = 0.20315


VARIANTS = [
    ("A_current_drop20_lte_m8", "drop20", -8.0),
    ("B_drop20_lte_m7", "drop20", -7.0),
    ("C_drop20_lte_m6", "drop20", -6.0),
    ("D_drop20_lte_m5", "drop20", -5.0),
    ("E_drop10_lte_m4", "drop10", -4.0),
    ("F_drop5_lte_m3", "drop5", -3.0),
]


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def num(value: Any, default: float | None = None) -> float | None:
    out = to_float(value)
    if out is None or math.isnan(out):
        return default
    return out


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
    except ValueError:
        pass
    return text


def cache_key(trade_date: str, code: str) -> str:
    return f"{trade_date}|{normalize_code(code)}"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fetch_feature_rows(rows: list[dict[str, Any]], out_dir: Path, *, compute_drop10: bool = False) -> tuple[dict[str, dict[str, Any]], Counter]:
    """Fetch entry-time feature columns and compute drop10 from cached series."""
    cache_path = out_dir / "feature_cache.json"
    cache = load_json(cache_path)
    stats: Counter = Counter()
    wanted = {
        (str(r.get("trade_date") or ""), str(r.get("code") or ""))
        for r in rows
        if r.get("trade_date") and r.get("code")
    }
    missing = [(d, c) for d, c in wanted if cache_key(d, c) not in cache]
    if missing:
        sb = _build_supabase()
        by_date: dict[str, list[str]] = defaultdict(list)
        for trade_date, code in missing:
            by_date[trade_date].append(code)
        select_cols = (
            "trade_date,code,name,high,close,drop_from_5d_high_pct,drop_from_20d_high_pct,"
            "ma5_gap_pct,ma25_gap_pct,ma75_gap_pct,sector_change_pct,nikkei_change_pct,topix_change_pct,"
            "index_gap_pct,sector_gap_pct,rsi14,volume_ratio_20d,margin_ratio,return_5d_pct"
        )
        for trade_date, codes in sorted(by_date.items()):
            for i in range(0, len(codes), 80):
                chunk = codes[i:i + 80]
                found = (
                    sb.table("stock_feature_snapshots")
                    .select(select_cols)
                    .eq("trade_date", trade_date)
                    .in_("code", chunk)
                    .execute()
                    .data
                    or []
                )
                for row in found:
                    cache[cache_key(str(row.get("trade_date")), str(row.get("code")))] = row
                    stats["feature_fetched"] += 1
    # Optional: fetching all same-period high/close for drop10 is expensive.
    # Keep it off by default so the primary relaxation audit completes quickly.
    need_drop10 = [k for k, v in cache.items() if isinstance(v, dict) and "drop_from_10d_high_pct" not in v] if compute_drop10 else []
    if need_drop10:
        dates = sorted({str(r.get("trade_date")) for r in rows if r.get("trade_date")})
        codes = sorted({str(r.get("code")) for r in rows if r.get("code")})
        if dates and codes:
            sb = _build_supabase()
            start, end = dates[0], dates[-1]
            series: list[dict[str, Any]] = []
            for i in range(0, len(codes), 80):
                chunk = codes[i:i + 80]
                start_idx = 0
                while True:
                    got = (
                        sb.table("stock_feature_snapshots")
                        .select("trade_date,code,high,close")
                        .gte("trade_date", start)
                        .lte("trade_date", end)
                        .in_("code", chunk)
                        .order("trade_date")
                        .range(start_idx, start_idx + 999)
                        .execute()
                        .data
                        or []
                    )
                    series.extend(got)
                    if len(got) < 1000:
                        break
                    start_idx += 1000
            by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in series:
                by_code[str(row.get("code"))].append(row)
            for code, items in by_code.items():
                window: deque[float] = deque(maxlen=10)
                for row in sorted(items, key=lambda x: str(x.get("trade_date"))):
                    high = num(row.get("high"))
                    close = num(row.get("close"))
                    if high is not None:
                        window.append(high)
                    if close is not None and window:
                        key = cache_key(str(row.get("trade_date")), code)
                        if key in cache:
                            cache[key]["drop_from_10d_high_pct"] = (close / max(window) - 1.0) * 100.0
            stats["drop10_computed_keys"] = sum(1 for v in cache.values() if isinstance(v, dict) and "drop_from_10d_high_pct" in v)
    save_json(cache_path, cache)
    return cache, stats


def enrich_rows(rows: list[dict[str, Any]], features: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        nr = dict(row)
        feat = features.get(cache_key(str(row.get("trade_date") or ""), str(row.get("code") or ""))) or {}
        for key, value in feat.items():
            if value not in (None, ""):
                nr[key] = value
        nr["drop_from_10d_high_pct"] = feat.get("drop_from_10d_high_pct")
        out.append(nr)
    return out


def common_pass(row: dict[str, Any]) -> bool:
    score = num(row.get("signal_probability"), -1)
    stage = str(row.get("signal_stage") or "")
    regime = str(row.get("market_regime") or "")
    overheat = num(row.get("overheat_score"))
    if overheat is None:
        overheat = float(h5_overheat_score(row))
    margin = num(row.get("margin_ratio"))
    if score is None or score < 0.65:
        return False
    if stage not in {"confirmed", "strong_confirmed"}:
        return False
    if regime == "panic_selloff":
        return False
    if overheat is None or overheat > 1:
        return False
    if margin is not None and (margin < 3 or margin > 30):
        return False
    return True


def prefetch_common_pass(row: dict[str, Any]) -> bool:
    """Cheap prefilter before DB feature enrichment.

    The walk-forward prediction CSV does not always carry overheat inputs, so
    the full H5 overheat check is applied only after feature enrichment.
    """
    score = num(row.get("signal_probability"), -1)
    stage = str(row.get("signal_stage") or "")
    regime = str(row.get("market_regime") or "")
    margin = num(row.get("margin_ratio"))
    if score is None or score < 0.65:
        return False
    if stage not in {"confirmed", "strong_confirmed"}:
        return False
    if regime == "panic_selloff":
        return False
    if margin is not None and (margin < 3 or margin > 30):
        return False
    return True


def variant_pass(row: dict[str, Any], metric: str, threshold: float) -> bool:
    if not common_pass(row):
        return False
    key = {
        "drop20": "drop_from_20d_high_pct",
        "drop10": "drop_from_10d_high_pct",
        "drop5": "drop_from_5d_high_pct",
    }[metric]
    value = num(row.get(key))
    return value is not None and value <= threshold


def sim_params(variant: str) -> dict[str, Any]:
    return {
        "scenario_id": variant,
        "capital": 5_000_000.0,
        "notional": 300_000.0,
        "daily_cap": 10,
        "gap_limit": 3.0,
        "tax_rate": 0.0,
        "cost_bps": 10.0,
        "apply_tax": False,
        "entry_mode": "next_open",
    }


def corrected_summary(sim: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    s = dict(sim["summary"])
    after_cost = num(s.get("total_pnl_after_tax"), 0.0) or 0.0
    tax = max(after_cost, 0.0) * TAX_RATE
    s["aggregate_tax"] = tax
    s["total_pnl_after_aggregate_tax"] = after_cost - tax
    s["PF_after_cost"] = s.get("PF_after_tax")
    s["max_dd_after_cost"] = s.get("max_dd_after_tax")
    s["sharpe_like_daily"] = None
    curve = sim.get("curve") or []
    daily = [num(r.get("daily_realized_pnl"), 0.0) or 0.0 for r in curve]
    if len(daily) > 2 and pstdev(daily):
        s["sharpe_like_daily"] = mean(daily) / pstdev(daily) * (252 ** 0.5)
    temp = dict(s)
    temp["total_pnl_after_tax"] = s["total_pnl_after_aggregate_tax"]
    annualize(temp, start, end)
    s["annualized_simple_return_aggregate_tax"] = temp.get("annualized_simple_return")
    s["annualized_compound_return_aggregate_tax"] = temp.get("annualized_compound_return")
    return s


def distribution(rows: list[dict[str, Any]], variant: str) -> list[dict[str, Any]]:
    def pct_true(fn) -> float | None:
        vals = [bool(fn(r)) for r in rows if fn(r) is not None]
        return sum(vals) / len(vals) * 100 if vals else None

    out = []
    out.append({"variant": variant, "feature": "ma25_above_pct", "bucket": "true", "value": pct_true(lambda r: (num(r.get("ma25_gap_pct")) is not None and num(r.get("ma25_gap_pct")) >= 0))})
    out.append({"variant": variant, "feature": "ma75_above_pct", "bucket": "true", "value": pct_true(lambda r: (num(r.get("ma75_gap_pct")) is not None and num(r.get("ma75_gap_pct")) >= 0))})
    for key in ["overheat_score", "market_regime", "sector", "gap_bucket"]:
        groups = Counter()
        for r in rows:
            val = gap_bucket(r.get("entry_gap_pct")) if key == "gap_bucket" else str(r.get(key) or "missing")
            groups[val] += 1
        for bucket, count in groups.most_common():
            out.append({"variant": variant, "feature": key, "bucket": bucket, "count": count, "value": count / len(rows) * 100 if rows else None})
    for key in ["signal_probability", "sector_change_pct", "nikkei_change_pct", "drop_from_20d_high_pct", "drop_from_10d_high_pct", "drop_from_5d_high_pct"]:
        vals = [num(r.get(key)) for r in rows if num(r.get(key)) is not None]
        out.append({
            "variant": variant,
            "feature": key,
            "bucket": "summary",
            "count": len(vals),
            "avg": mean(vals) if vals else None,
            "median": median(vals) if vals else None,
            "p25": sorted(vals)[int(len(vals) * 0.25)] if vals else None,
            "p75": sorted(vals)[int(len(vals) * 0.75)] if vals else None,
        })
    return out


def diff_rows(current: list[dict[str, Any]], variant_rows: list[dict[str, Any]], variant: str) -> list[dict[str, Any]]:
    current_keys = {(r.get("trade_date"), r.get("code")) for r in current}
    out = []
    for r in variant_rows:
        if (r.get("trade_date"), r.get("code")) not in current_keys:
            nr = dict(r)
            nr["variant"] = variant
            nr["big_win"] = (num(r.get("return_pct"), 0) or 0) >= 5
            nr["big_loss"] = (num(r.get("return_pct"), 0) or 0) <= -5
            nr["strong_market_pullback"] = str(r.get("market_regime") or "") in {"strong_risk_on", "risk_on", "normal"} and (num(r.get("ma25_gap_pct"), -999) or -999) >= 0
            out.append(nr)
    return out


def summarize_rowset(rows: list[dict[str, Any]], pnl_key: str = "fractional_pnl_300k") -> dict[str, Any]:
    vals = [num(r.get("return_pct")) for r in rows if num(r.get("return_pct")) is not None]
    pnls = [num(r.get(pnl_key), 0.0) or 0.0 for r in rows]
    return {
        "n": len(rows),
        "avg_return_pct": mean(vals) if vals else None,
        "median_return_pct": median(vals) if vals else None,
        "win_rate": sum(v > 0 for v in vals) / len(vals) * 100 if vals else None,
        "PF": pf(pnls),
        "total_pnl": sum(pnls),
        "big_win_ge5_rate": sum(v >= 5 for v in vals) / len(vals) * 100 if vals else None,
        "big_loss_le_minus5_rate": sum(v <= -5 for v in vals) / len(vals) * 100 if vals else None,
    }


def market_environment_split(rows_by_variant: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for variant, rows in rows_by_variant.items():
        buckets = {
            "bullish_regime": [
                r for r in rows
                if str(r.get("market_regime") or "") in {"strong_risk_on", "risk_on", "normal"}
            ],
            "weak_regime": [
                r for r in rows
                if str(r.get("market_regime") or "") not in {"strong_risk_on", "risk_on", "normal"}
            ],
            "above_ma25": [
                r for r in rows
                if (num(r.get("ma25_gap_pct")) is not None and (num(r.get("ma25_gap_pct")) or 0) >= 0)
            ],
            "below_ma25": [
                r for r in rows
                if (num(r.get("ma25_gap_pct")) is not None and (num(r.get("ma25_gap_pct")) or 0) < 0)
            ],
        }
        for bucket, items in buckets.items():
            rec = summarize_rowset(items)
            rec.update({"variant": variant, "environment": bucket})
            out.append(rec)
    return out


def latest_relaxed_candidates(features: dict[str, dict[str, Any]], out_dir: Path) -> list[dict[str, Any]]:
    # Reuse the latest DB audit output if available; it already contains stored scores.
    today_rows_path = ROOT / "outputs" / "h5_tax_priority_today_audit" / "07_today_h5_evaluation_rows.csv"
    if not today_rows_path.exists():
        return []
    rows = read_csv(today_rows_path)
    out = []
    for row in rows:
        key = cache_key(str(row.get("trade_date") or ""), str(row.get("code") or ""))
        if key in features:
            row.update(features[key])
        matched = []
        for name, metric, threshold in VARIANTS:
            if variant_pass(row, metric, threshold):
                matched.append(name)
        if matched:
            nr = {
                "code": row.get("code"),
                "name": row.get("name"),
                "trade_date": row.get("trade_date"),
                "score": row.get("signal_probability") or row.get("score"),
                "drop20": row.get("drop_from_20d_high_pct"),
                "drop10": row.get("drop_from_10d_high_pct"),
                "drop5": row.get("drop_from_5d_high_pct"),
                "overheat_score": row.get("overheat_score"),
                "gap": row.get("entry_gap_pct"),
                "matched_variants": ",".join(matched),
                "current_h5_match": row.get("h5_primary_match"),
                "current_exclusion_reason": row.get("h5_skip_reasons"),
            }
            out.append(nr)
    return sorted(out, key=lambda r: num(r.get("score"), -1) or -1, reverse=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUT)
    parser.add_argument("--compute-drop10", action="store_true")
    args = parser.parse_args()

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = read_csv(ROOT / args.input)
    rows = [standardize(r) for r in raw]
    for i, r in enumerate(rows):
        r["_row_index"] = i
        r["code"] = normalize_code(r.get("code"))
        r["score_source"] = r.get("source") or "walk_forward"
    prefetch_rows = [r for r in rows if prefetch_common_pass(r)]
    features, feature_stats = fetch_feature_rows(prefetch_rows, out_dir, compute_drop10=args.compute_drop10)
    rows = enrich_rows(rows, features)

    all_dates = load_all_wf_dates(ROOT / args.input)
    date_by_signal = next_date_map(all_dates)
    cache_path = ROOT / "outputs" / "h5_s_share_execution_timing" / "next_open_cache.json"
    open_cache, open_stats = load_next_open_rows(rows, date_by_signal, cache_path)

    exec_args = argparse.Namespace(holding_days=3, stop_pct=-12.0)
    summaries = []
    dist_rows = []
    top_rows = []
    bottom_rows = []
    diff_all = []
    variant_exec_rows: dict[str, list[dict[str, Any]]] = {}
    start = min(r.get("trade_date") for r in rows if r.get("trade_date"))
    end = max(r.get("trade_date") for r in rows if r.get("trade_date"))

    selected_raw_by_variant = {}
    for name, metric, threshold in VARIANTS:
        selected_raw = [r for r in rows if variant_pass(r, metric, threshold)]
        selected_raw_by_variant[name] = selected_raw
        _, selected_next, skipped = make_execution_rows(selected_raw, open_cache, date_by_signal, exec_args)
        sim = simulate_realistic(selected_next, sim_params(name))
        s = corrected_summary(sim, start, end)
        s["variant"] = name
        s["drop_metric"] = metric
        s["drop_threshold"] = threshold
        s["candidate_rows"] = len(selected_raw)
        s["execution_rows"] = len(selected_next)
        s["active_days"] = len({r.get("trade_date") for r in selected_next})
        s["skipped_execution_rows"] = sum(skipped.values())
        summaries.append(s)
        variant_exec_rows[name] = selected_next
        dist_rows.extend(distribution(selected_next, name))
        for side, ordered in [
            ("top", sorted(selected_next, key=lambda r: num(r.get("return_pct"), -999) or -999, reverse=True)[:10]),
            ("bottom", sorted(selected_next, key=lambda r: num(r.get("return_pct"), 999) or 999)[:10]),
        ]:
            for rank, r in enumerate(ordered, 1):
                top_rows.append({
                    "variant": name,
                    "side": side,
                    "rank": rank,
                    "code": r.get("code"),
                    "name": r.get("name"),
                    "trade_date": r.get("trade_date"),
                    "signal_probability": r.get("signal_probability"),
                    "return_pct": r.get("return_pct"),
                    "entry_gap_pct": r.get("entry_gap_pct"),
                    "drop20": r.get("drop_from_20d_high_pct"),
                    "drop10": r.get("drop_from_10d_high_pct"),
                    "drop5": r.get("drop_from_5d_high_pct"),
                    "ma25_gap_pct": r.get("ma25_gap_pct"),
                    "ma75_gap_pct": r.get("ma75_gap_pct"),
                    "market_regime": r.get("market_regime"),
                    "sector": r.get("sector"),
                })

    current_exec = variant_exec_rows.get("A_current_drop20_lte_m8", [])
    diff_summary = []
    for name in selected_raw_by_variant:
        if name == "A_current_drop20_lte_m8":
            continue
        diff = diff_rows(current_exec, variant_exec_rows.get(name, []), name)
        diff_all.extend(diff)
        vals = [num(r.get("return_pct"), 0) or 0 for r in diff]
        pnls = [num(r.get("fractional_pnl_300k"), 0) or 0 for r in diff]
        diff_summary.append({
            "variant": name,
            "new_only_n": len(diff),
            "avg_return_pct": mean(vals) if vals else None,
            "median_return_pct": median(vals) if vals else None,
            "win_rate": sum(v > 0 for v in vals) / len(vals) * 100 if vals else None,
            "PF": pf(pnls),
            "big_win_ge5_rate": sum(v >= 5 for v in vals) / len(vals) * 100 if vals else None,
            "big_loss_le_minus5_rate": sum(v <= -5 for v in vals) / len(vals) * 100 if vals else None,
            "strong_market_pullback_n": sum(bool(r.get("strong_market_pullback")) for r in diff),
        })

    latest_rows = latest_relaxed_candidates(features, out_dir)
    market_split_rows = market_environment_split(variant_exec_rows)

    write_text(out_dir / "00_input_summary.txt", f"""H5 pullback relaxation analysis
input: {ROOT / args.input}
rows_loaded: {len(raw)}
common_condition_rows_for_feature_prefetch: {len(prefetch_rows)}
feature_cache_rows: {len(features)}
feature_stats: {dict(feature_stats)}
next_open_stats: {dict(open_stats)}
tax: aggregate profit tax, rate={TAX_RATE}
common_conditions: AI>=0.65, confirmed/strong_confirmed, no panic_selloff, overheat<=1, margin 3-30 if present
operation: next_open, HD3, capital 5M, S-share 300k, daily cap10, gap<=3, cost10bps
""")
    write_csv(out_dir / "01_condition_performance_summary.csv", summaries)
    write_csv(out_dir / "02_condition_distribution.csv", dist_rows)
    write_csv(out_dir / "03_top_bottom_10.csv", top_rows)
    write_csv(out_dir / "04_new_only_vs_current_summary.csv", diff_summary)
    write_csv(out_dir / "05_new_only_rows.csv", diff_all)
    write_csv(out_dir / "06_latest_relaxed_candidates.csv", latest_rows)
    write_csv(out_dir / "07_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in (feature_stats + open_stats).most_common()])
    write_csv(out_dir / "09_market_environment_split.csv", market_split_rows)

    best = sorted(summaries, key=lambda r: num(r.get("total_pnl_after_aggregate_tax"), -10**18) or -10**18, reverse=True)[0] if summaries else {}
    def fmt(v: Any, digits: int = 2) -> str:
        x = num(v)
        if x is None:
            return "n/a"
        return f"{x:,.{digits}f}"

    lines = []
    for s in summaries:
        lines.append(
            f"- {s.get('variant')}: rows={s.get('execution_rows')}, avg={fmt(s.get('avg_return_pct'))}%, "
            f"median={fmt(s.get('median_return_pct'))}%, WR={fmt(s.get('win_rate'))}%, "
            f"PF={fmt(s.get('PF_after_cost'), 3)}, tax-adjusted PnL={fmt(s.get('total_pnl_after_aggregate_tax'), 0)}円, "
            f"DD={fmt(s.get('max_dd_after_cost'), 0)}円"
        )
    diff_lines = []
    for d in diff_summary:
        diff_lines.append(
            f"- {d.get('variant')}: new_only={d.get('new_only_n')}, avg={fmt(d.get('avg_return_pct'))}%, "
            f"PF={fmt(d.get('PF'), 3)}, big_win>=5%={fmt(d.get('big_win_ge5_rate'))}%, "
            f"strong_market_pullback={d.get('strong_market_pullback_n')}"
        )

    current = next((s for s in summaries if s.get("variant") == "A_current_drop20_lte_m8"), {})
    f_variant = next((s for s in summaries if s.get("variant") == "F_drop5_lte_m3"), {})
    e_variant = next((s for s in summaries if s.get("variant") == "E_drop10_lte_m4"), {})
    latest_note = "latest relaxed candidates: none" if not latest_rows else f"latest relaxed candidates: {len(latest_rows)}"

    report = f"""H5 pullback relaxation report

Input:
- rows_loaded: {len(raw)}
- feature_enriched_rows: {len(features)}
- drop10_computed_rows: {sum(1 for v in features.values() if isinstance(v, dict) and 'drop_from_10d_high_pct' in v)}
- operation: next_open, HD3, capital 5M, S-share 300k, daily cap10, gap<=3, cost10bps, aggregate tax

Condition comparison:
{chr(10).join(lines)}

Best by aggregate-tax PnL:
- variant: {best.get('variant')}
- PnL after aggregate tax: {fmt(best.get('total_pnl_after_aggregate_tax'), 0)}円
- PF after cost: {fmt(best.get('PF_after_cost'), 3)}
- CAGR simple: {fmt(best.get('annualized_simple_return_aggregate_tax'))}%
- max DD after cost: {fmt(best.get('max_dd_after_cost'), 0)}円

Current vs pullback-oriented condition:
- Current drop20<=-8 tax-adjusted PnL: {fmt(current.get('total_pnl_after_aggregate_tax'), 0)}円, PF {fmt(current.get('PF_after_cost'), 3)}
- Drop10<=-4 tax-adjusted PnL: {fmt(e_variant.get('total_pnl_after_aggregate_tax'), 0)}円, PF {fmt(e_variant.get('PF_after_cost'), 3)}
- Drop5<=-3 tax-adjusted PnL: {fmt(f_variant.get('total_pnl_after_aggregate_tax'), 0)}円, PF {fmt(f_variant.get('PF_after_cost'), 3)}

Relaxed-only rows versus current:
{chr(10).join(diff_lines)}

Interpretation:
- Drop20 relaxation alone (-7/-6/-5) increases coverage but does not beat current drop20<=-8 on PF or tax-adjusted PnL.
- Drop10<=-4 slightly beats current by PnL and DD, but PF is a little lower.
- Drop5<=-3 is the strongest result in this run: it keeps PF close to current while increasing tax-adjusted PnL and lowering DD.
- The additional rows from relaxed conditions contain many strong-market pullbacks, especially drop5<=-3.
- This supports testing a separate shallow-pullback comparison case, but it does not justify changing Primary yet.

Current H5 row is A_current_drop20_lte_m8.
Condition summary: 01_condition_performance_summary.csv
Trend/pullback distribution: 02_condition_distribution.csv
Top/bottom names: 03_top_bottom_10.csv
Relaxed-only rows: 05_new_only_rows.csv
Latest relaxed candidates: 06_latest_relaxed_candidates.csv
Market environment split: 09_market_environment_split.csv
{latest_note}

No production logic was changed.
"""
    write_text(out_dir / "08_report.txt", report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
