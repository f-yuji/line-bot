"""Audit H5 tax drag, skipped AI70 candidates, and today's zero-signal state.

Analysis only. Does not mutate Primary, H5 rules, DB case definitions, UI,
LINE, actual_trade_logs, or auto-trading state.
"""

from __future__ import annotations

import csv
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.h5_primary import evaluate_h5_primary_entry, h5_overheat_score
from services.trade_case_tester import _build_supabase

from analyze_h5_s_share_realistic_operation import (
    annualize,
    parse_date,
    pf,
    scenario_id,
    simulate_realistic,
)
from analyze_h5_primary_fractional_sizing import to_float, write_csv, write_text


BASE = ROOT / "outputs" / "h5_s_share_realistic_operation"
OUT = ROOT / "outputs" / "h5_tax_priority_today_audit"
SCENARIO = "cap5000000_not300000_dcap10_gap3_0_cost10_tax"
CAPITAL = 5_000_000.0
TAX_RATE = 0.20315


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def fnum(value: Any, default: float = 0.0) -> float:
    out = to_float(value)
    return default if out is None or math.isnan(out) else out


def pct(value: float | None) -> float | None:
    return None if value is None else value * 100.0


def metrics(rows: list[dict[str, Any]], pnl_key: str = "_pnl_after_tax") -> dict[str, Any]:
    rets = [fnum(r.get("return_pct")) for r in rows]
    pnls = [fnum(r.get(pnl_key)) for r in rows]
    gp = sum(x for x in pnls if x > 0)
    gl = -sum(x for x in pnls if x < 0)
    wins = sum(x > 0 for x in rets)
    return {
        "n": len(rows),
        "avg_return_pct": sum(rets) / len(rets) if rets else None,
        "win_rate": wins / len(rows) * 100 if rows else None,
        "total_pnl": sum(pnls),
        "profit_factor": gp / gl if gl else None,
        "max_return_pct": max(rets) if rets else None,
        "min_return_pct": min(rets) if rets else None,
    }


def tax_audit(executed: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    before = sum(fnum(r.get("_pnl_before_cost_tax")) for r in executed)
    total_cost = sum(fnum(r.get("_round_trip_cost")) for r in executed)
    after_cost = sum(fnum(r.get("_pnl_after_cost")) for r in executed)
    per_trade_tax = sum(fnum(r.get("_tax")) for r in executed)
    per_trade_final = sum(fnum(r.get("_pnl_after_tax")) for r in executed)
    aggregate_tax = max(after_cost, 0.0) * TAX_RATE
    aggregate_final = after_cost - aggregate_tax
    no_tax_cost10_final = after_cost
    rows = [
        {
            "method": "pretax_no_cost_reference",
            "pnl_before_cost_tax": before,
            "total_cost": 0,
            "tax": 0,
            "final_pnl": before,
            "tax_basis": "none",
        },
        {
            "method": "cost10bps_no_tax",
            "pnl_before_cost_tax": before,
            "total_cost": total_cost,
            "tax": 0,
            "final_pnl": no_tax_cost10_final,
            "tax_basis": "none",
        },
        {
            "method": "current_per_winning_trade_tax",
            "pnl_before_cost_tax": before,
            "total_cost": total_cost,
            "tax": per_trade_tax,
            "final_pnl": per_trade_final,
            "tax_basis": "sum(max(trade_pnl_after_cost, 0))",
        },
        {
            "method": "corrected_aggregate_profit_tax",
            "pnl_before_cost_tax": before,
            "total_cost": total_cost,
            "tax": aggregate_tax,
            "final_pnl": aggregate_final,
            "tax_basis": "max(total_pnl_after_cost, 0)",
        },
    ]
    summary = {
        "before_cost_tax": before,
        "total_cost": total_cost,
        "after_cost_before_tax": after_cost,
        "per_trade_tax": per_trade_tax,
        "per_trade_final": per_trade_final,
        "aggregate_tax": aggregate_tax,
        "aggregate_final": aggregate_final,
        "tax_overstatement": per_trade_tax - aggregate_tax,
        "cost_definition": "round-trip bps on actual entry notional; not profit-based; 10bps means total round trip 0.10%",
    }
    return rows, summary


def clone_with_priority(rows: list[dict[str, Any]], mode: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("entry_date") or row.get("trade_date") or "")].append(dict(row))

    def prio(row: dict[str, Any]) -> tuple:
        score = fnum(row.get("signal_probability"), -1)
        gap = fnum(row.get("entry_gap_pct"), 999)
        original = int(fnum(row.get("_row_index"), 0))
        if mode == "current_first":
            return (original,)
        if mode == "ai_ge_070_priority":
            return (0 if score >= 0.70 else 1, -score, original)
        if mode == "ai_070_080_priority":
            return (0 if 0.70 <= score < 0.80 else 1, -score, original)
        if mode == "ai_ge_080_priority":
            return (0 if score >= 0.80 else 1, -score, original)
        if mode == "ai_ge_070_gap_le_3_priority":
            return (0 if score >= 0.70 and gap <= 3 else 1, -score, gap, original)
        if mode == "ai_ge_070_gap_lt_0_priority":
            return (0 if score >= 0.70 and gap < 0 else 1, -score, gap, original)
        if mode == "ai_070_080_gap_lt_0_priority":
            return (0 if 0.70 <= score < 0.80 and gap < 0 else 1, -score, gap, original)
        return (original,)

    out: list[dict[str, Any]] = []
    idx = 0
    for day in sorted(grouped):
        for row in sorted(grouped[day], key=prio):
            row["_row_index"] = idx
            row["_priority_mode"] = mode
            idx += 1
            out.append(row)
    return out


def priority_audit(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    params = {
        "capital": CAPITAL,
        "notional": 300_000.0,
        "daily_cap": 10,
        "gap_limit": 3.0,
        "tax_rate": TAX_RATE,
        "cost_bps": 10.0,
        "apply_tax": True,
        "entry_mode": "next_open",
    }
    modes = [
        "current_first",
        "ai_ge_070_priority",
        "ai_070_080_priority",
        "ai_ge_080_priority",
        "ai_ge_070_gap_le_3_priority",
        "ai_ge_070_gap_lt_0_priority",
        "ai_070_080_gap_lt_0_priority",
    ]
    summaries = []
    executed_all = []
    skipped_all = []
    start = min(str(r.get("entry_date") or r.get("trade_date")) for r in rows if r.get("entry_date") or r.get("trade_date"))
    end = max(str(r.get("exit_date") or r.get("entry_date") or r.get("trade_date")) for r in rows if r.get("entry_date") or r.get("trade_date"))
    for mode in modes:
        ordered = clone_with_priority(rows, mode)
        params["scenario_id"] = scenario_id(params) + "_" + mode
        result = simulate_realistic(ordered, params)
        s = dict(result["summary"])
        s["priority_mode"] = mode
        annualize(s, start, end)
        summaries.append(s)
        for r in result["executed"]:
            nr = dict(r)
            nr["priority_mode"] = mode
            executed_all.append(nr)
        for r in result["skipped"]:
            nr = dict(r)
            nr["priority_mode"] = mode
            skipped_all.append(nr)
    return summaries, executed_all, skipped_all


def skipped_executed_diff(executed: list[dict[str, Any]], skipped: list[dict[str, Any]], *, score70: bool) -> list[dict[str, Any]]:
    ex_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    sk_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in executed:
        if score70 and fnum(row.get("signal_probability")) < 0.70:
            continue
        ex_by_day[str(row.get("trade_date") or row.get("entry_date"))].append(row)
    for row in skipped:
        if score70 and fnum(row.get("signal_probability")) < 0.70:
            continue
        sk_by_day[str(row.get("trade_date") or row.get("entry_date"))].append(row)
    out = []
    for day in sorted(set(ex_by_day) & set(sk_by_day)):
        ex = ex_by_day[day]
        sk = sk_by_day[day]
        ex_avg = sum(fnum(r.get("return_pct")) for r in ex) / len(ex)
        sk_avg = sum(fnum(r.get("return_pct")) for r in sk) / len(sk)
        ex_max = max(fnum(r.get("return_pct")) for r in ex)
        sk_max = max(fnum(r.get("return_pct")) for r in sk)
        out.append({
            "scope": "AI>=0.70" if score70 else "all",
            "trade_date": day,
            "executed_n": len(ex),
            "skipped_n": len(sk),
            "executed_avg_return_pct": ex_avg,
            "skipped_avg_return_pct": sk_avg,
            "skipped_minus_executed_avg_pt": sk_avg - ex_avg,
            "executed_max_return_pct": ex_max,
            "skipped_max_return_pct": sk_max,
            "max_missed_winner_pt": sk_max - ex_max,
            "skipped_better_avg": sk_avg > ex_avg,
            "skipped_better_top": sk_max > ex_max,
        })
    return out


def fetch_all(sb, table: str, select: str, *, page_size: int = 1000, **eqs) -> list[dict[str, Any]]:
    out = []
    start = 0
    while True:
        q = sb.table(table).select(select)
        for key, value in eqs.items():
            q = q.eq(key, value)
        rows = q.range(start, start + page_size - 1).execute().data or []
        out.extend(rows)
        if len(rows) < page_size:
            break
        start += page_size
    return out


def market_regime_for(sb, trade_date: str) -> str:
    try:
        rows = (
            sb.table("market_regime")
            .select("trade_date,mode")
            .lte("trade_date", trade_date)
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return str(rows[0].get("mode") or "normal") if rows else "normal"
    except Exception:
        return "normal"


def today_h5_audit() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], Counter]:
    sb = _build_supabase()
    latest_snap = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    latest_pred = (
        sb.table("model_predictions")
        .select("trade_date,model_key,model_version,source")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    snap_date = str(latest_snap[0]["trade_date"]) if latest_snap else ""
    pred_meta = latest_pred[0] if latest_pred else {}
    pred_date = str(pred_meta.get("trade_date") or "")
    model_key = str(pred_meta.get("model_key") or "rebound_lgbm_5d")
    model_version = str(pred_meta.get("model_version") or "")
    source = str(pred_meta.get("source") or "daily_prediction")
    regime = market_regime_for(sb, pred_date or snap_date)

    preds = fetch_all(
        sb,
        "model_predictions",
        "code,trade_date,model_key,model_version,source,signal_probability,signal_stage,prediction_date,created_at",
        trade_date=pred_date,
        model_key=model_key,
        model_version=model_version,
        source=source,
    )
    snaps = fetch_all(
        sb,
        "stock_feature_snapshots",
        "code,name,trade_date,open,high,low,close,volume,turnover_value,drop_from_20d_high_pct,rsi14,volume_ratio_20d,margin_ratio,day_change_pct,ma5_gap_pct,ma25_gap_pct,ma75_gap_pct,is_tradeable,exclude_reason",
        trade_date=snap_date,
    )
    snap_by_code = {str(r.get("code")): r for r in snaps}
    rows = []
    counts: Counter = Counter()
    scores = []
    for pred in preds:
        code = str(pred.get("code") or "")
        snap = snap_by_code.get(code, {})
        score = to_float(pred.get("signal_probability"))
        if score is None:
            counts["score_nan"] += 1
        else:
            scores.append(score)
        row = {**snap, **pred}
        row["market_regime"] = regime
        row["overheat_score"] = h5_overheat_score({**row, "market_regime": regime})
        passed, reasons, meta = evaluate_h5_primary_entry(row)
        row["h5_primary_match"] = passed
        row["h5_skip_reason"] = "" if passed else (reasons[0] if reasons else "")
        row["h5_skip_reasons"] = ",".join(reasons)
        row["drop_from_20d_high_pct"] = snap.get("drop_from_20d_high_pct")
        row["margin_ratio"] = snap.get("margin_ratio")
        row["name"] = snap.get("name")
        row["close"] = snap.get("close")
        row["score"] = score
        rows.append(row)
        counts["predictions"] += 1
        counts["h5_match" if passed else "h5_not_match"] += 1
        if not passed:
            for reason in reasons or ["unknown"]:
                counts[reason] += 1
        if score is not None and score >= 0.65:
            counts["score_ge_065"] += 1
        if str(pred.get("signal_stage")) in {"confirmed", "strong_confirmed"}:
            counts["stage_confirmed"] += 1
        drop = to_float(snap.get("drop_from_20d_high_pct"))
        if drop is not None and drop <= -8:
            counts["drop_lte_m8"] += 1
        margin = to_float(snap.get("margin_ratio"))
        if margin is not None and 3 <= margin <= 30:
            counts["margin_3_30"] += 1
        if row["overheat_score"] <= 1:
            counts["overheat_lte_1"] += 1

    top = sorted(rows, key=lambda r: fnum(r.get("score"), -1), reverse=True)[:20]
    summary = {
        "snapshot_latest_trade_date": snap_date,
        "prediction_latest_trade_date": pred_date,
        "model_key": model_key,
        "model_version": model_version,
        "source": source,
        "market_regime": regime,
        "snapshot_rows": len(snaps),
        "prediction_rows": len(preds),
        "joined_rows": sum(1 for p in preds if str(p.get("code")) in snap_by_code),
        "score_nan_count": counts["score_nan"],
        "score_max": max(scores) if scores else None,
        "score_mean": sum(scores) / len(scores) if scores else None,
        "score_p95": sorted(scores)[int(len(scores) * 0.95)] if scores else None,
        "h5_match_count": counts["h5_match"],
        "today_jst": date.today().isoformat(),
    }
    return rows, top, summary, counts


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    executed = [r for r in read_csv(BASE / "10_executed_trades.csv") if r.get("scenario_id") == SCENARIO]
    skipped = [r for r in read_csv(BASE / "11_skipped_trades.csv") if r.get("scenario_id") == SCENARIO]
    normalized = read_csv(BASE / "01_normalized_dataset.csv")

    tax_rows, tax_summary = tax_audit(executed)
    write_csv(OUT / "01_tax_recalculation_audit.csv", tax_rows)

    priority_rows, priority_executed, priority_skipped = priority_audit(normalized)
    write_csv(OUT / "02_priority_rule_comparison.csv", priority_rows)
    write_csv(OUT / "03_priority_executed_sample.csv", priority_executed[:5000])
    write_csv(OUT / "04_priority_skipped_sample.csv", priority_skipped[:5000])

    diff_rows = skipped_executed_diff(executed, skipped, score70=False) + skipped_executed_diff(executed, skipped, score70=True)
    write_csv(OUT / "05_skipped_vs_executed_daily_diff.csv", diff_rows)

    score_rows = []
    for label, rows in [("executed", executed), ("skipped", skipped)]:
        for threshold in [0.65, 0.70, 0.75, 0.80]:
            subset = [r for r in rows if fnum(r.get("signal_probability"), -1) >= threshold]
            rec = {"group": label, "score_condition": f">={threshold}"}
            rec.update(metrics(subset))
            score_rows.append(rec)
    write_csv(OUT / "06_ai_score70_skipped_summary.csv", score_rows)

    today_rows, top20, today_summary, today_counts = today_h5_audit()
    write_csv(OUT / "07_today_h5_evaluation_rows.csv", today_rows)
    write_csv(OUT / "08_today_score_top20.csv", [
        {
            "code": r.get("code"),
            "name": r.get("name"),
            "score": r.get("score"),
            "signal_stage": r.get("signal_stage"),
            "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
            "market_regime": r.get("market_regime"),
            "overheat_score": r.get("overheat_score"),
            "margin_ratio": r.get("margin_ratio"),
            "h5_primary_match": r.get("h5_primary_match"),
            "h5_skip_reasons": r.get("h5_skip_reasons"),
            "close": r.get("close"),
            "volume_ratio_20d": r.get("volume_ratio_20d"),
        }
        for r in top20
    ])
    write_csv(OUT / "09_today_filter_counts.csv", [{"filter_or_reason": k, "count": v} for k, v in today_counts.most_common()])

    skipped_better = [r for r in diff_rows if r["scope"] == "AI>=0.70" and r["skipped_better_avg"]]
    max_miss = max([fnum(r.get("max_missed_winner_pt")) for r in diff_rows if r["scope"] == "AI>=0.70"], default=0)
    report = f"""H5 tax / priority / today-zero audit

Scenario audited: {SCENARIO}

1. Tax calculation
- Current script taxes each winning trade after cost:
  tax = max(trade_pnl_after_cost, 0) * {TAX_RATE}
- This is intentionally conservative but does not model annual aggregate profit/loss netting.
- Cost definition: {tax_summary['cost_definition']}

Tax rows:
{tax_rows}

Tax overstatement vs aggregate-profit tax:
{tax_summary['tax_overstatement']:.0f} yen

2. Priority / skipped right-tail
- Current daily cap order is input/order-by-date row order, not AI priority.
- Priority comparison is in 02_priority_rule_comparison.csv.
- Skipped/executed score bucket comparison is in 06_ai_score70_skipped_summary.csv.
- AI>=0.70 skipped-better-average days: {len(skipped_better)}
- Max missed-winner gap vs same-day executed top, AI>=0.70 scope: {max_miss:.4f} pt

3. Today's H5 zero check
Today JST: {today_summary.get('today_jst')}
Latest snapshot date: {today_summary.get('snapshot_latest_trade_date')}
Latest prediction date: {today_summary.get('prediction_latest_trade_date')}
Prediction rows: {today_summary.get('prediction_rows')}
Snapshot rows: {today_summary.get('snapshot_rows')}
Joined rows: {today_summary.get('joined_rows')}
Model: {today_summary.get('model_key')} / {today_summary.get('model_version')} / {today_summary.get('source')}
Market regime: {today_summary.get('market_regime')}
Score max: {today_summary.get('score_max')}
Score mean: {today_summary.get('score_mean')}
Score p95: {today_summary.get('score_p95')}
Score NaN count: {today_summary.get('score_nan_count')}
H5 match count: {today_summary.get('h5_match_count')}

Top20 and filter counts are in 08_today_score_top20.csv and 09_today_filter_counts.csv.

No Primary, H5 rule, DB case definition, UI, LINE, actual_trade_logs, or auto-trading changes were made.
"""
    write_text(OUT / "10_audit_report.txt", report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
