"""Analyze H5 using stored model predictions only.

This script can use either model_predictions from DB or a predictions CSV
(for example the walk-forward CSV) as a stored-prediction equivalent.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import analyze_h5_walk_forward_baseline as wf
from services.model_predictions import load_model_predictions
from services.trade_case_tester import _build_supabase, _load_candidates_v2


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    wf.write_csv(path, rows)


def prediction_csv_from_db(output_dir: Path, args: argparse.Namespace) -> Path:
    sb = _build_supabase()
    start = wf.parse_date(args.test_start)
    end = wf.parse_date(args.test_end) if args.test_end != "latest" else wf.date.today()
    candidates = _load_candidates_v2(
        sb,
        start,
        end,
        score_source="stored_predictions",
        model_key=args.model_key,
        model_version=None if args.model_version == "latest" else args.model_version,
        allow_score_fallback=args.allow_score_fallback,
    )
    rows = []
    for row in candidates:
        out = dict(row)
        out["trade_date"] = str(row.get("trade_date") or row.get("label_trade_date"))
        rows.append(out)
    path = output_dir / "_stored_predictions_joined_candidates.csv"
    write_csv(path, rows)
    return path


def run_analysis(rows: list[dict], output_dir: Path, active_baseline: Path, walk_forward_baseline: Path) -> dict:
    wf.attach_returns(rows)
    rows = [row for row in rows if row.get("_hd3") is not None]
    h5_rows = wf.rows_for_strategy(rows, wf.passes_h5_full)

    perf_rows = []
    for name, pred in wf.STRATEGIES:
        perf_rows.append(wf.summarize(wf.rows_for_strategy(rows, pred), name, "test"))

    seed_rows, _draws = wf.random_same_day(rows, h5_rows, exclude_h5=True)
    sector_seed = wf.group_random(rows, h5_rows, lambda r: str(r.get("sector") or "unknown"), "same_sector_random")
    vol_seed = wf.group_random(rows, h5_rows, wf.volume_bucket, "same_volume_bucket_random")
    drop_seed = wf.group_random(rows, h5_rows, wf.drop_bucket, "same_drop_bucket_random")
    perf_rows.extend([
        wf.mean_seed_summary(seed_rows, "market_random_same_day_exclude_h5_mean"),
        wf.mean_seed_summary(sector_seed, "same_sector_random_mean"),
        wf.mean_seed_summary(vol_seed, "same_volume_bucket_random_mean"),
        wf.mean_seed_summary(drop_seed, "same_drop_bucket_random_mean"),
    ])

    write_csv(output_dir / "01_strategy_performance_matrix.csv", perf_rows)
    ablation = []
    previous = None
    ai_drop = None
    for name, pred in wf.STRATEGIES[:7]:
        summary = wf.summarize(wf.rows_for_strategy(rows, pred), name, "test")
        cur = wf.to_float(summary.get("HD3_avg"), None)
        prev = wf.to_float(previous.get("HD3_avg"), None) if previous else None
        summary["delta_vs_previous"] = cur - prev if cur is not None and prev is not None else None
        if name == "AI_plus_drop":
            ai_drop = summary
        ai_drop_v = wf.to_float(ai_drop.get("HD3_avg"), None) if ai_drop else None
        summary["delta_vs_AI_plus_drop"] = cur - ai_drop_v if cur is not None and ai_drop_v is not None else None
        ablation.append(summary)
        previous = summary
    write_csv(output_dir / "02_filter_ablation.csv", ablation)
    write_csv(output_dir / "03_market_random_comparison.csv", seed_rows + sector_seed + vol_seed + drop_seed)
    write_csv(output_dir / "04_k_no_normal_comparison.csv", [wf.summarize(wf.rows_for_strategy(rows, wf.passes_k_no_normal), "K_no_normal", "test")])

    active = wf.load_active_baseline(active_baseline)
    stored_by = {(r.get("strategy"), r.get("period")): r for r in perf_rows}
    compare = []
    for row in active:
        compare.append({**row, "version": "active_model_rescore", "delta_stored_minus_active": None})
        stored = stored_by.get((row.get("strategy"), "test"))
        if stored:
            s = dict(stored)
            s["version"] = "stored_predictions"
            s["delta_stored_minus_active"] = (wf.to_float(s.get("HD3_avg"), None) or 0) - (wf.to_float(row.get("HD3_avg"), None) or 0)
            compare.append(s)
    if walk_forward_baseline.exists():
        wf_rows = wf.read_csv(walk_forward_baseline)
        for row in wf_rows:
            if row.get("strategy") in {"AI_only", "AI_plus_drop", "H5_full", "K_no_normal", "market_random_same_day_exclude_h5_mean"}:
                compare.append({**row, "version": "walk_forward_reference"})
    write_csv(output_dir / "05_active_vs_stored_comparison.csv", compare)

    monthly_rows = []
    by_month = {}
    for name, pred in wf.STRATEGIES:
        for row in wf.rows_for_strategy(rows, pred):
            by_month.setdefault((name, row["_month"]), []).append(row)
    for (name, month), group in sorted(by_month.items()):
        s = wf.summarize(group, name, "test")
        s["month"] = month
        vals = [row["_hd3"] for row in group if row.get("_hd3") is not None]
        s["HD3_total_return_sum"] = sum(vals)
        monthly_rows.append(s)
    write_csv(output_dir / "06_monthly_stability.csv", monthly_rows)

    regime_rows = []
    for name, pred in wf.STRATEGIES:
        groups = {}
        for row in wf.rows_for_strategy(rows, pred):
            groups.setdefault(str(row.get("market_regime") or "unknown"), []).append(row)
        for regime, group in sorted(groups.items()):
            s = wf.summarize(group, name, "test")
            s["regime"] = regime
            regime_rows.append(s)
    write_csv(output_dir / "07_regime_breakdown.csv", regime_rows)

    bucket_rows = []
    groups = {}
    for row in rows:
        groups.setdefault(wf.score_bucket(row), []).append(row)
    for bucket, group in sorted(groups.items()):
        s = wf.summarize(group, "score_bucket", "test")
        s["bucket"] = bucket
        bucket_rows.append(s)
    write_csv(output_dir / "08_ai_score_bucket_performance.csv", bucket_rows)
    write_csv(output_dir / "09_missing_score_rows.csv", [{"reason": "rows_loaded", "count": len(rows)}, {"reason": "h5_full_candidate_count", "count": len(h5_rows)}])

    by_name = {r["strategy"]: r for r in perf_rows}
    h5 = by_name.get("H5_full", {})
    rand = by_name.get("market_random_same_day_exclude_h5_mean", {})
    h5_minus_rand = (wf.to_float(h5.get("HD3_avg"), None) or 0) - (wf.to_float(rand.get("HD3_avg"), None) or 0)
    write_text(output_dir / "10_stored_prediction_baseline_report.txt", f"""
# H5 Stored Prediction Baseline Report

stored_prediction_rows: {len(rows)}
AI_only: HD3_avg={by_name.get('AI_only', {}).get('HD3_avg')} PF={by_name.get('AI_only', {}).get('PF_HD3')}
drop_only: HD3_avg={by_name.get('drop_only', {}).get('HD3_avg')} PF={by_name.get('drop_only', {}).get('PF_HD3')}
AI_plus_drop: HD3_avg={by_name.get('AI_plus_drop', {}).get('HD3_avg')} PF={by_name.get('AI_plus_drop', {}).get('PF_HD3')}
H5_full: HD3_avg={h5.get('HD3_avg')} PF={h5.get('PF_HD3')}
K_no_normal: HD3_avg={by_name.get('K_no_normal', {}).get('HD3_avg')} PF={by_name.get('K_no_normal', {}).get('PF_HD3')}
same_day_random_exclude_h5: HD3_avg={rand.get('HD3_avg')} PF={rand.get('PF_HD3')}
H5_full_minus_same_day_random: {h5_minus_rand}

fallback_used: false unless --allow-score-fallback was used in DB mode.
Primary change: no.
Comparison case candidates:
- h5_ai65_hd3_est12_stored_pred_full_research
- h5_ai65_hd3_est12_stored_pred_no_normal_research
- h5_ai65_hd3_est12_stored_pred_full_live_limited
""")
    return {"rows": len(rows), "h5": len(h5_rows), "h5_minus_random": h5_minus_rand}


def write_infrastructure_reports(
    infra_dir: Path,
    baseline_dir: Path,
    *,
    analysis_result: dict,
    prediction_source: str,
    allow_score_fallback: bool,
) -> None:
    infra_dir.mkdir(parents=True, exist_ok=True)
    write_text(infra_dir / "05_score_source_mode_report.txt", f"""
# score_source mode report

_load_candidates_v2 score_source added: yes

active_model:
- Uses the existing active model bundle and recomputes signal_probability.
- score_source is recorded as active_model_rescore.
- point_in_time_valid is false.

stored_predictions:
- Loads signal_probability from model_predictions.
- Does not call active model predict_proba.
- Missing scores are excluded unless allow_score_fallback is true.
- score_source is recorded as stored_predictions.

stored_or_active_fallback:
- Uses stored scores when present.
- Falls back to active model only for missing scores.
- Fallback rows are marked score_fallback_used=true.
- This mode is not recommended for strict verification.

current_analysis_prediction_source: {prediction_source}
allow_score_fallback: {allow_score_fallback}
stored_predictions_active_model_fallback_used: {allow_score_fallback}
Primary impact: none.
""")
    table_result = infra_dir / "02_model_predictions_table_create_result.txt"
    save_result = infra_dir / "03_prediction_save_test_result.txt"
    write_text(infra_dir / "06_stored_prediction_infrastructure_report.txt", f"""
# stored prediction infrastructure report

model_predictions DDL generated: yes
schema_sql: outputs/h5_stored_prediction_infrastructure/01_model_predictions_schema.sql
table_create_result_file_exists: {table_result.exists()}
save_load_test_file_exists: {save_result.exists()}

immutable save implementation:
- Existing rows are checked by code/trade_date/model_key/model_version.
- Existing rows are skipped, not updated.
- Same model_version re-runs do not overwrite signal_probability.
- New model_version creates a separate row.

daily prediction integration:
- scripts/predict_rebound.py now prepares prediction rows during daily prediction.
- It calls save_model_predictions after watchlist upsert when not dry_run.
- source is daily_prediction for model predictions and fallback_rule for fallback rows.

stored baseline:
- stored_prediction_rows: {analysis_result.get('rows')}
- h5_full_candidate_count: {analysis_result.get('h5')}
- h5_full_minus_same_day_random: {analysis_result.get('h5_minus_random')}

DB table status:
- Automatic DDL execution requires an exec_sql RPC and was not assumed.
- If table creation failed, run the generated SQL in Supabase SQL Editor.

recommended score_source for future verification:
- stored_predictions
""")
    missing_src = baseline_dir / "09_missing_score_rows.csv"
    active_cmp_src = baseline_dir / "05_active_vs_stored_comparison.csv"
    if missing_src.exists():
        shutil.copyfile(missing_src, infra_dir / "07_missing_prediction_summary.csv")
    if active_cmp_src.exists():
        shutil.copyfile(active_cmp_src, infra_dir / "08_active_vs_stored_comparison.csv")
    write_text(infra_dir / "09_next_steps.txt", """
# next steps

1. Run outputs/h5_stored_prediction_infrastructure/01_model_predictions_schema.sql in Supabase SQL Editor.
2. Rerun scripts/test_model_predictions_save_load.py and confirm PASS.
3. Run daily prediction once and confirm source=daily_prediction rows are inserted.
4. Use score_source=stored_predictions for strict H5 verification.
5. Keep active_model mode only for research/backward compatibility.
6. Do not promote stored H5_full or K_no_normal to Primary until enough daily stored predictions accumulate.
""")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_stored_prediction_baseline")
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default="latest")
    parser.add_argument("--score-source", default="stored_predictions")
    parser.add_argument("--allow-score-fallback", action="store_true")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="latest")
    parser.add_argument("--predictions-csv", default="outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv")
    parser.add_argument("--active-baseline", default="outputs/h5_market_random_baseline/01_strategy_performance_matrix.csv")
    parser.add_argument("--walk-forward-baseline", default="outputs/h5_walk_forward_baseline/01_strategy_performance_matrix.csv")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    pred_path = ROOT / args.predictions_csv if args.predictions_csv else None
    if pred_path and pred_path.exists():
        rows = wf.read_csv(pred_path)
        prediction_source = str(pred_path.relative_to(ROOT))
    else:
        csv_path = prediction_csv_from_db(output_dir, args)
        rows = wf.read_csv(csv_path)
        prediction_source = "model_predictions"
    result = run_analysis(rows, output_dir, ROOT / args.active_baseline, ROOT / args.walk_forward_baseline)
    write_infrastructure_reports(
        ROOT / "outputs/h5_stored_prediction_infrastructure",
        output_dir,
        analysis_result=result,
        prediction_source=prediction_source,
        allow_score_fallback=args.allow_score_fallback,
    )


if __name__ == "__main__":
    main()
