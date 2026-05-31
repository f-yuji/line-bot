"""Create daily H5 forward-test logs using stored model_predictions only."""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.model_predictions import (
    get_latest_model_version_from_predictions,
    join_predictions_to_candidates,
    load_model_predictions,
)
from services.trade_case_tester import (
    _attach_market_regime,
    _attach_weekly_margin,
    _build_supabase,
    _load_market_regime_rows,
    _load_weekly_margin_rows,
)

from scripts.smoke_test_h5_stored_predictions import (
    fetch_snapshot_universe,
    flags_for,
    h5_overheat_score,
    passes_ai,
    passes_drop,
    passes_h5_full,
    passes_k_no_normal,
    passes_stage,
    to_float,
)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def append_csv_unique(path: Path, rows: list[dict], key_fn: Callable[[dict], tuple[Any, ...]]) -> tuple[int, int]:
    existing = read_csv(path)
    seen = {key_fn(row) for row in existing}
    to_add = [row for row in rows if key_fn(row) not in seen]
    all_rows = existing + to_add
    write_csv(path, all_rows)
    return len(to_add), len(rows) - len(to_add)


def next_weekday(start: date, n: int) -> date:
    cur = start
    remaining = n
    while remaining > 0:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            remaining -= 1
    return cur


def passes_k_no_normal_plus_no_overheat(row: dict) -> bool | None:
    if not passes_k_no_normal(row):
        return False
    index_overheat = to_float(row.get("index_overheat_score"))
    if index_overheat is None:
        return None
    return index_overheat <= 1


def candidate_base(row: dict) -> dict:
    overheat = h5_overheat_score(row)
    k_no_normal_plus = passes_k_no_normal_plus_no_overheat(row)
    return {
        "trade_date": row.get("trade_date"),
        "code": row.get("code"),
        "name": row.get("name"),
        "signal_probability": row.get("signal_probability"),
        "signal_stage": row.get("signal_stage"),
        "drop_from_20d_high_pct": row.get("drop_from_20d_high_pct"),
        "market_regime": row.get("market_regime"),
        "overheat_score": overheat,
        "margin_ratio": row.get("margin_ratio"),
        "volume_ratio": row.get("volume_ratio_20d") or row.get("volume_ratio"),
        "sector": row.get("sector") or row.get("sector_name") or row.get("industry"),
        "score_source": row.get("score_source"),
        "model_key": row.get("model_key"),
        "model_version": row.get("model_version"),
        "prediction_date": row.get("prediction_date"),
        "prediction_created_at": row.get("prediction_created_at"),
        "source": row.get("prediction_source"),
        "score_missing": row.get("score_missing"),
        "score_fallback_used": row.get("score_fallback_used"),
        "AI_only": passes_ai(row),
        "drop_only": passes_drop(row),
        "AI_plus_drop": passes_ai(row) and passes_drop(row),
        "AI_plus_drop_stage": passes_ai(row) and passes_drop(row) and passes_stage(row),
        "H5_full": passes_h5_full(row),
        "K_no_normal": passes_k_no_normal(row),
        "K_no_normal_plus_no_overheat": bool(k_no_normal_plus),
        "notes": "index_overheat_score missing" if k_no_normal_plus is None else "",
    }


def enrich_exit_columns(row: dict) -> dict:
    out = dict(row)
    out.update({
        "suggested_exit_model": "HD3_EST12",
        "peak_pullback_enabled": False,
        "emergency_stop_pct": -12,
        "planned_holding_days": 3,
        "manual_review_required": True,
    })
    return out


def sorted_candidates(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda r: (
            -(to_float(r.get("signal_probability"), -1) or -1),
            to_float(r.get("drop_from_20d_high_pct"), 999) or 999,
            to_float(r.get("overheat_score"), 999) or 999,
        ),
    )


def seed_row(row: dict, strategy_group: str, trade_date: date) -> dict:
    return {
        "trade_date": row.get("trade_date"),
        "code": row.get("code"),
        "name": row.get("name"),
        "strategy_group": strategy_group,
        "signal_probability": row.get("signal_probability"),
        "signal_stage": row.get("signal_stage"),
        "entry_price": row.get("close"),
        "entry_price_source": "signal_date_close",
        "score_source": row.get("score_source"),
        "model_key": row.get("model_key"),
        "model_version": row.get("model_version"),
        "prediction_date": row.get("prediction_date"),
        "planned_exit_model": "HD3_EST12",
        "planned_exit_date_hd1": next_weekday(trade_date, 1).isoformat(),
        "planned_exit_date_hd2": next_weekday(trade_date, 2).isoformat(),
        "planned_exit_date_hd3": next_weekday(trade_date, 3).isoformat(),
        "emergency_stop_pct": -12,
        "peak_pullback_enabled": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "result_status": "pending",
    }


def build_seed_rows(candidates: list[dict], trade_date: date) -> list[dict]:
    rows: list[dict] = []
    for row in candidates:
        if row.get("AI_plus_drop"):
            rows.append(seed_row(row, "AI_plus_drop", trade_date))
        if row.get("H5_full"):
            rows.append(seed_row(row, "H5_full", trade_date))
        if row.get("K_no_normal"):
            rows.append(seed_row(row, "K_no_normal", trade_date))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default="latest")
    parser.add_argument("--source", default="daily_prediction")
    parser.add_argument("--score-source", default="stored_predictions")
    parser.add_argument("--allow-score-fallback", default="false")
    parser.add_argument("--output-dir", default="outputs/h5_stored_forward_test")
    parser.add_argument("--write-latest", default="true")
    args = parser.parse_args()

    if args.score_source != "stored_predictions":
        raise SystemExit("run_h5_stored_forward_test.py only supports score_source=stored_predictions")
    allow_fallback = str(args.allow_score_fallback).lower() in {"1", "true", "yes", "y"}
    if allow_fallback:
        raise SystemExit("allow-score-fallback=true is not allowed for stored forward-test")

    sb = _build_supabase()
    trade_date = date.fromisoformat(args.trade_date)
    model_version = args.model_version
    if model_version in {"", "latest", None}:
        model_version = get_latest_model_version_from_predictions(sb, args.model_key) or ""
    if not model_version:
        raise SystemExit("No model_version found in model_predictions")

    predictions = load_model_predictions(
        sb,
        model_key=args.model_key,
        model_version=model_version,
        trade_date_from=trade_date,
        trade_date_to=trade_date,
        source=args.source,
        active_only=True,
    )
    universe = fetch_snapshot_universe(sb, trade_date.isoformat())
    _attach_weekly_margin(universe, _load_weekly_margin_rows(sb, trade_date, trade_date))
    _attach_market_regime(universe, _load_market_regime_rows(sb, trade_date, trade_date))
    join_result = join_predictions_to_candidates(universe, predictions)
    stored_rows = [row for row in universe if not row.get("score_missing")]
    fallback_used = sum(1 for row in stored_rows if row.get("score_fallback_used"))
    active_model_called = False

    candidate_rows = [candidate_base(row) for row in stored_rows]
    candidate_rows = sorted_candidates(candidate_rows)
    h5_full = [enrich_exit_columns(row) for row in candidate_rows if row.get("H5_full")]
    k_no_normal = [enrich_exit_columns(row) for row in candidate_rows if row.get("K_no_normal")]
    k_plus_note = "index_overheat_score missing"
    k_plus_count = 0 if all(row.get("notes") == k_plus_note or not row.get("K_no_normal") for row in candidate_rows) else sum(
        1 for row in candidate_rows if row.get("K_no_normal_plus_no_overheat")
    )

    groups = {
        "saved_predictions_all": len(predictions),
        "loaded_candidates_total": len(universe),
        "AI_only": sum(1 for row in candidate_rows if row.get("AI_only")),
        "drop_only": sum(1 for row in candidate_rows if row.get("drop_only")),
        "AI_plus_drop": sum(1 for row in candidate_rows if row.get("AI_plus_drop")),
        "AI_plus_drop_stage": sum(1 for row in candidate_rows if row.get("AI_plus_drop_stage")),
        "H5_full": len(h5_full),
        "K_no_normal": len(k_no_normal),
        "K_no_normal_plus_no_overheat": k_plus_count,
        "missing_prediction": int(join_result.get("missing", 0) or 0),
        "fallback_used": fallback_used,
    }
    strategy_counts = [
        {
            "trade_date": trade_date.isoformat(),
            "strategy": name,
            "count": count,
            "notes": "index_overheat_score missing" if name == "K_no_normal_plus_no_overheat" else "",
        }
        for name, count in groups.items()
    ]

    missing_summary = [
        {"reason": "missing_model_prediction", "count": join_result.get("missing", 0), "notes": ""},
        {"reason": "missing_feature_snapshot", "count": max(0, len(predictions) - int(join_result.get("matched", 0) or 0)), "notes": ""},
        {"reason": "missing_signal_stage", "count": sum(1 for row in stored_rows if not row.get("signal_stage")), "notes": ""},
        {"reason": "missing_margin_ratio", "count": sum(1 for row in stored_rows if row.get("margin_ratio") is None), "notes": "allowed by require_margin_data=false"},
        {"reason": "missing_market_regime", "count": sum(1 for row in stored_rows if not row.get("market_regime")), "notes": ""},
        {"reason": "fallback_used", "count": fallback_used, "notes": ""},
        {"reason": "active_model_called", "count": 1 if active_model_called else 0, "notes": ""},
    ]

    pass_test = (
        len(predictions) > 0
        and len(universe) > 0
        and fallback_used == 0
        and not active_model_called
        and all(row.get("score_source") == "stored_predictions" for row in candidate_rows)
    )

    out_root = ROOT / args.output_dir
    day_dir = out_root / trade_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    seed_rows = build_seed_rows(candidate_rows, trade_date)

    write_text(day_dir / "01_saved_prediction_summary.txt", f"""
trade_date: {trade_date}
model_key: {args.model_key}
model_version: {model_version}
source: {args.source}
score_source: stored_predictions
saved_predictions_count: {len(predictions)}
loaded_candidates_total: {len(universe)}
candidates_with_stored_score: {len(stored_rows)}
missing_prediction_count: {join_result.get('missing', 0)}
fallback_used_count: {fallback_used}
active_model_called: false
result: {'PASS' if pass_test else 'FAIL'}
""")
    write_csv(day_dir / "02_strategy_counts.csv", strategy_counts)
    write_csv(day_dir / "03_h5_candidates.csv", candidate_rows)
    write_csv(day_dir / "04_h5_full_candidates.csv", h5_full)
    write_csv(day_dir / "05_k_no_normal_candidates.csv", k_no_normal)
    write_csv(day_dir / "06_forward_test_seed_rows.csv", seed_rows)
    write_csv(day_dir / "07_missing_prediction_summary.csv", missing_summary)
    write_text(day_dir / "08_stored_forward_test_report.txt", f"""
# H5 Stored Forward Test

trade_date: {trade_date}
model_key: {args.model_key}
model_version: {model_version}
score_source: stored_predictions

saved_predictions_count: {len(predictions)}
stored_predictions_candidate_generation: {'yes' if stored_rows else 'no'}
active_model_predict_proba_called: false
fallback_used_count: {fallback_used}
missing_prediction_count: {join_result.get('missing', 0)}
AI_only_count: {groups['AI_only']}
AI_plus_drop_count: {groups['AI_plus_drop']}
H5_full_count: {groups['H5_full']}
K_no_normal_count: {groups['K_no_normal']}

H5_full_candidates:
{chr(10).join([f"- {r.get('code')} {r.get('name')} prob={r.get('signal_probability')} drop20={r.get('drop_from_20d_high_pct')} regime={r.get('market_regime')} margin={r.get('margin_ratio')}" for r in h5_full]) or "- none"}

K_no_normal_candidates:
{chr(10).join([f"- {r.get('code')} {r.get('name')} prob={r.get('signal_probability')} drop20={r.get('drop_from_20d_high_pct')} regime={r.get('market_regime')} margin={r.get('margin_ratio')}" for r in k_no_normal]) or "- none"}

planned_exit_model: HD3_EST12
manual_review_required: true
Primary changed: no
result: {'PASS' if pass_test else 'FAIL'}
""")

    if str(args.write_latest).lower() in {"1", "true", "yes", "y"}:
        latest_map = {
            day_dir / "08_stored_forward_test_report.txt": out_root / "latest_stored_forward_test_report.txt",
            day_dir / "03_h5_candidates.csv": out_root / "latest_h5_candidates.csv",
            day_dir / "04_h5_full_candidates.csv": out_root / "latest_h5_full_candidates.csv",
            day_dir / "05_k_no_normal_candidates.csv": out_root / "latest_k_no_normal_candidates.csv",
        }
        for src, dst in latest_map.items():
            shutil.copyfile(src, dst)

    created_at = datetime.now(timezone.utc).isoformat()
    daily_row = {
        "trade_date": trade_date.isoformat(),
        "model_key": args.model_key,
        "model_version": model_version,
        "score_source": "stored_predictions",
        "saved_predictions_count": len(predictions),
        "loaded_candidates_total": len(universe),
        "AI_only_count": groups["AI_only"],
        "AI_plus_drop_count": groups["AI_plus_drop"],
        "H5_full_count": groups["H5_full"],
        "K_no_normal_count": groups["K_no_normal"],
        "fallback_used_count": fallback_used,
        "missing_prediction_count": join_result.get("missing", 0),
        "active_model_called": active_model_called,
        "result": "PASS" if pass_test else "FAIL",
        "created_at": created_at,
        "rerun_count": len([
            r for r in read_csv(out_root / "forward_test_daily_summary.csv")
            if r.get("trade_date") == trade_date.isoformat()
            and r.get("model_key") == args.model_key
            and r.get("model_version") == model_version
            and r.get("score_source") == "stored_predictions"
        ]) + 1,
    }
    existing_daily = read_csv(out_root / "forward_test_daily_summary.csv")
    write_csv(out_root / "forward_test_daily_summary.csv", existing_daily + [daily_row])

    cumulative_candidates = []
    for row in seed_rows:
        cumulative_candidates.append({
            "trade_date": row.get("trade_date"),
            "code": row.get("code"),
            "name": row.get("name"),
            "strategy_group": row.get("strategy_group"),
            "signal_probability": row.get("signal_probability"),
            "signal_stage": row.get("signal_stage"),
            "drop_from_20d_high_pct": next((r.get("drop_from_20d_high_pct") for r in candidate_rows if r.get("code") == row.get("code")), None),
            "market_regime": next((r.get("market_regime") for r in candidate_rows if r.get("code") == row.get("code")), None),
            "overheat_score": next((r.get("overheat_score") for r in candidate_rows if r.get("code") == row.get("code")), None),
            "margin_ratio": next((r.get("margin_ratio") for r in candidate_rows if r.get("code") == row.get("code")), None),
            "score_source": row.get("score_source"),
            "model_key": row.get("model_key"),
            "model_version": row.get("model_version"),
            "prediction_date": row.get("prediction_date"),
            "planned_exit_model": row.get("planned_exit_model"),
            "emergency_stop_pct": row.get("emergency_stop_pct"),
            "manual_review_required": True,
            "auto_buy_enabled": False,
            "result_status": row.get("result_status"),
            "created_at": created_at,
        })
    added, skipped = append_csv_unique(
        out_root / "forward_test_candidate_log.csv",
        cumulative_candidates,
        lambda r: (r.get("trade_date"), r.get("code"), r.get("strategy_group"), r.get("model_version")),
    )

    print(f"saved_predictions_count={len(predictions)}")
    print(f"loaded_candidates_total={len(universe)}")
    print(f"fallback_used_count={fallback_used}")
    print("active_model_called=false")
    print(f"AI_only={groups['AI_only']}")
    print(f"AI_plus_drop={groups['AI_plus_drop']}")
    print(f"H5_full={groups['H5_full']}")
    print(f"K_no_normal={groups['K_no_normal']}")
    print(f"candidate_log_added={added}")
    print(f"candidate_log_skipped={skipped}")
    print(f"result={'PASS' if pass_test else 'FAIL'}")
    if not pass_test:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
