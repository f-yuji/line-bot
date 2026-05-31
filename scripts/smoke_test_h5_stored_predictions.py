"""Smoke test H5 candidate generation using stored model_predictions only."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_primary import h5_overheat_score
from services.model_predictions import join_predictions_to_candidates, load_model_predictions
from services.trade_case_tester import (
    _attach_market_regime,
    _attach_weekly_margin,
    _build_supabase,
    _load_market_regime_rows,
    _load_weekly_margin_rows,
)


def to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


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


def fetch_snapshot_universe(sb, trade_date: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        data = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .eq("trade_date", trade_date)
            .eq("is_drop_candidate", True)
            .eq("is_tradeable", True)
            .order("day_change_pct")
            .range(offset, offset + 999)
            .execute()
            .data
            or []
        )
        rows.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return rows


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


def passes_overheat(row: dict) -> bool:
    return h5_overheat_score(row) <= 1


def passes_margin(row: dict) -> bool:
    margin = to_float(row.get("margin_ratio"))
    if margin is None:
        return True
    return 3.0 <= margin <= 30.0


def passes_h5_full(row: dict) -> bool:
    return (
        passes_ai(row)
        and passes_drop(row)
        and passes_stage(row)
        and passes_no_panic(row)
        and passes_overheat(row)
        and passes_margin(row)
    )


def passes_k_no_normal(row: dict) -> bool:
    return passes_h5_full(row) and str(row.get("market_regime") or "") not in {"normal", "euphoria"}


def flags_for(row: dict) -> list[str]:
    flags: list[str] = []
    if passes_ai(row):
        flags.append("AI_only")
    if passes_drop(row):
        flags.append("drop_only")
    if passes_ai(row) and passes_drop(row):
        flags.append("AI_plus_drop")
    if passes_ai(row) and passes_drop(row) and passes_stage(row):
        flags.append("AI_plus_drop_stage")
    if passes_h5_full(row):
        flags.append("H5_full")
    if passes_k_no_normal(row):
        flags.append("K_no_normal")
    return flags


def slim_candidate(row: dict) -> dict:
    overheat = h5_overheat_score(row)
    flags = flags_for(row)
    return {
        "code": row.get("code"),
        "name": row.get("name"),
        "trade_date": row.get("trade_date"),
        "signal_probability": row.get("signal_probability"),
        "signal_stage": row.get("signal_stage"),
        "drop_from_20d_high_pct": row.get("drop_from_20d_high_pct"),
        "market_regime": row.get("market_regime"),
        "overheat_score": overheat,
        "margin_ratio": row.get("margin_ratio"),
        "volume_ratio": row.get("volume_ratio_20d") or row.get("volume_ratio"),
        "score_source": row.get("score_source"),
        "model_key": row.get("model_key"),
        "model_version": row.get("model_version"),
        "prediction_date": row.get("prediction_date"),
        "prediction_created_at": row.get("prediction_created_at"),
        "score_missing": row.get("score_missing"),
        "score_fallback_used": row.get("score_fallback_used"),
        "group_flags": ",".join(flags),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="2026-05-29")
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default="20260507_061730")
    parser.add_argument("--source", default="daily_prediction")
    parser.add_argument("--allow-score-fallback", default="false")
    parser.add_argument("--output-dir", default="outputs/h5_stored_prediction_smoke_test")
    parser.add_argument("--expected-count", type=int, default=79)
    args = parser.parse_args()

    allow_fallback = str(args.allow_score_fallback).lower() in {"1", "true", "yes", "y"}
    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    sb = _build_supabase()
    predictions = load_model_predictions(
        sb,
        model_key=args.model_key,
        model_version=args.model_version,
        trade_date_from=args.trade_date,
        trade_date_to=args.trade_date,
        source=args.source,
        active_only=True,
    )
    saved_count = len(predictions)
    count_match = saved_count == args.expected_count

    universe = fetch_snapshot_universe(sb, args.trade_date)
    _attach_weekly_margin(universe, _load_weekly_margin_rows(sb, __import__("datetime").date.fromisoformat(args.trade_date), __import__("datetime").date.fromisoformat(args.trade_date)))
    _attach_market_regime(universe, _load_market_regime_rows(sb, __import__("datetime").date.fromisoformat(args.trade_date), __import__("datetime").date.fromisoformat(args.trade_date)))
    join_result = join_predictions_to_candidates(universe, predictions)
    candidates = [row for row in universe if not row.get("score_missing")]

    if allow_fallback:
        raise SystemExit("allow-score-fallback=true is not allowed in this smoke test")

    fallback_used = sum(1 for row in candidates if row.get("score_fallback_used"))
    wrong_source = sum(1 for row in candidates if row.get("score_source") != "stored_predictions")
    wrong_version = sum(1 for row in candidates if str(row.get("model_version")) != args.model_version)

    groups = {
        "saved_predictions_all": predictions,
        "loaded_candidates_total": universe,
        "candidates_with_stored_score": candidates,
        "candidates_missing_score": [row for row in universe if row.get("score_missing")],
        "fallback_used": [row for row in candidates if row.get("score_fallback_used")],
        "AI_only": [row for row in candidates if passes_ai(row)],
        "drop_only": [row for row in candidates if passes_drop(row)],
        "AI_plus_drop": [row for row in candidates if passes_ai(row) and passes_drop(row)],
        "AI_plus_drop_stage": [
            row for row in candidates if passes_ai(row) and passes_drop(row) and passes_stage(row)
        ],
        "H5_full": [row for row in candidates if passes_h5_full(row)],
        "K_no_normal": [row for row in candidates if passes_k_no_normal(row)],
    }
    summary_rows = [
        {"group": name, "count": len(rows), "notes": ""}
        for name, rows in groups.items()
    ]

    top_saved = sorted(candidates, key=lambda r: to_float(r.get("signal_probability"), -1) or -1, reverse=True)[:20]
    top_rows = [slim_candidate(row) for row in top_saved]
    candidate_rows = [slim_candidate(row) for row in sorted(candidates, key=lambda r: to_float(r.get("signal_probability"), -1) or -1, reverse=True)]

    missing_summary = [
        {
            "reason": "missing_model_prediction",
            "count": join_result.get("missing", 0),
            "notes": "snapshot universe rows without matching stored prediction",
        },
        {
            "reason": "missing_feature_snapshot",
            "count": max(0, saved_count - join_result.get("matched", 0)),
            "notes": "stored predictions not represented in loaded snapshot universe",
        },
        {
            "reason": "missing_margin_ratio",
            "count": sum(1 for row in candidates if row.get("margin_ratio") is None),
            "notes": "allowed by require_margin_data=false",
        },
        {
            "reason": "missing_signal_stage",
            "count": sum(1 for row in candidates if not row.get("signal_stage")),
            "notes": "",
        },
        {
            "reason": "missing_market_regime",
            "count": sum(1 for row in candidates if not row.get("market_regime")),
            "notes": "",
        },
    ]

    top_expected = {
        "6376": 0.680346,
        "6508": 0.655515,
        "6507": 0.654328,
        "6235": 0.642882,
        "6616": 0.642170,
    }
    sample_matches = []
    for code, expected in top_expected.items():
        row = next((r for r in candidates if str(r.get("code")) == code), None)
        actual = to_float(row.get("signal_probability") if row else None)
        sample_matches.append(actual is not None and abs(actual - expected) < 0.000001)

    pass_test = (
        count_match
        and saved_count == args.expected_count
        and join_result.get("matched", 0) == args.expected_count
        and fallback_used == 0
        and wrong_source == 0
        and wrong_version == 0
        and all(sample_matches)
    )

    write_text(output_dir / "01_saved_prediction_count.txt", f"""
trade_date: {args.trade_date}
model_key: {args.model_key}
model_version: {args.model_version}
source: {args.source}
saved_count: {saved_count}
expected_count: {args.expected_count}
count_match: {count_match}
""")
    write_csv(output_dir / "02_loaded_candidates_summary.csv", summary_rows)
    write_csv(output_dir / "03_top_saved_predictions.csv", top_rows)
    write_csv(output_dir / "04_h5_candidate_rows.csv", candidate_rows)
    write_csv(output_dir / "05_missing_prediction_summary.csv", missing_summary)
    write_text(output_dir / "06_smoke_test_report.txt", f"""
# H5 Stored Predictions Smoke Test

trade_date: {args.trade_date}
model_key: {args.model_key}
model_version: {args.model_version}
source: {args.source}

1. model_predictions read: {'yes' if saved_count else 'no'}
2. saved_count: {saved_count}
3. stored_predictions candidate generation: {'yes' if candidates else 'no'}
4. active_model predict_proba called: no
5. fallback_used: {fallback_used}
6. missing_prediction_count: {join_result.get('missing', 0)}
7. AI_only_count: {len(groups['AI_only'])}
8. AI_plus_drop_count: {len(groups['AI_plus_drop'])}
9. H5_full_count: {len(groups['H5_full'])}
10. K_no_normal_count: {len(groups['K_no_normal'])}
11. top_sample_probability_match: {all(sample_matches)}
12. score_source_all_stored_predictions: {wrong_source == 0}
13. model_version_all_{args.model_version}: {wrong_version == 0}
14. Primary / DB case / UI impact: none
15. smoke_test_result: {'PASS' if pass_test else 'FAIL'}

loaded_candidates_total: {len(universe)}
candidates_with_stored_score: {len(candidates)}
join_matched: {join_result.get('matched', 0)}
join_missing: {join_result.get('missing', 0)}
""")
    print(f"saved_count={saved_count}")
    print(f"loaded_candidates_total={len(universe)}")
    print(f"candidates_with_stored_score={len(candidates)}")
    print(f"fallback_used={fallback_used}")
    print(f"AI_only={len(groups['AI_only'])}")
    print(f"AI_plus_drop={len(groups['AI_plus_drop'])}")
    print(f"H5_full={len(groups['H5_full'])}")
    print(f"K_no_normal={len(groups['K_no_normal'])}")
    print(f"result={'PASS' if pass_test else 'FAIL'}")
    if not pass_test:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
