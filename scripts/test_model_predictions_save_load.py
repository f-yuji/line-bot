"""Smoke test immutable save/load behavior for model_predictions."""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.model_predictions import load_model_predictions, save_model_predictions
from services.trade_case_tester import _build_supabase


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_stored_prediction_infrastructure")
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", default="test_model_predictions_v1")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    today = date.today().isoformat()
    test_run_id = f"test_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    rows = [
        {
            "code": "TEST001",
            "trade_date": today,
            "signal_probability": 0.651,
            "signal_stage": "confirmed",
            "metadata": {"test_insert": True, "test_run_id": test_run_id},
        },
        {
            "code": "TEST002",
            "trade_date": today,
            "signal_probability": 0.712,
            "signal_stage": "strong_confirmed",
            "metadata": {"test_insert": True, "test_run_id": test_run_id},
        },
    ]

    try:
        sb = _build_supabase()
        first = save_model_predictions(
            sb,
            rows,
            model_key=args.model_key,
            model_version=args.model_version,
            source="test_prediction",
            metadata={"test_insert": True, "test_run_id": test_run_id},
        )
        second = save_model_predictions(
            sb,
            [{**r, "signal_probability": 0.111} for r in rows],
            model_key=args.model_key,
            model_version=args.model_version,
            source="test_prediction",
            metadata={"test_insert": True, "test_run_id": test_run_id, "second_attempt": True},
        )
        loaded = load_model_predictions(
            sb,
            model_key=args.model_key,
            model_version=args.model_version,
            trade_date_from=today,
            trade_date_to=today,
            source="test_prediction",
        )
    except Exception as exc:
        write_text(output_dir / "03_prediction_save_test_result.txt", f"""
# model_predictions save/load test

test_run_id: {test_run_id}
result: FAIL
error: {exc}
next: Run outputs/h5_stored_prediction_infrastructure/01_model_predictions_schema.sql in Supabase SQL Editor, then rerun this script.
""")
        write_csv(output_dir / "04_stored_prediction_load_test.csv", [])
        raise
    test_loaded = [r for r in loaded if (r.get("metadata") or {}).get("test_insert")]
    overwrite_detected = any(abs(float(r.get("signal_probability") or 0) - 0.111) < 1e-9 for r in test_loaded)
    passed = first.get("inserted", 0) >= 0 and second.get("inserted", 0) == 0 and not overwrite_detected
    write_csv(output_dir / "04_stored_prediction_load_test.csv", test_loaded)
    write_text(output_dir / "03_prediction_save_test_result.txt", f"""
# model_predictions save/load test

test_run_id: {test_run_id}
first_insert_count: {first.get('inserted')}
first_skipped_count: {first.get('skipped')}
first_errors: {first.get('errors')}
second_insert_count: {second.get('inserted')}
second_insert_skipped_count: {second.get('skipped')}
second_errors: {second.get('errors')}
loaded_rows_count: {len(test_loaded)}
overwrite_detected: {overwrite_detected}
result: {'PASS' if passed else 'FAIL'}
""")


if __name__ == "__main__":
    main()
