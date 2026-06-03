#!/usr/bin/env python3
"""Smoke-test one SHAP explanation for an H5 candidate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_shap_explainer import compute_shap_for_candidate, is_shap_available, save_shap_cache
from services.h5_shap_reason_builder import merge_shap_reason


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-test H5 SHAP explanation")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--name", default="")
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--signal-probability", default="")
    parser.add_argument("--output-dir", default="outputs/h5_shap_explanations")
    parser.add_argument("--force", default="true")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    cache_root = output_dir
    row = {
        "code": args.code,
        "name": args.name or args.code,
        "trade_date": args.trade_date,
        "model_key": args.model_key,
        "model_version": args.model_version,
        "signal_probability": args.signal_probability,
    }
    result = compute_shap_for_candidate(
        row,
        cache_root=cache_root,
        force=str(args.force).lower() in {"1", "true", "yes", "on"},
    )
    merged = merge_shap_reason(result)
    cache_path = save_shap_cache(merged, cache_root=cache_root)
    result_path = output_dir / "smoke_test_result.json"
    result_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    status = "PASS" if merged.get("ok") else ("SKIP" if merged.get("reason") == "shap_not_installed" else "FAIL")
    report = f"""
H5 SHAP smoke test

shap_available: {is_shap_available()}
model_loaded: {bool(merged.get('ok')) or merged.get('reason') not in {'model_not_found', 'model_file_not_found', 'model_load_error'}}
feature_columns_loaded: {bool(merged.get('ok')) or merged.get('reason') != 'feature_columns_not_found'}
feature_row_loaded: {bool(merged.get('ok')) or merged.get('reason') != 'feature_row_not_found'}
shap_computed: {bool(merged.get('ok'))}
positive_count: {len(merged.get('positive_contributions') or [])}
negative_count: {len(merged.get('negative_contributions') or [])}
cache_saved: {bool(cache_path)}
cache_path: {cache_path or ''}
reason: {merged.get('reason') or ''}
warnings: {' / '.join(str(w) for w in (merged.get('warnings') or []))}
result: {status}
"""
    write_text(output_dir / "smoke_test_report.txt", report)
    print(report.strip())


if __name__ == "__main__":
    main()
