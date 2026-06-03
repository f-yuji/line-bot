#!/usr/bin/env python3
"""Generate SHAP explanation cache for H5 candidate CSV rows."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_shap_explainer import compute_shap_for_candidate, load_shap_cache, save_shap_cache
from services.h5_shap_reason_builder import merge_shap_reason


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


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


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate H5 SHAP explanations from a candidate CSV")
    parser.add_argument("--input", default="outputs/h5_stored_forward_test/latest_h5_full_candidates.csv")
    parser.add_argument("--model-key", default="rebound_lgbm_5d")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--output-dir", default="outputs/h5_shap_explanations")
    parser.add_argument("--force", default="false")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--code", default="")
    args = parser.parse_args()

    input_path = ROOT / args.input
    output_dir = ROOT / args.output_dir
    force = str(args.force).lower() in {"1", "true", "yes", "on"}
    rows = read_csv(input_path)
    if args.code:
        rows = [r for r in rows if str(r.get("code") or "") == str(args.code)]
    if args.limit:
        rows = rows[: args.limit]

    summary: list[dict] = []
    ok_count = 0
    skip_count = 0
    fail_count = 0
    for row in rows:
        code = str(row.get("code") or "").strip()
        trade_date = str(row.get("trade_date") or "")[:10]
        model_key = str(row.get("model_key") or args.model_key)
        model_version = str(row.get("model_version") or args.model_version)
        candidate = {
            **row,
            "model_key": model_key,
            "model_version": model_version,
            "signal_probability": row.get("signal_probability"),
        }
        cached = None if force else load_shap_cache(code, trade_date, model_key, model_version, cache_root=output_dir)
        if cached:
            result = cached
            skip_count += 1
        else:
            result = merge_shap_reason(compute_shap_for_candidate(candidate, cache_root=output_dir, force=force))
            save_shap_cache(result, cache_root=output_dir)
        ok = bool(result.get("ok"))
        ok_count += int(ok)
        fail_count += int(not ok and not cached)
        positives = result.get("positive_contributions") or []
        negatives = result.get("negative_contributions") or []
        summary.append({
            "code": code,
            "name": row.get("name"),
            "trade_date": trade_date,
            "model_key": model_key,
            "model_version": model_version,
            "signal_probability": row.get("signal_probability"),
            "ok": ok,
            "reason": result.get("reason") or "",
            "positive_top_features": ",".join(str(i.get("feature")) for i in positives[:5]),
            "negative_top_features": ",".join(str(i.get("feature")) for i in negatives[:5]),
            "cache_path": result.get("cache_path") or "",
            "warnings": " / ".join(str(w) for w in (result.get("warnings") or [])),
        })

    write_csv(output_dir / "latest_summary.csv", summary)
    report = f"""
H5 SHAP batch generation

input: {input_path}
rows: {len(rows)}
ok_count: {ok_count}
cache_skip_count: {skip_count}
fail_count: {fail_count}
output_dir: {output_dir}
summary: {output_dir / 'latest_summary.csv'}
"""
    write_text(output_dir / "latest_report.txt", report)
    print(report.strip())


if __name__ == "__main__":
    main()
