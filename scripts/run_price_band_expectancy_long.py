#!/usr/bin/env python3
"""Run resumable long price-band expectancy analysis in symbol chunks.

Research only. This runner only launches read-only analysis chunks and merges
CSV outputs under outputs/price_band_expectancy_long. It never writes to DB or
changes production H5/Primary/LINE/actual_trade_logs/auto-trading logic.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "outputs" / "price_band_expectancy_long"
ANALYZER = ROOT / "scripts" / "analyze_price_band_expectancy.py"

MERGE_MAP = {
    "data_availability_report.csv": "data_availability_report.csv",
    "price_band_expectancy_summary.csv": "price_band_all_case_summary.csv",
    "price_band_robust_best_cases.csv": "price_band_robust_best_cases.csv",
    "price_band_overfit_warning_cases.csv": "price_band_overfit_warning_cases.csv",
    "monthly_summary.csv": "price_band_monthly_summary.csv",
    "yearly_summary.csv": "price_band_yearly_summary.csv",
    "symbol_expectancy_ranking.csv": "symbol_expectancy_ranking.csv",
    "mean_reversion_symbol_types.csv": "symbol_type_classification.csv",
    "buy_zone_sell_zone_matrix.csv": "buy_zone_sell_zone_matrix.csv",
    "h5_vs_normal_reversion.csv": "h5_vs_price_band_comparison.csv",
    "environment_reversion_summary.csv": "environment_reversion_summary.csv",
    "train_test_stability.csv": "train_test_stability.csv",
    "outlier_sensitivity.csv": "outlier_sensitivity.csv",
    "top_reversion_cases.csv": "top_reversion_cases.csv",
    "worst_breakdown_cases.csv": "worst_breakdown_cases.csv",
    "current_price_expectancy.csv": "current_price_expectancy.csv",
    "join_diagnostics.csv": "join_diagnostics.csv",
    "proxy_usage.csv": "proxy_usage.csv",
}


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "nan", "NaN"):
            return default
        return float(value)
    except Exception:
        return default


def sort_rows(file_name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    if file_name in {"price_band_robust_best_cases.csv"}:
        return sorted(rows, key=lambda r: fnum(r.get("robust_score")), reverse=True)
    if file_name in {"price_band_overfit_warning_cases.csv"}:
        return sorted(rows, key=lambda r: (fnum(r.get("PF")), fnum(r.get("events"))), reverse=True)
    if file_name in {"buy_zone_sell_zone_matrix.csv", "price_band_all_case_summary.csv"}:
        return sorted(rows, key=lambda r: (fnum(r.get("PF")), fnum(r.get("hit_rate")), fnum(r.get("events"))), reverse=True)
    if file_name in {"symbol_expectancy_ranking.csv", "current_price_expectancy.csv"}:
        return sorted(rows, key=lambda r: (fnum(r.get("PF") or r.get("historical_PF")), fnum(r.get("hit_rate") or r.get("historical_hit_rate"))), reverse=True)
    return rows


def merge_chunk_outputs(output_dir: Path, chunk_dirs: list[Path]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for src_name, dst_name in MERGE_MAP.items():
        rows: list[dict[str, Any]] = []
        for chunk_dir in chunk_dirs:
            rows.extend(read_csv(chunk_dir / src_name))
        rows = sort_rows(dst_name, rows)
        if dst_name in {"price_band_robust_best_cases.csv", "price_band_overfit_warning_cases.csv", "top_reversion_cases.csv", "worst_breakdown_cases.csv"}:
            rows = rows[:2000]
        write_csv(output_dir / dst_name, rows)
        counts[dst_name] = len(rows)

    # Required placeholder comparison file. Real complement analysis can be
    # deepened later; this keeps the long run self-contained and explicit.
    h5_rows = read_csv(output_dir / "h5_vs_price_band_comparison.csv")
    complement = []
    for row in h5_rows:
        complement.append({
            "period": row.get("period"),
            "bucket": row.get("bucket"),
            "events": row.get("events"),
            "PF": row.get("PF"),
            "avg_return_pct": row.get("avg_return_pct"),
            "note": "Chunk-level H5-like vs normal price-band comparison. Month-level H5 complement requires aligned H5 monthly rows.",
        })
    write_csv(output_dir / "h5_bad_month_price_band_complement.csv", complement)
    counts["h5_bad_month_price_band_complement.csv"] = len(complement)
    return counts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--universe", default="topix500", choices=["nikkei225", "topix500", "prime", "all"])
    p.add_argument("--period", default="all", choices=["1y", "2y", "3y", "5y", "all"])
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    p.add_argument("--max-symbols", type=int, default=500)
    p.add_argument("--chunk-symbols", type=int, default=50)
    p.add_argument("--max-rows", type=int, default=80_000)
    p.add_argument("--min-events", type=int, default=30)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--light", action="store_true")
    p.add_argument("--full", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    output_dir = Path(args.output_dir)
    chunks_dir = output_dir / "chunks"
    logs_dir = output_dir / "logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = output_dir / "run_manifest.json"
    manifest = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "status": "running",
        "args": vars(args),
        "chunks": [],
    }
    if args.resume and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["resumed_at"] = datetime.now().isoformat(timespec="seconds")
            manifest["status"] = "running"
        except Exception:
            pass
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    completed = {
        int(chunk.get("offset"))
        for chunk in manifest.get("chunks", [])
        if chunk.get("status") == "ok" and str(chunk.get("offset", "")).isdigit()
    }
    chunk_dirs: list[Path] = []
    errors: list[dict[str, Any]] = []
    for offset in range(0, args.max_symbols, args.chunk_symbols):
        chunk_name = f"chunk_{offset:05d}_{offset + args.chunk_symbols - 1:05d}"
        chunk_dir = chunks_dir / chunk_name
        chunk_dirs.append(chunk_dir)
        done_marker = chunk_dir / "_SUCCESS"
        if (args.resume or args.skip_existing) and offset in completed and done_marker.exists():
            print(f"[skip] {chunk_name}", flush=True)
            continue
        if args.skip_existing and done_marker.exists():
            print(f"[skip-existing] {chunk_name}", flush=True)
            continue
        chunk_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            str(ANALYZER),
            "--output-dir",
            str(chunk_dir),
            "--universe",
            args.universe,
            "--period",
            args.period,
            "--max-symbols",
            str(args.chunk_symbols),
            "--symbol-offset",
            str(offset),
            "--max-rows",
            str(args.max_rows),
            "--min-events",
            str(args.min_events),
        ]
        if args.full:
            cmd.append("--full")
        if args.light:
            cmd.append("--light")
        log_path = logs_dir / f"{chunk_name}.log"
        print(f"[run] {chunk_name} offset={offset} symbols={args.chunk_symbols}", flush=True)
        t0 = time.time()
        with log_path.open("w", encoding="utf-8") as log:
            proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=str(ROOT))
        elapsed = time.time() - t0
        entry = {
            "chunk": chunk_name,
            "offset": offset,
            "status": "ok" if proc.returncode == 0 else "failed",
            "returncode": proc.returncode,
            "elapsed_sec": round(elapsed, 1),
            "output_dir": str(chunk_dir),
            "log": str(log_path),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        }
        manifest.setdefault("chunks", []).append(entry)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        if proc.returncode == 0:
            done_marker.write_text(datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
        else:
            errors.append(entry)
            print(f"[error] {chunk_name} returncode={proc.returncode}", flush=True)

    print("[merge] aggregating chunk CSVs", flush=True)
    counts = merge_chunk_outputs(output_dir, [d for d in chunk_dirs if (d / "_SUCCESS").exists()])
    elapsed_total = time.time() - started
    robust = read_csv(output_dir / "price_band_robust_best_cases.csv")
    best = robust[0] if robust else {}
    manifest["status"] = "complete_with_errors" if errors else "complete"
    manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
    manifest["elapsed_sec"] = round(elapsed_total, 1)
    manifest["merged_counts"] = counts
    manifest["errors"] = errors
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report = [
        "# Price Band Expectancy Long Run",
        "",
        "Research-only long run. No production H5/Primary/LINE/actual_trade_logs/auto-trading changes.",
        "",
        f"- status: {manifest['status']}",
        f"- elapsed_hours: {elapsed_total / 3600:.2f}",
        f"- output_dir: {output_dir}",
        f"- universe: {args.universe}",
        f"- period: {args.period}",
        f"- requested_symbols: {args.max_symbols}",
        f"- chunk_symbols: {args.chunk_symbols}",
        f"- successful_chunks: {sum(1 for d in chunk_dirs if (d / '_SUCCESS').exists())}",
        f"- failed_chunks: {len(errors)}",
        "",
        "## Best Robust Case",
        json.dumps(best, ensure_ascii=False, indent=2, default=str) if best else "No robust case found.",
        "",
        "## Output Counts",
        *(f"- {k}: {v}" for k, v in sorted(counts.items())),
        "",
        "## Notes",
        "- Resume with the same command plus --resume --skip-existing.",
        "- `all_available` excludes the latest forward horizon because future outcomes are required.",
        "- H5 comparison is H5-like price-zone comparison unless full aligned monthly H5 files are present.",
    ]
    (output_dir / "report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    print(f"[done] {manifest['status']} elapsed={elapsed_total / 3600:.2f}h output={output_dir}", flush=True)


if __name__ == "__main__":
    main()
