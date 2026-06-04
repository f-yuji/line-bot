#!/usr/bin/env python3
"""Audit old LIVE narrowing vs balanced LIVE allocation.

Reads the latest stored forward-test CSV when available and writes comparison
artifacts under outputs/live_allocation_audit/. This script is read-only with
respect to production tables.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_live_allocator import (  # noqa: E402
    LIVE_ALLOCATION_BUCKETS,
    LIVE_MAX_DAILY_CANDIDATES,
    allocate_balanced_live_candidates,
    current_h5_core_reasons,
    short_pullback_reasons,
    trend_support_reasons,
)

OUT_DIR = ROOT / "outputs" / "live_allocation_audit"
SOURCE_CANDIDATES = [
    ROOT / "outputs" / "h5_stored_forward_test" / "latest_h5_candidates.csv",
    ROOT / "outputs" / "h5_stored_forward_cases" / "latest_candidates.csv",
]


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _score(row: dict[str, Any]) -> float:
    value = _float(row.get("signal_probability") or row.get("score") or row.get("probability"))
    return value or 0.0


def _overheat(row: dict[str, Any]) -> float:
    value = _float(row.get("entry_overheat_score") or row.get("overheat_score"))
    return value if value is not None else 99.0


def _volume(row: dict[str, Any]) -> float:
    value = _float(row.get("volume_ratio_20d") or row.get("volume_ratio"))
    return value or 0.0


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if "signal_probability" not in out or out.get("signal_probability") in {None, ""}:
        out["signal_probability"] = out.get("score") or out.get("probability")
    if "trade_date" not in out or not out.get("trade_date"):
        out["trade_date"] = out.get("signal_date") or out.get("latest_date")
    if "drop_from_20d_high_pct" not in out or not out.get("drop_from_20d_high_pct"):
        out["drop_from_20d_high_pct"] = out.get("drop20")
    if "entry_overheat_score" not in out or not out.get("entry_overheat_score"):
        out["entry_overheat_score"] = out.get("overheat_score")
    return out


def _old_live_limited(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [row for row in rows if not current_h5_core_reasons(row)]
    ranked = sorted(candidates, key=lambda r: (_score(r), -_overheat(r), _volume(r)), reverse=True)
    selected = []
    for rank, row in enumerate(ranked[:10], start=1):
        if len(selected) >= 2:
            break
        out = dict(row)
        out["old_live_selected"] = True
        out["old_selected_rank"] = rank
        selected.append(out)
    return selected


def _filter_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ai = [r for r in rows if _score(r) >= 0.65]
    confirmed = [r for r in ai if str(r.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}]
    current_drop = [r for r in confirmed if not current_h5_core_reasons(r) or "drop20_gt_m8" not in current_h5_core_reasons(r)]
    short_drop = [r for r in confirmed if "drop5_gt_m3" not in short_pullback_reasons(r)]
    overheat = [r for r in confirmed if "overheat_gt_1" not in current_h5_core_reasons(r)]
    gap = [
        r
        for r in confirmed
        if "gap_gt_3" not in current_h5_core_reasons(r)
        or "gap_gt_3" not in short_pullback_reasons(r)
    ]
    return [
        {"filter": "all_predictions", "count": len(rows)},
        {"filter": "AI>=0.65", "count": len(ai)},
        {"filter": "confirmed_stage", "count": len(confirmed)},
        {"filter": "drop20<=-8", "count": len(current_drop)},
        {"filter": "drop5<=-3", "count": len(short_drop)},
        {"filter": "overheat<=1", "count": len(overheat)},
        {"filter": "gap<=3", "count": len(gap)},
    ]


def main() -> None:
    source = next((p for p in SOURCE_CANDIDATES if p.exists()), SOURCE_CANDIDATES[0])
    rows = [_normalize(r) for r in _read_csv(source)]
    entries = [
        {"code": row.get("code"), "sector": row.get("sector"), "data": row, "meta": dict(row), "source_row": row}
        for row in rows
    ]
    allocate_balanced_live_candidates(entries, sector_counts={}, max_sector_positions=2)

    latest_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for entry in entries:
        meta = entry["meta"]
        row = dict(entry["source_row"])
        row.update(
            {
                "new_live_selected": bool(meta.get("is_live_candidate")),
                "case_key": meta.get("case_key"),
                "live_allocation_bucket": meta.get("live_allocation_bucket"),
                "allocation_rank": meta.get("allocation_rank"),
                "selected_rank": meta.get("selected_rank"),
                "live_skip_reason": meta.get("live_skip_reason"),
            }
        )
        if meta.get("is_live_candidate"):
            latest_rows.append(row)
        else:
            skipped_rows.append(row)

    old_rows = _old_live_limited(rows)
    old_keys = {(str(r.get("trade_date") or r.get("signal_date") or ""), str(r.get("code") or "")) for r in old_rows}
    new_keys = {(str(r.get("trade_date") or r.get("signal_date") or ""), str(r.get("code") or "")) for r in latest_rows}

    bucket_counts: dict[str, int] = {}
    for row in latest_rows:
        bucket = str(row.get("live_allocation_bucket") or "none")
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    summary = [
        {"metric": "source_file", "value": str(source.relative_to(ROOT)) if source.exists() else "missing"},
        {"metric": "source_rows", "value": len(rows)},
        {"metric": "old_live_limited_count", "value": len(old_rows)},
        {"metric": "new_balanced_live_count", "value": len(latest_rows)},
        {"metric": "new_only_count", "value": len(new_keys - old_keys)},
        {"metric": "old_only_count", "value": len(old_keys - new_keys)},
        {"metric": "max_daily_live_candidates", "value": LIVE_MAX_DAILY_CANDIDATES},
    ]
    summary.extend({"metric": f"bucket_{bucket}", "value": count} for bucket, count in sorted(bucket_counts.items()))

    filter_counts = _filter_counts(rows)
    filter_counts.append({"filter": "final_selected", "count": len(latest_rows)})

    focus_rows = []
    for row in rows:
        if str(row.get("code") or "") == "6507":
            trend_case, trend_reasons = trend_support_reasons(row)
            focus_rows.append(
                {
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "score": row.get("signal_probability") or row.get("score") or row.get("probability"),
                    "current_h5_reasons": ",".join(current_h5_core_reasons(row)),
                    "short_pullback_reasons": ",".join(short_pullback_reasons(row)),
                    "trend_case_key": trend_case,
                    "trend_reasons": ",".join(trend_reasons),
                    "decision": "selected" if any(str(r.get("code") or "") == "6507" for r in latest_rows) else "not_selected",
                }
            )
    if not focus_rows:
        focus_rows.append({"code": "6507", "decision": "not_in_latest_predictions"})

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_csv(OUT_DIR / "latest_live_candidates.csv", latest_rows)
    _write_csv(OUT_DIR / "latest_live_filter_counts.csv", filter_counts)
    _write_csv(OUT_DIR / "latest_live_skipped.csv", skipped_rows)
    _write_csv(OUT_DIR / "live_allocation_summary.csv", summary)
    _write_csv(OUT_DIR / "focus_6507.csv", focus_rows)

    report = [
        "LIVE allocation audit",
        f"source: {summary[0]['value']}",
        f"old_live_limited_count: {len(old_rows)}",
        f"new_balanced_live_count: {len(latest_rows)}",
        "bucket_limits: " + ", ".join(f"{bucket}={limit}" for bucket, limit in LIVE_ALLOCATION_BUCKETS),
        "bucket_counts: " + ", ".join(f"{k}={v}" for k, v in sorted(bucket_counts.items())),
        f"6507_decision: {focus_rows[0].get('decision')}",
    ]
    (OUT_DIR / "report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
