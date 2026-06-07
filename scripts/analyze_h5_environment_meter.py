#!/usr/bin/env python3
"""Backtest the display-only H5 environment meter."""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.h5_market_environment import build_h5_environment_snapshot  # noqa: E402


DEFAULT_OUTPUT = ROOT / "outputs/h5_environment_meter"
MARKET_DAILY = ROOT / "outputs/market_data/daily_market_indices.csv"
CASE_DAILY = ROOT / "outputs/h5_stored_forward_cases/case_daily_rows.csv"
MIX_DAILY = ROOT / "outputs/trend_following_deep_backtest/12_portfolio_mix_daily_rows.csv"
TARGET_CASES = {
    "current_h5": "current_h5_core",
    "H5_short_pullback_drop5_m3": "H5_short_pullback_drop5_m3",
    "H5_current7_short3": "H5_current7_short3",
}
MIX_CASE = "mix_current7_short3_trend_7_3"


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
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def date_text(value: Any) -> str:
    return str(value or "").split("T", 1)[0][:10]


def fnum(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "nan", "NaN"):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except Exception:
        return default


def pearson(xs: list[float], ys: list[float]) -> float | None:
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    xvals = [p[0] for p in pairs]
    yvals = [p[1] for p in pairs]
    xm = mean(xvals)
    ym = mean(yvals)
    num = sum((x - xm) * (y - ym) for x, y in pairs)
    xd = sum((x - xm) ** 2 for x in xvals) ** 0.5
    yd = sum((y - ym) ** 2 for y in yvals) ** 0.5
    return num / (xd * yd) if xd and yd else None


def pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses == 0:
        return None if gains == 0 else 999.0
    return gains / losses


def score_bucket(score: float | None) -> str:
    if score is None:
        return "unknown"
    if score < 30:
        return "0_30_unfavorable"
    if score < 60:
        return "30_60_neutral"
    return "60_100_favorable"


def load_environment_rows() -> list[dict[str, Any]]:
    market = read_csv(MARKET_DAILY)
    dates = sorted({date_text(r.get("date")) for r in market if r.get("date")})
    out = []
    for day in dates:
        try:
            from datetime import datetime
            as_of = datetime.fromisoformat(day).date()
        except Exception:
            continue
        snap = build_h5_environment_snapshot(as_of=as_of)
        if not snap.get("available"):
            continue
        out.append({
            "date": day,
            "environment_score": snap.get("score"),
            "score_bucket": score_bucket(fnum(snap.get("score"))),
            "environment_status": snap.get("status"),
            "environment_tags": snap.get("tags_text"),
            "darasage_score": snap.get("darasage_score"),
            "crash_rebound_score": snap.get("crash_rebound_score"),
            "vix": snap.get("vix"),
            "vix_max": snap.get("vix_max"),
            "nikkei_daily_vol": snap.get("nikkei_daily_vol"),
            "nikkei_max_daily_drop": snap.get("nikkei_max_daily_drop"),
            "sox_daily_vol": snap.get("sox_daily_vol"),
            "sox_max_daily_drop": snap.get("sox_max_daily_drop"),
            "sox_down_3pct_days": snap.get("sox_down_3pct_days"),
            "reason": snap.get("reason"),
        })
    return out


def load_case_rows() -> list[dict[str, Any]]:
    out = []
    for row in read_csv(CASE_DAILY):
        case = TARGET_CASES.get(str(row.get("case_key") or ""))
        if not case:
            continue
        nr = dict(row)
        nr["case_key"] = case
        nr["date"] = date_text(row.get("entry_date") or row.get("signal_date"))
        nr["pnl_after_cost"] = fnum(row.get("pnl_after_cost"), 0.0) or 0.0
        nr["return_pct"] = fnum(row.get("return_pct"), 0.0) or 0.0
        out.append(nr)
    for row in read_csv(MIX_DAILY):
        if str(row.get("mix_case_key") or "") != MIX_CASE:
            continue
        nr = dict(row)
        nr["case_key"] = MIX_CASE
        nr["date"] = date_text(row.get("entry_date") or row.get("signal_date"))
        nr["pnl_after_cost"] = fnum(row.get("pnl_after_cost"), 0.0) or 0.0
        nr["return_pct"] = fnum(row.get("return_pct"), 0.0) or 0.0
        out.append(nr)
    return out


def joined_daily(case_rows: list[dict[str, Any]], env_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    env = {r["date"]: r for r in env_rows}
    out = []
    for row in case_rows:
        e = env.get(str(row.get("date")))
        if not e:
            continue
        out.append({
            **e,
            "case_key": row.get("case_key"),
            "code": row.get("code"),
            "name": row.get("name"),
            "return_pct": row.get("return_pct"),
            "pnl_after_cost": row.get("pnl_after_cost"),
        })
    return out


def summarize(rows: list[dict[str, Any]], group_col: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(str(row.get("case_key")), str(row.get(group_col) or ""))].append(row)
    out = []
    for (case, group), items in sorted(groups.items()):
        pnls = [fnum(r.get("pnl_after_cost"), 0.0) or 0.0 for r in items]
        rets = [fnum(r.get("return_pct"), 0.0) or 0.0 for r in items]
        out.append({
            "case_key": case,
            group_col: group,
            "rows": len(items),
            "active_days": len({r.get("date") for r in items}),
            "avg_environment_score": mean([fnum(r.get("environment_score"), 0.0) or 0.0 for r in items]) if items else None,
            "avg_return_pct": mean(rets) if rets else None,
            "win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
            "PF": pf(pnls),
            "pnl_after_cost": sum(pnls),
        })
    return out


def correlation_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for case in sorted({r.get("case_key") for r in rows}):
        items = [r for r in rows if r.get("case_key") == case]
        y = [fnum(r.get("pnl_after_cost")) for r in items]
        for metric in ["environment_score", "darasage_score", "crash_rebound_score", "vix", "nikkei_daily_vol", "sox_daily_vol"]:
            x = [fnum(r.get(metric)) for r in items]
            out.append({
                "case_key": case,
                "metric": metric,
                "correlation_with_pnl": pearson(x, y),
                "rows": sum(1 for a, b in zip(x, y) if a is not None and b is not None),
            })
    return out


def tag_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded = []
    for row in rows:
        tags = [t.strip() for t in str(row.get("environment_tags") or "").split(",") if t.strip()]
        for tag in tags:
            nr = dict(row)
            nr["environment_tag"] = tag
            expanded.append(nr)
    return summarize(expanded, "environment_tag")


def current_snapshot_row() -> list[dict[str, Any]]:
    snap = build_h5_environment_snapshot()
    return [{
        "as_of": snap.get("as_of"),
        "environment_score": snap.get("score"),
        "environment_status": snap.get("status"),
        "environment_tags": snap.get("tags_text"),
        "darasage_score": snap.get("darasage_score"),
        "crash_rebound_score": snap.get("crash_rebound_score"),
        "vix": snap.get("vix"),
        "vix_max": snap.get("vix_max"),
        "nikkei_daily_vol": snap.get("nikkei_daily_vol"),
        "sox_daily_vol": snap.get("sox_daily_vol"),
        "reason": snap.get("reason"),
    }]


def report_text(corr: list[dict[str, Any]], bucket: list[dict[str, Any]], fav: list[dict[str, Any]], current: list[dict[str, Any]]) -> str:
    h5_corr = [r for r in corr if r.get("case_key") == "current_h5_core" and r.get("metric") == "environment_score"]
    h5_fav = [r for r in fav if r.get("case_key") == "current_h5_core" and r.get("environment_status") == "H5 favorable"]
    h5_warn = [r for r in fav if r.get("case_key") == "current_h5_core" and r.get("environment_status") in {"H5 warning", "darasage risk"}]
    crash = [r for r in fav if r.get("case_key") == "current_h5_core" and r.get("environment_status") == "H5 favorable"]
    cur = current[0] if current else {}
    lines = [
        "H5 environment meter analysis",
        "",
        "Production impact: none. The meter is display-only and is not an entry filter.",
        f"environment_score correlation with current_h5_core pnl: {fnum((h5_corr[0] if h5_corr else {}).get('correlation_with_pnl'), 0):.3f}",
        f"favorable PF: {fnum((h5_fav[0] if h5_fav else {}).get('PF'), 0):.3f}",
        f"warning PF: {fnum((h5_warn[0] if h5_warn else {}).get('PF'), 0):.3f}",
        f"crash rebound / favorable rows: {(h5_fav[0] if h5_fav else {}).get('rows', 0)}",
        "",
        "Current market:",
        f"- as_of: {cur.get('as_of')}",
        f"- score: {cur.get('environment_score')}",
        f"- status: {cur.get('environment_status')}",
        f"- tags: {cur.get('environment_tags')}",
        f"- VIX: {cur.get('vix')} / VIX max: {cur.get('vix_max')}",
        "",
        "How to use:",
        "- Use as context on LIVE candidates and dashboard only.",
        "- Do not change H5 entry thresholds from this score.",
        "- favorable means the environment resembles high-volatility / crash-rebound months.",
        "- darasage risk means slow weakness without enough rebound; be more skeptical, but do not auto-filter.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    out_dir = Path(args.output_dir)

    env_rows = load_environment_rows()
    case_rows = load_case_rows()
    joined = joined_daily(case_rows, env_rows)
    bucket = summarize(joined, "score_bucket")
    fav = summarize(joined, "environment_status")
    tags = tag_summary(joined)
    corr = correlation_summary(joined)
    current = current_snapshot_row()

    write_csv(out_dir / "environment_daily_rows.csv", env_rows)
    write_csv(out_dir / "environment_trade_joined_rows.csv", joined)
    write_csv(out_dir / "environment_bucket_summary.csv", bucket)
    write_csv(out_dir / "favorable_vs_warning.csv", fav + tags)
    write_csv(out_dir / "current_environment_snapshot.csv", current)
    write_csv(out_dir / "correlation_summary.csv", corr)
    write_text(out_dir / "report.txt", report_text(corr, bucket, fav, current))

    print(f"output_dir={out_dir}")
    print(f"environment_days={len(env_rows)}")
    print(f"joined_rows={len(joined)}")
    print(f"current_score={current[0].get('environment_score') if current else ''}")
    print(f"current_status={current[0].get('environment_status') if current else ''}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
