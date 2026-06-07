#!/usr/bin/env python3
"""Analyze H5 monthly performance by volatility and crash/rebound regimes.

Research only. Reads CSV outputs and writes analysis reports. No DB writes,
LINE changes, actual_trade_logs changes, or auto-trading changes.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "outputs/h5_volatility_regime"
CASE_SUMMARY = ROOT / "outputs/strategy_monthly_regime_performance/monthly_case_summary.csv"
MARKET_MONTHLY = ROOT / "outputs/market_data/monthly_market_volatility.csv"
MARKET_DAILY = ROOT / "outputs/market_data/daily_market_indices.csv"

TARGET_CASES = [
    "current_h5_core",
    "H5_short_pullback_drop5_m3",
    "H5_current7_short3",
    "trend_support_best",
    "mix_current7_short3_trend_7_3",
]


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
    if not xd or not yd:
        return None
    return num / (xd * yd)


def bucket_tertile(value: float | None, values: list[float]) -> str:
    if value is None or not values:
        return "unknown"
    ordered = sorted(values)
    low = ordered[len(ordered) // 3]
    high = ordered[(len(ordered) * 2) // 3]
    if value <= low:
        return "low_vol"
    if value <= high:
        return "mid_vol"
    return "high_vol"


def group_label_count(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0:
        return "0_days"
    if value <= 1:
        return "1_day"
    return "2plus_days"


def by_symbol_month(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(r.get("name") or r.get("ticker")), str(r.get("year_month"))): r for r in rows}


def darasage_score(nikkei: dict[str, Any], topix: dict[str, Any]) -> int:
    src = nikkei or topix or {}
    score = 0
    monthly = fnum(src.get("monthly_return_pct"))
    daily_mean = fnum(src.get("daily_return_mean"))
    down2 = fnum(src.get("down_2pct_days"), 0) or 0
    max_gain = fnum(src.get("max_daily_gain_pct"), 0) or 0
    longest_down = fnum(src.get("longest_down_streak"), 0) or 0
    if monthly is not None and monthly < 0:
        score += 1
    if down2 <= 1:
        score += 1
    if daily_mean is not None and daily_mean < 0:
        score += 1
    if max_gain < 2.0:
        score += 1
    if longest_down >= 3:
        score += 1
    return score


def crash_rebound_score(nikkei: dict[str, Any], sox: dict[str, Any]) -> int:
    score = 0
    for src in [nikkei or {}, sox or {}]:
        down3 = fnum(src.get("down_3pct_days"), 0) or 0
        max_gain = fnum(src.get("max_daily_gain_pct"), 0) or 0
        monthly = fnum(src.get("monthly_return_pct"))
        dd = fnum(src.get("max_drawdown_pct"), 0) or 0
        if down3 >= 1:
            score += 1
        if max_gain >= 3.0:
            score += 1
        if monthly is not None and monthly > 0:
            score += 1
        if dd <= -5.0 and monthly is not None and monthly > dd / 2:
            score += 1
    return score


def build_joined(case_rows: list[dict[str, Any]], market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    market = by_symbol_month(market_rows)
    out = []
    for row in case_rows:
        case = str(row.get("case_key") or "")
        if case not in TARGET_CASES:
            continue
        ym = str(row.get("year_month") or "")
        vix = market.get(("VIX", ym), {})
        nikkei = market.get(("nikkei225", ym), {})
        topix = market.get(("topix_etf_proxy", ym), {})
        sox = market.get(("sox", ym), {})
        nasdaq = market.get(("nasdaq", ym), {})
        usdjpy = market.get(("usdjpy", ym), {})
        tnx = market.get(("us10y_yield", ym), {})
        d_score = darasage_score(nikkei, topix)
        c_score = crash_rebound_score(nikkei, sox)
        vol_proxy = fnum(vix.get("avg_close"))
        if vol_proxy is None:
            vol_proxy = fnum(nikkei.get("prev_day_return_std")) or fnum(topix.get("prev_day_return_std"))
        out.append({
            "year_month": ym,
            "case_key": case,
            "count": row.get("count"),
            "taxed_pnl": row.get("taxed_pnl"),
            "PF": row.get("PF"),
            "win_rate": row.get("win_rate"),
            "max_dd": row.get("max_dd"),
            "avg_return_pct": row.get("avg_return_pct"),
            "vix_avg": vix.get("avg_close"),
            "vix_max": vix.get("max_close"),
            "vix_end": vix.get("end_close"),
            "vix_month_return": vix.get("monthly_return_pct"),
            "vix_jump_10pct_days": vix.get("up_2pct_days"),
            "nikkei_month_return": nikkei.get("monthly_return_pct"),
            "topix_month_return": topix.get("monthly_return_pct"),
            "nikkei_daily_vol": nikkei.get("prev_day_return_std"),
            "topix_daily_vol": topix.get("prev_day_return_std"),
            "nikkei_max_daily_drop": nikkei.get("max_daily_drop_pct"),
            "topix_max_daily_drop": topix.get("max_daily_drop_pct"),
            "nikkei_down_2pct_days": nikkei.get("down_2pct_days"),
            "nikkei_down_3pct_days": nikkei.get("down_3pct_days"),
            "topix_down_2pct_days": topix.get("down_2pct_days"),
            "topix_down_3pct_days": topix.get("down_3pct_days"),
            "sox_month_return": sox.get("monthly_return_pct"),
            "sox_daily_vol": sox.get("prev_day_return_std"),
            "sox_max_daily_drop": sox.get("max_daily_drop_pct"),
            "sox_down_3pct_days": sox.get("down_3pct_days"),
            "nasdaq_month_return": nasdaq.get("monthly_return_pct"),
            "nasdaq_daily_vol": nasdaq.get("prev_day_return_std"),
            "usdjpy_month_return": usdjpy.get("monthly_return_pct"),
            "us10y_month_change": tnx.get("monthly_return_pct"),
            "darasage_score": d_score,
            "crash_rebound_score": c_score,
            "regime_type": "crash_rebound" if c_score >= 3 else ("darasage" if d_score >= 3 else "normal"),
            "vol_proxy": vol_proxy,
        })
    vals = [fnum(r.get("vol_proxy")) for r in out if r.get("case_key") == "current_h5_core"]
    vals = [v for v in vals if v is not None]
    for row in out:
        row["vol_bucket"] = bucket_tertile(fnum(row.get("vol_proxy")), vals)
        row["nikkei_down2_bucket"] = group_label_count(fnum(row.get("nikkei_down_2pct_days")))
        row["sox_down3_bucket"] = group_label_count(fnum(row.get("sox_down_3pct_days")))
    return out


def correlation_rows(joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    h5 = [r for r in joined if r.get("case_key") == "current_h5_core"]
    targets = [
        "vix_avg", "vix_max", "nikkei_daily_vol", "topix_daily_vol", "sox_daily_vol",
        "nikkei_max_daily_drop", "sox_max_daily_drop", "nikkei_down_2pct_days",
        "sox_down_3pct_days", "darasage_score", "crash_rebound_score",
    ]
    out = []
    y = [fnum(r.get("taxed_pnl")) for r in h5]
    for target in targets:
        x = [fnum(r.get(target)) for r in h5]
        out.append({
            "case_key": "current_h5_core",
            "metric": target,
            "correlation_with_taxed_pnl": pearson(x, y),
            "usable_months": sum(1 for a, b in zip(x, y) if a is not None and b is not None),
        })
    return out


def summary_by(joined: list[dict[str, Any]], group_col: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in joined:
        groups[(str(row.get("case_key")), str(row.get(group_col) or "unknown"))].append(row)
    out = []
    for (case, group), rows in sorted(groups.items()):
        pnls = [fnum(r.get("taxed_pnl"), 0.0) or 0.0 for r in rows]
        pfs = [fnum(r.get("PF")) for r in rows]
        pfs = [v for v in pfs if v is not None]
        dds = [fnum(r.get("max_dd"), 0.0) or 0.0 for r in rows]
        out.append({
            "case_key": case,
            group_col: group,
            "months": len(rows),
            "taxed_pnl_total": sum(pnls),
            "avg_monthly_taxed_pnl": mean(pnls) if pnls else None,
            "monthly_win_rate": sum(1 for v in pnls if v > 0) / len(pnls) * 100 if pnls else None,
            "avg_PF": mean(pfs) if pfs else None,
            "avg_max_dd": mean(dds) if dds else None,
        })
    return out


def current_month_assessment(joined: list[dict[str, Any]], market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    h5 = [r for r in joined if r.get("case_key") == "current_h5_core"]
    market = by_symbol_month(market_rows)
    latest_market = max((str(r.get("year_month")) for r in market_rows), default="")
    latest_h5 = max((str(r.get("year_month")) for r in h5), default="")
    latest = max(latest_market, latest_h5)
    if not latest:
        return []
    row = next((r for r in h5 if str(r.get("year_month")) == latest), None)
    if row is None:
        vix = market.get(("VIX", latest), {})
        nikkei = market.get(("nikkei225", latest), {})
        topix = market.get(("topix_etf_proxy", latest), {})
        sox = market.get(("sox", latest), {})
        row = {
            "year_month": latest,
            "vix_avg": vix.get("avg_close"),
            "vix_max": vix.get("max_close"),
            "nikkei_down_2pct_days": nikkei.get("down_2pct_days"),
            "sox_down_3pct_days": sox.get("down_3pct_days"),
            "darasage_score": darasage_score(nikkei, topix),
            "crash_rebound_score": crash_rebound_score(nikkei, sox),
            "vol_bucket": "current_partial_month",
        }
    vix_avg = fnum(row.get("vix_avg"))
    vix_max = fnum(row.get("vix_max"))
    high_vix = (vix_avg is not None and vix_avg >= 25) or (vix_max is not None and vix_max >= 30)
    crash = fnum(row.get("crash_rebound_score"), 0) or 0
    dara = fnum(row.get("darasage_score"), 0) or 0
    favorable = high_vix or crash >= 3
    caution = []
    if dara >= 3:
        caution.append("darasage_proxy")
    if fnum(row.get("sox_down_3pct_days"), 0):
        caution.append("sox_large_down_days")
    if fnum(row.get("nikkei_down_2pct_days"), 0):
        caution.append("nikkei_large_down_days")
    return [{
        "year_month": latest,
        "assessment_basis": "h5_and_market" if latest == latest_h5 else "market_only_partial_month",
        "vix_avg": row.get("vix_avg"),
        "vix_max": row.get("vix_max"),
        "vol_bucket": row.get("vol_bucket"),
        "nikkei_down_2pct_days": row.get("nikkei_down_2pct_days"),
        "sox_down_3pct_days": row.get("sox_down_3pct_days"),
        "darasage_score": row.get("darasage_score"),
        "crash_rebound_score": row.get("crash_rebound_score"),
        "darasage_like": dara >= 3,
        "crash_rebound_like": crash >= 3,
        "h5_favorable": favorable and dara < 4,
        "caution": ",".join(caution),
        "note": "High VIX / crash-rebound is favorable; persistent darasage remains a warning. Market-only rows have no completed H5 monthly PnL yet.",
    }]


def proxy_usage(market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = {str(r.get("name")) for r in market_rows}
    requested = {
        "VIX": "VIX",
        "Nikkei 225": "nikkei225",
        "TOPIX": "topix_etf_proxy",
        "NASDAQ": "nasdaq",
        "SOX": "sox",
        "USDJPY": "usdjpy",
        "US 10Y": "us10y_yield",
        "Nikkei VI": "not_fetched",
    }
    out = []
    for label, name in requested.items():
        out.append({
            "requested_data": label,
            "status": "available" if name in names else ("proxy" if name == "topix_etf_proxy" and name in names else "not_available"),
            "source": "outputs/market_data/monthly_market_volatility.csv" if name in names else "",
            "note": "TOPIX uses 1306.T ETF proxy" if label == "TOPIX" else "",
        })
    return out


def report(joined: list[dict[str, Any]], corr: list[dict[str, Any]], current: list[dict[str, Any]]) -> str:
    h5 = [r for r in joined if r.get("case_key") == "current_h5_core"]
    high = [r for r in h5 if r.get("vol_bucket") == "high_vol"]
    low = [r for r in h5 if r.get("vol_bucket") == "low_vol"]
    dara = [r for r in h5 if r.get("regime_type") == "darasage"]
    crash = [r for r in h5 if r.get("regime_type") == "crash_rebound"]
    best_corr = sorted(
        [r for r in corr if fnum(r.get("correlation_with_taxed_pnl")) is not None],
        key=lambda r: abs(fnum(r.get("correlation_with_taxed_pnl"), 0) or 0),
        reverse=True,
    )[:5]

    def pnl(rows: list[dict[str, Any]]) -> float:
        return sum(fnum(r.get("taxed_pnl"), 0.0) or 0.0 for r in rows)

    lines = [
        "H5 volatility regime report",
        "",
        "Production impact: none. CSV analysis only; no DB/LINE/actual_trade_logs/auto-trading changes.",
        f"months: {len(h5)}",
        f"high_vol_months: {len(high)}, taxed_pnl={pnl(high):,.0f}",
        f"low_vol_months: {len(low)}, taxed_pnl={pnl(low):,.0f}",
        f"darasage_months: {len(dara)}, taxed_pnl={pnl(dara):,.0f}",
        f"crash_rebound_months: {len(crash)}, taxed_pnl={pnl(crash):,.0f}",
        "",
        "Strongest correlations with current_h5_core taxed PnL:",
        *[
            f"- {r.get('metric')}: corr={fnum(r.get('correlation_with_taxed_pnl'), 0):.3f}, months={r.get('usable_months')}"
            for r in best_corr
        ],
        "",
        "Current/latest month assessment:",
        *[
            f"- {r.get('year_month')} ({r.get('assessment_basis')}): vix_avg={r.get('vix_avg')}, vix_max={r.get('vix_max')}, "
            f"vol={r.get('vol_bucket')}, darasage={r.get('darasage_score')}, crash_rebound={r.get('crash_rebound_score')}, "
            f"h5_favorable={r.get('h5_favorable')}, caution={r.get('caution')}"
            for r in current
        ],
        "",
        "Interpretation:",
        "- If VIX is 30+ and this is a sharp selloff followed by rebound, it is the regime H5 is designed to exploit.",
        "- If VIX is high because selling persists without rebound, H5 can still suffer; darasage_score is the warning.",
        "- Use high VIX as an alert, not a buy override. Keep AI/stage/gap/overheat filters intact.",
        "",
        "Filter idea:",
        "- Monitoring-only: mark H5 favorable when VIX max >= 30 or VIX avg >= 25 and crash_rebound_score >= 3.",
        "- Warning: reduce confidence when darasage_score >= 3 and crash_rebound_score < 3.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    case_rows = read_csv(CASE_SUMMARY)
    market_rows = read_csv(MARKET_MONTHLY)
    joined = build_joined(case_rows, market_rows)
    corr = correlation_rows(joined)
    vol_summary = summary_by(joined, "vol_bucket")
    regime_summary = summary_by(joined, "regime_type")
    nikkei_down = summary_by(joined, "nikkei_down2_bucket")
    sox_down = summary_by(joined, "sox_down3_bucket")
    drawdown_summary = nikkei_down + sox_down
    current = current_month_assessment(joined, market_rows)
    proxy = proxy_usage(market_rows)

    write_csv(out_dir / "monthly_h5_volatility_joined.csv", joined)
    write_csv(out_dir / "volatility_correlation.csv", corr)
    write_csv(out_dir / "volatility_bucket_summary.csv", vol_summary)
    write_csv(out_dir / "crash_vs_darasage_summary.csv", regime_summary)
    write_csv(out_dir / "drawdown_day_count_summary.csv", drawdown_summary)
    write_csv(out_dir / "current_month_volatility_assessment.csv", current)
    write_csv(out_dir / "proxy_usage.csv", proxy)
    write_text(out_dir / "report.txt", report(joined, corr, current))

    print(f"output_dir={out_dir}")
    print(f"joined_rows={len(joined)}")
    print(f"correlation_rows={len(corr)}")
    print(f"current_month={current[0].get('year_month') if current else ''}")
    print("production_changes=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
