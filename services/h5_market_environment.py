"""Display-only H5 market environment meter.

The meter explains whether the current market backdrop historically resembles
H5-friendly regimes. It must not be used as an entry filter.
"""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DAILY_MARKET_PATH = ROOT / "outputs/market_data/daily_market_indices.csv"

H5_ENV_VERSION = "h5_env_meter_v1"

INDEX_NAMES = {
    "VIX": "VIX",
    "nikkei225": "Nikkei 225",
    "topix_etf_proxy": "TOPIX proxy",
    "nasdaq": "NASDAQ",
    "sox": "SOX",
    "usdjpy": "USDJPY",
    "us10y_yield": "US 10Y",
}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _date_text(value: Any) -> str:
    return str(value or "").split("T", 1)[0][:10]


def _parse_date(value: Any) -> date | None:
    text = _date_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "nan", "NaN"):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except Exception:
        return default


def _longest_streak(values: list[float], predicate) -> int:
    best = cur = 0
    for value in values:
        if predicate(value):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _pct_change(first: float | None, last: float | None) -> float | None:
    if first in (None, 0) or last is None:
        return None
    return (last / first - 1.0) * 100.0


def _symbol_rows(rows: list[dict[str, Any]], name: str, latest_date: date, lookback_days: int) -> list[dict[str, Any]]:
    start_ord = latest_date.toordinal() - lookback_days
    out = []
    for row in rows:
        if row.get("name") != name:
            continue
        dt = _parse_date(row.get("date"))
        if dt and start_ord <= dt.toordinal() <= latest_date.toordinal():
            out.append(row)
    return sorted(out, key=lambda r: _date_text(r.get("date")))


def _series_metrics(symbol_rows: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [_num(r.get("close")) for r in symbol_rows]
    closes = [v for v in closes if v is not None]
    returns = [_num(r.get("return_pct")) for r in symbol_rows]
    returns = [v for v in returns if v is not None]
    if not closes:
        return {}
    return {
        "latest_close": closes[-1],
        "first_close": closes[0],
        "max_close": max(closes),
        "return_pct": _pct_change(closes[0], closes[-1]),
        "return_5d_pct": _pct_change(closes[-6], closes[-1]) if len(closes) >= 6 else _pct_change(closes[0], closes[-1]),
        "daily_vol": pstdev(returns) if len(returns) > 1 else None,
        "daily_return_mean": mean(returns) if returns else None,
        "max_daily_drop": min(returns) if returns else None,
        "max_daily_gain": max(returns) if returns else None,
        "down_1pct_days": sum(1 for v in returns if v <= -1.0),
        "down_2pct_days": sum(1 for v in returns if v <= -2.0),
        "down_3pct_days": sum(1 for v in returns if v <= -3.0),
        "up_2pct_days": sum(1 for v in returns if v >= 2.0),
        "up_3pct_days": sum(1 for v in returns if v >= 3.0),
        "longest_down_streak": _longest_streak(returns, lambda v: v < 0),
        "last_return_pct": returns[-1] if returns else None,
        "prev_return_pct": returns[-2] if len(returns) >= 2 else None,
    }


def _darasage_score(nikkei: dict[str, Any], topix: dict[str, Any]) -> int:
    src = nikkei or topix or {}
    score = 0
    if (_num(src.get("return_pct")) or 0) < 0:
        score += 1
    if (_num(src.get("down_2pct_days"), 0) or 0) <= 1:
        score += 1
    if (_num(src.get("daily_return_mean")) or 0) < 0:
        score += 1
    if (_num(src.get("max_daily_gain"), 0) or 0) < 2.0:
        score += 1
    if (_num(src.get("longest_down_streak"), 0) or 0) >= 3:
        score += 1
    return score


def _crash_rebound_score(nikkei: dict[str, Any], sox: dict[str, Any]) -> int:
    score = 0
    for src in [nikkei or {}, sox or {}]:
        if (_num(src.get("down_3pct_days"), 0) or 0) >= 1:
            score += 1
        if (_num(src.get("max_daily_gain"), 0) or 0) >= 3.0:
            score += 1
        if (_num(src.get("last_return_pct"), 0) or 0) > 0 and (_num(src.get("prev_return_pct"), 0) or 0) <= -2.0:
            score += 1
        if (_num(src.get("return_5d_pct"), 0) or 0) > 0 and (_num(src.get("max_daily_drop"), 0) or 0) <= -3.0:
            score += 1
    return min(score, 5)


def _classify(score: int, darasage: int, crash: int, tags: list[str]) -> tuple[str, list[str]]:
    labels = list(tags)
    if crash >= 3:
        labels.append("crash rebound mode")
    if darasage >= 3 and crash < 3:
        labels.append("darasage risk")
    if score >= 60:
        status = "H5 favorable"
    elif score >= 35:
        status = "neutral"
    else:
        status = "H5 warning"
    if darasage >= 3 and score < 60:
        status = "darasage risk"
    return status, labels


def build_h5_environment_snapshot(
    *,
    daily_path: Path | None = None,
    as_of: date | None = None,
    lookback_days: int = 21,
    h5_candidate_count: int | None = None,
) -> dict[str, Any]:
    """Return a display-only current environment snapshot."""
    path = daily_path or DAILY_MARKET_PATH
    rows = _read_csv(path)
    if not rows:
        return {
            "available": False,
            "score": None,
            "status": "environment unavailable",
            "tags": ["market data missing"],
            "reason": f"{path} not found or empty",
            "version": H5_ENV_VERSION,
        }
    dates = [_parse_date(r.get("date")) for r in rows]
    dates = [d for d in dates if d]
    latest = as_of or (max(dates) if dates else None)
    if latest is None:
        return {
            "available": False,
            "score": None,
            "status": "environment unavailable",
            "tags": ["market data date missing"],
            "reason": "no usable dates in market data",
            "version": H5_ENV_VERSION,
        }

    metrics = {
        name: _series_metrics(_symbol_rows(rows, name, latest, lookback_days))
        for name in INDEX_NAMES
    }
    vix = metrics.get("VIX") or {}
    nikkei = metrics.get("nikkei225") or {}
    topix = metrics.get("topix_etf_proxy") or {}
    sox = metrics.get("sox") or {}
    nasdaq = metrics.get("nasdaq") or {}

    score = 40
    tags: list[str] = []
    reasons: list[str] = []

    vix_latest = _num(vix.get("latest_close"))
    vix_max = _num(vix.get("max_close"))
    vix_ret = _num(vix.get("return_pct"), 0) or 0
    if vix_max is not None and vix_max >= 30:
        score += 20
        tags.append("VIX 30+")
        reasons.append(f"VIX max {vix_max:.1f}")
    elif vix_latest is not None and vix_latest >= 22:
        score += 10
        tags.append("elevated VIX")
    elif vix_latest is not None and vix_latest < 15:
        score -= 10
        tags.append("low volatility")
    if vix_ret >= 15:
        score += 8
        tags.append("VIX rising")

    nikkei_vol = _num(nikkei.get("daily_vol"))
    if nikkei_vol is not None and nikkei_vol >= 1.8:
        score += 12
        tags.append("Nikkei high volatility")
    elif nikkei_vol is not None and nikkei_vol < 0.8:
        score -= 8
        tags.append("Nikkei low volatility")

    sox_drop = _num(sox.get("max_daily_drop"))
    sox_down3 = _num(sox.get("down_3pct_days"), 0) or 0
    if sox_drop is not None and sox_drop <= -3.0:
        score += 12
        tags.append("SOX shock")
        reasons.append(f"SOX max daily drop {sox_drop:.1f}%")
    if sox_down3 >= 2:
        score += 8
        tags.append("SOX repeated selloff")

    nikkei_drop = _num(nikkei.get("max_daily_drop"))
    nikkei_down2 = _num(nikkei.get("down_2pct_days"), 0) or 0
    if nikkei_drop is not None and nikkei_drop <= -2.0:
        score += 8
        tags.append("Nikkei sharp drop")
    if nikkei_down2 >= 2:
        score += 6

    crash = _crash_rebound_score(nikkei, sox)
    dara = _darasage_score(nikkei, topix)
    score += crash * 5
    score -= dara * 6

    if (_num(nikkei.get("last_return_pct"), 0) or 0) > 0 and (_num(nikkei.get("prev_return_pct"), 0) or 0) <= -1.5:
        score += 8
        tags.append("drop then rebound")
    if (_num(nasdaq.get("max_daily_drop"), 0) or 0) <= -2.0:
        tags.append("NASDAQ shock")

    if h5_candidate_count is not None:
        if h5_candidate_count >= 5:
            score += 5
            tags.append("H5 candidates active")
        elif h5_candidate_count == 0:
            tags.append("no H5 candidates")

    score = max(0, min(100, int(round(score))))
    status, tags = _classify(score, dara, crash, tags)
    tags = list(dict.fromkeys(tags))
    return {
        "available": True,
        "as_of": latest.isoformat(),
        "score": score,
        "status": status,
        "tags": tags,
        "tags_text": ", ".join(tags),
        "darasage_score": dara,
        "crash_rebound_score": crash,
        "vix": vix_latest,
        "vix_max": vix_max,
        "vix_return_pct": vix_ret,
        "nikkei_return_pct": nikkei.get("return_pct"),
        "nikkei_daily_vol": nikkei_vol,
        "nikkei_max_daily_drop": nikkei_drop,
        "nikkei_down_2pct_days": nikkei_down2,
        "topix_return_pct": topix.get("return_pct"),
        "sox_return_pct": sox.get("return_pct"),
        "sox_daily_vol": sox.get("daily_vol"),
        "sox_max_daily_drop": sox_drop,
        "sox_down_3pct_days": sox_down3,
        "nasdaq_return_pct": nasdaq.get("return_pct"),
        "reason": " / ".join(reasons) if reasons else "display-only market environment meter",
        "version": H5_ENV_VERSION,
    }


def attach_environment_to_rows(rows: list[dict[str, Any]], snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    for row in rows:
        row["current_environment_score"] = snapshot.get("score")
        row["environment_tags"] = snapshot.get("tags_text") or ", ".join(snapshot.get("tags") or [])
        row["environment_status"] = snapshot.get("status")
    return rows
