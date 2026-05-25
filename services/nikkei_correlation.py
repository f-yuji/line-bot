"""Display-only Nikkei correlation metrics for stock rows.

These metrics describe how closely a stock has moved with the Nikkei 225.
They are intentionally not used by signal generation or trade execution.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _pearson(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 20:
        return None
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in pairs)
    x_var = sum((x - x_mean) ** 2 for x in xs)
    y_var = sum((y - y_mean) ** 2 for y in ys)
    denominator = math.sqrt(x_var * y_var)
    if denominator == 0:
        return None
    return max(-1.0, min(1.0, numerator / denominator))


def _display_metric(correlation: float | None, observations: int) -> dict:
    if correlation is None:
        return {
            "nikkei_correlation_60d": None,
            "nikkei_link_score": None,
            "nikkei_link_level": "-",
            "nikkei_link_observations": observations,
        }
    score = round(max(0.0, correlation) * 100)
    if correlation < -0.20:
        level = "逆行"
    elif correlation >= 0.75:
        level = "高"
    elif correlation >= 0.45:
        level = "中"
    else:
        level = "低"
    return {
        "nikkei_correlation_60d": round(correlation, 2),
        "nikkei_link_score": score,
        "nikkei_link_level": level,
        "nikkei_link_observations": observations,
    }


def decorate_nikkei_correlation(sb, display_rows: list[dict], *, window: int = 60) -> list[dict]:
    """Attach current 60-session Nikkei linkage values to UI row dictionaries."""
    codes = sorted({str(row.get("code") or "") for row in display_rows if row.get("code")})
    if not codes:
        return display_rows

    histories: dict[str, list[dict]] = defaultdict(list)
    # Fetch a modest extra buffer for rows with missing index changes.
    limit_rows = max(window + 20, 80)
    cutoff = (date.today() - timedelta(days=130)).isoformat()
    for start in range(0, len(codes), 10):
        chunk = codes[start:start + 10]
        rows = (
            sb.table("stock_feature_snapshots")
            .select("trade_date,code,day_change_pct,nikkei_change_pct")
            .in_("code", chunk)
            .gte("trade_date", cutoff)
            .order("trade_date", desc=True)
            .limit(1000)
            .execute()
            .data
            or []
        )
        for row in rows:
            code = str(row.get("code") or "")
            if code and len(histories[code]) < limit_rows:
                histories[code].append(row)

    metrics: dict[str, dict] = {}
    for code, rows in histories.items():
        pairs: list[tuple[float, float]] = []
        for row in rows:
            stock_return = _to_float(row.get("day_change_pct"))
            nikkei_return = _to_float(row.get("nikkei_change_pct"))
            if stock_return is not None and nikkei_return is not None:
                pairs.append((stock_return, nikkei_return))
            if len(pairs) >= window:
                break
        metrics[code] = _display_metric(_pearson(pairs), len(pairs))

    for row in display_rows:
        row.update(metrics.get(str(row.get("code") or ""), _display_metric(None, 0)))
    return display_rows
