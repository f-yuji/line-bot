"""Shared display-only position sizing for virtual trades.

This module is intentionally independent from production entry, notification,
actual_trade_logs, and auto-trading paths. It only answers: if this virtual
trade were sized around 300k JPY, how many shares would we display?
"""

from __future__ import annotations

import math
from typing import Any

TARGET_POSITION_SIZE = 300_000.0
ONE_UNIT_MIN_VALUE = 250_000.0
ONE_UNIT_MAX_VALUE = 400_000.0
UNIT_SHARES = 100


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def calculate_virtual_position_size(
    entry_price: Any,
    *,
    target_position_size: float = TARGET_POSITION_SIZE,
) -> dict[str, Any]:
    price = _to_float(entry_price)
    if price is None or price <= 0:
        return {
            "target_position_size": target_position_size,
            "theoretical_shares": None,
            "theoretical_position_size": None,
            "lot_type": None,
            "position_sizing_rule": "missing_entry_price",
            "sizing_note": "-",
            "is_capital_constrained": False,
            "actual_position_size": None,
        }

    unit_value = price * UNIT_SHARES
    if ONE_UNIT_MIN_VALUE <= unit_value <= ONE_UNIT_MAX_VALUE:
        shares = UNIT_SHARES
        lot_type = "one_unit"
        rule = "one_unit_25_40"
        note = "100株で範囲内"
    elif unit_value < ONE_UNIT_MIN_VALUE:
        units = max(1, int(round(target_position_size / unit_value)))
        shares = units * UNIT_SHARES
        lot_type = "multi_unit" if shares > UNIT_SHARES else "one_unit"
        rule = "multi_unit_to_300k"
        note = f"{shares}株で30万円寄せ"
    else:
        shares = max(1, int(math.floor(target_position_size / price)))
        lot_type = "s_share"
        rule = "s_share_to_300k"
        note = f"S株{shares}株で30万円寄せ"

    return {
        "target_position_size": target_position_size,
        "theoretical_shares": shares,
        "theoretical_position_size": round(price * shares, 4),
        "lot_type": lot_type,
        "position_sizing_rule": rule,
        "sizing_note": note,
        "is_capital_constrained": False,
        "actual_position_size": None,
    }


def calculate_theoretical_position_size(price: Any, target: float = TARGET_POSITION_SIZE) -> dict[str, Any]:
    return calculate_virtual_position_size(price, target_position_size=target)


def decorate_virtual_trade_position(row: dict[str, Any], *, price_key: str = "buy_price") -> dict[str, Any]:
    sizing = calculate_virtual_position_size(row.get(price_key))
    for key, value in sizing.items():
        if row.get(key) in (None, "") or key in {"theoretical_position_size", "theoretical_shares"}:
            row[key] = value
    return row
