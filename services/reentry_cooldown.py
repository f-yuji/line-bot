"""Re-entry cooldown rules for virtual trades."""
from __future__ import annotations

from typing import Any


DEFAULT_REENTRY_COOLDOWN_DAYS = 10
PROFIT_REENTRY_COOLDOWN_DAYS = 3

SHORT_PROFIT_EXIT_REASONS = {
    "rsi75_pullback1",
    "pullback2",
    "take_profit",
}


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def reentry_cooldown_days_for_closed_trade(row: dict[str, Any]) -> int:
    """Return cooldown days based on the previous trade's exit type.

    Profit-taking exits can re-enter sooner; stop/timeout/unknown exits keep
    the conservative default cooldown.
    """

    reason = str(row.get("exit_reason") or row.get("sell_reason") or "").strip()
    pnl_pct = _float(row.get("profit_loss_pct") or row.get("virtual_pnl_pct") or row.get("actual_pnl_pct"))
    if reason in SHORT_PROFIT_EXIT_REASONS:
        return PROFIT_REENTRY_COOLDOWN_DAYS
    if reason == "peak_pullback_exit" and pnl_pct is not None and pnl_pct > 0:
        return PROFIT_REENTRY_COOLDOWN_DAYS
    return DEFAULT_REENTRY_COOLDOWN_DAYS
