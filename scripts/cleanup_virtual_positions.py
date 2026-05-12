#!/usr/bin/env python3
"""Clean up excessive open virtual trades without deleting history.

Default is dry-run. Add --execute to close selected rows.
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from settings_loader import get_settings

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STAGE_RANK = {
    "strong_confirmed": 3,
    "confirmed": 2,
    "early": 1,
    "none": 0,
    None: 0,
}


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _rank(row: dict) -> tuple:
    return (
        STAGE_RANK.get(row.get("signal_stage"), 0),
        _float(row.get("expected_value"), -999.0),
        _float(row.get("entry_probability"), 0.0),
        _float(row.get("entry_score") or row.get("buy_score"), 0.0),
        str(row.get("created_at") or row.get("buy_date") or ""),
    )


def _close_payload(row: dict, reason: str, now: str) -> dict:
    buy = _float(row.get("buy_price"), 0.0)
    qty = int(_float(row.get("quantity"), 100))
    sell = row.get("current_price") or row.get("sell_price") or row.get("buy_price")
    sell_f = _float(sell, buy)
    pnl = (sell_f - buy) * qty if buy > 0 else None
    pnl_pct = (sell_f - buy) / buy * 100 if buy > 0 else None
    payload = {
        "status": "closed",
        "sell_date": now,
        "sell_price": sell_f,
        "sell_reason": "manual",
        "exit_reason": reason,
        "exit_checked_at": now,
        "updated_at": now,
    }
    if pnl is not None:
        payload["profit_loss"] = round(pnl, 0)
        payload["profit_loss_pct"] = round(pnl_pct, 2)
    return payload


def _select_cleanup_targets(open_rows: list[dict], max_open: int, keep_per_code: int) -> list[tuple[dict, str]]:
    targets: list[tuple[dict, str]] = []
    keep_ids: set[str] = set()

    by_code: dict[str, list[dict]] = {}
    for row in open_rows:
        by_code.setdefault(str(row.get("code") or ""), []).append(row)

    for code, rows in by_code.items():
        ranked = sorted(rows, key=_rank, reverse=True)
        keep = ranked[:max(1, keep_per_code)]
        extras = ranked[max(1, keep_per_code):]
        keep_ids.update(str(r.get("id")) for r in keep if r.get("id"))
        for row in extras:
            targets.append((row, "cleanup_duplicate_open"))
            logger.info(
                "[cleanup_duplicate] close code=%s id=%s stage=%s ev=%s prob=%s",
                code,
                row.get("id"),
                row.get("signal_stage"),
                row.get("expected_value"),
                row.get("entry_probability"),
            )

    remaining = [r for r in open_rows if str(r.get("id")) in keep_ids]
    if max_open > 0 and len(remaining) > max_open:
        ranked_remaining = sorted(remaining, key=_rank, reverse=True)
        close_excess = ranked_remaining[max_open:]
        for row in close_excess:
            targets.append((row, "cleanup_position_limit"))
            logger.info(
                "[cleanup_position_limit] close code=%s id=%s stage=%s ev=%s prob=%s",
                row.get("code"),
                row.get("id"),
                row.get("signal_stage"),
                row.get("expected_value"),
                row.get("entry_probability"),
            )

    seen: set[str] = set()
    deduped: list[tuple[dict, str]] = []
    for row, reason in targets:
        row_id = str(row.get("id") or "")
        if row_id and row_id not in seen:
            deduped.append((row, reason))
            seen.add(row_id)
    return deduped


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    cfg = get_settings(force_reload=True)
    max_open = args.max_open if args.max_open is not None else int(cfg.get("max_open_positions") or 20)

    rows = (
        sb.table("virtual_trades")
        .select("*")
        .eq("status", "open")
        .is_("sell_date", "null")
        .execute()
        .data or []
    )
    targets = _select_cleanup_targets(rows, max_open=max_open, keep_per_code=args.keep_per_code)
    logger.info(
        "cleanup plan: open=%d max_open=%d keep_per_code=%d close=%d execute=%s",
        len(rows),
        max_open,
        args.keep_per_code,
        len(targets),
        args.execute,
    )
    if not args.execute:
        logger.info("dry-run only. Add --execute to close these virtual trades.")
        return

    now = datetime.now(timezone.utc).isoformat()
    closed = 0
    for row, reason in targets:
        try:
            sb.table("virtual_trades").update(_close_payload(row, reason, now)).eq("id", row["id"]).execute()
            closed += 1
        except Exception as e:
            logger.error("close failed code=%s id=%s error=%s", row.get("code"), row.get("id"), e)
    logger.info("cleanup complete: closed=%d remaining_estimate=%d", closed, len(rows) - closed)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean up excessive open virtual trades")
    p.add_argument("--execute", action="store_true", help="Actually close selected rows")
    p.add_argument("--max-open", type=int, help="Target max open positions. Defaults to strategy_settings.max_open_positions")
    p.add_argument("--keep-per-code", type=int, default=1, help="Keep this many open rows per code")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
