#!/usr/bin/env python3
"""Backfill display-only theoretical position sizing for virtual_trades.

This script updates only virtual_trades sizing metadata. It never writes
actual_trade_logs, notifications, or auto-trading state.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
from supabase import create_client  # noqa: E402

from services.position_sizing import calculate_virtual_position_size  # noqa: E402


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def build_supabase():
    load_dotenv()
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is missing")
    return create_client(url, key)


def _entry_price(row: dict[str, Any]) -> Any:
    return row.get("buy_price") or row.get("virtual_entry_price") or row.get("signal_price")


def update_optional(sb, row_id: str, payload: dict[str, Any]) -> bool:
    remaining = dict(payload)
    while remaining:
        try:
            sb.table("virtual_trades").update(remaining).eq("id", row_id).execute()
            return True
        except Exception as e:
            msg = str(e)
            marker = "Could not find the '"
            if marker in msg:
                missing = msg.split(marker, 1)[1].split("'", 1)[0]
                remaining.pop(missing, None)
                continue
            raise
    return False


def run(args: argparse.Namespace) -> int:
    sb = build_supabase()
    q = (
        sb.table("virtual_trades")
        .select("id,code,name,buy_price,virtual_entry_price,signal_price,status")
        .order("buy_date", desc=True)
    )
    if args.status:
        q = q.eq("status", args.status)
    if args.limit:
        q = q.limit(args.limit)
    rows = q.execute().data or []

    changed = skipped = 0
    samples: list[str] = []
    for row in rows:
        sizing = calculate_virtual_position_size(_entry_price(row))
        if not sizing.get("theoretical_shares"):
            skipped += 1
            continue
        payload = {
            "target_position_size": sizing["target_position_size"],
            "theoretical_shares": sizing["theoretical_shares"],
            "theoretical_position_size": sizing["theoretical_position_size"],
            "lot_type": sizing["lot_type"],
            "position_sizing_rule": sizing["position_sizing_rule"],
            "sizing_note": sizing["sizing_note"],
            "is_capital_constrained": False,
            "actual_position_size": None,
        }
        if len(samples) < 8:
            samples.append(
                f"{row.get('code')} {row.get('name') or ''}: "
                f"{payload['theoretical_shares']} shares / "
                f"{payload['theoretical_position_size']:.0f} / "
                f"{payload['position_sizing_rule']} / "
                f"{payload['sizing_note']}"
            )
        if args.apply:
            update_optional(sb, str(row["id"]), payload)
        changed += 1

    print(f"apply={args.apply}")
    print(f"rows_loaded={len(rows)}")
    print(f"rows_sized={changed}")
    print(f"rows_skipped={skipped}")
    for s in samples:
        print(f"sample={s}")
    print("safety=virtual_trades sizing columns only; actual_trade_logs/LINE/auto-trading untouched")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="Write sizing metadata to virtual_trades")
    p.add_argument("--status", default="", help="Optional status filter, e.g. open")
    p.add_argument("--limit", type=int, default=0)
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
