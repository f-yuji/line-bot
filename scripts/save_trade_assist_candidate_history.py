#!/usr/bin/env python3
"""Persist current trade-assist candidates for date-based UI history."""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from settings_loader import get_settings
from services.trade_assist_history import save_trade_assist_candidate_history

load_dotenv()


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Save trade assist candidate history")
    parser.add_argument("--date")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = get_settings(force_reload=True)
    stop_loss_pct = float(cfg.get("virtual_exit_stop_loss_pct") or 4.0)
    result = save_trade_assist_candidate_history(
        _build_supabase(),
        trade_date=args.date,
        stop_loss_pct=stop_loss_pct,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
