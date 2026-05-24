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
from services.trading_calendar import latest_feature_date, today_jst

load_dotenv()


def _clear_proxy_env() -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY"):
        os.environ[key] = ""


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
    _clear_proxy_env()
    parser = argparse.ArgumentParser(description="Save trade assist candidate history")
    parser.add_argument("--date")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-non-trading-day", action="store_true")
    args = parser.parse_args()

    sb = _build_supabase()
    latest_date = latest_feature_date(sb)
    today = today_jst().isoformat()
    if not args.date and not args.allow_non_trading_day and latest_date and latest_date != today:
        result = {
            "trade_date": latest_date,
            "rows": 0,
            "skipped": True,
            "reason": f"latest_feature_date_is_not_today:{latest_date}!={today}",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    cfg = get_settings(force_reload=True)
    stop_loss_pct = float(cfg.get("virtual_exit_stop_loss_pct") or 4.0)
    result = save_trade_assist_candidate_history(
        sb,
        trade_date=args.date,
        stop_loss_pct=stop_loss_pct,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
