#!/usr/bin/env python3
"""Write a simple GitHub Actions cron run log to research_import_logs.

This is intentionally generic and read-mostly except for the single insert into
the research log table. It does not touch trading state, virtual_trades, models,
or trade_case_* tables.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

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


def _github_context() -> dict[str, Any]:
    return {
        "github_run_id": _opt("GITHUB_RUN_ID"),
        "github_run_number": _opt("GITHUB_RUN_NUMBER"),
        "github_job": _opt("GITHUB_JOB"),
        "github_workflow": _opt("GITHUB_WORKFLOW"),
        "github_event_name": _opt("GITHUB_EVENT_NAME"),
        "github_ref": _opt("GITHUB_REF"),
        "github_sha": _opt("GITHUB_SHA"),
        "github_server_url": _opt("GITHUB_SERVER_URL"),
        "github_repository": _opt("GITHUB_REPOSITORY"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Log a cron job run to research_import_logs")
    parser.add_argument("--job", required=True)
    parser.add_argument("--status", default="completed")
    parser.add_argument("--schedule", default="")
    parser.add_argument("--error-message")
    parser.add_argument("--notes", default="")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).isoformat()
    status = args.status or "completed"
    if args.error_message and status == "completed":
        status = "failed"

    params = {
        "job": args.job,
        "schedule": args.schedule,
        "notes": args.notes,
        **_github_context(),
    }
    row = {
        "dataset_key": f"cron:{args.job}:{now[:10]}",
        "job_type": f"cron:{args.job}",
        "status": status,
        "started_at": now,
        "finished_at": now,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "error_message": args.error_message,
        "params": params,
    }
    _build_supabase().table("research_import_logs").insert(row).execute()
    print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
