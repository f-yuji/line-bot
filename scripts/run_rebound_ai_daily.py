#!/usr/bin/env python3
"""Run the end-of-day rebound AI pipeline as a single cron command.

This is the entry point intended for Render Cron, Fly workers, or GitHub
Actions. Keeping the pipeline in one script prevents cron providers from
drifting apart.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class Step:
    name: str
    args: list[str]


def _clear_proxy_env() -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY"):
        os.environ[key] = ""


def _ts() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S JST")


def _run_step(step: Step, *, dry_run: bool = False) -> None:
    cmd = [sys.executable, *step.args]
    print(f"[rebound_ai_daily] start step={step.name} at={_ts()} cmd={' '.join(step.args)}", flush=True)
    if dry_run:
        print(f"[rebound_ai_daily] DRYRUN skip step={step.name}", flush=True)
        return
    started = time.monotonic()
    subprocess.run(cmd, check=True)
    elapsed = time.monotonic() - started
    print(f"[rebound_ai_daily] done step={step.name} elapsed={elapsed:.1f}s", flush=True)


def _run_log(status: str, *, error_message: str | None = None, dry_run: bool = False) -> None:
    if dry_run:
        print(f"[rebound_ai_daily] DRYRUN skip logs status={status} error={error_message or ''}", flush=True)
        return
    commands = [
        [
            sys.executable,
            "scripts/log_rebound_ai_daily.py",
            "--status",
            status,
            *(["--error-message", error_message] if error_message else []),
        ],
        [
            sys.executable,
            "scripts/log_cron_run.py",
            "--job",
            "rebound-ai-daily",
            "--status",
            status,
            "--schedule",
            "Render Cron / end-of-day JST",
            *(["--error-message", error_message] if error_message else []),
        ],
    ]
    for cmd in commands:
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"[rebound_ai_daily] log command failed cmd={' '.join(cmd[1:])} error={e}", flush=True)


def _steps(args: argparse.Namespace) -> list[Step]:
    feature_args = [
        "scripts/generate_feature_snapshots.py",
        "--date",
        args.date,
        "--source",
        "jquants",
        "--sleep-seconds",
        str(args.feature_sleep_seconds),
        "--max-retries",
        str(args.feature_max_retries),
        "--retry-wait-seconds",
        str(args.feature_retry_wait_seconds),
        "--cooldown-on-429",
        str(args.feature_cooldown_on_429),
    ]
    margin_args = [
        "scripts/import_latest_entry_margins.py",
        "--lookback-days",
        str(args.margin_lookback_days),
        "--limit",
        str(args.margin_limit),
        "--sleep-sec",
        str(args.margin_sleep_seconds),
        "--retry-wait-seconds",
        str(args.margin_retry_wait_seconds),
    ]
    return [
        Step("generate_feature_snapshots", feature_args),
        Step("update_long_term_market_regime", ["scripts/update_long_term_market_regime.py"]),
        Step("check_virtual_trades", ["scripts/check_virtual_trades.py"]),
        Step("import_latest_entry_margins", margin_args),
        Step("predict_rebound", ["scripts/predict_rebound.py", "--latest"]),
        Step("save_trade_assist_candidate_history", ["scripts/save_trade_assist_candidate_history.py"]),
    ]


def run(args: argparse.Namespace) -> int:
    _clear_proxy_env()
    os.environ.setdefault("ENV", "prod")
    os.environ.setdefault("SUPABASE_MODE", "prod")

    print(f"[rebound_ai_daily] pipeline started at={_ts()} dry_run={args.dry_run}", flush=True)
    try:
        for step in _steps(args):
            if step.name in set(args.skip_step or []):
                print(f"[rebound_ai_daily] skip step={step.name}", flush=True)
                continue
            _run_step(step, dry_run=args.dry_run)
    except subprocess.CalledProcessError as e:
        message = f"step failed: returncode={e.returncode} cmd={' '.join(e.cmd if isinstance(e.cmd, list) else [str(e.cmd)])}"
        print(f"[rebound_ai_daily] failed {message}", flush=True)
        _run_log("failed", error_message=message[:500], dry_run=args.dry_run)
        return int(e.returncode or 1)
    except Exception as e:
        message = f"pipeline failed: {e}"
        print(f"[rebound_ai_daily] failed {message}", flush=True)
        _run_log("failed", error_message=message[:500], dry_run=args.dry_run)
        return 1

    _run_log("completed", dry_run=args.dry_run)
    print(f"[rebound_ai_daily] pipeline completed at={_ts()}", flush=True)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the rebound AI daily cron pipeline")
    parser.add_argument("--date", default="today")
    parser.add_argument("--feature-sleep-seconds", type=float, default=0.2)
    parser.add_argument("--feature-max-retries", type=int, default=2)
    parser.add_argument("--feature-retry-wait-seconds", type=float, default=30)
    parser.add_argument("--feature-cooldown-on-429", type=float, default=120)
    parser.add_argument("--margin-lookback-days", type=int, default=45)
    parser.add_argument("--margin-limit", type=int, default=500)
    parser.add_argument("--margin-sleep-seconds", type=float, default=0.25)
    parser.add_argument("--margin-retry-wait-seconds", type=float, default=30)
    parser.add_argument("--skip-step", action="append", choices=[
        "generate_feature_snapshots",
        "update_long_term_market_regime",
        "check_virtual_trades",
        "import_latest_entry_margins",
        "predict_rebound",
        "save_trade_assist_candidate_history",
    ])
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(_parse_args()))
