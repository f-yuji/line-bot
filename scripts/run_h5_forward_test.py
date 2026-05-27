"""H5 forward-test: Primary と比較4ケースを train/test に分けて比較する。

Usage:
    python scripts/run_h5_forward_test.py
    python scripts/run_h5_forward_test.py --period test   # 2025-01-01以降のみ
    python scripts/run_h5_forward_test.py --period train  # ~2024-12-31のみ
    python scripts/run_h5_forward_test.py --period all    # 全期間
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()
from services.h5_primary import H5_LIVE_LIMITED_CASE_KEY, H5_RESEARCH_CASE_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

H5_CASES = [
    H5_LIVE_LIMITED_CASE_KEY,                 # Primary: live candidate view
    H5_RESEARCH_CASE_KEY,                     # Research: no H5 position limits
    "h5_ai65_pb20_hd3_est12_cm_range330",    # Legacy compatibility
    "h5_ai65_pb20_hd3_nostop_cm_range330",   # Compare 1: no initial stop
    "h5_ai65_pb20_hd3_est12_cm_mr20",        # Compare 2: old credit cap
    "h5_ai65_pb20_hd3_est8_cm_range330",     # Compare 3: earlier stop
    "h5_ai60_pb20_hd3_est12_cm_range330",    # Compare 4: broader AI gate
]

PERIODS = {
    "train": (date(2023, 1, 1),  date(2024, 12, 31)),
    "test":  (date(2025, 1, 1),  date(2026, 5, 28)),
    "all":   (date(2023, 1, 1),  date(2026, 5, 28)),
}


def _fmt(v, fmt=".2f") -> str:
    if v is None:
        return "  -  "
    try:
        return format(float(v), fmt)
    except (TypeError, ValueError):
        return str(v)


def _print_result(period_name: str, start: date, end: date, results: dict[str, dict]) -> None:
    print(f"\n{'='*72}")
    print(f"  Period: {period_name.upper()}  ({start} ~ {end})")
    print(f"{'='*72}")
    header = f"{'case_key':<58} {'n':>4} {'WR%':>6} {'EV%':>6} {'Tot%':>7} {'mxDD':>6} {'HD':>5} {'SL':>4}"
    print(header)
    print("-" * 92)
    primary_key = H5_LIVE_LIMITED_CASE_KEY
    for ck in H5_CASES:
        r = results.get(ck, {})
        n   = r.get("entry_count", 0)
        wr  = _fmt(r.get("win_rate"), ".1f") if r.get("win_rate") is not None else "  -  "
        ev  = _fmt(r.get("expected_value_pct"))
        tot = _fmt(r.get("total_profit_pct"))
        dd  = _fmt(r.get("max_drawdown_pct"))
        hd  = _fmt(r.get("avg_holding_days"), ".1f")
        sl  = r.get("sl_count", 0)
        marker = " <-- Primary" if ck == primary_key else ""
        print(f"{ck:<58} {n:>4} {wr:>6} {ev:>6} {tot:>7} {dd:>6} {hd:>5} {sl:>4}{marker}")
    print()
    # exit_reason breakdown
    print("  exit breakdown (primary):")
    prim = results.get(primary_key, {})
    n = prim.get("entry_count", 0) or 1
    tp = prim.get("tp_count", 0)
    sl = prim.get("sl_count", 0)
    to = prim.get("timeout_count", 0)
    op = prim.get("open_count", 0)
    pb = n - tp - sl - to - op
    print(f"    peak_pullback={pb}  timeout={to}  sl={sl}  open={op}  (n={prim.get('entry_count',0)})")


def run(args: argparse.Namespace) -> None:
    from services.trade_case_tester import run_trade_case_test_readonly

    periods_to_run = (
        {args.period: PERIODS[args.period]}
        if args.period != "both"
        else {"train": PERIODS["train"], "test": PERIODS["test"]}
    )

    for period_name, (start, end) in periods_to_run.items():
        logger.info("[h5_forward] running period=%s %s..%s cases=%s", period_name, start, end, H5_CASES)
        try:
            _cases, _sims, results = run_trade_case_test_readonly(
                start, end, case_keys=H5_CASES
            )
        except Exception as e:
            logger.error("[h5_forward] failed: %s", e)
            import traceback; traceback.print_exc()
            continue

        _print_result(period_name, start, end, results)

    print("\nDone.")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--period",
        choices=["train", "test", "all", "both"],
        default="both",
        help="train=~2024, test=2025~, all=全期間, both=train+test（デフォルト）",
    )
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
