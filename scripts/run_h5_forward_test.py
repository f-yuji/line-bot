"""H5 forward-test: Primary と比較4ケースを train/test に分けて比較する。

NOTE: このスクリプトは active_model を使ったシミュレーションであり point-in-time ではない。
Live購入候補の参照元としては使わない。
Live候補は scripts/run_h5_stored_forward_test.py (score_source=stored_predictions) を使用する。

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
from services.h5_primary import (
    H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_ALLOW_RESEARCH_CASE_KEY,
    H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_BAN_RESEARCH_CASE_KEY,
    H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,
    H5_EXTENSION_D3RET_M1_RESEARCH_CASE_KEY,
    H5_LIVE_LIMITED_CASE_KEY,
    H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,
    H5_OLD_PB20_RESEARCH_CASE_KEY,
    H5_RESEARCH_CASE_KEY,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

H5_CASES = [
    H5_LIVE_LIMITED_CASE_KEY,                 # Primary: HD3+EST12, live candidate view
    H5_RESEARCH_CASE_KEY,                     # Primary: HD3+EST12, no position limits
    H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY,  # Research compare: day3 <= -1% extends to HD5
    H5_EXTENSION_D3RET_M1_RESEARCH_CASE_KEY,      # Research compare: no position limits
    H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY,       # Research compare: extension with deep upper-shadow RSI ban
    H5_EXTENSION_BAN_RESEARCH_CASE_KEY,           # Research compare: ban rule, no position limits
    H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY,     # Research compare: extension allow rule from rule-search
    H5_EXTENSION_ALLOW_RESEARCH_CASE_KEY,         # Research compare: allow rule, no position limits
    H5_OLD_PB20_LIVE_LIMITED_CASE_KEY,        # Compare: old PB20 live limited
    H5_OLD_PB20_RESEARCH_CASE_KEY,            # Compare: old PB20 research
    "h5_ai65_pb20_hd3_est12_cm_range330",    # Legacy compatibility
    "h5_ai65_pb20_hd3_nostop_cm_range330",   # Compare: no initial stop
    "h5_ai65_pb20_hd3_est12_cm_mr20",        # Compare: old credit cap
    "h5_ai65_pb20_hd3_est8_cm_range330",     # Compare: earlier stop
    "h5_ai60_pb20_hd3_est12_cm_range330",    # Compare: broader AI gate
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


def _sim_count(sims: list[dict], key: str, value: str) -> int:
    return len([s for s in sims if str(s.get(key) or "") == value])


def _print_result(period_name: str, start: date, end: date, results: dict[str, dict], sims_by_case: dict[str, list[dict]]) -> None:
    print(f"\n{'='*72}")
    print(f"  Period: {period_name.upper()}  ({start} ~ {end})")
    print(f"{'='*72}")
    header = f"{'case_key':<62} {'n':>4} {'WR%':>6} {'EV%':>6} {'Tot%':>7} {'mxDD':>6} {'HD':>5} {'SL':>4} {'CAND':>4} {'EXT':>4} {'BAN':>4} {'ALLOW':>5}"
    print(header)
    print("-" * 104)
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
        sims = sims_by_case.get(ck, [])
        ext_candidate = len([
            s for s in sims
            if (
                str(s.get("exit_indicator") or "").startswith("extension_")
                or str(s.get("exit_indicator") or "") == "extension_banned_deep_upper_rsi"
            )
        ])
        ext = len([
            s for s in sims
            if str(s.get("exit_indicator") or "") in {
                "extension_time_stop",
                "extension_initial_sl",
                "extension_allowed_time_stop",
                "extension_allowed_initial_sl",
            }
        ])
        banned = len([s for s in sims if str(s.get("exit_indicator") or "") == "extension_banned_deep_upper_rsi"])
        allowed = len([s for s in sims if str(s.get("exit_indicator") or "") in {"extension_allowed_time_stop", "extension_allowed_initial_sl"}])
        marker = " <-- Primary" if ck == primary_key else ""
        print(f"{ck:<62} {n:>4} {wr:>6} {ev:>6} {tot:>7} {dd:>6} {hd:>5} {sl:>4} {ext_candidate:>4} {ext:>4} {banned:>4} {allowed:>5}{marker}")
    print()
    # exit_reason breakdown
    print("  exit breakdown (primary):")
    prim = results.get(primary_key, {})
    sl = prim.get("sl_count", 0)
    to = prim.get("timeout_count", 0)
    op = prim.get("open_count", 0)
    print(f"    time_stop={to}  emergency_stop={sl}  open={op}  peak_pullback=0  (n={prim.get('entry_count',0)})")
    ext_key = H5_EXTENSION_D3RET_M1_LIVE_LIMITED_CASE_KEY
    ext_sims = sims_by_case.get(ext_key, [])
    if ext_sims:
        ext_enabled = len([s for s in ext_sims if str(s.get("exit_indicator") or "") in {"extension_time_stop", "extension_initial_sl"}])
        ext_time = _sim_count(ext_sims, "exit_reason", "extension_time_stop")
        ext_sl = len([s for s in ext_sims if s.get("exit_reason") == "sl" and str(s.get("exit_indicator") or "").startswith("extension_")])
        day3_values = [
            float(s.get("exit_signal_value"))
            for s in ext_sims
            if str(s.get("exit_indicator") or "").startswith("extension_") and s.get("exit_signal_value") is not None
        ]
        avg_day3 = sum(day3_values) / len(day3_values) if day3_values else None
        print("  exit breakdown (extension compare):")
        print(
            f"    extension_enabled={ext_enabled}  extension_time_stop={ext_time}  "
            f"extension_emergency_stop={ext_sl}  peak_pullback=0  "
            f"threshold=day3_return<=-1.0  avg_day3_ext={_fmt(avg_day3)}"
        )
    ban_key = H5_EXTENSION_BAN_LIVE_LIMITED_CASE_KEY
    ban_sims = sims_by_case.get(ban_key, [])
    if ban_sims:
        ban_enabled = len([s for s in ban_sims if str(s.get("exit_indicator") or "") in {"extension_time_stop", "extension_initial_sl"}])
        ban_banned = len([s for s in ban_sims if str(s.get("exit_indicator") or "") == "extension_banned_deep_upper_rsi"])
        ban_candidates = ban_enabled + ban_banned
        ban_time = _sim_count(ban_sims, "exit_reason", "extension_time_stop")
        ban_sl = len([s for s in ban_sims if s.get("exit_reason") == "sl" and str(s.get("exit_indicator") or "") == "extension_initial_sl"])
        ban_values = [
            float(s.get("exit_signal_value"))
            for s in ban_sims
            if str(s.get("exit_indicator") or "") in {"extension_time_stop", "extension_initial_sl"} and s.get("exit_signal_value") is not None
        ]
        banned_values = [
            float(s.get("exit_signal_value"))
            for s in ban_sims
            if str(s.get("exit_indicator") or "") == "extension_banned_deep_upper_rsi" and s.get("exit_signal_value") is not None
        ]
        print("  exit breakdown (extension ban compare):")
        print(
            f"    extension_candidate={ban_candidates}  extension_enabled={ban_enabled}  "
            f"extension_banned={ban_banned}  extension_ban_rate={_fmt((ban_banned / ban_candidates * 100) if ban_candidates else None, '.1f')}%  "
            f"extension_time_stop={ban_time}  extension_emergency_stop={ban_sl}  "
            f"avg_day3_extended={_fmt(sum(ban_values) / len(ban_values) if ban_values else None)}  "
            f"avg_day3_banned={_fmt(sum(banned_values) / len(banned_values) if banned_values else None)}  "
            f"proxy_open_rate=100.0%  proxy_rsi_rate=100.0%"
        )
    allow_key = H5_EXTENSION_ALLOW_LIVE_LIMITED_CASE_KEY
    allow_sims = sims_by_case.get(allow_key, [])
    if allow_sims:
        allowed = [
            s for s in allow_sims
            if str(s.get("exit_indicator") or "") in {"extension_allowed_time_stop", "extension_allowed_initial_sl"}
        ]
        rejected = [
            s for s in allow_sims
            if str(s.get("exit_indicator") or "").startswith("extension_rejected:")
        ]
        reject_day1 = len([s for s in rejected if "day1_weak" in str(s.get("exit_indicator") or "")])
        reject_body = len([s for s in rejected if "day3_body_large" in str(s.get("exit_indicator") or "")])
        reject_volume = len([s for s in rejected if "day3_volume_hot" in str(s.get("exit_indicator") or "")])
        allow_time = _sim_count(allow_sims, "exit_indicator", "extension_allowed_time_stop")
        allow_sl = _sim_count(allow_sims, "exit_indicator", "extension_allowed_initial_sl")
        values = [
            float(s.get("exit_signal_value"))
            for s in allowed
            if s.get("exit_signal_value") is not None
        ]
        # Proxy rate: among trades that reached the allow/reject decision (extension candidates)
        ext_cand_sims = [s for s in allow_sims if s.get("extension_candidate")]
        n_cand = len(ext_cand_sims)
        proxy_open = sum(1 for s in ext_cand_sims if s.get("day3_open_is_proxy"))
        proxy_vol = sum(1 for s in ext_cand_sims if s.get("day3_volume_ratio_is_proxy"))
        proxy_open_rate = proxy_open / n_cand * 100 if n_cand else 0.0
        proxy_vol_rate = proxy_vol / n_cand * 100 if n_cand else 0.0
        source_counts: dict[str, int] = {}
        for s in ext_cand_sims:
            src = str(s.get("day3_feature_source") or "proxy")
            source_counts[src] = source_counts.get(src, 0) + 1
        print("  exit breakdown (extension allow compare):")
        print(
            f"    extension_allowed={len(allowed)}  extension_rejected={len(rejected)}  "
            f"reject_day1_weak={reject_day1}  reject_body_large={reject_body}  reject_volume_hot={reject_volume}  "
            f"extension_time_stop={allow_time}  extension_emergency_stop={allow_sl}  "
            f"avg_day3_allowed={_fmt(sum(values) / len(values) if values else None)}"
        )
        print(
            f"    [proxy] open={proxy_open_rate:.1f}%  volume_ratio={proxy_vol_rate:.1f}%  "
            f"(n_candidates={n_cand})  sources={source_counts}"
        )


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
            _cases, sims_by_case, results = run_trade_case_test_readonly(
                start, end, case_keys=H5_CASES
            )
        except Exception as e:
            logger.error("[h5_forward] failed: %s", e)
            import traceback; traceback.print_exc()
            continue

        _print_result(period_name, start, end, results, sims_by_case)

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
