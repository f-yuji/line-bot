"""Upsert the H5 Primary and comparison cases into trade_case_definitions."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.h5_primary import (
    H5_ENTRY_EXECUTION_NOTE,
    H5_LEGACY_PRIMARY_CASE_KEY,
    H5_LIVE_LIMITED_CASE_KEY,
    H5_LIVE_LIMITED_RULES,
    H5_RESEARCH_CASE_KEY,
    H5_RESEARCH_RULES,
)

load_dotenv()


def _case(case_key: str, case_name: str, description: str, **overrides) -> dict:
    rules = {
        **H5_LIVE_LIMITED_RULES,
        "entry_sort": "expected_value_desc",
        "max_daily_entries": 999,
        "max_open_positions": 999,
        "max_sector_positions": 999,
        "credit_profile": "margin_range_3_30",
    }
    rules.update(overrides)
    return {
        "case_key": case_key,
        "case_name": case_name,
        "description": description,
        "rules": rules,
    }


H5_CASES = [
    _case(
        H5_RESEARCH_CASE_KEY,
        "H5 Research: AI65 / PB2 / HD3 / EST12 / Credit 3-30 / No limits",
        "Research H5 case: all qualified signals are tracked for pure strategy observation.",
        **H5_RESEARCH_RULES,
        credit_profile="margin_range_3_30",
    ),
    _case(
        H5_LIVE_LIMITED_CASE_KEY,
        "H5 Live Limited: AI65 / PB2 / HD3 / EST12 / Credit 3-30",
        "Execution candidate H5 case: top ranked signals are marked as live candidates with 2/2 limits.",
        **H5_LIVE_LIMITED_RULES,
        credit_profile="margin_range_3_30",
    ),
    _case(
        H5_LEGACY_PRIMARY_CASE_KEY,
        "H5 Primary Legacy: AI65 / PB2 / HD3 / EST12 / Credit 3-30",
        "Legacy H5 Primary kept for compatibility and comparison.",
        is_primary_h5=False,
        h5_comparison=True,
    ),
    _case(
        "h5_ai65_pb20_hd3_nostop_cm_range330",
        "H5 Compare: NOSTOP / Credit 3-30",
        "Theoretical comparison without an initial price stop; not an execution Primary.",
        initial_sl_pct=None,
        is_primary_h5=False,
        h5_comparison=True,
    ),
    _case(
        "h5_ai65_pb20_hd3_est12_cm_mr20",
        "H5 Compare: EST12 / Credit <=20",
        "Old credit-cap comparison using the new -12% emergency stop.",
        min_margin_ratio=None,
        max_margin_ratio=20,
        credit_profile="margin_le20",
        is_primary_h5=False,
        h5_comparison=True,
    ),
    _case(
        "h5_ai65_pb20_hd3_est8_cm_range330",
        "H5 Compare: EST8 / Credit 3-30",
        "Comparison for observing whether the former -8% emergency stop is too early.",
        initial_sl_pct=-0.08,
        is_primary_h5=False,
        h5_comparison=True,
    ),
    _case(
        "h5_ai60_pb20_hd3_est12_cm_range330",
        "H5 Compare: AI60 / EST12 / Credit 3-30",
        "Broader signal-count comparison; not an execution Primary.",
        min_ai_score=0.60,
        is_primary_h5=False,
        h5_comparison=True,
    ),
]


def _supabase():
    mode = (os.getenv("SUPABASE_MODE") or os.getenv("ENV") or "").strip().upper()
    url = (os.getenv(f"SUPABASE_URL_{mode}") if mode else None) or os.getenv("SUPABASE_URL")
    key = (os.getenv(f"SUPABASE_KEY_{mode}") if mode else None) or os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def run() -> None:
    sb = _supabase()
    for case in H5_CASES:
        payload = {**case, "is_enabled": True}
        sb.table("trade_case_definitions").upsert(payload, on_conflict="case_key").execute()
        print(f"upserted: {case['case_key']}")
    rows = (
        sb.table("trade_case_definitions")
        .select("case_key,case_name,is_enabled")
        .like("case_key", "h5_%")
        .order("case_key")
        .execute()
        .data
        or []
    )
    print(f"H5 cases in DB: {len(rows)}")
    for row in rows:
        print(f"  {row.get('case_key')}: enabled={row.get('is_enabled')}")
    combo_count = (
        sb.table("trade_case_definitions")
        .select("case_key", count="exact")
        .like("case_key", "combo_%")
        .limit(1)
        .execute()
        .count
    )
    print(f"existing combo cases remain: {combo_count}")
    print(f"entry note: {H5_ENTRY_EXECUTION_NOTE}")


if __name__ == "__main__":
    run()
