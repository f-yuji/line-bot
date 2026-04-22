#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _mode_env(base: str, mode: str, *, required: bool = False) -> str:
    mode_upper = (mode or "").strip().upper()
    for cand in ([f"{base}_{mode_upper}"] if mode_upper else []) + [base]:
        value = _opt(cand)
        if value:
            return value
    if required:
        raise KeyError(base)
    return ""


def main() -> int:
    load_dotenv()

    if len(sys.argv) < 2:
        print("usage: python import_nikkei_company_profiles.py <json_path>")
        return 1

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"file not found: {json_path}")
        return 1

    supabase_mode = _opt("SUPABASE_MODE") or _opt("ENV")
    supabase_url = _mode_env("SUPABASE_URL", supabase_mode, required=True)
    supabase_key = _mode_env("SUPABASE_KEY", supabase_mode, required=True)
    supabase = create_client(supabase_url, supabase_key)

    data = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("json must be an array")
        return 1

    rows = []
    for i, item in enumerate(data, 1):
        if not isinstance(item, dict):
            print(f"invalid row at index {i}: not an object")
            return 1

        code = str(item.get("code") or "").strip()
        name = str(item.get("name") or "").strip()
        sector = str(item.get("sector") or "").strip()
        business_summary = str(item.get("business_summary") or "").strip()

        if not code or not name:
            print(f"invalid row at index {i}: code/name required")
            return 1

        rows.append(
            {
                "code": code,
                "name": name,
                "sector": sector,
                "business_summary": business_summary,
            }
        )

    supabase.table("nikkei_company_profiles").upsert(rows, on_conflict="code").execute()
    print(f"upserted {len(rows)} rows into nikkei_company_profiles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
