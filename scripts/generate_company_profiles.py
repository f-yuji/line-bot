#!/usr/bin/env python3
"""prime_stocks_cache の全銘柄について OpenAI で事業概要を生成し
nikkei_company_profiles に保存する。

Usage:
    python scripts/generate_company_profiles.py
    python scripts/generate_company_profiles.py --dry-run
    python scripts/generate_company_profiles.py --batch-size 20
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _build_supabase():
    from supabase import create_client

    def _opt(n):
        return os.getenv(n, "").strip()

    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mu = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mu}") if mu else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mu}") if mu else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY が未設定")
    return create_client(url, key)


def _fetch_all_prime_stocks(sb) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    page = 1000
    while True:
        data = (
            sb.table("prime_stocks_cache")
            .select("code,name,sector")
            .range(offset, offset + page - 1)
            .execute()
            .data or []
        )
        rows.extend(data)
        if len(data) < page:
            break
        offset += page
    return rows


def _fetch_existing_codes(sb) -> set[str]:
    rows: list[dict] = []
    offset = 0
    page = 1000
    while True:
        data = (
            sb.table("nikkei_company_profiles")
            .select("code")
            .not_.is_("business_summary", "null")
            .neq("business_summary", "")
            .range(offset, offset + page - 1)
            .execute()
            .data or []
        )
        rows.extend(data)
        if len(data) < page:
            break
        offset += page
    return {str(r["code"]) for r in rows}


def _generate_summaries(batch: list[dict], client) -> dict[str, str]:
    lines = "\n".join(f"{r['code']}: {r['name']}" for r in batch)
    prompt = (
        "以下の日本株について、それぞれの事業概要を1〜2文（70字以内）で簡潔に説明してください。\n"
        "JSONのみ返し、証券コードをキー・概要テキストを値にしてください。\n\n"
        + lines
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content or "{}"
    return json.loads(content)


def run(args: argparse.Namespace) -> None:
    from openai import OpenAI

    sb = _build_supabase()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    logger.info("prime_stocks_cache 取得中...")
    all_stocks = _fetch_all_prime_stocks(sb)
    logger.info("全銘柄数: %d", len(all_stocks))

    existing = _fetch_existing_codes(sb)
    logger.info("既存登録済み: %d件 → スキップ", len(existing))

    targets = [r for r in all_stocks if str(r["code"]) not in existing]
    logger.info("生成対象: %d件", len(targets))

    if not targets:
        logger.info("全件登録済みです。終了。")
        return

    batch_size = args.batch_size
    total_batches = (len(targets) + batch_size - 1) // batch_size
    saved = 0
    errors = 0

    for i in range(0, len(targets), batch_size):
        batch = targets[i: i + batch_size]
        batch_num = i // batch_size + 1
        logger.info("[%d/%d] %s...", batch_num, total_batches, ", ".join(r["code"] for r in batch))

        if args.dry_run:
            logger.info("  DRY-RUN: スキップ")
            continue

        try:
            summaries = _generate_summaries(batch, client)
        except Exception as e:
            logger.warning("  API失敗: %s", e)
            errors += 1
            time.sleep(2)
            continue

        rows = []
        for r in batch:
            code = str(r["code"])
            summary = str(summaries.get(code) or summaries.get(str(int(code))) or "").strip()
            if not summary:
                logger.warning("  概要なし: %s %s", code, r.get("name"))
            rows.append({
                "code": code,
                "name": r.get("name") or "",
                "sector": r.get("sector") or "",
                "business_summary": summary,
            })

        try:
            sb.table("nikkei_company_profiles").upsert(rows, on_conflict="code").execute()
            saved += len(rows)
        except Exception as e:
            logger.warning("  DB保存失敗: %s", e)
            errors += 1

        time.sleep(args.sleep)

    logger.info("完了: saved=%d errors=%d", saved, errors)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="会社概要一括生成")
    p.add_argument("--dry-run", action="store_true", help="API呼び出し・DB保存なし")
    p.add_argument("--batch-size", type=int, default=10, help="1回のAPI呼び出しで処理する件数（デフォルト:10）")
    p.add_argument("--sleep", type=float, default=0.5, help="バッチ間のスリープ秒数（デフォルト:0.5）")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
