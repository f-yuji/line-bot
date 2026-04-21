#!/usr/bin/env python3
"""
補助金・助成金の取得、整形、通知まわり。
cron: python subsidy_bot.py
"""
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from dotenv import load_dotenv
from supabase import create_client

try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    OpenAI = None
    _OPENAI_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"
JGRANTS_API_BASE = "https://api.jgrants-portal.go.jp/exp/v1/public"
JGRANTS_PORTAL_BASE = "https://www.jgrants-portal.go.jp/subsidy"
SUBSIDY_PAGE_SIZE = 5
SUBSIDY_CACHE_LIMIT = 20


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


_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_SUPABASE_URL = _mode_env("SUPABASE_URL", _SUPABASE_MODE, required=True)
_SUPABASE_KEY = _mode_env("SUPABASE_KEY", _SUPABASE_MODE, required=True)
_OPENAI_API_KEY = _opt("OPENAI_API_KEY")
_LINE_MODE = _opt("LINE_MODE")
_LINE_TOKEN = _mode_env("LINE_CHANNEL_ACCESS_TOKEN", _LINE_MODE, required=True)

supabase = create_client(_SUPABASE_URL, _SUPABASE_KEY)
_openai = OpenAI(api_key=_OPENAI_API_KEY) if (_OPENAI_AVAILABLE and _OPENAI_API_KEY) else None

SUBSIDY_CATEGORIES = [
    "建設",
    "飲食",
    "小売",
    "製造",
    "IT",
    "省エネ",
    "農業",
    "医療",
    "事業承継",
]

CATEGORY_TO_INDUSTRY: dict[str, str] = {
    "建設": "建設業",
    "飲食": "飲食業・宿泊サービス業",
    "小売": "小売業・卸売業",
    "製造": "製造業",
    "IT": "情報通信業",
    "省エネ": "省エネ・再エネ",
    "農業": "農業・林業",
    "医療": "医療業・福祉業",
    "事業承継": "事業承継サービス業・士業",
}

_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県",
]

_PREF_NORMALIZE: dict[str, str] = {}
for pref in _PREFECTURES:
    _PREF_NORMALIZE[pref] = pref
    stripped = pref.removesuffix("都").removesuffix("道").removesuffix("府").removesuffix("県")
    _PREF_NORMALIZE[stripped] = pref

CIRCLE_NUMS = "①②③④⑤"


def normalize_prefecture(text: str) -> Optional[str]:
    text = (text or "").strip()
    return _PREF_NORMALIZE.get(text)


def _fetch_from_api(
    keyword: str,
    prefecture: Optional[str] = None,
    industry: Optional[str] = None,
) -> list[dict]:
    params: dict[str, str] = {
        "keyword": keyword,
        "sort": "created_date",
        "order": "DESC",
        "acceptance": "1",
    }
    if prefecture:
        params["target_area_search"] = prefecture
    if industry:
        params["industry"] = industry

    try:
        response = requests.get(f"{JGRANTS_API_BASE}/subsidies", params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("result", [])
        logger.warning("jGrants API error status=%s keyword=%s", response.status_code, keyword)
    except Exception as e:
        logger.error("jGrants API error keyword=%s: %s", keyword, e)
    return []


def _build_summary_fallback(item: dict) -> str:
    region = (item.get("target_area_search") or "").strip()
    institution = (item.get("institution_name") or "").strip()
    deadline = (item.get("acceptance_end_datetime") or "").strip()

    parts = []
    if region:
        parts.append(f"対象: {region}")
    if institution:
        parts.append(f"実施: {institution}")
    if deadline:
        parts.append(f"締切: {deadline[:10]}")
    parts.append("詳細はリンク先で確認して")
    return " / ".join(parts)


def _matches_category(item: dict, category: Optional[str]) -> bool:
    if not category:
        return True

    industry_hint = CATEGORY_TO_INDUSTRY.get(category, "")
    haystacks = [
        str(item.get("title") or ""),
        str(item.get("industry") or ""),
        str(item.get("target_area_search") or ""),
        str(item.get("institution_name") or ""),
    ]
    text = " ".join(haystacks)
    if industry_hint and industry_hint in text:
        return True

    tokens = [category]
    tokens.extend([tok for tok in re.split(r"[・/／,，、\s]+", industry_hint) if tok])
    return any(tok and tok in text for tok in tokens)


def _generate_summary(title: str) -> str:
    if not _openai:
        return "制度の要点はリンク先で確認して"

    try:
        response = _openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": (
                    f"補助金タイトル: {title}\n\n"
                    "この補助金・助成金がどんな制度か、1文だけ日本語で短く説明して。"
                    "曖昧な断定は避けて、業種や用途の雰囲気が伝わる表現にして。"
                ),
            }],
            max_tokens=100,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error("AI summary error: %s", e)
        return "制度の要点はリンク先で確認して"


def get_subsidy_list(
    prefecture: Optional[str],
    category: Optional[str],
    limit: int = SUBSIDY_CACHE_LIMIT,
) -> list[dict]:
    industry = CATEGORY_TO_INDUSTRY.get(category) if category else None
    items1 = _fetch_from_api("補助金", prefecture, industry)
    items2 = _fetch_from_api("助成金", prefecture, industry)

    seen: set[str] = set()
    merged: list[dict] = []
    for item in items1 + items2:
        item_id = item.get("id")
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        merged.append(item)
        if len(merged) >= limit:
            break

    if not merged:
        return []

    ids = [item["id"] for item in merged]
    summary_map: dict[str, str] = {}
    try:
        response = supabase.table("subsidy_items").select("id,summary_short").in_("id", ids).execute()
        summary_map = {row["id"]: row["summary_short"] for row in (response.data or []) if row.get("summary_short")}
    except Exception as e:
        logger.warning("summary load error: %s", e)

    result = []
    for item in merged:
        result.append({
            "id": item["id"],
            "title": item.get("title", ""),
            "region": item.get("target_area_search", ""),
            "summary": summary_map.get(item["id"]) or _build_summary_fallback(item),
            "url": f"{JGRANTS_PORTAL_BASE}/{item['id']}",
            "deadline": item.get("acceptance_end_datetime", ""),
        })
    return result


def save_last_subsidy_batch(
    user_id: str,
    items: list[dict],
    prefecture: Optional[str],
    category: Optional[str],
    next_offset: int = SUBSIDY_PAGE_SIZE,
) -> None:
    payload = {
        "user_id": user_id,
        "prefecture": prefecture or "",
        "category": category or "",
        "items": items,
        "next_offset": next_offset,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        supabase.table("last_subsidy_batch").upsert(payload, on_conflict="user_id").execute()
    except Exception as e:
        logger.error("last_subsidy_batch save error user=%s: %s", user_id, e)


def get_last_subsidy_batch(user_id: str) -> Optional[dict]:
    try:
        response = (
            supabase.table("last_subsidy_batch")
            .select("user_id,prefecture,category,items,next_offset,saved_at")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        return response.data or None
    except Exception as e:
        logger.error("last_subsidy_batch load error user=%s: %s", user_id, e)
        return None


def update_last_subsidy_batch_offset(user_id: str, next_offset: int) -> None:
    try:
        supabase.table("last_subsidy_batch").update({"next_offset": next_offset}).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("last_subsidy_batch offset update error user=%s: %s", user_id, e)


def format_subsidy_page(
    items: list[dict],
    prefecture: Optional[str],
    category: Optional[str],
    *,
    offset: int = 0,
    page_size: int = SUBSIDY_PAGE_SIZE,
) -> str:
    page_items = items[offset: offset + page_size]
    base_text = format_subsidy_list(page_items, prefecture, category)
    if not page_items:
        return base_text

    shown_to = offset + len(page_items)
    total = len(items)
    if shown_to < total:
        return (
            f"{base_text}\n\n"
            f"表示: {offset + 1}-{shown_to} / {total}件\n"
            "続きは「補助金続き」"
        )

    return (
        f"{base_text}\n\n"
        f"表示: {offset + 1}-{shown_to} / {total}件\n"
        "これで全部"
    )


def format_subsidy_list(
    items: list[dict],
    prefecture: Optional[str],
    category: Optional[str],
) -> str:
    pref_str = prefecture or "全国"
    cat_str = category or "全カテゴリ"
    header = f"【補助金・助成金 / {pref_str} / {cat_str}】"

    if not items:
        body = "今出せる案件が見つからなかった\n条件を変えてもう一回見てみて"
    else:
        lines = []
        for i, item in enumerate(items):
            num = CIRCLE_NUMS[i] if i < len(CIRCLE_NUMS) else f"{i + 1}."
            lines.append(f"{num} {item['title']}\n{item['summary']}\n{item['url']}")
        body = "\n\n".join(lines)

    footer = "\n\n------\n都道府県変更 / 業種変更 で条件を変えられる"
    return f"{header}\n\n{body}{footer}"


def save_subsidies(items: list[dict]) -> list[str]:
    if not items:
        return []

    incoming_ids = [item["id"] for item in items if item.get("id")]
    try:
        existing_res = supabase.table("subsidy_items").select("id").in_("id", incoming_ids).execute()
        existing_ids = {row["id"] for row in (existing_res.data or [])}
    except Exception as e:
        logger.error("existing subsidy id load error: %s", e)
        existing_ids = set()

    now = datetime.now(timezone.utc).isoformat()
    new_ids: list[str] = []
    records: list[dict] = []

    for item in items:
        item_id = item["id"]
        is_new = item_id not in existing_ids

        record = {
            "id": item_id,
            "title": item.get("title", ""),
            "region_prefecture": item.get("target_area_search", ""),
            "url": f"{JGRANTS_PORTAL_BASE}/{item_id}",
            "status": "open",
            "subsidy_max_limit": item.get("subsidy_max_limit"),
            "acceptance_start_at": item.get("acceptance_start_datetime"),
            "acceptance_end_at": item.get("acceptance_end_datetime"),
            "institution_name": item.get("institution_name") or "",
            "updated_at": now,
        }

        if is_new:
            record["summary_short"] = _generate_summary(item.get("title", ""))
            record["created_at"] = now
            new_ids.append(item_id)

        records.append(record)

    if records:
        try:
            supabase.table("subsidy_items").upsert(records).execute()
            logger.info("subsidy saved total=%d new=%d", len(records), len(new_ids))
        except Exception as e:
            logger.error("subsidy save error: %s", e)

    return new_ids


def _push_text(user_id: str, text: str) -> None:
    try:
        requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={"Authorization": f"Bearer {_LINE_TOKEN}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("push error user=%s: %s", user_id, e)


def _resolve_plan(user: dict, now_dt: datetime) -> str:
    if user.get("membership_status") == "active":
        return "paid"

    trial_started_at = user.get("trial_started_at")
    if trial_started_at:
        try:
            trial_dt = datetime.fromisoformat(str(trial_started_at).replace("Z", "+00:00"))
            if now_dt <= trial_dt + timedelta(days=7):
                return "paid"
        except Exception:
            pass

    extended_until = user.get("trial_extended_until")
    if extended_until:
        try:
            ext_dt = datetime.fromisoformat(str(extended_until).replace("Z", "+00:00"))
            if now_dt <= ext_dt:
                return "paid"
        except Exception:
            pass

    return "free"


def send_subsidy_alerts(new_items: list[dict]) -> None:
    if not new_items:
        return

    try:
        users_res = supabase.table("users").select(
            "user_id,plan,trial_started_at,trial_extended_until,membership_status,active,subsidy_prefecture,subsidy_category"
        ).execute()
        users = users_res.data or []
    except Exception as e:
        logger.error("user load error: %s", e)
        return

    now_utc = datetime.now(timezone.utc)
    sent = 0

    for item in new_items:
        title = item.get("title", "")
        summary = _build_summary_fallback(item)
        url = f"{JGRANTS_PORTAL_BASE}/{item['id']}"
        region = str(item.get("target_area_search") or "")

        msg = f"補助金の新着\n\n{title}\n\n{summary}\n\n{url}"

        for user in users:
            if not user.get("active", True):
                continue
            if _resolve_plan(user, now_utc) != "paid":
                continue

            pref = user.get("subsidy_prefecture")
            if pref and pref not in region and "全国" not in region:
                continue

            category = user.get("subsidy_category")
            if not _matches_category(item, category):
                continue

            _push_text(user["user_id"], msg)
            sent += 1

    logger.info("subsidy alerts sent=%d", sent)


def run_subsidy_fetch() -> None:
    logger.info("=== subsidy fetch start ===")

    items1 = _fetch_from_api("補助金")
    items2 = _fetch_from_api("助成金")
    merged = {item["id"]: item for item in items1 + items2 if item.get("id")}
    logger.info("jGrants fetched total=%d subsidy=%d grant=%d", len(merged), len(items1), len(items2))

    if not merged:
        logger.warning("subsidy fetch returned no items")
        return

    merged_items = list(merged.values())
    new_ids = save_subsidies(merged_items)
    logger.info("new subsidy items=%d", len(new_ids))

    if new_ids:
        new_items = [item for item in merged_items if item["id"] in set(new_ids)]
        send_subsidy_alerts(new_items)

    logger.info("=== subsidy fetch done ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_subsidy_fetch()
