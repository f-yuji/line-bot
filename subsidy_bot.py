#!/usr/bin/env python3
"""
補助金・助成金 取得・表示・通知モジュール
cron: python subsidy_bot.py  (毎日1回)
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests as _requests
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


# ─── 環境変数 ───
def _opt(name: str) -> str:
    return os.getenv(name, "").strip()

def _mode_env(base: str, mode: str, *, required: bool = False) -> str:
    mode_upper = (mode or "").strip().upper()
    for cand in ([f"{base}_{mode_upper}"] if mode_upper else []) + [base]:
        v = _opt(cand)
        if v:
            return v
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


# ─── 定数・マッピング ───
SUBSIDY_CATEGORIES = ["建設", "飲食", "小売", "製造", "IT", "医療福祉", "農業", "運送", "生活総合"]

CATEGORY_TO_INDUSTRY: dict[str, str] = {
    "建設":   "建設業",
    "飲食":   "宿泊業、飲食サービス業",
    "小売":   "卸売業、小売業",
    "製造":   "製造業",
    "IT":     "情報通信業",
    "医療福祉": "医療、福祉",
    "農業":   "農業、林業",
    "運送":   "運輸業、郵便業",
    "生活総合": "生活関連サービス業、娯楽業",
}

_PREF_NORMALIZE: dict[str, str] = {
    "北海道": "北海道",
    "青森": "青森県", "岩手": "岩手県", "宮城": "宮城県", "秋田": "秋田県",
    "山形": "山形県", "福島": "福島県", "茨城": "茨城県", "栃木": "栃木県",
    "群馬": "群馬県", "埼玉": "埼玉県", "千葉": "千葉県",
    "東京": "東京都", "神奈川": "神奈川県", "新潟": "新潟県", "富山": "富山県",
    "石川": "石川県", "福井": "福井県", "山梨": "山梨県", "長野": "長野県",
    "岐阜": "岐阜県", "静岡": "静岡県", "愛知": "愛知県", "三重": "三重県",
    "滋賀": "滋賀県", "京都": "京都府", "大阪": "大阪府", "兵庫": "兵庫県",
    "奈良": "奈良県", "和歌山": "和歌山県", "鳥取": "鳥取県", "島根": "島根県",
    "岡山": "岡山県", "広島": "広島県", "山口": "山口県", "徳島": "徳島県",
    "香川": "香川県", "愛媛": "愛媛県", "高知": "高知県", "福岡": "福岡県",
    "佐賀": "佐賀県", "長崎": "長崎県", "熊本": "熊本県", "大分": "大分県",
    "宮崎": "宮崎県", "鹿児島": "鹿児島県", "沖縄": "沖縄県",
}
# フル名（都・道・府・県付き）もそのまま通す
for _k, _v in list(_PREF_NORMALIZE.items()):
    _PREF_NORMALIZE[_v] = _v

CIRCLE_NUMS = "①②③④⑤"


def normalize_prefecture(text: str) -> Optional[str]:
    """入力テキストを正規化された都道府県名に変換。マッチしなければNone"""
    text = text.strip()
    if text in _PREF_NORMALIZE:
        return _PREF_NORMALIZE[text]
    for suffix in ["都", "道", "府", "県"]:
        stripped = text.replace(suffix, "")
        if stripped in _PREF_NORMALIZE:
            return _PREF_NORMALIZE[stripped]
    return None


# ─── jGrants API ───
def _fetch_from_api(
    keyword: str,
    prefecture: Optional[str] = None,
    industry: Optional[str] = None,
) -> list[dict]:
    params: dict = {
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
        r = _requests.get(f"{JGRANTS_API_BASE}/subsidies", params=params, timeout=15)
        if r.status_code == 200:
            return r.json().get("result", [])
        logger.warning("jGrants API %s keyword=%s", r.status_code, keyword)
    except Exception as e:
        logger.error("jGrants API取得エラー keyword=%s: %s", keyword, e)
    return []


def get_subsidy_list(
    prefecture: Optional[str],
    category: Optional[str],
    limit: int = 5,
) -> list[dict]:
    """API検索 + DBのAI要約を合わせて返す"""
    industry = CATEGORY_TO_INDUSTRY.get(category) if category else None

    # 「補助金」「助成金」の2キーワードで取得してマージ
    items1 = _fetch_from_api("補助金", prefecture, industry)
    items2 = _fetch_from_api("助成金", prefecture, industry)
    seen: set[str] = set()
    merged: list[dict] = []
    for item in items1 + items2:
        if item["id"] not in seen:
            seen.add(item["id"])
            merged.append(item)
        if len(merged) >= limit:
            break

    if not merged:
        return []

    # DBからAI要約を補完
    ids = [i["id"] for i in merged]
    summary_map: dict[str, str] = {}
    try:
        res = supabase.table("subsidy_items").select("id,summary_short").in_("id", ids).execute()
        summary_map = {r["id"]: r["summary_short"] for r in (res.data or []) if r.get("summary_short")}
    except Exception as e:
        logger.warning("要約取得エラー: %s", e)

    result = []
    for item in merged:
        result.append({
            "id": item["id"],
            "title": item.get("title", ""),
            "region": item.get("target_area_search", ""),
            "summary": summary_map.get(item["id"], "詳細条件・申請方法は公式サイトで確認してください。"),
            "url": f"{JGRANTS_PORTAL_BASE}/{item['id']}",
            "deadline": item.get("acceptance_end_datetime", ""),
        })
    return result


def format_subsidy_list(
    items: list[dict],
    prefecture: Optional[str],
    category: Optional[str],
) -> str:
    pref_str = prefecture or "全国"
    cat_str = category or "全カテゴリ"
    header = f"【補助金・助成金 / {pref_str} / {cat_str}】"

    if not items:
        body = "現在受付中の案件が見つかりませんでした。\n条件を変更して再検索してください。"
    else:
        lines = []
        for i, item in enumerate(items):
            num = CIRCLE_NUMS[i] if i < len(CIRCLE_NUMS) else f"{i+1}."
            lines.append(f"{num} {item['title']}\n{item['summary']}\n{item['url']}")
        body = "\n\n".join(lines)

    footer = "\n\n─────\n条件変更：\n「都道府県変更」または「業種変更」と送ってください"
    return f"{header}\n\n{body}{footer}"


# ─── AI要約（cron専用）───
def _generate_summary(title: str) -> str:
    if not _openai:
        return "詳細条件・申請方法は公式サイトで確認してください。"
    try:
        resp = _openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": (
                    f"補助金名: {title}\n\n"
                    "この補助金・助成金について1〜2文の短い説明を生成してください。\n"
                    "断定しすぎない。数字は名称に含まれる場合のみ使用。"
                    "最後は「詳細条件は公式確認。」で締める。"
                )
            }],
            max_tokens=100,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.error("AI要約エラー: %s", e)
        return "詳細条件・申請方法は公式サイトで確認してください。"


# ─── DB保存 ───
def save_subsidies(items: list[dict]) -> list[str]:
    """upsertして新規追加されたIDリストを返す"""
    if not items:
        return []

    try:
        existing_res = supabase.table("subsidy_items").select("id").execute()
        existing_ids = {r["id"] for r in (existing_res.data or [])}
    except Exception as e:
        logger.error("既存ID取得エラー: %s", e)
        existing_ids = set()

    now = datetime.now(timezone.utc).isoformat()
    new_ids: list[str] = []
    records: list[dict] = []

    for item in items:
        item_id = item["id"]
        is_new = item_id not in existing_ids

        record: dict = {
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
            logger.info("補助金保存: %d件（新規%d件）", len(records), len(new_ids))
        except Exception as e:
            logger.error("補助金保存エラー: %s", e)

    return new_ids


# ─── 速報通知 ───
def _push_text(user_id: str, text: str) -> None:
    try:
        _requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={"Authorization": f"Bearer {_LINE_TOKEN}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("push失敗 user=%s: %s", user_id, e)


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


def send_subsidy_alerts(new_ids: list[str]) -> None:
    """新規補助金を条件一致の有料ユーザーへ通知"""
    if not new_ids:
        return

    try:
        new_res = supabase.table("subsidy_items").select("*").in_("id", new_ids).execute()
        new_items = new_res.data or []
    except Exception as e:
        logger.error("新規補助金取得エラー: %s", e)
        return

    try:
        users_res = supabase.table("users").select(
            "user_id,plan,trial_started_at,trial_extended_until,membership_status,active,subsidy_prefecture"
        ).execute()
        users = users_res.data or []
    except Exception as e:
        logger.error("ユーザー取得エラー: %s", e)
        return

    now_utc = datetime.now(timezone.utc)
    sent = 0

    for item in new_items:
        title = item.get("title", "")
        summary = item.get("summary_short", "")
        url = item.get("url", "")
        region = item.get("region_prefecture", "")

        msg = f"補助金速報\n\n{title}\n\n{summary}\n\n{url}"

        for u in users:
            if not u.get("active", True):
                continue
            if _resolve_plan(u, now_utc) != "paid":
                continue
            pref = u.get("subsidy_prefecture")
            if pref and pref not in region and "全国" not in region:
                continue
            _push_text(u["user_id"], msg)
            sent += 1

    logger.info("補助金速報: %d件通知", sent)


# ─── cron エントリポイント ───
def run_subsidy_fetch() -> None:
    logger.info("=== 補助金取得開始 ===")

    items1 = _fetch_from_api("補助金")
    items2 = _fetch_from_api("助成金")
    merged = {i["id"]: i for i in items1 + items2}
    logger.info("jGrants取得: %d件（補助金%d + 助成金%d）", len(merged), len(items1), len(items2))

    if not merged:
        logger.warning("補助金データ取得失敗")
        return

    new_ids = save_subsidies(list(merged.values()))
    logger.info("新規補助金: %d件", len(new_ids))

    if new_ids:
        send_subsidy_alerts(new_ids)

    logger.info("=== 補助金取得完了 ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_subsidy_fetch()
