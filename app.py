import functools
import json
import logging
import os
import re
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, abort, render_template, redirect, url_for, session, flash
from openai import OpenAI
from supabase import create_client
from services.signal_stage import SIGNAL_STAGES, STAGE_RANK, evaluate_signal_stage

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    QuickReply,
    QuickReplyItem,
    MessageAction,
    PostbackAction,
    FlexMessage,
    FlexBubble,
    FlexBox,
    FlexButton,
    FlexText,
)
from linebot.v3.webhooks import FollowEvent, MessageEvent, PostbackEvent, TextMessageContent

import settings_loader as _settings_loader
from send_news import fetch_news_for_reply, get_recent_sent_links
from nikkei_alert import (
    get_drop_list_for_reply,
    get_single_stock_change,
    format_drop_list_text,
    format_company_profile_text,
    _format_day_change_text,
    get_ai_comment as get_stock_ai_comment,
    get_nikkei_change_pct,
    NIKKEI225,
)
from market_summary import get_all_markets_reply
from subsidy_bot import (
    get_subsidy_list,
    format_subsidy_page,
    get_last_subsidy_batch,
    normalize_prefecture,
    save_last_subsidy_batch,
    SUBSIDY_CATEGORIES,
    SUBSIDY_PAGE_SIZE,
    update_last_subsidy_batch_offset,
)

# 急落株一覧コンテキスト（user_id → drop list）
_user_drop_list: dict[str, list] = {}
# 補助金 条件入力待ち状態（user_id → "await_prefecture" | "await_category"）
_user_subsidy_state: dict[str, str] = {}

# ─── 初期設定 ───
load_dotenv()


def _get_optional_env(name: str) -> str:
    return os.getenv(name, "").strip()


def _get_mode_env(base_name: str, mode: str, *, required: bool = False) -> str:
    candidates = []
    normalized_mode = (mode or "").strip().upper()
    if normalized_mode:
        candidates.append(f"{base_name}_{normalized_mode}")
    candidates.append(base_name)

    for candidate in candidates:
        value = _get_optional_env(candidate)
        if value:
            return value

    if required:
        raise KeyError(candidates[0])
    return ""

SUPABASE_MODE = _get_optional_env("SUPABASE_MODE") or _get_optional_env("ENV")
SUPABASE_URL = _get_mode_env("SUPABASE_URL", SUPABASE_MODE, required=True)
SUPABASE_KEY = _get_mode_env("SUPABASE_KEY", SUPABASE_MODE, required=True)
LINE_CHANNEL_ACCESS_TOKEN = _get_optional_env("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = _get_optional_env("LINE_CHANNEL_SECRET")
if not LINE_CHANNEL_ACCESS_TOKEN:
    raise KeyError("LINE_CHANNEL_ACCESS_TOKEN")
if not LINE_CHANNEL_SECRET:
    raise KeyError("LINE_CHANNEL_SECRET")
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ENV = os.getenv("ENV", "prod")
LINE_API_BASE = "https://api.line.me"
print("SUPABASE_URL =", SUPABASE_URL)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)
app.secret_key = _get_optional_env("SECRET_KEY") or _get_optional_env("WEB_ADMIN_TOKEN") or "changeme"
WEB_ADMIN_TOKEN = _get_optional_env("WEB_ADMIN_TOKEN")
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
JST = ZoneInfo("Asia/Tokyo")


@app.template_filter("jst")
def jst_filter(value, fmt="%Y-%m-%d %H:%M"):
    if not value:
        return ""
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST).strftime(fmt)
    except Exception:
        return str(value)


def _with_ai_priority_stage(row: dict, fallback_market_adjustment: dict | None = None) -> dict:
    copied = dict(row)
    copied["raw_signal_stage"] = row.get("signal_stage")
    try:
        bad_score = float(copied.get("bad_news_score") or 0)
    except Exception:
        bad_score = 0.0
    market_adjustment = {
        "regime": copied.get("market_regime") or (fallback_market_adjustment or {}).get("regime") or "normal",
        "label": copied.get("market_regime_label") or (fallback_market_adjustment or {}).get("label") or "通常",
        "ai_threshold_adjust": copied.get("market_threshold_adjust")
        if copied.get("market_threshold_adjust") is not None
        else (fallback_market_adjustment or {}).get("ai_threshold_adjust", 0),
        "entry_size_multiplier": 1.0,
        "reason": copied.get("market_regime_reason") or (fallback_market_adjustment or {}).get("reason") or "",
    }
    if copied.get("is_excluded") or bad_score >= 80:
        stage_result = evaluate_signal_stage(None, copied.get("signal_score") or copied.get("score"))
    else:
        stage_result = evaluate_signal_stage(
            copied.get("signal_probability"),
            copied.get("signal_score") if copied.get("signal_score") is not None else copied.get("score"),
            copied.get("expected_value"),
            _settings_loader.get_settings(),
            market_adjustment,
        )
    copied["signal_stage"] = stage_result["stage"]
    copied["stage_label"] = stage_result["stage_label"]
    copied["stage_rank"] = stage_result["stage_rank"]
    copied["stage_reason"] = stage_result["reason"]
    return copied


def _current_market_adjustment() -> dict:
    """DB保存値から market_adjustment を生成する (表示・stage計算共通ソース)."""
    from datetime import date as _date, timezone as _tz, timedelta as _td
    _JST = _tz(_td(hours=9))
    today_jst = datetime.now(_JST).date()

    result: dict = {
        "regime": "normal",
        "label": "通常",
        "ai_threshold_adjust": 0.0,
        "entry_size_multiplier": 1.0,
        "reason": "",
        "nikkei_pct": None,
        "topix_pct": None,
        "nikkei_change_yen": None,
        "updated_at": None,
        "trade_date": None,
        "trade_date_stale": False,
    }

    # nikkei/topix 実使用値は stock_drop_watchlist から
    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select(
                "market_regime,market_regime_label,market_threshold_adjust,"
                "market_regime_reason,market_nikkei_pct,market_topix_pct,"
                "market_nikkei_change_yen,updated_at"
            )
            .not_.is_("market_regime", "null")
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            ctx = rows[0]
            result.update({
                "regime": ctx.get("market_regime") or "normal",
                "label": ctx.get("market_regime_label") or "通常",
                "ai_threshold_adjust": float(ctx.get("market_threshold_adjust") or 0),
                "reason": ctx.get("market_regime_reason") or "",
                "nikkei_pct": ctx.get("market_nikkei_pct"),
                "topix_pct": ctx.get("market_topix_pct"),
                "nikkei_change_yen": ctx.get("market_nikkei_change_yen"),
                "updated_at": ctx.get("updated_at"),
            })
    except Exception as e:
        logger.warning("market context from DB failed: %s", e)

    # trade_date は market_regime テーブルから
    try:
        mr_rows = (
            supabase.table("market_regime")
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if mr_rows:
            td_str = mr_rows[0].get("trade_date")
            result["trade_date"] = td_str
            if td_str:
                try:
                    delta = (today_jst - _date.fromisoformat(str(td_str))).days
                    result["trade_date_stale"] = delta >= 2
                except Exception:
                    pass
    except Exception as e:
        logger.warning("market_regime trade_date lookup failed: %s", e)

    return result

# ─── ログ ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

print("=== 起動確認 ===")
print(f"環境: {ENV}")
print(f"SUPABASE_MODE: {SUPABASE_MODE or 'legacy'}")
if ENV == "test":
    print("◎ テスト環境で実行中")
elif ENV == "prod":
    print("！！ 本番環境で実行中（注意）")
    print("！！ 本番環境です。内容を確認してください")

# ─── ジャンル定義 ───
DISPLAY_GENRE_MAP = {
    "経済": ["economy", "interest_rates", "real_estate"],
    "仕事": ["business", "construction"],
    "国際": ["international"],
    "AI・テック": ["ai", "tech"],
    "暮らし": ["energy", "materials"],
    "海外": ["overseas"],
}

INTERNAL_TO_DISPLAY = {
    cat: display
    for display, cats in DISPLAY_GENRE_MAP.items()
    for cat in cats
}

DISPLAY_GENRE_ORDER = list(DISPLAY_GENRE_MAP.keys())

DISPLAY_GENRE_ALIASES: dict = {
    "お金": "経済",
    "世界": "国際",
    "AI": "AI・テック",
    "テック": "AI・テック",
    "IT": "AI・テック",
    "it": "AI・テック",
    "生活": "暮らし",
    "海外ニュース": "海外",
    "world": "海外",
}


# ─── DB操作 ───
def get_user(user_id: str):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def get_line_profile(user_id: str) -> str:
    """LINE APIからdisplay_nameを取得。失敗時は空文字を返す"""
    try:
        res = requests.get(
            f"{LINE_API_BASE}/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            timeout=10,
        )
        if res.status_code == 200:
            return res.json().get("displayName", "")
    except Exception as e:
        logger.error("LINEプロフィール取得失敗: user=%s %s", user_id, e)
    return ""


def save_user(user_id: str, active=True, genres=None, display_name: str = ""):
    if genres is None:
        genres = []
    supabase.table("users").upsert({
        "user_id": user_id,
        "active": active,
        "genres": genres,
        "display_name": display_name,
        "drop_alert_enabled": False,
    }).execute()
    logger.info("Supabase保存: user=%s active=%s genres=%s", user_id, active, genres)


def ensure_user(user_id: str):
    user = get_user(user_id)
    if not user:
        display_name = get_line_profile(user_id)
        save_user(user_id, active=True, genres=[], display_name=display_name)
        logger.info("新規ユーザー登録: %s display_name=%s", user_id, display_name)
        return {
            "user_id": user_id,
            "active": True,
            "genres": [],
            "subsidy_continue_pending": False,
            "drop_alert_enabled": False,
        }, True
    display_name = get_line_profile(user_id)
    if display_name:
        try:
            supabase.table("users").update({"display_name": display_name}).eq("user_id", user_id).execute()
        except Exception as e:
            logger.error("display_name更新失敗: user=%s %s", user_id, e)
    user.setdefault("last_news_question_targets", None)
    user.setdefault("last_news_question_at", None)
    user.setdefault("subsidy_continue_pending", False)
    user.setdefault("drop_alert_enabled", False)
    return user, False


def set_subsidy_continue_pending(user_id: str, user: dict, pending: bool) -> None:
    user["subsidy_continue_pending"] = pending
    try:
        supabase.table("users").update({"subsidy_continue_pending": pending}).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("subsidy_continue_pending update error user=%s pending=%s %s", user_id, pending, e)


# ─── 補助 ───
def normalize_genres(raw_text: str):
    text = raw_text.replace("\u3000", " ")
    items = [x.strip() for x in text.split(",") if x.strip()]
    result = []
    lower_map = {k.lower(): k for k in DISPLAY_GENRE_MAP}
    alias_lower = {k.lower(): v for k, v in DISPLAY_GENRE_ALIASES.items()}

    for item in items:
        key = item.lower()
        display = lower_map.get(key)
        if not display:
            display = alias_lower.get(key)

        if display and display in DISPLAY_GENRE_MAP:
            for cat in DISPLAY_GENRE_MAP[display]:
                if cat not in result:
                    result.append(cat)

    return result


def format_genres(genres):
    if not genres:
        return "なし"

    seen = []
    for cat in genres:
        d = INTERNAL_TO_DISPLAY.get(cat)
        if d and d not in seen:
            seen.append(d)
    return ", ".join(seen) if seen else "なし"


# ─── LINE UI ヘルパー ───

def _subsidy_category_quick_reply() -> QuickReply:
    items = [QuickReplyItem(action=MessageAction(label=c, text=c)) for c in SUBSIDY_CATEGORIES]
    return QuickReply(items=items)


def main_quick_reply() -> QuickReply:
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label="ニュース", text="ニュース")),
        QuickReplyItem(action=MessageAction(label="リンク", text="リンク")),
        QuickReplyItem(action=MessageAction(label="相場", text="相場")),
        QuickReplyItem(action=MessageAction(label="急落株", text="急落株")),
        QuickReplyItem(action=MessageAction(label="補助金", text="補助金")),
        QuickReplyItem(action=MessageAction(label="使い方", text="使い方")),
    ])


def reply_text(reply_token: str, text: str, quick_reply: QuickReply = None) -> None:
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            msg = TextMessage(text=text)
            if quick_reply:
                msg.quick_reply = quick_reply
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[msg])
            )
    except Exception as e:
        logger.error("LINE返信エラー: %s", e)


def reply_flex(reply_token: str, flex_msg: FlexMessage) -> None:
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[flex_msg])
            )
    except Exception as e:
        logger.error("LINE返信エラー: %s", e)


GENRE_DESC = {
    "経済": "金利・為替・不動産",
    "仕事": "業界・法改正・労働",
    "国際": "海外情勢・外交",
    "AI・テック": "AI・IT・科学",
    "暮らし": "医療・教育・生活",
    "海外": "米・英・韓・亜・印の現地ニュース",
}


def build_genre_flex(current_genres: list) -> FlexMessage:
    rows = []
    layout_rows = [
        ["経済", "仕事"],
        ["国際", "AI・テック"],
        ["暮らし", "海外"],
    ]

    for chunk in layout_rows:
        cells = []
        for display in chunk:
            internals = DISPLAY_GENRE_MAP[display]
            selected = any(c in current_genres for c in internals)

            cell = FlexBox(
                layout="vertical",
                contents=[
                    FlexButton(
                        action=PostbackAction(
                            label=f"✓{display}" if selected else display,
                            data=f"toggle_display_genre:{display}",
                            display_text=display,
                        ),
                        style="primary" if selected else "secondary",
                        height="sm",
                    ),
                    FlexText(
                        text=GENRE_DESC.get(display, ""),
                        size="xs",
                        color="#999999",
                        wrap=True,
                    ),
                ],
                spacing="xs",
                flex=1,
            )
            cells.append(cell)

        rows.append(FlexBox(layout="horizontal", contents=cells, spacing="sm"))

    header_note = f"現在: {format_genres(current_genres)}" if current_genres else "未選択の場合はジャンル指定なしで\n配信されます"

    bubble = FlexBubble(
        header=FlexBox(
            layout="vertical",
            contents=[
                FlexText(text="受け取るニュース", weight="bold", size="md"),
                FlexText(text=header_note, size="xs", color="#888888", wrap=True),
            ],
        ),
        body=FlexBox(
            layout="vertical",
            contents=rows,
            spacing="md",
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    action=PostbackAction(
                        label="決定",
                        data="confirm_genres",
                        display_text="ジャンルを決定",
                    ),
                    style="primary",
                    height="sm",
                ),
                FlexButton(
                    action=PostbackAction(
                        label="すべて解除",
                        data="clear_genres",
                        display_text="クリア",
                    ),
                    style="link",
                    height="sm",
                    color="#aaaaaa",
                ),
            ],
        ),
    )

    flex_msg = FlexMessage(alt_text="受け取るニュース", contents=bubble)
    flex_msg.quick_reply = main_quick_reply()
    return flex_msg


# ─── Q&A ───
_DETAIL_KEYWORDS = ["詳しく", "もう少し", "なんで", "なぜ", "具体的に", "仕組み"]


def is_link_request(text: str) -> bool:
    """リンク要求として自然な入力かどうか（部分一致の誤反応を防ぐ）"""
    import re
    t = (text or "").strip()
    if t in {"リンク", "URL", "url", "記事リンク", "元記事"}:
        return True
    # 番号付きリンク要求（数字や丸数字を含み、「リンク」「URL」で終わる or 「のリンク」系）
    if re.search(r"[0-9①-⑩]", t) and (
        t.endswith("リンク") or t.endswith("URL") or t.endswith("url")
        or "のリンク" in t or "のURL" in t or "のurl" in t
    ):
        return True
    # 自然な短文要求: 「リンク」「URL」「url」を含む短い発話（20文字以内）
    # 例: 「リンクちょうだい」「URLくれ」「リンク見せて」
    _LINK_WORDS = ("リンク", "URL", "url")
    if len(t) <= 20 and any(w in t for w in _LINK_WORDS):
        return True
    return False


def _strip_any_leading_number(text: str) -> str:
    """返答文頭の番号（丸数字・半角数字どちらも）を1個除去する（先頭のみ、本文中は触らない）"""
    import re
    t = (text or "").strip()
    t = re.sub(r"^\s*[①-⑩]\s*", "", t)
    t = re.sub(r"^\s*\d+[\.．]?\s*", "", t)
    return t.strip()

_CONTEXT_TOKEN_STOPWORDS = {
    "経済", "金利", "影響", "理由", "内容", "状況",
    "問題", "情報", "世界", "ニュース", "話題",
}


_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"

_CONTEXT_TTL_HOURS = 24

_BLOCKLIST = [
    "付き合", "好き",
    "お前誰", "何者", "自己紹介",
    "会話ネタ", "話のネタ", "雑談ネタ", "ネタ教えて", "何話せばいい", "何話す",
]

_REJECT_TEXT = "ニュースの内容で気になることあれば聞いて\n番号やリンクでもいけるよ"
_BLOCKLIST_TEXT = "ニュースの話で聞いてほしいな"

_QUESTION_SIGNALS = [
    "？", "?", "って", "とは", "なに", "なぜ", "なんで",
    "どう", "どこ", "いつ", "誰", "だれ", "教えて",
    "知りたい", "聞きたい", "意味", "仕組み", "違い",
]


def get_latest_news_context(user_id: str) -> Optional[dict]:
    try:
        res = (
            supabase.table("news_contexts")
            .select("payload, sent_at")
            .eq("user_id", user_id)
            .order("sent_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("ニュースコンテキスト取得失敗: %s", e)
        return None


def get_last_news_batch(user_id: str) -> Optional[list]:
    """last_news_batchから直近5件のアイテムを取得。未取得時はNone。"""
    try:
        res = (
            supabase.table("last_news_batch")
            .select("items")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        if res.data:
            return res.data.get("items") or []
    except Exception as e:
        logger.error("last_news_batch取得失敗: %s %s", user_id, e)
    return None


def _build_link_message(items: list) -> str:
    """last_news_batchのitemsから①タイトル\nURL形式のリンク一覧を生成"""
    lines = []
    # index昇順でソートして①〜⑤の対応を本文と一致させる
    sorted_items = sorted(items, key=lambda x: x.get("index", 0))
    for item in sorted_items[:5]:
        idx = item.get("index", 0)
        title = str(item.get("title", "") or "")
        if len(title) > 30:
            title = title[:29] + "…"
        link = item.get("link", "")
        num = _CIRCLED[idx - 1] if 1 <= idx <= len(_CIRCLED) else f"{idx}."
        lines.append(f"{num} {title}")
        lines.append(link)
        lines.append("")
    return "\n".join(lines).strip()


def _push_text(user_id: str, text: str) -> None:
    """LINE push APIでテキストを送信"""
    try:
        requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers=get_line_headers(),
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("push失敗: user=%s %s", user_id, e)


def is_detail_only_request(text: str) -> bool:
    t = (text or "").strip()
    return t in {"詳しく", "もっと詳しく", "深掘り", "くわしく"}


def save_last_news_question_targets(user_id: str, targets: list, ctx: dict) -> None:
    if not targets:
        return
    try:
        sent_at = ctx.get("sent_at") if ctx else None
        supabase.table("users").update({
            "last_news_question_targets": targets,
            "last_news_question_at": datetime.now(timezone.utc).isoformat(),
            "last_news_context_sent_at": sent_at,
        }).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("last_news_question_targets保存失敗: %s", e)


def get_reusable_last_news_targets(user: dict, ctx: dict) -> list:
    targets = user.get("last_news_question_targets")
    asked_at = user.get("last_news_question_at")
    saved_ctx = user.get("last_news_context_sent_at")

    if not targets or not ctx:
        return []

    if saved_ctx != ctx.get("sent_at"):
        return []

    if asked_at:
        try:
            asked_dt = datetime.fromisoformat(str(asked_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - asked_dt > timedelta(minutes=10):
                return []
        except Exception:
            return []

    return [int(x) for x in targets if isinstance(x, int)]


def clear_last_news_question_targets(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "last_news_question_targets": None,
            "last_news_question_at": None,
            "last_news_context_sent_at": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").lower())


def _normalize_query_for_match(text: str) -> str:
    t = _normalize_text(text)

    suffixes = [
        "ってなんなの", "ってなんだ", "ってなに", "って何",
        "とはなんなの", "とはなんだ", "とはなに", "とは何",
        "ってだれ", "って誰", "とはだれ", "とは誰",
        "ってどこ", "とはどこ", "って何者",
        "って", "とは", "とは？", "とは?", "？", "?", "ー", "～",
    ]

    changed = True
    while changed:
        changed = False
        for s in suffixes:
            ns = _normalize_text(s)
            if ns and t.endswith(ns):
                t = t[: -len(ns)]
                changed = True

    return t.strip()


def _resolve_drop_stock_from_text(user_id: str, text: str) -> Optional[dict]:
    query = _normalize_text(text)
    if not query:
        return None

    drops = _user_drop_list.get(user_id, [])
    if drops:
        for stock in drops:
            code = str(stock.get("code") or "")
            name = str(stock.get("name") or "")
            norm_name = _normalize_text(name)
            if query == code or query == norm_name:
                return stock
        for stock in drops:
            name = str(stock.get("name") or "")
            norm_name = _normalize_text(name)
            if norm_name and (query in norm_name or norm_name in query):
                return stock
        return None

    if re.fullmatch(r"[0-9]{4}", text) and text in NIKKEI225:
        return {"code": text, "name": NIKKEI225[text]}

    for code, name in NIKKEI225.items():
        norm_name = _normalize_text(name)
        if query == code or query == norm_name:
            return {"code": code, "name": name}
    for code, name in NIKKEI225.items():
        norm_name = _normalize_text(name)
        if norm_name and (query in norm_name or norm_name in query):
            return {"code": code, "name": name}
    return None


def _is_context_alive(ctx: dict, ttl_hours: int = _CONTEXT_TTL_HOURS) -> bool:
    if not ctx:
        return False

    sent_at = ctx.get("sent_at")
    if not sent_at:
        return False

    try:
        dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return now - dt <= timedelta(hours=ttl_hours)
    except Exception as e:
        logger.warning("sent_at判定失敗: %s", e)
        return False


def _collect_context_tokens(payload: dict) -> List[str]:
    tokens: List[str] = []

    def add_text(s: str):
        s = (s or "").strip()
        if not s:
            return

        tokens.append(s)

        parts = re.split(r"[、。・,\s/\-\[\]（）()「」『』:：\n]+", s)
        for p in parts:
            p = p.strip()
            if len(p) >= 3 and p not in _CONTEXT_TOKEN_STOPWORDS:
                tokens.append(p)

    for item in payload.get("news_items", []):
        add_text(item.get("title", ""))
        add_text(item.get("reason", ""))
        add_text(item.get("interpretation", ""))

    seen = set()
    uniq = []
    for t in tokens:
        key = _normalize_text(t)
        if key and key not in seen:
            seen.add(key)
            uniq.append(t)
    return uniq


def parse_article_numbers(text: str, max_n: int = 10) -> List[int]:
    """テキスト中の記事番号を全て抽出して昇順リストで返す。
    「135」→[1,3,5]、「①③⑤」→[1,3,5]、「1と3と5」→[1,3,5] のように処理する。

    【仕様メモ】
    max_n を超える多桁数値（例: max_n=5 のとき「135」）は、
    桁ごとに分解して記事番号の候補として扱う（135 → 1, 3, 5）。
    これは「135」を記事1・3・5の同時指定とみなすUX上の意図的な仕様。
    ロジックを変更する際はこの挙動が崩れないよう注意すること。
    """
    found: set = set()
    # 丸数字を抽出
    for i, ch in enumerate(_CIRCLED[:max_n], 1):
        if ch in text:
            found.add(i)
    # 全角数字→半角に変換
    normalized = text.translate(str.maketrans("１２３４５６７８９０", "1234567890"))
    # 区切り文字（と・、,，／/-）を空白に統一
    normalized = re.sub(r"[と・、,，／/－\-]+", " ", normalized)
    # 数字以外を空白に変換
    normalized = re.sub(r"[^0-9\s]", " ", normalized)
    for token in normalized.split():
        if not token.isdigit():
            continue
        n = int(token)
        if 1 <= n <= max_n:
            found.add(n)
        elif n > max_n:
            # 2桁以上でmax_nを超える数値は桁ごとに分解して記事番号候補として扱う
            # 例: max_n=5, "135" → [1, 3, 5]（記事1・3・5の同時指定とみなす）
            for ch in token:
                d = int(ch)
                if 1 <= d <= max_n:
                    found.add(d)
    return sorted(found)



def is_news_question(text: str) -> bool:
    keywords = [
        "何", "なに", "どういう", "どうな", "意味", "影響",
        "金利", "円安", "株", "経済",
        "AI", "ニュース", "話", "やつ", "って何", "どういうこと",
    ]
    return any(k in text for k in keywords)


def _looks_like_article_reference(text: str) -> bool:
    if parse_article_numbers(text, max_n=10):
        return True

    refs = [
        "このニュース", "そのニュース", "この話", "その話", "この件", "その件",
        "これ", "それ", "さっきの", "今の", "例の",
        "リンク", "url", "記事",
        "詳しく", "なんで", "なぜ", "理由", "影響", "どういうこと",
    ]
    return any(r in text for r in refs)


def _looks_like_question_or_command(text: str) -> bool:
    if parse_article_numbers(text, max_n=10):
        return True
    if is_link_request(text):
        return True
    if any(s in text for s in _QUESTION_SIGNALS):
        return True
    return False


def is_related_to_news_context(user_id: str, text: str) -> bool:
    ctx = get_latest_news_context(user_id)
    if not ctx or not _is_context_alive(ctx):
        return False

    payload = ctx.get("payload", {}) or {}
    norm_text = _normalize_text(text)
    match_text = _normalize_query_for_match(text)

    if not norm_text:
        return False

    if _looks_like_article_reference(text):
        return True

    if not match_text:
        return False

    for token in _collect_context_tokens(payload):
        norm_token = _normalize_text(token)
        if not norm_token:
            continue

        if norm_token in norm_text or norm_text in norm_token:
            return True

        if norm_token in match_text or match_text in norm_token:
            return True

    # コンテキストが生きていて自然文ニュース質問なら一致とみなす
    if is_news_question(text):
        return True

    return False



def parse_detail_request(text: str) -> List[int]:
    """番号系入力を深掘りリクエストとして判定し、記事番号リストを返す。
    半角数字・全角数字・丸数字に対応。
    """
    raw = (text or "").strip()
    if not raw:
        return []

    # 全角→半角変換
    normalized = raw.translate(str.maketrans("１２３４５６７８９０", "1234567890"))

    # 先頭が数字 or 丸数字でなければスルー
    if not re.match(r"^[①-⑩1-9]", normalized):
        return []

    nums = parse_article_numbers(normalized, max_n=10)

    logger.info(
        "parse_detail_request: input=%r normalized=%r → nums=%s",
        text, normalized, nums
    )
    return nums


_DETAIL_NEW_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "detail_articles",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "articles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "index": {"type": "integer"},
                            "headline": {"type": "string"},
                            "p1": {"type": "string"},
                            "p2": {"type": "string"},
                            "p3": {"type": "string"},
                            "stocks": {"type": "string"},
                        },
                        "required": ["index", "headline", "p1", "p2", "p3", "stocks"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["articles"],
            "additionalProperties": False,
        },
    },
}


def answer_detail_new(user_id: str, nums: List[int]) -> str:
    """新仕様の深掘り: 事実+背景+展開の3要素、150〜220文字程度"""
    logger.info("answer_detail_new 実行: user=%s nums=%s", user_id, nums)
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がない\n一度配信を受けてから試して"

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])
    index_map = {item.get("index", 0): item for item in news_items}

    target_items = sorted(
        [index_map[n] for n in nums if n in index_map],
        key=lambda x: x.get("index", 0),
    )
    if not target_items:
        return "指定の番号が見つからなかった"

    news_text = "\n".join(
        f"{n['index']}. 【{n.get('category', '')}】{n['title']}"
        f"（事実: {n.get('reason', '')} / 解釈: {n.get('interpretation', '')}）"
        for n in target_items
    )

    system_prompt = (
        "お前はLINEでニュースを読みやすく解説するやつ\n\n"
        "各記事を以下の構成で書け:\n"
        "headline: 内容の核心を一言で\n"
        "p1: 何が起きているか（要点＋評価）\n"
        "p2: なぜそうなっているか（背景・構造）\n"
        "p3: 今後どうなるか（予測・影響）\n\n"
        "ルール:\n"
        "・各段落2文程度\n"
        "・情報を削るな\n"
        "・言い換えだけは禁止\n"
        "・本文コピペ禁止\n"
        "・敬語禁止\n"
        "・抽象論禁止\n"
        "・具体的に書け\n"
        "・会話誘導禁止\n"
        "・予測は断定せず『〜可能性』『〜になりそう』で書く\n"
        "・全記事について必ず全フィールドを埋めろ（欠損禁止）\n"
        "・1件でも欠けたら不正とみなす\n"
        "stocks: このニュースで影響を受けそうな日本株の銘柄・セクターを具体的に列挙（証券コードがあれば添える）\n"
        "・例: 三菱UFJ(8306)↑、銀行セクター全般↑、REIT売り\n"
        "・直接関係なければ「関連銘柄なし」と書く\n"
    )
    user_prompt = (
        f"以下のニュース記事を深掘りしろ:\n{news_text}\n\n"
        f"指定番号: {nums}\n"
        "全記事について必ず headline / p1 / p2 / p3 / stocks を埋めろ。\n"
        "1件でも欠けたら失敗。\n"
        "JSONで返せ。"
    )

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=900,
            timeout=20,
            response_format=_DETAIL_NEW_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        raw_articles = data.get("articles", [])
        for a in raw_articles:
            logger.info("AI出力 index=%s keys=%s", a.get("index"), list(a.keys()))
        article_map = {a.get("index"): a for a in raw_articles}

        articles = []
        for idx in nums:
            if idx in article_map:
                articles.append(article_map[idx])

        parts = []
        for a in articles:
            idx = a.get("index", 0)
            item = index_map.get(idx)
            title = item.get("title", "") if item else ""
            headline = (a.get("headline") or "").strip()
            p1 = (a.get("p1") or "").strip()
            p2 = (a.get("p2") or "").strip()
            p3 = (a.get("p3") or "").strip()
            stocks = (a.get("stocks") or "").strip()

            if not headline:
                headline = title[:40]
            if not p1:
                p1 = f"{title}が話題になっていて、状況に影響している。単なる一時的な動きではなく流れを変える要因になっている。"
            if not p2:
                p2 = "背景としては最近の動向や周囲の評価が積み上がっていて、全体にも影響が出ている。単体の結果以上に構造的に効いている状態。"
            if not p3:
                p3 = "この流れが続けば影響はさらに広がる可能性があるが、止まれば一気にバランスが崩れるリスクもある。"

            block = (
                f"{idx}. {title}\n\n"
                f"[{headline}]\n\n"
                f"{p1}\n\n"
                f"{p2}\n\n"
                f"{p3}"
            )
            if stocks and stocks != "関連銘柄なし":
                block += f"\n\n📌 {stocks}"
            parts.append(block)

        return "\n\nーーーーー\n\n".join(parts)
    except Exception as e:
        logger.error("answer_detail_new エラー: %s", e)
        return "今ちょっと返せない\nもう一回送ってみて"


def answer_single_news_item(item: dict, question: str, is_detail: bool) -> str:
    news_text = (
        f"{item['index']}. 【{item.get('category', '')}】{item['title']}"
        f"（{item.get('reason', '')} / {item.get('interpretation', '')}）"
    )
    system_prompt = (
        "お前はLINEでニュース解説するやつ\n"
        "・敬語禁止\n"
        "・結論から\n"
        "・短く\n"
        "・会話調\n"
    )
    mode = "詳細で答えろ" if is_detail else "短く答えろ"
    user_prompt = (
        f"ニュース:\n{news_text}\n\n"
        f"質問:\n{question}\n\n"
        f"{item['index']}番の記事として答えろ。番号は変えるな。{mode}"
    )
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=220,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.error("単記事エラー: %s", e)
        return "今ちょっと返せない"


def answer_news_question(user_id: str, question: str) -> tuple:
    # 番号入力は深掘りルートで処理するので、ここには来ない想定
    if parse_detail_request(question):
        logger.info("answer_news_question: 番号入力は対象外 question=%r", question)
        return "番号指定は深掘りルートで処理する", []

    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がないから答えられないかも\n一度配信を受けてから聞いてみて", []

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])

    is_detail = any(k in question for k in _DETAIL_KEYWORDS)

    # 自然文：タイトル/reason/interpretationにキーワード一致で最大2件
    norm_q = _normalize_text(question)
    matched = []
    for item in news_items:
        fields = item.get("title", "") + item.get("reason", "") + item.get("interpretation", "")
        if norm_q and any(_normalize_text(tok) in norm_q for tok in re.split(r"[　\s、。・,/\-（）「」\n]+", fields) if len(tok) >= 2):
            matched.append(item)
        if len(matched) >= 2:
            break
    logger.info("自然文一致件数: %s", [n.get("index") for n in matched])
    target_items = matched if matched else (news_items[:1] if news_items else [])
    logger.info("自然文最終対象: %s", [n.get("index") for n in target_items])

    targets = [item["index"] for item in target_items]

    # 複数記事 → 1記事ずつGPTに投げて番号ズレを防ぐ
    if len(target_items) >= 2:
        logger.info("複数記事: %s", targets)
        results = []
        for item in target_items:
            ans = answer_single_news_item(item, question, is_detail)
            idx = item["index"]
            ans = _strip_any_leading_number(ans)
            results.append(f"{idx}. {ans}")
        return "\n\n".join(results), targets

    # 単一記事
    news_text = "\n".join(
        f"{n['index']}. 【{n.get('category', '')}】{n['title']}（{n.get('reason', '')} / {n.get('interpretation', '')}）"
        for n in target_items
    )

    system_prompt = (
        "お前はLINEでニュース解説するやつ\n\n"
        "・敬語禁止\n"
        "・結論から\n"
        "・短く（通常1〜3文）\n"
        "・詳細なら3〜5文\n"
        "・会話調\n"
        "・断定しすぎない（〜かも）\n\n"
        "ニュース文脈に沿って答えろ"
    )
    mode = "詳細モードで答えろ。" if is_detail else "通常モードで答えろ。"
    user_prompt = (
        f"ニュース:\n{news_text}\n\n"
        f"質問:\n{question}\n\n"
        f"{mode}短く答えろ。"
    )

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=400,
            timeout=15,
        )
        raw = res.choices[0].message.content.strip()
        return raw, targets
    except Exception as e:
        logger.error("Q&A OpenAI エラー: %s", e)
        return "今ちょっと返答うまくいかない\n\nもう一回送るか\n「使い方」押してみて", []


def _clear_pending(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": None,
            "pending_count": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass



# ─── Webhook ───
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        abort(400)

    body = request.get_data(as_text=True)

    # 署名検証を最初に行う（LINE SDK側で検証）
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        sig_head = (signature or "")[:10]
        logger.warning(
            "署名検証失敗 ip=%s method=%s path=%s ua=%s sig=%s",
            request.remote_addr,
            request.method,
            request.path,
            request.headers.get("User-Agent", ""),
            sig_head,
        )
        abort(400)

    # 署名検証通過後にmembershipイベントを処理
    return "OK"



@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    ensure_user(user_id)
    reply_text(
        event.reply_token,
        "追加ありがとう\n\n"
        "ニュースは朝に届く\n"
        "寡てる時間帯だからミュートでもOK\n\n"
        "配信とは別で、ボタンから追加ニュースも見れるよ\n\n"
        "まずは「使い方」見てみて",
        quick_reply=main_quick_reply(),
    )


_GENRE_WORDS = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}

def normalize_user_text(text: str) -> str:
    """表記ゆれを正規化してキーワードマッチの精度を上げる"""
    t = (text or "").strip()
    t = t.replace("出来る", "できる")
    t = t.replace("出来んの", "できんの")
    t = t.replace("できんの", "できる")
    t = t.replace("なに", "何")
    t = re.sub(r"\s+", "", t)
    return t


_STATUS_WORDS = {"状態", "今どんな感じ", "設定どうなってる", "今の設定"}
_HELP_WORDS = {"聞く", "使い方", "何できる", "どう使うの", "何聞ける"}



_NEWS_TRIGGER_KW = {"ニュース", "ニュースくれ", "最新"}

# 強コマンド — pending を問答無用でスキップ・クリアする
_MAIN_COMMANDS = {
    "ニュース",
    "ニュースくれ",
    "最新",
    "追加ニュース",
    "リンク",
    "相場",
    "急落株",
    "急落",
    "急落銘柄",
    "補助金",
    "助成金",
    "補助金続き",
    "助成金続き",
    "続き",
    "都道府県変更",
    "業種変更",
    "登録",
    "解約",
    "プラン",
    "設定",
}

_STRONG_COMMANDS = (
    _GENRE_WORDS
    | _STATUS_WORDS
    | _HELP_WORDS
    | _NEWS_TRIGGER_KW
    | _MAIN_COMMANDS
)



@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = normalize_user_text(event.message.text)

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)
    user, _ = ensure_user(user_id)

    now_dt = datetime.now(timezone.utc)

    _last = user.get("last_reply_time")
    if _last is not None:
        try:
            _last_dt = datetime.fromisoformat(str(_last).replace("Z", "+00:00"))
            if now_dt - _last_dt < timedelta(seconds=3):
                return
        except Exception:
            pass

    genres = user.get("genres", [])
    qr = main_quick_reply()

    if text in _STRONG_COMMANDS and user.get("pending_action"):
        _clear_pending(user_id)
        user["pending_action"] = None
        user["pending_count"] = None

    if text == "使い方":
        _help_text = (
            "使い方ガイド\n\n"
            "ニュース → 今日のニュースを見る\n"
            "リンク → 直近ニュースのURLを見る\n"
            "補助金 → 今使える制度を探す\n"
            "相場 → 市場の動きを見る\n"
            "急落株 → 急落録柄を見る\n"
            "状態 → 設定確認\n\n"
            "――――――\n\n"
            "[ニュース活用]\n"
            "1詳しく → 記事を深掘り\n"
            "「ｏｏってなに？」 → 用語解説\n"
            "今後どうなる？ → 追加質問OK\n\n"
            "――――――\n\n"
            "[今の状態]\n"
            f"ジャンル：{format_genres(genres) if genres else '全ジャンル'}"
        )
        try:
            with ApiClient(configuration) as api_client:
                api = MessagingApi(api_client)
                _msgs_to_send = [
                    build_genre_flex(genres),
                    TextMessage(text=_help_text),
                ]
                _msgs_to_send[-1].quick_reply = qr
                api.reply_message(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=_msgs_to_send)
                )
        except Exception as e:
            logger.error("使い方返信エラー: %s", e)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _HELP_WORDS:
        reply_text(
            event.reply_token,
            "気になるニュースそのまま聞けばOK\n"
            "「3番目なに？」とかでもいける\n"
            "リンクだけ欲しい時も返せる\n\n"
            "ジャンル変えたい時もそのまま言って",
            quick_reply=qr,
        )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "設定":
        reply_text(event.reply_token, "設定する？\n使い方見てみて", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _GENRE_WORDS:
        reply_flex(event.reply_token, build_genre_flex(genres))
        return

    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        new_genres = normalize_genres(raw)
        if not new_genres:
            reply_text(
                event.reply_token,
                "ジャンル認識できなかった\n例: ジャンル 経済,AI・テック,海外",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        supabase.table("users").update({"genres": new_genres}).eq("user_id", user_id).execute()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, f"{format_genres(new_genres)}に変更した\nニュースで確認できる", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _STATUS_WORDS:
        genre_label = f"ジャンル: {format_genres(genres)}" if genres else "ジャンル: 未設定（全部配信）"
        reply_text(event.reply_token, f"今こんな感じ\n\n{genre_label}", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if _user_subsidy_state.get(user_id) == "await_prefecture":
        _KNOWN_CMDS = {"急落株", "急落", "急落録柄", "相場", "補助金", "助成金", "ニュース", "都道府県変更", "業種変更"}
        if text not in _KNOWN_CMDS:
            pref = normalize_prefecture(text)
            if pref:
                _user_subsidy_state.pop(user_id, None)
                try:
                    supabase.table("users").update({"subsidy_prefecture": pref}).eq("user_id", user_id).execute()
                    user["subsidy_prefecture"] = pref
                    logger.info("補助金都道府県保存: user=%s pref=%s", user_id, pref)
                except Exception as e:
                    logger.error("都道府県保存エラー: %s", e)
                try:
                    items = get_subsidy_list(pref, user.get("subsidy_category"))
                    save_last_subsidy_batch(user_id, items, pref, user.get("subsidy_category"))
                    set_subsidy_continue_pending(user_id, user, bool(items))
                    reply_text(event.reply_token, format_subsidy_page(items, pref, user.get("subsidy_category")), quick_reply=qr)
                except Exception as e:
                    logger.error("補助金一覧取得エラー: %s", e)
                    reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
            else:
                reply_text(event.reply_token, "都道府県が認識できませんでした\n例：東京　神奈川　大阪", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        else:
            _user_subsidy_state.pop(user_id, None)

    if _user_subsidy_state.get(user_id) == "await_category" and text in SUBSIDY_CATEGORIES:
        _user_subsidy_state.pop(user_id, None)
        try:
            supabase.table("users").update({"subsidy_category": text}).eq("user_id", user_id).execute()
            user["subsidy_category"] = text
            logger.info("補助金業種保存: user=%s cat=%s", user_id, text)
        except Exception as e:
            logger.error("業種保存エラー: %s", e)
        try:
            items = get_subsidy_list(user.get("subsidy_prefecture"), text)
            save_last_subsidy_batch(user_id, items, user.get("subsidy_prefecture"), text)
            set_subsidy_continue_pending(user_id, user, bool(items))
            reply_text(event.reply_token, format_subsidy_page(items, user.get("subsidy_prefecture"), text), quick_reply=qr)
        except Exception as e:
            logger.error("補助金一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in {"急落株", "急落", "急落録柄"}:
        try:
            drops, nikkei_pct, fetched_at, stale_fallback = get_drop_list_for_reply()
            if fetched_at is None:
                reply_text(event.reply_token, "急落株データがまだない\n前場寄り後か後場引け後の更新を待って", quick_reply=qr)
                return
            _user_drop_list[user_id] = drops
            reply_text(event.reply_token, format_drop_list_text(drops, nikkei_pct, fetched_at, stale_fallback), quick_reply=qr)
        except Exception as e:
            logger.error("急落株一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    stock_from_text = _resolve_drop_stock_from_text(user_id, text)
    if stock_from_text:
        try:
            nikkei_pct = get_nikkei_change_pct()
            stock = stock_from_text if stock_from_text.get("price") is not None else get_single_stock_change(stock_from_text["code"])
            if not stock:
                reply_text(event.reply_token, "銀柄データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
                return
            change_pct = stock["change_pct"] if stock else None
            comment = get_stock_ai_comment(stock["code"], stock["name"], change_pct, nikkei_pct)
            company_profile = format_company_profile_text(stock["code"])
            company_block = f"{company_profile}\n\n" if company_profile else ""
            week_pct = stock.get("week_pct")
            month_pct = stock.get("month_pct")
            from_high_pct = stock.get("from_high_pct")
            week_text = f"{week_pct:+.1f}%" if week_pct is not None else "N/A"
            month_text = f"{month_pct:+.1f}%" if month_pct is not None else "N/A"
            from_high_text = f"{from_high_pct:+.1f}%" if from_high_pct is not None else "N/A"
            reply_text(
                event.reply_token,
                f"{stock['code']} {stock['name']}\n"
                f"{company_block}"
                f"取得: {stock.get('fetched_at', '-')}\n\n"
                f"価格   {stock['price']:,.0f}円\n"
                f"前日比 {_format_day_change_text(stock.get('price'), stock.get('day_pct'))}\n"
                f"週次   {week_text}\n"
                f"月次   {month_text}\n"
                f"高値差 {from_high_text}\n\n{comment}",
                quick_reply=qr,
            )
        except Exception as e:
            logger.error("急落株銀柄入力エラー: %s", e)
            reply_text(event.reply_token, "解説取得に失敗した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "相場":
        try:
            content = get_all_markets_reply()
            reply_text(event.reply_token, content, quick_reply=qr)
        except Exception as e:
            logger.error("相場取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in {"補助金", "助成金"}:
        _user_subsidy_state.pop(user_id, None)
        pref = user.get("subsidy_prefecture")
        cat = user.get("subsidy_category")
        logger.info("補助金一覧起動: user=%s pref=%s cat=%s", user_id, pref, cat)
        try:
            items = get_subsidy_list(pref, cat)
            save_last_subsidy_batch(user_id, items, pref, cat)
            set_subsidy_continue_pending(user_id, user, bool(items))
            reply_text(event.reply_token, format_subsidy_page(items, pref, cat), quick_reply=qr)
        except Exception as e:
            logger.error("補助金一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if user.get("subsidy_continue_pending") and text not in {"補助金", "助成金", "補助金続き", "助成金続き", "続き", "都道府県変更", "業種変更"}:
        set_subsidy_continue_pending(user_id, user, False)

    if text in {"補助金続き", "助成金続き"} or (text == "続き" and user.get("subsidy_continue_pending")):
        batch = get_last_subsidy_batch(user_id)
        if not batch or not batch.get("items"):
            reply_text(event.reply_token, "先に「補助金」を押して\n一覧を出してから続きが見れる", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        items = batch.get("items") or []
        offset = int(batch.get("next_offset") or 0)
        pref = batch.get("prefecture") or user.get("subsidy_prefecture")
        cat = batch.get("category") or user.get("subsidy_category")
        if offset >= len(items):
            set_subsidy_continue_pending(user_id, user, False)
            reply_text(event.reply_token, "これで全部見た\n条件を変えるなら都道府県変更 / 業種変更", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        next_offset = min(offset + SUBSIDY_PAGE_SIZE, len(items))
        update_last_subsidy_batch_offset(user_id, next_offset)
        set_subsidy_continue_pending(user_id, user, next_offset < len(items))
        reply_text(event.reply_token, format_subsidy_page(items, pref, cat, offset=offset, page_size=SUBSIDY_PAGE_SIZE), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "都道府県変更":
        _user_subsidy_state[user_id] = "await_prefecture"
        reply_text(event.reply_token, "都道府県を入力してください\n例：東京　神奈川　大阪", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "業種変更":
        _user_subsidy_state[user_id] = "await_category"
        reply_text(event.reply_token, "業種を選んでください", quick_reply=_subsidy_category_quick_reply())
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _NEWS_TRIGGER_KW:
        _user_drop_list.pop(user_id, None)
        _exclude = get_recent_sent_links(user_id, article_limit=30)
        messages, _ = fetch_news_for_reply(user_id, exclude_links=_exclude)
        if not messages:
            reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
        else:
            reply_text(event.reply_token, messages[0], quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if any(w in text for w in _BLOCKLIST):
        reply_text(event.reply_token, _BLOCKLIST_TEXT, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if is_link_request(text):
        batch_items = get_last_news_batch(user_id)
        if batch_items:
            reply_text(event.reply_token, _build_link_message(batch_items), quick_reply=qr)
        else:
            messages, news_filtered = fetch_news_for_reply(user_id)
            if not messages:
                reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
            else:
                reply_text(event.reply_token, messages[0], quick_reply=qr)
                if news_filtered:
                    _link_items = [
                        {"index": i + 1, "title": n.get("title", ""), "link": n.get("link", "")}
                        for i, n in enumerate(news_filtered)
                    ]
                    _push_text(user_id, _build_link_message(_link_items))
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if is_detail_only_request(text):
        _detail_ctx = get_latest_news_context(user_id)
        _reuse = get_reusable_last_news_targets(user, _detail_ctx)
        if _reuse:
            text = "と".join(str(x) for x in _reuse) + "詳しく"
        else:
            reply_text(event.reply_token, "直前の質問が見つからない\n番号で指定してみて（例: 1詳しく）", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

    _nums = parse_detail_request(text)
    if _nums:
        logger.info("深掘り: user=%s nums=%s text=%r", user_id, _nums, text)
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer = answer_detail_new(user_id, _nums)
        reply_text(event.reply_token, answer, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        save_last_news_question_targets(user_id, _nums, get_latest_news_context(user_id))
        return

    logger.info("自然文Q&A: user=%s text=%r", user_id, text)
    _matched_by_ctx = is_related_to_news_context(user_id, text)

    if _matched_by_ctx:
        if not is_news_question(text) and not _looks_like_question_or_command(text):
            reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer, _q_targets = answer_news_question(user_id, text)
        reply_text(event.reply_token, answer, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        if _q_targets:
            save_last_news_question_targets(user_id, _q_targets, get_latest_news_context(user_id))
        return

    reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
    try:
        supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
    except Exception:
        pass


@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    data = event.postback.data

    logger.info("Postback受信: user=%s data=%s", user_id, data)

    user, _ = ensure_user(user_id)
    genres = list(user.get("genres", []) or [])

    if data.startswith("toggle_display_genre:"):
        display = data.split(":", 1)[1]
        internals = DISPLAY_GENRE_MAP.get(display, [])

        was_selected = any(c in genres for c in internals)
        if was_selected:
            genres = [c for c in genres if c not in internals]
        else:
            for cat in internals:
                if cat not in genres:
                    genres.append(cat)

        supabase.table("users").update({"genres": genres}).eq("user_id", user_id).execute()
        # トグル時はニュース取得せず、画面更新のみ
        reply_flex(event.reply_token, build_genre_flex(genres))

    elif data == "confirm_genres":
        # 決定ボタン: 完了メッセージのみ返す（ニュース取得はユーザーが「ニュース」で行う）
        qr = main_quick_reply()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, "ジャンル設定した\nニュースで確認できる", quick_reply=qr)

    elif data == "clear_genres":
        supabase.table("users").update({"genres": []}).eq("user_id", user_id).execute()
        reply_flex(event.reply_token, build_genre_flex([]))


# ─── Web UI ───────────────────────────────────────────────────────────────


def get_signal_badge_label(row: dict) -> str:
    status = row.get("status") or ""
    stage = row.get("signal_stage") or "none"
    if status == "excluded":
        return "除外"
    if stage == "strong_confirmed":
        return "本命シグナル"
    if stage == "confirmed":
        return "通常シグナル"
    if stage == "early":
        return "初動シグナル"
    if status == "notified" or row.get("rebound_notified_at"):
        return "通知済み"
    if status == "watching":
        return "監視中"
    return "シグナルなし"


def get_watchlist_counts(rows: list[dict]) -> dict:
    """ダッシュボード集計。各一覧ページの表示条件と一致させる。"""
    valid_stages = SIGNAL_STAGES  # {"early", "confirmed", "strong_confirmed"}

    watching = [r for r in rows if r.get("status") == "watching"]

    active_signal = [
        r for r in rows
        if r.get("status") == "rebound_signal"
        and r.get("signal_stage") in valid_stages
    ]

    notified = [
        r for r in rows
        if r.get("rebound_notified_at") or r.get("status") == "notified"
    ]

    unique_ids = {
        r.get("id") for r in watching + active_signal + notified if r.get("id")
    }

    return {
        "watching": len(watching),
        "active_signal": len(active_signal),
        "notified": len(notified),
        "total": len(unique_ids),
    }


@app.route("/web/")
@app.route("/web/dashboard")
def web_dashboard():
    market_adjustment = _current_market_adjustment()
    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0

    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .neq("status", "closed")
            .order("drop_pct", desc=False)
            .limit(500)
            .execute()
            .data or []
        )
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
    except Exception as e:
        logger.error("dashboard error: %s", e)
        rows = []
    signal_rows = [
        r for r in rows
        if r.get("status") == "rebound_signal"
        and r.get("signal_stage") in SIGNAL_STAGES
    ]
    signal_rows.sort(
        key=lambda r: (
            STAGE_RANK.get(r.get("signal_stage"), 0),
            _num(r, "signal_probability", "ai_probability"),
            _num(r, "expected_value"),
            _num(r, "signal_score", "rebound_score", "score"),
        ),
        reverse=True,
    )
    watching_rows = [r for r in rows if r.get("status") == "watching"]
    stats = get_watchlist_counts(rows)
    return render_template("web/dashboard.html",
        rows=rows,
        signal_rows=signal_rows,
        watching_rows=watching_rows,
        stats=stats,
        market_adjustment=market_adjustment,
    )


@app.route("/web/actions/refresh", methods=["POST"])
def web_refresh():
    try:
        from services.market_regime_updater import update_market_regime_for_latest_trade_date

        update_market_regime_for_latest_trade_date(supabase, force=True)
        flash("市場環境を更新しました", "success")
    except Exception as e:
        logger.exception("manual refresh failed")
        flash(f"更新失敗: {e}", "danger")
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/web/watchlist")
def web_watchlist():
    status_filter = request.args.get("status", "all")
    market_adjustment = _current_market_adjustment()
    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0

    try:
        q = supabase.table("stock_drop_watchlist").select("*").order("drop_pct", desc=False)
        if status_filter != "all":
            q = q.eq("status", status_filter)
        rows = q.limit(200).execute().data or []
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
        rows.sort(
            key=lambda r: (
                STAGE_RANK.get(r.get("signal_stage"), 0),
                _num(r, "signal_probability", "ai_probability"),
                _num(r, "expected_value"),
                _num(r, "signal_score", "rebound_score", "score"),
            ),
            reverse=True,
        )
    except Exception as e:
        logger.error("watchlist error: %s", e)
        rows = []
    return render_template("web/watchlist.html", rows=rows, status_filter=status_filter, market_adjustment=market_adjustment)


@app.route("/web/watchlist/<item_id>/close", methods=["POST"])
def web_watchlist_close(item_id):
    try:
        supabase.table("stock_drop_watchlist").update({
            "status": "closed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", item_id).execute()
        flash("クローズした", "success")
    except Exception as e:
        flash(f"エラー: {e}", "danger")
    return redirect(url_for("web_watchlist", status=request.args.get("status", "all")))


@app.route("/web/signals")
def web_signals():
    market_adjustment = _current_market_adjustment()
    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0

    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .eq("status", "rebound_signal")
            .order("last_signal_at", desc=True)
            .limit(300)
            .execute()
            .data or []
        )
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
    except Exception as e:
        logger.error("signals error: %s", e)
        rows = []

    rows = [
        r for r in rows
        if r.get("status") == "rebound_signal"
        and r.get("signal_stage") in SIGNAL_STAGES
    ]
    rows.sort(
        key=lambda r: (
            STAGE_RANK.get(r.get("signal_stage"), 0),
            _num(r, "signal_probability", "ai_probability"),
            _num(r, "expected_value"),
            _num(r, "signal_score", "rebound_score", "score"),
            r.get("last_signal_at") or "",
        ),
        reverse=True,
    )
    return render_template("web/signals.html", rows=rows, market_adjustment=market_adjustment)


@app.route("/web/settings", methods=["GET", "POST"])
def web_settings():
    if request.method == "POST":
        bool_fields = {
            "ma5_cross_enabled", "drop_notify_enabled", "rebound_notify_enabled",
            "morning_summary_enabled", "portfolio_notify_enabled",
            "ai_predict_enabled", "ai_notify_enabled", "ai_notify_early_enabled",
            "jquants_enabled", "jquants_prefer_source", "jquants_fallback_yfinance",
        }
        int_fields = {"watch_days_limit", "jquants_max_retry"}
        def _upsert_settings(payload: dict) -> None:
            remaining = dict(payload)
            for _ in range(8):
                try:
                    user_id = remaining.get("user_id", "global")
                    update_payload = {k: v for k, v in remaining.items() if k != "user_id"}
                    updated = (
                        supabase.table("strategy_settings")
                        .update(update_payload)
                        .eq("user_id", user_id)
                        .execute()
                        .data or []
                    )
                    if updated:
                        return
                    supabase.table("strategy_settings").insert(remaining).execute()
                    return
                except Exception as e:
                    msg = str(e)
                    marker = "Could not find the '"
                    missing = None
                    if marker in msg:
                        missing = msg.split(marker, 1)[1].split("'", 1)[0]
                    if missing and missing in remaining:
                        logger.warning("strategy_settings column missing; skip field for this save: %s", missing)
                        remaining.pop(missing, None)
                        continue
                    raise

        try:
            data: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
            for key, default in _settings_loader.DEFAULTS.items():
                if key in bool_fields:
                    data[key] = key in request.form
                elif key in int_fields:
                    data[key] = int(request.form.get(key, default))
                else:
                    data[key] = float(request.form.get(key, default))
            _upsert_settings({**data, "user_id": "global"})
            _settings_loader._cache = None
            flash("設定を保存した", "success")
        except Exception as e:
            logger.error("settings save error: %s", e)
            flash(f"保存失敗: {e}", "danger")
        return redirect(url_for("web_settings"))
    cfg = _settings_loader.get_settings()
    return render_template("web/settings.html", cfg=cfg)


@app.route("/web/virtual-trades")
def web_virtual_trades():
    try:
        open_trades = (
            supabase.table("virtual_trades").select("*")
            .eq("status", "open").order("buy_date", desc=True).execute().data or []
        )
        closed_trades = (
            supabase.table("virtual_trades").select("*")
            .eq("status", "closed").order("sell_date", desc=True).limit(100).execute().data or []
        )
    except Exception as e:
        logger.error("virtual_trades error: %s", e)
        open_trades, closed_trades = [], []

    open_cost_total = 0.0
    open_value_total = 0.0
    open_unrealized_pnl_total = 0.0

    # 保有中の現在価格・含み損益を取得
    try:
        import yfinance as yf
        for t in open_trades:
            code = t.get("code", "")
            market = t.get("market", "")
            ticker = code if market == "dow" else f"{code}.T"
            buy = float(t.get("buy_price") or 0)
            qty = int(t.get("quantity") or 100)
            cost = buy * qty
            t["cost_amount"] = cost
            open_cost_total += cost
            try:
                hist = yf.Ticker(ticker).history(period="2d", auto_adjust=True)
                if not hist.empty:
                    current = float(hist["Close"].iloc[-1])
                    value = current * qty
                    pnl = value - cost
                    t["current_price"] = current
                    t["market_value"] = value
                    t["unrealized_pct"] = (current - buy) / buy * 100 if buy > 0 else None
                    t["unrealized_pnl"] = pnl if buy > 0 else None
                    open_value_total += value
                    open_unrealized_pnl_total += pnl if buy > 0 else 0
            except Exception:
                t["current_price"] = None
                t["market_value"] = None
                t["unrealized_pct"] = None
                t["unrealized_pnl"] = None
    except ImportError:
        for t in open_trades:
            buy = float(t.get("buy_price") or 0)
            qty = int(t.get("quantity") or 100)
            cost = buy * qty
            t["cost_amount"] = cost
            t["market_value"] = None
            open_cost_total += cost

    total_pnl = sum(t.get("profit_loss") or 0 for t in closed_trades)
    win_count = sum(1 for t in closed_trades if (t.get("profit_loss") or 0) > 0)
    open_unrealized_pct_total = (
        open_unrealized_pnl_total / open_cost_total * 100
        if open_cost_total > 0 else None
    )
    return render_template(
        "web/virtual_trades.html",
        open_trades=open_trades,
        closed_trades=closed_trades,
        total_pnl=total_pnl,
        win_count=win_count,
        open_cost_total=open_cost_total,
        open_value_total=open_value_total,
        open_unrealized_pnl_total=open_unrealized_pnl_total,
        open_unrealized_pct_total=open_unrealized_pct_total,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/admin/models")
@app.route("/web/models")
def web_models():
    try:
        rows = (
            supabase.table("ml_models")
            .select("*")
            .order("created_at", desc=True)
            .limit(50)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.error("models error: %s", e)
        rows = []
    return render_template("web/models.html", rows=rows)


@app.route("/web/portfolio")
def web_portfolio():
    return render_template("web/stub.html", title="ポートフォリオ", message="Phase 3 で実装予定")


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        debug=False,
        use_reloader=False,
    )
