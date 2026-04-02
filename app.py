import json
import logging
import os
import re
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dotenv import load_dotenv
from flask import Flask, request, abort
from openai import OpenAI
from supabase import create_client

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
    URIAction,
    FlexMessage,
    FlexBubble,
    FlexBox,
    FlexButton,
    FlexText,
)
from linebot.v3.webhooks import FollowEvent, MessageEvent, PostbackEvent, TextMessageContent

from send_news import send_news_to_user

# ─── 初期設定 ───
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ENV = os.getenv("ENV", "prod")
PAYMENT_URL = os.getenv("PAYMENT_URL", "").strip()
LINE_MEMBERSHIP_USE_API_SYNC = os.getenv("LINE_MEMBERSHIP_USE_API_SYNC", "true").lower() == "true"
LINE_API_BASE = "https://api.line.me"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── ログ ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

print("=== 起動確認 ===")
print(f"環境: {ENV}")
if ENV == "test":
    print("🟢 テスト環境で実行中")
elif ENV == "prod":
    print("🔴 本番環境で実行中（注意）")
    print("🔴 本番環境です。内容を確認してください")

# ─── ジャンル定義 ───
DISPLAY_GENRE_MAP = {
    "経済": ["economy", "interest_rates", "real_estate"],
    "仕事": ["business", "construction"],
    "国際": ["international"],
    "AI・テック": ["ai", "tech"],
    "暮らし": ["energy", "materials"],
    "話題": ["entertainment", "scandal", "other"],
    "スポーツ": ["sports"],
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
    "芸能": "話題",
    "エンタメ": "話題",
    "生活": "暮らし",
}


# ─── DB操作 ───
def get_user(user_id: str):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def normalize_plan(plan: str) -> str:
    if plan in ["light", "premium", "paid"]:
        return "paid"
    return "free"


def resolve_effective_plan(user: dict, now_dt) -> str:
    """membership_status == active なら即 paid。それ以外はトライアル判定へ。"""
    if user.get("membership_status", "none") == "active":
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


def save_user(user_id: str, active=True, plan="free", genres=None):
    if genres is None:
        genres = []
    plan = normalize_plan(plan)

    supabase.table("users").upsert({
        "user_id": user_id,
        "active": active,
        "plan": plan,
        "genres": genres,
    }).execute()

    logger.info("Supabase保存: user=%s active=%s plan=%s genres=%s", user_id, active, plan, genres)


def ensure_user(user_id: str):
    user = get_user(user_id)
    if not user:
        save_user(user_id, active=True, plan="free", genres=[])
        logger.info("新規ユーザー登録: %s", user_id)
        return {
            "user_id": user_id,
            "active": True,
            "plan": "free",
            "genres": [],
            "extended_trial_ended_notified": False,
            "night_delivery": True,
            "ai_count": 0,
            "pending_action": None,
            "trial_started_at": None,
            "trial_extended_until": None,
            "feedback_reward_used": False,
            "feedback_pending": False,
            "membership_status": "none",
            "membership_expires_at": None,
            "membership_plan_id": None,
            "membership_last_event_type": None,
            "membership_updated_at": None,
        }, True
    user["plan"] = normalize_plan(user.get("plan", "free"))
    user.setdefault("night_delivery", True)
    user.setdefault("ai_count", 0)
    user.setdefault("pending_action", None)
    user.setdefault("trial_started_at", None)
    user.setdefault("trial_extended_until", None)
    user.setdefault("feedback_reward_used", False)
    user.setdefault("feedback_pending", False)
    user.setdefault("extended_trial_ended_notified", False)
    user.setdefault("membership_status", "none")
    user.setdefault("membership_expires_at", None)
    user.setdefault("membership_plan_id", None)
    user.setdefault("membership_last_event_type", None)
    user.setdefault("membership_updated_at", None)
    user.setdefault("last_news_question_targets", None)
    user.setdefault("last_news_question_at", None)
    user.setdefault("last_news_context_sent_at", None)
    return user, False


# ─── LINEメンバーシップ ───
def get_line_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_membership_subscription_status(user_id):
    url = f"{LINE_API_BASE}/v2/bot/membership/subscription/{user_id}"
    try:
        res = requests.get(url, headers=get_line_headers(), timeout=10)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logger.error("membership API失敗 user=%s %s", user_id, e)
        return None


def membership_state_from_api(data):
    if not data:
        return {
            "membership_status": "none",
            "membership_expires_at": None,
            "membership_plan_id": None,
        }
    active = bool(data.get("active", False))
    expires = (
        data.get("endTime")
        or data.get("expiredAt")
        or data.get("expiresAt")
        or data.get("membershipExpiresAt")
    )
    plan_id = (
        data.get("planId")
        or data.get("membershipPlanId")
        or data.get("id")
    )
    return {
        "membership_status": "active" if active else "none",
        "membership_expires_at": expires,
        "membership_plan_id": str(plan_id) if plan_id is not None else None,
    }


def membership_state_from_event(event):
    source = event.get("membership") or event
    event_type = (
        event.get("type")
        or source.get("type")
        or source.get("eventType")
        or source.get("membershipEventType")
        or ""
    )
    plan_id = (
        source.get("planId")
        or source.get("membershipPlanId")
        or source.get("id")
    )
    expires_at = (
        source.get("endTime")
        or source.get("expiredAt")
        or source.get("expiresAt")
        or source.get("membershipExpiresAt")
    )
    event_type_l = str(event_type).lower()
    if any(k in event_type_l for k in ["join", "activate", "start", "renew"]):
        status = "active"
    else:
        status = "none"
    return {
        "membership_status": status,
        "membership_expires_at": expires_at,
        "membership_plan_id": str(plan_id) if plan_id is not None else None,
    }


def apply_membership(user_id, state, event_type=None):
    status = state.get("membership_status", "none")
    update = {
        "membership_status": status,
        "membership_expires_at": state.get("membership_expires_at"),
        "membership_plan_id": state.get("membership_plan_id"),
        "membership_last_event_type": event_type,
        "membership_updated_at": datetime.now(timezone.utc).isoformat(),
        "plan": "paid" if status == "active" else "free",
    }

    if status == "active":
        update["night_delivery"] = True

    try:
        supabase.table("users").upsert({"user_id": user_id, **update}).execute()
        logger.info(
            "membership反映成功 user=%s status=%s plan=%s expires=%s plan_id=%s event=%s night_delivery=%s",
            user_id,
            status,
            update["plan"],
            state.get("membership_expires_at"),
            state.get("membership_plan_id"),
            event_type,
            update.get("night_delivery"),
        )
    except Exception as e:
        logger.error("membership反映失敗 user=%s %s", user_id, e)


def sync_membership_from_api(user_id, event_type=None):
    data = get_membership_subscription_status(user_id)
    state = membership_state_from_api(data)
    apply_membership(user_id, state, event_type=event_type)
    return state


def handle_membership_event(event):
    user_id = event.get("source", {}).get("userId") or event.get("userId")
    if not user_id:
        logger.warning("membership event user_id不明: %s", event)
        return
    event_type = (
        event.get("type")
        or (event.get("membership") or {}).get("type")
        or (event.get("membership") or {}).get("eventType")
        or (event.get("membership") or {}).get("membershipEventType")
        or "membership"
    )
    _, _ = ensure_user(user_id)
    if LINE_MEMBERSHIP_USE_API_SYNC:
        sync_membership_from_api(user_id, event_type=event_type)
    else:
        state = membership_state_from_event(event)
        apply_membership(user_id, state, event_type=event_type)


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
def main_quick_reply() -> QuickReply:
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label="使い方", text="使い方")),
        QuickReplyItem(action=MessageAction(label="停止", text="停止")),
        QuickReplyItem(action=MessageAction(label="ジャンル", text="ジャンル")),
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


def reply_with_payment(reply_token: str, text: str, quick_reply: QuickReply = None) -> None:
    """テキスト＋課金ボタン（Flex）を1回のreplyで送る。PAYMENT_URL未設定時は通常テキスト返信にフォールバック。"""
    if not PAYMENT_URL:
        reply_text(reply_token, text, quick_reply)
        return
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            text_msg = TextMessage(text=text)
            payment_bubble = FlexBubble(
                body=FlexBox(
                    layout="vertical",
                    contents=[
                        FlexButton(
                            action=URIAction(label="このまま続ける", uri=PAYMENT_URL),
                            style="primary",
                            height="sm",
                        )
                    ],
                )
            )
            flex_msg = FlexMessage(alt_text="このまま続ける", contents=payment_bubble)
            if quick_reply:
                flex_msg.quick_reply = quick_reply
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[text_msg, flex_msg])
            )
    except Exception as e:
        logger.error("LINE課金ボタン返信エラー: %s", e)


GENRE_DESC = {
    "経済": "金利・為替・不動産",
    "仕事": "業界・法改正・労働",
    "国際": "海外情勢・外交",
    "AI・テック": "AI・IT・科学",
    "暮らし": "医療・教育・生活",
    "話題": "芸能・SNS・流行",
    "スポーツ": "メジャー・マイナー",
}


def build_genre_flex(current_genres: list) -> FlexMessage:
    rows = []
    layout_rows = [
        ["経済", "仕事"],
        ["国際", "AI・テック"],
        ["暮らし", "話題"],
        ["スポーツ"],
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
                        label="すべて解除",
                        data="clear_genres",
                        display_text="クリア",
                    ),
                    style="link",
                    height="sm",
                    color="#aaaaaa",
                )
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
    return False


def strip_leading_number(text: str) -> str:
    """返答文頭の番号（例: '13.' / '13 ' / '13　'）を除去する（先頭のみ）"""
    import re
    return re.sub(r"^\s*\d+[\.．\s　]+", "", text)


def _strip_any_leading_number(text: str) -> str:
    """返答文頭の番号（丸数字・半角数字どちらも）を1個除去する（先頭のみ、本文中は触らない）"""
    import re
    t = (text or "").strip()
    t = re.sub(r"^\s*[①-⑩]\s*", "", t)
    t = re.sub(r"^\s*\d+[\.．]?\s*", "", t)
    return t.strip()
_MAIN_MORE_KW = ["ほかに", "他にニュース", "もっとニュース", "追加ニュース"]
_SUB_MORE_KW = ["ほか", "他に", "もっと", "追加", "それ以外", "他にも"]
_FOLLOWUP_KW = ["他には", "別のニュース", "続き", "次"]

_CONTEXT_TOKEN_STOPWORDS = {
    "経済", "金利", "影響", "理由", "内容", "状況",
    "問題", "情報", "世界", "ニュース", "話題",
}


_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"

_CONTEXT_TTL_HOURS = 6

_BLOCKLIST = [
    "付き合", "好き",
    "お前誰", "何者", "自己紹介",
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

    for item in payload.get("extra_items", []):
        add_text(item.get("title", ""))
        add_text(item.get("reason", ""))
        add_text(item.get("interpretation", ""))

    for s in payload.get("summary", []):
        add_text(s)

    for s in payload.get("impact", []):
        add_text(s)

    for t in payload.get("topics", []):
        add_text(t.get("theme", ""))
        add_text(t.get("line", ""))

    add_text(payload.get("message_1", ""))
    add_text(payload.get("message_2", ""))

    seen = set()
    uniq = []
    for t in tokens:
        key = _normalize_text(t)
        if key and key not in seen:
            seen.add(key)
            uniq.append(t)
    return uniq


def _parse_article_num(question: str, max_n: int = 5) -> Optional[int]:
    nums = parse_article_numbers(question, max_n=max_n)
    return nums[0] if len(nums) == 1 else None


def extract_number(text: str) -> Optional[int]:
    return _parse_article_num(text, max_n=10)


def parse_article_numbers(text: str, max_n: int = 10) -> List[int]:
    """テキスト中の記事番号を全て抽出して昇順リストで返す。
    「135」→[1,3,5]、「①③⑤」→[1,3,5]、「1と3と5」→[1,3,5] のように処理する。
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
            # 桁ごとに分解: "135" → 1, 3, 5
            for ch in token:
                d = int(ch)
                if 1 <= d <= max_n:
                    found.add(d)
    return sorted(found)


def is_followup(text: str) -> bool:
    return any(kw in text for kw in _FOLLOWUP_KW)


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
        "もっと", "追加ニュース", "他にニュース",
        "全部", "全リンク", "リンク全部", "全部のリンク",
    ]
    return any(r in text for r in refs)


def _looks_like_question_or_command(text: str) -> bool:
    if parse_article_numbers(text, max_n=10):
        return True
    if is_link_request(text):
        return True
    if any(kw in text for kw in _MAIN_MORE_KW + _SUB_MORE_KW):
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


def _answer_more_news(shown: list, display_start: int) -> str:
    """shown: 表示対象記事リスト, display_start: 最初の記事の表示番号（1始まり）"""
    if not shown:
        return "一旦このへんかな\n\n気になるやつあれば聞いて\nこの先もう少し見れるようにしてる"

    lines = ["あとこれも出てる", ""]
    for i, n in enumerate(shown):
        num = display_start + i
        lines.append(f"{num}. {n.get('title', '')}")
        lines.append("")

    lines.append("気になるのあれば言って")
    return "\n".join(lines).rstrip()


def _answer_url(question: str, news_items: list, extra_items: list = None, visible_extra: int = 0) -> str:
    """visible_extra: 表示済みのextra件数。番号指定なし時はnews_items + その分だけ返す"""
    extra_items = extra_items or []
    # 番号指定なし時の返却範囲（表示済み範囲）
    visible_items = news_items + extra_items[:visible_extra]
    all_items = news_items + extra_items
    total = len(all_items)

    index_map = {item.get("index", 0): item for item in all_items}

    nums = parse_article_numbers(question, max_n=total)

    if nums:
        if len(nums) == 1:
            item = index_map.get(nums[0])
            if not item:
                return f"{nums[0]}番目のニュースが見つからなかった"
            link = item.get("link", "")
            return f"{nums[0]}. の元記事\n{link}" if link else "この記事は元リンクが取れなかった"
        lines = ["元記事リンク", ""]
        for n in nums:
            item = index_map.get(n)
            if not item:
                continue
            link = item.get("link", "")
            lines.append(f"{n}. {link}" if link else f"{n}. (リンクなし)")
        return "\n".join(lines)

    # 番号指定なし → 表示済み範囲のみ
    lines = ["元記事リンク", ""]
    for item in visible_items:
        idx = item.get("index", 0)
        link = item.get("link", "")
        lines.append(f"{idx}. {link}" if link else f"{idx}. (リンクなし)")
    return "\n".join(lines)


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
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がないから答えられないかも\n一度配信を受けてから聞いてみて", []

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])
    extra_items = payload.get("extra_items", [])

    if is_link_request(question):
        visible_extra = payload.get("extra_index", 0)
        return _answer_url(question, news_items, extra_items, visible_extra=visible_extra), []

    is_more_news = (
        any(k in question for k in _MAIN_MORE_KW)
        or (any(k in question for k in _SUB_MORE_KW) and "ニュース" in question)
    )
    if is_more_news and any(g in question for g in ["影響", "意味", "問題", "理由", "なぜ", "なんで"]):
        is_more_news = False
    if is_more_news:
        idx = payload.get("extra_index", 0)
        shown = extra_items[idx:idx + 5]
        return _answer_more_news(shown, len(news_items) + idx + 1), []

    is_detail = any(k in question for k in _DETAIL_KEYWORDS)

    all_items = news_items + extra_items
    total = len(all_items)
    index_map = {item.get("index", 0): item for item in all_items}

    # 番号指定があれば対象記事だけに絞る
    specified_nums = parse_article_numbers(question, max_n=total)
    logger.info("抽出番号: %s", specified_nums)
    if specified_nums:
        target_items = [index_map[n] for n in specified_nums if n in index_map]
        logger.info("対象: %s", [n.get("index") for n in target_items])
    else:
        # 自然文：タイトル/reason/interpretationにキーワード一致で最大2件
        norm_q = _normalize_text(question)
        matched = []
        for item in all_items:
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

    # 単一記事 → 従来通り
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
    number_rule = f"指定番号は {specified_nums}。必ずこの番号で返答しろ。\n\n" if specified_nums else ""
    user_prompt = (
        f"ニュース:\n{news_text}\n\n"
        f"{number_rule}"
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
        if len(specified_nums) == 1:
            raw = _strip_any_leading_number(raw)
        return raw, targets
    except Exception as e:
        logger.error("Q&A OpenAI エラー: %s", e)
        return "今ちょっと返答うまくいかない\n\nもう一回送るか\n「使い方」押してみて", []


_CHAT_TOPIC_FOLLOW_UP_FREE = (
    "\n\n相手に合わせた話題は\n"
    "メンバーシップで使える"
)

_CHAT_TOPIC_FOLLOW_UP_PAID = (
    "\n\nちなみに今日誰かと話す予定ある？\n\n"
    "誰と話すか教えてくれれば\n"
    "その人に合わせて話題出すよ。\n"
)

# ─── 会話ネタ共通 ───

_CHAT_TOPIC_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "chat_topic",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "genre":  {"type": "string"},
                "main":   {"type": "string"},
                "reply":  {"type": "string"},
                "next":   {"type": "string"},
                "point":  {"type": "string"},
                "trivia": {"type": "string"},
            },
            "required": ["genre", "main", "reply", "next", "point", "trivia"],
            "additionalProperties": False,
        },
    },
}

_CHAT_TOPIC_SYSTEM_BASE = """\
お前はLINEで使える会話ネタを提案するやつ。

【ルール】
・敬語禁止
・会話調
・そのまま使える形で出す
・短すぎず薄くしない
・AIっぽい文章禁止
・汎用テンプレ禁止。「今っぽさ」を優先

【季節ルール】
・現在の季節を必ず考慮する（日本基準）
・季節とズレた会話は禁止（例：夏に寒い話）
・自然に会話に溶け込ませる（説明しない）

【ジャンル選択】
生活関連 / 仕事関連 / 世界関連 / AI関連 / 話題関連 / スポーツ関連
※迷ったら「話題関連」
※「お金」は使用禁止（生活関連に含める）

【会話フレーズ】
・質問形式を基本にする
・ニュースや現実と軽く接続する

【よくある返し】
・現実的な返答。短く自然

【次の一手】
・会話を広げる or 深める。自然に続く一言

【使いやすいポイント】
・1行のみ。具体的に

【小ネタ】
・1文のみ。20〜30文字。「らしい」「っぽい」で柔らかく

【NG】
・カテゴリ名（例：経済ニュース）
・説明文
・長文
・季節ズレ
・中身のない雑談

必ずJSON形式のみで出力すること。キー：genre / main / reply / next / point / trivia
"""


def format_topic(data: dict) -> str:
    """会話ネタの dict → LINEメッセージ文字列"""
    return (
        f"【{data['genre']}】\n"
        f"「{data['main']}」\n\n"
        "ーーー\n\n"
        f"よくある返し\n『{data['reply']}』\n\n"
        f"→「{data['next']}」\n\n"
        "ーーー\n\n"
        f"・使いやすいポイント\n{data['point']}\n\n"
        "ーーー\n\n"
        f"小ネタ\n《{data['trivia']}》"
    )


def _get_season() -> str:
    month = datetime.now(timezone.utc).month
    if month in (3, 4, 5):
        return "春"
    elif month in (6, 7, 8):
        return "夏"
    elif month in (9, 10, 11):
        return "秋"
    else:
        return "冬"


def generate_chat_topic_free(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    system_prompt = _CHAT_TOPIC_SYSTEM_BASE + f"\n現在の季節：{season}"
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んでJSONで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data) + _CHAT_TOPIC_FOLLOW_UP_FREE
    except Exception as e:
        logger.error("会話ネタ生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


def generate_chat_topic_paid(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    system_prompt = _CHAT_TOPIC_SYSTEM_BASE + f"\n現在の季節：{season}"
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んでJSONで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data) + _CHAT_TOPIC_FOLLOW_UP_PAID
    except Exception as e:
        logger.error("生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


_POLITE_TONE_RULE = (
    "【口調ルール】\n"
    "・会話文は自然な敬語にする\n"
    "・タメ語は禁止\n"
    "・堅すぎるビジネス文書調も禁止\n"
    "・実際の雑談で使える、柔らかい丁寧語にする\n"
)

_CASUAL_TONE_RULE = (
    "【口調ルール】\n"
    "・友達や恋人に話すような自然なくだけた口調でよい\n"
    "・ただし荒い口調は禁止\n"
)


def infer_tone_for_person(person_desc: str) -> str:
    """相手の説明文からカジュアル/丁寧を判定する。"""
    polite_keywords = [
        "上司", "部下", "先輩", "後輩", "同僚", "職場", "会社",
        "仕事相手", "営業", "営業先", "取引先", "お客", "顧客", "客",
        "初対面", "現場", "近所", "元請け", "社長",
    ]
    casual_keywords = [
        "彼女", "彼氏", "好きな人", "気になる人",
        "嫁", "妻", "旦那", "夫",
        "友達", "親友", "知人",
        "家族", "兄弟", "姉妹", "親", "母親", "父親",
    ]
    if any(k in person_desc for k in polite_keywords):
        return "polite"
    if any(k in person_desc for k in casual_keywords):
        return "casual"
    return "casual"


def generate_chat_for_person(user_id: str, person_desc: str) -> str:
    ctx = get_latest_news_context(user_id)
    news_text = ""
    if ctx:
        news_items = ctx.get("payload", {}).get("news_items", [])
        news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    tone = infer_tone_for_person(person_desc)
    tone_rule = _POLITE_TONE_RULE if tone == "polite" else _CASUAL_TONE_RULE
    system_prompt = (
        _CHAT_TOPIC_SYSTEM_BASE
        + f"\n現在の季節：{season}\n\n"
        "【相手ごとの優先ジャンル】\n"
        "彼女・彼氏・家族・友達：日常、食べ物、休日、エンタメ、軽い流行、身近なニュースを優先。"
        "地政学・軍事・金利政策のような重い話題は基本避ける\n"
        "上司・取引先・営業・顧客・初対面：無難で広げやすい話題を優先。"
        "仕事・景気・AI・生活コスト・スポーツ・季節ネタを優先。"
        "政治・宗教・下ネタ・重すぎる事件は避ける\n\n"
        "【追加ルール】\n"
        "・ニュースに縛られず相手に最適な話題を選ぶ\n"
        "・相手に合わない重い話題は避ける\n"
        "・ニュースを使う場合も、そのまま出さず会話向けに軽く変換する\n\n"
        + tone_rule
    )
    user_prompt = (
        f"話す相手: {person_desc}\n\n"
        + (f"参考ニュース（必要なら使っていい）:\n{news_text}\n\n" if news_text else "")
        + "この相手に使えそうな会話ネタを1つJSONで出せ。"
    )
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data)
    except Exception as e:
        logger.error("相手別会話生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"



def can_use_paid_ai(user: dict, effective_plan: str) -> bool:
    return effective_plan == "paid" and user.get("ai_count", 0) < 30


def get_paid_usage_tail(ai_count_after: int) -> str:
    if ai_count_after == 25:
        return "\n\n今日だいぶ使ってるな\nあと少しだな"
    if ai_count_after == 28:
        return "\n\n今日かなり使ってるな\nあとちょっとで上限だ"
    return ""


def _set_pending_person_topic(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": "person_topic",
            "pending_count": 1,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _clear_pending(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": None,
            "pending_count": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _is_pending_person_topic(user: dict) -> bool:
    return (
        user.get("pending_action") == "person_topic"
        and (user.get("pending_count") or 0) > 0
    )


def increment_ai_count(user_id: str, count: int, today: str, now_dt) -> None:
    try:
        supabase.table("users").update({
            "ai_count": count + 1,
            "ai_count_date": today,
            "last_reply_time": now_dt.isoformat(),
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
    try:
        payload = json.loads(body)
    except Exception:
        payload = {}

    events = payload.get("events", []) if isinstance(payload, dict) else []

    for ev in events:
        if isinstance(ev, dict) and ("membership" in ev or ev.get("type") == "membership"):
            try:
                handle_membership_event(ev)
            except Exception as e:
                logger.error("membership event処理失敗: %s", e)

    return "OK"


@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    user, is_new_user = ensure_user(user_id)

    # トライアル開始日が未設定なら登録（新規ユーザー or システム導入前の既存ユーザー）
    if not user.get("trial_started_at"):
        _now = datetime.now(timezone.utc)
        try:
            supabase.table("users").update({
                "trial_started_at": _now.isoformat(),
                "feedback_reward_used": False,
                "feedback_pending": False,
            }).eq("user_id", user_id).execute()
        except Exception as e:
            logger.error("trial_started_at設定失敗: %s", e)

    reply_text(
        event.reply_token,
        "追加ありがとう\n\n"
        "ニュースは朝起きる前に届く\n"
        "きっと寝てる間だからミュートでOK\n\n"
        "必要なジャンルに絞れるから\n"
        "必要ならあとで変えればOK👌\n\n"
        "とりあえず直近のニュース流すから、気になるやつあればそのまま質問して\n"
        "番号でもいけるし、リンクも出せる\n"
        "そのまま使える会話ネタもある\n"
        "詳しくは「使い方」で確認してみて",
        quick_reply=main_quick_reply(),
    )

    if is_new_user:
        try:
            send_news_to_user(user_id)
        except Exception as e:
            logger.error("初回配信失敗: user=%s %s", user_id, e)


_STOP_WORDS = {"停止", "止めて", "停止して", "配信止めて", "もういい", "オフ"}
_START_WORDS = {"開始", "スタート", "再開", "もう一回", "配信して", "オン", "はじめて", "始めて"}
_GENRE_WORDS = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}
_MEMBERSHIP_KW = [
    "メンバー", "有料", "課金",
    "いくら", "料金", "値段",
    "入り方", "登録",
    "何できる", "何ができる",
]


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
_CHAT_TOPIC_KW = ["会話ネタ", "話のネタ", "雑談ネタ", "ネタ教えて", "何話せばいい", "何話す"]
_CHAT_TOPIC_EXACT = {"会話", "会話ネタ", "雑談", "ネタ"}
_PERSON_KW = [
    # 恋愛・パートナー
    "彼女", "彼氏", "好きな人", "気になる人", "嫁", "妻", "旦那", "夫",
    # 友人・知人
    "友達", "親友", "知人",
    # 職場・ビジネス
    "上司", "部下", "先輩", "後輩", "同僚", "職場の人", "会社の人", "仕事相手",
    "営業", "営業先", "取引先", "お客さん", "顧客", "客", "初対面",
    # 関係が難しい人
    "きまずい人", "苦手な人", "微妙な人", "仲悪い人", "距離ある人",
    # 家族
    "親", "母親", "父親", "家族", "兄弟", "姉妹",
    # その他
    "現場の人", "近所の人",
]
_DIRECT_TOPIC_KW = [
    "会話", "話す", "話題", "雑談", "ネタ",
    "何話", "なに話", "何しゃべ", "何喋",
    "使える話題", "向けの話題", "どう話す", "どう会話する",
]


def looks_like_feedback(text: str) -> bool:
    import re

    t = (text or "").strip()
    if not t:
        return False

    # 数字だけ
    if re.fullmatch(r"[0-9０-９]+", t):
        return False

    # 記号だけ
    if re.fullmatch(r"[\W_。、，,！？!?ー〜～…]+", t):
        return False

    # NG短文
    short_ng = {"はい", "いいえ", "ok", "OK", "了解", "うん", "いや"}
    if t in short_ng:
        return False

    # テンプレ項目
    markers = ["使いやすさ", "よかった点", "微妙だった点", "あったらいい機能"]
    if any(m in t for m in markers):
        return True

    # 感想ワード
    feedback_words = [
        "使いやすい", "使いにくい",
        "わかりやすい", "分かりやすい",
        "わかりにくい", "分かりにくい",
        "よかった", "良かった",
        "微妙", "いい", "悪い",
        "便利", "不便",
        "見やすい", "見づらい",
        "難しい", "むずい",
    ]
    if any(w in t for w in feedback_words):
        return True

    # 最低10文字以上
    return len(t) >= 10


def is_direct_person_chat_request(text: str) -> bool:
    """会話ネタ意図＋相手説明を含む一発入力を判定する（辞書不要の自由記述対応）。"""
    # 会話ネタ意図がなければ問答無用でFalse
    topic_hit = any(k in text for k in _DIRECT_TOPIC_KW)
    if not topic_hit:
        return False

    # 辞書一致があればTrue
    if any(kw in text for kw in _PERSON_KW):
        return True

    # 「と」「向け」「相手」があればTrue
    if any(p in text for p in ("と", "向け", "相手")):
        return True

    # 会話ワードを除いた残り文字が4文字以上あればTrue（自由記述）
    stripped = text
    for k in _DIRECT_TOPIC_KW:
        stripped = stripped.replace(k, "")
    return len(stripped.strip()) >= 4


_ALL_LINK_KW = ["全部", "全て", "一覧", "まとめ", "全リンク", "リンク全部", "全部のリンク"]

# 強コマンド — pending を問答無用でスキップ・クリアする
_STRONG_COMMANDS = (
    _STOP_WORDS
    | _START_WORDS
    | _GENRE_WORDS
    | _STATUS_WORDS
    | _HELP_WORDS
    | {"夜停止", "夜いらない", "夜開始", "夜オン"}
)


def _plan_status_text(plan: str, active: bool, genres: list, night_delivery: bool = True) -> str:
    plan = normalize_plan(plan)
    genre_label = f"ジャンル: {format_genres(genres)}" if genres else "ジャンル: 未設定（全部配信）"
    if plan != "free":
        day_label = "昼配信オン" if active else "昼配信オフ"
        night_label = "夜配信オン" if night_delivery else "夜配信オフ"
        return f"今こんな感じ\n\nメンバーシップ\n{day_label}\n{night_label}\n{genre_label}"
    else:
        active_label = "配信オン" if active else "配信オフ"
        return f"今こんな感じ\n\n無料プラン\n{active_label}\n{genre_label}"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = normalize_user_text(event.message.text)

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)
    user, _ = ensure_user(user_id)

    now_dt = datetime.now(timezone.utc)

    _base_plan = normalize_plan(user.get("plan", "free"))
    if _base_plan == "free" and not user.get("trial_started_at"):
        try:
            supabase.table("users").update({
                "trial_started_at": now_dt.isoformat(),
            }).eq("user_id", user_id).execute()
            user["trial_started_at"] = now_dt.isoformat()
        except Exception as e:
            logger.error("trial_started_at補完失敗: %s", e)

    # ── 軽い連投制限（3秒）──
    _last = user.get("last_reply_time")
    if _last is not None:
        try:
            _last_dt = datetime.fromisoformat(str(_last).replace("Z", "+00:00"))
            if now_dt - _last_dt < timedelta(seconds=3):
                return
        except Exception:
            pass

    # ── 日付リセット ──
    today = datetime.now(timezone.utc).date().isoformat()
    if user.get("free_reply_date") != today:
        try:
            supabase.table("users").update({
                "free_reply_used": False,
                "free_reply_date": today,
                "ai_count": 0,
                "ai_count_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["free_reply_used"] = False
        user["free_reply_date"] = today
        user["ai_count"] = 0
        user["ai_count_date"] = today

    if user.get("free_chat_topic_date") != today:
        try:
            supabase.table("users").update({
                "free_chat_topic_used": False,
                "free_chat_topic_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["free_chat_topic_used"] = False
        user["free_chat_topic_date"] = today

    if user.get("ai_count_date") != today:
        try:
            supabase.table("users").update({
                "ai_count": 0,
                "ai_count_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["ai_count"] = 0
        user["ai_count_date"] = today

    plan = resolve_effective_plan(user, now_dt)
    active = user.get("active", True)
    genres = user.get("genres", [])
    qr = main_quick_reply()

    # ─── フィードバック受理（最優先・トライアル延長）───
    if user.get("feedback_pending") and not user.get("feedback_reward_used"):
        if looks_like_feedback(text):
            try:
                supabase.table("trial_feedbacks").insert({
                    "user_id": user_id,
                    "feedback": text,
                    "created_at": now_dt.isoformat(),
                }).execute()
            except Exception as e:
                logger.error("trial_feedbacks insert失敗: %s", e)

            try:
                supabase.table("users").update({
                    "trial_extended_until": (now_dt + timedelta(days=7)).isoformat(),
                    "feedback_reward_used": True,
                    "feedback_pending": False,
                }).eq("user_id", user_id).execute()
            except Exception as e:
                logger.error("延長付与失敗: %s", e)

            user["trial_extended_until"] = (now_dt + timedelta(days=7)).isoformat()
            user["feedback_reward_used"] = True
            user["feedback_pending"] = False

            reply_text(
                event.reply_token,
                "ありがとう\n感想ちゃんと受け取った\n\n"
                "freeでも使えるけど\n"
                "メンバーシップで機能を開放してる\n\n"
                "お礼であと1週間はこの状態で使えるようにした",
                quick_reply=qr,
            )
            return

    # ── 強コマンドは pending を無条件クリアしてから通常処理 ──
    if text in _STRONG_COMMANDS and user.get("pending_action"):
        _clear_pending(user_id)
        user["pending_action"] = None
        user["pending_count"] = None

    if text in _STOP_WORDS:
        supabase.table("users").update({"active": False}).eq("user_id", user_id).execute()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, "配信止めた\n再開したい時は「再開」って言って", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _START_WORDS:
        supabase.table("users").update({"active": True}).eq("user_id", user_id).execute()
        reply_text(event.reply_token, "配信再開した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in ["夜停止", "夜いらない"]:
        try:
            supabase.table("users").update({"night_delivery": False, "last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        reply_text(event.reply_token, "夜の配信止めた\n再開したいときは「夜再開」って言って", quick_reply=qr)
        return

    if text in ["夜開始","夜再開", "夜オン"]:
        if plan != "paid":
            reply_text(event.reply_token, "夜配信はメンバーシップでやってる", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        try:
            supabase.table("users").update({"night_delivery": True, "last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        reply_text(event.reply_token, "夜の配信再開したよ", quick_reply=qr)
        return

    if any(kw in text for kw in _MEMBERSHIP_KW):
        if any(w in text for w in ["いくら", "料金", "値段"]):
            reply_text(
                event.reply_token,
                "月400円でやってる\n\n"
                "メンバーシップだと\n"
                "・ニュースを深掘りできる\n"
                "・会話ネタの返しまで出せる\n"
                "・相手に合わせた話題も出せる",
                quick_reply=qr,
            )
        elif any(w in text for w in ["入り方", "登録"]):
            _reg_url = f"\n\n👇ここから入れる\n{PAYMENT_URL}" if PAYMENT_URL else ""
            reply_with_payment(
                event.reply_token,
                "メンバーシップってやつで見れるようにしてる" + _reg_url,
                quick_reply=qr,
            )
        elif any(w in text for w in ["何できる", "何ができる"]):
            if plan == "paid":
                _msg = (
                    "今使えるのはこんな感じ\n\n"
                    "・ニュースを深掘りできる\n"
                    "・会話ネタの返しまで出せる\n"
                    "・相手に合わせた話題も出せる\n\n"
                    "そのまま送ればOK"
                )
            else:
                _msg = (
                    "無料でも使えるけど\n\n"
                    "メンバーシップだと\n"
                    "・ニュースを深掘りできる\n"
                    "・会話ネタの返しまで出せる\n"
                    "・相手に合わせた話題も出せる\n\n"
                    "気になるならそのまま聞いてくれればOK\n\n"
                    "ちなみに\n"
                    "メンバーシップはここから",
                )
            reply_text(
                event.reply_token,
                _msg,
                quick_reply=qr,
            )
        else:
            reply_text(
                event.reply_token,
                "無料でも使えるけど\n\n"
                "メンバーシップだと\n"
                "・ニュースを深掘りできる\n"
                "・会話ネタの返しまで出せる\n"
                "・相手に合わせた話題も出せる\n\n"
                "気になるならそのまま聞いてくれればOK\n\n"
                "ちなみに\n"
                "メンバーシップはここから",
                quick_reply=qr,
            )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "使い方":
        _genre_text = format_genres(genres) if genres else "未設定（すべて配信）"
        if normalize_plan(plan) != "free":
            _setting_lines = (
                f"・プラン：メンバーシップ\n"
                f"・昼配信：{'オン' if active else 'オフ'}\n"
                f"・夜配信：{'オン' if user.get('night_delivery', True) else 'オフ'}\n"
                f"・ジャンル：{_genre_text}"
            )
        else:
            _setting_lines = (
                f"・プラン：無料（基本機能）\n"
                f"・配信：{'オン' if active else '停止中'}\n"
                f"・ジャンル：{_genre_text}"
            )
        reply_text(
            event.reply_token,
            "使い方\n\n"
            "【ニュースを見る】\n"
            "・例えば、「1」や「1と2」と入力 → ニュースの要約を表示\n"
            "・「1詳しく」や、要約を見た後に続けて「詳しく」で詳しい説明を表示\n"
            "・「1-3-5」みたいな複数指定OK\n"
            "・「リンク」→ 元記事のURLを表示\n\n"
            "【追加でニュースを見る】\n"
            "・「他には」→ 別のニュースを表示\n\n"
            "【ニュースについて質問する】\n"
            "・ニュースのタイトルやキーワードを送る → そのまま解説できる\n\n"
            "ーーー\n\n"
            "【会話ネタを使う】\n"
            "・「会話」→ 会話で使える話題を表示\n\n"
            "※メンバーシップの場合\n"
            "・ニュースの理解に加えて、会話で使える「返し」まで出る\n"
            "・誰と話すか入力すると相手に合わせた話題が出せる\n"
            "例：彼女 / 上司 / お客さん など\n\n"
            "→ そのまま会話で使えるレベルまで仕上がる\n"
            "→ 登録は案内メッセージからそのまま進められる\n\n"
            "ーーー\n\n"
            "【プランについて】\n\n"
            "無料でも基本機能は使える\n\n"
            "メンバーシップでは\n"
            "・朝と夜の2回ニュースが届く\n"
            "・追加で見られるニュースが増える\n"
            "・会話ネタ、ニュースQ＆Aが上限数が増える\n\n"
            "メンバーシップ加入はここから\n\n"
            "ーーー\n\n"
            "【配信の操作】\n"
            "・「止めて」→ 配信停止\n"
            "・「再開」→ 配信再開\n"
            "・「夜停止」→ 夜配信だけ止める\n"
            "・「夜開始」→ 夜配信だけ再開\n\n"            
            "ーーー\n\n"
            "【現在の設定】\n"
            f"{_setting_lines}\n\n"
            "ーーー\n\n"
            "そのままテキストを送れば操作できる",
            quick_reply=qr,
        )
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

    if text == "プラン":
        reply_text(event.reply_token, _plan_status_text(plan, active, genres, night_delivery=user.get("night_delivery", True)), quick_reply=qr)
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
                "ジャンル認識できなかった\n例: ジャンル 経済,AI・テック,スポーツ",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        supabase.table("users").update({"genres": new_genres}).eq("user_id", user_id).execute()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, f"ジャンル変えた: {format_genres(new_genres)}", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _STATUS_WORDS:
        reply_text(event.reply_token, _plan_status_text(plan, active, genres, night_delivery=user.get("night_delivery", True)), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ─── トライアル終了通知（初回のみ）───
    _base_plan = normalize_plan(user.get("plan", "free"))
    if (plan == "free" and _base_plan == "free"
            and user.get("trial_started_at")
            and not user.get("feedback_pending")
            and not user.get("feedback_reward_used")):
        reply_with_payment(
            event.reply_token,
            "トライアルはここまで\n\n"
            "free会員でもニュースは見れるけど\n"
            "メンバーシップなら、会話ネタとか深掘りまで全部使える\n\n"
            "よければ感想もらえたら\n"
            "もう1週間トライアルが延長できる\n\n"
            "コピペして軽くでOK👇\n\n"
            "【使いやすさ】\n"
            "（例：使いやすい / わかりにくい）\n\n"
            "【よかった点】\n\n"
            "【微妙だった点】\n\n"
            "【あったらいい機能】\n\n"
            "一言でもOK",
            quick_reply=qr,
        )
        try:
            supabase.table("users").update({
                "feedback_pending": True,
                "last_reply_time": now_dt.isoformat(),
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ─── 延長トライアル終了通知（1回のみ）───
    _base_plan = normalize_plan(user.get("plan", "free"))
    extended_until = user.get("trial_extended_until")
    if (
        plan == "free"
        and _base_plan == "free"
        and user.get("feedback_reward_used") == True
        and extended_until
        and not user.get("extended_trial_ended_notified")
    ):
        try:
            ext_dt = datetime.fromisoformat(str(extended_until).replace("Z", "+00:00"))
        except Exception:
            ext_dt = None

        if ext_dt and now_dt > ext_dt:
            try:
                supabase.table("users").update({
                    "extended_trial_ended_notified": True
                }).eq("user_id", user_id).execute()
            except Exception:
                pass

            user["extended_trial_ended_notified"] = True

            reply_with_payment(
                event.reply_token,
                "延長分はここまで\n\n"
                "freeでもニュースは見れるけど\n"
                "メンバーシップなら、会話ネタとか深掘りまで全部使える\n\n"
                "よかったらこのまま続けてみて",
                quick_reply=qr,
            )
            return

    # ★2.4 相手別会話ネタ直発火（「彼女と会話」のような一発入力）
    if is_direct_person_chat_request(text):
        if plan == "paid":
            if not can_use_paid_ai(user, plan):
                reply_text(
                    event.reply_token,
                    "今日はここまでにしよ\nまた明日使えるようになってる",
                    quick_reply=qr,
                )
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            if user.get("pending_action"):
                _clear_pending(user_id)
                user["pending_action"] = None
                user["pending_count"] = None
            answer = generate_chat_for_person(user_id, text)
            next_count = user.get("ai_count", 0) + 1
            reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        else:
            reply_text(
                event.reply_token,
                "相手に合わせた話題は\nメンバーシップで使える",
                quick_reply=qr,
            )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★2.5 会話ネタ完全一致トリガー（Q&Aより前に処理）
    if text in _CHAT_TOPIC_EXACT or any(kw in text for kw in _CHAT_TOPIC_KW):
        if plan == "paid":
            if not can_use_paid_ai(user, plan):
                reply_text(
                    event.reply_token,
                    "今日はここまでにしよ\nまた明日使えるようになってる",
                    quick_reply=qr,
                )
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            answer = generate_chat_topic_paid(user_id)
            next_count = user.get("ai_count", 0) + 1
            reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
            _set_pending_person_topic(user_id)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        else:
            if user.get("free_chat_topic_used", False):
                reply_text(
                    event.reply_token,
                    "free版の会話ネタは1日1回まで\n\n続きはメンバーシップで使える",
                    quick_reply=qr,
                )
            else:
                answer = generate_chat_topic_free(user_id)
                reply_text(event.reply_token, answer, quick_reply=qr)
                try:
                    supabase.table("users").update({
                        "free_chat_topic_used": True,
                        "free_chat_topic_date": today,
                        "last_reply_time": now_dt.isoformat(),
                    }).eq("user_id", user_id).execute()
                    user["free_chat_topic_used"] = True
                except Exception:
                    pass
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
        return

    # ★3 相手別会話ネタ（paid only）— pending中の次の1ターン限定
    if plan == "paid" and _is_pending_person_topic(user):
        _qa_would_fire = bool(re.match(r"^[①-⑩1-9]", text)) or is_related_to_news_context(user_id, text)
        if not _qa_would_fire:
            if not can_use_paid_ai(user, plan):
                reply_text(
                    event.reply_token,
                    "今日はここまでにしよ\nまた明日使えるようになってる",
                    quick_reply=qr,
                )
                _clear_pending(user_id)
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            answer = generate_chat_for_person(user_id, text)
            next_count = user.get("ai_count", 0) + 1
            reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
            _clear_pending(user_id)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
            return
        # _qa_would_fire の場合は ★7 Q&A 側でクリア

    if is_followup(text):
        ctx = get_latest_news_context(user_id)
        if not ctx or not _is_context_alive(ctx):
            reply_text(event.reply_token, "先に聞くでニュース出して", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        payload = ctx.get("payload", {})
        news_items = payload.get("news_items", [])
        extra_items = payload.get("extra_items", [])
        idx = payload.get("extra_index", 0)
        count = payload.get("extra_count", 0)

        if plan == "free" and count >= 1:
            reply_text(event.reply_token, "他の記事はメンバーシップで見れるようにしてる", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        if plan == "paid" and count >= 3:
            reply_text(event.reply_token, "今のところはこんなもんかな\n\n気になるやつあればそのまま聞いて", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        shown = extra_items[idx:idx + 5]

        answer = _answer_more_news(shown, len(news_items) + idx + 1)
        reply_text(event.reply_token, answer, quick_reply=qr)

        if shown:
            new_payload = {**payload, "extra_index": idx + len(shown), "extra_count": count + 1}
            try:
                supabase.table("news_contexts").insert({
                    "user_id": user_id,
                    "sent_at": now_dt.isoformat(),
                    "payload": new_payload,
                }).execute()
            except Exception:
                pass
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★5 blocklist
    if any(w in text for w in _BLOCKLIST):
        reply_text(event.reply_token, _BLOCKLIST_TEXT, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★6 ALL_LINK（リンク一覧）
    if any(kw in text for kw in _ALL_LINK_KW) and not parse_article_numbers(text, max_n=10):
        ctx = get_latest_news_context(user_id)
        if not ctx or not _is_context_alive(ctx):
            reply_text(event.reply_token, "先に聞くでニュース出して", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        _ctx_payload = ctx.get("payload", {})
        _news_items = _ctx_payload.get("news_items", [])
        _extra_items = _ctx_payload.get("extra_items", [])
        _visible_extra = _ctx_payload.get("extra_index", 0)
        _all_link_items = _news_items + _extra_items[:_visible_extra]
        lines = ["まとめてどうぞ👇"]
        for item in _all_link_items:
            idx = item.get("index", 0)
            lines.append(f"{idx}. {item.get('link', '')}")
        reply_text(event.reply_token, "\n".join(lines), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★7 ニュースQ&A（番号最優先＋文脈＋自然文）

    # 「詳しく」単体 → 直前の対象記事に再発火（pending機能）
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

    _is_number_start = bool(re.match(r"^[①-⑩1-9]", text))
    _matched_by_ctx = is_related_to_news_context(user_id, text)

    if _is_number_start or _matched_by_ctx:
        # 文脈マッチのみ（番号でも質問語でもない）→ 弾く
        if _matched_by_ctx and not _is_number_start and not is_news_question(text) and not _looks_like_question_or_command(text):
            reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        free_reply_used = user.get("free_reply_used", False)

        if plan == "paid":
            if not can_use_paid_ai(user, plan):
                reply_text(
                    event.reply_token,
                    "今日はここまでにしよ\nまた明日使えるようになってる",
                    quick_reply=qr,
                )
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
            next_count = user.get("ai_count", 0) + 1
            reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
            if _q_targets:
                save_last_news_question_targets(user_id, _q_targets, get_latest_news_context(user_id))

        elif not free_reply_used:
            answer, _q_targets = answer_news_question(user_id, text)
            msg = f"詳しくはこんな感じ👇\n\n{answer}\n\nこの先はもう少し深く見ることもできる"
            reply_text(event.reply_token, msg, quick_reply=qr)
            try:
                supabase.table("users").update({
                    "free_reply_used": True,
                    "last_reply_time": now_dt.isoformat(),
                }).eq("user_id", user_id).execute()
                user["free_reply_used"] = True
            except Exception:
                pass
            if _q_targets:
                save_last_news_question_targets(user_id, _q_targets, get_latest_news_context(user_id))

        else:
            reply_text(
                event.reply_token,
                "free版は1回だけ深掘り対応してる\nメンバーシップで続けられるよ",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
        return


    # ★9 fallback
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

        if any(c in genres for c in internals):
            genres = [c for c in genres if c not in internals]
        else:
            for cat in internals:
                if cat not in genres:
                    genres.append(cat)

        supabase.table("users").update({"genres": genres}).eq("user_id", user_id).execute()
        reply_flex(event.reply_token, build_genre_flex(genres))

    elif data == "clear_genres":
        supabase.table("users").update({"genres": []}).eq("user_id", user_id).execute()
        reply_flex(event.reply_token, build_genre_flex([]))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))