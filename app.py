import logging
import os

from dotenv import load_dotenv
from flask import Flask, request, abort
from supabase import create_client

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import FollowEvent, MessageEvent, TextMessageContent

# ─── 初期設定 ───
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── ログ ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── ジャンル定義 ───
ALLOWED_GENRES = {
    "不動産": "real_estate",
    "real_estate": "real_estate",

    "建築": "construction",
    "建設": "construction",
    "construction": "construction",

    "金利": "interest_rates",
    "interest_rates": "interest_rates",

    "資材": "materials",
    "建材": "materials",
    "materials": "materials",

    "経済": "economy",
    "economy": "economy",

    "ai": "ai",
    "AI": "ai",
    "人工知能": "ai",

    "テック": "tech",
    "tech": "tech",
    "IT": "tech",

    "ビジネス": "business",
    "business": "business",

    "エネルギー": "energy",
    "energy": "energy",

    "スポーツ": "sports",
    "sports": "sports",

    "スキャンダル": "scandal",
    "scandal": "scandal",

    "芸能": "entertainment",
    "entertainment": "entertainment",
}

GENRE_LABELS = {
    "real_estate": "不動産",
    "construction": "建築",
    "interest_rates": "金利",
    "materials": "資材",
    "economy": "経済",
    "ai": "AI",
    "tech": "テック",
    "business": "ビジネス",
    "energy": "エネルギー",
    "sports": "スポーツ",
    "scandal": "スキャンダル",
    "entertainment": "芸能",
}

# テスト中はfreeでも選べるようにしておく
def plan_rules(plan: str):
    return {
        "free": {"max_items": 3, "max_genres": 10},
        "light": {"max_items": 5, "max_genres": 10},
        "premium": {"max_items": 8, "max_genres": 12},
    }.get(plan, {"max_items": 3, "max_genres": 10})


# ─── DB操作 ───
def get_user(user_id: str):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def save_user(user_id: str, active=True, plan="free", genres=None):
    if genres is None:
        genres = []

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
        return {"user_id": user_id, "active": True, "plan": "free", "genres": []}
    return user


# ─── 補助 ───
def normalize_genres(raw_text: str):
    normalized_text = raw_text.replace("\u3000", " ")
    items = [x.strip() for x in normalized_text.split(",") if x.strip()]
    result = []

    lower_map = {k.lower(): v for k, v in ALLOWED_GENRES.items()}

    for item in items:
        mapped = lower_map.get(item.lower())
        if mapped and mapped not in result:
            result.append(mapped)

    return result


def format_genres(genres):
    if not genres:
        return "なし"
    return ", ".join(GENRE_LABELS.get(g, g) for g in genres)


def reply_text(reply_token, text):
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            api.reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[TextMessage(text=text)],
                )
            )
    except Exception as e:
        logger.error("LINE返信エラー: %s", e)


# ─── Webhook ───
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        logger.warning("署名検証失敗")
        abort(400)

    return "OK"


@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    ensure_user(user_id)

    reply_text(
        event.reply_token,
        "登録完了\n\n"
        "使い方:\n"
        "・開始\n"
        "・停止\n"
        "・プラン\n"
        "・ジャンル\n"
        "・ジャンル 不動産,建築,金利\n\n"
        "設定可能:\n"
        "不動産, 建築, 金利, 資材, 経済, AI, テック, ビジネス, エネルギー, スポーツ, スキャンダル, 芸能"
    )


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)

    user = ensure_user(user_id)
    plan = user.get("plan", "free")
    active = user.get("active", True)
    genres = user.get("genres", [])
    rules = plan_rules(plan)

    if text in ["開始", "スタート", "再開"]:
        save_user(user_id, active=True, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信を開始した")
        return

    if text == "停止":
        save_user(user_id, active=False, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信を停止した")
        return

    if text == "プラン":
        reply_text(
            event.reply_token,
            f"plan: {plan}\n"
            f"active: {active}\n"
            f"genres: {format_genres(genres)}\n"
            f"max_items: {rules['max_items']}\n"
            f"max_genres: {rules['max_genres']}"
        )
        return

    if text == "ジャンル":
        reply_text(
            event.reply_token,
            "設定可能ジャンル:\n"
            "不動産, 建築, 金利, 資材, 経済, AI, テック, ビジネス, エネルギー, スポーツ, スキャンダル, 芸能\n\n"
            f"現在: {format_genres(genres)}\n\n"
            "例:\n"
            "ジャンル 不動産,建築,金利"
        )
        return

    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        new_genres = normalize_genres(raw)

        if not new_genres:
            reply_text(
                event.reply_token,
                "ジャンル認識できなかった\n"
                "例:\n"
                "ジャンル 不動産,建築,金利"
            )
            return

        new_genres = new_genres[:rules["max_genres"]]

        save_user(user_id, active=True, plan=plan, genres=new_genres)

        reply_text(
            event.reply_token,
            f"ジャンル更新: {format_genres(new_genres)}"
        )
        return

    reply_text(
        event.reply_token,
        "使えるコマンド:\n"
        "・開始\n"
        "・停止\n"
        "・プラン\n"
        "・ジャンル\n"
        "・ジャンル 不動産,建築,金利"
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))