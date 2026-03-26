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

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]

app = Flask(__name__)

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── ログ ───
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── ジャンル ───
ALLOWED_GENRES = {
    "ai": "ai", "人工知能": "ai",
    "economy": "economy", "経済": "economy",
    "sports": "sports", "スポーツ": "sports",
    "construction": "construction", "建設": "construction",
    "real_estate": "real_estate", "不動産": "real_estate",
    "interest_rates": "interest_rates", "金利": "interest_rates",
    "energy": "energy", "エネルギー": "energy",
    "business": "business", "ビジネス": "business",
    "tech": "tech", "テック": "tech",
}

# ─── DB操作 ───

def get_user(user_id):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def save_user(user_id, active=True, plan="free", genres=None):
    if genres is None:
        genres = []

    supabase.table("users").upsert({
        "user_id": user_id,
        "active": active,
        "plan": plan,
        "genres": genres
    }).execute()

    logger.info(f"Supabase保存: {user_id}")


def ensure_user(user_id):
    user = get_user(user_id)
    if not user:
        save_user(user_id)
        logger.info(f"新規ユーザー登録: {user_id}")
        return {"plan": "free", "active": True, "genres": []}
    return user


# ─── ユーティリティ ───

def normalize_genres(raw_text: str):
    items = [x.strip().lower() for x in raw_text.split(",") if x.strip()]
    return [ALLOWED_GENRES[x] for x in items if x in ALLOWED_GENRES]


def plan_rules(plan: str):
    return {
        "free": {"max_items": 3, "max_genres": 0},
        "light": {"max_items": 5, "max_genres": 2},
        "premium": {"max_items": 8, "max_genres": 6},
    }.get(plan, {"max_items": 3, "max_genres": 0})


def reply_text(reply_token, text):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)]
            )
        )


# ─── Webhook ───

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return "OK"


@handler.add(FollowEvent)
def follow(event):
    user_id = event.source.user_id
    ensure_user(user_id)

    reply_text(event.reply_token, "登録完了")


@handler.add(MessageEvent, message=TextMessageContent)
def handle(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    logger.info(f"受信: {user_id} {text}")

    user = ensure_user(user_id)
    plan = user.get("plan", "free")
    rules = plan_rules(plan)

    if text == "スタート":
        save_user(user_id, active=True, plan=plan, genres=user.get("genres", []))
        reply_text(event.reply_token, "開始した")
        return

    if text == "停止":
        save_user(user_id, active=False, plan=plan, genres=user.get("genres", []))
        reply_text(event.reply_token, "停止した")
        return

    if text == "ジャンル":
        reply_text(event.reply_token, f"現在: {user.get('genres', [])}")
        return

    if text.startswith("ジャンル "):
        genres = normalize_genres(text.replace("ジャンル ", ""))

        if rules["max_genres"] == 0:
            genres = []

        genres = genres[:rules["max_genres"]]

        save_user(user_id, active=True, plan=plan, genres=genres)

        reply_text(event.reply_token, f"更新: {genres}")
        return

    reply_text(event.reply_token, "OK")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))