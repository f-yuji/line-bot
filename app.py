import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, abort

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

load_dotenv()

# ─── ログ設定 ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]

app = Flask(__name__)

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

USER_SETTINGS_PATH = Path("user_settings.json")

ALLOWED_GENRES = {
    "ai": "ai",
    "人工知能": "ai",
    "economy": "economy",
    "経済": "economy",
    "sports": "sports",
    "スポーツ": "sports",
    "construction": "construction",
    "建設": "construction",
    "real_estate": "real_estate",
    "不動産": "real_estate",
    "interest_rates": "interest_rates",
    "金利": "interest_rates",
    "energy": "energy",
    "エネルギー": "energy",
    "business": "business",
    "ビジネス": "business",
    "tech": "tech",
    "テック": "tech",
}


# ─── ユーザー設定の読み書き ───

def _default_user() -> dict:
    """新規ユーザーのデフォルト設定を返す"""
    return {
        "name": "user",
        "plan": "free",
        "active": True,
        "genres": [],
        "max_items": 3,
    }


def load_settings() -> dict:
    if not USER_SETTINGS_PATH.exists():
        return {"users": {}}
    try:
        with open(USER_SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("user_settings.json の読み込みに失敗: %s", e)
        return {"users": {}}


def save_settings(data: dict) -> None:
    try:
        with open(USER_SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("設定保存完了 (ユーザー数: %d)", len(data.get("users", {})))
    except OSError as e:
        logger.error("user_settings.json の書き込みに失敗: %s", e)


def ensure_user_exists(user_id: str) -> dict:
    data = load_settings()
    users = data.setdefault("users", {})

    if user_id not in users:
        users[user_id] = _default_user()
        save_settings(data)
        logger.info("新規ユーザー登録: %s", user_id)
    else:
        logger.debug("既存ユーザー: %s", user_id)

    return users[user_id]


def update_user(user_id: str, **kwargs) -> dict:
    data = load_settings()
    users = data.setdefault("users", {})

    if user_id not in users:
        users[user_id] = _default_user()

    users[user_id].update(kwargs)
    save_settings(data)
    return users[user_id]


def get_user(user_id: str) -> dict:
    data = load_settings()
    return data.get("users", {}).get(user_id, {})


# ─── ジャンル・プラン ───

def normalize_genres(raw_text: str) -> list[str]:
    items = [x.strip().lower() for x in raw_text.split(",") if x.strip()]
    result = []
    for item in items:
        mapped = ALLOWED_GENRES.get(item)
        if mapped:
            result.append(mapped)
    # 重複除去しつつ順序維持
    return list(dict.fromkeys(result))


def plan_rules(plan: str) -> dict:
    rules = {
        "free":    {"max_items": 3, "max_genres": 0},
        "light":   {"max_items": 5, "max_genres": 2},
        "premium": {"max_items": 8, "max_genres": 6},
    }
    return rules.get(plan, rules["free"])


# ─── LINE返信 ───

def reply_text(reply_token: str, text: str) -> None:
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
    ensure_user_exists(user_id)

    reply_text(
        event.reply_token,
        "登録完了\n"
        "現在は free プラン\n\n"
        "使い方:\n"
        "・プラン\n"
        "・ジャンル\n"
        "・ジャンル AI,経済\n"
        "・停止\n"
        "・再開",
    )


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    # ★ 修正: 変数定義を print より先に移動
    user_id = event.source.user_id
    text = event.message.text.strip()
    logger.info("メッセージ受信: user=%s text=%s", user_id, text)

    user = ensure_user_exists(user_id)
    plan = user.get("plan", "free")
    rules = plan_rules(plan)

    if text == "登録":
        ensure_user_exists(user_id)
        reply_text(event.reply_token, "登録済み")
        return

    if text == "停止":
        update_user(user_id, active=False)
        reply_text(event.reply_token, "配信を停止した")
        return

    if text == "再開":
        update_user(user_id, active=True)
        reply_text(event.reply_token, "配信を再開した")
        return

    if text == "プラン":
        current = get_user(user_id)
        reply_text(
            event.reply_token,
            f"plan: {current.get('plan', 'free')}\n"
            f"active: {current.get('active', True)}\n"
            f"genres: {', '.join(current.get('genres', [])) or 'なし'}\n"
            f"max_items: {current.get('max_items', 3)}",
        )
        return

    if text == "ジャンル":
        current = get_user(user_id)
        current_genres = current.get("genres", [])
        if rules["max_genres"] == 0:
            limit_text = "freeはジャンル指定不可"
        else:
            limit_text = f"最大{rules['max_genres']}個"
        reply_text(
            event.reply_token,
            f"現在のジャンル: {', '.join(current_genres) or 'なし'}\n"
            f"設定上限: {limit_text}\n\n"
            f"例:\nジャンル AI,経済",
        )
        return

    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        genres = normalize_genres(raw)

        if rules["max_genres"] == 0:
            update_user(user_id, genres=[], max_items=rules["max_items"])
            reply_text(
                event.reply_token,
                "freeプランはジャンル指定不可\n"
                "有料化後に解放する設計でOK",
            )
            return

        genres = genres[: rules["max_genres"]]
        update_user(user_id, genres=genres, max_items=rules["max_items"])
        reply_text(
            event.reply_token,
            f"ジャンル更新: {', '.join(genres) or 'なし'}",
        )
        return

    reply_text(
        event.reply_token,
        "使えるコマンド:\n"
        "・プラン\n"
        "・ジャンル\n"
        "・ジャンル AI,経済\n"
        "・停止\n"
        "・再開",
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)