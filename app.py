import logging
import os

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

# ─── ジャンル定義 ───
# 表示ジャンル → 内部カテゴリ配列
DISPLAY_GENRE_MAP = {
    "お金":     ["economy", "interest_rates", "real_estate"],
    "仕事":     ["business", "construction"],
    "世界":     ["international"],
    "AI":       ["ai", "tech"],
    "暮らし":   ["energy", "materials"],
    "話題":     ["entertainment", "scandal", "other"],
    "スポーツ": ["sports"],
}

# 内部カテゴリ → 表示ジャンル（逆引き）
INTERNAL_TO_DISPLAY = {
    cat: display
    for display, cats in DISPLAY_GENRE_MAP.items()
    for cat in cats
}

DISPLAY_GENRE_ORDER = list(DISPLAY_GENRE_MAP.keys())  # 表示順固定


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
    """表示ジャンル名 → 内部カテゴリ配列に展開する"""
    text = raw_text.replace("\u3000", " ")
    items = [x.strip() for x in text.split(",") if x.strip()]
    result = []
    lower_map = {k.lower(): k for k in DISPLAY_GENRE_MAP}
    for item in items:
        display = lower_map.get(item.lower())
        if display:
            for cat in DISPLAY_GENRE_MAP[display]:
                if cat not in result:
                    result.append(cat)
    return result


def format_genres(genres):
    """内部カテゴリ配列 → 重複のない表示ジャンル名に変換する"""
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
        QuickReplyItem(action=MessageAction(label="開始", text="開始")),
        QuickReplyItem(action=MessageAction(label="停止", text="停止")),
        QuickReplyItem(action=MessageAction(label="ジャンル", text="ジャンル")),
        QuickReplyItem(action=MessageAction(label="プラン", text="プラン")),
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


def build_genre_flex(current_genres: list) -> FlexMessage:
    """表示ジャンル単位のトグルパネル。タップで選択/解除、再送信で状態反映。"""
    rows = []
    for i in range(0, len(DISPLAY_GENRE_ORDER), 3):
        chunk = DISPLAY_GENRE_ORDER[i:i + 3]
        buttons = []
        for display in chunk:
            internals = DISPLAY_GENRE_MAP[display]
            selected = any(c in current_genres for c in internals)
            buttons.append(FlexButton(
                action=PostbackAction(
                    label=f"✓{display}" if selected else display,
                    data=f"toggle_display_genre:{display}",
                    display_text=display,
                ),
                style="primary" if selected else "secondary",
                height="sm",
                flex=1,
            ))
        # 最終行が3未満の場合は空Boxでパディング
        while len(buttons) < 3:
            buttons.append(FlexBox(layout="vertical", contents=[], flex=1))
        rows.append(FlexBox(layout="horizontal", contents=buttons, spacing="xs"))

    header_note = f"現在: {format_genres(current_genres)}" if current_genres else "未選択（全ジャンル配信）"

    bubble = FlexBubble(
        header=FlexBox(
            layout="vertical",
            contents=[
                FlexText(text="ジャンル選択", weight="bold", size="md"),
                FlexText(text=header_note, size="xs", color="#888888", wrap=True),
            ],
        ),
        body=FlexBox(
            layout="vertical",
            contents=rows,
            spacing="xs",
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    action=PostbackAction(
                        label="クリア（全解除）",
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
    flex_msg = FlexMessage(alt_text="ジャンル選択", contents=bubble)
    flex_msg.quick_reply = main_quick_reply()
    return flex_msg


# ─── ニュースQ&A ───

def get_latest_news_context(user_id: str):
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


_URL_KEYWORDS    = ["URL", "url", "リンク", "記事"]
_DETAIL_KEYWORDS = ["詳しく", "もう少し", "なんで", "なぜ", "具体的に", "仕組み"]
_NUM_MAP = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
            "１": 1, "２": 2, "３": 3, "４": 4, "５": 5}
_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"


def _answer_url(question: str, news_items: list) -> str:
    """URL系の質問に対してリンクを返す"""
    # 番号指定チェック
    num = next((n for ch, n in _NUM_MAP.items() if ch in question), None)

    if num is not None:
        item = next((n for n in news_items if n.get("index") == num), None)
        if not item:
            return f"{num}番目のニュースが見つからなかった"
        link = item.get("link", "")
        circle = _CIRCLED[num - 1] if num <= len(_CIRCLED) else str(num)
        return f"{circle}の元記事\n{link}" if link else "この記事は元リンクが取れなかった"

    # 全体
    lines = ["元記事リンク", ""]
    for item in news_items:
        idx  = item.get("index", 0)
        link = item.get("link", "")
        circle = _CIRCLED[idx - 1] if 0 < idx <= len(_CIRCLED) else str(idx)
        lines.append(f"{circle} {link}" if link else f"{circle} (リンクなし)")
    return "\n".join(lines)


def answer_news_question(user_id: str, question: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return (
            "まだニュース履歴がないから答えられないかも\n"
            "一度配信を受けてから聞いてみて"
        )

    payload    = ctx.get("payload", {})
    news_items = payload.get("news_items", [])
    summary    = payload.get("summary", [])
    impact     = payload.get("impact", [])

    # URL系の質問はGPTを通さず直接返す
    if any(kw in question for kw in _URL_KEYWORDS):
        return _answer_url(question, news_items)

    is_detail = any(k in question for k in _DETAIL_KEYWORDS)

    news_text    = "\n".join(
        f"{n['index']}. 【{n['category']}】{n['title']}"
        f"（{n.get('reason', '')} / {n.get('interpretation', '')}）"
        for n in news_items
    )
    summary_text = "\n".join(f"・{s}" for s in summary)
    impact_text  = "\n".join(f"・{i}" for i in impact)

    system_prompt = (
        "お前はLINEでニュースを補足する相手。\n\n"
        "【共通ルール】\n"
        "・敬語禁止\n"
        "・会話調\n"
        "・結論から\n"
        "・1文短く\n"
        "・説明しすぎない\n"
        "・AIっぽい文章禁止\n"
        "・最後に誘導しない（公式サイト見て等は禁止）\n"
        "・断定しすぎない（〜っぽい、〜かも）\n\n"
        "【通常モード】1〜3文。短く一発で理解できる形。\n"
        "【詳細モード】3〜6文。結論→理由→もう一歩の構造。難しい言葉禁止。\n\n"
        "【禁止】です/ます口調、長文（6文以上）、教科書みたいな説明"
    )
    mode = "詳細モードで答えろ。" if is_detail else "通常モードで答えろ。"
    user_prompt = (
        f"直近ニュース:\n{news_text}\n\n"
        f"まとめ:\n{summary_text}\n\n"
        f"影響:\n{impact_text}\n\n"
        f"質問:\n{question}\n\n"
        f"{mode}短く答えろ。"
    )

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=400,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.error("Q&A OpenAI エラー: %s", e)
        return "今ちょっと返答うまくいかない\n少し置いてもう一回送って"


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
        "朝6時・夜8時にニュースを届けます。\n\n"
        "使い方:\n"
        "・開始 / 停止\n"
        "・ジャンル → ボタンで選べる\n"
        "・プラン → 現在の設定を確認\n\n"
        "選べるジャンル:\n"
        "お金 / 仕事 / 世界 / AI / 暮らし / 話題 / スポーツ",
        quick_reply=main_quick_reply(),
    )

    try:
        send_news_to_user(user_id)
    except Exception as e:
        logger.error("初回配信失敗: user=%s %s", user_id, e)


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
    qr = main_quick_reply()

    if text in ["開始", "スタート", "再開"]:
        save_user(user_id, active=True, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信を開始した", quick_reply=qr)
        return

    if text == "停止":
        save_user(user_id, active=False, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信を停止した", quick_reply=qr)
        return

    if text == "プラン":
        reply_text(
            event.reply_token,
            f"plan: {plan}\n"
            f"active: {'ON' if active else 'OFF'}\n"
            f"genres: {format_genres(genres)}\n"
            f"max_items: {rules['max_items']}件",
            quick_reply=qr,
        )
        return

    if text == "ジャンル":
        reply_flex(event.reply_token, build_genre_flex(genres))
        return

    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        new_genres = normalize_genres(raw)

        if not new_genres:
            reply_text(
                event.reply_token,
                "ジャンル認識できなかった\n"
                "例:\n"
                "ジャンル お金,AI,スポーツ",
                quick_reply=qr,
            )
            return

        save_user(user_id, active=True, plan=plan, genres=new_genres)
        reply_text(
            event.reply_token,
            f"ジャンル更新: {format_genres(new_genres)}",
            quick_reply=qr,
        )
        return

    answer = answer_news_question(user_id, text)
    reply_text(event.reply_token, answer, quick_reply=qr)


@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    data = event.postback.data

    logger.info("Postback受信: user=%s data=%s", user_id, data)

    user = ensure_user(user_id)
    plan = user.get("plan", "free")
    active = user.get("active", True)
    genres = list(user.get("genres", []) or [])

    if data.startswith("toggle_display_genre:"):
        display = data.split(":", 1)[1]
        internals = DISPLAY_GENRE_MAP.get(display, [])
        if any(c in genres for c in internals):
            # 選択中 → 全解除
            genres = [c for c in genres if c not in internals]
        else:
            # 未選択 → 全追加
            for cat in internals:
                if cat not in genres:
                    genres.append(cat)
        save_user(user_id, active=active, plan=plan, genres=genres)
        reply_flex(event.reply_token, build_genre_flex(genres))

    elif data == "clear_genres":
        save_user(user_id, active=active, plan=plan, genres=[])
        reply_flex(event.reply_token, build_genre_flex([]))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
