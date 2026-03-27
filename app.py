import logging
import os
import time
from typing import Optional

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
DISPLAY_GENRE_MAP = {
    "経済":     ["economy", "interest_rates", "real_estate"],
    "仕事":     ["business", "construction"],
    "国際":     ["international"],
    "AI・テック": ["ai", "tech"],
    "暮らし":   ["energy", "materials"],
    "話題":     ["entertainment", "scandal", "other"],
    "スポーツ": ["sports"],
}

INTERNAL_TO_DISPLAY = {
    cat: display
    for display, cats in DISPLAY_GENRE_MAP.items()
    for cat in cats
}

DISPLAY_GENRE_ORDER = list(DISPLAY_GENRE_MAP.keys())

# 別名 → 正式表示ジャンル名
DISPLAY_GENRE_ALIASES: dict = {
    "お金":     "経済",
    "世界":     "国際",
    "AI":       "AI・テック",
    "テック":   "AI・テック",
    "IT":       "AI・テック",
    "it":       "AI・テック",
    "芸能":     "話題",
    "エンタメ": "話題",
    "生活":     "暮らし",
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
    """表示ジャンル名（別名含む）→ 内部カテゴリ配列に展開する"""
    text = raw_text.replace("\u3000", " ")
    items = [x.strip() for x in text.split(",") if x.strip()]
    result = []
    lower_map = {k.lower(): k for k in DISPLAY_GENRE_MAP}
    alias_lower = {k.lower(): v for k, v in DISPLAY_GENRE_ALIASES.items()}
    for item in items:
        key = item.lower()
        display = lower_map.get(key)
        if not display:
            canonical = alias_lower.get(key)
            if canonical:
                display = canonical
        if display and display in DISPLAY_GENRE_MAP:
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
        QuickReplyItem(action=MessageAction(label="聞く", text="聞く")),
        QuickReplyItem(action=MessageAction(label="設定", text="設定")),
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
    """2列レイアウトのジャンルトグルパネル"""
    rows = []
    for i in range(0, len(DISPLAY_GENRE_ORDER), 2):
        chunk = DISPLAY_GENRE_ORDER[i:i + 2]
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
        rows.append(FlexBox(layout="horizontal", contents=buttons, spacing="sm"))

    header_note = f"現在: {format_genres(current_genres)}" if current_genres else "未選択なら全部届く"

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


_URL_KEYWORDS      = ["URL", "url", "リンク", "記事"]
_DETAIL_KEYWORDS   = ["詳しく", "もう少し", "なんで", "なぜ", "具体的に", "仕組み"]
_MAIN_MORE_KW      = ["ほかに", "他にニュース", "もっとニュース", "追加ニュース"]
_SUB_MORE_KW       = ["ほか", "他に", "もっと", "追加", "それ以外", "他にも"]
_NUM_MAP = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
            "１": 1, "２": 2, "３": 3, "４": 4, "５": 5}
_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"


def _parse_article_num(question: str, max_n: int = 5) -> Optional[int]:
    """「3番目」「最初」「②のやつ」などから番号を抽出する"""
    if any(w in question for w in ["最初", "1番目", "一番目", "1つ目", "①"]):
        return 1
    if any(w in question for w in ["最後", f"{max_n}番目"]):
        return max_n
    # ②③④⑤... の丸数字（①は上で処理済み）
    for i, ch in enumerate(_CIRCLED[1:max_n], 2):
        if ch in question:
            return i
    for ch, n in _NUM_MAP.items():
        if ch in question and n <= max_n:
            return n
    return None


def _answer_more_news(news_items: list, extra_items: list) -> str:
    sent_links = {n.get("link") for n in news_items}
    candidates = [n for n in extra_items if n.get("link") not in sent_links][:3]
    if not candidates:
        return "今日はこれ以上ストックないかも"
    lines = ["あとこれも出てる", ""]
    for i, n in enumerate(candidates):
        circle = _CIRCLED[i] if i < len(_CIRCLED) else f"{i + 1}."
        lines.append(f"{circle} {n.get('title', '')}")
        reason = n.get("reason", "")
        if reason:
            lines.append(f"→ {reason}")
        lines.append("")
    lines.append("気になるのあれば言って")
    return "\n".join(lines).rstrip()


def _answer_url(question: str, news_items: list) -> str:
    """URL系の質問に対してリンクを返す"""
    num = _parse_article_num(question, max_n=len(news_items))

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

    # 追加ニュース要求
    is_more_news = (
        any(k in question for k in _MAIN_MORE_KW)
        or (any(k in question for k in _SUB_MORE_KW) and "ニュース" in question)
    )
    # 誤爆ガード：「影響」「意味」「問題」を含む場合は通常Q&Aに流す
    if is_more_news and any(g in question for g in ["影響", "意味", "問題", "理由", "なぜ", "なんで"]):
        is_more_news = False
    if is_more_news:
        extra_items = payload.get("extra_items", [])
        return _answer_more_news(news_items, extra_items)

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

    return "OK"


@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    ensure_user(user_id)

    reply_text(
        event.reply_token,
        "追加ありがとう🙌\n"
        "よかったら使ってみて\n\n"
        "ニュースは朝5時と夜8時に届く\n"
        "朝早いからミュートでOK\n\n"
        "とりあえず直近のニュース流すから\n"
        "気になるやつあればそのまま聞いて\n"
        "番号でもいけるし、リンクも出せる\n\n"
        "ジャンルでも絞れるから\n"
        "必要ならあとで変えればOK👌",
        quick_reply=main_quick_reply(),
    )

    try:
        time.sleep(6)
        send_news_to_user(user_id)
    except Exception as e:
        logger.error("初回配信失敗: user=%s %s", user_id, e)


_STOP_WORDS   = {"停止", "止めて", "停止して", "配信止めて", "もういい", "オフ"}
_START_WORDS  = {"開始", "スタート", "再開", "もう一回", "配信して", "オン", "はじめて", "始めて"}
_GENRE_WORDS  = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}
_STATUS_WORDS = {"プラン", "状態", "今どんな感じ", "設定どうなってる", "今の設定"}
_HELP_WORDS   = {"聞く", "使い方", "何できる", "どう使うの", "何聞ける"}


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)

    user = ensure_user(user_id)
    plan = user.get("plan", "free")
    active = user.get("active", True)
    genres = user.get("genres", [])
    qr = main_quick_reply()

    # ── 停止 ──
    if text in _STOP_WORDS:
        save_user(user_id, active=False, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信止めた\n再開したい時は言って", quick_reply=qr)
        return

    # ── 開始 ──
    if text in _START_WORDS:
        save_user(user_id, active=True, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信再開した", quick_reply=qr)
        return

    # ── ヘルプ ──
    if text in _HELP_WORDS:
        reply_text(
            event.reply_token,
            "気になるニュースそのまま聞けばOK\n"
            "「3番目なに？」とかでもいける\n"
            "リンクだけ欲しい時も返せる\n\n"
            "ジャンル変えたい時もそのまま言って",
            quick_reply=qr,
        )
        return

    # ── 設定ボタン ──
    if text == "設定":
        reply_text(
            event.reply_token,
            "どうする？\nそのまま言ってもOK",
            quick_reply=qr,
        )
        return

    # ── ジャンル導線 ──
    if text in _GENRE_WORDS:
        reply_flex(event.reply_token, build_genre_flex(genres))
        return

    # ── ジャンルテキスト直接設定 ──
    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        new_genres = normalize_genres(raw)
        if not new_genres:
            reply_text(
                event.reply_token,
                "ジャンル認識できなかった\n例: ジャンル 経済,AI・テック,スポーツ",
                quick_reply=qr,
            )
            return
        save_user(user_id, active=True, plan=plan, genres=new_genres)
        reply_text(event.reply_token, f"ジャンル変えた: {format_genres(new_genres)}", quick_reply=qr)
        return

    # ── 状態確認 ──
    if text in _STATUS_WORDS:
        plan_label = {"free": "無料で動いてる", "light": "ライトで動いてる", "premium": "プレミアムで動いてる"}.get(plan, "無料で動いてる")
        active_label = "配信オン" if active else "配信オフ"
        genre_label = f"ジャンルは {format_genres(genres)}" if genres else "ジャンルは未設定（全部配信）"
        reply_text(
            event.reply_token,
            f"今こんな感じ\n\n{plan_label}\n{active_label}\n\n{genre_label}\n\n変えたければそのまま言って",
            quick_reply=qr,
        )
        return

    # ── Q&A（最後に処理） ──
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
