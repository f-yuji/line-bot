import logging
import os
import re
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
        return {"user_id": user_id, "active": True, "plan": "free", "genres": []}
    user["plan"] = normalize_plan(user.get("plan", "free"))
    return user


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
_URL_KEYWORDS = ["URL", "url", "リンク", "記事"]
_DETAIL_KEYWORDS = ["詳しく", "もう少し", "なんで", "なぜ", "具体的に", "仕組み"]
_MAIN_MORE_KW = ["ほかに", "他にニュース", "もっとニュース", "追加ニュース"]
_SUB_MORE_KW = ["ほか", "他に", "もっと", "追加", "それ以外", "他にも"]
_FOLLOWUP_KW = ["他には", "別のニュース", "続き", "次"]

_CONTEXT_TOKEN_STOPWORDS = {
    "経済", "金利", "影響", "理由", "内容", "状況",
    "問題", "情報", "世界", "ニュース", "話題",
}

_NUM_MAP = {
    "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
    "１": 1, "２": 2, "３": 3, "４": 4, "５": 5,
}
_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"

_CONTEXT_TTL_HOURS = 6

_BLOCKLIST = [
    "付き合", "好き",
    "お前誰", "何者", "自己紹介",
]

_REJECT_TEXT = "ニュースの内容で気になることあれば聞いて\n番号やリンクでもいけるよ"
_BLOCKLIST_TEXT = "ニュースの話で聞いてくれな"

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
    if any(w in question for w in ["最初", "1番目", "一番目", "1つ目", "①"]):
        return 1
    if any(w in question for w in ["最後", f"{max_n}番目"]):
        return max_n

    for i, ch in enumerate(_CIRCLED[1:max_n], 2):
        if ch in question:
            return i

    for ch, n in _NUM_MAP.items():
        if ch in question and n <= max_n:
            return n

    return None


def extract_number(text: str) -> Optional[int]:
    return _parse_article_num(text, max_n=5)


def is_followup(text: str) -> bool:
    return any(kw in text for kw in _FOLLOWUP_KW)


def _looks_like_article_reference(text: str) -> bool:
    if _parse_article_num(text, max_n=10) is not None:
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
    if _parse_article_num(text, max_n=10) is not None:
        return True
    if any(kw in text for kw in _URL_KEYWORDS):
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

    return False


def _answer_more_news(news_items: list, extra_items: list) -> str:
    sent_links = {n.get("link") for n in news_items}
    candidates = [n for n in extra_items if n.get("link") not in sent_links][:3]
    if not candidates:
        return "この辺はメンバーシップで見れるようになってる\n\n気になるならそのまま聞いてくれればOK"

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
    num = _parse_article_num(question, max_n=len(news_items))

    if num is not None:
        item = next((n for n in news_items if n.get("index") == num), None)
        if not item:
            return f"{num}番目のニュースが見つからなかった"
        link = item.get("link", "")
        circle = _CIRCLED[num - 1] if num <= len(_CIRCLED) else str(num)
        return f"{circle}の元記事\n{link}" if link else "この記事は元リンクが取れなかった"

    lines = ["元記事リンク", ""]
    for item in news_items:
        idx = item.get("index", 0)
        link = item.get("link", "")
        circle = _CIRCLED[idx - 1] if 0 < idx <= len(_CIRCLED) else str(idx)
        lines.append(f"{circle} {link}" if link else f"{circle} (リンクなし)")
    return "\n".join(lines)


def answer_news_question(user_id: str, question: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がないから答えられないかも\n一度配信を受けてから聞いてみて"

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])
    extra_items = payload.get("extra_items", [])
    summary = payload.get("summary", [])
    impact = payload.get("impact", [])

    if any(kw in question for kw in _URL_KEYWORDS):
        return _answer_url(question, news_items)

    is_more_news = (
        any(k in question for k in _MAIN_MORE_KW)
        or (any(k in question for k in _SUB_MORE_KW) and "ニュース" in question)
    )
    if is_more_news and any(g in question for g in ["影響", "意味", "問題", "理由", "なぜ", "なんで"]):
        is_more_news = False
    if is_more_news:
        return _answer_more_news(news_items, extra_items)

    is_detail = any(k in question for k in _DETAIL_KEYWORDS)

    news_text = "\n".join(
        f"{n['index']}. 【{n['category']}】{n['title']}（{n.get('reason', '')} / {n.get('interpretation', '')}）"
        for n in news_items
    )
    summary_text = "\n".join(f"・{s}" for s in summary)
    impact_text = "\n".join(f"・{i}" for i in impact)

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
        "【単語・用語質問】「金利とは」「半導体って？」みたいな単語系は\n"
        "  一般的な簡単な説明（1文）＋直近ニュースとの関連（1〜2文）で答えろ。\n"
        "  辞書説明だけで終わるな。「分かりません」「ニュースにありません」で終わるな。\n\n"
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
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=400,
            timeout=15,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.error("Q&A OpenAI エラー: %s", e)
        return "今ちょっと返答うまくいかない\n少し置いてもう一回送って"


_CHAT_TOPIC_FOLLOW_UP_FREE = (
    "\n\n相手に合わせた話題は\n"
    "メンバーシップで使える"
)

_CHAT_TOPIC_FOLLOW_UP_PAID = (
    "\n\nちなみに今日誰かと話す予定ある？\n\n"
    "どんな人か教えてくれれば\n"
    "その人に合わせて話題出すよ\n\n"
    "ざっくりでもいいけど\n"
    "詳しいほど精度上がる"
)


def generate_chat_topic_free(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    system_prompt = (
        "お前はLINEで使える会話ネタを提案する相手。\n\n"
        "【ルール】\n・敬語禁止\n・会話調\n・1文短く\n・すぐ使える形で出す\n・AIっぽい文章禁止\n\n"
        "【出力形式】\n"
        "カテゴリ名\n"
        "「会話フレーズ（自然に話を振れる1文）」\n\n"
        "・広げやすいポイント（1〜2行）"
    )
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んで、上記フォーマットで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=200,
            timeout=15,
        )
        return res.choices[0].message.content.strip() + _CHAT_TOPIC_FOLLOW_UP_FREE
    except Exception as e:
        logger.error("会話ネタ生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


def generate_chat_topic_paid(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    system_prompt = (
        "お前はLINEで使える会話ネタを提案する相手。\n\n"
        "【ルール】\n・敬語禁止\n・会話調\n・1文短く\n・すぐ使える形で出す\n・AIっぽい文章禁止\n\n"
        "【出力形式】\n"
        "カテゴリ名\n"
        "「会話フレーズ（自然に話を振れる1文）」\n\n"
        "・広げやすいポイント（1〜2行）\n\n"
        "よくある返し\n"
        "「相手の典型的な返答（1文）」\n\n"
        "→「次の一手（すぐ使える短い質問形式）」"
    )
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んで、上記フォーマットで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
        )
        return res.choices[0].message.content.strip() + _CHAT_TOPIC_FOLLOW_UP_PAID
    except Exception as e:
        logger.error("会話ネタ生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


def generate_chat_for_person(user_id: str, person_desc: str) -> str:
    ctx = get_latest_news_context(user_id)
    news_text = ""
    if ctx:
        news_items = ctx.get("payload", {}).get("news_items", [])
        news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    system_prompt = (
        "お前はLINEで使える会話ネタを相手に合わせて提案する相手。\n\n"
        "【ルール】\n・敬語禁止\n・会話調\n・1文短く\n・すぐ使える形で出す\n・AIっぽい文章禁止\n"
        "・ニュースに縛られず相手に最適な話題を選ぶ\n\n"
        "【出力形式】\n"
        "（相手に最適化された話題）\n\n"
        "・使いやすいポイント（1〜2行）\n\n"
        "よくある返し\n"
        "「相手の典型的な返答（1文）」\n\n"
        "→「次の一手（すぐ使える短い質問形式）」"
    )
    user_prompt = (
        f"話す相手: {person_desc}\n\n"
        + (f"参考ニュース（必要なら使っていい）:\n{news_text}\n\n" if news_text else "")
        + "この相手に使えそうな会話ネタを1つ、上記フォーマットで出せ。"
    )
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.error("相手別会話生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


def can_use_paid_ai(user: dict) -> bool:
    return user.get("plan") == "paid" and user.get("ai_count", 0) < 10


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
        "追加ありがとう\n"
        "よかったら使ってみて🙌\n\n"
        "ニュースは朝起きる前に届く\n"
        "きっと寝てる間だからミュートでOK\n\n"
        "ジャンルでも絞れるから\n"
        "必要ならあとで変えればOK👌\n\n"
        "とりあえず直近のニュース流すから\n"
        "気になるやつあればそのまま聞いて\n"
        "番号でもいけるし、リンクも出せる",
        quick_reply=main_quick_reply(),
    )

    try:
        send_news_to_user(user_id)
    except Exception as e:
        logger.error("初回配信失敗: user=%s %s", user_id, e)


_STOP_WORDS = {"停止", "止めて", "停止して", "配信止めて", "もういい", "オフ"}
_START_WORDS = {"開始", "スタート", "再開", "もう一回", "配信して", "オン", "はじめて", "始めて"}
_GENRE_WORDS = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}
_MEMBERSHIP_KW = [
    "メンバー", "メンバーシップ", "有料", "課金",
    "いくら", "料金", "値段",
    "どうやる", "入り方", "登録",
    "何できる", "何ができる",
]
_STATUS_WORDS = {"状態", "今どんな感じ", "設定どうなってる", "今の設定"}
_HELP_WORDS = {"聞く", "使い方", "何できる", "どう使うの", "何聞ける"}
_CHAT_TOPIC_KW = ["会話ネタ", "話のネタ", "雑談ネタ", "ネタ教えて", "何話せばいい", "何話す"]
_PERSON_KW = ["営業", "取引先", "上司", "部下", "先輩", "後輩", "同僚", "友達", "初対面", "客", "顧客", "彼女", "彼氏", "親", "家族"]
_PERSON_REQUEST_KW = ["会話ネタ", "話のネタ", "何話す", "何話せばいい", "雑談ネタ", "ネタ教えて", "話振る", "会話", "話題"]


def _plan_status_text(plan: str, active: bool, genres: list) -> str:
    plan = normalize_plan(plan)
    plan_label = "メンバーシップ" if plan != "free" else "無料プラン"
    active_label = "配信オン" if active else "配信オフ"
    genre_label = f"ジャンル: {format_genres(genres)}" if genres else "ジャンル: 未設定（全部配信）"
    return f"今こんな感じ\n\n{plan_label}\n{active_label}\n{genre_label}"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)
    user = ensure_user(user_id)

    now_dt = datetime.now(timezone.utc)

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
                "all_links_used": False,
                "free_reply_date": today,
                "ai_count": 0,
                "ai_count_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["free_reply_used"] = False
        user["all_links_used"] = False
        user["free_reply_date"] = today
        user["ai_count"] = 0
        user["ai_count_date"] = today

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

    plan = user.get("plan", "free")
    active = user.get("active", True)
    genres = user.get("genres", [])
    qr = main_quick_reply()

    if text in _STOP_WORDS:
        save_user(user_id, active=False, plan=plan, genres=genres)
        reply_text(event.reply_token, "配信止めた\n再開したい時は言って", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _START_WORDS:
        save_user(user_id, active=True, plan=plan, genres=genres)
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
        reply_text(event.reply_token, "夜の配信止めたよ", quick_reply=qr)
        return

    if text in ["夜開始", "夜オン"]:
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
                "ニュースの深掘りとか\n"
                "まとめて見れるようにしてる",
                quick_reply=qr,
            )
        elif any(w in text for w in ["どうやる", "入り方", "登録"]):
            reply_text(
                event.reply_token,
                "メンバーシップってやつで見れるようにしてる\n\n"
                "LINEの画面からそのまま入れるようになってるよ",
                quick_reply=qr,
            )
        elif any(w in text for w in ["何できる", "何ができる"]):
            reply_text(
                event.reply_token,
                "気になるニュース送ってくれれば\n"
                "もう少し深く解説できるようにしてる\n\n"
                "あとまとめて一覧で見れるようにもしてる",
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
                "気になるならそのまま聞いてくれればOK",
                quick_reply=qr,
            )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "使い方":
        reply_text(
            event.reply_token,
            "使い方はこんな感じ\n\n"
            "【ニュースを見る】\n"
            "・番号 → 詳しく見る（例：①）\n"
            "・リンク → 記事URL出す\n\n"
            "【追加で見る】\n"
            "・他には → 別のニュース出す\n\n"
            "【深掘りする】\n"
            "気になるニュース送れば解説する\n\n"
            "無料でも使えるけど\n"
            "メンバーシップだと\n"
            "・ニュースを深掘りできる\n"
            "・会話ネタの返しまで出る\n"
            "・相手に合わせた話題も出せる\n\n"
            "そのまま送ればOK",
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
        reply_text(event.reply_token, _plan_status_text(plan, active, genres), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "設定":
        reply_text(event.reply_token, "どうする？\nそのまま言ってもOK", quick_reply=qr)
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

        save_user(user_id, active=True, plan=plan, genres=new_genres)
        reply_text(event.reply_token, f"ジャンル変えた: {format_genres(new_genres)}", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _STATUS_WORDS:
        reply_text(event.reply_token, _plan_status_text(plan, active, genres), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if any(kw in text for kw in _CHAT_TOPIC_KW):
        if plan == "paid":
            if not can_use_paid_ai(user):
                reply_text(
                    event.reply_token,
                    "今日は結構使ってるみたい\n\nまた明日使えるようになってるから\n気になるやつあれば明日聞いてくれればOK",
                    quick_reply=qr,
                )
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            answer = generate_chat_topic_paid(user_id)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        else:
            answer = generate_chat_topic_free(user_id)
        reply_text(event.reply_token, answer, quick_reply=qr)
        if plan != "paid":
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
        return

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
        answer = _answer_more_news(news_items, extra_items)
        reply_text(event.reply_token, answer, quick_reply=qr)
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

    if is_related_to_news_context(user_id, text):
        if not _looks_like_question_or_command(text):
            reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        free_reply_used = user.get("free_reply_used", False)

        if plan == "paid":
            # 有料ユーザー：1日10回まで
            if not can_use_paid_ai(user):
                reply_text(
                    event.reply_token,
                    "今日は結構使ってるみたい\n\nまた明日使えるようになってるから\n気になるやつあれば明日聞いてくれればOK",
                    quick_reply=qr,
                )
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            answer = answer_news_question(user_id, text)
            reply_text(event.reply_token, answer, quick_reply=qr)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)

        elif not free_reply_used:
            # 無料ユーザー初回：AI回答＋導線
            _ALL_LINK_KW = ["全部", "全リンク", "リンク全部", "全部のリンク"]
            question_for_ai = "今日のニュース5本をまとめて簡単に教えて" if any(kw in text for kw in _ALL_LINK_KW) else text
            answer = answer_news_question(user_id, question_for_ai)
            msg = f"詳しくはこんな感じ👇\n\n{answer}\n\nこの先はもう少し深く見れるようにしてる"
            reply_text(event.reply_token, msg, quick_reply=qr)
            try:
                supabase.table("users").update({
                    "free_reply_used": True,
                    "last_reply_time": now_dt.isoformat(),
                }).eq("user_id", user_id).execute()
                user["free_reply_used"] = True
            except Exception:
                pass

        else:
            # 無料ユーザー2回目以降：数字/全リンク/固定文、AI呼ばない
            _ALL_LINK_KW = ["全部", "全リンク", "リンク全部", "全部のリンク"]
            num = extract_number(text)
            all_links_used = user.get("all_links_used", False)

            if num is not None:
                # ① 数字 → 該当ニュースのURL返却
                ctx = get_latest_news_context(user_id)
                url = ""
                if ctx and _is_context_alive(ctx):
                    items = ctx.get("payload", {}).get("news_items", [])
                    item = next((n for n in items if n.get("index") == num), None)
                    if item:
                        url = item.get("link", "")
                if url:
                    circle = _CIRCLED[num - 1] if num <= len(_CIRCLED) else str(num)
                    reply_text(event.reply_token, f"この記事👇\n{url}", quick_reply=qr)
                else:
                    reply_text(event.reply_token, f"{num}番目の記事が見つからなかった", quick_reply=qr)

            elif any(kw in text for kw in _ALL_LINK_KW):
                if all_links_used:
                    # ③ 全リンク2回目以降
                    reply_text(event.reply_token, "今日はもう全部出してる\n番号で見てくれ", quick_reply=qr)
                else:
                    # ② 全リンク初回
                    ctx = get_latest_news_context(user_id)
                    if ctx and _is_context_alive(ctx):
                        items = ctx.get("payload", {}).get("news_items", [])
                        lines = ["まとめてどうぞ👇"]
                        for item in items:
                            idx = item.get("index", 0)
                            circle = _CIRCLED[idx - 1] if 0 < idx <= len(_CIRCLED) else str(idx)
                            lines.append(f"{circle} {item.get('link', '')}")
                        reply_text(event.reply_token, "\n".join(lines), quick_reply=qr)
                        try:
                            supabase.table("users").update({"all_links_used": True}).eq("user_id", user_id).execute()
                            user["all_links_used"] = True
                        except Exception:
                            pass
                    else:
                        reply_text(event.reply_token, "先に聞くでニュース出して", quick_reply=qr)

            else:
                # ④ その他
                reply_text(
                    event.reply_token,
                    "無料版は1回だけ深掘り対応してる\n番号か「全部」で見てくれ",
                    quick_reply=qr,
                )

            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
        return

    if plan == "paid" and any(kw in text for kw in _PERSON_KW) and any(kw in text for kw in _PERSON_REQUEST_KW):
        if not can_use_paid_ai(user):
            reply_text(
                event.reply_token,
                "今日は結構使ってるみたい\n\nまた明日使えるようになってるから\n気になるやつあれば明日聞いてくれればOK",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        answer = generate_chat_for_person(user_id, text)
        reply_text(event.reply_token, answer, quick_reply=qr)
        increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
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

    user = ensure_user(user_id)
    plan = user.get("plan", "free")
    active = user.get("active", True)
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

        save_user(user_id, active=active, plan=plan, genres=genres)
        reply_flex(event.reply_token, build_genre_flex(genres))

    elif data == "clear_genres":
        save_user(user_id, active=active, plan=plan, genres=[])
        reply_flex(event.reply_token, build_genre_flex([]))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))