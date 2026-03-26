import json
import logging
import os
import re
import time
from typing import List, Dict, Any
from urllib.parse import urlparse

import feedparser
import requests
from dotenv import load_dotenv
from openai import OpenAI
from supabase import create_client

# 環境変数読み込み
load_dotenv()

# ─── ログ設定 ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── 環境変数 ───
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ─── クライアント ───
client = OpenAI(api_key=OPENAI_API_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── 定数 ───
LINE_URL = "https://api.line.me/v2/bot/message/push"
RSS_URL = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"

MAX_FETCH_ITEMS = 40
DEFAULT_MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "アイドル", "不倫", "炎上",
]

GENRE_KEYWORDS: Dict[str, List[str]] = {
    "real_estate": ["不動産", "土地", "賃貸", "地価", "マンション", "住宅"],
    "construction": ["建設", "工事", "建材", "施工", "塗装", "防水", "資材"],
    "interest_rates": ["金利", "日銀", "利上げ", "利下げ", "長期金利", "政策金利"],
    "energy": ["原油", "電力", "ガス", "燃料", "LNG"],
    "ai": ["AI", "人工知能", "半導体", "生成AI", "LLM", "OpenAI"],
    "sports": ["野球", "サッカー", "試合", "五輪", "W杯"],
    "economy": ["経済", "株価", "インフレ", "景気", "為替", "円安", "円高"],
    "business": ["企業", "決算", "業績", "M&A", "値上げ", "市場"],
    "tech": ["テック", "IT", "ソフトウェア", "クラウド", "データセンター", "半導体"],
}


# =========================
# ユーティリティ
# =========================

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", str(text or "")).strip()


def extract_source_name(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if "nikkei" in host:
            return "日経"
        if "nhk" in host:
            return "NHK"
        if "itmedia" in host:
            return "ITmedia"
        if "reuters" in host:
            return "Reuters"
        return host
    except Exception:
        return "不明"


def shorten_url(url: str) -> str:
    try:
        res = requests.get(
            "https://is.gd/create.php",
            params={"format": "simple", "url": url},
            timeout=10,
        )
        res.raise_for_status()
        return res.text.strip()
    except Exception as e:
        logger.warning("URL短縮失敗 (%s): %s", url, e)
        return url


def plan_max_items(plan: str) -> int:
    return {
        "free": 3,
        "light": 5,
        "premium": 8,
    }.get(plan, DEFAULT_MAX_ITEMS)


def load_users() -> Dict[str, Any]:
    try:
        res = supabase.table("users").select("*").eq("active", True).execute()
        rows = res.data or []

        users: Dict[str, Any] = {}
        for row in rows:
            user_id = row["user_id"]
            plan = row.get("plan", "free")
            users[user_id] = {
                "plan": plan,
                "active": row.get("active", True),
                "genres": row.get("genres", []) or [],
                "max_items": plan_max_items(plan),
            }

        logger.info("Supabaseユーザー読込: %d件", len(users))
        return users

    except Exception as e:
        logger.error("Supabase users 読み込み失敗: %s", e)
        return {}


# =========================
# ニュース取得
# =========================

def _fetch_rss(url: str, max_retries: int = 3) -> feedparser.FeedParserDict:
    """RSSを取得。不正XMLをサニタイズしてからパースし、失敗時はリトライする"""
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(url, timeout=20, headers={
                "User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"
            })
            res.raise_for_status()
            raw_xml = res.text

            # Google News RSSに混入する不正な制御文字を除去
            raw_xml = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw_xml)

            feed = feedparser.parse(raw_xml)

            if feed.entries:
                if feed.bozo:
                    logger.info("RSS bozo検出だがエントリあり(%d件)、続行", len(feed.entries))
                return feed

            logger.warning("RSS取得 試行%d/%d: エントリ0件", attempt, max_retries)

        except Exception as e:
            logger.warning("RSS取得 試行%d/%d 失敗: %s", attempt, max_retries, e)

        if attempt < max_retries:
            time.sleep(2 * attempt)

    logger.error("RSS取得: %d回リトライ後も失敗", max_retries)
    return feedparser.FeedParserDict(entries=[])


def fetch_news() -> List[Dict[str, str]]:
    feed = _fetch_rss(RSS_URL)

    news = []
    for entry in feed.entries[:MAX_FETCH_ITEMS]:
        title = clean_text(entry.get("title", ""))
        link = entry.get("link", "")
        summary = strip_html(entry.get("summary", ""))

        if not title or not link:
            continue
        if any(word in title for word in EXCLUDE_KEYWORDS):
            continue

        news.append({
            "title": title,
            "link": link,
            "summary": summary,
            "source": extract_source_name(link),
            "short_link": shorten_url(link),
        })

    logger.info("ニュース取得: %d件", len(news))
    return news


# =========================
# フィルタ
# =========================

def match_keywords(news: Dict[str, str], keywords: List[str]) -> bool:
    text = f"{news['title']} {news.get('summary', '')}"
    return any(k in text for k in keywords)


def filter_news(
    news_list: List[Dict[str, str]], user: Dict[str, Any]
) -> List[Dict[str, str]]:
    plan = user.get("plan", "free")
    genres = user.get("genres", []) or []
    max_items = user.get("max_items", DEFAULT_MAX_ITEMS)

    if plan == "free" or not genres:
        return news_list[:max_items]

    keywords: List[str] = []
    for g in genres:
        keywords += GENRE_KEYWORDS.get(g, [])

    if not keywords:
        return news_list[:max_items]

    matched = [n for n in news_list if match_keywords(n, keywords)]
    return matched[:max_items] if matched else news_list[:3]


# =========================
# AI要約
# =========================

def summarize(news_list: List[Dict[str, str]]) -> tuple[list, list]:
    if not news_list:
        return ["ニュースなし"], ["★ 影響なし"]

    titles = "\n".join(
        f"{i + 1}. {n['title']}" for i, n in enumerate(news_list)
    )
    count = len(news_list)

    prompt = (
        f"以下の{count}件のニュース見出しについて、それぞれ1〜2文で要約し、"
        "ビジネスへの影響を簡潔に分析してください。"
        "JSON形式で返してください。"
        "キーは summary（配列）と impact（配列）です。"
        "他のテキストは含めず、JSONのみ出力してください。\n\n"
        f"{titles}"
    )

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        raw = res.choices[0].message.content.strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        return (
            data.get("summary", ["要約失敗"]),
            data.get("impact", ["★ 影響不明"]),
        )

    except json.JSONDecodeError as e:
        logger.error("OpenAI応答のJSONパース失敗: %s", e)
        return ["要約失敗"], ["★ 影響不明"]
    except Exception as e:
        logger.error("OpenAI API エラー: %s", e)
        return ["要約失敗"], ["★ 影響不明"]


# =========================
# メッセージ作成
# =========================

def build_message(
    news: List[Dict[str, str]],
    summary: list,
    impact: list,
) -> List[str]:
    lines = []
    for i, n in enumerate(news):
        s = summary[i] if i < len(summary) else n["title"]
        lines.append(f"{i + 1}. {s}\n{n['short_link']}")

    msg1 = "\n\n".join(lines)
    msg2 = "\n\n".join(impact)

    if len(msg1) > LINE_TEXT_SAFE_LIMIT:
        msg1 = msg1[:LINE_TEXT_SAFE_LIMIT] + "\n…(省略)"
    if len(msg2) > LINE_TEXT_SAFE_LIMIT:
        msg2 = msg2[:LINE_TEXT_SAFE_LIMIT] + "\n…(省略)"

    return [msg1, msg2]


# =========================
# LINE送信
# =========================

def send(user_id: str, messages: List[str]) -> None:
    message_objects = [{"type": "text", "text": m} for m in messages]
    message_objects = message_objects[:LINE_MAX_MESSAGE_OBJECTS]

    try:
        res = requests.post(
            LINE_URL,
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": user_id, "messages": message_objects},
            timeout=30,
        )
        res.raise_for_status()
        logger.info("送信成功: %s", user_id)
    except requests.RequestException as e:
        logger.error("LINE送信失敗 (user=%s): %s", user_id, e)


# =========================
# 実行
# =========================

def main():
    users = load_users()
    if not users:
        logger.warning("配信対象ユーザーが0件です")
        return

    news = fetch_news()
    if not news:
        logger.warning("ニュースが0件のため配信スキップ")
        return

    sent_count = 0
    for user_id, user in users.items():
        if not user.get("active", True):
            logger.info("非アクティブのためスキップ: %s", user_id)
            continue

        filtered = filter_news(news, user)
        summary, impact = summarize(filtered)
        messages = build_message(filtered, summary, impact)
        send(user_id, messages)
        sent_count += 1

    logger.info("配信完了: %d/%d ユーザー", sent_count, len(users))


if __name__ == "__main__":
    main()