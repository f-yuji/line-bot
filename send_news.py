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

RSS_SOURCES = [
    "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja",
    "https://news.google.com/rss/search?q=不動産+建設+金利+AI+スポーツ&hl=ja&gl=JP&ceid=JP:ja",
]

MAX_FETCH_ITEMS = 50
DEFAULT_MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500
RSS_RETRY = 2
RSS_TIMEOUT = 20

GOOD_SOURCES = [
    "nikkei.com",
    "nhk.or.jp",
    "itmedia.co.jp",
    "reuters.com",
    "asahi.com",
    "yomiuri.co.jp",
    "mainichi.jp",
]

CATEGORY_RULES: Dict[str, Dict[str, List[str]]] = {
    "real_estate": {
        "include": ["不動産", "土地", "地価", "住宅", "マンション", "賃貸", "家賃", "空室", "再開発", "物件"],
        "exclude": ["アイドル", "芸能", "ライブ", "試合"],
    },
    "construction": {
        "include": ["建設", "建築", "工事", "施工", "改修", "修繕", "塗装", "防水", "現場", "職人"],
        "exclude": ["野球", "サッカー", "アイドル"],
    },
    "interest_rates": {
        "include": ["金利", "利上げ", "利下げ", "日銀", "政策金利", "長期金利", "短期金利", "国債"],
        "exclude": ["熱愛", "芸能", "ドラマ"],
    },
    "materials": {
        "include": ["資材", "建材", "鋼材", "木材", "セメント", "塗料", "シンナー", "原料", "アスファルト"],
        "exclude": ["芸能", "映画", "ライブ"],
    },
    "economy": {
        "include": ["経済", "景気", "インフレ", "物価", "消費", "円安", "円高", "為替", "景況感"],
        "exclude": [],
    },
    "ai": {
        "include": ["AI", "人工知能", "生成AI", "LLM", "OpenAI", "半導体", "推論", "学習モデル"],
        "exclude": ["芸能", "熱愛"],
    },
    "tech": {
        "include": ["IT", "テック", "ソフトウェア", "クラウド", "データセンター", "SaaS", "半導体"],
        "exclude": ["芸能", "アイドル"],
    },
    "business": {
        "include": ["決算", "業績", "M&A", "買収", "上場", "企業", "市場", "事業", "値上げ"],
        "exclude": [],
    },
    "energy": {
        "include": ["電力", "ガス", "原油", "LNG", "燃料", "再エネ", "太陽光", "発電"],
        "exclude": [],
    },
    "sports": {
        "include": ["野球", "サッカー", "バスケ", "テニス", "試合", "優勝", "五輪", "W杯", "リーグ"],
        "exclude": ["不動産", "建設", "金利"],
    },
    "scandal": {
        "include": ["不倫", "熱愛", "炎上", "謝罪", "流出", "不祥事", "スキャンダル", "離婚", "ゴシップ"],
        "exclude": [],
    },
    "entertainment": {
        "include": ["芸能", "俳優", "女優", "アイドル", "映画", "ドラマ", "ライブ", "番組", "タレント"],
        "exclude": [],
    },
}

# 強制除外ではなく、主に一般配信ノイズ抑制用
HARD_EXCLUDE = []


# =========================
# ユーティリティ
# =========================
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", str(text or "")).strip()


def sanitize_xml(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\x00", "")
    text = re.sub(r"[^\x09\x0A\x0D\x20-\x7E\u00A0-\uFFFF]", "", text)
    return text


def extract_source_name(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        host = host.replace("www.", "")
        return host
    except Exception:
        return "unknown"


def source_bonus(source: str) -> int:
    for good in GOOD_SOURCES:
        if good in source:
            return 2
    return 0


def shorten_url(url: str) -> str:
    # 安定優先。重いなら短縮しない。
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
# RSS取得
# =========================
def _fetch_single_rss(url: str) -> List[Dict[str, str]]:
    for attempt in range(1, RSS_RETRY + 1):
        try:
            res = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=RSS_TIMEOUT,
            )
            res.raise_for_status()

            content_type = res.headers.get("Content-Type", "")
            logger.info("RSS取得 status=%s content-type=%s url=%s", res.status_code, content_type, url)

            xml_text = sanitize_xml(res.text)
            feed = feedparser.parse(xml_text)

            if feed.bozo:
                logger.warning("RSSパースに問題あり: %s", feed.bozo_exception)
                logger.warning("先頭200文字: %s", xml_text[:200])
                if attempt < RSS_RETRY:
                    time.sleep(1)
                    continue
                return []

            news = []
            for entry in feed.entries[:MAX_FETCH_ITEMS]:
                title = clean_text(entry.get("title", ""))
                link = entry.get("link", "")
                summary = strip_html(entry.get("summary", ""))

                if not title or not link:
                    continue
                if any(word in title for word in HARD_EXCLUDE):
                    continue

                news.append({
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "source": extract_source_name(link),
                    "short_link": shorten_url(link),
                })

            return news

        except Exception as e:
            logger.warning("RSS取得失敗 attempt=%s url=%s err=%s", attempt, url, e)
            if attempt < RSS_RETRY:
                time.sleep(1)

    return []


def dedupe_news(news_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result = []

    for item in news_list:
        key = clean_text(item.get("title", "")).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)

    return result


def fetch_news() -> List[Dict[str, str]]:
    all_news = []

    for url in RSS_SOURCES:
        part = _fetch_single_rss(url)
        all_news.extend(part)

    all_news = dedupe_news(all_news)
    logger.info("ニュース取得: %d件", len(all_news))
    return all_news


# =========================
# 抽出スコアリング
# =========================
def score_article_by_category(article: dict, category: str) -> int:
    rules = CATEGORY_RULES.get(category, {})
    include = rules.get("include", [])
    exclude = rules.get("exclude", [])

    title = str(article.get("title", ""))
    summary = str(article.get("summary", ""))
    source = str(article.get("source", ""))
    text = f"{title} {summary}"

    score = 0

    for kw in include:
        if kw in title:
            score += 3
        elif kw in summary:
            score += 1

    for kw in exclude:
        if kw in text:
            score -= 3

    score += source_bonus(source)
    return score


def detect_categories(article: dict, min_score: int = 2) -> Dict[str, int]:
    matched = {}
    for category in CATEGORY_RULES.keys():
        score = score_article_by_category(article, category)
        if score >= min_score:
            matched[category] = score
    return matched


def filter_news_for_user(news_list: List[Dict[str, str]], user: Dict[str, Any]) -> List[Dict[str, str]]:
    selected = user.get("genres", []) or []
    max_items = user.get("max_items", DEFAULT_MAX_ITEMS)

    # freeでジャンル未指定なら上から返す
    if not selected:
        return news_list[:max_items]

    scored_articles = []

    for article in news_list:
        matched = detect_categories(article)
        total_score = sum(matched.get(cat, 0) for cat in selected)

        if total_score > 0:
            scored_articles.append((article, total_score, matched))

    scored_articles.sort(key=lambda x: x[1], reverse=True)

    result = [x[0] for x in scored_articles[:max_items]]

    # 何も当たらないなら最低限返す
    if not result:
        return news_list[:min(3, len(news_list))]

    return result


# =========================
# AI要約
# =========================
def summarize(news_list: List[Dict[str, str]], user: Dict[str, Any]) -> tuple[list, list]:
    if not news_list:
        return ["ニュースなし"], ["★ 影響なし"]

    genres = user.get("genres", []) or []
    genre_text = ", ".join(genres) if genres else "general"

    lines = []
    for i, n in enumerate(news_list):
        lines.append(f"{i + 1}. {n['title']} / {n.get('summary', '')}")

    prompt = f"""
以下のニュースを、選択ジャンル {genre_text} に合う視点で要約してください。

条件:
- 1記事につき1〜2文
- 無駄な前置き禁止
- 事実ベース
- 日本語
- JSONのみ出力

出力形式:
{{
  "summary": ["...","..."],
  "impact": ["★ ...","★ ..."]
}}

impact は「そのジャンルを追う人にとっての影響」を短く書くこと。
たとえば不動産・建築・金利・資材・スポーツ・スキャンダルなど、選択ジャンルに合わせて書くこと。

対象ニュース:
{chr(10).join(lines)}
"""

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
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
def build_message(news: List[Dict[str, str]], summary: list, impact: list) -> List[str]:
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

        filtered = filter_news_for_user(news, user)
        summary, impact = summarize(filtered, user)
        messages = build_message(filtered, summary, impact)
        send(user_id, messages)
        sent_count += 1

    logger.info("配信完了: %d/%d ユーザー", sent_count, len(users))


if __name__ == "__main__":
    main()