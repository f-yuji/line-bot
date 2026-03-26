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

# Google Newsがブロックされた場合のフォールバック付きRSSソース
RSS_SOURCES = [
    "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja",
    "https://news.yahoo.co.jp/rss/topics/top-picks.xml",
    "https://assets.wor.jp/rss/rdf/nikkei/news.rdf",
]

MAX_FETCH_ITEMS = 40
DEFAULT_MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "アイドル", "不倫", "炎上",
    "占い", "グラビア", "プレゼント", "ランキング", "キャンペーン",
    "話題", "SNSで反響", "バズ", "トレンド",
]

SCORE_THRESHOLD = 3

# 強キーワード: +3
STRONG_KEYWORDS: Dict[str, List[str]] = {
    "real_estate":    ["不動産価格", "地価上昇", "住宅ローン", "マンション価格"],
    "construction":   ["建設受注", "工事費上昇", "建設コスト", "鋼材"],
    "interest_rates": ["政策金利", "日銀", "利上げ", "利下げ", "長期金利"],
    "energy":         ["原油急騰", "電力不足", "LNG"],
    "ai":             ["生成AI", "LLM", "OpenAI", "ChatGPT", "GPU"],
    "sports":         ["優勝", "W杯", "五輪", "金メダル"],
    "economy":        ["株価急落", "景気後退", "GDP", "リセッション"],
    "business":       ["大型M&A", "倒産", "上場廃止", "業績下方修正"],
    "tech":           ["クラウド障害", "データ漏洩", "サイバー攻撃"],
    "international":  ["停戦", "制裁", "外交交渉", "G7", "NATO"],
    "materials":      ["資材高騰", "セメント", "鉄骨"],
}

# 弱キーワード: +1 (カテゴリ判定にも使用)
CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "real_estate":    ["不動産", "土地", "賃貸", "地価", "マンション", "住宅"],
    "construction":   ["建設", "工事", "建材", "施工", "塗装", "防水", "資材", "ゼネコン"],
    "interest_rates": ["金利", "金融政策", "中央銀行"],
    "energy":         ["原油", "電力", "ガス", "燃料"],
    "ai":             ["AI", "人工知能", "半導体", "機械学習"],
    "sports":         ["野球", "サッカー", "試合", "五輪", "W杯", "ラグビー"],
    "economy":        ["経済", "株価", "インフレ", "景気", "為替", "円安", "円高", "賃金"],
    "business":       ["企業", "決算", "業績", "M&A", "値上げ", "市場"],
    "tech":           ["テック", "IT", "ソフトウェア", "クラウド", "データセンター"],
    "international":  ["米国", "中国", "欧州", "ロシア", "戦争", "制裁", "外交"],
    "materials":      ["鋼材", "コンクリート", "セメント", "木材"],
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
        if "yahoo" in host:
            return "Yahoo"
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
                "user_id": user_id,
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
# カテゴリ判定・スコアリング
# =========================

def classify_category(article: Dict[str, str]) -> str:
    text = f"{article['title']} {article.get('summary', '')}"
    scores: Dict[str, int] = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        count = sum(1 for k in keywords if k in text)
        if count:
            scores[cat] = count
    return max(scores, key=scores.get) if scores else "other"


def score_article(article: Dict[str, str], user_genres: List[str]) -> int:
    text = f"{article['title']} {article.get('summary', '')}"
    score = 0

    for word in EXCLUDE_KEYWORDS:
        if word in text:
            score -= 3

    for keywords in STRONG_KEYWORDS.values():
        for k in keywords:
            if k in text:
                score += 3

    for keywords in CATEGORY_KEYWORDS.values():
        for k in keywords:
            if k in text:
                score += 1

    if user_genres and article.get("category") in user_genres:
        score += 2

    return score


# =========================
# ニュース取得
# =========================

def _fetch_single_rss(url: str, max_retries: int = 2) -> feedparser.FeedParserDict:
    """単一RSSソースを取得。リトライ付き"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Referer": "https://news.google.com/",
        "Cache-Control": "no-cache",
    })

    for attempt in range(1, max_retries + 1):
        try:
            res = session.get(url, timeout=20)

            logger.info(
                "RSS HTTP応答: url=%s status=%d size=%d",
                url, res.status_code, len(res.text),
            )

            if res.status_code == 403:
                logger.warning(
                    "RSS 403 Forbidden: %s | body=%s",
                    url, res.text[:200],
                )
                return feedparser.FeedParserDict(entries=[])

            res.raise_for_status()
            raw_xml = res.text

            # 不正な制御文字を除去
            raw_xml = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw_xml)

            feed = feedparser.parse(raw_xml)

            if feed.entries:
                if feed.bozo:
                    logger.info(
                        "RSS bozo検出だがエントリあり(%d件)、続行: %s",
                        len(feed.entries), url,
                    )
                logger.info("RSS取得成功: url=%s entries=%d", url, len(feed.entries))
                return feed

            logger.warning(
                "RSS 試行%d/%d エントリ0件: %s | body=%s",
                attempt, max_retries, url, res.text[:200],
            )

        except requests.RequestException as e:
            logger.warning(
                "RSS 試行%d/%d HTTPエラー: %s → %s",
                attempt, max_retries, url, e,
            )
        except Exception as e:
            logger.warning(
                "RSS 試行%d/%d 予期しないエラー: %s → %s",
                attempt, max_retries, url, e,
            )

        if attempt < max_retries:
            time.sleep(3 * attempt)

    logger.error("RSS全試行失敗: %s", url)
    return feedparser.FeedParserDict(entries=[])


def fetch_news() -> List[Dict[str, str]]:
    """複数RSSソースを順に試し、最初に成功したものを使う"""
    feed = None

    for rss_url in RSS_SOURCES:
        logger.info("RSSソース試行: %s", rss_url)
        result = _fetch_single_rss(rss_url)
        if result.entries:
            logger.info("RSSソース成功: %s (%d件)", rss_url, len(result.entries))
            feed = result
            break
        logger.warning("RSSソース失敗、次へ: %s", rss_url)

    if feed is None or not feed.entries:
        logger.error("全RSSソース失敗")
        return []

    news = []
    for entry in feed.entries[:MAX_FETCH_ITEMS]:
        title = clean_text(entry.get("title", ""))
        link = entry.get("link", "")
        summary = strip_html(entry.get("summary", entry.get("description", "")))

        if not title or not link:
            continue
        if any(word in title for word in EXCLUDE_KEYWORDS):
            continue

        article = {
            "title": title,
            "link": link,
            "summary": summary,
            "source": extract_source_name(link),
        }
        article["category"] = classify_category(article)
        news.append(article)

    logger.info("ニュース取得: %d件", len(news))
    return news


# =========================
# フィルタ
# =========================

def filter_news(
    news_list: List[Dict[str, str]], user: Dict[str, Any]
) -> List[Dict[str, str]]:
    user_id = user.get("user_id", "?")
    genres = user.get("genres", []) or []
    max_items = user.get("max_items", DEFAULT_MAX_ITEMS)

    scored = []
    for n in news_list:
        s = score_article(n, genres)
        if s >= SCORE_THRESHOLD:
            scored.append((s, n))
    scored.sort(key=lambda x: x[0], reverse=True)

    logger.info(
        "スコアフィルタ: user=%s 全%d件→%d件(閾値%d)",
        user_id, len(news_list), len(scored), SCORE_THRESHOLD,
    )
    for s, n in scored:
        logger.info(
            "  [%s] score=%d %s",
            n.get("category", "other"), s, n["title"],
        )

    filtered = [n for _, n in scored]

    if genres:
        genre_matched = [n for n in filtered if n.get("category") in genres]
        if genre_matched:
            logger.info(
                "ジャンル絞り込み: user=%s genres=%s → %d件",
                user_id, genres, len(genre_matched),
            )
            filtered = genre_matched

    return filtered[:max_items] if filtered else news_list[:3]


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
        short = shorten_url(n["link"])
        lines.append(f"{i + 1}. {s}\n{short}")

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

        logger.info(
            "配信開始: user=%s plan=%s genres=%s",
            user_id, user.get("plan"), user.get("genres"),
        )
        filtered = filter_news(news, user)
        logger.info("送信件数: user=%s %d件", user_id, len(filtered))
        summary, impact = summarize(filtered)
        messages = build_message(filtered, summary, impact)
        send(user_id, messages)
        sent_count += 1

    logger.info("配信完了: %d/%d ユーザー", sent_count, len(users))


if __name__ == "__main__":
    main()