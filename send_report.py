import json
import os
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse

import feedparser
import requests
from openai import OpenAI

# ========= 環境変数 =========
LINE_TOKEN = os.environ["LINE_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_API_KEY)

LINE_URL = "https://api.line.me/v2/bot/message/broadcast"
RSS_URL = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"

# ========= 設定 =========
MAX_ITEMS = 8
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500
CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "タレント", "アイドル", "結婚", "離婚", "熱愛",
    "不倫", "炎上", "スキャンダル", "逮捕", "グラビア", "ドラマ", "映画",
    "歌手", "YouTuber", "インフルエンサー"
]

INCLUDE_KEYWORDS = [
    "不動産", "建設", "工事", "改修", "塗装", "防水", "資材", "建材", "物流",
    "倉庫", "土地", "金利", "日銀", "利上げ", "利下げ", "為替", "円安", "円高",
    "原油", "エネルギー", "AI", "人工知能", "半導体", "中小企業", "経済", "投資",
    "DX", "自動化", "省人化", "ロボット", "住宅", "マンション", "オフィス",
    "再開発", "インフレ", "物価", "サプライチェーン", "輸送", "運賃",
    "ホルムズ", "海運"
]

PREFERRED_SOURCES = [
    "Reuters", "Bloomberg", "日本経済新聞", "NHK", "共同通信", "時事通信"
]


# =========================
# ユーティリティ
# =========================
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def strip_html(text: str) -> str:
    text = text or ""
    text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.S)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_excluded(text: str) -> bool:
    return any(word in text for word in EXCLUDE_KEYWORDS)


def is_included(text: str) -> bool:
    return any(word in text for word in INCLUDE_KEYWORDS)


def normalize_star_line(line: str) -> str:
    s = clean_text(line)
    s = re.sub(r"^★1", "★", s)
    s = re.sub(r"^★2", "★★", s)
    s = re.sub(r"^★3", "★★★", s)
    if not s.startswith("★"):
        s = "★ " + s
    return s


def impact_priority(line: str) -> int:
    if line.startswith("★★★"):
        return 0
    if line.startswith("★★"):
        return 1
    return 2


def sort_impact_lines(lines):
    return sorted(lines, key=impact_priority)


def chunk_text(text: str, limit: int = LINE_TEXT_SAFE_LIMIT):
    if len(text) <= limit:
        return [text]

    chunks = []
    while text:
        chunks.append(text[:limit])
        text = text[limit:]
    return chunks


def extract_source_name(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if "nikkei" in host:
            return "日本経済新聞"
        if "reuters" in host:
            return "Reuters"
        if "bloomberg" in host:
            return "Bloomberg"
        if "nhk" in host:
            return "NHK"
        return host.replace("www.", "")
    except:
        return "不明"


def fetch_article_text(url: str) -> str:
    try:
        res = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla"})
        if res.status_code != 200:
            return ""
        return strip_html(res.text)[:1500]
    except:
        return ""


# =========================
# 重複排除
# =========================
def is_same_topic(t1: str, t2: str) -> bool:
    t1 = clean_text(t1)
    t2 = clean_text(t2)
    return t1[:20] == t2[:20]


def dedupe_news(items):
    result = []
    for item in items:
        dup = False
        for r in result:
            if is_same_topic(item["title"], r["title"]):
                dup = True
                break
        if not dup:
            result.append(item)
    return result


# =========================
# ニュース取得
# =========================
def fetch_news():
    feed = feedparser.parse(RSS_URL)
    picked = []

    for entry in feed.entries:
        title = clean_text(entry.title)
        link = clean_text(entry.link)
        summary = strip_html(getattr(entry, "summary", ""))

        text = f"{title} {summary}"

        if is_excluded(text):
            continue

        if not is_included(text):
            article = fetch_article_text(link)
            if not article or not is_included(article):
                continue

        picked.append({
            "title": title,
            "link": link,
            "source": extract_source_name(link)
        })

        if len(picked) >= MAX_ITEMS * 3:
            break

    picked = dedupe_news(picked)
    return picked[:MAX_ITEMS]


# =========================
# OpenAI
# =========================
def analyze(news):
    if not news:
        return {"summary": ["該当なし"], "impact": ["★ 影響なし"]}

    text = "\n".join([n["title"] for n in news])

    res = client.responses.create(
        model="gpt-5.4",
        input=f"""
ニュースを要約しろ

{text}

JSONで:
summary: 要約
impact: 影響
"""
    )

    try:
        data = json.loads(res.output_text)
    except:
        return {"summary": ["失敗"], "impact": ["★ 失敗"]}

    summary = [clean_text(x) for x in data.get("summary", [])]
    impact = [normalize_star_line(x) for x in data.get("impact", [])]

    return {
        "summary": summary,
        "impact": sort_impact_lines(impact)
    }


# =========================
# メッセージ
# =========================
def build_messages(news):
    a = analyze(news)

    summary = "\n".join([f"{i+1}. {x}" for i, x in enumerate(a["summary"])])

    detail = "\n\n".join([
        f"{i+1}. {n['title']}\n[{n['source']}]\n{n['link']}"
        for i, n in enumerate(news)
    ])

    impact = "\n".join(a["impact"])

    msg1 = f"""【ニュース要約】
{summary}

--- 詳細 ---
{detail}
"""

    msg2 = f"""--- お前にとってはこんな影響がある ---
{impact}
"""

    messages = []
    for m in [msg1, msg2]:
        messages += chunk_text(m)

    return messages[:LINE_MAX_MESSAGE_OBJECTS]


# =========================
# LINE送信
# =========================
def send(messages):
    requests.post(
        LINE_URL,
        headers={
            "Authorization": f"Bearer {LINE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={"messages": [{"type": "text", "text": m} for m in messages]}
    )


# =========================
# 実行
# =========================
if __name__ == "__main__":
    news = fetch_news()
    msgs = build_messages(news)
    send(msgs)