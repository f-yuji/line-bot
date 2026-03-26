import json
import os
import re
from typing import List, Dict, Any
from urllib.parse import urlparse

import feedparser
import requests
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_API_KEY)

LINE_URL = "https://api.line.me/v2/bot/message/push"
RSS_URL = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"

MAX_FETCH_ITEMS = 40
DEFAULT_MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "アイドル", "不倫", "炎上"
]

GENRE_KEYWORDS = {
    "real_estate": ["不動産", "土地", "賃貸", "地価"],
    "construction": ["建設", "工事", "建材", "施工"],
    "interest_rates": ["金利", "日銀", "利上げ", "利下げ"],
    "energy": ["原油", "電力", "ガス"],
    "ai": ["AI", "人工知能", "半導体", "生成AI"],
    "sports": ["野球", "サッカー", "試合"],
    "economy": ["経済", "株価", "インフレ"],
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
        return host
    except:
        return "不明"

def shorten_url(url: str) -> str:
    try:
        res = requests.get(
            "https://is.gd/create.php",
            params={"format": "simple", "url": url},
            timeout=10
        )
        return res.text.strip()
    except:
        return url

def load_users():
    with open("user_settings.json", "r", encoding="utf-8") as f:
        return json.load(f).get("users", {})

# =========================
# ニュース取得
# =========================

def fetch_news():
    feed = feedparser.parse(RSS_URL)
    news = []

    for entry in feed.entries[:MAX_FETCH_ITEMS]:
        title = clean_text(entry.title)
        link = entry.link
        summary = strip_html(entry.summary)

        if any(word in title for word in EXCLUDE_KEYWORDS):
            continue

        news.append({
            "title": title,
            "link": link,
            "summary": summary,
            "source": extract_source_name(link),
            "short_link": shorten_url(link)
        })

    return news

# =========================
# フィルタ
# =========================

def match_keywords(news, keywords):
    text = news["title"] + news["summary"]
    return any(k in text for k in keywords)

def filter_news(news_list, user):
    plan = user.get("plan", "free")
    genres = user.get("genres", [])

    if plan == "free":
        return news_list[:3]

    keywords = []
    for g in genres:
        keywords += GENRE_KEYWORDS.get(g, [])

    if not keywords:
        return news_list[:5]

    matched = [n for n in news_list if match_keywords(n, keywords)]

    return matched[:5] if matched else news_list[:3]

# =========================
# AI要約
# =========================

def summarize(news_list):
    if not news_list:
        return ["なし"], ["★ 影響なし"]

    text = "\n".join([n["title"] for n in news_list])

    prompt = f"""
ニュース:
{text}

要約と影響をJSONで返せ
"""

    try:
        res = client.responses.create(
            model="gpt-5",
            input=prompt
        )
        data = json.loads(res.output_text)
        return data["summary"], data["impact"]
    except:
        return ["要約失敗"], ["★ 影響不明"]

# =========================
# メッセージ作成
# =========================

def build_message(news, summary, impact, user):
    lines = []
    for i, n in enumerate(news):
        s = summary[i] if i < len(summary) else n["title"]
        lines.append(f"{i+1}. {s}\n{n['short_link']}")

    msg1 = "\n\n".join(lines)
    msg2 = "\n\n".join(impact)

    return [msg1, msg2]

# =========================
# LINE送信
# =========================

def send(user_id, messages):
    requests.post(
        LINE_URL,
        headers={
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "to": user_id,
            "messages": [{"type": "text", "text": m} for m in messages]
        }
    )

# =========================
# 実行
# =========================

def main():
    users = load_users()
    news = fetch_news()

    for user_id, user in users.items():
        if not user.get("active", True):
            continue

        filtered = filter_news(news, user)
        summary, impact = summarize(filtered)
        messages = build_message(filtered, summary, impact, user)

        send(user_id, messages)
        print("sent:", user_id)

if __name__ == "__main__":
    main()