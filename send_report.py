import os
from datetime import datetime

import feedparser
import requests

TOKEN = os.environ["LINE_TOKEN"]

LINE_URL = "https://api.line.me/v2/bot/message/broadcast"
RSS_URL = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "タレント", "アイドル", "結婚", "離婚", "熱愛",
    "不倫", "炎上", "スキャンダル", "逮捕", "グラビア", "ドラマ", "映画",
    "歌手", "YouTuber", "ユーチューバー", "インフルエンサー"
]

INCLUDE_KEYWORDS = [
    "不動産", "建設", "工事", "改修", "塗装", "防水", "資材", "物流",
    "倉庫", "土地", "金利", "日銀", "為替", "円安", "原油", "AI",
    "人工知能", "半導体", "中小企業", "経済", "投資"
]


def is_excluded(title: str) -> bool:
    return any(word in title for word in EXCLUDE_KEYWORDS)


def is_included(title: str) -> bool:
    return any(word in title for word in INCLUDE_KEYWORDS)


def fetch_news(max_items=5):
    feed = feedparser.parse(RSS_URL)
    picked = []

    for entry in feed.entries:
        title = entry.title.strip()

        if is_excluded(title):
            continue

        if not is_included(title):
            continue

        picked.append(title)

        if len(picked) >= max_items:
            break

    return picked


def build_message(news_items):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not news_items:
        body = "該当ニュースなし"
    else:
        body = "\n".join([f"{i+1}. {n}" for i, n in enumerate(news_items)])

    return f"""ニュースレポ {now}

■抽出結果
{body}
"""


def send_line(message):
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    data = {
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }

    res = requests.post(LINE_URL, headers=headers, json=data)
    print(res.status_code, res.text)


if __name__ == "__main__":
    news = fetch_news()
    msg = build_message(news)
    send_line(msg)