import os
from datetime import datetime

import feedparser
import requests
from openai import OpenAI

LINE_TOKEN = os.environ["LINE_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_API_KEY)

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
        link = entry.link.strip()

        if is_excluded(title):
            continue

        if not is_included(title):
            continue

        picked.append({
            "title": title,
            "link": link
        })

        if len(picked) >= max_items:
            break

    return picked


def summarize_news(news_items):
    if not news_items:
        return "要約対象ニュースなし"

    headlines = "\n".join(
        [f"{i+1}. {item['title']}" for i, item in enumerate(news_items)]
    )

    prompt = f"""
以下は今日のニュース見出しです。
ユーザーは、不動産・建設・資材価格・金利・AIに関心があります。

やること:
1. 全体を3〜5行で日本語要約
2. 最後に「実務インパクト」を1〜2行で書く
3. 芸能・感情表現・大げさな煽りは不要
4. 端的に書く

ニュース見出し:
{headlines}
"""

    response = client.responses.create(
        model="gpt-5.4",
        input=prompt
    )

    return response.output_text.strip()


def build_message(news_items):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not news_items:
        raw_news = "該当ニュースなし"
        summary = "要約なし"
    else:
        raw_news = "\n".join(
            [f"{i+1}. {item['title']}\nURL: {item['link']}" for i, item in enumerate(news_items)]
        )
        summary = summarize_news(news_items)

    return f"""ニュースレポ {now}

■抽出結果
{raw_news}

■AI要約
{summary}
"""


def send_line(message):
    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
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

    res = requests.post(LINE_URL, headers=headers, json=data, timeout=30)
    print(res.status_code, res.text)


if __name__ == "__main__":
    news = fetch_news()
    msg = build_message(news)
    send_line(msg)