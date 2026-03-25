import os
import re
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
    "歌手", "YouTuber", "インフルエンサー"
]

INCLUDE_KEYWORDS = [
    "不動産", "建設", "工事", "改修", "塗装", "防水", "資材", "物流",
    "倉庫", "土地", "金利", "日銀", "為替", "円安", "原油", "AI",
    "人工知能", "半導体", "中小企業", "経済", "投資",
    "DX", "自動化", "省人化", "ロボット"
]


def is_excluded(title):
    return any(word in title for word in EXCLUDE_KEYWORDS)


def is_included(title):
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

        picked.append({"title": title, "link": link})

        if len(picked) >= max_items:
            break

    return picked


def summarize_news(news_items):
    if not news_items:
        return "・該当なし"

    headlines = "\n".join([item["title"] for item in news_items])

    prompt = f"""
以下のニュースを要約しろ。

条件:
・必ず箇条書き
・5行以内
・1行1トピック
・無駄な説明禁止

ニュース:
{headlines}
"""

    response = client.responses.create(
        model="gpt-5.4",
        input=prompt
    )

    text = response.output_text.strip()
    lines = text.split("\n")

    return "\n".join([f"・{l.strip('・ ')}" for l in lines if l.strip()])


def analyze_impact(news_items):
    if not news_items:
        return "・影響なし"

    headlines = "\n".join([item["title"] for item in news_items])

    prompt = f"""
以下のニュースから実務的な影響を抽出しろ。

対象:
・建設、改修、塗装、防水
・建材、資材、物流
・不動産投資

観点:
・建材価格、原油、物流
・金利、為替、景気
・AI、DX、自動化

条件:
・箇条書き
・最大5行
・行頭に★1〜3
・具体的に書け

ニュース:
{headlines}
"""

    response = client.responses.create(
        model="gpt-5.4",
        input=prompt
    )

    text = response.output_text.strip()
    lines = text.split("\n")

    result = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        line = re.sub(r"★1", "★", line)
        line = re.sub(r"★2", "★★", line)
        line = re.sub(r"★3", "★★★", line)

        if not line.startswith("★"):
            line = "★ " + line

        result.append(line)

    return "\n".join(result)


def build_message(news_items):
    if not news_items:
        raw_news = "該当ニュースなし"
    else:
        raw_news = "\n\n".join([
            f"{i+1}. {item['title']}\n{item['link']}"
            for i, item in enumerate(news_items)
        ])

    summary = summarize_news(news_items)
    impact = analyze_impact(news_items)

    return f"""【ニュース要約】
{summary}

--- 詳細 ---
{raw_news}

--- 事業・投資への影響 ---
{impact}
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