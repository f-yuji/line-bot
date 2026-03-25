import json
import os
import re
from typing import List, Dict
from urllib.parse import urlparse

import feedparser
import requests
from openai import OpenAI

LINE_TOKEN = os.environ["LINE_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_API_KEY)

LINE_URL = "https://api.line.me/v2/bot/message/broadcast"
RSS_URL = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"

MAX_ITEMS = 8
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "タレント", "アイドル", "結婚", "離婚", "熱愛",
    "不倫", "炎上", "スキャンダル", "逮捕", "グラビア", "ドラマ", "映画",
    "歌手", "YouTuber", "インフルエンサー"
]

INCLUDE_KEYWORDS = [
    "不動産", "建設", "工事", "資材", "建材", "物流",
    "倉庫", "土地", "金利", "日銀", "利上げ", "利下げ", "為替", "円安", "円高",
    "原油", "エネルギー", "AI", "人工知能", "半導体", "中小企業", "経済", "投資",
    "DX", "自動化", "省人化", "ロボット", "住宅", "マンション", "オフィス",
    "再開発", "インフレ", "物価", "サプライチェーン", "株価", "運賃",
]


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def strip_html(text: str) -> str:
    text = str(text or "")
    text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_multiline_text(text: str) -> str:
    text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_excluded(text: str) -> bool:
    return any(word in text for word in EXCLUDE_KEYWORDS)


def is_included(text: str) -> bool:
    return any(word in text for word in INCLUDE_KEYWORDS)


def normalize_star_line(line: str) -> str:
    s = clean_text(line)
    s = re.sub(r"^★1\b", "★", s)
    s = re.sub(r"^★2\b", "★★", s)
    s = re.sub(r"^★3\b", "★★★", s)

    if s.startswith("★★★"):
        return s if s.startswith("★★★ ") else s.replace("★★★", "★★★ ", 1)
    if s.startswith("★★"):
        return s if s.startswith("★★ ") else s.replace("★★", "★★ ", 1)
    if s.startswith("★"):
        return s if s.startswith("★ ") else s.replace("★", "★ ", 1)

    return "★ " + s


def impact_priority(line: str) -> int:
    if line.startswith("★★★"):
        return 0
    if line.startswith("★★"):
        return 1
    return 2


def sort_impact_lines(lines: List[str]) -> List[str]:
    return sorted(lines, key=impact_priority)


def chunk_text(text: str, limit: int = LINE_TEXT_SAFE_LIMIT) -> List[str]:
    text = normalize_multiline_text(text)

    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""

    for block in text.split("\n\n"):
        block = block.strip()
        if not block:
            continue

        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = block

    if current:
        chunks.append(current)

    final_chunks = []
    for c in chunks:
        while len(c) > limit:
            final_chunks.append(c[:limit])
            c = c[limit:]
        if c:
            final_chunks.append(c)

    return final_chunks


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
        if "jiji" in host:
            return "時事通信"
        if "kyodonews" in host:
            return "共同通信"
        if "asahi" in host:
            return "朝日新聞"
        if "yomiuri" in host:
            return "読売新聞"
        if "mainichi" in host:
            return "毎日新聞"
        if "sankei" in host:
            return "産経新聞"
        if "itmedia" in host:
            return "ITmedia"
        return host.replace("www.", "") or "不明"
    except Exception:
        return "不明"


def shorten_url(url: str) -> str:
    try:
        res = requests.get(
            "https://is.gd/create.php",
            params={"format": "simple", "url": url},
            timeout=10
        )
        if res.status_code == 200:
            short_url = res.text.strip()
            if short_url.startswith("http"):
                return short_url
    except Exception:
        pass
    return url


def fetch_article_text(url: str) -> str:
    try:
        res = requests.get(
            url,
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if res.status_code != 200:
            return ""
        return strip_html(res.text)[:1500]
    except Exception:
        return ""


def normalize_title_for_grouping(title: str) -> str:
    t = clean_text(title).lower()
    t = re.sub(r"[【】\[\]（）()「」『』〈〉<>]", " ", t)
    t = re.sub(r"[!！?？:：/／|｜・,，。.．\-—]", " ", t)
    t = re.sub(r"\d+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    stopwords = [
        "速報", "続報", "詳報", "解説", "最新", "映像", "動画",
        "写真", "まとめ", "について", "に関する", "を受け", "受けて",
        "発表", "表明"
    ]
    for w in stopwords:
        t = t.replace(w, " ")

    t = re.sub(r"\s+", " ", t).strip()
    return t


def title_token_set(title: str) -> set:
    t = normalize_title_for_grouping(title)
    return {x for x in re.split(r"\s+", t) if len(x) >= 2}


def is_same_topic(title1: str, title2: str) -> bool:
    n1 = normalize_title_for_grouping(title1)
    n2 = normalize_title_for_grouping(title2)

    if not n1 or not n2:
        return False

    if n1 == n2:
        return True

    if n1 in n2 or n2 in n1:
        shorter = min(len(n1), len(n2))
        longer = max(len(n1), len(n2))
        if longer > 0 and shorter / longer >= 0.7:
            return True

    s1 = title_token_set(title1)
    s2 = title_token_set(title2)

    if not s1 or not s2:
        return False

    overlap = len(s1 & s2)
    base = min(len(s1), len(s2))
    return base >= 2 and overlap / base >= 0.7


def dedupe_news(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []

    for item in items:
        duplicated = False
        for kept in result:
            if item["link"] == kept["link"]:
                duplicated = True
                break
            if is_same_topic(item["title"], kept["title"]):
                duplicated = True
                break

        if not duplicated:
            result.append(item)

    return result


def fetch_news() -> List[Dict[str, str]]:
    feed = feedparser.parse(RSS_URL)
    picked: List[Dict[str, str]] = []

    for entry in getattr(feed, "entries", []):
        title = clean_text(getattr(entry, "title", ""))
        link = clean_text(getattr(entry, "link", ""))
        summary = strip_html(getattr(entry, "summary", ""))

        if not title or not link:
            continue

        base_text = f"{title} {summary}"

        if is_excluded(base_text):
            continue

        if not is_included(base_text):
            article = fetch_article_text(link)
            if not article:
                continue
            if is_excluded(article):
                continue
            if not is_included(article):
                continue

        picked.append({
            "title": title,
            "link": link,
            "short_link": shorten_url(link),
            "source": extract_source_name(link)
        })

        if len(picked) >= MAX_ITEMS * 4:
            break

    picked = dedupe_news(picked)
    return picked[:MAX_ITEMS]


def analyze(news: List[Dict[str, str]]) -> Dict[str, List[str]]:
    if not news:
        return {"summary": ["該当なし"], "impact": ["★ 影響なし"]}

    text = "\n".join([f"{i+1}. {n['title']}" for i, n in enumerate(news)])

    prompt = f"""
以下のニュースを実務向けに整理しろ。
必ずJSONのみで返すこと。

条件:
- summary は配列
- impact は配列
- summary はニュースごとの要点を簡潔に
- impact は実務・投資への影響
- impact の各要素は ★ / ★★ / ★★★ のいずれかで始める
- 不明なことは断定しない

ニュース:
{text}

返却形式:
{{
  "summary": ["...", "..."],
  "impact": ["★★ ...", "★ ..."]
}}
"""

    try:
        res = client.responses.create(
            model="gpt-5.4",
            input=prompt
        )
        raw = res.output_text.strip()
        data = json.loads(raw)
    except Exception:
        return {"summary": ["要約取得失敗"], "impact": ["★ 影響分析取得失敗"]}

    summary = data.get("summary", [])
    impact = data.get("impact", [])

    if isinstance(summary, str):
        summary = [summary]
    if isinstance(impact, str):
        impact = [impact]

    if not isinstance(summary, list):
        summary = [str(summary)]
    if not isinstance(impact, list):
        impact = [str(impact)]

    summary = [clean_text(x) for x in summary if clean_text(x)]
    impact = [normalize_star_line(x) for x in impact if clean_text(x)]
    impact = sort_impact_lines(impact)

    if not summary:
        summary = ["該当なし"]
    if not impact:
        impact = ["★ 影響なし"]

    return {
        "summary": summary[:MAX_ITEMS],
        "impact": impact[:5]
    }


def build_messages(news: List[Dict[str, str]]) -> List[str]:
    a = analyze(news)

    summary_text = "\n".join([
        f"{i+1}. {x}" for i, x in enumerate(a["summary"])
    ])

    if not news:
        detail_text = "該当ニュースなし"
    else:
        detail_text = "\n\n".join([
            f"{i+1}. {n['title']}\n[{n['source']}]\n{n.get('short_link') or n['link']}"
            for i, n in enumerate(news)
        ])

    impact_text = "\n\n".join(a["impact"])

    msg1 = f"""【ニュース要約】
{summary_text}

--- 詳細 ---
{detail_text}
"""

    msg2 = f"""--- お前にとってはこんな影響がある ---
{impact_text}
"""

    messages: List[str] = []
    for m in [msg1, msg2]:
        messages.extend(chunk_text(m))

    return messages[:LINE_MAX_MESSAGE_OBJECTS]


def send(messages: List[str]) -> None:
    res = requests.post(
        LINE_URL,
        headers={
            "Authorization": f"Bearer {LINE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={"messages": [{"type": "text", "text": m} for m in messages]},
        timeout=30
    )
    print("LINE status:", res.status_code)
    print("LINE response:", res.text)
    res.raise_for_status()


if __name__ == "__main__":
    news = fetch_news()
    msgs = build_messages(news)
    send(msgs)