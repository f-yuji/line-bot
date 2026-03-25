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

# ========= API =========
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
    "再開発", "インフレ", "物価", "サプライチェーン", "輸送", "運賃"
]


# =========================
# ユーティリティ
# =========================
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def strip_html(text: str) -> str:
    text = text or ""
    text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_excluded(text: str) -> bool:
    return any(word in text for word in EXCLUDE_KEYWORDS)


def is_included(text: str) -> bool:
    return any(word in text for word in INCLUDE_KEYWORDS)


def dedupe_news(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    result = []

    for item in items:
        title = item.get("title", "")
        link = item.get("link", "")
        source = item.get("source", "")

        duplicated = False

        for kept in result:
            kept_title = kept.get("title", "")

            # 完全一致
            if title == kept_title or link == kept.get("link", ""):
                duplicated = True
                break

            # 同一テーマっぽいなら重複扱い
            if is_same_topic(title, kept_title):
                duplicated = True

                # より信頼したい媒体を残したいならここで置換
                preferred_sources = [
                    "Reuters", "Bloomberg", "日本経済新聞", "NHK",
                    "共同通信", "時事通信"
                ]

                kept_rank = preferred_sources.index(kept.get("source")) if kept.get("source") in preferred_sources else 999
                new_rank = preferred_sources.index(source) if source in preferred_sources else 999

                # 新しい方が優先媒体なら差し替え
                if new_rank < kept_rank:
                    kept.update(item)

                break

        if not duplicated:
            result.append(item)

    return result


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
    line = clean_text(line)
    if line.startswith("★★★"):
        return 0
    if line.startswith("★★"):
        return 1
    return 2


def sort_impact_lines(lines: List[str]) -> List[str]:
    return sorted(lines, key=impact_priority)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_text(text: str, limit: int = LINE_TEXT_SAFE_LIMIT) -> List[str]:
    text = text.strip()
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
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()

        if "nikkei.com" in host:
            return "日本経済新聞"
        if "reuters.com" in host:
            return "Reuters"
        if "bloomberg.co.jp" in host or "bloomberg.com" in host:
            return "Bloomberg"
        if "itmedia.co.jp" in host:
            return "ITmedia"
        if "asahi.com" in host:
            return "朝日新聞"
        if "yomiuri.co.jp" in host:
            return "読売新聞"
        if "mainichi.jp" in host:
            return "毎日新聞"
        if "sankei.com" in host:
            return "産経新聞"
        if "nhk.or.jp" in host:
            return "NHK"
        if "jiji.com" in host:
            return "時事通信"
        if "kyodonews.jp" in host:
            return "共同通信"
        if "news.yahoo.co.jp" in host:
            return "Yahoo!ニュース"
        if "news.google.com" in host:
            return "Google News"
        if host.startswith("www."):
            host = host[4:]
        return host or "不明"
    except Exception:
        return "不明"


def shorten_url(url: str, timeout: int = 10) -> str:
    try:
        res = requests.get(
            "https://is.gd/create.php",
            params={"format": "simple", "url": url},
            timeout=timeout
        )
        if res.status_code == 200:
            short_url = res.text.strip()
            if short_url.startswith("http"):
                return short_url
    except Exception:
        pass
    return url


def fetch_article_text(url: str, timeout: int = 5) -> str:
    try:
        res = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if res.status_code != 200:
            return ""

        text = strip_html(res.text)
        return text[:2000]
    except Exception:
        return ""


# =========================
# ニュース取得
# =========================
def fetch_news(max_items: int = MAX_ITEMS) -> List[Dict[str, str]]:
    feed = feedparser.parse(RSS_URL)
    picked = []

    for entry in getattr(feed, "entries", []):
        title = clean_text(getattr(entry, "title", ""))
        link = clean_text(getattr(entry, "link", ""))
        summary = strip_html(getattr(entry, "summary", ""))

        if not title or not link:
            continue

        source = extract_source_name(link)
        base_text = f"{title} {summary} {source}"

        if is_excluded(base_text):
            continue

        article_text = ""
        if not is_included(base_text):
            article_text = fetch_article_text(link)
            if not article_text:
                continue
            if is_excluded(article_text):
                continue
            if not is_included(article_text):
                continue

        short_link = shorten_url(link)

        picked.append({
            "title": title,
            "link": link,
            "short_link": short_link,
            "source": source,
            "summary": summary,
            "article_text": article_text
        })

        # 多めに見てから最後に切る
        if len(picked) >= max_items * 3:
            break

    picked = dedupe_news(picked)
    return picked[:max_items]


# =========================
# OpenAI 1回だけ
# =========================
def build_analysis_input(news_items: List[Dict[str, str]]) -> str:
    if not news_items:
        return "該当ニュースなし"

    rows = []
    for i, item in enumerate(news_items):
        body = item.get("summary") or item.get("article_text") or ""
        body = clean_text(body)[:300]
        rows.append(
            f"{i+1}. {item['title']}\n"
            f"ソース: {item.get('source', '不明')}\n"
            f"補足: {body}\n"
            f"URL: {item['link']}"
        )
    return "\n\n".join(rows)


def get_cache_path(headlines_blob: str) -> Path:
    digest = sha256_text(headlines_blob)
    return CACHE_DIR / f"{digest}.json"


def load_cached_analysis(headlines_blob: str) -> Dict[str, Any] | None:
    path = get_cache_path(headlines_blob)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_cached_analysis(headlines_blob: str, data: Dict[str, Any]) -> None:
    path = get_cache_path(headlines_blob)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def analyze_news_once(news_items: List[Dict[str, str]]) -> Dict[str, List[str]]:
    if not news_items:
        return {
            "summary": ["該当なし"],
            "impact": ["★ 影響なし"]
        }

    headlines_blob = build_analysis_input(news_items)
    cached = load_cached_analysis(headlines_blob)
    if cached:
        return cached

    prompt = f"""
以下のニュース一覧を材料に、必ずJSONだけで返せ。

【対象読者】
・建設、改修、塗装、防水の実務者
・建材、資材、物流に関心がある事業者
・不動産投資をしている事業者

【出力仕様】
- JSONのみ
- キーは "summary" と "impact"
- "summary": 配列、最大8件
- "impact": 配列、最大5件
- summary は1行で簡潔に
- impact は各要素の先頭を ★ / ★★ / ★★★ のいずれかにする
- 誇張禁止
- 不明なことは断定しない
- 日本語で返す

ニュース:
{headlines_blob}
"""

    response = client.responses.create(
        model="gpt-5.4",
        input=prompt,
        store=False,
        background=False,
        text={
            "format": {
                "type": "json_schema",
                "name": "news_digest",
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "summary": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 8
                        },
                        "impact": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 5
                        }
                    },
                    "required": ["summary", "impact"]
                }
            }
        }
    )

    raw = response.output_text.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {
            "summary": ["要約取得失敗"],
            "impact": ["★ 影響分析取得失敗"]
        }

    summary = data.get("summary", [])
    impact = data.get("impact", [])

    if not isinstance(summary, list):
        summary = ["要約形式不正"]
    if not isinstance(impact, list):
        impact = ["★ 影響形式不正"]

    summary = [clean_text(str(x).strip("・ ")) for x in summary if clean_text(str(x))]
    impact = [normalize_star_line(str(x)) for x in impact if clean_text(str(x))]
    impact = sort_impact_lines(impact)

    if not summary:
        summary = ["該当なし"]
    if not impact:
        impact = ["★ 影響なし"]

    result = {
        "summary": summary[:8],
        "impact": impact[:5]
    }
    save_cached_analysis(headlines_blob, result)
    return result


# =========================
# LINEメッセージ生成
# =========================
def build_messages(news_items: List[Dict[str, str]]) -> List[str]:
    analysis = analyze_news_once(news_items)

    summary_text = "\n".join([f"・{x}" for x in analysis["summary"]])

    if not news_items:
        detail_text = "該当ニュースなし"
    else:
        detail_lines = []
        for i, item in enumerate(news_items):
            source = item.get("source", "不明")
            url = item.get("short_link") or item.get("link")
            body = item.get("summary") or item.get("article_text") or ""
            body = clean_text(body)

            if body:
                body = body[:120] + ("..." if len(body) > 120 else "")
                detail_lines.append(
                    f"{i+1}. {item['title']}\n[{source}]\n{body}\n{url}"
                )
            else:
                detail_lines.append(
                    f"{i+1}. {item['title']}\n[{source}]\n{url}"
                )

        detail_text = "\n\n".join(detail_lines)

    impact_text = "\n".join(analysis["impact"])

    msg1 = f"""【ニュース要約】
{summary_text}

--- 詳細 ---
{detail_text}
"""

    msg2 = f"""--- お前にとってはこんな影響がある ---
{impact_text}
"""

    messages: List[str] = []
    for block in [msg1, msg2]:
        messages.extend(chunk_text(block, LINE_TEXT_SAFE_LIMIT))

    return messages[:LINE_MAX_MESSAGE_OBJECTS]


# =========================
# LINE送信
# =========================
def send_line(messages: List[str]) -> None:
    if not messages:
        print("送信対象なし")
        return

    if len(messages) > LINE_MAX_MESSAGE_OBJECTS:
        messages = messages[:LINE_MAX_MESSAGE_OBJECTS]

    payload = {
        "messages": [
            {"type": "text", "text": msg}
            for msg in messages
        ]
    }

    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json"
    }

    res = requests.post(LINE_URL, headers=headers, json=payload, timeout=30)
    print("LINE status:", res.status_code)
    print("LINE response:", res.text)
    res.raise_for_status()


# =========================
# 実行
# =========================
if __name__ == "__main__":
    news = fetch_news()
    msgs = build_messages(news)
    send_line(msgs)