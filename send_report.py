import json
import os
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any

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
MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5   # LINE公式: 1リクエスト最大5 message objects
LINE_TEXT_SAFE_LIMIT = 4500    # 安全側
CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

EXCLUDE_KEYWORDS = [
    "芸能", "女優", "俳優", "タレント", "アイドル", "結婚", "離婚", "熱愛",
    "不倫", "炎上", "スキャンダル", "逮捕", "グラビア", "ドラマ", "映画",
    "歌手", "YouTuber", "インフルエンサー"
]

INCLUDE_KEYWORDS = [
    "不動産", "建設", "工事", "改修", "塗装", "防水", "資材", "建材", "物流",
    "倉庫", "土地", "金利", "日銀", "為替", "円安", "原油", "AI",
    "人工知能", "半導体", "中小企業", "経済", "投資",
    "DX", "自動化", "省人化", "ロボット"
]


# =========================
# ユーティリティ
# =========================
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def is_excluded(title: str) -> bool:
    return any(word in title for word in EXCLUDE_KEYWORDS)


def is_included(title: str) -> bool:
    return any(word in title for word in INCLUDE_KEYWORDS)


def dedupe_news(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result = []
    for item in items:
        key = (item["title"], item["link"])
        if key in seen:
            continue
        seen.add(key)
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


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_text(text: str, limit: int = LINE_TEXT_SAFE_LIMIT) -> List[str]:
    text = text.strip()
    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""

    # 段落単位でなるべく分割
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

    # それでも長い塊は強制切断
    final_chunks = []
    for c in chunks:
        while len(c) > limit:
            final_chunks.append(c[:limit])
            c = c[limit:]
        if c:
            final_chunks.append(c)

    return final_chunks


# =========================
# ニュース取得
# =========================
def fetch_news(max_items: int = MAX_ITEMS) -> List[Dict[str, str]]:
    feed = feedparser.parse(RSS_URL)
    picked = []

    for entry in getattr(feed, "entries", []):
        title = clean_text(getattr(entry, "title", ""))
        link = clean_text(getattr(entry, "link", ""))

        if not title or not link:
            continue
        if is_excluded(title):
            continue
        if not is_included(title):
            continue

        picked.append({"title": title, "link": link})

        # 少し多めに拾ってから重複排除
        if len(picked) >= max_items * 2:
            break

    picked = dedupe_news(picked)
    return picked[:max_items]


# =========================
# OpenAI 1回だけ
# =========================
def build_analysis_input(news_items: List[Dict[str, str]]) -> str:
    if not news_items:
        return "該当ニュースなし"

    return "\n".join(
        [f"{i+1}. {item['title']}\nURL: {item['link']}" for i, item in enumerate(news_items)]
    )


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
- "summary": 配列、最大5件
- "impact": 配列、最大5件
- summary は1行で簡潔に
- impact は各要素の先頭を ★ / ★★ / ★★★ のいずれかにする
- 見出しとURLから合理的に言える範囲だけ
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
                            "maxItems": 5
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

    if not summary:
        summary = ["該当なし"]
    if not impact:
        impact = ["★ 影響なし"]

    result = {
        "summary": summary[:5],
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
        detail_text = "\n\n".join([
            f"{i+1}. {item['title']}\n{item['link']}"
            for i, item in enumerate(news_items)
        ])

    impact_text = "\n".join(analysis["impact"])

    blocks = [
        f"""【ニュース要約】
{summary_text}""",
        f"""--- 詳細 ---
{detail_text}""",
        f"""--- お前にとってはこんな影響がある ---
{impact_text}"""
    ]

    messages: List[str] = []
    for block in blocks:
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
            {
                "type": "text",
                "text": msg
            }
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