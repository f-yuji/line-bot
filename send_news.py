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
    "芸能", "女優", "俳優", "タレント", "アイドル", "結婚", "離婚", "熱愛",
    "不倫", "炎上", "スキャンダル", "グラビア", "逮捕", "ドラマ", "映画",
    "歌手", "YouTuber", "インフルエンサー"
]

GENRE_KEYWORDS = {
    "real_estate": [
        "不動産", "土地", "住宅", "マンション", "オフィス", "再開発", "賃貸", "地価"
    ],
    "construction": [
        "建設", "工事", "資材", "建材", "物流", "倉庫", "施工", "職人", "建築"
    ],
    "interest_rates": [
        "金利", "日銀", "利上げ", "利下げ", "為替", "円安", "円高", "長期金利"
    ],
    "energy": [
        "原油", "エネルギー", "電力", "ガス", "燃料", "LNG", "発電"
    ],
    "ai": [
        "AI", "人工知能", "半導体", "DX", "自動化", "省人化", "ロボット", "生成AI"
    ],
    "sports": [
        "野球", "サッカー", "バスケ", "テニス", "ゴルフ", "試合", "優勝",
        "リーグ", "代表", "五輪", "オリンピック", "W杯"
    ],
    "economy": [
        "経済", "投資", "株価", "インフレ", "物価", "景気", "中小企業", "金融"
    ],
    "business": [
        "企業", "決算", "業績", "買収", "M&A", "事業", "設備投資"
    ],
    "tech": [
        "テック", "IT", "ソフトウェア", "クラウド", "データセンター", "サイバー"
    ]
}


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


def is_excluded(text: str) -> bool:
    return any(word in text for word in EXCLUDE_KEYWORDS)


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
        return strip_html(res.text)[:2000]
    except Exception:
        return ""


def dedupe_news(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []

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


def load_user_settings(path: str = "user_settings.json") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def collect_genre_keywords(genres: List[str]) -> List[str]:
    keywords: List[str] = []
    for genre in genres:
        keywords.extend(GENRE_KEYWORDS.get(genre, []))
    return list(dict.fromkeys(keywords))


def fetch_news() -> List[Dict[str, Any]]:
    feed = feedparser.parse(RSS_URL)
    picked: List[Dict[str, Any]] = []

    for entry in getattr(feed, "entries", []):
        title = clean_text(getattr(entry, "title", ""))
        link = clean_text(getattr(entry, "link", ""))
        summary = strip_html(getattr(entry, "summary", ""))

        if not title or not link:
            continue

        base_text = f"{title} {summary}"
        if is_excluded(base_text):
            continue

        picked.append({
            "title": title,
            "summary": summary,
            "link": link,
            "short_link": shorten_url(link),
            "source": extract_source_name(link),
            "article_text": ""
        })

        if len(picked) >= MAX_FETCH_ITEMS:
            break

    return dedupe_news(picked)


def news_matches_keywords(news_item: Dict[str, Any], keywords: List[str]) -> bool:
    base_text = f"{news_item.get('title', '')} {news_item.get('summary', '')}"
    if any(word in base_text for word in keywords):
        return True

    if not news_item.get("article_text"):
        news_item["article_text"] = fetch_article_text(news_item["link"])

    article = news_item.get("article_text", "")
    if article and any(word in article for word in keywords):
        return True

    return False


def apply_plan_rules(user: Dict[str, Any]) -> Dict[str, Any]:
    plan = user.get("plan", "free")
    raw_max_items = int(user.get("max_items", DEFAULT_MAX_ITEMS))
    raw_genres = user.get("genres", [])

    if plan == "free":
        return {
            "plan": "free",
            "max_items": min(raw_max_items, 3),
            "genres": [],
            "analysis_depth": "light"
        }

    if plan == "light":
        return {
            "plan": "light",
            "max_items": min(raw_max_items, 5),
            "genres": raw_genres[:2],
            "analysis_depth": "medium"
        }

    if plan == "premium":
        return {
            "plan": "premium",
            "max_items": min(raw_max_items, 8),
            "genres": raw_genres[:6],
            "analysis_depth": "deep"
        }

    return {
        "plan": plan,
        "max_items": min(raw_max_items, 5),
        "genres": raw_genres,
        "analysis_depth": "medium"
    }


def filter_news_for_user(all_news: List[Dict[str, Any]], user: Dict[str, Any]) -> tuple[List[Dict[str, Any]], bool]:
    rules = apply_plan_rules(user)
    plan = rules["plan"]
    max_items = rules["max_items"]
    genres = rules["genres"]

    if plan == "free":
        return all_news[:max_items], False

    keywords = collect_genre_keywords(genres)
    if not keywords:
        return all_news[:max_items], False

    matched = [item for item in all_news if news_matches_keywords(item, keywords)]
    if matched:
        return matched[:max_items], False

    fallback_items = all_news[:min(3, len(all_news))]
    return fallback_items, True


def analyze(news: List[Dict[str, Any]], user: Dict[str, Any], used_fallback: bool) -> Dict[str, List[str]]:
    if not news:
        return {"summary": ["該当ニュースなし"], "impact": ["★ ニュース取得なし"]}

    rules = apply_plan_rules(user)
    plan = rules["plan"]
    genres = rules["genres"]
    depth = rules["analysis_depth"]

    lines = []
    for i, n in enumerate(news):
        lines.append(f"{i+1}. {n['title']} ({n['source']})")

    news_text = "\n".join(lines)
    genre_text = ", ".join(genres) if genres else "全般"

    prompt = f"""
以下のニュースを日本語で実務向けに整理しろ。
必ずJSONのみで返すこと。

条件:
- summary は配列
- impact は配列
- summary はニュースごとの要点を簡潔に
- impact は実務・投資・生活への影響
- impact の各要素は ★ / ★★ / ★★★ のいずれかで始める
- 断定しすぎない
- plan が free の場合は浅め
- plan が light の場合は要点＋実務影響を中程度に具体化
- plan が premium の場合は実務判断に使えるよう少し深めに書く
- fallback_used が true の場合、無理に「該当」と言い張らず、近い一般ニュースとして整理
- summary の件数はニュース件数に合わせる

plan: {plan}
analysis_depth: {depth}
genres: {genre_text}
fallback_used: {str(used_fallback).lower()}

ニュース:
{news_text}

返却形式:
{{
  "summary": ["...", "..."],
  "impact": ["★★ ...", "★ ..."]
}}
"""

    try:
        res = client.responses.create(
            model="gpt-5",
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
        summary = ["該当ニュースなし"]
    if not impact:
        impact = ["★ 影響整理なし"]

    return {
        "summary": summary,
        "impact": impact
    }


def build_messages(news: List[Dict[str, Any]], user: Dict[str, Any], used_fallback: bool) -> List[str]:
    analysis = analyze(news, user, used_fallback)
    name = user.get("name", "配信")
    rules = apply_plan_rules(user)
    plan = rules["plan"]
    genres = rules["genres"]

    header = f"【{name}向けニュース】"

    if plan == "light":
        genre_label = " / ".join(genres) if genres else "選択ジャンル"
        header += f"\nプラン: light | ジャンル: {genre_label}"

    if plan == "premium":
        genre_label = " / ".join(genres) if genres else "選択ジャンル"
        header += f"\nプラン: premium | ジャンル: {genre_label}\n※プレミアム分析"

    if used_fallback:
        header += "\n※該当が薄かったため近い一般ニュースも含む"

    if not news:
        main_text = "該当ニュースなし"
    else:
        lines = []
        for i, n in enumerate(news):
            summary_line = analysis["summary"][i] if i < len(analysis["summary"]) else n["title"]
            block = f"""{i+1}. {summary_line}
[{n['source']}]
{n['short_link']}"""
            lines.append(block)
        main_text = "\n\n".join(lines)

    impact_title = "--- こんな影響 ---"
    if plan == "premium":
        impact_title = "--- こんな影響 / 実務メモ ---"

    impact_text = "\n\n".join(analysis["impact"])

    msg1 = f"""{header}

{main_text}
"""

    msg2 = f"""{impact_title}

{impact_text}
"""

    messages: List[str] = []
    for m in [msg1, msg2]:
        messages.extend(chunk_text(m))

    return messages[:LINE_MAX_MESSAGE_OBJECTS]


def send(target_user_id: str, messages: List[str]) -> None:
    res = requests.post(
        LINE_URL,
        headers={
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "to": target_user_id,
            "messages": [{"type": "text", "text": m} for m in messages]
        },
        timeout=30
    )
    print("LINE status:", res.status_code)
    print("LINE response:", res.text)
    res.raise_for_status()


def main() -> None:
    settings = load_user_settings()
    users = settings.get("users", {})
    all_news = fetch_news()

    if not all_news:
        print("news fetch result: 0")
        return

    for user_id, user in users.items():
        if not user.get("active", True):
            print(f"skip inactive: {user_id}")
            continue

        filtered_news, used_fallback = filter_news_for_user(all_news, user)
        messages = build_messages(filtered_news, user, used_fallback)
        send(user_id, messages)
        print(f"sent to: {user.get('name', 'unknown')} ({user_id})")


if __name__ == "__main__":
    main()