"""
Bad-news filter for rebound candidates.

This module keeps the old has_bad_news(item) boolean API, and adds
analyze_bad_news(item) for severity, score, keywords, and future features.
Naphtha and energy-related terms are recorded as feature candidates only;
they do not exclude a stock in this phase.
"""
import logging

logger = logging.getLogger(__name__)

STRONG_BAD_NEWS_KEYWORDS = [
    "下方修正",
    "赤字転落",
    "不祥事",
    "不正",
    "粉飾",
    "上場廃止",
    "債務超過",
    "民事再生",
    "行政処分",
    "課徴金",
    "逮捕",
    "虚偽記載",
    "大幅減益",
    "経営危機",
    "不正会計",
    "特別損失",
    "減損",
]

MEDIUM_BAD_NEWS_KEYWORDS = [
    "減収",
    "減益",
    "受注減",
    "需要減",
    "販売不振",
    "通期見通し",
    "業績予想",
    "ガイダンス",
    "未達",
    "下振れ",
    "減配",
    "無配",
    "延期",
    "中止",
    "撤退",
]

WEAK_BAD_NEWS_KEYWORDS = [
    "リコール",
    "自主回収",
    "訴訟",
    "調査",
    "警告",
]

NAPHTHA_KEYWORDS = [
    "ナフサ",
    "原油高",
    "樹脂",
    "塗料",
    "溶剤",
    "防水材",
    "建材",
]

BAD_NEWS_KEYWORDS = (
    STRONG_BAD_NEWS_KEYWORDS + MEDIUM_BAD_NEWS_KEYWORDS + WEAK_BAD_NEWS_KEYWORDS
)


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def should_check_bad_news(item: dict) -> bool:
    drop_pct = _to_float(item.get("drop_pct"))
    nikkei_pct = item.get("nikkei_pct")
    volume_ratio = _to_float(item.get("volume_ratio"))
    news_text = _collect_local_news_text(item)

    if drop_pct <= -8.0:
        return True
    if nikkei_pct is not None and drop_pct <= _to_float(nikkei_pct) - 5.0:
        return True
    if drop_pct < 0 and volume_ratio >= 3.0:
        return True
    if news_text:
        return True
    return False


def _collect_local_news_text(item: dict) -> str:
    parts: list[str] = []
    for key in ("news_title", "news_summary", "headline", "title", "memo", "reason"):
        val = item.get(key)
        if val:
            parts.append(str(val))
    for key in ("news", "articles", "related_news"):
        val = item.get(key)
        if isinstance(val, list):
            for obj in val[:10]:
                if isinstance(obj, dict):
                    parts.append(str(obj.get("title") or ""))
                    parts.append(str(obj.get("summary") or ""))
                else:
                    parts.append(str(obj))
    return " ".join(p for p in parts if p)


def _check_yfinance_news(code: str) -> list[str]:
    if not code:
        return []
    try:
        import yfinance as yf

        ticker = yf.Ticker(f"{code}.T")
        news = ticker.news or []
        titles: list[str] = []
        for article in news[:5]:
            title = article.get("title") or article.get("content", {}).get("title") or ""
            if title:
                titles.append(str(title))
        return titles
    except Exception as e:
        logger.debug("news check error code=%s: %s", code, e)
        return []


def _match_keywords(text: str, keywords: list[str]) -> list[str]:
    return [kw for kw in keywords if kw and kw in text]


def analyze_bad_news(item: dict) -> dict:
    """
    Return detailed bad-news analysis.

    Shape:
    {
      has_bad_news, bad_news_score, severity, matched_keywords, reason,
      energy_naphtha_score
    }
    """
    texts = [_collect_local_news_text(item)]
    if should_check_bad_news(item):
        texts.extend(_check_yfinance_news(str(item.get("code") or "")))
    text = " ".join(t for t in texts if t)

    strong = _match_keywords(text, STRONG_BAD_NEWS_KEYWORDS)
    medium = _match_keywords(text, MEDIUM_BAD_NEWS_KEYWORDS)
    weak = _match_keywords(text, WEAK_BAD_NEWS_KEYWORDS)
    naphtha = _match_keywords(text, NAPHTHA_KEYWORDS)

    if strong:
        severity = "strong"
        score = 100.0
        matched = strong
    elif medium:
        severity = "medium"
        score = min(70.0, 40.0 + 10.0 * (len(medium) - 1))
        matched = medium
    elif weak:
        severity = "weak"
        score = min(30.0, 10.0 + 5.0 * (len(weak) - 1))
        matched = weak
    else:
        severity = "none"
        score = 0.0
        matched = []

    reason = ""
    if matched:
        reason = f"{severity}: " + ", ".join(matched[:5])

    return {
        "has_bad_news": severity in {"medium", "strong"},
        "bad_news_score": score,
        "severity": severity,
        "matched_keywords": matched,
        "reason": reason,
        "energy_naphtha_score": float(len(naphtha) * 10),
        "naphtha_keywords": naphtha,
    }


def has_bad_news(item: dict) -> bool:
    """Backward-compatible boolean API."""
    return bool(analyze_bad_news(item).get("has_bad_news"))
