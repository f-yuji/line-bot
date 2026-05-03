"""
悪材料フィルター。yfinance のニュース取得 + キーワードマッチングで判定。
個別要因での急落（市場全体が安定しているのに -15% 以上下落）のみチェック対象。
"""
import logging

logger = logging.getLogger(__name__)

BAD_NEWS_KEYWORDS = [
    "不祥事", "不正", "粉飾", "下方修正", "業績悪化", "赤字転落",
    "倒産", "民事再生", "上場廃止", "行政処分", "課徴金", "逮捕",
    "虚偽記載", "リコール", "自主回収", "大幅減益", "経営危機",
    "債務超過", "特別損失", "不正会計", "損失計上",
]

# 個別要因急落の判定基準
_INDIVIDUAL_DROP_THR = -15.0   # 下落率
_MARKET_SAFE_THR = -3.0        # この値より市場が安定していれば個別要因とみなす


def _is_individual_issue(item: dict) -> bool:
    """市場全体が安定しているのに大幅下落 → 個別悪材料の可能性"""
    drop = float(item.get("drop_pct") or 0.0)
    nikkei = float(item.get("nikkei_pct") or -999.0)
    return drop <= _INDIVIDUAL_DROP_THR and nikkei > _MARKET_SAFE_THR


def _check_yfinance_news(code: str) -> bool:
    try:
        import yfinance as yf
        ticker = yf.Ticker(f"{code}.T")
        news = ticker.news
        if not news:
            return False
        for article in news[:5]:
            title = article.get("title", "")
            if any(kw in title for kw in BAD_NEWS_KEYWORDS):
                logger.info("悪材料キーワード検出: %s → %s", code, title[:60])
                return True
    except Exception as e:
        logger.debug("news check error code=%s: %s", code, e)
    return False


def has_bad_news(item: dict) -> bool:
    """
    悪材料があれば True。True の場合はリバウンド通知をスキップ推奨。
    個別要因の急落（市場安定 & -15% 以下）のみ API を叩く。
    """
    if not _is_individual_issue(item):
        return False
    return _check_yfinance_news(item.get("code", ""))
