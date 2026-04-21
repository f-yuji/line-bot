#!/usr/bin/env python3
"""
相場サマリーのキャッシュ生成と返信用整形。
cron: python market_summary.py
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    OpenAI = None
    _OPENAI_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _mode_env(base: str, mode: str, *, required: bool = False) -> str:
    mode_upper = (mode or "").strip().upper()
    for cand in ([f"{base}_{mode_upper}"] if mode_upper else []) + [base]:
        value = _opt(cand)
        if value:
            return value
    if required:
        raise KeyError(base)
    return ""


_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_SUPABASE_URL = _mode_env("SUPABASE_URL", _SUPABASE_MODE, required=True)
_SUPABASE_KEY = _mode_env("SUPABASE_KEY", _SUPABASE_MODE, required=True)
_OPENAI_API_KEY = _opt("OPENAI_API_KEY")

supabase = create_client(_SUPABASE_URL, _SUPABASE_KEY)
_openai = OpenAI(api_key=_OPENAI_API_KEY) if (_OPENAI_AVAILABLE and _OPENAI_API_KEY) else None

MARKETS: dict[str, dict[str, str]] = {
    "japan": {"label": "日本株", "ticker": "^N225"},
    "usdjpy": {"label": "ドル円", "ticker": "USDJPY=X"},
    "us_stocks": {"label": "米株", "ticker": "^GSPC"},
    "gold": {"label": "ゴールド", "ticker": "GC=F"},
    "bitcoin": {"label": "ビットコイン", "ticker": "BTC-USD"},
    "oil": {"label": "原油", "ticker": "CL=F"},
}

MARKET_CACHE_TTL_HOURS = 12


def _fetch_metrics_once(ticker: str) -> Optional[dict]:
    hist = yf.Ticker(ticker).history(period="1y")
    if hist.empty or len(hist) < 2:
        return None

    closes = hist["Close"]
    highs = hist["High"]

    today_price = float(closes.iloc[-1])
    prev_day = float(closes.iloc[-2])
    day_pct = round((today_price - prev_day) / prev_day * 100, 2) if prev_day else None

    week_pct = None
    if len(closes) >= 6:
        prev_week = float(closes.iloc[-6])
        week_pct = round((today_price - prev_week) / prev_week * 100, 2) if prev_week else None

    month_pct = None
    if len(closes) >= 21:
        prev_month = float(closes.iloc[-21])
        month_pct = round((today_price - prev_month) / prev_month * 100, 2) if prev_month else None

    high_52w = float(highs.max())
    from_high_pct = round((today_price - high_52w) / high_52w * 100, 2) if high_52w else None

    return {
        "price": round(today_price, 4),
        "day_pct": day_pct,
        "week_pct": week_pct,
        "month_pct": month_pct,
        "from_high_pct": from_high_pct,
        "high_52w": round(high_52w, 4),
    }


def fetch_market_metrics(key: str) -> Optional[dict]:
    if not HAS_YFINANCE:
        return None

    ticker = MARKETS[key]["ticker"]
    for attempt in range(3):
        try:
            metrics = _fetch_metrics_once(ticker)
            if metrics:
                return metrics
        except Exception as e:
            logger.warning("market fetch error key=%s attempt=%d: %s", key, attempt + 1, e)
    return None


def _generate_ai_summary(key: str, metrics: dict) -> Optional[str]:
    if not _openai:
        return None

    label = MARKETS[key]["label"]

    def fp(value):
        return f"{value:+.1f}%" if value is not None else "N/A"

    prompt = (
        f"相場: {label}\n"
        f"前日比: {fp(metrics.get('day_pct'))}\n"
        f"週次: {fp(metrics.get('week_pct'))}\n"
        f"月次: {fp(metrics.get('month_pct'))}\n"
        f"52週高値から: {fp(metrics.get('from_high_pct'))}\n\n"
        "この数字からわかる雰囲気を日本語で1文だけ短くまとめて。"
        "投資助言っぽくせず、状況説明だけにして。"
    )

    try:
        response = _openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error("market ai summary error key=%s: %s", key, e)
        return None


def _format_content(key: str, metrics: dict, ai: Optional[str], fetched_at: datetime) -> str:
    label = MARKETS[key]["label"]
    fetched_str = fetched_at.strftime("%m/%d %H:%M")

    def fp(value):
        return f"{value:+.1f}%" if value is not None else "N/A"

    lines = [
        f"{label}相場",
        f"取得: {fetched_str}",
        "",
        f"前日比 {fp(metrics.get('day_pct'))}",
        f"週次   {fp(metrics.get('week_pct'))}",
        f"月次   {fp(metrics.get('month_pct'))}",
        f"高値差 {fp(metrics.get('from_high_pct'))}",
    ]
    if ai:
        lines.extend(["", ai])
    return "\n".join(lines)


def _save_market_cache(key: str, content: str, raw_metrics: dict, now_jst: datetime) -> None:
    expires_at = (now_jst + timedelta(hours=MARKET_CACHE_TTL_HOURS)).isoformat()
    record = {
        "key": key,
        "content": content,
        "raw_metrics": raw_metrics,
        "created_at": now_jst.isoformat(),
        "expires_at": expires_at,
        "fetch_status": "ok",
        "last_success_at": now_jst.isoformat(),
    }
    try:
        supabase.table("market_cache").upsert(record).execute()
        logger.info("market cache saved key=%s", key)
    except Exception as e:
        logger.error("market cache save error key=%s: %s", key, e)


def _get_market_cache_row(key: str) -> Optional[dict]:
    try:
        response = supabase.table("market_cache").select("*").eq("key", key).execute()
        if response.data:
            return response.data[0]
    except Exception as e:
        logger.error("market cache row load error key=%s: %s", key, e)
    return None


def _is_cache_fresh(row: Optional[dict], now_jst: datetime) -> bool:
    if not row:
        return False
    expires_str = row.get("expires_at") or ""
    if not expires_str:
        return False
    try:
        expires_dt = datetime.fromisoformat(expires_str.replace("Z", "+00:00")).astimezone(JST)
    except Exception:
        return False
    return now_jst <= expires_dt


def load_market_cache(key: str) -> Optional[str]:
    try:
        row = _get_market_cache_row(key)
        if not row:
            return None
        content = row.get("content") or ""
        if not content:
            return None

        now_jst = datetime.now(JST)
        if _is_cache_fresh(row, now_jst):
            return content

        last_str = row.get("last_success_at") or row.get("created_at") or ""
        try:
            last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00")).astimezone(JST)
            note = f"\n\n※ 最新取得に失敗したため、直近キャッシュを表示\n取得: {last_dt.strftime('%m/%d %H:%M')}"
        except Exception:
            note = "\n\n※ 最新取得に失敗したため、直近キャッシュを表示"
        return content + note
    except Exception as e:
        logger.error("market cache load error key=%s: %s", key, e)
        return None


def _refresh_market_cache_if_needed(key: str) -> None:
    now_jst = datetime.now(JST)
    cache_row = _get_market_cache_row(key)
    if _is_cache_fresh(cache_row, now_jst):
        return

    metrics = fetch_market_metrics(key)
    if metrics is None:
        logger.warning("market fetch failed on demand key=%s", key)
        return

    ai = _generate_ai_summary(key, metrics)
    content = _format_content(key, metrics, ai, now_jst)
    _save_market_cache(key, content, metrics, now_jst)


def get_market_reply(key: str) -> str:
    _refresh_market_cache_if_needed(key)
    content = load_market_cache(key)
    if content:
        return content
    label = MARKETS.get(key, {}).get("label", key)
    return f"{label}の相場データがまだない\n少し待ってから試してみて"


def get_all_markets_reply() -> str:
    parts = []
    for key in MARKETS:
        _refresh_market_cache_if_needed(key)
        content = load_market_cache(key)
        if content:
            parts.append(content)
        else:
            parts.append(f"{MARKETS[key]['label']}相場\nデータ未取得")
    return "\n\n------\n\n".join(parts)


def run_market_update() -> None:
    logger.info("=== market update start ===")
    now_jst = datetime.now(JST)

    for key in MARKETS:
        try:
            cache_row = _get_market_cache_row(key)
            if _is_cache_fresh(cache_row, now_jst):
                logger.info("market cache still fresh; skip update key=%s", key)
                continue
            metrics = fetch_market_metrics(key)
            if metrics is None:
                logger.warning("market fetch failed key=%s", key)
                continue
            ai = _generate_ai_summary(key, metrics)
            content = _format_content(key, metrics, ai, now_jst)
            _save_market_cache(key, content, metrics, now_jst)
        except Exception as e:
            logger.error("market update error key=%s: %s", key, e)

    logger.info("=== market update done ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_market_update()
