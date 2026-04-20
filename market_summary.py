#!/usr/bin/env python3
"""
市場動向要約モジュール
cron実行時に各市場データを取得・AI要約を生成してSupabaseに保存。
ユーザー問い合わせ時はキャッシュを即返し（AI呼び出しなし）。
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

# ─── 環境変数 ───
def _opt(name: str) -> str:
    return os.getenv(name, "").strip()

def _mode_env(base: str, mode: str, *, required: bool = False) -> str:
    mode_upper = (mode or "").strip().upper()
    for cand in ([f"{base}_{mode_upper}"] if mode_upper else []) + [base]:
        v = _opt(cand)
        if v:
            return v
    if required:
        raise KeyError(base)
    return ""

_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_SUPABASE_URL = _mode_env("SUPABASE_URL", _SUPABASE_MODE, required=True)
_SUPABASE_KEY = _mode_env("SUPABASE_KEY", _SUPABASE_MODE, required=True)
_OPENAI_API_KEY = _opt("OPENAI_API_KEY")

supabase = create_client(_SUPABASE_URL, _SUPABASE_KEY)
_openai = OpenAI(api_key=_OPENAI_API_KEY) if (_OPENAI_AVAILABLE and _OPENAI_API_KEY) else None

# ─── 市場定義 ───
MARKETS: dict[str, dict] = {
    "gold":      {"label": "ゴールド",     "ticker": "GC=F"},
    "us_stocks": {"label": "米株",         "ticker": "^GSPC"},
    "japan":     {"label": "日本株",       "ticker": "^N225"},
    "bitcoin":   {"label": "ビットコイン", "ticker": "BTC-USD"},
    "usdjpy":    {"label": "ドル円",       "ticker": "USDJPY=X"},
    "oil":       {"label": "原油",         "ticker": "CL=F"},
}

MARKET_LABEL_TO_KEY: dict[str, str] = {v["label"]: k for k, v in MARKETS.items()}
MARKET_CACHE_TTL_HOURS = 12


# ─── データ取得 ───

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
    """指定市場のメトリクスを取得（最大3回リトライ）"""
    if not HAS_YFINANCE:
        return None
    ticker = MARKETS[key]["ticker"]
    for attempt in range(3):
        try:
            result = _fetch_metrics_once(ticker)
            if result:
                return result
        except Exception as e:
            logger.warning("市場データ取得失敗 key=%s attempt=%d: %s", key, attempt + 1, e)
    return None


# ─── AI要約生成 ───

def _generate_ai_summary(key: str, metrics: dict) -> Optional[dict]:
    if not _openai:
        return None
    label = MARKETS[key]["label"]

    def fp(v):
        return f"{v:+.1f}%" if v is not None else "N/A"

    prompt = (
        f"以下の市場データを見て、投資家向けの相場コメントを生成してください。\n\n"
        f"市場: {label}\n"
        f"前日比: {fp(metrics.get('day_pct'))}\n"
        f"週比（5営業日）: {fp(metrics.get('week_pct'))}\n"
        f"月比（20営業日）: {fp(metrics.get('month_pct'))}\n"
        f"52週高値からの距離: {fp(metrics.get('from_high_pct'))}\n\n"
        "以下の形式のみで出力してください（各50字以内）：\n"
        "要約: （全体的なトレンドを1文で）\n"
        "背景: （価格動向の背景要因を1文で）\n"
        "注目点: （投資家が注意すべき点を1文で）\n\n"
        "断定口調は避ける。投資助言禁止。"
    )
    try:
        resp = _openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        raw = resp.choices[0].message.content.strip()
        result: dict[str, str] = {"summary": "", "background": "", "note": ""}
        for line in raw.splitlines():
            if line.startswith("要約:"):
                result["summary"] = line[3:].strip()
            elif line.startswith("背景:"):
                result["background"] = line[3:].strip()
            elif line.startswith("注目点:"):
                result["note"] = line[4:].strip()
        return result
    except Exception as e:
        logger.error("AI要約生成エラー key=%s: %s", key, e)
        return None


# ─── フォーマット ───

def _format_content(key: str, metrics: dict, ai: Optional[dict], fetched_at: datetime) -> str:
    label = MARKETS[key]["label"]
    fetched_str = fetched_at.strftime("%m/%d %H:%M")

    def fp(v):
        return f"{v:+.1f}%" if v is not None else "N/A"

    lines = [
        f"{label}相場",
        f"（取得: {fetched_str}）",
        "",
        f"前日比: {fp(metrics.get('day_pct'))}",
        f"週比: {fp(metrics.get('week_pct'))}",
        f"月比: {fp(metrics.get('month_pct'))}",
        f"高値から: {fp(metrics.get('from_high_pct'))}",
    ]
    if ai:
        if ai.get("summary"):
            lines += ["", f"要約:\n{ai['summary']}"]
        if ai.get("background"):
            lines += ["", f"背景:\n{ai['background']}"]
        if ai.get("note"):
            lines += ["", f"注目点:\n{ai['note']}"]
    else:
        lines += ["", "（AI解説は現在利用不可）"]
    return "\n".join(lines)


# ─── キャッシュ保存・読み込み ───

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
        logger.info("市場キャッシュ保存完了: %s", key)
    except Exception as e:
        logger.error("市場キャッシュ保存エラー key=%s: %s", key, e)


def load_market_cache(key: str) -> Optional[str]:
    """キャッシュからコンテンツを返す。TTL切れなら直近データ＋注記を返す"""
    try:
        res = supabase.table("market_cache").select("*").eq("key", key).execute()
        if not res.data:
            return None
        row = res.data[0]
        content = row.get("content") or ""
        if not content:
            return None

        now_jst = datetime.now(JST)
        expires_str = row.get("expires_at") or ""
        if expires_str:
            try:
                expires_dt = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
                if now_jst > expires_dt:
                    last_str = row.get("last_success_at") or row.get("created_at") or ""
                    try:
                        last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00")).astimezone(JST)
                        note = f"\n\n※最新取得に失敗したため、直近データを表示\n（取得: {last_dt.strftime('%m/%d %H:%M')}）"
                    except Exception:
                        note = "\n\n※最新データの取得に失敗しました"
                    return content + note
            except Exception:
                pass
        return content
    except Exception as e:
        logger.error("市場キャッシュ読み込みエラー key=%s: %s", key, e)
        return None


def get_market_reply(key: str) -> str:
    """ユーザーへの返信テキストを返す（キャッシュ優先）"""
    content = load_market_cache(key)
    if content:
        return content
    label = MARKETS.get(key, {}).get("label", key)
    return f"{label}の相場データがまだ準備できていません。\nしばらく待ってから試してください。"


# ─── cron エントリポイント ───

def run_market_update() -> None:
    """全市場のデータ取得・AI要約生成・キャッシュ保存（cronから呼び出す）"""
    logger.info("=== 市場要約更新開始 ===")
    now_jst = datetime.now(JST)

    if now_jst.weekday() >= 5:
        logger.info("土日のためスキップ")
        return

    for key in MARKETS:
        try:
            metrics = fetch_market_metrics(key)
            if metrics is None:
                logger.warning("市場データ取得失敗（スキップ）: %s", key)
                continue
            ai = _generate_ai_summary(key, metrics)
            content = _format_content(key, metrics, ai, now_jst)
            _save_market_cache(key, content, metrics, now_jst)
        except Exception as e:
            logger.error("市場更新エラー key=%s: %s", key, e)

    logger.info("=== 市場要約更新完了 ===")
