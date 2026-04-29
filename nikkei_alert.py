#!/usr/bin/env python3
"""
日経225 急落検知・通知モジュール
cron: python nikkei_alert.py  (15:40 JST 実行)
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

try:
    import yfinance as yf
    import pandas as pd
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    OpenAI = None
    _OPENAI_AVAILABLE = False

import requests as _requests
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── 閾値定数（後で調整可能）───
DROP_LIST_THRESHOLD = -2.0      # 急落一覧閾値（%）
ALERT_THRESHOLD = -9.0          # push通知閾値（%）。一覧より厳しめにする
NIKKEI_GAP_THRESHOLD = -1.5     # 指数乖離閾値（pt）
AI_COMMENT_CACHE_TTL_DAYS = 7
DROP_CACHE_KEY = "latest"
DROP_CACHE_TTL_HOURS = 12
DROP_VALUATION_DISPLAY_LIMIT = 10
MAX_ALERT_SIGNALS = 10

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"

CIRCLE_NUMS = "①②③④⑤⑥⑦⑧⑨⑩"


# ─── 環境変数（app.py と同方式）───
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


SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
SUPABASE_URL = _mode_env("SUPABASE_URL", SUPABASE_MODE, required=True)
SUPABASE_KEY = _mode_env("SUPABASE_KEY", SUPABASE_MODE, required=True)
LINE_MODE = _opt("LINE_MODE")
LINE_CHANNEL_ACCESS_TOKEN = _mode_env("LINE_CHANNEL_ACCESS_TOKEN", LINE_MODE, required=True)
OPENAI_API_KEY = _opt("OPENAI_API_KEY")
JQUANTS_API_KEY = _opt("JQUANTS_API_KEY") or _opt("JQUANTS_REFRESH_TOKEN")
JQUANTS_API_BASE = "https://api.jquants.com/v2"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY) if (_OPENAI_AVAILABLE and OPENAI_API_KEY) else None


# ─── 日経225銘柄 {証券コード: 会社名} ───
NIKKEI225: dict[str, str] = {
    "1332": "ニッスイ",
    "1333": "マルハニチロ",
    "1605": "INPEX",
    "1721": "コムシスHD",
    "1801": "大成建設",
    "1802": "大林組",
    "1803": "清水建設",
    "1808": "長谷工コーポレーション",
    "1812": "鹿島建設",
    "1925": "大和ハウス工業",
    "1928": "積水ハウス",
    "1963": "日揮HD",
    "2002": "日清製粉グループ",
    "2269": "明治HD",
    "2282": "日本ハム",
    "2413": "エムスリー",
    "2432": "DeNA",
    "2501": "サッポロHD",
    "2502": "アサヒグループHD",
    "2503": "キリンHD",
    "2531": "宝HD",
    "2768": "双日",
    "2801": "キッコーマン",
    "2802": "味の素",
    "2871": "ニチレイ",
    "2914": "JT",
    "3086": "J.フロントリテイリング",
    "3099": "三越伊勢丹HD",
    "3382": "セブン&アイHD",
    "3401": "帝人",
    "3402": "東レ",
    "3407": "旭化成",
    "3436": "SUMCO",
    "3659": "ネクソン",
    "3861": "王子HD",
    "3863": "日本製紙",
    "4004": "レゾナック・HD",
    "4005": "住友化学",
    "4021": "日産化学",
    "4042": "東ソー",
    "4043": "トクヤマ",
    "4061": "デンカ",
    "4063": "信越化学工業",
    "4151": "協和キリン",
    "4183": "三井化学",
    "4188": "三菱ケミカルグループ",
    "4208": "UBE",
    "4272": "日本化薬",
    "4452": "花王",
    "4502": "武田薬品工業",
    "4503": "アステラス製薬",
    "4507": "塩野義製薬",
    "4519": "中外製薬",
    "4523": "エーザイ",
    "4528": "小野薬品工業",
    "4543": "テルモ",
    "4568": "第一三共",
    "4578": "大塚HD",
    "4661": "オリエンタルランド",
    "4689": "LINEヤフー",
    "4704": "トレンドマイクロ",
    "4751": "サイバーエージェント",
    "4755": "楽天グループ",
    "4901": "富士フイルムHD",
    "4902": "コニカミノルタ",
    "5019": "出光興産",
    "5020": "ENEOSホールディングス",
    "5101": "横浜ゴム",
    "5108": "ブリヂストン",
    "5201": "AGC",
    "5214": "日本電気硝子",
    "5232": "住友大阪セメント",
    "5233": "太平洋セメント",
    "5301": "東海カーボン",
    "5332": "TOTO",
    "5333": "日本碍子",
    "5401": "日本製鉄",
    "5406": "神戸製鋼所",
    "5411": "JFEホールディングス",
    "5631": "日本製鋼所",
    "5706": "三井金属鉱業",
    "5707": "東邦亜鉛",
    "5711": "三菱マテリアル",
    "5713": "住友金属鉱山",
    "5714": "DOWAホールディングス",
    "5801": "古河電気工業",
    "5802": "住友電気工業",
    "5803": "フジクラ",
    "6098": "リクルートHD",
    "6103": "オークマ",
    "6113": "アマダ",
    "6146": "ディスコ",
    "6178": "日本郵政",
    "6273": "SMC",
    "6301": "コマツ",
    "6302": "住友重機械工業",
    "6305": "日立建機",
    "6326": "クボタ",
    "6361": "荏原製作所",
    "6367": "ダイキン工業",
    "6370": "栗田工業",
    "6376": "日機装",
    "6471": "NSK",
    "6472": "NTN",
    "6473": "ジェイテクト",
    "6501": "日立製作所",
    "6503": "三菱電機",
    "6504": "富士電機",
    "6506": "安川電機",
    "6594": "ニデック",
    "6645": "オムロン",
    "6674": "GSユアサ",
    "6701": "NEC",
    "6702": "富士通",
    "6723": "ルネサスエレクトロニクス",
    "6724": "セイコーエプソン",
    "6752": "パナソニックHD",
    "6753": "シャープ",
    "6758": "ソニーグループ",
    "6762": "TDK",
    "6770": "アルプスアルパイン",
    "6857": "アドバンテスト",
    "6861": "キーエンス",
    "6902": "デンソー",
    "6920": "レーザーテック",
    "6952": "カシオ計算機",
    "6954": "ファナック",
    "6971": "京セラ",
    "6976": "太陽誘電",
    "6981": "村田製作所",
    "7003": "三井E&S",
    "7011": "三菱重工業",
    "7012": "川崎重工業",
    "7013": "IHI",
    "7182": "ゆうちょ銀行",
    "7201": "日産自動車",
    "7202": "いすゞ自動車",
    "7203": "トヨタ自動車",
    "7205": "日野自動車",
    "7211": "三菱自動車工業",
    "7261": "マツダ",
    "7267": "本田技研工業",
    "7269": "スズキ",
    "7270": "SUBARU",
    "7272": "ヤマハ発動機",
    "7731": "ニコン",
    "7733": "オリンパス",
    "7735": "SCREENホールディングス",
    "7741": "HOYA",
    "7751": "キヤノン",
    "7752": "リコー",
    "7762": "シチズン時計",
    "7832": "バンダイナムコHD",
    "7911": "TOPPANホールディングス",
    "7912": "大日本印刷",
    "7951": "ヤマハ",
    "8001": "伊藤忠商事",
    "8002": "丸紅",
    "8015": "豊田通商",
    "8031": "三井物産",
    "8035": "東京エレクトロン",
    "8053": "住友商事",
    "8058": "三菱商事",
    "8113": "ユニ・チャーム",
    "8233": "高島屋",
    "8252": "丸井グループ",
    "8253": "クレディセゾン",
    "8267": "イオン",
    "8306": "三菱UFJフィナンシャルグループ",
    "8308": "りそなHD",
    "8309": "三井住友トラストHD",
    "8316": "三井住友フィナンシャルグループ",
    "8354": "ふくおかフィナンシャルグループ",
    "8411": "みずほフィナンシャルグループ",
    "8601": "大和証券グループ本社",
    "8604": "野村HD",
    "8628": "松井証券",
    "8630": "SOMPOホールディングス",
    "8725": "MS&ADインシュアランスグループHD",
    "8750": "第一生命HD",
    "8766": "東京海上HD",
    "8795": "T&Dホールディングス",
    "9001": "東武鉄道",
    "9005": "東急",
    "9007": "小田急電鉄",
    "9008": "京王電鉄",
    "9009": "京成電鉄",
    "9020": "東日本旅客鉄道",
    "9021": "西日本旅客鉄道",
    "9022": "東海旅客鉄道",
    "9041": "近鉄グループHD",
    "9044": "南海電気鉄道",
    "9045": "京阪HD",
    "9064": "ヤマトHD",
    "9101": "日本郵船",
    "9104": "商船三井",
    "9107": "川崎汽船",
    "9202": "ANAホールディングス",
    "9301": "三菱倉庫",
    "9432": "NTT",
    "9433": "KDDI",
    "9434": "ソフトバンク",
    "9501": "東京電力HD",
    "9502": "中部電力",
    "9503": "関西電力",
    "9531": "東京ガス",
    "9532": "大阪ガス",
    "9602": "東宝",
    "9735": "セコム",
    "9766": "コナミグループ",
    "9983": "ファーストリテイリング",
    "9984": "ソフトバンクグループ",
}

_HIGH_DIVIDEND_CODES = {"8058", "8001", "8031", "8053", "8002", "2914", "9432", "9433", "8306", "8316", "8411"}
_company_profile_cache: dict[str, dict | None] = {}
_valuation_cache: dict[str, dict | None] = {}


def _fp(value: float | None) -> str:
    return f"{value:+.1f}%" if value is not None else "N/A"


def _fmt_ratio(value: float | None, *, digits: int = 1, suffix: str = "") -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}{suffix}"


def _fmt_dividend_yield(value: float | None, status: str | None = None) -> str:
    if status == "missing":
        return "取得不可"
    if status == "invalid":
        return "N/A"
    if value is None:
        return "取得不可"
    if abs(value) < 0.05:
        return "無配"
    return f"{value:.1f}%"


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _score_bucket(value: float | None, buckets: list[tuple[float, int]], *, reverse: bool = False) -> int:
    if value is None:
        return 0
    if reverse:
        for threshold, score in buckets:
            if value >= threshold:
                return score
    else:
        for threshold, score in buckets:
            if value <= threshold:
                return score
    return 0


def _valuation_score(
    per: float | None,
    pbr: float | None,
    dividend_pct: float | None,
    from_high_pct: float | None,
) -> int:
    score = 20
    score += _score_bucket(per, [(8, 28), (12, 23), (16, 18), (20, 12), (30, 6)])
    score += _score_bucket(pbr, [(0.8, 28), (1.0, 23), (1.5, 16), (2.0, 9), (3.0, 4)])
    score += _score_bucket(dividend_pct, [(4.0, 12), (3.0, 9), (2.0, 6), (1.0, 3)], reverse=True)

    if from_high_pct is not None:
        high_gap = abs(min(from_high_pct, 0))
        score += _score_bucket(high_gap, [(35, 12), (25, 9), (15, 6), (8, 3)], reverse=True)

    return max(5, min(95, score))


def _format_day_change_text(price: float | None, day_pct: float | None) -> str:
    if price is None or day_pct is None or day_pct <= -100:
        return _fp(day_pct)
    prev_price = price / (1 + day_pct / 100)
    day_diff = price - prev_price
    return f"{day_diff:+,.0f}円（{_fp(day_pct)}）"


def get_valuation_metrics(code: str) -> dict | None:
    if code in _valuation_cache:
        return _valuation_cache[code]

    if not HAS_YFINANCE:
        _valuation_cache[code] = None
        return None

    try:
        info = yf.Ticker(f"{code}.T").info or {}
        forward_per = _to_float(info.get("forwardPE"))
        trailing_per = _to_float(info.get("trailingPE"))
        per = forward_per if forward_per and forward_per > 0 else trailing_per
        if per is not None and per <= 0:
            per = None
        pbr = _to_float(info.get("priceToBook"))
        if pbr is not None and pbr <= 0:
            pbr = None
        dividend_yield = _to_float(info.get("dividendYield"))
        if dividend_yield is None:
            dividend_pct = None
            dividend_status = "missing"
        elif dividend_yield <= 1:
            dividend_pct = dividend_yield * 100
            dividend_status = "ok"
        else:
            dividend_pct = dividend_yield
            dividend_status = "ok"
        if dividend_pct is not None and dividend_pct > 20:
            dividend_pct = None
            dividend_status = "invalid"

        result = {
            "per": per,
            "pbr": pbr,
            "dividend_yield_pct": dividend_pct,
            "dividend_yield_status": dividend_status,
        }
        _valuation_cache[code] = result
        return result
    except Exception as e:
        logger.warning("valuation info load error code=%s: %s", code, e)
        _valuation_cache[code] = None
        return None


def _enrich_drop_valuations(drops: list[dict], limit: int = DROP_VALUATION_DISPLAY_LIMIT) -> list[dict]:
    """急落リストにPER/PBR/配当を埋め込む。cron側だけで呼ぶ想定。"""
    enriched: list[dict] = []
    for i, stock in enumerate(drops):
        item = dict(stock)
        if i < limit:
            item["valuation"] = get_valuation_metrics(str(stock.get("code", ""))) or {}
        enriched.append(item)
    return enriched


def _build_stock_snapshot(code: str, name: str, closes, highs, fetched_at: str) -> dict | None:
    prices = closes.dropna()
    high_values = highs.dropna()
    if len(prices) < 2 or high_values.empty:
        return None

    prev = float(prices.iloc[-2])
    today = float(prices.iloc[-1])
    if prev == 0:
        return None

    day_pct = round((today - prev) / prev * 100, 2)

    week_pct = None
    if len(prices) >= 6:
        prev_week = float(prices.iloc[-6])
        week_pct = round((today - prev_week) / prev_week * 100, 2) if prev_week else None

    month_pct = None
    if len(prices) >= 21:
        prev_month = float(prices.iloc[-21])
        month_pct = round((today - prev_month) / prev_month * 100, 2) if prev_month else None

    high_52w = float(high_values.max())
    from_high_pct = round((today - high_52w) / high_52w * 100, 2) if high_52w else None

    return {
        "code": code,
        "name": name,
        "change_pct": day_pct,
        "day_pct": day_pct,
        "week_pct": week_pct,
        "month_pct": month_pct,
        "from_high_pct": from_high_pct,
        "price": round(today, 1),
        "fetched_at": fetched_at,
    }


# ─── 株価取得 ───

def get_nikkei_change_pct() -> float | None:
    """日経平均の当日騰落率（%）を返す"""
    if not HAS_YFINANCE:
        return None
    try:
        hist = yf.Ticker("^N225").history(period="2d")
        if len(hist) < 2:
            return None
        prev = float(hist["Close"].iloc[-2])
        today = float(hist["Close"].iloc[-1])
        return round((today - prev) / prev * 100, 2) if prev else None
    except Exception as e:
        logger.error("日経平均取得エラー: %s", e)
        return None


def get_stock_changes() -> dict[str, dict]:
    """日経225全銘柄の変動情報を一括取得。"""
    if not HAS_YFINANCE:
        return {}
    tickers = [f"{c}.T" for c in NIKKEI225]
    try:
        fetched_at = datetime.now(JST).strftime("%m/%d %H:%M")
        df = yf.download(
            tickers,
            period="1y",
            interval="1d",
            progress=False,
            auto_adjust=True,
            threads=True,
        )
        if df.empty:
            return {}
        close = df["Close"] if isinstance(df.columns, pd.MultiIndex) else df[["Close"]]
        high = df["High"] if isinstance(df.columns, pd.MultiIndex) else df[["High"]]
        result: dict[str, dict] = {}
        for code, name in NIKKEI225.items():
            ticker = f"{code}.T"
            if ticker not in close.columns or ticker not in high.columns:
                continue
            snapshot = _build_stock_snapshot(code, name, close[ticker], high[ticker], fetched_at)
            if snapshot:
                result[code] = snapshot
        return result
    except Exception as e:
        logger.error("株価一括取得エラー: %s", e)
        return {}


def get_single_stock_change(code: str) -> dict | None:
    """単一銘柄の変動情報を取得"""
    if not HAS_YFINANCE or code not in NIKKEI225:
        return None
    try:
        hist = yf.Ticker(f"{code}.T").history(period="1y", auto_adjust=True)
        if len(hist) < 2:
            return None
        fetched_at = datetime.now(JST).strftime("%m/%d %H:%M")
        return _build_stock_snapshot(code, NIKKEI225[code], hist["Close"], hist["High"], fetched_at)
    except Exception as e:
        logger.error("単一株取得エラー: code=%s %s", code, e)
        return None


# ─── 急落一覧 ───

def get_drop_list(threshold: float = DROP_LIST_THRESHOLD) -> list[dict]:
    """閾値以上に下落した銘柄を騰落率の悪い順で返す"""
    stocks = get_stock_changes()
    drops = [s for s in stocks.values() if s["change_pct"] <= threshold]
    return sorted(drops, key=lambda x: x["change_pct"])


def save_drop_cache(drops: list[dict], nikkei_pct: float | None) -> None:
    """cronで取得した急落一覧をDBへ保存する。LINE表示はこのキャッシュを読む。"""
    now_utc = datetime.now(timezone.utc)
    fetched_at = drops[0].get("fetched_at") if drops else datetime.now(JST).strftime("%m/%d %H:%M")
    row = {
        "cache_key": DROP_CACHE_KEY,
        "drops": drops,
        "nikkei_pct": nikkei_pct,
        "fetched_at": fetched_at,
        "updated_at": now_utc.isoformat(),
    }
    try:
        supabase.table("nikkei_drop_cache").upsert(row, on_conflict="cache_key").execute()
        logger.info("急落株キャッシュ保存: drops=%d nikkei_pct=%s fetched_at=%s", len(drops), nikkei_pct, fetched_at)
    except Exception as e:
        logger.error("急落株キャッシュ保存エラー: %s", e)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _is_drop_cache_fresh(updated_at: str | None) -> bool:
    dt = _parse_dt(updated_at)
    if not dt:
        return False
    return datetime.now(timezone.utc) - dt.astimezone(timezone.utc) <= timedelta(hours=DROP_CACHE_TTL_HOURS)


def load_drop_cache() -> tuple[list[dict], float | None, str | None, str | None]:
    """最新の急落一覧キャッシュを返す。外部API取得はしない。"""
    try:
        res = (
            supabase.table("nikkei_drop_cache")
            .select("drops, nikkei_pct, fetched_at, updated_at")
            .eq("cache_key", DROP_CACHE_KEY)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return [], None, None, None
        row = rows[0]
        drops = row.get("drops") or []
        nikkei_pct = _to_float(row.get("nikkei_pct"))
        fetched_at = row.get("fetched_at") or None
        updated_at = row.get("updated_at") or None
        return drops, nikkei_pct, fetched_at, updated_at
    except Exception as e:
        logger.error("急落株キャッシュ取得エラー: %s", e)
        return [], None, None, None


def get_drop_list_for_reply() -> tuple[list[dict], float | None, str | None, bool]:
    """
    LINE返信用の急落一覧を返す。
    12時間以内のキャッシュがあればDBのみ。古ければその場で再取得する。
    戻り値の最後は stale_fallback（取得失敗で古いキャッシュを返したか）。
    """
    cached_drops, cached_nikkei, cached_fetched_at, updated_at = load_drop_cache()
    if _is_drop_cache_fresh(updated_at):
        return cached_drops, cached_nikkei, cached_fetched_at, False

    nikkei_pct = get_nikkei_change_pct()
    fresh_drops = _enrich_drop_valuations(get_drop_list())
    if fresh_drops or nikkei_pct is not None:
        save_drop_cache(fresh_drops, nikkei_pct)
        fetched_at = fresh_drops[0].get("fetched_at") if fresh_drops else datetime.now(JST).strftime("%m/%d %H:%M")
        return fresh_drops, nikkei_pct, fetched_at, False

    if cached_fetched_at is not None:
        return cached_drops, cached_nikkei, cached_fetched_at, True
    return [], None, None, False


def format_drop_list_text(
    drops: list[dict],
    nikkei_pct: float | None = None,
    fetched_at: str | None = None,
    stale_fallback: bool = False,
) -> str:
    """急落一覧のLINE表示テキストを生成"""
    nikkei_line = f"日経平均 {_fp(nikkei_pct)}\n" if nikkei_pct is not None else ""
    fetched_at = fetched_at or (drops[0].get("fetched_at") if drops else None)
    fetched_line = f"取得: {fetched_at}\n\n" if fetched_at else "\n"
    if not drops:
        note = "\n※ 最新取得に失敗したため、直近キャッシュを表示" if stale_fallback else ""
        return f"日経225 急落銘柄\n{nikkei_line}{fetched_line}本日の急落銘柄はなし\n（基準: -2%以上の下落）{note}"
    lines = [f"日経225 急落銘柄\n{nikkei_line}{fetched_line}"]
    for i, s in enumerate(drops[:10]):
        num = CIRCLE_NUMS[i] if i < len(CIRCLE_NUMS) else f"{i+1}."
        company_profile = format_company_profile_text(s["code"])
        company_block = f"   {company_profile.replace(chr(10), chr(10) + '   ')}\n" if company_profile else ""
        valuation = s.get("valuation") or {}
        valuation_score = _valuation_score(
            valuation.get("per"),
            valuation.get("pbr"),
            valuation.get("dividend_yield_pct"),
            s.get("from_high_pct"),
        )
        valuation_line = (
            f"   PER {_fmt_ratio(valuation.get('per'), digits=1, suffix='倍')} / "
            f"PBR {_fmt_ratio(valuation.get('pbr'), digits=2, suffix='倍')} / "
            f"配当 {_fmt_dividend_yield(valuation.get('dividend_yield_pct'), valuation.get('dividend_yield_status'))}\n"
            f"   指標割安度 {valuation_score}/100\n"
        )
        lines.append(
            f"{num} {s['code']} {s['name']} / {s['price']:,.0f}円\n"
            f"{company_block}"
            f"   前日比 {_format_day_change_text(s.get('price'), s.get('day_pct'))}\n"
            f"   高値差 {_fp(s.get('from_high_pct'))}"
            f"\n{valuation_line}"
        )
    if stale_fallback:
        lines.append("※ 最新取得に失敗したため、直近キャッシュを表示")
    lines.append("銘柄コードか銘柄名で詳しく見れる\n例: 7731 / ニコン / リソル")
    return "\n\n".join(lines)


# ─── J-Quants 財務キャッシュ ───

def fetch_and_cache_financials() -> None:
    """J-Quantsから財務データを取得してSupabaseにキャッシュ（8:00 cron）"""
    logger.info("=== 財務データ取得開始 ===")
    now_jst = datetime.now(JST)
    if now_jst.weekday() >= 5:
        logger.info("土日のためスキップ")
        return

    if not JQUANTS_API_KEY:
        logger.error("JQUANTS_API_KEY未設定。中断")
        return

    headers = {"x-api-key": JQUANTS_API_KEY}
    today = now_jst.date().isoformat()

    # 銘柄ごとに最新財務サマリーを取得（赤字判定に使用）
    import time as _time
    fin_by_code: dict[str, bool] = {}
    for code in NIKKEI225:
        try:
            r = _requests.get(
                f"{JQUANTS_API_BASE}/fins/summary",
                params={"code": code + "0"},
                headers=headers,
                timeout=10,
            )
            if r.status_code == 200:
                entries = r.json().get("data", [])
                if entries:
                    latest = entries[-1]
                    profit = latest.get("NetIncome") or latest.get("Profit") or latest.get("OrdinaryProfit")
                    if profit is not None:
                        try:
                            fin_by_code[code] = float(profit) < 0
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.warning("財務取得エラー code=%s: %s", code, e)
        _time.sleep(0.1)

    logger.info("財務データ取得: %d銘柄", len(fin_by_code))

    # 配当はFreeプランで取得不可のため静的リストを使用
    records = [
        {
            "code": code,
            "is_deficit": fin_by_code.get(code, False),
            "dividend_per_share": None,
            "updated_at": today,
        }
        for code in NIKKEI225
    ]

    try:
        supabase.table("nikkei_financials").upsert(records).execute()
        logger.info("財務キャッシュ保存完了: %d銘柄", len(records))
    except Exception as e:
        logger.error("財務キャッシュ保存エラー: %s", e)

    logger.info("=== 財務データ取得完了 ===")


def _load_financials_cache() -> dict[str, dict]:
    """{code: {is_deficit, dividend_per_share}} をSupabaseから取得"""
    try:
        res = supabase.table("nikkei_financials").select("code, is_deficit, dividend_per_share").execute()
        return {r["code"]: r for r in (res.data or [])}
    except Exception as e:
        logger.warning("財務キャッシュ読み込みエラー（スキップ）: %s", e)
        return {}


# ─── 買いシグナル判定 ───

def is_buy_signal(stock: dict, nikkei_pct: float | None, financials: dict | None = None) -> bool:
    """買いシグナル条件を全て満たすか判定"""
    if stock["change_pct"] > ALERT_THRESHOLD:
        return False
    if nikkei_pct is not None:
        if stock["change_pct"] - nikkei_pct > NIKKEI_GAP_THRESHOLD:
            return False
    # 財務キャッシュがあれば赤字銘柄を除外（J-Quants有料プランで精度向上予定）
    if financials:
        fin = financials.get(stock["code"])
        if fin and fin.get("is_deficit") is True:
            return False
    return True


def _build_signal_reason(stock: dict, nikkei_pct: float | None, financials: dict | None = None) -> str:
    reasons = ["指数より弱い下げ"]
    if financials:
        fin = financials.get(stock["code"])
        dpf = fin.get("dividend_per_share") if fin else None
        if dpf and dpf > 0:
            reasons.append("配当あり")
    elif stock["code"] in _HIGH_DIVIDEND_CODES:
        reasons.append("高配当")
    reasons.append("大型株")
    return " / ".join(reasons)


def get_company_profile(code: str) -> dict | None:
    if code in _company_profile_cache:
        return _company_profile_cache[code]

    try:
        res = (
            supabase.table("nikkei_company_profiles")
            .select("code,name,sector,business_summary")
            .eq("code", code)
            .limit(1)
            .execute()
        )
        profile = res.data[0] if res.data else None
        _company_profile_cache[code] = profile
        return profile
    except Exception as e:
        logger.warning("company profile load error code=%s: %s", code, e)
        _company_profile_cache[code] = None
        return None


def format_company_profile_text(code: str) -> str:
    profile = get_company_profile(code)
    if not profile:
        return ""

    lines = []
    sector = str(profile.get("sector") or "").strip()
    summary = str(profile.get("business_summary") or "").strip()
    if sector:
        lines.append(sector)
    if summary:
        lines.append(summary)
    return "\n".join(lines).strip()


# ─── 通知履歴 ───

def has_notified_recently(code: str) -> bool:
    """同じ銘柄を同日に通知済みか確認"""
    today = datetime.now(JST).date().isoformat()
    try:
        res = (
            supabase.table("nikkei_alert_log")
            .select("id")
            .eq("code", code)
            .eq("alerted_at", today)
            .execute()
        )
        return len(res.data) > 0
    except Exception as e:
        logger.error("通知履歴確認エラー: %s", e)
        return False


def get_notified_codes_today() -> set[str]:
    """本日通知済みの銘柄コードを一括取得"""
    today = datetime.now(JST).date().isoformat()
    try:
        res = (
            supabase.table("nikkei_alert_log")
            .select("code")
            .eq("alerted_at", today)
            .execute()
        )
        return {str(r.get("code")) for r in (res.data or []) if r.get("code")}
    except Exception as e:
        logger.error("通知履歴一括取得エラー: %s", e)
        return set()


def log_alert(code: str, change_pct: float) -> None:
    """通知ログをDBに保存"""
    today = datetime.now(JST).date().isoformat()
    try:
        supabase.table("nikkei_alert_log").insert({
            "code": code,
            "alerted_at": today,
            "change_pct": change_pct,
        }).execute()
    except Exception as e:
        logger.error("通知ログ保存エラー: %s", e)


# ─── AI解説 ───

def _comment_cache_bucket(value: float | None) -> str:
    if value is None:
        return "none"
    return f"{value:.1f}"


def _get_cached_ai_comment(code: str, change_pct: float | None, nikkei_pct: float | None) -> str | None:
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase.table("nikkei_ai_comment_cache")
            .select("comment")
            .eq("code", code)
            .eq("change_bucket", _comment_cache_bucket(change_pct))
            .eq("nikkei_bucket", _comment_cache_bucket(nikkei_pct))
            .gte("expires_at", now_iso)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0].get("comment")
    except Exception as e:
        logger.warning("AI comment cache load error: %s", e)
    return None


def _save_ai_comment_cache(
    code: str,
    name: str,
    change_pct: float | None,
    nikkei_pct: float | None,
    comment: str,
) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "code": code,
        "name": name,
        "change_bucket": _comment_cache_bucket(change_pct),
        "nikkei_bucket": _comment_cache_bucket(nikkei_pct),
        "comment": comment,
        "updated_at": now.isoformat(),
        "expires_at": (now + timedelta(days=AI_COMMENT_CACHE_TTL_DAYS)).isoformat(),
    }
    try:
        supabase.table("nikkei_ai_comment_cache").upsert(
            row, on_conflict="code,change_bucket,nikkei_bucket"
        ).execute()
    except Exception as e:
        logger.warning("AI comment cache save error: %s", e)


def get_ai_comment(
    code: str,
    name: str,
    change_pct: float | None,
    nikkei_pct: float | None = None,
) -> str:
    """急落理由と見通しをGPTで生成"""
    if not openai_client:
        return "解説取得できなかった（OpenAI未設定）"
    cached = _get_cached_ai_comment(code, change_pct, nikkei_pct)
    if cached:
        return cached
    try:
        pct_str = f"{change_pct:+.1f}%" if change_pct is not None else "急落"
        nikkei_str = f"日経平均は{nikkei_pct:+.1f}%。" if nikkei_pct is not None else ""
        prompt = (
            f"日経225銘柄の{name}（証券コード{code}）が本日{pct_str}の急落。"
            f"{nikkei_str}\n\n"
            "以下を150字以内で答えてください。\n"
            "1. 急落の主な理由（推定でも可）\n"
            "2. 短期的な見通しコメント\n\n"
            "投資助言は禁止。「注目候補」「監視候補」「シグナル検知」などの表現を使うこと。"
        )
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
        )
        comment = resp.choices[0].message.content.strip()
        if comment:
            _save_ai_comment_cache(code, name, change_pct, nikkei_pct, comment)
        return comment
    except Exception as e:
        logger.error("AI解説エラー: %s", e)
        return "解説の取得に失敗した"


# ─── プッシュ通知 ───

def _push_text(user_id: str, text: str) -> None:
    try:
        _requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("push失敗: user=%s %s", user_id, e)


def _build_alert_digest(signals: list[dict], nikkei_pct: float | None, financials: dict) -> str:
    nikkei_line = f"日経平均 {_fp(nikkei_pct)}\n" if nikkei_pct is not None else ""
    lines = [f"急落株速報\n{nikkei_line}"]
    for i, stock in enumerate(signals[:MAX_ALERT_SIGNALS]):
        num = CIRCLE_NUMS[i] if i < len(CIRCLE_NUMS) else f"{i+1}."
        valuation = stock.get("valuation") or {}
        lines.append(
            f"{num} {stock['code']} {stock['name']}\n"
            f"前日比 {_format_day_change_text(stock.get('price'), stock.get('change_pct'))}\n"
            f"PER {_fmt_ratio(valuation.get('per'), digits=1, suffix='倍')} / "
            f"PBR {_fmt_ratio(valuation.get('pbr'), digits=2, suffix='倍')} / "
            f"配当 {_fmt_dividend_yield(valuation.get('dividend_yield_pct'), valuation.get('dividend_yield_status'))}"
        )
    lines.append("詳しく見るなら「急落株」")
    return "\n\n".join(lines)


def _resolve_plan(user: dict, now_dt: datetime) -> str:
    """app.py の resolve_effective_plan と同等ロジック"""
    if user.get("membership_status") == "active":
        return "paid"
    trial_started_at = user.get("trial_started_at")
    if trial_started_at:
        try:
            trial_dt = datetime.fromisoformat(str(trial_started_at).replace("Z", "+00:00"))
            if now_dt <= trial_dt + timedelta(days=7):
                return "paid"
        except Exception:
            pass
    extended_until = user.get("trial_extended_until")
    if extended_until:
        try:
            ext_dt = datetime.fromisoformat(str(extended_until).replace("Z", "+00:00"))
            if now_dt <= ext_dt:
                return "paid"
        except Exception:
            pass
    return "free"


# ─── cron エントリポイント ───

def run_alert() -> None:
    logger.info("=== 日経225急落チェック開始 ===")
    now_jst = datetime.now(JST)

    if now_jst.weekday() >= 5:
        logger.info("土日のためスキップ")
        return

    if not HAS_YFINANCE:
        logger.error("yfinanceが未インストール。pip install yfinance")
        return

    nikkei_pct = get_nikkei_change_pct()
    logger.info("日経平均: %s%%", nikkei_pct)

    stocks = get_stock_changes()
    if not stocks:
        logger.warning("株価データ取得失敗")
        return

    drops = sorted(
        [s for s in stocks.values() if s["change_pct"] <= DROP_LIST_THRESHOLD],
        key=lambda x: x["change_pct"],
    )
    drops = _enrich_drop_valuations(drops)
    save_drop_cache(drops, nikkei_pct)

    financials = _load_financials_cache()
    logger.info("財務キャッシュ: %d銘柄", len(financials))

    notified_today = get_notified_codes_today()
    signals = [
        s for s in stocks.values()
        if is_buy_signal(s, nikkei_pct, financials) and s["code"] not in notified_today
    ]
    signals = sorted(signals, key=lambda x: x["change_pct"])[:MAX_ALERT_SIGNALS]
    signals = _enrich_drop_valuations(signals, limit=MAX_ALERT_SIGNALS)

    logger.info("買いシグナル候補: %d銘柄", len(signals))
    if not signals:
        logger.info("通知対象なし。終了")
        return

    try:
        res = supabase.table("users").select(
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active, drop_alert_enabled"
        ).execute()
        users = res.data or []
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return

    now_utc = datetime.now(timezone.utc)
    msg = _build_alert_digest(signals, nikkei_pct, financials)
    sent_count = 0
    for u in users:
        if not u.get("active", True):
            continue
        if not u.get("drop_alert_enabled", False):
            continue
        if _resolve_plan(u, now_utc) != "paid":
            continue
        _push_text(u["user_id"], msg)
        sent_count += 1

    notified_codes: set[str] = set()
    if sent_count > 0:
        for stock in signals:
            log_alert(stock["code"], stock["change_pct"])
            notified_codes.add(stock["code"])
        logger.info("まとめ通知: %d銘柄 → %d人", len(signals), sent_count)

    logger.info("=== 完了: %d銘柄通知 ===", len(notified_codes))

    # 市場要約キャッシュも更新
    try:
        from market_summary import run_market_update
        run_market_update()
    except Exception as e:
        logger.error("市場要約更新エラー: %s", e)


if __name__ == "__main__":
    if "--fetch-financials" in sys.argv:
        fetch_and_cache_financials()
    else:
        run_alert()
