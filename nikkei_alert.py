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
DROP_LIST_THRESHOLD = -2.0     # 急落一覧閾値（%）
ALERT_THRESHOLD = -2.5          # 買いシグナル閾値（%）
NIKKEI_GAP_THRESHOLD = -1.5     # 指数乖離閾値（pt）
RESEND_COOLDOWN_DAYS = 3        # 再通知抑制日数

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
    """日経225全銘柄の騰落率を一括取得。{code: {code, name, change_pct, price}}"""
    if not HAS_YFINANCE:
        return {}
    tickers = [f"{c}.T" for c in NIKKEI225]
    try:
        df = yf.download(
            tickers,
            period="2d",
            interval="1d",
            progress=False,
            auto_adjust=True,
            threads=True,
        )
        if df.empty:
            return {}
        close = df["Close"] if isinstance(df.columns, pd.MultiIndex) else df[["Close"]]
        result: dict[str, dict] = {}
        for code, name in NIKKEI225.items():
            ticker = f"{code}.T"
            if ticker not in close.columns:
                continue
            prices = close[ticker].dropna()
            if len(prices) < 2:
                continue
            prev = float(prices.iloc[-2])
            today = float(prices.iloc[-1])
            if prev == 0:
                continue
            result[code] = {
                "code": code,
                "name": name,
                "change_pct": round((today - prev) / prev * 100, 2),
                "price": round(today, 1),
            }
        return result
    except Exception as e:
        logger.error("株価一括取得エラー: %s", e)
        return {}


def get_single_stock_change(code: str) -> dict | None:
    """単一銘柄の当日騰落率を取得"""
    if not HAS_YFINANCE or code not in NIKKEI225:
        return None
    try:
        hist = yf.Ticker(f"{code}.T").history(period="2d")
        if len(hist) < 2:
            return None
        prev = float(hist["Close"].iloc[-2])
        today = float(hist["Close"].iloc[-1])
        if prev == 0:
            return None
        return {
            "code": code,
            "name": NIKKEI225[code],
            "change_pct": round((today - prev) / prev * 100, 2),
            "price": round(today, 1),
        }
    except Exception as e:
        logger.error("単一株取得エラー: code=%s %s", code, e)
        return None


# ─── 急落一覧 ───

def get_drop_list(threshold: float = DROP_LIST_THRESHOLD) -> list[dict]:
    """閾値以上に下落した銘柄を騰落率の悪い順で返す"""
    stocks = get_stock_changes()
    drops = [s for s in stocks.values() if s["change_pct"] <= threshold]
    return sorted(drops, key=lambda x: x["change_pct"])


def format_drop_list_text(drops: list[dict], nikkei_pct: float | None = None) -> str:
    """急落一覧のLINE表示テキストを生成"""
    nikkei_line = f"日経平均 {nikkei_pct:+.1f}%\n\n" if nikkei_pct is not None else ""
    if not drops:
        return f"{nikkei_line}本日の急落銘柄はなし\n（基準: -2%以上の下落）"
    lines = [f"{nikkei_line}日経225 急落銘柄\n"]
    for i, s in enumerate(drops[:10]):
        num = CIRCLE_NUMS[i] if i < len(CIRCLE_NUMS) else f"{i+1}."
        lines.append(f"{num} {s['name']} {s['change_pct']:+.1f}%")
    lines.append("\n番号で理由を見れる\n例: 1")
    return "\n".join(lines)


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


# ─── 通知履歴 ───

def has_notified_recently(code: str) -> bool:
    """RESEND_COOLDOWN_DAYS以内に通知済みか確認"""
    cutoff = (datetime.now(JST).date() - timedelta(days=RESEND_COOLDOWN_DAYS)).isoformat()
    today = datetime.now(JST).date().isoformat()
    try:
        res = (
            supabase.table("nikkei_alert_log")
            .select("id")
            .eq("code", code)
            .gte("alerted_at", cutoff)
            .lte("alerted_at", today)
            .execute()
        )
        return len(res.data) > 0
    except Exception as e:
        logger.error("通知履歴確認エラー: %s", e)
        return False


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

def get_ai_comment(
    code: str,
    name: str,
    change_pct: float | None,
    nikkei_pct: float | None = None,
) -> str:
    """急落理由と見通しをGPTで生成"""
    if not openai_client:
        return "解説取得できなかった（OpenAI未設定）"
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
        return resp.choices[0].message.content.strip()
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

    financials = _load_financials_cache()
    logger.info("財務キャッシュ: %d銘柄", len(financials))

    signals = [
        s for s in stocks.values()
        if is_buy_signal(s, nikkei_pct, financials) and not has_notified_recently(s["code"])
    ]

    logger.info("買いシグナル候補: %d銘柄", len(signals))
    if not signals:
        logger.info("通知対象なし。終了")
        return

    try:
        res = supabase.table("users").select(
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active"
        ).execute()
        users = res.data or []
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return

    now_utc = datetime.now(timezone.utc)
    notified_codes: set[str] = set()

    for stock in signals:
        reason = _build_signal_reason(stock, nikkei_pct, financials)
        msg = (
            f"買いシグナル速報\n\n"
            f"{stock['code']} {stock['name']} {stock['change_pct']:+.1f}%\n\n"
            f"理由:\n{reason}\n\n"
            f"詳しく見るなら\n{stock['code']} で返信"
        )

        sent_count = 0
        for u in users:
            if not u.get("active", True):
                continue
            if _resolve_plan(u, now_utc) != "paid":
                continue
            _push_text(u["user_id"], msg)
            sent_count += 1

        if sent_count > 0:
            log_alert(stock["code"], stock["change_pct"])
            notified_codes.add(stock["code"])
            logger.info("通知: %s %s → %d人", stock["code"], stock["name"], sent_count)

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
