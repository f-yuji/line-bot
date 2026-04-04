import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from urllib.parse import urlparse

import feedparser
import httpx
import requests
from dotenv import load_dotenv
from openai import OpenAI
from supabase import create_client

# 環境変数読み込み
load_dotenv()

# ─── ログ設定 ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── 環境変数 ───
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OWNER_LINE_USER_ID = os.getenv("OWNER_LINE_USER_ID")
ENV = os.getenv("ENV", "prod")

# ─── クライアント ───
client = OpenAI(api_key=OPENAI_API_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── 定数 ───
LINE_URL = "https://api.line.me/v2/bot/message/push"

# Google Newsがブロックされた場合のフォールバック付きRSSソース
RSS_SOURCES = [
    "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja",
    "https://news.yahoo.co.jp/rss/topics/top-picks.xml",
    "https://assets.wor.jp/rss/rdf/nikkei/news.rdf",
]

MAX_FETCH_ITEMS = 40
DEFAULT_MAX_ITEMS = 5
LINE_MAX_MESSAGE_OBJECTS = 5
LINE_TEXT_SAFE_LIMIT = 4500
LINE_RETRY_MAX = 3

_NEWS_QUICK_REPLY = {
    "items": [
        {"type": "action", "action": {"type": "message", "label": "ニュース",  "text": "ニュース"}},
        {"type": "action", "action": {"type": "message", "label": "会話ネタ",  "text": "会話ネタ"}},
        {"type": "action", "action": {"type": "message", "label": "リンク",    "text": "リンク"}},
        {"type": "action", "action": {"type": "message", "label": "使い方",    "text": "使い方"}},
    ]
}

EXCLUDE_KEYWORDS = [
    "占い", "グラビア", "プレゼント", "キャンペーン",
]

SCORE_THRESHOLD = 3

# 強キーワード: +3
STRONG_KEYWORDS: Dict[str, List[str]] = {
    "real_estate": [
        "住宅ローン", "固定金利", "変動金利", "マンション価格", "中古マンション",
        "新築マンション", "地価上昇", "地価下落", "公示地価", "路線価",
        "家賃上昇", "空室率", "不動産投資", "REIT", "J-REIT",
        "住宅市場", "不動産市況", "再開発計画", "都市再開発", "不動産価格",
        "住宅販売", "分譲マンション", "賃貸市況", "住宅着工", "不動産融資",
        "住宅取得", "住宅需要", "投資用不動産", "収益物件", "商業地価格"
    ],

    "construction": [
        "建設受注", "公共工事", "工事費上昇", "資材不足", "職人不足",
        "人手不足", "インフラ整備", "再開発工事", "施工不良", "建設業界",
        "建築基準法", "建設コスト", "工期遅延", "ゼネコン", "建設会社",
        "設備工事", "改修工事", "修繕工事", "耐震工事", "建設需要",
        "入札不調", "建築確認", "現場事故", "外壁改修", "大規模修繕",
        "道路工事", "橋梁工事", "建築着工", "設備更新", "施工管理",
        "資材高騰", "建設資材", "人手不足倒産", "現場監督", "職人確保",
        "建築単価", "労務費", "資材価格", "大規模改修"
    ],

    "interest_rates": [
        "政策金利", "日銀会合", "金融政策決定会合", "利上げ", "利下げ",
        "長期金利", "短期金利", "国債利回り", "10年国債", "YCC",
        "イールドカーブ", "金融引き締め", "金融緩和", "植田総裁", "FRB利下げ",
        "FF金利", "住宅ローン金利", "追加利上げ", "金利上昇", "金利低下",
        "実質金利", "名目金利", "マイナス金利", "ゼロ金利", "利回り上昇",
        "債券利回り", "金利据え置き", "金融正常化", "利上げ観測", "利下げ観測"
    ],

    "energy": [
        "原油急騰", "原油安", "WTI", "ブレント原油", "ガソリン価格",
        "電気料金", "ガス料金", "燃料費調整", "LNG価格", "電力需給",
        "再エネ賦課金", "中東情勢", "ホルムズ海峡", "OPEC", "OPECプラス",
        "産油国", "燃料費高騰", "電気代値上げ", "ガス代値上げ", "エネルギー価格",
        "原油価格", "天然ガス価格", "火力発電", "再生可能エネルギー", "太陽光発電",
        "原発再稼働", "送電網", "停電リスク", "電力逼迫", "石炭価格"
    ],

    "ai": [
        "生成AI", "ChatGPT", "OpenAI", "Gemini", "Claude",
        "Copilot", "Perplexity", "LLM", "大規模言語モデル", "マルチモーダル",
        "AI半導体", "GPU需要", "NVIDIA", "エヌビディア", "半導体需要",
        "推論AI", "AIモデル", "AI規制", "AI新機能", "生成AI市場",
        "Anthropic", "Grok", "DeepSeek", "Mistral", "Cursor",
        "RAG", "エージェントAI", "音声AI", "画像生成AI", "推論モデル",
        "学習データ", "ファインチューニング", "AI導入", "生成AI活用", "推論コスト"
    ],

    "sports": [
        "大谷翔平", "ドジャース", "MLB", "メジャーリーグ", "W杯",
        "ワールドカップ", "五輪", "オリンピック", "サッカー日本代表", "侍ジャパン",
        "Jリーグ", "プロ野球", "阪神タイガース", "読売ジャイアンツ", "巨人",
        "浦和レッズ", "ヴィッセル神戸", "箱根駅伝", "ラグビー日本代表", "NBA",
        "久保建英", "三笘薫", "井上尚弥", "大坂なおみ", "F1",
        "佐々木朗希", "山本由伸", "ダルビッシュ", "WBC", "チャンピオンズリーグ",
        "プレミアリーグ", "日本シリーズ", "センバツ", "甲子園", "全英オープン",
        "松山英樹", "Bリーグ", "駅伝", "世界陸上", "格闘技イベント"
    ],

    "economy": [
        "物価上昇", "物価高", "インフレ", "デフレ", "円安進行",
        "円高進行", "GDP", "実質賃金", "景気後退", "景気回復",
        "消費者物価", "CPI", "PPI", "スタグフレーション", "景気判断",
        "個人消費", "設備投資", "輸出減速", "輸入増", "経済成長率",
        "賃上げ", "実質成長率", "名目成長率", "景気減速", "景況感",
        "景気指数", "内需", "外需", "家計消費", "雇用統計"
    ],

    "business": [
        "決算発表", "業績予想", "業績下方修正", "業績上方修正", "通期見通し",
        "値上げ発表", "M&A", "買収提案", "上場廃止", "新規上場",
        "IPO", "倒産", "希望退職", "リストラ", "事業再編",
        "工場新設", "設備投資計画", "販売不振", "増収増益", "減収減益",
        "四半期決算", "営業利益", "最終利益", "売上高", "収益改善",
        "不採算事業", "子会社売却", "自社株買い", "増配", "減配",
        "社長交代", "経営統合", "提携解消", "サプライチェーン", "価格改定",
        "下方修正", "上方修正", "連結決算", "営業赤字", "最終赤字",
        "増収", "減収", "増益", "減益"
    ],

    "tech": [
        "新機能", "OS更新", "クラウド障害", "システム障害", "情報漏えい",
        "不正アクセス", "サイバー攻撃", "ランサムウェア", "個人情報流出", "アプリ障害",
        "サービス障害", "通信障害", "データセンター", "セキュリティ更新", "ブラウザ更新",
        "スマホ新機能", "ソフトウェア更新", "クラウドサービス", "障害復旧", "脆弱性",
        "iOS更新", "Android更新", "Windows更新", "Macアップデート", "通信キャリア障害",
        "ネット障害", "SNS障害", "API障害", "認証障害", "ゼロデイ脆弱性",
        "サーバーダウン", "セキュリティ事故", "不具合修正", "機能追加", "バグ修正"
    ],

    "international": [
        "停戦協議", "停戦交渉", "首脳会談", "関税措置", "制裁強化",
        "報復措置", "外交交渉", "軍事支援", "ミサイル攻撃", "停戦案",
        "米中対立", "中東情勢", "ロシア制裁", "ウクライナ侵攻", "台湾海峡",
        "南シナ海", "NATO", "G7首脳会議", "国連安保理", "核開発",
        "米大統領選", "中国景気", "欧州委員会", "イスラエル", "イラン",
        "ガザ地区", "トランプ政権", "バイデン政権", "米中会談", "対中制裁",
        "対ロ制裁", "防衛支援", "国境紛争", "難民問題", "地政学リスク",
        "停戦", "空爆", "攻撃", "報復", "軍", "米軍",
        "国防", "安保", "首脳声明", "外交筋", "EU", "欧州連合",
        "ホワイトハウス", "国務長官"
    ],

    "materials": [
        "コメ価格", "米価高騰", "食品価格", "日用品値上げ", "洗剤値上げ",
        "シャンプー値上げ", "ティッシュ値上げ", "紙製品値上げ", "物流コスト", "輸送コスト",
        "生活コスト", "食費上昇", "日用品高騰", "小売価格", "値上げラッシュ",
        "スーパー価格", "消費者負担", "家計負担", "節約需要", "生活必需品",
        "食料品値上げ", "調味料値上げ", "乳製品値上げ", "飲料値上げ", "菓子値上げ",
        "日用品価格", "家計圧迫", "仕入れ価格上昇", "店頭価格", "物価負担"
    ],

    "scandal": [
        "辞任表明", "辞職勧告", "不祥事発覚", "不適切発言", "炎上騒動",
        "逮捕容疑", "書類送検", "不起訴処分", "疑惑浮上", "調査報告書",
        "隠蔽疑惑", "内部告発", "コンプラ違反", "ハラスメント疑惑", "謝罪会見",
        "処分発表", "説明責任", "告発文書", "不正受給", "不正会計",
        "裏金問題", "贈収賄疑惑", "パワハラ疑惑", "セクハラ疑惑", "情報隠蔽",
        "第三者委員会", "処分検討", "会見拒否", "不適切投稿", "発言撤回",
        "殺人", "遺体", "送検", "起訴", "立てこもり",
        "強盗", "傷害", "暴行", "詐欺", "横領", "汚職"
    ],

    "entertainment": [
        "映画公開", "ドラマ放送", "俳優出演", "女優出演", "アーティスト",
        "新曲発表", "ライブ開催", "興行収入", "配信開始", "主演決定",
        "訃報", "受賞", "舞台挨拶", "テレビ出演", "Netflix",
        "アニメ化", "続編決定", "主題歌", "音楽番組", "芸能ニュース",
        "映画ランキング", "ドラマ最終回", "声優出演", "番組終了", "番組改編",
        "舞台公演", "アルバム発売", "MV公開", "フェス開催", "芸能事務所"
    ],
}

CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "real_estate": [
        "不動産", "住宅", "マンション", "戸建て", "土地",
        "賃貸", "家賃", "再開発", "REIT", "物件",
        "分譲", "地価", "住宅市場", "住宅販売", "収益物件"
    ],

    "construction": [
        "建設", "建築", "工事", "施工", "インフラ",
        "再開発", "ゼネコン", "改修", "設備", "職人",
        "修繕", "現場", "公共工事", "建築基準法", "施工管理"
    ],

    "interest_rates": [
        "金利", "利回り", "日銀", "中央銀行", "利上げ",
        "利下げ", "金融政策", "国債", "住宅ローン", "政策金利",
        "長期金利", "短期金利", "債券", "YCC", "FRB"
    ],

    "energy": [
        "原油", "石油", "ガス", "電気", "燃料",
        "LNG", "電力", "エネルギー", "ガソリン", "燃料費",
        "原発", "再エネ", "発電", "電気代", "ガス代"
    ],

    "ai": [
        "AI", "人工知能", "生成AI", "ChatGPT", "Gemini",
        "OpenAI", "Claude", "Copilot", "半導体", "GPU",
        "LLM", "NVIDIA", "推論AI", "AIモデル", "生成モデル"
    ],

    "sports": [
        "野球", "サッカー", "バスケ", "テニス", "ラグビー",
        "格闘技", "MLB", "Jリーグ", "五輪", "日本代表",
        "W杯", "NBA", "大谷", "甲子園", "駅伝"
    ],

    "economy": [
        "経済", "景気", "物価", "インフレ", "デフレ",
        "円安", "円高", "GDP", "賃金", "消費",
        "雇用", "景況感", "成長率", "家計", "物価高"
    ],

    "business": [
        "企業", "決算", "業績", "値上げ", "倒産",
        "M&A", "買収", "上場", "事業", "販売",
        "利益", "売上", "工場", "価格改定", "経営"
    ],

    "tech": [
        "テック", "IT", "アプリ", "ソフトウェア", "クラウド",
        "障害", "情報漏えい", "サイバー攻撃", "OS", "サービス",
        "脆弱性", "不正アクセス", "更新", "通信障害", "システム"
    ],

    "international": [
        "米国", "中国", "ロシア", "欧州", "中東",
        "台湾", "外交", "関税", "戦争", "停戦",
        "制裁", "首脳会談", "軍事", "イラン", "イスラエル",
        "G7", "G20", "外相", "首脳", "大統領", "会談",
        "攻撃", "報復", "空爆"
    ],

    "materials": [
        "食品", "コメ", "日用品", "洗剤", "シャンプー",
        "物流", "生活コスト", "食費", "家計", "値上げ",
        "スーパー", "小売価格", "生活必需品", "消費者負担", "日用品価格"
    ],

    "scandal": [
        "不祥事", "炎上", "辞任", "逮捕", "疑惑",
        "不起訴", "謝罪", "告発", "不正", "処分",
        "ハラスメント", "隠蔽", "会見", "裏金", "コンプラ",
        "刺殺", "殺害", "死亡", "事件", "容疑者", "被害者", "通り魔", "発生",
        "殺人"
    ],

    "entertainment": [
        "芸能", "映画", "ドラマ", "俳優", "女優",
        "音楽", "ライブ", "アニメ", "配信", "訃報",
        "番組", "主題歌", "アーティスト", "舞台", "受賞"
    ],
}

CATEGORY_LABELS: Dict[str, str] = {
    "real_estate":    "不動産",
    "construction":   "建設",
    "interest_rates": "金利",
    "energy":         "エネルギー",
    "ai":             "AI",
    "sports":         "スポーツ",
    "economy":        "経済",
    "business":       "企業",
    "tech":           "テック",
    "international":  "国際",
    "materials":      "生活",
    "scandal":        "話題",
    "entertainment":  "芸能",
    "other":          "その他",
}

CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"


# =========================
# ユーティリティ
# =========================

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", str(text or "")).strip()


def extract_source_name(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if "nikkei" in host:
            return "日経"
        if "nhk" in host:
            return "NHK"
        if "itmedia" in host:
            return "ITmedia"
        if "reuters" in host:
            return "Reuters"
        if "yahoo" in host:
            return "Yahoo"
        return host
    except Exception:
        return "不明"


# URL短縮機能（未使用のため削除）
# 必要になったら is.gd 等で再実装する


def normalize_plan(plan: str) -> str:
    if plan in ["light", "premium", "paid"]:
        return "paid"
    return "free"


def resolve_effective_plan(user: dict, now_dt) -> str:
    """membership_status == active なら即 paid。それ以外はトライアル判定へ。"""
    if user.get("membership_status", "none") == "active":
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


def plan_max_items(plan: str) -> int:
    plan = normalize_plan(plan)
    return {
        "free": 5,
        "paid": 5,
    }.get(plan, DEFAULT_MAX_ITEMS)


def filter_sent(news_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """送信済み記事を除外"""
    if not news_list:
        return news_list
    links = [n["link"] for n in news_list]
    try:
        twelve_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
        _CHUNK_SIZE = 30
        sent_links: set = set()
        for i in range(0, len(links), _CHUNK_SIZE):
            chunk = links[i:i + _CHUNK_SIZE]
            res = supabase.table("sent_articles").select("link").in_("link", chunk).gte("sent_at", twelve_hours_ago).execute()
            for row in res.data or []:
                sent_links.add(row["link"])
        filtered = [n for n in news_list if n["link"] not in sent_links]
        logger.info("送信済み除外: %d件 → %d件", len(news_list), len(filtered))
        return filtered
    except Exception as e:
        logger.error("sent_articles取得失敗（除外スキップ）: %s", e)
        return news_list


def record_sent(news_list: List[Dict[str, str]]) -> None:
    """送信済み記事を記録"""
    if not news_list:
        return
    rows = [{"link": n["link"]} for n in news_list]
    try:
        supabase.table("sent_articles").upsert(rows, on_conflict="link").execute()
        logger.info("送信済み記録: %d件", len(rows))
    except Exception as e:
        logger.error("sent_articles記録失敗: %s", e)


def save_news_context(
    user_id: str,
    news: List[Dict[str, str]],
    summaries: Dict[str, Dict[str, str]],
) -> None:
    """配信内容を履歴として保存（深掘り・Q&A用コンテキスト）"""
    def _build_item(i, n):
        link = n.get("link", "")
        s = summaries.get(link, {})
        return {
            "index":          i + 1,
            "category":       CATEGORY_LABELS.get(n.get("category", "other"), "その他"),
            "title":          n["title"],
            "link":           link,
            "reason":         s.get("fact", ""),
            "interpretation": s.get("chat", ""),
        }

    news_items = [_build_item(i, n) for i, n in enumerate(news)]
    payload = {"news_items": news_items}

    try:
        supabase.table("news_contexts").insert({
            "user_id": user_id,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }).execute()
        logger.info("ニュース保存成功 user=%s 件数=%d", user_id, len(news))
    except Exception as e:
        logger.error("ニュースコンテキスト保存失敗: %s", e)


def save_last_news_batch(user_id: str, news: List[Dict[str, str]]) -> None:
    """リンク専用バッチをlast_news_batchにupsert（news_contextsとは分離）"""
    items = [
        {"index": i + 1, "title": n["title"], "link": n.get("link", "")}
        for i, n in enumerate(news)
    ]
    try:
        supabase.table("last_news_batch").upsert({
            "user_id":  user_id,
            "items":    items,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="user_id").execute()
        logger.info("last_news_batch保存: user=%s %d件", user_id, len(items))
    except Exception as e:
        logger.error("last_news_batch保存失敗: user=%s %s", user_id, e)


def load_users() -> Dict[str, Any]:
    try:
        res = supabase.table("users").select("*").eq("active", True).execute()
        rows = res.data or []

        users: Dict[str, Any] = {}
        for row in rows:
            user_id = row["user_id"]
            plan = row.get("plan", "free")
            users[user_id] = {
                "user_id": user_id,
                "plan": normalize_plan(plan),
                "active": row.get("active", True),
                "genres": row.get("genres", []) or [],
                "max_items": plan_max_items(plan),
                "membership_status": row.get("membership_status", "none"),
                "membership_expires_at": row.get("membership_expires_at"),
                "trial_started_at": row.get("trial_started_at"),
                "trial_extended_until": row.get("trial_extended_until"),
            }

        logger.info("Supabaseユーザー読込: %d件", len(users))
        return users

    except Exception as e:
        logger.error("Supabase users 読み込み失敗: %s", e)
        return {}


# =========================
# カテゴリ判定・スコアリング
# =========================

_SCANDAL_PRIORITY    = {"刺殺", "殺害", "死亡", "事件", "容疑者", "被害者", "通り魔", "逮捕"}
_INTL_PRIORITY       = {"G7", "G20", "外相", "首脳", "大統領",  "首脳会談"}

# construction の誤爆を防ぐ語（含まれる場合は construction を除外）
_CONSTRUCTION_BLOCK  = {"外相", "首相", "大統領", "G7", "G20", "首脳", "会談", "外交",
                        "イラン", "イスラエル", "米国", "中国", "ロシア"}

# business はこのうち1語以上ないとスコア対象外
_BUSINESS_REQUIRED   = {"決算", "業績", "企業", "値上げ", "売上", "利益", "事業", "IPO", "上場", "買収"}

CATEGORY_WEIGHTS: Dict[str, int] = {
    "scandal":        3,
    "international":  3,
    "economy":        2,
    "interest_rates": 2,
    "energy":         2,
    "business":       2,
    "construction":   1,
    "real_estate":    2,
    "ai":             2,
    "tech":           2,
    "sports":         2,
    "materials":      2,
    "entertainment":  2,
    "other":          1,
}


def classify_category(article: Dict[str, str]) -> str:
    text = f"{article['title']} {article.get('summary', '')}"

    # 優先判定（1: scandal → 2: international → 3: スコアベース）
    if any(w in text for w in _SCANDAL_PRIORITY):
        return "scandal"
    if any(w in text for w in _INTL_PRIORITY):
        return "international"

    block_construction = any(w in text for w in _CONSTRUCTION_BLOCK)
    has_business_word  = any(w in text for w in _BUSINESS_REQUIRED)

    scores: Dict[str, int] = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if cat == "construction" and block_construction:
            continue
        if cat == "business" and not has_business_word:
            continue
        count = sum(1 for k in keywords if k in text)
        if count:
            scores[cat] = count * CATEGORY_WEIGHTS.get(cat, 1)
    if scores:
        return max(scores, key=scores.get)
    # fallback: スコアが付かなかった場合でも意味語で拾う
    _SCANDAL_FB = {"事件", "逮捕", "殺人", "刺殺", "容疑者", "被害者"}
    _INTL_FB    = {"米国", "中国", "ロシア", "イラン", "イスラエル", "G7", "外交"}
    if any(w in text for w in _SCANDAL_FB):
        return "scandal"
    if any(w in text for w in _INTL_FB):
        return "international"
    return "other"


def score_article(article: Dict[str, str], user_genres: List[str]) -> int:
    text = f"{article['title']} {article.get('summary', '')}"
    score = 0

    for word in EXCLUDE_KEYWORDS:
        if word in text:
            score -= 3

    block_construction = any(w in text for w in _CONSTRUCTION_BLOCK)
    has_business_word  = any(w in text for w in _BUSINESS_REQUIRED)

    # NOTE:
    # 同一キーワードが STRONG_KEYWORDS と CATEGORY_KEYWORDS の両方に含まれる場合、
    # 意図的にダブルカウントされる設計
    # 理由：重要キーワードの重みを強めるため

    # カテゴリごとにcap: 強キーワードmax+3、弱キーワードmax+3
    for cat in set(list(STRONG_KEYWORDS.keys()) + list(CATEGORY_KEYWORDS.keys())):
        if cat == "construction" and block_construction:
            continue
        if cat == "business" and not has_business_word:
            continue
        strong_hits = sum(1 for k in STRONG_KEYWORDS.get(cat, []) if k in text)
        weak_hits = sum(1 for k in CATEGORY_KEYWORDS.get(cat, []) if k in text)
        score += min(strong_hits * 3, 3)
        score += min(weak_hits, 3)

    if user_genres and article.get("category") in user_genres:
        score += 2

    return score


# =========================
# ニュース取得
# =========================

_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
    "Referer": "https://news.google.com/",
    "Cache-Control": "no-cache",
}


async def _fetch_rss_async(url: str, client: httpx.AsyncClient, max_retries: int = 2) -> feedparser.FeedParserDict:
    """単一RSSソースを非同期取得。リトライ付き"""
    for attempt in range(1, max_retries + 1):
        try:
            res = await client.get(url, timeout=20)

            logger.info(
                "RSS HTTP応答: url=%s status=%d size=%d",
                url, res.status_code, len(res.text),
            )

            if res.status_code == 403:
                logger.warning(
                    "RSS 403 Forbidden: %s | body=%s",
                    url, res.text[:200],
                )
                return feedparser.FeedParserDict(entries=[])

            res.raise_for_status()
            raw_xml = res.text

            # 不正な制御文字を除去
            raw_xml = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw_xml)

            feed = feedparser.parse(raw_xml)

            if feed.entries:
                if feed.bozo:
                    logger.info(
                        "RSS bozo検出だがエントリあり(%d件)、続行: %s",
                        len(feed.entries), url,
                    )
                logger.info("RSS取得成功: url=%s entries=%d", url, len(feed.entries))
                return feed

            logger.warning(
                "RSS 試行%d/%d エントリ0件: %s | body=%s",
                attempt, max_retries, url, res.text[:200],
            )

        except httpx.HTTPError as e:
            logger.warning(
                "RSS 試行%d/%d HTTPエラー: %s → %s",
                attempt, max_retries, url, e,
            )
        except Exception as e:
            logger.warning(
                "RSS 試行%d/%d 予期しないエラー: %s → %s",
                attempt, max_retries, url, e,
            )

        if attempt < max_retries:
            await asyncio.sleep(3 * attempt)

    logger.error("RSS全試行失敗: %s", url)
    return feedparser.FeedParserDict(entries=[])


async def _fetch_all_rss_async(urls: list) -> list:
    sem = asyncio.Semaphore(3)

    async def fetch_with_limit(url, c):
        async with sem:
            return await _fetch_rss_async(url, c)

    async with httpx.AsyncClient(headers=_RSS_HEADERS) as c:
        tasks = [fetch_with_limit(url, c) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)


def fetch_news() -> List[Dict[str, str]]:
    """複数RSSソースを並列取得してマージして返す"""
    all_entries: List[Any] = []

    results = asyncio.run(_fetch_all_rss_async(RSS_SOURCES))
    for i, result in enumerate(results):
        rss_url = RSS_SOURCES[i]
        if isinstance(result, Exception):
            logger.warning("RSSソース失敗: %s → %s", rss_url, result)
            continue
        if result.entries:
            logger.info("RSSソース成功: url=%s entries=%d", rss_url, len(result.entries))
            all_entries.extend(result.entries[:MAX_FETCH_ITEMS])
        else:
            logger.warning("RSSソース失敗: %s", rss_url)

    if not all_entries:
        logger.error("RSS取得失敗: 全ソースからエントリ0件")
        return []

    logger.info("マージ前記事数: %d件", len(all_entries))

    news = []
    seen_links: set = set()
    seen_titles: set = set()

    for entry in all_entries:
        title = clean_text(entry.get("title", ""))
        link = entry.get("link", "")
        summary = strip_html(entry.get("summary", entry.get("description", "")))

        if not title or not link:
            continue
        if link in seen_links or title in seen_titles:
            continue
        if any(word in title for word in EXCLUDE_KEYWORDS):
            continue

        seen_links.add(link)
        seen_titles.add(title)

        article = {
            "title": title,
            "link": link,
            "summary": summary,
            "source": extract_source_name(link),
        }
        article["category"] = classify_category(article)
        news.append(article)

    logger.info("重複除外後記事数: %d件", len(news))
    logger.info("ニュース取得: %d件", len(news))
    return news


# =========================
# フィルタ
# =========================

def filter_news(
    news_list: List[Dict[str, str]], user: Dict[str, Any]
) -> List[Dict[str, str]]:
    user_id = user.get("user_id", "?")
    genres = user.get("genres", []) or []
    max_items = user.get("max_items", DEFAULT_MAX_ITEMS)

    scored = []
    for n in news_list:
        s = score_article(n, genres)
        if s >= SCORE_THRESHOLD:
            scored.append((s, n))
    scored.sort(key=lambda x: x[0], reverse=True)

    logger.info(
        "スコアフィルタ: user=%s 全%d件→%d件(閾値%d)",
        user_id, len(news_list), len(scored), SCORE_THRESHOLD,
    )
    for s, n in scored:
        logger.info(
            "  [%s] score=%d %s",
            n.get("category", "other"), s, n["title"],
        )

    filtered = [n for _, n in scored]

    if genres:
        genre_matched = [n for n in filtered if n.get("category") in genres]
        if genre_matched:
            logger.info(
                "ジャンル絞り込み: user=%s genres=%s → %d件",
                user_id, genres, len(genre_matched),
            )
            filtered = genre_matched

    return filtered[:max_items] if filtered else news_list[:3]


# =========================
# AI要約
# =========================


_ARTICLE_SUMMARY_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "article_summaries",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "articles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "fact": {"type": "string"},
                            "chat": {"type": "string"},
                        },
                        "required": ["fact", "chat"],
                        "additionalProperties": False,
                    },
                },
                "topic": {"type": "string"},
            },
            "required": ["articles", "topic"],
            "additionalProperties": False,
        },
    },
}


def get_cached_summaries(links: List[str]) -> Dict[str, str]:
    """article_summariesから24時間以内のキャッシュを取得。{link: summary_short}を返す"""
    if not links:
        return {}
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    try:
        res = (
            supabase.table("article_summaries")
            .select("link,summary_short")
            .in_("link", links)
            .gte("updated_at", cutoff)
            .execute()
        )
        return {row["link"]: row["summary_short"] for row in (res.data or [])}
    except Exception as e:
        logger.error("article_summariesキャッシュ取得失敗: %s", e)
        return {}


def save_article_summaries(news_list: List[Dict[str, str]], new_summaries: Dict[str, str]) -> None:
    """新規要約をarticle_summariesにupsert"""
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for n in news_list:
        link = n.get("link", "")
        if link and link in new_summaries:
            rows.append({
                "link": link,
                "title": n.get("title", ""),
                "summary_short": new_summaries[link],
                "updated_at": now,
            })
    if not rows:
        return
    try:
        supabase.table("article_summaries").upsert(rows, on_conflict="link").execute()
        logger.info("article_summaries保存: %d件", len(rows))
    except Exception as e:
        logger.error("article_summaries保存失敗: %s", e)


def _generate_topic(news_list: List[Dict[str, str]]) -> str:
    """ニュース全体から話題フレーズを1個AI生成"""
    titles = "\n".join(
        f"【{CATEGORY_LABELS.get(n.get('category', 'other'), 'その他')}】{n['title']}"
        for n in news_list
    )
    prompt = (
        "以下のニュース全体から、最も会話で使いやすい話題フレーズを1個だけ出してください。\n"
        "・30〜50文字、そのまま口に出せる自然な一言\n"
        "・「」は含めない\n"
        "・JSONで {\"topic\": \"...\"} の形のみ返してください\n\n"
        f"{titles}"
    )
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=100,
            timeout=10,
            response_format={"type": "json_object"},
        )
        data = json.loads(res.choices[0].message.content)
        return str(data.get("topic", "")).strip()
    except Exception as e:
        logger.error("話題フレーズ生成失敗: %s", e)
        return ""


def summarize_articles(news_list: List[Dict[str, str]]) -> tuple:
    """
    記事ごとの要約とトピックフレーズを返す。キャッシュがあれば再利用。
    戻り値: (summaries, topic)
      summaries: {link: {"fact": str, "chat": str}}
      topic: str
    """
    if not news_list:
        return {}, ""

    links = [n.get("link", "") for n in news_list if n.get("link")]
    cached_raw = get_cached_summaries(links)

    # キャッシュをfact/chatに分解
    summaries: Dict[str, Dict[str, str]] = {}
    for link, summary_short in cached_raw.items():
        parts = summary_short.split("\n", 1)
        summaries[link] = {
            "fact": parts[0] if parts else "",
            "chat": parts[1] if len(parts) > 1 else "",
        }

    uncached = [n for n in news_list if n.get("link", "") not in summaries]

    if uncached:
        all_titles = "\n".join(
            f"{i+1}. 【{CATEGORY_LABELS.get(n.get('category', 'other'), 'その他')}】{n['title']}"
            for i, n in enumerate(news_list)
        )
        prompt = (
            "以下のニュース記事を要約してください。\n\n"
            "【fact】事実要約（主語＋行動＋現状、1文完結、40〜50文字以内、接続詞1つまで、タイトルの言い換え禁止）\n"
            "【chat】会話文（そのまま口に出せる、短く、軽いトーン、誰でも使いやすい）\n\n"
            "【topic】全ニュースから最も会話で使いやすい話題フレーズを1個\n"
            "・30〜50文字、そのまま口に出せる自然な一言、「」は含めない\n\n"
            f"articlesの数は入力ニュース数（{len(news_list)}件）と一致させること。\n"
            "JSONのみ返してください。\n\n"
            f"{all_titles}"
        )
        try:
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
                max_tokens=700,
                timeout=25,
                response_format=_ARTICLE_SUMMARY_SCHEMA,
            )
            data = json.loads(res.choices[0].message.content)
            articles_ai = data.get("articles", [])
            topic = str(data.get("topic", "")).strip()

            new_summaries: Dict[str, str] = {}
            for i, n in enumerate(news_list):
                link = n.get("link", "")
                if link in summaries:
                    continue  # キャッシュ済みはスキップ
                a = articles_ai[i] if i < len(articles_ai) else {}
                fact = str(a.get("fact", "")).strip() or n["title"][:40]
                chat = str(a.get("chat", "")).strip() or "気になる動きかも"
                summaries[link] = {"fact": fact, "chat": chat}
                new_summaries[link] = fact + "\n" + chat

            save_article_summaries(news_list, new_summaries)

        except Exception as e:
            logger.error("AI要約失敗、フォールバック使用: %s", e)
            for n in uncached:
                link = n.get("link", "")
                summaries[link] = {
                    "fact": n["title"][:40] if n.get("title") else "詳細不明",
                    "chat": "気になる動きかも",
                }
            topic = _generate_topic(news_list)
    else:
        # 全記事キャッシュ済み → topicだけ生成
        topic = _generate_topic(news_list)

    return summaries, topic


# =========================
# メッセージ作成ヘルパー
# =========================

def trim_text(text: str, max_len: int) -> str:
    text = str(text or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "…"


def normalize_tone(text: str) -> str:
    text = str(text or "").strip()
    replacements = {
        "影響を与える可能性があります": "影響出そう",
        "懸念されています": "気にされてる",
        "注目されています": "注目されてそう",
        "示しています": "っぽい",
        "と考えられます": "かも",
        "可能性があります": "かも",
        "されています": "てる",
        "です。": "",
        "ます。": "",
        "です": "",
        "ます": "",
    }
    for before, after in replacements.items():
        text = text.replace(before, after)
    return text.strip()


# =========================
# メッセージ作成
# =========================

def build_message(
    news: List[Dict[str, str]],
    summaries: Dict[str, Dict[str, str]],
    topic: str,
    mode: str = "push",
) -> List[str]:
    """ニュース本文を生成する。
    mode='push': 定期配信用（導入文・フッター付き）
    mode='reply': 手動取得用（記事リストのみ、シンプル）
    """
    lines = [] if mode == "reply" else ["今日のニュース、ここだけ", ""]
    for i, n in enumerate(news):
        num = CIRCLED[i] if i < len(CIRCLED) else f"{i + 1}."
        title = trim_text(n.get("title", ""), 30)
        link = n.get("link", "")
        s = summaries.get(link, {})
        fact = s.get("fact", "") or n["title"][:40]
        chat = s.get("chat", "") or "気になる動きかも"
        lines.append(f"{num}［{title}］")
        lines.append(f"👉 {fact}")
        lines.append(chat)
        lines.append("")

    if mode == "push":
        lines += [
            "気になる番号をそのまま入力",
            "例：1詳しく",
            "",
            "話題に困ったらこれで👇",
            "",
            f"「{topic}」" if topic else "",
        ]
    text = "\n".join(lines).strip()
    if len(text) > LINE_TEXT_SAFE_LIMIT:
        text = text[:LINE_TEXT_SAFE_LIMIT] + "\n…(省略)"
    return [text]


# =========================
# LINE送信
# =========================

def send(user_id: str, messages: List[str], with_quick_reply: bool = False) -> bool:
    message_objects = [{"type": "text", "text": m} for m in messages]
    message_objects = message_objects[:LINE_MAX_MESSAGE_OBJECTS]
    if with_quick_reply and message_objects:
        message_objects[-1]["quickReply"] = _NEWS_QUICK_REPLY

    for attempt in range(1, LINE_RETRY_MAX + 1):
        try:
            res = requests.post(
                LINE_URL,
                headers={
                    "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"to": user_id, "messages": message_objects},
                timeout=30,
            )
            if res.status_code == 429:
                wait = 2 ** (attempt - 1)  # 1s → 2s → 4s
                logger.warning(
                    "429 Too Many Requests: user=%s attempt=%d/%d wait=%ds body=%s",
                    user_id, attempt, LINE_RETRY_MAX, wait, res.text[:200],
                )
                if attempt < LINE_RETRY_MAX:
                    time.sleep(wait)
                continue
            res.raise_for_status()
            logger.info("送信成功: %s", user_id)
            return True
        except requests.RequestException as e:
            logger.error("LINE送信失敗 (user=%s): %s", user_id, e)
            return False

    logger.error("LINE送信失敗: 429リトライ上限超過 user=%s", user_id)
    return False




# =========================
# 実行
# =========================

def main():
    users = load_users()
    if not users:
        logger.warning("配信対象ユーザーが0件です")
        return

    news = fetch_news()
    if not news:
        logger.warning("RSS取得失敗: 全ユーザーへフォールバック通知")
        for uid, u in users.items():
            if u.get("active", True):
                send(uid, ["今日はニュース取得不安定っぽい\n少し時間置いてまた見て"])
        return

    news = filter_sent(news)
    if not news:
        logger.warning("未送信ニュースが0件のため配信スキップ")
        return

    sent_count = 0
    successfully_sent: set = set()

    for user_id, user in users.items():
        if not user.get("active", True):
            logger.info("非アクティブのためスキップ: %s", user_id)
            continue

        effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
        user["max_items"] = plan_max_items(effective_plan)

        logger.info("ニュース配信開始 user=%s raw_plan=%s effective_plan=%s genres=%s",
                    user_id, user.get("plan"), effective_plan, user.get("genres"))
        filtered = filter_news(news, user)
        logger.info("送信件数: user=%s %d件", user_id, len(filtered))

        summaries, topic = summarize_articles(filtered)
        messages = build_message(filtered, summaries, topic, mode="push")
        ok = send(user_id, messages, with_quick_reply=True)

        save_news_context(user_id, filtered, summaries)
        save_last_news_batch(user_id, filtered)

        if ok:
            sent_links = {n["link"] for n in filtered}
            successfully_sent.update(sent_links)
            sent_count += 1
            try:
                record_sent(filtered)
            except Exception as e:
                logger.error("sent_articles記録失敗: user=%s %s", user_id, e)
        else:
            logger.warning("送信失敗のためsent_articles記録スキップ: user=%s", user_id)

        time.sleep(2)

    logger.info("配信完了: %d/%d ユーザー", sent_count, len(users))


def notify_owner(text: str) -> None:
    if not OWNER_LINE_USER_ID:
        return
    try:
        requests.post(
            LINE_URL,
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": OWNER_LINE_USER_ID, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("オーナー通知失敗: %s", e)


def send_news_to_user(user_id: str) -> None:
    """1ユーザーへの即時配信（初回登録時など）"""
    news = fetch_news()
    if not news:
        logger.warning("初回配信: ニュース0件のためスキップ: %s", user_id)
        return

    plan = "free"
    genres: List[str] = []
    res_data = None

    try:
        res = supabase.table("users").select(
            "plan,genres,membership_status,membership_expires_at,trial_started_at,trial_extended_until"
        ).eq("user_id", user_id).single().execute()
        res_data = res.data
        if res_data:
            plan = res_data.get("plan", "free") or "free"
            genres = res_data.get("genres") or []
    except Exception as e:
        logger.warning("send_news_to_user: ユーザー情報取得失敗、デフォルト使用: %s %s", user_id, e)

    membership_status = res_data.get("membership_status", "none") if res_data else "none"
    membership_expires_at = res_data.get("membership_expires_at") if res_data else None
    trial_started_at = res_data.get("trial_started_at") if res_data else None
    trial_extended_until = res_data.get("trial_extended_until") if res_data else None

    user = {
        "user_id": user_id,
        "plan": plan,
        "genres": genres,
        "membership_status": membership_status,
        "membership_expires_at": membership_expires_at,
        "trial_started_at": trial_started_at,
        "trial_extended_until": trial_extended_until,
    }

    effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
    user["max_items"] = plan_max_items(effective_plan)

    filtered = filter_news(news, user)
    if not filtered:
        logger.warning("初回配信: フィルタ後0件のためスキップ: %s", user_id)
        return

    summaries, topic = summarize_articles(filtered)
    messages = build_message(filtered, summaries, topic, mode="push")
    send(user_id, messages, with_quick_reply=True)

    save_news_context(user_id, filtered, summaries)
    save_last_news_batch(user_id, filtered)
    logger.info(
        "初回配信完了: user=%s raw_plan=%s effective_plan=%s",
        user_id, plan, effective_plan,
    )


def fetch_news_for_reply(user_id: str, exclude_links: set = None) -> tuple:
    """手動ニュース取得（reply用）。fetch+filter+summarize+save。(messages, filtered_news)を返す。送信はしない。"""
    news = fetch_news()
    if not news:
        logger.warning("fetch_news_for_reply: ニュース0件: %s", user_id)
        return [], []

    plan = "free"
    genres: List[str] = []
    membership_status = "none"
    membership_expires_at = None
    trial_started_at = None
    trial_extended_until = None
    try:
        res = supabase.table("users").select(
            "plan,genres,membership_status,membership_expires_at,trial_started_at,trial_extended_until"
        ).eq("user_id", user_id).single().execute()
        if res.data:
            plan = res.data.get("plan", "free") or "free"
            genres = res.data.get("genres") or []
            membership_status = res.data.get("membership_status", "none")
            membership_expires_at = res.data.get("membership_expires_at")
            trial_started_at = res.data.get("trial_started_at")
            trial_extended_until = res.data.get("trial_extended_until")
    except Exception as e:
        logger.warning("fetch_news_for_reply: ユーザー情報取得失敗: %s %s", user_id, e)

    user = {
        "user_id": user_id,
        "plan": plan,
        "genres": genres,
        "membership_status": membership_status,
        "membership_expires_at": membership_expires_at,
        "trial_started_at": trial_started_at,
        "trial_extended_until": trial_extended_until,
    }
    effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
    actual_max = plan_max_items(effective_plan)
    # 重複除外前に十分な候補を確保するため、filter_news には大きめの上限を渡す
    user["max_items"] = MAX_FETCH_ITEMS

    filtered = filter_news(news, user)
    if not filtered:
        logger.warning("fetch_news_for_reply: フィルタ後0件: %s", user_id)
        return [], []

    # 重複除外（ハード）: exclude_linksにある記事を完全除外してから件数調整
    if exclude_links:
        filtered = [n for n in filtered if n.get("link") not in exclude_links]
        if not filtered:
            logger.info("fetch_news_for_reply: 全記事が重複、フォールバックメッセージ返却: %s", user_id)
            return ["今のところはこんなもんかな"], []

    # 重複除外後に実際の上限を適用
    filtered = filtered[:actual_max]

    summaries, topic = summarize_articles(filtered)
    messages = build_message(filtered, summaries, topic, mode="reply")

    # 1〜4件の場合は末尾に補足を追記
    if 0 < len(filtered) < 5:
        messages = [messages[0] + "\n\n今のところはこんなもんかな"]

    save_news_context(user_id, filtered, summaries)
    save_last_news_batch(user_id, filtered)

    return messages, filtered


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv:
        news = fetch_news()
        news = news[:5]
        summaries, topic = summarize_articles(news)
        msgs = build_message(news, summaries, topic, mode="push")
        for i, m in enumerate(msgs, 1):
            print(f"\n{'='*30}\n【{i}通目】\n{'='*30}\n{m}")
    else:
        print("=== 起動確認 ===")
        print(f"環境: {ENV}")
        if ENV == "test":
            print("◎ テスト環境で実行中")
        elif ENV == "prod":
            print("！！ 本番環境で実行中（注意）")
            print("！！ 本番環境です。内容を確認してください")
        try:
            main()
        except Exception as e:
            logger.error("main()で予期しないエラー: %s", e)
            notify_owner(f"[send_news] エラー発生\n{type(e).__name__}: {e}")