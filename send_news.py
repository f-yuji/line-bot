import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from urllib.parse import urlparse

import feedparser
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
        {"type": "action", "action": {"type": "message", "label": "開始", "text": "開始"}},
        {"type": "action", "action": {"type": "message", "label": "停止", "text": "停止"}},
        {"type": "action", "action": {"type": "message", "label": "ジャンル", "text": "ジャンル"}},
        {"type": "action", "action": {"type": "message", "label": "プラン", "text": "プラン"}},
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
        "道路工事", "橋梁工事", "建築着工", "設備更新", "施工管理"
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
        "社長交代", "経営統合", "提携解消", "サプライチェーン", "価格改定"
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
        "対ロ制裁", "防衛支援", "国境紛争", "難民問題", "地政学リスク"
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
        "第三者委員会", "処分検討", "会見拒否", "不適切投稿", "発言撤回"
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
        "G7", "G20", "外相", "首脳", "大統領", "会談"
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
        "刺殺", "殺害", "死亡", "事件", "容疑者", "被害者", "通り魔", "発生"
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


def shorten_url(url: str) -> str:
    try:
        res = requests.get(
            "https://is.gd/create.php",
            params={"format": "simple", "url": url},
            timeout=10,
        )
        res.raise_for_status()
        return res.text.strip()
    except Exception as e:
        logger.warning("URL短縮失敗 (%s): %s", url, e)
        return url


def plan_max_items(plan: str) -> int:
    # 暫定：全プラン5件。プラン制に戻す場合はここを修正
    return {
        "free": 5,
        "light": 5,
        "premium": 8,
    }.get(plan, DEFAULT_MAX_ITEMS)


def filter_sent(news_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """送信済み記事を除外"""
    if not news_list:
        return news_list
    links = [n["link"] for n in news_list]
    try:
        twelve_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
        res = supabase.table("sent_articles").select("link").in_("link", links).gte("sent_at", twelve_hours_ago).execute()
        sent_links = {row["link"] for row in res.data or []}
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
    ai: Dict[str, Any],
    messages: List[str],
    extra_news: List[Dict[str, str]] = None,
    extra_ai: List[Dict[str, Any]] = None,
) -> None:
    """配信内容を履歴として保存（Q&A用コンテキスト）"""
    articles_ai = ai.get("articles", [])

    def _build_item(i, n, a):
        return {
            "index": i + 1,
            "category": CATEGORY_LABELS.get(n.get("category", "other"), "その他"),
            "title": n["title"],
            "link": n.get("link", ""),
            "reason": a.get("reason", "") if isinstance(a, dict) else "",
            "interpretation": a.get("interpretation", "") if isinstance(a, dict) else "",
        }

    news_items  = [_build_item(i, n, articles_ai[i] if i < len(articles_ai) else {}) for i, n in enumerate(news)]
    extra_items = [_build_item(i, n, (extra_ai or [])[i] if extra_ai and i < len(extra_ai) else {}) for i, n in enumerate(extra_news or [])]

    payload = {
        "news_items":  news_items,
        "extra_items": extra_items,
        "summary":     ai.get("summary", []),
        "impact":      ai.get("impact", []),
        "topics":      ai.get("topics", []),
        "message_1":   messages[0] if len(messages) > 0 else "",
        "message_2":   messages[1] if len(messages) > 1 else "",
    }

    try:
        supabase.table("news_contexts").insert({
            "user_id": user_id,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }).execute()
        logger.info("ニュース保存成功 user=%s 件数=%d", user_id, len(news))
    except Exception as e:
        logger.error("ニュースコンテキスト保存失敗: %s", e)


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
                "plan": plan,
                "active": row.get("active", True),
                "genres": row.get("genres", []) or [],
                "max_items": plan_max_items(plan),
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
_INTL_PRIORITY       = {"G7", "G20", "外相", "首脳", "大統領", "会談", "首脳会談"}


def classify_category(article: Dict[str, str]) -> str:
    text = f"{article['title']} {article.get('summary', '')}"

    # 優先判定（1: scandal → 2: international → 3: スコアベース）
    if any(w in text for w in _SCANDAL_PRIORITY):
        return "scandal"
    if any(w in text for w in _INTL_PRIORITY):
        return "international"

    scores: Dict[str, int] = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        count = sum(1 for k in keywords if k in text)
        if count:
            scores[cat] = count
    return max(scores, key=scores.get) if scores else "other"


def score_article(article: Dict[str, str], user_genres: List[str]) -> int:
    text = f"{article['title']} {article.get('summary', '')}"
    score = 0

    for word in EXCLUDE_KEYWORDS:
        if word in text:
            score -= 3

    # カテゴリごとにcap: 強キーワードmax+3、弱キーワードmax+3
    for cat in set(list(STRONG_KEYWORDS.keys()) + list(CATEGORY_KEYWORDS.keys())):
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

def _fetch_single_rss(url: str, max_retries: int = 2) -> feedparser.FeedParserDict:
    """単一RSSソースを取得。リトライ付き"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Referer": "https://news.google.com/",
        "Cache-Control": "no-cache",
    })

    for attempt in range(1, max_retries + 1):
        try:
            res = session.get(url, timeout=20)

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

        except requests.RequestException as e:
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
            time.sleep(3 * attempt)

    logger.error("RSS全試行失敗: %s", url)
    return feedparser.FeedParserDict(entries=[])


def fetch_news() -> List[Dict[str, str]]:
    """複数RSSソースをすべて試し、成功したものをマージして返す"""
    all_entries: List[Any] = []

    for rss_url in RSS_SOURCES:
        logger.info("RSSソース試行: %s", rss_url)
        result = _fetch_single_rss(rss_url)
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

def _to_list(val) -> list:
    """GPTが配列の代わりに文字列を返した場合に改行で分割してリスト化する"""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        return [v.strip() for v in val.split("\n") if v.strip()]
    return []


def fallback_summary(news_list: List[Dict[str, str]]) -> Dict[str, Any]:
    """AI要約失敗時のタイトルベースフォールバック"""
    articles = [
        {
            "headline": n["title"][:25] if n.get("title") else "",
            "reason": "要約取得失敗",
            "interpretation": "詳細不明だが動きありそう",
        }
        for n in news_list
    ]
    return {"articles": articles, "summary": [], "impact": [], "topics": []}


def summarize(news_list: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    各記事の reason / interpretation と、全体の summary / impact / topics を返す。
    戻り値: {
        "articles": [{"reason": str, "interpretation": str}, ...],
        "summary":  [str, str, str],
        "impact":   [str, str, str],
        "topics":   [{"theme": str, "line": str}, ...],
    }
    """
    if not news_list:
        return {"articles": [], "summary": [], "impact": [], "topics": []}

    count = len(news_list)
    titles = "\n".join(f"{i + 1}. {n['title']}" for i, n in enumerate(news_list))

    prompt = (
        f"以下の{count}件のニュース見出しを、会話に使いやすい形でまとめてください。\n\n"
        "【文体ルール（必須）】\n"
        "・短く、会話調にする\n"
        "・敬語禁止、1文短く、主語省略OK\n"
        "・「〜そう」「〜かも」「〜っぽい」を適度に使う\n"
        "・説明調・専門家コメント調にしない\n"
        "・長い文は禁止\n\n"
        "【articles】各記事ごとに：\n"
        "・headline: 元の見出しを12〜25文字に再構成（「何が起きたか」含む、専門用語削る、体言止めOK）\n"
        "  例：「日銀、追加利上げを決定」「円安が150円台に再突入」\n"
        "・reason: 背景（10〜15文字、体言止め）\n"
        "  例：「金利差の拡大が影響」\n"
        "・interpretation: 読者視点（15〜25文字、〜そう/〜っぽい調）\n"
        "  例：「輸入コスト上がりやすい」\n"
        "  ※ 👉 は含めない\n\n"
        "【summary】全体を2〜3行：\n"
        "・各20文字以内、全体の流れを抽象的に一言\n"
        "  例：「生活コストじわ上げ」「金利と物価が同時に効いてる」\n\n"
        "【impact】3行：\n"
        "・各20文字以内、短く具体的に\n"
        "  例：「電気代じわ上げ」「ローンは慎重」「生活コスト全体に効く」\n\n"
        "【topics】会話ネタを3つ（summary/impactと重複しすぎない話題で）：\n"
        "・theme: テーマ（10文字以内）\n"
        "・line: そのまま使える一言（です・ます調OK、30〜50文字）\n"
        "  例：「最近ちょっと荒れてるらしいですね」\n\n"
        "JSONのみ返してください。キー: articles, summary, impact, topics\n\n"
        f"{titles}"
    )

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        raw = res.choices[0].message.content.strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        articles_raw = data.get("articles", [])
        return {
            "articles": articles_raw if isinstance(articles_raw, list) else [],
            "summary":  _to_list(data.get("summary", [])),
            "impact":   _to_list(data.get("impact", [])),
            "topics":   _to_list(data.get("topics", [])),
        }

    except json.JSONDecodeError as e:
        logger.error("AI要約失敗 fallback使用: JSONパース失敗 %s", e)
        return fallback_summary(news_list)
    except Exception as e:
        logger.error("AI要約失敗 fallback使用: %s", e)
        return fallback_summary(news_list)


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
# 会話フレーズ生成（テンプレ固定）
# =========================

_CATEGORY_PHRASES: Dict[str, List[tuple]] = {
    "real_estate": [
        ("不動産", "最近マンション価格すごいですよね。買い時ってどう思います？"),
        ("住宅",   "家賃や住宅費って最近どうです？やっぱ高くなってます？"),
    ],
    "interest_rates": [
        ("金利", "金利って最近また動いてますよね。住宅ローンとか気にしてます？"),
        ("金融", "最近金利の話よく聞きますけど、預金とか見直してます？"),
    ],
    "energy": [
        ("電気代", "電気代やガス代って最近どうです？やっぱ高いですよね。"),
        ("光熱費", "光熱費って節約してます？最近じわじわ上がってて気になって。"),
    ],
    "ai": [
        ("AI", "AIって最近どこでも聞きますけど、周りで使ってる人増えてません？"),
        ("AI", "ChatGPTとか使ってます？最近かなり便利になってきてますよね。"),
    ],
    "sports": [
        ("スポーツ", "最近スポーツのニュース多いですよね。何か注目してます？"),
        ("スポーツ", "大谷とか日本人選手の話よく出てますよね。チェックしてます？"),
    ],
    "economy": [
        ("物価", "最近なんでも高くないですか。買い物ちょっと迷いません？"),
        ("景気", "景気の話よく出てますよね。周りどんな感じです？"),
    ],
    "business": [
        ("ビジネス", "最近どこの会社も値上げしてますよね。仕事への影響とかあります？"),
        ("企業",    "業界的にどんな感じですか？最近いろいろ変化多そうで。"),
    ],
    "tech": [
        ("テック", "最近スマホとかアプリ、何か新しいの使ってます？"),
        ("IT",    "システム障害とかセキュリティの話、最近多いですよね。"),
    ],
    "international": [
        ("世界情勢", "海外のニュース最近多いですよね。なんか気になるやつあります？"),
        ("国際",    "世界ちょっとざわついてますよね。影響感じたりしてます？"),
    ],
    "materials": [
        ("食品",   "最近食費どうです？スーパー行くたびに値段変わってません？"),
        ("日用品", "日用品もじわじわ高くなってますよね。節約とか工夫してます？"),
    ],
    "construction": [
        ("建設", "工事費とか建材って最近すごい上がってるらしいですね。家の値段に影響しそうで。"),
        ("建設", "どこでも工事してますよね。インフラ整備ってお金かかるなって感じません？"),
    ],
    "entertainment": [
        ("エンタメ", "最近何か面白いドラマや映画とかあります？"),
        ("エンタメ", "Netflixとかサブスク、何か見てます？最近話題多いですよね。"),
    ],
    "scandal": [
        ("ニュース", "最近ちょっと騒がしいニュース多いですよね。気になったのあります？"),
    ],
    "other": [
        ("ニュース", "最近気になったニュース、何かありました？"),
        ("話題",    "最近周りで何か話題になってることありますか？"),
    ],
}

_PHRASE_FALLBACKS = [
    ("物価", "最近なんでも高くないですか。買い物ちょっと迷いません？"),
    ("AI",   "AIって最近どこでも聞きますけど、周りで使ってる人増えてません？"),
    ("金利", "金利って最近また動いてますよね。住宅ローンとか気にしてます？"),
]


def _generate_phrases(news: List[Dict[str, str]]) -> List[tuple]:
    phrases: List[tuple] = []
    used_cats: set = set()

    for n in news:
        if len(phrases) >= 3:
            break
        cat = n.get("category", "other")
        if cat in used_cats:
            continue
        options = _CATEGORY_PHRASES.get(cat)
        if not options:
            continue
        phrases.append(random.choice(options))
        used_cats.add(cat)

    for fb in _PHRASE_FALLBACKS:
        if len(phrases) >= 3:
            break
        if fb not in phrases:
            phrases.append(fb)

    return phrases[:3]


# =========================
# メッセージ作成
# =========================

def _build_msg1(news_lines: list, summary_lines: list, impact_lines: list) -> str:
    """msg1をLINE文字数制限内に収める（優先度順削除: impact→summary→ニュースのみ）"""
    def _assemble(summ: list, imp: list) -> str:
        parts = list(news_lines)
        if summ:
            parts += ["要するにこんな感じ", ""] + summ
        if imp:
            parts += ["", "影響あるとしたら", ""] + imp
        return "\n".join(parts)

    text = _assemble(summary_lines, impact_lines)
    if len(text) <= LINE_TEXT_SAFE_LIMIT:
        return text

    # impact を1行ずつ削る
    imp = list(impact_lines)
    while imp:
        imp.pop()
        text = _assemble(summary_lines, imp)
        if len(text) <= LINE_TEXT_SAFE_LIMIT:
            return text

    # summary を1行ずつ削る
    summ = list(summary_lines)
    while summ:
        summ.pop()
        text = _assemble(summ, [])
        if len(text) <= LINE_TEXT_SAFE_LIMIT:
            return text

    # 最終手段：ニュースのみ（末尾カット）
    text = _assemble([], [])
    if len(text) > LINE_TEXT_SAFE_LIMIT:
        text = text[:LINE_TEXT_SAFE_LIMIT] + "\n…(省略)"
    return text


def build_message(
    news: List[Dict[str, str]],
    ai: Dict[str, Any],
) -> List[str]:
    articles_ai = ai.get("articles", [])
    summary     = _to_list(ai.get("summary", []))
    impact      = _to_list(ai.get("impact", []))


    # ── 1通目 ──
    news_lines = ["今日のニュース、ここだけ。", ""]
    for i, n in enumerate(news):
        num = CIRCLED[i] if i < len(CIRCLED) else f"{i + 1}."
        cat = CATEGORY_LABELS.get(n.get("category", "other"), "その他")
        a = articles_ai[i] if i < len(articles_ai) else {}
        reason = normalize_tone(trim_text(a.get("reason", "")        if isinstance(a, dict) else "", 20))
        interp = normalize_tone(trim_text(a.get("interpretation", "") if isinstance(a, dict) else "", 24))
        if not interp:
            interp = "気になる動きかも"

        headline = trim_text((a.get("headline") or "") if isinstance(a, dict) else "", 25) or trim_text(n["title"], 25)
        news_lines.append(f"{num}【{cat}】")
        news_lines.append(headline)
        if reason:
            news_lines.append(f"→ {reason}")
        news_lines.append(f"👉 {interp}")
        news_lines.append("")

    summary_lines = [
        f"・{normalize_tone(trim_text(s, 24))}"
        for s in summary[:3]
        if normalize_tone(trim_text(s, 24))
    ]
    impact_lines = [
        f"・{normalize_tone(trim_text(imp, 22))}"
        for imp in impact[:3]
        if normalize_tone(trim_text(imp, 22))
    ]

    msg1 = _build_msg1(news_lines, summary_lines, impact_lines)

    # ── 話題フレーズ ──
    t_lines = ["話題に困ったらこれで乗り切ろう⬇️", ""]
    for label, phrase in _generate_phrases(news):
        t_lines.append(label)
        t_lines.append(phrase)
        t_lines.append("")
    t_lines.append("気になるニュース、このLINEで聞いてもらえれば👌\n記事のリンクほしいときも言って")
    phrase_section = "\n".join(t_lines)

    merged = msg1 + "\n\n" + phrase_section
    if len(merged) > LINE_TEXT_SAFE_LIMIT:
        merged = merged[:LINE_TEXT_SAFE_LIMIT] + "\n…(省略)"

    return [merged]


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

        logger.info("ニュース配信開始 user=%s plan=%s genres=%s",
                    user_id, user.get("plan"), user.get("genres"))
        filtered = filter_news(news, user)
        logger.info("送信件数: user=%s %d件", user_id, len(filtered))

        ai_result = summarize(filtered)
        messages = build_message(filtered, ai_result)
        ok = send(user_id, messages, with_quick_reply=True)

        sent_links = {n["link"] for n in filtered}
        extra_news = [n for n in news if n["link"] not in sent_links][:5]
        save_news_context(user_id, filtered, ai_result, messages, extra_news)

        if ok:
            successfully_sent.update(sent_links)
            sent_count += 1
        else:
            logger.warning("送信失敗のためsent_articles記録スキップ: user=%s", user_id)

        time.sleep(2)

    sent_news = [n for n in news if n["link"] in successfully_sent]
    record_sent(sent_news)
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

    user = {
        "user_id": user_id,
        "plan": "free",
        "genres": [],
        "max_items": 5,
    }
    filtered = filter_news(news, user)
    if not filtered:
        logger.warning("初回配信: フィルタ後0件のためスキップ: %s", user_id)
        return

    ai_result  = summarize(filtered)
    messages   = build_message(filtered, ai_result)
    send(user_id, messages, with_quick_reply=True)

    sent_links = {n["link"] for n in filtered}
    extra_news = [n for n in news if n["link"] not in sent_links][:5]
    save_news_context(user_id, filtered, ai_result, messages, extra_news)
    logger.info("初回配信完了: %s", user_id)


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv:
        news = fetch_news()
        news = news[:5]
        ai_result = summarize(news)
        msgs = build_message(news, ai_result)
        for i, m in enumerate(msgs, 1):
            print(f"\n{'='*30}\n【{i}通目】\n{'='*30}\n{m}")
    else:
        print("=== 起動確認 ===")
        print(f"環境: {ENV}")
        if ENV == "test":
            print("🟢 テスト環境で実行中")
        elif ENV == "prod":
            print("🔴 本番環境で実行中（注意）")
            print("🔴 本番環境です。内容を確認してください")
        print("===== 処理開始 =====")
        try:
            main()
        except Exception as e:
            logger.error("main()で予期しないエラー: %s", e)
            notify_owner(f"[send_news] エラー発生\n{type(e).__name__}: {e}")
        print("===== 処理終了 =====")
