import functools
import html
import csv
import json
import logging
import os
import re
import requests
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, abort, render_template, redirect, url_for, session, flash, Response, jsonify
from openai import OpenAI
from supabase import create_client
from services.signal_stage import SIGNAL_STAGES, STAGE_RANK, evaluate_signal_stage
from services.entry_mode import ENTRY_MODE_LABELS, classify_entry_case, ma_gap_pct, regime_scores, resolve_entry_mode
from services.h5_primary import (
    H5_ENTRY_EXECUTION_NOTE,
    H5_ACTIVE_CASE_KEYS,
    H5_LIVE_LIMITED_CASE_KEY,
    H5_PRIMARY_CASE_KEY,
    H5_PRIMARY_DISPLAY_NAME,
    H5_PRIMARY_RULES,
    H5_RESEARCH_CASE_KEY,
    evaluate_h5_primary_entry,
    h5_overheat_score,
)
from services.price_fetcher import (
    H5_ENTRY_STATUS_PRIORITY,
    build_h5_price_assist_fields,
    decorate_h5_price_assist_cards,
    get_yfinance_current_price,
)
from services.trade_assist_history import decorate_history_rows
from services.nikkei_correlation import decorate_nikkei_correlation
from services.rebound_diagnostics import decorate_rebound_diagnostics
from services.h5_market_environment import attach_environment_to_rows, build_h5_environment_snapshot
from services.h5_reason_builder import (
    build_h5_ai_reasons,
    get_cached_reasons,
    load_reason_cache,
    upsert_cached_reasons,
)
from services.h5_shap_explainer import (
    DEFAULT_MODEL_KEY as H5_SHAP_DEFAULT_MODEL_KEY,
    compute_shap_for_candidate,
    load_shap_cache,
    save_shap_cache,
)
from services.h5_shap_reason_builder import build_shap_reason_comment, merge_shap_reason
from services.position_sizing import calculate_virtual_position_size

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    QuickReply,
    QuickReplyItem,
    MessageAction,
    PostbackAction,
    FlexMessage,
    FlexBubble,
    FlexBox,
    FlexButton,
    FlexText,
)
from linebot.v3.webhooks import FollowEvent, MessageEvent, PostbackEvent, TextMessageContent

import settings_loader as _settings_loader
from send_news import fetch_news_for_reply, get_recent_sent_links
from nikkei_alert import (
    get_drop_list_for_reply,
    get_single_stock_change,
    format_drop_list_text,
    format_company_profile_text,
    _format_day_change_text,
    get_ai_comment as get_stock_ai_comment,
    get_nikkei_change_pct,
    NIKKEI225,
)
from market_summary import get_all_markets_reply
from subsidy_bot import (
    get_subsidy_list,
    format_subsidy_page,
    get_last_subsidy_batch,
    normalize_prefecture,
    save_last_subsidy_batch,
    SUBSIDY_CATEGORIES,
    SUBSIDY_PAGE_SIZE,
    update_last_subsidy_batch_offset,
)

# 急落株一覧コンテキスト（user_id → drop list）
_user_drop_list: dict[str, list] = {}
# 補助金 条件入力待ち状態（user_id → "await_prefecture" | "await_category"）
_user_subsidy_state: dict[str, str] = {}

# ─── 初期設定 ───
load_dotenv()


def _get_optional_env(name: str) -> str:
    return os.getenv(name, "").strip()


def _get_mode_env(base_name: str, mode: str, *, required: bool = False) -> str:
    candidates = []
    normalized_mode = (mode or "").strip().upper()
    if normalized_mode:
        candidates.append(f"{base_name}_{normalized_mode}")
    candidates.append(base_name)

    for candidate in candidates:
        value = _get_optional_env(candidate)
        if value:
            return value

    if required:
        raise KeyError(candidates[0])
    return ""

SUPABASE_MODE = _get_optional_env("SUPABASE_MODE") or _get_optional_env("ENV")
SUPABASE_URL = _get_mode_env("SUPABASE_URL", SUPABASE_MODE, required=True)
SUPABASE_KEY = _get_mode_env("SUPABASE_KEY", SUPABASE_MODE, required=True)
LINE_CHANNEL_ACCESS_TOKEN = _get_optional_env("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = _get_optional_env("LINE_CHANNEL_SECRET")
if not LINE_CHANNEL_ACCESS_TOKEN:
    raise KeyError("LINE_CHANNEL_ACCESS_TOKEN")
if not LINE_CHANNEL_SECRET:
    raise KeyError("LINE_CHANNEL_SECRET")
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ENV = os.getenv("ENV", "prod")
LINE_API_BASE = "https://api.line.me"
print("SUPABASE_URL =", SUPABASE_URL)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)
app.secret_key = _get_optional_env("SECRET_KEY") or _get_optional_env("WEB_ADMIN_TOKEN") or "changeme"
WEB_ADMIN_TOKEN = _get_optional_env("WEB_ADMIN_TOKEN")
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
JST = ZoneInfo("Asia/Tokyo")


@app.template_filter("jst")
def jst_filter(value, fmt="%Y-%m-%d %H:%M"):
    if not value:
        return ""
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST).strftime(fmt)
    except Exception:
        return str(value)


def _with_ai_priority_stage(row: dict, fallback_market_adjustment: dict | None = None) -> dict:
    copied = dict(row)
    copied["raw_signal_stage"] = row.get("signal_stage")
    try:
        bad_score = float(copied.get("bad_news_score") or 0)
    except Exception:
        bad_score = 0.0
    market_adjustment = {
        "regime": copied.get("market_regime") or (fallback_market_adjustment or {}).get("regime") or "normal",
        "label": copied.get("market_regime_label") or (fallback_market_adjustment or {}).get("label") or "通常",
        "ai_threshold_adjust": copied.get("market_threshold_adjust")
        if copied.get("market_threshold_adjust") is not None
        else (fallback_market_adjustment or {}).get("ai_threshold_adjust", 0),
        "entry_size_multiplier": 1.0,
        "reason": copied.get("market_regime_reason") or (fallback_market_adjustment or {}).get("reason") or "",
    }
    if copied.get("is_excluded") or bad_score >= 80:
        stage_result = evaluate_signal_stage(None, copied.get("signal_score") or copied.get("score"))
    else:
        stage_result = evaluate_signal_stage(
            copied.get("signal_probability"),
            copied.get("signal_score") if copied.get("signal_score") is not None else copied.get("score"),
            copied.get("expected_value"),
            _settings_loader.get_settings(),
            market_adjustment,
        )
    copied["signal_stage"] = stage_result["stage"]
    copied["stage_label"] = stage_result["stage_label"]
    copied["stage_rank"] = stage_result["stage_rank"]
    copied["stage_reason"] = stage_result["reason"]
    return copied


def _current_market_adjustment() -> dict:
    """DB保存値から market_adjustment を生成する (表示・stage計算共通ソース)."""
    from datetime import date as _date, timezone as _tz, timedelta as _td
    from services.trading_calendar import trading_day_distance
    _JST = _tz(_td(hours=9))
    today_jst = datetime.now(_JST).date()

    regime_meta = {
        "panic_selloff": ("パニック売り", 0.10, 0.0),
        "panic_rebound": ("パニック反発", 0.10, 0.5),
        "risk_off": ("弱地合い", 0.05, 1.0),
        "strong_risk_on": ("強リスクオン", 0.05, 1.0),
        "risk_on": ("リスクオン", 0.0, 1.0),
        "normal": ("通常", 0.0, 1.0),
    }

    def _float_or_none(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    result: dict = {
        "regime": "normal",
        "label": "通常",
        "ai_threshold_adjust": 0.0,
        "entry_size_multiplier": 1.0,
        "reason": "",
        "nikkei_pct": None,
        "topix_pct": None,
        "nikkei_change_yen": None,
        "updated_at": None,
        "trade_date": None,
        "trade_date_stale": False,
    }

    # UIの市場表示は market_regime を正にする。
    # watchlist は銘柄判定時点のスナップショットなので、最新地合い表示に使うと日付と数値がズレる。
    try:
        mr_rows = (
            supabase.table("market_regime")
            .select(
                "trade_date,mode,reason,nikkei_change_pct,topix_change_pct,"
                "shock_score,created_at,updated_at"
            )
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if mr_rows:
            ctx = mr_rows[0]
            regime = ctx.get("mode") or "normal"
            label, threshold_adjust, size_multiplier = regime_meta.get(regime, (regime, 0.0, 1.0))
            td_str = ctx.get("trade_date")
            result.update({
                "regime": regime,
                "label": label,
                "ai_threshold_adjust": threshold_adjust,
                "entry_size_multiplier": size_multiplier,
                "reason": ctx.get("reason") or "",
                "nikkei_pct": _float_or_none(ctx.get("nikkei_change_pct")),
                "topix_pct": _float_or_none(ctx.get("topix_change_pct")),
                "nikkei_change_yen": None,
                "updated_at": ctx.get("updated_at") or ctx.get("created_at"),
                "trade_date": td_str,
            })
            if td_str:
                try:
                    latest_rows = (
                        supabase.table("stock_feature_snapshots")
                        .select("trade_date")
                        .order("trade_date", desc=True)
                        .limit(1)
                        .execute()
                        .data or []
                    )
                    latest_feature_date = str(latest_rows[0].get("trade_date")) if latest_rows else today_jst.isoformat()
                    distance = trading_day_distance(supabase, str(td_str), latest_feature_date)
                    if distance is None:
                        delta = (today_jst - _date.fromisoformat(str(td_str))).days
                        result["trade_date_stale"] = delta >= 4
                    else:
                        result["trade_date_stale"] = distance >= 2
                except Exception:
                    pass
            return result
    except Exception as e:
        logger.warning("market_regime context lookup failed: %s", e)

    # fallback: market_regime が未作成/未取得の場合だけ watchlist の判定時点情報を使う
    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select(
                "market_regime,market_regime_label,market_threshold_adjust,"
                "market_regime_reason,market_nikkei_pct,market_topix_pct,"
                "market_nikkei_change_yen,updated_at"
            )
            .not_.is_("market_regime", "null")
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            ctx = rows[0]
            result.update({
                "regime": ctx.get("market_regime") or "normal",
                "label": ctx.get("market_regime_label") or "通常",
                "ai_threshold_adjust": float(ctx.get("market_threshold_adjust") or 0),
                "reason": ctx.get("market_regime_reason") or "",
                "nikkei_pct": _float_or_none(ctx.get("market_nikkei_pct")),
                "topix_pct": _float_or_none(ctx.get("market_topix_pct")),
                "nikkei_change_yen": _float_or_none(ctx.get("market_nikkei_change_yen")),
                "updated_at": ctx.get("updated_at"),
            })
    except Exception as e:
        logger.warning("market context fallback from watchlist failed: %s", e)

    return result


def _current_long_term_market_regime() -> dict:
    fallback = {
        "regime": "neutral",
        "label": "中立",
        "score": None,
        "trade_date": None,
        "ma25_above_ratio": None,
        "ma75_above_ratio": None,
        "vix": None,
        "reasons": [],
    }
    try:
        rows = (
            supabase.table("long_term_market_regime")
            .select("*")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if not rows:
            return fallback
        row = rows[0]
        reasons = row.get("reasons") or []
        if isinstance(reasons, str):
            try:
                reasons = json.loads(reasons)
            except Exception:
                reasons = [reasons]
        row["reasons"] = reasons
        return {**fallback, **row}
    except Exception as e:
        logger.warning("long_term_market_regime lookup failed: %s", e)
        return fallback

# ─── ログ ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

print("=== 起動確認 ===")
print(f"環境: {ENV}")
print(f"SUPABASE_MODE: {SUPABASE_MODE or 'legacy'}")
if ENV == "test":
    print("◎ テスト環境で実行中")
elif ENV == "prod":
    print("！！ 本番環境で実行中（注意）")
    print("！！ 本番環境です。内容を確認してください")

# ─── ジャンル定義 ───
DISPLAY_GENRE_MAP = {
    "経済": ["economy", "interest_rates", "real_estate"],
    "仕事": ["business", "construction"],
    "国際": ["international"],
    "AI・テック": ["ai", "tech"],
    "暮らし": ["energy", "materials"],
    "海外": ["overseas"],
}

INTERNAL_TO_DISPLAY = {
    cat: display
    for display, cats in DISPLAY_GENRE_MAP.items()
    for cat in cats
}

DISPLAY_GENRE_ORDER = list(DISPLAY_GENRE_MAP.keys())

DISPLAY_GENRE_ALIASES: dict = {
    "お金": "経済",
    "世界": "国際",
    "AI": "AI・テック",
    "テック": "AI・テック",
    "IT": "AI・テック",
    "it": "AI・テック",
    "生活": "暮らし",
    "海外ニュース": "海外",
    "world": "海外",
}


# ─── DB操作 ───
def get_user(user_id: str):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


def get_line_profile(user_id: str) -> str:
    """LINE APIからdisplay_nameを取得。失敗時は空文字を返す"""
    try:
        res = requests.get(
            f"{LINE_API_BASE}/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            timeout=10,
        )
        if res.status_code == 200:
            return res.json().get("displayName", "")
    except Exception as e:
        logger.error("LINEプロフィール取得失敗: user=%s %s", user_id, e)
    return ""


def save_user(user_id: str, active=True, genres=None, display_name: str = ""):
    if genres is None:
        genres = []
    supabase.table("users").upsert({
        "user_id": user_id,
        "active": active,
        "genres": genres,
        "display_name": display_name,
        "drop_alert_enabled": False,
    }).execute()
    logger.info("Supabase保存: user=%s active=%s genres=%s", user_id, active, genres)


def ensure_user(user_id: str):
    user = get_user(user_id)
    if not user:
        display_name = get_line_profile(user_id)
        save_user(user_id, active=True, genres=[], display_name=display_name)
        logger.info("新規ユーザー登録: %s display_name=%s", user_id, display_name)
        return {
            "user_id": user_id,
            "active": True,
            "genres": [],
            "subsidy_continue_pending": False,
            "drop_alert_enabled": False,
        }, True
    display_name = get_line_profile(user_id)
    if display_name:
        try:
            supabase.table("users").update({"display_name": display_name}).eq("user_id", user_id).execute()
        except Exception as e:
            logger.error("display_name更新失敗: user=%s %s", user_id, e)
    user.setdefault("last_news_question_targets", None)
    user.setdefault("last_news_question_at", None)
    user.setdefault("subsidy_continue_pending", False)
    user.setdefault("drop_alert_enabled", False)
    return user, False


def set_subsidy_continue_pending(user_id: str, user: dict, pending: bool) -> None:
    user["subsidy_continue_pending"] = pending
    try:
        supabase.table("users").update({"subsidy_continue_pending": pending}).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("subsidy_continue_pending update error user=%s pending=%s %s", user_id, pending, e)


# ─── 補助 ───
def normalize_genres(raw_text: str):
    text = raw_text.replace("\u3000", " ")
    items = [x.strip() for x in text.split(",") if x.strip()]
    result = []
    lower_map = {k.lower(): k for k in DISPLAY_GENRE_MAP}
    alias_lower = {k.lower(): v for k, v in DISPLAY_GENRE_ALIASES.items()}

    for item in items:
        key = item.lower()
        display = lower_map.get(key)
        if not display:
            display = alias_lower.get(key)

        if display and display in DISPLAY_GENRE_MAP:
            for cat in DISPLAY_GENRE_MAP[display]:
                if cat not in result:
                    result.append(cat)

    return result


def format_genres(genres):
    if not genres:
        return "なし"

    seen = []
    for cat in genres:
        d = INTERNAL_TO_DISPLAY.get(cat)
        if d and d not in seen:
            seen.append(d)
    return ", ".join(seen) if seen else "なし"


# ─── LINE UI ヘルパー ───

def _subsidy_category_quick_reply() -> QuickReply:
    items = [QuickReplyItem(action=MessageAction(label=c, text=c)) for c in SUBSIDY_CATEGORIES]
    return QuickReply(items=items)


def main_quick_reply() -> QuickReply:
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label="ニュース", text="ニュース")),
        QuickReplyItem(action=MessageAction(label="リンク", text="リンク")),
        QuickReplyItem(action=MessageAction(label="相場", text="相場")),
        QuickReplyItem(action=MessageAction(label="急落株", text="急落株")),
        QuickReplyItem(action=MessageAction(label="補助金", text="補助金")),
        QuickReplyItem(action=MessageAction(label="使い方", text="使い方")),
    ])


def reply_text(reply_token: str, text: str, quick_reply: QuickReply = None) -> None:
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            msg = TextMessage(text=text)
            if quick_reply:
                msg.quick_reply = quick_reply
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[msg])
            )
    except Exception as e:
        logger.error("LINE返信エラー: %s", e)


def reply_flex(reply_token: str, flex_msg: FlexMessage) -> None:
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[flex_msg])
            )
    except Exception as e:
        logger.error("LINE返信エラー: %s", e)


GENRE_DESC = {
    "経済": "金利・為替・不動産",
    "仕事": "業界・法改正・労働",
    "国際": "海外情勢・外交",
    "AI・テック": "AI・IT・科学",
    "暮らし": "医療・教育・生活",
    "海外": "米・英・韓・亜・印の現地ニュース",
}


def build_genre_flex(current_genres: list) -> FlexMessage:
    rows = []
    layout_rows = [
        ["経済", "仕事"],
        ["国際", "AI・テック"],
        ["暮らし", "海外"],
    ]

    for chunk in layout_rows:
        cells = []
        for display in chunk:
            internals = DISPLAY_GENRE_MAP[display]
            selected = any(c in current_genres for c in internals)

            cell = FlexBox(
                layout="vertical",
                contents=[
                    FlexButton(
                        action=PostbackAction(
                            label=f"✓{display}" if selected else display,
                            data=f"toggle_display_genre:{display}",
                            display_text=display,
                        ),
                        style="primary" if selected else "secondary",
                        height="sm",
                    ),
                    FlexText(
                        text=GENRE_DESC.get(display, ""),
                        size="xs",
                        color="#999999",
                        wrap=True,
                    ),
                ],
                spacing="xs",
                flex=1,
            )
            cells.append(cell)

        rows.append(FlexBox(layout="horizontal", contents=cells, spacing="sm"))

    header_note = f"現在: {format_genres(current_genres)}" if current_genres else "未選択の場合はジャンル指定なしで\n配信されます"

    bubble = FlexBubble(
        header=FlexBox(
            layout="vertical",
            contents=[
                FlexText(text="受け取るニュース", weight="bold", size="md"),
                FlexText(text=header_note, size="xs", color="#888888", wrap=True),
            ],
        ),
        body=FlexBox(
            layout="vertical",
            contents=rows,
            spacing="md",
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    action=PostbackAction(
                        label="決定",
                        data="confirm_genres",
                        display_text="ジャンルを決定",
                    ),
                    style="primary",
                    height="sm",
                ),
                FlexButton(
                    action=PostbackAction(
                        label="すべて解除",
                        data="clear_genres",
                        display_text="クリア",
                    ),
                    style="link",
                    height="sm",
                    color="#aaaaaa",
                ),
            ],
        ),
    )

    flex_msg = FlexMessage(alt_text="受け取るニュース", contents=bubble)
    flex_msg.quick_reply = main_quick_reply()
    return flex_msg


# ─── Q&A ───
_DETAIL_KEYWORDS = ["詳しく", "もう少し", "なんで", "なぜ", "具体的に", "仕組み"]


def is_link_request(text: str) -> bool:
    """リンク要求として自然な入力かどうか（部分一致の誤反応を防ぐ）"""
    import re
    t = (text or "").strip()
    if t in {"リンク", "URL", "url", "記事リンク", "元記事"}:
        return True
    # 番号付きリンク要求（数字や丸数字を含み、「リンク」「URL」で終わる or 「のリンク」系）
    if re.search(r"[0-9①-⑩]", t) and (
        t.endswith("リンク") or t.endswith("URL") or t.endswith("url")
        or "のリンク" in t or "のURL" in t or "のurl" in t
    ):
        return True
    # 自然な短文要求: 「リンク」「URL」「url」を含む短い発話（20文字以内）
    # 例: 「リンクちょうだい」「URLくれ」「リンク見せて」
    _LINK_WORDS = ("リンク", "URL", "url")
    if len(t) <= 20 and any(w in t for w in _LINK_WORDS):
        return True
    return False


def _strip_any_leading_number(text: str) -> str:
    """返答文頭の番号（丸数字・半角数字どちらも）を1個除去する（先頭のみ、本文中は触らない）"""
    import re
    t = (text or "").strip()
    t = re.sub(r"^\s*[①-⑩]\s*", "", t)
    t = re.sub(r"^\s*\d+[\.．]?\s*", "", t)
    return t.strip()

_CONTEXT_TOKEN_STOPWORDS = {
    "経済", "金利", "影響", "理由", "内容", "状況",
    "問題", "情報", "世界", "ニュース", "話題",
}


_CIRCLED = "①②③④⑤⑥⑦⑧⑨⑩"

_CONTEXT_TTL_HOURS = 24

_BLOCKLIST = [
    "付き合", "好き",
    "お前誰", "何者", "自己紹介",
    "会話ネタ", "話のネタ", "雑談ネタ", "ネタ教えて", "何話せばいい", "何話す",
]

_REJECT_TEXT = "ニュースの内容で気になることあれば聞いて\n番号やリンクでもいけるよ"
_BLOCKLIST_TEXT = "ニュースの話で聞いてほしいな"

_QUESTION_SIGNALS = [
    "？", "?", "って", "とは", "なに", "なぜ", "なんで",
    "どう", "どこ", "いつ", "誰", "だれ", "教えて",
    "知りたい", "聞きたい", "意味", "仕組み", "違い",
]


def get_latest_news_context(user_id: str) -> Optional[dict]:
    try:
        res = (
            supabase.table("news_contexts")
            .select("payload, sent_at")
            .eq("user_id", user_id)
            .order("sent_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("ニュースコンテキスト取得失敗: %s", e)
        return None


def get_last_news_batch(user_id: str) -> Optional[list]:
    """last_news_batchから直近5件のアイテムを取得。未取得時はNone。"""
    try:
        res = (
            supabase.table("last_news_batch")
            .select("items")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        if res.data:
            return res.data.get("items") or []
    except Exception as e:
        logger.error("last_news_batch取得失敗: %s %s", user_id, e)
    return None


def _build_link_message(items: list) -> str:
    """last_news_batchのitemsから①タイトル\nURL形式のリンク一覧を生成"""
    lines = []
    # index昇順でソートして①〜⑤の対応を本文と一致させる
    sorted_items = sorted(items, key=lambda x: x.get("index", 0))
    for item in sorted_items[:5]:
        idx = item.get("index", 0)
        title = str(item.get("title", "") or "")
        if len(title) > 30:
            title = title[:29] + "…"
        link = item.get("link", "")
        num = _CIRCLED[idx - 1] if 1 <= idx <= len(_CIRCLED) else f"{idx}."
        lines.append(f"{num} {title}")
        lines.append(link)
        lines.append("")
    return "\n".join(lines).strip()


def _push_text(user_id: str, text: str) -> None:
    """LINE push APIでテキストを送信"""
    try:
        requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers=get_line_headers(),
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        logger.error("push失敗: user=%s %s", user_id, e)


def is_detail_only_request(text: str) -> bool:
    t = (text or "").strip()
    return t in {"詳しく", "もっと詳しく", "深掘り", "くわしく"}


def save_last_news_question_targets(user_id: str, targets: list, ctx: dict) -> None:
    if not targets:
        return
    try:
        sent_at = ctx.get("sent_at") if ctx else None
        supabase.table("users").update({
            "last_news_question_targets": targets,
            "last_news_question_at": datetime.now(timezone.utc).isoformat(),
            "last_news_context_sent_at": sent_at,
        }).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("last_news_question_targets保存失敗: %s", e)


def get_reusable_last_news_targets(user: dict, ctx: dict) -> list:
    targets = user.get("last_news_question_targets")
    asked_at = user.get("last_news_question_at")
    saved_ctx = user.get("last_news_context_sent_at")

    if not targets or not ctx:
        return []

    if saved_ctx != ctx.get("sent_at"):
        return []

    if asked_at:
        try:
            asked_dt = datetime.fromisoformat(str(asked_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - asked_dt > timedelta(minutes=10):
                return []
        except Exception:
            return []

    return [int(x) for x in targets if isinstance(x, int)]


def clear_last_news_question_targets(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "last_news_question_targets": None,
            "last_news_question_at": None,
            "last_news_context_sent_at": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").lower())


def _normalize_query_for_match(text: str) -> str:
    t = _normalize_text(text)

    suffixes = [
        "ってなんなの", "ってなんだ", "ってなに", "って何",
        "とはなんなの", "とはなんだ", "とはなに", "とは何",
        "ってだれ", "って誰", "とはだれ", "とは誰",
        "ってどこ", "とはどこ", "って何者",
        "って", "とは", "とは？", "とは?", "？", "?", "ー", "～",
    ]

    changed = True
    while changed:
        changed = False
        for s in suffixes:
            ns = _normalize_text(s)
            if ns and t.endswith(ns):
                t = t[: -len(ns)]
                changed = True

    return t.strip()


def _resolve_drop_stock_from_text(user_id: str, text: str) -> Optional[dict]:
    query = _normalize_text(text)
    if not query:
        return None

    drops = _user_drop_list.get(user_id, [])
    if drops:
        for stock in drops:
            code = str(stock.get("code") or "")
            name = str(stock.get("name") or "")
            norm_name = _normalize_text(name)
            if query == code or query == norm_name:
                return stock
        for stock in drops:
            name = str(stock.get("name") or "")
            norm_name = _normalize_text(name)
            if norm_name and (query in norm_name or norm_name in query):
                return stock
        return None

    if re.fullmatch(r"[0-9]{4}", text) and text in NIKKEI225:
        return {"code": text, "name": NIKKEI225[text]}

    for code, name in NIKKEI225.items():
        norm_name = _normalize_text(name)
        if query == code or query == norm_name:
            return {"code": code, "name": name}
    for code, name in NIKKEI225.items():
        norm_name = _normalize_text(name)
        if norm_name and (query in norm_name or norm_name in query):
            return {"code": code, "name": name}
    return None


def _is_context_alive(ctx: dict, ttl_hours: int = _CONTEXT_TTL_HOURS) -> bool:
    if not ctx:
        return False

    sent_at = ctx.get("sent_at")
    if not sent_at:
        return False

    try:
        dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return now - dt <= timedelta(hours=ttl_hours)
    except Exception as e:
        logger.warning("sent_at判定失敗: %s", e)
        return False


def _collect_context_tokens(payload: dict) -> List[str]:
    tokens: List[str] = []

    def add_text(s: str):
        s = (s or "").strip()
        if not s:
            return

        tokens.append(s)

        parts = re.split(r"[、。・,\s/\-\[\]（）()「」『』:：\n]+", s)
        for p in parts:
            p = p.strip()
            if len(p) >= 3 and p not in _CONTEXT_TOKEN_STOPWORDS:
                tokens.append(p)

    for item in payload.get("news_items", []):
        add_text(item.get("title", ""))
        add_text(item.get("reason", ""))
        add_text(item.get("interpretation", ""))

    seen = set()
    uniq = []
    for t in tokens:
        key = _normalize_text(t)
        if key and key not in seen:
            seen.add(key)
            uniq.append(t)
    return uniq


def parse_article_numbers(text: str, max_n: int = 10) -> List[int]:
    """テキスト中の記事番号を全て抽出して昇順リストで返す。
    「135」→[1,3,5]、「①③⑤」→[1,3,5]、「1と3と5」→[1,3,5] のように処理する。

    【仕様メモ】
    max_n を超える多桁数値（例: max_n=5 のとき「135」）は、
    桁ごとに分解して記事番号の候補として扱う（135 → 1, 3, 5）。
    これは「135」を記事1・3・5の同時指定とみなすUX上の意図的な仕様。
    ロジックを変更する際はこの挙動が崩れないよう注意すること。
    """
    found: set = set()
    # 丸数字を抽出
    for i, ch in enumerate(_CIRCLED[:max_n], 1):
        if ch in text:
            found.add(i)
    # 全角数字→半角に変換
    normalized = text.translate(str.maketrans("１２３４５６７８９０", "1234567890"))
    # 区切り文字（と・、,，／/-）を空白に統一
    normalized = re.sub(r"[と・、,，／/－\-]+", " ", normalized)
    # 数字以外を空白に変換
    normalized = re.sub(r"[^0-9\s]", " ", normalized)
    for token in normalized.split():
        if not token.isdigit():
            continue
        n = int(token)
        if 1 <= n <= max_n:
            found.add(n)
        elif n > max_n:
            # 2桁以上でmax_nを超える数値は桁ごとに分解して記事番号候補として扱う
            # 例: max_n=5, "135" → [1, 3, 5]（記事1・3・5の同時指定とみなす）
            for ch in token:
                d = int(ch)
                if 1 <= d <= max_n:
                    found.add(d)
    return sorted(found)



def is_news_question(text: str) -> bool:
    keywords = [
        "何", "なに", "どういう", "どうな", "意味", "影響",
        "金利", "円安", "株", "経済",
        "AI", "ニュース", "話", "やつ", "って何", "どういうこと",
    ]
    return any(k in text for k in keywords)


def _looks_like_article_reference(text: str) -> bool:
    if parse_article_numbers(text, max_n=10):
        return True

    refs = [
        "このニュース", "そのニュース", "この話", "その話", "この件", "その件",
        "これ", "それ", "さっきの", "今の", "例の",
        "リンク", "url", "記事",
        "詳しく", "なんで", "なぜ", "理由", "影響", "どういうこと",
    ]
    return any(r in text for r in refs)


def _looks_like_question_or_command(text: str) -> bool:
    if parse_article_numbers(text, max_n=10):
        return True
    if is_link_request(text):
        return True
    if any(s in text for s in _QUESTION_SIGNALS):
        return True
    return False


def is_related_to_news_context(user_id: str, text: str) -> bool:
    ctx = get_latest_news_context(user_id)
    if not ctx or not _is_context_alive(ctx):
        return False

    payload = ctx.get("payload", {}) or {}
    norm_text = _normalize_text(text)
    match_text = _normalize_query_for_match(text)

    if not norm_text:
        return False

    if _looks_like_article_reference(text):
        return True

    if not match_text:
        return False

    for token in _collect_context_tokens(payload):
        norm_token = _normalize_text(token)
        if not norm_token:
            continue

        if norm_token in norm_text or norm_text in norm_token:
            return True

        if norm_token in match_text or match_text in norm_token:
            return True

    # コンテキストが生きていて自然文ニュース質問なら一致とみなす
    if is_news_question(text):
        return True

    return False



def parse_detail_request(text: str) -> List[int]:
    """番号系入力を深掘りリクエストとして判定し、記事番号リストを返す。
    半角数字・全角数字・丸数字に対応。
    """
    raw = (text or "").strip()
    if not raw:
        return []

    # 全角→半角変換
    normalized = raw.translate(str.maketrans("１２３４５６７８９０", "1234567890"))

    # 先頭が数字 or 丸数字でなければスルー
    if not re.match(r"^[①-⑩1-9]", normalized):
        return []

    nums = parse_article_numbers(normalized, max_n=10)

    logger.info(
        "parse_detail_request: input=%r normalized=%r → nums=%s",
        text, normalized, nums
    )
    return nums


_DETAIL_NEW_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "detail_articles",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "articles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "index": {"type": "integer"},
                            "headline": {"type": "string"},
                            "p1": {"type": "string"},
                            "p2": {"type": "string"},
                            "p3": {"type": "string"},
                            "stocks": {"type": "string"},
                        },
                        "required": ["index", "headline", "p1", "p2", "p3", "stocks"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["articles"],
            "additionalProperties": False,
        },
    },
}


def answer_detail_new(user_id: str, nums: List[int]) -> str:
    """新仕様の深掘り: 事実+背景+展開の3要素、150〜220文字程度"""
    logger.info("answer_detail_new 実行: user=%s nums=%s", user_id, nums)
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がない\n一度配信を受けてから試して"

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])
    index_map = {item.get("index", 0): item for item in news_items}

    target_items = sorted(
        [index_map[n] for n in nums if n in index_map],
        key=lambda x: x.get("index", 0),
    )
    if not target_items:
        return "指定の番号が見つからなかった"

    news_text = "\n".join(
        f"{n['index']}. 【{n.get('category', '')}】{n['title']}"
        f"（事実: {n.get('reason', '')} / 解釈: {n.get('interpretation', '')}）"
        for n in target_items
    )

    system_prompt = (
        "お前はLINEでニュースを読みやすく解説するやつ\n\n"
        "各記事を以下の構成で書け:\n"
        "headline: 内容の核心を一言で\n"
        "p1: 何が起きているか（要点＋評価）\n"
        "p2: なぜそうなっているか（背景・構造）\n"
        "p3: 今後どうなるか（予測・影響）\n\n"
        "ルール:\n"
        "・各段落2文程度\n"
        "・情報を削るな\n"
        "・言い換えだけは禁止\n"
        "・本文コピペ禁止\n"
        "・敬語禁止\n"
        "・抽象論禁止\n"
        "・具体的に書け\n"
        "・会話誘導禁止\n"
        "・予測は断定せず『〜可能性』『〜になりそう』で書く\n"
        "・全記事について必ず全フィールドを埋めろ（欠損禁止）\n"
        "・1件でも欠けたら不正とみなす\n"
        "stocks: このニュースで影響を受けそうな日本株の銘柄・セクターを具体的に列挙（証券コードがあれば添える）\n"
        "・例: 三菱UFJ(8306)↑、銀行セクター全般↑、REIT売り\n"
        "・直接関係なければ「関連銘柄なし」と書く\n"
    )
    user_prompt = (
        f"以下のニュース記事を深掘りしろ:\n{news_text}\n\n"
        f"指定番号: {nums}\n"
        "全記事について必ず headline / p1 / p2 / p3 / stocks を埋めろ。\n"
        "1件でも欠けたら失敗。\n"
        "JSONで返せ。"
    )

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=900,
            timeout=20,
            response_format=_DETAIL_NEW_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        raw_articles = data.get("articles", [])
        for a in raw_articles:
            logger.info("AI出力 index=%s keys=%s", a.get("index"), list(a.keys()))
        article_map = {a.get("index"): a for a in raw_articles}

        articles = []
        for idx in nums:
            if idx in article_map:
                articles.append(article_map[idx])

        parts = []
        for a in articles:
            idx = a.get("index", 0)
            item = index_map.get(idx)
            title = item.get("title", "") if item else ""
            headline = (a.get("headline") or "").strip()
            p1 = (a.get("p1") or "").strip()
            p2 = (a.get("p2") or "").strip()
            p3 = (a.get("p3") or "").strip()
            stocks = (a.get("stocks") or "").strip()

            if not headline:
                headline = title[:40]
            if not p1:
                p1 = f"{title}が話題になっていて、状況に影響している。単なる一時的な動きではなく流れを変える要因になっている。"
            if not p2:
                p2 = "背景としては最近の動向や周囲の評価が積み上がっていて、全体にも影響が出ている。単体の結果以上に構造的に効いている状態。"
            if not p3:
                p3 = "この流れが続けば影響はさらに広がる可能性があるが、止まれば一気にバランスが崩れるリスクもある。"

            block = (
                f"{idx}. {title}\n\n"
                f"[{headline}]\n\n"
                f"{p1}\n\n"
                f"{p2}\n\n"
                f"{p3}"
            )
            if stocks and stocks != "関連銘柄なし":
                block += f"\n\n📌 {stocks}"
            parts.append(block)

        return "\n\nーーーーー\n\n".join(parts)
    except Exception as e:
        logger.error("answer_detail_new エラー: %s", e)
        return "今ちょっと返せない\nもう一回送ってみて"


def answer_single_news_item(item: dict, question: str, is_detail: bool) -> str:
    news_text = (
        f"{item['index']}. 【{item.get('category', '')}】{item['title']}"
        f"（{item.get('reason', '')} / {item.get('interpretation', '')}）"
    )
    system_prompt = (
        "お前はLINEでニュース解説するやつ\n"
        "・敬語禁止\n"
        "・結論から\n"
        "・短く\n"
        "・会話調\n"
    )
    mode = "詳細で答えろ" if is_detail else "短く答えろ"
    user_prompt = (
        f"ニュース:\n{news_text}\n\n"
        f"質問:\n{question}\n\n"
        f"{item['index']}番の記事として答えろ。番号は変えるな。{mode}"
    )
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=220,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.error("単記事エラー: %s", e)
        return "今ちょっと返せない"


def answer_news_question(user_id: str, question: str) -> tuple:
    # 番号入力は深掘りルートで処理するので、ここには来ない想定
    if parse_detail_request(question):
        logger.info("answer_news_question: 番号入力は対象外 question=%r", question)
        return "番号指定は深掘りルートで処理する", []

    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュース履歴がないから答えられないかも\n一度配信を受けてから聞いてみて", []

    payload = ctx.get("payload", {})
    news_items = payload.get("news_items", [])

    is_detail = any(k in question for k in _DETAIL_KEYWORDS)

    # 自然文：タイトル/reason/interpretationにキーワード一致で最大2件
    norm_q = _normalize_text(question)
    matched = []
    for item in news_items:
        fields = item.get("title", "") + item.get("reason", "") + item.get("interpretation", "")
        if norm_q and any(_normalize_text(tok) in norm_q for tok in re.split(r"[　\s、。・,/\-（）「」\n]+", fields) if len(tok) >= 2):
            matched.append(item)
        if len(matched) >= 2:
            break
    logger.info("自然文一致件数: %s", [n.get("index") for n in matched])
    target_items = matched if matched else (news_items[:1] if news_items else [])
    logger.info("自然文最終対象: %s", [n.get("index") for n in target_items])

    targets = [item["index"] for item in target_items]

    # 複数記事 → 1記事ずつGPTに投げて番号ズレを防ぐ
    if len(target_items) >= 2:
        logger.info("複数記事: %s", targets)
        results = []
        for item in target_items:
            ans = answer_single_news_item(item, question, is_detail)
            idx = item["index"]
            ans = _strip_any_leading_number(ans)
            results.append(f"{idx}. {ans}")
        return "\n\n".join(results), targets

    # 単一記事
    news_text = "\n".join(
        f"{n['index']}. 【{n.get('category', '')}】{n['title']}（{n.get('reason', '')} / {n.get('interpretation', '')}）"
        for n in target_items
    )

    system_prompt = (
        "お前はLINEでニュース解説するやつ\n\n"
        "・敬語禁止\n"
        "・結論から\n"
        "・短く（通常1〜3文）\n"
        "・詳細なら3〜5文\n"
        "・会話調\n"
        "・断定しすぎない（〜かも）\n\n"
        "ニュース文脈に沿って答えろ"
    )
    mode = "詳細モードで答えろ。" if is_detail else "通常モードで答えろ。"
    user_prompt = (
        f"ニュース:\n{news_text}\n\n"
        f"質問:\n{question}\n\n"
        f"{mode}短く答えろ。"
    )

    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.5,
            max_tokens=400,
            timeout=15,
        )
        raw = res.choices[0].message.content.strip()
        return raw, targets
    except Exception as e:
        logger.error("Q&A OpenAI エラー: %s", e)
        return "今ちょっと返答うまくいかない\n\nもう一回送るか\n「使い方」押してみて", []


def _clear_pending(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": None,
            "pending_count": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass



# ─── Webhook ───
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        abort(400)

    body = request.get_data(as_text=True)

    # 署名検証を最初に行う（LINE SDK側で検証）
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        sig_head = (signature or "")[:10]
        logger.warning(
            "署名検証失敗 ip=%s method=%s path=%s ua=%s sig=%s",
            request.remote_addr,
            request.method,
            request.path,
            request.headers.get("User-Agent", ""),
            sig_head,
        )
        abort(400)

    # 署名検証通過後にmembershipイベントを処理
    return "OK"



@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    ensure_user(user_id)
    reply_text(
        event.reply_token,
        "追加ありがとう\n\n"
        "ニュースは朝に届く\n"
        "寡てる時間帯だからミュートでもOK\n\n"
        "配信とは別で、ボタンから追加ニュースも見れるよ\n\n"
        "まずは「使い方」見てみて",
        quick_reply=main_quick_reply(),
    )


_GENRE_WORDS = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}

def normalize_user_text(text: str) -> str:
    """表記ゆれを正規化してキーワードマッチの精度を上げる"""
    t = (text or "").strip()
    t = t.replace("出来る", "できる")
    t = t.replace("出来んの", "できんの")
    t = t.replace("できんの", "できる")
    t = t.replace("なに", "何")
    t = re.sub(r"\s+", "", t)
    return t


_STATUS_WORDS = {"状態", "今どんな感じ", "設定どうなってる", "今の設定"}
_HELP_WORDS = {"聞く", "使い方", "何できる", "どう使うの", "何聞ける"}



_NEWS_TRIGGER_KW = {"ニュース", "ニュースくれ", "最新"}

# 強コマンド — pending を問答無用でスキップ・クリアする
_MAIN_COMMANDS = {
    "ニュース",
    "ニュースくれ",
    "最新",
    "追加ニュース",
    "リンク",
    "相場",
    "急落株",
    "急落",
    "急落銘柄",
    "補助金",
    "助成金",
    "補助金続き",
    "助成金続き",
    "続き",
    "都道府県変更",
    "業種変更",
    "登録",
    "解約",
    "プラン",
    "設定",
}

_STRONG_COMMANDS = (
    _GENRE_WORDS
    | _STATUS_WORDS
    | _HELP_WORDS
    | _NEWS_TRIGGER_KW
    | _MAIN_COMMANDS
)



@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = normalize_user_text(event.message.text)

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)
    user, _ = ensure_user(user_id)

    now_dt = datetime.now(timezone.utc)

    _last = user.get("last_reply_time")
    if _last is not None:
        try:
            _last_dt = datetime.fromisoformat(str(_last).replace("Z", "+00:00"))
            if now_dt - _last_dt < timedelta(seconds=3):
                return
        except Exception:
            pass

    genres = user.get("genres", [])
    qr = main_quick_reply()

    if text in _STRONG_COMMANDS and user.get("pending_action"):
        _clear_pending(user_id)
        user["pending_action"] = None
        user["pending_count"] = None

    if text == "使い方":
        _help_text = (
            "使い方ガイド\n\n"
            "ニュース → 今日のニュースを見る\n"
            "リンク → 直近ニュースのURLを見る\n"
            "補助金 → 今使える制度を探す\n"
            "相場 → 市場の動きを見る\n"
            "急落株 → 急落録柄を見る\n"
            "状態 → 設定確認\n\n"
            "――――――\n\n"
            "[ニュース活用]\n"
            "1詳しく → 記事を深掘り\n"
            "「ｏｏってなに？」 → 用語解説\n"
            "今後どうなる？ → 追加質問OK\n\n"
            "――――――\n\n"
            "[今の状態]\n"
            f"ジャンル：{format_genres(genres) if genres else '全ジャンル'}"
        )
        try:
            with ApiClient(configuration) as api_client:
                api = MessagingApi(api_client)
                _msgs_to_send = [
                    build_genre_flex(genres),
                    TextMessage(text=_help_text),
                ]
                _msgs_to_send[-1].quick_reply = qr
                api.reply_message(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=_msgs_to_send)
                )
        except Exception as e:
            logger.error("使い方返信エラー: %s", e)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _HELP_WORDS:
        reply_text(
            event.reply_token,
            "気になるニュースそのまま聞けばOK\n"
            "「3番目なに？」とかでもいける\n"
            "リンクだけ欲しい時も返せる\n\n"
            "ジャンル変えたい時もそのまま言って",
            quick_reply=qr,
        )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "設定":
        reply_text(event.reply_token, "設定する？\n使い方見てみて", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _GENRE_WORDS:
        reply_flex(event.reply_token, build_genre_flex(genres))
        return

    if text.startswith("ジャンル "):
        raw = text.replace("ジャンル ", "", 1).strip()
        new_genres = normalize_genres(raw)
        if not new_genres:
            reply_text(
                event.reply_token,
                "ジャンル認識できなかった\n例: ジャンル 経済,AI・テック,海外",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        supabase.table("users").update({"genres": new_genres}).eq("user_id", user_id).execute()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, f"{format_genres(new_genres)}に変更した\nニュースで確認できる", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _STATUS_WORDS:
        genre_label = f"ジャンル: {format_genres(genres)}" if genres else "ジャンル: 未設定（全部配信）"
        reply_text(event.reply_token, f"今こんな感じ\n\n{genre_label}", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if _user_subsidy_state.get(user_id) == "await_prefecture":
        _KNOWN_CMDS = {"急落株", "急落", "急落銘柄", "急落録柄", "相場", "補助金", "助成金", "ニュース", "都道府県変更", "業種変更"}
        if text not in _KNOWN_CMDS:
            pref = normalize_prefecture(text)
            if pref:
                _user_subsidy_state.pop(user_id, None)
                try:
                    supabase.table("users").update({"subsidy_prefecture": pref}).eq("user_id", user_id).execute()
                    user["subsidy_prefecture"] = pref
                    logger.info("補助金都道府県保存: user=%s pref=%s", user_id, pref)
                except Exception as e:
                    logger.error("都道府県保存エラー: %s", e)
                try:
                    items = get_subsidy_list(pref, user.get("subsidy_category"))
                    save_last_subsidy_batch(user_id, items, pref, user.get("subsidy_category"))
                    set_subsidy_continue_pending(user_id, user, bool(items))
                    reply_text(event.reply_token, format_subsidy_page(items, pref, user.get("subsidy_category")), quick_reply=qr)
                except Exception as e:
                    logger.error("補助金一覧取得エラー: %s", e)
                    reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
            else:
                reply_text(event.reply_token, "都道府県が認識できませんでした\n例：東京　神奈川　大阪", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        else:
            _user_subsidy_state.pop(user_id, None)

    if _user_subsidy_state.get(user_id) == "await_category" and text in SUBSIDY_CATEGORIES:
        _user_subsidy_state.pop(user_id, None)
        try:
            supabase.table("users").update({"subsidy_category": text}).eq("user_id", user_id).execute()
            user["subsidy_category"] = text
            logger.info("補助金業種保存: user=%s cat=%s", user_id, text)
        except Exception as e:
            logger.error("業種保存エラー: %s", e)
        try:
            items = get_subsidy_list(user.get("subsidy_prefecture"), text)
            save_last_subsidy_batch(user_id, items, user.get("subsidy_prefecture"), text)
            set_subsidy_continue_pending(user_id, user, bool(items))
            reply_text(event.reply_token, format_subsidy_page(items, user.get("subsidy_prefecture"), text), quick_reply=qr)
        except Exception as e:
            logger.error("補助金一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in {"急落株", "急落", "急落銘柄", "急落録柄"}:
        try:
            drops, nikkei_pct, fetched_at, stale_fallback = get_drop_list_for_reply()
            if fetched_at is None:
                reply_text(event.reply_token, "急落株データがまだない\n前場寄り後か後場引け後の更新を待って", quick_reply=qr)
                return
            _user_drop_list[user_id] = drops
            reply_text(event.reply_token, format_drop_list_text(drops, nikkei_pct, fetched_at, stale_fallback), quick_reply=qr)
        except Exception as e:
            logger.error("急落株一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    stock_from_text = _resolve_drop_stock_from_text(user_id, text)
    if stock_from_text:
        try:
            nikkei_pct = get_nikkei_change_pct()
            stock = stock_from_text if stock_from_text.get("price") is not None else get_single_stock_change(stock_from_text["code"])
            if not stock:
                reply_text(event.reply_token, "銀柄データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
                return
            change_pct = stock["change_pct"] if stock else None
            comment = get_stock_ai_comment(stock["code"], stock["name"], change_pct, nikkei_pct)
            company_profile = format_company_profile_text(stock["code"])
            company_block = f"{company_profile}\n\n" if company_profile else ""
            week_pct = stock.get("week_pct")
            month_pct = stock.get("month_pct")
            from_high_pct = stock.get("from_high_pct")
            week_text = f"{week_pct:+.1f}%" if week_pct is not None else "N/A"
            month_text = f"{month_pct:+.1f}%" if month_pct is not None else "N/A"
            from_high_text = f"{from_high_pct:+.1f}%" if from_high_pct is not None else "N/A"
            reply_text(
                event.reply_token,
                f"{stock['code']} {stock['name']}\n"
                f"{company_block}"
                f"取得: {stock.get('fetched_at', '-')}\n\n"
                f"価格   {stock['price']:,.0f}円\n"
                f"前日比 {_format_day_change_text(stock.get('price'), stock.get('day_pct'))}\n"
                f"週次   {week_text}\n"
                f"月次   {month_text}\n"
                f"高値差 {from_high_text}\n\n{comment}",
                quick_reply=qr,
            )
        except Exception as e:
            logger.error("急落株銀柄入力エラー: %s", e)
            reply_text(event.reply_token, "解説取得に失敗した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "相場":
        try:
            content = get_all_markets_reply()
            reply_text(event.reply_token, content, quick_reply=qr)
        except Exception as e:
            logger.error("相場取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in {"補助金", "助成金"}:
        _user_subsidy_state.pop(user_id, None)
        pref = user.get("subsidy_prefecture")
        cat = user.get("subsidy_category")
        logger.info("補助金一覧起動: user=%s pref=%s cat=%s", user_id, pref, cat)
        try:
            items = get_subsidy_list(pref, cat)
            save_last_subsidy_batch(user_id, items, pref, cat)
            set_subsidy_continue_pending(user_id, user, bool(items))
            reply_text(event.reply_token, format_subsidy_page(items, pref, cat), quick_reply=qr)
        except Exception as e:
            logger.error("補助金一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if user.get("subsidy_continue_pending") and text not in {"補助金", "助成金", "補助金続き", "助成金続き", "続き", "都道府県変更", "業種変更"}:
        set_subsidy_continue_pending(user_id, user, False)

    if text in {"補助金続き", "助成金続き"} or (text == "続き" and user.get("subsidy_continue_pending")):
        batch = get_last_subsidy_batch(user_id)
        if not batch or not batch.get("items"):
            reply_text(event.reply_token, "先に「補助金」を押して\n一覧を出してから続きが見れる", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        items = batch.get("items") or []
        offset = int(batch.get("next_offset") or 0)
        pref = batch.get("prefecture") or user.get("subsidy_prefecture")
        cat = batch.get("category") or user.get("subsidy_category")
        if offset >= len(items):
            set_subsidy_continue_pending(user_id, user, False)
            reply_text(event.reply_token, "これで全部見た\n条件を変えるなら都道府県変更 / 業種変更", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        next_offset = min(offset + SUBSIDY_PAGE_SIZE, len(items))
        update_last_subsidy_batch_offset(user_id, next_offset)
        set_subsidy_continue_pending(user_id, user, next_offset < len(items))
        reply_text(event.reply_token, format_subsidy_page(items, pref, cat, offset=offset, page_size=SUBSIDY_PAGE_SIZE), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "都道府県変更":
        _user_subsidy_state[user_id] = "await_prefecture"
        reply_text(event.reply_token, "都道府県を入力してください\n例：東京　神奈川　大阪", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "業種変更":
        _user_subsidy_state[user_id] = "await_category"
        reply_text(event.reply_token, "業種を選んでください", quick_reply=_subsidy_category_quick_reply())
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _NEWS_TRIGGER_KW:
        _user_drop_list.pop(user_id, None)
        _exclude = get_recent_sent_links(user_id, article_limit=30)
        messages, _ = fetch_news_for_reply(user_id, exclude_links=_exclude)
        if not messages:
            reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
        else:
            reply_text(event.reply_token, messages[0], quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if any(w in text for w in _BLOCKLIST):
        reply_text(event.reply_token, _BLOCKLIST_TEXT, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if is_link_request(text):
        batch_items = get_last_news_batch(user_id)
        if batch_items:
            reply_text(event.reply_token, _build_link_message(batch_items), quick_reply=qr)
        else:
            messages, news_filtered = fetch_news_for_reply(user_id)
            if not messages:
                reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
            else:
                reply_text(event.reply_token, messages[0], quick_reply=qr)
                if news_filtered:
                    _link_items = [
                        {"index": i + 1, "title": n.get("title", ""), "link": n.get("link", "")}
                        for i, n in enumerate(news_filtered)
                    ]
                    _push_text(user_id, _build_link_message(_link_items))
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if is_detail_only_request(text):
        _detail_ctx = get_latest_news_context(user_id)
        _reuse = get_reusable_last_news_targets(user, _detail_ctx)
        if _reuse:
            text = "と".join(str(x) for x in _reuse) + "詳しく"
        else:
            reply_text(event.reply_token, "直前の質問が見つからない\n番号で指定してみて（例: 1詳しく）", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

    _nums = parse_detail_request(text)
    if _nums:
        logger.info("深掘り: user=%s nums=%s text=%r", user_id, _nums, text)
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer = answer_detail_new(user_id, _nums)
        reply_text(event.reply_token, answer, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        save_last_news_question_targets(user_id, _nums, get_latest_news_context(user_id))
        return

    logger.info("自然文Q&A: user=%s text=%r", user_id, text)
    _matched_by_ctx = is_related_to_news_context(user_id, text)

    if _matched_by_ctx:
        if not is_news_question(text) and not _looks_like_question_or_command(text):
            reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer, _q_targets = answer_news_question(user_id, text)
        reply_text(event.reply_token, answer, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        if _q_targets:
            save_last_news_question_targets(user_id, _q_targets, get_latest_news_context(user_id))
        return

    reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
    try:
        supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
    except Exception:
        pass


@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    data = event.postback.data

    logger.info("Postback受信: user=%s data=%s", user_id, data)

    user, _ = ensure_user(user_id)
    genres = list(user.get("genres", []) or [])

    if data.startswith("toggle_display_genre:"):
        display = data.split(":", 1)[1]
        internals = DISPLAY_GENRE_MAP.get(display, [])

        was_selected = any(c in genres for c in internals)
        if was_selected:
            genres = [c for c in genres if c not in internals]
        else:
            for cat in internals:
                if cat not in genres:
                    genres.append(cat)

        supabase.table("users").update({"genres": genres}).eq("user_id", user_id).execute()
        # トグル時はニュース取得せず、画面更新のみ
        reply_flex(event.reply_token, build_genre_flex(genres))

    elif data == "confirm_genres":
        # 決定ボタン: 完了メッセージのみ返す（ニュース取得はユーザーが「ニュース」で行う）
        qr = main_quick_reply()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, "ジャンル設定した\nニュースで確認できる", quick_reply=qr)

    elif data == "clear_genres":
        supabase.table("users").update({"genres": []}).eq("user_id", user_id).execute()
        reply_flex(event.reply_token, build_genre_flex([]))


# ─── Web UI ───────────────────────────────────────────────────────────────


def get_signal_badge_label(row: dict) -> str:
    status = row.get("status") or ""
    stage = row.get("signal_stage") or "none"
    if status == "excluded":
        return "除外"
    if status == "entered":
        return "保有中"
    if status == "signal_skipped":
        return "見送り"
    if status == "expired":
        return "期限切れ"
    if status == "ai_dropped":
        return "AI低下"
    if status == "closed":
        return "終了"
    if stage == "strong_confirmed":
        return "強本命"
    if stage == "confirmed":
        return "本命"
    if stage == "early" or status == "rebound_candidate":
        return "候補"
    if status == "notified" or row.get("rebound_notified_at"):
        return "通知済み"
    if status == "watching":
        return "監視中"
    return "シグナルなし"


def _open_virtual_trade_codes() -> set[str]:
    try:
        rows = (
            supabase.table("virtual_trades")
            .select("code")
            .eq("status", "open")
            .is_("sell_date", "null")
            .limit(1000)
            .execute()
            .data or []
        )
        return {str(r.get("code")) for r in rows if r.get("code")}
    except Exception as e:
        logger.warning("open virtual trade codes failed: %s", e)
        return set()


def get_watchlist_counts(rows: list[dict], open_trade_codes: set[str] | None = None) -> dict:
    """ダッシュボード集計。各一覧ページの表示条件と一致させる。"""
    now_utc = datetime.now(timezone.utc)
    open_trade_codes = open_trade_codes or set()

    def _not_expired(row: dict) -> bool:
        value = row.get("signal_expires_at")
        if not value:
            return True
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt > now_utc
        except Exception:
            return True

    def _dedupe_signal_rows(items):
        status_score = {
            "entered": 90,
            "rebound_signal": 80,
            "signal_skipped": 50,
        }

        def _key(row):
            snapshot_id = row.get("feature_snapshot_id")
            if snapshot_id:
                return f"snapshot:{snapshot_id}"
            return f"code-date:{row.get('code')}:{str(row.get('drop_detected_at') or '')[:10]}"

        def _better(row):
            return (
                status_score.get(str(row.get("status") or ""), 0),
                STAGE_RANK.get(row.get("signal_stage"), 0),
                _num(row, "signal_probability", "ai_probability"),
                _num(row, "expected_value"),
                str(row.get("updated_at") or ""),
            )

        by_key = {}
        for row in items:
            key = _key(row)
            current = by_key.get(key)
            if current is None or _better(row) > _better(current):
                by_key[key] = row
        return list(by_key.values())

    watching = [r for r in rows if r.get("status") == "watching"]
    active_signal = [
        r for r in rows
        if r.get("status") == "rebound_signal"
        and r.get("signal_stage") in {"confirmed", "strong_confirmed"}
        and not r.get("is_excluded")
        and not r.get("virtual_trade_id")
        and str(r.get("code") or "") not in open_trade_codes
        and _not_expired(r)
    ]

    notified = [
        r for r in rows
        if r.get("rebound_notified_at") or r.get("status") == "notified"
    ]

    unique_ids = {
        r.get("id") for r in watching + active_signal + notified if r.get("id")
    }

    return {
        "watching": len(watching),
        "candidate": 0,
        "candidate_count": 0,
        "active_signal": len(active_signal),
        "notified": len(notified),
        "total": len(unique_ids),
    }


H5_WATCH_ACTIVE_STATUSES = ("watch", "near_trigger", "pre_signal", "intraday_h5")
H5_WATCH_AI_MIN = 0.60
H5_WATCH_DROP20_MAX = -6.5
H5_WATCH_OVERHEAT_MAX = 2
H5_INTRADAY_AI_MIN = 0.65
H5_INTRADAY_OVERHEAT_MAX = 1


def _h5_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


H5_STORED_FORWARD_DIR = Path("outputs") / "h5_stored_forward_test"
SCREENSHOT_UPLOAD_DIR = Path("uploads") / "h5_screenshots"


def _csv_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _csv_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _read_csv_dicts(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _parse_key_value_report(path: Path) -> dict:
    values: dict = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if ":" not in line or line.lstrip().startswith("-"):
            continue
        key, value = line.split(":", 1)
        key = key.strip().lstrip("#").strip()
        if key:
            values[key] = value.strip()
    return values


def _normalize_h5_stored_row(row: dict) -> dict:
    out = dict(row)
    for key in (
        "signal_probability",
        "drop_from_20d_high_pct",
        "overheat_score",
        "margin_ratio",
        "volume_ratio",
        "emergency_stop_pct",
    ):
        out[key] = _csv_float(out.get(key))
    for key in (
        "score_missing",
        "score_fallback_used",
        "AI_only",
        "drop_only",
        "AI_plus_drop",
        "AI_plus_drop_stage",
        "H5_full",
        "K_no_normal",
        "K_no_normal_plus_no_overheat",
        "peak_pullback_enabled",
        "manual_review_required",
    ):
        out[key] = _csv_bool(out.get(key))
    out["planned_holding_days"] = int(_csv_float(out.get("planned_holding_days")) or 3)
    out["suggested_exit_model"] = out.get("suggested_exit_model") or "HD3_EST12"
    out["manual_review_required"] = True
    out["auto_buy_enabled"] = False
    out["prediction_source"] = out.get("prediction_source") or out.get("source") or ""
    return out


def _is_valid_stored_candidate(row: dict) -> bool:
    return (
        str(row.get("score_source") or "") == "stored_predictions"
        and not _csv_bool(row.get("score_fallback_used"))
    )


def load_latest_h5_stored_candidates(base_dir: Path | None = None) -> dict:
    """Load display-only H5 stored forward-test outputs without recomputing scores."""
    base_dir = base_dir or H5_STORED_FORWARD_DIR
    report_path = base_dir / "latest_stored_forward_test_report.txt"
    h5_candidates_path = base_dir / "latest_h5_candidates.csv"
    h5_full_path = base_dir / "latest_h5_full_candidates.csv"
    k_no_normal_path = base_dir / "latest_k_no_normal_candidates.csv"
    summary_path = base_dir / "forward_test_daily_summary.csv"

    result = {
        "available": False,
        "error": "",
        "summary": {},
        "warnings": [],
        "h5_candidates": [],
        "h5_full_candidates": [],
        "k_no_normal_candidates": [],
        "ai_plus_drop_candidates": [],
        "files": {
            "latest_h5_candidates": str(h5_candidates_path),
            "latest_h5_full_candidates": str(h5_full_path),
            "latest_k_no_normal_candidates": str(k_no_normal_path),
            "latest_report": str(report_path),
        },
    }
    try:
        required = [h5_candidates_path, h5_full_path, k_no_normal_path, report_path]
        missing = [str(p) for p in required if not p.exists()]
        if missing:
            result["error"] = "no stored forward-test latest files"
            result["warnings"].append("Run scripts/run_h5_stored_forward_test.py before opening the UI.")
            result["missing_files"] = missing
            return result

        summary = _parse_key_value_report(report_path)
        daily_rows = _read_csv_dicts(summary_path)
        if daily_rows:
            latest_daily = daily_rows[-1]
            summary.update({
                "trade_date": latest_daily.get("trade_date") or summary.get("trade_date"),
                "model_key": latest_daily.get("model_key") or summary.get("model_key"),
                "model_version": latest_daily.get("model_version") or summary.get("model_version"),
                "score_source": latest_daily.get("score_source") or summary.get("score_source"),
                "saved_predictions_count": latest_daily.get("saved_predictions_count") or summary.get("saved_predictions_count"),
                "loaded_candidates_total": latest_daily.get("loaded_candidates_total") or summary.get("loaded_candidates_total"),
                "AI_only_count": latest_daily.get("AI_only_count") or summary.get("AI_only_count"),
                "AI_plus_drop_count": latest_daily.get("AI_plus_drop_count") or summary.get("AI_plus_drop_count"),
                "H5_full_count": latest_daily.get("H5_full_count") or summary.get("H5_full_count"),
                "K_no_normal_count": latest_daily.get("K_no_normal_count") or summary.get("K_no_normal_count"),
                "fallback_used_count": latest_daily.get("fallback_used_count") or summary.get("fallback_used_count"),
                "missing_prediction_count": latest_daily.get("missing_prediction_count") or summary.get("missing_prediction_count"),
                "active_model_called": latest_daily.get("active_model_called") or summary.get("active_model_predict_proba_called"),
                "result": latest_daily.get("result") or summary.get("result"),
            })

        all_rows = [_normalize_h5_stored_row(row) for row in _read_csv_dicts(h5_candidates_path)]
        full_rows = [_normalize_h5_stored_row(row) for row in _read_csv_dicts(h5_full_path)]
        no_normal_rows = [_normalize_h5_stored_row(row) for row in _read_csv_dicts(k_no_normal_path)]

        valid_all = [row for row in all_rows if _is_valid_stored_candidate(row)]
        result["h5_candidates"] = valid_all
        result["h5_full_candidates"] = [
            row for row in full_rows if _is_valid_stored_candidate(row) and _csv_bool(row.get("H5_full"))
        ]
        result["k_no_normal_candidates"] = [
            row for row in no_normal_rows if _is_valid_stored_candidate(row) and _csv_bool(row.get("K_no_normal"))
        ]
        result["ai_plus_drop_candidates"] = [
            row for row in valid_all if _csv_bool(row.get("AI_plus_drop"))
        ]
        result["summary"] = summary
        result["available"] = True

        if int(_csv_float(summary.get("fallback_used_count")) or 0) > 0:
            result["warnings"].append("fallback was used; stored candidates should not be treated as primary display.")
        if _csv_bool(summary.get("active_model_called")) or _csv_bool(summary.get("active_model_predict_proba_called")):
            result["warnings"].append("active_model was called; stored candidates are invalid for this display.")
    except Exception as e:
        logger.warning("load latest h5 stored candidates failed: %s", e)
        result["error"] = str(e)
    return result


def load_h5_stored_forward_test_context(base_dir: Path | None = None) -> dict:
    """Load full H5 stored forward-test context for the verification page."""
    base = load_latest_h5_stored_candidates(base_dir)
    candidate_log_path = (base_dir or H5_STORED_FORWARD_DIR) / "forward_test_candidate_log.csv"
    daily_summary_path = (base_dir or H5_STORED_FORWARD_DIR) / "forward_test_daily_summary.csv"

    all_rows = base.get("h5_candidates") or []
    h5_rejected = [
        row for row in all_rows
        if _csv_bool(row.get("AI_plus_drop")) and not _csv_bool(row.get("H5_full"))
    ]

    candidate_log = _read_csv_dicts(candidate_log_path)
    daily_summary = _read_csv_dicts(daily_summary_path)

    base["h5_rejected_candidates"] = h5_rejected
    base["candidate_log"] = candidate_log[-50:]
    base["daily_summary"] = daily_summary[-20:]
    return base


def _h5_intraday_static_check(row: dict) -> tuple[bool, str]:
    probability = _h5_float(row.get("signal_probability") or row.get("ai_score"))
    if probability is None or probability < H5_INTRADAY_AI_MIN:
        return False, "ai_below_065"
    if str(row.get("signal_stage") or "") not in {"confirmed", "strong_confirmed"}:
        return False, "stage_not_confirmed"
    if str(row.get("market_regime") or "") == "panic_selloff":
        return False, "panic_selloff"
    overheat = row.get("overheat_score")
    try:
        overheat_score = int(overheat) if overheat is not None else 0
    except Exception:
        overheat_score = 0
    if overheat_score > H5_INTRADAY_OVERHEAT_MAX:
        return False, "overheat_hot"
    margin = _h5_float(row.get("margin_ratio"))
    if margin is not None and (margin < 3 or margin > 30):
        return False, "margin_out_of_range"
    liquidity = _h5_float(row.get("liquidity"))
    if liquidity is not None and liquidity <= 0:
        return False, "liquidity_low"
    return True, "price_triggered_and_static_conditions_passed"


def _h5_watch_status(row: dict, current_price: float | None, trigger_price: float | None) -> tuple[str, float | None, str | None]:
    if current_price is None or trigger_price is None or trigger_price <= 0:
        return "watch", None, None
    if str(row.get("market_regime") or "") == "panic_selloff":
        distance = (current_price / trigger_price - 1) * 100
        return "rejected", distance, "panic_selloff"
    distance = (current_price / trigger_price - 1) * 100
    if distance <= 0:
        intraday_ok, reason = _h5_intraday_static_check(row)
        return ("intraday_h5" if intraday_ok else "pre_signal"), distance, reason
    if distance <= 1.0:
        return "near_trigger", distance, None
    return "watch", distance, None


def _latest_snapshot_trade_date() -> str | None:
    try:
        rows = (
            supabase.table("stock_feature_snapshots")
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        return str(rows[0].get("trade_date")) if rows else None
    except Exception as e:
        logger.warning("[h5_watch] latest snapshot date fetch failed: %s", e)
        return None


def _fetch_all_ranges(table_name: str, *, select: str = "*", page_size: int = 1000, max_rows: int = 5000, **filters) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while offset < max_rows:
        query = supabase.table(table_name).select(select)
        for op, key, value in filters.get("conditions", []):
            if op == "eq":
                query = query.eq(key, value)
            elif op == "gte":
                query = query.gte(key, value)
            elif op == "lt":
                query = query.lt(key, value)
            elif op == "in":
                query = query.in_(key, value)
        page = query.range(offset, offset + page_size - 1).execute().data or []
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def _fetch_h5_watchlist_rows(limit: int = 80) -> list[dict]:
    try:
        rows = (
            supabase.table("h5_watchlist")
            .select("*")
            .in_("watch_status", list(H5_WATCH_ACTIVE_STATUSES))
            .order("watch_date", desc=True)
            .limit(max(limit * 3, 200))
            .execute()
            .data or []
        )
        rows.sort(
            key=lambda r: (
                str(r.get("watch_date") or ""),
                abs(_h5_float(r.get("current_distance_to_trigger_pct")) if r.get("current_distance_to_trigger_pct") is not None else (_h5_float(r.get("distance_to_trigger_pct")) or 9999)),
            ),
            reverse=False,
        )
        latest_date = str(rows[-1].get("watch_date") or "") if rows else ""
        latest_rows = [r for r in rows if str(r.get("watch_date") or "") == latest_date] if latest_date else rows
        latest_rows.sort(
            key=lambda r: abs(
                _h5_float(r.get("current_distance_to_trigger_pct"))
                if r.get("current_distance_to_trigger_pct") is not None
                else (_h5_float(r.get("distance_to_trigger_pct")) or 9999)
            )
        )
        return latest_rows[:limit]
    except Exception as e:
        logger.warning("[h5_watch] fetch failed: %s", e)
        return []


def _build_h5_watchlist() -> dict:
    watch_date = _latest_snapshot_trade_date()
    if not watch_date:
        return {"ok": False, "error": "latest_snapshot_missing", "created": 0, "watch_date": None}

    try:
        watch_day = datetime.fromisoformat(watch_date).date()
    except Exception:
        watch_day = datetime.now(JST).date()
    start_utc = datetime(watch_day.year, watch_day.month, watch_day.day, tzinfo=JST).astimezone(timezone.utc).isoformat()
    end_day = watch_day + timedelta(days=1)
    end_utc = datetime(end_day.year, end_day.month, end_day.day, tzinfo=JST).astimezone(timezone.utc).isoformat()

    snapshots = _fetch_all_ranges(
        "stock_feature_snapshots",
        conditions=[("eq", "trade_date", watch_date)],
        max_rows=4000,
    )
    snapshot_by_code = {str(r.get("code")): r for r in snapshots if r.get("code")}
    if not snapshot_by_code:
        return {"ok": False, "error": "snapshot_rows_missing", "created": 0, "watch_date": watch_date}

    watch_rows = _fetch_all_ranges(
        "stock_drop_watchlist",
        conditions=[("gte", "drop_detected_at", start_utc), ("lt", "drop_detected_at", end_utc)],
        max_rows=4000,
    )
    market_adjustment = _current_market_adjustment()
    candidates: list[dict] = []
    skipped_exact_h5 = 0
    rejected = 0

    for row in watch_rows:
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        snapshot = snapshot_by_code.get(code) or {}
        combined = {**snapshot, **row}
        enriched = _with_ai_priority_stage(combined, market_adjustment)
        probability = _h5_float(enriched.get("signal_probability") or enriched.get("ai_probability") or enriched.get("probability"))
        stage = str(enriched.get("signal_stage") or row.get("signal_stage") or "")
        drop20 = _h5_float(enriched.get("drop_from_20d_high_pct") or snapshot.get("drop_from_20d_high_pct"))
        close_price = _h5_float(enriched.get("close") or snapshot.get("close") or row.get("price_at_drop"))
        margin = _h5_float(enriched.get("margin_ratio") or snapshot.get("margin_ratio"))
        regime = str(enriched.get("market_regime") or market_adjustment.get("regime") or "normal")
        overheat = h5_overheat_score(enriched)

        if probability is None or probability < H5_WATCH_AI_MIN:
            rejected += 1
            continue
        if stage not in {"early", "confirmed", "strong_confirmed"}:
            rejected += 1
            continue
        if drop20 is None or drop20 > H5_WATCH_DROP20_MAX:
            rejected += 1
            continue
        if regime == "panic_selloff":
            rejected += 1
            continue
        if overheat > H5_WATCH_OVERHEAT_MAX:
            rejected += 1
            continue
        if margin is not None and (margin < 3 or margin > 30):
            rejected += 1
            continue
        if close_price is None or close_price <= 0:
            rejected += 1
            continue

        is_h5, _, _ = evaluate_h5_primary_entry({**enriched, "signal_probability": probability, "signal_stage": stage})
        if is_h5:
            skipped_exact_h5 += 1
            continue

        denominator = 1 + (drop20 / 100)
        if denominator <= 0:
            rejected += 1
            continue
        high20 = close_price / denominator
        trigger_price = high20 * 0.92
        distance = (close_price / trigger_price - 1) * 100 if trigger_price > 0 else None
        if trigger_price <= 0 or distance is None:
            rejected += 1
            continue

        candidates.append({
            "watch_date": watch_date,
            "code": code,
            "name": row.get("name") or snapshot.get("name"),
            "ai_score": probability,
            "signal_probability": probability,
            "signal_stage": stage,
            "high_20d": round(high20, 4),
            "close_price": close_price,
            "h5_trigger_price": round(trigger_price, 4),
            "distance_to_trigger_pct": round(distance, 4),
            "drop_from_20d_high_pct": drop20,
            "market_regime": regime,
            "overheat_score": overheat,
            "overheat_bucket": "hot" if overheat >= 2 else ("mild" if overheat == 1 else "cool"),
            "margin_ratio": margin,
            "volume_ratio": _h5_float(enriched.get("volume_ratio_20d") or enriched.get("volume_ratio")),
            "liquidity": _h5_float(enriched.get("turnover_value") or enriched.get("liquidity")),
            "watch_status": "watch",
            "intraday_h5_reason": None,
            "reject_reason": None,
            "memo": "h5_watch_candidate",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

    if candidates:
        try:
            supabase.table("h5_watchlist").upsert(candidates, on_conflict="watch_date,code").execute()
        except Exception as e:
            if "column" not in str(e).lower() and "schema cache" not in str(e).lower():
                raise
            legacy_candidates = [
                {k: v for k, v in row.items() if k not in {"signal_probability", "intraday_h5_reason"}}
                for row in candidates
            ]
            supabase.table("h5_watchlist").upsert(legacy_candidates, on_conflict="watch_date,code").execute()
    return {
        "ok": True,
        "watch_date": watch_date,
        "created": len(candidates),
        "skipped_exact_h5": skipped_exact_h5,
        "rejected": rejected,
    }


def _refresh_h5_watchlist_rows(rows: list[dict], *, force: bool = False) -> dict:
    updated = 0
    failed = 0
    watch_count = 0
    near_trigger = 0
    pre_signal = 0
    intraday_h5 = 0
    rejected = 0
    now_utc = datetime.now(timezone.utc).isoformat()
    for row in rows:
        code = str(row.get("code") or "").strip()
        trigger_price = _h5_float(row.get("h5_trigger_price"))
        if not code or not trigger_price:
            failed += 1
            continue
        quote = get_yfinance_current_price(code, force=force)
        current = _h5_float(quote.get("current_price"))
        if quote.get("status") != "ok" or current is None:
            failed += 1
            try:
                supabase.table("h5_watchlist").update({
                    "memo": f"price_fetch_failed: {quote.get('error') or 'unknown'}",
                    "updated_at": now_utc,
                }).eq("id", row["id"]).execute()
            except Exception:
                logger.warning("[h5_watch] failed memo update code=%s", code)
            continue
        status, distance, intraday_reason = _h5_watch_status(row, current, trigger_price)
        payload = {
            "current_price": current,
            "current_price_yf": current,
            "current_price_source": quote.get("source") or "yfinance",
            "current_price_fetched_at": quote.get("fetched_at") or now_utc,
            "current_distance_to_trigger_pct": round(distance, 4) if distance is not None else None,
            "watch_status": status,
            "intraday_h5_checked_at": now_utc if status in {"pre_signal", "intraday_h5"} else row.get("intraday_h5_checked_at"),
            "intraday_h5_reason": intraday_reason,
            "reject_reason": intraday_reason if status == "rejected" else None,
            "memo": None,
            "updated_at": now_utc,
        }
        try:
            try:
                supabase.table("h5_watchlist").update(payload).eq("id", row["id"]).execute()
            except Exception as e:
                legacy_payload = {
                    k: v for k, v in payload.items()
                    if k not in {"current_price", "intraday_h5_checked_at", "intraday_h5_reason"}
                }
                if "column" not in str(e).lower() and "schema cache" not in str(e).lower():
                    raise
                supabase.table("h5_watchlist").update(legacy_payload).eq("id", row["id"]).execute()
            updated += 1
            if status == "watch":
                watch_count += 1
            elif status == "near_trigger":
                near_trigger += 1
            elif status == "pre_signal":
                pre_signal += 1
            elif status == "intraday_h5":
                intraday_h5 += 1
            elif status == "rejected":
                rejected += 1
        except Exception as e:
            failed += 1
            logger.warning("[h5_watch] update failed code=%s error=%s", code, e)
    return {
        "updated": updated,
        "failed": failed,
        "watch": watch_count,
        "near_trigger": near_trigger,
        "pre_signal": pre_signal,
        "intraday_h5": intraday_h5,
        "rejected": rejected,
    }


def _price_for_position_sizing(row: dict | None) -> float | None:
    if not row:
        return None
    for key in (
        "expected_entry_price",
        "entry_price",
        "buy_price",
        "signal_price",
        "latest_price",
        "current_price",
        "current_price_yf",
        "close",
        "price_at_drop",
    ):
        try:
            value = row.get(key)
            if value is not None and float(value) > 0:
                return float(value)
        except Exception:
            continue
    return None


def _decorate_position_sizing(row: dict, *sources: dict | None) -> dict:
    merged: dict = {}
    for source in sources:
        if source:
            merged.update({k: v for k, v in source.items() if v not in (None, "")})
    merged.update({k: v for k, v in row.items() if v not in (None, "")})
    sizing = calculate_virtual_position_size(_price_for_position_sizing(merged))
    row.update(sizing)
    return row


@app.route("/web/")
@app.route("/web/dashboard")
@app.route("/lab/rebound")
@app.route("/lab/rebound/dashboard")
def web_dashboard():
    market_adjustment = _current_market_adjustment()
    long_term_market = _current_long_term_market_regime()
    h5_environment = build_h5_environment_snapshot()
    cfg = _settings_loader.get_settings()
    entry_mode_context = resolve_entry_mode(cfg, market_adjustment, long_term_market)
    entry_mode_context["scores"] = regime_scores(market_adjustment)
    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0
    now_utc = datetime.now(timezone.utc)

    def _not_expired(row: dict) -> bool:
        value = row.get("signal_expires_at")
        if not value:
            return True
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt > now_utc
        except Exception:
            return True

    def _dedupe_signal_rows(items):
        status_score = {
            "entered": 90,
            "rebound_signal": 80,
            "signal_skipped": 50,
        }

        def _key(row):
            snapshot_id = row.get("feature_snapshot_id")
            if snapshot_id:
                return f"snapshot:{snapshot_id}"
            return f"code-date:{row.get('code')}:{str(row.get('drop_detected_at') or '')[:10]}"

        def _better(row):
            return (
                status_score.get(str(row.get("status") or ""), 0),
                STAGE_RANK.get(row.get("signal_stage"), 0),
                _num(row, "signal_probability", "ai_probability"),
                _num(row, "expected_value"),
                str(row.get("updated_at") or ""),
            )

        by_key = {}
        for row in items:
            key = _key(row)
            current = by_key.get(key)
            if current is None or _better(row) > _better(current):
                by_key[key] = row
        return list(by_key.values())

    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .neq("status", "closed")
            .order("drop_pct", desc=False)
            .limit(500)
            .execute()
            .data or []
        )
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
    except Exception as e:
        logger.error("dashboard error: %s", e)
        rows = []
    holding_count = 0
    open_trade_codes: set[str] = set()
    try:
        open_trade_rows = (
            supabase.table("virtual_trades")
            .select("id,code", count="exact")
            .eq("status", "open")
            .is_("sell_date", "null")
            .limit(1000)
            .execute()
        )
        holding_count = int(open_trade_rows.count or 0)
        open_trade_codes = {str(r.get("code")) for r in (open_trade_rows.data or []) if r.get("code")}
    except Exception as e:
        logger.warning("holding count failed: %s", e)
    signal_rows = [
        r for r in rows
        if r.get("status") == "rebound_signal"
        and r.get("signal_stage") in {"confirmed", "strong_confirmed"}
        and not r.get("is_excluded")
        and not r.get("virtual_trade_id")
        and str(r.get("code") or "") not in open_trade_codes
        and _not_expired(r)
    ]
    signal_rows.sort(
        key=lambda r: (
            STAGE_RANK.get(r.get("signal_stage"), 0),
            _num(r, "signal_probability", "ai_probability"),
            _num(r, "expected_value"),
            _num(r, "signal_score", "rebound_score", "score"),
        ),
        reverse=True,
    )
    for r in signal_rows:
        _decorate_position_sizing(r)
    _with_rebound_diagnostics(signal_rows, market_adjustment, cfg)
    watching_rows = [r for r in rows if r.get("status") == "watching"]
    for r in watching_rows:
        _decorate_position_sizing(r)
    stats = get_watchlist_counts(rows, open_trade_codes)
    stats["holding"] = holding_count

    h5_watch_rows: list[dict] = []
    h5_watch_stats = {"total": 0, "watch": 0, "near_trigger": 0, "pre_signal": 0, "intraday_h5": 0}
    try:
        h5_watch_rows = _fetch_h5_watchlist_rows(limit=80)
        h5_watch_stats = {
            "total": len(h5_watch_rows),
            "watch": sum(1 for r in h5_watch_rows if r.get("watch_status") == "watch"),
            "near_trigger": sum(1 for r in h5_watch_rows if r.get("watch_status") == "near_trigger"),
            "pre_signal": sum(1 for r in h5_watch_rows if r.get("watch_status") == "pre_signal"),
            "intraday_h5": sum(1 for r in h5_watch_rows if r.get("watch_status") == "intraday_h5"),
        }
    except Exception as e:
        logger.warning("h5 watchlist fetch failed: %s", e)

    # H5 open positions
    h5_open_trades: list[dict] = []
    try:
        h5_open_raw = (
            supabase.table("virtual_trades")
            .select("id,code,name,buy_price,buy_date,peak_price,current_price,unrealized_pnl_pct,case_key,is_primary_h5,is_live_candidate,is_h5_research,is_h5_live_limited,selected_rank,entry_probability,live_allocation_bucket,allocation_rank,live_skip_reason,live_case_key,position_limit_mode")
            .eq("status", "open")
            .is_("sell_date", "null")
            .order("buy_date", desc=True)
            .limit(100)
            .execute()
            .data or []
        )
        h5_open_trades = [
            t for t in h5_open_raw
            if str(t.get("case_key") or "") in H5_ACTIVE_CASE_KEYS
            or str(t.get("live_case_key") or "") in H5_ACTIVE_CASE_KEYS
            or str(t.get("case_key") or "") == "H5_short_pullback_drop5_m3"
            or str(t.get("live_case_key") or "") == "H5_short_pullback_drop5_m3"
            or bool(t.get("is_h5_research"))
            or bool(t.get("is_h5_live_limited"))
            or bool(t.get("is_live_candidate"))
        ][:20]
        for t in h5_open_trades:
            _decorate_position_sizing(t)
    except Exception as e:
        logger.warning("h5 open trades fetch failed: %s", e)

    # Today's H5 evaluation log (signals evaluated today, any h5 result)
    h5_today_evals: list[dict] = []
    try:
        _jst_today = datetime.now(JST).date()
        _today_start_utc = datetime(_jst_today.year, _jst_today.month, _jst_today.day, tzinfo=JST).astimezone(timezone.utc).isoformat()
        h5_today_evals = (
            supabase.table("stock_drop_watchlist")
            .select("code,name,h5_primary_match,h5_skip_reason,h5_overheat_score,signal_probability,drop_detected_at,is_live_candidate,selected_rank,live_allocation_bucket,allocation_rank,live_skip_reason,signal_price,price_at_drop,current_price")
            .not_.is_("h5_case_key", "null")
            .gte("last_signal_at", _today_start_utc)
            .order("h5_primary_match", desc=True)
            .order("signal_probability", desc=True)
            .limit(30)
            .execute()
            .data or []
        )
        attach_environment_to_rows(h5_today_evals, h5_environment)
        for r in h5_today_evals:
            _decorate_position_sizing(r)
    except Exception as e:
        logger.warning("h5 today evals fetch failed: %s", e)

    execution_reviews: list[dict] = []
    actual_trade_logs: list[dict] = []
    try:
        execution_reviews = (
            supabase.table("trade_execution_reviews")
            .select("*")
            .order("created_at", desc=True)
            .limit(20)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("execution reviews fetch failed: %s", e)
    try:
        actual_trade_logs = (
            supabase.table("actual_trade_logs")
            .select("*")
            .order("created_at", desc=True)
            .limit(30)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("actual trade logs fetch failed: %s", e)

    # separate holdings (no exit recorded yet)
    actual_holdings = [
        a for a in actual_trade_logs
        if a.get("actual_exit_status") in (None, "holding")
        and not a.get("actual_exit_date")
        and a.get("actual_entry_price")
    ]
    today_jst_str = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")

    # AI diary (latest rebound_ai_daily log)
    ai_diary: dict = {}
    try:
        diary_rows = (
            supabase.table("research_import_logs")
            .select("finished_at,params,status")
            .eq("job_type", "rebound_ai_daily")
            .order("finished_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if diary_rows:
            row = diary_rows[0]
            params = row.get("params") or {}
            ai_diary = {
                "date": (params.get("latest_feature_date") or str(row.get("finished_at") or ""))[:10],
                "text": params.get("ai_summary") or "",
                "status": row.get("status"),
            }
    except Exception as e:
        logger.warning("ai diary fetch failed: %s", e)

    return render_template("web/dashboard.html",
        rows=rows,
        signal_rows=signal_rows,
        watching_rows=watching_rows,
        stats=stats,
        market_adjustment=market_adjustment,
        long_term_market=long_term_market,
        entry_mode_context=entry_mode_context,
        h5_open_trades=h5_open_trades,
        h5_today_evals=h5_today_evals,
        h5_watch_rows=h5_watch_rows,
        h5_watch_stats=h5_watch_stats,
        execution_reviews=execution_reviews,
        actual_trade_logs=actual_trade_logs,
        actual_holdings=actual_holdings,
        today_jst_str=today_jst_str,
        ai_diary=ai_diary,
        h5_environment=h5_environment,
    )


BOX_SETTINGS_DEFAULTS = {
    "entry_mode": "normal",
    "box_width_pct": 12.0,
    "signal_box_position_pct": 45.0,
    "max_pending_days": 5,
    "atr_max_pct": 4.0,
    "gu_skip_pct": 3.0,
    "gd_skip_pct": 5.0,
    "max_open_positions": 5,
    "max_sector_positions": 2,
    "min_turnover_value": 1000000000,
    "min_price": 1000,
    "min_equity_ratio": 30,
    "max_per": 40,
    "max_pbr": 5,
    "note": "",
}


def _box_load_settings() -> tuple[dict, bool, str | None]:
    try:
        rows = (
            supabase.table("box_settings")
            .select("*")
            .eq("user_id", "global")
            .limit(1)
            .execute()
            .data or []
        )
        return {**BOX_SETTINGS_DEFAULTS, **(rows[0] if rows else {})}, True, None
    except Exception as e:
        logger.warning("box_settings load failed: %s", e)
        return dict(BOX_SETTINGS_DEFAULTS), False, str(e)


def _box_fetch_rows(table: str, query_fn=None) -> tuple[list[dict], bool, str | None]:
    try:
        query = supabase.table(table).select("*")
        if query_fn:
            query = query_fn(query)
        return query.execute().data or [], True, None
    except Exception as e:
        logger.warning("%s load failed: %s", table, e)
        return [], False, str(e)


def _with_nikkei_link(rows: list[dict]) -> list[dict]:
    """Add display-only Nikkei correlation values without affecting strategy logic."""
    try:
        return decorate_nikkei_correlation(supabase, rows)
    except Exception as e:
        logger.warning("nikkei correlation display lookup failed: %s", e)
        for row in rows:
            row.setdefault("nikkei_correlation_60d", None)
            row.setdefault("nikkei_link_score", None)
            row.setdefault("nikkei_link_level", "-")
        return rows


def _with_rebound_diagnostics(
    rows: list[dict],
    market_adjustment: dict | None = None,
    settings: dict | None = None,
) -> list[dict]:
    """Add display-only evidence for the existing rebound AI decision path."""
    try:
        return decorate_rebound_diagnostics(
            supabase,
            rows,
            settings or _settings_loader.get_settings(),
            market_adjustment,
        )
    except Exception as e:
        logger.warning("rebound diagnostic display lookup failed: %s", e)
        for row in rows:
            row.setdefault("diagnostic_engine_label", "引け後AIモデル判定")
            row.setdefault("diagnostic_rule_note", "判定詳細を取得できませんでした。")
        return rows


def _box_counts() -> dict:
    rows, ok, _ = _box_fetch_rows("box_signals", lambda q: q.order("trade_date", desc=True).limit(500))
    watch_rows, watch_ok, _ = _box_fetch_rows("box_watchlist", lambda q: q.order("trade_date", desc=True).limit(500))
    trades, trades_ok, _ = _box_fetch_rows("box_virtual_trades", lambda q: q.order("created_at", desc=True).limit(500))
    return {
        "schema_ok": ok and watch_ok and trades_ok,
        "watchlist": len(watch_rows),
        "signals": len(rows),
        "entry_pending": sum(1 for r in rows if r.get("entry_status") == "entry_pending"),
        "generated": sum(1 for r in rows if r.get("entry_status") == "signal_generated"),
        "skipped": sum(1 for r in rows if r.get("entry_status") == "skipped"),
        "open_positions": sum(1 for r in trades if r.get("status") == "open" and not r.get("sell_date")),
        "closed_positions": sum(1 for r in trades if r.get("status") == "closed" or r.get("sell_date")),
    }


@app.route("/lab/box")
@app.route("/lab/box/dashboard")
def web_box_dashboard():
    market_adjustment = _current_market_adjustment()
    long_term_market = _current_long_term_market_regime()
    settings, settings_ok, settings_error = _box_load_settings()
    cfg = _settings_loader.get_settings()
    entry_mode_context = resolve_entry_mode(cfg, market_adjustment, long_term_market)
    entry_mode_context["scores"] = regime_scores(market_adjustment)
    counts = _box_counts()
    latest_signals, signals_ok, signals_error = _box_fetch_rows(
        "box_signals",
        lambda q: q.order("trade_date", desc=True).order("created_at", desc=True).limit(20),
    )
    latest_signals = _with_nikkei_link(latest_signals)
    schema_ok = settings_ok and signals_ok and counts.get("schema_ok", False)
    schema_error = settings_error or signals_error
    return render_template(
        "web/dashboard_box.html",
        market_adjustment=market_adjustment,
        long_term_market=long_term_market,
        entry_mode_context=entry_mode_context,
        box_settings=settings,
        box_counts=counts,
        latest_signals=latest_signals,
        schema_ok=schema_ok,
        schema_error=schema_error,
    )


@app.route("/web/actions/refresh", methods=["POST"])
def web_refresh():
    try:
        from services.market_regime_updater import update_market_regime_for_latest_trade_date

        update_market_regime_for_latest_trade_date(supabase, force=True)
        flash("市場環境を更新しました", "success")
    except Exception as e:
        logger.exception("manual refresh failed")
        flash(f"更新失敗: {e}", "danger")
    return redirect(request.referrer or url_for("web_dashboard"))


def _price_refresh_ticker(code: str, market: str | None = None) -> str:
    code = str(code or "").strip()
    market = str(market or "").strip().lower()
    return code if market == "dow" or code.endswith(".T") else f"{code}.T"


def _fetch_latest_price(code: str, market: str | None = None) -> float | None:
    try:
        import yfinance as yf

        hist = yf.Ticker(_price_refresh_ticker(code, market)).history(period="2d", auto_adjust=True)
        if hist is None or hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning("[refresh_prices] price fetch failed code=%s error=%s", code, e)
        return None


def _h5_price_assist_update_payload(trade: dict, quote: dict, now_utc: str) -> dict:
    fields = build_h5_price_assist_fields(trade, quote)
    return {
        "signal_price": fields.get("signal_price"),
        "entry_limit_2pct": fields.get("entry_limit_2pct"),
        "entry_limit_3pct": fields.get("entry_limit_3pct"),
        "current_price_yf": fields.get("current_price_yf"),
        "current_price_fetched_at": fields.get("current_price_fetched_at"),
        "entry_gap_pct": fields.get("entry_gap_pct"),
        "entry_status": fields.get("entry_status"),
        "entry_status_label": fields.get("entry_status_label"),
        "price_source": fields.get("price_source"),
        "price_fetch_error": fields.get("price_fetch_error"),
        "updated_at": now_utc,
    }


@app.route("/web/actions/refresh-prices", methods=["POST"])
def web_refresh_prices():
    now_utc = datetime.now(timezone.utc).isoformat()
    try:
        open_trades = (
            supabase.table("virtual_trades")
            .select(
                "id,code,market,buy_price,quantity,status,sell_date,"
                "is_live_candidate,live_case_key,case_key,virtual_entry_price"
            )
            .eq("status", "open")
            .is_("sell_date", "null")
            .limit(200)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.exception("[refresh_prices] open trade fetch failed")
        flash(f"株価更新失敗: {e}", "danger")
        return redirect(request.referrer or url_for("web_dashboard"))

    try:
        active_watch = (
            supabase.table("stock_drop_watchlist")
            .select("id,code,market,status,signal_stage")
            .eq("status", "rebound_signal")
            .in_("signal_stage", list(SIGNAL_STAGES))
            .limit(200)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("[refresh_prices] watchlist fetch failed: %s", e)
        active_watch = []

    targets: dict[str, dict] = {}
    for row in open_trades + active_watch:
        code = str(row.get("code") or "").strip()
        if not code or code in targets:
            continue
        targets[code] = {"code": code, "market": row.get("market")}
        if len(targets) >= 50:
            break

    updated_trades = 0
    updated_watch = 0
    updated_h5 = 0
    errors = 0
    for code, meta in targets.items():
        current = _fetch_latest_price(code, meta.get("market"))
        if current is None:
            errors += 1
            continue
        h5_quote: dict | None = None

        try:
            watch_rows = [r for r in active_watch if str(r.get("code")) == code]
            if watch_rows:
                supabase.table("stock_drop_watchlist").update({
                    "current_price": current,
                    "updated_at": now_utc,
                }).eq("code", code).eq("status", "rebound_signal").in_("signal_stage", list(SIGNAL_STAGES)).execute()
                updated_watch += len(watch_rows)
        except Exception as e:
            errors += 1
            logger.warning("[refresh_prices] watchlist update failed code=%s error=%s", code, e)

        for trade in [t for t in open_trades if str(t.get("code")) == code]:
            try:
                buy = float(trade.get("buy_price") or 0)
                qty = int(trade.get("quantity") or 100)
                pnl = (current - buy) * qty if buy > 0 else None
                pnl_pct = (current - buy) / buy * 100 if buy > 0 else None
                update_payload = {
                    "current_price": current,
                    "unrealized_pnl": round(pnl, 0) if pnl is not None else None,
                    "unrealized_pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
                    "updated_at": now_utc,
                }
                if trade.get("is_live_candidate") or trade.get("live_case_key") == H5_LIVE_LIMITED_CASE_KEY:
                    if h5_quote is None:
                        h5_quote = {
                            "code": code,
                            "ticker": _price_refresh_ticker(code, trade.get("market")),
                            "current_price": current,
                            "fetched_at": now_utc,
                            "source": "yfinance",
                            "status": "ok",
                            "error": None,
                        }
                    update_payload.update(_h5_price_assist_update_payload(trade, h5_quote, now_utc))
                    updated_h5 += 1
                supabase.table("virtual_trades").update(update_payload).eq("id", trade["id"]).execute()
                updated_trades += 1
            except Exception as e:
                errors += 1
                logger.warning("[refresh_prices] trade update failed code=%s error=%s", code, e)

    logger.info(
        "[refresh_prices] target_codes=%d updated_watchlist=%d updated_trades=%d updated_h5=%d errors=%d",
        len(targets), updated_watch, updated_trades, updated_h5, errors,
    )
    if errors:
        flash(f"株価更新: {updated_trades}保有 / {updated_watch}監視 / H5補助{updated_h5}件を更新（一部失敗 {errors}）", "warning")
    else:
        flash(f"株価更新: {updated_trades}保有 / {updated_watch}監視 / H5補助{updated_h5}件を更新", "success")
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/web/actions/h5/refresh_price", methods=["POST"])
def web_h5_refresh_price():
    virtual_trade_id = str(
        request.form.get("virtual_trade_id")
        or (request.get_json(silent=True) or {}).get("virtual_trade_id")
        or ""
    ).strip()
    wants_json = request.is_json or "application/json" in str(request.headers.get("Accept") or "")
    if not virtual_trade_id:
        if wants_json:
            return jsonify({"ok": False, "error": "virtual_trade_id_missing"}), 400
        flash("H5現在値更新に失敗しました: virtual_trade_idがありません。", "warning")
        return redirect(request.referrer or url_for("web_trade_assist"))
    try:
        rows = (
            supabase.table("virtual_trades")
            .select("*")
            .eq("id", virtual_trade_id)
            .limit(1)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.exception("[h5_price_refresh] trade fetch failed")
        if wants_json:
            return jsonify({"ok": False, "error": str(e)}), 500
        flash(f"H5現在値更新に失敗しました: {e}", "warning")
        return redirect(request.referrer or url_for("web_trade_assist"))
    trade = rows[0] if rows else None
    if not trade:
        if wants_json:
            return jsonify({"ok": False, "error": "virtual_trade_not_found"}), 404
        flash("H5現在値更新に失敗しました: 対象の仮想売買が見つかりません。", "warning")
        return redirect(request.referrer or url_for("web_trade_assist"))
    if not (trade.get("is_live_candidate") or trade.get("live_case_key") == H5_LIVE_LIMITED_CASE_KEY):
        if wants_json:
            return jsonify({"ok": False, "error": "not_h5_live_limited"}), 400
        flash("H5 Live Limited候補ではないため、補助価格更新は行いませんでした。", "warning")
        return redirect(request.referrer or url_for("web_trade_assist"))

    quote = get_yfinance_current_price(str(trade.get("code") or ""), force=bool(request.form.get("force")))
    now_utc = datetime.now(timezone.utc).isoformat()
    fields = _h5_price_assist_update_payload(trade, quote, now_utc)
    try:
        supabase.table("virtual_trades").update(fields).eq("id", virtual_trade_id).execute()
    except Exception as e:
        logger.exception("[h5_price_refresh] update failed")
        if wants_json:
            return jsonify({"ok": False, "error": str(e), **fields}), 500
        flash(f"H5現在値更新の保存に失敗しました。db/h5_primary_virtual_trades.sql を再実行してください: {e}", "warning")
        return redirect(request.referrer or url_for("web_trade_assist"))

    response = {
        "ok": quote.get("status") == "ok",
        "virtual_trade_id": virtual_trade_id,
        "code": trade.get("code"),
        "name": trade.get("name"),
        **fields,
    }
    if wants_json:
        return jsonify(response)
    status = fields.get("entry_status_label") or "更新しました。"
    flash(f"H5現在値を更新しました: {trade.get('code')} / {status}", "success" if quote.get("status") == "ok" else "warning")
    return redirect(request.referrer or url_for("web_trade_assist"))


@app.route("/web/actions/h5/build_watchlist", methods=["POST"])
def web_h5_build_watchlist():
    try:
        result = _build_h5_watchlist()
    except Exception as e:
        logger.exception("[h5_watch] build failed")
        flash(f"H5 Watchlist作成に失敗しました。db/h5_primary_virtual_trades.sql を再実行してください: {e}", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    if not result.get("ok"):
        flash(f"H5 Watchlist作成に失敗しました: {result.get('error')}", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    flash(
        f"H5 Watchlist作成: {result.get('watch_date')} / 予備軍{result.get('created', 0)}件 "
        f"(確定H5除外{result.get('skipped_exact_h5', 0)}件)",
        "success",
    )
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/web/actions/h5/watchlist/refresh_prices", methods=["POST"])
def web_h5_watchlist_refresh_prices():
    force = bool(request.form.get("force"))
    rows = _fetch_h5_watchlist_rows(limit=200)
    if not rows:
        flash("H5 Watchlistの更新対象がありません。先にWatchlistを作成してください。", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    result = _refresh_h5_watchlist_rows(rows, force=force)
    flash(
        f"H5予備軍 現在値更新: 更新{result['updated']}件 / Near {result['near_trigger']}件 / "
        f"Pre-Signal {result['pre_signal']}件 / Intraday H5 {result['intraday_h5']}件 / "
        f"失敗{result['failed']}件",
        "success" if result["failed"] == 0 else "warning",
    )
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/web/actions/h5/watchlist/refresh_price", methods=["POST"])
def web_h5_watchlist_refresh_price():
    watchlist_id = str(request.form.get("watchlist_id") or "").strip()
    if not watchlist_id:
        flash("H5予備軍の現在値更新に失敗しました: watchlist_idがありません。", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    try:
        rows = (
            supabase.table("h5_watchlist")
            .select("*")
            .eq("id", watchlist_id)
            .limit(1)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.exception("[h5_watch] fetch one failed")
        flash(f"H5予備軍の取得に失敗しました: {e}", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    if not rows:
        flash("H5予備軍が見つかりません。", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    result = _refresh_h5_watchlist_rows(rows, force=bool(request.form.get("force")))
    flash(
        f"H5予備軍 現在値更新: 更新{result['updated']}件 / Near {result['near_trigger']}件 / "
        f"Pre-Signal {result['pre_signal']}件 / Intraday H5 {result['intraday_h5']}件 / "
        f"失敗{result['failed']}件",
        "success" if result["failed"] == 0 else "warning",
    )
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/web/actions/h5/watchlist/recheck_intraday", methods=["POST"])
def web_h5_watchlist_recheck_intraday():
    rows = _fetch_h5_watchlist_rows(limit=200)
    rows = [r for r in rows if r.get("watch_status") in {"watch", "near_trigger", "pre_signal", "intraday_h5"}]
    if not rows:
        flash("Intraday H5再判定の対象がありません。", "warning")
        return redirect(request.referrer or url_for("web_dashboard"))
    result = _refresh_h5_watchlist_rows(rows, force=True)
    flash(
        f"Intraday H5再判定: 更新{result['updated']}件 / Near {result['near_trigger']}件 / "
        f"Pre-Signal {result['pre_signal']}件 / Intraday H5 {result['intraday_h5']}件 / "
        f"失敗{result['failed']}件",
        "success" if result["failed"] == 0 else "warning",
    )
    return redirect(request.referrer or url_for("web_dashboard"))


@app.route("/lab/box/actions/refresh-prices", methods=["POST"])
def web_box_refresh_prices():
    now_utc = datetime.now(timezone.utc).isoformat()
    try:
        open_trades = (
            supabase.table("box_virtual_trades")
            .select("id,code,buy_price,quantity,status,sell_date,exit_date")
            .eq("status", "open")
            .limit(200)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.exception("[box_refresh_prices] open trade fetch failed")
        flash(f"box株価更新失敗: {e}", "danger")
        return redirect(request.referrer or url_for("web_box_positions"))

    updated = 0
    errors = 0
    for trade in open_trades:
        code = str(trade.get("code") or "").strip()
        if not code:
            continue
        current = _fetch_latest_price(code)
        if current is None:
            errors += 1
            continue
        try:
            buy = float(trade.get("buy_price") or 0)
            qty = int(trade.get("quantity") or 100)
            pnl = (current - buy) * qty if buy > 0 else None
            pnl_pct = (current / buy - 1) * 100 if buy > 0 else None
            supabase.table("box_virtual_trades").update({
                "current_price": current,
                "unrealized_pnl": round(pnl, 0) if pnl is not None else None,
                "unrealized_pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
                "updated_at": now_utc,
            }).eq("id", trade["id"]).execute()
            updated += 1
        except Exception as e:
            errors += 1
            logger.warning("[box_refresh_prices] trade update failed code=%s error=%s", code, e)

    logger.info("[box_refresh_prices] open_trades=%d updated=%d errors=%d", len(open_trades), updated, errors)
    if errors:
        flash(f"box株価更新: {updated}件更新（一部失敗 {errors}）", "warning")
    else:
        flash(f"box株価更新: {updated}件更新", "success")
    return redirect(request.referrer or url_for("web_box_positions"))


@app.route("/web/watchlist")
def web_watchlist():
    status_filter = request.args.get("status", "all")
    if status_filter == "rebound_candidate":
        return redirect(url_for("web_watchlist", status="all"))
    market_adjustment = _current_market_adjustment()
    terminal_statuses = {"closed", "expired", "ai_dropped", "signal_skipped", "excluded"}

    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0

    def _row_dt(row: dict) -> datetime:
        for key in ("closed_at", "updated_at", "last_signal_at", "drop_detected_at"):
            value = row.get(key)
            if not value:
                continue
            try:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        return datetime.min.replace(tzinfo=timezone.utc)

    def _status_rank(row: dict) -> int:
        status = row.get("status") or ""
        stage = row.get("signal_stage") or ""
        if status == "entered":
            return 0
        if status == "rebound_signal" and stage == "strong_confirmed":
            return 1
        if status == "rebound_signal":
            return 2
        if status == "rebound_candidate":
            return 3
        if status == "watching":
            return 4
        if status == "closed":
            return 9
        if status in terminal_statuses:
            return 8
        return 7

    def _dedupe_current_rows(items):
        status_score = {
            "entered": 90,
            "rebound_signal": 80,
            "rebound_candidate": 70,
            "watching": 60,
            "signal_skipped": 50,
            "closed": 20,
            "expired": 15,
            "ai_dropped": 10,
            "excluded": 5,
        }

        def _key(row):
            snapshot_id = row.get("feature_snapshot_id")
            if snapshot_id:
                return f"snapshot:{snapshot_id}"
            return f"code-date:{row.get('code')}:{str(row.get('drop_detected_at') or '')[:10]}"

        def _better(row):
            return (
                status_score.get(str(row.get("status") or ""), 0),
                STAGE_RANK.get(row.get("signal_stage"), 0),
                _num(row, "signal_probability", "ai_probability"),
                _num(row, "expected_value"),
                _row_dt(row),
            )

        by_key = {}
        for row in items:
            key = _key(row)
            current = by_key.get(key)
            if current is None or _better(row) > _better(current):
                by_key[key] = row
        return list(by_key.values())

    try:
        q = supabase.table("stock_drop_watchlist").select("*").order("updated_at", desc=True)
        if status_filter != "all":
            q = q.eq("status", status_filter)
        rows = q.limit(1000).execute().data or []
        if status_filter == "all":
            rows = [r for r in rows if r.get("status") != "rebound_candidate"]
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
        rows = _dedupe_current_rows(rows)
        if status_filter == "all":
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            rows = [
                r for r in rows
                if r.get("status") != "rebound_candidate"
                and (r.get("status") not in terminal_statuses or _row_dt(r) >= cutoff)
            ]
        rows.sort(
            key=lambda r: (
                _status_rank(r),
                -_num(r, "signal_probability", "ai_probability"),
                -_num(r, "expected_value"),
                -_num(r, "signal_score", "rebound_score", "score"),
                -_row_dt(r).timestamp(),
            ),
        )
        rows = rows[:200]
    except Exception as e:
        logger.error("watchlist error: %s", e)
        rows = []
    closable_watchlist_statuses = {"watching", "rebound_signal"}
    return render_template(
        "web/watchlist.html",
        rows=rows,
        status_filter=status_filter,
        market_adjustment=market_adjustment,
        closable_watchlist_statuses=closable_watchlist_statuses,
    )


@app.route("/web/watchlist/<item_id>/close", methods=["POST"])
def web_watchlist_close(item_id):
    try:
        now = datetime.now(timezone.utc).isoformat()
        supabase.table("stock_drop_watchlist").update({
            "status": "closed",
            "closed_at": now,
            "close_reason": "manual_watchlist_close",
            "signal_status_reason": "manual_watchlist_close",
            "updated_at": now,
        }).eq("id", item_id).execute()
        flash("クローズした", "success")
    except Exception as e:
        flash(f"エラー: {e}", "danger")
    return redirect(url_for("web_watchlist", status=request.args.get("status", "all")))


def _dedupe_signal_rows(items):
    status_score = {
        "entered": 90,
        "rebound_signal": 80,
        "signal_skipped": 50,
    }

    def _num_value(row, *keys):
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0

    def _key(row):
        snapshot_id = row.get("feature_snapshot_id")
        if snapshot_id:
            return f"snapshot:{snapshot_id}"
        return f"code-date:{row.get('code')}:{str(row.get('drop_detected_at') or '')[:10]}"

    def _better(row):
        return (
            status_score.get(str(row.get("status") or ""), 0),
            STAGE_RANK.get(row.get("signal_stage"), 0),
            _num_value(row, "signal_probability", "ai_probability"),
            _num_value(row, "expected_value"),
            str(row.get("updated_at") or ""),
        )

    by_key = {}
    for row in items:
        key = _key(row)
        current = by_key.get(key)
        if current is None or _better(row) > _better(current):
            by_key[key] = row
    return list(by_key.values())


@app.route("/web/signals")
def web_signals():
    market_adjustment = _current_market_adjustment()
    def _num(row: dict, *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return 0.0
    now_utc = datetime.now(timezone.utc)

    def _not_expired(row: dict) -> bool:
        value = row.get("signal_expires_at")
        if not value:
            return True
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt > now_utc
        except Exception:
            return True

    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .in_("status", ["rebound_signal", "entered", "signal_skipped"])
            .order("last_signal_at", desc=True)
            .limit(500)
            .execute()
            .data or []
        )
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
    except Exception as e:
        logger.error("signals error: %s", e)
        rows = []

    rows = [
        r for r in rows
        if r.get("signal_stage") in {"confirmed", "strong_confirmed"}
        and not r.get("is_excluded")
        and (r.get("status") != "rebound_signal" or _not_expired(r))
    ]
    rows = _dedupe_signal_rows(rows)
    status_rank = {"rebound_signal": 3, "entered": 2, "signal_skipped": 1}
    rows.sort(
        key=lambda r: (
            status_rank.get(str(r.get("status") or ""), 0),
            STAGE_RANK.get(r.get("signal_stage"), 0),
            _num(r, "signal_probability", "ai_probability"),
            _num(r, "expected_value"),
            _num(r, "signal_score", "rebound_score", "score"),
            r.get("last_signal_at") or "",
        ),
        reverse=True,
    )
    rows = _with_nikkei_link(rows)
    rows = _with_rebound_diagnostics(rows, market_adjustment)
    signal_stats = {
        "total": len(rows),
        "active": sum(1 for r in rows if r.get("status") == "rebound_signal"),
        "entered": sum(1 for r in rows if r.get("status") == "entered"),
        "skipped": sum(1 for r in rows if r.get("status") == "signal_skipped"),
    }
    return render_template("web/signals.html", rows=rows, market_adjustment=market_adjustment, signal_stats=signal_stats)


@app.route("/lab")
def lab_select():
    return render_template("web/lab_select.html", market_adjustment=_current_market_adjustment())


def _trade_assist_chart_placeholder(message: str) -> Response:
    safe_message = html.escape(message or "chart unavailable")
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="720" height="260" viewBox="0 0 720 260" role="img">
  <rect width="720" height="260" fill="#0a0c10"/>
  <rect x="1" y="1" width="718" height="258" rx="10" fill="none" stroke="#1f242e"/>
  <text x="360" y="130" fill="#6c7280" font-family="system-ui, sans-serif" font-size="16" text-anchor="middle">{safe_message}</text>
</svg>"""
    return Response(svg, mimetype="image/svg+xml")


def _trade_assist_svg_polyline(points: list[tuple[float, float]], color: str, width: float = 2.0, opacity: float = 1.0) -> str:
    if len(points) < 2:
        return ""
    coords = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round" opacity="{opacity}"/>'


@app.route("/web/trade-assist/chart/<code>.svg")
def web_trade_assist_chart(code):
    code = re.sub(r"[^0-9A-Za-z.]", "", str(code or ""))[:16]
    if not code:
        return _trade_assist_chart_placeholder("code missing")

    def _to_float(value):
        try:
            if value in (None, ""):
                return None
            return float(value)
        except Exception:
            return None

    try:
        rows = (
            supabase.table("stock_feature_snapshots")
            .select("trade_date,close,high,low,ma5,ma25,ma75")
            .eq("code", code)
            .order("trade_date", desc=True)
            .limit(90)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("trade assist chart load failed code=%s: %s", code, e)
        return _trade_assist_chart_placeholder("chart data unavailable")

    rows = list(reversed(rows))
    points_src = []
    for row in rows:
        close = _to_float(row.get("close"))
        if close is None:
            continue
        points_src.append({
            "date": str(row.get("trade_date") or ""),
            "close": close,
            "high": _to_float(row.get("high")) or close,
            "low": _to_float(row.get("low")) or close,
            "ma5": _to_float(row.get("ma5")),
            "ma25": _to_float(row.get("ma25")),
            "ma75": _to_float(row.get("ma75")),
        })

    if len(points_src) < 3:
        return _trade_assist_chart_placeholder("not enough chart data")

    width, height = 720, 260
    left, right, top, bottom = 54, 18, 22, 36
    plot_w = width - left - right
    plot_h = height - top - bottom

    values = []
    for row in points_src:
        values.extend([row["close"], row["high"], row["low"]])
        for key in ("ma5", "ma25", "ma75"):
            if row.get(key) is not None:
                values.append(row[key])
    min_v, max_v = min(values), max(values)
    if max_v <= min_v:
        max_v = min_v + 1
    pad = (max_v - min_v) * 0.08
    min_v -= pad
    max_v += pad

    def _x(i: int) -> float:
        return left + (plot_w * i / max(len(points_src) - 1, 1))

    def _y(value: float) -> float:
        return top + (max_v - value) * plot_h / (max_v - min_v)

    def _series(key: str) -> list[tuple[float, float]]:
        return [(_x(i), _y(row[key])) for i, row in enumerate(points_src) if row.get(key) is not None]

    recent = points_src[-20:] if len(points_src) >= 20 else points_src
    support = min(row["low"] for row in recent)
    resistance = max(row["high"] for row in recent)
    last = points_src[-1]
    first_date = html.escape(points_src[0].get("date") or "")
    last_date = html.escape(last.get("date") or "")
    code_label = html.escape(code)
    close_label = f"{last['close']:,.0f}"
    support_label = f"{support:,.0f}"
    resistance_label = f"{resistance:,.0f}"

    grid_lines = []
    for step in range(5):
        y = top + plot_h * step / 4
        value = max_v - (max_v - min_v) * step / 4
        grid_lines.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#1f242e" stroke-width="1"/>'
            f'<text x="{left-8}" y="{y+4:.1f}" fill="#6c7280" font-family="ui-monospace, monospace" font-size="10" text-anchor="end">{value:,.0f}</text>'
        )

    support_y = _y(support)
    resistance_y = _y(resistance)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="720" height="260" viewBox="0 0 720 260" role="img" aria-label="{code_label} daily chart">
  <rect width="720" height="260" fill="#0a0c10"/>
  <rect x="1" y="1" width="718" height="258" rx="10" fill="none" stroke="#1f242e"/>
  <g>{''.join(grid_lines)}</g>
  <line x1="{left}" y1="{support_y:.1f}" x2="{width-right}" y2="{support_y:.1f}" stroke="#5ee6a8" stroke-width="1.3" stroke-dasharray="5 5" opacity="0.75"/>
  <line x1="{left}" y1="{resistance_y:.1f}" x2="{width-right}" y2="{resistance_y:.1f}" stroke="#ff7a85" stroke-width="1.3" stroke-dasharray="5 5" opacity="0.75"/>
  {_trade_assist_svg_polyline(_series("ma75"), "#7d8597", 1.4, 0.55)}
  {_trade_assist_svg_polyline(_series("ma25"), "#e6c860", 1.6, 0.75)}
  {_trade_assist_svg_polyline(_series("ma5"), "#56b7ff", 1.7, 0.85)}
  {_trade_assist_svg_polyline(_series("close"), "#e6e9ef", 2.4, 1.0)}
  <circle cx="{_x(len(points_src)-1):.1f}" cy="{_y(last['close']):.1f}" r="3.4" fill="#e6e9ef"/>
  <text x="{left}" y="16" fill="#e6e9ef" font-family="system-ui, sans-serif" font-size="13" font-weight="700">{code_label} 日足</text>
  <text x="{width-right}" y="16" fill="#9aa0aa" font-family="ui-monospace, monospace" font-size="11" text-anchor="end">終値 {close_label}円</text>
  <text x="{left}" y="{height-12}" fill="#6c7280" font-family="ui-monospace, monospace" font-size="10">{first_date}</text>
  <text x="{width-right}" y="{height-12}" fill="#6c7280" font-family="ui-monospace, monospace" font-size="10" text-anchor="end">{last_date}</text>
  <text x="{width-right-4}" y="{support_y-5:.1f}" fill="#5ee6a8" font-family="system-ui, sans-serif" font-size="10" text-anchor="end">支持 {support_label}</text>
  <text x="{width-right-4}" y="{resistance_y+12:.1f}" fill="#ff7a85" font-family="system-ui, sans-serif" font-size="10" text-anchor="end">抵抗 {resistance_label}</text>
  <g font-family="system-ui, sans-serif" font-size="10">
    <text x="78" y="238" fill="#e6e9ef">終値</text>
    <text x="122" y="238" fill="#56b7ff">MA5</text>
    <text x="166" y="238" fill="#e6c860">MA25</text>
    <text x="218" y="238" fill="#7d8597">MA75</text>
  </g>
</svg>"""
    return Response(svg, mimetype="image/svg+xml")


def _extract_box_bounce_points(points: list[dict], box_low: float) -> list[dict]:
    """Pick historical touches that actually rebounded after touching the box lower band.

    This is display-only for the box detail chart. It intentionally looks forward up
    to 5 sessions, so it must not be reused as a live signal condition.
    """
    if not points or box_low is None:
        return []
    bounces: list[dict] = []
    last_index = -999
    for i, row in enumerate(points):
        close = row.get("close")
        low = row.get("low", close)
        if close in (None, 0) or low is None:
            continue
        if low > box_low * 1.03:
            continue
        if close < box_low * 0.99:
            continue
        future_window = [p.get("close") for p in points[i + 1 : i + 6] if p.get("close") is not None]
        if not future_window:
            continue
        future_max_close = max(future_window)
        rebound_pct = (future_max_close - close) / close * 100
        if rebound_pct < 3.0:
            continue
        if i - last_index < 5:
            continue
        bounces.append(
            {
                "date": row.get("date"),
                "price": low,
                "rebound_pct": round(rebound_pct, 1),
            }
        )
        last_index = i
    return bounces[-10:]


@app.route("/lab/box/chart/<code>.svg")
def web_box_detail_chart(code):
    code = re.sub(r"[^0-9A-Za-z.]", "", str(code or ""))[:16]
    if not code:
        return _trade_assist_chart_placeholder("code missing")

    def _to_float(value):
        try:
            if value in (None, ""):
                return None
            return float(value)
        except Exception:
            return None

    def _first_float(source: dict, *keys: str):
        for key in keys:
            value = _to_float(source.get(key))
            if value is not None:
                return value
        return None

    context = {k: v for k, v in request.args.items() if v not in (None, "")}
    if not context:
        for table, order in (("box_signals", "trade_date.desc,created_at.desc"), ("box_watchlist", "trade_date.desc,watch_score.desc")):
            try:
                rows = (
                    supabase.table(table)
                    .select("*")
                    .eq("code", code)
                    .order(order.split(",")[0].split(".")[0], desc=True)
                    .limit(1)
                    .execute()
                    .data
                    or []
                )
                if rows:
                    context = rows[0]
                    break
            except Exception as e:
                logger.debug("box detail chart context load failed table=%s code=%s: %s", table, code, e)

    try:
        rows = (
            supabase.table("stock_feature_snapshots")
            .select("trade_date,close,high,low,ma5,ma25,ma75")
            .eq("code", code)
            .order("trade_date", desc=True)
            .limit(120)
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.warning("box detail chart load failed code=%s: %s", code, e)
        return _trade_assist_chart_placeholder("chart data unavailable")

    rows = list(reversed(rows))
    points = []
    for row in rows:
        close = _to_float(row.get("close"))
        if close is None:
            continue
        low = _to_float(row.get("low")) or close
        points.append(
            {
                "date": str(row.get("trade_date") or ""),
                "close": close,
                "low": low,
                "ma5": _to_float(row.get("ma5")) or close,
                "ma25": _to_float(row.get("ma25")) or close,
                "ma75": _to_float(row.get("ma75")) or close,
            }
        )

    if len(points) < 3:
        return _trade_assist_chart_placeholder("not enough chart data")

    recent = points[-60:] if len(points) >= 60 else points
    recent_close = [p["close"] for p in recent]
    latest_close = points[-1]["close"]
    box_high = _first_float(context, "box_high", "box_upper") or max(recent_close)
    box_low = _first_float(context, "box_low", "box_lower") or min(recent_close)
    if box_high <= box_low:
        box_high = max(recent_close)
        box_low = min(recent_close)
    entry_min = _first_float(context, "entry_price_min") or box_low
    entry_max = _first_float(context, "entry_price_max") or (box_low * 1.02)
    current_price = _first_float(context, "current_price", "close") or latest_close
    strategy_type = str(context.get("strategy_type") or "box_pullback")
    support_line = _first_float(context, "support_line")
    bounce_base = support_line if strategy_type == "support_bounce" and support_line else box_low
    bounce_points = _extract_box_bounce_points(points, bounce_base)

    try:
        from box_chart import render_chart

        svg = render_chart(
            code=code,
            name=str(context.get("name") or request.args.get("name") or ""),
            trade_date=[p["date"] for p in points],
            close=[p["close"] for p in points],
            ma5=[p["ma5"] for p in points],
            ma25=[p["ma25"] for p in points],
            ma75=[p["ma75"] for p in points],
            box_high=box_high,
            box_low=box_low,
            entry_min=entry_min,
            entry_max=entry_max,
            current_price=current_price,
            box_position_pct=_first_float(context, "box_position_pct"),
            bounce_count=int(_first_float(context, "bounce_count") or 0) if _first_float(context, "bounce_count") is not None else None,
            bounce_points=bounce_points,
            rsi14=_first_float(context, "rsi14"),
            margin_ratio=_first_float(context, "margin_ratio"),
            box_score=_first_float(context, "box_score", "watch_score", "signal_box_score"),
            stop_loss_price=_first_float(context, "stop_loss_price"),
            take_profit_price=_first_float(context, "take_profit_price") or box_high,
            atr_pct=_first_float(context, "atr_pct"),
            ma5_gap_pct=_first_float(context, "ma5_gap_pct"),
            ma25_gap_pct=_first_float(context, "ma25_gap_pct"),
            ma75_gap_pct=_first_float(context, "ma75_gap_pct"),
            strategy_type=strategy_type,
            support_line=support_line,
            support_zone_low=_first_float(context, "support_zone_low"),
            support_zone_high=_first_float(context, "support_zone_high"),
            support_touch_count=int(_first_float(context, "support_touch_count") or 0)
            if _first_float(context, "support_touch_count") is not None
            else None,
            support_break_count=int(_first_float(context, "support_break_count") or 0)
            if _first_float(context, "support_break_count") is not None
            else None,
            support_distance_pct=_first_float(context, "support_distance_pct"),
            avg_bounce_return_pct=_first_float(context, "avg_bounce_return_pct"),
        )
    except Exception as e:
        logger.warning("box detail chart render failed code=%s: %s", code, e)
        return _trade_assist_chart_placeholder("chart render failed")

    return Response(svg, mimetype="image/svg+xml")


@app.route("/web/trade-assist")
def web_trade_assist():
    market_adjustment = _current_market_adjustment()
    long_term_market = _current_long_term_market_regime()
    h5_environment = build_h5_environment_snapshot()
    now_utc = datetime.now(timezone.utc)
    settings = _settings_loader.get_settings()
    entry_mode_context = resolve_entry_mode(settings, market_adjustment, long_term_market)
    entry_mode_context["scores"] = regime_scores(market_adjustment)
    stop_loss_pct = float(settings.get("virtual_exit_stop_loss_pct") or 4.0)
    h5_exit_display = {
        "case_key": H5_PRIMARY_CASE_KEY,
        "case_label": H5_PRIMARY_DISPLAY_NAME,
        "stop_loss_pct": abs(float(H5_PRIMARY_RULES["initial_sl_pct"])) * 100,
        "holding_days": int(H5_PRIMARY_RULES["max_holding_days"]),
        "entry_execution_note": H5_ENTRY_EXECUTION_NOTE,
    }
    exit_display = {
        "pullback_pct": float(settings.get("virtual_exit_pullback_pct") or 2.0),
        "rsi_level": float(settings.get("virtual_exit_rsi_level") or 75.0),
        "rsi_pullback_pct": float(settings.get("virtual_exit_rsi_pullback_pct") or 1.0),
        "stop_loss_pct": stop_loss_pct,
        "ma5_failure_pct": float(settings.get("virtual_exit_ma5_failure_pct") or 2.0),
        "holding_days": int(settings.get("virtual_exit_holding_days") or 5),
        "extend_high_update_days": int(settings.get("virtual_exit_extend_high_update_days") or 2),
    }

    def _num(row: dict, *keys: str, default: float = 0.0) -> float:
        for key in keys:
            try:
                value = row.get(key)
                if value is not None:
                    return float(value)
            except Exception:
                continue
        return default

    def _not_expired(row: dict) -> bool:
        value = row.get("signal_expires_at")
        if not value:
            return True
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt > now_utc
        except Exception:
            return True

    def _date_text(value) -> str | None:
        if not value:
            return None
        text = str(value)
        return text.split("T", 1)[0][:10]

    def _same_trade_date(value, trade_date: str | None) -> bool:
        left = None
        if value:
            text = str(value)
            if "T" in text:
                try:
                    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    left = dt.astimezone(JST).date().isoformat()
                except Exception:
                    left = _date_text(value)
            else:
                left = _date_text(value)
        right = _date_text(trade_date)
        return bool(left and right and left == right)

    def _is_prior_open_trade(trade: dict | None) -> bool:
        if not trade:
            return False
        if str((trade or {}).get("status") or "") != "open" or (trade or {}).get("sell_date"):
            return False
        return not _same_trade_date((trade or {}).get("buy_date") or (trade or {}).get("created_at"), latest_feature_date)

    def _days_between(start: str | None, end: str | None) -> int | None:
        try:
            if not start or not end:
                return None
            start_dt = datetime.fromisoformat(start[:10])
            end_dt = datetime.fromisoformat(end[:10])
            return (end_dt.date() - start_dt.date()).days
        except Exception:
            return None

    def _recent_margin_from_row(row: dict, ref_date: str | None, max_age_days: int = 60) -> dict:
        margin_date = _date_text(row.get("margin_date"))
        margin_ratio = row.get("margin_ratio")
        if margin_ratio is None or not margin_date:
            return {}
        age = _days_between(margin_date, ref_date)
        if age is not None and 0 <= age <= max_age_days:
            return {"date": margin_date, "margin_ratio": margin_ratio}
        return {}

    latest_log = None
    latest_feature_date = None
    snapshot_count = None
    update_status = "不明"
    latest_trade_entries = []
    try:
        logs = (
            supabase.table("research_import_logs")
            .select("*")
            .eq("job_type", "rebound_ai_daily")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        latest_log = logs[0] if logs else None
        params = (latest_log or {}).get("params") or {}
        latest_feature_date = params.get("latest_feature_date")
        snapshot_count = params.get("feature_snapshots") or (latest_log or {}).get("rows_updated")
        latest_trade_entries = ((params.get("trade_activity") or {}).get("entries") or [])
        update_status = "正常" if (latest_log or {}).get("status") == "completed" else ((latest_log or {}).get("status") or "不明")
    except Exception as e:
        logger.warning("trade assist log load failed: %s", e)

    try:
        if not latest_feature_date:
            latest = (
                supabase.table("stock_feature_snapshots")
                .select("trade_date")
                .order("trade_date", desc=True)
                .limit(1)
                .execute()
                .data or []
            )
            latest_feature_date = latest[0]["trade_date"] if latest else None
        if latest_feature_date and snapshot_count is None:
            snapshot_count = (
                supabase.table("stock_feature_snapshots")
                .select("id", count="exact")
                .eq("trade_date", latest_feature_date)
                .limit(1)
                .execute()
                .count
            )
    except Exception as e:
        logger.warning("trade assist snapshot summary failed: %s", e)

    def _fetch_by_ids(table: str, ids: list[str], select: str = "*") -> dict[str, dict]:
        clean_ids = [str(x) for x in ids if x]
        if not clean_ids:
            return {}
        try:
            data = supabase.table(table).select(select).in_("id", clean_ids).execute().data or []
            return {str(r.get("id")): r for r in data if r.get("id")}
        except Exception as e:
            logger.warning("trade assist %s lookup failed: %s", table, e)
            return {}

    latest_trade_entries = list(latest_trade_entries)
    trade_ids = [str(e.get("id")) for e in latest_trade_entries if e.get("id")]
    latest_trades = _fetch_by_ids("virtual_trades", trade_ids)

    def _is_trade_assist_buy_candidate(trade: dict | None) -> bool:
        if not trade:
            return False
        case_key = str((trade or {}).get("case_key") or "")
        live_case_key = str((trade or {}).get("live_case_key") or "")
        return (
            bool((trade or {}).get("is_live_candidate"))
            or bool((trade or {}).get("is_primary_h5"))
            or bool((trade or {}).get("is_h5_live_limited"))
            or case_key == H5_LIVE_LIMITED_CASE_KEY
            or live_case_key == H5_LIVE_LIMITED_CASE_KEY
            or live_case_key == "H5_short_pullback_drop5_m3"
        )

    try:
        if latest_feature_date:
            live_rows = (
                supabase.table("virtual_trades")
                .select("*")
                .gte("buy_date", f"{latest_feature_date}T00:00:00+09:00")
                .lt("buy_date", f"{latest_feature_date}T23:59:59+09:00")
                .eq("is_live_candidate", True)
                .order("selected_rank", desc=False)
                .limit(50)
                .execute()
                .data or []
            )
            known_trade_ids = set(latest_trades)
            for trade in live_rows:
                trade_id = str(trade.get("id") or "")
                if not trade_id:
                    continue
                latest_trades[trade_id] = trade
                if trade_id not in known_trade_ids:
                    latest_trade_entries.append({
                        "id": trade_id,
                        "code": trade.get("code"),
                        "name": trade.get("name"),
                        "entry_probability": trade.get("entry_probability"),
                        "expected_value": trade.get("expected_value"),
                        "signal_stage": trade.get("signal_stage"),
                    })
    except Exception as e:
        logger.warning("trade assist live candidate lookup failed: %s", e)

    snapshot_ids = [str(t.get("feature_snapshot_id")) for t in latest_trades.values() if t.get("feature_snapshot_id")]
    watchlist_ids = [str(t.get("watchlist_id")) for t in latest_trades.values() if t.get("watchlist_id")]
    snapshots = _fetch_by_ids("stock_feature_snapshots", snapshot_ids)
    watchlists = _fetch_by_ids("stock_drop_watchlist", watchlist_ids)

    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .in_("status", ["rebound_signal", "signal_skipped", "entered"])
            .order("last_signal_at", desc=True)
            .limit(300)
            .execute()
            .data or []
        )
        rows = [_with_ai_priority_stage(r, market_adjustment) for r in rows]
    except Exception as e:
        logger.exception("trade assist load failed")
        flash(f"トレード補助の取得に失敗しました: {e}", "warning")
        rows = []

    candidate_codes = {
        str(e.get("code") or "")
        for e in latest_trade_entries
        if e.get("code")
    } | {
        str(r.get("code") or "")
        for r in rows
        if r.get("code")
    }
    try:
        margin_start_date = None
        if latest_feature_date:
            try:
                margin_start_date = (datetime.fromisoformat(str(latest_feature_date)[:10]) - timedelta(days=60)).date().isoformat()
            except Exception:
                margin_start_date = None
        margin_query = (
            supabase.table("stock_weekly_margin_interest")
            .select("code,date,margin_ratio")
            .in_("code", list(candidate_codes))
            .order("date", desc=True)
            .limit(1000)
        )
        if latest_feature_date:
            margin_query = margin_query.lte("date", latest_feature_date)
        if margin_start_date:
            margin_query = margin_query.gte("date", margin_start_date)
        margin_rows = margin_query.execute().data or [] if candidate_codes else []
        margin_by_code = {}
        for margin_row in margin_rows:
            code = str(margin_row.get("code") or "")
            if code and code not in margin_by_code:
                margin_by_code[code] = margin_row
    except Exception as e:
        logger.warning("trade assist margin lookup failed: %s", e)
        margin_by_code = {}

    try:
        profile_rows = (
            supabase.table("nikkei_company_profiles")
            .select("code,name,sector,business_summary")
            .in_("code", list(candidate_codes))
            .execute()
            .data or []
        ) if candidate_codes else []
        company_profiles = {str(p.get("code")): p for p in profile_rows if p.get("code")}
    except Exception as e:
        logger.warning("trade assist company profile lookup failed: %s", e)
        company_profiles = {}

    try:
        comment_rows = (
            supabase.table("nikkei_ai_comment_cache")
            .select("code,comment,updated_at,expires_at")
            .in_("code", list(candidate_codes))
            .gte("expires_at", now_utc.isoformat())
            .order("updated_at", desc=True)
            .limit(200)
            .execute()
            .data or []
        ) if candidate_codes else []
        drop_comments = {}
        for comment_row in comment_rows:
            code = str(comment_row.get("code") or "")
            if code and code not in drop_comments:
                drop_comments[code] = str(comment_row.get("comment") or "").strip()
    except Exception as e:
        logger.warning("trade assist drop comment lookup failed: %s", e)
        drop_comments = {}
    h5_ai_reason_cache = load_reason_cache()

    def _merge_card_sources(*sources: dict | None) -> dict:
        merged = {}
        for source in sources:
            if not source:
                continue
            for key, value in source.items():
                if value is not None and merged.get(key) is None:
                    merged[key] = value
        return merged

    def _build_card(base: dict, *, trade: dict | None = None, source_label: str = "") -> dict:
        row = _with_ai_priority_stage(dict(base), market_adjustment)
        code = str(row.get("code") or "")
        row["trade_date"] = row.get("trade_date") or latest_feature_date
        profile = company_profiles.get(str(row.get("code") or "")) or {}
        entry_price = _num(trade or {}, "buy_price", default=0.0) or _num(row, "price_at_drop", "close", default=0.0)
        stop_price = entry_price * (1.0 - stop_loss_pct / 100.0) if entry_price > 0 else None
        ma5 = _num(row, "ma5", default=0.0)
        risk_100 = (entry_price - stop_price) * 100 if stop_price is not None else None
        profile_sector = str(profile.get("sector") or "").strip()
        business_summary = str(profile.get("business_summary") or "").strip()
        drop_comment = drop_comments.get(code) or ""
        if not drop_comment:
            reason_bits = []
            drop_pct = _num(row, "drop_pct", default=0.0)
            if drop_pct:
                reason_bits.append(f"急落率 {drop_pct:.1f}%")
            regime_reason = str(row.get("market_regime_reason") or "").strip()
            if regime_reason:
                reason_bits.append(f"地合い: {regime_reason}")
            bad_news_score = _num(row, "bad_news_score", default=0.0)
            if bad_news_score > 0:
                reason_bits.append(f"悪材料スコア {bad_news_score:.0f}")
            sector_risk_score = _num(row, "sector_risk_score", default=0.0)
            if sector_risk_score > 0:
                reason_bits.append(f"セクターリスク {sector_risk_score:.0f}")
            drop_comment = " / ".join(reason_bits) if reason_bits else "急落理由コメントは未生成です。"
        is_open_virtual_trade = _is_prior_open_trade(trade)
        row["is_existing_virtual_trade"] = bool(trade)
        row["is_open_virtual_trade"] = is_open_virtual_trade
        if is_open_virtual_trade:
            row["display_status"] = "LIVE保有中"
        else:
            row["display_status"] = "強本命" if row.get("signal_stage") == "strong_confirmed" else "翌日購入候補"
        row["sector"] = row.get("sector") or profile_sector
        row["company_summary"] = business_summary
        row["company_profile_status"] = "registered" if business_summary else ("sector_only" if row.get("sector") else "missing")
        row["drop_reason_comment"] = drop_comment
        row["drop_reason_source"] = "AIキャッシュ" if code in drop_comments else "指標メモ"
        row["entry_price"] = entry_price if entry_price > 0 else None
        _decorate_position_sizing(row, trade, {"expected_entry_price": row.get("expected_entry_price") or row.get("entry_price")})
        row["gu_3_price"] = entry_price * 1.03 if entry_price > 0 else None
        row["gu_5_price"] = entry_price * 1.05 if entry_price > 0 else None
        row["gd_3_price"] = entry_price * 0.97 if entry_price > 0 else None
        row["ma5_gd_price"] = ma5 * 0.98 if ma5 > 0 else None
        row["stop_loss_price"] = stop_price
        row["risk_100"] = risk_100
        row["stop_loss_pct"] = stop_loss_pct
        row["source_label"] = source_label
        entry_probability = _num(trade or {}, "entry_probability", default=None) if trade else _num(row, "entry_probability", default=None)
        signal_probability = _num(row, "signal_probability", "ai_probability", default=None)
        row["display_probability"] = entry_probability if entry_probability is not None else signal_probability
        row["ai_score_label"] = "判定時AIスコア" if entry_probability is not None else "現在AIスコア"
        row["entry_case"] = row.get("entry_case") or classify_entry_case(row)
        row["entry_ma5_gap_pct"] = (
            row.get("entry_ma5_gap_pct")
            if row.get("entry_ma5_gap_pct") is not None
            else ma_gap_pct(row, "ma5")
        )
        row["entry_ma25_gap_pct"] = (
            row.get("entry_ma25_gap_pct")
            if row.get("entry_ma25_gap_pct") is not None
            else ma_gap_pct(row, "ma25")
        )
        row["entry_ma75_gap_pct"] = (
            row.get("entry_ma75_gap_pct")
            if row.get("entry_ma75_gap_pct") is not None
            else ma_gap_pct(row, "ma75")
        )
        margin = margin_by_code.get(code) or _recent_margin_from_row(row, latest_feature_date)
        row["margin_ratio"] = margin.get("margin_ratio")
        row["margin_date"] = margin.get("date")
        row["entry_mode_used"] = row.get("entry_mode_used") or entry_mode_context.get("effective")
        row["recommended_entry_mode"] = row.get("recommended_entry_mode") or entry_mode_context.get("recommended")
        row["entry_mode_label"] = ENTRY_MODE_LABELS.get(str(row.get("entry_mode_used") or ""), row.get("entry_mode_used") or "-")
        row["recommended_entry_mode_label"] = ENTRY_MODE_LABELS.get(
            str(row.get("recommended_entry_mode") or ""),
            row.get("recommended_entry_mode") or "-",
        )
        h5_input = {
            **row,
            "signal_probability": row.get("display_probability") or row.get("signal_probability"),
        }
        h5_passed, h5_reasons, h5_meta = evaluate_h5_primary_entry(h5_input)
        persisted_case_key = str((trade or {}).get("case_key") or row.get("case_key") or row.get("live_case_key") or "")
        is_short_pullback_case = persisted_case_key == "H5_short_pullback_drop5_m3"
        persisted_h5 = bool((trade or {}).get("is_primary_h5")) or persisted_case_key in H5_ACTIVE_CASE_KEYS or is_short_pullback_case
        # An already-created legacy virtual trade must not be relabeled as H5,
        # because its stored exit rule remains the legacy one.
        row["h5_primary_match"] = persisted_h5 if trade else h5_passed
        row["h5_case_key"] = persisted_case_key if is_short_pullback_case else H5_PRIMARY_CASE_KEY
        row["h5_case_label"] = "H5短期押し目: drop5 -3%" if is_short_pullback_case else H5_PRIMARY_DISPLAY_NAME
        row["model_key"] = row.get("model_key") or H5_SHAP_DEFAULT_MODEL_KEY
        row["position_limit_mode"] = (trade or row).get("position_limit_mode") or ("live_limited" if row.get("is_live_candidate") else "research")
        row["is_live_candidate"] = bool((trade or row).get("is_live_candidate"))
        row["is_h5_research"] = bool((trade or row).get("is_h5_research")) or (persisted_case_key == H5_RESEARCH_CASE_KEY)
        row["is_h5_live_limited"] = bool((trade or row).get("is_h5_live_limited")) or (persisted_case_key == H5_LIVE_LIMITED_CASE_KEY)
        row["selected_rank"] = (trade or row).get("selected_rank")
        row["live_allocation_bucket"] = (trade or row).get("live_allocation_bucket")
        row["allocation_rank"] = (trade or row).get("allocation_rank")
        row["live_skip_reason"] = (trade or row).get("live_skip_reason")
        row["h5_candidate_count"] = (trade or row).get("h5_candidate_count")
        row["h5_selected_count"] = (trade or row).get("h5_selected_count")
        row["h5_skip_reason"] = None if row["h5_primary_match"] or trade else " / ".join(h5_reasons)
        row["h5_overheat_score"] = h5_meta.get("entry_overheat_score")
        cached_reasons = get_cached_reasons(row, h5_ai_reason_cache)
        if cached_reasons:
            for reason_key in (
                "h5_reason_comment",
                "h5_reason_source",
                "h5_reason_generated_at",
                "ai_score_reason_comment",
                "ai_score_reason_source",
                "ai_score_reason_generated_at",
                "risk_reason_comment",
                "risk_reason_source",
                "risk_reason_generated_at",
            ):
                row[reason_key] = cached_reasons.get(reason_key)
        cached_shap = load_shap_cache(
            code,
            str(row.get("trade_date") or latest_feature_date or "")[:10],
            str(row.get("model_key") or H5_SHAP_DEFAULT_MODEL_KEY),
            str(row.get("model_version") or ""),
        )
        if cached_shap:
            shap_reason = build_shap_reason_comment(cached_shap)
            row["shap_reason_comment"] = cached_shap.get("shap_reason_comment") or shap_reason.get("shap_reason_comment")
            row["shap_reason_source"] = cached_shap.get("shap_reason_source") or shap_reason.get("shap_reason_source")
            row["shap_generated_at"] = cached_shap.get("shap_generated_at") or shap_reason.get("shap_generated_at")
            row["shap_positive_contributions"] = cached_shap.get("positive_contributions") or []
            row["shap_negative_contributions"] = cached_shap.get("negative_contributions") or []
            row["shap_warnings"] = cached_shap.get("warnings") or []
        if row["h5_primary_match"]:
            row["stop_loss_pct"] = h5_exit_display["stop_loss_pct"]
            row["stop_loss_price"] = entry_price * (1.0 - row["stop_loss_pct"] / 100.0) if entry_price > 0 else None
            row["risk_100"] = (entry_price - row["stop_loss_price"]) * 100 if row["stop_loss_price"] is not None else None
        if trade:
            row["virtual_trade_id"] = trade.get("id") or row.get("virtual_trade_id")
            row["trade_created_at"] = trade.get("created_at")
        return row

    cards = []
    seen_codes = set()
    for entry in latest_trade_entries:
        trade = latest_trades.get(str(entry.get("id"))) or entry
        if not _is_trade_assist_buy_candidate(trade):
            continue
        if _is_prior_open_trade(trade):
            continue
        snapshot = snapshots.get(str(trade.get("feature_snapshot_id")))
        watchlist = watchlists.get(str(trade.get("watchlist_id")))
        base = _merge_card_sources(
            watchlist,
            snapshot,
            trade,
            entry,
            {
                "code": entry.get("code") or trade.get("code"),
                "name": entry.get("name") or trade.get("name"),
                "entry_probability": trade.get("entry_probability") or entry.get("entry_probability"),
                "signal_probability": trade.get("entry_probability") or entry.get("entry_probability"),
                "expected_value": trade.get("expected_value") or entry.get("expected_value"),
                "signal_stage": trade.get("signal_stage") or entry.get("signal_stage"),
            },
        )
        if base.get("signal_stage") not in {"confirmed", "strong_confirmed"}:
            continue
        code = str(base.get("code") or "")
        if not code:
            continue
        source_label = "LIVE保有中" if str(trade.get("status") or "") == "open" and not trade.get("sell_date") else "今日のAI判定"
        cards.append(_build_card(base, trade=trade, source_label=source_label))
        seen_codes.add(code)

    for row in rows:
        if not row.get("is_live_candidate"):
            continue
        if row.get("status") != "rebound_signal" and not row.get("is_live_candidate"):
            if not (row.get("status") == "entered" and _same_trade_date(row.get("drop_detected_at") or row.get("last_signal_at") or row.get("created_at"), latest_feature_date)):
                continue
        if row.get("status") == "entered" and not _same_trade_date(row.get("drop_detected_at") or row.get("last_signal_at") or row.get("created_at"), latest_feature_date):
            continue
        if row.get("signal_stage") not in {"confirmed", "strong_confirmed"}:
            continue
        if row.get("is_excluded"):
            continue
        if row.get("virtual_trade_id") and not row.get("is_live_candidate") and row.get("status") != "entered":
            continue
        if str(row.get("code") or "") in seen_codes:
            continue
        if not _not_expired(row):
            continue
        if row.get("status") == "entered":
            source_label = "今日のAI判定"
        elif row.get("status") == "signal_skipped":
            source_label = "LIVE見送り"
        else:
            source_label = "未エントリー"
        cards.append(_build_card(row, source_label=source_label))
        seen_codes.add(str(row.get("code") or ""))

    decorate_h5_price_assist_cards(cards)
    for card in cards:
        _decorate_position_sizing(
            card,
            {
                "expected_entry_price": card.get("expected_entry_price")
                or card.get("signal_price")
                or card.get("entry_price")
                or card.get("current_price_yf")
            },
        )
    cards.sort(
        key=lambda r: (
            bool(r.get("h5_primary_match")),
            bool(r.get("is_live_candidate")),
            H5_ENTRY_STATUS_PRIORITY.get(str(r.get("entry_status") or ""), -1)
            if r.get("h5_primary_match") and r.get("is_live_candidate") else -1,
            _num(r, "signal_probability", "ai_probability")
            if r.get("h5_primary_match") else STAGE_RANK.get(r.get("signal_stage"), 0),
            -_num(r, "entry_gap_pct", default=999.0)
            if r.get("h5_primary_match") else _num(r, "signal_probability", "ai_probability"),
            _num(r, "expected_value"),
            _num(r, "signal_score", "rebound_score", "score"),
        ),
        reverse=True,
    )
    cards = cards[:30]
    attach_environment_to_rows(cards, h5_environment)
    cards = _with_nikkei_link(cards)
    cards = _with_rebound_diagnostics(cards, market_adjustment, settings)
    try:
        history_rows = (
            supabase.table("trade_assist_candidate_history")
            .select("*")
            .neq("trade_date", latest_feature_date)
            .order("trade_date", desc=True)
            .order("ai_score", desc=True)
            .limit(120)
            .execute()
            .data or []
        )
        history_rows = decorate_history_rows(history_rows)
    except Exception as e:
        logger.warning("trade assist history load failed: %s", e)
        history_rows = []
    history_groups = []
    for history_row in history_rows:
        trade_date = str(history_row.get("trade_date") or "-")
        if not history_groups or history_groups[-1]["trade_date"] != trade_date:
            history_groups.append({"trade_date": trade_date, "rows": []})
        history_groups[-1]["rows"].append(history_row)
    history_groups = history_groups[:10]

    summary = {
        "latest_feature_date": latest_feature_date,
        "snapshot_count": snapshot_count,
        "ai_status": "完了" if update_status == "正常" else update_status,
        "buy_candidates": len(cards),
        "update_status": update_status,
        "last_updated": (latest_log or {}).get("finished_at") or (latest_log or {}).get("started_at"),
    }
    return render_template(
        "web/trade_assist.html",
        rows=cards,
        history_groups=history_groups,
        summary=summary,
        exit_display=exit_display,
        market_adjustment=market_adjustment,
        long_term_market=long_term_market,
        entry_mode_context=entry_mode_context,
        h5_primary=h5_exit_display,
        h5_environment=h5_environment,
    )


@app.route("/lab/box/trade_assist")
@app.route("/lab/box/trade-assist")
def web_box_trade_assist():
    market_adjustment = _current_market_adjustment()
    long_term_market = _current_long_term_market_regime()
    settings, settings_ok, settings_error = _box_load_settings()
    pending_rows, pending_ok, pending_error = _box_fetch_rows(
        "box_signals",
        lambda q: q.eq("entry_status", "entry_pending").order("trade_date", desc=True).order("created_at", desc=True).limit(100),
    )
    watch_rows, watch_ok, watch_error = _box_fetch_rows(
        "box_watchlist",
        lambda q: q.eq("status", "watching").order("trade_date", desc=True).order("watch_score", desc=True).limit(150),
    )
    history_rows, history_ok, history_error = _box_fetch_rows(
        "box_signals",
        lambda q: q.neq("entry_status", "entry_pending").order("trade_date", desc=True).order("created_at", desc=True).limit(120),
    )
    schema_ok = settings_ok and pending_ok and watch_ok and history_ok
    schema_error = settings_error or pending_error or watch_error or history_error
    _with_nikkei_link(pending_rows + watch_rows)
    summary = {
        "pending": len(pending_rows),
        "watchlist": len(watch_rows),
        "history": len(history_rows),
        "entry_mode": settings.get("entry_mode"),
        "gu_skip_pct": settings.get("gu_skip_pct"),
        "gd_skip_pct": settings.get("gd_skip_pct"),
    }
    return render_template(
        "web/trade_assist_box.html",
        rows=pending_rows,
        watch_rows=watch_rows,
        history_rows=history_rows,
        summary=summary,
        box_settings=settings,
        market_adjustment=market_adjustment,
        long_term_market=long_term_market,
        schema_ok=schema_ok,
        schema_error=schema_error,
    )


@app.route("/lab/box/watchlist")
def web_box_watchlist():
    rows, ok, error = _box_fetch_rows(
        "box_watchlist",
        lambda q: q.order("trade_date", desc=True).order("watch_score", desc=True).limit(300),
    )
    signal_rows, signal_ok, signal_error = _box_fetch_rows(
        "box_signals",
        lambda q: q.in_("entry_status", ["entry_pending", "entered"]).order("trade_date", desc=True).limit(500),
    )
    signal_by_date_code = {
        (str(r.get("trade_date")), str(r.get("code"))): r
        for r in signal_rows
        if r.get("trade_date") and r.get("code")
    }
    latest_signal_by_code = {}
    for sig in signal_rows:
        code = str(sig.get("code") or "")
        if code and code not in latest_signal_by_code:
            latest_signal_by_code[code] = sig
    for row in rows:
        key = (str(row.get("trade_date")), str(row.get("code")))
        sig = signal_by_date_code.get(key) or latest_signal_by_code.get(str(row.get("code") or ""))
        if sig:
            row["signal_entry_status"] = sig.get("entry_status")
            row["signal_box_score"] = sig.get("box_score")
            row["signal_trade_date"] = sig.get("trade_date")
            row["signal_id"] = sig.get("id")
    rows = _with_nikkei_link(rows)
    schema_ok = ok and signal_ok
    schema_error = error or signal_error
    return render_template(
        "web/box_watchlist.html",
        rows=rows,
        title="boxウォッチリスト",
        subtitle="6か月ボックス監視リスト",
        schema_ok=schema_ok,
        schema_error=schema_error,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/lab/box/signals")
def web_box_signals():
    rows, ok, error = _box_fetch_rows(
        "box_signals",
        lambda q: q.order("trade_date", desc=True).order("box_score", desc=True).limit(300),
    )
    stats = {
        "total": len(rows),
        "active": sum(1 for r in rows if r.get("entry_status") == "entry_pending"),
        "entered": sum(1 for r in rows if r.get("entry_status") == "entered"),
        "skipped": sum(1 for r in rows if r.get("entry_status") == "skipped"),
    }
    rows = _with_nikkei_link(rows)
    return render_template(
        "web/box_signals.html",
        rows=rows,
        signal_stats=stats,
        schema_ok=ok,
        schema_error=error,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/web/trade-assist/generate-reason", methods=["POST"])
def web_trade_assist_generate_reason():
    def _to_float(value, default=None):
        try:
            if value in (None, ""):
                return default
            return float(value)
        except Exception:
            return default

    code = str(request.form.get("code") or "").strip()
    name = str(request.form.get("name") or "").strip()
    drop_pct = _to_float(request.form.get("drop_pct"))
    nikkei_pct = _to_float(request.form.get("nikkei_pct"))

    if not code:
        flash("急落理由を生成する銘柄が見つかりません。", "warning")
        return redirect(url_for("web_trade_assist"))

    try:
        if not name or drop_pct is None or nikkei_pct is None:
            rows = (
                supabase.table("stock_drop_watchlist")
                .select("code,name,drop_pct,market_nikkei_pct,nikkei_pct")
                .eq("code", code)
                .order("updated_at", desc=True)
                .limit(1)
                .execute()
                .data or []
            )
            row = rows[0] if rows else {}
            name = name or str(row.get("name") or code)
            drop_pct = drop_pct if drop_pct is not None else _to_float(row.get("drop_pct"))
            nikkei_pct = nikkei_pct if nikkei_pct is not None else _to_float(row.get("market_nikkei_pct"), _to_float(row.get("nikkei_pct")))

        comment = get_stock_ai_comment(code, name or code, drop_pct, nikkei_pct)
        if comment:
            flash(f"{code} の急落理由AIコメントを生成しました。", "success")
        else:
            flash(f"{code} の急落理由AIコメントを生成できませんでした。", "warning")
    except Exception as e:
        logger.exception("trade assist reason generation failed code=%s", code)
        flash(f"{code} の急落理由生成に失敗しました: {e}", "warning")

    return redirect(url_for("web_trade_assist"))


@app.route("/web/trade-assist/generate-h5-ai-reasons", methods=["POST"])
def web_trade_assist_generate_h5_ai_reasons():
    code = str(request.form.get("code") or "").strip()
    if not code:
        flash("H5/AI理由を生成する銘柄が見つかりません。", "warning")
        return redirect(url_for("web_trade_assist"))

    form_row = {
        key: value
        for key, value in request.form.items()
        if value not in (None, "")
    }
    form_row["code"] = code
    try:
        rows = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .eq("code", code)
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        db_row = rows[0] if rows else {}
    except Exception as e:
        logger.warning("trade assist h5/ai reason candidate lookup failed code=%s: %s", code, e)
        db_row = {}

    row = {**db_row, **form_row}
    row = _with_ai_priority_stage(row, _current_market_adjustment())
    row["name"] = row.get("name") or code

    if "h5_primary_match" not in row or str(row.get("h5_primary_match") or "").strip() == "":
        try:
            h5_input = {
                **row,
                "signal_probability": row.get("ai_score")
                or row.get("display_probability")
                or row.get("signal_probability")
                or row.get("entry_probability"),
            }
            h5_passed, h5_reasons, h5_meta = evaluate_h5_primary_entry(h5_input)
            row["h5_primary_match"] = h5_passed
            row["h5_skip_reason"] = None if h5_passed else " / ".join(h5_reasons)
            row["h5_overheat_score"] = h5_meta.get("entry_overheat_score") or row.get("h5_overheat_score")
        except Exception as e:
            logger.warning("trade assist h5 reason entry evaluation failed code=%s: %s", code, e)

    try:
        reasons = build_h5_ai_reasons(row)
        upsert_cached_reasons(row, reasons)
        flash(f"{code} のH5/AI理由を生成しました。", "success")
    except Exception as e:
        logger.exception("trade assist h5/ai reason generation failed code=%s", code)
        flash(f"{code} のH5/AI理由生成に失敗しました: {e}", "warning")

    return redirect(url_for("web_trade_assist"))


@app.route("/web/trade-assist/generate-shap-reason", methods=["POST"])
def web_trade_assist_generate_shap_reason():
    code = _form_text("code")
    if not code:
        flash("SHAP理由を生成する銘柄が見つかりません。", "warning")
        return redirect(url_for("web_trade_assist"))

    row = {
        key: value
        for key, value in request.form.items()
        if value not in (None, "")
    }
    row["code"] = code
    row["name"] = row.get("name") or code
    row["model_key"] = row.get("model_key") or H5_SHAP_DEFAULT_MODEL_KEY
    row["trade_date"] = str(row.get("trade_date") or "")[:10]
    try:
        result = compute_shap_for_candidate(row, allow_active_fallback=False, force=False)
        merged = merge_shap_reason(result)
        if merged.get("ok"):
            save_shap_cache(merged)
            flash(f"SHAP理由を生成しました: {code} {row.get('name') or ''}", "success")
        else:
            reason = merged.get("reason") or merged.get("shap_reason_comment") or "unknown"
            save_shap_cache(merged)
            flash(f"SHAP理由生成に失敗しました: {reason}", "warning")
    except Exception as e:
        logger.exception("trade assist shap reason generation failed code=%s", code)
        flash(f"SHAP理由生成に失敗しました: {e}", "warning")
    return redirect(url_for("web_trade_assist"))


def _form_float(name: str, default=None):
    try:
        value = request.form.get(name)
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _form_text(name: str, default: str = "") -> str:
    return str(request.form.get(name) or default).strip()


@app.route("/web/h5/execution-reviews", methods=["POST"])
@app.route("/web/h5/mistakes", methods=["POST"])
def web_h5_execution_review_create():
    code = _form_text("code")
    if not code:
        flash("執行レビューの銘柄コードがありません。", "warning")
        return redirect(url_for("web_trade_assist"))
    free_text = _form_text("free_text")
    if not free_text:
        free_text = _form_text("free_text_preset")
    payload = {
        "trade_date": _form_text("trade_date") or None,
        "code": code,
        "name": _form_text("name") or code,
        "review_type": _form_text("review_type", _form_text("mistake_type", "missed_entry")),
        "case_key": _form_text("case_key") or H5_PRIMARY_CASE_KEY,
        "virtual_trade_id": _form_text("virtual_trade_id") or None,
        "signal_price": _form_float("signal_price"),
        "actual_price": _form_float("actual_price"),
        "missed_entry_price": _form_float("missed_entry_price"),
        "exit_price_after": _form_float("exit_price_after"),
        "expected_action": _form_text("expected_action"),
        "actual_action": _form_text("actual_action"),
        "reason_category": _form_text("reason_category"),
        "reason_emotion": _form_text("reason_emotion"),
        "result_summary": _form_text("result_summary"),
        "opportunity_loss_pct": _form_float("opportunity_loss_pct"),
        "actual_loss_pct": _form_float("actual_loss_pct"),
        "lesson": _form_text("lesson"),
        "prevention_rule": _form_text("prevention_rule"),
        "free_text": free_text,
        "status": _form_text("status", "open"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        supabase.table("trade_execution_reviews").insert(payload).execute()
        flash(f"{code} の執行レビューを保存しました。", "success")
    except Exception as e:
        logger.exception("h5 execution review create failed")
        flash(f"執行レビューの保存に失敗しました。db/h5_primary_virtual_trades.sql を再実行してください: {e}", "warning")
    return redirect(url_for("web_trade_assist"))


@app.route("/web/h5/execution-reviews/<review_id>/delete", methods=["POST"])
def web_h5_execution_review_delete(review_id: str):
    try:
        supabase.table("trade_execution_reviews").delete().eq("id", review_id).execute()
        flash("執行レビューを削除しました。", "success")
    except Exception as e:
        logger.exception("h5 execution review delete failed")
        flash(f"執行レビューの削除に失敗しました: {e}", "warning")
    return redirect(url_for("web_dashboard"))


@app.route("/web/h5/actual-trades", methods=["POST"])
def web_h5_actual_trade_create():
    code = _form_text("code")
    if not code:
        flash("実弾ログの銘柄コードがありません。", "warning")
        return redirect(url_for("web_trade_assist"))
    virtual_entry_price = _form_float("virtual_entry_price")
    actual_entry_price = _form_float("actual_entry_price")
    actual_exit_price = _form_float("actual_exit_price")
    virtual_exit_price = _form_float("virtual_exit_price")
    entry_slippage_pct = None
    if virtual_entry_price and actual_entry_price:
        entry_slippage_pct = (actual_entry_price / virtual_entry_price - 1.0) * 100.0
    payload = {
        "virtual_trade_id": _form_text("virtual_trade_id") or None,
        "case_key": _form_text("case_key") or H5_PRIMARY_CASE_KEY,
        "trade_date": _form_text("trade_date") or None,
        "code": code,
        "name": _form_text("name") or code,
        "virtual_entry_price": virtual_entry_price,
        "actual_entry_price": actual_entry_price,
        "actual_entry_date": _form_text("actual_entry_date") or None,
        "actual_order_type": _form_text("actual_order_type"),
        "actual_fill_status": _form_text("actual_fill_status"),
        "virtual_exit_price": virtual_exit_price,
        "actual_exit_price": actual_exit_price,
        "actual_exit_date": _form_text("actual_exit_date") or None,
        "virtual_pnl_pct": _form_float("virtual_pnl_pct"),
        "actual_pnl_pct": _form_float("actual_pnl_pct"),
        "entry_slippage_pct": entry_slippage_pct,
        "lot_amount": _form_float("lot_amount"),
        "quantity": _form_float("quantity"),
        "skip_reason": _form_text("skip_reason"),
        "note": _form_text("note"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        supabase.table("actual_trade_logs").insert(payload).execute()
        vt_id = payload.get("virtual_trade_id")
        if vt_id:
            update_payload = {
                "actual_entry_price": actual_entry_price,
                "actual_entry_date": payload.get("actual_entry_date"),
                "actual_order_type": payload.get("actual_order_type"),
                "actual_fill_status": payload.get("actual_fill_status"),
                "actual_exit_price": actual_exit_price,
                "actual_exit_date": payload.get("actual_exit_date"),
                "actual_pnl_pct": payload.get("actual_pnl_pct"),
                "entry_slippage_pct": entry_slippage_pct,
                "skip_reason": payload.get("skip_reason"),
                "actual_note": payload.get("note"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                supabase.table("virtual_trades").update(update_payload).eq("id", vt_id).execute()
            except Exception as e:
                logger.warning("virtual trade actual fields update failed: %s", e)
        flash(f"{code} の実弾ログを保存しました。", "success")
    except Exception as e:
        logger.exception("h5 actual trade create failed")
        flash(f"実弾ログの保存に失敗しました。db/h5_primary_virtual_trades.sql を再実行してください: {e}", "warning")
    return redirect(url_for("web_trade_assist"))


def _get_next_trading_days(start_date: date, n: int) -> list[date]:
    """Return the next n trading days after start_date.

    Uses stock_feature_snapshots trade_dates (JPX actual calendar).
    Falls back to weekday-only if DB query fails.
    """
    try:
        rows = (
            supabase.table("stock_feature_snapshots")
            .select("trade_date")
            .gt("trade_date", start_date.isoformat())
            .order("trade_date")
            .limit(n * 30)
            .execute()
            .data or []
        )
        seen: set = set()
        dates: list[date] = []
        for row in rows:
            d = str(row.get("trade_date", ""))[:10]
            if d and d not in seen:
                seen.add(d)
                try:
                    dates.append(date.fromisoformat(d))
                except ValueError:
                    pass
                if len(dates) >= n:
                    break
        if len(dates) >= n:
            return dates
    except Exception as e:
        logger.warning("_get_next_trading_days failed: %s", e)
    result: list[date] = []
    current = start_date
    while len(result) < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            result.append(current)
    return result


def _build_actual_trade_fields(
    actual_entry_date_str: str | None,
    actual_entry_price: float | None,
    virtual_entry_price: float | None,
) -> dict:
    """Calculate derived fields for a new actual trade entry."""
    fields: dict = {}
    if actual_entry_date_str and actual_entry_price:
        try:
            entry_date = date.fromisoformat(actual_entry_date_str[:10])
            trading_days = _get_next_trading_days(entry_date, 3)
            if len(trading_days) >= 1:
                fields["actual_day1_date"] = trading_days[0].isoformat()
            if len(trading_days) >= 2:
                fields["actual_day2_date"] = trading_days[1].isoformat()
            if len(trading_days) >= 3:
                fields["actual_day3_exit_due_date"] = trading_days[2].isoformat()
        except Exception as e:
            logger.warning("actual trade date calc failed: %s", e)
        fields["actual_emergency_stop_price"] = round(actual_entry_price * 0.88, 0)
    if actual_entry_price and virtual_entry_price:
        fields["entry_slippage_pct"] = round(
            (actual_entry_price / virtual_entry_price - 1.0) * 100.0, 3
        )
    return fields


@app.route("/web/actions/h5/actual_entry", methods=["POST"])
def web_h5_actual_entry():
    """Record a new H5 live trade entry with auto-computed day1/day2/day3 and emergency stop."""
    code = _form_text("code")
    if not code:
        flash("銘柄コードが必要です。", "warning")
        return redirect(url_for("web_dashboard"))
    actual_entry_price = _form_float("actual_entry_price")
    virtual_entry_price = _form_float("virtual_entry_price")
    actual_entry_date_str = _form_text("actual_entry_date")
    derived = _build_actual_trade_fields(actual_entry_date_str, actual_entry_price, virtual_entry_price)
    payload = {
        "code": code,
        "name": _form_text("name") or code,
        "case_key": _form_text("case_key") or H5_PRIMARY_CASE_KEY,
        "virtual_trade_id": _form_text("virtual_trade_id") or None,
        "watchlist_id": _form_text("watchlist_id") or None,
        "signal_date": _form_text("signal_date") or None,
        "signal_price": _form_float("signal_price"),
        "virtual_entry_date": _form_text("virtual_entry_date") or None,
        "virtual_entry_price": virtual_entry_price,
        "virtual_exit_due_date": _form_text("virtual_exit_due_date") or None,
        "actual_entry_date": actual_entry_date_str or None,
        "actual_entry_price": actual_entry_price,
        "actual_entry_model": _form_text("actual_entry_model") or None,
        "actual_order_type": _form_text("actual_order_type") or None,
        "actual_fill_status": _form_text("actual_fill_status") or "filled",
        "quantity": _form_float("quantity"),
        "lot_amount": _form_float("lot_amount"),
        "actual_exit_status": "holding",
        "actual_exit_due_reason": "hd3_time_stop",
        "trade_date": (actual_entry_date_str or "")[:10] or None,
        "note": _form_text("note"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **derived,
    }
    try:
        supabase.table("actual_trade_logs").insert(payload).execute()
        d3 = derived.get("actual_day3_exit_due_date", "?")
        flash(f"{code} の実弾entryを記録しました。day3期限: {d3}", "success")
    except Exception as e:
        logger.exception("actual entry create failed")
        flash(f"実弾entryの保存に失敗しました。db/actual_trade_logs_v2.sql を実行してください: {e}", "warning")
    return redirect(url_for("web_dashboard"))


@app.route("/web/actions/h5/actual_exit", methods=["POST"])
def web_h5_actual_exit():
    """Record exit for an existing H5 live trade, computing PnL."""
    trade_id = _form_text("actual_trade_id")
    if not trade_id:
        flash("actual_trade_id がありません。", "warning")
        return redirect(url_for("web_dashboard"))
    actual_exit_price = _form_float("actual_exit_price")
    actual_entry_price = _form_float("actual_entry_price")
    quantity = _form_float("quantity")
    actual_exit_reason = _form_text("actual_exit_reason") or "hd3_time_stop"
    status_map = {
        "hd3_time_stop": "time_stopped",
        "emergency_stop_12": "stopped",
        "peak_pullback_2": "peak_pullback_exited",
        "manual_exit": "exited",
        "other": "exited",
    }
    actual_exit_status = status_map.get(actual_exit_reason, "exited")
    payload: dict = {
        "actual_exit_date": _form_text("actual_exit_date") or None,
        "actual_exit_price": actual_exit_price,
        "actual_exit_reason": actual_exit_reason,
        "actual_exit_status": actual_exit_status,
        "note": _form_text("exit_note"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if actual_exit_price and actual_entry_price:
        pnl_pct = (actual_exit_price / actual_entry_price - 1.0) * 100.0
        payload["actual_pnl_pct"] = round(pnl_pct, 3)
        if quantity:
            payload["actual_pnl_amount"] = round(
                (actual_exit_price - actual_entry_price) * quantity, 0
            )
    try:
        supabase.table("actual_trade_logs").update(payload).eq("id", trade_id).execute()
        flash("実弾exitを記録しました。", "success")
    except Exception as e:
        logger.exception("actual exit update failed")
        flash(f"実弾exitの保存に失敗しました: {e}", "warning")
    return redirect(url_for("web_dashboard"))


def _entry_mode_migration_status() -> bool:
    try:
        supabase.table("strategy_settings").select("entry_mode").limit(1).execute()
        supabase.table("stock_drop_watchlist").select("entry_mode_used,entry_case,entry_ma5_gap_pct").limit(1).execute()
        supabase.table("virtual_trades").select("entry_mode_used,entry_case,entry_ma5_gap_pct").limit(1).execute()
        return True
    except Exception as e:
        logger.warning("entry_mode migration check failed: %s", e)
        return False


def _h5_primary_migration_status() -> bool:
    try:
        supabase.table("virtual_trades").select(
            "case_key,is_primary_h5,exit_rule,peak_pullback_pct,initial_sl_pct,max_holding_days,"
            "position_limit_mode,is_live_candidate,is_h5_research_candidate,is_h5_live_candidate,"
            "live_candidate_rank,selected_rank,live_skip_reason,actual_entry_price,actual_exit_date,"
            "signal_price,current_price_yf,current_price_fetched_at,entry_gap_pct,entry_status"
        ).limit(1).execute()
        supabase.table("stock_drop_watchlist").select(
            "position_limit_mode,is_live_candidate,is_h5_research_candidate,is_h5_live_candidate,"
            "live_candidate_rank,selected_rank,live_skip_reason"
        ).limit(1).execute()
        supabase.table("trade_execution_reviews").select("id").limit(1).execute()
        supabase.table("actual_trade_logs").select("id,watchlist_id,intraday_h5_status").limit(1).execute()
        supabase.table("h5_watchlist").select(
            "id,signal_probability,current_price,intraday_h5_checked_at,intraday_h5_reason"
        ).limit(1).execute()
        return True
    except Exception as e:
        logger.warning("H5 Primary migration check failed: %s", e)
        return False


@app.route("/web/settings", methods=["GET", "POST"])
def web_settings():
    if request.method == "POST":
        bool_fields = {
            "ma5_cross_enabled", "drop_notify_enabled", "rebound_notify_enabled",
            "morning_summary_enabled", "portfolio_notify_enabled",
            "ai_predict_enabled", "ai_notify_enabled", "ai_notify_early_enabled",
            "jquants_enabled", "jquants_prefer_source", "jquants_fallback_yfinance",
            "entry_margin_filter_enabled", "entry_margin_require_data",
        }
        int_fields = {
            "watch_days_limit", "jquants_max_retry",
            "max_open_positions", "max_daily_entries",
            "entry_rank_limit", "max_sector_positions",
            "virtual_exit_holding_days", "virtual_exit_extend_high_update_days",
        }
        string_fields = {"entry_mode", "entry_mode_note"}
        def _upsert_settings(payload: dict) -> None:
            remaining = dict(payload)
            for _ in range(8):
                try:
                    user_id = remaining.get("user_id", "global")
                    update_payload = {k: v for k, v in remaining.items() if k != "user_id"}
                    updated = (
                        supabase.table("strategy_settings")
                        .update(update_payload)
                        .eq("user_id", user_id)
                        .execute()
                        .data or []
                    )
                    if updated:
                        return
                    supabase.table("strategy_settings").insert(remaining).execute()
                    return
                except Exception as e:
                    msg = str(e)
                    marker = "Could not find the '"
                    missing = None
                    if marker in msg:
                        missing = msg.split(marker, 1)[1].split("'", 1)[0]
                    if missing and missing in remaining:
                        logger.warning("strategy_settings column missing; skip field for this save: %s", missing)
                        remaining.pop(missing, None)
                        continue
                    raise

        try:
            data: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
            for key, default in _settings_loader.DEFAULTS.items():
                if key in bool_fields:
                    data[key] = key in request.form
                elif key in int_fields:
                    data[key] = int(request.form.get(key, default))
                elif key in string_fields:
                    data[key] = str(request.form.get(key, default) or default)
                else:
                    data[key] = float(request.form.get(key, default))
            if data.get("entry_mode") not in {"auto", "normal", "risk_on_pullback", "panic_deep_rebound", "paused"}:
                data["entry_mode"] = "normal"
            data["entry_mode_updated_at"] = data["updated_at"]
            _upsert_settings({**data, "user_id": "global"})
            _settings_loader._cache = None
            flash("設定を保存した", "success")
        except Exception as e:
            logger.error("settings save error: %s", e)
            flash(f"保存失敗: {e}", "danger")
        return redirect(request.path)
    cfg = _settings_loader.get_settings()
    entry_mode_migration_ok = _entry_mode_migration_status()
    h5_primary_migration_ok = _h5_primary_migration_status()
    return render_template(
        "web/settings.html",
        cfg=cfg,
        entry_mode_labels=ENTRY_MODE_LABELS,
        entry_mode_migration_ok=entry_mode_migration_ok,
        h5_primary_migration_ok=h5_primary_migration_ok,
        h5_primary={
            "case_key": H5_PRIMARY_CASE_KEY,
            "label": H5_PRIMARY_DISPLAY_NAME,
            "rules": H5_PRIMARY_RULES,
            "entry_execution_note": H5_ENTRY_EXECUTION_NOTE,
        },
    )


@app.route("/lab/box/settings", methods=["GET", "POST"])
def web_box_settings():
    numeric_fields = {
        "box_width_pct": float,
        "signal_box_position_pct": float,
        "atr_max_pct": float,
        "gu_skip_pct": float,
        "gd_skip_pct": float,
        "min_turnover_value": float,
        "min_price": float,
        "min_equity_ratio": float,
        "max_per": float,
        "max_pbr": float,
    }
    int_fields = {"max_open_positions", "max_sector_positions", "max_pending_days"}
    if request.method == "POST":
        settings, settings_ok, settings_error = _box_load_settings()
        if not settings_ok:
            flash(f"box_settings が未適用です。db/box_lab.sql をSupabase SQL Editorで実行してください: {settings_error}", "danger")
            return redirect(url_for("web_box_settings"))
        payload = {"updated_at": datetime.now(timezone.utc).isoformat()}
        try:
            entry_mode = str(request.form.get("entry_mode") or "normal")
            if entry_mode not in {"normal", "box_pullback", "paused"}:
                entry_mode = "normal"
            payload["entry_mode"] = entry_mode
            for key, caster in numeric_fields.items():
                payload[key] = caster(request.form.get(key, settings.get(key)))
            for key in int_fields:
                payload[key] = int(request.form.get(key, settings.get(key)))
            payload["note"] = str(request.form.get("note") or "")
            updated = (
                supabase.table("box_settings")
                .update(payload)
                .eq("user_id", "global")
                .execute()
                .data or []
            )
            if not updated:
                supabase.table("box_settings").insert({**payload, "user_id": "global"}).execute()
            flash("box_lab設定を保存しました", "success")
        except Exception as e:
            logger.exception("box_settings save failed")
            flash(f"box_lab設定の保存に失敗しました: {e}", "danger")
        return redirect(url_for("web_box_settings"))

    settings, settings_ok, settings_error = _box_load_settings()
    return render_template(
        "web/settings_box.html",
        cfg=settings,
        schema_ok=settings_ok,
        schema_error=settings_error,
    )


@app.route("/web/virtual-trades")
def web_virtual_trades():
    strategy_options = [
        ("all", "All"),
        ("H5_PRIMARY", "H5 Primary"),
        ("H5_SHORT_PULLBACK", "H5 Short Pullback"),
        ("H5_MIX", "H5 Mix"),
        ("TREND_SUPPORT", "Trend Support"),
        ("PRICE_BAND", "Price Band"),
    ]

    def _strategy_group(row: dict) -> str:
        group = str(row.get("strategy_group") or "").upper()
        case = str(row.get("case_key") or "")
        if group == "PRICE_BAND" or case.startswith("PB_"):
            return "PRICE_BAND"
        if group in {"TREND_SUPPORT", "TREND_FOLLOWING"} or case.startswith("tf_"):
            return "TREND_SUPPORT"
        if row.get("is_primary_h5") or case == H5_PRIMARY_CASE_KEY or case in H5_ACTIVE_CASE_KEYS:
            return "H5_PRIMARY"
        if case == "H5_short_pullback_drop5_m3":
            return "H5_SHORT_PULLBACK"
        if case.startswith("mix_") or case == "H5_current7_short3" or group in {"H5_MIX", "EXPERIMENTAL_MIX"}:
            return "H5_MIX"
        return group or "H5_PRIMARY"

    def _strategy_label(row: dict) -> str:
        label = row.get("strategy_label") or row.get("case_label")
        if label:
            return str(label)
        group = _strategy_group(row)
        return dict(strategy_options).get(group, group)

    def _decorate_strategy(row: dict) -> dict:
        row["strategy_group_display"] = _strategy_group(row)
        row["strategy_label_display"] = _strategy_label(row)
        sizing = calculate_virtual_position_size(row.get("buy_price"))
        for key, value in sizing.items():
            if row.get(key) in (None, "") or key in {"theoretical_shares", "theoretical_position_size"}:
                row[key] = value
        buy = float(row.get("buy_price") or 0)
        shares = int(row.get("theoretical_shares") or 0)
        current = row.get("current_price")
        exit_price = row.get("sell_price") or row.get("exit_price") or row.get("virtual_exit_price")
        try:
            if buy > 0 and shares > 0 and current is not None:
                cur = float(current)
                row["unrealized_pnl"] = (cur - buy) * shares
                row["unrealized_pnl_pct"] = (cur / buy - 1.0) * 100.0
            if buy > 0 and shares > 0 and exit_price is not None:
                ex = float(exit_price)
                row["profit_loss"] = (ex - buy) * shares
                row["profit_loss_pct"] = (ex / buy - 1.0) * 100.0
        except Exception:
            pass
        buy_dt = row.get("buy_date")
        try:
            bd = datetime.fromisoformat(str(buy_dt).replace("Z", "+00:00")).date() if buy_dt else None
            row["hold_days"] = (datetime.now(JST).date() - bd).days if bd else None
        except Exception:
            row["hold_days"] = None
        return row

    def _exit_date_value(row: dict):
        return row.get("exit_date") or row.get("sell_date")

    def _is_open_virtual_trade(row: dict) -> bool:
        return row.get("status") == "open" and not _exit_date_value(row)

    def _is_closed_virtual_trade(row: dict) -> bool:
        return row.get("status") == "closed" or bool(_exit_date_value(row))

    def _load_virtual_trades(exit_col: str = "exit_date") -> tuple[list[dict], list[dict]]:
        open_rows = (
            supabase.table("virtual_trades").select("*")
            .eq("status", "open")
            .is_(exit_col, "null")
            .order("buy_date", desc=True)
            .execute()
            .data or []
        )
        closed_condition = f"status.eq.closed,{exit_col}.not.is.null"
        if exit_col != "sell_date":
            closed_condition += ",sell_date.not.is.null"
        closed_rows = (
            supabase.table("virtual_trades").select("*")
            .or_(closed_condition)
            .order(exit_col, desc=True)
            .limit(100)
            .execute()
            .data or []
        )
        return open_rows, closed_rows

    try:
        try:
            open_trades, closed_trades = _load_virtual_trades("exit_date")
        except Exception as e:
            # Older virtual_trades schemas use sell_date as the exit date column.
            logger.warning("virtual_trades exit_date query failed; fallback to sell_date: %s", e)
            open_trades, closed_trades = _load_virtual_trades("sell_date")
        open_trades = [t for t in open_trades if _is_open_virtual_trade(t)]
        closed_trades = [t for t in closed_trades if _is_closed_virtual_trade(t)]
        closed_ids = {str(t.get("id")) for t in closed_trades if t.get("id")}
        open_trades = [t for t in open_trades if str(t.get("id")) not in closed_ids]
        open_trades = [_decorate_strategy(t) for t in open_trades]
        closed_trades = [_decorate_strategy(t) for t in closed_trades]
    except Exception as e:
        logger.error("virtual_trades error: %s", e)
        open_trades, closed_trades = [], []

    selected_strategy_group = request.args.get("strategy_group", "all")
    all_open_trades_for_summary = list(open_trades)
    all_closed_trades_for_summary = list(closed_trades)
    if selected_strategy_group != "all":
        open_trades = [t for t in open_trades if t.get("strategy_group_display") == selected_strategy_group]
        closed_trades = [t for t in closed_trades if t.get("strategy_group_display") == selected_strategy_group]

    open_cost_total = 0.0
    open_value_total = 0.0
    open_unrealized_pnl_total = 0.0

    for t in open_trades:
        buy = float(t.get("buy_price") or 0)
        qty = int(t.get("theoretical_shares") or 0) or 100
        t["display_quantity"] = qty
        if not t.get("quantity"):
            t["quantity"] = qty
        cost = buy * qty
        current = t.get("current_price")
        t["cost_amount"] = cost
        open_cost_total += cost
        if current is None:
            t["market_value"] = cost
            t["unrealized_pct"] = t.get("unrealized_pnl_pct")
            t["unrealized_pnl"] = t.get("unrealized_pnl")
            open_value_total += cost
            continue
        try:
            current_f = float(current)
            value = current_f * qty
            pnl = value - cost
            t["market_value"] = value
            t["unrealized_pct"] = (current_f - buy) / buy * 100 if buy > 0 else None
            t["unrealized_pnl"] = pnl if buy > 0 else None
            open_value_total += value
            open_unrealized_pnl_total += pnl if buy > 0 else 0
        except Exception:
            t["current_price"] = None
            t["market_value"] = cost
            t["unrealized_pct"] = None
            t["unrealized_pnl"] = None
            open_value_total += cost

    cleanup_reasons = {"cleanup_position_limit", "cleanup_duplicate_open"}
    performance_closed_trades = [
        t for t in closed_trades
        if (t.get("exit_reason") or "") not in cleanup_reasons
    ]
    cleanup_closed_count = len(closed_trades) - len(performance_closed_trades)
    market_adjustment = _current_market_adjustment()
    _with_rebound_diagnostics(open_trades + performance_closed_trades, market_adjustment)
    total_pnl = sum(t.get("profit_loss") or 0 for t in performance_closed_trades)
    win_count = sum(1 for t in performance_closed_trades if (t.get("profit_loss") or 0) > 0)
    strategy_summary = []
    for group_key, group_label in strategy_options:
        if group_key == "all":
            continue
        open_group = [t for t in all_open_trades_for_summary if t.get("strategy_group_display") == group_key]
        closed_group = [
            t for t in all_closed_trades_for_summary
            if t.get("strategy_group_display") == group_key and (t.get("exit_reason") or "") not in cleanup_reasons
        ]
        wins = [t for t in closed_group if (t.get("profit_loss") or 0) > 0]
        profits = [float(t.get("profit_loss") or 0) for t in closed_group if (t.get("profit_loss") or 0) > 0]
        losses = [float(t.get("profit_loss") or 0) for t in closed_group if (t.get("profit_loss") or 0) < 0]
        returns = [float(t.get("profit_loss_pct") or 0) for t in closed_group if t.get("profit_loss_pct") is not None]
        position_sizes = [
            float(t.get("theoretical_position_size") or 0)
            for t in open_group + closed_group
            if t.get("theoretical_position_size") is not None
        ]
        shares = [
            float(t.get("theoretical_shares") or 0)
            for t in open_group + closed_group
            if t.get("theoretical_shares") is not None
        ]
        lot_counts = {}
        for t in open_group + closed_group:
            lot = t.get("lot_type") or "unknown"
            lot_counts[lot] = lot_counts.get(lot, 0) + 1
        eq = peak = max_dd = 0.0
        for t in sorted(closed_group, key=lambda x: str(x.get("sell_date") or x.get("exit_date") or "")):
            eq += float(t.get("profit_loss") or 0)
            peak = max(peak, eq)
            max_dd = min(max_dd, eq - peak)
        strategy_summary.append({
            "strategy_group": group_key,
            "strategy_label": group_label,
            "open_count": len(open_group),
            "closed_count": len(closed_group),
            "win_rate": (len(wins) / len(closed_group) * 100) if closed_group else None,
            "PF": (sum(profits) / abs(sum(losses))) if losses else (None if not profits else float("inf")),
            "avg_return": (sum(returns) / len(returns)) if returns else None,
            "unrealized_pnl": sum(float(t.get("unrealized_pnl") or 0) for t in open_group),
            "realized_pnl": sum(float(t.get("profit_loss") or 0) for t in closed_group),
            "max_dd": abs(max_dd),
            "avg_position_size": (sum(position_sizes) / len(position_sizes)) if position_sizes else None,
            "avg_shares": (sum(shares) / len(shares)) if shares else None,
            "lot_type_counts": ", ".join(f"{k}:{v}" for k, v in sorted(lot_counts.items())),
        })
    open_unrealized_pct_total = (
        open_unrealized_pnl_total / open_cost_total * 100
        if open_cost_total > 0 else None
    )
    return render_template(
        "web/virtual_trades.html",
        open_trades=open_trades,
        closed_trades=performance_closed_trades,
        total_closed_count=len(closed_trades),
        performance_closed_count=len(performance_closed_trades),
        cleanup_closed_count=cleanup_closed_count,
        total_pnl=total_pnl,
        win_count=win_count,
        open_cost_total=open_cost_total,
        open_value_total=open_value_total,
        open_unrealized_pnl_total=open_unrealized_pnl_total,
        open_unrealized_pct_total=open_unrealized_pct_total,
        market_adjustment=market_adjustment,
        strategy_options=strategy_options,
        selected_strategy_group=selected_strategy_group,
        strategy_summary=strategy_summary,
    )


@app.route("/lab/box/positions")
@app.route("/lab/box/virtual-trades")
def web_box_positions():
    open_trades, open_ok, open_error = _box_fetch_rows(
        "box_virtual_trades",
        lambda q: q.eq("status", "open").order("buy_date", desc=True).limit(100),
    )
    closed_trades, closed_ok, closed_error = _box_fetch_rows(
        "box_virtual_trades",
        lambda q: q.or_("status.eq.closed,sell_date.not.is.null").order("sell_date", desc=True).limit(100),
    )
    schema_ok = open_ok and closed_ok
    schema_error = open_error or closed_error

    open_cost_total = 0.0
    open_value_total = 0.0
    open_unrealized_pnl_total = 0.0
    for trade in open_trades:
        buy = float(trade.get("buy_price") or 0)
        qty = int(trade.get("quantity") or 100)
        current = trade.get("current_price")
        cost = buy * qty
        open_cost_total += cost
        try:
            current_f = float(current) if current is not None else buy
            value = current_f * qty
            pnl = value - cost
            trade["market_value"] = value
            trade["unrealized_pnl"] = pnl
            trade["unrealized_pnl_pct"] = (current_f / buy - 1) * 100 if buy > 0 else None
            open_value_total += value
            open_unrealized_pnl_total += pnl
        except Exception:
            trade["market_value"] = cost
            open_value_total += cost

    total_pnl = sum(float(t.get("profit_loss") or 0) for t in closed_trades)
    win_count = sum(1 for t in closed_trades if float(t.get("profit_loss") or 0) > 0)
    open_unrealized_pct_total = open_unrealized_pnl_total / open_cost_total * 100 if open_cost_total > 0 else None
    return render_template(
        "web/positions_box.html",
        open_trades=open_trades,
        closed_trades=closed_trades,
        total_pnl=total_pnl,
        win_count=win_count,
        open_cost_total=open_cost_total,
        open_value_total=open_value_total,
        open_unrealized_pnl_total=open_unrealized_pnl_total,
        open_unrealized_pct_total=open_unrealized_pct_total,
        market_adjustment=_current_market_adjustment(),
        schema_ok=schema_ok,
        schema_error=schema_error,
    )


@app.route("/lab/box/virtual-trades/performance")
def web_box_virtual_trade_performance():
    from services.virtual_trade_performance import aggregate, open_summary, top_card_summary

    period = request.args.get("period", "weekly")
    if period not in ("daily", "weekly", "monthly"):
        period = "weekly"
    try:
        all_rows = (
            supabase.table("box_virtual_trades")
            .select("*")
            .order("buy_date", desc=True)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("[box_virtual_trade_performance] load failed: %s", e)
        all_rows = []
        flash("box_virtual_trades が未作成かもしれません。db/box_lab.sql をSupabaseで実行してください。", "warning")

    rows = aggregate(all_rows, period)
    open_sum = open_summary(all_rows)
    top = top_card_summary(all_rows)
    return render_template(
        "web/virtual_trade_performance.html",
        rows=rows,
        period=period,
        open_sum=open_sum,
        top=top,
        base_path="/lab/box/virtual-trades/performance",
    )


@app.route("/lab/box/virtual-trades/performance/detail")
def web_box_virtual_trade_performance_detail():
    from services.virtual_trade_performance import detail_trades

    period = request.args.get("period", "weekly")
    period_start = request.args.get("period_start", "")
    period_end = request.args.get("period_end", "")
    try:
        all_rows = (
            supabase.table("box_virtual_trades")
            .select("*")
            .order("buy_date", desc=True)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("[box_virtual_trade_performance_detail] load failed: %s", e)
        all_rows = []
    trades = detail_trades(all_rows, period_start, period_end)
    return render_template(
        "web/virtual_trade_performance_detail.html",
        trades=trades,
        period=period,
        period_start=period_start,
        period_end=period_end,
    )


@app.route("/lab/box/case-tests")
def web_box_case_tests():
    return render_template(
        "web/stub.html",
        title="box比較テスト",
        message="box_lab専用の比較テストは次フェーズで実装します。UI導線だけ先に分離しています。",
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/lab/box/research-db")
def web_box_research_db():
    return render_template(
        "web/stub.html",
        title="box検証データベース",
        message="box_lab専用の検証DBビューは次フェーズで実装します。市場データはrebound_labと共有します。",
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/lab/box/models")
def web_box_models():
    return render_template(
        "web/stub.html",
        title="box AI Models",
        message="box_lab専用モデル管理は未実装です。まずはルールベースのbox_watchlist / box_signalsを使います。",
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/lab/rebound/watchlist")
def web_rebound_watchlist_alias():
    return web_watchlist()


@app.route("/lab/rebound/signals")
def web_rebound_signals_alias():
    return web_signals()


@app.route("/lab/rebound/trade-assist")
def web_rebound_trade_assist_alias():
    return web_trade_assist()


@app.route("/lab/rebound/virtual-trades")
def web_rebound_virtual_trades_alias():
    return web_virtual_trades()


@app.route("/lab/rebound/virtual-trades/performance")
def web_rebound_virtual_trade_performance_alias():
    return web_virtual_trade_performance()


@app.route("/lab/rebound/case-tests")
def web_rebound_case_tests_alias():
    return web_case_tests()


@app.route("/lab/rebound/research-db")
def web_rebound_research_db_alias():
    return web_research_db()


@app.route("/lab/rebound/models")
def web_rebound_models_alias():
    return web_models()


@app.route("/lab/rebound/settings", methods=["GET", "POST"])
def web_rebound_settings_alias():
    return web_settings()


@app.route("/web/virtual-trades/performance")
def web_virtual_trade_performance():
    from services.virtual_trade_performance import aggregate, open_summary, top_card_summary

    period = request.args.get("period", "weekly")
    if period not in ("daily", "weekly", "monthly"):
        period = "weekly"

    try:
        all_rows = (
            supabase.table("virtual_trades")
            .select("*")
            .order("buy_date", desc=True)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.error("[virtual_trade_performance] load failed: %s", e)
        all_rows = []

    rows = aggregate(all_rows, period)
    open_sum = open_summary(all_rows)
    top = top_card_summary(all_rows)

    return render_template(
        "web/virtual_trade_performance.html",
        rows=rows,
        period=period,
        open_sum=open_sum,
        top=top,
    )


@app.route("/web/virtual-trades/performance/detail")
def web_virtual_trade_performance_detail():
    from services.virtual_trade_performance import detail_trades

    period = request.args.get("period", "weekly")
    period_start = request.args.get("period_start", "")
    period_end = request.args.get("period_end", "")

    try:
        all_rows = (
            supabase.table("virtual_trades")
            .select("*")
            .order("buy_date", desc=True)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.error("[virtual_trade_performance_detail] load failed: %s", e)
        all_rows = []

    trades = detail_trades(all_rows, period_start, period_end)

    return render_template(
        "web/virtual_trade_performance_detail.html",
        trades=trades,
        period=period,
        period_start=period_start,
        period_end=period_end,
    )


CASE_TEST_LABELS = {
    "current_settings": "現状設定",
    "current_rule": "現行ルール",
    "ai_top10": "AI上位10件",
    "ev_top10": "期待値上位10件",
    "position_limited": "最大保有20・1日5件",
    "sector_limited": "セクター最大2件",
    "regime_strict": "地合い厳格化",
    "model_agreement": "5日/10日一致のみ",
    "fixed_tp_7": "固定利確7%",
    "fixed_tp_10": "固定利確10%",
    "trailing_3": "トレーリング3%",
    "trailing_5": "トレーリング5%",
    "pullback_2": "前日比-2%利確",
    "ma5_exit": "5日MA割れ利確",
    "rsi70_exit": "RSI70反落利確",
    "volume_fade": "出来高減衰利確",
    "atr_trailing_15": "ATRトレーリングx1.5",
    "h5_ai65_hd3_est12_cm_range330_live_limited": "H5 Live Limited：AI65 / HD3 / EST12 / 信用3-30",
    "h5_ai65_hd3_est12_cm_range330_research": "H5 Research（制限なし）：AI65 / HD3 / EST12 / 信用3-30",
    "h5_ai65_hd5_ext_d3ret_m1_est12_cm_range330_live_limited": "H5 Extension研究：day3 -1%以下ならHD5延長",
    "h5_ai65_hd5_ext_d3ret_m1_est12_cm_range330_research": "H5 Extension研究（制限なし）：day3 -1%以下ならHD5延長",
    "h5_ai65_hd5_ext_m1_ban_uwrsi_est12_range330_live_limited": "H5 Ext Ban研究：D3<-1延長 / 深掘り上ヒゲRSI禁止",
    "h5_ai65_hd5_ext_m1_ban_uwrsi_est12_range330_research": "H5 Ext Ban研究（制限なし）：D3<-1延長 / 深掘り上ヒゲRSI禁止",
    "h5_ai65_hd5_ext_m1_allow_d1bodyvol_est12_range330_live_limited": "H5 Ext Allow研究：D1/Body/Vol条件でHD5延長",
    "h5_ai65_hd5_ext_m1_allow_d1bodyvol_est12_range330_research": "H5 Ext Allow研究（制限なし）：D1/Body/Vol条件でHD5延長",
    "h5_ai65_pb20_hd3_est12_cm_range330_live_limited": "H5比較 PB20 Live：AI65 / PB2 / HD3 / EST12 / 信用3-30",
    "h5_ai65_pb20_hd3_est12_cm_range330_research": "H5比較 PB20 Research：AI65 / PB2 / HD3 / EST12 / 信用3-30",
    "h5_ai65_pb20_hd3_est12_cm_range330": "H5 Legacy：AI65 / PB2 / HD3 / EST12 / 信用3-30",
    "h5_ai65_pb20_hd3_nostop_cm_range330": "H5比較：NOSTOP / 信用3-30",
    "h5_ai65_pb20_hd3_est12_cm_mr20": "H5比較：EST12 / 信用20以下",
    "h5_ai65_pb20_hd3_est8_cm_range330": "H5比較：EST8 / 信用3-30",
    "h5_ai60_pb20_hd3_est12_cm_range330": "H5比較：AI60 / EST12 / 信用3-30",
}

ENTRY_PROFILE_LABELS = {
    "current": "現行入口",
    "ai_top10": "AI上位10件",
    "ev_top10": "期待値上位10件",
    "position_limited": "保有数制限",
    "sector_limited": "セクター制限",
    "regime_strict": "地合い厳格化",
}

EXIT_PROFILE_LABELS = {
    "fixed6": "固定6%",
    "fixed7": "固定7%",
    "fixed10": "固定10%",
    "trailing3": "トレーリング3%",
    "trailing5": "トレーリング5%",
    "pullback2": "反落-2%",
    "ma5": "5日MA割れ",
    "rsi70": "RSI70反落",
    "atr15": "ATR 1.5倍",
}

EXIT_TYPE_LABELS = {
    "fixed_tp_sl": "固定利確/損切",
    "time_stop": "期間タイムアウト+事故停止",
    "trailing_stop": "トレーリング",
    "pullback_exit": "反落検知",
    "ma_break_exit": "MA割れ",
    "rsi_reversal_exit": "RSI反落",
    "volume_fade_exit": "出来高減衰",
    "atr_trailing": "ATRトレーリング",
    "peak_pullback_exit": "ピーク反落利確",
    "conditional_extension": "条件付き延長",
    "conditional_extension_with_ban": "条件付き延長（禁止条件あり）",
    "conditional_extension_allow": "条件付き延長（許可条件あり）",
}

CREDIT_PROFILE_LABELS = {
    "no_margin": "",
    "margin_le20": "信用倍率20倍以下",
    "margin_le10": "信用倍率10倍以下",
    "margin_le5": "信用倍率5倍以下",
    "short_pressure": "売り残比率10%以上",
    "margin_range_3_30": "信用倍率3〜30倍",
}


def _case_rule_summary(rules: dict) -> str:
    entry = ENTRY_PROFILE_LABELS.get(str(rules.get("entry_profile") or ""), "")
    exit_profile = EXIT_PROFILE_LABELS.get(str(rules.get("exit_profile") or ""), "")
    parts = []
    if entry:
        parts.append(f"入口は「{entry}」です。")
    allowed = rules.get("allowed_stages") or []
    if allowed:
        stage_labels = {
            "early": "初動",
            "confirmed": "本命",
            "strong_confirmed": "強本命",
        }
        stages = "、".join(stage_labels.get(str(s), str(s)) for s in allowed)
        parts.append(f"対象段階は {stages}。")
    if rules.get("min_ai_score") is not None:
        parts.append(f"AI最低値は {float(rules.get('min_ai_score') or 0) * 100:.0f}%。")
    if rules.get("entry_rank_limit") is not None:
        parts.append(f"候補上位 {int(rules.get('entry_rank_limit') or 0)} 件まで。")
    if rules.get("max_daily_entries") is not None:
        parts.append(f"1日最大 {int(rules.get('max_daily_entries') or 0)} 件。")
    if rules.get("max_open_positions") is not None:
        parts.append(f"最大保有 {int(rules.get('max_open_positions') or 0)} 件。")
    if rules.get("max_sector_positions") not in (None, 99):
        parts.append(f"同一セクターは最大 {int(rules.get('max_sector_positions') or 0)} 件。")
    if rules.get("use_margin_filter"):
        if rules.get("min_margin_ratio") is not None and rules.get("max_margin_ratio") is not None:
            parts.append(
                f"信用倍率は {float(rules.get('min_margin_ratio') or 0):.0f}〜"
                f"{float(rules.get('max_margin_ratio') or 0):.0f} 倍。"
            )
        elif rules.get("max_margin_ratio") is not None:
            parts.append(f"信用倍率は {float(rules.get('max_margin_ratio') or 0):.0f} 倍以下。")
        elif rules.get("min_margin_ratio") is not None:
            parts.append(f"信用倍率は {float(rules.get('min_margin_ratio') or 0):.1f} 倍以上。")
        if rules.get("min_short_long_ratio") is not None:
            parts.append(f"信用売残/買残は {float(rules.get('min_short_long_ratio') or 0) * 100:.0f}% 以上。")
        if rules.get("require_margin_data"):
            parts.append("信用残データがある銘柄だけを対象にします。")

    exit_type = str(rules.get("exit_type") or "fixed_tp_sl")
    if exit_type == "fixed_tp_sl":
        parts.append(
            f"出口は固定利確/損切りで、利確 {float(rules.get('tp_pct') or 0) * 100:.0f}%、"
            f"損切り {float(rules.get('sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "trailing_stop":
        parts.append(
            f"出口はトレーリングで、最高値から {abs(float(rules.get('trailing_drop_pct') or 0)) * 100:.0f}% 下落で決済。"
            f"初期損切りは {float(rules.get('initial_sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "pullback_exit":
        parts.append(
            f"出口は反落検知で、含み益中に前日比 {float(rules.get('pullback_day_pct') or 0) * 100:.0f}% 以下なら決済。"
            f"初期損切りは {float(rules.get('initial_sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "ma_break_exit":
        parts.append(
            f"出口は {int(rules.get('ma_period') or 5)} 日移動平均割れ。"
            f"初期損切りは {float(rules.get('initial_sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "rsi_reversal_exit":
        parts.append(
            f"出口はRSI反落で、RSI {float(rules.get('overbought_rsi') or 70):.0f} 超え後の失速を検知して決済。"
            f"初期損切りは {float(rules.get('initial_sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "atr_trailing":
        parts.append(
            f"出口はATRトレーリングで、ATR x{float(rules.get('atr_multiplier') or 0):.1f} を基準に追随。"
            f"初期損切りは {float(rules.get('initial_sl_pct') or 0) * 100:.0f}%、"
            f"最大 {int(rules.get('max_holding_days') or 0)} 日保有。"
        )
    elif exit_type == "time_stop":
        initial_sl = rules.get("initial_sl_pct")
        stop_text = (
            "事故停止なし"
            if initial_sl is None or float(initial_sl) <= -0.49
            else f"事故停止 {abs(float(initial_sl)) * 100:.0f}%"
        )
        parts.append(
            f"出口は {int(rules.get('max_holding_days') or 0)} 営業日目終値で撤退。{stop_text}、ピーク反落なし。"
        )
    elif exit_type == "conditional_extension":
        initial_sl = rules.get("initial_sl_pct")
        stop_text = (
            "事故停止なし"
            if initial_sl is None or float(initial_sl) <= -0.49
            else f"事故停止 {abs(float(initial_sl)) * 100:.0f}%"
        )
        parts.append(
            f"出口は原則 {int(rules.get('base_holding_days') or 3)} 営業日目終値で撤退。"
            f"day3損益が {float(rules.get('extension_return_threshold_pct') or -1.0):.1f}% 以下なら"
            f"{int(rules.get('extension_holding_days') or 5)} 営業日まで延長。{stop_text}、ピーク反落なし。研究枠です。"
        )
    elif exit_type == "conditional_extension_with_ban":
        initial_sl = rules.get("initial_sl_pct")
        stop_text = (
            "事故停止なし"
            if initial_sl is None or float(initial_sl) <= -0.49
            else f"事故停止 {abs(float(initial_sl)) * 100:.0f}%"
        )
        parts.append(
            f"出口は原則 {int(rules.get('base_holding_days') or 3)} 営業日目終値で撤退。"
            f"day3損益が {float(rules.get('extension_return_threshold_pct') or -1.0):.1f}% 以下なら"
            f"{int(rules.get('extension_holding_days') or 5)} 営業日まで延長します。"
            f"ただし day3損益 {float(rules.get('ban_day3_return_lte_pct') or -3.0):.1f}% 以下、"
            f"上ヒゲ {float(rules.get('ban_day3_upper_shadow_gte_pct') or 1.0):.1f}% 以上、"
            f"RSI {float(rules.get('ban_day3_rsi_min') or 20):.0f}〜{float(rules.get('ban_day3_rsi_max') or 35):.0f} は延長禁止。"
            f"{stop_text}、ピーク反落なし。研究枠です。"
        )
    elif exit_type == "conditional_extension_allow":
        initial_sl = rules.get("initial_sl_pct")
        stop_text = (
            "事故停止なし"
            if initial_sl is None or float(initial_sl) <= -0.49
            else f"事故停止 {abs(float(initial_sl)) * 100:.0f}%"
        )
        parts.append(
            f"出口は原則 {int(rules.get('base_holding_days') or 3)} 営業日目終値で撤退。"
            f"day3損益が {float(rules.get('extension_return_threshold_pct') or -1.0):.1f}% 以下、"
            f"day1損益が {float(rules.get('allow_day1_return_gte_pct') or -2.22):.2f}% 以上、"
            f"day3実体が {float(rules.get('allow_day3_body_lte_pct') or 3.74):.2f}% 以下、"
            f"day3出来高倍率が {float(rules.get('allow_day3_volume_ratio_lte') or 2.0):.1f} 以下なら"
            f"{int(rules.get('extension_holding_days') or 5)} 営業日まで延長。"
            f"{stop_text}、ピーク反落なし。研究枠です。"
        )
    elif exit_type == "peak_pullback_exit":
        initial_sl = rules.get("initial_sl_pct")
        stop_text = (
            "初期損切りなし"
            if initial_sl is None or float(initial_sl) <= -0.49
            else f"事故停止 {float(initial_sl) * 100:.0f}%"
        )
        parts.append(
            f"出口はピーク反落で、entry後高値から {abs(float(rules.get('peak_pullback_pct') or -0.02)) * 100:.0f}% 反落で決済。"
            f"{stop_text}、最大 {int(rules.get('max_holding_days') or 0)} 営業日保有。"
        )
    elif exit_profile:
        parts.append(f"出口は「{exit_profile}」。")
    return "".join(parts)


def _decorate_case_test_case(case):
    if not case:
        return {}
    case = dict(case)
    key = str(case.get("case_key") or "")
    rules = case.get("rules") or {}
    if isinstance(rules, str):
        try:
            import json
            rules = json.loads(rules)
        except Exception:
            rules = {}
    exit_type = str(rules.get("exit_type") or "fixed_tp_sl")
    entry_profile = str(rules.get("entry_profile") or "")
    exit_profile = str(rules.get("exit_profile") or "")
    credit_profile = str(rules.get("credit_profile") or "")
    if entry_profile and exit_profile:
        name_parts = [
            ENTRY_PROFILE_LABELS.get(entry_profile, entry_profile),
            EXIT_PROFILE_LABELS.get(exit_profile, exit_profile),
        ]
        credit_label = CREDIT_PROFILE_LABELS.get(credit_profile, credit_profile)
        if credit_label:
            name_parts.append(credit_label)
        display_name = " × ".join(name_parts)
    else:
        display_name = CASE_TEST_LABELS.get(key) or case.get("case_name") or key
    case["display_name"] = display_name
    case["rules_dict"] = rules
    case["entry_profile"] = entry_profile
    case["entry_label"] = ENTRY_PROFILE_LABELS.get(entry_profile, entry_profile)
    case["exit_profile"] = exit_profile
    case["credit_profile"] = credit_profile
    case["credit_label"] = CREDIT_PROFILE_LABELS.get(credit_profile, credit_profile)
    case["exit_type"] = exit_type
    case["exit_label"] = EXIT_PROFILE_LABELS.get(exit_profile) or EXIT_TYPE_LABELS.get(exit_type, exit_type)
    case["display_description"] = _case_rule_summary(rules)
    case["is_primary_h5"] = bool(rules.get("is_primary_h5"))
    return case


PROFIT_EXIT_REASONS = {
    "tp",
    "trailing_stop",
    "pullback_exit",
    "ma_break_exit",
    "rsi_reversal_exit",
    "volume_fade_exit",
    "atr_trailing",
    "peak_pullback_exit",
}


def _case_test_profit_exit_counts(run_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    start = 0
    page_size = 1000
    while True:
        rows = (
            supabase.table("trade_case_simulations")
            .select("case_id,exit_reason,profit_pct,status")
            .eq("run_id", run_id)
            .range(start, start + page_size - 1)
            .execute()
            .data or []
        )
        for row in rows:
            if row.get("status") != "closed":
                continue
            try:
                profit_pct = float(row.get("profit_pct") or 0)
            except Exception:
                profit_pct = 0
            if row.get("exit_reason") in PROFIT_EXIT_REASONS and profit_pct > 0:
                case_id = str(row.get("case_id"))
                counts[case_id] = counts.get(case_id, 0) + 1
        if len(rows) < page_size:
            break
        start += page_size
    return counts

@app.route("/web/case-tests")
def web_case_tests():
    today = datetime.now(JST).date()
    default_start = (today - timedelta(days=30)).isoformat()
    default_end = today.isoformat()
    selected_run_id = request.args.get("run_id")
    runs: list[dict] = []
    results: list[dict] = []
    cases_by_id: dict[str, dict] = {}
    selected_run = None

    try:
        cases = (
            supabase.table("trade_case_definitions")
            .select("*")
            .order("case_key")
            .execute()
            .data or []
        )
        cases_by_id = {str(c.get("id")): _decorate_case_test_case(c) for c in cases}
        runs = (
            supabase.table("trade_case_runs")
            .select("*")
            .order("started_at", desc=True)
            .limit(20)
            .execute()
            .data or []
        )
        if not selected_run_id and runs:
            selected_run_id = runs[0].get("id")
        selected_run = next((r for r in runs if str(r.get("id")) == str(selected_run_id)), None)
        if selected_run_id:
            results = (
                supabase.table("trade_case_results")
                .select("*")
                .eq("run_id", selected_run_id)
                .order("total_profit_pct", desc=True)
                .execute()
                .data or []
            )
            profit_exit_counts = _case_test_profit_exit_counts(str(selected_run_id))
            for row in results:
                case = cases_by_id.get(str(row.get("case_id")), {})
                row["case"] = case
                row["exit_type"] = case.get("exit_type") or "fixed_tp_sl"
                row["exit_label"] = case.get("exit_label") or row["exit_type"]
                row["profit_exit_count"] = profit_exit_counts.get(str(row.get("case_id")), row.get("tp_count") or 0)
            results.sort(
                key=lambda r: (
                    1 if (r.get("case") or {}).get("is_primary_h5") else 0,
                    float(r.get("total_profit_pct") or -999999),
                    float(r.get("expected_value_pct") or -999999),
                ),
                reverse=True,
            )
    except Exception as e:
        logger.warning("case tests page failed: %s", e)
        msg = str(e)
        if "WinError 10061" in msg or "Connection" in msg or "接続" in msg:
            flash("Supabaseへの接続に失敗しています。ローカル端末からFlaskを起動し直してください。", "danger")
        else:
            flash("比較テスト用テーブルが未作成かもしれません。db/trade_case_tests.sql をSupabaseで実行してください。", "warning")

    return render_template(
        "web/case_tests.html",
        runs=runs,
        results=results,
        selected_run=selected_run,
        default_start=default_start,
        default_end=default_end,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/web/case-tests/run", methods=["POST"])
def web_case_tests_run():
    try:
        start_s = request.form.get("period_start") or ""
        end_s = request.form.get("period_end") or ""
        start = datetime.fromisoformat(start_s).date()
        end = datetime.fromisoformat(end_s).date()
        if end < start:
            flash("終了日は開始日以降にしてください。", "danger")
            return redirect(url_for("web_case_tests"))
        if (end - start).days > 90:
            flash("比較テストは初期実装では90日以内に制限しています。", "danger")
            return redirect(url_for("web_case_tests"))

        from services.trade_case_tester import run_trade_case_test

        result = run_trade_case_test(start, end, sb=supabase)
        flash(f"比較テスト完了: candidates={result.get('candidates')} cases={result.get('cases')}", "success")
        return redirect(url_for("web_case_tests", run_id=result.get("run_id")))
    except Exception as e:
        logger.exception("case test run failed")
        flash(f"比較テスト失敗: {e}", "danger")
        return redirect(url_for("web_case_tests"))


@app.route("/web/case-tests/<run_id>/<case_id>")
def web_case_test_detail(run_id, case_id):
    run = None
    case = None
    rows: list[dict] = []
    try:
        run_rows = supabase.table("trade_case_runs").select("*").eq("id", run_id).limit(1).execute().data or []
        case_rows = supabase.table("trade_case_definitions").select("*").eq("id", case_id).limit(1).execute().data or []
        run = run_rows[0] if run_rows else None
        case = _decorate_case_test_case(case_rows[0]) if case_rows else None
        rows = (
            supabase.table("trade_case_simulations")
            .select("*")
            .eq("run_id", run_id)
            .eq("case_id", case_id)
            .order("entry_date", desc=True)
            .limit(500)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.exception("case test detail failed")
        flash(f"詳細取得失敗: {e}", "danger")
    return render_template(
        "web/case_test_detail.html",
        run=run,
        case=case,
        rows=rows,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/web/research-db")
def web_research_db():
    datasets: list[dict] = []
    snapshots: list[dict] = []
    periods: list[dict] = []
    logs: list[dict] = []
    daily_logs: list[dict] = []
    cron_logs: list[dict] = []
    try:
        datasets = (
            supabase.table("research_datasets")
            .select("*")
            .order("updated_at", desc=True)
            .limit(200)
            .execute()
            .data or []
        )
        snapshots = (
            supabase.table("research_case_snapshots")
            .select("*")
            .order("total_profit_pct", desc=True)
            .limit(200)
            .execute()
            .data or []
        )
        periods = (
            supabase.table("research_periods")
            .select("*")
            .order("period_start", desc=True)
            .execute()
            .data or []
        )
        logs = (
            supabase.table("research_import_logs")
            .select("*")
            .order("started_at", desc=True)
            .limit(100)
            .execute()
            .data or []
        )
        daily_logs = [r for r in logs if r.get("job_type") == "rebound_ai_daily"][:10]
        cron_logs = [r for r in logs if str(r.get("job_type") or "").startswith("cron:")][:20]
    except Exception as e:
        logger.exception("research db page failed")
        flash(f"検証データベースの取得に失敗しました: {e}", "warning")
    return render_template(
        "web/research_db.html",
        datasets=datasets,
        snapshots=snapshots,
        periods=periods,
        logs=logs,
        daily_logs=daily_logs,
        cron_logs=cron_logs,
        market_adjustment=_current_market_adjustment(),
    )


@app.route("/web/research-db/h5-stored-forward-test")
def web_h5_stored_forward_test():
    ctx = load_h5_stored_forward_test_context()
    return render_template("web/h5_stored_forward_test.html", sf=ctx)


@app.route("/web/h5/screenshot-assist/<side>")
def web_h5_screenshot_assist(side):
    if side not in ("buy", "sell"):
        flash("side は buy または sell のみ有効です。", "warning")
        return redirect(url_for("web_dashboard"))
    return render_template("web/h5_screenshot_assist.html", side=side)


@app.route("/web/h5/screenshot-assist/<side>/analyze", methods=["POST"])
def web_h5_screenshot_assist_analyze(side):
    import re as _re
    from datetime import datetime as _dt
    from services.h5_screenshot_assist import (
        allowed_file,
        analyze_sbi_screenshot_with_ai,
        normalize_screenshot_extract,
        validate_screenshot_extract,
        match_buy_h5_candidate,
        match_sell_open_position,
        build_entry_prefill,
        build_exit_prefill,
    )

    if side not in ("buy", "sell"):
        flash("side は buy または sell のみ有効です。", "warning")
        return redirect(url_for("web_dashboard"))

    f = request.files.get("screenshot")
    if not f or not f.filename:
        flash("スクショファイルが選択されていません。", "warning")
        return redirect(url_for("web_h5_screenshot_assist", side=side))

    if not allowed_file(f.filename):
        flash("対応していないファイル形式です（png / jpg / jpeg / webp）。", "warning")
        return redirect(url_for("web_h5_screenshot_assist", side=side))

    data = f.read()
    if len(data) > 10 * 1024 * 1024:
        flash("ファイルサイズが10MBを超えています。", "warning")
        return redirect(url_for("web_h5_screenshot_assist", side=side))

    ext = Path(f.filename).suffix.lstrip(".").lower()
    safe_orig = _re.sub(r"[^\w\-]", "_", Path(f.filename).stem)[:40]
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{ts}_{safe_orig}.{ext}"
    upload_path = SCREENSHOT_UPLOAD_DIR / fname
    SCREENSHOT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(data)

    try:
        raw = analyze_sbi_screenshot_with_ai(upload_path, side, openai_client)
    except Exception as e:
        logger.exception("screenshot AI call failed")
        flash(f"AI Vision 呼び出しに失敗しました: {e}", "warning")
        return redirect(url_for("web_h5_screenshot_assist", side=side))

    result = normalize_screenshot_extract(raw)
    errors, warnings_list = validate_screenshot_extract(result, side)

    if side == "buy":
        match = match_buy_h5_candidate(result)
        prefill = build_entry_prefill(result, match, screenshot_filename=fname)
        entry_gap = prefill.pop("_entry_gap", {})
    else:
        try:
            resp = (
                supabase.table("actual_trade_logs")
                .select("*")
                .eq("actual_exit_status", "holding")
                .execute()
            )
            open_positions = resp.data or []
        except Exception as e:
            logger.warning("screenshot assist: open positions load failed: %s", e)
            open_positions = []
        match = match_sell_open_position(result, open_positions)
        prefill = build_exit_prefill(result, match, screenshot_filename=fname)
        entry_gap = {}

    return render_template(
        "web/h5_screenshot_assist_result.html",
        side=side,
        result=result,
        errors=errors,
        warnings=warnings_list,
        match=match,
        prefill=prefill,
        entry_gap=entry_gap,
        screenshot_filename=fname,
    )


@app.route("/web/research-db/register-existing", methods=["POST"])
def web_research_db_register_existing():
    try:
        from services.research_database import register_existing_datasets

        rows = register_existing_datasets(sb=supabase)
        flash(f"既存データを登録しました: {len(rows)}件", "success")
    except Exception as e:
        logger.exception("research db register-existing failed")
        flash(f"既存データ登録に失敗しました: {e}", "danger")
    return redirect(url_for("web_research_db"))


@app.route("/web/research-db/snapshot-case-results", methods=["POST"])
def web_research_db_snapshot_case_results():
    try:
        run_id = request.form.get("run_id") or ""
        if not run_id:
            latest = (
                supabase.table("trade_case_runs")
                .select("id")
                .eq("status", "completed")
                .order("started_at", desc=True)
                .limit(1)
                .execute()
                .data or []
            )
            run_id = str(latest[0].get("id")) if latest else ""
        if not run_id:
            flash("保存できる比較テスト実行履歴がありません。", "warning")
            return redirect(url_for("web_research_db"))

        from services.research_database import snapshot_case_results

        result = snapshot_case_results(run_id, sb=supabase)
        flash(f"比較テスト結果を保存しました: {result.get('rows')}ケース", "success")
    except Exception as e:
        logger.exception("research db snapshot-case-results failed")
        flash(f"比較テスト結果の保存に失敗しました: {e}", "danger")
    return redirect(url_for("web_research_db"))


@app.route("/web/research-db/add-period", methods=["POST"])
def web_research_db_add_period():
    try:
        payload = {
            "period_key": (request.form.get("period_key") or "").strip(),
            "period_name": (request.form.get("period_name") or "").strip(),
            "regime_type": (request.form.get("regime_type") or "custom").strip(),
            "period_start": (request.form.get("period_start") or "").strip(),
            "period_end": (request.form.get("period_end") or "").strip(),
            "description": (request.form.get("description") or "").strip(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if not payload["period_key"] or not payload["period_name"] or not payload["period_start"] or not payload["period_end"]:
            flash("期間キー、期間名、開始日、終了日は必須です。", "danger")
            return redirect(url_for("web_research_db"))
        if datetime.fromisoformat(payload["period_end"]).date() < datetime.fromisoformat(payload["period_start"]).date():
            flash("終了日は開始日以降にしてください。", "danger")
            return redirect(url_for("web_research_db"))
        supabase.table("research_periods").upsert(payload, on_conflict="period_key").execute()
        flash("相場期間を保存しました。", "success")
    except Exception as e:
        logger.exception("research db add-period failed")
        flash(f"相場期間の保存に失敗しました: {e}", "danger")
    return redirect(url_for("web_research_db"))


@app.route("/admin/models")
@app.route("/web/models")
def web_models():
    try:
        rows = (
            supabase.table("ml_models")
            .select("*")
            .order("created_at", desc=True)
            .limit(50)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.error("models error: %s", e)
        rows = []
    return render_template("web/models.html", rows=rows)


@app.route("/web/portfolio")
def web_portfolio():
    return render_template("web/stub.html", title="ポートフォリオ", message="Phase 3 で実装予定")


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        debug=False,
        use_reloader=False,
    )

