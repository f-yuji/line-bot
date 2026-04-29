import json
import logging
import os
import re
from html import escape
import requests
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dotenv import load_dotenv
from flask import Flask, request, abort, redirect
from openai import OpenAI
from supabase import create_client

try:
    import stripe
except ImportError:
    stripe = None

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
    URIAction,
    FlexMessage,
    FlexBubble,
    FlexBox,
    FlexButton,
    FlexText,
)
from linebot.v3.webhooks import FollowEvent, MessageEvent, PostbackEvent, TextMessageContent

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
LINE_MODE = _get_optional_env("LINE_MODE")
LINE_CHANNEL_ACCESS_TOKEN = _get_mode_env("LINE_CHANNEL_ACCESS_TOKEN", LINE_MODE, required=True)
LINE_CHANNEL_SECRET = _get_mode_env("LINE_CHANNEL_SECRET", LINE_MODE, required=True)
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ENV = os.getenv("ENV", "prod")
PAYMENT_URL = os.getenv("PAYMENT_URL", "").strip()
APP_BASE_URL = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "").strip()
OWNER_LINE_USER_ID = _get_mode_env("OWNER_LINE_USER_ID", LINE_MODE)
LINE_MEMBERSHIP_USE_API_SYNC = os.getenv("LINE_MEMBERSHIP_USE_API_SYNC", "true").lower() == "true"
LINE_API_BASE = "https://api.line.me"
print("SUPABASE_URL =", SUPABASE_URL)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)
if stripe and STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

app = Flask(__name__)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── ログ ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

print("=== 起動確認 ===")
print(f"環境: {ENV}")
print(f"SUPABASE_MODE: {SUPABASE_MODE or 'legacy'}")
print(f"LINE_MODE: {LINE_MODE or 'legacy'}")
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
    "話題": ["entertainment", "scandal", "other"],
    "スポーツ": ["sports"],
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
    "芸能": "話題",
    "エンタメ": "話題",
    "生活": "暮らし",
    "海外ニュース": "海外",
    "world": "海外",
}


# ─── DB操作 ───
def get_user(user_id: str):
    res = supabase.table("users").select("*").eq("user_id", user_id).execute()
    return res.data[0] if res.data else None


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


def save_user(user_id: str, active=True, plan="free", genres=None, display_name: str = ""):
    if genres is None:
        genres = []
    plan = normalize_plan(plan)

    supabase.table("users").upsert({
        "user_id": user_id,
        "active": active,
        "plan": plan,
        "genres": genres,
        "display_name": display_name,
        "drop_alert_enabled": False,
    }).execute()

    logger.info("Supabase保存: user=%s active=%s plan=%s genres=%s", user_id, active, plan, genres)


def ensure_user(user_id: str):
    user = get_user(user_id)
    if not user:
        display_name = get_line_profile(user_id)
        save_user(user_id, active=True, plan="free", genres=[], display_name=display_name)
        logger.info("新規ユーザー登録: %s display_name=%s", user_id, display_name)
        return {
            "user_id": user_id,
            "active": True,
            "plan": "free",
            "genres": [],
            "extended_trial_ended_notified": False,
            "ai_count": 0,
            "pending_action": None,
            "trial_started_at": None,
            "trial_extended_until": None,
            "feedback_reward_used": False,
            "feedback_pending": False,
            "membership_status": "none",
            "membership_expires_at": None,
            "membership_plan_id": None,
            "membership_last_event_type": None,
            "membership_updated_at": None,
            "free_news_count": 0,
            "free_news_date": None,
            "subsidy_continue_pending": False,
            "drop_alert_enabled": False,
        }, True
    # 既存ユーザー: display_nameを更新
    display_name = get_line_profile(user_id)
    if display_name:
        try:
            supabase.table("users").update({"display_name": display_name}).eq("user_id", user_id).execute()
        except Exception as e:
            logger.error("display_name更新失敗: user=%s %s", user_id, e)

    user["plan"] = normalize_plan(user.get("plan", "free"))
    user.setdefault("ai_count", 0)
    user.setdefault("pending_action", None)
    user.setdefault("trial_started_at", None)
    user.setdefault("trial_extended_until", None)
    user.setdefault("feedback_reward_used", False)
    user.setdefault("feedback_pending", False)
    user.setdefault("extended_trial_ended_notified", False)
    user.setdefault("membership_status", "none")
    user.setdefault("membership_expires_at", None)
    user.setdefault("membership_plan_id", None)
    user.setdefault("membership_last_event_type", None)
    user.setdefault("membership_updated_at", None)
    user.setdefault("last_news_question_targets", None)
    user.setdefault("last_news_question_at", None)
    user.setdefault("last_news_context_sent_at", None)
    user.setdefault("free_news_count", 0)
    user.setdefault("free_news_date", None)
    user.setdefault("subsidy_continue_pending", False)
    user.setdefault("drop_alert_enabled", False)
    return user, False


def set_subsidy_continue_pending(user_id: str, user: dict, pending: bool) -> None:
    user["subsidy_continue_pending"] = pending
    try:
        supabase.table("users").update({"subsidy_continue_pending": pending}).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("subsidy_continue_pending update error user=%s pending=%s %s", user_id, pending, e)


# ─── LINEメンバーシップ ───
def get_line_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_membership_subscription_status(user_id):
    url = f"{LINE_API_BASE}/v2/bot/membership/subscription/{user_id}"
    try:
        res = requests.get(url, headers=get_line_headers(), timeout=10)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logger.error("membership API失敗 user=%s %s", user_id, e)
        return None


def membership_state_from_api(data):
    if not data:
        return {
            "membership_status": "none",
            "membership_expires_at": None,
            "membership_plan_id": None,
        }
    active = bool(data.get("active", False))
    expires = (
        data.get("endTime")
        or data.get("expiredAt")
        or data.get("expiresAt")
        or data.get("membershipExpiresAt")
    )
    plan_id = (
        data.get("planId")
        or data.get("membershipPlanId")
        or data.get("id")
    )
    return {
        "membership_status": "active" if active else "none",
        "membership_expires_at": expires,
        "membership_plan_id": str(plan_id) if plan_id is not None else None,
    }


def membership_state_from_event(event):
    source = event.get("membership") or event
    event_type = (
        event.get("type")
        or source.get("type")
        or source.get("eventType")
        or source.get("membershipEventType")
        or ""
    )
    plan_id = (
        source.get("planId")
        or source.get("membershipPlanId")
        or source.get("id")
    )
    expires_at = (
        source.get("endTime")
        or source.get("expiredAt")
        or source.get("expiresAt")
        or source.get("membershipExpiresAt")
    )
    event_type_l = str(event_type).lower()
    if any(k in event_type_l for k in ["join", "activate", "start", "renew"]):
        status = "active"
    else:
        status = "none"
    return {
        "membership_status": status,
        "membership_expires_at": expires_at,
        "membership_plan_id": str(plan_id) if plan_id is not None else None,
    }


def apply_membership(user_id, state, event_type=None):
    status = state.get("membership_status", "none")
    update = {
        "membership_status": status,
        "membership_expires_at": state.get("membership_expires_at"),
        "membership_plan_id": state.get("membership_plan_id"),
        "membership_last_event_type": event_type,
        "membership_updated_at": datetime.now(timezone.utc).isoformat(),
        "plan": "paid" if status == "active" else "free",
    }

    try:
        supabase.table("users").upsert({"user_id": user_id, **update}).execute()
        logger.info(
            "membership反映成功 user=%s status=%s plan=%s expires=%s plan_id=%s event=%s",
            user_id,
            status,
            update["plan"],
            state.get("membership_expires_at"),
            state.get("membership_plan_id"),
            event_type,
        )
    except Exception as e:
        logger.error("membership反映失敗 user=%s %s", user_id, e)


def sync_membership_from_api(user_id, event_type=None):
    data = get_membership_subscription_status(user_id)
    state = membership_state_from_api(data)
    apply_membership(user_id, state, event_type=event_type)
    return state


def stripe_is_enabled() -> bool:
    return bool(stripe and STRIPE_SECRET_KEY and STRIPE_PRICE_ID and APP_BASE_URL)


def _append_query_params(base_url: str, params: dict[str, str]) -> str:
    parsed = urlsplit(base_url)
    existing = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            continue
        existing[key] = value
    return urlunsplit(parsed._replace(query=urlencode(existing)))


def build_payment_url(user_id: Optional[str] = None) -> str:
    cache_bust = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")

    if PAYMENT_URL and user_id and "{user_id}" in PAYMENT_URL:
        return (
            PAYMENT_URL
            .replace("{user_id}", user_id)
            .replace("{cache_bust}", cache_bust)
            .replace("{billing_base}", APP_BASE_URL)
        )

    if PAYMENT_URL and user_id:
        return _append_query_params(PAYMENT_URL, {
            "user_id": user_id,
            "billing_base": APP_BASE_URL,
            "_cb": cache_bust,
        })

    if stripe_is_enabled() and user_id:
        return f"{APP_BASE_URL}/billing?{urlencode({'user_id': user_id, '_cb': cache_bust})}"

    if PAYMENT_URL:
        joiner = "&" if "?" in PAYMENT_URL else "?"
        return f"{PAYMENT_URL}{joiner}_cb={cache_bust}"

    return PAYMENT_URL


def _stripe_timestamp_to_iso(value) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except Exception:
        return None


def _stripe_to_dict(value):
    if isinstance(value, dict):
        return {k: _stripe_to_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_stripe_to_dict(v) for v in value]
    if hasattr(value, "_data"):
        return _stripe_to_dict(getattr(value, "_data"))
    if hasattr(value, "to_dict_recursive"):
        try:
            return value.to_dict_recursive()
        except Exception:
            pass
    return value


def _stripe_customer_id_from_subscription(subscription_id: str) -> str:
    if not stripe or not subscription_id:
        return ""
    subscription = stripe.Subscription.retrieve(subscription_id)
    subscription_data = _stripe_to_dict(subscription) or {}
    customer_id = subscription_data.get("customer")
    return str(customer_id or "")


def _membership_state_from_stripe_subscription(subscription) -> dict:
    subscription = _stripe_to_dict(subscription) or {}
    sub_status = str(subscription.get("status", "")).lower()
    # cancel_at_period_end / canceled_at は見ない。
    # status が終了系（canceled / unpaid / incomplete_expired 等）になって初めて none へ落とす。
    # 解約予約中（cancel_at_period_end=true）でも current_period_end までは paid を維持する。
    is_active = sub_status in {"trialing", "active", "past_due"}
    return {
        "membership_status": "active" if is_active else "none",
        "membership_expires_at": _stripe_timestamp_to_iso(subscription.get("current_period_end")),
        "membership_plan_id": subscription.get("id"),
    }


def _resolve_user_id_from_checkout_session(session) -> str:
    session = _stripe_to_dict(session) or {}
    metadata = session.get("metadata") or {}
    return metadata.get("line_user_id") or session.get("client_reference_id") or ""


def build_billing_manage_url(user_id: str) -> str:
    if not APP_BASE_URL or not user_id:
        return ""
    return f"{APP_BASE_URL}/billing/manage?{urlencode({'user_id': user_id})}"


def _billing_page_html(user_id: str, canceled: bool = False) -> str:
    safe_user_id = escape(user_id)
    cancel_message = ""
    if canceled:
        cancel_message = (
            "<p class='notice'>決済はキャンセルされました。準備ができたらもう一度進めてOKです。</p>"
        )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>メンバーシップ登録</title>
  <style>
    :root {{
      color-scheme: light;
      --bg1: #fff8ef;
      --bg2: #f5fbff;
      --card: rgba(255, 255, 255, 0.92);
      --text: #1f2937;
      --muted: #5b6472;
      --accent: #ff6b35;
      --border: rgba(31, 41, 55, 0.08);
      --shadow: 0 18px 48px rgba(15, 23, 42, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Yu Gothic UI", "Hiragino Sans", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, #ffe3cf 0, transparent 36%),
        radial-gradient(circle at bottom right, #d8f3ff 0, transparent 32%),
        linear-gradient(135deg, var(--bg1), var(--bg2));
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    .card {{
      width: min(100%, 560px);
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 32px 24px;
      backdrop-filter: blur(8px);
    }}
    h1 {{
      font-size: 30px;
      line-height: 1.2;
      margin: 0 0 12px;
    }}
    p {{
      margin: 0 0 16px;
      line-height: 1.7;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
    }}
    .box {{
      margin: 20px 0;
      padding: 16px;
      border-radius: 18px;
      background: #fff;
      border: 1px solid var(--border);
    }}
    .notice {{
      color: #9a3412;
      background: #fff7ed;
      border: 1px solid #fed7aa;
      padding: 12px 14px;
      border-radius: 14px;
    }}
    button {{
      width: 100%;
      border: 0;
      border-radius: 999px;
      padding: 16px 20px;
      font-size: 16px;
      font-weight: 700;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(135deg, var(--accent), #ff8a5b);
      box-shadow: 0 12px 24px rgba(255, 107, 53, 0.28);
    }}
    ul {{
      padding-left: 20px;
      margin: 0;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <main class="card">
    <p class="sub">LINE user: {safe_user_id}</p>
    <h1>メンバーシップを開始</h1>
    <p>Stripeの安全な決済画面に移動します。登録が完了すると、有料機能を順次使えるようになります。</p>
    {cancel_message}
    <div class="box">
      <ul>
        <li>最新ニュースを深掘りで読める</li>
        <li>人物トピックの会話機能を開放</li>
        <li>より広い範囲の質問に対応</li>
      </ul>
    </div>
    <form method="post" action="/stripe/create-checkout-session">
      <input type="hidden" name="user_id" value="{safe_user_id}">
      <button type="submit">Stripeで登録する</button>
    </form>
    <p class="sub" style="margin-top:14px;">登録後はこの画面を閉じて、LINEに戻って使い始めてください。</p>
  </main>
</body>
</html>"""


def _billing_already_active_html(user_id: str) -> str:
    safe_user_id = escape(user_id)
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>すでに有効です</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      font-family: "Yu Gothic UI", "Hiragino Sans", sans-serif;
      background: linear-gradient(135deg, #effaf4, #eef6ff);
      color: #1f2937;
    }}
    .card {{
      width: min(100%, 520px);
      background: rgba(255,255,255,0.94);
      border-radius: 28px;
      padding: 32px 24px;
      box-shadow: 0 18px 48px rgba(15, 23, 42, 0.12);
      text-align: center;
    }}
    h1 {{ margin: 0 0 12px; font-size: 30px; }}
    p {{ line-height: 1.8; }}
    .sub {{ color: #5b6472; font-size: 14px; }}
  </style>
</head>
<body>
  <main class="card">
    <h1>すでにメンバーシップ有効中</h1>
    <p>このLINEアカウントでは、すでに有料機能を使える状態になっています。LINEに戻ってそのまま使ってOKです。</p>
    <p class="sub">LINE user: {safe_user_id}</p>
  </main>
</body>
</html>"""


def _billing_manage_done_html(user_id: str) -> str:
    safe_user_id = escape(user_id)
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>手続き完了</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      font-family: "Yu Gothic UI", "Hiragino Sans", sans-serif;
      background: linear-gradient(135deg, #f8fafc, #eef6ff);
      color: #1f2937;
    }}
    .card {{
      width: min(100%, 520px);
      background: rgba(255,255,255,0.94);
      border-radius: 28px;
      padding: 32px 24px;
      box-shadow: 0 18px 48px rgba(15, 23, 42, 0.12);
      text-align: center;
    }}
    h1 {{ margin: 0 0 12px; font-size: 30px; }}
    p {{ line-height: 1.8; }}
    .sub {{ color: #5b6472; font-size: 14px; }}
  </style>
</head>
<body>
  <main class="card">
    <h1>手続きページを閉じてOK</h1>
    <p>Stripe上の手続きが終わったら、この画面を閉じてLINEに戻ってくれ。状態反映には少しだけ時間がかかることがある。</p>
    <p class="sub">LINE user: {safe_user_id}</p>
  </main>
</body>
</html>"""


def _billing_page_html_clean(user_id: str, canceled: bool = False) -> str:
    safe_user_id = escape(user_id)
    cancel_message = ""
    if canceled:
        cancel_message = (
            "<p class='notice'>決済はキャンセルされました。内容を確認して、準備ができたらもう一度お進みください。</p>"
        )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LINEニュースBot メンバーシップ登録</title>
  <style>
    :root {{
      color-scheme: light;
      --page: #f6f7f9;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #667085;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --line: #d9dee7;
      --soft: #eef6f5;
      --shadow: 0 16px 44px rgba(15, 23, 42, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 20px;
      color: var(--text);
      background: var(--page);
      font-family: "Yu Gothic UI", "Hiragino Sans", sans-serif;
    }}
    .card {{
      width: min(100%, 620px);
      padding: 28px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }}
    .brand {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding-bottom: 18px;
      margin-bottom: 24px;
      border-bottom: 1px solid var(--line);
    }}
    .brand-name {{
      font-size: 15px;
      font-weight: 700;
    }}
    .badge {{
      padding: 7px 11px;
      color: var(--accent-dark);
      background: var(--soft);
      border: 1px solid #cce7e3;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 26px;
      line-height: 1.35;
    }}
    p {{
      margin: 0 0 16px;
      line-height: 1.7;
    }}
    .lead {{
      color: #344054;
      font-size: 15px;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
    }}
    .notice {{
      padding: 12px 14px;
      color: #9a3412;
      background: #fff7ed;
      border: 1px solid #fed7aa;
      border-radius: 8px;
    }}
    .box {{
      margin: 20px 0;
      padding: 18px;
      background: #fbfcfd;
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .checklist {{
      display: grid;
      gap: 11px;
      margin: 0;
      padding: 0;
      color: #344054;
      font-size: 14px;
      list-style: none;
    }}
    .checklist li {{
      display: grid;
      grid-template-columns: 22px 1fr;
      gap: 10px;
      align-items: start;
    }}
    .checklist li::before {{
      content: "✓";
      display: grid;
      place-items: center;
      width: 22px;
      height: 22px;
      color: #fff;
      background: var(--accent);
      border-radius: 999px;
      font-size: 13px;
      font-weight: 700;
    }}
    .meta {{
      display: grid;
      gap: 8px;
      margin: 18px 0 22px;
      padding: 14px;
      color: var(--muted);
      background: #f8fafc;
      border: 1px solid var(--line);
      border-radius: 8px;
      font-size: 13px;
    }}
    .meta-row {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
    }}
    .meta strong {{
      color: var(--text);
      font-weight: 700;
      overflow-wrap: anywhere;
    }}
    button {{
      width: 100%;
      padding: 16px 20px;
      color: #fff;
      background: var(--accent);
      border: 0;
      border-radius: 8px;
      box-shadow: 0 10px 22px rgba(15, 118, 110, 0.24);
      cursor: pointer;
      font-size: 16px;
      font-weight: 700;
    }}
    button:hover {{
      background: var(--accent-dark);
    }}
    .footnote {{
      margin-top: 14px;
      margin-bottom: 0;
      text-align: center;
    }}
    @media (max-width: 520px) {{
      body {{ padding: 12px; }}
      .card {{ padding: 22px 18px; }}
      .brand {{
        align-items: flex-start;
        flex-direction: column;
        gap: 10px;
      }}
      h1 {{ font-size: 23px; }}
      .meta-row {{
        flex-direction: column;
        gap: 2px;
      }}
    }}
  </style>
</head>
<body>
  <main class="card">
    <div class="brand">
      <div>
        <div class="brand-name">LINEニュースBot</div>
        <div class="sub">メンバーシップ登録</div>
      </div>
      <div class="badge">Stripe決済</div>
    </div>
    <h1>有料メンバーシップを開始します</h1>
    <p class="lead">このあとStripeの決済画面へ移動します。登録が完了すると、LINEニュースBotの有料機能がこのLINEアカウントで利用できます。</p>
    {cancel_message}
    <div class="box">
      <ul class="checklist">
        <li>最新ニュースの深掘り解説を利用できます</li>
        <li>人物・企業・市場テーマについて会話できます</li>
        <li>登録内容の変更や解約はStripeの管理画面から行えます</li>
      </ul>
    </div>
    <div class="meta">
      <div class="meta-row"><span>登録先LINE user</span><strong>{safe_user_id}</strong></div>
      <div class="meta-row"><span>決済処理</span><strong>Stripe Checkout</strong></div>
    </div>
    <form method="post" action="/stripe/create-checkout-session">
      <input type="hidden" name="user_id" value="{safe_user_id}">
      <button type="submit">Stripeの決済画面へ進む</button>
    </form>
    <p class="sub footnote">カード情報はこのページでは保存せず、Stripeの安全な画面で入力します。</p>
  </main>
</body>
</html>"""


@app.route("/billing", methods=["GET"])
def billing_page():
    user_id = (request.args.get("user_id") or "").strip()
    canceled = request.args.get("canceled") == "1"

    if not user_id:
        return "user_id is required", 400

    if not stripe_is_enabled():
        fallback_url = build_payment_url()
        if fallback_url:
            return redirect(fallback_url, code=302)
        return "Stripe is not configured", 503

    user, _ = ensure_user(user_id)
    effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
    if effective_plan == "paid":
        return _billing_already_active_html(user_id)

    return _billing_page_html_clean(user_id, canceled=canceled)


@app.route("/stripe/create-checkout-session", methods=["POST"])
def create_checkout_session():
    if not stripe_is_enabled():
        return "Stripe is not configured", 503

    user_id = (request.form.get("user_id") or request.args.get("user_id") or "").strip()
    if not user_id:
        return "user_id is required", 400

    user, _ = ensure_user(user_id)
    effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
    if effective_plan == "paid":
        return "already active", 409

    success_url = f"{APP_BASE_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{APP_BASE_URL}/billing?{urlencode({'user_id': user_id, 'canceled': '1'})}"

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            client_reference_id=user_id,
            allow_promotion_codes=True,
            metadata={"line_user_id": user_id},
            subscription_data={"metadata": {"line_user_id": user_id}},
        )
    except Exception as e:
        logger.error("Stripe checkout session error user=%s %s", user_id, e)
        return "Failed to create Stripe session", 500

    return redirect(session.url, code=303)


@app.route("/billing/success", methods=["GET"])
def billing_success():
    session_id = (request.args.get("session_id") or "").strip()
    user_id = ""

    if stripe_is_enabled() and session_id:
        try:
            session = stripe.checkout.Session.retrieve(session_id, expand=["subscription"])
            session_data = _stripe_to_dict(session) or {}
            user_id = _resolve_user_id_from_checkout_session(session)
            subscription = session_data.get("subscription")
            if user_id and subscription:
                apply_membership(
                    user_id,
                    _membership_state_from_stripe_subscription(subscription),
                    event_type="stripe.checkout.session.completed",
                )
        except Exception as e:
            logger.error("Stripe success lookup error session=%s %s", session_id, e)

    safe_user_id = escape(user_id or "確認中")
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>登録完了</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      font-family: "Yu Gothic UI", "Hiragino Sans", sans-serif;
      background: linear-gradient(135deg, #f7fee7, #ecfeff);
      color: #1f2937;
    }}
    .card {{
      width: min(100%, 520px);
      background: rgba(255,255,255,0.94);
      border-radius: 28px;
      padding: 32px 24px;
      box-shadow: 0 18px 48px rgba(15, 23, 42, 0.12);
      text-align: center;
    }}
    h1 {{ margin: 0 0 12px; font-size: 30px; }}
    p {{ line-height: 1.7; }}
    .sub {{ color: #5b6472; font-size: 14px; }}
  </style>
</head>
<body>
  <main class="card">
    <h1>登録ありがとうございます</h1>
    <p>決済完了を受け付けました。LINEに戻って、ニュースや会話機能をそのまま使ってみてください。</p>
    <p class="sub">LINE user: {safe_user_id}</p>
  </main>
</body>
</html>"""


@app.route("/billing/manage", methods=["GET"])
def billing_manage():
    user_id = (request.args.get("user_id") or "").strip()
    if not user_id:
        return "user_id is required", 400

    if not stripe_is_enabled():
        return "Stripe is not configured", 503

    user, _ = ensure_user(user_id)
    effective_plan = resolve_effective_plan(user, datetime.now(timezone.utc))
    if effective_plan != "paid":
        return "membership is not active", 409

    subscription_id = str(user.get("membership_plan_id") or "")
    if not subscription_id:
        return "subscription is not linked", 409

    try:
        customer_id = _stripe_customer_id_from_subscription(subscription_id)
        if not customer_id:
            return "customer is not linked", 409

        # Billing Portal の解約操作は「期間終了時に解約」(cancel_at_period_end) 前提。
        # 即時解約ではなく current_period_end まで有料機能を使える。
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{APP_BASE_URL}/billing/manage/done?{urlencode({'user_id': user_id})}",
        )
    except Exception as e:
        logger.error("Stripe portal session error user=%s %s", user_id, e)
        return "Failed to create portal session", 500

    return redirect(session.url, code=303)


@app.route("/billing/manage/done", methods=["GET"])
def billing_manage_done():
    user_id = (request.args.get("user_id") or "").strip()
    return _billing_manage_done_html(user_id)


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    if not stripe or not STRIPE_WEBHOOK_SECRET:
        return "Stripe webhook is not configured", 503

    payload = request.get_data()
    signature = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, signature, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        abort(400)
    except stripe.error.SignatureVerificationError:
        abort(400)

    event_data = _stripe_to_dict(event) or {}
    event_type = event_data.get("type", "")
    data_object = (event_data.get("data") or {}).get("object") or {}

    try:
        if event_type == "checkout.session.completed":
            user_id = _resolve_user_id_from_checkout_session(data_object)
            if user_id:
                apply_membership(
                    user_id,
                    {
                        "membership_status": "active",
                        "membership_expires_at": None,
                        "membership_plan_id": data_object.get("subscription"),
                    },
                    event_type=event_type,
                )
        elif event_type in {"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"}:
            user_id = (data_object.get("metadata") or {}).get("line_user_id")
            if user_id:
                apply_membership(
                    user_id,
                    _membership_state_from_stripe_subscription(data_object),
                    event_type=event_type,
                )
    except Exception as e:
        logger.error("Stripe webhook handler error type=%s %s", event_type, e)
        return "webhook handler error", 500

    return "ok", 200


def handle_membership_event(event):
    user_id = event.get("source", {}).get("userId") or event.get("userId")
    if not user_id:
        logger.warning("membership event user_id不明: %s", event)
        return
    event_type = (
        event.get("type")
        or (event.get("membership") or {}).get("type")
        or (event.get("membership") or {}).get("eventType")
        or (event.get("membership") or {}).get("membershipEventType")
        or "membership"
    )
    _, _ = ensure_user(user_id)
    if LINE_MEMBERSHIP_USE_API_SYNC:
        sync_membership_from_api(user_id, event_type=event_type)
    else:
        state = membership_state_from_event(event)
        apply_membership(user_id, state, event_type=event_type)


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
        QuickReplyItem(action=MessageAction(label="会話ネタ", text="会話ネタ")),
        QuickReplyItem(action=MessageAction(label="補助金", text="補助金")),
        QuickReplyItem(action=MessageAction(label="使い方", text="使い方")),
        QuickReplyItem(action=MessageAction(label="停止", text="停止")),
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


def reply_with_payment(reply_token: str, text: str, quick_reply: QuickReply = None) -> None:
    """テキスト＋課金ボタン（Flex）を1回のreplyで送る。PAYMENT_URL未設定時は通常テキスト返信にフォールバック。"""
    if not PAYMENT_URL:
        reply_text(reply_token, text, quick_reply)
        return
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            text_msg = TextMessage(text=text)
            payment_bubble = FlexBubble(
                body=FlexBox(
                    layout="vertical",
                    contents=[
                        FlexButton(
                            action=URIAction(label="機能を開放する", uri=PAYMENT_URL),
                            style="primary",
                            height="sm",
                        )
                    ],
                )
            )
            flex_msg = FlexMessage(alt_text="機能を開放する", contents=payment_bubble)
            if quick_reply:
                flex_msg.quick_reply = quick_reply
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[text_msg, flex_msg])
            )
    except Exception as e:
        logger.error("LINE課金ボタン返信エラー: %s", e)


def reply_with_payment_for_user(reply_token: str, user_id: str, text: str, quick_reply: QuickReply = None) -> None:
    payment_url = build_payment_url(user_id)
    if not payment_url:
        reply_text(reply_token, text, quick_reply)
        return
    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            text_msg = TextMessage(text=text)
            payment_bubble = FlexBubble(
                body=FlexBox(
                    layout="vertical",
                    contents=[
                        FlexButton(
                            action=URIAction(label="機能を開放する", uri=payment_url),
                            style="primary",
                            height="sm",
                        )
                    ],
                )
            )
            flex_msg = FlexMessage(alt_text="機能を開放する", contents=payment_bubble)
            if quick_reply:
                flex_msg.quick_reply = quick_reply
            api.reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[text_msg, flex_msg])
            )
    except Exception as e:
        logger.error("LINE課金ボタン返信エラー: %s", e)


GENRE_DESC = {
    "経済": "金利・為替・不動産",
    "仕事": "業界・法改正・労働",
    "国際": "海外情勢・外交",
    "AI・テック": "AI・IT・科学",
    "暮らし": "医療・教育・生活",
    "話題": "芸能・SNS・流行",
    "スポーツ": "メジャー・マイナー",
    "海外": "米・英・韓・亜・印の現地ニュース",
}


def build_genre_flex(current_genres: list) -> FlexMessage:
    rows = []
    layout_rows = [
        ["経済", "仕事"],
        ["国際", "AI・テック"],
        ["暮らし", "話題"],
        ["スポーツ", "海外"],
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
                        },
                        "required": ["index", "headline", "p1", "p2", "p3"],
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
    )
    user_prompt = (
        f"以下のニュース記事を深掘りしろ:\n{news_text}\n\n"
        f"指定番号: {nums}\n"
        "全記事について必ず headline / p1 / p2 / p3 を埋めろ。\n"
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


_CHAT_TOPIC_FOLLOW_UP_FREE = (
    "\n\n相手に合わせた話題は\n"
    "メンバーシップで使える"
)

_CHAT_TOPIC_FOLLOW_UP_PAID = (
    "\n\nちなみに今日誰かと話す予定ある？\n\n"
    "誰と話すか教えてくれれば\n"
    "その人に合わせて話題出すよ。\n"
)

# ─── 会話ネタ共通 ───

_CHAT_TOPIC_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "chat_topic",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "genre":  {"type": "string"},
                "main":   {"type": "string"},
                "reply":  {"type": "string"},
                "next":   {"type": "string"},
                "point":  {"type": "string"},
                "trivia": {"type": "string"},
            },
            "required": ["genre", "main", "reply", "next", "point", "trivia"],
            "additionalProperties": False,
        },
    },
}

_CHAT_TOPIC_SYSTEM_BASE = """\
お前はLINEで使える会話ネタを提案するやつ。

【ルール】
・会話調
・そのまま使える形で出す
・短すぎず薄くしない
・AIっぽい文章禁止
・汎用テンプレ禁止。「今っぽさ」を優先

【季節ルール】
・現在の季節を必ず考慮する（日本基準）
・季節とズレた会話は禁止（例：夏に寒い話）
・自然に会話に溶け込ませる（説明しない）

【ジャンル選択】
生活関連 / 仕事関連 / 世界関連 / AI関連 / 話題関連 / スポーツ関連
※迷ったら「話題関連」
※「お金」は使用禁止（生活関連に含める）

【会話フレーズ】
・必ずニュース内容と具体的に接続する（「〇〇が〜したって聞いた？」「〇〇って影響出てきてるけど、どう思う？」のような形）
・そのまま口に出せる自然な一文にする
・問いかけ or 感想共有の形にする
・抽象的な表現は禁止（「色々あるね」「最近多いよね」のような中身のない感想は使わない）

【よくある返し】
・現実的な返答。短く自然

【次の一手】
・会話を広げる or 深める。自然に続く一言

【使いやすいポイント】
・1行のみ。具体的に

【小ネタ】
・1文のみ。20〜30文字。「らしい」「っぽい」で柔らかく

【NG】
・カテゴリ名（例：経済ニュース）
・説明文
・長文
・季節ズレ
・中身のない雑談
・「色々あるね」などの抽象的感想
・ニュースと接続していない汎用フレーズ

必ずJSON形式のみで出力すること。キー：genre / main / reply / next / point / trivia
"""


def format_topic(data: dict) -> str:
    """会話ネタの dict → LINEメッセージ文字列"""
    return (
        f"【{data['genre']}】\n"
        f"「{data['main']}」\n\n"
        "ーーー\n\n"
        f"よくある返し\n『{data['reply']}』\n\n"
        f"→「{data['next']}」\n\n"
        "ーーー\n\n"
        f"・使いやすいポイント\n{data['point']}\n\n"
        "ーーー\n\n"
        f"小ネタ\n《{data['trivia']}》"
    )


def _get_season() -> str:
    month = datetime.now(timezone.utc).month
    if month in (3, 4, 5):
        return "春"
    elif month in (6, 7, 8):
        return "夏"
    elif month in (9, 10, 11):
        return "秋"
    else:
        return "冬"


def generate_chat_topic_free(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    system_prompt = _CHAT_TOPIC_SYSTEM_BASE + f"\n現在の季節：{season}"
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んでJSONで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data) + _CHAT_TOPIC_FOLLOW_UP_FREE
    except Exception as e:
        logger.error("会話ネタ生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


def generate_chat_topic_paid(user_id: str) -> str:
    ctx = get_latest_news_context(user_id)
    if not ctx:
        return "まだニュースが届いてないから\n一度配信受けてから使ってみて"
    news_items = ctx.get("payload", {}).get("news_items", [])
    news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    system_prompt = _CHAT_TOPIC_SYSTEM_BASE + f"\n現在の季節：{season}"
    user_prompt = f"今日のニュース:\n{news_text}\n\nこの中から会話ネタになりそうなものを1つ選んでJSONで出せ。"
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data) + _CHAT_TOPIC_FOLLOW_UP_PAID
    except Exception as e:
        logger.error("生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"


_POLITE_TONE_RULE = (
    "【口調ルール】\n"
    "・会話文は自然な敬語にする\n"
    "・タメ語は禁止\n"
    "・先輩、上司、取引先、お客さん相手として不自然な軽さは禁止\n"
    "・一人称は基本使わない\n"
    "・『私』『あたし』は禁止\n"
    "・日本人男性ユーザーがそのまま使って違和感のない自然な言い回しにする\n"
    "・堅すぎるビジネス文書調は禁止\n"
    "・実際の雑談で使える、柔らかい丁寧語にする\n"
)

_CASUAL_TONE_RULE = (
    "【口調ルール】\n"
    "・友達や恋人に話すような自然なくだけた口調でよい\n"
    "・ただし荒い口調は禁止\n"
    "・一人称は基本使わない\n"
    "・『私』『あたし』は禁止\n"
    "・日本人男性ユーザーがそのまま使って違和感のない自然な言い回しにする\n"
    "・馴れ馴れしすぎる作り物っぽいテンションは禁止\n"
)


def infer_tone_for_person(person_desc: str) -> str:
    """相手の説明文からカジュアル/丁寧を判定する。"""
    polite_keywords = [
        "上司", "部下", "先輩", "後輩", "同僚", "職場", "会社",
        "仕事相手", "営業", "営業先", "取引先", "お客", "顧客", "客",
        "初対面", "現場", "近所", "元請け", "社長",
    ]
    casual_keywords = [
        "彼女", "彼氏", "好きな人", "気になる人",
        "嫁", "妻", "旦那", "夫","友達",
        "友達", "親友", "知人",
        "家族", "兄弟", "姉妹", "親", "母親", "父親",
    ]
    if any(k in person_desc for k in polite_keywords):
        return "polite"
    if any(k in person_desc for k in casual_keywords):
        return "casual"
    return "polite"


def generate_chat_for_person(user_id: str, person_desc: str) -> str:
    ctx = get_latest_news_context(user_id)
    news_text = ""
    if ctx:
        news_items = ctx.get("payload", {}).get("news_items", [])
        news_text = "\n".join(f"【{n['category']}】{n['title']}" for n in news_items)
    season = _get_season()
    tone = infer_tone_for_person(person_desc)
    tone_rule = _POLITE_TONE_RULE if tone == "polite" else _CASUAL_TONE_RULE
    system_prompt = (
        _CHAT_TOPIC_SYSTEM_BASE
        + f"\n現在の季節：{season}\n\n"
        "【最重要】\n"
        "・出力するのは『ユーザー本人が相手に送るセリフ』として自然であること\n"
        "・相手に合わせた口調を最優先すること\n"
        "・彼女/友達向けなのに女口調にしない\n"
        "・先輩/上司/取引先向けなのにタメ口にしない\n"
        "・ユーザー側のセリフで『私』『あたし』は禁止\n"
        "・日本人男性がそのまま使って違和感のない表現にする\n"
        "・恋人向けでもベタすぎる作り物感は禁止\n"
        "・先輩向けは敬語ベースだが、固すぎる文章は禁止\n\n"
        "【相手ごとの優先ジャンル】\n"
        "彼女・彼氏・家族・友達：日常、食べ物、休日、エンタメ、軽い流行、身近なニュースを優先。"
        "地政学・軍事・金利政策のような重い話題は基本避ける\n"
        "上司・取引先・営業・顧客・初対面：無難で広げやすい話題を優先。"
        "仕事・景気・AI・生活コスト・スポーツ・季節ネタを優先。"
        "政治・宗教・下ネタ・重すぎる事件は避ける\n\n"
        "【追加ルール】\n"
        "・ニュースに縛られず相手に最適な話題を選ぶ\n"
        "・相手に合わない重い話題は避ける\n"
        "・ニュースを使う場合も、そのまま出さず会話向けに軽く変換する\n\n"
        + tone_rule
    )
    user_prompt = (
        f"話す相手: {person_desc}\n"
        "話す側: 日本人男性ユーザー\n"
        "目的: その相手と自然に会話を始めて、返答が返しやすいネタを出す\n"
        "絶対条件: 相手に失礼のない口調にする。彼女向けで女口調禁止。先輩向けでタメ口禁止。\n\n"
        + (f"参考ニュース（必要なら使っていい）:\n{news_text}\n\n" if news_text else "")
        + "この相手に使えそうな会話ネタを1つJSONで出せ。"
    )
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=15,
            response_format=_CHAT_TOPIC_SCHEMA,
        )
        data = json.loads(res.choices[0].message.content)
        return format_topic(data)
    except Exception as e:
        logger.error("相手別会話生成エラー: %s", e)
        return "今ちょっとうまく生成できない\n少し置いてもう一回送って"



def can_use_paid_ai(user: dict, effective_plan: str) -> bool:
    return effective_plan == "paid" and user.get("ai_count", 0) < 30


def get_paid_usage_tail(ai_count_after: int) -> str:
    if ai_count_after == 25:
        return "\n\n今日だいぶ使ってるな\nあと少しだな"
    if ai_count_after == 28:
        return "\n\n今日かなり使ってるな\nあとちょっとで上限だ"
    return ""


def _set_pending_person_topic(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": "person_topic",
            "pending_count": 1,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _clear_pending(user_id: str) -> None:
    try:
        supabase.table("users").update({
            "pending_action": None,
            "pending_count": None,
        }).eq("user_id", user_id).execute()
    except Exception:
        pass


def _is_pending_person_topic(user: dict) -> bool:
    return (
        user.get("pending_action") == "person_topic"
        and (user.get("pending_count") or 0) > 0
    )


def increment_ai_count(user_id: str, count: int, today: str, now_dt) -> None:
    try:
        supabase.table("users").update({
            "ai_count": count + 1,
            "ai_count_date": today,
            "last_reply_time": now_dt.isoformat(),
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
    try:
        payload = json.loads(body)
    except Exception:
        payload = {}

    events = payload.get("events", []) if isinstance(payload, dict) else []

    for ev in events:
        if isinstance(ev, dict) and ("membership" in ev or ev.get("type") == "membership"):
            try:
                handle_membership_event(ev)
            except Exception as e:
                logger.error("membership event処理失敗: %s", e)

    return "OK"


@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    user, _ = ensure_user(user_id)

    # トライアル開始日が未設定なら登録（新規ユーザー or システム導入前の既存ユーザー）
    if not user.get("trial_started_at"):
        _now = datetime.now(timezone.utc)
        try:
            supabase.table("users").update({
                "trial_started_at": _now.isoformat(),
                "feedback_reward_used": False,
                "feedback_pending": False,
            }).eq("user_id", user_id).execute()
        except Exception as e:
            logger.error("trial_started_at設定失敗: %s", e)

    reply_text(
        event.reply_token,
        "追加ありがとう\n\n"
        "ニュースは朝に届く\n"
        "寝てる時間帯だからミュートでもOK👌\n\n"
        "配信とは別で、ボタンから追加ニュースも見れるよ\n\n"
        "まずは「使い方」見てみて",
        quick_reply=main_quick_reply(),
    )


_STOP_WORDS = {"停止", "止めて", "停止して", "配信止めて", "もういい", "オフ"}
_START_WORDS = {"開始", "スタート", "再開", "もう一回", "配信して", "オン", "はじめて", "始めて"}
_GENRE_WORDS = {"ジャンル", "ジャンル変えたい", "ジャンル変える", "ジャンル設定", "ジャンル選びたい", "設定したい"}
_MEMBERSHIP_KW = [
    "メンバー", "有料", "課金",
    "いくら", "料金", "値段",
    "入り方", "登録",
    "何できる", "何ができる",
]
_MEMBERSHIP_CANCEL_WORDS = {"解約", "キャンセル", "退会"}


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
_CHAT_TOPIC_KW = ["会話ネタ", "話のネタ", "雑談ネタ", "ネタ教えて", "何話せばいい", "何話す"]
_CHAT_TOPIC_EXACT = {"会話", "会話ネタ", "雑談", "ネタ"}
_PERSON_KW = [
    # 恋愛・パートナー
    "彼女", "彼氏", "好きな人", "気になる人", "嫁", "妻", "旦那", "夫",
    # 友人・知人
    "友達", "親友", "知人",
    # 職場・ビジネス
    "上司", "部下", "先輩", "後輩", "同僚", "職場の人", "会社の人", "仕事相手",
    "営業", "営業先", "取引先", "お客さん", "顧客", "客", "初対面",
    # 関係が難しい人
    "きまずい人", "苦手な人", "微妙な人", "仲悪い人", "距離ある人",
    # 家族
    "親", "母親", "父親", "家族", "兄弟", "姉妹",
    # その他
    "現場の人", "近所の人",
]
_DIRECT_TOPIC_KW = [
    "会話", "話す", "話題", "雑談", "ネタ",
    "何話", "なに話", "何しゃべ", "何喋",
    "使える話題", "向けの話題", "どう話す", "どう会話する",
]


def looks_like_feedback(text: str) -> bool:
    import re

    t = (text or "").strip()
    if not t:
        return False

    # 数字だけ
    if re.fullmatch(r"[0-9０-９]+", t):
        return False

    # 記号だけ
    if re.fullmatch(r"[\W_。、，,！？!?ー〜～…]+", t):
        return False

    # NG短文
    short_ng = {"はい", "いいえ", "ok", "OK", "了解", "うん", "いや"}
    if t in short_ng:
        return False

    # テンプレ項目
    markers = ["使いやすさ", "よかった点", "微妙だった点", "あったらいい機能"]
    if any(m in t for m in markers):
        return True

    # 感想ワード
    feedback_words = [
        "使いやすい", "使いにくい",
        "わかりやすい", "分かりやすい",
        "わかりにくい", "分かりにくい",
        "よかった", "良かった",
        "微妙", "いい", "悪い",
        "便利", "不便",
        "見やすい", "見づらい",
        "難しい", "むずい",
    ]
    if any(w in t for w in feedback_words):
        return True

    # 最低10文字以上
    return len(t) >= 10


def is_direct_person_chat_request(text: str) -> bool:
    """会話ネタ意図＋相手説明を含む一発入力を判定する（辞書不要の自由記述対応）。"""
    # 会話ネタ意図がなければ問答無用でFalse
    topic_hit = any(k in text for k in _DIRECT_TOPIC_KW)
    if not topic_hit:
        return False

    # 辞書一致があればTrue
    if any(kw in text for kw in _PERSON_KW):
        return True

    # 「と」「向け」「相手」があればTrue
    if any(p in text for p in ("と", "向け", "相手")):
        return True

    # 会話ワードを除いた残り文字が4文字以上あればTrue（自由記述）
    stripped = text
    for k in _DIRECT_TOPIC_KW:
        stripped = stripped.replace(k, "")
    return len(stripped.strip()) >= 4


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
    "会話ネタ",
    "会話",
    "雑談",
    "ネタ",
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
    "急落通知オン",
    "急落通知オフ",
}

_STRONG_COMMANDS = (
    _STOP_WORDS
    | _START_WORDS
    | _GENRE_WORDS
    | _STATUS_WORDS
    | _HELP_WORDS
    | _NEWS_TRIGGER_KW
    | _MAIN_COMMANDS
)


def _plan_label(plan: str, membership_status: str) -> str:
    if membership_status == "active":
        return "本会員"
    if normalize_plan(plan) == "paid":
        return "トライアル中"
    return "無料プラン"


def _plan_status_text(
    plan: str,
    active: bool,
    genres: list,
    membership_status: str = "none",
    drop_alert_enabled: bool = False,
) -> str:
    genre_label = f"ジャンル: {format_genres(genres)}" if genres else "ジャンル: 未設定（全部配信）"
    active_label = "配信：オン" if active else "配信：オフ"
    drop_alert_label = "急落通知：オン" if drop_alert_enabled else "急落通知：オフ"
    return f"今こんな感じ\n\n{_plan_label(plan, membership_status)}\n{active_label}\n{genre_label}\n{drop_alert_label}"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = normalize_user_text(event.message.text)

    logger.info("メッセージ受信: user=%s text=%s", user_id, text)
    user, _ = ensure_user(user_id)

    now_dt = datetime.now(timezone.utc)

    _base_plan = normalize_plan(user.get("plan", "free"))
    if _base_plan == "free" and not user.get("trial_started_at"):
        try:
            supabase.table("users").update({
                "trial_started_at": now_dt.isoformat(),
            }).eq("user_id", user_id).execute()
            user["trial_started_at"] = now_dt.isoformat()
        except Exception as e:
            logger.error("trial_started_at補完失敗: %s", e)

    # ── 軽い連投制限（3秒）──
    _last = user.get("last_reply_time")
    if _last is not None:
        try:
            _last_dt = datetime.fromisoformat(str(_last).replace("Z", "+00:00"))
            if now_dt - _last_dt < timedelta(seconds=3):
                return
        except Exception:
            pass

    # ── 日付リセット ──
    today = datetime.now(timezone.utc).date().isoformat()

    if user.get("ai_count_date") != today:
        try:
            supabase.table("users").update({
                "ai_count": 0,
                "ai_count_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["ai_count"] = 0
        user["ai_count_date"] = today

    if user.get("free_news_date") != today:
        try:
            supabase.table("users").update({
                "free_news_count": 0,
                "free_news_date": today,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        user["free_news_count"] = 0
        user["free_news_date"] = today

    plan = resolve_effective_plan(user, now_dt)
    active = user.get("active", True)
    genres = user.get("genres", [])
    drop_alert_enabled = bool(user.get("drop_alert_enabled", False))
    qr = main_quick_reply()

    # ─── フィードバック受理（最優先・トライアル延長）───
    if user.get("feedback_pending") and not user.get("feedback_reward_used"):
        if looks_like_feedback(text):
            try:
                supabase.table("trial_feedbacks").insert({
                    "user_id": user_id,
                    "feedback": text,
                    "created_at": now_dt.isoformat(),
                }).execute()
            except Exception as e:
                logger.error("trial_feedbacks insert失敗: %s", e)

            try:
                supabase.table("users").update({
                    "trial_extended_until": (now_dt + timedelta(days=7)).isoformat(),
                    "feedback_reward_used": True,
                    "feedback_pending": False,
                }).eq("user_id", user_id).execute()
            except Exception as e:
                logger.error("延長付与失敗: %s", e)

            user["trial_extended_until"] = (now_dt + timedelta(days=7)).isoformat()
            user["feedback_reward_used"] = True
            user["feedback_pending"] = False

            reply_text(
                event.reply_token,
                "ありがとう\n感想ちゃんと受け取った\n\n"
                "freeでも使えるけど\n"
                "メンバーシップで機能を開放してる\n\n"
                "お礼であと1週間はこの状態で使えるようにした",
                quick_reply=qr,
            )
            return

    # ── 強コマンドは pending を無条件クリアしてから通常処理 ──
    if text in _STRONG_COMMANDS and user.get("pending_action"):
        _clear_pending(user_id)
        user["pending_action"] = None
        user["pending_count"] = None

    if text in _STOP_WORDS:
        supabase.table("users").update({"active": False}).eq("user_id", user_id).execute()
        clear_last_news_question_targets(user_id)
        reply_text(event.reply_token, "配信を停止した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text in _START_WORDS:
        supabase.table("users").update({"active": True}).eq("user_id", user_id).execute()
        reply_text(event.reply_token, "配信再開した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "急落通知オン":
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "急落通知は有料会員向けの機能\n\nメンバーシップで使えるようになる",
                quick_reply=qr,
            )
        else:
            supabase.table("users").update({"drop_alert_enabled": True}).eq("user_id", user_id).execute()
            user["drop_alert_enabled"] = True
            reply_text(event.reply_token, "急落株通知をオンにした", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "急落通知オフ":
        supabase.table("users").update({"drop_alert_enabled": False}).eq("user_id", user_id).execute()
        user["drop_alert_enabled"] = False
        reply_text(event.reply_token, "急落株通知をオフにした", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if any(w in text for w in _MEMBERSHIP_CANCEL_WORDS):
        if plan != "paid":
            reply_text(
                event.reply_token,
                "いま有効な課金はないよ\n\n必要になったらまた「登録」で始めればOK",
                quick_reply=qr,
            )
        else:
            manage_url = build_billing_manage_url(user_id)
            if not manage_url:
                reply_text(
                    event.reply_token,
                    "解約ページのURLがまだ作れない状態\n\n.env の APP_BASE_URL を確認してくれ",
                    quick_reply=qr,
                )
            else:
                reply_text(
                    event.reply_token,
                    "解約はこのページからできる\n"
                    "解約しても次回更新日まではそのまま使える\n"
                    "期間終了後に自動でfreeへ戻る\n\n"
                    f"{manage_url}",
                    quick_reply=qr,
                )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if any(kw in text for kw in _MEMBERSHIP_KW):
        if any(w in text for w in ["いくら", "料金", "値段"]):
            reply_text(
                event.reply_token,
                "月400円でやってる\n\n"
                "メンバーシップだと\n"
                "・ニュースを深掘りできる\n"
                "・会話ネタの返しまで出せる\n"
                "・相手に合わせた話題も出せる",
                quick_reply=qr,
            )
        elif any(w in text for w in ["入り方", "登録"]):
            if plan == "paid":
                reply_text(
                    event.reply_token,
                    "すでにメンバーシップ有効中\n\nLINEに戻ってそのまま使ってOK",
                    quick_reply=qr,
                )
                return
            payment_url = build_payment_url(user_id)
            _reg_url = f"\n\n👇ここから入れる\n{payment_url}" if payment_url else ""
            reply_with_payment_for_user(
                event.reply_token,
                user_id,
                "メンバーシップってやつで見れるようにしてる" + _reg_url,
                quick_reply=qr,
            )
        elif any(w in text for w in ["何できる", "何ができる"]):
            if plan == "paid":
                _msg = (
                    "今使えるのはこんな感じ\n\n"
                    "・ニュースを深掘りできる\n"
                    "・会話ネタの返しまで出せる\n"
                    "・相手に合わせた話題も出せる\n\n"
                    "そのまま送ればOK"
                )
            else:
                _msg = (
                    "無料でも使えるけど\n\n"
                    "メンバーシップだと\n"
                    "・ニュースを深掘りできる\n"
                    "・会話ネタの返しまで出せる\n"
                    "・相手に合わせた話題も出せる\n\n"
                    "気になるならそのまま聞いてくれればOK\n\n"
                    "ちなみに\n"
                    "メンバーシップはここから",
                )
            reply_text(
                event.reply_token,
                _msg,
                quick_reply=qr,
            )
        else:
            reply_text(
                event.reply_token,
                "無料でも使えるけど\n\n"
                "メンバーシップだと\n"
                "・ニュースを深掘りできる\n"
                "・会話ネタの返しまで出せる\n"
                "・相手に合わせた話題も出せる\n\n"
                "気になるならそのまま聞いてくれればOK\n\n"
                "ちなみに\n"
                "メンバーシップはここから",
                quick_reply=qr,
            )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    if text == "使い方":
        if plan == "paid":
            _help_text = (
                "使い方ガイド（メンバーシップ）\n\n"
                "【できること】\n"
                "・毎朝ニュース要約配信\n"
                "・優良株急落通知\n\n"
                "ニュース → 今日のニュースを見る\n"
                "会話ネタ → 話題に使えるネタを見る\n"
                "リンク → 直近ニュースのURLを見る\n"
                "補助金 → 今使える制度を探す\n"
                "相場 → 市場の動きを見る\n"
                "急落株 → 急落銘柄を見る\n"
                "急落通知オン → 急落株通知を受け取る\n"
                "急落通知オフ → 急落株通知を止める\n"
                "停止 → 配信停止\n"
                "再開 → 配信再開\n"
                "状態 → 設定確認\n\n"
                "――――――\n\n"
                "【ニュース活用】\n"
                "1詳しく → 記事を深掘り\n"
                "〇〇ってなに？ → 用語解説\n"
                "今後どうなる？ → 追加質問OK\n\n"
                "――――――\n\n"
                "【解約するには】\n"
                "解約 → 自動更新を止める\n"
                "解約後も次回更新日までは使える\n\n"
                "――――――\n\n"
                "【今の状態】\n"
                f"プラン：{_plan_label(plan, user.get('membership_status', 'none'))}\n"
                f"配信：{'オン' if active else 'オフ'}\n"
                f"ジャンル：{format_genres(genres) if genres else '全ジャンル'}\n"
                f"急落通知：{'オン' if drop_alert_enabled else 'オフ'}"
            )
        else:
            _help_text = (
                "使い方ガイド（無料プラン）\n\n"
                "【使える機能】\n"
                "・毎朝ニュース要約配信\n\n"
                "ニュース → 今日のニュースを見る\n"
                "追加ニュース → 1日2回まで\n"
                "リンク → 直近ニュースのURLを見る\n"
                "停止 → 配信停止\n"
                "再開 → 配信再開\n"
                "状態 → 利用状況を見る\n\n"
                "――――――\n\n"
                "【月額298円で使える機能】\n"
                "・追加ニュース 無制限\n"
                "・記事を深掘り\n"
                "・ニュースに質問できる\n"
                "・会話ネタ作成\n"
                "・補助金検索\n"
                "・相場チェック\n"
                "・急落株チェック\n"
                "・優良株急落通知\n"
                "・ジャンル設定\n\n"
                "気になる方は「登録」と入力\n\n"
                "――――――\n\n"
                "【今の状態】\n"
                f"プラン：{_plan_label(plan, user.get('membership_status', 'none'))}\n"
                f"配信：{'オン' if active else 'オフ'}\n"
                f"ジャンル：{format_genres(genres) if genres else '全ジャンル'}\n"
                f"急落通知：{'オン' if drop_alert_enabled else 'オフ'}"
            )
        # ジャンルFlexと使い方本文 (+ 登録ボタン) を1回のreplyにまとめる
        try:
            with ApiClient(configuration) as api_client:
                api = MessagingApi(api_client)
                _msgs_to_send = [
                    build_genre_flex(genres),
                    TextMessage(text=_help_text),
                ]
                # 課金ボタンは free ユーザーかつ PAYMENT_URL が設定されている時のみ表示
                payment_url = build_payment_url(user_id)
                if payment_url and plan != "paid":
                    _payment_bubble = FlexBubble(
                        body=FlexBox(
                            layout="vertical",
                            contents=[
                                FlexButton(
                                    action=URIAction(label="機能を開放する", uri=payment_url),
                                    style="primary",
                                    height="sm",
                                )
                            ],
                        )
                    )
                    _payment_flex = FlexMessage(alt_text="機能を開放する", contents=_payment_bubble)
                    _payment_flex.quick_reply = qr
                    _msgs_to_send.append(_payment_flex)
                else:
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

    if text == "プラン":
        reply_text(
            event.reply_token,
            _plan_status_text(
                plan,
                active,
                genres,
                user.get("membership_status", "none"),
                drop_alert_enabled,
            ),
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
                "ジャンル認識できなかった\n例: ジャンル 経済,AI・テック,スポーツ",
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
        reply_text(event.reply_token, _plan_status_text(plan, active, genres, user.get("membership_status", "none")), quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ─── トライアル終了通知（初回のみ）───
    _base_plan = normalize_plan(user.get("plan", "free"))
    if (plan == "free" and _base_plan == "free"
            and user.get("trial_started_at")
            and not user.get("feedback_pending")
            and not user.get("feedback_reward_used")):
        reply_with_payment_for_user(
            event.reply_token,
            user_id,
            "トライアルはここまで\n\n"
            "free会員でもニュースは見れるけど\n"
            "メンバーシップなら、会話ネタとか深掘りまで全部使える\n\n"
            "よければ感想もらえたら\n"
            "もう1週間トライアルが延長できる\n\n"
            "コピペして軽くでOK👇\n\n"
            "【使いやすさ】\n"
            "（例：使いやすい / わかりにくい）\n\n"
            "【よかった点】\n\n"
            "【微妙だった点】\n\n"
            "【あったらいい機能】\n\n"
            "一言でもOK",
            quick_reply=qr,
        )
        try:
            supabase.table("users").update({
                "feedback_pending": True,
            }).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ─── 延長トライアル終了通知（1回のみ）───
    _base_plan = normalize_plan(user.get("plan", "free"))
    extended_until = user.get("trial_extended_until")
    if (
        plan == "free"
        and _base_plan == "free"
        and user.get("feedback_reward_used") == True
        and extended_until
        and not user.get("extended_trial_ended_notified")
    ):
        try:
            ext_dt = datetime.fromisoformat(str(extended_until).replace("Z", "+00:00"))
        except Exception:
            ext_dt = None

        if ext_dt and now_dt > ext_dt:
            try:
                supabase.table("users").update({
                    "extended_trial_ended_notified": True
                }).eq("user_id", user_id).execute()
            except Exception:
                pass

            user["extended_trial_ended_notified"] = True

            reply_with_payment_for_user(
                event.reply_token,
                user_id,
                "延長分はここまで\n\n"
                "freeでもニュースは見れるけど\n"
                "メンバーシップなら、会話ネタとか深掘りまで全部使える\n\n"
                "よかったらこのまま続けてみて",
                quick_reply=qr,
            )
            return

    # ★2.4 相手別会話ネタ直発火（「彼女と会話」のような一発入力）
    if is_direct_person_chat_request(text):
        if plan != "paid":
            reply_text(
                event.reply_token,
                "相手別の会話ネタはトライアルかメンバーシップで使える\n\n無料版はニュース受け取りまで",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if not can_use_paid_ai(user, plan):
            reply_text(
                event.reply_token,
                "今日はここまでにしよ\nまた明日使えるようになってる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer = generate_chat_for_person(user_id, text)
        next_count = user.get("ai_count", 0) + 1
        reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
        increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★2.5 会話ネタ完全一致トリガー（Q&Aより前に処理）
    if text in _CHAT_TOPIC_EXACT or any(kw in text for kw in _CHAT_TOPIC_KW):
        if plan != "paid":
            reply_text(
                event.reply_token,
                "会話ネタはトライアルかメンバーシップで使える\n\n無料版はニュース受け取りまで",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if not can_use_paid_ai(user, plan):
            reply_text(
                event.reply_token,
                "今日はここまでにしよ\nまた明日使えるようになってる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        answer = generate_chat_topic_paid(user_id)
        next_count = user.get("ai_count", 0) + 1
        reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
        _set_pending_person_topic(user_id)
        increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★3 相手別会話ネタ（paid only）— pending中の次の1ターン限定
    if plan == "paid" and _is_pending_person_topic(user):
        _qa_would_fire = bool(re.match(r"^[①-⑩1-9]", text)) or is_related_to_news_context(user_id, text)
        if not _qa_would_fire:
            if not can_use_paid_ai(user, plan):
                reply_text(
                    event.reply_token,
                    "今日はここまでにしよ\nまた明日使えるようになってる",
                    quick_reply=qr,
                )
                _clear_pending(user_id)
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
            answer = generate_chat_for_person(user_id, text)
            next_count = user.get("ai_count", 0) + 1
            reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
            _clear_pending(user_id)
            increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
            return
        # _qa_would_fire の場合は ★7 Q&A 側でクリア

    # ★補助金 都道府県入力待ち
    if _user_subsidy_state.get(user_id) == "await_prefecture":
        _KNOWN_CMDS = {"急落株", "急落", "急落銘柄", "相場", "補助金", "助成金", "ニュース", "停止", "配信停止", "都道府県変更", "業種変更"}
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
                    reply_text(
                        event.reply_token,
                        format_subsidy_page(items, pref, user.get("subsidy_category")),
                        quick_reply=qr,
                    )
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

    # ★補助金 業種入力待ち
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
            reply_text(
                event.reply_token,
                format_subsidy_page(items, user.get("subsidy_prefecture"), text),
                quick_reply=qr,
            )
        except Exception as e:
            logger.error("補助金一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★急落株一覧
    if text in {"急落株", "急落", "急落銘柄"}:
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "急落株速報は有料会員向けの機能\n\nメンバーシップで使えるようになる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        try:
            drops, nikkei_pct, fetched_at, stale_fallback = get_drop_list_for_reply()
            if fetched_at is None:
                reply_text(
                    event.reply_token,
                    "急落株データがまだない\n前場寄り後か後場引け後の更新を待って",
                    quick_reply=qr,
                )
                return
            _user_drop_list[user_id] = drops
            reply_text(
                event.reply_token,
                format_drop_list_text(drops, nikkei_pct, fetched_at, stale_fallback),
                quick_reply=qr,
            )
        except Exception as e:
            logger.error("急落株一覧取得エラー: %s", e)
            reply_text(event.reply_token, "データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★急落株 銘柄コード / 銘柄名入力
    stock_from_text = _resolve_drop_stock_from_text(user_id, text)
    if stock_from_text:
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "個別解説は有料会員向けの機能",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        try:
            nikkei_pct = get_nikkei_change_pct()
            stock = stock_from_text if stock_from_text.get("price") is not None else get_single_stock_change(stock_from_text["code"])
            if not stock:
                reply_text(event.reply_token, "銘柄データ取得に失敗した\nしばらく待ってから試して", quick_reply=qr)
                return
            change_pct = stock["change_pct"] if stock else None
            comment = get_stock_ai_comment(stock["code"], stock["name"], change_pct, nikkei_pct)
            company_profile = format_company_profile_text(stock["code"])
            company_block = f"{company_profile}\n\n" if company_profile else ""
            reply_text(
                event.reply_token,
                f"{stock['code']} {stock['name']}\n"
                f"{company_block}"
                f"取得: {stock.get('fetched_at', '-')}\n\n"
                f"価格   {stock['price']:,.0f}円\n"
                f"前日比 {_format_day_change_text(stock.get('price'), stock.get('day_pct'))}\n"
                f"週次   {(f'{stock.get('week_pct'):+.1f}%') if stock.get('week_pct') is not None else 'N/A'}\n"
                f"月次   {(f'{stock.get('month_pct'):+.1f}%') if stock.get('month_pct') is not None else 'N/A'}\n"
                f"高値差 {(f'{stock.get('from_high_pct'):+.1f}%') if stock.get('from_high_pct') is not None else 'N/A'}\n\n{comment}",
                quick_reply=qr,
            )
        except Exception as e:
            logger.error("急落株銘柄入力エラー: %s", e)
            reply_text(event.reply_token, "解説取得に失敗した", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★相場
    if text == "相場":
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "相場機能は有料会員向けの機能\n\nメンバーシップで使えるようになる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
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

    # ★補助金一覧
    if text in {"補助金", "助成金"}:
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "補助金・助成金機能は有料会員向けの機能\n\nメンバーシップで使えるようになる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
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
        if plan != "paid":
            reply_with_payment_for_user(
                event.reply_token, user_id,
                "補助金・助成金機能は有料会員向けの機能\n\nメンバーシップで使えるようになる",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

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
        reply_text(
            event.reply_token,
            format_subsidy_page(items, pref, cat, offset=offset, page_size=SUBSIDY_PAGE_SIZE),
            quick_reply=qr,
        )
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★補助金 都道府県変更
    if text == "都道府県変更":
        _user_subsidy_state[user_id] = "await_prefecture"
        reply_text(event.reply_token, "都道府県を入力してください\n例：東京　神奈川　大阪", quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★補助金 業種変更
    if text == "業種変更":
        _user_subsidy_state[user_id] = "await_category"
        reply_text(event.reply_token, "業種を選んでください", quick_reply=_subsidy_category_quick_reply())
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★4 ニュース取得トリガー
    if text in _NEWS_TRIGGER_KW:
        _user_drop_list.pop(user_id, None)
        if plan != "paid":
            logger.info("free_news_count user=%s count=%s", user_id, user.get("free_news_count", 0))
            if user.get("free_news_count", 0) >= 2:
                reply_text(event.reply_token, "無料版の追加ニュースは今日はここまで\nまた明日見れる", quick_reply=qr)
                try:
                    supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
                except Exception:
                    pass
                return
        _exclude = get_recent_sent_links(user_id, article_limit=30)
        messages, _ = fetch_news_for_reply(user_id, exclude_links=_exclude)
        if not messages:
            reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
        else:
            reply_text(event.reply_token, messages[0], quick_reply=qr)
            if plan != "paid":
                new_count = user.get("free_news_count", 0) + 1
                try:
                    supabase.table("users").update({
                        "free_news_count": new_count,
                        "free_news_date": today,
                    }).eq("user_id", user_id).execute()
                    user["free_news_count"] = new_count
                except Exception:
                    pass
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★5 blocklist
    if any(w in text for w in _BLOCKLIST):
        reply_text(event.reply_token, _BLOCKLIST_TEXT, quick_reply=qr)
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★6 リンク
    if is_link_request(text):
        batch_items = get_last_news_batch(user_id)
        if batch_items:
            logger.info("★6リンク: last_news_batch取得済み items=%d件 user=%s", len(batch_items), user_id)
            # items が1件以上ある場合はリンク一覧のみ返す（本文再送なし）
            reply_text(event.reply_token, _build_link_message(batch_items), quick_reply=qr)
            logger.info("★6リンク: リンク一覧返却ルート user=%s", user_id)
        else:
            logger.info("★6リンク: last_news_batch未取得(None or 空) user=%s → 本文返却ルート", user_id)
            # last_news_batch が存在しない or items が空の初回のみ: 本文 → リンク
            messages, news_filtered = fetch_news_for_reply(user_id)
            if not messages:
                reply_text(event.reply_token, "今ちょっとニュース取れなかった\n少し時間おいてまた試して", quick_reply=qr)
            else:
                reply_text(event.reply_token, messages[0], quick_reply=qr)
                # save_last_news_batch は fetch_news_for_reply 内で済んでいるが、
                # ここでは news_filtered を直接使いリンク一覧を push（DB再取得なし）
                if news_filtered:
                    _link_items = [
                        {"index": i + 1, "title": n.get("title", ""), "link": n.get("link", "")}
                        for i, n in enumerate(news_filtered)
                    ]
                    logger.info("★6リンク: 本文後リンク push items=%d件 user=%s", len(_link_items), user_id)
                    _push_text(user_id, _build_link_message(_link_items))
        try:
            supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
        except Exception:
            pass
        return

    # ★7 ニュースQ&A

    # 「詳しく」単体 → 直前の対象記事に再発火（pending機能）
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

    # ★7-a 番号入力はすべて深掘りに統一（単体番号・番号+キーワード 問わず）
    _nums = parse_detail_request(text)
    if _nums:
        logger.info("★7-a 深掘り新仕様ルート: user=%s nums=%s text=%r", user_id, _nums, text)

        if plan != "paid":
            reply_text(
                event.reply_token,
                "ニュースの詳しい解説はトライアルかメンバーシップで使える\n\n無料版はニュース受け取りまで",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        if not can_use_paid_ai(user, plan):
            reply_text(event.reply_token, "今日はここまでにしよ\nまた明日使えるようになってる", quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return
        if user.get("pending_action"):
            _clear_pending(user_id)
            user["pending_action"] = None
            user["pending_count"] = None
        answer = answer_detail_new(user_id, _nums)
        next_count = user.get("ai_count", 0) + 1
        reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
        increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        save_last_news_question_targets(user_id, _nums, get_latest_news_context(user_id))
        return

    # ★7-b 自然文Q&A（番号系入力は ★7-a で処理済み）
    logger.info("★7-b 自然文Q&Aルート: user=%s text=%r", user_id, text)
    _matched_by_ctx = is_related_to_news_context(user_id, text)

    if _matched_by_ctx:
        if not is_news_question(text) and not _looks_like_question_or_command(text):
            reply_text(event.reply_token, _REJECT_TEXT, quick_reply=qr)
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        if plan != "paid":
            reply_text(
                event.reply_token,
                "ニュースの質問や深掘りはトライアルかメンバーシップで使える\n\n無料版はニュース受け取りまで",
                quick_reply=qr,
            )
            try:
                supabase.table("users").update({"last_reply_time": now_dt.isoformat()}).eq("user_id", user_id).execute()
            except Exception:
                pass
            return

        if not can_use_paid_ai(user, plan):
            reply_text(event.reply_token, "今日はここまでにしよ\nまた明日使えるようになってる", quick_reply=qr)
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
        next_count = user.get("ai_count", 0) + 1
        reply_text(event.reply_token, answer + get_paid_usage_tail(next_count), quick_reply=qr)
        increment_ai_count(user_id, user.get("ai_count", 0), today, now_dt)
        if _q_targets:
            save_last_news_question_targets(user_id, _q_targets, get_latest_news_context(user_id))
        return


    # ★9 fallback
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


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        debug=False,
        use_reloader=False,
    )
