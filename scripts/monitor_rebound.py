#!/usr/bin/env python3
"""
リバウンド監視クロン
cron: 平日 9:00 / 12:00 / 15:30 / 18:00 JST
実行: python scripts/monitor_rebound.py
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

# プロジェクトルート（line_bot/）をパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

try:
    import yfinance as yf
    import pandas as pd
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

import requests as _req
from supabase import create_client
from settings_loader import get_settings

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"


# ─── 環境変数（nikkei_alert.py と同方式）───

def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_mode_upper = (_SUPABASE_MODE or "").upper()
SUPABASE_URL = (_opt(f"SUPABASE_URL_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_URL")
SUPABASE_KEY = (_opt(f"SUPABASE_KEY_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_KEY")
LINE_TOKEN = _opt("LINE_CHANNEL_ACCESS_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise KeyError("SUPABASE_URL / SUPABASE_KEY が未設定です")
if not LINE_TOKEN:
    raise KeyError("LINE_CHANNEL_ACCESS_TOKEN が未設定です")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── テクニカル指標 ───

def _rsi(closes: "pd.Series", period: int = 14) -> float | None:
    """RSI(period) の最新値を返す。データ不足時は None。"""
    if len(closes) < period + 2:
        return None
    delta = closes.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    if loss.iloc[-1] == 0:
        return 100.0
    return round(100 - 100 / (1 + gain.iloc[-1] / loss.iloc[-1]), 1)


def _rsi_series(closes: "pd.Series", period: int = 14) -> "pd.Series":
    """RSI の時系列を返す（NaN → 0 で埋める）。"""
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - 100 / (1 + rs)).fillna(0)


def _biz_days(from_dt: datetime, to_dt: datetime) -> int:
    """土日除外の営業日数（祝日は未考慮）。"""
    days, cur = 0, from_dt.date()
    end = to_dt.date()
    while cur < end:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def _fetch_history(code: str) -> "tuple[pd.Series, pd.Series] | None":
    """(closes, volumes) を返す。3ヶ月分。失敗時は None。"""
    if not HAS_YFINANCE:
        return None
    try:
        hist = yf.Ticker(f"{code}.T").history(period="3mo", interval="1d", auto_adjust=True)
        if len(hist) < 10:
            logger.warning("データ不足: %s (%d件)", code, len(hist))
            return None
        return hist["Close"], hist["Volume"]
    except Exception as e:
        logger.warning("株価取得エラー: %s %s", code, e)
        return None


# ─── リバウンド条件チェック ───

def check_rebound(
    price_at_drop: float | None,
    closes: "pd.Series",
    volumes: "pd.Series",
    cfg: dict,
) -> list[str]:
    """
    リバウンド条件を判定し、成立したシグナル名のリストを返す。
    空リストは「シグナルなし」。
    """
    if len(closes) < 2:
        return []

    current = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])
    triggered: list[str] = []

    # ① 5日移動平均を上抜け（昨日 <= MA5 かつ 今日 > MA5）
    if cfg.get("ma5_cross_enabled", True) and len(closes) >= 5:
        ma5 = float(closes.tail(5).mean())
        if prev <= ma5 < current:
            triggered.append("5日線上抜け")

    # ② 前日比 +N% 以上
    if prev > 0:
        day_pct = (current - prev) / prev * 100
        if day_pct >= cfg.get("daily_rebound_threshold", 3.0):
            triggered.append(f"前日比+{day_pct:.1f}%")

    # ③ 急落価格から +N% 以上
    if price_at_drop and price_at_drop > 0:
        from_drop = (current - price_at_drop) / price_at_drop * 100
        if from_drop >= cfg.get("drop_rebound_threshold", 5.0):
            triggered.append(f"急落時+{from_drop:.1f}%")

    # ④ 出来高が20日平均の N 倍以上（当日を除いた直近20日平均と比較）
    if len(volumes) >= 21:
        vol_avg = float(volumes.iloc[-21:-1].mean())
        vol_now = float(volumes.iloc[-1])
        if vol_avg > 0 and vol_now >= vol_avg * cfg.get("volume_ratio_threshold", 1.5):
            triggered.append(f"出来高{vol_now / vol_avg:.1f}倍")

    # ⑤ RSI: 直近5日以内に rsi_low 以下 → 現在が rsi_recover 以上に回復
    rsi_now = _rsi(closes)
    if rsi_now is not None and rsi_now >= cfg.get("rsi_recover_threshold", 35.0):
        rsi_s = _rsi_series(closes).dropna()
        recent_prev = list(rsi_s.tail(6).values[:-1])  # 前5日
        if any(r < cfg.get("rsi_low_threshold", 30.0) for r in recent_prev):
            triggered.append(f"RSI回復({rsi_now:.0f})")

    return triggered


# ─── LINE 通知 ───

def _push(user_id: str, text: str) -> bool:
    try:
        r = _req.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {LINE_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
        if r.status_code >= 400:
            logger.error("LINE push 失敗: user=%s status=%s", user_id, r.status_code)
            return False
        return True
    except Exception as e:
        logger.error("LINE push 例外: user=%s %s", user_id, e)
        return False


def _resolve_plan(user: dict, now_utc: datetime) -> str:
    """app.py / nikkei_alert.py と同等のプラン判定。"""
    if user.get("membership_status") == "active":
        return "paid"
    trial_at = user.get("trial_started_at")
    if trial_at:
        try:
            dt = datetime.fromisoformat(str(trial_at).replace("Z", "+00:00"))
            if now_utc <= dt + timedelta(days=7):
                return "paid"
        except Exception:
            pass
    ext_until = user.get("trial_extended_until")
    if ext_until:
        try:
            dt = datetime.fromisoformat(str(ext_until).replace("Z", "+00:00"))
            if now_utc <= dt:
                return "paid"
        except Exception:
            pass
    return "free"


def _eligible_users() -> list[dict]:
    """急落通知が有効な有料ユーザーを返す。"""
    try:
        res = supabase.table("users").select(
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active, drop_alert_enabled"
        ).execute()
        now_utc = datetime.now(timezone.utc)
        return [
            u for u in (res.data or [])
            if u.get("active")
            and u.get("drop_alert_enabled")
            and _resolve_plan(u, now_utc) == "paid"
        ]
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return []


def _build_msg(item: dict, signals: list[str], is_strong: bool, current: float) -> str:
    code = item.get("code", "")
    name = item.get("name", "")
    drop_pct = item.get("drop_pct") or 0.0
    price_at_drop = item.get("price_at_drop") or 0.0

    recovery_line = ""
    if price_at_drop > 0:
        rec = (current - price_at_drop) / price_at_drop * 100
        recovery_line = f"現在: {rec:+.1f}%（急落時比）\n"

    sig_lines = "\n".join(f"・{s}" for s in signals)
    strength = "強シグナル★★" if is_strong else "シグナル検知"

    return (
        f"【リバウンド候補】\n"
        f"{code} {name}\n\n"
        f"急落: {drop_pct:+.1f}%\n"
        f"{recovery_line}"
        f"\nシグナル:\n{sig_lines}\n\n"
        f"判定:\n{strength}\n短期反発候補"
    )


# ─── メイン ───

def run_monitor() -> None:
    logger.info("=== リバウンド監視開始 ===")
    now_jst = datetime.now(JST)
    now_utc = datetime.now(timezone.utc)

    if now_jst.weekday() >= 5:
        logger.info("土日のためスキップ")
        return

    if not HAS_YFINANCE:
        logger.error("yfinance が未インストール（pip install yfinance）")
        return

    cfg = get_settings(force_reload=True)
    watch_days_limit = int(cfg.get("watch_days_limit", 10))

    # watching + rebound_signal（前回 push 失敗のリトライ）を取得
    try:
        res = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .in_("status", ["watching", "rebound_signal"])
            .execute()
        )
        watchlist = res.data or []
    except Exception as e:
        logger.error("watchlist 取得失敗: %s", e)
        return

    logger.info("監視対象: %d銘柄", len(watchlist))
    if not watchlist:
        logger.info("監視対象なし。終了")
        return

    # (item, signals, is_strong, current_price) を収集
    to_notify: list[tuple[dict, list[str], bool, float]] = []

    for item in watchlist:
        code = item.get("code", "")
        item_id = item.get("id")
        status = item.get("status", "watching")

        # rebound_signal → 通知失敗リトライ（条件を再チェックして最新シグナルでメッセージ生成）
        if status == "rebound_signal":
            hist = _fetch_history(code)
            if hist:
                closes, volumes = hist
                current = float(closes.iloc[-1])
                signals = check_rebound(item.get("price_at_drop"), closes, volumes, cfg)
                if not signals:
                    signals = ["リバウンドシグナル（継続）"]
                to_notify.append((item, signals, len(signals) >= 2, current))
            else:
                logger.warning("リトライ中の株価取得失敗: %s", code)
            continue

        # ── watching の処理 ──

        # 監視期限チェック
        drop_detected_at = item.get("drop_detected_at")
        if drop_detected_at:
            try:
                drop_dt = datetime.fromisoformat(str(drop_detected_at).replace("Z", "+00:00"))
                biz = _biz_days(drop_dt, now_utc)
                if biz > watch_days_limit:
                    supabase.table("stock_drop_watchlist").update({
                        "status": "closed",
                        "updated_at": now_utc.isoformat(),
                    }).eq("id", item_id).execute()
                    logger.info("監視終了（%d営業日経過）: %s", biz, code)
                    continue
            except Exception as e:
                logger.warning("期限チェックエラー: %s %s", code, e)

        # 株価取得
        hist = _fetch_history(code)
        if hist is None:
            supabase.table("stock_drop_watchlist").update({
                "last_checked_at": now_utc.isoformat(),
                "updated_at": now_utc.isoformat(),
            }).eq("id", item_id).execute()
            continue

        closes, volumes = hist
        current = float(closes.iloc[-1])

        # リバウンド条件チェック
        signals = check_rebound(item.get("price_at_drop"), closes, volumes, cfg)
        is_strong = len(signals) >= 2

        logger.info(
            "チェック: %s signals=%d%s",
            code, len(signals),
            f" [{', '.join(signals)}]" if signals else "",
        )

        update_data: dict = {
            "last_checked_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }
        if signals:
            update_data["status"] = "rebound_signal"
            to_notify.append((item, signals, is_strong, current))

        try:
            supabase.table("stock_drop_watchlist").update(update_data).eq("id", item_id).execute()
        except Exception as e:
            logger.error("watchlist 更新エラー: %s %s", code, e)

    logger.info("リバウンドシグナル: %d銘柄", len(to_notify))

    if not to_notify:
        logger.info("通知対象なし。終了")
        return

    if not cfg.get("rebound_notify_enabled", True):
        logger.info("rebound_notify_enabled=False → LINE 通知スキップ")
        return

    users = _eligible_users()
    logger.info("通知対象ユーザー: %d人", len(users))

    for item, signals, is_strong, current in to_notify:
        msg = _build_msg(item, signals, is_strong, current)
        sent = sum(1 for u in users if _push(u["user_id"], msg))
        logger.info("通知送信: %s → %d人", item.get("code"), sent)

        # push 成功 or 通知対象ユーザーがいない場合は notified に更新
        if sent > 0 or not users:
            try:
                supabase.table("stock_drop_watchlist").update({
                    "status": "notified",
                    "rebound_notified_at": now_utc.isoformat(),
                    "updated_at": now_utc.isoformat(),
                }).eq("id", item.get("id")).execute()
            except Exception as e:
                logger.error("notified 更新エラー: %s %s", item.get("code"), e)

    logger.info("=== リバウンド監視完了 ===")


if __name__ == "__main__":
    run_monitor()
