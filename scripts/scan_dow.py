#!/usr/bin/env python3
"""
DJIA（ダウ平均）構成銘柄 急落検知・watchlist 保存・LINE 通知
cron: 平日 6:30 JST（NY市場引け後）
実行: python scripts/scan_dow.py
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
load_dotenv()

try:
    import yfinance as yf
    import pandas as pd
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

import requests as _req
from supabase import create_client
from settings_loader import get_settings
from dow_stocks import DOW30

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_mode_upper = (_SUPABASE_MODE or "").upper()
_IS_TEST = _opt("ENV").upper() == "TEST"
SUPABASE_URL = (_opt(f"SUPABASE_URL_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_URL")
SUPABASE_KEY = (_opt(f"SUPABASE_KEY_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_KEY")
LINE_TOKEN = _opt("LINE_CHANNEL_ACCESS_TOKEN")
_WEB_URL = _opt("WEB_URL") or "https://line-bot-ukz5kw.fly.dev/web/dashboard"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise KeyError("SUPABASE_URL / SUPABASE_KEY が未設定です")
if not LINE_TOKEN:
    raise KeyError("LINE_CHANNEL_ACCESS_TOKEN が未設定です")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _push(user_id: str, text: str) -> bool:
    try:
        r = _req.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
        return r.status_code < 400
    except Exception as e:
        logger.error("LINE push error: %s", e)
        return False


def _resolve_plan(user: dict, now_utc: datetime) -> str:
    if user.get("membership_status") == "active":
        return "paid"
    for key, days in [("trial_started_at", 7), ("trial_extended_until", 0)]:
        val = user.get(key)
        if val:
            try:
                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                if days:
                    if now_utc <= dt + timedelta(days=days):
                        return "paid"
                else:
                    if now_utc <= dt:
                        return "paid"
            except Exception:
                pass
    return "free"


def _eligible_users() -> list[dict]:
    try:
        now = datetime.now(timezone.utc)
        res = supabase.table("users").select(
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active"
        ).execute()
        return [
            u for u in (res.data or [])
            if u.get("active") and _resolve_plan(u, now) == "paid"
        ]
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return []


def _get_dow_change_pct() -> float | None:
    """ダウ平均の前日比（%）を取得"""
    try:
        hist = yf.Ticker("^DJI").history(period="5d", interval="1d", auto_adjust=True)
        closes = hist["Close"].dropna()
        if len(closes) >= 2:
            return round((float(closes.iloc[-1]) - float(closes.iloc[-2])) / float(closes.iloc[-2]) * 100, 2)
    except Exception as e:
        logger.warning("ダウ平均取得失敗: %s", e)
    return None


def _batch_day_change(tickers: list[str]) -> dict[str, float]:
    """ティッカーリスト → {ticker: day_pct}"""
    if not tickers:
        return {}
    result: dict[str, float] = {}
    try:
        data = yf.download(
            tickers,
            period="5d",
            interval="1d",
            auto_adjust=True,
            group_by="ticker",
            progress=False,
            threads=True,
        )
        for ticker in tickers:
            try:
                closes = (
                    data["Close"].dropna()
                    if len(tickers) == 1
                    else data[ticker]["Close"].dropna()
                )
                if len(closes) >= 2:
                    prev = float(closes.iloc[-2])
                    cur = float(closes.iloc[-1])
                    if prev > 0:
                        result[ticker] = round((cur - prev) / prev * 100, 2)
            except Exception:
                pass
    except Exception as e:
        logger.warning("batch download エラー: %s", e)
    return result


def _save_to_watchlist(
    ticker: str, name: str, drop_pct: float, dow_pct: float | None, price: float | None
) -> bool:
    try:
        existing = (
            supabase.table("stock_drop_watchlist")
            .select("id")
            .eq("code", ticker)
            .in_("status", ["watching", "rebound_signal", "notified"])
            .execute()
        )
        if existing.data:
            return False

        now = datetime.now(timezone.utc)
        supabase.table("stock_drop_watchlist").insert({
            "code": ticker,
            "name": name,
            "market": "dow",
            "source_index": "dow",
            "drop_detected_at": now.isoformat(),
            "drop_pct": drop_pct,
            "price_at_drop": price,
            "nikkei_pct": dow_pct,
            "sector": None,
            "status": "watching",
            "last_checked_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }).execute()
        return True
    except Exception as e:
        logger.error("watchlist保存エラー: ticker=%s %s", ticker, e)
        return False


def run_scan() -> None:
    logger.info("=== ダウ急落スキャン開始 ===")
    now_jst = datetime.now(JST)

    # NY市場は JST で月曜朝〜土曜朝に開くため、土曜朝の実行は許可
    # 日曜は確実に休場
    if now_jst.weekday() == 6 and not _IS_TEST:
        logger.info("日曜のためスキップ")
        return

    if not HAS_YFINANCE:
        logger.error("yfinance が未インストール")
        return

    cfg = get_settings(force_reload=True)
    drop_list_thr = float(cfg.get("drop_list_threshold", -2.0))
    alert_thr = float(cfg.get("alert_threshold", -9.0))

    dow_pct = _get_dow_change_pct()
    logger.info("ダウ平均: %s%%", f"{dow_pct:+.2f}" if dow_pct is not None else "取得失敗")

    tickers = list(DOW30.keys())
    day_changes = _batch_day_change(tickers)
    logger.info("取得済み: %d銘柄", len(day_changes))

    drops: list[tuple[str, float]] = []
    alerts: list[tuple[str, float]] = []

    for ticker, pct in day_changes.items():
        if pct <= drop_list_thr:
            drops.append((ticker, pct))
            if pct <= alert_thr:
                alerts.append((ticker, pct))

    logger.info("急落: %d銘柄 / 通知対象: %d銘柄", len(drops), len(alerts))

    # 価格取得してwatchlist保存
    saved = 0
    for ticker, pct in drops:
        price = None
        try:
            hist = yf.Ticker(ticker).history(period="2d", auto_adjust=True)
            closes = hist["Close"].dropna()
            if len(closes) >= 1:
                price = float(closes.iloc[-1])
        except Exception:
            pass
        if _save_to_watchlist(ticker, DOW30.get(ticker, ticker), pct, dow_pct, price):
            saved += 1
    logger.info("watchlist保存: %d銘柄", saved)

    logger.info("Dow notifications are disabled; watchlist logging only.")
    return

    if not alerts or not cfg.get("drop_notify_enabled", True):
        logger.info("=== スキャン完了（通知なし）===")
        return

    users = _eligible_users()
    if not users:
        logger.info("=== スキャン完了（通知対象ユーザーなし）===")
        return

    lines = ["【ダウ急落】"]
    for ticker, pct in sorted(alerts, key=lambda x: x[1])[:10]:
        lines.append(f"・{ticker} {DOW30.get(ticker, ticker)} {pct:+.1f}%")
    if dow_pct is not None:
        lines.append(f"\nダウ平均: {dow_pct:+.1f}%")
    lines.append(f"詳細 → {_WEB_URL}")
    msg = "\n".join(lines)

    sent = sum(1 for u in users if _push(u["user_id"], msg))
    logger.info("通知送信: %d人", sent)
    logger.info("=== ダウ急落スキャン完了 ===")


if __name__ == "__main__":
    run_scan()
