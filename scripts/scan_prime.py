#!/usr/bin/env python3
"""
TSE プライム全銘柄 急落検知・watchlist 保存・LINE 通知
cron: 平日 15:40 JST
実行: python scripts/scan_prime.py
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
from prime_stocks import get_prime_tickers
from nikkei_alert import NIKKEI225, get_nikkei_change_pct

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"
BATCH_SIZE = 200


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


_SUPABASE_MODE = _opt("SUPABASE_MODE") or _opt("ENV")
_mode_upper = (_SUPABASE_MODE or "").upper()
_IS_TEST = _opt("ENV").upper() == "TEST"
SUPABASE_URL = (_opt(f"SUPABASE_URL_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_URL")
SUPABASE_KEY = (_opt(f"SUPABASE_KEY_{_mode_upper}") if _mode_upper else "") or _opt("SUPABASE_KEY")
LINE_TOKEN = _opt("LINE_CHANNEL_ACCESS_TOKEN")

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
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active, drop_alert_enabled"
        ).execute()
        return [
            u for u in (res.data or [])
            if u.get("active") and u.get("drop_alert_enabled") and _resolve_plan(u, now) == "paid"
        ]
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return []


def _batch_day_change(codes: list[str]) -> dict[str, float]:
    """コードリスト → {code: day_pct}。失敗銘柄は除外。"""
    if not codes:
        return {}
    tickers = [f"{c}.T" for c in codes]
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
        for code, ticker in zip(codes, tickers):
            try:
                closes = (data["Close"].dropna() if len(tickers) == 1 else data[ticker]["Close"].dropna())
                if len(closes) >= 2:
                    prev = float(closes.iloc[-2])
                    cur = float(closes.iloc[-1])
                    if prev > 0:
                        result[code] = round((cur - prev) / prev * 100, 2)
            except Exception:
                pass
    except Exception as e:
        logger.warning("batch download エラー（batch %d): %s", len(codes), e)
    return result


def _save_to_watchlist(code: str, name: str, sector: str, drop_pct: float, nikkei_pct: float | None) -> bool:
    try:
        existing = (
            supabase.table("stock_drop_watchlist")
            .select("id")
            .eq("code", code)
            .in_("status", ["watching", "rebound_signal"])
            .execute()
        )
        if existing.data:
            return False

        now = datetime.now(timezone.utc)
        price = None
        try:
            hist = yf.Ticker(f"{code}.T").history(period="2d", auto_adjust=True)
            closes = hist["Close"].dropna()
            if len(closes) >= 1:
                price = float(closes.iloc[-1])
        except Exception:
            pass

        supabase.table("stock_drop_watchlist").insert({
            "code": code,
            "name": name,
            "market": "prime",
            "source_index": "prime",
            "drop_detected_at": now.isoformat(),
            "drop_pct": drop_pct,
            "price_at_drop": price,
            "nikkei_pct": nikkei_pct,
            "sector": sector or None,
            "status": "watching",
            "last_checked_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }).execute()
        return True
    except Exception as e:
        logger.error("watchlist保存エラー: code=%s %s", code, e)
        return False


def run_scan() -> None:
    logger.info("=== TSEプライム急落スキャン開始 ===")
    now_jst = datetime.now(JST)

    if now_jst.weekday() >= 5 and not _IS_TEST:
        logger.info("土日のためスキップ")
        return

    if not HAS_YFINANCE:
        logger.error("yfinance が未インストール")
        return

    cfg = get_settings(force_reload=True)
    drop_list_thr = float(cfg.get("drop_list_threshold", -2.0))
    alert_thr = float(cfg.get("alert_threshold", -9.0))

    nikkei_pct = get_nikkei_change_pct()

    # 月曜日はキャッシュを強制更新
    force_refresh = (now_jst.weekday() == 0)
    stocks = get_prime_tickers(supabase, force_refresh=force_refresh)

    # Nikkei225 と重複するコードは nikkei_alert.py が処理済みのためスキップ
    nikkei_codes = set(NIKKEI225.keys())
    prime_only = [s for s in stocks if s["code"] not in nikkei_codes]
    logger.info("スキャン対象: %d銘柄（Nikkei225除く）", len(prime_only))

    if not prime_only:
        logger.info("スキャン対象なし")
        return

    codes = [s["code"] for s in prime_only]
    code_to_info = {s["code"]: s for s in prime_only}

    drops: list[tuple[str, float]] = []
    alerts: list[tuple[str, float]] = []

    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i:i + BATCH_SIZE]
        logger.info("batch %d〜%d を処理中...", i, i + len(batch))
        day_changes = _batch_day_change(batch)
        for code, pct in day_changes.items():
            if pct <= drop_list_thr:
                drops.append((code, pct))
                if pct <= alert_thr:
                    alerts.append((code, pct))

    logger.info("急落: %d銘柄 / 通知対象: %d銘柄", len(drops), len(alerts))

    saved = 0
    for code, pct in drops:
        info = code_to_info.get(code, {})
        if _save_to_watchlist(code, info.get("name", ""), info.get("sector", ""), pct, nikkei_pct):
            saved += 1
    logger.info("watchlist保存: %d銘柄", saved)

    if not alerts or not cfg.get("drop_notify_enabled", True):
        logger.info("=== スキャン完了（通知なし）===")
        return

    users = _eligible_users()
    if not users:
        logger.info("=== スキャン完了（通知対象ユーザーなし）===")
        return

    lines = ["【TSEプライム急落】"]
    for code, pct in sorted(alerts, key=lambda x: x[1])[:10]:
        info = code_to_info.get(code, {})
        lines.append(f"・{code} {info.get('name', '')} {pct:+.1f}%")
    if nikkei_pct is not None:
        lines.append(f"\n日経平均: {nikkei_pct:+.1f}%")
    msg = "\n".join(lines)

    sent = sum(1 for u in users if _push(u["user_id"], msg))
    logger.info("通知送信: %d人", sent)
    logger.info("=== TSEプライム急落スキャン完了 ===")


if __name__ == "__main__":
    run_scan()
