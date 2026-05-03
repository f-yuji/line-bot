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
from scoring import calculate_score
from bad_news_filter import has_bad_news

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"


# ─── 環境変数 ───

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


# ─── テクニカル指標 ───

def _rsi(closes: "pd.Series", period: int = 14) -> float | None:
    if len(closes) < period + 2:
        return None
    delta = closes.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    if loss.iloc[-1] == 0:
        return 100.0
    return round(100 - 100 / (1 + gain.iloc[-1] / loss.iloc[-1]), 1)


def _rsi_series(closes: "pd.Series", period: int = 14) -> "pd.Series":
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - 100 / (1 + rs)).fillna(0)


def _biz_days(from_dt: datetime, to_dt: datetime) -> int:
    days, cur = 0, from_dt.date()
    end = to_dt.date()
    while cur < end:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def _fetch_history(code: str) -> "tuple[pd.Series, pd.Series] | None":
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
    if len(closes) < 2:
        return []

    current = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])
    triggered: list[str] = []

    # ① 5日移動平均を上抜け
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

    # ④ 出来高が20日平均の N 倍以上
    if len(volumes) >= 21:
        vol_avg = float(volumes.iloc[-21:-1].mean())
        vol_now = float(volumes.iloc[-1])
        if vol_avg > 0 and vol_now >= vol_avg * cfg.get("volume_ratio_threshold", 1.5):
            triggered.append(f"出来高{vol_now / vol_avg:.1f}倍")

    # ⑤ RSI: 直近5日以内に rsi_low 以下 → 現在が rsi_recover 以上に回復
    rsi_now = _rsi(closes)
    if rsi_now is not None and rsi_now >= cfg.get("rsi_recover_threshold", 35.0):
        rsi_s = _rsi_series(closes).dropna()
        recent_prev = list(rsi_s.tail(6).values[:-1])
        if any(r < cfg.get("rsi_low_threshold", 30.0) for r in recent_prev):
            triggered.append(f"RSI回復({rsi_now:.0f})")

    return triggered


# ─── スコア計算ヘルパー ───

def _get_score(item: dict, closes: "pd.Series", volumes: "pd.Series", cfg: dict) -> dict:
    try:
        from nikkei_alert import get_valuation_metrics, _load_financials_cache as _load_fin
        val = get_valuation_metrics(item.get("code", "")) or {}
        fin = _load_fin().get(item.get("code", ""), {})
        score = calculate_score(
            item, closes, volumes, cfg,
            per=val.get("per"),
            pbr=val.get("pbr"),
            div_yield_pct=val.get("dividend_yield_pct"),
            is_deficit=fin.get("is_deficit"),
        )
        score["per"] = val.get("per")
        score["pbr"] = val.get("pbr")
        score["div_yield_pct"] = val.get("dividend_yield_pct")
        return score
    except Exception as e:
        logger.debug("スコア計算エラー: %s %s", item.get("code"), e)
    return {"total": 0.0, "technical": 0.0, "fundamental": 0.0, "market": 0.0, "label": "-", "per": None, "pbr": None, "div_yield_pct": None}


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


def _build_msg(item: dict, signals: list[str], is_strong: bool, current: float, score_data: dict | None = None) -> str:
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

    score_line = ""
    if score_data and score_data.get("total", 0) > 0:
        score_line = f"スコア: {score_data['total']:.0f}点（{score_data['label']}）\n"

    return (
        f"【リバウンド候補】\n"
        f"{code} {name}\n\n"
        f"急落: {drop_pct:+.1f}%\n"
        f"{recovery_line}"
        f"\nシグナル:\n{sig_lines}\n\n"
        f"{score_line}"
        f"判定:\n{strength}\n短期反発候補"
    )


# ─── 仮想売買 ───

def _create_virtual_trade(item: dict, price: float, score: float, now_utc: datetime) -> None:
    code = item.get("code", "")
    try:
        existing = (
            supabase.table("virtual_trades")
            .select("id")
            .eq("code", code)
            .eq("status", "open")
            .execute()
        )
        if existing.data:
            return
        supabase.table("virtual_trades").insert({
            "watchlist_id": item.get("id"),
            "code": code,
            "name": item.get("name", ""),
            "buy_price": price,
            "buy_date": now_utc.isoformat(),
            "quantity": 100,
            "buy_score": round(score, 1),
            "status": "open",
            "created_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }).execute()
        logger.info("仮想買い: %s price=%.0f score=%.1f", code, price, score)
    except Exception as e:
        logger.error("virtual_trade作成エラー: %s %s", code, e)


def _manage_virtual_trades(cfg: dict, now_utc: datetime) -> None:
    """Open 仮想ポジションの P&L をチェックして take_profit / stop_loss / expired で決済。"""
    try:
        res = supabase.table("virtual_trades").select("*").eq("status", "open").execute()
        open_trades = res.data or []
    except Exception as e:
        logger.error("virtual_trades取得失敗: %s", e)
        return

    if not open_trades:
        return

    watch_limit = int(cfg.get("watch_days_limit", 10))
    logger.info("open virtual trades: %d件", len(open_trades))

    for trade in open_trades:
        code = trade.get("code", "")
        hist = _fetch_history(code)
        if hist is None:
            continue
        closes, _ = hist
        current = float(closes.iloc[-1])
        buy_price = float(trade.get("buy_price") or 0)
        if buy_price <= 0:
            continue

        pnl_pct = round((current - buy_price) / buy_price * 100, 2)

        biz = 0
        buy_dt = trade.get("buy_date", "")
        if buy_dt:
            try:
                dt = datetime.fromisoformat(str(buy_dt).replace("Z", "+00:00"))
                biz = _biz_days(dt, now_utc)
            except Exception:
                pass

        sell_reason = None
        if pnl_pct >= 10.0:
            sell_reason = "take_profit"
        elif pnl_pct <= -7.0:
            sell_reason = "stop_loss"
        elif biz > watch_limit:
            sell_reason = "expired"

        if sell_reason:
            pnl = round((current - buy_price) * int(trade.get("quantity") or 100), 0)
            try:
                supabase.table("virtual_trades").update({
                    "sell_price": current,
                    "sell_date": now_utc.isoformat(),
                    "sell_reason": sell_reason,
                    "profit_loss": pnl,
                    "profit_loss_pct": pnl_pct,
                    "status": "closed",
                    "updated_at": now_utc.isoformat(),
                }).eq("id", trade["id"]).execute()
                logger.info("仮想決済: %s %s pnl=%.1f%%", code, sell_reason, pnl_pct)
            except Exception as e:
                logger.error("virtual_trade決済エラー: %s %s", code, e)


# ─── メイン ───

def run_monitor() -> None:
    logger.info("=== リバウンド監視開始 ===")
    now_jst = datetime.now(JST)
    now_utc = datetime.now(timezone.utc)

    if now_jst.weekday() >= 5 and not _IS_TEST:
        logger.info("土日のためスキップ")
        return

    if not HAS_YFINANCE:
        logger.error("yfinance が未インストール（pip install yfinance）")
        return

    cfg = get_settings(force_reload=True)
    watch_days_limit = int(cfg.get("watch_days_limit", 10))
    strong_thr = float(cfg.get("strong_watch_score", 80.0))

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

    # (item, signals, is_strong, current_price, score_data)
    to_notify: list[tuple[dict, list[str], bool, float, dict]] = []

    for item in watchlist:
        code = item.get("code", "")
        item_id = item.get("id")
        status = item.get("status", "watching")

        # rebound_signal → 通知失敗リトライ
        if status == "rebound_signal":
            hist = _fetch_history(code)
            if hist:
                closes, volumes = hist
                current = float(closes.iloc[-1])
                signals = check_rebound(item.get("price_at_drop"), closes, volumes, cfg)
                if not signals:
                    signals = ["リバウンドシグナル（継続）"]
                score_data = _get_score(item, closes, volumes, cfg)
                to_notify.append((item, signals, len(signals) >= 2, current, score_data))
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

        # スコア計算
        score_data = _get_score(item, closes, volumes, cfg)

        # 悪材料チェック（スコアが watch_score 以上の場合のみ API を叩く）
        bad_news = False
        if signals and score_data["total"] >= float(cfg.get("watch_score", 70.0)):
            try:
                bad_news = has_bad_news(item)
                if bad_news:
                    logger.info("悪材料フィルター適用: %s", code)
            except Exception as e:
                logger.debug("悪材料チェックエラー: %s %s", code, e)

        logger.info(
            "チェック: %s signals=%d score=%.1f%s%s",
            code, len(signals), score_data["total"],
            f" [{', '.join(signals)}]" if signals else "",
            " [悪材料]" if bad_news else "",
        )

        closes_list = [round(float(v), 2) for v in closes.tail(10).tolist()]
        update_data: dict = {
            "last_checked_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
            "score": score_data["total"],
            "score_technical": score_data["technical"],
            "score_fundamental": score_data["fundamental"],
            "score_market": score_data["market"],
            "score_label": score_data["label"],
            "has_bad_news": bad_news,
            "price_history": closes_list,
            "per": score_data.get("per"),
            "pbr": score_data.get("pbr"),
            "div_yield_pct": score_data.get("div_yield_pct"),
        }

        if signals and not bad_news:
            update_data["status"] = "rebound_signal"
            to_notify.append((item, signals, is_strong, current, score_data))

        try:
            supabase.table("stock_drop_watchlist").update(update_data).eq("id", item_id).execute()
        except Exception as e:
            logger.error("watchlist 更新エラー: %s %s", code, e)

    logger.info("リバウンドシグナル: %d銘柄", len(to_notify))

    if not to_notify:
        logger.info("通知対象なし。仮想売買チェックへ")
        _manage_virtual_trades(cfg, now_utc)
        return

    if not cfg.get("rebound_notify_enabled", True):
        logger.info("rebound_notify_enabled=False → LINE 通知スキップ")
        _manage_virtual_trades(cfg, now_utc)
        return

    users = _eligible_users()
    logger.info("通知対象ユーザー: %d人", len(users))

    for item, signals, is_strong, current, score_data in to_notify:
        msg = _build_msg(item, signals, is_strong, current, score_data)
        sent = sum(1 for u in users if _push(u["user_id"], msg))
        logger.info("通知送信: %s → %d人", item.get("code"), sent)

        if sent > 0 or not users:
            try:
                supabase.table("stock_drop_watchlist").update({
                    "status": "notified",
                    "rebound_notified_at": now_utc.isoformat(),
                    "updated_at": now_utc.isoformat(),
                }).eq("id", item.get("id")).execute()
            except Exception as e:
                logger.error("notified 更新エラー: %s %s", item.get("code"), e)

        # 強シグナル → 仮想買い
        if score_data["total"] >= strong_thr:
            _create_virtual_trade(item, current, score_data["total"], now_utc)

    # 保有中仮想ポジションの P&L チェック
    _manage_virtual_trades(cfg, now_utc)
    logger.info("=== リバウンド監視完了 ===")


if __name__ == "__main__":
    run_monitor()
