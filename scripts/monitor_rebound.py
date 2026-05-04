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
_WEB_URL = _opt("WEB_URL") or "https://line-bot-ukz5kw.fly.dev/web/dashboard"

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


def _fetch_history(code: str, market: str = "") -> "tuple[pd.Series, pd.Series] | None":
    if not HAS_YFINANCE:
        return None
    # US株（ダウ等）はティッカーそのまま、日本株は .T を付加
    ticker = code if market == "dow" else f"{code}.T"
    try:
        hist = yf.Ticker(ticker).history(period="3mo", interval="1d", auto_adjust=True)
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
            "user_id, plan, trial_started_at, trial_extended_until, membership_status, active"
        ).execute()
        now_utc = datetime.now(timezone.utc)
        return [
            u for u in (res.data or [])
            if u.get("active")
            and _resolve_plan(u, now_utc) == "paid"
        ]
    except Exception as e:
        logger.error("ユーザー取得失敗: %s", e)
        return []


def _build_summary_msg(to_notify: list) -> str:
    sorted_n = sorted(to_notify, key=lambda x: x[4].get("total", 0), reverse=True)
    top_item, _, is_strong, _, top_score = sorted_n[0]

    code = top_item.get("code", "")
    name = top_item.get("name", "")
    drop_pct = top_item.get("drop_pct") or 0.0
    score = top_score.get("total", 0)
    label = top_score.get("label", "")
    strength = "強シグナル★★" if is_strong else "シグナル"

    lines = [
        f"⚡ リバウンド候補",
        f"{code} {name}",
        f"急落 {drop_pct:+.1f}%　スコア {score:.0f}点（{label}）",
        f"判定: {strength}",
    ]

    others = len(sorted_n) - 1
    if others > 0:
        other_codes = " / ".join(x[0].get("code", "") for x in sorted_n[1:4])
        suffix = f"…他{others - 3}件" if others > 3 else ""
        lines.append(f"他 {others}銘柄: {other_codes}{suffix}")

    lines.append(f"詳細 → {_WEB_URL}")
    return "\n".join(lines)


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
        hist = _fetch_history(code, trade.get("market", ""))
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
        prev_status = item.get("status", "watching")

        # 監視期限チェック（watching のみ）
        if prev_status == "watching":
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
        hist = _fetch_history(code, item.get("market", ""))
        if hist is None:
            supabase.table("stock_drop_watchlist").update({
                "last_checked_at": now_utc.isoformat(),
                "updated_at": now_utc.isoformat(),
            }).eq("id", item_id).execute()
            continue

        closes, volumes = hist
        if len(closes) < 2:
            continue

        current = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])

        # テクニカル指標
        day_pct = (current - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
        price_at_drop = item.get("price_at_drop")
        from_drop = (
            (current - float(price_at_drop)) / float(price_at_drop) * 100
            if price_at_drop and float(price_at_drop) > 0 else 0.0
        )
        volume_ratio = 0.0
        if len(volumes) >= 21:
            avg_vol = float(volumes.iloc[-21:-1].mean())
            if avg_vol > 0:
                volume_ratio = float(volumes.iloc[-1]) / avg_vol

        score_data = _get_score(item, closes, volumes, cfg)
        score = score_data["total"]

        # 4条件 AND 判定
        is_signal = (
            day_pct >= float(cfg.get("daily_rebound_threshold", 3.0))
            and from_drop >= float(cfg.get("drop_rebound_threshold", 5.0))
            and volume_ratio >= float(cfg.get("volume_ratio_threshold", 1.5))
            and score >= float(cfg.get("watch_score", 70.0))
        )

        # 悪材料チェック（シグナル候補のみ）
        bad_news = False
        if is_signal:
            try:
                bad_news = has_bad_news(item)
                if bad_news:
                    logger.info("悪材料フィルター適用: %s", code)
            except Exception as e:
                logger.debug("悪材料チェックエラー: %s %s", code, e)

        # 新しいステータス決定（状態上書き）
        if is_signal and not bad_news:
            new_status = "rebound_signal"
        elif prev_status == "rebound_signal":
            new_status = "watching"  # 条件外れ → 監視に戻す
        else:
            new_status = "watching"

        logger.info(
            "チェック: %s %s→%s day=%.1f%% drop=%.1f%% vol=%.1fx score=%.0f%s",
            code, prev_status, new_status,
            day_pct, from_drop, volume_ratio, score,
            " [悪材料]" if bad_news else "",
        )

        closes_list = [round(float(v), 2) for v in closes.tail(10).tolist()]
        update_data: dict = {
            "status": new_status,
            "last_checked_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
            "score": score,
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

        try:
            supabase.table("stock_drop_watchlist").update(update_data).eq("id", item_id).execute()
        except Exception as e:
            logger.error("watchlist 更新エラー: %s %s", code, e)

        # 未通知の rebound_signal のみ通知キューへ（通知失敗リトライも含む）
        if new_status == "rebound_signal" and not item.get("rebound_notified_at"):
            signals = [
                f"前日比+{day_pct:.1f}%",
                f"急落時+{from_drop:.1f}%",
                f"出来高{volume_ratio:.1f}倍",
                f"スコア{score:.0f}点",
            ]
            is_strong = score >= strong_thr
            to_notify.append((item, signals, is_strong, current, score_data))

    logger.info("新規シグナル: %d銘柄", len(to_notify))

    _manage_virtual_trades(cfg, now_utc)

    if not to_notify:
        logger.info("通知対象なし。終了")
        return

    if not cfg.get("rebound_notify_enabled", True):
        logger.info("rebound_notify_enabled=False → LINE 通知スキップ")
        return

    users = _eligible_users()
    logger.info("通知対象ユーザー: %d人", len(users))

    msg = _build_summary_msg(to_notify)
    sent = sum(1 for u in users if _push(u["user_id"], msg))
    logger.info("通知送信: %d銘柄まとめ → %d人", len(to_notify), sent)

    if sent > 0 or not users:
        for item, _, _, current, score_data in to_notify:
            try:
                supabase.table("stock_drop_watchlist").update({
                    "status": "notified",
                    "rebound_notified_at": now_utc.isoformat(),
                    "updated_at": now_utc.isoformat(),
                }).eq("id", item.get("id")).execute()
            except Exception as e:
                logger.error("notified 更新エラー: %s %s", item.get("code"), e)

            if score_data["total"] >= strong_thr:
                _create_virtual_trade(item, current, score_data["total"], now_utc)

    logger.info("=== リバウンド監視完了 ===")


if __name__ == "__main__":
    run_monitor()
