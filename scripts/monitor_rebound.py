#!/usr/bin/env python3
"""
Rebound monitor cron.

Phase 1 of the rebound AI system is rule-based:
detect rebound conditions, apply bad-news filtering, persist signal_stage,
notify LINE by stage, and create virtual trades for all signal stages.
"""
import logging
import os
import sys
import argparse
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

from bad_news_filter import analyze_bad_news
from scoring import calculate_score
from services.market_regime import evaluate_market_regime
from services.signal_stage import SIGNAL_STAGES, evaluate_signal_stage
from settings_loader import get_settings

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LINE_API_BASE = "https://api.line.me"
JAPAN_MARKETS = {"nikkei225", "nikkei", "prime", "tse_prime", "japan"}
NON_JAPAN_MARKETS = {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}
SMOKE_RELAXED_OVERRIDES = {
    "daily_rebound_threshold": 2.0,
    "drop_rebound_threshold": 3.0,
    "volume_ratio_threshold": 1.2,
    "rsi_low_threshold": 35.0,
    "rsi_recover_threshold": 30.0,
    "ignore_score": 20.0,
    "watch_score": 30.0,
    "strong_watch_score": 40.0,
}
MODE_SETTING_OVERRIDES = {
    "normal": {
        "drop_list_threshold": -3.5,
        "daily_rebound_threshold": 4.0,
        "drop_rebound_threshold": 8.0,
        "volume_ratio_threshold": 2.0,
        "rsi_recover_threshold": 40.0,
    },
    "shock": {
        "drop_list_threshold": -5.0,
        "daily_rebound_threshold": 3.0,
        "drop_rebound_threshold": 5.0,
        "volume_ratio_threshold": 1.5,
        "rsi_recover_threshold": 35.0,
    },
    "panic": {
        "drop_list_threshold": -7.0,
        "daily_rebound_threshold": 3.0,
        "drop_rebound_threshold": 5.0,
        "volume_ratio_threshold": 1.3,
        "rsi_recover_threshold": 35.0,
    },
    "recovery": {
        "drop_list_threshold": -3.5,
        "daily_rebound_threshold": 2.5,
        "drop_rebound_threshold": 4.0,
        "volume_ratio_threshold": 1.5,
        "rsi_recover_threshold": 35.0,
    },
}


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
    raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
if not LINE_TOKEN:
    raise KeyError("LINE_CHANNEL_ACCESS_TOKEN is not set")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _to_float(value, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_market(value) -> str:
    return str(value or "").strip().lower()


def _is_alphabetic_ticker(code: str) -> bool:
    return bool(code) and code.isalpha()


def is_japanese_watchlist_item(item: dict) -> bool:
    code = str(item.get("code") or "").strip()
    market = _norm_market(item.get("market"))
    source_index = _norm_market(item.get("source_index"))

    if _is_alphabetic_ticker(code):
        return False
    if market in NON_JAPAN_MARKETS or source_index in NON_JAPAN_MARKETS:
        return False
    if market in JAPAN_MARKETS or source_index in JAPAN_MARKETS:
        return True
    return True


def _apply_smoke_relaxed(cfg: dict) -> dict:
    relaxed = dict(cfg)
    relaxed.update(SMOKE_RELAXED_OVERRIDES)
    return relaxed


def get_current_market_regime(target_date: datetime | None = None) -> dict:
    d = (target_date or datetime.now(JST)).date().isoformat()
    try:
        rows = (
            supabase.table("market_regime")
            .select("trade_date,mode,shock_score,reason,nikkei_change_pct,topix_change_pct,decliners_ratio")
            .lte("trade_date", d)
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return {
                "mode": rows[0].get("mode") or "normal",
                "shock_score": float(rows[0].get("shock_score") or 0),
                "reason": rows[0].get("reason") or "",
                "trade_date": rows[0].get("trade_date"),
                "nikkei_change_pct": rows[0].get("nikkei_change_pct"),
                "topix_change_pct": rows[0].get("topix_change_pct"),
                "decliners_ratio": rows[0].get("decliners_ratio"),
            }
    except Exception as e:
        logger.warning("market_regime lookup failed; normal mode used: %s", e)
    return {"mode": "normal", "shock_score": 0.0, "reason": "market_regime unavailable", "trade_date": d}


def get_settings_for_mode(base_settings: dict, mode: str) -> dict:
    cfg = dict(base_settings)
    cfg.update(MODE_SETTING_OVERRIDES.get(mode or "normal", MODE_SETTING_OVERRIDES["normal"]))
    return cfg


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
    ticker = code if market == "dow" else f"{code}.T"
    try:
        hist = yf.Ticker(ticker).history(period="3mo", interval="1d", auto_adjust=True)
        if len(hist) < 10:
            logger.warning("not enough history: %s (%d)", code, len(hist))
            return None
        return hist["Close"], hist["Volume"]
    except Exception as e:
        logger.warning("price fetch error: %s %s", code, e)
        return None


def check_rebound(
    price_at_drop: float | None,
    closes: "pd.Series",
    volumes: "pd.Series",
    cfg: dict,
) -> dict:
    result = {
        "has_signal": False,
        "signal_reasons": [],
        "signal_count": 0,
        "day_rebound_pct": None,
        "from_drop_pct": None,
        "volume_ratio": None,
        "rsi": None,
        "rsi_recovered": False,
        "ma5_cross": False,
    }
    if len(closes) < 2:
        return result

    current = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])
    reasons: list[str] = []
    count = 0

    if prev > 0:
        day_pct = (current - prev) / prev * 100
        result["day_rebound_pct"] = day_pct
        if day_pct >= float(cfg.get("daily_rebound_threshold", 4.0)):
            count += 1
            reasons.append(f"前日比+{day_pct:.1f}%")

    if price_at_drop and price_at_drop > 0:
        from_drop = (current - price_at_drop) / price_at_drop * 100
        result["from_drop_pct"] = from_drop
        if from_drop >= float(cfg.get("drop_rebound_threshold", 8.0)):
            count += 1
            reasons.append(f"急落時から+{from_drop:.1f}%")

    if len(volumes) >= 21:
        vol_avg = float(volumes.iloc[-21:-1].mean())
        vol_now = float(volumes.iloc[-1])
        if vol_avg > 0:
            ratio = vol_now / vol_avg
            result["volume_ratio"] = ratio
            if ratio >= float(cfg.get("volume_ratio_threshold", 2.0)):
                count += 1
                reasons.append(f"出来高{ratio:.1f}倍")

    rsi_now = _rsi(closes)
    result["rsi"] = rsi_now
    if rsi_now is not None and rsi_now >= float(cfg.get("rsi_recover_threshold", 40.0)):
        rsi_s = _rsi_series(closes).dropna()
        recent_prev = list(rsi_s.tail(6).values[:-1])
        if any(r < float(cfg.get("rsi_low_threshold", 25.0)) for r in recent_prev):
            result["rsi_recovered"] = True
            count += 1
            reasons.append(f"RSI回復({rsi_now:.0f})")

    if cfg.get("ma5_cross_enabled", False) and len(closes) >= 5:
        ma5 = float(closes.tail(5).mean())
        if prev <= ma5 < current:
            result["ma5_cross"] = True
            count += 1
            reasons.append("5日線上抜け")

    result["signal_reasons"] = reasons
    result["signal_count"] = count
    result["has_signal"] = count > 0
    return result


def determine_signal_stage(
    score: float,
    signal_count: int,
    has_bad_news: bool,
    is_excluded: bool,
    cfg: dict,
    ai_probability: float | None = None,
    market_regime: dict | None = None,
) -> str:
    if is_excluded or has_bad_news:
        return "none"
    return evaluate_signal_stage(ai_probability, score, None, cfg, market_regime)["stage"]


def _stage_label(stage: str) -> str:
    return {
        "early": "初動",
        "confirmed": "本命",
        "strong_confirmed": "強本命",
        "none": "シグナルなし",
    }.get(stage or "none", "シグナルなし")


def _get_score(
    item: dict,
    closes: "pd.Series",
    volumes: "pd.Series",
    cfg: dict,
    bad_analysis: dict | None = None,
) -> dict:
    bad_analysis = bad_analysis or {}
    severity = bad_analysis.get("severity")
    bad_score = float(bad_analysis.get("bad_news_score") or 0)
    if severity == "medium":
        bad_penalty = min(25.0, max(10.0, bad_score * 0.35))
    elif severity == "weak":
        bad_penalty = min(8.0, bad_score * 0.25)
    else:
        bad_penalty = 0.0

    try:
        from nikkei_alert import _load_financials_cache as _load_fin
        from nikkei_alert import get_valuation_metrics

        val = get_valuation_metrics(item.get("code", "")) or {}
        fin = _load_fin().get(item.get("code", ""), {})
        score = calculate_score(
            item,
            closes,
            volumes,
            cfg,
            per=val.get("per"),
            pbr=val.get("pbr"),
            div_yield_pct=val.get("dividend_yield_pct"),
            is_deficit=fin.get("is_deficit"),
            adjustments={"bad_news_penalty": bad_penalty},
        )
        score["per"] = val.get("per")
        score["pbr"] = val.get("pbr")
        score["div_yield_pct"] = val.get("dividend_yield_pct")
        return score
    except Exception as e:
        logger.debug("score error: %s %s", item.get("code"), e)
    return {
        "total": 0.0,
        "technical": 0.0,
        "fundamental": 0.0,
        "market": 0.0,
        "label": "-",
        "per": None,
        "pbr": None,
        "div_yield_pct": None,
    }


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
            logger.error("LINE push failed user=%s status=%s", user_id, r.status_code)
            return False
        return True
    except Exception as e:
        logger.error("LINE push exception user=%s %s", user_id, e)
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
            if u.get("active") and _resolve_plan(u, now_utc) == "paid"
        ]
    except Exception as e:
        logger.error("user fetch failed: %s", e)
        return []


def _build_signal_msg(item: dict, rebound: dict, current: float, score_data: dict, cfg: dict) -> str:
    stage = item.get("signal_stage", "none")
    score = float(score_data.get("total") or 0)
    title = {
        "early": "【初動リバ候補】",
        "confirmed": "【本命リバ候補】",
        "strong_confirmed": "【強本命】",
    }.get(stage, "【リバ候補】")

    drop_pct = _to_float(item.get("drop_pct"), 0.0) or 0.0
    day_pct = rebound.get("day_rebound_pct")
    from_drop = rebound.get("from_drop_pct")
    vol_ratio = rebound.get("volume_ratio")
    rsi = rebound.get("rsi")

    lines = [
        title,
        f"{item.get('code', '')} {item.get('name', '')}".strip(),
        "",
        f"ルールスコア：{score:.0f}",
        f"段階：{_stage_label(stage)}",
        f"急落：{drop_pct:+.1f}%",
    ]
    if from_drop is not None:
        lines.append(f"急落時から：{from_drop:+.1f}%")
    if day_pct is not None:
        lines.append(f"反発：{day_pct:+.1f}%")
    if vol_ratio is not None:
        lines.append(f"出来高：{vol_ratio:.1f}倍")
    if rsi is not None:
        lines.append(f"RSI：{rsi:.0f}")

    lines.append("")
    if stage == "early":
        lines.extend(["判断：", "初動。まだ本命ではない。", "監視候補。"])
    elif stage == "confirmed":
        lines.extend([
            "想定：",
            "利確 +5%",
            "損切 -4%",
            f"期限 {int(cfg.get('watch_days_limit', 5))}営業日",
        ])
    else:
        lines.append("優先確認候補。")

    lines.extend(["", f"詳細：{_WEB_URL}"])
    return "\n".join(lines)


def _signal_digest_block(item: dict, rebound: dict, current: float, score_data: dict) -> str:
    stage = item.get("signal_stage", "none")
    stage_label = {
        "early": "初動",
        "confirmed": "本命",
        "strong_confirmed": "強本命",
    }.get(stage, stage)
    lines = [
        f"{stage_label} {item.get('code', '')} {item.get('name', '')}".strip(),
        f"ルールスコア {float(score_data.get('total') or 0):.0f} / 急落 {(_to_float(item.get('drop_pct'), 0) or 0):+.1f}%",
    ]
    if rebound.get("from_drop_pct") is not None:
        lines[-1] += f" / 急落時から {rebound.get('from_drop_pct'):+.1f}%"
    if rebound.get("rsi") is not None or rebound.get("volume_ratio") is not None:
        lines.append(
            " / ".join([
                f"RSI {rebound.get('rsi'):.0f}" if rebound.get("rsi") is not None else "",
                f"出来高 {rebound.get('volume_ratio'):.1f}倍" if rebound.get("volume_ratio") is not None else "",
            ]).strip(" /")
        )
    return "\n".join([line for line in lines if line])


def _build_signal_digest(to_notify: list[tuple[dict, dict, float, dict, dict]], cfg: dict) -> list[str]:
    if not to_notify:
        return []
    strong = sum(1 for item, *_ in to_notify if item.get("signal_stage") == "strong_confirmed")
    confirmed = sum(1 for item, *_ in to_notify if item.get("signal_stage") == "confirmed")
    early = sum(1 for item, *_ in to_notify if item.get("signal_stage") == "early")
    header = "\n".join([
        "【リバウンド候補まとめ】",
        f"通知候補：{len(to_notify)}件",
        f"強本命 {strong} / 本命 {confirmed} / 初動 {early}",
        "",
    ])
    footer = f"\n\n詳細：\n{_WEB_URL}"
    chunks: list[str] = []
    current_msg = header
    for idx, (item, rebound, current, score_data, _bad) in enumerate(to_notify, start=1):
        block = f"{idx}. " + _signal_digest_block(item, rebound, current, score_data)
        addition = ("\n\n" if current_msg != header else "") + block
        if len(current_msg) + len(addition) + len(footer) > 4300:
            chunks.append(current_msg + footer)
            current_msg = header + block
        else:
            current_msg += addition
    chunks.append(current_msg + footer)
    return chunks


def _create_virtual_trade(
    item: dict,
    price: float,
    score: float,
    now_utc: datetime,
    rebound: dict | None = None,
    bad_analysis: dict | None = None,
) -> None:
    code = item.get("code", "")
    stage = item.get("signal_stage")
    if stage not in SIGNAL_STAGES:
        return
    if item.get("market_regime") == "panic_selloff":
        logger.info("virtual buy skipped by market regime: %s panic_selloff", code)
        return
    rebound = rebound or {}
    bad_analysis = bad_analysis or {}
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
            "market": item.get("market", ""),
            "buy_price": price,
            "buy_date": now_utc.isoformat(),
            "quantity": 100,
            "buy_score": round(score, 1),
            "signal_stage": stage,
            "entry_reason": ", ".join(rebound.get("signal_reasons") or []),
            "entry_score": round(score, 1),
            "entry_probability": None,
            "expected_value": None,
            "mode": item.get("mode") or "normal",
            "bad_news_score": float(bad_analysis.get("bad_news_score") or 0),
            "sector_risk_score": float(item.get("sector_risk_score") or 0),
            "market_shock_score": float(item.get("market_shock_score") or 0),
            "feature_snapshot_id": item.get("feature_snapshot_id"),
            "market_regime": item.get("market_regime"),
            "market_regime_label": item.get("market_regime_label"),
            "entry_size_multiplier": float(item.get("entry_size_multiplier") or 1.0),
            "status": "open",
            "created_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }).execute()
        logger.info("virtual buy: %s price=%.0f score=%.1f stage=%s", code, price, score, stage)
    except Exception as e:
        logger.error("virtual_trade create error: %s %s", code, e)


def _manage_virtual_trades(cfg: dict, now_utc: datetime, *, dry_run: bool = False) -> None:
    try:
        res = supabase.table("virtual_trades").select("*").eq("status", "open").execute()
        open_trades = res.data or []
    except Exception as e:
        logger.error("virtual_trades fetch failed: %s", e)
        return

    if not open_trades:
        return

    if dry_run:
        logger.info("DRYRUN virtual trade management skipped: open=%d", len(open_trades))
        return

    watch_limit = int(cfg.get("watch_days_limit", 5))
    for trade in open_trades:
        if not is_japanese_watchlist_item(trade):
            logger.info(
                "skip non-japanese virtual trade in monitor_rebound: %s market=%s",
                trade.get("code", ""),
                trade.get("market", ""),
            )
            continue
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
        max_return_pct = max(_to_float(trade.get("max_return_pct"), pnl_pct) or pnl_pct, pnl_pct)
        max_drawdown_pct = min(_to_float(trade.get("max_drawdown_pct"), pnl_pct) or pnl_pct, pnl_pct)

        biz = 0
        buy_dt = trade.get("buy_date", "")
        if buy_dt:
            try:
                dt = datetime.fromisoformat(str(buy_dt).replace("Z", "+00:00"))
                biz = _biz_days(dt, now_utc)
            except Exception:
                pass

        exit_reason = None
        if pnl_pct >= 5.0:
            exit_reason = "take_profit"
        elif pnl_pct <= -4.0:
            exit_reason = "stop_loss"
        elif biz >= watch_limit:
            exit_reason = "expired"

        update_data = {
            "max_return_pct": round(max_return_pct, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "exit_checked_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }

        if exit_reason:
            pnl = round((current - buy_price) * int(trade.get("quantity") or 100), 0)
            update_data.update({
                "sell_price": current,
                "sell_date": now_utc.isoformat(),
                "sell_reason": exit_reason,
                "exit_reason": exit_reason,
                "profit_loss": pnl,
                "profit_loss_pct": pnl_pct,
                "status": "closed",
            })

        try:
            supabase.table("virtual_trades").update(update_data).eq("id", trade["id"]).execute()
            if exit_reason:
                logger.info("virtual exit: %s %s pnl=%.1f%%", code, exit_reason, pnl_pct)
        except Exception as e:
            logger.error("virtual_trade update error: %s %s", code, e)


def run_monitor(*, smoke_relaxed: bool = False, dry_run: bool = False, force_no_notify: bool = False) -> None:
    logger.info("=== rebound monitor start ===")
    now_jst = datetime.now(JST)
    now_utc = datetime.now(timezone.utc)

    if now_jst.weekday() >= 5 and not _IS_TEST:
        logger.info("weekend skip")
        return
    if not HAS_YFINANCE:
        logger.error("yfinance is not installed")
        return

    cfg = get_settings(force_reload=True)
    if smoke_relaxed:
        cfg = _apply_smoke_relaxed(cfg)
        logger.info("smoke relaxed thresholds enabled for this run only")
    regime = get_current_market_regime(now_jst)
    logger.info("[market_data_for_regime] %s", regime)
    market_adjustment = evaluate_market_regime(regime)
    if not smoke_relaxed:
        cfg = get_settings_for_mode(cfg, regime.get("mode", "normal"))
    logger.info(
        "current market mode: mode=%s shock_score=%.0f date=%s reason=%s",
        regime.get("mode"),
        float(regime.get("shock_score") or 0),
        regime.get("trade_date"),
        regime.get("reason"),
    )
    logger.info(
        "[market_regime] %s: AI threshold +%.2f, entry size %.1f reason=%s",
        market_adjustment["regime"],
        market_adjustment["ai_threshold_adjust"],
        market_adjustment["entry_size_multiplier"],
        market_adjustment["reason"],
    )
    if dry_run:
        logger.info("DRYRUN enabled: DB updates, LINE pushes, and virtual trades are skipped")
    watch_days_limit = int(cfg.get("watch_days_limit", 5))

    try:
        res = (
            supabase.table("stock_drop_watchlist")
            .select("*")
            .in_("status", ["watching", "rebound_signal"])
            .execute()
        )
        watchlist = res.data or []
    except Exception as e:
        logger.error("watchlist fetch failed: %s", e)
        return

    logger.info("watchlist targets: %d", len(watchlist))
    to_notify: list[tuple[dict, dict, float, dict, dict]] = []

    for item in watchlist:
        code = item.get("code", "")
        item_id = item.get("id")
        prev_status = item.get("status", "watching")

        if not is_japanese_watchlist_item(item):
            logger.info(
                "skip non-japanese ticker in monitor_rebound: %s market=%s source_index=%s",
                code,
                item.get("market", ""),
                item.get("source_index", ""),
            )
            continue

        if prev_status == "watching":
            drop_detected_at = item.get("drop_detected_at")
            if drop_detected_at:
                try:
                    drop_dt = datetime.fromisoformat(str(drop_detected_at).replace("Z", "+00:00"))
                    biz = _biz_days(drop_dt, now_utc)
                    if biz > watch_days_limit:
                        if dry_run:
                            logger.info("DRYRUN watch expired: %s biz=%d", code, biz)
                        else:
                            supabase.table("stock_drop_watchlist").update({
                                "status": "closed",
                                "updated_at": now_utc.isoformat(),
                            }).eq("id", item_id).execute()
                        logger.info("watch expired: %s biz=%d", code, biz)
                        continue
                except Exception as e:
                    logger.warning("watch limit check error: %s %s", code, e)

        hist = _fetch_history(code, item.get("market", ""))
        if hist is None:
            if not dry_run:
                supabase.table("stock_drop_watchlist").update({
                    "last_checked_at": now_utc.isoformat(),
                    "updated_at": now_utc.isoformat(),
                }).eq("id", item_id).execute()
            continue

        closes, volumes = hist
        if len(closes) < 2:
            continue

        current = float(closes.iloc[-1])
        price_at_drop = _to_float(item.get("price_at_drop"))
        rebound = check_rebound(price_at_drop, closes, volumes, cfg)
        item_for_news = {
            **item,
            "volume_ratio": rebound.get("volume_ratio"),
        }
        bad_analysis = analyze_bad_news(item_for_news)
        is_excluded = bad_analysis.get("severity") == "strong"

        score_data = _get_score(item, closes, volumes, cfg, bad_analysis)
        score = float(score_data["total"])
        stage = determine_signal_stage(
            score,
            int(rebound["signal_count"]),
            bad_analysis.get("severity") == "strong",
            bool(is_excluded),
            cfg,
            _to_float(item.get("signal_probability"), None),
            market_adjustment,
        )

        if is_excluded:
            new_status = "excluded"
            exclude_reason = "強悪材料検出: " + (bad_analysis.get("reason") or "keyword matched")
        elif stage in SIGNAL_STAGES:
            new_status = "rebound_signal"
            exclude_reason = None
        elif prev_status == "rebound_signal":
            new_status = "rebound_signal"
            exclude_reason = None
        else:
            new_status = "watching"
            exclude_reason = None

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
            "has_bad_news": bool(bad_analysis.get("has_bad_news")),
            "price_history": closes_list,
            "per": score_data.get("per"),
            "pbr": score_data.get("pbr"),
            "div_yield_pct": score_data.get("div_yield_pct"),
            "signal_stage": stage,
            "signal_score": score,
            "bad_news_score": float(bad_analysis.get("bad_news_score") or 0),
            "energy_naphtha_score": float(bad_analysis.get("energy_naphtha_score") or 0),
            "exclude_reason": exclude_reason,
            "is_excluded": bool(is_excluded),
            "excluded_at": now_utc.isoformat() if is_excluded else None,
            "last_signal_at": now_utc.isoformat() if stage in SIGNAL_STAGES else item.get("last_signal_at"),
            "signal_count": int(rebound["signal_count"]),
            "mode": item.get("mode") or "normal",
            "market_regime": market_adjustment["regime"],
            "market_regime_label": market_adjustment["label"],
            "market_threshold_adjust": market_adjustment["ai_threshold_adjust"],
            "market_regime_reason": market_adjustment["reason"],
        }

        if not dry_run:
            try:
                supabase.table("stock_drop_watchlist").update(update_data).eq("id", item_id).execute()
            except Exception as e:
                logger.error("watchlist update error: %s %s", code, e)
                continue

        logger.info(
            "checked: %s %s->%s stage=%s count=%s score=%.0f bad=%s "
            "day=%s from_drop=%s vol=%s rsi=%s reasons=%s exclude_reason=%s",
            code,
            prev_status,
            new_status,
            stage,
            rebound["signal_count"],
            score,
            bad_analysis.get("severity"),
            f"{rebound.get('day_rebound_pct'):.1f}" if rebound.get("day_rebound_pct") is not None else "None",
            f"{rebound.get('from_drop_pct'):.1f}" if rebound.get("from_drop_pct") is not None else "None",
            f"{rebound.get('volume_ratio'):.1f}" if rebound.get("volume_ratio") is not None else "None",
            f"{rebound.get('rsi'):.0f}" if rebound.get("rsi") is not None else "None",
            rebound.get("signal_reasons") or [],
            exclude_reason,
        )
        logger.info(
            "[market_regime_save] code=%s regime=%s adjust=%s",
            code,
            market_adjustment.get("regime"),
            market_adjustment.get("ai_threshold_adjust"),
        )

        notified_stage = item.get("signal_stage")
        should_notify = (
            stage in SIGNAL_STAGES
            and (dry_run or not item.get("rebound_notified_at"))
            and (dry_run or notified_stage != stage)
            and not is_excluded
        )
        if should_notify:
            notify_item = {**item, **update_data}
            to_notify.append((notify_item, rebound, current, score_data, bad_analysis))
            if dry_run:
                logger.info(
                    "DRYRUN signal candidate: %s stage=%s score=%.0f count=%s reasons=%s",
                    code,
                    stage,
                    score,
                    rebound.get("signal_count"),
                    rebound.get("signal_reasons") or [],
                )

        if stage in SIGNAL_STAGES and not is_excluded and not dry_run:
            trade_item = {**item, **update_data}
            trade_item["entry_size_multiplier"] = market_adjustment["entry_size_multiplier"]
            _create_virtual_trade(trade_item, current, score, now_utc, rebound, bad_analysis)

    _manage_virtual_trades(cfg, now_utc, dry_run=dry_run)

    logger.info("new signals: %d", len(to_notify))
    if not to_notify:
        logger.info("=== rebound monitor complete ===")
        return
    if dry_run:
        logger.info("DRYRUN complete: %d signal candidates, no notifications sent", len(to_notify))
        return
    if force_no_notify:
        logger.info("force_no_notify=True; LINE skipped")
        logger.info("=== rebound monitor complete ===")
        return
    if not cfg.get("rebound_notify_enabled", False):
        logger.info("rebound_notify_enabled=False; LINE skipped")
        logger.info("=== rebound monitor complete ===")
        return

    users = _eligible_users()
    logger.info("notify users: %d", len(users))
    sent_any = False
    messages = _build_signal_digest(to_notify, cfg)
    for msg in messages:
        sent = sum(1 for u in users if _push(u["user_id"], msg))
        sent_any = sent_any or sent > 0
        logger.info("signal digest sent users=%d", sent)

    if sent_any or not users:
        for item, rebound, _current, _score_data, _bad_analysis in to_notify:
            try:
                supabase.table("stock_drop_watchlist").update({
                    "status": "rebound_signal",
                    "rebound_notified_at": now_utc.isoformat(),
                    "last_signal_at": now_utc.isoformat(),
                    "signal_count": int(rebound.get("signal_count") or 0),
                    "updated_at": now_utc.isoformat(),
                }).eq("id", item.get("id")).execute()
            except Exception as e:
                logger.error("notified mark update error: %s %s", item.get("code"), e)

    logger.info("=== rebound monitor complete sent=%s ===", sent_any)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Japanese rebound candidates")
    parser.add_argument(
        "--smoke-relaxed",
        action="store_true",
        help="Temporarily use relaxed thresholds for smoke testing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not update DB, send LINE messages, or create virtual trades.",
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Run monitor updates but skip LINE notifications.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_monitor(smoke_relaxed=args.smoke_relaxed, dry_run=args.dry_run, force_no_notify=args.no_notify)
