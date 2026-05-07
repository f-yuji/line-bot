"""Fetch latest Nikkei/TOPIX data and keep market_regime table up to date."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _calc_shock_score(nikkei_pct: float | None) -> float:
    if nikkei_pct is None:
        return 0.0
    a = abs(nikkei_pct)
    if a >= 5:
        return 100.0
    if a >= 3:
        return 60.0
    if a >= 2:
        return 30.0
    return 0.0


def _warn_if_stale(trade_date_str: str, now_jst: datetime) -> None:
    try:
        td = date.fromisoformat(trade_date_str)
        delta = (now_jst.date() - td).days
        if delta >= 3:
            logger.warning(
                "[market_regime_stale] latest_trade_date=%s today=%s delta_days=%d",
                trade_date_str, now_jst.date().isoformat(), delta,
            )
    except Exception:
        pass


def update_market_regime_for_latest_trade_date(supabase, *, force: bool = False) -> dict | None:
    """
    Fetch latest Nikkei/TOPIX via yfinance and upsert market_regime table.

    nikkei_change_pct / topix_change_pct は %値で保存 (5.58 = 5.58%)。
    戻り値: upsert後のデータ dict、スキップ・失敗時は None。
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[market_regime_update] yfinance not installed; skipping")
        return None

    now_jst = datetime.now(JST)

    if now_jst.weekday() >= 5 and not force:
        logger.info("[market_regime_update] weekend skip")
        return None

    # ── Nikkei 225 取得 ──
    nikkei_close = nikkei_prev = None
    trade_date_str = now_jst.date().isoformat()

    try:
        hist_n = yf.Ticker("^N225").history(period="5d", interval="1d", auto_adjust=True)
        if len(hist_n) >= 2:
            nikkei_close = float(hist_n["Close"].iloc[-1])
            nikkei_prev = float(hist_n["Close"].iloc[-2])
            trade_date_str = hist_n.index[-1].strftime("%Y-%m-%d")
            logger.info(
                "[market_regime_update] N225 close=%.0f prev=%.0f date=%s",
                nikkei_close, nikkei_prev, trade_date_str,
            )
        else:
            logger.warning("[market_regime_update] N225 history too short (%d rows)", len(hist_n))
    except Exception as e:
        logger.warning("[market_regime_update] N225 fetch failed: %s", e)

    # ── 既存チェック (force=False のとき最新 trade_date が一致すればスキップ) ──
    if not force:
        try:
            existing = (
                supabase.table("market_regime")
                .select("trade_date,nikkei_change_pct,topix_change_pct,mode")
                .eq("trade_date", trade_date_str)
                .limit(1)
                .execute()
                .data or []
            )
            if existing:
                logger.info(
                    "[market_regime_update] already up to date for %s (nikkei=%s)",
                    trade_date_str, existing[0].get("nikkei_change_pct"),
                )
                _warn_if_stale(trade_date_str, now_jst)
                return existing[0]
        except Exception as e:
            logger.warning("[market_regime_update] existing check failed: %s", e)

    # ── 騰落率を %値で計算 ──
    nikkei_change_pct: float | None = None
    nikkei_change_yen: float | None = None
    if nikkei_close is not None and nikkei_prev is not None and nikkei_prev > 0:
        nikkei_change_pct = round((nikkei_close - nikkei_prev) / nikkei_prev * 100, 4)
        nikkei_change_yen = round(nikkei_close - nikkei_prev, 0)

    # ── TOPIX 取得 ──
    topix_change_pct: float | None = None
    for topix_ticker in ("^TOPX", "^TOPIX", "1306.T"):
        try:
            hist_t = yf.Ticker(topix_ticker).history(period="5d", interval="1d", auto_adjust=True)
            if len(hist_t) >= 2:
                t_close = float(hist_t["Close"].iloc[-1])
                t_prev = float(hist_t["Close"].iloc[-2])
                if t_prev > 0:
                    topix_change_pct = round((t_close - t_prev) / t_prev * 100, 4)
                logger.info(
                    "[market_regime_update] TOPIX(%s) close=%.2f prev=%.2f pct=%.4f",
                    topix_ticker, t_close, t_prev, topix_change_pct or 0,
                )
                break
        except Exception as e:
            logger.warning("[market_regime_update] TOPIX(%s) failed: %s", topix_ticker, e)

    if nikkei_change_pct is None and topix_change_pct is None:
        logger.warning("[market_regime_update] both Nikkei and TOPIX fetch failed; not updating")
        return None

    # ── regime 判定 ──
    from services.market_regime import evaluate_market_regime
    regime_result = evaluate_market_regime({
        "nikkei_change_pct": nikkei_change_pct,
        "topix_change_pct": topix_change_pct,
    })

    payload = {
        "trade_date": trade_date_str,
        "mode": regime_result["regime"],
        "nikkei_change_pct": nikkei_change_pct,
        "topix_change_pct": topix_change_pct,
        "shock_score": _calc_shock_score(nikkei_change_pct),
        "reason": regime_result["reason"],
    }

    # ── upsert ──
    try:
        supabase.table("market_regime").upsert(payload, on_conflict="trade_date").execute()
        logger.info(
            "[market_regime_update] trade_date=%s nikkei=%.2f topix=%s mode=%s",
            trade_date_str,
            nikkei_change_pct or 0,
            f"{topix_change_pct:.2f}" if topix_change_pct is not None else "None",
            regime_result["regime"],
        )
    except Exception as e:
        logger.error("[market_regime_update] upsert failed: %s", e)
        return None

    _warn_if_stale(trade_date_str, now_jst)

    return {**payload, **regime_result, "nikkei_change_yen": nikkei_change_yen}
