"""Long-term market structure classification.

This module is intentionally separate from the short-term market_regime logic.
Short-term regime is about a few days of supply/demand. Long-term regime is a
half-year to one-year market structure snapshot.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

LONG_TERM_LABELS = {
    "secular_risk_on": "長期上昇基調",
    "late_bull": "上昇継続・やや過熱",
    "distribution": "指数は耐えるが個別は弱い",
    "secular_bear": "長期下落基調",
    "panic_crisis": "危機的な全面安",
    "neutral": "中立",
}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _pct(value: float | None, digits: int = 1) -> str:
    return "-" if value is None else f"{value:.{digits}f}%"


def evaluate_long_term_market_regime(metrics: dict | None) -> dict:
    m = metrics or {}
    nikkei_above = bool(m.get("nikkei_above_200ma")) if m.get("nikkei_above_200ma") is not None else None
    topix_above = bool(m.get("topix_above_200ma")) if m.get("topix_above_200ma") is not None else None
    ma25_ratio = _to_float(m.get("ma25_above_ratio"))
    ma75_ratio = _to_float(m.get("ma75_above_ratio"))
    vix = _to_float(m.get("vix"))
    nikkei_gap = _to_float(m.get("nikkei_200ma_gap_pct"))
    topix_gap = _to_float(m.get("topix_200ma_gap_pct"))
    nikkei_change = _to_float(m.get("nikkei_change_pct"))
    topix_change = _to_float(m.get("topix_change_pct"))

    index_above_count = sum(1 for x in (nikkei_above, topix_above) if x is True)
    index_below_count = sum(1 for x in (nikkei_above, topix_above) if x is False)
    broad_weak = ma25_ratio is not None and ma25_ratio < 0.50
    broad_strong = ma25_ratio is not None and ma25_ratio >= 0.65 and (ma75_ratio is None or ma75_ratio >= 0.60)
    high_vol = vix is not None and vix >= 20
    crisis_vol = vix is not None and vix >= 30
    index_overheated = max([x for x in (nikkei_gap, topix_gap) if x is not None], default=0.0) >= 10.0
    daily_crash = any(x is not None and x <= -4.0 for x in (nikkei_change, topix_change))

    reasons: list[str] = []
    if nikkei_above is not None:
        reasons.append(f"日経200MA{'上' if nikkei_above else '下'}({_pct(nikkei_gap)})")
    if topix_above is not None:
        reasons.append(f"TOPIX200MA{'上' if topix_above else '下'}({_pct(topix_gap)})")
    if ma25_ratio is not None:
        reasons.append(f"MA25上銘柄率{ma25_ratio:.0%}")
    if ma75_ratio is not None:
        reasons.append(f"MA75上銘柄率{ma75_ratio:.0%}")
    if vix is not None:
        reasons.append(f"VIX {vix:.1f}")

    regime = "neutral"
    score = 45

    if crisis_vol and (daily_crash or (broad_weak and index_below_count >= 1)):
        regime = "panic_crisis"
        score = 90
        reasons.append("VIX急騰と市場内部悪化")
    elif index_below_count >= 2 and (broad_weak or (ma75_ratio is not None and ma75_ratio < 0.45)):
        regime = "secular_bear"
        score = 78
        reasons.append("指数200MA下かつ市場内部が弱い")
    elif index_above_count >= 1 and broad_weak:
        regime = "distribution"
        score = 68 + (8 if high_vol else 0)
        reasons.append("指数は耐える一方で市場内部が弱い")
    elif index_above_count >= 2 and broad_strong and (high_vol or index_overheated):
        regime = "late_bull"
        score = 68 + (8 if index_overheated else 0) + (6 if high_vol else 0)
        reasons.append("指数は強いが過熱またはボラ上昇")
    elif index_above_count >= 2 and broad_strong and not high_vol:
        regime = "secular_risk_on"
        score = 76
        reasons.append("指数200MA上かつ市場内部も強い")
    elif index_below_count >= 1 and broad_weak:
        regime = "secular_bear"
        score = 62
        reasons.append("指数弱含みと市場内部悪化")
    else:
        reasons.append("長期構造は中立")

    score = max(0, min(100, int(round(score))))
    return {
        "regime": regime,
        "label": LONG_TERM_LABELS.get(regime, regime),
        "score": score,
        "reasons": reasons,
        "metrics": m,
    }


def _latest_feature_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return str(rows[0]["trade_date"]) if rows else None


def _market_breadth_metrics(sb, trade_date: str) -> dict:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("close,ma25,ma75,day_change_pct")
        .eq("trade_date", trade_date)
        .execute()
        .data or []
    )
    ma25_total = ma75_total = adv_total = 0
    ma25_above = ma75_above = advancers = decliners = 0
    for row in rows:
        close = _to_float(row.get("close"))
        ma25 = _to_float(row.get("ma25"))
        ma75 = _to_float(row.get("ma75"))
        day_change = _to_float(row.get("day_change_pct"))
        if close is not None and ma25 is not None and ma25 > 0:
            ma25_total += 1
            if close >= ma25:
                ma25_above += 1
        if close is not None and ma75 is not None and ma75 > 0:
            ma75_total += 1
            if close >= ma75:
                ma75_above += 1
        if day_change is not None:
            adv_total += 1
            if day_change > 0:
                advancers += 1
            elif day_change < 0:
                decliners += 1
    return {
        "ma25_above_ratio": ma25_above / ma25_total if ma25_total else None,
        "ma75_above_ratio": ma75_above / ma75_total if ma75_total else None,
        "advancers_ratio": advancers / adv_total if adv_total else None,
        "decliners_ratio": decliners / adv_total if adv_total else None,
        "snapshot_count": len(rows),
    }


def _fetch_index_structure(ticker: str, *, as_of: str | None = None) -> dict:
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[long_term_market_regime] yfinance not installed")
        return {}

    try:
        if as_of:
            d = date.fromisoformat(str(as_of)[:10])
            hist = yf.Ticker(ticker).history(
                start=(d - timedelta(days=430)).isoformat(),
                end=(d + timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=True,
            )
        else:
            hist = yf.Ticker(ticker).history(period="18mo", interval="1d", auto_adjust=True)
        if hist is None or hist.empty:
            return {}
        close = hist["Close"].dropna()
        if len(close) < 2:
            return {}
        value = float(close.iloc[-1])
        prev = float(close.iloc[-2])
        ma200 = float(close.tail(200).mean()) if len(close) >= 200 else None
        high_52w = float(close.tail(252).max()) if len(close) >= 20 else None
        return {
            "close": value,
            "change_pct": (value / prev - 1.0) * 100.0 if prev > 0 else None,
            "ma200": ma200,
            "above_200ma": value >= ma200 if ma200 else None,
            "ma200_gap_pct": (value / ma200 - 1.0) * 100.0 if ma200 else None,
            "high_52w_pct": (value / high_52w) * 100.0 if high_52w else None,
            "date": close.index[-1].strftime("%Y-%m-%d"),
        }
    except Exception as e:
        logger.warning("[long_term_market_regime] index fetch failed ticker=%s: %s", ticker, e)
        return {}


def collect_long_term_market_metrics(sb, *, trade_date: str | None = None) -> dict:
    trade_date = trade_date or _latest_feature_date(sb)
    if not trade_date:
        raise RuntimeError("stock_feature_snapshots has no trade_date")

    breadth = _market_breadth_metrics(sb, trade_date)
    nikkei = _fetch_index_structure("^N225", as_of=trade_date)
    topix = {}
    for ticker in ("^TOPX", "^TOPIX", "1306.T"):
        topix = _fetch_index_structure(ticker, as_of=trade_date)
        if topix:
            break
    vix = _fetch_index_structure("^VIX", as_of=trade_date)

    return {
        "trade_date": trade_date,
        "nikkei_close": nikkei.get("close"),
        "nikkei_200ma": nikkei.get("ma200"),
        "nikkei_above_200ma": nikkei.get("above_200ma"),
        "nikkei_200ma_gap_pct": nikkei.get("ma200_gap_pct"),
        "nikkei_52w_high_pct": nikkei.get("high_52w_pct"),
        "nikkei_change_pct": nikkei.get("change_pct"),
        "topix_close": topix.get("close"),
        "topix_200ma": topix.get("ma200"),
        "topix_above_200ma": topix.get("above_200ma"),
        "topix_200ma_gap_pct": topix.get("ma200_gap_pct"),
        "topix_52w_high_pct": topix.get("high_52w_pct"),
        "topix_change_pct": topix.get("change_pct"),
        "vix": vix.get("close"),
        "vix_change_pct": vix.get("change_pct"),
        **breadth,
    }


def build_long_term_market_regime(sb, *, trade_date: str | None = None) -> dict:
    metrics = collect_long_term_market_metrics(sb, trade_date=trade_date)
    result = evaluate_long_term_market_regime(metrics)
    return {**metrics, **result}


def upsert_long_term_market_regime(sb, *, trade_date: str | None = None, dry_run: bool = False) -> dict:
    result = build_long_term_market_regime(sb, trade_date=trade_date)
    metrics = result.get("metrics") or {}
    payload = {
        "trade_date": result["trade_date"],
        "regime": result["regime"],
        "label": result["label"],
        "score": result["score"],
        "nikkei_above_200ma": metrics.get("nikkei_above_200ma"),
        "topix_above_200ma": metrics.get("topix_above_200ma"),
        "nikkei_200ma_gap_pct": metrics.get("nikkei_200ma_gap_pct"),
        "topix_200ma_gap_pct": metrics.get("topix_200ma_gap_pct"),
        "ma25_above_ratio": metrics.get("ma25_above_ratio"),
        "ma75_above_ratio": metrics.get("ma75_above_ratio"),
        "advancers_ratio": metrics.get("advancers_ratio"),
        "decliners_ratio": metrics.get("decliners_ratio"),
        "vix": metrics.get("vix"),
        "vix_change_pct": metrics.get("vix_change_pct"),
        "reasons": result.get("reasons") or [],
        "metrics": metrics,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "[long_term_market_regime] date=%s regime=%s score=%s ma25=%s ma75=%s vix=%s",
        payload["trade_date"],
        payload["regime"],
        payload["score"],
        payload["ma25_above_ratio"],
        payload["ma75_above_ratio"],
        payload["vix"],
    )
    if not dry_run:
        try:
            sb.table("long_term_market_regime").upsert(payload, on_conflict="trade_date").execute()
        except Exception as e:
            msg = str(e)
            if "long_term_market_regime" in msg or "Could not find" in msg or "relation" in msg:
                logger.warning(
                    "[long_term_market_regime] table missing; run db/long_term_market_regime.sql: %s",
                    e,
                )
            else:
                raise
    return payload
