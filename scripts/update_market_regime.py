#!/usr/bin/env python3
"""Update daily market_regime rows and optionally apply scores to features."""
import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

try:
    import pandas as pd
    import yfinance as yf
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from supabase import create_client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _target_dates(args: argparse.Namespace) -> list[date]:
    if args.today:
        return [datetime.now(JST).date()]
    if args.date:
        return [datetime.strptime(args.date, "%Y-%m-%d").date()]
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else datetime.now(JST).date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else start
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _fetch_index(ticker: str, d: date) -> tuple[float | None, float | None]:
    try:
        hist = yf.Ticker(ticker).history(start=(d - timedelta(days=10)).isoformat(), end=(d + timedelta(days=1)).isoformat(), interval="1d")
        if hist is None or len(hist) < 2:
            return None, None
        close = hist["Close"].dropna()
        value = float(close.iloc[-1])
        pct = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1.0) * 100.0
        return value, pct
    except Exception as e:
        logger.warning("index fetch failed ticker=%s date=%s: %s", ticker, d, e)
        return None, None


def _market_breadth(sb, d: date) -> tuple[float | None, float | None]:
    try:
        rows = (
            sb.table("stock_feature_snapshots")
            .select("day_change_pct")
            .eq("trade_date", d.isoformat())
            .execute()
            .data or []
        )
        vals = [float(r["day_change_pct"]) for r in rows if r.get("day_change_pct") is not None]
        if not vals:
            return None, None
        decliners = sum(1 for v in vals if v < 0) / len(vals)
        advancers = sum(1 for v in vals if v > 0) / len(vals)
        return decliners, advancers
    except Exception as e:
        logger.warning("breadth fetch failed date=%s: %s", d, e)
        return None, None


def _news_scores(sb, d: date) -> dict:
    cols = "market_shock_score,geopolitical_score,interest_rate_score,fx_yen_score,energy_naphtha_score"
    try:
        rows = sb.table("market_news_signals").select(cols).eq("signal_date", d.isoformat()).execute().data or []
    except Exception as e:
        logger.warning("market_news_signals fetch failed date=%s: %s", d, e)
        rows = []
    result = {c: 0.0 for c in cols.split(",")}
    for c in result:
        result[c] = max(float(r.get(c) or 0) for r in rows) if rows else 0.0
    return result


def _previous_modes(sb, d: date) -> list[str]:
    try:
        start = (d - timedelta(days=7)).isoformat()
        rows = (
            sb.table("market_regime")
            .select("mode,trade_date")
            .gte("trade_date", start)
            .lt("trade_date", d.isoformat())
            .order("trade_date", desc=True)
            .limit(3)
            .execute()
            .data or []
        )
        return [str(r.get("mode") or "normal") for r in rows]
    except Exception:
        return []


def _score_and_mode(metrics: dict, prev_modes: list[str]) -> tuple[str, float, list[str]]:
    score = 0.0
    panic_hits = 0
    cond: list[str] = []

    def add(name: str, points: float):
        nonlocal score
        score += points
        cond.append(name)

    n = metrics.get("nikkei_change_pct")
    t = metrics.get("topix_change_pct")
    dec = metrics.get("decliners_ratio")
    adv = metrics.get("advancers_ratio")
    vix = metrics.get("vix_value")
    vix_chg = metrics.get("vix_change_pct")
    nvi = metrics.get("nikkei_vi_value")
    nvi_chg = metrics.get("nikkei_vi_change_pct")
    mshock = metrics.get("market_shock_score") or 0
    geo = metrics.get("geopolitical_score") or 0
    rate = metrics.get("interest_rate_score") or 0

    market_data_stress = any([
        n is not None and n <= -1.0,
        t is not None and t <= -1.0,
        dec is not None and dec >= 0.65,
        vix is not None and vix >= 20,
        vix_chg is not None and vix_chg >= 10,
        nvi is not None and nvi >= 25,
        nvi_chg is not None and nvi_chg >= 15,
    ])
    panic_market_data_stress = any([
        n is not None and n <= -4.0,
        t is not None and t <= -4.0,
        dec is not None and dec >= 0.90,
        vix is not None and vix >= 30,
        vix_chg is not None and vix_chg >= 30,
        nvi is not None and nvi >= 35,
        nvi_chg is not None and nvi_chg >= 30,
    ])
    news_scores_high = any([
        mshock >= 50,
        geo >= 50,
        rate >= 50,
    ])
    cond.append(f"market_data_stress={str(market_data_stress).lower()}")
    cond.append(f"panic_market_data_stress={str(panic_market_data_stress).lower()}")
    cond.append(f"news_scores_high={str(news_scores_high).lower()}")

    if n is not None and n <= -2.0: add("nikkei<=-2", 20)
    if t is not None and t <= -2.0: add("topix<=-2", 20)
    if dec is not None and dec >= 0.75: add("decliners>=75%", 20)
    if vix is not None and vix >= 20: add("vix>=20", 10)
    if vix_chg is not None and vix_chg >= 15: add("vix_change>=15", 10)
    if mshock >= 50: add("market_shock>=50", 20)
    if geo >= 50: add("geopolitical>=50", 10)
    if rate >= 50: add("interest_rate>=50", 10)

    if n is not None and n <= -4.0: panic_hits += 1
    if t is not None and t <= -4.0: panic_hits += 1
    if dec is not None and dec >= 0.90: panic_hits += 1
    if vix is not None and vix >= 30: panic_hits += 1
    if vix_chg is not None and vix_chg >= 30: panic_hits += 1
    if nvi is not None and nvi >= 35: panic_hits += 1
    if nvi_chg is not None and nvi_chg >= 30: panic_hits += 1

    score = min(100.0, score)
    mode = "normal"
    if panic_market_data_stress and (score >= 70 or panic_hits >= 2):
        mode = "panic"
    elif market_data_stress and score >= 40:
        mode = "shock"
    if mode == "normal" and any(m in {"shock", "panic"} for m in prev_modes):
        if ((t is not None and t >= 1.0) or (n is not None and n >= 1.0)) and (adv is None or adv >= 0.60):
            mode = "recovery"
            cond.append("recent_shock_and_rebound")
    return mode, score, cond


def build_regime(sb, d: date) -> dict:
    _nikkei_value, nikkei_pct = _fetch_index("^N225", d)
    _topix_value, topix_pct = _fetch_index("1306.T", d)
    vix_value, vix_pct = _fetch_index("^VIX", d)
    decliners, advancers = _market_breadth(sb, d)
    news = _news_scores(sb, d)
    metrics = {
        "trade_date": d.isoformat(),
        "nikkei_change_pct": nikkei_pct,
        "topix_change_pct": topix_pct,
        "decliners_ratio": decliners,
        "advancers_ratio": advancers,
        "vix_value": vix_value,
        "vix_change_pct": vix_pct,
        "nikkei_vi_value": None,
        "nikkei_vi_change_pct": None,
        **news,
    }
    mode, shock_score, cond = _score_and_mode(metrics, _previous_modes(sb, d))
    metrics.update({
        "mode": mode,
        "shock_score": shock_score,
        "reason": ", ".join(cond) if cond else "no shock conditions",
        "matched_conditions": cond,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return metrics


def apply_to_features(sb, row: dict, dry_run: bool) -> int:
    update = {
        "market_shock_score": row.get("market_shock_score") or row.get("shock_score") or 0,
        "fx_yen_score": row.get("fx_yen_score") or 0,
        "energy_naphtha_score": row.get("energy_naphtha_score") or 0,
        "interest_rate_score": row.get("interest_rate_score") or 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if dry_run:
        logger.info("DRYRUN apply_to_features: date=%s update=%s", row["trade_date"], update)
        return 0
    res = sb.table("stock_feature_snapshots").update(update).eq("trade_date", row["trade_date"]).execute()
    return len(res.data or [])


def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas and yfinance are required")
    sb = _build_supabase()
    for d in _target_dates(args):
        logger.info("start market regime update: date=%s", d)
        row = build_regime(sb, d)
        logger.info(
            "mode=%s shock_score=%.0f nikkei=%s topix=%s decliners=%s vix=%s reason=%s",
            row["mode"], row["shock_score"], row.get("nikkei_change_pct"), row.get("topix_change_pct"),
            row.get("decliners_ratio"), row.get("vix_value"), row.get("reason"),
        )
        if args.dry_run:
            logger.info("DRYRUN upsert market_regime: %s", row)
        else:
            sb.table("market_regime").upsert(row, on_conflict="trade_date").execute()
            logger.info("upsert market_regime: date=%s mode=%s", d, row["mode"])
        if args.apply_to_features:
            updated = apply_to_features(sb, row, args.dry_run)
            logger.info("apply_to_features: date=%s updated_rows=%d", d, updated)
    logger.info("complete errors=0")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update market regime")
    p.add_argument("--date")
    p.add_argument("--today", action="store_true")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--apply-to-features", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
