#!/usr/bin/env python3
"""Backfill market_regime table from 2020 to present with 6-mode classification."""
import argparse
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone

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

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

def _build_supabase():
    mode = os.getenv("SUPABASE_MODE", "").strip() or os.getenv("ENV", "").strip()
    mode_upper = mode.upper() if mode else ""
    url = (os.getenv(f"SUPABASE_URL_{mode_upper}", "").strip() if mode_upper else "") or os.getenv("SUPABASE_URL", "").strip()
    key = (os.getenv(f"SUPABASE_KEY_{mode_upper}", "").strip() if mode_upper else "") or os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY not set")
    return create_client(url, key)


# ---------------------------------------------------------------------------
# Price data
# ---------------------------------------------------------------------------

def _fetch_prices(start: date, end: date) -> pd.DataFrame:
    """Download Nikkei + TOPIX-proxy and return a merged DataFrame indexed by date."""
    fetch_start = (start - timedelta(days=90)).isoformat()  # extra buffer for MA25 + warm-up
    fetch_end = (end + timedelta(days=1)).isoformat()

    logger.info("Fetching ^N225 %s -> %s", fetch_start, fetch_end)
    nk_raw = yf.Ticker("^N225").history(start=fetch_start, end=fetch_end, interval="1d")
    logger.info("Fetching 1306.T %s -> %s", fetch_start, fetch_end)
    tp_raw = yf.Ticker("1306.T").history(start=fetch_start, end=fetch_end, interval="1d")

    if nk_raw is None or nk_raw.empty:
        raise RuntimeError("yfinance returned no data for ^N225")
    if tp_raw is None or tp_raw.empty:
        logger.warning("yfinance returned no data for 1306.T; topix columns will be NaN")
        tp_raw = pd.DataFrame(columns=["Close"])

    nk = nk_raw[["Close"]].rename(columns={"Close": "nikkei_close"})
    tp = tp_raw[["Close"]].rename(columns={"Close": "topix_close"})

    # Normalize index to date only (remove tz)
    nk.index = pd.to_datetime(nk.index).tz_localize(None).normalize()
    tp.index = pd.to_datetime(tp.index).tz_localize(None).normalize()

    df = nk.join(tp, how="outer").sort_index()

    # Daily returns
    df["nikkei_pct"] = df["nikkei_close"].pct_change() * 100.0
    df["topix_pct"] = df["topix_close"].pct_change() * 100.0

    # MA25
    df["nikkei_ma25"] = df["nikkei_close"].rolling(25, min_periods=1).mean()
    df["topix_ma25"] = df["topix_close"].rolling(25, min_periods=1).mean()

    # MA25 gap (%)
    df["nikkei_ma25_gap"] = (df["nikkei_close"] - df["nikkei_ma25"]) / df["nikkei_ma25"] * 100.0
    df["topix_ma25_gap"] = (df["topix_close"] - df["topix_ma25"]) / df["topix_ma25"] * 100.0

    # 20-day annualised volatility
    df["nikkei_ret"] = df["nikkei_close"].pct_change()
    df["volatility_score"] = df["nikkei_ret"].rolling(20, min_periods=5).std() * math.sqrt(252) * 100.0

    return df


# ---------------------------------------------------------------------------
# Mode classification
# ---------------------------------------------------------------------------

def _count_consecutive_down(df: pd.DataFrame, idx: int) -> int:
    """Count consecutive trading days with nikkei_pct < 0 ending at position idx."""
    count = 0
    i = idx
    while i >= 0 and not pd.isna(df["nikkei_pct"].iloc[i]) and df["nikkei_pct"].iloc[i] < 0:
        count += 1
        i -= 1
    return count


def _compute_panic_score(nikkei_pct: float, nikkei_ma25_gap: float, consecutive_down: int) -> float:
    """Composite 0-100 panic score."""
    score = 0.0
    # Large single-day drops
    if nikkei_pct <= -4.0:
        score += 40.0
    elif nikkei_pct <= -2.5:
        score += 25.0
    elif nikkei_pct <= -1.5:
        score += 10.0
    # MA25 gap contribution
    if nikkei_ma25_gap <= -6.0:
        score += 35.0
    elif nikkei_ma25_gap <= -3.0:
        score += 20.0
    elif nikkei_ma25_gap <= -1.0:
        score += 8.0
    # Consecutive down days
    if consecutive_down >= 5:
        score += 25.0
    elif consecutive_down >= 3:
        score += 15.0
    elif consecutive_down >= 2:
        score += 5.0
    return min(100.0, score)


def _classify_mode(
    nikkei_pct: float,
    nikkei_ma25_gap: float,
    panic_score: float,
    days_since_last_panic: int | None,
) -> str:
    """Apply priority-ordered 6-mode classification."""
    # 1. euphoria  (highest priority)
    if nikkei_ma25_gap > 8:
        return "euphoria"
    # 2. strong_risk_on
    if nikkei_ma25_gap > 2 and nikkei_pct >= -0.5:
        return "strong_risk_on"
    # 3. panic_selloff
    if nikkei_pct < -2.5 and (nikkei_ma25_gap < -3 or nikkei_pct < -4):
        return "panic_selloff"
    # 4. panic_rebound
    if days_since_last_panic is not None and days_since_last_panic <= 15 and nikkei_pct > 1.5:
        return "panic_rebound"
    # 5. risk_off
    if nikkei_ma25_gap < -3 or (nikkei_pct < -1 and nikkei_ma25_gap < 0):
        return "risk_off"
    # 6. normal (default)
    return "normal"


# ---------------------------------------------------------------------------
# Existing dates query
# ---------------------------------------------------------------------------

def _fetch_existing_dates(sb, start_s: str, end_s: str) -> set:
    existing = (
        sb.table("market_regime")
        .select("trade_date")
        .gte("trade_date", start_s)
        .lte("trade_date", end_s)
        .execute()
        .data
    )
    return {r["trade_date"] for r in (existing or [])}


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas and yfinance are required: pip install pandas yfinance")

    start: date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end: date = datetime.strptime(args.end, "%Y-%m-%d").date()
    start_s = start.isoformat()
    end_s = end.isoformat()

    logger.info("backfill_market_regime start=%s end=%s force=%s dry_run=%s batch_size=%d",
                start_s, end_s, args.force, args.dry_run, args.batch_size)

    # Fetch price data (includes warm-up window before `start`)
    try:
        df = _fetch_prices(start, end)
    except Exception as exc:
        logger.error("Failed to fetch price data: %s", exc)
        raise

    if df.empty:
        logger.warning("No price data returned — aborting")
        return

    sb = None if args.dry_run else _build_supabase()

    # Determine which dates already exist (skip unless --force)
    existing_dates: set = set()
    if not args.force and not args.dry_run:
        try:
            existing_dates = _fetch_existing_dates(sb, start_s, end_s)
            logger.info("Found %d existing trade_dates in range (will skip)", len(existing_dates))
        except Exception as exc:
            logger.warning("Could not fetch existing dates: %s — proceeding without skip", exc)

    # Restrict DataFrame index to business days within [start, end]
    df_range = df.loc[
        (df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))
    ]

    if df_range.empty:
        logger.warning("No trading days found in %s .. %s after yfinance fetch", start_s, end_s)
        return

    # Build full df positions for consecutive-down-day counting (includes warm-up)
    all_positions = {ts: i for i, ts in enumerate(df.index)}

    # Track last panic_selloff index (position in full df)
    last_panic_selloff_pos: int | None = None

    batch: list[dict] = []
    total_written = 0
    total_skipped = 0
    error_count = 0
    utcnow = datetime.now(timezone.utc).isoformat()

    for ts, row in df_range.iterrows():
        date_str = ts.strftime("%Y-%m-%d")

        # Skip if exists and not forcing
        if date_str in existing_dates:
            total_skipped += 1
            continue

        nikkei_pct = row.get("nikkei_pct")
        topix_pct = row.get("topix_pct")
        nikkei_ma25_gap = row.get("nikkei_ma25_gap")
        topix_ma25_gap = row.get("topix_ma25_gap")
        volatility_score = row.get("volatility_score")

        # Skip holiday / missing price rows
        if pd.isna(nikkei_pct) or pd.isna(nikkei_ma25_gap):
            continue

        # Safe float conversion
        def _f(v):
            return None if (v is None or (isinstance(v, float) and math.isnan(v))) else round(float(v), 4)

        nikkei_pct_f = _f(nikkei_pct)
        topix_pct_f = _f(topix_pct)
        nikkei_ma25_gap_f = _f(nikkei_ma25_gap)
        topix_ma25_gap_f = _f(topix_ma25_gap)
        volatility_score_f = _f(volatility_score)

        # Consecutive down days
        pos = all_positions.get(ts, 0)
        consec_down = _count_consecutive_down(df, pos)

        # Panic score
        panic_score = _compute_panic_score(nikkei_pct_f or 0.0, nikkei_ma25_gap_f or 0.0, consec_down)

        # Days since last panic_selloff (in trading-day count within df_range)
        days_since_panic: int | None = None
        if last_panic_selloff_pos is not None:
            # Count trading days between last panic pos and current pos in full df
            days_since_panic = pos - last_panic_selloff_pos

        mode = _classify_mode(
            nikkei_pct_f or 0.0,
            nikkei_ma25_gap_f or 0.0,
            panic_score,
            days_since_panic,
        )

        if mode == "panic_selloff":
            last_panic_selloff_pos = pos

        record = {
            "trade_date": date_str,
            "mode": mode,
            "nikkei_change_pct": nikkei_pct_f,
            "topix_change_pct": topix_pct_f,
            "nikkei_ma25_gap": nikkei_ma25_gap_f,
            "topix_ma25_gap": topix_ma25_gap_f,
            "volatility_score": volatility_score_f,
            "panic_score": round(panic_score, 2),
            "updated_at": utcnow,
        }

        logger.info("date=%s mode=%-15s nikkei_pct=%6.2f ma25_gap=%6.2f panic_score=%5.1f",
                    date_str, mode, nikkei_pct_f or 0, nikkei_ma25_gap_f or 0, panic_score)

        if args.dry_run:
            total_written += 1
            continue

        batch.append(record)

        if len(batch) >= args.batch_size:
            try:
                sb.table("market_regime").upsert(batch, on_conflict="trade_date").execute()
                logger.info("upserted batch of %d rows (last=%s)", len(batch), batch[-1]["trade_date"])
                total_written += len(batch)
            except Exception as exc:
                logger.error("upsert error (batch ending %s): %s", batch[-1]["trade_date"], exc)
                error_count += 1
            batch = []

    # Flush remaining batch
    if batch and not args.dry_run:
        try:
            sb.table("market_regime").upsert(batch, on_conflict="trade_date").execute()
            logger.info("upserted final batch of %d rows (last=%s)", len(batch), batch[-1]["trade_date"])
            total_written += len(batch)
        except Exception as exc:
            logger.error("upsert error (final batch): %s", exc)
            error_count += 1
    elif args.dry_run:
        pass  # total_written already counted row by row above

    logger.info(
        "done: written=%d skipped=%d errors=%d dry_run=%s",
        total_written, total_skipped, error_count, args.dry_run,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    today = date.today().isoformat()
    p = argparse.ArgumentParser(description="Backfill market_regime table with 6-mode classification")
    p.add_argument("--start", default="2020-01-01", help="Start date YYYY-MM-DD (default: 2020-01-01)")
    p.add_argument("--end", default=today, help=f"End date YYYY-MM-DD (default: today={today})")
    p.add_argument("--force", action="store_true", help="Overwrite existing rows (default: skip)")
    p.add_argument("--dry-run", action="store_true", help="Log only, no DB writes")
    p.add_argument("--batch-size", type=int, default=100, help="Upsert batch size (default: 100)")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
