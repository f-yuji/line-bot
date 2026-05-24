#!/usr/bin/env python3
"""
Generate stock_feature_snapshots rows for Japanese rebound training data.

Phase 2 only builds feature rows. It does not create labels, train ML models,
send LINE messages, or alter the news bot.
"""
import argparse
import logging
import math
import os
import sys
import time
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

from jquants_client import get_daily_quotes, normalize_code
from prime_stocks import fetch_prime_from_jquants, get_prime_tickers
from settings_loader import get_settings
from services.trading_calendar import is_weekend, today_jst

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
INTERNAL_LOOKBACK_DAYS = 430
DEFAULT_BATCH_SIZE = 200
MIN_TRADE_PRICE = 100.0
MIN_TURNOVER_VALUE = 100_000_000.0


class StopOn429(RuntimeError):
    def __init__(self, code: str, original: Exception):
        super().__init__(f"STOP_ON_429 code={code}: {original}")
        self.code = code
        self.original = original


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


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(args: argparse.Namespace) -> tuple[date, date, date]:
    if args.date:
        end = datetime.now(JST).date() if str(args.date).lower() == "today" else _parse_date(args.date)
        start = end
        fetch_start = start - timedelta(days=INTERNAL_LOOKBACK_DAYS)
        return start, end, fetch_start
    end = _parse_date(args.end) or datetime.now(JST).date()
    if args.start:
        start = _parse_date(args.start)
    else:
        years = int(args.years or 1)
        start = end - timedelta(days=365 * years)
    fetch_start = start - timedelta(days=INTERNAL_LOOKBACK_DAYS)
    return start, end, fetch_start


def _is_alpha_code(code: str) -> bool:
    return bool(code) and code.isalpha()


def _normalize_code(code: Any) -> str:
    text = str(code or "").strip()
    if text.endswith(".T"):
        text = text[:-2]
    return text[:4] if text[:4].isdigit() else text


def _load_codes(sb, args: argparse.Namespace) -> list[dict]:
    if args.code:
        code = _normalize_code(args.code)
        if _is_alpha_code(code):
            logger.info("skip alphabetic ticker: %s", code)
            return []
        try:
            rows = (
                sb.table("prime_stocks_cache")
                .select("code, name, sector")
                .eq("code", code)
                .limit(1)
                .execute()
                .data or []
            )
            if rows:
                return [{"code": code, "name": rows[0].get("name", ""), "sector": rows[0].get("sector", "")}]
        except Exception as e:
            logger.debug("prime_stocks_cache lookup failed: %s", e)
        return [{"code": code, "name": "", "sector": ""}]

    if getattr(args, "source", "auto") == "jquants" and getattr(args, "dry_run", False):
        stocks = fetch_prime_from_jquants()
    else:
        stocks = get_prime_tickers(sb, force_refresh=(getattr(args, "source", "auto") == "jquants"))
    cleaned: list[dict] = []
    for s in stocks:
        code = _normalize_code(s.get("code"))
        if not code or _is_alpha_code(code):
            logger.info("skip alphabetic/non-japanese ticker: %s", code)
            continue
        cleaned.append({"code": code, "name": s.get("name", ""), "sector": s.get("sector", "")})

    cleaned = sorted(cleaned, key=lambda x: str(x.get("code", "")))
    if args.start_after_code:
        start_after = _normalize_code(args.start_after_code)
        cleaned = [s for s in cleaned if str(s.get("code", "")) > start_after]
    if args.code_from:
        code_from = _normalize_code(args.code_from)
        cleaned = [s for s in cleaned if str(s.get("code", "")) >= code_from]
    if args.code_to:
        code_to = _normalize_code(args.code_to)
        cleaned = [s for s in cleaned if str(s.get("code", "")) <= code_to]
    if args.limit:
        cleaned = cleaned[: int(args.limit)]
    return cleaned


def _fetch_yfinance_history(ticker: str, fetch_start: date, end: date) -> "pd.DataFrame":
    try:
        df = yf.Ticker(ticker).history(
            start=fetch_start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.copy()
        df.index = pd.to_datetime(df.index).tz_localize(None).date
        return df
    except Exception as e:
        logger.warning("history fetch failed: %s %s", ticker, e)
        return pd.DataFrame()


def _first_present(row: dict, keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


def _fetch_jquants_history(code: str, fetch_start: date, end: date) -> "pd.DataFrame":
    rows = get_daily_quotes(code=code, from_date=fetch_start, to_date=end)
    if not rows:
        return pd.DataFrame()
    out = []
    for r in rows:
        d = r.get("Date") or r.get("date")
        if not d:
            continue
        close = _first_present(r, ["AdjustmentClose", "AdjustedClose", "AdjC", "Close", "C"])
        open_ = _first_present(r, ["AdjustmentOpen", "AdjustedOpen", "AdjO", "Open", "O"])
        high = _first_present(r, ["AdjustmentHigh", "AdjustedHigh", "AdjH", "High", "H"])
        low = _first_present(r, ["AdjustmentLow", "AdjustedLow", "AdjL", "Low", "L"])
        volume = _first_present(r, ["AdjustmentVolume", "AdjustedVolume", "AdjVo", "Volume", "Vo"])
        if close is None or open_ is None or high is None or low is None:
            continue
        out.append({
            "Date": pd.to_datetime(d).date(),
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
            "TurnoverValue": _first_present(r, ["TurnoverValue", "TradingValue", "Va"]),
        })
    if not out:
        return pd.DataFrame()
    df = pd.DataFrame(out).drop_duplicates("Date").sort_values("Date").set_index("Date")
    for col in ["Open", "High", "Low", "Close", "Volume", "TurnoverValue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["Open", "High", "Low", "Close"])


def _fetch_price_history(code: str, fetch_start: date, end: date, source: str) -> tuple["pd.DataFrame", str]:
    source = (source or "auto").lower()
    if source in {"auto", "jquants"}:
        try:
            df = _fetch_jquants_history(code, fetch_start, end)
            if not df.empty:
                return df, "jquants"
            if source == "jquants":
                logger.warning("J-Quants price data empty: %s", code)
                return pd.DataFrame(), "jquants"
        except Exception as e:
            if source == "jquants":
                logger.warning("J-Quants price fetch failed: %s %s", code, e)
                return pd.DataFrame(), "jquants"
            logger.warning("J-Quants price fetch failed; yfinance fallback: %s %s", code, e)
    df = _fetch_yfinance_history(f"{code}.T", fetch_start, end)
    return df, "yfinance"


def _is_429_error(error: Exception) -> bool:
    msg = str(error).lower()
    return "429" in msg or "too many 429" in msg or "too many requests" in msg


def _sleep_after_code(args: argparse.Namespace) -> None:
    seconds = float(getattr(args, "sleep_seconds", 0) or 0)
    if seconds > 0:
        time.sleep(seconds)


def fetch_prices_with_retry(
    code: str,
    fetch_start: date,
    end: date,
    args: argparse.Namespace,
) -> tuple["pd.DataFrame", str, bool]:
    source = (getattr(args, "source", "auto") or "auto").lower()
    if source == "yfinance":
        return _fetch_yfinance_history(f"{code}.T", fetch_start, end), "yfinance", False

    max_retries = max(1, int(getattr(args, "max_retries", 5) or 5))
    retry_wait = float(getattr(args, "retry_wait_seconds", 60) or 0)
    cooldown = float(getattr(args, "cooldown_on_429", 300) or 0)
    had_429 = False

    for attempt in range(1, max_retries + 1):
        try:
            df = _fetch_jquants_history(code, fetch_start, end)
            if not df.empty:
                return df, "jquants", had_429
            logger.warning("J-Quants price data empty: %s", code)
            break
        except Exception as e:
            is_429 = _is_429_error(e)
            if is_429:
                had_429 = True
                logger.warning(
                    "[429] code=%s retry=%d/%d cooldown=%ss",
                    code,
                    attempt,
                    max_retries,
                    int(cooldown),
                )
                if getattr(args, "stop_on_429", False):
                    raise StopOn429(code, e) from e
                if attempt < max_retries and cooldown > 0:
                    time.sleep(cooldown)
                continue

            logger.warning(
                "[retry] code=%s retry=%d/%d wait=%ss error=%s",
                code,
                attempt,
                max_retries,
                int(retry_wait),
                e,
            )
            if attempt < max_retries and retry_wait > 0:
                time.sleep(retry_wait)

    if source == "auto":
        logger.warning("J-Quants failed or empty; yfinance fallback: %s", code)
        return _fetch_yfinance_history(f"{code}.T", fetch_start, end), "yfinance", had_429

    if had_429:
        logger.warning("[skip] code=%s no price data after 429 retries", code)
    else:
        logger.warning("[skip] code=%s no price data after retries", code)
    return pd.DataFrame(), "jquants", had_429


def _pct_change(series: "pd.Series", periods: int = 1) -> "pd.Series":
    return (series / series.shift(periods) - 1.0) * 100.0


def _rsi_series(close: "pd.Series", period: int = 14) -> "pd.Series":
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - 100 / (1 + rs)).replace([float("inf"), -float("inf")], None)


def _add_technical_features(df: "pd.DataFrame", cfg: dict) -> "pd.DataFrame":
    out = df.copy()
    close = out["Close"]
    high = out["High"]
    low = out["Low"]
    volume = out["Volume"]
    prev_close = close.shift(1)

    out["prev_close"] = prev_close
    out["day_change"] = close - prev_close
    out["day_change_pct"] = _pct_change(close, 1)
    out["drop_pct"] = out["day_change_pct"]

    out["drop_from_5d_high_pct"] = (close / high.rolling(5).max() - 1.0) * 100.0
    out["drop_from_20d_high_pct"] = (close / high.rolling(20).max() - 1.0) * 100.0
    out["drop_from_52w_high_pct"] = (close / high.rolling(252).max() - 1.0) * 100.0

    out["return_1d_pct"] = out["day_change_pct"]
    out["return_3d_pct"] = _pct_change(close, 3)
    out["return_5d_pct"] = _pct_change(close, 5)
    out["return_10d_pct"] = _pct_change(close, 10)

    for window, col in ((5, "ma5"), (25, "ma25"), (75, "ma75")):
        out[col] = close.rolling(window).mean()
        out[f"{col}_gap_pct"] = (close / out[col] - 1.0) * 100.0

    out["rsi14"] = _rsi_series(close, 14)
    out["rsi_min_5d"] = out["rsi14"].rolling(5).min()
    out["rsi_recover_flag"] = (
        (out["rsi_min_5d"] <= float(cfg.get("rsi_low_threshold", 25.0)))
        & (out["rsi14"] >= float(cfg.get("rsi_recover_threshold", 40.0)))
    )

    out["volume_avg_20d"] = volume.rolling(20).mean()
    out["volume_ratio_20d"] = volume / out["volume_avg_20d"]
    out["volume_spike_flag"] = out["volume_ratio_20d"] >= float(cfg.get("volume_ratio_threshold", 2.0))

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr14"] = tr.rolling(14).mean()
    out["volatility_20d"] = out["day_change_pct"].rolling(20).std()
    if "TurnoverValue" in out.columns and out["TurnoverValue"].notna().any():
        out["turnover_value"] = out["TurnoverValue"]
    else:
        out["turnover_value"] = close * volume
    return out


def _series_map(df: "pd.DataFrame", value_col: str) -> dict[str, float | None]:
    if df.empty or value_col not in df.columns:
        return {}
    return {str(idx): _clean_value(v) for idx, v in df[value_col].items()}


def _fetch_index_bundle(fetch_start: date, end: date) -> dict[str, dict[str, float | None]]:
    logger.info("fetch index data start")
    bundle: dict[str, dict[str, float | None]] = {
        "nikkei_change_pct": {},
        "topix_change_pct": {},
        "vix_value": {},
        "vix_change_pct": {},
    }

    specs = {
        "nikkei": "^N225",
        "topix": "1306.T",
        "vix": "^VIX",
    }
    for name, ticker in specs.items():
        df = _fetch_yfinance_history(ticker, fetch_start, end)
        if df.empty or "Close" not in df.columns:
            logger.warning("index fetch failed or empty: %s %s", name, ticker)
            continue
        df = df.copy()
        df["change_pct"] = _pct_change(df["Close"], 1)
        if name == "nikkei":
            bundle["nikkei_change_pct"] = _series_map(df, "change_pct")
            logger.info("index fetched: nikkei rows=%d", len(df))
        elif name == "topix":
            bundle["topix_change_pct"] = _series_map(df, "change_pct")
            logger.info("index fetched: topix-proxy rows=%d", len(df))
        elif name == "vix":
            bundle["vix_value"] = _series_map(df, "Close")
            bundle["vix_change_pct"] = _series_map(df, "change_pct")
            logger.info("index fetched: vix rows=%d", len(df))
    logger.info("fetch index data complete")
    return bundle


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _load_financials(sb) -> dict[str, dict]:
    try:
        rows = sb.table("nikkei_financials").select("*").execute().data or []
        result = {str(r.get("code")): r for r in rows if r.get("code")}
        logger.info("financial rows loaded: %d", len(result))
        return result
    except Exception as e:
        logger.warning("financial rows load failed: %s", e)
        return {}


def _financial_values(fin: dict, close: float | None) -> dict:
    div_yield = _first_present(fin, ["dividend_yield_pct", "dividend_yield", "div_yield_pct"])
    dividend_per_share = _first_present(fin, ["dividend_per_share", "dividend"])
    if div_yield is None and dividend_per_share is not None and close and close > 0:
        div_yield = float(dividend_per_share) / close * 100.0
    return {
        "per": _first_present(fin, ["per", "PER"]),
        "pbr": _first_present(fin, ["pbr", "PBR"]),
        "dividend_yield_pct": div_yield,
        "is_deficit": fin.get("is_deficit"),
        "roe": _first_present(fin, ["roe", "ROE"]),
        "operating_profit_growth_pct": _first_present(
            fin, ["operating_profit_growth_pct", "operating_profit_growth"]
        ),
        "net_income_growth_pct": _first_present(fin, ["net_income_growth_pct", "net_income_growth"]),
    }


def _tradeable(close: float | None, turnover_value: float | None) -> tuple[bool, str | None]:
    reasons: list[str] = []
    if close is None or close < MIN_TRADE_PRICE:
        reasons.append("株価100円未満")
    if turnover_value is None or turnover_value < MIN_TURNOVER_VALUE:
        reasons.append("売買代金1億円未満")
    return (len(reasons) == 0, " / ".join(reasons) if reasons else None)


def _build_rows_for_code(
    stock: dict,
    df: "pd.DataFrame",
    *,
    start: date,
    end: date,
    market: str,
    cfg: dict,
    index_bundle: dict[str, dict[str, float | None]],
    financials: dict[str, dict],
    only_drop_candidates: bool,
) -> list[dict]:
    code = stock["code"]
    name = stock.get("name", "")
    sector = stock.get("sector", "")
    feat = _add_technical_features(df, cfg)
    feat = feat[(pd.to_datetime(feat.index) >= pd.Timestamp(start)) & (pd.to_datetime(feat.index) <= pd.Timestamp(end))]

    rows: list[dict] = []
    drop_thr = float(cfg.get("drop_list_threshold", -3.5))
    fin = financials.get(code, {})

    for idx, r in feat.iterrows():
        trade_date = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)
        close = _clean_value(r.get("Close"))
        turnover = _clean_value(r.get("turnover_value"))
        day_change_pct = _clean_value(r.get("day_change_pct"))
        is_drop_candidate = bool(day_change_pct is not None and float(day_change_pct) <= drop_thr)
        if only_drop_candidates and not is_drop_candidate:
            continue
        is_tradeable, exclude_reason = _tradeable(close, turnover)

        nikkei_change = index_bundle["nikkei_change_pct"].get(trade_date)
        topix_change = index_bundle["topix_change_pct"].get(trade_date)
        benchmark_change = topix_change if topix_change is not None else nikkei_change
        index_gap = (
            float(day_change_pct) - float(benchmark_change)
            if day_change_pct is not None and benchmark_change is not None
            else None
        )

        fvals = _financial_values(fin, close)
        row = {
            "trade_date": trade_date,
            "code": code,
            "name": name,
            "market": market,
            "sector": sector,
            "open": r.get("Open"),
            "high": r.get("High"),
            "low": r.get("Low"),
            "close": close,
            "volume": r.get("Volume"),
            "turnover_value": turnover,
            "prev_close": r.get("prev_close"),
            "day_change": r.get("day_change"),
            "day_change_pct": day_change_pct,
            "drop_pct": r.get("drop_pct"),
            "drop_from_5d_high_pct": r.get("drop_from_5d_high_pct"),
            "drop_from_20d_high_pct": r.get("drop_from_20d_high_pct"),
            "drop_from_52w_high_pct": r.get("drop_from_52w_high_pct"),
            "return_1d_pct": r.get("return_1d_pct"),
            "return_3d_pct": r.get("return_3d_pct"),
            "return_5d_pct": r.get("return_5d_pct"),
            "return_10d_pct": r.get("return_10d_pct"),
            "ma5": r.get("ma5"),
            "ma25": r.get("ma25"),
            "ma75": r.get("ma75"),
            "ma5_gap_pct": r.get("ma5_gap_pct"),
            "ma25_gap_pct": r.get("ma25_gap_pct"),
            "ma75_gap_pct": r.get("ma75_gap_pct"),
            "rsi14": r.get("rsi14"),
            "rsi_min_5d": r.get("rsi_min_5d"),
            "rsi_recover_flag": bool(r.get("rsi_recover_flag")),
            "volume_avg_20d": r.get("volume_avg_20d"),
            "volume_ratio_20d": r.get("volume_ratio_20d"),
            "volume_spike_flag": bool(r.get("volume_spike_flag")),
            "atr14": r.get("atr14"),
            "volatility_20d": r.get("volatility_20d"),
            "nikkei_change_pct": nikkei_change,
            "topix_change_pct": topix_change,
            "sector_change_pct": None,
            "index_gap_pct": index_gap,
            "sector_gap_pct": None,
            "decliners_ratio": None,
            "advancers_ratio": None,
            "vix_value": index_bundle["vix_value"].get(trade_date),
            "vix_change_pct": index_bundle["vix_change_pct"].get(trade_date),
            "nikkei_vi_value": None,
            "nikkei_vi_change_pct": None,
            **fvals,
            "margin_buy_balance": None,
            "margin_sell_balance": None,
            "margin_ratio": None,
            "margin_buy_change_pct": None,
            "margin_sell_change_pct": None,
            "short_selling_ratio": None,
            "short_balance_ratio": None,
            "earnings_soon_flag": False,
            "earnings_within_5d_flag": False,
            "earnings_recent_flag": False,
            "tdnet_disclosure_flag": False,
            "market_shock_score": 0,
            "sector_risk_score": 0,
            "bad_news_score": 0,
            "fx_yen_score": 0,
            "energy_naphtha_score": 0,
            "interest_rate_score": 0,
            "is_drop_candidate": is_drop_candidate,
            "is_tradeable": is_tradeable,
            "exclude_reason": exclude_reason,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        rows.append({k: _clean_value(v) for k, v in row.items()})
    return rows


def _existing_dates(sb, code: str, start: date, end: date) -> set[str]:
    try:
        rows = (
            sb.table("stock_feature_snapshots")
            .select("trade_date")
            .eq("code", code)
            .gte("trade_date", start.isoformat())
            .lte("trade_date", end.isoformat())
            .execute()
            .data or []
        )
        return {str(r.get("trade_date")) for r in rows if r.get("trade_date")}
    except Exception as e:
        logger.warning("existing snapshot lookup failed code=%s: %s", code, e)
        return set()


def _upsert_rows(sb, rows: list[dict], batch_size: int) -> int:
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        sb.table("stock_feature_snapshots").upsert(batch, on_conflict="code,trade_date").execute()
        total += len(batch)
    return total


def _log_dryrun_sample(code: str, rows: list[dict]) -> None:
    logger.info("DRYRUN code=%s rows=%d", code, len(rows))
    if not rows:
        return
    sample = rows[-1]
    logger.info(
        "sample: %s close=%s day=%s rsi=%s vol_ratio=%s drop_candidate=%s",
        sample.get("trade_date"),
        _round(sample.get("close")),
        _round(sample.get("day_change_pct")),
        _round(sample.get("rsi14")),
        _round(sample.get("volume_ratio_20d")),
        sample.get("is_drop_candidate"),
    )


def _round(value: Any, digits: int = 2) -> Any:
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except Exception:
        return value


def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas and yfinance are required")

    if (
        args.date
        and str(args.date).lower() == "today"
        and not args.allow_non_trading_day
        and is_weekend(today_jst())
    ):
        logger.info("skip feature generation: non-trading-day weekend=%s", today_jst().isoformat())
        return

    sb = _build_supabase()
    cfg = get_settings(force_reload=True)
    start, end, fetch_start = _date_range(args)
    market = args.market or "prime"
    batch_size = int(args.batch_size or DEFAULT_BATCH_SIZE)

    logger.info(
        "start feature generation: market=%s source=%s start=%s end=%s fetch_start=%s dry_run=%s force=%s",
        market,
        args.source,
        start,
        end,
        fetch_start,
        args.dry_run,
        args.force,
    )

    stocks = _load_codes(sb, args)
    logger.info("target codes: %d", len(stocks))
    if not stocks:
        logger.info("no target codes; complete")
        return

    index_bundle = _fetch_index_bundle(fetch_start, end)
    financials = _load_financials(sb)

    inserted_or_updated = 0
    errors = 0
    drop_candidates_total = 0
    untradeable_total = 0
    processed_codes = 0
    skipped_existing_codes = 0
    skipped_no_price = 0
    skipped_429 = 0
    stopped_on_429 = False
    last_code: str | None = None
    last_completed_code: str | None = None

    for n, stock in enumerate(stocks, start=1):
        code = stock["code"]
        last_code = code
        logger.info("[%d/%d] processing code=%s name=%s", n, len(stocks), code, stock.get("name", ""))
        try:
            df, used_source, had_429 = fetch_prices_with_retry(code, fetch_start, end, args)
            if df.empty:
                if had_429:
                    skipped_429 += 1
                else:
                    skipped_no_price += 1
                logger.warning("[skip] code=%s no price data source=%s", code, used_source)
                continue
            rows = _build_rows_for_code(
                stock,
                df,
                start=start,
                end=end,
                market=market,
                cfg=cfg,
                index_bundle=index_bundle,
                financials=financials,
                only_drop_candidates=bool(args.only_drop_candidates),
            )
            if not args.force and not args.dry_run:
                existing = _existing_dates(sb, code, start, end)
                if existing:
                    before_existing_filter = len(rows)
                    rows = [r for r in rows if r["trade_date"] not in existing]
                    if before_existing_filter > 0 and not rows:
                        skipped_existing_codes += 1
                        logger.info("[skip-existing] code=%s existing rows found", code)

            drop_count = sum(1 for r in rows if r.get("is_drop_candidate"))
            untradeable_count = sum(1 for r in rows if not r.get("is_tradeable"))
            drop_candidates_total += drop_count
            untradeable_total += untradeable_count
            processed_codes += 1

            if args.dry_run:
                _log_dryrun_sample(code, rows)
                logger.info(
                    "[%d/%d] code=%s name=%s rows=%d saved=0 drop_candidates=%d source=%s dry_run=True",
                    n,
                    len(stocks),
                    code,
                    stock.get("name", ""),
                    len(rows),
                    drop_count,
                    used_source,
                )
                continue
            if not rows:
                logger.info("upsert stock_feature_snapshots: code=%s rows=0", code)
                continue

            count = _upsert_rows(sb, rows, batch_size)
            inserted_or_updated += count
            logger.info(
                "[%d/%d] code=%s name=%s rows=%d saved=%d drop_candidates=%d untradeable=%d source=%s",
                n,
                len(stocks),
                code,
                stock.get("name", ""),
                len(rows),
                count,
                drop_count,
                untradeable_count,
                used_source,
            )
        except StopOn429 as e:
            stopped_on_429 = True
            logger.error(
                "stopped_on_429=True code=%s last_completed_code=%s resume_same=\"--code-from %s\" resume_after_completed=\"--start-after-code %s\" error=%s",
                e.code,
                last_completed_code,
                e.code,
                last_completed_code or "",
                e.original,
            )
            break
        except Exception as e:
            errors += 1
            logger.exception("[error] code=%s message=%s last_code=%s", code, e, last_code)
            continue
        finally:
            if not stopped_on_429:
                last_completed_code = code
                if args.progress_every and n % int(args.progress_every) == 0:
                    logger.info(
                        "progress: %d/%d processed=%d saved=%d skipped_existing=%d skipped_no_price=%d skipped_429=%d errors=%d last_code=%s",
                        n,
                        len(stocks),
                        processed_codes,
                        inserted_or_updated,
                        skipped_existing_codes,
                        skipped_no_price,
                        skipped_429,
                        errors,
                        last_code,
                    )
                _sleep_after_code(args)

    logger.info(
        "complete: processed_codes=%d skipped_existing_codes=%d skipped_no_price=%d skipped_429=%d inserted_or_updated=%d drop_candidates=%d untradeable=%d errors=%d stopped_on_429=%s last_code=%s",
        processed_codes,
        skipped_existing_codes,
        skipped_no_price,
        skipped_429,
        inserted_or_updated,
        drop_candidates_total,
        untradeable_total,
        errors,
        stopped_on_429,
        last_code,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate stock feature snapshots")
    parser.add_argument("--years", type=int, default=1)
    parser.add_argument("--date")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--code")
    parser.add_argument("--market", default="prime")
    parser.add_argument("--source", choices=["auto", "jquants", "yfinance"], default="auto")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--only-drop-candidates", action="store_true")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-wait-seconds", type=float, default=60.0)
    parser.add_argument("--cooldown-on-429", type=float, default=300.0)
    parser.add_argument("--start-after-code")
    parser.add_argument("--code-from")
    parser.add_argument("--code-to")
    parser.add_argument("--stop-on-429", action="store_true")
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
