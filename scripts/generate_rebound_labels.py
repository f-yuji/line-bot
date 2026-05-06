#!/usr/bin/env python3
"""
Generate stock_rebound_labels from stock_feature_snapshots.

This creates labels only. It intentionally does not write label_success back
to stock_feature_snapshots.
"""
import argparse
import logging
import math
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

JST = timezone(timedelta(hours=9))
DEFAULT_BATCH_SIZE = 200


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


def _date_range(args: argparse.Namespace) -> tuple[date, date]:
    end = _parse_date(args.end) or datetime.now(JST).date()
    if args.start:
        start = _parse_date(args.start)
    else:
        start = end - timedelta(days=365 * int(args.years or 1))
    return start, end


def _is_alpha_code(code: str) -> bool:
    return bool(code) and code.isalpha()


def _is_non_japanese(row: dict) -> bool:
    code = str(row.get("code") or "").strip()
    market = str(row.get("market") or "").strip().lower()
    return _is_alpha_code(code) or market in {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}


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


def _to_float(value: Any) -> float | None:
    value = _clean_value(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_all(query_factory, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        builder = query_factory()
        res = builder.range(start, start + page_size - 1).execute()
        data = res.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size
    return rows


def _load_candidate_codes(sb, args: argparse.Namespace) -> list[str]:
    if args.code:
        return [str(args.code).replace(".T", "")]

    def query():
        return sb.table("prime_stocks_cache").select("code").order("code")

    try:
        rows = _fetch_all(query)
        codes = sorted({str(r.get("code") or "").replace(".T", "") for r in rows if r.get("code")})
        codes = [c for c in codes if c and not _is_alpha_code(c)]
        if codes:
            logger.info("candidate code list loaded from prime_stocks_cache: %d", len(codes))
            return codes
    except Exception as e:
        logger.warning("prime_stocks_cache code load failed: %s", e)

    logger.warning("candidate code list is empty; use --code for a narrow run or refresh prime_stocks_cache")
    return []


def _load_candidates(sb, args: argparse.Namespace, start: date, end: date) -> list[dict]:
    cols = (
        "id, trade_date, code, name, market, sector, close, is_tradeable, "
        "is_drop_candidate"
    )
    codes = _load_candidate_codes(sb, args)
    if not codes:
        return []

    filtered: list[dict] = []
    limit = int(args.limit) if args.limit else None
    for idx, code in enumerate(codes, start=1):
        if limit and len(filtered) >= limit:
            break

        def query_for_code(code=code):
            q = (
                sb.table("stock_feature_snapshots")
                .select(cols)
                .eq("code", code)
                .eq("is_drop_candidate", True)
                .gte("trade_date", start.isoformat())
                .lte("trade_date", end.isoformat())
                .order("trade_date")
            )
            if not args.include_untradeable:
                q = q.eq("is_tradeable", True)
            return q

        try:
            rows = _fetch_all(query_for_code)
        except Exception as e:
            logger.warning("candidate load failed code=%s: %s", code, e)
            continue

        for row in rows:
            close = _to_float(row.get("close"))
            if _is_non_japanese(row) or close is None or close <= 0 or not row.get("trade_date") or not row.get("code"):
                continue
            filtered.append(row)
            if limit and len(filtered) >= limit:
                break
        if idx % 100 == 0:
            logger.info("candidate load progress: codes=%d/%d candidates=%d", idx, len(codes), len(filtered))
    return filtered


def _existing_label_keys(sb, candidates: list[dict]) -> set[tuple[str, str]]:
    if not candidates:
        return set()
    keys: set[tuple[str, str]] = set()
    by_code: dict[str, list[str]] = {}
    for c in candidates:
        by_code.setdefault(str(c["code"]), []).append(str(c["trade_date"]))
    for code, dates in by_code.items():
        try:
            min_d, max_d = min(dates), max(dates)
            rows = (
                sb.table("stock_rebound_labels")
                .select("code, trade_date")
                .eq("code", code)
                .gte("trade_date", min_d)
                .lte("trade_date", max_d)
                .execute()
                .data or []
            )
            keys.update((str(r["code"]), str(r["trade_date"])) for r in rows)
        except Exception as e:
            logger.warning("existing labels lookup failed code=%s: %s", code, e)
    return keys


def _load_snapshot_rows(sb, code: str, min_date: str, max_date: str) -> list[dict]:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date, high, low, close")
        .eq("code", code)
        .gte("trade_date", min_date)
        .lte("trade_date", max_date)
        .order("trade_date")
        .execute()
        .data or []
    )
    return rows


def _fetch_yfinance_future(code: str, trade_date: str, holding_days: int) -> list[dict]:
    try:
        start = datetime.strptime(trade_date, "%Y-%m-%d").date() + timedelta(days=1)
        end = start + timedelta(days=holding_days * 3 + 10)
        hist = yf.Ticker(f"{code}.T").history(
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=False,
        )
        if hist is None or hist.empty:
            return []
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        rows = []
        for idx, r in hist.iterrows():
            d = pd.Timestamp(idx).tz_localize(None).date().isoformat()
            rows.append({"trade_date": d, "high": r.get("High"), "low": r.get("Low"), "close": r.get("Close")})
        return rows[:holding_days]
    except Exception as e:
        logger.warning("yfinance future fetch failed code=%s date=%s: %s", code, trade_date, e)
        return []


def _future_rows_for_candidate(sb, candidate: dict, holding_days: int) -> list[dict]:
    code = str(candidate["code"])
    t = str(candidate["trade_date"])
    start = (datetime.strptime(t, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
    max_date = (datetime.strptime(t, "%Y-%m-%d").date() + timedelta(days=holding_days * 3 + 10)).isoformat()
    rows = _load_snapshot_rows(sb, code, start, max_date)
    rows = [r for r in rows if str(r.get("trade_date")) > t and _to_float(r.get("close")) is not None]
    if len(rows) >= holding_days:
        return rows[:holding_days]
    yf_rows = _fetch_yfinance_future(code, t, holding_days)
    return yf_rows[:holding_days]


def build_label(candidate: dict, future_rows: list[dict], args: argparse.Namespace) -> dict | None:
    holding_days = int(args.holding_days)
    if len(future_rows) < holding_days:
        logger.info("skip label: %s %s future data insufficient", candidate.get("code"), candidate.get("trade_date"))
        return None

    entry_price = _to_float(candidate.get("close"))
    if entry_price is None or entry_price <= 0:
        return None

    take_profit_pct = float(args.take_profit)
    stop_loss_pct = float(args.stop_loss)
    take_profit_price = entry_price * (1 + take_profit_pct / 100.0)
    stop_loss_price = entry_price * (1 + stop_loss_pct / 100.0)

    highs = [_to_float(r.get("high")) for r in future_rows[:holding_days]]
    lows = [_to_float(r.get("low")) for r in future_rows[:holding_days]]
    closes = [_to_float(r.get("close")) for r in future_rows[:holding_days]]
    if any(v is None for v in highs + lows + closes):
        logger.info("skip label: %s %s future data incomplete", candidate.get("code"), candidate.get("trade_date"))
        return None

    take_profit_day = None
    stop_loss_day = None
    for i in range(holding_days):
        day = i + 1
        if take_profit_day is None and highs[i] >= take_profit_price:
            take_profit_day = day
        if stop_loss_day is None and closes[i] <= stop_loss_price:
            stop_loss_day = day

    hit_tp = take_profit_day is not None
    hit_sl = stop_loss_day is not None
    max_return = (max(highs) / entry_price - 1.0) * 100.0
    max_drawdown = (min(lows) / entry_price - 1.0) * 100.0

    if hit_tp and not hit_sl:
        success = True
        reason = f"success_take_profit_day_{take_profit_day}"
    elif hit_tp and hit_sl:
        if take_profit_day < stop_loss_day:
            success = True
            reason = f"success_take_profit_day_{take_profit_day}"
        elif take_profit_day == stop_loss_day:
            success = False
            reason = "failed_same_day_tp_sl"
        else:
            success = False
            reason = f"failed_stop_loss_day_{stop_loss_day}"
    else:
        success = False
        reason = f"failed_stop_loss_day_{stop_loss_day}" if hit_sl else "failed_timeout"

    row = {
        "feature_snapshot_id": candidate.get("id"),
        "trade_date": str(candidate.get("trade_date")),
        "code": str(candidate.get("code")),
        "name": candidate.get("name"),
        "market": candidate.get("market") or "prime",
        "sector": candidate.get("sector"),
        "entry_price": entry_price,
        "entry_basis": "close",
        "max_return_5d_pct": max_return,
        "max_drawdown_5d_pct": max_drawdown,
        "take_profit_pct": take_profit_pct,
        "stop_loss_pct": stop_loss_pct,
        "holding_days": holding_days,
        "take_profit_price": take_profit_price,
        "stop_loss_price": stop_loss_price,
        "hit_take_profit": hit_tp,
        "hit_stop_loss": hit_sl,
        "take_profit_day": take_profit_day,
        "stop_loss_day": stop_loss_day,
        "label_success": success,
        "label_reason": reason,
        "is_valid_label": True,
        "invalid_reason": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    for i in range(holding_days):
        day = i + 1
        row[f"future_high_{day}d"] = highs[i]
        row[f"future_low_{day}d"] = lows[i]
        row[f"future_close_{day}d"] = closes[i]
    return {k: _clean_value(v) for k, v in row.items()}


def _upsert_rows(sb, rows: list[dict], batch_size: int) -> int:
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        sb.table("stock_rebound_labels").upsert(batch, on_conflict="code,trade_date").execute()
        total += len(batch)
    return total


def _summary(rows: list[dict]) -> dict:
    total = len(rows)
    success = sum(1 for r in rows if r.get("label_success") is True)
    fail = total - success
    tp = sum(1 for r in rows if r.get("hit_take_profit"))
    sl = sum(1 for r in rows if r.get("hit_stop_loss"))
    timeout = sum(1 for r in rows if r.get("label_reason") == "failed_timeout")
    avg_ret = sum(float(r.get("max_return_5d_pct") or 0) for r in rows) / total if total else 0
    avg_dd = sum(float(r.get("max_drawdown_5d_pct") or 0) for r in rows) / total if total else 0
    return {
        "total": total,
        "success": success,
        "fail": fail,
        "success_rate": (success / total * 100.0) if total else 0.0,
        "avg_max_return": avg_ret,
        "avg_max_drawdown": avg_dd,
        "take_profit": tp,
        "stop_loss": sl,
        "timeout": timeout,
    }


def _empty_stats() -> dict:
    return {
        "total": 0,
        "success": 0,
        "fail": 0,
        "take_profit": 0,
        "stop_loss": 0,
        "timeout": 0,
        "sum_max_return": 0.0,
        "sum_max_drawdown": 0.0,
    }


def _add_label_stats(stats: dict, row: dict) -> None:
    stats["total"] += 1
    if row.get("label_success") is True:
        stats["success"] += 1
    else:
        stats["fail"] += 1
    if row.get("hit_take_profit"):
        stats["take_profit"] += 1
    if row.get("hit_stop_loss"):
        stats["stop_loss"] += 1
    if row.get("label_reason") == "failed_timeout":
        stats["timeout"] += 1
    stats["sum_max_return"] += float(row.get("max_return_5d_pct") or 0)
    stats["sum_max_drawdown"] += float(row.get("max_drawdown_5d_pct") or 0)


def _summary_from_stats(stats: dict) -> dict:
    total = int(stats["total"])
    return {
        "total": total,
        "success": int(stats["success"]),
        "fail": int(stats["fail"]),
        "success_rate": (float(stats["success"]) / total * 100.0) if total else 0.0,
        "avg_max_return": (float(stats["sum_max_return"]) / total) if total else 0.0,
        "avg_max_drawdown": (float(stats["sum_max_drawdown"]) / total) if total else 0.0,
        "take_profit": int(stats["take_profit"]),
        "stop_loss": int(stats["stop_loss"]),
        "timeout": int(stats["timeout"]),
    }


def run(args: argparse.Namespace) -> None:
    if not HAS_DEPS:
        raise RuntimeError("pandas and yfinance are required")
    sb = _build_supabase()
    start, end = _date_range(args)
    logger.info(
        "start label generation: start=%s end=%s code=%s dry_run=%s force=%s",
        start,
        end,
        args.code or "",
        args.dry_run,
        args.force,
    )

    candidates = _load_candidates(sb, args, start, end)
    logger.info("candidates=%d", len(candidates))
    if not candidates:
        return

    if not args.force and not args.dry_run:
        existing = _existing_label_keys(sb, candidates)
        if existing:
            before = len(candidates)
            candidates = [c for c in candidates if (str(c["code"]), str(c["trade_date"])) not in existing]
            logger.info("skip existing labels: %d", before - len(candidates))

    labels: list[dict] = []
    pending_labels: list[dict] = []
    stats = _empty_stats()
    skipped = 0
    errors = 0
    by_code: dict[str, int] = {}
    saved = 0
    flush_every = max(1, int(args.flush_every or args.batch_size or DEFAULT_BATCH_SIZE))

    for c in candidates:
        by_code[str(c["code"])] = by_code.get(str(c["code"]), 0) + 1
        try:
            future = _future_rows_for_candidate(sb, c, int(args.holding_days))
            label = build_label(c, future, args)
            if label is None:
                skipped += 1
                continue
            _add_label_stats(stats, label)
            if args.dry_run:
                labels.append(label)
            else:
                pending_labels.append(label)
            if args.dry_run:
                logger.info(
                    "DRYRUN label: %s %s success=%s reason=%s max_return=%.2f max_dd=%.2f",
                    label["code"],
                    label["trade_date"],
                    label["label_success"],
                    label["label_reason"],
                    float(label["max_return_5d_pct"]),
                    float(label["max_drawdown_5d_pct"]),
                )
            else:
                if len(pending_labels) >= flush_every:
                    saved += _upsert_rows(sb, pending_labels, int(args.batch_size or DEFAULT_BATCH_SIZE))
                    logger.info(
                        "flush stock_rebound_labels: saved_total=%d flushed=%d skipped=%d errors=%d last=%s %s",
                        saved,
                        len(pending_labels),
                        skipped,
                        errors,
                        label["code"],
                        label["trade_date"],
                    )
                    pending_labels.clear()
                if args.progress_every and int(stats["total"]) % int(args.progress_every) == 0:
                    logger.info(
                        "label progress: labels=%d saved=%d pending=%d skipped=%d errors=%d last=%s %s",
                        int(stats["total"]),
                        saved,
                        len(pending_labels),
                        skipped,
                        errors,
                        label["code"],
                        label["trade_date"],
                    )
        except Exception as e:
            errors += 1
            logger.exception("label failed code=%s date=%s: %s", c.get("code"), c.get("trade_date"), e)

    for code, count in sorted(by_code.items()):
        logger.info("processed code=%s candidates=%d", code, count)

    if args.dry_run:
        logger.info("DRYRUN rows=%d; no DB save", len(labels))
        s = _summary(labels)
    else:
        if pending_labels:
            flushed = _upsert_rows(sb, pending_labels, int(args.batch_size or DEFAULT_BATCH_SIZE))
            saved += flushed
            logger.info("flush stock_rebound_labels: saved_total=%d flushed=%d final=True", saved, flushed)
            pending_labels.clear()
        logger.info("upsert stock_rebound_labels: rows=%d", saved)
        s = _summary_from_stats(stats)

    logger.info(
        "summary: total=%d success=%d fail=%d success_rate=%.1f%% avg_max_return=%.2f "
        "avg_max_drawdown=%.2f take_profit=%d stop_loss=%d timeout=%d skipped=%d errors=%d saved=%d",
        s["total"],
        s["success"],
        s["fail"],
        s["success_rate"],
        s["avg_max_return"],
        s["avg_max_drawdown"],
        s["take_profit"],
        s["stop_loss"],
        s["timeout"],
        skipped,
        errors,
        saved,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate rebound success labels")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--code")
    parser.add_argument("--years", type=int, default=1)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--only-unlabeled", action="store_true", default=True)
    parser.add_argument("--include-untradeable", action="store_true")
    parser.add_argument("--take-profit", type=float, default=5.0)
    parser.add_argument("--stop-loss", type=float, default=-4.0)
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--flush-every", type=int, default=1000)
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
