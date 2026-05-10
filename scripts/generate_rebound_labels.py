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

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

JST = timezone(timedelta(hours=9))
DEFAULT_BATCH_SIZE = 200
LABEL_5D = {"holding_days": 5, "take_profit_pct": 5.0, "stop_loss_pct": -3.0}
LABEL_10D = {"holding_days": 10, "take_profit_pct": 7.0, "stop_loss_pct": -4.0}
MAX_FUTURE_DAYS = 20


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


def _with_retry(label: str, fn, *, retries: int = 4, base_sleep: float = 3.0):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            retryable = any(
                marker in msg
                for marker in (
                    "ConnectionTerminated",
                    "RemoteProtocolError",
                    "ReadTimeout",
                    "ConnectTimeout",
                    "Server disconnected",
                    "temporarily unavailable",
                    "429",
                    "500",
                    "502",
                    "503",
                    "504",
                )
            )
            if not retryable or attempt >= retries:
                raise
            wait = base_sleep * attempt
            logger.warning("%s retry attempt=%d/%d sleep=%.1fs error=%s", label, attempt, retries, wait, msg[:180])
            time.sleep(wait)


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
    rows = _with_retry(
        f"snapshot load code={code}",
        lambda: (
            sb.table("stock_feature_snapshots")
            .select("trade_date, high, low, close")
            .eq("code", code)
            .gte("trade_date", min_date)
            .lte("trade_date", max_date)
            .order("trade_date")
            .execute()
            .data or []
        ),
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


def evaluate_rebound(
    future_rows: list[dict],
    entry_price: float,
    holding_days: int,
    take_profit_pct: float,
    stop_loss_pct: float,
) -> dict:
    if len(future_rows) < holding_days:
        return {
            "success": None,
            "tp_hit": False,
            "sl_hit": False,
            "max_return": None,
            "max_drawdown": None,
            "days_to_tp": None,
            "days_to_sl": None,
            "reason": "invalid_future_data_insufficient",
        }
    take_profit_price = entry_price * (1 + take_profit_pct / 100.0)
    stop_loss_price = entry_price * (1 + stop_loss_pct / 100.0)

    highs = [_to_float(r.get("high")) for r in future_rows[:holding_days]]
    lows = [_to_float(r.get("low")) for r in future_rows[:holding_days]]
    closes = [_to_float(r.get("close")) for r in future_rows[:holding_days]]
    if any(v is None for v in highs + lows + closes):
        return {
            "success": None,
            "tp_hit": False,
            "sl_hit": False,
            "max_return": None,
            "max_drawdown": None,
            "days_to_tp": None,
            "days_to_sl": None,
            "reason": "invalid_future_data_incomplete",
        }

    take_profit_day = None
    stop_loss_day = None
    for i in range(holding_days):
        day = i + 1
        if take_profit_day is None and highs[i] >= take_profit_price:
            take_profit_day = day
        if stop_loss_day is None and lows[i] <= stop_loss_price:
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
        else:
            success = False
            reason = "failed_same_day_tp_sl" if take_profit_day == stop_loss_day else f"failed_stop_loss_day_{stop_loss_day}"
    else:
        success = False
        reason = f"failed_stop_loss_day_{stop_loss_day}" if hit_sl else "failed_timeout"

    return {
        "success": success,
        "tp_hit": hit_tp,
        "sl_hit": hit_sl,
        "max_return": max_return,
        "max_drawdown": max_drawdown,
        "days_to_tp": take_profit_day,
        "days_to_sl": stop_loss_day,
        "reason": reason,
    }


def build_label(candidate: dict, future_rows: list[dict], args: argparse.Namespace) -> dict | None:
    entry_price = _to_float(candidate.get("close"))
    if entry_price is None or entry_price <= 0:
        return None

    label_mode = str(args.label_mode or "both")
    needs_5d = label_mode in {"5d", "both"} or bool(args.force_5d) or bool(args.force)
    needs_10d = label_mode in {"10d", "both"} or bool(args.force_10d) or bool(args.force)

    eval_5d = evaluate_rebound(future_rows, entry_price, **LABEL_5D) if needs_5d else None
    eval_10d = evaluate_rebound(future_rows, entry_price, **LABEL_10D) if needs_10d else None
    if needs_5d and (eval_5d or {}).get("success") is None and not needs_10d:
        logger.info("skip label: %s %s future data incomplete", candidate.get("code"), candidate.get("trade_date"))
        return None
    if needs_10d and (eval_10d or {}).get("success") is None and not needs_5d:
        logger.info("skip label: %s %s future data incomplete", candidate.get("code"), candidate.get("trade_date"))
        return None
    if needs_5d and needs_10d and (eval_5d or {}).get("success") is None and (eval_10d or {}).get("success") is None:
        logger.info("skip label: %s %s future data insufficient", candidate.get("code"), candidate.get("trade_date"))
        return None

    compat = eval_5d if eval_5d and eval_5d.get("success") is not None else None
    future_days = max(
        5,
        min(MAX_FUTURE_DAYS, int(getattr(args, "future_days", MAX_FUTURE_DAYS) or MAX_FUTURE_DAYS)),
    )
    highs = [_to_float(r.get("high")) for r in future_rows[:future_days]]
    lows = [_to_float(r.get("low")) for r in future_rows[:future_days]]
    closes = [_to_float(r.get("close")) for r in future_rows[:future_days]]
    take_profit_price = entry_price * 1.05
    stop_loss_price = entry_price * 0.97

    row = {
        "feature_snapshot_id": candidate.get("id"),
        "trade_date": str(candidate.get("trade_date")),
        "code": str(candidate.get("code")),
        "name": candidate.get("name"),
        "market": candidate.get("market") or "prime",
        "sector": candidate.get("sector"),
        "entry_price": entry_price,
        "entry_basis": "close",
        "is_valid_label": True,
        "invalid_reason": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    if compat is not None:
        row.update({
            "max_return_5d_pct": compat.get("max_return"),
            "max_drawdown_5d_pct": compat.get("max_drawdown"),
            "take_profit_pct": 5.0,
            "stop_loss_pct": -3.0,
            "holding_days": 5,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "hit_take_profit": compat.get("tp_hit"),
            "hit_stop_loss": compat.get("sl_hit"),
            "take_profit_day": compat.get("days_to_tp"),
            "stop_loss_day": compat.get("days_to_sl"),
            "label_success": compat.get("success"),
            "label_reason": compat.get("reason"),
        })

    if eval_5d is not None:
        row.update({
            "label_5d_success": eval_5d.get("success"),
            "label_5d_tp_hit": eval_5d.get("tp_hit"),
            "label_5d_sl_hit": eval_5d.get("sl_hit"),
            "label_5d_max_return": eval_5d.get("max_return"),
            "label_5d_max_drawdown": eval_5d.get("max_drawdown"),
            "label_5d_days_to_tp": eval_5d.get("days_to_tp"),
            "label_5d_days_to_sl": eval_5d.get("days_to_sl"),
            "label_5d_take_profit_pct": LABEL_5D["take_profit_pct"],
            "label_5d_stop_loss_pct": LABEL_5D["stop_loss_pct"],
        })
    if eval_10d is not None:
        row.update({
            "label_10d_success": eval_10d.get("success"),
            "label_10d_tp_hit": eval_10d.get("tp_hit"),
            "label_10d_sl_hit": eval_10d.get("sl_hit"),
            "label_10d_max_return": eval_10d.get("max_return"),
            "label_10d_max_drawdown": eval_10d.get("max_drawdown"),
            "label_10d_days_to_tp": eval_10d.get("days_to_tp"),
            "label_10d_days_to_sl": eval_10d.get("days_to_sl"),
            "label_10d_take_profit_pct": LABEL_10D["take_profit_pct"],
            "label_10d_stop_loss_pct": LABEL_10D["stop_loss_pct"],
        })

    for i in range(min(future_days, len(highs), len(lows), len(closes))):
        day = i + 1
        row[f"future_high_{day}d"] = highs[i]
        row[f"future_low_{day}d"] = lows[i]
        row[f"future_close_{day}d"] = closes[i]
    return {k: _clean_value(v) for k, v in row.items()}


def _upsert_rows(sb, rows: list[dict], batch_size: int) -> int:
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        _with_retry(
            "stock_rebound_labels upsert",
            lambda batch=batch: sb.table("stock_rebound_labels").upsert(batch, on_conflict="code,trade_date").execute(),
        )
        total += len(batch)
    return total


def _summary(rows: list[dict]) -> dict:
    total = len(rows)
    success = sum(1 for r in rows if (r.get("label_success") if r.get("label_success") is not None else r.get("label_10d_success")) is True)
    fail = total - success
    tp = sum(1 for r in rows if (r.get("hit_take_profit") if r.get("hit_take_profit") is not None else r.get("label_10d_tp_hit")))
    sl = sum(1 for r in rows if (r.get("hit_stop_loss") if r.get("hit_stop_loss") is not None else r.get("label_10d_sl_hit")))
    timeout = sum(1 for r in rows if r.get("label_reason") == "failed_timeout")
    avg_ret = sum(float(r.get("max_return_5d_pct") or r.get("label_10d_max_return") or 0) for r in rows) / total if total else 0
    avg_dd = sum(float(r.get("max_drawdown_5d_pct") or r.get("label_10d_max_drawdown") or 0) for r in rows) / total if total else 0
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
    success_value = row.get("label_success")
    if success_value is None:
        success_value = row.get("label_10d_success")
    if success_value is True:
        stats["success"] += 1
    else:
        stats["fail"] += 1
    tp_value = row.get("hit_take_profit")
    sl_value = row.get("hit_stop_loss")
    if tp_value is None:
        tp_value = row.get("label_10d_tp_hit")
    if sl_value is None:
        sl_value = row.get("label_10d_sl_hit")
    if tp_value:
        stats["take_profit"] += 1
    if sl_value:
        stats["stop_loss"] += 1
    if row.get("label_reason") == "failed_timeout":
        stats["timeout"] += 1
    stats["sum_max_return"] += float(row.get("max_return_5d_pct") or row.get("label_10d_max_return") or 0)
    stats["sum_max_drawdown"] += float(row.get("max_drawdown_5d_pct") or row.get("label_10d_max_drawdown") or 0)


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

    if not args.force and not args.force_5d and not args.force_10d and not args.dry_run:
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
    required_label_days = 10 if str(args.label_mode or "both") in {"10d", "both"} or args.force_10d or args.force else 5
    max_holding_days = max(required_label_days, min(MAX_FUTURE_DAYS, int(args.future_days or MAX_FUTURE_DAYS)))

    for c in candidates:
        by_code[str(c["code"])] = by_code.get(str(c["code"]), 0) + 1
        try:
            future = _future_rows_for_candidate(sb, c, max_holding_days)
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
    parser.add_argument("--label-mode", choices=["5d", "10d", "both"], default="both")
    parser.add_argument("--future-days", type=int, default=MAX_FUTURE_DAYS)
    parser.add_argument("--force-5d", action="store_true")
    parser.add_argument("--force-10d", action="store_true")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--flush-every", type=int, default=1000)
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())
