#!/usr/bin/env python3
"""Predict rebound probabilities and persist AI-assisted watchlist signals."""
import argparse
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

try:
    import joblib
    import numpy as np
    import pandas as pd

    HAS_BASE_DEPS = True
except ImportError:
    HAS_BASE_DEPS = False

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from supabase import create_client

from settings_loader import get_settings
from services.market_regime import evaluate_market_regime
from services.entry_credit_filter import attach_entry_margin_data, evaluate_entry_credit_filter
from services.entry_mode import classify_entry_case, entry_mode_filter, resolve_entry_mode
from services.model_storage import download_model_artifact
from services.signal_stage import SIGNAL_STAGES, evaluate_signal_stage
from services.signal_history import record_rebound_signal
from services.trading_calendar import latest_feature_matches_today, should_skip_today_cron

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
LINE_API_BASE = "https://api.line.me"
FALLBACK_FEATURES = [
    "day_change_pct", "drop_from_20d_high_pct", "rsi14", "rsi_min_5d",
    "volume_ratio_20d", "index_gap_pct", "bad_news_score",
]
TARGET_CONFIG = {
    "5d": {"model_name": "rebound_lgbm_5d", "legacy_model_name": "rebound_lgbm", "take_profit_pct": 5.0, "stop_loss_pct": -3.0, "holding_days": 5},
    "10d": {"model_name": "rebound_lgbm_10d", "legacy_model_name": None, "take_profit_pct": 7.0, "stop_loss_pct": -4.0, "holding_days": 10},
}
VIRTUAL_REENTRY_COOLDOWN_DAYS = 10
ENTRY_SIGNAL_STAGES = {"confirmed", "strong_confirmed"}
ACTIVE_SIGNAL_STAGES = {"confirmed", "strong_confirmed"}


def _target_config(args: argparse.Namespace) -> dict:
    return TARGET_CONFIG.get(str(getattr(args, "target_label", "5d") or "5d"), TARGET_CONFIG["5d"])


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


def _current_long_term_market_regime(sb) -> dict:
    fallback = {"regime": "neutral", "label": "中立", "score": None}
    try:
        rows = (
            sb.table("long_term_market_regime")
            .select("trade_date,regime,label,score")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        return {**fallback, **(rows[0] if rows else {})}
    except Exception as e:
        logger.warning("long_term_market_regime lookup failed: %s", e)
        return fallback


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fetch_all(builder, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        res = builder.range(start, start + page_size - 1).execute()
        data = res.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size
    return rows


def _latest_snapshot_date(sb) -> str | None:
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return str(rows[0]["trade_date"]) if rows else None


def _target_date(sb, args: argparse.Namespace) -> str:
    if args.date:
        return args.date
    latest = _latest_snapshot_date(sb)
    if not latest:
        raise RuntimeError("stock_feature_snapshots has no rows")
    return latest


def _parse_trade_date_jst(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        return dt.astimezone(JST).date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def _snapshot_trade_dates(sb, snapshot_ids: list[Any]) -> dict[Any, str]:
    out: dict[Any, str] = {}
    clean_ids = [sid for sid in snapshot_ids if sid]
    for i in range(0, len(clean_ids), 100):
        rows = (
            sb.table("stock_feature_snapshots")
            .select("id,trade_date")
            .in_("id", clean_ids[i:i + 100])
            .execute()
            .data or []
        )
        for row in rows:
            out[row.get("id")] = str(row.get("trade_date"))
    return out


def _close_stale_watchlist_rows(sb, target_date: str, *, dry_run: bool) -> None:
    """Close stale current-state rows before writing today's predictions.

    The current state table is intentionally not history. For daily prediction
    runs, only rows tied to the target feature snapshot date should remain in
    watching/rebound_signal. Signal history stays in rebound_signal_history.
    """

    rows = (
        sb.table("stock_drop_watchlist")
        .select("id,code,status,feature_snapshot_id,drop_detected_at")
        .in_("status", ["watching", "rebound_signal", "rebound_candidate", "signal_skipped"])
        .execute()
        .data or []
    )
    if not rows:
        return

    snapshot_dates = _snapshot_trade_dates(sb, [r.get("feature_snapshot_id") for r in rows])
    stale_rows: list[dict] = []
    stale_by_status: dict[str, int] = {}
    for row in rows:
        snapshot_id = row.get("feature_snapshot_id")
        trade_date = snapshot_dates.get(snapshot_id) if snapshot_id else None
        if trade_date is None:
            trade_date = _parse_trade_date_jst(row.get("drop_detected_at"))
        if trade_date and trade_date >= target_date:
            continue
        stale_rows.append(row)
        status = str(row.get("status") or "unknown")
        stale_by_status[status] = stale_by_status.get(status, 0) + 1

    stale_ids = [r.get("id") for r in stale_rows if r.get("id")]
    if not stale_ids:
        logger.info("stale watchlist cleanup: none target_date=%s active_rows=%d", target_date, len(rows))
        return

    logger.info(
        "%sstale watchlist cleanup: close=%d target_date=%s by_status=%s",
        "DRYRUN " if dry_run else "",
        len(stale_ids),
        target_date,
        stale_by_status,
    )
    if dry_run:
        return

    now = datetime.now(timezone.utc).isoformat()
    for i in range(0, len(stale_ids), 100):
        sb.table("stock_drop_watchlist").update({
            "status": "expired",
            "closed_at": now,
            "close_reason": "stale_signal",
            "signal_status_reason": "stale_signal_cleanup",
            "updated_at": now,
        }).in_("id", stale_ids[i:i + 100]).execute()
    for row in stale_rows:
        logger.info(
            "[signal_lifecycle] code=%s watchlist_id=%s status %s -> expired reason=stale_signal_cleanup",
            row.get("code"),
            row.get("id"),
            row.get("status"),
        )


def _load_active_model_row(sb, model_name: str = "rebound_lgbm") -> dict | None:
    try:
        rows = (
            sb.table("ml_models")
            .select("*")
            .eq("model_name", model_name)
            .eq("is_active", True)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        return rows[0] if rows else None
    except Exception as e:
        logger.warning("active model lookup failed: %s", e)
        return None


def _load_model_bundle(sb, args: argparse.Namespace) -> tuple[dict | None, dict | None]:
    target = _target_config(args)
    names = [args.model_name] if args.model_name else [target["model_name"]]
    if target.get("legacy_model_name"):
        names.append(target["legacy_model_name"])
    row = None
    for name in names:
        if not name:
            continue
        row = _load_active_model_row(sb, name)
        if row:
            break
    if not row:
        logger.warning("active model not found; fallback rule will be used")
        return None, None
    model_path = ROOT / str(row.get("model_path") or "")
    if not model_path.exists():
        storage_path = str(row.get("storage_path") or row.get("model_path") or "")
        if storage_path:
            logger.warning("active model missing locally; downloading from storage path=%s", storage_path)
            download_model_artifact(sb, storage_path, model_path)
    try:
        bundle = joblib.load(model_path)
        logger.info("active model loaded: version=%s path=%s", row.get("model_version"), model_path)
        return row, bundle
    except Exception as e:
        logger.warning("active model load failed: %s; fallback rule will be used", e)
        return row, None


def _load_snapshots(sb, target_date: str, args: argparse.Namespace) -> list[dict]:
    q = (
        sb.table("stock_feature_snapshots")
        .select("*")
        .eq("trade_date", target_date)
        .eq("is_drop_candidate", True)
        .eq("is_tradeable", True)
        .order("day_change_pct")
    )
    if args.code:
        q = q.eq("code", str(args.code).replace(".T", ""))
    if args.limit:
        q = q.limit(int(args.limit))
    rows = _fetch_all(q)
    out = []
    for r in rows:
        code = str(r.get("code") or "")
        market = str(r.get("market") or "").lower()
        if code.isalpha() or market in {"dow", "dow30", "us", "usa", "nyse", "nasdaq", "djia"}:
            logger.info("skip non-japanese ticker in predict_rebound: %s market=%s", code, market)
            continue
        out.append(r)
    return out


def _prepare_model_frame(rows: list[dict], bundle: dict) -> "pd.DataFrame":
    df = pd.DataFrame(rows)
    numeric_cols = list(bundle.get("numeric_columns") or [])
    categorical_cols = list(bundle.get("categorical_columns") or [])
    fill_values = dict(bundle.get("fill_values") or {})
    feature_columns = list(bundle.get("feature_columns") or [])

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    x_num = df[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(fill_values) if numeric_cols else pd.DataFrame(index=df.index)

    for col in categorical_cols:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = pd.get_dummies(df[categorical_cols], prefix=categorical_cols, dummy_na=False) if categorical_cols else pd.DataFrame(index=df.index)
    x = pd.concat([x_num, x_cat], axis=1)
    return x.reindex(columns=feature_columns, fill_value=0)


def _fallback_probability(row: dict) -> float:
    p = 0.35
    rsi = _to_float(row.get("rsi14"))
    rsi_min = _to_float(row.get("rsi_min_5d"))
    vol = _to_float(row.get("volume_ratio_20d"), 0) or 0
    day = _to_float(row.get("day_change_pct"), 0) or 0
    drop20 = _to_float(row.get("drop_from_20d_high_pct"), 0) or 0
    gap = _to_float(row.get("index_gap_pct"), 0) or 0
    bad = _to_float(row.get("bad_news_score"), 0) or 0
    if rsi is not None and 25 <= rsi <= 45:
        p += 0.08
    if rsi_min is not None and rsi_min <= 30:
        p += 0.05
    if vol >= 1.3:
        p += 0.07
    if day <= -3.5:
        p += 0.05
    if drop20 <= -8:
        p += 0.04
    if gap <= -2:
        p += 0.03
    if bad >= 80:
        p -= 0.25
    elif bad >= 40:
        p -= 0.10
    return max(0.01, min(0.89, p))


def _cap_fallback_probability(probability: float, cfg: dict) -> float:
    # Fallback is only a rough rule score. Keep it below the confirmed threshold
    # so model outages cannot create confirmed/strong virtual entries.
    confirmed = _to_float(cfg.get("ai_probability_confirmed"), 0.50) or 0.50
    return min(probability, max(0.01, confirmed - 0.001))


def _expected_value(probability: float, take_profit_pct: float = 5.0, stop_loss_pct: float = -3.0) -> float:
    return probability * take_profit_pct - (1.0 - probability) * abs(stop_loss_pct)


def _days_since(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc).date() - dt.date()).days
    except Exception:
        return None


def _recent_closed_trade(sb, code: str, cooldown_days: int = VIRTUAL_REENTRY_COOLDOWN_DAYS) -> dict | None:
    try:
        rows = (
            sb.table("virtual_trades")
            .select("id,code,status,sell_date,sell_reason,exit_reason,exit_checked_at,updated_at")
            .eq("code", code)
            .eq("status", "closed")
            .order("updated_at", desc=True)
            .limit(5)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.warning("recent closed trade lookup failed code=%s: %s", code, e)
        return None
    for row in rows:
        days = _days_since(row.get("sell_date") or row.get("exit_checked_at") or row.get("updated_at"))
        if days is not None and days <= cooldown_days:
            row["days_since_exit"] = days
            return row
    return None


def _same_signal_trade_exists(sb, snapshot: dict, watch: dict) -> str | None:
    watchlist_id = watch.get("id") or snapshot.get("watchlist_id")
    feature_snapshot_id = snapshot.get("id") or snapshot.get("feature_snapshot_id")
    try:
        if watchlist_id:
            rows = (
                sb.table("virtual_trades")
                .select("id,status")
                .eq("watchlist_id", watchlist_id)
                .limit(1)
                .execute()
                .data or []
            )
            if rows:
                logger.info(
                    "virtual_trade skipped by same watchlist signal: code=%s watchlist_id=%s status=%s",
                    snapshot.get("code"),
                    watchlist_id,
                    rows[0].get("status"),
                )
                return "duplicate_signal"
        if feature_snapshot_id:
            rows = (
                sb.table("virtual_trades")
                .select("id,status")
                .eq("feature_snapshot_id", feature_snapshot_id)
                .limit(1)
                .execute()
                .data or []
            )
            if rows:
                logger.info(
                    "virtual_trade skipped by same feature snapshot: code=%s feature_snapshot_id=%s status=%s",
                    snapshot.get("code"),
                    feature_snapshot_id,
                    rows[0].get("status"),
                )
                return "duplicate_signal"
    except Exception as e:
        logger.warning("same signal trade lookup failed code=%s: %s", snapshot.get("code"), e)
    return None


def _mark_watchlist_status(sb, watch: dict, snapshot: dict, status: str, reason: str, *, dry_run: bool, trade_id: Any = None) -> None:
    watchlist_id = (watch or {}).get("id")
    if not watchlist_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    update = {
        "status": status,
        "signal_status_reason": reason,
        "updated_at": now,
    }
    if status == "entered":
        update["entered_at"] = now
        update["virtual_trade_id"] = str(trade_id) if trade_id is not None else None
    if status in {"closed", "expired", "ai_dropped", "excluded"}:
        update["closed_at"] = now
        update["close_reason"] = reason
    if dry_run:
        logger.info("DRYRUN watchlist status update: id=%s %s", watchlist_id, update)
        return
    sb.table("stock_drop_watchlist").update(update).eq("id", watchlist_id).execute()
    _signal_lifecycle_log(snapshot.get("code"), watchlist_id, watch.get("status"), status, reason, trade_id=trade_id)


def _insert_virtual_trade_with_optional_columns(sb, row: dict) -> list[dict]:
    remaining = dict(row)
    for _ in range(10):
        try:
            return sb.table("virtual_trades").insert(remaining).execute().data or []
        except Exception as e:
            msg = str(e)
            marker = "Could not find the '"
            missing = None
            if marker in msg:
                missing = msg.split(marker, 1)[1].split("'", 1)[0]
            if missing and missing in remaining:
                logger.warning("virtual_trades column missing; skip optional field for insert: %s", missing)
                remaining.pop(missing, None)
                continue
            raise
    return sb.table("virtual_trades").insert(remaining).execute().data or []


def _missing_column_from_error(error: Exception) -> str | None:
    msg = str(error)
    marker = "Could not find the '"
    if marker in msg:
        return msg.split(marker, 1)[1].split("'", 1)[0]
    return None


def _update_watchlist_with_optional_columns(sb, watchlist_id: Any, update: dict) -> list[dict]:
    remaining = dict(update)
    for _ in range(12):
        try:
            return sb.table("stock_drop_watchlist").update(remaining).eq("id", watchlist_id).execute().data or []
        except Exception as e:
            missing = _missing_column_from_error(e)
            if missing and missing in remaining:
                logger.warning("stock_drop_watchlist column missing; skip optional field for update: %s", missing)
                remaining.pop(missing, None)
                continue
            raise
    return sb.table("stock_drop_watchlist").update(remaining).eq("id", watchlist_id).execute().data or []


def _insert_watchlist_with_optional_columns(sb, update: dict) -> list[dict]:
    remaining = dict(update)
    for _ in range(12):
        try:
            return sb.table("stock_drop_watchlist").insert(remaining).execute().data or []
        except Exception as e:
            missing = _missing_column_from_error(e)
            if missing and missing in remaining:
                logger.warning("stock_drop_watchlist column missing; skip optional field for insert: %s", missing)
                remaining.pop(missing, None)
                continue
            raise
    return sb.table("stock_drop_watchlist").insert(remaining).execute().data or []


def _current_mode(sb, target_date: str) -> dict:
    try:
        rows = (
            sb.table("market_regime")
            .select("trade_date,mode,shock_score,reason,nikkei_change_pct,topix_change_pct,decliners_ratio")
            .lte("trade_date", target_date)
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            return rows[0]
    except Exception as e:
        logger.warning("market_regime lookup failed: %s", e)
    return {"trade_date": target_date, "mode": "normal", "shock_score": 0, "reason": "fallback normal"}


def _determine_stage(row: dict, probability: float, expected_value: float, cfg: dict, rule_score: float) -> tuple[str, bool, str | None]:
    bad = _to_float(row.get("bad_news_score"), 0) or 0
    if bad >= 80:
        return "none", True, f"AI除外: bad_news_score={bad:.0f}"
    result = evaluate_signal_stage(probability, rule_score, expected_value, cfg, row.get("market_regime_adjustment"))
    return result["stage"], False, None


def _score_like(row: dict, probability: float) -> float:
    tech = 100.0 * probability
    penalty = min(25.0, (_to_float(row.get("bad_news_score"), 0) or 0) * 0.2)
    return max(0.0, min(100.0, tech - penalty))


def _find_watchlist(sb, code: str) -> dict | None:
    rows = (
        sb.table("stock_drop_watchlist")
        .select("id,code,status,signal_stage,signal_count")
        .eq("code", code)
        .in_("status", ["watching", "rebound_candidate", "rebound_signal", "notified"])
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return rows[0] if rows else None


def _find_watchlist_by_snapshot(sb, code: str, snapshot_id: Any) -> dict | None:
    if not snapshot_id:
        return None
    rows = (
        sb.table("stock_drop_watchlist")
        .select("id,code,status,signal_stage,signal_count,feature_snapshot_id,virtual_trade_id")
        .eq("code", code)
        .eq("feature_snapshot_id", snapshot_id)
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return rows[0] if rows else None


def _status_for_stage(stage: str, is_excluded: bool) -> str:
    if is_excluded:
        return "excluded"
    if stage == "early":
        return "rebound_candidate"
    if stage in ACTIVE_SIGNAL_STAGES:
        return "rebound_signal"
    return "ai_dropped"


def _signal_lifecycle_log(code: Any, watchlist_id: Any, old_status: Any, new_status: str, reason: str, **extra: Any) -> None:
    suffix = " ".join(f"{k}={v}" for k, v in extra.items() if v is not None)
    logger.info(
        "[signal_lifecycle] code=%s watchlist_id=%s status %s -> %s reason=%s%s%s",
        code,
        watchlist_id,
        old_status,
        new_status,
        reason,
        " " if suffix else "",
        suffix,
    )


def _virtual_entry_check_log(snapshot: dict, result: dict, decision: str, reason: str) -> None:
    """Log the existing AI-entry decision without changing its behavior."""
    logger.info(
        "[virtual_entry_check] code=%s decision=%s reason=%s stage=%s ai_pct=%.1f "
        "signal_close=%s buy_price_basis=signal_date_close day_change_pct=%s rsi14=%s "
        "volume_ratio_20d=%s entry_mode=%s entry_case=%s ma5_gap_pct=%s margin_ratio=%s",
        snapshot.get("code"),
        decision,
        reason,
        result.get("signal_stage"),
        float(result.get("probability") or 0.0) * 100.0,
        snapshot.get("close"),
        snapshot.get("day_change_pct"),
        snapshot.get("rsi14"),
        snapshot.get("volume_ratio_20d"),
        result.get("entry_mode_used"),
        result.get("entry_case"),
        result.get("entry_ma5_gap_pct"),
        snapshot.get("margin_ratio"),
    )


def _persist_watchlist(sb, row: dict, result: dict, *, dry_run: bool, force: bool) -> dict | None:
    now = datetime.now(timezone.utc).isoformat()
    exact_existing = _find_watchlist_by_snapshot(sb, str(row["code"]), row.get("id"))
    terminal_statuses = {"entered", "signal_skipped", "closed", "expired", "ai_dropped", "excluded"}
    if exact_existing and exact_existing.get("status") in terminal_statuses and not force:
        result["skip_entry_candidate"] = True
        logger.info(
            "watchlist exact snapshot already terminal: code=%s snapshot_id=%s status=%s",
            row.get("code"),
            row.get("id"),
            exact_existing.get("status"),
        )
        return exact_existing
    existing = exact_existing or _find_watchlist(sb, str(row["code"]))
    prev_count = int((existing or {}).get("signal_count") or 0)
    prev_stage = (existing or {}).get("signal_stage")
    prev_status = (existing or {}).get("status")
    stage = result["signal_stage"]
    status = _status_for_stage(stage, bool(result["is_excluded"]))
    signal_count = prev_count + 1 if stage in SIGNAL_STAGES and (existing or {}).get("signal_stage") == stage else (1 if stage in SIGNAL_STAGES else prev_count)
    update = {
        "code": str(row["code"]),
        "name": row.get("name"),
        "market": row.get("market") or "prime",
        "sector": row.get("sector"),
        "status": status,
        "drop_pct": row.get("drop_pct") or row.get("day_change_pct"),
        "price_at_drop": row.get("close"),
        "drop_detected_at": f"{row.get('trade_date')}T00:00:00+09:00",
        "signal_stage": stage,
        "signal_score": result["signal_score"],
        "signal_probability": result["probability"],
        "expected_value": result["expected_value"],
        "mode": result["mode"],
        "bad_news_score": row.get("bad_news_score") or 0,
        "market_shock_score": row.get("market_shock_score") or 0,
        "sector_risk_score": row.get("sector_risk_score") or 0,
        "fx_yen_score": row.get("fx_yen_score") or 0,
        "energy_naphtha_score": row.get("energy_naphtha_score") or 0,
        "interest_rate_score": row.get("interest_rate_score") or 0,
        "feature_snapshot_id": row.get("id"),
        "last_signal_at": now if stage in SIGNAL_STAGES else None,
        "signal_count": signal_count,
        "is_excluded": result["is_excluded"],
        "exclude_reason": result["exclude_reason"],
        "entered_at": None,
        "closed_at": now if status == "ai_dropped" else None,
        "close_reason": "ai_score_below_threshold" if status == "ai_dropped" else None,
        "virtual_trade_id": None,
        "signal_expires_at": None,
        "signal_status_reason": (
            "early_candidate"
            if status == "rebound_candidate"
            else "confirmed_signal"
            if status == "rebound_signal"
            else "ai_score_below_threshold"
            if status == "ai_dropped"
            else result["exclude_reason"]
        ),
        "market_regime": result.get("market_regime"),
        "market_regime_label": result.get("market_regime_label"),
        "market_threshold_adjust": result.get("market_threshold_adjust", 0),
        "market_regime_reason": result.get("market_regime_reason"),
        "market_nikkei_pct": result.get("market_nikkei_pct"),
        "market_topix_pct": result.get("market_topix_pct"),
        "market_nikkei_change_yen": result.get("market_nikkei_change_yen"),
        "entry_mode_used": result.get("entry_mode_used"),
        "entry_mode_reason": result.get("entry_mode_reason"),
        "recommended_entry_mode": result.get("recommended_entry_mode"),
        "entry_ma5_gap_pct": result.get("entry_ma5_gap_pct"),
        "entry_ma25_gap_pct": result.get("entry_ma25_gap_pct"),
        "entry_ma75_gap_pct": result.get("entry_ma75_gap_pct"),
        "entry_case": result.get("entry_case"),
        "updated_at": now,
    }
    if result["is_excluded"]:
        update["status"] = "excluded"
        update["excluded_at"] = now
        update["closed_at"] = now
        update["close_reason"] = "excluded"
        update["signal_status_reason"] = result["exclude_reason"] or "excluded"
    if dry_run:
        logger.info("DRYRUN watchlist %s: %s", "update" if existing else "insert", update)
        saved = {**(existing or {}), **update}
        record_rebound_signal(sb, source="predict_rebound", snapshot=row, watchlist=saved, result=result, dry_run=True)
        return saved
    if existing and not force:
        _update_watchlist_with_optional_columns(sb, existing["id"], update)
        saved = {**existing, **update}
        if prev_stage and prev_stage != stage:
            logger.info("[signal_stage_transition] code=%s stage %s -> %s", row.get("code"), prev_stage, stage)
        if prev_status != status:
            _signal_lifecycle_log(row.get("code"), existing["id"], prev_status, status, update["signal_status_reason"])
        record_rebound_signal(sb, source="predict_rebound", snapshot=row, watchlist=saved, result=result, dry_run=dry_run)
        return saved
    if existing and force:
        _update_watchlist_with_optional_columns(sb, existing["id"], update)
        saved = {**existing, **update}
        if prev_stage and prev_stage != stage:
            logger.info("[signal_stage_transition] code=%s stage %s -> %s", row.get("code"), prev_stage, stage)
        if prev_status != status:
            _signal_lifecycle_log(row.get("code"), existing["id"], prev_status, status, update["signal_status_reason"])
        record_rebound_signal(sb, source="predict_rebound", snapshot=row, watchlist=saved, result=result, dry_run=dry_run)
        return saved
    inserted = _insert_watchlist_with_optional_columns(sb, update)
    saved = inserted[0] if inserted else update
    _signal_lifecycle_log(row.get("code"), saved.get("id"), None, status, update["signal_status_reason"])
    record_rebound_signal(sb, source="predict_rebound", snapshot=row, watchlist=saved, result=result, dry_run=dry_run)
    return saved


def _int_setting(cfg: dict, key: str, default: int) -> int:
    try:
        return max(0, int(cfg.get(key, default)))
    except Exception:
        return default


def _entry_limit_state(sb) -> tuple[int, int, dict[str, int]]:
    open_count = 0
    today_entries = 0
    sector_counts: dict[str, int] = {}
    try:
        rows = (
            sb.table("virtual_trades")
            .select("id,sector,status,sell_date")
            .eq("status", "open")
            .is_("sell_date", "null")
            .execute()
            .data or []
        )
        open_count = len(rows)
        for row in rows:
            sector = str(row.get("sector") or "unknown")
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
    except Exception as e:
        logger.warning("entry limit lookup failed: %s", e)
    try:
        start = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        rows = (
            sb.table("virtual_trades")
            .select("id")
            .gte("created_at", start.astimezone(timezone.utc).isoformat())
            .lt("created_at", end.astimezone(timezone.utc).isoformat())
            .execute()
            .data or []
        )
        today_entries = len(rows)
    except Exception as e:
        logger.warning("daily entry lookup failed: %s", e)
    return open_count, today_entries, sector_counts


def _create_virtual_trade(sb, snapshot: dict, watch: dict, result: dict, *, dry_run: bool) -> bool:
    if result["signal_stage"] not in ENTRY_SIGNAL_STAGES:
        return False
    if result.get("market_regime") == "panic_selloff":
        logger.info("virtual_trade skipped by market regime: code=%s regime=panic_selloff", snapshot.get("code"))
        _virtual_entry_check_log(snapshot, result, "skip", "panic_selloff")
        _mark_watchlist_status(sb, watch, snapshot, "signal_skipped", "panic_selloff", dry_run=dry_run)
        return False
    try:
        existing = (
            sb.table("virtual_trades")
            .select("id")
            .eq("code", snapshot["code"])
            .eq("status", "open")
            .is_("sell_date", "null")
            .limit(1)
            .execute()
            .data or []
        )
        if existing:
            _virtual_entry_check_log(snapshot, result, "skip", "already_open_virtual_trade")
            _mark_watchlist_status(sb, watch, snapshot, "signal_skipped", "already_open_virtual_trade", dry_run=dry_run)
            return False
        duplicate_reason = _same_signal_trade_exists(sb, snapshot, watch)
        if duplicate_reason:
            _virtual_entry_check_log(snapshot, result, "skip", str(duplicate_reason))
            _mark_watchlist_status(sb, watch, snapshot, "signal_skipped", duplicate_reason, dry_run=dry_run)
            return False
        recent = _recent_closed_trade(sb, str(snapshot["code"]))
        if recent:
            logger.info(
                "virtual_trade skipped by reentry cooldown: code=%s reason=%s days=%s",
                snapshot.get("code"),
                recent.get("exit_reason") or recent.get("sell_reason"),
                recent.get("days_since_exit"),
            )
            _virtual_entry_check_log(snapshot, result, "skip", "reentry_cooldown")
            _mark_watchlist_status(sb, watch, snapshot, "signal_skipped", "reentry_cooldown", dry_run=dry_run)
            return False
        now = datetime.now(timezone.utc).isoformat()
        reason = (
            f"AI probability={result['probability']:.2f} expected_value={result['expected_value']:.2f} "
            f"stage={result['signal_stage']} mode={result['mode']} horizon={result.get('prediction_horizon', '5d')}"
        )
        row = {
            "watchlist_id": watch.get("id"),
            "code": snapshot["code"],
            "name": snapshot.get("name"),
            "sector": snapshot.get("sector"),
            "buy_price": snapshot.get("close"),
            "buy_date": f"{snapshot.get('trade_date')}T00:00:00+09:00",
            "buy_score": result["signal_score"],
            "signal_stage": result["signal_stage"],
            "entry_reason": reason,
            "entry_score": result["signal_score"],
            "entry_probability": result["probability"],
            "expected_value": result["expected_value"],
            "mode": result["mode"],
            "bad_news_score": snapshot.get("bad_news_score") or 0,
            "sector_risk_score": snapshot.get("sector_risk_score") or 0,
            "market_shock_score": snapshot.get("market_shock_score") or 0,
            "feature_snapshot_id": snapshot.get("id"),
            "market_regime": result.get("market_regime"),
            "market_regime_label": result.get("market_regime_label"),
            "entry_size_multiplier": result.get("entry_size_multiplier", 1.0),
            "market_nikkei_pct": result.get("market_nikkei_pct"),
            "market_topix_pct": result.get("market_topix_pct"),
            "market_nikkei_change_yen": result.get("market_nikkei_change_yen"),
            "entry_mode_used": result.get("entry_mode_used"),
            "entry_mode_reason": result.get("entry_mode_reason"),
            "recommended_entry_mode": result.get("recommended_entry_mode"),
            "entry_ma5_gap_pct": result.get("entry_ma5_gap_pct"),
            "entry_ma25_gap_pct": result.get("entry_ma25_gap_pct"),
            "entry_ma75_gap_pct": result.get("entry_ma75_gap_pct"),
            "entry_case": result.get("entry_case"),
            "status": "open",
        }
        _virtual_entry_check_log(snapshot, result, "enter", "confirmed_signal_passed_filters")
        if dry_run:
            logger.info("DRYRUN virtual_trade insert: %s", row)
            return True
        inserted = _insert_virtual_trade_with_optional_columns(sb, row)
        trade = inserted[0] if inserted else {}
        _mark_watchlist_status(
            sb,
            watch,
            snapshot,
            "entered",
            "virtual_trade_created",
            dry_run=False,
            trade_id=trade.get("id"),
        )
        return True
    except Exception as e:
        logger.error("virtual_trade create failed code=%s: %s", snapshot.get("code"), e)
        return False


def _entry_rank_value(candidate: tuple[dict, dict, dict]) -> tuple[float, float]:
    _row, _watch, result = candidate
    stage_rank = 1 if result.get("signal_stage") == "strong_confirmed" else 0
    return stage_rank, float(result.get("expected_value") or -999.0), float(result.get("probability") or 0.0)


def _create_ranked_virtual_trades(
    sb,
    candidates: list[tuple[dict, dict, dict]],
    cfg: dict,
    market_adjustment: dict,
    long_term_market: dict | None = None,
    *,
    dry_run: bool,
) -> None:
    if not candidates:
        return
    entry_mode_ctx = resolve_entry_mode(cfg, market_adjustment, long_term_market)
    logger.info(
        "[entry_mode] configured=%s recommended=%s effective=%s short_regime=%s long_regime=%s basis=%s",
        entry_mode_ctx["configured"],
        entry_mode_ctx["recommended"],
        entry_mode_ctx["effective"],
        entry_mode_ctx["regime"],
        entry_mode_ctx.get("long_term_regime"),
        entry_mode_ctx.get("recommendation_basis"),
    )
    effective_entry_mode = str(entry_mode_ctx["effective"])
    max_open = _int_setting(cfg, "max_open_positions", 20)
    max_daily = _int_setting(cfg, "max_daily_entries", 5)
    max_sector = _int_setting(cfg, "max_sector_positions", 2)
    rank_limit = _int_setting(cfg, "entry_rank_limit", 10)
    if market_adjustment.get("regime") == "panic_rebound" and rank_limit > 0:
        original = rank_limit
        rank_limit = max(1, rank_limit // 2)
        logger.info("[market_regime_limit] regime=panic_rebound entry_rank_limit=%d original=%d", rank_limit, original)
    candidate_rows = [row for row, _watch, _result in candidates]
    attach_entry_margin_data(sb, candidate_rows)
    filtered_candidates: list[tuple[dict, dict, dict]] = []
    for row, watch, result in candidates:
        passed_mode, mode_reason, mode_meta = entry_mode_filter(row, effective_entry_mode)
        result.update(mode_meta)
        result["entry_mode_used"] = effective_entry_mode
        result["entry_mode_reason"] = mode_reason or "entry_mode_passed"
        result["recommended_entry_mode"] = entry_mode_ctx["recommended"]
        if not passed_mode:
            logger.info(
                "[entry_mode_filter] skip code=%s mode=%s reason=%s ma5_gap=%s case=%s drop=%s",
                row.get("code"),
                effective_entry_mode,
                mode_reason,
                mode_meta.get("entry_ma5_gap_pct"),
                mode_meta.get("entry_case"),
                row.get("drop_pct"),
            )
            _virtual_entry_check_log(row, result, "skip", str(mode_reason or "entry_mode_filter"))
            _mark_watchlist_status(sb, watch, row, "signal_skipped", str(mode_reason or "entry_mode_filter"), dry_run=dry_run)
            continue
        credit = evaluate_entry_credit_filter(sb, row, cfg)
        if not credit.passed:
            logger.info(
                "[entry_margin_filter] skip code=%s reason=%s margin_ratio=%s margin_date=%s limit=%s",
                row.get("code"),
                credit.reason,
                credit.margin_ratio,
                credit.margin_date,
                cfg.get("entry_max_margin_ratio"),
            )
            _virtual_entry_check_log(row, result, "skip", str(credit.reason or "margin_ratio_filter"))
            _mark_watchlist_status(sb, watch, row, "signal_skipped", str(credit.reason or "margin_ratio_filter"), dry_run=dry_run)
            continue
        if credit.margin_ratio is not None:
            row["margin_ratio"] = credit.margin_ratio
            row["margin_date"] = credit.margin_date
        filtered_candidates.append((row, watch, result))
    if not filtered_candidates:
        return

    ranked_all = sorted(filtered_candidates, key=_entry_rank_value, reverse=True)
    ranked = ranked_all[:rank_limit] if rank_limit > 0 else ranked_all
    ranked_ids = {(w or {}).get("id") for _r, w, _res in ranked if (w or {}).get("id")}
    for row, watch, result in ranked_all:
        watch_id = (watch or {}).get("id")
        if watch_id and watch_id not in ranked_ids:
            _virtual_entry_check_log(row, result, "skip", "entry_rank_limit")
            _mark_watchlist_status(sb, watch, row, "signal_skipped", "entry_rank_limit", dry_run=dry_run)
    open_count, today_entries, sector_counts = _entry_limit_state(sb)
    for row, watch, result in ranked:
        code = row.get("code")
        sector = str(row.get("sector") or "unknown")
        if max_open and open_count >= max_open:
            logger.info("[position_limit] skip code=%s open_positions=%d limit=%d", code, open_count, max_open)
            _virtual_entry_check_log(row, result, "skip", "max_open_positions")
            _mark_watchlist_status(sb, watch, row, "signal_skipped", "max_open_positions", dry_run=dry_run)
            continue
        if max_daily and today_entries >= max_daily:
            logger.info("[daily_entry_limit] skip code=%s today_entries=%d limit=%d", code, today_entries, max_daily)
            _virtual_entry_check_log(row, result, "skip", "max_daily_entries")
            _mark_watchlist_status(sb, watch, row, "signal_skipped", "max_daily_entries", dry_run=dry_run)
            continue
        current_sector = sector_counts.get(sector, 0)
        if max_sector and current_sector >= max_sector:
            logger.info("[sector_limit] skip code=%s sector=%s current=%d limit=%d", code, sector, current_sector, max_sector)
            _virtual_entry_check_log(row, result, "skip", "max_sector_positions")
            _mark_watchlist_status(sb, watch, row, "signal_skipped", "max_sector_positions", dry_run=dry_run)
            continue
        if _create_virtual_trade(sb, row, watch, result, dry_run=dry_run):
            open_count += 1
            today_entries += 1
            sector_counts[sector] = current_sector + 1


def _eligible_users(sb) -> list[dict]:
    try:
        rows = sb.table("users").select("user_id,active,membership_status").eq("active", True).execute().data or []
        return [r for r in rows if r.get("user_id") and r.get("membership_status") in {None, "active"}]
    except Exception as e:
        logger.warning("eligible users lookup failed: %s", e)
        return []


def _push(user_id: str, text: str) -> bool:
    token = _opt("LINE_CHANNEL_ACCESS_TOKEN")
    if not HAS_REQUESTS or not token:
        logger.warning("LINE push skipped: requests or token unavailable")
        return False
    try:
        r = requests.post(
            f"{LINE_API_BASE}/v2/bot/message/push",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
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


def _message(row: dict, result: dict) -> str:
    title = {
        "early": "【初動リバ候補】",
        "confirmed": "【本命リバ候補】",
        "strong_confirmed": "【強本命】",
    }.get(result["signal_stage"], "【リバ候補】")
    return "\n".join([
        title,
        f"{row.get('code')} {row.get('name') or ''}".strip(),
        "",
        f"AIスコア：{result['probability'] * 100:.0f}",
        f"期待値：{result['expected_value']:+.1f}%",
        f"ステージ：{result['signal_stage']}",
        f"期間：{result.get('prediction_horizon', '5d')}",
        f"相場モード：{result['mode']}",
        f"急落率：{_to_float(row.get('day_change_pct'), 0):+.1f}%",
        f"RSI：{_to_float(row.get('rsi14'), 0):.0f}",
        f"出来高：{_to_float(row.get('volume_ratio_20d'), 0):.1f}倍",
        "",
        "材料：",
        f"悪材料スコア {_to_float(row.get('bad_news_score'), 0):.0f}",
        f"市場ショック {_to_float(row.get('market_shock_score'), 0):.0f}",
        f"ナフサ {_to_float(row.get('energy_naphtha_score'), 0):.0f}",
        f"金利 {_to_float(row.get('interest_rate_score'), 0):.0f}",
        "",
        "想定：",
        f"利確 +{result.get('take_profit_pct', 5.0):.0f}%",
        f"損切 {result.get('stop_loss_pct', -3.0):.0f}%",
        f"期限 {result.get('holding_days', 5)}営業日",
    ])


def _notification_allowed(row: dict, result: dict, cfg: dict) -> bool:
    if not cfg.get("ai_notify_enabled", True):
        return False
    stage = result["signal_stage"]
    if stage == "early" and not cfg.get("ai_notify_early_enabled", False):
        logger.info("early notify skipped by settings: %s", row.get("code"))
        return False
    return stage in {"confirmed", "strong_confirmed", "early"} and not result.get("is_excluded")


def _candidate_summary(row: dict, result: dict) -> str:
    stage_label = {
        "early": "初動",
        "confirmed": "本命",
        "strong_confirmed": "強本命",
    }.get(result["signal_stage"], result["signal_stage"])
    return "\n".join([
        f"{stage_label} {row.get('code')} {row.get('name') or ''}".strip(),
        f"AIスコア {result['probability'] * 100:.0f} / 期待値 {result['expected_value']:+.1f}%",
        f"急落 {_to_float(row.get('day_change_pct'), 0):+.1f}% / RSI {_to_float(row.get('rsi14'), 0):.0f} / 出来高 {_to_float(row.get('volume_ratio_20d'), 0):.1f}倍",
        f"悪材料 {_to_float(row.get('bad_news_score'), 0):.0f} / 市場ショック {_to_float(row.get('market_shock_score'), 0):.0f}",
    ])


def _build_batch_messages(items: list[tuple[dict, dict]], target_date: str, mode: str) -> list[str]:
    if not items:
        return []
    strong = sum(1 for _, r in items if r["signal_stage"] == "strong_confirmed")
    confirmed = sum(1 for _, r in items if r["signal_stage"] == "confirmed")
    early = sum(1 for _, r in items if r["signal_stage"] == "early")
    header = "\n".join([
        "【AIリバ候補】",
        f"対象日：{target_date}",
        f"期間：{items[0][1].get('prediction_horizon', '5d')}",
        f"相場モード：{mode}",
        f"通知候補：{len(items)}件（強本命 {strong} / 本命 {confirmed} / 初動 {early}）",
        "",
        "想定："
        f"利確 +{items[0][1].get('take_profit_pct', 5.0):.0f}% / "
        f"損切 {items[0][1].get('stop_loss_pct', -3.0):.0f}% / "
        f"期限 {items[0][1].get('holding_days', 5)}営業日",
        "",
    ])
    footer = ""
    chunks: list[str] = []
    current = header
    for row, result in items:
        block = _candidate_summary(row, result)
        addition = ("\n---\n" if current != header else "") + block
        if len(current) + len(addition) + len(footer) > 4300:
            chunks.append(current)
            current = header + block
        else:
            current += addition
    chunks.append(current)
    return chunks


def _notify_batch(sb, items: list[tuple[dict, dict]], target_date: str, mode: str) -> None:
    messages = _build_batch_messages(items, target_date, mode)
    if not messages:
        logger.info("LINE batch notify skipped: no candidates")
        return
    users = _eligible_users(sb)
    sent = 0
    for user in users:
        for msg in messages:
            if _push(user["user_id"], msg):
                sent += 1
    logger.info(
        "LINE batch notify attempted: candidates=%d chunks=%d users=%d sent=%d",
        len(items), len(messages), len(users), sent,
    )


def run(args: argparse.Namespace) -> None:
    if not HAS_BASE_DEPS:
        raise RuntimeError("pandas, numpy and joblib are required")
    sb = _build_supabase()
    cfg = get_settings(force_reload=True)
    if not cfg.get("ai_predict_enabled", True):
        logger.info("ai_predict_enabled=False; exit")
        return
    logger.info(
        "[entry_settings_check] engine=predict_rebound_ai_snapshot ai_early=%s ai_confirmed=%s "
        "ai_strong=%s entry_mode=%s margin_enabled=%s max_margin_ratio=%s "
        "max_open_positions=%s max_daily_entries=%s entry_rank_limit=%s max_sector_positions=%s "
        "monitor_rebound_only_daily_rebound=%s monitor_rebound_only_rsi_low=%s "
        "monitor_rebound_only_rsi_recover=%s",
        cfg.get("ai_probability_early"),
        cfg.get("ai_probability_confirmed"),
        cfg.get("ai_probability_strong"),
        cfg.get("entry_mode"),
        cfg.get("entry_margin_filter_enabled"),
        cfg.get("entry_max_margin_ratio"),
        cfg.get("max_open_positions"),
        cfg.get("max_daily_entries"),
        cfg.get("entry_rank_limit"),
        cfg.get("max_sector_positions"),
        cfg.get("daily_rebound_threshold"),
        cfg.get("rsi_low_threshold"),
        cfg.get("rsi_recover_threshold"),
    )

    target_date = _target_date(sb, args)
    if not args.date and not args.allow_non_trading_day:
        skip, reason = should_skip_today_cron()
        if skip:
            logger.info("skip prediction: %s target_date=%s", reason, target_date)
            return
        matches_today, latest, today = latest_feature_matches_today(sb)
        if not matches_today:
            logger.info(
                "skip prediction: latest_feature_date_is_not_today latest=%s today=%s target_date=%s",
                latest,
                today,
                target_date,
            )
            return

    from services.market_regime_updater import update_market_regime_for_latest_trade_date
    update_market_regime_for_latest_trade_date(sb)
    if not args.date and not args.code:
        _close_stale_watchlist_rows(sb, target_date, dry_run=args.dry_run)
    regime = _current_mode(sb, target_date)
    mode = str(regime.get("mode") or "normal")
    logger.info("[market_data_for_regime] %s", regime)
    market_adjustment = evaluate_market_regime(regime)
    logger.info(
        "[market_regime] %s: AI threshold +%.2f, entry size %.1f reason=%s",
        market_adjustment["regime"],
        market_adjustment["ai_threshold_adjust"],
        market_adjustment["entry_size_multiplier"],
        market_adjustment["reason"],
    )
    long_term_market = _current_long_term_market_regime(sb)
    entry_mode_ctx = resolve_entry_mode(cfg, market_adjustment, long_term_market)
    effective_entry_mode = str(entry_mode_ctx["effective"])
    logger.info(
        "[entry_mode] configured=%s recommended=%s effective=%s short_regime=%s long_regime=%s basis=%s",
        entry_mode_ctx["configured"],
        entry_mode_ctx["recommended"],
        effective_entry_mode,
        entry_mode_ctx["regime"],
        entry_mode_ctx.get("long_term_regime"),
        entry_mode_ctx.get("recommendation_basis"),
    )
    target = _target_config(args)
    model_row, bundle = _load_model_bundle(sb, args)
    if args.fallback_rule:
        logger.warning("--fallback-rule specified; active model predictions are disabled")
        bundle = None
    using_model = bundle is not None
    if not using_model:
        logger.warning("using fallback rule probabilities")

    snapshots = _load_snapshots(sb, target_date, args)
    logger.info("prediction target: date=%s rows=%d mode=%s model=%s", target_date, len(snapshots), mode, bool(using_model))
    if not snapshots:
        return

    probabilities: list[float]
    if using_model:
        x = _prepare_model_frame(snapshots, bundle)
        probabilities = [float(v) for v in bundle["model"].predict_proba(x)[:, 1]]
    else:
        probabilities = [_cap_fallback_probability(_fallback_probability(r), cfg) for r in snapshots]
        logger.warning("fallback probabilities are capped below confirmed threshold; virtual entries disabled by stage")

    signal_count = 0
    notify_items: list[tuple[dict, dict]] = []
    entry_candidates: list[tuple[dict, dict, dict]] = []
    for row, prob in zip(snapshots, probabilities):
        row["market_regime_adjustment"] = market_adjustment
        ev = _expected_value(prob, target["take_profit_pct"], target["stop_loss_pct"])
        signal_score = round(_score_like(row, prob), 2)
        stage, is_excluded, exclude_reason = _determine_stage(row, prob, ev, cfg, signal_score)
        stage_check = evaluate_signal_stage(prob, signal_score, ev, cfg, market_adjustment)
        result = {
            "probability": round(prob, 6),
            "expected_value": round(ev, 4),
            "signal_stage": stage,
            "signal_score": signal_score,
            "mode": mode,
            "is_excluded": is_excluded,
            "exclude_reason": exclude_reason,
            "model_version": (model_row or {}).get("model_version") if model_row else "fallback_rule",
            "prediction_horizon": args.target_label,
            "take_profit_pct": target["take_profit_pct"],
            "stop_loss_pct": target["stop_loss_pct"],
            "holding_days": target["holding_days"],
            "market_regime": market_adjustment["regime"],
            "market_regime_label": market_adjustment["label"],
            "market_threshold_adjust": market_adjustment["ai_threshold_adjust"],
            "market_regime_reason": market_adjustment["reason"],
            "entry_size_multiplier": market_adjustment["entry_size_multiplier"],
            "market_nikkei_pct": market_adjustment.get("nikkei_pct_used"),
            "market_topix_pct": market_adjustment.get("topix_pct_used"),
            "market_nikkei_change_yen": market_adjustment.get("nikkei_change_yen_used"),
        }
        _passed_mode, mode_reason, mode_meta = entry_mode_filter(row, effective_entry_mode)
        result.update(mode_meta)
        result["entry_mode_used"] = effective_entry_mode
        result["entry_mode_reason"] = mode_reason or "entry_mode_candidate"
        result["recommended_entry_mode"] = entry_mode_ctx["recommended"]
        if stage in SIGNAL_STAGES:
            signal_count += 1
            thresholds = stage_check.get("thresholds") or {}
            logger.info(
                "[signal_check] code=%s engine=predict_rebound_ai_snapshot stage=%s ai_pct=%.1f "
                "confirmed_pct=%.1f strong_pct=%.1f rule_score=%.1f rule_strong_min=60 "
                "day_change_pct=%s drop20_pct=%s close=%s rsi14=%s rsi_min_5d=%s "
                "rsi_recover_flag=%s volume_ratio_20d=%s entry_basis=signal_date_close "
                "rebound_rule_gate=not_applied_in_predict_rebound reason=%s",
                row.get("code"),
                stage,
                prob * 100.0,
                float(thresholds.get("confirmed") or 0.0) * 100.0,
                float(thresholds.get("strong") or 0.0) * 100.0,
                signal_score,
                row.get("day_change_pct"),
                row.get("drop_from_20d_high_pct"),
                row.get("close"),
                row.get("rsi14"),
                row.get("rsi_min_5d"),
                row.get("rsi_recover_flag"),
                row.get("volume_ratio_20d"),
                stage_check.get("reason"),
            )
        logger.info(
            "%spredict: %s %s prob=%.3f ev=%.2f stage=%s bad=%.0f mode=%s",
            "DRYRUN " if args.dry_run else "",
            row.get("code"), row.get("name") or "", prob, ev, stage,
            _to_float(row.get("bad_news_score"), 0) or 0, mode,
        )
        watch = _persist_watchlist(sb, row, result, dry_run=args.dry_run, force=args.force)
        logger.info(
            "[market_regime_save] code=%s regime=%s adjust=%s nikkei=%s topix=%s",
            row.get("code"),
            result.get("market_regime"),
            result.get("market_threshold_adjust"),
            result.get("market_nikkei_pct"),
            result.get("market_topix_pct"),
        )
        if result["signal_stage"] in ENTRY_SIGNAL_STAGES and not result.get("skip_entry_candidate"):
            entry_candidates.append((row, watch or {}, result))
        if args.notify and not args.dry_run and _notification_allowed(row, result, cfg):
            notify_items.append((row, result))
    _create_ranked_virtual_trades(sb, entry_candidates, cfg, market_adjustment, long_term_market, dry_run=args.dry_run)
    if args.notify and not args.dry_run:
        _notify_batch(sb, notify_items, target_date, mode)
    logger.info("complete: predictions=%d signals=%d dry_run=%s", len(snapshots), signal_count, args.dry_run)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict rebound probabilities")
    p.add_argument("--date")
    p.add_argument("--latest", action="store_true")
    p.add_argument("--code")
    p.add_argument("--notify", action="store_true")
    p.add_argument("--target-label", choices=["5d", "10d"], default="5d")
    p.add_argument("--model-name")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--limit", type=int)
    p.add_argument("--fallback-rule", action="store_true")
    p.add_argument("--allow-non-trading-day", action="store_true", help="Allow processing latest data on weekends/holidays.")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
