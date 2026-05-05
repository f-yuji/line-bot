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

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
LINE_API_BASE = "https://api.line.me"
SIGNAL_STAGES = {"early", "confirmed", "strong_confirmed"}
MODE_THRESHOLDS = {
    "normal": {"early": 0.55, "confirmed": 0.65, "strong": 0.72},
    "shock": {"early": 0.57, "confirmed": 0.67, "strong": 0.75},
    "panic": {"early": 0.60, "confirmed": 0.70, "strong": 0.78},
    "recovery": {"early": 0.53, "confirmed": 0.63, "strong": 0.70},
}
FALLBACK_FEATURES = [
    "day_change_pct", "drop_from_20d_high_pct", "rsi14", "rsi_min_5d",
    "volume_ratio_20d", "index_gap_pct", "bad_news_score",
]


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


def _load_model_bundle(sb) -> tuple[dict | None, dict | None]:
    row = _load_active_model_row(sb)
    if not row:
        logger.warning("active model not found; fallback rule will be used")
        return None, None
    model_path = ROOT / str(row.get("model_path") or "")
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


def _expected_value(probability: float, take_profit_pct: float = 5.0, stop_loss_pct: float = -4.0) -> float:
    return probability * take_profit_pct - (1.0 - probability) * abs(stop_loss_pct)


def _current_mode(sb, target_date: str) -> dict:
    try:
        rows = (
            sb.table("market_regime")
            .select("trade_date,mode,shock_score,reason")
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


def _determine_stage(row: dict, probability: float, expected_value: float, mode: str, cfg: dict) -> tuple[str, bool, str | None]:
    bad = _to_float(row.get("bad_news_score"), 0) or 0
    if bad >= 80:
        return "none", True, f"AI除外: bad_news_score={bad:.0f}"
    if expected_value <= float(cfg.get("ai_expected_value_min", 0.0)):
        return "none", False, None
    thresholds = MODE_THRESHOLDS.get(mode or "normal", MODE_THRESHOLDS["normal"]).copy()
    thresholds["early"] = float(cfg.get("ai_probability_early", thresholds["early"]))
    thresholds["confirmed"] = float(cfg.get("ai_probability_confirmed", thresholds["confirmed"]))
    thresholds["strong"] = float(cfg.get("ai_probability_strong", thresholds["strong"]))
    vol = _to_float(row.get("volume_ratio_20d"), 0) or 0
    if probability >= thresholds["strong"] and bad < 60 and vol >= 1.3:
        return "strong_confirmed", False, None
    if probability >= thresholds["confirmed"]:
        return "confirmed", False, None
    if probability >= thresholds["early"]:
        return "early", False, None
    return "none", False, None


def _score_like(row: dict, probability: float) -> float:
    tech = 100.0 * probability
    penalty = min(25.0, (_to_float(row.get("bad_news_score"), 0) or 0) * 0.2)
    return max(0.0, min(100.0, tech - penalty))


def _find_watchlist(sb, code: str) -> dict | None:
    rows = (
        sb.table("stock_drop_watchlist")
        .select("id,code,status,signal_stage,signal_count")
        .eq("code", code)
        .in_("status", ["watching", "rebound_signal", "notified"])
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    return rows[0] if rows else None


def _persist_watchlist(sb, row: dict, result: dict, *, dry_run: bool, force: bool) -> dict | None:
    now = datetime.now(timezone.utc).isoformat()
    existing = _find_watchlist(sb, str(row["code"]))
    prev_count = int((existing or {}).get("signal_count") or 0)
    stage = result["signal_stage"]
    status = "rebound_signal" if stage in SIGNAL_STAGES else "watching"
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
        "updated_at": now,
    }
    if result["is_excluded"]:
        update["status"] = "excluded"
        update["excluded_at"] = now
    if dry_run:
        logger.info("DRYRUN watchlist %s: %s", "update" if existing else "insert", update)
        return {**(existing or {}), **update}
    if existing and not force:
        sb.table("stock_drop_watchlist").update(update).eq("id", existing["id"]).execute()
        return {**existing, **update}
    if existing and force:
        sb.table("stock_drop_watchlist").update(update).eq("id", existing["id"]).execute()
        return {**existing, **update}
    inserted = sb.table("stock_drop_watchlist").insert(update).execute().data or []
    return inserted[0] if inserted else update


def _create_virtual_trade(sb, snapshot: dict, watch: dict, result: dict, *, dry_run: bool) -> None:
    if result["signal_stage"] not in SIGNAL_STAGES:
        return
    try:
        existing = (
            sb.table("virtual_trades")
            .select("id")
            .eq("code", snapshot["code"])
            .eq("status", "open")
            .limit(1)
            .execute()
            .data or []
        )
        if existing:
            return
        now = datetime.now(timezone.utc).isoformat()
        reason = (
            f"AI probability={result['probability']:.2f} expected_value={result['expected_value']:.2f} "
            f"stage={result['signal_stage']} mode={result['mode']}"
        )
        row = {
            "code": snapshot["code"],
            "name": snapshot.get("name"),
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
            "status": "open",
        }
        if dry_run:
            logger.info("DRYRUN virtual_trade insert: %s", row)
            return
        sb.table("virtual_trades").insert(row).execute()
    except Exception as e:
        logger.error("virtual_trade create failed code=%s: %s", snapshot.get("code"), e)


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
        f"AI確率：{result['probability'] * 100:.0f}%",
        f"期待値：{result['expected_value']:+.1f}%",
        f"ステージ：{result['signal_stage']}",
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
        "利確 +5%",
        "損切 -4%",
        "期限 5営業日",
        "",
        "※投資判断は自分で確認",
    ])


def _notify(sb, row: dict, result: dict, cfg: dict) -> None:
    if not cfg.get("ai_notify_enabled", True):
        logger.info("AI notify disabled")
        return
    stage = result["signal_stage"]
    if stage == "early" and not cfg.get("ai_notify_early_enabled", False):
        logger.info("early notify skipped by settings: %s", row.get("code"))
        return
    if stage not in {"confirmed", "strong_confirmed", "early"}:
        return
    msg = _message(row, result)
    users = _eligible_users(sb)
    sent = sum(1 for u in users if _push(u["user_id"], msg))
    logger.info("LINE notify attempted: code=%s users=%d sent=%d", row.get("code"), len(users), sent)


def run(args: argparse.Namespace) -> None:
    if not HAS_BASE_DEPS:
        raise RuntimeError("pandas, numpy and joblib are required")
    sb = _build_supabase()
    cfg = get_settings(force_reload=True)
    if not cfg.get("ai_predict_enabled", True):
        logger.info("ai_predict_enabled=False; exit")
        return

    target_date = _target_date(sb, args)
    regime = _current_mode(sb, target_date)
    mode = str(regime.get("mode") or "normal")
    model_row, bundle = _load_model_bundle(sb)
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
        probabilities = [_fallback_probability(r) for r in snapshots]

    signal_count = 0
    for row, prob in zip(snapshots, probabilities):
        ev = _expected_value(prob)
        stage, is_excluded, exclude_reason = _determine_stage(row, prob, ev, mode, cfg)
        result = {
            "probability": round(prob, 6),
            "expected_value": round(ev, 4),
            "signal_stage": stage,
            "signal_score": round(_score_like(row, prob), 2),
            "mode": mode,
            "is_excluded": is_excluded,
            "exclude_reason": exclude_reason,
            "model_version": (model_row or {}).get("model_version") if model_row else "fallback_rule",
        }
        if stage in SIGNAL_STAGES:
            signal_count += 1
        logger.info(
            "%spredict: %s %s prob=%.3f ev=%.2f stage=%s bad=%.0f mode=%s",
            "DRYRUN " if args.dry_run else "",
            row.get("code"), row.get("name") or "", prob, ev, stage,
            _to_float(row.get("bad_news_score"), 0) or 0, mode,
        )
        watch = _persist_watchlist(sb, row, result, dry_run=args.dry_run, force=args.force)
        _create_virtual_trade(sb, row, watch or {}, result, dry_run=args.dry_run)
        if args.notify and not args.dry_run and stage in {"confirmed", "strong_confirmed", "early"} and not is_excluded:
            _notify(sb, row, result, cfg)
    logger.info("complete: predictions=%d signals=%d dry_run=%s", len(snapshots), signal_count, args.dry_run)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict rebound probabilities")
    p.add_argument("--date")
    p.add_argument("--latest", action="store_true")
    p.add_argument("--code")
    p.add_argument("--notify", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--limit", type=int)
    p.add_argument("--fallback-rule", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
