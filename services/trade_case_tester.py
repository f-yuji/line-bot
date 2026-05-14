"""Trade case comparison engine.

This module writes only trade_case_* tables. It never updates virtual_trades,
watchlist rows, notifications, or active model state.
"""

from __future__ import annotations

import logging
import math
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from bisect import bisect_right
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from services.signal_stage import SIGNAL_STAGES, evaluate_signal_stage

try:
    import joblib
    import numpy as np
    import pandas as pd

    HAS_MODEL_DEPS = True
except Exception:  # pragma: no cover - optional runtime fallback
    HAS_MODEL_DEPS = False

try:
    from scripts.train_rebound_model import BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES
except Exception:  # pragma: no cover
    BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES = [], [], []


load_dotenv()
logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
MAX_FUTURE_DAYS = 20
PROFIT_EXIT_REASONS = {
    "tp",
    "trailing_stop",
    "pullback_exit",
    "ma_break_exit",
    "rsi_reversal_exit",
    "volume_fade_exit",
    "atr_trailing",
}


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


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _fetch_all(query_factory, *, page_size: int = 1000, label: str = "rows") -> list[dict]:
    rows: list[dict] = []
    last_id = 0
    while True:
        query = query_factory(last_id).limit(page_size)
        data = query.execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        last_id = max(int(r.get("id") or last_id) for r in data)
        if len(rows) % 10000 == 0:
            logger.info("[case_test] load %s progress rows=%d", label, len(rows))
    return rows


def _active_model_bundle(sb) -> dict | None:
    if not HAS_MODEL_DEPS:
        return None
    for model_name in ("rebound_lgbm_5d", "rebound_lgbm"):
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
        if not rows:
            continue
        path = ROOT / str(rows[0].get("model_path") or "")
        if not path.exists():
            logger.warning("[case_test] active model file missing: %s", path)
            return None
        bundle = joblib.load(path)
        logger.info("[case_test] active model loaded name=%s path=%s", model_name, path)
        return bundle
    return None


def _model_frame(df: "pd.DataFrame", bundle: dict) -> "pd.DataFrame":
    numeric_cols = list(bundle.get("numeric_columns") or [])
    categorical_cols = list(bundle.get("categorical_columns") or [])
    fill_values = dict(bundle.get("fill_values") or {})
    feature_columns = list(bundle.get("feature_columns") or [])
    work = df.copy()

    for col in numeric_cols:
        if col not in work.columns:
            work[col] = 0
        work[col] = pd.to_numeric(work[col], errors="coerce")
    x_num = (
        work[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(fill_values)
        if numeric_cols else pd.DataFrame(index=work.index)
    )

    for col in categorical_cols:
        if col not in work.columns:
            work[col] = "unknown"
        work[col] = work[col].fillna("unknown").replace("", "unknown").astype(str)
    x_cat = (
        pd.get_dummies(work[categorical_cols], prefix=categorical_cols, dummy_na=False)
        if categorical_cols else pd.DataFrame(index=work.index)
    )
    x = pd.concat([x_num, x_cat], axis=1)
    return x.reindex(columns=feature_columns, fill_value=0)


def _proxy_rule_score(row: dict) -> float:
    score = 45.0
    drop = abs(_to_float(row.get("drop_pct"), 0) or 0)
    rsi = _to_float(row.get("rsi14"), 50) or 50
    vol = _to_float(row.get("volume_ratio_20d"), 1) or 1
    score += min(20, drop * 3.0)
    if rsi <= 35:
        score += 12
    elif rsi <= 45:
        score += 6
    score += min(12, max(0, vol - 1) * 8)
    score -= min(20, _to_float(row.get("bad_news_score"), 0) or 0)
    return round(max(0, min(100, score)), 2)


def _proxy_ai_score(row: dict) -> float:
    rule = _proxy_rule_score(row)
    draw = abs(_to_float(row.get("market_shock_score"), 0) or 0)
    return round(max(0.05, min(0.95, (rule / 100.0) - draw * 0.02)), 6)


def _score_candidates(rows: list[dict], bundle: dict | None) -> list[dict]:
    if bundle and HAS_MODEL_DEPS and rows:
        try:
            df = pd.DataFrame(rows)
            x = _model_frame(df, bundle)
            probs = bundle["model"].predict_proba(x)[:, 1]
            for row, prob in zip(rows, probs):
                row["signal_probability"] = round(float(prob), 6)
        except Exception as e:
            logger.warning("[case_test] model scoring failed; use proxy score: %s", e)
            bundle = None

    for row in rows:
        if row.get("signal_probability") is None:
            row["signal_probability"] = _proxy_ai_score(row)
        row["rule_score"] = _proxy_rule_score(row)
        row["expected_value"] = round(_expected_value_for_rules(row, {"tp_pct": 0.06, "sl_pct": -0.04}), 3)
        stage = evaluate_signal_stage(row["signal_probability"], row["rule_score"], row["expected_value"])
        row["signal_stage"] = stage["stage"]
    return rows


def _load_weekly_margin_rows(sb, period_start: date, period_end: date) -> list[dict]:
    cols = (
        "id,code,date,short_margin_outstanding,long_margin_outstanding,"
        "margin_ratio,short_margin_change,long_margin_change"
    )
    start_s = (period_start - timedelta(days=45)).isoformat()
    end_s = period_end.isoformat()
    page_size = 1000

    # Use offset pagination ordered by date — date index is efficient;
    # id-cursor ordering is slow when recently-imported historical rows have
    # high IDs and the query can't use the date index for ordering.
    try:
        rows: list[dict] = []
        offset = 0
        while True:
            data = (
                sb.table("stock_weekly_margin_interest")
                .select(cols)
                .gte("date", start_s)
                .lte("date", end_s)
                .order("date")
                .range(offset, offset + page_size - 1)
                .execute()
                .data or []
            )
            rows.extend(data)
            if len(data) < page_size:
                break
            offset += page_size
            if len(rows) % 10000 == 0:
                logger.info("[case_test] load weekly_margin progress rows=%d", len(rows))
        logger.info("[case_test] loaded weekly margin rows=%d", len(rows))
        return rows
    except Exception as e:
        logger.warning("[case_test] weekly margin load failed: %s", e)
        return []


def _load_strategy_settings(sb) -> dict:
    from settings_loader import DEFAULTS
    try:
        rows = sb.table("strategy_settings").select("*").eq("user_id", "global").limit(1).execute().data or []
        if rows:
            row = rows[0]
            return {k: (row[k] if row.get(k) is not None else v) for k, v in DEFAULTS.items()}
    except Exception as e:
        logger.warning("[case_test] strategy_settings load failed: %s", e)
    return dict(DEFAULTS)


def _build_current_settings_rules(cfg: dict) -> dict:
    rules: dict = {
        "exit_type": "pullback_exit",
        "pullback_day_pct": -float(cfg.get("virtual_exit_pullback_pct", 2.0)) / 100,
        "initial_sl_pct": -float(cfg.get("virtual_exit_stop_loss_pct", 4.0)) / 100,
        "max_holding_days": int(cfg.get("virtual_exit_holding_days", 5)),
        "entry_sort": "expected_value_desc",
        "entry_rank_limit": int(cfg.get("entry_rank_limit", 10)),
        "max_open_positions": int(cfg.get("max_open_positions", 20)),
        "max_daily_entries": int(cfg.get("max_daily_entries", 5)),
        "max_sector_positions": int(cfg.get("max_sector_positions", 2)),
        "min_ai_score": float(cfg.get("ai_probability_confirmed", 0.50)),
        "allowed_stages": ["confirmed", "strong_confirmed"],
    }
    if cfg.get("entry_margin_filter_enabled", True):
        rules["use_margin_filter"] = True
        rules["require_margin_data"] = bool(cfg.get("entry_margin_require_data", True))
        rules["max_margin_ratio"] = float(cfg.get("entry_max_margin_ratio", 5.0))
    return rules


def _upsert_current_settings_case(sb) -> None:
    cfg = _load_strategy_settings(sb)
    rules = _build_current_settings_rules(cfg)
    sb.table("trade_case_definitions").upsert(
        {
            "case_key": "current_settings",
            "case_name": "現状設定",
            "description": "比較テスト実行時点の strategy_settings を反映したケース。",
            "is_enabled": True,
            "rules": rules,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="case_key",
    ).execute()


def _attach_weekly_margin(candidates: list[dict], margin_rows: list[dict]) -> None:
    by_code: dict[str, list[tuple[date, dict]]] = defaultdict(list)
    for row in margin_rows:
        code = str(row.get("code") or "")
        if not code:
            continue
        try:
            d = _to_date(row.get("date"))
        except Exception:
            continue
        by_code[code].append((d, row))

    index: dict[str, tuple[list[date], list[dict]]] = {}
    for code, items in by_code.items():
        items.sort(key=lambda x: x[0])
        index[code] = ([d for d, _ in items], [r for _, r in items])

    for row in candidates:
        code = str(row.get("code") or "")
        trade_date = _to_date(row.get("trade_date"))
        dates, rows = index.get(code, ([], []))
        pos = bisect_right(dates, trade_date) - 1
        if pos < 0:
            continue
        margin = rows[pos]
        row["margin_date"] = margin.get("date")
        row["margin_short_outstanding"] = _to_float(margin.get("short_margin_outstanding"), None)
        row["margin_long_outstanding"] = _to_float(margin.get("long_margin_outstanding"), None)
        row["margin_ratio"] = _to_float(margin.get("margin_ratio"), None)
        row["margin_short_change"] = _to_float(margin.get("short_margin_change"), None)
        row["margin_long_change"] = _to_float(margin.get("long_margin_change"), None)


def _load_candidates(sb, period_start: date, period_end: date) -> list[dict]:
    snap_cols = sorted(set(
        [
            "id", "trade_date", "code", "name", "market", "sector", "close",
            "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
            "volume_ratio_20d", "bad_news_score", "market_shock_score",
        ]
        + list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES)
    ))
    future_cols = []
    for day in range(1, MAX_FUTURE_DAYS + 1):
        future_cols.extend([
            f"future_high_{day}d",
            f"future_low_{day}d",
            f"future_close_{day}d",
        ])
    label_cols = [
        "id", "feature_snapshot_id", "trade_date", "code", "entry_price",
    ] + future_cols
    start_s = period_start.isoformat()
    end_s = period_end.isoformat()

    def snapshot_query(last_id: int):
        q = (
            sb.table("stock_feature_snapshots")
            .select(",".join(snap_cols))
            .eq("is_drop_candidate", True)
            .eq("is_tradeable", True)
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .order("id")
        )
        return q.gt("id", last_id) if last_id else q

    def label_query(last_id: int):
        q = (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("id")
        )
        return q.gt("id", last_id) if last_id else q

    snapshots = _fetch_all(snapshot_query, label="snapshots")
    labels = _fetch_all(label_query, label="labels")
    label_by_snapshot = {str(r.get("feature_snapshot_id")): r for r in labels if r.get("feature_snapshot_id")}

    rows: list[dict] = []
    for snap in snapshots:
        label = label_by_snapshot.get(str(snap.get("id")))
        if not label:
            continue
        merged = dict(snap)
        for key, value in label.items():
            if key in {"id", "code", "trade_date"}:
                merged[f"label_{key}"] = value
            else:
                merged[key] = value
        rows.append(merged)
    logger.info("[case_test] loaded candidate rows=%d", len(rows))
    _attach_weekly_margin(rows, _load_weekly_margin_rows(sb, period_start, period_end))
    return _score_candidates(rows, _active_model_bundle(sb))


def _expected_value_for_rules(row: dict, rules: dict) -> float:
    """Pre-entry EV used for ranking.

    This intentionally differs from raw AI score. The AI score is the main
    signal, while EV also reflects the rule's TP/SL and a small rule-score
    adjustment so AI-top and EV-top can diverge.
    """

    ai = _to_float(row.get("signal_probability"), 0.0) or 0.0
    rule = _to_float(row.get("rule_score"), 50.0) or 50.0
    tp_pct = abs(float(rules.get("tp_pct", 0.06))) * 100
    sl_pct = -abs(float(rules.get("sl_pct", -0.04))) * 100
    bad = _to_float(row.get("bad_news_score"), 0.0) or 0.0
    rule_adjust = (rule - 50.0) * 0.035
    bad_adjust = min(1.5, bad * 0.20)
    return round((ai * tp_pct) + ((1.0 - ai) * sl_pct) + rule_adjust - bad_adjust, 3)


def _sort_candidates(rows: list[dict], sort_key: str, rules: dict) -> list[dict]:
    if sort_key == "signal_probability_desc":
        return sorted(
            rows,
            key=lambda r: (
                _to_float(r.get("signal_probability"), 0) or 0,
                _expected_value_for_rules(r, rules),
            ),
            reverse=True,
        )
    return sorted(
        rows,
        key=lambda r: (
            _expected_value_for_rules(r, rules),
            _to_float(r.get("signal_probability"), 0) or 0,
        ),
        reverse=True,
    )


def _case_rules(case: dict) -> dict:
    rules = case.get("rules") or {}
    if isinstance(rules, str):
        import json
        rules = json.loads(rules)
    return dict(rules)


def _adjusted_rules(base_rules: dict, sample_row: dict | None = None) -> dict:
    rules = dict(base_rules)
    regime = (sample_row or {}).get("market_regime")
    regime_rules = ((rules.get("regime_adjust") or {}).get(regime) or {}) if regime else {}
    if regime_rules:
        if regime_rules.get("entry_rank_limit_multiplier") is not None:
            rules["entry_rank_limit"] = max(
                1,
                int(math.ceil(_to_int(rules.get("entry_rank_limit"), 10) * float(regime_rules["entry_rank_limit_multiplier"]))),
            )
        if regime_rules.get("min_ai_score_add") is not None:
            rules["min_ai_score"] = float(rules.get("min_ai_score") or 0) + float(regime_rules["min_ai_score_add"])
    return rules


def _passes_credit_rules(row: dict, rules: dict) -> bool:
    if not rules.get("use_margin_filter"):
        return True

    margin_ratio = _to_float(row.get("margin_ratio"), None)
    long_out = _to_float(row.get("margin_long_outstanding"), None)
    short_out = _to_float(row.get("margin_short_outstanding"), None)

    if rules.get("require_margin_data") and margin_ratio is None:
        return False
    if margin_ratio is None:
        return True

    if rules.get("max_margin_ratio") is not None and margin_ratio > float(rules["max_margin_ratio"]):
        return False
    if rules.get("min_margin_ratio") is not None and margin_ratio < float(rules["min_margin_ratio"]):
        return False

    short_long_ratio = (short_out / long_out) if long_out and short_out is not None else None
    if rules.get("min_short_long_ratio") is not None:
        if short_long_ratio is None or short_long_ratio < float(rules["min_short_long_ratio"]):
            return False
    if rules.get("max_short_long_ratio") is not None:
        if short_long_ratio is not None and short_long_ratio > float(rules["max_short_long_ratio"]):
            return False
    return True


def _price_path(row: dict, rules: dict) -> tuple[float | None, date | None, list[dict]]:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    if not entry or entry <= 0:
        return None, None, []
    max_days = min(MAX_FUTURE_DAYS, max(1, _to_int(rules.get("max_holding_days"), 5)))
    entry_date = _to_date(row.get("trade_date"))
    days: list[dict] = []
    prev_close = entry
    for day in range(1, max_days + 1):
        high = _to_float(row.get(f"future_high_{day}d"), None)
        low = _to_float(row.get(f"future_low_{day}d"), None)
        close = _to_float(row.get(f"future_close_{day}d"), None)
        if high is None and low is None and close is None:
            break
        if high is None and close is not None:
            high = close
        if low is None and close is not None:
            low = close
        if close is None:
            close = prev_close
        days.append({
            "day": day,
            "date": entry_date + timedelta(days=day),
            "high": high,
            "low": low,
            "close": close,
            "prev_close": prev_close,
        })
        prev_close = close
    return entry, entry_date, days


def _peak_drawdown(entry: float, days: list[dict]) -> tuple[float | None, float | None]:
    peak = None
    trough = None
    for d in days:
        high = _to_float(d.get("high"), None)
        low = _to_float(d.get("low"), None)
        if high is not None:
            val = (high - entry) / entry * 100
            peak = val if peak is None else max(peak, val)
        if low is not None:
            val = (low - entry) / entry * 100
            trough = val if trough is None else min(trough, val)
    return (
        round(peak, 3) if peak is not None else None,
        round(trough, 3) if trough is not None else None,
    )


def _close_trade(
    entry: float,
    exit_date: date,
    exit_price: float,
    reason: str,
    holding_days: int,
    *,
    days: list[dict],
    exit_signal_value: float | None = None,
    exit_indicator: str | None = None,
    trailing_triggered: bool = False,
) -> dict:
    profit_pct = (exit_price - entry) / entry * 100
    peak, dd = _peak_drawdown(entry, days[:holding_days])
    return {
        "status": "closed",
        "exit_reason": reason,
        "exit_date": exit_date.isoformat(),
        "exit_price": round(exit_price, 4),
        "profit_pct": round(profit_pct, 3),
        "profit_yen": round((exit_price - entry) * 100, 0),
        "holding_days": holding_days,
        "peak_profit_pct": peak,
        "max_drawdown_pct": dd,
        "trailing_triggered": trailing_triggered,
        "exit_signal_value": round(exit_signal_value, 4) if exit_signal_value is not None else None,
        "exit_indicator": exit_indicator,
    }


def _timeout_or_open(entry: float, days: list[dict], max_days: int) -> dict:
    if not days:
        return {
            "status": "open",
            "exit_reason": "open",
            "holding_days": None,
            "peak_profit_pct": None,
            "max_drawdown_pct": None,
            "trailing_triggered": False,
        }
    last = days[min(max_days, len(days)) - 1]
    return _close_trade(
        entry,
        last["date"],
        _to_float(last.get("close"), entry) or entry,
        "timeout",
        int(last["day"]),
        days=days,
        exit_indicator="max_holding_days",
    )


def simulate_fixed_tp_sl(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    tp_pct = float(rules.get("tp_pct", 0.06))
    sl_pct = float(rules.get("sl_pct", -0.04))
    tp_price = entry * (1 + tp_pct)
    sl_price = entry * (1 + sl_pct)
    for d in days:
        high = _to_float(d.get("high"), None)
        low = _to_float(d.get("low"), None)
        if high is None or low is None:
            continue
        if low <= sl_price:
            return _close_trade(entry, d["date"], sl_price, "sl", d["day"], days=days, exit_signal_value=sl_pct * 100, exit_indicator="fixed_sl")
        if high >= tp_price:
            return _close_trade(entry, d["date"], tp_price, "tp", d["day"], days=days, exit_signal_value=tp_pct * 100, exit_indicator="fixed_tp")
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def simulate_trailing_stop(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    trailing_drop = abs(float(rules.get("trailing_drop_pct", -0.03)))
    peak_price = entry
    for d in days:
        high = _to_float(d.get("high"), None)
        low = _to_float(d.get("low"), None)
        if high is not None:
            peak_price = max(peak_price, high)
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        stop_price = peak_price * (1 - trailing_drop)
        if peak_price > entry and low is not None and low <= stop_price:
            return _close_trade(entry, d["date"], stop_price, "trailing_stop", d["day"], days=days, exit_signal_value=(stop_price - entry) / entry * 100, exit_indicator="trailing_stop", trailing_triggered=True)
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def simulate_pullback_exit(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    pullback = float(rules.get("pullback_day_pct", -0.02))
    for d in days:
        low = _to_float(d.get("low"), None)
        close = _to_float(d.get("close"), None)
        prev = _to_float(d.get("prev_close"), entry) or entry
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        day_return = (close - prev) / prev if close is not None and prev else 0
        if close is not None and close > entry and day_return <= pullback:
            return _close_trade(entry, d["date"], close, "pullback_exit", d["day"], days=days, exit_signal_value=day_return * 100, exit_indicator="daily_pullback")
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def simulate_ma_break_exit(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    period = max(2, _to_int(rules.get("ma_period"), 5))
    closes = [entry]
    for d in days:
        low = _to_float(d.get("low"), None)
        close = _to_float(d.get("close"), None)
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        if close is not None:
            closes.append(close)
            ma = sum(closes[-period:]) / min(period, len(closes))
            if len(closes) >= period and close < ma and close > entry:
                return _close_trade(entry, d["date"], close, "ma_break_exit", d["day"], days=days, exit_signal_value=(close - ma) / ma * 100, exit_indicator=f"ma{period}_break")
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def _rsi_from_closes(closes: list[float], period: int = 5) -> float | None:
    if len(closes) < 3:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    use = deltas[-period:]
    gains = [d for d in use if d > 0]
    losses = [-d for d in use if d < 0]
    avg_gain = sum(gains) / len(use) if use else 0
    avg_loss = sum(losses) / len(use) if use else 0
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def simulate_rsi_exit(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    threshold = float(rules.get("overbought_rsi", 70))
    closes = [entry]
    was_overbought = False
    prev_rsi = None
    for d in days:
        low = _to_float(d.get("low"), None)
        close = _to_float(d.get("close"), None)
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        if close is None:
            continue
        closes.append(close)
        rsi = _rsi_from_closes(closes)
        if rsi is not None and rsi >= threshold:
            was_overbought = True
        if was_overbought and rsi is not None and prev_rsi is not None and rsi < prev_rsi and close > entry:
            return _close_trade(entry, d["date"], close, "rsi_reversal_exit", d["day"], days=days, exit_signal_value=rsi, exit_indicator="rsi_reversal")
        prev_rsi = rsi
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def simulate_volume_fade_exit(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    volume_ratio = _to_float(row.get("volume_ratio_20d"), None)
    fade_ratio = float(rules.get("volume_drop_ratio", 0.5))
    for d in days:
        low = _to_float(d.get("low"), None)
        close = _to_float(d.get("close"), None)
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        # Future volume is not in stock_rebound_labels, so this is a conservative
        # proxy using the entry-day volume ratio only.
        if close is not None and close > entry and volume_ratio is not None and volume_ratio <= fade_ratio:
            return _close_trade(entry, d["date"], close, "volume_fade_exit", d["day"], days=days, exit_signal_value=volume_ratio, exit_indicator="entry_volume_fade_proxy")
    out = _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))
    out["exit_indicator"] = out.get("exit_indicator") or "future_volume_unavailable"
    return out


def simulate_atr_trailing(row: dict, rules: dict) -> dict:
    entry, _entry_date, days = _price_path(row, rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry"}
    initial_sl = entry * (1 + float(rules.get("initial_sl_pct", rules.get("sl_pct", -0.04))))
    multiplier = float(rules.get("atr_multiplier", 1.5))
    peak_price = entry
    true_ranges: list[float] = []
    for d in days:
        high = _to_float(d.get("high"), None)
        low = _to_float(d.get("low"), None)
        prev = _to_float(d.get("prev_close"), entry) or entry
        if high is not None:
            peak_price = max(peak_price, high)
        if high is not None and low is not None:
            true_ranges.append(max(high - low, abs(high - prev), abs(low - prev)))
        atr = (sum(true_ranges[-5:]) / min(5, len(true_ranges))) if true_ranges else 0
        if low is not None and low <= initial_sl:
            return _close_trade(entry, d["date"], initial_sl, "sl", d["day"], days=days, exit_signal_value=(initial_sl - entry) / entry * 100, exit_indicator="initial_sl")
        stop_price = peak_price - (atr * multiplier)
        if atr > 0 and peak_price > entry and low is not None and low <= stop_price:
            return _close_trade(entry, d["date"], stop_price, "atr_trailing", d["day"], days=days, exit_signal_value=atr, exit_indicator="atr_trailing", trailing_triggered=True)
    return _timeout_or_open(entry, days, _to_int(rules.get("max_holding_days"), 5))


def _exit_for_candidate(row: dict, rules: dict) -> dict:
    exit_type = str(rules.get("exit_type") or "fixed_tp_sl")
    if exit_type == "trailing_stop":
        return simulate_trailing_stop(row, rules)
    if exit_type == "pullback_exit":
        return simulate_pullback_exit(row, rules)
    if exit_type == "ma_break_exit":
        return simulate_ma_break_exit(row, rules)
    if exit_type == "rsi_reversal_exit":
        return simulate_rsi_exit(row, rules)
    if exit_type == "volume_fade_exit":
        return simulate_volume_fade_exit(row, rules)
    if exit_type == "atr_trailing":
        return simulate_atr_trailing(row, rules)
    return simulate_fixed_tp_sl(row, rules)

    last_close = None
    for day in range(1, max_days + 1):
        high = _to_float(row.get(f"future_high_{day}d"), None)
        low = _to_float(row.get(f"future_low_{day}d"), None)
        close = _to_float(row.get(f"future_close_{day}d"), None)
        if close is not None:
            last_close = close
        if high is None or low is None:
            continue
        hit_tp = high >= tp_price
        hit_sl = low <= sl_price
        if hit_sl:
            profit_pct = sl_pct * 100
            return {
                "status": "closed",
                "exit_reason": "sl",
                "exit_date": (entry_date + timedelta(days=day)).isoformat(),
                "exit_price": round(sl_price, 4),
                "profit_pct": round(profit_pct, 3),
                "profit_yen": round((sl_price - entry) * 100, 0),
                "holding_days": day,
            }
        if hit_tp:
            profit_pct = tp_pct * 100
            return {
                "status": "closed",
                "exit_reason": "tp",
                "exit_date": (entry_date + timedelta(days=day)).isoformat(),
                "exit_price": round(tp_price, 4),
                "profit_pct": round(profit_pct, 3),
                "profit_yen": round((tp_price - entry) * 100, 0),
                "holding_days": day,
            }

    if last_close is None:
        return {"status": "open", "exit_reason": "open", "holding_days": None}
    profit_pct = (last_close - entry) / entry * 100
    return {
        "status": "closed",
        "exit_reason": "timeout",
        "exit_date": (entry_date + timedelta(days=max_days)).isoformat(),
        "exit_price": round(last_close, 4),
        "profit_pct": round(profit_pct, 3),
        "profit_yen": round((last_close - entry) * 100, 0),
        "holding_days": max_days,
    }


def _simulate_case(run_id: str, case: dict, candidates: list[dict]) -> tuple[list[dict], dict]:
    base_rules = _case_rules(case)
    if base_rules.get("require_model_agreement"):
        logger.info("[case_test] case=%s skipped: model agreement scores are not stored yet", case.get("case_key"))
        return [], _empty_result(run_id, case["id"])

    by_date: dict[str, list[dict]] = defaultdict(list)
    allowed = set(base_rules.get("allowed_stages") or list(SIGNAL_STAGES))
    base_min_ai = float(base_rules.get("min_ai_score") or 0)
    for row in candidates:
        rules = _adjusted_rules(base_rules, row)
        min_ai = float(rules.get("min_ai_score") or base_min_ai)
        if row.get("signal_stage") not in allowed:
            continue
        if (_to_float(row.get("signal_probability"), 0) or 0) < min_ai:
            continue
        if not _passes_credit_rules(row, rules):
            continue
        by_date[str(row.get("trade_date"))].append(row)

    open_positions: list[dict] = []
    simulations: list[dict] = []
    max_concurrent = 0

    for trade_date in sorted(by_date):
        today = _to_date(trade_date)
        open_positions = [
            p for p in open_positions
            if not p.get("exit_date") or _to_date(p["exit_date"]) >= today
        ]
        max_concurrent = max(max_concurrent, len(open_positions))

        sample_rules = _adjusted_rules(base_rules, by_date[trade_date][0])
        rank_limit = _to_int(sample_rules.get("entry_rank_limit"), 10)
        max_daily = _to_int(sample_rules.get("max_daily_entries"), 5)
        max_open = _to_int(sample_rules.get("max_open_positions"), 20)
        max_sector = _to_int(sample_rules.get("max_sector_positions"), 99)
        daily_rows = _sort_candidates(
            by_date[trade_date],
            str(sample_rules.get("entry_sort") or "expected_value_desc"),
            sample_rules,
        )[:rank_limit]
        daily_entries = 0

        for row in daily_rows:
            if daily_entries >= max_daily:
                break
            if len(open_positions) >= max_open:
                break
            sector = row.get("sector") or "unknown"
            sector_count = sum(1 for p in open_positions if (p.get("sector") or "unknown") == sector)
            if sector_count >= max_sector:
                continue

            exit_data = _exit_for_candidate(row, sample_rules)
            entry_ev = _expected_value_for_rules(row, sample_rules)
            sim = {
                "run_id": run_id,
                "case_id": case["id"],
                "exit_type": str(sample_rules.get("exit_type") or "fixed_tp_sl"),
                "code": str(row.get("code")),
                "name": row.get("name"),
                "sector": sector,
                "entry_date": trade_date,
                "entry_price": row.get("entry_price") or row.get("close"),
                "signal_stage": row.get("signal_stage"),
                "signal_probability": row.get("signal_probability"),
                "expected_value": entry_ev,
                "rule_score": row.get("rule_score"),
                "market_regime": row.get("market_regime"),
                "market_regime_label": row.get("market_regime_label"),
                "market_nikkei_pct": row.get("market_nikkei_pct"),
                "market_topix_pct": row.get("market_topix_pct"),
                "margin_date": row.get("margin_date"),
                "margin_ratio": row.get("margin_ratio"),
                "margin_long_outstanding": row.get("margin_long_outstanding"),
                "margin_short_outstanding": row.get("margin_short_outstanding"),
                **exit_data,
            }
            simulations.append(sim)
            open_positions.append(sim)
            daily_entries += 1
            max_concurrent = max(max_concurrent, len(open_positions))

    result = _build_result(run_id, case["id"], simulations, max_concurrent)
    logger.info("[case_test] case=%s entries=%d", case.get("case_key"), len(simulations))
    return simulations, result


def _empty_result(run_id: str, case_id: str) -> dict:
    return {
        "run_id": run_id,
        "case_id": case_id,
        "entry_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "open_count": 0,
        "tp_count": 0,
        "sl_count": 0,
        "timeout_count": 0,
        "avg_peak_profit_pct": None,
        "avg_trade_drawdown_pct": None,
    }


def _build_result(run_id: str, case_id: str, simulations: list[dict], max_concurrent: int) -> dict:
    closed = [s for s in simulations if s.get("status") == "closed" and s.get("profit_pct") is not None]
    wins = [s for s in closed if (_to_float(s.get("profit_pct"), 0) or 0) > 0]
    losses = [s for s in closed if (_to_float(s.get("profit_pct"), 0) or 0) <= 0]
    total_profit_yen = sum(_to_float(s.get("profit_yen"), 0) or 0 for s in closed)
    entry_costs = [
        (_to_float(s.get("entry_price"), 0) or 0) * 100
        for s in simulations
        if _to_float(s.get("entry_price"), 0)
    ]
    avg_position_cost = sum(entry_costs) / len(entry_costs) if entry_costs else 0
    capital_base = avg_position_cost * max(1, max_concurrent)
    equity_yen = 0.0
    peak_yen = 0.0
    max_dd_yen = 0.0
    for sim in sorted(closed, key=lambda r: (r.get("exit_date") or "", r.get("entry_date") or "")):
        equity_yen += _to_float(sim.get("profit_yen"), 0) or 0
        peak_yen = max(peak_yen, equity_yen)
        max_dd_yen = min(max_dd_yen, equity_yen - peak_yen)
    closed_count = len(closed)
    peak_values = [
        _to_float(s.get("peak_profit_pct"), None)
        for s in closed
        if _to_float(s.get("peak_profit_pct"), None) is not None
    ]
    trade_dd_values = [
        _to_float(s.get("max_drawdown_pct"), None)
        for s in closed
        if _to_float(s.get("max_drawdown_pct"), None) is not None
    ]
    total_profit_pct = (total_profit_yen / capital_base * 100) if capital_base > 0 else None
    max_dd_pct = (max_dd_yen / capital_base * 100) if capital_base > 0 else None
    return {
        "run_id": run_id,
        "case_id": case_id,
        "entry_count": len(simulations),
        "win_count": len(wins),
        "loss_count": len(losses),
        "open_count": len([s for s in simulations if s.get("status") != "closed"]),
        "win_rate": round(len(wins) / closed_count * 100, 1) if closed_count else None,
        "avg_profit_pct": round(sum(_to_float(s.get("profit_pct"), 0) or 0 for s in wins) / len(wins), 3) if wins else None,
        "avg_loss_pct": round(sum(_to_float(s.get("profit_pct"), 0) or 0 for s in losses) / len(losses), 3) if losses else None,
        "expected_value_pct": round(sum(_to_float(s.get("profit_pct"), 0) or 0 for s in closed) / closed_count, 3) if closed_count else None,
        "total_profit_pct": round(total_profit_pct, 3) if total_profit_pct is not None else None,
        "total_profit_yen": round(total_profit_yen, 0),
        "max_drawdown_pct": round(max_dd_pct, 3) if max_dd_pct is not None else None,
        "max_open_positions": max_concurrent,
        "avg_holding_days": round(sum(_to_float(s.get("holding_days"), 0) or 0 for s in closed) / closed_count, 2) if closed_count else None,
        "tp_count": len([
            s for s in closed
            if s.get("exit_reason") in PROFIT_EXIT_REASONS and (_to_float(s.get("profit_pct"), 0) or 0) > 0
        ]),
        "sl_count": len([s for s in closed if s.get("exit_reason") == "sl"]),
        "timeout_count": len([s for s in closed if s.get("exit_reason") == "timeout"]),
        "avg_peak_profit_pct": round(sum(peak_values) / len(peak_values), 3) if peak_values else None,
        "avg_trade_drawdown_pct": round(sum(trade_dd_values) / len(trade_dd_values), 3) if trade_dd_values else None,
    }


# ─── Readonly helpers (offset pagination + ID-batch loading) ─────────────────

def _fetch_all_by_offset(query_factory, *, page_size: int = 1000, label: str = "rows") -> list[dict]:
    """Offset pagination ordered by trade_date. Avoids timeout from cursor on mixed-date IDs."""
    rows: list[dict] = []
    offset = 0
    while True:
        data = query_factory().range(offset, offset + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        offset += page_size
        if len(rows) % 10000 == 0:
            logger.info("[case_test] load %s: %d rows", label, len(rows))
    return rows


def _fetch_snapshots_by_ids(sb, ids: list[int], snap_cols: list[str], *, batch_size: int = 500) -> list[dict]:
    """Batch load snapshots by ID set. Bypasses slow composite-filter date+flag scans."""
    rows: list[dict] = []
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        data = (
            sb.table("stock_feature_snapshots")
            .select(",".join(snap_cols))
            .in_("id", batch)
            .execute()
            .data or []
        )
        rows.extend(data)
    return rows


def _load_candidates_v2(sb, period_start: date, period_end: date) -> list[dict]:
    """Timeout-safe loader: labels first (offset pagination) → snapshot IDs → batch load."""
    snap_cols = sorted(set(
        [
            "id", "trade_date", "code", "name", "market", "sector", "close",
            "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
            "volume_ratio_20d", "bad_news_score", "market_shock_score",
        ]
        + list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES)
    ))
    future_cols: list[str] = []
    for day in range(1, MAX_FUTURE_DAYS + 1):
        future_cols += [f"future_high_{day}d", f"future_low_{day}d", f"future_close_{day}d"]
    label_cols = ["id", "feature_snapshot_id", "trade_date", "code", "entry_price"] + future_cols
    start_s = period_start.isoformat()
    end_s = period_end.isoformat()

    def label_query():
        return (
            sb.table("stock_rebound_labels")
            .select(",".join(label_cols))
            .gte("trade_date", start_s)
            .lte("trade_date", end_s)
            .not_.is_("future_high_5d", "null")
            .not_.is_("future_low_5d", "null")
            .order("trade_date")
        )

    labels = _fetch_all_by_offset(label_query, label="labels")
    logger.info("[case_test] v2 labels loaded rows=%d", len(labels))

    snap_ids = [int(r["feature_snapshot_id"]) for r in labels if r.get("feature_snapshot_id")]
    if not snap_ids:
        logger.warning("[case_test] v2 no labels for period %s..%s", start_s, end_s)
        return []

    snapshots = _fetch_snapshots_by_ids(sb, snap_ids, snap_cols)
    logger.info("[case_test] v2 snapshots loaded rows=%d", len(snapshots))

    snap_by_id = {
        str(s["id"]): s
        for s in snapshots
        if s.get("is_drop_candidate") and s.get("is_tradeable")
    }

    rows: list[dict] = []
    for label in labels:
        snap = snap_by_id.get(str(label.get("feature_snapshot_id")))
        if not snap:
            continue
        merged = dict(snap)
        for key, value in label.items():
            if key in {"id", "code", "trade_date"}:
                merged[f"label_{key}"] = value
            else:
                merged[key] = value
        rows.append(merged)

    logger.info("[case_test] v2 merged candidate rows=%d", len(rows))
    _attach_weekly_margin(rows, _load_weekly_margin_rows(sb, period_start, period_end))
    return _score_candidates(rows, _active_model_bundle(sb))


def run_trade_case_test_readonly(
    period_start: str | date,
    period_end: str | date,
    case_keys: list[str] | None = None,
    sb=None,
) -> tuple[list[dict], dict[str, list[dict]], dict[str, dict]]:
    """Read-only variant: no 90-day limit, no DB writes.

    Returns (cases, sims_by_case_key, results_by_case_key).
    """
    sb = sb or _build_supabase()
    start = _to_date(period_start)
    end = _to_date(period_end)
    if end < start:
        raise ValueError("period_end must be after period_start")

    run_id = f"readonly_{start.isoformat()}_{end.isoformat()}"
    logger.info("[case_test] readonly start period=%s..%s", start, end)

    q = sb.table("trade_case_definitions").select("*").eq("is_enabled", True).order("case_key")
    if case_keys:
        q = q.in_("case_key", case_keys)
    cases = q.execute().data or []

    candidates = _load_candidates_v2(sb, start, end)
    logger.info("[case_test] readonly candidates=%d cases=%d", len(candidates), len(cases))

    sims_by_case: dict[str, list[dict]] = {}
    results_by_case: dict[str, dict] = {}
    for case in cases:
        case_key = str(case.get("case_key") or case.get("id"))
        sims, result = _simulate_case(run_id, case, candidates)
        sims_by_case[case_key] = sims
        results_by_case[case_key] = result

    return cases, sims_by_case, results_by_case


# ─────────────────────────────────────────────────────────────────────────────

def _insert_batch(sb, table: str, rows: list[dict], batch_size: int = 500) -> None:
    for i in range(0, len(rows), batch_size):
        sb.table(table).insert(rows[i:i + batch_size]).execute()


def run_trade_case_test(
    period_start: str | date,
    period_end: str | date,
    case_keys: list[str] | None = None,
    sb=None,
) -> dict:
    """Run case comparison and persist only trade_case_* rows."""

    sb = sb or _build_supabase()
    start = _to_date(period_start)
    end = _to_date(period_end)
    if end < start:
        raise ValueError("period_end must be after period_start")
    if (end - start).days > 90:
        raise ValueError("comparison period is limited to 90 days")

    logger.info("[case_test] start period=%s..%s", start, end)
    run_row = (
        sb.table("trade_case_runs")
        .insert({
            "run_name": f"{start.isoformat()}..{end.isoformat()}",
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "source": "stock_feature_snapshots",
            "status": "running",
            "memo": "Uses feature snapshots + rebound labels. virtual_trades is untouched.",
        })
        .execute()
        .data[0]
    )
    run_id = run_row["id"]

    try:
        try:
            _upsert_current_settings_case(sb)
            logger.info("[case_test] current_settings case upserted")
        except Exception as _e:
            logger.warning("[case_test] current_settings upsert failed: %s", _e)

        q = sb.table("trade_case_definitions").select("*").eq("is_enabled", True).order("case_key")
        if case_keys:
            q = q.in_("case_key", case_keys)
        cases = q.execute().data or []
        candidates = _load_candidates(sb, start, end)

        all_results: list[dict] = []
        for case in cases:
            sims, result = _simulate_case(run_id, case, candidates)
            if sims:
                _insert_batch(sb, "trade_case_simulations", sims)
            all_results.append(result)
        if all_results:
            _insert_batch(sb, "trade_case_results", all_results)
        sb.table("trade_case_runs").update({
            "status": "completed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", run_id).execute()
        try:
            from services.research_database import snapshot_case_results

            snapshot_case_results(str(run_id), sb=sb)
        except Exception:
            logger.exception("[research_db] snapshot failed run_id=%s", run_id)
        logger.info("[case_test] completed run_id=%s cases=%d", run_id, len(cases))
        return {"run_id": run_id, "cases": len(cases), "candidates": len(candidates)}
    except Exception as e:
        logger.exception("[case_test] failed run_id=%s", run_id)
        sb.table("trade_case_runs").update({
            "status": "failed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "memo": str(e),
        }).eq("id", run_id).execute()
        raise
