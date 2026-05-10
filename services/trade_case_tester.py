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


def _load_candidates(sb, period_start: date, period_end: date) -> list[dict]:
    snap_cols = sorted(set(
        [
            "id", "trade_date", "code", "name", "market", "sector", "close",
            "is_drop_candidate", "is_tradeable", "drop_pct", "rsi14",
            "volume_ratio_20d", "bad_news_score", "market_shock_score",
        ]
        + list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES)
    ))
    label_cols = [
        "id", "feature_snapshot_id", "trade_date", "code", "entry_price",
        "future_high_1d", "future_high_2d", "future_high_3d", "future_high_4d", "future_high_5d",
        "future_low_1d", "future_low_2d", "future_low_3d", "future_low_4d", "future_low_5d",
        "future_close_1d", "future_close_2d", "future_close_3d", "future_close_4d", "future_close_5d",
    ]
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


def _exit_for_candidate(row: dict, rules: dict) -> dict:
    entry = _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)
    if not entry or entry <= 0:
        return {"status": "open", "exit_reason": "invalid_entry"}
    max_days = min(5, max(1, _to_int(rules.get("max_holding_days"), 5)))
    tp_pct = float(rules.get("tp_pct", 0.06))
    sl_pct = float(rules.get("sl_pct", -0.04))
    tp_price = entry * (1 + tp_pct)
    sl_price = entry * (1 + sl_pct)
    entry_date = _to_date(row.get("trade_date"))

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
        "tp_count": len([s for s in closed if s.get("exit_reason") == "tp"]),
        "sl_count": len([s for s in closed if s.get("exit_reason") == "sl"]),
        "timeout_count": len([s for s in closed if s.get("exit_reason") == "timeout"]),
    }


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
