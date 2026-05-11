"""Research DB helpers.

This module only writes research_* tables. It does not modify production
virtual trades, notifications, active models, or trade case source results.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
logger = logging.getLogger(__name__)


ENTRY_PROFILE_LABELS = {
    "current": "現行入口",
    "ai_top10": "AI上位10件",
    "ev_top10": "期待値上位10件",
    "position_limited": "保有数制限",
    "sector_limited": "セクター制限",
    "regime_strict": "地合い厳格化",
}

EXIT_PROFILE_LABELS = {
    "fixed6": "固定6%",
    "fixed7": "固定7%",
    "fixed10": "固定10%",
    "trailing3": "トレーリング3%",
    "trailing5": "トレーリング5%",
    "pullback2": "反落-2%",
    "ma5": "5日MA割れ",
    "rsi70": "RSI70反落",
    "atr15": "ATR 1.5倍",
}

CREDIT_PROFILE_LABELS = {
    "no_margin": "",
    "margin_le20": "信用倍率20倍以下",
    "margin_le10": "信用倍率10倍以下",
    "margin_le5": "信用倍率5倍以下",
    "short_pressure": "売り残比率10%以上",
}


DATASET_SPECS = [
    {
        "dataset_type": "feature_snapshot",
        "dataset_name": "特徴量スナップショット",
        "source_table": "stock_feature_snapshots",
        "date_col": "trade_date",
        "memo": "急落候補、テクニカル、ファンダ、地合い特徴量",
    },
    {
        "dataset_type": "rebound_label_20d",
        "dataset_name": "反発ラベル 20日先",
        "source_table": "stock_rebound_labels",
        "date_col": "trade_date",
        "memo": "1日から20日先の高値・安値・終値を含む反発ラベル",
        "params": {"future_days": 20},
    },
    {
        "dataset_type": "signal_history",
        "dataset_name": "リバウンドシグナル履歴",
        "source_table": "rebound_signal_history",
        "date_col": "signal_date",
        "memo": "実際に検出されたAIシグナル履歴",
    },
    {
        "dataset_type": "weekly_margin",
        "dataset_name": "週次信用残",
        "source_table": "stock_weekly_margin_interest",
        "date_col": "date",
        "memo": "銘柄別の信用買残・信用売残・信用倍率",
        "source": "jquants_standard",
    },
    {
        "dataset_type": "daily_margin",
        "dataset_name": "日々公表信用残",
        "source_table": "stock_daily_margin_interest",
        "date_col": "application_date",
        "memo": "日々公表対象の信用残",
        "source": "jquants_standard",
    },
    {
        "dataset_type": "sector_short_selling",
        "dataset_name": "業種別空売り比率",
        "source_table": "sector_short_selling",
        "date_col": "date",
        "memo": "33業種単位の空売り比率",
        "source": "jquants_standard",
    },
    {
        "dataset_type": "case_result",
        "dataset_name": "比較テスト集計結果",
        "source_table": "trade_case_results",
        "date_col": "created_at",
        "memo": "比較テストのケース別集計結果",
    },
    {
        "dataset_type": "case_simulation",
        "dataset_name": "比較テスト個別売買",
        "source_table": "trade_case_simulations",
        "date_col": "entry_date",
        "memo": "比較テストの個別売買シミュレーション",
    },
]


def _case_display_name(case: dict) -> str:
    rules = case.get("rules") or {}
    if not isinstance(rules, dict):
        rules = {}
    entry = str(rules.get("entry_profile") or "")
    exit_profile = str(rules.get("exit_profile") or "")
    credit = str(rules.get("credit_profile") or "")
    parts = []
    if entry:
        parts.append(ENTRY_PROFILE_LABELS.get(entry, entry))
    if exit_profile:
        parts.append(EXIT_PROFILE_LABELS.get(exit_profile, exit_profile))
    credit_label = CREDIT_PROFILE_LABELS.get(credit, credit)
    if credit_label:
        parts.append(credit_label)
    if parts:
        return " × ".join(parts)
    return str(case.get("case_name") or case.get("case_key") or "")


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _params_hash(params: dict | None) -> str:
    if not params:
        return "default"
    text = json.dumps(_json_safe(params), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def _dataset_key(dataset_type: str, source_table: str | None, period_start: Any, period_end: Any, params: dict | None) -> str:
    start = str(period_start or "none")[:10]
    end = str(period_end or "none")[:10]
    return f"{dataset_type}:{source_table or 'manual'}:{start}:{end}:{_params_hash(params)}"


def _count_and_period(sb, table: str, date_col: str) -> tuple[int, str | None, str | None]:
    count_res = sb.table(table).select(date_col, count="exact").limit(1).execute()
    row_count = int(count_res.count or 0)
    if row_count <= 0:
        return 0, None, None
    start_rows = sb.table(table).select(date_col).order(date_col).limit(1).execute().data or []
    end_rows = sb.table(table).select(date_col).order(date_col, desc=True).limit(1).execute().data or []
    start = start_rows[0].get(date_col) if start_rows else None
    end = end_rows[0].get(date_col) if end_rows else None
    return row_count, str(start)[:10] if start else None, str(end)[:10] if end else None


def _log_import(
    sb,
    *,
    dataset_key: str | None,
    job_type: str,
    status: str,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    error_message: str | None = None,
    params: dict | None = None,
) -> None:
    sb.table("research_import_logs").insert({
        "dataset_key": dataset_key,
        "job_type": job_type,
        "status": status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_skipped": rows_skipped,
        "error_message": error_message,
        "params": _json_safe(params or {}),
    }).execute()


def register_dataset(
    dataset_type: str,
    dataset_name: str,
    source_table: str | None,
    period_start: Any,
    period_end: Any,
    row_count: int,
    *,
    source: str | None = None,
    memo: str | None = None,
    params: dict | None = None,
    sb=None,
) -> dict:
    sb = sb or build_supabase()
    hash_key = _params_hash(params)
    key = _dataset_key(dataset_type, source_table, period_start, period_end, params)
    existing = (
        sb.table("research_datasets")
        .select("id,row_count")
        .eq("dataset_key", key)
        .limit(1)
        .execute()
        .data or []
    )
    payload = {
        "dataset_key": key,
        "dataset_name": dataset_name,
        "dataset_type": dataset_type,
        "source_table": source_table,
        "source": source,
        "period_start": str(period_start)[:10] if period_start else None,
        "period_end": str(period_end)[:10] if period_end else None,
        "row_count": int(row_count or 0),
        "status": "ready",
        "hash_key": hash_key,
        "memo": memo,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    data = sb.table("research_datasets").upsert(payload, on_conflict="dataset_key").execute().data or []
    dataset = data[0] if data else (
        sb.table("research_datasets").select("*").eq("dataset_key", key).limit(1).execute().data or [{}]
    )[0]
    if existing:
        _log_import(
            sb,
            dataset_key=key,
            job_type="register_dataset",
            status="skipped",
            rows_updated=1,
            rows_skipped=int(existing[0].get("row_count") or 0),
            error_message="duplicate dataset_key",
            params=params,
        )
    else:
        _log_import(
            sb,
            dataset_key=key,
            job_type="register_dataset",
            status="ready",
            rows_inserted=1,
            params=params,
        )
    return dataset


def register_existing_datasets(sb=None) -> list[dict]:
    sb = sb or build_supabase()
    registered: list[dict] = []
    for spec in DATASET_SPECS:
        try:
            row_count, start, end = _count_and_period(sb, spec["source_table"], spec["date_col"])
            registered.append(register_dataset(
                spec["dataset_type"],
                spec["dataset_name"],
                spec["source_table"],
                start,
                end,
                row_count,
                source=spec.get("source"),
                memo=spec.get("memo"),
                params=spec.get("params"),
                sb=sb,
            ))
        except Exception as e:
            logger.exception("[research_db] register failed table=%s", spec.get("source_table"))
            _log_import(
                sb,
                dataset_key=None,
                job_type="register_existing",
                status="failed",
                error_message=f"{spec.get('source_table')}: {e}",
                params=spec,
            )
    return registered


def snapshot_case_results(run_id: str, sb=None) -> dict:
    sb = sb or build_supabase()
    run_rows = sb.table("trade_case_runs").select("*").eq("id", run_id).limit(1).execute().data or []
    if not run_rows:
        raise ValueError(f"trade_case_run not found: {run_id}")
    run = run_rows[0]
    results = sb.table("trade_case_results").select("*").eq("run_id", run_id).execute().data or []
    cases = sb.table("trade_case_definitions").select("*").execute().data or []
    case_by_id = {str(c.get("id")): c for c in cases}
    params = {"run_id": str(run_id), "case_count": len(results)}
    dataset = register_dataset(
        "case_result",
        f"比較テスト {run.get('period_start')} - {run.get('period_end')}",
        "trade_case_results",
        run.get("period_start"),
        run.get("period_end"),
        len(results),
        source="trade_case_test",
        memo=run.get("memo"),
        params=params,
        sb=sb,
    )
    dataset_id = dataset.get("id")
    rows = []
    for r in results:
        case = case_by_id.get(str(r.get("case_id")), {})
        rules = case.get("rules") or {}
        metrics = {
            "entry_count": r.get("entry_count"),
            "win_count": r.get("win_count"),
            "loss_count": r.get("loss_count"),
            "open_count": r.get("open_count"),
            "avg_peak_profit_pct": r.get("avg_peak_profit_pct"),
            "avg_trade_drawdown_pct": r.get("avg_trade_drawdown_pct"),
        }
        rows.append({
            "dataset_id": dataset_id,
            "run_id": run_id,
            "case_id": r.get("case_id"),
            "case_key": case.get("case_key") or str(r.get("case_id")),
            "case_name": _case_display_name(case) or str(r.get("case_id")),
            "period_start": run.get("period_start"),
            "period_end": run.get("period_end"),
            "entry_count": r.get("entry_count"),
            "win_rate": r.get("win_rate"),
            "expected_value_pct": r.get("expected_value_pct"),
            "total_profit_pct": r.get("total_profit_pct"),
            "total_profit_yen": r.get("total_profit_yen"),
            "max_drawdown_pct": r.get("max_drawdown_pct"),
            "avg_profit_pct": r.get("avg_profit_pct"),
            "avg_loss_pct": r.get("avg_loss_pct"),
            "avg_holding_days": r.get("avg_holding_days"),
            "max_open_positions": r.get("max_open_positions"),
            "tp_count": r.get("tp_count"),
            "sl_count": r.get("sl_count"),
            "timeout_count": r.get("timeout_count"),
            "rules": _json_safe(rules),
            "metrics": _json_safe(metrics),
        })
    for i in range(0, len(rows), 500):
        sb.table("research_case_snapshots").upsert(
            rows[i:i + 500],
            on_conflict="dataset_id,case_key,period_start,period_end",
        ).execute()
    _log_import(
        sb,
        dataset_key=dataset.get("dataset_key"),
        job_type="snapshot_case_results",
        status="ready",
        rows_inserted=len(rows),
        params=params,
    )
    return {"dataset": dataset, "rows": len(rows)}
