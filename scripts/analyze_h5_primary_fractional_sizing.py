#!/usr/bin/env python3
"""Analyze H5 Primary with fixed-notional and fractional sizing.

Analysis only. This script does not update Primary, trade_case_definitions,
UI, LINE, actual_trade_logs, or any trading table.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
from supabase import create_client

try:
    from services.h5_primary import H5_PRIMARY_CASE_KEY
except Exception:
    H5_PRIMARY_CASE_KEY = "h5_ai65_hd3_est12_cm_range330_live_limited"


NOTIONALS = [100_000, 200_000, 300_000, 500_000, 1_000_000]
FRACTIONAL_NOTIONALS = [100_000, 200_000, 300_000, 500_000, 1_000_000]
RISK_BUDGETS = [3_000, 5_000, 10_000, 20_000]
STOP_PCTS = [4, 8, 12]
MAX_RISK_NOTIONAL = 500_000
DEFAULT_AUDIT_DATASET = ROOT / "outputs/h5_live_selection_audit/04_live_selection_dataset.csv"
DEFAULT_WF_PREDICTIONS = ROOT / "outputs/h5_walk_forward_predictions/01_walk_forward_predictions.csv"
TRAIN_END = date(2024, 12, 31)
TEST_START = date(2025, 1, 1)


def to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "None", "nan"):
            return default
        return float(value)
    except Exception:
        return default


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "pass"}:
        return True
    try:
        return float(text) == 1.0
    except Exception:
        return False


def date_text(value: Any) -> str:
    if not value:
        return ""
    return str(value).split("T", 1)[0][:10]


def parse_date(value: Any) -> date | None:
    text = date_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def get_optional_env(name: str) -> str:
    return os.getenv(name, "").strip()


def get_mode_env(base: str, mode: str) -> str:
    mode = (mode or "").upper()
    names = ([f"{base}_{mode}"] if mode else []) + [base]
    for name in names:
        value = get_optional_env(name)
        if value:
            return value
    return ""


def build_supabase():
    load_dotenv()
    mode = get_optional_env("SUPABASE_MODE") or get_optional_env("ENV")
    url = get_mode_env("SUPABASE_URL", mode)
    key = get_mode_env("SUPABASE_KEY", mode)
    if not url or not key:
        raise RuntimeError("Supabase credentials are missing")
    return create_client(url, key)


def fetch_all(query, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        data = query.range(start, start + page_size - 1).execute().data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size
    return rows


def load_virtual_trades_from_db(start_date: str = "", end_date: str = "") -> list[dict]:
    sb = build_supabase()
    q = sb.table("virtual_trades").select("*").order("buy_date", desc=False)
    if start_date:
        q = q.gte("buy_date", start_date)
    if end_date:
        q = q.lte("buy_date", end_date)
    rows = fetch_all(q)
    for row in rows:
        row["_input_source"] = "db_virtual_trades"
    return rows


def period_for_entry(entry_date: str) -> str:
    dt = parse_date(entry_date)
    if not dt:
        return "unknown"
    if dt <= TRAIN_END:
        return "train"
    if dt >= TEST_START:
        return "test"
    return "unknown"


def audit_row_to_trade(row: dict, *, group: str) -> dict | None:
    """Convert historical H5 live-selection audit rows into HD3+EST12 trade rows.

    The audit dataset is the broad historical H5 research population used by
    prior H5 analyses. It already contains entry_price, hd3_ret_est12,
    hd3_exit_reason, and live-selection flags, so this avoids using the
    one-day stored-forward log as the sizing population.
    """
    entry_date = date_text(row.get("entry_date"))
    entry_price = to_float(row.get("entry_price"))
    ret = to_float(row.get("hd3_ret_est12"))
    if ret is None:
        ret = to_float(row.get("hd3_ret_raw"))
    if not entry_date or not entry_price or ret is None:
        return None
    exit_date = business_exit(parse_date(entry_date) or date.today(), 3).isoformat()
    exit_price = entry_price * (1 + ret / 100)
    selected = to_bool(row.get("selected_by_live_limited"))
    out = dict(row)
    out.update({
        "_input_source": "outputs/h5_live_selection_audit/04_live_selection_dataset.csv",
        "analysis_population": group,
        "code": str(row.get("code") or "").replace(".0", "").replace(".T", ""),
        "name": row.get("name"),
        "entry_date": entry_date,
        "trade_date": entry_date,
        "exit_date": exit_date,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "return_pct": ret,
        "pnl_100_share": (exit_price - entry_price) * 100,
        "case_key": H5_PRIMARY_CASE_KEY,
        "h5_case_key": H5_PRIMARY_CASE_KEY,
        "h5_primary_match": True,
        "H5_full": True,
        "is_primary_h5": True,
        "score_source": row.get("score_source") or "historical_audit_saved_scores",
        "prediction_source": row.get("prediction_source") or "historical_audit_saved_scores",
        "signal_probability": to_float(row.get("signal_probability")),
        "fallback_used": False,
        "exit_reason": row.get("hd3_exit_reason") or ("emergency_stop" if ret <= -12 else "time_stop"),
        "period": period_for_entry(entry_date),
        "is_live_limited": selected,
        "selected_by_live_limited": selected,
        "live_candidate_rank": to_float(row.get("live_candidate_rank")),
    })
    return out


def load_historical_audit_rows(path: Path = DEFAULT_AUDIT_DATASET) -> list[dict]:
    rows = read_csv(path)
    out: list[dict] = []
    for row in rows:
        trade = audit_row_to_trade(row, group="Research_Audit")
        if trade:
            out.append(trade)
    return out


def load_historical_loader_rows(args) -> tuple[list[dict], str]:
    """Load DB-backed historical H5_full rows using the same loader as prior analyses.

    This is optional because it may call the active-model scoring path unless
    score_source is explicitly changed. The output is labeled separately so it
    is not confused with point-in-time stored predictions.
    """
    try:
        from analyze_h5_breakeven_escape import (
            _build_dataset,
            _close_ret,
            _first_est12_day,
            _load_candidates_v2,
            _to_float as h5_to_float,
            EST12_STOP_MULT,
            EST12_STOP_PCT,
        )
        from services.trade_case_tester import _build_supabase
    except Exception as exc:
        return [], f"historical_loader import failed: {exc}"

    try:
        sb = _build_supabase()
        start = parse_date(args.start_date) or date(2023, 1, 1)
        end = parse_date(args.end_date) or date.today()
        candidates = _load_candidates_v2(
            sb,
            start,
            end,
            score_source=args.loader_score_source,
            model_key=args.model_key or "rebound_lgbm_5d",
            model_version=args.model_version or None,
            allow_score_fallback=False,
        )
        dataset, skipped = _build_dataset(candidates, TRAIN_END, TEST_START)
        out: list[dict] = []
        for row in dataset:
            if row.get("group") != "H5_full":
                continue
            entry = h5_to_float(row.get("entry_price"), None)
            path = row.get("_path") or []
            if not entry or not path:
                continue
            est_day = _first_est12_day(path, entry, 1, 3)
            if est_day is not None:
                ret = EST12_STOP_PCT
                exit_price = entry * EST12_STOP_MULT
                exit_reason = "emergency_stop"
            else:
                ret = _close_ret(path, entry, 3)
                if ret is None:
                    continue
                exit_price = entry * (1 + ret / 100)
                exit_reason = "time_stop"
            entry_date = str(row.get("trade_date") or "")
            trade = dict(row)
            trade.update({
                "_path": None,
                "_input_source": f"historical_loader_{args.loader_score_source}",
                "analysis_population": f"Historical_Loader_H5_full_{args.loader_score_source}",
                "entry_date": entry_date,
                "trade_date": entry_date,
                "exit_date": business_exit(parse_date(entry_date) or date.today(), 3).isoformat(),
                "entry_price": entry,
                "exit_price": exit_price,
                "return_pct": ret,
                "pnl_100_share": (exit_price - entry) * 100,
                "case_key": H5_PRIMARY_CASE_KEY,
                "h5_case_key": H5_PRIMARY_CASE_KEY,
                "h5_primary_match": True,
                "H5_full": True,
                "is_primary_h5": True,
                "score_source": args.loader_score_source,
                "prediction_source": args.loader_score_source,
                "fallback_used": False,
                "exit_reason": exit_reason,
                "period": row.get("period") or period_for_entry(entry_date),
            })
            out.append(trade)
        return out, f"historical_loader_{args.loader_score_source}: candidates={len(candidates)} rows={len(out)} skipped={len(skipped)}"
    except Exception as exc:
        return [], f"historical_loader failed: {exc}"


def current_candidate_paths(args) -> list[Path]:
    paths: list[Path] = []
    if args.candidate_log:
        paths.append(ROOT / args.candidate_log)
    defaults = [
        ROOT / "outputs/h5_stored_forward_test/forward_test_candidate_log.csv",
        ROOT / "outputs/h5_stored_forward_test/latest_h5_full_candidates.csv",
        ROOT / "outputs/h5_stored_forward_test/latest_h5_candidates.csv",
    ]
    for path in defaults:
        if path.exists() and path not in paths:
            paths.append(path)
    return paths


def walk_forward_candidate_paths(args) -> list[Path]:
    paths: list[Path] = []
    if args.candidate_log:
        paths.append(ROOT / args.candidate_log)
    candidates = [DEFAULT_WF_PREDICTIONS]
    wf_dir = ROOT / "outputs/h5_walk_forward_predictions"
    if wf_dir.exists():
        candidates.extend(sorted(wf_dir.glob("*h5_full*.csv")))
        candidates.extend(sorted(wf_dir.glob("*stored*.csv")))
    candidates.extend([
        ROOT / "outputs/h5_stored_forward_test/forward_test_candidate_log.csv",
        ROOT / "outputs/h5_stored_forward_test/latest_h5_full_candidates.csv",
    ])
    for path in candidates:
        if path.exists() and path not in paths:
            paths.append(path)
    return paths


def wf_overheat_score(row: dict) -> float:
    value = to_float(first(row, "overheat_score", "h5_overheat_score", "index_overheat_score"), None)
    return value if value is not None else 0.0


def is_wf_h5_full_candidate(row: dict) -> tuple[bool, str]:
    source_path = str(row.get("_input_source") or "")
    text = " ".join(str(row.get(k) or "") for k in (
        "case_key", "h5_case_key", "strategy", "strategy_group", "bucket",
        "candidate_type", "population", "label",
    ))
    explicit_h5 = (
        "h5_full" in source_path.lower()
        or "walk_forward_h5_full" in text
        or "H5_full" in text
        or to_bool(first(row, "H5_full", "h5_full", "h5_primary_match", "is_h5_primary"))
    )
    score_source = str(first(row, "score_source", "prediction_source", "source", "ai_score_source", "model_prediction_source") or "")
    if "active_model" in score_source or "active_rescore" in score_source or "manual_score" in score_source:
        return False, "active_or_manual_score_source"
    if to_bool(first(row, "fallback_used", "score_fallback_used", "allow_score_fallback")):
        return False, "fallback_used"
    if explicit_h5:
        return True, ""
    if score_source and score_source not in {"walk_forward", "stored_predictions", "stored_forward", "walk_forward_stored", "model_predictions", "daily_prediction"}:
        return False, f"non_walk_forward_score_source:{score_source}"
    if not score_source and "h5_walk_forward_predictions" not in source_path:
        return False, "score_source_missing_non_wf_file"
    prob = to_float(row.get("signal_probability"), None)
    drop = to_float(row.get("drop_from_20d_high_pct"), None)
    if prob is None or prob < 0.65:
        return False, "ai_lt_065"
    if drop is None or drop > -8.0:
        return False, "drop_not_lte_m8"
    if str(row.get("signal_stage") or "") not in {"confirmed", "strong_confirmed"}:
        return False, "stage_not_confirmed"
    if str(row.get("market_regime") or "") == "panic_selloff":
        return False, "panic_selloff"
    if wf_overheat_score(row) > 1:
        return False, "overheat_gt_1"
    margin = to_float(row.get("margin_ratio"), None)
    if margin is not None and not (3.0 <= margin <= 30.0):
        return False, "margin_out_of_range"
    return True, ""


def wf_hd3_exit(row: dict) -> tuple[float | None, float | None, str]:
    entry = to_float(first(row, "entry_price", "close", "signal_price"), None)
    if not entry or entry <= 0:
        return None, None, "missing_entry_price"
    stop = entry * 0.88
    for day in range(1, 4):
        low = to_float(row.get(f"future_low_{day}d"), None)
        if low is not None and low <= stop:
            return -12.0, stop, "emergency_stop"
    close3 = to_float(row.get("future_close_3d"), None)
    if close3 is None:
        return None, None, "missing_future_close_3d"
    return (close3 / entry - 1) * 100, close3, "time_stop"


def load_walk_forward_h5_full_rows(args) -> tuple[list[dict], Counter, list[str]]:
    skipped: Counter = Counter()
    sources: list[str] = []
    by_key: dict[tuple[str, str, str], dict] = {}
    for path in walk_forward_candidate_paths(args):
        rows = read_csv(path)
        sources.append(f"{path}: {len(rows)} rows")
        for row in rows:
            row["_input_source"] = str(path.relative_to(ROOT))
            ok, reason = is_wf_h5_full_candidate(row)
            if not ok:
                skipped[reason] += 1
                continue
            trade_date = date_text(first(row, "trade_date", "entry_date", "signal_date"))
            code = str(first(row, "code", "symbol") or "").replace(".0", "").replace(".T", "")
            if not trade_date or not code:
                skipped["missing_trade_date_or_code"] += 1
                continue
            ret, exit_price, reason = wf_hd3_exit(row)
            if ret is None:
                skipped[reason] += 1
                continue
            entry = to_float(first(row, "entry_price", "close", "signal_price"), None)
            model_version = str(row.get("model_version") or "")
            out = dict(row)
            out.update({
                "code": code,
                "entry_date": trade_date,
                "trade_date": trade_date,
                "exit_date": business_exit(parse_date(trade_date) or date.today(), 3).isoformat(),
                "entry_price": entry,
                "signal_price": to_float(first(row, "signal_price", "close", "entry_price"), entry),
                "exit_price": exit_price,
                "return_pct": ret,
                "pnl_100_share": (exit_price - entry) * 100 if exit_price is not None and entry is not None else None,
                "unit_amount": entry * 100 if entry else None,
                "case_key": "H5_full",
                "h5_case_key": "H5_full",
                "population": "walk_forward_h5_full",
                "score_source": first(row, "score_source", "source") or "walk_forward",
                "prediction_source": first(row, "prediction_source", "source") or "walk_forward",
                "fallback_used": False,
                "active_model_called": False,
                "H5_full": True,
                "h5_primary_match": True,
                "exit_reason": reason,
                "period": period_for_entry(trade_date),
                "entry_gap_pct": to_float(row.get("entry_gap_pct"), None),
            })
            key = (trade_date, code, model_version)
            by_key[key] = out
    return list(by_key.values()), skipped, sources


def is_active_model_called(row: dict) -> bool:
    return to_bool(first(row, "active_model_called", "active_model_used", "rescored_by_active_model"))


def is_current_h5_full_row(row: dict) -> tuple[bool, str]:
    source_path = str(row.get("_input_source") or "")
    text = " ".join(str(row.get(k) or "") for k in (
        "case_key", "h5_case_key", "strategy", "strategy_group", "bucket",
        "signal_type", "candidate_type", "label",
    ))
    if "latest_h5_full_candidates.csv" in source_path:
        h5_full = True
    else:
        h5_full = (
            to_bool(first(row, "H5_full", "h5_full", "H5_FULL", "h5_primary_match", "is_h5_primary"))
            or any(token == text.strip() for token in ("H5_full", "h5_full"))
            or str(row.get("strategy_group") or "") == "H5_full"
            or str(row.get("case_key") or "") == "H5_full"
            or str(row.get("h5_case_key") or "") == "H5_full"
            or str(row.get("candidate_type") or "") == "H5_full"
        )
    if not h5_full:
        return False, "not_h5_full"
    if any(bad in text for bad in ("AI_plus_drop", "K_no_normal", "Watch", "Intraday", "Research_ALL", "Live Limited")) and "H5_full" not in text:
        return False, "excluded_strategy_group"
    score_source = str(first(row, "score_source", "prediction_source", "ai_score_source", "source") or "")
    if "active_model" in score_source:
        return False, "active_model_score_source"
    if score_source and score_source not in {"stored_predictions", "daily_prediction"}:
        return False, f"non_stored_score_source:{score_source}"
    if to_bool(first(row, "fallback_used", "fallback_used_count", "allow_score_fallback", "score_fallback_used")):
        return False, "fallback_used"
    if is_active_model_called(row):
        return False, "active_model_called"
    return True, ""


def load_current_primary_raw(args) -> tuple[list[dict], Counter, list[str]]:
    skipped: Counter = Counter()
    sources: list[str] = []
    rows_by_key: dict[tuple[str, str, str], dict] = {}
    for path in current_candidate_paths(args):
        rows = read_csv(path)
        sources.append(f"{path}: {len(rows)} rows")
        for row in rows:
            row["_input_source"] = str(path.relative_to(ROOT))
            ok, reason = is_current_h5_full_row(row)
            if not ok:
                skipped[reason] += 1
                continue
            trade_date = date_text(first(row, "trade_date", "entry_date"))
            code = str(first(row, "code", "symbol") or "").replace(".T", "")
            model_version = str(row.get("model_version") or "")
            if not trade_date or not code:
                skipped["missing_trade_date_or_code"] += 1
                continue
            # Prefer detailed latest_h5_full rows over cumulative candidate-log rows.
            priority = 2 if "latest_h5_full_candidates.csv" in str(path) else 1
            row["_current_source_priority"] = priority
            key = (trade_date, code, model_version)
            old = rows_by_key.get(key)
            if old is None or priority >= int(old.get("_current_source_priority") or 0):
                rows_by_key[key] = row
    out = list(rows_by_key.values())
    return out, skipped, sources


def enrich_current_prices_from_db(rows: list[dict], args, skipped: Counter) -> str:
    if not rows or not to_bool(args.enrich_prices_from_db):
        return "price_enrichment skipped"
    missing = [r for r in rows if to_float(first(r, "entry_price", "close", "signal_price")) is None]
    if not missing:
        return "price_enrichment not needed"
    try:
        sb = build_supabase()
        dates = sorted({date_text(r.get("trade_date")) for r in missing if date_text(r.get("trade_date"))})
        codes = sorted({str(r.get("code") or "") for r in missing if r.get("code")})
        if not dates or not codes:
            return "price_enrichment skipped: missing date/code"
        date_min, date_max = dates[0], dates[-1]
        price_by_key: dict[tuple[str, str], dict] = {}
        for i in range(0, len(codes), 100):
            batch = codes[i:i + 100]
            query = (
                sb.table("stock_feature_snapshots")
                .select("id,trade_date,code,close")
                .gte("trade_date", date_min)
                .lte("trade_date", date_max)
                .in_("code", batch)
            )
            for row in fetch_all(query):
                price_by_key[(date_text(row.get("trade_date")), str(row.get("code") or ""))] = row
        filled = 0
        for row in missing:
            key = (date_text(row.get("trade_date")), str(row.get("code") or ""))
            price_row = price_by_key.get(key)
            close = to_float(price_row.get("close") if price_row else None)
            if close:
                row["entry_price"] = close
                row["signal_price"] = close
                row["entry_price_source"] = "stock_feature_snapshots.close"
                row["feature_snapshot_id"] = price_row.get("id")
                filled += 1
            else:
                skipped["missing_entry_price_after_db_enrichment"] += 1
        return f"price_enrichment filled={filled} missing={len(missing) - filled}"
    except Exception as exc:
        skipped["price_enrichment_failed"] += len(missing)
        return f"price_enrichment failed: {exc}"


def first(row: dict, *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def standardize(row: dict) -> dict:
    entry_date = date_text(first(row, "entry_date", "buy_date", "actual_entry_date", "virtual_entry_date", "trade_date"))
    exit_date = date_text(first(row, "exit_date", "sell_date", "actual_exit_date", "virtual_exit_date"))
    entry_price = to_float(first(row, "entry_price", "buy_price", "actual_entry_price", "virtual_entry_price", "signal_price", "close"))
    exit_price = to_float(first(row, "exit_price", "sell_price", "actual_exit_price", "virtual_exit_price"))
    current_price = to_float(first(row, "current_price", "current_price_yf", "close"))
    return_pct = to_float(first(row, "return_pct", "pnl_pct", "profit_pct", "actual_return_pct"))
    pnl_100 = to_float(first(row, "pnl_100_share", "pnl", "profit_loss", "actual_pnl", "realized_pnl"))
    if return_pct is None and entry_price and exit_price:
        return_pct = (exit_price / entry_price - 1) * 100
    if pnl_100 is None and entry_price and exit_price:
        pnl_100 = (exit_price - entry_price) * 100
    if exit_price is None and return_pct is not None and entry_price:
        exit_price = entry_price * (1 + return_pct / 100)
    if return_pct is None and pnl_100 is not None and entry_price:
        return_pct = pnl_100 / (entry_price * 100) * 100
    status = str(first(row, "status", "result_status") or "").lower()
    is_open = to_bool(first(row, "is_open")) or status in {"open", "pending", "holding"}

    out = dict(row)
    out.update({
        "code": str(first(row, "code", "symbol") or "").replace(".T", ""),
        "name": first(row, "name", "company_name"),
        "entry_date": entry_date,
        "exit_date": exit_date,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "current_price": current_price,
        "return_pct": return_pct,
        "pnl_100_share": pnl_100,
        "quantity": to_float(first(row, "quantity", "shares")),
        "unit_amount": entry_price * 100 if entry_price else None,
        "case_key": first(row, "case_key", "strategy", "strategy_group", "bucket"),
        "h5_case_key": first(row, "h5_case_key"),
        "score_source": first(row, "score_source"),
        "prediction_source": first(row, "prediction_source", "source"),
        "model_key": first(row, "model_key"),
        "model_version": first(row, "model_version"),
        "signal_probability": to_float(first(row, "signal_probability", "ai_score", "entry_probability")),
        "fallback_used": to_bool(first(row, "fallback_used", "score_fallback_used")),
        "exit_reason": first(row, "exit_reason", "planned_exit_model"),
        "status": status,
        "is_open": is_open,
        "entry_gap_pct": to_float(first(row, "entry_gap_pct")),
        "signal_price": to_float(first(row, "signal_price")),
        "trade_date": date_text(first(row, "trade_date", "buy_date", "entry_date")),
        "_input_source": row.get("_input_source") or "csv",
    })
    return out


def h5_filter_reason(row: dict) -> str:
    text = " ".join(str(row.get(k) or "") for k in ("case_key", "h5_case_key", "strategy_group", "strategy", "bucket", "label"))
    score_source = str(row.get("score_source") or row.get("prediction_source") or "")
    if row.get("fallback_used"):
        return "fallback_used"
    if score_source and score_source not in {"stored_predictions", "daily_prediction", "walk_forward"}:
        if "active_model" in score_source or "proxy" in score_source:
            return "score_source_not_point_in_time"
    if row.get("h5_primary_match") is not None and not to_bool(row.get("h5_primary_match")):
        return "h5_primary_match_false"
    if row.get("H5_full") is not None and not to_bool(row.get("H5_full")):
        return "H5_full_false"
    if any(term in text for term in ("AI_plus_drop", "Watch", "Intraday", "PULLBACK", "RSI75", "MA5_FAILED", "HOLDING_TIMEOUT")) and "H5_full" not in text and H5_PRIMARY_CASE_KEY not in text:
        return "non_primary_strategy"
    if (
        H5_PRIMARY_CASE_KEY in text
        or "H5_full" in text
        or row.get("is_primary_h5") is True
        or to_bool(row.get("is_primary_h5"))
        or to_bool(row.get("h5_primary_match"))
    ):
        return ""
    return "not_h5_primary"


def has_evaluable_return(row: dict, include_open: bool) -> bool:
    if row.get("return_pct") is not None and row.get("entry_price") is not None:
        return True
    if include_open and row.get("is_open") and row.get("entry_price") and row.get("current_price"):
        row["exit_price"] = row["current_price"]
        row["return_pct"] = (row["current_price"] / row["entry_price"] - 1) * 100
        row["pnl_100_share"] = (row["current_price"] - row["entry_price"]) * 100
        return True
    return False


def load_inputs(args) -> tuple[list[dict], list[str]]:
    sources: list[str] = []
    raw: list[dict] = []
    paths: list[Path] = []
    historical_source = str(args.historical_source or "audit").lower()
    if historical_source in {"audit", "both"}:
        if DEFAULT_AUDIT_DATASET.exists():
            audit_rows = load_historical_audit_rows(DEFAULT_AUDIT_DATASET)
            raw.extend(audit_rows)
            sources.append(f"{DEFAULT_AUDIT_DATASET}: {len(audit_rows)} historical audit trade rows")
        else:
            sources.append(f"{DEFAULT_AUDIT_DATASET}: missing")
    if historical_source in {"loader", "both"}:
        loader_rows, message = load_historical_loader_rows(args)
        raw.extend(loader_rows)
        sources.append(message)
    for attr in ("input", "trade_log", "candidate_log"):
        value = getattr(args, attr, None)
        if value:
            paths.append(ROOT / value)
    if not paths and historical_source == "none":
        candidates = [
            ROOT / "outputs/h5_stored_forward_test/forward_test_candidate_log.csv",
            ROOT / "outputs/h5_stored_forward_test/latest_h5_full_candidates.csv",
        ]
        paths.extend([p for p in candidates if p.exists()])
    for path in paths:
        rows = read_csv(path)
        for row in rows:
            row["_input_source"] = str(path.relative_to(ROOT))
        raw.extend(rows)
        sources.append(f"{path}: {len(rows)} rows")
    if to_bool(args.load_db):
        try:
            db_rows = load_virtual_trades_from_db(args.start_date, args.end_date)
            raw.extend(db_rows)
            sources.append(f"db.virtual_trades: {len(db_rows)} rows")
        except Exception as exc:
            sources.append(f"db.virtual_trades failed: {exc}")
    return raw, sources


def profit_factor(returns: Iterable[float]) -> float | None:
    vals = [v for v in returns if v is not None]
    gross_profit = sum(v for v in vals if v > 0)
    gross_loss = -sum(v for v in vals if v < 0)
    if gross_loss == 0:
        return None if gross_profit == 0 else float("inf")
    return gross_profit / gross_loss


def max_streak(signs: list[bool], target: bool) -> int:
    best = cur = 0
    for sign in signs:
        if sign == target:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def equity_drawdown(pnls: list[tuple[str, float]]) -> dict:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    dd_start = ""
    dd_end = ""
    peak_date = ""
    for dt, pnl in sorted(pnls, key=lambda x: x[0]):
        equity += pnl
        if equity > peak:
            peak = equity
            peak_date = dt
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            dd_start = peak_date
            dd_end = dt
    return {"max_dd_yen": max_dd, "max_dd_start": dd_start, "max_dd_end": dd_end}


def summarize_returns(rows: list[dict], pnl_key: str, return_key: str = "return_pct") -> dict:
    returns = [to_float(r.get(return_key), 0) or 0 for r in rows]
    pnls = [to_float(r.get(pnl_key), 0) or 0 for r in rows]
    wins = [v > 0 for v in returns]
    dd = equity_drawdown([(r.get("exit_date") or r.get("entry_date") or "", p) for r, p in zip(rows, pnls)])
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)
    pf = None if gross_loss == 0 else gross_profit / gross_loss
    return {
        "trades": len(rows),
        "wins": sum(wins),
        "losses": len(rows) - sum(wins),
        "win_rate": sum(wins) / len(rows) * 100 if rows else None,
        "avg_return_pct": sum(returns) / len(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "total_return_pct_sum": sum(returns),
        "total_pnl": sum(pnls),
        "avg_pnl_per_trade": sum(pnls) / len(pnls) if pnls else None,
        "profit_factor": pf,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "max_win": max(pnls) if pnls else None,
        "max_loss": min(pnls) if pnls else None,
        "max_dd": dd["max_dd_yen"],
        "max_consecutive_losses": max_streak(wins, False),
        "max_consecutive_wins": max_streak(wins, True),
    }


def add_sizing_columns(rows: list[dict]) -> None:
    for r in rows:
        entry = to_float(r.get("entry_price"))
        exitp = to_float(r.get("exit_price"))
        ret = to_float(r.get("return_pct"))
        diff = (exitp - entry) if entry is not None and exitp is not None else None
        r["one_share_notional"] = entry if entry else None
        r["one_share_pnl"] = diff
        for n in NOTIONALS:
            r[f"fixed_notional_pnl_{n//1000}k"] = n * (ret or 0) / 100 if ret is not None else None
        for n in FRACTIONAL_NOTIONALS:
            if entry:
                shares = max(1, math.floor(n / entry))
                r[f"fractional_qty_{n//1000}k"] = shares
                r[f"fractional_notional_{n//1000}k"] = shares * entry
                r[f"fractional_pnl_{n//1000}k"] = shares * diff if diff is not None else None
            else:
                r[f"fractional_qty_{n//1000}k"] = None
                r[f"fractional_notional_{n//1000}k"] = None
                r[f"fractional_pnl_{n//1000}k"] = None


def business_exit(entry: date, holding_days: int) -> date:
    cur = entry
    remaining = holding_days
    while remaining > 0:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            remaining -= 1
    return cur


def active_dates(row: dict, holding_days: int) -> list[str]:
    start = parse_date(row.get("entry_date"))
    if not start:
        return []
    end = parse_date(row.get("exit_date")) or business_exit(start, holding_days)
    dates = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            dates.append(cur.isoformat())
        cur += timedelta(days=1)
    return dates


def bucket_unit(unit: float | None) -> str:
    if unit is None:
        return "unknown"
    bands = [
        (0, 50_000, "0_50k"),
        (50_000, 100_000, "50_100k"),
        (100_000, 300_000, "100_300k"),
        (300_000, 500_000, "300_500k"),
        (500_000, 1_000_000, "500_1000k"),
        (1_000_000, 3_000_000, "1000_3000k"),
    ]
    for lo, hi, label in bands:
        if lo <= unit < hi:
            return label
    return "gt_3000k"


def bucket_gap(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= -3:
        return "lte_-3"
    if value <= -1:
        return "-3_-1"
    if value <= 0:
        return "-1_0"
    if value <= 1:
        return "0_1"
    if value <= 2:
        return "1_2"
    if value <= 3:
        return "2_3"
    return "gt_3"


def bucket_score(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value > 1:
        value /= 100
    if value < 0.60:
        return "lt_0.60"
    if value < 0.65:
        return "0.60_0.65"
    if value < 0.70:
        return "0.65_0.70"
    if value < 0.75:
        return "0.70_0.75"
    return "gte_0.75"


def grouped_summary(rows: list[dict], key_fn, pnl_key: str = "fixed_notional_pnl_300k") -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[str(key_fn(r))].append(r)
    out = []
    for key, items in sorted(groups.items()):
        s = summarize_returns(items, pnl_key)
        out.append({"bucket": key, **s})
    return out


def week_start(dt: date) -> date:
    return dt - timedelta(days=dt.weekday())


def rows_with_return(rows: list[dict]) -> list[dict]:
    return [r for r in rows if to_float(r.get("return_pct")) is not None and to_float(r.get("entry_price")) is not None]


def summarize_returns_available(rows: list[dict], pnl_key: str) -> dict:
    items = rows_with_return(rows)
    out = summarize_returns(items, pnl_key)
    out["candidate_rows"] = len(rows)
    out["priced_rows"] = sum(1 for r in rows if to_float(r.get("entry_price")) is not None)
    out["return_evaluable_rows"] = len(items)
    out["pending_or_missing_return_rows"] = len(rows) - len(items)
    return out


def overlap_rows_for(rows: list[dict], holding_days: int) -> list[dict]:
    overlap: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        for dt in active_dates(r, holding_days):
            overlap[dt].append(r)
    out = []
    for dt, items in sorted(overlap.items()):
        low = [i for i in items if (to_float(i.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        high = [i for i in items if (to_float(i.get("unit_amount"), 0) or 0) > 300_000]
        out.append({
            "date": dt,
            "open_count": len(items),
            "open_low_unit_300k_count": len(low),
            "open_high_unit_300k_count": len(high),
            "required_100k": len(items) * 100_000,
            "required_200k": len(items) * 200_000,
            "required_300k": len(items) * 300_000,
            "required_500k": len(items) * 500_000,
            "required_1000k": len(items) * 1_000_000,
            "required_s_share_100k": sum(to_float(i.get("fractional_notional_100k"), 0) or 0 for i in items),
            "required_s_share_200k": sum(to_float(i.get("fractional_notional_200k"), 0) or 0 for i in items),
            "required_s_share_300k": sum(to_float(i.get("fractional_notional_300k"), 0) or 0 for i in items),
            "required_s_share_500k": sum(to_float(i.get("fractional_notional_500k"), 0) or 0 for i in items),
            "required_s_share_1000k": sum(to_float(i.get("fractional_notional_1000k"), 0) or 0 for i in items),
            "required_100share": sum(to_float(i.get("unit_amount"), 0) or 0 for i in items),
            "required_low_unit_100share": sum(to_float(i.get("unit_amount"), 0) or 0 for i in low),
            "required_overlay_300k_threshold_s20": sum((to_float(i.get("unit_amount"), 0) or 0) if (to_float(i.get("unit_amount"), 0) or 0) <= 300_000 else 200_000 for i in items),
            "required_overlay_300k_threshold_s30": sum((to_float(i.get("unit_amount"), 0) or 0) if (to_float(i.get("unit_amount"), 0) or 0) <= 300_000 else 300_000 for i in items),
            "required_overlay_300k_threshold_s50": sum((to_float(i.get("unit_amount"), 0) or 0) if (to_float(i.get("unit_amount"), 0) or 0) <= 300_000 else 500_000 for i in items),
        })
    return out


def capital_summary_from_overlap(overlap_rows: list[dict]) -> list[dict]:
    scenarios = {
        "fixed_100k": "required_100k",
        "fixed_200k": "required_200k",
        "fixed_300k": "required_300k",
        "fixed_500k": "required_500k",
        "fixed_1000k": "required_1000k",
        "s_share_100k": "required_s_share_100k",
        "s_share_200k": "required_s_share_200k",
        "s_share_300k": "required_s_share_300k",
        "s_share_500k": "required_s_share_500k",
        "s_share_1000k": "required_s_share_1000k",
        "unit_100share": "required_100share",
        "low_unit_300k_only_100share": "required_low_unit_100share",
        "overlay_300k_threshold_s20": "required_overlay_300k_threshold_s20",
        "overlay_300k_threshold_s30": "required_overlay_300k_threshold_s30",
        "overlay_300k_threshold_s50": "required_overlay_300k_threshold_s50",
    }
    counts = [int(r.get("open_count") or 0) for r in overlap_rows]
    sorted_counts = sorted(counts)
    def pctile(vals: list[float], p: float) -> float:
        if not vals:
            return 0
        idx = max(0, min(len(vals) - 1, math.ceil(len(vals) * p) - 1))
        return vals[idx]
    out = []
    for scenario, key in scenarios.items():
        capitals = sorted([to_float(r.get(key), 0) or 0 for r in overlap_rows])
        out.append({
            "scenario": scenario,
            "avg_open_count": sum(counts) / len(counts) if counts else 0,
            "median_open_count": median(counts) if counts else 0,
            "max_open_count": max(counts) if counts else 0,
            "p90_open_count": pctile(sorted_counts, 0.90),
            "p95_open_count": pctile(sorted_counts, 0.95),
            "required_capital_avg": sum(capitals) / len(capitals) if capitals else 0,
            "required_capital_p95": pctile(capitals, 0.95),
            "required_capital_max": max(capitals) if capitals else 0,
            "required_capital_1_5x": pctile(capitals, 0.95) * 1.5,
            "required_capital_2_0x": pctile(capitals, 0.95) * 2.0,
        })
    return out


def write_current_primary_outputs(args) -> None:
    outdir = ROOT / args.output_dir / "current_primary"
    raw, current_skipped, sources = load_current_primary_raw(args)
    price_message = enrich_current_prices_from_db(raw, args, current_skipped)
    standardized = [standardize(r) for r in raw]
    for r in standardized:
        r["population"] = "current_h5_primary"
        r["case_key"] = r.get("case_key") or "H5_full"
        r["h5_case_key"] = r.get("h5_case_key") or "H5_full"
        r["h5_primary_match"] = True
        r["H5_full"] = True
    if args.start_date:
        standardized = [r for r in standardized if (r.get("entry_date") or r.get("trade_date") or "") >= args.start_date]
    if args.end_date:
        standardized = [r for r in standardized if (r.get("entry_date") or r.get("trade_date") or "") <= args.end_date]
    add_sizing_columns(standardized)

    daily_groups: dict[str, list[dict]] = defaultdict(list)
    for r in standardized:
        dt = r.get("entry_date") or r.get("trade_date")
        if dt:
            daily_groups[dt].append(r)
    daily_rows = []
    for dt, items in sorted(daily_groups.items()):
        low = [i for i in items if (to_float(i.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        high = [i for i in items if (to_float(i.get("unit_amount"), 0) or 0) > 300_000]
        daily_rows.append({
            "date": dt,
            "current_h5_primary_count": len(items),
            "low_unit_300k_count": len(low),
            "high_unit_300k_count": len(high),
            "total_unit_amount_100share": sum(to_float(i.get("unit_amount"), 0) or 0 for i in items),
            "required_200k_fixed": len(items) * 200_000,
            "required_300k_fixed": len(items) * 300_000,
            "required_500k_fixed": len(items) * 500_000,
        })
    write_csv(outdir / "current_primary_02_daily_counts.csv", daily_rows)

    week_groups: dict[str, list[dict]] = defaultdict(list)
    for dt, items in daily_groups.items():
        parsed = parse_date(dt)
        if parsed:
            week_groups[week_start(parsed).isoformat()].extend(items)
    weekly_rows = []
    for ws, items in sorted(week_groups.items()):
        start = parse_date(ws) or date.today()
        days = Counter((i.get("entry_date") or i.get("trade_date")) for i in items)
        low = [i for i in items if (to_float(i.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        high = [i for i in items if (to_float(i.get("unit_amount"), 0) or 0) > 300_000]
        weekly_rows.append({
            "week_start": ws,
            "week_end": (start + timedelta(days=6)).isoformat(),
            "current_h5_primary_count": len(items),
            "active_days": len(days),
            "avg_per_day": len(items) / len(days) if days else 0,
            "max_daily_count": max(days.values()) if days else 0,
            "low_unit_300k_count": len(low),
            "high_unit_300k_count": len(high),
        })
    write_csv(outdir / "current_primary_03_weekly_counts.csv", weekly_rows)

    overlap_rows = overlap_rows_for(standardized, args.holding_days)
    write_csv(outdir / "current_primary_04_overlap_3day.csv", overlap_rows)
    cap_rows = capital_summary_from_overlap(overlap_rows)
    write_csv(outdir / "current_primary_05_capital_requirement.csv", cap_rows)

    scenario_map = {
        "equal_weight_return": "fixed_notional_pnl_100k",
        "unit_100_share": "pnl_100_share",
        "fixed_100k": "fixed_notional_pnl_100k",
        "fixed_200k": "fixed_notional_pnl_200k",
        "fixed_300k": "fixed_notional_pnl_300k",
        "fixed_500k": "fixed_notional_pnl_500k",
        "s_share_100k": "fractional_pnl_100k",
        "s_share_200k": "fractional_pnl_200k",
        "s_share_300k": "fractional_pnl_300k",
        "s_share_500k": "fractional_pnl_500k",
    }
    fixed_rows = [{"scenario": name, **summarize_returns_available(standardized, key)} for name, key in scenario_map.items()]
    write_csv(outdir / "current_primary_06_fixed_notional_summary.csv", fixed_rows)
    write_csv(outdir / "current_primary_07_fractional_share_summary.csv", [r for r in fixed_rows if r["scenario"].startswith("s_share")])
    write_csv(outdir / "current_primary_08_unit_100_share_summary.csv", [r for r in fixed_rows if r["scenario"] == "unit_100_share"])

    low_rows = []
    for threshold in [100_000, 200_000, 300_000, 500_000, 1_000_000]:
        items = [r for r in standardized if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= threshold]
        item_overlap = overlap_rows_for(items, args.holding_days)
        max_open = max((int(r.get("open_count") or 0) for r in item_overlap), default=0)
        low_rows.append({
            "threshold": threshold,
            "trades": len(items),
            "coverage_pct": len(items) / len(standardized) * 100 if standardized else None,
            **summarize_returns_available(items, "fixed_notional_pnl_300k"),
            "unit_100_share_pnl": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)),
            "fixed_100k_pnl": sum(to_float(i.get("fixed_notional_pnl_100k"), 0) or 0 for i in rows_with_return(items)),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_300k_pnl": sum(to_float(i.get("fractional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "max_open_count_3day": max_open,
            "required_capital_3day_100share": max((to_float(r.get("required_low_unit_100share"), 0) or 0 for r in item_overlap), default=0),
            "required_capital_3day_fixed300k": max_open * 300_000,
        })
    write_csv(outdir / "current_primary_09_low_unit_only_summary.csv", low_rows)

    overlay_specs = [
        ("all_100share", None, None, "pnl_100_share"),
        ("all_s_share_200k", 0, 200_000, None),
        ("all_s_share_300k", 0, 300_000, None),
        ("all_s_share_500k", 0, 500_000, None),
        ("unit_le_300k_100share_else_s20", 300_000, 200_000, None),
        ("unit_le_300k_100share_else_s30", 300_000, 300_000, None),
        ("unit_le_300k_100share_else_s50", 300_000, 500_000, None),
        ("unit_le_500k_100share_else_s20", 500_000, 200_000, None),
        ("unit_le_500k_100share_else_s30", 500_000, 300_000, None),
        ("unit_le_500k_100share_else_s50", 500_000, 500_000, None),
    ]
    overlay_rows = []
    for name, threshold, frac_notional, direct_key in overlay_specs:
        sim = []
        too_expensive = 0
        for r in standardized:
            entry = to_float(r.get("entry_price"))
            exitp = to_float(r.get("exit_price"))
            nr = dict(r)
            notional = 0.0
            pnl = None
            if entry:
                if direct_key:
                    notional = to_float(r.get("unit_amount"), 0) or 0
                    pnl = to_float(r.get(direct_key))
                elif threshold and (to_float(r.get("unit_amount"), 0) or 0) <= threshold:
                    notional = to_float(r.get("unit_amount"), 0) or 0
                    pnl = to_float(r.get("pnl_100_share"))
                else:
                    shares = math.floor((frac_notional or 0) / entry)
                    if shares <= 0:
                        too_expensive += 1
                    notional = max(0, shares) * entry
                    pnl = shares * (exitp - entry) if exitp is not None and shares > 0 else None
            nr["overlay_pnl"] = pnl
            nr["overlay_notional"] = notional
            sim.append(nr)
        overlay_overlap = overlap_rows
        overlay_rows.append({
            "scenario": name,
            "skipped_too_expensive": too_expensive,
            "avg_position_notional": sum(to_float(r.get("overlay_notional"), 0) or 0 for r in sim) / len(sim) if sim else None,
            "max_position_notional": max((to_float(r.get("overlay_notional"), 0) or 0 for r in sim), default=0),
            "max_open_required_capital": max((int(r.get("open_count") or 0) for r in overlay_overlap), default=0) * (frac_notional or 100_000),
            "p95_required_capital": next((r["required_capital_p95"] for r in cap_rows if r["scenario"] == "fixed_300k"), 0),
            **summarize_returns_available(sim, "overlay_pnl"),
        })
    write_csv(outdir / "current_primary_10_high_unit_s_share_overlay_summary.csv", overlay_rows)

    audit_rows = load_historical_audit_rows(DEFAULT_AUDIT_DATASET) if DEFAULT_AUDIT_DATASET.exists() else []
    audit_std = [standardize(r) for r in audit_rows]
    add_sizing_columns(audit_std)
    old_live = [r for r in audit_std if to_bool(r.get("selected_by_live_limited"))]
    not_selected = [r for r in audit_std if not to_bool(r.get("selected_by_live_limited"))]
    populations = {
        "research_all": audit_std,
        "old_live_selected": old_live,
        "not_selected": not_selected,
        "current_h5_primary": standardized,
        "current_h5_primary_low_unit_300k": [r for r in standardized if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= 300_000],
        "current_h5_primary_high_unit_300k": [r for r in standardized if (to_float(r.get("unit_amount"), 0) or 0) > 300_000],
    }
    pop_rows = []
    for pop, items in populations.items():
        item_daily = defaultdict(list)
        for r in items:
            dt = r.get("entry_date") or r.get("trade_date")
            if dt:
                item_daily[dt].append(r)
        item_week = defaultdict(list)
        for dt, group_items in item_daily.items():
            d = parse_date(dt)
            if d:
                item_week[week_start(d).isoformat()].extend(group_items)
        item_overlap = overlap_rows_for(items, args.holding_days)
        pop_rows.append({
            "population": pop,
            "trades": len(items),
            **summarize_returns_available(items, "fixed_notional_pnl_300k"),
            "unit_100_share_pnl": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_300k_pnl": sum(to_float(i.get("fractional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "avg_daily_count": len(items) / len(item_daily) if item_daily else 0,
            "avg_weekly_count": len(items) / len(item_week) if item_week else 0,
            "max_daily_count": max((len(v) for v in item_daily.values()), default=0),
            "max_3day_overlap": max((int(r.get("open_count") or 0) for r in item_overlap), default=0),
            "required_capital_fixed300k_max": max((int(r.get("open_count") or 0) for r in item_overlap), default=0) * 300_000,
            "required_capital_fixed300k_p95": next((r["required_capital_p95"] for r in capital_summary_from_overlap(item_overlap) if r["scenario"] == "fixed_300k"), 0),
        })
    write_csv(outdir / "current_primary_11_population_comparison.csv", pop_rows)
    write_csv(outdir / "current_primary_12_trade_log_normalized.csv", standardized)
    write_csv(outdir / "current_primary_13_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in current_skipped.most_common()])

    count_rows = [
        {"population": "research_all", "count": len(audit_std), "notes": "historical audit comparison"},
        {"population": "old_live_selected", "count": len(old_live), "notes": "historical old live limited"},
        {"population": "current_h5_primary", "count": len(standardized), "notes": "stored_predictions H5_full forward-test candidates"},
        {"population": "current_h5_primary_low_unit_300k", "count": len(populations["current_h5_primary_low_unit_300k"]), "notes": "unit_amount <= 300k"},
        {"population": "current_h5_primary_high_unit_300k", "count": len(populations["current_h5_primary_high_unit_300k"]), "notes": "unit_amount > 300k"},
    ]
    write_csv(outdir / "current_primary_01_population_counts.csv", count_rows)

    daily_counts = [int(r["current_h5_primary_count"]) for r in daily_rows]
    weekly_counts = [int(r["current_h5_primary_count"]) for r in weekly_rows]
    week_avg = sum(weekly_counts) / len(weekly_counts) if weekly_counts else 0
    week_median = median(weekly_counts) if weekly_counts else 0
    week_max = max(weekly_counts) if weekly_counts else 0
    week10_pass = week_avg < 10 and week_median < 10
    max_overlap = max((int(r.get("open_count") or 0) for r in overlap_rows), default=0)
    fixed300_cap = next((r for r in cap_rows if r["scenario"] == "fixed_300k"), {})
    fixed200_cap = next((r for r in cap_rows if r["scenario"] == "fixed_200k"), {})
    fixed500_cap = next((r for r in cap_rows if r["scenario"] == "fixed_500k"), {})
    fixed300 = next((r for r in fixed_rows if r["scenario"] == "fixed_300k"), {})
    s300 = next((r for r in fixed_rows if r["scenario"] == "s_share_300k"), {})
    unit100 = next((r for r in fixed_rows if r["scenario"] == "unit_100_share"), {})
    report = f"""
Current H5 Primary fractional sizing analysis

Input sources:
{chr(10).join('- ' + s for s in sources) or '- none'}
price_enrichment: {price_message}

Population counts:
research_all: {len(audit_std)}
old_live_selected: {len(old_live)}
current_h5_primary: {len(standardized)}
current_low_unit_300k: {len(populations['current_h5_primary_low_unit_300k'])}
current_high_unit_300k: {len(populations['current_h5_primary_high_unit_300k'])}

Current H5 Primary extraction:
- H5_full rows only
- score_source/prediction_source stored_predictions or daily_prediction only
- active_model score_source excluded
- fallback rows excluded
- active_model_called rows excluded
- AI_plus_drop-only, K_no_normal-only, Watch, Intraday, Research rows excluded

Daily / weekly appearance:
active_days: {len(daily_rows)}
avg_signals_per_active_day: {(sum(daily_counts) / len(daily_counts)) if daily_counts else 0}
median_signals_per_active_day: {median(daily_counts) if daily_counts else 0}
max_signals_per_day: {max(daily_counts) if daily_counts else 0}
active_weeks: {len(weekly_rows)}
avg_signals_per_week: {week_avg}
median_signals_per_week: {week_median}
max_signals_per_week: {week_max}
week_under_10_hypothesis: {'PASS' if week10_pass else 'FAIL'}

3 business-day overlap:
avg_open_count: {fixed300_cap.get('avg_open_count')}
p95_open_count: {fixed300_cap.get('p95_open_count')}
max_open_count: {max_overlap}

Required capital:
fixed_200k_p95: {fixed200_cap.get('required_capital_p95')}
fixed_200k_max: {fixed200_cap.get('required_capital_max')}
fixed_300k_p95: {fixed300_cap.get('required_capital_p95')}
fixed_300k_max: {fixed300_cap.get('required_capital_max')}
fixed_500k_p95: {fixed500_cap.get('required_capital_p95')}
fixed_500k_max: {fixed500_cap.get('required_capital_max')}
fixed_300k_1_5x_p95: {fixed300_cap.get('required_capital_1_5x')}
fixed_300k_2_0x_p95: {fixed300_cap.get('required_capital_2_0x')}

PnL availability:
return_evaluable_rows: {fixed300.get('return_evaluable_rows')}
pending_or_missing_return_rows: {fixed300.get('pending_or_missing_return_rows')}
unit_100_share_total_pnl: {unit100.get('total_pnl')}
fixed_300k_total_pnl: {fixed300.get('total_pnl')}
s_share_300k_total_pnl: {s300.get('total_pnl')}

Interpretation:
This current-primary report is separated from Research ALL. Research ALL capital numbers
must not be used as current stored_predictions operating capital. If current rows are still
pending, PnL fields are intentionally based only on rows with exit/return data.

Analysis only. Primary, H5 rules, DB case definitions, UI, LINE, actual_trade_logs,
and auto-trading were not changed.
"""
    write_text(outdir / "current_primary_00_input_summary.txt", report)
    write_text(outdir / "current_primary_14_report.txt", report)
    print(report.strip())


def unit_bucket_summary(rows: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[bucket_unit(to_float(r.get("unit_amount")))].append(r)
    out = []
    for bucket, items in sorted(groups.items()):
        out.append({
            "unit_amount_bucket": bucket,
            "coverage_pct": len(items) / len(rows) * 100 if rows else None,
            **summarize_returns_available(items, "fixed_notional_pnl_300k"),
            "unit_100share_pnl": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_300k_pnl": sum(to_float(i.get("fractional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "fixed_500k_pnl": sum(to_float(i.get("fixed_notional_pnl_500k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_500k_pnl": sum(to_float(i.get("fractional_pnl_500k"), 0) or 0 for i in rows_with_return(items)),
            "max_win_100share": max((to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)), default=0),
            "max_loss_100share": min((to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)), default=0),
            "max_unit_amount": max((to_float(i.get("unit_amount"), 0) or 0 for i in items), default=0),
        })
    return out


def dd_summary_rows(rows: list[dict], cap_rows: list[dict]) -> list[dict]:
    scenarios = {
        "unit_100share": ("pnl_100_share", "unit_100share"),
        "fixed_200k": ("fixed_notional_pnl_200k", "fixed_200k"),
        "fixed_300k": ("fixed_notional_pnl_300k", "fixed_300k"),
        "fixed_500k": ("fixed_notional_pnl_500k", "fixed_500k"),
        "s_share_200k": ("fractional_pnl_200k", "s_share_200k"),
        "s_share_300k": ("fractional_pnl_300k", "s_share_300k"),
        "s_share_500k": ("fractional_pnl_500k", "s_share_500k"),
        "low_unit_300k_only_100share": ("pnl_100_share", "low_unit_300k_only_100share"),
    }
    cap_by = {r["scenario"]: r for r in cap_rows}
    out = []
    for scenario, (pnl_key, cap_key) in scenarios.items():
        items = rows
        if scenario == "low_unit_300k_only_100share":
            items = [r for r in rows if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        s = summarize_returns_available(items, pnl_key)
        cap = cap_by.get(cap_key, {})
        worst = min(rows_with_return(items), key=lambda r: to_float(r.get(pnl_key), 0) or 0, default={})
        out.append({
            "scenario": scenario,
            "total_pnl": s.get("total_pnl"),
            "max_dd_yen": s.get("max_dd"),
            "max_dd_start": "",
            "max_dd_end": "",
            "max_dd_duration_days": "",
            "required_capital_p95": cap.get("required_capital_p95"),
            "required_capital_max": cap.get("required_capital_max"),
            "max_dd_pct_of_required_capital_p95": (s.get("max_dd") / cap.get("required_capital_p95") * 100) if s.get("max_dd") is not None and cap.get("required_capital_p95") else None,
            "max_dd_pct_of_required_capital_max": (s.get("max_dd") / cap.get("required_capital_max") * 100) if s.get("max_dd") is not None and cap.get("required_capital_max") else None,
            "max_consecutive_losses": s.get("max_consecutive_losses"),
            "worst_trade_code": worst.get("code"),
            "worst_trade_name": worst.get("name"),
            "worst_trade_return_pct": worst.get("return_pct"),
            "worst_trade_pnl": worst.get(pnl_key),
        })
    return out


def write_walk_forward_outputs(args) -> None:
    outdir = ROOT / args.output_dir / "walk_forward_h5_full"
    rows, skipped, sources = load_walk_forward_h5_full_rows(args)
    standardized = [standardize(r) for r in rows]
    for r in standardized:
        r["population"] = "walk_forward_h5_full"
        r["case_key"] = "H5_full"
        r["h5_case_key"] = "H5_full"
        r["H5_full"] = True
        r["h5_primary_match"] = True
    if args.start_date:
        standardized = [r for r in standardized if (r.get("entry_date") or "") >= args.start_date]
    if args.end_date:
        standardized = [r for r in standardized if (r.get("entry_date") or "") <= args.end_date]
    add_sizing_columns(standardized)

    daily_groups: dict[str, list[dict]] = defaultdict(list)
    for r in standardized:
        daily_groups[r["entry_date"]].append(r)
    daily_rows = []
    for dt, items in sorted(daily_groups.items()):
        low = [i for i in items if (to_float(i.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        high = [i for i in items if (to_float(i.get("unit_amount"), 0) or 0) > 300_000]
        daily_rows.append({
            "date": dt,
            "h5_full_count": len(items),
            "low_unit_300k_count": len(low),
            "high_unit_300k_count": len(high),
            "avg_unit_amount": sum(to_float(i.get("unit_amount"), 0) or 0 for i in items) / len(items) if items else None,
            "max_unit_amount": max((to_float(i.get("unit_amount"), 0) or 0 for i in items), default=0),
            "total_theoretical_fixed_300k_entry_notional": len(items) * 300_000,
            "total_s_share_300k_entry_notional": sum(to_float(i.get("fractional_notional_300k"), 0) or 0 for i in items),
            "total_100share_entry_notional": sum(to_float(i.get("unit_amount"), 0) or 0 for i in items),
        })
    write_csv(outdir / "wf_02_daily_counts.csv", daily_rows)

    week_groups: dict[str, list[dict]] = defaultdict(list)
    for dt, items in daily_groups.items():
        d = parse_date(dt)
        if d:
            week_groups[week_start(d).isoformat()].extend(items)
    weekly_rows = []
    for ws, items in sorted(week_groups.items()):
        start = parse_date(ws) or date.today()
        days = Counter(i.get("entry_date") for i in items)
        low = [i for i in items if (to_float(i.get("unit_amount"), math.inf) or math.inf) <= 300_000]
        high = [i for i in items if (to_float(i.get("unit_amount"), 0) or 0) > 300_000]
        weekly_rows.append({
            "week_start": ws,
            "week_end": (start + timedelta(days=6)).isoformat(),
            "h5_full_count": len(items),
            "active_days": len(days),
            "avg_per_day": len(items) / len(days) if days else 0,
            "max_daily_count": max(days.values()) if days else 0,
            "low_unit_300k_count": len(low),
            "high_unit_300k_count": len(high),
            "fixed_300k_week_required_notional": len(items) * 300_000,
        })
    write_csv(outdir / "wf_03_weekly_counts.csv", weekly_rows)

    overlap = overlap_rows_for(standardized, args.holding_days)
    write_csv(outdir / "wf_04_overlap_3day.csv", overlap)
    cap_rows = capital_summary_from_overlap(overlap)
    write_csv(outdir / "wf_05_capital_requirement.csv", cap_rows)

    scenarios = {
        "equal_weight_return": "fixed_notional_pnl_100k",
        "unit_100share": "pnl_100_share",
        "fixed_100k": "fixed_notional_pnl_100k",
        "fixed_200k": "fixed_notional_pnl_200k",
        "fixed_300k": "fixed_notional_pnl_300k",
        "fixed_500k": "fixed_notional_pnl_500k",
        "fixed_1000k": "fixed_notional_pnl_1000k",
        "s_share_100k": "fractional_pnl_100k",
        "s_share_200k": "fractional_pnl_200k",
        "s_share_300k": "fractional_pnl_300k",
        "s_share_500k": "fractional_pnl_500k",
        "s_share_1000k": "fractional_pnl_1000k",
        "one_share": "one_share_pnl",
    }
    summary_rows = []
    cap_by = {r["scenario"]: r for r in cap_rows}
    for scenario, key in scenarios.items():
        s = summarize_returns_available(standardized, key)
        cap_key = scenario if scenario in cap_by else ("fixed_100k" if scenario == "equal_weight_return" else "")
        cap = cap_by.get(cap_key, {})
        s["required_capital_p95"] = cap.get("required_capital_p95")
        s["required_capital_max"] = cap.get("required_capital_max")
        s["max_dd_pct_of_required_capital_p95"] = (s.get("max_dd") / cap.get("required_capital_p95") * 100) if s.get("max_dd") is not None and cap.get("required_capital_p95") else None
        s["max_dd_pct_of_required_capital_max"] = (s.get("max_dd") / cap.get("required_capital_max") * 100) if s.get("max_dd") is not None and cap.get("required_capital_max") else None
        summary_rows.append({"scenario": scenario, **s})
    write_csv(outdir / "wf_06_fixed_notional_summary.csv", summary_rows)
    write_csv(outdir / "wf_07_fractional_share_summary.csv", [r for r in summary_rows if str(r["scenario"]).startswith("s_share") or r["scenario"] == "one_share"])
    write_csv(outdir / "wf_08_unit_100_share_summary.csv", [r for r in summary_rows if r["scenario"] == "unit_100share"])

    low_rows = []
    for threshold in [100_000, 200_000, 300_000, 500_000, 1_000_000]:
        items = [r for r in standardized if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= threshold]
        item_overlap = overlap_rows_for(items, args.holding_days)
        item_cap = capital_summary_from_overlap(item_overlap)
        low_rows.append({
            "threshold": threshold,
            "trades": len(items),
            "coverage_pct": len(items) / len(standardized) * 100 if standardized else None,
            **summarize_returns_available(items, "fixed_notional_pnl_300k"),
            "unit_100share_pnl": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_300k_pnl": sum(to_float(i.get("fractional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "max_open_count_3day": max((int(r.get("open_count") or 0) for r in item_overlap), default=0),
            "required_capital_100share_p95": next((r["required_capital_p95"] for r in item_cap if r["scenario"] == "unit_100share"), 0),
            "required_capital_100share_max": next((r["required_capital_max"] for r in item_cap if r["scenario"] == "unit_100share"), 0),
            "required_capital_fixed300k_p95": next((r["required_capital_p95"] for r in item_cap if r["scenario"] == "fixed_300k"), 0),
            "required_capital_fixed300k_max": next((r["required_capital_max"] for r in item_cap if r["scenario"] == "fixed_300k"), 0),
        })
    write_csv(outdir / "wf_09_low_unit_only_summary.csv", low_rows)

    overlay_specs = [
        ("all_100share", None, None, "pnl_100_share"),
        ("all_s_share_200k", 0, 200_000, None),
        ("all_s_share_300k", 0, 300_000, None),
        ("all_s_share_500k", 0, 500_000, None),
        ("unit_le_300k_100share_else_s20", 300_000, 200_000, None),
        ("unit_le_300k_100share_else_s30", 300_000, 300_000, None),
        ("unit_le_300k_100share_else_s50", 300_000, 500_000, None),
        ("unit_le_500k_100share_else_s20", 500_000, 200_000, None),
        ("unit_le_500k_100share_else_s30", 500_000, 300_000, None),
        ("unit_le_500k_100share_else_s50", 500_000, 500_000, None),
    ]
    overlay_rows = []
    for name, threshold, frac_notional, direct_key in overlay_specs:
        sim = []
        skipped_expensive = 0
        for r in standardized:
            entry = to_float(r.get("entry_price"))
            exitp = to_float(r.get("exit_price"))
            nr = dict(r)
            if direct_key:
                pnl = to_float(r.get(direct_key))
                notional = to_float(r.get("unit_amount"), 0) or 0
            elif threshold and (to_float(r.get("unit_amount"), 0) or 0) <= threshold:
                pnl = to_float(r.get("pnl_100_share"))
                notional = to_float(r.get("unit_amount"), 0) or 0
            elif entry:
                shares = math.floor((frac_notional or 0) / entry)
                if shares <= 0:
                    skipped_expensive += 1
                pnl = shares * (exitp - entry) if exitp is not None and shares > 0 else None
                notional = shares * entry if shares > 0 else 0
            else:
                pnl = None
                notional = 0
            nr["overlay_pnl"] = pnl
            nr["overlay_notional"] = notional
            sim.append(nr)
        s = summarize_returns_available(sim, "overlay_pnl")
        overlay_rows.append({
            "scenario": name,
            "skipped_too_expensive": skipped_expensive,
            "avg_position_notional": sum(to_float(r.get("overlay_notional"), 0) or 0 for r in sim) / len(sim) if sim else None,
            "max_position_notional": max((to_float(r.get("overlay_notional"), 0) or 0 for r in sim), default=0),
            "required_capital_p95": next((r["required_capital_p95"] for r in cap_rows if r["scenario"] == "fixed_300k"), 0),
            "required_capital_max": next((r["required_capital_max"] for r in cap_rows if r["scenario"] == "fixed_300k"), 0),
            **s,
        })
    write_csv(outdir / "wf_10_high_unit_s_share_overlay_summary.csv", overlay_rows)

    audit_rows = [standardize(r) for r in load_historical_audit_rows(DEFAULT_AUDIT_DATASET)] if DEFAULT_AUDIT_DATASET.exists() else []
    add_sizing_columns(audit_rows)
    stored_rows, _, _ = load_current_primary_raw(args)
    stored_std = [standardize(r) for r in stored_rows]
    enrich_current_prices_from_db(stored_std, args, Counter())
    add_sizing_columns(stored_std)
    pop_defs = {
        "research_all": audit_rows,
        "old_live_selected": [r for r in audit_rows if to_bool(r.get("selected_by_live_limited"))],
        "walk_forward_h5_full": standardized,
        "stored_forward_current_primary": stored_std,
        "walk_forward_low_unit_300k": [r for r in standardized if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= 300_000],
        "walk_forward_high_unit_300k": [r for r in standardized if (to_float(r.get("unit_amount"), 0) or 0) > 300_000],
    }
    pop_rows = []
    for pop, items in pop_defs.items():
        item_daily: dict[str, list[dict]] = defaultdict(list)
        for r in items:
            if r.get("entry_date"):
                item_daily[r["entry_date"]].append(r)
        item_week: dict[str, list[dict]] = defaultdict(list)
        for dt, vals in item_daily.items():
            d = parse_date(dt)
            if d:
                item_week[week_start(d).isoformat()].extend(vals)
        item_overlap = overlap_rows_for(items, args.holding_days)
        item_cap = capital_summary_from_overlap(item_overlap)
        pop_rows.append({
            "population": pop,
            "trades": len(items),
            **summarize_returns_available(items, "fixed_notional_pnl_300k"),
            "unit_100share_pnl": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in rows_with_return(items)),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "s_share_300k_pnl": sum(to_float(i.get("fractional_pnl_300k"), 0) or 0 for i in rows_with_return(items)),
            "avg_daily_count": len(items) / len(item_daily) if item_daily else 0,
            "avg_weekly_count": len(items) / len(item_week) if item_week else 0,
            "max_daily_count": max((len(v) for v in item_daily.values()), default=0),
            "max_weekly_count": max((len(v) for v in item_week.values()), default=0),
            "max_3day_overlap": max((int(r.get("open_count") or 0) for r in item_overlap), default=0),
            "required_capital_fixed300k_p95": next((r["required_capital_p95"] for r in item_cap if r["scenario"] == "fixed_300k"), 0),
            "required_capital_fixed300k_max": next((r["required_capital_max"] for r in item_cap if r["scenario"] == "fixed_300k"), 0),
        })
    write_csv(outdir / "wf_11_population_comparison.csv", pop_rows)
    write_csv(outdir / "wf_12_trade_log_normalized.csv", standardized)
    write_csv(outdir / "wf_13_unit_amount_bucket_summary.csv", unit_bucket_summary(standardized))
    gap_rows = grouped_summary(standardized, lambda r: bucket_gap(to_float(r.get("entry_gap_pct"))), "fixed_notional_pnl_300k")
    for r in gap_rows:
        r["entry_gap_bucket"] = r.pop("bucket")
    write_csv(outdir / "wf_14_entry_gap_summary.csv", gap_rows)
    write_csv(outdir / "wf_15_drawdown_summary.csv", dd_summary_rows(standardized, cap_rows))
    score_rows = grouped_summary(standardized, lambda r: bucket_score(to_float(r.get("signal_probability"))), "fixed_notional_pnl_300k")
    for r in score_rows:
        r["score_bucket"] = r.pop("bucket")
    write_csv(outdir / "wf_16_score_bucket_summary.csv", score_rows)
    write_csv(outdir / "wf_17_market_benchmark_comparison.csv", [{"status": "benchmark_unavailable", "notes": "No market benchmark CSV was supplied."}])
    write_csv(outdir / "wf_18_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in skipped.most_common()])
    write_csv(outdir / "wf_01_population_counts.csv", [
        {"population": "research_all", "count": len(audit_rows), "notes": "historical audit comparison"},
        {"population": "old_live_selected", "count": len(pop_defs["old_live_selected"]), "notes": "historical old live limited"},
        {"population": "walk_forward_h5_full", "count": len(standardized), "notes": "walk-forward OOS H5_full"},
        {"population": "stored_forward_current_primary", "count": len(stored_std), "notes": "stored_predictions current forward candidates"},
    ])

    weekly_counts = [int(r["h5_full_count"]) for r in weekly_rows]
    daily_counts = [int(r["h5_full_count"]) for r in daily_rows]
    week_avg = sum(weekly_counts) / len(weekly_counts) if weekly_counts else 0
    week_median = median(weekly_counts) if weekly_counts else 0
    week_max = max(weekly_counts) if weekly_counts else 0
    week10_avg = week_avg < 10
    week10_median = week_median < 10
    fixed200 = next((r for r in cap_rows if r["scenario"] == "fixed_200k"), {})
    fixed300 = next((r for r in cap_rows if r["scenario"] == "fixed_300k"), {})
    fixed500 = next((r for r in cap_rows if r["scenario"] == "fixed_500k"), {})
    s200 = next((r for r in summary_rows if r["scenario"] == "s_share_200k"), {})
    s300 = next((r for r in summary_rows if r["scenario"] == "s_share_300k"), {})
    s500 = next((r for r in summary_rows if r["scenario"] == "s_share_500k"), {})
    unit = next((r for r in summary_rows if r["scenario"] == "unit_100share"), {})
    f300 = next((r for r in summary_rows if r["scenario"] == "fixed_300k"), {})
    report = f"""
Walk-forward H5_full fractional sizing analysis

Input sources:
{chr(10).join('- ' + s for s in sources) or '- none'}

walk_forward_h5_full_rows: {len(standardized)}
period_start: {min((r.get('entry_date') for r in standardized), default='')}
period_end: {max((r.get('entry_date') for r in standardized), default='')}

Appearance:
active_signal_days: {len(daily_rows)}
avg_signals_per_active_day: {(sum(daily_counts) / len(daily_counts)) if daily_counts else 0}
median_signals_per_active_day: {median(daily_counts) if daily_counts else 0}
max_signals_per_day: {max(daily_counts) if daily_counts else 0}
active_weeks: {len(weekly_rows)}
avg_signals_per_week: {week_avg}
median_signals_per_week: {week_median}
max_signals_per_week: {week_max}
week10_hypothesis_avg: {'PASS' if week10_avg else 'FAIL'}
week10_hypothesis_median: {'PASS' if week10_median else 'FAIL'}

3 business-day overlap:
avg_open_count: {fixed300.get('avg_open_count')}
p95_open_count: {fixed300.get('p95_open_count')}
max_open_count: {fixed300.get('max_open_count')}

Required capital:
fixed_200k_p95: {fixed200.get('required_capital_p95')}
fixed_200k_max: {fixed200.get('required_capital_max')}
fixed_300k_p95: {fixed300.get('required_capital_p95')}
fixed_300k_max: {fixed300.get('required_capital_max')}
fixed_500k_p95: {fixed500.get('required_capital_p95')}
fixed_500k_max: {fixed500.get('required_capital_max')}
fixed_300k_1_5x_p95: {fixed300.get('required_capital_1_5x')}
fixed_300k_2_0x_p95: {fixed300.get('required_capital_2_0x')}

PnL:
unit_100share_total_pnl: {unit.get('total_pnl')}
fixed_300k_total_pnl: {f300.get('total_pnl')}
s_share_200k_total_pnl: {s200.get('total_pnl')}
s_share_300k_total_pnl: {s300.get('total_pnl')}
s_share_500k_total_pnl: {s500.get('total_pnl')}
fixed_300k_pf: {f300.get('profit_factor')}
s_share_300k_pf: {s300.get('profit_factor')}

Conclusion:
This is the walk-forward OOS H5_full population, separated from Research ALL and old Live Selected.
It should be used for practical sizing estimates before enough stored_predictions forward rows accumulate.
Benchmark comparison is unavailable unless a market benchmark CSV is supplied.

Analysis only. Primary, H5 rules, DB case definitions, UI, LINE, actual_trade_logs,
and auto-trading were not changed.
"""
    write_text(outdir / "wf_00_input_summary.txt", report)
    write_text(outdir / "wf_19_week10_hypothesis_report.txt", report)
    write_text(outdir / "wf_20_report.txt", report)
    print(report.strip())


def main() -> None:
    p = argparse.ArgumentParser(description="Analyze H5 Primary fractional/fixed-notional sizing")
    p.add_argument("--input", default="")
    p.add_argument("--trade-log", default="")
    p.add_argument("--candidate-log", default="")
    p.add_argument("--market-benchmark", default="")
    p.add_argument("--start-date", default="")
    p.add_argument("--end-date", default="")
    p.add_argument("--model-key", default="")
    p.add_argument("--model-version", default="")
    p.add_argument("--case-key", default=H5_PRIMARY_CASE_KEY)
    p.add_argument("--only-h5-primary", default="true")
    p.add_argument("--holding-days", type=int, default=3)
    p.add_argument("--output-dir", default="outputs/h5_primary_fractional_sizing")
    p.add_argument("--include-open", default="false")
    p.add_argument("--historical-source", choices=["none", "audit", "loader", "both", "current-primary", "walk-forward-h5-full"], default="audit")
    p.add_argument("--compare-populations", default="false")
    p.add_argument("--enrich-prices-from-db", default="true")
    p.add_argument("--loader-score-source", choices=["active_model", "stored_predictions", "stored_or_active_fallback"], default="active_model")
    p.add_argument("--load-db", default="false")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    if args.historical_source == "current-primary":
        write_current_primary_outputs(args)
        return
    if args.historical_source == "walk-forward-h5-full":
        write_walk_forward_outputs(args)
        return

    outdir = ROOT / args.output_dir
    raw, sources = load_inputs(args)
    standardized = [standardize(r) for r in raw]
    if args.start_date:
        standardized = [r for r in standardized if (r.get("entry_date") or "") >= args.start_date]
    if args.end_date:
        standardized = [r for r in standardized if (r.get("entry_date") or "") <= args.end_date]

    skipped = Counter()
    primary_rows = []
    non_primary = []
    for r in standardized:
        reason = h5_filter_reason(r)
        if reason:
            skipped[reason] += 1
            non_primary.append(r)
            continue
        if not r.get("code"):
            skipped["missing_code"] += 1
            continue
        if not r.get("entry_price"):
            skipped["missing_entry_price"] += 1
            continue
        if not has_evaluable_return(r, to_bool(args.include_open)):
            skipped["missing_exit_or_return"] += 1
            continue
        primary_rows.append(r)
    primary_rows.sort(key=lambda r: (r.get("entry_date") or "", r.get("code") or ""))
    add_sizing_columns(primary_rows)

    daily_counts: dict[str, list[dict]] = defaultdict(list)
    for r in primary_rows:
        daily_counts[r["entry_date"]].append(r)
    daily_rows = []
    for dt, items in sorted(daily_counts.items()):
        daily_rows.append({
            "date": dt,
            "h5_primary_count": len(items),
            "closed_count": sum(not i.get("is_open") for i in items),
            "open_count": sum(bool(i.get("is_open")) for i in items),
            "new_notional_100k": len(items) * 100_000,
            "new_notional_300k": len(items) * 300_000,
            "new_notional_500k": len(items) * 500_000,
        })
    write_csv(outdir / "01_h5_primary_daily_counts.csv", daily_rows)

    overlap: dict[str, list[dict]] = defaultdict(list)
    for r in primary_rows:
        for dt in active_dates(r, args.holding_days):
            overlap[dt].append(r)
    overlap_rows = []
    for dt, items in sorted(overlap.items()):
        overlap_rows.append({
            "date": dt,
            "open_positions_count": len(items),
            "open_notional_100k": len(items) * 100_000,
            "open_notional_200k": len(items) * 200_000,
            "open_notional_300k": len(items) * 300_000,
            "open_notional_500k": len(items) * 500_000,
            "open_fractional_notional_100k": sum(to_float(i.get("fractional_notional_100k"), 0) or 0 for i in items),
            "open_fractional_notional_300k": sum(to_float(i.get("fractional_notional_300k"), 0) or 0 for i in items),
            "open_fractional_notional_500k": sum(to_float(i.get("fractional_notional_500k"), 0) or 0 for i in items),
        })
    write_csv(outdir / "02_h5_primary_overlap_summary.csv", overlap_rows)

    scenario_map = {
        "equal_weight_return": "fixed_notional_pnl_100k",
        "unit_100_share": "pnl_100_share",
        "fixed_100k": "fixed_notional_pnl_100k",
        "fixed_200k": "fixed_notional_pnl_200k",
        "fixed_300k": "fixed_notional_pnl_300k",
        "fixed_500k": "fixed_notional_pnl_500k",
        "fixed_1000k": "fixed_notional_pnl_1000k",
        "fractional_100k": "fractional_pnl_100k",
        "fractional_200k": "fractional_pnl_200k",
        "fractional_300k": "fractional_pnl_300k",
        "fractional_500k": "fractional_pnl_500k",
    }
    summary_rows = [{"scenario": name, **summarize_returns(primary_rows, key)} for name, key in scenario_map.items()]
    write_csv(outdir / "03_position_sizing_summary.csv", summary_rows)
    write_csv(outdir / "04_fixed_notional_pnl_summary.csv", summary_rows)
    write_csv(outdir / "05_fractional_sizing_trade_log.csv", primary_rows)
    write_csv(outdir / "06_unit_100_share_trade_log.csv", primary_rows)

    population_rows = []
    populations = {
        "Research_Audit_ALL": primary_rows,
        "Historical_Live_Limited_Selected": [r for r in primary_rows if to_bool(r.get("selected_by_live_limited"))],
        "Historical_Not_Selected": [r for r in primary_rows if not to_bool(r.get("selected_by_live_limited"))],
    }
    for pop_name, items in populations.items():
        for scenario, key in {
            "unit_100_share": "pnl_100_share",
            "fixed_300k": "fixed_notional_pnl_300k",
            "fractional_300k": "fractional_pnl_300k",
        }.items():
            population_rows.append({
                "population": pop_name,
                "scenario": scenario,
                **summarize_returns(items, key),
            })
    write_csv(outdir / "20_population_sizing_comparison.csv", population_rows)

    risk_rows = []
    for budget in RISK_BUDGETS:
        for stop_pct in STOP_PCTS:
            sim = []
            skipped_expensive = 0
            for r in primary_rows:
                entry = to_float(r.get("entry_price"))
                exitp = to_float(r.get("exit_price"))
                if not entry or exitp is None:
                    continue
                notional = min(MAX_RISK_NOTIONAL, budget / (stop_pct / 100))
                shares = math.floor(notional / entry)
                if shares <= 0:
                    skipped_expensive += 1
                    continue
                nr = dict(r)
                nr[f"risk_budget_{budget}_stop{stop_pct}_pnl"] = shares * (exitp - entry)
                nr["risk_position_notional"] = shares * entry
                sim.append(nr)
            key = f"risk_budget_{budget}_stop{stop_pct}_pnl"
            s = summarize_returns(sim, key)
            risk_rows.append({
                "risk_budget": budget,
                "stop_pct": stop_pct,
                "max_position_notional": MAX_RISK_NOTIONAL,
                "skipped_too_expensive": skipped_expensive,
                "required_capital_estimate": max((to_float(x.get("risk_position_notional"), 0) or 0 for x in sim), default=0),
                **s,
            })
    write_csv(outdir / "07_risk_budget_sizing_summary.csv", risk_rows)

    bucket_rows = []
    for b, items in defaultdict(list, {}).items():
        pass
    unit_groups: dict[str, list[dict]] = defaultdict(list)
    for r in primary_rows:
        unit_groups[bucket_unit(to_float(r.get("unit_amount")))].append(r)
    for bucket, items in sorted(unit_groups.items()):
        bucket_rows.append({
            "unit_amount_bucket": bucket,
            **summarize_returns(items, "fixed_notional_pnl_300k"),
            "total_pnl_100_share": sum(to_float(i.get("pnl_100_share"), 0) or 0 for i in items),
            "total_pnl_fixed_100k": sum(to_float(i.get("fixed_notional_pnl_100k"), 0) or 0 for i in items),
            "total_pnl_fixed_300k": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in items),
            "total_pnl_fixed_500k": sum(to_float(i.get("fixed_notional_pnl_500k"), 0) or 0 for i in items),
            "max_loss_100_share": min((to_float(i.get("pnl_100_share"), 0) or 0 for i in items), default=0),
            "max_win_100_share": max((to_float(i.get("pnl_100_share"), 0) or 0 for i in items), default=0),
        })
    write_csv(outdir / "08_unit_amount_bucket_summary.csv", bucket_rows)

    low_rows = []
    for threshold in [100_000, 200_000, 300_000, 500_000, 1_000_000]:
        items = [r for r in primary_rows if (to_float(r.get("unit_amount"), math.inf) or math.inf) <= threshold]
        s = summarize_returns(items, "fixed_notional_pnl_300k")
        max_open = max((row["open_positions_count"] for row in overlap_rows), default=0)
        low_rows.append({
            "threshold": threshold,
            "coverage_pct": len(items) / len(primary_rows) * 100 if primary_rows else None,
            "required_capital_3day_100k": max_open * 100_000,
            "required_capital_3day_300k": max_open * 300_000,
            **s,
            "fixed_100k_pnl": sum(to_float(i.get("fixed_notional_pnl_100k"), 0) or 0 for i in items),
            "fixed_300k_pnl": sum(to_float(i.get("fixed_notional_pnl_300k"), 0) or 0 for i in items),
        })
    write_csv(outdir / "09_low_unit_only_summary.csv", low_rows)

    overlay_rows = []
    overlay_specs = [
        ("all_100_share", None, None, "pnl_100_share"),
        ("lte300k_100share_over_100k_frac", 300_000, 100_000, None),
        ("lte300k_100share_over_200k_frac", 300_000, 200_000, None),
        ("lte300k_100share_over_300k_frac", 300_000, 300_000, None),
        ("lte500k_100share_over_200k_frac", 500_000, 200_000, None),
        ("all_fractional_200k", 0, 200_000, None),
        ("all_fractional_300k", 0, 300_000, None),
        ("all_fractional_500k", 0, 500_000, None),
    ]
    for name, threshold, frac_notional, direct_key in overlay_specs:
        sim = []
        for r in primary_rows:
            entry = to_float(r.get("entry_price"))
            exitp = to_float(r.get("exit_price"))
            if not entry or exitp is None:
                continue
            nr = dict(r)
            if direct_key:
                pnl = to_float(r.get(direct_key), 0) or 0
                notional = to_float(r.get("unit_amount"), 0) or 0
            elif threshold and (to_float(r.get("unit_amount"), 0) or 0) <= threshold:
                pnl = to_float(r.get("pnl_100_share"), 0) or 0
                notional = to_float(r.get("unit_amount"), 0) or 0
            else:
                shares = max(1, math.floor((frac_notional or 0) / entry))
                pnl = shares * (exitp - entry)
                notional = shares * entry
            nr["overlay_pnl"] = pnl
            nr["overlay_notional"] = notional
            sim.append(nr)
        overlay_rows.append({
            "scenario": name,
            "avg_position_notional": sum(to_float(r.get("overlay_notional"), 0) or 0 for r in sim) / len(sim) if sim else None,
            "max_position_notional": max((to_float(r.get("overlay_notional"), 0) or 0 for r in sim), default=0),
            "required_capital_max": max((row["open_positions_count"] for row in overlap_rows), default=0) * (frac_notional or 100_000),
            "skipped": 0,
            **summarize_returns(sim, "overlay_pnl"),
        })
    write_csv(outdir / "10_high_unit_fractional_overlay_summary.csv", overlay_rows)

    gap_rows = grouped_summary(primary_rows, lambda r: bucket_gap(to_float(r.get("entry_gap_pct"))))
    for r in gap_rows:
        r["entry_gap_bucket"] = r.pop("bucket")
    write_csv(outdir / "11_entry_gap_summary.csv", gap_rows)

    benchmark_rows = []
    if args.market_benchmark:
        benchmark_rows.append({"status": "benchmark_csv_not_implemented", "path": args.market_benchmark})
    else:
        benchmark_rows.append({"status": "benchmark_unavailable", "notes": "No market benchmark CSV was supplied. H5 vs Nikkei/TOPIX/1570 comparison skipped."})
    write_csv(outdir / "12_market_benchmark_comparison.csv", benchmark_rows)

    dd_rows = []
    for name, key in scenario_map.items():
        s = summarize_returns(primary_rows, key)
        max_open = max((row["open_positions_count"] for row in overlap_rows), default=0)
        capital = max_open * (300_000 if "300" in name else 100_000)
        dd_rows.append({
            "scenario": name,
            "total_pnl": s["total_pnl"],
            "max_dd_yen": s["max_dd"],
            "max_dd_pct_of_required_capital": (s["max_dd"] / capital * 100) if capital else None,
            "max_consecutive_losses": s["max_consecutive_losses"],
            "worst_trade": s["max_loss"],
        })
    write_csv(outdir / "13_drawdown_summary.csv", dd_rows)

    cap_rows = []
    counts = [row["open_positions_count"] for row in overlap_rows]
    for notional in [100_000, 200_000, 300_000, 500_000]:
        counts_sorted = sorted(counts)
        p95 = counts_sorted[int(len(counts_sorted) * 0.95) - 1] if counts_sorted else 0
        p90 = counts_sorted[int(len(counts_sorted) * 0.90) - 1] if counts_sorted else 0
        avg_open = sum(counts) / len(counts) if counts else 0
        max_open = max(counts) if counts else 0
        cap_rows.append({
            "notional_per_trade": notional,
            "avg_open_positions": avg_open,
            "max_open_positions": max_open,
            "p90_open_positions": p90,
            "p95_open_positions": p95,
            "required_capital_avg": avg_open * notional,
            "required_capital_max": max_open * notional,
            "required_capital_p95": p95 * notional,
            "required_capital_1_5x": p95 * notional * 1.5,
            "required_capital_2_0x": p95 * notional * 2.0,
        })
    write_csv(outdir / "14_capital_requirement_summary.csv", cap_rows)

    exit_rows = grouped_summary(primary_rows, lambda r: r.get("exit_reason") or "unknown")
    for r in exit_rows:
        r["exit_reason"] = r.pop("bucket")
    write_csv(outdir / "15_exit_reason_summary.csv", exit_rows)

    score_rows = grouped_summary(primary_rows, lambda r: bucket_score(to_float(r.get("signal_probability"))))
    for r in score_rows:
        r["score_bucket"] = r.pop("bucket")
    write_csv(outdir / "16_score_bucket_summary.csv", score_rows)

    today = max((parse_date(r.get("entry_date")) for r in primary_rows if parse_date(r.get("entry_date"))), default=None)
    periods = [("all", None), ("recent_1m", 31), ("recent_3m", 93)]
    recent_rows = []
    for label, days in periods:
        if days and today:
            start = today - timedelta(days=days)
            items = [r for r in primary_rows if (parse_date(r.get("entry_date")) or date.min) >= start]
        else:
            items = primary_rows
        recent_rows.append({"period": label, **summarize_returns(items, "fixed_notional_pnl_300k")})
    since = date(2026, 5, 12)
    items = [r for r in primary_rows if (parse_date(r.get("entry_date")) or date.min) >= since]
    recent_rows.append({"period": "since_2026_05_12", **summarize_returns(items, "fixed_notional_pnl_300k")})
    write_csv(outdir / "17_recent_period_summary.csv", recent_rows)
    write_csv(outdir / "18_skipped_rows_summary.csv", [{"reason": k, "count": v} for k, v in skipped.most_common()])

    total_trade_days = len(daily_rows)
    signal_counts = [r["h5_primary_count"] for r in daily_rows]
    selected_rows = populations["Historical_Live_Limited_Selected"]
    not_selected_rows = populations["Historical_Not_Selected"]
    def scenario_value(name: str, key: str) -> Any:
        row = next((r for r in summary_rows if r["scenario"] == name), {})
        return row.get(key)
    def population_value(pop: str, scenario: str, key: str) -> Any:
        row = next((r for r in population_rows if r["population"] == pop and r["scenario"] == scenario), {})
        return row.get(key)
    fixed300_pf = scenario_value("fixed_300k", "profit_factor")
    fractional300_pf = scenario_value("fractional_300k", "profit_factor")
    unit_pf = scenario_value("unit_100_share", "profit_factor")
    summary_text = f"""
H5 Primary fractional sizing analysis

Input sources:
{chr(10).join('- ' + s for s in sources) or '- none'}

input_rows: {len(raw)}
standardized_rows: {len(standardized)}
h5_primary_evaluable_rows: {len(primary_rows)}
excluded_rows: {sum(skipped.values())}
case_key: {args.case_key}
holding_days: {args.holding_days}
historical_source: {args.historical_source}

Population note:
This run uses the broad historical H5 live-selection audit dataset by default,
not only the current one-day stored-forward log. Main rows are Research_Audit_ALL.
The old Live Limited selection is reported separately in 20_population_sizing_comparison.csv.
historical_live_limited_selected_rows: {len(selected_rows)}
historical_not_selected_rows: {len(not_selected_rows)}

Daily appearance:
active_days: {total_trade_days}
avg_signals_per_active_day: {(sum(signal_counts) / len(signal_counts)) if signal_counts else 0:.3f}
median_signals_per_active_day: {median(signal_counts) if signal_counts else 0}
max_signals_per_day: {max(signal_counts) if signal_counts else 0}

Overlap / capital:
max_open_positions: {max((r['open_positions_count'] for r in overlap_rows), default=0)}
required_capital_300k_max: {max((r['open_positions_count'] for r in overlap_rows), default=0) * 300_000:,.0f}

PnL summary:
unit_100_share_total_pnl: {(scenario_value('unit_100_share', 'total_pnl') or 0):,.0f}
unit_100_share_pf: {unit_pf}
fixed_300k_total_pnl: {(scenario_value('fixed_300k', 'total_pnl') or 0):,.0f}
fixed_300k_pf: {fixed300_pf}
fractional_300k_total_pnl: {(scenario_value('fractional_300k', 'total_pnl') or 0):,.0f}
fractional_300k_pf: {fractional300_pf}

Research vs old Live Limited at fixed_300k:
Research_Audit_ALL_trades: {len(primary_rows)}
Research_Audit_ALL_total_pnl: {(population_value('Research_Audit_ALL', 'fixed_300k', 'total_pnl') or 0):,.0f}
Research_Audit_ALL_avg_return_pct: {population_value('Research_Audit_ALL', 'fixed_300k', 'avg_return_pct')}
Historical_Live_Limited_Selected_trades: {len(selected_rows)}
Historical_Live_Limited_Selected_total_pnl: {(population_value('Historical_Live_Limited_Selected', 'fixed_300k', 'total_pnl') or 0):,.0f}
Historical_Live_Limited_Selected_avg_return_pct: {population_value('Historical_Live_Limited_Selected', 'fixed_300k', 'avg_return_pct')}
Historical_Not_Selected_trades: {len(not_selected_rows)}
Historical_Not_Selected_total_pnl: {(population_value('Historical_Not_Selected', 'fixed_300k', 'total_pnl') or 0):,.0f}
Historical_Not_Selected_avg_return_pct: {population_value('Historical_Not_Selected', 'fixed_300k', 'avg_return_pct')}

Benchmark:
benchmark_unavailable unless --market-benchmark is supplied.

Recommendation note:
This script compares 100-share distortion against fixed-notional and S-share style sizing.
For the broad historical Research population, fixed-notional and fractional sizing preserve the
percentage-return edge more cleanly than 100-share sizing, while required capital becomes large
if every historical H5 signal is taken. It is analysis only; Primary, H5 rules, DB definitions,
UI, LINE, and actual_trade_logs were not changed.
"""
    write_text(outdir / "00_input_dataset_summary.txt", summary_text)
    write_text(outdir / "19_h5_primary_fractional_sizing_report.txt", summary_text)

    print(summary_text.strip())


if __name__ == "__main__":
    main()
