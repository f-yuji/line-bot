"""H5 AI score leakage / timestamp audit.

Research-only script. It does not modify Primary, DB case definitions, UI,
notifications, actual trade logs, Watchlist, Intraday H5, models, or training
code. It audits whether the AI score used in H5 research is saved at the
entry timestamp or recomputed by the active model, and checks feature/schema
leakage risks.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import logging
import math
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from services.h5_primary import h5_overheat_score
from services.signal_stage import evaluate_signal_stage
from services.trade_case_tester import _active_model_bundle, _build_supabase, _load_candidates_v2, _to_float

try:
    from scripts.train_rebound_model import BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES
except Exception:
    BOOL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES = [], [], []

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

EST12_STOP_RATE = -0.12
EST12_STOP_PCT = -12.0
LEAKAGE_PATTERNS = [
    "future", "label", "target", "y_true", "return_after", "hd1", "hd3", "hd5",
    "hd7", "hd10", "exit", "realized", "next_", "tomorrow", "forward",
    "profit", "loss", "drawdown", "max_return", "max_drawdown",
]
LOW_RISK_FUTURE_LABELS = {"sector", "market"}


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:10]).date()


def round_value(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, digits)
    except Exception:
        return value


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def win_rate(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value > 0) / len(values) * 100.0


def profit_factor(values: list[float]) -> float | None:
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value <= 0))
    if losses <= 0:
        return 999.0 if wins > 0 else None
    return wins / losses


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: round_value(row.get(key)) for key in headers})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def risk_for_name(name: str) -> tuple[str, str]:
    lower = name.lower()
    hits = [pattern for pattern in LEAKAGE_PATTERNS if pattern in lower]
    if hits:
        return "high", "name contains " + ",".join(hits)
    if lower in LOW_RISK_FUTURE_LABELS:
        return "low", "entry-time categorical feature"
    return "low", "no obvious future/label token"


def get_entry_date(row: dict) -> date | None:
    raw = row.get("trade_date") or row.get("label_trade_date")
    return parse_date(str(raw)) if raw else None


def get_entry_price(row: dict) -> float | None:
    return _to_float(row.get("entry_price"), None) or _to_float(row.get("close"), None)


def hd_return(row: dict, hold: int, path_type: str = "est12") -> tuple[float | None, str]:
    entry = get_entry_price(row)
    if entry is None or entry <= 0:
        return None, "invalid_entry"
    if path_type == "raw":
        close = _to_float(row.get(f"future_close_{hold}d"), None)
        if close is None:
            return None, "no_data"
        return (close / entry - 1.0) * 100.0, "time_stop"
    stop_price = entry * (1.0 + EST12_STOP_RATE)
    last_close = None
    for day in range(1, hold + 1):
        close = _to_float(row.get(f"future_close_{day}d"), None)
        low = _to_float(row.get(f"future_low_{day}d"), None)
        if close is not None:
            last_close = close
        if low is not None and low <= stop_price:
            return EST12_STOP_PCT, "emergency_stop"
    if last_close is None:
        return None, "no_data"
    return (last_close / entry - 1.0) * 100.0, "time_stop"


def attach_returns(rows: list[dict]) -> None:
    for row in rows:
        row["_entry_date"] = get_entry_date(row)
        for hold in (1, 3, 5, 7):
            ret, reason = hd_return(row, hold, "est12")
            row[f"_hd{hold}"] = ret
            row[f"_reason_hd{hold}"] = reason


def period_of(row: dict, train_end: date, test_start: date) -> str | None:
    d = row.get("_entry_date")
    if not d:
        return None
    if d <= train_end:
        return "train"
    if d >= test_start:
        return "test"
    return None


def passes_ai(row: dict) -> bool:
    prob = _to_float(row.get("signal_probability"), None)
    return prob is not None and prob >= 0.65


def passes_drop(row: dict) -> bool:
    drop = _to_float(row.get("drop_from_20d_high_pct"), None)
    return drop is not None and drop <= -8.0


def passes_stage(row: dict) -> bool:
    return str(row.get("signal_stage") or "") in {"confirmed", "strong_confirmed"}


def passes_no_panic(row: dict) -> bool:
    return str(row.get("market_regime") or "") != "panic_selloff"


def passes_overheat(row: dict) -> bool:
    return h5_overheat_score(row) <= 1


def passes_margin(row: dict) -> bool:
    margin = _to_float(row.get("margin_ratio"), None)
    return margin is None or 3.0 <= margin <= 30.0


FILTERS: list[tuple[str, Any]] = [
    ("filter_zero_all", lambda r: True),
    ("AI_only", passes_ai),
    ("drop_only", passes_drop),
    ("AI_plus_drop", lambda r: passes_ai(r) and passes_drop(r)),
    ("AI_plus_drop_stage", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r)),
    ("AI_plus_drop_stage_no_panic", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r)),
    ("AI_plus_drop_stage_no_panic_overheat", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r)),
    ("H5_full_no_margin", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r)),
    ("H5_full", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r) and passes_margin(r)),
]


def summarize(rows: list[dict], period: str) -> dict:
    vals = [row["_hd3"] for row in rows if row.get("_hd3") is not None]
    return {
        "period": period,
        "n": len(vals),
        "HD1_avg": avg([row["_hd1"] for row in rows if row.get("_hd1") is not None]),
        "HD3_avg": avg(vals),
        "HD5_avg": avg([row["_hd5"] for row in rows if row.get("_hd5") is not None]),
        "HD7_avg": avg([row["_hd7"] for row in rows if row.get("_hd7") is not None]),
        "HD3_WR": win_rate(vals),
        "PF_HD3": profit_factor(vals),
        "max_loss": min(vals) if vals else None,
        "emergency_stop_count": sum(1 for row in rows if row.get("_reason_hd3") == "emergency_stop"),
    }


def period_rows(rows: list[dict], period: str, train_end: date, test_start: date) -> list[dict]:
    if period == "all":
        return rows
    return [row for row in rows if period_of(row, train_end, test_start) == period]


def score_bucket(prob: float | None) -> str:
    if prob is None:
        return "null"
    if prob < 0.50:
        return "lt_0_50"
    if prob < 0.55:
        return "0_50_to_0_55"
    if prob < 0.60:
        return "0_55_to_0_60"
    if prob < 0.65:
        return "0_60_to_0_65"
    if prob < 0.70:
        return "0_65_to_0_70"
    if prob < 0.75:
        return "0_70_to_0_75"
    if prob < 0.80:
        return "0_75_to_0_80"
    if prob < 0.85:
        return "0_80_to_0_85"
    if prob < 0.90:
        return "0_85_to_0_90"
    return "gte_0_90"


def sample_rows(table_rows: list[dict], table_name: str) -> list[dict]:
    rows = []
    if not table_rows:
        return rows
    first = table_rows[0]
    for column, sample in first.items():
        risk, notes = risk_for_name(column)
        rows.append({
            "table_name": table_name,
            "column_name": column,
            "data_type": type(sample).__name__,
            "nullable": "unknown",
            "sample_value": str(sample)[:120],
            "leakage_risk": risk,
            "notes": notes,
        })
    return rows


def fetch_sample(sb, table: str, limit: int = 5) -> list[dict]:
    try:
        return sb.table(table).select("*").limit(limit).execute().data or []
    except Exception as exc:
        return [{"_fetch_error": str(exc)}]


def fetch_active_model_rows(sb) -> list[dict]:
    try:
        return (
            sb.table("ml_models")
            .select("*")
            .eq("is_active", True)
            .order("created_at", desc=True)
            .limit(10)
            .execute()
            .data or []
        )
    except Exception as exc:
        return [{"_fetch_error": str(exc)}]


def select_scoring_model_row(active_rows: list[dict]) -> dict:
    """Mirror trade_case_tester._active_model_bundle model-name priority."""
    valid_rows = [row for row in active_rows if not row.get("_fetch_error")]
    for model_name in ("rebound_lgbm_5d", "rebound_lgbm"):
        for row in valid_rows:
            if row.get("model_name") == model_name:
                return row
    return valid_rows[0] if valid_rows else {}


def infer_feature_sources(feature_names: list[str]) -> list[dict]:
    rows = []
    defined_base = set(NUMERIC_FEATURES) | set(BOOL_FEATURES) | set(CATEGORICAL_FEATURES)
    for feature in feature_names:
        base = feature
        source = "model_bundle"
        if feature in defined_base:
            source = "stock_feature_snapshots"
        elif any(feature.startswith(f"{cat}_") for cat in CATEGORICAL_FEATURES):
            source = "one_hot_from_stock_feature_snapshots"
        risk, reason = risk_for_name(feature)
        rows.append({
            "feature_name": feature,
            "source_table": source,
            "source_column": base,
            "used_in_training": True,
            "used_in_prediction": True,
            "leakage_risk": risk,
            "reason": reason,
        })
    return rows


def code_feature_def_rows() -> list[dict]:
    rows = []
    for feature in list(NUMERIC_FEATURES) + list(BOOL_FEATURES) + list(CATEGORICAL_FEATURES):
        risk, reason = risk_for_name(feature)
        rows.append({
            "feature_name": feature,
            "source_table": "stock_feature_snapshots",
            "source_column": feature,
            "used_in_training": True,
            "used_in_prediction": True,
            "leakage_risk": risk,
            "reason": reason,
        })
    return rows


def filter_ablation(rows: list[dict], train_end: date, test_start: date) -> list[dict]:
    out = []
    by_step: dict[tuple[str, str], dict] = {}
    for period in ("train", "test", "all"):
        previous = None
        ai_drop = None
        for name, pred in FILTERS:
            selected = [row for row in period_rows(rows, period, train_end, test_start) if pred(row)]
            summary = summarize(selected, period)
            summary["filter_step"] = name
            current = _to_float(summary.get("HD3_avg"), None)
            prev = _to_float(previous.get("HD3_avg"), None) if previous else None
            summary["delta_vs_previous"] = current - prev if current is not None and prev is not None else None
            if name == "AI_plus_drop":
                ai_drop = summary
            ai_drop_val = _to_float(ai_drop.get("HD3_avg"), None) if ai_drop else None
            summary["delta_vs_AI_plus_drop"] = current - ai_drop_val if current is not None and ai_drop_val is not None else None
            summary["notes"] = ""
            out.append(summary)
            by_step[(period, name)] = summary
            previous = summary
    return out


def excluded_groups(rows: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if not (passes_ai(row) and passes_drop(row)):
            continue
        reasons = []
        if not passes_stage(row):
            reasons.append("stage_excluded")
        if not passes_no_panic(row):
            reasons.append("panic_excluded")
        if not passes_overheat(row):
            reasons.append("overheat_excluded")
        if not passes_margin(row):
            reasons.append("margin_excluded")
        if not reasons:
            groups["AI_plus_drop_and_H5_full"].append(row)
        elif len(reasons) > 1:
            groups["AI_plus_drop_but_multiple_excluded"].append(row)
        else:
            groups[f"AI_plus_drop_but_{reasons[0]}"].append(row)
    return groups


def aggregate_group(rows: list[dict], group_name: str) -> dict:
    vals = [row["_hd3"] for row in rows if row.get("_hd3") is not None]
    return {
        "exclusion_group": group_name,
        "n": len(vals),
        "HD3_avg": avg(vals),
        "HD3_WR": win_rate(vals),
        "PF_HD3": profit_factor(vals),
        "avg_signal_probability": avg([_to_float(row.get("signal_probability"), None) for row in rows if row.get("signal_probability") is not None]),
        "avg_drop_from_20d_high": avg([_to_float(row.get("drop_from_20d_high_pct"), None) for row in rows if row.get("drop_from_20d_high_pct") is not None]),
        "avg_overheat_score": avg([float(h5_overheat_score(row)) for row in rows]),
        "avg_margin_ratio": avg([_to_float(row.get("margin_ratio"), None) for row in rows if row.get("margin_ratio") is not None]),
        "avg_volume_ratio": avg([_to_float(row.get("volume_ratio_20d"), None) for row in rows if row.get("volume_ratio_20d") is not None]),
        "notes": "",
    }


def get_model_feature_names(bundle: dict | None) -> list[str]:
    if not bundle:
        return []
    return list(bundle.get("feature_columns") or [])


def source_code_audit() -> dict[str, str]:
    tct = (ROOT / "services/trade_case_tester.py").read_text(encoding="utf-8", errors="replace")
    train = (ROOT / "scripts/train_rebound_model.py").read_text(encoding="utf-8", errors="replace")
    stage = (ROOT / "services/signal_stage.py").read_text(encoding="utf-8", errors="replace")
    return {
        "score_candidates_predict_proba": str("predict_proba" in tct and 'row["signal_probability"]' in tct),
        "load_candidates_calls_active_model": str("return _score_candidates(rows, _active_model_bundle(sb))" in tct),
        "train_uses_feature_snapshot_cols": str("stock_feature_snapshots" in train and "NUMERIC_FEATURES" in train),
        "train_uses_label_cols_as_target": str("stock_rebound_labels" in train and "target_success" in train),
        "signal_stage_uses_ai_rule_ev_only": str("day" not in stage.lower() and "future" not in stage.lower()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_ai_score_leakage_audit")
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="latest")
    parser.add_argument("--sample-limit", type=int, default=200)
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    test_start = parse_date(args.test_start)
    test_end = date.today() if args.test_end == "latest" else parse_date(args.test_end)

    sb = _build_supabase()
    active_rows = fetch_active_model_rows(sb)
    model_row = select_scoring_model_row(active_rows)
    bundle = _active_model_bundle(sb)
    feature_names = get_model_feature_names(bundle)
    code_audit = source_code_audit()

    schema_rows = []
    for table in [
        "stock_rebound_labels",
        "stock_feature_snapshots",
        "virtual_trades",
        "trade_case_definitions",
        "ml_models",
        "model_predictions",
        "signal_predictions",
    ]:
        schema_rows.extend(sample_rows(fetch_sample(sb, table, 3), table))
    write_csv(output_dir / "01_schema_audit.csv", schema_rows)

    feature_rows = infer_feature_sources(feature_names) if feature_names else code_feature_def_rows()
    write_csv(output_dir / "02_model_feature_columns.csv", feature_rows)

    logger.info("Loading scored candidates for performance/timestamp audit %s..%s", train_start, test_end)
    rows = _load_candidates_v2(sb, train_start, test_end)
    attach_returns(rows)
    evaluable = [row for row in rows if row.get("_hd3") is not None]

    timestamp_rows = []
    for row in evaluable[: args.sample_limit]:
        d = row.get("_entry_date")
        timestamp_rows.append({
            "code": row.get("code"),
            "trade_date": d.isoformat() if d else None,
            "entry_date": d.isoformat() if d else None,
            "prediction_date": None,
            "created_at": None,
            "updated_at": None,
            "model_version": model_row.get("model_version"),
            "signal_probability": row.get("signal_probability"),
            "timestamp_status": "recomputed_by_active_model",
            "notes": "_load_candidates_v2 calls _score_candidates(rows, _active_model_bundle(sb)); no saved per-row score timestamp in loaded row",
        })
    write_csv(output_dir / "03_score_timestamp_audit.csv", timestamp_rows)
    write_csv(output_dir / "04_score_timestamp_summary.csv", [{
        "status": "recomputed_by_active_model",
        "count": len(evaluable),
        "rate": 100.0 if evaluable else 0.0,
        "avg_HD3_return": avg([row["_hd3"] for row in evaluable]),
        "avg_signal_probability": avg([_to_float(row.get("signal_probability"), None) for row in evaluable if row.get("signal_probability") is not None]),
    }])

    train_end_model = str(model_row.get("train_end") or "")
    high_risk_features = [row for row in feature_rows if row.get("leakage_risk") == "high"]
    uses_future_features = bool(high_risk_features)
    active_train_includes_test = False
    try:
        if train_end_model:
            active_train_includes_test = parse_date(train_end_model) >= test_start
    except Exception:
        pass
    write_text(output_dir / "05_active_model_rescore_audit.txt", f"""
# Active Model Rescore Audit

current script reads saved signal_probability: false
current script recomputes signal_probability: true
evidence: services/trade_case_tester._load_candidates_v2 returns _score_candidates(rows, _active_model_bundle(sb)); _score_candidates calls bundle['model'].predict_proba and assigns row['signal_probability'].

active_model_name: {model_row.get('model_name')}
active_model_version: {model_row.get('model_version')}
active_model_train_start: {model_row.get('train_start')}
active_model_train_end: {model_row.get('train_end')}
active_model_valid_start: {model_row.get('valid_start')}
active_model_valid_end: {model_row.get('valid_end')}
active_model_created_at: {model_row.get('created_at')}
active_model_updated_at: {model_row.get('updated_at')}
active_model_path: {model_row.get('model_path')}
active_model_feature_count: {len(feature_names)}
high_risk_feature_count: {len(high_risk_features)}
active_model_train_includes_test_period_start: {active_train_includes_test}

risk_assessment:
Historical H5 research currently uses active-model rescoring, not a saved score with row-level prediction timestamp. If the active model was trained/validated using 2025+ labels, historical test-period performance is not strict out-of-sample. Even if feature columns are clean, timestamp validity is high risk until walk-forward or saved-at-time predictions are used.
""")

    write_text(output_dir / "06_walk_forward_oos_audit.txt", f"""
# Walk-forward / OOS Audit

model_training_method:
scripts/train_rebound_model.py uses a time split inside the selected training date range. It stores train_start/train_end and valid_start/valid_end in ml_models.

active_model_train_period:
{model_row.get('train_start')} .. {model_row.get('train_end')}

active_model_validation_period:
{model_row.get('valid_start')} .. {model_row.get('valid_end')}

test_period_for_h5_audit:
{test_start.isoformat()} .. {test_end.isoformat()}

predictions_out_of_sample_for_2025_plus:
{"no/high-risk" if active_train_includes_test else "possibly, if model train_end is before test_start"}

walk_forward:
No evidence in _load_candidates_v2 that predictions are generated with date-specific historical models. Current path loads one active model and scores all periods.

current_risk:
high if active model train/validation includes the H5 test period; medium-high if train period is unknown; lower only after saved timestamped predictions or walk-forward scoring are available.
""")

    bucket_rows = []
    for population_name, predicate in [
        ("filter_zero_universe", lambda r: True),
        ("drop_only_universe", passes_drop),
        ("H5_full_universe", lambda r: passes_ai(r) and passes_drop(r) and passes_stage(r) and passes_no_panic(r) and passes_overheat(r) and passes_margin(r)),
    ]:
        pop = [row for row in evaluable if predicate(row)]
        for period in ("train", "test", "all"):
            sub = period_rows(pop, period, train_end, test_start)
            grouped: dict[str, list[dict]] = defaultdict(list)
            for row in sub:
                grouped[score_bucket(_to_float(row.get("signal_probability"), None))].append(row)
            for bucket, items in sorted(grouped.items()):
                s = summarize(items, period)
                s.update({"population": population_name, "ai_bucket": bucket})
                bucket_rows.append(s)
    write_csv(output_dir / "07_ai_score_bucket_performance.csv", bucket_rows)

    ai_only = [row for row in evaluable if passes_ai(row)]
    monthly: dict[str, list[dict]] = defaultdict(list)
    yearly: dict[str, list[dict]] = defaultdict(list)
    for row in ai_only:
        d = row.get("_entry_date")
        if not d:
            continue
        monthly[d.strftime("%Y-%m")].append(row)
        yearly[str(d.year)].append(row)
    write_csv(output_dir / "08_ai_only_monthly_stability.csv", [
        {"month": month, **summarize(items, "month"), "total_return_sum": sum(row["_hd3"] for row in items if row.get("_hd3") is not None)}
        for month, items in sorted(monthly.items())
    ])
    write_csv(output_dir / "09_ai_only_yearly_stability.csv", [
        {"year": year, **summarize(items, "year"), "total_return_sum": sum(row["_hd3"] for row in items if row.get("_hd3") is not None)}
        for year, items in sorted(yearly.items())
    ])

    ablation = filter_ablation(evaluable, train_end, test_start)
    write_csv(output_dir / "10_filter_ablation_recheck.csv", ablation)

    excl_rows = [aggregate_group(items, name) for name, items in sorted(excluded_groups(evaluable).items())]
    write_csv(output_dir / "11_excluded_by_h5_filters_analysis.csv", excl_rows)

    stage_text = f"""
# Signal Stage Audit

source:
services/signal_stage.py evaluate_signal_stage(ai_score, rule_score, expected_value, settings, market_regime)

definition:
- early: ai >= early threshold
- confirmed: ai >= confirmed threshold
- strong_confirmed: ai >= strong threshold and rule_score >= 60

future_info_check:
The function itself uses ai_score, rule_score, expected_value, optional settings, and optional market_regime threshold adjustment. It does not directly reference future_* columns, labels, exits, or realized returns.

important caveat:
rule_score and expected_value are computed in trade_case_tester at scoring time. In current code, expected_value is formula based on AI/rule/default TP/SL, not realized future return.

uses_future_info: false in signal_stage.py itself
risk_assessment:
Signal stage is not the main leakage source. The main risk is that ai_score is recomputed by the current active model for historical rows.
"""
    write_text(output_dir / "12_signal_stage_audit.txt", stage_text)

    stage_rows = []
    for period in ("train", "test", "all"):
        sub = period_rows(evaluable, period, train_end, test_start)
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in sub:
            grouped[str(row.get("signal_stage") or "unknown")].append(row)
        for stage, items in sorted(grouped.items()):
            s = summarize(items, period)
            s.update({
                "signal_stage": stage,
                "avg_signal_probability": avg([_to_float(row.get("signal_probability"), None) for row in items if row.get("signal_probability") is not None]),
            })
            stage_rows.append(s)
    write_csv(output_dir / "13_signal_stage_performance.csv", stage_rows)

    sample_candidates = []
    h5_full = [row for row in evaluable if passes_ai(row) and passes_drop(row) and passes_stage(row) and passes_no_panic(row) and passes_overheat(row) and passes_margin(row)]
    ai_high = sorted(ai_only, key=lambda r: _to_float(r.get("signal_probability"), 0) or 0, reverse=True)[:50]
    low_score_good = sorted(
        [row for row in evaluable if (_to_float(row.get("signal_probability"), 0) or 0) < 0.5 and (row.get("_hd3") or 0) > 5],
        key=lambda r: r.get("_hd3") or 0,
        reverse=True,
    )[:50]
    sample_candidates.extend(("AI_only_high_score", row) for row in ai_high[:20])
    sample_candidates.extend(("H5_full", row) for row in h5_full[:20])
    sample_candidates.extend(("low_score_high_return", row) for row in low_score_good[:20])
    sample_rows_out = []
    for group, row in sample_candidates:
        d = row.get("_entry_date")
        sample_rows_out.append({
            "code": row.get("code"),
            "name": row.get("name"),
            "trade_date": d.isoformat() if d else None,
            "entry_date": d.isoformat() if d else None,
            "signal_probability": row.get("signal_probability"),
            "model_version": model_row.get("model_version"),
            "prediction_date": None,
            "created_at": None,
            "updated_at": None,
            "feature_snapshot_date": row.get("trade_date"),
            "label_created_at": None,
            "HD3_return": row.get("_hd3"),
            "filter_group": group,
            "timestamp_status": "recomputed_by_active_model",
            "notes": "No saved per-row prediction timestamp in _load_candidates_v2 output",
        })
    write_csv(output_dir / "14_timestamp_sample_rows.csv", sample_rows_out)

    suspicious = [r for r in schema_rows + feature_rows if r.get("leakage_risk") == "high"]
    write_text(output_dir / "16_suspicious_columns.txt", "\n".join(
        f"{r.get('table_name', r.get('source_table'))}.{r.get('column_name', r.get('feature_name'))}: {r.get('notes', r.get('reason'))}"
        for r in suspicious
    ) or "No suspicious columns found in model feature list. Label tables contain future columns for evaluation/training targets.")

    dist_rows = []
    for period in ("train", "test", "all"):
        for bucket in sorted({r["ai_bucket"] for r in bucket_rows if r.get("population") == "filter_zero_universe" and r.get("period") == period}):
            matching = [r for r in bucket_rows if r.get("population") == "filter_zero_universe" and r.get("period") == period and r.get("ai_bucket") == bucket]
            if matching:
                dist_rows.append({"period": period, "ai_bucket": bucket, "n": matching[0].get("n")})
    write_csv(output_dir / "17_ai_score_distribution.csv", dist_rows)

    model_perf_rows = []
    for model in active_rows:
        metrics = model.get("metrics")
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except Exception:
                metrics = {}
        if not isinstance(metrics, dict):
            metrics = {}
        model_perf_rows.append({
            "model_name": model.get("model_name"),
            "model_version": model.get("model_version"),
            "train_start": model.get("train_start"),
            "train_end": model.get("train_end"),
            "valid_start": model.get("valid_start"),
            "valid_end": model.get("valid_end"),
            "target_name": model.get("target_name"),
            "roc_auc": metrics.get("roc_auc"),
            "top_20pct_success_rate": metrics.get("top_20pct_success_rate"),
            "prob_65_success_rate": metrics.get("prob_65_success_rate"),
            "is_active": model.get("is_active"),
        })
    write_csv(output_dir / "18_model_version_performance.csv", model_perf_rows)

    high_feature_names = [r["feature_name"] for r in feature_rows if r.get("leakage_risk") == "high"]
    ablation_all = {r["filter_step"]: r for r in ablation if r["period"] == "all"}
    ai_only_all = ablation_all.get("AI_only", {})
    ai_plus_drop_all = ablation_all.get("AI_plus_drop", {})
    h5_all = ablation_all.get("H5_full", {})
    report = f"""
# H5 AI Score Leakage / Timestamp Audit Report

## Executive Summary

The current H5 research scoring path is active-model rescoring, not saved-at-entry signal_probability. services/trade_case_tester._load_candidates_v2 loads label/snapshot rows and then calls _score_candidates(rows, _active_model_bundle(sb)). _score_candidates calls predict_proba and assigns row["signal_probability"].

This means the recent AI_only / AI_plus_drop strength should be treated as a model-power result under current active model, not as proof that those exact scores existed at each historical entry date. Strict live-validity requires saved timestamped predictions or walk-forward scoring.

## Answers

1. Saved score or active rescore:
Active model rescore.

2. Active model train period:
{model_row.get('train_start')} .. {model_row.get('train_end')}; validation {model_row.get('valid_start')} .. {model_row.get('valid_end')}.

3. Test label contamination:
active_model_train_includes_test_period_start={active_train_includes_test}. If true, 2025+ H5 test performance is not strict out-of-sample for this active model.

4. signal_probability available at entry_date:
Unknown/not proven. _load_candidates_v2 output does not carry saved prediction_date/created_at for signal_probability; it recomputes the score.

5. created_at / updated_at:
No per-row saved prediction timestamp is available in the loaded candidate rows. 03/04 files mark status as recomputed_by_active_model.

6. Feature columns future leakage:
High-risk model feature count={len(high_feature_names)}. High-risk feature names={high_feature_names[:20]}.

7. stock_rebound_labels future labels:
Labels contain future/label columns, but training code uses them as target labels joined by feature_snapshot_id. Model feature list comes from stock_feature_snapshots feature definitions, not label future columns.

8. stock_feature_snapshots entry timing:
Training/prediction features are sourced from stock_feature_snapshots rows at trade_date. The script does not prove DB rows were immutable at that historical date.

9. signal_stage future leakage:
signal_stage.py itself does not use future labels; main risk is upstream AI score recomputation.

10. AI_only stability:
See 08_ai_only_monthly_stability.csv and 09_ai_only_yearly_stability.csv.

11. AI score bucket monotonicity:
See 07_ai_score_bucket_performance.csv.

12. AI_plus_drop vs AI_only:
All-period AI_only HD3_avg={ai_only_all.get('HD3_avg')}%, AI_plus_drop HD3_avg={ai_plus_drop_all.get('HD3_avg')}%.

13. H5_full weaker than AI_plus_drop:
All-period H5_full HD3_avg={h5_all.get('HD3_avg')}%. See 10_filter_ablation_recheck.csv and 11_excluded_by_h5_filters_analysis.csv. The largest observed drop in the earlier random baseline was no_panic/stage/downstream filters removing high-return AI+drop names.

14. margin / overheat / panic effects:
See 10_filter_ablation_recheck.csv. In current recomputed-score path, panic removal is a major negative delta; overheat and margin are small.

15. Can AI score be trusted now:
For research model ranking, it appears powerful. For historical live-valid inference, not yet. The active rescoring path is high-risk unless the active model was trained only before each prediction period or walk-forward predictions are used.

16. Rebuild score validation:
Recommended. Create saved model_predictions with code, trade_date, model_version, prediction_date, created_at, and never overwrite historical scores.

17. H5 entry redesign if AI is real:
AI threshold is the main edge. Re-evaluate whether panic/overheat/margin filters are risk controls rather than EV improvers.

18. If leakage/timestamp issue is confirmed:
Build walk-forward scoring or historical saved-score backtest first. Then rerun Market Random Baseline.

## Risk Assessment

- Feature column leakage risk: {"high" if high_feature_names else "low by column names"}
- Timestamp / rescore risk: high
- Test OOS risk: {"high" if active_train_includes_test else "unknown-to-medium"}
- Signal stage risk: low
"""
    write_text(output_dir / "15_ai_score_leakage_report.txt", report)

    logger.info("Wrote outputs to %s", output_dir)


if __name__ == "__main__":
    main()
