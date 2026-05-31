"""Generate and, if possible, create the model_predictions table."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.trade_case_tester import _build_supabase


DDL = """
create table if not exists model_predictions (
    id bigserial primary key,

    code text not null,
    trade_date date not null,

    model_key text not null,
    model_version text not null,

    prediction_date timestamp with time zone not null default now(),
    created_at timestamp with time zone not null default now(),

    signal_probability double precision not null,
    signal_stage text,
    prediction_label text,

    feature_snapshot_trade_date date,
    feature_snapshot_id bigint,

    feature_hash text,
    feature_version text,

    source text not null default 'daily_prediction',

    metadata jsonb not null default '{}'::jsonb,

    is_active boolean not null default true,

    unique (code, trade_date, model_key, model_version)
);

create index if not exists idx_model_predictions_trade_date
on model_predictions (trade_date);

create index if not exists idx_model_predictions_code_trade_date
on model_predictions (code, trade_date);

create index if not exists idx_model_predictions_model_key_version
on model_predictions (model_key, model_version);

create index if not exists idx_model_predictions_prediction_date
on model_predictions (prediction_date);

create index if not exists idx_model_predictions_active
on model_predictions (is_active);
""".strip()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="outputs/h5_stored_prediction_infrastructure")
    parser.add_argument("--execute", action="store_true", help="Try Supabase RPC exec_sql. SQL Editor remains the fallback.")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    write_text(output_dir / "01_model_predictions_schema.sql", DDL)

    lines = [
        "# model_predictions table create result",
        f"created_at: {datetime.now(timezone.utc).isoformat()}",
        "sql_file: outputs/h5_stored_prediction_infrastructure/01_model_predictions_schema.sql",
    ]
    if not args.execute:
        lines += [
            "execute_attempted: false",
            "result: SQL generated only.",
            "next: Run the SQL file in Supabase SQL Editor, or rerun with --execute if an exec_sql RPC exists.",
        ]
    else:
        try:
            sb = _build_supabase()
            sb.rpc("exec_sql", {"sql": DDL}).execute()
            lines += ["execute_attempted: true", "result: PASS"]
        except Exception as exc:
            lines += [
                "execute_attempted: true",
                "result: RPC execution unavailable or failed.",
                f"error: {exc}",
                "next: Run 01_model_predictions_schema.sql in Supabase SQL Editor.",
            ]
    write_text(output_dir / "02_model_predictions_table_create_result.txt", "\n".join(lines))


if __name__ == "__main__":
    main()
