create table if not exists trade_assist_candidate_history (
  id uuid primary key default gen_random_uuid(),
  trade_date date not null,
  code text not null,
  name text,
  sector text,
  source_kind text,
  signal_stage text,
  display_status text,
  entry_price numeric,
  stop_loss_price numeric,
  risk_100 numeric,
  ai_score numeric,
  signal_probability numeric,
  expected_value numeric,
  drop_pct numeric,
  rsi14 numeric,
  volume_ratio_20d numeric,
  margin_ratio numeric,
  margin_date date,
  entry_case text,
  entry_mode_used text,
  recommended_entry_mode text,
  entry_ma5_gap_pct numeric,
  entry_ma25_gap_pct numeric,
  entry_ma75_gap_pct numeric,
  feature_snapshot_id text,
  watchlist_id text,
  virtual_trade_id text,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (trade_date, code, source_kind)
);

create index if not exists idx_trade_assist_candidate_history_trade_date
  on trade_assist_candidate_history (trade_date desc);

create index if not exists idx_trade_assist_candidate_history_code
  on trade_assist_candidate_history (code);
