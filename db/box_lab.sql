-- box_lab initial schema
--
-- Shared market data remains in existing tables:
-- stock_feature_snapshots, market_regime, long_term_market_regime, financials,
-- sector data, and price data.
--
-- box_lab stores only strategy-specific state.

create table if not exists box_settings (
  id uuid primary key default gen_random_uuid(),
  user_id text not null default 'global',
  entry_mode text not null default 'normal',
  box_width_pct numeric default 12.0,
  signal_box_position_pct numeric default 45.0,
  max_pending_days integer default 5,
  atr_max_pct numeric default 4.0,
  gu_skip_pct numeric default 3.0,
  gd_skip_pct numeric default 5.0,
  max_open_positions integer default 5,
  max_sector_positions integer default 2,
  min_turnover_value numeric default 1000000000,
  min_price numeric default 1000,
  min_equity_ratio numeric default 30,
  max_per numeric default 40,
  max_pbr numeric default 5,
  note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id)
);

create table if not exists box_signals (
  id uuid primary key default gen_random_uuid(),
  trade_date date not null,
  code text not null,
  name text,
  sector text,
  status text not null default 'signal_generated',
  entry_status text not null default 'signal_generated',
  signal_generated_at timestamptz default now(),
  entry_pending_at timestamptz,
  entered_at timestamptz,
  closed_at timestamptz,
  box_upper numeric,
  box_lower numeric,
  box_days integer,
  bounce_count integer,
  atr_pct numeric,
  close numeric,
  entry_target_price numeric,
  entry_price_min numeric,
  entry_price_max numeric,
  entry_skip_gu_pct numeric default 3.0,
  entry_skip_gd_pct numeric default 5.0,
  entry_reason text,
  entry_mode text,
  short_market_regime text,
  long_market_regime text,
  ma5_gap_pct numeric,
  ma25_gap_pct numeric,
  rsi14 numeric,
  volume_ratio_20d numeric,
  margin_ratio numeric,
  margin_date date,
  operating_cf numeric,
  per numeric,
  pbr numeric,
  equity_ratio numeric,
  virtual_trade_id uuid,
  skip_reason text,
  raw jsonb default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (trade_date, code)
);

create index if not exists idx_box_signals_status_date on box_signals (entry_status, trade_date desc);
create index if not exists idx_box_signals_code_date on box_signals (code, trade_date desc);

create table if not exists box_virtual_trades (
  id uuid primary key default gen_random_uuid(),
  signal_id uuid references box_signals(id) on delete set null,
  code text not null,
  name text,
  sector text,
  status text not null default 'open',
  buy_date timestamptz,
  buy_price numeric,
  quantity integer default 100,
  sell_date timestamptz,
  sell_price numeric,
  profit_loss numeric,
  profit_loss_pct numeric,
  current_price numeric,
  unrealized_pnl numeric,
  unrealized_pnl_pct numeric,
  entry_reason text,
  entry_mode text,
  exit_reason text,
  box_upper numeric,
  box_lower numeric,
  highest_close numeric,
  highest_close_at timestamptz,
  raw jsonb default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_box_virtual_trades_status on box_virtual_trades (status, buy_date desc);
create index if not exists idx_box_virtual_trades_code on box_virtual_trades (code, buy_date desc);

insert into box_settings (user_id)
values ('global')
on conflict (user_id) do nothing;

alter table box_settings add column if not exists signal_box_position_pct numeric default 45.0;
alter table box_settings add column if not exists max_pending_days integer default 5;
alter table box_settings alter column box_width_pct set default 12.0;
alter table box_settings alter column signal_box_position_pct set default 45.0;
alter table box_settings alter column max_pending_days set default 5;

-- Current box_lab baseline from long portfolio backtest:
-- signal_box_position_pct=45, max_pending_days=5, ideal box width=12.
update box_settings
set
  box_width_pct = 12.0,
  signal_box_position_pct = 45.0,
  max_pending_days = 5,
  updated_at = now()
where user_id = 'global';

create table if not exists box_watchlist (
  id uuid primary key default gen_random_uuid(),
  trade_date date not null,
  code text not null,
  name text,
  sector text,
  status text not null default 'watching',
  strategy_type text not null default 'box_pullback',
  close numeric,
  box_high numeric,
  box_low numeric,
  box_width_pct numeric,
  box_position_pct numeric,
  box_days integer,
  bounce_count integer,
  watch_score numeric,
  watch_reason text,
  signal_status text default 'watching',
  atr_pct numeric,
  ma5_gap_pct numeric,
  ma25_gap_pct numeric,
  ma75_gap_pct numeric,
  rsi14 numeric,
  volume_ratio_20d numeric,
  turnover_value numeric,
  per numeric,
  pbr numeric,
  equity_ratio numeric,
  warnings text,
  raw jsonb default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (trade_date, code)
);

create index if not exists idx_box_watchlist_status_score
  on box_watchlist (status, watch_score desc, trade_date desc);

create index if not exists idx_box_watchlist_code_date
  on box_watchlist (code, trade_date desc);

alter table box_signals add column if not exists strategy_type text;
alter table box_signals add column if not exists box_high numeric;
alter table box_signals add column if not exists box_low numeric;
alter table box_signals add column if not exists box_width_pct numeric;
alter table box_signals add column if not exists box_position_pct numeric;
alter table box_signals add column if not exists box_score numeric;
alter table box_signals add column if not exists signal_reason text;
alter table box_signals add column if not exists stop_loss_price numeric;
alter table box_signals add column if not exists take_profit_price numeric;
alter table box_signals add column if not exists warnings text;
alter table box_signals add column if not exists market_support_status text;
alter table box_signals add column if not exists nikkei_trend_warning text;
alter table box_signals add column if not exists relative_strength_vs_nikkei numeric;
alter table box_watchlist add column if not exists equity_ratio numeric;
alter table box_watchlist add column if not exists market_support_status text;
alter table box_watchlist add column if not exists nikkei_trend_warning text;
alter table box_watchlist add column if not exists relative_strength_vs_nikkei numeric;

create index if not exists idx_box_signals_strategy_score
  on box_signals (strategy_type, box_score desc, trade_date desc);

alter table box_watchlist add column if not exists margin_ratio numeric;
alter table box_watchlist add column if not exists margin_date date;
alter table box_watchlist add column if not exists margin_buy_balance numeric;
alter table box_watchlist add column if not exists margin_sell_balance numeric;

alter table box_signals add column if not exists margin_buy_balance numeric;
alter table box_signals add column if not exists margin_sell_balance numeric;
