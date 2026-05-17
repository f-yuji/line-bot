create table if not exists long_term_market_regime (
  trade_date date primary key,
  regime text not null,
  label text,
  score numeric,
  nikkei_above_200ma boolean,
  topix_above_200ma boolean,
  nikkei_200ma_gap_pct numeric,
  topix_200ma_gap_pct numeric,
  ma25_above_ratio numeric,
  ma75_above_ratio numeric,
  advancers_ratio numeric,
  decliners_ratio numeric,
  vix numeric,
  vix_change_pct numeric,
  reasons jsonb default '[]'::jsonb,
  metrics jsonb default '{}'::jsonb,
  created_at timestamptz default now()
);

create index if not exists idx_long_term_market_regime_trade_date
  on long_term_market_regime (trade_date desc);

create index if not exists idx_long_term_market_regime_regime
  on long_term_market_regime (regime);
