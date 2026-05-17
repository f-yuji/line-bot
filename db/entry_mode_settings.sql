-- Entry-mode settings and optional audit columns.
-- Safe to run multiple times.

alter table if exists strategy_settings
  add column if not exists entry_mode text default 'normal',
  add column if not exists entry_mode_updated_at timestamptz,
  add column if not exists entry_mode_note text;

alter table if exists strategy_settings
  drop constraint if exists strategy_settings_entry_mode_check;

alter table if exists strategy_settings
  add constraint strategy_settings_entry_mode_check
  check (entry_mode in ('auto', 'normal', 'risk_on_pullback', 'panic_deep_rebound', 'paused'));

alter table if exists virtual_trades
  add column if not exists entry_mode_used text,
  add column if not exists entry_mode_reason text,
  add column if not exists recommended_entry_mode text,
  add column if not exists entry_ma5_gap_pct numeric,
  add column if not exists entry_ma25_gap_pct numeric,
  add column if not exists entry_ma75_gap_pct numeric,
  add column if not exists entry_case text;

alter table if exists stock_drop_watchlist
  add column if not exists entry_mode_used text,
  add column if not exists entry_mode_reason text,
  add column if not exists recommended_entry_mode text,
  add column if not exists entry_ma5_gap_pct numeric,
  add column if not exists entry_ma25_gap_pct numeric,
  add column if not exists entry_ma75_gap_pct numeric,
  add column if not exists entry_case text;
