-- H5 Primary metadata for rebound virtual trades.
-- Additive only: existing trades keep their original exit behavior unless tagged as H5.

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS case_key text,
    ADD COLUMN IF NOT EXISTS case_label text,
    ADD COLUMN IF NOT EXISTS is_primary_h5 boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS exit_rule text,
    ADD COLUMN IF NOT EXISTS peak_pullback_pct numeric,
    ADD COLUMN IF NOT EXISTS initial_sl_pct numeric,
    ADD COLUMN IF NOT EXISTS max_holding_days integer,
    ADD COLUMN IF NOT EXISTS peak_price numeric,
    ADD COLUMN IF NOT EXISTS peak_price_at timestamptz,
    ADD COLUMN IF NOT EXISTS entry_drop_from_20d_high_pct numeric,
    ADD COLUMN IF NOT EXISTS entry_overheat_score integer,
    ADD COLUMN IF NOT EXISTS margin_ratio numeric,
    ADD COLUMN IF NOT EXISTS margin_date date,
    ADD COLUMN IF NOT EXISTS virtual_entry_price numeric,
    ADD COLUMN IF NOT EXISTS virtual_entry_model text,
    ADD COLUMN IF NOT EXISTS virtual_entry_date timestamptz,
    ADD COLUMN IF NOT EXISTS actual_entry_price numeric,
    ADD COLUMN IF NOT EXISTS actual_entry_date timestamptz,
    ADD COLUMN IF NOT EXISTS entry_slippage_pct numeric,
    ADD COLUMN IF NOT EXISTS gap_pct numeric,
    ADD COLUMN IF NOT EXISTS actual_order_type text,
    ADD COLUMN IF NOT EXISTS actual_fill_status text,
    ADD COLUMN IF NOT EXISTS skip_reason text,
    ADD COLUMN IF NOT EXISTS virtual_exit_price numeric,
    ADD COLUMN IF NOT EXISTS actual_exit_price numeric,
    ADD COLUMN IF NOT EXISTS virtual_pnl_pct numeric,
    ADD COLUMN IF NOT EXISTS actual_pnl_pct numeric,
    ADD COLUMN IF NOT EXISTS position_limit_mode text,
    ADD COLUMN IF NOT EXISTS is_h5_research boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_limited boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS selected_rank integer,
    ADD COLUMN IF NOT EXISTS live_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_candidate_count integer,
    ADD COLUMN IF NOT EXISTS h5_selected_count integer;

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS h5_case_key text,
    ADD COLUMN IF NOT EXISTS h5_primary_match boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS h5_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_overheat_score integer,
    ADD COLUMN IF NOT EXISTS position_limit_mode text,
    ADD COLUMN IF NOT EXISTS is_h5_research boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_limited boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS selected_rank integer,
    ADD COLUMN IF NOT EXISTS live_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_candidate_count integer,
    ADD COLUMN IF NOT EXISTS h5_selected_count integer;

CREATE INDEX IF NOT EXISTS idx_virtual_trades_case_key_status
    ON virtual_trades (case_key, status, buy_date DESC);

NOTIFY pgrst, 'reload schema';
