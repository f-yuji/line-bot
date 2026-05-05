-- =====================================================
-- Rebound AI system phase 1 additions
-- Run in Supabase SQL Editor before scripts/monitor_rebound.py.
-- =====================================================

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS signal_stage text DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS signal_score numeric,
    ADD COLUMN IF NOT EXISTS signal_probability numeric,
    ADD COLUMN IF NOT EXISTS expected_value numeric,
    ADD COLUMN IF NOT EXISTS mode text DEFAULT 'normal',
    ADD COLUMN IF NOT EXISTS bad_news_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_shock_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sector_risk_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fx_yen_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS energy_naphtha_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS interest_rate_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS exclude_reason text,
    ADD COLUMN IF NOT EXISTS is_excluded boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS excluded_at timestamptz,
    ADD COLUMN IF NOT EXISTS feature_snapshot_id bigint,
    ADD COLUMN IF NOT EXISTS last_signal_at timestamptz,
    ADD COLUMN IF NOT EXISTS signal_count integer DEFAULT 0;

ALTER TABLE stock_drop_watchlist DROP CONSTRAINT IF EXISTS stock_drop_watchlist_status_check;
ALTER TABLE stock_drop_watchlist
    ADD CONSTRAINT stock_drop_watchlist_status_check
    CHECK (status IN ('watching', 'rebound_signal', 'notified', 'closed', 'excluded'));

CREATE INDEX IF NOT EXISTS idx_watchlist_signal_stage
    ON stock_drop_watchlist (signal_stage);
CREATE INDEX IF NOT EXISTS idx_watchlist_excluded
    ON stock_drop_watchlist (is_excluded);

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS signal_stage text,
    ADD COLUMN IF NOT EXISTS entry_reason text,
    ADD COLUMN IF NOT EXISTS entry_score numeric,
    ADD COLUMN IF NOT EXISTS entry_probability numeric,
    ADD COLUMN IF NOT EXISTS expected_value numeric,
    ADD COLUMN IF NOT EXISTS mode text,
    ADD COLUMN IF NOT EXISTS bad_news_score numeric,
    ADD COLUMN IF NOT EXISTS sector_risk_score numeric,
    ADD COLUMN IF NOT EXISTS market_shock_score numeric,
    ADD COLUMN IF NOT EXISTS feature_snapshot_id bigint,
    ADD COLUMN IF NOT EXISTS label_id bigint,
    ADD COLUMN IF NOT EXISTS max_return_pct numeric,
    ADD COLUMN IF NOT EXISTS max_drawdown_pct numeric,
    ADD COLUMN IF NOT EXISTS exit_reason text,
    ADD COLUMN IF NOT EXISTS exit_checked_at timestamptz;

ALTER TABLE virtual_trades DROP CONSTRAINT IF EXISTS virtual_trades_sell_reason_check;
ALTER TABLE virtual_trades
    ADD CONSTRAINT virtual_trades_sell_reason_check
    CHECK (sell_reason IN ('take_profit', 'stop_loss', 'expired', 'manual', 'manual_closed', 'excluded_after_entry'));

CREATE INDEX IF NOT EXISTS idx_virtual_trades_signal_stage
    ON virtual_trades (signal_stage);
