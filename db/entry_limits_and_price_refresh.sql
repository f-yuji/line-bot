-- =====================================================
-- Entry limits and lightweight price refresh support
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

ALTER TABLE strategy_settings
    ADD COLUMN IF NOT EXISTS max_open_positions integer DEFAULT 20,
    ADD COLUMN IF NOT EXISTS max_daily_entries integer DEFAULT 5,
    ADD COLUMN IF NOT EXISTS entry_rank_limit integer DEFAULT 10,
    ADD COLUMN IF NOT EXISTS max_sector_positions integer DEFAULT 2;

UPDATE strategy_settings
SET
    max_open_positions = COALESCE(max_open_positions, 20),
    max_daily_entries = COALESCE(max_daily_entries, 5),
    entry_rank_limit = COALESCE(entry_rank_limit, 10),
    max_sector_positions = COALESCE(max_sector_positions, 2),
    updated_at = now()
WHERE user_id = 'global';

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS current_price numeric;

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS sector text,
    ADD COLUMN IF NOT EXISTS current_price numeric,
    ADD COLUMN IF NOT EXISTS unrealized_pnl numeric,
    ADD COLUMN IF NOT EXISTS unrealized_pnl_pct numeric;

CREATE INDEX IF NOT EXISTS idx_virtual_trades_watchlist_id
    ON virtual_trades (watchlist_id);
CREATE INDEX IF NOT EXISTS idx_virtual_trades_feature_snapshot_id
    ON virtual_trades (feature_snapshot_id);

NOTIFY pgrst, 'reload schema';
