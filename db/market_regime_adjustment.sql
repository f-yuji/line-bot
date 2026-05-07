-- =====================================================
-- Market regime adjustment columns for rebound signals.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS market_regime text,
    ADD COLUMN IF NOT EXISTS market_regime_label text,
    ADD COLUMN IF NOT EXISTS market_threshold_adjust numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_regime_reason text;

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS market_regime text,
    ADD COLUMN IF NOT EXISTS market_regime_label text,
    ADD COLUMN IF NOT EXISTS entry_size_multiplier numeric DEFAULT 1.0;

NOTIFY pgrst, 'reload schema';
