-- =====================================================
-- Add backfill columns to market_regime table.
-- Run in Supabase SQL Editor before running backfill_market_regime.py
-- =====================================================

ALTER TABLE market_regime
    ADD COLUMN IF NOT EXISTS nikkei_ma25_gap  numeric,
    ADD COLUMN IF NOT EXISTS topix_ma25_gap   numeric,
    ADD COLUMN IF NOT EXISTS volatility_score numeric DEFAULT 0,
    ADD COLUMN IF NOT EXISTS panic_score      numeric DEFAULT 0;

-- Refresh PostgREST schema cache
NOTIFY pgrst, 'reload schema';
