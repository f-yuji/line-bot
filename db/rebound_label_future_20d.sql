-- =====================================================
-- Rebound label future price path extension
-- Adds daily high/low/close columns through 20 future trading days.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

ALTER TABLE stock_rebound_labels
    ADD COLUMN IF NOT EXISTS future_high_6d numeric,
    ADD COLUMN IF NOT EXISTS future_high_7d numeric,
    ADD COLUMN IF NOT EXISTS future_high_8d numeric,
    ADD COLUMN IF NOT EXISTS future_high_9d numeric,
    ADD COLUMN IF NOT EXISTS future_high_10d numeric,
    ADD COLUMN IF NOT EXISTS future_high_11d numeric,
    ADD COLUMN IF NOT EXISTS future_high_12d numeric,
    ADD COLUMN IF NOT EXISTS future_high_13d numeric,
    ADD COLUMN IF NOT EXISTS future_high_14d numeric,
    ADD COLUMN IF NOT EXISTS future_high_15d numeric,
    ADD COLUMN IF NOT EXISTS future_high_16d numeric,
    ADD COLUMN IF NOT EXISTS future_high_17d numeric,
    ADD COLUMN IF NOT EXISTS future_high_18d numeric,
    ADD COLUMN IF NOT EXISTS future_high_19d numeric,
    ADD COLUMN IF NOT EXISTS future_high_20d numeric,
    ADD COLUMN IF NOT EXISTS future_low_6d numeric,
    ADD COLUMN IF NOT EXISTS future_low_7d numeric,
    ADD COLUMN IF NOT EXISTS future_low_8d numeric,
    ADD COLUMN IF NOT EXISTS future_low_9d numeric,
    ADD COLUMN IF NOT EXISTS future_low_10d numeric,
    ADD COLUMN IF NOT EXISTS future_low_11d numeric,
    ADD COLUMN IF NOT EXISTS future_low_12d numeric,
    ADD COLUMN IF NOT EXISTS future_low_13d numeric,
    ADD COLUMN IF NOT EXISTS future_low_14d numeric,
    ADD COLUMN IF NOT EXISTS future_low_15d numeric,
    ADD COLUMN IF NOT EXISTS future_low_16d numeric,
    ADD COLUMN IF NOT EXISTS future_low_17d numeric,
    ADD COLUMN IF NOT EXISTS future_low_18d numeric,
    ADD COLUMN IF NOT EXISTS future_low_19d numeric,
    ADD COLUMN IF NOT EXISTS future_low_20d numeric,
    ADD COLUMN IF NOT EXISTS future_close_6d numeric,
    ADD COLUMN IF NOT EXISTS future_close_7d numeric,
    ADD COLUMN IF NOT EXISTS future_close_8d numeric,
    ADD COLUMN IF NOT EXISTS future_close_9d numeric,
    ADD COLUMN IF NOT EXISTS future_close_10d numeric,
    ADD COLUMN IF NOT EXISTS future_close_11d numeric,
    ADD COLUMN IF NOT EXISTS future_close_12d numeric,
    ADD COLUMN IF NOT EXISTS future_close_13d numeric,
    ADD COLUMN IF NOT EXISTS future_close_14d numeric,
    ADD COLUMN IF NOT EXISTS future_close_15d numeric,
    ADD COLUMN IF NOT EXISTS future_close_16d numeric,
    ADD COLUMN IF NOT EXISTS future_close_17d numeric,
    ADD COLUMN IF NOT EXISTS future_close_18d numeric,
    ADD COLUMN IF NOT EXISTS future_close_19d numeric,
    ADD COLUMN IF NOT EXISTS future_close_20d numeric;

NOTIFY pgrst, 'reload schema';
