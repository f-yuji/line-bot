-- =====================================================
-- J-Quants Light settings columns.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

ALTER TABLE strategy_settings
    ADD COLUMN IF NOT EXISTS ai_predict_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS ai_notify_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS ai_notify_early_enabled boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ai_probability_early numeric DEFAULT 0.55,
    ADD COLUMN IF NOT EXISTS ai_probability_confirmed numeric DEFAULT 0.65,
    ADD COLUMN IF NOT EXISTS ai_probability_strong numeric DEFAULT 0.72,
    ADD COLUMN IF NOT EXISTS ai_expected_value_min numeric DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS jquants_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_prefer_source boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_fallback_yfinance boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_sleep_sec numeric DEFAULT 0.2,
    ADD COLUMN IF NOT EXISTS jquants_max_retry integer DEFAULT 2;
