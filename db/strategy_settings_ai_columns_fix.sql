-- =====================================================
-- Strategy settings AI / notification safety migration
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

ALTER TABLE strategy_settings
    ADD COLUMN IF NOT EXISTS ai_predict_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS ai_notify_enabled boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ai_notify_early_enabled boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ai_probability_early numeric DEFAULT 0.35,
    ADD COLUMN IF NOT EXISTS ai_probability_confirmed numeric DEFAULT 0.50,
    ADD COLUMN IF NOT EXISTS ai_probability_strong numeric DEFAULT 0.65,
    ADD COLUMN IF NOT EXISTS ai_expected_value_min numeric DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS jquants_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_prefer_source boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_fallback_yfinance boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS jquants_sleep_sec numeric DEFAULT 0.2,
    ADD COLUMN IF NOT EXISTS jquants_max_retry integer DEFAULT 2;

ALTER TABLE strategy_settings
    ALTER COLUMN rebound_notify_enabled SET DEFAULT false,
    ALTER COLUMN ai_probability_early SET DEFAULT 0.35,
    ALTER COLUMN ai_probability_confirmed SET DEFAULT 0.50,
    ALTER COLUMN ai_probability_strong SET DEFAULT 0.65;

UPDATE strategy_settings
SET
    rebound_notify_enabled = false,
    ai_notify_enabled = false,
    ai_notify_early_enabled = false,
    ai_probability_early = 0.35,
    ai_probability_confirmed = 0.50,
    ai_probability_strong = 0.65,
    updated_at = now()
WHERE user_id = 'global';

UPDATE stock_drop_watchlist
SET status = 'rebound_signal',
    updated_at = now()
WHERE status = 'notified'
  AND signal_stage IN ('early', 'confirmed', 'strong_confirmed');

NOTIFY pgrst, 'reload schema';
