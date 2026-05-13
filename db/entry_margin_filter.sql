-- Entry-side margin-ratio filter settings.
-- Safe to run multiple times in Supabase SQL Editor.

ALTER TABLE strategy_settings
    ADD COLUMN IF NOT EXISTS entry_margin_filter_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS entry_margin_require_data boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS entry_max_margin_ratio numeric DEFAULT 5.0;

UPDATE strategy_settings
SET
    entry_margin_filter_enabled = COALESCE(entry_margin_filter_enabled, true),
    entry_margin_require_data = COALESCE(entry_margin_require_data, true),
    entry_max_margin_ratio = COALESCE(entry_max_margin_ratio, 5.0)
WHERE user_id = 'global';

NOTIFY pgrst, 'reload schema';
