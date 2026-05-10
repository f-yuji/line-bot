-- =====================================================
-- Rebound signal event history
-- Safe to run multiple times in Supabase SQL Editor.
-- This does not modify stock_drop_watchlist or virtual_trades behavior.
-- =====================================================

CREATE TABLE IF NOT EXISTS rebound_signal_history (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Deterministic key to avoid recording the exact same signal repeatedly.
    signal_key text UNIQUE NOT NULL,

    source text NOT NULL DEFAULT 'unknown',
    source_run_id text,

    watchlist_id uuid,
    feature_snapshot_id bigint,

    code text NOT NULL,
    name text,
    market text DEFAULT 'prime',
    sector text,

    signal_date date,
    detected_at timestamptz DEFAULT now(),
    last_seen_at timestamptz DEFAULT now(),
    occurrence_count integer DEFAULT 1,

    signal_stage text NOT NULL,
    signal_probability numeric,
    expected_value numeric,
    rule_score numeric,

    current_price numeric,
    price_at_signal numeric,
    drop_pct numeric,
    rsi14 numeric,
    volume_ratio numeric,
    status_at_signal text,

    bad_news_score numeric DEFAULT 0,
    is_excluded boolean DEFAULT false,
    exclude_reason text,

    market_regime text,
    market_regime_label text,
    market_threshold_adjust numeric DEFAULT 0,
    market_regime_reason text,
    market_nikkei_pct numeric,
    market_topix_pct numeric,
    market_nikkei_change_yen numeric,

    payload jsonb DEFAULT '{}'::jsonb,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rebound_signal_history_code_date
    ON rebound_signal_history (code, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_rebound_signal_history_stage
    ON rebound_signal_history (signal_stage);
CREATE INDEX IF NOT EXISTS idx_rebound_signal_history_detected_at
    ON rebound_signal_history (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_rebound_signal_history_feature_snapshot
    ON rebound_signal_history (feature_snapshot_id);

NOTIFY pgrst, 'reload schema';
