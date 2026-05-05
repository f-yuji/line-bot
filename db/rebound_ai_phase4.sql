-- =====================================================
-- Rebound AI system phase 4
-- Market/news signals and market regime.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

CREATE TABLE IF NOT EXISTS market_news_signals (
    id bigserial PRIMARY KEY,

    signal_date date NOT NULL,
    source text,
    title text NOT NULL,
    url text,
    url_hash text,
    summary text,

    category text,
    subcategory text,

    related_codes jsonb DEFAULT '[]'::jsonb,
    related_sectors jsonb DEFAULT '[]'::jsonb,

    market_shock_score numeric DEFAULT 0,
    sector_risk_score numeric DEFAULT 0,
    bad_news_score numeric DEFAULT 0,
    fx_yen_score numeric DEFAULT 0,
    energy_naphtha_score numeric DEFAULT 0,
    interest_rate_score numeric DEFAULT 0,
    geopolitical_score numeric DEFAULT 0,
    supply_chain_score numeric DEFAULT 0,

    severity text,
    action_type text,
    reason text,
    matched_keywords jsonb DEFAULT '[]'::jsonb,

    is_applied_to_features boolean DEFAULT false,
    applied_at timestamptz,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (signal_date, url_hash)
);

CREATE INDEX IF NOT EXISTS idx_market_news_signals_date
    ON market_news_signals (signal_date);
CREATE INDEX IF NOT EXISTS idx_market_news_signals_category
    ON market_news_signals (category);
CREATE INDEX IF NOT EXISTS idx_market_news_signals_severity
    ON market_news_signals (severity);
CREATE INDEX IF NOT EXISTS idx_market_news_signals_action_type
    ON market_news_signals (action_type);
CREATE INDEX IF NOT EXISTS idx_market_news_signals_applied
    ON market_news_signals (is_applied_to_features);
CREATE INDEX IF NOT EXISTS idx_market_news_signals_url_hash
    ON market_news_signals (url_hash);

CREATE TABLE IF NOT EXISTS market_regime (
    id bigserial PRIMARY KEY,

    trade_date date NOT NULL UNIQUE,
    mode text DEFAULT 'normal',

    nikkei_change_pct numeric,
    topix_change_pct numeric,

    decliners_ratio numeric,
    advancers_ratio numeric,

    vix_value numeric,
    vix_change_pct numeric,
    nikkei_vi_value numeric,
    nikkei_vi_change_pct numeric,

    market_shock_score numeric DEFAULT 0,
    geopolitical_score numeric DEFAULT 0,
    interest_rate_score numeric DEFAULT 0,
    fx_yen_score numeric DEFAULT 0,
    energy_naphtha_score numeric DEFAULT 0,

    shock_score numeric DEFAULT 0,

    reason text,
    matched_conditions jsonb DEFAULT '[]'::jsonb,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_market_regime_trade_date
    ON market_regime (trade_date);
CREATE INDEX IF NOT EXISTS idx_market_regime_mode
    ON market_regime (mode);
CREATE INDEX IF NOT EXISTS idx_market_regime_shock_score
    ON market_regime (shock_score);
