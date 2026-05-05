-- =====================================================
-- Rebound AI system phase 2
-- Feature snapshot table for rule/ML training data.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

CREATE TABLE IF NOT EXISTS stock_feature_snapshots (
    id bigserial PRIMARY KEY,

    trade_date date NOT NULL,
    code text NOT NULL,
    name text,
    market text DEFAULT 'prime',
    sector text,

    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    turnover_value numeric,

    prev_close numeric,
    day_change numeric,
    day_change_pct numeric,

    drop_pct numeric,
    drop_from_5d_high_pct numeric,
    drop_from_20d_high_pct numeric,
    drop_from_52w_high_pct numeric,

    return_1d_pct numeric,
    return_3d_pct numeric,
    return_5d_pct numeric,
    return_10d_pct numeric,

    ma5 numeric,
    ma25 numeric,
    ma75 numeric,
    ma5_gap_pct numeric,
    ma25_gap_pct numeric,
    ma75_gap_pct numeric,

    rsi14 numeric,
    rsi_min_5d numeric,
    rsi_recover_flag boolean DEFAULT false,

    volume_avg_20d numeric,
    volume_ratio_20d numeric,
    volume_spike_flag boolean DEFAULT false,

    atr14 numeric,
    volatility_20d numeric,

    nikkei_change_pct numeric,
    topix_change_pct numeric,
    sector_change_pct numeric,
    index_gap_pct numeric,
    sector_gap_pct numeric,

    decliners_ratio numeric,
    advancers_ratio numeric,

    vix_value numeric,
    vix_change_pct numeric,
    nikkei_vi_value numeric,
    nikkei_vi_change_pct numeric,

    per numeric,
    pbr numeric,
    dividend_yield_pct numeric,
    is_deficit boolean,
    roe numeric,
    operating_profit_growth_pct numeric,
    net_income_growth_pct numeric,

    margin_buy_balance numeric,
    margin_sell_balance numeric,
    margin_ratio numeric,
    margin_buy_change_pct numeric,
    margin_sell_change_pct numeric,

    short_selling_ratio numeric,
    short_balance_ratio numeric,

    earnings_soon_flag boolean DEFAULT false,
    earnings_within_5d_flag boolean DEFAULT false,
    earnings_recent_flag boolean DEFAULT false,
    tdnet_disclosure_flag boolean DEFAULT false,

    market_shock_score numeric DEFAULT 0,
    sector_risk_score numeric DEFAULT 0,
    bad_news_score numeric DEFAULT 0,
    fx_yen_score numeric DEFAULT 0,
    energy_naphtha_score numeric DEFAULT 0,
    interest_rate_score numeric DEFAULT 0,

    is_drop_candidate boolean DEFAULT false,
    is_tradeable boolean DEFAULT true,
    exclude_reason text,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_code
    ON stock_feature_snapshots (code);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_trade_date
    ON stock_feature_snapshots (trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_code_date
    ON stock_feature_snapshots (code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_drop_candidate
    ON stock_feature_snapshots (is_drop_candidate);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_tradeable
    ON stock_feature_snapshots (is_tradeable);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_sector
    ON stock_feature_snapshots (sector);
CREATE INDEX IF NOT EXISTS idx_stock_feature_snapshots_market
    ON stock_feature_snapshots (market);
