-- =====================================================
-- J-Quants Standard market credit / short-selling data
-- Safe to run multiple times in Supabase SQL Editor.
-- These tables are stored for analysis first; model features can use them later.
-- =====================================================

CREATE TABLE IF NOT EXISTS stock_weekly_margin_interest (
    id bigserial PRIMARY KEY,
    code text NOT NULL,
    date date NOT NULL,
    published_date date,

    short_margin_outstanding numeric,
    long_margin_outstanding numeric,
    margin_ratio numeric,

    short_margin_change numeric,
    long_margin_change numeric,
    short_margin_listed_share_ratio numeric,
    long_margin_listed_share_ratio numeric,

    raw jsonb,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (code, date)
);

CREATE INDEX IF NOT EXISTS idx_stock_weekly_margin_interest_code
    ON stock_weekly_margin_interest (code);
CREATE INDEX IF NOT EXISTS idx_stock_weekly_margin_interest_date
    ON stock_weekly_margin_interest (date);

CREATE TABLE IF NOT EXISTS stock_daily_margin_interest (
    id bigserial PRIMARY KEY,
    code text NOT NULL,
    application_date date NOT NULL,
    published_date date,

    short_margin_outstanding numeric,
    long_margin_outstanding numeric,
    margin_ratio numeric,

    short_margin_change numeric,
    long_margin_change numeric,
    short_margin_listed_share_ratio numeric,
    long_margin_listed_share_ratio numeric,

    publish_reason jsonb,
    raw jsonb,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (code, application_date, published_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_daily_margin_interest_code
    ON stock_daily_margin_interest (code);
CREATE INDEX IF NOT EXISTS idx_stock_daily_margin_interest_application_date
    ON stock_daily_margin_interest (application_date);

CREATE TABLE IF NOT EXISTS sector_short_selling (
    id bigserial PRIMARY KEY,
    date date NOT NULL,
    sector33_code text NOT NULL,

    selling_excluding_short_value numeric,
    short_selling_with_restrictions_value numeric,
    short_selling_without_restrictions_value numeric,
    total_selling_value numeric,
    total_short_selling_value numeric,
    short_selling_ratio numeric,

    raw jsonb,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (date, sector33_code)
);

CREATE INDEX IF NOT EXISTS idx_sector_short_selling_date
    ON sector_short_selling (date);
CREATE INDEX IF NOT EXISTS idx_sector_short_selling_sector
    ON sector_short_selling (sector33_code);

NOTIFY pgrst, 'reload schema';
