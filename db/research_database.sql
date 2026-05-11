-- =====================================================
-- Research database
-- Safe to run multiple times in Supabase SQL Editor.
-- This is a management layer for verification data only.
-- It does not modify virtual_trades or production signal logic.
-- =====================================================

CREATE TABLE IF NOT EXISTS research_datasets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_key text UNIQUE NOT NULL,
    dataset_name text NOT NULL,
    dataset_type text NOT NULL,
    source_table text,
    source text,
    period_start date,
    period_end date,
    row_count integer DEFAULT 0,
    status text DEFAULT 'ready',
    hash_key text,
    memo text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_datasets_type
    ON research_datasets (dataset_type);
CREATE INDEX IF NOT EXISTS idx_research_datasets_updated_at
    ON research_datasets (updated_at DESC);

CREATE TABLE IF NOT EXISTS research_case_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id uuid REFERENCES research_datasets(id) ON DELETE CASCADE,
    run_id uuid,
    case_id uuid,
    case_key text,
    case_name text,
    period_start date,
    period_end date,
    entry_count integer,
    win_rate numeric,
    expected_value_pct numeric,
    total_profit_pct numeric,
    total_profit_yen numeric,
    max_drawdown_pct numeric,
    avg_profit_pct numeric,
    avg_loss_pct numeric,
    avg_holding_days numeric,
    max_open_positions integer,
    tp_count integer,
    sl_count integer,
    timeout_count integer,
    rules jsonb,
    metrics jsonb,
    created_at timestamptz DEFAULT now(),
    UNIQUE (dataset_id, case_key, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_research_case_snapshots_dataset
    ON research_case_snapshots (dataset_id);
CREATE INDEX IF NOT EXISTS idx_research_case_snapshots_profit
    ON research_case_snapshots (total_profit_pct DESC);

CREATE TABLE IF NOT EXISTS research_periods (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    period_key text UNIQUE NOT NULL,
    period_name text NOT NULL,
    regime_type text NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    description text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_periods_type
    ON research_periods (regime_type);
CREATE INDEX IF NOT EXISTS idx_research_periods_start
    ON research_periods (period_start);

CREATE TABLE IF NOT EXISTS research_import_logs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_key text,
    job_type text,
    status text,
    started_at timestamptz DEFAULT now(),
    finished_at timestamptz,
    rows_inserted integer DEFAULT 0,
    rows_updated integer DEFAULT 0,
    rows_skipped integer DEFAULT 0,
    error_message text,
    params jsonb,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_import_logs_started_at
    ON research_import_logs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_import_logs_dataset_key
    ON research_import_logs (dataset_key);

INSERT INTO research_periods (period_key, period_name, regime_type, period_start, period_end, description)
VALUES
    ('2020_covid_crash', '2020 コロナショック', 'panic', '2020-02-20', '2020-04-30', '急落と急反発を含むコロナショック期'),
    ('2022_rate_hike_bear', '2022 利上げ下落相場', 'bear', '2022-01-01', '2022-12-31', '米国利上げとグロース株調整が強かった期間'),
    ('2023_rebound', '2023 反発相場', 'rebound', '2023-01-01', '2023-12-31', '日本株の反発基調が強かった期間'),
    ('2024_ai_bubble', '2024 AI上昇相場', 'bull', '2024-01-01', '2024-12-31', 'AI・半導体主導の上昇相場'),
    ('2025_ai_bubble', '2025 AI上昇相場', 'bull', '2025-01-01', '2025-12-31', 'AI関連銘柄の影響を確認する期間'),
    ('custom_recent', '直近検証', 'custom', CURRENT_DATE - INTERVAL '90 days', CURRENT_DATE, '直近90日の検証用')
ON CONFLICT (period_key) DO UPDATE
SET
    period_name = EXCLUDED.period_name,
    regime_type = EXCLUDED.regime_type,
    period_start = EXCLUDED.period_start,
    period_end = EXCLUDED.period_end,
    description = EXCLUDED.description,
    updated_at = now();

NOTIFY pgrst, 'reload schema';
