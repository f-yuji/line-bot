-- =====================================================
-- Trade case comparison tests
-- Safe to run multiple times in Supabase SQL Editor.
-- This is separated from virtual_trades.
-- =====================================================

CREATE TABLE IF NOT EXISTS trade_case_definitions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    case_key text UNIQUE NOT NULL,
    case_name text NOT NULL,
    description text,
    is_enabled boolean DEFAULT true,
    rules jsonb NOT NULL,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trade_case_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name text,
    period_start date,
    period_end date,
    source text DEFAULT 'stock_feature_snapshots',
    status text DEFAULT 'running',
    started_at timestamptz DEFAULT now(),
    finished_at timestamptz,
    memo text
);

CREATE TABLE IF NOT EXISTS trade_case_results (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES trade_case_runs(id) ON DELETE CASCADE,
    case_id uuid REFERENCES trade_case_definitions(id) ON DELETE CASCADE,
    entry_count integer DEFAULT 0,
    win_count integer DEFAULT 0,
    loss_count integer DEFAULT 0,
    open_count integer DEFAULT 0,
    win_rate numeric,
    avg_profit_pct numeric,
    avg_loss_pct numeric,
    expected_value_pct numeric,
    total_profit_pct numeric,
    total_profit_yen numeric,
    max_drawdown_pct numeric,
    max_open_positions integer,
    avg_holding_days numeric,
    tp_count integer DEFAULT 0,
    sl_count integer DEFAULT 0,
    timeout_count integer DEFAULT 0,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trade_case_simulations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES trade_case_runs(id) ON DELETE CASCADE,
    case_id uuid REFERENCES trade_case_definitions(id) ON DELETE CASCADE,
    code text NOT NULL,
    name text,
    sector text,
    entry_date date,
    entry_price numeric,
    exit_date date,
    exit_price numeric,
    status text,
    exit_reason text,
    profit_pct numeric,
    profit_yen numeric,
    holding_days integer,
    signal_stage text,
    signal_probability numeric,
    expected_value numeric,
    rule_score numeric,
    market_regime text,
    market_regime_label text,
    market_nikkei_pct numeric,
    market_topix_pct numeric,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trade_case_runs_started_at
    ON trade_case_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_case_results_run_id
    ON trade_case_results (run_id);
CREATE INDEX IF NOT EXISTS idx_trade_case_simulations_run_case
    ON trade_case_simulations (run_id, case_id);
CREATE INDEX IF NOT EXISTS idx_trade_case_simulations_entry_date
    ON trade_case_simulations (entry_date);

INSERT INTO trade_case_definitions (case_key, case_name, description, rules)
VALUES
(
    'current_rule',
    '現行ルール',
    '現在の運用設定に近い基準。AI50以上、本命以上を中心に採用。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'ai_top10',
    'AI上位10件',
    'AIスコア順に上位10件だけ採用。',
    '{
      "entry_sort": "signal_probability_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'ev_top10',
    '期待値上位10件',
    '期待値順に上位10件だけ採用。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'position_limited',
    '最大保有20・1日5件',
    '最大保有20件、1日最大5件に制限。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 50,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'sector_limited',
    'セクター最大2件',
    '同一セクターの同時保有を2件までに制限。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 50,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'regime_strict',
    'panic_rebound厳格化',
    '異常急反発ではエントリー数を半減し、AI最低値を引き上げる。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "regime_adjust": {
        "panic_rebound": {
          "entry_rank_limit_multiplier": 0.5,
          "min_ai_score_add": 0.1
        }
      },
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'model_agreement',
    '5d/10d一致のみ',
    '5dと10dモデルの両方が一定以上のときだけ採用。現時点では定義のみ。',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "require_model_agreement": true,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
)
ON CONFLICT (case_key) DO UPDATE
SET
    case_name = EXCLUDED.case_name,
    description = EXCLUDED.description,
    rules = EXCLUDED.rules,
    updated_at = now();

NOTIFY pgrst, 'reload schema';
