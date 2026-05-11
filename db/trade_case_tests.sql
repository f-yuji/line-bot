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
    avg_peak_profit_pct numeric,
    avg_trade_drawdown_pct numeric,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trade_case_simulations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES trade_case_runs(id) ON DELETE CASCADE,
    case_id uuid REFERENCES trade_case_definitions(id) ON DELETE CASCADE,
    exit_type text,
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
    peak_profit_pct numeric,
    max_drawdown_pct numeric,
    trailing_triggered boolean,
    exit_signal_value numeric,
    exit_indicator text,
    signal_stage text,
    signal_probability numeric,
    expected_value numeric,
    rule_score numeric,
    market_regime text,
    market_regime_label text,
    market_nikkei_pct numeric,
    market_topix_pct numeric,
    margin_date date,
    margin_ratio numeric,
    margin_long_outstanding numeric,
    margin_short_outstanding numeric,
    created_at timestamptz DEFAULT now()
);

ALTER TABLE trade_case_results
    ADD COLUMN IF NOT EXISTS avg_peak_profit_pct numeric,
    ADD COLUMN IF NOT EXISTS avg_trade_drawdown_pct numeric;

ALTER TABLE trade_case_simulations
    ADD COLUMN IF NOT EXISTS exit_type text,
    ADD COLUMN IF NOT EXISTS peak_profit_pct numeric,
    ADD COLUMN IF NOT EXISTS max_drawdown_pct numeric,
    ADD COLUMN IF NOT EXISTS trailing_triggered boolean,
    ADD COLUMN IF NOT EXISTS exit_signal_value numeric,
    ADD COLUMN IF NOT EXISTS exit_indicator text,
    ADD COLUMN IF NOT EXISTS margin_date date,
    ADD COLUMN IF NOT EXISTS margin_ratio numeric,
    ADD COLUMN IF NOT EXISTS margin_long_outstanding numeric,
    ADD COLUMN IF NOT EXISTS margin_short_outstanding numeric;

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
    'Current Rule',
    'Baseline rule close to current virtual trade entry logic.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'ai_top10',
    'AI Top 10',
    'Take top 10 candidates by AI score.',
    '{
      "entry_sort": "signal_probability_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'ev_top10',
    'EV Top 10',
    'Take top 10 candidates by expected value.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'position_limited',
    'Position Limited',
    'Maximum 20 open positions and 5 daily entries.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 50,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 99,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'sector_limited',
    'Sector Limited',
    'Limit open positions in the same sector to 2.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 50,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.35,
      "allowed_stages": ["early", "confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'regime_strict',
    'Regime Strict',
    'Tighten entries during panic_rebound market regime.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
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
    '5d/10d Agreement Only',
    'Definition only until both model scores are stored together.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "require_model_agreement": true,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.06,
      "sl_pct": -0.04,
      "max_holding_days": 5
    }'::jsonb
),
(
    'fixed_tp_7',
    'Fixed TP 7%',
    'Fixed take profit 7%, stop loss 4%.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.07,
      "sl_pct": -0.04,
      "max_holding_days": 10
    }'::jsonb
),
(
    'fixed_tp_10',
    'Fixed TP 10%',
    'Fixed take profit 10%, stop loss 4%.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "fixed_tp_sl",
      "tp_pct": 0.10,
      "sl_pct": -0.04,
      "max_holding_days": 10
    }'::jsonb
),
(
    'trailing_3',
    'Trailing 3%',
    'Exit when price drops 3% from the post-entry peak.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "trailing_stop",
      "trailing_drop_pct": -0.03,
      "initial_sl_pct": -0.04,
      "max_holding_days": 15
    }'::jsonb
),
(
    'trailing_5',
    'Trailing 5%',
    'Exit when price drops 5% from the post-entry peak.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "trailing_stop",
      "trailing_drop_pct": -0.05,
      "initial_sl_pct": -0.04,
      "max_holding_days": 15
    }'::jsonb
),
(
    'pullback_2',
    'Pullback -2%',
    'Exit profitable trades when daily close falls 2% or more.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "pullback_exit",
      "pullback_day_pct": -0.02,
      "initial_sl_pct": -0.04,
      "max_holding_days": 10
    }'::jsonb
),
(
    'ma5_exit',
    'MA5 Break Exit',
    'Exit profitable trades when close breaks below 5-day moving average proxy.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "ma_break_exit",
      "ma_period": 5,
      "initial_sl_pct": -0.04,
      "max_holding_days": 15
    }'::jsonb
),
(
    'rsi70_exit',
    'RSI70 Reversal',
    'Exit after estimated RSI exceeds 70 and then reverses.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "rsi_reversal_exit",
      "overbought_rsi": 70,
      "initial_sl_pct": -0.04,
      "max_holding_days": 15
    }'::jsonb
),
(
    'volume_fade',
    'Volume Fade',
    'Exit profitable trades when the available volume proxy is weak.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "volume_fade_exit",
      "volume_drop_ratio": 0.5,
      "initial_sl_pct": -0.04,
      "max_holding_days": 10
    }'::jsonb
),
(
    'atr_trailing_15',
    'ATR Trailing x1.5',
    'ATR-based trailing stop using available high/low path.',
    '{
      "entry_sort": "expected_value_desc",
      "entry_rank_limit": 10,
      "max_open_positions": 20,
      "max_daily_entries": 5,
      "max_sector_positions": 2,
      "min_ai_score": 0.50,
      "allowed_stages": ["confirmed", "strong_confirmed"],
      "exit_type": "atr_trailing",
      "atr_multiplier": 1.5,
      "initial_sl_pct": -0.04,
      "max_holding_days": 20
    }'::jsonb
)
ON CONFLICT (case_key) DO UPDATE
SET
    case_name = EXCLUDED.case_name,
    description = EXCLUDED.description,
    rules = EXCLUDED.rules,
    updated_at = now();

WITH entry_templates(entry_key, entry_name, entry_rules) AS (
    VALUES
    (
        'current',
        'Current Entry',
        '{
          "entry_sort": "expected_value_desc",
          "entry_rank_limit": 10,
          "max_open_positions": 20,
          "max_daily_entries": 5,
          "max_sector_positions": 2,
          "min_ai_score": 0.50,
          "allowed_stages": ["confirmed", "strong_confirmed"]
        }'::jsonb
    ),
    (
        'ai_top10',
        'AI Top 10 Entry',
        '{
          "entry_sort": "signal_probability_desc",
          "entry_rank_limit": 10,
          "max_open_positions": 20,
          "max_daily_entries": 5,
          "max_sector_positions": 99,
          "min_ai_score": 0.35,
          "allowed_stages": ["early", "confirmed", "strong_confirmed"]
        }'::jsonb
    ),
    (
        'ev_top10',
        'EV Top 10 Entry',
        '{
          "entry_sort": "expected_value_desc",
          "entry_rank_limit": 10,
          "max_open_positions": 20,
          "max_daily_entries": 5,
          "max_sector_positions": 99,
          "min_ai_score": 0.35,
          "allowed_stages": ["early", "confirmed", "strong_confirmed"]
        }'::jsonb
    ),
    (
        'position_limited',
        'Position Limited Entry',
        '{
          "entry_sort": "expected_value_desc",
          "entry_rank_limit": 50,
          "max_open_positions": 20,
          "max_daily_entries": 5,
          "max_sector_positions": 99,
          "min_ai_score": 0.35,
          "allowed_stages": ["early", "confirmed", "strong_confirmed"]
        }'::jsonb
    ),
    (
        'sector_limited',
        'Sector Limited Entry',
        '{
          "entry_sort": "expected_value_desc",
          "entry_rank_limit": 50,
          "max_open_positions": 20,
          "max_daily_entries": 5,
          "max_sector_positions": 2,
          "min_ai_score": 0.35,
          "allowed_stages": ["early", "confirmed", "strong_confirmed"]
        }'::jsonb
    ),
    (
        'regime_strict',
        'Regime Strict Entry',
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
          }
        }'::jsonb
    )
),
exit_templates(exit_key, exit_name, exit_rules) AS (
    VALUES
    (
        'fixed6',
        'Fixed TP 6%',
        '{
          "exit_type": "fixed_tp_sl",
          "tp_pct": 0.06,
          "sl_pct": -0.04,
          "max_holding_days": 5
        }'::jsonb
    ),
    (
        'fixed7',
        'Fixed TP 7%',
        '{
          "exit_type": "fixed_tp_sl",
          "tp_pct": 0.07,
          "sl_pct": -0.04,
          "max_holding_days": 10
        }'::jsonb
    ),
    (
        'fixed10',
        'Fixed TP 10%',
        '{
          "exit_type": "fixed_tp_sl",
          "tp_pct": 0.10,
          "sl_pct": -0.04,
          "max_holding_days": 10
        }'::jsonb
    ),
    (
        'trailing3',
        'Trailing 3%',
        '{
          "exit_type": "trailing_stop",
          "trailing_drop_pct": -0.03,
          "initial_sl_pct": -0.04,
          "max_holding_days": 15
        }'::jsonb
    ),
    (
        'trailing5',
        'Trailing 5%',
        '{
          "exit_type": "trailing_stop",
          "trailing_drop_pct": -0.05,
          "initial_sl_pct": -0.04,
          "max_holding_days": 15
        }'::jsonb
    ),
    (
        'pullback2',
        'Pullback -2%',
        '{
          "exit_type": "pullback_exit",
          "pullback_day_pct": -0.02,
          "initial_sl_pct": -0.04,
          "max_holding_days": 10
        }'::jsonb
    ),
    (
        'ma5',
        'MA5 Break',
        '{
          "exit_type": "ma_break_exit",
          "ma_period": 5,
          "initial_sl_pct": -0.04,
          "max_holding_days": 15
        }'::jsonb
    ),
    (
        'rsi70',
        'RSI70 Reversal',
        '{
          "exit_type": "rsi_reversal_exit",
          "overbought_rsi": 70,
          "initial_sl_pct": -0.04,
          "max_holding_days": 15
        }'::jsonb
    ),
    (
        'atr15',
        'ATR Trailing x1.5',
        '{
          "exit_type": "atr_trailing",
          "atr_multiplier": 1.5,
          "initial_sl_pct": -0.04,
          "max_holding_days": 20
        }'::jsonb
    )
),
credit_templates(credit_key, credit_name, credit_rules) AS (
    VALUES
    (
        'no_margin',
        '',
        '{}'::jsonb
    ),
    (
        'margin_le20',
        '信用倍率20倍以下',
        '{
          "credit_profile": "margin_le20",
          "use_margin_filter": true,
          "require_margin_data": true,
          "max_margin_ratio": 20
        }'::jsonb
    ),
    (
        'margin_le10',
        '信用倍率10倍以下',
        '{
          "credit_profile": "margin_le10",
          "use_margin_filter": true,
          "require_margin_data": true,
          "max_margin_ratio": 10
        }'::jsonb
    ),
    (
        'margin_le5',
        '信用倍率5倍以下',
        '{
          "credit_profile": "margin_le5",
          "use_margin_filter": true,
          "require_margin_data": true,
          "max_margin_ratio": 5
        }'::jsonb
    ),
    (
        'short_pressure',
        '売り残比率10%以上',
        '{
          "credit_profile": "short_pressure",
          "use_margin_filter": true,
          "require_margin_data": true,
          "min_short_long_ratio": 0.10
        }'::jsonb
    )
)
INSERT INTO trade_case_definitions (case_key, case_name, description, rules)
SELECT
    'combo_' || e.entry_key || '__' || x.exit_key ||
      CASE WHEN c.credit_key = 'no_margin' THEN '' ELSE '__' || c.credit_key END AS case_key,
    e.entry_name || ' x ' || x.exit_name ||
      CASE WHEN c.credit_key = 'no_margin' THEN '' ELSE ' x ' || c.credit_name END AS case_name,
    'Generated entry, exit and credit-filter combination case.' AS description,
    e.entry_rules
      || x.exit_rules
      || c.credit_rules
      || jsonb_build_object(
            'entry_profile', e.entry_key,
            'exit_profile', x.exit_key,
            'credit_profile', c.credit_key
         ) AS rules
FROM entry_templates e
CROSS JOIN exit_templates x
CROSS JOIN credit_templates c
ON CONFLICT (case_key) DO UPDATE
SET
    case_name = EXCLUDED.case_name,
    description = EXCLUDED.description,
    rules = EXCLUDED.rules,
    updated_at = now();

-- Keep the comparison page focused on the 54 generated entry x exit cases.
UPDATE trade_case_definitions
SET is_enabled = false,
    updated_at = now()
WHERE case_key IN (
    'current_rule',
    'ai_top10',
    'ev_top10',
    'position_limited',
    'sector_limited',
    'regime_strict',
    'model_agreement',
    'fixed_tp_7',
    'fixed_tp_10',
    'trailing_3',
    'trailing_5',
    'pullback_2',
    'ma5_exit',
    'rsi70_exit',
    'volume_fade',
    'atr_trailing_15'
);

UPDATE trade_case_definitions
SET is_enabled = true,
    updated_at = now()
WHERE case_key LIKE 'combo\_%' ESCAPE '\';

NOTIFY pgrst, 'reload schema';
