-- Experimental comparison/paper-trade metadata.
-- Additive only. Does not change Primary/H5 production rules, LINE, actual_trade_logs,
-- or auto-trading behavior.

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS strategy_group text,
    ADD COLUMN IF NOT EXISTS is_experimental boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS source_case text,
    ADD COLUMN IF NOT EXISTS allocation_bucket text,
    ADD COLUMN IF NOT EXISTS experimental_definition_version text,
    ADD COLUMN IF NOT EXISTS trend_flags text,
    ADD COLUMN IF NOT EXISTS momentum_flags text,
    ADD COLUMN IF NOT EXISTS credit_flags text,
    ADD COLUMN IF NOT EXISTS drop5 numeric,
    ADD COLUMN IF NOT EXISTS drop10 numeric,
    ADD COLUMN IF NOT EXISTS drop20 numeric,
    ADD COLUMN IF NOT EXISTS planned_exit_rule text,
    ADD COLUMN IF NOT EXISTS planned_holding_days integer,
    ADD COLUMN IF NOT EXISTS pnl_before_cost numeric,
    ADD COLUMN IF NOT EXISTS pnl_after_cost numeric,
    ADD COLUMN IF NOT EXISTS tax_adjusted_pnl numeric,
    ADD COLUMN IF NOT EXISTS cumulative_pnl numeric,
    ADD COLUMN IF NOT EXISTS exclusion_reason text;

CREATE INDEX IF NOT EXISTS idx_virtual_trades_experimental_case_status
    ON virtual_trades (is_experimental, case_key, status, buy_date DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_trades_experimental_code_case
    ON virtual_trades (is_experimental, code, case_key, buy_date DESC);

CREATE TABLE IF NOT EXISTS experimental_case_definitions (
    case_key text PRIMARY KEY,
    original_case_key text,
    definition_version text NOT NULL,
    condition_json jsonb NOT NULL,
    proxy_used boolean DEFAULT false,
    hd integer,
    daily_cap integer,
    gap_limit numeric,
    tax_mode text,
    strategy_group text,
    allocation_bucket text,
    is_enabled boolean DEFAULT true,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_experimental_case_definitions_enabled
    ON experimental_case_definitions (is_enabled, strategy_group, allocation_bucket);
