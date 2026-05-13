-- Virtual trade exit state for pullback/RSI/MA5 based close rules.
-- Additive migration except for widening the sell_reason CHECK constraint.

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS highest_close numeric,
    ADD COLUMN IF NOT EXISTS highest_close_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_high_update_at timestamptz,
    ADD COLUMN IF NOT EXISTS rsi75_touched boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS rsi75_touched_at timestamptz,
    ADD COLUMN IF NOT EXISTS ma5_recovered boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ma5_recovered_at timestamptz,
    ADD COLUMN IF NOT EXISTS exit_mode text,
    ADD COLUMN IF NOT EXISTS exit_trigger_value numeric;

ALTER TABLE virtual_trades
    DROP CONSTRAINT IF EXISTS virtual_trades_sell_reason_check;

ALTER TABLE virtual_trades
    ADD CONSTRAINT virtual_trades_sell_reason_check
    CHECK (
        sell_reason IS NULL OR sell_reason IN (
            'take_profit',
            'stop_loss',
            'expired',
            'manual',
            'manual_closed',
            'excluded_after_entry',
            'pullback2',
            'rsi75_pullback1',
            'stop_loss_4pct',
            'ma5_failed_recovery',
            'holding_timeout'
        )
    );

CREATE INDEX IF NOT EXISTS idx_virtual_trades_exit_state
    ON virtual_trades (status, exit_checked_at DESC);

ALTER TABLE strategy_settings
    ADD COLUMN IF NOT EXISTS virtual_exit_pullback_pct numeric DEFAULT 2.0,
    ADD COLUMN IF NOT EXISTS virtual_exit_rsi_level numeric DEFAULT 75.0,
    ADD COLUMN IF NOT EXISTS virtual_exit_rsi_pullback_pct numeric DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS virtual_exit_stop_loss_pct numeric DEFAULT 4.0,
    ADD COLUMN IF NOT EXISTS virtual_exit_ma5_failure_pct numeric DEFAULT 2.0,
    ADD COLUMN IF NOT EXISTS virtual_exit_holding_days integer DEFAULT 5,
    ADD COLUMN IF NOT EXISTS virtual_exit_extend_high_update_days integer DEFAULT 2;

UPDATE strategy_settings
SET
    virtual_exit_pullback_pct = COALESCE(virtual_exit_pullback_pct, 2.0),
    virtual_exit_rsi_level = COALESCE(virtual_exit_rsi_level, 75.0),
    virtual_exit_rsi_pullback_pct = COALESCE(virtual_exit_rsi_pullback_pct, 1.0),
    virtual_exit_stop_loss_pct = COALESCE(virtual_exit_stop_loss_pct, 4.0),
    virtual_exit_ma5_failure_pct = COALESCE(virtual_exit_ma5_failure_pct, 2.0),
    virtual_exit_holding_days = COALESCE(virtual_exit_holding_days, 5),
    virtual_exit_extend_high_update_days = COALESCE(virtual_exit_extend_high_update_days, 2)
WHERE user_id = 'global';
