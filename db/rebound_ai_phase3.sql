-- =====================================================
-- Rebound AI system phase 3
-- Success/failure labels for rebound training data.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

CREATE TABLE IF NOT EXISTS stock_rebound_labels (
    id bigserial PRIMARY KEY,

    feature_snapshot_id bigint REFERENCES stock_feature_snapshots(id),
    trade_date date NOT NULL,
    code text NOT NULL,
    name text,
    market text DEFAULT 'prime',
    sector text,

    entry_price numeric NOT NULL,
    entry_basis text DEFAULT 'close',

    future_high_1d numeric,
    future_high_2d numeric,
    future_high_3d numeric,
    future_high_4d numeric,
    future_high_5d numeric,

    future_low_1d numeric,
    future_low_2d numeric,
    future_low_3d numeric,
    future_low_4d numeric,
    future_low_5d numeric,

    future_close_1d numeric,
    future_close_2d numeric,
    future_close_3d numeric,
    future_close_4d numeric,
    future_close_5d numeric,

    max_return_5d_pct numeric,
    max_drawdown_5d_pct numeric,

    take_profit_pct numeric DEFAULT 5.0,
    stop_loss_pct numeric DEFAULT -4.0,
    holding_days integer DEFAULT 5,

    take_profit_price numeric,
    stop_loss_price numeric,

    hit_take_profit boolean DEFAULT false,
    hit_stop_loss boolean DEFAULT false,

    take_profit_day integer,
    stop_loss_day integer,

    label_success boolean,
    label_reason text,

    is_valid_label boolean DEFAULT true,
    invalid_reason text,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_code
    ON stock_rebound_labels (code);
CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_trade_date
    ON stock_rebound_labels (trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_code_date
    ON stock_rebound_labels (code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_success
    ON stock_rebound_labels (label_success);
CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_valid
    ON stock_rebound_labels (is_valid_label);
CREATE INDEX IF NOT EXISTS idx_stock_rebound_labels_feature_snapshot_id
    ON stock_rebound_labels (feature_snapshot_id);
