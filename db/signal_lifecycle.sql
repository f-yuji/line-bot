-- Signal lifecycle columns for stock_drop_watchlist.
-- Safe additive migration: no existing columns are removed.

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS entered_at timestamptz,
    ADD COLUMN IF NOT EXISTS closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS close_reason text,
    ADD COLUMN IF NOT EXISTS virtual_trade_id text,
    ADD COLUMN IF NOT EXISTS signal_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS signal_status_reason text;

CREATE INDEX IF NOT EXISTS idx_stock_drop_watchlist_signal_lifecycle
    ON stock_drop_watchlist (status, signal_stage, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_drop_watchlist_virtual_trade_id
    ON stock_drop_watchlist (virtual_trade_id);

