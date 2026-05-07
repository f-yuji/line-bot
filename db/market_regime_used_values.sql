-- Add columns to store the actual market values used in regime judgment
ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS market_nikkei_pct numeric,
    ADD COLUMN IF NOT EXISTS market_topix_pct numeric,
    ADD COLUMN IF NOT EXISTS market_nikkei_change_yen numeric;

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS market_nikkei_pct numeric,
    ADD COLUMN IF NOT EXISTS market_topix_pct numeric,
    ADD COLUMN IF NOT EXISTS market_nikkei_change_yen numeric;

NOTIFY pgrst, 'reload schema';
