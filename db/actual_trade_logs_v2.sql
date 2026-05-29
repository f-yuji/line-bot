-- actual_trade_logs v2: H5実弾Exit管理のための追加カラム
-- すべて ADD COLUMN IF NOT EXISTS なので既存環境に安全に適用可能

ALTER TABLE actual_trade_logs
    ADD COLUMN IF NOT EXISTS actual_entry_model text,
    ADD COLUMN IF NOT EXISTS signal_date date,
    ADD COLUMN IF NOT EXISTS signal_price numeric,
    ADD COLUMN IF NOT EXISTS virtual_entry_date date,
    ADD COLUMN IF NOT EXISTS virtual_exit_due_date date,
    ADD COLUMN IF NOT EXISTS actual_day1_date date,
    ADD COLUMN IF NOT EXISTS actual_day2_date date,
    ADD COLUMN IF NOT EXISTS actual_day3_exit_due_date date,
    ADD COLUMN IF NOT EXISTS actual_emergency_stop_price numeric,
    ADD COLUMN IF NOT EXISTS actual_exit_status text DEFAULT 'holding',
    ADD COLUMN IF NOT EXISTS actual_exit_reason text,
    ADD COLUMN IF NOT EXISTS actual_exit_due_reason text DEFAULT 'hd3_time_stop',
    ADD COLUMN IF NOT EXISTS actual_pnl_amount numeric,
    ADD COLUMN IF NOT EXISTS actual_exit_time timestamptz;

-- actual_exit_status の想定値:
--   holding / time_stopped / stopped / peak_pullback_exited / exited / skipped / cancelled

-- actual_exit_reason の想定値:
--   hd3_time_stop / emergency_stop_12 / peak_pullback_2 / manual_exit / other

-- actual_entry_model の想定値:
--   same_day_close / next_open / manual_intraday / manual_close / other

NOTIFY pgrst, 'reload schema';
