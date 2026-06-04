-- H5 Primary metadata for rebound virtual trades.
-- Additive only: existing trades keep their original exit behavior unless tagged as H5.

ALTER TABLE virtual_trades
    ADD COLUMN IF NOT EXISTS case_key text,
    ADD COLUMN IF NOT EXISTS case_label text,
    ADD COLUMN IF NOT EXISTS is_primary_h5 boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS exit_rule text,
    ADD COLUMN IF NOT EXISTS peak_pullback_pct numeric,
    ADD COLUMN IF NOT EXISTS initial_sl_pct numeric,
    ADD COLUMN IF NOT EXISTS max_holding_days integer,
    ADD COLUMN IF NOT EXISTS peak_price numeric,
    ADD COLUMN IF NOT EXISTS peak_price_at timestamptz,
    ADD COLUMN IF NOT EXISTS entry_drop_from_20d_high_pct numeric,
    ADD COLUMN IF NOT EXISTS entry_overheat_score integer,
    ADD COLUMN IF NOT EXISTS margin_ratio numeric,
    ADD COLUMN IF NOT EXISTS margin_date date,
    ADD COLUMN IF NOT EXISTS virtual_entry_price numeric,
    ADD COLUMN IF NOT EXISTS virtual_entry_model text,
    ADD COLUMN IF NOT EXISTS virtual_entry_date timestamptz,
    ADD COLUMN IF NOT EXISTS actual_entry_price numeric,
    ADD COLUMN IF NOT EXISTS actual_entry_date timestamptz,
    ADD COLUMN IF NOT EXISTS entry_slippage_pct numeric,
    ADD COLUMN IF NOT EXISTS gap_pct numeric,
    ADD COLUMN IF NOT EXISTS actual_order_type text,
    ADD COLUMN IF NOT EXISTS actual_fill_status text,
    ADD COLUMN IF NOT EXISTS skip_reason text,
    ADD COLUMN IF NOT EXISTS virtual_exit_price numeric,
    ADD COLUMN IF NOT EXISTS actual_exit_price numeric,
    ADD COLUMN IF NOT EXISTS virtual_pnl_pct numeric,
    ADD COLUMN IF NOT EXISTS actual_pnl_pct numeric,
    ADD COLUMN IF NOT EXISTS actual_exit_date timestamptz,
    ADD COLUMN IF NOT EXISTS actual_note text,
    ADD COLUMN IF NOT EXISTS position_limit_mode text,
    ADD COLUMN IF NOT EXISTS is_h5_research boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_limited boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_research_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS live_candidate_rank integer,
    ADD COLUMN IF NOT EXISTS live_case_key text,
    ADD COLUMN IF NOT EXISTS h5_base_case_key text,
    ADD COLUMN IF NOT EXISTS h5_research_case_key text,
    ADD COLUMN IF NOT EXISTS h5_live_case_key text,
    ADD COLUMN IF NOT EXISTS selected_rank integer,
    ADD COLUMN IF NOT EXISTS live_allocation_bucket text,
    ADD COLUMN IF NOT EXISTS allocation_rank integer,
    ADD COLUMN IF NOT EXISTS live_allocation_mode text,
    ADD COLUMN IF NOT EXISTS live_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_candidate_count integer,
    ADD COLUMN IF NOT EXISTS h5_selected_count integer,
    ADD COLUMN IF NOT EXISTS signal_price numeric,
    ADD COLUMN IF NOT EXISTS entry_limit_2pct numeric,
    ADD COLUMN IF NOT EXISTS entry_limit_3pct numeric,
    ADD COLUMN IF NOT EXISTS current_price_yf numeric,
    ADD COLUMN IF NOT EXISTS current_price_fetched_at timestamptz,
    ADD COLUMN IF NOT EXISTS entry_gap_pct numeric,
    ADD COLUMN IF NOT EXISTS entry_status text,
    ADD COLUMN IF NOT EXISTS entry_status_label text,
    ADD COLUMN IF NOT EXISTS price_source text,
    ADD COLUMN IF NOT EXISTS price_fetch_error text;

ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS h5_case_key text,
    ADD COLUMN IF NOT EXISTS h5_primary_match boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS h5_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_overheat_score integer,
    ADD COLUMN IF NOT EXISTS position_limit_mode text,
    ADD COLUMN IF NOT EXISTS is_h5_research boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_limited boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_research_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_h5_live_candidate boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS live_candidate_rank integer,
    ADD COLUMN IF NOT EXISTS live_case_key text,
    ADD COLUMN IF NOT EXISTS h5_base_case_key text,
    ADD COLUMN IF NOT EXISTS h5_research_case_key text,
    ADD COLUMN IF NOT EXISTS h5_live_case_key text,
    ADD COLUMN IF NOT EXISTS selected_rank integer,
    ADD COLUMN IF NOT EXISTS live_allocation_bucket text,
    ADD COLUMN IF NOT EXISTS allocation_rank integer,
    ADD COLUMN IF NOT EXISTS live_allocation_mode text,
    ADD COLUMN IF NOT EXISTS live_skip_reason text,
    ADD COLUMN IF NOT EXISTS h5_candidate_count integer,
    ADD COLUMN IF NOT EXISTS h5_selected_count integer;

CREATE INDEX IF NOT EXISTS idx_virtual_trades_case_key_status
    ON virtual_trades (case_key, status, buy_date DESC);

CREATE TABLE IF NOT EXISTS h5_watchlist (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    watch_date date NOT NULL,
    code text NOT NULL,
    name text,

    ai_score numeric,
    signal_probability numeric,
    signal_stage text,

    high_20d numeric,
    close_price numeric,
    h5_trigger_price numeric,
    distance_to_trigger_pct numeric,
    drop_from_20d_high_pct numeric,

    market_regime text,
    overheat_score integer,
    overheat_bucket text,
    margin_ratio numeric,
    volume_ratio numeric,
    liquidity numeric,

    current_price numeric,
    current_price_yf numeric,
    current_price_source text,
    current_price_fetched_at timestamptz,
    current_distance_to_trigger_pct numeric,

    watch_status text DEFAULT 'watch',
    intraday_h5_checked_at timestamptz,
    intraday_h5_reason text,
    promoted_virtual_trade_id uuid,
    promoted_at timestamptz,

    reject_reason text,
    memo text,

    UNIQUE (watch_date, code)
);

ALTER TABLE h5_watchlist
    ADD COLUMN IF NOT EXISTS signal_probability numeric,
    ADD COLUMN IF NOT EXISTS current_price numeric,
    ADD COLUMN IF NOT EXISTS current_price_yf numeric,
    ADD COLUMN IF NOT EXISTS current_price_source text,
    ADD COLUMN IF NOT EXISTS current_price_fetched_at timestamptz,
    ADD COLUMN IF NOT EXISTS current_distance_to_trigger_pct numeric,
    ADD COLUMN IF NOT EXISTS intraday_h5_checked_at timestamptz,
    ADD COLUMN IF NOT EXISTS intraday_h5_reason text;

CREATE INDEX IF NOT EXISTS idx_h5_watchlist_status_date
    ON h5_watchlist (watch_status, watch_date DESC);

CREATE INDEX IF NOT EXISTS idx_h5_watchlist_code_date
    ON h5_watchlist (code, watch_date DESC);

CREATE TABLE IF NOT EXISTS trade_execution_reviews (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  trade_date date,
  code text,
  name text,
  review_type text NOT NULL,
  case_key text,
  virtual_trade_id uuid,
  signal_price numeric,
  actual_price numeric,
  missed_entry_price numeric,
  exit_price_after numeric,
  expected_action text,
  actual_action text,
  reason_category text,
  reason_emotion text,
  result_summary text,
  opportunity_loss_pct numeric,
  actual_loss_pct numeric,
  lesson text,
  prevention_rule text,
  free_text text,
  status text DEFAULT 'open',
  reviewed_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_trade_execution_reviews_status_created
    ON trade_execution_reviews (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_execution_reviews_code_date
    ON trade_execution_reviews (code, trade_date DESC);

-- Kept for old environments. The app now writes trade_execution_reviews.
CREATE TABLE IF NOT EXISTS trade_mistake_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  trade_date date,
  code text,
  name text,
  mistake_type text,
  case_key text,
  virtual_trade_id uuid,
  signal_price numeric,
  actual_price numeric,
  missed_entry_price numeric,
  exit_price_after numeric,
  expected_action text,
  actual_action text,
  reason_emotion text,
  result_summary text,
  opportunity_loss_pct numeric,
  actual_loss_pct numeric,
  lesson text,
  prevention_rule text,
  status text DEFAULT 'open',
  reviewed_at timestamptz
);

CREATE TABLE IF NOT EXISTS actual_trade_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  virtual_trade_id uuid,
  watchlist_id uuid,
  case_key text,
  trade_date date,
  code text,
  name text,
  virtual_entry_price numeric,
  actual_entry_price numeric,
  actual_entry_date timestamptz,
  actual_entry_time timestamptz,
  actual_order_type text,
  actual_fill_status text,
  entry_reason text,
  h5_trigger_price numeric,
  current_price_at_judgement numeric,
  current_distance_to_trigger_pct numeric,
  intraday_h5_status text,
  virtual_exit_price numeric,
  actual_exit_price numeric,
  actual_exit_date timestamptz,
  virtual_pnl_pct numeric,
  actual_pnl_pct numeric,
  entry_slippage_pct numeric,
  lot_amount numeric,
  quantity numeric,
  skip_reason text,
  note text
);

ALTER TABLE actual_trade_logs
    ADD COLUMN IF NOT EXISTS watchlist_id uuid,
    ADD COLUMN IF NOT EXISTS actual_entry_time timestamptz,
    ADD COLUMN IF NOT EXISTS entry_reason text,
    ADD COLUMN IF NOT EXISTS h5_trigger_price numeric,
    ADD COLUMN IF NOT EXISTS current_price_at_judgement numeric,
    ADD COLUMN IF NOT EXISTS current_distance_to_trigger_pct numeric,
    ADD COLUMN IF NOT EXISTS intraday_h5_status text;

CREATE INDEX IF NOT EXISTS idx_actual_trade_logs_code_date
    ON actual_trade_logs (code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_actual_trade_logs_virtual_trade_id
    ON actual_trade_logs (virtual_trade_id);

NOTIFY pgrst, 'reload schema';
