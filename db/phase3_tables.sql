-- =====================================================
-- Phase 3 テーブル定義 / カラム追加
-- Supabase の SQL Editor に貼り付けて実行する
-- =====================================================

-- ─── 1. stock_drop_watchlist にスコアカラム追加 ───
ALTER TABLE stock_drop_watchlist
    ADD COLUMN IF NOT EXISTS score              float,
    ADD COLUMN IF NOT EXISTS score_technical   float,
    ADD COLUMN IF NOT EXISTS score_fundamental float,
    ADD COLUMN IF NOT EXISTS score_market      float,
    ADD COLUMN IF NOT EXISTS score_label       text,
    ADD COLUMN IF NOT EXISTS has_bad_news      boolean DEFAULT false;


-- ─── 2. 仮想売買テーブル ────────────────────────────
CREATE TABLE IF NOT EXISTS virtual_trades (
    id               uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
    watchlist_id     uuid,                               -- stock_drop_watchlist.id（任意）
    code             text        NOT NULL,
    name             text,
    buy_price        float       NOT NULL,
    buy_date         timestamptz NOT NULL,
    sell_price       float,
    sell_date        timestamptz,
    quantity         int         NOT NULL DEFAULT 100,
    buy_score        float,
    sell_reason      text        CHECK (sell_reason IN ('take_profit', 'stop_loss', 'expired', 'manual')),
    profit_loss      float,                              -- 損益額（円）
    profit_loss_pct  float,                              -- 損益率（%）
    status           text        NOT NULL DEFAULT 'open'
                                 CHECK (status IN ('open', 'closed')),
    created_at       timestamptz DEFAULT now(),
    updated_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_virtual_trades_code_status ON virtual_trades (code, status);
CREATE INDEX IF NOT EXISTS idx_virtual_trades_status      ON virtual_trades (status);


-- ─── 3. TSEプライム銘柄キャッシュ ──────────────────
CREATE TABLE IF NOT EXISTS prime_stocks_cache (
    code       text        PRIMARY KEY,
    name       text,
    sector     text,
    updated_at timestamptz DEFAULT now()
);
