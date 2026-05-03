-- =====================================================
-- Phase 1 テーブル定義
-- Supabase の SQL Editor に貼り付けて実行する
-- =====================================================

-- ─── 1. 急落株 watchlist ───────────────────────────
CREATE TABLE IF NOT EXISTS stock_drop_watchlist (
    id               uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
    code             text        NOT NULL,
    name             text,
    market           text        DEFAULT 'nikkei225',   -- nikkei225 / prime / dow
    source_index     text        DEFAULT 'nikkei',      -- nikkei / dow
    drop_detected_at timestamptz NOT NULL,
    drop_pct         float,                             -- 急落率（%）
    price_at_drop    float,                             -- 急落時の株価
    volume_at_drop   float,                             -- 急落時の出来高（将来拡張用）
    nikkei_pct       float,                             -- 急落時の日経平均騰落率
    dow_pct          float,                             -- 急落時のダウ騰落率（将来拡張用）
    sector           text,
    status           text        NOT NULL DEFAULT 'watching'
                                 CHECK (status IN ('watching', 'rebound_signal', 'notified', 'closed')),
    last_checked_at  timestamptz,
    rebound_notified_at timestamptz,
    memo             text,
    created_at       timestamptz DEFAULT now(),
    updated_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_watchlist_code_status
    ON stock_drop_watchlist (code, status);

CREATE INDEX IF NOT EXISTS idx_watchlist_status
    ON stock_drop_watchlist (status);

CREATE INDEX IF NOT EXISTS idx_watchlist_drop_detected_at
    ON stock_drop_watchlist (drop_detected_at DESC);


-- ─── 2. 戦略設定 ──────────────────────────────────
-- user_id = 'global' が唯一のグローバル設定レコード
-- 将来ユーザー別設定に拡張可能
CREATE TABLE IF NOT EXISTS strategy_settings (
    id                       uuid    DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id                  text    NOT NULL DEFAULT 'global',

    -- 急落検知閾値
    drop_list_threshold      float   NOT NULL DEFAULT -2.0,   -- watchlist登録閾値（%）
    alert_threshold          float   NOT NULL DEFAULT -9.0,   -- push通知閾値（%）
    index_gap_threshold      float   NOT NULL DEFAULT -1.5,   -- 指数乖離閾値（%pt）

    -- リバウンド判定
    daily_rebound_threshold  float   NOT NULL DEFAULT 3.0,    -- 前日比反発率（%）
    drop_rebound_threshold   float   NOT NULL DEFAULT 5.0,    -- 急落時からの反発率（%）
    volume_ratio_threshold   float   NOT NULL DEFAULT 1.5,    -- 出来高倍率（20日平均比）
    rsi_low_threshold        float   NOT NULL DEFAULT 30.0,   -- RSI下限（売られすぎ判定）
    rsi_recover_threshold    float   NOT NULL DEFAULT 35.0,   -- RSI回復ライン
    ma5_cross_enabled        boolean NOT NULL DEFAULT true,   -- 5日線上抜けを条件に含める

    -- 監視期限
    watch_days_limit         int     NOT NULL DEFAULT 10,     -- 監視終了までの営業日数

    -- スコアリング配点
    technical_score_weight   float   NOT NULL DEFAULT 50.0,
    fundamental_score_weight float   NOT NULL DEFAULT 30.0,
    market_score_weight      float   NOT NULL DEFAULT 20.0,
    strong_watch_score       float   NOT NULL DEFAULT 80.0,   -- 強監視スコア閾値
    watch_score              float   NOT NULL DEFAULT 70.0,   -- 監視スコア閾値
    ignore_score             float   NOT NULL DEFAULT 60.0,   -- スルースコア閾値

    -- 通知設定
    drop_notify_enabled      boolean NOT NULL DEFAULT true,
    rebound_notify_enabled   boolean NOT NULL DEFAULT true,
    morning_summary_enabled  boolean NOT NULL DEFAULT true,
    portfolio_notify_enabled boolean NOT NULL DEFAULT true,

    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_settings_user_id
    ON strategy_settings (user_id);

-- グローバル設定の初期レコードを投入（存在しない場合のみ）
INSERT INTO strategy_settings (user_id)
VALUES ('global')
ON CONFLICT (user_id) DO NOTHING;
