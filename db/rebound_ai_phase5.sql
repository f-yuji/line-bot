-- =====================================================
-- Rebound AI system phase 5
-- ML model registry.
-- Safe to run multiple times in Supabase SQL Editor.
-- =====================================================

CREATE TABLE IF NOT EXISTS ml_models (
    id bigserial PRIMARY KEY,

    model_name text NOT NULL,
    model_version text NOT NULL,
    target_name text NOT NULL DEFAULT 'rebound_5d_5pct',

    train_start date,
    train_end date,
    valid_start date,
    valid_end date,

    features jsonb DEFAULT '[]'::jsonb,
    params jsonb DEFAULT '{}'::jsonb,
    metrics jsonb DEFAULT '{}'::jsonb,

    model_path text NOT NULL,
    feature_path text,
    importance_path text,

    is_active boolean DEFAULT false,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    UNIQUE (model_name, model_version)
);

CREATE INDEX IF NOT EXISTS idx_ml_models_name
    ON ml_models (model_name);
CREATE INDEX IF NOT EXISTS idx_ml_models_active
    ON ml_models (is_active);
CREATE INDEX IF NOT EXISTS idx_ml_models_created_at
    ON ml_models (created_at);
