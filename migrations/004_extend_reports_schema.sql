-- Migration 004: Extend reports table with prediction detail columns

ALTER TABLE reports
    ADD COLUMN IF NOT EXISTS win_probability  NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS method_prediction TEXT,
    ADD COLUMN IF NOT EXISTS key_factors       TEXT[]  DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS red_flags         TEXT[]  DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS green_flags       TEXT[]  DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS statistical_edge  TEXT,
    ADD COLUMN IF NOT EXISTS intangibles_edge  TEXT;
