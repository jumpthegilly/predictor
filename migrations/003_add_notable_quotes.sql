-- Migration 003: Add notable_quotes column to signal_logs
-- Stores notable direct quotes or paraphrased statements extracted from news articles.

ALTER TABLE signal_logs
    ADD COLUMN IF NOT EXISTS notable_quotes text[] DEFAULT '{}';
