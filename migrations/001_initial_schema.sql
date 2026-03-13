-- ============================================================
-- Migration 001 — Initial schema
-- Apply this in: Supabase Dashboard → SQL Editor → Run
-- ============================================================

CREATE TABLE IF NOT EXISTS fighters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    nickname        TEXT,
    weight_class    TEXT,
    record_wins     INTEGER NOT NULL DEFAULT 0,
    record_losses   INTEGER NOT NULL DEFAULT 0,
    record_draws    INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    date        DATE,
    location    TEXT,
    status      TEXT
);

CREATE TABLE IF NOT EXISTS bouts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID REFERENCES events(id) ON DELETE CASCADE,
    fighter_a_id    UUID REFERENCES fighters(id) ON DELETE SET NULL,
    fighter_b_id    UUID REFERENCES fighters(id) ON DELETE SET NULL,
    weight_class    TEXT,
    is_main_event   BOOLEAN NOT NULL DEFAULT FALSE,
    is_title_fight  BOOLEAN NOT NULL DEFAULT FALSE,
    result          TEXT,
    method          TEXT,
    round           INTEGER,
    time            TEXT
);

CREATE TABLE IF NOT EXISTS signal_logs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fighter_id        UUID REFERENCES fighters(id) ON DELETE SET NULL,
    event_id          UUID REFERENCES events(id) ON DELETE SET NULL,
    source_type       TEXT,
    raw_summary       TEXT,
    injury_flags      BOOLEAN NOT NULL DEFAULT FALSE,
    confidence_score  NUMERIC(4,3),
    red_flags         TEXT[],
    green_flags       TEXT[],
    sentiment_score   NUMERIC(4,3),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reports (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bout_id           UUID REFERENCES bouts(id) ON DELETE CASCADE,
    prediction        TEXT,
    confidence_tier   TEXT,
    narrative         TEXT,
    upset_alert       BOOLEAN NOT NULL DEFAULT FALSE,
    is_published      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
