-- ============================================================
-- Migration 002 — Unique constraints for idempotent upserts
-- Apply in: Supabase Dashboard → SQL Editor → Run
-- ============================================================

ALTER TABLE fighters
    ADD CONSTRAINT fighters_name_unique UNIQUE (name);

ALTER TABLE events
    ADD CONSTRAINT events_name_date_unique UNIQUE (name, date);

ALTER TABLE bouts
    ADD CONSTRAINT bouts_event_fighters_unique
        UNIQUE (event_id, fighter_a_id, fighter_b_id);
