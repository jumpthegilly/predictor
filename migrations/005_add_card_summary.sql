-- Migration 005: Add card_summary column to events table

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS card_summary TEXT;
