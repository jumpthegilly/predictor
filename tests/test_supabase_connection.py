"""
Test that the Supabase client can connect and all expected tables exist.
Requires a live Supabase project and the migration in migrations/001_initial_schema.sql
to have been applied.
"""
import pytest
from db.client import get_supabase_client

EXPECTED_TABLES = ["fighters", "events", "bouts", "signal_logs", "reports"]


def test_supabase_client_initialises():
    """Client is created without raising."""
    client = get_supabase_client()
    assert client is not None


def test_expected_tables_exist():
    """Each domain table is queryable — confirms the migration has been applied."""
    client = get_supabase_client()
    for table in EXPECTED_TABLES:
        response = client.table(table).select("*").limit(1).execute()
        assert response.data is not None, f"Table '{table}' did not return a data payload"
