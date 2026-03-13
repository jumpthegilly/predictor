"""
TDD tests for scripts/load_ufc_data.py.
Validates that the CSV loader runs cleanly and persists data to Supabase.

Uses limit=10 so tests stay fast — enough to prove the pipeline works.
"""
import pytest
from scripts.load_ufc_data import load_all
from db.client import get_supabase_client


def test_load_runs_without_error():
    """load_all() completes without raising for a small sample."""
    load_all(limit=10)


def test_at_least_one_fighter_exists_after_load():
    """After loading, the fighters table contains at least one row."""
    load_all(limit=10)
    client = get_supabase_client()
    response = client.table("fighters").select("id").limit(1).execute()
    assert len(response.data) >= 1, "Expected at least one fighter in Supabase after load"
