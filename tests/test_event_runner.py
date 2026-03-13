"""
TDD tests for src/pipeline/event_runner.py

All Supabase and pipeline calls are mocked — no real network or DB access.
"""
from unittest.mock import MagicMock, patch, call

import pytest

from src.pipeline.event_runner import run_event_pipeline

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_ID = "event-uuid-001"

FIGHTER_ID_A = "fighter-uuid-a"
FIGHTER_ID_B = "fighter-uuid-b"
FIGHTER_NAME_A = "Islam Makhachev"
FIGHTER_NAME_B = "Charles Oliveira"

MOCK_BOUTS = [
    {"fighter_a_id": FIGHTER_ID_A, "fighter_b_id": FIGHTER_ID_B},
]

FIGHTER_NAME_MAP = {
    FIGHTER_ID_A: FIGHTER_NAME_A,
    FIGHTER_ID_B: FIGHTER_NAME_B,
}

MOCK_PIPELINE_SUCCESS = {
    "fighter_name": FIGHTER_NAME_A,
    "fighter_id": FIGHTER_ID_A,
    "event_id": EVENT_ID,
    "articles_found": 3,
    "markets_found": 2,
    "news_signals_stored": True,
    "market_signals_stored": True,
    "errors": [],
}

# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

PATCH_SUPABASE  = "src.pipeline.event_runner.get_supabase_client"
PATCH_PIPELINE  = "src.pipeline.event_runner.run_signal_pipeline"
PATCH_SLEEP     = "src.pipeline.event_runner.time.sleep"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_supabase_mock(bouts_data, name_map=None):
    """
    Build a MagicMock Supabase client.

    - client.table("bouts").select(...).eq(...).execute().data == bouts_data
    - client.table("fighters").select(...).eq(col, fighter_id).execute().data
      == [{"name": name_map[fighter_id]}]
    """
    if name_map is None:
        name_map = FIGHTER_NAME_MAP

    client = MagicMock()

    def table_side_effect(table_name):
        tbl = MagicMock()
        if table_name == "bouts":
            tbl.select.return_value.eq.return_value.execute.return_value.data = bouts_data
        elif table_name == "fighters":
            def fighters_eq(col, val):
                eq_mock = MagicMock()
                eq_mock.execute.return_value.data = [{"name": name_map.get(val, "Unknown")}]
                return eq_mock
            tbl.select.return_value.eq.side_effect = fighters_eq
        return tbl

    client.table.side_effect = table_side_effect
    return client


def _pipeline_success_result(fighter_name, fighter_id, event_id):
    return {
        "fighter_name": fighter_name,
        "fighter_id": fighter_id,
        "event_id": event_id,
        "articles_found": 2,
        "markets_found": 1,
        "news_signals_stored": True,
        "market_signals_stored": True,
        "errors": [],
    }


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestReturnShape:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_returns_dict(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        assert isinstance(result, dict)

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_contains_required_keys(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        for key in ("event_id", "fighters_processed", "fighters_failed",
                    "total_signals_stored", "errors"):
            assert key in result, f"Missing key: {key}"

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_event_id_echoed(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        assert result["event_id"] == EVENT_ID


# ---------------------------------------------------------------------------
# Full card run — all fighters succeed
# ---------------------------------------------------------------------------


class TestFullCardRun:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_processes_both_fighters(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        assert result["fighters_processed"] == 2

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_no_failures_on_clean_run(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        assert result["fighters_failed"] == 0
        assert result["errors"] == []

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_counts_signals_stored(self, MockSupa, MockPipeline, MockSleep):
        """2 fighters × (news + market) = 4 signals stored."""
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        result = run_event_pipeline(EVENT_ID)
        assert result["total_signals_stored"] == 4

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_pipeline_called_for_each_fighter(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        assert MockPipeline.call_count == 2

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_pipeline_receives_correct_event_id(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        for c in MockPipeline.call_args_list:
            assert c.args[2] == EVENT_ID

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_deduplicates_fighter_ids(self, MockSupa, MockPipeline, MockSleep):
        """Fighter appearing in multiple bouts is only processed once."""
        bouts_with_duplicate = [
            {"fighter_a_id": FIGHTER_ID_A, "fighter_b_id": FIGHTER_ID_B},
            {"fighter_a_id": FIGHTER_ID_A, "fighter_b_id": FIGHTER_ID_B},
        ]
        MockSupa.return_value = _make_supabase_mock(bouts_with_duplicate)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        assert MockPipeline.call_count == 2  # still only 2 unique fighters


# ---------------------------------------------------------------------------
# Rate-limit delay
# ---------------------------------------------------------------------------


class TestRateLimitDelay:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_sleep_called_between_fighters(self, MockSupa, MockPipeline, MockSleep):
        """2 fighters → exactly 1 sleep call (before the second fighter)."""
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        assert MockSleep.call_count == 1

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_sleep_duration_is_two_seconds(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        MockSleep.assert_called_with(2)

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_no_sleep_for_single_fighter(self, MockSupa, MockPipeline, MockSleep):
        single_fighter_bouts = [{"fighter_a_id": FIGHTER_ID_A, "fighter_b_id": FIGHTER_ID_A}]
        name_map = {FIGHTER_ID_A: FIGHTER_NAME_A}
        MockSupa.return_value = _make_supabase_mock(single_fighter_bouts, name_map)
        MockPipeline.side_effect = _pipeline_success_result
        run_event_pipeline(EVENT_ID)
        MockSleep.assert_not_called()


# ---------------------------------------------------------------------------
# Partial failure — one fighter's pipeline raises
# ---------------------------------------------------------------------------


class TestPartialFailure:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_one_failure_does_not_abort(self, MockSupa, MockPipeline, MockSleep):
        """Pipeline runs for both fighters even if one raises."""
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)

        def flaky(fighter_name, fighter_id, event_id):
            if fighter_id == FIGHTER_ID_B:
                raise Exception("pipeline crashed")
            return _pipeline_success_result(fighter_name, fighter_id, event_id)

        MockPipeline.side_effect = flaky
        result = run_event_pipeline(EVENT_ID)
        assert MockPipeline.call_count == 2

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_failed_fighter_counted(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)

        def flaky(fighter_name, fighter_id, event_id):
            if fighter_id == FIGHTER_ID_B:
                raise Exception("pipeline crashed")
            return _pipeline_success_result(fighter_name, fighter_id, event_id)

        MockPipeline.side_effect = flaky
        result = run_event_pipeline(EVENT_ID)
        assert result["fighters_failed"] == 1
        assert result["fighters_processed"] == 1

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_error_recorded_for_failed_fighter(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)

        def flaky(fighter_name, fighter_id, event_id):
            if fighter_id == FIGHTER_ID_B:
                raise Exception("pipeline crashed")
            return _pipeline_success_result(fighter_name, fighter_id, event_id)

        MockPipeline.side_effect = flaky
        result = run_event_pipeline(EVENT_ID)
        assert len(result["errors"]) == 1

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_successful_signals_still_counted_on_partial_failure(
        self, MockSupa, MockPipeline, MockSleep
    ):
        MockSupa.return_value = _make_supabase_mock(MOCK_BOUTS)

        def flaky(fighter_name, fighter_id, event_id):
            if fighter_id == FIGHTER_ID_B:
                raise Exception("pipeline crashed")
            return _pipeline_success_result(fighter_name, fighter_id, event_id)

        MockPipeline.side_effect = flaky
        result = run_event_pipeline(EVENT_ID)
        # One fighter succeeded with news + market stored
        assert result["total_signals_stored"] == 2


# ---------------------------------------------------------------------------
# Empty event — no bouts found
# ---------------------------------------------------------------------------


class TestEmptyEvent:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_empty_event_returns_zeroes(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock([])
        result = run_event_pipeline(EVENT_ID)
        assert result["fighters_processed"] == 0
        assert result["fighters_failed"] == 0
        assert result["total_signals_stored"] == 0
        assert result["errors"] == []

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_empty_event_does_not_call_pipeline(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock([])
        run_event_pipeline(EVENT_ID)
        MockPipeline.assert_not_called()

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_empty_event_does_not_sleep(self, MockSupa, MockPipeline, MockSleep):
        MockSupa.return_value = _make_supabase_mock([])
        run_event_pipeline(EVENT_ID)
        MockSleep.assert_not_called()


# ---------------------------------------------------------------------------
# Supabase query failure
# ---------------------------------------------------------------------------


class TestSupabaseFailure:
    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_bouts_query_failure_returns_dict(self, MockSupa, MockPipeline, MockSleep):
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.execute.side_effect = (
            Exception("DB connection failed")
        )
        MockSupa.return_value = client
        result = run_event_pipeline(EVENT_ID)
        assert isinstance(result, dict)

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_bouts_query_failure_records_error(self, MockSupa, MockPipeline, MockSleep):
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.execute.side_effect = (
            Exception("DB connection failed")
        )
        MockSupa.return_value = client
        result = run_event_pipeline(EVENT_ID)
        assert len(result["errors"]) >= 1

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_bouts_query_failure_does_not_call_pipeline(
        self, MockSupa, MockPipeline, MockSleep
    ):
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.execute.side_effect = (
            Exception("DB connection failed")
        )
        MockSupa.return_value = client
        run_event_pipeline(EVENT_ID)
        MockPipeline.assert_not_called()

    @patch(PATCH_SLEEP)
    @patch(PATCH_PIPELINE)
    @patch(PATCH_SUPABASE)
    def test_bouts_query_failure_returns_zero_counts(
        self, MockSupa, MockPipeline, MockSleep
    ):
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.execute.side_effect = (
            Exception("DB connection failed")
        )
        MockSupa.return_value = client
        result = run_event_pipeline(EVENT_ID)
        assert result["fighters_processed"] == 0
        assert result["fighters_failed"] == 0
