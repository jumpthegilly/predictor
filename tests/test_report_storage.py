"""
TDD tests for src/storage/report_storage.py

All Supabase calls are mocked — no database access required.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.storage.report_storage import get_bout_report, get_event_reports

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BOUT_ID = "bout-uuid-001"
EVENT_ID = "event-uuid-001"

MOCK_REPORT = {
    "id": "report-uuid-001",
    "bout_id": BOUT_ID,
    "prediction": "Jon Jones",
    "confidence_tier": "High",
    "win_probability": 0.78,
    "method_prediction": "Decision",
    "key_factors": ["wrestling", "reach"],
    "red_flags": [],
    "green_flags": ["healthy camp"],
    "upset_alert": False,
    "statistical_edge": "Jones has the stats edge.",
    "intangibles_edge": "Jones has the camp edge.",
    "narrative": "Jones wins this fight.",
    "created_at": "2025-11-15T10:00:00Z",
}

MOCK_REPORT_2 = {**MOCK_REPORT, "id": "report-uuid-002", "bout_id": "bout-uuid-002"}


def _make_supabase_mock():
    """Return a mock Supabase client with fluent chain support."""
    client = MagicMock()
    chain = MagicMock()
    client.table.return_value = chain
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.in_.return_value = chain
    chain.order.return_value = chain
    chain.limit.return_value = chain
    return client, chain


# ---------------------------------------------------------------------------
# get_bout_report
# ---------------------------------------------------------------------------


class TestGetBoutReport:
    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_report_when_found(self, mock_client_fn):
        """Returns the report dict when a matching row exists."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[MOCK_REPORT])

        result = get_bout_report(BOUT_ID)

        assert result == MOCK_REPORT

    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_none_when_not_found(self, mock_client_fn):
        """Returns None when no report exists for the bout."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[])

        result = get_bout_report(BOUT_ID)

        assert result is None

    @patch("src.storage.report_storage.get_supabase_client")
    def test_queries_reports_table(self, mock_client_fn):
        """Queries the reports table."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[])

        get_bout_report(BOUT_ID)

        client.table.assert_called_once_with("reports")

    @patch("src.storage.report_storage.get_supabase_client")
    def test_filters_by_bout_id(self, mock_client_fn):
        """Filters results by bout_id."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[])

        get_bout_report(BOUT_ID)

        chain.eq.assert_called_with("bout_id", BOUT_ID)

    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_most_recent_report(self, mock_client_fn):
        """Orders by created_at desc and limits to 1 (returns latest report)."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[MOCK_REPORT])

        get_bout_report(BOUT_ID)

        chain.order.assert_called_with("created_at", desc=True)
        chain.limit.assert_called_with(1)


# ---------------------------------------------------------------------------
# get_event_reports
# ---------------------------------------------------------------------------


class TestGetEventReports:
    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_list_of_reports(self, mock_client_fn):
        """Returns a list of report dicts for the event."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client

        # First call: fetch bouts; second call: fetch reports
        chain.execute.side_effect = [
            MagicMock(data=[{"id": BOUT_ID}, {"id": "bout-uuid-002"}]),
            MagicMock(data=[MOCK_REPORT, MOCK_REPORT_2]),
        ]

        result = get_event_reports(EVENT_ID)

        assert isinstance(result, list)
        assert len(result) == 2

    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_empty_list_when_no_bouts(self, mock_client_fn):
        """Returns [] when the event has no bouts."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.return_value = MagicMock(data=[])

        result = get_event_reports(EVENT_ID)

        assert result == []

    @patch("src.storage.report_storage.get_supabase_client")
    def test_returns_empty_list_when_no_reports(self, mock_client_fn):
        """Returns [] when bouts exist but have no reports."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.side_effect = [
            MagicMock(data=[{"id": BOUT_ID}]),  # bouts found
            MagicMock(data=[]),                  # no reports
        ]

        result = get_event_reports(EVENT_ID)

        assert result == []

    @patch("src.storage.report_storage.get_supabase_client")
    def test_queries_bouts_table_first(self, mock_client_fn):
        """First queries bouts table to get bout IDs for the event."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.side_effect = [
            MagicMock(data=[{"id": BOUT_ID}]),
            MagicMock(data=[MOCK_REPORT]),
        ]

        get_event_reports(EVENT_ID)

        calls = client.table.call_args_list
        assert calls[0].args[0] == "bouts"

    @patch("src.storage.report_storage.get_supabase_client")
    def test_queries_reports_table_second(self, mock_client_fn):
        """Second queries reports table using the collected bout IDs."""
        client, chain = _make_supabase_mock()
        mock_client_fn.return_value = client
        chain.execute.side_effect = [
            MagicMock(data=[{"id": BOUT_ID}]),
            MagicMock(data=[MOCK_REPORT]),
        ]

        get_event_reports(EVENT_ID)

        calls = client.table.call_args_list
        assert calls[1].args[0] == "reports"
