"""
TDD tests for src/storage/signal_storage.py

The Supabase client is fully mocked — no real database writes occur.
"""
from unittest.mock import MagicMock, call, patch

import pytest

from src.storage.signal_storage import get_latest_signals, get_signal_logs, store_signal_log

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIGHTER_ID = "fighter-uuid-001"
EVENT_ID = "event-uuid-001"
SOURCE_TYPE = "news"

SAMPLE_SIGNALS = {
    "raw_summary": "Jon Jones looks sharp ahead of UFC 309.",
    "injury_flags": False,
    "confidence_score": 0.82,
    "red_flags": [],
    "green_flags": ["strong camp", "no injuries reported"],
    "sentiment_score": 0.75,
    "notable_quotes": ["fighter looks sharp in camp"],
}

SAMPLE_LOG_ROW = {
    "id": "log-uuid-001",
    "fighter_id": FIGHTER_ID,
    "event_id": EVENT_ID,
    "source_type": SOURCE_TYPE,
    "raw_summary": SAMPLE_SIGNALS["raw_summary"],
    "injury_flags": SAMPLE_SIGNALS["injury_flags"],
    "confidence_score": SAMPLE_SIGNALS["confidence_score"],
    "red_flags": SAMPLE_SIGNALS["red_flags"],
    "green_flags": SAMPLE_SIGNALS["green_flags"],
    "sentiment_score": SAMPLE_SIGNALS["sentiment_score"],
    "notable_quotes": SAMPLE_SIGNALS["notable_quotes"],
    "created_at": "2025-11-16T10:00:00Z",
}


def _make_client(data=None):
    """Return a MagicMock Supabase client whose fluent chain returns *data*."""
    if data is None:
        data = []
    mock_response = MagicMock()
    mock_response.data = data

    mock_client = MagicMock()
    # table(...).anything().anything()...execute() always returns mock_response
    mock_client.table.return_value.insert.return_value.execute.return_value = mock_response
    mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_response
    mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = mock_response
    return mock_client


# ---------------------------------------------------------------------------
# store_signal_log
# ---------------------------------------------------------------------------


class TestStoreSignalLog:
    @patch("src.storage.signal_storage.get_supabase_client")
    def test_calls_insert_on_signal_logs_table(self, mock_get_client):
        """store_signal_log calls insert on the signal_logs table."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        store_signal_log(FIGHTER_ID, EVENT_ID, SOURCE_TYPE, SAMPLE_SIGNALS)

        mock_client.table.assert_called_with("signal_logs")
        mock_client.table.return_value.insert.assert_called_once()

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_insert_payload_contains_all_signal_fields(self, mock_get_client):
        """The inserted record maps all processed_signals keys correctly."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        store_signal_log(FIGHTER_ID, EVENT_ID, SOURCE_TYPE, SAMPLE_SIGNALS)

        inserted = mock_client.table.return_value.insert.call_args[0][0]
        assert inserted["fighter_id"] == FIGHTER_ID
        assert inserted["event_id"] == EVENT_ID
        assert inserted["source_type"] == SOURCE_TYPE
        assert inserted["raw_summary"] == SAMPLE_SIGNALS["raw_summary"]
        assert inserted["injury_flags"] == SAMPLE_SIGNALS["injury_flags"]
        assert inserted["confidence_score"] == SAMPLE_SIGNALS["confidence_score"]
        assert inserted["red_flags"] == SAMPLE_SIGNALS["red_flags"]
        assert inserted["green_flags"] == SAMPLE_SIGNALS["green_flags"]
        assert inserted["sentiment_score"] == SAMPLE_SIGNALS["sentiment_score"]
        assert inserted["notable_quotes"] == SAMPLE_SIGNALS["notable_quotes"]

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_inserted_record(self, mock_get_client):
        """store_signal_log returns the first element of response.data."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        result = store_signal_log(FIGHTER_ID, EVENT_ID, SOURCE_TYPE, SAMPLE_SIGNALS)

        assert result == SAMPLE_LOG_ROW

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_empty_dict_when_response_data_is_empty(self, mock_get_client):
        """If Supabase returns no data (e.g. RLS block), return empty dict."""
        mock_client = _make_client([])
        mock_get_client.return_value = mock_client

        result = store_signal_log(FIGHTER_ID, EVENT_ID, SOURCE_TYPE, SAMPLE_SIGNALS)

        assert result == {}

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_missing_optional_fields_default_gracefully(self, mock_get_client):
        """Omitting optional keys from processed_signals does not raise."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        sparse = {"raw_summary": "minimal signal"}
        # Should not raise
        store_signal_log(FIGHTER_ID, EVENT_ID, SOURCE_TYPE, sparse)

        inserted = mock_client.table.return_value.insert.call_args[0][0]
        assert inserted["injury_flags"] is False
        assert inserted["confidence_score"] is None
        assert inserted["red_flags"] == []
        assert inserted["green_flags"] == []
        assert inserted["sentiment_score"] is None
        assert inserted["notable_quotes"] == []


# ---------------------------------------------------------------------------
# get_signal_logs
# ---------------------------------------------------------------------------


class TestGetSignalLogs:
    @patch("src.storage.signal_storage.get_supabase_client")
    def test_queries_signal_logs_table(self, mock_get_client):
        """get_signal_logs queries the signal_logs table."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_signal_logs(FIGHTER_ID, EVENT_ID)

        mock_client.table.assert_called_with("signal_logs")

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_filters_by_fighter_id_and_event_id(self, mock_get_client):
        """Query applies eq filters for both fighter_id and event_id."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_signal_logs(FIGHTER_ID, EVENT_ID)

        eq_chain = mock_client.table.return_value.select.return_value.eq
        first_call = eq_chain.call_args_list[0]
        assert first_call == call("fighter_id", FIGHTER_ID)

        second_call = eq_chain.return_value.eq.call_args_list[0]
        assert second_call == call("event_id", EVENT_ID)

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_list_of_logs(self, mock_get_client):
        """get_signal_logs returns a list."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        result = get_signal_logs(FIGHTER_ID, EVENT_ID)

        assert isinstance(result, list)
        assert result == [SAMPLE_LOG_ROW]

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_empty_list_when_no_logs_found(self, mock_get_client):
        """get_signal_logs returns [] when there are no matching rows."""
        mock_client = _make_client([])
        mock_get_client.return_value = mock_client

        result = get_signal_logs(FIGHTER_ID, EVENT_ID)

        assert result == []


# ---------------------------------------------------------------------------
# get_latest_signals
# ---------------------------------------------------------------------------


class TestGetLatestSignals:
    @patch("src.storage.signal_storage.get_supabase_client")
    def test_queries_signal_logs_table(self, mock_get_client):
        """get_latest_signals queries the signal_logs table."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_latest_signals(FIGHTER_ID)

        mock_client.table.assert_called_with("signal_logs")

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_filters_by_fighter_id(self, mock_get_client):
        """Query applies eq filter for fighter_id."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_latest_signals(FIGHTER_ID)

        eq_call = mock_client.table.return_value.select.return_value.eq.call_args
        assert eq_call == call("fighter_id", FIGHTER_ID)

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_orders_by_created_at_descending(self, mock_get_client):
        """Query orders by created_at descending to get the most recent row."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_latest_signals(FIGHTER_ID)

        order_call = (
            mock_client.table.return_value
            .select.return_value
            .eq.return_value
            .order.call_args
        )
        assert order_call == call("created_at", desc=True)

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_limits_to_one_result(self, mock_get_client):
        """Query applies limit(1) to retrieve only the latest row."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        get_latest_signals(FIGHTER_ID)

        limit_call = (
            mock_client.table.return_value
            .select.return_value
            .eq.return_value
            .order.return_value
            .limit.call_args
        )
        assert limit_call == call(1)

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_most_recent_log(self, mock_get_client):
        """get_latest_signals returns the single dict from response.data."""
        mock_client = _make_client([SAMPLE_LOG_ROW])
        mock_get_client.return_value = mock_client

        result = get_latest_signals(FIGHTER_ID)

        assert result == SAMPLE_LOG_ROW

    @patch("src.storage.signal_storage.get_supabase_client")
    def test_returns_none_when_no_logs_exist(self, mock_get_client):
        """get_latest_signals returns None when the fighter has no signal logs."""
        mock_client = _make_client([])
        mock_get_client.return_value = mock_client

        result = get_latest_signals(FIGHTER_ID)

        assert result is None
