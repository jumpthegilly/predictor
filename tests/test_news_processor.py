"""
TDD tests for src/processors/news_processor.py

All Anthropic API calls are mocked — no real network access.

DB-aligned schema (signal_logs columns):
    raw_summary, injury_flags (bool), confidence_score (float),
    red_flags, green_flags, sentiment_score
"""
from unittest.mock import MagicMock, patch, call
import json

import pytest

from src.processors.news_processor import extract_signals

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIGHTER_NAME = "Islam Makhachev"

MOCK_ARTICLES = [
    {
        "title": "Islam Makhachev looks sharp in camp",
        "raw_text": "Islam Makhachev has been training hard for the upcoming fight.",
        "source": "MMA Junkie",
        "url": "https://mmajunkie.com/1",
        "published_date": "2024-01-01",
    },
    {
        "title": "Makhachev camp: no injuries",
        "raw_text": "The team confirms Islam is 100% healthy heading into the bout.",
        "source": "ESPN MMA",
        "url": "https://espn.com/1",
        "published_date": "2024-01-02",
    },
]

# DB-aligned: matches signal_logs columns exactly.
VALID_SIGNALS = {
    "raw_summary": "Islam Makhachev appears to be in excellent shape heading into the fight.",
    "injury_flags": False,
    "confidence_score": 0.9,
    "red_flags": [],
    "green_flags": ["100% healthy", "looking sharp"],
    "sentiment_score": 0.85,
    "notable_quotes": ["100% healthy heading into the bout"],
}

PATCH_ANTHROPIC = "src.processors.news_processor.anthropic.Anthropic"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(response_text: str):
    """Return a mock Anthropic client that yields response_text on .messages.create()."""
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=response_text)]
    mock_client.messages.create.return_value = mock_msg
    return mock_client


# ---------------------------------------------------------------------------
# DB schema alignment — these are the RED tests before the fix
# ---------------------------------------------------------------------------


class TestSchemaAlignment:
    """extract_signals output must align exactly with signal_logs DB columns."""

    @patch(PATCH_ANTHROPIC)
    def test_output_has_raw_summary_not_summary(self, MockAnthropic):
        """Processor must produce 'raw_summary', not 'summary'."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert "raw_summary" in result
        assert "summary" not in result

    @patch(PATCH_ANTHROPIC)
    def test_output_has_confidence_score_not_confidence_indicators(self, MockAnthropic):
        """Processor must produce 'confidence_score' (float), not 'confidence_indicators' (list)."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert "confidence_score" in result
        assert "confidence_indicators" not in result

    @patch(PATCH_ANTHROPIC)
    def test_output_has_notable_quotes(self, MockAnthropic):
        """'notable_quotes' is a DB column — must appear in output."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert "notable_quotes" in result

    @patch(PATCH_ANTHROPIC)
    def test_injury_flags_is_bool(self, MockAnthropic):
        """DB column injury_flags is boolean — processor must return bool."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert isinstance(result["injury_flags"], bool)

    @patch(PATCH_ANTHROPIC)
    def test_confidence_score_is_float(self, MockAnthropic):
        """DB column confidence_score is float — processor must return float."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert isinstance(result["confidence_score"], float)

    @patch(PATCH_ANTHROPIC)
    def test_output_keys_match_db_columns_exactly(self, MockAnthropic):
        """No unexpected keys — output must be a subset of DB columns (plus optional 'error')."""
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        db_columns = {
            "raw_summary", "injury_flags", "confidence_score",
            "red_flags", "green_flags", "sentiment_score", "notable_quotes",
        }
        extra_keys = set(result.keys()) - db_columns - {"error"}
        assert extra_keys == set(), f"Keys not in DB schema: {extra_keys}"

    @patch(PATCH_ANTHROPIC)
    def test_empty_schema_notable_quotes_is_empty_list(self, MockAnthropic):
        """Empty schema must default notable_quotes to []."""
        MockAnthropic.return_value = MagicMock()
        result = extract_signals([], FIGHTER_NAME)
        assert result["notable_quotes"] == []

    @patch(PATCH_ANTHROPIC)
    def test_empty_schema_has_raw_summary_not_summary(self, MockAnthropic):
        """Empty schema (no articles) must use 'raw_summary', not 'summary'."""
        MockAnthropic.return_value = MagicMock()
        result = extract_signals([], FIGHTER_NAME)
        assert "raw_summary" in result
        assert "summary" not in result

    @patch(PATCH_ANTHROPIC)
    def test_empty_schema_injury_flags_is_false(self, MockAnthropic):
        """Empty schema must default injury_flags to False (bool), not []."""
        MockAnthropic.return_value = MagicMock()
        result = extract_signals([], FIGHTER_NAME)
        assert result["injury_flags"] is False

    @patch(PATCH_ANTHROPIC)
    def test_empty_schema_confidence_score_is_zero(self, MockAnthropic):
        """Empty schema must default confidence_score to 0.0."""
        MockAnthropic.return_value = MagicMock()
        result = extract_signals([], FIGHTER_NAME)
        assert result["confidence_score"] == 0.0


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestReturnShape:
    @patch(PATCH_ANTHROPIC)
    def test_returns_dict(self, MockAnthropic):
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert isinstance(result, dict)

    @patch(PATCH_ANTHROPIC)
    def test_all_required_keys_present(self, MockAnthropic):
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        for key in (
            "raw_summary",
            "injury_flags",
            "confidence_score",
            "red_flags",
            "green_flags",
            "sentiment_score",
            "notable_quotes",
        ):
            assert key in result, f"Missing key: {key}"

    @patch(PATCH_ANTHROPIC)
    def test_successful_extraction_values(self, MockAnthropic):
        MockAnthropic.return_value = _make_mock_client(json.dumps(VALID_SIGNALS))
        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert result["injury_flags"] is False
        assert result["sentiment_score"] == 0.85
        assert result["confidence_score"] == 0.9


# ---------------------------------------------------------------------------
# API call behaviour
# ---------------------------------------------------------------------------


class TestApiCall:
    @patch(PATCH_ANTHROPIC)
    def test_makes_exactly_one_api_call_for_multiple_articles(self, MockAnthropic):
        mock_client = _make_mock_client(json.dumps(VALID_SIGNALS))
        MockAnthropic.return_value = mock_client
        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert mock_client.messages.create.call_count == 1

    @patch(PATCH_ANTHROPIC)
    def test_uses_correct_model(self, MockAnthropic):
        mock_client = _make_mock_client(json.dumps(VALID_SIGNALS))
        MockAnthropic.return_value = mock_client
        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        call_kwargs = mock_client.messages.create.call_args
        assert call_kwargs.kwargs["model"] == "claude-haiku-4-5-20251001"

    @patch(PATCH_ANTHROPIC)
    def test_system_prompt_mentions_mma(self, MockAnthropic):
        mock_client = _make_mock_client(json.dumps(VALID_SIGNALS))
        MockAnthropic.return_value = mock_client
        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        call_kwargs = mock_client.messages.create.call_args
        system = call_kwargs.kwargs.get("system", "")
        assert "mma" in system.lower() or "MMA" in system

    @patch(PATCH_ANTHROPIC)
    def test_fighter_name_in_user_prompt(self, MockAnthropic):
        mock_client = _make_mock_client(json.dumps(VALID_SIGNALS))
        MockAnthropic.return_value = mock_client
        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        call_kwargs = mock_client.messages.create.call_args
        messages = call_kwargs.kwargs["messages"]
        user_content = messages[0]["content"]
        assert FIGHTER_NAME in user_content

    @patch(PATCH_ANTHROPIC)
    def test_article_text_in_user_prompt(self, MockAnthropic):
        mock_client = _make_mock_client(json.dumps(VALID_SIGNALS))
        MockAnthropic.return_value = mock_client
        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        call_kwargs = mock_client.messages.create.call_args
        messages = call_kwargs.kwargs["messages"]
        user_content = messages[0]["content"]
        assert MOCK_ARTICLES[0]["raw_text"] in user_content


# ---------------------------------------------------------------------------
# Empty article list
# ---------------------------------------------------------------------------


class TestEmptyArticles:
    @patch(PATCH_ANTHROPIC)
    def test_empty_list_returns_dict(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client
        result = extract_signals([], FIGHTER_NAME)
        assert isinstance(result, dict)

    @patch(PATCH_ANTHROPIC)
    def test_empty_list_does_not_call_api(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client
        extract_signals([], FIGHTER_NAME)
        mock_client.messages.create.assert_not_called()

    @patch(PATCH_ANTHROPIC)
    def test_empty_list_returns_empty_schema(self, MockAnthropic):
        MockAnthropic.return_value = MagicMock()
        result = extract_signals([], FIGHTER_NAME)
        assert result["injury_flags"] is False
        assert result["red_flags"] == []
        assert result["green_flags"] == []
        assert result["confidence_score"] == 0.0
        assert result["raw_summary"] == ""
        assert result["sentiment_score"] == 0.0
        assert result["notable_quotes"] == []


# ---------------------------------------------------------------------------
# Retry on malformed JSON
# ---------------------------------------------------------------------------


class TestRetryBehaviour:
    @patch(PATCH_ANTHROPIC)
    def test_malformed_json_triggers_retry(self, MockAnthropic):
        """First call returns garbage JSON; second call returns valid JSON."""
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="not valid json at all")]

        good_msg = MagicMock()
        good_msg.content = [MagicMock(text=json.dumps(VALID_SIGNALS))]

        mock_client.messages.create.side_effect = [bad_msg, good_msg]

        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert mock_client.messages.create.call_count == 2
        assert result["sentiment_score"] == 0.85

    @patch(PATCH_ANTHROPIC)
    def test_retry_includes_correction_prompt(self, MockAnthropic):
        """Second API call should include a correction message."""
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="```json\n{broken")]

        good_msg = MagicMock()
        good_msg.content = [MagicMock(text=json.dumps(VALID_SIGNALS))]

        mock_client.messages.create.side_effect = [bad_msg, good_msg]

        extract_signals(MOCK_ARTICLES, FIGHTER_NAME)

        first_call_messages = mock_client.messages.create.call_args_list[0].kwargs["messages"]
        second_call_messages = mock_client.messages.create.call_args_list[1].kwargs["messages"]
        assert second_call_messages != first_call_messages

    @patch(PATCH_ANTHROPIC)
    def test_double_failure_returns_empty_schema(self, MockAnthropic):
        """Both attempts return invalid JSON → return zeroed schema with error flag."""
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="still not json")]

        mock_client.messages.create.return_value = bad_msg

        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert mock_client.messages.create.call_count == 2
        assert result.get("error") is True
        assert result["injury_flags"] is False
        assert result["red_flags"] == []
        assert result["sentiment_score"] == 0.0

    @patch(PATCH_ANTHROPIC)
    def test_double_failure_does_not_raise(self, MockAnthropic):
        """Double JSON failure must not raise an exception."""
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="{}")]  # empty, will fail Pydantic validation

        mock_client.messages.create.return_value = bad_msg

        result = extract_signals(MOCK_ARTICLES, FIGHTER_NAME)
        assert isinstance(result, dict)
