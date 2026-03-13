"""
TDD tests for src/generators/card_report.py

All Supabase and Anthropic API calls are mocked — no real network or DB access.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.generators.card_report import generate_card_report

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_ID = "event-uuid-001"
EVENT_NAME = "UFC 300"
BOUT_ID_1 = "bout-uuid-001"
BOUT_ID_2 = "bout-uuid-002"
CARD_SUMMARY = "UFC 300 is shaping up to be a historic card with two elite title fights."

MOCK_EVENT = {
    "id": EVENT_ID,
    "name": EVENT_NAME,
    "date": "2025-04-13",
    "location": "Las Vegas",
    "status": "upcoming",
}

MOCK_BOUTS = [
    {
        "id": BOUT_ID_1,
        "event_id": EVENT_ID,
        "fighter_a_id": "fa1",
        "fighter_b_id": "fb1",
        "weight_class": "lightweight",
        "is_main_event": True,
        "is_title_fight": True,
    },
    {
        "id": BOUT_ID_2,
        "event_id": EVENT_ID,
        "fighter_a_id": "fa2",
        "fighter_b_id": "fb2",
        "weight_class": "bantamweight",
        "is_main_event": False,
        "is_title_fight": False,
    },
]

MOCK_REPORT_1 = {
    "id": "report-uuid-001",
    "bout_id": BOUT_ID_1,
    "prediction": "Islam Makhachev",
    "confidence_tier": "High",
    "win_probability": 0.78,
    "method_prediction": "Submission",
    "key_factors": ["elite grappling", "champion pressure"],
    "red_flags": [],
    "green_flags": ["dominant camp"],
    "upset_alert": False,
    "statistical_edge": "Makhachev leads in grappling exchanges.",
    "intangibles_edge": "Zero camp concerns.",
    "narrative": "Makhachev should dominate this fight at range and on the mat.",
}

MOCK_REPORT_2 = {
    "id": "report-uuid-002",
    "bout_id": BOUT_ID_2,
    "prediction": "Sean O'Malley",
    "confidence_tier": "Low",
    "win_probability": 0.45,
    "method_prediction": "KO/TKO",
    "key_factors": ["striking power"],
    "red_flags": ["takedown vulnerability"],
    "green_flags": ["improved boxing"],
    "upset_alert": True,
    "statistical_edge": "O'Malley's striking is elite-level.",
    "intangibles_edge": "Motivated underdog with improved wrestling defence.",
    "narrative": "O'Malley can cause an upset if he keeps it standing.",
}

MOCK_ALL_REPORTS = [MOCK_REPORT_1, MOCK_REPORT_2]

PATCH_SUPABASE = "src.generators.card_report.get_supabase_client"
PATCH_ANTHROPIC = "src.generators.card_report.anthropic.Anthropic"
PATCH_GENERATE_BOUT = "src.generators.card_report.generate_bout_report"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_api_client(response_text: str = CARD_SUMMARY) -> MagicMock:
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=response_text)]
    mock_client.messages.create.return_value = mock_msg
    return mock_client


def _make_supabase_mock(
    event=None,
    bouts=None,
    pre_existing_reports=None,
    all_reports=None,
):
    """
    Build a mock Supabase client for card_report tests.

    pre_existing_reports: list of {bout_id: ...} rows returned by the
        initial "which bouts have reports?" check (select "bout_id").
    all_reports: list of full report rows returned after generation
        (select "*").
    """
    if event is None:
        event = MOCK_EVENT
    if bouts is None:
        bouts = MOCK_BOUTS
    if pre_existing_reports is None:
        pre_existing_reports = []
    if all_reports is None:
        all_reports = MOCK_ALL_REPORTS

    client = MagicMock()
    _tbl_cache: dict = {}

    def table_side_effect(table_name):
        if table_name in _tbl_cache:
            return _tbl_cache[table_name]

        tbl = MagicMock()

        if table_name == "events":
            tbl.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
                [event] if event else []
            )
            tbl.update.return_value.eq.return_value.execute.return_value.data = (
                [event] if event else []
            )

        elif table_name == "bouts":
            tbl.select.return_value.eq.return_value.execute.return_value.data = bouts

        elif table_name == "reports":
            def reports_select_side_effect(cols):
                m = MagicMock()
                if cols == "bout_id":
                    # First-pass check: which bouts already have reports
                    m.in_.return_value.execute.return_value.data = pre_existing_reports
                else:
                    # Fetch full reports after generation
                    m.in_.return_value.execute.return_value.data = all_reports
                return m
            tbl.select.side_effect = reports_select_side_effect

        _tbl_cache[table_name] = tbl
        return tbl

    client.table.side_effect = table_side_effect
    return client


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestReturnShape:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_returns_dict(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert isinstance(result, dict)

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_contains_required_keys(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        for key in ("event_id", "event_name", "bouts_processed", "card_summary", "upset_alerts"):
            assert key in result, f"Missing key: {key}"

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_event_id_echoed(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["event_id"] == EVENT_ID

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_event_name_in_result(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["event_name"] == EVENT_NAME

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_upset_alerts_is_list(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert isinstance(result["upset_alerts"], list)

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_card_summary_in_result(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(CARD_SUMMARY)
        result = generate_card_report(EVENT_ID)
        assert result["card_summary"] == CARD_SUMMARY


# ---------------------------------------------------------------------------
# Database fetch behaviour
# ---------------------------------------------------------------------------


class TestEventFetching:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_events_table(self, MockSupa, MockAnthropic, MockGenBout):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        mock_client.table.assert_any_call("events")

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_bouts_table(self, MockSupa, MockAnthropic, MockGenBout):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        mock_client.table.assert_any_call("bouts")

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_reports_table(self, MockSupa, MockAnthropic, MockGenBout):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        mock_client.table.assert_any_call("reports")

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_bouts_processed_equals_total_bouts(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["bouts_processed"] == len(MOCK_BOUTS)


# ---------------------------------------------------------------------------
# Report generation — skip existing, generate missing
# ---------------------------------------------------------------------------


class TestReportGeneration:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_calls_generate_bout_report_for_bouts_without_reports(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """No pre-existing reports → generate_bout_report called for every bout."""
        MockSupa.return_value = _make_supabase_mock(pre_existing_reports=[])
        MockAnthropic.return_value = _make_mock_api_client()
        MockGenBout.return_value = MOCK_REPORT_1
        generate_card_report(EVENT_ID)
        assert MockGenBout.call_count == len(MOCK_BOUTS)

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_skips_bouts_with_existing_reports(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """All bouts already have reports → generate_bout_report never called."""
        pre_existing = [{"bout_id": BOUT_ID_1}, {"bout_id": BOUT_ID_2}]
        MockSupa.return_value = _make_supabase_mock(pre_existing_reports=pre_existing)
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        MockGenBout.assert_not_called()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_partial_existing_reports_generates_only_missing(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """One of two bouts has a report → only the other is generated."""
        pre_existing = [{"bout_id": BOUT_ID_1}]
        MockSupa.return_value = _make_supabase_mock(pre_existing_reports=pre_existing)
        MockAnthropic.return_value = _make_mock_api_client()
        MockGenBout.return_value = MOCK_REPORT_2
        generate_card_report(EVENT_ID)
        assert MockGenBout.call_count == 1
        MockGenBout.assert_called_once_with(BOUT_ID_2)

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_generate_bout_report_called_with_bout_id(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """generate_bout_report is called with the bout's UUID."""
        MockSupa.return_value = _make_supabase_mock(pre_existing_reports=[])
        MockAnthropic.return_value = _make_mock_api_client()
        MockGenBout.return_value = MOCK_REPORT_1
        generate_card_report(EVENT_ID)
        called_ids = {c.args[0] for c in MockGenBout.call_args_list}
        assert BOUT_ID_1 in called_ids
        assert BOUT_ID_2 in called_ids


# ---------------------------------------------------------------------------
# Prompt construction — batch sent to Claude
# ---------------------------------------------------------------------------


class TestPromptConstruction:
    def _capture_prompt(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client()
        MockAnthropic.return_value = mock_api
        generate_card_report(EVENT_ID)
        call_kwargs = mock_api.messages.create.call_args
        return call_kwargs.kwargs["messages"][0]["content"]

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_mentions_senior_mma_analyst(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert "senior MMA analyst" in prompt

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_event_name(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert EVENT_NAME in prompt

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_requests_card_level_narrative(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert "card-level" in prompt.lower() or "card level" in prompt.lower()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_requests_plain_text_not_json(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert "plain text" in prompt.lower()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_requests_best_bet(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert "best bet" in prompt.lower()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_includes_bout_narratives(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """Each report's narrative appears in the prompt body."""
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert MOCK_REPORT_1["narrative"] in prompt
        assert MOCK_REPORT_2["narrative"] in prompt

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_includes_predictions(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """Each report's prediction appears in the prompt body."""
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert MOCK_REPORT_1["prediction"] in prompt
        assert MOCK_REPORT_2["prediction"] in prompt

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_requests_upset_alerts(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        prompt = self._capture_prompt(MockSupa, MockAnthropic, MockGenBout)
        assert "upset" in prompt.lower()


# ---------------------------------------------------------------------------
# API call behaviour
# ---------------------------------------------------------------------------


class TestApiCall:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_uses_claude_sonnet(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client()
        MockAnthropic.return_value = mock_api
        generate_card_report(EVENT_ID)
        call_kwargs = mock_api.messages.create.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-6"

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_makes_exactly_one_api_call(self, MockSupa, MockAnthropic, MockGenBout):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client()
        MockAnthropic.return_value = mock_api
        generate_card_report(EVENT_ID)
        assert mock_api.messages.create.call_count == 1

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_card_summary_is_stripped_response_text(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        padded = f"  {CARD_SUMMARY}  \n"
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(padded)
        result = generate_card_report(EVENT_ID)
        assert result["card_summary"] == CARD_SUMMARY


# ---------------------------------------------------------------------------
# Storage — updates events table
# ---------------------------------------------------------------------------


class TestStorage:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_updates_events_table(self, MockSupa, MockAnthropic, MockGenBout):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        events_calls = [c for c in mock_client.table.call_args_list if c.args[0] == "events"]
        assert len(events_calls) >= 1
        # update must have been called on the events table mock
        events_tbl = mock_client.table("events")
        events_tbl.update.assert_called_once()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_stored_card_summary_matches_api_response(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(CARD_SUMMARY)
        generate_card_report(EVENT_ID)
        update_arg = mock_client.table("events").update.call_args[0][0]
        assert update_arg["card_summary"] == CARD_SUMMARY

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_update_targets_correct_event_id(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        events_tbl = mock_client.table("events")
        eq_call = events_tbl.update.return_value.eq.call_args
        assert eq_call.args[1] == EVENT_ID


# ---------------------------------------------------------------------------
# Upset alerts — extracted from reports
# ---------------------------------------------------------------------------


class TestUpsetAlerts:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_upset_alerts_extracted_from_reports(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """One report has upset_alert=True → that prediction appears in upset_alerts."""
        MockSupa.return_value = _make_supabase_mock(all_reports=MOCK_ALL_REPORTS)
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert MOCK_REPORT_2["prediction"] in result["upset_alerts"]

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_non_upset_predictions_not_in_upset_alerts(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        MockSupa.return_value = _make_supabase_mock(all_reports=MOCK_ALL_REPORTS)
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert MOCK_REPORT_1["prediction"] not in result["upset_alerts"]

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_no_upset_alerts_when_none_flagged(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        no_upset_reports = [
            {**MOCK_REPORT_1, "upset_alert": False},
            {**MOCK_REPORT_2, "upset_alert": False},
        ]
        MockSupa.return_value = _make_supabase_mock(all_reports=no_upset_reports)
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["upset_alerts"] == []

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_multiple_upset_alerts_captured(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        all_upset_reports = [
            {**MOCK_REPORT_1, "upset_alert": True},
            {**MOCK_REPORT_2, "upset_alert": True},
        ]
        MockSupa.return_value = _make_supabase_mock(all_reports=all_upset_reports)
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert len(result["upset_alerts"]) == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_empty_event_returns_zero_bouts_processed(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        MockSupa.return_value = _make_supabase_mock(bouts=[], all_reports=[])
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["bouts_processed"] == 0

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_empty_event_returns_empty_upset_alerts(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        MockSupa.return_value = _make_supabase_mock(bouts=[], all_reports=[])
        MockAnthropic.return_value = _make_mock_api_client()
        result = generate_card_report(EVENT_ID)
        assert result["upset_alerts"] == []

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_empty_event_does_not_call_generate_bout_report(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        MockSupa.return_value = _make_supabase_mock(bouts=[], all_reports=[])
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        MockGenBout.assert_not_called()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_all_reports_pre_existing_skips_all_generation(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        pre_existing = [{"bout_id": BOUT_ID_1}, {"bout_id": BOUT_ID_2}]
        MockSupa.return_value = _make_supabase_mock(pre_existing_reports=pre_existing)
        MockAnthropic.return_value = _make_mock_api_client()
        generate_card_report(EVENT_ID)
        MockGenBout.assert_not_called()

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_card_summary_stripped_of_whitespace(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(f"\n\n  {CARD_SUMMARY}\n  ")
        result = generate_card_report(EVENT_ID)
        assert result["card_summary"] == CARD_SUMMARY

    @patch(PATCH_GENERATE_BOUT)
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_still_calls_claude_when_no_reports_available(
        self, MockSupa, MockAnthropic, MockGenBout
    ):
        """Even with no reports, Claude is still called (with empty context)."""
        MockSupa.return_value = _make_supabase_mock(bouts=[], all_reports=[])
        mock_api = _make_mock_api_client()
        MockAnthropic.return_value = mock_api
        generate_card_report(EVENT_ID)
        assert mock_api.messages.create.call_count == 1
