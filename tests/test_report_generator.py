"""
TDD tests for src/generators/report_generator.py

All Supabase and Anthropic API calls are mocked — no real network or DB access.
"""
from unittest.mock import MagicMock, patch
import json

import pytest

from src.generators.report_generator import generate_bout_report

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOUT_ID = "bout-uuid-001"
EVENT_ID = "event-uuid-001"
FIGHTER_A_ID = "fighter-uuid-a"
FIGHTER_B_ID = "fighter-uuid-b"

MOCK_BOUT = {
    "id": BOUT_ID,
    "event_id": EVENT_ID,
    "fighter_a_id": FIGHTER_A_ID,
    "fighter_b_id": FIGHTER_B_ID,
    "weight_class": "lightweight",
    "is_main_event": True,
    "is_title_fight": False,
}

MOCK_FIGHTER_A = {
    "id": FIGHTER_A_ID,
    "name": "Islam Makhachev",
    "nickname": "The Eagle's Pupil",
    "record_wins": 26,
    "record_losses": 1,
    "record_draws": 0,
    "weight_class": "lightweight",
}

MOCK_FIGHTER_B = {
    "id": FIGHTER_B_ID,
    "name": "Dustin Poirier",
    "nickname": "The Diamond",
    "record_wins": 29,
    "record_losses": 8,
    "record_draws": 0,
    "weight_class": "lightweight",
}

MOCK_SIGNAL_A = {
    "id": "signal-uuid-a",
    "fighter_id": FIGHTER_A_ID,
    "event_id": EVENT_ID,
    "source_type": "news",
    "raw_summary": "Makhachev looks dominant in camp.",
    "injury_flags": False,
    "confidence_score": 0.85,
    "red_flags": [],
    "green_flags": ["no injuries", "dominant camp"],
    "sentiment_score": 0.8,
    "notable_quotes": ["He looks unstoppable in training"],
}

MOCK_SIGNAL_B = {
    "id": "signal-uuid-b",
    "fighter_id": FIGHTER_B_ID,
    "event_id": EVENT_ID,
    "source_type": "news",
    "raw_summary": "Poirier confident heading in.",
    "injury_flags": False,
    "confidence_score": 0.7,
    "red_flags": ["tough matchup stylistically"],
    "green_flags": ["motivated challenger"],
    "sentiment_score": 0.5,
    "notable_quotes": [],
}

VALID_REPORT = {
    "prediction": "Islam Makhachev",
    "confidence_tier": "High",
    "win_probability": 0.78,
    "method_prediction": "Submission",
    "key_factors": ["elite grappling", "champion pressure"],
    "red_flags": ["Poirier's durability and chin"],
    "green_flags": ["Makhachev's dominant title camp"],
    "upset_alert": False,
    "statistical_edge": "Makhachev leads in grappling exchanges and takedown success.",
    "intangibles_edge": "Camp signals show zero concerns; Poirier carries a tough matchup style.",
    "narrative": "This is a tough fight for Poirier. Makhachev has looked unstoppable.",
}

MOCK_STORED_REPORT = {
    "id": "report-uuid-001",
    "bout_id": BOUT_ID,
    **VALID_REPORT,
}

PATCH_SUPABASE = "src.generators.report_generator.get_supabase_client"
PATCH_ANTHROPIC = "src.generators.report_generator.anthropic.Anthropic"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_MISSING = object()  # sentinel — allows callers to explicitly pass None for signals


def _make_mock_api_client(response_text: str) -> MagicMock:
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=response_text)]
    mock_client.messages.create.return_value = mock_msg
    return mock_client


def _make_supabase_mock(
    bout=None,
    fighter_a=None,
    fighter_b=None,
    signal_a=_MISSING,
    signal_b=_MISSING,
    stored_report=None,
):
    """Build a mock Supabase client that dispatches by table name.

    Table mocks are cached so that assertions on the same table (e.g.
    ``mock_client.table("reports").insert.call_args``) see the same object
    that was used during the function under test.
    """
    if bout is None:
        bout = MOCK_BOUT
    if fighter_a is None:
        fighter_a = MOCK_FIGHTER_A
    if fighter_b is None:
        fighter_b = MOCK_FIGHTER_B
    if signal_a is _MISSING:
        signal_a = MOCK_SIGNAL_A
    if signal_b is _MISSING:
        signal_b = MOCK_SIGNAL_B
    if stored_report is None:
        stored_report = MOCK_STORED_REPORT

    client = MagicMock()
    _tbl_cache: dict = {}

    def table_side_effect(table_name):
        if table_name in _tbl_cache:
            return _tbl_cache[table_name]

        tbl = MagicMock()

        if table_name == "bouts":
            tbl.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
                [bout] if bout else []
            )

        elif table_name == "fighters":
            def fighters_eq(col, val):
                m = MagicMock()
                if val == FIGHTER_A_ID:
                    m.limit.return_value.execute.return_value.data = [fighter_a]
                else:
                    m.limit.return_value.execute.return_value.data = [fighter_b]
                return m
            tbl.select.return_value.eq.side_effect = fighters_eq

        elif table_name == "signal_logs":
            def signals_eq(col, val):
                m = MagicMock()
                if val == FIGHTER_A_ID:
                    data = [signal_a] if signal_a is not None else []
                else:
                    data = [signal_b] if signal_b is not None else []
                m.eq.return_value.order.return_value.limit.return_value.execute.return_value.data = data
                return m
            tbl.select.return_value.eq.side_effect = signals_eq

        elif table_name == "reports":
            tbl.insert.return_value.execute.return_value.data = [stored_report]

        _tbl_cache[table_name] = tbl
        return tbl

    client.table.side_effect = table_side_effect
    return client


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestReturnShape:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_returns_dict(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        result = generate_bout_report(BOUT_ID)
        assert isinstance(result, dict)

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_result_contains_bout_id(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        result = generate_bout_report(BOUT_ID)
        assert result.get("bout_id") == BOUT_ID

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_result_contains_prediction(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        result = generate_bout_report(BOUT_ID)
        assert "prediction" in result

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_result_contains_all_schema_keys(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        result = generate_bout_report(BOUT_ID)
        for key in (
            "prediction",
            "confidence_tier",
            "win_probability",
            "method_prediction",
            "key_factors",
            "red_flags",
            "green_flags",
            "upset_alert",
            "statistical_edge",
            "intangibles_edge",
            "narrative",
        ):
            assert key in result, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# Database fetch behaviour
# ---------------------------------------------------------------------------


class TestBoutFetching:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_bouts_table(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        mock_client.table.assert_any_call("bouts")

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_fighters_table_for_both_fighters(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        fighters_calls = [c for c in mock_client.table.call_args_list if c.args[0] == "fighters"]
        assert len(fighters_calls) == 2

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_queries_signal_logs_for_both_fighters(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        signal_calls = [c for c in mock_client.table.call_args_list if c.args[0] == "signal_logs"]
        assert len(signal_calls) == 2


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------


class TestPromptConstruction:
    def _get_user_prompt(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)
        call_kwargs = mock_api.messages.create.call_args
        return call_kwargs.kwargs["messages"][0]["content"]

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_a_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "FIGHTER A" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_b_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "FIGHTER B" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_bout_context_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "BOUT CONTEXT" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_a_signals_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "FIGHTER A SIGNALS" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_b_signals_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "FIGHTER B SIGNALS" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_a_name(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_FIGHTER_A["name"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_b_name(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_FIGHTER_B["name"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_a_record(self, MockSupa, MockAnthropic):
        """Fighter A's wins appear in the prompt."""
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert str(MOCK_FIGHTER_A["record_wins"]) in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_weight_class(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_BOUT["weight_class"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_main_event_in_bout_context(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "main event" in prompt.lower() or "Main event" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_signal_a_raw_summary(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_SIGNAL_A["raw_summary"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_signal_b_raw_summary(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_SIGNAL_B["raw_summary"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_task_label(self, MockSupa, MockAnthropic):
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert "TASK" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_a_nickname_when_present(self, MockSupa, MockAnthropic):
        """Fighter A's nickname appears in the prompt when the fighter has one."""
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_FIGHTER_A["nickname"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_contains_fighter_b_nickname_when_present(self, MockSupa, MockAnthropic):
        """Fighter B's nickname appears in the prompt when the fighter has one."""
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        assert MOCK_FIGHTER_B["nickname"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_prompt_record_shows_full_win_loss_draw_format(self, MockSupa, MockAnthropic):
        """Fighter A's record is rendered as 'W-L-D' (e.g. '26-1-0') in the prompt."""
        prompt = self._get_user_prompt(MockSupa, MockAnthropic)
        expected = (
            f"{MOCK_FIGHTER_A['record_wins']}-"
            f"{MOCK_FIGHTER_A['record_losses']}-"
            f"{MOCK_FIGHTER_A['record_draws']}"
        )
        assert expected in prompt


# ---------------------------------------------------------------------------
# Signal handling — missing signals
# ---------------------------------------------------------------------------


class TestSignalHandling:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_no_signals_shows_unavailable_message(self, MockSupa, MockAnthropic):
        """When no signal_log exists for a fighter, prompt says 'No current signals available'."""
        mock_client = _make_supabase_mock(signal_a=None, signal_b=None)
        MockSupa.return_value = mock_client
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)

        prompt = mock_api.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "No current signals available" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_missing_fighter_a_signal_only(self, MockSupa, MockAnthropic):
        """Only fighter B has signals; fighter A gets the fallback message."""
        mock_client = _make_supabase_mock(signal_a=None)
        MockSupa.return_value = mock_client
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)

        prompt = mock_api.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "No current signals available" in prompt
        # Fighter B's signal must still appear
        assert MOCK_SIGNAL_B["raw_summary"] in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_signals_include_notable_quotes_field(self, MockSupa, MockAnthropic):
        """When signals exist, notable_quotes appears in the prompt."""
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)

        prompt = mock_api.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "notable_quotes" in prompt

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_signals_include_sentiment_score_field(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)

        prompt = mock_api.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "sentiment_score" in prompt


# ---------------------------------------------------------------------------
# API call behaviour
# ---------------------------------------------------------------------------


class TestApiCall:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_uses_claude_sonnet(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)
        call_kwargs = mock_api.messages.create.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-6"

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_system_prompt_mentions_mma_analyst(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)
        system = mock_api.messages.create.call_args.kwargs.get("system", "")
        assert "mma" in system.lower()

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_system_prompt_conveys_conviction(self, MockSupa, MockAnthropic):
        """System prompt should indicate analyst makes clear picks, not neutral."""
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)
        system = mock_api.messages.create.call_args.kwargs.get("system", "")
        assert "conviction" in system.lower() or "reasoned" in system.lower() or "neutral" in system.lower()

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_makes_exactly_one_api_call_on_success(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client(json.dumps(VALID_REPORT))
        MockAnthropic.return_value = mock_api
        generate_bout_report(BOUT_ID)
        assert mock_api.messages.create.call_count == 1


# ---------------------------------------------------------------------------
# Retry behaviour — identical pattern to news_processor
# ---------------------------------------------------------------------------


class TestRetryBehaviour:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_malformed_json_triggers_retry(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="not valid json")]
        good_msg = MagicMock()
        good_msg.content = [MagicMock(text=json.dumps(VALID_REPORT))]
        mock_api.messages.create.side_effect = [bad_msg, good_msg]
        MockAnthropic.return_value = mock_api

        result = generate_bout_report(BOUT_ID)
        assert mock_api.messages.create.call_count == 2
        assert "prediction" in result

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_retry_includes_correction_prompt(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="```json\n{broken")]
        good_msg = MagicMock()
        good_msg.content = [MagicMock(text=json.dumps(VALID_REPORT))]
        mock_api.messages.create.side_effect = [bad_msg, good_msg]
        MockAnthropic.return_value = mock_api

        generate_bout_report(BOUT_ID)
        first_msgs = mock_api.messages.create.call_args_list[0].kwargs["messages"]
        second_msgs = mock_api.messages.create.call_args_list[1].kwargs["messages"]
        assert second_msgs != first_msgs
        # Retry must have more messages (original + assistant echo + correction)
        assert len(second_msgs) > len(first_msgs)

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_double_failure_returns_error_dict(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="still not json")]
        mock_api.messages.create.return_value = bad_msg
        MockAnthropic.return_value = mock_api

        result = generate_bout_report(BOUT_ID)
        assert mock_api.messages.create.call_count == 2
        assert result.get("error") is True

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_double_failure_does_not_raise(self, MockSupa, MockAnthropic):
        MockSupa.return_value = _make_supabase_mock()
        mock_api = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="{}")]  # fails Pydantic validation
        mock_api.messages.create.return_value = bad_msg
        MockAnthropic.return_value = mock_api

        result = generate_bout_report(BOUT_ID)
        assert isinstance(result, dict)

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_fenced_json_accepted_on_first_try(self, MockSupa, MockAnthropic):
        """Markdown-fenced JSON is stripped and accepted without retry."""
        MockSupa.return_value = _make_supabase_mock()
        mock_api = _make_mock_api_client(
            f"```json\n{json.dumps(VALID_REPORT)}\n```"
        )
        MockAnthropic.return_value = mock_api

        result = generate_bout_report(BOUT_ID)
        assert mock_api.messages.create.call_count == 1
        assert result.get("prediction") == VALID_REPORT["prediction"]


# ---------------------------------------------------------------------------
# Storage — inserts to reports table
# ---------------------------------------------------------------------------


class TestStorage:
    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_inserts_to_reports_table(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        mock_client.table.assert_any_call("reports")

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_bout_id(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)

        # Find the reports insert call
        reports_tbl = None
        for c in mock_client.table.call_args_list:
            if c.args[0] == "reports":
                reports_tbl = mock_client.table.return_value
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert insert_arg["bout_id"] == BOUT_ID

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_prediction(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert insert_arg["prediction"] == VALID_REPORT["prediction"]

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_confidence_tier(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert insert_arg["confidence_tier"] == VALID_REPORT["confidence_tier"]

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_upset_alert(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert "upset_alert" in insert_arg

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_narrative(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert insert_arg["narrative"] == VALID_REPORT["narrative"]

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_insert_contains_win_probability(self, MockSupa, MockAnthropic):
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        MockAnthropic.return_value = _make_mock_api_client(json.dumps(VALID_REPORT))
        generate_bout_report(BOUT_ID)
        insert_arg = mock_client.table("reports").insert.call_args[0][0]
        assert insert_arg["win_probability"] == VALID_REPORT["win_probability"]

    @patch(PATCH_ANTHROPIC)
    @patch(PATCH_SUPABASE)
    def test_double_failure_does_not_insert(self, MockSupa, MockAnthropic):
        """On double JSON failure, nothing is inserted to reports table."""
        mock_client = _make_supabase_mock()
        MockSupa.return_value = mock_client
        mock_api = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text="bad json")]
        mock_api.messages.create.return_value = bad_msg
        MockAnthropic.return_value = mock_api

        generate_bout_report(BOUT_ID)
        mock_client.table("reports").insert.assert_not_called()
