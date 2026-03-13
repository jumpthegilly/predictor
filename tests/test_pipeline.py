"""
TDD tests for src/pipeline/signal_pipeline.py

All sub-components are mocked — no network or database access.
"""
from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline.signal_pipeline import run_signal_pipeline, _market_signals

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIGHTER_NAME = "Jon Jones"
FIGHTER_ID = "fighter-uuid-001"
EVENT_ID = "event-uuid-001"

MOCK_ARTICLES = [
    MagicMock(title="Jon Jones looks sharp", source="MMA Junkie"),
    MagicMock(title="Jon Jones camp update", source="ESPN MMA"),
]

MOCK_PROCESSED_SIGNALS = {
    "raw_summary": "Jon Jones camp is healthy. No injuries.",
    "injury_flags": False,
    "confidence_score": 0.84,
    "red_flags": [],
    "green_flags": ["healthy camp", "sharp in training"],
    "sentiment_score": 0.78,
    "notable_quotes": [],
}

MOCK_MARKETS = [
    MagicMock(market_id="pm-001", question="Will Jon Jones win?", probability_a=0.72),
    MagicMock(market_id="kalshi-001", question="Jon Jones UFC 309", probability_a=0.70),
]

MOCK_STORED_ROW = {"id": "log-uuid-001", "fighter_id": FIGHTER_ID}

# ---------------------------------------------------------------------------
# Patch targets (imported names inside signal_pipeline module)
# ---------------------------------------------------------------------------

PATCH_NEWS = "src.pipeline.signal_pipeline.fetch_articles"
PATCH_PROC = "src.pipeline.signal_pipeline.extract_signals"
PATCH_MARKET = "src.pipeline.signal_pipeline.fetch_markets"
PATCH_STORE = "src.pipeline.signal_pipeline.store_signal_log"


def _all_mocks(articles=None, signals=None, markets=None, stored=None):
    """Return a dict of side_effects for all four patch targets."""
    return {
        PATCH_NEWS: MagicMock(return_value=articles if articles is not None else MOCK_ARTICLES),
        PATCH_PROC: MagicMock(return_value=signals if signals is not None else MOCK_PROCESSED_SIGNALS),
        PATCH_MARKET: MagicMock(return_value=markets if markets is not None else MOCK_MARKETS),
        PATCH_STORE: MagicMock(return_value=stored if stored is not None else MOCK_STORED_ROW),
    }


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestSummaryShape:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_returns_dict(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_contains_required_keys(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert "fighter_name" in result
        assert "fighter_id" in result
        assert "event_id" in result
        assert "articles_found" in result
        assert "markets_found" in result
        assert "news_signals_stored" in result
        assert "market_signals_stored" in result
        assert "errors" in result

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_counts_are_correct(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["fighter_name"] == FIGHTER_NAME
        assert result["fighter_id"] == FIGHTER_ID
        assert result["event_id"] == EVENT_ID
        assert result["articles_found"] == len(MOCK_ARTICLES)
        assert result["markets_found"] == len(MOCK_MARKETS)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_no_errors_on_clean_run(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["errors"] == []
        assert result["news_signals_stored"] is True
        assert result["market_signals_stored"] is True


# ---------------------------------------------------------------------------
# Orchestration — correct calls to sub-components
# ---------------------------------------------------------------------------


class TestOrchestration:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_calls_news_harvester_with_fighter_name(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_news.assert_called_once_with(FIGHTER_NAME)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_calls_news_processor_with_articles(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_proc.assert_called_once_with(MOCK_ARTICLES, FIGHTER_NAME)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_calls_market_harvester_with_fighter_name_as_keyword(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_market.assert_called_once_with(keywords=[FIGHTER_NAME])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_stores_news_signals(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        news_call = mock_store.call_args_list[0]
        assert news_call == call(FIGHTER_ID, EVENT_ID, "news", MOCK_PROCESSED_SIGNALS)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_stores_market_signals(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        market_call = mock_store.call_args_list[1]
        assert market_call.args[0] == FIGHTER_ID
        assert market_call.args[1] == EVENT_ID
        assert market_call.args[2] == "market"
        market_signals = market_call.args[3]
        assert "raw_summary" in market_signals

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_store_called_twice_on_clean_run(self, mock_news, mock_proc, mock_market, mock_store):
        """store_signal_log called once for news, once for market."""
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert mock_store.call_count == 2


# ---------------------------------------------------------------------------
# Error handling — one failure must not crash the pipeline
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_news_harvester_failure_does_not_crash(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.side_effect = Exception("network error")
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["articles_found"] == 0
        assert result["news_signals_stored"] is False
        assert any("news" in e.lower() for e in result["errors"])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_news_processor_failure_does_not_crash(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.side_effect = Exception("parsing failed")
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["news_signals_stored"] is False
        assert any("news" in e.lower() for e in result["errors"])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_market_harvester_failure_does_not_crash(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.side_effect = Exception("API timeout")
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["markets_found"] == 0
        assert result["market_signals_stored"] is False
        assert any("market" in e.lower() for e in result["errors"])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_storage_failure_does_not_crash(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.side_effect = Exception("DB write failed")

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["news_signals_stored"] is False
        assert result["market_signals_stored"] is False

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_multiple_failures_all_recorded_in_errors(self, mock_news, mock_proc, mock_market, mock_store):
        mock_news.side_effect = Exception("news down")
        mock_market.side_effect = Exception("market down")
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert len(result["errors"]) >= 2

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_empty_articles_skips_processor_and_news_storage(self, mock_news, mock_proc, mock_market, mock_store):
        """If no articles found, skip processor and news storage — no error recorded."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_proc.assert_not_called()
        assert result["articles_found"] == 0
        assert result["news_signals_stored"] is False

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_empty_markets_skips_market_storage(self, mock_news, mock_proc, mock_market, mock_store):
        """If no markets found, skip market storage — no error recorded."""
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_PROCESSED_SIGNALS
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["markets_found"] == 0
        assert result["market_signals_stored"] is False
        # Store called once for news only
        assert mock_store.call_count == 1


# ---------------------------------------------------------------------------
# _market_signals — structured output from prediction market data
# ---------------------------------------------------------------------------


def _make_markets(prob_a: float, count: int = 1):
    """Return a list of mock market objects with probability_a set."""
    return [
        MagicMock(
            market_id=f"pm-{i}",
            question=f"Will {FIGHTER_NAME} win?",
            probability_a=prob_a,
        )
        for i in range(count)
    ]


class TestMarketSignals:
    def test_raw_summary_is_clean_sentence(self):
        """raw_summary must be a single sentence, not multi-line market data."""
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert "\n" not in result["raw_summary"]

    def test_raw_summary_contains_fighter_name(self):
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert FIGHTER_NAME in result["raw_summary"]

    def test_raw_summary_contains_percentage(self):
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert "72%" in result["raw_summary"]

    def test_sentiment_not_none(self):
        """sentiment_score must be derived, not hardcoded None."""
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["sentiment_score"] is not None

    def test_sentiment_positive_when_favored(self):
        """avg_prob > 0.5 → sentiment_score > 0."""
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["sentiment_score"] > 0

    def test_sentiment_negative_when_underdog(self):
        """avg_prob < 0.5 → sentiment_score < 0."""
        markets = _make_markets(0.30)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["sentiment_score"] < 0

    def test_sentiment_zero_at_fifty_percent(self):
        """avg_prob == 0.5 → sentiment_score == 0.0."""
        markets = _make_markets(0.50)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["sentiment_score"] == 0.0

    def test_sentiment_formula(self):
        """sentiment_score == round((avg_prob - 0.5) * 2, 4)."""
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        expected = round((0.72 - 0.5) * 2, 4)
        assert result["sentiment_score"] == expected

    def test_green_flags_populated_when_favored(self):
        """Favored fighter (avg_prob >= 0.5) gets a green flag."""
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert len(result["green_flags"]) >= 1

    def test_red_flags_populated_when_underdog(self):
        """Underdog fighter (avg_prob < 0.5) gets a red flag."""
        markets = _make_markets(0.30)
        result = _market_signals(markets, FIGHTER_NAME)
        assert len(result["red_flags"]) >= 1

    def test_green_flags_empty_when_underdog(self):
        markets = _make_markets(0.30)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["green_flags"] == []

    def test_red_flags_empty_when_favored(self):
        markets = _make_markets(0.72)
        result = _market_signals(markets, FIGHTER_NAME)
        assert result["red_flags"] == []

    def test_averages_multiple_markets(self):
        """With multiple markets, avg_prob is the mean of all probability_a values."""
        markets = [
            MagicMock(probability_a=0.60, question="q1"),
            MagicMock(probability_a=0.80, question="q2"),
        ]
        result = _market_signals(markets, FIGHTER_NAME)
        expected_sentiment = round((0.70 - 0.5) * 2, 4)
        assert result["sentiment_score"] == expected_sentiment
