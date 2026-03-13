"""
TDD tests for src/harvesters/market_harvester.py

All HTTP calls are mocked — no network access required.
Mocks match the real Polymarket gamma API and Kalshi v2 API shapes.
"""
import json
from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from src.harvesters.market_harvester import Market, fetch_markets

# ---------------------------------------------------------------------------
# Realistic mock payloads (match live API shapes)
# ---------------------------------------------------------------------------

POLYMARKET_UFC_MARKET = {
    "id": "pm-ufc-309-jones",
    "question": "Will Jon Jones defeat Stipe Miocic at UFC 309?",
    "outcomes": json.dumps(["Jon Jones", "Stipe Miocic"]),
    "outcomePrices": json.dumps(["0.72", "0.28"]),
    "volume": "125000.50",
    "updatedAt": "2025-11-16T22:00:00Z",
    "active": True,
    "closed": False,
}

POLYMARKET_NON_MMA_MARKET = {
    "id": "pm-election-2024",
    "question": "Will the Democrats win the 2026 midterms?",
    "outcomes": json.dumps(["Yes", "No"]),
    "outcomePrices": json.dumps(["0.45", "0.55"]),
    "volume": "500000.00",
    "updatedAt": "2025-11-01T00:00:00Z",
    "active": True,
    "closed": False,
}

POLYMARKET_MMA_MARKET = {
    "id": "pm-mma-fight-night",
    "question": "Will the MMA Fight Night main event go to decision?",
    "outcomes": json.dumps(["Yes", "No"]),
    "outcomePrices": json.dumps(["0.38", "0.62"]),
    "volume": "8000.00",
    "updatedAt": "2025-11-10T00:00:00Z",
    "active": True,
    "closed": False,
}

KALSHI_UFC_MARKET = {
    "ticker": "UFC309-JONES-WIN",
    "subtitle": "Will Jon Jones win at UFC 309?",
    "status": "active",
    "last_price_dollars": "0.7100",
    "volume": 42000,
    "close_time": "2025-11-17T00:00:00Z",
    "market_type": "binary",
}

KALSHI_NON_MMA_MARKET = {
    "ticker": "NASDAQ-CLOSE-HIGH",
    "subtitle": "Will NASDAQ close above 20000 today?",
    "status": "active",
    "last_price_dollars": "0.5500",
    "volume": 15000,
    "close_time": "2025-11-16T21:00:00Z",
    "market_type": "binary",
}


def _poly_response(markets: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = markets
    resp.raise_for_status = MagicMock()
    return resp


def _kalshi_response(markets: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"markets": markets, "cursor": ""}
    resp.raise_for_status = MagicMock()
    return resp


def _timeout_response(*args, **kwargs):
    raise httpx.TimeoutException("timed out")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFetchMarkets:
    @patch("src.harvesters.market_harvester.httpx.get")
    def test_returns_list_of_market_objects(self, mock_get):
        """A successful fetch returns Market instances."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET]),
            _kalshi_response([KALSHI_UFC_MARKET]),
        ]

        results = fetch_markets()

        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(m, Market) for m in results)

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_market_has_all_required_fields(self, mock_get):
        """Each Market carries all eight required fields."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET]),
            _kalshi_response([]),
        ]

        result = fetch_markets()[0]

        assert result.market_id
        assert result.question
        assert result.outcome_a
        assert result.outcome_b
        assert isinstance(result.probability_a, float)
        assert isinstance(result.probability_b, float)
        assert isinstance(result.volume, float)
        assert result.last_updated

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_default_keywords_filter_to_ufc_and_mma(self, mock_get):
        """Without explicit keywords, only UFC/MMA markets are returned."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET, POLYMARKET_NON_MMA_MARKET, POLYMARKET_MMA_MARKET]),
            _kalshi_response([KALSHI_UFC_MARKET, KALSHI_NON_MMA_MARKET]),
        ]

        results = fetch_markets()

        questions = [r.question for r in results]
        assert all(
            any(kw in q for kw in ("UFC", "MMA", "Jon Jones"))
            for q in questions
        )
        assert not any("midterms" in q for q in questions)
        assert not any("NASDAQ" in q for q in questions)

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_custom_keywords_filter_correctly(self, mock_get):
        """Passing explicit keywords overrides the defaults."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET, POLYMARKET_MMA_MARKET]),
            _kalshi_response([KALSHI_UFC_MARKET]),
        ]

        results = fetch_markets(keywords=["Jon Jones"])

        assert all("Jon Jones" in r.question for r in results)

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_filtering_is_case_insensitive(self, mock_get):
        """Keyword matching ignores case."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_MMA_MARKET]),
            _kalshi_response([]),
        ]

        upper = fetch_markets(keywords=["MMA"])
        mock_get.side_effect = [
            _poly_response([POLYMARKET_MMA_MARKET]),
            _kalshi_response([]),
        ]
        lower = fetch_markets(keywords=["mma"])

        assert len(upper) == len(lower) == 1

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_empty_results_returns_empty_list(self, mock_get):
        """Both APIs returning no matching markets yields an empty list."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_NON_MMA_MARKET]),
            _kalshi_response([KALSHI_NON_MMA_MARKET]),
        ]

        results = fetch_markets()

        assert results == []

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_polymarket_timeout_falls_back_to_kalshi(self, mock_get):
        """A timeout on Polymarket is skipped and Kalshi results still come through."""
        mock_get.side_effect = [
            _timeout_response,  # Polymarket times out
            _kalshi_response([KALSHI_UFC_MARKET]),
        ]

        results = fetch_markets()

        assert isinstance(results, list)
        assert any("UFC" in r.question or "Jones" in r.question for r in results)

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_kalshi_timeout_falls_back_to_polymarket(self, mock_get):
        """A timeout on Kalshi is skipped and Polymarket results still come through."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET]),
            _timeout_response,  # Kalshi times out
        ]

        results = fetch_markets()

        assert isinstance(results, list)
        assert any("UFC" in r.question for r in results)

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_both_sources_timing_out_returns_empty_list(self, mock_get):
        """When both APIs time out, an empty list is returned without raising."""
        mock_get.side_effect = _timeout_response

        results = fetch_markets()

        assert results == []

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_polymarket_probabilities_parsed_correctly(self, mock_get):
        """outcomePrices JSON strings are decoded into floats."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET]),
            _kalshi_response([]),
        ]

        result = fetch_markets()[0]

        assert abs(result.probability_a - 0.72) < 1e-6
        assert abs(result.probability_b - 0.28) < 1e-6

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_kalshi_probabilities_parsed_correctly(self, mock_get):
        """Kalshi last_price_dollars is used as probability_a; probability_b = 1 - prob_a."""
        mock_get.side_effect = [
            _poly_response([]),
            _kalshi_response([KALSHI_UFC_MARKET]),
        ]

        result = fetch_markets()[0]

        assert abs(result.probability_a - 0.71) < 1e-6
        assert abs(result.probability_b - 0.29) < 1e-6

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_queries_both_sources(self, mock_get):
        """httpx.get is called once for each source."""
        mock_get.side_effect = [
            _poly_response([]),
            _kalshi_response([]),
        ]

        fetch_markets()

        assert mock_get.call_count == 2

    @patch("src.harvesters.market_harvester.httpx.get")
    def test_combined_results_from_both_sources(self, mock_get):
        """Markets from Polymarket and Kalshi are merged into one list."""
        mock_get.side_effect = [
            _poly_response([POLYMARKET_UFC_MARKET]),
            _kalshi_response([KALSHI_UFC_MARKET]),
        ]

        results = fetch_markets()

        assert len(results) == 2
