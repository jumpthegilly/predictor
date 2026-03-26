"""
TDD tests for src/api/main.py — FastAPI HTTP layer.

Tests all endpoints using FastAPI TestClient.
All pipeline/generator calls are mocked — no network or database access.
"""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app

# ---------------------------------------------------------------------------
# Test client fixture
# ---------------------------------------------------------------------------

client = TestClient(app)


# ---------------------------------------------------------------------------
# Mock payloads
# ---------------------------------------------------------------------------

MOCK_PIPELINE_RESULT = {
    "fighter_name": "Jon Jones",
    "fighter_id": "fighter-uuid-001",
    "event_id": "event-uuid-001",
    "articles_found": 3,
    "reddit_posts_found": 5,
    "youtube_videos_found": 2,
    "markets_found": 2,
    "news_signals_stored": True,
    "reddit_signals_stored": True,
    "youtube_signals_stored": True,
    "market_signals_stored": True,
    "errors": [],
}

MOCK_EVENT_PIPELINE_RESULT = {
    "event_id": "event-uuid-001",
    "fighters_processed": 4,
    "fighters_failed": 0,
    "total_signals_stored": 16,
    "errors": [],
}

MOCK_BOUT_REPORT = {
    "bout_id": "bout-uuid-001",
    "prediction": "Jon Jones",
    "confidence_tier": "High",
    "win_probability": 0.78,
    "method_prediction": "Decision",
    "key_factors": ["wrestling dominance", "reach advantage"],
    "red_flags": ["short camp"],
    "green_flags": ["healthy", "motivated"],
    "upset_alert": False,
    "statistical_edge": "Jones has the stats edge.",
    "intangibles_edge": "Jones has the camp edge.",
    "narrative": "Jones should win this fight.",
}

MOCK_CARD_REPORT = {
    "event_id": "event-uuid-001",
    "event_name": "UFC 309",
    "bouts_processed": 5,
    "card_summary": "UFC 309 is headlined by Jon Jones vs Stipe Miocic.",
    "upset_alerts": [],
}


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    def test_health_returns_200(self):
        """GET /health returns 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status_ok(self):
        """GET /health body contains status=ok."""
        response = client.get("/health")
        data = response.json()
        assert data.get("status") == "ok"


# ---------------------------------------------------------------------------
# POST /pipeline/fighter — run signal pipeline for one fighter
# ---------------------------------------------------------------------------


class TestFighterPipelineEndpoint:
    @patch("src.api.main.run_signal_pipeline")
    def test_returns_200_on_success(self, mock_pipeline):
        """POST /pipeline/fighter returns 200 when pipeline succeeds."""
        mock_pipeline.return_value = MOCK_PIPELINE_RESULT

        response = client.post("/pipeline/fighter", json={
            "fighter_name": "Jon Jones",
            "fighter_id": "fighter-uuid-001",
            "event_id": "event-uuid-001",
        })

        assert response.status_code == 200

    @patch("src.api.main.run_signal_pipeline")
    def test_returns_pipeline_summary(self, mock_pipeline):
        """POST /pipeline/fighter returns the pipeline summary dict."""
        mock_pipeline.return_value = MOCK_PIPELINE_RESULT

        response = client.post("/pipeline/fighter", json={
            "fighter_name": "Jon Jones",
            "fighter_id": "fighter-uuid-001",
            "event_id": "event-uuid-001",
        })

        data = response.json()
        assert data["fighter_name"] == "Jon Jones"
        assert data["articles_found"] == 3
        assert data["news_signals_stored"] is True

    @patch("src.api.main.run_signal_pipeline")
    def test_calls_pipeline_with_correct_args(self, mock_pipeline):
        """POST /pipeline/fighter forwards body fields to run_signal_pipeline."""
        mock_pipeline.return_value = MOCK_PIPELINE_RESULT

        client.post("/pipeline/fighter", json={
            "fighter_name": "Jon Jones",
            "fighter_id": "fighter-uuid-001",
            "event_id": "event-uuid-001",
        })

        mock_pipeline.assert_called_once_with(
            fighter_name="Jon Jones",
            fighter_id="fighter-uuid-001",
            event_id="event-uuid-001",
        )

    def test_missing_fighter_name_returns_422(self):
        """POST /pipeline/fighter with missing required field returns 422."""
        response = client.post("/pipeline/fighter", json={
            "fighter_id": "fighter-uuid-001",
            "event_id": "event-uuid-001",
        })

        assert response.status_code == 422

    def test_missing_fighter_id_returns_422(self):
        """POST /pipeline/fighter with missing fighter_id returns 422."""
        response = client.post("/pipeline/fighter", json={
            "fighter_name": "Jon Jones",
            "event_id": "event-uuid-001",
        })

        assert response.status_code == 422

    def test_missing_event_id_returns_422(self):
        """POST /pipeline/fighter with missing event_id returns 422."""
        response = client.post("/pipeline/fighter", json={
            "fighter_name": "Jon Jones",
            "fighter_id": "fighter-uuid-001",
        })

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /pipeline/event — run signal pipeline for all fighters in an event
# ---------------------------------------------------------------------------


class TestEventPipelineEndpoint:
    @patch("src.api.main.run_event_pipeline")
    def test_returns_200_on_success(self, mock_event):
        """POST /pipeline/event returns 200 when pipeline succeeds."""
        mock_event.return_value = MOCK_EVENT_PIPELINE_RESULT

        response = client.post("/pipeline/event", json={"event_id": "event-uuid-001"})

        assert response.status_code == 200

    @patch("src.api.main.run_event_pipeline")
    def test_returns_event_summary(self, mock_event):
        """POST /pipeline/event returns the event pipeline summary."""
        mock_event.return_value = MOCK_EVENT_PIPELINE_RESULT

        response = client.post("/pipeline/event", json={"event_id": "event-uuid-001"})

        data = response.json()
        assert data["event_id"] == "event-uuid-001"
        assert data["fighters_processed"] == 4

    @patch("src.api.main.run_event_pipeline")
    def test_calls_event_pipeline_with_event_id(self, mock_event):
        """POST /pipeline/event forwards event_id to run_event_pipeline."""
        mock_event.return_value = MOCK_EVENT_PIPELINE_RESULT

        client.post("/pipeline/event", json={"event_id": "event-uuid-001"})

        mock_event.assert_called_once_with(event_id="event-uuid-001")

    def test_missing_event_id_returns_422(self):
        """POST /pipeline/event with empty body returns 422."""
        response = client.post("/pipeline/event", json={})

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /reports/bout — generate a bout report
# ---------------------------------------------------------------------------


class TestBoutReportEndpoint:
    @patch("src.api.main.generate_bout_report")
    def test_returns_200_on_success(self, mock_report):
        """POST /reports/bout returns 200 when report generates successfully."""
        mock_report.return_value = MOCK_BOUT_REPORT

        response = client.post("/reports/bout", json={"bout_id": "bout-uuid-001"})

        assert response.status_code == 200

    @patch("src.api.main.generate_bout_report")
    def test_returns_report_data(self, mock_report):
        """POST /reports/bout returns the bout report dict."""
        mock_report.return_value = MOCK_BOUT_REPORT

        response = client.post("/reports/bout", json={"bout_id": "bout-uuid-001"})

        data = response.json()
        assert data["prediction"] == "Jon Jones"
        assert data["confidence_tier"] == "High"
        assert data["upset_alert"] is False

    @patch("src.api.main.generate_bout_report")
    def test_calls_generator_with_bout_id(self, mock_report):
        """POST /reports/bout forwards bout_id to generate_bout_report."""
        mock_report.return_value = MOCK_BOUT_REPORT

        client.post("/reports/bout", json={"bout_id": "bout-uuid-001"})

        mock_report.assert_called_once_with(bout_id="bout-uuid-001")

    @patch("src.api.main.generate_bout_report")
    def test_generator_error_returns_500(self, mock_report):
        """POST /reports/bout returns 500 when generator returns error dict."""
        mock_report.return_value = {"error": True, "bout_id": "bout-uuid-001"}

        response = client.post("/reports/bout", json={"bout_id": "bout-uuid-001"})

        assert response.status_code == 500

    def test_missing_bout_id_returns_422(self):
        """POST /reports/bout with empty body returns 422."""
        response = client.post("/reports/bout", json={})

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /reports/card — generate a full card report
# ---------------------------------------------------------------------------


class TestCardReportEndpoint:
    @patch("src.api.main.generate_card_report")
    def test_returns_200_on_success(self, mock_card):
        """POST /reports/card returns 200 when card report generates."""
        mock_card.return_value = MOCK_CARD_REPORT

        response = client.post("/reports/card", json={"event_id": "event-uuid-001"})

        assert response.status_code == 200

    @patch("src.api.main.generate_card_report")
    def test_returns_card_report_data(self, mock_card):
        """POST /reports/card returns the card report dict."""
        mock_card.return_value = MOCK_CARD_REPORT

        response = client.post("/reports/card", json={"event_id": "event-uuid-001"})

        data = response.json()
        assert data["event_name"] == "UFC 309"
        assert data["bouts_processed"] == 5
        assert "card_summary" in data

    @patch("src.api.main.generate_card_report")
    def test_calls_generator_with_event_id(self, mock_card):
        """POST /reports/card forwards event_id to generate_card_report."""
        mock_card.return_value = MOCK_CARD_REPORT

        client.post("/reports/card", json={"event_id": "event-uuid-001"})

        mock_card.assert_called_once_with(event_id="event-uuid-001")

    def test_missing_event_id_returns_422(self):
        """POST /reports/card with empty body returns 422."""
        response = client.post("/reports/card", json={})

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /reports/bout/{bout_id} — fetch a stored bout report
# ---------------------------------------------------------------------------


class TestGetBoutReportEndpoint:
    @patch("src.api.main.get_bout_report")
    def test_returns_200_when_report_exists(self, mock_get):
        """GET /reports/bout/{bout_id} returns 200 when report found."""
        mock_get.return_value = MOCK_BOUT_REPORT

        response = client.get("/reports/bout/bout-uuid-001")

        assert response.status_code == 200

    @patch("src.api.main.get_bout_report")
    def test_returns_report_data(self, mock_get):
        """GET /reports/bout/{bout_id} returns the stored report."""
        mock_get.return_value = MOCK_BOUT_REPORT

        response = client.get("/reports/bout/bout-uuid-001")

        data = response.json()
        assert data["prediction"] == "Jon Jones"

    @patch("src.api.main.get_bout_report")
    def test_returns_404_when_not_found(self, mock_get):
        """GET /reports/bout/{bout_id} returns 404 when no report exists."""
        mock_get.return_value = None

        response = client.get("/reports/bout/nonexistent-bout")

        assert response.status_code == 404

    @patch("src.api.main.get_bout_report")
    def test_calls_get_with_bout_id(self, mock_get):
        """GET /reports/bout/{bout_id} passes the path param to get_bout_report."""
        mock_get.return_value = MOCK_BOUT_REPORT

        client.get("/reports/bout/bout-uuid-001")

        mock_get.assert_called_once_with(bout_id="bout-uuid-001")


# ---------------------------------------------------------------------------
# GET /reports/event/{event_id} — fetch all stored reports for an event
# ---------------------------------------------------------------------------


class TestGetEventReportsEndpoint:
    @patch("src.api.main.get_event_reports")
    def test_returns_200_with_reports(self, mock_get):
        """GET /reports/event/{event_id} returns 200 with list of reports."""
        mock_get.return_value = [MOCK_BOUT_REPORT]

        response = client.get("/reports/event/event-uuid-001")

        assert response.status_code == 200

    @patch("src.api.main.get_event_reports")
    def test_returns_list_of_reports(self, mock_get):
        """GET /reports/event/{event_id} returns a list."""
        mock_get.return_value = [MOCK_BOUT_REPORT, MOCK_BOUT_REPORT]

        response = client.get("/reports/event/event-uuid-001")

        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2

    @patch("src.api.main.get_event_reports")
    def test_returns_empty_list_when_no_reports(self, mock_get):
        """GET /reports/event/{event_id} returns empty list when no reports."""
        mock_get.return_value = []

        response = client.get("/reports/event/event-uuid-001")

        assert response.status_code == 200
        assert response.json() == []

    @patch("src.api.main.get_event_reports")
    def test_calls_get_with_event_id(self, mock_get):
        """GET /reports/event/{event_id} passes path param to get_event_reports."""
        mock_get.return_value = []

        client.get("/reports/event/event-uuid-001")

        mock_get.assert_called_once_with(event_id="event-uuid-001")
