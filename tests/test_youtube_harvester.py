"""
TDD tests for src/harvesters/youtube_harvester.py

All HTTP calls are mocked — no network access required.
Mocks match the YouTube Data API v3 search response shape.
"""
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.harvesters.youtube_harvester import Video, fetch_videos


# ---------------------------------------------------------------------------
# Module-level fixture: inject a fake API key for all tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _set_youtube_api_key(monkeypatch):
    """Ensure YOUTUBE_API_KEY is always set unless a test overrides it."""
    monkeypatch.setenv("YOUTUBE_API_KEY", "test-api-key-fixture")


# ---------------------------------------------------------------------------
# Realistic mock payloads (match YouTube Data API v3 search.list shape)
# ---------------------------------------------------------------------------


def _make_video_item(
    title,
    description="",
    video_id="dQw4w9WgXcQ",
    channel_title="UFC",
    published_at="2025-11-15T18:00:00Z",
):
    return {
        "kind": "youtube#searchResult",
        "id": {"kind": "youtube#video", "videoId": video_id},
        "snippet": {
            "title": title,
            "description": description,
            "channelTitle": channel_title,
            "publishedAt": published_at,
        },
    }


def _search_response(items: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {
        "kind": "youtube#searchListResponse",
        "items": items,
        "nextPageToken": None,
    }
    resp.raise_for_status = MagicMock()
    return resp


def _error_response(*args, **kwargs):
    raise httpx.TimeoutException("timed out")


def _http_error_response(*args, **kwargs):
    raise httpx.HTTPStatusError("403", request=MagicMock(), response=MagicMock())


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestVideoDataclass:
    def test_video_has_required_fields(self):
        """Video dataclass must expose all required fields."""
        video = Video(
            video_id="dQw4w9WgXcQ",
            title="Jon Jones UFC 309 Highlights",
            description="Jon Jones destroys Stipe Miocic.",
            channel_title="UFC",
            published_at="2025-11-15T18:00:00Z",
            url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        )
        assert video.video_id == "dQw4w9WgXcQ"
        assert video.title
        assert video.description
        assert video.channel_title
        assert video.published_at
        assert "youtube.com" in video.url


# ---------------------------------------------------------------------------
# fetch_videos return type & shape
# ---------------------------------------------------------------------------


class TestFetchVideos:
    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_returns_list_of_video_objects(self, mock_get):
        """A successful fetch returns Video instances."""
        mock_get.return_value = _search_response([
            _make_video_item("Jon Jones UFC 309 Highlights"),
        ])

        results = fetch_videos("Jon Jones")

        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(v, Video) for v in results)

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_video_fields_populated(self, mock_get):
        """Video objects carry all required fields from the API response."""
        mock_get.return_value = _search_response([
            _make_video_item(
                "Jon Jones Pre-Fight Breakdown",
                description="Full analysis of the Jones vs Stipe matchup",
                video_id="abc123xyz",
                channel_title="MMA Analysis",
                published_at="2025-11-14T12:00:00Z",
            )
        ])

        video = fetch_videos("Jon Jones")[0]

        assert video.video_id == "abc123xyz"
        assert "jones" in video.title.lower()
        assert video.description
        assert video.channel_title == "MMA Analysis"
        assert video.published_at == "2025-11-14T12:00:00Z"
        assert "abc123xyz" in video.url

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_url_is_valid_youtube_link(self, mock_get):
        """Video URL is a properly formatted youtube.com watch link."""
        mock_get.return_value = _search_response([
            _make_video_item("Jon Jones Highlights", video_id="vid999"),
        ])

        video = fetch_videos("Jon Jones")[0]

        assert video.url == "https://www.youtube.com/watch?v=vid999"

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_empty_results_returns_empty_list(self, mock_get):
        """API returning no items yields an empty list."""
        mock_get.return_value = _search_response([])

        results = fetch_videos("Jon Jones")

        assert results == []

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_timeout_returns_empty_list(self, mock_get):
        """A timeout returns an empty list without raising."""
        mock_get.side_effect = _error_response

        results = fetch_videos("Jon Jones")

        assert results == []

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_http_error_returns_empty_list(self, mock_get):
        """An HTTP error (e.g. 403 bad API key) returns empty list without raising."""
        mock_get.side_effect = _http_error_response

        results = fetch_videos("Jon Jones")

        assert results == []

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_generic_exception_returns_empty_list(self, mock_get):
        """Any unexpected exception returns an empty list without raising."""
        mock_get.side_effect = Exception("unexpected")

        results = fetch_videos("Jon Jones")

        assert results == []


# ---------------------------------------------------------------------------
# Filtering / matching
# ---------------------------------------------------------------------------


class TestFetchVideosFiltering:
    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_only_matching_videos_returned(self, mock_get):
        """Videos not mentioning the fighter are excluded."""
        mock_get.return_value = _search_response([
            _make_video_item("Jon Jones vs Stipe Full Breakdown"),
            _make_video_item("Islam Makhachev title defense highlights"),
            _make_video_item("UFC 309 Jones training camp", description="Jon Jones looks ready"),
        ])

        results = fetch_videos("Jon Jones")

        for v in results:
            text = (v.title + " " + v.description).lower()
            assert "jones" in text
        assert not any("makhachev" in v.title.lower() for v in results)

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_matching_is_case_insensitive(self, mock_get):
        """Fighter name matching ignores case."""
        mock_get.return_value = _search_response([
            _make_video_item("JON JONES BRUTAL KNOCKOUT", description="JONES WINS"),
        ])

        results = fetch_videos("jon jones")

        assert len(results) == 1

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_last_name_match_included(self, mock_get):
        """A video containing only the last name is included."""
        mock_get.return_value = _search_response([
            _make_video_item("Jones destroys everyone in heavyweight division"),
        ])

        results = fetch_videos("Jon Jones")

        assert len(results) >= 1

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_diacritics_normalized_for_matching(self, mock_get):
        """Fighter names with diacritics are matched against normalized text."""
        mock_get.return_value = _search_response([
            _make_video_item("Prochazka vs Pereira 3 Full Fight Preview"),
        ])

        results = fetch_videos("Jiří Procházka")

        assert len(results) >= 1

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_description_matched_as_well_as_title(self, mock_get):
        """Videos that mention the fighter only in description are included."""
        mock_get.return_value = _search_response([
            _make_video_item(
                "UFC 309 Full Card Preview",
                description="Jon Jones headlines this massive card against Stipe.",
            ),
        ])

        results = fetch_videos("Jon Jones")

        assert len(results) >= 1


# ---------------------------------------------------------------------------
# API call details
# ---------------------------------------------------------------------------


class TestFetchVideosApiCall:
    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_calls_youtube_data_api(self, mock_get):
        """Requests target the YouTube Data API v3 endpoint."""
        mock_get.return_value = _search_response([])

        fetch_videos("Jon Jones")

        assert mock_get.called
        url = mock_get.call_args.args[0]
        assert "googleapis.com" in url

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_search_query_includes_fighter_name(self, mock_get):
        """The API request includes the fighter name in the query params."""
        mock_get.return_value = _search_response([])

        fetch_videos("Jon Jones")

        call_kwargs = mock_get.call_args.kwargs
        params = call_kwargs.get("params", {})
        query = params.get("q", "")
        assert "jones" in query.lower() or "jon" in query.lower()

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_api_key_sent_as_param(self, mock_get):
        """The YOUTUBE_API_KEY env var is sent as a request parameter."""
        mock_get.return_value = _search_response([])

        with patch.dict("os.environ", {"YOUTUBE_API_KEY": "test-key-123"}):
            fetch_videos("Jon Jones")

        params = mock_get.call_args.kwargs.get("params", {})
        assert params.get("key") == "test-key-123"

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_missing_api_key_returns_empty_list(self, mock_get, monkeypatch):
        """If YOUTUBE_API_KEY is not set, return empty list without calling API."""
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

        results = fetch_videos("Jon Jones")

        assert results == []
        assert not mock_get.called


# ---------------------------------------------------------------------------
# Limit
# ---------------------------------------------------------------------------


class TestFetchVideosLimit:
    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_limit_caps_results(self, mock_get):
        """fetch_videos respects an optional limit parameter."""
        mock_get.return_value = _search_response([
            _make_video_item(f"Jon Jones video {i}", video_id=f"vid{i}") for i in range(20)
        ])

        results = fetch_videos("Jon Jones", limit=5)

        assert len(results) <= 5

    @patch("src.harvesters.youtube_harvester.httpx.get")
    def test_default_limit_is_reasonable(self, mock_get):
        """Without an explicit limit, at most 10 videos are returned."""
        mock_get.return_value = _search_response([
            _make_video_item(f"Jon Jones video {i}", video_id=f"vid{i}") for i in range(30)
        ])

        results = fetch_videos("Jon Jones")

        assert len(results) <= 10
