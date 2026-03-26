"""
TDD tests for Reddit + YouTube integration in src/pipeline/signal_pipeline.py

Tests the new reddit_posts_found, youtube_videos_found, reddit_signals_stored,
and youtube_signals_stored keys added to the pipeline summary, and verifies
that the new harvest/store steps are called correctly.

All sub-components are mocked — no network or database access.
"""
from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline.signal_pipeline import run_signal_pipeline

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIGHTER_NAME = "Jon Jones"
FIGHTER_ID = "fighter-uuid-001"
EVENT_ID = "event-uuid-001"

MOCK_ARTICLES = [MagicMock(title="Jon Jones looks sharp", source="MMA Junkie")]
MOCK_POSTS = [
    MagicMock(post_id="r1", title="Jones training camp update", body="Looking sharp", score=200),
]
MOCK_VIDEOS = [
    MagicMock(video_id="yt1", title="Jon Jones UFC 309 breakdown", description="In depth analysis"),
]
MOCK_MARKETS = [MagicMock(market_id="pm-001", question="Will Jon Jones win?", probability_a=0.72)]

MOCK_SIGNALS = {
    "raw_summary": "Jon Jones camp is healthy.",
    "injury_flags": False,
    "confidence_score": 0.84,
    "red_flags": [],
    "green_flags": ["healthy camp"],
    "sentiment_score": 0.78,
    "notable_quotes": [],
}

MOCK_STORED_ROW = {"id": "log-uuid-001", "fighter_id": FIGHTER_ID}

# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

PATCH_NEWS = "src.pipeline.signal_pipeline.fetch_articles"
PATCH_PROC = "src.pipeline.signal_pipeline.extract_signals"
PATCH_REDDIT = "src.pipeline.signal_pipeline.fetch_posts"
PATCH_YOUTUBE = "src.pipeline.signal_pipeline.fetch_videos"
PATCH_MARKET = "src.pipeline.signal_pipeline.fetch_markets"
PATCH_STORE = "src.pipeline.signal_pipeline.store_signal_log"


def _full_mock(
    articles=None,
    posts=None,
    videos=None,
    markets=None,
    signals=None,
    stored=None,
):
    """Patch all six targets and return a dict of mocks."""
    return {
        PATCH_NEWS: MagicMock(return_value=articles if articles is not None else MOCK_ARTICLES),
        PATCH_PROC: MagicMock(return_value=signals if signals is not None else MOCK_SIGNALS),
        PATCH_REDDIT: MagicMock(return_value=posts if posts is not None else MOCK_POSTS),
        PATCH_YOUTUBE: MagicMock(return_value=videos if videos is not None else MOCK_VIDEOS),
        PATCH_MARKET: MagicMock(return_value=markets if markets is not None else MOCK_MARKETS),
        PATCH_STORE: MagicMock(return_value=stored if stored is not None else MOCK_STORED_ROW),
    }


# ---------------------------------------------------------------------------
# Summary shape — new keys present
# ---------------------------------------------------------------------------


class TestSummaryNewKeys:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_contains_reddit_posts_found(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert "reddit_posts_found" in result

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_contains_youtube_videos_found(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert "youtube_videos_found" in result

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_contains_reddit_signals_stored(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert "reddit_signals_stored" in result

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_summary_contains_youtube_signals_stored(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert "youtube_signals_stored" in result

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_counts_are_correct_on_clean_run(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["reddit_posts_found"] == len(MOCK_POSTS)
        assert result["youtube_videos_found"] == len(MOCK_VIDEOS)
        assert result["reddit_signals_stored"] is True
        assert result["youtube_signals_stored"] is True


# ---------------------------------------------------------------------------
# Orchestration — Reddit and YouTube harvesters called correctly
# ---------------------------------------------------------------------------


class TestRedditYouTubeOrchestration:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_calls_reddit_harvester_with_fighter_name(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_reddit.assert_called_once_with(FIGHTER_NAME)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_calls_youtube_harvester_with_fighter_name(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        mock_yt.assert_called_once_with(FIGHTER_NAME)

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_reddit_posts_processed_with_extract_signals(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """Reddit posts are converted to text and passed to extract_signals."""
        mock_news.return_value = []  # no news so only reddit proc call
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = []
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        # extract_signals should be called once for reddit posts
        assert mock_proc.call_count >= 1
        call_args = mock_proc.call_args_list[0]
        assert call_args.args[1] == FIGHTER_NAME

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_youtube_videos_processed_with_extract_signals(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """YouTube videos are converted to text and passed to extract_signals."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = []
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert mock_proc.call_count >= 1
        call_args = mock_proc.call_args_list[0]
        assert call_args.args[1] == FIGHTER_NAME

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_stores_reddit_signals_with_correct_source_type(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """Reddit signals are stored with source_type='reddit'."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = []
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        source_types = [c.args[2] for c in mock_store.call_args_list]
        assert "reddit" in source_types

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_stores_youtube_signals_with_correct_source_type(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """YouTube signals are stored with source_type='youtube'."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = []
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        source_types = [c.args[2] for c in mock_store.call_args_list]
        assert "youtube" in source_types

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_all_four_signals_stored_on_clean_run(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """On a clean run, store is called for news, reddit, youtube, and market."""
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert mock_store.call_count == 4
        source_types = {c.args[2] for c in mock_store.call_args_list}
        assert source_types == {"news", "reddit", "youtube", "market"}


# ---------------------------------------------------------------------------
# Error isolation — Reddit/YouTube failures must not crash the pipeline
# ---------------------------------------------------------------------------


class TestRedditYouTubeErrorHandling:
    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_reddit_harvester_failure_does_not_crash(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.side_effect = Exception("Reddit API down")
        mock_yt.return_value = MOCK_VIDEOS
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["reddit_posts_found"] == 0
        assert result["reddit_signals_stored"] is False
        assert any("reddit" in e.lower() for e in result["errors"])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_youtube_harvester_failure_does_not_crash(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        mock_news.return_value = MOCK_ARTICLES
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = MOCK_POSTS
        mock_yt.side_effect = Exception("YouTube quota exceeded")
        mock_market.return_value = MOCK_MARKETS
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert isinstance(result, dict)
        assert result["youtube_videos_found"] == 0
        assert result["youtube_signals_stored"] is False
        assert any("youtube" in e.lower() for e in result["errors"])

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_empty_reddit_posts_skips_storage(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """No Reddit posts → no processor call for reddit, no storage."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = []
        mock_yt.return_value = []
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["reddit_posts_found"] == 0
        assert result["reddit_signals_stored"] is False
        assert result["errors"] == []

    @patch(PATCH_STORE)
    @patch(PATCH_MARKET)
    @patch(PATCH_YOUTUBE)
    @patch(PATCH_REDDIT)
    @patch(PATCH_PROC)
    @patch(PATCH_NEWS)
    def test_empty_youtube_videos_skips_storage(
        self, mock_news, mock_proc, mock_reddit, mock_yt, mock_market, mock_store
    ):
        """No YouTube videos → no processor call for youtube, no storage."""
        mock_news.return_value = []
        mock_proc.return_value = MOCK_SIGNALS
        mock_reddit.return_value = []
        mock_yt.return_value = []
        mock_market.return_value = []
        mock_store.return_value = MOCK_STORED_ROW

        result = run_signal_pipeline(FIGHTER_NAME, FIGHTER_ID, EVENT_ID)

        assert result["youtube_videos_found"] == 0
        assert result["youtube_signals_stored"] is False
        assert result["errors"] == []
