"""
TDD tests for src/harvesters/reddit_harvester.py

All HTTP calls are mocked — no network access required.
Mocks match the Reddit JSON API shape (oauth.reddit.com).
"""
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.harvesters.reddit_harvester import Post, fetch_posts


# ---------------------------------------------------------------------------
# Realistic mock payloads (match Reddit listing JSON shape)
# ---------------------------------------------------------------------------

_post_counter = 0


def _make_post(title, selftext="", score=100, num_comments=20, url="https://reddit.com/r/ufc/123", post_id=None):
    global _post_counter
    _post_counter += 1
    pid = post_id if post_id is not None else f"post{_post_counter}"
    return {
        "kind": "t3",
        "data": {
            "id": pid,
            "title": title,
            "selftext": selftext,
            "score": score,
            "num_comments": num_comments,
            "url": url,
            "permalink": f"/r/ufc/comments/{pid}/some_post/",
            "subreddit": "ufc",
            "created_utc": 1700000000.0,
        },
    }


def _listing_response(posts: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {
        "data": {
            "children": posts,
            "after": None,
        }
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


class TestPostDataclass:
    def test_post_has_required_fields(self):
        """Post dataclass must expose all required fields."""
        post = Post(
            post_id="abc123",
            title="Jon Jones vs Stipe: Who wins?",
            body="Full breakdown of the matchup.",
            score=250,
            num_comments=45,
            url="https://reddit.com/r/ufc/comments/abc123/",
            subreddit="ufc",
            created_utc=1700000000.0,
        )
        assert post.post_id == "abc123"
        assert post.title
        assert post.body == "Full breakdown of the matchup."
        assert isinstance(post.score, int)
        assert isinstance(post.num_comments, int)
        assert post.url
        assert post.subreddit
        assert isinstance(post.created_utc, float)


# ---------------------------------------------------------------------------
# fetch_posts return type & shape
# ---------------------------------------------------------------------------


class TestFetchPosts:
    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_returns_list_of_post_objects(self, mock_get):
        """A successful fetch returns Post instances."""
        mock_get.return_value = _listing_response([
            _make_post("Jon Jones vs Stipe: Full breakdown", selftext="Jones is the GOAT"),
        ])

        results = fetch_posts("Jon Jones")

        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(p, Post) for p in results)

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_post_fields_are_populated(self, mock_get):
        """Post objects carry all required fields from the API response."""
        mock_get.return_value = _listing_response([
            _make_post(
                "Jon Jones vs Stipe",
                selftext="Jones has the edge in grappling",
                score=300,
                num_comments=50,
            )
        ])

        post = fetch_posts("Jon Jones")[0]

        assert post.post_id
        assert "Jones" in post.title
        assert post.score == 300
        assert post.num_comments == 50
        assert post.url
        assert post.subreddit
        assert isinstance(post.created_utc, float)

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_empty_results_returns_empty_list(self, mock_get):
        """No matching posts returns an empty list."""
        mock_get.return_value = _listing_response([])

        results = fetch_posts("Jon Jones")

        assert results == []

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_timeout_returns_empty_list(self, mock_get):
        """A timeout returns an empty list without raising."""
        mock_get.side_effect = _error_response

        results = fetch_posts("Jon Jones")

        assert results == []

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_http_error_returns_empty_list(self, mock_get):
        """An HTTP error returns an empty list without raising."""
        mock_get.side_effect = _http_error_response

        results = fetch_posts("Jon Jones")

        assert results == []

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_generic_exception_returns_empty_list(self, mock_get):
        """Any unexpected exception returns an empty list without raising."""
        mock_get.side_effect = Exception("unexpected")

        results = fetch_posts("Jon Jones")

        assert results == []


# ---------------------------------------------------------------------------
# Filtering / matching
# ---------------------------------------------------------------------------


class TestFetchPostsFiltering:
    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_only_matching_posts_returned(self, mock_get):
        """Posts not mentioning the fighter are excluded."""
        mock_get.return_value = _listing_response([
            _make_post("Jon Jones vs Stipe: Full breakdown"),
            _make_post("Islam Makhachev defends title"),
            _make_post("Jones training camp report", selftext="Jon Jones looks sharp"),
        ])

        results = fetch_posts("Jon Jones")

        assert all(
            "jones" in p.title.lower() or "jones" in p.body.lower()
            for p in results
        )
        assert not any("makhachev" in p.title.lower() for p in results)

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_matching_is_case_insensitive(self, mock_get):
        """Fighter name matching ignores case."""
        mock_get.return_value = _listing_response([
            _make_post("JON JONES is the GOAT", selftext="JONES will dominate"),
        ])

        results = fetch_posts("jon jones")

        assert len(results) == 1

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_last_name_match_included(self, mock_get):
        """A post containing only the last name is included."""
        mock_get.return_value = _listing_response([
            _make_post("Jones will dominate the heavyweight division"),
        ])

        results = fetch_posts("Jon Jones")

        assert len(results) >= 1

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_diacritics_normalized_for_matching(self, mock_get):
        """Fighter names with diacritics are matched against normalized text."""
        mock_get.return_value = _listing_response([
            _make_post("Prochazka lands vicious elbow on Pereira"),
        ])

        results = fetch_posts("Jiří Procházka")

        assert len(results) >= 1

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_body_text_matched_as_well_as_title(self, mock_get):
        """Posts that mention the fighter only in the body are still included."""
        mock_get.return_value = _listing_response([
            _make_post(
                "UFC 300 predictions thread",
                selftext="I think Jon Jones will knock out his opponent in round 2.",
            ),
        ])

        results = fetch_posts("Jon Jones")

        assert len(results) >= 1


# ---------------------------------------------------------------------------
# Subreddit targeting
# ---------------------------------------------------------------------------


class TestFetchPostsSubreddits:
    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_queries_ufc_and_mma_subreddits(self, mock_get):
        """fetch_posts queries both r/ufc and r/MMA."""
        mock_get.return_value = _listing_response([])

        fetch_posts("Jon Jones")

        urls_called = [call.args[0] for call in mock_get.call_args_list]
        assert any("ufc" in url.lower() for url in urls_called)
        assert any("/mma" in url.lower() or "r/mma" in url.lower() for url in urls_called)

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_results_from_both_subreddits_merged(self, mock_get):
        """Posts from r/ufc and r/MMA are merged into one list."""
        mock_get.side_effect = [
            _listing_response([_make_post("Jon Jones thread", selftext="")]),
            _listing_response([_make_post("Jones camp update", selftext="Jon Jones looks sharp")]),
        ]

        results = fetch_posts("Jon Jones")

        assert len(results) == 2

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_one_subreddit_timeout_still_returns_other(self, mock_get):
        """A timeout on one subreddit still returns results from the other."""
        mock_get.side_effect = [
            _error_response,
            _listing_response([_make_post("Jon Jones thread", selftext="Jones is ready")]),
        ]

        results = fetch_posts("Jon Jones")

        assert len(results) >= 1


# ---------------------------------------------------------------------------
# Sorting / ranking
# ---------------------------------------------------------------------------


class TestFetchPostsSorting:
    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_results_sorted_by_score_descending(self, mock_get):
        """Posts are returned sorted by score (highest first)."""
        mock_get.return_value = _listing_response([
            _make_post("Jones post A", selftext="Jon Jones", score=50),
            _make_post("Jones post B", selftext="Jon Jones", score=500),
            _make_post("Jones post C", selftext="Jon Jones", score=200),
        ])

        results = fetch_posts("Jon Jones")

        scores = [p.score for p in results]
        assert scores == sorted(scores, reverse=True)

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_limit_caps_results(self, mock_get):
        """fetch_posts respects an optional limit parameter."""
        mock_get.return_value = _listing_response([
            _make_post(f"Jon Jones post {i}", selftext="Jon Jones") for i in range(20)
        ])

        results = fetch_posts("Jon Jones", limit=5)

        assert len(results) <= 5

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_default_limit_is_reasonable(self, mock_get):
        """Without an explicit limit, at most 25 posts are returned."""
        mock_get.return_value = _listing_response([
            _make_post(f"Jon Jones post {i}", selftext="Jon Jones") for i in range(50)
        ])

        results = fetch_posts("Jon Jones")

        assert len(results) <= 25


# ---------------------------------------------------------------------------
# API call details
# ---------------------------------------------------------------------------


class TestFetchPostsApiCalls:
    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_uses_reddit_json_api(self, mock_get):
        """Requests target reddit.com search or listing endpoints."""
        mock_get.return_value = _listing_response([])

        fetch_posts("Jon Jones")

        for call in mock_get.call_args_list:
            url = call.args[0]
            assert "reddit.com" in url

    @patch("src.harvesters.reddit_harvester.httpx.get")
    def test_sends_user_agent_header(self, mock_get):
        """Requests include a User-Agent header (Reddit requires this)."""
        mock_get.return_value = _listing_response([])

        fetch_posts("Jon Jones")

        for call in mock_get.call_args_list:
            headers = call.kwargs.get("headers", {})
            assert "User-Agent" in headers or "user-agent" in {k.lower() for k in headers}
