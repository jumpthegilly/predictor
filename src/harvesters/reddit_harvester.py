"""
Reddit harvester — fetches MMA pre-fight discussion posts from r/ufc and r/MMA.

Matching strategy
-----------------
Searches Reddit's JSON listing API for posts in r/ufc and r/MMA that mention
the fighter by full name or last name. Diacritics are stripped for matching.
Results are merged, de-duplicated, and sorted by score descending.

Usage
-----
    from src.harvesters.reddit_harvester import fetch_posts

    posts = fetch_posts("Jiří Procházka")
    for p in posts:
        print(p.title, p.score)
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass

import httpx


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUBREDDITS = ["ufc", "MMA"]
DEFAULT_LIMIT = 25
_USER_AGENT = "MMA-Predictor/1.0 (pre-fight signal harvester)"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Post:
    post_id: str
    title: str
    body: str
    score: int
    num_comments: int
    url: str
    subreddit: str
    created_utc: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Lowercase and strip diacritics: 'Procházka' → 'prochazka'."""
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def _name_tokens(fighter_name: str) -> set[str]:
    """Return normalized full name and last name as matching tokens."""
    norm = _normalize(fighter_name)
    tokens: set[str] = {norm}
    parts = norm.split()
    if parts:
        tokens.add(parts[-1])
    return tokens


def _post_matches(post_data: dict, tokens: set[str]) -> bool:
    title = _normalize(post_data.get("title", ""))
    body = _normalize(post_data.get("selftext", ""))
    combined = title + " " + body
    return any(t in combined for t in tokens)


def _parse_post(data: dict) -> Post:
    return Post(
        post_id=data.get("id", ""),
        title=data.get("title", ""),
        body=data.get("selftext", ""),
        score=int(data.get("score", 0)),
        num_comments=int(data.get("num_comments", 0)),
        url=data.get("url", ""),
        subreddit=data.get("subreddit", ""),
        created_utc=float(data.get("created_utc", 0.0)),
    )


def _fetch_subreddit(subreddit: str, fighter_name: str) -> list[Post]:
    """Fetch and filter posts from one subreddit. Returns [] on any error."""
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    tokens = _name_tokens(fighter_name)
    # Use the last name as the search query for broader coverage
    parts = _normalize(fighter_name).split()
    query = parts[-1] if parts else _normalize(fighter_name)

    try:
        response = httpx.get(
            url,
            params={"q": query, "restrict_sr": "1", "sort": "new", "limit": 100},
            headers={"User-Agent": _USER_AGENT},
            timeout=15,
            follow_redirects=True,
        )
        response.raise_for_status()
        data = response.json()
        children = data.get("data", {}).get("children", [])
        return [
            _parse_post(child["data"])
            for child in children
            if _post_matches(child.get("data", {}), tokens)
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_posts(fighter_name: str, limit: int = DEFAULT_LIMIT) -> list[Post]:
    """
    Fetch Reddit posts mentioning *fighter_name* from r/ufc and r/MMA.

    Matching is diacritics-insensitive and checks both title and body text.
    Results from both subreddits are merged and sorted by score (highest first).

    Parameters
    ----------
    fighter_name:
        Display name, e.g. ``"Jiří Procházka"`` or ``"Jon Jones"``.
    limit:
        Maximum number of posts to return (default 25).

    Returns
    -------
    list[Post]
        Matching posts sorted by score descending, capped at *limit*.
    """
    all_posts: list[Post] = []
    seen_ids: set[str] = set()

    for subreddit in SUBREDDITS:
        for post in _fetch_subreddit(subreddit, fighter_name):
            if post.post_id not in seen_ids:
                seen_ids.add(post.post_id)
                all_posts.append(post)

    all_posts.sort(key=lambda p: p.score, reverse=True)
    return all_posts[:limit]
