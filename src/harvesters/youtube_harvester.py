"""
YouTube harvester — fetches MMA pre-fight video metadata from YouTube Data API v3.

Matching strategy
-----------------
Queries the YouTube search API for videos mentioning the fighter name.
Results are filtered to only include videos whose title or description
mention the fighter (diacritics-insensitive, last-name matching).

Usage
-----
    from src.harvesters.youtube_harvester import fetch_videos

    videos = fetch_videos("Jiří Procházka")
    for v in videos:
        print(v.title, v.url)

Environment
-----------
    YOUTUBE_API_KEY — required. Requests return [] if not set.
"""
from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass

import httpx


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_API_URL = "https://www.googleapis.com/youtube/v3/search"
DEFAULT_LIMIT = 10


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Video:
    video_id: str
    title: str
    description: str
    channel_title: str
    published_at: str
    url: str


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


def _item_matches(item: dict, tokens: set[str]) -> bool:
    snippet = item.get("snippet", {})
    title = _normalize(snippet.get("title", ""))
    desc = _normalize(snippet.get("description", ""))
    combined = title + " " + desc
    return any(t in combined for t in tokens)


def _parse_video(item: dict) -> Video:
    video_id = item.get("id", {}).get("videoId", "")
    snippet = item.get("snippet", {})
    return Video(
        video_id=video_id,
        title=snippet.get("title", ""),
        description=snippet.get("description", ""),
        channel_title=snippet.get("channelTitle", ""),
        published_at=snippet.get("publishedAt", ""),
        url=f"https://www.youtube.com/watch?v={video_id}",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_videos(fighter_name: str, limit: int = DEFAULT_LIMIT) -> list[Video]:
    """
    Fetch YouTube videos mentioning *fighter_name*.

    Uses the YouTube Data API v3 search endpoint. Requires the
    ``YOUTUBE_API_KEY`` environment variable to be set.

    Matching is diacritics-insensitive and also matches on last name.
    Only videos (kind == "youtube#video") are returned.

    Parameters
    ----------
    fighter_name:
        Display name, e.g. ``"Jiří Procházka"`` or ``"Jon Jones"``.
    limit:
        Maximum number of videos to return (default 10).

    Returns
    -------
    list[Video]
        Matching videos, capped at *limit*. Returns [] on any error or
        if ``YOUTUBE_API_KEY`` is not configured.
    """
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        return []

    # Build search query from normalized last name for best recall
    parts = _normalize(fighter_name).split()
    query = f"{fighter_name} UFC MMA" if parts else fighter_name
    tokens = _name_tokens(fighter_name)

    try:
        response = httpx.get(
            _API_URL,
            params={
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": min(limit * 3, 50),  # over-fetch to allow filtering
                "key": api_key,
            },
            timeout=15,
            follow_redirects=True,
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        matching = [
            _parse_video(item)
            for item in items
            if item.get("id", {}).get("kind") == "youtube#video"
            and _item_matches(item, tokens)
        ]
        return matching[:limit]
    except Exception:
        return []
