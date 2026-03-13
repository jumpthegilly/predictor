"""
News harvester — fetches UFC/MMA RSS feeds and filters by fighter name.

Matching strategy
-----------------
Primary: scan all feed entries, match on normalized full name OR last name.
         Diacritics are stripped so "Procházka" matches "Prochazka" in articles.
Fallback: if primary returns 0 results, re-scan the first 20 entries per feed
          using first name as an additional token (broader sweep).

Usage
-----
    from src.harvesters.news_harvester import fetch_articles

    articles = fetch_articles("Jiří Procházka")
    for a in articles:
        print(a.title, a.source)
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass

import feedparser
import httpx

# ---------------------------------------------------------------------------
# RSS feed registry
# ---------------------------------------------------------------------------

RSS_FEEDS: dict[str, str] = {
    "MMA Weekly": "https://www.mmaweekly.com/feed",
    "MMA Fighting": "https://www.mmafighting.com/rss/index.xml",
    "Sherdog": "https://www.sherdog.com/rss/news.xml",
}

FALLBACK_LIMIT = 20  # entries per feed considered in the fallback sweep

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Article:
    title: str
    url: str
    published_date: str
    source: str
    raw_text: str


# ---------------------------------------------------------------------------
# Matching helpers (module-level so tests can import them directly)
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Lowercase and strip diacritics: 'Procházka' → 'prochazka'."""
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def _name_tokens(fighter_name: str, include_first: bool = False) -> set[str]:
    """
    Build a set of search tokens from a fighter name.

    Always includes: normalized full name, normalized last name.
    ``include_first=True`` adds the normalized first name (used for fallback).
    """
    tokens: set[str] = set()
    norm = _normalize(fighter_name)
    tokens.add(norm)               # "jiri prochazka"
    parts = norm.split()
    if parts:
        tokens.add(parts[-1])      # "prochazka"
        if include_first and len(parts) > 1:
            tokens.add(parts[0])   # "jiri"
    return tokens


def _article_matches(raw_text: str, tokens: set[str]) -> bool:
    norm = _normalize(raw_text)
    return any(t in norm for t in tokens)


def _entry_raw_text(entry) -> str:
    title = entry.get("title", "")
    summary = entry.get("summary", "")
    content_blocks = entry.get("content", [])
    content = content_blocks[0].get("value", "") if content_blocks else ""
    return " ".join(filter(None, [title, summary, content])).strip()


def _make_article(entry, source: str) -> Article:
    return Article(
        title=entry.get("title", ""),
        url=entry.get("link", ""),
        published_date=entry.get("published", ""),
        source=source,
        raw_text=_entry_raw_text(entry),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_articles(fighter_name: str) -> list[Article]:
    """
    Fetch articles from all configured RSS feeds and return those that
    mention *fighter_name*.

    Matching is diacritics-insensitive and also matches on last name alone
    (e.g. "Procházka" matches articles containing "Prochazka" or just
    "Prochazka").  If the primary scan returns nothing, a fallback sweep
    checks the 20 most-recent entries per feed using first name as an
    additional token.

    Dead or error-returning feeds are silently skipped.

    Parameters
    ----------
    fighter_name:
        Display name, e.g. ``"Jiří Procházka"`` or ``"Curtis Blaydes"``.

    Returns
    -------
    list[Article]
        Articles mentioning the fighter, ordered feed-first.
    """
    # ------------------------------------------------------------------
    # Step 1: collect all entries from every feed (single HTTP pass)
    # ------------------------------------------------------------------
    # Each element: (source_name, entry_object, raw_text_str, position_in_feed)
    all_entries: list[tuple[str, object, str, int]] = []

    for source, url in RSS_FEEDS.items():
        try:
            response = httpx.get(url, timeout=15, follow_redirects=True)
            response.raise_for_status()
            feed = feedparser.parse(response.text)
        except Exception:
            continue

        for pos, entry in enumerate(feed.entries):
            all_entries.append((source, entry, _entry_raw_text(entry), pos))

    # ------------------------------------------------------------------
    # Step 2: primary filter — full name + last name
    # ------------------------------------------------------------------
    primary_tokens = _name_tokens(fighter_name, include_first=False)
    articles = [
        _make_article(entry, source)
        for source, entry, raw, _pos in all_entries
        if _article_matches(raw, primary_tokens)
    ]

    if articles:
        return articles

    # ------------------------------------------------------------------
    # Step 3: fallback sweep — first 20 per feed, add first name token
    # ------------------------------------------------------------------
    fallback_tokens = _name_tokens(fighter_name, include_first=True)
    fallback_articles: list[Article] = []
    for source, entry, raw, pos in all_entries:
        if pos >= FALLBACK_LIMIT:
            continue
        if _article_matches(raw, fallback_tokens):
            fallback_articles.append(_make_article(entry, source))

    return fallback_articles
