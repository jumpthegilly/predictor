"""
Diagnostic script for the news harvester.

Prints:
  1. The 5 most recent headlines from each RSS feed (no fighter filter)
  2. Article counts for a set of UFC 327 fighter names

Run:
    python -m scripts.diagnose_harvester
"""
from __future__ import annotations

import feedparser
import httpx
from dotenv import load_dotenv

load_dotenv()

from src.harvesters.news_harvester import RSS_FEEDS, fetch_articles

PROBE_NAMES = ["Prochazka", "Costa", "Blaydes", "Gastelum", "Holland"]


def separator(char: str = "─", width: int = 70) -> str:
    return char * width


def fetch_raw_feed(source: str, url: str) -> list[str]:
    """Return titles of all entries from one feed, or an error string."""
    try:
        resp = httpx.get(url, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
        return [e.get("title", "<no title>") for e in feed.entries]
    except Exception as exc:
        return [f"ERROR: {exc}"]


def run() -> None:
    # ------------------------------------------------------------------
    # 1. Raw feed headlines — no fighter filter
    # ------------------------------------------------------------------
    print(f"\n{separator('═')}")
    print("  RSS FEED FRESHNESS CHECK  (5 most recent headlines per feed)")
    print(separator("═"))

    for source, url in RSS_FEEDS.items():
        print(f"\n  {source}  ({url})")
        print(f"  {separator('·', 66)}")
        titles = fetch_raw_feed(source, url)
        if not titles:
            print("  (no entries returned)")
        for i, title in enumerate(titles[:5], 1):
            print(f"  {i}. {title}")

    # ------------------------------------------------------------------
    # 2. Per-fighter article counts
    # ------------------------------------------------------------------
    print(f"\n{separator('═')}")
    print("  ARTICLE COUNTS FOR UFC 327 FIGHTERS")
    print(separator("═"))
    print(f"  {'Fighter':<25}  {'Articles found':>15}")
    print(f"  {separator('·', 42)}")

    for name in PROBE_NAMES:
        articles = fetch_articles(name)
        count = len(articles)
        flag = "✓" if count > 0 else "✗"
        print(f"  {flag} {name:<23}  {count:>15}")
        if articles:
            for a in articles[:3]:
                snippet = a.title[:60]
                print(f"      → [{a.source}] {snippet}")

    print(separator("═"))


if __name__ == "__main__":
    run()
