"""
Signal pipeline orchestrator — coordinates all signal harvesting steps
for a given fighter and event.

Usage
-----
    from src.pipeline.signal_pipeline import run_signal_pipeline

    summary = run_signal_pipeline("Jon Jones", fighter_id, event_id)
"""
from __future__ import annotations

from src.harvesters.market_harvester import fetch_markets
from src.harvesters.news_harvester import fetch_articles
from src.harvesters.reddit_harvester import fetch_posts
from src.harvesters.youtube_harvester import fetch_videos
from src.processors.news_processor import extract_signals
from src.storage.signal_storage import store_signal_log


def _market_signals(markets: list, fighter_name: str) -> dict:
    """Serialise market data into the processed_signals format for storage."""
    if not markets:
        return {}

    avg_prob = sum(m.probability_a for m in markets) / len(markets)
    sentiment = round((avg_prob - 0.5) * 2, 4)
    green_flags = (
        [f"Market favorite: {avg_prob:.0%} implied win probability"]
        if avg_prob >= 0.5
        else []
    )
    red_flags = (
        [f"Market underdog: {avg_prob:.0%} implied win probability"]
        if avg_prob < 0.5
        else []
    )

    return {
        "raw_summary": f"Prediction market: {fighter_name} implied win probability {avg_prob:.0%}",
        "injury_flags": False,
        "confidence_score": round(avg_prob, 4),
        "red_flags": red_flags,
        "green_flags": green_flags,
        "sentiment_score": sentiment,
        "notable_quotes": [],
    }


def _posts_to_articles(posts: list) -> list[dict]:
    """Convert Reddit Post objects to article-compatible dicts for extract_signals."""
    return [
        {
            "title": post.title,
            "raw_text": f"{post.title}\n{post.body}".strip(),
            "source": f"r/{post.subreddit}",
            "url": post.url,
            "published_date": str(post.created_utc),
        }
        for post in posts
    ]


def _videos_to_articles(videos: list) -> list[dict]:
    """Convert YouTube Video objects to article-compatible dicts for extract_signals."""
    return [
        {
            "title": video.title,
            "raw_text": f"{video.title}\n{video.description}".strip(),
            "source": f"YouTube/{video.channel_title}",
            "url": video.url,
            "published_date": video.published_at,
        }
        for video in videos
    ]


def run_signal_pipeline(
    fighter_name: str,
    fighter_id: str,
    event_id: str,
) -> dict:
    """
    Run the full signal harvesting pipeline for one fighter and event.

    Steps
    -----
    1. Fetch news articles → extract signals → store (source_type='news')
    2. Fetch Reddit posts → extract signals → store (source_type='reddit')
    3. Fetch YouTube videos → extract signals → store (source_type='youtube')
    4. Fetch prediction market odds → store (source_type='market')

    Each step is isolated — a failure in one step is recorded in the
    ``errors`` list but does not abort subsequent steps.

    Parameters
    ----------
    fighter_name:
        Display name used for filtering, e.g. ``"Jon Jones"``.
    fighter_id:
        Supabase UUID for the fighter row.
    event_id:
        Supabase UUID for the event row.

    Returns
    -------
    dict
        Summary with keys: fighter_name, fighter_id, event_id,
        articles_found, reddit_posts_found, youtube_videos_found,
        markets_found, news_signals_stored, reddit_signals_stored,
        youtube_signals_stored, market_signals_stored, errors.
    """
    summary: dict = {
        "fighter_name": fighter_name,
        "fighter_id": fighter_id,
        "event_id": event_id,
        "articles_found": 0,
        "reddit_posts_found": 0,
        "youtube_videos_found": 0,
        "markets_found": 0,
        "news_signals_stored": False,
        "reddit_signals_stored": False,
        "youtube_signals_stored": False,
        "market_signals_stored": False,
        "errors": [],
    }

    # ------------------------------------------------------------------
    # Step 1: News harvest → process → store
    # ------------------------------------------------------------------
    articles = []
    try:
        articles = fetch_articles(fighter_name)
        summary["articles_found"] = len(articles)
    except Exception as exc:
        summary["errors"].append(f"news harvester: {exc}")

    if articles:
        try:
            processed = extract_signals(articles, fighter_name)
            store_signal_log(fighter_id, event_id, "news", processed)
            summary["news_signals_stored"] = True
        except Exception as exc:
            summary["errors"].append(f"news processor/storage: {exc}")

    # ------------------------------------------------------------------
    # Step 2: Reddit harvest → process → store
    # ------------------------------------------------------------------
    posts = []
    try:
        posts = fetch_posts(fighter_name)
        summary["reddit_posts_found"] = len(posts)
    except Exception as exc:
        summary["errors"].append(f"reddit harvester: {exc}")

    if posts:
        try:
            reddit_articles = _posts_to_articles(posts)
            processed_reddit = extract_signals(reddit_articles, fighter_name)
            store_signal_log(fighter_id, event_id, "reddit", processed_reddit)
            summary["reddit_signals_stored"] = True
        except Exception as exc:
            summary["errors"].append(f"reddit processor/storage: {exc}")

    # ------------------------------------------------------------------
    # Step 3: YouTube harvest → process → store
    # ------------------------------------------------------------------
    videos = []
    try:
        videos = fetch_videos(fighter_name)
        summary["youtube_videos_found"] = len(videos)
    except Exception as exc:
        summary["errors"].append(f"youtube harvester: {exc}")

    if videos:
        try:
            yt_articles = _videos_to_articles(videos)
            processed_yt = extract_signals(yt_articles, fighter_name)
            store_signal_log(fighter_id, event_id, "youtube", processed_yt)
            summary["youtube_signals_stored"] = True
        except Exception as exc:
            summary["errors"].append(f"youtube processor/storage: {exc}")

    # ------------------------------------------------------------------
    # Step 4: Market harvest → store
    # ------------------------------------------------------------------
    markets = []
    try:
        markets = fetch_markets(keywords=[fighter_name])
        summary["markets_found"] = len(markets)
    except Exception as exc:
        summary["errors"].append(f"market harvester: {exc}")

    if markets:
        try:
            market_sigs = _market_signals(markets, fighter_name)
            store_signal_log(fighter_id, event_id, "market", market_sigs)
            summary["market_signals_stored"] = True
        except Exception as exc:
            summary["errors"].append(f"market storage: {exc}")

    return summary
