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


def run_signal_pipeline(
    fighter_name: str,
    fighter_id: str,
    event_id: str,
) -> dict:
    """
    Run the full signal harvesting pipeline for one fighter and event.

    Steps
    -----
    1. Fetch news articles mentioning the fighter.
    2. Extract structured signals from those articles.
    3. Fetch prediction market odds for the fighter.
    4. Persist news signals and market signals to Supabase.

    Each step is isolated — a failure in one step is recorded in the
    ``errors`` list but does not abort subsequent steps.

    Parameters
    ----------
    fighter_name:
        Display name used for news and market filtering, e.g. ``"Jon Jones"``.
    fighter_id:
        Supabase UUID for the fighter row.
    event_id:
        Supabase UUID for the event row.

    Returns
    -------
    dict
        Summary with keys: fighter_name, fighter_id, event_id,
        articles_found, markets_found, news_signals_stored,
        market_signals_stored, errors.
    """
    summary: dict = {
        "fighter_name": fighter_name,
        "fighter_id": fighter_id,
        "event_id": event_id,
        "articles_found": 0,
        "markets_found": 0,
        "news_signals_stored": False,
        "market_signals_stored": False,
        "errors": [],
    }

    # ------------------------------------------------------------------
    # Step 1 + 2: News harvest → process → store
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
    # Step 3 + 4: Market harvest → store
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
