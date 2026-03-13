"""
Prediction market harvester — queries Polymarket and Kalshi for UFC/MMA markets.

Usage
-----
    from src.harvesters.market_harvester import fetch_markets

    markets = fetch_markets()                        # default: UFC + MMA
    markets = fetch_markets(keywords=["Jon Jones"])  # fighter-specific
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

POLYMARKET_URL = "https://gamma-api.polymarket.com/markets"
KALSHI_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"

DEFAULT_KEYWORDS = ["UFC", "MMA"]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Market:
    market_id: str
    question: str
    outcome_a: str
    outcome_b: str
    probability_a: float
    probability_b: float
    volume: float
    last_updated: str


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _matches(text: str, keywords: list[str]) -> bool:
    lower = text.lower()
    return any(kw.lower() in lower for kw in keywords)


def _fetch_polymarket(keywords: list[str]) -> list[Market]:
    params = {
        "active": "true",
        "closed": "false",
        "limit": "100",
    }
    response = httpx.get(POLYMARKET_URL, params=params, timeout=15)
    response.raise_for_status()
    raw: list[dict] = response.json()

    markets = []
    for m in raw:
        question = m.get("question", "")
        if not _matches(question, keywords):
            continue

        try:
            outcomes = json.loads(m.get("outcomes", "[]"))
            prices = json.loads(m.get("outcomePrices", "[]"))
        except (json.JSONDecodeError, TypeError):
            outcomes, prices = [], []

        outcome_a = outcomes[0] if len(outcomes) > 0 else ""
        outcome_b = outcomes[1] if len(outcomes) > 1 else ""
        probability_a = float(prices[0]) if len(prices) > 0 else 0.0
        probability_b = float(prices[1]) if len(prices) > 1 else 0.0

        markets.append(
            Market(
                market_id=str(m.get("id", "")),
                question=question,
                outcome_a=outcome_a,
                outcome_b=outcome_b,
                probability_a=probability_a,
                probability_b=probability_b,
                volume=float(m.get("volume") or 0),
                last_updated=m.get("updatedAt", ""),
            )
        )

    return markets


def _fetch_kalshi(keywords: list[str]) -> list[Market]:
    params = {"limit": "200", "status": "open"}
    response = httpx.get(KALSHI_URL, params=params, timeout=15)
    response.raise_for_status()
    raw: list[dict] = response.json().get("markets", [])

    markets = []
    for m in raw:
        question = m.get("subtitle", "") or m.get("title", "")
        if not _matches(question, keywords):
            continue

        last_price = float(m.get("last_price_dollars", "0") or 0)
        probability_a = last_price
        probability_b = round(1.0 - probability_a, 10)

        markets.append(
            Market(
                market_id=str(m.get("ticker", "")),
                question=question,
                outcome_a="Yes",
                outcome_b="No",
                probability_a=probability_a,
                probability_b=probability_b,
                volume=float(m.get("volume") or 0),
                last_updated=m.get("close_time", ""),
            )
        )

    return markets


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_markets(keywords: list[str] | None = None) -> list[Market]:
    """
    Fetch UFC/MMA prediction markets from Polymarket and Kalshi.

    Parameters
    ----------
    keywords:
        Case-insensitive terms to match against market questions.
        Defaults to ``["UFC", "MMA"]``.

    Returns
    -------
    list[Market]
        Combined results from both sources. Dead or erroring sources are
        silently skipped.
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    results: list[Market] = []

    for fetcher in (_fetch_polymarket, _fetch_kalshi):
        try:
            results.extend(fetcher(keywords))
        except Exception:
            continue

    return results
