"""
Event pipeline runner — orchestrates signal harvesting for every fighter
on a given UFC event card.

Usage
-----
    from src.pipeline.event_runner import run_event_pipeline

    summary = run_event_pipeline(event_id)
"""
from __future__ import annotations

import time

from db.client import get_supabase_client
from src.pipeline.signal_pipeline import run_signal_pipeline


def run_event_pipeline(event_id: str) -> dict:
    """
    Run the signal pipeline for every fighter on an event card.

    Steps
    -----
    1. Query the bouts table for all bouts belonging to ``event_id``.
    2. Collect all unique fighter IDs across those bouts.
    3. For each fighter, fetch their name from the fighters table.
    4. Call ``run_signal_pipeline`` for each fighter, with a 2-second
       delay between calls to avoid API rate limits.

    A failure for one fighter is recorded in ``errors`` and does not
    abort processing of the remaining fighters.

    Parameters
    ----------
    event_id:
        Supabase UUID of the event row.

    Returns
    -------
    dict
        Summary with keys: event_id, fighters_processed, fighters_failed,
        total_signals_stored, errors.
    """
    summary: dict = {
        "event_id": event_id,
        "fighters_processed": 0,
        "fighters_failed": 0,
        "total_signals_stored": 0,
        "errors": [],
    }

    client = get_supabase_client()

    # ------------------------------------------------------------------
    # Step 1: fetch bouts for this event
    # ------------------------------------------------------------------
    try:
        resp = (
            client.table("bouts")
            .select("fighter_a_id, fighter_b_id")
            .eq("event_id", event_id)
            .execute()
        )
        bouts = resp.data
    except Exception as exc:
        summary["errors"].append(f"bouts query: {exc}")
        return summary

    if not bouts:
        return summary

    # ------------------------------------------------------------------
    # Step 2: collect unique fighter IDs
    # ------------------------------------------------------------------
    fighter_ids: set[str] = set()
    for bout in bouts:
        if bout.get("fighter_a_id"):
            fighter_ids.add(bout["fighter_a_id"])
        if bout.get("fighter_b_id"):
            fighter_ids.add(bout["fighter_b_id"])

    # ------------------------------------------------------------------
    # Step 3 + 4: process each fighter
    # ------------------------------------------------------------------
    for i, fighter_id in enumerate(fighter_ids):
        if i > 0:
            time.sleep(2)

        try:
            name_resp = (
                client.table("fighters")
                .select("name")
                .eq("id", fighter_id)
                .execute()
            )
            fighter_name = name_resp.data[0]["name"]

            result = run_signal_pipeline(fighter_name, fighter_id, event_id)

            signals = (1 if result.get("news_signals_stored") else 0) + (
                1 if result.get("market_signals_stored") else 0
            )
            summary["fighters_processed"] += 1
            summary["total_signals_stored"] += signals

        except Exception as exc:
            summary["fighters_failed"] += 1
            summary["errors"].append(f"fighter {fighter_id}: {exc}")

    return summary
