"""
Signal storage — reads and writes signal_logs in Supabase.

Usage
-----
    from src.storage.signal_storage import store_signal_log, get_signal_logs, get_latest_signals

    record = store_signal_log(fighter_id, event_id, "news", processed_signals)
    logs   = get_signal_logs(fighter_id, event_id)
    latest = get_latest_signals(fighter_id)
"""
from __future__ import annotations

from db.client import get_supabase_client


def store_signal_log(
    fighter_id: str,
    event_id: str,
    source_type: str,
    processed_signals: dict,
) -> dict:
    """
    Insert a signal log row into Supabase.

    Parameters
    ----------
    fighter_id:
        UUID of the fighter this signal concerns.
    event_id:
        UUID of the event this signal concerns.
    source_type:
        Origin of the signal, e.g. ``"news"``, ``"reddit"``, ``"youtube"``.
    processed_signals:
        Dict with any of: raw_summary, injury_flags, confidence_score,
        red_flags, green_flags, sentiment_score.

    Returns
    -------
    dict
        The inserted row as returned by Supabase, or ``{}`` if no data
        came back (e.g. RLS policy blocked the read).
    """
    client = get_supabase_client()

    record = {
        "fighter_id": fighter_id,
        "event_id": event_id,
        "source_type": source_type,
        "raw_summary": processed_signals.get("raw_summary"),
        "injury_flags": processed_signals.get("injury_flags", False),
        "confidence_score": processed_signals.get("confidence_score"),
        "red_flags": processed_signals.get("red_flags", []),
        "green_flags": processed_signals.get("green_flags", []),
        "sentiment_score": processed_signals.get("sentiment_score"),
        "notable_quotes": processed_signals.get("notable_quotes", []),
    }

    response = client.table("signal_logs").insert(record).execute()
    return response.data[0] if response.data else {}


def get_signal_logs(fighter_id: str, event_id: str) -> list[dict]:
    """
    Retrieve all signal logs for a fighter/event pair.

    Returns
    -------
    list[dict]
        All matching rows, ordered by Supabase default (insertion order).
    """
    client = get_supabase_client()

    response = (
        client.table("signal_logs")
        .select("*")
        .eq("fighter_id", fighter_id)
        .eq("event_id", event_id)
        .execute()
    )
    return response.data


def get_latest_signals(fighter_id: str) -> dict | None:
    """
    Return the most recent signal log for a fighter across all events.

    Returns
    -------
    dict | None
        The latest row, or ``None`` if no logs exist for this fighter.
    """
    client = get_supabase_client()

    response = (
        client.table("signal_logs")
        .select("*")
        .eq("fighter_id", fighter_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    return response.data[0] if response.data else None
