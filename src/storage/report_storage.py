"""
Report storage — reads bout reports and card summaries from Supabase.

Usage
-----
    from src.storage.report_storage import get_bout_report, get_event_reports

    report = get_bout_report(bout_id)
    reports = get_event_reports(event_id)
"""
from __future__ import annotations

from db.client import get_supabase_client


def get_bout_report(bout_id: str) -> dict | None:
    """
    Fetch the most recent stored report for a bout.

    Parameters
    ----------
    bout_id:
        Supabase UUID of the bout.

    Returns
    -------
    dict | None
        The latest report row, or ``None`` if no report exists.
    """
    client = get_supabase_client()
    response = (
        client.table("reports")
        .select("*")
        .eq("bout_id", bout_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    return response.data[0] if response.data else None


def get_event_reports(event_id: str) -> list[dict]:
    """
    Fetch all stored bout reports for an event.

    Joins reports → bouts → events to filter by event_id.

    Parameters
    ----------
    event_id:
        Supabase UUID of the event.

    Returns
    -------
    list[dict]
        All report rows for bouts belonging to this event.
    """
    client = get_supabase_client()

    # First fetch all bout_ids for this event
    bouts_resp = (
        client.table("bouts")
        .select("id")
        .eq("event_id", event_id)
        .execute()
    )
    bout_ids = [b["id"] for b in bouts_resp.data]
    if not bout_ids:
        return []

    # Fetch reports for those bouts
    reports_resp = (
        client.table("reports")
        .select("*")
        .in_("bout_id", bout_ids)
        .execute()
    )
    return reports_resp.data
