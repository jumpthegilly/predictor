"""
Card report generator — produces a card-level narrative summary using Claude Sonnet.

Usage
-----
    from src.generators.card_report import generate_card_report

    summary = generate_card_report(event_id)
"""
from __future__ import annotations

import anthropic

from db.client import get_supabase_client
from src.generators.report_generator import generate_bout_report


MODEL = "claude-sonnet-4-6"


def _format_reports(reports: list[dict]) -> str:
    if not reports:
        return "No bout reports available."
    lines: list[str] = []
    for i, r in enumerate(reports, 1):
        lines.append(f"--- Bout {i} ---")
        lines.append(f"Prediction: {r.get('prediction', 'Unknown')}")
        lines.append(f"Confidence: {r.get('confidence_tier', 'Unknown')}")
        lines.append(f"Method: {r.get('method_prediction', 'Unknown')}")
        lines.append(f"Upset alert: {r.get('upset_alert', False)}")
        lines.append(f"Statistical edge: {r.get('statistical_edge', '')}")
        lines.append(f"Intangibles edge: {r.get('intangibles_edge', '')}")
        lines.append(f"Narrative: {r.get('narrative', '')}")
        lines.append("")
    return "\n".join(lines)


def generate_card_report(event_id: str) -> dict:
    """
    Generate a card-level narrative summary for a UFC event.

    Steps
    -----
    1. Fetch the event name from the events table.
    2. Fetch all bouts for the event.
    3. Check which bouts already have stored reports.
    4. Call generate_bout_report for each bout that lacks a report.
    5. Fetch all reports for the event's bouts.
    6. Send the batch to Claude Sonnet for a card narrative.
    7. Store the narrative in events.card_summary.
    8. Return the summary dict with upset_alerts extracted from reports.

    Parameters
    ----------
    event_id:
        Supabase UUID of the event row.

    Returns
    -------
    dict
        Keys: event_id, event_name, bouts_processed, card_summary, upset_alerts.
    """
    client = get_supabase_client()

    # ------------------------------------------------------------------
    # 1. Fetch event
    # ------------------------------------------------------------------
    event_resp = (
        client.table("events")
        .select("*")
        .eq("id", event_id)
        .limit(1)
        .execute()
    )
    event = event_resp.data[0]
    event_name = event["name"]

    # ------------------------------------------------------------------
    # 2. Fetch bouts
    # ------------------------------------------------------------------
    bouts_resp = (
        client.table("bouts")
        .select("*")
        .eq("event_id", event_id)
        .execute()
    )
    bouts = bouts_resp.data
    bout_ids = [b["id"] for b in bouts]

    # ------------------------------------------------------------------
    # 3. Check existing reports
    # ------------------------------------------------------------------
    if bout_ids:
        existing_resp = (
            client.table("reports")
            .select("bout_id")
            .in_("bout_id", bout_ids)
            .execute()
        )
        reported_ids = {r["bout_id"] for r in existing_resp.data}
    else:
        reported_ids = set()

    # ------------------------------------------------------------------
    # 4. Generate missing reports
    # ------------------------------------------------------------------
    for bout in bouts:
        if bout["id"] not in reported_ids:
            generate_bout_report(bout["id"])

    # ------------------------------------------------------------------
    # 5. Fetch all reports for the event's bouts
    # ------------------------------------------------------------------
    if bout_ids:
        all_resp = (
            client.table("reports")
            .select("*")
            .in_("bout_id", bout_ids)
            .execute()
        )
        all_reports = all_resp.data
    else:
        all_reports = []

    # ------------------------------------------------------------------
    # 6. Build prompt and call Claude
    # ------------------------------------------------------------------
    reports_text = _format_reports(all_reports)
    prompt = (
        f"You are a senior MMA analyst. Given the following individual bout predictions "
        f"for {event_name}, produce a card-level narrative summary. Include: the two most "
        f"compelling fights and why, any notable upset alerts, the overall card quality "
        f"assessment, and a best bet recommendation with reasoning. Write for a knowledgeable "
        f"MMA fan audience. Return plain text, not JSON.\n\n{reports_text}"
    )

    api_client = anthropic.Anthropic()
    response = api_client.messages.create(
        model=MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    card_summary = response.content[0].text.strip()

    # ------------------------------------------------------------------
    # 7. Store card narrative in events table
    # ------------------------------------------------------------------
    client.table("events").update({"card_summary": card_summary}).eq("id", event_id).execute()

    # ------------------------------------------------------------------
    # 8. Extract upset alerts
    # ------------------------------------------------------------------
    upset_alerts = [
        r.get("prediction", "")
        for r in all_reports
        if r.get("upset_alert") is True
    ]

    return {
        "event_id": event_id,
        "event_name": event_name,
        "bouts_processed": len(bouts),
        "card_summary": card_summary,
        "upset_alerts": upset_alerts,
    }
