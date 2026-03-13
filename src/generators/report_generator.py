"""
Report generator — produces pre-fight bout reports using Claude Sonnet.

Usage
-----
    from src.generators.report_generator import generate_bout_report

    report = generate_bout_report(bout_id)
"""
from __future__ import annotations

import json

import anthropic
from pydantic import BaseModel, ValidationError

from db.client import get_supabase_client


MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = (
    "You are an expert MMA analyst with deep knowledge of fighting styles, "
    "fighter histories, and pre-fight intelligence signals. "
    "Your job is to analyse the available data and produce a well-reasoned "
    "bout prediction with conviction. Do not hedge or remain neutral — make a "
    "clear, data-supported pick backed by reasoned analysis. "
    "Always respond with raw JSON only — no preamble, no markdown fences, "
    "no explanation. Output must be a single JSON object."
)

_SCHEMA_DESCRIPTION = """{
  "prediction": "<name of the fighter you pick to win>",
  "confidence_tier": "<'High', 'Medium', or 'Low'>",
  "win_probability": <float between 0.0 and 1.0 for the predicted winner>,
  "method_prediction": "<'KO/TKO', 'Submission', 'Decision', or 'Unknown'>",
  "key_factors": ["list of decisive factors driving the pick (strings)"],
  "red_flags": ["list of concerns or risks for the predicted winner (strings)"],
  "green_flags": ["list of positive signals for the predicted winner (strings)"],
  "upset_alert": <true if this pick is an upset, false if picking the favourite>,
  "statistical_edge": "<one-sentence summary of the stats-based edge>",
  "intangibles_edge": "<one-sentence summary of camp/momentum/intangibles edge>",
  "narrative": "<one-paragraph fight narrative and prediction>"
}"""


class ReportSchema(BaseModel):
    prediction: str
    confidence_tier: str
    win_probability: float
    method_prediction: str
    key_factors: list[str]
    red_flags: list[str]
    green_flags: list[str]
    upset_alert: bool
    statistical_edge: str
    intangibles_edge: str
    narrative: str


def _strip_fences(text: str) -> str:
    """Remove markdown code fences if present."""
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        inner = lines[1:]
        if inner and inner[-1].strip() == "```":
            inner = inner[:-1]
        stripped = "\n".join(inner)
    return stripped.strip()


def _parse_response(text: str) -> dict | None:
    """Try to parse and validate the model's text as a ReportSchema. Returns None on failure."""
    try:
        data = json.loads(_strip_fences(text))
        validated = ReportSchema(**data)
        return validated.model_dump()
    except (json.JSONDecodeError, ValidationError, TypeError):
        return None


def _format_signals(signal: dict | None) -> str:
    """Render a signal_log row as a labelled text block for the prompt."""
    if signal is None:
        return "No current signals available"
    lines = []
    skip = {"id", "fighter_id", "event_id"}
    for key, val in signal.items():
        if key not in skip:
            lines.append(f"  {key}: {val}")
    return "\n".join(lines)


def _build_user_prompt(
    bout: dict,
    fighter_a: dict,
    fighter_b: dict,
    signal_a: dict | None,
    signal_b: dict | None,
) -> str:
    lines: list[str] = []

    lines.append("=== BOUT CONTEXT ===")
    lines.append(f"Weight class: {bout.get('weight_class', 'Unknown')}")
    lines.append(f"Main event: {bout.get('is_main_event', False)}")
    lines.append(f"Title fight: {bout.get('is_title_fight', False)}")
    lines.append("")

    lines.append("=== FIGHTER A ===")
    lines.append(f"Name: {fighter_a.get('name')}")
    if fighter_a.get("nickname"):
        lines.append(f"Nickname: {fighter_a['nickname']}")
    lines.append(
        f"Record: {fighter_a.get('record_wins')}-"
        f"{fighter_a.get('record_losses')}-"
        f"{fighter_a.get('record_draws')}"
    )
    lines.append(f"Weight class: {fighter_a.get('weight_class')}")
    lines.append("")

    lines.append("=== FIGHTER B ===")
    lines.append(f"Name: {fighter_b.get('name')}")
    if fighter_b.get("nickname"):
        lines.append(f"Nickname: {fighter_b['nickname']}")
    lines.append(
        f"Record: {fighter_b.get('record_wins')}-"
        f"{fighter_b.get('record_losses')}-"
        f"{fighter_b.get('record_draws')}"
    )
    lines.append(f"Weight class: {fighter_b.get('weight_class')}")
    lines.append("")

    lines.append("=== FIGHTER A SIGNALS ===")
    lines.append(_format_signals(signal_a))
    lines.append("")

    lines.append("=== FIGHTER B SIGNALS ===")
    lines.append(_format_signals(signal_b))
    lines.append("")

    lines.append("=== TASK ===")
    lines.append(
        f"Analyse the bout between {fighter_a.get('name')} (Fighter A) and "
        f"{fighter_b.get('name')} (Fighter B). "
        f"Return ONLY a JSON object matching this schema:\n{_SCHEMA_DESCRIPTION}"
    )

    return "\n".join(lines)


def _fetch_signals(client, fighter_id: str, event_id: str) -> dict | None:
    response = (
        client.table("signal_logs")
        .select("*")
        .eq("fighter_id", fighter_id)
        .eq("event_id", event_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    return response.data[0] if response.data else None


def _store_report(client, bout_id: str, report: dict) -> dict:
    record = {"bout_id": bout_id, **report}
    response = client.table("reports").insert(record).execute()
    return response.data[0] if response.data else record


def generate_bout_report(bout_id: str) -> dict:
    """
    Generate a pre-fight report for a bout.

    Parameters
    ----------
    bout_id:
        Supabase UUID of the bout row.

    Returns
    -------
    dict
        The stored report row (includes bout_id and all schema fields),
        or ``{"error": True, "bout_id": bout_id}`` on parse failure.
    """
    client = get_supabase_client()

    # ------------------------------------------------------------------
    # Fetch bout
    # ------------------------------------------------------------------
    bout_resp = (
        client.table("bouts")
        .select("*")
        .eq("id", bout_id)
        .limit(1)
        .execute()
    )
    bout = bout_resp.data[0]

    fighter_a_id = bout["fighter_a_id"]
    fighter_b_id = bout["fighter_b_id"]
    event_id = bout["event_id"]

    # ------------------------------------------------------------------
    # Fetch fighters
    # ------------------------------------------------------------------
    fa_resp = (
        client.table("fighters")
        .select("*")
        .eq("id", fighter_a_id)
        .limit(1)
        .execute()
    )
    fighter_a = fa_resp.data[0]

    fb_resp = (
        client.table("fighters")
        .select("*")
        .eq("id", fighter_b_id)
        .limit(1)
        .execute()
    )
    fighter_b = fb_resp.data[0]

    # ------------------------------------------------------------------
    # Fetch signal logs
    # ------------------------------------------------------------------
    signal_a = _fetch_signals(client, fighter_a_id, event_id)
    signal_b = _fetch_signals(client, fighter_b_id, event_id)

    # ------------------------------------------------------------------
    # Build prompt and call Claude
    # ------------------------------------------------------------------
    user_prompt = _build_user_prompt(bout, fighter_a, fighter_b, signal_a, signal_b)

    api_client = anthropic.Anthropic()

    response = api_client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = response.content[0].text
    result = _parse_response(raw)

    if result is not None:
        return _store_report(client, bout_id, result)

    # ------------------------------------------------------------------
    # Retry with correction prompt
    # ------------------------------------------------------------------
    correction_prompt = (
        "Your previous response could not be parsed as valid JSON matching the required schema. "
        "Please respond ONLY with a raw JSON object — no markdown, no explanation. "
        f"Required schema:\n{_SCHEMA_DESCRIPTION}"
    )
    response2 = api_client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": raw},
            {"role": "user", "content": correction_prompt},
        ],
    )
    raw2 = response2.content[0].text
    result2 = _parse_response(raw2)

    if result2 is not None:
        return _store_report(client, bout_id, result2)

    return {"error": True, "bout_id": bout_id}
