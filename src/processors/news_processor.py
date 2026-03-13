"""
News signal processor — extracts structured signals from MMA news articles
using Claude Haiku via a single batched API call.
"""
from __future__ import annotations

import json

import anthropic
from pydantic import BaseModel, ValidationError


MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = (
    "You are an expert MMA intelligence analyst. "
    "Your job is to read MMA news articles and extract structured signals "
    "about a specific fighter's pre-fight situation. "
    "Always respond with raw JSON only — no preamble, no markdown fences, "
    "no explanation. Output must be a single JSON object."
)

_SCHEMA_DESCRIPTION = """{
  "raw_summary": "<one-paragraph summary of the fighter's pre-fight situation>",
  "injury_flags": <true if any injury concerns exist, false otherwise>,
  "confidence_score": <float between 0.0 (no confidence) and 1.0 (very high confidence)>,
  "red_flags": ["list of concerning signals (strings)"],
  "green_flags": ["list of positive signals (strings)"],
  "sentiment_score": <float between -1.0 (very negative) and 1.0 (very positive)>,
  "notable_quotes": ["list of notable direct quotes or paraphrased statements from coaches, fighters, or sources"]
}"""


class SignalSchema(BaseModel):
    raw_summary: str
    injury_flags: bool
    confidence_score: float
    red_flags: list[str]
    green_flags: list[str]
    sentiment_score: float
    notable_quotes: list[str]


def _empty_schema(error: bool = False) -> dict:
    base = {
        "raw_summary": "",
        "injury_flags": False,
        "confidence_score": 0.0,
        "red_flags": [],
        "green_flags": [],
        "sentiment_score": 0.0,
        "notable_quotes": [],
    }
    if error:
        base["error"] = True
    return base


def _article_field(art, field: str, default: str = "") -> str:
    """Read a field from either a dict or an Article dataclass."""
    if isinstance(art, dict):
        return art.get(field, default)
    return getattr(art, field, default)


def _build_user_prompt(articles: list, fighter_name: str) -> str:
    lines = [f"Fighter: {fighter_name}\n"]
    for i, art in enumerate(articles, 1):
        title  = _article_field(art, "title")
        source = _article_field(art, "source")
        text   = _article_field(art, "raw_text")
        lines.append(f"--- Article {i}: {title} ({source}) ---")
        lines.append(text)
        lines.append("")
    lines.append(
        f"\nAnalyse the articles above and extract signals about {fighter_name}. "
        f"Return ONLY a JSON object matching this schema:\n{_SCHEMA_DESCRIPTION}"
    )
    return "\n".join(lines)


def _strip_fences(text: str) -> str:
    """Remove markdown code fences if present (e.g. ```json ... ```)."""
    stripped = text.strip()
    if stripped.startswith("```"):
        # Drop the opening fence line and the closing fence
        lines = stripped.splitlines()
        # Remove first line (```json or ```) and last line (```)
        inner = lines[1:] if lines[-1].strip() == "```" else lines[1:]
        if inner and inner[-1].strip() == "```":
            inner = inner[:-1]
        stripped = "\n".join(inner)
    return stripped.strip()


def _parse_response(text: str) -> dict | None:
    """Try to parse and validate the model's text as a SignalSchema. Returns None on failure."""
    try:
        data = json.loads(_strip_fences(text))
        validated = SignalSchema(**data)
        return validated.model_dump()
    except (json.JSONDecodeError, ValidationError, TypeError):
        return None


def extract_signals(articles: list, fighter_name: str) -> dict:
    """
    Extract structured pre-fight signals from a list of news articles.

    Parameters
    ----------
    articles:
        List of article dicts with keys: title, raw_text, source, url, published_date.
    fighter_name:
        The fighter whose signals we want to extract.

    Returns
    -------
    dict
        Validated signal dict (keys: injury_flags, confidence_indicators, red_flags,
        green_flags, sentiment_score, notable_quotes, summary).
        On empty articles: zeroed schema, no API call.
        On parse failure after one retry: zeroed schema with ``error: True``.
    """
    if not articles:
        return _empty_schema()

    client = anthropic.Anthropic()
    user_prompt = _build_user_prompt(articles, fighter_name)

    # --- First attempt ---
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = response.content[0].text
    result = _parse_response(raw)
    if result is not None:
        return result

    # --- Retry with correction prompt ---
    correction_prompt = (
        "Your previous response could not be parsed as valid JSON matching the required schema. "
        "Please respond ONLY with a raw JSON object — no markdown, no explanation. "
        f"Required schema:\n{_SCHEMA_DESCRIPTION}\n\n"
        f"Original task: analyse articles about {fighter_name} and fill the schema."
    )
    response2 = client.messages.create(
        model=MODEL,
        max_tokens=1024,
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
        return result2

    return _empty_schema(error=True)
