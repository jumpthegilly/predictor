"""
Live integration test for the news processor.

Runs against the real Anthropic API and real RSS feeds.
NOT a pytest file — run directly:

    python -m scripts.test_live_processor

Picks a fighter with reliable recent news coverage, fetches articles,
calls Claude Haiku, and prints the structured output alongside a cost estimate.
"""
from __future__ import annotations

import json
import time
from dotenv import load_dotenv

load_dotenv()

from src.harvesters.news_harvester import fetch_articles, Article
from src.processors.news_processor import (
    MODEL,
    SYSTEM_PROMPT,
    SignalSchema,
    _build_user_prompt,
    _parse_response,
    _empty_schema,
)
import anthropic

# ---------------------------------------------------------------------------
# Haiku pricing (per million tokens, as of 2025)
# ---------------------------------------------------------------------------
INPUT_COST_PER_M  = 1.00   # USD per 1M input tokens
OUTPUT_COST_PER_M = 5.00   # USD per 1M output tokens

FIGHTERS = ["Jon Jones", "Alex Pereira", "Conor McGregor"]


def estimate_cost(usage) -> tuple[float, float, float]:
    input_tokens  = usage.input_tokens
    output_tokens = usage.output_tokens
    input_cost    = (input_tokens  / 1_000_000) * INPUT_COST_PER_M
    output_cost   = (output_tokens / 1_000_000) * OUTPUT_COST_PER_M
    return input_cost, output_cost, input_cost + output_cost


def separator(char: str = "─", width: int = 70) -> str:
    return char * width


def print_articles(articles: list[Article], fighter_name: str) -> None:
    print(f"\n{separator('═')}")
    print(f"  ARTICLES FOUND FOR: {fighter_name.upper()}")
    print(separator("═"))
    if not articles:
        print("  ⚠  No articles found.")
        return
    for i, art in enumerate(articles, 1):
        print(f"\n  [{i}] {art.title}")
        print(f"       Source  : {art.source}")
        print(f"       Date    : {art.published_date}")
        print(f"       URL     : {art.url}")
        snippet = art.raw_text[:200].replace("\n", " ")
        if len(art.raw_text) > 200:
            snippet += "…"
        print(f"       Snippet : {snippet}")


def print_raw_response(raw: str) -> None:
    print(f"\n{separator('─')}")
    print("  RAW HAIKU RESPONSE")
    print(separator("─"))
    print(raw)


def print_parsed_output(parsed: dict) -> None:
    print(f"\n{separator('─')}")
    print("  PARSED SIGNAL SCHEMA")
    print(separator("─"))
    print(json.dumps(parsed, indent=2))


def print_cost(usage, attempt: int) -> None:
    input_cost, output_cost, total = estimate_cost(usage)
    print(f"\n{separator('─')}")
    print(f"  API CALL COST ESTIMATE  (attempt {attempt})")
    print(separator("─"))
    print(f"  Model          : {MODEL}")
    print(f"  Input tokens   : {usage.input_tokens:,}  → ${input_cost:.6f}")
    print(f"  Output tokens  : {usage.output_tokens:,}  → ${output_cost:.6f}")
    print(f"  Total          : ${total:.6f}")


def run(fighter_name: str) -> None:
    print(f"\n{'═' * 70}")
    print(f"  LIVE PROCESSOR TEST — {fighter_name.upper()}")
    print(f"{'═' * 70}")

    # ------------------------------------------------------------------
    # 1. Fetch articles
    # ------------------------------------------------------------------
    print(f"\n→ Fetching RSS articles for '{fighter_name}'…")
    t0 = time.time()
    articles = fetch_articles(fighter_name)
    elapsed = time.time() - t0
    print(f"  Found {len(articles)} article(s) in {elapsed:.1f}s")
    print_articles(articles, fighter_name)

    if not articles:
        print("\n⚠  No articles — skipping API call.")
        return

    # ------------------------------------------------------------------
    # 2. Build prompt and call Haiku
    # ------------------------------------------------------------------
    client = anthropic.Anthropic()
    user_prompt = _build_user_prompt(articles, fighter_name)

    print(f"\n→ Calling {MODEL}…")
    t1 = time.time()
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    elapsed = time.time() - t1
    print(f"  Response received in {elapsed:.1f}s")

    raw = response.content[0].text
    print_raw_response(raw)
    print_cost(response.usage, attempt=1)

    # ------------------------------------------------------------------
    # 3. Parse + validate
    # ------------------------------------------------------------------
    parsed = _parse_response(raw)

    if parsed is None:
        print("\n⚠  Parse failed on first attempt — retrying with correction prompt…")

        from src.processors.news_processor import _SCHEMA_DESCRIPTION
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
        print_raw_response(raw2)
        print_cost(response2.usage, attempt=2)
        parsed = _parse_response(raw2)

        if parsed is None:
            print("\n✗  Both attempts failed — returning empty schema with error flag.")
            parsed = _empty_schema(error=True)

    print_parsed_output(parsed)

    # ------------------------------------------------------------------
    # 4. Summary
    # ------------------------------------------------------------------
    print(f"\n{separator('═')}")
    print("  SUMMARY")
    print(separator("═"))
    print(f"  Fighter        : {fighter_name}")
    print(f"  Articles used  : {len(articles)}")
    print(f"  Sentiment score: {parsed.get('sentiment_score', 'N/A')}")
    print(f"  Red flags      : {len(parsed.get('red_flags', []))}")
    print(f"  Green flags    : {len(parsed.get('green_flags', []))}")
    print(f"  Injury flags   : {len(parsed.get('injury_flags', []))}")
    error = parsed.get("error")
    status = "✗ PARSE ERROR" if error else "✓ OK"
    print(f"  Status         : {status}")
    print(separator("═"))


if __name__ == "__main__":
    import sys

    fighter = sys.argv[1] if len(sys.argv) > 1 else FIGHTERS[0]
    run(fighter)
