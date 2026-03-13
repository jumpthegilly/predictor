"""
Live integration test for the report generator.

Finds the nearest upcoming event, lists its bouts, then runs
generate_bout_report() for each bout and prints the results.

NOT a pytest file — run directly:

    python -m scripts.test_live_report
"""
from __future__ import annotations

import sys
from dotenv import load_dotenv

load_dotenv()

from db.client import get_supabase_client
from src.generators.report_generator import generate_bout_report

# ~$0.003 input + ~$0.008 output per Sonnet call (rough estimate)
COST_PER_BOUT_USD = 0.012


# ---------------------------------------------------------------------------
# Helpers (shared pattern with test_live_event_runner)
# ---------------------------------------------------------------------------

def separator(char: str = "─", width: int = 70) -> str:
    return char * width


def fetch_upcoming_event(client) -> dict | None:
    resp = (
        client.table("events")
        .select("id, name, date, location, status")
        .eq("status", "upcoming")
        .order("date", desc=False)
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]
    resp2 = (
        client.table("events")
        .select("id, name, date, location, status")
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    return resp2.data[0] if resp2.data else None


def fetch_bouts_for_event(client, event_id: str) -> list[dict]:
    resp = (
        client.table("bouts")
        .select("id, fighter_a_id, fighter_b_id, weight_class, is_main_event, is_title_fight")
        .eq("event_id", event_id)
        .execute()
    )
    return resp.data or []


def fetch_fighter_name(client, fighter_id: str) -> str:
    resp = (
        client.table("fighters")
        .select("name")
        .eq("id", fighter_id)
        .limit(1)
        .execute()
    )
    return resp.data[0]["name"] if resp.data else f"<unknown {fighter_id[:8]}>"


def print_event_header(event: dict) -> None:
    print(f"\n{separator('═')}")
    print(f"  EVENT: {event['name']}")
    print(separator("═"))
    print(f"  Date     : {event.get('date', 'TBD')}")
    print(f"  Location : {event.get('location', 'TBD')}")
    print(f"  Status   : {event.get('status', 'unknown')}")
    print(f"  ID       : {event['id']}")


def print_card(client, bouts: list[dict]) -> list[tuple[dict, str, str]]:
    """Print the card and return list of (bout, name_a, name_b) tuples."""
    print(f"\n{separator('─')}")
    print(f"  CARD ({len(bouts)} bout(s))")
    print(separator("─"))
    enriched = []
    for i, bout in enumerate(bouts):
        name_a = fetch_fighter_name(client, bout["fighter_a_id"])
        name_b = fetch_fighter_name(client, bout["fighter_b_id"])
        tags = []
        if bout.get("is_main_event"):
            tags.append("MAIN EVENT")
        if bout.get("is_title_fight"):
            tags.append("TITLE")
        tag_str = f"  [{', '.join(tags)}]" if tags else ""
        weight = bout.get("weight_class") or ""
        print(f"  {i + 1}. {name_a} vs {name_b}  {weight}{tag_str}")
        enriched.append((bout, name_a, name_b))
    return enriched


def print_report(report: dict, name_a: str, name_b: str) -> None:
    print(f"\n{separator('═')}")
    print(f"  REPORT: {name_a} vs {name_b}")
    print(separator("═"))

    if report.get("error"):
        print("  ✗  Report generation failed (Claude returned unparseable JSON)")
        return

    pick = report.get("prediction", "—")
    tier = report.get("confidence_tier", "—")
    prob = report.get("win_probability")
    method = report.get("method_prediction", "—")
    upset = report.get("upset_alert", False)

    print(f"  Pick            : {pick}")
    print(f"  Confidence      : {tier}")
    if prob is not None:
        print(f"  Win probability : {prob:.0%}")
    print(f"  Method          : {method}")
    if upset:
        print("  ⚠  UPSET ALERT")

    green = report.get("green_flags") or []
    red = report.get("red_flags") or []
    key = report.get("key_factors") or []

    if key:
        print(f"\n  Key factors:")
        for f in key:
            print(f"    + {f}")
    if green:
        print(f"\n  Green flags:")
        for f in green:
            print(f"    ✓ {f}")
    if red:
        print(f"\n  Red flags:")
        for f in red:
            print(f"    ✗ {f}")

    stat_edge = report.get("statistical_edge")
    int_edge = report.get("intangibles_edge")
    if stat_edge:
        print(f"\n  Statistical edge  : {stat_edge}")
    if int_edge:
        print(f"  Intangibles edge  : {int_edge}")

    narrative = report.get("narrative")
    if narrative:
        print(f"\n  Narrative:")
        # Wrap at ~65 chars
        words = narrative.split()
        line = "    "
        for word in words:
            if len(line) + len(word) + 1 > 68:
                print(line)
                line = "    " + word + " "
            else:
                line += word + " "
        if line.strip():
            print(line)

    report_id = report.get("id")
    if report_id:
        print(f"\n  Stored report ID  : {report_id}")

    print(separator("─"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    client = get_supabase_client()

    # ------------------------------------------------------------------
    # 1. Find event
    # ------------------------------------------------------------------
    event = fetch_upcoming_event(client)
    if event is None:
        print("✗  No event found in database.")
        print("   Run: python -m scripts.test_live_event_runner")
        sys.exit(1)

    print_event_header(event)
    event_id = event["id"]

    # ------------------------------------------------------------------
    # 2. Fetch and print card
    # ------------------------------------------------------------------
    bouts = fetch_bouts_for_event(client, event_id)
    if not bouts:
        print("\n✗  No bouts found for this event.")
        sys.exit(1)

    enriched = print_card(client, bouts)

    # ------------------------------------------------------------------
    # 3. Confirm
    # ------------------------------------------------------------------
    bout_count = len(enriched)
    estimated_cost = bout_count * COST_PER_BOUT_USD
    print(f"\n  {bout_count} bout(s) to generate reports for.")
    print(f"  Estimated cost: ${estimated_cost:.3f} (~${COST_PER_BOUT_USD}/bout, Claude Sonnet)")
    print()
    try:
        input("  Press Enter to generate reports for all bouts... (Ctrl-C to abort) ")
    except KeyboardInterrupt:
        print("\n  Aborted.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 4. Generate reports
    # ------------------------------------------------------------------
    results = []
    for i, (bout, name_a, name_b) in enumerate(enriched):
        bout_id = bout["id"]
        print(f"\n→ [{i + 1}/{bout_count}] Generating report: {name_a} vs {name_b}…")
        try:
            report = generate_bout_report(bout_id)
            results.append((report, name_a, name_b, None))
            status = "✓" if not report.get("error") else "✗ (parse error)"
            print(f"  {status}")
        except Exception as exc:
            results.append((None, name_a, name_b, str(exc)))
            print(f"  ✗ Exception: {exc}")

    # ------------------------------------------------------------------
    # 5. Print reports
    # ------------------------------------------------------------------
    for report, name_a, name_b, err in results:
        if err:
            print(f"\n{separator('═')}")
            print(f"  REPORT: {name_a} vs {name_b}")
            print(separator("═"))
            print(f"  ✗  Error: {err}")
            print(separator("─"))
        else:
            print_report(report, name_a, name_b)

    # ------------------------------------------------------------------
    # 6. Summary
    # ------------------------------------------------------------------
    ok = sum(1 for r, *_ in results if r and not r.get("error"))
    failed = bout_count - ok
    print(f"\n{separator('═')}")
    print(f"  DONE: {ok}/{bout_count} reports generated successfully", end="")
    if failed:
        print(f"  ({failed} failed)", end="")
    print(f"\n{separator('═')}\n")


if __name__ == "__main__":
    run()
