"""
Live integration test for the event pipeline runner.

Queries Supabase for the nearest upcoming event, prints the card,
asks for confirmation, then runs run_event_pipeline and prints results.

NOT a pytest file — run directly:

    python -m scripts.test_live_event_runner

If no upcoming event exists in the database, a placeholder event is
inserted using real fighters from the fighters table.
"""
from __future__ import annotations

import sys
from datetime import date
from dotenv import load_dotenv

load_dotenv()

from db.client import get_supabase_client
from src.pipeline.event_runner import run_event_pipeline

# ---------------------------------------------------------------------------
# Cost estimate constant (matches live processor)
# ---------------------------------------------------------------------------
COST_PER_FIGHTER_USD = 0.0018

# ---------------------------------------------------------------------------
# Placeholder event — real upcoming UFC card fighters.
# Fighter names must exist in the fighters table (loaded via load_ufc_data).
# ---------------------------------------------------------------------------
PLACEHOLDER_EVENT = {
    "name": "UFC 313: Pereira vs Ankalaev",
    "date": "2026-03-08",
    "location": "T-Mobile Arena, Las Vegas",
    "status": "upcoming",
}

# Real fighters who should exist in the fighters table from UFC-DataLab data
PLACEHOLDER_FIGHTERS = [
    "Alex Pereira",
    "Magomed Ankalaev",
    "Islam Makhachev",
]

PLACEHOLDER_BOUTS = [
    # Main event
    ("Alex Pereira", "Magomed Ankalaev", "Light Heavyweight", True, True),
    # Co-main
    ("Islam Makhachev", "Arman Tsarukyan", "Lightweight", False, True),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def separator(char: str = "─", width: int = 70) -> str:
    return char * width


def lookup_fighter_id(client, name: str) -> str | None:
    """Return the Supabase UUID for a fighter by exact name (case-insensitive)."""
    resp = (
        client.table("fighters")
        .select("id, name")
        .ilike("name", name)
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]["id"]
    return None


def fetch_upcoming_event(client) -> dict | None:
    """
    Return the nearest upcoming event row, or None if none exists.

    Strategy:
      1. Try status = 'upcoming', ordered by date ascending.
      2. Fall back to the most recent event by date regardless of status.
    """
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

    # Fallback: most recent event in the table
    resp2 = (
        client.table("events")
        .select("id, name, date, location, status")
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    if resp2.data:
        return resp2.data[0]

    return None


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
    if resp.data:
        return resp.data[0]["name"]
    return f"<unknown {fighter_id[:8]}>"


def insert_placeholder_event(client) -> dict:
    """
    Insert a placeholder upcoming event with real fighters from the DB.
    Returns the inserted event row.
    """
    print("\n⚠  No upcoming event found in database.")
    print("   Inserting placeholder event with real fighters from the fighters table…")

    # Insert event
    event_resp = (
        client.table("events")
        .insert(PLACEHOLDER_EVENT)
        .execute()
    )
    event = event_resp.data[0]
    event_id = event["id"]
    print(f"   ✓ Created event: {event['name']} ({event_id[:8]}…)")

    # Insert bouts using looked-up fighter IDs
    inserted_bouts = 0
    for fighter_a_name, fighter_b_name, weight_class, is_main, is_title in PLACEHOLDER_BOUTS:
        id_a = lookup_fighter_id(client, fighter_a_name)
        id_b = lookup_fighter_id(client, fighter_b_name)

        if not id_a:
            print(f"   ⚠  Fighter not found in DB: '{fighter_a_name}' — skipping bout")
            continue
        if not id_b:
            print(f"   ⚠  Fighter not found in DB: '{fighter_b_name}' — skipping bout")
            continue

        client.table("bouts").insert({
            "event_id": event_id,
            "fighter_a_id": id_a,
            "fighter_b_id": id_b,
            "weight_class": weight_class,
            "is_main_event": is_main,
            "is_title_fight": is_title,
        }).execute()
        print(f"   ✓ Bout: {fighter_a_name} vs {fighter_b_name}")
        inserted_bouts += 1

    if inserted_bouts == 0:
        print("\n✗  No bouts could be inserted — fighters not found in fighters table.")
        print("   Run: python -m scripts.load_ufc_data")
        sys.exit(1)

    return event


def print_event_header(event: dict) -> None:
    print(f"\n{separator('═')}")
    print(f"  EVENT: {event['name']}")
    print(separator("═"))
    print(f"  Date     : {event.get('date', 'TBD')}")
    print(f"  Location : {event.get('location', 'TBD')}")
    print(f"  Status   : {event.get('status', 'unknown')}")
    print(f"  ID       : {event['id']}")


def print_card(client, bouts: list[dict]) -> None:
    print(f"\n{separator('─')}")
    print(f"  CARD ({len(bouts)} bout(s))")
    print(separator("─"))
    if not bouts:
        print("  (no bouts on card)")
        return
    for bout in bouts:
        name_a = fetch_fighter_name(client, bout["fighter_a_id"]) if bout.get("fighter_a_id") else "TBD"
        name_b = fetch_fighter_name(client, bout["fighter_b_id"]) if bout.get("fighter_b_id") else "TBD"
        tags = []
        if bout.get("is_main_event"):
            tags.append("MAIN EVENT")
        if bout.get("is_title_fight"):
            tags.append("TITLE")
        tag_str = f"  [{', '.join(tags)}]" if tags else ""
        weight = bout.get("weight_class") or ""
        print(f"  • {name_a} vs {name_b}  {weight}{tag_str}")


def print_summary(summary: dict, fighter_count: int) -> None:
    cost = fighter_count * COST_PER_FIGHTER_USD
    print(f"\n{separator('═')}")
    print("  PIPELINE SUMMARY")
    print(separator("═"))
    print(f"  Event ID            : {summary['event_id']}")
    print(f"  Fighters processed  : {summary['fighters_processed']}")
    print(f"  Fighters failed     : {summary['fighters_failed']}")
    print(f"  Total signals stored: {summary['total_signals_stored']}")
    print(f"  Errors              : {len(summary['errors'])}")
    for err in summary["errors"]:
        print(f"    ✗ {err}")

    print(f"\n{separator('─')}")
    print("  COST ESTIMATE")
    print(separator("─"))
    print(f"  Fighters × $0.0018/fighter = {fighter_count} × $0.0018 = ${cost:.4f}")
    print(separator("═"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    client = get_supabase_client()

    # ------------------------------------------------------------------
    # 1. Find or create upcoming event
    # ------------------------------------------------------------------
    event = fetch_upcoming_event(client)
    if event is None:
        event = insert_placeholder_event(client)

    print_event_header(event)
    event_id = event["id"]

    # ------------------------------------------------------------------
    # 2. Fetch and print card
    # ------------------------------------------------------------------
    bouts = fetch_bouts_for_event(client, event_id)
    print_card(client, bouts)

    # Collect unique fighter count for cost estimate
    fighter_ids: set[str] = set()
    for bout in bouts:
        if bout.get("fighter_a_id"):
            fighter_ids.add(bout["fighter_a_id"])
        if bout.get("fighter_b_id"):
            fighter_ids.add(bout["fighter_b_id"])
    fighter_count = len(fighter_ids)

    if fighter_count == 0:
        print("\n⚠  No fighters on card — nothing to run.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 3. Confirm
    # ------------------------------------------------------------------
    estimated_cost = fighter_count * COST_PER_FIGHTER_USD
    print(f"\n  {fighter_count} unique fighter(s) to process.")
    print(f"  Estimated cost: ${estimated_cost:.4f} (~$0.0018/fighter)")
    print()
    try:
        input("  Press Enter to run signal pipeline for all fighters... (Ctrl-C to abort) ")
    except KeyboardInterrupt:
        print("\n  Aborted.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 4. Run pipeline
    # ------------------------------------------------------------------
    print(f"\n→ Running event pipeline for '{event['name']}'…")
    summary = run_event_pipeline(event_id)

    # ------------------------------------------------------------------
    # 5. Print results
    # ------------------------------------------------------------------
    print_summary(summary, fighter_count)


if __name__ == "__main__":
    run()
