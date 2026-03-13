"""
One-off script: insert all UFC Fight Night: Emmett vs Vallejos bouts into the bouts table.

Looks up each fighter by name (case-insensitive ilike).
Inserts a placeholder fighters row for any fighter not found.
Confirms all bouts with a final SELECT.

Run:
    python -m scripts.insert_fn_emmett_vallejos_bouts
"""
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

from db.client import get_supabase_client

EVENT_ID = "3b3cc4f2-ac9f-4cf5-a6a9-a0b5d42a52bd"

# (fighter_a, fighter_b, weight_class, is_main_event, is_title_fight)
BOUTS = [
    # Main card
    ("Josh Emmett",          "Kevin Vallejos",     "featherweight", True,  False),
    ("Amanda Lemos",         "Gillian Robertson",  "strawweight",   False, False),
    ("Ion Cutelaba",         "Oumar Sy",           "light_heavyweight", False, False),
    ("Andre Fili",           "Jose Miguel Delgado","featherweight", False, False),
    ("Marwan Rahiki",        "Harry Hardwick",     "featherweight", False, False),
    ("Vitor Petrino",        "Steven Asplund",     "heavyweight",   False, False),
    # Prelims
    ("Charles Johnson",      "Bruno Silva",        "flyweight",     False, False),
    ("Brad Tavares",         "Eryk Anders",        "middleweight",  False, False),
    ("Chris Curtis",         "Mateusz Orolbai",    "welterweight",  False, False),
]


def lookup_or_create(client, name: str, missing_log: list[str]) -> str:
    resp = client.table("fighters").select("id, name").ilike("name", name).limit(1).execute()
    if resp.data:
        return resp.data[0]["id"]

    # Not found — insert placeholder
    missing_log.append(name)
    ins = client.table("fighters").insert({"name": name}).execute()
    return ins.data[0]["id"]


def run() -> None:
    client = get_supabase_client()
    missing: list[str] = []
    inserted: list[dict] = []

    print(f"Event ID : {EVENT_ID}")
    print(f"Bouts    : {len(BOUTS)}\n")

    for fighter_a_name, fighter_b_name, weight_class, is_main, is_title in BOUTS:
        id_a = lookup_or_create(client, fighter_a_name, missing)
        id_b = lookup_or_create(client, fighter_b_name, missing)

        resp = client.table("bouts").insert({
            "event_id":       EVENT_ID,
            "fighter_a_id":   id_a,
            "fighter_b_id":   id_b,
            "weight_class":   weight_class,
            "is_main_event":  is_main,
            "is_title_fight": is_title,
        }).execute()

        row = resp.data[0]
        inserted.append(row)
        tag = ""
        if is_main:  tag += " [MAIN EVENT]"
        if is_title: tag += " [TITLE]"
        print(f"  ✓ {fighter_a_name} vs {fighter_b_name}  ({weight_class}){tag}")

    # ------------------------------------------------------------------
    # Report placeholders
    # ------------------------------------------------------------------
    if missing:
        print(f"\n⚠  {len(missing)} fighter(s) NOT found in fighters table — placeholder rows inserted:")
        for name in missing:
            print(f"     • {name}")
    else:
        print("\n✓  All fighters found in fighters table — no placeholders needed.")

    # ------------------------------------------------------------------
    # Confirm with SELECT
    # ------------------------------------------------------------------
    confirm = (
        client.table("bouts")
        .select(
            "id, fighter_a_id, fighter_b_id, weight_class, is_main_event, is_title_fight"
        )
        .eq("event_id", EVENT_ID)
        .execute()
    )
    rows = confirm.data or []
    print(f"\n─── SELECT confirmation ───────────────────────────────────────────")
    print(f"  Bouts found in DB for this event: {len(rows)}")

    # Resolve names for display
    all_fighter_ids = set()
    for r in rows:
        if r.get("fighter_a_id"): all_fighter_ids.add(r["fighter_a_id"])
        if r.get("fighter_b_id"): all_fighter_ids.add(r["fighter_b_id"])

    name_map: dict[str, str] = {}
    for fid in all_fighter_ids:
        nr = client.table("fighters").select("name").eq("id", fid).limit(1).execute()
        if nr.data:
            name_map[fid] = nr.data[0]["name"]

    for r in rows:
        a = name_map.get(r["fighter_a_id"], r["fighter_a_id"][:8])
        b = name_map.get(r["fighter_b_id"], r["fighter_b_id"][:8])
        tag = ""
        if r["is_main_event"]:  tag += " [MAIN]"
        if r["is_title_fight"]: tag += " [TITLE]"
        print(f"  • {a} vs {b}  ({r['weight_class']}){tag}")

    print(f"───────────────────────────────────────────────────────────────────")
    expected = len(BOUTS)
    actual   = len(rows)
    if actual == expected:
        print(f"  ✓ {actual}/{expected} bouts confirmed in database.")
    else:
        print(f"  ✗ Only {actual}/{expected} bouts found — check for errors above.")


if __name__ == "__main__":
    run()
