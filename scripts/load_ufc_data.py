"""
Reads UFC-DataLab CSVs and upserts fighters, events, and bouts into Supabase.

Sources
-------
UFC-DataLab/data/external_data/raw_fighter_details.csv  — fighter roster
UFC-DataLab/data/stats/stats_raw.csv                    — bout + event data

Usage
-----
    python -m scripts.load_ufc_data          # full load
    python -m scripts.load_ufc_data --limit 50
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

from db.client import get_supabase_client

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_ROOT = Path(__file__).parent.parent / "UFC-DataLab" / "data"
FIGHTERS_CSV = DATA_ROOT / "external_data" / "raw_fighter_details.csv"
STATS_CSV = DATA_ROOT / "stats" / "stats_raw.csv"

# ---------------------------------------------------------------------------
# Weight-class extraction
# ---------------------------------------------------------------------------
_WEIGHT_CLASSES = [
    "Women's Strawweight",
    "Women's Flyweight",
    "Women's Bantamweight",
    "Women's Featherweight",
    "Super Heavyweight",
    "Open Weight",
    "Catch Weight",
    "Strawweight",
    "Flyweight",
    "Bantamweight",
    "Featherweight",
    "Lightweight",
    "Welterweight",
    "Middleweight",
    "Light Heavyweight",
    "Heavyweight",
]

_WC_PATTERN = re.compile(
    "(" + "|".join(re.escape(wc) for wc in _WEIGHT_CLASSES) + ")",
    re.IGNORECASE,
)


def _extract_weight_class(bout_type: str) -> str | None:
    m = _WC_PATTERN.search(str(bout_type))
    return m.group(1).title() if m else None


def _weight_to_class(weight_str: str) -> str | None:
    """Map raw weight string (e.g. '155 lbs.') to a division name."""
    mapping = {
        115: "Strawweight",
        125: "Flyweight",
        135: "Bantamweight",
        145: "Featherweight",
        155: "Lightweight",
        170: "Welterweight",
        185: "Middleweight",
        205: "Light Heavyweight",
        265: "Heavyweight",
    }
    m = re.search(r"(\d+)", str(weight_str))
    if not m:
        return None
    lbs = int(m.group(1))
    return mapping.get(lbs)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _fetch_all_name_id(client, table: str, page_size: int = 1000) -> dict[str, str]:
    """Fetch the full {name.title(): id} map from a table, paginating past the 1000-row default."""
    result: dict[str, str] = {}
    offset = 0
    while True:
        resp = (
            client.table(table)
            .select("id,name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        for r in resp.data:
            result[r["name"].title()] = r["id"]
        if len(resp.data) < page_size:
            break
        offset += page_size
    return result


def _compute_records(df_stats: pd.DataFrame) -> dict[str, dict[str, int]]:
    """Compute win/loss/draw counts per fighter name (title-cased) from bout results."""
    records: dict[str, dict[str, int]] = {}

    for _, row in df_stats.iterrows():
        red = str(row.get("red_fighter_name", "")).strip().title()
        blue = str(row.get("blue_fighter_name", "")).strip().title()
        result = str(row.get("red_fighter_result", "")).strip().upper()

        for name in (red, blue):
            if not name or name == "Nan":
                continue
            if name not in records:
                records[name] = {"record_wins": 0, "record_losses": 0, "record_draws": 0}

        if not red or red == "Nan" or not blue or blue == "Nan":
            continue

        if result == "W":
            records[red]["record_wins"] += 1
            records[blue]["record_losses"] += 1
        elif result == "L":
            records[blue]["record_wins"] += 1
            records[red]["record_losses"] += 1
        elif result in ("D", "DRAW", "NC"):
            records[red]["record_draws"] += 1
            records[blue]["record_draws"] += 1

    return records


def _load_fighters(
    client,
    df_fighters: pd.DataFrame,
    nickname_map: dict[str, str],
    record_map: dict[str, dict[str, int]],
) -> dict[str, str]:
    """Upsert fighters; return {name: id} map."""
    records = []
    for _, row in df_fighters.iterrows():
        name = str(row["fighter_name"]).strip()
        if not name or name == "nan":
            continue
        fighter_record = record_map.get(name, {"record_wins": 0, "record_losses": 0, "record_draws": 0})
        records.append(
            {
                "name": name,
                "nickname": nickname_map.get(name),
                "weight_class": _weight_to_class(row.get("Weight", "")),
                "record_wins": fighter_record["record_wins"],
                "record_losses": fighter_record["record_losses"],
                "record_draws": fighter_record["record_draws"],
            }
        )

    # Batch upsert in chunks of 500
    for i in range(0, len(records), 500):
        client.table("fighters").upsert(
            records[i : i + 500], on_conflict="name"
        ).execute()

    # Fetch back name→id map (paginate — PostgREST caps at 1000 rows per request)
    return _fetch_all_name_id(client, "fighters")


def _load_events(client, df_stats: pd.DataFrame) -> dict[tuple[str, str], str]:
    """Upsert unique events; return {(name, date): id} map."""
    events_df = (
        df_stats[["event_name", "event_date", "event_location"]]
        .drop_duplicates(subset=["event_name", "event_date"])
        .copy()
    )

    records = []
    for _, row in events_df.iterrows():
        name = str(row["event_name"]).strip()
        raw_date = str(row["event_date"]).strip()
        try:
            iso_date = pd.to_datetime(raw_date, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            iso_date = None

        records.append(
            {
                "name": name,
                "date": iso_date,
                "location": str(row["event_location"]).strip() or None,
                "status": "completed",
            }
        )

    for i in range(0, len(records), 500):
        client.table("events").upsert(
            records[i : i + 500], on_conflict="name,date"
        ).execute()

    result: dict[tuple[str, str], str] = {}
    offset = 0
    page_size = 1000
    while True:
        resp = (
            client.table("events")
            .select("id,name,date")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        for r in resp.data:
            result[(r["name"], r["date"])] = r["id"]
        if len(resp.data) < page_size:
            break
        offset += page_size
    return result


def _load_bouts(
    client,
    df_stats: pd.DataFrame,
    fighter_id_map: dict[str, str],
    event_id_map: dict[tuple[str, str], str],
) -> None:
    """Upsert bouts."""
    records = []
    for _, row in df_stats.iterrows():
        fighter_a = str(row["red_fighter_name"]).strip().title()
        fighter_b = str(row["blue_fighter_name"]).strip().title()
        fighter_a_id = fighter_id_map.get(fighter_a)
        fighter_b_id = fighter_id_map.get(fighter_b)

        if not fighter_a_id or not fighter_b_id:
            continue  # skip bouts where either fighter wasn't loaded

        raw_date = str(row["event_date"]).strip()
        try:
            iso_date = pd.to_datetime(raw_date, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            iso_date = None

        event_id = event_id_map.get((str(row["event_name"]).strip(), iso_date))
        if not event_id:
            continue

        bout_type = str(row.get("bout_type", ""))
        red_result = str(row.get("red_fighter_result", "")).strip().upper()
        result = fighter_a if red_result == "W" else (fighter_b if red_result == "L" else None)  # already title-cased

        round_val = row.get("round")
        try:
            round_int = int(float(round_val)) if pd.notna(round_val) else None
        except (ValueError, TypeError):
            round_int = None

        records.append(
            {
                "event_id": event_id,
                "fighter_a_id": fighter_a_id,
                "fighter_b_id": fighter_b_id,
                "weight_class": _extract_weight_class(bout_type),
                "is_main_event": False,
                "is_title_fight": "title" in bout_type.lower(),
                "result": result,
                "method": str(row.get("method", "")).strip() or None,
                "round": round_int,
                "time": str(row.get("time", "")).strip() or None,
            }
        )

    for i in range(0, len(records), 500):
        client.table("bouts").upsert(
            records[i : i + 500],
            on_conflict="event_id,fighter_a_id,fighter_b_id",
        ).execute()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load_all(limit: int | None = None) -> None:
    """
    Load UFC data from CSVs into Supabase.

    Parameters
    ----------
    limit : int | None
        If set, restricts the number of fighter rows and stat rows processed.
        Useful for smoke-testing without a full load.
    """
    client = get_supabase_client()

    df_fighters = pd.read_csv(FIGHTERS_CSV)
    df_stats = pd.read_csv(STATS_CSV, sep=";")

    if limit is not None:
        df_fighters = df_fighters.head(limit)
        df_stats = df_stats.head(limit)

    # Build nickname lookup from stats (red + blue columns)
    nickname_map: dict[str, str] = {}
    for _, row in df_stats.iterrows():
        for name_col, nick_col in [
            ("red_fighter_name", "red_fighter_nickname"),
            ("blue_fighter_name", "blue_fighter_nickname"),
        ]:
            name = str(row.get(name_col, "")).strip().title()
            nick = str(row.get(nick_col, "")).strip()
            if name and nick and nick != "nan":
                nickname_map[name] = nick

    record_map = _compute_records(df_stats)
    fighter_id_map = _load_fighters(client, df_fighters, nickname_map, record_map)

    # Seed any fighters that appear in bouts but are absent from raw_fighter_details
    known_names = set(fighter_id_map.keys())
    extra_records = []
    seen: set[str] = set()
    for _, row in df_stats.iterrows():
        for name_col, nick_col, wc_col in [
            ("red_fighter_name", "red_fighter_nickname", "bout_type"),
            ("blue_fighter_name", "blue_fighter_nickname", "bout_type"),
        ]:
            name = str(row.get(name_col, "")).strip().title()
            if not name or name in known_names or name in seen:
                continue
            seen.add(name)
            nick = str(row.get(nick_col, "")).strip()
            fighter_record = record_map.get(name, {"record_wins": 0, "record_losses": 0, "record_draws": 0})
            extra_records.append(
                {
                    "name": name,
                    "nickname": nick if nick and nick != "nan" else None,
                    "weight_class": _extract_weight_class(str(row.get(wc_col, ""))),
                    "record_wins": fighter_record["record_wins"],
                    "record_losses": fighter_record["record_losses"],
                    "record_draws": fighter_record["record_draws"],
                }
            )

    for i in range(0, len(extra_records), 500):
        client.table("fighters").upsert(
            extra_records[i : i + 500], on_conflict="name"
        ).execute()

    # Refresh map to include newly seeded fighters (paginated)
    fighter_id_map = _fetch_all_name_id(client, "fighters")

    event_id_map = _load_events(client, df_stats)
    _load_bouts(client, df_stats, fighter_id_map, event_id_map)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load UFC DataLab CSVs into Supabase")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows (for testing)")
    args = parser.parse_args()
    load_all(limit=args.limit)
    print("Done.")
