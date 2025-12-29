from pathlib import Path
import sqlite3
from datetime import datetime, timezone
import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "tcg.sqlite"

CATEGORY_ID = 3  # Pokemon
GROUPS_URL = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/groups"

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def main():
    resp = requests.get(GROUPS_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Unexpected response shape. Keys={list(payload.keys())}")

    updated_at = utc_now_iso()

    rows = []
    for g in results:
        rows.append((
            g.get("groupId"),
            g.get("name"),
            g.get("abbreviation"),
            g.get("publishedOn"),
            updated_at,
        ))

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany(
            """
            INSERT INTO sets (group_id, name, abbreviation, published_on, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(group_id) DO UPDATE SET
              name=excluded.name,
              abbreviation=excluded.abbreviation,
              published_on=excluded.published_on,
              updated_at=excluded.updated_at
            """,
            rows
        )
        conn.commit()
        print(f"Upserted {len(rows)} sets into sets table.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
