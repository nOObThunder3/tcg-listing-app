# scripts/ingest_products.py
#
# Ingest ONLY single Pokémon cards into SQLite by requiring:
# - product.extendedData contains an item with name == "Number" and a non-empty value
#
# This treats "Number" (Card Number) as the authoritative discriminator for singles.
#
# Usage:
#   source .venv/bin/activate
#   python scripts/ingest_products.py

from __future__ import annotations

import re
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "tcg.sqlite"

BASE = "https://tcgcsv.com/tcgplayer"
CATEGORY_ID = 3  # Pokemon


# ----------------------------
# Helpers: time / HTTP
# ----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_results(url: str, session: requests.Session, retries: int = 3, timeout: int = 30) -> List[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            results = payload.get("results")
            if not isinstance(results, list):
                raise ValueError(f"Unexpected response shape (missing/invalid 'results'). Keys={list(payload.keys())}")
            return results
        except Exception as e:
            last_err = e
            time.sleep(0.75 * attempt)
    raise last_err  # type: ignore[misc]


# ----------------------------
# Helpers: normalization
# ----------------------------

def clean_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9\s\-'/]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _normalize_num_part(part: str) -> str:
    part = part.strip().upper()
    if not part:
        return part

    m = re.match(r"^([A-Z]+)(\d+)$", part)
    if m:
        prefix, digits = m.group(1), m.group(2)
        try:
            return f"{prefix}{int(digits)}"
        except ValueError:
            return part

    if part.isdigit():
        try:
            return str(int(part))
        except ValueError:
            return part

    return part


def normalize_collector_number(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().upper()
    s = re.sub(r"\s+", "", s)

    if "/" in s:
        left, right = s.split("/", 1)
        return f"{_normalize_num_part(left)}/{_normalize_num_part(right)}"

    return _normalize_num_part(s)


# ----------------------------
# Singles discriminator: Card Number
# ----------------------------

def extract_card_number(product: Dict[str, Any]) -> Optional[str]:
    """
    Return the extendedData 'Number' value (Card Number), if present.
    Example extendedData item:
      { "name": "Number", "displayName": "Card Number", "value": "001/102" }
    """
    ext_list = product.get("extendedData") or []
    if not isinstance(ext_list, list):
        return None

    for item in ext_list:
        if not isinstance(item, dict):
            continue
        if item.get("name") == "Number":
            val = item.get("value")
            if val is None:
                continue
            val_str = str(val).strip()
            if val_str:
                return val_str
    return None


# ----------------------------
# DB helpers
# ----------------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def ensure_cards_table_exists(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cards';"
    ).fetchone()
    if not row:
        raise RuntimeError(
            "Missing table 'cards'. Add the cards table DDL to src/core/schema.sql and re-run: python scripts/init_db.py"
        )


def fetch_all_group_ids(conn: sqlite3.Connection) -> List[int]:
    rows = conn.execute("SELECT group_id FROM sets ORDER BY published_on ASC").fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def upsert_cards(conn: sqlite3.Connection, rows: List[Tuple[Any, ...]]) -> None:
    """
    Expects a cards table with columns:
      product_id, group_id, product_name, clean_name,
      collector_number_raw, collector_number_norm,
      rarity, image_url, tcgplayer_url, product_type, updated_at

    If you removed product_type from your schema, tell me and I'll provide the adjusted SQL.
    """
    conn.executemany(
        """
        INSERT INTO cards (
          product_id, group_id, product_name, clean_name,
          collector_number_raw, collector_number_norm,
          rarity, image_url, tcgplayer_url, product_type, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
          group_id=excluded.group_id,
          product_name=excluded.product_name,
          clean_name=excluded.clean_name,
          collector_number_raw=excluded.collector_number_raw,
          collector_number_norm=excluded.collector_number_norm,
          rarity=excluded.rarity,
          image_url=excluded.image_url,
          tcgplayer_url=excluded.tcgplayer_url,
          product_type=excluded.product_type,
          updated_at=excluded.updated_at
        """,
        rows,
    )


# ----------------------------
# Main
# ----------------------------

def main():
    if not DB_PATH.exists():
        raise RuntimeError(f"Database not found at {DB_PATH}. Run: python scripts/init_db.py")

    updated_at = utc_now_iso()

    conn = db_connect()
    try:
        ensure_cards_table_exists(conn)
        group_ids = fetch_all_group_ids(conn)
        if not group_ids:
            raise RuntimeError("No sets found in 'sets' table. Run: python scripts/ingest_groups.py")

        print(f"Found {len(group_ids)} Pokémon sets in DB. Ingesting SINGLE cards only (requires Card Number)...")

        total_fetched = 0
        total_kept = 0

        with requests.Session() as session:
            for i, gid in enumerate(group_ids, start=1):
                url = f"{BASE}/{CATEGORY_ID}/{gid}/products"
                try:
                    products = get_results(url, session=session)
                except Exception as e:
                    print(f"[{i}/{len(group_ids)}] group_id={gid} ERROR fetching products: {e}")
                    continue

                fetched = len(products)
                total_fetched += fetched

                rows: List[Tuple[Any, ...]] = []

                for p in products:
                    if not isinstance(p, dict):
                        continue

                    card_number_raw = extract_card_number(p)
                    if not card_number_raw:
                        # Not a single (by your discriminator)
                        continue

                    product_id = p.get("productId")
                    product_name = p.get("name")
                    image_url = p.get("imageUrl")
                    tcgplayer_url = p.get("url")

                    card_number_norm = normalize_collector_number(card_number_raw)

                    # We can optionally pull rarity if it exists in extendedData; not required for filtering
                    rarity = None
                    ext_list = p.get("extendedData") or []
                    if isinstance(ext_list, list):
                        for item in ext_list:
                            if isinstance(item, dict) and item.get("name") == "Rarity":
                                val = item.get("value")
                                if val is not None and str(val).strip():
                                    rarity = str(val).strip()
                                    break

                    rows.append((
                        product_id,
                        gid,
                        product_name,
                        clean_name(product_name),
                        card_number_raw,
                        card_number_norm,
                        rarity,
                        image_url,
                        tcgplayer_url,
                        "single",
                        updated_at,
                    ))

                if rows:
                    upsert_cards(conn, rows)
                    conn.commit()

                kept = len(rows)
                total_kept += kept
                print(f"[{i}/{len(group_ids)}] group_id={gid}: fetched={fetched} kept_singles={kept}")

                time.sleep(0.15)

        print("Done.")
        print(f"Total fetched products: {total_fetched}")
        print(f"Total singles kept:     {total_kept}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
