from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path("data/tcg.sqlite")

SLASH_RE = re.compile(r"^\s*(\d{1,4})\s*/\s*(\d{1,4})\s*$")
PROMO_ALPHA_RE = re.compile(r"^\s*([A-Z]{2,6})\s*-?\s*(\d{1,4})\s*$", re.I)

def norm_ext_number(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return raw

    m = SLASH_RE.match(raw)
    if m:
        a, b = m.group(1), m.group(2)
        return f"{int(a)}/{int(b)}"

    # Normalize things like "SVP 123" -> "SVP123", "SM05" -> "SM05"
    m2 = PROMO_ALPHA_RE.match(raw.replace(" ", ""))
    if m2:
        prefix = m2.group(1).upper()
        num = int(m2.group(2))
        # Preserve leading zeros for 2-digit promo ids like SM05 by formatting based on original length
        # If original ends with leading zeros, keep width up to 3; otherwise minimal.
        tail = re.sub(r"\D", "", raw)
        width = max(2, min(4, len(tail)))  # heuristic
        return f"{prefix}{num:0{width}d}" if tail.startswith("0") else f"{prefix}{num}"

    # Pure numeric (e.g., WOTC Black Star Promo numbers)
    digits = re.sub(r"\D", "", raw)
    if digits and digits == raw:
        return str(int(digits))

    # Fallback: uppercase + strip spaces/hyphens
    return re.sub(r"[\s\-]", "", raw).upper()

def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure columns exist
    cols = {r["name"] for r in cur.execute("PRAGMA table_info(cards);").fetchall()}
    if "ext_number_raw" not in cols:
        cur.execute("ALTER TABLE cards ADD COLUMN ext_number_raw TEXT;")
    if "ext_number_norm" not in cols:
        cur.execute("ALTER TABLE cards ADD COLUMN ext_number_norm TEXT;")

    rows = cur.execute("""
        SELECT product_id, collector_number_raw
        FROM cards
        WHERE (ext_number_raw IS NULL OR ext_number_norm IS NULL)
          AND collector_number_raw IS NOT NULL
          AND TRIM(collector_number_raw) <> ''
    """).fetchall()

    updated = 0
    for r in rows:
        pid = r["product_id"]
        raw = r["collector_number_raw"]
        ext_raw = raw.strip()
        ext_norm = norm_ext_number(ext_raw)

        cur.execute("""
            UPDATE cards
            SET ext_number_raw = COALESCE(ext_number_raw, ?),
                ext_number_norm = COALESCE(ext_number_norm, ?)
            WHERE product_id = ?
        """, (ext_raw, ext_norm, pid))
        updated += 1

    conn.commit()
    conn.close()
    print(f"Backfilled ext_number_* for {updated} cards.")

if __name__ == "__main__":
    main()
