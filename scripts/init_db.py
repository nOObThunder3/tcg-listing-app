from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "tcg.sqlite"
SCHEMA_PATH = ROOT / "src" / "core" / "schema.sql"

def main():
    (ROOT / "data").mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema)
        conn.commit()
        print(f"Initialized DB at: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
