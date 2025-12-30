from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "tcg.sqlite"

BASE = "https://tcgcsv.com/tcgplayer"
CATEGORY_ID = 3  # Pokemon


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_today_str() -> str:
    return date.today().isoformat() if datetime.now(timezone.utc).date() == date.today() else datetime.now(timezone.utc).date().isoformat()


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


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def ensure_tables_exist(conn: sqlite3.Connection) -> None:
    required = {"sets", "cards", "prices_latest", "prices_history", "trends_latest", "run_log"}
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    existing = {r[0] for r in rows}
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            f"Missing tables: {missing}. Ensure schema.sql includes pricing/trends/run_log and re-run: python scripts/init_db.py"
        )


def get_group_ids_in_cards(conn: sqlite3.Connection, only_group_id: Optional[int] = None) -> List[int]:
    if only_group_id is not None:
        return [only_group_id]
    rows = conn.execute("SELECT DISTINCT group_id FROM cards ORDER BY group_id;").fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def get_product_ids_for_group(conn: sqlite3.Connection, group_id: int) -> Set[int]:
    rows = conn.execute("SELECT product_id FROM cards WHERE group_id = ?;", (group_id,)).fetchall()
    out: Set[int] = set()
    for r in rows:
        if r and r[0] is not None:
            out.add(int(r[0]))
    return out


def insert_run_log_start(conn: sqlite3.Connection, snapshot_date: str) -> int:
    started_at = utc_now_iso()
    cur = conn.execute(
        """
        INSERT INTO run_log (job_name, snapshot_date, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        ("refresh_prices_daily", snapshot_date, started_at, "running"),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_run_log_finish(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    notes: Optional[str],
    groups_count: int,
    price_rows_fetched: int,
    price_rows_kept: int,
    latest_upserts: int,
    history_inserts: int,
    trends_upserts: int,
) -> None:
    finished_at = utc_now_iso()
    conn.execute(
        """
        UPDATE run_log
        SET finished_at = ?,
            status = ?,
            groups_count = ?,
            price_rows_fetched = ?,
            price_rows_kept = ?,
            latest_upserts = ?,
            history_inserts = ?,
            trends_upserts = ?,
            notes = ?
        WHERE run_id = ?
        """,
        (
            finished_at,
            status,
            groups_count,
            price_rows_fetched,
            price_rows_kept,
            latest_upserts,
            history_inserts,
            trends_upserts,
            notes,
            run_id,
        ),
    )
    conn.commit()


def upsert_prices_latest(conn: sqlite3.Connection, rows: List[Tuple[int, str, Optional[float], str]]) -> int:
    """
    rows: (product_id, sub_type, market_price, updated_at)
    """
    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO prices_latest (product_id, sub_type, market_price, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_id, sub_type) DO UPDATE SET
          market_price = excluded.market_price,
          updated_at = excluded.updated_at
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def insert_prices_history(conn: sqlite3.Connection, rows: List[Tuple[int, str, str, Optional[float], str]]) -> int:
    """
    rows: (product_id, sub_type, snapshot_date, market_price, captured_at)
    Idempotent per day via PK(product_id, sub_type, snapshot_date)
    """
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO prices_history (product_id, sub_type, snapshot_date, market_price, captured_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def compute_and_upsert_trends(conn: sqlite3.Connection, snapshot_date: str) -> int:
    """
    For each series in today's snapshot_date, compute:
      pct_change_7d  vs latest snapshot on or before (snapshot_date - 7d)
      pct_change_30d vs latest snapshot on or before (snapshot_date - 30d)
    and upsert into trends_latest.
    """
    computed_at = utc_now_iso()

    # Pull today's series + lookback prices via correlated subqueries (single query)
    rows = conn.execute(
        """
        SELECT
          h.product_id,
          h.sub_type,
          h.market_price AS p0,
          (
            SELECT h7.market_price
            FROM prices_history h7
            WHERE h7.product_id = h.product_id
              AND h7.sub_type = h.sub_type
              AND h7.snapshot_date <= date(h.snapshot_date, '-7 day')
            ORDER BY h7.snapshot_date DESC
            LIMIT 1
          ) AS p7,
          (
            SELECT h30.market_price
            FROM prices_history h30
            WHERE h30.product_id = h.product_id
              AND h30.sub_type = h.sub_type
              AND h30.snapshot_date <= date(h.snapshot_date, '-30 day')
            ORDER BY h30.snapshot_date DESC
            LIMIT 1
          ) AS p30
        FROM prices_history h
        WHERE h.snapshot_date = ?
        """,
        (snapshot_date,),
    ).fetchall()

    upsert_rows: List[Tuple[int, str, str, Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], str]] = []

    for product_id, sub_type, p0, p7, p30 in rows:
        pct7 = None
        pct30 = None

        # Guard against null/zero
        if p0 is not None and p7 is not None and float(p7) != 0.0:
            pct7 = (float(p0) - float(p7)) / float(p7)
        if p0 is not None and p30 is not None and float(p30) != 0.0:
            pct30 = (float(p0) - float(p30)) / float(p30)

        upsert_rows.append(
            (
                int(product_id),
                str(sub_type),
                snapshot_date,
                float(p0) if p0 is not None else None,
                float(p7) if p7 is not None else None,
                float(p30) if p30 is not None else None,
                pct7,
                pct30,
                computed_at,
            )
        )

    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO trends_latest (
          product_id, sub_type, snapshot_date,
          market_price, market_price_7d, market_price_30d,
          pct_change_7d, pct_change_30d,
          computed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id, sub_type) DO UPDATE SET
          snapshot_date = excluded.snapshot_date,
          market_price = excluded.market_price,
          market_price_7d = excluded.market_price_7d,
          market_price_30d = excluded.market_price_30d,
          pct_change_7d = excluded.pct_change_7d,
          pct_change_30d = excluded.pct_change_30d,
          computed_at = excluded.computed_at
        """,
        upsert_rows,
    )
    conn.commit()
    return conn.total_changes - before


def main():
    parser = argparse.ArgumentParser(description="Daily marketPrice refresh + trends (variant-aware).")
    parser.add_argument("--snapshot-date", help="UTC snapshot date YYYY-MM-DD (default: today UTC).")
    parser.add_argument("--only-group-id", type=int, help="Limit run to a single group_id for testing.")
    parser.add_argument("--throttle-seconds", type=float, default=0.15, help="Sleep between groups (default: 0.15).")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise RuntimeError(f"Database not found at {DB_PATH}. Run: python scripts/init_db.py")

    snapshot_date = args.snapshot_date or datetime.now(timezone.utc).date().isoformat()
    captured_at = utc_now_iso()

    conn = db_connect()
    try:
        ensure_tables_exist(conn)

        run_id = insert_run_log_start(conn, snapshot_date)

        group_ids = get_group_ids_in_cards(conn, only_group_id=args.only_group_id)
        if not group_ids:
            update_run_log_finish(
                conn,
                run_id,
                "failed",
                "No group_ids found in cards table. Ingest cards first.",
                0, 0, 0, 0, 0, 0
            )
            raise RuntimeError("No group_ids found in cards table. Ingest cards first.")

        total_fetched = 0
        total_kept = 0
        total_latest_upserts = 0
        total_history_inserts = 0

        with requests.Session() as session:
            for idx, gid in enumerate(group_ids, start=1):
                valid_ids = get_product_ids_for_group(conn, gid)
                if not valid_ids:
                    print(f"[{idx}/{len(group_ids)}] group_id={gid}: no cards in DB, skipping.")
                    continue

                url = f"{BASE}/{CATEGORY_ID}/{gid}/prices"

                try:
                    price_rows = get_results(url, session=session)
                except Exception as e:
                    print(f"[{idx}/{len(group_ids)}] group_id={gid}: ERROR fetching prices: {e}")
                    continue

                fetched = len(price_rows)
                total_fetched += fetched

                latest_rows: List[Tuple[int, str, Optional[float], str]] = []
                hist_rows: List[Tuple[int, str, str, Optional[float], str]] = []

                kept = 0
                for pr in price_rows:
                    if not isinstance(pr, dict):
                        continue

                    pid = pr.get("productId")
                    if pid is None:
                        continue
                    try:
                        pid_int = int(pid)
                    except Exception:
                        continue

                    if pid_int not in valid_ids:
                        continue

                    sub_type = pr.get("subTypeName") or "Unknown"
                    mp = pr.get("marketPrice", None)
                    try:
                        mp_val = float(mp) if mp is not None else None
                    except Exception:
                        mp_val = None

                    # We still store rows even if market_price is None; your choice:
                    # For cleaner analytics, you may prefer to skip None values.
                    if mp_val is None:
                        continue

                    kept += 1
                    latest_rows.append((pid_int, str(sub_type), mp_val, captured_at))
                    hist_rows.append((pid_int, str(sub_type), snapshot_date, mp_val, captured_at))

                total_kept += kept

                # write latest + history
                if latest_rows:
                    total_latest_upserts += upsert_prices_latest(conn, latest_rows)
                if hist_rows:
                    total_history_inserts += insert_prices_history(conn, hist_rows)

                print(f"[{idx}/{len(group_ids)}] group_id={gid}: fetched={fetched} kept(with marketPrice)={kept}")

                time.sleep(max(0.0, float(args.throttle_seconds)))

        # trends step (uses prices_history for snapshot_date)
        trends_upserts = compute_and_upsert_trends(conn, snapshot_date)

        update_run_log_finish(
            conn,
            run_id,
            "success",
            None,
            len(group_ids),
            total_fetched,
            total_kept,
            total_latest_upserts,
            total_history_inserts,
            trends_upserts,
        )

        print("Done.")
        print(f"snapshot_date (UTC): {snapshot_date}")
        print(f"price_rows_fetched:  {total_fetched}")
        print(f"price_rows_kept:     {total_kept}")
        print(f"latest_upserts:      {total_latest_upserts}")
        print(f"history_inserts:     {total_history_inserts}")
        print(f"trends_upserts:      {trends_upserts}")

    except Exception as e:
        # best-effort: mark run failed if we already created it
        try:
            conn.execute(
                "UPDATE run_log SET status='failed', finished_at=?, notes=? WHERE status='running' AND job_name=? AND snapshot_date=?;",
                (utc_now_iso(), str(e)[:500], "refresh_prices_daily", snapshot_date),
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
