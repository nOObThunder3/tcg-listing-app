#!/usr/bin/env python3
"""
One-time backfill of TCGCSV daily price archive into prices_history.

Per day:
- Downloads: https://tcgcsv.com/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z
- Extracts with system `7z` into tmp/tcgcsv_extract/YYYY-MM-DD/
- Reads Pokemon category (3): tmp/.../YYYY-MM-DD/3/*/prices
- Parses JSON shaped as either:
    A) JSON array: [ {...}, {...} ]
    B) JSON object: {"success": true, "results": [ {...}, {...} ]}
    C) line-delimited JSON dicts
- Keeps only records with non-null marketPrice
- Filters to your singles universe by requiring productId to exist in `cards`
- Inserts into prices_history with INSERT OR IGNORE (idempotent)
- Optionally skips a day if prices_history already has any rows for that snapshot_date
- Deletes extracted day folder unless --keep-extracted

Requirements:
- `7z` installed on your system (macOS: `brew install p7zip`)
- `requests` installed in your venv
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, List, Tuple

import requests

ARCHIVE_URL_TMPL = "https://tcgcsv.com/archive/tcgplayer/prices-{d}.ppmd.7z"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def daterange_inclusive(start: date, end: date) -> Iterator[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def download_file(url: str, out_path: Path, timeout: int = 120) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp_path.replace(out_path)


def prices_file_records(prices_path: Path) -> Iterator[dict]:
    """
    Handles:
    - JSON array: [ {...}, {...} ]
    - JSON object: {"success": true, "results": [ {...}, {...} ]}
    - line-delimited JSON dicts
    """
    raw = prices_path.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        return

    # Try full JSON parse first (covers both array and object shapes)
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield item
            return
        if isinstance(obj, dict):
            results = obj.get("results")
            if isinstance(results, list):
                for item in results:
                    if isinstance(item, dict):
                        yield item
                return
            # if dict but not recognized, fall through to line parsing
    except json.JSONDecodeError:
        pass

    # Fallback: line-delimited JSON dicts
    for line in raw.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                # if it's the wrapper dict, unwrap results
                if "results" in obj and isinstance(obj["results"], list):
                    for item in obj["results"]:
                        if isinstance(item, dict):
                            yield item
                else:
                    yield obj
        except json.JSONDecodeError:
            continue


def day_already_loaded(con: sqlite3.Connection, snapshot_date: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM prices_history WHERE snapshot_date = ? LIMIT 1;",
        (snapshot_date,),
    ).fetchone()
    return row is not None


def load_cards_product_ids(con: sqlite3.Connection) -> set[int]:
    ids: set[int] = set()
    for (pid,) in con.execute("SELECT DISTINCT product_id FROM cards;"):
        if pid is not None:
            ids.add(int(pid))
    return ids


def insert_history_rows(
    con: sqlite3.Connection,
    rows: List[Tuple[int, str, float, str, str]],
) -> None:
    con.executemany(
        """
        INSERT OR IGNORE INTO prices_history
          (product_id, sub_type, market_price, snapshot_date, captured_at)
        VALUES (?, ?, ?, ?, ?);
        """,
        rows,
    )


def extract_archive(archive_path: Path, day_extract_dir: Path, verbose: bool) -> None:
    """
    Extract archive into day_extract_dir using 7z with cwd set.
    Use absolute archive path to avoid cwd-related path issues.
    """
    if day_extract_dir.exists():
        shutil.rmtree(day_extract_dir)
    ensure_dir(day_extract_dir)

    archive_abs = archive_path.resolve()

    if verbose:
        print(f"extracting -> {day_extract_dir}")

    subprocess.run(
        ["7z", "x", str(archive_abs)],
        check=True,
        cwd=str(day_extract_dir),
    )


def backfill_day(
    *,
    con: sqlite3.Connection,
    product_ids: set[int],
    d: date,
    cache_dir: Path,
    extract_root: Path,
    keep_extracted: bool,
    verbose: bool,
) -> Tuple[int, int]:
    """
    Returns: (rows_with_marketPrice_parsed, rows_kept_after_filter_to_cards)
    """
    d_str = d.isoformat()
    archive_url = ARCHIVE_URL_TMPL.format(d=d_str)
    archive_path = cache_dir / f"prices-{d_str}.ppmd.7z"

    if not archive_path.exists():
        if verbose:
            print(f"[{d_str}] downloading -> {archive_path}")
        download_file(archive_url, archive_path)

    # Extract under tmp/tcgcsv_extract/<date>/
    day_extract_dir = extract_root / d_str
    if verbose:
        print(f"[{d_str}] ", end="")
    extract_archive(archive_path, day_extract_dir, verbose=False)

    # Expected extracted structure:
    #   <day_extract_dir>/<YYYY-MM-DD>/3/<groupId>/prices
    root = day_extract_dir / d_str / "3"
    if not root.exists():
        if not keep_extracted:
            shutil.rmtree(day_extract_dir, ignore_errors=True)
        return (0, 0)

    prices_files = list(root.glob("*/prices"))
    if not prices_files:
        if not keep_extracted:
            shutil.rmtree(day_extract_dir, ignore_errors=True)
        return (0, 0)

    captured_at = utc_now_iso()
    snapshot_date = d_str

    parsed_with_mp = 0
    kept = 0
    batch: List[Tuple[int, str, float, str, str]] = []

    for pf in prices_files:
        for rec in prices_file_records(pf):
            mp = rec.get("marketPrice")
            if mp is None:
                continue

            try:
                pid = int(rec.get("productId"))
            except Exception:
                continue

            st = rec.get("subTypeName") or "Unknown"

            try:
                mp_f = float(mp)
            except Exception:
                continue

            parsed_with_mp += 1

            if pid not in product_ids:
                continue

            kept += 1
            batch.append((pid, st, mp_f, snapshot_date, captured_at))

    with con:
        insert_history_rows(con, batch)

    if not keep_extracted:
        shutil.rmtree(day_extract_dir, ignore_errors=True)

    return (parsed_with_mp, kept)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/tcg.sqlite", help="Path to SQLite DB")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--cache-dir", default="cache/tcgcsv_archive", help="Where to cache .7z archives")
    ap.add_argument("--extract-dir", default="tmp/tcgcsv_extract", help="Where to extract archives")
    ap.add_argument("--skip-existing", action="store_true", help="Skip a day if any history exists for that snapshot_date")
    ap.add_argument("--keep-extracted", action="store_true", help="Keep extracted folders (debug only)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    cache_dir = Path(args.cache_dir)
    extract_dir = Path(args.extract_dir)

    ensure_dir(cache_dir)
    ensure_dir(extract_dir)

    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    product_ids = load_cards_product_ids(con)
    if args.verbose:
        print(f"Loaded {len(product_ids):,} product_ids from cards (singles universe)")

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)

    for d in daterange_inclusive(start, end):
        d_str = d.isoformat()

        if args.skip_existing and day_already_loaded(con, d_str):
            if args.verbose:
                print(f"[{d_str}] skip (already loaded)")
            continue

        try:
            parsed_mp, kept = backfill_day(
                con=con,
                product_ids=product_ids,
                d=d,
                cache_dir=cache_dir,
                extract_root=extract_dir,
                keep_extracted=args.keep_extracted,
                verbose=args.verbose,
            )
            print(f"[{d_str}] parsed_with_marketPrice={parsed_mp:,} kept_in_cards={kept:,}")
        except requests.HTTPError as e:
            print(f"[{d_str}] download failed / archive unavailable: {e}")
            continue
        except FileNotFoundError as e:
            raise RuntimeError("Missing `7z` executable. Install with: brew install p7zip") from e
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"7z extraction failed for {d_str}: {e}") from e

    con.close()
    print("Backfill complete.")


if __name__ == "__main__":
    main()
