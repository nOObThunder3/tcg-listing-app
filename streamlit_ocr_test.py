from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import streamlit as st

DB_PATH = Path("data/tcg.sqlite")

# -----------------------------
# Regex helpers
# -----------------------------

# Standard set number like 059/131
SLASH_RE = re.compile(r"\b(\d{1,4}\s*/\s*\d{1,4})\b")

# Promo-like identifiers: SM05, SM125, SWSH125, SVP 123, XY123, BW123, etc.
PROMO_RE = re.compile(r"\b(SVP|SWSH|SM|XY|BW|DP|HGSS|POP)\s*-?\s*(\d{1,4})\b", re.I)

# WOTC-ish: word PROMO then a small integer somewhere shortly after (e.g., PROMO ... 28)
WOTC_PROMO_NUM_RE = re.compile(r"\bPROMO\b[\s\S]{0,140}?\b(\d{1,3})\b", re.I)

# Tokens to ignore when trying to pull the Pokémon name from OCR
STOPWORDS = {
    "basic", "stage", "pokemon", "pokémon", "trainer", "energy",
    "hp", "weakness", "resistance", "retreat", "rule", "illus",
    "illustration", "attack", "attacks", "ability", "abilities",
}

# Suffixes we strip from the name line if present (we want base Pokémon name)
CARD_SUFFIXES = {
    "gx", "ex", "v", "vmax", "vstar", "lv", "lvl", "tag", "team",
    "break", "prime", "δ", "delta", "radiant",
}

# -----------------------------
# Utility
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_ocr_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ocr_runs (
          run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at        TEXT NOT NULL,
          provider          TEXT NOT NULL,
          filename          TEXT,
          image_sha256      TEXT NOT NULL,
          image_bytes       INTEGER NOT NULL,
          status            TEXT NOT NULL,
          elapsed_ms        INTEGER,
          error_message     TEXT
        );

        CREATE TABLE IF NOT EXISTS ocr_results (
          run_id                 INTEGER PRIMARY KEY,
          full_text              TEXT NOT NULL,
          collector_number_raw   TEXT,
          collector_number_norm  TEXT,
          promo_number_raw       TEXT,
          promo_number_norm      TEXT,
          pokemon_name           TEXT,
          match_strategy         TEXT,
          match_count            INTEGER,
          variant_product_count  INTEGER,
          variant_subtype_count  INTEGER,
          FOREIGN KEY(run_id) REFERENCES ocr_runs(run_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_ocr_runs_created_at ON ocr_runs(created_at);
        CREATE INDEX IF NOT EXISTS idx_ocr_results_collector_norm ON ocr_results(collector_number_norm);
        CREATE INDEX IF NOT EXISTS idx_ocr_results_promo_norm ON ocr_results(promo_number_norm);
        """
    )
    conn.commit()

def save_run(conn: sqlite3.Connection, filename: str, image_bytes: bytes, status: str,
             elapsed_ms: int, error_message: Optional[str] = None) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ocr_runs (created_at, provider, filename, image_sha256, image_bytes, status, elapsed_ms, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (utc_now_iso(), "google_vision", filename, sha256_hex(image_bytes), len(image_bytes), status, elapsed_ms, error_message),
    )
    run_id = int(cur.lastrowid)
    conn.commit()
    return run_id

def save_result(conn: sqlite3.Connection, run_id: int, full_text: str,
                collector_raw: Optional[str], collector_norm: Optional[str],
                promo_raw: Optional[str], promo_norm: Optional[str],
                pokemon_name: Optional[str],
                match_strategy: Optional[str],
                match_count: int,
                variant_product_count: int,
                variant_subtype_count: int) -> None:
    conn.execute(
        """
        INSERT INTO ocr_results (
          run_id, full_text,
          collector_number_raw, collector_number_norm,
          promo_number_raw, promo_number_norm,
          pokemon_name,
          match_strategy, match_count,
          variant_product_count, variant_subtype_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id, full_text,
            collector_raw, collector_norm,
            promo_raw, promo_norm,
            pokemon_name,
            match_strategy, match_count,
            variant_product_count, variant_subtype_count
        ),
    )
    conn.commit()

# -----------------------------
# Parsing (number + name)
# -----------------------------

def parse_slash_number(text: str) -> Optional[str]:
    m = SLASH_RE.search(text or "")
    if not m:
        return None
    return m.group(1).replace(" ", "")

def normalize_slash(raw: str) -> str:
    try:
        a, b = raw.split("/")
        return f"{int(a)}/{int(b)}"
    except Exception:
        return raw.strip()

def parse_promo_number(text: str) -> Optional[str]:
    m = PROMO_RE.search(text or "")
    if m:
        prefix = m.group(1).upper()
        num = m.group(2)
        width = len(num)  # preserve SM05 style width
        return f"{prefix}{int(num):0{width}d}" if num.startswith("0") else f"{prefix}{int(num)}"

    m2 = WOTC_PROMO_NUM_RE.search(text or "")
    if m2:
        return str(int(m2.group(1)))

    return None

def normalize_promo(raw: str) -> str:
    return re.sub(r"[\s\-]", "", (raw or "").strip()).upper()

def extract_pokemon_name(text: str) -> Optional[str]:
    """
    Pull a best-effort Pokémon name from OCR text.
    Goal: good enough to filter OUT wrong-Pokémon collisions (e.g., Infernape),
    not to be a perfect canonical name resolver.

    Strategy:
    - Scan early lines (name appears near top for most scans)
    - Remove obvious junk tokens (BASIC, STAGE, HP, etc.)
    - Strip suffixes like GX/EX/V
    - Return a short, title-ish result
    """
    if not text:
        return None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    # Search early lines first; then widen if needed
    candidate_lines = lines[:30] + lines[30:60]

    for ln in candidate_lines:
        low = ln.lower()

        # reject boilerplate lines quickly
        if any(tok in low for tok in ["weakness", "resistance", "retreat", "illus", "rule"]):
            continue

        # keep mostly alpha content
        letters = sum(c.isalpha() for c in ln)
        if letters < 4:
            continue

        # normalize: keep letters/digits/apostrophes/hyphens/spaces
        cleaned = re.sub(r"[^A-Za-z0-9'’\-\s&]", " ", ln)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # tokenize and drop stopwords / suffixes
        toks = cleaned.split()
        kept = []
        for t in toks:
            tl = t.lower()
            if tl in STOPWORDS:
                continue
            if tl in CARD_SUFFIXES:
                continue
            # drop lone punctuation-like tokens
            if tl in {"&"}:
                continue
            kept.append(t)

        if not kept:
            continue

        # handle "Snorlax GX" -> Snorlax
        # handle "Blastoise & Piplup GX" -> Blastoise Piplup
        name = " ".join(kept).strip()

        # avoid obviously bad "names"
        name_low = name.lower()
        if name_low in STOPWORDS:
            continue
        if len(name) < 3 or len(name) > 30:
            continue

        return name

    return None

# -----------------------------
# DB matching
# -----------------------------

def query_candidates_by_number(
    conn: sqlite3.Connection,
    collector_norm: Optional[str],
    promo_norm: Optional[str],
) -> Tuple[str | None, pd.DataFrame]:
    """
    Returns (strategy, df).
    Strategy is one of: 'collector_number_norm', 'ext_number_norm', or None.
    """
    if collector_norm:
        df = pd.read_sql_query(
            """
            SELECT
              c.product_id,
              c.group_id,
              c.product_name,
              c.collector_number_raw,
              c.collector_number_norm,
              c.ext_number_raw,
              c.ext_number_norm,
              p.sub_type,
              p.market_price,
              p.updated_at
            FROM cards c
            LEFT JOIN prices_latest p ON p.product_id = c.product_id
            WHERE c.collector_number_norm = ?
            ORDER BY c.group_id, c.product_name, p.sub_type;
            """,
            conn,
            params=[collector_norm],
        )
        return "collector_number_norm", df

    if promo_norm:
        df = pd.read_sql_query(
            """
            SELECT
              c.product_id,
              c.group_id,
              c.product_name,
              c.collector_number_raw,
              c.collector_number_norm,
              c.ext_number_raw,
              c.ext_number_norm,
              p.sub_type,
              p.market_price,
              p.updated_at
            FROM cards c
            LEFT JOIN prices_latest p ON p.product_id = c.product_id
            WHERE c.ext_number_norm = ?
            ORDER BY c.group_id, c.product_name, p.sub_type;
            """,
            conn,
            params=[promo_norm],
        )
        return "ext_number_norm", df

    return None, pd.DataFrame()

def filter_candidates_by_pokemon_name(df: pd.DataFrame, pokemon_name: Optional[str]) -> Tuple[pd.DataFrame, bool]:
    """
    Filters OUT cross-Pokémon collisions while keeping all variants of the same Pokémon.

    Returns (filtered_df, applied_filter).
    If filtering yields empty, returns original df and applied_filter=False.
    """
    if df is None or df.empty or not pokemon_name:
        return df, False

    needle = pokemon_name.strip().lower()
    if not needle:
        return df, False

    mask = df["product_name"].str.lower().str.contains(re.escape(needle), na=False)
    filtered = df[mask].copy()
    if filtered.empty:
        return df, False  # don't over-filter
    return filtered, True

def summarize_variants(df: pd.DataFrame) -> Dict[str, Any]:
    """
    For the remaining candidate set, compute variant info and build dropdown options.
    Dropdown options are keyed by (product_id, sub_type) so holo/reverse show distinctly.
    """
    if df is None or df.empty:
        return {
            "has_variations": False,
            "variant_product_count": 0,
            "variant_subtype_count": 0,
            "options_df": pd.DataFrame(),
        }

    # Ensure uniqueness at (product_id, sub_type) level
    opt = (
        df.sort_values(["group_id", "product_name", "sub_type"])
          .drop_duplicates(subset=["product_id", "sub_type"])
          .reset_index(drop=True)
    )

    product_ct = int(opt["product_id"].nunique())
    subtype_ct = int(opt["sub_type"].nunique()) if "sub_type" in opt.columns else 0

    has_variations = (product_ct > 1) or (subtype_ct > 1)

    return {
        "has_variations": has_variations,
        "variant_product_count": product_ct,
        "variant_subtype_count": subtype_ct,
        "options_df": opt,
    }

# -----------------------------
# Google Vision OCR
# -----------------------------

def google_vision_ocr(image_bytes: bytes) -> str:
    from google.cloud import vision
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_bytes)
    resp = client.document_text_detection(image=image)
    if resp.error and resp.error.message:
        raise RuntimeError(resp.error.message)
    if resp.full_text_annotation and resp.full_text_annotation.text:
        return resp.full_text_annotation.text
    if resp.text_annotations:
        return resp.text_annotations[0].description or ""
    return ""

# -----------------------------
# Streamlit UI
# -----------------------------

st.set_page_config(page_title="OCR Test (Google Vision)", layout="wide")
st.title("OCR Test — Google Vision (Front Image Only)")

persist = st.checkbox("Save OCR result to SQLite", value=True)

up = st.file_uploader("Upload card front image (PNG/JPG/WebP)", type=["png", "jpg", "jpeg", "webp"])
if not up:
    st.info("Upload one image to run OCR.")
    st.stop()

image_bytes = up.getvalue()
st.image(image_bytes, caption=up.name, use_container_width=True)

if st.button("Run Google OCR", type="primary"):
    if persist and not DB_PATH.exists():
        st.error(f"SQLite DB not found at {DB_PATH.resolve()}. Create it (or disable saving).")
        st.stop()

    t0 = time.time()
    try:
        text = google_vision_ocr(image_bytes)
        elapsed_ms = int((time.time() - t0) * 1000)

        collector_raw = parse_slash_number(text)
        collector_norm = normalize_slash(collector_raw) if collector_raw else None

        promo_raw = parse_promo_number(text)
        promo_norm = normalize_promo(promo_raw) if promo_raw else None

        pokemon_name = extract_pokemon_name(text)

        strategy = None
        df = pd.DataFrame()
        applied_name_filter = False

        if DB_PATH.exists():
            with get_conn() as conn:
                strategy, df = query_candidates_by_number(conn, collector_norm, promo_norm)
                df, applied_name_filter = filter_candidates_by_pokemon_name(df, pokemon_name)

        summary = summarize_variants(df)
        has_variations = summary["has_variations"]
        variant_product_count = summary["variant_product_count"]
        variant_subtype_count = summary["variant_subtype_count"]
        options_df = summary["options_df"]

        st.subheader("Parsed Output")
        st.json(
            {
                "filename": up.name,
                "collector_number_raw": collector_raw,
                "collector_number_norm": collector_norm,
                "promo_number_raw": promo_raw,
                "promo_number_norm": promo_norm,
                "pokemon_name": pokemon_name,
                "elapsed_ms": elapsed_ms,
                "match_strategy": strategy,
                "candidate_rows_after_name_filter": int(len(df)) if df is not None else 0,
                "applied_name_filter": applied_name_filter,
                "has_variations": has_variations,
                "variant_product_count": variant_product_count,
                "variant_subtype_count": variant_subtype_count,
            }
        )

        st.subheader("Raw OCR Text (first 5,000 chars)")
        st.text((text or "")[:5000])

        # Variant dropdown (keeps Umbreon variants; removes unrelated Pokémon if name filter applies)
        if options_df is not None and not options_df.empty:
            st.subheader("Variant Selection (dropdown)")

            def label_row(r: pd.Series) -> str:
                mp = r.get("market_price")
                mp_s = f"${mp:.2f}" if isinstance(mp, (int, float)) and mp is not None else "N/A"
                return f"{r['product_name']} | {r.get('sub_type','')} | {mp_s} | group_id={r['group_id']} | product_id={r['product_id']}"

            options_df = options_df.copy()
            options_df["label"] = options_df.apply(label_row, axis=1)

            labels = options_df["label"].tolist()
            idx = st.selectbox("Choose the correct variant", range(len(labels)), format_func=lambda i: labels[i])

            chosen = options_df.iloc[int(idx)].to_dict()
            st.write("Selected:")
            st.json(
                {
                    "product_id": int(chosen["product_id"]),
                    "group_id": int(chosen["group_id"]),
                    "product_name": chosen["product_name"],
                    "sub_type": chosen.get("sub_type"),
                    "market_price": chosen.get("market_price"),
                    "updated_at": chosen.get("updated_at"),
                }
            )

        # Show the full candidate set (post-filter)
        if df is not None and not df.empty:
            st.subheader("DB Match Candidates (post-filter)")
            st.dataframe(df, use_container_width=True, hide_index=True)

        if persist:
            with get_conn() as conn:
                ensure_ocr_tables(conn)
                run_id = save_run(conn, up.name, image_bytes, status="success", elapsed_ms=elapsed_ms)
                save_result(
                    conn,
                    run_id,
                    text,
                    collector_raw,
                    collector_norm,
                    promo_raw,
                    promo_norm,
                    pokemon_name,
                    strategy,
                    int(len(df)) if df is not None else 0,
                    variant_product_count,
                    variant_subtype_count,
                )
            st.success(f"Saved OCR run + result to {DB_PATH}")

    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        msg = str(e)
        msg = msg[:800] + "..." if len(msg) > 800 else msg
        st.error("OCR failed: " + msg)

        if persist and DB_PATH.exists():
            with get_conn() as conn:
                ensure_ocr_tables(conn)
                run_id = save_run(conn, up.name, image_bytes, status="error", elapsed_ms=elapsed_ms, error_message=msg)
            st.warning(f"Saved failed OCR run_id={run_id} to {DB_PATH}")
