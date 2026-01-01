CREATE TABLE IF NOT EXISTS cards (
  product_id             INTEGER PRIMARY KEY,              -- TCGplayer productId
  group_id               INTEGER NOT NULL REFERENCES sets(group_id),
  product_name           TEXT NOT NULL,
  clean_name             TEXT,                             -- normalized name for lookup
  collector_number_raw   TEXT,
  collector_number_norm  TEXT,                             -- normalized number for OCR
  rarity                 TEXT,
  image_url              TEXT,
  tcgplayer_url          TEXT,
  product_type           TEXT NOT NULL DEFAULT 'unknown',  -- single / sealed / other / unknown
  updated_at             TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cards_group_num
ON cards (group_id, collector_number_norm);

CREATE INDEX IF NOT EXISTS idx_cards_group_name
ON cards (group_id, clean_name);

CREATE INDEX IF NOT EXISTS idx_cards_group_type
ON cards (group_id, product_type);

-- ----------------------------
-- Pricing (marketPrice-only, variant-aware)
-- ----------------------------

CREATE TABLE IF NOT EXISTS prices_latest (
  product_id   INTEGER NOT NULL REFERENCES cards(product_id),
  sub_type     TEXT NOT NULL,               -- e.g., Normal / Holofoil / Reverse Holofoil
  market_price REAL,
  updated_at   TEXT NOT NULL,               -- UTC timestamp of last refresh
  PRIMARY KEY (product_id, sub_type)
);

CREATE TABLE IF NOT EXISTS prices_history (
  product_id    INTEGER NOT NULL REFERENCES cards(product_id),
  sub_type      TEXT NOT NULL,
  snapshot_date TEXT NOT NULL,              -- UTC date YYYY-MM-DD (one snapshot per day)
  market_price  REAL,
  captured_at   TEXT NOT NULL,              -- UTC timestamp when captured
  PRIMARY KEY (product_id, sub_type, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_prices_hist_series_date
ON prices_history (product_id, sub_type, snapshot_date);

-- ----------------------------
-- Trends (latest snapshot only)
-- ----------------------------

CREATE TABLE IF NOT EXISTS trends_latest (
  product_id      INTEGER NOT NULL REFERENCES cards(product_id),
  sub_type        TEXT NOT NULL,
  snapshot_date   TEXT NOT NULL,            -- date of the "today" point used
  market_price    REAL,
  market_price_7d REAL,
  market_price_30d REAL,
  pct_change_7d   REAL,
  pct_change_30d  REAL,
  computed_at     TEXT NOT NULL,
  PRIMARY KEY (product_id, sub_type)
);

-- ----------------------------
-- Run log (operational visibility)
-- ----------------------------

CREATE TABLE IF NOT EXISTS run_log (
  run_id                INTEGER PRIMARY KEY AUTOINCREMENT,
  job_name              TEXT NOT NULL,
  snapshot_date         TEXT NOT NULL,
  started_at            TEXT NOT NULL,
  finished_at           TEXT,
  status                TEXT NOT NULL,      -- running / success / failed
  groups_count          INTEGER DEFAULT 0,
  price_rows_fetched    INTEGER DEFAULT 0,
  price_rows_kept       INTEGER DEFAULT 0,
  latest_upserts        INTEGER DEFAULT 0,
  history_inserts       INTEGER DEFAULT 0,
  trends_upserts        INTEGER DEFAULT 0,
  notes                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_log_job_date
ON run_log (job_name, snapshot_date);




-- OCR run log (one row per uploaded image processed)
CREATE TABLE IF NOT EXISTS ocr_runs (
  run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at        TEXT NOT NULL,          -- ISO8601 UTC timestamp
  provider          TEXT NOT NULL,          -- "google_vision"
  filename          TEXT,
  image_sha256      TEXT NOT NULL,
  image_bytes       INTEGER NOT NULL,
  status            TEXT NOT NULL,          -- "success" | "error"
  elapsed_ms        INTEGER,
  error_message     TEXT
);

-- OCR extracted + parsed data (one row per run)
CREATE TABLE IF NOT EXISTS ocr_results (
  run_id                 INTEGER PRIMARY KEY,
  full_text              TEXT NOT NULL,
  collector_number_raw   TEXT,
  collector_number_norm  TEXT,
  name_hint              TEXT,
  FOREIGN KEY(run_id) REFERENCES ocr_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ocr_runs_created_at ON ocr_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_ocr_results_collector_norm ON ocr_results(collector_number_norm);
