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
