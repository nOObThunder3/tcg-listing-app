PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sets (
  group_id     INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  abbreviation TEXT,
  published_on TEXT,
  updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sets_name
ON sets (name);
