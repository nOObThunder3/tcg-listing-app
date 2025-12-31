#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")/.."

# Simple lock to prevent overlapping runs
LOCKFILE="logs/refresh_prices_daily.lock"
if [[ -f "$LOCKFILE" ]]; then
  echo "$(date -u +'%F %T') UTC - Lock exists, exiting." >> logs/refresh_prices_daily.log
  exit 0
fi
trap 'rm -f "$LOCKFILE"' EXIT
touch "$LOCKFILE"

# Activate venv
source .venv/bin/activate

# Force snapshot_date to "today in UTC" (same calendar day at 21:00 UTC)
SNAPSHOT_DATE="$(date -u +%F)"

echo "$(date -u +'%F %T') UTC - Starting refresh for snapshot_date=$SNAPSHOT_DATE" >> logs/refresh_prices_daily.log

python scripts/refresh_prices_daily.py --snapshot-date "$SNAPSHOT_DATE" >> logs/refresh_prices_daily.log 2>&1

echo "$(date -u +'%F %T') UTC - Finished refresh for snapshot_date=$SNAPSHOT_DATE" >> logs/refresh_prices_daily.log
