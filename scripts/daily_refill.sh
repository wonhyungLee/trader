#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

export PYTHONUNBUFFERED=1
export PYTHONPATH="$ROOT"

PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

LOCK_DIR="data/locks"
FLOCK_PATH="$LOCK_DIR/daily_refill.flock"
PID_LOCK_PATH="$LOCK_DIR/refill_loader.lock"

mkdir -p "$LOCK_DIR"

# Prevent concurrent daily runs.
exec 9>"$FLOCK_PATH"
if ! flock -n 9; then
  echo "[daily_refill] another run is active; exit"
  exit 0
fi

echo "$$" > "$PID_LOCK_PATH"
trap 'rm -f "$PID_LOCK_PATH"' EXIT

echo "[daily_refill] start at $(date -u +'%F %T UTC')"

echo "[daily_refill] excd backfill (best-effort)"
$PYBIN -u -m src.collectors.excd_backfill --apply || true

echo "[daily_refill] refill audit (mark stale)"
$PYBIN -u -m src.collectors.refill_audit --apply

TODO_CNT="$(
  $PYBIN - <<'PY'
from src.utils.config import load_settings
import sqlite3

settings = load_settings()
db_path = (settings.get("database") or {}).get("path") or "data/market_data.db"
conn = sqlite3.connect(db_path)
try:
    todo = conn.execute(
        """
        SELECT COUNT(*)
        FROM universe_members u
        LEFT JOIN refill_progress r ON u.code = r.code
        WHERE r.status IS NULL OR UPPER(r.status) != 'DONE'
        """
    ).fetchone()[0]
finally:
    conn.close()
print(int(todo or 0))
PY
)"

if [ "${TODO_CNT:-0}" -le 0 ]; then
  echo "[daily_refill] no missing/stale targets; done"
  exit 0
fi

echo "[daily_refill] targets to refill: $TODO_CNT"
$PYBIN -u -m src.collectors.refill_loader \
  --chunk-days 150 \
  --start-mode listing \
  --sleep 0.1 \
  --resume

echo "[daily_refill] finished at $(date -u +'%F %T UTC')"

