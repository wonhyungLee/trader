#!/usr/bin/env bash
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"
export PYTHONUNBUFFERED=1
INTERVAL_SEC=${STATUS_NOTIFY_INTERVAL_SEC:-600}
PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

while true; do
  if ! "$PYBIN" - <<'PY'
import sqlite3, time
from src.utils.config import load_settings
from src.utils.notifier import maybe_notify

settings = load_settings()
conn = sqlite3.connect('data/market_data.db')
conn.row_factory = sqlite3.Row

total_universe = conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0]
price_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM daily_price").fetchone()[0]
price_rows = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
min_date, max_date = conn.execute("SELECT MIN(date), MAX(date) FROM daily_price").fetchone()
refill_done = conn.execute("SELECT COUNT(*) FROM refill_progress WHERE status='DONE'").fetchone()[0]
job_recent = conn.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0]
refill_remaining = max(total_universe - refill_done, 0)

ts = time.strftime("%Y-%m-%d %H:%M:%S")
msg = (
    f"[VIEWER-US STATUS] {ts}\n"
    f"Universe: {total_universe} | Price codes: {price_codes} | Price rows: {price_rows}\n"
    f"Daily range: {min_date} ~ {max_date}\n"
    f"Refill done: {refill_done}/{total_universe} (remaining {refill_remaining})\n"
    f"Job runs: {job_recent}\n"
)

maybe_notify(settings, msg)
print(msg, flush=True)
conn.close()
PY
  then
    echo "[WARN] status_notifier python failed" >&2
  fi
  sleep "$INTERVAL_SEC"
done
