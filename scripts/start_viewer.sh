#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

WATCHDOG_AUTOSTART="${WATCHDOG_AUTOSTART:-1}"
if [ "$WATCHDOG_AUTOSTART" = "1" ]; then
  if ! (pgrep -af "src.utils.data_watchdog" | grep -F "$ROOT" >/dev/null 2>&1); then
    mkdir -p logs
    nohup "$PYBIN" -u -m src.utils.data_watchdog >> logs/watchdog.out 2>&1 &
  fi
fi

# Keep data collection out of web process for stable API latency.
export BNF_DB_WATCHDOG_ENABLED=0
exec "$PYBIN" main.py
