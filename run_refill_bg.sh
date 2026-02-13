#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

LOG_PATH="logs/refill_bg.log"
mkdir -p logs

PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

nohup "$PYBIN" -u -m src.collectors.refill_loader \
  --chunk-days 150 \
  --start-mode listing \
  --sleep 0.1 \
  --resume \
  > "$LOG_PATH" 2>&1 &

echo "refill started in background (pid=$!), log=$LOG_PATH"
