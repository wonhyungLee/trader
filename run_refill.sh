#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export PYTHONUNBUFFERED=1

PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

# Viewer-US 기준 리필 (NASDAQ100 + S&P500)
"$PYBIN" -u -m src.collectors.refill_loader \
  --chunk-days 150 \
  --start-mode listing \
  --sleep 0.1 \
  --resume
