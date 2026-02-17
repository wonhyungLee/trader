#!/usr/bin/env bash
set -euo pipefail

UNIT="${1:-trader-us-autotrade-worker.service}"
LINES="${SHOW_LOG_LINES:-20}"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl not found"
  exit 2
fi

if ! systemctl list-unit-files "$UNIT" --no-legend >/dev/null 2>&1; then
  echo "unit not found: $UNIT"
  if pgrep -af "[s]rc.autotrade.worker" >/dev/null 2>&1; then
    echo "manual worker process is running"
    pgrep -af "[s]rc.autotrade.worker"
    exit 0
  fi
  exit 2
fi

enabled="no"
if systemctl is-enabled "$UNIT" >/dev/null 2>&1; then
  enabled="yes"
fi

active="no"
if systemctl is-active --quiet "$UNIT"; then
  active="yes"
fi

main_pid="$(systemctl show "$UNIT" -p MainPID --value 2>/dev/null || echo 0)"
sub_state="$(systemctl show "$UNIT" -p SubState --value 2>/dev/null || echo unknown)"

echo "unit=$UNIT enabled=$enabled active=$active sub_state=$sub_state pid=$main_pid"
echo "recent logs:"
journalctl -u "$UNIT" -n "$LINES" --no-pager || true

if [ "$active" != "yes" ]; then
  exit 1
fi
