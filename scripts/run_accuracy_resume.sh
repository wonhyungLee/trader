#!/usr/bin/env bash
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"
export PYTHONUNBUFFERED=1

echo "run_accuracy_resume.sh is deprecated in Viewer-US mode."
echo "Use ./scripts/diagnose_viewer.py for health checks."
exit 1
