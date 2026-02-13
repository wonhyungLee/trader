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
import json
import sqlite3
import time
from pathlib import Path

from src.utils.config import load_settings
from src.utils.notifier import maybe_notify

settings = load_settings()
db_path = settings.get("database", {}).get("path", "data/market_data.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

total_universe = conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0]
price_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM daily_price").fetchone()[0]
price_rows = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
min_date, max_date = conn.execute("SELECT MIN(date), MAX(date) FROM daily_price").fetchone()
refill_done = conn.execute("SELECT COUNT(*) FROM refill_progress WHERE status='DONE'").fetchone()[0]
job_recent = conn.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0]
refill_remaining = max(total_universe - refill_done, 0)

ts = time.strftime("%Y-%m-%d %H:%M:%S")

# --- Recommendation block (final candidates) ---
state_path = Path("data/discord_reco_state.json")
state = {}
if state_path.exists():
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        state = {}
last_codes = [str(c).upper().strip() for c in (state.get("last_codes") or []) if str(c).strip()]
last_items = state.get("last_items") or []
last_code_set = set(last_codes)

selection_date = None
candidate_items = []
candidate_codes = []
selection_error = None
try:
    from server import _build_selection_summary

    sel = _build_selection_summary(conn, settings) or {}
    selection_date = sel.get("date")
    for row in (sel.get("candidates") or []):
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").upper().strip()
        if not code:
            continue
        candidate_items.append(row)
        candidate_codes.append(code)
except Exception as e:
    selection_error = str(e)
    candidate_items = []
    candidate_codes = []

new_codes = [c for c in candidate_codes if c not in last_code_set]
item_by_code = {str(r.get("code") or "").upper().strip(): r for r in candidate_items if isinstance(r, dict)}

reco_label = "최근 추천"
display_codes = []
display_items = []
if new_codes:
    reco_label = f"신규 추천 ({len(new_codes)})"
    display_codes = new_codes[:10]
    display_items = [item_by_code.get(c, {"code": c}) for c in display_codes]
else:
    # Prefer the latest computed candidates; if unavailable fall back to last snapshot.
    display_items = candidate_items if candidate_items else last_items
    display_items = display_items[:10]
    display_codes = [str(r.get("code") or "").upper().strip() for r in display_items if isinstance(r, dict)]

reco_lines = []
for r in display_items:
    if not isinstance(r, dict):
        continue
    code = str(r.get("code") or "").upper().strip()
    if not code:
        continue
    name = str(r.get("name") or "").strip()
    sector = str(r.get("sector_name") or "").strip()
    rank = r.get("rank")
    disp = r.get("disparity")
    disp_pct = None
    try:
        disp_pct = float(disp) * 100.0
    except Exception:
        disp_pct = None
    parts = [code]
    if name:
        parts.append(name)
    if sector:
        parts.append(f"[{sector}]")
    if rank:
        parts.append(f"(rank {rank})")
    if disp_pct is not None:
        parts.append(f"disp {disp_pct:+.2f}%")
    reco_lines.append("- " + " ".join(parts))
if not reco_lines:
    reco_lines = ["- (추천 종목 없음)"]

# Persist last snapshot so we can diff "newly appeared" next cycle.
try:
    state_out = dict(state)
    if candidate_codes:
        state_out["last_codes"] = candidate_codes
        state_out["last_items"] = candidate_items[:20]
        state_out["last_selection_date"] = selection_date
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state_out, ensure_ascii=False, indent=2), encoding="utf-8")
except Exception:
    pass

# --- System health block ---
system_errors = []
try:
    job_err = conn.execute(
        "SELECT COUNT(*) FROM job_runs WHERE status IN ('ERROR','FAIL') AND datetime(started_at) >= datetime('now','-1 day')"
    ).fetchone()[0]
except Exception:
    job_err = 0
if job_err:
    system_errors.append(f"job_errors_24h={job_err}")
if not max_date:
    system_errors.append("daily_price_empty")
if selection_error:
    system_errors.append("selection_failed")

try:
    wd_path = Path("data/watchdog_state.json")
    if wd_path.exists():
        wd = json.loads(wd_path.read_text(encoding="utf-8"))
        wd_daily_rc = wd.get("last_daily_rc")
        wd_acc_rc = wd.get("last_accuracy_rc")
        if wd_daily_rc not in (None, 0):
            system_errors.append(f"watchdog_daily_rc={wd_daily_rc}")
        if wd_acc_rc not in (None, 0):
            system_errors.append(f"watchdog_accuracy_rc={wd_acc_rc}")
except Exception:
    pass

sys_state = "OK" if not system_errors else "ERROR"
sys_detail = "" if not system_errors else " (" + ", ".join(system_errors) + ")"

sel_label = f"{selection_date}" if selection_date else "-"
msg = (
    f"[VIEWER-US] {ts}\n"
    f"[{reco_label}] (date={sel_label})\n"
    + "\n".join(reco_lines)
    + "\n\n"
    f"[시스템] {sys_state}{sys_detail}\n"
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
