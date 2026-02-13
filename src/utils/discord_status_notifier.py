from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.utils.config import load_settings
from src.utils.notifier import maybe_notify


UNCLASSIFIED_LABEL = "미분류"


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _now_kst_label() -> str:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        # Fallback: server local time (likely UTC)
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _dedup_by_sector(items: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    """Return at most 1 item per sector, preferring the best rank (smallest number)."""
    best: Dict[str, tuple] = {}
    for idx, it in enumerate(items or []):
        if not isinstance(it, dict):
            continue
        code = _safe_text(it.get("code")).upper()
        if not code:
            continue
        sector = _safe_text(it.get("sector_name")) or UNCLASSIFIED_LABEL

        rank_val: Optional[int] = None
        try:
            rank_val = int(it.get("rank"))
            if rank_val <= 0:
                rank_val = None
        except Exception:
            rank_val = None

        cand = (rank_val if rank_val is not None else 10**9, idx, it)
        prev = best.get(sector)
        if prev is None or cand[:2] < prev[:2]:
            best[sector] = cand

    ordered = sorted(best.values(), key=lambda x: (x[0], x[1]))
    return [x[2] for x in ordered][: max(0, int(limit))]


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_message(conn: sqlite3.Connection, settings: Dict[str, Any], state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    total_universe = conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0]
    price_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM daily_price").fetchone()[0]
    price_rows = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
    min_date, max_date = conn.execute("SELECT MIN(date), MAX(date) FROM daily_price").fetchone()
    refill_done = conn.execute("SELECT COUNT(*) FROM refill_progress WHERE status='DONE'").fetchone()[0]
    job_recent = conn.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0]
    refill_remaining = max(int(total_universe) - int(refill_done), 0)

    # --- Recommendation block (final candidates) ---
    last_codes = [str(c).upper().strip() for c in (state.get("last_codes") or []) if str(c).strip()]
    last_items = state.get("last_items") or []
    last_code_set = set(last_codes)

    selection_date = None
    candidate_items: List[Dict[str, Any]] = []
    candidate_codes: List[str] = []
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
    except Exception as exc:
        selection_error = str(exc)
        candidate_items = []
        candidate_codes = []

    new_codes = [c for c in candidate_codes if c not in last_code_set]
    item_by_code = {str(r.get("code") or "").upper().strip(): r for r in candidate_items if isinstance(r, dict)}

    reco_label = "최근 추천"
    display_items: List[Dict[str, Any]]
    if new_codes:
        raw_items = [item_by_code.get(c, {"code": c}) for c in new_codes[:10]]
        display_items = _dedup_by_sector(raw_items, limit=10)
        reco_label = f"신규 추천 ({len(new_codes)}->{len(display_items)})"
    else:
        display_items = candidate_items if candidate_items else (last_items if isinstance(last_items, list) else [])
        display_items = _dedup_by_sector(display_items, limit=10)

    reco_lines: List[str] = []
    for r in display_items:
        if not isinstance(r, dict):
            continue
        code = str(r.get("code") or "").upper().strip()
        if not code:
            continue
        name = _safe_text(r.get("name"))
        sector = _safe_text(r.get("sector_name"))
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

    state_updates: Dict[str, Any] = {}
    if candidate_codes:
        state_updates["last_codes"] = candidate_codes
        state_updates["last_items"] = candidate_items[:20]
        state_updates["last_selection_date"] = selection_date

    # --- System health block ---
    system_errors: List[str] = []
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
    ts_label = _now_kst_label()

    msg = (
        f"[VIEWER-US] {ts_label}\n"
        f"[{reco_label}] (date={sel_label})\n"
        + "\n".join(reco_lines)
        + "\n\n"
        f"[시스템] {sys_state}{sys_detail}\n"
        f"Universe: {total_universe} | Price codes: {price_codes} | Price rows: {price_rows}\n"
        f"Daily range: {min_date} ~ {max_date}\n"
        f"Refill done: {refill_done}/{total_universe} (remaining {refill_remaining})\n"
        f"Job runs: {job_recent}\n"
    )
    return msg, state_updates


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-file", default="data/discord_reco_state.json", help="diff state json file")
    ap.add_argument("--cooldown-sec", type=int, default=21600, help="minimum interval between sends")
    ap.add_argument("--force", action="store_true", help="ignore cooldown and send now")
    args = ap.parse_args()

    settings = load_settings()
    db_path = settings.get("database", {}).get("path", "data/market_data.db")
    state_path = Path(args.state_file)
    state = _load_state(state_path)

    now_ts = time.time()
    cooldown = max(0, int(args.cooldown_sec))
    last_sent_ts = float(state.get("last_sent_ts") or 0.0)
    if (not args.force) and last_sent_ts and cooldown and (now_ts - last_sent_ts) < cooldown:
        remain = int(cooldown - (now_ts - last_sent_ts))
        print(f"[discord_status_notifier] skipped by cooldown (remain {remain}s)", flush=True)
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        msg, updates = _build_message(conn, settings, state)
    finally:
        conn.close()

    maybe_notify(settings, msg)
    print(msg, flush=True)

    # Persist only when the message is actually sent (so "newly appeared" is relative to last delivered alert).
    try:
        state_out = dict(state)
        state_out.update(updates or {})
        state_out["last_sent_ts"] = now_ts
        state_out["last_sent_at"] = _now_kst_label()
        _save_state(state_path, state_out)
    except Exception:
        pass


if __name__ == "__main__":
    main()
