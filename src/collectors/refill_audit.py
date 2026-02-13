from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def find_stale_codes(store: SQLiteStore) -> List[Tuple[str, str]]:
    """Return list of (code, reason) pairs that should be refilled again."""
    # Daily stats per code
    cur = store.conn.execute(
        """
        SELECT u.code,
               COALESCE(dp.cnt, 0) AS cnt,
               dp.min_date
        FROM universe_members u
        LEFT JOIN (
            SELECT code, COUNT(*) AS cnt, MIN(date) AS min_date
            FROM daily_price
            GROUP BY code
        ) dp
        ON u.code = dp.code
        ORDER BY u.code
        """
    )
    daily_map: Dict[str, Tuple[int, str]] = {}
    for code, cnt, min_date in cur.fetchall():
        daily_map[str(code).strip().upper()] = (int(cnt or 0), _safe_text(min_date))

    # Refill progress map (optional)
    cur = store.conn.execute("SELECT code, status, last_min_date FROM refill_progress")
    rp_map: Dict[str, Tuple[str, str]] = {}
    for code, status, last_min_date in cur.fetchall():
        rp_map[str(code).strip().upper()] = (_safe_text(status), _safe_text(last_min_date))

    out: List[Tuple[str, str]] = []
    for code, (cnt, _) in daily_map.items():
        status, last_min = rp_map.get(code, ("", ""))

        if cnt <= 0:
            out.append((code, "no_daily_price_rows"))
            continue

        if not status:
            out.append((code, "no_refill_progress_row"))
            continue

        # Historical bug case: refill marked DONE without ever saving a chunk (last_min_date NULL).
        if status.upper() == "DONE" and not last_min:
            out.append((code, "done_but_last_min_missing"))
            continue

    return out


def apply_stale_marks(store: SQLiteStore, targets: List[Tuple[str, str]]) -> int:
    now = datetime.utcnow().isoformat()
    updated = 0
    for code, reason in targets:
        row = store.get_refill_status(code)
        next_end = row["next_end_date"] if row and row["next_end_date"] else None
        last_min = row["last_min_date"] if row and row["last_min_date"] else None
        msg = f"audit_mark_stale reason={reason} at={now}"
        store.upsert_refill_status(code=code, next_end=next_end, last_min=last_min, status="STALE", message=msg)
        updated += 1
    return updated


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="DB에 STALE 마킹을 실제 반영")
    args = ap.parse_args()

    settings = load_settings()
    db_path = settings.get("database", {}).get("path", "data/market_data.db")
    store = SQLiteStore(db_path)

    targets = find_stale_codes(store)
    logging.info("stale targets: %s", len(targets))
    if targets:
        logging.info("sample: %s", targets[:20])

    if args.apply and targets:
        updated = apply_stale_marks(store, targets)
        logging.info("updated: %s", updated)


if __name__ == "__main__":
    main()

