#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VENV_PY = ROOT / ".venv" / "bin" / "python"
if VENV_PY.exists() and Path(sys.executable) != VENV_PY:
    os.execv(str(VENV_PY), [str(VENV_PY), str(Path(__file__).resolve()), *sys.argv[1:]])


def check_database(db_path: Path, max_stale_days: int) -> tuple[bool, list[str]]:
    logs: list[str] = []
    if not db_path.exists():
        return False, [f"DB not found: {db_path}"]

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = {row[0] for row in cur.fetchall()}
        required_tables = {
            "universe_members",
            "daily_price",
            "sector_map",
            "refill_progress",
            "job_runs",
        }
        missing = sorted(required_tables - tables)
        if missing:
            return False, [f"Missing required tables: {', '.join(missing)}"]

        universe_count = conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0]
        price_count = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
        sector_count = conn.execute("SELECT COUNT(*) FROM sector_map").fetchone()[0]
        refill_done = conn.execute(
            "SELECT COUNT(*) FROM refill_progress WHERE status='DONE'"
        ).fetchone()[0]
        minmax = conn.execute("SELECT MIN(date), MAX(date) FROM daily_price").fetchone()
        min_date, max_date = minmax[0], minmax[1]

        if universe_count < 100:
            return False, [f"universe_members too small: {universe_count}"]
        if price_count <= 0:
            return False, ["daily_price is empty"]
        if not max_date:
            return False, ["daily_price max(date) is null"]

        max_dt = datetime.strptime(max_date, "%Y-%m-%d").date()
        stale_days = (date.today() - max_dt).days
        if stale_days > max_stale_days:
            return False, [
                f"daily_price stale: latest={max_date}, stale_days={stale_days}, limit={max_stale_days}"
            ]

        missing_price_codes = conn.execute(
            """
            SELECT COUNT(*)
            FROM universe_members u
            LEFT JOIN (SELECT DISTINCT code FROM daily_price) d
            ON u.code = d.code
            WHERE d.code IS NULL
            """
        ).fetchone()[0]

        logs.append(
            (
                "DB OK"
                f" | universe={universe_count}"
                f" prices={price_count}"
                f" sectors={sector_count}"
                f" refill_done={refill_done}"
                f" date={min_date}~{max_date}"
                f" stale_days={stale_days}"
                f" missing_price_codes={missing_price_codes}"
            )
        )
        return True, logs
    finally:
        conn.close()


def check_api() -> tuple[bool, list[str]]:
    logs: list[str] = []
    try:
        from server import app  # lazy import after DB checks
    except Exception as exc:
        return False, [f"Failed to import server app: {exc}"]

    client = app.test_client()
    targets = [
        "/status",
        "/universe",
        "/sectors",
        "/selection",
        "/strategy",
        "/prices?code=AAPL&days=5",
    ]
    for path in targets:
        try:
            resp = client.get(path)
        except Exception as exc:
            return False, [f"API call failed: {path} ({exc})"]
        if resp.status_code != 200:
            return False, [f"API status not 200: {path} -> {resp.status_code}"]
        body = resp.get_json(silent=True)
        if body is None:
            return False, [f"API body is not JSON: {path}"]
        kind = "list" if isinstance(body, list) else "dict"
        size = len(body) if hasattr(body, "__len__") else 0
        logs.append(f"API OK | {path} -> {kind}[{size}]")
    return True, logs


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose BNF Viewer (US) health.")
    parser.add_argument("--db", default="data/market_data.db", help="SQLite DB path")
    parser.add_argument(
        "--max-stale-days",
        type=int,
        default=7,
        help="Fail when latest daily_price date is older than this many days",
    )
    args = parser.parse_args()

    db_ok, db_logs = check_database(Path(args.db), args.max_stale_days)
    api_ok, api_logs = check_api()

    print("== Diagnose Viewer ==")
    for line in db_logs + api_logs:
        print(line)

    if db_ok and api_ok:
        print("RESULT: PASS")
        return 0
    print("RESULT: FAIL")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
