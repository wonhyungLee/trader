from __future__ import annotations

import argparse
import json
import logging

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings

from .planner import generate_daytrade_orders


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    ap = argparse.ArgumentParser(description="TraderUS selection + Daytrade(Balanced) order planner")
    ap.add_argument(
        "cmd",
        choices=["close", "dryrun"],
        help="close: generate & write orders into order_queue, dryrun: generate only",
    )
    ap.add_argument("--db", default="data/market_data.db", help="SQLite DB path")
    ap.add_argument("--signal-date", default=None, help="YYYY-MM-DD (default: latest in daily_price)")
    ap.add_argument("--exec-date", default=None, help="YYYY-MM-DD (default: next business day)")
    ap.add_argument("--total-assets", type=float, default=None, help="override total assets for sizing")
    args = ap.parse_args()

    settings = load_settings()
    store = SQLiteStore(args.db)
    res = generate_daytrade_orders(
        store,
        settings=settings,
        signal_date=args.signal_date,
        exec_date=args.exec_date,
        total_assets_override=args.total_assets,
        dry_run=(args.cmd == "dryrun"),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
