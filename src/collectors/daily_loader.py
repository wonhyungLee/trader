"""일일 증분 수집 (KIS)."""

import argparse
from datetime import datetime, timedelta
import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db
from src.collectors.kis_price_client import KISPriceClient
from src.collectors.refill_loader import _parse_kis_daily


def fetch_prices_kis(client: KISPriceClient, code: str, start: str, end: str) -> pd.DataFrame:
    res = client.get_daily_prices(code, start.replace("-", ""), end.replace("-", ""))
    return _parse_kis_daily(res)


def main(limit: int | None = None, chunk_days: int = 90):
    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    job_id = store.start_job("daily_loader")
    client = KISPriceClient(settings)
    client.broker.reset_sessions()

    codes = store.list_universe_codes()
    if not codes:
        raise SystemExit("universe_members is empty. Run universe_loader first.")
    if limit:
        codes = codes[:limit]
    today = datetime.today().date()
    try:
        for code in codes:
            last = store.last_price_date(code)
            if not last:
                # refill이 먼저
                continue
            start_dt = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
            if start_dt > today:
                continue

            # forward chunk
            cur_start = start_dt
            while cur_start <= today:
                cur_end = min(cur_start + timedelta(days=chunk_days), today)
                df = fetch_prices_kis(client, code, cur_start.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d"))
                if df.empty:
                    break
                store.upsert_daily_prices(code, df)
                max_date = df["date"].max()
                next_start = datetime.strptime(max_date, "%Y-%m-%d").date() + timedelta(days=1)
                if next_start <= cur_start:
                    break
                cur_start = next_start

        store.finish_job(job_id, "SUCCESS", f"codes={len(codes)}")
    except Exception as exc:
        store.finish_job(job_id, "ERROR", str(exc))
        raise

    maybe_export_db(settings, store.db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="처리할 종목 수 제한(테스트 용)")
    parser.add_argument("--chunk-days", type=int, default=90, help="증분 호출 범위(캘린더일)")
    args = parser.parse_args()
    main(args.limit, args.chunk_days)
