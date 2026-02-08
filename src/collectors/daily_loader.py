"""일일 증분 수집.

각 종목의 마지막 저장 일자를 조회하여 이후 데이터만 수집한다.
기본 구현은 FinanceDataReader를 사용하며, KIS 시세 API로 교체할 수 있도록 함수 분리.
"""

import argparse
from datetime import datetime, timedelta
import pandas as pd
import FinanceDataReader as fdr  # type: ignore

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db
from src.collectors.bulk_loader import compute_features


def fetch_prices(code: str, start: str, end: str) -> pd.DataFrame:
    raw = fdr.DataReader(code, start=start, end=end)
    if raw.empty:
        return pd.DataFrame()
    return compute_features(raw)


def main(limit: int | None = None):
    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    codes = store.list_stock_codes()
    if limit:
        codes = codes[:limit]
    today = datetime.today().strftime("%Y-%m-%d")
    for code in codes:
        last = store.last_price_date(code)
        start_date = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last else (datetime.today() - timedelta(days=60)).strftime("%Y-%m-%d")
        if start_date > today:
            continue
        df = fetch_prices(code, start_date, today)
        if df.empty:
            continue
        store.upsert_daily_prices(code, df)
        print(f"updated {code} {len(df)} rows")
    maybe_export_db(settings, store.db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="처리할 종목 수 제한(테스트 용)" )
    args = parser.parse_args()
    main(args.limit)
