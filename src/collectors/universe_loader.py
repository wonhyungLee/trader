"""유니버스(종목 마스터) 수집 스크립트.

기본 구현은 FinanceDataReader를 사용해 KRX 전체를 받아 SQLite `stock_info`에 저장한다.
실전 운영 시 KIS 종목마스터 API로 교체해도 동일한 인터페이스를 유지한다.
"""

import argparse
from datetime import datetime
import pandas as pd
import FinanceDataReader as fdr  # type: ignore

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db


EXCLUDE_MARKETS = {"ETF", "ETN", "ELW", "KONEX"}


def load_krx_master() -> pd.DataFrame:
    df = fdr.StockListing("KRX")
    # FinanceDataReader 0.9.x returns columns with capitalized names (Code/Name/Market/Marcap).
    # Keep backward compatibility with old Symbol naming.
    rename_map = {
        "Symbol": "code",
        "Code": "code",
        "Name": "name",
        "Market": "market",
        "Marcap": "marcap",
    }
    df = df.rename(columns=rename_map)
    missing = {"code", "name", "market", "marcap"} - set(df.columns)
    if missing:
        raise ValueError(f"Unexpected columns from StockListing, missing: {missing}")
    df = df[["code", "name", "market", "marcap"]]
    df = df[~df["market"].isin(EXCLUDE_MARKETS)]
    df = df[df["code"].str.len() == 6]
    return df


def main(top_n: int | None = None):
    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))

    df = load_krx_master()
    if top_n:
        df = df.sort_values("marcap", ascending=False).head(top_n)

    store.upsert_stock_info(df.to_dict(orient="records"))
    print(f"stored {len(df)} symbols at {datetime.now():%Y-%m-%d %H:%M:%S}")
    maybe_export_db(settings, store.db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=None, help="상위 시가총액 종목만 저장")
    args = parser.parse_args()
    main(args.top)
