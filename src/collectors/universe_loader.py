"""유니버스(250개) 스냅샷 로더.

오직 data/universe_kospi100.csv + data/universe_kosdaq150.csv만 사용한다.
"""

import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db


def load_universe_csv(path: str, group_name: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"universe file missing: {path}")
    df = pd.read_csv(p)
    required = {"code", "name", "market"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Missing columns in {path}: {required - set(df.columns)}")
    df = df.copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["group_name"] = group_name
    return df[["code", "name", "market", "group_name"]]


def main():
    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    job_id = store.start_job("universe_loader")

    try:
        df_kospi = load_universe_csv("data/universe_kospi100.csv", "KOSPI100")
        df_kosdaq = load_universe_csv("data/universe_kosdaq150.csv", "KOSDAQ150")
        df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)

        # universe_members 고정
        store.upsert_universe_members(df.to_dict(orient="records"))
        # stock_info는 universe_members 기준 250개만 유지
        store.replace_stock_info(
            df.assign(marcap=0).to_dict(orient="records")
        )

        print(f"stored universe {len(df)} symbols at {datetime.now():%Y-%m-%d %H:%M:%S}")
        maybe_export_db(settings, store.db_path)
        store.finish_job(job_id, "SUCCESS", f"stored {len(df)} symbols")
    except Exception as exc:
        store.finish_job(job_id, "ERROR", str(exc))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.parse_args()
    main()
