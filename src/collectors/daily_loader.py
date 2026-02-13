"""일일 증분 수집 (KIS 해외주식)."""

import argparse
import csv
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db
from src.collectors.kis_price_client import KISPriceClient
from src.collectors.refill_loader import _parse_overseas_daily


def fetch_prices_kis_overseas(client: KISPriceClient, excd: str, code: str, end: str) -> pd.DataFrame:
    res = client.get_overseas_daily_prices(excd, code, end.replace("-", ""))
    return _parse_overseas_daily(res)


def _read_codes_file(path: Optional[str]) -> List[str]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"codes-file missing: {path}")
    # Expect CSV with a 'code' column, but accept 1-column CSV as well.
    with p.open("r", encoding="utf-8") as f:
        sample = f.read(4096)
    if "," not in sample and "\n" in sample and "code" not in sample.lower():
        # plain text file: one code per line
        out = [line.strip().upper() for line in sample.splitlines() if line.strip()]
        # read rest
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                code = line.strip().upper()
                if code:
                    out.append(code)
        return list(dict.fromkeys(out))

    codes: List[str] = []
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return []
        header_l = [str(c).strip().lower() for c in header]
        code_idx = header_l.index("code") if "code" in header_l else 0
        for row in reader:
            if not row:
                continue
            if code_idx >= len(row):
                continue
            code = str(row[code_idx]).strip().upper()
            if code:
                codes.append(code)
    return list(dict.fromkeys(codes))


def _sleep_on_error(exc: Exception, settings: dict) -> None:
    msg = str(exc)
    if "403" in msg:
        sleep_sec = float(settings.get("kis", {}).get("auth_forbidden_cooldown_sec", 600))
    elif "500" in msg:
        sleep_sec = float(settings.get("kis", {}).get("consecutive_error_cooldown_sec", 180))
    else:
        sleep_sec = 5.0
    logging.warning("daily_loader error. cooling down %.1fs: %s", sleep_sec, msg)
    time.sleep(max(1.0, sleep_sec))


def main(limit: int | None = None, chunk_days: int = 90, codes_file: str | None = None):
    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    job_id = store.start_job("daily_loader")
    client = KISPriceClient(settings)
    client.broker.reset_sessions()

    codes = _read_codes_file(codes_file) if codes_file else store.list_universe_codes()
    if not codes:
        raise SystemExit("universe_members is empty. Run universe_loader first.")
    if limit:
        codes = codes[:limit]
    excd_map = store.list_universe_excd_map()
    universe_df = store.load_universe_df()
    group_map = {row["code"]: row.get("group_name") for _, row in universe_df.iterrows()}
    today = datetime.today().date()
    max_lookback_start = today - timedelta(days=max(1, int(chunk_days)))
    errors = 0
    for code in codes:
        try:
            last = store.last_price_date(code)
            if last:
                start_dt = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
            else:
                # Bootstrap missing symbols with a bounded lookback window.
                start_dt = max_lookback_start
            if start_dt < max_lookback_start:
                start_dt = max_lookback_start
            if start_dt > today:
                continue

            group = str(group_map.get(code, "")).upper()
            base_excd = excd_map.get(code) or ("NAS" if "NASDAQ" in group else "NYS")
            excd_candidates: List[str] = []
            for cand in [base_excd, "NAS", "NYS", "AMS"]:
                c = str(cand or "").strip().upper()
                if not c:
                    continue
                if c not in excd_candidates:
                    excd_candidates.append(c)

            # backward from today, keep rows after last date
            cur_end = today
            while cur_end >= start_dt:
                try:
                    df_all = pd.DataFrame()
                    for excd in excd_candidates:
                        df_all = fetch_prices_kis_overseas(client, excd, code, cur_end.strftime("%Y-%m-%d"))
                        if not df_all.empty:
                            break
                except Exception as exc:
                    errors += 1
                    logging.warning("daily_loader fetch failed %s: %s", code, exc)
                    _sleep_on_error(exc, settings)
                    break
                if df_all.empty:
                    break
                df = df_all[df_all["date"] >= start_dt.strftime("%Y-%m-%d")]
                if not df.empty:
                    store.upsert_daily_prices(code, df)
                min_date_str = df_all["date"].min()
                if not min_date_str:
                    break
                next_end = datetime.strptime(min_date_str, "%Y-%m-%d").date() - timedelta(days=1)
                if next_end >= cur_end:
                    break
                cur_end = next_end
        except Exception as exc:
            errors += 1
            logging.exception("daily_loader failed for %s", code)
            _sleep_on_error(exc, settings)
            continue

    status = "SUCCESS" if errors == 0 else "PARTIAL"
    store.finish_job(job_id, status, f"codes={len(codes)} errors={errors}")

    maybe_export_db(settings, store.db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="처리할 종목 수 제한(테스트 용)")
    parser.add_argument("--chunk-days", type=int, default=90, help="증분 호출 범위(캘린더일)")
    parser.add_argument("--codes-file", type=str, default=None, help="CSV/텍스트 파일(코드 컬럼) 지정 시 해당 코드만 처리")
    args = parser.parse_args()
    main(args.limit, args.chunk_days, args.codes_file)
