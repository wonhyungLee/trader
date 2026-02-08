"""상장일~현재까지 일봉을 chunk 단위로 백필(refill)하는 스크립트.

기본 데이터 소스는 FinanceDataReader이며, KIS API로 교체할 수 있도록 함수 단위로 분리했다.

주요 기능:
- universe CSV 여러 개 또는 단일 코드 선택
- backward 스캔(오늘→과거)로 빈 데이터 3회 연속이면 종료
- 진행상황은 sqlite `refill_progress` 테이블에 저장(resume 지원)
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import FinanceDataReader as fdr  # type: ignore

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.collectors.bulk_loader import compute_features
from src.collectors.kis_price_client import KISPriceClient
from src.utils.notifier import maybe_notify
from src.utils.db_exporter import maybe_export_db


def read_universe(paths: Iterable[str]) -> List[str]:
    codes: List[str] = []
    for p in paths:
        df = pd.read_csv(p)
        col = "code" if "code" in df.columns else "Code" if "Code" in df.columns else df.columns[0]
        codes.extend(df[col].astype(str).str.zfill(6).tolist())
    # unique preserving order
    seen = set()
    uniq = []
    for c in codes:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


def fetch_prices_fdr(code: str, start: str, end: str) -> pd.DataFrame:
    raw = fdr.DataReader(code, start=start, end=end)
    if raw.empty:
        return pd.DataFrame()
    return compute_features(raw)


def fetch_prices_kis(client: KISPriceClient, code: str, start: str, end: str) -> pd.DataFrame:
    """KIS 기간별 시세 → 표준 컬럼 변환."""
    res = client.get_daily_prices(code, start.replace("-", ""), end.replace("-", ""))
    outputs = res.get("output") or res.get("output2") or []
    if not isinstance(outputs, list) or not outputs:
        return pd.DataFrame()
    recs = []
    for o in outputs:
        recs.append({
            "date": o.get("stck_bsop_date"),
            "open": float(o.get("stck_oprc") or 0),
            "high": float(o.get("stck_hgpr") or 0),
            "low": float(o.get("stck_lwpr") or 0),
            "close": float(o.get("stck_clpr") or 0),
            "volume": float(o.get("acml_vol") or 0),
            "amount": float(o.get("acml_tr_pbmn") or 0),
        })
    df = pd.DataFrame(recs)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["ma25"] = df["close"].rolling(25, min_periods=5).mean()
    df["disparity"] = df["close"] / df["ma25"] - 1
    return df[["date", "open", "high", "low", "close", "volume", "amount", "ma25", "disparity"]]


def backward_refill(
    store: SQLiteStore,
    code: str,
    chunk_days: int,
    sleep: float,
    empty_limit: int = 3,
    source: str = "fdr",
    kis_client: Optional[KISPriceClient] = None,
    notify_cb=None,
    notify_every: int = 1,
):
    today = datetime.today().date()
    end = today
    empty_cnt = 0
    min_date_in_db: Optional[str] = None

    chunk_idx = 0
    while True:
        start = end - timedelta(days=chunk_days)
        chunk_idx += 1
        if source == "kis":
            df = fetch_prices_kis(kis_client, code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))  # type: ignore
        else:
            df = fetch_prices_fdr(code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if df.empty:
            empty_cnt += 1
            if empty_cnt >= empty_limit:
                if notify_cb:
                    notify_cb(f"[refill] {code} 종료: empty {empty_cnt}회 연속 (chunk {chunk_idx})")
                break
        else:
            empty_cnt = 0
            store.upsert_daily_prices(code, df)
            min_date_in_db = df["date"].min()
        if notify_cb and (chunk_idx == 1 or (notify_every > 0 and chunk_idx % notify_every == 0)):
            rows = 0 if df.empty else len(df)
            notify_cb(
                f"[refill] {code} 진행 {chunk_idx} (start={start:%Y-%m-%d}, end={end:%Y-%m-%d}, rows={rows}, empty={empty_cnt})"
            )
        end = start - timedelta(days=1)
        if end.year < 1990:
            if notify_cb:
                notify_cb(f"[refill] {code} 종료: 연도 하한 도달 (chunk {chunk_idx})")
            break
        time.sleep(sleep)

    store.upsert_refill_status(code, last_end=end.strftime("%Y-%m-%d"), min_date=min_date_in_db, status="DONE")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", action="append", help="CSV 파일 경로 (code 컬럼 필요)", default=[])
    parser.add_argument("--code", help="단일 종목 코드", default=None)
    parser.add_argument("--chunk-days", type=int, default=90)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--resume", action="store_true", help="refill_progress 기준으로 완료된 종목 건너뛰기")
    parser.add_argument("--source", choices=["fdr", "kis"], default="fdr")
    parser.add_argument("--cooldown", type=float, default=None, help="호출 간 슬립(초). 기본 fdr=0.5, kis=80")
    parser.add_argument("--notify-every", type=int, default=1, help="n 청크마다 진행 알림 (기본 1)")
    args = parser.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    kis_client = KISPriceClient(settings) if args.source == "kis" else None
    sleep = args.cooldown if args.cooldown is not None else (80.0 if args.source == "kis" else args.sleep)

    if args.code:
        codes = [args.code.zfill(6)]
    elif args.universe:
        codes = read_universe(args.universe)
    else:
        codes = store.list_stock_codes()

    total = len(codes)
    processed = 0

    # With --resume, we should not count already-DONE codes as "remaining".
    if args.resume:
        done_set = set(
            r[0]
            for r in store.conn.execute(
                "SELECT code FROM refill_progress WHERE status='DONE'"
            ).fetchall()
        )
        codes = [c for c in codes if c not in done_set]
        total = len(codes)

    for code in codes:
        status = store.get_refill_status(code)
        if args.resume and status and status["status"] == "DONE":
            continue
        try:
            maybe_notify(settings, f"[refill] {code} 시작 (source={args.source}, chunk={args.chunk_days})")
            backward_refill(
                store,
                code,
                args.chunk_days,
                sleep,
                source=args.source,
                kis_client=kis_client,
                notify_cb=lambda msg: maybe_notify(settings, msg),
                notify_every=max(1, int(args.notify_every)),
            )
            print(f"{code} refill DONE")
            processed += 1
            remaining = total - processed
            maybe_notify(settings, f"[refill] {code} 완료 (source={args.source}, chunk={args.chunk_days}) / 남은 종목 {remaining}/{total}")
        except Exception as e:
            store.upsert_refill_status(code, last_end=None, min_date=None, status=f"ERROR:{e}")
            print(f"{code} refill ERROR: {e}")
            processed += 1
            remaining = total - processed
            maybe_notify(settings, f"[refill] {code} ERROR {e} / 남은 종목 {remaining}/{total}")

    maybe_export_db(settings, store.db_path)


if __name__ == "__main__":
    main()
