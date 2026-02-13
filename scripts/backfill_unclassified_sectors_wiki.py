#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.collectors.sector_seed_loader import build_sector_csvs
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}

ICB_TO_GICS_SECTOR = {
    "Technology": "Information Technology",
    "Healthcare": "Health Care",
    "Health Care": "Health Care",
    "Basic Materials": "Materials",
    "Materials": "Materials",
    "Telecommunications": "Communication Services",
    "Communication Services": "Communication Services",
    "Consumer Discretionary": "Consumer Discretionary",
    "Consumer Staples": "Consumer Staples",
    "Industrials": "Industrials",
    "Financials": "Financials",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Real Estate": "Real Estate",
}


def _clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "null", "na", "n/a", "unknown"}:
        return None
    return text


def _read_tables(url: str) -> List[pd.DataFrame]:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return pd.read_html(io.StringIO(resp.text))


def _pick_table(tables: List[pd.DataFrame], must_have: List[str]) -> pd.DataFrame:
    must = [m.lower() for m in must_have]
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if all(any(m in c for c in cols) for m in must):
            return t
    raise RuntimeError(f"no table found with columns: {must_have}")


def _fetch_sp500_secmap() -> Dict[str, Tuple[str, str, str]]:
    tables = _read_tables(SP500_URL)
    t = _pick_table(tables, ["Symbol", "Security"])
    cols = {str(c).lower(): c for c in t.columns}

    sym = cols.get("symbol")
    sector = cols.get("gics sector") or cols.get("gics_sector") or cols.get("sector")
    industry = cols.get("gics sub-industry") or cols.get("gics sub industry") or cols.get("sub-industry") or cols.get("sub industry")
    if not sym or not sector:
        return {}

    out: Dict[str, Tuple[str, str, str]] = {}
    work = pd.DataFrame({
        "code": t[sym].astype(str).str.strip().str.upper(),
        "sector_name": t[sector].apply(_clean_text),
        "industry_name": t[industry].apply(_clean_text) if industry else None,
    }).drop_duplicates(subset=["code"], keep="first")
    for _, row in work.iterrows():
        code = row.get("code")
        sec = row.get("sector_name")
        ind = row.get("industry_name") or ""
        if code and sec:
            out[str(code)] = (str(sec), str(ind), "WIKI_SP500")
    return out


def _fetch_nasdaq100_secmap() -> Dict[str, Tuple[str, str, str]]:
    tables = _read_tables(NASDAQ100_URL)
    try:
        t = _pick_table(tables, ["Ticker", "Company"])
    except Exception:
        t = _pick_table(tables, ["Ticker", "Security"])
    cols = {str(c).lower(): c for c in t.columns}

    ticker = cols.get("ticker") or cols.get("ticker symbol") or cols.get("symbol")
    if not ticker:
        return {}

    # Prefer GICS if present; otherwise use ICB Industry/Subsector mapping.
    sector_col = cols.get("gics sector") or cols.get("sector")
    industry_col = cols.get("gics sub-industry") or cols.get("sub-industry") or cols.get("sub industry")
    if not sector_col:
        sector_col = next((c for k, c in cols.items() if "icb industry" in k), None)
        industry_col = next((c for k, c in cols.items() if "icb subsector" in k), None)

    out: Dict[str, Tuple[str, str, str]] = {}
    work = pd.DataFrame({
        "code": t[ticker].astype(str).str.strip().str.upper(),
        "sector_raw": t[sector_col].apply(_clean_text) if sector_col else None,
        "industry_name": t[industry_col].apply(_clean_text) if industry_col else None,
    }).drop_duplicates(subset=["code"], keep="first")
    for _, row in work.iterrows():
        code = row.get("code")
        raw = row.get("sector_raw")
        ind = row.get("industry_name") or ""
        if not code or not raw:
            continue
        mapped = ICB_TO_GICS_SECTOR.get(str(raw).strip(), str(raw).strip())
        if mapped:
            out[str(code)] = (mapped, str(ind), "WIKI_NASDAQ100")
    return out


def _list_unclassified(store: SQLiteStore) -> List[Tuple[str, str, str]]:
    cur = store.conn.execute(
        """
        SELECT u.code, u.name, u.group_name
        FROM universe_members u
        LEFT JOIN sector_map s ON u.code=s.code
        WHERE s.sector_name IS NULL
           OR trim(coalesce(s.sector_name,'')) = ''
           OR lower(trim(coalesce(s.sector_name,''))) IN ('nan','none','null','na','n/a','unknown')
           OR trim(coalesce(s.sector_name,'')) = '미분류'
        ORDER BY u.code
        """
    )
    out: List[Tuple[str, str, str]] = []
    for code, name, group_name in cur.fetchall():
        out.append((str(code).strip().upper(), str(name or "").strip(), str(group_name or "").strip()))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill sector_map for 미분류(US) symbols using Wikipedia tables.")
    ap.add_argument("--apply", action="store_true", help="apply DB updates")
    ap.add_argument("--build-csv", action="store_true", help="rebuild data/universe_sectors CSV files after apply")
    args = ap.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))

    targets = _list_unclassified(store)
    if not targets:
        print("No unclassified symbols found.")
        return 0

    sp_map = _fetch_sp500_secmap()
    nas_map = _fetch_nasdaq100_secmap()
    merged = {**nas_map, **sp_map}  # SP500 wins when duplicated

    rows = []
    missing = []
    for code, name, group_name in targets:
        hit = merged.get(code)
        if not hit:
            missing.append((code, name, group_name))
            continue
        sector_name, industry_name, source = hit
        rows.append(
            {
                "code": code,
                "sector_code": None,
                "sector_name": sector_name,
                "industry_code": None,
                "industry_name": industry_name or sector_name,
                "updated_at": datetime.utcnow().isoformat(),
                "source": source,
            }
        )

    print(f"Targets: {len(targets)}  Resolved: {len(rows)}  Missing: {len(missing)}")
    for r in rows:
        print(f" - {r['code']:5s} | {r['sector_name']:<22s} | {r['industry_name'][:50]}")
    if missing:
        print("Missing symbols (need manual mapping):")
        for code, name, group_name in missing:
            print(f" - {code} | {name} | {group_name}")

    if not args.apply:
        print("Dry-run only. Re-run with --apply to write sector_map.")
        return 1 if missing else 0

    if missing:
        print("Refusing to apply because some symbols are unresolved.")
        return 2

    store.upsert_sector_map(rows)
    print(f"✅ sector_map updated: {len(rows)} rows")

    if args.build_csv:
        summary = build_sector_csvs(store, Path("data/universe_sectors"))
        print(
            "✅ sector CSVs built: total={total} unknown={unknown} files={files}".format(**summary)
        )

    # Verify result quickly.
    remaining = _list_unclassified(store)
    print(f"Remaining unclassified: {len(remaining)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
