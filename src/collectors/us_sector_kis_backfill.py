from __future__ import annotations

import argparse
import logging
import ssl
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from src.brokers.kis_broker import KISBroker
from src.collectors.sector_seed_loader import build_sector_csvs
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


API_URL_INDUSTRY_PRICE = "/uapi/overseas-price/v1/quotations/industry-price"
API_URL_INDUSTRY_THEME = "/uapi/overseas-price/v1/quotations/industry-theme"
API_URL_SEARCH_INFO = "/uapi/overseas-price/v1/quotations/search-info"

TR_ID_INDUSTRY_PRICE = "HHDFS76370100"
TR_ID_INDUSTRY_THEME = "HHDFS76370000"
TR_ID_SEARCH_INFO = "CTPF1702R"

UNKNOWN_TOKENS = {"", "nan", "none", "null", "na", "n/a", "unknown"}
SUPPORTED_EXCDS = ("NAS", "NYS", "AMS")
EXCD_TO_PRDT_TYPE = {"NAS": "512", "NYS": "513", "AMS": "529"}
EXCD_TO_MASTER_PREFIX = {"NAS": "nas", "NYS": "nys", "AMS": "ams"}

MASTER_COLUMNS = [
    "National code",
    "Exchange id",
    "Exchange code",
    "Exchange name",
    "Symbol",
    "realtime symbol",
    "Korea name",
    "English name",
    "Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)",
    "currency",
    "float position",
    "data type",
    "base price",
    "Bid order size",
    "Ask order size",
    "market start time(HHMM)",
    "market end time(HHMM)",
    "DR 여부(Y/N)",
    "DR 국가코드",
    "업종분류코드",
    "지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)",
    "Tick size Type",
    "구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)",
    "Tick size type 상세",
]


def _norm_text(value: object) -> Optional[str]:
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
    if text.lower() in UNKNOWN_TOKENS:
        return None
    return text


def _norm_icod(value: object) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if text.isdigit():
        return text.zfill(3)
    return text


def _load_targets(store: SQLiteStore, only_unknown: bool, limit: Optional[int]) -> List[Tuple[str, str]]:
    if only_unknown:
        where = """
            WHERE u.excd IN ('NAS','NYS','AMS')
              AND (
                   s.sector_name IS NULL
                OR lower(trim(coalesce(s.sector_name,''))) IN ('', 'nan', 'none', 'null', 'na', 'n/a', 'unknown')
              )
        """
    else:
        where = "WHERE u.excd IN ('NAS','NYS','AMS')"

    sql = f"""
        SELECT u.code, u.excd
        FROM universe_members u
        LEFT JOIN sector_map s ON u.code = s.code
        {where}
        ORDER BY u.code
    """
    rows = [(str(r[0]).strip().upper(), str(r[1] or "").strip().upper()) for r in store.conn.execute(sql).fetchall()]
    rows = [(c, e if e in SUPPORTED_EXCDS else "NAS") for c, e in rows if c]
    if limit and limit > 0:
        rows = rows[: int(limit)]
    return rows


def _fetch_industry_codes(broker: KISBroker, excd: str) -> Dict[str, str]:
    res = broker.request(
        TR_ID_INDUSTRY_PRICE,
        f"{broker.base_url}{API_URL_INDUSTRY_PRICE}",
        params={"EXCD": excd, "AUTH": ""},
        priority="LOW",
    )
    items = res.get("output2") or []
    out: Dict[str, str] = {}
    for item in items:
        icod = _norm_icod(item.get("icod"))
        name = _norm_text(item.get("name"))
        if icod and name:
            out[icod] = name
    return out


def _fetch_industry_theme_index(
    broker: KISBroker,
    industry_codes: Dict[str, Dict[str, str]],
) -> Tuple[Dict[Tuple[str, str], Tuple[str, str]], Dict[str, Tuple[str, str, str]]]:
    pair_map: Dict[Tuple[str, str], Tuple[str, str]] = {}
    symbol_map: Dict[str, Tuple[str, str, str]] = {}

    for excd, icod_name_map in industry_codes.items():
        for icod, sector_name in sorted(icod_name_map.items()):
            if icod == "000":
                continue
            try:
                res = broker.request(
                    TR_ID_INDUSTRY_THEME,
                    f"{broker.base_url}{API_URL_INDUSTRY_THEME}",
                    params={"EXCD": excd, "ICOD": icod, "VOL_RANG": "0", "AUTH": "", "KEYB": ""},
                    priority="LOW",
                )
            except Exception as exc:
                logging.warning("industry-theme failed excd=%s icod=%s: %s", excd, icod, exc)
                continue

            items = res.get("output2") or []
            for item in items:
                sym = (_norm_text(item.get("symb")) or "").upper()
                if not sym:
                    continue
                pair_map[(sym, excd)] = (icod, sector_name)
                symbol_map.setdefault(sym, (excd, icod, sector_name))

    return pair_map, symbol_map


def _download_master_df(cache_dir: Path, excd: str, force_refresh: bool) -> pd.DataFrame:
    prefix = EXCD_TO_MASTER_PREFIX[excd]
    zip_path = cache_dir / f"{prefix}mst.cod.zip"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if force_refresh or not zip_path.exists():
        ssl._create_default_https_context = ssl._create_unverified_context
        url = f"https://new.real.download.dws.co.kr/common/master/{prefix}mst.cod.zip"
        urllib.request.urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(cache_dir)
        cod_name = zf.namelist()[0]

    cod_path = cache_dir / cod_name
    return pd.read_table(cod_path, sep="\t", encoding="cp949", header=None, names=MASTER_COLUMNS)


def _build_master_index(
    targets: Iterable[str],
    exchanges: Iterable[str],
    cache_dir: Path,
    force_refresh: bool,
) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str]]:
    target_set = {str(c).strip().upper() for c in targets if str(c).strip()}
    pair_map: Dict[Tuple[str, str], str] = {}
    symbol_map: Dict[str, str] = {}

    for excd in exchanges:
        try:
            df = _download_master_df(cache_dir, excd, force_refresh)
        except Exception as exc:
            logging.warning("master download failed excd=%s: %s", excd, exc)
            continue

        work = df[["Symbol", "업종분류코드"]].copy()
        work["Symbol"] = work["Symbol"].astype(str).str.strip().str.upper()
        work["업종분류코드"] = work["업종분류코드"].apply(_norm_icod)
        work = work[work["Symbol"].isin(target_set)]
        work = work.drop_duplicates(subset=["Symbol"], keep="first")

        for _, row in work.iterrows():
            sym = row["Symbol"]
            icod = row["업종분류코드"] or "000"
            pair_map[(sym, excd)] = icod
            symbol_map.setdefault(sym, icod)
            # Prefer specific code over 000 if duplicated across exchanges.
            if symbol_map.get(sym) == "000" and icod != "000":
                symbol_map[sym] = icod

    return pair_map, symbol_map


def _search_info_sector(broker: KISBroker, code: str, excd: str) -> Tuple[Optional[str], Optional[str]]:
    prdt_type = EXCD_TO_PRDT_TYPE.get(excd)
    if not prdt_type:
        return None, None

    try:
        res = broker.request(
            TR_ID_SEARCH_INFO,
            f"{broker.base_url}{API_URL_SEARCH_INFO}",
            params={"PRDT_TYPE_CD": prdt_type, "PDNO": code},
            priority="LOW",
        )
    except Exception:
        return None, None

    output = res.get("output") or {}
    sector_code = _norm_text(output.get("prdt_clsf_cd"))
    sector_name = _norm_text(output.get("prdt_clsf_name"))
    return sector_code, sector_name


def run_backfill(
    store: SQLiteStore,
    broker: KISBroker,
    only_unknown: bool = True,
    limit: Optional[int] = None,
    cache_dir: str = "data/cache/kis_master",
    force_refresh_master: bool = False,
    fill_unclassified_name: str = "미분류",
) -> Dict[str, int]:
    targets = _load_targets(store, only_unknown=only_unknown, limit=limit)
    if not targets:
        return {"targets": 0, "updated": 0, "theme": 0, "master": 0, "search": 0, "fallback": 0}

    target_codes = [code for code, _ in targets]
    exchanges = sorted({excd for _, excd in targets if excd in SUPPORTED_EXCDS})
    if not exchanges:
        exchanges = list(SUPPORTED_EXCDS)

    industry_codes: Dict[str, Dict[str, str]] = {}
    for excd in exchanges:
        try:
            industry_codes[excd] = _fetch_industry_codes(broker, excd)
        except Exception as exc:
            logging.warning("industry-price failed excd=%s: %s", excd, exc)
            industry_codes[excd] = {}

    theme_pair_map, theme_symbol_map = _fetch_industry_theme_index(broker, industry_codes)
    master_pair_map, master_symbol_map = _build_master_index(
        targets=target_codes,
        exchanges=exchanges,
        cache_dir=Path(cache_dir),
        force_refresh=force_refresh_master,
    )

    # icod -> sector_name (merge all exchanges)
    global_icod_name: Dict[str, str] = {}
    for m in industry_codes.values():
        for icod, name in m.items():
            global_icod_name.setdefault(icod, name)

    rows: List[Dict[str, object]] = []
    stat_theme = 0
    stat_master = 0
    stat_search = 0
    stat_fallback = 0

    for code, excd in targets:
        sector_code: Optional[str] = None
        sector_name: Optional[str] = None
        source: str = "KIS_FALLBACK"

        hit = theme_pair_map.get((code, excd))
        if hit is None:
            sym_hit = theme_symbol_map.get(code)
            if sym_hit is not None:
                _, icod, sname = sym_hit
                hit = (icod, sname)
        if hit is not None:
            sector_code, sector_name = hit
            source = "KIS_INDUSTRY_THEME"
            stat_theme += 1

        if not sector_name:
            icod = master_pair_map.get((code, excd)) or master_symbol_map.get(code)
            if icod:
                sector_code = icod
                if icod == "000":
                    sector_name = fill_unclassified_name
                else:
                    sector_name = global_icod_name.get(icod)
                if not sector_name:
                    sector_name = f"업종코드 {icod}"
                source = "KIS_MASTER_CODE"
                stat_master += 1

        if not sector_name:
            s_code, s_name = _search_info_sector(broker, code, excd)
            if s_name:
                sector_code = s_code or sector_code
                sector_name = s_name
                source = "KIS_SEARCH_INFO"
                stat_search += 1

        if not sector_name:
            sector_code = sector_code or "000"
            sector_name = fill_unclassified_name
            source = "KIS_UNCLASSIFIED"
            stat_fallback += 1

        rows.append(
            {
                "code": code,
                "sector_code": sector_code,
                "sector_name": sector_name,
                "industry_code": sector_code,
                "industry_name": sector_name,
                "source": source,
            }
        )

    store.upsert_sector_map(rows)
    return {
        "targets": len(targets),
        "updated": len(rows),
        "theme": stat_theme,
        "master": stat_master,
        "search": stat_search,
        "fallback": stat_fallback,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill US sector_map using KIS overseas APIs.")
    ap.add_argument("--all", action="store_true", help="update all universe codes, not only unknown")
    ap.add_argument("--limit", type=int, default=None, help="limit target rows")
    ap.add_argument("--cache-dir", default="data/cache/kis_master", help="KIS master file cache dir")
    ap.add_argument("--force-refresh-master", action="store_true", help="redownload KIS master files")
    ap.add_argument("--fill-unclassified-name", default="미분류", help="name for unclassified codes")
    ap.add_argument("--build-csv", action="store_true", help="rebuild data/universe_sectors CSV files")
    args = ap.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    broker = KISBroker(settings)

    summary = run_backfill(
        store=store,
        broker=broker,
        only_unknown=not args.all,
        limit=args.limit,
        cache_dir=args.cache_dir,
        force_refresh_master=bool(args.force_refresh_master),
        fill_unclassified_name=args.fill_unclassified_name,
    )
    print(
        "✅ sector backfill: targets={targets} updated={updated} theme={theme} master={master} "
        "search={search} fallback={fallback}".format(**summary)
    )

    if args.build_csv:
        csv_summary = build_sector_csvs(store, Path("data/universe_sectors"))
        print(
            "✅ sector CSVs built: total={total} unknown={unknown} files={files}".format(**csv_summary)
        )


if __name__ == "__main__":
    main()
