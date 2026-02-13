from __future__ import annotations

import argparse
import logging
import ssl
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


EXCD_PREFIX = {"NAS": "nas", "NYS": "nys", "AMS": "ams"}
EXCD_PRIORITY = ("NAS", "NYS", "AMS")


def _download_master_zip(cache_dir: Path, excd: str, force_refresh: bool) -> Path:
    prefix = EXCD_PREFIX[excd]
    zip_path = cache_dir / f"{prefix}mst.cod.zip"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if force_refresh or not zip_path.exists():
        ssl._create_default_https_context = ssl._create_unverified_context
        url = f"https://new.real.download.dws.co.kr/common/master/{prefix}mst.cod.zip"
        logging.info("downloading master: %s", url)
        urllib.request.urlretrieve(url, zip_path)

    return zip_path


def _extract_cod(zip_path: Path, cache_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(cache_dir)
        cod_name = zf.namelist()[0]
    return cache_dir / cod_name


def _load_master_symbols(cod_path: Path) -> Set[str]:
    # Master files are tab-separated, CP949 encoded. Symbol is the 5th column.
    out: Set[str] = set()
    with cod_path.open("r", encoding="cp949", errors="ignore") as f:
        for raw in f:
            parts = raw.rstrip("\n").split("\t")
            if len(parts) <= 4:
                continue
            sym = (parts[4] or "").strip().upper()
            if sym:
                out.add(sym)
    return out


def load_exchange_symbol_sets(
    cache_dir: Path = Path("data/cache/kis_master"),
    force_refresh: bool = False,
    exchanges: Sequence[str] = EXCD_PRIORITY,
) -> Dict[str, Set[str]]:
    sets: Dict[str, Set[str]] = {}
    for excd in exchanges:
        if excd not in EXCD_PREFIX:
            continue
        zip_path = _download_master_zip(cache_dir, excd, force_refresh)
        cod_path = _extract_cod(zip_path, cache_dir)
        sets[excd] = _load_master_symbols(cod_path)
        logging.info("master loaded excd=%s symbols=%s file=%s", excd, len(sets[excd]), cod_path.name)
    return sets


def _code_to_master_symbol(code: str) -> str:
    # KIS master (and several overseas endpoints) use slash for class shares (e.g., BRK/B).
    return str(code).strip().upper().replace(".", "/")


def resolve_excd(code: str, current_excd: Optional[str], symbol_sets: Dict[str, Set[str]]) -> Optional[str]:
    sym = _code_to_master_symbol(code)
    candidates = [excd for excd, s in symbol_sets.items() if sym in s]
    if not candidates:
        return None
    cur = (current_excd or "").strip().upper()
    if cur in candidates:
        return cur
    # Stable priority for ambiguous matches.
    for excd in EXCD_PRIORITY:
        if excd in candidates:
            return excd
    return candidates[0]


def backfill_universe_excd(
    store: SQLiteStore,
    symbol_sets: Dict[str, Set[str]],
    apply: bool,
    limit: Optional[int] = None,
) -> Dict[str, int]:
    cur = store.conn.execute("SELECT code, excd FROM universe_members ORDER BY code")
    rows = [(str(r[0]).strip().upper(), (r[1] or "").strip().upper()) for r in cur.fetchall()]
    if limit and limit > 0:
        rows = rows[: int(limit)]

    updates: List[Tuple[str, str]] = []
    unresolved = 0
    for code, excd in rows:
        resolved = resolve_excd(code, excd, symbol_sets)
        if not resolved:
            unresolved += 1
            continue
        if resolved != excd:
            updates.append((code, resolved))

    logging.info("universe_members: total=%s unresolved=%s updates=%s", len(rows), unresolved, len(updates))

    if not apply or not updates:
        return {"total": len(rows), "unresolved": unresolved, "updated": 0}

    now = datetime.utcnow().isoformat()
    store.conn.executemany(
        "UPDATE universe_members SET excd=?, updated_at=? WHERE code=?",
        [(new_excd, now, code) for (code, new_excd) in updates],
    )
    # Keep optional table in sync (if present).
    try:
        store.conn.executemany(
            "UPDATE ovrs_stock_info SET excd=?, updated_at=? WHERE code=?",
            [(new_excd, now, code) for (code, new_excd) in updates],
        )
    except Exception:
        pass
    store.conn.commit()
    return {"total": len(rows), "unresolved": unresolved, "updated": len(updates)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="DB에 실제로 반영")
    ap.add_argument("--limit", type=int, default=None, help="테스트용 처리 개수 제한")
    ap.add_argument("--cache-dir", type=str, default="data/cache/kis_master", help="마스터 캐시 폴더")
    ap.add_argument("--force-refresh-master", action="store_true", help="마스터 ZIP 재다운로드")
    args = ap.parse_args()

    settings = load_settings()
    db_path = settings.get("database", {}).get("path", "data/market_data.db")
    store = SQLiteStore(db_path)

    symbol_sets = load_exchange_symbol_sets(
        cache_dir=Path(args.cache_dir),
        force_refresh=bool(args.force_refresh_master),
    )
    result = backfill_universe_excd(store, symbol_sets, apply=bool(args.apply), limit=args.limit)
    logging.info("done: %s", result)


if __name__ == "__main__":
    main()

