import json
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Tuple

from utils.logger import logger

# 캐시 파일 경로
CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_PATH = CACHE_DIR / "fx_rates.json"
MAX_HISTORY = 5


def _load_cache(today: str, allow_stale: bool = False) -> Tuple[Dict[str, float], Dict]:
    """캐시에서 환율을 읽는다. allow_stale=True이면 가장 최근 스냅샷을 날짜와 무관하게 돌려준다."""

    if not CACHE_PATH.exists():
        return {}, {}

    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        date = data.get("date")
        rates = data.get("rates", {}) or {}
        meta = data.get("meta", {}) or {}

        if rates and date == today:
            meta = {
                **meta,
                "source": meta.get("source", "cache"),
                "from_cache": True,
                "date": date,
            }
            return rates, meta

        if allow_stale:
            history = data.get("history") or []
            latest = history[-1] if history else None

            # 가장 최근 history 우선, 없으면 본문 rates라도 사용
            if latest and latest.get("rates"):
                rates = latest.get("rates", {}) or {}
                date = latest.get("date") or date
                meta = {
                    "source": latest.get("source") or meta.get("source") or "cache",
                    "from_cache": True,
                    "date": date,
                    "stale": date != today,
                }
                return rates, meta

            if rates:
                meta = {
                    **meta,
                    "source": meta.get("source", "cache"),
                    "from_cache": True,
                    "date": date,
                    "stale": date != today,
                }
                return rates, meta

        return {}, {}

    except Exception as e:
        logger.warning(f"FX 캐시 읽기 실패: {e}")
        return {}, {}


def _save_cache(rates: Dict[str, float], source: str, today: str):
    """최신 환율 스냅샷과 최근 MAX_HISTORY개 기록을 저장."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        history = []
        if CACHE_PATH.exists():
            try:
                with open(CACHE_PATH, "r", encoding="utf-8") as f:
                    prev = json.load(f)
                    history = prev.get("history", [])
            except Exception:
                history = []

        history.append(
            {
                "ts": datetime.now().isoformat(),
                "date": today,
                "source": source,
                "rates": rates,
            }
        )
        history = history[-MAX_HISTORY:]

        payload = {
            "date": today,
            "rates": rates,
            "meta": {"source": source, "from_cache": False, "date": today},
            "history": history,
        }
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"FX 캐시 저장 실패: {e}")


def get_rates(
    fetch_tr_fn: Callable[[], Dict[str, float]],
    fetch_mcp_fn: Callable[[], Dict[str, float]] | None = None,
) -> Tuple[Dict[str, float], Dict]:
    """
    환율을 MCP -> TR -> 캐시 순으로 조회.
    fetch_tr_fn: 기존 TR 호출 함수
    fetch_mcp_fn: MCP 호출 함수 (없으면 TR 우선)
    Returns: (rates, meta)
      meta = {"source": "mcp"|"tr"|"cache"|None, "from_cache": bool, "date": YYYYMMDD}
    """
    today = datetime.now().strftime("%Y%m%d")
    meta: Dict[str, object] = {"source": None, "from_cache": False, "date": today}

    # 0) 캐시(웹훅) 우선: 같은 날짜면 바로 반환
    cached_rates, cached_meta = _load_cache(today, allow_stale=True)
    if cached_rates and not cached_meta.get("stale"):
        meta = {
            **cached_meta,
            "source": cached_meta.get("source", "cache"),
            "from_cache": True,
            "date": cached_meta.get("date", today),
        }
        return cached_rates, meta

    # 1) MCP 우선
    if fetch_mcp_fn:
        try:
            rates = fetch_mcp_fn() or {}
            if rates:
                meta["source"] = "mcp"
                _save_cache(rates, "mcp", today)
                return rates, meta
        except Exception as e:
            logger.warning(f"FX MCP 조회 실패: {e}")

    # 2) TR 호출
    try:
        rates = fetch_tr_fn() or {}
        if rates:
            meta["source"] = "tr"
            _save_cache(rates, "tr", today)
            return rates, meta
    except Exception as e:
        logger.warning(f"FX TR 조회 실패: {e}")

    # 3) 캐시 활용 (stale라도 마지막 스냅샷 사용)
    if cached_rates:
        meta = {
            **meta,
            **cached_meta,
            "source": cached_meta.get("source", "cache"),
            "from_cache": True,
            "date": cached_meta.get("date", today),
        }
        logger.info("FX 캐시 사용: %s", cached_rates)
        return cached_rates, meta

    logger.warning("FX 조회 실패: MCP/TR/캐시 모두 비어 있음")
    return {}, meta
