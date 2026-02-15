from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return None


def fetch_current_price_us(code: str) -> Dict[str, Any]:
    try:
        return _fetch_stooq_current_price(code)
    except Exception as stooq_exc:
        try:
            return _fetch_yahoo_current_price(code)
        except Exception as yahoo_exc:
            logging.warning("[autotrade] quote fetch failed for %s: stooq=%s yahoo=%s", code, stooq_exc, yahoo_exc)
            return {"code": code, "price": None, "asof": None, "source": "none"}


def _fetch_stooq_current_price(code: str) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper()
    if not symbol:
        raise ValueError("empty symbol")
    stooq_symbol = f"{symbol}.US".lower()
    resp = requests.get(
        "https://stooq.com/q/l/",
        params={"s": stooq_symbol, "i": "1"},
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            "Accept": "text/plain",
        },
        timeout=(4, 8),
    )
    resp.raise_for_status()
    raw = (resp.text or "").strip()
    if not raw or raw.upper().startswith("N/D"):
        raise RuntimeError(f"stooq no data for {symbol}")

    # format: SYMBOL,YYYYMMDD,HHMMSS,OPEN,HIGH,LOW,CLOSE,VOLUME,...
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 7:
        raise RuntimeError(f"unexpected stooq format: {raw[:80]}")

    price = _safe_float(parts[6])
    d = parts[1] if len(parts) > 1 else ""
    t = parts[2] if len(parts) > 2 else ""
    asof = None
    if len(d) == 8 and len(t) == 6 and d.isdigit() and t.isdigit():
        try:
            asof = datetime.strptime(d + t, "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            asof = None
    if not asof:
        asof = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "code": symbol,
        "price": price,
        "asof": asof,
        "source": "stooq",
    }


def _fetch_yahoo_current_price(code: str) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper().replace(".", "-")
    if not symbol:
        raise ValueError("empty symbol")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    resp = requests.get(
        url,
        params={"range": "1d", "interval": "1m"},
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept": "application/json",
        },
        timeout=(4, 8),
    )
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    chart = payload.get("chart") or {}
    result_list = chart.get("result") or []
    if not result_list:
        raise RuntimeError(f"no result for symbol={symbol}")
    result = result_list[0] if isinstance(result_list, list) else result_list
    meta = result.get("meta") or {}
    indicators = (result.get("indicators") or {}).get("quote") or [{}]
    quote = indicators[0] if isinstance(indicators, list) and indicators else {}
    closes = quote.get("close") or []

    price = None
    for value in reversed(closes):
        if value is None:
            continue
        price = _safe_float(value)
        if price is not None:
            break
    if price is None:
        price = _safe_float(meta.get("regularMarketPrice"))

    market_time = meta.get("regularMarketTime")
    if market_time:
        asof = datetime.utcfromtimestamp(int(market_time)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        asof = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "code": symbol,
        "price": price,
        "asof": asof,
        "source": "yahoo",
    }

