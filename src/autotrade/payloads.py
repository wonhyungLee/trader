from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional


def infer_quote_currency(code: str) -> str:
    # KR numeric codes -> KRW. Otherwise assume US ticker -> USD.
    return "KRW" if str(code or "").strip().isdigit() else "USD"


def infer_exchange(excd: Optional[str], code: str) -> str:
    if str(code or "").strip().isdigit():
        return "KRX"
    excd = str(excd or "").strip().upper()
    if excd in {"NAS", "NASDAQ"}:
        return "NASDAQ"
    if excd in {"NYS", "NYSE"}:
        return "NYSE"
    if excd in {"AMS", "AMEX"}:
        # Webhook server may not support AMEX; fallback to NYSE.
        return "NYSE"
    return "NASDAQ"

_PRICE_Q_1DP = Decimal("0.1")


def format_price_1dp_trunc(value: float) -> str:
    """Format price with exactly 1 decimal place (truncate, not round)."""
    d = Decimal(str(value)).quantize(_PRICE_Q_1DP, rounding=ROUND_DOWN)
    return format(d, "f")


def build_limit_order(
    *,
    password: str,
    exchange: str,
    base: str,
    quote: str,
    side: str,
    amount: int,
    price: float,
    order_name: str,
    kis_number: str,
) -> Dict[str, str]:
    return {
        "password": str(password),
        "exchange": str(exchange),
        "base": str(base),
        "quote": str(quote),
        "side": str(side).lower(),
        "type": "limit",
        "amount": str(int(amount)),
        "price": format_price_1dp_trunc(float(price)),
        "percent": "NaN",
        "order_name": str(order_name),
        "kis_number": str(kis_number),
    }


def build_market_sell_all(
    *,
    password: str,
    exchange: str,
    base: str,
    quote: str,
    amount: int,
    price: float,
    order_name: str,
    kis_number: str,
) -> Dict[str, str]:
    return {
        "password": str(password),
        "exchange": str(exchange),
        "base": str(base),
        "quote": str(quote),
        "side": "sell",
        "type": "market",
        "amount": str(int(amount)),
        "price": format_price_1dp_trunc(float(price)),
        "percent": "100",
        "order_name": str(order_name),
        "kis_number": str(kis_number),
    }
