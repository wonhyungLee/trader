from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


_WEBHOOK_RE = re.compile(r"웹훅\s*주소\s*:?\s*(\S+)")
_PASS_RE = re.compile(r"\"password\"\s*:\s*\"([^\"]+)\"")
_KIS_NO_RE = re.compile(r"\"kis_number\"\s*:\s*\"?([0-9]+)\"?")


@dataclass(frozen=True)
class AutoTradeInfo:
    webhook_url: str = ""
    password: str = ""
    kis_number: str = ""


def load_autotrade_info(path: str) -> AutoTradeInfo:
    p = Path(path).expanduser()
    if not p.exists():
        return AutoTradeInfo()
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return AutoTradeInfo()

    webhook_url = _first_group(_WEBHOOK_RE, text) or ""
    password = _first_group(_PASS_RE, text) or ""
    kis_number = _first_group(_KIS_NO_RE, text) or ""
    return AutoTradeInfo(webhook_url=webhook_url.strip(), password=password.strip(), kis_number=kis_number.strip())


def _first_group(pattern: re.Pattern[str], text: str) -> Optional[str]:
    try:
        m = pattern.search(text)
    except Exception:
        m = None
    if not m:
        return None
    try:
        return m.group(1)
    except Exception:
        return None
