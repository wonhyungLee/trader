import logging
import requests
from typing import Optional


def send_telegram(bot_token: str, chat_id: str, text: str, parse_mode: str = "Markdown") -> bool:
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text, "parse_mode": parse_mode}, timeout=5)
    if not resp.ok:
        logging.warning("telegram send failed: %s", resp.text)
        return False
    return True


def maybe_notify(settings: dict, message: str):
    # Discord 우선, 없으면 텔레그램
    dc = settings.get("discord", {})
    if dc and dc.get("enabled") and dc.get("webhook"):
        try:
            resp = requests.post(dc["webhook"], json={"content": message}, timeout=5)
            if not resp.ok:
                logging.warning("discord send failed: %s", resp.text)
        except Exception:
            logging.exception("discord send failed")
        return

    tg = settings.get("telegram", {})
    if tg and tg.get("enabled"):
        send_telegram(tg.get("token"), tg.get("chat_id"), message)
