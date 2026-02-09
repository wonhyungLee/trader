from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import requests
from requests import HTTPError, RequestException

from src.utils.config import load_settings
from src.utils.http_retry import is_retryable_status, sleep_backoff


TOKEN_CACHE_DEFAULT = ".cache/kis_token.json"


class KISBroker:
    def __init__(self, settings: Optional[Dict[str, Any]] = None):
        self.settings = settings or load_settings()
        self.env = self.settings.get("kis", {}).get("env", self.settings.get("env", "paper"))
        self.app_key = self.settings["kis"].get("app_key")
        self.app_secret = self.settings["kis"].get("app_secret")
        self.account_no = self.settings["kis"].get("account_no") or self.settings["kis"].get("cano")
        self.account_product = self.settings["kis"].get("acnt_prdt_cd", "01")
        self.custtype = self.settings["kis"].get("custtype", "P")
        self.rate_limit_sleep = float(self.settings["kis"].get("rate_limit_sleep_sec", 0.5))
        self.timeout_connect = float(self.settings["kis"].get("timeout_connect_sec", 5))
        self.timeout_read = float(self.settings["kis"].get("timeout_read_sec", 20))
        self.max_retries = int(self.settings["kis"].get("max_retries", 8))
        self.backoff_base = float(self.settings["kis"].get("backoff_base_sec", 2))
        self.backoff_cap = float(self.settings["kis"].get("backoff_cap_sec", 60))
        self.backoff_jitter = float(self.settings["kis"].get("backoff_jitter_sec", 0.5))
        self.consecutive_error_cooldown_after = int(
            self.settings["kis"].get("consecutive_error_cooldown_after", 10)
        )
        self.consecutive_error_cooldown_sec = float(
            self.settings["kis"].get("consecutive_error_cooldown_sec", 180)
        )
        self.session_reset_every = int(self.settings["kis"].get("session_reset_every", 3))
        self.base_url = self.settings["kis"].get(
            "base_url_prod" if self.env == "prod" else "base_url_paper",
            "https://openapivts.koreainvestment.com:29443",
        )
        self.ws_url = self.settings["kis"].get(
            "ws_url_prod" if self.env == "prod" else "ws_url_paper",
            "ws://ops.koreainvestment.com:21000" if self.env == "prod" else "ws://ops.koreainvestment.com:31000",
        )
        self.token_cache_path = self.settings["kis"].get("token_cache_path", TOKEN_CACHE_DEFAULT)
        os.makedirs(os.path.dirname(self.token_cache_path), exist_ok=True)
        self.session = requests.Session()
        self._token: Optional[str] = None
        self._token_expire: Optional[datetime] = None
        self._consecutive_errors = 0

    # ---------------- Token -----------------
    def _load_token_cache(self):
        if not os.path.exists(self.token_cache_path):
            return
        try:
            with open(self.token_cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._token = data.get("access_token")
            exp = data.get("expires_at")
            if exp:
                self._token_expire = datetime.fromisoformat(exp)
        except Exception:
            return

    def _save_token_cache(self, token: str, expires_at: datetime):
        self._token = token
        self._token_expire = expires_at
        payload = {"access_token": token, "expires_at": expires_at.isoformat()}
        with open(self.token_cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def ensure_token(self) -> str:
        if not self._token:
            self._load_token_cache()
        if self._token and self._token_expire and self._token_expire > datetime.utcnow() + timedelta(minutes=5):
            return self._token
        token, exp = self.issue_token()
        self._save_token_cache(token, exp)
        return token

    def issue_token(self):
        url = f"{self.base_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        resp = self.session.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token") or data.get("access_token_token") or data.get("approval_key")
        exp_sec = int(data.get("expires_in", 3600))
        expires_at = datetime.utcnow() + timedelta(seconds=exp_sec)
        return token, expires_at

    def issue_ws_approval(self) -> str:
        url = f"{self.base_url}/oauth2/Approval"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        resp = self.session.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        key = data.get("approval_key")
        if not key:
            raise RuntimeError(f"ws approval_key missing: {data}")
        return key

    # --------------- Base request ---------------
    def request(
        self,
        tr_id: str,
        url: str,
        method: str = "GET",
        params=None,
        data=None,
        json_body=None,
        max_retries: Optional[int] = None,
    ) -> Dict[str, Any]:
        method = method.upper()
        last_exc: Optional[Exception] = None
        token_refreshed = False
        retries = self.max_retries if max_retries is None else max(1, int(max_retries))

        for attempt in range(1, max(1, retries) + 1):
            if attempt > 1 and self.session_reset_every > 0 and (attempt - 1) % self.session_reset_every == 0:
                self.session = requests.Session()

            token = self.ensure_token()
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": tr_id,
            }
            if self.custtype:
                headers["custtype"] = self.custtype

            try:
                time.sleep(self.rate_limit_sleep)
                timeout = (self.timeout_connect, self.timeout_read)
                if method == "GET":
                    resp = self.session.get(url, headers=headers, params=params, timeout=timeout)
                else:
                    resp = self.session.post(url, headers=headers, params=params, data=data, json=json_body, timeout=timeout)

                if resp.status_code in (401, 403) and not token_refreshed:
                    token, exp = self.issue_token()
                    self._save_token_cache(token, exp)
                    token_refreshed = True
                    continue

                if is_retryable_status(resp.status_code):
                    raise HTTPError(f"{resp.status_code} retryable", response=resp)

                resp.raise_for_status()
                self._consecutive_errors = 0
                try:
                    return resp.json()
                except Exception:
                    return {"text": resp.text}
            except HTTPError as exc:
                status = exc.response.status_code if getattr(exc, "response", None) else None
                if not is_retryable_status(status):
                    raise
                last_exc = exc
            except RequestException as exc:
                last_exc = exc

            self._consecutive_errors += 1
            if (
                self.consecutive_error_cooldown_after > 0
                and self._consecutive_errors >= self.consecutive_error_cooldown_after
            ):
                logging.warning(
                    "KIS consecutive errors=%s, cooldown %.1fs",
                    self._consecutive_errors,
                    self.consecutive_error_cooldown_sec,
                )
                time.sleep(self.consecutive_error_cooldown_sec)
                self._consecutive_errors = 0

            if attempt < retries:
                delay = sleep_backoff(attempt, self.backoff_base, self.backoff_cap, self.backoff_jitter)
                logging.warning("KIS retry %s/%s in %.1fs (%s)", attempt, retries, delay, tr_id)

        if last_exc:
            raise last_exc
        raise RuntimeError("request failed")

    # --------------- Trading ---------------
    def _tr_id(self, paper_code: str, prod_code: str) -> str:
        return paper_code if self.env == "paper" else prod_code

    def send_order(self, code: str, side: str, qty: int, price: Optional[float] = None, ord_dvsn: str = "01") -> Dict[str, Any]:
        # side: BUY/SELL
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = self._tr_id("VTTC0802U", "TTTC0802U") if side.upper() == "BUY" else self._tr_id("VTTC0801U", "TTTC0801U")
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product,
            "PDNO": code,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price or 0),
        }
        res = self.request(tr_id, url, method="POST", json_body=body)
        return res

    def cancel_order(self, code: str, qty: int, orgn_odno: str, ord_orgno: str, ord_dvsn: str = "01") -> Dict[str, Any]:
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
        tr_id = self._tr_id("VTTC0803U", "TTTC0803U")
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product,
            "KRX_FWDG_ORD_ORGNO": ord_orgno,
            "ORGN_ODNO": orgn_odno,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(qty),
            "RVSE_CNCL_DVSN_CD": "02",  # 취소
            "PDNO": code,
            "ORD_UNPR": "0",
        }
        return self.request(tr_id, url, method="POST", json_body=body)

    def get_orders(self, start_date: str, end_date: str) -> Dict[str, Any]:
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_id = self._tr_id("VTTC8001R", "TTTC8001R")
        params = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product,
            "INQR_STRT_DT": start_date.replace("-", ""),
            "INQR_END_DT": end_date.replace("-", ""),
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        return self.request(tr_id, url, params=params)

    def get_balance(self) -> Dict[str, Any]:
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = self._tr_id("VTTC8434R", "TTTC8434R")
        params = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "00",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        return self.request(tr_id, url, params=params)

    def get_current_price(self, code: str) -> Dict[str, Any]:
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }
        return self.request(tr_id, url, params=params)

    def get_multi_price(self, codes: list[str]) -> Dict[str, Any]:
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/intstock-multprice"
        tr_id = "FHKST11300006"
        params: Dict[str, Any] = {}
        for idx, code in enumerate(codes[:30], start=1):
            params[f"FID_COND_MRKT_DIV_CODE_{idx}"] = "J"
            params[f"FID_INPUT_ISCD_{idx}"] = code
        return self.request(tr_id, url, params=params)


if __name__ == "__main__":
    broker = KISBroker()
    print(broker.issue_token())
