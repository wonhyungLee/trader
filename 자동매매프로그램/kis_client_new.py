import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import requests
from utils.logger import logger


class KISClient:
    """KIS(한국투자증권) 잔고 조회 클라이언트.

    - 키 접두어로 실전/모의를 구분하고 올바른 도메인을 선택한다.
    - 국내 주식 잔고(TR: TTTC8434R / VTTC8434R), 해외 주식 잔고(TR: TTTS3012R / VTTS3012R), 환율(TR: FHKST03030100),
      해외선물옵션 미결제(TR: OTFM1412R)를 조회해 원화 합산 정보를 만든다.
    - TR 연속조회, 토큰 만료 재발급, 헤더 소문자 변환 등을 처리한다.
    """

    _token_cache: Dict[str, Dict[str, object]] = {}

    def __init__(self, key: str, secret: str, account_number: str, account_code: str, kis_number: int = 1):
        self.key = key
        self.secret = secret
        self.account_number = account_number
        self.account_code = account_code
        self.kis_number = kis_number

        # 모의투자는 키가 V로 시작, 실전은 P로 시작
        key_upper = (self.key or "").upper()
        self.is_paper = key_upper.startswith("V")
        self.base_url = (
            "https://openapivts.koreainvestment.com:29443" if self.is_paper else "https://openapi.koreainvestment.com:9443"
        )

        cache_key = self._cache_key()
        if cache_key not in KISClient._token_cache:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json; charset=utf-8"})
            KISClient._token_cache[cache_key] = {"session": session, "access_token": None, "expires_at": None}

        cache_entry = KISClient._token_cache[cache_key]
        self.session: requests.Session = cache_entry["session"]
        self.access_token = cache_entry.get("access_token")
        self.token_expires_at: datetime | None = cache_entry.get("expires_at")

        self._ensure_token()

    def _cache_key(self) -> str:
        return f"{self.base_url}:{self.key}:{self.secret}"

    def _refresh_token(self) -> bool:
        try:
            endpoint = f"{self.base_url}/oauth2/tokenP"
            payload = {"grant_type": "client_credentials", "appkey": self.key, "appsecret": self.secret}
            resp = self.session.post(endpoint, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            access_token = data.get("access_token")
            expires_str = data.get("access_token_token_expired")
            if access_token and expires_str:
                self.access_token = access_token
                self.token_expires_at = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S")
                cache_entry = KISClient._token_cache[self._cache_key()]
                cache_entry["access_token"] = self.access_token
                cache_entry["expires_at"] = self.token_expires_at
                logger.info(f"KIS{self.kis_number}: 토큰 발급 성공 (만료 {expires_str})")
                return True
            raise ValueError(f"토큰 응답이 올바르지 않습니다: {data}")
        except Exception as e:
            logger.error(f"KIS{self.kis_number}: 토큰 발급 실패 - {e}")
            return False

    def _token_valid(self) -> bool:
        if not self.access_token or not self.token_expires_at:
            return False
        return datetime.now() + timedelta(minutes=5) < self.token_expires_at

    def _ensure_token(self) -> bool:
        if self._token_valid():
            return True
        return self._refresh_token()

    def _get_headers(self, tr_id: str, tr_cont: str = "") -> Dict[str, str]:
        return {
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.key,
            "appsecret": self.secret,
            "tr_id": tr_id,
            "custtype": "P",
            "tr_cont": tr_cont,
        }

    def _call_api(self, endpoint: str, tr_id: str, params: Dict, tr_cont: str = "") -> Tuple[Dict, Dict]:
        if not self._ensure_token():
            raise RuntimeError("KIS 토큰 발급 실패")

        headers = self._get_headers(tr_id, tr_cont)
        url = f"{self.base_url}{endpoint}"
        for attempt in range(3):
            resp = self.session.get(url, headers=headers, params=params, timeout=15)
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}

            # 토큰 만료 (EGW00123) → 재발급 후 재시도
            if data.get("msg_cd") == "EGW00123":
                logger.warning(f"KIS{self.kis_number}: 토큰 만료 감지, 재발급 시도")
                if self._refresh_token():
                    headers = self._get_headers(tr_id, tr_cont)
                    continue

            if resp.status_code == 200 and data.get("rt_cd") == "0":
                header_lower = {k.lower(): v for k, v in resp.headers.items()}
                return data, header_lower

            if attempt < 2:
                time.sleep(1 + attempt)
                continue

            raise RuntimeError(
                f"KIS{self.kis_number}: API 오류 ({resp.status_code}) - {data.get('msg_cd') or data}"
            )

        raise RuntimeError("KIS API 호출 실패")

    # ----------------------- 국내 주식 -----------------------
    def get_domestic_balance(self) -> Dict:
        """국내 주식 잔고 (연속조회 포함)."""
        base_params = {
            "CANO": self.account_number,
            "ACNT_PRDT_CD": self.account_code,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",  # 종목별
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        tr_id = "VTTC8434R" if self.is_paper else "TTTC8434R"

        stocks: List[Dict] = []
        total_cost = 0.0
        total_eval = 0.0
        total_krw = 0
        cash = 0

        next_fk = ""
        next_nk = ""
        tr_cont = ""

        for _ in range(10):  # 안전장치: 최대 10 페이지
            params = {**base_params, "CTX_AREA_FK100": next_fk, "CTX_AREA_NK100": next_nk}
            data, headers = self._call_api("/uapi/domestic-stock/v1/trading/inquire-balance", tr_id, params, tr_cont)

            output1 = data.get("output1", []) or []
            output2 = data.get("output2", []) or []

            if output2:
                total_krw = int(float(output2[0].get("tot_evlu_amt", 0) or 0))
                cash = int(float(output2[0].get("ord_psbl_cash", 0) or 0))

            for item in output1:
                qty = float(item.get("hldg_qty", 0) or 0)
                if qty <= 0:
                    continue
                avg = float(item.get("pchs_avg_pric", 0) or 0)
                cur = float(item.get("prpr", 0) or 0)
                eval_amt = float(item.get("evlu_amt", 0) or 0)
                cost = avg * qty
                pnl = eval_amt - cost
                pnl_rate = (pnl / cost * 100) if cost > 0 else 0.0

                total_cost += cost
                total_eval += eval_amt

                stocks.append(
                    {
                        "symbol": item.get("pdno", ""),
                        "name": item.get("prdt_name", ""),
                        "quantity": qty,
                        "average_price": avg,
                        "current_price": cur,
                        "eval_amount": eval_amt,
                        "pnl": pnl,
                        "pnl_rate": pnl_rate,
                    }
                )

            tr_cont = (headers.get("tr_cont") or "").upper()
            next_fk = data.get("ctx_area_fk100", "") or ""
            next_nk = data.get("ctx_area_nk100", "") or ""

            if tr_cont not in ("M", "F"):
                break

            time.sleep(0.2)

        account_pnl = total_eval - total_cost
        account_pnl_rate = (account_pnl / total_cost * 100) if total_cost > 0 else 0.0

        return {
            "total_krw": total_krw,
            "cash": cash,
            "pnl_krw": account_pnl,
            "pnl_rate": account_pnl_rate,
            "stocks": stocks,
        }

    # ----------------------- 환율 -----------------------
    def get_fx_rates(self) -> Dict[str, float]:
        """USD/HKD/JPY 환율 조회 (당일 일봉, 시장분류 X)."""
        today = datetime.now().strftime("%Y%m%d")
        currencies = ["USD", "HKD", "JPY"]
        rates: Dict[str, float] = {}

        for cur in currencies:
            params = {
                "FID_COND_MRKT_DIV_CODE": "X",
                "FID_INPUT_ISCD": cur,
                "FID_INPUT_DATE_1": today,
                "FID_INPUT_DATE_2": today,
                "FID_PERIOD_DIV_CODE": "D",
            }
            try:
                data, _ = self._call_api(
                    "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
                    "FHKST03030100",
                    params,
                    "",
                )
                output1 = data.get("output1") or []
                if isinstance(output1, dict):
                    output1 = [output1]
                if output1:
                    rate = float(output1[-1].get("ovrs_nmix_prpr", 0) or 0)
                    if rate > 0:
                        rates[cur] = rate
            except Exception as e:
                logger.error(f"KIS{self.kis_number} 환율 조회 실패({cur}): {e}")
        return rates

    # ----------------------- 해외 주식 -----------------------
    def get_overseas_balance(self) -> Dict:
        """해외 주식 잔고 (여러 거래소/통화 시도)."""
        markets = [
            ("NASD", "USD"),
            ("NYSE", "USD"),
            ("AMEX", "USD"),
            ("SEHK", "HKD"),
            ("TKSE", "JPY"),
        ]

        tr_id = "VTTS3012R" if self.is_paper else "TTTS3012R"
        endpoint = "/uapi/overseas-stock/v1/trading/inquire-balance"

        totals: Dict[str, Dict[str, float]] = {}
        stocks: List[Dict] = []

        for exchange, currency in markets:
            next_fk = ""
            next_nk = ""
            tr_cont = ""

            for _ in range(6):
                params = {
                    "CANO": self.account_number,
                    "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": exchange,
                    "TR_CRCY_CD": currency,
                    "CTX_AREA_FK200": next_fk,
                    "CTX_AREA_NK200": next_nk,
                }
                try:
                    data, headers = self._call_api(endpoint, tr_id, params, tr_cont)
                except Exception as e:
                    logger.error(f"KIS{self.kis_number} 해외잔고 조회 실패({exchange}/{currency}): {e}")
                    break

                output1 = data.get("output1") or []
                if isinstance(output1, dict):
                    output1 = [output1]

                for item in output1:
                    qty = float(item.get("ovrs_cblc_qty", 0) or 0)
                    if qty <= 0:
                        continue
                    avg = float(item.get("pchs_avg_pric", 0) or 0)
                    cur = float(item.get("now_pric2", 0) or 0)
                    eval_amt = float(item.get("ovrs_stck_evlu_amt", 0) or 0)
                    pnl_amt = float(item.get("frcr_evlu_pfls_amt", 0) or 0)
                    pnl_rate = float(item.get("evlu_pfls_rt", 0) or 0)
                    name = item.get("ovrs_item_name") or item.get("ovrs_item_nm") or item.get("prdt_name", "")
                    symbol = item.get("ovrs_pdno") or item.get("pdno") or ""

                    totals.setdefault(currency, {"total_eval": 0.0, "total_pnl": 0.0})
                    totals[currency]["total_eval"] += eval_amt
                    totals[currency]["total_pnl"] += pnl_amt

                    stocks.append(
                        {
                            "exchange": exchange,
                            "currency": currency,
                            "symbol": symbol,
                            "name": name,
                            "quantity": qty,
                            "average_price": avg,
                            "current_price": cur,
                            "eval_amount": eval_amt,
                            "pnl": pnl_amt,
                            "pnl_rate": pnl_rate,
                        }
                    )

                tr_cont = (headers.get("tr_cont") or "").upper()
                next_fk = data.get("ctx_area_fk200", "") or ""
                next_nk = data.get("ctx_area_nk200", "") or ""
                if tr_cont not in ("M", "F"):
                    break
                time.sleep(0.2)

        fx_rates = self.get_fx_rates()
        krw_total = 0.0
        for cur, vals in totals.items():
            rate = fx_rates.get(cur)
            if rate and vals.get("total_eval"):
                krw_total += vals.get("total_eval", 0.0) * rate

        return {"per_currency": totals, "stocks": stocks, "fx_rates": fx_rates, "total_krw": krw_total}

    # ----------------------- 해외선물/옵션 -----------------------
    def get_overseas_futures_balance(self) -> Dict:
        """해외선물옵션 미결제 잔고 조회 (TR: OTFM1412R)."""
        tr_id = "OTFM1412R"
        endpoint = "/uapi/overseas-futureoption/v1/trading/inquire-unpd"

        positions: List[Dict] = []
        per_currency: Dict[str, Dict[str, float]] = {}

        next_fk = ""
        next_nk = ""
        tr_cont = ""

        for _ in range(6):
            params = {
                "CANO": self.account_number,
                "ACNT_PRDT_CD": self.account_code,
                "FUOP_DVSN": "00",  # 전체
                "CTX_AREA_FK100": next_fk,
                "CTX_AREA_NK100": next_nk,
            }
            try:
                data, headers = self._call_api(endpoint, tr_id, params, tr_cont)
            except Exception as e:
                logger.error(f"KIS{self.kis_number} 해외선물 잔고 조회 실패: {e}")
                break

            output = data.get("output") or []
            if isinstance(output, dict):
                output = [output]

            for item in output:
                currency = item.get("crcy_cd") or ""
                side = item.get("sll_buy_dvsn_cd")  # 매도/매수
                qty = float(item.get("fm_ustl_qty", 0) or 0)
                avg = float(item.get("fm_ccld_avg_pric", 0) or 0)
                cur_price = float(item.get("fm_now_pric", 0) or 0)
                pnl = float(item.get("fm_evlu_pfls_amt", 0) or 0)
                symbol = item.get("ovrs_futr_fx_pdno") or ""

                per_currency.setdefault(currency, {"pnl": 0.0})
                per_currency[currency]["pnl"] += pnl

                positions.append(
                    {
                        "symbol": symbol,
                        "currency": currency,
                        "side": side,
                        "quantity": qty,
                        "average_price": avg,
                        "current_price": cur_price,
                        "pnl": pnl,
                    }
                )

            tr_cont = (headers.get("tr_cont") or "").upper()
            next_fk = data.get("ctx_area_fk100", "") or ""
            next_nk = data.get("ctx_area_nk100", "") or ""
            if tr_cont not in ("M", "F"):
                break
            time.sleep(0.2)

        fx_rates = self.get_fx_rates()
        total_pnl_krw = 0.0
        for cur, vals in per_currency.items():
            rate = fx_rates.get(cur)
            if rate:
                vals["pnl_krw"] = vals.get("pnl", 0.0) * rate
                total_pnl_krw += vals["pnl_krw"]
            else:
                vals["pnl_krw"] = None

        return {"per_currency": per_currency, "positions": positions, "fx_rates": fx_rates, "total_pnl_krw": total_pnl_krw}

    # ----------------------- 통합 -----------------------
    def get_balance(self) -> Dict:
        try:
            domestic = self.get_domestic_balance()
            overseas = self.get_overseas_balance()
            futures = self.get_overseas_futures_balance()
            return {
                "domestic": domestic,
                "overseas": overseas,
                "futures": futures,
                "total_krw": domestic.get("total_krw", 0) + overseas.get("total_krw", 0),
                "pnl_krw": domestic.get("pnl_krw", 0),
                "pnl_rate": domestic.get("pnl_rate", 0),
            }
        except Exception as e:
            logger.error(f"KIS{self.kis_number} 자산 조회 실패: {e}")
            return {"error": str(e)}

    def close(self):
        # 공유 세션이라 즉시 닫지 않는다.
        pass
