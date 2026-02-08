import ccxt
from typing import Dict, Optional
from utils.logger import logger
from config import config

class UpbitClient:
    def __init__(self):
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Upbit 클라이언트 초기화"""
        try:
            if config.UPBIT_KEY and config.UPBIT_SECRET:
                self.client = ccxt.upbit({
                    'apiKey': config.UPBIT_KEY,
                    'secret': config.UPBIT_SECRET,
                    'sandbox': False,
                    'enableRateLimit': True,
                })
                logger.info("Upbit 클라이언트 초기화 완료")
            else:
                logger.warning("Upbit API 키가 설정되지 않았습니다")
        except Exception as e:
            logger.error(f"Upbit 클라이언트 초기화 실패: {e}")
    
    def get_balance(self) -> Dict:
        """잔고 조회"""
        if not self.client:
            return {}
        
        try:
            balance = self.client.fetch_balance()
            if isinstance(balance, list):
                logger.error("Upbit 잔고 응답이 리스트 형태로 반환되었습니다. 건너뜁니다.")
                return {}
            # ccxt의 표준 balance 형태로 정규화
            if hasattr(self.client, "safe_balance"):
                balance = self.client.safe_balance(balance)
            return balance if isinstance(balance, dict) else {}
        except Exception as e:
            logger.error(f"Upbit 잔고 조회 실패: {e}")
            return {}
    
    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """시세 조회"""
        if not self.client:
            return None
        
        try:
            ticker = self.client.fetch_ticker(symbol)
            return ticker
        except Exception as e:
            logger.error(f"Upbit 시세 조회 실패 ({symbol}): {e}")
            return None
    
    def place_order(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float] = None) -> Optional[Dict]:
        """주문 실행"""
        if not self.client:
            return None
        
        try:
            order = self.client.create_order(symbol, order_type, side, amount, price)
            logger.info(f"Upbit 주문 실행: {symbol} {side} {amount} {order_type}")
            return order
        except Exception as e:
            logger.error(f"Upbit 주문 실패: {e}")
            return None
