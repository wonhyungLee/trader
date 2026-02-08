import ccxt
from typing import Dict, Optional
from utils.logger import logger
from config import config

class BithumbClient:
    def __init__(self):
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Bithumb 클라이언트 초기화"""
        try:
            if config.BITHUMB_KEY and config.BITHUMB_SECRET:
                self.client = ccxt.bithumb({
                    'apiKey': config.BITHUMB_KEY,
                    'secret': config.BITHUMB_SECRET,
                    'sandbox': False,
                    'enableRateLimit': True,
                })
                logger.info("Bithumb 클라이언트 초기화 완료")
            else:
                logger.warning("Bithumb API 키가 설정되지 않았습니다")
        except Exception as e:
            logger.error(f"Bithumb 클라이언트 초기화 실패: {e}")
    
    def get_balance(self) -> Dict:
        """잔고 조회"""
        if not self.client:
            return {}
        
        try:
            balance = self.client.fetch_balance()
            return balance
        except Exception as e:
            logger.error(f"Bithumb 잔고 조회 실패: {e}")
            return {}
    
    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """시세 조회"""
        if not self.client:
            return None
        
        try:
            ticker = self.client.fetch_ticker(symbol)
            return ticker
        except Exception as e:
            logger.error(f"Bithumb 시세 조회 실패 ({symbol}): {e}")
            return None
    
    def place_order(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float] = None) -> Optional[Dict]:
        """주문 실행"""
        if not self.client:
            return None
        
        try:
            order = self.client.create_order(symbol, order_type, side, amount, price)
            logger.info(f"Bithumb 주문 실행: {symbol} {side} {amount} {order_type}")
            return order
        except Exception as e:
            logger.error(f"Bithumb 주문 실패: {e}")
            return None