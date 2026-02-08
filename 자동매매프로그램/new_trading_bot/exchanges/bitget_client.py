import ccxt
from typing import Dict, Optional
from utils.logger import logger
from config import config

class BitgetClient:
    def __init__(self, use_demo=True):
        self.client = None
        self.use_demo = use_demo
        self._initialize_client()
    
    def _initialize_client(self):
        """Bitget 클라이언트 초기화 (V2 API 사용)"""
        try:
            base_params = {
                'enableRateLimit': True,
                'version': 'v2',  # V1 폐기 대비
                'options': {
                    'defaultType': 'spot',
                }
            }
            if self.use_demo and config.BITGET_DEMO_MODE:
                # 데모 계정 사용
                if config.BITGET_DEMO_KEY and config.BITGET_DEMO_SECRET:
                    self.client = ccxt.bitget({
                        **base_params,
                        'apiKey': config.BITGET_DEMO_KEY,
                        'secret': config.BITGET_DEMO_SECRET,
                        'password': config.BITGET_DEMO_PASSPHRASE,
                        'sandbox': True,
                    })
                    logger.info("Bitget 데모 클라이언트 초기화 완료 (V2)")
                else:
                    logger.warning("Bitget 데모 API 키가 설정되지 않았습니다")
            else:
                # 실제 계정 사용
                if config.BITGET_KEY and config.BITGET_SECRET:
                    self.client = ccxt.bitget({
                        **base_params,
                        'apiKey': config.BITGET_KEY,
                        'secret': config.BITGET_SECRET,
                        'password': config.BITGET_PASSPHRASE,
                        'sandbox': False,
                    })
                    logger.info("Bitget 실계좌 클라이언트 초기화 완료 (V2)")
                else:
                    logger.warning("Bitget API 키가 설정되지 않았습니다")
        except Exception as e:
            logger.error(f"Bitget 클라이언트 초기화 실패: {e}")
    
    def get_balance(self) -> Dict:
        """잔고 조회"""
        if not self.client:
            return {}
        
        try:
            balance = self.client.fetch_balance()
            return balance
        except Exception as e:
            logger.error(f"Bitget 잔고 조회 실패: {e}")
            return {}
    
    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """시세 조회"""
        if not self.client:
            return None
        
        try:
            ticker = self.client.fetch_ticker(symbol)
            return ticker
        except Exception as e:
            logger.error(f"Bitget 시세 조회 실패 ({symbol}): {e}")
            return None
    
    def place_order(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float] = None) -> Optional[Dict]:
        """주문 실행"""
        if not self.client:
            return None
        
        try:
            order = self.client.create_order(symbol, order_type, side, amount, price)
            logger.info(f"Bitget 주문 실행: {symbol} {side} {amount} {order_type}")
            return order
        except Exception as e:
            logger.error(f"Bitget 주문 실패: {e}")
            return None
