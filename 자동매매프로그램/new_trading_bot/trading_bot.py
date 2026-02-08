import asyncio
import signal
import sys
from datetime import datetime, timedelta
from utils.logger import logger
from config import config
from asset_monitor import AssetMonitor
from exchanges.upbit_client import UpbitClient
from exchanges.bitget_client import BitgetClient
from exchanges.bithumb_client import BithumbClient

class TradingBot:
    def __init__(self):
        self.running = False
        self.start_time = None
        self.initial_report_done = False
        self.asset_monitor = AssetMonitor()
        self.upbit = UpbitClient()
        self.bitget = BitgetClient(use_demo=config.BITGET_DEMO_MODE)
        self.bithumb = BithumbClient()
        
        # 종료 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        logger.info(f"종료 시그널 수신: {signum}")
        self.running = False
    
    async def initialize(self):
        """봇 초기화"""
        try:
            logger.info("=== 자동매매봇 초기화 시작 ===")
            
            # 초기 자산 현황 체크
            await self.asset_monitor.generate_asset_report()
            self.initial_report_done = True
            
            self.start_time = datetime.now()
            self.running = True
            
            logger.info(f"봇 초기화 완료 - 시작 시간: {self.start_time}")
            logger.info(f"6시간 후 자동 재시작 예정: {self.start_time + timedelta(hours=config.RESTART_INTERVAL_HOURS)}")
            
        except Exception as e:
            logger.error(f"봇 초기화 실패: {e}")
            raise
    
    async def check_restart_time(self):
        """재시작 시간 체크"""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            if elapsed >= timedelta(hours=config.RESTART_INTERVAL_HOURS):
                logger.info(f"{config.RESTART_INTERVAL_HOURS}시간이 경과하여 재시작을 시작합니다.")
                return True
        return False
    
    async def trading_loop(self):
        """메인 트레이딩 루프"""
        logger.info("=== 트레이딩 루프 시작 ===")
        
        while self.running:
            try:
                # 재시작 시간 체크
                if await self.check_restart_time():
                    logger.info("재시작 시간 도달 - 봇을 종료합니다.")
                    self.running = False
                    break
                
                # 여기에 실제 트레이딩 로직을 구현합니다
                # 예: 시장 분석, 매매 신호 확인, 주문 실행 등
                
                # 현재는 단순히 상태 로깅만 수행
                logger.info(f"트레이딩 봇 실행 중... (가동 시간: {datetime.now() - self.start_time})")
                
                # 30분마다 루프 실행
                await asyncio.sleep(1800)
                
            except Exception as e:
                logger.error(f"트레이딩 루프 오류: {e}")
                await asyncio.sleep(60)  # 1분 후 재시도
    
    async def asset_monitoring_loop(self):
        """자산 모니터링 루프 (별도 태스크)"""
        logger.info("=== 자산 모니터링 루프 시작 ===")

        first_cycle = True
        while self.running:
            try:
                # 초기화에서 이미 자산 리포트를 보냈다면, 첫 루프에서는 바로 보내지 않고 주기만큼 대기해 중복 발송을 피한다.
                if first_cycle and self.initial_report_done:
                    await asyncio.sleep(config.RESTART_INTERVAL_HOURS * 3600)
                    if not self.running:
                        break

                first_cycle = False
                await self.asset_monitor.generate_asset_report()

                # 6시간마다 자산 리포트 생성
                await asyncio.sleep(config.RESTART_INTERVAL_HOURS * 3600)
                
            except Exception as e:
                logger.error(f"자산 모니터링 오류: {e}")
                await asyncio.sleep(300)  # 5분 후 재시도
    
    async def run(self):
        """봇 실행"""
        try:
            await self.initialize()
            
            # 트레이딩 루프와 자산 모니터링을 동시에 실행
            tasks = [
                asyncio.create_task(self.trading_loop()),
                asyncio.create_task(self.asset_monitoring_loop())
            ]
            
            # 모든 태스크 완료 대기
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"봇 실행 오류: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """정리 작업"""
        logger.info("=== 봇 종료 작업 시작 ===")
        
        try:
            # 마지막 자산 리포트
            await self.asset_monitor.generate_asset_report()
            
            end_time = datetime.now()
            runtime = end_time - self.start_time if self.start_time else timedelta(0)
            
            logger.info(f"봇 종료 - 총 실행 시간: {runtime}")
            logger.info("=== 봇 종료 완료 ===")
            
        except Exception as e:
            logger.error(f"종료 작업 오류: {e}")

def main():
    """메인 함수"""
    logger.info("=== 자동매매봇 시작 ===")
    
    bot = TradingBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 봇이 중단되었습니다.")
    except Exception as e:
        logger.error(f"봇 실행 중 오류: {e}")
    finally:
        logger.info("프로그램 종료")

if __name__ == "__main__":
    main()
