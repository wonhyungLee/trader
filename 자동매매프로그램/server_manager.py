#!/usr/bin/env python3
"""
POA Bot 서버 자동 관리 스크립트
6시간마다 자산조회와 자동매매 서버를 재시작합니다.
"""
import asyncio
import subprocess
import os
import signal
import time
import logging
from datetime import datetime
from pathlib import Path
from dotenv import dotenv_values

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ServerManager:
    def __init__(self):
        # 서버 프로세스 저장
        self.server_process = None
        self.asset_monitor_process = None
        
        # 백업 폴더 경로
        self.backup_path = Path('/home/ubuntu/POA-v7_backup_20250721')
        
        # 환경 변수 설정 (.env 기반)
        self.env_file = self.backup_path / '.env'
        self.env_vars = {}
        self._load_env_vars()

    def _load_env_vars(self):
        """항상 최신 .env를 반영하도록 파일을 다시 읽는다."""
        if self.env_file.exists():
            self.env_vars = {k: v for k, v in dotenv_values(self.env_file).items() if v is not None}
            logger.info(f".env 로드 완료: {self.env_file}")
        else:
            self.env_vars = {}
            logger.warning(f".env 파일을 찾지 못했습니다: {self.env_file}")
        
    def set_environment(self):
        """환경 변수 설정"""
        self._load_env_vars()
        if self.env_vars:
            os.environ.update(self.env_vars)
            logger.info("환경 변수 설정 완료 (.env 적용)")
        else:
            logger.warning("적용할 .env 환경 변수가 없습니다")
    
    def stop_processes(self):
        """실행 중인 프로세스들 종료"""
        try:
            if self.server_process and self.server_process.poll() is None:
                logger.info("자동매매 서버 종료 중...")
                self.server_process.terminate()
                try:
                    self.server_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.server_process.kill()
                    self.server_process.wait()
                logger.info("자동매매 서버 종료 완료")
            
            if self.asset_monitor_process and self.asset_monitor_process.poll() is None:
                logger.info("자산조회 프로세스 종료 중...")
                self.asset_monitor_process.terminate()
                try:
                    self.asset_monitor_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.asset_monitor_process.kill()
                    self.asset_monitor_process.wait()
                logger.info("자산조회 프로세스 종료 완료")
                
        except Exception as e:
            logger.error(f"프로세스 종료 중 오류: {e}")
            
    def start_trading_server(self):
        """자동매매 서버 시작"""
        try:
            os.chdir(self.backup_path)
            
            # Python 환경에서 서버 시작
            env = os.environ.copy()
            env.update(self.env_vars)
            
            self.server_process = subprocess.Popen(
                ['python3', 'run.py'],
                cwd=str(self.backup_path),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            logger.info(f"자동매매 서버 시작 완료 (PID: {self.server_process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"자동매매 서버 시작 실패: {e}")
            return False
    
    def start_asset_monitor(self):
        """자산조회 모니터 시작"""
        try:
            os.chdir(self.backup_path)
            
            env = os.environ.copy()
            env.update(self.env_vars)
            
            self.asset_monitor_process = subprocess.Popen(
                ['python3', 'asset_monitor.py'],
                cwd=str(self.backup_path),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            logger.info(f"자산조회 모니터 시작 완료 (PID: {self.asset_monitor_process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"자산조회 모니터 시작 실패: {e}")
            return False
    
    def check_processes_health(self):
        """프로세스 상태 확인"""
        server_alive = self.server_process and self.server_process.poll() is None
        monitor_alive = self.asset_monitor_process and self.asset_monitor_process.poll() is None
        
        logger.info(f"프로세스 상태 - 자동매매서버: {'실행중' if server_alive else '중지됨'}, 자산조회: {'실행중' if monitor_alive else '중지됨'}")
        
        return server_alive, monitor_alive
    
    def restart_servers(self):
        """서버들 재시작"""
        logger.info("=" * 50)
        logger.info("서버 재시작 시작")
        logger.info("=" * 50)
        
        # 환경 변수 설정
        self.set_environment()
        
        # 기존 프로세스 종료
        self.stop_processes()
        
        # 잠시 대기
        time.sleep(3)
        
        # 자동매매 서버 시작
        if self.start_trading_server():
            logger.info("자동매매 서버 재시작 성공")
        else:
            logger.error("자동매매 서버 재시작 실패")
        
        # 잠시 대기 후 자산조회 시작
        time.sleep(5)
        
        if self.start_asset_monitor():
            logger.info("자산조회 모니터 재시작 성공")
        else:
            logger.error("자산조회 모니터 재시작 실패")
        
        logger.info("서버 재시작 완료")
        logger.info("=" * 50)
    
    async def run_periodic_restart(self, interval_hours=6):
        """주기적으로 서버 재시작 실행"""
        logger.info(f"서버 자동 관리 시작 - {interval_hours}시간 주기")
        
        # 첫 시작
        self.restart_servers()
        
        while True:
            try:
                # 6시간 대기
                await asyncio.sleep(interval_hours * 3600)
                
                # 서버 재시작
                self.restart_servers()
                
                # 프로세스 상태 확인
                await asyncio.sleep(10)
                self.check_processes_health()
                
            except KeyboardInterrupt:
                logger.info("키보드 인터럽트 감지, 종료 중...")
                break
            except Exception as e:
                logger.error(f"주기적 재시작 중 오류: {e}")
                await asyncio.sleep(60)  # 1분 후 재시도
        
        # 정리
        self.stop_processes()
        logger.info("서버 관리 종료")

def signal_handler(signum, frame):
    """시그널 핸들러"""
    logger.info(f"시그널 {signum} 수신, 종료 중...")
    exit(0)

async def main():
    """메인 실행 함수"""
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    manager = ServerManager()
    
    # 6시간마다 서버 재시작
    await manager.run_periodic_restart(interval_hours=6)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("프로그램 종료")
    except Exception as e:
        logger.error(f"실행 중 오류: {e}")
