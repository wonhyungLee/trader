#!/usr/bin/env python3
"""
6시간마다 자동으로 재시작하는 매니저 스크립트
"""

import os
import sys
import time
import signal
import subprocess
from datetime import datetime, timedelta
from utils.logger import setup_logger

# 별도 로거 설정 (매니저용)
logger = setup_logger("restart_manager")

class RestartManager:
    def __init__(self):
        self.running = False
        self.process = None
        self.restart_count = 0
        self.next_restart_at = self._next_anchor_time()
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        logger.info(f"매니저 종료 시그널 수신: {signum}")
        self.running = False
        if self.process:
            self._stop_process()
    
    def _stop_process(self):
        """프로세스 중단"""
        if self.process and self.process.poll() is None:
            logger.info("트레이딩 봇 프로세스를 종료합니다.")
            try:
                # SIGTERM으로 정상 종료 시도
                self.process.terminate()
                
                # 10초 대기
                try:
                    self.process.wait(timeout=10)
                    logger.info("트레이딩 봇이 정상적으로 종료되었습니다.")
                except subprocess.TimeoutExpired:
                    # 강제 종료
                    logger.warning("정상 종료 실패, 강제 종료합니다.")
                    self.process.kill()
                    self.process.wait()
                    
            except Exception as e:
                logger.error(f"프로세스 종료 오류: {e}")
            finally:
                self.process = None
    
    def _start_process(self):
        """프로세스 시작"""
        try:
            logger.info("새로운 트레이딩 봇 프로세스를 시작합니다.")
            
            # Python 인터프리터 경로와 스크립트 경로 설정
            python_path = sys.executable
            script_path = os.path.join(os.path.dirname(__file__), "trading_bot.py")
            
            # 새 프로세스 시작
            self.process = subprocess.Popen(
                [python_path, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            self.restart_count += 1
            logger.info(f"트레이딩 봇 프로세스 시작됨 (재시작 횟수: {self.restart_count}, PID: {self.process.pid})")
            
        except Exception as e:
            logger.error(f"프로세스 시작 오류: {e}")
            self.process = None
    
    def _monitor_process(self):
        """프로세스 모니터링"""
        if not self.process:
            return False
        
        # 프로세스 상태 확인
        poll_result = self.process.poll()
        
        if poll_result is not None:
            # 프로세스가 종료됨
            logger.info(f"트레이딩 봇 프로세스가 종료되었습니다. (Exit code: {poll_result})")
            
            # 출력 로그 읽기
            try:
                if self.process.stdout:
                    output = self.process.stdout.read()
                    if output:
                        logger.info(f"프로세스 최종 출력:\n{output}")
            except Exception as e:
                logger.error(f"프로세스 출력 읽기 오류: {e}")
            
            self.process = None
            return False
        
        return True

    def _next_anchor_time(self, now: datetime | None = None) -> datetime:
        """다음 리스타트 기준시각(06/12/18/24시) 반환"""
        if now is None:
            now = datetime.now()
        anchors = [6, 12, 18, 24]
        for h in anchors:
            anchor_dt = now.replace(hour=h % 24, minute=0, second=0, microsecond=0)
            if h == 24:
                anchor_dt += timedelta(days=1)
            if anchor_dt > now:
                return anchor_dt
        # fallback: 다음날 06시
        return now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    def run(self):
        """매니저 메인 루프"""
        logger.info("=== 재시작 매니저 시작 ===")
        self.running = True
        
        while self.running:
            try:
                # 프로세스가 없거나 종료되었으면 새로 시작
                if not self._monitor_process():
                    if self.running:  # 매니저가 종료 중이 아닌 경우에만 재시작
                        logger.info("트레이딩 봇을 재시작합니다.")
                        time.sleep(5)  # 5초 대기 후 재시작
                        self._start_process()
                        self.next_restart_at = self._next_anchor_time()

                # 6시 기준 6시간마다 재시작
                now = datetime.now()
                if now >= self.next_restart_at:
                    logger.info(f"기준시각 도달({self.next_restart_at}), 트레이딩 봇을 재시작합니다.")
                    self._stop_process()
                    time.sleep(2)
                    self._start_process()
                    self.next_restart_at = self._next_anchor_time()
                
                # 10초마다 상태 체크
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"매니저 루프 오류: {e}")
                time.sleep(30)  # 30초 대기 후 재시도
        
        # 종료 시 프로세스 정리
        self._stop_process()
        logger.info("=== 재시작 매니저 종료 ===")

def main():
    """메인 함수"""
    logger.info("자동매매봇 재시작 매니저 시작")
    
    manager = RestartManager()
    
    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 매니저가 중단되었습니다.")
    except Exception as e:
        logger.error(f"매니저 실행 오류: {e}")
    finally:
        logger.info("매니저 프로그램 종료")

if __name__ == "__main__":
    main()
