import logging
from datetime import datetime
import os

def setup_logger(name="trading_bot"):
    """로거 설정"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 로그 디렉토리 생성
    os.makedirs("logs", exist_ok=True)
    
    # 파일 핸들러
    file_handler = logging.FileHandler(f"logs/{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포매터
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가 (중복 방지)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 전역 로거 인스턴스
logger = setup_logger()