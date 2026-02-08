import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Upbit 설정
    UPBIT_KEY = os.getenv("UPBIT_KEY")
    UPBIT_SECRET = os.getenv("UPBIT_SECRET")
    
    # Bitget 설정
    BITGET_KEY = os.getenv("BITGET_KEY")
    BITGET_SECRET = os.getenv("BITGET_SECRET")
    BITGET_PASSPHRASE = os.getenv("BITGET_PASSPHRASE")
    
    # Bitget Demo 설정
    BITGET_DEMO_MODE = os.getenv("BITGET_DEMO_MODE", "true").lower() == "true"
    BITGET_DEMO_KEY = os.getenv("BITGET_DEMO_KEY")
    BITGET_DEMO_SECRET = os.getenv("BITGET_DEMO_SECRET")
    BITGET_DEMO_PASSPHRASE = os.getenv("BITGET_DEMO_PASSPHRASE")
    
    # Bithumb 설정
    BITHUMB_KEY = os.getenv("BITHUMB_KEY")
    BITHUMB_SECRET = os.getenv("BITHUMB_SECRET")
    
    # KIS 계좌 설정 (1-50)
    KIS_ACCOUNTS = {}
    for i in range(1, 51):
        key = os.getenv(f"KIS{i}_KEY")
        secret = os.getenv(f"KIS{i}_SECRET")
        account_number = os.getenv(f"KIS{i}_ACCOUNT_NUMBER")
        account_code = os.getenv(f"KIS{i}_ACCOUNT_CODE")
        
        if key and secret and account_number and account_code:
            KIS_ACCOUNTS[f"KIS{i}"] = {
                "key": key,
                "secret": secret,
                "account_number": account_number,
                "account_code": account_code
            }
    
    # 디스코드 웹훅
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    
    # 재시작 간격 (시간)
    RESTART_INTERVAL_HOURS = int(os.getenv("RESTART_INTERVAL_HOURS", 6))

config = Config()
