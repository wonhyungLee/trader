# 자동매매 및 자산관리 프로그램

6시간마다 자동으로 재시작하는 자동매매 및 자산관리 프로그램입니다.

## 주요 기능

- **자동매매**: Upbit, Bitget(실/데모), Bithumb 연동
- **자산관리**: 암호화폐 및 주식(KIS) 자산 모니터링
- **자동재시작**: 6시간마다 프로그램 자동 재시작
- **디스코드 알림**: 자산 현황 및 거래 내역 디스코드 전송
- **로그 관리**: 체계적인 로그 기록 및 관리

## 설치 및 실행

### 1. 의존성 설치
```bash
cd /home/ubuntu/new_trading_bot
pip3 install -r requirements.txt
```

### 2. 환경 설정
`.env` 파일에서 API 키 및 설정을 확인하고 필요시 수정하세요.

### 3. 봇 실행

#### 방법 1: 자동 설정 스크립트 사용 (권장)
```bash
./start_bot.sh
```

#### 방법 2: systemd 서비스로 실행
```bash
sudo systemctl start trading-bot
sudo systemctl enable trading-bot  # 부팅시 자동 시작
```

#### 방법 3: 직접 실행
```bash
# 재시작 매니저와 함께 실행 (권장)
python3 restart_manager.py

# 단일 실행 (6시간 후 종료)
python3 trading_bot.py
```

## 관리 명령어

### 서비스 제어
```bash
# 서비스 시작
sudo systemctl start trading-bot

# 서비스 중지
sudo systemctl stop trading-bot

# 서비스 재시작
sudo systemctl restart trading-bot

# 서비스 상태 확인
sudo systemctl status trading-bot
```

### 로그 확인
```bash
# 실시간 로그 확인
sudo journalctl -u trading-bot -f

# 최근 로그 확인
sudo journalctl -u trading-bot --since "1 hour ago"

# 파일 로그 확인
tail -f logs/$(date +%Y%m%d).log
```

## 파일 구조

```
new_trading_bot/
├── .env                    # 환경 변수 (API 키 등)
├── config.py              # 설정 관리
├── requirements.txt       # Python 의존성
├── trading_bot.py         # 메인 트레이딩 봇
├── restart_manager.py     # 6시간 재시작 매니저
├── asset_monitor.py       # 자산 모니터링
├── start_bot.sh          # 자동 설정 및 시작 스크립트
├── trading-bot.service   # systemd 서비스 파일
├── utils/
│   └── logger.py         # 로깅 유틸리티
└── exchanges/
    ├── upbit_client.py   # Upbit 클라이언트
    ├── bitget_client.py  # Bitget 클라이언트
    ├── bithumb_client.py # Bithumb 클라이언트
    └── kis_client.py     # KIS 클라이언트
```

## 설정 옵션

### .env 파일 주요 설정
- `RESTART_INTERVAL_HOURS`: 재시작 간격 (기본 6시간)
- `DISCORD_WEBHOOK_URL`: 디스코드 웹훅 URL (선택사항)
- 거래소별 API 키 및 시크릿(Upbit, Bitget 실/데모, Bithumb)
- KIS 계좌 정보: `KIS1_...` ~ `KIS50_...` 형식 (키/시크릿/계좌번호/계좌코드)

### 거래소 지원
- **Upbit**: 원화 거래
- **Bitget**: USDT 거래 (실계좌/데모)
- **Bithumb**: 원화 거래
- **KIS**: 주식 거래 (최대 50개 계좌)

## 주의사항

1. **API 키 보안**: .env 파일의 API 키를 안전하게 관리하세요.
2. **테스트**: 실제 거래 전에 데모 모드로 충분히 테스트하세요.
3. **모니터링**: 정기적으로 로그와 자산 상태를 확인하세요.
4. **백업**: 중요한 설정과 로그를 정기적으로 백업하세요.

## 문제 해결

### 일반적인 문제
1. **서비스 시작 실패**: 로그 확인 후 API 키 및 네트워크 연결 점검
2. **자산 조회 실패**: 각 거래소 API 키 유효성 확인
3. **디스코드 알림 실패**: 웹훅 URL 유효성 확인

### 로그 위치
- systemd 로그: `sudo journalctl -u trading-bot`
- 파일 로그: `logs/YYYYMMDD.log`

## 개발 정보

- Python 3.10 이상 권장
- CCXT 라이브러리 기반 거래소 연동
- 비동기 처리로 성능 최적화
- systemd를 통한 안정적인 서비스 관리
