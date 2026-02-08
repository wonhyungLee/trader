# POA Bot 서버 자동 관리 시스템

## 개요
POA Bot의 자산조회와 자동매매 서버를 6시간마다 자동으로 재시작하는 프로그램입니다.

## 구성 파일들

### 1. server_manager.py
- **기능**: 메인 서버 관리 프로그램
- **역할**: 
  - 6시간마다 자동으로 서버 재시작
  - 환경 변수 자동 설정 (API 키들)
  - 프로세스 상태 모니터링
  - 로그 기록

### 2. start_manager.sh
- **기능**: 서버 관리자 시작 스크립트
- **사용법**: `./start_manager.sh`

### 3. systemd_setup.sh
- **기능**: systemd 서비스 등록 스크립트
- **사용법**: `./systemd_setup.sh`

## 사용 방법

### 방법 1: 직접 실행
```bash
# 바로 실행
./start_manager.sh

# 또는 Python으로 직접 실행
python3 server_manager.py
```

### 방법 2: systemd 서비스 등록 (권장)
```bash
# 서비스 등록
./systemd_setup.sh

# 서비스 시작
sudo systemctl start poa-server-manager

# 서비스 상태 확인
sudo systemctl status poa-server-manager

# 로그 실시간 확인
sudo journalctl -u poa-server-manager -f
```

## 설정된 API 정보

### 암호화폐 거래소
- **UPBIT**: 업비트 API 키/시크릿
- **BITGET**: 비트겟 API 키/시크릿/패스프레이즈

### 주식 계좌 (KIS)
- **KIS1~KIS7**: 한국투자증권 API 키/시크릿/계좌번호

## 로그 파일
- **server_manager.log**: 프로그램 실행 로그
- **systemd 로그**: `sudo journalctl -u poa-server-manager -f`

## 특징
- ✅ 6시간마다 자동 재시작
- ✅ 프로세스 상태 모니터링
- ✅ 안전한 프로세스 종료/시작
- ✅ 상세한 로그 기록
- ✅ systemd 서비스 지원
- ✅ 환경 변수 자동 설정
- ✅ 오류 처리 및 재시도

## 주의사항
1. backup 폴더 경로가 올바른지 확인: `/home/ubuntu/POA-v7_backup_20250721`
2. 필요한 패키지가 설치되어 있는지 확인
3. .env 파일이 backup 폴더에 있는지 확인

## 문제 해결
```bash
# 서비스 재시작
sudo systemctl restart poa-server-manager

# 로그 확인
sudo journalctl -u poa-server-manager --no-pager

# 수동으로 중지
sudo systemctl stop poa-server-manager
```