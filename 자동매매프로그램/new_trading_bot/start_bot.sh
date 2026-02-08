#!/bin/bash

# 트레이딩 봇 시작 스크립트

BOT_DIR="/home/ubuntu/new_trading_bot"
SERVICE_NAME="trading-bot"

echo "=== 자동매매봇 시작 스크립트 ==="

# 디렉토리로 이동
cd "$BOT_DIR" || {
    echo "ERROR: 봇 디렉토리를 찾을 수 없습니다: $BOT_DIR"
    exit 1
}

# 권한 설정
chmod +x restart_manager.py
chmod +x trading_bot.py

# 로그 디렉토리 생성
mkdir -p logs

# Python 패키지 설치 확인
echo "Python 의존성 확인 중..."
if ! pip3 install -r requirements.txt > /dev/null 2>&1; then
    echo "WARNING: 일부 패키지 설치에 실패했을 수 있습니다."
fi

# systemd 서비스 설정
echo "systemd 서비스 설정 중..."
if sudo cp trading-bot.service /etc/systemd/system/; then
    sudo systemctl daemon-reload
    sudo systemctl enable $SERVICE_NAME
    echo "systemd 서비스 설정 완료"
else
    echo "WARNING: systemd 서비스 설정 실패"
fi

echo ""
echo "=== 사용 가능한 명령어 ==="
echo "서비스 시작: sudo systemctl start $SERVICE_NAME"
echo "서비스 중지: sudo systemctl stop $SERVICE_NAME"
echo "서비스 상태: sudo systemctl status $SERVICE_NAME"
echo "로그 확인: sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "직접 실행: python3 restart_manager.py"
echo "단일 실행: python3 trading_bot.py"
echo ""

# 서비스 시작 여부 확인
read -p "지금 서비스를 시작하시겠습니까? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "서비스를 시작합니다..."
    sudo systemctl start $SERVICE_NAME
    sleep 3
    sudo systemctl status $SERVICE_NAME --no-pager
    echo ""
    echo "실시간 로그 확인: sudo journalctl -u $SERVICE_NAME -f"
else
    echo "수동으로 서비스를 시작하려면: sudo systemctl start $SERVICE_NAME"
fi

echo "설정 완료!"