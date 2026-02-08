#!/bin/bash
# POA Bot 서버 관리자 시작 스크립트

echo ""
echo "🔄 POA Bot 서버 관리자 시작"
echo "=========================="
echo ""

# Python 버전 확인
echo "🐍 Python 버전 확인..."
python3 --version
echo ""

# 실행 권한 부여
chmod +x /home/ubuntu/server_manager.py

echo "📊 6시간마다 자산조회와 자동매매 서버를 재시작합니다."
echo "로그는 server_manager.log 파일에 저장됩니다."
echo ""
echo "중단하려면 Ctrl+C를 누르세요."
echo ""

# 서버 관리자 실행
python3 /home/ubuntu/server_manager.py