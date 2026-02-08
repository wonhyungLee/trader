#!/bin/bash
# systemd 서비스로 등록하는 스크립트

echo "🔧 POA Bot 서버 관리자를 systemd 서비스로 등록합니다..."

# systemd 서비스 파일 생성
sudo tee /etc/systemd/system/poa-server-manager.service > /dev/null << 'EOF'
[Unit]
Description=POA Bot Server Manager
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu
ExecStart=/usr/bin/python3 /home/ubuntu/server_manager.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# 환경 변수
Environment="PYTHONPATH=/home/ubuntu"
Environment="PATH=/usr/local/bin:/usr/bin:/bin"

[Install]
WantedBy=multi-user.target
EOF

echo "✅ systemd 서비스 파일 생성 완료"

# systemd 데몬 리로드
sudo systemctl daemon-reload
echo "✅ systemd 데몬 리로드 완료"

# 서비스 활성화 (부팅시 자동 시작)
sudo systemctl enable poa-server-manager.service
echo "✅ 서비스 자동 시작 설정 완료"

echo ""
echo "🎉 설정 완료!"
echo ""
echo "사용 방법:"
echo "  서비스 시작: sudo systemctl start poa-server-manager"
echo "  서비스 중지: sudo systemctl stop poa-server-manager"
echo "  서비스 상태: sudo systemctl status poa-server-manager"
echo "  로그 확인:   sudo journalctl -u poa-server-manager -f"
echo ""