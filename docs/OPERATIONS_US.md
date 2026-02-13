# Operations (Viewer-US)

## 목표

- `trader-US` 단일 폴더에서 조회 전용 기능만 운영
- 계좌조회/자동매매는 비활성화(지원 범위 제외)

## 필수 기능

1. 유니버스 적재
```bash
python3 -m src.collectors.universe_loader
```

2. 초기 데이터 적재
```bash
python3 -m src.collectors.bulk_loader --days 500
```

3. 증분/리필
```bash
./run_refill.sh
python3 -m src.collectors.daily_loader
```

4. 서버 실행
```bash
./scripts/start_viewer.sh
```

5. 진단
```bash
./scripts/diagnose_viewer.py
```

## 비지원 범위

- 주문/계좌/포트폴리오 동기화
- 자동매매 스케줄 실행

## 운영 체크포인트

- DB: `data/market_data.db`
- 프론트 빌드: `cd frontend && npm run build`
- 기본 주소: `http://localhost:5002`
