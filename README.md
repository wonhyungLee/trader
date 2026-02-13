# BNF Viewer (US)

NASDAQ100 + S&P500 종목을 대상으로 하는 **조회 전용 대시보드**입니다.

- ✅ 종목/섹터 목록 조회
- ✅ 일봉 차트(종가, MA25, 거래량) 조회
- ✅ 전략 조건 기반 '매수 후보(Selection)' 조회
- ✅ 선택 종목 `Current Price` 1분 갱신 표시
- ✅ DB watchdog 기반 주기적 데이터 증분/보정
- ❌ 자동매매/주문 기능 없음
- ❌ 잔고/포트폴리오 기능 없음

---

## 지원 범위 (안정화 기준)

- `main.py` / `server.py` : 조회 전용 API + 대시보드
- `src/collectors/universe_loader.py` : 유니버스 적재
- `src/collectors/bulk_loader.py` : 초기 일봉 적재(FDR)
- `src/collectors/refill_loader.py` / `src/collectors/daily_loader.py` : KIS 기반 증분/리필
- `src/collectors/sector_seed_loader.py` : 섹터 시드 반영
- `scripts/diagnose_viewer.py` : DB + API 헬스체크

---

## 1) 설치

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cd frontend
npm install
npm run build
cd ..
```

## 2) 유니버스 CSV 준비 (필수)

`data/universe_nasdaq100.csv` + `data/universe_sp500.csv`가 필요합니다.

- 기본으로 헤더만 포함된 placeholder가 포함되어 있습니다.
- 아래 스크립트로 위키피디아에서 최신 리스트를 받아 CSV 생성합니다. (인터넷 필요)

```bash
python scripts/generate_universe_us.py
```

## 3) DB에 유니버스 적재

```bash
python -m src.collectors.universe_loader
```

## 4) 가격 데이터 적재

초기 적재(최근 500일) 예시:

```bash
python -m src.collectors.bulk_loader --days 500
```

> `bulk_loader`는 FinanceDataReader를 사용합니다. 실행 환경에 인터넷 연결이 필요합니다.

## 5) 서버 실행

```bash
# 방법 1
python main.py

# 방법 2
python server.py

# 또는
./scripts/start_viewer.sh

# 기본 포트: http://localhost:5002
```

## 6) 헬스체크 (권장)

```bash
./scripts/diagnose_viewer.py
```

## 7) 리필 실행 스크립트

```bash
./run_refill.sh      # foreground
./run_refill_bg.sh   # background + logs/refill_bg.log
```

---

## 폴더 구조

- `main.py` : 조회 전용 서버 엔트리포인트
- `server.py` : Flask API + 정적 프론트(dist) 서빙
- `frontend/` : React(Vite) 프론트엔드
- `src/collectors/` : universe_loader, bulk_loader 등 데이터 적재 스크립트
- `data/market_data.db` : SQLite DB
