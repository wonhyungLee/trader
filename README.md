# BNF-K: Next-Open 자동매매 시스템 (KIS 연동)

> **목표:** “폭락장에서의 회복 탄력성”을 노리는 **Mean Reversion(괴리율)** 전략을  
> **백테스트(Next-Open) → 실전 주문(모의/실계좌) → 체결 동기화/미체결 취소 → 텔레그램 리포트**까지 *방치형(Fire & Forget)*으로 운영할 수 있게 만든 프로젝트.

---

## 0) 운영 루프 (Daily Loop)

> **핵심 원칙:** **전일 종가로 신호 생성 → 익일 시가(Next-Open)로 집행**

아래 스케줄은 **KST(Asia/Seoul)** 기준 예시입니다.

- **15:35** `daily_loader` (증분 수집)
- **(기본) 15:55 / (권장) 16:05** `trader close` (신호 생성 & 주문서 적재)
  - ⚠️ 데이터 소스/엔드포인트에 따라 **당일 일봉이 15:30 직후 즉시 확정되지 않거나 지연 반영**될 수 있습니다.  
    운영 안정성을 위해 **권장값은 16:05** 입니다.
- **08:50** `trader open` (장전 동시호가 구간 발주)
- **09:10** `trader sync` (체결 확인 + 잔고 동기화 + **Morning Report**)
- **09:20** `trader cancel` (미체결 잔량 일괄 취소)
- **(선택) 09:25** `trader sync` (취소 후 최종 확정)

---

## 1) 전략 개요 (BNF-K)

- **매수(BUY):** 유동성 상위(거래대금) 종목 중  
  25일 이평 대비 괴리율이 특정 임계치 이하로 하락한 종목을 **역추세 매수**
- **매도(SELL):** Mean Reversion / StopLoss / TimeStop 중 하나를 만족하면 청산
- **보유기간(Time Stop):** `MAX_HOLD_DAYS`로 강제 제한 (전략의 “짧게 먹고 튀기” 성격 유지)

> **BNF 전략의 검증 포인트:** 급락장 구간에서 **월별 수익률**과 **MDD(낙폭)**, 그리고 **회복 속도**가 의도대로 나오는지 확인.

---

## 2) KIS(Open API) 연동 범위

본 시스템은 한국투자증권 Open API의 **시세/주문/정정취소/체결조회/잔고조회**를 조합하여 운영합니다. citeturn16view0

- 기본 운영은 **REST 기반 폴링(sync)** 으로 충분히 가능
- 실시간 체결통보(WebSocket)는 **선택 사항** (운영 난이도/차단 리스크 때문에 기본값은 REST)

> 공식 샘플/레퍼런스는 KIS의 공개 저장소 및 개발자 포털을 참고하세요. citeturn11view0turn16view0

---

## 3) 프로젝트 구조

```
src/
  brokers/
    kis_broker.py              # 주문/취소/잔고/체결조회 래퍼
  collectors/
    universe_loader.py         # 유니버스(종목 마스터) 갱신
    bulk_loader.py             # 초기 대량 적재(예: 1~2년치)
    daily_loader.py            # 증분 적재(마지막 날짜 이후만)
  analyzer/
    backtest_runner.py         # Next-Open 백테스트 엔진(v0.3)
    performance_viewer.py      # 월별 수익률/월별 MDD/리포트 이미지 생성(v0.4)
  utils/
    notifier.py                # 텔레그램 알림
  trader.py                    # close/open/sync/cancel (운영 루프의 중심)
data/
  market_data.db               # SQLite (daily_price, stock_info, order_queue, position_state ...)
  report.png                   # 성과 차트(백테스트 결과)
logs/
  cron.log                     # 크론 로그(로테이션 권장)
config/
  settings.yaml
```

---

## 4) 설치

```bash
pip install -r requirements.txt
```

권장 `requirements.txt`:

```text
pandas
numpy
requests
pyyaml
matplotlib
tqdm
finance-datareader
```

---

## 5) 설정 (`config/settings.yaml`)

### 5.1 KIS 설정(모의/실전)

- **실전/모의** 환경(base_url, tr_id prefix 등)은 설정으로 분리하세요.
- **토큰 발급은 빈번하면 제한될 수 있으므로 캐시/재사용이 필수**입니다. citeturn18view0

예시:

```yaml
kis:
  env: "paper"          # "paper" | "prod"
  app_key: "..."
  app_secret: "..."
  cano: "12345678"
  acnt_prdt_cd: "01"
  rate_limit_sleep_sec: 0.5   # ✅ 보수적 기본값 권장 (환경 따라 0.3~1.0 조정)
telegram:
  token: "YOUR_BOT_TOKEN"
  chat_id: "YOUR_CHAT_ID"
discord:
  webhook: "YOUR_DISCORD_WEBHOOK_URL"
  enabled: true|false
```

환경변수 치환: 실행 시 `.env`와 `개인정보` 파일을 자동 로딩하며, `개인정보`에 있는 `KIS1_*` 값이 기본 사용됩니다. 다른 계정/환경을 쓰려면 `.env`에 `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`(8자리), `KIS_ACNT_PRDT_CD`(2자리), `TG_BOT_TOKEN`, `TG_CHAT_ID` 등을 명시적으로 적어 덮어쓰세요.

### 5.2 Next-Open 주문 타입 고정 (백테스트 ↔ 실전 정합성)

- **Next-Open 실행모델은 “주문 타입”에 의해 실제 체결 품질이 크게 달라집니다.**
- 본 프로젝트는 `ORD_DVSN_OPEN`(주문구분 코드)을 **설정 키로 고정**해 실행모델을 결정합니다.

#### 지원/권장 값(운영 기준)

- `00`: 지정가
- `01`: 시장가  
  (예: 주문 타입 코드로 `00/01`이 사용되는 예시는 공개 구현체/레퍼런스에서 확인할 수 있습니다.) citeturn18view0

> ⚠️ 그 외 조건부/최유리 등 확장 타입은 계정/상품/문서 버전에 따라 다를 수 있으니,  
> **공식 KIS 문서 기준으로 확인 후** 프로젝트 코드에 반영하세요.

---

## 6) 데이터 정의 & Sanity Check

### 6.1 `daily_price.amount` 정의(고정)

- `daily_price.amount`는 **거래대금(원 단위)** 로 저장한다고 가정합니다.
- 데이터 소스에 따라 `원/천원` 등 단위가 달라질 수 있으니 **반드시 한 번 검증**하세요.

### 6.2 부트스트랩 Sanity Check(권장)

초기 1회 또는 유니버스 갱신 직후, 임의 종목 1~2개로 아래를 체크:

- `amount ≈ close × volume` (스케일이 10^3 단위로 어긋나면 단위 문제 가능성)
- `close > 0`, `volume > 0`, `amount > 0`가 아닌 데이터는 신호 생성에서 제외 권장

---

## 7) 유니버스(종목 마스터) 정합성 가이드

장기 운영에서 가장 많이 틀어지는 부분이 **종목 마스터**입니다.

권장 정책:

- **정기 갱신:** 주 1회(또는 매일 장마감 후)
- **제외 예시(옵션):**
  - 거래정지/관리/투자주의·경고 등 이벤트 종목
  - 스팩/우선주/ETF(전략 의도와 다르면 제외)
  - 데이터 결측(가격=0, 거래대금=0, 최근 N일 수집 누락)

---

## 8) 주문 생명주기(State Machine)

`order_queue.status`는 아래 흐름을 따릅니다.

- `PENDING` : close 단계에서 생성된 “주문서”
- `SENT` : open 단계에서 KIS로 발주 성공 (핵심 키 저장 완료)
- `DONE` : 전량 체결
- `PARTIAL` : 부분 체결
- `NOT_FOUND` : 당일 체결/주문 조회에서 확인되지 않음(거부/지연 등)
- `CANCELLED` : 미체결 잔량 취소 처리
- `ERROR` : API/파싱/권한 등 오류

**중요:** 발주 직후 응답에서 아래 키를 반드시 DB에 저장합니다.

- `ODNO` (주문번호)
- `KRX_FWDG_ORD_ORGNO` (주문조직번호; 취소/정정에 사실상 필수)

---

## 9) 재실행 안전성(Idempotency) — 운영 원칙

실전 운영에서 `cron` 오작동/재시작 등으로 같은 명령이 **하루에 여러 번** 실행될 수 있습니다.  
이 프로젝트는 아래 원칙을 지켜야 “중복 주문”을 원천 차단합니다.

### 9.1 `close` (신호 생성)
- 같은 `exec_date`에 대해 **PENDING을 중복 생성하지 않는다.**
- 권장 구현(둘 중 하나):
  - (현재 코드) `exec_date`의 `status='PENDING'`을 **삭제 후 재생성**
  - (권장 강화) `(exec_date, code, side)`에 **UNIQUE 제약/인덱스** 추가 후 `UPSERT`

### 9.2 `open` (발주)
- **status='PENDING'만 발주**한다.
- 이미 `SENT/DONE/...`이면 발주 금지.

### 9.3 `sync` (동기화)
- **“오늘(exec_date=today) 발주한 주문만”** 체결조회로 상태 갱신한다.
- 최종 진실은 **잔고**이므로, 조회 실패/지연 시에도 `reconcile_positions_with_broker()`가 안전장치가 된다.

### 9.4 `cancel` (미체결 정리)
- `SENT/PARTIAL/NOT_FOUND` 중 **미체결 잔량이 확인되는 주문만 취소**한다.
- 취소 후 상태는 `CANCELLED`로 고정한다.

---

## 10) 실행 방법(로컬 테스트)

### Step 1) 데이터 준비 & 신호 생성 (Close)

```bash
python -m src.collectors.universe_loader
python -m src.collectors.daily_loader

python -m src.trader close
```

### Step 2) 주문 전송 (Open)

```bash
python -m src.trader open
```

### Step 3) 체결 동기화 & Morning Report (Sync)

```bash
python -m src.trader sync
```

### Step 4) 미체결 잔량 취소 (Cancel)

```bash
python -m src.trader cancel
```

---

## 11) Crontab 예시 (KST)

> 서버가 UTC인 경우가 많습니다. **서버 TZ 설정** 또는 `cron` 실행 시간을 반드시 확인하세요.

### (권장) 16:05 close 버전

```bash
# 08:50 장전 주문
50 08 * * 1-5 cd /path/to/BNF_Trader && python -m src.trader open >> logs/cron.log 2>&1

# 09:10 체결 확인 + 리포트
10 09 * * 1-5 cd /path/to/BNF_Trader && python -m src.trader sync >> logs/cron.log 2>&1

# 09:20 미체결 일괄 취소
20 09 * * 1-5 cd /path/to/BNF_Trader && python -m src.trader cancel >> logs/cron.log 2>&1

# 15:35 증분 수집
35 15 * * 1-5 cd /path/to/BNF_Trader && python -m src.collectors.daily_loader >> logs/cron.log 2>&1

# 16:05 신호 생성(권장)
05 16 * * 1-5 cd /path/to/BNF_Trader && python -m src.trader close >> logs/cron.log 2>&1

# (선택) 16:10 최종 sync
10 16 * * 1-5 cd /path/to/BNF_Trader && python -m src.trader sync >> logs/cron.log 2>&1
```

---

## 12) 레이트리밋 / 재시도 / 백오프

- KIS API는 호출 제한/차단 리스크가 있으므로 **슬립 + 재시도 + 백오프**는 운영 필수입니다. citeturn16view0
- 설정 기본값은 **`rate_limit_sleep_sec: 0.5`**처럼 보수적으로 두고, 안정화 후 낮추세요.

권장 패턴(요약):

- HTTP 429/일시 오류 → `0.5s → 1s → 2s → 4s` 형태로 backoff
- 토큰 발급은 하루 1회 수준으로 재사용/캐시 (잦은 발급은 제한 가능) citeturn18view0

---

## 13) DB 성능 최적화 (인덱스)

데이터가 쌓이면 조회가 느려질 수 있으니, **배포 후 1회** 아래 인덱스를 추가하세요.

```bash
sqlite3 data/market_data.db "CREATE INDEX IF NOT EXISTS idx_daily_price_code_date ON daily_price(code, date);"
sqlite3 data/market_data.db "CREATE INDEX IF NOT EXISTS idx_order_queue_exec_status ON order_queue(exec_date, status);"
sqlite3 data/market_data.db "CREATE INDEX IF NOT EXISTS idx_order_queue_status ON order_queue(status);"
```

### (선택) close 중복 방지 UNIQUE 강화

```bash
sqlite3 data/market_data.db "CREATE UNIQUE INDEX IF NOT EXISTS uq_order_queue_exec_code_side_pending ON order_queue(exec_date, code, side) WHERE status='PENDING';"
```

---

## 14) 로그 관리 (logrotate)

`logs/cron.log`가 무한히 커지면 서버 디스크를 터뜨릴 수 있습니다.  
리눅스 `logrotate`로 관리하세요.

`/etc/logrotate.d/bnf_trader`:

```text
/path/to/BNF_Trader/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 user user
}
```

---

## 15) 백테스트 & 성과 검증

```bash
python -m src.analyzer.backtest_runner
python -m src.analyzer.performance_viewer
```

- **월별 수익률 매트릭스**
- **월별 MDD(낙폭)**
- **누적 수익 곡선 + DD 차트(`data/report.png`)**
- **Trade analysis (승률/평균 보유일/Top Wins/Loss)**

---

## 16) 운영 체크리스트(배포 전)

- [ ] `config/settings.yaml`에 **모의/실전 키** 정확히 입력
- [ ] `order_test.py`로 **주문 권한/환경 전환** 확인
- [ ] DB 스키마/인덱스 적용 확인
- [ ] `cron`의 `PATH`, 작업 디렉토리(`cd ...`) 확인
- [ ] 텔레그램 알림 수신 확인
- [ ] **소액/모의로 2~3일** 검증 후 실전 확장

---

## 면책

본 프로젝트는 학습/연구 목적의 예시입니다.  
실전 투자는 손실 가능성이 있으며, 모든 책임은 사용자에게 있습니다.
