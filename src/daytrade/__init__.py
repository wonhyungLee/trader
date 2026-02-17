"""Day-trade (당일청산) 전략 모듈.

이 프로젝트는 원래 조회/선정(Viewer) 중심이지만,
`config/strategy.yaml`의 daytrade 섹션을 이용해
"TraderUS 선정 + Daytrade (Balanced)"의 *주문 계획*을 생성할 수 있습니다.

주의: 본 레포는 브로커 주문 전송까지는 포함하지 않으며(order_queue 적재까지만),
실제 체결/취소/브라켓(OCO) 실행은 별도 실행기(또는 사용자의 브로커 연동)에서 수행해야 합니다.
"""
