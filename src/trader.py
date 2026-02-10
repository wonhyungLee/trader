"""운영 엔트리포인트.

close -> open -> sync -> cancel 순으로 Next-Open 루프를 수행한다.
"""
from __future__ import annotations

import argparse
import os
import json
import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List

from src.utils.config import load_settings
from src.utils.notifier import maybe_notify
from src.utils.db_exporter import maybe_export_db
from src.utils.project_root import ensure_repo_root
from src.storage.sqlite_store import SQLiteStore
from src.brokers.kis_broker import KISBroker
from src.analyzer.backtest_runner import load_strategy

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/bnf_trader.log"), logging.StreamHandler()],
)


def today_str() -> str:
    return datetime.today().strftime("%Y-%m-%d")


def next_bizday_str() -> str:
    # 단순 +1일. 한국 휴장일은 반영하지 않는다.
    return (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")


def generate_signals(store: SQLiteStore, settings: Dict) -> List[Dict]:
    params = load_strategy(settings)
    prices = store.load_all_prices()
    if prices.empty:
        logging.error("daily_price가 비어있습니다. daily_loader를 먼저 실행하세요.")
        return []
    # latest row per code (+ ma25_prev for trend filter)
    prices = prices.sort_values("date")
    prices["ret3"] = prices.groupby("code")["close"].pct_change(3)
    last2 = prices.groupby("code").tail(2).copy()
    last2["ma25_prev"] = last2.groupby("code")["ma25"].shift(1)
    latest = last2.groupby("code").tail(1)

    stock_info = store.conn.execute("SELECT code,name,market,group_name FROM universe_members").fetchall()
    stock_df = {row[0]: {"name": row[1], "market": row[2], "group": row[3]} for row in stock_info}
    group_map = {row[0]: (row[3] or "UNKNOWN") for row in stock_info}
    universe_codes = set(stock_df.keys())
    if universe_codes:
        latest = latest[latest["code"].isin(universe_codes)]
    # liquidity filter
    latest = latest.sort_values("amount", ascending=False)
    latest = latest[latest["amount"] >= params.min_amount]
    latest = latest.head(params.liquidity_rank)

    max_positions = int(settings.get("trading", {}).get("max_positions") or getattr(params, "max_positions", 10))
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()

    budget_per_pos = float(settings.get("trading", {}).get("order_value") or 0)
    if budget_per_pos <= 0:
        initial_cash = float(getattr(params, "initial_cash", 0) or 0)
        util = float(getattr(params, "capital_utilization", 0) or 0)
        if initial_cash > 0 and util > 0 and max_positions > 0:
            budget_per_pos = (initial_cash * util) / max_positions
        else:
            budget_per_pos = 1_000_000

    sector_counts: Dict[str, int] = {}
    try:
        held = store.conn.execute("SELECT code FROM position_state").fetchall()
        for (code,) in held:
            sec = group_map.get(code) or "UNKNOWN"
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}

    candidates = []
    for _, row in latest.iterrows():
        market = stock_df.get(row["code"], {}).get("market", "KOSPI")
        buy_th = params.buy_kospi if "KOSPI" in market else params.buy_kosdaq

        # trend filter (optional)
        if getattr(params, "trend_ma25_rising", False):
            ma25_prev = row.get("ma25_prev")
            if ma25_prev is None:
                continue
            try:
                if float(row.get("ma25") or 0) <= float(ma25_prev):
                    continue
            except Exception:
                continue

        try:
            disp = float(row.get("disparity") or 0)
        except Exception:
            disp = 0.0
        try:
            r3 = float(row.get("ret3") or 0)
        except Exception:
            r3 = 0.0

        if entry_mode == "trend_follow":
            if disp >= buy_th and r3 >= 0:
                try:
                    amt = float(row.get("amount", 0) or 0)
                except Exception:
                    amt = 0.0
                score = (disp) + (0.8 * (r3)) + (0.05 * math.log1p(max(amt, 0.0)))
                candidates.append((row["code"], group_map.get(row["code"], "UNKNOWN"), score, row))
        else:
            if disp <= buy_th:
                try:
                    amt = float(row.get("amount", 0) or 0)
                except Exception:
                    amt = 0.0
                score = (-disp) + (0.8 * (-r3)) + (0.05 * math.log1p(max(amt, 0.0)))
                candidates.append((row["code"], group_map.get(row["code"], "UNKNOWN"), score, row))

    if rank_mode == "score":
        candidates.sort(key=lambda x: x[2], reverse=True)

    orders: List[Dict] = []
    for code, sec, _score, row in candidates:
        if max_per_sector and sector_counts.get(sec, 0) >= max_per_sector:
            continue
        if len(orders) >= max_positions:
            break
        try:
            close = float(row["close"] or 0)
        except Exception:
            close = 0.0
        if close <= 0:
            continue
        qty = int(budget_per_pos // close)
        if qty <= 0:
            continue
        orders.append({
            "signal_date": today_str(),
            "code": code,
            "side": "BUY",
            "qty": qty,
            "rank": len(orders) + 1,
            "ord_dvsn": settings.get("trading", {}).get("ord_dvsn", "01"),
            "ord_unpr": close,
        })
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    logging.info("generated %d signals (rank_mode=%s entry_mode=%s)", len(orders), rank_mode, entry_mode)
    return orders


def cmd_close(store: SQLiteStore, settings: Dict):
    orders = generate_signals(store, settings)
    if not orders:
        return
    exec_date = next_bizday_str()
    store.add_pending_orders(orders, exec_date)
    maybe_notify(settings, f"[close] {len(orders)}건 주문 후보 생성 (exec_date={exec_date})")


def parse_order_response(resp: Dict) -> Dict:
    output = resp.get("output") or resp.get("output1") or resp
    return {
        "odno": output.get("ODNO") or output.get("ORD_NO") or output.get("odno"),
        "ord_orgno": output.get("KRX_FWDG_ORD_ORGNO") or output.get("ORD_ORGNO") or output.get("ord_orgno"),
    }


def parse_balance_cash(resp: Dict) -> float:
    try:
        outputs = resp.get("output2") or resp.get("output") or []
        if isinstance(outputs, list) and outputs:
            cash_str = outputs[0].get("prcs_bal") or outputs[0].get("dnca_tot_amt")
            return float(cash_str)
    except Exception:
        return 0.0
    return 0.0


def cmd_open(store: SQLiteStore, settings: Dict, broker: KISBroker):
    today = today_str()
    pendings = store.list_orders(status=["PENDING"], exec_date=today)
    if not pendings:
        logging.info("no pending orders for today")
        return

    cash_available = parse_balance_cash(broker.get_balance())
    budget_per_pos = cash_available / max(1, len(pendings)) if cash_available > 0 else float(settings.get("trading", {}).get("order_value", 1_000_000))

    for row in pendings:
        try:
            qty = row["qty"]
            price = row["ord_unpr"] if row["ord_unpr"] else None
            if cash_available > 0:
                qty = max(1, int(budget_per_pos // (price or 1)))
            resp = broker.send_order(row["code"], row["side"], qty, price, ord_dvsn=row["ord_dvsn"])
            parsed = parse_order_response(resp)
            store.update_order_status(row["id"], "SENT", odno=parsed.get("odno"), ord_orgno=parsed.get("ord_orgno"), api_resp=json.dumps(resp), sent_at=datetime.utcnow().isoformat())
            logging.info("order sent %s qty=%s odno=%s", row["code"], qty, parsed.get("odno"))
        except Exception as e:
            logging.exception("open failed for %s", row["code"])
            store.update_order_status(row["id"], "ERROR", api_resp=str(e))

    maybe_notify(settings, f"[open] {len(pendings)}건 발주 시도 완료")


def cmd_sync(store: SQLiteStore, settings: Dict, broker: KISBroker):
    today = today_str()
    res = broker.get_orders(today, today)
    outputs = res.get("output") or res.get("output1") or []
    odno_map = {o.get("odno") or o.get("ODNO") or o.get("ord_no"): o for o in outputs} if isinstance(outputs, list) else {}

    sent_orders = store.list_orders(status=["SENT", "PARTIAL", "NOT_FOUND"], exec_date=today)
    for row in sent_orders:
        od = row["odno"]
        if od and od in odno_map:
            o = odno_map[od]
            filled = int(float(o.get("tot_ccld_qty") or o.get("tot_ccl_qty") or o.get("ccld_qty", 0)))
            avg = float(o.get("avr_prvs" ,0) or o.get("avg_prc", 0))
            status = "DONE" if filled >= row["qty"] else "PARTIAL"
            store.update_order_status(row["id"], status, filled_qty=filled, avg_price=avg, api_resp=json.dumps(o))
        else:
            store.update_order_status(row["id"], "NOT_FOUND")

    # reconcile positions from balance
    bal = broker.get_balance()
    pos_list = bal.get("output1") or []
    positions = []
    for p in pos_list:
        code = p.get("pdno") or p.get("PDNO")
        name = p.get("prdt_name") or p.get("PRDT_NAME") or ""
        qty = int(float(p.get("hldg_qty") or p.get("HLDG_QTY") or 0))
        avg_price = float(p.get("pchs_avg_pric") or p.get("PCHS_AVG_PRIC") or 0)
        if qty > 0 and code:
            positions.append({"code": code, "name": name, "qty": qty, "avg_price": avg_price})

    # 최종 진실은 잔고이므로, position_state를 통째로 재구성(유령 포지션 제거)
    store.replace_positions(positions, entry_date=today)
    maybe_notify(settings, "[sync] 주문/포지션 동기화 완료")


def cmd_cancel(store: SQLiteStore, settings: Dict, broker: KISBroker):
    today = today_str()
    targets = store.list_orders(status=["SENT", "PARTIAL", "NOT_FOUND"], exec_date=today)
    cancelled = 0
    for row in targets:
        if not row["ord_orgno"] or not row["odno"]:
            continue
        try:
            resp = broker.cancel_order(row["code"], row["qty"], orgn_odno=row["odno"], ord_orgno=row["ord_orgno"], ord_dvsn=row["ord_dvsn"])
            store.update_order_status(row["id"], "CANCELLED", cancel_resp=json.dumps(resp))
            cancelled += 1
        except Exception:
            logging.exception("cancel failed for %s", row["code"])
    maybe_notify(settings, f"[cancel] {cancelled}건 취소 완료")


def main():
    ensure_repo_root()
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["close", "open", "sync", "cancel"], help="실행 모드")
    args = parser.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    broker = KISBroker(settings)
    job_id = store.start_job(f"trader_{args.mode}")

    try:
        if args.mode == "close":
            cmd_close(store, settings)
        elif args.mode == "open":
            cmd_open(store, settings, broker)
        elif args.mode == "sync":
            cmd_sync(store, settings, broker)
        elif args.mode == "cancel":
            cmd_cancel(store, settings, broker)
        store.finish_job(job_id, "SUCCESS", f"mode={args.mode}")
        maybe_export_db(settings, store.db_path)
    except Exception as exc:
        store.finish_job(job_id, "ERROR", str(exc))
        raise


if __name__ == "__main__":
    main()
