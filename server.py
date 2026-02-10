from __future__ import annotations

import os
import sqlite3
import subprocess
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import numpy as np
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path('data/market_data.db')
FRONTEND_DIST = Path('frontend/dist')
ACCOUNT_SNAPSHOT_PATH = Path('data/account_snapshot.json')
_balance_cache: Dict[str, Any] = {"ts": 0.0, "data": None}

app = Flask(__name__, static_folder=str(FRONTEND_DIST), static_url_path='')
CORS(app, resources={r"/*": {"origins": "*"}})

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0

def _minmax(conn: sqlite3.Connection, table: str) -> dict:
    try:
        row = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
        return {"min": row[0], "max": row[1]}
    except Exception:
        return {"min": None, "max": None}

def _distinct_code_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0]
    except Exception:
        return 0

def _missing_codes(conn: sqlite3.Connection, table: str) -> int:
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM universe_members u
            LEFT JOIN (SELECT DISTINCT code FROM {table}) t
            ON u.code = t.code
            WHERE t.code IS NULL
            """
        ).fetchone()
        return row[0]
    except Exception:
        return 0

def _pgrep(pattern: str) -> bool:
    try:
        res = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, check=False)
        return res.returncode == 0
    except Exception:
        return False

def _read_accuracy_progress() -> dict:
    path = Path("data/accuracy_progress.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return None


def _pick_float(payload: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for key in keys:
        if key in payload:
            val = _safe_float(payload.get(key))
            if val is not None:
                return val
    return None


def _latest_price_map(conn: sqlite3.Connection, codes: list[str]) -> Dict[str, Dict[str, Any]]:
    if not codes:
        return {}
    placeholder = ",".join("?" * len(codes))
    sql = f"""
        SELECT d.code, d.close, d.date
        FROM daily_price d
        JOIN (
            SELECT code, MAX(date) AS max_date
            FROM daily_price
            WHERE code IN ({placeholder})
            GROUP BY code
        ) m
        ON d.code = m.code AND d.date = m.max_date
    """
    rows = conn.execute(sql, tuple(codes)).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        out[row[0]] = {"close": row[1], "date": row[2]}
    return out


def _load_account_snapshot() -> Optional[Dict[str, Any]]:
    if not ACCOUNT_SNAPSHOT_PATH.exists():
        return None
    try:
        return json.loads(ACCOUNT_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_account_snapshot(total_assets: Optional[float]) -> Optional[Dict[str, Any]]:
    if total_assets is None:
        return None
    if ACCOUNT_SNAPSHOT_PATH.exists():
        return _load_account_snapshot()
    snapshot = {
        "connected_at": pd.Timestamp.utcnow().isoformat(),
        "initial_total": total_assets,
    }
    try:
        ACCOUNT_SNAPSHOT_PATH.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return snapshot


def _fetch_live_balance(settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        from src.brokers.kis_broker import KISBroker
    except Exception:
        return None
    try:
        broker = KISBroker(settings)
        return broker.get_balance()
    except Exception:
        return None


def _build_account_summary(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Dict[str, Any]:
    now_ts = time.time()
    if _balance_cache.get("data") and now_ts - _balance_cache.get("ts", 0) < 120:
        return _balance_cache["data"]

    resp = _fetch_live_balance(settings)
    if not resp:
        data = {"connected": False, "reason": "balance_unavailable"}
        _balance_cache.update({"ts": now_ts, "data": data})
        return data

    output2 = resp.get("output2") or resp.get("output") or []
    summary = output2[0] if isinstance(output2, list) and output2 else (output2 if isinstance(output2, dict) else {})
    cash = _pick_float(summary, ("prcs_bal", "dnca_tot_amt", "cash_bal", "cash_bal_amt"))
    total_eval = _pick_float(summary, ("tot_evlu_amt", "tot_evlu_amt", "tot_asst_evlu_amt"))
    total_pnl = _pick_float(summary, ("tot_pfls", "tot_pfls_amt", "tot_pfls_amt"))

    positions = resp.get("output1") or []
    codes = []
    parsed_positions = []
    for p in positions:
        code = p.get("pdno") or p.get("PDNO")
        if not code:
            continue
        codes.append(code)
        parsed_positions.append({
            "code": code,
            "name": p.get("prdt_name") or p.get("PRDT_NAME") or "",
            "qty": int(float(p.get("hldg_qty") or p.get("HLDG_QTY") or 0)),
            "avg_price": _safe_float(p.get("pchs_avg_pric") or p.get("PCHS_AVG_PRIC")),
            "eval_amount": _safe_float(p.get("evlu_amt") or p.get("EVLU_AMT")),
        })

    price_map = _latest_price_map(conn, list(set(codes)))
    positions_value = 0.0
    for p in parsed_positions:
        if p["eval_amount"] is not None:
            positions_value += p["eval_amount"]
            continue
        last_close = price_map.get(p["code"], {}).get("close")
        if last_close is not None:
            positions_value += last_close * (p["qty"] or 0)

    if total_eval is None:
        total_eval = (cash or 0.0) + positions_value
    if total_pnl is None and total_eval is not None:
        # approximate PnL using cost basis from positions
        cost = sum((p.get("avg_price") or 0) * (p.get("qty") or 0) for p in parsed_positions)
        total_pnl = total_eval - cost if cost else None

    snapshot = _save_account_snapshot(total_eval)
    since_pnl = None
    since_pct = None
    connected_at = None
    if snapshot and total_eval is not None:
        connected_at = snapshot.get("connected_at")
        initial_total = snapshot.get("initial_total") or 0
        since_pnl = total_eval - initial_total
        since_pct = (since_pnl / initial_total * 100) if initial_total else None

    data = {
        "connected": True,
        "connected_at": connected_at,
        "summary": {
            "cash": cash,
            "positions_value": positions_value,
            "total_assets": total_eval,
            "total_pnl": total_pnl,
            "total_pnl_pct": (total_pnl / total_eval * 100) if total_pnl is not None and total_eval else None,
        },
        "since_connected": {
            "pnl": since_pnl,
            "pnl_pct": since_pct,
        },
    }
    _balance_cache.update({"ts": now_ts, "data": data})
    return data

def _read_accuracy_lock() -> dict:
    path = Path("data/accuracy_loader.lock")
    if not path.exists():
        return {"running": False}
    pid = None
    try:
        pid = int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        pid = None
    running = False
    if pid:
        try:
            os.kill(pid, 0)
            running = True
        except Exception:
            running = False
    return {"running": running, "pid": pid}

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/bnf')
def serve_index_bnf():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    if (FRONTEND_DIST / path).exists():
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/bnf/<path:path>')
def serve_static_bnf(path):
    if (FRONTEND_DIST / path).exists():
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

@app.get('/universe')
def universe():
    conn = get_conn()
    sector = request.args.get('sector')
    if sector:
        if sector.upper() == "UNKNOWN":
            where = "s.sector_name IS NULL"
            params = ()
        else:
            where = "s.sector_name = ?"
            params = (sector,)
    else:
        where = "1=1"
        params = ()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT u.code, u.name, u.market, u.group_name as 'group',
                   COALESCE(s.sector_name, 'UNKNOWN') AS sector_name,
                   s.industry_name
            FROM universe_members u
            LEFT JOIN sector_map s ON u.code = s.code
            WHERE {where}
            ORDER BY u.code
            """,
            conn,
            params=params,
        )
    except Exception:
        df = pd.read_sql_query(
            "SELECT code, name, market, group_name as 'group' FROM universe_members ORDER BY code",
            conn,
        )
    return jsonify(df.to_dict(orient='records'))

@app.get('/sectors')
def sectors():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT u.market,
                   COALESCE(s.sector_name, 'UNKNOWN') AS sector_name,
                   COUNT(*) AS count
            FROM universe_members u
            LEFT JOIN sector_map s ON u.code = s.code
            GROUP BY u.market, COALESCE(s.sector_name, 'UNKNOWN')
            ORDER BY u.market, count DESC, sector_name
            """,
            conn,
        )
    except Exception:
        df = pd.DataFrame([], columns=["market", "sector_name", "count"])
    return jsonify(df.to_dict(orient='records'))

@app.get('/prices')
def prices():
    code = request.args.get('code')
    days = int(request.args.get('days', 60))
    if not code:
        return jsonify([])
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT date, open, high, low, close, volume, amount, ma25, disparity FROM daily_price WHERE code=? ORDER BY date DESC LIMIT ?",
        conn,
        params=(code, days),
    )
    # Ensure JSON-safe values (NaN/inf -> null)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient='records'))

@app.get('/signals')
def signals():
    conn = get_conn()
    df = pd.read_sql_query("SELECT signal_date, code, side, qty FROM order_queue ORDER BY created_at DESC LIMIT 30", conn)
    return jsonify(df.to_dict(orient='records'))

@app.get('/orders')
def orders():
    conn = get_conn()
    limit = int(request.args.get('limit', 200))
    df = pd.read_sql_query(
        """
        SELECT
          signal_date, exec_date, code, side, qty, status, ord_dvsn, ord_unpr, filled_qty, avg_price, created_at, updated_at
        FROM order_queue
        ORDER BY created_at DESC
        LIMIT ?
        """,
        conn,
        params=(limit,)
    )
    return jsonify(df.to_dict(orient='records'))

@app.get('/positions')
def positions():
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT code, name, qty, avg_price, entry_date, updated_at FROM position_state ORDER BY updated_at DESC",
        conn
    )
    return jsonify(df.to_dict(orient='records'))


@app.get('/portfolio')
def portfolio():
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT p.code, p.name, p.qty, p.avg_price, p.entry_date, p.updated_at,
               u.market, s.sector_name, s.industry_name
        FROM position_state p
        LEFT JOIN universe_members u ON p.code = u.code
        LEFT JOIN sector_map s ON p.code = s.code
        ORDER BY p.updated_at DESC
        """,
        conn,
    )
    codes = df["code"].dropna().astype(str).unique().tolist()
    price_map = _latest_price_map(conn, codes)
    records = []
    total_value = 0.0
    total_cost = 0.0
    for row in df.to_dict(orient="records"):
        code = row.get("code")
        last = price_map.get(code, {})
        last_close = last.get("close")
        last_date = last.get("date")
        qty = float(row.get("qty") or 0)
        avg_price = float(row.get("avg_price") or 0)
        cost = qty * avg_price if qty and avg_price else None
        market_value = qty * last_close if qty and last_close is not None else None
        pnl = market_value - cost if market_value is not None and cost is not None else None
        pnl_pct = (pnl / cost * 100) if pnl is not None and cost else None
        if market_value is not None:
            total_value += market_value
        if cost is not None:
            total_cost += cost
        row.update({
            "last_close": last_close,
            "last_date": last_date,
            "market_value": market_value,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
        })
        records.append(row)
    totals = {
        "positions_value": total_value,
        "cost": total_cost,
        "pnl": total_value - total_cost if total_cost else None,
        "pnl_pct": ((total_value - total_cost) / total_cost * 100) if total_cost else None,
    }
    return jsonify({"positions": records, "totals": totals})


@app.get('/plans')
def plans():
    conn = get_conn()
    exec_date = request.args.get("exec_date")
    if not exec_date:
        try:
            exec_date = conn.execute("SELECT MAX(exec_date) FROM order_queue").fetchone()[0]
        except Exception:
            exec_date = None
    if not exec_date:
        return jsonify({"exec_date": None, "buys": [], "sells": []})
    df = pd.read_sql_query(
        """
        SELECT o.id, o.signal_date, o.exec_date, o.code, o.side, o.qty, o.rank, o.status,
               o.ord_dvsn, o.ord_unpr, o.created_at, o.updated_at,
               u.name, u.market, s.sector_name, s.industry_name
        FROM order_queue o
        LEFT JOIN universe_members u ON o.code = u.code
        LEFT JOIN sector_map s ON o.code = s.code
        WHERE o.exec_date = ? AND o.status IN ('PENDING','SENT','PARTIAL','NOT_FOUND')
        ORDER BY o.rank ASC, o.id ASC
        """,
        conn,
        params=(exec_date,),
    )
    codes = df["code"].dropna().astype(str).unique().tolist()
    price_map = _latest_price_map(conn, codes)
    buys = []
    sells = []
    for row in df.to_dict(orient="records"):
        code = row.get("code")
        last = price_map.get(code, {})
        planned_price = row.get("ord_unpr") if row.get("ord_unpr") else last.get("close")
        row.update({
            "planned_price": planned_price,
            "last_close": last.get("close"),
            "last_date": last.get("date"),
        })
        if row.get("side") == "SELL":
            sells.append(row)
        else:
            buys.append(row)
    return jsonify({
        "exec_date": exec_date,
        "buys": buys,
        "sells": sells,
        "counts": {"buys": len(buys), "sells": len(sells)},
    })


@app.get('/account')
def account():
    conn = get_conn()
    settings = load_settings()
    return jsonify(_build_account_summary(conn, settings))

@app.get('/status')
def status():
    conn = get_conn()
    out = {
        "universe": {
            "total": _count(conn, "universe_members"),
        },
        "daily_price": {
            "rows": _count(conn, "daily_price"),
            "codes": _distinct_code_count(conn, "daily_price"),
            "missing_codes": _missing_codes(conn, "daily_price"),
            "date": _minmax(conn, "daily_price"),
        },
        "accuracy": {
            "investor_flow_daily": {"missing_codes": _missing_codes(conn, "investor_flow_daily")},
            "program_trade_daily": {"missing_codes": _missing_codes(conn, "program_trade_daily")},
            "short_sale_daily": {"missing_codes": _missing_codes(conn, "short_sale_daily")},
            "credit_balance_daily": {"missing_codes": _missing_codes(conn, "credit_balance_daily")},
            "loan_trans_daily": {"missing_codes": _missing_codes(conn, "loan_trans_daily")},
            "vi_status_daily": {"missing_codes": _missing_codes(conn, "vi_status_daily")},
        }
    }
    return jsonify(out)

@app.get('/jobs')
def jobs():
    conn = get_conn()
    limit = int(request.args.get('limit', 10))
    df = pd.read_sql_query(
        "SELECT * FROM job_runs ORDER BY started_at DESC LIMIT ?",
        conn,
        params=(limit,)
    )
    return jsonify(df.to_dict(orient='records'))

@app.get('/engines')
def engines():
    conn = get_conn()
    try:
        last_signal = conn.execute("SELECT MAX(created_at) FROM order_queue").fetchone()[0]
    except:
        last_signal = None
    pending = _count(conn, "order_queue WHERE status='PENDING'")
    sent = _count(conn, "order_queue WHERE status='SENT'")
    done = _count(conn, "order_queue WHERE status='DONE'")
    
    monitor_running = _pgrep("src.monitor.monitor_main")
    accuracy_lock = _read_accuracy_lock()
    progress = _read_accuracy_progress()
    
    return jsonify({
        "monitor": {"running": monitor_running},
        "trader": {
            "last_signal": last_signal,
            "pending": pending,
            "sent": sent,
            "done": done
        },
        "accuracy_loader": {
            "running": accuracy_lock.get("running"),
            "pid": accuracy_lock.get("pid"),
            "progress": progress
        }
    })

@app.get('/strategy')
def strategy():
    settings = load_settings()
    strat = settings.get("strategy", {})
    trading = settings.get("trading", {})
    return jsonify({
        "liquidity_rank": strat.get("liquidity_rank"),
        "min_amount": strat.get("min_amount"),
        "disparity_buy_kospi": strat.get("disparity_buy_kospi"),
        "disparity_buy_kosdaq": strat.get("disparity_buy_kosdaq"),
        "disparity_sell": strat.get("disparity_sell"),
        "stop_loss": strat.get("stop_loss"),
        "max_holding_days": strat.get("max_holding_days"),
        "order_value": trading.get("order_value"),
        "ord_dvsn": trading.get("ord_dvsn")
    })

# CSV 내보내기 엔드포인트
@app.post('/export')
def export_csv():
    settings = load_settings()
    maybe_export_db(settings, str(DB_PATH))
    return jsonify({"status": "success", "message": "CSV export completed"})


def _register_bnf_aliases():
    aliases = [
        ("/universe", universe, ["GET"]),
        ("/sectors", sectors, ["GET"]),
        ("/prices", prices, ["GET"]),
        ("/signals", signals, ["GET"]),
        ("/orders", orders, ["GET"]),
        ("/positions", positions, ["GET"]),
        ("/portfolio", portfolio, ["GET"]),
        ("/plans", plans, ["GET"]),
        ("/account", account, ["GET"]),
        ("/status", status, ["GET"]),
        ("/jobs", jobs, ["GET"]),
        ("/engines", engines, ["GET"]),
        ("/strategy", strategy, ["GET"]),
        ("/export", export_csv, ["POST"]),
    ]
    for path, view, methods in aliases:
        endpoint = f"bnf_{path.strip('/').replace('/', '_')}"
        app.add_url_rule(f"/bnf{path}", endpoint=endpoint, view_func=view, methods=methods)

if __name__ == '__main__':
    _register_bnf_aliases()
    host = os.getenv("BNFK_API_HOST", "0.0.0.0")
    port = int(os.getenv("BNFK_API_PORT", "5001"))
    app.run(host=host, port=port)
