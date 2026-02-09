from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

DB_PATH = Path('data/market_data.db')

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _minmax(conn: sqlite3.Connection, table: str) -> dict:
    row = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
    return {"min": row[0], "max": row[1]}


def _distinct_code_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0]


def _missing_codes(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM stock_info s
        LEFT JOIN (SELECT DISTINCT code FROM {table}) t
        ON s.code = t.code
        WHERE t.code IS NULL
        """
    ).fetchone()
    return row[0]


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
        import json
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


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


@app.get('/universe')
def universe():
    conn = get_conn()
    df_kospi = pd.read_sql_query("SELECT code,name,market FROM stock_info WHERE market LIKE '%KOSPI%' ORDER BY marcap DESC LIMIT 100", conn)
    df_kospi['group'] = 'KOSPI100'
    df_kosdaq = pd.read_sql_query("SELECT code,name,market FROM stock_info WHERE market LIKE '%KOSDAQ%' ORDER BY marcap DESC LIMIT 150", conn)
    df_kosdaq['group'] = 'KOSDAQ150'
    data = pd.concat([df_kospi, df_kosdaq]).to_dict(orient='records')
    return jsonify(data)


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
    return jsonify(df.to_dict(orient='records'))


@app.get('/signals')
def signals():
    conn = get_conn()
    df = pd.read_sql_query("SELECT signal_date, code, side, qty FROM order_queue ORDER BY created_at DESC LIMIT 30", conn)
    return jsonify(df.to_dict(orient='records'))

@app.get('/orders')
def orders():
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT
          signal_date,
          exec_date,
          code,
          side,
          qty,
          status,
          ord_dvsn,
          ord_unpr,
          filled_qty,
          avg_price,
          created_at,
          updated_at
        FROM order_queue
        ORDER BY created_at DESC
        LIMIT 200
        """,
        conn,
    )
    return jsonify(df.to_dict(orient='records'))


@app.get('/positions')
def positions():
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT code, name, qty, avg_price, entry_date, updated_at
        FROM position_state
        ORDER BY updated_at DESC
        """,
        conn,
    )
    return jsonify(df.to_dict(orient='records'))


@app.get('/strategy')
def strategy():
    # expose key thresholds for UI display
    from src.utils.config import load_settings
    settings = load_settings()
    strat = settings.get("strategy", {})
    trading = settings.get("trading", {})
    return jsonify(
        {
            "liquidity_rank": strat.get("liquidity_rank"),
            "min_amount": strat.get("min_amount"),
            "disparity_buy_kospi": strat.get("disparity_buy_kospi"),
            "disparity_buy_kosdaq": strat.get("disparity_buy_kosdaq"),
            "disparity_sell": strat.get("disparity_sell"),
            "stop_loss": strat.get("stop_loss"),
            "max_holding_days": strat.get("max_holding_days"),
            "order_value": trading.get("order_value"),
            "ord_dvsn": trading.get("ord_dvsn"),
        }
    )



@app.get('/status')
def status():
    conn = get_conn()
    out = {
        "stock_info": {
            "rows": _count(conn, "stock_info"),
        },
        "daily_price": {
            "rows": _count(conn, "daily_price"),
            "codes": _distinct_code_count(conn, "daily_price"),
            "date": _minmax(conn, "daily_price"),
        },
        "accuracy": {
            "investor_flow_daily": {
                "rows": _count(conn, "investor_flow_daily"),
                "codes": _distinct_code_count(conn, "investor_flow_daily"),
                "missing_codes": _missing_codes(conn, "investor_flow_daily"),
                "date": _minmax(conn, "investor_flow_daily"),
            },
            "program_trade_daily": {
                "rows": _count(conn, "program_trade_daily"),
                "codes": _distinct_code_count(conn, "program_trade_daily"),
                "missing_codes": _missing_codes(conn, "program_trade_daily"),
                "date": _minmax(conn, "program_trade_daily"),
            },
            "short_sale_daily": {
                "rows": _count(conn, "short_sale_daily"),
                "codes": _distinct_code_count(conn, "short_sale_daily"),
                "missing_codes": _missing_codes(conn, "short_sale_daily"),
                "date": _minmax(conn, "short_sale_daily"),
            },
            "credit_balance_daily": {
                "rows": _count(conn, "credit_balance_daily"),
                "codes": _distinct_code_count(conn, "credit_balance_daily"),
                "missing_codes": _missing_codes(conn, "credit_balance_daily"),
                "date": _minmax(conn, "credit_balance_daily"),
            },
            "loan_trans_daily": {
                "rows": _count(conn, "loan_trans_daily"),
                "codes": _distinct_code_count(conn, "loan_trans_daily"),
                "missing_codes": _missing_codes(conn, "loan_trans_daily"),
                "date": _minmax(conn, "loan_trans_daily"),
            },
            "vi_status_daily": {
                "rows": _count(conn, "vi_status_daily"),
                "codes": _distinct_code_count(conn, "vi_status_daily"),
                "missing_codes": _missing_codes(conn, "vi_status_daily"),
                "date": _minmax(conn, "vi_status_daily"),
            },
        },
        "refill_progress": {
            "rows": _count(conn, "refill_progress"),
        },
        "order_queue": {
            "rows": _count(conn, "order_queue"),
        },
    }
    return jsonify(out)


@app.get('/engines')
def engines():
    conn = get_conn()
    last_signal = conn.execute("SELECT MAX(created_at) FROM order_queue").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM order_queue WHERE status='PENDING'").fetchone()[0]
    sent = conn.execute("SELECT COUNT(*) FROM order_queue WHERE status='SENT'").fetchone()[0]
    done = conn.execute("SELECT COUNT(*) FROM order_queue WHERE status='DONE'").fetchone()[0]
    monitor_running = _pgrep("src.monitor.monitor_main")
    accuracy_lock = _read_accuracy_lock()
    progress = _read_accuracy_progress()
    return jsonify(
        {
            "monitor": {
                "running": monitor_running,
            },
            "trader": {
                "last_signal": last_signal,
                "pending": pending,
                "sent": sent,
                "done": done,
            },
            "accuracy_loader": {
                "running": accuracy_lock.get("running"),
                "pid": accuracy_lock.get("pid"),
                "progress": progress,
            },
        }
    )


if __name__ == '__main__':
    host = os.getenv("BNFK_API_HOST", "0.0.0.0")
    port = int(os.getenv("BNFK_API_PORT", "5000"))
    app.run(host=host, port=port)
