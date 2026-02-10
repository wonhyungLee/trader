from __future__ import annotations

import os
import sqlite3
import subprocess
import json
import logging
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path('data/market_data.db')
FRONTEND_DIST = Path('frontend/dist')

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

@app.route('/<path:path>')
def serve_static(path):
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

if __name__ == '__main__':
    host = os.getenv("BNFK_API_HOST", "0.0.0.0")
    port = int(os.getenv("BNFK_API_PORT", "5001"))
    app.run(host=host, port=port)
