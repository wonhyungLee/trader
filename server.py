from __future__ import annotations

import os
import sqlite3
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


if __name__ == '__main__':
    host = os.getenv("BNFK_API_HOST", "127.0.0.1")
    port = int(os.getenv("BNFK_API_PORT", "5000"))
    app.run(host=host, port=port)
