"""Next-Open 백테스트 러너.

- 전일 종가에서 신호 생성, 익일 시가 체결 가정
- 전략 파라미터는 config/settings.yaml 또는 config/strategy.yaml을 사용
- 결과: data/trade_log.csv, data/equity_curve.csv
"""

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings, load_yaml


@dataclass
class StrategyParams:
    liquidity_rank: int
    min_amount: float
    buy_kospi: float
    buy_kosdaq: float
    sell_disparity: float
    stop_loss: float
    max_holding_days: int
    max_positions: int


def load_strategy(settings: Dict) -> StrategyParams:
    strat_file = Path("config/strategy.yaml")
    strat = load_yaml(strat_file) if strat_file.exists() else settings.get("strategy", {})
    return StrategyParams(
        liquidity_rank=int(strat.get("liquidity_rank", 300)),
        min_amount=float(strat.get("min_amount", 5e10)),
        buy_kospi=float(strat.get("buy", {}).get("kospi_disparity", strat.get("disparity_buy_kospi", -0.05))),
        buy_kosdaq=float(strat.get("buy", {}).get("kosdaq_disparity", strat.get("disparity_buy_kosdaq", -0.10))),
        sell_disparity=float(strat.get("sell", {}).get("take_profit_disparity", strat.get("disparity_sell", -0.01))),
        stop_loss=float(strat.get("sell", {}).get("stop_loss", strat.get("stop_loss", -0.05))),
        max_holding_days=int(strat.get("sell", {}).get("max_holding_days", strat.get("max_holding_days", 3))),
        max_positions=int(strat.get("position", {}).get("max_positions", strat.get("max_positions", 10))),
    )


def select_universe(prices: pd.DataFrame, stock_info: pd.DataFrame, params: StrategyParams) -> List[str]:
    latest = prices.sort_values("date").groupby("code").tail(1)
    merged = latest.merge(stock_info[["code", "market"]], on="code", how="left")
    merged = merged.sort_values("amount", ascending=False)
    merged = merged[merged["amount"] >= params.min_amount]
    return merged.head(params.liquidity_rank)["code"].tolist()


def run_backtest(store: SQLiteStore, params: StrategyParams, output_dir: Path = Path("data")):
    prices = store.load_all_prices()
    if prices.empty:
        raise SystemExit("daily_price 가 비어있습니다. 먼저 데이터를 적재하세요.")
    stock_info = pd.read_sql_query("SELECT * FROM stock_info", store.conn)
    universe = select_universe(prices, stock_info, params)
    prices = prices[prices["code"].isin(universe)].copy()
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["code", "date"])

    dates = sorted(prices["date"].unique())
    if len(dates) < 2:
        raise SystemExit("가격 데이터가 부족합니다.")

    cash = float(getattr(params, "initial_cash", 10_000_000) or 10_000_000)
    equity_curve: List[Dict] = []
    trade_log: List[Dict] = []
    positions: Dict[str, Dict] = {}

    grouped = {code: df.reset_index(drop=True) for code, df in prices.groupby("code")}

    for i in range(len(dates) - 1):
        today_dt = dates[i]
        next_dt = dates[i + 1]
        today_date = today_dt.date()

        for code, dfc in grouped.items():
            row_today = dfc[dfc["date"] == today_dt]
            row_next = dfc[dfc["date"] == next_dt]
            if row_today.empty or row_next.empty:
                continue

            today = row_today.iloc[0]
            next_row = row_next.iloc[0]
            market_row = stock_info.loc[stock_info["code"] == code]
            market = market_row.iloc[0]["market"] if not market_row.empty else "KOSPI"
            buy_th = params.buy_kospi if "KOSPI" in market else params.buy_kosdaq

            # entry
            if today["disparity"] <= buy_th and code not in positions and len(positions) < params.max_positions:
                open_price = float(next_row["open"])
                if open_price <= 0:
                    continue
                qty = int(cash // (params.max_positions * open_price))
                if qty > 0:
                    cost = qty * open_price
                    cash -= cost
                    positions[code] = {
                        "qty": qty,
                        "avg_price": open_price,
                        "entry_date": next_dt.date(),
                        "hold_days": 0,
                    }
                    trade_log.append({
                        "date": next_dt.date(),
                        "code": code,
                        "action": "BUY",
                        "price": open_price,
                        "qty": qty,
                        "cash": cash,
                    })

            # exit
            if code in positions:
                pos = positions[code]
                pos["hold_days"] += 1
                exit_price = float(next_row["open"])
                if pos["avg_price"] <= 0 or exit_price <= 0:
                    continue
                ret = (exit_price / pos["avg_price"]) - 1
                should_sell = False
                if today["disparity"] >= params.sell_disparity:
                    should_sell = True
                if ret <= params.stop_loss:
                    should_sell = True
                if pos["hold_days"] >= params.max_holding_days:
                    should_sell = True
                if should_sell:
                    cash += pos["qty"] * exit_price
                    trade_log.append({
                        "date": next_dt.date(),
                        "code": code,
                        "action": "SELL",
                        "price": exit_price,
                        "qty": pos["qty"],
                        "cash": cash,
                    })
                    del positions[code]

        equity = cash
        for code, pos in positions.items():
            dfc = grouped[code]
            row_today = dfc[dfc["date"] == today_dt]
            if row_today.empty:
                continue
            close_price = row_today.iloc[0]["close"]
            equity += pos["qty"] * close_price
        equity_curve.append({"date": today_date, "equity": equity})

    equity_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trade_log)
    output_dir.mkdir(parents=True, exist_ok=True)
    equity_df.to_csv(output_dir / "equity_curve.csv", index=False)
    trades_df.to_csv(output_dir / "trade_log.csv", index=False)
    print(f"saved {len(trades_df)} trades, equity_curve {len(equity_df)} rows")


def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    settings = load_settings()
    params = load_strategy(settings)
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    run_backtest(store, params)


if __name__ == "__main__":
    main()
