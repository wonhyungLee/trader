"""주문/잔고 테스트 스크립트.

예:
  python src/brokers/order_test.py --balance
  python src/brokers/order_test.py --price 005930
"""

import argparse
import pprint

from src.brokers.kis_broker import KISBroker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--balance", action="store_true")
    parser.add_argument("--price", type=str, default=None)
    args = parser.parse_args()

    broker = KISBroker()
    if args.balance:
        pprint.pprint(broker.get_balance())
    if args.price:
        pprint.pprint(broker.get_current_price(args.price))


if __name__ == "__main__":
    main()
