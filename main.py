import argparse
from src import trader


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["close", "open", "sync", "cancel"], help="실행 모드")
    args = parser.parse_args()
    trader.main()


if __name__ == "__main__":
    main()
