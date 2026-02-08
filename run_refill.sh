#!/usr/bin/env bash
set -x
cd /home/ubuntu/종목선별매매프로그램 || exit 1
source venv2/bin/activate
export PYTHONUNBUFFERED=1
python -u -m src.collectors.refill_loader \
  --universe data/universe_kospi100.csv \
  --universe data/universe_kosdaq150.csv \
  --source kis \
  --chunk-days 5000 \
  --cooldown 80 \
  --resume
