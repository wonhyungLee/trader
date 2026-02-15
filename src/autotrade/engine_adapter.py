from __future__ import annotations

from typing import Any, Dict, Optional

from stock_daytrade_engine.config import EngineConfig
from stock_daytrade_engine.recommender import recommend_code


def recommend_daytrade_plan(
    *,
    db_path: str,
    code: str,
    table: str = "daily_price",
    optimize: bool = True,
    optimize_lookback_bars: Optional[int] = None,
    risk_pct: Optional[float] = None,
) -> Dict[str, Any]:
    cfg = EngineConfig(db_path=db_path, table=table)
    return recommend_code(
        code=str(code).strip().upper(),
        cfg=cfg,
        optimize=bool(optimize),
        optimize_lookback=int(optimize_lookback_bars) if optimize_lookback_bars else None,
        risk_pct=risk_pct,
    )

