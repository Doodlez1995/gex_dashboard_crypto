from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd

from pro.snapshot_store import load_snapshot_timeseries


@dataclass
class BacktestResult:
    trades: int
    hit_rate: float
    expectancy: float
    total_return: float
    max_drawdown: float


def _max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    running_max = equity_curve.cummax()
    safe_max = running_max.where(running_max > 0)
    dd = (equity_curve - running_max) / safe_max
    dd = dd.dropna()
    return float(dd.min()) if not dd.empty else 0.0


def run_walk_forward_backtest(db_path: Path, symbol: str) -> Dict:
    raw = load_snapshot_timeseries(db_path, symbol, limit=4000)
    if raw.empty:
        return {"ok": False, "reason": "no snapshot history"}
    by_time = raw.groupby("ts_utc", as_index=False).agg(
        spot=("spot_price", "last"),
        net_gex=("total_gex", "sum"),
    )
    by_time = by_time.sort_values("ts_utc").reset_index(drop=True)
    if len(by_time) < 20:
        return {"ok": False, "reason": "insufficient history"}

    # Simple baseline: if net GEX positive, expect smaller move; if negative, momentum.
    by_time["ret_fwd"] = by_time["spot"].shift(-1) / by_time["spot"].replace(0.0, float("nan")) - 1.0
    by_time["signal"] = by_time["net_gex"].apply(lambda x: -1.0 if x > 0 else 1.0)
    by_time["pnl"] = by_time["signal"] * by_time["ret_fwd"]
    trades = by_time.dropna(subset=["pnl"]).copy()
    if trades.empty:
        return {"ok": False, "reason": "no tradable samples"}
    equity = (1.0 + trades["pnl"]).cumprod()
    wins = float((trades["pnl"] > 0).mean())
    expectancy = float(trades["pnl"].mean())
    total_return = float(equity.iloc[-1] - 1.0)
    mdd = _max_drawdown(equity)
    result = BacktestResult(
        trades=int(len(trades)),
        hit_rate=wins,
        expectancy=expectancy,
        total_return=total_return,
        max_drawdown=mdd,
    )
    return {"ok": True, "result": result.__dict__}

