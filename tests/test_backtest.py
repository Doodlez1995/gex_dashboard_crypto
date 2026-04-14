from pathlib import Path

import pandas as pd

from pro.backtest import run_walk_forward_backtest
from pro.snapshot_store import write_snapshot


def test_backtest_runs(tmp_path):
    db = Path(tmp_path) / "snap.db"
    rows = []
    ts0 = pd.Timestamp("2026-01-01T00:00:00Z")
    for i in range(30):
        ts = ts0 + pd.Timedelta(minutes=5 * i)
        spot = 100 + i
        rows.append(
            {
                "symbol": "BTC",
                "expiry": "2026-03-06",
                "strike": 100,
                "call_gex": 10 + i,
                "put_gex": -5,
                "spot_price": spot,
            }
        )
        write_snapshot(db, pd.DataFrame([rows[-1]]), ts_utc=ts)
    result = run_walk_forward_backtest(db, "BTC")
    assert "ok" in result


def test_backtest_uses_timestamp_window_not_raw_row_limit(tmp_path):
    db = Path(tmp_path) / "dense_snap.db"
    ts0 = pd.Timestamp("2026-01-01T00:00:00Z")
    strikes = list(range(50, 300))
    for i in range(30):
        ts = ts0 + pd.Timedelta(minutes=5 * i)
        spot = 100 + i
        frame = pd.DataFrame(
            [
                {
                    "symbol": "BTC",
                    "expiry": "2026-06-30",
                    "strike": strike,
                    "call_gex": 15 + i,
                    "put_gex": -5,
                    "spot_price": spot,
                }
                for strike in strikes
            ]
        )
        write_snapshot(db, frame, ts_utc=ts)

    result = run_walk_forward_backtest(db, "BTC")
    assert result.get("ok") is True
    assert result["result"]["trades"] >= 20
