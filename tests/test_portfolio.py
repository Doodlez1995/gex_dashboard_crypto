import pandas as pd

from pro.portfolio import build_portfolio_snapshot


def test_portfolio_snapshot_basic():
    positions = pd.DataFrame(
        [
            {"symbol": "BTC", "expiry": "2026-06-30", "strike": 80000, "type": "call", "quantity": 1, "avg_price": 1000},
            {"symbol": "BTC", "expiry": "2026-06-30", "strike": 70000, "type": "put", "quantity": -1, "avg_price": 900},
        ]
    )
    options_df = pd.DataFrame(
        [
            {"symbol": "BTC", "spot_price": 75000, "exchange": "Deribit", "expiry": "2026-06-30", "strike": 75000, "call_gex": 1.0, "put_gex": -1.0},
        ]
    )
    report = build_portfolio_snapshot(positions, options_df, "BTC", spot_shift_pct=0.0, vol_shift_pct=0.0)
    assert report.get("ok")
    summary = report["summary"]
    assert "net_delta" in summary
    assert "net_gex" in summary
    assert report["by_expiry"].shape[0] >= 1
