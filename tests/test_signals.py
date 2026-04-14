import pandas as pd

from pro.signals import gamma_flip_level, build_profile


def test_gamma_flip_cross():
    by = pd.DataFrame(
        {
            "strike": [90, 100, 110],
            "total_gex": [-10, 5, 5],
            "abs_gex": [10, 5, 5],
        }
    )
    flip = gamma_flip_level(by)
    assert flip is not None
    assert 90 <= flip <= 100


def test_build_profile():
    df = pd.DataFrame(
        [
            {"symbol": "BTC", "expiry": "2026-03-06", "strike": 60000, "call_gex": 10, "put_gex": -5, "spot_price": 65000},
            {"symbol": "BTC", "expiry": "2026-03-06", "strike": 65000, "call_gex": 12, "put_gex": -20, "spot_price": 65000},
            {"symbol": "BTC", "expiry": "2026-03-06", "strike": 70000, "call_gex": 30, "put_gex": -3, "spot_price": 65000},
        ]
    )
    p = build_profile(df, "BTC", "2026-03-06")
    assert p is not None
    assert p.symbol == "BTC"
    assert len(p.available_strikes) == 3

