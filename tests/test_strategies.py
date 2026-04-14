import pandas as pd

import pro.strategies as strategies
from pro.models import Profile


def test_derive_legs_keeps_spread_wings_outside_atm():
    profile = Profile(
        symbol="BTC",
        expiry="2026-06-30",
        spot=100.0,
        flip=100.0,
        net_gex=1_000_000.0,
        pos_levels=[101.0, 105.0],
        neg_levels=[99.0, 95.0],
        abs_levels=[],
        available_strikes=[85.0, 90.0, 95.0, 100.0, 105.0, 110.0, 115.0],
    )

    legs = strategies._derive_legs(profile)

    assert legs is not None
    assert legs["atm"] == 100.0
    assert legs["up"] > legs["atm"]
    assert legs["down"] < legs["atm"]


def test_debit_spread_metrics_rejects_debit_ge_width():
    debit, max_profit, max_loss, reason = strategies._debit_spread_metrics(8.0, 3.0, width=5.0)
    assert debit == 5.0
    assert max_profit is None
    assert max_loss is None
    assert reason == "debit_ge_width"


def test_condor_metrics_rejects_non_positive_credit():
    credit, max_profit, max_loss, reason = strategies._condor_metrics(3.0, 2.0, 2.0, 3.0, width=5.0)
    assert credit == -2.0
    assert max_profit is None
    assert max_loss is None
    assert reason == "non_positive_credit"


class _FakeDeribitClient:
    def __init__(self):
        # 100c=12, 110c=2 ⇒ debit 10 ≥ width 10 ⇒ trips the
        # `debit_ge_width` pricing check the test asserts on.
        self.call_px = {
            85.0: 18.0,
            90.0: 15.0,
            95.0: 13.0,
            100.0: 12.0,
            105.0: 6.0,
            110.0: 2.0,
            115.0: 1.0,
        }
        self.put_px = {
            85.0: 1.0,
            90.0: 2.0,
            95.0: 4.0,
            100.0: 7.0,
            105.0: 10.0,
            110.0: 13.0,
            115.0: 16.0,
        }
        self.lookup = {}

    def get_instrument_lookup(self, symbol):
        if self.lookup:
            return self.lookup
        today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
        expiries = [(today + pd.Timedelta(days=d)).strftime("%Y-%m-%d") for d in (7, 14, 28)]
        for expiry in expiries:
            for strike in (85.0, 90.0, 95.0, 100.0, 105.0, 110.0, 115.0):
                self.lookup[(expiry, strike, "call")] = f"{expiry}-{int(strike)}-call"
                self.lookup[(expiry, strike, "put")] = f"{expiry}-{int(strike)}-put"
        return self.lookup

    def get_option_mid_usd(self, instrument_name, spot_price):
        parts = instrument_name.split("-")
        strike = float(parts[-2])
        side = parts[-1]
        if side == "call":
            return self.call_px[strike]
        return self.put_px[strike]


def test_generate_professional_ideas_flags_invalid_directional_pricing(monkeypatch):
    fake = _FakeDeribitClient()
    monkeypatch.setattr(strategies, "DeribitClient", lambda: fake)

    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    expiries = [(today + pd.Timedelta(days=d)).strftime("%Y-%m-%d") for d in (7, 14, 28)]
    # Curve crosses zero just above spot (~101.4) so flip > spot. With the
    # new directional logic this is positive-gamma + spot below flip, i.e.
    # mean-reverting drift up → Bull Call Spread (as the test name implies).
    total_by_strike = {
        85.0: -10.0,
        90.0: -8.0,
        95.0: -5.0,
        100.0: -2.0,
        105.0: 5.0,
        110.0: 8.0,
        115.0: 12.0,
    }
    rows = []
    for expiry in expiries:
        for strike, total in total_by_strike.items():
            call_gex = 10.0
            put_gex = total - call_gex
            rows.append(
                {
                    "symbol": "BTC",
                    "expiry": expiry,
                    "strike": strike,
                    "call_gex": call_gex,
                    "put_gex": put_gex,
                    "spot_price": 100.0,
                }
            )
    df = pd.DataFrame(rows)

    payload = strategies.generate_professional_ideas(df, "BTC", account_equity=100000.0)

    assert payload["ok"] is True
    directional = next(x for x in payload["ideas"] if x["name"] == "Bull Call Spread")
    assert directional["checks"].get("pricing") == "debit_ge_width"
    assert directional["max_loss"] is None
    assert directional["ticket"]["quantity"] == 0
