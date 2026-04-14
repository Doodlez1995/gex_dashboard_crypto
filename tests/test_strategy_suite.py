from pathlib import Path

import pandas as pd

from pro.portfolio import bs_price
from pro.strategy_suite import (
    build_optimizer_candidates,
    default_builder_legs,
    delete_strategy,
    evaluate_strategy,
    load_saved_strategies,
    save_strategy,
)


def _make_chain(symbol="BTC", spot=100.0, iv=0.6):
    today = pd.Timestamp.now(tz="UTC").normalize()
    expiries = [today + pd.Timedelta(days=14), today + pd.Timedelta(days=35)]
    strikes = [80.0, 90.0, 100.0, 110.0, 120.0]
    rows = []
    for expiry in expiries:
        expiry_str = expiry.strftime("%Y-%m-%d")
        t = max((expiry - today).days / 365.0, 1 / 365.0)
        for strike in strikes:
            for option_type in ("call", "put"):
                mid_usd = bs_price(option_type, spot, strike, t, iv)
                rows.append(
                    {
                        "instrument_name": f"{symbol}-{expiry_str}-{strike:.0f}-{option_type}",
                        "symbol": symbol,
                        "expiry": expiry_str,
                        "expiry_ts": expiry,
                        "strike": strike,
                        "type": option_type,
                        "bid": mid_usd / spot * 0.98,
                        "ask": mid_usd / spot * 1.02,
                        "mark": mid_usd / spot,
                        "mid": mid_usd / spot,
                        "mid_usd": mid_usd,
                        "iv": iv,
                        "open_interest": 1000.0,
                        "volume": 100.0,
                        "volume_usd": 50_000.0,
                        "spot": spot,
                    }
                )
    return pd.DataFrame(rows)


def test_default_builder_legs_bull_call_spread_stays_ordered():
    chain = _make_chain()

    legs = default_builder_legs("bull_call_spread", chain, "BTC")
    active = [leg for leg in legs if leg.get("enabled")]

    assert len(active) == 2
    assert active[0]["type"] == "call"
    assert active[1]["type"] == "call"
    assert active[0]["action"] == "buy"
    assert active[1]["action"] == "sell"
    assert active[0]["strike"] < active[1]["strike"]


def test_evaluate_strategy_long_call_returns_sane_metrics():
    chain = _make_chain()
    expiry = sorted(chain["expiry"].unique())[0]
    legs = [
        {
            "row_id": 1,
            "enabled": True,
            "action": "buy",
            "type": "call",
            "expiry": expiry,
            "strike": 100.0,
            "quantity": 1.0,
        }
    ]

    report = evaluate_strategy(chain, "BTC", legs, commission_per_contract=1.0, eval_days=7)

    assert report["ok"] is True
    assert report["net_cost"] > 0
    assert len(report["grid"]) >= 100
    assert "delta" in report["net_greeks_now"]
    assert isinstance(report["scenario_rows"], list)


def test_build_optimizer_candidates_returns_ranked_results():
    chain = _make_chain()

    candidates = build_optimizer_candidates(
        chain,
        "BTC",
        "bullish",
        objective="balanced",
        eval_days=7,
        max_cost_pct=0.5,
        min_pop=0.0,
        max_results=5,
    )

    assert candidates
    assert len(candidates) <= 5
    assert candidates[0]["score"] >= candidates[-1]["score"]
    assert candidates[0]["template_id"] in {"long_call", "bull_call_spread", "bull_put_spread", "covered_call", "protective_put"}


def test_strategy_save_load_and_delete_round_trip(tmp_path: Path):
    path = tmp_path / "strategy_suite_saved.json"
    payload = {
        "name": "BTC Test Trade",
        "symbol": "BTC",
        "template": "long_call",
        "commission": 2.0,
        "eval_days": 7.0,
        "legs": [{"row_id": 1, "enabled": True, "action": "buy", "type": "call", "expiry": "2026-06-30", "strike": 100.0, "quantity": 1.0}],
    }

    save_strategy(path, payload)
    loaded = load_saved_strategies(path)
    assert len(loaded) == 1
    assert loaded[0]["name"] == "BTC Test Trade"

    payload["template"] = "bull_call_spread"
    save_strategy(path, payload)
    loaded = load_saved_strategies(path)
    assert len(loaded) == 1
    assert loaded[0]["template"] == "bull_call_spread"

    delete_strategy(path, "BTC Test Trade", symbol="BTC")
    assert load_saved_strategies(path) == []
