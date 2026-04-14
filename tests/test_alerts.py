import pandas as pd

from pro.alerts import evaluate_rules


def test_alert_rules_trigger():
    rules = [
        {"id": "flip_cross", "type": "flip_cross", "enabled": True, "severity": "medium"},
        {"id": "net_gex_threshold", "type": "net_gex_threshold", "enabled": True, "severity": "high", "threshold": 100, "direction": "abs"},
        {"id": "oi_wall_shift", "type": "oi_wall_shift", "enabled": True, "severity": "medium", "min_shift_pct": 0.01},
        {"id": "vol_regime_change", "type": "vol_regime_change", "enabled": True, "severity": "low"},
    ]
    ctx = {
        "symbol": "BTC",
        "spot": 1000,
        "prev_flip": 900,
        "curr_flip": 1100,
        "net_gex": 150,
        "prev_oi_wall": 100,
        "oi_wall": 130,
        "prev_vol_regime": "low",
        "vol_regime": "high",
        "ts_utc": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    alerts = evaluate_rules(ctx, rules=rules)
    rule_ids = {alert["rule_id"] for alert in alerts}
    assert "flip_cross" in rule_ids
    assert "net_gex_threshold" in rule_ids
    assert "oi_wall_shift" in rule_ids
    assert "vol_regime_change" in rule_ids
