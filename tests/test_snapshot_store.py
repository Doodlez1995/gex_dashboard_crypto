import pandas as pd

from pro.snapshot_store import write_snapshot, load_snapshot_range, write_alert, load_alerts, get_last_alert_ts


def test_snapshot_store_range_and_alerts(tmp_path):
    db_path = tmp_path / "snapshots.db"
    df = pd.DataFrame(
        [
            {
                "symbol": "BTC",
                "expiry": "2026-06-30",
                "strike": 80000,
                "call_gex": 1.0,
                "put_gex": -0.5,
                "spot_price": 75000,
            }
        ]
    )
    ts = pd.Timestamp("2026-03-01T00:00:00Z")
    write_snapshot(db_path, df, ts_utc=ts)
    out = load_snapshot_range(db_path, "BTC", ts - pd.Timedelta(hours=1), ts + pd.Timedelta(hours=1))
    assert not out.empty

    write_alert(db_path, symbol="BTC", rule_id="flip_cross", severity="high", message="test", payload={"a": 1})
    alerts = load_alerts(db_path, symbol="BTC")
    assert not alerts.empty
    last_ts = get_last_alert_ts(db_path, "BTC", "flip_cross")
    assert last_ts is not None
