from pro.models import RiskLimits
from pro.risk import position_size, validate_trade


def test_position_size_positive():
    limits = RiskLimits(max_risk_pct_per_trade=0.01)
    qty = position_size(100000, 250, limits)
    assert qty == 4


def test_validate_trade_flags():
    limits = RiskLimits(max_risk_pct_per_trade=0.01, max_risk_pct_per_day=0.03, max_open_trades=2)
    ok, reasons = validate_trade(
        account_equity=100000,
        proposed_max_loss=2500,
        open_trades=2,
        daily_loss_used=1000,
        limits=limits,
    )
    assert not ok
    assert "open_trades" in reasons
    assert "trade_risk" in reasons

