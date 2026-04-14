from typing import Dict, Tuple

from pro.models import RiskLimits


def position_size(account_equity: float, max_loss_per_contract: float, limits: RiskLimits) -> int:
    if account_equity <= 0 or max_loss_per_contract is None or max_loss_per_contract <= 0:
        return 0
    risk_budget = account_equity * limits.max_risk_pct_per_trade
    contracts = int(risk_budget // max_loss_per_contract)
    return max(0, contracts)


def validate_trade(
    account_equity: float,
    proposed_max_loss: float,
    open_trades: int,
    daily_loss_used: float,
    limits: RiskLimits,
) -> Tuple[bool, Dict[str, str]]:
    reasons: Dict[str, str] = {}
    if open_trades >= limits.max_open_trades:
        reasons["open_trades"] = "max open trades reached"
    if proposed_max_loss is None or proposed_max_loss <= 0:
        reasons["loss"] = "invalid max loss"
    else:
        if proposed_max_loss > (account_equity * limits.max_risk_pct_per_trade):
            reasons["trade_risk"] = "exceeds per-trade risk budget"
        if (daily_loss_used + proposed_max_loss) > (account_equity * limits.max_risk_pct_per_day):
            reasons["daily_risk"] = "exceeds daily loss budget"
    return (len(reasons) == 0, reasons)

