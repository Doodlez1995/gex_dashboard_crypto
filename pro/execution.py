from typing import Dict, List

from pro.models import StrategyIdea
from pro.risk import position_size


def build_trade_ticket(idea: StrategyIdea, account_equity: float, max_loss_per_contract: float, risk_limits) -> Dict:
    qty = position_size(account_equity, max_loss_per_contract, risk_limits)
    legs: List[Dict] = []
    for leg in idea.legs:
        legs.append(
            {
                "side": leg.side,
                "type": leg.option_type,
                "strike": leg.strike,
                "expiry": leg.expiry,
                "premium_usd": leg.premium_usd,
                "quantity": qty,
            }
        )
    return {
        "strategy": idea.name,
        "symbol": idea.symbol,
        "expiry": idea.expiry,
        "quantity": qty,
        "legs": legs,
        "checklist": [
            "Verify spread widths < 1% of spot",
            "Confirm quote freshness < 30s",
            "Confirm max loss within daily budget",
            "Submit as defined-risk order",
        ],
    }

