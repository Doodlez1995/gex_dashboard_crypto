from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StrategyLeg:
    side: str  # buy/sell
    option_type: str  # call/put
    strike: float
    expiry: str
    premium_usd: Optional[float] = None


@dataclass
class StrategyIdea:
    name: str
    expiry: str
    symbol: str
    legs: List[StrategyLeg] = field(default_factory=list)
    conviction: int = 0
    max_profit: Optional[float] = None
    max_loss: Optional[float] = None
    rr: Optional[float] = None
    rationale: str = ""
    hedge: str = ""
    checks: Dict[str, str] = field(default_factory=dict)


@dataclass
class Profile:
    symbol: str
    expiry: str
    spot: float
    flip: Optional[float]
    net_gex: float
    pos_levels: List[float] = field(default_factory=list)
    neg_levels: List[float] = field(default_factory=list)
    abs_levels: List[float] = field(default_factory=list)
    available_strikes: List[float] = field(default_factory=list)


@dataclass
class RiskLimits:
    max_risk_pct_per_trade: float = 0.01
    max_risk_pct_per_day: float = 0.03
    max_open_trades: int = 3

