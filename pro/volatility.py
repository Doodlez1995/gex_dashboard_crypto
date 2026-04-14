"""Volatility helpers backed by real chain IV (Deribit mark IV).

The previous version of this module used option premium / spot as a proxy
for IV, which was actually moneyness-scaled premium and had no relationship
to volatility. The downstream regime classifier therefore had no real
volatility signal. This rewrite reads `chain_df.iv` (decimal, e.g. 0.65)
which Deribit ships per-strike in the book summary.
"""
from typing import Dict, Iterable, Optional

import pandas as pd

from pro.strategy_suite import fetch_deribit_option_chain


def _atm_iv_for_expiry(chain_df: pd.DataFrame, expiry: str, spot: float) -> Optional[float]:
    """Average call/put mark-IV at the strike closest to spot for one expiry."""
    if chain_df is None or chain_df.empty or not spot or spot <= 0:
        return None
    sub = chain_df[chain_df["expiry"] == str(expiry)]
    sub = sub[pd.notna(sub.get("iv"))]
    sub = sub[sub["iv"].astype(float) > 0]
    if sub.empty:
        return None
    nearest = sub.iloc[(sub["strike"].astype(float) - float(spot)).abs().argsort()[:2]]
    iv_vals = nearest["iv"].astype(float)
    return float(iv_vals.mean()) if not iv_vals.empty else None


def estimate_term_iv(
    symbol: str,
    spot: float,
    expiries: Iterable[str],
    client=None,  # kept for API compatibility — unused
) -> Dict[str, Optional[float]]:
    """Real ATM mark-IV per expiry, sourced from the cached Deribit chain.

    The ``client`` argument is accepted but ignored — IV now comes directly
    from ``fetch_deribit_option_chain``'s `iv` column rather than from a
    premium-as-IV proxy.
    """
    try:
        chain_df = fetch_deribit_option_chain(symbol)
    except Exception:
        chain_df = pd.DataFrame()
    out: Dict[str, Optional[float]] = {}
    for expiry in expiries:
        out[str(expiry)] = _atm_iv_for_expiry(chain_df, str(expiry), float(spot or 0.0))
    return out


def classify_vol_regime(term_iv: Dict[str, Optional[float]]) -> str:
    """Front-vs-term IV regime: high if front IV is well above the term median.

    With real IV in hand the thresholds are interpreted directly: front IV
    20% above the median of the term structure ⇒ "high"; 20% below ⇒ "low".
    """
    vals = [float(v) for v in term_iv.values() if v is not None and v > 0]
    if not vals:
        return "unknown"
    front = vals[0]
    median = float(pd.Series(vals).median())
    if median <= 0:
        return "unknown"
    if front > median * 1.2:
        return "high"
    if front < median * 0.8:
        return "low"
    return "normal"
