"""Higher-order options Greeks: vanna, charm, plus dealer hedge-flow estimator.

These functions take a Deribit-style option chain dataframe (the one returned by
``pro.strategy_suite.fetch_deribit_option_chain``) and aggregate per-strike
exposures so the dashboard can display dealer-side risk that pure GEX misses.

Conventions
-----------
* Sign convention assumes dealers are short customer flow: ``customer_oi`` is
  the open interest customers hold, ``dealer = -customer``. Vanna and charm
  exposures returned here are the customer-side aggregate; dashboards typically
  invert the sign before displaying ("dealer view").
* Vanna is reported as the dollar P/L change per +1 vol-point (1 IV-point = 1%)
  per +1% spot move, expressed as a USD amount.
* Charm is reported as the dollar P/L change per calendar day, expressed as a
  USD amount (per the standard "delta decay" definition).
* Risk-free and dividend yields default to zero — the right convention for
  USD-margined crypto perps in the absence of a clean curve.
"""

from __future__ import annotations

import math
from typing import Optional

import pandas as pd


# ───────────────────────────────────────────────────────────────────────────
# Black-Scholes building blocks
# ───────────────────────────────────────────────────────────────────────────

_SQRT_2PI = math.sqrt(2.0 * math.pi)


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _d1_d2(spot: float, strike: float, t: float, sigma: float, r: float = 0.0, q: float = 0.0):
    if spot <= 0 or strike <= 0 or sigma <= 0 or t <= 0:
        return None, None
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return d1, d2


def bs_vanna(spot: float, strike: float, t: float, sigma: float, r: float = 0.0, q: float = 0.0) -> float:
    """Black-Scholes vanna: dDelta/dSigma. Same value for calls and puts.

    Returns 0.0 if inputs are degenerate. Units: change in delta per 1.00 of
    sigma (so multiply by 0.01 for "per IV-point").
    """
    d1, d2 = _d1_d2(spot, strike, t, sigma, r, q)
    if d1 is None:
        return 0.0
    return -math.exp(-q * t) * _norm_pdf(d1) * d2 / sigma


def bs_charm(spot: float, strike: float, t: float, sigma: float, option_type: str,
             r: float = 0.0, q: float = 0.0) -> float:
    """Black-Scholes charm: -dDelta/dT (delta decay per year).

    Sign matches the standard "delta decay" definition — positive charm means
    delta drifts toward 1 (calls) or 0 (puts) as time passes for ITM options.
    Units: change in delta per year. Divide by 365 for per-day.
    """
    d1, d2 = _d1_d2(spot, strike, t, sigma, r, q)
    if d1 is None:
        return 0.0
    sqrt_t = math.sqrt(t)
    common = math.exp(-q * t) * _norm_pdf(d1) * (2.0 * (r - q) * t - d2 * sigma * sqrt_t) / (2.0 * t * sigma * sqrt_t)
    if option_type.lower().startswith("c"):
        return q * math.exp(-q * t) * _norm_cdf(d1) - common
    return -q * math.exp(-q * t) * _norm_cdf(-d1) - common


# ───────────────────────────────────────────────────────────────────────────
# Per-row exposure
# ───────────────────────────────────────────────────────────────────────────


def compute_chain_exposures(
    chain_df: pd.DataFrame,
    spot_override: Optional[float] = None,
    contract_size: float = 1.0,
) -> pd.DataFrame:
    """Compute per-row vanna/charm exposures from a Deribit option chain.

    Expects columns: strike, type ('call'|'put'), iv (decimal, e.g. 0.65),
    open_interest, expiry_ts (tz-aware UTC). Missing rows are dropped.

    Returns a copy of the input with extra columns:
        - dte_years
        - vanna           : per-contract vanna (delta per 1.0 sigma)
        - charm           : per-contract charm (delta per year)
        - vanna_exposure  : USD P/L per +1 vol-point per +1% spot move
        - charm_exposure  : USD delta-decay per calendar day, USD-valued
    """
    if chain_df is None or chain_df.empty:
        return pd.DataFrame(
            columns=[
                "strike", "type", "iv", "open_interest", "spot",
                "dte_years", "vanna", "charm", "vanna_exposure", "charm_exposure",
            ]
        )

    df = chain_df.copy()
    df["strike"] = pd.to_numeric(df.get("strike"), errors="coerce")
    df["iv"] = pd.to_numeric(df.get("iv"), errors="coerce")
    df["open_interest"] = pd.to_numeric(df.get("open_interest"), errors="coerce").fillna(0.0)
    df["spot"] = pd.to_numeric(df.get("spot"), errors="coerce")
    df = df.dropna(subset=["strike", "iv", "spot"])
    df = df[(df["iv"] > 0) & (df["open_interest"] > 0) & (df["spot"] > 0)]
    if df.empty:
        df["dte_years"] = []
        df["vanna"] = []
        df["charm"] = []
        df["vanna_exposure"] = []
        df["charm_exposure"] = []
        return df

    now = pd.Timestamp.now(tz="UTC")
    expiry_ts = pd.to_datetime(df.get("expiry_ts"), utc=True, errors="coerce")
    df["dte_years"] = (expiry_ts - now).dt.total_seconds() / (365.25 * 24 * 3600)
    df = df[df["dte_years"] > 0].copy()
    if df.empty:
        return df

    # Spot used in BS — prefer the per-row spot from the chain summary, but
    # let callers force a single spot for cross-row consistency.
    if spot_override is not None and spot_override > 0:
        df["spot"] = float(spot_override)

    vannas, charms = [], []
    for row in df.itertuples(index=False):
        v = bs_vanna(row.spot, row.strike, row.dte_years, row.iv)
        c = bs_charm(row.spot, row.strike, row.dte_years, row.iv, row.type)
        vannas.append(v)
        charms.append(c)
    df["vanna"] = vannas
    df["charm"] = charms

    # Convert per-contract greeks to USD-aggregated dealer exposure.
    #
    # Vanna exposure ($) per +1 vol-point per +1% spot move:
    #   delta_change = vanna * 0.01      (1 vol-point = 0.01 sigma)
    #   pnl_per_1pct = delta_change * spot * 0.01 * oi * contract_size
    df["vanna_exposure"] = (
        df["vanna"] * 0.01 * df["spot"] * 0.01 * df["open_interest"] * contract_size
    )
    # Charm exposure ($) per calendar day:
    #   delta_change_per_day = charm / 365.25
    #   pnl_per_day = delta_change_per_day * spot * oi * contract_size
    df["charm_exposure"] = (
        df["charm"] / 365.25 * df["spot"] * df["open_interest"] * contract_size
    )
    return df


def aggregate_by_strike(exposures_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Sum per-row vanna/charm exposures into a per-strike view.

    Returns a DataFrame indexed by strike with columns
    ``vanna_exposure`` and ``charm_exposure``, sorted by strike ascending,
    keeping the ``top_n`` strikes ranked by absolute combined magnitude.
    """
    if exposures_df is None or exposures_df.empty:
        return pd.DataFrame(columns=["strike", "vanna_exposure", "charm_exposure"])

    grouped = (
        exposures_df.groupby("strike", as_index=False)
        .agg(
            vanna_exposure=("vanna_exposure", "sum"),
            charm_exposure=("charm_exposure", "sum"),
        )
    )
    if grouped.empty:
        return grouped

    if top_n and len(grouped) > top_n:
        grouped["_rank"] = grouped["vanna_exposure"].abs() + grouped["charm_exposure"].abs()
        grouped = grouped.sort_values("_rank", ascending=False).head(top_n)
        grouped = grouped.drop(columns=["_rank"])

    return grouped.sort_values("strike").reset_index(drop=True)


# ───────────────────────────────────────────────────────────────────────────
# Dealer hedge flow
# ───────────────────────────────────────────────────────────────────────────


def compute_dealer_hedge_flow(
    gex_df: pd.DataFrame,
    spot: float,
    shocks: tuple = (-0.02, -0.01, 0.01, 0.02),
) -> dict:
    """Estimate the notional dealers must trade to stay delta-neutral on a shock.

    Standard derivation:
        GEX_strike = gamma * OI * S²              (per strike, signed)
        For a small spot shock dS:
            delta_change_strike ≈ gamma * OI * dS = GEX_strike * dS / S²
        Notional traded by dealers ≈ delta_change * S = GEX_strike * dS / S
        Summed over strikes: hedge_$ = NetGEX * (dS / S)

    Sign convention: NetGEX is the customer-side net. Dealers are short, so
    if NetGEX > 0 (positive gamma) and spot rises, dealers must SELL spot to
    rebalance — mean-reverting (suppressive) flow. If NetGEX < 0, dealers
    must BUY into rallies — destabilising (trend-following) flow.

    Parameters
    ----------
    gex_df : DataFrame with columns ``call_gex`` and ``put_gex``
    spot   : current spot price
    shocks : iterable of fractional spot shocks (e.g. -0.01 for -1%)

    Returns
    -------
    dict with keys:
        net_gex          : float, total signed GEX in dollars
        regime           : "Mean-Reverting" | "Trend-Following" | "Neutral"
        spot             : float, the spot used
        shocks           : list of {pct, hedge_usd, hedge_units, direction}
        zero_gamma_dist  : None  (placeholder for future flip-distance estimate)
    """
    if gex_df is None or gex_df.empty or not spot or spot <= 0:
        return {
            "net_gex": 0.0,
            "regime": "Neutral",
            "spot": float(spot or 0.0),
            "shocks": [
                {"pct": s, "hedge_usd": 0.0, "hedge_units": 0.0, "direction": "neutral"}
                for s in shocks
            ],
        }

    call_gex = pd.to_numeric(gex_df.get("call_gex"), errors="coerce").fillna(0.0).sum()
    put_gex = pd.to_numeric(gex_df.get("put_gex"), errors="coerce").fillna(0.0).sum()
    net_gex = float(call_gex + put_gex)

    if abs(net_gex) < 1e6:
        regime = "Neutral"
    elif net_gex > 0:
        regime = "Mean-Reverting"
    else:
        regime = "Trend-Following"

    out_shocks = []
    for s in shocks:
        # Hedge required to neutralise dealer delta change after shock dS = s*spot.
        # Dealer flow is OPPOSITE the customer position, so hedge sign = -sign(net_gex)*sign(s).
        # Magnitude in $ = |net_gex * s|.
        hedge_usd = -net_gex * s   # negative because dealer = -customer
        hedge_units = hedge_usd / spot
        if abs(hedge_usd) < 1.0:
            direction = "neutral"
        elif hedge_usd > 0:
            direction = "buy"
        else:
            direction = "sell"
        out_shocks.append({
            "pct": float(s),
            "hedge_usd": float(hedge_usd),
            "hedge_units": float(hedge_units),
            "direction": direction,
        })

    return {
        "net_gex": net_gex,
        "regime": regime,
        "spot": float(spot),
        "shocks": out_shocks,
    }
