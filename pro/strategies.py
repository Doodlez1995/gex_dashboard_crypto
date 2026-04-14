from typing import Dict, List, Optional, Tuple

import pandas as pd

from pro.deribit_client import DeribitClient
from pro.execution import build_trade_ticket
from pro.models import Profile, RiskLimits, StrategyIdea, StrategyLeg
from pro.risk import validate_trade
from pro.scoring import score_profile
from pro.signals import build_profile, choose_expiry_window
from pro.volatility import classify_vol_regime, estimate_term_iv


def _nearest(values: List[float], target: float, above: Optional[bool] = None) -> Optional[float]:
    if not values:
        return None
    if above is True:
        vals = [v for v in values if v > target]
    elif above is False:
        vals = [v for v in values if v < target]
    else:
        vals = values
    if not vals:
        return None
    return min(vals, key=lambda x: abs(float(x) - float(target)))


def _next_above(values: List[float], target: float, step: int = 1) -> Optional[float]:
    vals = sorted([v for v in values if v > target])
    if not vals:
        return None
    idx = min(max(step - 1, 0), len(vals) - 1)
    return vals[idx]


def _next_below(values: List[float], target: float, step: int = 1) -> Optional[float]:
    vals = sorted([v for v in values if v < target])
    if not vals:
        return None
    idx = min(max(step - 1, 0), len(vals) - 1)
    return vals[-(idx + 1)]


def _derive_legs(profile: Profile) -> Optional[Dict[str, float]]:
    strikes = sorted(profile.available_strikes)
    if not strikes:
        return None
    spot = float(profile.spot)
    atm = _nearest(strikes, spot)
    if atm is None:
        return None
    near_res = _nearest(profile.pos_levels + profile.abs_levels, spot, above=True)
    near_sup = _nearest(profile.neg_levels + profile.abs_levels, spot, above=False)
    up = _nearest(strikes, near_res) if near_res is not None else _next_above(strikes, atm, step=2)
    down = _nearest(strikes, near_sup) if near_sup is not None else _next_below(strikes, atm, step=2)
    if up is not None and up <= atm:
        up = _next_above(strikes, atm, step=1)
    if down is not None and down >= atm:
        down = _next_below(strikes, atm, step=1)
    if up is None or down is None:
        return None
    if up <= atm or down >= atm:
        return None
    condor_top = _next_above(strikes, atm, step=2)
    condor_bottom = _next_below(strikes, atm, step=2)
    if condor_top is None or condor_bottom is None:
        return None
    put_long = _next_below(strikes, condor_bottom, step=1)
    call_long = _next_above(strikes, condor_top, step=1)
    if put_long is None or call_long is None:
        return None
    return {
        "atm": float(atm),
        "up": float(up),
        "down": float(down),
        "condor_top": float(condor_top),
        "condor_bottom": float(condor_bottom),
        "put_long": float(put_long),
        "call_long": float(call_long),
    }


def _price_leg(client: DeribitClient, symbol: str, expiry: str, strike: float, option_type: str, spot: float) -> Tuple[Optional[str], Optional[float]]:
    lookup = client.get_instrument_lookup(symbol)
    inst = lookup.get((expiry, float(strike), option_type))
    if not inst:
        return None, None
    try:
        px = client.get_option_mid_usd(inst, spot)
    except Exception:
        px = None
    return inst, px


def _with_rr(idea: StrategyIdea, max_profit: Optional[float], max_loss: Optional[float]) -> StrategyIdea:
    idea.max_profit = max_profit
    idea.max_loss = max_loss
    if max_profit is not None and max_loss is not None and max_profit > 0 and max_loss > 0:
        idea.rr = max_profit / max_loss
    else:
        idea.rr = None
    return idea


def _debit_spread_metrics(long_px: Optional[float], short_px: Optional[float], width: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    if long_px is None or short_px is None:
        return None, None, None, "missing_quote"
    if width <= 0:
        return None, None, None, "invalid_width"
    debit = float(long_px) - float(short_px)
    if debit <= 0:
        return None, None, None, "non_positive_debit"
    if debit >= width:
        return debit, None, None, "debit_ge_width"
    return debit, max(0.0, width - debit), max(0.0, debit), None


def _long_premium_loss(*premiums: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
    if any(px is None for px in premiums):
        return None, "missing_quote"
    total = float(sum(float(px) for px in premiums if px is not None))
    if total <= 0:
        return None, "non_positive_debit"
    return total, None


def _condor_metrics(
    long_put_px: Optional[float],
    short_put_px: Optional[float],
    short_call_px: Optional[float],
    long_call_px: Optional[float],
    width: float,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    if None in (long_put_px, short_put_px, short_call_px, long_call_px):
        return None, None, None, "missing_quote"
    if width <= 0:
        return None, None, None, "invalid_width"
    credit = (float(short_put_px) + float(short_call_px)) - (float(long_put_px) + float(long_call_px))
    if credit <= 0:
        return credit, None, None, "non_positive_credit"
    if credit >= width:
        return credit, None, None, "credit_ge_width"
    return credit, max(0.0, credit), max(0.0, width - credit), None


def _fmt_strike(value: float) -> str:
    if float(value).is_integer():
        return f"{value:.0f}"
    return f"{value:.2f}"


def _condor_calendar_hedge_text(legs: Dict[str, float], spot: float, short_expiry: str) -> str:
    put_strike = _fmt_strike(float(legs["condor_bottom"]))
    call_strike = _fmt_strike(float(legs["condor_top"]))
    dist_up = abs(float(legs["condor_top"]) - float(spot))
    dist_down = abs(float(spot) - float(legs["condor_bottom"]))
    if dist_up <= dist_down:
        one_side = f"one-sided call calendar at {call_strike}C"
    else:
        one_side = f"one-sided put calendar at {put_strike}P"
    return (
        f"Event-based hedge: {one_side} (sell {short_expiry}, buy next expiry). "
        f"If direction is uncertain, use both-sided calendars at {put_strike}P and {call_strike}C "
        f"(sell {short_expiry}, buy next expiry)."
    )


def _serialize_idea(idea: StrategyIdea, ticket: Dict) -> Dict:
    return {
        "name": idea.name,
        "symbol": idea.symbol,
        "expiry": idea.expiry,
        "conviction": idea.conviction,
        "max_profit": idea.max_profit,
        "max_loss": idea.max_loss,
        "rr": idea.rr,
        "rationale": idea.rationale,
        "hedge": idea.hedge,
        "checks": idea.checks,
        "ticket": ticket,
    }


def generate_professional_ideas(
    df_symbol: pd.DataFrame,
    symbol: str,
    account_equity: float = 100000.0,
    daily_loss_used: float = 0.0,
    open_trades: int = 0,
    stability_cv: float = 0.25,
) -> Dict:
    if df_symbol is None or df_symbol.empty:
        return {"ok": False, "reason": "empty symbol frame", "ideas": []}
    work = df_symbol.copy()
    work["expiry"] = pd.to_datetime(work["expiry"]).dt.normalize()
    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    forward = work[work["expiry"] >= today].copy()
    if forward.empty:
        return {"ok": False, "reason": "no forward expiries", "ideas": []}

    expiries = sorted(forward["expiry"].unique().tolist())
    expiry_map = {
        "regime": choose_expiry_window(expiries, today, 5, 12),
        "directional": choose_expiry_window(expiries, today, 10, 21),
        "income": choose_expiry_window(expiries, today, 14, 35),
    }
    fallback = pd.Timestamp(expiries[0]).strftime("%Y-%m-%d")
    for key in ("regime", "directional", "income"):
        if not expiry_map[key]:
            expiry_map[key] = fallback

    profiles: Dict[str, Optional[Profile]] = {}
    for key, exp in expiry_map.items():
        exp_df = forward[forward["expiry"].dt.strftime("%Y-%m-%d") == exp]
        profiles[key] = build_profile(exp_df, symbol, exp)

    client = DeribitClient()
    iv_term = estimate_term_iv(symbol, float(forward["spot_price"].iloc[-1]), list(expiry_map.values()), client)
    vol_regime = classify_vol_regime(iv_term)
    limits = RiskLimits()
    ideas: List[Dict] = []
    for key in ("regime", "directional", "income"):
        profile = profiles.get(key)
        if profile is None:
            continue
        legs = _derive_legs(profile)
        if legs is None:
            continue
        aligned = True
        score = score_profile(profile, stability_cv=stability_cv, vol_regime=vol_regime, term_aligned=aligned)
        conv = int(score["score"])
        exp = profile.expiry

        if key == "regime":
            c_inst, c_px = _price_leg(client, symbol, exp, legs["atm"], "call", profile.spot)
            p_inst, p_px = _price_leg(client, symbol, exp, legs["atm"], "put", profile.spot)
            max_loss, pricing_issue = _long_premium_loss(c_px, p_px)
            idea = StrategyIdea(
                name="Long Straddle",
                expiry=exp,
                symbol=symbol,
                conviction=conv,
                rationale="Spot near flip can produce volatility expansion.",
                hedge="After expansion, convert to strangle by selling wings.",
            )
            idea.legs = [
                StrategyLeg("buy", "call", legs["atm"], exp, c_px),
                StrategyLeg("buy", "put", legs["atm"], exp, p_px),
            ]
            idea = _with_rr(idea, None, max_loss)
        elif key == "directional":
            # Direction is derived from regime + spot vs flip — NOT from sign(NetGEX),
            # which only tells us whether dealer flow suppresses or amplifies moves.
            #   Positive NetGEX (mean-reverting): expect drift back toward flip.
            #   Negative NetGEX (trend-following): expect continuation away from flip.
            flip = profile.flip
            spot_val = float(profile.spot)
            if flip is None:
                # No flip crossing → no coherent directional thesis; skip this bucket.
                continue
            pos_gamma = profile.net_gex >= 0
            if pos_gamma:
                bullish = spot_val < float(flip)
                regime_word = "Mean-reverting"
            else:
                bullish = spot_val > float(flip)
                regime_word = "Trend-following"
            side_word = "below" if spot_val < float(flip) else "above"
            if bullish:
                b_inst, b_px = _price_leg(client, symbol, exp, legs["atm"], "call", profile.spot)
                s_inst, s_px = _price_leg(client, symbol, exp, legs["up"], "call", profile.spot)
                width = max(0.0, legs["up"] - legs["atm"])
                debit, max_profit, max_loss, pricing_issue = _debit_spread_metrics(b_px, s_px, width)
                idea = StrategyIdea(
                    name="Bull Call Spread",
                    expiry=exp,
                    symbol=symbol,
                    conviction=conv,
                    rationale=f"{regime_word} regime, spot {side_word} flip {float(flip):.0f} → upside bias.",
                    hedge=f"Buy protective put near {legs['down']:.0f}.",
                )
                idea.legs = [
                    StrategyLeg("buy", "call", legs["atm"], exp, b_px),
                    StrategyLeg("sell", "call", legs["up"], exp, s_px),
                ]
                idea = _with_rr(idea, max_profit, max_loss)
            else:
                b_inst, b_px = _price_leg(client, symbol, exp, legs["atm"], "put", profile.spot)
                s_inst, s_px = _price_leg(client, symbol, exp, legs["down"], "put", profile.spot)
                width = max(0.0, legs["atm"] - legs["down"])
                debit, max_profit, max_loss, pricing_issue = _debit_spread_metrics(b_px, s_px, width)
                idea = StrategyIdea(
                    name="Bear Put Spread",
                    expiry=exp,
                    symbol=symbol,
                    conviction=conv,
                    rationale=f"{regime_word} regime, spot {side_word} flip {float(flip):.0f} → downside bias.",
                    hedge=f"Buy upside call near {legs['up']:.0f}.",
                )
                idea.legs = [
                    StrategyLeg("buy", "put", legs["atm"], exp, b_px),
                    StrategyLeg("sell", "put", legs["down"], exp, s_px),
                ]
                idea = _with_rr(idea, max_profit, max_loss)
        else:
            lp_inst, lp_px = _price_leg(client, symbol, exp, legs["put_long"], "put", profile.spot)
            sp_inst, sp_px = _price_leg(client, symbol, exp, legs["condor_bottom"], "put", profile.spot)
            sc_inst, sc_px = _price_leg(client, symbol, exp, legs["condor_top"], "call", profile.spot)
            lc_inst, lc_px = _price_leg(client, symbol, exp, legs["call_long"], "call", profile.spot)
            width = max(float(legs["condor_bottom"] - legs["put_long"]), float(legs["call_long"] - legs["condor_top"]))
            credit, max_profit, max_loss, pricing_issue = _condor_metrics(lp_px, sp_px, sc_px, lc_px, width)
            idea = StrategyIdea(
                name="Iron Condor",
                expiry=exp,
                symbol=symbol,
                conviction=conv,
                rationale="Income structure around expected pin range.",
                hedge=_condor_calendar_hedge_text(legs, float(profile.spot), exp),
            )
            idea.legs = [
                StrategyLeg("buy", "put", legs["put_long"], exp, lp_px),
                StrategyLeg("sell", "put", legs["condor_bottom"], exp, sp_px),
                StrategyLeg("sell", "call", legs["condor_top"], exp, sc_px),
                StrategyLeg("buy", "call", legs["call_long"], exp, lc_px),
            ]
            idea = _with_rr(idea, max_profit, max_loss)

        ok, reasons = validate_trade(
            account_equity=account_equity,
            proposed_max_loss=idea.max_loss if idea.max_loss is not None else 0.0,
            open_trades=open_trades,
            daily_loss_used=daily_loss_used,
            limits=limits,
        )
        idea.checks = {"risk_ok": str(ok), **reasons}
        if pricing_issue:
            idea.checks["pricing"] = pricing_issue
        per_contract_loss = float(idea.max_loss) if idea.max_loss and idea.max_loss > 0 else 0.0
        ticket = build_trade_ticket(idea, account_equity=account_equity, max_loss_per_contract=per_contract_loss, risk_limits=limits)
        ideas.append(_serialize_idea(idea, ticket))

    return {
        "ok": True,
        "expiry_map": expiry_map,
        "vol_regime": vol_regime,
        "ideas": ideas,
    }
