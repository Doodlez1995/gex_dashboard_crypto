from typing import Dict, List, Optional

import pandas as pd

from pro.models import Profile


def gamma_flip_level(by_strike: pd.DataFrame) -> Optional[float]:
    if by_strike is None or by_strike.empty:
        return None
    df = by_strike.sort_values("strike")[["strike", "total_gex"]].copy()
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["total_gex"] = pd.to_numeric(df["total_gex"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["strike"])
    if df.empty:
        return None

    strikes = df["strike"].astype(float).tolist()
    gex_values = df["total_gex"].astype(float).tolist()
    closest = strikes[min(range(len(gex_values)), key=lambda i: abs(gex_values[i]))]

    # Prefer the first direct sign crossing of net GEX by strike and
    # interpolate between adjacent strikes to avoid stepwise jumps.
    for i in range(len(gex_values) - 1):
        g1 = gex_values[i]
        g2 = gex_values[i + 1]
        s1 = strikes[i]
        s2 = strikes[i + 1]
        if g1 == 0:
            return s1
        if g2 == 0:
            return s2
        if (g1 < 0 < g2) or (g1 > 0 > g2):
            weight = -g1 / (g2 - g1)
            return s1 + weight * (s2 - s1)

    return closest


def build_profile(df: pd.DataFrame, symbol: str, expiry_key: str) -> Optional[Profile]:
    if df is None or df.empty:
        return None
    work = df.copy()
    work["total_gex"] = work["call_gex"] + work["put_gex"]
    work["abs_gex"] = work["call_gex"].abs() + work["put_gex"].abs()
    by_strike = work.groupby("strike", as_index=False)[["total_gex", "abs_gex"]].sum()
    if by_strike.empty:
        return None
    pos = by_strike[by_strike["total_gex"] > 0].nlargest(2, "total_gex")["strike"].astype(float).tolist()
    neg = by_strike[by_strike["total_gex"] < 0].nsmallest(2, "total_gex")["strike"].astype(float).tolist()
    abs_lvls = by_strike.nlargest(2, "abs_gex")["strike"].astype(float).tolist()
    return Profile(
        symbol=symbol,
        expiry=expiry_key,
        spot=float(work["spot_price"].iloc[-1]),
        flip=gamma_flip_level(by_strike),
        net_gex=float(by_strike["total_gex"].sum()),
        pos_levels=pos,
        neg_levels=neg,
        abs_levels=abs_lvls,
        available_strikes=sorted(by_strike["strike"].astype(float).tolist()),
    )


def term_structure(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["expiry"] = pd.to_datetime(work["expiry"]).dt.normalize()
    work["total_gex"] = work["call_gex"] + work["put_gex"]
    out = work.groupby("expiry", as_index=False).agg(
        net_gex=("total_gex", "sum"),
        strikes=("strike", "nunique"),
        rows=("strike", "size"),
        spot=("spot_price", "last"),
    )
    return out.sort_values("expiry")


def choose_expiry_window(expiries: List[pd.Timestamp], today: pd.Timestamp, min_dte: int, max_dte: int) -> Optional[str]:
    if not expiries:
        return None
    normalized = sorted({pd.Timestamp(x).normalize() for x in expiries})
    candidates = []
    target = (min_dte + max_dte) / 2.0
    for exp in normalized:
        dte = int((exp - today).days)
        if min_dte <= dte <= max_dte:
            candidates.append((abs(dte - target), dte, exp))
    if candidates:
        best = sorted(candidates, key=lambda x: (x[0], x[1]))[0][2]
        return best.strftime("%Y-%m-%d")
    later = [x for x in normalized if int((x - today).days) > max_dte]
    if later:
        return later[0].strftime("%Y-%m-%d")
    return normalized[-1].strftime("%Y-%m-%d")


def concentration_score(profile: Profile) -> float:
    levels = profile.pos_levels + profile.neg_levels + profile.abs_levels
    if not levels:
        return 0.0
    spot = max(profile.spot, 1.0)
    near = sum(1 for lvl in levels if abs(lvl - spot) / spot <= 0.03)
    return min(1.0, near / max(len(levels), 1))
