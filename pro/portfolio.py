from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd

DEFAULT_IV_BY_SYMBOL = {"BTC": 0.60, "ETH": 0.70}
RISK_FREE_RATE = 0.0
POSITION_COLUMNS = ["symbol", "expiry", "strike", "type", "quantity", "avg_price"]


def normalize_positions(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame(columns=POSITION_COLUMNS)
    else:
        df = df.copy()
    for col in POSITION_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[POSITION_COLUMNS]
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["type"] = df["type"].astype(str).str.lower().str.strip()
    df["type"] = df["type"].replace({"c": "call", "p": "put"})
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)
    df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce").fillna(0.0)
    df = df[df["type"].isin(["call", "put"]) & df["strike"].notna()]
    return df


def load_positions(path: Path) -> pd.DataFrame:
    if not path or not Path(path).exists():
        return normalize_positions(pd.DataFrame())
    df = pd.read_csv(path)
    return normalize_positions(df)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


def _bs_d1_d2(spot: float, strike: float, t: float, vol: float, r: float) -> Optional[Dict[str, float]]:
    if spot <= 0 or strike <= 0 or t <= 0 or vol <= 0:
        return None
    vsqrt = vol * math.sqrt(t)
    if vsqrt == 0:
        return None
    d1 = (math.log(spot / strike) + (r + 0.5 * vol * vol) * t) / vsqrt
    d2 = d1 - vsqrt
    return {"d1": d1, "d2": d2, "vsqrt": vsqrt}


def bs_price(option_type: str, spot: float, strike: float, t: float, vol: float, r: float = RISK_FREE_RATE) -> float:
    intrinsic = max(spot - strike, 0.0) if option_type == "call" else max(strike - spot, 0.0)
    if t <= 0 or vol <= 0 or spot <= 0 or strike <= 0:
        return intrinsic
    d = _bs_d1_d2(spot, strike, t, vol, r)
    if not d:
        return intrinsic
    d1 = d["d1"]
    d2 = d["d2"]
    if option_type == "call":
        return spot * _norm_cdf(d1) - strike * math.exp(-r * t) * _norm_cdf(d2)
    return strike * math.exp(-r * t) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def bs_greeks(option_type: str, spot: float, strike: float, t: float, vol: float, r: float = RISK_FREE_RATE) -> Dict[str, float]:
    if t <= 0 or vol <= 0 or spot <= 0 or strike <= 0:
        delta = 1.0 if (option_type == "call" and spot > strike) else -1.0 if (option_type == "put" and spot < strike) else 0.0
        return {"delta": delta, "gamma": 0.0, "vega": 0.0}
    d = _bs_d1_d2(spot, strike, t, vol, r)
    if not d:
        return {"delta": 0.0, "gamma": 0.0, "vega": 0.0}
    d1 = d["d1"]
    pdf = _norm_pdf(d1)
    if option_type == "call":
        delta = _norm_cdf(d1)
    else:
        delta = _norm_cdf(d1) - 1.0
    gamma = pdf / (spot * d["vsqrt"]) if spot > 0 else 0.0
    vega = spot * pdf * math.sqrt(t)
    return {"delta": delta, "gamma": gamma, "vega": vega}


def _time_to_expiry(expiry: pd.Timestamp, now: pd.Timestamp) -> float:
    if pd.isna(expiry):
        return 0.0
    exp_ts = pd.Timestamp(expiry)
    now_ts = pd.Timestamp(now)
    exp_ts = exp_ts.tz_localize("UTC") if exp_ts.tzinfo is None else exp_ts.tz_convert("UTC")
    now_ts = now_ts.tz_localize("UTC") if now_ts.tzinfo is None else now_ts.tz_convert("UTC")
    dt = (exp_ts - now_ts).total_seconds()
    return max(dt / (365.0 * 24.0 * 60.0 * 60.0), 0.0)


def build_portfolio_snapshot(
    positions: pd.DataFrame,
    options_df: pd.DataFrame,
    symbol: str,
    spot_shift_pct: float = 0.0,
    vol_shift_pct: float = 0.0,
) -> Dict[str, Any]:
    if positions is None or positions.empty:
        return {"ok": False, "reason": "no positions"}
    df_symbol = positions[positions["symbol"] == symbol].copy()
    if df_symbol.empty:
        return {"ok": False, "reason": "no positions for symbol"}
    spot = None
    if options_df is not None and not options_df.empty:
        spot_rows = options_df[options_df["symbol"] == symbol]
        if not spot_rows.empty:
            spot = float(spot_rows["spot_price"].iloc[-1])
    if spot is None:
        spot = float(df_symbol["strike"].median()) if not df_symbol["strike"].isna().all() else 0.0
    base_vol = DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60)
    now = pd.Timestamp.now(tz="UTC")

    scenario_spot = spot * (1.0 + float(spot_shift_pct) / 100.0)
    scenario_vol = max(0.05, base_vol * (1.0 + float(vol_shift_pct) / 100.0))

    rows = []
    for _, row in df_symbol.iterrows():
        expiry = pd.to_datetime(row.get("expiry"), errors="coerce")
        t = _time_to_expiry(expiry, now)
        option_type = row.get("type", "call")
        strike = float(row.get("strike"))
        qty = float(row.get("quantity"))
        avg_price = float(row.get("avg_price"))

        greeks = bs_greeks(option_type, spot, strike, t, base_vol)
        price_now = bs_price(option_type, spot, strike, t, base_vol)
        price_scn = bs_price(option_type, scenario_spot, strike, t, scenario_vol)

        delta = greeks["delta"] * qty
        gamma = greeks["gamma"] * qty
        vega = greeks["vega"] * qty
        gex = greeks["gamma"] * (spot ** 2) * qty

        pnl = (price_now - avg_price) * qty
        pnl_scn = (price_scn - avg_price) * qty

        rows.append(
            {
                "expiry": expiry.date().isoformat() if pd.notna(expiry) else "n/a",
                "strike": strike,
                "type": option_type,
                "quantity": qty,
                "delta": delta,
                "gamma": gamma,
                "vega": vega,
                "gex": gex,
                "pnl": pnl,
                "pnl_scn": pnl_scn,
            }
        )

    out = pd.DataFrame(rows)
    summary = {
        "spot": spot,
        "base_vol": base_vol,
        "scenario_spot": scenario_spot,
        "scenario_vol": scenario_vol,
        "net_delta": float(out["delta"].sum()),
        "net_gamma": float(out["gamma"].sum()),
        "net_vega": float(out["vega"].sum()),
        "net_gex": float(out["gex"].sum()),
        "pnl": float(out["pnl"].sum()),
        "pnl_scn": float(out["pnl_scn"].sum()),
    }

    by_expiry = out.groupby("expiry", as_index=False).agg(
        net_delta=("delta", "sum"),
        net_gamma=("gamma", "sum"),
        net_vega=("vega", "sum"),
        net_gex=("gex", "sum"),
        pnl=("pnl", "sum"),
        pnl_scn=("pnl_scn", "sum"),
    )

    by_strike = out.groupby("strike", as_index=False).agg(
        net_delta=("delta", "sum"),
        net_gamma=("gamma", "sum"),
        net_vega=("vega", "sum"),
        net_gex=("gex", "sum"),
        pnl=("pnl", "sum"),
        pnl_scn=("pnl_scn", "sum"),
    )

    return {
        "ok": True,
        "summary": summary,
        "by_expiry": by_expiry.sort_values("expiry"),
        "by_strike": by_strike.sort_values("strike"),
        "positions": out,
    }
