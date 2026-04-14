from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from pro.portfolio import DEFAULT_IV_BY_SYMBOL, bs_greeks, bs_price

DERIBIT_PUBLIC_BASE_URL = "https://www.deribit.com/api/v2/public"
CHAIN_CACHE_TTL_SEC = 20
FLOW_CACHE_TTL_SEC = 15
DEFAULT_COMMISSION_PER_CONTRACT = 2.0
DEFAULT_STRATEGY_TEMPLATES = [
    ("custom", "Custom"),
    ("long_call", "Long Call"),
    ("long_put", "Long Put"),
    ("bull_call_spread", "Bull Call Spread"),
    ("bear_put_spread", "Bear Put Spread"),
    ("bull_put_spread", "Bull Put Spread"),
    ("bear_call_spread", "Bear Call Spread"),
    ("straddle", "Straddle"),
    ("strangle", "Strangle"),
    ("iron_condor", "Iron Condor"),
    ("calendar_call", "Calendar Call Spread"),
    ("calendar_put", "Calendar Put Spread"),
    ("covered_call", "Covered Call"),
    ("protective_put", "Protective Put"),
    ("collar", "Collar"),
]
OPTIMIZER_TEMPLATE_GROUPS = {
    "bullish": ["long_call", "bull_call_spread", "bull_put_spread", "covered_call", "protective_put"],
    "bearish": ["long_put", "bear_put_spread", "bear_call_spread"],
    "neutral": ["iron_condor", "calendar_call", "calendar_put", "covered_call"],
    "volatility": ["straddle", "strangle", "calendar_call", "calendar_put"],
}
_TEMPLATE_BIAS = {
    "long_call": "bullish",
    "long_put": "bearish",
    "bull_call_spread": "bullish",
    "bear_put_spread": "bearish",
    "bull_put_spread": "bullish",
    "bear_call_spread": "bearish",
    "straddle": "volatility",
    "strangle": "volatility",
    "iron_condor": "neutral",
    "calendar_call": "neutral",
    "calendar_put": "neutral",
    "covered_call": "neutral",
    "protective_put": "bullish",
    "collar": "neutral",
    "custom": "custom",
}
_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _now_ts() -> float:
    return time.time()


def _get_deribit(endpoint: str, params: Dict[str, Any], timeout: int = 8) -> Dict[str, Any]:
    query = urllib.parse.urlencode(params)
    url = f"{DERIBIT_PUBLIC_BASE_URL}/{endpoint}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": "gex-dashboard/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError):
        return {}
    return payload.get("result") or {}


def _cache_get(namespace: str, symbol: str, ttl_sec: int) -> Optional[Any]:
    key = (namespace, symbol.upper())
    item = _CACHE.get(key)
    if not item:
        return None
    if (_now_ts() - float(item.get("ts", 0.0))) > ttl_sec:
        return None
    value = item.get("value")
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return json.loads(json.dumps(value))


def _cache_put(namespace: str, symbol: str, value: Any) -> None:
    key = (namespace, symbol.upper())
    cached = value.copy() if isinstance(value, pd.DataFrame) else json.loads(json.dumps(value))
    _CACHE[key] = {"ts": _now_ts(), "value": cached}


def fetch_deribit_option_chain(symbol: str) -> pd.DataFrame:
    cached = _cache_get("chain", symbol, CHAIN_CACHE_TTL_SEC)
    if isinstance(cached, pd.DataFrame):
        return cached

    symbol = str(symbol or "BTC").upper()
    instruments = _get_deribit("get_instruments", {"currency": symbol, "kind": "option", "expired": "false"})
    summaries = _get_deribit("get_book_summary_by_currency", {"currency": symbol, "kind": "option"})
    if not isinstance(instruments, list):
        instruments = []
    if not isinstance(summaries, list):
        summaries = []

    summary_map = {str(item.get("instrument_name")): item for item in summaries if item.get("instrument_name")}
    rows: List[Dict[str, Any]] = []
    for inst in instruments:
        name = str(inst.get("instrument_name") or "")
        if not name:
            continue
        summary = summary_map.get(name, {})
        expiry = pd.to_datetime(inst.get("expiration_timestamp"), unit="ms", utc=True, errors="coerce")
        if pd.isna(expiry):
            continue
        option_type = str(inst.get("option_type") or "").lower()
        if option_type not in {"call", "put"}:
            continue
        strike = pd.to_numeric(inst.get("strike"), errors="coerce")
        if not pd.notna(strike):
            continue
        bid = pd.to_numeric(summary.get("bid_price"), errors="coerce")
        ask = pd.to_numeric(summary.get("ask_price"), errors="coerce")
        mark = pd.to_numeric(summary.get("mark_price"), errors="coerce")
        mid = pd.to_numeric(summary.get("mid_price"), errors="coerce")
        if not pd.notna(mid) or float(mid) <= 0:
            if pd.notna(bid) and pd.notna(ask) and float(bid) > 0 and float(ask) > 0:
                mid = (float(bid) + float(ask)) / 2.0
            else:
                mid = mark
        spot = pd.to_numeric(summary.get("underlying_price"), errors="coerce")
        if not pd.notna(spot) or float(spot) <= 0:
            spot = pd.to_numeric(summary.get("estimated_delivery_price"), errors="coerce")
        iv = pd.to_numeric(summary.get("mark_iv"), errors="coerce")
        iv = float(iv) / 100.0 if pd.notna(iv) and float(iv) > 0 else None
        mid_usd = float(mid) * float(spot) if pd.notna(mid) and pd.notna(spot) and float(mid) > 0 and float(spot) > 0 else None
        rows.append(
            {
                "instrument_name": name,
                "symbol": symbol,
                "expiry": expiry.strftime("%Y-%m-%d"),
                "expiry_ts": expiry,
                "strike": float(strike),
                "type": option_type,
                "bid": float(bid) if pd.notna(bid) else None,
                "ask": float(ask) if pd.notna(ask) else None,
                "mark": float(mark) if pd.notna(mark) else None,
                "mid": float(mid) if pd.notna(mid) else None,
                "mid_usd": mid_usd,
                "iv": iv,
                "open_interest": float(summary.get("open_interest") or 0.0),
                "volume": float(summary.get("volume") or 0.0),
                "volume_usd": float(summary.get("volume_usd") or 0.0),
                "spot": float(spot) if pd.notna(spot) else None,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "instrument_name",
                "symbol",
                "expiry",
                "expiry_ts",
                "strike",
                "type",
                "bid",
                "ask",
                "mark",
                "mid",
                "mid_usd",
                "iv",
                "open_interest",
                "volume",
                "volume_usd",
                "spot",
            ]
        )
    else:
        out = out.sort_values(["expiry_ts", "strike", "type"]).reset_index(drop=True)
    _cache_put("chain", symbol, out)
    return out.copy()


def parse_deribit_instrument_name(instrument_name: str) -> Tuple[Optional[str], Optional[float], Optional[str]]:
    parts = str(instrument_name or "").split("-")
    if len(parts) != 4:
        return None, None, None
    _, expiry_raw, strike_raw, option_raw = parts
    try:
        expiry = pd.to_datetime(expiry_raw, format="%d%b%y", utc=True).strftime("%Y-%m-%d")
    except Exception:
        return None, None, None
    try:
        strike = float(strike_raw)
    except (TypeError, ValueError):
        return None, None, None
    option_type = "call" if option_raw.upper().startswith("C") else "put" if option_raw.upper().startswith("P") else None
    return expiry, strike, option_type


def fetch_deribit_options_flow(symbol: str, count: int = 60) -> pd.DataFrame:
    symbol = str(symbol or "BTC").upper()
    cache_key = f"{symbol}:{int(count)}"
    cached = _cache_get("flow", cache_key, FLOW_CACHE_TTL_SEC)
    if isinstance(cached, pd.DataFrame):
        return cached

    payload = _get_deribit(
        "get_last_trades_by_currency",
        {"currency": symbol, "kind": "option", "count": int(max(5, min(count, 150)))},
    )
    trades = payload.get("trades", []) if isinstance(payload, dict) else []
    rows: List[Dict[str, Any]] = []
    for item in trades:
        inst = str(item.get("instrument_name") or "")
        expiry, strike, option_type = parse_deribit_instrument_name(inst)
        if not expiry or strike is None or option_type is None:
            continue
        price = float(item.get("price") or 0.0)
        mark_price = float(item.get("mark_price") or 0.0)
        index_price = float(item.get("index_price") or 0.0)
        contracts = float(item.get("contracts") or item.get("amount") or 0.0)
        premium_usd = price * index_price * contracts if price > 0 and index_price > 0 else 0.0
        slippage_bps = ((price - mark_price) / mark_price * 10000.0) if mark_price > 0 else 0.0
        rows.append(
            {
                "timestamp": pd.to_datetime(item.get("timestamp"), unit="ms", utc=True, errors="coerce"),
                "symbol": symbol,
                "instrument_name": inst,
                "expiry": expiry,
                "strike": float(strike),
                "type": option_type,
                "direction": str(item.get("direction") or "").lower(),
                "contracts": contracts,
                "price": price,
                "mark_price": mark_price,
                "index_price": index_price,
                "iv": float(item.get("iv") or 0.0) / 100.0 if item.get("iv") is not None else None,
                "premium_usd": premium_usd,
                "slippage_bps": slippage_bps,
                "trade_id": str(item.get("trade_id") or ""),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("timestamp", ascending=False).reset_index(drop=True)
    _cache_put("flow", cache_key, out)
    return out.copy()


def option_chain_store_data(chain_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if chain_df is None or chain_df.empty:
        return []
    work = chain_df.copy()
    if "expiry_ts" in work.columns:
        work["expiry_ts"] = pd.to_datetime(work["expiry_ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return work.to_dict("records")


def option_chain_from_store(data: List[Dict[str, Any]]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "expiry_ts" in df.columns:
        df["expiry_ts"] = pd.to_datetime(df["expiry_ts"], utc=True, errors="coerce")
    if "strike" in df.columns:
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    numeric_cols = ["bid", "ask", "mark", "mid", "mid_usd", "iv", "open_interest", "volume", "volume_usd", "spot"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_chain_spot(chain_df: pd.DataFrame, symbol: str) -> Optional[float]:
    """Median of the per-row underlying_price column, or None if unavailable.

    Previously this returned ``100 / DEFAULT_IV`` (≈166 for BTC) on failure,
    which silently propagated a fake price into ATM strike selection, vol-
    surface centering, and idea generation. Callers must now handle None.
    """
    if chain_df is not None and not chain_df.empty and "spot" in chain_df.columns:
        vals = pd.to_numeric(chain_df["spot"], errors="coerce").dropna()
        if not vals.empty:
            return float(vals.median())
    return None


def list_expiries(chain_df: pd.DataFrame) -> List[str]:
    if chain_df is None or chain_df.empty:
        return []
    expiries = chain_df["expiry"].dropna().astype(str).drop_duplicates().tolist()
    return sorted(expiries)


def list_strikes(chain_df: pd.DataFrame, expiry: Optional[str], leg_type: Optional[str]) -> List[float]:
    if chain_df is None or chain_df.empty:
        return []
    work = chain_df.copy()
    if expiry:
        work = work[work["expiry"] == str(expiry)]
    if leg_type in {"call", "put"}:
        work = work[work["type"] == leg_type]
    strikes = sorted({float(x) for x in pd.to_numeric(work["strike"], errors="coerce").dropna().tolist()})
    return strikes


def nearest_strike(strikes: List[float], target: float) -> Optional[float]:
    values = [float(x) for x in strikes if x is not None]
    if not values:
        return None
    return min(values, key=lambda item: abs(float(item) - float(target)))


def next_strike(strikes: List[float], anchor: Optional[float], direction: str = "up", steps: int = 1) -> Optional[float]:
    if anchor is None:
        return None
    values = sorted({float(x) for x in strikes if x is not None})
    if direction == "up":
        higher = [x for x in values if x > float(anchor)]
        if not higher:
            return anchor
        return higher[min(max(int(steps) - 1, 0), len(higher) - 1)]
    lower = [x for x in values if x < float(anchor)]
    if not lower:
        return anchor
    return lower[-min(max(int(steps), 1), len(lower))]


def make_blank_leg(row_id: int) -> Dict[str, Any]:
    return {
        "row_id": int(row_id),
        "enabled": row_id == 1,
        "action": "buy",
        "type": "call",
        "expiry": None,
        "strike": None,
        "quantity": 1.0,
    }


def default_builder_legs(template_id: str, chain_df: pd.DataFrame, symbol: str) -> List[Dict[str, Any]]:
    template_id = str(template_id or "custom")
    legs = [make_blank_leg(i) for i in range(1, 5)]
    expiries = list_expiries(chain_df)
    if not expiries:
        return legs
    primary = expiries[0]
    secondary = expiries[1] if len(expiries) > 1 else expiries[0]
    spot = get_chain_spot(chain_df, symbol)
    primary_strikes = list_strikes(chain_df, primary, "call")
    secondary_strikes = list_strikes(chain_df, secondary, "call")
    if not primary_strikes:
        return legs
    atm = nearest_strike(primary_strikes, spot)
    above_1 = next_strike(primary_strikes, atm, direction="up", steps=1)
    above_2 = next_strike(primary_strikes, atm, direction="up", steps=2)
    below_1 = next_strike(primary_strikes, atm, direction="down", steps=1)
    below_2 = next_strike(primary_strikes, atm, direction="down", steps=2)
    high_call = above_1 if above_1 is not None else atm
    wide_call = above_2 if above_2 is not None else high_call
    low_put = below_1 if below_1 is not None else atm
    wide_put = below_2 if below_2 is not None else low_put
    secondary_atm = nearest_strike(secondary_strikes or primary_strikes, spot)

    def set_leg(idx: int, enabled: bool, action: str, leg_type: str, expiry: Optional[str], strike: Optional[float], quantity: float = 1.0) -> None:
        legs[idx - 1].update(
            {
                "enabled": enabled,
                "action": action,
                "type": leg_type,
                "expiry": expiry,
                "strike": float(strike) if strike is not None else None,
                "quantity": float(quantity),
            }
        )

    if template_id == "long_call":
        set_leg(1, True, "buy", "call", primary, atm)
    elif template_id == "long_put":
        set_leg(1, True, "buy", "put", primary, atm)
    elif template_id == "bull_call_spread":
        set_leg(1, True, "buy", "call", primary, atm)
        set_leg(2, True, "sell", "call", primary, high_call)
    elif template_id == "bear_put_spread":
        set_leg(1, True, "buy", "put", primary, atm)
        set_leg(2, True, "sell", "put", primary, low_put)
    elif template_id == "bull_put_spread":
        set_leg(1, True, "sell", "put", primary, atm)
        set_leg(2, True, "buy", "put", primary, low_put)
    elif template_id == "bear_call_spread":
        set_leg(1, True, "sell", "call", primary, atm)
        set_leg(2, True, "buy", "call", primary, high_call)
    elif template_id == "straddle":
        set_leg(1, True, "buy", "call", primary, atm)
        set_leg(2, True, "buy", "put", primary, atm)
    elif template_id == "strangle":
        set_leg(1, True, "buy", "put", primary, low_put)
        set_leg(2, True, "buy", "call", primary, high_call)
    elif template_id == "iron_condor":
        set_leg(1, True, "buy", "put", primary, wide_put)
        set_leg(2, True, "sell", "put", primary, low_put)
        set_leg(3, True, "sell", "call", primary, high_call)
        set_leg(4, True, "buy", "call", primary, wide_call)
    elif template_id == "calendar_call":
        set_leg(1, True, "sell", "call", primary, atm)
        set_leg(2, True, "buy", "call", secondary, secondary_atm)
    elif template_id == "calendar_put":
        set_leg(1, True, "sell", "put", primary, atm)
        set_leg(2, True, "buy", "put", secondary, secondary_atm)
    elif template_id == "covered_call":
        set_leg(1, True, "buy", "spot", None, None)
        set_leg(2, True, "sell", "call", primary, high_call)
    elif template_id == "protective_put":
        set_leg(1, True, "buy", "spot", None, None)
        set_leg(2, True, "buy", "put", primary, low_put)
    elif template_id == "collar":
        set_leg(1, True, "buy", "spot", None, None)
        set_leg(2, True, "buy", "put", primary, low_put)
        set_leg(3, True, "sell", "call", primary, high_call)
    return legs


def normalize_builder_legs(legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(legs or [], start=1):
        leg = dict(raw or {})
        leg.setdefault("row_id", idx)
        leg["enabled"] = bool(leg.get("enabled", False))
        leg["action"] = str(leg.get("action") or "buy").lower()
        if leg["action"] not in {"buy", "sell"}:
            leg["action"] = "buy"
        leg["type"] = str(leg.get("type") or "call").lower()
        if leg["type"] not in {"call", "put", "spot"}:
            leg["type"] = "call"
        expiry = leg.get("expiry")
        leg["expiry"] = str(expiry) if expiry not in {None, "", "None"} else None
        strike = pd.to_numeric(leg.get("strike"), errors="coerce")
        leg["strike"] = float(strike) if pd.notna(strike) else None
        qty = pd.to_numeric(leg.get("quantity"), errors="coerce")
        leg["quantity"] = float(qty) if pd.notna(qty) and float(qty) > 0 else 0.0
        out.append(leg)
    return out


def time_to_expiry(expiry: Any, now: Optional[pd.Timestamp] = None) -> float:
    now_ts = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize("UTC")
    exp_ts = pd.to_datetime(expiry, utc=True, errors="coerce")
    if pd.isna(exp_ts):
        return 0.0
    total_seconds = (exp_ts - now_ts).total_seconds()
    return max(total_seconds / (365.0 * 24.0 * 60.0 * 60.0), 0.0)


def build_price_grid(spot: float, strikes: List[float]) -> List[float]:
    anchors = [float(spot)] + [float(x) for x in strikes if x is not None]
    low = min(anchors) if anchors else float(spot)
    high = max(anchors) if anchors else float(spot)
    left = max(1.0, min(low, float(spot)) * 0.6)
    right = max(high, float(spot)) * 1.45
    if right <= left:
        right = left + 10.0
    step_count = 121
    grid = [left + ((right - left) * i / (step_count - 1)) for i in range(step_count)]
    if float(spot) not in grid:
        grid.append(float(spot))
        grid = sorted(grid)
    return grid


def _quote_for_leg(chain_df: pd.DataFrame, symbol: str, leg: Dict[str, Any], spot: float) -> Dict[str, Any]:
    leg_type = leg.get("type")
    if leg_type == "spot":
        return {
            "instrument_name": f"{symbol}-SPOT",
            "entry_price": float(spot),
            "iv": DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60),
            "spot": float(spot),
        }
    expiry = leg.get("expiry")
    strike = leg.get("strike")
    if chain_df is None or chain_df.empty or not expiry or strike is None:
        return {}
    row = chain_df[
        (chain_df["expiry"] == str(expiry))
        & (pd.to_numeric(chain_df["strike"], errors="coerce") == float(strike))
        & (chain_df["type"] == leg_type)
    ]
    if row.empty:
        return {}
    record = row.iloc[0].to_dict()
    entry_price = record.get("mid_usd")
    if entry_price is None or not pd.notna(entry_price) or float(entry_price) <= 0:
        base_iv = record.get("iv") or DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60)
        t = time_to_expiry(record.get("expiry"))
        entry_price = bs_price(str(leg_type), float(spot), float(strike), t, max(float(base_iv), 0.05))
    record["entry_price"] = float(entry_price)
    record["spot"] = float(record.get("spot") or spot)
    record["iv"] = float(record.get("iv") or DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60))
    return record


def _leg_mark_to_market(leg: Dict[str, Any], quote: Dict[str, Any], price: float, horizon_days: float) -> float:
    leg_type = str(leg.get("type") or "call")
    if leg_type == "spot":
        return float(price)
    expiry = quote.get("expiry") or leg.get("expiry")
    strike = float(leg.get("strike"))
    full_t = time_to_expiry(expiry)
    horizon_years = max(float(horizon_days), 0.0) / 365.0
    remaining_t = max(full_t - horizon_years, 0.0)
    base_iv = float(quote.get("iv") or DEFAULT_IV_BY_SYMBOL.get(str(quote.get("symbol") or "BTC"), 0.60))
    return bs_price(leg_type, float(price), strike, remaining_t, max(base_iv, 0.05))


def _leg_greeks_at(leg: Dict[str, Any], quote: Dict[str, Any], price: float, horizon_days: float) -> Dict[str, float]:
    leg_type = str(leg.get("type") or "call")
    if leg_type == "spot":
        return {"delta": 1.0, "gamma": 0.0, "vega": 0.0}
    expiry = quote.get("expiry") or leg.get("expiry")
    strike = float(leg.get("strike"))
    full_t = time_to_expiry(expiry)
    horizon_years = max(float(horizon_days), 0.0) / 365.0
    remaining_t = max(full_t - horizon_years, 0.0)
    base_iv = float(quote.get("iv") or DEFAULT_IV_BY_SYMBOL.get(str(quote.get("symbol") or "BTC"), 0.60))
    return bs_greeks(leg_type, float(price), strike, remaining_t, max(base_iv, 0.05))


def _zero_crossings(price_grid: List[float], values: List[float]) -> List[float]:
    out: List[float] = []
    for idx in range(len(price_grid) - 1):
        x1, x2 = float(price_grid[idx]), float(price_grid[idx + 1])
        y1, y2 = float(values[idx]), float(values[idx + 1])
        if y1 == 0:
            out.append(x1)
            continue
        if y1 * y2 > 0:
            continue
        if y2 == y1:
            out.append(x1)
            continue
        ratio = abs(y1) / (abs(y1) + abs(y2))
        out.append(x1 + (x2 - x1) * ratio)
    cleaned: List[float] = []
    for value in out:
        if not cleaned or abs(cleaned[-1] - value) > 1.0:
            cleaned.append(value)
    return cleaned


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _lognormal_interval_probability(left: float, right: float, spot: float, vol: float, t: float) -> float:
    left = max(left, 1e-9)
    right = max(right, left + 1e-9)
    if t <= 0 or vol <= 0:
        return 1.0 if left <= spot <= right else 0.0
    mu = math.log(max(spot, 1e-9)) - 0.5 * (vol ** 2) * t
    sigma = max(vol * math.sqrt(t), 1e-9)
    z_left = (math.log(left) - mu) / sigma
    z_right = (math.log(right) - mu) / sigma
    return max(0.0, _norm_cdf(z_right) - _norm_cdf(z_left))


def estimate_probability_of_profit(price_grid: List[float], expiry_values: List[float], spot: float, vol: float, t: float) -> float:
    if not price_grid or not expiry_values or len(price_grid) != len(expiry_values):
        return 0.0
    total = 0.0
    for idx in range(len(price_grid) - 1):
        left, right = float(price_grid[idx]), float(price_grid[idx + 1])
        y1, y2 = float(expiry_values[idx]), float(expiry_values[idx + 1])
        if y1 >= 0 and y2 >= 0:
            total += _lognormal_interval_probability(left, right, spot, vol, t)
            continue
        if y1 < 0 and y2 < 0:
            continue
        if y2 == y1:
            continue
        zero = left + (right - left) * (abs(y1) / (abs(y1) + abs(y2)))
        if y1 >= 0:
            total += _lognormal_interval_probability(left, zero, spot, vol, t)
        else:
            total += _lognormal_interval_probability(zero, right, spot, vol, t)
    return min(max(total, 0.0), 1.0)


def classify_strategy_bias(legs: List[Dict[str, Any]]) -> str:
    score = 0.0
    for leg in legs:
        qty = float(leg.get("quantity") or 0.0)
        sign = 1.0 if leg.get("action") == "buy" else -1.0
        leg_type = leg.get("type")
        if leg_type == "call":
            score += sign * qty
        elif leg_type == "put":
            score -= sign * qty
        elif leg_type == "spot":
            score += sign * qty
    if score > 0.5:
        return "bullish"
    if score < -0.5:
        return "bearish"
    return "neutral"


def evaluate_strategy(
    chain_df: pd.DataFrame,
    symbol: str,
    legs: List[Dict[str, Any]],
    commission_per_contract: float = DEFAULT_COMMISSION_PER_CONTRACT,
    eval_days: float = 7.0,
) -> Dict[str, Any]:
    symbol = str(symbol or "BTC").upper()
    norm_legs = [leg for leg in normalize_builder_legs(legs) if leg.get("enabled") and float(leg.get("quantity") or 0) > 0]
    if not norm_legs:
        return {"ok": False, "reason": "no active legs"}

    spot = get_chain_spot(chain_df, symbol)
    enriched: List[Dict[str, Any]] = []
    strike_anchors: List[float] = []
    total_commission = 0.0
    premium_out = 0.0
    premium_in = 0.0
    for leg in norm_legs:
        quote = _quote_for_leg(chain_df, symbol, leg, spot)
        if not quote:
            return {"ok": False, "reason": f"missing quote for row {leg.get('row_id')}"}
        entry_price = float(quote.get("entry_price") or 0.0)
        qty = float(leg.get("quantity") or 0.0)
        if qty <= 0:
            continue
        sign = 1.0 if leg.get("action") == "buy" else -1.0
        if sign > 0:
            premium_out += entry_price * qty
        else:
            premium_in += entry_price * qty
        total_commission += abs(qty) * float(commission_per_contract) * 2.0
        if leg.get("strike") is not None:
            strike_anchors.append(float(leg.get("strike")))
        item = dict(leg)
        item.update(
            {
                "entry_price": entry_price,
                "iv": float(quote.get("iv") or DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60)),
                "instrument_name": quote.get("instrument_name"),
                "spot": float(quote.get("spot") or spot),
            }
        )
        enriched.append(item)

    if not enriched:
        return {"ok": False, "reason": "no priced legs"}

    grid = build_price_grid(spot, strike_anchors)
    curves = {"now": [], "eval": [], "expiry": []}
    greek_now = {"delta": 0.0, "gamma": 0.0, "vega": 0.0}
    greek_eval = {"delta": 0.0, "gamma": 0.0, "vega": 0.0}
    avg_iv = float(pd.Series([float(leg.get("iv") or DEFAULT_IV_BY_SYMBOL.get(symbol, 0.60)) for leg in enriched]).median())
    max_t = max([time_to_expiry(leg.get("expiry")) for leg in enriched if leg.get("type") != "spot"] + [0.0])

    for price in grid:
        pnl_now = 0.0
        pnl_eval = 0.0
        pnl_expiry = 0.0
        for leg in enriched:
            qty = float(leg.get("quantity") or 0.0)
            sign = 1.0 if leg.get("action") == "buy" else -1.0
            current_value = _leg_mark_to_market(leg, leg, price, 0.0)
            eval_value = _leg_mark_to_market(leg, leg, price, eval_days)
            expiry_value = _leg_mark_to_market(leg, leg, price, max_t * 365.0)
            pnl_now += sign * qty * (current_value - float(leg.get("entry_price") or 0.0))
            pnl_eval += sign * qty * (eval_value - float(leg.get("entry_price") or 0.0))
            pnl_expiry += sign * qty * (expiry_value - float(leg.get("entry_price") or 0.0))
        curves["now"].append(pnl_now - total_commission)
        curves["eval"].append(pnl_eval - total_commission)
        curves["expiry"].append(pnl_expiry - total_commission)

    for leg in enriched:
        qty = float(leg.get("quantity") or 0.0)
        sign = 1.0 if leg.get("action") == "buy" else -1.0
        g_now = _leg_greeks_at(leg, leg, spot, 0.0)
        g_eval = _leg_greeks_at(leg, leg, spot, eval_days)
        for key in greek_now:
            greek_now[key] += sign * qty * float(g_now.get(key, 0.0))
            greek_eval[key] += sign * qty * float(g_eval.get(key, 0.0))

    slope_up = 0.0
    for leg in enriched:
        qty = float(leg.get("quantity") or 0.0)
        sign = 1.0 if leg.get("action") == "buy" else -1.0
        if leg.get("type") in {"call", "spot"}:
            slope_up += sign * qty
    max_profit = None if slope_up > 0 else float(max(curves["expiry"]))
    max_loss = None if slope_up < 0 else float(abs(min(curves["expiry"])))
    breakevens = _zero_crossings(grid, curves["expiry"])
    pop = estimate_probability_of_profit(grid, curves["expiry"], spot, max(avg_iv, 0.05), max_t)

    scenario_steps = [-10, -5, -2, 0, 2, 5, 10]
    scenario_rows: List[Dict[str, Any]] = []
    for move_pct in scenario_steps:
        px = float(spot) * (1.0 + (move_pct / 100.0))
        nearest_idx = min(range(len(grid)), key=lambda i: abs(grid[i] - px))
        scenario_rows.append(
            {
                "move_pct": move_pct,
                "spot": px,
                "now_pnl": curves["now"][nearest_idx],
                "eval_pnl": curves["eval"][nearest_idx],
                "expiry_pnl": curves["expiry"][nearest_idx],
            }
        )

    net_debit = premium_out - premium_in + total_commission
    net_credit = max(-net_debit, 0.0)
    net_cost = max(net_debit, 0.0)
    return {
        "ok": True,
        "symbol": symbol,
        "spot": float(spot),
        "legs": enriched,
        "premium_out": float(premium_out),
        "premium_in": float(premium_in),
        "net_debit": float(net_debit),
        "net_cost": float(net_cost),
        "net_credit": float(net_credit),
        "commission_total": float(total_commission),
        "max_profit": max_profit,
        "max_loss": max_loss,
        "breakevens": breakevens,
        "probability_of_profit": float(pop),
        "avg_iv": float(avg_iv),
        "net_greeks_now": greek_now,
        "net_greeks_eval": greek_eval,
        "grid": grid,
        "curves": curves,
        "scenario_rows": scenario_rows,
        "eval_days": float(max(eval_days, 0.0)),
        "max_days": float(max_t * 365.0),
        "bias": classify_strategy_bias(enriched),
    }


def template_label(template_id: str) -> str:
    for value, label in DEFAULT_STRATEGY_TEMPLATES:
        if value == template_id:
            return label
    return str(template_id).replace("_", " ").title()


def score_optimizer_candidate(report: Dict[str, Any], template_id: str, bias: str, objective: str) -> float:
    pop = float(report.get("probability_of_profit") or 0.0)
    max_profit = report.get("max_profit")
    max_loss = report.get("max_loss")
    if max_profit is None:
        reward_score = 1.5
    elif max_loss in (None, 0):
        reward_score = 0.0
    else:
        reward_score = float(max_profit) / max(float(max_loss), 1.0)
    reward_score = min(reward_score, 5.0)
    cost_penalty = float(report.get("net_cost") or 0.0) / max(float(report.get("spot") or 1.0), 1.0)
    bias_bonus = 0.15 if _TEMPLATE_BIAS.get(template_id) == bias else 0.0
    objective = str(objective or "balanced")
    if objective == "max_return":
        return (reward_score * 1.25) + (pop * 0.6) - cost_penalty + bias_bonus
    if objective == "chance":
        return (pop * 2.5) + (reward_score * 0.45) - (cost_penalty * 0.5) + bias_bonus
    return (pop * 1.5) + (reward_score * 0.8) - (cost_penalty * 0.6) + bias_bonus


def build_optimizer_candidates(
    chain_df: pd.DataFrame,
    symbol: str,
    bias: str,
    objective: str = "balanced",
    eval_days: float = 7.0,
    max_cost_pct: float = 0.20,
    min_pop: float = 0.0,
    max_results: int = 12,
    max_expiries: int = 4,
    template_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    symbol = str(symbol or "BTC").upper()
    if chain_df is None or chain_df.empty:
        return []

    work = chain_df.copy()
    expiries = list_expiries(work)[: max(int(max_expiries), 1)]
    if not expiries:
        return []
    spot = get_chain_spot(work, symbol)
    selected_templates = template_ids or OPTIMIZER_TEMPLATE_GROUPS.get(str(bias or "").lower()) or [
        item[0] for item in DEFAULT_STRATEGY_TEMPLATES if item[0] != "custom"
    ]
    max_cost_pct = max(float(max_cost_pct or 0.0), 0.0)
    min_pop = max(float(min_pop or 0.0), 0.0)
    seen = set()
    candidates: List[Dict[str, Any]] = []

    def _shift_legs(legs: List[Dict[str, Any]], source_df: pd.DataFrame, step: int) -> List[Dict[str, Any]]:
        if step == 0:
            return normalize_builder_legs(legs)
        shifted: List[Dict[str, Any]] = []
        for leg in normalize_builder_legs(legs):
            item = dict(leg)
            if item.get("type") in {"call", "put"} and item.get("expiry") and item.get("strike") is not None:
                strikes = list_strikes(source_df, item.get("expiry"), item.get("type"))
                moved = next_strike(
                    strikes,
                    float(item.get("strike")),
                    direction="up" if step > 0 else "down",
                    steps=abs(int(step)),
                )
                if moved is not None:
                    item["strike"] = float(moved)
            shifted.append(item)
        return shifted

    def _register_candidate(template_id: str, legs: List[Dict[str, Any]], source_df: pd.DataFrame) -> None:
        signature = tuple(
            (
                str(leg.get("action")),
                str(leg.get("type")),
                str(leg.get("expiry")),
                float(leg.get("strike")) if leg.get("strike") is not None else None,
                float(leg.get("quantity") or 0.0),
            )
            for leg in normalize_builder_legs(legs)
            if leg.get("enabled")
        )
        if not signature or signature in seen:
            return
        seen.add(signature)
        report = evaluate_strategy(source_df, symbol, legs, eval_days=eval_days)
        if not report.get("ok"):
            return
        pop = float(report.get("probability_of_profit") or 0.0)
        if pop < min_pop:
            return
        if max_cost_pct > 0:
            cost_ratio = float(report.get("net_cost") or 0.0) / max(float(report.get("spot") or spot or 1.0), 1.0)
            if cost_ratio > max_cost_pct:
                return
        score = score_optimizer_candidate(report, template_id, str(bias or ""), objective)
        expiries_used = sorted({str(leg.get("expiry")) for leg in report.get("legs", []) if leg.get("expiry")})
        candidates.append(
            {
                "template_id": template_id,
                "template_label": template_label(template_id),
                "score": float(score),
                "probability_of_profit": pop,
                "net_cost": float(report.get("net_cost") or 0.0),
                "net_credit": float(report.get("net_credit") or 0.0),
                "max_profit": report.get("max_profit"),
                "max_loss": report.get("max_loss"),
                "breakevens": report.get("breakevens") or [],
                "primary_expiry": expiries_used[0] if expiries_used else None,
                "secondary_expiry": expiries_used[1] if len(expiries_used) > 1 else None,
                "bias": report.get("bias") or _TEMPLATE_BIAS.get(template_id) or "neutral",
                "legs": report.get("legs") or normalize_builder_legs(legs),
                "report": report,
            }
        )

    for template_id in selected_templates:
        template_id = str(template_id or "")
        if not template_id or template_id == "custom":
            continue
        if template_id.startswith("calendar_"):
            for idx in range(len(expiries) - 1):
                subset = work[work["expiry"].isin([expiries[idx], expiries[idx + 1]])].copy()
                if subset.empty:
                    continue
                base_legs = default_builder_legs(template_id, subset, symbol)
                for shift in (-1, 0, 1):
                    _register_candidate(template_id, _shift_legs(base_legs, subset, shift), subset)
            continue
        for expiry in expiries:
            subset = work[work["expiry"] == expiry].copy()
            if subset.empty:
                continue
            base_legs = default_builder_legs(template_id, subset, symbol)
            for shift in (-1, 0, 1):
                _register_candidate(template_id, _shift_legs(base_legs, subset, shift), subset)

    ordered = sorted(
        candidates,
        key=lambda item: (
            float(item.get("score") or 0.0),
            float(item.get("probability_of_profit") or 0.0),
            -(float(item.get("net_cost") or 0.0)),
        ),
        reverse=True,
    )
    return ordered[: max(int(max_results), 1)]


def load_saved_strategies(path: Path) -> List[Dict[str, Any]]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(payload, list):
        return []
    out = []
    for item in payload:
        if isinstance(item, dict) and item.get("name"):
            out.append(item)
    return out


def save_strategy(path: Path, item: Dict[str, Any]) -> List[Dict[str, Any]]:
    record = dict(item or {})
    name = str(record.get("name") or "").strip()
    if not name:
        raise ValueError("strategy name is required")
    symbol = str(record.get("symbol") or "").upper().strip()
    record["name"] = name
    record["symbol"] = symbol
    record["saved_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    existing = load_saved_strategies(path)
    filtered = [
        entry
        for entry in existing
        if not (
            str(entry.get("name") or "").strip().lower() == name.lower()
            and str(entry.get("symbol") or "").upper().strip() == symbol
        )
    ]
    filtered.append(record)
    filtered = sorted(
        filtered,
        key=lambda entry: (
            str(entry.get("symbol") or ""),
            str(entry.get("name") or "").lower(),
            str(entry.get("saved_at") or ""),
        ),
    )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(filtered, indent=2), encoding="utf-8")
    return filtered


def delete_strategy(path: Path, name: str, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    name = str(name or "").strip()
    if not name:
        return load_saved_strategies(path)
    symbol = str(symbol or "").upper().strip()
    remaining = []
    for entry in load_saved_strategies(path):
        same_name = str(entry.get("name") or "").strip().lower() == name.lower()
        same_symbol = not symbol or str(entry.get("symbol") or "").upper().strip() == symbol
        if same_name and same_symbol:
            continue
        remaining.append(entry)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(remaining, indent=2), encoding="utf-8")
    return remaining
