"""Options selling backtest engine.

Fetches historical BTC/ETH prices from Deribit (daily candles) and simulates
selling weekly or monthly options at a target delta.  Settlement is at Friday
08:00 UTC (Deribit expiry convention).

Supported strategies
--------------------
* Short Put
* Short Call
* Short Strangle   (sell OTM put + sell OTM call)
* Iron Condor      (strangle + long wings for protection)

Each trade opens on a Friday 08:00 UTC (after the previous weekly/monthly
expiry settles) and closes at the next Friday 08:00 UTC settlement price.
"""

from __future__ import annotations

import json
import math
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ── Black-Scholes helpers (reused from portfolio.py logic) ──────────────

_SQRT_2PI = math.sqrt(2.0 * math.pi)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def _bs_d1(spot: float, strike: float, t: float, vol: float) -> Optional[float]:
    if spot <= 0 or strike <= 0 or t <= 0 or vol <= 0:
        return None
    return (math.log(spot / strike) + 0.5 * vol * vol * t) / (vol * math.sqrt(t))


def bs_price(option_type: str, spot: float, strike: float, t: float, vol: float) -> float:
    d1 = _bs_d1(spot, strike, t, vol)
    if d1 is None:
        return max(spot - strike, 0.0) if option_type == "call" else max(strike - spot, 0.0)
    d2 = d1 - vol * math.sqrt(t)
    if option_type == "call":
        return spot * _norm_cdf(d1) - strike * _norm_cdf(d2)
    return strike * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def bs_delta(option_type: str, spot: float, strike: float, t: float, vol: float) -> float:
    d1 = _bs_d1(spot, strike, t, vol)
    if d1 is None:
        if option_type == "call":
            return 1.0 if spot > strike else 0.0
        return -1.0 if spot < strike else 0.0
    if option_type == "call":
        return _norm_cdf(d1)
    return _norm_cdf(d1) - 1.0


def _snap_to_deribit_strike(raw_strike: float, symbol: str) -> float:
    """Round a raw strike to the nearest valid Deribit strike increment.

    Deribit strike grids (as of 2024-2025):
      BTC — $500 steps  (e.g. 60000, 60500, 61000 …)
      ETH — $25 steps   (e.g. 2800, 2825, 2850 …)
    """
    sym = symbol.upper() if symbol else ""
    if sym == "ETH":
        step = 25.0
    else:  # BTC and default
        step = 500.0
    return round(round(raw_strike / step) * step, 2)


def strike_from_delta(option_type: str, spot: float, target_delta: float,
                      t: float, vol: float, symbol: str = "BTC") -> float:
    """Binary search for the strike that gives the target |delta|,
    snapped to the nearest valid Deribit strike increment."""
    if t <= 0 or vol <= 0 or spot <= 0:
        return _snap_to_deribit_strike(spot, symbol)
    abs_target = abs(target_delta)
    lo, hi = spot * 0.3, spot * 3.0
    for _ in range(80):
        mid = (lo + hi) / 2.0
        d = abs(bs_delta(option_type, spot, mid, t, vol))
        if option_type == "call":
            if d > abs_target:
                lo = mid
            else:
                hi = mid
        else:
            if d > abs_target:
                hi = mid
            else:
                lo = mid
    return _snap_to_deribit_strike((lo + hi) / 2.0, symbol)


# ── Deribit historical price fetch ─────────────────────────────────────

DERIBIT_CHART_URL = "https://www.deribit.com/api/v2/public/get_tradingview_chart_data"
DERIBIT_INSTRUMENTS_URL = "https://www.deribit.com/api/v2/public/get_instruments"


def fetch_deribit_listed_instruments(symbol: str) -> List[Dict]:
    """Fetch the live list of option instruments for a currency.

    Each entry has ``strike``, ``option_type``, ``expiration_timestamp`` (ms),
    and ``instrument_name``. Returns [] on failure.
    """
    params = {"currency": symbol.upper(), "kind": "option", "expired": "false"}
    url = f"{DERIBIT_INSTRUMENTS_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "gex-backtest/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError):
        return []
    result = data.get("result") or []
    return [r for r in result if isinstance(r, dict)]


def _pick_listed_expiry(instruments: List[Dict], target: datetime) -> Optional[datetime]:
    """Pick the listed expiry closest (by absolute day delta) to ``target``."""
    if not instruments:
        return None
    seen: Dict[int, datetime] = {}
    for inst in instruments:
        exp_ms = inst.get("expiration_timestamp")
        if exp_ms is None:
            continue
        try:
            exp_ms_int = int(exp_ms)
        except (TypeError, ValueError):
            continue
        if exp_ms_int in seen:
            continue
        seen[exp_ms_int] = datetime.fromtimestamp(exp_ms_int / 1000, tz=timezone.utc)
    if not seen:
        return None
    target_ts = target.timestamp()
    return min(seen.values(), key=lambda d: abs(d.timestamp() - target_ts))


def _snap_to_listed_strike(
    instruments: List[Dict],
    expiry: datetime,
    option_type: str,
    raw_strike: float,
) -> Optional[Tuple[float, str]]:
    """Snap ``raw_strike`` to the nearest listed strike on ``expiry`` for ``option_type``.

    Returns (strike, instrument_name) or None if no matching listings.
    """
    expiry_ms = int(expiry.timestamp() * 1000)
    wanted = option_type.lower()
    candidates: List[Tuple[float, str]] = []
    for inst in instruments:
        try:
            inst_exp = int(inst.get("expiration_timestamp"))
        except (TypeError, ValueError):
            continue
        if abs(inst_exp - expiry_ms) > 60 * 1000:
            continue
        if str(inst.get("option_type") or "").lower() != wanted:
            continue
        try:
            strike = float(inst.get("strike"))
        except (TypeError, ValueError):
            continue
        name = str(inst.get("instrument_name") or "")
        if strike > 0 and name:
            candidates.append((strike, name))
    if not candidates:
        return None
    return min(candidates, key=lambda row: abs(row[0] - raw_strike))


def fetch_deribit_daily_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    """Fetch daily OHLC candles from Deribit for BTC or ETH index.

    Returns DataFrame with columns: timestamp, open, high, low, close.
    Timestamps are UTC.
    """
    instrument = f"{symbol.upper()}-PERPETUAL"
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000
    params = {
        "instrument_name": instrument,
        "start_timestamp": str(start_ms),
        "end_timestamp": str(now_ms),
        "resolution": "1D",
    }
    query = urllib.parse.urlencode(params)
    url = f"{DERIBIT_CHART_URL}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": "gex-backtest/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError):
        return pd.DataFrame()

    result = data.get("result", {})
    ticks = result.get("ticks", [])
    closes = result.get("close", [])
    opens = result.get("open", [])
    highs = result.get("high", [])
    lows = result.get("low", [])
    if not ticks or not closes:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(ticks, unit="ms", utc=True),
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
    })
    return df.sort_values("timestamp").reset_index(drop=True)


def fetch_deribit_hourly_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    """Fetch hourly candles in chunks (Deribit limits ~5000 candles per call)."""
    instrument = f"{symbol.upper()}-PERPETUAL"
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000
    all_frames = []
    chunk_ms = 200 * 24 * 60 * 60 * 1000  # ~200 days = 4800 hourly candles
    cursor = start_ms
    while cursor < now_ms:
        end = min(cursor + chunk_ms, now_ms)
        params = {
            "instrument_name": instrument,
            "start_timestamp": str(cursor),
            "end_timestamp": str(end),
            "resolution": "60",
        }
        query = urllib.parse.urlencode(params)
        url = f"{DERIBIT_CHART_URL}?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "gex-backtest/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError):
            cursor = end
            continue
        result = data.get("result", {})
        ticks = result.get("ticks", [])
        closes = result.get("close", [])
        opens = result.get("open", [])
        if ticks and closes:
            chunk_df = pd.DataFrame({
                "timestamp": pd.to_datetime(ticks, unit="ms", utc=True),
                "open": opens,
                "close": closes,
            })
            all_frames.append(chunk_df)
        cursor = end
    if not all_frames:
        return pd.DataFrame()
    df = pd.concat(all_frames, ignore_index=True)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    return df


def get_friday_8am_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    """Return a series of Friday 08:00 UTC prices.

    First attempts hourly candles to get the exact 08:00 bar.
    Falls back to daily candles (using close of the Friday daily bar).
    """
    hourly = fetch_deribit_hourly_prices(symbol, days)
    if not hourly.empty:
        hourly["weekday"] = hourly["timestamp"].dt.weekday  # 4 = Friday
        hourly["hour"] = hourly["timestamp"].dt.hour
        fridays = hourly[(hourly["weekday"] == 4) & (hourly["hour"] == 8)].copy()
        if not fridays.empty:
            fridays = fridays.rename(columns={"close": "settlement_price"})
            fridays["date"] = fridays["timestamp"].dt.date
            return fridays[["date", "timestamp", "settlement_price"]].reset_index(drop=True)

    # Fallback: daily candles
    daily = fetch_deribit_daily_prices(symbol, days)
    if daily.empty:
        return pd.DataFrame()
    daily["weekday"] = daily["timestamp"].dt.weekday
    fridays = daily[daily["weekday"] == 4].copy()
    fridays = fridays.rename(columns={"close": "settlement_price"})
    fridays["date"] = fridays["timestamp"].dt.date
    return fridays[["date", "timestamp", "settlement_price"]].reset_index(drop=True)


# ── Historical volatility helper ───────────────────────────────────────

def estimate_historical_iv(daily_df: pd.DataFrame, window: int = 30) -> pd.Series:
    """Rolling annualised realised vol from daily closes (used as IV proxy)."""
    if daily_df.empty or "close" not in daily_df.columns:
        return pd.Series(dtype=float)
    log_ret = daily_df["close"].astype(float).apply(math.log).diff()
    rv = log_ret.rolling(window).std() * math.sqrt(365)
    return rv


# ── Data models ────────────────────────────────────────────────────────

@dataclass
class BacktestTrade:
    entry_date: str
    expiry_date: str
    symbol: str
    strategy: str
    spot_at_entry: float
    settlement_price: float
    iv_at_entry: float
    # Put leg
    put_strike: Optional[float] = None
    put_premium: Optional[float] = None
    put_delta: Optional[float] = None
    # Call leg
    call_strike: Optional[float] = None
    call_premium: Optional[float] = None
    call_delta: Optional[float] = None
    # Wing legs (iron condor)
    long_put_strike: Optional[float] = None
    long_put_premium: Optional[float] = None
    long_call_strike: Optional[float] = None
    long_call_premium: Optional[float] = None
    # Results
    premium_collected: float = 0.0
    settlement_pnl: float = 0.0
    net_pnl: float = 0.0
    won: bool = False
    # Position sizing (set during equity simulation)
    position_size: float = 0.0
    contracts: float = 0.0


@dataclass
class BacktestSummary:
    symbol: str
    strategy: str
    cycle: str
    delta: float
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    max_win: float = 0.0
    max_loss: float = 0.0
    max_drawdown: float = 0.0
    sharpe: float = 0.0
    avg_premium: float = 0.0
    trades: List[Dict] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    dates: List[str] = field(default_factory=list)


# ── Core backtest engine ───────────────────────────────────────────────

STRATEGY_TYPES = ["short_put", "cash_secured_put", "short_call", "short_strangle", "iron_condor", "covered_call", "covered_put"]
CYCLE_TYPES = ["weekly", "monthly"]
DEFAULT_DELTAS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
DEFAULT_IV_BUMP = {"BTC": 0.0, "ETH": 0.0}
IRON_CONDOR_WING_WIDTH_PCT = 0.05  # 5% of spot for wing width

# Deribit tradeable increments (min_trade_amount per Deribit API).
# BTC options trade in 0.1 BTC steps; ETH in 1 ETH steps.
DERIBIT_MIN_TRADE_AMOUNT = {"BTC": 0.1, "ETH": 1.0}
DEFAULT_MIN_TRADE_AMOUNT = 0.1


def _round_contracts_to_increment(raw_contracts: float, symbol: str) -> float:
    """Round contracts DOWN to the nearest exchange-allowed increment."""
    step = DERIBIT_MIN_TRADE_AMOUNT.get(symbol.upper(), DEFAULT_MIN_TRADE_AMOUNT)
    if step <= 0:
        return raw_contracts
    return math.floor(raw_contracts / step) * step


def _intrinsic_at_expiry(option_type: str, strike: float, settlement: float) -> float:
    if option_type == "call":
        return max(settlement - strike, 0.0)
    return max(strike - settlement, 0.0)


def _is_monthly_expiry(dt: datetime) -> bool:
    """Deribit monthly expiry = last Friday of the month.

    A Friday is the last Friday of its month iff adding 7 days crosses
    into the next month.
    """
    return (dt + timedelta(days=7)).month != dt.month


def _next_entry_expiry(cycle: str) -> Tuple[datetime, datetime]:
    """Return (entry, expiry) for the NEXT trade given today's date.

    Weekly: entry = upcoming Friday, expiry = Friday after.
    Monthly: entry = upcoming last-Friday-of-month, expiry = the one after.
    """
    today = datetime.now(timezone.utc).date()
    # Upcoming Friday (today counts only if today IS Friday and still morning UTC;
    # to stay conservative, we pick strictly the next Friday).
    days_ahead = (4 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    first_friday = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc) \
        + timedelta(days=days_ahead)

    if cycle == "monthly":
        # Walk forward week-by-week until we hit a last-Friday-of-month.
        candidate = first_friday
        while not _is_monthly_expiry(candidate):
            candidate = candidate + timedelta(days=7)
        entry = candidate
        expiry = entry + timedelta(days=7)
        while not _is_monthly_expiry(expiry):
            expiry = expiry + timedelta(days=7)
    else:
        entry = first_friday
        expiry = entry + timedelta(days=7)
    return entry, expiry


def suggest_next_trade(
    symbol: str,
    strategy: str = "short_put",
    cycle: str = "weekly",
    target_delta: float = 0.15,
    iv_override: Optional[float] = None,
    wing_width_pct: float = IRON_CONDOR_WING_WIDTH_PCT,
    capital: float = 100000.0,
    reinvest_equity: Optional[float] = None,
) -> Dict:
    """Project the NEXT trade for a given strategy using live spot + RV.

    Uses the most recent daily close as spot and the same RV+bump IV
    convention as the historical backtest so the suggestion is
    methodologically consistent with prior trades.

    Returns an empty dict if live data is unavailable.
    """
    daily = fetch_deribit_daily_prices(symbol, days=60)
    if daily.empty:
        return {"ok": False, "reason": "no live price data"}
    spot = float(daily["close"].iloc[-1])
    if spot <= 0:
        return {"ok": False, "reason": "invalid spot"}

    rv_series = estimate_historical_iv(daily, window=30)
    rv = float(rv_series.dropna().iloc[-1]) if not rv_series.dropna().empty else None
    default_iv = iv_override if iv_override else (0.60 if symbol == "BTC" else 0.70)
    if iv_override:
        iv = float(iv_override)
    elif rv:
        iv = max(rv * 1.15, default_iv * 0.8)
    else:
        iv = default_iv

    target_entry, target_expiry = _next_entry_expiry(cycle)
    instruments = fetch_deribit_listed_instruments(symbol)
    listed_expiry = _pick_listed_expiry(instruments, target_expiry) if instruments else None
    if listed_expiry is None:
        expiry = target_expiry
        entry = target_entry
        listed_available = False
    else:
        expiry = listed_expiry
        # Keep entry = today's logical entry (next Friday) unless listed expiry
        # is earlier than it; in that case collapse entry to now.
        entry = target_entry if target_entry < expiry else datetime.now(timezone.utc)
        listed_available = True
    days_to_exp = max((expiry.date() - entry.date()).days, 0)
    t = max(days_to_exp / 365.0, 1 / 365.0)

    def _resolve_strike(option_type: str, raw_strike: float) -> Tuple[float, Optional[str]]:
        """Return (strike, instrument_name) snapped to a listed strike when possible."""
        if listed_available:
            snapped = _snap_to_listed_strike(instruments, expiry, option_type, raw_strike)
            if snapped is not None:
                return snapped
        return _snap_to_deribit_strike(raw_strike, symbol), None

    out: Dict[str, object] = {
        "ok": True,
        "symbol": symbol,
        "strategy": strategy,
        "cycle": cycle,
        "entry_date": entry.date().isoformat(),
        "expiry_date": expiry.date().isoformat(),
        "days_to_expiry": days_to_exp,
        "spot": round(spot, 2),
        "iv": round(iv, 4),
        "delta": target_delta,
        "listed_expiry": listed_available,
    }

    premium_per_contract = 0.0
    max_loss_per_contract = 0.0  # realised if settlement moves all the way through

    if strategy in ("short_put", "cash_secured_put"):
        raw = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
        strike, inst_name = _resolve_strike("put", raw)
        premium_per_contract = bs_price("put", spot, strike, t, iv)
        out["put_strike"] = round(strike, 2)
        out["put_delta"] = round(bs_delta("put", spot, strike, t, iv), 4)
        if inst_name:
            out["put_instrument"] = inst_name
        max_loss_per_contract = max(strike - premium_per_contract, 0.0)
    elif strategy == "short_call":
        raw = strike_from_delta("call", spot, target_delta, t, iv, symbol)
        strike, inst_name = _resolve_strike("call", raw)
        premium_per_contract = bs_price("call", spot, strike, t, iv)
        out["call_strike"] = round(strike, 2)
        out["call_delta"] = round(bs_delta("call", spot, strike, t, iv), 4)
        if inst_name:
            out["call_instrument"] = inst_name
        max_loss_per_contract = float("inf")  # uncapped upside
    elif strategy == "short_strangle":
        raw_put = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
        raw_call = strike_from_delta("call", spot, target_delta, t, iv, symbol)
        put_k, put_inst = _resolve_strike("put", raw_put)
        call_k, call_inst = _resolve_strike("call", raw_call)
        put_prem = bs_price("put", spot, put_k, t, iv)
        call_prem = bs_price("call", spot, call_k, t, iv)
        premium_per_contract = put_prem + call_prem
        out["put_strike"] = round(put_k, 2)
        out["call_strike"] = round(call_k, 2)
        out["put_delta"] = round(bs_delta("put", spot, put_k, t, iv), 4)
        out["call_delta"] = round(bs_delta("call", spot, call_k, t, iv), 4)
        if put_inst:
            out["put_instrument"] = put_inst
        if call_inst:
            out["call_instrument"] = call_inst
        max_loss_per_contract = float("inf")
    elif strategy == "iron_condor":
        raw_put = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
        raw_call = strike_from_delta("call", spot, target_delta, t, iv, symbol)
        put_k, put_inst = _resolve_strike("put", raw_put)
        call_k, call_inst = _resolve_strike("call", raw_call)
        wing_w = spot * wing_width_pct
        long_put, lp_inst = _resolve_strike("put", put_k - wing_w)
        long_call, lc_inst = _resolve_strike("call", call_k + wing_w)
        sp = bs_price("put", spot, put_k, t, iv)
        sc = bs_price("call", spot, call_k, t, iv)
        lp = bs_price("put", spot, long_put, t, iv)
        lc = bs_price("call", spot, long_call, t, iv)
        premium_per_contract = sp + sc - lp - lc
        out["put_strike"] = round(put_k, 2)
        out["call_strike"] = round(call_k, 2)
        out["long_put_strike"] = round(long_put, 2)
        out["long_call_strike"] = round(long_call, 2)
        if put_inst:
            out["put_instrument"] = put_inst
        if call_inst:
            out["call_instrument"] = call_inst
        if lp_inst:
            out["long_put_instrument"] = lp_inst
        if lc_inst:
            out["long_call_instrument"] = lc_inst
        max_loss_per_contract = max(
            (put_k - long_put) - premium_per_contract,
            (long_call - call_k) - premium_per_contract,
        )
    else:
        return {"ok": False, "reason": f"unsupported strategy: {strategy}"}

    # Position sizing — mirror run_options_backtest.
    available = float(reinvest_equity) if reinvest_equity and reinvest_equity > 0 else float(capital)
    if strategy == "cash_secured_put":
        unit_cost = float(out["put_strike"])
    else:
        unit_cost = spot
    raw_contracts = available / unit_cost if unit_cost > 0 else 0.0
    contracts = _round_contracts_to_increment(raw_contracts, symbol)
    deployed = contracts * unit_cost
    premium_usd = premium_per_contract * contracts
    max_loss_usd = (
        max_loss_per_contract * contracts
        if math.isfinite(max_loss_per_contract)
        else float("inf")
    )

    out["premium_per_contract"] = round(premium_per_contract, 2)
    out["contracts"] = round(contracts, 4)
    out["position_size"] = round(deployed, 2)
    out["available_capital"] = round(available, 2)
    out["expected_premium"] = round(premium_usd, 2)
    out["max_loss"] = (
        round(max_loss_usd, 2) if math.isfinite(max_loss_usd) else None
    )
    out["tradeable"] = contracts > 0
    if contracts <= 0:
        out["reason"] = (
            f"capital ${available:,.0f} is below one Deribit increment "
            f"({DERIBIT_MIN_TRADE_AMOUNT.get(symbol.upper(), DEFAULT_MIN_TRADE_AMOUNT)} "
            f"{symbol}) at unit cost ${unit_cost:,.0f}"
        )
    return out


def run_options_backtest(
    symbol: str,
    strategy: str = "short_put",
    cycle: str = "weekly",
    target_delta: float = 0.15,
    iv_override: Optional[float] = None,
    wing_width_pct: float = IRON_CONDOR_WING_WIDTH_PCT,
    capital: float = 100000.0,
    days: int = 730,
    reinvest: bool = False,
) -> BacktestSummary:
    """Run the full backtest and return a summary with trade details."""

    # 1) Get Friday 08:00 settlement prices
    fridays = get_friday_8am_prices(symbol, days)
    if fridays.empty or len(fridays) < 3:
        return BacktestSummary(symbol=symbol, strategy=strategy, cycle=cycle, delta=target_delta)

    # 2) Get daily candles for historical vol estimation
    daily = fetch_deribit_daily_prices(symbol, days)
    rv_series = pd.Series(dtype=float)
    if not daily.empty:
        rv_series = estimate_historical_iv(daily, window=30)
        daily["rv"] = rv_series

    # Build a date -> rv lookup
    rv_lookup: Dict[str, float] = {}
    if not daily.empty and "rv" in daily.columns:
        for _, row in daily.iterrows():
            d = row["timestamp"].strftime("%Y-%m-%d")
            if pd.notna(row["rv"]) and row["rv"] > 0:
                rv_lookup[d] = float(row["rv"])

    default_iv = iv_override if iv_override else (0.60 if symbol == "BTC" else 0.70)

    # 3) Pair Fridays: entry -> expiry
    friday_dates = fridays["date"].tolist()
    friday_prices = dict(zip(
        [str(d) for d in fridays["date"]],
        fridays["settlement_price"].tolist(),
    ))

    # For monthly: only use last-Friday-of-month expiries
    if cycle == "monthly":
        monthly_fridays = []
        for d in friday_dates:
            dt = datetime(d.year, d.month, d.day)
            if _is_monthly_expiry(dt):
                monthly_fridays.append(d)
        expiry_dates = monthly_fridays
    else:
        expiry_dates = friday_dates

    if len(expiry_dates) < 2:
        return BacktestSummary(symbol=symbol, strategy=strategy, cycle=cycle, delta=target_delta)

    trades: List[BacktestTrade] = []

    for i in range(len(expiry_dates) - 1):
        entry_date = expiry_dates[i]
        expiry_date = expiry_dates[i + 1]
        entry_key = str(entry_date)
        expiry_key = str(expiry_date)

        spot = friday_prices.get(entry_key)
        settlement = friday_prices.get(expiry_key)
        if spot is None or settlement is None:
            continue

        # Time to expiry in years
        entry_ts = pd.Timestamp(entry_date)
        expiry_ts = pd.Timestamp(expiry_date)
        days_to_exp = (expiry_ts - entry_ts).days
        if days_to_exp <= 0:
            continue
        t = days_to_exp / 365.0

        # IV: use historical RV near entry date, with a premium bump
        iv = rv_lookup.get(entry_key, default_iv)
        # Options typically trade at a premium to RV
        iv = max(iv * 1.15, default_iv * 0.8)
        if iv_override:
            iv = iv_override

        trade = BacktestTrade(
            entry_date=entry_key,
            expiry_date=expiry_key,
            symbol=symbol,
            strategy=strategy,
            spot_at_entry=spot,
            settlement_price=settlement,
            iv_at_entry=iv,
        )

        if strategy in ("short_put", "cash_secured_put"):
            strike = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
            premium = bs_price("put", spot, strike, t, iv)
            intrinsic = _intrinsic_at_expiry("put", strike, settlement)
            trade.put_strike = strike
            trade.put_premium = premium
            trade.put_delta = bs_delta("put", spot, strike, t, iv)
            trade.premium_collected = premium
            trade.settlement_pnl = -intrinsic
            trade.net_pnl = premium - intrinsic

        elif strategy == "short_call":
            strike = strike_from_delta("call", spot, target_delta, t, iv, symbol)
            premium = bs_price("call", spot, strike, t, iv)
            intrinsic = _intrinsic_at_expiry("call", strike, settlement)
            trade.call_strike = strike
            trade.call_premium = premium
            trade.call_delta = bs_delta("call", spot, strike, t, iv)
            trade.premium_collected = premium
            trade.settlement_pnl = -intrinsic
            trade.net_pnl = premium - intrinsic

        elif strategy == "short_strangle":
            put_strike = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
            call_strike = strike_from_delta("call", spot, target_delta, t, iv, symbol)
            put_prem = bs_price("put", spot, put_strike, t, iv)
            call_prem = bs_price("call", spot, call_strike, t, iv)
            put_intr = _intrinsic_at_expiry("put", put_strike, settlement)
            call_intr = _intrinsic_at_expiry("call", call_strike, settlement)
            trade.put_strike = put_strike
            trade.put_premium = put_prem
            trade.put_delta = bs_delta("put", spot, put_strike, t, iv)
            trade.call_strike = call_strike
            trade.call_premium = call_prem
            trade.call_delta = bs_delta("call", spot, call_strike, t, iv)
            trade.premium_collected = put_prem + call_prem
            trade.settlement_pnl = -(put_intr + call_intr)
            trade.net_pnl = (put_prem + call_prem) - (put_intr + call_intr)

        elif strategy == "iron_condor":
            put_strike = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
            call_strike = strike_from_delta("call", spot, target_delta, t, iv, symbol)
            wing_w = spot * wing_width_pct
            long_put_strike = _snap_to_deribit_strike(put_strike - wing_w, symbol)
            long_call_strike = _snap_to_deribit_strike(call_strike + wing_w, symbol)
            # Premiums
            sp_prem = bs_price("put", spot, put_strike, t, iv)
            sc_prem = bs_price("call", spot, call_strike, t, iv)
            lp_prem = bs_price("put", spot, long_put_strike, t, iv)
            lc_prem = bs_price("call", spot, long_call_strike, t, iv)
            net_credit = (sp_prem + sc_prem) - (lp_prem + lc_prem)
            # Settlement
            sp_intr = _intrinsic_at_expiry("put", put_strike, settlement)
            sc_intr = _intrinsic_at_expiry("call", call_strike, settlement)
            lp_intr = _intrinsic_at_expiry("put", long_put_strike, settlement)
            lc_intr = _intrinsic_at_expiry("call", long_call_strike, settlement)
            payout = -(sp_intr + sc_intr) + (lp_intr + lc_intr)

            trade.put_strike = put_strike
            trade.put_premium = sp_prem
            trade.put_delta = bs_delta("put", spot, put_strike, t, iv)
            trade.call_strike = call_strike
            trade.call_premium = sc_prem
            trade.call_delta = bs_delta("call", spot, call_strike, t, iv)
            trade.long_put_strike = long_put_strike
            trade.long_put_premium = lp_prem
            trade.long_call_strike = long_call_strike
            trade.long_call_premium = lc_prem
            trade.premium_collected = net_credit
            trade.settlement_pnl = payout
            trade.net_pnl = net_credit + payout

        elif strategy == "covered_call":
            # Long 1 unit of underlying + sell OTM call at target delta
            strike = strike_from_delta("call", spot, target_delta, t, iv, symbol)
            premium = bs_price("call", spot, strike, t, iv)
            call_intr = _intrinsic_at_expiry("call", strike, settlement)
            # Underlying PnL: settlement - spot
            underlying_pnl = settlement - spot
            # Short call PnL: premium collected minus intrinsic owed
            option_pnl = premium - call_intr
            trade.call_strike = strike
            trade.call_premium = premium
            trade.call_delta = bs_delta("call", spot, strike, t, iv)
            trade.premium_collected = premium
            trade.settlement_pnl = underlying_pnl - call_intr
            trade.net_pnl = underlying_pnl + option_pnl

        elif strategy == "covered_put":
            # Short 1 unit of underlying + sell OTM put at target delta
            strike = strike_from_delta("put", spot, -target_delta, t, iv, symbol)
            premium = bs_price("put", spot, strike, t, iv)
            put_intr = _intrinsic_at_expiry("put", strike, settlement)
            # Short underlying PnL: spot - settlement (profit when price drops)
            underlying_pnl = spot - settlement
            # Short put PnL: premium collected minus intrinsic owed
            option_pnl = premium - put_intr
            trade.put_strike = strike
            trade.put_premium = premium
            trade.put_delta = bs_delta("put", spot, strike, t, iv)
            trade.premium_collected = premium
            trade.settlement_pnl = underlying_pnl - put_intr
            trade.net_pnl = underlying_pnl + option_pnl

        trade.won = trade.net_pnl >= 0
        trades.append(trade)

    # 4) Compute summary stats
    if not trades:
        return BacktestSummary(symbol=symbol, strategy=strategy, cycle=cycle, delta=target_delta)

    # Position sizing: contracts = available_capital / spot_at_entry
    # This makes PnL proportional to the capital you allocate.
    # When reinvest=True, available capital = current equity (compounds).
    # When reinvest=False, available capital = starting capital (fixed size).
    equity = [capital]
    pnls = []
    for t in trades:
        available = equity[-1] if reinvest else capital
        if t.spot_at_entry <= 0:
            raise ValueError(
                f"invalid spot_at_entry={t.spot_at_entry} for {symbol} trade on {t.entry_date}"
            )
        # Cash-secured puts reserve collateral at the strike (worst-case
        # assignment cost), not at spot. Everything else scales at spot.
        if strategy == "cash_secured_put" and t.put_strike and t.put_strike > 0:
            unit_cost = float(t.put_strike)
        else:
            unit_cost = t.spot_at_entry
        raw_contracts = available / unit_cost
        contracts = _round_contracts_to_increment(raw_contracts, symbol)
        # Capital actually deployed at the exchange's tradeable increment.
        deployed = contracts * unit_cost
        scaled_pnl = t.net_pnl * contracts
        # Store the scaled values back so trade_dicts reflect actual $ PnL
        t.net_pnl = scaled_pnl
        t.premium_collected = t.premium_collected * contracts
        t.won = scaled_pnl >= 0
        t.position_size = deployed
        t.contracts = contracts
        pnls.append(scaled_pnl)
        equity.append(equity[-1] + scaled_pnl)

    premiums = [t.premium_collected for t in trades]
    wins = sum(1 for p in pnls if p >= 0)
    losses = len(pnls) - wins

    # Max drawdown
    peak = equity[0]
    max_dd = 0.0
    for val in equity:
        if val > peak:
            peak = val
        dd = (peak - val) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Sharpe (annualised, assuming weekly trades)
    pnl_series = pd.Series(pnls)
    avg = float(pnl_series.mean())
    std = float(pnl_series.std()) if len(pnls) > 1 else 0.0
    periods_per_year = 52 if cycle == "weekly" else 12
    sharpe = (avg / std * math.sqrt(periods_per_year)) if std > 0 else 0.0

    trade_dicts = []
    for t in trades:
        td = {
            "entry_date": t.entry_date,
            "expiry_date": t.expiry_date,
            "spot": round(t.spot_at_entry, 2),
            "settlement": round(t.settlement_price, 2),
            "iv": round(t.iv_at_entry, 4),
            "premium": round(t.premium_collected, 2),
            "pnl": round(t.net_pnl, 2),
            "won": t.won,
            "position_size": round(t.position_size, 2),
            "contracts": round(t.contracts, 4),
        }
        if t.put_strike:
            td["put_strike"] = round(t.put_strike, 2)
            td["put_delta"] = round(t.put_delta, 4) if t.put_delta else None
        if t.call_strike:
            td["call_strike"] = round(t.call_strike, 2)
            td["call_delta"] = round(t.call_delta, 4) if t.call_delta else None
        if t.long_put_strike:
            td["long_put_strike"] = round(t.long_put_strike, 2)
        if t.long_call_strike:
            td["long_call_strike"] = round(t.long_call_strike, 2)
        trade_dicts.append(td)

    dates = [t.entry_date for t in trades]

    return BacktestSummary(
        symbol=symbol,
        strategy=strategy,
        cycle=cycle,
        delta=target_delta,
        total_trades=len(trades),
        wins=wins,
        losses=losses,
        win_rate=wins / len(trades) if trades else 0.0,
        total_pnl=round(sum(pnls), 2),
        avg_pnl=round(avg, 2),
        max_win=round(max(pnls), 2),
        max_loss=round(min(pnls), 2),
        max_drawdown=round(max_dd, 4),
        sharpe=round(sharpe, 3),
        avg_premium=round(sum(premiums) / len(premiums), 2) if premiums else 0.0,
        trades=trade_dicts,
        equity_curve=[round(e, 2) for e in equity],
        dates=dates,
    )
