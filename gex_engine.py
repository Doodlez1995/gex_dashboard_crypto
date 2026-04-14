import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from config import OPTIONS_FILE, SUPPORTED_SYMBOLS, SUPPORTED_EXCHANGES, GEX_NOISE_THRESHOLD, ORDERBOOK_WORKERS

DERIBIT_BASE_URL = "https://www.deribit.com/api/v2/public"
BYBIT_BASE_URL = "https://api.bybit.com/v5/market"
BINANCE_EAPI_URL = "https://eapi.binance.com/eapi/v1"
BINANCE_SPOT_URL = "https://api.binance.com/api/v3"
OKX_BASE_URL = "https://www.okx.com/api/v5"
CONTRACT_SIZE = 1  # BTC / ETH options

OUTPUT_COLUMNS = ["exchange", "symbol", "expiry", "strike", "call_gex", "put_gex", "call_oi", "put_oi", "spot_price"]


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _empty_frame():
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def parse_bybit_option_symbol(option_symbol):
    parts = str(option_symbol).split("-")
    if len(parts) < 4:
        raise ValueError(f"Unexpected Bybit option symbol: {option_symbol}")
    expiry = datetime.strptime(parts[1].upper(), "%d%b%y").date()
    strike = float(parts[2])
    right = parts[3].upper()
    if right.startswith("C"):
        option_type = "call"
    elif right.startswith("P"):
        option_type = "put"
    else:
        raise ValueError(f"Unknown Bybit option side in symbol: {option_symbol}")
    return expiry, strike, option_type


def parse_binance_option_symbol(option_symbol):
    parts = str(option_symbol).split("-")
    if len(parts) < 4:
        raise ValueError(f"Unexpected Binance option symbol: {option_symbol}")
    expiry = datetime.strptime(parts[1], "%y%m%d").date()
    strike = float(parts[2])
    right = parts[3].upper()
    if right.startswith("C"):
        option_type = "call"
    elif right.startswith("P"):
        option_type = "put"
    else:
        raise ValueError(f"Unknown Binance option side in symbol: {option_symbol}")
    return expiry, strike, option_type


def parse_okx_option_symbol(option_symbol):
    parts = str(option_symbol).split("-")
    if len(parts) < 5:
        raise ValueError(f"Unexpected OKX option symbol: {option_symbol}")
    expiry = datetime.strptime(parts[2], "%y%m%d").date()
    strike = float(parts[3])
    right = parts[4].upper()
    if right.startswith("C"):
        option_type = "call"
    elif right.startswith("P"):
        option_type = "put"
    else:
        raise ValueError(f"Unknown OKX option side in symbol: {option_symbol}")
    return expiry, strike, option_type


def _binance_expiry_code(option_symbol):
    parts = str(option_symbol).split("-")
    if len(parts) < 2:
        return None
    expiry_code = parts[1]
    if len(expiry_code) == 6 and expiry_code.isdigit():
        return expiry_code
    return None


def get_deribit_spot_price(symbol):
    r = requests.get(
        f"{DERIBIT_BASE_URL}/get_index_price",
        params={"index_name": f"{symbol.lower()}_usd"},
        timeout=15,
    )
    r.raise_for_status()
    return float(r.json()["result"]["index_price"])


def get_deribit_instruments(symbol):
    r = requests.get(
        f"{DERIBIT_BASE_URL}/get_instruments",
        params={"currency": symbol, "kind": "option"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["result"]


def get_deribit_orderbook(instrument_name):
    r = requests.get(
        f"{DERIBIT_BASE_URL}/get_order_book",
        params={"instrument_name": instrument_name},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["result"]


def process_deribit_option(inst, symbol, spot):
    try:
        book = get_deribit_orderbook(inst["instrument_name"])
        greeks = book.get("greeks") or {}
        gamma = _safe_float(greeks.get("gamma"))
        oi = _safe_float(book.get("open_interest"))
        if gamma is None or oi is None or gamma == 0 or oi == 0:
            return None

        gex = gamma * oi * CONTRACT_SIZE * spot**2
        expiry_ts = float(inst["expiration_timestamp"]) / 1000
        option_type = str(inst["option_type"]).lower()
        return {
            "exchange": "Deribit",
            "symbol": symbol,
            "expiry": datetime.utcfromtimestamp(expiry_ts).date(),
            "strike": float(inst["strike"]),
            "call_gex": gex if option_type == "call" else 0.0,
            "put_gex": -gex if option_type == "put" else 0.0,
            "call_oi": float(oi) if option_type == "call" else 0.0,
            "put_oi": float(oi) if option_type == "put" else 0.0,
            "spot_price": float(spot),
        }
    except (KeyError, TypeError, ValueError, requests.RequestException):
        return None


def calculate_gex_deribit(symbol):
    spot = get_deribit_spot_price(symbol)
    instruments = get_deribit_instruments(symbol)
    rows = []
    with ThreadPoolExecutor(max_workers=ORDERBOOK_WORKERS) as executor:
        futures = [executor.submit(process_deribit_option, inst, symbol, spot) for inst in instruments]
        for future in as_completed(futures):
            res = future.result()
            if res:
                rows.append(res)
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def get_bybit_spot_price(symbol):
    r = requests.get(
        f"{BYBIT_BASE_URL}/tickers",
        params={"category": "spot", "symbol": f"{symbol}USDT"},
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()
    items = (payload.get("result") or {}).get("list") or []
    if not items:
        raise RuntimeError(f"Bybit spot ticker returned no rows for {symbol}")
    price = _safe_float(items[0].get("lastPrice"))
    if price is None:
        raise RuntimeError(f"Bybit spot ticker missing lastPrice for {symbol}")
    return float(price)


def get_bybit_option_tickers(symbol):
    rows = []
    cursor = None
    while True:
        params = {"category": "option", "baseCoin": symbol, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(f"{BYBIT_BASE_URL}/tickers", params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
        if payload.get("retCode") not in (0, "0"):
            raise RuntimeError(f"Bybit tickers error: {payload.get('retMsg', 'unknown')}")
        result = payload.get("result") or {}
        items = result.get("list") or []
        rows.extend(items)
        cursor = result.get("nextPageCursor")
        if not cursor or not items:
            break
    return rows


def process_bybit_option(ticker_row, symbol, spot):
    try:
        option_symbol = ticker_row["symbol"]
        expiry, strike, option_type = parse_bybit_option_symbol(option_symbol)
        gamma = _safe_float(ticker_row.get("gamma"))
        oi = _safe_float(ticker_row.get("openInterest"))
        if gamma is None or oi is None or gamma == 0 or oi == 0:
            return None
        gex = gamma * oi * CONTRACT_SIZE * spot**2
        return {
            "exchange": "Bybit",
            "symbol": symbol,
            "expiry": expiry,
            "strike": float(strike),
            "call_gex": gex if option_type == "call" else 0.0,
            "put_gex": -gex if option_type == "put" else 0.0,
            "call_oi": float(oi) if option_type == "call" else 0.0,
            "put_oi": float(oi) if option_type == "put" else 0.0,
            "spot_price": float(spot),
        }
    except (KeyError, TypeError, ValueError):
        return None


def calculate_gex_bybit(symbol):
    spot = get_bybit_spot_price(symbol)
    tickers = get_bybit_option_tickers(symbol)
    rows = []
    for ticker_row in tickers:
        parsed = process_bybit_option(ticker_row, symbol, spot)
        if parsed:
            rows.append(parsed)
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def get_binance_spot_price(symbol):
    underlying = f"{symbol}USDT"
    try:
        r = requests.get(
            f"{BINANCE_EAPI_URL}/index",
            params={"underlying": underlying},
            timeout=15,
        )
        r.raise_for_status()
        index_price = _safe_float((r.json() or {}).get("indexPrice"))
        if index_price is not None:
            return float(index_price)
    except requests.RequestException:
        pass

    r = requests.get(
        f"{BINANCE_SPOT_URL}/ticker/price",
        params={"symbol": underlying},
        timeout=15,
    )
    r.raise_for_status()
    price = _safe_float((r.json() or {}).get("price"))
    if price is None:
        raise RuntimeError(f"Binance spot ticker missing price for {symbol}")
    return float(price)


def get_binance_mark_rows(symbol):
    r = requests.get(
        f"{BINANCE_EAPI_URL}/mark",
        params={"underlying": f"{symbol}USDT"},
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, list) else []


def _okx_public_data(endpoint, params, timeout=20):
    r = requests.get(
        f"{OKX_BASE_URL}/{endpoint}",
        params=params,
        timeout=timeout,
    )
    r.raise_for_status()
    payload = r.json() or {}
    if str(payload.get("code")) != "0":
        raise RuntimeError(f"OKX {endpoint} error: {payload.get('msg', 'unknown')}")
    data = payload.get("data")
    return data if isinstance(data, list) else []


def get_okx_spot_price(symbol):
    rows = _okx_public_data("market/ticker", {"instId": f"{symbol}-USDT"}, timeout=15)
    if not rows:
        raise RuntimeError(f"OKX spot ticker returned no rows for {symbol}")
    price = _safe_float(rows[0].get("last"))
    if price is None:
        raise RuntimeError(f"OKX spot ticker missing last for {symbol}")
    return float(price)


def get_okx_option_instruments(symbol):
    return _okx_public_data(
        "public/instruments",
        {"instType": "OPTION", "instFamily": f"{symbol}-USD"},
        timeout=20,
    )


def get_okx_option_summary(symbol):
    return _okx_public_data("public/opt-summary", {"instFamily": f"{symbol}-USD"}, timeout=20)


def get_okx_open_interest_rows(symbol):
    return _okx_public_data(
        "public/open-interest",
        {"instType": "OPTION", "instFamily": f"{symbol}-USD"},
        timeout=20,
    )


def build_okx_instrument_map(instruments):
    instrument_by_id = {}
    for inst in instruments:
        inst_id = inst.get("instId")
        if not inst_id:
            continue
        try:
            expiry, strike, option_type = parse_okx_option_symbol(inst_id)
        except ValueError:
            continue
        contract_size = _safe_float(inst.get("ctMult"))
        if contract_size is None or contract_size <= 0:
            contract_size = CONTRACT_SIZE
        instrument_by_id[str(inst_id)] = {
            "expiry": expiry,
            "strike": float(strike),
            "option_type": option_type,
            "contract_size": float(contract_size),
        }
    return instrument_by_id


def build_okx_open_interest_map(oi_rows):
    oi_by_symbol = {}
    for item in oi_rows:
        option_symbol = item.get("instId")
        oi = _safe_float(item.get("oi"))
        if option_symbol and oi is not None:
            oi_by_symbol[str(option_symbol)] = float(oi)
    return oi_by_symbol


def get_binance_open_interest_for_expiry(symbol, expiry_code):
    r = requests.get(
        f"{BINANCE_EAPI_URL}/openInterest",
        params={"underlyingAsset": symbol, "expiration": expiry_code},
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    if not isinstance(payload, list):
        return {}
    out = {}
    for item in payload:
        option_symbol = item.get("symbol")
        oi = _safe_float(item.get("sumOpenInterest"))
        if option_symbol and oi is not None:
            out[str(option_symbol)] = float(oi)
    return out


def build_binance_open_interest_map(symbol, mark_rows):
    expiries = {
        expiry_code
        for row in mark_rows
        for expiry_code in [_binance_expiry_code(row.get("symbol"))]
        if expiry_code
    }
    if not expiries:
        return {}

    oi_by_symbol = {}
    workers = min(max(len(expiries), 1), 8)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_by_expiry = {executor.submit(get_binance_open_interest_for_expiry, symbol, exp): exp for exp in expiries}
        for future in as_completed(future_by_expiry):
            try:
                oi_by_symbol.update(future.result())
            except requests.RequestException:
                continue
    return oi_by_symbol


def process_binance_option(mark_row, symbol, spot, oi_by_symbol):
    try:
        option_symbol = mark_row["symbol"]
        expiry, strike, option_type = parse_binance_option_symbol(option_symbol)
        gamma = _safe_float(mark_row.get("gamma"))
        oi = _safe_float(oi_by_symbol.get(option_symbol))
        if gamma is None or oi is None or gamma == 0 or oi == 0:
            return None
        gex = gamma * oi * CONTRACT_SIZE * spot**2
        return {
            "exchange": "Binance",
            "symbol": symbol,
            "expiry": expiry,
            "strike": float(strike),
            "call_gex": gex if option_type == "call" else 0.0,
            "put_gex": -gex if option_type == "put" else 0.0,
            "call_oi": float(oi) if option_type == "call" else 0.0,
            "put_oi": float(oi) if option_type == "put" else 0.0,
            "spot_price": float(spot),
        }
    except (KeyError, TypeError, ValueError):
        return None


def process_okx_option(summary_row, symbol, spot, oi_by_symbol, instrument_by_id):
    try:
        option_symbol = summary_row["instId"]
        inst_meta = instrument_by_id.get(option_symbol)
        if not inst_meta:
            return None
        gamma = _safe_float(summary_row.get("gammaBS"))
        if gamma is None:
            signed_gamma = _safe_float(summary_row.get("gamma"))
            gamma = abs(signed_gamma) if signed_gamma is not None else None
        oi = _safe_float(oi_by_symbol.get(option_symbol))
        if gamma is None or oi is None or gamma == 0 or oi == 0:
            return None
        contract_size = _safe_float(inst_meta.get("contract_size")) or CONTRACT_SIZE
        gex = gamma * oi * contract_size * spot**2
        option_type = inst_meta["option_type"]
        return {
            "exchange": "OKX",
            "symbol": symbol,
            "expiry": inst_meta["expiry"],
            "strike": float(inst_meta["strike"]),
            "call_gex": gex if option_type == "call" else 0.0,
            "put_gex": -gex if option_type == "put" else 0.0,
            "call_oi": float(oi) if option_type == "call" else 0.0,
            "put_oi": float(oi) if option_type == "put" else 0.0,
            "spot_price": float(spot),
        }
    except (KeyError, TypeError, ValueError):
        return None


def calculate_gex_binance(symbol):
    spot = get_binance_spot_price(symbol)
    mark_rows = get_binance_mark_rows(symbol)
    if not mark_rows:
        return _empty_frame()
    oi_by_symbol = build_binance_open_interest_map(symbol, mark_rows)
    rows = []
    for mark_row in mark_rows:
        parsed = process_binance_option(mark_row, symbol, spot, oi_by_symbol)
        if parsed:
            rows.append(parsed)
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def calculate_gex_okx(symbol):
    spot = get_okx_spot_price(symbol)
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_instruments = executor.submit(get_okx_option_instruments, symbol)
        future_summary = executor.submit(get_okx_option_summary, symbol)
        future_oi = executor.submit(get_okx_open_interest_rows, symbol)
        instruments = future_instruments.result()
        summary_rows = future_summary.result()
        oi_rows = future_oi.result()
    if not instruments or not summary_rows or not oi_rows:
        return _empty_frame()

    instrument_by_id = build_okx_instrument_map(instruments)
    oi_by_symbol = build_okx_open_interest_map(oi_rows)
    rows = []
    for summary_row in summary_rows:
        parsed = process_okx_option(summary_row, symbol, spot, oi_by_symbol, instrument_by_id)
        if parsed:
            rows.append(parsed)
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


EXCHANGE_COLLECTORS = {
    "Deribit": calculate_gex_deribit,
    "Bybit": calculate_gex_bybit,
    "Binance": calculate_gex_binance,
    "OKX": calculate_gex_okx,
}


def canonical_exchange_name(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return None
    lower = text.lower()
    if lower == "deribit":
        return "Deribit"
    if lower == "bybit":
        return "Bybit"
    if lower == "binance":
        return "Binance"
    if lower in {"okx", "okx.com"}:
        return "OKX"
    return None


def normalize_exchanges(exchanges):
    raw = exchanges or SUPPORTED_EXCHANGES
    if isinstance(raw, str):
        raw = [raw]
    out = []
    seen = set()
    for exchange in raw:
        match = canonical_exchange_name(exchange)
        if match and match not in seen:
            seen.add(match)
            out.append(match)
    if not out:
        raise ValueError("No supported exchanges selected")
    return out


def _collect_one_exchange(symbol, exchange):
    collector = EXCHANGE_COLLECTORS[exchange]
    try:
        df_exchange = collector(symbol)
        row_count = len(df_exchange)
        print(f"  {exchange}: {row_count} rows")
        if row_count > 0:
            return df_exchange
    except Exception as exc:
        print(f"  {exchange}: failed ({exc})")
    return None


def collect_symbol(symbol, exchanges):
    frames = []
    with ThreadPoolExecutor(max_workers=len(exchanges)) as executor:
        futures = {
            executor.submit(_collect_one_exchange, symbol, ex): ex
            for ex in exchanges
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                frames.append(result)
    if not frames:
        return _empty_frame()
    return pd.concat(frames, ignore_index=True)


def collect_all_symbols(symbols=SUPPORTED_SYMBOLS, exchanges=SUPPORTED_EXCHANGES):
    selected_exchanges = normalize_exchanges(exchanges)
    frames = []
    for sym in symbols:
        print(f"Processing {sym} options...")
        symbol_frame = collect_symbol(sym, selected_exchanges)
        if not symbol_frame.empty:
            frames.append(symbol_frame)

    if not frames:
        raise RuntimeError("No option data collected for any symbol/exchange")

    df_all = pd.concat(frames, ignore_index=True)
    df_all["total_gex"] = df_all["call_gex"] + df_all["put_gex"]
    df_all = df_all[df_all["total_gex"].abs() > GEX_NOISE_THRESHOLD]
    if df_all.empty:
        raise RuntimeError("Collected option data is empty after GEX noise filter")

    return (
        df_all.groupby(["exchange", "symbol", "expiry", "strike"], as_index=False)
        .agg(
            call_gex=("call_gex", "sum"),
            put_gex=("put_gex", "sum"),
            call_oi=("call_oi", "sum"),
            put_oi=("put_oi", "sum"),
            spot_price=("spot_price", "last"),
        )
    )


if __name__ == "__main__":
    df_out = collect_all_symbols()
    df_out.to_csv(OPTIONS_FILE, index=False)
    print("options_data.csv saved")
    print(df_out.head())
