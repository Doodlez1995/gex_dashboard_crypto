from datetime import date

from gex_engine import (
    parse_bybit_option_symbol,
    parse_binance_option_symbol,
    parse_okx_option_symbol,
    normalize_exchanges,
)


def test_parse_bybit_option_symbol():
    expiry, strike, option_type = parse_bybit_option_symbol("BTC-28FEB26-69500-C-USDT")
    assert expiry == date(2026, 2, 28)
    assert strike == 69500.0
    assert option_type == "call"


def test_parse_binance_option_symbol():
    expiry, strike, option_type = parse_binance_option_symbol("BTC-260327-100000-P")
    assert expiry == date(2026, 3, 27)
    assert strike == 100000.0
    assert option_type == "put"


def test_parse_okx_option_symbol():
    expiry, strike, option_type = parse_okx_option_symbol("BTC-USD-260327-100000-C")
    assert expiry == date(2026, 3, 27)
    assert strike == 100000.0
    assert option_type == "call"


def test_normalize_exchanges_case_insensitive():
    exchanges = normalize_exchanges(["deribit", "BYBIT", "binance", "okx", "Bybit", "OKX.com"])
    assert exchanges == ["Deribit", "Bybit", "Binance", "OKX"]
