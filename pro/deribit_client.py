import json
from typing import Dict, Optional, Tuple
import urllib.parse
import urllib.request
import urllib.error

import pandas as pd


DERIBIT_PUBLIC_BASE_URL = "https://www.deribit.com/api/v2/public"


class DeribitClient:
    def __init__(self):
        self._instrument_cache: Dict[str, Dict[Tuple[str, float, str], str]] = {}

    def _get(self, endpoint: str, params: Dict[str, str], timeout: int = 4) -> Dict:
        query = urllib.parse.urlencode(params)
        url = f"{DERIBIT_PUBLIC_BASE_URL}/{endpoint}?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "gex-dashboard/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError):
            return {}

    def get_instrument_lookup(self, symbol: str) -> Dict[Tuple[str, float, str], str]:
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        payload = self._get("get_instruments", {"currency": symbol, "kind": "option", "expired": "false"}, timeout=6)
        lookup: Dict[Tuple[str, float, str], str] = {}
        for item in payload.get("result", []):
            exp_ts = item.get("expiration_timestamp")
            strike = item.get("strike")
            option_type = item.get("option_type")
            name = item.get("instrument_name")
            if exp_ts is None or strike is None or option_type is None or not name:
                continue
            exp = pd.to_datetime(exp_ts, unit="ms", utc=True).strftime("%Y-%m-%d")
            lookup[(exp, float(strike), str(option_type))] = name
        self._instrument_cache[symbol] = lookup
        return lookup

    def get_option_mid_usd(self, instrument_name: str, spot_price: float) -> Optional[float]:
        payload = self._get("get_order_book", {"instrument_name": instrument_name}, timeout=5)
        result = payload.get("result") or {}
        mark = result.get("mark_price")
        bid = result.get("best_bid_price")
        ask = result.get("best_ask_price")
        mid = None
        if mark is not None and mark > 0:
            mid = float(mark)
        elif bid is not None and ask is not None and bid > 0 and ask > 0:
            mid = (float(bid) + float(ask)) / 2.0
        if mid is None:
            return None
        return mid * float(spot_price)
