"""Lightweight in-process TTL cache for dashboard callbacks.

A single tiny class shared across modules so we stop sprinkling ad-hoc
dicts (CHAIN_CACHE, _CANDLE_CACHE, etc.) everywhere. Thread-safe enough
for the Dash worker model where each callback runs on its own thread.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Optional, Tuple


class TTLCache:
    def __init__(self, ttl_seconds: float = 60.0, max_entries: int = 256) -> None:
        self._ttl = float(ttl_seconds)
        self._max = int(max_entries)
        self._store: dict[Any, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            ts, value = entry
            if (time.monotonic() - ts) > self._ttl:
                # Expired
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: Any, value: Any) -> None:
        with self._lock:
            if len(self._store) >= self._max:
                # Evict oldest entry — cheap O(n) since max is small.
                oldest_key = min(self._store, key=lambda k: self._store[k][0])
                self._store.pop(oldest_key, None)
            self._store[key] = (time.monotonic(), value)

    def get_or_set(self, key: Any, producer: Callable[[], Any]) -> Any:
        cached = self.get(key)
        if cached is not None:
            return cached
        value = producer()
        if value is not None:
            self.set(key, value)
        return value

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
