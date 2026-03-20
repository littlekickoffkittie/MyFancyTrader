from __future__ import annotations

import threading
import time
from typing import Any, Dict, Tuple


class SimpleCache:
    def __init__(self, ttl: float = 30.0):
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._ttl = float(ttl)
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            ts, val = entry
            if time.time() - ts > self._ttl:
                del self._data[key]
                return None
            return val

    def set(self, key: str, val: Any):
        with self._lock:
            self._data[key] = (time.time(), val)

    # patch[4]: proactive eviction so stale keys don't accumulate forever
    def _sweep(self) -> None:
        """Background thread: evict entries whose TTL has expired."""
        import threading as _t
        while True:
            time.sleep(self._ttl)
            cutoff = time.time() - self._ttl
            with self._lock:
                expired = [k for k, (ts, _) in self._data.items() if ts < cutoff]
                for k in expired:
                    del self._data[k]

    def start_sweeper(self) -> None:
        """Start the background eviction thread. Call once at module load."""
        import threading as _t
        _t.Thread(target=self._sweep, daemon=True, name="cache-sweeper").start()


# global shared cache instance with default 30s TTL
CACHE = SimpleCache(ttl=30.0)
CACHE.start_sweeper()
