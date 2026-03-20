from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# thread-local session for re-use in concurrent environments
_thread_local = threading.local()

# global rate limiter state
_rate_lock = threading.Lock()
_last_request_time_global = 0.0


def build_session(timeout: int = 15, max_retries: int = 3) -> requests.Session:
    """Create a requests.Session configured with retries and sane headers."""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    retry = Retry(
        total=max_retries,
        backoff_factor=0.6,
        status_forcelist=[500, 502, 503, 504],  # patch[2]: 429 handled manually
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def get_thread_session() -> requests.Session:
    """Return a session instance that is unique to the current thread."""
    if getattr(_thread_local, "session", None) is None:
        _thread_local.session = build_session()
    return _thread_local.session


def throttle(rps: float) -> None:
    """Sleep as needed to respect a global requests-per-second limit."""
    if not rps or rps <= 0:
        return
    interval = 1.0 / rps
    global _last_request_time_global

    with _rate_lock:
        now = time.time()
        wait_until = _last_request_time_global + interval
        if now < wait_until:
            sleep_time = wait_until - now
            _last_request_time_global = wait_until
        else:
            sleep_time = 0
            _last_request_time_global = now

    if sleep_time > 0.001:
        time.sleep(sleep_time)


def safe_request(
    method: str,
    url: str,
    params: dict = None,
    json_data: dict = None,
    headers: dict = None,
    rps: float = None,
    timeout: int = 12,
    stream: bool = False,
) -> Optional[requests.Response]:
    """Wrapper around requests to handle retries, rate limiting and errors."""
    try:
        if rps:
            throttle(rps)

        sess = get_thread_session()
        resp = sess.request(
            method,
            url,
            params=params,
            json=json_data,
            headers=headers,
            timeout=timeout,
            stream=stream,
        )

        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2))
            time.sleep(wait)
            resp = sess.request(
                method,
                url,
                params=params,
                json=json_data,
                headers=headers,
                timeout=timeout,
                stream=stream,
            )

        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        # swallow network-related errors; caller can handle None
        return None
    except Exception:
        return None
