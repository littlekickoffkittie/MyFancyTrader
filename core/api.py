"""
API module for interacting with exchange and external services.
"""
from __future__ import annotations

import json
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

from . import config, cache, network

# Global variables for Cryptopanic news cache
_news_cache: Dict[str, Any] = {}
_news_cache_lock = threading.Lock()
_news_rate_lock = threading.Lock()
_news_last_request = [0.0]


def _resolve_resolution(timeframe: str) -> int:
    """Resolve timeframe string to resolution in seconds."""
    return config.TIMEFRAME_MAP.get(timeframe, 900)


def get_tickers(rps: float = None) -> List[Dict[str, Any]]:
    """Fetch all USDT-M perpetual 24hr tickers."""
    url = f"{config.BASE_URL}/md/v3/ticker/24hr/all"
    resp = network.safe_request("GET", url, rps=rps)
    if not resp:
        return []
    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return []
    if data.get("error") is not None:
        return []
    result = data.get("result", []) or []
    filtered = []
    for t in result:
        if not isinstance(t, dict):
            continue
        symbol = t.get("symbol", "")
        if not symbol.endswith("USDT") or symbol.startswith("s"):
            continue
        filtered.append(t)
    filtered.sort(key=lambda x: float(x.get("turnoverRv") or 0.0), reverse=True)
    return filtered


def get_candles(symbol: str, timeframe: str = "15m", limit: int = 100,
                rps: float = None) -> List[List[Any]]:
    """Fetch candlestick data for a symbol."""
    resolution = _resolve_resolution(timeframe)
    cache_key = f"candles:{symbol}:{resolution}:{limit}"
    cached = cache.CACHE.get(cache_key)
    if cached is not None:
        return cached

    api_symbol = symbol.replace(".", "")
    url = f"{config.BASE_URL}/exchange/public/md/v2/kline/last"
    params = {"symbol": api_symbol, "resolution": resolution, "limit": limit}
    try:
        resp = network.safe_request("GET", url, params=params, rps=rps)
        if not resp:
            return []
        data = resp.json()
        if data.get("code") == 0:
            rows = data.get("data", {}).get("rows", [])
            if rows:
                rows_sorted = sorted(rows, key=lambda r: r[0])
                cache.CACHE.set(cache_key, rows_sorted)
                return rows_sorted
        return []
    except (json.JSONDecodeError, ValueError, KeyError):
        return []


def get_funding_rate_info(symbol: str, rps: float = None) -> \
        Tuple[Optional[float], Optional[float], float]:
    """Fetch current and previous funding rates for a symbol."""
    cache_key = f"funding:{symbol}"
    cached = cache.CACHE.get(cache_key)
    if cached is not None:
        return cached

    url = f"{config.BASE_URL}/contract-biz/public/real-funding-rates"
    resp = network.safe_request("GET", url, params={"symbol": symbol}, rps=rps)
    res: Tuple[Optional[float], Optional[float], float] = (None, None, 0.0)

    if resp:
        try:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                return _get_funding_rate_history(symbol, rps)

            entry = None
            if isinstance(items, list):
                for it in items:
                    if it.get("symbol") == symbol:
                        entry = it
                        break
                if entry is None and items:
                    entry = items[0]
            else:
                entry = items

            if entry:
                current_fr = float(entry.get("fundingRate", 0.0))
                res = (current_fr, current_fr, 0.0)
                cache.CACHE.set(cache_key, res)
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    return res


def prefetch_all_funding_rates(rps: float = None):
    """Prefetch all funding rates and cache them."""
    url = f"{config.BASE_URL}/contract-biz/public/real-funding-rates"
    resp = network.safe_request("GET", url, rps=rps)
    if not resp:
        return
    try:
        data = resp.json()
        res_data = data.get("data", {})
        if isinstance(res_data, list):
            items = res_data
        else:
            items = res_data.get("rows", [])
        if not items:
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol")
            if not sym:
                continue
            fr_raw = item.get("fundingRate") or item.get("fundingRateRr")
            if fr_raw is not None:
                fr = float(fr_raw)
                cache.CACHE.set(f"funding:{sym}", (fr, fr, 0.0))
    except (json.JSONDecodeError, ValueError, TypeError):
        pass


def _get_funding_rate_history(symbol: str, rps: float = None) -> \
        Tuple[Optional[float], Optional[float], float]:
    """Fetch funding rate history as a fallback."""
    base = symbol.replace("USDT", "")
    fr_symbol = f".{base}USDTFR8H"
    url = f"{config.BASE_URL}/api-data/public/data/funding-rate-history"
    params = {"symbol": fr_symbol, "limit": 2, "latestOnly": False}
    resp = network.safe_request("GET", url, params=params, rps=rps)
    if not resp:
        return None, None, 0.0
    try:
        data = resp.json()
        if data.get("code") != 0:
            return None, None, 0.0
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            return None, None, 0.0
        current_fr = float(rows[-1].get("fundingRate", 0.0))
        prev_fr = float(rows[-2].get("fundingRate", current_fr)) if len(rows) > 1 else current_fr
        return current_fr, prev_fr, current_fr - prev_fr
    except (json.JSONDecodeError, ValueError, IndexError):
        return None, None, 0.0


# ── Stubs for missing functions to satisfy imports ──

def get_account_positions(*_args, **_kwargs):
    """Stub for getting account positions."""
    return []

def set_leverage(*_args, **_kwargs):
    """Stub for setting leverage."""
    return None

def create_order(*_args, **_kwargs):
    """Stub for creating an order."""
    return None

def get_active_orders(*_args, **_kwargs):
    """Stub for getting active orders."""
    return []

def cancel_all_orders(*_args, **_kwargs):
    """Stub for cancelling all orders."""
    return None


def get_order_book(symbol: str, rps: float = None):
    """Fetch order book for a symbol and calculate spread and depth."""
    url = f"{config.BASE_URL}/md/v2/orderbook"
    resp = network.safe_request("GET", url, params={"symbol": symbol}, rps=rps)
    if not resp:
        return None, None, None, 0.0
    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return None, None, None, 0.0
    if data.get("error") is not None:
        return None, None, None, 0.0
    result = data.get("result", {}) or {}
    book = result.get("orderbook_p", {}) or {}
    bids, asks = book.get("bids", []), book.get("asks", [])
    if not bids or not asks:
        return None, None, None, 0.0
    try:
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
    except (IndexError, ValueError, TypeError):
        return None, None, None, 0.0
    spread_pct = (best_ask - best_bid) / best_bid * 100.0 if best_bid != 0.0 else None

    def depth_sum(entries):
        total = 0.0
        for row in entries:
            try:
                total += float(row[0]) * float(row[1])
            except (IndexError, ValueError, TypeError):
                continue
        return total

    return best_bid, best_ask, spread_pct, (depth_sum(bids) + depth_sum(asks))


def get_cryptopanic_news(coin_symbol: str) -> Tuple[int, List[str]]:
    """Fetch news from Cryptopanic for a given coin symbol."""
    if not config.CRYPTOPANIC_API_KEY:
        return 0, []

    with _news_cache_lock:
        if coin_symbol in _news_cache:
            return _news_cache[coin_symbol]

    with _news_rate_lock:
        elapsed = time.time() - _news_last_request[0]
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        _news_last_request[0] = time.time()

    try:
        url = "https://cryptopanic.com/api/developer/v2/posts/"
        params = {
            "auth_token": config.CRYPTOPANIC_API_KEY,
            "currencies": coin_symbol,
            "filter": "news",
        }
        resp = network.safe_request("GET", url, params=params)
        if not resp:
            return 0, []
        data = resp.json()
        results = data.get("results", []) or []
        count = data.get("count", len(results))
        titles = [res.get("title", "") for res in results[:5]]
        result = (min(count, 99), titles)
    except (json.JSONDecodeError, ValueError, KeyError):
        result = (0, [])

    with _news_cache_lock:
        _news_cache[coin_symbol] = result
    return result


def make_entity_request(entity_name: str, method: str = "POST",
                        data: dict = None, entity_id: str = None):
    """Make a request to the entity API."""
    if not config.ENTITY_API_KEY:
        return None
    base_url = config.ENTITY_API_BASE_URL
    app_id = config.ENTITY_APP_ID
    url = f"{base_url}/api/apps/{app_id}/entities/{entity_name}"
    if entity_id:
        url = f"{url}/{entity_id}"
    headers = {"api_key": config.ENTITY_API_KEY, "Content-Type": "application/json"}
    try:
        m = method.upper()
        if m == "GET":
            r = network.safe_request("GET", url, params=data, headers=headers)
        elif m == "PUT":
            r = network.safe_request("PUT", url, json_data=data, headers=headers)
        else:
            r = network.safe_request("POST", url, json_data=data, headers=headers)
        return r.json() if r else None
    except (json.JSONDecodeError, ValueError):
        return None


def _process_deepseek_stream(resp) -> str:
    """Helper to process streaming response from DeepSeek."""
    full_text = ""
    for line in resp.iter_lines():
        if not line:
            continue
        line_str = line.decode("utf-8")
        if not line_str.startswith("data: "):
            continue
        data_raw = line_str[len("data: "):]
        if data_raw == "[DONE]":
            break
        try:
            d = json.loads(data_raw)
            content = d["choices"][0]["delta"].get("content", "")
            if content:
                print(content, end="", flush=True)
                full_text += content
        except (json.JSONDecodeError, KeyError, IndexError):
            continue
    print()
    return full_text


def call_deepseek(prompt: str,
                  system_prompt: str = "You are an expert trader. Use plain text.",
                  stream: bool = True):
    """Call DeepSeek API for analysis."""
    if not config.DEEPSEEK_API_KEY:
        return None
    url = "https://api.deepseek.com/chat/completions"
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {config.DEEPSEEK_API_KEY.strip()}"}
    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "system", "content": system_prompt},
                     {"role": "user", "content": prompt}],
        "temperature": 0.7,
        "stream": stream,
    }
    try:
        resp = network.safe_request("POST", url, json_data=payload, headers=headers, stream=stream)
        if not resp:
            return None
        if stream:
            return _process_deepseek_stream(resp)
        return resp.json()["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, ValueError, KeyError):
        return None
