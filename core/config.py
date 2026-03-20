from __future__ import annotations

import os

# core endpoints and maps
BASE_URL = os.getenv("PHEMEX_BASE_URL", "https://api.phemex.com")

TIMEFRAME_MAP = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1H": 3600,
    "2H": 7200,
    "4H": 14400,
    "6H": 21600,
    "12H": 43200,
    "1D": 86400,
    "1W": 604800,
}

DEFAULTS = {
    "MIN_VOLUME": int(os.getenv("MIN_VOLUME", 1_000_000)),
    "TIMEFRAME": os.getenv("TIMEFRAME", "15m"),
    "TOP_N": int(os.getenv("TOP_N", 20)),
    "MIN_SCORE": int(os.getenv("MIN_SCORE", 130)),
    "MAX_WORKERS": int(os.getenv("MAX_WORKERS", 100)),
    "RATE_LIMIT_RPS": float(os.getenv("RATE_LIMIT_RPS", 100.0)),
}

# third-party API keys
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
ENTITY_API_KEY = os.getenv("ENTITY_API_KEY")
ENTITY_API_BASE_URL = os.getenv("ENTITY_API_BASE_URL", "https://acoustic-trade-scan-now.base44.app")
ENTITY_APP_ID = os.getenv("ENTITY_APP_ID", "69a3845341f04ab2db0682fb")
