"""Backward‑compatible wrapper around the new modular common package.

The original monolithic implementation lived here; to facilitate a gradual
refactor the heavy lifting has been split into submodules under
``fangblenny_bot/common``.  This module simply imports and re‑exports the
symbols that other parts of the codebase (and external scripts) expect.

Consumers can continue using ``import phemex_common as pc`` and nothing will
break, but the code is now easier to maintain and test.
"""

from __future__ import annotations

# keep commonly-used stdlib / third-party modules available on the
# ``pc`` namespace for backwards compatibility with existing code that
# accessed attributes like ``pc.os`` or ``pc.np``.
import os
import sys
import threading
import time
import json
import math

import numpy as np
import requests
from colorama import Fore

import logging

# Re-exporting stdlib modules for backwards compatibility
# Some legacy code may use pc.os, pc.np, etc.
np = np
# os is already imported above

# expose configuration constants
try:
    from ..core.config import (
        BASE_URL,
        TIMEFRAME_MAP,
        DEFAULTS,
        CRYPTOPANIC_API_KEY,
        DEEPSEEK_API_KEY,
        ENTITY_API_KEY,
        ENTITY_API_BASE_URL,
        ENTITY_APP_ID,
    )
except (ImportError, ValueError):
    try:
        from core.config import (
            BASE_URL,
            TIMEFRAME_MAP,
            DEFAULTS,
            CRYPTOPANIC_API_KEY,
            DEEPSEEK_API_KEY,
            ENTITY_API_KEY,
            ENTITY_API_BASE_URL,
            ENTITY_APP_ID,
        )
    except ImportError:
        from fangblenny_bot.core.config import (
            BASE_URL,
            TIMEFRAME_MAP,
            DEFAULTS,
            CRYPTOPANIC_API_KEY,
            DEEPSEEK_API_KEY,
            ENTITY_API_KEY,
            ENTITY_API_BASE_URL,
            ENTITY_APP_ID,
        )

# networking helpers
try:
    from ..core.network import (
        build_session,
        get_thread_session,
        throttle,
        safe_request,
    )
except (ImportError, ValueError):
    try:
        from core.network import (
            build_session,
            get_thread_session,
            throttle,
            safe_request,
        )
    except ImportError:
        from fangblenny_bot.core.network import (
            build_session,
            get_thread_session,
            throttle,
            safe_request,
        )

# cache
try:
    from ..core.cache import (
        SimpleCache,
        CACHE,
    )
except (ImportError, ValueError):
    try:
        from core.cache import (
            SimpleCache,
            CACHE,
        )
    except ImportError:
        from fangblenny_bot.core.cache import (
            SimpleCache,
            CACHE,
        )

# indicators
try:
    from ..core.indicators import (
        calc_ema_series,
        calc_rsi,
        calc_bb,
        calc_atr,
        calc_ema_slope,
        calc_volume_profile,
        calc_volume_spike,
    )
    # Re-exporting directly to the module namespace
    calc_ema_series = calc_ema_series
    calc_rsi = calc_rsi
    calc_bb = calc_bb
    calc_atr = calc_atr
    calc_ema_slope = calc_ema_slope
    calc_volume_profile = calc_volume_profile
    calc_volume_spike = calc_volume_spike
except (ImportError, ValueError):
    try:
        from core.indicators import (
            calc_ema_series,
            calc_rsi,
            calc_bb,
            calc_atr,
            calc_ema_slope,
            calc_volume_profile,
            calc_volume_spike,
        )
        calc_ema_series = calc_ema_series
        calc_rsi = calc_rsi
        calc_bb = calc_bb
        calc_atr = calc_atr
        calc_ema_slope = calc_ema_slope
        calc_volume_profile = calc_volume_profile
        calc_volume_spike = calc_volume_spike
    except ImportError:
        from fangblenny_bot.core.indicators import (
            calc_ema_series,
            calc_rsi,
            calc_bb,
            calc_atr,
            calc_ema_slope,
            calc_volume_profile,
            calc_volume_spike,
        )
        calc_ema_series = calc_ema_series
        calc_rsi = calc_rsi
        calc_bb = calc_bb
        calc_atr = calc_atr
        calc_ema_slope = calc_ema_slope
        calc_volume_profile = calc_volume_profile
        calc_volume_spike = calc_volume_spike

# aliases for indicators
calculate_ema = calc_ema_series
calculate_rsi = calc_rsi
calculate_bollinger_bands = calc_bb
calculate_atr = calc_atr

# math utils
try:
    from ..core.math_utils import (
        pct_change,
        clamp,
        fmt_vol,
        grade,
    )
except (ImportError, ValueError):
    try:
        from core.math_utils import (
            pct_change,
            clamp,
            fmt_vol,
            grade,
        )
    except ImportError:
        from fangblenny_bot.core.math_utils import (
            pct_change,
            clamp,
            fmt_vol,
            grade,
        )

# api
try:
    from ..core.api import (
        get_tickers,
        get_candles,
        get_funding_rate_info,
        get_account_positions,
        set_leverage,
        create_order,
        get_active_orders,
        cancel_all_orders,
        prefetch_all_funding_rates,
        get_cryptopanic_news,
        get_order_book,
        make_entity_request,
    )
except (ImportError, ValueError):
    try:
        from core.api import (
            get_tickers,
            get_candles,
            get_funding_rate_info,
            get_account_positions,
            set_leverage,
            create_order,
            get_active_orders,
            cancel_all_orders,
            prefetch_all_funding_rates,
            get_cryptopanic_news,
            get_order_book,
            make_entity_request,
        )
    except ImportError:
        from fangblenny_bot.core.api import (
            get_tickers,
            get_candles,
            get_funding_rate_info,
            get_account_positions,
            set_leverage,
            create_order,
            get_active_orders,
            cancel_all_orders,
            prefetch_all_funding_rates,
            get_cryptopanic_news,
            get_order_book,
            make_entity_request,
        )

# aliases for api
get_funding_rate = get_funding_rate_info

# logger
logger = logging.getLogger("phemex_common")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
