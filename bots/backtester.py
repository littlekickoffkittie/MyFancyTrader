#!/usr/bin/env python3
"""
FangBleeny Backtester
=====================
Walk-forward backtester that replays scanner signals on real historical candle data.

How it works:
  1. Fetches N candles per symbol from Phemex (same endpoints the scanner uses)
  2. Slides a 100-candle window forward one candle at a time
  3. At each step, scores the window using the EXACT same logic as phemex_long/short.py
  4. When score >= threshold: enters at NEXT candle OPEN (no lookahead bias)
  5. Slippage = spread/2 on entry + spread/2 on exit (market order crosses half the spread)
  6. Trailing stop tracked on candle HIGH/LOW (not close) — stops hit realistically
  7. Hard stop loss fires before trailing stop if price blows through (optional)
  8. Records every trade with full signal breakdown

Modes:
  python backtest.py              -- single run with defaults
  python backtest.py --sweep      -- grid search over trail%, score, leverage

Usage examples:
  python backtest.py --timeframe 15m --candles 500 --min-score 100
  python backtest.py --symbols BTCUSDT ETHUSDT SOLUSDT --trail-pct 0.008
  python backtest.py --stop-loss-pct 0.03 --cooldown 5 --direction LONG
  python backtest.py --sweep --sweep-symbols 30
  python backtest.py --csv  -- also saves trades to backtest_results.csv
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests
from colorama import init, Fore, Style
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
init(autoreset=True)

BASE_URL = os.getenv("PHEMEX_BASE_URL", "https://api.phemex.com").replace(
    "testnet-api.phemex.com", "api.phemex.com"  # always use mainnet for market data
)

TIMEFRAME_MAP = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1H": 3600, "2H": 7200, "4H": 14400, "6H": 21600, "12H": 43200,
    "1D": 86400,
}

# Candles per year per timeframe — used for Sharpe/Sortino annualisation
CANDLES_PER_YEAR = {
    "1m": 525_600, "3m": 175_200, "5m": 105_120, "15m": 35_040,
    "30m": 17_520,  "1H": 8_760,   "2H": 4_380,   "4H": 2_190,
    "6H": 1_460,    "12H": 730,    "1D": 365,
}

BANNER = r"""
 ███████╗ █████╗ ███╗   ██╗ ██████╗     ██████╗  ██████╗ ████████╗
 ██╔════╝██╔══██╗████╗  ██║██╔════╝     ██╔══██╗██╔═══██╗╚══██╔══╝
 █████╗  ███████║██╔██╗ ██║██║  ███╗    ██████╔╝██║   ██║   ██║
 ██╔══╝  ██╔══██║██║╚██╗██║██║   ██║    ██╔══██╗██║   ██║   ██║
 ██║     ██║  ██║██║ ╚████║╚██████╔╝    ██████╔╝╚██████╔╝   ██║
 ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═════╝  ╚═════╝   ╚═╝
              FangBleeny Backtester  v2.0
"""

# ─────────────────────────────────────────────────────────────────────
# Scoring weights — exact match to phemex_long.py / phemex_short.py
# ─────────────────────────────────────────────────────────────────────
LONG_WEIGHTS = {
    "divergence":        20,
    "rsi_recovery":      25,
    "rsi_oversold":      22,
    "bb_lower_90":       30,
    "bb_lower_75":       22,
    "ema_stretch_3":     15,
    "vol_spike_2":       15,
    "funding_negative":  22,
    "htf_align_oversold":15,
    "funding_momentum":  10,
}

SHORT_WEIGHTS = {
    "divergence":           20,
    "rsi_rollover":         25,
    "rsi_overbought":       22,
    "bb_upper_90":          30,
    "bb_upper_75":          22,
    "ema_stretch_3":        15,
    "vol_spike_2":          15,
    "funding_high":         22,
    "htf_align_overbought": 15,
    "funding_momentum":     10,
}

TAKER_FEE = 0.0006  # 0.06% per side (Phemex USDT-M maker/taker)

# ─────────────────────────────────────────────────────────────────────
# HTTP session
# ─────────────────────────────────────────────────────────────────────
_thread_local = threading.local()

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        sess = requests.Session()
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        sess.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50))
        _thread_local.session = sess
    return _thread_local.session

def _get(url: str, params: dict = None, timeout: int = 15) -> Optional[dict]:
    try:
        r = _get_session().get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────
# Market data fetchers
# ─────────────────────────────────────────────────────────────────────
def get_tickers(min_vol: float = 1_000_000) -> List[dict]:
    data = _get(f"{BASE_URL}/md/v3/ticker/24hr/all")
    if not data or data.get("error"):
        return []
    result = data.get("result") or data.get("data") or {}
    tickers = result.get("tickers", []) if isinstance(result, dict) else result
    if not tickers:
        tickers = data if isinstance(data, list) else []
    return [
        t for t in tickers
        if str(t.get("symbol", "")).endswith("USDT")
        and float(t.get("turnoverRv") or 0) >= min_vol
    ]

def get_candles(symbol: str, timeframe: str = "15m", limit: int = 500) -> List[list]:
    """Fetch OHLCV. Row format: [ts, interval, last_close, open, high, low, close, volume, turnover]"""
    resolution = TIMEFRAME_MAP.get(timeframe, 900)
    api_symbol = symbol.replace(".", "")
    data = _get(
        "https://api.phemex.com/exchange/public/md/v2/kline/last",
        params={"symbol": api_symbol, "resolution": resolution, "limit": limit},
    )
    if not data or data.get("code") != 0:
        return []
    rows = data.get("data", {}).get("rows", [])
    return sorted(rows, key=lambda r: r[0])

def get_spread_pct(symbol: str) -> Optional[float]:
    """Best bid-ask spread as % of mid price."""
    data = _get(f"{BASE_URL}/md/v2/orderbook", params={"symbol": symbol})
    if not data or data.get("error"):
        return None
    try:
        book = (data.get("result") or {}).get("orderbook_p") or {}
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        if bid <= 0:
            return None
        return (ask - bid) / bid * 100.0
    except Exception:
        return None

def get_funding(symbol: str) -> Optional[float]:
    data = _get(
        f"{BASE_URL}/contract-biz/public/real-funding-rates",
        params={"symbol": symbol},
    )
    if not data:
        return None
    try:
        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            return None
        entry = next((i for i in items if i.get("symbol") == symbol), items[0])
        return float(entry.get("fundingRate", 0.0))
    except Exception:
        return None

def get_htf_rsi(symbol: str, tf: str = "1H") -> Optional[float]:
    rows = get_candles(symbol, timeframe=tf, limit=50)
    if not rows:
        return None
    closes = []
    for r in rows:
        try:
            closes.append(float(r[6]))
        except Exception:
            continue
    if len(closes) < 16:
        return None
    rsi, _, _ = calc_rsi(closes)
    return rsi

# ─────────────────────────────────────────────────────────────────────
# Indicators (ported 1:1 from scanner files)
# ─────────────────────────────────────────────────────────────────────
def calc_rsi(closes: List[float], period: int = 14
             ) -> Tuple[Optional[float], Optional[float], List[Optional[float]]]:
    n = len(closes)
    if n <= period:
        return None, None, [None] * n
    arr = np.asarray(closes, dtype=float)
    diffs = np.diff(arr)
    gains = np.where(diffs > 0, diffs, 0.0)
    losses = np.where(diffs < 0, -diffs, 0.0)
    ag = float(gains[:period].sum() / period)
    al = float(losses[:period].sum() / period)
    history: List[Optional[float]] = [None] * period

    def _rsi(g, l):
        return 100.0 if l == 0 else 100.0 - 100.0 / (1.0 + g / l)

    history.append(_rsi(ag, al))
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + float(gains[i])) / period
        al = (al * (period - 1) + float(losses[i])) / period
        history.append(_rsi(ag, al))

    return history[-1], history[-2] if len(history) >= 2 else None, history


def calc_bb(closes: List[float], period: int = 21, mult: float = 2.0) -> Optional[Dict]:
    if len(closes) < period:
        return None
    w = np.asarray(closes[-period:], dtype=float)
    mid = float(w.mean())
    std = float(np.sqrt(((w - mid) ** 2).sum() / period))
    return {"upper": mid + mult * std, "mid": mid, "lower": mid - mult * std}


def calc_ema(closes: List[float], period: int = 21) -> Optional[float]:
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = float(np.mean(closes[:period]))
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
    return ema


def calc_ema_series(closes: List[float], period: int = 21) -> List[Optional[float]]:
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1)
    ema = float(np.mean(closes[:period]))
    result: List[Optional[float]] = [None] * (period - 1)
    result.append(ema)
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


def vol_spike_ratio(vols: List[float], lookback: int = 20) -> float:
    if len(vols) < lookback + 1:
        return 1.0
    avg = float(np.mean(vols[-lookback - 1:-1]))
    return vols[-1] / avg if avg > 0 else 1.0


def check_divergence(
    closes: List[float],
    rsi_hist: List[Optional[float]],
    window: int = 20,
    bullish: bool = True,
) -> bool:
    if len(closes) < window:
        return False
    prices = closes[-window:]
    rsies = [r for r in rsi_hist[-window:] if r is not None]
    if len(rsies) < window // 2:
        return False
    if bullish:
        p_ll = prices[-1] < min(prices[:-5]) if len(prices) > 5 else False
        r_hl = rsies[-1] > min(rsies[:-3]) if len(rsies) > 3 else False
        return p_ll and r_hl and rsies[-1] < 45
    else:
        p_hh = prices[-1] > max(prices[:-5]) if len(prices) > 5 else False
        r_lh = rsies[-1] < max(rsies[:-3]) if len(rsies) > 3 else False
        return p_hh and r_lh and rsies[-1] > 55

# ─────────────────────────────────────────────────────────────────────
# Scoring — exact match to scanner weights/thresholds
# ─────────────────────────────────────────────────────────────────────
def _pct(new: float, base: float) -> float:
    return (new - base) / base * 100.0 if base else 0.0


def score_long_window(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    vols: List[float],
    rsi_1h: Optional[float] = None,
    funding: Optional[float] = None,
    fr_prev: Optional[float] = None,
    spread: Optional[float] = None,
) -> Tuple[int, List[str]]:
    W = LONG_WEIGHTS
    score = 0
    signals: List[str] = []

    rsi, prev_rsi, rsi_hist = calc_rsi(closes)
    bb = calc_bb(closes)
    ema_series = calc_ema_series(closes)
    ema21 = ema_series[-1] if ema_series else None
    vs = vol_spike_ratio(vols)
    has_div = check_divergence(closes, rsi_hist, bullish=True)

    price   = closes[-1]
    open24  = closes[0]
    low24   = min(lows)
    change  = _pct(price, open24)
    dist_low = _pct(price, low24) if low24 > 0 else None

    # EMA slope
    ema200 = calc_ema(closes, period=200)
    
    if len(ema_series) >= 4:
        valid_emas = [e for e in ema_series[-4:] if e is not None]
        if len(valid_emas) >= 2:
            slope = valid_emas[-1] - valid_emas[-2]
            if slope > 0:
                score += 12
                signals.append(f"Positive EMA Slope ({slope:.4f}) — Uptrend confirmed")
            elif slope < -0.01:
                score += 5
                signals.append(f"EMA Slope Flattening — Downtrend slowing")

    # Trend Filter (Hard)
    if ema200 is not None:
        if price < ema200:
            # In a downtrend, we penalize longs or can even block them if we add a hard filter later
            score -= 15
            signals.append(f"Price below EMA200 ({price:.2f} < {ema200:.2f}) — Downtrend")
        else:
            score += 10
            signals.append(f"Price above EMA200 ({price:.2f} > {ema200:.2f}) — Uptrend")

    # Spread / liquidity
    if spread is not None:
        if spread > 0.15:
            signals.append(f"Low Liquidity (Spread {spread:.2f}%)")
        elif spread < 0.05:
            score += 5
            signals.append(f"High Liquidity (Spread {spread:.2f}%)")

    # Funding momentum
    if funding is not None and fr_prev is not None:
        fr_change = funding - fr_prev
        if fr_change < 0:
            score += W["funding_momentum"]
            signals.append(f"Funding Momentum (becoming more negative {fr_change*100:.4f}% — squeeze building)")

    # HTF alignment
    if rsi_1h is not None:
        if rsi_1h < 30:
            score += W["htf_align_oversold"]
            signals.append(f"HTF Alignment (1H RSI {rsi_1h:.1f}) — deeply oversold")
        elif rsi_1h < 45:
            score += 8
            signals.append(f"HTF Alignment (1H RSI {rsi_1h:.1f}) — oversold territory")

    # Divergence
    if has_div:
        score += W["divergence"]
        signals.append("Bullish Divergence (Price LL vs RSI HL) — sellers exhausted")

    # RSI
    if rsi is not None:
        rolling_up = prev_rsi is not None and rsi > prev_rsi
        if rsi < 25:
            score += W["rsi_oversold"]
            signals.append(f"RSI {rsi:.1f} (extremely oversold)")
        elif rsi < 35:
            score += W["rsi_recovery"] if rolling_up else 18
            signals.append(f"RSI {rsi:.1f} (oversold{' ✓ recovering' if rolling_up else ''})")
        elif 35 <= rsi <= 50 and rolling_up and prev_rsi is not None and prev_rsi < 35:
            score += W["rsi_recovery"]
            signals.append(f"RSI Recovery {prev_rsi:.1f}→{rsi:.1f}")
        elif rsi > 70:
            signals.append(f"RSI {rsi:.1f} (overbought — risky long)")

    # BB
    if bb is not None:
        bb_range = bb["upper"] - bb["lower"]
        if bb_range > 0:
            bb_pct = (price - bb["lower"]) / bb_range
            if bb_pct <= 0.10:
                score += W["bb_lower_90"]
                signals.append(f"Price at/below BB lower ({bb_pct*100:.0f}%) — extreme oversold")
            elif bb_pct <= 0.25:
                score += W["bb_lower_75"]
                signals.append(f"Near BB lower ({bb_pct*100:.0f}%) — oversold")
            elif bb_pct <= 0.50:
                score += 5
                signals.append(f"Below BB mid ({bb_pct*100:.0f}%)")
            elif bb_pct > 0.85:
                signals.append(f"Above BB mid — fading long ({bb_pct*100:.0f}%)")

    # EMA stretch
    if ema21 is not None:
        pct_ema = _pct(price, ema21)
        if pct_ema < -3.0:
            score += W["ema_stretch_3"]
            signals.append(f"Price {abs(pct_ema):.1f}% below EMA21 (mean-reversion opportunity)")
            if rsi is not None and rsi < 35:
                score += 5
                signals.append("Stretch bonus: Deeply oversold RSI + Below EMA21")
        elif pct_ema < -1.0:
            score += 5
            signals.append(f"Price {abs(pct_ema):.1f}% below EMA21")

    # Volume spike
    if vs >= 2.0:
        score += W["vol_spike_2"]
        signals.append(f"Volume spike {vs:.1f}x (capitulation/demand)")
    elif vs >= 1.4:
        score += 7
        signals.append(f"Elevated volume {vs:.1f}x")

    # 24h change
    if change < -15:
        score += 20
        signals.append(f"{change:.1f}% crash (capitulation)")
    elif change < -7:
        score += 12
        signals.append(f"{change:.1f}% dip (oversold bounce)")
    elif 5 < change < 15:
        score += 5
        signals.append(f"+{change:.1f}% (bullish momentum)")
    elif change >= 15:
        signals.append(f"+{change:.1f}% (overextended — risky long)")

    # Near 24h low
    if dist_low is not None and dist_low < 1.5:
        score += 10
        signals.append(f"Near 24h low ({dist_low:.1f}% above)")

    # Funding
    if funding is not None:
        fr_pct = funding * 100
        if fr_pct < -0.01:
            score += W["funding_negative"]
            signals.append(f"Negative Funding ({fr_pct:.4f}%) — crowded shorts, squeeze fuel")
        elif fr_pct > 0.10:
            score -= 10
            signals.append(f"Positive Funding ({fr_pct:.4f}%) — crowded longs, caution")

    return max(int(round(score)), 0), signals


def score_short_window(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    vols: List[float],
    rsi_1h: Optional[float] = None,
    funding: Optional[float] = None,
    fr_prev: Optional[float] = None,
    spread: Optional[float] = None,
) -> Tuple[int, List[str]]:
    W = SHORT_WEIGHTS
    score = 0
    signals: List[str] = []

    rsi, prev_rsi, rsi_hist = calc_rsi(closes)
    bb = calc_bb(closes)
    ema_series = calc_ema_series(closes)
    ema21 = ema_series[-1] if ema_series else None
    vs = vol_spike_ratio(vols)
    has_div = check_divergence(closes, rsi_hist, bullish=False)

    price  = closes[-1]
    open24 = closes[0]
    high24 = max(highs)
    change = _pct(price, open24)
    dist_high = _pct(high24, price) if price > 0 else None

    # EMA slope
    ema200 = calc_ema(closes, period=200)

    if len(ema_series) >= 4:
        valid_emas = [e for e in ema_series[-4:] if e is not None]
        if len(valid_emas) >= 2:
            slope = valid_emas[-1] - valid_emas[-2]
            if slope < 0:
                score += 12
                signals.append(f"Negative EMA Slope ({slope:.4f}) — Downtrend confirmed")
            elif slope > 0.01:
                score += 5
                signals.append(f"EMA Slope Flattening — Uptrend slowing")

    # Trend Filter (Hard for Shorts)
    if ema200 is not None:
        if price > ema200:
            score -= 15
            signals.append(f"Price above EMA200 ({price:.2f} > {ema200:.2f}) — Uptrend (risky short)")
        else:
            score += 10
            signals.append(f"Price below EMA200 ({price:.2f} < {ema200:.2f}) — Downtrend (trend-aligned short)")

    # Spread / liquidity
    if spread is not None:
        if spread > 0.15:
            score -= 10
            signals.append(f"Low Liquidity (Spread {spread:.2f}%)")
        elif spread < 0.05:
            score += 5
            signals.append(f"High Liquidity (Spread {spread:.2f}%)")

    # Funding momentum
    if funding is not None and fr_prev is not None:
        fr_change = funding - fr_prev
        if fr_change > 0:
            score += W["funding_momentum"]
            signals.append(f"Funding Momentum (becoming more positive +{fr_change*100:.4f}% — fade building)")

    # HTF alignment
    if rsi_1h is not None:
        if rsi_1h > 65:
            score += W["htf_align_overbought"]
            signals.append(f"HTF Alignment (1H RSI {rsi_1h:.1f}) — deeply overbought")
        elif rsi_1h > 55:
            score += 8
            signals.append(f"HTF Alignment (1H RSI {rsi_1h:.1f}) — overbought territory")

    # Divergence
    if has_div:
        score += W["divergence"]
        signals.append("Bearish Divergence (Price HH vs RSI LH) — buyers exhausted")

    # RSI
    if rsi is not None:
        rolling_over = prev_rsi is not None and rsi < prev_rsi
        if rsi > 75:
            score += W["rsi_overbought"]
            signals.append(f"RSI {rsi:.1f} (extremely overbought)")
        elif 55 <= rsi <= 75:
            pts = W["rsi_rollover"] + (8 if rolling_over else 0)
            signals.append(f"RSI {rsi:.1f} (rollover zone{' ✓ rolling over' if rolling_over else ''})")
            score += pts
        elif rsi < 35:
            score -= 5
            signals.append(f"RSI {rsi:.1f} (oversold — risky short)")

    # BB
    if bb is not None:
        bb_range = bb["upper"] - bb["lower"]
        if bb_range > 0:
            bb_pct = (price - bb["lower"]) / bb_range
            if bb_pct >= 0.90:
                score += W["bb_upper_90"]
                signals.append(f"Price at/above BB upper ({bb_pct*100:.0f}%) — extreme overbought")
            elif bb_pct >= 0.75:
                score += W["bb_upper_75"]
                signals.append(f"Near BB upper ({bb_pct*100:.0f}%) — overbought")
            elif bb_pct >= 0.55:
                score += 5
                signals.append(f"Above BB mid ({bb_pct*100:.0f}%)")
            elif bb_pct < 0.45:
                score -= 5
                signals.append(f"Below BB mid — fading short ({bb_pct*100:.0f}%)")

    # EMA stretch
    if ema21 is not None:
        pct_ema = _pct(price, ema21)
        if pct_ema > 3.0:
            score += W["ema_stretch_3"]
            signals.append(f"Price {pct_ema:.1f}% above EMA21 (mean-reversion opportunity)")
            if rsi is not None and rsi > 65:
                score += 5
                signals.append("Stretch bonus: Deeply overbought RSI + Above EMA21")
        elif pct_ema > 1.0:
            score += 5
            signals.append(f"Price {pct_ema:.1f}% above EMA21")
        elif pct_ema < -1.0:
            score -= 10
            signals.append(f"Price {abs(pct_ema):.1f}% below EMA21 (extended)")

    # Volume spike
    if vs >= 2.0:
        score += W["vol_spike_2"]
        signals.append(f"Volume spike {vs:.1f}x (exhaustion/distribution)")
    elif vs >= 1.4:
        score += 7
        signals.append(f"Elevated volume {vs:.1f}x")

    # 24h change
    if change > 12:
        score += 20
        signals.append(f"+{change:.1f}% pump (overbought fade)")
    elif 5 <= change <= 12:
        score += 12
        signals.append(f"+{change:.1f}% rally (fade opportunity)")
    elif 2 < change < 5:
        score += 5
        signals.append(f"+{change:.1f}% small rally (fade entry)")
    elif change < -15:
        signals.append(f"{change:.1f}% crash — already crashed (risky short)")

    # Near 24h high
    if dist_high is not None and dist_high < 1.0:
        score += 12
        signals.append(f"Near 24h High ({dist_high:.1f}% distance) — supply zone")
    elif dist_high is not None and dist_high < 2.0:
        score += 6
        signals.append(f"Close to 24h High ({dist_high:.1f}% distance)")

    # Funding
    if funding is not None:
        fr_pct = funding * 100
        if fr_pct > 0.10:
            score += W["funding_high"]
            signals.append(f"Funding +{fr_pct:.4f}% (heavily crowded longs — fade primed)")
        elif fr_pct > 0.05:
            score += 16
            signals.append(f"Funding +{fr_pct:.4f}% (crowded longs)")
        elif fr_pct > 0.01:
            score += 8
            signals.append(f"Funding +{fr_pct:.4f}% (mild long bias)")
        elif fr_pct < -0.05:
            score -= 12
            signals.append(f"Funding {fr_pct:.4f}% (crowded shorts — risky short)")

    return max(int(round(score)), 0), signals

# ─────────────────────────────────────────────────────────────────────
# Trade dataclass
# ─────────────────────────────────────────────────────────────────────
@dataclass
class Trade:
    symbol:        str
    direction:     str           # "LONG" | "SHORT"
    entry_idx:     int           # index into ohlcv list
    entry_price:   float
    exit_idx:      Optional[int] = None
    exit_price:    Optional[float] = None
    pnl_usdt:      Optional[float] = None
    pnl_pct:       Optional[float] = None   # raw price move %
    score:         int = 0
    signals:       List[str] = field(default_factory=list)
    exit_reason:   str = "open"  # "trail_stop" | "hard_stop" | "max_hold" | "end_of_data"
    hold_candles:  int = 0
    slippage_pct:  float = 0.0
    leverage:      int = 30
    margin:        float = 10.0
    trail_pct:     float = 0.005
    is_low_liq:    bool = False

# ─────────────────────────────────────────────────────────────────────
# Core walk-forward backtester for one symbol
# ─────────────────────────────────────────────────────────────────────
def backtest_symbol(
    symbol:            str,
    candles:           List[list],
    spread:            Optional[float],
    funding:           Optional[float],
    rsi_1h:            Optional[float],
    min_score:         int,
    trail_pct:         float,
    leverage:          int,
    margin:            float,
    window:            int   = 100,
    max_hold:          int   = 96,
    min_score_low_liq: int   = 145,
    hard_stop_pct:     float = 0.0,   # 0 = disabled; e.g. 0.03 = 3% hard stop from entry
    take_profit_pct:   float = 0.0,   # 0 = disabled; e.g. 0.05 = 5% take profit from entry
    cooldown:          int   = 0,     # min candles between trades (re-entry guard)
    direction:         str   = "BOTH",# "LONG" | "SHORT" | "BOTH"
    min_score_gap:     int   = 0,     # min gap between long and short scores to enter
) -> List[Trade]:

    if len(candles) < window + 2:
        return []

    # Parse to OHLCV tuples
    ohlcv: List[Tuple[float, float, float, float, float]] = []
    for c in candles:
        try:
            ohlcv.append((float(c[3]), float(c[4]), float(c[5]), float(c[6]),
                          float(c[7]) if len(c) > 7 else 0.0))
        except Exception:
            continue
    if len(ohlcv) < window + 2:
        return []

    is_low_liq  = spread is not None and spread > 0.15
    eff_min     = min_score_low_liq if is_low_liq else min_score

    # Slippage: half the spread on each side (market order crosses half the spread)
    slip_one_side = (spread / 2.0 / 100.0) if spread is not None else 0.0008
    fee_one_side  = TAKER_FEE

    direction_upper = direction.upper()

    trades:       List[Trade] = []
    in_pos:       bool        = False
    pos:          Optional[Trade] = None
    high_water:   float       = 0.0
    low_water:    float       = float("inf")
    stop_px:      float       = 0.0
    last_exit_idx: int        = -(cooldown + 1)   # allows entry on very first bar

    for i in range(window, len(ohlcv) - 1):
        c_open, c_high, c_low, c_close, c_vol = ohlcv[i]

        # ── Manage open position ──────────────────────────────────────
        if in_pos and pos is not None and i > pos.entry_idx:
            if pos.direction == "LONG":
                # Take profit check
                if take_profit_pct > 0:
                    tp_level = pos.entry_price * (1.0 + take_profit_pct)
                    if c_high >= tp_level:
                        exit_px  = tp_level * (1.0 - slip_one_side - fee_one_side)
                        raw_ret  = (exit_px - pos.entry_price) / pos.entry_price
                        pos.exit_idx     = i
                        pos.exit_price   = exit_px
                        pos.pnl_pct      = raw_ret * 100
                        pos.pnl_usdt     = raw_ret * leverage * margin
                        pos.exit_reason  = "take_profit"
                        pos.hold_candles = i - pos.entry_idx
                        trades.append(pos)
                        in_pos = False; pos = None
                        last_exit_idx = i
                        continue

                # Hard stop check (fires before trailing stop)
                if hard_stop_pct > 0:
                    hard_stop_level = pos.entry_price * (1.0 - hard_stop_pct)
                    if c_low <= hard_stop_level:
                        exit_px  = hard_stop_level * (1.0 - slip_one_side - fee_one_side)
                        raw_ret  = (exit_px - pos.entry_price) / pos.entry_price
                        pos.exit_idx     = i
                        pos.exit_price   = exit_px
                        pos.pnl_pct      = raw_ret * 100
                        pos.pnl_usdt     = raw_ret * leverage * margin
                        pos.exit_reason  = "hard_stop"
                        pos.hold_candles = i - pos.entry_idx
                        trades.append(pos)
                        in_pos = False; pos = None
                        last_exit_idx = i
                        continue

                # Ratchet high-water on candle high
                if c_close > high_water:
                    high_water = c_close
                    stop_px = high_water * (1.0 - trail_pct)
                # Trail stop hit if candle low touches or crosses stop
                if c_close <= stop_px:
                    exit_px   = stop_px * (1.0 - slip_one_side - fee_one_side)
                    raw_ret   = (exit_px - pos.entry_price) / pos.entry_price
                    pos.exit_idx     = i
                    pos.exit_price   = exit_px
                    pos.pnl_pct      = raw_ret * 100
                    pos.pnl_usdt     = raw_ret * leverage * margin
                    pos.exit_reason  = "trail_stop"
                    pos.hold_candles = i - pos.entry_idx
                    trades.append(pos)
                    in_pos = False; pos = None
                    last_exit_idx = i
                    continue

            else:  # SHORT
                # Take profit check
                if take_profit_pct > 0:
                    tp_level = pos.entry_price * (1.0 - take_profit_pct)
                    if c_low <= tp_level:
                        exit_px  = tp_level * (1.0 + slip_one_side + fee_one_side)
                        raw_ret  = (pos.entry_price - exit_px) / pos.entry_price
                        pos.exit_idx     = i
                        pos.exit_price   = exit_px
                        pos.pnl_pct      = raw_ret * 100
                        pos.pnl_usdt     = raw_ret * leverage * margin
                        pos.exit_reason  = "take_profit"
                        pos.hold_candles = i - pos.entry_idx
                        trades.append(pos)
                        in_pos = False; pos = None
                        last_exit_idx = i
                        continue

                # Hard stop check
                if hard_stop_pct > 0:
                    hard_stop_level = pos.entry_price * (1.0 + hard_stop_pct)
                    if c_high >= hard_stop_level:
                        exit_px  = hard_stop_level * (1.0 + slip_one_side + fee_one_side)
                        raw_ret  = (pos.entry_price - exit_px) / pos.entry_price
                        pos.exit_idx     = i
                        pos.exit_price   = exit_px
                        pos.pnl_pct      = raw_ret * 100
                        pos.pnl_usdt     = raw_ret * leverage * margin
                        pos.exit_reason  = "hard_stop"
                        pos.hold_candles = i - pos.entry_idx
                        trades.append(pos)
                        in_pos = False; pos = None
                        last_exit_idx = i
                        continue

                if c_close < low_water:
                    low_water = c_close
                    stop_px = low_water * (1.0 + trail_pct)
                if c_close >= stop_px:
                    exit_px   = stop_px * (1.0 + slip_one_side + fee_one_side)
                    raw_ret   = (pos.entry_price - exit_px) / pos.entry_price
                    pos.exit_idx     = i
                    pos.exit_price   = exit_px
                    pos.pnl_pct      = raw_ret * 100
                    pos.pnl_usdt     = raw_ret * leverage * margin
                    pos.exit_reason  = "trail_stop"
                    pos.hold_candles = i - pos.entry_idx
                    trades.append(pos)
                    in_pos = False; pos = None
                    last_exit_idx = i
                    continue

            # Max hold exit at candle close
            if i - pos.entry_idx >= max_hold:
                exit_px = c_close
                if pos.direction == "LONG":
                    raw_ret = (exit_px - pos.entry_price) / pos.entry_price
                else:
                    raw_ret = (pos.entry_price - exit_px) / pos.entry_price
                pos.exit_idx     = i
                pos.exit_price   = exit_px
                pos.pnl_pct      = raw_ret * 100
                pos.pnl_usdt     = raw_ret * leverage * margin
                pos.exit_reason  = "max_hold"
                pos.hold_candles = i - pos.entry_idx
                trades.append(pos)
                in_pos = False; pos = None
                last_exit_idx = i
            continue

        # ── Cooldown guard ────────────────────────────────────────────
        if cooldown > 0 and (i - last_exit_idx) < cooldown:
            continue

        # ── Look for entry signal on this window ──────────────────────
        w = ohlcv[i - window: i]
        closes_w = [x[3] for x in w]
        highs_w  = [x[1] for x in w]
        lows_w   = [x[2] for x in w]
        vols_w   = [x[4] for x in w]

        l_score, l_sigs = (0, []) if direction_upper == "SHORT" else \
            score_long_window(closes_w, highs_w, lows_w, vols_w,
                              rsi_1h, funding, None, spread)
        s_score, s_sigs = (0, []) if direction_upper == "LONG" else \
            score_short_window(closes_w, highs_w, lows_w, vols_w,
                               rsi_1h, funding, None, spread)

        best = max(l_score, s_score)
        if best < eff_min:
            continue

        # Score gap filter — skip ambiguous signals
        if min_score_gap > 0 and abs(l_score - s_score) < min_score_gap:
            continue

        if l_score >= s_score:
            direction_trade, entry_score, entry_sigs = "LONG",  l_score, l_sigs
        else:
            direction_trade, entry_score, entry_sigs = "SHORT", s_score, s_sigs

        # Enter at NEXT candle OPEN (no lookahead bias)
        n_open = ohlcv[i + 1][0]
        if n_open <= 0:
            continue

        if direction_trade == "LONG":
            entry_px  = n_open * (1.0 + slip_one_side + fee_one_side)
            stop_px   = entry_px * (1.0 - trail_pct)
            high_water = entry_px
        else:
            entry_px  = n_open * (1.0 - slip_one_side - fee_one_side)
            stop_px   = entry_px * (1.0 + trail_pct)
            low_water  = entry_px

        pos = Trade(
            symbol=symbol, direction=direction_trade,
            entry_idx=i + 1, entry_price=entry_px,
            score=entry_score, signals=entry_sigs,
            slippage_pct=(slip_one_side + fee_one_side) * 100,
            leverage=leverage, margin=margin, trail_pct=trail_pct,
            is_low_liq=is_low_liq,
        )
        in_pos = True

    # Close any remaining position at last candle close
    if in_pos and pos is not None and i > pos.entry_idx:
        exit_px = ohlcv[-1][3]
        if pos.direction == "LONG":
            raw_ret = (exit_px - pos.entry_price) / pos.entry_price
        else:
            raw_ret = (pos.entry_price - exit_px) / pos.entry_price
        pos.exit_idx     = len(ohlcv) - 1
        pos.exit_price   = exit_px
        pos.pnl_pct      = raw_ret * 100
        pos.pnl_usdt     = raw_ret * leverage * margin
        pos.exit_reason  = "end_of_data"
        pos.hold_candles = len(ohlcv) - 1 - pos.entry_idx
        trades.append(pos)

    return trades

# ─────────────────────────────────────────────────────────────────────
# Risk metrics
# ─────────────────────────────────────────────────────────────────────
def compute_drawdown(trades: List[Trade]) -> Tuple[float, float]:
    """Max drawdown (absolute USDT and %) from the sequential trade order."""
    if not trades:
        return 0.0, 0.0
    equity = 0.0
    peak   = 0.0
    max_dd = 0.0
    for t in trades:
        equity += t.pnl_usdt or 0.0
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = (max_dd / peak * 100.0) if peak > 0 else 0.0
    return max_dd, max_dd_pct


def compute_sharpe(trades: List[Trade], timeframe: str = "15m") -> float:
    """Annualised Sharpe ratio (risk-free rate = 0) from per-trade PnL."""
    if len(trades) < 2:
        return 0.0
    pnls = np.array([t.pnl_usdt or 0.0 for t in trades], dtype=float)
    avg_hold = float(np.mean([t.hold_candles for t in trades])) or 1.0
    cpy = CANDLES_PER_YEAR.get(timeframe, 35_040)
    trades_per_year = cpy / avg_hold
    mean_r = float(np.mean(pnls))
    std_r  = float(np.std(pnls, ddof=1))
    if std_r == 0:
        return 0.0
    return float(mean_r / std_r * math.sqrt(trades_per_year))


def compute_sortino(trades: List[Trade], timeframe: str = "15m") -> float:
    """Annualised Sortino ratio — only penalises downside deviation."""
    if len(trades) < 2:
        return 0.0
    pnls = np.array([t.pnl_usdt or 0.0 for t in trades], dtype=float)
    avg_hold = float(np.mean([t.hold_candles for t in trades])) or 1.0
    cpy = CANDLES_PER_YEAR.get(timeframe, 35_040)
    trades_per_year = cpy / avg_hold
    mean_r  = float(np.mean(pnls))
    neg_pnl = pnls[pnls < 0]
    if len(neg_pnl) < 2:
        return float("inf") if mean_r > 0 else 0.0
    down_std = float(np.std(neg_pnl, ddof=1))
    if down_std == 0:
        return 0.0
    return float(mean_r / down_std * math.sqrt(trades_per_year))


def max_streaks(trades: List[Trade]) -> Tuple[int, int]:
    """Return (max_win_streak, max_loss_streak) from sequential trade list."""
    max_w = max_l = cur_w = cur_l = 0
    for t in trades:
        if (t.pnl_usdt or 0.0) > 0:
            cur_w += 1; cur_l = 0
        else:
            cur_l += 1; cur_w = 0
        max_w = max(max_w, cur_w)
        max_l = max(max_l, cur_l)
    return max_w, max_l

# ─────────────────────────────────────────────────────────────────────
# Parameter sweep
# ─────────────────────────────────────────────────────────────────────
@dataclass
class SweepResult:
    trail_pct:      float
    stop_loss_pct:  float
    take_profit_pct: float
    min_score:      int
    leverage:       int
    total_trades:   int
    wins:           int
    losses:         int
    win_rate:       float
    total_pnl:      float
    avg_win:        float
    avg_loss:       float
    profit_factor:  float
    avg_hold:       float
    expectancy:     float
    max_drawdown:   float = 0.0


def sweep(
    sym_data:       List[Tuple],
    trail_pcts:     List[float],
    sl_pcts:        List[float],
    tp_pcts:        List[float],
    min_scores:     List[int],
    leverages:      List[int],
    margin:         float = 10.0,
    max_hold:       int   = 96,
    cooldown:       int   = 0,
    direction:      str   = "BOTH",
    min_score_gap:  int   = 0,
) -> List[SweepResult]:
    combos = [(t, sl, tp, m, l) for t in trail_pcts for sl in sl_pcts for tp in tp_pcts for m in min_scores for l in leverages]
    results = []
    for idx, (tp_trail, sl, tp, ms, lv) in enumerate(combos):
        all_t = []
        for sym, candles, spread, funding, rsi_1h in sym_data:
            all_t.extend(backtest_symbol(
                sym, candles, spread, funding, rsi_1h,
                min_score=ms, trail_pct=tp_trail, leverage=lv,
                margin=margin, max_hold=max_hold,
                hard_stop_pct=sl, take_profit_pct=tp,
                cooldown=cooldown,
                direction=direction, min_score_gap=min_score_gap,
            ))
        closed = [t for t in all_t if t.pnl_usdt is not None and t.exit_reason != "open"]
        print(f"\r  Sweeping {idx+1}/{len(combos)} — trail={tp_trail*100:.1f}% sl={sl*100:.1f}% tp={tp*100:.1f}% score={ms} lev={lv}x"
              f" → {len(closed)} trades", end="", flush=True)
        if not closed:
            continue
        wins   = [t for t in closed if t.pnl_usdt > 0]
        losses = [t for t in closed if t.pnl_usdt <= 0]
        tpnl   = sum(t.pnl_usdt for t in closed)
        gw     = sum(t.pnl_usdt for t in wins)
        gl     = abs(sum(t.pnl_usdt for t in losses))
        max_dd, _ = compute_drawdown(closed)
        results.append(SweepResult(
            trail_pct=tp_trail, stop_loss_pct=sl, take_profit_pct=tp, min_score=ms, leverage=lv,
            total_trades=len(closed),
            wins=len(wins), losses=len(losses),
            win_rate=len(wins)/len(closed)*100,
            total_pnl=tpnl,
            avg_win=float(np.mean([t.pnl_usdt for t in wins])) if wins else 0,
            avg_loss=float(np.mean([t.pnl_usdt for t in losses])) if losses else 0,
            profit_factor=gw/gl if gl > 0 else float("inf"),
            avg_hold=float(np.mean([t.hold_candles for t in closed])),
            expectancy=tpnl/len(closed),
            max_drawdown=max_dd,
        ))
    print()
    return sorted(results, key=lambda r: r.expectancy, reverse=True)

# ─────────────────────────────────────────────────────────────────────
# Stats & reporting
# ─────────────────────────────────────────────────────────────────────
def bar(pct: float, width: int = 20) -> str:
    filled = int(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)


def print_stats(trades: List[Trade], label: str = "", timeframe: str = "15m"):
    closed = [t for t in trades if t.pnl_usdt is not None and t.exit_reason != "open"]
    if not closed:
        print(Fore.YELLOW + "  No closed trades to analyse."); return

    wins   = [t for t in closed if t.pnl_usdt > 0]
    losses = [t for t in closed if t.pnl_usdt <= 0]
    tpnl   = sum(t.pnl_usdt for t in closed)
    wr     = len(wins) / len(closed) * 100
    aw     = float(np.mean([t.pnl_usdt for t in wins])) if wins else 0
    al     = float(np.mean([t.pnl_usdt for t in losses])) if losses else 0
    gw     = sum(t.pnl_usdt for t in wins)
    gl     = abs(sum(t.pnl_usdt for t in losses))
    pf     = gw / gl if gl > 0 else float("inf")
    ah     = float(np.mean([t.hold_candles for t in closed]))
    exp    = tpnl / len(closed)
    pc     = Fore.GREEN if tpnl >= 0 else Fore.RED

    max_dd, max_dd_pct = compute_drawdown(closed)
    sharpe  = compute_sharpe(closed, timeframe)
    sortino = compute_sortino(closed, timeframe)
    mws, mls = max_streaks(closed)
    best_t  = max(closed, key=lambda t: t.pnl_usdt or 0.0)
    worst_t = min(closed, key=lambda t: t.pnl_usdt or 0.0)

    print(Fore.CYAN + f"\n{'═'*70}")
    if label:
        print(Fore.CYAN + Style.BRIGHT + f"  RESULTS — {label}")
    print(Fore.CYAN + f"{'═'*70}")
    print(f"  Trades      : {len(closed)}  ({len(wins)} W / {len(losses)} L)")
    print(f"  Win Rate    : {wr:.1f}%  [{bar(wr)}]")
    print(f"  Total PnL   : {pc}{tpnl:+.4f} USDT{Style.RESET_ALL}")
    print(f"  Expectancy  : {pc}{exp:+.4f} USDT/trade{Style.RESET_ALL}")
    print(f"  Avg Win     : {Fore.GREEN}{aw:+.4f}{Style.RESET_ALL}  "
          f"Avg Loss: {Fore.RED}{al:+.4f}{Style.RESET_ALL}")
    pf_c = Fore.GREEN if pf >= 1.5 else (Fore.YELLOW if pf >= 1.0 else Fore.RED)
    pf_str = f"{pf:.2f}" if pf < 99 else "∞"
    print(f"  Prof. Factor: {pf_c}{pf_str}{Style.RESET_ALL}")
    print(f"  Avg Hold    : {ah:.1f} candles")
    print()

    # ── NEW: Risk metrics ─────────────────────────────────────────────
    dd_c = Fore.RED if max_dd > 0 else Fore.GREEN
    sh_c = Fore.GREEN if sharpe >= 1.0 else (Fore.YELLOW if sharpe >= 0 else Fore.RED)
    so_c = Fore.GREEN if sortino >= 1.5 else (Fore.YELLOW if sortino >= 0 else Fore.RED)
    print(f"  Max Drawdown: {dd_c}{max_dd:+.4f} USDT  ({max_dd_pct:.1f}%){Style.RESET_ALL}")
    print(f"  Sharpe (ann): {sh_c}{sharpe:+.2f}{Style.RESET_ALL}   "
          f"Sortino (ann): {so_c}{sortino:+.2f}{Style.RESET_ALL}")
    print(f"  Max Streak  : {Fore.GREEN}{mws}W{Style.RESET_ALL} / {Fore.RED}{mls}L{Style.RESET_ALL}")
    print(f"  Best Trade  : {Fore.GREEN}{best_t.pnl_usdt:+.4f}{Style.RESET_ALL} "
          f"({best_t.symbol} {best_t.direction})")
    print(f"  Worst Trade : {Fore.RED}{worst_t.pnl_usdt:+.4f}{Style.RESET_ALL} "
          f"({worst_t.symbol} {worst_t.direction})")
    print()

    # Direction breakdown
    for dir_label, group in [("LONG", [t for t in closed if t.direction == "LONG"]),
                              ("SHORT",[t for t in closed if t.direction == "SHORT"])]:
        if not group: continue
        g_wr  = len([t for t in group if t.pnl_usdt > 0]) / len(group) * 100
        g_pnl = sum(t.pnl_usdt for t in group)
        g_exp = g_pnl / len(group)
        dc    = Fore.GREEN if g_pnl >= 0 else Fore.RED
        print(f"  {dir_label:<6}: {len(group):3} trades | WR {g_wr:.0f}% "
              f"| PnL {dc}{g_pnl:+.4f}{Style.RESET_ALL} | exp {dc}{g_exp:+.4f}{Style.RESET_ALL}")

    print()
    # Exit reason breakdown
    for reason in ["trail_stop", "hard_stop", "take_profit", "max_hold", "end_of_data"]:
        group = [t for t in closed if t.exit_reason == reason]
        if not group: continue
        g_wr  = len([t for t in group if t.pnl_usdt > 0]) / len(group) * 100
        g_pnl = sum(t.pnl_usdt for t in group)
        rc    = Fore.GREEN if g_pnl >= 0 else Fore.RED
        print(f"  {reason:<14}: {len(group):3} trades | WR {g_wr:.0f}% | PnL {rc}{g_pnl:+.4f}{Style.RESET_ALL}")

    # Score tier breakdown
    print(Fore.CYAN + f"\n  {'─'*64}")
    print("  SCORE TIER BREAKDOWN:")
    tiers = [(145, 999, "145+"), (120, 144, "120-144"),
             (100, 119, "100-119"), (80, 99, "80-99"), (0, 79, "<80")]
    for lo, hi, tlabel in tiers:
        group = [t for t in closed if lo <= t.score <= hi]
        if len(group) < 2: continue
        g_wr  = len([t for t in group if t.pnl_usdt > 0]) / len(group) * 100
        g_exp = sum(t.pnl_usdt for t in group) / len(group)
        wc    = Fore.GREEN if g_wr >= 50 else Fore.RED
        ec    = Fore.GREEN if g_exp >= 0 else Fore.RED
        print(f"  {tlabel:>8}: [{wc}{bar(g_wr)}{Style.RESET_ALL}] "
              f"{g_wr:4.0f}% WR | {len(group):3} trades | "
              f"exp {ec}{g_exp:+.4f}{Style.RESET_ALL}")

    # Signal type analysis
    print(Fore.CYAN + f"\n  {'─'*64}")
    print("  SIGNAL → OUTCOME ANALYSIS  (n ≥ 3 only):")
    signal_groups = [
        ("RSI oversold",        ["extremely oversold", "RSI.*oversold", "RSI.*recovering"]),
        ("RSI overbought",      ["extremely overbought", "rollover zone"]),
        ("BB lower",            ["BB lower", "below BB lower"]),
        ("BB upper",            ["BB upper", "above BB upper"]),
        ("Bullish Divergence",  ["Bullish Divergence"]),
        ("Bearish Divergence",  ["Bearish Divergence"]),
        ("HTF Alignment",       ["HTF Alignment"]),
        ("Volume spike",        ["Volume spike"]),
        ("Negative Funding",    ["Negative Funding"]),
        ("Positive Funding",    ["Positive Funding", "crowded longs"]),
        ("Low Liquidity",       ["Low Liquidity"]),
        ("EMA stretch below",   ["below EMA21"]),
        ("EMA stretch above",   ["above EMA21"]),
        ("Crash/Dip",           ["crash", "dip \\(oversold"]),
        ("Pump",                ["pump \\(overbought", "rally \\(fade"]),
        ("Near 24h low",        ["Near 24h low"]),
        ("Near 24h high",       ["Near 24h High", "Close to 24h High"]),
    ]
    for slabel, patterns in signal_groups:
        def has_signal(t, pats=patterns):
            return any(
                any(re.search(p, s, re.IGNORECASE) for p in pats)
                for s in t.signals
            )
        group = [t for t in closed if has_signal(t)]
        if len(group) < 3: continue
        g_wr  = len([t for t in group if t.pnl_usdt > 0]) / len(group) * 100
        g_exp = sum(t.pnl_usdt for t in group) / len(group)
        wc = Fore.GREEN if g_wr >= 50 else Fore.RED
        ec = Fore.GREEN if g_exp >= 0 else Fore.RED
        print(f"  {slabel:<22}: {wc}{g_wr:4.0f}% WR{Style.RESET_ALL} "
              f"| {ec}{g_exp:+.5f} exp{Style.RESET_ALL} | n={len(group)}")

    # Low-liq vs normal
    print(Fore.CYAN + f"\n  {'─'*64}")
    for ll_label, ll_val in [("Normal liquidity", False), ("Low liquidity", True)]:
        group = [t for t in closed if t.is_low_liq == ll_val]
        if not group: continue
        g_wr  = len([t for t in group if t.pnl_usdt > 0]) / len(group) * 100
        g_pnl = sum(t.pnl_usdt for t in group)
        lc    = Fore.GREEN if g_pnl >= 0 else Fore.RED
        print(f"  {ll_label:<22}: WR {g_wr:.0f}% | PnL {lc}{g_pnl:+.4f}{Style.RESET_ALL} | n={len(group)}")


def print_per_symbol_stats(trades: List[Trade], top_n: int = 20):
    """Print a per-symbol performance table sorted by total PnL."""
    closed = [t for t in trades if t.pnl_usdt is not None and t.exit_reason != "open"]
    if not closed:
        return

    from collections import defaultdict
    sym_map: Dict[str, List[Trade]] = defaultdict(list)
    for t in closed:
        sym_map[t.symbol].append(t)

    rows = []
    for sym, ts in sym_map.items():
        wins_   = [t for t in ts if t.pnl_usdt > 0]
        pnl     = sum(t.pnl_usdt for t in ts)
        wr      = len(wins_) / len(ts) * 100
        exp     = pnl / len(ts)
        rows.append((sym, len(ts), wr, pnl, exp))

    rows.sort(key=lambda r: r[3], reverse=True)   # sort by total PnL

    print(Fore.CYAN + f"\n{'═'*70}")
    print(Fore.CYAN + Style.BRIGHT + f"  PER-SYMBOL BREAKDOWN  (top/bottom {top_n}, sorted by PnL)")
    print(Fore.CYAN + f"{'═'*70}")
    print(f"  {'Symbol':<16} {'Trades':>7} {'WR%':>6} {'PnL':>11} {'Exp/Trade':>11}")
    print(f"  {'─'*55}")

    display = rows[:top_n]
    if len(rows) > top_n * 2:
        display += [None]   # separator
        display += rows[-top_n:]

    for row in display:
        if row is None:
            print(f"  {'  ···':^55}")
            continue
        sym, n, wr, pnl, exp = row
        pc  = Fore.GREEN if pnl >= 0 else Fore.RED
        wrc = Fore.GREEN if wr >= 50 else Fore.RED
        print(f"  {sym:<16} {n:>7} "
              f"{wrc}{wr:>5.1f}%{Style.RESET_ALL} "
              f"{pc}{pnl:>+10.4f}{Style.RESET_ALL} "
              f"{pc}{exp:>+10.4f}{Style.RESET_ALL}")


def print_sweep_results(results: List[SweepResult], top_n: int = 15):
    print(Fore.CYAN + f"\n{'═'*100}")
    print(Fore.CYAN + Style.BRIGHT + f"  PARAMETER SWEEP — TOP {top_n} BY EXPECTANCY")
    print(Fore.CYAN + f"{'═'*100}")
    print(f"  {'Trail%':>6} {'SL%':>6} {'TP%':>6} {'MinScore':>9} {'Lev':>4} {'Trades':>7} "
          f"{'WR%':>6} {'PnL':>10} {'PF':>6} {'Exp/Trade':>10} {'MaxDD':>9}")
    print(f"  {'─'*96}")
    for r in results[:top_n]:
        pc   = Fore.GREEN if r.total_pnl >= 0 else Fore.RED
        pf_c = Fore.GREEN if r.profit_factor >= 1.5 else (Fore.YELLOW if r.profit_factor >= 1.0 else Fore.RED)
        wr_c = Fore.GREEN if r.win_rate >= 50 else Fore.RED
        dd_c = Fore.RED if r.max_drawdown > abs(r.total_pnl) * 0.5 else Fore.YELLOW
        pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 99 else "∞"
        print(
            f"  {r.trail_pct*100:>5.1f}% "
            f"{r.stop_loss_pct*100:>5.1f}% "
            f"{r.take_profit_pct*100:>5.1f}% "
            f"{r.min_score:>9} "
            f"{r.leverage:>4}x "
            f"{r.total_trades:>7} "
            f"{wr_c}{r.win_rate:>5.1f}%{Style.RESET_ALL} "
            f"{pc}{r.total_pnl:>+9.2f}{Style.RESET_ALL} "
            f"{pf_c}{pf_str:>6}{Style.RESET_ALL} "
            f"{pc}{r.expectancy:>+9.4f}{Style.RESET_ALL} "
            f"{dd_c}{r.max_drawdown:>8.2f}{Style.RESET_ALL} "
        )


def save_trades_csv(trades: List[Trade], path: str):
    """Export closed trades to CSV."""
    closed = [t for t in trades if t.pnl_usdt is not None and t.exit_reason != "open"]
    if not closed:
        return
    fields = ["symbol", "direction", "score", "entry_price", "exit_price",
              "pnl_usdt", "pnl_pct", "hold_candles", "exit_reason",
              "slippage_pct", "leverage", "margin", "trail_pct", "is_low_liq"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in closed:
            w.writerow({k: getattr(t, k) for k in fields})
    print(Fore.GREEN + f"  CSV saved → {path}")

# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="FangBleeny Backtester v2.0 — walk-forward signal replay on real OHLCV",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # ── Core settings ─────────────────────────────────────────────────
    parser.add_argument("--symbols",    nargs="+", default=[],
                        help="Specific symbols to test")
    parser.add_argument("--timeframe",  default="15m")
    parser.add_argument("--candles",    type=int,   default=500,
                        help="Historical candles per symbol")
    parser.add_argument("--min-score",  type=int,   default=100)
    parser.add_argument("--trail-pct",  type=float, default=0.005)
    parser.add_argument("--leverage",   type=int,   default=30)
    parser.add_argument("--margin",     type=float, default=10.0,
                        help="USDT margin per trade")
    parser.add_argument("--max-hold",   type=int,   default=96,
                        help="Max candles to hold a trade")
    parser.add_argument("--min-vol",    type=int,   default=5_000_000)
    parser.add_argument("--workers",    type=int,   default=30,
                        help="Parallel fetch workers")

    # ── NEW: Risk / filter options ────────────────────────────────────
    parser.add_argument("--stop-loss-pct", type=float, default=0.0,
                        help="Hard stop loss %% from entry (0 = disabled, e.g. 0.03 = 3%%)")
    parser.add_argument("--take-profit-pct", type=float, default=0.0,
                        help="Take profit %% from entry (0 = disabled, e.g. 0.05 = 5%%)")
    parser.add_argument("--cooldown",   type=int,   default=0,
                        help="Min candles between trades on same symbol (re-entry guard)")
    parser.add_argument("--direction",  default=os.getenv("BOT_DIRECTION", "BOTH"),
                        choices=["LONG", "SHORT", "BOTH"],
                        help="Only take LONG or SHORT trades, or BOTH")
    parser.add_argument("--min-score-gap", type=int, default=0,
                        help="Min score gap between LONG and SHORT to avoid ambiguous entries")

    # ── Sweep ─────────────────────────────────────────────────────────
    parser.add_argument("--sweep",      action="store_true",
                        help="Run parameter grid sweep")
    parser.add_argument("--sweep-n",    type=int,   default=25,
                        help="Symbol count for sweep")

    # ── Output ────────────────────────────────────────────────────────
    parser.add_argument("--output",     default="backtest_results.json")
    parser.add_argument("--csv",        action="store_true",
                        help="Also save trade log as CSV alongside JSON output")
    parser.add_argument("--no-htf",     action="store_true",
                        help="Skip 1H RSI fetch (faster)")
    args = parser.parse_args()

    print(Fore.CYAN + BANNER)

    # Print active settings summary
    flags = []
    if args.stop_loss_pct > 0:
        flags.append(f"hard-stop {args.stop_loss_pct*100:.1f}%")
    if args.cooldown > 0:
        flags.append(f"cooldown {args.cooldown}c")
    if args.direction != "BOTH":
        flags.append(f"direction={args.direction}")
    if args.min_score_gap > 0:
        flags.append(f"score-gap≥{args.min_score_gap}")
    if flags:
        print(Fore.YELLOW + f"  Active options: {' | '.join(flags)}\n")

    # ── Symbol universe ───────────────────────────────────────────────
    if args.symbols:
        symbols = args.symbols
    else:
        print(Fore.WHITE + "  Fetching ticker universe...", end="", flush=True)
        tickers = get_tickers(min_vol=args.min_vol)
        tickers.sort(key=lambda t: float(t.get("turnoverRv") or 0), reverse=True)
        n = args.sweep_n if args.sweep else min(50, len(tickers))
        symbols = [t["symbol"] for t in tickers[:n]]
        print(f" {len(symbols)} symbols (vol ≥ ${args.min_vol:,.0f})")

    print(Fore.WHITE + f"  Fetching {args.candles}x {args.timeframe} candles"
          f"{' + 1H RSI' if not args.no_htf else ''}...")
    print(Fore.WHITE + f"  (This takes ~{max(5, len(symbols)//3)}s with {args.workers} workers)\n")

    # ── Parallel data fetch ───────────────────────────────────────────
    sym_data = []
    lock = threading.Lock()
    done_count = [0]

    def fetch(sym):
        candles = get_candles(sym, timeframe=args.timeframe, limit=args.candles)
        spread  = get_spread_pct(sym)
        funding = get_funding(sym)
        rsi_1h  = None if args.no_htf else get_htf_rsi(sym)
        with lock:
            sym_data.append((sym, candles, spread, funding, rsi_1h))
            done_count[0] += 1
            print(f"\r  Fetching data: {done_count[0]}/{len(symbols)} symbols", end="", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        ex.map(fetch, symbols)
    print()

    valid = [(s, c, sp, f, r) for s, c, sp, f, r in sym_data if len(c) >= 110]
    print(Fore.WHITE + f"  {len(valid)}/{len(symbols)} symbols with sufficient data\n")
    if not valid:
        print(Fore.RED + "  No valid data — check your BASE_URL and network.")
        print(Fore.YELLOW + f"  BASE_URL being used: {BASE_URL}")
        print(Fore.YELLOW + f"  Symbols attempted: {symbols[:5]}")
        print(Fore.YELLOW + f"  sym_data entries: {len(sym_data)} | candle counts: {[len(c) for _,c,*_ in sym_data[:5]]}")
        return

    # Shared kwargs for backtest_symbol
    bt_kwargs = dict(
        margin=args.margin, max_hold=args.max_hold,
        hard_stop_pct=args.stop_loss_pct, 
        take_profit_pct=args.take_profit_pct,
        cooldown=args.cooldown,
        direction=args.direction, min_score_gap=args.min_score_gap,
    )

    # ── Sweep or single run ───────────────────────────────────────────
    if args.sweep:
        print(Fore.CYAN + Style.BRIGHT + f"  🔍 PARAMETER SWEEP ({args.direction})\n")
        if args.timeframe in ["4H", "6H", "8H", "12H", "1D"]:
            trail_pcts  = [0.01, 0.02, 0.03, 0.04]
            sl_pcts     = [0.0, 0.04, 0.06]
            tp_pcts     = [0.0, 0.05, 0.1, 0.15]
        else:
            trail_pcts  = [0.003, 0.005, 0.008, 0.012]
            sl_pcts     = [0.015, 0.02, 0.03, 0.0]
            tp_pcts     = [0.0, 0.02, 0.04, 0.06]

        min_scores  = [110, 120, 130, 140]
        leverages   = [20, 30]

        # Remove parameters that are part of the sweep grid to avoid multiple values
        sweep_kwargs = bt_kwargs.copy()
        for p in ["min_score", "trail_pct", "leverage", "hard_stop_pct", "take_profit_pct"]:
            sweep_kwargs.pop(p, None)

        sweep_res = sweep(valid, trail_pcts, sl_pcts, tp_pcts, min_scores, leverages, **sweep_kwargs)
        print_sweep_results(sweep_res, top_n=15)

        # Detailed stats for the top config
        if sweep_res:
            best = sweep_res[0]
            print(Fore.CYAN + Style.BRIGHT + f"\n  Running detailed analysis on best config...")
            best_trades = []

            # Use separate kwargs to avoid duplicates
            analysis_kwargs = bt_kwargs.copy()
            for p in ["min_score", "trail_pct", "leverage", "hard_stop_pct", "take_profit_pct"]:
                analysis_kwargs.pop(p, None)
            
            for sym, candles, spread, funding, rsi_1h in valid:
                best_trades.extend(backtest_symbol(
                    sym, candles, spread, funding, rsi_1h,
                    min_score=best.min_score, trail_pct=best.trail_pct,
                    leverage=best.leverage,
                    hard_stop_pct=best.stop_loss_pct,
                    take_profit_pct=best.take_profit_pct,
                    **analysis_kwargs,
                ))
            print_stats(best_trades,
                label=f"SL {best.stop_loss_pct*100:.1f}% | TP {best.take_profit_pct*100:.1f}% | Score ≥{best.min_score} | {best.leverage}x lev",
                timeframe=args.timeframe)
            print_per_symbol_stats(best_trades)

        Path(args.output).write_text(json.dumps([
            {"trail_pct": r.trail_pct, "sl_pct": r.stop_loss_pct, "tp_pct": r.take_profit_pct,
             "min_score": r.min_score, "leverage": r.leverage,
             "total_trades": r.total_trades, "win_rate": r.win_rate, "total_pnl": r.total_pnl,
             "profit_factor": r.profit_factor if r.profit_factor < 9999 else 9999,
             "expectancy": r.expectancy, "max_drawdown": r.max_drawdown}
            for r in sweep_res
        ], indent=2))

    else:
        all_trades = []
        for sym, candles, spread, funding, rsi_1h in valid:
            all_trades.extend(backtest_symbol(
                sym, candles, spread, funding, rsi_1h,
                min_score=args.min_score, trail_pct=args.trail_pct,
                leverage=args.leverage, **bt_kwargs,
            ))

        label = (f"Trail {args.trail_pct*100:.1f}% | Score ≥{args.min_score} "
                 f"| {args.leverage}x | {args.timeframe} | {args.candles} candles")
        print_stats(all_trades, label=label, timeframe=args.timeframe)
        print_per_symbol_stats(all_trades)

        # Individual trade log
        closed = [t for t in all_trades if t.pnl_usdt is not None]
        if closed:
            print(Fore.CYAN + f"\n  TRADE LOG (worst → best, last 40):")
            print(f"  {'Symbol':<14} {'Dir':>5} {'Score':>6} {'PnL':>9} "
                  f"{'Hold':>6} {'Slip%':>6} {'Exit':<14} {'LowLiq':>6}")
            print(f"  {'─'*76}")
            for t in sorted(closed, key=lambda x: x.pnl_usdt or 0)[-40:]:
                pc = Fore.GREEN if (t.pnl_usdt or 0) > 0 else Fore.RED
                print(
                    f"  {t.symbol:<14} {t.direction:>5} {t.score:>6} "
                    f"{pc}{t.pnl_usdt:>+8.4f}{Style.RESET_ALL} "
                    f"{t.hold_candles:>5}c {t.slippage_pct:>5.3f}% "
                    f"{t.exit_reason:<14} {'⚠' if t.is_low_liq else '  '}"
                )

        Path(args.output).write_text(json.dumps([
            {"symbol": t.symbol, "direction": t.direction, "score": t.score,
             "entry": t.entry_price, "exit": t.exit_price,
             "pnl_usdt": t.pnl_usdt, "pnl_pct": t.pnl_pct,
             "hold_candles": t.hold_candles, "exit_reason": t.exit_reason,
             "signals": t.signals, "slippage_pct": t.slippage_pct,
             "is_low_liq": t.is_low_liq}
            for t in closed
        ], indent=2))

        if args.csv:
            csv_path = str(Path(args.output).with_suffix(".csv"))
            save_trades_csv(all_trades, csv_path)

    print(Fore.GREEN + f"\n  Results saved → {args.output}\n")

if __name__ == "__main__":
    main()
