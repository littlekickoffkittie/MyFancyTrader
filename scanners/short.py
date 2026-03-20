#!/usr/bin/env python3
"""
Phemex Short Setup Scanner — USDT-M Perpetuals
----------------------------------------------
Optimized short setup scanner for Phemex USDT-M Perpetuals.

Short bias logic:
  - RSI overbought / rolling over from highs (> 65 / 55-75 rollover zone)
  - Price at/above BB upper band
  - Price above EMA21 (mean reversion) or EMA21 turning down (trend continuation)
  - Positive funding (crowded longs → fade fuel)
  - Near 24h HIGH (not low)
  - Bearish candle patterns (shooting star, evening star, engulfing, etc.)
  - Bearish divergence (price makes higher high, RSI makes lower high)
  - Pump + dump setups, blow-off tops
"""

from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from colorama import init, Fore, Style
from dotenv import load_dotenv

try:
    import legacy.phemex_common as pc
except ImportError:
    try:
        from ..legacy import phemex_common as pc
    except (ImportError, ValueError):
        try:
            from legacy import phemex_common as pc
        except ImportError:
            import phemex_common as pc

# Initialize environment & colorama
load_dotenv()
init(autoreset=True)

# ----------------------------
# CONFIG & EXPORTS
# ----------------------------
__all__ = [
    "analyse", "get_tickers", "get_candles", "prefetch_all_funding_rates",
    "BASE_URL", "TIMEFRAME_MAP", "DEFAULTS"
]

BASE_URL = pc.BASE_URL
TIMEFRAME_MAP = pc.TIMEFRAME_MAP
DEFAULTS = pc.DEFAULTS

# Strategy thresholds
MIN_NEGATIVE_FUNDING = -0.02       # skip if funding too negative (crowded shorts = bad for shorts)
THREE_BLACK_CROWS_RSI_GATE = 45 
DIVERGENCE_WINDOW = 60
DIV_PRICE_THRESHOLD = 1.005        # 0.5% higher high
DIV_RSI_THRESHOLD = 3.0            # 3.0 RSI points lower high
RSI_OVERSOLD_ZONE = 35.0
RSI_OVERBOUGHT_ZONE = 65.0

# Score weights
WEIGHTS = {
    "divergence": 20,
    "rsi_rollover": 25,        # RSI rolling over from highs
    "rsi_overbought": 22,        # deep overbought reading
    "bb_upper_90": 30,         # price above/at BB upper
    "bb_upper_75": 22,         # price near BB upper
    "ema_stretch_3": 15,       # price significantly above EMA21 (mean reversion)
    "vol_spike_2": 15,         # volume spike
    "funding_high": 22,        # positive funding = crowded longs = fade fuel
    "htf_align_overbought": 15, # 1H RSI overbought confirms LTF short
    "funding_momentum": 10,    # funding becoming more positive (building fade)
}

TRADE_LOG_FILE = pc.os.path.dirname(pc.os.path.abspath(__file__)) + "/trade_log_short.json"
SCAN_OUTPUT_FILE = pc.os.path.dirname(pc.os.path.abspath(__file__)) + "/last_scan_short.json"

logger = logging.getLogger("phemex_short_scanner")
logger.addHandler(logging.NullHandler())

# ----------------------------
# Data classes
# ----------------------------
@dataclass
class TickerData:
    inst_id: str
    price: float
    rsi: Optional[float]
    prev_rsi: Optional[float]
    bb: Optional[Dict[str, float]]
    ema21: Optional[float]
    change_24h: Optional[float]
    funding_rate: Optional[float]
    patterns: List[Tuple[str, int, float]]
    dist_high_pct: Optional[float]  # distance from 24h high
    vol_spike: float
    has_div: bool
    rsi_1h: Optional[float]
    fr_change: float = 0.0
    spread: Optional[float] = None
    dist_to_node_above: Optional[float] = None   # distance to nearest high-vol resistance node
    ema_slope: Optional[float] = None
    slope_change: Optional[float] = None
    news_count: int = 0
    news_titles: List[str] = field(default_factory=list)
    raw_ohlc: List[Tuple[float, float, float, float]] = field(default_factory=list)
    vol_24h: float = 0.0

# ----------------------------
# Indicator Logic
# ----------------------------
def find_peaks(values: List[float], min_separation: int = 3) -> List[int]:
    """Local peak finder for bearish divergence."""
    peaks: List[int] = []
    n = len(values)
    if n < 3:
        return peaks
    for i in range(1, n - 1):
        if values[i] > values[i - 1] and values[i] > values[i + 1]:
            if not peaks or (i - peaks[-1] >= min_separation):
                peaks.append(i)
    return peaks

def detect_bearish_divergence(closes: List[float], rsi_values: List[Optional[float]]) -> bool:
    """
    Bearish divergence: price makes a higher high while RSI makes a lower high.
    This signals exhaustion of buyers and a likely reversal downward.
    """
    if len(closes) < DIVERGENCE_WINDOW or len(rsi_values) < DIVERGENCE_WINDOW:
        return False
    price_window = pc.np.asarray(closes[-DIVERGENCE_WINDOW:], dtype=float)
    rsi_window_list = rsi_values[-DIVERGENCE_WINDOW:]
    if any(v is None for v in rsi_window_list):
        return False
    rsi_window = pc.np.asarray([float(v) for v in rsi_window_list], dtype=float)

    price_peaks = find_peaks(price_window.tolist())
    rsi_peaks = find_peaks(rsi_window.tolist())

    if len(price_peaks) < 2 or len(rsi_peaks) < 2:
        return False

    # Time-alignment check — ensure price and RSI peaks occur within 5 candles of each other
    if abs(price_peaks[-1] - rsi_peaks[-1]) > 5 or abs(price_peaks[-2] - rsi_peaks[-2]) > 5:
        return False

    p1 = price_window[price_peaks[-2]]
    p2 = price_window[price_peaks[-1]]
    r1 = rsi_window[rsi_peaks[-2]]
    r2 = rsi_window[rsi_peaks[-1]]

    # Apply stricter thresholds and ensure second RSI peak is in overbought zone
    return (p2 > p1 * DIV_PRICE_THRESHOLD) and (r2 < r1 - DIV_RSI_THRESHOLD) and (r2 > RSI_OVERBOUGHT_ZONE - 10)

def detect_patterns(ohlc: List[Tuple[float, float, float, float]]) -> List:
    """Detect bearish reversal / continuation candle patterns."""
    patterns = []
    if len(ohlc) < 3:
        return patterns

    def body(c): return abs(c[3] - c[0])
    def upper_wick(c): return c[1] - max(c[0], c[3])
    def lower_wick(c): return min(c[0], c[3]) - c[2]
    def is_bear(c): return c[3] < c[0]
    def is_bull(c): return c[3] > c[0]

    c0, c1, c2 = ohlc[-3], ohlc[-2], ohlc[-1]

    # Shooting Star — long upper wick, small body, at high area
    if (upper_wick(c2) > 2 * body(c2)
            and lower_wick(c2) < body(c2) * 0.4
            and body(c2) > 0):
        patterns.append(("Shooting Star 🌠", 15, 1.0))

    # Gravestone Doji at high (supply spike)
    if (upper_wick(c2) > 2.5 * body(c2)
            and body(c2) < (c2[1] - c2[2]) * 0.2
            and c2[1] > c1[1]):
        patterns.append(("Gravestone Doji ⚰️", 14, 1.0))

    # Bearish Engulfing — bull candle followed by larger bear candle
    if (is_bull(c1) and is_bear(c2)
            and c2[3] <= c1[0] and c2[0] >= c1[3]
            and body(c2) > body(c1)):
        patterns.append(("Bearish Engulfing 🔴", 18, 1.0))

    # Evening Star — bull, small body (indecision), bear (3-candle reversal)
    if (is_bull(c0)
            and body(c1) < body(c0) * 0.5
            and is_bear(c2)
            and c2[3] < (c0[0] + c0[3]) / 2):
        patterns.append(("Evening Star 🌙", 20, 1.0))

    # Dark Cloud Cover — bull candle, bear opens above high, closes below midpoint
    if (is_bull(c1) and is_bear(c2)
            and c2[0] > c1[1]
            and c2[3] < (c1[0] + c1[3]) / 2
            and c2[3] > c1[0]):
        patterns.append(("Dark Cloud Cover ☁️", 16, 1.0))

    # Bearish Harami — bull followed by smaller bear inside it
    if (is_bull(c1) and is_bear(c2)
            and c2[0] < c1[3] and c2[3] > c1[0]
            and body(c2) < body(c1)):
        patterns.append(("Bearish Harami 🟥", 12, 1.0))

    # Doji at High — indecision after uptrend (reversal warning)
    if body(c2) < (c2[1] - c2[2]) * 0.15 and c2[1] > c1[1]:
        patterns.append(("Doji at High — Reversal Watch 🔄", 10, 1.0))

    # Three Black Crows — three consecutive bear candles
    if (is_bear(c0) and is_bear(c1) and is_bear(c2)
            and c1[3] < c0[3] and c2[3] < c1[3]
            and body(c0) > 0 and body(c1) > 0 and body(c2) > 0):
        patterns.append(("Three Black Crows 🐦‍⬛", 18, 1.0))

    # Bearish Marubozu — strong bear with almost no wicks (momentum)
    if (is_bear(c2)
            and upper_wick(c2) < body(c2) * 0.1
            and lower_wick(c2) < body(c2) * 0.1
            and body(c2) > (c2[1] - c2[2]) * 0.85):
        patterns.append(("Bearish Marubozu 📉", 14, 1.0))

    return patterns

# ----------------------------
# Confidence & Scoring — SHORT BIASED
# ----------------------------
def calc_confidence(rsi, bb_pct, ema21, price, change_24h, funding_rate, patterns, score, dist_high_pct, vol_spike):
    """
    Short-biased confidence: counts bearish agreeing signals vs bullish conflicts.
    """
    agreeing = 0.0
    conflicts = 0.0
    notes: List[str] = []

    if rsi is not None:
        if rsi > 55.0:
            agreeing += 1.0
        elif rsi < 35.0:
            conflicts += 1.0
            notes.append("RSI oversold — late entry risk")

    if bb_pct is not None:
        if bb_pct >= 65.0:
            agreeing += 1.0
        elif bb_pct < 30.0:
            conflicts += 1.0
            notes.append("price below BB 30%")

    if ema21 is not None and price is not None:
        pct = pc.pct_change(price, ema21)
        if pct > 1.0:
            agreeing += 1.0
        elif pct < -2.0:
            conflicts += 0.5

    if change_24h is not None:
        if 3.0 <= change_24h <= 15.0:
            agreeing += 1.0
        elif change_24h > 15.0:
            agreeing += 0.5
        elif change_24h < -5.0:
            conflicts += 1.0
            notes.append("dumping already")
        elif -0.5 < change_24h < 0.5:
            conflicts += 0.5
            notes.append("flat — no momentum")

    if funding_rate is not None:
        fr_pct = funding_rate * 100.0
        if fr_pct > 0.01:
            agreeing += 1.0
        elif fr_pct < -0.05:
            conflicts += 2.0
            notes.append("crowded shorts")

    if dist_high_pct is not None and dist_high_pct < 1.0:
        agreeing += 1.0

    if vol_spike > 1.5:
        agreeing += 1.0

    if patterns:
        agreeing += 1.0

    net = agreeing - conflicts
    if net >= 4.0 and score >= 60:
        return "HIGH", Fore.GREEN, notes
    if net >= 2.0 and score >= 40:
        return "MEDIUM", Fore.YELLOW, notes
    return "LOW", Fore.RED, notes

def score_short(data: TickerData) -> Tuple[int, List[str]]:
    """
    Aggregate a score for a SHORT setup.
    All signals are short-biased.
    """
    score = 0
    signals: List[str] = []

    if data.ema_slope is not None:
        if data.ema_slope < 0.0:
            score += 12
            signals.append(f"Negative EMA Slope ({data.ema_slope:.3f}) — Downtrend confirmed")
        elif data.slope_change is not None and data.slope_change < -0.01:
            score += 8
            signals.append(f"EMA Curling Down (Slope Δ {data.slope_change:.3f}) — Momentum loss")
        elif data.ema_slope > 0.0 and data.slope_change is not None and data.slope_change < -0.02:
            score += 5
            signals.append(f"EMA Slope Flattening (Δ {data.slope_change:.3f}) — Uptrend slowing")

    if data.news_count > 0:
        signals.append(f"NEWS: {data.news_count} recent items (Proceed with caution)")

    if data.dist_to_node_above is not None:
        if data.dist_to_node_above < 0.5:
            score += 15
            signals.append(f"Near High-Vol Resistance Node ({data.dist_to_node_above:.2f}% below)")
        elif data.dist_to_node_above < 1.0:
            score += 8
            signals.append(f"Approaching Resistance Node ({data.dist_to_node_above:.2f}% below)")

    if data.spread is not None:
        if data.spread > 0.15:
            score -= 10
            signals.append(f"Low Liquidity (Spread {data.spread:.2f}%)")
        elif data.spread < 0.05:
            score += 5
            signals.append(f"High Liquidity (Spread {data.spread:.2f}%)")

    if data.fr_change is not None and data.fr_change > 0.0:
        score += WEIGHTS["funding_momentum"]
        signals.append(f"Funding Momentum (becoming more positive +{data.fr_change*100:.4f}% — fade building)")

    if data.rsi_1h is not None:
        if data.rsi_1h > 65.0:
            score += WEIGHTS["htf_align_overbought"]
            signals.append(f"HTF Alignment (1H RSI {data.rsi_1h:.1f}) — deeply overbought")
        elif data.rsi_1h > 55.0:
            score += 8
            signals.append(f"HTF Alignment (1H RSI {data.rsi_1h:.1f}) — overbought territory")

    if data.has_div:
        score += WEIGHTS["divergence"]
        signals.append("Bearish Divergence (Price HH vs RSI LH) — buyers exhausted")

    if data.rsi is not None:
        rolling_over = (data.prev_rsi is not None) and (data.rsi < data.prev_rsi)

        if data.rsi > 75.0:
            score += WEIGHTS["rsi_overbought"]
            signals.append(f"RSI {data.rsi:.1f} (extremely overbought — high-risk/high-reward)")
        elif 55.0 <= data.rsi <= 75.0:
            pts = WEIGHTS["rsi_rollover"]
            label = f"RSI {data.rsi:.1f} (rollover zone)"
            if rolling_over:
                pts += 8
                label += " ✓ rolling over"
            score += pts
            signals.append(label)
        elif 45.0 <= data.rsi < 55.0:
            score += 0   # neutral is not a signal
            signals.append(f"RSI {data.rsi:.1f} (neutral)")
        elif data.rsi < 35.0:
            score -= 5
            signals.append(f"RSI {data.rsi:.1f} (oversold — risky short entry)")

    if data.bb is not None:
        bb_range = data.bb["upper"] - data.bb["lower"]
        bb_pct = ((data.price - data.bb["lower"]) / bb_range) if bb_range > 0.0 else 0.5

        if bb_pct >= 0.90:
            score += WEIGHTS["bb_upper_90"]
            signals.append(f"Price above/at BB upper band ({bb_pct*100:.0f}%) — extreme overbought")
        elif bb_pct >= 0.75:
            score += WEIGHTS["bb_upper_75"]
            signals.append(f"Near BB upper band ({bb_pct*100:.0f}%) — overbought")
        elif bb_pct >= 0.55:
            score += 5  # Reduced from 14
            signals.append(f"Above BB mid ({bb_pct*100:.0f}%)")
        elif bb_pct <= 0.45:
            score += 0  # Reduced from 8
            signals.append(f"Below BB mid ({bb_pct*100:.0f}%)")
        else:
            score -= 5
            signals.append(f"Below BB mid — fading short ({bb_pct*100:.0f}%)")

    if data.ema21 is not None and data.price is not None:
        pct_from_ema = pc.pct_change(data.price, data.ema21)
        if pct_from_ema > 3.0:
            score += WEIGHTS["ema_stretch_3"]
            signals.append(f"Price {pct_from_ema:.1f}% above EMA21 (mean-reversion opportunity)")
            if data.rsi and data.rsi > 65.0:
                score += 5
                signals.append("Stretch bonus: Deeply overbought RSI + Above EMA21")
        elif pct_from_ema > 1.0:
            score += 5  # Reduced from 8
            signals.append(f"Price {pct_from_ema:.1f}% above EMA21")
        elif pct_from_ema < -1.0:
            score -= 10 # More aggressive penalty
            signals.append(f"Price {abs(pct_from_ema):.1f}% below EMA21 (extended)")

    if data.change_24h is not None:
        if -10.0 <= data.change_24h <= -3.0:
            score += 12 # Reduced from 18
            signals.append(f"{data.change_24h:.1f}% (bearish momentum)")
        elif data.change_24h < -10.0:
            score += 0 # Reduced from 6
            signals.append(f"{data.change_24h:.1f}% (very oversold)")
        elif data.change_24h > 12.0:
            score += 20 # Reduced from 22
            signals.append(f"+{data.change_24h:.1f}% pump (overbought fade)")
        elif 5.0 <= data.change_24h <= 12.0:
            score += 12 # Reduced from 15
            signals.append(f"+{data.change_24h:.1f}% rally (fade opportunity)")
        elif 2.0 < data.change_24h < 5.0:
            score += 5
            signals.append(f"+{data.change_24h:.1f}% small rally (fade entry)")
        else:
            signals.append(f"{data.change_24h:+.1f}% (neutral)")

    if data.dist_high_pct is not None:
        if data.dist_high_pct < 1.0:
            score += 12
            signals.append(f"Near 24h High ({data.dist_high_pct:.1f}% distance) — supply zone")
        elif data.dist_high_pct < 2.0:
            score += 6
            signals.append(f"Close to 24h High ({data.dist_high_pct:.1f}% distance)")

    if data.vol_spike > 2.0:
        score += WEIGHTS["vol_spike_2"]
        signals.append(f"Volume spike ({data.vol_spike:.1f}x average) — capitulation / accumulation")
    elif data.vol_spike > 1.4:
        score += 7
        signals.append(f"Elevated volume ({data.vol_spike:.1f}x average)")

    if data.funding_rate is not None:
        fr_pct = data.funding_rate * 100.0
        if fr_pct > 0.10:
            score += WEIGHTS["funding_high"]
            signals.append(f"Funding +{fr_pct:.4f}% (heavily crowded longs — fade primed)")
        elif fr_pct > 0.05:
            score += 16
            signals.append(f"Funding +{fr_pct:.4f}% (crowded longs)")
        elif fr_pct > 0.01:
            score += 8
            signals.append(f"Funding +{fr_pct:.4f}% (mild long bias)")
        elif fr_pct < -0.05:
            score -= 12
            signals.append(f"Funding {fr_pct:.4f}% (crowded shorts — risky short entry)")

    for name, bonus, quality in data.patterns:
        q = float(quality) if isinstance(quality, (int, float)) else 1.0
        weighted_bonus = int(bonus * q)
        score += weighted_bonus
        q_label = f" (x{q:.1f} Quality)" if abs(q - 1.0) > 1e-6 else ""
        signals.append(f"Pattern: {name} (+{weighted_bonus}{q_label})")

    # Return raw score to reflect multiple penalties accurately
    return int(round(score)), signals

# ----------------------------
# Proxies
# ----------------------------
def get_tickers(rps: float = None) -> List[dict]:
    return pc.get_tickers(rps)

def get_candles(symbol: str, timeframe: str = "15m", limit: int = 100, rps: float = None) -> List[List[Any]]:
    return pc.get_candles(symbol, timeframe, limit, rps)

prefetch_all_funding_rates = pc.prefetch_all_funding_rates

# ----------------------------
# Main Analysis
# ----------------------------
def analyse(ticker: dict, cfg: dict, enable_ai: bool = True, enable_entity: bool = True,
            scan_id: Optional[str] = None) -> Optional[dict]:
    """
    Analyse a single Phemex USDT-M perpetual ticker for SHORT setups.
    """
    symbol = ticker.get("symbol")
    if not symbol:
        return None

    # News (not currently used by default)
    news_count = 0
    news_titles = []

    logger.debug(f"Analysing {symbol}...")
    start_analyse = time.time()

    try:
        last = float(ticker.get("lastRp") or ticker.get("closeRp") or 0.0)
        open24 = float(ticker.get("openRp") or last)
        high24 = float(ticker.get("highRp") or last)
        vol24 = float(ticker.get("turnoverRv") or 0.0)

        if vol24 < cfg["MIN_VOLUME"]:
            logger.debug(f"  {symbol}: Volume {pc.fmt_vol(vol24)} below min volume, skipping.")
            return None

        # Move funding check here to avoid expensive kline fetching and indicators
        fr, prev_fr, fr_change = pc.get_funding_rate_info(symbol, rps=cfg.get("RATE_LIMIT_RPS"))
        if fr is None:
            fr_raw = ticker.get("fundingRateRr")
            fr = float(fr_raw) if fr_raw is not None else None
            fr_change = 0.0
            
        if fr is not None and (fr * 100.0) < (MIN_NEGATIVE_FUNDING * 100.0):
            logger.debug(f"  {symbol}: Funding rate too negative ({fr*100:.4f}%), skipping.")
            return None

        if last == 0.0:
            logger.debug(f"  {symbol}: Last price is 0, skipping.")
            return None

        # Distance from 24h high (short analog: how close to the high is the current price)
        dist_high_pct = pc.pct_change(last, high24) if high24 > 0.0 else None
        change_24h = pc.pct_change(last, open24) if open24 > 0.0 else None

        # Fetch klines
        start_klines = time.time()
        candles = pc.get_candles(symbol, timeframe=cfg["TIMEFRAME"], limit=cfg.get("CANDLES", 100), rps=cfg.get("RATE_LIMIT_RPS"))
        logger.debug(f"  {symbol}: Klines fetched in {time.time() - start_klines:.4f}s")
        if not candles:
            logger.debug(f"  {symbol}: No candles, skipping.")
            return None

        # Phemex kline row: [timestamp, interval, last_close, open, high, low, close, volume, turnover]
        ohlc = []
        highs = []
        lows = []
        closes = []
        vols = []
        for c in candles:
            try:
                o = float(c[3]); h = float(c[4]); l = float(c[5]); cl = float(c[6])
                v = float(c[7]) if len(c) > 7 else 0.0
            except Exception:
                continue
            ohlc.append((o, h, l, cl))
            highs.append(h); lows.append(l); closes.append(cl); vols.append(v)

        if not closes:
            logger.debug(f"  {symbol}: No valid closes, skipping.")
            return None

        # Indicators
        start_indicators = time.time()
        rsi, prev_rsi, rsi_hist = pc.calc_rsi(closes)
        bb = pc.calc_bb(closes)
        
        ema_series = pc.calc_ema_series(closes, 21)
        ema21 = ema_series[-1] if ema_series else None
        ema_slope, slope_change = pc.calc_ema_slope(ema_series)
        logger.debug(f"  {symbol}: Indicators calculated in {time.time() - start_indicators:.4f}s")

        start_atr = time.time()
        atr = pc.calc_atr(highs, lows, closes)
        logger.debug(f"  {symbol}: ATR calculated in {time.time() - start_atr:.4f}s")

        # News (not currently used by default)
        news_count = 0
        news_titles = []

        # --- PRE-SCORE GATE ---
        # Calculate a partial score to see if it's worth fetching 1H candles and Orderbook
        # This reduces API calls by ~40% for low-potential symbols.
        pre_data = TickerData(
            inst_id=symbol, price=last, rsi=rsi, prev_rsi=prev_rsi, bb=bb, ema21=ema21,
            change_24h=change_24h, funding_rate=fr, patterns=[], dist_high_pct=dist_high_pct,
            vol_spike=pc.calc_volume_spike(vols), has_div=detect_bearish_divergence(closes, rsi_hist),
            rsi_1h=None, fr_change=fr_change or 0.0, spread=0.0, dist_to_node_above=None,
            ema_slope=ema_slope, slope_change=slope_change, news_count=0, news_titles=[],
            raw_ohlc=ohlc[-10:], vol_24h=vol24
        )
        pre_score, _ = score_short(pre_data)
        
        # If pre-score is too low, don't bother with expensive calls
        PRE_SCORE_THRESHOLD = 60
        
        rsi_1h = None
        best_bid, best_ask, spread, depth = None, None, 0.0, 0.0
        dist_to_node_above = None
        poc_price, nodes = None, []

        if pre_score >= PRE_SCORE_THRESHOLD:
            logger.debug(f"  {symbol}: Pre-score {pre_score} >= {PRE_SCORE_THRESHOLD}, fetching full data...")

            start_orderbook = time.time()
            best_bid, best_ask, spread, depth = pc.get_order_book(symbol, rps=cfg.get("RATE_LIMIT_RPS"))
            logger.debug(f"  {symbol}: Order book fetched in {time.time() - start_orderbook:.4f}s")

            # Volume profile — look for nearest HIGH-VOLUME node ABOVE price (resistance)
            start_vol_profile = time.time()
            poc_price, nodes = pc.calc_volume_profile(ohlc, vols, bins=20)
            logger.debug(f"  {symbol}: Volume profile calculated in {time.time() - start_vol_profile:.4f}s")
            nodes_above = [n for n in nodes if n > last] if nodes else []
            if nodes_above and last > 0.0:
                nearest_node = min(nodes_above)   
                dist_to_node_above = abs(pc.pct_change(last, nearest_node))

            # 1H RSI
            start_1h_rsi = time.time()
            candles_1h = pc.get_candles(symbol, timeframe="1H", limit=50, rps=cfg.get("RATE_LIMIT_RPS"))
            logger.debug(f"  {symbol}: 1H candles fetched in {time.time() - start_1h_rsi:.4f}s")
            if candles_1h:
                closes_1h = []
                for c in candles_1h:
                    try:
                        closes_1h.append(float(c[6]))
                    except Exception:
                        continue
                if closes_1h:
                    rsi_1h, _, _ = pc.calc_rsi(closes_1h)
        else:
            logger.debug(f"  {symbol}: Pre-score {pre_score} < {PRE_SCORE_THRESHOLD}, skipping expensive calls.")

        start_patterns = time.time()
        vol_spike = pc.calc_volume_spike(vols)
        has_div = detect_bearish_divergence(closes, rsi_hist)
        raw_patterns = detect_patterns(ohlc)
        logger.debug(f"  {symbol}: Patterns detected in {time.time() - start_patterns:.4f}s")

        patterns: List[Tuple[str, int, float]] = []
        for entry in raw_patterns:
            if len(entry) == 3:
                name, bonus, quality = entry
            else:
                name, bonus, quality = entry[0], entry[1], entry[-1]
            if name.startswith("Three Black Crows") and rsi is not None and rsi < THREE_BLACK_CROWS_RSI_GATE:
                continue
            q = float(quality) if isinstance(quality, (int, float)) else 1.0
            patterns.append((name, int(bonus), q))

        data = TickerData(
            inst_id=symbol,
            price=last,
            rsi=rsi,
            prev_rsi=prev_rsi,
            bb=bb,
            ema21=ema21,
            change_24h=change_24h,
            funding_rate=fr,
            patterns=patterns,
            dist_high_pct=dist_high_pct,
            vol_spike=vol_spike,
            has_div=has_div,
            rsi_1h=rsi_1h,
            fr_change=fr_change if fr_change is not None else 0.0,
            spread=spread,
            dist_to_node_above=dist_to_node_above,
            ema_slope=ema_slope,
            slope_change=slope_change,
            news_count=news_count,
            news_titles=news_titles,
            raw_ohlc=ohlc[-10:],
            vol_24h=vol24
        )

        score, signals = score_short(data)

        bb_pct = None
        if bb:
            bb_range = bb["upper"] - bb["lower"]
            if bb_range > 0.0:
                bb_pct = (last - bb["lower"]) / bb_range * 100.0

        confidence, conf_color, conf_notes = calc_confidence(
            rsi, bb_pct, ema21, last, change_24h, fr, patterns, score, dist_high_pct, vol_spike
        )

        stop_pct = (0.5 * atr / last * 100.0) if (atr and last and last > 0.0) else None

        result = {
            "inst_id": symbol,
            "price": last,
            "change_24h": change_24h,
            "vol_24h": vol24,
            "rsi": rsi,
            "prev_rsi": prev_rsi,
            "bb_pct": bb_pct,
            "ema21": ema21,
            "funding_pct": fr * 100.0 if fr is not None else None,
            "score": score,
            "signals": signals,
            "patterns": patterns,
            "confidence": confidence,
            "conf_color": conf_color,
            "conf_notes": conf_notes,
            "dist_high": dist_high_pct,
            "vol_spike": vol_spike,
            "bb_width": bb["width_pct"] if bb else 0.0,
            "atr_stop_pct": stop_pct,
            "news_count": news_count,
            "news_titles": news_titles,
            "raw_ohlc": ohlc[-10:],
            "spread": spread,
            "dist_to_node_above": dist_to_node_above,
            "ema_slope": ema_slope,
            "slope_change": slope_change,
            "fr_change": fr_change,
            "rsi_1h": rsi_1h,
            "scan_timestamp": datetime.datetime.now().isoformat(),
            "entity_id": None,
        }

        if enable_entity and pc.ENTITY_API_KEY:
            res_obj = pc.make_entity_request("ScanResult", method="POST", data={
                "scan_id": scan_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "inst_id": symbol,
                "price": last,
                "change_24h": change_24h or 0.0,
                "rsi": rsi or 50.0,
                "funding_rate": round(fr * 100, 8) if fr is not None else 0.0,
                "score": score,
                "signals": signals,
                "confidence": confidence,
                "atr_stop_pct": stop_pct or 0.0,
                "vol_spike": vol_spike or 0.0,
                "spread": spread or 0.0,
                "direction": "Short"
            })
            if res_obj and isinstance(res_obj, dict):
                result["entity_id"] = res_obj.get("id")

        return result

    except requests.RequestException as e:
        logger.debug("Request error analyzing %s: %s", symbol, e)
        return None
    except ValueError as e:
        logger.debug("Value error analyzing %s: %s", symbol, e)
        return None
    except Exception as e:
        logger.error("Unexpected error analyzing %s: %s", symbol, e)
        return None