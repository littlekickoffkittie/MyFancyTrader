#!/usr/bin/env python3
"""
Phemex Long Setup Scanner — USDT-M Perpetuals
----------------------------------------------
Mirror of phemex_short.py with all signals inverted for long setups.

Long bias logic:
  - RSI oversold / bouncing from lows (< 35 / 25-45 recovery zone)
  - Price at/below BB lower band
  - Price below EMA21 (mean reversion) or EMA21 turning up (trend continuation)
  - Negative funding (crowded shorts → squeeze fuel)
  - Near 24h LOW (not high)
  - Bullish candle patterns (hammer, morning star, engulfing, etc.)
  - Bullish divergence (price makes lower low, RSI makes higher low)
  - Drop + bounce setups, capitulation candles
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

# Strategy thresholds — long-biased
MAX_POSITIVE_FUNDING = 0.02        # skip if funding too positive (crowded longs = bad for longs)
THREE_WHITE_SOLDIERS_RSI_GATE = 55 # three white soldiers only valid if RSI not already overbought
DIVERGENCE_WINDOW = 60
DIV_PRICE_THRESHOLD = 0.995        # 0.5% lower low
DIV_RSI_THRESHOLD = 3.0            # 3.0 RSI points higher low
RSI_OVERSOLD_ZONE = 35.0
RSI_OVERBOUGHT_ZONE = 65.0

# Score weights (long-biased)
WEIGHTS = {
    "divergence": 20,
    "rsi_recovery": 25,        # RSI bouncing from oversold
    "rsi_oversold": 22,        # deep oversold reading
    "bb_lower_90": 30,         # price below/at BB lower
    "bb_lower_75": 22,         # price near BB lower
    "ema_stretch_3": 15,       # price significantly below EMA21 (mean reversion)
    "vol_spike_2": 15,         # volume spike (capitulation or demand)
    "funding_negative": 22,    # negative funding = crowded shorts = squeeze fuel
    "htf_align_oversold": 15,  # 1H RSI oversold confirms LTF long
    "funding_momentum": 10,    # funding becoming more negative (building squeeze)
}

TRADE_LOG_FILE = pc.os.path.dirname(pc.os.path.abspath(__file__)) + "/trade_log_long.json"
SCAN_OUTPUT_FILE = pc.os.path.dirname(pc.os.path.abspath(__file__)) + "/last_scan_long.json"

logger = logging.getLogger("phemex_long_scanner")
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
    dist_low_pct: Optional[float]          # distance from 24h LOW (long analog of dist_high)
    vol_spike: float
    has_div: bool                           # bullish divergence
    rsi_1h: Optional[float]
    fr_change: float = 0.0
    spread: Optional[float] = None
    dist_to_node_below: Optional[float] = None   # distance to nearest high-vol support node
    ema_slope: Optional[float] = None
    slope_change: Optional[float] = None
    news_count: int = 0
    news_titles: List[str] = field(default_factory=list)
    raw_ohlc: List[Tuple[float, float, float, float]] = field(default_factory=list)
    vol_24h: float = 0.0

# ----------------------------
# Indicator Logic
# ----------------------------
def find_troughs(values: List[float], min_separation: int = 3) -> List[int]:
    """Local trough finder (mirror of find_peaks for bullish divergence)."""
    troughs: List[int] = []
    n = len(values)
    if n < 3:
        return troughs
    for i in range(1, n - 1):
        if values[i] < values[i - 1] and values[i] < values[i + 1]:
            if not troughs or (i - troughs[-1] >= min_separation):
                troughs.append(i)
    return troughs

def detect_bullish_divergence(closes: List[float], rsi_values: List[Optional[float]]) -> bool:
    """
    Bullish divergence: price makes a lower low while RSI makes a higher low.
    This signals exhaustion of sellers and a likely reversal upward.
    """
    if len(closes) < DIVERGENCE_WINDOW or len(rsi_values) < DIVERGENCE_WINDOW:
        return False
    price_window = pc.np.asarray(closes[-DIVERGENCE_WINDOW:], dtype=float)
    rsi_window_list = rsi_values[-DIVERGENCE_WINDOW:]
    if any(v is None for v in rsi_window_list):
        return False
    rsi_window = pc.np.asarray([float(v) for v in rsi_window_list], dtype=float)

    price_troughs = find_troughs(price_window.tolist())
    rsi_troughs = find_troughs(rsi_window.tolist())

    if len(price_troughs) < 2 or len(rsi_troughs) < 2:
        return False

    # Time-alignment check — ensure price and RSI troughs occur within 5 candles of each other
    if abs(price_troughs[-1] - rsi_troughs[-1]) > 5 or abs(price_troughs[-2] - rsi_troughs[-2]) > 5:
        return False

    p1 = price_window[price_troughs[-2]]
    p2 = price_window[price_troughs[-1]]
    r1 = rsi_window[rsi_troughs[-2]]
    r2 = rsi_window[rsi_troughs[-1]]

    # p2 meaningfully lower, r2 meaningfully higher → bullish divergence
    # Apply stricter thresholds and ensure second RSI trough is in oversold zone
    return (p2 < p1 * DIV_PRICE_THRESHOLD) and (r2 > r1 + DIV_RSI_THRESHOLD) and (r2 < RSI_OVERSOLD_ZONE + 10)

def detect_patterns(ohlc: List[Tuple[float, float, float, float]]) -> List:
    """Detect bullish reversal / continuation candle patterns."""
    patterns = []
    if len(ohlc) < 3:
        return patterns

    def body(c): return abs(c[3] - c[0])
    def upper_wick(c): return c[1] - max(c[0], c[3])
    def lower_wick(c): return min(c[0], c[3]) - c[2]
    def is_bear(c): return c[3] < c[0]
    def is_bull(c): return c[3] > c[0]

    c0, c1, c2 = ohlc[-3], ohlc[-2], ohlc[-1]

    # Hammer — long lower wick, small body, at low area (bullish reversal)
    if (lower_wick(c2) > 2 * body(c2)
            and upper_wick(c2) < body(c2) * 0.4
            and body(c2) > 0):
        patterns.append(("Hammer 🔨", 15, 1.0))

    # Inverted Hammer / Dragonfly Doji at low (demand spike)
    if (lower_wick(c2) > 2.5 * body(c2)
            and body(c2) < (c2[1] - c2[2]) * 0.2
            and c2[2] < c1[2]):
        patterns.append(("Dragonfly Doji / Inv Hammer 🐉", 14, 1.0))

    # Bullish Engulfing — bear candle followed by larger bull candle
    if (is_bear(c1) and is_bull(c2)
            and c2[0] <= c1[3] and c2[3] >= c1[0]
            and body(c2) > body(c1)):
        patterns.append(("Bullish Engulfing 🟢", 18, 1.0))

    # Morning Star — bear, small body (indecision), bull (3-candle reversal)
    if (is_bear(c0)
            and body(c1) < body(c0) * 0.5
            and is_bull(c2)
            and c2[3] > (c0[0] + c0[3]) / 2):
        patterns.append(("Morning Star ⭐", 20, 1.0))

    # Piercing Line — bear candle, bull opens below low, closes above midpoint
    if (is_bear(c1) and is_bull(c2)
            and c2[0] < c1[2]
            and c2[3] > (c1[0] + c1[3]) / 2
            and c2[3] < c1[0]):
        patterns.append(("Piercing Line 💉", 16, 1.0))

    # Bullish Harami — bear followed by smaller bull inside it
    if (is_bear(c1) and is_bull(c2)
            and c2[0] > c1[3] and c2[3] < c1[0]
            and body(c2) < body(c1)):
        patterns.append(("Bullish Harami 🟩", 12, 1.0))

    # Doji at Low — indecision after downtrend (reversal warning)
    if body(c2) < (c2[1] - c2[2]) * 0.15 and c2[2] < c1[2]:
        patterns.append(("Doji at Low — Reversal Watch 🔄", 10, 1.0))

    # Three White Soldiers (gate applied later — RSI must not be overbought)
    if (is_bull(c0) and is_bull(c1) and is_bull(c2)
            and c1[3] > c0[3] and c2[3] > c1[3]
            and body(c0) > 0 and body(c1) > 0 and body(c2) > 0):
        patterns.append(("Three White Soldiers 🪖", 18, 1.0))

    # Bullish Marubozu — strong bull with almost no wicks (momentum)
    if (is_bull(c2)
            and upper_wick(c2) < body(c2) * 0.1
            and lower_wick(c2) < body(c2) * 0.1
            and body(c2) > (c2[1] - c2[2]) * 0.85):
        patterns.append(("Bullish Marubozu 💪", 14, 1.0))

    return patterns

# ----------------------------
# Confidence & Scoring — LONG BIASED
# ----------------------------
def calc_confidence(rsi, bb_pct, ema21, price, change_24h, funding_rate, patterns, score, dist_low_pct, vol_spike):
    """
    Long-biased confidence: counts bullish agreeing signals vs bearish conflicts.
    """
    agreeing = 0.0
    conflicts = 0.0
    notes: List[str] = []

    # RSI — oversold is bullish, overbought is conflict for longs
    if rsi is not None:
        if rsi < 45.0:
            agreeing += 1.0
        elif rsi > 65.0:
            conflicts += 1.0
            notes.append("RSI overbought — late entry risk")

    # BB position — at/below lower is bullish, above upper is conflict
    if bb_pct is not None:
        if bb_pct <= 35.0:
            agreeing += 1.0
        elif bb_pct > 70.0:
            conflicts += 1.0
            notes.append("price above BB 70%")

    # EMA distance — below EMA is mean-reversion fuel
    if ema21 is not None and price is not None:
        pct = pc.pct_change(price, ema21)
        if pct < -1.0:
            agreeing += 1.0        # price meaningfully below EMA21
        elif pct > 2.0:
            conflicts += 0.5       # already stretched above EMA21

    # 24h change — a significant drop is oversold fuel; extreme dump is risky
    if change_24h is not None:
        if -15.0 <= change_24h <= -3.0:
            agreeing += 1.0
        elif change_24h < -15.0:
            agreeing += 0.5        # capitulation possible but risky
        elif change_24h > 5.0:
            conflicts += 1.0
            notes.append("pumping already")
        elif -0.5 < change_24h < 0.5:
            conflicts += 0.5
            notes.append("flat — no momentum")

    # Funding — negative funding is bullish (shorts crowded = squeeze)
    if funding_rate is not None:
        fr_pct = funding_rate * 100.0
        if fr_pct < -0.01:
            agreeing += 1.0
        elif fr_pct > 0.05:
            conflicts += 2.0
            notes.append("crowded longs")

    # Near 24h low
    if dist_low_pct is not None and dist_low_pct < 1.0:
        agreeing += 1.0

    # Volume spike (capitulation buying or short squeeze)
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

def score_long(data: TickerData) -> Tuple[int, List[str]]:
    """
    Aggregate a score for a LONG setup.
    All signals are long-biased (inverse of the short scanner).
    """
    score = 0
    signals: List[str] = []

    # --- EMA slope ---
    if data.ema_slope is not None:
        if data.ema_slope > 0.0:
            score += 12
            signals.append(f"Positive EMA Slope ({data.ema_slope:.3f}) — Uptrend confirmed")
        elif data.slope_change is not None and data.slope_change > 0.01:
            score += 8
            signals.append(f"EMA Curling Up (Slope Δ +{data.slope_change:.3f}) — Momentum building")
        elif data.ema_slope < 0.0 and data.slope_change is not None and data.slope_change > 0.02:
            score += 5
            signals.append(f"EMA Slope Flattening (Δ +{data.slope_change:.3f}) — Downtrend slowing")

    # --- News ---
    if data.news_count > 0:
        signals.append(f"NEWS: {data.news_count} recent items (Proceed with caution)")

    # --- Volume profile support node ---
    if data.dist_to_node_below is not None:
        if data.dist_to_node_below < 0.5:
            score += 15
            signals.append(f"Near High-Vol Support Node ({data.dist_to_node_below:.2f}% above)")
        elif data.dist_to_node_below < 1.0:
            score += 8
            signals.append(f"Approaching Support Node ({data.dist_to_node_below:.2f}% above)")

    # --- Spread / liquidity ---
    if data.spread is not None:
        if data.spread > 0.15:
            score -= 10
            signals.append(f"Low Liquidity (Spread {data.spread:.2f}%)")
        elif data.spread < 0.05:
            score += 5
            signals.append(f"High Liquidity (Spread {data.spread:.2f}%)")

    # --- Funding momentum (becoming more negative = more squeeze fuel) ---
    if data.fr_change is not None and data.fr_change < 0.0:
        score += WEIGHTS["funding_momentum"]
        signals.append(f"Funding Momentum (becoming more negative {data.fr_change*100:.4f}% — squeeze building)")

    # --- 1H RSI alignment ---
    if data.rsi_1h is not None:
        if data.rsi_1h < 35.0:
            score += WEIGHTS["htf_align_oversold"]
            signals.append(f"HTF Alignment (1H RSI {data.rsi_1h:.1f}) — deeply oversold")
        elif data.rsi_1h < 45.0:
            score += 8
            signals.append(f"HTF Alignment (1H RSI {data.rsi_1h:.1f}) — oversold territory")

    # --- Bullish divergence ---
    if data.has_div:
        score += WEIGHTS["divergence"]
        signals.append("Bullish Divergence (Price LL vs RSI HL) — sellers exhausted")

    # --- RSI scoring bands ---
    if data.rsi is not None:
        recovering = (data.prev_rsi is not None) and (data.rsi > data.prev_rsi)

        if data.rsi < 25.0:
            score += WEIGHTS["rsi_oversold"]
            signals.append(f"RSI {data.rsi:.1f} (extremely oversold — high-risk/high-reward)")
        elif 25.0 <= data.rsi <= 45.0:
            pts = WEIGHTS["rsi_recovery"]
            label = f"RSI {data.rsi:.1f} (oversold recovery zone)"
            if recovering:
                pts += 8
                label += " ✓ turning up"
            score += pts
            signals.append(label)
        elif 55.0 < data.rsi <= 65.0:
            score += 2
            signals.append(f"RSI {data.rsi:.1f} (mildly elevated)")
        elif data.rsi > 65.0:
            score -= 5
            signals.append(f"RSI {data.rsi:.1f} (overbought — risky long entry)")

    # --- Bollinger Band position ---
    if data.bb is not None:
        bb_range = data.bb["upper"] - data.bb["lower"]
        bb_pct = ((data.price - data.bb["lower"]) / bb_range) if bb_range > 0.0 else 0.5

        if bb_pct <= 0.10:
            score += WEIGHTS["bb_lower_90"]
            signals.append(f"Price below/at BB lower band ({bb_pct*100:.0f}%) — extreme oversold")
        elif bb_pct <= 0.25:
            score += WEIGHTS["bb_lower_75"]
            signals.append(f"Near BB lower band ({bb_pct*100:.0f}%) — oversold")
        elif bb_pct <= 0.45:
            score += 5  # Fixed to +5
            signals.append(f"Below BB mid ({bb_pct*100:.0f}%)")
        elif bb_pct <= 0.55:
            score += 0  # Fixed to 0
            signals.append(f"At BB mid ({bb_pct*100:.0f}%)")
        else:
            score -= 5
            signals.append(f"Above BB mid — fading long ({bb_pct*100:.0f}%)")

    # --- EMA21 distance (mean reversion) ---
    if data.ema21 is not None and data.price is not None:
        pct_from_ema = pc.pct_change(data.price, data.ema21)
        if pct_from_ema < -3.0:
            score += WEIGHTS["ema_stretch_3"]
            signals.append(f"Price {abs(pct_from_ema):.1f}% below EMA21 (mean-reversion opportunity)")
            if data.rsi and data.rsi < 35.0:
                score += 5
                signals.append("Stretch bonus: Deeply oversold RSI + Below EMA21")
        elif pct_from_ema < -1.0:
            score += 5  
            signals.append(f"Price {abs(pct_from_ema):.1f}% below EMA21")
        elif pct_from_ema > 1.0:
            score -= 10 
            signals.append(f"Price {pct_from_ema:.1f}% above EMA21 (extended)")

    # --- 24h change scoring ---
    if data.change_24h is not None:
        if 3.0 <= data.change_24h <= 10.0:
            score += 12 
            signals.append(f"+{data.change_24h:.1f}% (bullish momentum)")
        elif data.change_24h > 10.0:
            score += 0 
            signals.append(f"+{data.change_24h:.1f}% (overextended)")
        elif data.change_24h < -12.0:
            score += 20 
            signals.append(f"{data.change_24h:.1f}% crash (capitulation)")
        elif -12.0 <= data.change_24h <= -5.0:
            score += 12 
            signals.append(f"{data.change_24h:.1f}% dip (oversold bounce)")
        elif -5.0 < data.change_24h < -2.0:
            score += 5  # Fixed to +5
            signals.append(f"{data.change_24h:.1f}% pullback (controlled dip buy)")
        else:
            signals.append(f"{data.change_24h:+.1f}% (neutral)")

    # --- Distance from 24h LOW ---
    if data.dist_low_pct is not None:
        if data.dist_low_pct < 1.0:
            score += 12
            signals.append(f"Near 24h Low ({data.dist_low_pct:.1f}% distance) — demand zone")
        elif data.dist_low_pct < 2.0:
            score += 6
            signals.append(f"Close to 24h Low ({data.dist_low_pct:.1f}% distance)")

    # --- Volume spike (capitulation or institutional accumulation) ---
    if data.vol_spike > 2.0:
        score += WEIGHTS["vol_spike_2"]
        signals.append(f"Volume spike ({data.vol_spike:.1f}x average) — capitulation / accumulation")
    elif data.vol_spike > 1.4:
        score += 7
        signals.append(f"Elevated volume ({data.vol_spike:.1f}x average)")

    # --- Funding rate scoring ---
    if data.funding_rate is not None:
        fr_pct = data.funding_rate * 100.0
        if fr_pct < -0.10:
            score += WEIGHTS["funding_negative"]
            signals.append(f"Funding {fr_pct:.4f}% (heavily crowded shorts — squeeze primed)")
        elif fr_pct < -0.05:
            score += 16
            signals.append(f"Funding {fr_pct:.4f}% (crowded shorts)")
        elif fr_pct < -0.01:
            score += 8
            signals.append(f"Funding {fr_pct:.4f}% (mild short bias)")
        elif fr_pct > 0.05:
            score -= 12
            signals.append(f"Funding +{fr_pct:.4f}% (crowded longs — risky entry)")

    # --- Candle patterns ---
    for name, bonus, quality in data.patterns:
        q = float(quality) if isinstance(quality, (int, float)) else 1.0
        weighted_bonus = int(bonus * q)
        score += weighted_bonus
        q_label = f" (x{q:.1f} Quality)" if abs(q - 1.0) > 1e-6 else ""
        signals.append(f"Pattern: {name} (+{weighted_bonus}{q_label})")

    # Return the raw score unclamped to accurately reflect multiple penalties
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
    Analyse a single Phemex USDT-M perpetual ticker for LONG setups.
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
        low24 = float(ticker.get("lowRp") or last)
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
            
        if fr is not None and (fr * 100.0) > (MAX_POSITIVE_FUNDING * 100.0):
            logger.debug(f"  {symbol}: Funding rate too positive ({fr*100:.4f}%), skipping.")
            return None

        if last == 0.0:
            logger.debug(f"  {symbol}: Last price is 0, skipping.")
            return None

        # Distance from 24h low (long analog: how close to the low is the current price)
        dist_low_pct = pc.pct_change(last, low24) if low24 > 0.0 else None
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
            change_24h=change_24h, funding_rate=fr, patterns=[], dist_low_pct=dist_low_pct,
            vol_spike=pc.calc_volume_spike(vols), has_div=detect_bullish_divergence(closes, rsi_hist),
            rsi_1h=None, fr_change=fr_change or 0.0, spread=0.0, dist_to_node_below=None,
            ema_slope=ema_slope, slope_change=slope_change, news_count=0, news_titles=[],
            raw_ohlc=ohlc[-10:], vol_24h=vol24
        )
        pre_score, _ = score_long(pre_data)
        
        # If pre-score is too low, don't bother with expensive calls
        # Threshold 60 is conservative (Min trade score is usually 120-130)
        PRE_SCORE_THRESHOLD = 60
        
        rsi_1h = None
        best_bid, best_ask, spread, depth = None, None, 0.0, 0.0
        dist_to_node_below = None
        poc_price, nodes = None, []

        if pre_score >= PRE_SCORE_THRESHOLD:
            logger.debug(f"  {symbol}: Pre-score {pre_score} >= {PRE_SCORE_THRESHOLD}, fetching full data...")
            
            start_orderbook = time.time()
            best_bid, best_ask, spread, depth = pc.get_order_book(symbol, rps=cfg.get("RATE_LIMIT_RPS"))
            logger.debug(f"  {symbol}: Order book fetched in {time.time() - start_orderbook:.4f}s")

            # Volume profile — look for nearest HIGH-VOLUME node BELOW price (support)
            start_vol_profile = time.time()
            poc_price, nodes = pc.calc_volume_profile(ohlc, vols, bins=20)
            logger.debug(f"  {symbol}: Volume profile calculated in {time.time() - start_vol_profile:.4f}s")
            nodes_below = [n for n in nodes if n < last] if nodes else []
            if nodes_below and last > 0.0:
                nearest_node = max(nodes_below)   # closest node below price
                dist_to_node_below = abs(pc.pct_change(last, nearest_node))

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
        has_div = detect_bullish_divergence(closes, rsi_hist)
        raw_patterns = detect_patterns(ohlc)
        logger.debug(f"  {symbol}: Patterns detected in {time.time() - start_patterns:.4f}s")

        patterns: List[Tuple[str, int, float]] = []
        for entry in raw_patterns:
            if len(entry) == 3:
                name, bonus, quality = entry
            else:
                name, bonus, quality = entry[0], entry[1], entry[-1]
            # Three White Soldiers gate: RSI must not be overbought (already at top)
            if name.startswith("Three White Soldiers") and rsi is not None and rsi > THREE_WHITE_SOLDIERS_RSI_GATE:
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
            dist_low_pct=dist_low_pct,
            vol_spike=vol_spike,
            has_div=has_div,
            rsi_1h=rsi_1h,
            fr_change=fr_change if fr_change is not None else 0.0,
            spread=spread,
            dist_to_node_below=dist_to_node_below,
            ema_slope=ema_slope,
            slope_change=slope_change,
            news_count=news_count,
            news_titles=news_titles,
            raw_ohlc=ohlc[-10:],
            vol_24h=vol24
        )

        score, signals = score_long(data)

        bb_pct = None
        if bb:
            bb_range = bb["upper"] - bb["lower"]
            if bb_range > 0.0:
                bb_pct = (last - bb["lower"]) / bb_range * 100.0

        confidence, conf_color, conf_notes = calc_confidence(
            rsi, bb_pct, ema21, last, change_24h, fr, patterns, score, dist_low_pct, vol_spike
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
            "dist_low": dist_low_pct,
            "vol_spike": vol_spike,
            "bb_width": bb["width_pct"] if bb else 0.0,
            "atr_stop_pct": stop_pct,
            "news_count": news_count,
            "news_titles": news_titles,
            "raw_ohlc": ohlc[-10:],
            "spread": spread,
            "dist_to_node_below": dist_to_node_below,
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
                "direction": "Long"
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
