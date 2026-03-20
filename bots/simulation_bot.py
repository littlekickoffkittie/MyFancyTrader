#!/usr/bin/env python3
"""
Phemex Simulation (Paper Trading) Bot
======================================
Runs on LIVE production market data but simulates all trades locally.
Maintains a local 'paper_account.json' to track balance and positions.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import math
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if sys.platform != "win32":
    import select
    import termios
    import tty

import blessed
import requests
import websocket
from colorama import Fore, Style, init
from dotenv import load_dotenv

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.append(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

# ── Scanner & Legacy imports ─────────────────────────────────────────
try:
    from legacy import phemex_common as pc
    from scanners import long as scanner_long
    from scanners import short as scanner_short
except ImportError:
    try:
        import legacy.phemex_common as pc
        from scanners import long as scanner_long
        from scanners import short as scanner_short
    except ImportError:
        try:
            from ..legacy import phemex_common as pc
            from ..scanners import long as scanner_long
            from ..scanners import short as scanner_short
        except (ImportError, ValueError):
            import phemex_common as pc
            import long as scanner_long
            import short as scanner_short
# Safely import p_bot
try:
    import p_bot
except ImportError:
    # Try adding the parent directory if we are running from bots/
    # (sys and os are already imported above)
    sys.path.append(PARENT_DIR)  # patch[1]: root_dir was undefined
    try:
        import p_bot
    except ImportError:
        print(Fore.RED + "CRITICAL: 'p_bot.py' not found. This module is required for risk parameters.")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Initialization
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()
init(autoreset=True)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration & Constants
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR          = Path(__file__).parent
PAPER_ACCOUNT_FILE  = SCRIPT_DIR / "paper_account.json"
SIM_COOLDOWN_FILE   = SCRIPT_DIR / "sim_cooldowns.json"
SIM_LOGS_FILE       = SCRIPT_DIR / "sim_logs.json"
SIM_RESULTS_FILE    = SCRIPT_DIR / "sim_trade_results.json"
INITIAL_BALANCE     = float(os.getenv("INITIAL_BALANCE", "100.0"))
TAKER_FEE_RATE      = 0.001  # 0.1% Phemex taker fee
SHOW_PROGRESS       = os.getenv("BOT_SHOW_PROGRESS", "true").lower() == "true"
SIM_SESSION         = os.getenv("SIM_SESSION", "1")

# Telegram
TG_CHAT_ID          = os.getenv("TG_CHAT_ID", "")
TG_BOT_TOKEN        = os.getenv("TG_BOT_TOKEN", "")

# Fast-track entry: fire immediately when score exceeds threshold
FAST_TRACK_SCORE            = 130
FAST_TRACK_COOLDOWN_SECONDS = 300   # seconds before same symbol can fast-track again
RESULT_STALENESS_SECONDS    = 120   # discard scan results older than this

# Per-symbol re-entry cooldown (4 candles × 4H = 16 hours)
COOLDOWN_SECONDS = 4 * 4 * 3600

# Unicode Block Elements U+2581–U+2588 (8 chars; index math in sparkline() depends on count=8)
_SPARK_CHARS = "▁▂▃▄▅▆▇█"

# ─────────────────────────────────────────────────────────────────────────────

# Global State

# ─────────────────────────────────────────────────────────────────────────────



PAPER_ACCOUNT_LOCK = threading.Lock()

_live_prices: Dict[str, float] = {}

_prices_lock = threading.Lock()


_cooldown_lock   = threading.Lock()
_stop_lock       = threading.Lock()
_log_lock        = threading.Lock()
_display_lock    = threading.Lock()
_fast_track_lock = threading.Lock()
_file_io_lock    = threading.Lock()  # patch[5]: guards sim_trade_results JSONL writes

_ws_app:    Optional[websocket.WebSocketApp] = None
_ws_thread: Optional[threading.Thread]       = None

_slot_available_event  = threading.Event()
_display_paused        = threading.Event()
_display_thread_running = False

FAST_TRACK_COOLDOWN: Dict[str, float] = {}  # symbol → timestamp of last fast-track
_fast_track_opened:  set[str]         = set()
LAST_EXIT_TIME:      Dict[str, Tuple[float, int]] = {}  # symbol → (timestamp, score)

# TUI log buffer
_bot_logs: List[str] = []
_max_logs  = 100

# Equity sparkline history
_equity_history: List[float] = []
_max_history     = 50

logger = logging.getLogger("sim_bot")
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def tui_log(msg: str) -> None:
    """Appends a timestamped message to the internal TUI log buffer and saves to JSON."""
    with _log_lock:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        _bot_logs.append(f"[{timestamp}] {msg}")
        if len(_bot_logs) > _max_logs:
            _bot_logs.pop(0)
        try:
            SIM_LOGS_FILE.write_text(json.dumps(_bot_logs))
        except OSError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram_message(message: str) -> None:
    """Sends a message to the configured Telegram chat."""
    try:
        url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10)
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Paper Account Management
# ─────────────────────────────────────────────────────────────────────────────

def load_paper_account() -> dict:
    """Loads the paper account, creating it with defaults if it doesn't exist."""
    with PAPER_ACCOUNT_LOCK:
        if not PAPER_ACCOUNT_FILE.exists():
            data = {"balance": INITIAL_BALANCE, "positions": []}
            PAPER_ACCOUNT_FILE.write_text(json.dumps(data, indent=2))
            return data
        try:
            return json.loads(PAPER_ACCOUNT_FILE.read_text())
        except json.JSONDecodeError:
            logger.error("Failed to decode paper_account.json — reinitializing.")
            data = {"balance": INITIAL_BALANCE, "positions": []}
            PAPER_ACCOUNT_FILE.write_text(json.dumps(data, indent=2))
            return data


def save_paper_account(data: dict) -> None:
    """Persists the current paper account state to disk."""
    with PAPER_ACCOUNT_LOCK:
        PAPER_ACCOUNT_FILE.write_text(json.dumps(data, indent=2))


def _close_all_positions() -> None:
    """Manually closes every active paper position at the current market price."""
    acc = load_paper_account()
    if not acc["positions"]:
        print(Fore.YELLOW + "  No positions to close.")
        return

    print(Fore.CYAN + f"  Closing {len(acc['positions'])} positions...")

    for pos in acc["positions"]:
        symbol = pos["symbol"]
        side   = pos["side"]
        entry  = pos["entry"]
        size   = float(pos["size"])

        with _prices_lock:
            now = _live_prices.get(symbol)

        if now is None:
            try:
                ticker = pc.get_tickers()
                now = next((float(t["lastRp"]) for t in ticker if t["symbol"] == symbol), entry)
            except Exception:
                now = entry

        pnl = (now - entry) * size if side == "Buy" else (entry - now) * size
        acc["balance"] += (pos.get("margin", 0.0) + pnl)

        with _cooldown_lock:
            LAST_EXIT_TIME[symbol] = (time.time(), pos.get("entry_score", 0))

        pnl_emoji = "✅" if pnl > 0 else "❌"
        send_telegram_message(
            f"⏹ *SIM TRADES MANUALLY CLOSED (V2)*\n\n"
            f"*Symbol:* {symbol}\n"
            f"*Side:* {side}\n"
            f"*Exit Price:* {now}\n"
            f"*PnL:* {pnl_emoji} {pnl:+.4f} USDT\n"
            f"*Time:* {datetime.datetime.now().strftime('%H:%M:%S')}"
        )

        _log_closed_trade(
            symbol, side, entry, now, size,
            pos.get("entry_score", 0), pos.get("entry_time"), "manual_all_v2"
        )
        print(Fore.GREEN + f"  Closed {symbol} at {now}")

    acc["positions"] = []
    save_paper_account(acc)
    save_sim_cooldowns()
    _slot_available_event.set()
    print(Fore.GREEN + Style.BRIGHT + "  All positions closed successfully.")


def save_sim_cooldowns() -> None:
    """Persists active re-entry and fast-track cooldowns to disk, pruning expired entries."""
    with _cooldown_lock:
        active_exit = {}
        for s, data in LAST_EXIT_TIME.items():
            if isinstance(data, (tuple, list)) and len(data) == 2:
                ts, sc = data
                dur = p_bot.get_cooldown_duration(sc)
                if time.time() - ts < dur:
                    active_exit[s] = [ts, sc]
            else:
                # Handle old or corrupt data where only timestamp was saved
                try:
                    ts = float(data[0]) if isinstance(data, (tuple, list)) and data else float(data)
                except (IndexError, TypeError, ValueError):
                    ts = time.time()
                if time.time() - ts < 1200:
                    active_exit[s] = [ts, 0]
    with _fast_track_lock:
        active_ft = {s: ts for s, ts in FAST_TRACK_COOLDOWN.items() if time.time() - ts < FAST_TRACK_COOLDOWN_SECONDS}

    data = {
        "last_exit": active_exit,
        "fast_track": active_ft
    }
    try:
        SIM_COOLDOWN_FILE.write_text(json.dumps(data))
    except OSError:
        logger.error("Failed to save simulation cooldowns.")


def load_sim_cooldowns() -> None:
    """Loads re-entry and fast-track cooldowns from disk and discards any that have expired."""
    global LAST_EXIT_TIME, FAST_TRACK_COOLDOWN
    if not SIM_COOLDOWN_FILE.exists():
        return
    try:
        data = json.loads(SIM_COOLDOWN_FILE.read_text())
        # Support old format (just exit times) and new format (dict with keys)
        if isinstance(data, dict) and "last_exit" in data and "fast_track" in data:
            exit_data = data["last_exit"]
            ft_data   = data["fast_track"]
        else:
            exit_data = data
            ft_data   = {}

        with _cooldown_lock:
            LAST_EXIT_TIME = {}
            for s, val in exit_data.items():
                if isinstance(val, list) and len(val) == 2:
                    ts, sc = val
                    dur = p_bot.get_cooldown_duration(sc)
                    if time.time() - float(ts) < dur:
                        LAST_EXIT_TIME[s] = (float(ts), int(sc))
                else:
                    # Fallback for old format (just timestamp)
                    if time.time() - float(val) < 1200: # 20m default
                        LAST_EXIT_TIME[s] = (float(val), 0)
        with _fast_track_lock:
            FAST_TRACK_COOLDOWN = {
                s: float(ts) for s, ts in ft_data.items()
                if time.time() - float(ts) < FAST_TRACK_COOLDOWN_SECONDS
            }
        logger.info(f"Loaded {len(LAST_EXIT_TIME)} exit and {len(FAST_TRACK_COOLDOWN)} fast-track cooldowns.")
    except (json.JSONDecodeError, ValueError, AttributeError):
        logger.error("Failed to load simulation cooldowns — JSON is invalid.")


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket & Live Price Feed
# ─────────────────────────────────────────────────────────────────────────────

def _ws_on_message(ws: websocket.WebSocketApp, message: str) -> None:
    """Handles inbound WebSocket messages and updates the live price cache."""
    try:
        data = json.loads(message)
        if "market24h_p" in data:
            tick   = data["market24h_p"]
            symbol = tick.get("symbol")
            close  = tick.get("closeRp")
            if symbol and close is not None:
                with _prices_lock:
                    _live_prices[symbol] = float(close)
                _check_stops_live(symbol)
    except json.JSONDecodeError as e:
        logger.debug(f"WS message parse error: {e}")


def _ws_on_open(ws: websocket.WebSocketApp) -> None:
    """Subscribes to all currently open positions on WebSocket connect."""
    logger.info("WebSocket connection opened.")
    acc     = load_paper_account()
    symbols = [p["symbol"] for p in acc.get("positions", [])]
    if symbols:
        ws.send(json.dumps({"id": 1, "method": "market24h_p.subscribe", "params": symbols}))


def _ws_heartbeat(ws: websocket.WebSocketApp, stop_event: threading.Event) -> None:
    """Keeps the WebSocket alive by sending periodic pings."""
    while not stop_event.is_set():
        time.sleep(5)
        # Check if this heartbeat instance is still the active one
        if ws is not _ws_app:
            logger.debug("Heartbeat thread detected stale WS app — exiting.")
            break
        try:
            if ws.sock and ws.sock.connected:
                ws.send(json.dumps({"id": 0, "method": "server.ping", "params": []}))
            else:
                # Exit if socket is no longer connected
                break
        except (websocket.WebSocketConnectionClosedException, BrokenPipeError):
            logger.debug("WebSocket closed during heartbeat — exiting heartbeat thread.")
            break
        except Exception as e:
            logger.debug(f"Heartbeat error: {e}")
            break


def _ws_run_loop() -> None:
    """Maintains the WebSocket connection, reconnecting while positions are open."""
    global _ws_app
    ws_url = "wss://testnet.phemex.com/ws" if "testnet" in pc.BASE_URL else "wss://ws.phemex.com"

    retries = 0
    while True:
        stop_event = threading.Event()
        _ws_app = websocket.WebSocketApp(ws_url, on_message=_ws_on_message, on_open=_ws_on_open)
        threading.Thread(target=_ws_heartbeat, args=(_ws_app, stop_event), daemon=True).start()
        _ws_app.run_forever()

        # Signal heartbeat to stop after run_forever exits
        stop_event.set()

        # Grace period to allow pending saves/subscriptions to complete
        time.sleep(2.0)
        if not load_paper_account().get("positions"):
            break

        retries += 1
        delay = min(2**retries, 60)
        logger.info(f"WebSocket disconnected. Retrying in {delay}s (attempt {retries})...")
        time.sleep(delay)


def _ensure_ws_started() -> None:
    """Starts the WebSocket thread if it is not already running."""
    global _ws_thread
    if _ws_thread is None or not _ws_thread.is_alive():
        _ws_thread = threading.Thread(target=_ws_run_loop, daemon=True)
        _ws_thread.start()


def _subscribe_symbol(symbol: str) -> None:
    """Subscribes the WebSocket to a new symbol after a short delay."""
    def _do_sub() -> None:
        time.sleep(1.5)
        if _ws_app and _ws_app.sock and _ws_app.sock.connected:
            symbols = [p["symbol"] for p in load_paper_account().get("positions", [])]
            _ws_app.send(json.dumps({"id": 1, "method": "market24h_p.subscribe", "params": symbols}))

    threading.Thread(target=_do_sub, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Stop / Take-Profit Monitoring
# ─────────────────────────────────────────────────────────────────────────────

def _check_stops_live(symbol: str) -> None:
    """Evaluates trailing-stop and take-profit levels for a symbol on each price tick."""
    # Narrow lock scope — copy data then release lock
    exit_to_process = None
    with _stop_lock:
        acc       = load_paper_account()
        positions = acc.get("positions", [])
        pos_idx   = next((i for i, p in enumerate(positions) if p["symbol"] == symbol), None)
        if pos_idx is None:
            return

        pos = positions[pos_idx]
        with _prices_lock:
            current_price = _live_prices.get(symbol)
        if current_price is None:
            return

        side  = pos["side"]
        entry = pos["entry"]
        size  = float(pos["size"])
        # Use .get() for stop_price and check existence
        stop_price = pos.get("stop_price")
        if stop_price is None:
            return

        stop_hit   = False
        tp_hit     = False
        exit_price = current_price
        partial_tp = False

        if side == "Buy":
            if current_price > pos.get("high_water", 0.0):
                pos["high_water"]  = current_price
                pos["stop_price"]  = current_price * (1.0 - p_bot.TRAIL_PCT)
            if current_price <= stop_price:
                stop_hit   = True
                exit_price = stop_price
            else:
                # Check stages
                for stage in pos.get("tp_stages", []):
                    if not stage["hit"] and current_price >= stage["price"]:
                        stage["hit"] = True
                        tp_hit = True
                        exit_price = stage["price"]
                        # Partial TP logic: reduce size, realized pnl
                        q_to_close = stage["qty"]
                        if q_to_close >= pos["size"]:
                            partial_tp = False # full close
                        else:
                            partial_tp = True
                            pnl_stage = (exit_price - entry) * q_to_close
                            acc["balance"] += pnl_stage
                            pos["size"] -= q_to_close
                            
                            # Log partial TP to history for realized PnL update
                            _log_closed_trade(
                                symbol, side, entry, exit_price, q_to_close,
                                pos.get("entry_score", 0), pos.get("entry_time"), "partial_tp"
                            )
                            
                            # Log partial TP
                            tui_log(f"PARTIAL TP: {symbol} 1/2 or 1/4 closed at {exit_price}")
        else:
            if current_price < pos.get("low_water", 9_999_999.0):
                pos["low_water"]  = current_price
                pos["stop_price"] = current_price * (1.0 + p_bot.TRAIL_PCT)
            if current_price >= stop_price:
                stop_hit   = True
                exit_price = stop_price
            else:
                # Check stages
                for stage in pos.get("tp_stages", []):
                    if not stage["hit"] and current_price <= stage["price"]:
                        stage["hit"] = True
                        tp_hit = True
                        exit_price = stage["price"]
                        # Partial TP logic: reduce size, realized pnl
                        q_to_close = stage["qty"]
                        if q_to_close >= pos["size"]:
                            partial_tp = False # full close
                        else:
                            partial_tp = True
                            pnl_stage = (entry - exit_price) * q_to_close
                            acc["balance"] += pnl_stage
                            pos["size"] -= q_to_close
                            
                            # Log partial TP to history for realized PnL update
                            _log_closed_trade(
                                symbol, side, entry, exit_price, q_to_close,
                                pos.get("entry_score", 0), pos.get("entry_time"), "partial_tp"
                            )
                            
                            # Log partial TP
                            tui_log(f"PARTIAL TP: {symbol} 1/2 or 1/4 closed at {exit_price}")

        if not (stop_hit or tp_hit):
            pos["mark_price"] = current_price
            save_paper_account(acc)
            return

        if partial_tp:
            # We already updated balance/size, just save and continue monitoring
            pos["mark_price"] = current_price
            save_paper_account(acc)
            return

        exit_reason = "Stop Hit" if stop_hit else "Take Profit Hit"
        pnl = (exit_price - entry) * size if side == "Buy" else (entry - exit_price) * size
        acc["balance"] += (pos.get("margin", 0.0) + pnl)

        # Prepare for I/O outside the lock
        exit_to_process = {
            "symbol": symbol,
            "side": side,
            "exit_reason": exit_reason,
            "exit_price": exit_price,
            "pnl": pnl,
            "entry": entry,
            "size": size,
            "entry_score": pos.get("entry_score", 0),
            "entry_time": pos.get("entry_time"),
            "stop_hit": stop_hit
        }

        positions.pop(pos_idx)
        save_paper_account(acc)

    # Process I/O outside the lock
    if exit_to_process:
        with _cooldown_lock:
            LAST_EXIT_TIME[symbol] = (time.time(), exit_to_process["entry_score"])
        save_sim_cooldowns()
        _slot_available_event.set()

        msg_color = Fore.RED if exit_to_process["stop_hit"] else Fore.GREEN
        print(msg_color + Style.BRIGHT + f"\n  [SIM] {exit_to_process['exit_reason'].upper()}: {symbol} {exit_to_process['side']} closed at {exit_to_process['exit_price']}")

        pnl_emoji = "✅" if exit_to_process["pnl"] > 0 else "❌"
        send_telegram_message(
            f"🔔 *SIM TRADE CLOSED ({exit_to_process['exit_reason']})*\n\n"
            f"*Symbol:* {symbol}\n"
            f"*Side:* {exit_to_process['side']}\n"
            f"*Exit Price:* {exit_to_process['exit_price']}\n"
            f"*PnL:* {pnl_emoji} {exit_to_process['pnl']:+.4f} USDT\n"
            f"*Time:* {datetime.datetime.now().strftime('%H:%M:%S')}"
        )
        _log_closed_trade(
            symbol, exit_to_process["side"], exit_to_process["entry"],
            exit_to_process["exit_price"], exit_to_process["size"],
            exit_to_process["entry_score"], exit_to_process["entry_time"],
            "stop" if exit_to_process["stop_hit"] else "tp"
        )


def _log_closed_trade(
    symbol: str,
    direction: str,
    entry: float,
    exit_price: float,
    size: float,
    entry_score: float,
    entry_time: Optional[str],
    reason: str,
) -> None:
    """Appends a closed-trade record to sim_trade_results.json."""
    pnl = (exit_price - entry) * size if direction == "Buy" else (entry - exit_price) * size

    hold_time = 0
    if entry_time:
        try:
            hold_time = (datetime.datetime.now() - datetime.datetime.fromisoformat(entry_time)).total_seconds()
        except ValueError:
            logger.error("Invalid entry_time format — using zero hold time.")

    record = {
        "symbol":      symbol,
        "direction":   "LONG" if direction == "Buy" else "SHORT",
        "entry":       entry,
        "exit":        exit_price,
        "pnl":         round(pnl, 4),
        "hold_time_s": int(hold_time),
        "score":       entry_score,
        "reason":      reason,
        "timestamp":   datetime.datetime.now().isoformat(),
    }

    # patch[5]: JSONL append — O(1), no read-modify-write race
    try:
        with _file_io_lock:
            with open(SIM_RESULTS_FILE, "a", encoding="utf-8") as _fh:
                _fh.write(json.dumps(record) + "\n")
    except OSError:
        logger.error("Failed to write trade record to sim_trade_results.jsonl.")

    m, s = divmod(int(hold_time), 60)
    print(
        Fore.CYAN +
        f"  ✓ CLOSED {symbol} | Entry: {entry}  Exit: {exit_price} | "
        f"PnL: {pnl:+.4f} USDT | Held: {m}m {s}s | Score: {entry_score}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TUI — Drawing Helpers  (v2)
# ─────────────────────────────────────────────────────────────────────────────

# ── String helpers ────────────────────────────────────────────────────────────

def _vlen(s: str) -> int:
    """Visible length of a string — strips ANSI/escape codes."""
    return len(re.sub(r"\x1b\[[0-9;]*[mKHJ]", "", s))


def _rpad(s: str, width: int, char: str = " ") -> str:
    """Right-pad a styled string to exact visible `width`."""
    return s + char * max(0, width - _vlen(s))


# ── Box/panel primitives ──────────────────────────────────────────────────────

def _box_top(title: str, width: int, right_tag: str = "") -> str:
    """Single-line top border: ┌─ TITLE ──────────── right_tag ┐"""
    inner = width - 2
    if title:
        lbl = f"─ {title} "
        if right_tag:
            gap = inner - _vlen(lbl) - _vlen(right_tag) - 1
            return f"┌{lbl}{'─' * max(gap, 1)}{right_tag}┐"
        return f"┌{lbl}{'─' * (inner - _vlen(lbl))}┐"
    return f"┌{'─' * inner}┐"


def _box_bot(width: int) -> str:
    return f"└{'─' * (width - 2)}┘"


def _box_row(term: blessed.Terminal, content: str, width: int) -> str:
    """│ content (padded) │  — content is already styled."""
    inner = width - 4
    padded = _rpad(content, inner)
    return term.cyan("│") + " " + padded + term.normal + " " + term.cyan("│")


def _box_empty(term: blessed.Terminal, width: int) -> str:
    return term.cyan("│") + " " * (width - 2) + term.cyan("│")


# ── Sparkline ─────────────────────────────────────────────────────────────────

def sparkline(data: List[float], width: int) -> str:
    """Returns a unicode sparkline of `width` characters from the given data."""
    if not data:
        return "▁" * width
    data = data[-width:]
    lo, hi = min(data), max(data)
    rng = hi - lo if hi != lo else 1.0
    return "".join(_SPARK_CHARS[min(int((v - lo) / rng * 7), 7)] for v in data)


# ── Header ────────────────────────────────────────────────────────────────────

def _draw_header(term: blessed.Terminal, current_time: str, max_width: int = 80) -> None:
    """Draws the top header: double outer box, title left, clock right."""
    w = max_width
    title     = "PHEMEX SIM BOT"
    badge     = "◈ PAPER"
    session_s = f"SESS: {SIM_SESSION}"
    
    # Raw visible widths for gap calculation
    left_raw  = f"  ⚡ {title}  {badge}"
    right_raw = f"{session_s}  {current_time}  "
    gap       = max(0, w - 2 - len(left_raw) - len(right_raw))

    left_styled  = (
        "  ⚡ "
        + term.bold_cyan(title)
        + "  "
        + term.yellow(badge)
    )
    right_styled = term.bold_white(session_s) + "  " + term.bold_white(current_time) + "  "
    body = term.cyan("║") + left_styled + " " * gap + right_styled + term.cyan("║")

    print(term.move_xy(2, 1) + term.cyan("╔" + "═" * (w - 2) + "╗"))
    print(term.move_xy(2, 2) + body)
    print(term.move_xy(2, 3) + term.cyan("╠" + "═" * (w - 2) + "╣"))


# ── Positions ─────────────────────────────────────────────────────────────────

def _draw_positions_section(
    term: blessed.Terminal,
    positions: List[Dict[str, Any]],
    current_prices: Dict[str, float],
    start_row: int,
    max_width: int = 80,
) -> int:
    """Renders the active-positions panel."""
    w   = max_width
    row = start_row
    n   = len(positions)

    slot_tag = f"─ {n} open " if n else "─ idle "
    print(term.move_xy(2, row) + term.cyan(_box_top("OPEN POSITIONS", w, slot_tag)))
    row += 1

    if not positions:
        msg = term.white("  Waiting for qualifying setups") + term.cyan(" ·")
        print(term.move_xy(2, row) + _box_row(term, _rpad(msg, w - 4), w))
        row += 1
    else:
        for pos in positions:
            sym        = pos["symbol"]
            side       = pos["side"]
            entry      = float(pos["entry"])
            size       = float(pos["size"])
            stop       = float(pos.get("stop_price", 0))
            tp         = float(pos.get("take_profit", 0))
            orig_stop  = float(pos.get("original_stop", stop))
            score      = pos.get("entry_score", 0)
            now        = current_prices.get(sym)
            is_long    = side == "Buy"

            upnl = 0.0
            if now:
                upnl = (now - entry) * size if is_long else (entry - now) * size
            
            # patch[8]: Position entity logging removed from render loop.
            # It fired a blocking HTTP POST per position per frame (~0.8s),
            # causing TUI stutter. Aggregate state is captured by Snapshot.

            # Direction badge
            dir_badge = (
                term.bold_green("▲ LONG ") if is_long else term.bold_red("▼ SHORT")
            )
            now_str = f"{now:.5g}" if now else "·······"
            if now:
                pnl_str = (
                    term.bold_green(f"+{upnl:.4f}")
                    if upnl >= 0 else term.bold_red(f"{upnl:.4f}")
                )
            else:
                pnl_str = term.white("·······")

            margin_s = term.cyan(f"M: ${pos.get('margin', 0.0):.1f}")
            lev_s    = term.cyan(f"{pos.get('leverage', 0)}x")
            # ── Row 1: direction · symbol · entry → now · pnl · score ──────
            score_badge = term.yellow(f"[{score}]")
            arrow       = term.white("──▶")
            entry_s     = term.white(f"{entry:.5g}")
            now_s       = term.white(now_str)
            sym_s       = term.bold_white(f"{sym:<12}")

            line1 = f" {dir_badge} {sym_s} {entry_s} {arrow} {now_s}  {margin_s} ({lev_s})  {pnl_str}  {score_badge}"
            print(term.move_xy(2, row) + _box_row(term, line1, w))
            row += 1

            # ── Row 2: price-position bar ────────────────────────────────────
            if now:
                bar_w = w - 16
                pts   = [orig_stop, stop, entry, now, tp]
                lo    = min(pts)
                hi    = max(pts)
                rng   = (hi - lo) if hi != lo else 1.0

                def gp(v: float) -> int:
                    return max(0, min(bar_w - 1, int((v - lo) / rng * (bar_w - 1))))

                bar = list("─" * bar_w)
                bar[gp(orig_stop)] = term.red("╳")
                bar[gp(stop)]      = term.bold_red("S")
                bar[gp(entry)]     = term.yellow("E")
                bar[gp(tp)]        = term.bold_green("T")
                bar[gp(now)]       = term.bold_white("●")

                sl_s  = term.red(f"{stop:.4g}")
                tp_s  = term.green(f"{tp:.4g}")
                label = f"    ╰ SL {sl_s}  TP {tp_s}  " + term.cyan("[") + "".join(bar) + term.cyan("]")
                print(term.move_xy(2, row) + _box_row(term, label, w))
                row += 1

    print(term.move_xy(2, row) + term.cyan(_box_bot(w)))
    row += 1
    return row


# ── Account + Session (two columns) ──────────────────────────────────────────

def _draw_account_session_section(
    term: blessed.Terminal,
    balance: float,
    locked_margin: float,
    current_upnl: float,
    equity: float,
    total_trades: int,
    wins: int,
    losses: int,
    win_rate: float,
    total_closed_pnl: float,
    start_row: int,
    max_width: int = 80,
    equity_history: List[float] = None
) -> int:
    """Two-column panel: wallet left, session stats right."""

    # Use passed history to avoid global mutation side-effects
    spark_data = equity_history if equity_history else []

    w   = max_width
    lw  = 36          # left column width
    gap = 2
    rw  = w - lw - gap

    eq_delta   = equity - INITIAL_BALANCE
    eq_color   = term.bold_green  if eq_delta  >= 0 else term.bold_red
    upnl_color = term.green       if current_upnl >= 0 else term.red
    rpnl_color = term.bold_green  if total_closed_pnl >= 0 else term.bold_red

    spark = sparkline(spark_data, lw - 6)
    spark_colored = (
        term.green(spark) if eq_delta >= 0 else term.red(spark)
    )

    # ── Left panel: wallet ────────────────────────────────────────────────────
    left_lines: List[str] = []
    left_lines.append(term.cyan(_box_top("WALLET", lw)))
    left_lines.append(_box_row(term,
        "  Available" + term.bold_white(f"${balance:9.2f}") + term.cyan(" USDT"), lw))
    left_lines.append(_box_row(term,
        "  Locked   " + term.yellow(f"${locked_margin:9.2f}") + term.cyan(" USDT"), lw))
    left_lines.append(_box_row(term,
        "  uPnL     " + upnl_color(f"{current_upnl:+.4f}") + term.cyan(" USDT"), lw))
    left_lines.append(_box_row(term,
        "  Equity   " + eq_color(f"${equity:9.2f}") + term.cyan(" USDT"), lw))
    left_lines.append(_box_row(term, "  " + spark_colored + "  equity", lw))
    left_lines.append(term.cyan(_box_bot(lw)))

    # ── Right panel: statistics ───────────────────────────────────────────────
    right_lines: List[str] = []
    right_lines.append(term.cyan(_box_top("STATISTICS", rw)))
    right_lines.append(_box_row(term,
        "  Trades  " + term.bold_white(str(total_trades).ljust(4)), rw))
    right_lines.append(_box_row(term,
        f"  {term.bold_green(f'✅ {wins}W')}   {term.bold_red(f'❌ {losses}L')}"
        f"   Rate {term.yellow(f'{win_rate:.1f}%')}", rw))
    right_lines.append(_box_row(term,
        "  Realized  " + rpnl_color(f"{total_closed_pnl:+.4f}") + term.cyan(" USDT"), rw))
    right_lines.append(_box_empty(term, rw))
    right_lines.append(term.cyan(_box_bot(rw)))

    row = start_row
    for l_line, r_line in zip(left_lines, right_lines):
        print(term.move_xy(2, row) + l_line + " " * gap + r_line)
        row += 1

    return row


# ── Trade history ─────────────────────────────────────────────────────────────

def _draw_history_section(
    term: blessed.Terminal,
    history: List[Dict[str, Any]],
    start_row: int,
    max_width: int = 80,
) -> int:
    """Two-per-row closed trade history (last 6)."""
    w      = max_width
    row    = start_row
    recent = history[::-1][:6]

    print(term.move_xy(2, row) + term.cyan(_box_top("TRADE HISTORY", w)))
    row += 1

    if not recent:
        msg = term.white("  No closed trades yet")
        print(term.move_xy(2, row) + _box_row(term, msg, w))
        row += 1
    else:
        col_w = (w - 6) // 2  # visible width for one trade cell

        def _fmt(t: dict) -> str:
            pnl   = t["pnl"]
            c     = term.bold_green if pnl > 0 else term.bold_red
            badge = "✅" if pnl > 0 else "❌"
            ts    = t["timestamp"][11:16]
            sym   = t["symbol"][:10].ljust(10)
            d     = t["direction"][:5].ljust(5)
            return f" {term.white(ts)} {term.bold_white(sym)} {term.cyan(d)} {badge} {c(f'{pnl:+.4f}')}"

        for i in range(0, len(recent), 2):
            left_cell = _fmt(recent[i])
            if i + 1 < len(recent):
                right_cell = _fmt(recent[i + 1])
                sep        = term.cyan("│")
                content    = _rpad(left_cell, col_w) + sep + right_cell
            else:
                content = left_cell
            print(term.move_xy(2, row) + _box_row(term, content, w))
            row += 1

    print(term.move_xy(2, row) + term.cyan(_box_bot(w)))
    row += 1
    return row


# ── System log ────────────────────────────────────────────────────────────────

def _draw_system_logs_section(
    term: blessed.Terminal,
    logs: List[str],
    start_row: int,
    max_width: int = 80,
) -> int:
    """Color-coded scrolling log panel (last 6 entries)."""
    w   = max_width
    row = start_row

    print(term.move_xy(2, row) + term.cyan(_box_top("SYSTEM LOG", w)))
    row += 1

    with _log_lock:
        display_logs = list(logs[-6:])

    # Always render exactly 6 rows
    while len(display_logs) < 6:
        display_logs.append("")

    for entry in display_logs:
        if not entry:
            print(term.move_xy(2, row) + _box_empty(term, w))
        else:
            ts_end   = entry.find("]") + 1
            ts_part  = entry[:ts_end]
            msg_part = entry[ts_end:]

            # Colour by content
            msg_upper = msg_part.upper()
            if "⚡" in entry or "FAST" in msg_upper:
                msg_color = term.bold_yellow
            elif "TAKE PROFIT" in msg_upper or "ENTERED" in msg_upper:
                msg_color = term.green
            elif "STOP" in msg_upper or "CLOSED" in msg_upper:
                msg_color = term.red
            elif "ERROR" in msg_upper or "WARN" in msg_upper:
                msg_color = term.bold_red
            elif "SCAN" in msg_upper or "COMPLETE" in msg_upper:
                msg_color = term.cyan
            else:
                msg_color = term.white

            styled = term.white(ts_part) + msg_color(msg_part)
            print(term.move_xy(2, row) + _box_row(term, styled, w))

        row += 1

    print(term.move_xy(2, row) + term.cyan(_box_bot(w)))
    row += 1
    return row


# ── Footer ────────────────────────────────────────────────────────────────────

def _draw_footer(term: blessed.Terminal, row: int, max_width: int = 80) -> None:
    """Bottom bar with keyboard shortcuts."""
    w          = max_width
    left_raw   = "  [S] Close All  [Q] Quit  "
    right_raw  = "  ⚡ FANCYBOT v2  "
    gap        = max(0, w - 2 - len(left_raw) - len(right_raw))

    left_part  = (
        "  "
        + term.bold_white("[S]") + term.white(" Close All")
        + "  "
        + term.bold_white("[Q]") + term.white(" Quit")
        + "  "
    )
    right_part = "  ⚡ " + term.bold_cyan("FANCYBOT") + term.white(" v2") + "  "
    inner      = left_part + term.cyan("─" * gap) + right_part
    line       = term.cyan("╚═") + inner + term.normal + term.cyan("═╝")
    print(term.move_xy(2, row) + line)


# ─────────────────────────────────────────────────────────────────────────────
# TUI — Main Display Loop
# ─────────────────────────────────────────────────────────────────────────────


def _migrate_results_to_jsonl() -> None:
    """patch[6]: one-time migration of JSON array → JSONL on first bot run."""
    import json as _json
    f = SCRIPT_DIR / "sim_trade_results.json"
    if not f.exists():
        return
    try:
        raw = f.read_text(encoding="utf-8").strip()
        if not raw or raw[0] != "[":
            return  # already JSONL or empty
        records = _json.loads(raw)
        if not isinstance(records, list):
            return
        with _file_io_lock:
            with open(f, "w", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(_json.dumps(rec) + "\n")
        logger.info(f"Migrated {len(records)} records to JSONL format.")
    except Exception as exc:
        logger.warning(f"JSONL migration skipped: {exc}")

def _live_pnl_display() -> None:
    """Full-screen TUI dashboard — runs in a dedicated daemon thread."""
    global _display_thread_running, _equity_history
    term         = blessed.Terminal()
    results_file = SCRIPT_DIR / "sim_trade_results.json"

    with term.fullscreen(), term.cbreak(), term.hidden_cursor():
        try:
            while True:
                if _display_paused.is_set():
                    time.sleep(0.5)
                    continue

                acc       = load_paper_account()
                positions = acc.get("positions", [])

                # patch[6]: parse JSONL (one record per line)
                history: List[dict] = []
                if results_file.exists():
                    try:
                        history = [
                            json.loads(ln)
                            for ln in results_file.read_text(encoding="utf-8").splitlines()
                            if ln.strip()
                        ]
                    except Exception:
                        pass

                wins             = [t for t in history if t["pnl"] > 0]
                losses           = [t for t in history if t["pnl"] <= 0]
                total_trades     = len(history)
                win_rate         = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0
                total_closed_pnl = sum(t["pnl"] for t in history)
                current_time     = datetime.datetime.now().strftime("%H:%M:%S")
                balance          = acc.get("balance", 0.0)

                with _prices_lock:
                    current_upnl = 0.0
                    locked_margin = 0.0
                    for p in positions:
                        locked_margin += p.get("margin", 0.0)
                        if _live_prices.get(p["symbol"]):
                            now = _live_prices[p["symbol"]]
                            entry = p["entry"]
                            size = float(p["size"])
                            current_upnl += (now - entry) * size if p["side"] == "Buy" else (entry - now) * size

                equity = balance + locked_margin + current_upnl

                # Update equity history here (state mutation), strictly outside render function
                _equity_history.append(equity)
                if len(_equity_history) > _max_history:
                    _equity_history.pop(0)

                # Use a string buffer to draw the entire frame at once (reduces flicker)
                frame = []
                frame.append(term.home)
                
                # We need to adapt the drawing helpers to return strings instead of printing
                # But for now, let's just use a simple capture or move cursor to home
                # To really fix the rolling shutter, we should stop calling 'print' inside helpers
                # and instead make them return strings.
                
                # Since changing all helpers is large, let's try just removing term.clear 
                # and using term.home, while ensuring we clear to end of screen if needed.
                # However, term.clear is what causes the 'rolling' flash.
                
                max_w = 80
                # Instead of print(term.clear), we'll use term.home and hope the boxes overwrite
                # or we can use term.clear_eol for each line.
                
                # Let's try to gather all output into a single string if possible.
                # Since the helpers use 'print', we can't easily gather them without changing them.
                # I'll modify _live_pnl_display to use a more stable redraw.
                
                # Optimization: only clear if necessary, otherwise use home
                print(term.home, end="")
                _draw_header(term, current_time, max_w)
                row = 4
                row = _draw_positions_section(term, positions, _live_prices, row, max_w)
                # Pass explicit history to render
                row = _draw_account_session_section(
                    term, balance, locked_margin, current_upnl, equity,
                    total_trades, len(wins), len(losses),
                    win_rate, total_closed_pnl, row, max_w,
                    _equity_history
                )

                # ── Snapshot Entity Logging ──────────────────────────────────────
                if not _display_paused.is_set():
                    p_bot.make_entity_request("Snapshot", data={
                        "timestamp": datetime.datetime.now().isoformat(),
                        "equity": equity,
                        "available_balance": balance,
                        "locked_margin": locked_margin,
                        "open_position_count": len(positions),
                        "unrealized_pnl": current_upnl,
                        "realized_pnl_cumulative": total_closed_pnl,
                        "win_rate": win_rate,
                        "profit_factor": 0 # TODO: Calculate profit factor
                    })

                row = _draw_history_section(term, history, row, max_w)

                row = _draw_system_logs_section(term, _bot_logs, row, max_w)
                _draw_footer(term, row, max_w)

                key = term.inkey(timeout=0.8)
                if key.lower() == "s":
                    _display_paused.set()
                    confirm_row = row + 1
                    print(
                        term.move_xy(4, confirm_row)
                        + term.on_red(term.bold_white("  ⚠  CLOSE ALL TRADES?  "))
                        + term.bold_yellow("  (Y / N)  "),
                        end="", flush=True,
                    )
                    if term.inkey().lower() == "y":
                        _close_all_positions()
                        time.sleep(1)
                    _display_paused.clear()
                elif key.lower() == "q":
                    break

        except KeyboardInterrupt:
            pass
        finally:
            with _display_lock:
                _display_thread_running = False


# ─────────────────────────────────────────────────────────────────────────────
# Simulation Overrides
# ─────────────────────────────────────────────────────────────────────────────

def get_sim_balance() -> float:
    """Returns the current wallet balance from the paper account."""
    return load_paper_account().get("balance", 0.0)


def get_sim_positions() -> List[dict]:
    """Returns the list of open paper positions."""
    return load_paper_account().get("positions", [])


def update_pnl_and_stops() -> None:
    """
    Polls live prices for all open positions, updates PnL, and evaluates
    trailing-stop and take-profit levels.
    """
    # Initialize outside lock to avoid UnboundLocalError
    ticker_map: Dict[str, Any] = {}
    missing: List[str] = []

    # Narrow lock scope — only hold lock while reading/writing the account structure.
    # Move all I/O (Telegram, Logging) outside the lock.
    with _stop_lock:
        acc = load_paper_account()
        if not acc["positions"]:
            return

        # Fetch REST tickers only for symbols not yet in the live-price cache
        missing = [p["symbol"] for p in acc["positions"] if p["symbol"] not in _live_prices]
        if missing:
            # Releasing stop lock during ticker fetch to avoid blocking stop evaluations
            pass

    if missing:
        try:
            tickers    = pc.get_tickers()
            ticker_map = {t["symbol"]: t for t in tickers}
        except Exception as e:
            logger.debug(f"Failed to fetch REST tickers in update loop: {e}")

    # To store events for I/O outside the lock
    exits_to_process = []

    with _stop_lock:
        # Reload account in case it changed during the REST fetch
        acc = load_paper_account()

        # Re-derive current positions state to avoid phantom PnL on just-closed symbols
        # (Though update_pnl_and_stops iterates over acc["positions"],
        # using ticker_map from a previous stale missing list is the risk)

        new_positions: List[dict] = []
        closed_any = False

        for pos in acc["positions"]:
            symbol = pos["symbol"]
            with _prices_lock:
                current_price = _live_prices.get(symbol)

            if current_price is None:
                ticker = ticker_map.get(symbol)
                if ticker:
                    current_price = float(ticker.get("lastRp") or 0.0)

            if not current_price:
                new_positions.append(pos)
                continue

            side = pos["side"]
            exit_reason = None
            exit_price  = 0.0
            pnl         = 0.0
            partial_tp  = False

            # Use .get() for stop_price and check existence
            stop_price = pos.get("stop_price")
            if stop_price is None:
                new_positions.append(pos)
                continue

            if side == "Buy":
                if current_price > pos.get("high_water", 0.0):
                    pos["high_water"] = current_price
                    pos["stop_price"] = current_price * (1.0 - p_bot.TRAIL_PCT)

                if current_price <= stop_price:
                    exit_reason, exit_price = "Stop Loss", stop_price
                else:
                    for stage in pos.get("tp_stages", []):
                        if not stage["hit"] and current_price >= stage["price"]:
                            stage["hit"] = True
                            exit_price = stage["price"]
                            q_to_close = stage["qty"]
                            if q_to_close >= pos["size"]:
                                exit_reason = "Take Profit"
                            else:
                                partial_tp = True
                                pnl_stage = (exit_price - pos["entry"]) * q_to_close
                                acc["balance"] += pnl_stage
                                pos["size"] -= q_to_close
                                
                                # Log partial TP to history for realized PnL update
                                _log_closed_trade(
                                    symbol, side, pos["entry"], exit_price, q_to_close,
                                    pos.get("entry_score", 0), pos.get("entry_time"), "partial_tp"
                                )
                                
                                tui_log(f"PARTIAL TP: {symbol} closed at {exit_price}")
            else:
                if current_price < pos.get("low_water", 999_999_999.0):
                    pos["low_water"]  = current_price
                    pos["stop_price"] = current_price * (1.0 + p_bot.TRAIL_PCT)

                if current_price >= stop_price:
                    exit_reason, exit_price = "Stop Loss", stop_price
                else:
                    for stage in pos.get("tp_stages", []):
                        if not stage["hit"] and current_price <= stage["price"]:
                            stage["hit"] = True
                            exit_price = stage["price"]
                            q_to_close = stage["qty"]
                            if q_to_close >= pos["size"]:
                                exit_reason = "Take Profit"
                            else:
                                partial_tp = True
                                pnl_stage = (pos["entry"] - exit_price) * q_to_close
                                acc["balance"] += pnl_stage
                                pos["size"] -= q_to_close
                                
                                # Log partial TP to history for realized PnL update
                                _log_closed_trade(
                                    symbol, side, pos["entry"], exit_price, q_to_close,
                                    pos.get("entry_score", 0), pos.get("entry_time"), "partial_tp"
                                )
                                
                                tui_log(f"PARTIAL TP: {symbol} closed at {exit_price}")
            if partial_tp:
                # Still active
                pos["pnl"] = (current_price - pos["entry"]) * pos["size"] if side == "Buy" else (pos["entry"] - current_price) * pos["size"]
                new_positions.append(pos)
                closed_any = True # we updated balance
                continue

            if exit_reason:
                if side == "Buy":
                    pnl = (exit_price - pos["entry"]) * pos["size"]
                else:
                    pnl = (pos["entry"] - exit_price) * pos["size"]

            if exit_reason:
                # Store the exit data for processing after the lock is released
                exits_to_process.append({
                    "symbol": symbol,
                    "side": side,
                    "exit_reason": exit_reason,
                    "exit_price": exit_price,
                    "pnl": pnl,
                    "margin": pos.get("margin", 0.0),
                    "entry": pos["entry"],
                    "size": pos["size"],
                    "entry_score": pos.get("entry_score", 0),
                    "entry_time": pos.get("entry_time"),
                })
                acc["balance"] += (pos.get("margin", 0.0) + pnl)
                closed_any = True
                continue

            # Position remains open
            pos["pnl"] = (current_price - pos["entry"]) * pos["size"] if side == "Buy" else (pos["entry"] - current_price) * pos["size"]
            pos["mark_price"] = current_price
            new_positions.append(pos)

        if closed_any:
            acc["positions"] = new_positions
            save_paper_account(acc)

    # Process I/O (exits) outside the lock to avoid blocking
    for ex in exits_to_process:
        symbol = ex["symbol"]
        with _cooldown_lock:
            LAST_EXIT_TIME[symbol] = (time.time(), ex["entry_score"])
        save_sim_cooldowns()
        _slot_available_event.set()

        tui_log(f"{ex['exit_reason'].upper()} HIT: {symbol} closed at {ex['exit_price']}")
        pnl_emoji = "✅" if ex['pnl'] > 0 else "❌"
        send_telegram_message(
            f"🔔 *SIM TRADE CLOSED ({ex['exit_reason']})*\n\n"
            f"*Symbol:* {symbol}\n*Side:* {ex['side']}\n"
            f"*Exit Price:* {ex['exit_price']}\n"
            f"*PnL:* {pnl_emoji} {ex['pnl']:+.4f} USDT\n"
            f"*Time:* {datetime.datetime.now().strftime('%H:%M:%S')}"
        )
        _log_closed_trade(
            symbol, ex['side'], ex['entry'], ex['exit_price'], ex['size'],
            ex['entry_score'], ex['entry_time'],
            "stop" if "Stop" in ex['exit_reason'] else "tp"
        )



def _get_single_ticker(symbol: str) -> Optional[dict]:
    """patch[7]: fetch one ticker directly — ~200x cheaper than get_tickers()."""
    url = f"{pc.BASE_URL}/md/v3/ticker/24hr"
    try:
        resp = requests.get(url, params={"symbol": symbol}, timeout=8)
        data = resp.json()
        if data.get("error") is not None:
            return None
        return data.get("result")
    except Exception:
        return None

def verify_sim_candidate(symbol: str, direction: str, original_score: int, wait_seconds: int = 20) -> Optional[dict]:
    """
    Waits, then re-scans a single symbol to verify the signal is still valid for simulation.
    Performs iterative checks to ensure price action isn't moving against the signal.
    """
    steps = 3
    step_wait = wait_seconds / steps
    initial_price = None
    last_result = None

    tui_log(f"VERIFY: {symbol} ({direction}) for {wait_seconds}s...")

    for i in range(steps):
        time.sleep(step_wait)

        # Fetch fresh ticker — single-symbol endpoint (patch[7])
        try:
            ticker = _get_single_ticker(symbol)
        except Exception as e:
            tui_log(f"FAIL: Error fetching ticker for {symbol}: {e}")
            return None

        if not ticker:
            tui_log(f"FAIL: {symbol} ticker not found during verification.")
            return None

        current_price = float(ticker.get("lastRp") or ticker.get("closeRp") or 0.0)
        if initial_price is None:
            initial_price = current_price

        # Price movement check
        price_change = pc.pct_change(current_price, initial_price)

        if direction == "LONG":
            if price_change < -0.6: # Dropping too much during verification
                tui_log(f"FAIL: {symbol} dropping during verify: {price_change:+.2f}%")
                return None
        else: # SHORT
            if price_change > 0.6: # Pumping too much during verification
                tui_log(f"FAIL: {symbol} pumping during verify: {price_change:+.2f}%")
                return None

        # Re-scan using the appropriate scanner module
        scanner = scanner_long if direction == "LONG" else scanner_short

        # Minimal config for re-scan (using p_bot's constants)
        cfg = {
            "TIMEFRAME": p_bot.TIMEFRAME,
            "MIN_VOLUME": p_bot.MIN_VOLUME,
            "RATE_LIMIT_RPS": p_bot.RATE_LIMIT_RPS,
            "CANDLES": 100
        }

        fresh_result = scanner.analyse(ticker, cfg, enable_ai=False, enable_entity=False)

        if not fresh_result:
            tui_log(f"FAIL: {symbol} no longer qualifies at step {i+1}")
            return None

        fresh_score = fresh_result["score"]

        # Spread check: avoid illiquid assets that may have fake signals
        current_spread = fresh_result.get("spread", 0.0)
        if current_spread is not None and current_spread > 0.25:
            tui_log(f"FAIL: {symbol} spread too high: {current_spread:.2f}%")
            return None

        # RSI Momentum Check: Ensure RSI isn't deep in the "over-exhaustion" zone already
        current_rsi = fresh_result.get("rsi")
        if current_rsi:
            if direction == "LONG" and current_rsi > 70:
                tui_log(f"FAIL: {symbol} RSI {current_rsi:.1f} — overbought after wait.")
                return None
            elif direction == "SHORT" and current_rsi < 30:
                tui_log(f"FAIL: {symbol} RSI {current_rsi:.1f} — oversold after wait.")
                return None

        # Allow 15% score degradation during the iterative check
        if fresh_score < original_score * 0.85:
            tui_log(f"FAIL: {symbol} score dropped: {original_score} -> {fresh_score}")
            return None

        last_result = fresh_result
        tui_log(f"  Step {i+1}/{steps}: {symbol} score {fresh_score} ({price_change:+.2f}%)")

    # Final overextension check - avoid chasing if it moved too far in our direction too fast
    final_change = pc.pct_change(last_result["price"], initial_price)
    if abs(final_change) > 1.5:
        tui_log(f"FAIL: {symbol} overextended ({final_change:+.2f}%) during verify.")
        return None

    tui_log(f"VERIFIED: {symbol} score {last_result['score']} — ready for SIM entry.")
    return last_result

def execute_sim_setup(result: dict, direction: str) -> bool:
    """
    Opens a new simulated position from a scanner result.
    Returns True on success, False if the trade is skipped.
    """
    symbol = result["inst_id"]
    price  = result["price"]
    score  = result["score"]

    with _stop_lock:
        acc = load_paper_account()

        if any(p["symbol"] == symbol for p in acc["positions"]):
            return False

        with _cooldown_lock:
            last_exit_data = LAST_EXIT_TIME.get(symbol)
        
        if last_exit_data:
            last_exit, last_score = last_exit_data
            cooldown_dur = p_bot.get_cooldown_duration(last_score)
            if time.time() - last_exit < cooldown_dur:
                remaining_m = (cooldown_dur - (time.time() - last_exit)) / 60
                print(Fore.YELLOW + f"  [SIM] COOLDOWN: {symbol} — {remaining_m:.1f}m remaining before re-entry")
                return False

        margin_target = 10.0
        margin_to_use = margin_target

        if acc["balance"] < margin_target:
            tui_log(f"MARGIN FAIL: ${margin_target} needed, only ${acc['balance']:.2f} available.")
            return False

        # Dynamic leverage based on score
        current_leverage = p_bot.get_score_leverage(score)
        notional = margin_to_use * current_leverage
        size     = notional / price

        # Deduct margin AND mock taker fee (0.1%)
        fee = notional * 0.001
        acc["balance"] -= (margin_to_use + fee)

        side = "Buy" if direction == "LONG" else "Sell"

        # Calculate 3 TP stages
        q1 = round(size * 0.5, 8)
        q2 = round(size * 0.25, 8)
        q3 = round(size - q1 - q2, 8)
        
        tp1_mult = p_bot.TAKE_PROFIT_PCT * 0.5
        tp2_mult = p_bot.TAKE_PROFIT_PCT * 0.75
        tp3_mult = p_bot.TAKE_PROFIT_PCT
        
        if direction == "LONG":
            stop_px  = price * (1.0 - p_bot.TRAIL_PCT)
            tp1_px   = price * (1.0 + tp1_mult)
            tp2_px   = price * (1.0 + tp2_mult)
            tp3_px   = price * (1.0 + tp3_mult)
            high_water = price
            low_water  = None
        else:
            stop_px  = price * (1.0 + p_bot.TRAIL_PCT)
            tp1_px   = price * (1.0 - tp1_mult)
            tp2_px   = price * (1.0 - tp2_mult)
            tp3_px   = price * (1.0 - tp3_mult)
            high_water = None
            low_water  = price

        new_pos = {
            "symbol":        symbol,
            "side":          side,
            "size":          size,
            "margin":        margin_to_use,
            "leverage":      current_leverage,
            "fee":           fee,
            "entry":         price,
            "pnl":           0.0,
            "stop_price":    stop_px,
            "original_stop": stop_px,
            "take_profit":   tp3_px, # final target
            "tp_stages": [
                {"price": tp1_px, "qty": q1, "hit": False},
                {"price": tp2_px, "qty": q2, "hit": False},
                {"price": tp3_px, "qty": q3, "hit": False},
            ],
            "high_water":    high_water,
            "low_water":     low_water,
            "timestamp":     datetime.datetime.now().isoformat(),
            "entry_time":    datetime.datetime.now().isoformat(),
            "entry_score":   score,
        }

        acc["positions"].append(new_pos)
        save_paper_account(acc)

    arrow = "▲ LONG" if direction == "LONG" else "▼ SHORT"
    tui_log(f"ENTERED {arrow} {symbol} @ {price} (Score: {score})")


    # --- Entity API Logging ---
    trade_id = f"sim-tr-{symbol}-{int(time.time())}"
    p_bot.make_entity_request("Trade", data={
        "trade_id": trade_id,
        "symbol": symbol,
        "direction": direction,
        "trade_mode": "SIMULATION",
        "entry_price": price,
        "margin_usdt": margin_to_use,
        "score": score,
        "entry_time": datetime.datetime.now().isoformat()
    })

    p_bot.make_entity_request("MarketContext", data={
        "timestamp": datetime.datetime.now().isoformat(),
        "symbol": symbol,
        "volume_24h": result.get("volume_24h", 0),
        "funding_rate": result.get("funding_rate", 0),
        "volatility_atr": result.get("atr", 0)
    })

    p_bot.log_trade({
        "timestamp": new_pos["timestamp"],
        "symbol":    symbol,
        "direction": direction,
        "price":     price,
        "qty":       str(size),
        "leverage":  current_leverage,
        "score":     score,
        "status":    "simulated_entry",
    })

    _subscribe_symbol(symbol)
    _ensure_ws_started()

    with _display_lock:
        global _display_thread_running
        if not _display_thread_running:
            _display_thread_running = True
            threading.Thread(target=_live_pnl_display, daemon=True).start()

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main Bot Loop
# ─────────────────────────────────────────────────────────────────────────────

# Hoist helper functions out of the loop
def is_fresh(r: dict, now_dt: datetime.datetime) -> bool:
    ts_raw = r.get("scan_timestamp")
    if not ts_raw:
        return True
    try:
        # Handle string or datetime
        ts = datetime.datetime.fromisoformat(ts_raw) if isinstance(ts_raw, str) else ts_raw
        return (now_dt - ts).total_seconds() < RESULT_STALENESS_SECONDS
    except (ValueError, TypeError):
        return True

def on_scan_result(r: dict, direction: str) -> None:
    result_time_raw = r.get("scan_timestamp")
    if result_time_raw:
        try:
            # Parse ISO string back to datetime for comparison
            if isinstance(result_time_raw, str):
                result_time = datetime.datetime.fromisoformat(result_time_raw)
            else:
                result_time = result_time_raw

            if (datetime.datetime.now() - result_time).total_seconds() > RESULT_STALENESS_SECONDS:
                return
        except (ValueError, TypeError):
            pass

    # Move balance and position check inside _fast_track_lock for atomicity
    with _fast_track_lock:
        acc = load_paper_account()
        if acc.get("balance", 0.0) < 10.0:
            return

        current_positions = acc.get("positions", [])
        current_syms = {p["symbol"] for p in current_positions}
        if r["inst_id"] in current_syms or r["inst_id"] in _fast_track_opened:
            return

        if r["score"] < FAST_TRACK_SCORE:
            return

        if time.time() - FAST_TRACK_COOLDOWN.get(r["inst_id"], 0) < FAST_TRACK_COOLDOWN_SECONDS:
            return

        _fast_track_opened.add(r["inst_id"])
        FAST_TRACK_COOLDOWN[r["inst_id"]] = time.time()

    tui_log(f"⚡ FAST-TRACK: {r['inst_id']} score {r['score']}!")

    # ── Wait & Verify ────────────────────────────────────
    verified_result = verify_sim_candidate(r["inst_id"], direction, r["score"])
    if verified_result:
        execute_sim_setup(verified_result, direction)
    else:
        # If verification fails, clear fast-track flag so it can try again normally
        with _fast_track_lock:
            if r["inst_id"] in _fast_track_opened:
                _fast_track_opened.remove(r["inst_id"])


def sim_bot_loop(args) -> None:
    """The main scan-and-execute loop for the simulation bot."""
    cfg = {
        "MIN_VOLUME":     args.min_vol,
        "TIMEFRAME":      args.timeframe,
        "TOP_N":          50,
        "MIN_SCORE":      0,
        "MAX_WORKERS":    args.workers,
        "RATE_LIMIT_RPS": args.rate,
    }

    _migrate_results_to_jsonl()  # patch[6]: migrate JSON→JSONL once
    _ensure_ws_started()
    load_sim_cooldowns()

    acc = load_paper_account()
    for p in acc.get("positions", []):
        _subscribe_symbol(p["symbol"])

    with _display_lock:
        global _display_thread_running
        if not _display_thread_running:
            _display_thread_running = True
            threading.Thread(target=_live_pnl_display, daemon=True).start()

    while True:
        update_pnl_and_stops()

        acc = load_paper_account()
        balance = acc.get("balance", 0.0)
        
        # If we have at least $10, we can potentially open a new position
        if balance >= 10.0:
            tui_log(f"Scanning LIVE market ({args.timeframe})... [Balance: ${balance:.2f}]")
            if SHOW_PROGRESS:
                _display_paused.set()
            t0 = time.time()
            long_r, short_r = p_bot.run_scanner_both(cfg, args, on_result=on_scan_result, show_progress=SHOW_PROGRESS)
            elapsed = time.time() - t0
            if SHOW_PROGRESS:
                _display_paused.clear()
            tui_log(f"Scan complete in {elapsed:.1f}s — L: {len(long_r)}  S: {len(short_r)}")

            now_dt = datetime.datetime.now()
            fresh_long  = [r for r in long_r  if is_fresh(r, now_dt)]
            fresh_short = [r for r in short_r if is_fresh(r, now_dt)]

            # Re-check balance and positions immediately after scan
            acc_updated = load_paper_account()
            balance_updated = acc_updated.get("balance", 0.0)
            in_pos_updated    = {p["symbol"] for p in acc_updated.get("positions", [])}

            # We pass a large number for available_slots since we want no limit other than balance
            candidates = p_bot.pick_candidates(
                fresh_long, fresh_short,
                min_score=args.min_score,
                min_score_gap=args.min_score_gap,
                direction_filter=args.direction,
                in_position=in_pos_updated,
                available_slots=1000, 
            )

            if candidates:
                tui_log(f"Picked {len(candidates)} candidate(s).")
                for res, direction in candidates:
                    # Final check before each entry
                    if load_paper_account().get("balance", 0.0) < 10.0:
                        tui_log("Insufficient balance for further entries ($10 needed).")
                        break
                    
                    verified_result = verify_sim_candidate(res["inst_id"], direction, res["score"])
                    if verified_result:
                        execute_sim_setup(verified_result, direction)
            else:
                tui_log("No qualifying setups found.")

            with _fast_track_lock:
                _fast_track_opened.clear()

        # Dynamic sleep: Check PnL/Stops frequently (5s)
        # If balance is high, we scan every 60s or args.interval.
        current_balance = load_paper_account().get("balance", 0.0)
        if current_balance >= 10.0:
            sleep_interval = min(60, args.interval)
        else:
            sleep_interval = 5
            tui_log(f"Balance low (${current_balance:.2f}). Monitoring PnL...")

        # Wait for a slot to open (via balance increasing) OR for the sleep interval to expire
        _slot_available_event.wait(timeout=sleep_interval)
        _slot_available_event.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Parses arguments and starts the simulation bot."""
    parser = argparse.ArgumentParser(description="Phemex Sim Bot (Paper Trading)")
    parser.add_argument("--interval",       type=int,   default=300)
    parser.add_argument("--min-score",      type=int,   default=125)
    parser.add_argument("--min-score-gap",  type=int,   default=30)
    parser.add_argument("--direction",      default=os.getenv("BOT_DIRECTION", "BOTH"), choices=["LONG", "SHORT", "BOTH"])
    parser.add_argument("--timeframe",      default="4H")
    parser.add_argument("--min-vol",        type=int,   default=1_000_000)
    parser.add_argument("--workers",        type=int,   default=100)
    parser.add_argument("--rate",           type=float, default=100.0)
    parser.add_argument("--no-ai",          action="store_true")
    parser.add_argument("--no-entity",      action="store_true")
    args = parser.parse_args()

    print(Fore.GREEN + Style.BRIGHT + "  🚀 Phemex SIMULATION Bot Starting (Paper Trading)")
    print(f"  Market   : LIVE (api.phemex.com)")
    print(f"  Account  : LOCAL (paper_account.json)")
    print(f"  Balance  : {INITIAL_BALANCE} USDT")
    print(f"  Margin   : $10.0 per trade")
    print(f"  Positions: UNLIMITED (Balance gated)")
    print(f"  Interval : {args.interval}s")
    print(f"  Score    : {args.min_score} (gap: {args.min_score_gap})  Direction: {args.direction}\n")

    # --- Session Entity Start ---
    p_bot.make_entity_request("Session", data={
        "session_id": p_bot.SESSION_ID,
        "session_number": int(SIM_SESSION),
        "start_time": datetime.datetime.now().isoformat(),
        "starting_equity": INITIAL_BALANCE,
        "status": "STARTED"
    })

    try:
        sim_bot_loop(args)
        # --- Session Entity End ---
        p_bot.make_entity_request("Session", method="PUT", entity_id=p_bot.SESSION_ID, data={
            "end_time": datetime.datetime.now().isoformat(),
            "status": "FINISHED"
        })
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n  Bot stopped.")
        p_bot.make_entity_request("Session", method="PUT", entity_id=p_bot.SESSION_ID, data={
            "end_time": datetime.datetime.now().isoformat(),
            "status": "INTERRUPTED"
        })


if __name__ == "__main__":
    main()