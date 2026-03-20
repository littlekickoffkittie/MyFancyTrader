"""
control/bot_controller.py
─────────────────────────
Manages the live_bot as a subprocess.

start()  → spawn   python -m bots.live_bot [args]
stop()   → SIGTERM the process
pause()  → write .bot_paused flag file   (bot_loop checks it)
resume() → remove .bot_paused flag file

Also:
  • Watches bot_trades.json for new entries → pushes to Supabase
  • Polls bot state from cached state the bot writes → syncs to Supabase
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from control import supabase_client as supa

logger = logging.getLogger("fangblenny.controller")

# ── paths (resolved relative to project root) ─────────────────────
PROJECT_ROOT  = Path(__file__).parent.parent
BOTS_DIR      = PROJECT_ROOT / "bots"
TRADES_FILE   = BOTS_DIR / "bot_trades.json"
BLACKLIST_FILE = BOTS_DIR / "bot_blacklist.json"
PAUSE_FILE    = BOTS_DIR / ".bot_paused"
STATE_FILE    = BOTS_DIR / ".bot_state.json"   # written by patched live_bot


class BotController:
    """Singleton — import and call BotController.get()."""

    _instance: Optional["BotController"] = None
    _lock = threading.Lock()

    # ── singleton ──────────────────────────────────────────────────
    @classmethod
    def get(cls) -> "BotController":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ── init ───────────────────────────────────────────────────────
    def __init__(self):
        self._proc: Optional[subprocess.Popen] = None
        self._proc_lock = threading.Lock()
        self._started_at: Optional[float] = None
        self._known_trade_count: int = self._count_trades()

        # in-memory cache updated by watcher threads
        self._state: Dict[str, Any] = {
            "running": False,
            "paused": False,
            "balance_usdt": 0.0,
            "unrealized_pnl": 0.0,
            "equity": 0.0,
            "open_positions": 0,
            "max_positions": 3,
            "account_halted": False,
            "scan_number": 0,
            "uptime_seconds": 0,
            "started_at": None,
        }
        self._positions: List[Dict] = []

        # start background watchers
        threading.Thread(target=self._watch_trades,    daemon=True).start()
        threading.Thread(target=self._watch_state,     daemon=True).start()
        threading.Thread(target=self._watch_process,   daemon=True).start()

    # ── public control API ─────────────────────────────────────────

    def start(self, extra_args: Optional[List[str]] = None) -> Dict[str, str]:
        with self._proc_lock:
            if self._proc and self._proc.poll() is None:
                return {"status": "already_running"}

            # Remove pause flag if present
            PAUSE_FILE.unlink(missing_ok=True)

            env = {**os.environ}  # inherit full env (has .env loaded by main.py)

            cmd = [sys.executable, "-m", "bots.live_bot", "--no-dashboard"]
            if extra_args:
                cmd.extend(extra_args)

            log_file = open(PROJECT_ROOT / "bot.log", "a")
            self._proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                env=env,
                stdout=log_file,
                stderr=log_file,
            )
            self._started_at = time.time()
            self._state["running"] = True
            self._state["paused"] = False
            self._state["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            logger.info("Bot started — PID %s", self._proc.pid)
            supa.push_bot_state(self._state)
            return {"status": "started", "pid": str(self._proc.pid)}

    def stop(self) -> Dict[str, str]:
        with self._proc_lock:
            if not self._proc or self._proc.poll() is not None:
                self._state["running"] = False
                supa.push_bot_state(self._state)
                return {"status": "not_running"}
            try:
                self._proc.terminate()
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            PAUSE_FILE.unlink(missing_ok=True)
            self._state["running"] = False
            self._state["paused"] = False
            supa.push_bot_state(self._state)
            logger.info("Bot stopped")
            return {"status": "stopped"}

    def pause(self) -> Dict[str, str]:
        PAUSE_FILE.touch()
        self._state["paused"] = True
        supa.push_bot_state(self._state)
        logger.info("Bot paused")
        return {"status": "paused"}

    def resume(self) -> Dict[str, str]:
        PAUSE_FILE.unlink(missing_ok=True)
        self._state["paused"] = False
        supa.push_bot_state(self._state)
        logger.info("Bot resumed")
        return {"status": "resumed"}

    def get_status(self) -> Dict[str, Any]:
        with self._proc_lock:
            alive = self._proc is not None and self._proc.poll() is None
        if self._started_at and alive:
            self._state["uptime_seconds"] = int(time.time() - self._started_at)
        self._state["running"] = alive
        self._state["paused"] = PAUSE_FILE.exists()
        return {**self._state}

    def get_positions(self) -> List[Dict]:
        return list(self._positions)

    def get_trades(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Return from Supabase; fall back to local file."""
        rows = supa.fetch_trades(limit=limit, offset=offset)
        if rows:
            return rows
        return self._read_local_trades(limit=limit, offset=offset)

    def get_blacklist(self) -> Dict:
        if BLACKLIST_FILE.exists():
            try:
                return json.loads(BLACKLIST_FILE.read_text())
            except Exception:
                pass
        return {}

    def remove_from_blacklist(self, symbol: str) -> bool:
        bl = self.get_blacklist()
        if symbol in bl:
            del bl[symbol]
            BLACKLIST_FILE.write_text(json.dumps(bl, indent=2))
            return True
        return False

    def is_running(self) -> bool:
        with self._proc_lock:
            return self._proc is not None and self._proc.poll() is None

    # ── background watchers ────────────────────────────────────────

    def _watch_trades(self):
        """Poll bot_trades.json; push new entries to Supabase."""
        while True:
            try:
                count = self._count_trades()
                if count > self._known_trade_count:
                    trades = self._read_local_trades(limit=count)
                    new = trades[self._known_trade_count:]
                    for t in new:
                        supa.push_trade(t)
                    self._known_trade_count = count
            except Exception as exc:
                logger.debug("watch_trades: %s", exc)
            time.sleep(5)

    def _watch_state(self):
        """Poll STATE_FILE written by the bot; update in-memory cache."""
        while True:
            try:
                if STATE_FILE.exists():
                    data = json.loads(STATE_FILE.read_text())
                    self._state.update({
                        "balance_usdt":   data.get("balance_usdt", 0.0),
                        "unrealized_pnl": data.get("unrealised_pnl", 0.0),
                        "equity":         data.get("equity", 0.0),
                        "open_positions": data.get("open_positions", 0),
                        "max_positions":  data.get("max_positions_allowed", 3),
                        "account_halted": data.get("account_trading_halted", False),
                        "scan_number":    data.get("scan_number", 0),
                    })
                    self._positions = data.get("positions_detail", [])
                    supa.push_bot_state(self._state)
                    supa.push_positions(self._positions)
            except Exception as exc:
                logger.debug("watch_state: %s", exc)
            time.sleep(10)

    def _watch_process(self):
        """Detect when the bot process dies unexpectedly."""
        while True:
            time.sleep(15)
            with self._proc_lock:
                if self._proc and self._proc.poll() is not None:
                    logger.warning("Bot process exited unexpectedly (code %s)", self._proc.returncode)
                    self._state["running"] = False
                    supa.push_bot_state(self._state)
                    self._proc = None

    # ── helpers ───────────────────────────────────────────────────

    def _count_trades(self) -> int:
        if TRADES_FILE.exists():
            try:
                return len(json.loads(TRADES_FILE.read_text()))
            except Exception:
                pass
        return 0

    def _read_local_trades(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        if not TRADES_FILE.exists():
            return []
        try:
            trades = json.loads(TRADES_FILE.read_text())
            trades.reverse()  # newest first
            return trades[offset: offset + limit]
        except Exception:
            return []
