"""
control/supabase_client.py
──────────────────────────
Thin persistence layer.  All writes are fire-and-forget so they never
block the trading loop.  Failures are logged but swallowed.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger("fangblenny.supabase")

# ── lazy import so missing lib doesn't break startup ─────────────────
_client = None
_client_lock = threading.Lock()


def _get_client():
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_SERVICE_KEY", "")
        if not url or not key:
            logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY not set — Supabase persistence disabled")
            return None
        try:
            from supabase import create_client
            _client = create_client(url, key)
            logger.info("Supabase client initialised → %s", url)
        except Exception as exc:
            logger.warning("Could not initialise Supabase client: %s", exc)
            _client = None
    return _client


def _fire(fn):
    """Run fn in a daemon thread — never block caller."""
    t = threading.Thread(target=fn, daemon=True)
    t.start()


# ── Public API ───────────────────────────────────────────────────────

def push_trade(entry: Dict[str, Any]) -> None:
    """Upsert a trade log entry into the trades table."""
    def _do():
        client = _get_client()
        if not client:
            return
        try:
            row = {
                "timestamp":   entry.get("timestamp"),
                "symbol":      entry.get("symbol"),
                "direction":   entry.get("direction"),
                "status":      entry.get("status"),
                "price":       entry.get("price"),
                "qty":         entry.get("qty"),
                "margin_usdt": entry.get("margin_usdt"),
                "leverage":    entry.get("leverage"),
                "score":       entry.get("score"),
                "pnl":         entry.get("pnl"),
                "reason":      entry.get("reason"),
                "raw":         entry,
            }
            client.table("trades").insert(row).execute()
        except Exception as exc:
            logger.debug("push_trade failed: %s", exc)
    _fire(_do)


def push_signal(signal: Dict[str, Any]) -> None:
    """Upsert a scanner signal into the signals table."""
    def _do():
        client = _get_client()
        if not client:
            return
        try:
            row = {
                "signal_id":           signal.get("signal_id"),
                "timestamp":           signal.get("timestamp"),
                "symbol":              signal.get("symbol"),
                "direction":           signal.get("direction"),
                "raw_score":           signal.get("raw_score"),
                "effective_score":     signal.get("effective_score"),
                "passed_quality_gate": signal.get("passed_quality_gate", False),
                "executed":            signal.get("executed", False),
                "skip_reason":         signal.get("skip_reason"),
            }
            client.table("signals").upsert(row, on_conflict="signal_id").execute()
        except Exception as exc:
            logger.debug("push_signal failed: %s", exc)
    _fire(_do)


def push_positions(positions: List[Dict[str, Any]]) -> None:
    """Overwrite the positions snapshot table."""
    def _do():
        client = _get_client()
        if not client:
            return
        try:
            rows = []
            for p in positions:
                rows.append({
                    "symbol":         p.get("symbol"),
                    "side":           p.get("side"),
                    "direction":      "LONG" if p.get("side") == "Buy" else "SHORT",
                    "qty":            p.get("qty"),
                    "entry_price":    p.get("entry_price"),
                    "mark_price":     p.get("price"),
                    "unrealized_pnl": p.get("pnl"),
                    "leverage":       p.get("leverage"),
                    "stop_price":     p.get("stop_price"),
                    "score":          p.get("score"),
                    "updated_at":     "now()",
                })
            if rows:
                client.table("positions").upsert(rows, on_conflict="symbol").execute()
            # delete symbols no longer open
            open_syms = [p.get("symbol") for p in positions]
            if open_syms:
                client.table("positions").delete().not_.in_("symbol", open_syms).execute()
            else:
                client.table("positions").delete().neq("symbol", "").execute()
        except Exception as exc:
            logger.debug("push_positions failed: %s", exc)
    _fire(_do)


def push_bot_state(state: Dict[str, Any]) -> None:
    """Upsert the singleton bot_state row."""
    def _do():
        client = _get_client()
        if not client:
            return
        try:
            row = {**state, "id": 1, "updated_at": "now()"}
            client.table("bot_state").upsert(row, on_conflict="id").execute()
        except Exception as exc:
            logger.debug("push_bot_state failed: %s", exc)
    _fire(_do)


def fetch_trades(limit: int = 100, offset: int = 0) -> List[Dict]:
    client = _get_client()
    if not client:
        return []
    try:
        res = (client.table("trades")
               .select("*")
               .order("timestamp", desc=True)
               .range(offset, offset + limit - 1)
               .execute())
        return res.data or []
    except Exception as exc:
        logger.debug("fetch_trades failed: %s", exc)
        return []


def fetch_signals(limit: int = 50) -> List[Dict]:
    client = _get_client()
    if not client:
        return []
    try:
        res = (client.table("signals")
               .select("*")
               .order("timestamp", desc=True)
               .limit(limit)
               .execute())
        return res.data or []
    except Exception as exc:
        logger.debug("fetch_signals failed: %s", exc)
        return []


def fetch_config() -> Dict[str, str]:
    client = _get_client()
    if not client:
        return {}
    try:
        res = client.table("bot_config").select("key,value,description").execute()
        return {row["key"]: row for row in (res.data or [])}
    except Exception as exc:
        logger.debug("fetch_config failed: %s", exc)
        return {}


def set_config(key: str, value: str) -> bool:
    client = _get_client()
    if not client:
        return False
    try:
        client.table("bot_config").upsert(
            {"key": key, "value": value, "updated_at": "now()"},
            on_conflict="key"
        ).execute()
        return True
    except Exception as exc:
        logger.debug("set_config failed: %s", exc)
        return False
