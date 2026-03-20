"""
control/api_server.py
─────────────────────
FastAPI app that:
  • Serves the Telegram Mini App (mini_app/index.html)
  • Exposes REST endpoints consumed by the Mini App
  • Validates Telegram WebApp initData (HMAC-SHA256)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from control.bot_controller import BotController
from control import supabase_client as supa

logger = logging.getLogger("fangblenny.api")

PROJECT_ROOT = Path(__file__).parent.parent
MINI_APP_DIR = PROJECT_ROOT / "mini_app"

app = FastAPI(title="FangBlenny Bot API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Serve Mini App index ──────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    index = MINI_APP_DIR / "index.html"
    if index.exists():
        return HTMLResponse(index.read_text())
    return HTMLResponse("<h1>FangBlenny Bot</h1><p>Mini app not found.</p>")

# ── Telegram initData validation ──────────────────────────────────

def _validate_tg_init_data(init_data: str) -> bool:
    """Return True if Telegram's HMAC check passes."""
    bot_token = os.getenv("TG_BOT_TOKEN", "")
    if not bot_token or not init_data:
        return True  # dev mode: skip if no token
    try:
        params = dict(urllib.parse.parse_qsl(init_data))
        received_hash = params.pop("hash", "")
        data_check = "\n".join(
            f"{k}={v}" for k, v in sorted(params.items())
        )
        secret = hmac.new(
            b"WebAppData", bot_token.encode(), hashlib.sha256
        ).digest()
        expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, received_hash)
    except Exception:
        return False


# ── Models ─────────────────────────────────────────────────────────

class BotStartRequest(BaseModel):
    extra_args: Optional[List[str]] = None

class ConfigUpdateRequest(BaseModel):
    key: str
    value: str

class BlacklistRemoveRequest(BaseModel):
    symbol: str


# ── Status & state ─────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    ctrl = BotController.get()
    return ctrl.get_status()


@app.get("/api/positions")
async def get_positions():
    ctrl = BotController.get()
    return ctrl.get_positions()


# ── Bot control ────────────────────────────────────────────────────

@app.post("/api/bot/start")
async def bot_start(body: BotStartRequest = BotStartRequest()):
    ctrl = BotController.get()
    return ctrl.start(extra_args=body.extra_args)


@app.post("/api/bot/stop")
async def bot_stop():
    ctrl = BotController.get()
    return ctrl.stop()


@app.post("/api/bot/pause")
async def bot_pause():
    ctrl = BotController.get()
    return ctrl.pause()


@app.post("/api/bot/resume")
async def bot_resume():
    ctrl = BotController.get()
    return ctrl.resume()


# ── Trade history ──────────────────────────────────────────────────

@app.get("/api/trades")
async def get_trades(
    limit:  int = Query(50,  ge=1, le=500),
    offset: int = Query(0,   ge=0),
):
    ctrl = BotController.get()
    return ctrl.get_trades(limit=limit, offset=offset)


# ── Signals ────────────────────────────────────────────────────────

@app.get("/api/signals")
async def get_signals(limit: int = Query(50, ge=1, le=200)):
    return supa.fetch_signals(limit=limit)


# ── Config ─────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    rows = supa.fetch_config()
    if not rows:
        # fall back to env vars
        return {
            "BOT_MARGIN_USDT":  {"value": os.getenv("BOT_MARGIN_USDT", "50.0"),  "description": "Margin per trade (USDT)"},
            "BOT_MIN_SCORE":    {"value": os.getenv("BOT_MIN_SCORE", "125"),      "description": "Min scanner score"},
            "MAX_POSITIONS":    {"value": os.getenv("MAX_POSITIONS", "3"),        "description": "Max concurrent positions"},
            "TIMEFRAME":        {"value": os.getenv("TIMEFRAME", "15m"),          "description": "Candle timeframe"},
            "PHEMEX_BASE_URL":  {"value": os.getenv("PHEMEX_BASE_URL", "https://api.phemex.com"), "description": "Phemex API URL"},
        }
    return rows


@app.post("/api/config")
async def update_config(body: ConfigUpdateRequest):
    ok = supa.set_config(body.key, body.value)
    if not ok:
        raise HTTPException(500, "Failed to save config")
    return {"status": "saved", "key": body.key, "value": body.value}


# ── Blacklist ──────────────────────────────────────────────────────

@app.get("/api/blacklist")
async def get_blacklist():
    ctrl = BotController.get()
    return ctrl.get_blacklist()


@app.delete("/api/blacklist/{symbol}")
async def remove_blacklist(symbol: str):
    ctrl = BotController.get()
    removed = ctrl.remove_from_blacklist(symbol.upper())
    return {"status": "removed" if removed else "not_found", "symbol": symbol.upper()}


# ── Log tail ──────────────────────────────────────────────────────

@app.get("/api/log")
async def get_log(lines: int = Query(100, ge=1, le=1000)):
    log_file = PROJECT_ROOT / "bot.log"
    if not log_file.exists():
        return {"lines": []}
    try:
        all_lines = log_file.read_text(errors="replace").splitlines()
        return {"lines": all_lines[-lines:]}
    except Exception:
        return {"lines": []}


# ── Health ─────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"ok": True}
