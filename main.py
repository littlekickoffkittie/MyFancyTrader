#!/usr/bin/env python3
"""
main.py — Railway entry point
──────────────────────────────
Loads .env, pre-warms the BotController, then starts uvicorn.
The Mini App and REST API are served on $PORT (Railway sets this).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

# ── load .env before anything else ───────────────────────────────
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fangblenny.main")

# ── pre-warm singleton (starts watchers) ──────────────────────────
from control.bot_controller import BotController
BotController.get()
logger.info("BotController initialised")

# ── start API server ──────────────────────────────────────────────
import uvicorn
from control.api_server import app

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info("Starting FangBlenny API on port %s", port)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
