╔══════════════════════════════════════════════════════════════════╗
║          FangBlenny Bot — Cloud Deployment Guide                 ║
║          Railway + Supabase + Telegram Mini App                  ║
╚══════════════════════════════════════════════════════════════════╝

This guide takes you from zero to a fully running cloud bot in about
20 minutes.  Read each section completely before running commands.

────────────────────────────────────────────────────────────────────
 OVERVIEW
────────────────────────────────────────────────────────────────────

  Railway (cloud server)
    └─ main.py  ← uvicorn starts FastAPI
         ├─ control/api_server.py   ← REST API + serves Mini App HTML
         ├─ control/bot_controller.py  ← spawns live_bot subprocess
         └─ control/supabase_client.py ← pushes data to Supabase

  Supabase (Postgres)
    ├─ trades       — every entry/exit logged
    ├─ positions    — live snapshot (upserted every 10s)
    ├─ signals      — every scanner result
    ├─ bot_state    — singleton: balance, equity, pnl, status
    └─ bot_config   — editable config (Mini App → API → Supabase)

  Telegram
    ├─ Bot (existing) — sends trade alerts (already in live_bot)
    └─ Mini App       — served from Railway URL, opened via bot

────────────────────────────────────────────────────────────────────
 STEP 1 — Prepare your repo
────────────────────────────────────────────────────────────────────

1a. Merge this deployment package into your project root:

    your-project/
    ├── bots/           ← EXISTING (unchanged)
    ├── core/           ← EXISTING (unchanged)
    ├── scanners/       ← EXISTING (unchanged)
    ├── legacy/         ← EXISTING (unchanged)
    ├── p_bot.py        ← EXISTING (unchanged)
    ├── control/        ← NEW
    ├── mini_app/       ← NEW
    ├── main.py         ← NEW
    ├── patch_live_bot.py ← NEW (run once, then delete)
    ├── Dockerfile      ← NEW
    ├── railway.toml    ← NEW
    ├── requirements_deploy.txt ← NEW
    └── .env.example    ← NEW (copy → .env, fill in secrets)

1b. Apply the live_bot patch (adds pause + state file + Supabase hooks):

    python patch_live_bot.py

    Expected output:
      ✅ Patched bots/live_bot.py
         Applied 6 change(s):
         • 1. Added PAUSE_FILE / STATE_FILE constants
         • 2. Added Supabase import block
         • 3. Supabase push wired into log_trade
         • 4. Pause check + state file writer added to bot_loop
         • 5. Added --no-dashboard flag to run subparser
         • 6. NO_DASHBOARD / --no-dashboard handling added to bot_loop

    Run the patch only ONCE.  If you need to re-run it, restore
    bots/live_bot.py from git first.

1c. Commit everything to GitHub (or GitLab):

    git add .
    git commit -m "feat: Railway + Supabase + Telegram Mini App deployment"
    git push

────────────────────────────────────────────────────────────────────
 STEP 2 — Supabase setup
────────────────────────────────────────────────────────────────────

2a. Create a free project at https://supabase.com
    Note your Project URL and service role key:
      Project Settings → API → Project URL
      Project Settings → API → service_role (SECRET — keep safe!)

2b. Run the schema:
    Supabase dashboard → SQL Editor → New query
    Paste the entire contents of supabase_schema.sql → Run

2c. Verify tables were created:
    Table Editor → you should see:
      trades | positions | signals | bot_state | bot_config

────────────────────────────────────────────────────────────────────
 STEP 3 — Railway setup
────────────────────────────────────────────────────────────────────

3a. Create account at https://railway.app
    New Project → Deploy from GitHub repo → select your repo

3b. Railway will detect the Dockerfile automatically.
    Wait for the first build to complete (usually ~2 min).

3c. Add environment variables:
    Railway dashboard → your service → Variables → Raw Editor
    Paste the contents of .env.example, fill in real values:

    PHEMEX_API_KEY=<your key>
    PHEMEX_API_SECRET=<your secret>
    PHEMEX_BASE_URL=https://api.phemex.com
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY=eyJ...
    TG_BOT_TOKEN=<your token>
    TG_CHAT_ID=<your chat id>
    BOT_MARGIN_USDT=50.0
    BOT_MIN_SCORE=125
    NO_DASHBOARD=1

    ⚠ Start with PHEMEX_BASE_URL=https://testnet-api.phemex.com
      until you've confirmed the cloud bot is working correctly.

3d. Set a custom domain (optional but recommended for the Mini App):
    Railway → your service → Settings → Networking → Generate Domain
    You'll get something like: fangblenny-production.up.railway.app

3e. Redeploy after adding variables:
    Railway → Deployments → Redeploy (or push a commit)

3f. Check health:
    curl https://your-app.up.railway.app/health
    → {"ok": true}

────────────────────────────────────────────────────────────────────
 STEP 4 — Telegram Mini App setup
────────────────────────────────────────────────────────────────────

4a. Open @BotFather in Telegram

4b. Create a menu button that opens your Railway URL:
    /mybots → your bot → Bot Settings → Menu Button
    → Set URL: https://your-app.up.railway.app
    → Set text: 📊 Dashboard

4c. Alternatively, create a Web App:
    /newapp → choose your bot
    → Short name: fangblenny
    → URL: https://your-app.up.railway.app
    → Users open it via: https://t.me/your_bot/fangblenny

4d. Test it: open your bot in Telegram → tap the menu button
    You should see the FangBlenny dashboard.

────────────────────────────────────────────────────────────────────
 STEP 5 — Starting the bot from the Mini App
────────────────────────────────────────────────────────────────────

The bot does NOT start automatically on deploy — you control it from
the Mini App (or via the API directly).

From the Mini App dashboard:
  → Tap ▶ START

This calls POST /api/bot/start which spawns:
  python -m bots.live_bot run --no-dashboard

From the command line (e.g. Railway shell):
  curl -X POST https://your-app.up.railway.app/api/bot/start

To start with custom args:
  curl -X POST https://your-app.up.railway.app/api/bot/start \
    -H 'Content-Type: application/json' \
    -d '{"extra_args": ["run", "--direction", "BOTH", "--dry-run"]}'

────────────────────────────────────────────────────────────────────
 STEP 6 — Verify data flowing into Supabase
────────────────────────────────────────────────────────────────────

After starting the bot, within ~30 seconds you should see:
  Supabase → Table Editor → bot_state → 1 row with running=true
  Supabase → Table Editor → signals → rows appearing per scan cycle

If not, check logs:
  Railway → Deployments → your deployment → View Logs
  Or: curl https://your-app.up.railway.app/api/log?lines=50

────────────────────────────────────────────────────────────────────
 API REFERENCE
────────────────────────────────────────────────────────────────────

GET  /health             → {"ok": true}
GET  /api/status         → full bot state (balance, pnl, positions, etc.)
GET  /api/positions      → open positions array
GET  /api/trades         → trade history (?limit=50&offset=0)
GET  /api/signals        → recent signals (?limit=50)
GET  /api/config         → config key/value pairs
POST /api/config         → {"key":"BOT_MIN_SCORE","value":"130"}
GET  /api/blacklist      → blacklisted symbols with reasons
DEL  /api/blacklist/BTC  → remove symbol from blacklist
GET  /api/log            → recent bot log lines (?lines=100)
POST /api/bot/start      → start bot subprocess
POST /api/bot/stop       → stop bot subprocess (SIGTERM)
POST /api/bot/pause      → write pause flag (bot skips scan loops)
POST /api/bot/resume     → remove pause flag

────────────────────────────────────────────────────────────────────
 PERSISTENT STORAGE NOTE
────────────────────────────────────────────────────────────────────

Railway's filesystem is ephemeral — it resets on redeploy.
This means bots/bot_trades.json and bots/bot_blacklist.json are
lost on each deploy.

Mitigation:
  • Trades are pushed to Supabase (persistent) — the Mini App
    reads from Supabase, so trade history survives redeploys.
  • Blacklist is NOT yet persisted to Supabase — this is on the
    roadmap.  For now, re-add blacklist entries after redeploy,
    or store bot_blacklist.json in a Railway volume:
      Railway → your service → Volumes → Add Volume → /app/bots

────────────────────────────────────────────────────────────────────
 TROUBLESHOOTING
────────────────────────────────────────────────────────────────────

Bot not starting:
  → Check Railway logs for Python import errors
  → Confirm all env vars are set (especially PHEMEX_API_KEY)
  → Try dry-run first: POST /api/bot/start with
    {"extra_args":["run","--dry-run","--yes","--no-dashboard"]}

Supabase not receiving data:
  → Confirm SUPABASE_URL and SUPABASE_SERVICE_KEY are set
  → Check policy: the schema uses service_role for all access
  → Test: GET /api/trades — if empty, check Railway logs for
    "Could not initialise Supabase client" warning

Mini App shows blank/error:
  → Confirm the Railway URL is correct in BotFather
  → Check CORS — api_server.py allows all origins by default
  → Open the URL directly in a browser to check it loads

Patch script warning "anchor not found":
  → The live_bot.py may have changed — check the anchor strings
    in patch_live_bot.py against your actual live_bot.py and
    adjust the OLD strings to match.

────────────────────────────────────────────────────────────────────
 SECURITY NOTES
────────────────────────────────────────────────────────────────────

• The API has NO authentication by default — anyone with the URL
  can start/stop your bot.  Recommended hardening:
    1. Add a shared secret header check in api_server.py
    2. Or validate Telegram WebApp initData (skeleton is in the
       _validate_tg_init_data function — wire it to your endpoints)

• Never commit your .env file.  .gitignore it:
    echo ".env" >> .gitignore

• Use Railway's private networking if adding a DB there directly.

• Rotate your Phemex API key if it was ever in plaintext in your
  Termux terminal history.
