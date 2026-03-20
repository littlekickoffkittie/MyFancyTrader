#!/bin/bash

# Path to the session tracking file
SESSION_FILE=".sim_session_counter"
ACCOUNT_FILE="paper_account.json"
COOLDOWN_FILE="sim_cooldowns.json"
RESULTS_FILE="sim_trade_results.json"

# 1. Read or initialize session number
if [ -f "$SESSION_FILE" ]; then
    SESSION_NUM=$(cat "$SESSION_FILE")
    SESSION_NUM=$((SESSION_NUM + 1))
else
    SESSION_NUM=1
fi

# 2. Save the new session number
echo "$SESSION_NUM" > "$SESSION_FILE"

# 3. Reset the paper account to fresh state ($100 balance, no positions)
echo "{"balance": 100.0, "positions": []}" > "$ACCOUNT_FILE"

# 4. Clear cooldowns and trade results for a truly fresh start
if [ -f "$COOLDOWN_FILE" ]; then rm "$COOLDOWN_FILE"; fi
if [ -f "$RESULTS_FILE" ]; then rm "$RESULTS_FILE"; fi

echo "------------------------------------------------"
echo "  Starting SIM SESSION: $SESSION_NUM (Fresh Start)"
echo "------------------------------------------------"

# 5. Export session number for the bot to pick up
export SIM_SESSION="$SESSION_NUM"

# 6. Start the bot
python3 simulation_bot.py "$@"
