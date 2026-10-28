#!/bin/bash
# Double-click (macOS) or run  ./"Start Binance Bots.command"  (Linux) to start the bots (they keep running after this window closes; stop them with "Stop Binance Bots.command").
#
#   TESTNET bot  - test money. Starts right away. Dashboard http://127.0.0.1:8767
#   MAINNET bot  - REAL MONEY. Only if you say so here: the read-only account check must pass first, then you type the
#                  confirmation yourself. Dashboard http://127.0.0.1:8765
#
# The testnet bot starts with arbitrage entries PAUSED (risk.start_paused); resume from the dashboard or /resume.
# The mainnet bot NEVER opens arbitrage entries (--no-trade, monitor only: signals and alerts; Resume cannot turn
# entries on). Its only real-money orders are the ones you place yourself in the dashboard's order ticket.
# The mainnet dashboard's order ticket trades REAL money (--real-ticket): at most REAL_TICKET_MAX_USD per order, each
# order asks you to confirm, and pausing the arbitrage bot does NOT pause it.
# Only one bot may answer Telegram commands: the mainnet bot when it runs, otherwise the testnet bot.
# --lan: both dashboards also open on your phone (same Wi-Fi) through the "on your phone" links printed below; the
# link carries a private key (data/lan-key-<port>.txt), and any device without it only sees a "locked" page.
cd "$(dirname "$0")" || exit 1
PY=.venv/bin/python
REAL_TICKET_MAX_USD=100               # largest real-money order from the mainnet dashboard's ticket, in USDT
TESTNET="run.py --live --testnet"
MAINNET="run.py --live --config config.toml"
mkdir -p logs

running() { pgrep -f "$1" >/dev/null; }

open_url() {   # Linux: xdg-open (only with a desktop session); macOS: open. Silently skipped on a headless machine.
    # (Not "open" first: on Debian/Ubuntu "open" is openvt, which is not a browser launcher.)
    if [[ "$(uname)" == "Darwin" ]]; then open "$1" >/dev/null 2>&1
    elif [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]] && command -v xdg-open >/dev/null 2>&1; then xdg-open "$1" >/dev/null 2>&1 &
    fi
    return 0
}

wait_for() {   # $1 = log file, $2 = name, $3 = dashboard url, $4 = process pattern
    for _ in $(seq 1 60); do
        if grep -q "dashboard:" "$1" 2>/dev/null; then
            echo "  $2 is up: $3   (log: $1)"
            sleep 1
            grep -h "phone:" "$1" | sed 's/.*phone: /    on your phone (same Wi-Fi): /'
            open_url "$3"
            return 0
        fi
        if ! running "$4"; then
            echo "  $2 STOPPED during startup. Last lines of $1:"
            tail -15 "$1"
            return 1
        fi
        sleep 2
    done
    echo "  $2 is still starting (slow machine?). Watch: tail -f $1"
}

start_testnet() {   # $1 = extra flags
    : > logs/testnet-bot.out           # so wait_for never reads the previous run's lines
    DASHBOARD_PUSH_URL= DASHBOARD_PUSH_TOKEN= nohup "$PY" run.py --live --testnet --config config.testnet.toml \
        --min-samples 20 --swing --swing-mirror trend,swing,smc --swing-mirror-copy-open --lan $1 > logs/testnet-bot.out 2>&1 &
    disown
}

stop_matching() {
    pkill -f "$1"
    for _ in $(seq 1 30); do running "$1" || return 0; sleep 2; done
    echo "  it did not stop in 60 s; stop it with \"Stop Binance Bots.command\" and try again"
    exit 1
}

echo "=== Binance bots ==="
echo

# ---- mainnet: real money, only on an explicit yes ------------------------------------------------------------------
MAIN=no
if running "$MAINNET"; then
    echo "MAINNET (real money) bot is already running: http://127.0.0.1:8765"
    MAIN=running
else
    read -r -p "Also start the REAL-MONEY mainnet bot? [y/N] " answer
    if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
        echo
        echo "Read-only check of your mainnet account (no orders are sent):"
        if "$PY" run.py --check --config config.toml; then
            echo
            echo "!!! The mainnet bot sends REAL orders with REAL money on your Binance account."
            echo "!!! It never opens arbitrage trades by itself (monitor only: signals and alerts)."
            echo "!!! Its dashboard order ticket sends REAL orders right away (up to $REAL_TICKET_MAX_USD USDT each, you confirm each)."
            read -r -p "Type  TRADE REAL MONEY  to start it (anything else skips it): " confirm
            if [[ "$confirm" == "TRADE REAL MONEY" ]]; then
                MAIN=start
            else
                echo "Not confirmed: the mainnet bot was NOT started."
            fi
        else
            echo "The check failed (see above): the mainnet bot was NOT started."
        fi
    fi
fi
echo

# ---- testnet: test money ------------------------------------------------------------------------------------------
if [[ "$MAIN" == "no" ]]; then FLAGS=""; else FLAGS="--no-commands"; fi
if running "$TESTNET"; then
    if [[ "$MAIN" == "start" ]] && ! pgrep -f "$TESTNET.*--no-commands" >/dev/null; then
        echo "Restarting the TESTNET bot so the mainnet bot can take the Telegram commands..."
        stop_matching "$TESTNET"
        start_testnet "$FLAGS"
        wait_for logs/testnet-bot.out "TESTNET bot" http://127.0.0.1:8767 "$TESTNET"
    else
        echo "TESTNET bot is already running: http://127.0.0.1:8767"
    fi
else
    echo "Starting the TESTNET bot (test money)..."
    start_testnet "$FLAGS"
    wait_for logs/testnet-bot.out "TESTNET bot" http://127.0.0.1:8767 "$TESTNET"
fi

# ---- start mainnet last, once the testnet bot has let go of the Telegram commands -------------------------------
if [[ "$MAIN" == "start" ]]; then
    echo "Starting the MAINNET bot (monitor only; REAL-MONEY order ticket)..."
    : > logs/mainnet-bot.out
    ARBBOT_LIVE_ACK=I_UNDERSTAND_THIS_TRADES_REAL_MONEY DASHBOARD_PUSH_URL= DASHBOARD_PUSH_TOKEN= \
        nohup "$PY" run.py --live --config config.toml --no-trade --real-ticket --real-ticket-max-usd "$REAL_TICKET_MAX_USD" --lan \
        > logs/mainnet-bot.out 2>&1 &
    disown
    wait_for logs/mainnet-bot.out "MAINNET bot" http://127.0.0.1:8765 "$MAINNET"
fi

echo
echo "Done. You can close this window; the bots keep running."
echo "Stop them with \"Stop Binance Bots.command\". Emergency stop for every bot: touch KILL in this folder."
