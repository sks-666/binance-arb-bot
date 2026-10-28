#!/bin/bash
# Double-click to stop the testnet and mainnet bots. Open positions stay open on Binance (arbitrage pairs stay hedged);
# close them from the dashboard or Telegram /flatten first if you want to be flat.
# Keep the window open when the script ends (or fails), so a terminal opened by double-click does not vanish before
# you can read the output.
pause_on_exit() { if [[ -t 0 ]]; then echo; read -r -p "Press Enter to close this window... "; fi; }
trap pause_on_exit EXIT
cd "$(dirname "$0")" || exit 1

stop() {   # $1 = name, $2 = pattern
    if ! pgrep -f "$2" >/dev/null; then
        echo "$1 bot: not running"
        return
    fi
    pkill -f "$2"
    for _ in $(seq 1 30); do
        pgrep -f "$2" >/dev/null || { echo "$1 bot: stopped"; return; }
        sleep 2
    done
    echo "$1 bot: still running after 60 s"
}

stop "MAINNET (real money)" "run.py --live --config config.toml"
stop "TESTNET" "run.py --live --testnet"
