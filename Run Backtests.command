#!/bin/bash
# Double-click to run backtests from a menu. Research only: nothing here places an order or touches your account
# (the swing backtests download public candles and funding history from Binance; the replays read data/recordings).
# Every run is also saved to logs/backtests/ (the report as .txt, swing trades as .csv).
cd "$(dirname "$0")" || exit 1
PY=.venv/bin/python
OUT=logs/backtests
mkdir -p "$OUT"
export PYTHONUNBUFFERED=1            # so the report shows up as it runs, not all at the end

ask() {   # $1 = question, $2 = default; the answer goes to $REPLY
    read -r -p "  $1 [$2]: " REPLY
    REPLY=${REPLY:-$2}
}

run() {   # $1 = short name for the saved file, then the command; shows the output and saves it
    local name=$1; shift
    local file="$OUT/$(date +%Y-%m-%d_%H%M%S)-$name.txt"
    echo
    echo "  > $*"
    echo
    { echo "# $(date)"; echo "# $*"; echo; } > "$file"
    "$@" 2>&1 | tee -a "$file"
    if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
        echo
        echo "  Saved: $file"
    else
        echo
        echo "  The run stopped with an error (see above). Output saved: $file"
    fi
}

recordings() {   # the replay tools need recorded days; say so instead of failing
    if ! ls data/recordings/quotes-*.jsonl.gz >/dev/null 2>&1; then
        echo
        echo "  No recordings yet in data/recordings. The bot records the market while it runs on a real feed;"
        echo "  start it, let it run for a few days, then come back."
        return 1
    fi
    echo
    echo "  Recorded days: $(ls data/recordings/quotes-*.jsonl.gz | sed 's/.*quotes-//; s/.jsonl.gz//' | tr '\n' ' ')"
    echo "  (each day is up to ~100 MB, so this can take a few minutes)"
}

swing_custom() {
    echo
    echo "  Press Enter to keep the value in [brackets]."
    ask "Strategy: trend, swing, smc or all" all; local strategy=$REPLY
    ask "Coins, comma separated" BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT; local symbols=$REPLY
    ask "Candle size: 1h 2h 4h 6h 8h 12h 1d" 4h; local interval=$REPLY
    ask "Days of history" 730; local days=$REPLY
    ask "Risk per trade in % (2 at most)" 1; local risk=$REPLY
    ask "Starting capital in USDT" 10000; local capital=$REPLY
    ask "Also short? y/n" y; local short=$REPLY
    ask "Save the trades to the journal too? y/n" n; local journal=$REPLY
    if ! [[ "$days" =~ ^[0-9]+$ ]]; then echo "  Days must be a whole number."; return; fi
    local stamp; stamp=$(date +%Y-%m-%d_%H%M%S)
    local cmd=("$PY" swing.py backtest --strategy "$strategy" --symbols "$symbols" --interval "$interval"
               --days "$days" --risk-pct "$risk" --capital "$capital" --csv "$OUT/$stamp-trades.csv")
    [[ "$short" == "n" || "$short" == "N" ]] && cmd+=(--no-short)
    [[ "$journal" == "y" || "$journal" == "Y" ]] && cmd+=(--journal)
    run "swing-$strategy-$interval" "${cmd[@]}"
}

while true; do
    echo
    echo "=== Backtesting (research only: no orders, no account access) ==="
    echo
    echo "  Swing toolkit: trend, swing and SMC on past Binance candles"
    echo "    1) All three strategies, 2 years of 4h candles, BTC ETH SOL BNB XRP"
    echo "    2) All three strategies, 4 years of daily candles"
    echo "    3) Choose: strategy, coins, candle size, days, risk, shorts"
    echo "    4) Journal summary (backtests you saved to the journal, and paper trades)"
    echo
    echo "  Arbitrage bot: replay the days it recorded (data/recordings)"
    echo "    5) What has been recorded"
    echo "    6) Replay the strategy over the recorded days"
    echo "    7) Compare settings (sweep), e.g. strategy.z_entry=2.5,3,4,5"
    echo "    8) Slippage: price impact by order size"
    echo
    echo "    q) Quit"
    echo
    read -r -p "Choose: " choice
    case "$choice" in
        1) run swing-all-4h "$PY" swing.py backtest --csv "$OUT/$(date +%Y-%m-%d_%H%M%S)-trades.csv" ;;
        2) run swing-all-1d "$PY" swing.py backtest --interval 1d --days 1460 \
               --csv "$OUT/$(date +%Y-%m-%d_%H%M%S)-trades.csv" ;;
        3) swing_custom ;;
        4) run journal "$PY" swing.py journal --summary ;;
        5) recordings && run replay-info "$PY" backtest.py info ;;
        6) recordings && run replay "$PY" backtest.py replay ;;
        7) if recordings; then
               ask "Setting and values to compare" strategy.z_entry=2.5,3,4,5
               run replay-sweep "$PY" backtest.py replay --sweep "$REPLY"
           fi ;;
        8) if recordings; then
               ask "Order sizes in USDT" 200,1000,5000,25000
               run slippage "$PY" backtest.py slippage --sizes "$REPLY"
           fi ;;
        q|Q) break ;;
        *) echo "  Type a number from the menu, or q." ;;
    esac
done
echo "Results are in $OUT/. You can close this window."
