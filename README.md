# binance-arb-bot

Scans 50+ Binance **USD-M perpetuals** against their **spot** counterparts every second, finds
perp/spot basis dislocations, and trades them as hedged cash-and-carry positions (long spot + short
perp). Includes a live multi-market dashboard, risk controls, a paper-trading mode (default) and an
opt-in live executor.

## Quick start

```bash
cd ~/Documents/binance-arb-bot
.venv/bin/python run.py --feed sim          # offline demo with synthetic dislocations
.venv/bin/python run.py                     # REAL Binance WebSocket data (REST fallback), PAPER trading, no keys
```

Open <http://127.0.0.1:8765>. Run the tests with `.venv/bin/python -m unittest tests.test_core`.

| Command | What it does |
|---|---|
| `run.py --feed sim` | Offline simulator. Dislocations are synthetic. State kept separately. |
| `run.py` | Live public Binance data over WebSockets (REST fallback), **paper** fills (no orders ever sent). |
| `run.py --feed rest` | REST polling only (1 Hz). |
| `run.py --no-trade` | Monitor only. |
| `run.py --live --testnet` | Real order flow against Binance **testnet**. Needs testnet API keys. |
| `run.py --live` | Real money. Also needs `ARBBOT_LIVE_ACK` (see below). |

Useful flags: `--max-markets N`, `--interval S`, `--min-samples N`, `--port P`.

## The strategy, and what it can honestly deliver

### Core entry formula (all in basis points)

```
B_mid  = (perp_mid − spot_mid) / spot_mid            B_entry = (perp_bid − spot_ask) / spot_ask   (executable)
z      = (B_mid − μ) / σ                              μ, σ = median and 1.4826·MAD of the last 5 min of B_mid
target = μ + 0.5σ + ExitCost                          ExitCost = p75 of recent exit spreads (never below today's)
Net    = (B_entry − target) + ExpectedFunding − Fees − Slippage − RiskBuffer
```

Enter only if `z ≥ 3` **and** `Net ≥ 1 bp` **and** the perp is at/above spot. What makes it conservative:

* **Robust baseline.** Median/MAD, so the dislocation itself can't inflate its own yardstick.
* **Frozen at entry.** For an open trade μ and σ are frozen; convergence means `(B_mid − μ_entry)/σ_entry ≤ 0.5`.
  A rolling baseline that the spike has dragged upward could declare "converged" while the basis never came back.
* **Discrete funding.** Funding counts only for funding events actually crossed during the expected hold, using each
  coin's real interval (in live data 32 of 55 coins settle every 8 h, 22 every 4 h, 1 every hour) and exchange caps.
* **Risk buffer.** A floor (`risk_buffer_bps`: **8 bps** in the shipped `config.toml`, because the first real paper
  trades missed their predictions by 30-180 bps; the code default is 2), rising to the 95th percentile of
  `predicted − realised` P&L over the last 100 trades (capped at 30 bps). If the model keeps over-promising,
  entries dry up until reality matches.
* **Thin-touch guard.** A market whose best bid or ask holds less than `min_touch_depth_usd` (**$300**) is skipped.
  Thin books produce fake signals: AVAUSDT showed a +69 bps "edge" on ~$50-110 of touch liquidity while the spot
  price was simply dumping, and lost 111 bps in 3 seconds.
* **Decide on fresh quotes.** The order-book request takes time and the market keeps moving. After it returns, the
  signal is **re-evaluated on the freshest quotes** and the trade is skipped if it has gone (`SKIP … signal gone`).
  The book request has a 1.5 s timeout, the entry basis recorded is the one **actually obtained** from the fill
  prices, and the position's open time is stamped at the real fill so `min_hold_s` counts real seconds.
* **Size-dependent edge.** Right before entering, both order books are walked (VWAP for the real size) and the
  bot takes the largest of `[100%, 75%, 50%, 35%, 20%]` of the target size whose Net still clears the bar.
* **P&L stop.** Exit when closing now would lose more than the round-trip cost plus 25 bps (real leg prices, funding,
  fees); plus a mid-basis stop, so a momentarily wide spread alone can't trigger it.

With default VIP-0 fees the round trip costs **34 bps** (live mode replaces this with your account's real fee tier),
so only dislocations well above that qualify. The dashboard and `/market BTC` show each market's net edge so you
can see exactly why nothing is firing.

**Where the 34 bps comes from:** `2 × (5 futures + 10 spot) + 4 × 1 slippage`, that is 30 bps of taker fees (entry and
exit on both legs) plus 4 bps of assumed slippage on the four fills. `ExpectedFunding` counts only funding
settlements crossed in the assumed 300 s hold (`expected_hold_s`), so most trades earn none. The baseline needs
`min_samples` = 60 seconds of data before any signal, and uses the last `lookback` = 300 seconds.

**Worked example** (basis points; μ = 3, σ = 2, exit cost 3, funding +1, buffer 8):

```
target = 3 + 0.5·2 + 3 = 7
Executable entry basis 45:  gross = 45 − 7 = 38   Net = 38 + 1 − 34 − 8 = −3   → no trade (a real gap, but too small)
Executable entry basis 60:  gross = 60 − 7 = 53   Net = 53 + 1 − 34 − 8 = 12   → trade (z = (62 − 3)/2 ≈ 30 ≥ 3,  Net ≥ 1)
```

Be realistic about the edge: on liquid USDT perps the basis normally sits within a few bps of spot, and
the rare large gaps are contested by co-located market makers with maker-fee rebates. A bot polling REST
once per second will usually be late to them. Paper mode exists so you can measure the *actual* net P&L
before risking money. Lowering fees (BNB discount, VIP tier, maker orders) is what widens the opportunity
set; edit `[fees]` in `config.toml` to match your account.

Only the **positive-basis** side is traded (buy spot, short perp), because the reverse needs borrowed spot.

## Backtesting: recorder and replay

On a real feed the bot records what it sees to `data/recordings/quotes-YYYY-MM-DD.jsonl.gz` (one gzip file per UTC
day, safe to read while the bot runs, ~100-130 MB/day; the oldest files are deleted beyond `[recorder] max_mb`):
top-of-book quotes for every fresh market each second, mark/index/funding, one **order-book snapshot per second**
(round-robin over all markets), the books fetched at each signal, and the bot's own entries and exits.
The simulator is never recorded. A recording failure disables recording only; it can never affect trading.

```bash
.venv/bin/python backtest.py info                    # what has been recorded
.venv/bin/python backtest.py slippage --sizes 200,1000,5000,25000
.venv/bin/python backtest.py replay                  # the real strategy over the recorded days
.venv/bin/python backtest.py replay --set strategy.z_entry=4 --set fees.futures_taker_bps=4
.venv/bin/python backtest.py replay --sweep strategy.z_entry=2.5,3,4,5     # one row per value
.venv/bin/python backtest.py replay --no-depth       # top-of-book only, to see what depth-aware sizing is worth
```

* **`slippage`** measures book impact per fill (bps vs the touch) by size from the recorded snapshots, the share of
  snapshots too thin to fill at all, the hardest markets, and compares the p75 with your `[fees] slippage_bps`.
* **`replay`** drives the same Engine, Scanner, Risk and PaperExecutor as the live bot on a simulated clock, so a
  parameter change shows exactly what it would have done on those days: trades, win rate, P&L per day, max drawdown,
  exit reasons, and the model error (predicted − realised bps) that feeds the adaptive risk buffer.
* **Honest limits.** Replay sizes and fills entries from the most recent recorded snapshot of that market (≤120 s old,
  `--depth-age`), otherwise top-of-book. Exits fill at top-of-book plus the configured slippage. Latency slippage
  (price moving between decision and fill) can't be measured from recordings, only from live fills, so keep
  `slippage_bps` as a cushion for it. A few days of paper data is anecdote, not proof: sweep widely, then check that
  the parameters you like still work on days they weren't tuned on.

## Swing trading, trend following and Smart Money Concepts (`swingkit/`)

A separate toolkit for the timescale where retail traders can compete: candles of hours to days, positions held days to
weeks, so fees and latency matter far less than they do for arbitrage. It follows the three rules that matter most:
**backtest first, risk only 1-2% per trade, keep a journal.** It is research and paper only: nothing here places an order,
and it shares no state with the arbitrage bot.

```bash
.venv/bin/python swing.py download --days 730            # cache 4h candles + real funding history (BTC ETH SOL BNB XRP)
.venv/bin/python swing.py backtest                       # all three strategies, long and short
.venv/bin/python swing.py backtest --strategy trend --symbols BTCUSDT,ETHUSDT --set trend.slow=100 --no-short
.venv/bin/python swing.py backtest --interval 1d --days 1460 --journal --csv data/trades.csv
.venv/bin/python swing.py scan                           # what the strategies say about the latest closed candle
.venv/bin/python swing.py journal --summary              # also: --last 20, --note 12 "text", --export trades.csv
```

| Strategy | The rule, written out |
|---|---|
| `trend` | Trend following. Close above the 20-candle high while EMA50 > EMA200 (mirror for shorts). Stop 3 ATR away, trailed up as a chandelier; exit on a close beyond the 10-candle low. No profit target: winners run. |
| `swing` | Swing trading. In an EMA50 > EMA200 uptrend, RSI dips to 40 or lower near the EMA50, then turns up with an up candle. Stop below the last 10 candles, target 2R, time stop after 30 candles. |
| `smc` | Smart Money Concepts as mechanical rules: a candle sweeps below the latest confirmed swing low and closes back above it (liquidity sweep); within 10 candles a close breaks the range high (break of structure); the order block is the last down candle before that break; enter when a later up candle dips into the block, stop beyond the block and the sweep, target 2.5R. SMC is a discretionary craft: this is one explicit, testable reading of it, not *the* method. |

Each strategy is written once for longs and run on the sign-flipped price series for shorts, so the two sides cannot drift apart.

**Risk.** Every trade is sized so being stopped out costs `risk_pct` of current equity: 1% by default, and **2% is a hard
ceiling** that no setting can lift (`--risk-pct 3` is refused). Position size is capped by leverage (3x), total open risk
("heat", 5%) and the number of open positions (5). After a 25% drawdown new entries pause for 14 days and then start over
from the current equity, because a halt that never lifts would freeze a strategy for good.

**How the backtester stays honest** (each rule has a test): a strategy decides at a candle's close from candles up to it and
the order fills at the *next* open, so there is no look-ahead (also proven by running every strategy on a prefix and on the
full series and requiring identical decisions); stops and targets are checked inside the candle, the stop wins a tie, and a gap
through a stop fills at the open; every fill pays the taker fee and slippage; funding is the **real Binance funding history**
(longs pay a positive rate, shorts receive it) and a coin without history falls back to a flat charge; positions still open at
the end are closed and marked "end of data". The report always shows what simply holding the coins would have returned.

**Journal.** `data/swing-journal.db` records every trade with the reason it was taken, stop, target, result in R, how it
ended and your own notes; backtest runs (`--journal`) and paper trades live in the same table, tagged by mode.

### Paper trading live (forward testing)

A backtest only tells you about the past. To see whether the same rules hold up on candles that have not happened yet, run them
live on paper. It starts **three paper accounts** (trend, swing, smc), $10,000 each, and risks 1% per trade:

```bash
.venv/bin/python swing.py run                       # alerts to Telegram + the journal; Ctrl-C stops it
.venv/bin/python swing.py run --once                # one refresh, print the status and any setups, and exit
.venv/bin/python run.py --swing                     # the arbitrage bot AND the swing paper accounts in one process
                                                    #   (adds the /setups /swing /journal Telegram commands)
```

* **Which markets.** `run.py --swing` paper-trades the **same markets the arbitrage bot scans** (`--swing-symbols arb`, the
  default; 55 with the standard universe). `swing.py run` defaults to BTC ETH SOL BNB XRP; give it `--symbols top55` (the 55
  most traded USDT perpetuals) or your own comma separated list. The list is **frozen when a paper run starts** and stored in
  the journal, so the forward test cannot change retroactively when the volume ranking moves; `--swing-restart` (or
  `--restart`) begins a new run, with a new start time, on a fresh list (old trades stay in the journal). A market with too
  little history or a failing download is skipped and named (`/swing`, the dashboard), never fatal for the others. Each
  account still risks 1% per trade and holds at most 5 positions / 5% total risk, so 55 markets give more *chances*, not
  more risk.
* **Light on Binance.** It only asks for data when a 4h candle boundary has passed (one small top-up per market, six at a
  time) and only replays when something changed, so it adds almost nothing next to the arbitrage feed.
* **No second engine.** Every poll (2 minutes) the runner replays the real candles from the day paper trading started
  through the *same backtester*, in its live mode, so forward results follow exactly the rules that were backtested. A
  decision is only ever taken on a **closed** candle; the still-forming candle may fill a waiting order at its open (and
  catch a stop that gapped), nothing more.
* **Reported once.** New entries and exits are written to the journal (`mode = paper`) and sent to Telegram, and a setup is
  announced the moment it is decided ("paper entry at the next 4h open"). The journal remembers what was reported, so
  restarting the bot never duplicates a trade or an alert.
* **Telegram (with `run.py --swing`, because only one program may read a bot's messages):** `/setups` setups waiting for
  the next open, `/swing` the three accounts with equity and open trades, `/journal [n]` the paper stats and the last *n*
  trades with the reason for each. `swing.py run` on its own sends alerts but cannot answer commands.
* **On the dashboard.** With `run.py --swing` the web dashboard (local, and the hosted read-only copies once they are
  redeployed) gets a full-width **"Swing paper accounts"** panel: the three accounts (equity, return, open and closed
  trades, win count, average R), the open swing positions with entry, current price, stop, target and P&L in dollars and R,
  the setups waiting for the next candle open with the reason, and the last ten closed trades. Without `--swing` the panel
  says so instead of showing zeros. The data is the same sanitised snapshot (no keys, no controls); after changing
  `web/index.html` rebuild the hosted page with `scripts/build_hosted_dashboard.py` (a test fails if it is out of date).
* **On / off.** The swing panel's *Toolkit ON/OFF* switch (local dashboard) or `/swing off` and `/swing on` in Telegram.
  OFF means the trend, swing and SMC accounts take **no new entries** and announce no setups; positions already open keep
  their stops, targets and exits (the candles keep being read for that, and an exit is still reported). ON again: they
  trade from that moment; the off time is skipped, never back-filled. The switch is saved in the journal (`paper_off`,
  every off window), so it survives a restart, and the replay honours the windows (`run_backtest(..., paused=...)`).
  It only exists when the bot runs with `--swing`.
* **Changing a fund from the web.** On the local dashboard (not the hosted read-only copies) the swing panel has a *Fund
  per account* row: type an amount from $100 to $10,000,000 for trend, swing or smc and press Set (it asks first). Position
  sizes follow the starting fund, so that **one account restarts from now**: its open paper trades are closed at their last
  price (reason "account reset: fund changed"), its stats start again from zero, the other two accounts are untouched, and
  the old trades stay in the journal. The change is saved, so it survives a restart, and is announced on Telegram. It uses
  `POST /api/swing-capital`, guarded by the same per-run token as the pause and flatten buttons.
* **Market panel (local dashboard).** Below the swing accounts, a Binance-style market view of USD-M futures: a candlestick
  **chart** (1m to 1W, MA 7/25/99, volume, crosshair, mouse-wheel zoom) with the swing paper positions on that market drawn
  as entry / stop / target lines; the **order book** (8 levels each side, size and running sum in USDT, spread, last and
  mark price); recent **trades**; **top movers** (24h gainers, losers, volume among liquid USDT perpetuals; click one to
  chart it); **Info** (24h stats, mark/index price, funding rate and countdown, open interest) and **Trading Data**
  (hourly accounts long %, taker buy/sell ratio, open interest). The page asks the bot (`GET /api/market/...`), which
  relays a fixed set of public, unsigned Binance requests with symbol/interval whitelists and 1 to 30 s caches
  (`arbbot/market.py`), so an open page costs roughly 250 request-weight a minute and nothing in it can place an order. It
  only polls while the tab is visible. Binance has no 1-second candles for futures, so the shortest interval is 1m. The
  hosted read-only copies leave the panel out.
* **Paper order ticket (local dashboard).** Beside the order book, a Binance-style ticket trades a separate **paper USD-M
  futures account** (starts with 1,000 paper USDT; the ⇆ button adds or takes out paper money) at live prices. **Nothing
  is ever sent to Binance**: it lives in `arbbot/paper_futures.py`, never touches the arbitrage executor or `live.py`, and
  every Telegram note it sends says so. It offers Limit (GTC / IOC / FOK, BBO = best bid for a buy, best ask for a sell,
  or click a book level to use its price), Market (walks the real order book, optional slippage tolerance) and
  Conditional (stop price on last or mark, releasing a limit or market order); attached TP/SL (a one-cancels-other pair of
  reduce-only stop-market orders); Reduce-Only; isolated margin with leverage 1x to 125x per market (change it while the
  market has no position or order); estimated liquidation price, cost and max size for both sides; a size slider. Below,
  **Positions / Open Orders / Order History / Trade History / Assets** with market close and cancel buttons; the chart
  draws the paper entry, liquidation price and working orders. Fills pay the maker 0.02% / taker 0.05% fees, positions
  pay or receive the real funding at each settlement, and a position is liquidated (losing its isolated margin) when the
  mark price reaches its liquidation price. Simplified on purpose: whole fills only (no partials), a resting limit fills
  when the price reaches it (no queue position), FOK behaves like IOC, one maintenance margin rate (0.5%) for every
  market, a 5 USDT minimum order. State: `data/paper-futures.json`.
* **Run only one of the two at a time.** Both write the same journal; they would agree, but you would get every alert twice.
* **It is paper only and separate from the arbitrage book.** If the candle download fails the runner logs it and retries
  (30 s, doubling to 15 min); it can never stop the arbitrage bot that shares the process.

### What the first real backtest said (2 years of 4h candles, BTC ETH SOL BNB XRP, run on 2026-10-24)

| Strategy | Trades | Win % | Expectancy | Profit factor | Return | Max drawdown |
|---|---|---|---|---|---|---|
| trend | 368 | 33 | +0.09R | 1.17 | +27.7% | 27.9% |
| swing | 276 | 42 | -0.02R | 0.93 | -9.0% | 25.3% |
| smc | 113 | 30 | -0.04R | 0.93 | -6.0% | 22.0% |
| *holding the coins* | | | | | *+27.6% on average* | |

Read this carefully. Trend following matched buy-and-hold with a drawdown of the same size, which is not an edge; swing and
SMC lost money after costs. One two-year window is one market regime, five coins is a small sample, and nothing was tuned
(deliberately: tuning on the same data you judge on only finds the past). Treat it as the starting line, not a verdict. The
next honest steps are other timeframes and years (`--interval 1d --days 1460`), other coins, and parameter sweeps that
are then checked on data they were not tuned on. A backtest also cannot see how you will behave in a 28% drawdown.

## Risk controls

| Control | Behaviour |
|---|---|
| Paper by default | Real orders need `--live` **and** `execution.mode="live"` in config; mainnet also needs `ARBBOT_LIVE_ACK`. |
| Size limits | Per-position size, max open positions, total exposure cap, ≤50% of top-of-book depth, the largest size whose edge survives the real order book, and a $300 minimum touch depth. |
| Daily loss limit | UTC-day realised loss limit halts entries **and flattens**. Rebuilt from the trade log, so a restart cannot reset it. |
| Kill switch | `touch KILL` in the project folder halts and flattens within a second. Delete the file to re-arm. |
| Fault breaker | 3 consecutive execution errors, or an unhedged leg, latches a halt that needs a restart. |
| Stale data | No signals on quotes older than 3 s; positions are force-closed after 30 s of staleness. |
| Data integrity | Markets with mark ≠ index by >50 bps are skipped (halted spot, thin books); entries need the perp at/above spot. |
| Exits | Convergence against the baseline **frozen at entry** (`(B_mid − μ₀)/σ₀ ≤ 0.5`, after ≥5 s), P&L stop (close-now loss > round-trip cost + 25 bps), mid-basis stop (+25 bps), 15-minute timeout. |
| Dashboard | Binds to localhost; Host/Origin checks; control endpoints need a per-run token. |

## Architecture

```
feeds.py      WsFeed (streams) + RestFeed fallback via HybridFeed | SimFeed (offline)  ->  Board
scanner.py    robust baseline, discrete funding, exit cost, risk buffer, fully-costed net edge  ->  Signal
sizing.py     order-book walk (VWAP), size-dependent edge, REST depth fetch
risk.py       sizing, caps, thin-touch guard, cooldowns, daily loss, kill switch, fault breaker
engine.py     each tick: scan -> exits (frozen baseline, P&L stop) -> entries (fresh-quote re-check) -> snapshot
executor.py   PaperExecutor (book-VWAP fills + fees + slippage)
live.py       LiveExecutor (both legs concurrently, partial-fill rebalancing, real fee tiers, reconcile)
recorder.py   gzip-JSONL market recorder + depth sampler (data/recordings)
backtest.py   (arbbot/backtest.py) info | slippage | replay with --set / --sweep
commands.py   Telegram questions, /pause /resume, and custom paper trades /open /confirm /close (your chat only)
portfolio.py  positions + SQLite persistence (data/<mode>-<env>-<feed>.db)
server.py     aiohttp dashboard + WebSocket snapshots + guarded controls
alerts.py     Telegram notifier (queued, rate-limited, token-safe)
swingkit/     (separate toolkit, see its section) data.py candles + funding | strategies.py trend/swing/smc |
              risk.py 1-2% sizing + drawdown pause | backtest.py | journal.py | paper.py live paper runner | cli.py  <- swing.py
```

`1000SHIB`-style perps are mapped onto the unscaled spot symbol with a price multiplier.

### Data feed

* **WebSocket (default).** Futures: `!markPrice@arr@1s` (mark, index, funding) plus a `@bookTicker` per
  market; spot: a `@bookTicker` per market. Two connections, auto-reconnect with backoff.
* **REST fallback.** If either stream goes quiet for `ws_stale_s` (5 s) the bot polls REST at 1 Hz until the
  streams recover, then hands back. The dashboard shows `WebSocket`, or `REST (fallback)`.
* **Quiet books.** A book stream only emits on change, so while a connection is alive quotes that changed
  within `ws_max_silent_s` are re-confirmed each scan. A symbol silent longer than that goes stale.
* **Speed.** Data is always the latest tick; scan cadence is `feed.interval_s` (try `0.25`). REST weight at 1 Hz
  is ~900/2400 per minute, so keep REST-only scans at 1 s or slower.
* Stream hosts are overridable with `ws_futures_url` / `ws_spot_url` in `config.toml` in case Binance changes
  its endpoints; the testnet defaults are best-known values and worth confirming.

## Telegram alerts

The bot messages you about **signals** (always `LONG spot / SHORT perp`; that is the only side it trades),
**entries and exits** with P&L, **halts / resumes** (manual pause, kill file, daily loss limit), **faults**
(unhedged leg, repeated execution errors, startup mismatch), **feed health** (REST fallback, outage, recovery)
and **start / stop / crash**. You can also **ask it questions** (next section).

1. In Telegram, message **@BotFather** → `/newbot` → copy the token into `.env` as `TELEGRAM_BOT_TOKEN=`.
2. Open your new bot and press **Start**, then run `.venv/bin/python run.py --find-chat-id` and put the id in
   `.env` as `TELEGRAM_CHAT_ID=`.
3. Verify: `.venv/bin/python run.py --test-alert`. Alerts then switch on automatically at startup.

Tuning is in `[alerts]` in `config.toml` (per-category switches, one signal alert per market per 5 minutes,
10 s debounce on feed problems). Messages are queued and rate-limited, failures never affect trading, and the
token is never logged.

### Ask the bot from Telegram

Type in plain words or use the `/` menu (registered automatically):

| You send | You get |
|---|---|
| "any trades long or short in the market?" or `/signals` | Live signals (LONG spot / SHORT perp) or, if none, the closest candidates and why they don't qualify, plus markets where the perp is cheaper than spot (information only) |
| "how is BTC?" or `/market BTC` | Perp/spot prices, basis vs its normal range, z-score, net edge after costs, funding, depth, and which side the trade would be |
| `/status` | Feed health, entries active/halted, open positions, P&L, uptime |
| `/positions` · `/pnl` · `/top 10` | Open positions (one summary each) · profit/loss and last trades · best markets by edge |
| `/positions BTC` (or "show me the BTC position") | **Full detail of one open trade:** AUTO or MANUAL, size, the spot and perp quantities and prices paid, time held, entry basis, fees paid, funding earned, the model's z and expected net at entry, the unwind basis now, P&L if closed now, and the exit rules with how far each is (convergence z, timeout countdown, P&L stop, basis stop) |
| `/pause` · `/resume` | Stop / allow new entries. The pause (also the dashboard's Pause and Flatten buttons) is saved in the state database, so a restart keeps entries paused until you resume; the bot says so in its log, its event list and on Telegram |
| `/setups` · `/swing` · `/journal [n]` | The swing / trend / SMC paper accounts (only with `run.py --swing`; see the swingkit section) |
| `/open BTC 200` → `/confirm` · `/cancel` | A custom **paper** trade (see below) |
| `/close BTC` | Close an open paper position now |

Safety: only your configured chat is answered (everyone else is ignored); commands sent while the bot was
offline are skipped, never replayed; plain words like "stop trading" or "open btc" never change anything (only the
explicit slash commands do); `/resume` cannot override a fault, the kill file or the daily loss limit; and no
command can place a **real** order (the trade commands refuse when the bot runs `--live`). Only **one** running bot
may poll a given Telegram bot token (a second one is detected and backs off). Turn it off with `commands = false`
under `[alerts]`.

### Custom paper trades from Telegram

`/open BTC 200` (market, then size in USD; the size defaults to `trade_notional_usd`) replies with a **preview**
and opens nothing. Send `/confirm` within 60 s to open it, or `/cancel`. Then `/close BTC` closes it whenever you like.

* **Long or short? Both, as one hedged pair.** `/open BTC 200` BUYS $200 of BTC spot (LONG) and SELLS $200 of BTC
  perp (SHORT) at the same time, at the current executable prices with the usual fees and slippage. It is not a bet
  on BTC going up or down: the two legs cancel the price move, and it earns only if the perp's premium over spot
  narrows. A pure long, a pure short, or the reverse pair (short spot / long perp, which needs borrowed spot) are not
  available.
* **Same limits as automatic entries:** it refuses while entries are halted (pause, kill file, daily loss limit,
  fault), when the market has no fresh quotes, is already open, would exceed `max_open_positions` or
  `max_total_notional_usd`, has a thin book (`min_touch_depth_usd`), or the size is more than half the touch depth
  (`depth_fraction`). The refusal says which. It skips only the *signal*, which is the point of choosing by hand.
  The preview is honest: with a flat basis a trade starts about the 34 bps round-trip cost under water.
* **Held until you close it.** The strategy's convergence and 15-minute timeout exits don't apply to a manual trade;
  the protective ones still do (P&L stop, +25 bps mid-basis stop, stale feed, kill file, daily loss limit, flatten).
* **Bookkeeping:** it counts in realised P&L and the daily loss limit like any paper trade, is recorded as exit
  reason `manual-close`, is flagged `manual` on the dashboard snapshot, and never feeds the adaptive risk buffer
  (no model forecast exists for it).
* **Paper only.** In `--live` mode `/open` and `/close` refuse; real orders are never placed from a chat message.

## Hosted read-only dashboard (Vercel)

The bot itself can't run on Vercel (it needs an always-on process, local storage, and Binance blocks many cloud
regions), so Vercel only hosts a **read-only view**: the bot pushes a sanitised snapshot every 10 s to Upstash Redis
and a password-protected Vercel app reads it. No endpoint on your computer is exposed, the page has no controls, and
the app serves nothing until a password is configured. Setup and architecture: `vercel-dashboard/README.md`.
Deployed at <https://arbbot-dashboard.vercel.app> (needs the setup steps before it shows data).

## Running 24/7 (macOS launchd)

`deploy/com.arbbot.plist` starts the bot at login and restarts it if it crashes (non-zero exit); a clean stop
is not restarted. Open positions and the daily-loss total survive restarts (SQLite).

```bash
mkdir -p ~/Library/LaunchAgents && cp deploy/com.arbbot.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.arbbot.plist   # start + enable
launchctl bootout gui/$(id -u)/com.arbbot                                   # stop + disable
tail -f logs/bot.log
```

Caveats: macOS may block a launch agent from reading `~/Documents` until the Python binary is granted
access under *System Settings → Privacy & Security → Files and Folders* (or move the project outside
`~/Documents`); the Mac must be awake and online (use `caffeinate` or Energy settings); and if your DNS
blocks Binance the bot stays safe but blind (feed down, no entries) until it recovers.

## Running without your computer (Render)

`python run.py --hosted` is the container-host mode: **paper only** (`--hosted --live` refuses to start), and the
recorder stays off unless `ARBBOT_RECORD=1`. Instead of the local dashboard (which has pause/flatten controls and is
never exposed) it serves, on `0.0.0.0:$PORT`, the same **read-only** page as the Vercel copy straight from the running
bot at `/` and `/api/snapshot`, behind HTTP Basic auth (user `viewer` or `DASHBOARD_USER`, password
`DASHBOARD_PASSWORD`, at least 12 characters). It fails closed: without a password only `GET /healthz` answers. Failed
logins are delayed and capped at 30 a minute. Entries can still be paused from Telegram (`/pause`, `/resume`). The
`Dockerfile` and `render.yaml` package it.

1. Put the project in a **private** GitHub repository. `.env`, `data/` and `logs/` are in `.gitignore`; check
   `git status` shows no `.env` before pushing. Never make this repo public.
2. In Render: New -> Blueprint -> pick the repo. `render.yaml` sets the **Frankfurt** region (Binance refuses US
   addresses; the region can't be changed later) and asks you for five secrets: `TELEGRAM_BOT_TOKEN`,
   `TELEGRAM_CHAT_ID`, `DASHBOARD_PUSH_URL`, `DASHBOARD_PUSH_TOKEN` (the Upstash **read-write** token; only needed for
   the Vercel copy) and `DASHBOARD_PASSWORD` (the login for the dashboard on Render itself).
3. **Stop the bot on your computer first.** Two instances would both publish to the same page and both poll Telegram.
4. Watch the Render logs for `hosted mode: health check on 0.0.0.0:...` and `hosted dashboard: publishing ...`.

Free vs paid: a free web service sleeps after 15 minutes without inbound requests (point an uptime pinger such as
UptimeRobot at `/healthz` every 5 minutes) and its disk is wiped on every restart, so the paper book and daily-loss
total reset. For history that survives, switch to a paid instance and uncomment the `disk` block in `render.yaml`
(mount `/app/data`).

## Verification status

| Area | Status |
|---|---|
| Scanner, risk, paper executor, persistence, engine cycle, kill switch, new core formula, recorder, replay | Unit-tested (186 tests, including regressions for the real losing trades) |
| Funding intervals/caps, order-book depth parsing, VWAP slippage | **Checked against real Binance** (public endpoints) |
| Partial-fill rebalancing, exit top-ups, fee-tier lookup | Tested against a fake exchange with programmable partial fills; **not run on Binance** |
| WebSocket feed, REST fallback, chunked connections, Telegram, reconciliation | Tested against local mocks that mimic Binance/Telegram message formats |
| REST universe load, WebSocket streams, REST→WS handoff, dashboard, pause/resume | **Run against real Binance** (2h+ paper session, 0 stream errors until DNS dropped; clean degradation afterwards) |
| Strategy on real data | Found and fixed: broken-price markets (halted spot / mark≠index) and entries below spot (see tests) |
| `live.py` orders, preflight balance endpoints, reconciliation endpoints | **Not run against Binance.** Run `--live --testnet` first |
| Telegram delivery to your phone | **Needs your bot token**; use `--test-alert` |

## What real paper trading has taught us

The first real-data paper session (8 trades, −$4.16, one win) exposed bugs that no unit test had caught; each now
has a regression test. Reviewing a trade means reading the recorded quotes around it (`data/recordings`), not just
the summary line.

| Symptom | Cause | Fix |
|---|---|---|
| ERA/SAND "signals" at −138/−75 bps basis | Halted/broken spot made mark ≠ index; rolling mean was itself broken | mark/index gate, entries need perp ≥ spot |
| ZEC: signal +32 bps, filled at −11 bps, lost 33.6 bps | Order-book fetch timed out (4 s); the bot then filled on later quotes but recorded the old signal | re-check on fresh quotes, 1.5 s timeout, real entry basis |
| "held 5 s" after 1 s | open time stamped at tick start, not at fill | stamped at fill |
| AVA: +69 bps predicted, −111 realised | ~$50-110 touch liquidity; spot dumping, perp lagging | $300 minimum touch depth, larger risk buffer |

The saved trade history persists across restarts on purpose (so a restart can't reset the daily loss limit). To start
a fresh paper book, stop the bot and move `data/paper-mainnet-real.db` aside (don't delete it); the next start creates
an empty one.

## Going live (read before using real money)

**`live.py` has not been run against the exchange.** The unit tests cover quantity alignment and the engine
logic, but not Binance's responses. Do these in order:

0. On startup the bot **reconciles** saved positions with Binance (short perp present, spot inventory present,
   no unknown perp positions in tracked markets) and trips a fault if anything disagrees. Use a dedicated
   account or sub-account so unrelated positions are not mistaken for orphans.
1. Create **testnet** keys, put them in `.env` (see `.env.example`), run `run.py --live --testnet`.
2. Fund both wallets: the **spot** wallet needs USDT for the long leg and the **USDⓈ-M futures** wallet
   needs USDT for margin (isolated, `leverage = 2`). The futures account must be in **One-way mode**.
3. Watch several full entry/exit cycles; compare fills and fees with the dashboard.
4. For mainnet use a dedicated API key with **withdrawals disabled** and IP restriction, set
   `ARBBOT_LIVE_ACK=I_UNDERSTAND_THIS_TRADES_REAL_MONEY`, and start with `trade_notional_usd` at the
   exchange minimum.

**Live execution details.** Partial fills are accepted, not treated as errors: after an entry the legs are compared
in delta terms (`spot_qty` vs `perp_qty × multiplier`) and any excess beyond 1% is unwound; exits top up partial
fills (up to 3 orders per leg) and fault if a non-dust remainder is left. Contract multipliers (`1000SHIB` etc.)
are handled on both price and quantity. The account's real taker fees are read at startup. None of this has run
against the exchange yet (see the verification table), so testnet first.

**Not possible yet: backtested slippage.** That needs historical order-book data the bot doesn't have. Instead the
bot measures its own model error live (predicted vs realised P&L) and widens the risk buffer accordingly, and
paper trades fill at the real book VWAP. A recorder plus replay tool would be the next step for true backtests.

Known limits: the WebSocket feed is tested against a local mock of Binance's combined-stream format, not
Binance itself; futures fees and funding are estimated (not queried); spot fees in BNB are estimated; orders
are still REST (no WebSocket order API).

*Not financial advice. Trading carries risk of loss.*
