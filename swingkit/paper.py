"""Live PAPER trading for the swingkit strategies. Nothing here places an order.

The runner does not have a second trading engine. When the candles change it replays the real candles, from the day
paper trading started, through the SAME backtester (swingkit.backtest.run_backtest, in its live mode), so forward results
follow exactly the rules that were backtested: decide at a candle's close, fill at the next open, fees + slippage + real
funding, 1-2% risk sizing, the drawdown pause. What the replay reports (a new fill, an exit, a setup waiting for the next
open) is written to the journal and sent to Telegram once; the journal remembers what was already reported, so restarting
the bot never duplicates an alert or a trade.

It is built for many markets (the arbitrage bot's 55):
  * the market list is FROZEN when a paper run starts, so a forward test cannot change retroactively when volume rankings
    move; `restart` begins a new run on a new list;
  * it only talks to Binance at candle boundaries (a new closed candle, a new forming candle), and only replays when
    something actually changed, so it adds almost no load to the arbitrage bot that shares the process;
  * one market that fails or has too little history never stops the others.

Each strategy is its own paper account with `capital` dollars."""
import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .backtest import CostCfg, Result, run_backtest
from .data import (DAY_MS, INTERVAL_MS, MARKETS, fetch_forming, load_funding, load_history, to_series,
                   top_futures_symbols)
from .journal import Journal, format_stats, format_trades
from .models import Candle
from .risk import RiskCfg
from .strategies import REGISTRY, make

log = logging.getLogger("swing")
WARMUP_BARS = 320          # indicators (EMA200, pivots) need this much history before the paper start date
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")
TOP_RE = re.compile(r"^top(\d{1,3})$", re.IGNORECASE)
MIN_CAPITAL, MAX_CAPITAL = 100.0, 10_000_000.0       # a paper account's fund, in dollars
MAX_MARKETS = 100
FETCH_PARALLEL = 6


@dataclass
class PaperCfg:
    symbols: list[str] = field(default_factory=lambda: list(DEFAULT_SYMBOLS))
    interval: str = "4h"
    strategies: list[str] = field(default_factory=lambda: list(REGISTRY))
    market: str = "futures"
    capital: float = 10_000.0
    risk_pct: float = 1.0
    allow_short: bool = True
    poll_s: float = 120.0
    cache_dir: str = "data/klines"
    params: dict = field(default_factory=dict)      # {"trend": {"slow": 100}}
    symbols_auto: bool = False                      # the list came from a rule (arb / topN): a running paper run keeps ITS list
    restart: bool = False                           # begin a NEW paper run (new start time, new market list)

    def __post_init__(self) -> None:
        self.symbols = [s.strip().upper() for s in self.symbols]
        if not self.symbols or any(not SYMBOL_RE.match(s) for s in self.symbols):
            raise ValueError(f"symbols must look like BTCUSDT, got {self.symbols[:5]}")
        if len(self.symbols) > MAX_MARKETS or len(set(self.symbols)) != len(self.symbols):
            raise ValueError(f"give between 1 and {MAX_MARKETS} different markets")
        if self.interval not in INTERVAL_MS:
            raise ValueError(f"interval must be one of {', '.join(INTERVAL_MS)}")
        if self.market not in MARKETS:
            raise ValueError("market must be 'futures' or 'spot'")
        bad = [s for s in self.strategies if s not in REGISTRY]
        if bad or not self.strategies:
            raise ValueError(f"strategies must be among {', '.join(REGISTRY)}, got {self.strategies}")
        if self.poll_s < 30 or self.capital <= 0:
            raise ValueError("poll_s must be at least 30 and capital above 0")
        RiskCfg(risk_pct=self.risk_pct)               # refuses anything above the 2% ceiling
        for name in self.strategies:                  # and a bad parameter now, not on the first candle
            make(name, **self.params.get(name, {}))


def add_paper_args(p, prefix: str = "") -> None:
    """The same options for `swing.py run` (prefix '') and for arbbot's --swing mode (prefix 'swing-')."""
    default = "arb" if prefix else ",".join(DEFAULT_SYMBOLS)
    p.add_argument(f"--{prefix}symbols", default=default,
                   help="comma separated; or top55 = the 55 most traded USDT perpetuals; or arb = the arbitrage bot's "
                        f"own market list (only with run.py --swing). Default: {default}")
    p.add_argument(f"--{prefix}interval", default="4h", choices=list(INTERVAL_MS))
    p.add_argument(f"--{prefix}strategies", default=",".join(REGISTRY), help=f"any of {', '.join(REGISTRY)}")
    p.add_argument(f"--{prefix}capital", type=float, default=10_000.0, help="paper dollars PER strategy account")
    p.add_argument(f"--{prefix}risk-pct", type=float, default=1.0, help="%% of equity risked per trade (max 2)")
    p.add_argument(f"--{prefix}market", default="futures", choices=list(MARKETS))
    p.add_argument(f"--{prefix}no-short", action="store_true")
    p.add_argument(f"--{prefix}restart", action="store_true",
                   help="begin a NEW paper run (new start time and market list); old trades stay in the journal")
    p.add_argument(f"--{prefix}journal-db", default="data/swing-journal.db")
    p.add_argument(f"--{prefix}cache-dir", default="data/klines")


async def resolve_spec(spec: str, client, universe: list[str] | None = None) -> tuple[list[str], bool]:
    """Turn the --symbols text into (markets, came_from_a_rule)."""
    spec = (spec or "").strip()
    if spec.lower() == "arb":
        if not universe:
            raise ValueError("'arb' needs the arbitrage bot's market list (run.py --swing with a real feed); "
                             "elsewhere use top55 or a comma separated list")
        return list(universe), True
    m = TOP_RE.match(spec)
    if m:
        syms = await top_futures_symbols(client, int(m.group(1)))
        if not syms:
            raise ValueError("Binance returned no USDT perpetuals to rank")
        return syms, True
    return [s for s in spec.split(",") if s.strip()], False


def paper_cfg_from(ns, prefix: str = "", symbols: list[str] | None = None, auto: bool = False) -> PaperCfg:
    key = prefix.replace("-", "_")
    get = lambda name: getattr(ns, f"{key}{name}")  # noqa: E731
    spot = get("market") == "spot"
    return PaperCfg(
        symbols=symbols if symbols is not None else get("symbols").split(","), symbols_auto=auto,
        restart=get("restart"), interval=get("interval"), strategies=get("strategies").split(","),
        market=get("market"), capital=get("capital"), risk_pct=get("risk_pct"), allow_short=not (get("no_short") or spot),
        cache_dir=get("cache_dir"))


def _utc(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")


def _ago(seconds: float) -> str:
    seconds = max(0, round(seconds))
    return f"{seconds}s" if seconds < 90 else f"{round(seconds / 60)}m" if seconds < 5400 else f"{seconds / 3600:.1f}h"


class SwingRunner:
    def __init__(self, cfg: PaperCfg, client, journal: Journal, notify: Callable[[str], None] | None = None,
                 clock: Callable[[], float] = time.time):
        self.cfg, self.client, self.journal, self.clock = cfg, client, journal, clock
        self.mirrors: list = []          # arbbot SwingMirrors copying these accounts to the futures testnet, if any
        self.notify = notify or (lambda text: None)
        self.risk, self.costs = RiskCfg(risk_pct=cfg.risk_pct), CostCfg()
        self.bar_ms = INTERVAL_MS[cfg.interval]
        self.symbols, self.tag, self.accounts = self._begin_run()
        self._lock = asyncio.Lock()                               # one replay at a time (the loop and a fund change)
        # when the toolkit was switched OFF: [[from ms, to ms or None while still off]]; no new entries inside these
        self.off_windows: list[list] = json.loads(journal.get_state("paper_off", "[]"))
        self.results: dict[str, Result] = {}
        self.checked_at = 0.0
        self.last_error = ""
        self.refreshes = 0
        self.unavailable: dict[str, str] = {}                     # markets that could not be loaded, and why
        self._funding: dict[str, list[tuple[int, float]]] = {}
        self._funding_at = 0.0
        self._candles: dict[str, list[Candle]] = {}
        self._forming: dict[str, Candle | None] = {}
        self._sig: tuple | None = None                            # what the last replay was computed from
        self._snap: dict | None = None                            # built once per replay: the dashboard asks every second

    def _begin_run(self) -> tuple[list[str], str, dict[str, dict]]:
        """The market list, the run tag and each account's {tag, start_ts, capital}. They are written down when a run starts
        and reused on restarts, so a forward test never changes underneath you (the arbitrage bot's volume ranking moves
        every day). An account gets its own tag and start when its fund is changed (set_capital)."""
        j, cfg = self.journal, self.cfg
        stored = j.get_state("paper_symbols")
        tag, start = j.get_state("paper_tag"), j.get_state("paper_start_ts")
        syms = json.loads(stored) if stored else None
        fresh = cfg.restart or syms is None or tag is None or start is None
        accounts = json.loads(j.get_state("paper_accounts") or "{}")
        if not fresh and not accounts:                            # a run saved before accounts had their own fund
            accounts = {n: dict(tag=tag, start_ts=int(start), capital=cfg.capital) for n in cfg.strategies}
        if not fresh and not cfg.symbols_auto and sorted(syms) != sorted(cfg.symbols):
            ids = [f"paper-{n}-{cfg.interval}-{a['tag']}" for n, a in accounts.items()]
            if j.trades(mode="paper", run_like=ids, limit=1):
                raise ValueError(f"this paper run started on {len(syms)} markets and already has trades; pass --restart "
                                 f"to begin a new run on these {len(cfg.symbols)}")
            fresh = True                                          # nothing has traded yet: adopting the new list loses nothing
        now_ms = int(self.clock() * 1000)
        if fresh:
            tag, syms = str(now_ms // 1000), list(cfg.symbols)
            accounts = {n: dict(tag=tag, start_ts=now_ms, capital=cfg.capital) for n in cfg.strategies}
            j.set_state("paper_symbols", json.dumps(syms))
            j.set_state("paper_tag", tag)
            j.set_state("paper_start_ts", str(now_ms))
        for n in cfg.strategies:                                  # a strategy added to the run later starts now
            accounts.setdefault(n, dict(tag=str(now_ms // 1000), start_ts=now_ms, capital=cfg.capital))
        accounts = {n: accounts[n] for n in cfg.strategies}
        j.set_state("paper_accounts", json.dumps(accounts))
        return list(syms), tag, accounts

    @property
    def start_ts(self) -> int:
        """When the oldest account started (how far back the candles must reach)."""
        return min(a["start_ts"] for a in self.accounts.values())

    def run_id(self, name: str) -> str:
        return f"paper-{name}-{self.cfg.interval}-{self.accounts[name]['tag']}"

    def _run_ids(self) -> list[str]:
        return [self.run_id(n) for n in self.accounts]

    def _coins(self) -> str:
        names = [s.removesuffix("USDT") for s in self.symbols]
        return " ".join(names) if len(names) <= 8 else f"{len(names)} markets"

    # ---- one pass: fetch, replay, report -------------------------------------------------------
    async def _fetch_symbol(self, sym: str, days: int, now_ms: int) -> tuple[list[Candle], Candle | None]:
        """Talks to Binance only when a candle boundary has passed since the last look."""
        cfg, bar = self.cfg, self.bar_ms
        forming_ts = now_ms // bar * bar
        candles = self._candles.get(sym)
        if not candles or candles[-1].ts < forming_ts - bar:     # the newest CLOSED candle is not in hand yet
            candles = await load_history(self.client, sym, cfg.interval, days, cfg.market, Path(cfg.cache_dir), now_ms=now_ms)
            self._candles[sym] = candles
        live = self._forming.get(sym)
        if live is None or live.ts != forming_ts:                 # the current candle's open is not in hand yet
            live = await fetch_forming(self.client, sym, cfg.interval, cfg.market, now_ms)
            self._forming[sym] = live
        return candles, live

    async def refresh(self) -> list[str]:
        async with self._lock:
            return await self._refresh()

    async def _refresh(self) -> list[str]:
        cfg, now = self.cfg, self.clock()
        now_ms = int(now * 1000)
        days = -(-(WARMUP_BARS * self.bar_ms + max(0, now_ms - self.start_ts)) // DAY_MS) + 1
        gate = asyncio.Semaphore(FETCH_PARALLEL)

        async def one(sym):
            async with gate:
                try:
                    candles, live = await self._fetch_symbol(sym, days, now_ms)
                    return sym, candles, live
                except asyncio.CancelledError:
                    raise
                except Exception as e:                            # one bad market must not stop the other 54
                    self.unavailable[sym] = f"{type(e).__name__}: {str(e)[:80]}"
                    return None

        fetched = [r for r in await asyncio.gather(*(one(s) for s in self.symbols)) if r]
        if not fetched:
            raise RuntimeError("no market data could be loaded: " + next(iter(self.unavailable.values()), "unknown"))
        for sym, _, _ in fetched:
            self.unavailable.pop(sym, None)
        sig = tuple((sym, len(c), c[-1].ts if c else 0, f.ts if f else 0) for sym, c, f in fetched)
        if self.results and sig == self._sig:                     # nothing new since the last replay
            self.checked_at, self.last_error = now, ""
            return []

        data, forming = {}, set()
        for sym, candles, live in fetched:
            full = candles + [live] if live is not None and candles and live.ts > candles[-1].ts else candles
            if full is not candles:
                forming.add(sym)
            data[sym] = to_series(full)
        if cfg.market == "futures" and now - self._funding_at > 3600:    # funding changes every 8 h: hourly is plenty
            async def fund(sym):
                async with gate:
                    try:
                        self._funding[sym] = await load_funding(self.client, sym, days + 2, Path(cfg.cache_dir), now_ms=now_ms)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.warning("no funding history for %s (%s); using the flat charge", sym, type(e).__name__)
            await asyncio.gather(*(fund(s) for s in data))
            self._funding_at = now
        results = await asyncio.to_thread(self._simulate, data, dict(self._funding), forming)
        messages = self._report(results)
        self.results, self.checked_at, self.last_error, self._sig = results, now, "", sig
        self._snap = self._build_snapshot()
        self.refreshes += 1
        for m in messages:
            self.notify(m)
        return messages

    def _simulate(self, data, funding, forming) -> dict[str, Result]:
        out = {}
        for name in self.cfg.strategies:
            factory = lambda n=name: make(n, allow_short=self.cfg.allow_short, **self.cfg.params.get(n, {}))  # noqa: E731
            acct = self.accounts[name]
            out[name] = run_backtest(data, factory, self.risk, self.costs, acct["capital"], self.bar_ms,
                                     funding or None, close_at_end=False, start_ts=acct["start_ts"] - self.bar_ms,
                                     partial_last=set(forming), paused=[tuple(w) for w in self.off_windows])
        return out

    def _report(self, results: dict[str, Result]) -> list[str]:
        """Journal rows for new fills and exits, plus one-time alerts. Idempotent: running it twice reports nothing new."""
        msgs, notified = [], set(json.loads(self.journal.get_state("paper_notified", "[]")))
        tf = self.cfg.interval
        for name, res in results.items():
            rid = self.run_id(name)
            for t in res.trades:
                row = self.journal.find(run_id=rid, strategy=name, symbol=t.symbol, opened_at=t.entry_ts)
                if row is None:
                    self.journal.record(t, mode="paper", run_id=rid, timeframe=tf)
                    msgs.append(self._exit_msg(name, t.symbol, t.side, t.pnl_usd, t.r_multiple, t.reason_out))
                elif row["status"] == "open":
                    self.journal.close_trade(row["id"], closed_at=t.exit_ts, exit=t.exit_px, pnl_usd=t.pnl_usd,
                                             fees_usd=t.fees_usd, r_multiple=t.r_multiple, reason_out=t.reason_out)
                    msgs.append(self._exit_msg(name, t.symbol, t.side, t.pnl_usd, t.r_multiple, t.reason_out))
            for p in res.open_positions:
                if self.journal.find(run_id=rid, strategy=name, symbol=p["symbol"], opened_at=p["entry_ts"]) is None:
                    self.journal.open_trade(
                        mode="paper", strategy=name, symbol=p["symbol"], side=p["side"], timeframe=tf,
                        opened_at=p["entry_ts"], entry=p["entry_px"], stop=p["stop0"], target=p["target"], qty=p["qty"],
                        risk_usd=p["risk_usd"], reason_in=p["reason_in"], run_id=rid)
                    msgs.append(f"📈 SWING PAPER ENTRY [{name}] {p['symbol']} {p['side'].upper()}\n"
                                f"   in {p['entry_px']:.6g}   stop {p['stop0']:.6g}   target {self._target(p['target'])}\n"
                                f"   risk ${p['risk_usd']:.2f} ({self.cfg.risk_pct:g}% of the account)\n   why: {p['reason_in']}")
            for sym, st in res.pending.items():
                key = f"{self.accounts[name]['tag']}:{name}:{sym}:{st.index}"
                if key not in notified and self.enabled:
                    notified.add(key)
                    msgs.append(f"🔔 SWING SETUP [{name}] {sym} {st.side.upper()}: paper entry at the next {tf} open\n"
                                f"   near {st.ref_price:.6g}   stop {st.stop:.6g}   target {self._target(st.target)}\n"
                                f"   why: {st.reason}")
        self.journal.set_state("paper_notified", json.dumps(sorted(notified)[-300:]))
        return msgs

    @staticmethod
    def _target(t) -> str:
        return "-" if t is None else f"{t:.6g}"

    @staticmethod
    def _exit_msg(name, sym, side, pnl, r, why) -> str:
        return f"{'✅' if pnl > 0 else '❌'} SWING PAPER EXIT [{name}] {sym} {side.upper()}: {pnl:+.2f} USD ({r:+.2f}R) · {why}"

    # ---- the loop ------------------------------------------------------------------------------
    async def run(self) -> None:
        backoff = 0.0
        while True:
            try:
                await self.refresh()
                backoff = 0.0
            except asyncio.CancelledError:
                raise
            except Exception as e:   # a failed refresh must never touch the arbitrage bot that shares this process
                self.last_error = f"{type(e).__name__}: {e}"
                backoff = min(max(backoff * 2, 30.0), 900.0)
                log.warning("swing refresh failed (%s); retrying in %.0fs", self.last_error, self.cfg.poll_s + backoff)
            await asyncio.sleep(self.cfg.poll_s + backoff)

    # ---- data for the web dashboard ------------------------------------------------------------
    def _meta(self) -> dict:
        c = self.cfg
        return dict(interval=c.interval, symbols=list(self.symbols), strategies=list(c.strategies), capital=c.capital,
                    risk_pct=c.risk_pct, start_ts=self.start_ts, market=c.market, unavailable=dict(self.unavailable),
                    funds={n: a["capital"] for n, a in self.accounts.items()}, enabled=self.enabled,
                    off_since=self.off_since)

    def _build_snapshot(self) -> dict:
        accounts, open_, setups = [], [], []
        day_start = int(self.clock() // 86_400 * 86_400 * 1000)       # today's UTC midnight, in ms like closed_at
        for name, res in self.results.items():
            acct = self.accounts[name]
            st = self.journal.stats(mode="paper", run_id=self.run_id(name))
            today = [t["pnl_usd"] for t in self.journal.trades(mode="paper", run_id=self.run_id(name), status="closed",
                                                                limit=None) if (t["closed_at"] or 0) >= day_start]
            equity = res.equity[-1][1] if res.equity else acct["capital"]
            accounts.append(dict(strategy=name, equity=equity, return_pct=(equity / acct["capital"] - 1) * 100,
                                 capital=acct["capital"], start_ts=acct["start_ts"],
                                 open=len(res.open_positions), closed=st["closed"], wins=st["wins"],
                                 losses=st["closed"] - st["wins"], win_rate=st["win_rate"], avg_r=st["avg_r"],
                                 total_pnl=st["total_pnl"], today_pnl=sum(today), closed_today=len(today)))
            open_ += [dict(strategy=name, symbol=p["symbol"], side=p["side"], entry=p["entry_px"], stop=p["stop"],
                           target=p["target"], mark=p["mark"], pnl=p["unrealised_usd"], r=p["unrealised_r"],
                           entry_ts=p["entry_ts"], reason=p["reason_in"]) for p in res.open_positions]
            setups += [dict(strategy=name, symbol=sym, side=s.side, near=s.ref_price, stop=s.stop, target=s.target,
                            reason=s.reason) for sym, s in res.pending.items()]
        trades = [dict(id=t["id"], strategy=t["strategy"], symbol=t["symbol"], side=t["side"], entry=t["entry"],
                       exit=t["exit"], pnl=t["pnl_usd"], r=t["r_multiple"], reason=t["reason_out"],
                       opened_at=t["opened_at"], closed_at=t["closed_at"])
                  for t in self.journal.trades(mode="paper", status="closed", run_like=self._run_ids(), limit=10)]
        return dict(self._meta(), loading=False, accounts=accounts, open=open_, setups=setups, trades=trades)

    def pnl_events(self, source: str = "paper") -> list[tuple[float, float, bool]]:
        """Closed trades as (epoch seconds, P&L USD, True) for the P&L reports: the paper accounts' journal, or with
        source="demo" the copies' real results on the futures testnet (after fees, from Binance's fills)."""
        if source == "demo":
            return [(c["closed_ms"] / 1000, c["pnl"], True) for m in self.mirrors for c in m.s["copies"]
                    if c.get("pnl") is not None]
        return [(t["closed_at"] / 1000, t["pnl_usd"], True) for t in
                self.journal.trades(mode="paper", status="closed", run_like=self._run_ids(), limit=None)
                if t["closed_at"] and t["pnl_usd"] is not None]

    def snapshot(self) -> dict:
        """What the dashboard shows. Cheap: the heavy part is built once per replay."""
        base = self._snap if self._snap is not None else dict(self._meta(), loading=True, accounts=[], open=[],
                                                              setups=[], trades=[])
        return dict(base, unavailable=dict(self.unavailable), error=self.last_error,
                    checked_age_s=(self.clock() - self.checked_at) if self.checked_at else None,
                    demo=[m.view() for m in self.mirrors] or None)

    # ---- text for Telegram / console -----------------------------------------------------------
    def _loading(self) -> str | None:
        if self.results:
            return None
        return ("Swing: still loading the first candles." if not self.last_error
                else f"Swing: the last refresh failed ({self.last_error}). Retrying.")

    def _unavailable_line(self) -> str:
        if not self.unavailable:
            return ""
        names = ", ".join(s.removesuffix("USDT") for s in list(self.unavailable)[:8])
        more = f" and {len(self.unavailable) - 8} more" if len(self.unavailable) > 8 else ""
        return f"{len(self.unavailable)} market(s) had no usable candles and are skipped: {names}{more}."

    def setups_text(self) -> str:
        if (msg := self._loading()):
            return msg
        lines = []
        for name, res in self.results.items():
            for sym, st in res.pending.items():
                lines.append(f"🔔 [{name}] {sym} {st.side.upper()}: paper entry at the next {self.cfg.interval} open\n"
                             f"   near {st.ref_price:.6g}   stop {st.stop:.6g}   target {self._target(st.target)}\n"
                             f"   why: {st.reason}")
        if not self.enabled:
            lines.append(f"⏸ The swing toolkit is OFF (since {_utc(self.off_since)} UTC): no new setups or entries. "
                         f"Open positions still follow their stops and exits. /swing on turns it back on.")
        elif not lines:
            lines.append(f"No setups waiting on the latest {self.cfg.interval} candles of {self._coins()}. That is the "
                         f"usual answer: these strategies wait for a pattern, they do not trade every candle.")
        lines.append(f"(checked {_ago(self.clock() - self.checked_at)} ago; paper only)")
        if (u := self._unavailable_line()):
            lines.append(u)
        if self.last_error:
            lines.append(f"The latest refresh failed: {self.last_error}")
        return "\n".join(lines)

    def status_text(self) -> str:
        if (msg := self._loading()):
            return msg
        lines = [f"📊 Swing paper accounts · {self.cfg.interval} · {self._coins()} · since {_utc(self.start_ts)} UTC · "
                 f"checked {_ago(self.clock() - self.checked_at)} ago"]
        for name, res in self.results.items():
            cap = self.accounts[name]["capital"]
            equity = res.equity[-1][1] if res.equity else cap
            closed = self.journal.stats(mode="paper", run_id=self.run_id(name))["closed"]
            lines.append(f"• {name}: ${equity:,.2f} ({(equity / cap - 1) * 100:+.1f}% on ${cap:,.0f}) · "
                         f"{len(res.open_positions)} open · {closed} closed")
            for p in res.open_positions:
                lines.append(f"    {p['symbol']} {p['side'].upper()} in {p['entry_px']:.6g}, now {p['mark']:.6g}, "
                             f"stop {p['stop']:.6g} · {p['unrealised_usd']:+.2f} USD ({p['unrealised_r']:+.2f}R)")
        if not self.enabled:
            lines.insert(1, f"⏸ OFF since {_utc(self.off_since)} UTC: no new entries; open positions keep their stops "
                            f"and exits. /swing on turns it back on.")
        lines.append(f"Each account risks {self.cfg.risk_pct:g}% of its own fund per trade. "
                     f"Paper only; the arbitrage bot is separate.")
        if (u := self._unavailable_line()):
            lines.append(u)
        if self.last_error:
            lines.append(f"The latest refresh failed: {self.last_error}")
        return "\n".join(lines)

    def journal_text(self, n: int = 5) -> str:
        n = max(1, min(n, 15))
        ids = self._run_ids()
        return (format_stats(self.journal.stats(mode="paper", run_like=ids)) + "\n\n"
                + format_trades(self.journal.trades(mode="paper", run_like=ids, limit=n)))

    # ---- changing an account's fund --------------------------------------------------------------
    # ---- switching the toolkit on and off ------------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return not (self.off_windows and self.off_windows[-1][1] is None)

    @property
    def off_since(self) -> int | None:
        return None if self.enabled else int(self.off_windows[-1][0])

    async def set_enabled(self, on: bool) -> str:
        """OFF: the accounts take no new entries and announce no setups; open positions keep their stops, targets and
        exits (the candles keep being read for that). ON again: they trade from that moment; the off time is skipped."""
        async with self._lock:
            if bool(on) == self.enabled:
                return f"The swing toolkit is already {'ON' if on else 'OFF'}."
            now_ms = int(self.clock() * 1000)
            if on:
                self.off_windows[-1][1] = now_ms
            else:
                self.off_windows.append([now_ms, None])
            self.off_windows = self.off_windows[-200:]
            self.journal.set_state("paper_off", json.dumps(self.off_windows))
            self._sig = None                                           # replay with the new windows
            if self.results:
                self._snap = self._build_snapshot()
            msg = ("▶️ Swing toolkit ON: trend, swing and SMC take new entries again from now." if on else
                   "⏸ Swing toolkit OFF: no new entries or setup alerts. Open positions keep their stops and exits.")
            self.notify(msg)
        try:
            await self.refresh()
        except Exception as e:                                         # the switch is saved; the loop retries the data
            self.last_error = f"{type(e).__name__}: {e}"
        return msg

    async def set_capital(self, name: str, amount) -> str:
        """Give one paper account a new fund. The replay fixes position sizes from the starting fund, so the honest way to
        change it is to restart THAT account from now: its open paper positions are closed at their last mark ("account
        reset"), the old trades stay in the journal but leave the account's stats, and the other accounts are untouched."""
        if name not in self.accounts:
            raise ValueError(f"unknown account {name!r}; choose one of {', '.join(self.accounts)}")
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            raise ValueError("the fund must be a number") from None
        if not (MIN_CAPITAL <= amount <= MAX_CAPITAL):                 # also rejects nan and inf
            raise ValueError(f"the fund must be between ${MIN_CAPITAL:,.0f} and ${MAX_CAPITAL:,.0f}")
        async with self._lock:
            old = self.accounts[name]
            if abs(amount - old["capital"]) < 0.005:
                return f"{name} already has ${amount:,.2f}"
            now_ms = int(self.clock() * 1000)
            marks = {(p["symbol"], p["entry_ts"]): p for p in (self.results[name].open_positions if name in self.results else [])}
            for row in self.journal.trades(mode="paper", strategy=name, status="open", run_id=self.run_id(name), limit=None):
                p = marks.get((row["symbol"], row["opened_at"]))
                self.journal.close_trade(
                    row["id"], closed_at=now_ms, exit=p["mark"] if p else row["entry"],
                    pnl_usd=p["unrealised_usd"] if p else 0.0, fees_usd=0.0, r_multiple=p["unrealised_r"] if p else 0.0,
                    reason_out="account reset: fund changed")
            self.accounts[name] = dict(tag=str(max(now_ms // 1000, int(old["tag"]) + 1)), start_ts=now_ms, capital=amount)
            self.journal.set_state("paper_accounts", json.dumps(self.accounts))
            self.results.pop(name, None)
            self._sig = None                                           # force a replay
            self.notify(f"💰 SWING fund [{name}] ${old['capital']:,.0f} → ${amount:,.0f}. The account restarted from now "
                        f"(open paper trades closed at their last price; the old trades stay in the journal).")
            await self._refresh()
        return f"{name} now has ${amount:,.2f}"
