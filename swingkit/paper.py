"""Live PAPER trading for the swingkit strategies. Nothing here places an order.

The runner does not have a second trading engine. On every poll it replays the real candles, from the day paper trading
started, through the SAME backtester (swingkit.backtest.run_backtest, in its live mode), so forward results follow exactly
the rules that were backtested: decide at a candle's close, fill at the next open, fees + slippage + real funding, 1-2%
risk sizing, the drawdown pause. What the replay reports (a new fill, an exit, a setup waiting for the next open) is
written to the journal and sent to Telegram once; the journal remembers what was already reported, so restarting
the bot never duplicates an alert or a trade.

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
from .data import DAY_MS, INTERVAL_MS, MARKETS, fetch_forming, load_funding, load_history, to_series
from .journal import Journal, format_stats, format_trades
from .risk import RiskCfg
from .strategies import REGISTRY, make

log = logging.getLogger("swing")
WARMUP_BARS = 320          # indicators (EMA200, pivots) need this much history before the paper start date
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")


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

    def __post_init__(self) -> None:
        self.symbols = [s.strip().upper() for s in self.symbols]
        if not self.symbols or any(not SYMBOL_RE.match(s) for s in self.symbols):
            raise ValueError(f"symbols must look like BTCUSDT, got {self.symbols}")
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
    p.add_argument(f"--{prefix}symbols", default=",".join(DEFAULT_SYMBOLS), help="comma separated")
    p.add_argument(f"--{prefix}interval", default="4h", choices=list(INTERVAL_MS))
    p.add_argument(f"--{prefix}strategies", default=",".join(REGISTRY), help=f"any of {', '.join(REGISTRY)}")
    p.add_argument(f"--{prefix}capital", type=float, default=10_000.0, help="paper dollars PER strategy account")
    p.add_argument(f"--{prefix}risk-pct", type=float, default=1.0, help="%% of equity risked per trade (max 2)")
    p.add_argument(f"--{prefix}market", default="futures", choices=list(MARKETS))
    p.add_argument(f"--{prefix}no-short", action="store_true")
    p.add_argument(f"--{prefix}journal-db", default="data/swing-journal.db")
    p.add_argument(f"--{prefix}cache-dir", default="data/klines")


def paper_cfg_from(ns, prefix: str = "") -> PaperCfg:
    key = prefix.replace("-", "_")
    get = lambda name: getattr(ns, f"{key}{name}")  # noqa: E731
    spot = get("market") == "spot"
    return PaperCfg(
        symbols=get("symbols").split(","), interval=get("interval"), strategies=get("strategies").split(","),
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
        self.notify = notify or (lambda text: None)
        self.risk, self.costs = RiskCfg(risk_pct=cfg.risk_pct), CostCfg()
        self.bar_ms = INTERVAL_MS[cfg.interval]
        start = journal.get_state("paper_start_ts")
        if start is None:
            start = str(int(clock() * 1000))
            journal.set_state("paper_start_ts", start)
        self.start_ts = int(start)
        self.results: dict[str, Result] = {}
        self.checked_at = 0.0
        self.last_error = ""
        self.refreshes = 0
        self._funding: dict[str, list[tuple[int, float]]] = {}
        self._funding_at = 0.0
        self._snap: dict | None = None                  # built once per refresh: the dashboard asks every second

    def run_id(self, name: str) -> str:
        return f"paper-{name}-{self.cfg.interval}"

    # ---- one pass: fetch, replay, report -------------------------------------------------------
    async def refresh(self) -> list[str]:
        cfg, now = self.cfg, self.clock()
        now_ms = int(now * 1000)
        days = -(-(WARMUP_BARS * self.bar_ms + max(0, now_ms - self.start_ts)) // DAY_MS) + 1
        data, forming = {}, set()
        for sym in cfg.symbols:
            candles = await load_history(self.client, sym, cfg.interval, days, cfg.market, Path(cfg.cache_dir), now_ms=now_ms)
            live = await fetch_forming(self.client, sym, cfg.interval, cfg.market, now_ms)
            if live is not None and candles and live.ts > candles[-1].ts:
                candles = candles + [live]
                forming.add(sym)
            data[sym] = to_series(candles)
        if cfg.market == "futures" and now - self._funding_at > 3600:    # funding changes every 8 h: hourly is plenty
            for sym in cfg.symbols:
                try:
                    self._funding[sym] = await load_funding(self.client, sym, days + 2, Path(cfg.cache_dir), now_ms=now_ms)
                except Exception as e:
                    log.warning("no funding history for %s (%s); using the flat charge", sym, type(e).__name__)
            self._funding_at = now
        results = await asyncio.to_thread(self._simulate, data, dict(self._funding), forming)
        messages = self._report(results)
        self.results, self.checked_at, self.last_error = results, now, ""
        self._snap = self._build_snapshot()
        self.refreshes += 1
        for m in messages:
            self.notify(m)
        return messages

    def _simulate(self, data, funding, forming) -> dict[str, Result]:
        out = {}
        for name in self.cfg.strategies:
            factory = lambda n=name: make(n, allow_short=self.cfg.allow_short, **self.cfg.params.get(n, {}))  # noqa: E731
            out[name] = run_backtest(data, factory, self.risk, self.costs, self.cfg.capital, self.bar_ms,
                                     funding or None, close_at_end=False, start_ts=self.start_ts - self.bar_ms,
                                     partial_last=set(forming))
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
                key = f"{name}:{sym}:{st.index}"
                if key not in notified:
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
        return dict(interval=c.interval, symbols=list(c.symbols), strategies=list(c.strategies), capital=c.capital,
                    risk_pct=c.risk_pct, start_ts=self.start_ts, market=c.market)

    def _build_snapshot(self) -> dict:
        accounts, open_, setups = [], [], []
        for name, res in self.results.items():
            st = self.journal.stats(mode="paper", strategy=name)
            equity = res.equity[-1][1] if res.equity else self.cfg.capital
            accounts.append(dict(strategy=name, equity=equity, return_pct=(equity / self.cfg.capital - 1) * 100,
                                 open=len(res.open_positions), closed=st["closed"], wins=st["wins"],
                                 win_rate=st["win_rate"], avg_r=st["avg_r"], total_pnl=st["total_pnl"]))
            open_ += [dict(strategy=name, symbol=p["symbol"], side=p["side"], entry=p["entry_px"], stop=p["stop"],
                           target=p["target"], mark=p["mark"], pnl=p["unrealised_usd"], r=p["unrealised_r"],
                           entry_ts=p["entry_ts"], reason=p["reason_in"]) for p in res.open_positions]
            setups += [dict(strategy=name, symbol=sym, side=s.side, near=s.ref_price, stop=s.stop, target=s.target,
                            reason=s.reason) for sym, s in res.pending.items()]
        trades = [dict(id=t["id"], strategy=t["strategy"], symbol=t["symbol"], side=t["side"], entry=t["entry"],
                       exit=t["exit"], pnl=t["pnl_usd"], r=t["r_multiple"], reason=t["reason_out"],
                       opened_at=t["opened_at"], closed_at=t["closed_at"])
                  for t in self.journal.trades(mode="paper", status="closed", limit=10)]
        return dict(self._meta(), loading=False, accounts=accounts, open=open_, setups=setups, trades=trades)

    def snapshot(self) -> dict:
        """What the dashboard shows. Cheap: the heavy part is built once per refresh."""
        base = self._snap if self._snap is not None else dict(self._meta(), loading=True, accounts=[], open=[],
                                                              setups=[], trades=[])
        return dict(base, error=self.last_error,
                    checked_age_s=(self.clock() - self.checked_at) if self.checked_at else None)

    # ---- text for Telegram / console -----------------------------------------------------------
    def _loading(self) -> str | None:
        if self.results:
            return None
        return ("Swing: still loading the first candles." if not self.last_error
                else f"Swing: the last refresh failed ({self.last_error}). Retrying.")

    def setups_text(self) -> str:
        if (msg := self._loading()):
            return msg
        lines = []
        for name, res in self.results.items():
            for sym, st in res.pending.items():
                lines.append(f"🔔 [{name}] {sym} {st.side.upper()}: paper entry at the next {self.cfg.interval} open\n"
                             f"   near {st.ref_price:.6g}   stop {st.stop:.6g}   target {self._target(st.target)}\n"
                             f"   why: {st.reason}")
        if not lines:
            lines.append(f"No setups waiting on the latest {self.cfg.interval} candles of "
                         f"{', '.join(s.removesuffix('USDT') for s in self.cfg.symbols)}. That is the usual answer: "
                         f"these strategies wait for a pattern, they do not trade every candle.")
        lines.append(f"(checked {_ago(self.clock() - self.checked_at)} ago; paper only)")
        if self.last_error:
            lines.append(f"The latest refresh failed: {self.last_error}")
        return "\n".join(lines)

    def status_text(self) -> str:
        if (msg := self._loading()):
            return msg
        lines = [f"📊 Swing paper accounts · {self.cfg.interval} · since {_utc(self.start_ts)} UTC · "
                 f"checked {_ago(self.clock() - self.checked_at)} ago"]
        for name, res in self.results.items():
            equity = res.equity[-1][1] if res.equity else self.cfg.capital
            closed = self.journal.stats(mode="paper", strategy=name)["closed"]
            lines.append(f"• {name}: ${equity:,.2f} ({(equity / self.cfg.capital - 1) * 100:+.1f}%) · "
                         f"{len(res.open_positions)} open · {closed} closed")
            for p in res.open_positions:
                lines.append(f"    {p['symbol']} {p['side'].upper()} in {p['entry_px']:.6g}, now {p['mark']:.6g}, "
                             f"stop {p['stop']:.6g} · {p['unrealised_usd']:+.2f} USD ({p['unrealised_r']:+.2f}R)")
        lines.append(f"Each account starts with ${self.cfg.capital:,.0f} and risks {self.cfg.risk_pct:g}% per trade. "
                     f"Paper only; the arbitrage bot is separate.")
        if self.last_error:
            lines.append(f"The latest refresh failed: {self.last_error}")
        return "\n".join(lines)

    def journal_text(self, n: int = 5) -> str:
        n = max(1, min(n, 15))
        return format_stats(self.journal.stats(mode="paper")) + "\n\n" + format_trades(self.journal.trades(mode="paper", limit=n))
