"""Mirror ONE swingkit paper account (e.g. trend) onto the Binance FUTURES TESTNET as real testnet orders.

The paper account stays the source of truth: its signals come from real (mainnet) candles and the replay decides
entries, trailing stops and exits exactly as backtested. The mirror copies what changes onto the testnet account through
the order ticket (arbbot/testnet_ticket.py), so every testnet order goes through the same tested code:

* a NEW paper entry (opened after mirroring started) -> a market order sized as if the account had `fund` dollars,
  with an exchange-side stop-market at the paper stop;
* the paper stop trails -> the exchange stop is moved;
* the paper position exits -> the testnet position is closed; if the exchange stop fired first, that is recorded.

Coins the arbitrage bot holds, or that already have a testnet position, are skipped (one position per coin in one-way
mode). Positions the account held before mirroring started are not copied: entering them now would be a different trade.
Several accounts can be mirrored at once (one SwingMirror each, `--swing-mirror trend,swing,smc`): the first to open a
coin owns it on the testnet and the others skip that trade.
Testnet only, by construction: the ticket refuses any non-testnet client. State: data/swing-mirror-testnet-<account>.json.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

from .binance_client import BinanceError, NotSentError

log = logging.getLogger("swing-mirror")
TRAIL_MIN_MOVE = 0.001          # move the exchange stop only when the paper stop moved at least 0.1%
ACCOUNTS = ("trend", "swing", "smc")


def parse_accounts(spec: str) -> list[str]:
    """--swing-mirror trend | trend,swing,smc | all  ->  the accounts to mirror, in a fixed order (first one wins a
    coin: the testnet account is one-way, so a coin another mirrored account already holds is skipped)."""
    names = list(ACCOUNTS) if spec.strip().lower() == "all" else [s.strip().lower() for s in spec.split(",") if s.strip()]
    bad = [n for n in names if n not in ACCOUNTS]
    if bad or not names:
        raise ValueError(f"--swing-mirror takes {', '.join(ACCOUNTS)} (comma separated) or all, got {spec!r}")
    return [n for n in ACCOUNTS if n in names]


def state_path(data_dir: Path, strategy: str) -> Path:
    """One state file per mirrored account. The trend mirror's older single file is taken over, so its record of what
    was already copied (and must not be copied again) survives the upgrade."""
    path = Path(data_dir) / f"swing-mirror-testnet-{strategy}.json"
    legacy = Path(data_dir) / "swing-mirror-testnet.json"
    if strategy == "trend" and legacy.exists() and not path.exists():
        os.replace(legacy, path)
    return path


class SwingMirror:
    def __init__(self, runner, ticket, strategy: str = "trend", fund: float = 10_000.0, path: Path | None = None,
                 notify=None, clock=time.time, poll_s: float = 60.0, copy_existing: bool = False):
        if strategy not in runner.cfg.strategies:
            raise ValueError(f"the swing runner has no '{strategy}' account (it runs {', '.join(runner.cfg.strategies)})")
        if not fund > 0:
            raise ValueError("the mirror fund must be above 0")
        self.runner, self.ticket, self.strategy, self.fund = runner, ticket, strategy, float(fund)
        self.path, self.notify, self.clock, self.poll_s = path, notify or (lambda text: None), clock, poll_s
        self.copy_existing = copy_existing    # also copy positions the account opened before mirroring started
        self.s = self._load() or {"start_ts": int(clock() * 1000), "open": {}, "done": {}}
        self.s.setdefault("copies", [])       # closed testnet copies with their real result (from Binance's fills)
        self.last_error = ""

    # ---- state ------------------------------------------------------------------------------------
    def _load(self) -> dict | None:
        if not self.path or not Path(self.path).exists():
            return None
        try:
            s = json.loads(Path(self.path).read_text(encoding="utf-8"))
            return s if isinstance(s, dict) and "open" in s else None
        except (OSError, ValueError):
            return None

    def _save(self) -> None:
        if not self.path:
            return
        p = Path(self.path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.s), encoding="utf-8")
        os.replace(tmp, p)

    def _done(self, key: str, why: str, say: bool = True) -> None:
        self.s["done"][key] = why
        if len(self.s["done"]) > 500:
            for k in list(self.s["done"])[:250]:
                del self.s["done"][k]
        if say:
            log.info("%s mirror %s: %s", self.strategy, key, why)

    def scale(self) -> float:
        """Testnet quantity per paper quantity: the testnet copy trades as if the account held `fund` dollars."""
        return self.fund / self.runner.accounts[self.strategy]["capital"]

    async def _enter(self, sym: str, side: str, qty: float, stop: float) -> dict:
        """One market entry, or several when the size is above the largest market order Binance accepts on the coin.
        The first piece carries the exchange stop (it closes the whole position, so one is enough). If a later piece
        fails, what did fill is kept and reported as the copy's size."""
        mx = await self.ticket.max_market_qty(sym) if hasattr(self.ticket, "max_market_qty") else None
        if not mx or qty <= mx:
            return await self.ticket.place_order(dict(symbol=sym, side=side, type="market", qty=qty, sl=stop))
        first = await self.ticket.place_order(dict(symbol=sym, side=side, type="market", qty=mx, sl=stop))
        if first.get("status") != "filled":
            return first
        filled, cost, left = float(first.get("qty") or 0), float(first.get("qty") or 0) * (first.get("filled_px") or 0), qty - mx
        while left > 0:
            try:
                o = await self.ticket.place_order(dict(symbol=sym, side=side, type="market", qty=min(left, mx)))
            except (ValueError, BinanceError) as e:
                log.warning("%s mirror %s: entered %g of %g; the rest was refused: %s", self.strategy, sym, filled, qty, e)
                break
            filled += float(o.get("qty") or 0)
            cost += float(o.get("qty") or 0) * (o.get("filled_px") or 0)
            left -= mx
        return dict(first, qty=filled, filled_px=cost / filled if filled and cost else first.get("filled_px"))

    # ---- results from the testnet's own fills ------------------------------------------------------
    async def _closed(self, sym: str, m: dict, why: str, now_ms: int) -> None:
        rec = dict(account=self.strategy, symbol=sym, side=m["side"], qty=m.get("qty"), entry=m.get("entry"),
                   opened_ms=m.get("opened_ms") or m["entry_ts"], seen_ms=m.get("seen_ms") or m["entry_ts"],
                   closed_ms=now_ms, why=why, exit=None, pnl=None, fees=None)
        self.s["copies"] = (self.s["copies"] + [rec])[-500:]
        await self._settle(rec)

    async def _settle(self, rec: dict) -> None:
        """Fill in what the copy really made on the testnet: Binance's realised P&L on the closing fills, minus the fees
        of the entry and the exit. Until the fills can be read, the result stays unknown (None) and is asked again."""
        rec["tries"] = rec.get("tries", 0) + 1
        out_side = "SELL" if rec["side"] == "buy" else "BUY"
        try:
            end = rec["closed_ms"] + 60_000
            start = max(min(rec["seen_ms"], rec["closed_ms"]) - 60_000, end - 6 * 86_400_000)   # 7-day limit per call
            exits = [f for f in await self.ticket.fills(rec["symbol"], start, end) if f.get("side") == out_side]
            entries = [f for f in await self.ticket.fills(rec["symbol"], rec["opened_ms"] - 60_000,
                                                           rec["opened_ms"] + 300_000) if f.get("side") != out_side]
        except Exception as e:                              # rate limit, network: try again next pass
            log.info("%s mirror: the result of %s is not readable yet (%s)", self.strategy, rec["symbol"], e)
            return
        if not exits:
            return
        usdt = lambda fs: sum(float(f.get("commission") or 0) for f in fs if f.get("commissionAsset", "USDT") == "USDT")  # noqa: E731
        vwap = lambda fs: (sum(float(f["price"]) * float(f["qty"]) for f in fs) / sum(float(f["qty"]) for f in fs)  # noqa: E731
                           if fs else None)
        rec["exit"], rec["entry"] = vwap(exits), vwap(entries) or rec.get("entry")
        rec["fees"] = usdt(exits) + usdt(entries)
        rec["pnl"] = sum(float(f.get("realizedPnl") or 0) for f in exits) - rec["fees"]

    def stats(self) -> dict:
        """The account's testnet results: what really happened on Binance, not the paper replay."""
        done = [c for c in self.s["copies"] if c.get("pnl") is not None]
        day = int(self.clock() // 86_400 * 86_400 * 1000)
        wins = sum(1 for c in done if c["pnl"] > 0)
        open_ = [dict(symbol=sym, **self.ticket.position_info(sym)) for sym in self.s["open"]]
        return dict(open=len(open_), unrealized=sum(p.get("pnl") or 0 for p in open_), closed=len(done), wins=wins,
                    losses=len(done) - wins, realized=sum(c["pnl"] for c in done),
                    today=sum(c["pnl"] for c in done if c["closed_ms"] >= day),
                    unsettled=len(self.s["copies"]) - len(done))

    # ---- one pass ---------------------------------------------------------------------------------
    async def step(self) -> None:
        res = self.runner.results.get(self.strategy)
        if res is None:                                   # the swing runner has not replayed yet
            return
        want = {p["symbol"]: p for p in res.open_positions if self.copy_existing or p["entry_ts"] >= self.s["start_ts"]}
        await self.ticket.refresh()
        now_ms = int(self.clock() * 1000)
        for rec in self.s["copies"]:                      # results Binance could not give yet: ask again
            if rec.get("pnl") is None and rec.get("tries", 0) < 10:
                await self._settle(rec)
        opened = self.s["open"]
        # 1. positions that ended: the paper exited, or the exchange stop fired first
        for sym, m in list(opened.items()):
            amt = self.ticket.position(sym)
            key = f"{sym}:{m['entry_ts']}"
            if sym not in want or want[sym]["entry_ts"] != m["entry_ts"]:
                if amt != 0:
                    await self.ticket.close_position(sym)
                    self.notify(f"🔁 TESTNET {self.strategy} mirror: {sym} closed because the paper position exited")
                del opened[sym]
                self._done(key, f"closed: {self.strategy} exited")
                await self._closed(sym, m, "paper exit", now_ms)
            elif amt == 0:
                del opened[sym]
                self._done(key, "closed on the exchange before the paper exit (its testnet stop fired, or it was "
                                "closed by hand); the paper position stays open")
                self.notify(f"🔁 TESTNET {self.strategy} mirror: {sym} is no longer open on the testnet (stop fired "
                            f"or closed by hand); the {self.strategy} paper position stays open and is not copied again")
                await self._closed(sym, m, "stop or by hand", now_ms)
            else:
                m["seen_ms"] = now_ms                     # still open: a later close happened after this
        # 2. new entries
        busy = self.ticket.arb_symbols()
        for sym, p in want.items():
            key = f"{sym}:{p['entry_ts']}"
            if sym in opened or key in self.s["done"]:
                continue
            if sym in busy:
                self._done(key, "skipped: the arbitrage bot holds this coin")
                continue
            if self.ticket.position(sym) != 0:
                self._done(key, "skipped: the testnet account already has a position on this coin")
                continue
            side = "buy" if p["side"] == "long" else "sell"
            try:
                o = await self._enter(sym, side, p["qty"] * self.scale(), p["stop"])
            except NotSentError:                           # held back by a rate-limit pause: try again next pass
                raise
            except (ValueError, BinanceError) as e:        # below the minimum size, stop already through the price...
                self._done(key, f"skipped: {getattr(e, 'msg', None) or e}")
                continue
            if o.get("status") == "filled":
                opened[sym] = {"side": side, "qty": o.get("qty"), "stop": p["stop"], "entry_ts": p["entry_ts"],
                               "entry": o.get("filled_px"), "opened_ms": now_ms, "seen_ms": now_ms}
                self.notify(f"🔁 TESTNET {self.strategy} mirror: {side.upper()} {sym} {o.get('qty')} with a stop at "
                            f"{p['stop']:.6g} (copy of the paper entry: {p.get('reason_in', '')})")
            else:
                self._done(key, f"skipped: the market order came back {o.get('status')}")
        # 3. trailing stops follow the paper account
        for sym, m in opened.items():
            new = want.get(sym, {}).get("stop")
            if new and abs(new / m["stop"] - 1) >= TRAIL_MIN_MOVE:
                await self.ticket.replace_stop(sym, m["side"], new)
                m["stop"] = new
        self._save()

    async def step_safely(self) -> None:
        try:
            await self.step()
            self.last_error = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:                             # never stop the bot; try again next round
            self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
            log.warning("%s mirror step failed: %s", self.strategy, self.last_error)

    async def run(self) -> None:
        await run_all([self])

    def view(self) -> dict:
        """For the dashboard: this account on the testnet. `status` says, per paper position (symbol:entry_ts), whether
        it is open on the testnet or why not."""
        res = self.runner.results.get(self.strategy)
        status = {}
        for p in (res.open_positions if res else []):
            key, m = f"{p['symbol']}:{p['entry_ts']}", self.s["open"].get(p["symbol"])
            status[key] = ("on demo" if m and m["entry_ts"] == p["entry_ts"] else self.s["done"].get(key)
                           or ("opened before mirroring started: not copied" if not self.copy_existing
                               and p["entry_ts"] < self.s["start_ts"] else "waiting for the next mirror pass"))
        return dict(strategy=self.strategy, fund=self.fund, start_ts=self.s["start_ts"], copy_existing=self.copy_existing,
                    open={sym: dict(m, **self.ticket.position_info(sym)) for sym, m in self.s["open"].items()},
                    status=status, copies=self.s["copies"][-30:], stats=self.stats(),
                    error=self.last_error)


async def run_all(mirrors: list[SwingMirror]) -> None:
    """Step the mirrors one after another, never two at once: two mirrors deciding on the same coin together could both
    see it free and both send an order. Each step starts by re-reading the testnet account, so the next mirror sees the
    previous one's fills."""
    while True:
        for m in mirrors:
            await m.step_safely()
        await asyncio.sleep(min(m.poll_s for m in mirrors))
