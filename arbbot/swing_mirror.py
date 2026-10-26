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
Testnet only, by construction: the ticket refuses any non-testnet client. State: data/swing-mirror-testnet.json.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("swing-mirror")
TRAIL_MIN_MOVE = 0.001          # move the exchange stop only when the paper stop moved at least 0.1%


class SwingMirror:
    def __init__(self, runner, ticket, strategy: str = "trend", fund: float = 10_000.0, path: Path | None = None,
                 notify=None, clock=time.time, poll_s: float = 60.0):
        if strategy not in runner.cfg.strategies:
            raise ValueError(f"the swing runner has no '{strategy}' account (it runs {', '.join(runner.cfg.strategies)})")
        if not fund > 0:
            raise ValueError("the mirror fund must be above 0")
        self.runner, self.ticket, self.strategy, self.fund = runner, ticket, strategy, float(fund)
        self.path, self.notify, self.clock, self.poll_s = path, notify or (lambda text: None), clock, poll_s
        self.s = self._load() or {"start_ts": int(clock() * 1000), "open": {}, "done": {}}
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

    # ---- one pass ---------------------------------------------------------------------------------
    async def step(self) -> None:
        res = self.runner.results.get(self.strategy)
        if res is None:                                   # the swing runner has not replayed yet
            return
        want = {p["symbol"]: p for p in res.open_positions if p["entry_ts"] >= self.s["start_ts"]}
        await self.ticket.refresh()
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
            elif amt == 0:
                del opened[sym]
                self._done(key, "closed on the exchange (its stop fired before the paper exit)")
                self.notify(f"🔁 TESTNET {self.strategy} mirror: {sym} was stopped out on the exchange")
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
                o = await self.ticket.place_order(dict(symbol=sym, side=side, type="market",
                                                       qty=p["qty"] * self.scale(), sl=p["stop"]))
            except ValueError as e:                        # below the minimum size, stop already through the price...
                self._done(key, f"skipped: {e}")
                continue
            if o.get("status") == "filled":
                opened[sym] = {"side": side, "qty": o.get("qty"), "stop": p["stop"], "entry_ts": p["entry_ts"]}
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

    async def run(self) -> None:
        while True:
            try:
                await self.step()
                self.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as e:                         # never stop the bot; try again next round
                self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
                log.warning("%s mirror step failed: %s", self.strategy, self.last_error)
            await asyncio.sleep(self.poll_s)

    def view(self) -> dict:
        return dict(strategy=self.strategy, fund=self.fund, start_ts=self.s["start_ts"], open=dict(self.s["open"]),
                    recent=list(self.s["done"].items())[-10:], error=self.last_error)
