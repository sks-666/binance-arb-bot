"""A PAPER USD-M futures account for the dashboard's order ticket. Nothing here ever sends an order to Binance.

Orders are matched against live public Binance prices (last price, best bid/ask, the order book, mark price) with the
futures fee schedule, real funding and isolated-margin liquidation, so the numbers behave like the real thing:

* Limit (GTC / IOC / FOK), Market (walks the order book, optional slippage tolerance), Conditional (a stop price on the
  last or mark price that releases a limit or market order). BBO prices a limit order at the best bid (buy) / ask (sell).
* TP/SL attached to an order become reduce-only stop-market orders once it fills; when one fires the other is cancelled.
* Reduce-only orders can only shrink a position; they are cancelled when the position is gone.
* One-way mode, isolated margin, a leverage per market (changeable while the market has no position or open order).
* A position is liquidated, losing its isolated margin, when the mark price reaches its liquidation price
  (maintenance margin MMR of 0.5%, a simplification of Binance's tiers); funding is paid or received at each settlement.

Simplifications, stated so nobody mistakes this for the exchange: orders fill whole (no partial fills), a resting limit
fills when the price reaches it (no queue position), and FOK behaves like IOC. State lives in data/paper-futures.json.
This module is separate from arbbot/live.py (the real-order executor) and never imports it.
"""
import asyncio
import json
import logging
import math
import os
import re
import time
from pathlib import Path

log = logging.getLogger("paper-futures")
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}USDT$")
MAKER_BPS = 2.0                     # Binance USD-M regular tier: maker 0.02%, taker 0.05% (taker comes from config.toml)
MMR = 0.005                         # maintenance margin rate
MIN_NOTIONAL = 5.0
MAX_LEVERAGE = 125
DEFAULT_LEVERAGE = 15
START_BALANCE = 1_000.0
MAX_BALANCE = 10_000_000.0
QUOTE_MAX_AGE_S = 10.0
TYPES = ("limit", "market", "stop_limit", "stop_market")
TIFS = ("GTC", "IOC", "FOK")
KEEP = 300                          # history rows kept


def _now_ms(clock) -> int:
    return int(clock() * 1000)


def walk_book(levels: list[list[float]], qty: float, limit: float | None = None, buy: bool = True) -> float:
    """Average fill price for `qty` taken from the book. Past `limit` the rest fills at the limit; beyond the book's
    depth it fills at the last level reached pushed 0.1% further, so a huge order is never priced at the top."""
    left, cost, last = qty, 0.0, None
    for px, q in levels:
        if limit is not None and ((buy and px > limit) or (not buy and px < limit)):
            break
        take = min(left, q)
        cost += take * px
        left -= take
        last = px
        if left <= 1e-15:
            return cost / qty
    if limit is not None:
        tail = limit
    else:
        base = last if last is not None else levels[0][0]
        tail = base * (1.001 if buy else 0.999)
    return (cost + left * tail) / qty


def liq_price(qty: float, entry: float, margin: float) -> float | None:
    """Isolated, one-way: the price at which the margin left equals the maintenance margin."""
    if qty == 0:
        return None
    q = abs(qty)
    px = (entry * q - margin) / (q * (1 - MMR)) if qty > 0 else (margin + entry * q) / (q * (1 + MMR))
    return max(px, 0.0)


class PaperFutures:
    def __init__(self, path: Path | None = None, client=None, taker_bps: float = 5.0, notify=None, clock=time.time,
                 poll_s: float = 2.0):
        self.path, self.client, self.clock, self.poll_s = path, client, clock, poll_s
        self.taker, self.maker = taker_bps / 1e4, MAKER_BPS / 1e4
        self.notify = notify or (lambda text: None)
        self.quotes: dict[str, dict] = {}       # symbol -> {last, mark, bid, ask, book?, rate, next_funding, ts}
        self.last_error = ""
        self._lock = asyncio.Lock()
        self.s = self._load() or self._fresh(START_BALANCE)

    # ---- persistence --------------------------------------------------------------------------
    @staticmethod
    def _fresh(balance: float) -> dict:
        return dict(balance=balance, deposits=balance, next_id=1, leverage={}, positions={}, orders=[],
                    order_history=[], trades=[])

    def _load(self) -> dict | None:
        if not self.path or not Path(self.path).exists():
            return None
        try:
            s = json.loads(Path(self.path).read_text(encoding="utf-8"))
            return s if isinstance(s, dict) and "balance" in s else None
        except (OSError, ValueError) as e:
            log.warning("paper futures state unreadable (%s); starting a fresh account", e)
            return None

    def _save(self) -> None:
        if not self.path:
            return
        p = Path(self.path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.s), encoding="utf-8")
        os.replace(tmp, p)                      # atomic: a crash never leaves half a file

    # ---- small helpers ------------------------------------------------------------------------
    def lev(self, sym: str) -> int:
        return int(self.s["leverage"].get(sym, DEFAULT_LEVERAGE))

    def available(self) -> float:
        used = sum(p["margin"] for p in self.s["positions"].values()) + sum(o["reserve"] for o in self.s["orders"])
        return self.s["balance"] - used

    def _quote(self, sym: str) -> dict:
        q = self.quotes.get(sym)
        if not q or self.clock() - q["ts"] > QUOTE_MAX_AGE_S:
            raise ValueError(f"no fresh price for {sym}; try again in a moment")
        return q

    @staticmethod
    def _trigger_px(o: dict, q: dict) -> float:
        return q["mark"] if o.get("trigger") == "mark" else q["last"]

    def _new_order(self, **kw) -> dict:
        now = _now_ms(self.clock)
        o = dict(id=self.s["next_id"], symbol="", side="buy", type="market", qty=0.0, price=None, stop_price=None,
                 trigger="last", tif="GTC", reduce_only=False, tp=None, sl=None, slippage_pct=None, leverage=1,
                 status="new", reason="", created=now, updated=now, reserve=0.0, oco=None, triggered=False, above=None,
                 filled_px=None, fee=0.0, realized=0.0)
        o.update(kw)
        self.s["next_id"] += 1
        return o

    def _archive(self, o: dict, status: str, reason: str = "") -> None:
        o.update(status=status, reason=reason or o.get("reason", ""), updated=_now_ms(self.clock), reserve=0.0)
        if o in self.s["orders"]:
            self.s["orders"].remove(o)
        self.s["order_history"] = (self.s["order_history"] + [dict(o)])[-KEEP:]

    def _cancel_where(self, pred, reason: str) -> None:
        for o in [o for o in self.s["orders"] if pred(o)]:
            self._archive(o, "canceled", reason)

    # ---- placing --------------------------------------------------------------------------------
    def _place(self, req: dict) -> dict:
        sym = str(req.get("symbol", "")).upper()
        if not SYMBOL_RE.match(sym):
            raise ValueError("symbol must look like BTCUSDT")
        side = req.get("side")
        if side not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        typ = req.get("type")
        if typ not in TYPES:
            raise ValueError(f"type must be one of {', '.join(TYPES)}")
        tif = str(req.get("tif") or "GTC").upper()
        if tif not in TIFS:
            raise ValueError("time in force must be GTC, IOC or FOK")
        q = self._quote(sym)
        buy = side == "buy"

        def num(key, required=False):
            v = req.get(key)
            if v in (None, ""):
                if required:
                    raise ValueError(f"{key.replace('_', ' ')} is required")
                return None
            try:
                v = float(v)
            except (TypeError, ValueError):
                raise ValueError(f"{key.replace('_', ' ')} must be a number") from None
            if not math.isfinite(v) or v <= 0:
                raise ValueError(f"{key.replace('_', ' ')} must be above 0")
            return v

        price = None
        if typ in ("limit", "stop_limit"):
            if req.get("bbo") and typ == "limit":
                price = q["bid"] if buy else q["ask"]          # BBO: queue 1 on our own side of the book
            else:
                price = num("price", True)
        stop = num("stop_price", True) if typ.startswith("stop") else None
        trigger = req.get("trigger") or "last"
        if trigger not in ("last", "mark"):
            raise ValueError("trigger must be last or mark")
        ref = price or stop or (q["ask"] if buy else q["bid"])  # the price the size is converted and checked at
        qty, size_usdt = num("qty"), num("size_usdt")
        if (qty is None) == (size_usdt is None):
            raise ValueError("give the size either in USDT or in coins")
        if qty is None:
            qty = size_usdt / ref
        qty = float(f"{qty:.8g}")
        reduce_only = bool(req.get("reduce_only"))
        pos = self.s["positions"].get(sym)
        cur = pos["qty"] if pos else 0.0
        opposite = cur != 0 and (cur > 0) != buy
        if reduce_only:
            if not opposite:
                raise ValueError("reduce-only needs an open position on the other side")
            qty = min(qty, abs(cur))
        elif qty * ref < MIN_NOTIONAL:
            raise ValueError(f"order value must be at least {MIN_NOTIONAL:g} USDT")
        tp, sl = num("tp"), num("sl")
        if (tp or sl) and reduce_only:
            raise ValueError("TP/SL cannot be attached to a reduce-only order")
        if tp and (tp <= ref if buy else tp >= ref):
            raise ValueError("take-profit must be above the entry for a long, below it for a short")
        if sl and (sl >= ref if buy else sl <= ref):
            raise ValueError("stop-loss must be below the entry for a long, above it for a short")
        lev = self.lev(sym)
        opening = 0.0 if reduce_only else max(0.0, qty - (abs(cur) if opposite else 0.0))
        reserve = 0.0 if reduce_only else opening * ref / lev + qty * ref * self.taker
        if reserve > self.available() + 1e-9:
            raise ValueError(f"insufficient balance: needs {reserve:,.2f} USDT, available {self.available():,.2f}")
        o = self._new_order(symbol=sym, side=side, type=typ, qty=qty, price=price, stop_price=stop, trigger=trigger,
                            tif=tif, reduce_only=reduce_only, tp=tp, sl=sl, slippage_pct=num("slippage_pct"),
                            leverage=lev, reserve=reserve)
        if stop is not None:
            o["above"] = stop > self._trigger_px(o, q)
        self.s["orders"].append(o)
        self._work(o, q, placing=True)
        return dict(o)

    def _work(self, o: dict, q: dict, placing: bool = False) -> bool:
        """Advance one open order with the latest quote. True if anything happened."""
        buy = o["side"] == "buy"
        fresh = placing
        if o["type"].startswith("stop"):
            if not o["triggered"]:
                tp = self._trigger_px(o, q)
                if not ((o["above"] and tp >= o["stop_price"]) or (not o["above"] and tp <= o["stop_price"])):
                    return False
                o["triggered"] = True
                fresh = True                               # a released stop-limit gets its "placing" moment now
            if o["type"] == "stop_market":
                return self._market_fill(o, q)
        elif o["type"] == "market":
            return self._market_fill(o, q)
        best = q["ask"] if buy else q["bid"]
        marketable = best <= o["price"] if buy else best >= o["price"]
        if fresh:
            if marketable:                                 # crosses the book: fills now, as a taker
                book = (q.get("book") or {}).get("asks" if buy else "bids") or [[best, math.inf]]
                return self._fill(o, walk_book(book, o["qty"], o["price"], buy), maker=False)
            if o["tif"] in ("IOC", "FOK"):
                self._archive(o, "expired", "not fillable immediately")
                return True
            return True
        touched = min(q["last"], q["ask"]) <= o["price"] if buy else max(q["last"], q["bid"]) >= o["price"]
        if touched:                                        # resting order reached: fills at its price, as a maker
            return self._fill(o, o["price"], maker=True)
        return False

    def _market_fill(self, o: dict, q: dict) -> bool:
        buy = o["side"] == "buy"
        top = q["ask"] if buy else q["bid"]
        book = (q.get("book") or {}).get("asks" if buy else "bids") or [[top, math.inf]]
        px = walk_book(book, o["qty"], None, buy)
        slip = abs(px / top - 1) * 100
        if o.get("slippage_pct") and slip > o["slippage_pct"]:
            self._archive(o, "rejected", f"would slip {slip:.2f}% (tolerance {o['slippage_pct']:g}%)")
            return True
        return self._fill(o, px, maker=False)

    def _fill(self, o: dict, px: float, maker: bool, liquidation: bool = False) -> bool:
        sym, buy = o["symbol"], o["side"] == "buy"
        sign = 1 if buy else -1
        positions = self.s["positions"]
        pos = positions.get(sym)
        cur = pos["qty"] if pos else 0.0
        qty = o["qty"]
        if o["reduce_only"]:
            if cur == 0 or (cur > 0) == buy:
                self._archive(o, "canceled", "nothing left to reduce")
                return True
            qty = min(qty, abs(cur))
        fee = 0.0 if liquidation else qty * px * (self.maker if maker else self.taker)
        self.s["balance"] -= fee
        realized, closing = 0.0, 0.0
        if cur != 0 and (cur > 0) != buy:                  # reduces (or flips) the position
            closing = min(qty, abs(cur))
            release = pos["margin"] * closing / abs(cur)
            realized = (px - pos["entry"]) * closing * (1 if cur > 0 else -1)
            realized = -release if liquidation else max(realized, -release)   # isolated: at most the margin behind it
            pos["margin"] -= release
            pos["qty"] = cur + sign * closing
            pos["realized"] += realized
            self.s["balance"] += realized
            if abs(pos["qty"]) < 1e-12:
                del positions[sym]
                pos = None
        opening = 0.0 if o["reduce_only"] else qty - closing
        if opening > 1e-15:
            lev = self.lev(sym)
            if pos is None:
                pos = positions[sym] = dict(symbol=sym, qty=0.0, entry=px, margin=0.0, leverage=lev, realized=0.0,
                                            funding=0.0, opened=_now_ms(self.clock), next_funding=None)
            new = pos["qty"] + sign * opening
            pos["entry"] = (abs(pos["qty"]) * pos["entry"] + opening * px) / abs(new)
            pos["qty"] = new
            pos["margin"] += opening * px / lev
        o.update(filled_px=px, fee=fee, realized=realized, qty=qty)
        self._archive(o, "liquidated" if liquidation else "filled")
        self.s["trades"] = (self.s["trades"] + [dict(time=_now_ms(self.clock), symbol=sym, side=o["side"], qty=qty,
                                                     price=px, fee=fee, realized=realized,
                                                     role="maker" if maker else "taker", order_id=o["id"],
                                                     liquidation=liquidation)])[-KEEP:]
        if o.get("oco") is not None:                       # one of a TP/SL pair fired: drop its partner
            self._cancel_where(lambda x: x.get("oco") == o["oco"], "the other side of the TP/SL pair filled")
        if sym not in positions:                           # flat: reduce-only orders have nothing left to do
            self._cancel_where(lambda x: x["symbol"] == sym and x["reduce_only"], "position closed")
        if opening > 1e-15 and (o.get("tp") or o.get("sl")):
            self._attach_tpsl(o, opening)
        what = "LIQUIDATED" if liquidation else f"{o['side'].upper()} {o['type'].replace('_', ' ')}"
        self.notify(f"{'💥' if liquidation else '🧾'} PAPER FUTURES {what} {sym}: {qty:.8g} @ {px:.8g}"
                    + (f" · P&L {realized:+.2f} USDT" if closing else "") + " (paper: nothing was sent to Binance)")
        return True

    def _attach_tpsl(self, parent: dict, qty: float) -> None:
        q = self.quotes.get(parent["symbol"])
        close_side = "sell" if parent["side"] == "buy" else "buy"
        for kind, px in (("take-profit", parent.get("tp")), ("stop-loss", parent.get("sl"))):
            if not px:
                continue
            ref = q["last"] if q else parent["filled_px"]
            self.s["orders"].append(self._new_order(
                symbol=parent["symbol"], side=close_side, type="stop_market", qty=qty, stop_price=px, trigger="last",
                reduce_only=True, leverage=parent["leverage"], reason=kind, oco=parent["id"], above=px > ref))

    # ---- the price loop's work: fills, triggers, liquidations, funding ----------------------------------
    def _tick(self) -> bool:
        changed = False
        for o in list(self.s["orders"]):
            q = self.quotes.get(o["symbol"])
            if q and o in self.s["orders"]:
                changed |= self._work(o, q)
        now = _now_ms(self.clock)
        for sym, pos in list(self.s["positions"].items()):
            q = self.quotes.get(sym)
            if not q:
                continue
            liq = liq_price(pos["qty"], pos["entry"], pos["margin"])
            if liq is not None and ((pos["qty"] > 0 and q["mark"] <= liq) or (pos["qty"] < 0 and q["mark"] >= liq)):
                o = self._new_order(symbol=sym, side="sell" if pos["qty"] > 0 else "buy", type="market",
                                    qty=abs(pos["qty"]), trigger="mark", reduce_only=True, leverage=pos["leverage"],
                                    reason="liquidation")
                self.s["orders"].append(o)
                self._fill(o, liq, maker=False, liquidation=True)
                changed = True
                continue
            nf = q.get("next_funding")
            if pos.get("next_funding") is None:
                if nf:
                    pos["next_funding"] = nf
                    changed = True
            elif now >= pos["next_funding"] and q.get("rate") is not None:
                pay = pos["qty"] * q["mark"] * q["rate"]        # long pays a positive rate, short receives it
                self.s["balance"] -= pay
                pos["funding"] -= pay
                pos["next_funding"] = nf if nf and nf > now else pos["next_funding"] + 8 * 3_600_000
                changed = True
        return changed

    # ---- actions (async: they fetch a fresh price first) --------------------------------------
    async def place_order(self, req: dict) -> dict:
        sym = str(req.get("symbol", "")).upper()
        if not SYMBOL_RE.match(sym):
            raise ValueError("symbol must look like BTCUSDT")
        await self._refresh([sym], book=True)
        async with self._lock:
            o = self._place(req)
            self._save()
            return o

    async def cancel(self, order_id) -> dict:
        async with self._lock:
            o = next((o for o in self.s["orders"] if o["id"] == order_id), None)
            if o is None:
                raise ValueError("no such open order")
            self._archive(o, "canceled", "canceled by you")
            if o.get("oco") is not None:
                self._cancel_where(lambda x: x.get("oco") == o["oco"], "the other side of the TP/SL pair was canceled")
            self._save()
            return dict(o)

    async def close_position(self, symbol: str) -> dict:
        sym = str(symbol or "").upper()
        pos = self.s["positions"].get(sym)
        if not pos:
            raise ValueError(f"no open position on {sym}")
        return await self.place_order(dict(symbol=sym, side="sell" if pos["qty"] > 0 else "buy", type="market",
                                           qty=abs(pos["qty"]), reduce_only=True))

    async def set_leverage(self, symbol: str, leverage) -> int:
        sym = str(symbol or "").upper()
        if not SYMBOL_RE.match(sym):
            raise ValueError("symbol must look like BTCUSDT")
        try:
            lev = int(leverage)
        except (TypeError, ValueError):
            raise ValueError("leverage must be a whole number") from None
        if not 1 <= lev <= MAX_LEVERAGE:
            raise ValueError(f"leverage must be between 1x and {MAX_LEVERAGE}x")
        async with self._lock:
            if sym in self.s["positions"] or any(o["symbol"] == sym for o in self.s["orders"]):
                raise ValueError("close the position and cancel the open orders on this market before changing leverage")
            self.s["leverage"][sym] = lev
            self._save()
            return lev

    async def transfer(self, amount) -> float:
        """Add (positive) or take out (negative) paper USDT."""
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            raise ValueError("amount must be a number") from None
        if not math.isfinite(amount) or amount == 0:
            raise ValueError("amount must be a non-zero number")
        async with self._lock:
            if amount < 0 and -amount > self.available() + 1e-9:
                raise ValueError(f"only {self.available():,.2f} USDT is available to take out")
            if self.s["balance"] + amount > MAX_BALANCE:
                raise ValueError(f"the paper balance is capped at {MAX_BALANCE:,.0f} USDT")
            self.s["balance"] += amount
            self.s["deposits"] += amount
            self._save()
            return self.s["balance"]

    # ---- prices ----------------------------------------------------------------------------------
    def _active(self) -> list[str]:
        return sorted({o["symbol"] for o in self.s["orders"]} | set(self.s["positions"]))

    async def _refresh(self, symbols: list[str], book: bool = False) -> None:
        if self.client is None:                    # tests feed self.quotes directly
            return
        get = lambda path, params=None: self.client.get("fapi", path, params, timeout=8.0)  # noqa: E731
        now = self.clock()
        if book or len(symbols) <= 3:               # a few markets: per-symbol requests are cheapest
            async def one(sym):
                calls = [get("/fapi/v1/ticker/price", {"symbol": sym}), get("/fapi/v1/premiumIndex", {"symbol": sym}),
                         get("/fapi/v1/ticker/bookTicker", {"symbol": sym})]
                if book:
                    calls.append(get("/fapi/v1/depth", {"symbol": sym, "limit": 20}))
                res = await asyncio.gather(*calls)
                q = dict(last=float(res[0]["price"]), mark=float(res[1]["markPrice"]),
                         rate=float(res[1]["lastFundingRate"]), next_funding=int(res[1]["nextFundingTime"]),
                         bid=float(res[2]["bidPrice"]), ask=float(res[2]["askPrice"]), ts=now)
                if book:
                    q["book"] = {k: [[float(p), float(v)] for p, v in res[3][k]] for k in ("bids", "asks")}
                self.quotes[sym] = q
            await asyncio.gather(*(one(s) for s in symbols))
            return
        prices, marks, books = await asyncio.gather(get("/fapi/v1/ticker/price"), get("/fapi/v1/premiumIndex"),
                                                    get("/fapi/v1/ticker/bookTicker"))
        want = set(symbols)
        last = {r["symbol"]: float(r["price"]) for r in prices if r["symbol"] in want}
        bb = {r["symbol"]: (float(r["bidPrice"]), float(r["askPrice"])) for r in books if r["symbol"] in want}
        for r in marks:
            s = r["symbol"]
            if s in want and s in last and s in bb:
                self.quotes[s] = dict(last=last[s], mark=float(r["markPrice"]), rate=float(r["lastFundingRate"]),
                                      next_funding=int(r["nextFundingTime"]), bid=bb[s][0], ask=bb[s][1], ts=now)

    async def run(self) -> None:
        """Keep open orders and positions moving with the market. Idle (no requests) when nothing is open."""
        while True:
            await asyncio.sleep(self.poll_s)
            symbols = self._active()
            if not symbols:
                continue
            try:
                await self._refresh(symbols)
                async with self._lock:
                    if self._tick():
                        self._save()
                self.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as e:                 # Binance unreachable: try again next round, never stop the bot
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                log.warning("paper futures price refresh failed: %s", self.last_error)

    # ---- what the page shows --------------------------------------------------------------------
    def view(self) -> dict:
        positions, upnl = [], 0.0
        for sym, p in sorted(self.s["positions"].items()):
            q = self.quotes.get(sym)
            mark = q["mark"] if q else p["entry"]
            pnl = (mark - p["entry"]) * p["qty"]
            upnl += pnl
            positions.append(dict(symbol=sym, side="long" if p["qty"] > 0 else "short", qty=abs(p["qty"]),
                                  entry=p["entry"], mark=mark, notional=abs(p["qty"]) * mark, margin=p["margin"],
                                  leverage=p["leverage"], pnl=pnl, roe=pnl / p["margin"] * 100 if p["margin"] else 0.0,
                                  liq=liq_price(p["qty"], p["entry"], p["margin"]), realized=p["realized"],
                                  funding=p["funding"], opened=p["opened"]))
        return dict(balance=self.s["balance"], available=self.available(), unrealized=upnl,
                    equity=self.s["balance"] + upnl, deposits=self.s["deposits"], leverage=dict(self.s["leverage"]),
                    default_leverage=DEFAULT_LEVERAGE, max_leverage=MAX_LEVERAGE, maker_bps=MAKER_BPS,
                    taker_bps=self.taker * 1e4, mmr=MMR, min_notional=MIN_NOTIONAL, positions=positions,
                    orders=[dict(o) for o in self.s["orders"]], order_history=self.s["order_history"][-50:][::-1],
                    trades=self.s["trades"][-50:][::-1], error=self.last_error)
