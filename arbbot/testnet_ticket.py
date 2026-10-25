"""The dashboard's order ticket on the Binance FUTURES TESTNET: real testnet orders, test money only.

Same methods as arbbot/paper_futures.PaperFutures (the dashboard routes call either), but every order is sent to the
testnet exchange and everything the page shows (balance, positions, open orders, fills) is read back from it.

Safety, by construction:
* It refuses to start unless its client points at a TESTNET host, so it can never send a mainnet order.
* The testnet account is shared with the arbitrage bot (one position per coin in one-way mode), so the two never touch
  the same coin: the ticket refuses coins the arbitrage bot holds, the arbitrage bot skips coins the ticket owns, and
  the bot's startup check ignores the coins saved in `owned` (data/ticket-<state>.json).

Order types map onto Binance's own: LIMIT (GTC/IOC/FOK), MARKET (with a slippage pre-check against the order book),
STOP / STOP_MARKET or TAKE_PROFIT / TAKE_PROFIT_MARKET for conditional orders (whichever triggers in the requested
direction), on the last (CONTRACT_PRICE) or mark price. TP/SL become closePosition orders once the entry has filled.
"""
import asyncio
import json
import logging
import math
import os
import re
import time
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Callable

from .binance_client import BinanceError
from .paper_futures import walk_book

log = logging.getLogger("testnet-ticket")
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}USDT$")
TYPES = ("limit", "market", "stop_limit", "stop_market")
TIFS = ("GTC", "IOC", "FOK")
STATUS = {"NEW": "new", "PARTIALLY_FILLED": "new", "FILLED": "filled", "CANCELED": "canceled", "EXPIRED": "expired",
          "REJECTED": "rejected", "EXPIRED_IN_MATCH": "expired"}
KIND = {"LIMIT": "limit", "MARKET": "market", "STOP": "stop_limit", "STOP_MARKET": "stop_market",
        "TAKE_PROFIT": "stop_limit", "TAKE_PROFIT_MARKET": "stop_market"}
DEFAULT_LEVERAGE, MAX_LEVERAGE, KEEP = 15, 125, 300


def _num(req: dict, key: str, required: bool = False) -> float | None:
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


def _fmt(d: Decimal) -> str:
    return format(d.normalize(), "f")


class TestnetTicket:
    venue = "testnet"

    def __init__(self, client, path: Path | None, arb_symbols: Callable[[], set[str]] = lambda: set(),
                 notify=None, taker_bps: float = 5.0, clock=time.time, poll_s: float = 4.0):
        if "testnet" not in client.hosts.get("fapi", ""):
            raise RuntimeError("the testnet order ticket only runs against a Binance TESTNET client")
        self.c, self.path, self.arb_symbols, self.clock, self.poll_s = client, path, arb_symbols, clock, poll_s
        self.notify = notify or (lambda text: None)
        self.taker_bps = taker_bps
        self.s = self._load() or {"owned": [], "tpsl": {}, "history": [], "start_balance": None}
        self.filters: dict[str, dict] = {}
        self.leverage: dict[str, int] = {}
        self._margin_ready: set[str] = set()
        self.account: dict | None = None
        self.risk: list[dict] = []
        self.open_orders: list[dict] = []
        self.fills: list[dict] = []
        self.last_error = ""
        self._fills_at = 0.0
        self._lock = asyncio.Lock()

    # ---- state ------------------------------------------------------------------------------------
    def _load(self) -> dict | None:
        if not self.path or not Path(self.path).exists():
            return None
        try:
            s = json.loads(Path(self.path).read_text(encoding="utf-8"))
            return s if isinstance(s, dict) and "owned" in s else None
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

    def owned(self) -> set[str]:
        """Coins the ticket is responsible for: the arbitrage bot stays off them and its startup check ignores them."""
        return set(self.s["owned"])

    def _own(self, sym: str) -> None:
        if sym not in self.s["owned"]:
            self.s["owned"].append(sym)

    def _history(self, o: dict) -> None:
        self.s["history"] = (self.s["history"] + [o])[-KEEP:]

    # ---- exchange rules -----------------------------------------------------------------------------
    async def _ensure_filters(self) -> None:
        if self.filters:
            return
        info = await self.c.get("fapi", "/fapi/v1/exchangeInfo")
        for s in info.get("symbols", []):
            f = {x["filterType"]: x for x in s.get("filters", [])}
            self.filters[s["symbol"]] = dict(
                tick=Decimal(f.get("PRICE_FILTER", {}).get("tickSize", "0.01")),
                step=Decimal(f.get("LOT_SIZE", {}).get("stepSize", "0.001")),
                min_qty=Decimal(f.get("LOT_SIZE", {}).get("minQty", "0")),
                min_notional=float(f.get("MIN_NOTIONAL", {}).get("notional", 5)))

    def _qty(self, sym: str, q: float) -> Decimal:
        step = self.filters[sym]["step"]
        return (Decimal(str(q)) / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _px(self, sym: str, p: float) -> Decimal:
        tick = self.filters[sym]["tick"]
        return (Decimal(str(p)) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick

    async def _ensure_margin(self, sym: str) -> None:
        if sym in self._margin_ready:
            return
        try:
            await self.c.post("fapi", "/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
        except BinanceError as e:
            if e.code != -4046:                   # "No need to change margin type"
                raise
        await self.c.post("fapi", "/fapi/v1/leverage", {"symbol": sym, "leverage": self.leverage.get(sym, DEFAULT_LEVERAGE)})
        self._margin_ready.add(sym)

    async def _quote(self, sym: str, book: bool = False) -> dict:
        calls = [self.c.get("fapi", "/fapi/v1/ticker/bookTicker", {"symbol": sym}),
                 self.c.get("fapi", "/fapi/v1/ticker/price", {"symbol": sym})]
        if book:
            calls.append(self.c.get("fapi", "/fapi/v1/depth", {"symbol": sym, "limit": 20}))
        res = await asyncio.gather(*calls)
        q = dict(bid=float(res[0]["bidPrice"]), ask=float(res[0]["askPrice"]), last=float(res[1]["price"]))
        if book:
            q["book"] = {k: [[float(p), float(v)] for p, v in res[2][k]] for k in ("bids", "asks")}
        return q

    def _check_symbol(self, sym: str) -> str:
        sym = str(sym or "").upper()
        if not SYMBOL_RE.match(sym):
            raise ValueError("symbol must look like BTCUSDT")
        if sym in self.arb_symbols():
            raise ValueError(f"{sym} is held by the arbitrage bot on this testnet account; pick another coin")
        return sym

    # ---- orders --------------------------------------------------------------------------------------
    async def place_order(self, req: dict) -> dict:
        sym = self._check_symbol(req.get("symbol"))
        side = req.get("side")
        if side not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        typ = req.get("type")
        if typ not in TYPES:
            raise ValueError(f"type must be one of {', '.join(TYPES)}")
        tif = str(req.get("tif") or "GTC").upper()
        if tif not in TIFS:
            raise ValueError("time in force must be GTC, IOC or FOK")
        trigger = req.get("trigger") or "last"
        if trigger not in ("last", "mark"):
            raise ValueError("trigger must be last or mark")
        reduce_only = bool(req.get("reduce_only"))
        tp, sl, slip = _num(req, "tp"), _num(req, "sl"), _num(req, "slippage_pct")
        if (tp or sl) and reduce_only:
            raise ValueError("TP/SL cannot be attached to a reduce-only order")
        async with self._lock:
            await self._ensure_filters()
            if sym not in self.filters:
                raise ValueError(f"{sym} is not listed on the futures testnet")
            buy = side == "buy"
            q = await self._quote(sym, book=typ == "market")
            price = None
            if typ in ("limit", "stop_limit"):
                price = (q["bid"] if buy else q["ask"]) if (req.get("bbo") and typ == "limit") else _num(req, "price", True)
            stop = _num(req, "stop_price", True) if typ.startswith("stop") else None
            ref = price or stop or (q["ask"] if buy else q["bid"])
            qty, size = _num(req, "qty"), _num(req, "size_usdt")
            if (qty is None) == (size is None):
                raise ValueError("give the size either in USDT or in coins")
            qd = self._qty(sym, qty if qty is not None else size / ref)
            f = self.filters[sym]
            if qd <= 0 or qd < f["min_qty"]:
                raise ValueError(f"size is below the minimum quantity for {sym} ({_fmt(f['min_qty'])})")
            if not reduce_only and float(qd) * ref < f["min_notional"]:
                raise ValueError(f"order value must be at least {f['min_notional']:g} USDT on {sym}")
            if tp and (tp <= ref if buy else tp >= ref):
                raise ValueError("take-profit must be above the entry for a long, below it for a short")
            if sl and (sl >= ref if buy else sl <= ref):
                raise ValueError("stop-loss must be below the entry for a long, above it for a short")
            if typ == "market" and slip:
                top = q["ask"] if buy else q["bid"]
                est = walk_book(q["book"]["asks" if buy else "bids"], float(qd), None, buy)
                moved = abs(est / top - 1) * 100
                if moved > slip:
                    raise ValueError(f"would slip about {moved:.2f}% through the order book (tolerance {slip:g}%)")
            params = {"symbol": sym, "side": side.upper(), "quantity": _fmt(qd), "newOrderRespType": "RESULT"}
            if typ == "limit":
                params.update(type="LIMIT", price=_fmt(self._px(sym, price)), timeInForce=tif)
            elif typ == "market":
                params.update(type="MARKET")
            else:
                # a buy that triggers ABOVE the price is a STOP, below it a TAKE_PROFIT (sells: the other way round)
                rises = stop > q["last"]
                kind = "STOP" if rises == buy else "TAKE_PROFIT"
                params.update(stopPrice=_fmt(self._px(sym, stop)),
                              workingType="MARK_PRICE" if trigger == "mark" else "CONTRACT_PRICE")
                if typ == "stop_limit":
                    params.update(type=kind, price=_fmt(self._px(sym, price)), timeInForce=tif)
                else:
                    params.update(type=f"{kind}_MARKET")
            if reduce_only:
                params["reduceOnly"] = "true"
            if not reduce_only:
                await self._ensure_margin(sym)
            o = await self.c.post("fapi", "/fapi/v1/order", params)
            out = await self._describe(sym, o)
            self._own(sym)
            if tp or sl:
                if out["status"] == "filled":
                    await self._place_tpsl(sym, side, tp, sl, o.get("orderId"))
                elif out["status"] == "new":
                    self.s["tpsl"][str(o["orderId"])] = {"symbol": sym, "side": side, "tp": tp, "sl": sl}
            self._history(dict(out, updated=int(self.clock() * 1000)))
            self._save()
        if out["status"] == "filled":
            self.notify(f"🧾 TESTNET FUTURES {side.upper()} {typ.replace('_', ' ')} {sym}: {out['qty']:.8g} @ "
                        f"{out['filled_px'] or 0:.8g} (testnet: test money)")
        await self._safe_refresh()
        return out

    async def _describe(self, sym: str, o: dict) -> dict:
        """One Binance order as the ticket's order dict (paper shape)."""
        status = STATUS.get(o.get("status", ""), "new")
        executed = float(o.get("executedQty") or 0)
        avg = float(o.get("avgPrice") or 0) or (float(o.get("cumQuote") or 0) / executed if executed else 0.0)
        if status == "filled" and avg <= 0:           # replies do not always carry the price (seen on testnet)
            try:
                again = await self.c.get("fapi", "/fapi/v1/order", {"symbol": sym, "orderId": o["orderId"]}, signed=True)
                avg = float(again.get("avgPrice") or 0) or (float(again.get("cumQuote") or 0) / executed if executed else 0)
            except Exception:
                avg = 0.0
        return dict(id=o.get("orderId"), symbol=sym, side=str(o.get("side", "")).lower(),
                    type=KIND.get(o.get("type") or o.get("origType", ""), "market"),
                    qty=executed or float(o.get("origQty") or 0), price=float(o.get("price") or 0) or None,
                    stop_price=float(o.get("stopPrice") or 0) or None, status=status, filled_px=avg or None,
                    fee=0.0, reason="", created=int(o.get("updateTime") or self.clock() * 1000))

    async def _place_tpsl(self, sym: str, side: str, tp: float | None, sl: float | None, parent) -> None:
        exit_side = "SELL" if side == "buy" else "BUY"
        for kind, px in (("TAKE_PROFIT_MARKET", tp), ("STOP_MARKET", sl)):
            if not px:
                continue
            try:
                await self.c.post("fapi", "/fapi/v1/order", {
                    "symbol": sym, "side": exit_side, "type": kind, "stopPrice": _fmt(self._px(sym, px)),
                    "closePosition": "true", "workingType": "CONTRACT_PRICE"})
            except BinanceError as e:                  # the entry stands; say clearly that its protection is missing
                log.error("%s: could not place the %s for order %s: %s", sym, kind, parent, e)
                self.notify(f"⚠️ TESTNET {sym}: the {kind.replace('_', ' ').lower()} could not be placed ({e.msg}). "
                            f"The position is open without it.")

    async def cancel(self, order_id) -> dict:
        async with self._lock:
            o = next((x for x in self.open_orders if x.get("orderId") == order_id), None)
            if o is None:
                raise ValueError("no such open order on the testnet account")
            self._check_symbol(o["symbol"])
            r = await self.c.request("fapi", "DELETE", "/fapi/v1/order", {"symbol": o["symbol"], "orderId": order_id},
                                     signed=True)
            self.s["tpsl"].pop(str(order_id), None)
            out = await self._describe(o["symbol"], r)
            self._history(dict(out, status="canceled", reason="canceled by you", updated=int(self.clock() * 1000)))
            self._save()
        await self._safe_refresh()
        return out

    async def close_position(self, symbol: str) -> dict:
        sym = self._check_symbol(symbol)
        await self._safe_refresh()
        row = next((p for p in self.risk if p["symbol"] == sym and float(p["positionAmt"]) != 0), None)
        if row is None:
            raise ValueError(f"no open position on {sym}")
        amt = float(row["positionAmt"])
        return await self.place_order(dict(symbol=sym, side="sell" if amt > 0 else "buy", type="market", qty=abs(amt),
                                           reduce_only=True))

    async def set_leverage(self, symbol: str, leverage) -> int:
        sym = self._check_symbol(symbol)
        try:
            lev = int(leverage)
        except (TypeError, ValueError):
            raise ValueError("leverage must be a whole number") from None
        if not 1 <= lev <= MAX_LEVERAGE:
            raise ValueError(f"leverage must be between 1x and {MAX_LEVERAGE}x")
        async with self._lock:
            try:
                await self.c.post("fapi", "/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
            except BinanceError as e:
                if e.code != -4046:
                    raise
            r = await self.c.post("fapi", "/fapi/v1/leverage", {"symbol": sym, "leverage": lev})
            self.leverage[sym] = int(r.get("leverage", lev))
            self._margin_ready.add(sym)
            return self.leverage[sym]

    async def transfer(self, amount) -> float:
        raise ValueError("testnet USDT comes from Binance's futures testnet itself; there is nothing to transfer here")

    # ---- reading the account -------------------------------------------------------------------------
    async def _get_v3(self, v3: str, v2: str, params=None):
        try:
            return await self.c.get("fapi", v3, params, signed=True)
        except BinanceError as e:
            if e.status != 404:
                raise
            return await self.c.get("fapi", v2, params, signed=True)

    async def refresh(self) -> None:
        account, risk = await asyncio.gather(self._get_v3("/fapi/v3/account", "/fapi/v2/account"),
                                             self._get_v3("/fapi/v3/positionRisk", "/fapi/v2/positionRisk"))
        # open orders only on the ticket's own coins (weight 1 each, instead of 40 for the all-coins call)
        mine = sorted(set(self.s["owned"]) | {t["symbol"] for t in self.s["tpsl"].values()})
        got = await asyncio.gather(*(self.c.get("fapi", "/fapi/v1/openOrders", {"symbol": s}, signed=True) for s in mine))
        orders = [o for g in got for o in g]
        self.account, self.risk, self.open_orders = account, risk, orders
        arb = self.arb_symbols()
        open_ids = {o["orderId"] for o in orders}
        for oid, t in list(self.s["tpsl"].items()):     # a limit entry with TP/SL: protect it once it has filled
            if int(oid) in open_ids:
                continue
            o = await self.c.get("fapi", "/fapi/v1/order", {"symbol": t["symbol"], "orderId": int(oid)}, signed=True)
            if o.get("status") == "FILLED":
                await self._place_tpsl(t["symbol"], t["side"], t["tp"], t["sl"], oid)
            if o.get("status") not in ("NEW", "PARTIALLY_FILLED"):
                self.s["tpsl"].pop(oid, None)
        flat = {p["symbol"] for p in risk if float(p["positionAmt"]) == 0}
        for o in orders:                                 # a closed position leaves its TP/SL behind: tidy them up
            if o["symbol"] in flat and o["symbol"] not in arb and o.get("closePosition") in (True, "true"):
                try:
                    await self.c.request("fapi", "DELETE", "/fapi/v1/order", {"symbol": o["symbol"], "orderId": o["orderId"]},
                                         signed=True)
                except BinanceError:
                    pass
        busy = {p["symbol"] for p in risk if float(p["positionAmt"]) != 0} | {o["symbol"] for o in orders}
        self.s["owned"] = sorted((set(self.s["owned"]) & busy) - arb)
        if self.s["start_balance"] is None:
            self.s["start_balance"] = self._usdt("walletBalance")
        if self.clock() - self._fills_at > 15:
            self._fills_at = self.clock()
            symbols = sorted({h["symbol"] for h in self.s["history"][-50:]} - arb)[:8]
            got = await asyncio.gather(*(self.c.get("fapi", "/fapi/v1/userTrades", {"symbol": s, "limit": 20}, signed=True)
                                         for s in symbols), return_exceptions=True)
            self.fills = sorted((f for g in got if isinstance(g, list) for f in g), key=lambda f: -int(f["time"]))[:50]
        self._save()

    async def _safe_refresh(self) -> None:
        try:
            await self.refresh()
            self.last_error = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {str(e)[:120]}"

    async def run(self) -> None:
        while True:
            await self._safe_refresh()
            if self.last_error:
                log.warning("testnet ticket refresh failed: %s", self.last_error)
            await asyncio.sleep(self.poll_s)

    def _usdt(self, key: str) -> float:
        a = next((x for x in (self.account or {}).get("assets", []) if x.get("asset") == "USDT"), {})
        return float(a.get(key) or 0)

    def view(self) -> dict:
        arb = self.arb_symbols()
        positions = []
        for p in self.risk:
            amt = float(p.get("positionAmt") or 0)
            if amt == 0 or p["symbol"] in arb:
                continue
            mark, entry = float(p.get("markPrice") or 0), float(p.get("entryPrice") or 0)
            margin = float(p.get("isolatedMargin") or p.get("isolatedWallet") or 0) or abs(amt) * entry / max(
                1, int(float(p.get("leverage") or DEFAULT_LEVERAGE)))
            pnl = float(p.get("unRealizedProfit") or p.get("unrealizedProfit") or 0)
            liq = float(p.get("liquidationPrice") or 0)
            positions.append(dict(symbol=p["symbol"], side="long" if amt > 0 else "short", qty=abs(amt), entry=entry,
                                  mark=mark, notional=abs(amt) * mark, margin=margin,
                                  leverage=int(float(p.get("leverage") or self.leverage.get(p["symbol"], DEFAULT_LEVERAGE))),
                                  pnl=pnl, roe=pnl / margin * 100 if margin else 0.0, liq=liq or None, realized=0.0,
                                  funding=0.0, opened=int(p.get("updateTime") or 0)))
        orders = []
        for o in self.open_orders:
            if o["symbol"] in arb:
                continue
            t = o.get("type") or o.get("origType", "")
            close = o.get("closePosition") in (True, "true")
            reason = ("take-profit" if t.startswith("TAKE_PROFIT") else "stop-loss") if close else ""
            buy = o.get("side") == "BUY"
            orders.append(dict(id=o["orderId"], symbol=o["symbol"], side="buy" if buy else "sell", type=KIND.get(t, "limit"),
                               qty=float(o.get("origQty") or 0), price=float(o.get("price") or 0) or None,
                               stop_price=float(o.get("stopPrice") or 0) or None,
                               trigger="mark" if o.get("workingType") == "MARK_PRICE" else "last",
                               tif=o.get("timeInForce", "GTC"), reduce_only=bool(o.get("reduceOnly")) or close,
                               reason=reason, oco=0 if close else None, triggered=False,
                               above=(t.startswith("STOP") == buy) if t not in ("LIMIT", "MARKET") else None,
                               created=int(o.get("time") or o.get("updateTime") or 0), status="new"))
        trades = [dict(time=int(f["time"]), symbol=f["symbol"], side=str(f["side"]).lower(), price=float(f["price"]),
                       qty=float(f["qty"]), fee=float(f.get("commission") or 0), realized=float(f.get("realizedPnl") or 0),
                       role="maker" if f.get("maker") else "taker", liquidation=False) for f in self.fills]
        balance, available = self._usdt("walletBalance"), self._usdt("availableBalance")
        upnl = sum(p["pnl"] for p in positions)
        return dict(venue="testnet", balance=balance, available=available, unrealized=upnl, equity=balance + upnl,
                    deposits=self.s["start_balance"] or balance, leverage=dict(self.leverage),
                    default_leverage=DEFAULT_LEVERAGE, max_leverage=MAX_LEVERAGE, maker_bps=2.0,
                    taker_bps=self.taker_bps, mmr=0.005, min_notional=5.0, positions=positions, orders=orders,
                    order_history=self.s["history"][-50:][::-1], trades=trades, error=self.last_error,
                    arb_symbols=sorted(arb))
