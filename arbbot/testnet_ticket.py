"""The dashboard's order ticket on a real Binance FUTURES account: the TESTNET (test money), or, only when opted in with
real_money=True (run.py --live --real-ticket), MAINNET with real money.

Same methods as arbbot/paper_futures.PaperFutures (the dashboard routes call either), but every order is sent to the
exchange and everything the page shows (balance, positions, open orders, fills) is read back from it.

Safety, by construction:
* It refuses a mainnet client unless real_money=True, and then it also needs a cap on each order's value
  (max_order_usd); new orders are refused while the KILL file is present (`blocked`). Closing is always allowed.
* The futures account is shared with the arbitrage bot (one position per coin in one-way mode), so the two never touch
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
SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}USDT$")   # one letter is real: TUSDT, GUSDT
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
    def __init__(self, client, path: Path | None, arb_symbols: Callable[[], set[str]] = lambda: set(),
                 notify=None, taker_bps: float = 5.0, clock=time.time, poll_s: float = 4.0, real_money: bool = False,
                 max_order_usd: float | None = None, default_leverage: int = DEFAULT_LEVERAGE,
                 blocked: Callable[[], str] = lambda: ""):
        if "testnet" in client.hosts.get("fapi", ""):
            self.venue = "testnet"
        elif not real_money:
            raise RuntimeError("the order ticket only runs against a Binance TESTNET client unless real_money=True")
        elif not (max_order_usd and max_order_usd > 0):
            raise RuntimeError("a real-money order ticket needs a cap on each order's value (max_order_usd)")
        else:
            self.venue = "mainnet"
        self.max_order_usd, self.default_leverage, self.blocked = max_order_usd, default_leverage, blocked
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
        self._income, self._income_at = None, 0.0     # P&L report cache (pnl_events)
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
                market_max=Decimal(f.get("MARKET_LOT_SIZE", {}).get("maxQty", "0")) or None,
                min_notional=float(f.get("MIN_NOTIONAL", {}).get("notional", 5)))

    async def max_market_qty(self, symbol: str) -> float | None:
        """The largest quantity Binance accepts in ONE market order on this coin (None when it sets no limit)."""
        await self._ensure_filters()
        mx = self.filters.get(symbol, {}).get("market_max")
        return float(mx) if mx else None

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
        await self.c.post("fapi", "/fapi/v1/leverage", {"symbol": sym, "leverage": self.leverage.get(sym, self.default_leverage)})
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
            raise ValueError(f"{sym} is held by the arbitrage bot on this {self.venue} account; pick another coin")
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
        why = "" if reduce_only else self.blocked()
        if why:
            raise ValueError(f"new orders are blocked: {why} (closing a position still works)")
        async with self._lock:
            await self._ensure_filters()
            if sym not in self.filters:
                raise ValueError(f"{sym} is not listed on the {self.venue} futures exchange")
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
            if typ == "market" and f.get("market_max") and qd > f["market_max"]:
                raise ValueError(f"{_fmt(qd)} {sym} is above the largest market order Binance accepts on it "
                                 f"({_fmt(f['market_max'])}); split it into smaller orders")
            if not reduce_only and float(qd) * ref < f["min_notional"]:
                raise ValueError(f"order value must be at least {f['min_notional']:g} USDT on {sym}")
            if self.max_order_usd and not reduce_only and float(qd) * ref > self.max_order_usd:
                raise ValueError(f"order value {float(qd) * ref:,.2f} USDT is above the {self.max_order_usd:g} USDT cap per "
                                 f"order (run.py --real-ticket-max-usd)")
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
            if typ.startswith("stop") and (tp or sl):
                raise ValueError("add TP/SL once the conditional order has filled (attach it to a limit or market "
                                 "order instead)")
            params = {"symbol": sym, "side": side.upper(), "quantity": _fmt(qd)}
            if reduce_only:
                params["reduceOnly"] = "true"
            if not reduce_only:
                await self._ensure_margin(sym)
            if typ in ("limit", "market"):
                params["newOrderRespType"] = "RESULT"
                if typ == "limit":
                    params.update(type="LIMIT", price=_fmt(self._px(sym, price)), timeInForce=tif)
                else:
                    params.update(type="MARKET")
                o = await self.c.post("fapi", "/fapi/v1/order", params)
                out = await self._describe(sym, o)
            else:
                # Conditional orders live on Binance's Algo Order API (POST /fapi/v1/order refuses them: code -4120).
                # A buy that triggers ABOVE the price is a STOP, below it a TAKE_PROFIT (sells: the other way round).
                kind = "STOP" if (stop > q["last"]) == buy else "TAKE_PROFIT"
                params.update(algoType="CONDITIONAL", triggerPrice=_fmt(self._px(sym, stop)),
                              workingType="MARK_PRICE" if trigger == "mark" else "CONTRACT_PRICE")
                if typ == "stop_limit":
                    params.update(type=kind, price=_fmt(self._px(sym, price)), timeInForce=tif)
                else:
                    params.update(type=f"{kind}_MARKET")
                o = await self.c.post("fapi", "/fapi/v1/algoOrder", params)
                out = dict(id=f"a{o.get('algoId')}", symbol=sym, side=side, type=typ, qty=float(qd), price=price,
                           stop_price=stop, status=STATUS.get(o.get("algoStatus", "NEW"), "new"), filled_px=None,
                           fee=0.0, reason="", created=int(o.get("createTime") or self.clock() * 1000))
            self._own(sym)
            if tp or sl:
                if out["status"] == "filled":
                    await self._place_tpsl(sym, side, tp, sl, o.get("orderId"))
                elif out["status"] == "new":
                    self.s["tpsl"][str(o["orderId"])] = {"symbol": sym, "side": side, "tp": tp, "sl": sl}
            self._history(dict(out, updated=int(self.clock() * 1000)))
            self._save()
        if out["status"] == "filled":
            self.notify(f"🧾 {self.venue.upper()} FUTURES {side.upper()} {typ.replace('_', ' ')} {sym}: {out['qty']:.8g} @ "
                        f"{out['filled_px'] or 0:.8g} ({'testnet: test money' if self.venue == 'testnet' else 'REAL MONEY'})")
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
                await self.c.post("fapi", "/fapi/v1/algoOrder", {
                    "algoType": "CONDITIONAL", "symbol": sym, "side": exit_side, "type": kind,
                    "triggerPrice": _fmt(self._px(sym, px)), "closePosition": "true", "workingType": "CONTRACT_PRICE"})
            except BinanceError as e:                  # the entry stands; say clearly that its protection is missing
                log.error("%s: could not place the %s for order %s: %s", sym, kind, parent, e)
                self.notify(f"⚠️ {self.venue.upper()} {sym}: the {kind.replace('_', ' ').lower()} could not be placed ({e.msg}). "
                            f"The position is open without it.")

    async def cancel(self, order_id) -> dict:
        async with self._lock:
            o = next((x for x in self.open_orders if str(x.get("orderId")) == str(order_id)), None)
            if o is None:
                raise ValueError(f"no such open order on the {self.venue} account")
            self._check_symbol(o["symbol"])
            if "_algo" in o:                                 # a conditional order: cancelled through the Algo API
                await self.c.request("fapi", "DELETE", "/fapi/v1/algoOrder", {"algoId": o["_algo"]}, signed=True)
                out = dict(id=o["orderId"], symbol=o["symbol"], side=o["side"].lower(), type=KIND.get(o["type"], "stop_market"),
                           qty=float(o.get("origQty") or 0), price=float(o.get("price") or 0) or None,
                           stop_price=float(o.get("stopPrice") or 0) or None, status="canceled", filled_px=None,
                           fee=0.0, reason="", created=int(o.get("time") or 0))
            else:
                r = await self.c.request("fapi", "DELETE", "/fapi/v1/order", {"symbol": o["symbol"], "orderId": o["orderId"]},
                                         signed=True)
                out = await self._describe(o["symbol"], r)
            self.s["tpsl"].pop(str(order_id), None)
            self._history(dict(out, status="canceled", reason="canceled by you", updated=int(self.clock() * 1000)))
            self._save()
        await self._safe_refresh()
        return out

    def position(self, symbol: str) -> float:
        """Signed position amount on this coin as of the last refresh (0 when flat)."""
        return next((float(p["positionAmt"]) for p in self.risk if p["symbol"] == symbol), 0.0)

    def position_info(self, symbol: str) -> dict:
        """Entry, mark and unrealised P&L of the open position on this coin as of the last refresh ({} when flat)."""
        p = next((p for p in self.risk if p["symbol"] == symbol and float(p.get("positionAmt") or 0) != 0), None)
        if p is None:
            return {}
        return dict(amount=float(p["positionAmt"]), entry_px=float(p.get("entryPrice") or 0) or None,
                    mark=float(p.get("markPrice") or 0) or None,
                    pnl=float(p.get("unRealizedProfit") or p.get("unrealizedProfit") or 0))

    async def fills_between(self, symbol: str, start_ms: float, end_ms: float) -> list[dict]:
        """This account's fills on one coin between two times (read-only; Binance allows at most 7 days per call)."""
        return await self.c.get("fapi", "/fapi/v1/userTrades", {"symbol": symbol, "startTime": int(start_ms),
                                                                 "endTime": int(end_ms), "limit": 1000}, signed=True)

    async def replace_stop(self, symbol: str, side: str, stop: float) -> None:
        """Move the exchange-side stop of the `side` ("buy" = long) position on `symbol`: cancel its closePosition
        stop-market and place a new one at `stop` (the swing mirror trails stops this way)."""
        sym = self._check_symbol(symbol)
        async with self._lock:
            await self._ensure_filters()
            for o in list(self.open_orders):
                if (o["symbol"] == sym and "_algo" in o and str(o.get("type", "")).startswith("STOP")
                        and o.get("closePosition") in (True, "true")):
                    await self.c.request("fapi", "DELETE", "/fapi/v1/algoOrder", {"algoId": o["_algo"]}, signed=True)
            await self._place_tpsl(sym, side, None, stop, "trailing stop")
        await self._safe_refresh()

    async def close_position(self, symbol: str) -> dict:
        sym = self._check_symbol(symbol)
        await self._safe_refresh()
        row = next((p for p in self.risk if p["symbol"] == sym and float(p["positionAmt"]) != 0), None)
        if row is None:
            raise ValueError(f"no open position on {sym}")
        amt = float(row["positionAmt"])
        left, mx, out = abs(amt), await self.max_market_qty(sym), None
        while left > 0:                                  # a big position closes in pieces of Binance's market maximum
            piece = min(left, mx) if mx else left
            out = await self.place_order(dict(symbol=sym, side="sell" if amt > 0 else "buy", type="market", qty=piece,
                                              reduce_only=True))
            left = round(left - piece, 12)
        return out

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
        if self.venue == "mainnet":
            raise ValueError("move real USDT between your spot and futures wallets in the Binance app, not here")
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
        orders = [o for g in got for o in g] + await self._open_algo(mine)
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
                    if "_algo" in o:
                        await self.c.request("fapi", "DELETE", "/fapi/v1/algoOrder", {"algoId": o["_algo"]}, signed=True)
                    else:
                        await self.c.request("fapi", "DELETE", "/fapi/v1/order",
                                             {"symbol": o["symbol"], "orderId": o["orderId"]}, signed=True)
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

    async def _open_algo(self, symbols: list[str]) -> list[dict]:
        """Open conditional (Algo API) orders on these coins, in the same shape as regular open orders."""
        out = []
        for s in symbols:
            try:
                r = await self.c.get("fapi", "/fapi/v1/openAlgoOrders", {"symbol": s}, signed=True)
            except BinanceError as e:
                if e.status == 404:                          # no algo endpoint on this host: nothing to list
                    return out
                raise
            rows = r if isinstance(r, list) else (r.get("orders") or r.get("rows") or [])
            for a in rows:
                out.append({"orderId": f"a{a['algoId']}", "_algo": a["algoId"], "symbol": a.get("symbol", s),
                            "side": a.get("side", ""), "type": a.get("orderType") or a.get("type", "STOP_MARKET"),
                            "origQty": a.get("quantity") or "0", "price": a.get("price") or "0",
                            "stopPrice": a.get("triggerPrice") or "0", "closePosition": a.get("closePosition"),
                            "reduceOnly": a.get("reduceOnly"), "workingType": a.get("workingType", "CONTRACT_PRICE"),
                            "timeInForce": a.get("timeInForce", "GTC"), "time": a.get("createTime") or 0})
        return out

    async def _safe_refresh(self) -> None:
        try:
            await self.refresh()
            self.last_error = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {str(e)[:120]}"

    async def run(self) -> None:
        said, seen = 0.0, {}
        while True:
            await self._safe_refresh()
            if self.last_error:
                log.warning("%s ticket refresh failed: %s", self.venue, self.last_error)
            # Binance reports this IP's request weight for the last minute in every reply; the testnet bans an IP that
            # keeps going over its limit (HTTP 418), so say it out loud before that happens.
            used = getattr(self.c, "used_weight", {}).get("fapi", 0)
            if self.clock() - said >= 30:
                said = self.clock()
                calls = dict(getattr(self.c, "calls", {}))
                busiest = sorted(((n - seen.get(k, 0), k) for k, n in calls.items()), reverse=True)[:6]
                seen = calls
                (log.warning if used >= 1200 else log.info)(
                    "%s request weight used this minute: %d of 2400%s | most requests since last: %s", self.venue, used,
                    " (high)" if used >= 1200 else "", ", ".join(f"{k} x{n}" for n, k in busiest if n))
            await asyncio.sleep(self.poll_s)

    def _usdt(self, key: str) -> float:
        a = next((x for x in (self.account or {}).get("assets", []) if x.get("asset") == "USDT"), {})
        return float(a.get(key) or 0)

    async def pnl_events(self) -> tuple[list[tuple[float, float, bool]], str]:
        """The whole futures account's P&L for the reports, from Binance's income history (it keeps about 3 months):
        realised P&L of closing fills (fills of one coin within 2 minutes count as one trade), fees and funding.
        Read-only; cached for 2 minutes (weight 30 per page)."""
        if self._income is not None and self.clock() - self._income_at < 120:
            return self._income
        start, rows = int((self.clock() - 89 * 86_400) * 1000), []
        for _ in range(10):
            got = await self.c.get("fapi", "/fapi/v1/income", {"startTime": start, "limit": 1000}, signed=True)
            rows += got
            if len(got) < 1000:
                break
            start = int(got[-1]["time"]) + 1
        events, last = [], {}
        for r in sorted(rows, key=lambda r: int(r["time"])):
            kind, amount, ts = r.get("incomeType"), float(r.get("income") or 0), int(r["time"]) / 1000
            if kind == "REALIZED_PNL":
                sym, prev = r.get("symbol", ""), last.get(r.get("symbol", ""))
                if prev is not None and ts - events[prev][0] <= 120:       # one order, several fills
                    events[prev] = (events[prev][0], events[prev][1] + amount, True)
                else:
                    last[sym] = len(events)
                    events.append((ts, amount, True))
            elif kind in ("COMMISSION", "FUNDING_FEE"):
                events.append((ts, amount, False))
        self._income = (events, f"{self.venue} futures account · realised P&L, fees and funding from Binance "
                                f"(about 3 months kept) · wins counted before fees")
        self._income_at = self.clock()
        return self._income

    def view(self) -> dict:
        arb = self.arb_symbols()
        positions = []
        for p in self.risk:
            amt = float(p.get("positionAmt") or 0)
            if amt == 0 or p["symbol"] in arb:
                continue
            mark, entry = float(p.get("markPrice") or 0), float(p.get("entryPrice") or 0)
            margin = float(p.get("isolatedMargin") or p.get("isolatedWallet") or 0) or abs(amt) * entry / max(
                1, int(float(p.get("leverage") or self.default_leverage)))
            pnl = float(p.get("unRealizedProfit") or p.get("unrealizedProfit") or 0)
            liq = float(p.get("liquidationPrice") or 0)
            positions.append(dict(symbol=p["symbol"], side="long" if amt > 0 else "short", qty=abs(amt), entry=entry,
                                  mark=mark, notional=abs(amt) * mark, margin=margin,
                                  leverage=int(float(p.get("leverage") or self.leverage.get(p["symbol"], self.default_leverage))),
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
        return dict(venue=self.venue, balance=balance, available=available, unrealized=upnl, equity=balance + upnl,
                    deposits=self.s["start_balance"] or balance, leverage=dict(self.leverage),
                    default_leverage=self.default_leverage, max_leverage=MAX_LEVERAGE, maker_bps=2.0,
                    max_order_usd=self.max_order_usd,
                    taker_bps=self.taker_bps, mmr=0.005, min_notional=5.0, positions=positions, orders=orders,
                    order_history=self.s["history"][-50:][::-1], trades=trades, error=self.last_error,
                    arb_symbols=sorted(arb))
