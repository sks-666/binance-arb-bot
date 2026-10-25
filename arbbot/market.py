"""Read-only Binance USD-M market data for the local dashboard's market panel (chart, order book, trades, movers, info).

The page never talks to Binance itself: it asks the local server, which forwards a FIXED set of public requests (no API key,
no signed endpoint, symbols and intervals checked against a whitelist) and caches each answer for a second or more, so a
dashboard left open adds a small, bounded load next to the arbitrage feed. Several open tabs share the same cached answers.
"""
import asyncio
import re
import time

from .binance_client import BinanceClient

SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}USDT$")
INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d", "1w")
MOVERS_MIN_VOLUME = 20e6   # USDT of 24h volume: ignore coins too thin to matter
MOVERS_N = 10

# kind -> seconds an answer is reused. Binance weights (per call): klines 2, depth(20) 2, trades 5, 24hr(all) 40.
TTL = {"klines": 2.0, "depth": 1.0, "trades": 1.5, "info": 5.0, "data": 30.0, "movers": 15.0}


class MarketData:
    def __init__(self, client: BinanceClient, clock=time.monotonic):
        self.client, self.clock = client, clock
        self._cache: dict[tuple, tuple[float, object]] = {}
        self._inflight: dict[tuple, asyncio.Future] = {}

    @staticmethod
    def check_symbol(symbol: str) -> str:
        symbol = (symbol or "").strip().upper()
        if not SYMBOL_RE.match(symbol):
            raise ValueError("symbol must look like BTCUSDT")
        return symbol

    async def _cached(self, key: tuple, load):
        """The cached answer while it is fresh; otherwise one request, shared by every caller asking meanwhile."""
        hit = self._cache.get(key)
        if hit and self.clock() - hit[0] < TTL[key[0]]:
            return hit[1]
        if key in self._inflight:
            return await asyncio.shield(self._inflight[key])
        fut = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        try:
            value = await load()
            self._cache[key] = (self.clock(), value)
            if len(self._cache) > 500:                    # many symbols browsed: drop the oldest answers
                for k in sorted(self._cache, key=lambda k: self._cache[k][0])[:250]:
                    del self._cache[k]
            fut.set_result(value)
            return value
        except asyncio.CancelledError:                    # the page went away mid-request: waiters just try again
            fut.cancel()
            raise
        except Exception as e:
            fut.set_exception(e)
            fut.exception()                               # mark retrieved: no "never retrieved" warning
            raise
        finally:
            del self._inflight[key]

    def _get(self, path: str, params: dict | None = None):
        return self.client.get("fapi", path, params, timeout=8.0)

    async def klines(self, symbol: str, interval: str) -> list[list]:
        symbol = self.check_symbol(symbol)
        if interval not in INTERVALS:
            raise ValueError(f"interval must be one of {', '.join(INTERVALS)}")

        async def load():
            rows = await self._get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": 300})
            return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[7])] for r in rows]
        return await self._cached(("klines", symbol, interval), load)

    async def depth(self, symbol: str) -> dict:
        symbol = self.check_symbol(symbol)

        async def load():
            d = await self._get("/fapi/v1/depth", {"symbol": symbol, "limit": 20})
            side = lambda rows: [[float(p), float(q)] for p, q in rows]  # noqa: E731
            return {"bids": side(d["bids"]), "asks": side(d["asks"]), "ts": d.get("E") or d.get("T")}
        return await self._cached(("depth", symbol), load)

    async def trades(self, symbol: str) -> list[dict]:
        symbol = self.check_symbol(symbol)

        async def load():
            rows = await self._get("/fapi/v1/trades", {"symbol": symbol, "limit": 40})
            # isBuyerMaker: the seller hit the bid, i.e. an aggressive SELL (shown red)
            return [{"price": float(r["price"]), "usd": float(r["quoteQty"]), "ts": int(r["time"]),
                     "sell": bool(r["isBuyerMaker"])} for r in reversed(rows)]
        return await self._cached(("trades", symbol), load)

    async def info(self, symbol: str) -> dict:
        symbol = self.check_symbol(symbol)

        async def load():
            t, p, oi = await asyncio.gather(
                self._get("/fapi/v1/ticker/24hr", {"symbol": symbol}),
                self._get("/fapi/v1/premiumIndex", {"symbol": symbol}),
                self._get("/fapi/v1/openInterest", {"symbol": symbol}))
            last = float(t["lastPrice"])
            return {"symbol": symbol, "last": last, "change_pct": float(t["priceChangePercent"]),
                    "high": float(t["highPrice"]), "low": float(t["lowPrice"]), "volume": float(t["volume"]),
                    "quote_volume": float(t["quoteVolume"]), "mark": float(p["markPrice"]), "index": float(p["indexPrice"]),
                    "funding_rate": float(p["lastFundingRate"]), "next_funding": int(p["nextFundingTime"]),
                    "open_interest": float(oi["openInterest"]), "open_interest_usd": float(oi["openInterest"]) * last}
        return await self._cached(("info", symbol), load)

    async def trading_data(self, symbol: str) -> dict:
        """Sentiment over the last 12 hours, hour by hour: accounts long vs short, taker buy vs sell, open interest."""
        symbol = self.check_symbol(symbol)

        async def load():
            q = {"symbol": symbol, "period": "1h", "limit": 12}
            ls, taker, oi = await asyncio.gather(self._get("/futures/data/globalLongShortAccountRatio", q),
                                                 self._get("/futures/data/takerlongshortRatio", q),
                                                 self._get("/futures/data/openInterestHist", q))
            rows: dict[int, dict] = {}
            for r in ls:
                rows.setdefault(int(r["timestamp"]), {})["long_pct"] = float(r["longAccount"]) * 100
            for r in taker:
                rows.setdefault(int(r["timestamp"]), {})["taker_ratio"] = float(r["buySellRatio"])
            for r in oi:
                rows.setdefault(int(r["timestamp"]), {})["oi_usd"] = float(r["sumOpenInterestValue"])
            return {"symbol": symbol, "rows": [dict(ts=ts, **v) for ts, v in sorted(rows.items(), reverse=True)]}
        return await self._cached(("data", symbol), load)

    async def movers(self) -> dict:
        async def load():
            rows = await self._get("/fapi/v1/ticker/24hr")
            liquid = [{"symbol": r["symbol"], "last": float(r["lastPrice"]), "change_pct": float(r["priceChangePercent"]),
                       "quote_volume": float(r["quoteVolume"])} for r in rows
                      if SYMBOL_RE.match(r["symbol"]) and float(r["quoteVolume"]) >= MOVERS_MIN_VOLUME]
            liquid.sort(key=lambda r: r["change_pct"])
            return {"gainers": liquid[::-1][:MOVERS_N], "losers": liquid[:MOVERS_N],
                    "volume": sorted(liquid, key=lambda r: -r["quote_volume"])[:MOVERS_N]}
        return await self._cached(("movers",), load)
