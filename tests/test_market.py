"""The market panel's read-only Binance relay (arbbot/market.py) and its /api/market routes."""
import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.market import TTL, MarketData
from arbbot.server import Dashboard

KLINE = [1_700_000_000_000, "100.0", "101.5", "99.5", "101.0", "12.5", 1_700_003_599_999, "1262.5", 10, "6", "606", "0"]


class FakeBinance:
    """Answers the public endpoints the relay uses and records every call."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.fail = None

    async def get(self, venue, path, params=None, timeout=None):
        assert venue == "fapi"
        self.calls.append((path, dict(params or {})))
        await asyncio.sleep(0)
        if self.fail:
            raise self.fail
        p = params or {}
        if path == "/fapi/v1/klines":
            return [KLINE] * 3
        if path == "/fapi/v1/depth":
            return {"bids": [["100.0", "2"], ["99.9", "1"]], "asks": [["100.1", "3"]], "E": 1}
        if path == "/fapi/v1/trades":
            return [{"price": "100.0", "quoteQty": "200.0", "time": 1, "isBuyerMaker": True},
                    {"price": "100.1", "quoteQty": "50.0", "time": 2, "isBuyerMaker": False}]
        if path == "/fapi/v1/ticker/24hr" and "symbol" in p:
            return {"lastPrice": "100", "priceChangePercent": "1.5", "highPrice": "105", "lowPrice": "95", "volume": "1000",
                    "quoteVolume": "100000"}
        if path == "/fapi/v1/ticker/24hr":
            return [{"symbol": s, "lastPrice": "1", "priceChangePercent": str(c), "quoteVolume": str(v)} for s, c, v in
                    [("AAAUSDT", 12, 5e7), ("BBBUSDT", -8, 9e7), ("CCCUSDT", 3, 3e8), ("THINUSDT", 90, 1e5),
                     ("BTCUSDT_261225", 50, 9e9), ("DDDUSDC", 40, 9e9)]]
        if path == "/fapi/v1/premiumIndex":
            return {"markPrice": "100.05", "indexPrice": "100.02", "lastFundingRate": "0.0001", "nextFundingTime": 9}
        if path == "/fapi/v1/openInterest":
            return {"openInterest": "50"}
        if path == "/futures/data/globalLongShortAccountRatio":
            return [{"timestamp": 10, "longAccount": "0.6"}, {"timestamp": 20, "longAccount": "0.55"}]
        if path == "/futures/data/takerlongshortRatio":
            return [{"timestamp": 20, "buySellRatio": "1.2"}]
        if path == "/futures/data/openInterestHist":
            return [{"timestamp": 20, "sumOpenInterestValue": "5000000"}]
        raise AssertionError(f"unexpected {path}")


class MarketDataTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.now = [0.0]
        self.api = FakeBinance()
        self.m = MarketData(self.api, clock=lambda: self.now[0])

    async def test_shapes(self):
        k = await self.m.klines("btcusdt", "1h")
        self.assertEqual(k[0], [1_700_000_000_000, 100.0, 101.5, 99.5, 101.0, 12.5, 1262.5])
        self.assertEqual(self.api.calls[0], ("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 300}))
        d = await self.m.depth("BTCUSDT")
        self.assertEqual((d["bids"][0], d["asks"][0]), ([100.0, 2.0], [100.1, 3.0]))
        t = await self.m.trades("BTCUSDT")
        self.assertEqual([(x["price"], x["sell"]) for x in t], [(100.1, False), (100.0, True)])   # newest first
        i = await self.m.info("BTCUSDT")
        self.assertEqual((i["mark"], i["funding_rate"], i["open_interest_usd"]), (100.05, 0.0001, 5000.0))
        data = await self.m.trading_data("BTCUSDT")
        self.assertEqual(data["rows"][0]["ts"], 20)
        self.assertAlmostEqual(data["rows"][0]["long_pct"], 55.0)
        self.assertEqual((data["rows"][0]["taker_ratio"], data["rows"][0]["oi_usd"]), (1.2, 5e6))
        self.assertEqual(data["rows"][1], {"ts": 10, "long_pct": 60.0})

    async def test_movers_keep_liquid_usdt_perpetuals_only(self):
        mv = await self.m.movers()
        self.assertEqual([r["symbol"] for r in mv["gainers"]], ["AAAUSDT", "CCCUSDT", "BBBUSDT"])
        self.assertEqual(mv["losers"][0]["symbol"], "BBBUSDT")
        self.assertEqual(mv["volume"][0]["symbol"], "CCCUSDT")

    async def test_bad_input_never_reaches_binance(self):
        for bad in ("", "BTC/USDT", "BTCUSDC", "../x", "A" * 40 + "USDT"):
            with self.assertRaises(ValueError):
                await self.m.depth(bad)
        with self.assertRaises(ValueError):
            await self.m.klines("BTCUSDT", "7m")
        self.assertEqual(self.api.calls, [])

    async def test_answers_are_cached_and_concurrent_asks_share_one_request(self):
        await asyncio.gather(*(self.m.depth("BTCUSDT") for _ in range(5)))
        self.assertEqual(len(self.api.calls), 1)
        await self.m.depth("BTCUSDT")
        self.assertEqual(len(self.api.calls), 1)                  # still fresh
        self.now[0] += TTL["depth"] + 0.01
        await self.m.depth("BTCUSDT")
        self.assertEqual(len(self.api.calls), 2)                  # expired: asked again
        await self.m.depth("ETHUSDT")
        self.assertEqual(len(self.api.calls), 3)                  # another market is another answer

    async def test_a_failure_is_not_cached(self):
        self.api.fail = RuntimeError("down")
        with self.assertRaises(RuntimeError):
            await self.m.trades("BTCUSDT")
        self.api.fail = None
        self.assertEqual(len(await self.m.trades("BTCUSDT")), 2)


class RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.api = FakeBinance()
        cfg = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=1), feed=SimpleNamespace(interval_s=1.0))
        self.dash = Dashboard(SimpleNamespace(swing=None), cfg, Path("web"), market=MarketData(self.api))
        server = TestServer(self.dash.make_app())
        await server.start_server()
        self.dash.allowed_hosts.add(f"127.0.0.1:{server.port}")
        self.client = TestClient(server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_routes(self):
        r = await self.client.get("/api/market/klines?symbol=BTCUSDT&interval=4h")
        self.assertEqual((r.status, len(await r.json())), (200, 3))
        r = await self.client.get("/api/market/movers")
        self.assertEqual(r.status, 200)
        r = await self.client.get("/api/market/depth?symbol=nope")
        self.assertEqual(r.status, 400)
        r = await self.client.get("/api/market/whatever?symbol=BTCUSDT")
        self.assertEqual(r.status, 404)
        self.api.fail = RuntimeError("binance down")
        r = await self.client.get("/api/market/trades?symbol=ETHUSDT")
        self.assertEqual((r.status, (await r.json())["error"]), (502, "RuntimeError: binance down"))

    async def test_cross_site_pages_are_refused(self):
        r = await self.client.get("/api/market/movers", headers={"Origin": "http://evil.example"})
        self.assertEqual(r.status, 403)

    async def test_without_a_relay_it_says_so(self):
        self.dash.market = None
        r = await self.client.get("/api/market/movers")
        self.assertEqual(r.status, 404)

    async def test_the_page_carries_the_panel_and_the_hosted_copy_does_not(self):
        root = Path(__file__).resolve().parent.parent
        page = (root / "web" / "index.html").read_text(encoding="utf-8")
        hosted = (root / "vercel-dashboard" / "views" / "dashboard.html").read_text(encoding="utf-8")
        for part in ('id="mk-canvas"', 'id="bk-asks"', 'id="mk-trades"', 'id="mk-movers"', "/api/market/"):
            self.assertIn(part, page)
            self.assertNotIn(part, hosted)
