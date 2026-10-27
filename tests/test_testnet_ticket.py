"""The order ticket on the Binance futures testnet (arbbot/testnet_ticket.py), against a fake testnet exchange."""
import tempfile
import unittest
from pathlib import Path

from arbbot.testnet_ticket import TestnetTicket


class FakeTestnet:
    """Just enough of the futures testnet: filters, quotes, orders (with the testnet habit of omitting avgPrice)."""

    def __init__(self):
        self.hosts = {"fapi": "https://testnet.binancefuture.com", "spot": "https://testnet.binance.vision"}
        self.posts: list[dict] = []
        self.deleted: list[int] = []
        self.next_id = 100
        self.open: list[dict] = []
        self.positions: dict[str, float] = {}
        self.status_of: dict[int, str] = {}
        self.algo: list[dict] = []

    async def get(self, venue, path, params=None, signed=False, timeout=None):
        p = params or {}
        if path == "/fapi/v1/exchangeInfo":
            return {"symbols": [{"symbol": s, "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "100"}]} for s in ("BTCUSDT", "ETHUSDT")]}
        if path == "/fapi/v1/ticker/bookTicker":
            return {"bidPrice": "79999.9", "askPrice": "80000.1"}
        if path == "/fapi/v1/ticker/price":
            return {"price": "80000.0"}
        if path == "/fapi/v1/depth":
            return {"bids": [["79999.9", "1"]], "asks": [["80000.1", "0.01"], ["80800.0", "5"]]}
        if path == "/fapi/v1/order":
            oid = p["orderId"]
            return {"orderId": oid, "symbol": p["symbol"], "status": self.status_of.get(oid, "FILLED"),
                    "executedQty": "0.002", "avgPrice": "80000.5", "side": "BUY", "type": "MARKET"}
        if path in ("/fapi/v3/account", "/fapi/v2/account"):
            return {"assets": [{"asset": "USDT", "walletBalance": "5000", "availableBalance": "4800"}]}
        if path in ("/fapi/v3/positionRisk", "/fapi/v2/positionRisk"):
            return [{"symbol": s, "positionAmt": str(a), "entryPrice": "80000", "markPrice": "80100", "leverage": "15",
                     "isolatedMargin": "10.7", "unRealizedProfit": "0.2", "liquidationPrice": "75000"}
                    for s, a in self.positions.items()]
        if path == "/fapi/v1/openOrders":
            return [o for o in self.open if o["symbol"] == p["symbol"]]
        if path == "/fapi/v1/openAlgoOrders":
            return [a for a in self.algo if a["symbol"] == p["symbol"]]
        if path == "/fapi/v1/userTrades":
            return []
        raise AssertionError(path)

    async def post(self, venue, path, params=None, signed=True):
        if path in ("/fapi/v1/marginType", "/fapi/v1/leverage"):
            return {"leverage": int(params.get("leverage", 15))} if path.endswith("leverage") else {}
        self.posts.append(dict(params, _path=path))
        self.next_id += 1
        t = params["type"]
        if path == "/fapi/v1/algoOrder":                    # conditional orders: the Algo Order API
            assert params["algoType"] == "CONDITIONAL" and "stopPrice" not in params, params
            self.algo.append({"algoId": self.next_id, "symbol": params["symbol"], "side": params["side"],
                              "orderType": t, "quantity": params.get("quantity", ""), "price": params.get("price", "0"),
                              "triggerPrice": params["triggerPrice"], "closePosition": params.get("closePosition") == "true",
                              "workingType": params.get("workingType", "CONTRACT_PRICE"), "algoStatus": "NEW"})
            return {"algoId": self.next_id, "algoStatus": "NEW"}
        assert path == "/fapi/v1/order", path
        assert t in ("LIMIT", "MARKET"), f"{t} must go to the Algo API (Binance answers -4120 here)"
        if t == "MARKET":                                   # the testnet reply: FILLED, but no avgPrice / cumQuote
            q = float(params["quantity"]) * (1 if params["side"] == "BUY" else -1)
            self.positions[params["symbol"]] = self.positions.get(params["symbol"], 0.0) + q
            return {"orderId": self.next_id, "status": "FILLED", "executedQty": params["quantity"], "side": params["side"],
                    "type": t, "origQty": params["quantity"]}
        o = {"orderId": self.next_id, "symbol": params["symbol"], "status": "NEW", "executedQty": "0",
             "side": params["side"], "type": t, "origQty": params.get("quantity", "0"), "price": params.get("price", "0"),
             "stopPrice": params.get("stopPrice", "0"), "closePosition": params.get("closePosition") == "true",
             "workingType": params.get("workingType", "CONTRACT_PRICE"), "timeInForce": params.get("timeInForce", "GTC")}
        self.open.append(o)
        return o

    async def request(self, venue, method, path, params=None, signed=False, timeout=None):
        assert method == "DELETE"
        if path == "/fapi/v1/algoOrder":
            self.deleted.append(f"a{params['algoId']}")
            self.algo = [a for a in self.algo if a["algoId"] != params["algoId"]]
            return {"algoId": params["algoId"], "code": "200", "msg": "success"}
        self.deleted.append(params["orderId"])
        self.open = [o for o in self.open if o["orderId"] != params["orderId"]]
        return {"orderId": params["orderId"], "status": "CANCELED", "symbol": params["symbol"], "side": "BUY",
                "type": "LIMIT", "origQty": "0.002"}


class TicketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ex = FakeTestnet()
        self.arb = {"ETHUSDT"}
        self.path = Path(tempfile.mkdtemp()) / "ticket.json"
        self.sent = []
        self.t = TestnetTicket(self.ex, self.path, arb_symbols=lambda: self.arb, notify=self.sent.append)

    def order(self, **kw):
        return self.t.place_order(dict(symbol="BTCUSDT", **kw))

    async def test_it_refuses_anything_but_a_testnet_client(self):
        ex = FakeTestnet()
        ex.hosts = {"fapi": "https://fapi.binance.com", "spot": "https://api.binance.com"}
        with self.assertRaises(RuntimeError):
            TestnetTicket(ex, None)

    async def test_coins_held_by_the_arbitrage_bot_are_refused(self):
        with self.assertRaises(ValueError) as cm:
            await self.t.place_order(dict(symbol="ETHUSDT", side="buy", type="market", size_usdt=200))
        self.assertIn("arbitrage bot", str(cm.exception))
        self.assertEqual(self.ex.posts, [])

    async def test_market_order_is_sent_rounded_and_priced_even_without_avgprice(self):
        o = await self.order(side="buy", type="market", size_usdt=200)
        sent = self.ex.posts[-1]
        self.assertEqual((sent["type"], sent["side"], sent["quantity"]), ("MARKET", "BUY", "0.002"))   # 200/80000.1, step
        self.assertEqual((o["status"], o["filled_px"]), ("filled", 80000.5))                         # read back
        self.assertEqual(self.t.owned(), {"BTCUSDT"})
        self.assertIn("TESTNET FUTURES BUY", self.sent[0])
        self.assertTrue(self.path.exists())

    async def test_limit_order_rounds_the_price_to_the_tick(self):
        o = await self.order(side="sell", type="limit", price=81234.567, size_usdt=200, tif="IOC")
        sent = self.ex.posts[-1]
        self.assertEqual((sent["type"], sent["price"], sent["timeInForce"]), ("LIMIT", "81234.6", "IOC"))
        self.assertEqual(o["status"], "new")

    async def test_conditional_orders_trigger_in_the_requested_direction(self):
        cases = [("buy", 81000, "STOP_MARKET"), ("buy", 79000, "TAKE_PROFIT_MARKET"),
                 ("sell", 79000, "STOP_MARKET"), ("sell", 81000, "TAKE_PROFIT_MARKET")]
        for side, stop, want in cases:
            await self.order(side=side, type="stop_market", stop_price=stop, size_usdt=200, trigger="mark")
            self.assertEqual((self.ex.posts[-1]["type"], self.ex.posts[-1]["workingType"]), (want, "MARK_PRICE"))
        await self.order(side="buy", type="stop_limit", stop_price=81000, price=81100, size_usdt=200)
        self.assertEqual((self.ex.posts[-1]["type"], self.ex.posts[-1]["workingType"]), ("STOP", "CONTRACT_PRICE"))

    async def test_tp_sl_follow_a_market_fill_as_close_position_orders(self):
        await self.order(side="buy", type="market", size_usdt=200, tp=84000, sl=78000)
        exits = [p for p in self.ex.posts if p.get("closePosition") == "true"]
        self.assertEqual(sorted((p["type"], p["side"], p["triggerPrice"], p["_path"]) for p in exits),
                         [("STOP_MARKET", "SELL", "78000", "/fapi/v1/algoOrder"),
                          ("TAKE_PROFIT_MARKET", "SELL", "84000", "/fapi/v1/algoOrder")])
        await self.t.refresh()
        reasons = sorted(o["reason"] for o in self.t.view()["orders"])
        self.assertEqual(reasons, ["stop-loss", "take-profit"])           # listed from openAlgoOrders

    async def test_tp_sl_on_a_resting_limit_wait_for_its_fill(self):
        o = await self.order(side="buy", type="limit", price=79000, size_usdt=200, tp=84000, sl=78000)
        self.assertEqual([p for p in self.ex.posts if p.get("closePosition") == "true"], [])
        self.assertIn(str(o["id"]), self.t.s["tpsl"])
        self.ex.open = []                                    # the limit filled on the exchange
        self.ex.positions["BTCUSDT"] = 0.002
        await self.t.refresh()
        self.assertEqual(len([p for p in self.ex.posts if p.get("closePosition") == "true"]), 2)
        self.assertEqual(self.t.s["tpsl"], {})

    async def test_refusals_never_reach_the_exchange(self):
        bad = [dict(side="buy", type="market", size_usdt=50),                 # below the 100 USDT minimum
               dict(side="buy", type="market", qty=0.0001),                   # below the minimum quantity
               dict(side="buy", type="market", size_usdt=200, tp=70000),      # TP below a long's entry
               dict(side="buy", type="twap", size_usdt=200)]
        for req in bad:
            with self.subTest(req=req), self.assertRaises(ValueError):
                await self.order(**req)
        with self.assertRaises(ValueError):                  # a big market order would walk 1% up this thin book
            await self.order(side="buy", type="market", qty=1.0, slippage_pct=0.5)
        with self.assertRaises(ValueError):
            await self.t.transfer(100)
        self.assertEqual(self.ex.posts, [])

    async def test_conditional_orders_use_the_algo_api_and_cancel_there(self):
        o = await self.order(side="buy", type="stop_market", stop_price=81000, size_usdt=200)
        self.assertTrue(str(o["id"]).startswith("a"))
        self.assertEqual(self.ex.posts[-1]["_path"], "/fapi/v1/algoOrder")
        await self.t.refresh()
        listed = [x for x in self.t.view()["orders"] if x["id"] == o["id"]]
        self.assertEqual((listed[0]["type"], listed[0]["stop_price"]), ("stop_market", 81000.0))
        await self.t.cancel(o["id"])
        self.assertEqual(self.ex.deleted, [o["id"]])
        self.assertEqual(self.ex.algo, [])
        with self.assertRaises(ValueError):                  # TP/SL on a not-yet-triggered stop: refused on testnet
            await self.order(side="buy", type="stop_market", stop_price=81000, size_usdt=200, tp=85000)

    async def test_a_trailing_stop_replaces_only_the_stop_loss(self):
        await self.order(side="buy", type="market", size_usdt=200, tp=84000, sl=78000)
        await self.t.replace_stop("BTCUSDT", "buy", 79000)
        self.assertEqual(sorted((a["orderType"], a["triggerPrice"]) for a in self.ex.algo),
                         [("STOP_MARKET", "79000"), ("TAKE_PROFIT_MARKET", "84000")])
        self.assertEqual(self.t.position("BTCUSDT"), 0.002)

    async def test_tp_sl_are_tidied_when_the_position_closes(self):
        await self.order(side="buy", type="market", size_usdt=200, tp=84000, sl=78000)
        self.assertEqual(len(self.ex.algo), 2)
        self.ex.positions["BTCUSDT"] = 0.0                   # closed elsewhere: its closePosition orders are now orphans
        await self.t.refresh()
        self.assertEqual(self.ex.algo, [])

    async def test_cancel_and_close(self):
        o = await self.order(side="buy", type="limit", price=79000, size_usdt=200)
        await self.t.refresh()
        await self.t.cancel(o["id"])
        self.assertEqual(self.ex.deleted, [o["id"]])
        await self.order(side="buy", type="market", size_usdt=200)
        out = await self.t.close_position("BTCUSDT")
        last = self.ex.posts[-1]
        self.assertEqual((last["side"], last["reduceOnly"], last["quantity"]), ("SELL", "true", "0.002"))
        self.assertEqual(out["status"], "filled")

    async def test_view_reads_the_account_and_hides_the_arbitrage_coins(self):
        self.ex.positions = {"BTCUSDT": 0.002, "ETHUSDT": -0.5}
        await self.t.refresh()
        v = self.t.view()
        self.assertEqual(v["venue"], "testnet")
        self.assertEqual([p["symbol"] for p in v["positions"]], ["BTCUSDT"])  # ETH is the arbitrage bot's
        p = v["positions"][0]
        self.assertEqual((p["side"], p["entry"], p["mark"], p["liq"], p["leverage"]), ("long", 80000.0, 80100.0, 75000.0, 15))
        self.assertEqual((v["balance"], v["available"]), (5000.0, 4800.0))


class RealMoneyTests(unittest.IsolatedAsyncioTestCase):
    """--live --real-ticket: the same ticket on a MAINNET client, only by explicit opt-in and with a cap per order.
    Every order here goes to the fake exchange; nothing reaches Binance."""

    def mainnet(self):
        ex = FakeTestnet()
        ex.hosts = {"fapi": "https://fapi.binance.com", "spot": "https://api.binance.com"}
        return ex

    async def asyncSetUp(self):
        self.ex, self.sent, self.why = self.mainnet(), [], ""
        self.t = TestnetTicket(self.ex, None, notify=self.sent.append, real_money=True, max_order_usd=300,
                               default_leverage=2, blocked=lambda: self.why)

    async def test_a_mainnet_client_needs_the_opt_in_and_a_cap(self):
        with self.assertRaises(RuntimeError):
            TestnetTicket(self.mainnet(), None)
        with self.assertRaises(RuntimeError):
            TestnetTicket(self.mainnet(), None, real_money=True)
        self.assertEqual(self.t.venue, "mainnet")
        self.assertEqual(TestnetTicket(FakeTestnet(), None, real_money=True).venue, "testnet")

    async def test_orders_within_the_cap_are_sent_and_announced_as_real_money(self):
        o = await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", size_usdt=200))
        self.assertEqual((o["status"], self.ex.posts[-1]["quantity"]), ("filled", "0.002"))
        self.assertIn("MAINNET FUTURES BUY", self.sent[0])
        self.assertIn("REAL MONEY", self.sent[0])
        await self.t.refresh()
        v = self.t.view()
        self.assertEqual((v["venue"], v["max_order_usd"], v["default_leverage"]), ("mainnet", 300, 2))

    async def test_an_order_above_the_cap_is_refused_but_closing_never_is(self):
        with self.assertRaises(ValueError) as cm:
            await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", size_usdt=500))
        self.assertIn("cap", str(cm.exception))
        self.assertEqual(self.ex.posts, [])
        for _ in range(2):                                   # 2 x 160 USDT: a 320 USDT position, above the cap
            await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", qty=0.002))
        out = await self.t.close_position("BTCUSDT")
        self.assertEqual((self.ex.posts[-1]["reduceOnly"], self.ex.posts[-1]["quantity"]), ("true", "0.004"))
        self.assertEqual(out["status"], "filled")

    async def test_the_kill_file_blocks_new_orders_but_not_closing(self):
        await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", qty=0.002))
        self.why = "the kill file KILL is present"
        with self.assertRaises(ValueError) as cm:
            await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", qty=0.002))
        self.assertIn("kill file", str(cm.exception))
        await self.t.close_position("BTCUSDT")
        self.assertEqual(self.ex.posts[-1]["reduceOnly"], "true")

    async def test_the_new_margin_uses_the_configured_leverage(self):
        calls = []
        post = self.ex.post

        async def spy(venue, path, params=None, signed=True):
            calls.append((path, dict(params or {})))
            return await post(venue, path, params, signed)
        self.ex.post = spy
        await self.t.place_order(dict(symbol="BTCUSDT", side="buy", type="market", qty=0.002))
        self.assertIn(("/fapi/v1/leverage", {"symbol": "BTCUSDT", "leverage": 2}), calls)

    async def test_no_transfers(self):
        with self.assertRaises(ValueError) as cm:
            await self.t.transfer(100)
        self.assertIn("Binance app", str(cm.exception))


class RealTicketFlagTests(unittest.TestCase):
    def test_the_flag_is_refused_outside_mainnet_live(self):
        import asyncio
        import os
        from unittest import mock
        from arbbot.main import amain, parse_args
        keys = {k: "x" for k in ("BINANCE_TESTNET_FUTURES_API_KEY", "BINANCE_TESTNET_FUTURES_API_SECRET",
                                 "BINANCE_TESTNET_SPOT_API_KEY", "BINANCE_TESTNET_SPOT_API_SECRET")}
        for argv in (["--real-ticket"], ["--real-ticket", "--live", "--testnet", "--config", "config.testnet.toml"]):
            with mock.patch("arbbot.main.load_dotenv"), mock.patch.dict(os.environ, keys), \
                    self.assertRaises(SystemExit) as cm:          # never read the real .env, never connect
                asyncio.run(amain(parse_args(argv)))
            self.assertIn("--real-ticket", str(cm.exception))
        self.assertEqual(parse_args([]).real_ticket_max_usd, 100.0)


class SeparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_arbitrage_bot_skips_the_tickets_coins(self):
        from arbbot.models import Signal
        from arbbot.portfolio import Portfolio, Store
        from arbbot.risk import Risk
        from tests.test_core import small_cfg
        pf = Portfolio(Store(Path(":memory:")), 1000.0)
        r = Risk(small_cfg(tempfile.mkdtemp()).risk, pf)
        sig = Signal("BTCUSDT", 0.0, fillable_usd=10_000.0)
        self.assertNotEqual(r.size_entry(sig, 0.0)[1], "held by the order ticket")
        r.external = lambda: {"BTCUSDT"}
        self.assertEqual(r.size_entry(sig, 0.0), (0.0, "held by the order ticket"))

    async def test_startup_check_ignores_the_tickets_coins(self):
        from arbbot.config import Config
        from arbbot.live import LiveExecutor
        from arbbot.models import Market

        class C:
            async def get(self, venue, path, params=None, signed=False):
                if "positionRisk" in path:
                    return [{"symbol": "BTCUSDT", "positionAmt": "0.002"}]
                return {"balances": []}

        ex = LiveExecutor(C(), Config())
        markets = {"BTCUSDT": Market("BTCUSDT", "BTCUSDT", "BTC")}
        self.assertEqual(len(await ex.reconcile({}, markets)), 1)                 # an unknown position: flagged
        self.assertEqual(await ex.reconcile({}, markets, ignore={"BTCUSDT"}), [])  # the ticket's: fine


if __name__ == "__main__":
    unittest.main()
