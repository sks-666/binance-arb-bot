"""The dashboard's PAPER futures account (arbbot/paper_futures.py) and its /api/paper routes."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.paper_futures import MMR, PaperFutures, liq_price, walk_book
from arbbot.server import Dashboard

TAKER, MAKER = 5e-4, 2e-4


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.clock = Clock()
        self.sent = []
        self.pf = PaperFutures(self.tmp / "pf.json", None, 5.0, self.sent.append, self.clock)
        self.quote(100.0)

    def quote(self, last, mark=None, bid=None, ask=None, sym="BTCUSDT", book=None, rate=0.0001, next_funding=None):
        q = dict(last=last, mark=last if mark is None else mark, bid=last - 0.1 if bid is None else bid,
                 ask=last + 0.1 if ask is None else ask, rate=rate,
                 next_funding=next_funding or int(self.clock.t * 1000) + 3_600_000, ts=self.clock.t)
        if book:
            q["book"] = book
        self.pf.quotes[sym] = q

    def order(self, **kw):
        return self.pf._place(dict(symbol="BTCUSDT", **kw))

    def tick(self, last, **kw):
        self.clock.t += 1
        self.quote(last, **kw)
        return self.pf._tick()

    def pos(self, sym="BTCUSDT"):
        return self.pf.s["positions"].get(sym)


class HelperTests(unittest.TestCase):
    def test_walk_book(self):
        book = [[100.0, 1.0], [101.0, 1.0]]
        self.assertAlmostEqual(walk_book(book, 1.5), (100 + 0.5 * 101) / 1.5)
        self.assertAlmostEqual(walk_book(book, 3.0), (100 + 101 + 101 * 1.001) / 3)          # beyond the book: pushed
        self.assertAlmostEqual(walk_book(book, 3.0, limit=100.5), (100 + 2 * 100.5) / 3)     # capped at the limit
        self.assertAlmostEqual(walk_book([[99.0, 1.0]], 1.0, buy=False), 99.0)

    def test_liquidation_price(self):
        # 10x long from 100: margin 10 per coin -> liq where the margin left equals MMR of the value
        self.assertAlmostEqual(liq_price(1.0, 100.0, 10.0), 90.0 / (1 - MMR))
        self.assertAlmostEqual(liq_price(-1.0, 100.0, 10.0), 110.0 / (1 + MMR))
        self.assertIsNone(liq_price(0, 100.0, 10.0))


class OrderTests(Base):
    def test_market_buy_opens_a_long_with_margin_and_taker_fee(self):
        o = self.order(side="buy", type="market", size_usdt=1000)
        self.assertEqual(o["status"], "filled")
        self.assertAlmostEqual(o["filled_px"], 100.1)                        # at the ask
        p = self.pos()
        qty = float(f"{1000 / 100.1:.8g}")
        self.assertAlmostEqual(p["qty"], qty)
        self.assertAlmostEqual(p["margin"], qty * 100.1 / 15)                # default 15x
        self.assertAlmostEqual(self.pf.s["balance"], 1000 - qty * 100.1 * TAKER)
        self.assertAlmostEqual(self.pf.available(), self.pf.s["balance"] - p["margin"])
        self.assertIn("nothing was sent to Binance", self.sent[0])

    def test_market_order_walks_the_book_and_respects_slippage_tolerance(self):
        self.quote(100.0, book={"asks": [[100.1, 1.0], [101.0, 100.0]], "bids": [[99.9, 100.0]]})
        o = self.order(side="buy", type="market", qty=2.0, slippage_pct=0.1)
        self.assertEqual((o["status"], o["reason"][:10]), ("rejected", "would slip"))
        self.assertIsNone(self.pos())
        o = self.order(side="buy", type="market", qty=2.0)
        self.assertAlmostEqual(o["filled_px"], (100.1 + 101.0) / 2)

    def test_resting_limit_reserves_margin_fills_as_maker_and_cancel_releases(self):
        o = self.order(side="buy", type="limit", price=95.0, size_usdt=950)
        self.assertEqual(o["status"], "new")
        self.assertAlmostEqual(self.pf.available(), 1000 - (10 * 95 / 15 + 10 * 95 * TAKER))
        self.assertFalse(self.tick(97.0))
        self.assertTrue(self.tick(94.9))
        self.assertEqual(self.pf.s["order_history"][-1]["status"], "filled")
        self.assertAlmostEqual(self.pos()["entry"], 95.0)
        self.assertAlmostEqual(self.pf.s["balance"], 1000 - 950 * MAKER)
        o2 = self.order(side="buy", type="limit", price=90.0, size_usdt=90)
        before = self.pf.available()
        self.pf._archive(next(x for x in self.pf.s["orders"] if x["id"] == o2["id"]), "canceled")
        self.assertGreater(self.pf.available(), before)

    def test_marketable_limit_fills_now_as_taker_and_ioc_expires_otherwise(self):
        o = self.order(side="sell", type="limit", price=99.0, size_usdt=99)
        self.assertEqual((o["status"], o["filled_px"]), ("filled", 99.9))    # at the bid, better than the limit
        o = self.order(side="sell", type="limit", price=150.0, size_usdt=150, tif="IOC")
        self.assertEqual(o["status"], "expired")

    def test_bbo_prices_at_our_side_of_the_book(self):
        o = self.order(side="buy", type="limit", bbo=True, size_usdt=100)
        self.assertEqual((o["price"], o["status"]), (99.9, "new"))           # best bid: rests at queue 1
        o = self.order(side="sell", type="limit", bbo=True, size_usdt=100)
        self.assertEqual(o["price"], 100.1)

    def test_closing_realises_pnl_and_releases_margin(self):
        self.order(side="buy", type="market", qty=10)                        # in at 100.1
        self.tick(110.0)
        o = self.order(side="sell", type="market", qty=10, reduce_only=True)  # out at 109.9
        self.assertAlmostEqual(o["realized"], (109.9 - 100.1) * 10)
        self.assertIsNone(self.pos())
        self.assertAlmostEqual(self.pf.available(), self.pf.s["balance"])     # nothing locked any more
        fees = 1001 * TAKER + 1099 * TAKER
        self.assertAlmostEqual(self.pf.s["balance"], 1000 + 98 - fees, places=6)

    def test_an_order_bigger_than_the_position_flips_it(self):
        self.order(side="buy", type="market", qty=5)
        self.order(side="sell", type="market", qty=8)
        self.assertAlmostEqual(self.pos()["qty"], -3)
        self.assertAlmostEqual(self.pos()["entry"], 99.9)

    def test_reduce_only_rules(self):
        with self.assertRaises(ValueError):
            self.order(side="sell", type="market", qty=1, reduce_only=True)    # nothing to reduce
        self.order(side="buy", type="market", qty=2)
        o = self.order(side="sell", type="market", qty=5, reduce_only=True)   # capped at the position
        self.assertEqual(o["qty"], 2)
        self.assertIsNone(self.pos())

    def test_refusals(self):
        bad = [dict(side="buy", type="market", size_usdt=100000),            # more than the margin allows
               dict(side="buy", type="market", size_usdt=1),                 # below the minimum value
               dict(side="buy", type="limit", size_usdt=100),                # no price
               dict(side="buy", type="stop_market", size_usdt=100),          # no stop price
               dict(side="hold", type="market", size_usdt=100),
               dict(side="buy", type="twap", size_usdt=100),
               dict(side="buy", type="market", size_usdt=100, qty=1),        # two sizes
               dict(side="buy", type="market", size_usdt=-5),
               dict(side="buy", type="market", size_usdt=float("nan")),
               dict(side="buy", type="market", size_usdt=100, tp=90),        # TP below a long's entry
               dict(side="sell", type="market", size_usdt=100, sl=90),       # SL below a short's entry
               dict(side="buy", type="limit", price=100, size_usdt=100, tif="DAY")]
        for req in bad:
            with self.subTest(req=req), self.assertRaises(ValueError):
                self.order(**req)
        with self.assertRaises(ValueError):
            self.pf._place(dict(symbol="BTC/USDT", side="buy", type="market", size_usdt=100))
        self.pf.quotes["BTCUSDT"]["ts"] -= 60                                  # a stale price is never traded on
        with self.assertRaises(ValueError):
            self.order(side="buy", type="market", size_usdt=100)
        self.assertEqual((self.pf.s["positions"], self.pf.s["orders"]), ({}, []))

    def test_stop_market_triggers_in_the_right_direction_on_last_or_mark(self):
        o = self.order(side="buy", type="stop_market", stop_price=105.0, size_usdt=105)
        self.assertTrue(o["above"])
        self.assertFalse(self.tick(104.0))
        self.tick(105.5)
        self.assertAlmostEqual(self.pos()["qty"], 1.0)
        o = self.order(side="sell", type="stop_market", stop_price=100.0, qty=1, reduce_only=True, trigger="mark")
        self.assertFalse(o["above"])
        self.assertFalse(self.tick(99.0, mark=101.0))                          # last crossed, mark did not
        self.tick(99.0, mark=99.5)
        self.assertIsNone(self.pos())

    def test_stop_limit_rests_after_triggering(self):
        self.order(side="buy", type="stop_limit", stop_price=105.0, price=104.0, size_usdt=104)
        self.tick(105.2)                                                       # released; ask 105.3 > 104: rests
        o = self.pf.s["orders"][0]
        self.assertTrue(o["triggered"])
        self.assertIsNone(self.pos())
        self.tick(103.9)
        self.assertAlmostEqual(self.pos()["entry"], 104.0)

    def test_tp_and_sl_become_an_oco_pair_after_the_fill(self):
        self.order(side="buy", type="market", qty=1, tp=110.0, sl=95.0)
        kids = self.pf.s["orders"]
        self.assertEqual(sorted(k["reason"] for k in kids), ["stop-loss", "take-profit"])
        self.assertTrue(all(k["reduce_only"] and k["side"] == "sell" for k in kids))
        self.tick(110.5)
        self.assertIsNone(self.pos())
        self.assertEqual(self.pf.s["orders"], [])                              # the stop-loss went with it
        hist = {o["reason"]: o["status"] for o in self.pf.s["order_history"]}
        self.assertEqual(hist["take-profit"], "filled")
        self.assertEqual(hist["the other side of the TP/SL pair filled"], "canceled")

    def test_stop_loss_fires_on_the_way_down(self):
        self.order(side="buy", type="market", qty=1, tp=110.0, sl=95.0)
        self.tick(96.0)
        self.assertIsNotNone(self.pos())
        self.tick(94.5)
        self.assertIsNone(self.pos())
        self.assertLess(self.pf.s["trades"][-1]["realized"], 0)

    def test_closing_by_hand_cancels_the_tp_sl(self):
        self.order(side="buy", type="market", qty=1, tp=110.0, sl=95.0)
        self.order(side="sell", type="market", qty=1, reduce_only=True)
        self.assertEqual(self.pf.s["orders"], [])

    def test_liquidation_takes_the_isolated_margin_only(self):
        self.pf.s["leverage"]["BTCUSDT"] = 10
        self.order(side="buy", type="market", qty=10)                          # 1001 notional, 100.1 margin
        balance = self.pf.s["balance"]
        margin = self.pos()["margin"]
        liq = liq_price(10, 100.1, margin)
        self.tick(liq + 0.5)
        self.assertIsNotNone(self.pos())                                       # just above: still alive
        self.tick(liq - 0.01)
        self.assertIsNone(self.pos())
        self.assertAlmostEqual(self.pf.s["balance"], balance - margin)
        self.assertTrue(self.pf.s["trades"][-1]["liquidation"])
        self.assertIn("LIQUIDATED", self.sent[-1])

    def test_funding_is_paid_at_settlement(self):
        self.order(side="buy", type="market", qty=10)
        settle = int(self.clock.t * 1000) + 5_000
        self.tick(100.0, next_funding=settle, rate=0.001)
        bal = self.pf.s["balance"]
        self.clock.t += 10
        self.quote(100.0, rate=0.001, next_funding=settle + 8 * 3_600_000)
        self.pf._tick()
        self.assertAlmostEqual(bal - self.pf.s["balance"], 10 * 100.0 * 0.001)   # long pays a positive rate
        self.pf._tick()
        self.assertAlmostEqual(bal - self.pf.s["balance"], 1.0)                  # once per settlement

    async def test_leverage_and_transfer(self):
        self.assertEqual(await self.pf.set_leverage("BTCUSDT", 20), 20)
        for bad in (0, 126, "x"):
            with self.assertRaises(ValueError):
                await self.pf.set_leverage("BTCUSDT", bad)
        self.order(side="buy", type="market", qty=1)
        with self.assertRaises(ValueError):
            await self.pf.set_leverage("BTCUSDT", 5)                              # not with a position open
        self.assertAlmostEqual(self.pos()["margin"], 100.1 / 20)
        await self.pf.transfer(500)
        with self.assertRaises(ValueError):
            await self.pf.transfer(-10_000)
        with self.assertRaises(ValueError):
            await self.pf.transfer(0)

    async def test_state_survives_a_restart(self):
        self.order(side="buy", type="market", qty=1, tp=120.0)
        self.pf._save()
        again = PaperFutures(self.tmp / "pf.json", None, 5.0, clock=self.clock)
        self.assertEqual(again.s["positions"].keys(), self.pf.s["positions"].keys())
        self.assertEqual(len(again.s["orders"]), 1)
        self.assertEqual(json.loads((self.tmp / "pf.json").read_text())["next_id"], self.pf.s["next_id"])

    def test_view(self):
        self.order(side="buy", type="market", qty=1)
        self.tick(110.0)
        v = self.pf.view()
        p = v["positions"][0]
        self.assertEqual((p["side"], p["leverage"]), ("long", 15))
        self.assertAlmostEqual(p["pnl"], 110.0 - 100.1)
        self.assertAlmostEqual(v["equity"], v["balance"] + p["pnl"])
        self.assertAlmostEqual(p["roe"], p["pnl"] / p["margin"] * 100)


class RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pf = PaperFutures(None, None, 5.0, clock=Clock())
        self.pf.quotes["BTCUSDT"] = dict(last=100.0, mark=100.0, bid=99.9, ask=100.1, rate=0.0, next_funding=0,
                                         ts=self.pf.clock())
        cfg = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=1), feed=SimpleNamespace(interval_s=1.0))
        self.dash = Dashboard(SimpleNamespace(swing=None), cfg, Path("web"), paper=self.pf)
        server = TestServer(self.dash.make_app())
        await server.start_server()
        self.dash.allowed_hosts.add(f"127.0.0.1:{server.port}")
        self.client = TestClient(server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    def post(self, op, body, token=True):
        return self.client.post(f"/api/paper/{op}", json=body, headers={"X-Token": self.dash.token} if token else {})

    async def test_orders_need_the_token(self):
        r = await self.post("order", dict(symbol="BTCUSDT", side="buy", type="market", size_usdt=100), token=False)
        self.assertEqual(r.status, 403)
        self.assertEqual(self.pf.s["positions"], {})

    async def test_order_cancel_close_flow(self):
        r = await self.post("order", dict(symbol="BTCUSDT", side="buy", type="limit", price=90, size_usdt=90))
        d = await r.json()
        self.assertEqual((r.status, d["result"]["status"], len(d["state"]["orders"])), (200, "new", 1))
        r = await self.post("cancel", {"id": d["result"]["id"]})
        self.assertEqual((await r.json())["state"]["orders"], [])
        await self.post("order", dict(symbol="BTCUSDT", side="buy", type="market", size_usdt=100))
        r = await self.post("close", {"symbol": "BTCUSDT"})
        self.assertEqual((await r.json())["state"]["positions"], [])
        r = await self.client.get("/api/paper/state")
        self.assertEqual((r.status, len((await r.json())["order_history"])), (200, 3))

    async def test_refusals_are_400_with_a_reason(self):
        r = await self.post("order", dict(symbol="BTCUSDT", side="buy", type="market", size_usdt=10 ** 9))
        self.assertEqual(r.status, 400)
        self.assertIn("insufficient balance", (await r.json())["error"])
        r = await self.post("leverage", {"symbol": "BTCUSDT", "leverage": 500})
        self.assertEqual(r.status, 400)
        r = await self.post("nope", {})
        self.assertEqual(r.status, 404)

    async def test_without_the_account_it_says_so(self):
        self.dash.paper = None
        self.assertEqual((await self.client.get("/api/paper/state")).status, 404)
