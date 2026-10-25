"""Live entry orders when Binance's futures reply is incomplete or lost (found on testnet: no avgPrice in the reply).

The bug it guards against: a futures sell FILLED, the reply had no avgPrice, the bot read that as "futures leg failed",
sold the spot leg back and left an unhedged short it did not know about.
"""
import asyncio
import unittest

from arbbot.binance_client import BinanceError
from arbbot.config import Config
from arbbot.executor import UnhedgedError
from arbbot.live import LiveExecutor
from arbbot.models import Market, Quote, Signal, SymbolFilter


class FakeExchange:
    def __init__(self, reply="bare", reget_avg=None, fills=None, post_error=None, fill_before_error=True,
                 position_error=None):
        self.reply, self.reget_avg, self.fills = reply, reget_avg, fills
        self.post_error, self.fill_before_error, self.position_error = post_error, fill_before_error, position_error
        self.fut_position = 0.0
        self.calls: list[tuple[str, str, dict]] = []

    async def post(self, venue, path, params=None, signed=True):
        self.calls.append(("POST", path, dict(params or {})))
        if path in ("/fapi/v1/marginType", "/fapi/v1/leverage"):
            return {}
        qty = float(params["quantity"])
        if path == "/fapi/v1/order":
            if self.post_error and not self.fill_before_error:
                raise self.post_error                       # refused before anything filled
            self.fut_position += qty * (-1 if params["side"] == "SELL" else 1)
            if self.post_error:
                raise self.post_error                       # filled on the exchange, but the reply was lost
            out = {"orderId": 7, "status": "FILLED", "executedQty": params["quantity"]}
            if self.reply == "avg":
                out["avgPrice"] = "100.0"
            elif self.reply == "cumquote":
                out["cumQuote"] = str(100.0 * qty)
            return out
        if path == "/api/v3/order":
            return {"status": "FILLED", "executedQty": params["quantity"], "cummulativeQuoteQty": str(100.2 * qty),
                    "fills": [{"price": "100.2", "qty": params["quantity"], "commission": "0", "commissionAsset": "USDT"}]}
        raise AssertionError(path)

    async def get(self, venue, path, params=None, signed=False):
        self.calls.append(("GET", path, dict(params or {})))
        if path == "/fapi/v1/order":
            out = {"orderId": 7, "status": "FILLED", "executedQty": "1"}
            if self.reget_avg:
                out["avgPrice"] = self.reget_avg
            return out
        if path == "/fapi/v1/userTrades":
            return self.fills or []
        if path == "/fapi/v1/premiumIndex":
            return {"markPrice": "99.9"}
        if path in ("/fapi/v3/positionRisk", "/fapi/v2/positionRisk"):
            if self.position_error:
                raise self.position_error
            return [{"symbol": "ETHUSDT", "positionAmt": str(self.fut_position), "entryPrice": "100.0"}]
        raise AssertionError(path)

    async def request(self, venue, method, path, params=None, signed=False, timeout=None):
        raise AssertionError(f"unexpected {method} {path}")

    def spot_sells(self):
        return [c for c in self.calls if c[1] == "/api/v3/order" and c[2]["side"] == "SELL"]


def market():
    m = Market("ETHUSDT", "ETHUSDT", "ETH")
    m.fut_filter = SymbolFilter(step=0.001, min_qty=0.001, min_notional=5.0)
    m.spot_filter = SymbolFilter(step=0.001, min_qty=0.001, min_notional=5.0)
    m.perp = Quote(100.0, 50.0, 100.1, 50.0, 0.0)
    m.spot = Quote(100.1, 50.0, 100.2, 50.0, 0.0)
    return m


async def open_with(ex: FakeExchange):
    return await LiveExecutor(ex, Config()).open(market(), Signal("ETHUSDT", 0.0), 100.0, 0.0)


class FuturesReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_normal_reply_still_works(self):
        ex = FakeExchange(reply="avg")
        pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_entry, 100.0)
        self.assertEqual(ex.spot_sells(), [])

    async def test_reply_without_avgprice_uses_cumquote(self):
        ex = FakeExchange(reply="cumquote")
        pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_entry, 100.0)
        self.assertEqual(ex.spot_sells(), [])                           # the spot leg is kept: the pair is hedged

    async def test_the_testnet_case_bare_reply_priced_from_the_fills(self):
        ex = FakeExchange(reply="bare", fills=[{"price": "100.5", "qty": "0.6"}, {"price": "100.3", "qty": "0.398"}])
        pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_entry, (100.5 * 0.6 + 100.3 * 0.398) / 0.998)
        self.assertEqual(ex.spot_sells(), [])
        self.assertAlmostEqual(ex.fut_position, -pos.perp_qty)          # the bot's book matches the exchange

    async def test_order_reread_is_used_before_the_fills(self):
        ex = FakeExchange(reply="bare", reget_avg="100.4")
        pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_entry, 100.4)
        self.assertFalse(any(p == "/fapi/v1/userTrades" for _, p, _ in ex.calls))

    async def test_no_price_anywhere_falls_back_to_the_mark_price(self):
        ex = FakeExchange(reply="bare")
        with self.assertLogs("live", "ERROR"):
            pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_entry, 99.9)
        self.assertEqual(ex.spot_sells(), [])


class LostReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_reply_lost_but_the_short_is_open_continues_hedged(self):
        ex = FakeExchange(reply="avg", post_error=asyncio.TimeoutError())
        with self.assertLogs("live", "ERROR"):
            pos = await open_with(ex)
        self.assertAlmostEqual(pos.perp_qty, -ex.fut_position)
        self.assertEqual(ex.spot_sells(), [])                           # NOT unwound: that was the testnet bug

    async def test_refused_before_any_fill_unwinds_the_spot_leg(self):
        ex = FakeExchange(post_error=BinanceError(400, -2019, "Margin is insufficient", "/fapi/v1/order"),
                          fill_before_error=False)
        with self.assertRaises(RuntimeError) as cm:
            await open_with(ex)
        self.assertNotIsInstance(cm.exception, UnhedgedError)
        self.assertIn("spot leg unwound", str(cm.exception))
        self.assertEqual(len(ex.spot_sells()), 1)
        self.assertEqual(ex.fut_position, 0.0)

    async def test_unknown_state_halts_instead_of_guessing(self):
        ex = FakeExchange(post_error=asyncio.TimeoutError(), position_error=asyncio.TimeoutError())
        with self.assertRaises(UnhedgedError) as cm:
            await open_with(ex)
        self.assertIn("Check the exchange", str(cm.exception))
        self.assertEqual(ex.spot_sells(), [])                           # nothing done blindly


if __name__ == "__main__":
    unittest.main()
