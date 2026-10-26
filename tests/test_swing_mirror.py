"""The swing-to-testnet mirror (arbbot/swing_mirror.py) against a fake swing runner and a fake testnet ticket."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from arbbot.swing_mirror import SwingMirror

START = 1_800_000_000_000


class FakeRunner:
    def __init__(self, capital=100.0):
        self.cfg = SimpleNamespace(strategies=["trend", "swing", "smc"])
        self.accounts = {"trend": {"capital": capital}}
        self.results = {}

    def hold(self, *positions):
        self.results["trend"] = SimpleNamespace(open_positions=list(positions))


def pos(sym="SOLUSDT", side="long", entry_ts=START + 1, stop=170.0, qty=0.05):
    return dict(symbol=sym, side=side, entry_ts=entry_ts, stop=stop, qty=qty, reason_in="Donchian breakout")


class FakeTicket:
    def __init__(self):
        self.positions: dict[str, float] = {}
        self.arb = set()
        self.orders, self.closed, self.stops = [], [], []
        self.refuse = None

    def arb_symbols(self):
        return set(self.arb)

    async def refresh(self):
        pass

    def position(self, sym):
        return self.positions.get(sym, 0.0)

    async def place_order(self, req):
        if self.refuse:
            raise ValueError(self.refuse)
        self.orders.append(req)
        self.positions[req["symbol"]] = req["qty"] * (1 if req["side"] == "buy" else -1)
        return {"status": "filled", "qty": req["qty"]}

    async def close_position(self, sym):
        self.closed.append(sym)
        self.positions[sym] = 0.0
        return {"status": "filled"}

    async def replace_stop(self, sym, side, stop):
        self.stops.append((sym, side, stop))


class MirrorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runner, self.ticket = FakeRunner(), FakeTicket()
        self.path = Path(tempfile.mkdtemp()) / "mirror.json"
        self.sent = []
        self.m = self.mirror()

    def mirror(self):
        return SwingMirror(self.runner, self.ticket, "trend", 10_000.0, self.path, notify=self.sent.append,
                           clock=lambda: START / 1000)

    async def test_a_new_entry_is_copied_scaled_with_an_exchange_stop(self):
        self.runner.hold(pos())
        await self.m.step()
        o = self.ticket.orders[0]
        self.assertEqual((o["symbol"], o["side"], o["type"], o["sl"]), ("SOLUSDT", "buy", "market", 170.0))
        self.assertAlmostEqual(o["qty"], 0.05 * 100)                       # $10,000 / $100 fund = 100x the paper size
        self.assertIn("SOLUSDT", self.m.s["open"])
        self.assertIn("TESTNET trend mirror: BUY SOLUSDT", self.sent[0])
        await self.m.step()
        self.assertEqual(len(self.ticket.orders), 1)                       # not copied twice

    async def test_shorts_sell_and_positions_from_before_mirroring_are_not_copied(self):
        self.runner.hold(pos("ETHUSDT", "short", stop=2400.0), pos("BTCUSDT", entry_ts=START - 1))
        await self.m.step()
        self.assertEqual([(o["symbol"], o["side"]) for o in self.ticket.orders], [("ETHUSDT", "sell")])

    async def test_coins_in_use_are_skipped_once(self):
        self.ticket.arb = {"SOLUSDT"}
        self.ticket.positions["XRPUSDT"] = 10.0                            # e.g. a manual ticket trade
        self.runner.hold(pos("SOLUSDT"), pos("XRPUSDT"))
        await self.m.step()
        await self.m.step()
        self.assertEqual(self.ticket.orders, [])
        self.assertIn("arbitrage bot", self.m.s["done"][f"SOLUSDT:{START + 1}"])
        self.assertIn("already has a position", self.m.s["done"][f"XRPUSDT:{START + 1}"])

    async def test_the_stop_trails_and_the_exit_closes(self):
        self.runner.hold(pos(stop=170.0))
        await self.m.step()
        self.runner.hold(pos(stop=170.05))                                 # under 0.1%: left alone
        await self.m.step()
        self.runner.hold(pos(stop=175.0))
        await self.m.step()
        self.assertEqual(self.ticket.stops, [("SOLUSDT", "buy", 175.0)])
        self.runner.hold()                                                 # the paper position exited
        await self.m.step()
        self.assertEqual(self.ticket.closed, ["SOLUSDT"])
        self.assertEqual(self.m.s["open"], {})

    async def test_an_exchange_stop_that_fired_first_is_recorded_and_not_re_entered(self):
        self.runner.hold(pos())
        await self.m.step()
        self.ticket.positions["SOLUSDT"] = 0.0                             # stopped out on the exchange
        await self.m.step()
        self.assertEqual(self.m.s["open"], {})
        self.assertIn("stop fired", self.m.s["done"][f"SOLUSDT:{START + 1}"])
        await self.m.step()
        self.assertEqual(len(self.ticket.orders), 1)                       # the same paper trade is not copied again
        self.assertEqual(self.ticket.closed, [])

    async def test_an_order_the_ticket_refuses_is_skipped_not_retried(self):
        self.ticket.refuse = "order value must be at least 100 USDT on SOLUSDT"
        self.runner.hold(pos())
        await self.m.step()
        self.ticket.refuse = None
        await self.m.step()
        self.assertEqual(self.ticket.orders, [])
        self.assertIn("at least 100 USDT", self.m.s["done"][f"SOLUSDT:{START + 1}"])

    async def test_state_survives_a_restart(self):
        self.runner.hold(pos())
        await self.m.step()
        again = self.mirror()
        await again.step()
        self.assertEqual(len(self.ticket.orders), 1)
        self.assertEqual(again.s["start_ts"], self.m.s["start_ts"])

    async def test_nothing_happens_before_the_first_replay_or_for_an_unknown_account(self):
        await self.m.step()
        self.assertEqual(self.ticket.orders, [])
        with self.assertRaises(ValueError):
            SwingMirror(self.runner, self.ticket, "momentum")


if __name__ == "__main__":
    unittest.main()
