"""The swing-to-testnet mirror (arbbot/swing_mirror.py) against a fake swing runner and a fake testnet ticket."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from arbbot.swing_mirror import SwingMirror, parse_accounts, state_path

START = 1_800_000_000_000


class FakeRunner:
    def __init__(self, capital=100.0):
        self.cfg = SimpleNamespace(strategies=["trend", "swing", "smc"])
        self.accounts = {name: {"capital": capital} for name in self.cfg.strategies}
        self.results = {}

    def hold(self, *positions, account="trend"):
        self.results[account] = SimpleNamespace(open_positions=list(positions))


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


class SeveralAccountsTests(unittest.IsolatedAsyncioTestCase):
    """--swing-mirror trend,swing,smc: one mirror per account on one testnet account, stepped one after another."""
    def setUp(self):
        self.runner, self.ticket, self.dir = FakeRunner(), FakeTicket(), Path(tempfile.mkdtemp())
        self.m = {n: SwingMirror(self.runner, self.ticket, n, 10_000.0, state_path(self.dir, n), clock=lambda: START / 1000)
                  for n in parse_accounts("all")}

    async def step_all(self):
        for m in self.m.values():                                          # the order run_all steps them in
            await m.step()

    async def test_each_account_copies_its_own_coins(self):
        self.runner.hold(pos("SOLUSDT"), account="trend")
        self.runner.hold(pos("ETHUSDT", "short", stop=2400.0), account="swing")
        self.runner.hold(pos("BNBUSDT"), account="smc")
        await self.step_all()
        self.assertEqual([(o["symbol"], o["side"]) for o in self.ticket.orders],
                         [("SOLUSDT", "buy"), ("ETHUSDT", "sell"), ("BNBUSDT", "buy")])
        self.assertEqual({n: list(m.s["open"]) for n, m in self.m.items()},
                         {"trend": ["SOLUSDT"], "swing": ["ETHUSDT"], "smc": ["BNBUSDT"]})

    async def test_a_coin_is_owned_by_the_first_account_and_only_it_closes_it(self):
        self.runner.hold(pos("SOLUSDT"), account="trend")
        self.runner.hold(pos("SOLUSDT", "short", stop=200.0), account="swing")
        await self.step_all()
        self.assertEqual([(o["symbol"], o["side"]) for o in self.ticket.orders], [("SOLUSDT", "buy")])
        self.assertIn("already has a position", self.m["swing"].s["done"][f"SOLUSDT:{START + 1}"])
        self.runner.hold(account="swing")                                  # swing's paper trade ends: trend's copy stays
        await self.step_all()
        self.assertEqual((self.ticket.closed, self.ticket.position("SOLUSDT")), ([], 0.05 * 100))
        self.runner.hold(account="trend")                                  # trend's ends: now it is closed
        await self.step_all()
        self.assertEqual(self.ticket.closed, ["SOLUSDT"])

    def test_account_lists(self):
        self.assertEqual(parse_accounts("all"), ["trend", "swing", "smc"])
        self.assertEqual(parse_accounts(" SMC, trend "), ["trend", "smc"])
        for bad in ("", "trend,momentum", ","):
            with self.assertRaises(ValueError):
                parse_accounts(bad)

    def test_the_old_trend_state_file_is_taken_over(self):
        d = Path(tempfile.mkdtemp())
        (d / "swing-mirror-testnet.json").write_text('{"start_ts": 1, "open": {}, "done": {"SOLUSDT:5": "x"}}')
        path = state_path(d, "trend")
        self.assertEqual((path.name, (d / "swing-mirror-testnet.json").exists()), ("swing-mirror-testnet-trend.json", False))
        self.assertEqual(SwingMirror(self.runner, self.ticket, "trend", path=path).s["done"], {"SOLUSDT:5": "x"})
        self.assertEqual(state_path(d, "swing").name, "swing-mirror-testnet-swing.json")


class MirrorFlagTests(unittest.TestCase):
    def test_an_unknown_account_is_refused_before_anything_connects(self):
        import asyncio
        import os
        from unittest import mock
        from arbbot.main import amain, parse_args
        keys = {k: "x" for k in ("BINANCE_TESTNET_FUTURES_API_KEY", "BINANCE_TESTNET_FUTURES_API_SECRET",
                                 "BINANCE_TESTNET_SPOT_API_KEY", "BINANCE_TESTNET_SPOT_API_SECRET")}
        argv = ["--live", "--testnet", "--config", "config.testnet.toml", "--swing", "--swing-mirror", "trend,momentum"]
        with mock.patch("arbbot.main.load_dotenv"), mock.patch.dict(os.environ, keys), \
                self.assertRaises(SystemExit) as cm:                      # never read the real .env, never connect
            asyncio.run(amain(parse_args(argv)))
        self.assertIn("--swing-mirror takes trend, swing, smc", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
