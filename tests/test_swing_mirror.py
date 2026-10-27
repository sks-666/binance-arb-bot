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
        self.log: list[dict] = []          # Binance-style fills (userTrades)
        self.close_pnl, self.fills_down = 0.0, False
        self.market_max: dict[str, float] = {}
        self.binance_refuses: dict[str, Exception] = {}

    async def max_market_qty(self, sym):
        return self.market_max.get(sym)

    def _fill(self, sym, side, qty, realized=0.0):
        self.log.append(dict(symbol=sym, side=side.upper(), price="100", qty=str(abs(qty)), realizedPnl=str(realized),
                             commission="0.05", commissionAsset="USDT", time=START))

    def position_info(self, sym):
        amt = self.position(sym)
        return dict(amount=amt, entry_px=100.0, mark=101.0, pnl=amt * 1.0) if amt else {}

    async def fills(self, sym, start_ms, end_ms):
        if self.fills_down:
            raise RuntimeError("rate limited")
        return [f for f in self.log if f["symbol"] == sym and start_ms <= f["time"] <= end_ms]

    def arb_symbols(self):
        return set(self.arb)

    async def refresh(self):
        pass

    def position(self, sym):
        return self.positions.get(sym, 0.0)

    async def place_order(self, req):
        if self.refuse:
            raise ValueError(self.refuse)
        if req["symbol"] in self.binance_refuses:
            raise self.binance_refuses[req["symbol"]]
        self.orders.append(req)
        self.positions[req["symbol"]] = self.positions.get(req["symbol"], 0.0) + req["qty"] * (1 if req["side"] == "buy" else -1)
        self._fill(req["symbol"], req["side"], req["qty"])
        return {"status": "filled", "qty": req["qty"], "filled_px": 100.0}

    async def close_position(self, sym):
        self.closed.append(sym)
        amt = self.positions[sym]
        self._fill(sym, "sell" if amt > 0 else "buy", amt, self.close_pnl)
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


class DemoResultTests(unittest.IsolatedAsyncioTestCase):
    """The testnet copies are the real record: their results come from Binance's fills, not from the paper replay."""
    def setUp(self):
        self.runner, self.ticket = FakeRunner(), FakeTicket()
        self.path = Path(tempfile.mkdtemp()) / "mirror.json"

    def mirror(self, **kw):
        return SwingMirror(self.runner, self.ticket, "trend", 10_000.0, self.path, clock=lambda: START / 1000, **kw)

    async def test_positions_held_before_mirroring_are_copied_when_asked(self):
        self.runner.hold(pos("BTCUSDT", entry_ts=START - 1))
        await self.mirror().step()
        self.assertEqual(self.ticket.orders, [])                             # the default: only new entries
        self.assertIn("before mirroring started", self.mirror().view()["status"][f"BTCUSDT:{START - 1}"])
        await self.mirror(copy_existing=True).step()
        self.assertEqual([o["symbol"] for o in self.ticket.orders], ["BTCUSDT"])

    async def test_a_paper_exit_closes_the_copy_and_records_the_real_result(self):
        m = self.mirror()
        self.runner.hold(pos())
        await m.step()
        v = m.view()
        self.assertEqual((v["status"][f"SOLUSDT:{START + 1}"], v["stats"]["open"], v["stats"]["unrealized"]),
                         ("on demo", 1, 5.0))
        self.ticket.close_pnl = 12.5
        self.runner.hold()
        await m.step()
        c = m.s["copies"][0]
        self.assertEqual((c["account"], c["symbol"], c["side"], c["why"], c["entry"], c["exit"]),
                         ("trend", "SOLUSDT", "buy", "paper exit", 100.0, 100.0))
        self.assertAlmostEqual(c["pnl"], 12.5 - 0.10)                        # Binance's realised P&L minus both fees
        s = m.stats()
        self.assertEqual((s["open"], s["closed"], s["wins"], s["losses"], s["unsettled"]), (0, 1, 1, 0, 0))
        self.assertAlmostEqual(s["realized"], 12.4)
        self.assertAlmostEqual(s["today"], 12.4)

    async def test_a_copy_closed_on_the_exchange_is_settled_from_its_fills(self):
        m = self.mirror()
        self.runner.hold(pos())
        await m.step()
        self.ticket._fill("SOLUSDT", "sell", 5.0, -30.0)                    # the testnet stop fired
        self.ticket.positions["SOLUSDT"] = 0.0
        await m.step()
        c = m.s["copies"][0]
        self.assertEqual(c["why"], "stop or by hand")
        self.assertAlmostEqual(c["pnl"], -30.10)
        self.assertEqual((m.stats()["wins"], m.stats()["losses"]), (0, 1))

    async def test_a_result_binance_cannot_give_yet_is_asked_again(self):
        m = self.mirror()
        self.runner.hold(pos())
        await m.step()
        self.ticket.fills_down, self.ticket.close_pnl = True, 3.0
        self.runner.hold()
        await m.step()
        self.assertIsNone(m.s["copies"][0]["pnl"])
        self.assertEqual(m.stats()["unsettled"], 1)
        self.ticket.fills_down = False
        await m.step()
        self.assertAlmostEqual(m.s["copies"][0]["pnl"], 2.9)
        self.assertEqual(self.mirror().s["copies"][0]["pnl"], m.s["copies"][0]["pnl"])   # saved across a restart


class BigAndRefusedOrderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runner, self.ticket = FakeRunner(), FakeTicket()
        self.m = SwingMirror(self.runner, self.ticket, "trend", 10_000.0, Path(tempfile.mkdtemp()) / "m.json",
                             clock=lambda: START / 1000)

    async def test_a_copy_above_binance_market_maximum_goes_in_pieces_with_one_stop(self):
        self.ticket.market_max["SOLUSDT"] = 2.0                           # the copy is 0.05 x 100 = 5 SOL
        self.runner.hold(pos())
        await self.m.step()
        self.assertEqual([(o["qty"], o.get("sl")) for o in self.ticket.orders], [(2.0, 170.0), (2.0, None), (1.0, None)])
        self.assertAlmostEqual(self.m.s["open"]["SOLUSDT"]["qty"], 5.0)
        self.assertAlmostEqual(self.ticket.position("SOLUSDT"), 5.0)

    async def test_binance_refusing_one_coin_does_not_stop_the_others(self):
        from arbbot.binance_client import BinanceError, NotSentError
        self.ticket.binance_refuses["BEAMXUSDT"] = BinanceError(400, -4005, "Quantity greater than max quantity.", "/fapi/v1/order")
        self.runner.hold(pos("BEAMXUSDT"), pos("SOLUSDT"))
        await self.m.step()
        self.assertEqual([o["symbol"] for o in self.ticket.orders], ["SOLUSDT"])
        self.assertIn("max quantity", self.m.s["done"][f"BEAMXUSDT:{START + 1}"])
        self.ticket.binance_refuses["ETHUSDT"] = NotSentError(418, None, "banned", "/fapi/v1/order")
        self.runner.hold(pos("ETHUSDT"), pos("SOLUSDT"))
        with self.assertRaises(NotSentError):                              # a rate-limit pause is retried, not skipped
            await self.m.step()
        self.assertNotIn(f"ETHUSDT:{START + 1}", self.m.s["done"])


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
