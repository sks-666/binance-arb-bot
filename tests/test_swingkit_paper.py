"""Live paper trading: the backtester's live mode, the runner (against a mock Binance), and the Telegram commands."""
import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import aiohttp

from arbbot.binance_client import BinanceClient
from arbbot.commands import CommandHandler
from swingkit.backtest import CostCfg, run_backtest
from swingkit.cli import main as swing_main
from swingkit.journal import Journal
from swingkit.models import Candle, Setup
from swingkit.paper import PaperCfg, SwingRunner, add_paper_args, paper_cfg_from, resolve_spec
from swingkit.risk import RiskCfg
from swingkit.strategies import Strategy
from tests.test_swingkit import H4, QUIET, Scripted, series_from, setup
from tests.test_swingkit_io import BAR, T0, MockBinance, candle


class AtTimes(Strategy):
    """Fires on specific candle open-times, whatever index the history window happens to give them."""
    name = "scripted"

    def __init__(self, signals: dict[int, dict] | None = None, **_):
        super().__init__()
        self.signals, self.s = signals or {}, None

    def prepare(self, s):
        self.s = s

    def manage(self, side, i, entry_i, stop):
        return stop, False, ""

    def entry(self, i):
        sig = self.signals.get(self.s.ts[i])
        return (Setup("scripted", sig["side"], i, self.s.c[i], sig["stop"], sig.get("target"), "scripted test setup")
                if sig else None)


class LiveModeTests(unittest.TestCase):
    def live(self, rows, setups, **kw):
        s = series_from(rows)
        return run_backtest({"AAAUSDT": s}, lambda: Scripted(setups), RiskCfg(), CostCfg(0, 0, 0), 10_000.0, H4,
                            close_at_end=False, **kw), s

    def test_open_positions_are_reported_not_closed_and_a_last_candle_setup_is_pending(self):
        res, _ = self.live(QUIET + [(100, 101, 99, 101)], {1: setup(1)})
        self.assertEqual(res.trades, [])
        p = res.open_positions[0]
        self.assertEqual((p["symbol"], p["side"], p["entry_px"], p["stop"], p["mark"]), ("AAAUSDT", "long", 100.0, 95.0, 101.0))
        self.assertAlmostEqual(p["unrealised_usd"], 20.0)          # 20 units x 1.0, no costs here
        self.assertAlmostEqual(p["unrealised_r"], 0.2)
        res, _ = self.live(QUIET, {2: setup(2)})                    # decided on the very last candle
        self.assertEqual(list(res.pending), ["AAAUSDT"])
        self.assertEqual(res.open_positions, [])

    def test_a_forming_candle_fills_at_its_open_but_nothing_is_decided_on_it(self):
        rows = QUIET + [(103, 150, 50, 120)]                        # the last candle's high/low/close are not known yet
        res, _ = self.live(rows, {2: setup(2), 3: setup(3)}, partial_last=True)
        p = res.open_positions[0]
        self.assertEqual(p["entry_px"], 103.0)                       # the order decided at candle 2's close fills at 3's open
        self.assertEqual(p["mark"], 103.0)                           # and is marked at that open, not at the unknown 120
        self.assertEqual(res.trades, [])                             # the wild high/low did not trigger anything
        self.assertEqual(res.pending, {})                            # and the signal "on" the forming candle was ignored

    def test_a_forming_candle_that_gaps_through_the_stop_closes_at_its_open(self):
        rows = QUIET + [(100, 101, 99, 100), (90, 90, 90, 90)]
        res, _ = self.live(rows, {1: setup(1)}, partial_last=True)
        self.assertEqual([(t.reason_out, t.exit_px) for t in res.trades], [("stop", 90.0)])

    def test_partial_applies_only_to_the_listed_coins(self):
        s = series_from(QUIET + [(103, 125, 100, 120)])
        res = run_backtest({"A": s, "B": s}, lambda: Scripted({1: setup(1)}), RiskCfg(max_open=5, max_heat_pct=5.0),
                           CostCfg(0, 0, 0), 10_000.0, H4, close_at_end=False, partial_last={"A"})
        marks = {p["symbol"]: p["mark"] for p in res.open_positions}
        self.assertEqual(marks["A"], 103.0)                          # forming: marked at its open
        self.assertEqual(marks["B"], 120.0)                          # closed: the real close

    def test_nothing_is_entered_before_the_start_time(self):
        rows = QUIET * 3
        s = series_from(rows)
        res, _ = self.live(rows, {1: setup(1), 4: setup(4)}, start_ts=s.ts[3])
        self.assertEqual([p["entry_ts"] for p in res.open_positions], [s.ts[5]])   # the candle-1 idea is ignored


def one_bar_clock(k: int, frac: float = 0.5):
    return lambda: (T0 + (k + frac) * BAR) / 1000.0


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.api = MockBinance(2001)                                   # candles 0..2000
        await self.api.start()
        self.session = aiohttp.ClientSession()
        self.client = BinanceClient(self.session, timeout_s=5.0)
        self.client.hosts = {"fapi": self.api.base, "spot": self.api.base}
        self.tmp = Path(tempfile.mkdtemp())
        self.journal = Journal(self.tmp / "j.db")
        self.sent: list[str] = []
        self.cfg = PaperCfg(symbols=["BTCUSDT"], strategies=["trend"], cache_dir=str(self.tmp / "klines"))
        sig_ts = T0 + 2000 * BAR                                       # the idea is decided at the close of candle 2000
        self.patch = mock.patch("swingkit.paper.make", lambda name, **kw: AtTimes({sig_ts: dict(side="long", stop=110.0)}))
        self.patch.start()

    async def asyncTearDown(self):
        self.patch.stop()
        await self.session.close()
        await self.api.stop()

    def runner(self, k, frac=0.5):
        return SwingRunner(self.cfg, self.client, self.journal, self.sent.append, clock=one_bar_clock(k, frac))

    async def test_a_signal_becomes_a_journal_entry_then_an_exit_and_is_reported_exactly_once(self):
        r = self.runner(2000)                                          # candle 2000 is forming: paper trading starts now
        await r.refresh()
        self.assertEqual((self.sent, r.results["trend"].open_positions), ([], []))
        self.assertIn("No setups waiting", r.setups_text())

        self.api.candles.append(candle(2001))                          # candle 2000 closes, candle 2001 opens
        r = self.runner(2001)                                          # (a fresh runner: the start time is remembered)
        self.assertEqual(r.start_ts, int(one_bar_clock(2000)() * 1000))
        await r.refresh()
        self.assertEqual(len(self.sent), 1)
        self.assertIn("SWING PAPER ENTRY [trend] BTCUSDT LONG", self.sent[0])
        self.assertIn("why: scripted test setup", self.sent[0])
        row = self.journal.trades(mode="paper")[0]
        self.assertEqual((row["status"], row["strategy"], row["symbol"], row["run_id"]),
                         ("open", "trend", "BTCUSDT", f"paper-trend-4h-{r.tag}"))
        self.assertAlmostEqual(row["entry"], candle(2001).open * 1.0003)       # the open of candle 2001 + 3 bps slippage
        self.assertEqual(row["stop"], 110.0)
        self.assertAlmostEqual(row["risk_usd"], 100.0, delta=1.0)               # 1% of 10,000

        await r.refresh()
        await self.runner(2001).refresh()                                      # again, and from a "restarted" runner
        self.assertEqual(len(self.sent), 1)                                    # no duplicate alert...
        self.assertEqual(len(self.journal.trades(mode="paper")), 1)           # ...and no duplicate trade
        self.assertIn("1 open", r.status_text())

        self.api.candles += [candle(2002), candle(2003), candle(2004)]
        self.api.candles[2003] = Candle(candle(2003).ts, 120.0, 121.0, 100.0, 105.0, 1.0)   # dives through the stop
        r = self.runner(2004)
        await r.refresh()
        self.assertEqual(len(self.sent), 2)
        self.assertIn("SWING PAPER EXIT [trend] BTCUSDT LONG", self.sent[1])
        self.assertIn("stop", self.sent[1])
        row = self.journal.trades(mode="paper")[0]
        self.assertEqual((row["status"], row["reason_out"]), ("closed", "stop"))
        self.assertLess(row["pnl_usd"], 0)
        self.assertAlmostEqual(row["r_multiple"], -1.0, delta=0.4)             # about one R, plus fees/slippage/gap
        self.assertEqual(self.journal.stats(mode="paper")["closed"], 1)
        self.assertIn("1 closed", r.status_text())

    async def test_a_setup_decided_before_the_next_candle_exists_is_announced_once(self):
        await self.runner(2000).refresh()
        # the clock has just passed the close of candle 2000, but the exchange has not published candle 2001 yet
        r = SwingRunner(self.cfg, self.client, self.journal, self.sent.append,
                        clock=lambda: (T0 + 2001 * BAR + 1_000) / 1000.0)
        await r.refresh()
        self.assertEqual(len(self.sent), 1)
        self.assertIn("SWING SETUP [trend] BTCUSDT LONG: paper entry at the next 4h open", self.sent[0])
        self.assertIn("BTCUSDT LONG", r.setups_text())
        await r.refresh()
        self.assertEqual(len(self.sent), 1)                                    # not announced again
        self.assertEqual(self.journal.trades(mode="paper"), [])                # nothing is a trade until it fills

    async def test_a_failing_refresh_is_survivable(self):
        r = self.runner(2000)
        with mock.patch.object(self.client, "get", side_effect=aiohttp.ClientConnectionError("down")):
            with self.assertRaises(asyncio.TimeoutError):                      # run() keeps looping; it never raises
                await asyncio.wait_for(r.run(), 0.5)
        self.assertIn("ClientConnectionError", r.last_error)
        self.assertIn("failed", r.status_text())
        self.assertIn("still loading", self.runner(2000).setups_text().lower())

    def make(self, symbols, k=2000, **kw):
        cfg = PaperCfg(symbols=symbols, strategies=["trend"], cache_dir=str(self.tmp / "klines"), **kw)
        return SwingRunner(cfg, self.client, self.journal, self.sent.append, clock=one_bar_clock(k))

    async def test_symbol_specs_arb_topn_and_lists(self):
        self.assertEqual(await resolve_spec("BTCUSDT,ETHUSDT", self.client), (["BTCUSDT", "ETHUSDT"], False))
        self.assertEqual(await resolve_spec("top2", self.client), (["BTCUSDT", "ETHUSDT"], True))
        self.assertEqual(await resolve_spec("arb", self.client, ["SOLUSDT", "BTCUSDT"]), (["SOLUSDT", "BTCUSDT"], True))
        with self.assertRaises(ValueError):
            await resolve_spec("arb", self.client)                              # no arbitrage universe to copy

    async def test_the_market_list_is_frozen_for_the_whole_run(self):
        a = self.make(["BTCUSDT", "ETHUSDT"], symbols_auto=True)
        b = self.make(["SOLUSDT"], 2001, symbols_auto=True)                     # the volume ranking moved overnight
        self.assertEqual((b.symbols, b.tag, b.start_ts), (a.symbols, a.tag, a.start_ts))
        c = self.make(["SOLUSDT"], 2001, symbols_auto=True, restart=True)       # --restart: a new run on the new list
        self.assertEqual(c.symbols, ["SOLUSDT"])
        self.assertNotEqual(c.tag, a.tag)
        self.assertEqual(self.make(["XRPUSDT"], 2002, symbols_auto=True).symbols, ["SOLUSDT"])   # the new run is the remembered one

    async def test_an_explicit_list_that_differs_is_refused_once_the_run_has_trades(self):
        await self.make(["BTCUSDT"]).refresh()
        self.assertEqual(len(self.make(["BTCUSDT", "ETHUSDT"]).symbols), 2)     # nothing traded yet: adopting it loses nothing
        self.api.candles.append(candle(2001))
        await self.make(["BTCUSDT", "ETHUSDT"], 2001).refresh()
        self.assertTrue(self.journal.trades(mode="paper"))
        with self.assertRaises(ValueError) as cm:
            self.make(["SOLUSDT"], 2001)
        self.assertIn("--restart", str(cm.exception))
        self.assertEqual(self.make(["SOLUSDT"], 2001, restart=True).symbols, ["SOLUSDT"])

    async def test_one_bad_market_does_not_stop_the_others(self):
        real = self.client.get

        async def flaky(venue, path, params=None, **kw):
            if params and params.get("symbol") == "ETHUSDT":
                raise aiohttp.ClientConnectionError("no such market")
            return await real(venue, path, params, **kw)

        r = self.make(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        with mock.patch.object(self.client, "get", flaky):
            await r.refresh()
        self.assertEqual(list(r.unavailable), ["ETHUSDT"])
        self.assertIn("ETHUSDT", r.snapshot()["unavailable"])
        self.assertFalse(r.snapshot()["loading"])
        self.assertIn("1 market(s) had no usable candles and are skipped: ETH", r.status_text())
        self.api.candles.append(candle(2001))
        r.clock = one_bar_clock(2001)
        await r.refresh()                                                       # the market is retried and recovers
        self.assertEqual(r.unavailable, {})

    async def test_every_market_failing_is_an_error_not_an_empty_success(self):
        r = self.make(["BTCUSDT", "ETHUSDT"])
        with mock.patch.object(self.client, "get", side_effect=aiohttp.ClientConnectionError("down")):
            with self.assertRaises(RuntimeError):
                await r.refresh()

    async def test_nothing_is_requested_again_until_a_candle_boundary_passes(self):
        r = self.make([f"COIN{i}USDT" for i in range(10)])
        self.assertIn("10 markets", r.status_text() + r._coins())
        await r.refresh()
        self.assertEqual(r.refreshes, 1)
        asked = len(self.api.requests)
        self.assertEqual(await r.refresh(), [])                                 # same candle: no requests, no replay
        self.assertEqual((len(self.api.requests), r.refreshes), (asked, 1))
        self.api.candles.append(candle(2001))
        r.clock = one_bar_clock(2001)                                           # a boundary passed
        await r.refresh()
        self.assertGreater(len(self.api.requests), asked)
        self.assertEqual(r.refreshes, 2)

    async def test_changing_an_accounts_fund_restarts_only_that_account(self):
        await self.runner(2000).refresh()
        self.api.candles.append(candle(2001))
        r = self.runner(2001)
        await r.refresh()
        self.assertEqual(len(r.results["trend"].open_positions), 1)
        old_tag = r.accounts["trend"]["tag"]
        self.sent.clear()

        for bad in (0, 50, 10_000_001, float("nan"), float("inf"), "abc", None):
            with self.assertRaises(ValueError):
                await r.set_capital("trend", bad)
        with self.assertRaises(ValueError):
            await r.set_capital("nope", 20_000)
        self.assertEqual(r.accounts["trend"]["capital"], 10_000.0)                # nothing changed
        self.assertIn("already has", await r.set_capital("trend", 10_000))        # same fund: a no-op
        self.assertEqual(self.sent, [])

        r.clock = one_bar_clock(2001, 0.75)
        self.assertIn("now has $20,000.00", await r.set_capital("trend", 20_000))
        acct = r.accounts["trend"]
        self.assertEqual(acct["capital"], 20_000.0)
        self.assertNotEqual(acct["tag"], old_tag)
        self.assertEqual(acct["start_ts"], int(one_bar_clock(2001, 0.75)() * 1000))
        old = self.journal.trades(mode="paper", run_id=f"paper-trend-4h-{old_tag}", limit=None)
        self.assertEqual((len(old), old[0]["status"], old[0]["reason_out"]), (1, "closed", "account reset: fund changed"))
        self.assertEqual(r.results["trend"].open_positions, [])                   # the old idea does not come back
        s = r.snapshot()
        a = s["accounts"][0]
        self.assertEqual((a["capital"], a["equity"], a["return_pct"], a["open"], a["closed"]),
                         (20_000.0, 20_000.0, 0.0, 0, 0))                          # a clean slate; old trade left the stats
        self.assertEqual((s["funds"], s["trades"]), ({"trend": 20_000.0}, []))
        self.assertTrue(any("fund [trend] $10,000 → $20,000" in m for m in self.sent))
        self.assertEqual(len(self.journal.trades(mode="paper", limit=None)), 1)  # and no duplicate rows appeared

        again = self.runner(2001)                                                  # a restarted bot keeps the new fund
        self.assertEqual(again.accounts["trend"], acct)
        self.assertEqual(again.snapshot()["funds"], {"trend": 20_000.0})

    async def test_a_fund_change_only_touches_the_named_account(self):
        cfg = PaperCfg(symbols=["BTCUSDT"], strategies=["trend", "swing"], cache_dir=str(self.tmp / "klines"))
        r = SwingRunner(cfg, self.client, self.journal, self.sent.append, clock=one_bar_clock(2000))
        await r.refresh()
        swing_before = dict(r.accounts["swing"])
        await r.set_capital("trend", 5_000)
        self.assertEqual(r.accounts["swing"], swing_before)
        self.assertEqual({a["strategy"]: a["capital"] for a in r.snapshot()["accounts"]},
                         {"trend": 5_000.0, "swing": 10_000.0})
        self.assertIn("5,000", r.status_text())

    async def test_telegram_commands_read_the_runner(self):
        from tests.test_commands import build, warm
        eng = build()
        await warm(eng)
        h = CommandHandler(eng, "T")
        self.assertIn("isn't running", h.answer("/setups"))
        self.assertIn("isn't running", h.answer("/journal"))
        h.swing = self.runner(2000)
        self.assertIn("still loading", h.answer("/swing").lower())
        await h.swing.refresh()
        self.api.candles.append(candle(2001))
        h.swing = self.runner(2001)
        await h.swing.refresh()
        self.assertIn("trend:", h.answer("/swing"))
        self.assertIn("1 open", h.answer("/swing"))
        self.assertIn("BTCUSDT LONG in", h.answer("/swing"))
        j = h.answer("/journal 3")
        self.assertIn("1 open", j)
        self.assertIn("why: scripted test setup", j)
        self.assertIn("No setups waiting", h.answer("/setups"))              # it already filled, so nothing is waiting
        self.assertIn("paper", h.answer("/setups").lower())


    async def test_the_dashboard_snapshot_follows_the_accounts(self):
        r = self.runner(2000)
        s = r.snapshot()
        self.assertEqual((s["loading"], s["accounts"], s["checked_age_s"]), (True, [], None))
        await r.refresh()
        s = r.snapshot()
        self.assertEqual((s["loading"], s["interval"], s["symbols"], s["capital"], s["risk_pct"]),
                         (False, "4h", ["BTCUSDT"], 10_000.0, 1.0))
        acct = s["accounts"][0]
        self.assertEqual((acct["strategy"], acct["equity"], acct["return_pct"], acct["open"], acct["closed"]),
                         ("trend", 10_000.0, 0.0, 0, 0))
        self.assertLess(s["checked_age_s"], 5)

        self.api.candles.append(candle(2001))
        r = self.runner(2001)
        await r.refresh()
        s = r.snapshot()
        pos = s["open"][0]
        self.assertEqual((pos["strategy"], pos["symbol"], pos["side"], pos["stop"]), ("trend", "BTCUSDT", "long", 110.0))
        self.assertIsNone(pos["target"])
        self.assertEqual(s["accounts"][0]["open"], 1)
        self.assertEqual((s["setups"], s["trades"]), ([], []))

        self.api.candles += [candle(2002), candle(2003), candle(2004)]
        self.api.candles[2003] = Candle(candle(2003).ts, 120.0, 121.0, 100.0, 105.0, 1.0)
        r = self.runner(2004)
        await r.refresh()
        s = r.snapshot()
        self.assertEqual((s["open"], len(s["trades"])), ([], 1))
        t = s["trades"][0]
        self.assertEqual((t["strategy"], t["symbol"], t["side"], t["reason"]), ("trend", "BTCUSDT", "long", "stop"))
        self.assertLess(t["pnl"], 0)
        acct = s["accounts"][0]
        self.assertEqual((acct["closed"], acct["wins"], acct["open"]), (1, 0, 0))
        self.assertLess(acct["equity"], 10_000.0)
        self.assertLess(acct["return_pct"], 0)

        text = json.dumps(s)                                                  # must travel to the browser and to Upstash
        for secret in ("token", "TOKEN", "password", "api_key"):
            self.assertNotIn(secret, text)

    async def test_a_failed_refresh_is_visible_on_the_dashboard(self):
        r = self.runner(2000)
        await r.refresh()
        r.last_error = "ClientConnectionError: down"
        self.assertEqual(r.snapshot()["error"], "ClientConnectionError: down")


class DashboardPlumbingTests(unittest.IsolatedAsyncioTestCase):
    async def test_engine_and_published_snapshots_carry_the_swing_data(self):
        from types import SimpleNamespace
        from arbbot.publisher import public_snapshot
        from tests.test_model import engine, warm
        eng, m, pf, cfg = engine()
        await warm(eng, m)
        self.assertIsNone(eng.snapshot()["swing"])                              # off unless --swing
        self.assertIsNone(public_snapshot(eng.snapshot())["swing"])
        eng.swing = SimpleNamespace(snapshot=lambda: {"accounts": [{"strategy": "trend", "equity": 10_012.3456789,
                                                                    "avg_r": float("nan")}], "setups": []})
        live = eng.snapshot()["swing"]
        self.assertEqual(live["accounts"][0]["strategy"], "trend")
        pub = public_snapshot(eng.snapshot())["swing"]["accounts"][0]            # the hosted copy: rounded, NaN -> null
        self.assertEqual((pub["equity"], pub["avg_r"]), (10_012.3457, None))


class FundEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from types import SimpleNamespace
        from aiohttp.test_utils import TestClient, TestServer
        from arbbot.server import Dashboard
        self.calls = []

        async def set_capital(name, amount):
            self.calls.append((name, amount))
            if name != "trend":
                raise ValueError("unknown account")
            return f"{name} now has ${float(amount):,.2f}"

        self.engine = SimpleNamespace(swing=SimpleNamespace(set_capital=set_capital))
        cfg = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=1), feed=SimpleNamespace(interval_s=1.0))
        self.dash = Dashboard(self.engine, cfg, Path("web"))
        self.server = TestServer(self.dash.make_app())
        await self.server.start_server()
        self.dash.allowed_hosts.add(f"127.0.0.1:{self.server.port}")
        self.client = TestClient(self.server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    def post(self, body, token=True, **kw):
        headers = {"X-Token": self.dash.token} if token else {}
        return self.client.post("/api/swing-capital", json=body, headers=headers, **kw)

    async def test_needs_the_control_token(self):
        r = await self.post({"strategy": "trend", "capital": 1000}, token=False)
        self.assertEqual(r.status, 403)
        self.assertEqual(self.calls, [])

    async def test_sets_the_fund_and_reports_bad_input_as_400(self):
        r = await self.post({"strategy": "trend", "capital": 15000})
        self.assertEqual((r.status, (await r.json())["message"]), (200, "trend now has $15,000.00"))
        r = await self.post({"strategy": "smc", "capital": 15000})
        self.assertEqual((r.status, (await r.json())["error"]), (400, "unknown account"))
        r = await self.client.post("/api/swing-capital", data="not json", headers={"X-Token": self.dash.token})
        self.assertEqual(r.status, 400)
        r = await self.post([1, 2])
        self.assertEqual(r.status, 400)

    async def test_without_swing_it_says_so(self):
        self.engine.swing = None
        r = await self.post({"strategy": "trend", "capital": 1000})
        self.assertEqual(r.status, 404)

    async def test_the_generic_actions_still_work(self):
        self.engine.request_halt = lambda: self.calls.append("halt")
        self.engine.request_resume = self.engine.request_flatten = lambda: None
        r = await self.client.post("/api/halt", headers={"X-Token": self.dash.token})
        self.assertEqual((r.status, self.calls), (200, ["halt"]))


class PageTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parent.parent

    def pages(self):
        return {"local": (self.ROOT / "web" / "index.html").read_text(encoding="utf-8"),
                "hosted": (self.ROOT / "vercel-dashboard" / "views" / "dashboard.html").read_text(encoding="utf-8")}

    def test_both_pages_have_the_swing_panel_and_the_hosted_one_has_no_controls(self):
        for name, html in self.pages().items():
            for needle in ("Swing paper accounts", "sw-card", "sw-accts", "sw-open", "sw-setups", "sw-trades",
                           "renderSwing()", "snap.swing"):
                self.assertIn(needle, html, f"{name} page is missing {needle}")
        for forbidden in ("btn-pause", "btn-flat", "WebSocket(", "__TOKEN__", "/api/halt", "/api/flatten"):
            self.assertNotIn(forbidden, self.pages()["hosted"])

    def test_the_hosted_page_is_in_sync_with_the_local_one(self):
        import subprocess, sys
        before = self.pages()["hosted"]
        subprocess.run([sys.executable, str(self.ROOT / "scripts" / "build_hosted_dashboard.py")], check=True,
                       capture_output=True)
        self.assertEqual(self.pages()["hosted"], before, "run scripts/build_hosted_dashboard.py and commit the result")

    def test_the_inline_scripts_are_valid_javascript(self):
        import re, shutil, subprocess
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not installed")
        for name, html in self.pages().items():
            js = re.search(r"<script>(.*?)</script>", html, re.S).group(1)
            path = Path(tempfile.mkdtemp()) / f"{name}.js"
            path.write_text(js, encoding="utf-8")
            done = subprocess.run([node, "--check", str(path)], capture_output=True, text=True)
            self.assertEqual(done.returncode, 0, f"{name}: {done.stderr}")


class ConfigTests(unittest.TestCase):
    def test_config_validation(self):
        PaperCfg()                                                         # the defaults are valid
        for bad in (dict(risk_pct=3.0), dict(interval="7m"), dict(strategies=["nope"]), dict(symbols=["btc/usdt"]),
                    dict(poll_s=5), dict(capital=0), dict(market="options"), dict(params={"trend": {"fats": 1}})):
            with self.assertRaises(ValueError, msg=str(bad)):
                PaperCfg(**bad)

    def test_arguments_for_both_entry_points(self):
        for prefix in ("", "swing-"):
            p = argparse.ArgumentParser()
            add_paper_args(p, prefix)
            ns = p.parse_args([f"--{prefix}symbols", "btcusdt,ethusdt", f"--{prefix}strategies", "trend,smc",
                               f"--{prefix}risk-pct", "1.5", f"--{prefix}no-short", f"--{prefix}interval", "1d"])
            cfg = paper_cfg_from(ns, prefix)
            self.assertEqual((cfg.symbols, cfg.strategies, cfg.risk_pct, cfg.allow_short, cfg.interval),
                             (["BTCUSDT", "ETHUSDT"], ["trend", "smc"], 1.5, False, "1d"))
            ns = p.parse_args([f"--{prefix}risk-pct", "2.5"])
            with self.assertRaises(ValueError):
                paper_cfg_from(ns, prefix)

    def test_the_main_bot_offers_the_swing_options_and_the_cli_refuses_bad_ones(self):
        from arbbot.main import parse_args
        a = parse_args(["--swing", "--swing-symbols", "BTCUSDT", "--swing-interval", "1d"])
        self.assertTrue(a.swing)
        self.assertEqual((a.swing_symbols, a.swing_interval, a.swing_risk_pct), ("BTCUSDT", "1d", 1.0))
        self.assertFalse(parse_args([]).swing)
        self.assertEqual(swing_main(["run", "--risk-pct", "3"]), 2)
        self.assertEqual(swing_main(["run", "--strategies", "nope"]), 2)


if __name__ == "__main__":
    unittest.main()
