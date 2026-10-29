"""P&L reports by day, week and month (arbbot/pnl_report.py), their sources, and the /api/pnl route."""
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.pnl_report import report
from arbbot.portfolio import Store
from arbbot.server import Dashboard
from arbbot.testnet_ticket import TestnetTicket


def ts(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


NOW = ts("2026-10-29 15:00")                         # a Thursday


class ReportTests(unittest.TestCase):
    EVENTS = [(ts("2026-10-29 09:00"), 5.0, True), (ts("2026-10-29 10:00"), -2.0, True),
              (ts("2026-10-27 23:59"), -3.0, True), (ts("2026-10-27 12:00"), -0.5, False),     # a fee: not a trade
              (ts("2026-10-25 08:00"), 4.0, True),                                             # Sunday: last week
              (ts("2026-09-30 12:00"), 7.0, True)]                                             # last month

    def test_daily(self):
        r = report(self.EVENTS, "day", NOW)
        self.assertEqual(len(r["rows"]), 30)
        today, yesterday, tue = r["rows"][0], r["rows"][1], r["rows"][2]
        self.assertEqual((today["label"], today["pnl"], today["trades"], today["wins"], today["losses"]),
                         ("2026-10-29", 3.0, 2, 1, 1))
        self.assertEqual((yesterday["pnl"], yesterday["trades"]), (0.0, 0))                   # empty days are kept
        self.assertEqual((tue["label"], tue["pnl"], tue["trades"]), ("2026-10-27", -3.5, 1))
        self.assertEqual((r["trades"], r["wins"], r["losses"], r["green"], r["red"]), (5, 3, 2, 3, 1))
        self.assertEqual((r["best"]["label"], r["worst"]["label"]), ("2026-09-30", "2026-10-27"))
        self.assertAlmostEqual(r["total"], 3.0 - 3.5 + 4.0 + 7.0)

    def test_weeks_start_on_monday_and_months_on_the_first(self):
        w = report(self.EVENTS, "week", NOW)
        self.assertEqual((w["rows"][0]["label"], w["rows"][0]["pnl"]), ("week of 2026-10-26", -0.5))
        self.assertEqual((w["rows"][1]["label"], w["rows"][1]["pnl"]), ("week of 2026-10-19", 4.0))
        m = report(self.EVENTS, "month", NOW)
        self.assertEqual([(r["label"], r["pnl"]) for r in m["rows"][:2]], [("2026-10", 3.5), ("2026-09", 7.0)])
        self.assertEqual(m["rows"][-1]["label"], "2025-11")                                   # 12 months back

    def test_nothing_yet_and_a_bad_period(self):
        r = report([], "month", NOW)
        self.assertEqual((r["total"], r["trades"], r["best"], r["avg"]), (0, 0, None, 0.0))
        with self.assertRaises(ValueError):
            report([], "year", NOW)


class SourceTests(unittest.IsolatedAsyncioTestCase):
    def test_arbitrage_store(self):
        from arbbot.models import ClosedTrade
        import dataclasses
        s = Store(Path(tempfile.mkdtemp()) / "a.db")
        fields = {f.name for f in dataclasses.fields(ClosedTrade)}
        base = {f: 0 for f in fields} | {"symbol": "BTCUSDT", "reason": "converged"}
        s.append_trade(ClosedTrade(**(base | {"closed_at": NOW - 10, "pnl_usd": 1.5})))
        s.append_trade(ClosedTrade(**(base | {"closed_at": NOW - 5, "pnl_usd": -0.5})))
        self.assertEqual(s.pnl_events(), [(NOW - 10, 1.5, True), (NOW - 5, -0.5, True)])

    async def test_futures_account_from_binance_income(self):
        rows = [dict(incomeType="REALIZED_PNL", income="3", symbol="SOLUSDT", time=int(NOW * 1000)),
                dict(incomeType="REALIZED_PNL", income="1", symbol="SOLUSDT", time=int(NOW * 1000) + 30_000),  # same order
                dict(incomeType="REALIZED_PNL", income="-2", symbol="ETHUSDT", time=int(NOW * 1000) + 40_000),
                dict(incomeType="COMMISSION", income="-0.2", symbol="SOLUSDT", time=int(NOW * 1000)),
                dict(incomeType="FUNDING_FEE", income="0.1", symbol="SOLUSDT", time=int(NOW * 1000)),
                dict(incomeType="TRANSFER", income="500", symbol="", time=int(NOW * 1000))]          # not P&L
        calls = []

        class Client:
            hosts = {"fapi": "https://testnet.binancefuture.com"}

            async def get(self, venue, path, params=None, signed=False, timeout=None):
                calls.append(path)
                return rows

        t = TestnetTicket(Client(), None, clock=lambda: NOW + 60)
        events, note = await t.pnl_events()
        self.assertEqual(sorted(events), sorted([(NOW, 4.0, True), (NOW + 40, -2.0, True), (NOW, -0.2, False),
                                                 (NOW, 0.1, False)]))
        self.assertIn("testnet futures account", note)
        await t.pnl_events()
        self.assertEqual(calls, ["/fapi/v1/income"])                                         # cached

    async def test_paper_account_fills(self):
        from arbbot.paper_futures import PaperFutures
        pf = PaperFutures(None, None, 5.0)
        pf.s["trades"] = [dict(time=1000, realized=0.0, fee=0.1), dict(time=2000, realized=4.0, fee=0.1)]
        events, _ = await pf.pnl_events()
        self.assertEqual(events, [(1.0, -0.1, False), (2.0, 3.9, True)])


class RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        store = Store(Path(tempfile.mkdtemp()) / "a.db")
        engine = SimpleNamespace(swing=None, pf=SimpleNamespace(store=store), snapshot=lambda: {})
        cfg = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=1), feed=SimpleNamespace(interval_s=1.0))
        self.dash = Dashboard(engine, cfg, Path("web"))
        server = TestServer(self.dash.make_app())
        await server.start_server()
        self.dash.allowed_hosts.add(f"127.0.0.1:{server.port}")
        self.client = TestClient(server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_reports_per_panel(self):
        r = await self.client.get("/api/pnl?panel=arb&period=week")
        d = await r.json()
        self.assertEqual((r.status, d["panel"], d["period"], len(d["rows"])), (200, "arb", "week", 12))
        self.assertIn("arbitrage", d["note"])
        self.assertEqual((await self.client.get("/api/pnl?panel=arb&period=year")).status, 400)
        self.assertEqual((await self.client.get("/api/pnl?panel=swing&period=day")).status, 404)   # no swing kit here
        self.assertEqual((await self.client.get("/api/pnl?panel=ticket&period=day")).status, 404)  # no ticket here


if __name__ == "__main__":
    unittest.main()
