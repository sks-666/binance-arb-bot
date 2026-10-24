"""swingkit data download (against a mock Binance that pages like the real one) and the trading journal."""
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

from arbbot.binance_client import BinanceClient
from swingkit.backtest import Trade
from swingkit.data import (DAY_MS, INTERVAL_MS, cache_path, fetch_funding, fetch_range, funding_path, load_csv,
                           load_funding, load_funding_csv, load_history, save_csv, save_funding, top_futures_symbols)
from swingkit.journal import COLUMNS, Journal, format_stats, format_trades
from swingkit.models import Candle

BAR = INTERVAL_MS["4h"]
T0 = 1_700_000_000_000 - (1_700_000_000_000 % BAR)  # a bar-aligned start


def candle(k: int) -> Candle:
    base = 100 + 0.01 * k
    return Candle(T0 + k * BAR, base, base + 1.5, base - 1.5, base + 0.5, 10.0 + k)


class MockBinance:
    """GET /fapi/v1/klines and /api/v3/klines over a growing list of candles, honouring startTime, endTime, limit."""

    def __init__(self, n: int):
        self.candles = [candle(k) for k in range(n)]
        # a funding event every 8 h (two per 4h candle pair), a few ms after the hour like the real thing
        self.funding = [(T0 + k * 8 * 3_600_000 + 5, 0.0001 * (k % 5 - 2)) for k in range(1500)]
        self.requests: list[dict] = []

    async def funding_rate(self, request):
        q = request.query
        self.requests.append({"path": request.path, **dict(q)})
        lo, hi, limit = int(q.get("startTime", 0)), int(q.get("endTime", 1 << 62)), int(q.get("limit", 100))
        rows = [{"symbol": q["symbol"], "fundingTime": ts, "fundingRate": repr(rate), "markPrice": "1.0"}
                for ts, rate in self.funding if lo <= ts <= hi][:limit]
        return web.json_response(rows)

    tickers = [("BTCUSDT", 9e9), ("ETHUSDT", 5e9), ("SOLUSDT", 1e9), ("DOGEUSDT", 3e7), ("TINYUSDT", 1e6),
               ("BTCUSDT_241227", 8e9), ("BTCUSDC", 7e9)]            # a dated future, a non-USDT quote and an illiquid coin

    async def ticker(self, request):
        self.requests.append({"path": request.path})
        return web.json_response([{"symbol": s, "quoteVolume": repr(v)} for s, v in self.tickers])

    async def start(self):
        app = web.Application()
        app.router.add_get("/fapi/v1/klines", self.klines)
        app.router.add_get("/api/v3/klines", self.klines)
        app.router.add_get("/fapi/v1/fundingRate", self.funding_rate)
        app.router.add_get("/fapi/v1/ticker/24hr", self.ticker)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def klines(self, request):
        q = request.query
        self.requests.append({"path": request.path, **dict(q)})
        lo, hi, limit = int(q.get("startTime", 0)), int(q.get("endTime", 1 << 62)), int(q.get("limit", 500))
        rows = [[k.ts, str(k.open), str(k.high), str(k.low), str(k.close), str(k.volume), k.ts + BAR - 1, "0", 0, "0", "0", "0"]
                for k in self.candles if lo <= k.ts <= hi]
        # like the real API: without a start or end time you get the LATEST candles, otherwise the first ones from startTime
        rows = rows[-limit:] if ("startTime" not in q and "endTime" not in q) else rows[:limit]
        return web.json_response(rows)

    async def stop(self):
        await self.runner.cleanup()


class DataTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.api = MockBinance(2500)
        await self.api.start()
        self.session = aiohttp.ClientSession()
        self.client = BinanceClient(self.session, timeout_s=5.0)
        self.client.hosts = {"fapi": self.api.base, "spot": self.api.base}
        self.tmp = Path(tempfile.mkdtemp())
        self.now = T0 + 2499 * BAR + BAR // 2           # the last candle (2499) is still forming

    async def asyncTearDown(self):
        await self.session.close()
        await self.api.stop()

    async def test_pages_through_history_and_drops_the_forming_candle(self):
        got = await fetch_range(self.client, "BTCUSDT", "4h", T0, self.now, "futures", self.now)
        self.assertEqual(len(got), 2499)                                   # 2500 minus the unfinished one
        self.assertEqual([k.ts for k in got], [T0 + k * BAR for k in range(2499)])  # in order, no gaps, no duplicates
        self.assertEqual(len(self.api.requests), 3)                        # 1000 + 1000 + 500
        self.assertTrue(all(r["symbol"] == "BTCUSDT" for r in self.api.requests))
        self.assertEqual([r["limit"] for r in self.api.requests[:2]], ["1000", "1000"])
        self.assertEqual(self.api.requests[2]["limit"], "501")             # only what is missing, not a full page

    async def test_spot_uses_the_spot_endpoint(self):
        await fetch_range(self.client, "BTCUSDT", "4h", T0, T0 + 10 * BAR, "spot", self.now)
        self.assertEqual(self.api.requests[0]["path"], "/api/v3/klines")

    async def test_top_symbols_ranks_usdt_perpetuals_by_volume_and_drops_dated_illiquid_and_other_quotes(self):
        self.assertEqual(await top_futures_symbols(self.client, 55), ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"])
        self.assertEqual(await top_futures_symbols(self.client, 2), ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(await top_futures_symbols(self.client, 55, min_quote_volume_usd=0),
                         ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "TINYUSDT"])

    async def test_bad_arguments_are_rejected(self):
        with self.assertRaises(ValueError):
            await fetch_range(self.client, "BTCUSDT", "7m", T0, self.now)
        with self.assertRaises(ValueError):
            await fetch_range(self.client, "BTCUSDT", "4h", T0, self.now, market="options")

    async def test_cache_is_written_and_only_the_missing_tail_is_downloaded_next_time(self):
        first = await load_history(self.client, "ETHUSDT", "4h", 400, "futures", self.tmp, now_ms=self.now)
        self.assertTrue(cache_path(self.tmp, "futures", "ETHUSDT", "4h").exists())
        self.api.candles += [candle(2500), candle(2501)]
        self.api.requests.clear()
        later = self.now + 2 * BAR                                         # candles 2499 and 2500 are now closed
        second = await load_history(self.client, "ETHUSDT", "4h", 400, "futures", self.tmp, now_ms=later)
        self.assertEqual(len(self.api.requests), 1)                        # just the tail, not 2,400 candles again
        self.assertEqual(int(self.api.requests[0]["startTime"]), T0 + 2499 * BAR)
        stamps = [k.ts for k in second]
        self.assertEqual(stamps, sorted(set(stamps)))                      # ordered, no duplicates
        self.assertEqual(stamps[-1], T0 + 2500 * BAR)                      # two new candles at the end...
        self.assertEqual(stamps[0], first[0].ts + 2 * BAR)                 # ...and the window slid forward by two
        self.assertEqual(len(second), len(first))

    async def test_a_coin_with_less_history_than_requested_still_uses_the_cache(self):
        first = await load_history(self.client, "NEWUSDT", "4h", 500, "futures", self.tmp, now_ms=self.now)
        self.assertEqual(len(first), 2499)                                 # all the exchange has, fewer than 500 days
        self.api.requests.clear()
        again = await load_history(self.client, "NEWUSDT", "4h", 500, "futures", self.tmp, now_ms=self.now)
        self.assertEqual(len(again), 2499)
        self.assertLessEqual(len(self.api.requests), 2)                    # one look for older candles + the tail

    async def test_refresh_downloads_everything_again(self):
        await load_history(self.client, "ETHUSDT", "4h", 500, "futures", self.tmp, now_ms=self.now)
        self.api.requests.clear()
        await load_history(self.client, "ETHUSDT", "4h", 500, "futures", self.tmp, refresh=True, now_ms=self.now)
        self.assertEqual(len(self.api.requests), 3)

    async def test_only_the_requested_window_is_returned(self):
        got = await load_history(self.client, "ETHUSDT", "4h", 100, "futures", self.tmp, now_ms=self.now)
        self.assertTrue(all(k.ts >= self.now - 100 * DAY_MS for k in got))
        self.assertLess(len(got), 700)

    async def test_funding_history_pages_signs_and_caches(self):
        end = T0 + 1500 * 8 * 3_600_000
        got = await fetch_funding(self.client, "BTCUSDT", T0, end)
        self.assertEqual(got, self.api.funding)                            # 1,500 events over two pages, in order
        self.assertEqual(len(self.api.requests), 2)
        self.assertIn(-0.0002, [r for _, r in got])                        # negative rates survive (longs get paid)
        now = end + 1
        first = await load_funding(self.client, "BTCUSDT", 450, self.tmp, now_ms=now)
        self.assertTrue(funding_path(self.tmp, "BTCUSDT").exists())
        self.api.funding.append((end + 5, 0.0003))
        self.api.requests.clear()
        again = await load_funding(self.client, "BTCUSDT", 450, self.tmp, now_ms=end + 10)
        self.assertEqual(len(self.api.requests), 1)                        # only what is new
        self.assertEqual(int(self.api.requests[0]["startTime"]), first[-1][0] + 1)
        self.assertEqual(again[-1], (end + 5, 0.0003))
        self.assertEqual(len({t for t, _ in again}), len(again))           # no duplicates

    def test_funding_csv_round_trip_is_exact(self):
        path = self.tmp / "f.csv"
        events = [(T0 + 5, 0.0001), (T0 + 8 * 3_600_000 + 5, -0.00012345678)]
        save_funding(path, events)
        self.assertEqual(load_funding_csv(path), events)
        self.assertEqual(load_funding_csv(self.tmp / "none.csv"), [])

    def test_csv_round_trip_is_exact(self):
        path = self.tmp / "x.csv"
        rows = [Candle(T0, 0.1 + 0.2, 1 / 3, 1e-9, 12345.6789, 7.0), candle(5)]
        save_csv(path, rows)
        self.assertEqual(load_csv(path), rows)
        self.assertEqual(load_csv(self.tmp / "missing.csv"), [])


def trade(pnl=50.0, r=1.0, symbol="BTCUSDT", side="long", strategy="trend", reason_out="target") -> Trade:
    return Trade(strategy, symbol, side, T0, T0 + 6 * BAR, 100.0, 105.0, 95.0, None, 2.0, 10.0, pnl, 0.5, 0.1, r, 6,
                 "Breakout above the 20-bar high", reason_out)


class JournalTests(unittest.TestCase):
    def test_open_close_lifecycle_and_notes(self):
        j = Journal(":memory:")
        tid = j.open_trade(mode="paper", strategy="swing", symbol="ETHUSDT", side="long", timeframe="4h", opened_at=T0,
                           entry=2000.0, stop=1900.0, target=2200.0, qty=1.0, risk_usd=100.0, reason_in="Pullback to EMA50")
        row = j.trades()[0]
        self.assertEqual((row["status"], row["symbol"], row["notes"]), ("open", "ETHUSDT", ""))
        j.close_trade(tid, closed_at=T0 + BAR, exit=2200.0, pnl_usd=195.0, fees_usd=5.0, r_multiple=1.95, reason_out="target")
        self.assertTrue(j.note(tid, "Waited for the candle close, good entry."))
        self.assertTrue(j.note(tid, "Could have held longer."))
        self.assertFalse(j.note(999, "no such trade"))
        row = j.trades()[0]
        self.assertEqual((row["status"], row["pnl_usd"], row["reason_out"]), ("closed", 195.0, "target"))
        self.assertEqual(row["notes"].splitlines(), ["Waited for the candle close, good entry.", "Could have held longer."])

    def test_backtest_trades_are_recorded_and_filtered(self):
        j = Journal(":memory:")
        for t in (trade(50, 1.0), trade(-100, -1.0, strategy="smc", symbol="SOLUSDT", reason_out="stop"), trade(30, 0.6)):
            j.record(t, run_id="run1", timeframe="4h")
        j.open_trade(mode="paper", strategy="trend", symbol="ETHUSDT", side="short", timeframe="1d", opened_at=T0,
                     entry=1.0, stop=2.0, target=None, qty=1.0, risk_usd=1.0, reason_in="x")
        self.assertEqual(len(j.trades(mode="backtest")), 3)
        self.assertEqual(len(j.trades(strategy="smc")), 1)
        self.assertEqual(len(j.trades(status="open")), 1)
        self.assertEqual(len(j.trades(run_id="run1")), 3)
        self.assertEqual(j.trades(limit=2)[0]["id"], 4)                       # newest first
        s = j.stats(mode="backtest")
        self.assertEqual((s["closed"], s["wins"]), (3, 2))
        self.assertAlmostEqual(s["win_rate"], 200 / 3)
        self.assertAlmostEqual(s["total_pnl"], -20.0)
        self.assertAlmostEqual(s["profit_factor"], 80 / 100)
        self.assertAlmostEqual(s["avg_r"], (1.0 - 1.0 + 0.6) / 3)
        self.assertEqual((s["best"], s["worst"]), (50.0, -100.0))
        self.assertEqual(j.stats(mode="paper")["open"], 1)

    def test_empty_journal(self):
        j = Journal(":memory:")
        self.assertEqual(format_trades(j.trades()), "The journal is empty.")
        s = j.stats()
        self.assertEqual((s["closed"], s["win_rate"], s["profit_factor"]), (0, 0.0, 0.0))
        self.assertIn("0 closed", format_stats(s))

    def test_export_csv_has_every_column_oldest_first(self):
        j = Journal(":memory:")
        j.record(trade(10), run_id="a")
        j.record(trade(20, symbol="ETHUSDT"), run_id="a")
        path = Path(tempfile.mkdtemp()) / "journal.csv"
        self.assertEqual(j.export_csv(path), 2)
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines[0].split(","), COLUMNS)
        self.assertIn("BTCUSDT", lines[1])
        self.assertIn("ETHUSDT", lines[2])

    def test_text_formats_show_why_and_how_it_ended(self):
        j = Journal(":memory:")
        j.record(trade(50, 1.0), run_id="a", timeframe="4h")
        open_id = j.open_trade(mode="paper", strategy="smc", symbol="ETHUSDT", side="short", timeframe="4h",
                               opened_at=T0, entry=2000.0, stop=2100.0, target=1800.0, qty=1.0, risk_usd=100.0,
                               reason_in="Sweep, break, order block retest")
        j.note(open_id, "Waiting for confirmation")
        text = format_trades(j.trades())
        for needle in ("BTCUSDT LONG", "+50.00 USD (+1.00R)", "out: target", "why in: Breakout above the 20-bar high",
                       "ETHUSDT SHORT", "OPEN since", "stop 2100", "target 1800", "why: Sweep, break, order block retest",
                       "note: Waiting for confirmation"):
            self.assertIn(needle, text)
        self.assertIn("1 closed, 1 open", format_stats(j.stats()))

    def test_the_file_survives_reopening(self):
        path = Path(tempfile.mkdtemp()) / "sub" / "j.db"
        Journal(path).record(trade(10))
        self.assertEqual(len(Journal(path).trades()), 1)


if __name__ == "__main__":
    unittest.main()
