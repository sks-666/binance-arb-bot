"""When Binance bans or slows down this IP, the request is held back locally, and the dashboard says so in words:
nothing was sent, why, and when to try again (not "Binance refused it ... code None")."""
import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.binance_client import BinanceClient, BinanceError, NotSentError
from arbbot.main import RECONCILE_WAIT, rate_limited, reconcile_when_allowed
from arbbot.server import Dashboard


class Reply:
    def __init__(self, status=200, body="{}", headers=None):
        self.status, self.body, self.headers = status, body, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return self.body


class Session:
    """Answers every request with the next reply; counts what was actually sent."""
    def __init__(self, *replies):
        self.replies, self.sent = list(replies), 0

    def request(self, method, url, headers=None, timeout=None):
        self.sent += 1
        return self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]


class BanTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_ban_lasts_until_the_time_binance_names(self):
        until_ms = int((time.time() + 5 * 3600) * 1000)
        body = json.dumps({"code": -1003, "msg": f"Way too many requests; IP(1.2.3.4) banned until {until_ms}. "
                                                 "Please use the websocket for live updates to avoid bans."})
        s = Session(Reply(418, body, {"Retry-After": "60"}))
        c = BinanceClient(s, "testnet")
        with self.assertRaises(BinanceError):
            await c.get("fapi", "/fapi/v3/positionRisk")
        self.assertAlmostEqual(c.backoff_until, until_ms / 1000, delta=1)   # 5 hours, not the 60 s of Retry-After
        with self.assertRaises(NotSentError):
            await c.get("fapi", "/fapi/v1/depth")
        self.assertEqual(s.sent, 1)

    async def test_a_ban_without_details_is_not_ten_seconds(self):
        c = BinanceClient(Session(Reply(418, "")), "testnet")
        with self.assertRaises(BinanceError):
            await c.get("fapi", "/fapi/v1/time")
        self.assertGreaterEqual(c.paused_for(), 110)

    async def test_a_restart_during_a_ban_sends_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "ban.json"
            body = json.dumps({"code": -1003, "msg": f"banned until {int((time.time() + 600) * 1000)}"})
            first = BinanceClient(Session(Reply(418, body)), "testnet", backoff_file=f)
            with self.assertRaises(BinanceError):
                await first.get("fapi", "/fapi/v1/time")
            s = Session(Reply(200, "{}"))
            second = BinanceClient(s, "testnet", backoff_file=f)        # the next start reads the saved ban
            with self.assertRaises(NotSentError) as cm:
                await second.get("fapi", "/fapi/v3/positionRisk", signed=False)
            self.assertEqual(cm.exception.status, 418)
            self.assertEqual(s.sent, 0)

    async def test_an_expired_saved_ban_is_ignored(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "ban.json"
            f.write_text(json.dumps({"until": time.time() - 1, "status": 418}))
            s = Session(Reply(200, "{}"))
            await BinanceClient(s, "testnet", backoff_file=f).get("fapi", "/fapi/v1/time")
            self.assertEqual(s.sent, 1)


class ThrottleTests(unittest.IsolatedAsyncioTestCase):
    async def test_near_the_weight_limit_market_data_waits_but_account_calls_go(self):
        s = Session(Reply(200, "{}", {"X-MBX-USED-WEIGHT-1M": "2000"}))
        c = BinanceClient(s, "testnet", api_key="k", api_secret="x")
        await c.get("fapi", "/fapi/v1/ticker/bookTicker")
        with self.assertRaises(NotSentError):
            await c.get("fapi", "/fapi/v1/premiumIndex")
        await c.get("fapi", "/fapi/v3/positionRisk", signed=True)      # positions can still be managed
        await c.get("spot", "/api/v3/ticker/bookTicker")              # the other venue has its own limit
        self.assertEqual(s.sent, 3)
        self.assertEqual(c.paused_for(), 0)                           # no entry/exit block: Binance has not refused

    async def test_below_the_limit_nothing_waits(self):
        s = Session(Reply(200, "{}", {"X-MBX-USED-WEIGHT-1M": "300"}))
        c = BinanceClient(s, "testnet")
        await c.get("fapi", "/fapi/v1/ticker/bookTicker")
        await c.get("fapi", "/fapi/v1/premiumIndex")
        self.assertEqual(s.sent, 2)


class ReconcileRetryTests(unittest.IsolatedAsyncioTestCase):
    def test_what_counts_as_rate_limited(self):
        self.assertTrue(rate_limited(BinanceError(418, -1003, "Way too many requests", "/fapi/v3/positionRisk")))
        self.assertTrue(rate_limited(NotSentError(418, None, "held back", "/fapi/v3/positionRisk")))
        self.assertFalse(rate_limited(BinanceError(401, -2015, "Invalid API-key", "/fapi/v3/positionRisk")))
        self.assertFalse(rate_limited(RuntimeError("boom")))

    async def _run(self, results):
        calls = []

        class Ex:
            async def reconcile(self, positions, markets, ignore=frozenset()):
                calls.append(1)
                r = results.pop(0)
                if isinstance(r, Exception):
                    raise r
                return r

        risk = SimpleNamespace(fault=f"{RECONCILE_WAIT} (banned)")
        risk.trip_fault = lambda reason: setattr(risk, "fault", reason)
        events = []
        client = SimpleNamespace(paused_for=lambda: 0.0)
        task = asyncio.create_task(reconcile_when_allowed(
            Ex(), client, SimpleNamespace(open={}), SimpleNamespace(markets={}), risk, frozenset,
            lambda lvl, msg: events.append((lvl, msg)), min_wait_s=0.0))
        for _ in range(50):
            await asyncio.sleep(0)
            if events:
                break
        self.assertFalse(task.done())          # a finished task would stop the bot: it idles instead
        task.cancel()
        return risk, events, calls

    async def test_the_fault_clears_once_the_exchange_agrees(self):
        risk, events, calls = await self._run([NotSentError(418, None, "still banned", "/x"), []])
        self.assertEqual(risk.fault, "")
        self.assertEqual(len(calls), 2)
        self.assertEqual(events[0][0], "info")

    async def test_a_real_mismatch_after_the_ban_stays_a_fault(self):
        risk, events, _ = await self._run([["BTCUSDT: bot expects short 1 but Binance reports positionAmt 0"]])
        self.assertIn("startup reconciliation failed: BTCUSDT", risk.fault)
        self.assertEqual(events[0][0], "error")


class BackoffTextTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_ban_is_held_back_and_explained(self):
        c = BinanceClient(None, "testnet")          # no session: a request that left this machine would crash
        c.backoff_until, c.backoff_status = time.time() + 1500, 418
        with self.assertRaises(NotSentError) as cm:
            await c.get("fapi", "/fapi/v1/ticker/bookTicker", {"symbol": "POLYXUSDT"})
        e = cm.exception
        self.assertEqual(e.status, 418)
        self.assertIn("banned this network's IP address", e.msg)
        self.assertIn(time.strftime("%H:%M", time.localtime(c.backoff_until)), e.msg)
        self.assertIn("25 min", e.msg)

    def test_a_slow_down_is_not_called_a_ban(self):
        c = BinanceClient(None, "mainnet")
        c.backoff_until = time.time() + 10
        self.assertIn("slow down (HTTP 429)", c.backoff_text())


class HeldBack:
    """A ticket whose every call is held back by the backoff."""
    async def close_position(self, symbol):
        raise NotSentError(418, None, "Binance has temporarily banned this network's IP address for too many requests "
                                      "(HTTP 418); requests are held back until 17:15:00 (24 min 0 s from now)",
                           "/fapi/v1/ticker/bookTicker")


class RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        cfg = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=1), feed=SimpleNamespace(interval_s=1.0))
        self.dash = Dashboard(SimpleNamespace(swing=None), cfg, Path("web"), paper=HeldBack())
        server = TestServer(self.dash.make_app())
        await server.start_server()
        self.dash.allowed_hosts.add(f"127.0.0.1:{server.port}")
        self.client = TestClient(server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_close_during_a_ban_says_nothing_was_sent_and_when_to_retry(self):
        r = await self.client.post("/api/paper/close", json={"symbol": "POLYXUSDT"}, headers={"X-Token": self.dash.token})
        d = await r.json()
        self.assertEqual(r.status, 503)
        self.assertTrue(d["error"].startswith("Not sent, nothing changed on your account"))
        self.assertIn("until 17:15:00", d["error"])
        self.assertNotIn("code None", d["error"])


if __name__ == "__main__":
    unittest.main()
