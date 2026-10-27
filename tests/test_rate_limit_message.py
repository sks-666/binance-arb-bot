"""When Binance bans or slows down this IP, the request is held back locally, and the dashboard says so in words:
nothing was sent, why, and when to try again (not "Binance refused it ... code None")."""
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.binance_client import BinanceClient, NotSentError
from arbbot.server import Dashboard


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
