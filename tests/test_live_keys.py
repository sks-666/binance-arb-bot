"""Per-venue API keys (testnet spot and futures are separate exchanges) and the read-only --check."""
import contextlib
import io
import unittest
from unittest import mock

import aiohttp
from aiohttp import web

from arbbot.binance_client import HOSTS, BinanceClient, BinanceError
from arbbot.config import Config
from arbbot.main import explain_binance_error, live_keys, parse_args, run_check


class FakeBinance:
    """Records which API key each request carried; can refuse one venue the way Binance does (401, -2015)."""

    def __init__(self):
        self.seen: list[tuple[str, str, str]] = []      # (method, path, X-MBX-APIKEY)
        self.refuse_spot = False

    async def handle(self, request):
        self.seen.append((request.method, request.path, request.headers.get("X-MBX-APIKEY", "")))
        if request.method != "GET":
            return web.json_response({"code": -1, "msg": "only reads expected"}, status=400)
        p = request.path
        if p.startswith("/api/") and self.refuse_spot:
            return web.json_response({"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."}, status=401)
        routes = {
            "/fapi/v1/time": {"serverTime": 1_800_000_000_000},
            "/fapi/v1/positionSide/dual": {"dualSidePosition": False},
            "/fapi/v3/balance": [{"asset": "USDT", "availableBalance": "15000.5"}],
            "/api/v3/account": {"canTrade": True, "balances": [{"asset": "USDT", "free": "10000"}],
                                "commissionRates": {"taker": "0.001"}},
            "/fapi/v1/commissionRate": {"takerCommissionRate": "0.0005"},
        }
        return web.json_response(routes[p]) if p in routes else web.json_response({"code": -1}, status=404)

    async def start(self):
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"


ENV = {"BINANCE_API_KEY": "MAINKEY", "BINANCE_API_SECRET": "MAINSEC",
       "BINANCE_TESTNET_FUTURES_API_KEY": "FUTKEY", "BINANCE_TESTNET_FUTURES_API_SECRET": "FUTSEC",
       "BINANCE_TESTNET_SPOT_API_KEY": "SPOTKEY", "BINANCE_TESTNET_SPOT_API_SECRET": "SPOTSEC"}


class KeyTests(unittest.TestCase):
    def test_mainnet_uses_one_pair_for_both(self):
        k = live_keys("mainnet", ENV)
        self.assertEqual((k["fapi"][:2], k["spot"][:2]), (("MAINKEY", "MAINSEC"), ("MAINKEY", "MAINSEC")))

    def test_testnet_uses_its_own_pair_per_venue(self):
        k = live_keys("testnet", ENV)
        self.assertEqual(k["fapi"], ("FUTKEY", "FUTSEC", "BINANCE_TESTNET_FUTURES_API_KEY"))
        self.assertEqual(k["spot"], ("SPOTKEY", "SPOTSEC", "BINANCE_TESTNET_SPOT_API_KEY"))

    def test_testnet_falls_back_to_the_general_pair(self):
        env = {k: v for k, v in ENV.items() if "SPOT" not in k}
        k = live_keys("testnet", env)
        self.assertEqual(k["spot"], ("MAINKEY", "MAINSEC", "BINANCE_API_KEY"))
        self.assertEqual(k["fapi"][0], "FUTKEY")
        self.assertEqual(live_keys("testnet", {})["fapi"], ("", "", "BINANCE_API_KEY"))

    def test_a_refusal_is_explained_without_the_key(self):
        keys = live_keys("testnet", ENV)
        msg = explain_binance_error(BinanceError(401, -2015, "Invalid API-key", "/fapi/v1/positionSide/dual"), "testnet", keys)
        self.assertIn("testnet futures", msg)
        self.assertIn("BINANCE_TESTNET_FUTURES_API_KEY", msg)
        self.assertIn("testnet.binancefuture.com", msg)
        self.assertNotIn("FUTKEY", msg)                                   # never the key itself
        msg = explain_binance_error(BinanceError(401, -2015, "Invalid", "/api/v3/account"), "testnet", keys)
        self.assertIn("testnet spot", msg)

    def test_check_is_a_flag(self):
        a = parse_args(["--check", "--testnet"])
        self.assertTrue(a.check and a.testnet and not a.live)


class ClientAndCheckTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.api = FakeBinance()
        await self.api.start()
        self.hosts = mock.patch.dict(HOSTS["testnet"], {"fapi": self.api.base, "spot": self.api.base})
        self.hosts.start()

    async def asyncTearDown(self):
        self.hosts.stop()
        await self.api.runner.cleanup()

    async def test_each_venue_is_signed_with_its_own_key(self):
        async with aiohttp.ClientSession() as s:
            c = BinanceClient(s, "testnet", venue_keys={"fapi": ("FUTKEY", "FUTSEC"), "spot": ("SPOTKEY", "SPOTSEC")})
            await c.get("fapi", "/fapi/v1/positionSide/dual", signed=True)
            await c.get("spot", "/api/v3/account", signed=True)
        self.assertEqual([h for _, _, h in self.api.seen], ["FUTKEY", "SPOTKEY"])

    async def check(self, env=ENV):
        cfg = Config()
        cfg.execution.env = "testnet"
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = await run_check(cfg, live_keys("testnet", env))
        return code, out.getvalue()

    async def test_check_reports_every_step_and_never_sends_an_order(self):
        code, out = await self.check()
        self.assertEqual(code, 0)
        for line in ("One-way Mode (ok)", "15,000.50 USDT available", "10,000.00 USDT free",
                     "futures taker 5.00 bps, spot taker 10.00 bps", "All checks passed. Nothing was traded."):
            self.assertIn(line, out)
        self.assertTrue(all(m == "GET" for m, _, _ in self.api.seen))  # reads only: no order endpoint, no POST
        self.assertFalse(any("order" in p.lower() for _, p, _ in self.api.seen))
        self.assertNotIn("FUTKEY", out)
        self.assertNotIn("SPOTSEC", out)

    async def test_check_names_the_venue_that_refuses(self):
        self.api.refuse_spot = True
        code, out = await self.check()
        self.assertEqual(code, 1)
        self.assertIn("✔ futures account mode", out)
        self.assertIn("✘ spot account: Binance testnet spot", out)
        self.assertIn("BINANCE_TESTNET_SPOT_API_KEY", out)
        self.assertIn("Some checks failed", out)

    async def test_check_marks_missing_keys(self):
        code, out = await self.check(env={})
        self.assertEqual(code, 1)
        self.assertIn("(NOT SET)", out)
