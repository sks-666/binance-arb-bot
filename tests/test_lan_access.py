"""--lan: the dashboard opens on a phone on the home network only with the private key; this Mac never needs it."""
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from arbbot.server import Dashboard, lan_host_ok, lan_key

KEY = "k" * 22


class Phone(Dashboard):
    """The test client connects from 127.0.0.1; pretend it is a phone on the Wi-Fi."""
    def is_local(self, request):
        return False


class LanTests(unittest.IsolatedAsyncioTestCase):
    async def start(self, cls=Phone, key=KEY):
        cfg = SimpleNamespace(server=SimpleNamespace(host="0.0.0.0", port=1), feed=SimpleNamespace(interval_s=1.0))
        engine = SimpleNamespace(swing=None, snapshot=lambda: {"ok": True})
        self.dash = cls(engine, cfg, Path("web"), lan_key=key)
        server = TestServer(self.dash.make_app())
        await server.start_server()
        self.dash.cfg.server.port = server.port               # the guard compares the Host header's port
        self.dash.allowed_hosts.add(f"127.0.0.1:{server.port}")
        self.client = TestClient(server)
        await self.client.start_server()
        self.port = server.port

    async def asyncTearDown(self):
        if hasattr(self, "client"):
            await self.client.close()

    def get(self, path, host=None, **kw):
        return self.client.get(path, headers={"Host": host or f"192.168.100.9:{self.port}"}, allow_redirects=False, **kw)

    async def test_a_phone_without_the_key_sees_a_locked_page_and_no_token(self):
        await self.start()
        for path in ("/", "/?key=wrong", "/api/snapshot", "/api/paper/state"):
            r = await self.get(path)
            body = await r.text()
            self.assertEqual(r.status, 401, path)
            self.assertIn("locked", body)
            self.assertNotIn(self.dash.token, body)

    async def test_the_key_link_sets_a_cookie_then_the_page_and_api_open(self):
        await self.start()
        r = await self.get(f"/?key={KEY}")
        self.assertEqual((r.status, r.headers["Location"]), (302, "/"))
        cookie = r.cookies["arb_lan_key"]
        self.assertEqual((cookie.value, bool(cookie["httponly"]), cookie["samesite"]), (KEY, True, "Strict"))
        r = await self.get("/", cookies={"arb_lan_key": KEY})
        self.assertEqual(r.status, 200)
        self.assertIn(self.dash.token, await r.text())        # the page works: it carries the control token
        self.assertEqual((await self.get("/api/snapshot", cookies={"arb_lan_key": KEY})).status, 200)

    async def test_hosts_a_phone_may_use_and_ones_it_may_not(self):
        await self.start()
        ok = {"arb_lan_key": KEY}
        self.assertEqual((await self.get("/", f"192.168.100.9:{self.port}", cookies=ok)).status, 200)
        self.assertEqual((await self.get("/", f"Surendras-Mac.local:{self.port}", cookies=ok)).status, 200)
        self.assertEqual((await self.get("/", f"evil.example.com:{self.port}", cookies=ok)).status, 403)
        self.assertEqual((await self.get("/", "192.168.100.9:9999", cookies=ok)).status, 403)
        self.assertFalse(lan_host_ok("8.8.8.8:80"))

    async def test_this_mac_needs_no_key_and_without_lan_a_phone_address_is_refused(self):
        await self.start(cls=Dashboard)
        self.assertEqual((await self.get("/", f"127.0.0.1:{self.port}")).status, 200)
        await self.client.close()
        await self.start(cls=Dashboard, key=None)
        self.assertEqual((await self.get("/", f"192.168.100.9:{self.port}")).status, 403)

    def test_the_key_is_kept_private_and_reused(self):
        path = Path(tempfile.mkdtemp()) / "lan-key-8767.txt"
        key = lan_key(path)
        self.assertGreaterEqual(len(key), 16)
        self.assertEqual(lan_key(path), key)                  # a bookmarked phone link keeps working
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
