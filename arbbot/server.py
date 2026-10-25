"""Local dashboard server: static page + WebSocket snapshot stream + three guarded control endpoints.

Bound to localhost by default. Guards against DNS-rebinding and cross-site requests: the Host header
must match the bound address, and control endpoints need the per-run token embedded in the page.
"""
import asyncio
import hmac
import json
import logging
import secrets
from pathlib import Path

from aiohttp import WSMsgType, web

from .config import Config
from .engine import Engine

log = logging.getLogger("server")
ACTIONS = ("halt", "resume", "flatten")


class Dashboard:
    def __init__(self, engine: Engine, cfg: Config, web_dir: Path, market=None):
        self.engine = engine
        self.market = market                      # an arbbot.market.MarketData for the chart / order book panel, or None
        self.cfg = cfg
        self.web_dir = web_dir
        self.token = secrets.token_urlsafe(24)
        self.clients: set[web.WebSocketResponse] = set()
        port = cfg.server.port
        self.allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}"}
        if cfg.server.host not in ("127.0.0.1", "localhost", "::1"):
            self.allowed_hosts.add(f"{cfg.server.host}:{port}")

    def make_app(self) -> web.Application:
        @web.middleware
        async def guard(request: web.Request, handler):
            if request.host not in self.allowed_hosts:
                raise web.HTTPForbidden(text="bad host")
            origin = request.headers.get("Origin")
            if origin and origin != f"http://{request.host}":
                raise web.HTTPForbidden(text="bad origin")
            return await handler(request)

        app = web.Application(middlewares=[guard])
        app.router.add_get("/", self.index)
        app.router.add_get("/ws", self.ws)
        app.router.add_get("/api/snapshot", self.snapshot)
        app.router.add_get("/api/market/{kind}", self.market_data)
        app.router.add_post("/api/swing-capital", self.swing_capital)      # before the generic /api/{action}
        app.router.add_post("/api/{action}", self.action)
        return app

    async def market_data(self, request: web.Request) -> web.Response:
        """Public Binance futures market data for the market panel (read-only; see arbbot/market.py)."""
        if self.market is None:
            return web.json_response({"error": "market data is not available in this bot"}, status=404)
        kind, q = request.match_info["kind"], request.query
        calls = {"klines": lambda: self.market.klines(q.get("symbol", ""), q.get("interval", "1h")),
                 "depth": lambda: self.market.depth(q.get("symbol", "")),
                 "trades": lambda: self.market.trades(q.get("symbol", "")),
                 "info": lambda: self.market.info(q.get("symbol", "")),
                 "data": lambda: self.market.trading_data(q.get("symbol", "")),
                 "movers": lambda: self.market.movers()}
        if kind not in calls:
            raise web.HTTPNotFound()
        try:
            return web.json_response(await calls[kind](), headers={"Cache-Control": "no-store"})
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:                    # Binance down, unknown symbol, rate limit: the panel shows it, nothing else
            return web.json_response({"error": f"{type(e).__name__}: {str(e)[:160]}"}, status=502)

    async def swing_capital(self, request: web.Request) -> web.Response:
        """Give one swing paper account a new fund (see SwingRunner.set_capital: that account restarts from now)."""
        if not hmac.compare_digest(request.headers.get("X-Token", ""), self.token):
            raise web.HTTPForbidden(text="bad token")
        swing = getattr(self.engine, "swing", None)
        if swing is None:
            return web.json_response({"ok": False, "error": "the swing toolkit is not running in this bot"}, status=404)
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError("expected a JSON object")
            message = await swing.set_capital(str(body.get("strategy", "")), body.get("capital"))
        except ValueError as e:                                             # bad json, unknown account, bad amount
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        except Exception as e:                                              # e.g. the candle download failed mid-restart
            log.warning("swing fund change: %s: %s", type(e).__name__, e)
            return web.json_response({"ok": False, "error": f"saved, but the refresh failed ({type(e).__name__}); "
                                                            "it will retry on its own"}, status=500)
        return web.json_response({"ok": True, "message": message})

    async def index(self, request: web.Request) -> web.Response:
        html = (self.web_dir / "index.html").read_text(encoding="utf-8").replace("__TOKEN__", self.token)
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})

    async def snapshot(self, request: web.Request) -> web.Response:
        return web.json_response(self.engine.snapshot())

    async def action(self, request: web.Request) -> web.Response:
        action = request.match_info["action"]
        supplied = request.headers.get("X-Token", "")
        if not hmac.compare_digest(supplied, self.token):
            raise web.HTTPForbidden(text="bad token")
        if action not in ACTIONS:
            raise web.HTTPNotFound()
        {"halt": self.engine.request_halt, "resume": self.engine.request_resume,
         "flatten": self.engine.request_flatten}[action]()
        return web.json_response({"ok": True, "action": action})

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)
        self.clients.add(ws)
        try:
            await ws.send_str(json.dumps(self.engine.snapshot()))
            async for msg in ws:
                if msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self.clients.discard(ws)
        return ws

    async def broadcast_loop(self) -> None:
        interval = max(0.25, self.cfg.feed.interval_s)
        while True:
            await asyncio.sleep(interval)
            if not self.clients:
                continue
            payload = json.dumps(self.engine.snapshot())
            for ws in list(self.clients):
                try:
                    await ws.send_str(payload)
                except Exception:
                    self.clients.discard(ws)
