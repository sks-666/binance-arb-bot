"""Local dashboard server: static page + WebSocket snapshot stream + three guarded control endpoints.

Bound to localhost by default. Guards against DNS-rebinding and cross-site requests: the Host header
must match the bound address, and control endpoints need the per-run token embedded in the page.
"""
import asyncio
import hmac
import ipaddress
import json
import logging
import os
import secrets
import subprocess
import time
from pathlib import Path

from aiohttp import WSMsgType, web

from .binance_client import BinanceError, NotSentError
from .config import Config
from .engine import Engine
from .pnl_report import PERIODS, report as pnl_report

log = logging.getLogger("server")
ACTIONS = ("halt", "resume", "flatten")
LOOPBACK = ("127.0.0.1", "::1")
LAN_COOKIE = "arb_lan_key"


def lan_host_ok(host: str) -> bool:
    """A Host header a phone on the home network sends: a private IP address or this Mac's .local name. (DNS-rebinding
    attacks need a domain name the attacker controls, so private IP literals and .local cannot carry one.)"""
    name = host.rsplit(":", 1)[0].strip("[]")
    if name.endswith(".local"):
        return True
    try:
        return ipaddress.ip_address(name).is_private
    except ValueError:
        return False


def lan_key(path: Path) -> str:
    """The key other devices need (--lan). Kept in data/ so a bookmarked phone link survives restarts."""
    path = Path(path)
    try:
        key = path.read_text(encoding="utf-8").strip()
    except OSError:
        key = ""
    if len(key) < 16:
        key = secrets.token_urlsafe(16)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(key, encoding="utf-8")
        os.chmod(path, 0o600)
    return key


def lan_addresses() -> list[str]:
    """This Mac's addresses on the home network (private IPv4 on real interfaces; VPN tunnels and loopback left out)."""
    try:
        out = subprocess.run(["ifconfig"], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    found, iface = [], ""
    for line in out.splitlines():
        if line and not line[0].isspace():
            iface = line.split(":", 1)[0]
        elif line.strip().startswith("inet ") and not iface.startswith(("lo", "utun", "bridge", "awdl", "llw")):
            ip = line.split()[1]
            if ipaddress.ip_address(ip).is_private:
                found.append(ip)
    return found


LOCKED = """<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Locked</title><body style="font:16px system-ui;padding:24px;max-width:520px;margin:auto">
<h2>This dashboard is locked</h2><p>Open it with the phone link the bot printed when it started (it ends in
<code>?key=…</code>). Run <code>Start Binance Bots.command</code> on the Mac to see it again.</p></body>"""


class Dashboard:
    def __init__(self, engine: Engine, cfg: Config, web_dir: Path, market=None, paper=None, lan_key: str | None = None):
        self.engine = engine
        self.market = market                      # an arbbot.market.MarketData for the chart / order book panel, or None
        self.paper = paper                        # an arbbot.paper_futures.PaperFutures for the order ticket, or None
        self.cfg = cfg
        self.web_dir = web_dir
        self.token = secrets.token_urlsafe(24)
        self.clients: set[web.WebSocketResponse] = set()
        port = cfg.server.port
        self.allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}"}
        if cfg.server.host not in ("127.0.0.1", "localhost", "::1"):
            self.allowed_hosts.add(f"{cfg.server.host}:{port}")
        # --lan: other devices on the home network may open the page, but only with this key (the page carries the
        # control token, and on mainnet the ticket trades real money). This Mac itself never needs it.
        self.lan_key = lan_key

    def is_local(self, request: web.Request) -> bool:
        return request.remote in LOOPBACK

    def make_app(self) -> web.Application:
        @web.middleware
        async def guard(request: web.Request, handler):
            lan_host = (bool(self.lan_key) and request.host.endswith(f":{self.cfg.server.port}")
                        and lan_host_ok(request.host))
            if request.host not in self.allowed_hosts and not lan_host:
                raise web.HTTPForbidden(text="bad host")
            origin = request.headers.get("Origin")
            if origin and origin != f"http://{request.host}":
                raise web.HTTPForbidden(text="bad origin")
            if self.lan_key and not self.is_local(request):
                if hmac.compare_digest(request.query.get("key", ""), self.lan_key):
                    resp = web.HTTPFound("/")          # remember the key, then drop it from the address bar
                    resp.set_cookie(LAN_COOKIE, self.lan_key, max_age=180 * 86_400, httponly=True, samesite="Strict")
                    raise resp
                if not hmac.compare_digest(request.cookies.get(LAN_COOKIE, ""), self.lan_key):
                    log.warning("locked out %s (no or wrong LAN key) on %s", request.remote, request.path)
                    return web.Response(text=LOCKED, content_type="text/html", status=401)
            return await handler(request)

        app = web.Application(middlewares=[guard])
        app.router.add_get("/", self.index)                 # the P&L reports
        for page in ("/arbitrage", "/swing", "/market"):    # the details: same page, another view (see pageView())
            app.router.add_get(page, self.index)
            app.router.add_get(page + "/", self.index)
        app.router.add_get("/ws", self.ws)
        app.router.add_get("/api/snapshot", self.snapshot)
        app.router.add_get("/api/pnl", self.pnl)
        app.router.add_get("/api/market/{kind}", self.market_data)
        app.router.add_get("/api/paper/state", self.paper_state)
        app.router.add_post("/api/paper/{op}", self.paper_action)
        app.router.add_post("/api/swing-capital", self.swing_capital)      # before the generic /api/{action}
        app.router.add_post("/api/swing-power", self.swing_power)
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

    async def paper_state(self, request: web.Request) -> web.Response:
        if self.paper is None:
            return web.json_response({"error": "the paper futures account is not running in this bot"}, status=404)
        return web.json_response(self.paper.view(), headers={"Cache-Control": "no-store"})

    async def paper_action(self, request: web.Request) -> web.Response:
        """The order ticket: a paper account (arbbot/paper_futures.py), or a real Binance futures account
        (arbbot/testnet_ticket.py: the testnet, or mainnet only under --live --real-ticket). Never the arbitrage executor."""
        if not hmac.compare_digest(request.headers.get("X-Token", ""), self.token):
            raise web.HTTPForbidden(text="bad token")
        if self.paper is None:
            return web.json_response({"ok": False, "error": "the paper futures account is not running in this bot"},
                                     status=404)
        op = request.match_info["op"]
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError("expected a JSON object")
            if op == "order":
                result = await self.paper.place_order(body)
            elif op == "cancel":
                result = await self.paper.cancel(body.get("id"))
            elif op == "close":
                result = await self.paper.close_position(body.get("symbol", ""))
            elif op == "leverage":
                result = await self.paper.set_leverage(body.get("symbol", ""), body.get("leverage"))
            elif op == "transfer":
                result = await self.paper.transfer(body.get("amount"))
            else:
                raise web.HTTPNotFound()
        except ValueError as e:                                             # a refused order: say why
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        except web.HTTPException:
            raise
        except NotSentError as e:                                           # held back by the rate-limit backoff: never sent
            log.warning("ticket %s not sent: %s", op, e)
            return web.json_response({"ok": False, "error": f"Not sent, nothing changed on your account: {e.msg}. "
                                                            f"Try again then."}, status=503)
        except BinanceError as e:                                           # Binance itself refused (e.g. not enough margin)
            log.warning("ticket %s refused by Binance: %s", op, e)
            return web.json_response({"ok": False, "error": f"Binance refused it: {e.msg} (code {e.code})"}, status=400)
        except Exception as e:                                              # Binance unreachable for the price
            log.warning("paper %s failed: %s: %s", op, type(e).__name__, e)
            return web.json_response({"ok": False, "error": f"could not get a price from Binance ({type(e).__name__})"},
                                     status=502)
        return web.json_response({"ok": True, "result": result, "state": self.paper.view()})

    async def swing_power(self, request: web.Request) -> web.Response:
        """Switch the swing toolkit on or off (SwingRunner.set_enabled): off = no new entries, exits keep working."""
        if not hmac.compare_digest(request.headers.get("X-Token", ""), self.token):
            raise web.HTTPForbidden(text="bad token")
        swing = getattr(self.engine, "swing", None)
        if swing is None:
            return web.json_response({"ok": False, "error": "the swing toolkit is not running in this bot (start it with --swing)"},
                                     status=404)
        try:
            body = await request.json()
            if not isinstance(body, dict) or not isinstance(body.get("on"), bool):
                raise ValueError('expected {"on": true} or {"on": false}')
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        message = await swing.set_enabled(body["on"])
        return web.json_response({"ok": True, "message": message, "enabled": swing.enabled})

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

    async def pnl(self, request: web.Request) -> web.Response:
        """P&L by day, week or month for one panel (read-only): ?panel=arb|swing|demo|ticket&period=day|week|month."""
        panel, period = request.query.get("panel", "arb"), request.query.get("period", "day")
        if period not in PERIODS:
            return web.json_response({"error": f"period must be one of {', '.join(PERIODS)}"}, status=400)
        swing = getattr(self.engine, "swing", None)
        try:
            if panel == "arb":
                events, note = self.engine.pf.store.pnl_events(), "arbitrage trades · after fees"
            elif panel in ("swing", "demo") and swing is not None:
                events = swing.pnl_events("demo" if panel == "demo" else "paper")
                note = ("swing kit trades on the futures testnet demo account · after fees, from Binance's fills"
                        if panel == "demo" else "swing kit paper accounts (trend, swing, smc) · after fees")
            elif panel == "ticket" and self.paper is not None:
                events, note = await self.paper.pnl_events()
            else:
                return web.json_response({"error": f"no {panel} panel in this bot"}, status=404)
        except Exception as e:                    # Binance down or rate limited for the account's history
            log.warning("pnl report %s failed: %s: %s", panel, type(e).__name__, e)
            return web.json_response({"error": f"could not read the history: {type(e).__name__}: {str(e)[:120]}"},
                                     status=502)
        return web.json_response(dict(pnl_report(events, period, time.time()), panel=panel, note=note),
                                 headers={"Cache-Control": "no-store"})

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
