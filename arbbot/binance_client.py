"""Minimal async Binance REST client (public + HMAC-signed) for USD-M futures and spot."""
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from collections import Counter

import aiohttp

log = logging.getLogger("binance")

HOSTS = {
    "mainnet": {"fapi": "https://fapi.binance.com", "spot": "https://api.binance.com"},
    "testnet": {"fapi": "https://testnet.binancefuture.com", "spot": "https://testnet.binance.vision"},
}


class BinanceError(Exception):
    def __init__(self, status: int, code: int | None, msg: str, path: str):
        super().__init__(f"{path} -> HTTP {status} code={code} {msg}")
        self.status, self.code, self.msg, self.path = status, code, msg, path


class NotSentError(BinanceError):
    """The request was held back by the local rate-limit backoff and never left this machine: for an order, a certain
    non-fill (unlike a timeout or an error reply, which leave the outcome unknown)."""


class BinanceClient:
    def __init__(
        self, session: aiohttp.ClientSession, env: str = "mainnet", api_key: str = "", api_secret: str = "",
        timeout_s: float = 4.0, venue_keys: dict[str, tuple[str, str]] | None = None,
    ):
        self.session = session
        self.hosts = HOSTS[env]
        self.env = env
        self.api_key = api_key
        self.api_secret = api_secret
        # per-venue (key, secret): on testnet, spot and futures are separate exchanges with separate keys
        self.venue_keys = dict(venue_keys or {})
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.time_offset_ms = 0
        self.used_weight = {"fapi": 0, "spot": 0}
        self.calls: Counter = Counter()     # "venue path" -> requests sent (to find what spends the request weight)
        self.backoff_until = 0.0
        self.backoff_status = 429          # 418 = Binance banned this IP for a while, 429 = slow down

    async def request(self, venue: str, method: str, path: str, params: dict | None = None, signed: bool = False,
                      timeout: float | None = None):
        if time.time() < self.backoff_until:
            raise NotSentError(self.backoff_status, None, self.backoff_text(), path)
        params = {k: v for k, v in (params or {}).items() if v is not None}
        headers = {}
        api_key, api_secret = self.venue_keys.get(venue, (self.api_key, self.api_secret))
        if signed:
            if not (api_key and api_secret):
                raise RuntimeError(f"an API key and secret for {venue} are required for signed requests")
            params["timestamp"] = int(time.time() * 1000) + self.time_offset_ms
            params["recvWindow"] = 5000
            query = urllib.parse.urlencode(params)
            sig = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query = f"{query}&signature={sig}"
        else:
            query = urllib.parse.urlencode(params)
        if api_key:
            headers["X-MBX-APIKEY"] = api_key
        self.calls[f"{venue} {path}"] += 1
        url = f"{self.hosts[venue]}{path}" + (f"?{query}" if query else "")
        limit = aiohttp.ClientTimeout(total=timeout) if timeout else self.timeout
        async with self.session.request(method, url, headers=headers, timeout=limit) as resp:
            body = await resp.text()
            weight = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT")
            if weight and weight.isdigit():
                self.used_weight[venue] = int(weight)
            if resp.status in (418, 429):
                retry_after = resp.headers.get("Retry-After", "10")
                self.backoff_until = time.time() + (int(retry_after) if retry_after.isdigit() else 10)
                self.backoff_status = resp.status
                log.warning("Binance %s answered HTTP %s on %s: %s", self.env, resp.status, path, self.backoff_text())
            try:
                data = json.loads(body)
            except ValueError:
                data = None
            if resp.status != 200:
                code = data.get("code") if isinstance(data, dict) else None
                msg = data.get("msg") if isinstance(data, dict) else body[:200]
                raise BinanceError(resp.status, code, str(msg), path)
            return data

    def paused_for(self) -> float:
        """Seconds left in the rate-limit backoff (Binance's 429 / 418 Retry-After); 0 when requests can be sent."""
        return max(0.0, self.backoff_until - time.time())

    def backoff_text(self) -> str:
        """The backoff in words: why, and when requests can be sent again (local time)."""
        mins, secs = divmod(round(self.paused_for()), 60)
        why = ("Binance has temporarily banned this network's IP address for too many requests (HTTP 418)"
               if self.backoff_status == 418 else "Binance asked this network's IP address to slow down (HTTP 429)")
        when = time.strftime("%H:%M:%S", time.localtime(self.backoff_until))
        return f"{why}; requests are held back until {when} ({mins} min {secs} s from now)"

    async def get(self, venue: str, path: str, params: dict | None = None, signed: bool = False,
                  timeout: float | None = None):
        return await self.request(venue, "GET", path, params, signed, timeout)

    async def post(self, venue: str, path: str, params: dict | None = None, signed: bool = True):
        return await self.request(venue, "POST", path, params, signed)

    async def sync_time(self) -> int:
        """Align signed-request timestamps with Binance server time; returns the offset in ms."""
        t0 = int(time.time() * 1000)
        data = await self.get("fapi", "/fapi/v1/time")
        t1 = int(time.time() * 1000)
        self.time_offset_ms = int(data["serverTime"]) - (t0 + t1) // 2
        return self.time_offset_ms
