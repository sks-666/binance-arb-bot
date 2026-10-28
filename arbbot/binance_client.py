"""Minimal async Binance REST client (public + HMAC-signed) for USD-M futures and spot."""
import hashlib
import hmac
import json
import logging
import os
import re
import time
import urllib.parse
from collections import Counter
from pathlib import Path

import aiohttp

log = logging.getLogger("binance")

HOSTS = {
    "mainnet": {"fapi": "https://fapi.binance.com", "spot": "https://api.binance.com"},
    "testnet": {"fapi": "https://testnet.binancefuture.com", "spot": "https://testnet.binance.vision"},
}
# Request weight Binance allows per IP per minute. Above THROTTLE_AT of it, public market-data requests (the heavy ones:
# tickers, depth, exchange info) are held back until the next minute, so the bot never runs into a 429 and then a 418.
WEIGHT_LIMIT = {"fapi": 2400, "spot": 6000}
THROTTLE_AT = 0.8
BAN_UNTIL = re.compile(r"banned until (\d{12,14})")
MIN_BAN_S = 120          # a 418 that says nothing about its length: Binance's shortest ban is 2 minutes


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
        timeout_s: float = 4.0, venue_keys: dict[str, tuple[str, str]] | None = None, backoff_file: Path | None = None,
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
        self.throttle_until = {"fapi": 0.0, "spot": 0.0}   # our own pause on public data near the weight limit
        # A ban outlives this process: written down so a restart (or a second bot on this machine) does not hit Binance
        # again while banned, which only makes Binance ban the IP for longer.
        self.backoff_file = backoff_file
        self._load_backoff()

    def _load_backoff(self) -> None:
        if not self.backoff_file:
            return
        try:
            d = json.loads(Path(self.backoff_file).read_text())
            if float(d["until"]) > time.time():
                self.backoff_until, self.backoff_status = float(d["until"]), int(d.get("status", 418))
        except (OSError, ValueError, KeyError, TypeError):
            pass

    def _save_backoff(self) -> None:
        if not self.backoff_file:
            return
        try:
            path = Path(self.backoff_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps({"until": self.backoff_until, "status": self.backoff_status, "env": self.env}))
            os.replace(tmp, path)
        except OSError as e:
            log.warning("could not save the rate-limit backoff to %s: %s", self.backoff_file, e)

    def _note_rate_limit(self, status: int, retry_after: str | None, body: str) -> None:
        """Binance answered 429 (slow down) or 418 (IP banned): hold every request back until it allows them again.
        A 418 names its end in the message ("banned until <ms>"); the longest of that and Retry-After wins."""
        wait = int(retry_after) if retry_after and retry_after.isdigit() else (MIN_BAN_S if status == 418 else 10)
        until = time.time() + wait
        m = BAN_UNTIL.search(body or "")
        if m:
            until = max(until, int(m.group(1)) / 1000)
        if until > self.backoff_until or status == 418:
            self.backoff_until = max(self.backoff_until, until)
            self.backoff_status = status
            self._save_backoff()

    def _note_weight(self, venue: str, used: int) -> None:
        """Past THROTTLE_AT of the minute's weight, public data waits for the next minute (Binance counts weight per
        calendar minute). Signed requests (account, orders) still go, so open positions can always be managed."""
        self.used_weight[venue] = used
        limit = WEIGHT_LIMIT.get(venue, 0)
        if limit and used >= THROTTLE_AT * limit:
            nxt = (int(time.time()) // 60 + 1) * 60 + 1
            if nxt > self.throttle_until.get(venue, 0.0):
                self.throttle_until[venue] = nxt
                log.warning("Binance %s %s request weight %d of %d this minute: market-data requests wait until %s",
                            self.env, venue, used, limit, time.strftime("%H:%M:%S", time.localtime(nxt)))

    async def request(self, venue: str, method: str, path: str, params: dict | None = None, signed: bool = False,
                      timeout: float | None = None):
        if time.time() < self.backoff_until:
            raise NotSentError(self.backoff_status, None, self.backoff_text(), path)
        if not signed and time.time() < self.throttle_until.get(venue, 0.0):
            when = time.strftime("%H:%M:%S", time.localtime(self.throttle_until[venue]))
            raise NotSentError(429, None, f"request weight near Binance's per-minute limit; this bot holds market-data "
                                          f"requests back until {when}", path)
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
                self._note_weight(venue, int(weight))
            if resp.status in (418, 429):
                self._note_rate_limit(resp.status, resp.headers.get("Retry-After"), body)
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
