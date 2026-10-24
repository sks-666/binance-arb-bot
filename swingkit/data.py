"""Binance candle history: download in pages, keep only CLOSED candles, cache as CSV, top up from the last cached candle.

Public endpoints only (no API key). Uses USD-M futures candles by default (the market the strategies are meant for) or spot.
"""
import csv
import time
from pathlib import Path

from arbbot.binance_client import BinanceClient

from .models import Candle, Series

INTERVAL_MS = {"1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
               "12h": 43_200_000, "1d": 86_400_000}
DAY_MS = 86_400_000
PAGE = 1000
MARKETS = {"futures": ("fapi", "/fapi/v1/klines"), "spot": ("spot", "/api/v3/klines")}
FIELDS = ["ts", "open", "high", "low", "close", "volume"]


def parse_klines(rows: list) -> list[Candle]:
    return [Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rows]


async def fetch_range(client: BinanceClient, symbol: str, interval: str, start_ms: int, end_ms: int,
                      market: str = "futures", now_ms: int | None = None) -> list[Candle]:
    """Closed candles with open time in [start_ms, end_ms], oldest first, no duplicates."""
    if interval not in INTERVAL_MS:
        raise ValueError(f"interval must be one of {', '.join(INTERVAL_MS)}")
    if market not in MARKETS:
        raise ValueError("market must be 'futures' or 'spot'")
    bar, venue_path = INTERVAL_MS[interval], MARKETS[market]
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    out: dict[int, Candle] = {}
    cursor = start_ms
    while cursor <= min(end_ms, now_ms):
        wanted = (min(end_ms, now_ms) - cursor) // bar + 2          # ask for what is missing, not a full page: a top-up of
        rows = await client.get(venue_path[0], venue_path[1], {     # a few candles costs 1 request weight instead of 5
            "symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end_ms,
            "limit": min(PAGE, max(2, wanted))}, timeout=20.0)
        if not rows:
            break
        for k in parse_klines(rows):
            out[k.ts] = k
        nxt = int(rows[-1][0]) + bar
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < PAGE:
            break
    return sorted((k for k in out.values() if k.ts + bar <= now_ms), key=lambda k: k.ts)  # the forming candle is dropped


def save_csv(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(FIELDS)
        for k in candles:
            w.writerow([k.ts, repr(k.open), repr(k.high), repr(k.low), repr(k.close), repr(k.volume)])


def load_csv(path: Path) -> list[Candle]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return [Candle(int(r["ts"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
                       float(r["volume"])) for r in csv.DictReader(fh)]


def cache_path(cache_dir: Path, market: str, symbol: str, interval: str) -> Path:
    return Path(cache_dir) / f"{market}-{symbol}-{interval}.csv"


async def load_history(client: BinanceClient, symbol: str, interval: str, days: int, market: str = "futures",
                       cache_dir: Path = Path("data/klines"), refresh: bool = False,
                       now_ms: int | None = None) -> list[Candle]:
    """The last `days` of closed candles. Reads the cache, downloads only what is missing, and writes the cache back."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    bar, path = INTERVAL_MS[interval], cache_path(cache_dir, market, symbol, interval)
    want_from = now_ms - days * DAY_MS
    cached = [] if refresh else load_csv(path)
    if cached:
        merged = {k.ts: k for k in cached}
        if cached[0].ts > want_from + bar:  # the window starts before the cache: ask for the head once (a coin listed
            head = await fetch_range(client, symbol, interval, want_from, cached[0].ts - bar, market, now_ms)  # later has none)
            merged.update({k.ts: k for k in head})
        tail = await fetch_range(client, symbol, interval, cached[-1].ts + bar, now_ms, market, now_ms)
        merged.update({k.ts: k for k in tail})
    else:
        merged = {k.ts: k for k in await fetch_range(client, symbol, interval, want_from, now_ms, market, now_ms)}
    candles = sorted(merged.values(), key=lambda k: k.ts)
    save_csv(path, candles)
    return [k for k in candles if k.ts >= want_from]


def to_series(candles: list[Candle]) -> Series:
    return Series.from_candles(candles)


async def top_futures_symbols(client: BinanceClient, n: int = 55, min_quote_volume_usd: float = 20e6) -> list[str]:
    """The n most traded USDT-margined perpetuals by 24h quote volume (the same ranking the arbitrage bot uses for its
    universe, minus its spot-counterpart requirement: swing trading here only needs the futures candles)."""
    rows = await client.get("fapi", "/fapi/v1/ticker/24hr", timeout=20.0)
    ranked = sorted(((float(r["quoteVolume"]), r["symbol"]) for r in rows
                     if r["symbol"].endswith("USDT") and "_" not in r["symbol"]
                     and float(r["quoteVolume"]) >= min_quote_volume_usd), reverse=True)
    return [sym for _, sym in ranked[:n]]


async def fetch_forming(client: BinanceClient, symbol: str, interval: str, market: str = "futures",
                        now_ms: int | None = None) -> Candle | None:
    """The candle that is still FORMING, reduced to its open price (all that is known), or None if there isn't one yet
    (right at a boundary). A forming candle may fill a pending order at its open; nothing else is ever decided on it."""
    bar, venue_path = INTERVAL_MS[interval], MARKETS[market]
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    rows = await client.get(venue_path[0], venue_path[1], {"symbol": symbol, "interval": interval, "limit": 1},
                            timeout=15.0)
    if not rows:
        return None
    k = parse_klines(rows[-1:])[0]
    return None if k.ts + bar <= now_ms else Candle(k.ts, k.open, k.open, k.open, k.open, 0.0)


# ---- funding rates (USD-M futures only): who pays whom every 8 hours -----------------------------
FUNDING_PAGE = 1000


async def fetch_funding(client: BinanceClient, symbol: str, start_ms: int, end_ms: int) -> list[tuple[int, float]]:
    """(funding time ms, rate) pairs in [start_ms, end_ms], oldest first. A positive rate means longs pay shorts."""
    out: dict[int, float] = {}
    cursor = start_ms
    while cursor <= end_ms:
        rows = await client.get("fapi", "/fapi/v1/fundingRate", {
            "symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": FUNDING_PAGE}, timeout=20.0)
        if not rows:
            break
        for r in rows:
            out[int(r["fundingTime"])] = float(r["fundingRate"])
        nxt = int(rows[-1]["fundingTime"]) + 1
        if nxt <= cursor or len(rows) < FUNDING_PAGE:
            break
        cursor = nxt
    return sorted(out.items())


def funding_path(cache_dir: Path, symbol: str) -> Path:
    return Path(cache_dir) / f"funding-{symbol}.csv"


def save_funding(path: Path, events: list[tuple[int, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["ts", "rate"])
        for ts, rate in events:
            w.writerow([ts, repr(rate)])


def load_funding_csv(path: Path) -> list[tuple[int, float]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return [(int(r["ts"]), float(r["rate"])) for r in csv.DictReader(fh)]


async def load_funding(client: BinanceClient, symbol: str, days: int, cache_dir: Path = Path("data/klines"),
                       refresh: bool = False, now_ms: int | None = None) -> list[tuple[int, float]]:
    """The last `days` of funding events, cached and topped up like the candles."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    path, want_from = funding_path(cache_dir, symbol), now_ms - days * DAY_MS
    cached = [] if refresh else load_funding_csv(path)
    merged = dict(cached)
    if cached:
        if cached[0][0] > want_from + 8 * 3_600_000:
            merged.update(await fetch_funding(client, symbol, want_from, cached[0][0] - 1))
        merged.update(await fetch_funding(client, symbol, cached[-1][0] + 1, now_ms))
    else:
        merged.update(await fetch_funding(client, symbol, want_from, now_ms))
    events = sorted(merged.items())
    save_funding(path, events)
    return [e for e in events if e[0] >= want_from]
