"""Market recorder: saves what the bot sees so strategies and slippage can be backtested later.

Format: gzip-compressed JSON lines, one file per UTC day (`quotes-YYYY-MM-DD.jsonl.gz`, appended across
restarts; concatenated gzip members are valid). Every line is one of:

  {"v":1,"type":"meta","t":..,"markets":{SYM:{"spot","base","mult","fi","cap","floor"}},"fees":{..}}
  {"t":..,"q":[[SYM, perp_bid, perp_bid_qty, perp_ask, perp_ask_qty, spot_bid, spot_bid_qty, spot_ask,
                 spot_ask_qty, mark, index, funding_rate, next_funding_ms], ...]}     only FRESH markets
  {"t":..,"d":SYM,"why":"sample|signal","pb":[[p,q],..],"pa":..,"sb":..,"sa":..}      order-book snapshot
  {"t":..,"e":"entry|exit", ...}                                                         the bot's own trades

The recorder can never hurt trading: any I/O error disables it (logged once) and the bot carries on.
"""
import asyncio
import gzip
import json
import logging
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

from .config import RecorderCfg
from .models import Market
from .sizing import Books

log = logging.getLogger("recorder")
FORMAT_VERSION = 1
FLUSH_EVERY_S = 5.0


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


GZ_MAGIC = b"\x1f\x8b\x08"


def _iter_lines(p: Path):
    """The lines of one recording, gzip member by member (each bot start appends a member). A member a crash cut off
    (no gzip end; the next start's member follows it directly) is read up to the break, then reading resumes at the
    next member header, so one killed bot never hides the rest of the day."""
    data = p.read_bytes()
    pos, broken = 0, 0
    while (start := data.find(GZ_MAGIC, pos)) >= 0:
        # Feed the member only up to the next place another member could start. A decompress call that runs into a
        # foreign member raises and discards everything it decoded, so it must never be handed bytes past a break.
        d, tail, seg, end, cut = zlib.decompressobj(16 + zlib.MAX_WBITS), b"", start, start, False
        while True:
            nxt = data.find(GZ_MAGIC, seg + len(GZ_MAGIC))
            end = nxt if nxt >= 0 else len(data)
            try:
                out = d.decompress(data[seg:end])
            except zlib.error:                      # `seg` is a real new member: this one was cut off just before it
                cut = True
                break
            lines = (tail + out).split(b"\n")
            tail = lines.pop()
            yield from (ln for ln in lines if ln)
            if d.eof or end >= len(data):
                break
            seg = end                               # that header was inside this member's data: keep feeding
        if d.eof:                                   # a complete member: carry on right after it
            if tail:
                yield tail
            pos = end - len(d.unused_data)
        elif cut:                                   # cut off by a stop mid-write: drop its half line, go on at `seg`
            broken += 1
            pos = seg
        else:                                       # the last member is still being written (or ends cut off)
            pos = len(data)
    if broken:
        log.info("%s: %d member(s) cut off by a stop mid-write; read each up to its break", p.name, broken)


def read_records(paths):
    """Yield every record from the given recording files in order, surviving members cut off by a crash mid-write."""
    for p in sorted(Path(x) for x in paths):
        try:
            for line in _iter_lines(p):
                try:
                    yield json.loads(line)
                except ValueError:
                    continue  # a line half-written at a crash
        except OSError as e:
            log.warning("%s could not be read (%s: %s)", p.name, type(e).__name__, e)


def books_to_record(books: Books) -> dict:
    return {"pb": [list(x) for x in books.perp_bids], "pa": [list(x) for x in books.perp_asks],
            "sb": [list(x) for x in books.spot_bids], "sa": [list(x) for x in books.spot_asks]}


def books_from_record(rec: dict) -> Books:
    return Books(spot_bids=[tuple(x) for x in rec["sb"]], spot_asks=[tuple(x) for x in rec["sa"]],
                 perp_bids=[tuple(x) for x in rec["pb"]], perp_asks=[tuple(x) for x in rec["pa"]])


class Recorder:
    def __init__(self, cfg: RecorderCfg, max_quote_age_s: float, root: Path | None = None, fees=None):
        self.cfg = cfg
        base = Path(cfg.dir)
        self.dir = base if base.is_absolute() or root is None else root / base
        self.max_age = max_quote_age_s
        self.fees = fees
        self.enabled = cfg.enabled
        self.lines = 0
        self._fh = None
        self._day = ""
        self._last_flush = 0.0
        self._markets: dict[str, Market] = {}

    # ---- file handling -------------------------------------------------------------------------
    def _path(self, day: str) -> Path:
        return self.dir / f"quotes-{day}.jsonl.gz"

    def _open(self, now: float) -> None:
        day = _day(now)
        if self._fh is not None and day == self._day:
            return
        self._close_file()
        self.dir.mkdir(parents=True, exist_ok=True)
        self._fh = gzip.open(self._path(day), "at", compresslevel=5, encoding="utf-8")
        self._day = day
        self._write_meta(now)
        self._enforce_cap()

    def _close_file(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None

    def _write(self, obj: dict, now: float) -> None:
        self._fh.write(json.dumps(obj, separators=(",", ":")) + "\n")
        self.lines += 1
        if now - self._last_flush >= FLUSH_EVERY_S:
            self._fh.flush()
            self._last_flush = now

    def _write_meta(self, now: float) -> None:
        markets = {
            sym: {"spot": m.spot_symbol, "base": m.base_asset, "mult": m.mult, "fi": m.funding_interval_h,
                  "cap": m.funding_cap, "floor": m.funding_floor}
            for sym, m in self._markets.items()
        }
        fees = {"futures_taker_bps": self.fees.futures_taker_bps, "spot_taker_bps": self.fees.spot_taker_bps,
                "slippage_bps": self.fees.slippage_bps} if self.fees is not None else {}
        self._write({"v": FORMAT_VERSION, "type": "meta", "t": now, "markets": markets, "fees": fees}, now)

    def _enforce_cap(self) -> None:
        files = sorted(self.dir.glob("quotes-*.jsonl.gz"))
        total = sum(f.stat().st_size for f in files)
        for f in files[:-1]:  # never delete today's file
            if total <= self.cfg.max_mb * 1_000_000:
                break
            total -= f.stat().st_size
            f.unlink(missing_ok=True)
            log.warning("recordings exceed %d MB: deleted %s", self.cfg.max_mb, f.name)

    def _fail(self, what: str, e: Exception) -> None:
        log.error("recorder %s failed (%s: %s); recording disabled for this run", what, type(e).__name__, e)
        self.enabled = False
        self._close_file()

    # ---- public hooks (never raise) ------------------------------------------------------------
    def tick(self, now: float, markets: dict[str, Market]) -> None:
        if not self.enabled:
            return
        try:
            self._markets = markets
            self._open(now)
            rows = []
            for sym, m in markets.items():
                sp, pp = m.spot, m.perp
                if sp is None or pp is None or now - min(sp.ts, pp.ts) > self.max_age:
                    continue
                rows.append([sym, pp.bid, pp.bid_qty, pp.ask, pp.ask_qty, sp.bid, sp.bid_qty, sp.ask, sp.ask_qty,
                             m.mark, m.index, m.funding_rate, m.next_funding_ms])
            if rows:
                self._write({"t": now, "q": rows}, now)
        except Exception as e:  # noqa: BLE001 - recording must never break trading
            self._fail("tick", e)

    def depth(self, symbol: str, books: Books, now: float, why: str = "sample") -> None:
        if not self.enabled:
            return
        try:
            self._open(now)
            self._write({"t": now, "d": symbol, "why": why, **books_to_record(books)}, now)
        except Exception as e:  # noqa: BLE001
            self._fail("depth", e)

    def event(self, kind: str, now: float, **data) -> None:
        if not self.enabled:
            return
        try:
            self._open(now)
            self._write({"t": now, "e": kind, **data}, now)
        except Exception as e:  # noqa: BLE001
            self._fail("event", e)

    def close(self) -> None:
        self._close_file()


class DepthSampler:
    """Samples one order book per interval, round-robin over every market (spreads REST weight evenly)."""

    def __init__(self, recorder: Recorder, depth, board, every_s: float = 1.0):
        self.rec, self.depth, self.board, self.every_s = recorder, depth, board, max(0.2, every_s)
        self.samples = 0

    async def run(self) -> None:
        i = 0
        while True:
            await asyncio.sleep(self.every_s)
            if not self.rec.enabled:
                continue
            syms = list(self.board.markets)
            if not syms:
                continue
            m = self.board.markets[syms[i % len(syms)]]
            i += 1
            try:
                books = await self.depth.books(m)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("depth sample failed: %s: %s", type(e).__name__, e)
                continue
            if books is not None:
                self.rec.depth(m.symbol, books, time.time(), "sample")
                self.samples += 1
