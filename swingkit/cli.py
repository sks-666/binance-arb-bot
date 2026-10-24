"""Command line for swingkit:  python swing.py download | backtest | scan | journal   (see --help of each).

Everything here is paper / research. Nothing places an order."""
import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from arbbot.binance_client import BinanceClient

from .backtest import CostCfg, format_report, run_backtest, summarize, trades_to_csv
from .data import (DAY_MS, INTERVAL_MS, cache_path, funding_path, load_csv, load_funding, load_funding_csv,
                   load_history, to_series)
from .journal import Journal, format_stats, format_trades
from .models import Series
from .paper import SwingRunner, add_paper_args, paper_cfg_from, resolve_spec
from .risk import HARD_MAX_RISK_PCT, RiskCfg
from .strategies import REGISTRY, make

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT"
SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")


def _symbols(text: str) -> list[str]:
    out = [s.strip().upper() for s in text.split(",") if s.strip()]
    bad = [s for s in out if not SYMBOL_RE.match(s)]
    if bad or not out:
        raise SystemExit(f"Not valid symbols: {', '.join(bad) or text!r}. Example: BTCUSDT,ETHUSDT")
    return out


def _params(pairs: list[str] | None) -> dict[str, dict]:
    """--set trend.slow=100 --set smc.rr=3  ->  {'trend': {'slow': 100}, 'smc': {'rr': 3}}"""
    out: dict[str, dict] = {}
    for item in pairs or []:
        m = re.match(r"^(\w+)\.(\w+)=(.+)$", item)
        if not m or m.group(1) not in REGISTRY:
            raise SystemExit(f"--set expects strategy.parameter=value with strategy one of "
                             f"{', '.join(REGISTRY)}, got {item!r}")
        try:
            value = json.loads(m.group(3))
        except ValueError:
            raise SystemExit(f"--set {item}: the value must be a number")
        out.setdefault(m.group(1), {})[m.group(2)] = value
    return out


def _add_data_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--symbols", default=DEFAULT_SYMBOLS, help=f"comma separated (default {DEFAULT_SYMBOLS})")
    p.add_argument("--interval", default="4h", choices=list(INTERVAL_MS), help="candle size (default 4h)")
    p.add_argument("--days", type=int, default=730, help="how much history (default 730 = two years)")
    p.add_argument("--market", default="futures", choices=["futures", "spot"])
    p.add_argument("--cache-dir", default="data/klines")
    p.add_argument("--refresh", action="store_true", help="download everything again instead of topping up the cache")
    p.add_argument("--offline", action="store_true", help="use the cache only; never touch the network")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="swing", description="Swing trading, trend following and Smart Money Concepts: "
                                                         "backtest them, scan for setups, keep a journal. Paper only.")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download", help="download and cache candle history")
    _add_data_args(d)

    b = sub.add_parser("backtest", help="replay a strategy over history, with fees, slippage and funding")
    _add_data_args(b)
    b.add_argument("--strategy", default="all", choices=["all", *REGISTRY])
    b.add_argument("--capital", type=float, default=10_000.0)
    b.add_argument("--risk-pct", type=float, default=1.0, help=f"%% of equity risked per trade (max {HARD_MAX_RISK_PCT:g})")
    b.add_argument("--max-open", type=int, default=5)
    b.add_argument("--max-leverage", type=float, default=3.0)
    b.add_argument("--fee-bps", type=float, default=5.0)
    b.add_argument("--slippage-bps", type=float, default=3.0)
    b.add_argument("--funding-bps", type=float, default=1.0,
                   help="flat funding per 8 hours held, both sides; only used with --flat-funding or when no real "
                        "funding history is available")
    b.add_argument("--flat-funding", action="store_true", help="ignore the real funding history")
    b.add_argument("--no-short", action="store_true", help="long only")
    b.add_argument("--set", action="append", dest="params", metavar="STRATEGY.PARAM=VALUE")
    b.add_argument("--csv", help="write every trade to this CSV file (one file per strategy: name_strategy.csv)")
    b.add_argument("--journal", action="store_true", help="also record the trades in the journal")
    b.add_argument("--journal-db", default="data/swing-journal.db")

    s = sub.add_parser("scan", help="what do the strategies say about the latest closed candle?")
    _add_data_args(s)
    s.add_argument("--set", action="append", dest="params", metavar="STRATEGY.PARAM=VALUE")
    s.add_argument("--no-short", action="store_true")

    r = sub.add_parser("run", help="paper-trade the strategies live: journal + Telegram alerts (no commands; use "
                                   "`run.py --swing` for /setups /journal /swing)")
    add_paper_args(r)
    r.add_argument("--once", action="store_true", help="refresh once, print the status and the setups, and exit")

    j = sub.add_parser("journal", help="read, annotate and export the trading journal")
    j.add_argument("--db", default="data/swing-journal.db")
    j.add_argument("--last", type=int, default=20)
    j.add_argument("--mode", choices=["backtest", "paper"])
    j.add_argument("--strategy", choices=list(REGISTRY))
    j.add_argument("--status", choices=["open", "closed"])
    j.add_argument("--summary", action="store_true", help="only the statistics")
    j.add_argument("--export", metavar="FILE.csv")
    j.add_argument("--note", nargs=2, metavar=("ID", "TEXT"), help="add a note to a trade")
    return p


async def _load_all(args) -> dict[str, Series]:
    cache_dir = Path(args.cache_dir)
    data: dict[str, Series] = {}
    if args.offline:
        for sym in _symbols(args.symbols):
            candles = load_csv(cache_path(cache_dir, args.market, sym, args.interval))
            if not candles:
                raise SystemExit(f"--offline: no cached {sym} {args.interval} candles in {cache_dir}. Run download first.")
            cutoff = candles[-1].ts - args.days * DAY_MS
            data[sym] = to_series([k for k in candles if k.ts >= cutoff])
        return data
    async with aiohttp.ClientSession() as session:
        client = BinanceClient(session, "mainnet", timeout_s=20.0)
        for sym in _symbols(args.symbols):
            try:
                candles = await load_history(client, sym, args.interval, args.days, args.market, cache_dir, args.refresh)
            except Exception as e:  # a bad symbol, a blocked network: say which one, do not dump a traceback
                raise SystemExit(f"Could not download {sym}: {type(e).__name__}: {e}")
            if len(candles) < 250:
                print(f"  note: {sym} has only {len(candles)} candles; indicators need ~200 to warm up", file=sys.stderr)
            data[sym] = to_series(candles)
            print(f"  {sym}: {len(candles)} candles", file=sys.stderr)
    return data


async def _load_funding(args, symbols: list[str]) -> dict[str, list[tuple[int, float]]]:
    """Real funding events per symbol (futures only). A symbol whose history is missing falls back to the flat charge."""
    cache_dir, out = Path(args.cache_dir), {}
    if args.offline:
        for sym in symbols:
            events = load_funding_csv(funding_path(cache_dir, sym))
            if events:
                out[sym] = events
        return out
    async with aiohttp.ClientSession() as session:
        client = BinanceClient(session, "mainnet", timeout_s=20.0)
        for sym in symbols:
            try:
                out[sym] = await load_funding(client, sym, args.days + 2, cache_dir, args.refresh)
            except Exception as e:
                print(f"  note: no funding history for {sym} ({type(e).__name__}); using the flat charge", file=sys.stderr)
    return out


def _run_backtests(args) -> int:
    params = _params(args.params)
    try:
        risk = RiskCfg(risk_pct=args.risk_pct, max_open=args.max_open, max_leverage=args.max_leverage)
    except ValueError as e:
        print(f"Refused: {e}", file=sys.stderr)
        return 2
    spot = args.market == "spot"          # spot has no funding and cannot be shorted
    costs = CostCfg(args.fee_bps, args.slippage_bps, 0.0 if spot else args.funding_bps)
    args.no_short = args.no_short or spot
    names = list(REGISTRY) if args.strategy == "all" else [args.strategy]
    for name in names:  # reject a bad --set before downloading anything
        try:
            make(name, **params.get(name, {}))
        except ValueError as e:
            print(f"Refused: {e}", file=sys.stderr)
            return 2
    data = asyncio.run(_load_all(args))
    funding = None if (spot or args.flat_funding) else asyncio.run(_load_funding(args, list(data)))
    if funding is not None and len(funding) < len(data):
        print(f"  note: real funding history for {len(funding)} of {len(data)} coins; the rest use the flat "
              f"{args.funding_bps:g} bps per 8h", file=sys.stderr)
    journal = Journal(args.journal_db) if args.journal else None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    summaries = []
    for name in names:
        factory = lambda n=name: make(n, allow_short=not args.no_short, **params.get(n, {}))  # noqa: E731
        res = run_backtest(data, factory, risk, costs, args.capital, INTERVAL_MS[args.interval], funding)
        m = summarize(res, data)
        summaries.append(m)
        print(format_report(m))
        if m["trades"] < 30:
            print(f"  caution        {m['trades']} trades is too few to say anything: a strategy needs a few dozen "
                  f"trades, and ideally years of data, before its numbers mean much")
        print()
        if args.csv:
            out = Path(args.csv)
            target = out.with_name(f"{out.stem}_{name}{out.suffix or '.csv'}")
            trades_to_csv(res.trades, str(target))
            print(f"  wrote {len(res.trades)} trades to {target}\n")
        if journal:
            for t in res.trades:
                journal.record(t, mode="backtest", run_id=f"{name}-{args.interval}-{stamp}", timeframe=args.interval)
    if len(summaries) > 1:
        print("=== side by side ===")
        print(f"{'strategy':10} {'trades':>6} {'win%':>5} {'exp.R':>6} {'PF':>5} {'return%':>8} {'maxDD%':>7}")
        for m in summaries:
            pf = "inf" if m["profit_factor"] == float("inf") else f"{m['profit_factor']:.2f}"
            print(f"{m['strategy']:10} {m['trades']:>6} {m['win_rate']:>5.0f} {m['avg_r']:>+6.2f} {pf:>5} "
                  f"{m['return_pct']:>+8.1f} {m['max_drawdown_pct']:>7.1f}")
        if "buy_hold_pct" in summaries[0]:
            print(f"(holding the coins instead: {summaries[0]['buy_hold_pct']:+.1f}% on average)")
    if journal:
        print(f"\nTrades recorded in the journal ({args.journal_db}); read them with: python swing.py journal")
    return 0


def _scan(args) -> int:
    params = _params(args.params)
    data = asyncio.run(_load_all(args))
    found = 0
    for sym, series in data.items():
        last = len(series) - 1
        for name in REGISTRY:
            strat = make(name, allow_short=not args.no_short, **params.get(name, {}))
            strat.prepare(series)
            st = strat.entry(last)
            if st:
                found += 1
                target = "-" if st.target is None else f"{st.target:.6g}"
                closed_at = datetime.fromtimestamp((series.ts[last] + INTERVAL_MS[args.interval]) / 1000, timezone.utc)
                print(f"{sym} {st.side.upper()} [{name}] on the candle that closed {closed_at:%Y-%m-%d %H:%M} UTC\n"
                      f"   enter near {st.ref_price:.6g}   stop {st.stop:.6g}   target {target}\n   why: {st.reason}")
    if not found:
        print(f"No setups on the latest closed {args.interval} candle of {', '.join(data)}. That is the usual answer: "
              f"these strategies wait.")
    return 0


def _journal(args) -> int:
    j = Journal(args.db)
    if args.note:
        if not args.note[0].isdigit():
            print("The trade id must be a number (see `journal` for the ids).", file=sys.stderr)
            return 2
        ok = j.note(int(args.note[0]), args.note[1])
        print("Note added." if ok else f"No trade #{args.note[0]}.")
        return 0 if ok else 1
    filters = dict(mode=args.mode, strategy=args.strategy)
    if args.export:
        print(f"Exported {j.export_csv(args.export, status=args.status, **filters)} trades to {args.export}")
        return 0
    print(format_stats(j.stats(**filters)))
    if not args.summary:
        print()
        print(format_trades(j.trades(status=args.status, limit=args.last, **filters)))
    return 0


async def _run_paper(args) -> int:
    from arbbot.alerts import Notifier
    from arbbot.config import AlertsCfg
    from arbbot.main import ROOT, load_dotenv
    load_dotenv(ROOT / ".env")
    journal = Journal(args.journal_db)
    async with aiohttp.ClientSession() as session:
        client = BinanceClient(session, "mainnet", timeout_s=20.0)
        try:
            symbols, auto = await resolve_spec(args.symbols, client)
            cfg = paper_cfg_from(args, symbols=symbols, auto=auto)
            runner = SwingRunner(cfg, client, journal)
        except (ValueError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Refused: {e}", file=sys.stderr)
            return 2
        notifier = Notifier(AlertsCfg(), os.environ.get("TELEGRAM_BOT_TOKEN", ""), os.environ.get("TELEGRAM_CHAT_ID", ""),
                            "SWING", session)

        def notify(text: str) -> None:
            print(text, flush=True)
            notifier.send(text)

        runner.notify = notify
        if args.once:
            await runner.refresh()
            print(runner.status_text() + "\n\n" + runner.setups_text())
            return 0
        print(f"Paper-trading {'+'.join(cfg.strategies)} on {cfg.interval} candles of {runner._coins()}; "
              f"Telegram alerts {'ON' if notifier.enabled else 'off'}. Ctrl-C stops it.", flush=True)
        task = asyncio.create_task(notifier.run())
        try:
            await runner.run()
        finally:
            task.cancel()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "run":
        try:
            return asyncio.run(_run_paper(args))
        except KeyboardInterrupt:
            print("Stopped.")
            return 0
    if args.cmd == "download":
        data = asyncio.run(_load_all(args))
        print(f"Cached {len(data)} symbol(s) in {args.cache_dir}")
        return 0
    if args.cmd == "backtest":
        return _run_backtests(args)
    if args.cmd == "scan":
        return _scan(args)
    return _journal(args)
