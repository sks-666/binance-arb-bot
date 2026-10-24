"""Candle-by-candle backtester for the swingkit strategies.

Honesty rules (each one is tested):
  * No look-ahead: a strategy decides at the CLOSE of candle i from candles <= i; the order fills at the OPEN of i+1.
  * Intrabar: stops and targets are checked against the candle's high/low; when both could have been hit inside one
    candle the STOP is assumed to hit first. A stop that gaps past its level fills at the open, not at the stop.
  * Costs on every fill: taker fee + slippage; funding is charged on notional while a position is held (both sides, as
    a conservative flat rate).
  * Sizing: every trade risks `risk_pct` of current equity (max 2%), subject to leverage, heat and drawdown limits.
  * Positions still open at the end of the data are closed at the last close and marked as such.
"""
import csv
import math
from bisect import bisect_right
from collections import Counter
from dataclasses import asdict, dataclass, field
from statistics import mean
from typing import Callable

from .models import Series, Setup
from .risk import DrawdownBreaker, RiskCfg, can_open, size_position
from .strategies import Strategy

DAY_MS = 86_400_000
YEAR_MS = 365.25 * DAY_MS


@dataclass
class CostCfg:
    fee_bps: float = 5.0           # taker fee per fill (USD-M futures, VIP 0)
    slippage_bps: float = 3.0      # adverse move per market-order fill
    funding_bps_8h: float = 1.0    # charged on notional per 8 h held; conservative: paid by longs AND shorts


@dataclass
class Trade:
    strategy: str
    symbol: str
    side: str
    entry_ts: int
    exit_ts: int
    entry_px: float
    exit_px: float
    stop: float                    # the INITIAL stop (what the risk was sized on)
    target: float | None
    qty: float
    risk_usd: float
    pnl_usd: float                 # after fees, slippage and funding
    fees_usd: float
    funding_usd: float
    r_multiple: float              # pnl / initial risk
    bars_held: int
    reason_in: str
    reason_out: str


@dataclass
class _Pos:
    symbol: str
    side: str
    sgn: int
    entry_i: int
    entry_ts: int
    entry_px: float
    qty: float
    stop: float
    stop0: float
    target: float | None
    risk_usd: float
    entry_fee: float
    reason_in: str
    funding: float = 0.0
    exit_next: str = ""            # the strategy asked to exit at the next open: why


@dataclass
class Result:
    strategy: str
    capital: float
    trades: list[Trade] = field(default_factory=list)
    equity: list[tuple[int, float]] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    first_ts: int = 0
    last_ts: int = 0
    open_positions: list[dict] = field(default_factory=list)   # live mode only: still open, not force-closed
    pending: dict = field(default_factory=dict)                # live mode only: decided at the last close, not yet filled


def run_backtest(data: dict[str, Series], make_strategy: Callable[[], Strategy], risk: RiskCfg | None = None,
                 costs: CostCfg | None = None, capital: float = 10_000.0, bar_ms: int = 14_400_000,
                 funding: dict[str, list[tuple[int, float]]] | None = None, close_at_end: bool = True,
                 start_ts: int = 0, partial_last: bool | set[str] = False) -> Result:
    """`funding`: real funding events per symbol, [(time ms, rate)]; a positive rate means longs pay shorts, and a short
    RECEIVES it. A symbol without events falls back to the flat `costs.funding_bps_8h` charge.

    Live mode (paper trading replays the real candles with exactly these rules):
      close_at_end=False  positions still open at the end are reported in `open_positions`, not closed, and a setup decided
                          on the very last candle is reported in `pending`;
      start_ts            no entries before this time (older candles only warm the indicators up);
      partial_last        the final candle is still FORMING (True for every coin, or the set of coins it applies to): only
                          its open is known, so it can fill orders and trigger a stop that gapped, nothing more (no
                          decision is ever taken on an unfinished candle)."""
    risk, costs = risk or RiskCfg(), costs or CostCfg()
    fund_ts = {sym: [t for t, _ in ev] for sym, ev in (funding or {}).items()}
    fee, slip = costs.fee_bps / 1e4, costs.slippage_bps / 1e4
    fund_per_bar = costs.funding_bps_8h / 1e4 * bar_ms / (8 * 3_600_000)
    strategies = {sym: make_strategy() for sym in data}
    for sym, s in data.items():
        strategies[sym].prepare(s)
    where = {sym: {ts: i for i, ts in enumerate(s.ts)} for sym, s in data.items()}
    timeline = sorted({ts for s in data.values() for ts in s.ts})
    name = next(iter(strategies.values())).name if strategies else "?"
    res = Result(strategy=name, capital=capital, first_ts=timeline[0] if timeline else 0,
                 last_ts=timeline[-1] if timeline else 0)
    cash = capital
    breaker = DrawdownBreaker(risk.drawdown_halt_pct, int(risk.drawdown_pause_days * DAY_MS))
    breaker.update(capital)
    pos: dict[str, _Pos] = {}
    pending: dict[str, Setup] = {}
    last_close: dict[str, float] = {}

    def unrealised(p: _Pos) -> float:
        return p.sgn * (last_close.get(p.symbol, p.entry_px) - p.entry_px) * p.qty

    def equity_now() -> float:
        return cash + sum(unrealised(p) for p in pos.values())

    def close(p: _Pos, px: float, ts: int, i: int, why: str) -> None:
        nonlocal cash
        gross = p.sgn * (px - p.entry_px) * p.qty
        exit_fee = fee * px * p.qty
        cash += gross - exit_fee
        pnl = gross - exit_fee - p.entry_fee - p.funding
        res.trades.append(Trade(
            strategy=name, symbol=p.symbol, side=p.side, entry_ts=p.entry_ts, exit_ts=ts, entry_px=p.entry_px,
            exit_px=px, stop=p.stop0, target=p.target, qty=p.qty, risk_usd=p.risk_usd, pnl_usd=pnl,
            fees_usd=p.entry_fee + exit_fee, funding_usd=p.funding, r_multiple=pnl / p.risk_usd if p.risk_usd else 0.0,
            bars_held=i - p.entry_i, reason_in=p.reason_in, reason_out=why))
        del pos[p.symbol]

    for ts in timeline:
        for sym, s in data.items():
            i = where[sym].get(ts)
            if i is None:
                continue
            o, h, l, c = s.o[i], s.h[i], s.l[i], s.c[i]
            partial = i == len(s) - 1 and (sym in partial_last if isinstance(partial_last, (set, frozenset))
                                           else bool(partial_last))
            if partial:
                h = l = c = o                      # all we know of a forming candle
            strat = strategies[sym]

            # 1. an order decided at the previous close fills at this open
            st = pending.pop(sym, None)
            if st is not None and sym not in pos:
                sgn = 1 if st.side == "long" else -1
                fill = o * (1 + sgn * slip)
                eq = equity_now()
                if (st.stop - fill) * sgn >= 0:
                    res.skipped["stop already breached at the open"] += 1
                else:
                    qty, why = 0.0, breaker.blocked(ts, eq)
                    if not why:
                        qty, why = size_position(eq, fill, st.stop, risk)
                    if qty:
                        risk_usd = qty * abs(fill - st.stop)
                        open_risks = [max(0.0, (last_close.get(q.symbol, q.entry_px) - q.stop) * q.sgn * q.qty)
                                      for q in pos.values()]
                        why = can_open(eq, open_risks, risk_usd, risk)
                    if not qty or why:
                        res.skipped[why] += 1
                    else:
                        entry_fee = fee * fill * qty
                        cash -= entry_fee
                        pos[sym] = _Pos(sym, st.side, sgn, i, ts, fill, qty, st.stop, st.stop, st.target, risk_usd,
                                        entry_fee, st.reason)

            p = pos.get(sym)
            # 2. the strategy asked for an exit at this open
            if p is not None and p.exit_next:
                close(p, o * (1 - p.sgn * slip), ts, i, p.exit_next)
                p = None
            # 3. stop / target inside this candle (the stop wins a tie)
            if p is not None:
                if p.sgn == 1:
                    if l <= p.stop:
                        close(p, min(o, p.stop) * (1 - slip), ts, i, "stop")
                    elif p.target is not None and h >= p.target:
                        close(p, max(o, p.target), ts, i, "target")
                else:
                    if h >= p.stop:
                        close(p, max(o, p.stop) * (1 + slip), ts, i, "stop")
                    elif p.target is not None and l <= p.target:
                        close(p, min(o, p.target), ts, i, "target")
                p = pos.get(sym)
            last_close[sym] = c
            if partial:
                continue                           # nothing is decided on a candle that has not closed
            # 4. at the close: funding, trailing stop, strategy exit
            if p is not None:
                if sym in fund_ts:   # real funding paid at the instants this position was held during the candle
                    events = funding[sym][bisect_right(fund_ts[sym], ts):bisect_right(fund_ts[sym], ts + bar_ms)]
                    charge = p.sgn * p.qty * c * sum(rate for _, rate in events)
                else:
                    charge = p.qty * c * fund_per_bar
                cash -= charge
                p.funding += charge
                new_stop, exit_now, why = strat.manage(p.side, i, p.entry_i, p.stop)
                p.stop = max(p.stop, new_stop) if p.sgn == 1 else min(p.stop, new_stop)  # stops only tighten
                if exit_now:
                    p.exit_next = why
            # 5. a new idea, to be filled at the next open
            if sym not in pos and ts >= start_ts and (i + 1 < len(s) or not close_at_end):
                setup = strat.entry(i)
                if setup is not None:
                    pending[sym] = setup

        eq = equity_now()
        breaker.update(eq)
        res.equity.append((ts, eq))

    if not close_at_end:
        res.pending = dict(pending)
        for p in pos.values():
            mark = last_close[p.symbol]
            net = p.sgn * (mark - p.entry_px) * p.qty - p.entry_fee - p.funding
            res.open_positions.append(dict(
                strategy=name, symbol=p.symbol, side=p.side, entry_ts=p.entry_ts, entry_px=p.entry_px, stop=p.stop,
                stop0=p.stop0, target=p.target, qty=p.qty, risk_usd=p.risk_usd, reason_in=p.reason_in, mark=mark,
                unrealised_usd=net, unrealised_r=net / p.risk_usd if p.risk_usd else 0.0))
        return res
    for p in list(pos.values()):
        i = where[p.symbol].get(timeline[-1], len(data[p.symbol]) - 1)
        close(p, last_close[p.symbol] * (1 - p.sgn * slip), timeline[-1], i, "end of data")
    if res.equity:
        res.equity[-1] = (timeline[-1], cash)
    return res


def summarize(res: Result, data: dict[str, Series] | None = None) -> dict:
    t, curve = res.trades, [e for _, e in res.equity]
    wins = [x for x in t if x.pnl_usd > 0]
    gross_win = sum(x.pnl_usd for x in wins)
    gross_loss = -sum(x.pnl_usd for x in t if x.pnl_usd <= 0)
    final = curve[-1] if curve else res.capital
    peak, max_dd = res.capital, 0.0
    for e in curve:
        peak = max(peak, e)
        max_dd = max(max_dd, (peak - e) / peak * 100 if peak else 0.0)
    years = (res.last_ts - res.first_ts) / YEAR_MS if res.last_ts > res.first_ts else 0.0
    out = dict(
        strategy=res.strategy, trades=len(t), win_rate=len(wins) / len(t) * 100 if t else 0.0,
        avg_r=mean(x.r_multiple for x in t) if t else 0.0,
        avg_win_r=mean(x.r_multiple for x in wins) if wins else 0.0,
        avg_loss_r=mean(x.r_multiple for x in t if x.pnl_usd <= 0) if len(wins) < len(t) else 0.0,
        profit_factor=(gross_win / gross_loss) if gross_loss else (math.inf if gross_win else 0.0),
        net_pnl=final - res.capital, return_pct=(final / res.capital - 1) * 100, max_drawdown_pct=max_dd,
        cagr_pct=((final / res.capital) ** (1 / years) - 1) * 100 if years > 0 and final > 0 else 0.0,
        avg_bars=mean(x.bars_held for x in t) if t else 0.0, fees=sum(x.fees_usd for x in t),
        funding=sum(x.funding_usd for x in t), years=years, skipped=dict(res.skipped),
        by_symbol={s: (sum(1 for x in t if x.symbol == s), sum(x.pnl_usd for x in t if x.symbol == s))
                   for s in sorted({x.symbol for x in t})},
        exits=dict(Counter(x.reason_out for x in t)),
    )
    if data:
        holds = [(s.c[-1] / s.c[0] - 1) * 100 for s in data.values() if len(s) > 1 and s.c[0] > 0]
        out["buy_hold_pct"] = mean(holds) if holds else 0.0
    return out


def format_report(m: dict) -> str:
    pf = "inf" if m["profit_factor"] == math.inf else f"{m['profit_factor']:.2f}"
    lines = [
        f"=== {m['strategy']}: {m['trades']} trades over {m['years']:.1f} years ===",
        f"  net P&L        {m['net_pnl']:+,.2f} USD   ({m['return_pct']:+.1f}%, about {m['cagr_pct']:+.1f}% a year)",
        f"  win rate       {m['win_rate']:.0f}%     avg win {m['avg_win_r']:+.2f}R   avg loss {m['avg_loss_r']:+.2f}R"
        f"   expectancy {m['avg_r']:+.2f}R per trade",
        f"  profit factor  {pf}     max drawdown {m['max_drawdown_pct']:.1f}%     avg hold {m['avg_bars']:.0f} candles",
        f"  costs paid     fees {m['fees']:,.2f} USD, funding {m['funding']:+,.2f} USD (negative = received)",
    ]
    if "buy_hold_pct" in m:
        lines.append(f"  for context    simply holding these coins would have returned {m['buy_hold_pct']:+.1f}% "
                     f"(average per coin)")
    if m["exits"]:
        lines.append("  exits          " + ", ".join(f"{k} {v}" for k, v in sorted(m["exits"].items(), key=lambda kv: -kv[1])))
    if m["by_symbol"]:
        lines.append("  by coin        " + ", ".join(f"{s} {n} trades {pnl:+,.0f}" for s, (n, pnl) in m["by_symbol"].items()))
    if m["skipped"]:
        lines.append("  skipped        " + ", ".join(f"{k} x{v}" for k, v in m["skipped"].items()))
    return "\n".join(lines)


def trades_to_csv(trades: list[Trade], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(asdict(trades[0]).keys()) if trades else ["strategy"])
        w.writeheader()
        for t in trades:
            w.writerow(asdict(t))
