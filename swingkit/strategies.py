"""Three retail strategies for higher timeframes. Each is written ONCE, for longs, and run on the mirrored (negated)
series for shorts, so the long and short logic cannot drift apart.

A strategy decides at the CLOSE of candle i (it only ever reads candles <= i) and the backtester fills at the NEXT open.

  trend  - trend following: Donchian breakout in the direction of the EMA trend, ATR trailing stop, channel exit.
  swing  - swing trading: a pullback inside a trend that turns back up (RSI dip and turn at the EMA), 2R target.
  smc    - Smart Money Concepts, as mechanical rules: liquidity sweep of a swing low, break of structure, then entry
           on the retest of the order block (the last opposite candle before the break).

SMC is a discretionary craft; these are one explicit, testable reading of it, not "the" method.
"""
import math

from .indicators import atr, ema, highest_prev, lowest_prev, pivot_lows, rsi
from .models import Series, Setup

LONG_W = dict(up="up", down="down", above="above", below="below", high="high", low="low", gt=">", rsi=lambda v: v)
SHORT_W = dict(up="down", down="up", above="below", below="above", high="low", low="high", gt="<", rsi=lambda v: 100 - v)


class Strategy:
    name = "base"
    defaults: dict = {}

    def __init__(self, allow_short: bool = True, **params):
        unknown = set(params) - set(self.defaults)
        if unknown:
            raise ValueError(f"{self.name}: unknown parameter(s) {', '.join(sorted(unknown))}; "
                             f"valid: {', '.join(sorted(self.defaults))}")
        self.p = {**self.defaults, **params}
        self.allow_short = allow_short
        self._ctx: dict = {}

    # ---- written once, for longs --------------------------------------------------------------
    def indicators(self, s: Series) -> dict:
        return {}

    def long_signals(self, s: Series, ind: dict, w: dict) -> dict[int, Setup]:
        return {}

    def long_manage(self, s: Series, ind: dict, i: int, entry_i: int, stop: float) -> tuple[float, bool, str]:
        """At the close of candle i, for a long filled at entry_i: (new stop, exit at the next open?, why)."""
        return stop, False, ""

    # ---- both directions -----------------------------------------------------------------------
    def prepare(self, s: Series) -> None:
        self._ctx = {}
        for side, series, sgn, w in (("long", s, 1, LONG_W), ("short", s.mirrored(), -1, SHORT_W)):
            if side == "short" and not self.allow_short:
                continue
            ind = self.indicators(series)
            self._ctx[side] = (series, ind, self.long_signals(series, ind, w), sgn)

    def entry(self, i: int) -> Setup | None:
        found = []
        for side, (_series, _ind, signals, sgn) in self._ctx.items():
            st = signals.get(i)
            if st is not None:
                found.append(Setup(st.strategy, side, st.index, st.ref_price * sgn, st.stop * sgn,
                                   None if st.target is None else st.target * sgn, st.reason))
        return found[0] if len(found) == 1 else None  # long and short on the same candle: stand aside

    def manage(self, side: str, i: int, entry_i: int, stop: float) -> tuple[float, bool, str]:
        series, ind, _signals, sgn = self._ctx[side]
        new_stop, exit_now, why = self.long_manage(series, ind, i, entry_i, stop * sgn)
        return new_stop * sgn, exit_now, why


class TrendFollowing(Strategy):
    name = "trend"
    defaults = dict(fast=50, slow=200, entry_n=20, exit_n=10, atr_n=14, atr_stop=3.0)

    def indicators(self, s):
        p = self.p
        return dict(fast=ema(s.c, p["fast"]), slow=ema(s.c, p["slow"]), atr=atr(s.h, s.l, s.c, p["atr_n"]),
                    hi=highest_prev(s.h, p["entry_n"]), lo=lowest_prev(s.l, p["exit_n"]))

    def long_signals(self, s, ind, w):
        p, out = self.p, {}
        for i in range(len(s)):
            a = ind["atr"][i]
            if math.isnan(a) or not (s.c[i] > ind["slow"][i] and ind["fast"][i] > ind["slow"][i] and s.c[i] > ind["hi"][i]):
                continue
            out[i] = Setup(self.name, "long", i, s.c[i], s.c[i] - p["atr_stop"] * a, None,
                           f"Close {w['above']} the {p['entry_n']}-bar {w['high']}, trend {w['up']} "
                           f"(EMA{p['fast']} {w['gt']} EMA{p['slow']}); stop {p['atr_stop']:g} ATR away, trailing")
        return out

    def long_manage(self, s, ind, i, entry_i, stop):
        a = ind["atr"][i]
        if not math.isnan(a):  # chandelier trail: highest high since the fill, minus N ATR; it only ever moves up
            stop = max(stop, max(s.h[entry_i:i + 1]) - self.p["atr_stop"] * a)
        if s.c[i] < ind["lo"][i]:  # (False while the channel is still warming up)
            return stop, True, f"closed beyond the {self.p['exit_n']}-bar exit channel"
        return stop, False, ""


class SwingPullback(Strategy):
    name = "swing"
    defaults = dict(fast=50, slow=200, rsi_n=14, rsi_dip=40.0, rsi_turn=45.0, dip_lookback=6, touch_pct=2.0,
                    stop_n=10, stop_buf=0.25, atr_n=14, rr=2.0, max_hold=30)

    def indicators(self, s):
        p = self.p
        return dict(fast=ema(s.c, p["fast"]), slow=ema(s.c, p["slow"]), rsi=rsi(s.c, p["rsi_n"]),
                    atr=atr(s.h, s.l, s.c, p["atr_n"]))

    def long_signals(self, s, ind, w):
        p, out, r = self.p, {}, ind["rsi"]
        lb, sn = p["dip_lookback"], p["stop_n"]
        for i in range(max(lb + 1, sn), len(s)):
            a, window = ind["atr"][i], r[i - lb:i]
            if math.isnan(a) or math.isnan(r[i]) or any(math.isnan(x) for x in window):
                continue
            if not (s.c[i] > ind["slow"][i] and ind["fast"][i] > ind["slow"][i]):
                continue                                               # not in a trend in this direction
            if min(window) > p["rsi_dip"]:
                continue                                               # there was no pullback
            if not (r[i] > r[i - 1] and r[i] >= p["rsi_turn"] and s.c[i] > s.o[i]):
                continue                                               # and it has not turned back yet
            if min(s.l[i - lb:i + 1]) > ind["fast"][i] * (1 + p["touch_pct"] / 100):
                continue                                               # the pullback never reached the EMA zone
            stop = min(s.l[i - sn + 1:i + 1]) - p["stop_buf"] * a
            risk = s.c[i] - stop
            if risk <= 0:
                continue
            out[i] = Setup(self.name, "long", i, s.c[i], stop, s.c[i] + p["rr"] * risk,
                           f"Pullback in a {w['up']}trend: RSI dipped to {w['rsi'](min(window)):.0f} and turned "
                           f"({w['rsi'](r[i]):.0f}) near EMA{p['fast']}; stop beyond the swing, target {p['rr']:g}R")
        return out

    def long_manage(self, s, ind, i, entry_i, stop):
        if i - entry_i >= self.p["max_hold"]:
            return stop, True, f"time stop after {self.p['max_hold']} candles"
        return stop, False, ""


class SmartMoney(Strategy):
    name = "smc"
    defaults = dict(pivot_k=3, bos_window=10, ob_valid=30, stop_buf=0.25, atr_n=14, rr=2.5)

    def indicators(self, s):
        return dict(atr=atr(s.h, s.l, s.c, self.p["atr_n"]), piv=pivot_lows(s.l, self.p["pivot_k"]))

    def long_signals(self, s, ind, w):
        """sweep -> break of structure -> order block -> retest.
        1. Liquidity sweep: a candle wicks BELOW the latest confirmed swing low and closes back above it.
        2. Break of structure: within bos_window candles, a close above the range high since that swing low.
        3. Order block: the last down candle between the sweep and the break.
        4. Entry: a later up candle that dips into the order block and closes back above its low; stop beyond the
           block and the sweep, target rr R. The block is dropped if a close goes below it or it gets too old."""
        p, k, out = self.p, self.p["pivot_k"], {}
        piv, atr_ = ind["piv"], ind["atr"]
        confirmed: list[int] = []
        ptr = 0
        pending = active = None
        for i in range(len(s)):
            while ptr < len(piv) and piv[ptr] + k <= i:  # a pivot is only known k candles after it formed
                confirmed.append(piv[ptr])
                ptr += 1
            if active is not None and i > active["bos_i"]:                     # 4. retest
                if i > active["valid_until"] or s.c[i] < active["ob_low"]:
                    active = None
                elif s.l[i] <= active["ob_high"] and s.c[i] > s.o[i] and s.c[i] > active["ob_low"] \
                        and not math.isnan(atr_[i]):
                    stop = min(active["ob_low"], active["sweep_low"]) - p["stop_buf"] * atr_[i]
                    risk = s.c[i] - stop
                    if risk > 0:
                        out[i] = Setup(self.name, "long", i, s.c[i], stop, s.c[i] + p["rr"] * risk,
                                       f"Liquidity sweep of a swing {w['low']}, break of structure {w['up']}, then a "
                                       f"retest of the order block; stop beyond the block, target {p['rr']:g}R")
                    active = None
            if pending is not None and i > pending["sweep_i"]:                 # 2. + 3. break of structure
                if i > pending["expires"]:
                    pending = None
                elif s.c[i] > pending["level_high"]:
                    ob = next((m for m in range(i - 1, pending["sweep_i"] - 1, -1) if s.c[m] < s.o[m]), None)
                    if ob is not None and active is None:
                        active = dict(ob_low=s.l[ob], ob_high=s.h[ob], bos_i=i, valid_until=i + p["ob_valid"],
                                      sweep_low=pending["sweep_low"])
                    pending = None
            if pending is None and active is None and confirmed:               # 1. liquidity sweep
                j = confirmed[-1]
                if s.l[i] < s.l[j] < s.c[i]:
                    pending = dict(sweep_i=i, sweep_low=s.l[i], level_high=max(s.h[j:i + 1]),
                                   expires=i + p["bos_window"])
        return out


REGISTRY = {"trend": TrendFollowing, "swing": SwingPullback, "smc": SmartMoney}


def make(name: str, **params) -> Strategy:
    if name not in REGISTRY:
        raise ValueError(f"unknown strategy {name!r}; choose from {', '.join(REGISTRY)}")
    return REGISTRY[name](**params)
