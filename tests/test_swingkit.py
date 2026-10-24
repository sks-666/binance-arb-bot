"""swingkit: indicators, risk sizing, the three strategies (incl. look-ahead and long/short symmetry), and the backtester's
honesty rules, all on synthetic candles."""
import math
import random
import unittest

from swingkit.backtest import CostCfg, run_backtest, summarize
from swingkit.indicators import NAN, atr, ema, highest_prev, lowest_prev, pivot_highs, pivot_lows, rsi
from swingkit.models import Series, Setup
from swingkit.risk import HARD_MAX_RISK_PCT, DrawdownBreaker, RiskCfg, can_open, size_position
from swingkit.strategies import REGISTRY, SmartMoney, Strategy, SwingPullback, TrendFollowing, make

H4 = 14_400_000


def series_from(rows, start=0, bar=H4) -> Series:
    """rows: (open, high, low, close)."""
    return Series([start + k * bar for k in range(len(rows))], [r[0] for r in rows], [r[1] for r in rows],
                  [r[2] for r in rows], [r[3] for r in rows], [1.0] * len(rows))


def walk(n, seed, vol=0.012):
    """A trending/ranging random walk (drift flips every 150 candles) so every strategy has something to trade."""
    rng, px, rows = random.Random(seed), 100.0, []
    drift = 0.0
    for i in range(n):
        if i % 150 == 0:
            drift = rng.choice([-0.003, -0.001, 0.0, 0.001, 0.003])
        o = px
        px *= math.exp(rng.gauss(drift, vol))
        hi, lo = max(o, px) * (1 + abs(rng.gauss(0, vol / 2))), min(o, px) * (1 - abs(rng.gauss(0, vol / 2)))
        rows.append((o, hi, lo, px))
    return series_from(rows)


class Scripted(Strategy):
    """Returns exactly the setups and manage decisions a test tells it to."""
    name = "scripted"

    def __init__(self, setups: dict[int, Setup] | None = None, manage=None):
        super().__init__()
        self.setups, self._manage = setups or {}, manage

    def prepare(self, s):
        pass

    def entry(self, i):
        return self.setups.get(i)

    def manage(self, side, i, entry_i, stop):
        return self._manage(side, i, entry_i, stop) if self._manage else (stop, False, "")


def setup(i, side="long", ref=100.0, stop=95.0, target=None):
    return Setup("scripted", side, i, ref, stop, target, "test setup")


def bt(rows, setups, manage=None, risk=None, costs=None, capital=10_000.0, bar=H4, funding=None):
    s = series_from(rows, bar=bar)
    res = run_backtest({"AAAUSDT": s}, lambda: Scripted(setups, manage), risk or RiskCfg(),
                       costs or CostCfg(0, 0, 0), capital, bar, funding)
    return res, s


QUIET = [(100, 101, 99, 100)] * 3  # candles 0, 1 (the signal) and 2 (the fill, at its open)


class IndicatorTests(unittest.TestCase):
    def test_values_depend_only_on_the_past(self):
        s = walk(400, 3)
        for fn in (lambda x: ema(x.c, 20), lambda x: rsi(x.c, 14), lambda x: atr(x.h, x.l, x.c, 14),
                   lambda x: highest_prev(x.h, 10), lambda x: lowest_prev(x.l, 10)):
            full, part = fn(s), fn(Series(s.ts[:250], s.o[:250], s.h[:250], s.l[:250], s.c[:250], s.v[:250]))
            for a, b in zip(full[:250], part):
                self.assertTrue((math.isnan(a) and math.isnan(b)) or abs(a - b) < 1e-9)

    def test_known_values(self):
        self.assertTrue(all(abs(x - 5.0) < 1e-12 for x in ema([5.0] * 10, 4)[3:]))
        self.assertTrue(math.isnan(ema([1, 2, 3], 4)[0]))
        self.assertEqual(rsi(list(range(1, 40)), 14)[-1], 100.0)           # only ever up
        self.assertAlmostEqual(rsi(list(range(40, 1, -1)), 14)[-1], 0.0)   # only ever down
        self.assertAlmostEqual(atr([2.0] * 30, [1.0] * 30, [1.5] * 30, 14)[-1], 1.0)
        self.assertEqual(highest_prev([1, 5, 2, 3], 2)[3], 5)               # excludes the current candle
        self.assertEqual(highest_prev([1, 2, 9], 2)[2], 2)

    def test_pivots_need_k_candles_on_each_side(self):
        self.assertEqual(pivot_lows([5, 4, 3, 1, 3, 4, 5, 6], 2), [3])
        self.assertEqual(pivot_highs([1, 2, 3, 9, 3, 2, 1], 2), [3])
        self.assertEqual(pivot_lows([5, 4, 3, 1, 3], 2), [])  # the right-hand side is not there yet

    def test_mirroring_twice_is_the_identity_and_swaps_highs_and_lows(self):
        s = walk(50, 1)
        m = s.mirrored()
        self.assertEqual(m.h, [-x for x in s.l])
        self.assertEqual(m.mirrored().c, s.c)
        self.assertEqual(m.mirrored().h, s.h)


class RiskTests(unittest.TestCase):
    def test_a_stop_out_costs_exactly_the_risk_percentage(self):
        for pct in (0.5, 1.0, 2.0):
            qty, why = size_position(10_000, 100.0, 95.0, RiskCfg(risk_pct=pct))
            self.assertEqual(why, "")
            self.assertAlmostEqual(qty * 5.0, 10_000 * pct / 100)

    def test_risk_above_two_percent_is_refused(self):
        for bad in (0, -1, 2.01, 5):
            with self.assertRaises(ValueError):
                RiskCfg(risk_pct=bad)
        self.assertEqual(HARD_MAX_RISK_PCT, 2.0)

    def test_leverage_cap_and_tiny_stops(self):
        qty, _ = size_position(10_000, 100.0, 99.0, RiskCfg(risk_pct=1.0, max_leverage=3.0))
        self.assertAlmostEqual(qty * 100.0, 10_000)  # a 1% stop: risking 1% means a 1x position
        qty, _ = size_position(10_000, 100.0, 99.69, RiskCfg(risk_pct=1.0, max_leverage=3.0))
        self.assertAlmostEqual(qty * 100.0, 30_000)  # a 0.31% stop would need ~3.2x: the 3x cap binds
        self.assertEqual(size_position(10_000, 100.0, 99.9, RiskCfg())[0], 0.0)  # a 0.1% stop is under the 0.3% floor
        self.assertEqual(size_position(0, 100.0, 95.0, RiskCfg())[0], 0.0)

    def test_open_limits(self):
        cfg = RiskCfg(max_open=2, max_heat_pct=3.0)
        self.assertEqual(can_open(10_000, [100.0], 100.0, cfg), "")
        self.assertIn("2 positions", can_open(10_000, [100.0, 100.0], 100.0, cfg))
        self.assertIn("exceed 3%", can_open(10_000, [150.0], 200.0, cfg))

    def test_drawdown_breaker_pauses_then_starts_over_instead_of_halting_forever(self):
        day = 86_400_000
        b = DrawdownBreaker(20.0, 10 * day)
        b.update(10_000)
        self.assertEqual(b.blocked(0, 9_000), "")                       # 10% down: fine
        self.assertIn("paused after a 20% drawdown", b.blocked(day, 7_900))
        self.assertIn("paused", b.blocked(5 * day, 9_500))              # recovering does not lift the pause early
        self.assertEqual(b.blocked(11 * day, 7_900), "")                # the pause is over: measured from here
        self.assertEqual(b.blocked(12 * day, 7_000), "")                # 11% below the NEW peak: allowed
        self.assertIn("paused", b.blocked(13 * day, 6_200))             # 21.5% below it: paused again


class StrategyTests(unittest.TestCase):
    def loosened(self, name):
        return {"trend": dict(fast=10, slow=40, entry_n=10, exit_n=5),
                "swing": dict(fast=10, slow=40, touch_pct=3.0),
                "smc": {}}[name]

    def test_unknown_parameters_and_names_are_rejected(self):
        with self.assertRaises(ValueError):
            TrendFollowing(fats=5)
        with self.assertRaises(ValueError):
            make("nope")
        self.assertEqual(set(REGISTRY), {"trend", "swing", "smc"})

    def test_no_look_ahead_decisions_on_a_prefix_equal_decisions_on_the_full_series(self):
        s = walk(3000, 11)
        for name in REGISTRY:
            full = make(name, **self.loosened(name))
            full.prepare(s)
            signals = [i for i in range(len(s)) if full.entry(i)]
            self.assertGreater(len(signals), 3, f"{name}: the test would be vacuous without signals")
            for cut in (900, 1700, 2500):
                part = make(name, **self.loosened(name))
                part.prepare(Series(s.ts[:cut], s.o[:cut], s.h[:cut], s.l[:cut], s.c[:cut], s.v[:cut]))
                for i in range(cut):
                    a, b = full.entry(i), part.entry(i)
                    self.assertEqual(a is None, b is None, f"{name}: decision at {i} changed with later candles")
                    if a:
                        self.assertEqual((a.side, a.ref_price, a.stop, a.target), (b.side, b.ref_price, b.stop, b.target))

    def test_long_and_short_are_mirror_images(self):
        s = walk(3000, 5)
        for name in REGISTRY:
            on_real, on_mirror = make(name, **self.loosened(name)), make(name, **self.loosened(name))
            on_real.prepare(s)
            on_mirror.prepare(s.mirrored())
            seen = 0
            for i in range(len(s)):
                a, b = on_real.entry(i), on_mirror.entry(i)
                if a and a.side == "long":
                    seen += 1
                    self.assertEqual(b.side, "short", f"{name} at {i}")
                    self.assertAlmostEqual(a.ref_price, -b.ref_price)
                    self.assertAlmostEqual(a.stop, -b.stop)
            self.assertGreater(seen, 0, name)

    def test_setups_are_well_formed(self):
        s = walk(3000, 8)
        for name in REGISTRY:
            strat = make(name, **self.loosened(name))
            strat.prepare(s)
            for i in range(len(s)):
                st = strat.entry(i)
                if not st:
                    continue
                self.assertEqual(st.ref_price, s.c[i])
                if st.side == "long":
                    self.assertLess(st.stop, st.ref_price)
                    self.assertTrue(st.target is None or st.target > st.ref_price)
                else:
                    self.assertGreater(st.stop, st.ref_price)
                    self.assertTrue(st.target is None or st.target < st.ref_price)
                self.assertTrue(st.reason)

    def test_trend_following_enters_a_breakout_with_the_trend_and_not_against_it(self):
        up = series_from([(100 + k, 101 + k, 99 + k, 100.5 + k) for k in range(60)])
        t = TrendFollowing(fast=5, slow=20, entry_n=5, exit_n=3)
        t.prepare(up)
        self.assertEqual({t.entry(i).side for i in range(len(up)) if t.entry(i)}, {"long"})
        down = series_from([(200 - k, 201 - k, 199 - k, 199.5 - k) for k in range(60)])
        t2 = TrendFollowing(fast=5, slow=20, entry_n=5, exit_n=3)
        t2.prepare(down)
        self.assertEqual({t2.entry(i).side for i in range(len(down)) if t2.entry(i)}, {"short"})

    def test_trend_following_trail_only_moves_up_and_exits_on_the_channel(self):
        up = series_from([(100 + k, 101 + k, 99 + k, 100.5 + k) for k in range(60)])
        t = TrendFollowing(fast=5, slow=20, entry_n=5, exit_n=3, atr_stop=3.0)
        t.prepare(up)
        stop = 90.0
        for i in range(40, 55):
            new, exit_now, _ = t.manage("long", i, 40, stop)
            self.assertGreaterEqual(new, stop)
            self.assertFalse(exit_now)
            stop = new
        crash = series_from([(100 + k, 101 + k, 99 + k, 100.5 + k) for k in range(50)] + [(150, 150, 120, 121)] * 3)
        t.prepare(crash)
        self.assertTrue(t.manage("long", 50, 40, 90.0)[1])  # a close beyond the 3-bar low ends the trade

    def test_swing_pullback_needs_trend_dip_and_turn(self):
        rows = [(100 + 0.6 * k, 101 + 0.6 * k, 99 + 0.6 * k, 100.4 + 0.6 * k) for k in range(80)]
        for _ in range(6):                                    # a sharp pullback...
            px = rows[-1][3]
            rows.append((px, px + 0.2, px - 3.0, px - 2.5))
        for _ in range(3):                                    # ...then a turn back up
            px = rows[-1][3]
            rows.append((px, px + 3.0, px - 0.2, px + 2.6))
        s = series_from(rows)
        sw = SwingPullback(fast=10, slow=40, touch_pct=8.0)
        sw.prepare(s)
        hits = [i for i in range(len(s)) if sw.entry(i)]
        self.assertTrue(hits and all(i >= 86 for i in hits), hits)
        st = sw.entry(hits[0])
        self.assertEqual(st.side, "long")
        self.assertAlmostEqual(st.target - st.ref_price, 2.0 * (st.ref_price - st.stop))
        out = sw.manage("long", hits[0] + 31, hits[0] + 1, st.stop)  # 30 candles after the fill
        self.assertTrue(out[1] and "time stop" in out[2])

    # sweep of the swing low at 95 (candle 8), break of structure (10), order block = candle 9, retest (12)
    SMC_ROWS = [(100, 101, 99, 100), (100, 100.5, 98.5, 99), (99, 99.5, 97, 97.5), (97.5, 98.5, 95, 96),
                (96, 98, 95.5, 97.5), (97.5, 99, 96.5, 98.5), (98.5, 100, 98, 99.5), (99.5, 100, 97.5, 98),
                (98, 98.5, 94, 98.2), (98.2, 98.4, 96.8, 97.0), (97.0, 101.5, 96.9, 101.2), (101.2, 102, 100.5, 101.8),
                (99.0, 101, 98.3, 100.5), (100.5, 102, 100, 101.5), (101.5, 103, 101, 102.5)]

    def test_smc_sweep_break_of_structure_order_block_retest_long(self):
        s = series_from(self.SMC_ROWS)
        smc = SmartMoney(pivot_k=2, atr_n=3)
        smc.prepare(s)
        self.assertEqual([i for i in range(len(s)) if smc.entry(i)], [12])
        st = smc.entry(12)
        self.assertEqual((st.side, st.ref_price), ("long", 100.5))
        self.assertLess(st.stop, 94.0)                    # beyond the sweep low and the order block
        self.assertAlmostEqual(st.target - 100.5, 2.5 * (100.5 - st.stop))
        self.assertIn("order block", st.reason)

    def test_smc_short_is_the_same_pattern_upside_down(self):
        s = series_from(self.SMC_ROWS).mirrored()         # prices negated: a sweep of a swing HIGH, break down...
        smc = SmartMoney(pivot_k=2, atr_n=3)
        smc.prepare(s)
        self.assertEqual([i for i in range(len(s)) if smc.entry(i)], [12])
        st = smc.entry(12)
        self.assertEqual(st.side, "short")
        self.assertGreater(st.stop, st.ref_price)
        self.assertLess(st.target, st.ref_price)

    def test_smc_ignores_an_order_block_that_is_broken_or_a_break_that_comes_too_late(self):
        broken = list(self.SMC_ROWS)
        broken[11] = (101.2, 101.5, 94.0, 95.0)           # closes through the block's low before any retest
        smc = SmartMoney(pivot_k=2, atr_n=3)
        smc.prepare(series_from(broken))
        self.assertFalse(any(smc.entry(i) for i in range(len(broken))))
        late = SmartMoney(pivot_k=2, atr_n=3, bos_window=1)  # the break must come within one candle of the sweep
        late.prepare(series_from(self.SMC_ROWS))
        self.assertFalse(any(late.entry(i) for i in range(len(self.SMC_ROWS))))


class BacktestHonestyTests(unittest.TestCase):
    def test_fills_at_the_next_open_never_at_the_signal_candle(self):
        rows = [(100, 101, 99, 100), (100, 101, 99, 100), (103, 104, 102, 103), (103, 104, 102, 103.5)]
        res, s = bt(rows, {1: setup(1, ref=100.0, stop=95.0)})
        t = res.trades[0]
        self.assertEqual((t.entry_px, t.entry_ts), (103.0, s.ts[2]))  # the open of candle 2, not the close of candle 1

    def test_slippage_is_adverse_on_both_fills(self):
        res, _ = bt(QUIET + [(100, 101, 94, 96)], {1: setup(1)}, costs=CostCfg(0, 10, 0))
        t = res.trades[0]
        self.assertAlmostEqual(t.entry_px, 100.0 * 1.001)        # bought 10 bps worse
        self.assertAlmostEqual(t.exit_px, 95.0 * 0.999)          # the stop sold 10 bps worse

    def test_a_stop_out_loses_one_percent_of_equity_and_minus_one_r(self):
        res, _ = bt(QUIET + [(100, 100, 94, 96)], {1: setup(1)})
        t = res.trades[0]
        self.assertEqual((t.reason_out, t.exit_px), ("stop", 95.0))
        self.assertAlmostEqual(t.pnl_usd, -100.0)                # 1% of 10,000
        self.assertAlmostEqual(t.r_multiple, -1.0)
        self.assertAlmostEqual(res.equity[-1][1], 9_900.0)

    def test_a_gap_through_the_stop_fills_at_the_open_and_loses_more_than_one_r(self):
        res, _ = bt(QUIET + [(90, 91, 88, 89)], {1: setup(1)})
        t = res.trades[0]
        self.assertEqual(t.exit_px, 90.0)
        self.assertAlmostEqual(t.r_multiple, -2.0)

    def test_stop_wins_when_stop_and_target_both_fall_inside_one_candle(self):
        res, _ = bt(QUIET + [(100, 106, 94, 100)], {1: setup(1, target=105.0)})
        self.assertEqual(res.trades[0].reason_out, "stop")

    def test_target_pays_the_planned_r(self):
        res, _ = bt(QUIET + [(100, 106, 99, 105)], {1: setup(1, target=110.0)})
        self.assertEqual(res.trades[0].reason_out, "end of data")  # not reached: closed at the last close
        res, _ = bt(QUIET + [(100, 111, 99, 105)], {1: setup(1, target=110.0)})
        t = res.trades[0]
        self.assertEqual((t.reason_out, t.exit_px), ("target", 110.0))
        self.assertAlmostEqual(t.r_multiple, 2.0)

    def test_shorts_profit_when_price_falls(self):
        res, _ = bt(QUIET + [(100, 101, 89, 90)], {1: setup(1, "short", 100.0, 105.0, 90.0)})
        t = res.trades[0]
        self.assertEqual((t.side, t.reason_out, t.exit_px), ("short", "target", 90.0))
        self.assertAlmostEqual(t.r_multiple, 2.0)
        res, _ = bt(QUIET + [(100, 106, 99, 104)], {1: setup(1, "short", 100.0, 105.0, 90.0)})
        self.assertAlmostEqual(res.trades[0].r_multiple, -1.0)

    def test_fees_and_funding_are_charged(self):
        res, _ = bt(QUIET + [(100, 111, 99, 105)], {1: setup(1, target=110.0)}, costs=CostCfg(10, 0, 8),
                    bar=8 * 3_600_000)
        t = res.trades[0]
        qty = 100.0 / 5.0
        entry_fee, exit_fee, funding = 0.001 * 100.0 * qty, 0.001 * 110.0 * qty, qty * 100.0 * 0.0008
        self.assertAlmostEqual(t.fees_usd, entry_fee + exit_fee)
        self.assertAlmostEqual(t.funding_usd, funding)                  # one candle held at its close
        self.assertAlmostEqual(t.pnl_usd, 10.0 * qty - entry_fee - exit_fee - funding)
        self.assertAlmostEqual(res.equity[-1][1], 10_000 + t.pnl_usd)

    def test_real_funding_is_paid_by_longs_and_received_by_shorts_only_while_held(self):
        # filled at the open of candle 2, stopped out inside candle 3; qty = 20, price 100 at the close of candle 2
        rows = QUIET + [(100, 100, 94, 96)]
        events = [(1 * H4 + 5, 0.01),      # before the fill: nobody pays
                  (2 * H4 + 5, 0.001),     # held through it: 0.1% of 20 x 100 = 2.0
                  (3 * H4 + 5, 0.01)]      # the position is stopped out inside candle 3, before its close
        longs, _ = bt(rows, {1: setup(1)}, funding={"AAAUSDT": events})
        self.assertAlmostEqual(longs.trades[0].funding_usd, 2.0)
        self.assertAlmostEqual(longs.trades[0].pnl_usd, -100.0 - 2.0)
        shorts, _ = bt(QUIET + [(100, 106, 99, 104)], {1: setup(1, "short", 100.0, 105.0, 90.0)},
                       funding={"AAAUSDT": events})
        self.assertAlmostEqual(shorts.trades[0].funding_usd, -2.0)            # received
        self.assertAlmostEqual(shorts.trades[0].pnl_usd, -100.0 + 2.0)
        neg, _ = bt(rows, {1: setup(1)}, funding={"AAAUSDT": [(2 * H4 + 5, -0.001)]})
        self.assertAlmostEqual(neg.trades[0].funding_usd, -2.0)               # negative funding: longs are paid

    def test_a_coin_without_funding_history_uses_the_flat_charge(self):
        rows = QUIET + [(100, 100, 94, 96)]
        res, _ = bt(rows, {1: setup(1)}, costs=CostCfg(0, 0, 8), bar=8 * 3_600_000, funding={"OTHER": [(1, 0.5)]})
        self.assertAlmostEqual(res.trades[0].funding_usd, 20 * 100 * 0.0008)

    def test_after_a_drawdown_entries_pause_and_then_resume(self):
        base, stop_bar = (100, 101, 99, 100), (100, 100, 94, 96)
        rows = [base, base, base, stop_bar, base, stop_bar, base, base, base, base, base, base, base, base, base]
        risk = RiskCfg(risk_pct=2.0, drawdown_halt_pct=3.0, drawdown_pause_days=1.0)  # 1 day = 6 candles
        res, s = bt(rows, {i: setup(i) for i in (1, 3, 5, 12)}, risk=risk)
        self.assertEqual([t.reason_out for t in res.trades], ["stop", "stop", "end of data"])
        self.assertEqual(res.skipped["paused after a 3% drawdown"], 1)         # the entry right after the second loss
        self.assertEqual(res.trades[2].entry_ts, s.ts[13])                     # resumed once the pause was over

    def test_a_signal_on_the_last_candle_cannot_be_filled(self):
        res, _ = bt(QUIET, {2: setup(2)})
        self.assertEqual(res.trades, [])

    def test_open_positions_are_closed_at_the_end_and_marked(self):
        res, _ = bt(QUIET + [(100, 101, 99, 101)], {1: setup(1)})
        t = res.trades[0]
        self.assertEqual((t.reason_out, t.exit_px), ("end of data", 101.0))

    def test_stops_only_tighten(self):
        rows = QUIET + [(100, 100, 96, 99), (99, 99, 90, 91)]
        res, _ = bt(rows, {1: setup(1)}, manage=lambda side, i, e, stop: (90.0, False, ""))   # tries to loosen
        self.assertEqual(res.trades[0].exit_px, 95.0)
        res, _ = bt(rows, {1: setup(1)}, manage=lambda side, i, e, stop: (99.5, False, ""))   # tightens
        self.assertEqual(res.trades[0].exit_px, 99.5)

    def test_a_strategy_exit_happens_at_the_next_open(self):
        rows = QUIET + [(102, 103, 101, 102), (104, 105, 103, 104)]
        res, s = bt(rows, {1: setup(1)}, manage=lambda side, i, e, stop: (stop, i == 2, "channel exit"))
        t = res.trades[0]
        self.assertEqual((t.reason_out, t.exit_px, t.exit_ts), ("channel exit", 102.0, s.ts[3]))

    def test_one_position_per_coin_and_limits_across_coins(self):
        s = series_from(QUIET + [(100, 101, 99, 100)] * 3)
        res = run_backtest({"A": s, "B": s}, lambda: Scripted({1: setup(1), 2: setup(2)}), RiskCfg(max_open=1),
                           CostCfg(0, 0, 0), 10_000.0, H4)
        self.assertEqual([t.symbol for t in res.trades], ["A"])
        self.assertTrue(any("already 1 positions open" in k for k in res.skipped))

    def test_summary_numbers(self):
        rows = QUIET + [(100, 111, 99, 105)] + [(105, 106, 104, 105)] * 3 + [(105, 105, 99, 100)]
        res, s = bt(rows, {1: setup(1, target=110.0), 4: setup(4, ref=105.0, stop=100.0)})
        m = summarize(res, {"AAAUSDT": s})
        self.assertEqual((m["trades"], m["win_rate"]), (2, 50.0))
        self.assertGreater(m["profit_factor"], 0)
        self.assertAlmostEqual(m["buy_hold_pct"], 0.0)
        self.assertEqual(m["exits"], {"target": 1, "stop": 1})
        self.assertAlmostEqual(m["return_pct"], m["net_pnl"] / 100)

    def test_whole_pipeline_runs_for_every_strategy_on_noise(self):
        data = {"AAAUSDT": walk(1500, 21), "BBBUSDT": walk(1500, 22)}
        for name in REGISTRY:
            res = run_backtest(data, lambda n=name: make(n), RiskCfg(), CostCfg(), 10_000.0, H4)
            m = summarize(res, data)
            for t in res.trades:
                self.assertLessEqual(t.risk_usd, 10_000 * 0.02 * 3)         # never anywhere near a runaway position
                self.assertGreater(t.qty, 0)
            self.assertTrue(math.isfinite(m["return_pct"]))
            self.assertLessEqual(m["max_drawdown_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
