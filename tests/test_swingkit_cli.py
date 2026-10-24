"""The swingkit command line, offline, on cached synthetic candles."""
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from swingkit.cli import main
from swingkit.data import cache_path, save_csv
from swingkit.models import Candle
from tests.test_swingkit import walk


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    code = None
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(list(argv))
        except SystemExit as e:
            code = e.code
    return code, out.getvalue(), err.getvalue()


class CliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        for sym, seed in (("AAAUSDT", 21), ("BBBUSDT", 22)):
            s = walk(2500, seed)
            save_csv(cache_path(cls.tmp / "klines", "futures", sym, "4h"),
                     [Candle(s.ts[k], s.o[k], s.h[k], s.l[k], s.c[k], s.v[k]) for k in range(len(s))])
        cls.base = ["--symbols", "AAAUSDT,BBBUSDT", "--cache-dir", str(cls.tmp / "klines"), "--offline", "--days", "2000"]

    def test_backtest_all_strategies_prints_reports_and_a_comparison(self):
        code, out, _ = run("backtest", *self.base)
        self.assertEqual(code, 0)
        for needle in ("=== trend:", "=== swing:", "=== smc:", "side by side", "simply holding", "expectancy"):
            self.assertIn(needle, out)

    def test_one_strategy_with_parameters_csv_and_journal(self):
        csv_path, db = self.tmp / "out.csv", self.tmp / "j.db"
        code, out, _ = run("backtest", *self.base, "--strategy", "trend", "--set", "trend.fast=10", "--set", "trend.slow=40",
                           "--csv", str(csv_path), "--journal", "--journal-db", str(db), "--no-short")
        self.assertEqual(code, 0, out)
        self.assertNotIn("=== swing", out)
        self.assertTrue((self.tmp / "out_trend.csv").exists())
        code, out, _ = run("journal", "--db", str(db), "--summary")
        self.assertEqual(code, 0)
        self.assertRegex(out, r"\d+ closed, 0 open")
        code, out, _ = run("journal", "--db", str(db), "--last", "3")
        self.assertIn("why in:", out)
        self.assertIn("backtest", out)
        code, out, _ = run("journal", "--db", str(db), "--note", "1", "Taken on a clean breakout.")
        self.assertEqual((code, out.strip()), (0, "Note added."))
        self.assertIn("note: Taken on a clean breakout.", run("journal", "--db", str(db), "--last", "500")[1])
        self.assertEqual(run("journal", "--db", str(db), "--note", "99999", "x")[0], 1)
        self.assertEqual(run("journal", "--db", str(db), "--note", "abc", "x")[0], 2)
        exp = self.tmp / "export.csv"
        code, out, _ = run("journal", "--db", str(db), "--export", str(exp))
        self.assertIn("Exported", out)
        self.assertTrue(exp.read_text().startswith("id,run_id,mode"))

    def test_risk_above_two_percent_is_refused_before_anything_runs(self):
        code, out, err = run("backtest", *self.base, "--risk-pct", "3")
        self.assertEqual(code, 2)
        self.assertIn("Refused", err)
        self.assertNotIn("===", out)

    def test_bad_inputs_get_clear_messages(self):
        code, _, err = run("backtest", *self.base, "--strategy", "trend", "--set", "trend.fats=5")
        self.assertEqual(code, 2)
        self.assertIn("unknown parameter", err)
        self.assertNotEqual(run("backtest", *self.base, "--set", "nonsense")[0], 0)
        self.assertNotEqual(run("backtest", *self.base, "--set", "trend.fast=abc")[0], 0)
        self.assertNotEqual(run("backtest", "--symbols", "btc/usdt", "--offline")[0], 0)
        code, _, _ = run("backtest", "--symbols", "NOPEUSDT", "--cache-dir", str(self.tmp / "klines"), "--offline")
        self.assertNotEqual(code, 0)  # nothing cached for it

    def test_scan_reports_setups_or_says_there_are_none(self):
        code, out, _ = run("scan", *self.base)
        self.assertEqual(code, 0)
        self.assertTrue("No setups" in out or "why:" in out)

    def test_defaults_are_sane(self):
        from swingkit.cli import build_parser
        a = build_parser().parse_args(["backtest"])
        self.assertEqual((a.interval, a.days, a.risk_pct, a.capital, a.strategy), ("4h", 730, 1.0, 10_000.0, "all"))


if __name__ == "__main__":
    unittest.main()
