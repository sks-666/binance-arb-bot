"""The manual entry pause survives a restart (it is saved in the state database)."""
import tempfile
import unittest
from pathlib import Path

from arbbot.commands import CommandHandler
from arbbot.engine import Engine
from arbbot.executor import PaperExecutor
from arbbot.feeds import Board
from arbbot.portfolio import Portfolio, Store
from arbbot.risk import Risk
from arbbot.scanner import Scanner
from tests.test_core import small_cfg


class PausePersistTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.path = self.dir / "state.db"
        self.cfg = small_cfg(str(self.dir))

    def boot(self):
        """What main.py does at startup: open the store, build the risk and restore a saved pause."""
        store = Store(self.path)
        pf = Portfolio(store, 1000.0)
        risk = Risk(self.cfg.risk, pf)
        risk.manual_halt = store.get_setting("manual_halt") == "1"
        eng = Engine(self.cfg, Board("sim"), Scanner(self.cfg.strategy, self.cfg.fees, self.cfg.feed.max_quote_age_s),
                     pf, risk, PaperExecutor(self.cfg.fees))
        return eng, store

    def test_settings_roundtrip(self):
        s = Store(self.path)
        self.assertIsNone(s.get_setting("manual_halt"))
        self.assertEqual(s.get_setting("manual_halt", "0"), "0")
        s.set_setting("manual_halt", "1")
        self.assertEqual(Store(self.path).get_setting("manual_halt"), "1")

    def test_a_pause_from_the_dashboard_survives_a_restart(self):
        eng, _ = self.boot()
        self.assertFalse(eng.risk.manual_halt)                     # a fresh bot is not paused
        eng.request_halt()
        eng, _ = self.boot()                                       # restart
        self.assertTrue(eng.risk.manual_halt)
        self.assertEqual(eng.risk.halt_reason, "paused manually")
        eng.request_resume()
        eng, _ = self.boot()
        self.assertFalse(eng.risk.manual_halt)                     # and resuming is remembered too

    def test_flatten_leaves_entries_paused_after_a_restart(self):
        eng, _ = self.boot()
        eng.request_flatten()
        eng, _ = self.boot()
        self.assertTrue(eng.risk.manual_halt)

    def test_telegram_pause_and_resume_are_saved(self):
        eng, store = self.boot()
        h = CommandHandler(eng, "T")
        self.assertIn("paused", h.answer("/pause").lower())
        self.assertEqual(store.get_setting("manual_halt"), "1")
        self.assertIn("resumed", h.answer("/resume").lower())
        self.assertEqual(store.get_setting("manual_halt"), "0")

    def test_an_engine_without_a_store_still_pauses(self):
        eng, _ = self.boot()
        eng.pf = type("NoStore", (), {})()                         # e.g. a test double: the pause just isn't saved
        eng.request_halt()
        self.assertTrue(eng.risk.manual_halt)
