"""Launcher that works from any directory:  python /path/to/binance-arb-bot/swing.py backtest|scan|journal|download ..."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from swingkit.cli import main  # noqa: E402

sys.exit(main())
