"""Risk: size every trade so that being stopped out costs a fixed fraction of equity (default 1%, never above 2%),
and refuse trades that would pile on too much total risk."""
from dataclasses import dataclass

HARD_MAX_RISK_PCT = 2.0  # the ceiling no setting can lift: "risk only 1-2% per trade"
MIN_NOTIONAL_USD = 10.0


@dataclass
class RiskCfg:
    risk_pct: float = 1.0            # % of equity lost if the stop is hit
    max_leverage: float = 3.0        # one position's notional / equity
    max_open: int = 5                # positions at once
    max_heat_pct: float = 5.0        # total % of equity at risk across all open positions
    min_stop_pct: float = 0.3        # a stop closer than this to the entry is noise: skip the trade
    drawdown_halt_pct: float = 25.0  # stop opening trades when equity is this far below its peak...
    drawdown_pause_days: float = 14.0  # ...for this long, then start over from the current equity

    def __post_init__(self) -> None:
        if not 0 < self.risk_pct <= HARD_MAX_RISK_PCT:
            raise ValueError(f"risk_pct must be above 0 and at most {HARD_MAX_RISK_PCT:g}% of equity per trade, "
                             f"got {self.risk_pct:g}")
        if self.max_leverage <= 0 or self.max_open < 1 or self.max_heat_pct < self.risk_pct:
            raise ValueError("max_leverage must be > 0, max_open >= 1 and max_heat_pct >= risk_pct")


def size_position(equity: float, entry: float, stop: float, cfg: RiskCfg) -> tuple[float, str]:
    """(quantity, '') or (0, why not). Quantity is in base-asset units."""
    if equity <= 0 or entry <= 0:
        return 0.0, "no equity"
    dist = abs(entry - stop)
    if dist / entry * 100 < cfg.min_stop_pct:
        return 0.0, f"stop is closer than {cfg.min_stop_pct:g}% to the entry"
    qty = min(equity * cfg.risk_pct / 100 / dist, equity * cfg.max_leverage / entry)
    if qty * entry < MIN_NOTIONAL_USD:
        return 0.0, "position would be below the minimum size"
    return qty, ""


def can_open(equity: float, open_risks_usd: list[float], new_risk_usd: float, cfg: RiskCfg) -> str:
    """'' when a new position is allowed, otherwise the reason."""
    if len(open_risks_usd) >= cfg.max_open:
        return f"already {cfg.max_open} positions open"
    if (sum(open_risks_usd) + new_risk_usd) / max(equity, 1e-9) * 100 > cfg.max_heat_pct:
        return f"total risk would exceed {cfg.max_heat_pct:g}% of equity"
    return ""


class DrawdownBreaker:
    """Pauses new entries after a drawdown, then starts over from the current equity. A halt that never lifts would
    freeze a strategy for good: with no new trades, equity cannot recover to the old peak."""

    def __init__(self, halt_pct: float, pause_ms: int):
        self.halt_pct, self.pause_ms = halt_pct, pause_ms
        self.peak = 0.0
        self.until = 0

    def update(self, equity: float) -> None:
        self.peak = max(self.peak, equity)

    def blocked(self, ts: int, equity: float) -> str:
        """'' when entries are allowed at time ts, otherwise why not."""
        if self.until and ts >= self.until:        # the pause is over: measure from here
            self.until, self.peak = 0, equity
        if not self.until and self.peak > 0 and (self.peak - equity) / self.peak * 100 >= self.halt_pct:
            self.until = ts + self.pause_ms
        return f"paused after a {self.halt_pct:g}% drawdown" if self.until else ""
