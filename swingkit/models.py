"""Plain data types. Prices are floats; times are milliseconds since the epoch (a candle's OPEN time)."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Series:
    """Candles as parallel lists (fast to index). Indicators are computed once over the whole series: every value at
    index i depends only on candles <= i, which is what keeps backtests free of look-ahead."""
    ts: list[int]
    o: list[float]
    h: list[float]
    l: list[float]
    c: list[float]
    v: list[float]

    @classmethod
    def from_candles(cls, candles: list[Candle]) -> "Series":
        return cls([k.ts for k in candles], [k.open for k in candles], [k.high for k in candles],
                   [k.low for k in candles], [k.close for k in candles], [k.volume for k in candles])

    def __len__(self) -> int:
        return len(self.ts)

    def mirrored(self) -> "Series":
        """Every price negated (highs and lows swap): a SHORT setup on the real series is a LONG setup on this one.
        Each strategy is written once, for longs, and runs on both the series and its mirror."""
        return Series(self.ts, [-x for x in self.o], [-x for x in self.l], [-x for x in self.h],
                      [-x for x in self.c], self.v)


@dataclass
class Setup:
    """A trade idea decided at the CLOSE of candle `index`. It would be filled at the next candle's open."""
    strategy: str
    side: str                 # "long" | "short"
    index: int
    ref_price: float          # that candle's close
    stop: float
    target: float | None
    reason: str
