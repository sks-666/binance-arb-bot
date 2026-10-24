"""Indicators over whole series. Each value at index i uses candles <= i only (no look-ahead). Warm-up values are NaN,
and any comparison with NaN is False, so a strategy can never fire before its indicators exist."""
import math

NAN = math.nan


def ema(values: list[float], n: int) -> list[float]:
    out = [NAN] * len(values)
    if n < 1 or len(values) < n:
        return out
    k = 2.0 / (n + 1)
    out[n - 1] = sum(values[:n]) / n  # seeded with the simple average of the first n values
    for i in range(n, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def rsi(closes: list[float], n: int = 14) -> list[float]:
    """Wilder's RSI. First value at index n."""
    out = [NAN] * len(closes)
    if len(closes) <= n:
        return out
    gain = loss = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gain += max(d, 0.0)
        loss += max(-d, 0.0)
    gain, loss = gain / n, loss / n
    out[n] = 100.0 if loss == 0 else 100.0 - 100.0 / (1 + gain / loss)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = (gain * (n - 1) + max(d, 0.0)) / n
        loss = (loss * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if loss == 0 else 100.0 - 100.0 / (1 + gain / loss)
    return out


def atr(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> list[float]:
    """Wilder's average true range. First value at index n."""
    size = len(closes)
    out = [NAN] * size
    if size <= n:
        return out
    tr = [highs[0] - lows[0]] + [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, size)
    ]
    out[n] = sum(tr[1:n + 1]) / n
    for i in range(n + 1, size):
        out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def highest_prev(values: list[float], n: int) -> list[float]:
    """Highest of the previous n values, NOT including the current one (a breakout level)."""
    return [max(values[i - n:i]) if i >= n else NAN for i in range(len(values))]


def lowest_prev(values: list[float], n: int) -> list[float]:
    return [min(values[i - n:i]) if i >= n else NAN for i in range(len(values))]


def pivot_lows(lows: list[float], k: int) -> list[int]:
    """Indexes j whose low is strictly below the k candles on each side. A pivot is only KNOWN k candles later (j + k):
    callers must not use pivot j before candle j + k has closed."""
    return [j for j in range(k, len(lows) - k)
            if lows[j] < min(lows[j - k:j]) and lows[j] < min(lows[j + 1:j + k + 1])]


def pivot_highs(highs: list[float], k: int) -> list[int]:
    return [j for j in range(k, len(highs) - k)
            if highs[j] > max(highs[j - k:j]) and highs[j] > max(highs[j + 1:j + k + 1])]
