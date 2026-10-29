"""P&L reports by day, week and month for the dashboard panels (arbitrage, swing paper, swing demo, order ticket).

Every source is reduced to events: (time in epoch seconds, P&L in USD, is_trade). A trade counts toward the number of
trades and wins/losses; a non-trade amount (a fee or funding payment on the futures account) only moves the P&L.
Periods are UTC: days, weeks starting on Monday, calendar months. Empty periods are kept so gaps are visible.
"""
from datetime import datetime, timedelta, timezone

PERIODS = {"day": 30, "week": 12, "month": 12}       # how many periods each report shows


def period_start(ts: float, period: str) -> datetime:
    d = datetime.fromtimestamp(ts, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        return d - timedelta(days=d.weekday())
    if period == "month":
        return d.replace(day=1)
    return d


def _previous(start: datetime, period: str) -> datetime:
    if period == "day":
        return start - timedelta(days=1)
    if period == "week":
        return start - timedelta(weeks=1)
    return (start - timedelta(days=1)).replace(day=1)


def _label(start: datetime, period: str) -> str:
    if period == "week":
        return f"week of {start:%Y-%m-%d}"
    return f"{start:%Y-%m}" if period == "month" else f"{start:%Y-%m-%d}"


def report(events, period: str, now: float, count: int | None = None) -> dict:
    """Bucket (ts, pnl, is_trade) events into the last `count` periods up to `now`, newest first, with totals."""
    if period not in PERIODS:
        raise ValueError(f"period must be one of {', '.join(PERIODS)}")
    count = count or PERIODS[period]
    starts, s = [], period_start(now, period)
    for _ in range(count):
        starts.append(s)
        s = _previous(s, period)
    rows = {st: dict(start=int(st.timestamp()), label=_label(st, period), pnl=0.0, trades=0, wins=0, losses=0)
            for st in starts}
    oldest = starts[-1].timestamp()
    for ts, pnl, is_trade in events:
        if ts < oldest or ts > now + 60:
            continue
        row = rows.get(period_start(ts, period))
        if row is None:
            continue
        row["pnl"] += pnl
        if is_trade:
            row["trades"] += 1
            row["wins" if pnl > 0 else "losses"] += 1
    out = [rows[st] for st in starts]
    active = [r for r in out if r["trades"] or abs(r["pnl"]) > 1e-9]
    best = max(active, key=lambda r: r["pnl"], default=None)
    worst = min(active, key=lambda r: r["pnl"], default=None)
    trades = sum(r["trades"] for r in out)
    wins = sum(r["wins"] for r in out)
    return dict(period=period, rows=out, total=sum(r["pnl"] for r in out), trades=trades, wins=wins,
                losses=trades - wins, active=len(active), green=sum(1 for r in active if r["pnl"] > 0),
                red=sum(1 for r in active if r["pnl"] < 0), avg=(sum(r["pnl"] for r in active) / len(active)) if active else 0.0,
                best=dict(label=best["label"], pnl=best["pnl"]) if best else None,
                worst=dict(label=worst["label"], pnl=worst["pnl"]) if worst else None)
