"""Trading journal: every trade with the reason it was taken, its stop, target and result, in SQLite, exportable to CSV.

Backtest runs and paper trading write to the same table (mode = 'backtest' | 'paper'), so what you tested and what you
traded can be compared line by line. Add your own notes to any trade afterwards."""
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

COLUMNS = ["id", "run_id", "mode", "strategy", "symbol", "side", "status", "timeframe", "opened_at", "closed_at",
           "entry", "stop", "target", "qty", "risk_usd", "exit", "pnl_usd", "fees_usd", "r_multiple",
           "reason_in", "reason_out", "notes"]


def _when(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M") if ms else "-"


class Journal:
    def __init__(self, path: Path | str = "data/swing-journal.db"):
        path = Path(path)
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS journal ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, mode TEXT NOT NULL, strategy TEXT NOT NULL, "
            "symbol TEXT NOT NULL, side TEXT NOT NULL, status TEXT NOT NULL, timeframe TEXT, opened_at INTEGER, "
            "closed_at INTEGER, entry REAL, stop REAL, target REAL, qty REAL, risk_usd REAL, exit REAL, "
            "pnl_usd REAL, fees_usd REAL, r_multiple REAL, reason_in TEXT, reason_out TEXT, notes TEXT DEFAULT '')")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")

    # ---- small key/value store (paper trading start date, alerts already sent) ------------------
    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self.db.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_state(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO state(key, value) VALUES (?, ?)", (key, value))

    def find(self, *, run_id: str, strategy: str, symbol: str, opened_at: int) -> dict | None:
        row = self.db.execute("SELECT * FROM journal WHERE run_id = ? AND strategy = ? AND symbol = ? AND opened_at = ?",
                              (run_id, strategy, symbol, opened_at)).fetchone()
        return dict(row) if row else None

    # ---- writing -------------------------------------------------------------------------------
    def open_trade(self, *, mode: str, strategy: str, symbol: str, side: str, timeframe: str, opened_at: int,
                   entry: float, stop: float, target: float | None, qty: float, risk_usd: float, reason_in: str,
                   run_id: str = "") -> int:
        cur = self.db.execute(
            "INSERT INTO journal(run_id, mode, strategy, symbol, side, status, timeframe, opened_at, entry, stop, "
            "target, qty, risk_usd, reason_in) VALUES (?,?,?,?,?,'open',?,?,?,?,?,?,?,?)",
            (run_id, mode, strategy, symbol, side, timeframe, opened_at, entry, stop, target, qty, risk_usd, reason_in))
        return int(cur.lastrowid)

    def close_trade(self, trade_id: int, *, closed_at: int, exit: float, pnl_usd: float, fees_usd: float,
                    r_multiple: float, reason_out: str) -> None:
        self.db.execute(
            "UPDATE journal SET status='closed', closed_at=?, exit=?, pnl_usd=?, fees_usd=?, r_multiple=?, "
            "reason_out=? WHERE id=?", (closed_at, exit, pnl_usd, fees_usd, r_multiple, reason_out, trade_id))

    def record(self, trade, *, mode: str = "backtest", run_id: str = "", timeframe: str = "") -> int:
        """A finished trade from the backtester."""
        tid = self.open_trade(mode=mode, strategy=trade.strategy, symbol=trade.symbol, side=trade.side,
                              timeframe=timeframe, opened_at=trade.entry_ts, entry=trade.entry_px, stop=trade.stop,
                              target=trade.target, qty=trade.qty, risk_usd=trade.risk_usd, reason_in=trade.reason_in,
                              run_id=run_id)
        self.close_trade(tid, closed_at=trade.exit_ts, exit=trade.exit_px, pnl_usd=trade.pnl_usd,
                         fees_usd=trade.fees_usd, r_multiple=trade.r_multiple, reason_out=trade.reason_out)
        return tid

    def note(self, trade_id: int, text: str) -> bool:
        cur = self.db.execute(
            "UPDATE journal SET notes = CASE WHEN notes = '' THEN ? ELSE notes || char(10) || ? END WHERE id = ?",
            (text, text, trade_id))
        return cur.rowcount > 0

    # ---- reading -------------------------------------------------------------------------------
    def _where(self, mode, strategy, status, run_id):
        clauses, args = [], []
        for col, val in (("mode", mode), ("strategy", strategy), ("status", status), ("run_id", run_id)):
            if val:
                clauses.append(f"{col} = ?")
                args.append(val)
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", args

    def trades(self, *, mode=None, strategy=None, status=None, run_id=None, limit: int | None = 20) -> list[dict]:
        where, args = self._where(mode, strategy, status, run_id)
        sql = f"SELECT * FROM journal{where} ORDER BY id DESC" + (" LIMIT ?" if limit else "")
        return [dict(r) for r in self.db.execute(sql, args + ([limit] if limit else [])).fetchall()]

    def stats(self, *, mode=None, strategy=None, run_id=None) -> dict:
        closed = self.trades(mode=mode, strategy=strategy, run_id=run_id, status="closed", limit=None)
        opened = self.trades(mode=mode, strategy=strategy, run_id=run_id, status="open", limit=None)
        pnl = [t["pnl_usd"] for t in closed]
        wins = [p for p in pnl if p > 0]
        losses = [-p for p in pnl if p <= 0]
        return dict(
            closed=len(closed), open=len(opened), wins=len(wins), win_rate=len(wins) / len(closed) * 100 if closed else 0.0,
            total_pnl=sum(pnl), avg_r=sum(t["r_multiple"] for t in closed) / len(closed) if closed else 0.0,
            profit_factor=(sum(wins) / sum(losses)) if losses and sum(losses) else (float("inf") if wins else 0.0),
            best=max(pnl, default=0.0), worst=min(pnl, default=0.0))

    def export_csv(self, path: Path | str, **filters) -> int:
        rows = self.trades(limit=None, **filters)[::-1]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(rows)
        return len(rows)


def format_trades(rows: list[dict]) -> str:
    if not rows:
        return "The journal is empty."
    lines = []
    for t in rows:
        head = f"#{t['id']} {t['symbol']} {t['side'].upper()} · {t['strategy']} · {t['mode']}"
        if t["status"] == "open":
            lines.append(f"{head} · OPEN since {_when(t['opened_at'])}\n"
                         f"   in {t['entry']:.6g}  stop {t['stop']:.6g}  target "
                         f"{'-' if t['target'] is None else format(t['target'], '.6g')}  risk ${t['risk_usd']:.2f}\n"
                         f"   why: {t['reason_in']}")
        else:
            lines.append(f"{head} · {_when(t['opened_at'])} → {_when(t['closed_at'])}\n"
                         f"   {t['entry']:.6g} → {t['exit']:.6g}  {t['pnl_usd']:+.2f} USD ({t['r_multiple']:+.2f}R) "
                         f"· out: {t['reason_out']}\n   why in: {t['reason_in']}")
        if t.get("notes"):
            lines.append(f"   note: {t['notes']}")
    return "\n".join(lines)


def format_stats(s: dict) -> str:
    pf = "inf" if s["profit_factor"] == float("inf") else f"{s['profit_factor']:.2f}"
    return (f"{s['closed']} closed, {s['open']} open · win rate {s['win_rate']:.0f}% · P&L {s['total_pnl']:+,.2f} USD · "
            f"expectancy {s['avg_r']:+.2f}R · profit factor {pf} · best {s['best']:+.2f} / worst {s['worst']:+.2f}")
