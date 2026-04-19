import sqlite3
import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Set

import pandas as pd

# Per-process set of db paths whose schema has already been ensured. Avoids
# running CREATE TABLE / CREATE INDEX on every write — previously every
# `write_snapshot`, `write_metric`, and `write_alert` re-opened a connection
# and re-issued the full schema DDL.
_INIT_DONE: Set[str] = set()

# Per-process set of db paths whose journal_mode has been verified as WAL.
# `PRAGMA journal_mode=WAL` is a persistent change in the DB header, so once
# it succeeds for a given file we never need to re-run it — but we do want to
# notice (and log) if the initial switch didn't take effect.
_WAL_DONE: Set[str] = set()


def _ensure_wal(db_path: Path) -> None:
    """Persistently switch the DB to WAL journal mode exactly once.

    Runs in a short-lived autocommit connection so no implicit transaction
    can wrap the PRAGMA. If WAL fails to engage (e.g. another connection
    holds the DB), falls back to DELETE but at least warns to stderr so the
    cause of any subsequent "database is locked" is visible.
    """
    key = str(Path(db_path).resolve())
    if key in _WAL_DONE:
        return
    try:
        setup = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    except sqlite3.Error as exc:  # pragma: no cover - defensive
        print(f"[snapshot_store] WAL setup: connect failed: {exc}", file=sys.stderr)
        return
    try:
        setup.execute("PRAGMA busy_timeout=30000")
        row = setup.execute("PRAGMA journal_mode=WAL").fetchone()
        mode = (row[0] if row else "").lower()
        if mode != "wal":
            print(
                f"[snapshot_store] warning: journal_mode is {mode!r}, expected 'wal' "
                "(another process may be holding the DB)",
                file=sys.stderr,
            )
        setup.execute("PRAGMA synchronous=NORMAL")
    finally:
        setup.close()
    _WAL_DONE.add(key)


def _connect(db_path: Path, timeout: float = 30.0) -> sqlite3.Connection:
    """Open a sqlite connection configured for concurrent LAN access.

    The collector thread and Dash request handlers both hit this DB from
    separate connections. Without WAL mode, any writer blocks every reader
    and sqlite3 raises "database is locked" almost immediately. WAL (set
    once via `_ensure_wal`) lets readers proceed against the last committed
    snapshot while a writer is active. `busy_timeout` makes writer/writer
    collisions wait-and-retry for up to 30s instead of failing fast.
    """
    _ensure_wal(db_path)
    con = sqlite3.connect(db_path, timeout=timeout)
    # busy_timeout is a per-connection setting and must be re-applied on
    # every open. Set it FIRST so it's active for any subsequent PRAGMA
    # or query that might hit contention.
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db(db_path: Path, force: bool = False) -> None:
    key = str(Path(db_path).resolve())
    if not force and key in _INIT_DONE:
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = _connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gex_snapshots (
                ts_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                call_gex REAL NOT NULL,
                put_gex REAL NOT NULL,
                spot_price REAL NOT NULL,
                total_gex REAL NOT NULL,
                abs_gex REAL NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gex_metrics (
                ts_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                net_gex REAL NOT NULL,
                row_count INTEGER NOT NULL,
                unique_strikes INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gex_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                payload TEXT NOT NULL,
                ack INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        # Non-unique indexes — always safe to create, dramatically speed up the
        # WHERE symbol = ? ORDER BY ts_utc queries used by the intraday GEX,
        # hedge backtest, and watchlist panels once history grows past a few
        # thousand rows.
        con.execute("CREATE INDEX IF NOT EXISTS ix_gex_snapshots_symbol_ts ON gex_snapshots(symbol, ts_utc)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_gex_metrics_symbol_ts ON gex_metrics(symbol, ts_utc)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_gex_alerts_symbol_ts ON gex_alerts(symbol, ts_utc)")
        # Unique indexes prevent the collector + UI both writing at the same
        # instant from creating duplicate rows that double-count NetGEX. We
        # try/except because existing databases may already contain dupes
        # from before this constraint existed; in that case the regular
        # indexes above still provide the perf benefit.
        try:
            con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_gex_metrics_ts_symbol ON gex_metrics(ts_utc, symbol)"
            )
        except sqlite3.IntegrityError:
            pass
        try:
            con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_gex_snapshots_ts_symbol_expiry_strike "
                "ON gex_snapshots(ts_utc, symbol, expiry, strike)"
            )
        except sqlite3.IntegrityError:
            pass
        con.commit()
    finally:
        con.close()
    _INIT_DONE.add(key)


def write_snapshot(db_path: Path, df: pd.DataFrame, ts_utc: Optional[pd.Timestamp] = None) -> None:
    if df is None or df.empty:
        return
    init_db(db_path)
    ts = ts_utc or pd.Timestamp.now(tz="UTC")
    out = df.copy()
    out["ts_utc"] = ts.isoformat()
    out["expiry"] = pd.to_datetime(out["expiry"]).dt.strftime("%Y-%m-%d")
    out["total_gex"] = out["call_gex"] + out["put_gex"]
    out["abs_gex"] = out["call_gex"].abs() + out["put_gex"].abs()
    cols = ["ts_utc", "symbol", "expiry", "strike", "call_gex", "put_gex", "spot_price", "total_gex", "abs_gex"]
    rows = list(out[cols].itertuples(index=False, name=None))
    con = _connect(db_path)
    try:
        # INSERT OR IGNORE so the (ts_utc, symbol, expiry, strike) unique index
        # silently drops duplicate rows from concurrent writers instead of
        # aborting the whole batch on IntegrityError.
        con.executemany(
            "INSERT OR IGNORE INTO gex_snapshots "
            "(ts_utc, symbol, expiry, strike, call_gex, put_gex, spot_price, total_gex, abs_gex) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.commit()
    finally:
        con.close()


def write_metric(db_path: Path, symbol: str, net_gex: float, row_count: int, unique_strikes: int, ts_utc: Optional[pd.Timestamp] = None) -> None:
    init_db(db_path)
    ts = ts_utc or pd.Timestamp.now(tz="UTC")
    con = _connect(db_path)
    try:
        con.execute(
            "INSERT OR IGNORE INTO gex_metrics (ts_utc, symbol, net_gex, row_count, unique_strikes) "
            "VALUES (?, ?, ?, ?, ?)",
            (ts.isoformat(), symbol, float(net_gex), int(row_count), int(unique_strikes)),
        )
        con.commit()
    finally:
        con.close()


def load_metric_history(db_path: Path, symbol: str, limit: int = 200) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    con = _connect(db_path)
    try:
        q = """
            SELECT ts_utc, symbol, net_gex, row_count, unique_strikes
            FROM gex_metrics
            WHERE symbol = ?
            ORDER BY ts_utc DESC
            LIMIT ?
        """
        df = pd.read_sql_query(q, con, params=[symbol, int(limit)])
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        return df.sort_values("ts_utc")
    finally:
        con.close()


def load_snapshot_timeseries(db_path: Path, symbol: str, limit: int = 400) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    con = _connect(db_path)
    try:
        q = """
            WITH recent_ts AS (
                SELECT ts_utc
                FROM gex_snapshots
                WHERE symbol = ?
                GROUP BY ts_utc
                ORDER BY ts_utc DESC
                LIMIT ?
            )
            SELECT s.ts_utc, s.symbol, s.strike, s.total_gex, s.abs_gex, s.spot_price
            FROM gex_snapshots s
            INNER JOIN recent_ts r ON r.ts_utc = s.ts_utc
            WHERE s.symbol = ?
            ORDER BY s.ts_utc DESC
        """
        df = pd.read_sql_query(q, con, params=[symbol, int(limit), symbol])
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        return df.sort_values("ts_utc")
    finally:
        con.close()


def load_snapshot_range(db_path: Path, symbol: str, start_ts: Optional[pd.Timestamp], end_ts: Optional[pd.Timestamp]) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    con = _connect(db_path)
    try:
        q = """
            SELECT ts_utc, symbol, expiry, strike, call_gex, put_gex, spot_price, total_gex, abs_gex
            FROM gex_snapshots
            WHERE symbol = ?
        """
        params: List[Any] = [symbol]
        if start_ts is not None:
            q += " AND ts_utc >= ?"
            stamp = pd.Timestamp(start_ts)
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
            params.append(stamp.isoformat())
        if end_ts is not None:
            q += " AND ts_utc <= ?"
            stamp = pd.Timestamp(end_ts)
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
            params.append(stamp.isoformat())
        q += " ORDER BY ts_utc ASC"
        df = pd.read_sql_query(q, con, params=params)
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
        return df
    finally:
        con.close()


def load_snapshot_at(db_path: Path, symbol: str, ts_utc: str) -> pd.DataFrame:
    if not db_path.exists() or not ts_utc:
        return pd.DataFrame()
    con = _connect(db_path)
    try:
        q = """
            SELECT ts_utc, symbol, expiry, strike, call_gex, put_gex, spot_price, total_gex, abs_gex
            FROM gex_snapshots
            WHERE symbol = ? AND ts_utc = ?
            ORDER BY strike ASC
        """
        df = pd.read_sql_query(q, con, params=[symbol, ts_utc])
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
        return df
    finally:
        con.close()


def load_snapshot_timestamps(
    db_path: Path,
    symbol: str,
    start_ts: Optional[pd.Timestamp] = None,
    end_ts: Optional[pd.Timestamp] = None,
    limit: int = 200,
) -> List[str]:
    if not db_path.exists():
        return []
    con = _connect(db_path)
    try:
        q = """
            SELECT ts_utc
            FROM gex_snapshots
            WHERE symbol = ?
        """
        params: List[Any] = [symbol]
        if start_ts is not None:
            q += " AND ts_utc >= ?"
            stamp = pd.Timestamp(start_ts)
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
            params.append(stamp.isoformat())
        if end_ts is not None:
            q += " AND ts_utc <= ?"
            stamp = pd.Timestamp(end_ts)
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
            params.append(stamp.isoformat())
        q += " GROUP BY ts_utc ORDER BY ts_utc DESC LIMIT ?"
        params.append(int(limit))
        rows = con.execute(q, params).fetchall()
        return [row[0] for row in rows if row and row[0]]
    finally:
        con.close()


def write_alert(
    db_path: Path,
    symbol: str,
    rule_id: str,
    severity: str,
    message: str,
    payload: Dict[str, Any],
    ts_utc: Optional[pd.Timestamp] = None,
) -> None:
    init_db(db_path)
    ts = ts_utc or pd.Timestamp.now(tz="UTC")
    row = pd.DataFrame(
        [
            {
                "ts_utc": ts.isoformat(),
                "symbol": symbol,
                "rule_id": rule_id,
                "severity": severity,
                "message": message,
                "payload": json.dumps(payload, default=str),
                "ack": 0,
            }
        ]
    )
    con = _connect(db_path)
    try:
        row.to_sql("gex_alerts", con, if_exists="append", index=False)
    finally:
        con.close()


def load_alerts(
    db_path: Path,
    symbol: Optional[str] = None,
    limit: int = 50,
    unacked_only: bool = False,
    severity_filter: Optional[List[str]] = None,
) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    con = _connect(db_path)
    try:
        q = "SELECT id, ts_utc, symbol, rule_id, severity, message, payload, ack FROM gex_alerts WHERE 1=1"
        params: List[Any] = []
        if symbol:
            q += " AND symbol = ?"
            params.append(symbol)
        if unacked_only:
            q += " AND ack = 0"
        if severity_filter:
            placeholders = ",".join(["?"] * len(severity_filter))
            q += f" AND severity IN ({placeholders})"
            params.extend(severity_filter)
        q += " ORDER BY ts_utc DESC LIMIT ?"
        params.append(int(limit))
        df = pd.read_sql_query(q, con, params=params)
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        return df
    finally:
        con.close()


def ack_alerts(db_path: Path, symbol: Optional[str] = None) -> int:
    if not db_path.exists():
        return 0
    con = _connect(db_path)
    try:
        if symbol:
            res = con.execute("UPDATE gex_alerts SET ack = 1 WHERE symbol = ? AND ack = 0", [symbol])
        else:
            res = con.execute("UPDATE gex_alerts SET ack = 1 WHERE ack = 0")
        con.commit()
        return int(res.rowcount or 0)
    finally:
        con.close()


def get_last_alert_ts(db_path: Path, symbol: str, rule_id: str) -> Optional[pd.Timestamp]:
    if not db_path.exists():
        return None
    con = _connect(db_path)
    try:
        row = con.execute(
            "SELECT ts_utc FROM gex_alerts WHERE symbol = ? AND rule_id = ? ORDER BY ts_utc DESC LIMIT 1",
            [symbol, rule_id],
        ).fetchone()
        if not row or not row[0]:
            return None
        return pd.to_datetime(row[0], utc=True)
    finally:
        con.close()


def load_latest_metric(db_path: Path, symbol: str) -> Optional[Dict[str, Any]]:
    if not db_path.exists():
        return None
    con = _connect(db_path)
    try:
        row = con.execute(
            "SELECT ts_utc, net_gex, row_count, unique_strikes FROM gex_metrics WHERE symbol = ? ORDER BY ts_utc DESC LIMIT 1",
            [symbol],
        ).fetchone()
        if not row:
            return None
        return {
            "ts_utc": pd.to_datetime(row[0], utc=True),
            "net_gex": float(row[1]),
            "row_count": int(row[2]),
            "unique_strikes": int(row[3]),
        }
    finally:
        con.close()
