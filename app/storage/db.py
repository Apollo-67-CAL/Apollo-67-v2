import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Tuple
from urllib.parse import urlparse

DEFAULT_DATABASE_URL = "sqlite:///./apollo67.db"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    source TEXT,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT,
    score REAL NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    decision_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(signal_id) REFERENCES signals(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_created_at ON decisions(created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_signal_id ON decisions(signal_id);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL,
    equity REAL NOT NULL,
    cash REAL NOT NULL,
    gross_exposure REAL NOT NULL DEFAULT 0,
    net_exposure REAL NOT NULL DEFAULT 0,
    heat REAL NOT NULL DEFAULT 0,
    payload TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_as_of ON portfolio_snapshots(as_of);

CREATE TABLE IF NOT EXISTS models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    status TEXT NOT NULL,
    metrics TEXT,
    trained_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(model_name, model_version)
);
CREATE INDEX IF NOT EXISTS idx_models_created_at ON models(created_at);
"""


def _sqlite_path_from_database_url(database_url: str) -> str:
    if database_url == "sqlite:///:memory:":
        return ":memory:"

    parsed = urlparse(database_url)
    if parsed.scheme != "sqlite":
        raise ValueError("Unsupported DATABASE_URL. Only sqlite:// URLs are supported.")

    if parsed.path:
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]
        return path or "apollo67.db"

    return "apollo67.db"


def _connect() -> sqlite3.Connection:
    db_path = _sqlite_path_from_database_url(DATABASE_URL)
    conn = sqlite3.connect(
        db_path,
        timeout=30,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(SCHEMA_SQL)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
            ("v1_initial",),
        )


def check_db_connectivity() -> Tuple[bool, str]:
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1;")
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
