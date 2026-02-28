import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

DEFAULT_DATABASE_URL = "sqlite:///./apollo67.db"

SQLITE_SCHEMA_SQL = """
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

POSTGRES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    source TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

CREATE TABLE IF NOT EXISTS signals (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe TEXT,
    score DOUBLE PRECISION NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);

CREATE TABLE IF NOT EXISTS decisions (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT,
    decision_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_decisions_signal_id
        FOREIGN KEY(signal_id)
        REFERENCES signals(id)
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_created_at ON decisions(created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_signal_id ON decisions(signal_id);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id BIGSERIAL PRIMARY KEY,
    as_of TIMESTAMPTZ NOT NULL,
    equity DOUBLE PRECISION NOT NULL,
    cash DOUBLE PRECISION NOT NULL,
    gross_exposure DOUBLE PRECISION NOT NULL DEFAULT 0,
    net_exposure DOUBLE PRECISION NOT NULL DEFAULT 0,
    heat DOUBLE PRECISION NOT NULL DEFAULT 0,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_as_of ON portfolio_snapshots(as_of);

CREATE TABLE IF NOT EXISTS models (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    status TEXT NOT NULL,
    metrics JSONB,
    trained_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(model_name, model_version)
);
CREATE INDEX IF NOT EXISTS idx_models_created_at ON models(created_at);
"""


class QueryResult:
    def __init__(
        self,
        rows: Optional[List[dict]] = None,
        lastrowid: Optional[int] = None,
    ) -> None:
        self._rows = rows or []
        self.lastrowid = lastrowid

    def fetchall(self) -> List[dict]:
        return self._rows


class DBConnection:
    def __init__(self, backend: str, raw_connection: Any) -> None:
        self.backend = backend
        self.raw_connection = raw_connection

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> QueryResult:
        params = tuple(params or ())
        if self.backend == "sqlite":
            cursor = self.raw_connection.execute(sql, params)
            rows = [dict(row) for row in cursor.fetchall()] if cursor.description else []
            return QueryResult(rows=rows, lastrowid=cursor.lastrowid)

        pg_sql = _convert_placeholders(sql)
        stripped = pg_sql.lstrip().upper()
        is_insert = stripped.startswith("INSERT")
        if is_insert and " RETURNING " not in stripped:
            pg_sql = f"{pg_sql.rstrip().rstrip(';')} RETURNING id"

        with self.raw_connection.cursor() as cursor:
            cursor.execute(pg_sql, params)
            rows: List[dict] = []
            lastrowid: Optional[int] = None

            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                fetched = cursor.fetchall()
                rows = [dict(zip(columns, row)) for row in fetched]

            if is_insert:
                if rows and "id" in rows[0]:
                    lastrowid = int(rows[0]["id"])
                elif rows and len(rows[0]) == 1:
                    lastrowid = int(next(iter(rows[0].values())))

            return QueryResult(rows=rows, lastrowid=lastrowid)

    def executescript(self, sql_script: str) -> None:
        if self.backend == "sqlite":
            self.raw_connection.executescript(sql_script)
            return

        for statement in _split_sql_statements(sql_script):
            with self.raw_connection.cursor() as cursor:
                cursor.execute(statement)

    def commit(self) -> None:
        self.raw_connection.commit()

    def rollback(self) -> None:
        self.raw_connection.rollback()

    def close(self) -> None:
        self.raw_connection.close()


def _is_local_mode() -> bool:
    mode = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APOLLO_ENV")
        or "local"
    )
    return mode.lower() in {"local", "dev", "development"}


def _resolve_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    if _is_local_mode():
        return DEFAULT_DATABASE_URL
    raise RuntimeError(
        "DATABASE_URL is required outside local mode. "
        "Set DATABASE_URL to postgresql://... (or sqlite:///... for local only)."
    )


def _parse_backend(database_url: str) -> str:
    if database_url.startswith("postgresql://") or database_url.startswith("postgres://"):
        return "postgres"
    if database_url.startswith("sqlite:///"):
        return "sqlite"
    raise ValueError(
        "Unsupported DATABASE_URL scheme. Use postgresql://, postgres://, or sqlite:///."
    )


def _sqlite_path_from_database_url(database_url: str) -> str:
    if database_url == "sqlite:///:memory:":
        return ":memory:"

    parsed = urlparse(database_url)
    if parsed.scheme != "sqlite":
        raise ValueError("Invalid sqlite DATABASE_URL. Expected sqlite:///path/to/db.sqlite3")

    path = parsed.path or ""
    if path.startswith("/"):
        path = path[1:]
    return path or "apollo67.db"


def _convert_placeholders(sql: str) -> str:
    return sql.replace("?", "%s")


def _split_sql_statements(sql_script: str) -> Iterable[str]:
    for part in sql_script.split(";"):
        statement = part.strip()
        if statement:
            yield statement + ";"


def _connect(database_url: str) -> DBConnection:
    backend = _parse_backend(database_url)
    if backend == "sqlite":
        db_path = _sqlite_path_from_database_url(database_url)
        conn = sqlite3.connect(
            db_path,
            timeout=30,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return DBConnection(backend="sqlite", raw_connection=conn)

    try:
        import psycopg2  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres DATABASE_URL configured but psycopg2 is not installed. "
            "Install psycopg2-binary in this environment."
        ) from exc

    conn = psycopg2.connect(database_url)
    return DBConnection(backend="postgres", raw_connection=conn)


@contextmanager
def get_connection() -> Generator[DBConnection, None, None]:
    database_url = _resolve_database_url()
    conn = _connect(database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    database_url = _resolve_database_url()
    backend = _parse_backend(database_url)
    with get_connection() as conn:
        conn.executescript(POSTGRES_SCHEMA_SQL if backend == "postgres" else SQLITE_SCHEMA_SQL)
        if backend == "postgres":
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v1_initial",),
            )
        else:
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
