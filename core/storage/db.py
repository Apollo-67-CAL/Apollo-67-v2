import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

DEFAULT_DATABASE_URL = "sqlite:///./apollo67.db"

DB_DRIVER_MARKER = "psycopg3"
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

CREATE TABLE IF NOT EXISTS raw_payloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    provider TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_dataset_received_at
    ON raw_payloads(dataset, received_at);

CREATE TABLE IF NOT EXISTS canonical_instruments (
    instrument_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    currency TEXT NOT NULL,
    is_tradable INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    source_provider TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_canonical_instruments_symbol
    ON canonical_instruments(symbol);

CREATE TABLE IF NOT EXISTS canonical_price_bars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_event TEXT NOT NULL,
    ts_ingest TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    source_provider TEXT NOT NULL,
    quality_flags TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_id, timeframe, ts_event)
);
CREATE INDEX IF NOT EXISTS idx_canonical_price_bars_symbol_time
    ON canonical_price_bars(instrument_id, ts_event);

CREATE TABLE IF NOT EXISTS canonical_corporate_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    factor_or_amount REAL NOT NULL,
    source_provider TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_id, action_type, effective_date)
);

CREATE TABLE IF NOT EXISTS canonical_session_calendars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    venue TEXT NOT NULL,
    session_date TEXT NOT NULL,
    is_open INTEGER NOT NULL,
    session_start TEXT NOT NULL,
    session_end TEXT NOT NULL,
    timezone TEXT NOT NULL,
    source_provider TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (venue, session_date)
);

CREATE TABLE IF NOT EXISTS curated_datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_name, dataset_version)
);

CREATE TABLE IF NOT EXISTS sentiment_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    weight REAL NOT NULL,
    recency_minutes INTEGER NOT NULL,
    bullish_threshold REAL NOT NULL,
    bearish_threshold REAL NOT NULL,
    payload TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sentiment_settings_scope ON sentiment_settings(scope);

CREATE TABLE IF NOT EXISTS sentiment_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    before_payload TEXT,
    after_payload TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sentiment_audit_log_scope_created_at
    ON sentiment_audit_log(scope, created_at);

CREATE TABLE IF NOT EXISTS trading_tactics (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    tags TEXT,
    parameters TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    deleted_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_trading_tactics_updated_at ON trading_tactics(updated_at);
CREATE INDEX IF NOT EXISTS idx_trading_tactics_deleted_at ON trading_tactics(deleted_at);

CREATE TABLE IF NOT EXISTS monitor_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    buy_amount REAL NOT NULL DEFAULT 0,
    buy_price REAL,
    buy_zone_low REAL,
    buy_zone_high REAL,
    status TEXT NOT NULL DEFAULT 'open',
    notes TEXT,
    last_price REAL,
    last_checked_at TEXT,
    pnl_pct REAL,
    max_up_pct REAL,
    max_down_pct REAL
);
CREATE INDEX IF NOT EXISTS idx_monitor_positions_symbol ON monitor_positions(symbol);
CREATE INDEX IF NOT EXISTS idx_monitor_positions_status ON monitor_positions(status);


CREATE TABLE IF NOT EXISTS source_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    agent TEXT NOT NULL,
    source TEXT NOT NULL,
    posts INTEGER NOT NULL,
    positive INTEGER NOT NULL,
    negative INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_source_snapshots_symbol_agent_created
    ON source_snapshots(symbol, agent, created_at);




CREATE TABLE IF NOT EXISTS scanner_source_breakdowns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    scanner_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_scanner_source_breakdowns_symbol_type
    ON scanner_source_breakdowns(symbol, scanner_type, created_at);

CREATE TABLE IF NOT EXISTS scanner_source_controls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scanner_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    display_name TEXT,
    blocked INTEGER NOT NULL DEFAULT 0,
    weight REAL NOT NULL DEFAULT 1.0,
    min_mentions INTEGER NOT NULL DEFAULT 0,
    min_confidence REAL NOT NULL DEFAULT 0.0,
    notes TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_scanner_source_controls_type_key
    ON scanner_source_controls(scanner_type, source_key);

CREATE TABLE IF NOT EXISTS scanner_connectors (
    id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_scanner_connectors_updated_at
    ON scanner_connectors(updated_at);

CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    strategy_group TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_strategies_group_created_at ON strategies(strategy_group, created_at);

CREATE TABLE IF NOT EXISTS monitors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT,
    symbol TEXT NOT NULL,
    entry_price REAL NOT NULL,
    quantity REAL NOT NULL,
    entry_date TEXT NOT NULL,
    notes TEXT,
    last_price REAL,
    pnl REAL,
    pnl_pct REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_monitors_strategy_id ON monitors(strategy_id);
CREATE INDEX IF NOT EXISTS idx_monitors_symbol ON monitors(symbol);

CREATE TABLE IF NOT EXISTS paper_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    notional REAL NOT NULL,
    price REAL NOT NULL,
    status TEXT NOT NULL,
    opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT,
    close_price REAL,
    pnl REAL,
    meta TEXT
);
CREATE INDEX IF NOT EXISTS idx_paper_orders_status_opened_at ON paper_orders(status, opened_at);
CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol ON paper_orders(symbol);

CREATE TABLE IF NOT EXISTS paper_positions (
    symbol TEXT PRIMARY KEY,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_price REAL,
    unrealised_pnl REAL NOT NULL DEFAULT 0,
    realised_pnl REAL NOT NULL DEFAULT 0,
    tactic_id TEXT,
    strategy_id TEXT,
    tactic_label TEXT,
    market TEXT,
    stop_loss REAL,
    take_profit REAL,
    trailing_stop REAL,
    highest_price REAL,
    status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE INDEX IF NOT EXISTS idx_paper_positions_updated_at ON paper_positions(updated_at);

CREATE TABLE IF NOT EXISTS paper_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tactic_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    net_pnl REAL NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL DEFAULT 0,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_paper_runs_tactic_started_at ON paper_runs(tactic_id, started_at);
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

CREATE TABLE IF NOT EXISTS raw_payloads (
    id BIGSERIAL PRIMARY KEY,
    dataset TEXT NOT NULL,
    provider TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_dataset_received_at
    ON raw_payloads(dataset, received_at);

CREATE TABLE IF NOT EXISTS canonical_instruments (
    instrument_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    currency TEXT NOT NULL,
    is_tradable BOOLEAN NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    source_provider TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_canonical_instruments_symbol
    ON canonical_instruments(symbol);

CREATE TABLE IF NOT EXISTS canonical_price_bars (
    id BIGSERIAL PRIMARY KEY,
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    source_provider TEXT NOT NULL,
    quality_flags JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_id, timeframe, ts_event)
);
CREATE INDEX IF NOT EXISTS idx_canonical_price_bars_symbol_time
    ON canonical_price_bars(instrument_id, ts_event);

CREATE TABLE IF NOT EXISTS canonical_corporate_actions (
    id BIGSERIAL PRIMARY KEY,
    instrument_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    effective_date DATE NOT NULL,
    factor_or_amount DOUBLE PRECISION NOT NULL,
    source_provider TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_id, action_type, effective_date)
);

CREATE TABLE IF NOT EXISTS canonical_session_calendars (
    id BIGSERIAL PRIMARY KEY,
    venue TEXT NOT NULL,
    session_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    session_start TIME NOT NULL,
    session_end TIME NOT NULL,
    timezone TEXT NOT NULL,
    source_provider TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (venue, session_date)
);

CREATE TABLE IF NOT EXISTS curated_datasets (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_name, dataset_version)
);

CREATE TABLE IF NOT EXISTS sentiment_settings (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    weight DOUBLE PRECISION NOT NULL,
    recency_minutes INTEGER NOT NULL,
    bullish_threshold DOUBLE PRECISION NOT NULL,
    bearish_threshold DOUBLE PRECISION NOT NULL,
    payload JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sentiment_settings_scope ON sentiment_settings(scope);

CREATE TABLE IF NOT EXISTS sentiment_audit_log (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    before_payload JSONB,
    after_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sentiment_audit_log_scope_created_at
    ON sentiment_audit_log(scope, created_at);

CREATE TABLE IF NOT EXISTS trading_tactics (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    tags JSONB,
    parameters JSONB,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_trading_tactics_updated_at ON trading_tactics(updated_at);
CREATE INDEX IF NOT EXISTS idx_trading_tactics_deleted_at ON trading_tactics(deleted_at);

CREATE TABLE IF NOT EXISTS monitor_positions (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    buy_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_price DOUBLE PRECISION,
    buy_zone_low DOUBLE PRECISION,
    buy_zone_high DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'open',
    notes TEXT,
    last_price DOUBLE PRECISION,
    last_checked_at TIMESTAMPTZ,
    pnl_pct DOUBLE PRECISION,
    max_up_pct DOUBLE PRECISION,
    max_down_pct DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_monitor_positions_symbol ON monitor_positions(symbol);
CREATE INDEX IF NOT EXISTS idx_monitor_positions_status ON monitor_positions(status);

CREATE TABLE IF NOT EXISTS source_snapshots (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    agent TEXT NOT NULL,
    source TEXT NOT NULL,
    posts INTEGER NOT NULL,
    positive INTEGER NOT NULL,
    negative INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_snapshots_symbol_agent_created
    ON source_snapshots(symbol, agent, created_at);

CREATE TABLE IF NOT EXISTS scanner_source_breakdowns (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    scanner_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_scanner_source_breakdowns_symbol_type
    ON scanner_source_breakdowns(symbol, scanner_type, created_at);

CREATE TABLE IF NOT EXISTS scanner_source_controls (
    id BIGSERIAL PRIMARY KEY,
    scanner_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    display_name TEXT,
    blocked INTEGER NOT NULL DEFAULT 0,
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    min_mentions INTEGER NOT NULL DEFAULT 0,
    min_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    notes TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_scanner_source_controls_type_key
    ON scanner_source_controls(scanner_type, source_key);

CREATE TABLE IF NOT EXISTS scanner_connectors (
    id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_scanner_connectors_updated_at
    ON scanner_connectors(updated_at);

CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    strategy_group TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_strategies_group_created_at ON strategies(strategy_group, created_at);

CREATE TABLE IF NOT EXISTS monitors (
    id BIGSERIAL PRIMARY KEY,
    strategy_id TEXT,
    symbol TEXT NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    entry_date TIMESTAMPTZ NOT NULL,
    notes TEXT,
    last_price DOUBLE PRECISION,
    pnl DOUBLE PRECISION,
    pnl_pct DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_monitors_strategy_id ON monitors(strategy_id);
CREATE INDEX IF NOT EXISTS idx_monitors_symbol ON monitors(symbol);

CREATE TABLE IF NOT EXISTS paper_orders (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    notional DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMPTZ,
    close_price DOUBLE PRECISION,
    pnl DOUBLE PRECISION,
    meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_paper_orders_status_opened_at ON paper_orders(status, opened_at);
CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol ON paper_orders(symbol);

CREATE TABLE IF NOT EXISTS paper_positions (
    symbol TEXT PRIMARY KEY,
    qty DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_price DOUBLE PRECISION,
    unrealised_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    realised_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    tactic_id TEXT,
    strategy_id TEXT,
    tactic_label TEXT,
    market TEXT,
    stop_loss DOUBLE PRECISION,
    take_profit DOUBLE PRECISION,
    trailing_stop DOUBLE PRECISION,
    highest_price DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE INDEX IF NOT EXISTS idx_paper_positions_updated_at ON paper_positions(updated_at);

CREATE TABLE IF NOT EXISTS paper_runs (
    id BIGSERIAL PRIMARY KEY,
    tactic_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    net_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    win_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes JSONB
);
CREATE INDEX IF NOT EXISTS idx_paper_runs_tactic_started_at ON paper_runs(tactic_id, started_at);
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
        import psycopg  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres DATABASE_URL configured but psycopg is not installed. "
            "Install psycopg[binary] in this environment."
        ) from exc

    conn = psycopg.connect(database_url)
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
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v2_ingestion_zones",),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v1_initial",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v2_ingestion_zones",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v3_admin_sentiment_tactics",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v4_monitor_positions",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v5_scanner_sources_overlay",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v6_strategies_monitor_dashboard",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v7_source_snapshots_rss",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v8_scanner_connectors_registry",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                ("v9_paper_trading_v1",),
            )
        if backend == "postgres":
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v3_admin_sentiment_tactics",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v4_monitor_positions",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v5_scanner_sources_overlay",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v6_strategies_monitor_dashboard",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v7_source_snapshots_rss",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v8_scanner_connectors_registry",),
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(version)
                VALUES (?)
                ON CONFLICT (version) DO NOTHING
                """,
                ("v9_paper_trading_v1",),
            )
        _ensure_paper_positions_columns(conn)


def _table_columns(conn: DBConnection, table_name: str) -> set[str]:
    table = str(table_name or "").strip().lower()
    if not table:
        return set()
    if conn.backend == "sqlite":
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row.get("name") or "").strip().lower() for row in rows}
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ?
        """,
        (table,),
    ).fetchall()
    return {str(row.get("column_name") or "").strip().lower() for row in rows}


def _ensure_column(conn: DBConnection, table_name: str, column_name: str, column_type_sql: str) -> None:
    cols = _table_columns(conn, table_name)
    col = str(column_name or "").strip().lower()
    if not col or col in cols:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}")


def _ensure_paper_positions_columns(conn: DBConnection) -> None:
    if conn.backend == "sqlite":
        additions = {
            "strategy_id": "TEXT",
            "tactic_label": "TEXT",
            "market": "TEXT",
            "stop_loss": "REAL",
            "take_profit": "REAL",
            "trailing_stop": "REAL",
            "highest_price": "REAL",
            "status": "TEXT DEFAULT 'OPEN'",
        }
    else:
        additions = {
            "strategy_id": "TEXT",
            "tactic_label": "TEXT",
            "market": "TEXT",
            "stop_loss": "DOUBLE PRECISION",
            "take_profit": "DOUBLE PRECISION",
            "trailing_stop": "DOUBLE PRECISION",
            "highest_price": "DOUBLE PRECISION",
            "status": "TEXT DEFAULT 'OPEN'",
        }
    for col_name, col_type in additions.items():
        _ensure_column(conn, "paper_positions", col_name, col_type)
    conn.execute(
        """
        UPDATE paper_positions
        SET status = 'OPEN'
        WHERE status IS NULL OR TRIM(COALESCE(status, '')) = ''
        """
    )
    conn.execute(
        """
        UPDATE paper_positions
        SET strategy_id = tactic_id
        WHERE strategy_id IS NULL OR TRIM(COALESCE(strategy_id, '')) = ''
        """
    )


def check_db_connectivity() -> Tuple[bool, str]:
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1;")
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
