"""Connections to the app database for store.py: SQLite, MySQL, MariaDB, PostgreSQL or SQL Server.

store.py is written in SQLite-flavoured SQL with `?` placeholders. Conn translates the differences between
engines: placeholders, reserved-word quoting, case-insensitive text, auto-increment keys, new ids, paging and table creation.
Drivers other than sqlite3 are imported lazily.
"""
from __future__ import annotations

import re
import sqlite3
import threading
from functools import lru_cache
from pathlib import Path

from . import dbconfig


class StoreError(Exception):
    """The app database can't be reached or set up. The message is safe to show to users."""


class IntegrityError(Exception):
    """A constraint (usually uniqueness) was violated."""


class Row(tuple):
    """Tuple row that also indexes by column name, like sqlite3.Row."""

    def __new__(cls, names: tuple[str, ...], values):
        row = super().__new__(cls, values)
        row._names = names
        return row

    def keys(self) -> list[str]:
        return list(self._names)

    def __getitem__(self, key):
        return super().__getitem__(self._names.index(key) if isinstance(key, str) else key)


class Cursor:
    def __init__(self, raw):
        self._raw = raw
        self._names: tuple[str, ...] | None = None

    @property
    def rowcount(self) -> int:
        return self._raw.rowcount

    def _row(self, values) -> Row:
        if self._names is None:
            self._names = tuple(d[0] for d in self._raw.description)
        return Row(self._names, values)

    def fetchone(self) -> Row | None:
        values = self._raw.fetchone()
        return None if values is None else self._row(values)

    def fetchall(self) -> list[Row]:
        return [self._row(values) for values in self._raw.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())


# ---- dialects --------------------------------------------------------------------------------

# Schema names that are reserved words in at least one engine. Double quotes work everywhere
# (MySQL/MariaDB sessions get ANSI_QUOTES).
_QUOTED = ("database", "databases", "trigger", "password", "schedule", "options", "indexes", "status", "message",
           "error", "details", "target", "server")
_QUOTE_RE = re.compile(r'(?<![\w"])(' + "|".join(_QUOTED) + r')(?![\w"])')
_LITERAL_RE = re.compile(r"('(?:[^']|'')*')")
_COMMENT_RE = re.compile(r"--[^\n]*")
_TRAILING_BLANKS_RE = re.compile(r"[ \t]+$", re.M)


class Dialect:
    name = ""
    placeholder = "?"
    types: dict[str, str] = {}

    def connect(self, cfg: dbconfig.StoreConfig):
        raise NotImplementedError

    def integrity_errors(self) -> tuple[type, ...]:
        raise NotImplementedError

    def table_exists(self, conn: "Conn", table: str) -> bool:
        raise NotImplementedError

    def prepare(self, conn: "Conn") -> None:
        """Hook for objects the tables depend on."""

    def translate(self, sql: str) -> str:
        return _translate(sql, self.placeholder)

    def insert(self, conn: "Conn", sql: str, params: tuple) -> int:
        cursor = conn.execute(sql, params)
        return cursor._raw.lastrowid

    def limit(self, sql: str, params: tuple, limit: int, offset: int) -> tuple[str, tuple]:
        return sql + " LIMIT ? OFFSET ?", (*params, limit, offset)


@lru_cache(maxsize=512)
def _translate(sql: str, placeholder: str) -> str:
    if "%" in sql:
        raise ValueError("store SQL must not contain '%'")
    parts = _LITERAL_RE.split(sql)
    for i in range(0, len(parts), 2):                  # even parts are outside string literals
        part = _QUOTE_RE.sub(r'"\1"', parts[i])
        part = re.sub(r"\s+COLLATE NOCASE", "", part)  # the schema already makes those columns case-insensitive
        parts[i] = part.replace("?", placeholder)
    return "".join(parts)


class SQLite(Dialect):
    name = "sqlite"
    types = {"ID": "INTEGER PRIMARY KEY AUTOINCREMENT", "KEY": "TEXT", "STR": "TEXT", "LONG": "TEXT", "INT": "INTEGER",
             "REAL": "REAL", "CI": "COLLATE NOCASE", "TAIL": ""}

    def translate(self, sql: str) -> str:
        return sql

    def connect(self, cfg):
        raise AssertionError("SQLite is opened by connect() itself")

    def integrity_errors(self):
        return (sqlite3.IntegrityError,)


class MySQL(Dialect):
    name = "mysql"
    placeholder = "%s"
    types = {"ID": "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY", "KEY": "VARCHAR(128)", "STR": "VARCHAR(255)", "LONG": "LONGTEXT",
             "INT": "BIGINT", "REAL": "DOUBLE", "CI": "COLLATE utf8mb4_general_ci", "TAIL": "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"}

    def connect(self, cfg):
        import pymysql
        return pymysql.connect(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=cfg.database,
                               charset="utf8mb4", autocommit=False, connect_timeout=10,
                               init_command="SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')")

    def integrity_errors(self):
        import pymysql
        return (pymysql.err.IntegrityError,)

    def table_exists(self, conn, table):
        return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
                            (table,)).fetchone() is not None


class Postgres(Dialect):
    name = "postgresql"
    placeholder = "%s"
    COLLATION = "index_manager_nocase"
    types = {"ID": "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY", "KEY": "VARCHAR(128)", "STR": "VARCHAR(255)", "LONG": "TEXT",
             "INT": "BIGINT", "REAL": "DOUBLE PRECISION", "CI": f'COLLATE "{COLLATION}"', "TAIL": ""}

    def connect(self, cfg):
        import psycopg
        return psycopg.connect(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=cfg.database,
                               connect_timeout=10, autocommit=False)

    def integrity_errors(self):
        import psycopg
        return (psycopg.IntegrityError,)

    def table_exists(self, conn, table):
        return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
                            (table,)).fetchone() is not None

    def prepare(self, conn):
        # Case-insensitive collation for unique names: needs a nondeterministic ICU collation (PostgreSQL 12+ built with ICU).
        conn.execute(f"CREATE COLLATION IF NOT EXISTS {self.COLLATION} (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")

    def insert(self, conn, sql, params):
        return conn.execute(sql + " RETURNING id", params).fetchone()[0]


class SQLServer(Dialect):
    name = "mssql"
    types = {"ID": "BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY", "KEY": "NVARCHAR(128)", "STR": "NVARCHAR(255)", "LONG": "NVARCHAR(MAX)",
             "INT": "BIGINT", "REAL": "FLOAT", "CI": "COLLATE Latin1_General_100_CI_AS", "TAIL": ""}

    def connect(self, cfg):
        import pyodbc
        driver = next((d for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server") if d in pyodbc.drivers()), None)
        if driver is None:
            raise StoreError("SQL Server needs the 'ODBC Driver 17 or 18 for SQL Server'; pyodbc sees none of them.")
        server = cfg.host + (f",{cfg.port}" if cfg.port else "")
        braced = lambda value: "{" + value.replace("}", "}}") + "}"
        return pyodbc.connect(f"Driver={{{driver}}};Server={server};Database={braced(cfg.database)};UID={braced(cfg.user)};"
                              f"PWD={braced(cfg.password)};Encrypt=yes;TrustServerCertificate={'yes' if cfg.trust_cert else 'no'};",
                              autocommit=False, timeout=10)

    def integrity_errors(self):
        import pyodbc
        return (pyodbc.IntegrityError,)

    def table_exists(self, conn, table):
        return conn.execute("SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID()", (table,)).fetchone() is not None

    def insert(self, conn, sql, params):
        conn.execute(sql, params)
        return int(conn.execute("SELECT @@IDENTITY").fetchone()[0])      # @@IDENTITY, not SCOPE_IDENTITY(): each execute is its own batch and scope

    def limit(self, sql, params, limit, offset):
        return sql + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", (*params, offset, limit)


_DIALECTS = {"sqlite": SQLite(), "mysql": MySQL(), "mariadb": MySQL(), "postgresql": Postgres(), "mssql": SQLServer()}

_MISSING_DRIVER = {"mysql": "PyMySQL", "mariadb": "PyMySQL", "postgresql": "psycopg[binary]", "mssql": "pyodbc"}


# ---- connections -----------------------------------------------------------------------------

class Conn:
    """Connection wrapper used by store.py. `with closing(conn)` releases it; `with conn:` is a transaction."""

    def __init__(self, raw, dialect: Dialect, pooled: bool):
        self._raw, self._dialect, self._pooled = raw, dialect, pooled
        self._errors = dialect.integrity_errors()

    def execute(self, sql: str, params: tuple = ()) -> Cursor:
        cursor = self._raw.cursor()
        try:
            cursor.execute(self._dialect.translate(sql), params) if params else cursor.execute(self._dialect.translate(sql))
        except self._errors as exc:
            raise IntegrityError(str(exc)) from exc
        return Cursor(cursor)

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        cursor = self._raw.cursor()
        try:
            cursor.executemany(self._dialect.translate(sql), rows)
        except self._errors as exc:
            raise IntegrityError(str(exc)) from exc

    def insert(self, sql: str, params: tuple = ()) -> int:
        """Runs an INSERT into a table with an `id` key; returns the new id."""
        try:
            return self._dialect.insert(self, sql, params)
        except self._errors as exc:
            raise IntegrityError(str(exc)) from exc

    def paged(self, sql: str, params: tuple, limit: int, offset: int = 0) -> Cursor:
        """Runs `sql` for one page. It must have an ORDER BY."""
        return self.execute(*self._dialect.limit(sql, params, limit, offset))

    def executescript(self, script: str) -> None:
        self._raw.executescript(script)           # sqlite3 only

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def __enter__(self) -> "Conn":
        return self

    def __exit__(self, exc_type, *_):
        self.commit() if exc_type is None else self.rollback()

    def close(self) -> None:
        raw, self._raw = self._raw, None
        if raw is None:
            return
        if self._pooled:
            _release(raw)
        else:
            raw.close()


_cfg: dbconfig.StoreConfig | None = None
_idle: list = []
_lock = threading.Lock()
MAX_IDLE = 4


def configure(cfg: dbconfig.StoreConfig) -> None:
    """Sets the app database config and drops idle pooled connections. Default: dbconfig.load() on first use."""
    global _cfg
    with _lock:
        _cfg = cfg
        stale, _idle[:] = list(_idle), []
    for raw in stale:
        _quiet_close(raw)


def config() -> dbconfig.StoreConfig:
    if _cfg is None:
        configure(dbconfig.load())              # raises dbconfig.ConfigError
    return _cfg


def identity(sqlite_path: Path) -> object:
    """Key identifying the database connect() opens; used to track which have had their tables set up."""
    cfg = config()
    return ("sqlite", sqlite_path) if cfg.engine == "sqlite" else cfg


def dialect() -> Dialect:
    return _DIALECTS[config().engine]


def _quiet_close(raw) -> None:
    try:
        raw.close()
    except Exception:
        pass


def _alive(raw) -> bool:
    try:
        cursor = raw.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        raw.rollback()
        return True
    except Exception:
        return False


def _release(raw) -> None:
    try:
        raw.rollback()
    except Exception:
        _quiet_close(raw)
        return
    with _lock:
        if len(_idle) < MAX_IDLE:
            _idle.append(raw)
            return
    _quiet_close(raw)


def connect(sqlite_path: Path) -> Conn:
    cfg, d = config(), dialect()
    if cfg.engine == "sqlite":
        return Conn(sqlite3.connect(sqlite_path, timeout=10), d, pooled=False)
    while True:
        with _lock:
            raw = _idle.pop() if _idle else None
        if raw is None:
            break
        if _alive(raw):
            return Conn(raw, d, pooled=True)
        _quiet_close(raw)
    try:
        return Conn(d.connect(cfg), d, pooled=True)
    except StoreError:
        raise
    except ImportError as exc:
        raise StoreError(f"The {cfg.engine} driver is not installed. Run: pip install {_MISSING_DRIVER[cfg.engine]}") from exc
    except Exception as exc:
        message = _message(exc)
        hint = (" If the server uses a self-signed certificate, set APP_DB_TRUST_CERT=yes (--db-trust-cert)."
                if cfg.engine == "mssql" and not cfg.trust_cert and "certificate" in message.lower() else "")
        raise StoreError(f"Could not connect to the app database ({cfg.describe()}): {message}{hint}") from exc


def _message(exc: Exception) -> str:
    text = str(exc.args[1]) if len(exc.args) > 1 and isinstance(exc.args[1], str) else str(exc)
    return " ".join(text.split())


# ---- tables ----------------------------------------------------------------------------------

def render_schema(template: str, d: Dialect) -> list[str]:
    """Splits store.SCHEMA into statements using this engine's column types."""
    script = _TRAILING_BLANKS_RE.sub("", _COMMENT_RE.sub("", template)).format(**d.types)
    return [s.strip() for s in script.split(";") if s.strip()]


def create_tables(conn: Conn, template: str) -> None:
    """Creates missing tables and their indexes; existing tables are untouched."""
    d = conn._dialect
    statements = render_schema(template, d)
    if d.name == "sqlite":
        conn.executescript(";\n".join(statements) + ";")
        return
    try:
        d.prepare(conn)
        made: set[str] = set()
        for statement in statements:
            plain = statement.replace(" IF NOT EXISTS", "", 1)
            if plain.startswith("CREATE TABLE"):
                table = re.match(r"CREATE TABLE (\w+)", plain).group(1)
                if d.table_exists(conn, table):
                    continue
                made.add(table)
            elif re.match(r"CREATE INDEX \w+ ON (\w+)", plain).group(1) not in made:
                continue
            conn.execute(plain)
        conn.commit()
    except IntegrityError:
        raise
    except Exception as exc:
        conn.rollback()
        raise StoreError(f"Could not set up the tables in the app database ({config().describe()}). It needs a login that may create "
                         f"tables in it: {_message(exc)}") from exc
