"""Database access.

Every database is configured in Settings: it belongs to an environment and connects with a saved account
(server plus SQL login, or a Microsoft Entra ID user). One account can serve many databases.
"""
from __future__ import annotations

import re
from contextlib import contextmanager
from decimal import Decimal
from typing import Iterator

import pyodbc

from . import entra
from . import store

# The database name goes into an ODBC connection string: allow plain names only, so a request
# can't inject attributes with ';' or '{'.
SAFE_DATABASE_NAME = re.compile(r"^[\w\-. $@#]{1,128}$")
# Server names like 'host', 'host\instance', 'tcp:host,3342'.
SAFE_SERVER = re.compile(r"^[\w\-.,:\\]{1,255}$")


class DbError(Exception):
    """A database or sign-in problem; the message is safe to show in the UI."""


class DbConnectError(DbError):
    """This database couldn't be opened (wrong login, gone, offline, no access)."""


def pick_driver() -> str:
    available = pyodbc.drivers()
    for candidate in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if candidate in available:
            return candidate
    raise DbError(
        "No supported SQL Server ODBC driver found (need 'ODBC Driver 17' or '18 for SQL Server'). "
        f"Drivers visible to pyodbc: {available}"
    )


def clean_db_error(exc: Exception) -> str:
    """Reduces a pyodbc error to the SQL Server message: drops the driver prefixes and the duplicate copy."""
    text = str(exc.args[1]) if len(exc.args) > 1 else str(exc)
    parts: list[str] = []
    for part in text.split("; "):
        part = re.sub(r"^\[\w+\]\s*(\[[^\]]*\])*", "", part).strip()
        part = re.sub(r"\s*\(SQLExecDirectW\)$", "", part)
        if part and part not in parts:
            parts.append(part)
    return " | ".join(parts)


def _braced(value: str) -> str:
    """An ODBC attribute value in braces, with '}' doubled."""
    return "{" + value.replace("}", "}}") + "}"


@contextmanager
def connection(environment: str, database: str) -> Iterator[pyodbc.Connection]:
    """Opens a configured database with its assigned account and closes it afterwards. Failures raise DbConnectError."""
    if not SAFE_DATABASE_NAME.match(database):
        raise DbConnectError(f"Invalid database name '{database}'.")
    info = store.connection_info(environment, database)
    if info is None:
        raise DbConnectError(f"{database} is not set up in environment {environment}. Add it in Settings.")
    if info["server"] is None:
        raise DbConnectError(f"{database} has no account yet. Pick one for it in Settings.")
    conn = _open(info, info["name"])
    try:
        yield conn
    finally:
        conn.close()


def _open(info: dict, database: str) -> pyodbc.Connection:
    """Opens `database` on the account's server. `info` has account, server, username, password and auth."""
    if not SAFE_SERVER.match(info["server"]):
        raise DbConnectError(f"The server of account '{info['account']}' has characters that are not allowed.")
    head = f"Driver={{{pick_driver()}}};Server={info['server']};Database={database};"
    tail = f"Encrypt=yes;TrustServerCertificate={'yes' if info.get('trust_cert') else 'no'};"
    try:
        if info.get("auth") == "entra":      # Entra ID: a token instead of login and password
            token = entra.access_token_struct(info["username"])
            return pyodbc.connect(head + tail, attrs_before={entra.SQL_COPT_SS_ACCESS_TOKEN: token}, autocommit=True)
        return pyodbc.connect(head + f"UID={_braced(info['username'])};PWD={_braced(info['password'])};" + tail, autocommit=True)
    except entra.SignInError as exc:
        raise DbConnectError(f"Could not sign in to {database} with account '{info['account']}': {exc}") from exc
    except pyodbc.Error as exc:
        reason = clean_db_error(exc)
        if "certificate" in reason.lower() and not info.get("trust_cert"):
            reason += " | If you trust this server (for example a local one), edit the account and set “Trust server certificate” to Yes."
        raise DbConnectError(f"Could not connect to {database} with account '{info['account']}': {reason}") from exc


def server_databases(account_id: int) -> tuple[list[str], list[str]]:
    """Databases the login can see, as (names that can be added, names with unusable characters). Skips system and offline databases."""
    info = store.account_login(account_id)
    conn = _open(info, "master")
    try:
        cursor = conn.cursor()
        try:
            try:      # HAS_DBACCESS drops databases the login can't open; fall back if the server lacks it
                rows = query(cursor, "SELECT name FROM sys.databases WHERE database_id > 4 AND state = 0 AND HAS_DBACCESS(name) = 1 ORDER BY name")
            except pyodbc.Error:
                rows = query(cursor, "SELECT name FROM sys.databases WHERE database_id > 4 AND state = 0 ORDER BY name")
        finally:
            cursor.close()
    except pyodbc.Error as exc:
        raise DbConnectError(f"Could not list the databases of account '{info['account']}': {clean_db_error(exc)}") from exc
    finally:
        conn.close()
    names = [row["name"] for row in rows]
    return [n for n in names if SAFE_DATABASE_NAME.match(n)], [n for n in names if not SAFE_DATABASE_NAME.match(n)]


def query(cursor: pyodbc.Cursor, sql: str, *params) -> list[dict]:
    """Runs a SELECT and returns rows as dicts; decimals become floats so they reach the UI as JSON numbers."""
    cursor.execute(sql, *params)
    columns = [col[0] for col in cursor.description]
    return [
        {col: float(v) if isinstance(v, Decimal) else v for col, v in zip(columns, row)}
        for row in cursor.fetchall()
    ]
