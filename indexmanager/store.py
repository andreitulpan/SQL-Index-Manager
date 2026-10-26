"""App storage: settings, manual scan results, job history and rules.

SQLite by default, or a database server if configured (see dbconfig.py); storedb.py hides the engine differences.
The UI only reads from here. A target server is contacted only on a scan, rebuild or rule run.
Only the latest result per (environment, database, kind) is kept.
"""
from __future__ import annotations

import json
import re
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from . import log
from . import passwords
from . import storedb
from .storedb import IntegrityError

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "index_manager.sqlite3"


class NotFound(Exception):
    """The environment, account, database or rule doesn't exist."""


class Conflict(Exception):
    """The change clashes with existing data (name taken, or still in use)."""


SCHEMA = """
-- {ID} {KEY} {STR} {LONG} {INT} {REAL} {CI} {TAIL}: the column types of the engine in use, see storedb.py. Comments are dropped.
CREATE TABLE IF NOT EXISTS machines (
    id          {ID},
    name        {KEY} {CI} NOT NULL UNIQUE,   -- where environments run; an environment is on one machine, a machine can have several
    description {STR}                          -- optional
) {TAIL};
CREATE TABLE IF NOT EXISTS environments (
    name       {KEY} {CI} NOT NULL PRIMARY KEY,
    machine_id {INT}                           -- NULL only in what was made before machines existed: it is asked for when the environment is edited
) {TAIL};
CREATE TABLE IF NOT EXISTS accounts (
    id          {ID},
    environment {KEY} {CI} NOT NULL,   -- an account belongs to one environment, like its databases
    name        {KEY} {CI} NOT NULL,
    server      {STR} NOT NULL,        -- e.g. myserver.public.abc123.database.windows.net,3342
    username    {STR} NOT NULL,        -- a SQL login, or the Entra ID e-mail; the account is not tied to any one database
    password    {LONG} NOT NULL,       -- stored as typed; empty for Entra ID
    auth        {STR} NOT NULL DEFAULT 'sql',   -- 'sql' (login and password) or 'entra' (browser sign-in, see entra.py)
    trust_cert  {INT} NOT NULL DEFAULT 0,       -- 1: accept the server's certificate even if no trusted authority issued it (a local server)
    UNIQUE (environment, name)
) {TAIL};
CREATE TABLE IF NOT EXISTS databases (
    id          {ID},
    environment {KEY} {CI} NOT NULL,
    name        {KEY} {CI} NOT NULL,
    account_id  {INT},              -- NULL only in what was migrated: such a database stays hidden until it gets an account
    UNIQUE (environment, name)
) {TAIL};
CREATE TABLE IF NOT EXISTS db_catalog (
    id          {ID},
    name        {KEY} {CI} NOT NULL UNIQUE,   -- the list on the Databases page: a database has to be in it before an environment can use it
    description {STR}                          -- optional
) {TAIL};
CREATE TABLE IF NOT EXISTS snapshots (
    environment {KEY} NOT NULL,
    database    {KEY} NOT NULL,
    kind        {KEY} NOT NULL,          -- 'indexes' or 'fragmentation'
    taken_at    {STR} NOT NULL,          -- UTC, ISO 8601
    taken_by    {STR} NOT NULL,          -- the UI user who pressed the scan button
    payload     {LONG} NOT NULL,          -- JSON
    PRIMARY KEY (environment, database, kind)
) {TAIL};
CREATE TABLE IF NOT EXISTS jobs (
    id          {ID},
    kind        {KEY} NOT NULL,       -- 'indexes' or 'fragmentation'
    environment {KEY} NOT NULL,
    database    {KEY} NOT NULL,
    target      {STR},                -- what the job acted on, e.g. 'dbo.Orders.IX_Orders_Date' (rebuilds)
    details     {LONG},                -- the options it ran with, e.g. 'ONLINE = ON, MAXDOP = 0, ...'
    started_by  {STR} NOT NULL,       -- the UI user who pressed the button
    started_at  {STR} NOT NULL,       -- UTC, ISO 8601
    ended_at    {STR},                -- NULL while running
    duration_ms {INT},
    status      {STR} NOT NULL,       -- 'running', 'success', 'failed' or 'interrupted'
    error       {LONG}
) {TAIL};
CREATE TABLE IF NOT EXISTS rules (
    id                {ID},
    name              {KEY} {CI} NOT NULL UNIQUE,
    kind              {STR} NOT NULL DEFAULT 'rebuild',   -- 'rebuild' (rebuilds the listed indexes) or 'scan' (only refreshes the stored scans)
    environment       {KEY} NOT NULL,
    databases         {LONG} NOT NULL,   -- JSON list of the databases it runs in
    schedule          {LONG} NOT NULL,   -- JSON, see schedule.py
    indexes           {LONG} NOT NULL,   -- JSON list of {schema_name, table_name, index_name}
    min_fragmentation {REAL},           -- only rebuild indexes fragmented above this %, NULL = all of them
    options           {LONG} NOT NULL,   -- JSON: online, data_compression, maxdop, sort_in_tempdb
    execution         {LONG} NOT NULL,   -- JSON: mode 'serial' | 'parallel', max_parallel
    priority          {INT} NOT NULL DEFAULT 0,   -- 1 = first: among the rules of one machine, the one with the lower number goes first (kept 1..n by _renumber)
    created_by        {STR} NOT NULL,
    created_at        {STR} NOT NULL,
    updated_by        {STR} NOT NULL,
    updated_at        {STR} NOT NULL
) {TAIL};
CREATE TABLE IF NOT EXISTS rule_runs (
    id          {ID},
    rule_id     {INT} NOT NULL,
    rule_name   {STR} NOT NULL,       -- as it was named when the run started
    trigger     {STR} NOT NULL,       -- 'schedule' or 'manual'
    started_by  {STR} NOT NULL,       -- the UI user, or 'Scheduler'
    started_at  {STR} NOT NULL,       -- UTC, ISO 8601
    ended_at    {STR},
    duration_ms {INT},
    status      {STR} NOT NULL,       -- 'running', 'success', 'partial', 'failed', 'stopped', 'skipped' or 'interrupted'
    error       {LONG}
) {TAIL};
CREATE INDEX IF NOT EXISTS rule_runs_by_rule ON rule_runs (rule_id, id DESC);
CREATE TABLE IF NOT EXISTS rule_run_items (
    id          {ID},
    run_id      {INT} NOT NULL,
    seq         {INT} NOT NULL,
    database_name {STR},
    schema_name {KEY} NOT NULL,
    table_name  {KEY} NOT NULL,
    index_name  {KEY} NOT NULL,
    status      {STR} NOT NULL,       -- 'pending', 'running', 'success', 'failed', 'skipped' or 'interrupted'
    message     {LONG},                -- why it was skipped, or what failed
    page_count  {INT},
    frag_before {REAL},
    frag_after  {REAL},
    options     {LONG},                -- the WITH options it ran with
    started_at  {STR},
    ended_at    {STR},
    duration_ms {INT}
) {TAIL};
CREATE INDEX IF NOT EXISTS rule_run_items_by_run ON rule_run_items (run_id, seq);
CREATE TABLE IF NOT EXISTS users (
    id            {ID},
    username      {KEY} {CI} NOT NULL UNIQUE,
    role          {STR} NOT NULL,       -- 'admin' or 'user'
    password_hash {STR} NOT NULL,       -- see passwords.py; the password itself is never stored
    created_at    {STR} NOT NULL,       -- UTC, ISO 8601
    must_change_password {INT} NOT NULL DEFAULT 0,  -- 1 until the user has replaced the password an administrator gave them
    timezone      {STR}                 -- the user's IANA time zone, NULL: not chosen
) {TAIL};
"""

_init_lock = threading.Lock()
_ready: set[object] = set()


def _connect() -> storedb.Conn:
    """Opens a connection; the first one also creates missing tables.

    Raises dbconfig.ConfigError (bad settings) or storedb.StoreError (can't connect or set up).
    """
    sqlite = storedb.config().engine == "sqlite"
    if sqlite:
        DB_PATH.parent.mkdir(exist_ok=True)
    key = storedb.identity(DB_PATH)
    first = key not in _ready or (sqlite and not DB_PATH.exists())
    conn = storedb.connect(DB_PATH)
    if first:
        try:
            with _init_lock:
                storedb.create_tables(conn, SCHEMA)
                conn.commit()
                _ready.add(key)
        except BaseException:
            conn.close()
            raise
    return conn


def check() -> None:
    """Opens and sets up the database; called at startup so bad settings fail fast."""
    with closing(_connect()):
        pass


def _json_default(value):
    return value.isoformat()  # datetimes from DMVs


# ---- users -----------------------------------------------------------------------------------

ADMIN_USERNAME = "admin"


def ensure_admin() -> str | None:
    """Creates the admin if none exists and returns its random password (shown only once), else None.

    Safe against two instances starting together: the unique username makes the second insert fail.
    """
    # Check and insert are separate transactions on purpose: in one, the check's locks deadlock
    # two simultaneous starts on MySQL / MariaDB instead of one failing cleanly.
    with closing(_connect()) as conn:
        if conn.execute("SELECT 1 FROM users WHERE role = 'admin'").fetchone():
            return None
    password = passwords.generate_password()
    with closing(_connect()) as conn, conn:
        try:
            conn.execute("INSERT INTO users (username, role, password_hash, created_at, must_change_password)"
                         " VALUES (?, 'admin', ?, ?, 1)",
                         (ADMIN_USERNAME, passwords.hash_password(password), datetime.now(timezone.utc).isoformat(timespec="seconds")))
        except IntegrityError:
            return None
        return password


def get_user(username: str) -> dict | None:
    """The public fields of the user (never the hash), or None."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT username, role, created_at, must_change_password, timezone FROM users WHERE username = ?",
                           (username,)).fetchone()
    return _public(row) if row else None


def _public(row) -> dict:
    return {"username": row["username"], "role": row["role"], "created_at": row["created_at"],
            "must_change_password": bool(row["must_change_password"]), "timezone": row["timezone"]}


def set_timezone(username: str, zone: str | None) -> None:
    """Saves the user's IANA time zone (None clears it). The caller validates the name."""
    with closing(_connect()) as conn, conn:
        if conn.execute("UPDATE users SET timezone = ? WHERE username = ?", (zone, username)).rowcount == 0:
            raise NotFound(f"There is no user called {username}.")


def list_users() -> list[dict]:
    with closing(_connect()) as conn:
        rows = conn.execute("SELECT username, role, created_at, must_change_password, timezone FROM users"
                            " ORDER BY username COLLATE NOCASE").fetchall()
    return [{**_public(row), "system": row["username"].lower() == ADMIN_USERNAME} for row in rows]


def check_credentials(username: str, password: str) -> dict | None:
    """The user as get_user returns it if the password is right, else None.

    An unknown name takes as long as a wrong password.
    """
    with closing(_connect()) as conn:
        row = conn.execute("SELECT username, role, created_at, must_change_password, timezone, password_hash FROM users"
                           " WHERE username = ?", (username,)).fetchone()
    if row is None:
        passwords.verify_nothing(password)
        return None
    return _public(row) if passwords.verify_password(password, row["password_hash"]) else None


def create_user(username: str) -> str:
    """Adds a non-admin user and returns its random password (shown only once).

    The user must change it at first sign-in. Raises Conflict if the name is taken.
    """
    password = passwords.generate_password()
    with closing(_connect()) as conn, conn:
        try:
            conn.execute("INSERT INTO users (username, role, password_hash, created_at, must_change_password)"
                         " VALUES (?, 'user', ?, ?, 1)",
                         (username, passwords.hash_password(password), datetime.now(timezone.utc).isoformat(timespec="seconds")))
        except IntegrityError:
            raise Conflict(f"There is already a user called {username}.") from None
    return password


def set_role(username: str, role: str) -> None:
    """Sets the user's role. The built-in admin account can't be demoted (Conflict); NotFound if unknown."""
    if role not in ("admin", "user"):
        raise ValueError(role)
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise NotFound(f"There is no user called {username}.")
        if row["username"].lower() == ADMIN_USERNAME:
            raise Conflict("The administrator the system created always stays an administrator.")
        conn.execute("UPDATE users SET role = ? WHERE username = ?", (role, row["username"]))


def reset_password(username: str) -> tuple[str, str]:
    """Sets a new random password the user must change at next sign-in; returns (saved name, password).

    Raises NotFound, or Conflict for the built-in admin, whose password only its owner can change.
    """
    password = passwords.generate_password()
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise NotFound(f"There is no user called {username}.")
        if row["username"].lower() == ADMIN_USERNAME:
            raise Conflict("Only the administrator the system created can change its password.")
        conn.execute("UPDATE users SET password_hash = ?, must_change_password = 1 WHERE username = ?",
                     (passwords.hash_password(password), row["username"]))
    return row["username"], password


def delete_user(username: str) -> str:
    """Deletes the user and returns the saved name. Job history keeps their name.

    Raises NotFound, or Conflict for the built-in admin.
    """
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise NotFound(f"There is no user called {username}.")
        if row["username"].lower() == ADMIN_USERNAME:
            raise Conflict("The administrator the system created can't be removed.")
        conn.execute("DELETE FROM users WHERE username = ?", (row["username"],))
    return row["username"]


def change_password(username: str, current: str, new: str) -> None:
    """Changes the password and clears must_change_password.

    Raises NotFound, or Conflict if the current password is wrong or the new one is identical.
    """
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT username, password_hash FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise NotFound(f"There is no user called {username}.")
        if not passwords.verify_password(current, row["password_hash"]):
            raise Conflict("The current password is wrong.")
        if new == current:
            raise Conflict("Choose a password different from the current one.")
        conn.execute("UPDATE users SET password_hash = ?, must_change_password = 0 WHERE username = ?",
                     (passwords.hash_password(new), row["username"]))


# ---- environments ----------------------------------------------------------------------------

def list_environments() -> list[dict]:
    """{name, machine_id, machine, n_databases, n_accounts}; machine is None for legacy environments."""
    with closing(_connect()) as conn:
        return [{"name": name, "machine_id": machine_id, "machine": machine, "n_databases": n, "n_accounts": a}
                for name, machine_id, machine, n, a in conn.execute(
            "SELECT e.name, m.id, m.name, (SELECT COUNT(*) FROM databases d WHERE d.environment = e.name),"
            " (SELECT COUNT(*) FROM accounts a WHERE a.environment = e.name)"
            " FROM environments e LEFT JOIN machines m ON m.id = e.machine_id ORDER BY e.name COLLATE NOCASE")]


def get_environment(name: str) -> str | None:
    """The saved spelling of the name (lookup ignores case), or None."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT name FROM environments WHERE name = ?", (name,)).fetchone()
    return row[0] if row else None


def _check_machine(conn: storedb.Conn, machine_id: int) -> None:
    if conn.execute("SELECT 1 FROM machines WHERE id = ?", (machine_id,)).fetchone() is None:
        raise NotFound("That machine no longer exists.")


def add_environment(name: str, machine_id: int | None = None) -> None:
    """The API always passes a machine; None is only for legacy environments."""
    with closing(_connect()) as conn, conn:
        if machine_id is not None:
            _check_machine(conn, machine_id)
        try:
            conn.execute("INSERT INTO environments (name, machine_id) VALUES (?, ?)", (name, machine_id))
        except IntegrityError:
            raise Conflict(f"There is already an environment called '{name}'.") from None


def rename_environment(old: str, new: str, machine_id: int | None = None) -> None:
    """Renames the environment in accounts, databases, scans, rules and job history.

    `machine_id` moves it to another machine; None keeps the current one.
    """
    with closing(_connect()) as conn, conn:
        found = conn.execute("SELECT name FROM environments WHERE name = ?", (old,)).fetchone()
        if found is None:
            raise NotFound(f"There is no environment '{old}'.")
        if old.lower() != new.lower() and conn.execute("SELECT 1 FROM environments WHERE name = ?", (new,)).fetchone():
            raise Conflict(f"There is already an environment called '{new}'.")
        if machine_id is not None:
            _check_machine(conn, machine_id)
            conn.execute("UPDATE environments SET machine_id = ? WHERE name = ?", (machine_id, old))
        conn.execute("UPDATE environments SET name = ? WHERE name = ?", (new, old))
        for table in ("accounts", "databases", "snapshots", "jobs", "rules"):
            conn.execute(f"UPDATE {table} SET environment = ? WHERE environment = ?", (new, found[0]))
        _renumber(conn)       # rules may have changed machine


def delete_environment(name: str) -> None:
    with closing(_connect()) as conn, conn:
        found = conn.execute("SELECT name FROM environments WHERE name = ?", (name,)).fetchone()
        if found is None:
            raise NotFound(f"There is no environment '{name}'.")
        n = conn.execute("SELECT COUNT(*) FROM databases WHERE environment = ?", (name,)).fetchone()[0]
        if n:
            raise Conflict(f"'{found[0]}' still has {n} database{'s' if n != 1 else ''}. Remove them first.")
        conn.execute("DELETE FROM accounts WHERE environment = ?", (name,))     # no databases left, so none use them
        conn.execute("DELETE FROM environments WHERE name = ?", (name,))


# ---- machines --------------------------------------------------------------------------------

def list_machines() -> list[dict]:
    """{id, name, description, environments}; environments are the ones on the machine."""
    with closing(_connect()) as conn:
        machines = conn.execute("SELECT id, name, description FROM machines ORDER BY name COLLATE NOCASE").fetchall()
        used = conn.execute("SELECT machine_id, name FROM environments WHERE machine_id IS NOT NULL").fetchall()
    on: dict[int, list[str]] = {}
    for machine_id, environment in used:
        on.setdefault(machine_id, []).append(environment)
    return [{"id": i, "name": name, "description": description or "", "environments": sorted(on.get(i, []), key=str.lower)}
            for i, name, description in machines]


def add_machine(name: str, description: str | None) -> int:
    with closing(_connect()) as conn, conn:
        try:
            return conn.insert("INSERT INTO machines (name, description) VALUES (?, ?)", (name, description or None))
        except IntegrityError:
            raise Conflict(f"There is already a machine called '{name}'.") from None


def update_machine(machine_id: int, name: str, description: str | None) -> None:
    with closing(_connect()) as conn, conn:
        _check_machine(conn, machine_id)
        try:
            conn.execute("UPDATE machines SET name = ?, description = ? WHERE id = ?", (name, description or None, machine_id))
        except IntegrityError:
            raise Conflict(f"There is already a machine called '{name}'.") from None


def delete_machine(machine_id: int) -> None:
    """Raises Conflict while an environment is on the machine."""
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT name FROM machines WHERE id = ?", (machine_id,)).fetchone()
        if row is None:
            raise NotFound("That machine no longer exists.")
        used = sorted((e for (e,) in conn.execute("SELECT name FROM environments WHERE machine_id = ?", (machine_id,))), key=str.lower)
        if used:
            raise Conflict(f"{row[0]} still has {'the environment' if len(used) == 1 else 'the environments'} {', '.join(used)}. "
                           "Move or remove " + ("it" if len(used) == 1 else "them") + " first.")
        conn.execute("DELETE FROM machines WHERE id = ?", (machine_id,))


# ---- accounts --------------------------------------------------------------------------------

def _account(row: storedb.Row) -> dict:
    return {"id": row["id"], "environment": row["environment"], "name": row["name"], "server": row["server"], "username": row["username"],
            "auth": row["auth"], "trust_cert": bool(row["trust_cert"]), "has_password": bool(row["password"]), "n_databases": row["n_databases"]}   # never expose the password


_ACCOUNT_SQL = "SELECT a.*, (SELECT COUNT(*) FROM databases d WHERE d.account_id = a.id) AS n_databases FROM accounts a"


def list_accounts(environment: str | None = None) -> list[dict]:
    with closing(_connect()) as conn:
        if environment is None:
            rows = conn.execute(_ACCOUNT_SQL + " ORDER BY a.environment COLLATE NOCASE, a.name COLLATE NOCASE")
        else:
            rows = conn.execute(_ACCOUNT_SQL + " WHERE a.environment = ? ORDER BY a.name COLLATE NOCASE", (environment,))
        return [_account(r) for r in rows]


def get_account(account_id: int) -> dict | None:
    with closing(_connect()) as conn:
        row = conn.execute(_ACCOUNT_SQL + " WHERE a.id = ?", (account_id,)).fetchone()
    return _account(row) if row else None


def add_account(environment: str, name: str, server: str, username: str, password: str, auth: str = "sql", trust_cert: bool = False) -> int:
    with closing(_connect()) as conn, conn:
        env = conn.execute("SELECT name FROM environments WHERE name = ?", (environment,)).fetchone()
        if env is None:
            raise NotFound(f"There is no environment '{environment}'. Create it first.")
        try:
            return conn.insert("INSERT INTO accounts (environment, name, server, username, password, auth, trust_cert) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (env[0], name, server, username, password if auth == "sql" else "", auth, int(trust_cert)))
        except IntegrityError:
            raise Conflict(f"There is already an account called '{name}' in {env[0]}.") from None


def update_account(account_id: int, name: str, server: str, username: str, password: str | None, auth: str = "sql", trust_cert: bool = False) -> None:
    """Updates the account in place. A None or empty `password` keeps the saved one; Entra accounts store none."""
    with closing(_connect()) as conn, conn:
        if conn.execute("SELECT 1 FROM accounts WHERE id = ?", (account_id,)).fetchone() is None:
            raise NotFound("That account no longer exists.")
        try:
            conn.execute("UPDATE accounts SET name = ?, server = ?, username = ?, auth = ?, trust_cert = ? WHERE id = ?",
                         (name, server, username, auth, int(trust_cert), account_id))
        except IntegrityError:
            raise Conflict(f"There is already an account called '{name}' in that environment.") from None
        if auth == "entra":
            conn.execute("UPDATE accounts SET password = '' WHERE id = ?", (account_id,))
        elif password:
            conn.execute("UPDATE accounts SET password = ? WHERE id = ?", (password, account_id))


def delete_account(account_id: int) -> None:
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT name FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            raise NotFound("That account no longer exists.")
        names = [f"{e}/{n}" for e, n in conn.execute(
            "SELECT environment, name FROM databases WHERE account_id = ? ORDER BY environment, name", (account_id,))]
        if names:
            shown = ", ".join(names[:5]) + (f" and {len(names) - 5} more" if len(names) > 5 else "")
            raise Conflict(f"'{row[0]}' is used by {shown}. Give those databases another account first.")
        conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


# ---- databases -------------------------------------------------------------------------------

_DATABASE_SQL = ("SELECT d.id, d.environment, d.name, d.account_id, a.name AS account_name FROM databases d"
                 " LEFT JOIN accounts a ON a.id = d.account_id")


def list_databases(environment: str | None = None) -> list[dict]:
    """All configured databases, including ones with no account (shown in Settings)."""
    with closing(_connect()) as conn:
        if environment is None:
            rows = conn.execute(_DATABASE_SQL + " ORDER BY d.environment COLLATE NOCASE, d.name COLLATE NOCASE")
        else:
            rows = conn.execute(_DATABASE_SQL + " WHERE d.environment = ? ORDER BY d.name COLLATE NOCASE", (environment,))
        return [dict(r) for r in rows]


def database_names(environment: str) -> list[str]:
    """Usable databases of the environment, i.e. those with an account. The rest appear only in Settings."""
    return [d["name"] for d in list_databases(environment) if d["account_id"] is not None and d["account_name"] is not None]


def _check_account(conn: storedb.Conn, account_id: int, environment: str) -> None:
    row = conn.execute("SELECT environment FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if row is None:
        raise NotFound("That account no longer exists.")
    if row[0].lower() != environment.lower():
        raise Conflict(f"That account belongs to {row[0]}, not to {environment}.")


class MissingFromCatalog(Conflict):
    """Databases not in the catalog yet. The caller can retry with create_missing=True."""

    def __init__(self, names: list[str]):
        self.names = names
        super().__init__(f"{', '.join(names)} {'is' if len(names) == 1 else 'are'} not in the Databases list yet.")


def _ensure_in_catalog(conn: storedb.Conn, names, create_missing: bool) -> None:
    missing: list[str] = []
    for name in names:
        if conn.execute("SELECT 1 FROM db_catalog WHERE name = ?", (name,)).fetchone() is None \
                and name.lower() not in (m.lower() for m in missing):
            missing.append(name)
    if missing and not create_missing:
        raise MissingFromCatalog(missing)
    for name in missing:
        conn.execute("INSERT INTO db_catalog (name) VALUES (?)", (name,))


def add_database(environment: str, name: str, account_id: int, create_missing: bool = True) -> int:
    with closing(_connect()) as conn, conn:
        env = conn.execute("SELECT name FROM environments WHERE name = ?", (environment,)).fetchone()
        if env is None:
            raise NotFound(f"There is no environment '{environment}'. Create it first.")
        _check_account(conn, account_id, env[0])
        _ensure_in_catalog(conn, [name], create_missing)
        try:
            return conn.insert("INSERT INTO databases (environment, name, account_id) VALUES (?, ?, ?)",
                               (env[0], name, account_id))
        except IntegrityError:
            raise Conflict(f"'{name}' is already in {env[0]}.") from None


def rules_using(environment: str, database: str) -> list[str]:
    return [r["name"] for r in list_rules() if r["environment"].lower() == environment.lower()
            and database.lower() in (d.lower() for d in r["databases"])]


def add_databases_for_account(account_id: int, names: list[str], create_missing: bool = True) -> dict[str, list[str]]:
    """Adds databases to the account's environment, or assigns the account to existing ones that have none.

    Databases that already have an account are left alone. Names missing from the catalog raise
    MissingFromCatalog unless create_missing is set.
    """
    added, assigned = [], []
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT environment FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            raise NotFound("That account no longer exists.")
        environment = row[0]
        _ensure_in_catalog(conn, names, create_missing)
        for name in dict.fromkeys(names):
            found = conn.execute(
                "SELECT d.id, a.id FROM databases d LEFT JOIN accounts a ON a.id = d.account_id WHERE d.environment = ? AND d.name = ?",
                (environment, name)).fetchone()
            if found is None:
                conn.execute("INSERT INTO databases (environment, name, account_id) VALUES (?, ?, ?)", (environment, name, account_id))
                added.append(name)
            elif found[1] is None:
                conn.execute("UPDATE databases SET account_id = ? WHERE id = ?", (account_id, found[0]))
                assigned.append(name)
    return {"added": added, "assigned": assigned}


def _renamed_in(conn: storedb.Conn, environment: str, old: str, name: str) -> None:
    """Applies a database rename to its stored scans and the rules that list it."""
    conn.execute("UPDATE snapshots SET database = ? WHERE environment = ? AND database = ?", (name, environment, old))
    for rule_id, listed in conn.execute("SELECT id, databases FROM rules WHERE environment = ?", (environment,)).fetchall():
        names = json.loads(listed)
        if any(n.lower() == old.lower() for n in names):
            names = [name if n.lower() == old.lower() else n for n in names]
            conn.execute("UPDATE rules SET databases = ? WHERE id = ?", (json.dumps(names), rule_id))


def update_database(database_id: int, name: str, account_id: int, create_missing: bool = True) -> None:
    """Renames the database and/or changes its account.

    A rename carries over to stored scans and rules. A new name missing from the catalog raises
    MissingFromCatalog unless create_missing is set.
    """
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT environment, name FROM databases WHERE id = ?", (database_id,)).fetchone()
        if row is None:
            raise NotFound("That database no longer exists.")
        environment, old = row
        _check_account(conn, account_id, environment)
        _ensure_in_catalog(conn, [name], create_missing)
        try:
            conn.execute("UPDATE databases SET name = ?, account_id = ? WHERE id = ?", (name, account_id, database_id))
        except IntegrityError:
            raise Conflict(f"'{name}' is already in {environment}.") from None
        if name != old:
            _renamed_in(conn, environment, old, name)


def delete_database(database_id: int) -> None:
    """Deletes the database and its stored scans; job history stays. Conflict while a rule lists it."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT environment, name FROM databases WHERE id = ?", (database_id,)).fetchone()
    if row is None:
        raise NotFound("That database no longer exists.")
    used = rules_using(*row)
    if used:
        raise Conflict(f"{row[1]} is used by the rule{'s' if len(used) != 1 else ''} {', '.join(used)}. Take it out of "
                       f"{'them' if len(used) != 1 else 'it'} first.")
    with closing(_connect()) as conn, conn:
        conn.execute("DELETE FROM snapshots WHERE environment = ? AND database = ?", row)
        conn.execute("DELETE FROM databases WHERE id = ?", (database_id,))


# ---- the Databases list (the catalog) ----------------------------------------------------------

def list_catalog() -> list[dict]:
    """{id, name, description, environments}; environments are those that include the database, with or without an account."""
    with closing(_connect()) as conn:
        entries = conn.execute("SELECT id, name, description FROM db_catalog ORDER BY name COLLATE NOCASE").fetchall()
        used = conn.execute("SELECT name, environment FROM databases").fetchall()
    where: dict[str, list[str]] = {}
    for name, environment in used:
        where.setdefault(name.lower(), []).append(environment)
    return [{"id": i, "name": name, "description": description or "", "environments": sorted(where.get(name.lower(), []), key=str.lower)}
            for i, name, description in entries]


def add_catalog(name: str, description: str | None) -> int:
    with closing(_connect()) as conn, conn:
        try:
            return conn.insert("INSERT INTO db_catalog (name, description) VALUES (?, ?)", (name, description or None))
        except IntegrityError:
            raise Conflict(f"'{name}' is already in the Databases list.") from None


def update_catalog(catalog_id: int, name: str, description: str | None) -> None:
    """Changes the description and/or name. A rename applies in every environment using it, with its scans and rules."""
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT name FROM db_catalog WHERE id = ?", (catalog_id,)).fetchone()
        if row is None:
            raise NotFound("That database is no longer in the list.")
        old = row[0]
        try:
            conn.execute("UPDATE db_catalog SET name = ?, description = ? WHERE id = ?", (name, description or None, catalog_id))
        except IntegrityError:
            raise Conflict(f"'{name}' is already in the Databases list.") from None
        if name != old:
            for database_id, environment, used_as in conn.execute("SELECT id, environment, name FROM databases WHERE name = ?", (old,)).fetchall():
                conn.execute("UPDATE databases SET name = ? WHERE id = ?", (name, database_id))
                _renamed_in(conn, environment, used_as, name)


def delete_catalog(catalog_id: int) -> None:
    """Removes the entry; Conflict while an environment still has it."""
    with closing(_connect()) as conn, conn:
        row = conn.execute("SELECT name FROM db_catalog WHERE id = ?", (catalog_id,)).fetchone()
        if row is None:
            raise NotFound("That database is no longer in the list.")
        used = sorted((e for (e,) in conn.execute("SELECT environment FROM databases WHERE name = ?", (row[0],))), key=str.lower)
        if used:
            raise Conflict(f"{row[0]} is still set up in {', '.join(used)}. Remove it there first.")
        conn.execute("DELETE FROM db_catalog WHERE id = ?", (catalog_id,))


def account_login(account_id: int) -> dict:
    """{account, server, username, password, auth, trust_cert}. Includes the password: for db.py only."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT name, server, username, password, auth, trust_cert FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if row is None:
        raise NotFound("That account no longer exists.")
    return {"account": row["name"], "server": row["server"], "username": row["username"], "password": row["password"],
            "auth": row["auth"], "trust_cert": bool(row["trust_cert"])}


def connection_info(environment: str, database: str) -> dict | None:
    """{name, account, server, username, password, auth, trust_cert} for connecting; includes the password, for db.connection only.

    None if the database isn't configured. server is None when it has no account.
    """
    with closing(_connect()) as conn:
        row = conn.execute(
            "SELECT d.name, a.name AS account, a.server, a.username, a.password, a.auth, a.trust_cert FROM databases d"
            " LEFT JOIN accounts a ON a.id = d.account_id WHERE d.environment = ? AND d.name = ?",
            (environment, database)).fetchone()
    if row is None:
        return None
    return {"name": row["name"], "account": row["account"], "server": row["server"], "username": row["username"],
            "password": row["password"] if row["server"] is not None else None, "auth": row["auth"], "trust_cert": bool(row["trust_cert"])}


# ---- scans -----------------------------------------------------------------------------------

def save_snapshot(environment: str, database: str, kind: str, taken_by: str, payload) -> dict:
    taken_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with closing(_connect()) as conn, conn:
        conn.execute("DELETE FROM snapshots WHERE environment = ? AND database = ? AND kind = ?", (environment, database, kind))
        conn.execute(
            "INSERT INTO snapshots (environment, database, kind, taken_at, taken_by, payload) VALUES (?, ?, ?, ?, ?, ?)",
            (environment, database, kind, taken_at, taken_by, json.dumps(payload, default=_json_default)),
        )
    return {"taken_at": taken_at, "taken_by": taken_by, "data": json.loads(json.dumps(payload, default=_json_default))}


def load_snapshot(environment: str, database: str, kind: str) -> dict | None:
    with closing(_connect()) as conn:
        row = conn.execute(
            "SELECT taken_at, taken_by, payload FROM snapshots WHERE environment = ? AND database = ? AND kind = ?",
            (environment, database, kind),
        ).fetchone()
    if row is None:
        return None
    return {"taken_at": row[0], "taken_by": row[1], "data": json.loads(row[2])}


def all_snapshots(environment: str) -> dict[str, dict[str, dict]]:
    """{database: {"indexes": snapshot, "fragmentation": snapshot}} for the environment."""
    result: dict[str, dict[str, dict]] = {}
    with closing(_connect()) as conn:
        for database, kind, taken_at, taken_by, payload in conn.execute(
                "SELECT database, kind, taken_at, taken_by, payload FROM snapshots WHERE environment = ?", (environment,)):
            result.setdefault(database, {})[kind] = {"taken_at": taken_at, "taken_by": taken_by, "data": json.loads(payload)}
    return result


def update_fragmentation_entry(environment: str, database: str, entry: dict) -> dict | None:
    """Replaces one index's row in the stored fragmentation scan after a rebuild, keeping its time and author.

    Returns the updated snapshot, or None if there is no scan.
    """
    with closing(_connect()) as conn, conn:
        row = conn.execute(
            "SELECT payload FROM snapshots WHERE environment = ? AND database = ? AND kind = 'fragmentation'",
            (environment, database),
        ).fetchone()
        if row is None:
            return None
        rows = [r for r in json.loads(row[0]) if (r["object_id"], r["index_id"]) != (entry["object_id"], entry["index_id"])]
        rows.append(entry)
        conn.execute(
            "UPDATE snapshots SET payload = ? WHERE environment = ? AND database = ? AND kind = 'fragmentation'",
            (json.dumps(rows, default=_json_default), environment, database),
        )
    return load_snapshot(environment, database, "fragmentation")


# ---- jobs (history of the manual scans) ------------------------------------------------------

def start_job(kind: str, environment: str, database: str, started_by: str, target: str | None = None) -> int:
    started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    with closing(_connect()) as conn, conn:
        job_id = conn.insert(
            "INSERT INTO jobs (kind, environment, database, target, started_by, started_at, status) VALUES (?, ?, ?, ?, ?, ?, 'running')",
            (kind, environment, database, target, started_by, started_at),
        )
    log.info("Job started", id=job_id, kind=kind, environment=environment, database=database, target=target, user=started_by)
    return job_id


def set_job_details(job_id: int, details: str) -> None:
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE jobs SET details = ? WHERE id = ?", (details, job_id))


def finish_job(job_id: int, status: str, duration_ms: int, error: str | None = None) -> None:
    ended_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    with closing(_connect()) as conn, conn:
        conn.execute(
            "UPDATE jobs SET ended_at = ?, duration_ms = ?, status = ?, error = ? WHERE id = ?",
            (ended_at, duration_ms, status, error, job_id),
        )
    (log.warning if status == "failed" else log.info)("Job finished", id=job_id, status=status, duration=f"{duration_ms}ms", error=error)


def interrupt_running_jobs() -> None:
    """Startup cleanup: jobs still marked running belong to a dead process."""
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE jobs SET status = 'interrupted' WHERE status = 'running'")


# Text filters of the history and the columns each one searches (any match). Other filters are in _job_where.
_JOB_TEXT = {"user": ("started_by",), "target": ("target", "details"), "error": ("error",)}


def _escape_like(text: str) -> str:
    return re.sub(r"[!%_\[]", lambda m: "!" + m.group(), text.lower())


def _like(text: str) -> str:
    """LIKE pattern for "contains text". Use with ESCAPE '!'; '[' is special in SQL Server."""
    return "%" + _escape_like(text) + "%"


def _job_where(filters: dict | None) -> tuple[str, tuple]:
    """WHERE clause for the history filters; missing or blank ones are skipped.

    user, target (also searches details), error: case-insensitive contains.
    database: case-insensitive match on one name within a rule job's "A, B" list.
    kind, environment, status: equal. started_*/ended_* from/to: inclusive ISO 8601 UTC strings.
    min_duration_ms/max_duration_ms: inclusive bounds.
    """
    filters = filters or {}
    where, params = [], []
    for key, columns in _JOB_TEXT.items():
        text = str(filters.get(key) or "").strip()
        if text:
            where.append("(" + " OR ".join(f"LOWER({column}) LIKE ? ESCAPE '!'" for column in columns) + ")")
            params += [_like(text)] * len(columns)
    name = str(filters.get("database") or "").strip()
    if name:
        where.append("(LOWER(database) = ? OR LOWER(database) LIKE ? ESCAPE '!' OR LOWER(database) LIKE ? ESCAPE '!'"
                     " OR LOWER(database) LIKE ? ESCAPE '!')")
        like = _escape_like(name)
        params += [name.lower(), like + ", %", "%, " + like, "%, " + like + ", %"]
    for key in ("kind", "environment", "status"):
        if filters.get(key):
            where.append(f"{key} = ?")
            params.append(filters[key])
    for column in ("started_at", "ended_at"):
        prefix = column.split("_")[0]
        if filters.get(prefix + "_from"):
            where.append(f"{column} >= ?")
            params.append(filters[prefix + "_from"])
        if filters.get(prefix + "_to"):
            where.append(f"{column} <= ?")
            params.append(filters[prefix + "_to"])
    for key, op in (("min_duration_ms", ">="), ("max_duration_ms", "<=")):
        if filters.get(key) is not None:
            where.append(f"duration_ms {op} ?")
            params.append(filters[key])
    return (" WHERE " + " AND ".join(where) if where else ""), tuple(params)


def list_jobs(limit: int = 200, offset: int = 0, filters: dict | None = None) -> list[dict]:
    where, params = _job_where(filters)
    with closing(_connect()) as conn:
        rows = conn.paged("SELECT * FROM jobs" + where + " ORDER BY id DESC", params, limit, offset).fetchall()
    return [dict(row) for row in rows]


def job_databases() -> list[str]:
    """Names for the history filter: the catalog plus any that appear only in old jobs.

    Rule jobs store "A, B", which is split; names differing only in case are merged.
    """
    with closing(_connect()) as conn:
        names = [r[0] for r in conn.execute("SELECT name FROM db_catalog")]
        for (value,) in conn.execute("SELECT DISTINCT database FROM jobs WHERE database IS NOT NULL"):
            names += [n.strip() for n in value.split(",") if n.strip()]
    found: dict[str, str] = {}
    for name in names:
        found.setdefault(name.lower(), name)
    return sorted(found.values(), key=str.lower)


def count_jobs(filters: dict | None = None) -> int:
    where, params = _job_where(filters)
    with closing(_connect()) as conn:
        return conn.execute("SELECT COUNT(*) FROM jobs" + where, params).fetchone()[0]


# ---- rules -----------------------------------------------------------------------------------

_RULE_JSON = ("databases", "schedule", "indexes", "options", "execution")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _rule(row: storedb.Row) -> dict:
    rule = dict(row)
    for column in _RULE_JSON:
        rule[column] = json.loads(rule[column])
    return rule


def list_rules() -> list[dict]:
    with closing(_connect()) as conn:
        return [_rule(r) for r in conn.execute("SELECT * FROM rules ORDER BY priority, name COLLATE NOCASE")]


def get_rule(rule_id: int) -> dict | None:
    with closing(_connect()) as conn:
        row = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
    return _rule(row) if row else None


def save_rule(rule: dict, user: str, rule_id: int | None = None) -> int:
    """Creates the rule, or replaces `rule_id`. Raises IntegrityError if the name is taken."""
    values = (
        rule["name"], rule.get("kind", "rebuild"), rule["environment"], json.dumps(rule["databases"]),
        json.dumps(rule["schedule"]), json.dumps(rule["indexes"]),
        rule["min_fragmentation"], json.dumps(rule["options"]), json.dumps(rule["execution"]),
    )
    position = rule.get("priority")      # None: new rules go last on their machine, edited ones keep their place
    with closing(_connect()) as conn, conn:
        if rule_id is None:
            now = _now()
            rule_id = conn.insert(
                "INSERT INTO rules (name, kind, environment, databases, schedule, indexes, min_fragmentation,"
                " options, execution, priority, created_by, created_at, updated_by, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (*values, _LAST, user, now, user, now),
            )
        else:
            conn.execute(
                "UPDATE rules SET name=?, kind=?, environment=?, databases=?, schedule=?, indexes=?,"
                " min_fragmentation=?, options=?, execution=?, updated_by=?, updated_at=? WHERE id=?",
                (*values, user, _now(), rule_id),
            )
        _renumber(conn, place=(rule_id, position) if position is not None else None)
        return rule_id


_LAST = 1_000_000_000      # sorts after any real priority


def _machine_groups(conn: storedb.Conn) -> dict[int, tuple[int | None, int, str]]:
    """rule id -> (machine id, priority, name). Rules whose environment has no machine share the None group."""
    machines = {name.lower(): machine for name, machine in conn.execute("SELECT name, machine_id FROM environments").fetchall()}
    return {r["id"]: (machines.get(r["environment"].lower()), r["priority"], r["name"])
            for r in conn.execute("SELECT id, environment, priority, name FROM rules").fetchall()}


def _renumber(conn: storedb.Conn, place: tuple[int, int] | None = None) -> None:
    """Numbers rules 1..n within each machine by (priority, id).

    `place` = (rule id, position) first moves that rule to that position in its machine.
    """
    groups: dict[int | None, list[tuple]] = {}
    for rule_id, (machine, priority, name) in _machine_groups(conn).items():
        groups.setdefault(machine, []).append((priority, rule_id))
    for rows in groups.values():
        order = [rule_id for _, rule_id in sorted(rows)]
        if place is not None and place[0] in order:
            order.remove(place[0])
            order.insert(max(0, min(place[1] - 1, len(order))), place[0])
        for number, rule_id in enumerate(order, 1):
            conn.execute("UPDATE rules SET priority = ? WHERE id = ? AND priority <> ?", (number, rule_id, number))


def reorder_rules(rule_ids: list[int]) -> None:
    """Sets the order (first = priority 1) of all rules of one machine.

    Conflict if the list isn't exactly that machine's rules, e.g. one was added, moved or removed meanwhile.
    """
    with closing(_connect()) as conn, conn:
        groups = _machine_groups(conn)
        if len(set(rule_ids)) != len(rule_ids) or any(i not in groups for i in rule_ids) or len({groups[i][0] for i in rule_ids}) != 1:
            raise Conflict("The rules changed meanwhile. Refresh the list and try again.")
        machine = groups[rule_ids[0]][0]
        if {i for i, g in groups.items() if g[0] == machine} != set(rule_ids):
            raise Conflict("The rules changed meanwhile. Refresh the list and try again.")
        for number, rule_id in enumerate(rule_ids, 1):
            conn.execute("UPDATE rules SET priority = ? WHERE id = ?", (number, rule_id))


def environment_machines() -> dict[str, int | None]:
    """Lower-cased environment name -> machine id (None if it has none)."""
    with closing(_connect()) as conn:
        return {name.lower(): machine for name, machine in conn.execute("SELECT name, machine_id FROM environments").fetchall()}


def delete_rule(rule_id: int) -> None:
    """Deletes the rule and its run history."""
    with closing(_connect()) as conn, conn:
        conn.execute("DELETE FROM rule_run_items WHERE run_id IN (SELECT id FROM rule_runs WHERE rule_id = ?)", (rule_id,))
        conn.execute("DELETE FROM rule_runs WHERE rule_id = ?", (rule_id,))
        conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
        _renumber(conn)


# ---- rule runs -------------------------------------------------------------------------------

_RUN_COLUMNS = """r.*,
    (SELECT COUNT(*) FROM rule_run_items i WHERE i.run_id = r.id) AS n_total,
    (SELECT COUNT(*) FROM rule_run_items i WHERE i.run_id = r.id AND i.status = 'success') AS n_success,
    (SELECT COUNT(*) FROM rule_run_items i WHERE i.run_id = r.id AND i.status = 'failed') AS n_failed,
    (SELECT COUNT(*) FROM rule_run_items i WHERE i.run_id = r.id AND i.status IN ('skipped', 'interrupted')) AS n_skipped"""


def start_rule_run(rule: dict, trigger: str, started_by: str) -> int:
    """Records a run with a pending row per (database, index). Scan rules get one row per database, with empty schema, table and index."""
    if rule.get("kind") == "scan":
        pending = [(database, {"schema_name": "", "table_name": "", "index_name": ""}) for database in rule["databases"]]
    else:
        pending = [(d, ix) for d in rule["databases"] for ix in rule["indexes"]]
    with closing(_connect()) as conn, conn:
        run_id = conn.insert(
            "INSERT INTO rule_runs (rule_id, rule_name, trigger, started_by, started_at, status) VALUES (?, ?, ?, ?, ?, 'running')",
            (rule["id"], rule["name"], trigger, started_by, _now()),
        )
        conn.executemany(
            "INSERT INTO rule_run_items (run_id, seq, database_name, schema_name, table_name, index_name, status)"
            " VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            [(run_id, seq, database, ix["schema_name"], ix["table_name"], ix["index_name"]) for seq, (database, ix) in enumerate(pending)],
        )
    return run_id


def add_skipped_run(rule: dict, trigger: str, started_by: str, reason: str) -> None:
    """Records a due run that was not started, e.g. because the previous one is still going."""
    now = _now()
    with closing(_connect()) as conn, conn:
        conn.execute(
            "INSERT INTO rule_runs (rule_id, rule_name, trigger, started_by, started_at, ended_at, duration_ms, status, error)"
            " VALUES (?, ?, ?, ?, ?, ?, 0, 'skipped', ?)",
            (rule["id"], rule["name"], trigger, started_by, now, now, reason),
        )


def start_item(item_id: int) -> None:
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE rule_run_items SET status = 'running', started_at = ? WHERE id = ?", (_now(), item_id))


def finish_item(item_id: int, status: str, message: str | None = None, *, page_count: int | None = None,
                frag_before: float | None = None, frag_after: float | None = None,
                options: str | None = None, duration_ms: int | None = None) -> None:
    with closing(_connect()) as conn, conn:
        conn.execute(
            "UPDATE rule_run_items SET status = ?, message = ?, page_count = ?, frag_before = ?, frag_after = ?,"
            " options = ?, ended_at = ?, duration_ms = ? WHERE id = ?",
            (status, message, page_count, frag_before, frag_after, options, _now(), duration_ms, item_id),
        )


def finish_rule_run(run_id: int, status: str, duration_ms: int, error: str | None = None) -> None:
    with closing(_connect()) as conn, conn:
        conn.execute(
            "UPDATE rule_runs SET ended_at = ?, duration_ms = ?, status = ?, error = ? WHERE id = ?",
            (_now(), duration_ms, status, error, run_id),
        )


def interrupt_running_rule_runs() -> None:
    """Startup cleanup: runs still marked running belong to a dead process."""
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE rule_run_items SET status = 'interrupted' WHERE status IN ('pending', 'running')")
        conn.execute("UPDATE rule_runs SET status = 'interrupted' WHERE status = 'running'")


def list_rule_runs(rule_id: int, limit: int = 100) -> list[dict]:
    with closing(_connect()) as conn:
        rows = conn.paged(f"SELECT {_RUN_COLUMNS} FROM rule_runs r WHERE r.rule_id = ? ORDER BY r.id DESC",
                          (rule_id,), limit).fetchall()
    return [dict(r) for r in rows]


def last_rule_runs() -> dict[int, dict]:
    """Latest run per rule id."""
    with closing(_connect()) as conn:
        rows = conn.execute(f"SELECT {_RUN_COLUMNS} FROM rule_runs r WHERE r.id = "
                            "(SELECT MAX(id) FROM rule_runs WHERE rule_id = r.rule_id)").fetchall()
    return {r["rule_id"]: dict(r) for r in rows}


def running_rule_runs() -> dict[int, int]:
    """rule id -> id of its running run."""
    with closing(_connect()) as conn:
        return dict(conn.execute("SELECT rule_id, MAX(id) FROM rule_runs WHERE status = 'running' GROUP BY rule_id").fetchall())


def get_rule_run(run_id: int) -> dict | None:
    with closing(_connect()) as conn:
        run = conn.execute(f"SELECT {_RUN_COLUMNS} FROM rule_runs r WHERE r.id = ?", (run_id,)).fetchone()
        if run is None:
            return None
        items = conn.execute("SELECT * FROM rule_run_items WHERE run_id = ? ORDER BY seq", (run_id,)).fetchall()
    return {**dict(run), "items": [dict(i) for i in items]}


def mark_unfinished_items(run_id: int, status: str, message: str) -> None:
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE rule_run_items SET status = ?, message = ? WHERE run_id = ? AND status IN ('pending', 'running')",
                     (status, message, run_id))
