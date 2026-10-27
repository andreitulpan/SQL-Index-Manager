#!/usr/bin/env python3
"""SQL Index Manager: local web app (FastAPI backend, static UI) to view, rebuild and schedule rebuilds of
SQL Server / Azure SQL indexes.

Admins set up machines, then environments (each on a machine), and per environment the accounts
(server + login) and databases (each uses one account). A database without an account is shown only
on the Environments page.

The UI shows stored scan results and never contacts a database server. Scanning and rebuilding do,
and every job is recorded in the history under the signed-in user.

The first start on an empty database creates "admin" with a random password, printed once to the
output (`docker logs`). Admins add users, who get a random password and must change it at first sign-in.

Rules (rules.py) rebuild a saved list of indexes on a schedule or on demand. Schedules only run while
the server is up.

Requirements: pip install -r requirements.txt, plus the "ODBC Driver 17/18 for SQL Server".
Usage: python app.py (serves http://127.0.0.1:8765 and opens it in the browser)

App data (settings, scans, history, rules) is kept in SQLite at data/index_manager.sqlite3 by default.
For a database server, set host, user and password together (a partial set is an error), as
environment variables or flags (flags win):
    APP_DB_HOST      host[:port]          --db-host        (SQL Server: host,port or host\\INSTANCE)
    APP_DB_USER      or APP_DB_USER_FILE  --db-user
    APP_DB_PASSWORD  or APP_DB_PASSWORD_FILE  --db-password   (_FILE variants read Docker secrets)
    APP_DB_ENGINE    mysql | mariadb | postgresql | mssql   --db-engine   (needed only for a non-default
                     port; defaults are 3306, 5432, 1433)
    APP_DB_NAME      default sql_index_manager; must already exist      --db-name
    APP_DB_TRUST_CERT  yes, for a SQL Server with a self-signed certificate    --db-trust-cert
    APP_DB_URL       engine://user:password@host:port/name, instead of host/user/password
The app creates its tables but never the database.

APP_THEME (or --theme): azure (default) or ocean.

The server listens only on 127.0.0.1 unless APP_HOST says otherwise (the Docker image sets 0.0.0.0).
Failed sign-ins are not rate-limited, so don't expose it to other machines.
"""
from __future__ import annotations

import argparse
import functools
import os
import platform
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Annotated, Literal
import webbrowser
import zoneinfo
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import pyodbc
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, StringConstraints
from starlette.middleware.trustedhost import TrustedHostMiddleware

from indexmanager import auth, busy, db, dbconfig, indexes, log, passwords, rules, schedule, store, storedb, theme, version

HOST, PORT = os.environ.get("APP_HOST", "127.0.0.1"), 8765
STATIC_DIR = Path(__file__).resolve().parent / "indexmanager" / "static"

@asynccontextmanager
async def lifespan(_app: FastAPI):
    auth.announce_admin()
    store.interrupt_running_jobs()
    store.interrupt_running_rule_runs()
    scheduler = rules.Scheduler()
    scheduler.start()
    log.info("Scheduler started", tick=f"{rules.TICK_SECONDS}s")
    log.info("Listening", url=f"http://{HOST}:{PORT}")
    yield
    log.info("Shutting down")
    scheduler.stop()


app = FastAPI(title="SQL Index Manager", version=version.VERSION, lifespan=lifespan)
# Blocks DNS rebinding: reject requests whose Host header isn't local.
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost"])


@app.exception_handler(store.Conflict)
def _conflict(_request: Request, exc: store.Conflict) -> JSONResponse:
    return JSONResponse({"detail": str(exc)}, status_code=409)


@app.exception_handler(store.MissingFromCatalog)
def _missing_from_catalog(_request: Request, exc: store.MissingFromCatalog) -> JSONResponse:
    return JSONResponse({"detail": str(exc), "missing": exc.names}, status_code=409)    # the UI offers to create the missing names


@app.exception_handler(store.NotFound)
def _not_found(_request: Request, exc: store.NotFound) -> JSONResponse:
    return JSONResponse({"detail": str(exc)}, status_code=404)


Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]


class ScanRequest(BaseModel):
    environment: str
    database: str | None = None


class RebuildRequest(BaseModel):
    environment: str
    database: str | None = None
    schema_name: str
    table_name: str
    index_name: str
    online: bool | None = None            # None: online when the index type allows it
    data_compression: Literal["NONE", "ROW", "PAGE", "COLUMNSTORE", "COLUMNSTORE_ARCHIVE"] | None = None
    maxdop: int = Field(0, ge=0, le=indexes.MAX_MAXDOP)
    sort_in_tempdb: bool = False


class ScheduleModel(BaseModel):
    type: Literal["weekly", "daily", "manual"]
    days: list[int] = []                   # weekly only; 0 = Monday ... 6 = Sunday
    time: str = "00:00:00"                 # wall-clock time in `tz`
    tz: str | None = None                  # IANA name, e.g. Europe/Bucharest; None: server local time


MAX_MAXDOP = indexes.MAX_MAXDOP   # RuleRequest's "indexes" field shadows the module inside the class body


class IndexRef(BaseModel):
    schema_name: str
    table_name: str
    index_name: str


class RuleRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    kind: Literal["rebuild", "scan"] = "rebuild"     # a scan rule only refreshes stored scans; no indexes or options
    environment: str
    databases: list[str] = Field(min_length=1, max_length=500)   # each index is rebuilt in every database that has it
    schedule: ScheduleModel
    indexes: list[IndexRef] = Field([], max_length=5000)         # required for a rebuild rule
    min_fragmentation: float | None = Field(None, ge=0, le=100)   # None: rebuild all listed indexes
    online: bool | None = None
    data_compression: Literal["NONE", "ROW", "PAGE", "COLUMNSTORE", "COLUMNSTORE_ARCHIVE"] | None = None
    maxdop: int = Field(0, ge=0, le=MAX_MAXDOP)
    sort_in_tempdb: bool = False
    execution: Literal["serial", "parallel"] = "serial"
    max_parallel: int = Field(4, ge=1, le=16)       # concurrent rebuilds when parallel
    priority: int | None = Field(None, ge=1, le=100000)   # 1 = first on its machine; None: last when new, unchanged when editing


class ReorderRequest(BaseModel):
    ids: list[int] = Field(min_length=1, max_length=5000)   # all rules of one machine, in the new order


class LoginRequest(BaseModel):
    username: str = Field(max_length=256)
    password: str = Field(max_length=1024)


class NewUser(BaseModel):
    username: str = Field(max_length=256)


class RoleChange(BaseModel):
    username: str = Field(max_length=256)
    role: Literal["admin", "user"]


class UserName(BaseModel):
    username: str = Field(max_length=256)


class TimezoneRequest(BaseModel):
    timezone: str | None = Field(None, max_length=64)      # IANA name, e.g. Europe/Bucharest; None or blank: unset


class PasswordChange(BaseModel):
    current_password: str = Field(max_length=1024)
    new_password: str = Field(max_length=1024)


class EnvironmentRequest(BaseModel):
    name: Name


class EnvironmentCreate(EnvironmentRequest):
    machine_id: int | None = None       # required; None gets a 422 from the handler


class EnvironmentRename(BaseModel):
    name: Name                  # current name
    new_name: Name
    machine_id: int | None = None       # None: stay on the current machine


class MachineRequest(BaseModel):
    name: Name
    description: Annotated[str, StringConstraints(strip_whitespace=True, max_length=255)] = ""


class AccountRequest(BaseModel):
    environment: str | None = None      # required when adding; an account can't change environment
    name: Name
    server: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
    username: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]   # Entra ID: the e-mail
    password: str = Field("", max_length=256)    # empty when editing: keep the saved one; unused with Entra ID
    auth: Literal["sql", "entra"] = "sql"        # SQL login, or Microsoft Entra ID browser sign-in
    trust_cert: bool = False                     # accept a self-signed server certificate


class DatabaseRequest(BaseModel):
    environment: str | None = None      # required when adding; a database can't change environment
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
    account_id: int | None = None       # one of the environment's accounts; required
    create_missing: bool = False        # add a name missing from the Databases list instead of refusing


class DatabaseNames(BaseModel):
    names: list[Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]] = Field(min_length=1, max_length=1000)
    create_missing: bool = False        # add names missing from the Databases list instead of refusing


class CatalogRequest(BaseModel):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
    description: Annotated[str, StringConstraints(strip_whitespace=True, max_length=255)] = ""


def require_session(request: Request) -> str:
    """The signed-in user's name, or 401. Allows users who still must change their password."""
    session = auth.session_for(request.cookies.get(auth.SESSION_COOKIE))
    if session is None:
        raise HTTPException(status_code=401, detail="Not signed in.")
    return session[0]


def require_user(request: Request) -> str:
    """The signed-in user's name for data endpoints: 401 if not signed in, 403 until the admin-set password is changed."""
    session = auth.session_for(request.cookies.get(auth.SESSION_COOKIE))
    if session is None:
        raise HTTPException(status_code=401, detail="Not signed in.")
    if session[1]:
        raise HTTPException(status_code=403, detail="Change your password first.")
    return session[0]


def require_admin(user: str = Depends(require_user)) -> str:
    """The admin's name, or 403."""
    found = store.get_user(user)
    if found is None or found["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can do this.")
    return user


def resolve_environment(environment: str) -> str:
    """The saved spelling of the environment name (matched ignoring case), or 404."""
    name = store.get_environment(environment)
    if name is None:
        raise HTTPException(status_code=404, detail=f"There is no environment '{environment}'.")
    return name


def resolve_database(environment: str, database: str | None) -> tuple[str, str]:
    """(environment, database) as saved; the environment's first database if none is given. 404 if either isn't set up."""
    environment = resolve_environment(environment)
    names = store.database_names(environment)
    if database is None:
        if not names:
            raise HTTPException(status_code=404, detail=f"{environment} has no databases with an account yet. An administrator can add one under Environments.")
        return environment, names[0]
    found = next((n for n in names if n.lower() == database.lower()), None)
    if found is None:
        raise HTTPException(status_code=404, detail=f"{database} is not set up in {environment} (or has no account). An administrator can add it under Environments.")
    return environment, found


@contextmanager
def tracked_job(kind: str, environment: str, database: str, user: str, target: str | None = None):
    """Records the body as a job in the history and maps database failures to HTTP errors. 409 if the same job is running."""
    # One job per (environment, database, kind, target): a repeat click is refused, not queued behind a heavy query.
    key = (environment, database, kind, target)
    if not busy.claim(key):
        what = f"A {kind} of {target}" if target else f"A {kind} scan of {database}"
        raise HTTPException(status_code=409, detail=f"{what} is already running.")
    job_id = store.start_job(kind, environment, database, user, target)
    started = time.monotonic()
    error = None
    try:
        yield job_id
    except HTTPException as exc:
        error = str(exc.detail)
        raise
    except indexes.RebuildRefused as exc:
        error = str(exc)
        raise HTTPException(status_code=409, detail=error) from exc
    except db.DbError as exc:
        error = str(exc)
        raise HTTPException(status_code=502, detail=error) from exc
    except pyodbc.Error as exc:
        error = db.clean_db_error(exc)
        raise HTTPException(status_code=502, detail=error) from exc
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        store.finish_job(job_id, "failed" if error else "success", round((time.monotonic() - started) * 1000), error)
        busy.release(key)


def run_scan(request: ScanRequest, kind: str, work, user: str) -> dict:
    """Runs work(cursor) and stores the result as the new snapshot of this kind. On failure the old snapshot stays."""
    environment, database = resolve_database(request.environment, request.database)
    with tracked_job(kind, environment, database, user):
        with db.connection(environment, database) as conn:
            cursor = conn.cursor()
            try:
                payload = work(cursor)
            finally:
                cursor.close()
        return store.save_snapshot(environment, database, kind, user, payload)


# Handlers are sync on purpose: FastAPI runs them in a worker thread, so blocking ODBC calls don't stall the server.

@app.get("/api/version")
def get_version() -> dict:
    """The running version. Public, the sign-in page shows it too."""
    return {"version": version.VERSION, "commit": version.COMMIT}


@app.post("/api/login")
def login(body: LoginRequest, response: Response) -> dict:
    if auth.clean_username(body.username) is None or not body.password:
        raise HTTPException(status_code=422, detail="Enter a username and a password.")
    token = auth.login(body.username, body.password)
    if token is None:
        raise HTTPException(status_code=401, detail="Wrong username or password.")
    # HttpOnly hides it from scripts; SameSite=Strict stops other sites sending it.
    response.set_cookie(
        auth.SESSION_COOKIE, token, max_age=auth.SESSION_SECONDS, httponly=True, samesite="strict", path="/"
    )
    return {"ok": True}


@app.post("/api/logout")
def logout(request: Request, response: Response) -> dict:
    auth.logout(request.cookies.get(auth.SESSION_COOKIE))
    response.delete_cookie(auth.SESSION_COOKIE, path="/")
    return {"ok": True}


@app.get("/api/me")
def me(user: str = Depends(require_session)) -> dict:
    return store.get_user(user) or {"username": user, "role": None, "created_at": None, "must_change_password": False, "timezone": None}


@functools.lru_cache(maxsize=1)
def _iana_zones() -> frozenset[str]:
    return frozenset(zoneinfo.available_timezones())


@app.post("/api/me/timezone")
def set_my_timezone(body: TimezoneRequest, user: str = Depends(require_user)) -> dict:
    """Saves the user's IANA time zone; empty clears it."""
    zone = (body.timezone or "").strip() or None
    if zone is not None and zone not in _iana_zones():
        raise HTTPException(status_code=422, detail=f"{zone} is not an IANA time zone (for example Europe/Bucharest).")
    store.set_timezone(user, zone)
    return {"timezone": zone}


@app.get("/api/time", dependencies=[Depends(require_user)])
def get_time() -> dict:
    """The server's current time, zone name and UTC offset in minutes."""
    now = datetime.now().astimezone()
    offset = int(now.utcoffset().total_seconds() // 60)
    sign = "-" if offset < 0 else "+"
    return {"server_time": now.isoformat(timespec="milliseconds"), "offset_minutes": offset,
            "timezone": f"{now.tzname()} (UTC{sign}{abs(offset) // 60:02d}:{abs(offset) % 60:02d})"}


@app.post("/api/me/password")
def change_my_password(body: PasswordChange, request: Request, user: str = Depends(require_session)) -> dict:
    if len(body.new_password) < passwords.MIN_LENGTH:
        raise HTTPException(status_code=422, detail=f"The new password needs at least {passwords.MIN_LENGTH} characters.")
    store.change_password(user, body.current_password, body.new_password)
    auth.password_changed(request.cookies.get(auth.SESSION_COOKIE), user)
    return {"ok": True}


# users (admin only)

@app.get("/api/users", dependencies=[Depends(require_admin)])
def get_users() -> dict:
    return {"users": store.list_users()}


@app.post("/api/users")
def create_user(body: NewUser, user: str = Depends(require_admin)) -> dict:
    """Adds a user. The generated password is returned here only; the admin passes it on."""
    name = auth.clean_username(body.username)
    if name is None:
        raise HTTPException(status_code=422, detail=f"Enter a name of up to {auth.MAX_NAME_LENGTH} characters.")
    return {"username": name, "password": store.create_user(name)}


@app.post("/api/users/reset-password")
def reset_user_password(body: UserName, user: str = Depends(require_admin)) -> dict:
    """Sets a new random password, returned here only, and ends the user's sessions."""
    if body.username.lower() == user.lower():
        raise HTTPException(status_code=409, detail="Change your own password from My profile.")
    name, password = store.reset_password(body.username)
    auth.end_sessions(name)
    return {"username": name, "password": password}


@app.post("/api/users/delete")
def delete_user(body: UserName, user: str = Depends(require_admin)) -> dict:
    if body.username.lower() == user.lower():
        raise HTTPException(status_code=409, detail="You can't remove your own account.")
    auth.end_sessions(store.delete_user(body.username))
    return {"ok": True}


@app.post("/api/users/role")
def set_user_role(body: RoleChange, user: str = Depends(require_admin)) -> dict:
    if body.username.lower() == user.lower():
        raise HTTPException(status_code=409, detail="You can't change your own role.")
    store.set_role(body.username, body.role)
    return {"ok": True}


def _utc(value: str | None, name: str, end: bool = False) -> str | None:
    """A filter date as UTC ISO 8601. A bare date covers the whole day (end = its last moment); no offset means UTC."""
    value = (value or "").strip()
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"{name}: '{value}' is not a date; use 2026-10-26 or 2026-10-26T08:30:00Z.") from None
    if "T" not in value and " " not in value:
        moment = datetime.combine(moment.date(), datetime.max.time() if end else datetime.min.time())
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")


@app.get("/api/jobs")
def get_jobs(page: int = Query(1, ge=1), page_size: int = Query(15, ge=1, le=100),
             user_filter: str | None = Query(None, alias="user"), kind: str | None = None, target: str | None = None,
             environment: str | None = None, database: str | None = None, status: str | None = None, error: str | None = None,
             started_from: str | None = None, started_to: str | None = None, ended_from: str | None = None, ended_to: str | None = None,
             min_duration: float | None = Query(None, ge=0, description="seconds"),
             max_duration: float | None = Query(None, ge=0, description="seconds"), user: str = Depends(require_user)) -> dict:
    """One page of job history, newest first; a page past the end returns the last page.

    Filters are optional and combined; `total` counts matches. user, target (also searches details) and error
    are case-insensitive substring matches. database, kind, environment and status must match exactly
    (a rule job covers several databases; database matches any of them). Date bounds are inclusive ISO 8601.
    Durations are in seconds; running jobs have none."""
    filters = {"user": user_filter, "kind": kind, "target": target, "environment": environment, "database": database,
               "status": status, "error": error, "started_from": _utc(started_from, "started_from"),
               "started_to": _utc(started_to, "started_to", end=True), "ended_from": _utc(ended_from, "ended_from"),
               "ended_to": _utc(ended_to, "ended_to", end=True),
               "min_duration_ms": None if min_duration is None else round(min_duration * 1000),
               "max_duration_ms": None if max_duration is None else round(max_duration * 1000)}
    total = store.count_jobs(filters)
    page = min(page, max(1, -(-total // page_size)))
    return {"jobs": store.list_jobs(page_size, (page - 1) * page_size, filters), "total": total, "page": page, "page_size": page_size}


@app.get("/api/jobs/databases", dependencies=[Depends(require_user)])
def get_job_databases() -> list[str]:
    """Databases for the history filter: the Databases list plus any only old jobs mention."""
    return store.job_databases()


@app.get("/api/environments", dependencies=[Depends(require_user)])
def get_environments() -> list[dict]:
    return store.list_environments()


@app.get("/api/databases", dependencies=[Depends(require_user)])
def get_databases(environment: str) -> dict:
    """The environment's databases that have an account; the first is the default. Reads stored data only."""
    names = store.database_names(resolve_environment(environment))
    return {"default": names[0] if names else None, "databases": names}


@app.get("/api/snapshot", dependencies=[Depends(require_user)])
def get_snapshot(environment: str, database: str | None = None) -> dict:
    """Stored scans for a database; None for a kind never scanned."""
    environment, database = resolve_database(environment, database)
    return {
        "database": database,
        "indexes": store.load_snapshot(environment, database, "indexes"),
        "fragmentation": store.load_snapshot(environment, database, "fragmentation"),
    }


@app.get("/api/snapshots", dependencies=[Depends(require_user)])
def get_snapshots(environment: str) -> dict:
    """Stored scans for every database in the environment."""
    environment = resolve_environment(environment)
    stored = store.all_snapshots(environment)
    return {"databases": [{"database": n, "indexes": stored.get(n, {}).get("indexes"),
                           "fragmentation": stored.get(n, {}).get("fragmentation")} for n in store.database_names(environment)]}


@app.get("/api/index-presence", dependencies=[Depends(require_user)])
def get_index_presence(environment: str, schema_name: str, table_name: str, index_name: str | None = None) -> dict:
    """Splits databases by whether the stored index scans contain this index: `has`, `missing`, `unscanned` (unknown).

    Without index_name it looks for the table's heap."""
    environment = resolve_environment(environment)
    names = store.database_names(environment)
    stored = store.all_snapshots(environment)
    want = (schema_name.casefold(), table_name.casefold(), (index_name or "").casefold())
    has, missing, unscanned = [], [], []
    for name in names:
        snap = stored.get(name, {}).get("indexes")
        if snap is None:
            unscanned.append(name)
            continue
        found = next((ix for ix in snap["data"]["indexes"]
                      if (ix["schema_name"].casefold(), ix["table_name"].casefold(), (ix["index_name"] or "").casefold()) == want), None)
        if found:
            has.append({"database": name, "taken_at": snap["taken_at"], **{k: found.get(k) for k in (
                "index_name", "type_desc", "row_count", "size_mb", "is_disabled", "key_columns")}})
        else:
            missing.append({"database": name, "taken_at": snap["taken_at"]})
    return {"has": has, "missing": missing, "unscanned": unscanned}


# POST with a JSON body: a cross-site page can't send that without a CORS preflight, and these open connections and run queries.

@app.post("/api/scan/indexes")
def scan_indexes(request: ScanRequest, user: str = Depends(require_user)) -> dict:
    return run_scan(request, "indexes", indexes.list_indexes, user)


@app.post("/api/scan/fragmentation")
def scan_fragmentation(request: ScanRequest, user: str = Depends(require_user)) -> dict:
    return run_scan(request, "fragmentation", indexes.scan_fragmentation, user)


@app.post("/api/rebuild")
def rebuild(request: RebuildRequest, user: str = Depends(require_user)) -> dict:
    """Rebuilds one index, re-measures it and updates its row in the stored fragmentation scan."""
    environment, database = resolve_database(request.environment, request.database)
    target = f"{request.schema_name}.{request.table_name}.{request.index_name}"
    options = indexes.RebuildOptions(request.online, request.data_compression, request.maxdop, request.sort_in_tempdb)
    with tracked_job("rebuild", environment, database, user, target) as job_id:
        with db.connection(environment, database) as conn:
            cursor = conn.cursor()
            try:
                result = indexes.rebuild_index(
                    cursor, request.schema_name, request.table_name, request.index_name, options,
                    on_statement=lambda clause: store.set_job_details(job_id, clause))
            finally:
                cursor.close()
        entry = result["fragmentation"]
        snapshot = store.update_fragmentation_entry(environment, database, entry) if entry else None
        return {"online": result["online"], "options": result["options"], "entry": entry, "fragmentation": snapshot}


# ---- environments, accounts, databases (admin only) ---------------------------
# Passwords are write-only: accounts report `has_password`, never the value.

@app.get("/api/settings", dependencies=[Depends(require_admin)])
def get_settings() -> dict:
    return {"machines": store.list_machines(), "environments": store.list_environments(), "accounts": store.list_accounts(),
            "databases": store.list_databases()}


@app.get("/api/machines", dependencies=[Depends(require_admin)])
def get_machines() -> dict:
    return {"machines": store.list_machines()}


def checked_machine(body: MachineRequest) -> MachineRequest:
    if not body.name.isprintable() or not body.description.isprintable():
        raise HTTPException(status_code=422, detail="The name or the description has characters that are not allowed.")
    return body


@app.post("/api/machines")
def create_machine(body: MachineRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_machine(body)
    return {"id": store.add_machine(body.name, body.description)}


@app.post("/api/machines/{machine_id}")
def update_machine(machine_id: int, body: MachineRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_machine(body)
    store.update_machine(machine_id, body.name, body.description)
    return {"ok": True}


@app.post("/api/machines/{machine_id}/delete")
def delete_machine(machine_id: int, user: str = Depends(require_admin)) -> dict:
    store.delete_machine(machine_id)
    return {"ok": True}


@app.post("/api/environments")
def create_environment(body: EnvironmentCreate, user: str = Depends(require_admin)) -> dict:
    """Creates an environment on a machine; fails with 422 if there are no machines."""
    if not store.list_machines():
        raise HTTPException(status_code=422, detail="There are no machines yet. Add a machine first (Machines page), then the environment can be put on it.")
    if body.machine_id is None:
        raise HTTPException(status_code=422, detail="Pick the machine the environment is on.")
    store.add_environment(body.name, body.machine_id)
    return {"ok": True}


@app.post("/api/environments/rename")
def rename_environment(body: EnvironmentRename, user: str = Depends(require_admin)) -> dict:
    """Renames the environment and/or moves it to another machine."""
    store.rename_environment(body.name, body.new_name, body.machine_id)
    return {"ok": True}


@app.post("/api/environments/delete")
def delete_environment(body: EnvironmentRequest, user: str = Depends(require_admin)) -> dict:
    store.delete_environment(body.name)
    return {"ok": True}


def checked_account(body: AccountRequest) -> AccountRequest:
    if not db.SAFE_SERVER.match(body.server):
        raise HTTPException(status_code=422, detail="The server can only have letters, digits and - . , : \\ (e.g. "
                                                    "myserver.database.windows.net,3342).")
    if not body.username.isprintable():
        raise HTTPException(status_code=422, detail="The username has characters that are not allowed.")
    if body.auth == "entra" and "@" not in body.username:
        raise HTTPException(status_code=422, detail="For Microsoft Entra ID enter the e-mail address you sign in with.")
    return body


@app.post("/api/accounts")
def create_account(body: AccountRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_account(body)
    if body.auth == "sql" and not body.password:
        raise HTTPException(status_code=422, detail="Enter the password of the login.")
    if not body.environment:
        raise HTTPException(status_code=422, detail="Pick the environment the account belongs to.")
    return {"id": store.add_account(resolve_environment(body.environment), body.name, body.server, body.username, body.password, body.auth, body.trust_cert)}


@app.post("/api/accounts/{account_id}/databases")
def scan_account(account_id: int, user: str = Depends(require_admin)) -> dict:
    """Lists the databases the login can see on the server and whether each is already in the environment. POST because it connects."""
    account = store.get_account(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="That account no longer exists.")
    try:
        found, skipped = db.server_databases(account_id)
    except db.DbError as exc:
        raise HTTPException(status_code=502, detail=f"{exc} Check the account's password. If the login is right but exists only inside one database (as can happen on Azure SQL), add that database by hand.") from exc
    configured = {d["name"].lower(): d for d in store.list_databases(account["environment"])}
    listed = []
    for name in found:
        have = configured.get(name.lower())
        if have is None:
            status = "new"
        elif have["account_id"] == account_id:
            status = "added"
        elif have["account_name"] is None:
            status = "unassigned"       # in the environment without an account; adding it assigns this one
        else:
            status = "other"
        listed.append({"name": name, "status": status, "account": have["account_name"] if have else None})
    seen = {n.lower() for n in found}
    gone = [d["name"] for d in configured.values() if d["account_id"] == account_id and d["name"].lower() not in seen]
    return {"environment": account["environment"], "account": account["name"], "databases": listed, "skipped": skipped, "gone": gone}


@app.post("/api/accounts/{account_id}/databases/add")
def add_account_databases(account_id: int, body: DatabaseNames, user: str = Depends(require_admin)) -> dict:
    """Adds the databases to the account's environment, using this account."""
    for name in body.names:
        if not db.SAFE_DATABASE_NAME.match(name):
            raise HTTPException(status_code=422, detail=f"'{name}' has characters a database name can't have here.")
    return store.add_databases_for_account(account_id, body.names, body.create_missing)


@app.post("/api/accounts/{account_id}")
def update_account(account_id: int, body: AccountRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_account(body)
    old = store.get_account(account_id)
    if old is not None and body.auth == "sql" and not body.password and not old["has_password"]:
        raise HTTPException(status_code=422, detail="Enter the password of the login.")      # switching from Entra ID: no password is stored
    store.update_account(account_id, body.name, body.server, body.username, body.password or None, body.auth, body.trust_cert)
    return {"ok": True}


@app.post("/api/accounts/{account_id}/delete")
def delete_account(account_id: int, user: str = Depends(require_admin)) -> dict:
    store.delete_account(account_id)
    return {"ok": True}


def checked_database(body: DatabaseRequest) -> DatabaseRequest:
    if not db.SAFE_DATABASE_NAME.match(body.name):
        raise HTTPException(status_code=422, detail="A database name can only have letters, digits and _ - . $ @ # and spaces.")
    return body


@app.get("/api/catalog", dependencies=[Depends(require_admin)])
def get_catalog() -> dict:
    """The Databases list, with the environments using each name."""
    return {"databases": store.list_catalog()}


def checked_catalog(body: CatalogRequest) -> CatalogRequest:
    if not db.SAFE_DATABASE_NAME.match(body.name):
        raise HTTPException(status_code=422, detail="A database name can only have letters, digits and _ - . $ @ # and spaces.")
    if not body.description.isprintable():
        raise HTTPException(status_code=422, detail="The description has characters that are not allowed.")
    return body


@app.post("/api/catalog")
def create_catalog_database(body: CatalogRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_catalog(body)
    return {"id": store.add_catalog(body.name, body.description)}


@app.post("/api/catalog/{catalog_id}")
def update_catalog_database(catalog_id: int, body: CatalogRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_catalog(body)
    store.update_catalog(catalog_id, body.name, body.description)
    return {"ok": True}


@app.post("/api/catalog/{catalog_id}/delete")
def delete_catalog_database(catalog_id: int, user: str = Depends(require_admin)) -> dict:
    store.delete_catalog(catalog_id)
    return {"ok": True}


@app.post("/api/databases")
def create_database(body: DatabaseRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_database(body)
    if not body.environment:
        raise HTTPException(status_code=422, detail="Pick the environment the database belongs to.")
    if body.account_id is None:
        raise HTTPException(status_code=422, detail="Pick the account to connect to the database with.")
    return {"id": store.add_database(body.environment, body.name, body.account_id, body.create_missing)}


@app.post("/api/databases/{database_id}")
def update_database(database_id: int, body: DatabaseRequest, user: str = Depends(require_admin)) -> dict:
    body = checked_database(body)
    if body.account_id is None:
        raise HTTPException(status_code=422, detail="Pick the account to connect to the database with.")
    store.update_database(database_id, body.name, body.account_id, body.create_missing)
    return {"ok": True}


@app.post("/api/databases/{database_id}/delete")
def delete_database(database_id: int, user: str = Depends(require_admin)) -> dict:
    store.delete_database(database_id)
    return {"ok": True}


@app.post("/api/databases/{database_id}/test")
def test_database(database_id: int, user: str = Depends(require_admin)) -> dict:
    """Connects with the database's account and reports the login. A failed connection returns ok=False, not an HTTP error."""
    row = next((d for d in store.list_databases() if d["id"] == database_id), None)
    if row is None:
        raise HTTPException(status_code=404, detail="That database no longer exists.")
    try:
        with db.connection(row["environment"], row["name"]) as conn:
            cursor = conn.cursor()
            try:
                login, name = db.query(cursor, "SELECT SUSER_SNAME() AS login, DB_NAME() AS name")[0].values()
            finally:
                cursor.close()
    except db.DbError as exc:
        return {"ok": False, "message": str(exc)}
    except pyodbc.Error as exc:
        return {"ok": False, "message": db.clean_db_error(exc)}
    return {"ok": True, "message": f"Connected to {name} as {login}."}


# ---- rules -----------------------------------------------------------------------------------

def rule_from_request(body: RuleRequest) -> dict:
    """Validates the request and returns the rule as stored, so a rule that can never run isn't saved."""
    environment = resolve_environment(body.environment)
    configured = {n.lower(): n for n in store.database_names(environment)}
    names: list[str] = []
    for name in body.databases:
        found = configured.get(name.strip().lower())
        if found is None:
            raise HTTPException(status_code=422, detail=f"{name.strip()} is not set up in {environment}, or has no account. An administrator can add it under Environments.")
        if found not in names:        # drop duplicates that differ only in case
            names.append(found)
    try:
        when = schedule.validate(body.schedule.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if body.kind == "scan":
        return {
            "name": body.name.strip(), "kind": "scan", "environment": environment, "databases": names, "schedule": when,
            "indexes": [], "min_fragmentation": None,
            "options": {"online": None, "data_compression": None, "maxdop": 0, "sort_in_tempdb": False},
            "execution": {"mode": "serial", "max_parallel": 1}, "priority": body.priority,
        }
    if not body.indexes:
        raise HTTPException(status_code=422, detail="Pick at least one index to rebuild.")
    refs = {(i.schema_name, i.table_name, i.index_name): i for i in body.indexes}
    return {
        "name": body.name.strip(), "kind": "rebuild", "environment": environment, "databases": names,
        "schedule": when,
        "indexes": [{"schema_name": s, "table_name": t, "index_name": n} for s, t, n in refs],
        "min_fragmentation": body.min_fragmentation,
        "options": {"online": body.online, "data_compression": body.data_compression,
                    "maxdop": body.maxdop, "sort_in_tempdb": body.sort_in_tempdb},
        "execution": {"mode": body.execution, "max_parallel": body.max_parallel}, "priority": body.priority,
    }


def rule_view(rule: dict, last_runs: dict[int, dict], running: dict[int, int], now: datetime, machines: dict[str, str | None] | None = None,
              waiting: dict[int, dict] | None = None) -> dict:
    """The rule plus run state. `machines` maps lower-cased environment name to machine; `waiting` is rules.queued(). Both are looked up if omitted."""
    if machines is None:
        machines = {e["name"].lower(): e["machine"] for e in store.list_environments()}
    if waiting is None:
        waiting = rules.queued()
    when = rule["schedule"]
    upcoming = schedule.next_occurrence(when, now)
    last = last_runs.get(rule["id"])
    return {
        **rule,
        "schedule_text": schedule.describe(when),
        "next_run": upcoming.isoformat() if upcoming else None,
        "last_run": last,
        "running": rule["id"] in running,
        "running_run_id": running.get(rule["id"]),
        "stopping": running.get(rule["id"]) in rules.stopping(),     # stop requested, run not ended yet
        "machine": machines.get(rule["environment"].lower()),
        "queued": waiting.get(rule["id"]),      # None or {"behind": name of the running rule, "position": 1 = next}
    }


def require_rule(rule_id: int) -> dict:
    rule = store.get_rule(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="That rule no longer exists.")
    return rule


def save_rule(body: RuleRequest, user: str, rule_id: int | None = None) -> dict:
    rule = rule_from_request(body)
    try:
        rule_id = store.save_rule(rule, user, rule_id)
    except store.IntegrityError as exc:
        raise HTTPException(status_code=409, detail=f"There is already a rule named '{rule['name']}'.") from exc
    return rule_view(store.get_rule(rule_id), store.last_rule_runs(), store.running_rule_runs(), datetime.now().astimezone())


@app.get("/api/rules", dependencies=[Depends(require_user)])
def get_rules() -> dict:
    now = datetime.now().astimezone()
    last_runs = store.last_rule_runs()
    running = store.running_rule_runs()
    machines = {e["name"].lower(): e["machine"] for e in store.list_environments()}
    waiting = rules.queued()
    return {
        "server_time": now.astimezone().isoformat(),
        "timezone": f"{now.astimezone().tzname()} (UTC{now.astimezone().strftime('%z')[:3]}:{now.astimezone().strftime('%z')[3:]})",
        "rules": [rule_view(r, last_runs, running, now, machines, waiting) for r in store.list_rules()],
    }


@app.post("/api/rules/reorder")
def reorder_rules(body: ReorderRequest, user: str = Depends(require_user)) -> dict:
    """Sets the order of one machine's rules; first = priority 1."""
    try:
        store.reorder_rules(body.ids)
    except store.Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True}


@app.post("/api/rules")
def create_rule(body: RuleRequest, user: str = Depends(require_user)) -> dict:
    return save_rule(body, user)


@app.post("/api/rules/{rule_id}")
def update_rule(rule_id: int, body: RuleRequest, user: str = Depends(require_user)) -> dict:
    require_rule(rule_id)
    return save_rule(body, user, rule_id)


class DuplicateRequest(BaseModel):
    environment: str
    name: Name
    manual: bool = False       # copy runs only when started by hand


def _index_key(schema_name: str, table_name: str, index_name: str | None) -> tuple[str, str, str]:
    return schema_name.casefold(), table_name.casefold(), (index_name or "").casefold()


def _list_some(items: list[str], limit: int = 5) -> str:
    return ", ".join(items[:limit]) + (f" and {len(items) - limit} more" if len(items) > limit else "")


def duplicate_plan(rule: dict, target: str) -> tuple[dict, list[str], list[dict]]:
    """Plans a copy of the rule into `target` (may be its own environment): (report, databases, indexes).

    `report["notes"]` holds "error" entries that block the copy and "warnings". Databases match by name,
    indexes by schema, table and name in the last stored index scan."""
    environment = resolve_environment(target)
    same = environment.casefold() == rule["environment"].casefold()
    usable = {n.casefold(): n for n in store.database_names(environment)}
    configured = {d["name"].casefold() for d in store.list_databases(environment)}
    notes: list[dict] = []
    warn = lambda text: notes.append({"level": "warning", "text": text})
    databases: list[str] = []
    no_account: list[str] = []
    absent: list[str] = []
    for name in rule["databases"]:
        if name.casefold() in usable:
            databases.append(usable[name.casefold()])
        elif name.casefold() in configured:
            no_account.append(name)
        else:
            absent.append(name)
    if no_account:
        one = len(no_account) == 1
        warn(f"{_list_some(no_account)} {'has' if one else 'have'} no account in {environment} and {'was' if one else 'were'} left out of the copy.")
    if absent:
        one = len(absent) == 1
        warn(f"{_list_some(absent)} {'doesn' if one else 'don'}'t exist in {environment} and {'was' if one else 'were'} left out of the copy.")
    refs = rule["indexes"] if rule["kind"] == "rebuild" else []
    found: dict[str, set] = {}
    unscanned: list[str] = []
    for name in databases:
        snap = store.load_snapshot(environment, name, "indexes")
        if snap is None:
            unscanned.append(name)
            continue
        found[name] = {_index_key(i["schema_name"], i["table_name"], i["index_name"]) for i in snap["data"]["indexes"]}
        if not refs:
            continue
        missing = [f"{r['schema_name']}.{r['table_name']}.{r['index_name'] or '(heap)'}" for r in refs
                   if _index_key(r["schema_name"], r["table_name"], r["index_name"]) not in found[name]]
        when = snap["taken_at"][:10]
        if len(missing) == len(refs):
            warn(f"None of the {len(refs)} indexes exist in {name} (last scan {when}), so the rule will do nothing there.")
        elif missing:
            warn(f"{name} lacks {len(missing)} of the {len(refs)} indexes (last scan {when}): {_list_some(missing)}. They are skipped there.")
    for name in unscanned:
        warn(f"{name} has never been scanned in {environment}, so its indexes can't be checked. " + (
            "Scan it first to see whether it has them; any it lacks are skipped when the rule runs." if refs else "A scan will find them."))
    if same and rule["schedule"]["type"] != "manual":
        warn(f"The copy is in the same environment and, unless you start it by hand, runs at the same time as '{rule['name']}' on the same databases.")
    kept = refs
    if refs and not unscanned:
        present = set().union(*found.values()) if found else set()
        kept = [r for r in refs if _index_key(r["schema_name"], r["table_name"], r["index_name"]) in present]
        dropped = [f"{r['schema_name']}.{r['table_name']}.{r['index_name'] or '(heap)'}" for r in refs if r not in kept]
        if dropped and kept:
            warn(f"{len(dropped)} indexes exist in none of the databases and are left out: {_list_some(dropped)}.")
    if not databases:
        notes.append({"level": "error", "text": f"None of the rule's databases ({_list_some(rule['databases'])}) can be used in {environment}, so there is nothing to copy."})
    elif refs and not kept:
        notes.append({"level": "error", "text": f"None of the rule's {len(refs)} indexes exist in the databases of {environment}, so the copy would rebuild nothing."})
    taken = {n.casefold() for n in (r["name"] for r in store.list_rules())}
    base = f"{rule['name']} ({'copy' if same else environment})"[:100]
    name, n = base, 1
    while name.casefold() in taken:
        n += 1
        name = f"{base[:100 - len(str(n)) - 1]} {n}"
    report = {
        "environment": environment, "same_environment": same, "name": name, "kind": rule["kind"], "schedule_text": schedule.describe(rule["schedule"]),
        "scheduled": rule["schedule"]["type"] != "manual",
        "databases": databases, "databases_total": len(rule["databases"]),
        "indexes": len(kept), "indexes_total": len(refs), "notes": notes,
        "can_create": not any(x["level"] == "error" for x in notes),
    }
    return report, databases, kept


@app.get("/api/rules/{rule_id}/duplicate", dependencies=[Depends(require_user)])
def check_duplicate_rule(rule_id: int, environment: str) -> dict:
    """Dry run of the copy: the report with warnings for missing databases, unscanned databases and missing indexes."""
    return duplicate_plan(require_rule(rule_id), environment)[0]


@app.post("/api/rules/{rule_id}/duplicate")
def duplicate_rule(rule_id: int, body: DuplicateRequest, user: str = Depends(require_user)) -> dict:
    """Creates the copy with the databases and indexes that fit, keeping options, execution and schedule (or manual only)."""
    rule = require_rule(rule_id)
    report, databases, kept = duplicate_plan(rule, body.environment)
    if not report["can_create"]:
        raise HTTPException(status_code=422, detail=next(x["text"] for x in report["notes"] if x["level"] == "error"))
    copy = {**rule, "name": body.name.strip(), "environment": report["environment"], "databases": databases, "indexes": kept,
            "schedule": {"type": "manual"} if body.manual else rule["schedule"], "priority": None}      # last in the queue
    try:
        new_id = store.save_rule(copy, user)
    except store.IntegrityError as exc:
        raise HTTPException(status_code=409, detail=f"There is already a rule named '{copy['name']}'.") from exc
    return {"rule": rule_view(store.get_rule(new_id), store.last_rule_runs(), store.running_rule_runs(), datetime.now().astimezone()), "report": report}


@app.post("/api/rules/{rule_id}/delete")
def delete_rule(rule_id: int, user: str = Depends(require_user)) -> dict:
    rule = require_rule(rule_id)
    if rule_id in store.running_rule_runs():
        raise HTTPException(status_code=409, detail=f"Rule '{rule['name']}' is running; stop it first.")
    rules.cancel_queued(rule_id)
    store.delete_rule(rule_id)
    return {"ok": True}


@app.post("/api/rules/{rule_id}/run")
def run_rule(rule_id: int, user: str = Depends(require_user)) -> dict:
    """Starts the rule in the background, or queues it (`queued`) if another rule on the same machine is running."""
    require_rule(rule_id)
    try:
        run_id = rules.request_run(rule_id, user, "manual")
    except rules.RuleBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"run_id": run_id, "queued": run_id is None}


@app.post("/api/rules/{rule_id}/unqueue")
def unqueue_rule(rule_id: int, user: str = Depends(require_user)) -> dict:
    """Removes a queued rule from the queue."""
    require_rule(rule_id)
    if not rules.cancel_queued(rule_id):
        raise HTTPException(status_code=409, detail="That rule isn't waiting any more.")
    return {"ok": True}


@app.get("/api/rules/{rule_id}/runs", dependencies=[Depends(require_user)])
def get_rule_runs(rule_id: int) -> list[dict]:
    require_rule(rule_id)
    stopping = rules.stopping()
    return [{**r, "stopping": r["id"] in stopping} for r in store.list_rule_runs(rule_id)]


@app.get("/api/rule-runs/{run_id}", dependencies=[Depends(require_user)])
def get_rule_run(run_id: int) -> dict:
    run = store.get_rule_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="That run no longer exists.")
    return run


@app.post("/api/rule-runs/{run_id}/stop", dependencies=[Depends(require_user)])
def stop_rule_run(run_id: int) -> dict:
    if not rules.stop_run(run_id):
        raise HTTPException(status_code=409, detail="That run is not running.")
    return {"ok": True}


# Always revalidate pages so an update isn't hidden by the browser cache.
NO_CACHE = {"Cache-Control": "no-cache"}


@app.get("/theme.css")
def theme_css():
    """The stylesheet for the configured theme. Public, the sign-in page uses it."""
    return FileResponse(theme.stylesheet(), media_type="text/css", headers=NO_CACHE)


@app.get("/login")
def login_page(request: Request):
    if auth.user_for(request.cookies.get(auth.SESSION_COOKIE)):
        return RedirectResponse("/")
    return FileResponse(STATIC_DIR / "login.html", headers=NO_CACHE)


@app.get("/change-password")
def change_password_page(request: Request):
    session = auth.session_for(request.cookies.get(auth.SESSION_COOKIE))
    if session is None:
        return RedirectResponse("/login")
    if not session[1]:
        return RedirectResponse("/")
    return FileResponse(STATIC_DIR / "change-password.html", headers=NO_CACHE)


@app.get("/")
def root(request: Request):
    session = auth.session_for(request.cookies.get(auth.SESSION_COOKIE))
    if session is None:
        return RedirectResponse("/login")
    if session[1]:
        return RedirectResponse("/change-password")
    return FileResponse(STATIC_DIR / "index.html", headers=NO_CACHE)


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def main() -> None:
    parser = argparse.ArgumentParser(description="SQL Index Manager: serves the UI on http://%s:%d." % (HOST, PORT))
    dbconfig.add_arguments(parser)
    theme.add_arguments(parser)
    args = parser.parse_args()
    log.banner(version.VERSION, version.COMMIT)
    log.info("Starting", python=platform.python_version(), os=f"{platform.system()} {platform.machine()}")
    try:                                   # bad config exits here, before serving
        theme.configure(theme.load(args))
        storedb.configure(dbconfig.load(args))
        cfg = storedb.config()
        log.info("Settings", theme=theme.current(), data_engine=cfg.engine)
        log.info("Connecting to the app database", where=store.DB_PATH if cfg.engine == "sqlite" else cfg.describe())
        store.check()
    except (dbconfig.ConfigError, storedb.StoreError) as exc:
        log.error("Cannot start", error=exc)
        sys.exit(2)
    log.info("Connected to the app database", engine=cfg.engine)
    if HOST == "127.0.0.1":
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
    log.info("Stopped")


if __name__ == "__main__":
    main()
