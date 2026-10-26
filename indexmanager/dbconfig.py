"""Where the app keeps its own data: a local SQLite file or a database server.

With nothing set it uses data/index_manager.sqlite3 (created if missing). To use a server, give host, user and
password together (the engine is inferred from a well-known port, otherwise name it) or one connection URL.
A partial set is an error. The database must already exist and the login must be able to create tables in it.

Each setting is an environment variable and a command-line flag (the flag wins). Secrets also accept a `_FILE`
variable naming a file that holds the value (Docker secrets).

    APP_DB_HOST      host[:port]  (SQL Server: also host\\INSTANCE or host,port)    --db-host
    APP_DB_USER                                                                     --db-user
    APP_DB_PASSWORD  (or APP_DB_PASSWORD_FILE)                                      --db-password
    APP_DB_ENGINE    mysql | mariadb | postgresql | mssql | sqlite                   --db-engine
    APP_DB_NAME      an existing database, default sql_index_manager                --db-name
    APP_DB_URL       mysql://user:password@host:3306/name  (instead of the 3 above) --db-url
    APP_DB_TRUST_CERT  yes: accept the SQL Server's self-signed certificate         --db-trust-cert
"""
from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import unquote, urlsplit

_ALIASES = {"postgres": "postgresql", "pgsql": "postgresql", "pg": "postgresql", "sqlserver": "mssql", "sql-server": "mssql",
            "mysql": "mysql", "mariadb": "mariadb", "postgresql": "postgresql", "mssql": "mssql", "sqlite": "sqlite"}
DEFAULT_PORTS = {"mysql": 3306, "mariadb": 3306, "postgresql": 5432, "mssql": 1433}
_ENGINE_BY_PORT = {3306: "mysql", 5432: "postgresql", 1433: "mssql"}
DEFAULT_DATABASE = "sql_index_manager"

_SAFE_HOST = re.compile(r"^[\w\-.\\]{1,255}$")
_SAFE_NAME = re.compile(r"^[\w\-. $]{1,64}$")
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class ConfigError(Exception):
    """Settings are wrong or incomplete; the message says what to fix."""


@dataclass(frozen=True)
class StoreConfig:
    engine: str                       # sqlite, mysql, mariadb, postgresql or mssql
    host: str | None = None
    port: int | None = None           # None for a SQL Server named instance
    user: str | None = None
    password: str | None = None
    database: str | None = None
    trust_cert: bool = False

    def describe(self) -> str:
        """Safe to log: no password."""
        if self.engine == "sqlite":
            return "the local SQLite file"
        where = self.host + (f":{self.port}" if self.port else "")
        return f"{self.engine} database '{self.database}' on {where} as {self.user}"


SQLITE = StoreConfig("sqlite")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group(
        "app database",
        "Where the app keeps its settings and history. Without any of these a local SQLite file is used. For a "
        "server give --db-host, --db-user and --db-password all together (or --db-url). Every one of them can "
        "also be an environment variable: APP_DB_HOST, APP_DB_USER, APP_DB_PASSWORD (or APP_DB_PASSWORD_FILE), "
        "APP_DB_ENGINE, APP_DB_NAME, APP_DB_URL, APP_DB_TRUST_CERT. A flag wins over its variable.")
    group.add_argument("--db-host", metavar="HOST[:PORT]", help="the database server, e.g. db.example.com:3306")
    group.add_argument("--db-user", metavar="USER")
    group.add_argument("--db-password", metavar="PASSWORD", help="visible in the process list: prefer APP_DB_PASSWORD or APP_DB_PASSWORD_FILE")
    group.add_argument("--db-engine", metavar="ENGINE", help="mysql, mariadb, postgresql or mssql; may be left out for the usual ports 3306, 5432 and 1433")
    group.add_argument("--db-name", metavar="NAME", help=f"the existing database to use (default {DEFAULT_DATABASE})")
    group.add_argument("--db-url", metavar="URL", help="engine://user:password@host:port/name, instead of host, user, password and engine")
    group.add_argument("--db-trust-cert", action="store_true", default=None, help="SQL Server: accept a self-signed server certificate")


def _setting(name: str, flag: str | None, env: Mapping[str, str], secret: bool = False) -> str | None:
    """The flag, else the variable, else (for secrets) the contents of the file named by the _FILE variable.
    Empty counts as unset."""
    if flag not in (None, ""):
        return flag
    value = env.get(name, "").strip() if not secret else env.get(name, "")
    path = env.get(name + "_FILE", "").strip() if secret else ""
    if value and path:
        raise ConfigError(f"Set only one of {name} and {name}_FILE.")
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").rstrip("\r\n") or None
        except OSError as exc:
            raise ConfigError(f"Cannot read {name}_FILE ({path}): {exc.strerror or exc}.") from exc
    return value or None


def _engine(value: str, where: str) -> str:
    engine = _ALIASES.get(value.strip().lower())
    if engine is None:
        raise ConfigError(f"{where}: unknown database engine '{value}'. Use one of mysql, mariadb, postgresql, mssql.")
    return engine


def _split_host(host: str, engine: str | None) -> tuple[str, int | None]:
    """'name', 'name:3306', '[::1]:5432', SQL Server's 'name,1433' and 'name\\INSTANCE' -> (name, port or None)."""
    if host.startswith("["):                                     # [IPv6] or [IPv6]:port
        name, _, rest = host[1:].partition("]")
        port_text = rest[1:] if rest.startswith(":") else rest
        if rest and not rest.startswith(":"):
            raise ConfigError(f"APP_DB_HOST '{host}' is not valid.")
    elif "," in host:
        name, _, port_text = host.partition(",")
    elif host.count(":") == 1:
        name, _, port_text = host.partition(":")
    else:
        name, port_text = host, ""
    if "\\" in name and port_text:
        raise ConfigError("A SQL Server named instance (host\\INSTANCE) takes no port.")
    if not (_SAFE_HOST.match(name) or ":" in name and name.replace(":", "").replace(".", "").isalnum()):
        raise ConfigError(f"The database host '{name}' has characters that are not allowed.")
    if port_text:
        if not port_text.isdigit() or not 0 < int(port_text) < 65536:
            raise ConfigError(f"'{port_text}' is not a valid port.")
        return name, int(port_text)
    if "\\" in name:
        return name, None
    return name, DEFAULT_PORTS.get(engine) if engine else None


def _from_url(url: str, trust_cert: bool) -> StoreConfig:
    parts = urlsplit(url)
    engine = _engine(parts.scheme.split("+")[0], "APP_DB_URL") if parts.scheme else None
    if engine in (None, "sqlite"):
        raise ConfigError("APP_DB_URL must look like mysql://user:password@host:3306/name "
                          "(the engine is mysql, mariadb, postgresql or mssql).")
    try:
        port = parts.port
    except ValueError as exc:
        raise ConfigError("APP_DB_URL has an invalid port.") from exc
    user, password = unquote(parts.username or ""), unquote(parts.password or "")
    missing = [n for n, v in (("host", parts.hostname), ("user", user), ("password", password)) if not v]
    if missing:
        raise ConfigError(f"APP_DB_URL is missing the {', '.join(missing)}. It needs all of user, password and host.")
    database = unquote(parts.path.lstrip("/")) or DEFAULT_DATABASE
    return _finish(engine, parts.hostname, port or DEFAULT_PORTS[engine], user, password, database, trust_cert)


def _finish(engine: str, host: str, port: int | None, user: str, password: str, database: str, trust_cert: bool) -> StoreConfig:
    if not _SAFE_NAME.match(database):
        raise ConfigError(f"The database name '{database}' has characters that are not allowed.")
    return StoreConfig(engine, host, port, user, password, database, trust_cert)


def load(args: argparse.Namespace | None = None, env: Mapping[str, str] | None = None) -> StoreConfig:
    """Settings from the command line and environment. Raises ConfigError."""
    env = os.environ if env is None else env
    flag = lambda name: getattr(args, name, None) if args is not None else None
    url = _setting("APP_DB_URL", flag("db_url"), env, secret=True)
    host = _setting("APP_DB_HOST", flag("db_host"), env)
    user = _setting("APP_DB_USER", flag("db_user"), env, secret=True)
    password = _setting("APP_DB_PASSWORD", flag("db_password"), env, secret=True)
    engine_text = _setting("APP_DB_ENGINE", flag("db_engine"), env)
    name = _setting("APP_DB_NAME", flag("db_name"), env)
    trust_text = flag("db_trust_cert")
    trust_text = _setting("APP_DB_TRUST_CERT", None if trust_text is None else ("yes" if trust_text else ""), env)
    if trust_text is not None and trust_text.lower() not in _TRUE | _FALSE:
        raise ConfigError(f"APP_DB_TRUST_CERT must be yes or no, not '{trust_text}'.")
    trust_cert = (trust_text or "").lower() in _TRUE
    engine = _engine(engine_text, "APP_DB_ENGINE") if engine_text else None

    if url:
        if host or user or password or engine or name:
            raise ConfigError("Give either APP_DB_URL or APP_DB_HOST, APP_DB_USER and APP_DB_PASSWORD, not both.")
        return _from_url(url, trust_cert)

    if engine == "sqlite" and not (host or user or password):
        return SQLITE
    if not (host or user or password):
        if engine or name or trust_cert:
            raise ConfigError("APP_DB_ENGINE, APP_DB_NAME and APP_DB_TRUST_CERT only apply together with APP_DB_HOST, APP_DB_USER "
                              "and APP_DB_PASSWORD, which are not set. Leave all of them out to use the local SQLite file.")
        return SQLITE
    missing = [label for label, value in (("APP_DB_HOST (--db-host)", host), ("APP_DB_USER (--db-user)", user),
                                           ("APP_DB_PASSWORD (--db-password)", password)) if not value]
    if missing:
        raise ConfigError(f"The app database settings are incomplete: {', '.join(missing)} not set. Give all of host, user and "
                          "password to use a database server, or none of them to use the local SQLite file.")
    if engine == "sqlite":
        raise ConfigError("APP_DB_ENGINE is sqlite but a host, user and password are also set. Leave them out, or name a server engine.")

    hostname, port = _split_host(host, engine)
    if engine is None:
        engine = _ENGINE_BY_PORT.get(port or 0)
        if engine is None:
            raise ConfigError("Cannot tell which database engine to use from the host. Set APP_DB_ENGINE (--db-engine) to one of "
                              "mysql, mariadb, postgresql, mssql.")
    elif port is None and "\\" not in hostname:
        port = DEFAULT_PORTS[engine]
    return _finish(engine, hostname, port, user, password, name or DEFAULT_DATABASE, trust_cert)
