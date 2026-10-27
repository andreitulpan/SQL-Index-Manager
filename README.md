<div align="center">

# SQL Index Manager

**See which indexes need attention, rebuild them, and schedule the maintenance, on SQL Server and Azure SQL.**

[![Python](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2017%2B-CC2927?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![Azure SQL](https://img.shields.io/badge/Azure%20SQL-supported-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/products/azure-sql)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

---

SQL Index Manager is a small web app for the people who look after SQL Server databases. It scans your databases,
shows every index with its size, usage and fragmentation, and lets you rebuild them by hand or with **rules** that
run on a schedule, for example every Sunday at 02:00. Every scan and rebuild is recorded under the user who started it.

It runs on your own machine, needs nothing but Python and an ODBC driver, and stores its data in SQLite by default.

## Features

**Look at your indexes**
- Scan the index definitions of a database: key and included columns, type, uniqueness, filters, fill factor,
  row count, size, partitions and data compression.
- Scan fragmentation and page count of every index. Indexes under 1,000 pages are skipped, since they are not worth
  maintaining.
- Usage statistics (reads, writes, last used) to help spot indexes nobody uses. This needs `VIEW DATABASE STATE`;
  without it you still get everything else.
- Scan results are stored, so the UI is fast and never queries your server just to render a page.
- Find where an index exists across your environments.

**Rebuild them safely**
- One-click `ALTER INDEX ... REBUILD`, with the options you would choose by hand: `ONLINE`, `DATA_COMPRESSION`,
  `MAXDOP` and `SORT_IN_TEMPDB`.
- The app adapts the options to the index type and refuses combinations the server would reject.
- The same index is never rebuilt twice at once, whether the second request comes from a button or from a rule.

**Automate it with rules**
- A **rebuild rule** is a saved list of indexes plus the options, an optional fragmentation condition (for example,
  rebuild only above 30%), a schedule, and whether to go one by one or several at once.
- A **scan rule** refreshes the scans of whole databases on a schedule, so the Indexes page does not go stale.
- Schedules are weekly (chosen days), daily, or manual only, in the time zone you pick.
- One rule runs at a time per machine. Rules that become due while another is running wait in a queue, ordered by
  a priority number you control.
- Rules can span several databases of an environment and can be duplicated into another environment.
- Every run records each index it looked at, and every run can be stopped.

**Organise your estate**
- **Machines**, **environments** (each on a machine), **accounts** (server plus login) and **databases**.
- Accounts can be a SQL login or **Microsoft Entra ID** sign-in for Azure SQL. The Entra token is cached, so
  sign-in is silent after the first time.
- A built-in **job history** with filtering.

**Multi-user**
- Admin and regular users, per-user time zone, forced password change on first sign-in.
- Passwords are stored as salted PBKDF2-HMAC-SHA256 hashes (600,000 iterations).
- Two UI themes, `azure` and `ocean`.

## Quick start

### Requirements

- Python 3.10 or newer
- [ODBC Driver 17 or 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
- SQL Server 2017 or newer, or Azure SQL, as the databases you want to manage (the catalog queries use `STRING_AGG`)

### Install and run

```bash
git clone https://github.com/andreitulpan/SQLIndexManager.git
cd SQLIndexManager

python -m venv .venv
# Windows:  .venv\Scripts\activate
# Linux/macOS:  source .venv/bin/activate

pip install -r requirements.txt
python app.py
```

The server starts on <http://127.0.0.1:8765> and opens it in your browser.

### Docker

The image includes the ODBC driver, so nothing else needs installing. Building it accepts Microsoft's
[ODBC driver license terms](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).

```bash
docker compose up -d --build
docker compose logs # shows the one-time admin password
```

Without Compose:

```bash
docker build -t sql-index-manager .
docker run -d --name sql-index-manager \
  -p 127.0.0.1:8765:8765 \
  -v sql-index-manager-data:/app/data \
  sql-index-manager
```

Publish the port on `127.0.0.1` as above unless you put something in front of the app (see Security notes). The
`APP_DB_*` and `APP_THEME` settings below work as `-e` variables. Entra ID sign-in needs a browser on the
machine running the app, so inside a container use SQL logins.

### First sign-in

On the first start with an empty database, the app creates an `admin` user with a **random password** and prints it
**once** to the console. Copy it, sign in, and you will be asked to choose a new one.

### Set up your first database

1. Open the account menu (top right) and add a **Machine**.
2. Add an **Environment** on that machine (for example `Production`).
3. In the environment, add an **Account** (server and login, or Entra ID). Servers look like `host,port`,
   `host\INSTANCE` or `myserver.database.windows.net,1433`.
4. Add the **Databases** the account can see. The app can list them for you.
5. Go to **Indexes**, pick the environment and database, then press **Scan indexes** and **Scan fragmentation**.
6. Create a rule under **Rules**, or rebuild an index straight from the table.

## Configuration

By default the app keeps its own data (settings, scans, history, rules) in SQLite at `data/index_manager.sqlite3`.
To use a database server instead, set the host, user and password together (a partial set is an error). Environment
variables and command-line flags both work, and flags win.

| Environment variable | Flag | Meaning |
|---|---|---|
| `APP_DB_HOST` | `--db-host` | `host[:port]` of the app's own database |
| `APP_DB_USER` / `APP_DB_USER_FILE` | `--db-user` | User name |
| `APP_DB_PASSWORD` / `APP_DB_PASSWORD_FILE` | `--db-password` | Password. The `_FILE` variants read Docker secrets |
| `APP_DB_ENGINE` | `--db-engine` | `mysql`, `mariadb`, `postgresql` or `mssql`. Needed only for a non-default port |
| `APP_DB_NAME` | `--db-name` | Database name, default `sql_index_manager`. It must already exist |
| `APP_DB_TRUST_CERT` | `--db-trust-cert` | `yes` for a SQL Server with a self-signed certificate |
| `APP_DB_URL` | | `engine://user:password@host:port/name`, instead of host, user and password |
| `APP_THEME` | `--theme` | `azure` (default) or `ocean` |

The app creates its tables but never the database itself.

> [!NOTE]
> The app's own store can be SQLite, MySQL, MariaDB, PostgreSQL or SQL Server. The databases it **manages** are
> SQL Server and Azure SQL only.

## How it works

```
 Browser (static UI)
        │  JSON over HTTP
        ▼
 FastAPI backend ──── app store (SQLite / MySQL / MariaDB / PostgreSQL / SQL Server)
        │               environments, accounts, scans, rules, history, users
        │
        ├── Scheduler thread ──▶ rules run in background threads, one at a time per machine
        │
        ▼  pyodbc, Encrypt=yes
 SQL Server / Azure SQL  (SQL login or Entra ID token)
```

| File | What it does |
|---|---|
| [app.py](app.py) | FastAPI app, API routes, start-up |
| [indexmanager/indexes.py](indexmanager/indexes.py) | Catalog queries, fragmentation scan and `ALTER INDEX` rebuilds |
| [indexmanager/rules.py](indexmanager/rules.py) | Rebuild and scan rules, the scheduler, the per-machine queue |
| [indexmanager/schedule.py](indexmanager/schedule.py) | Weekly, daily and manual schedules with time zones |
| [indexmanager/db.py](indexmanager/db.py) | Connections to the managed databases |
| [indexmanager/entra.py](indexmanager/entra.py) | Microsoft Entra ID sign-in and token cache |
| [indexmanager/store.py](indexmanager/store.py), [storedb.py](indexmanager/storedb.py), [dbconfig.py](indexmanager/dbconfig.py) | The app's own database |
| [indexmanager/auth.py](indexmanager/auth.py), [passwords.py](indexmanager/passwords.py) | Sessions, users and password hashing |
| [indexmanager/busy.py](indexmanager/busy.py) | Prevents the same job from running twice |
| [indexmanager/log.py](indexmanager/log.py) | Log lines on stdout/stderr (`docker logs`): start-up, database connection, jobs, errors |
| [indexmanager/static/](indexmanager/static) | The single-page UI and themes |

Local data lives next to `app.py`: `data/` (the SQLite file) and `.auth/` (saved Entra sign-ins). Both are git-ignored.

## Security notes

- The server listens only on `127.0.0.1` (the Docker image sets `APP_HOST=0.0.0.0` so the published port works; bind
  that port to `127.0.0.1` too). Failed sign-ins are **not rate-limited**, so do not expose it to other machines
  without putting something in front of it. Only the `Host` headers `127.0.0.1` and `localhost` are accepted.
- Connections to managed databases use `Encrypt=yes` and certificate validation unless you opt out per account.
- The passwords of the **accounts used to reach your databases** have to be sent to the server, so they are stored
  as typed in the app's database. Protect that database and `data/`, and prefer Entra ID where you can.
- Sessions live in memory. Restarting the app signs everyone out.
- Use a login with only the permissions it needs: `VIEW DATABASE STATE` to scan, and `ALTER` on the tables to rebuild.

## Limitations

- Schedules only run while the server is up.
- Only SQL Server and Azure SQL can be managed today.
- Index rebuilds only. `REORGANIZE` and statistics updates are not covered.

## Contributing

Issues and pull requests are welcome. If you plan a larger change, open an issue first so we can talk about it.

## License

Released under the [MIT License](LICENSE).
