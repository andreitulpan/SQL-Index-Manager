"""Rules: saved jobs that run on a schedule or on demand.

A rebuild rule lists indexes to rebuild, with options, an optional fragmentation condition, a schedule, and
whether to go one at a time or in parallel. It can cover several databases of an environment: each index is
looked up in every database and rebuilt only where it exists (and, with a condition, only where it holds).
With a condition, fragmentation of the listed indexes is measured just before each one and saved to the
database's stored scan; without one, nothing is scanned.

A scan rule scans the indexes and fragmentation of whole databases on a schedule, keeping the Indexes page current.

Runs start from the Scheduler thread or "Run now", work in a background thread, record every index looked at
in the rule's history, and show in the job History as one "Rule run".

Only one rule runs per machine at a time. Others wait in a queue: lowest priority number first, then earliest.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from datetime import datetime

import pyodbc

from . import busy
from . import db
from . import indexes
from . import log
from . import schedule
from . import store

SCHEDULER = "Scheduler"
TICK_SECONDS = 10


class RuleBusy(Exception):
    """The rule is already running, or already waiting for its turn."""


class StopSignal(threading.Event):
    """A run's stop flag. Setting it also cancels the statement each worker is executing; the server rolls back a
    rebuild, so the index is unchanged."""

    def __init__(self) -> None:
        super().__init__()
        self._cursors: set = set()
        self._guard = threading.Lock()

    def set(self) -> None:
        super().set()
        with self._guard:
            cursors = list(self._cursors)
        for cursor in cursors:
            try:
                cursor.cancel()
            except Exception:       # already finished, or the driver can't cancel: the run still stops after this item
                pass

    @contextmanager
    def watching(self, cursor):
        with self._guard:
            self._cursors.add(cursor)
        try:
            if self.is_set():       # stopped between the check and now
                cursor.cancel()
            yield
        finally:
            with self._guard:
                self._cursors.discard(cursor)


_stops: dict[int, StopSignal] = {}

_lock = threading.RLock()
_active: dict[int, int | None] = {}                    # running rule id -> machine id (None: no machine)
_queue: dict[int, tuple[str, str, float]] = {}         # waiting rule id -> (started_by, trigger, time it began to wait)


def _machine_of(rule: dict, machines: dict[str, int | None] | None = None) -> int | None:
    machines = store.environment_machines() if machines is None else machines
    return machines.get(rule["environment"].lower())


def queued() -> dict[int, dict]:
    """Waiting rules: rule id -> {"behind": name of the rule running on its machine or None, "position": 1 = next}."""
    with _lock:
        waiting = dict(_queue)
        active = dict(_active)
    if not waiting:
        return {}
    machines = store.environment_machines()
    all_rules = {r["id"]: r for r in store.list_rules()}      # in priority order
    out: dict[int, dict] = {}
    seen: dict[int | None, int] = {}
    for rule in sorted((all_rules[i] for i in waiting if i in all_rules), key=lambda r: (r["priority"], waiting[r["id"]][2])):
        machine = _machine_of(rule, machines)
        seen[machine] = seen.get(machine, 0) + 1
        running = next((all_rules[i]["name"] for i, m in active.items() if m == machine and i in all_rules), None)
        out[rule["id"]] = {"behind": running, "position": seen[machine]}
    return out


def cancel_queued(rule_id: int) -> bool:
    """Removes a rule from the queue; False if it wasn't waiting."""
    with _lock:
        return _queue.pop(rule_id, None) is not None


def request_run(rule_id: int, started_by: str, trigger: str) -> int | None:
    """Starts the rule, or queues it if another rule of its machine is running. Returns the run id, or None if
    queued. RuleBusy if it is already running or queued."""
    rule = store.get_rule(rule_id)
    if rule is None:
        raise LookupError(f"Rule {rule_id} does not exist.")
    with _lock:
        if rule_id in _active or rule_id in _queue:
            raise RuleBusy(f"Rule '{rule['name']}' is already {'running' if rule_id in _active else 'waiting for its turn'}.")
        _queue[rule_id] = (started_by, trigger, time.monotonic())
        started = dispatch()
    return next((run_id for rid, run_id in started if rid == rule_id), None)


def dispatch() -> list[tuple[int, int]]:
    """Starts the first waiting rule on every idle machine. Returns (rule id, run id) pairs."""
    started: list[tuple[int, int]] = []
    with _lock:
        if not _queue:
            return started
        machines = store.environment_machines()
        by_id = {r["id"]: r for r in store.list_rules()}
        for rule_id in [i for i in _queue if i not in by_id]:       # deleted while waiting
            del _queue[rule_id]
        busy_machines = set(_active.values())
        for rule in sorted((by_id[i] for i in _queue), key=lambda r: (r["priority"], _queue[r["id"]][2])):
            machine = _machine_of(rule, machines)
            if machine in busy_machines:
                continue
            started_by, trigger, _ = _queue.pop(rule["id"])
            try:
                started.append((rule["id"], start_run(rule["id"], started_by, trigger)))
                busy_machines.add(machine)
            except Exception as exc:        # couldn't start: record it and free the machine for the next one
                store.add_skipped_run(rule, trigger, started_by, f"Could not start: {exc}")
    return started


def stopping() -> set[int]:
    """Ids of runs asked to stop that haven't ended."""
    return {run_id for run_id, event in list(_stops.items()) if event.is_set()}


def stop_run(run_id: int) -> bool:
    """Asks a run to stop: cancels the current statement and skips the rest."""
    event = _stops.get(run_id)
    if event is None:
        return False
    event.set()
    return True


def databases_label(databases: list[str]) -> str:
    return ", ".join(databases) if len(databases) <= 3 else f"{len(databases)} databases"


def start_run(rule_id: int, started_by: str, trigger: str) -> int:
    """Starts a run in the background and returns its id. RuleBusy if the rule is already running."""
    rule = store.get_rule(rule_id)
    if rule is None:
        raise LookupError(f"Rule {rule_id} does not exist.")
    if not busy.claim(("rule", rule_id)):
        raise RuleBusy(f"Rule '{rule['name']}' is already running.")
    try:
        run_id = store.start_rule_run(rule, trigger, started_by)
        job_id = store.start_job("rule", rule["environment"], databases_label(rule["databases"]), started_by, rule["name"])
    except Exception:
        busy.release(("rule", rule_id))
        raise
    with _lock:
        _active[rule_id] = _machine_of(rule)
    stop = _stops[run_id] = StopSignal()
    threading.Thread(target=_execute, args=(rule, run_id, job_id, stop),
                     name=f"rule-{rule_id}-run-{run_id}", daemon=True).start()
    return run_id


def _execute(rule: dict, run_id: int, job_id: int, stop: threading.Event) -> None:
    started = time.monotonic()
    error = None
    try:
        run = store.get_rule_run(run_id)
        items = run["items"]
        fatal: list[str] = []          # e.g. no ODBC driver: nothing else can work
        unreachable: dict[str, str] = {}   # database -> why it couldn't be opened; its other indexes are skipped
        if rule.get("kind") == "scan":
            for item in items:
                _scan_item(rule, item, run["started_by"], stop, fatal)
        else:
            execution = rule["execution"]
            workers = min(execution["max_parallel"], len(items)) if execution["mode"] == "parallel" else 1
            if workers <= 1:
                for item in items:
                    _rebuild_item(rule, item, stop, fatal, unreachable)
            else:
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"rule-{rule['id']}") as pool:
                    list(pool.map(lambda item: _rebuild_item(rule, item, stop, fatal, unreachable), items))
    except Exception as exc:   # a bug, not a failed index (those are recorded per index)
        error = f"{type(exc).__name__}: {exc}"
        store.mark_unfinished_items(run_id, "interrupted", error)
    finally:
        try:
            _finish(rule, run_id, job_id, started, stop, error)
        finally:
            _stops.pop(run_id, None)
            busy.release(("rule", rule["id"]))
            with _lock:
                _active.pop(rule["id"], None)
            try:
                dispatch()          # machine is free: start the next waiting rule
            except Exception:
                pass


def _finish(rule: dict, run_id: int, job_id: int, started: float, stop: threading.Event, error: str | None) -> None:
    run = store.get_rule_run(run_id)
    scan = rule.get("kind") == "scan"
    done, failed, skipped = run["n_success"], run["n_failed"], run["n_skipped"]
    if error:
        status = "failed"
    elif stop.is_set():
        status = "stopped"
    elif failed:
        status = "partial" if done else "failed"
    else:
        status = "success"
    if not error and failed:
        first = next(i for i in run["items"] if i["status"] == "failed")
        if scan:
            error = f"{failed} of {run['n_total']} failed, first: {first['database_name']}: {first['message']}"
        else:
            where = f" in {first['database_name']}" if first["database_name"] and len(rule["databases"]) > 1 else ""
            error = f"{failed} of {run['n_total']} failed, first: {first['index_name']}{where}: {first['message']}"
    duration = round((time.monotonic() - started) * 1000)
    store.finish_rule_run(run_id, status, duration, error)
    store.set_job_details(job_id, f"{done} scanned, {skipped} skipped, {failed} failed" if scan else f"{done} rebuilt, {skipped} skipped, {failed} failed")
    store.finish_job(job_id, "failed" if status in ("failed", "partial") else status, duration, error)


# --- scanning ---

class ScanBusy(Exception):
    """That database is being scanned."""


def _why(exc: Exception) -> str:
    return db.clean_db_error(exc) if isinstance(exc, pyodbc.Error) else f"{type(exc).__name__}: {exc}"


def _scan_database(environment: str, database: str, user: str, stop: StopSignal | None = None) -> tuple[int, int]:
    """What both scan buttons do: reads indexes and fragmentation and stores them as the latest scans. Returns
    the index and fragmentation row counts. ScanBusy if the database is already being scanned."""
    keys = [(environment, database, kind, None) for kind in ("indexes", "fragmentation")]
    claimed = []
    try:
        for key in keys:
            if not busy.claim(key):
                raise ScanBusy(f"Someone is already scanning {database}.")
            claimed.append(key)
        with db.connection(environment, database) as conn:
            cursor = conn.cursor()
            try:
                with stop.watching(cursor) if stop else nullcontext():
                    found = indexes.list_indexes(cursor)
                    fragmentation = indexes.scan_fragmentation(cursor)
            finally:
                cursor.close()
        store.save_snapshot(environment, database, "indexes", user, found)
        store.save_snapshot(environment, database, "fragmentation", user, fragmentation)
        return len(found["indexes"]), len(fragmentation)
    finally:
        for key in claimed:
            busy.release(key)


def _scan_item(rule: dict, item: dict, user: str, stop: threading.Event, fatal: list[str]) -> None:
    item_id, database = item["id"], item["database_name"]
    if stop.is_set():
        return store.finish_item(item_id, "skipped", "Stopped before it started.")
    if fatal:
        return store.finish_item(item_id, "skipped", "Not tried: " + fatal[0])
    store.start_item(item_id)
    started = time.monotonic()
    status, message = "failed", None
    try:
        found, measured = _scan_database(rule["environment"], database, user, stop)
        status, message = "success", f"{found:,} indexes scanned, fragmentation measured on {measured:,}."
    except ScanBusy as exc:
        status, message = "skipped", str(exc)
    except db.DbError as exc:
        if not isinstance(exc, db.DbConnectError):
            fatal.append(str(exc))
        message = str(exc)
    except Exception as exc:
        if stop.is_set():
            status, message = "skipped", "Stopped while scanning; nothing was saved."
        else:
            message = _why(exc)
    finally:
        store.finish_item(item_id, status, message, duration_ms=round((time.monotonic() - started) * 1000))


def _rebuild_item(rule: dict, item: dict, stop: threading.Event, fatal: list[str], unreachable: dict[str, str]) -> None:
    item_id = item["id"]
    database = item["database_name"]
    schema_name, table_name, index_name = item["schema_name"], item["table_name"], item["index_name"]
    if stop.is_set():
        return store.finish_item(item_id, "skipped", "Stopped before it started.")
    if fatal:
        return store.finish_item(item_id, "skipped", "Not tried: " + fatal[0])
    if database in unreachable:
        return store.finish_item(item_id, "skipped", "Not tried: " + unreachable[database])
    key = (rule["environment"], database, "rebuild", f"{schema_name}.{table_name}.{index_name}")
    if not busy.claim(key):
        return store.finish_item(item_id, "skipped", "Someone else is rebuilding this index right now.")
    store.start_item(item_id)
    started = time.monotonic()
    result = {"status": "failed", "message": None}
    try:
        with db.connection(rule["environment"], database) as conn:
            cursor = conn.cursor()
            try:
                with stop.watching(cursor):
                    _rebuild(rule, database, cursor, item, result)
            finally:
                cursor.close()
    except indexes.IndexNotFound as exc:
        # normal with several databases: the index exists only in some of them
        result.update(status="skipped", message=f"Not in {database}." if len(rule["databases"]) > 1 else str(exc))
    except indexes.RebuildRefused as exc:
        result.update(status="skipped", message=str(exc))
    except db.DbConnectError as exc:
        unreachable[database] = str(exc)
        result.update(status="failed", message=str(exc))
    except db.DbError as exc:
        fatal.append(str(exc))
        result.update(status="failed", message=str(exc))
    except Exception as exc:
        if stop.is_set():       # stopped: the server rolled the statement back, the index is unchanged
            result.update(status="skipped", message="Stopped before it finished; nothing was changed.")
        elif isinstance(exc, pyodbc.Error):
            result.update(status="failed", message=db.clean_db_error(exc))
        else:
            result.update(status="failed", message=f"{type(exc).__name__}: {exc}")
    finally:
        busy.release(key)
        store.finish_item(item_id, result.pop("status"), result.pop("message"),
                          duration_ms=round((time.monotonic() - started) * 1000), **result)


def _rebuild(rule: dict, database: str, cursor: pyodbc.Cursor, item: dict, result: dict) -> None:
    """Rebuilds one index of one database if the rule's condition holds there. Fills `result` for finish_item."""
    ix = indexes.find_index(cursor, item["schema_name"], item["table_name"], item["index_name"])
    threshold = rule["min_fragmentation"]
    if threshold is not None:
        before = indexes.measure_index(cursor, ix)
        if before is None:
            return result.update(status="skipped", message="Columnstore indexes aren't measured, so the fragmentation "
                                 "condition can't be checked.")
        result.update(page_count=before["page_count"], frag_before=before["fragmentation_pct"])
        store.update_fragmentation_entry(rule["environment"], database, before)   # the Indexes page gets the fresh measurement
        if before["page_count"] < indexes.MIN_PAGES:
            return result.update(status="skipped", message=f"Too small ({before['page_count']:,} pages, under "
                                 f"{indexes.MIN_PAGES:,}); fragmentation isn't meaningful.")
        if before["fragmentation_pct"] <= threshold:
            return result.update(status="skipped", message=f"Fragmentation {before['fragmentation_pct']}% is not above {threshold:g}%.")

    options = indexes.adapt_options(ix, indexes.RebuildOptions(**rule["options"]))
    result["options"] = indexes.with_clause(ix, options)
    cursor.execute(indexes.rebuild_statement(ix, options))
    after = indexes.measure_index(cursor, ix)
    if after:
        store.update_fragmentation_entry(rule["environment"], database, after)
        result.update(page_count=after["page_count"], frag_after=after["fragmentation_pct"])
    result["status"] = "success"


# --- scheduler ---

class Scheduler:
    """Wakes every few seconds and starts due rules. Times that passed while the server was off are not made up
    (a rebuild at a random later hour is worse than a missed one)."""

    def __init__(self, tick: float = TICK_SECONDS):
        self._tick = tick
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="rule-scheduler", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        last = datetime.now().astimezone()
        while not self._stop.wait(self._tick):
            now = datetime.now().astimezone()
            try:
                self.fire_due(last, now)
            except Exception as exc:   # a bad rule must not stop the scheduler
                log.error("Scheduler failed to start due rules", error=f"{type(exc).__name__}: {exc}")
            try:
                dispatch()      # a busy machine may be free now
            except Exception as exc:
                log.error("Scheduler failed to dispatch waiting rules", error=f"{type(exc).__name__}: {exc}")
            last = now

    @staticmethod
    def fire_due(after: datetime, upto: datetime) -> list[int]:
        """Starts every rule scheduled in (after, upto]; returns the ids of the runs started."""
        due = [r for r in store.list_rules() if schedule.latest_occurrence(r["schedule"], after, upto) is not None]
        with _lock:
            for rule in due:          # all wait first so the lowest priority number starts
                if rule["id"] in _active or rule["id"] in _queue:
                    state = "running" if rule["id"] in _active else "waiting for its turn"
                    store.add_skipped_run(rule, "schedule", SCHEDULER, f"Rule '{rule['name']}' is already {state}. This scheduled run was skipped.")
                else:
                    _queue[rule["id"]] = (SCHEDULER, "schedule", time.monotonic())
            started = dispatch()
        due_ids = {r["id"] for r in due}
        return [run_id for rule_id, run_id in started if rule_id in due_ids]
