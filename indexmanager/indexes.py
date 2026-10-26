"""Read-only queries that describe the indexes of a database."""
from __future__ import annotations

from dataclasses import dataclass, replace

import pyodbc

from .db import query

# Catalog metadata only: no special permission needed, fast on large databases.
INDEXES_SQL = """
SELECT
    t.object_id,
    i.index_id,
    s.name                          AS schema_name,
    t.name                          AS table_name,
    i.name                          AS index_name,
    i.type_desc,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_unique,
    i.is_disabled,
    i.fill_factor,
    i.has_filter,
    i.filter_definition,
    COALESCE(cols.key_columns, '')      AS key_columns,
    COALESCE(cols.included_columns, '') AS included_columns,
    COALESCE(sz.row_count, 0)           AS row_count,
    COALESCE(sz.used_mb, 0)             AS size_mb,
    COALESCE(sz.partition_count, 1)     AS partition_count,
    cmp.data_compression
FROM sys.tables t
JOIN sys.schemas s  ON s.schema_id = t.schema_id
JOIN sys.indexes i  ON i.object_id = t.object_id
OUTER APPLY (
    -- SQL Server rejects two ordered aggregates with different ORDER BYs in one scope, so both use
    -- the same one: key columns have key_ordinal > 0 (in key order), included ones have 0 (in column order).
    SELECT
        STRING_AGG(CASE WHEN ic.key_ordinal > 0 THEN
                        c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
                    END, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id) AS key_columns,
        STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ', ')
            WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id)                    AS included_columns
    FROM sys.index_columns ic
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
) cols
OUTER APPLY (
    SELECT
        SUM(ps.row_count)                              AS row_count,
        CAST(SUM(ps.used_page_count) * 8 / 1024.0 AS decimal(18, 2)) AS used_mb,
        COUNT(*)                                       AS partition_count
    FROM sys.dm_db_partition_stats ps
    WHERE ps.object_id = i.object_id AND ps.index_id = i.index_id
) sz
OUTER APPLY (
    -- one value for the index, or MIXED when its partitions are compressed differently
    SELECT CASE WHEN MIN(p.data_compression_desc) = MAX(p.data_compression_desc)
                THEN MIN(p.data_compression_desc) ELSE 'MIXED' END AS data_compression
    FROM sys.partitions p
    WHERE p.object_id = i.object_id AND p.index_id = i.index_id
) cmp
WHERE t.is_ms_shipped = 0
  AND i.is_hypothetical = 0
ORDER BY s.name, t.name, i.index_id
"""

# Needs VIEW DATABASE STATE; fetched separately so a missing permission only costs the usage columns.
USAGE_SQL = """
SELECT
    object_id, index_id,
    user_seeks, user_scans, user_lookups, user_updates,
    (SELECT MAX(v) FROM (VALUES (last_user_seek), (last_user_scan), (last_user_lookup)) AS x(v)) AS last_read,
    last_user_update AS last_write
FROM sys.dm_db_index_usage_stats
WHERE database_id = DB_ID()
"""

# Indexes smaller than this aren't worth maintaining, so their fragmentation isn't measured.
MIN_PAGES = 1000

# dm_db_index_physical_stats over a whole database takes minutes with thousands of (mostly tiny)
# indexes, so catalog page counts are checked first and only indexes with at least MIN_PAGES are
# measured (LIMITED mode). Smaller ones are returned with a page count and no fragmentation, so the
# UI can tell "too small" from "never measured".
FRAGMENTATION_SQL = f"""
WITH candidates AS (
    SELECT ps.object_id, ps.index_id, SUM(ps.used_page_count) AS pages
    FROM sys.dm_db_partition_stats ps
    JOIN sys.tables t ON t.object_id = ps.object_id AND t.is_ms_shipped = 0
    GROUP BY ps.object_id, ps.index_id
)
SELECT c.object_id, c.index_id, c.pages AS page_count, CAST(NULL AS decimal(5, 2)) AS fragmentation_pct
FROM candidates c
WHERE c.pages < {MIN_PAGES}
UNION ALL
SELECT c.object_id, c.index_id, f.page_count, f.fragmentation_pct
FROM candidates c
CROSS APPLY (
    SELECT
        SUM(page_count) AS page_count,
        CASE WHEN SUM(page_count) > 0
             THEN CAST(SUM(avg_fragmentation_in_percent * page_count) / SUM(page_count) AS decimal(5, 2))
             ELSE 0 END AS fragmentation_pct
    FROM sys.dm_db_index_physical_stats(DB_ID(), c.object_id, c.index_id, NULL, 'LIMITED')
    WHERE alloc_unit_type_desc = 'IN_ROW_DATA'
) f
WHERE c.pages >= {MIN_PAGES}
"""


def list_indexes(cursor: pyodbc.Cursor) -> dict:
    """Every index (heaps included) on user tables, with size and, if the login may read it, usage stats."""
    indexes = query(cursor, INDEXES_SQL)

    usage_available = True
    try:
        usage = {(r["object_id"], r["index_id"]): r for r in query(cursor, USAGE_SQL)}
    except pyodbc.Error:
        usage, usage_available = {}, False

    for ix in indexes:
        u = usage.get((ix["object_id"], ix["index_id"]))
        ix["user_seeks"] = u["user_seeks"] if u else 0
        ix["user_scans"] = u["user_scans"] if u else 0
        ix["user_lookups"] = u["user_lookups"] if u else 0
        ix["user_updates"] = u["user_updates"] if u else 0
        ix["last_read"] = u["last_read"] if u else None
        ix["last_write"] = u["last_write"] if u else None
    return {"usage_available": usage_available, "indexes": indexes}


def scan_fragmentation(cursor: pyodbc.Cursor) -> list[dict]:
    return query(cursor, FRAGMENTATION_SQL)


# --- rebuild ---

class RebuildRefused(Exception):
    """The index can't or shouldn't be rebuilt, or the options don't fit it; the message says why."""


class IndexNotFound(RebuildRefused):
    """The index doesn't exist in this database."""


ROWSTORE_COMPRESSION = ("NONE", "ROW", "PAGE")
COLUMNSTORE_COMPRESSION = ("COLUMNSTORE", "COLUMNSTORE_ARCHIVE")
MAX_MAXDOP = 64


@dataclass(frozen=True)
class RebuildOptions:
    online: bool | None = None          # None: online when the index type allows
    data_compression: str | None = None  # None: keep the current compression
    maxdop: int = 0                      # 0: database/server default
    sort_in_tempdb: bool = False


# The client only names the index. What gets rebuilt is looked up here and the catalog's quoted names go
# into the statement; nothing from the request is pasted into SQL.
FIND_INDEX_SQL = """
SELECT i.object_id, i.index_id, i.type_desc, i.is_disabled,
       s.name AS schema_name, t.name AS table_name, i.name AS index_name
FROM sys.indexes i
JOIN sys.tables t  ON t.object_id = i.object_id AND t.is_ms_shipped = 0
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = ? AND t.name = ? AND i.name = ? AND i.is_hypothetical = 0
"""

MEASURE_SQL = """
SELECT
    SUM(page_count) AS page_count,
    CASE WHEN SUM(page_count) > 0
         THEN CAST(SUM(avg_fragmentation_in_percent * page_count) / SUM(page_count) AS decimal(5, 2))
         ELSE 0 END AS fragmentation_pct
FROM sys.dm_db_index_physical_stats(DB_ID(), ?, ?, NULL, 'LIMITED')
WHERE alloc_unit_type_desc = 'IN_ROW_DATA'
"""


def quote_name(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def supports_online(type_desc: str) -> bool:
    return not any(kind in type_desc for kind in ("XML", "SPATIAL"))


def compression_choices(type_desc: str) -> tuple[str, ...]:
    """DATA_COMPRESSION values that fit the index type (none for XML / spatial indexes)."""
    if "COLUMNSTORE" in type_desc:
        return COLUMNSTORE_COMPRESSION
    if supports_online(type_desc):
        return ROWSTORE_COMPRESSION
    return ()


def with_clause(ix: dict, options: RebuildOptions) -> str:
    """The WITH (...) options of the rebuild; raises RebuildRefused if they don't fit the index."""
    online = supports_online(ix["type_desc"]) if options.online is None else options.online
    if online and not supports_online(ix["type_desc"]):
        raise RebuildRefused(f"{ix['type_desc']} indexes can't be rebuilt online. Choose offline.")
    if not 0 <= options.maxdop <= MAX_MAXDOP:
        raise RebuildRefused(f"MAXDOP must be between 0 and {MAX_MAXDOP}.")
    parts = [f"ONLINE = {'ON' if online else 'OFF'}"]
    if options.data_compression is not None:
        if options.data_compression not in compression_choices(ix["type_desc"]):
            raise RebuildRefused(f"DATA_COMPRESSION = {options.data_compression} can't be used on a "
                                 f"{ix['type_desc']} index.")
        parts.append(f"DATA_COMPRESSION = {options.data_compression}")
    parts.append(f"MAXDOP = {options.maxdop}")
    parts.append(f"SORT_IN_TEMPDB = {'ON' if options.sort_in_tempdb else 'OFF'}")
    return ", ".join(parts)


def adapt_options(ix: dict, options: RebuildOptions) -> RebuildOptions:
    """For rules, which apply one set of options to many indexes: drops what doesn't fit an index (offline for
    XML / spatial, the index's own compression instead of a wrong kind) instead of refusing."""
    can_online = supports_online(ix["type_desc"])
    online = can_online if options.online is None else options.online and can_online
    fits = options.data_compression in compression_choices(ix["type_desc"])
    return replace(options, online=online, data_compression=options.data_compression if fits else None)


def rebuild_statement(ix: dict, options: RebuildOptions = RebuildOptions()) -> str:
    return (f"ALTER INDEX {quote_name(ix['index_name'])} ON "
            f"{quote_name(ix['schema_name'])}.{quote_name(ix['table_name'])} REBUILD WITH ({with_clause(ix, options)})")


def find_index(cursor: pyodbc.Cursor, schema: str, table: str, index: str) -> dict:
    """The index as the catalog knows it now; RebuildRefused if it is gone or can't be rebuilt."""
    found = query(cursor, FIND_INDEX_SQL, schema, table, index)
    if not found:
        raise IndexNotFound(f"Index {index} on {schema}.{table} was not found. It may have been dropped or "
                             "renamed since the last scan; scan the indexes again.")
    ix = found[0]
    if ix["is_disabled"]:
        raise RebuildRefused(f"Index {ix['index_name']} is disabled, and rebuilding would enable it again. "
                             "Enable it deliberately if that is what you want.")
    return ix


def measure_index(cursor: pyodbc.Cursor, ix: dict) -> dict | None:
    """A fragmentation scan row {object_id, index_id, page_count, fragmentation_pct}, or None for columnstore
    indexes (not measured)."""
    if "COLUMNSTORE" in ix["type_desc"]:
        return None
    measured = query(cursor, MEASURE_SQL, ix["object_id"], ix["index_id"])[0]
    pages = measured["page_count"] or 0
    return {
        "object_id": ix["object_id"], "index_id": ix["index_id"], "page_count": pages,
        # as in the scan: below MIN_PAGES, page count only
        "fragmentation_pct": measured["fragmentation_pct"] if pages >= MIN_PAGES else None,
    }


def rebuild_index(cursor: pyodbc.Cursor, schema: str, table: str, index: str,
                  options: RebuildOptions = RebuildOptions(), on_statement=None) -> dict:
    """Rebuilds one index and measures it again. on_statement(clause) is called with the WITH options just before
    the rebuild. Returns {"online": bool, "options": str, "fragmentation": scan row, None for columnstore}."""
    ix = find_index(cursor, schema, table, index)
    statement = rebuild_statement(ix, options)
    clause = with_clause(ix, options)
    if on_statement:
        on_statement(clause)
    cursor.execute(statement)
    return {"online": clause.startswith("ONLINE = ON"), "options": clause, "fragmentation": measure_index(cursor, ix)}
