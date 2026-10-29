"""What the dlt sources of the agoranomos bundle share: the connection of the loader role, the
hints of the record of the windows that each load replaces (_agoranomos_reloads), and how a load
reads that record (ADR-0004). The sources of the demo (demo/sources.py) and of the rates of the ECB
(ecb.py) load through them."""

from __future__ import annotations

import copy
import os
from collections.abc import Iterable, Iterator
from datetime import date

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation

MERGE = {"disposition": "merge", "strategy": "delete-insert"}
TEXT, DATE, TIMESTAMP = {"data_type": "text"}, {"data_type": "date"}, {"data_type": "timestamp"}

# Each load also appends the window of each account that it replaced, in the same run as the
# facts: the day of the load in the time zone of the account (as_of), the first and last day of
# the window, and the moment of the load. dlt writes a timestamp as a timestamp with time zone in
# PostgreSQL.
RELOADS = "_agoranomos_reloads"
RELOAD_HINTS = {
    "write_disposition": "append",
    "columns": {
        "account_id": TEXT,
        "as_of": DATE,
        "window_start": DATE,
        "window_end": DATE,
        "now": TIMESTAMP,
        "loaded_at": TIMESTAMP,
    },
}


def own(hints: dict) -> dict:
    """A copy of the hints of a table for one resource, with a dict of its own for each column.
    dlt writes the name of each column into the dict of its hint, in place (dlt 1.31.0,
    ensure_table_schema_columns), so a dict that two columns share, as TEXT and DATE are above,
    would carry the name of the last that dlt saw, and the other column would lose its type."""
    copied = copy.deepcopy(hints)
    copied["columns"] = {name: dict(schema) for name, schema in hints["columns"].items()}
    return copied


def or_marker(rows: Iterable[dict[str, object]]) -> Iterator[object]:
    """The rows of a fact table in its windows, or, when they hold none, the marker with which dlt
    still deletes the destination scope and inserts nothing: without it, a window that comes back
    empty keeps the rows that it had (dlt 1.31.0, merge loading)."""
    empty = True
    for row in rows:
        empty = False
        yield row
    if empty:
        yield dlt.mark.materialize_table_schema()


def credentials() -> dict[str, object]:
    """The connection of the loader role. The password comes from the environment of the task."""
    return {
        "host": os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
        "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
        "database": "warehouse",
        "username": "loader",
        "password": os.environ["WAREHOUSE_LOADER_PASSWORD"],
    }


def has_loads(pipeline) -> bool:
    """Whether the dataset of a pipeline holds a complete load already."""
    try:
        with pipeline.sql_client() as client:
            loads = client.make_qualified_table_name("_dlt_loads")
            # A name that dlt quotes, and no value from outside.
            rows = client.execute_sql(f"SELECT count(*) FROM {loads} WHERE status = 0")  # noqa: S608
    except DatabaseUndefinedRelation:
        return False
    return bool(rows and rows[0][0])


def recorded(pipeline) -> dict[str, tuple[date, date]]:
    """The day of the newest load (as_of) and the last day loaded (window_end) of each account, as
    the loads of the dataset of a pipeline recorded them in _agoranomos_reloads; none before the
    first load."""
    try:
        with pipeline.sql_client() as client:
            table = client.make_qualified_table_name(RELOADS)
            # A name that dlt quotes, and no value from outside.
            rows = client.execute_sql(
                f"SELECT account_id, max(as_of), max(window_end) FROM {table} GROUP BY account_id"  # noqa: S608
            )
    except DatabaseUndefinedRelation:
        return {}
    return {account_id: (as_of, end) for account_id, as_of, end in rows or ()}
