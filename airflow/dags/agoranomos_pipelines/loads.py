"""What the dlt sources of the agoranomos bundle share: the connection of the loader role, the
destination, the format of the files, the contract of the schema and the settings of dlt for each
load, the hints of the journal of the loads (_agoranomos_loads), which records the window of each
account that a load took, or that the account failed, how a load reads that journal, and how it
counts its rows (ADR-0004, ADR-0038). The sources of the demo (demo/sources.py), of the rates of
the ECB (ecb.py) and of Google Ads (google_ads.py) load through them, and the source of the
settings of the model (settings/source.py) takes the connection, the format and the counts."""

from __future__ import annotations

import copy
import os
from collections.abc import Iterable, Iterator
from datetime import date

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation

# The write of a fact table: each load appends its rows under its own _dlt_load_id, with no keys and
# no merge, and the staging models read, for each account and day, the rows of the newest load whose
# window in the journal covers the day, of the whole loads that the run of the transform pinned into
# core.loads (dbt/macros/pin_loads.sql and newest_load.sql, ADR-0038, ADR-0046). So a load replaces
# the days of its window whole, and a row that a platform no longer sends goes, as the newest load
# does not hold it. The rows of the earlier loads stay in raw until the core accepts them
# (ADR-0035): until then, raw grows by a window with each load.
APPEND = "append"
# The contract of the schema of every load of a source: a new table or column reaches raw, and a
# value whose type differs from that of its column stops the load, before any of it is written,
# until the connector is updated, rather than make a column of a variant type (dlt 1.32.0, schema
# contracts).
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
# The format of the files of every load, as an argument of each run: dlt writes the rows of a table
# into a CSV file, and PostgreSQL reads it with COPY ... FROM STDIN, row by row, instead of parsing
# statements of up to 16 MiB of SQL, the default insert_values, for which the kernel killed backends
# of warehouse-db in October 2026 (ADR-0038). The state of dlt alone still goes as an INSERT. The
# writer of csv puts an empty string, a None and a key that a row lacks alike, and COPY reads all
# three as NULL, in a nullable column, so a table of raw holds no empty string (dlt 1.31.0,
# PostgresCsvCopyJob, FORCE_NULL).
FILE_FORMAT = "csv"
# The settings of dlt 1.32.0 that every load sets, as environment variables of its configuration:
DLT_SETTINGS = {
    # The versions of the entities go through the staging dataset (scd2), which keeps the rows of
    # the last load unless dlt is told to empty it.
    "LOAD__TRUNCATE_STAGING_DATASET": "true",
    # Two jobs at a time, a COPY of a file or a merge of a table, each on a backend of warehouse-db,
    # and not the 20 of dlt.
    "LOAD__WORKERS": "2",
}
# Money is text in raw, or whole micros as text, never the type decimal of dlt, numeric(38,9) in
# PostgreSQL, which silently loses the tenth place (ADR-0038). A number of the API that a hint makes
# text keeps its digits: dlt writes it with str(), and staging casts the text exactly, where a cast
# from double precision keeps 15 significant digits. A table that an earlier release made keeps its
# double precision, which dlt does not change and COPY fills from the same text.
TEXT, DATE, TIMESTAMP = {"data_type": "text"}, {"data_type": "date"}, {"data_type": "timestamp"}

# Each load also appends a row for each account to the journal of its dataset, in the same run as
# its facts, so under the same _dlt_load_id. An account that the load took has the status loaded:
# the day of the load in the time zone of the account (as_of), the first and last day of the window
# that the load took (window_from, window_to), and the moment of the load. An account whose request
# to its API failed, which the load of Google Ads catches at the account and goes on without
# (ADR-0035), has the status failed, the text of the error as the source gives it, which names no
# secret, the moment of the attempt (now), and no day and no loaded_at: it loaded nothing, so the
# staging models and the next load pass it over, and the Ops collection shows the last load that
# was. A load is whole once _dlt_loads has its row with status 0, which dlt writes after the last
# table of the load. The journal of the releases before v0.5, _agoranomos_reloads, went with their
# merges: a dataset that holds it was loaded in their shape (OLD_JOURNAL). dlt writes a timestamp as
# a timestamp with time zone in PostgreSQL.
LOADS = "_agoranomos_loads"
OLD_JOURNAL = "_agoranomos_reloads"
LOAD_HINTS = {
    "write_disposition": APPEND,
    "columns": {
        "account_id": TEXT,
        "as_of": DATE,
        "window_from": DATE,
        "window_to": DATE,
        "now": TIMESTAMP,
        "loaded_at": TIMESTAMP,
        "status": TEXT,
        "error": TEXT,
    },
}
LOADED, FAILED = "loaded", "failed"


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
    still makes the table with its columns and inserts nothing (dlt 1.32.0). A window without a row
    needs no row to replace its days: the journal holds the window, and a day that the newest load
    covers and does not hold is empty in staging."""
    empty = True
    for row in rows:
        empty = False
        yield row
    if empty:
        yield dlt.mark.materialize_table_schema()


def row_counts(pipeline) -> dict[str, int]:
    """The rows of each table in the last run of a pipeline, as dlt extracted them, without the
    tables whose names start with an underscore: those of dlt and the journal. The counts of its
    normalize step are of no use with csv: its writer adds up the keys of each row (dlt 1.31.0,
    and 1.32.0, CsvWriter.write_data), so they say how many values a file holds. A list in a row,
    a child table of dlt, counts here as the row that holds it."""
    counts: dict[str, int] = {}
    for steps in pipeline.last_trace.last_extract_info.metrics.values():
        for step in steps:
            for table, metrics in step["table_metrics"].items():
                if not table.startswith("_"):
                    counts[table] = counts.get(table, 0) + metrics.items_count
    return counts


def credentials() -> dict[str, object]:
    """The connection of the loader role. The password comes from the environment of the task."""
    return {
        "host": os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
        "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
        "database": "warehouse",
        "username": "loader",
        "password": os.environ["WAREHOUSE_LOADER_PASSWORD"],
    }


def destination():
    """The warehouse as the destination of a load, as the loader role. dlt makes a new table
    without the UNIQUE index that it puts on _dlt_id by default, which cost 1.77 s and 86 MiB of
    WAL for each million rows of a COPY in November 2026 (ADR-0035): a load never looks a row up
    by it, the working folder of a load is temporary, so no file of a load is copied twice, and a
    duplicate is for the checks of staging to find. A table that an earlier release made keeps its
    index."""
    return dlt.destinations.postgres(credentials=credentials(), create_indexes=False)


def run(pipeline, resources: list) -> None:
    """Load the resources in one run of the pipeline, so under one _dlt_load_id, by COPY of CSV
    files and under the contract of the schema."""
    pipeline.run(resources, loader_file_format=FILE_FORMAT, schema_contract=SCHEMA_CONTRACT)


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
    """The day of the newest load (as_of) and the last day loaded (window_to) of each account, as
    the whole loads of the dataset of a pipeline recorded them in _agoranomos_loads; none before the
    first load. A load counts once _dlt_loads has its row with status 0, as for the staging models:
    the rows of a load that stopped half way are not read, so the next load takes their days again.
    A row of a failed account has no day, and an account whose loads all failed has no record, as
    one that no load took yet."""
    try:
        with pipeline.sql_client() as client:
            journal = client.make_qualified_table_name(LOADS)
            loads = client.make_qualified_table_name("_dlt_loads")
            # Names that dlt quotes, and a constant of this module.
            rows = client.execute_sql(
                "SELECT j.account_id, max(j.as_of), max(j.window_to) "  # noqa: S608
                f"FROM {journal} AS j JOIN {loads} AS l "
                "ON l.load_id = j._dlt_load_id AND l.status = 0 "
                f"WHERE j.status = '{LOADED}' GROUP BY j.account_id"
            )
    except DatabaseUndefinedRelation:
        return {}
    return {account_id: (as_of, end) for account_id, as_of, end in rows or () if end is not None}
