"""maintenance: the upkeep that the DAG agoranomos_maintenance runs once a day (ADR-0039), each step
safe to repeat, so that a step that fails leaves its work to the next run:

- the retention of ADR-0044: the logs of Airflow older than 30 days, the rows of the runs of dbt in
  the schema ops older than 90 days, the rows of _dlt_loads older than 90 days that no load, no
  state of dlt and no pin needs any more, the accepted rows of the kind window of core.loads older
  than 13 months, the months of the journals of restatements older than 13 months, and the
  Variables that the watcher mode of Cosmos leaves behind for a run older than 90 days;
- the upkeep of the partitions of the core (ADR-0036, ADR-0039): the partitions of the current
  month and the two after it, ANALYZE ONLY of the parents of the facts, which autovacuum does not
  analyze, and VACUUM (FREEZE) of the months of the facts that have left the window of
  restatements;
- what Ops shows: the used and the free disk, and once a month the core against the totals that
  the platforms give for each account and day (core.platform_totals_daily).

Each step on the core connects as transformer, which owns no table of the core: the partitions
come and go through core.ensure_partition and core.drop_partition, SECURITY DEFINER functions of
migrator, and VACUUM and ANALYZE take the right MAINTAIN that the migrations give transformer.
The rows of _dlt_loads belong to loader, which owns the raw schemas, and the cleanup of _dlt_loads
connects as loader. The steps write what they did into tables of ops, which dbt creates empty and
never fills (dbt/models/ops/ops_maintenance_steps.sql, ops_disk.sql, ops_core_totals.sql): a step
that runs before the first run of the transform has made them does its work and records nothing.

The rest of the retention runs elsewhere. A task has no access to the database of Airflow, so the
one-shot airflow-init cleans it with airflow db clean on each start, and Metabase cleans the log of
its queries itself (compose.yaml).
"""

from __future__ import annotations

import logging
import os
import re
import shlex
import shutil
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

log = logging.getLogger(__name__)

# Where compose.yaml mounts the volume of the logs of Airflow, airflow-logs, in every Airflow
# container: the logs of the tasks, dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<n>.log, and
# those of the DAG processor, dag_processor/<day>/<bundle>/<file>.log, with the link
# dag_processor/latest to the folder of the day (Airflow 3.3.2).
LOGS = "/opt/airflow/logs"
LOG_RETENTION_DAYS = 30
# The folders that the deletion of their files leaves empty go once they have been empty for a day,
# so that a folder that a task has just made, before it opens its log, stays.
EMPTY_FOLDER_MINUTES = 24 * 60

# How long the rows of the runs of dbt stay in ops. The Ops collection reads the last 30 days.
OPS_RETENTION_DAYS = 90
# The tables of elementary 0.26.1 in ops that grow with each run of dbt, each with the column of
# the age of a row, which their configs declare (meta.timestamp_column): elementary writes the
# moment of the insert there, as current_timestamp::timestamp of its session. The other tables of
# elementary hold the project as it is, its models, tests and sources, and replace their rows. A
# table that a later elementary adds is not cleaned until it is listed here or below, which is the
# safe side: tests/stack/test_retention.py compares the two with the manifest of the project.
OPS_TABLES = {
    "dbt_invocations": "created_at",
    "dbt_run_results": "created_at",
    "dbt_source_freshness_results": "created_at",
    "elementary_test_results": "created_at",
    "test_result_rows": "created_at",
}
# These grow too, but only with the tests of anomalies and of schema changes of elementary, which
# the project does not use, so they stay empty. They keep the history that those tests compare
# with, which a deletion by age would cut, so they stay out.
OPS_TABLES_LEFT_OUT = ("data_monitoring_metrics", "schema_columns_snapshot")

# The partitioned tables of the core (postgres/migrations/0005 and 0006): the facts, by month of
# report_date and kept for good, and the journals of restatements, by month of replaced_at and kept
# JOURNAL_MONTHS. Each partition is named <table>_YYYY_MM, as core.ensure_partition names it.
FACTS = (
    "ad_stats_campaign_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_conversions_campaign_daily",
    "web_sessions_daily",
)
JOURNALS = (
    "ad_stats_campaign_daily_restatements",
    "ad_stats_ad_group_daily_restatements",
    "ad_stats_ad_daily_restatements",
)
# The months after the current one, in UTC, whose partitions the maintenance makes: as many as
# core.ensure_partition allows, so that a stop of the maintenance of two months still leaves a
# partition for each row. The pin of the transform makes the months of the windows that it pins.
MONTHS_AHEAD = 2
# The window of restatements of the trigger of the freeze (0005_create_the_facts.sql): a row of a
# month that ended before date_trunc('month', current_date - 92) is frozen, so such a month of a
# fact changes only when a load of history comes again, and its partition is frozen by VACUUM once
# after each change. tests/unit/test_maintenance.py compares the number with the migration.
FROZEN_DAYS = 92
# How long the journals of restatements and the accepted rows of core.loads stay (ADR-0044,
# ADR-0046): a month of a journal goes once the whole of it is older than this, so a row stays 13
# to 14 months.
JOURNAL_MONTHS = 13
LOADS_MONTHS = 13
# How long the rows of _dlt_loads stay (ADR-0044), but those that a load, dlt or a pin still needs.
DLT_LOADS_DAYS = 90
# The Airflow Variables in which the producer of the watcher mode of Cosmos 1.16.0 keeps the backup
# of its XComs when its last write to the task store failed, named cosmos_xcom_backup__<DAG>__
# [<task group>__]<run>, with every character of the ID of the run that is not a letter, a digit, a
# hyphen or an underscore as _ (cosmos/operators/_watcher/xcom.py). A failed run leaves its
# Variable behind (astronomer/astronomer-cosmos#2823), and a Variable has no time of its own, so
# its age is that of the moment in the ID of its run, as Airflow 3.3.2 makes the IDs of the runs:
# <type>__<ISO moment>, with a random suffix when the run has no logical date (DagRun.
# generate_run_id, and the types of DagRunType). A Variable of a run older than VARIABLE_DAYS, the
# time that the database of Airflow keeps a run, goes; one whose name holds no such ID of a run, as
# a run that a person named, stays.
VARIABLE_PREFIX = "cosmos_xcom_backup__"
VARIABLE_DAYS = 90
VARIABLE_MOMENT = re.compile(
    r"__(?:backfill|scheduled|manual|operator_triggered|asset_triggered|asset_materialization)"
    r"__(\d{4})-(\d{2})-(\d{2})T(\d{2})_(\d{2})_(\d{2})"
)

# The tables of ops into which the maintenance writes what it did, which dbt creates, empty, with
# their columns (dbt/models/ops), and how long their rows stay, by their column of time.
# tests/unit/test_maintenance.py compares the columns with the models.
STEPS = "ops.ops_maintenance_steps"
DISK = "ops.ops_disk"
TOTALS = "ops.ops_core_totals"
STEP_COLUMNS = ("finished_at", "maintenance_run_id", "task", "target", "seconds", "rows_deleted")
DISK_COLUMNS = (
    "measured_at",
    "maintenance_run_id",
    "disk_bytes",
    "disk_free_bytes",
    "warehouse_bytes",
    "core_bytes",
    "window_bytes",
    "window_free_bytes",
)
TOTALS_COLUMNS = (
    "checked_at",
    "maintenance_run_id",
    "platform",
    "account_id",
    "first_day",
    "last_day",
    "days",
    "days_differing",
    "platform_impressions",
    "core_impressions",
    "platform_clicks",
    "core_clicks",
    "platform_spend",
    "core_spend",
)
OWN_TABLES = {
    STEPS: ("finished_at", "90 days"),
    DISK: ("measured_at", "90 days"),
    TOTALS: ("checked_at", "13 months"),
}
# The days before the current one, in UTC, that the monthly comparison takes (ADR-0039).
TOTALS_DAYS = 90

# How long VACUUM (FREEZE) may go on in one run: once it is past, the step starts no other
# partition, and the next run takes the rest. The first freeze of a long history can take longer
# than a run has, and every partition that it froze stays frozen.
FREEZE_BUDGET = timedelta(minutes=20)
# core.drop_partition waits at most 2 seconds for its lock, behind a reader or a writer of the
# journal (#189): a refusal is normal, and the step tries again a few times, then leaves the month
# to the next run, without failing.
DROP_ATTEMPTS = 4
DROP_WAIT_SECONDS = 15
# The SQLSTATE of a lock that the lock timeout gave up on.
LOCK_NOT_AVAILABLE = "55P03"


class MaintenanceError(RuntimeError):
    """A step of the maintenance whose work did not happen, though no statement failed."""


@dataclass(frozen=True)
class Step:
    """What a step of the maintenance did to one table: when it ended, the task, the table or the
    schema, the seconds that it took, and the rows that it deleted, where it deletes rows."""

    finished_at: datetime
    task: str
    target: str
    seconds: float
    rows: int | None = None


def clean_logs_command(folder: str = LOGS) -> str:
    """The script of the task that deletes the logs of Airflow older than LOG_RETENTION_DAYS, as the
    user of the tasks, with no database and no network. The files go first, by their own time: the
    folder of a run takes the time of the last entry made in it, not of the last write into a file,
    so a new attempt in an old run would go with its folder. Then the folders that are empty, one
    under another in one pass, once they have been empty for EMPTY_FOLDER_MINUTES: the deletion of
    a file gives its folder a new time, so the folders that the files leave go at a later run. A
    link, such as dag_processor/latest, is neither a file nor a folder for find, and stays."""
    return (
        "set -eu\n"
        f"logs={shlex.quote(folder)}\n"
        f'find "$logs" -mindepth 1 -type f -mmin +{LOG_RETENTION_DAYS * 24 * 60} -delete\n'
        f'find "$logs" -mindepth 1 -depth -type d -empty -mmin +{EMPTY_FOLDER_MINUTES} -delete\n'
    )


def transformer_credentials() -> dict[str, object]:
    """The connection of the transformer role, which owns the schema ops and the tables that dbt
    builds in it, elementary's among them. The password comes from the environment of the task,
    where dbt finds it too."""
    return {
        "host": os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
        "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
        "dbname": "warehouse",
        "user": "transformer",
        "password": os.environ["DBT_ENV_SECRET_WAREHOUSE_PASSWORD"],
    }


def loader_credentials() -> dict[str, object]:
    """The connection of the loader role, which owns the raw schemas, as the loads connect
    (loads.credentials, which this module does not import, as it imports dlt). The password comes
    from the environment of the task."""
    return {
        "host": os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
        "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
        "dbname": "warehouse",
        "user": "loader",
        "password": os.environ["WAREHOUSE_LOADER_PASSWORD"],
    }


@contextmanager
def connected(
    credentials: dict[str, object],
    *,
    autocommit: bool = False,
    application_name: str = "agoranomos_maintenance",
) -> Iterator:
    """A connection to the warehouse, closed at the end, with the name of its application, which
    pg_stat_activity shows. With autocommit, each statement commits by itself, as VACUUM, which
    runs in no transaction, needs, and a view of the statistics is read anew for each statement.
    The warnings of the server, such as those of a VACUUM or an ANALYZE that skips a table, go to
    the log."""
    # Imported here and not at the top: the DAG processor parses the file of the DAG often.
    import psycopg2

    connection = psycopg2.connect(
        **credentials, connect_timeout=10, application_name=application_name
    )
    connection.autocommit = autocommit
    with closing(connection):
        try:
            yield connection
        finally:
            log_notices(connection)


@contextmanager
def in_transaction(credentials: dict[str, object]) -> Iterator:
    """A cursor in one transaction of a connection of its own, which commits at the end of the
    block, or rolls back on an error, and is then closed."""
    with connected(credentials) as connection, connection, connection.cursor() as cursor:
        yield cursor


def log_notices(connection) -> None:
    """Log the warnings and notices that the server sent since the last call, and forget them."""
    for notice in connection.notices:
        log.warning("PostgreSQL: %s", notice.strip())
    del connection.notices[:]


def table_exists(cursor, name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
    return bool(cursor.fetchone()[0])


def record(cursor, table: str, columns: tuple[str, ...], rows: Iterable[tuple]) -> int:
    """Insert rows into a table of ops of the maintenance, and delete its rows past the retention
    of OWN_TABLES: the number inserted, which is 0 when the table does not exist yet, as before the
    first run of the transform."""
    rows = list(rows)
    if not table_exists(cursor, table):
        log.info(
            "%s does not exist yet, as no run of the transform has made it: not recorded", table
        )
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    for row in rows:
        cursor.execute(
            # The names are the constants of this module, and the values go as parameters.
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",  # noqa: S608
            row,
        )
    column, kept = OWN_TABLES[table]
    cursor.execute(f"DELETE FROM {table} WHERE {column} < now() - interval '{kept}'")  # noqa: S608
    return len(rows)


def record_steps(connection, run_id: str, steps: list[Step]) -> None:
    """Write the steps into ops.ops_maintenance_steps, in a transaction of their own."""
    rows = [(s.finished_at, run_id, s.task, s.target, s.seconds, s.rows) for s in steps]
    autocommit = connection.autocommit
    connection.autocommit = False
    try:
        with connection, connection.cursor() as cursor:
            record(cursor, STEPS, STEP_COLUMNS, rows)
    finally:
        connection.autocommit = autocommit


def clean_ops(days: int = OPS_RETENTION_DAYS) -> dict[str, int]:
    """Delete the rows of OPS_TABLES older than days, as transformer, each table in a transaction of
    its own, and return how many rows each lost. A table that does not exist yet, before the first
    run of the transform has built the tables of elementary, is passed over. A delete takes ROW
    EXCLUSIVE, which the inserts of the hooks of elementary do not wait for; a statement that waits
    on a lock is bounded by the lock timeout of the role."""
    # Imported here and not at the top: the DAG processor parses the file of the DAG often.
    import psycopg2

    deleted: dict[str, int] = {}
    connection = psycopg2.connect(
        **transformer_credentials(), connect_timeout=10, application_name="agoranomos_maintenance"
    )
    with closing(connection):
        for table, column in OPS_TABLES.items():
            # The block commits when it ends, and rolls back on an error.
            with connection, connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"ops.{table}",))
                if not cursor.fetchone()[0]:
                    log.info("ops.%s does not exist yet: nothing to clean", table)
                    continue
                cursor.execute(
                    # The names are the constants above, and the number goes as a parameter.
                    f"DELETE FROM ops.{table} "  # noqa: S608
                    f"WHERE {column} < localtimestamp - make_interval(days => %s)",
                    (days,),
                )
                deleted[table] = cursor.rowcount
            log.info("Deleted %d rows older than %d days from ops.%s", deleted[table], days, table)
    return deleted


def partitions(cursor, table: str) -> list[tuple[str, date]]:
    """The partitions of a partitioned table of the core, by name, with the first day of the month
    of each, in the order of the months. A partition that does not have the name that
    core.ensure_partition gives is passed over, and logged."""
    cursor.execute(
        "SELECT c.relname FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = %s::regclass",
        (f"core.{table}",),
    )
    found = []
    pattern = re.compile(rf"{re.escape(table)}_(\d{{4}})_(\d{{2}})")
    for (name,) in cursor.fetchall():
        match = pattern.fullmatch(name)
        if match is None:
            log.warning(
                "core.%s is a partition of core.%s without a month in its name", name, table
            )
            continue
        found.append((name, date(int(match[1]), int(match[2]), 1)))
    return sorted(found, key=lambda item: item[1])


def utc_month(cursor, months: int = 0, days: int = 0) -> date:
    """The first day of the month, in UTC on the clock of the warehouse, that lies the given months
    after the month of the day that lies the given days before today: as the migrations and
    core.ensure_partition count the months."""
    cursor.execute(
        "SELECT (date_trunc('month', (now() AT TIME ZONE 'UTC')::date - %s)"
        " + make_interval(months => %s))::date",
        (days, months),
    )
    return cursor.fetchone()[0]


def make_partitions(run_id: str) -> list[str]:
    """Make the partitions of the current month and of the MONTHS_AHEAD after it, in UTC, of every
    partitioned table of the core, through core.ensure_partition, and return those that it made.
    The function attaches a table made beforehand, so the parent takes SHARE UPDATE EXCLUSIVE and
    the readers do not wait; an existing partition takes no lock. Each call commits by itself, one
    table after another, so a call never holds the lock of another table (#189)."""
    made: list[str] = []
    steps: list[Step] = []
    with connected(transformer_credentials(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            months = [utc_month(cursor, months=n) for n in range(MONTHS_AHEAD + 1)]
            for table in FACTS + JOURNALS:
                for month in months:
                    started = time.monotonic()
                    cursor.execute(
                        "SELECT core.ensure_partition(%s::regclass, %s)", (f"core.{table}", month)
                    )
                    if cursor.fetchone()[0]:
                        name = f"core.{table}_{month:%Y_%m}"
                        made.append(name)
                        steps.append(
                            Step(datetime.now(UTC), "make_partitions", name, seconds_since(started))
                        )
        log.info("Made %d partitions of the core: %s", len(made), ", ".join(made) or "none")
        record_steps(connection, run_id, steps)
    return made


def seconds_since(started: float) -> float:
    return round(time.monotonic() - started, 3)


def statistics(cursor, relation: str) -> dict[str, object]:
    """The row of pg_stat_all_tables of a table: when it was last vacuumed and analyzed, by hand and
    by autovacuum, and its rows inserted since its last vacuum and its dead rows."""
    cursor.execute(
        "SELECT last_vacuum, last_autovacuum, last_analyze, n_ins_since_vacuum, n_dead_tup"
        " FROM pg_stat_all_tables WHERE relid = %s::regclass",
        (relation,),
    )
    row = cursor.fetchone()
    names = ("last_vacuum", "last_autovacuum", "last_analyze", "inserted", "dead")
    return dict(zip(names, row or (None,) * len(names), strict=True))


def analyze_the_facts(run_id: str) -> dict[str, float]:
    """ANALYZE ONLY each parent of the facts, which autovacuum does not analyze, so that PostgreSQL
    plans a query across the months from statistics of the whole table: the seconds of each.
    Without MAINTAIN on the parent, ANALYZE skips it with a WARNING and succeeds (#189), so each
    parent is checked afterwards in pg_stat_all_tables, and a parent that it did not analyze fails
    the step once the others are done."""
    seconds: dict[str, float] = {}
    skipped: list[str] = []
    steps: list[Step] = []
    with connected(transformer_credentials(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            for table in FACTS:
                relation = f"core.{table}"
                cursor.execute("SELECT clock_timestamp()")
                before = cursor.fetchone()[0]
                started = time.monotonic()
                cursor.execute(f"ANALYZE ONLY {relation}")
                seconds[table] = seconds_since(started)
                log_notices(connection)
                after = statistics(cursor, relation)["last_analyze"]
                if after is None or after < before:
                    skipped.append(relation)
                    continue
                steps.append(Step(datetime.now(UTC), "analyze_the_facts", relation, seconds[table]))
        log.info("ANALYZE ONLY of the facts, in seconds: %s", seconds)
        record_steps(connection, run_id, steps)
    if skipped:
        raise MaintenanceError(
            f"ANALYZE ONLY did not analyze {', '.join(skipped)}: transformer lacks MAINTAIN on"
            " them, as the warnings above say"
        )
    return seconds


def needs_freeze(found: dict[str, object]) -> bool:
    """Whether a partition of a frozen month changed since the maintenance last froze it: it has
    never been vacuumed by hand, or rows went in or died since, or autovacuum, which runs only
    after changes and does not freeze every row, ran after the last freeze. A reset of the
    statistics, as after a crash, freezes every frozen month once more."""
    if found["last_vacuum"] is None or found["inserted"] or found["dead"]:
        return True
    autovacuum = found["last_autovacuum"]
    return autovacuum is not None and autovacuum > found["last_vacuum"]


def freeze_old_months(run_id: str, budget: timedelta = FREEZE_BUDGET) -> list[str]:
    """VACUUM (FREEZE) each partition of a fact whose month has left the window of restatements and
    changed since it was last frozen, one after another, and return those that it froze. A frozen
    month changes only when a load of history comes again, so its rows can be frozen for good and
    no later vacuum needs to read them again. VACUUM takes SHARE UPDATE EXCLUSIVE on the partition,
    which neither the readers nor the writers of the fact wait for. Without MAINTAIN on a
    partition, VACUUM skips it with a WARNING and succeeds (#189), so each is checked afterwards in
    pg_stat_all_tables, and a partition that it did not vacuum fails the step once the others are
    done. The step starts no partition once the budget is spent, and leaves the rest to the next
    run."""
    frozen: list[str] = []
    skipped: list[str] = []
    left: list[str] = []
    steps: list[Step] = []
    started_all = time.monotonic()
    with connected(transformer_credentials(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            bound = utc_month(cursor, days=FROZEN_DAYS)
            for table in FACTS:
                for name, month in partitions(cursor, table):
                    relation = f"core.{name}"
                    if month >= bound or not needs_freeze(statistics(cursor, relation)):
                        continue
                    if time.monotonic() - started_all > budget.total_seconds():
                        left.append(relation)
                        continue
                    cursor.execute("SELECT clock_timestamp()")
                    before = cursor.fetchone()[0]
                    started = time.monotonic()
                    cursor.execute(f"VACUUM (FREEZE) {relation}")
                    took = seconds_since(started)
                    log_notices(connection)
                    after = statistics(cursor, relation)["last_vacuum"]
                    if after is None or after < before:
                        skipped.append(relation)
                        continue
                    frozen.append(relation)
                    steps.append(Step(datetime.now(UTC), "freeze_old_months", relation, took))
        log.info(
            "VACUUM (FREEZE) of %d partitions of the months before %s in %.1f s: %s",
            len(frozen),
            bound.isoformat(),
            time.monotonic() - started_all,
            ", ".join(frozen) or "none",
        )
        if left:
            log.info("The next run freezes the %d partitions left: %s", len(left), ", ".join(left))
        record_steps(connection, run_id, steps)
    if skipped:
        raise MaintenanceError(
            f"VACUUM (FREEZE) did not vacuum {', '.join(skipped)}: transformer lacks MAINTAIN on"
            " them, as the warnings above say"
        )
    return frozen


def drop_old_journal_months(
    run_id: str, attempts: int = DROP_ATTEMPTS, wait: float = DROP_WAIT_SECONDS
) -> list[str]:
    """Drop each month of a journal of restatements that ended more than JOURNAL_MONTHS months
    before the current month, in UTC, through core.drop_partition, and return those that it
    dropped. The function takes ACCESS EXCLUSIVE on the journal for at most 2 seconds and gives up
    behind a reader or a writer of it: the step tries again after a wait, and leaves a month that
    it could not take to the next run, which is no failure. A journal has no foreign key, so the
    drop touches no table that a dashboard reads (#189)."""
    # Imported here and not at the top: the DAG processor parses the file of the DAG often.
    import psycopg2

    dropped: list[str] = []
    refused: list[str] = []
    steps: list[Step] = []
    with connected(transformer_credentials(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            bound = utc_month(cursor, months=-JOURNAL_MONTHS)
            for table in JOURNALS:
                for name, month in partitions(cursor, table):
                    if month >= bound:
                        continue
                    started = time.monotonic()
                    if not drop_month(cursor, table, month, attempts, wait, psycopg2.Error):
                        # The journal is in use: its other months wait for the next run too.
                        refused.append(f"core.{name}")
                        break
                    dropped.append(f"core.{name}")
                    steps.append(
                        Step(
                            datetime.now(UTC),
                            "drop_old_journal_months",
                            f"core.{name}",
                            seconds_since(started),
                        )
                    )
        log.info(
            "Dropped %d months of the journals before %s: %s",
            len(dropped),
            bound.isoformat(),
            ", ".join(dropped) or "none",
        )
        if refused:
            log.warning("The next run drops the months that were in use: %s", ", ".join(refused))
        record_steps(connection, run_id, steps)
    return dropped


def drop_month(cursor, table: str, month: date, attempts: int, wait: float, error_class) -> bool:
    """Drop the partition of a month of a journal through core.drop_partition, trying again after a
    wait while its lock timeout gives up: whether it is gone, or was never there."""
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute("SELECT core.drop_partition(%s::regclass, %s)", (f"core.{table}", month))
            cursor.fetchone()
            return True
        except error_class as error:
            if error.pgcode != LOCK_NOT_AVAILABLE:
                raise
            log.info("core.%s of %s is in use (attempt %d of %d)", table, month, attempt, attempts)
            if attempt < attempts:
                time.sleep(wait)
    return False


def dlt_loads_delete(schema: str, has_state: bool, has_journal: bool) -> str:
    """The DELETE of the rows of <schema>._dlt_loads older than DLT_LOADS_DAYS, as %(days)s, but
    those that a load, dlt or a pin still reads:

    - the newest whole load of the schema, so that loads.has_loads() finds one;
    - the load of the newest state of each pipeline that dlt restores, as it joins
      _dlt_pipeline_state with the whole loads of _dlt_loads and takes the largest load_id
      (dlt 1.32.0, SqlJobClientBase.get_stored_state);
    - the loads that hold the newest day (as_of) and the last day loaded (window_to) of each
      account in the journal, from which loads.recorded() starts the next window of the account,
      so that no stop makes a source load its whole history again;
    - each load with a row of the journal that core.loads does not hold yet, which the next pin is
      to take, as the pin takes only the loads that _dlt_loads marks whole (pin_loads.sql).

    A load that core.loads holds is read through core.loads, and its row here only serves to pin it,
    so it goes; the pin needs both this row and the journal, so no later pin takes it again, and
    core.loads keeps it. The schema and its tables are names of the catalog, quoted."""
    if not re.fullmatch(r"raw_[a-z0-9_]+", schema):
        raise ValueError(f"{schema} is not the name of a raw schema of dlt")
    # The names below are those of the schema, checked above, and of the tables of dlt and of the
    # journal: no value from outside, and no quote to escape.
    quoted = f'"{schema}"'
    loads = f"{quoted}._dlt_loads"
    newest = f"SELECT load_id FROM {loads} WHERE status = 0 ORDER BY load_id DESC LIMIT 1"  # noqa: S608
    kept = [newest]
    if has_state:
        kept.append(
            "SELECT DISTINCT ON (s.pipeline_name) l.load_id"  # noqa: S608
            f" FROM {quoted}._dlt_pipeline_state AS s"
            f" JOIN {loads} AS l ON l.load_id = s._dlt_load_id AND l.status = 0"
            " ORDER BY s.pipeline_name, l.load_id DESC"
        )
    if has_journal:
        journal = f"{quoted}._agoranomos_loads"
        whole = (
            "SELECT j._dlt_load_id, j.account_id, j.as_of, j.window_to"  # noqa: S608
            f" FROM {journal} AS j"
            f" JOIN {loads} AS l ON l.load_id = j._dlt_load_id AND l.status = 0"
            " WHERE j.status = 'loaded'"
        )
        kept.append(
            f"SELECT w._dlt_load_id FROM ({whole}) AS w"  # noqa: S608
            " JOIN (SELECT account_id, max(as_of) AS as_of, max(window_to) AS window_to"
            f" FROM ({whole}) AS a GROUP BY account_id) AS b"
            " ON b.account_id = w.account_id AND (w.as_of = b.as_of OR w.window_to = b.window_to)"
        )
        kept.append(
            f"SELECT j._dlt_load_id FROM {journal} AS j"  # noqa: S608
            " WHERE NOT EXISTS (SELECT FROM core.loads AS p"
            f" WHERE p.source = '{schema.removeprefix('raw_')}'"
            " AND p.load_id = j._dlt_load_id AND p.account_id = j.account_id)"
        )
    # Each in parentheses, as a branch of a union with its own ORDER BY or LIMIT needs.
    union = "\n    UNION\n    ".join(f"({query})" for query in kept)
    return (
        f"WITH kept (load_id) AS (\n    {union}\n)\n"
        f"DELETE FROM {quoted}._dlt_loads AS d\n"
        "WHERE d.inserted_at < now() - make_interval(days => %(days)s)\n"
        "    AND NOT EXISTS (SELECT FROM kept WHERE kept.load_id = d.load_id)"
    )


def clean_dlt_loads(run_id: str, days: int = DLT_LOADS_DAYS) -> dict[str, int]:
    """Delete the rows of _dlt_loads older than days in each raw schema of loader, but those that
    dlt_loads_delete keeps, as loader, each schema in a transaction of its own, and return how many
    rows each lost. A delete takes ROW EXCLUSIVE, which the insert of a load does not wait for. The
    journal keeps its rows, and the other tables of raw keep theirs until the run of the transform
    that accepts their load deletes them (acceptance.py, ADR-0046)."""
    deleted: dict[str, int] = {}
    steps: list[Step] = []
    with connected(loader_credentials()) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT schemaname FROM pg_tables WHERE tablename = '_dlt_loads'"
                " AND schemaname LIKE 'raw\\_%' AND tableowner = current_user ORDER BY 1"
            )
            schemas = [row[0] for row in cursor.fetchall()]
        for schema in schemas:
            # Only the names of the catalog that dlt gives a dataset: no quote to escape.
            if not re.fullmatch(r"[a-z0-9_]+", schema):
                log.warning("%s is not a name of a dataset of dlt: passed over", schema)
                continue
            started = time.monotonic()
            with connection, connection.cursor() as cursor:
                has_state = table_exists(cursor, f"{schema}._dlt_pipeline_state")
                has_journal = table_exists(cursor, f"{schema}._agoranomos_loads")
                cursor.execute(dlt_loads_delete(schema, has_state, has_journal), {"days": days})
                deleted[schema] = cursor.rowcount
            steps.append(
                Step(
                    datetime.now(UTC),
                    "clean_dlt_loads",
                    f"{schema}._dlt_loads",
                    seconds_since(started),
                    deleted[schema],
                )
            )
            log.info(
                "Deleted %d rows older than %d days from %s._dlt_loads",
                deleted[schema],
                days,
                schema,
            )
    with connected(transformer_credentials()) as connection:
        record_steps(connection, run_id, steps)
    return deleted


def core_loads_delete(raw_loads: str | None) -> str:
    """The DELETE of the accepted rows of the kind window of core.loads of the source %(source)s
    that finished more than LOADS_MONTHS months ago, as %(months)s, but those that a run or a load
    still reads:

    - the newest accepted load of the kind window of each account, from whose last day a load is
      to start the next window of the account (ADR-0038);
    - each accepted load newer than the oldest pinned load of its account, which can still win a
      day of a pinned one in staging (pinned_loads in newest_load.sql);
    - a load whose row _dlt_loads of its raw schema, raw_loads, still holds, as the next pin would
      take it again as a new load: the cleanup of _dlt_loads removes that row first (ADR-0046).

    A pinned row stays whatever its age: no run has accepted it, and staging reads its rows. A row
    of the kind history stays too: the history of Google Ads plans its next portion from the
    windows that core.loads holds, and ops.ops_history counts its portions (ADR-0038)."""
    query = (
        "DELETE FROM core.loads AS l\n"
        "WHERE l.source = %(source)s\n"
        "    AND l.status = 'accepted'\n"
        "    AND l.kind = 'window'\n"
        "    AND l.finished_at < now() - make_interval(months => %(months)s)\n"
        "    AND EXISTS (SELECT FROM core.loads AS n WHERE n.source = l.source"
        " AND n.account_id = l.account_id AND n.status = 'accepted' AND n.outcome = 'loaded'"
        " AND n.kind = 'window'"
        " AND n.load_id::numeric > l.load_id::numeric)\n"
        "    AND NOT EXISTS (SELECT FROM core.loads AS p WHERE p.source = l.source"
        " AND p.account_id = l.account_id AND p.status = 'pinned' AND p.outcome = 'loaded'"
        " AND p.load_id::numeric < l.load_id::numeric)"
    )
    if raw_loads is not None:
        query += f"\n    AND NOT EXISTS (SELECT FROM {raw_loads} AS d WHERE d.load_id = l.load_id)"
    return query


def clean_core_loads(run_id: str, months: int = LOADS_MONTHS) -> dict[str, int]:
    """Delete the accepted rows of the kind window of core.loads older than months that
    core_loads_delete does not keep, as transformer, each source in a transaction of its own, and
    return how many rows each lost. A source whose raw schema has a table _dlt_loads that
    transformer may not read keeps its rows: the cleanup cannot tell whether a pin would take them
    again."""
    deleted: dict[str, int] = {}
    steps: list[Step] = []
    with connected(transformer_credentials()) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT source FROM core.loads WHERE status = 'accepted'"
                " AND finished_at < now() - make_interval(months => %s) ORDER BY 1",
                (months,),
            )
            sources = [row[0] for row in cursor.fetchall()]
        for source in sources:
            # The name of a schema raw_<source> that dlt gives a dataset: no quote to escape.
            if not re.fullmatch(r"[a-z0-9_]+", source):
                log.warning("%s is not the name of a source: core.loads keeps its rows", source)
                continue
            started = time.monotonic()
            with connection, connection.cursor() as cursor:
                # By the catalog, which any role may read, as a name of a schema that transformer
                # may not enter fails a lookup by name.
                cursor.execute(
                    "SELECT c.oid IS NOT NULL, coalesce(has_schema_privilege(n.oid, 'USAGE')"
                    " AND has_table_privilege(c.oid, 'SELECT'), false)"
                    " FROM (SELECT) AS one"
                    " LEFT JOIN pg_namespace AS n ON n.nspname = %s"
                    " LEFT JOIN pg_class AS c"
                    " ON c.relnamespace = n.oid AND c.relname = '_dlt_loads'",
                    (f"raw_{source}",),
                )
                exists, readable = cursor.fetchone()
                if exists and not readable:
                    log.warning(
                        "transformer may not read raw_%s._dlt_loads: core.loads keeps its rows",
                        source,
                    )
                    continue
                cursor.execute(
                    core_loads_delete(f'"raw_{source}"._dlt_loads' if exists else None),
                    {"source": source, "months": months},
                )
                deleted[source] = cursor.rowcount
            steps.append(
                Step(
                    datetime.now(UTC),
                    "clean_core_loads",
                    f"core.loads of {source}",
                    seconds_since(started),
                    deleted[source],
                )
            )
            log.info(
                "Deleted %d accepted rows older than %d months of %s from core.loads",
                deleted[source],
                months,
                source,
            )
        record_steps(connection, run_id, steps)
    return deleted


def variable_moment(key: str) -> datetime | None:
    """The moment in the ID of the run of a Variable of the backup of Cosmos, in UTC, or None: the
    last moment that its name holds, as the ID of the run comes last."""
    found = VARIABLE_MOMENT.findall(key)
    if not found:
        return None
    try:
        return datetime(*map(int, found[-1]), tzinfo=UTC)
    except ValueError:
        return None


def clean_cosmos_variables(
    keys: Iterable[str],
    delete: Callable[[str], None],
    now: datetime,
    days: int = VARIABLE_DAYS,
) -> tuple[list[str], list[str]]:
    """Delete each Variable of the backup of the watcher mode of Cosmos whose run is older than
    days, through delete, and return the names deleted and those kept for they hold no moment.
    The names are those that the Task SDK lists with the prefix, from the database of Airflow."""
    deleted: list[str] = []
    undated: list[str] = []
    for key in keys:
        if not key.startswith(VARIABLE_PREFIX):
            continue
        moment = variable_moment(key)
        if moment is None:
            undated.append(key)
            continue
        if moment < now - timedelta(days=days):
            delete(key)
            deleted.append(key)
    log.info("Deleted %d Variables of Cosmos of runs older than %d days", len(deleted), days)
    if undated:
        log.info("Kept %d Variables of Cosmos whose names hold no moment", len(undated))
    return deleted, undated


# The bytes of the heap of each partition of a fact whose month is in the window of restatements,
# with its indexes, and the free space in its heap, which PostgreSQL estimates from the rows that
# it counts and the average width of a row in the statistics of the parent, which ANALYZE ONLY
# keeps: 24 bytes of the header of a row and its columns, aligned to 8, and 4 bytes of its pointer,
# on pages of 8,192 bytes with a header of 24. After each run of the transform, the rows that a
# window replaced leave their room free until the next run takes it (#191).
WINDOW_SQL = """
WITH widths AS (
    SELECT tablename AS parent, sum(avg_width * (1 - null_frac)) AS width
    FROM pg_stats
    WHERE schemaname = 'core' AND inherited AND tablename = ANY(%(facts)s)
    GROUP BY tablename
),
months AS (
    SELECT
        p.relname AS parent,
        c.oid,
        s.n_live_tup,
        CASE WHEN c.relname ~ '_[0-9]{4}_[0-9]{2}$'
            THEN to_date(right(c.relname, 7), 'YYYY_MM') END AS month
    FROM pg_inherits AS i
    JOIN pg_class AS c ON c.oid = i.inhrelid
    JOIN pg_class AS p ON p.oid = i.inhparent
    JOIN pg_namespace AS n ON n.oid = p.relnamespace
    LEFT JOIN pg_stat_all_tables AS s ON s.relid = c.oid
    WHERE n.nspname = 'core' AND p.relname = ANY(%(facts)s)
),
sized AS (
    SELECT
        pg_total_relation_size(m.oid) AS total_bytes,
        pg_relation_size(m.oid) AS heap_bytes,
        ceil(coalesce(m.n_live_tup, 0)
            / floor((8192 - 24) / (ceil((24 + d.width) / 8) * 8 + 4))) * 8192 AS used_bytes
    FROM months AS m
    LEFT JOIN widths AS d ON d.parent = m.parent
    WHERE m.month >= %(bound)s
)
SELECT
    coalesce(sum(total_bytes), 0)::bigint,
    -- None while a parent has no statistics yet, before its first ANALYZE.
    sum(CASE WHEN used_bytes IS NOT NULL THEN greatest(heap_bytes - used_bytes, 0) END)::bigint
FROM sized
"""


def record_the_disk(run_id: str, folder: str = LOGS) -> dict[str, int | None]:
    """Record in ops.ops_disk the size and the free space of the disk of the volume of the logs of
    Airflow, which is the disk of the volumes of Docker, the warehouse among them, unless a
    deployment put them elsewhere; the bytes of the warehouse and of its schema core; and the bytes
    of the partitions of the facts of the window of restatements, with an estimate of the free space
    in them. The free space is that of a user that is not root, as shutil.disk_usage gives it."""
    usage = shutil.disk_usage(folder)
    with in_transaction(transformer_credentials()) as cursor:
        bound = utc_month(cursor, days=FROZEN_DAYS)
        cursor.execute(
            "SELECT pg_database_size(current_database()),"
            " (SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0) FROM pg_class AS c"
            " JOIN pg_namespace AS n ON n.oid = c.relnamespace"
            " WHERE n.nspname = 'core' AND c.relkind = 'r')::bigint"
        )
        warehouse, core = cursor.fetchone()
        cursor.execute(WINDOW_SQL, {"facts": list(FACTS), "bound": bound})
        window, window_free = cursor.fetchone()
        found = {
            "disk_bytes": usage.total,
            "disk_free_bytes": usage.free,
            "warehouse_bytes": warehouse,
            "core_bytes": core,
            "window_bytes": window,
            "window_free_bytes": window_free,
        }
        record(
            cursor,
            DISK,
            DISK_COLUMNS,
            [(datetime.now(UTC), run_id, *(found[c] for c in DISK_COLUMNS[2:]))],
        )
    log.info("The disk and the warehouse, in bytes: %s", found)
    return found


# The comparison of the core with the totals that each platform gives for an account and a day
# (G0a of ADR-0035), over the TOTALS_DAYS days before the current one, in UTC: for each account
# with totals, the days, those on which the impressions, the clicks or the spend of its campaigns in
# core.ad_stats_campaign_daily differ from the totals, and the sums of both. A day with totals and
# no row in the core counts as a day that differs. An account in quarantine has no row in
# core.dim_account, and so none in the core: its days differ.
TOTALS_SQL = """
WITH days AS (
    SELECT (now() AT TIME ZONE 'UTC')::date - %(days)s AS first_day,
        (now() AT TIME ZONE 'UTC')::date AS end_day
),
totals AS (
    SELECT t.source, t.account_id, t.report_date, t.impressions, t.clicks, t.spend
    FROM core.platform_totals_daily AS t, days
    WHERE t.report_date >= days.first_day AND t.report_date < days.end_day
),
in_core AS (
    SELECT a.platform AS source, a.account_id, f.report_date,
        sum(f.impressions) AS impressions, sum(f.clicks) AS clicks, sum(f.spend) AS spend
    FROM core.ad_stats_campaign_daily AS f
    JOIN core.dim_account AS a ON a.account_sk = f.account_sk
    CROSS JOIN days
    WHERE f.report_date >= days.first_day AND f.report_date < days.end_day
    GROUP BY a.platform, a.account_id, f.report_date
)
SELECT
    t.source,
    t.account_id,
    min(t.report_date),
    max(t.report_date),
    count(*),
    count(*) FILTER (WHERE c.report_date IS NULL OR c.impressions <> t.impressions
        OR c.clicks <> t.clicks OR c.spend <> t.spend),
    sum(t.impressions),
    coalesce(sum(c.impressions), 0),
    sum(t.clicks),
    coalesce(sum(c.clicks), 0),
    sum(t.spend),
    coalesce(sum(c.spend), 0)
FROM totals AS t
LEFT JOIN in_core AS c
    ON c.source = t.source AND c.account_id = t.account_id AND c.report_date = t.report_date
GROUP BY t.source, t.account_id
ORDER BY t.source, t.account_id
"""


def compare_with_platform_totals(run_id: str, days: int = TOTALS_DAYS) -> int | None:
    """Once a month, compare the core with core.platform_totals_daily over the days before today,
    and record a row for each account in ops.ops_core_totals: the number of accounts, or None when
    the month has its comparison already, or ops.ops_core_totals does not exist yet. A month whose
    first runs found no totals compares again the next day, as a source that writes them may come
    later; the comparison of a month is its first that found some."""
    with in_transaction(transformer_credentials()) as cursor:
        if not table_exists(cursor, TOTALS):
            log.info("%s does not exist yet, as no run of the transform has made it", TOTALS)
            return None
        cursor.execute(
            f"SELECT EXISTS (SELECT FROM {TOTALS} WHERE checked_at"  # noqa: S608 (a constant)
            " >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')"
        )
        if cursor.fetchone()[0]:
            log.info("The core has its comparison with the totals of this month already")
            return None
        cursor.execute(TOTALS_SQL, {"days": days})
        now = datetime.now(UTC)
        rows = [(now, run_id, *row) for row in cursor.fetchall()]
        record(cursor, TOTALS, TOTALS_COLUMNS, rows)
    differing = sum(1 for row in rows if row[7])
    log.info(
        "Compared the core with the totals of %d accounts over %d days: %d with days that differ",
        len(rows),
        days,
        differing,
    )
    return len(rows)
