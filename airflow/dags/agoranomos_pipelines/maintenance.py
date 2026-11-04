"""maintenance: the upkeep that the DAG agoranomos_maintenance runs once a day (ADR-0039), each step
safe to repeat, so that a step that fails leaves its work to the next run. Today it holds the
retention of ADR-0044: the logs of Airflow older than 30 days, and the rows of the runs of dbt in
the schema ops older than 90 days.

The rest of the retention runs elsewhere. A task has no access to the database of Airflow, so the
one-shot airflow-init cleans it with airflow db clean on each start, and Metabase cleans the log of
its queries itself (compose.yaml). The table _dlt_loads of each raw schema keeps its rows: a load
reads it to know whether its dataset holds a load (loads.has_loads), dlt restores the state of a
pipeline from it, and the clean layer is to choose its loads from it until the loads are pinned in
core.loads (ADR-0046). A deletion by date would lose all three after a stop of the stack, so it
comes with the upkeep of core.loads, which is planned.
"""

from __future__ import annotations

import logging
import os
import shlex
from contextlib import closing

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
