"""The one-shot job marts-init: the views of marts on every start (ADR-0037, ADR-0039, ADR-0044).

A run of the transform leaves a view of marts that exists as it is, with its oid, its grants and
its comments, and builds only one that does not exist (dbt/macros/marts_view.sql): a rebuild takes
ACCESS EXCLUSIVE on the name of the view, which would wait for a long query of Metabase and stop
every reader behind it. So a release that changes a view, or a reporting currency that changes,
reaches the warehouse here: ./agoranomos up runs this job after db-init has applied the migrations
of the core, and metabase-init waits for it, so that Metabase finds the views of this release. It
runs dbt of the Airflow image as transformer, as nobody, with the dbt project read-only, on the
network data alone, and writes nothing but the target and the logs of dbt, on a tmpfs.

What it builds, with the var rebuild_views:

* Where the snapshots of the versions of the core exist, the views of marts alone, dbt run
  --select marts: they read the core, which no run drops. Their tests run in the next run of the
  transform, the gate G1 (ADR-0039), as data to fix is no reason to keep the stack from starting.
* Where they do not, as in a new warehouse or one of v0.4, dbt build --select +marts: the seeds,
  the views of staging, the models of the core and the snapshots that the views read, in the order
  of the graph and with their tests (#187), over the loads that the transform has pinned, none in a
  new warehouse. dbt snapshot alone would fail on a warehouse without the views of staging, and the
  views of marts without the snapshots.
* Where core.fx_rates_daily holds rates and none in the reporting currency of .env, as after a
  change of AGORANOMOS_REPORTING_CURRENCY, also core_fx_rates_daily, with the var recompute_rates,
  which computes every day of the rates in the new currency (ADR-0042): the views take the new
  currency as they are built again.

The job takes the advisory lock of dbt/macros/marts_init.sql first, exclusive, on a connection of
its own, and keeps it until dbt ends: the nodes of a run of the transform that write take it,
shared, so the job waits for those that write when it starts, and the nodes that come after wait
for the job, as max_active_runs of the DAG does not reach a service outside Airflow. dbt runs
without DBT_JOB_RUN_ID, so its own nodes do not take the lock.

The file uses the standard library, and psycopg2 of the virtual environment of dbt, whose Python
runs it. Its own problems are one line that names the step, and the exit code is that of dbt.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# The dbt of the Airflow image, the project, and a folder of the tmpfs for the target and the logs.
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = "/tmp/dbt"  # noqa: S108 (a tmpfs of the marts-init container)
# The warehouse as dbt/profiles.yml names it, and the variable that holds the password of
# transformer, whose values dbt hides in its logs.
HOST, PORT, DATABASE, USER = "warehouse-db", 5432, "warehouse", "transformer"
PASSWORD = "DBT_ENV_SECRET_WAREHOUSE_PASSWORD"  # noqa: S105 (the name of a variable)
CURRENCY = "AGORANOMOS_REPORTING_CURRENCY"
# The key of the lock, as marts_init_lock() of dbt/macros/marts_init.sql gives it, and how long the
# job waits for it: a node of the transform writes for at most 10 minutes, its execution timeout.
LOCK = 1634168690
WAIT_FOR_LOCK = "SET lock_timeout = '11min'"
# The snapshots of the versions of the core (dbt/snapshots), which the views of marts read.
SNAPSHOTS = ("dim_ad_group_versions", "dim_ad_versions", "dim_campaign_versions")
RATES = "core_fx_rates_daily"
# How long dbt may take: a build of the whole graph over empty tables takes minutes on a slow
# machine.
DBT_TIMEOUT = 1800
# Whether every snapshot exists in core, and whether the rates of the core are of another reporting
# currency only. transformer reads the catalog and core.fx_rates_daily, which it writes.
STATE = (
    "SELECT (SELECT count(*) FROM pg_catalog.pg_class AS c "
    "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'core' AND c.relname = ANY(%s)) = %s, "
    "EXISTS (SELECT FROM core.fx_rates_daily) "
    "AND NOT EXISTS (SELECT FROM core.fx_rates_daily WHERE reporting_currency = %s)"
)

Connect = Callable[..., Any]
Run = Callable[..., Any]


class JobError(RuntimeError):
    """A problem of the job, explained in one line for the person who runs the stack."""


@dataclass(frozen=True)
class State:
    """What the warehouse holds that decides the build: the snapshots of the core, and rates in
    another reporting currency only."""

    snapshots: bool
    other_currency: bool


def command(state: State, dbt: str = DBT, project: str = PROJECT, work: str = WORK) -> list[str]:
    """The call of dbt for the state of the warehouse."""
    selected = ["marts"] if state.snapshots else ["+marts"]
    variables: dict[str, bool] = {"rebuild_views": True}
    if state.other_currency:
        variables["recompute_rates"] = True
        if state.snapshots:
            selected.append(RATES)
    return [
        dbt,
        "--no-use-colors",
        "run" if state.snapshots else "build",
        "--project-dir",
        project,
        "--profiles-dir",
        project,
        "--target-path",
        f"{work}/target",
        "--log-path",
        f"{work}/logs",
        "--select",
        *selected,
        "--vars",
        json.dumps(variables),
    ]


def described(state: State, currency: str) -> str:
    """The line that says what the job builds, and why."""
    if state.snapshots:
        line = "building the views of marts again"
    else:
        line = (
            "the snapshots of the core do not exist yet: building the views of marts with what "
            "they read, from the seeds to the snapshots, and their tests"
        )
    if state.other_currency:
        line += f", and the rates of the core in {currency}, the new reporting currency"
    return line


def first_line(error: BaseException) -> str:
    """The class of an error and the first line of its message."""
    message = (str(error).strip().splitlines() or [""])[0]
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


def connect_to_warehouse(environ: Mapping[str, str]) -> Any:
    """A connection to the warehouse as transformer, through psycopg2 of the environment of dbt."""
    password = environ.get(PASSWORD)
    if not password:
        raise JobError(f"{PASSWORD} is empty: run ./agoranomos init")
    # Only in the virtual environment of dbt, whose Python runs the job.
    import psycopg2

    return psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=password,
        connect_timeout=30,
        application_name="marts-init",
    )


def lock_and_read(connection: Any, currency: str, log: Callable[[str], None]) -> State:
    """Take the lock, waiting for the nodes of the transform that write now, and read the state."""
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOCK,))
        if not cursor.fetchone()[0]:
            log("waiting for the run of the transform that writes now (at most 11 minutes)")
            cursor.execute(WAIT_FOR_LOCK)
            cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK,))
            cursor.execute("RESET lock_timeout")
        cursor.execute(STATE, (list(SNAPSHOTS), len(SNAPSHOTS), currency))
        snapshots, other_currency = cursor.fetchone()
    return State(snapshots=bool(snapshots), other_currency=bool(other_currency))


def main(
    argv: Sequence[str],
    environ: Mapping[str, str] = os.environ,
    connect: Connect = connect_to_warehouse,
    run: Run = subprocess.run,
) -> int:
    """The marts-init service: python marts_init.py."""

    def log(line: str) -> None:
        print(f"marts-init: {line}", flush=True)

    if list(argv):
        print("marts-init: usage: marts_init.py", file=sys.stderr)
        return 2
    currency = environ.get(CURRENCY) or "USD"
    try:
        connection = connect(environ)
    except JobError as error:
        print(f"marts-init: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # any failure of psycopg2 is one line
        print(
            f"marts-init: cannot connect to the warehouse as {USER}: {first_line(error)}",
            file=sys.stderr,
        )
        return 1
    try:
        try:
            state = lock_and_read(connection, currency, log)
        except Exception as error:
            print(
                "marts-init: cannot take the lock of the views or read the core "
                f"(see ./agoranomos logs marts-init): {first_line(error)}",
                file=sys.stderr,
            )
            return 1
        log(described(state, currency))
        try:
            result = run(command(state), timeout=DBT_TIMEOUT, check=False)
        except subprocess.TimeoutExpired:
            print(f"marts-init: dbt did not end within {DBT_TIMEOUT} seconds", file=sys.stderr)
            return 1
        except OSError as error:
            print(f"marts-init: cannot run dbt ({DBT}): {type(error).__name__}", file=sys.stderr)
            return 1
        if result.returncode != 0:
            print(
                f"marts-init: dbt exited with {result.returncode}: the lines above say why",
                file=sys.stderr,
            )
            return result.returncode if result.returncode > 0 else 1
        log("done")
        return 0
    finally:
        # The lock goes with the connection.
        connection.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
