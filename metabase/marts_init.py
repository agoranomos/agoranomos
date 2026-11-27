"""The one-shot job marts-init: the views of marts on every start (ADR-0037, ADR-0039, ADR-0044).

A run of the transform leaves a view of marts that exists as it is, with its oid, its grants and
its comments, and builds only one that does not exist (dbt/macros/marts_view.sql): a rebuild takes
ACCESS EXCLUSIVE on the name of the view, which would wait for a long query of Metabase and stop
every reader behind it. So a release that changes a view, or a reporting currency that changes,
reaches the warehouse here: ./agoranomos up runs this job after db-init has applied the migrations
of the core, and metabase-init waits for it, so that Metabase finds the views of this release. It
runs dbt of the Airflow image as transformer, as nobody, with the dbt project read-only, on the
network data alone, and writes nothing but the target and the logs of dbt, on a tmpfs.

What it builds, with the var rebuild_views, always the seeds of the base package first: a view of
marts carries the rows of the seed of the platforms in its text (dbt/macros/platforms.sql), so a
view built from the seed table as the release before left it would lack the platforms that this
release adds, and a mart that joins them would lose their accounts until a second start. Loading a
seed truncates its table and fills it again; nothing reads the seed tables at query time but the
nodes of a run of the transform, which wait for the lock below, as the views of marts carry the
rows, and metabase_reader and ai_reader may not read staging.

* Where the snapshots that the views read exist, the versions of the campaigns of the core that
  ad_campaign_names reads, the seeds and the views of marts alone, in one dbt build with
  --exclude-resource-type test unit_test, which dbt 1.12 has for build (docs.getdbt.com, "Resource
  type", as of November 2026): dbt loads each seed before the views that read it, in the order of
  the graph, and a seed that fails fails the job. The views read the core, which no run drops.
  Their tests and those of the seeds run in the next run of the transform, the gate G1 (ADR-0039),
  as data to fix is no reason to keep the stack from starting.
* Where they do not, as in a new warehouse or one of v0.4, dbt build --select +marts with the
  seeds: the views of staging, the models of the core and the snapshots that the views read, in
  the order of the graph and with their tests (#187), over the loads that the transform has
  pinned, none in a new warehouse, the settings of the deployment and the sources that no load has
  written yet through empty queries (dbt/macros/relation_or_empty.sql). dbt snapshot alone would
  fail on a warehouse without the views of staging, and the views of marts without the snapshots.
* Where core.fx_rates_daily holds rates and none in the reporting currency of .env, as after a
  change of AGORANOMOS_REPORTING_CURRENCY, also core_fx_rates_daily, with the var recompute_rates,
  which computes every day of the rates in the new currency from what the core holds (ADR-0042),
  and the view ops.ops_rates, with the view of staging of the rates of the file of the deployment
  that the model reads, and the tables of those rates and of the currencies of the listed accounts
  that the view reads: the views take the new currency as they are built again. The job
  then reads ops.ops_rates, and when the core cannot give the rates of some days in the new
  currency, as the raw layer has lost the loads of the rates that the core accepted, it says so in
  a line of warning, which ./agoranomos up shows: the next load of the rates of their source takes
  their history again (agoranomos_pipelines/rates.py). The order of the sources of the rates,
  AGORANOMOS_RATES_SOURCE, reaches dbt here too, so the rates of the new currency come from the
  same sources as in the runs of the transform.

Once dbt has ended well, where a run of the transform has made the tables of elementary that the
views of ops read, the job builds the views and tables of ops of the base package (dbt/models/ops)
too, in a dbt build of their own, with the two tables of staging that ops.ops_rates reads, the
rates of the file of the deployment, with the view of staging that it takes them from, and the
currencies of the listed accounts, which the view reads in place of the views of staging so that
it stays through a run of the transform: the Ops collection
of metabase-init reads them (ops.py, ADR-0029), and a view that the release adds, or a column that
it adds to one, would otherwise make a question of Pipeline health fail until the next run of the
transform, up to a day later. dbt
builds them again on every run of the transform anyway, as the override of the views is for marts
alone. A build of them that fails is a warning and no failure, as the views of marts are built:
the questions on such a view show the error of PostgreSQL until a run of the transform builds it,
as on a new warehouse, where no run has made the tables of elementary yet and the job leaves the
views of ops to the first one.

Then the job drops the relations of earlier releases in LEFTOVERS that the warehouse holds, each
by its name and its kind, and names each in its log: no model of this release builds or reads
them, and no build of dbt takes them with it. On a warehouse of v0.4 the build has by then built
the views of staging again, and the views of v0.4 that read those relations went with the views
that they read. A drop that fails, as for a view of a deployment
over one of them, is a warning, and the relation stays until a start that can drop it.

The job takes the advisory lock of dbt/macros/marts_init.sql first, exclusive, on a connection of
its own, and keeps it until both builds of dbt end and the drops are done: the nodes of a run of
the transform that write take it, shared, so the job waits for those that write when it starts,
and the nodes that come after wait for the job, as max_active_runs of the DAG does not reach a
service outside Airflow. dbt runs without DBT_JOB_RUN_ID, so its own nodes do not take the lock.

The file uses the standard library, and psycopg2 of the virtual environment of dbt, whose Python
runs it. Its own problems are one line that names the step, and the exit code is that of the
build of the views of marts.
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
# The snapshots of the versions of the core (dbt/snapshots) that the views of marts read, and
# that a build of +marts builds: the other snapshots wait for the transform.
SNAPSHOTS = ("dim_campaign_versions",)
# The seeds of the base package, those of the dbt project agoranomos and of no package that it
# installs: an intersection of two methods of selection of dbt.
SEEDS = "package:agoranomos,resource_type:seed"
# What a build of the views leaves to the next run of the transform where the snapshots exist.
TESTS = ("test", "unit_test")
# The views and tables of ops of the base package, which the Ops collection reads: the models of
# dbt/models/ops, and none of the models of elementary, which share the schema.
OPS = "package:agoranomos,path:models/ops"
# The tables of elementary in ops that the views of ops read, which the task elementary of a run of
# the transform makes (ADR-0029): without them, as in a new warehouse, the views of ops wait for
# that run.
ELEMENTARY = ("dbt_invocations", "dbt_run_results", "elementary_test_results")
RATES = "core_fx_rates_daily"
# The view of staging of the rates of the file of the deployment (ADR-0042), which the model of the
# rates reads, and the table that each run fills from it in place, which the view of Ops of the
# rates reads: an upgrade starts before the first run of the transform of the release builds them,
# so a build that selects either reader builds them too. No view reads the view of staging, so a
# build of it takes nothing with it.
SETTINGS_RATES = "stg_settings__rates"
GIVEN_RATES = "int_rates__given"
# The table of the currencies of the accounts that the lists give, which the view of Ops of the
# rates reads, and a build that selects the view builds it too, from the lists that exist then: a
# list that the release before lacked gives no account. The job builds no list itself, as dbt
# replaces a view of staging by dropping the old one with cascade, with the views over it.
CURRENCIES = "int_accounts__currencies"
# The view of Ops of the rates, which names the days without a rate in the reporting currency.
OPS_RATES = "ops_rates"
GAPS = "SELECT first_day_without_rate, days_without_rate FROM ops.ops_rates"
# How long dbt may take: a build of the whole graph over empty tables takes minutes on a slow
# machine.
DBT_TIMEOUT = 1800
# Whether those snapshots exist in core, whether the rates of the core are of another reporting
# currency only, and whether those tables of elementary exist in ops. transformer reads the catalog
# and core.fx_rates_daily, which it writes.
STATE = (
    "SELECT (SELECT count(*) FROM pg_catalog.pg_class AS c "
    "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'core' AND c.relname = ANY(%s)) = %s, "
    "EXISTS (SELECT FROM core.fx_rates_daily) "
    "AND NOT EXISTS (SELECT FROM core.fx_rates_daily WHERE reporting_currency = %s), "
    "(SELECT count(*) FROM pg_catalog.pg_class AS c "
    "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'ops' AND c.relname = ANY(%s)) = %s"
)
# The relations of earlier releases that no model, seed or snapshot of this release builds or
# reads, and that an upgrade would otherwise leave, by name and kind; never a pattern, so that
# nothing of a deployment or of this release goes with them. v0.4 built the statistics of the ads
# and of the ad groups in staging as tables, which hold a copy of them, and the platforms as a view
# over a seed, which dbt fills again in place. Its other models were views over the views of
# staging: dbt builds a view again under its name and drops the old one with cascade, which takes
# the views of v0.4 over it. transformer owns all three, as it ran dbt in v0.4 too.
LEFTOVERS = (
    ("staging.int_ad_stats_ad__unioned", "TABLE"),
    ("staging.int_ad_stats_ad_group__unioned", "TABLE"),
    ("staging.int_ad_platforms__unioned", "VIEW"),
)
# The relkind of pg_class for each kind: DROP TABLE and DROP VIEW fail on a relation of the other
# kind, so a relation of the name and of another kind, which a person made, stays.
RELKINDS = {"TABLE": "r", "VIEW": "v"}
# The relkind of each name of LEFTOVERS, or NULL when the warehouse has no such relation.
KINDS = (
    "SELECT name, (SELECT c.relkind::text FROM pg_catalog.pg_class AS c "
    "WHERE c.oid = to_regclass(name)) FROM unnest(%s::text[]) AS name"
)

Connect = Callable[..., Any]
Run = Callable[..., Any]


class JobError(RuntimeError):
    """A problem of the job, explained in one line for the person who runs the stack."""


@dataclass(frozen=True)
class State:
    """What the warehouse holds that decides the build: the snapshots of the core, rates in
    another reporting currency only, and the tables of elementary that the views of ops read."""

    snapshots: bool
    other_currency: bool
    ops: bool = False


def command(state: State, dbt: str = DBT, project: str = PROJECT, work: str = WORK) -> list[str]:
    """The call of dbt for the state of the warehouse: one build, the seeds of the base package
    first, and without tests where the snapshots exist."""
    selected = [SEEDS, "marts" if state.snapshots else "+marts"]
    variables: dict[str, bool] = {"rebuild_views": True}
    if state.other_currency:
        variables["recompute_rates"] = True
        if state.snapshots:
            selected += [SETTINGS_RATES, RATES]
        selected += [GIVEN_RATES, CURRENCIES, OPS_RATES]
    excluded = ["--exclude-resource-type", *TESTS] if state.snapshots else []
    return build(selected, excluded, variables, dbt, project, work)


def ops_command(
    state: State, dbt: str = DBT, project: str = PROJECT, work: str = WORK
) -> list[str]:
    """The call of dbt that builds the views and tables of ops after the views of marts, with the
    vars of the build before it, and without tests, as the next run of the transform runs them."""
    variables: dict[str, bool] = {"rebuild_views": True}
    if state.other_currency:
        variables["recompute_rates"] = True
    return build(
        [SETTINGS_RATES, GIVEN_RATES, CURRENCIES, OPS],
        ["--exclude-resource-type", *TESTS],
        variables,
        dbt,
        project,
        work,
    )


def build(
    selected: Sequence[str],
    excluded: Sequence[str],
    variables: Mapping[str, bool],
    dbt: str,
    project: str,
    work: str,
) -> list[str]:
    """A dbt build of the read-only project, with its target and its logs on the tmpfs."""
    return [
        dbt,
        "--no-use-colors",
        "build",
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
        *excluded,
        "--vars",
        json.dumps(dict(variables)),
    ]


def described(state: State, currency: str) -> str:
    """The line that says what the job builds, and why."""
    if state.snapshots:
        line = "building the views of marts again from the seeds of the base package"
    else:
        line = (
            "the snapshots that the views of marts read do not exist yet: building the views "
            "with what they read, from the seeds to the snapshots, and their tests"
        )
    if state.other_currency:
        line += f", and the rates of the core in {currency}, the new reporting currency"
    return line


def gaps_line(connection: Any, currency: str) -> str | None:
    """The warning about the days whose rates the core cannot give in the reporting currency, as
    ops.ops_rates names them, or None when it names none."""
    with connection.cursor() as cursor:
        cursor.execute(GAPS)
        row = cursor.fetchone()
    if row is None or not row[1]:
        return None
    first, days = row
    return (
        f"warning: the core holds no rate into {currency} on {days} days from {first} on, of the "
        "currencies of the accounts, as their loads of the rates are accepted and have left the "
        "raw layer: the next load of the rates of their source takes their history again, "
        "agoranomos_bank_of_italy_daily, agoranomos_nbk_daily or agoranomos_ecb_daily, or with "
        "the demo alone agoranomos_demo_daily, and until the transform reads it, the amounts of "
        f"those days in {currency} are empty (ADR-0042)"
    )


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
        cursor.execute(
            STATE,
            (list(SNAPSHOTS), len(SNAPSHOTS), currency, list(ELEMENTARY), len(ELEMENTARY)),
        )
        snapshots, other_currency, ops = cursor.fetchone()
    return State(snapshots=bool(snapshots), other_currency=bool(other_currency), ops=bool(ops))


def build_ops(state: State, run: Run, log: Callable[[str], None]) -> None:
    """Build the views and tables of ops, where the tables of elementary that they read exist;
    a build that fails is a line of warning, and the job goes on."""
    if not state.ops:
        return
    log("building the views of ops of the base package, which the Ops collection reads")
    later = (
        "so the questions of the Ops collection on a view of ops that this release adds or "
        "changes show an error until the next run of the transform builds it"
    )
    try:
        result = run(ops_command(state), timeout=DBT_TIMEOUT, check=False)
    except subprocess.TimeoutExpired:
        log(f"warning: dbt did not build the views of ops within {DBT_TIMEOUT} seconds, {later}")
        return
    except OSError as error:
        log(
            f"warning: cannot run dbt ({DBT}) for the views of ops: {type(error).__name__}, {later}"
        )
        return
    if result.returncode != 0:
        log(
            f"warning: dbt exited with {result.returncode} on the views of ops, as the lines above "
            f"say, {later}"
        )


def drop_statement(name: str, kind: str) -> str:
    """The statement that drops a relation of LEFTOVERS: without CASCADE, so that a view of a
    deployment over it, and the relation with it, stay."""
    return f"DROP {kind} IF EXISTS {name}"


def drop_leftovers(connection: Any, log: Callable[[str], None]) -> None:
    """Drop each relation of LEFTOVERS that the warehouse holds with its kind, one line for each,
    and a warning for one that does not go, such as one that a view depends on or that a reader
    holds for longer than the lock_timeout of transformer."""
    with connection.cursor() as cursor:
        cursor.execute(KINDS, ([name for name, _ in LEFTOVERS],))
        found = dict(cursor.fetchall())
        for name, kind in LEFTOVERS:
            if found.get(name) != RELKINDS[kind]:
                continue
            try:
                cursor.execute(drop_statement(name, kind))
            except Exception as error:  # any failure of psycopg2 is one line
                log(
                    f"warning: cannot drop the {kind.lower()} {name} of an earlier release, which "
                    f"no model builds or reads, so it stays: {first_line(error)}"
                )
            else:
                log(
                    f"dropped the {kind.lower()} {name} of an earlier release, which no model "
                    "builds or reads"
                )


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
        build_ops(state, run, log)
        try:
            drop_leftovers(connection, log)
        except Exception as error:
            # The views are built: what is left of an earlier release waits for the next start.
            log(
                "warning: cannot read which relations of earlier releases are left: "
                f"{first_line(error)}"
            )
        if state.other_currency:
            # A line to read, not a failure: the stack starts, and the loads of the rates close
            # the days.
            try:
                line = gaps_line(connection, currency)
            except Exception as error:
                line = f"warning: cannot read ops.ops_rates: {first_line(error)}"
            if line:
                log(line)
        log("done")
        return 0
    finally:
        # The lock goes with the connection.
        connection.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
