"""The rows of a fact table of the demo in raw that the next run of the transform reads, as SQL
that does not use the macros of the model: for each account and day, those of the newest whole
load whose window in the journal _agoranomos_loads covers the day, when the core has not accepted
it. The run pins every whole load before its tasks of dbt, and its staging reads the newest load
of each account and day among the pinned and the accepted, when it is a pinned one
(dbt/macros/pin_loads.sql and newest_load.sql, ADR-0038, ADR-0046), which is this one when no load
ended after the pin. Each load of the demo appends its rows under its own _dlt_load_id, and a run
that succeeds accepts the loads that it read and deletes their rows from raw (ADR-0035): so these
are the rows of the loads that no run has read yet, and a test reads them between a load and the
run that reads it. A day whose newest load is accepted is the core's, as a day of the history that
the window of a newer history no longer reaches. The journal and _dlt_loads keep every load.

A test that needs rows in raw for the next run makes a load of its own with load_platform, as a
daily load does, in the container of the tasks and outside Airflow, so that no asset event starts
a run of the transform."""

from __future__ import annotations

from datetime import date, datetime

from agoranomos_pipelines.demo import platforms
from airflow_api import skip, trigger, unpause, wait_for_run
from stack_helpers import active_variant, compose, leaked, secrets, warehouse_output

# The newest whole load of each account and day of a dataset, {dataset} in it: only the loads that
# dlt finished, with their row in _dlt_loads and status 0, only the accounts that loaded, and the
# largest load id, a time in seconds as text, compared as a number.
DAYS = (
    "SELECT DISTINCT ON (j.account_id, w.day) j.account_id, w.day, j._dlt_load_id AS load_id "
    "FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "CROSS JOIN LATERAL (SELECT j.window_from + n AS day "
    "FROM generate_series(0, j.window_to - j.window_from) AS n) AS w "
    "WHERE j.status = 'loaded' "
    "ORDER BY j.account_id, w.day, j._dlt_load_id::numeric DESC"
)


def account_of(fact: platforms.FactTable) -> str | None:
    """The account of a row of a fact table as the journal names it, as SQL, or None for the
    rates, whose one account is the base of the rates: properties/{} of GA4 is the ID after the
    slash."""
    if fact.account_sql is None:
        return None
    if fact.account_text == "{}":
        return fact.account_sql
    return f"split_part({fact.account_sql}, '/', 2)"


def newest(code: str, table: str | None = None) -> str:
    """The rows of a fact table of a platform of the demo, its main table by default, that staging
    reads, as a subquery with the alias newest: the columns of the table, and _account and _day,
    the account and the day of the row. A day whose newest load the core accepted has none."""
    platform = platforms.PLATFORMS[code]
    [fact] = [f for f in platform.facts if f.table == (table or platform.main_table)]
    account = account_of(fact)
    rows = (
        f"SELECT *, {account or 'NULL'} AS _account, ({fact.day_date_sql})::date AS _day "  # noqa: S608 (constants of the demo)
        f"FROM {platform.dataset}.{fact.table}"
    )
    matched = "d.load_id = r._dlt_load_id AND d.day = r._day"
    if account is not None:
        matched += " AND d.account_id = r._account"
    days = DAYS.format(dataset=platform.dataset)
    unread = (
        "NOT EXISTS (SELECT FROM core.loads AS p "
        f"WHERE p.source = '{code}' AND p.load_id = d.load_id AND p.account_id = d.account_id "
        "AND p.status = 'accepted')"
    )
    return (
        f"(SELECT r.* FROM ({rows}) AS r JOIN ({days}) AS d ON {matched} WHERE {unread}) AS newest"  # noqa: S608 (constants of the demo)
    )


# The DAGs of the base package, with agoranomos_pipelines, in the containers of Airflow.
DAGS = "/opt/agoranomos/dags"
# A load of a platform of the demo as of a moment, from a first day to yesterday of each account,
# as a daily load writes it; with skip, it leaves out the day that the history leaves out. dlt
# reads the password of loader from the environment of the container, and the script prints
# nothing.
LOAD = """
import sys
from datetime import date, datetime

from agoranomos_pipelines.demo import platforms, sources

now = datetime.fromisoformat(sys.argv[1])
first = date.fromisoformat(sys.argv[2])
skip = platforms.gap(now) if sys.argv[3] == "skip" else frozenset()
sources.load(sys.argv[4], now, first=lambda account: first, skip=skip)
"""
# The newest whole load of a dataset, and the moment of the newest load as its journal says it.
NEWEST_LOAD = (
    "SELECT j._dlt_load_id FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "ORDER BY j._dlt_load_id::numeric DESC LIMIT 1"
)
LAST_MOMENT = (
    "SELECT to_char(max(j.now) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"') "
    "FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0"
)


def run_in_tasks(
    env: dict[str, str], script: str, *arguments: str, environment: dict[str, str] | None = None
):
    """Run a Python script in the container that runs the tasks, with the package of the DAGs and
    the variables of environment, such as another reporting currency, and return the result; a
    failure shows its last lines, unless they hold a secret."""
    service = "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"
    variables = [f"PYTHONPATH={DAGS}"]
    variables += [f"{name}={value}" for name, value in (environment or {}).items()]
    result = compose(
        "exec",
        "-T",
        *(item for variable in variables for item in ("-e", variable)),
        service,
        "python",
        "-c",
        script,
        *arguments,
        check=False,
    )
    output = f"{result.stdout}\n{result.stderr}"
    shown = [] if leaked(secrets(env), output) else output.strip().splitlines()[-5:]
    assert result.returncode == 0, f"the script exited with {result.returncode}: {shown}"
    return result


def last_moment(env: dict[str, str], code: str) -> datetime:
    """The moment of the newest whole load of a platform of the demo, as of which a load gives the
    same rows for the same days."""
    dataset = platforms.PLATFORMS[code].dataset
    found = warehouse_output(env, LAST_MOMENT.format(dataset=dataset))
    assert found, f"{dataset} has no whole load"
    return datetime.fromisoformat(found)


def load_platform(
    env: dict[str, str], code: str, now: datetime, first: date, skip: bool = True
) -> str:
    """Load a platform of the demo as LOAD says, and return the id of the load."""
    run_in_tasks(env, LOAD, now.isoformat(), first.isoformat(), "skip" if skip else "keep", code)
    dataset = platforms.PLATFORMS[code].dataset
    return warehouse_output(env, NEWEST_LOAD.format(dataset=dataset))


TRANSFORM = "agoranomos_demo_transform"


def transform_keeping_raw(airflow_url: str, token: str, skipped: str = "accept") -> dict:
    """Run the transform with its acceptance skipped, and so its cleanup too: the run pins and
    reads the loads, writes the core, and leaves the loads pinned and their rows in raw, as a run
    whose acceptance failed does, for a test that reads them or runs dbt by hand on them after the
    run. The next run that succeeds reads them again and accepts them. With skipped clean_raw, the
    run accepts its loads and leaves their rows in raw, as a run whose cleanup failed does."""
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    skip(airflow_url, token, run, skipped)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    return run
