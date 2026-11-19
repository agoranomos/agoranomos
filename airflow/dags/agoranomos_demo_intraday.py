"""The current day of the demo of agoranomos, every few hours (ADR-0051).

agoranomos_demo_intraday runs every AGORANOMOS_INTRADAY_HOURS hours of .env, 3 by default, at 27
minutes past the hour in UTC, from midnight, as the short cycle of Google Ads does, and loads with
dlt, as the loader role, into raw_demo_search, the platform of the demo in the shape of Google Ads,
for each of its accounts that a whole load has taken, its current day in its time zone, and the day
before while no whole load covers it: the impressions, clicks and cost of its campaigns so far, as
the share of the day that has passed at the moment of the run in the time zone of the account,
without conversions, as a load of the kind intraday (agoranomos_pipelines/demo/sources.py,
load_intraday). The moment is the run's, so a retry loads as of the same moment, and a moment always
gives the same numbers. The load updates an asset of its own, which starts the transform, and the
transform takes it like any load: the rows of a day that only such a load took are partial in the
core (is_partial_day), and the card Today, as of the load, of the dashboard Advertising shows them,
until agoranomos_demo_daily takes the day the next morning and replaces them whole. With 0, the DAG
has no schedule; a value that is neither 0 nor a divisor of 24 stops the parse of this file with a
line that says so (agoranomos_pipelines/intraday.py), as ./agoranomos stops before up.

The loads of the demo run one at a time in the pool demo_load, this one, the history and the daily
load. The first task, due, skips the run while a run of agoranomos_demo_history is queued or
running, as the history replaces every table of the demo and the transform comes after it, and a
run that waited longer than the period, for the pool or for a slot of the executor, ends without a
load, as the next one comes. A run with no account that a whole load has taken loads nothing. A run
ends within an hour, so that a load never lives longer than the cleanup of the rows of a load that
did not end waits (ADR-0046).

The DAG starts paused: with the demo profile, the one-shot demo-init job unpauses it with
agoranomos_demo_daily, after the history, on the first start that finds it without a run, so a
pause that a person made stays. Unpaused, it makes a run at once, for the last time of its
schedule, which loads the current day. With 0, demo-init leaves it paused, as it would never run
by itself: unpause it before a run by hand.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import intraday
from agoranomos_pipelines.assets import RAW_DEMO_INTRADAY
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

START = datetime(2026, 1, 1, tzinfo=UTC)
# The hours between two loads, from AGORANOMOS_INTRADAY_HOURS, which compose.yaml gives the DAG
# processor and the tasks: a value that is not allowed stops the parse of this file. 0 gives no
# schedule.
HOURS = intraday.period()
CRON = intraday.cron(HOURS)
SCHEDULE = None if CRON is None else CronTriggerTimetable(CRON, timezone="UTC")
# The DAG of the history of the demo, which replaces every table of the demo.
HISTORY = "agoranomos_demo_history"
# The pool of the loads of the demo, which airflow-init sets on every start: one slot, so that two
# loads never write raw_demo_search under one pipeline of dlt at the same time.
LOAD_POOL = "demo_load"


def waited_too_long() -> bool:
    """Whether this run has waited longer than the period, and the next one comes."""
    run_after = get_current_context()["dag_run"].run_after
    if not intraday.waited_too_long(run_after, datetime.now(UTC), HOURS):
        return False
    log.info(
        "The run was to start at %s, more than %d hours ago: the next one loads the current day",
        run_after.isoformat(),
        HOURS,
    )
    return True


@task.short_circuit(task_id="due")
def due() -> bool:
    context = get_current_context()
    active = context["ti"].get_dr_count(dag_id=HISTORY, states=["queued", "running"])
    if active:
        log.info(
            "%d runs of %s are queued or running: the next cycle comes after them", active, HISTORY
        )
        return False
    return not waited_too_long()


@task(task_id="load_demo_search_intraday", outlets=[RAW_DEMO_INTRADAY], pool=LOAD_POOL)
def load_demo_search_intraday() -> None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines.demo import sources

    # Should anything write the password of loader into the log, Airflow shows ***.
    mask_secret(sources.credentials()["password"])
    # A skipped task updates no asset, so the transform does not run for nothing.
    if waited_too_long():
        raise AirflowSkipException("the run waited longer than the period")
    # The moment of the load is the run's, so a retry loads as of the same moment.
    counts = sources.load_intraday(get_current_context()["dag_run"].run_after)
    if counts is None:
        raise AirflowSkipException("no account of demo_search has had a whole load yet")


@dag(
    dag_id="agoranomos_demo_intraday",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run ends within an hour, its two tries of up to 10 minutes and the wait between them
    # included, so that a load never lives longer than that: the cleanup of the rows of a load that
    # did not end is to wait at least as long (ADR-0046).
    dagrun_timeout=timedelta(hours=1),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004); the next cycle
        # comes anyway.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # A load of the current day takes seconds. The timeout ends a try that runs too long in
        # Python; a statement that waits on a lock is bounded by the lock timeout of the loader
        # role.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_intraday():
    due() >> load_demo_search_intraday()


agoranomos_demo_intraday()
