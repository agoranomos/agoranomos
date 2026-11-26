"""The current day of the Meta ad accounts of the deployment, every few hours (ADR-0051).

agoranomos_meta_ads_intraday runs every AGORANOMOS_INTRADAY_HOURS hours of .env, 3 by default, at
27 minutes past the hour in UTC, from midnight, at the same moments as the short cycle of Google
Ads, so that the loads of one cycle start the transform once or twice and not once each, and loads
with dlt, as the loader role, into raw_meta_ads, for each account of meta_ads that
extensions/model/clients.toml names and that a whole load has taken, its current day in its time
zone, and the day before while no whole load covers it: the impressions, link clicks and spend of
its campaigns, without actions, so without conversions, as a load of the kind intraday
(agoranomos_pipelines/meta_ads.py, load_intraday). That is two calls to the Graph API for each
account, the account and the insights of its campaigns, each a point of the score of the account.
The load updates an asset of its own, and the transform takes it like any load: the rows of a day
that only such a load took are partial in the core (is_partial_day), until a load of
agoranomos_meta_ads_daily takes the day and replaces them whole. An account that no whole load has
taken yet waits for its first daily load, and a run with no such account loads nothing. With 0,
the DAG has no schedule; a value that is neither 0 nor a divisor of 24 stops the parse of this file
with a line that says so (agoranomos_pipelines/intraday.py), as ./agoranomos stops before up.

The loads of Meta run one at a time in the pool meta_ads_load, which airflow-init sets with one
slot on every start: the daily load, its runs of the history and this one, which also keeps two
refreshes of a token that expires from running at once. The first task, due, skips the run while a
run of agoranomos_meta_ads_daily is queued or running, as its load can hold the pool for hours, and
a run that waited longer than the period, for the pool or for a slot of the executor, ends without
a load, as the next one comes. A run ends within an hour, so that a load never lives longer than
the cleanup of the rows of a load that did not end waits (ADR-0046).

The DAG starts paused: a deployment unpauses it with agoranomos_meta_ads_daily, once its token and
its accounts are in place ([Connect Meta](docs/how-to/connect-meta.md)). An error of the API on one
account does not stop the others: the load takes them, and the task fails at the end, without a
retry, with one line that lists the accounts and their errors; the failed account leaves no row in
the journal, and the next cycle takes it again.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import intraday
from agoranomos_pipelines.assets import RAW_META_ADS_INTRADAY
from airflow.sdk import CronTriggerTimetable, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

START = datetime(2026, 1, 1, tzinfo=UTC)
# The hours between two loads, from AGORANOMOS_INTRADAY_HOURS, which compose.yaml gives the DAG
# processor and the tasks: a value that is not allowed stops the parse of this file. 0 gives no
# schedule.
HOURS = intraday.period()
CRON = intraday.cron(HOURS)
SCHEDULE = None if CRON is None else CronTriggerTimetable(CRON, timezone="UTC")
# The DAG of the daily load of Meta and of its runs of the history.
DAILY = "agoranomos_meta_ads_daily"
# The pool of the loads of Meta, which airflow-init sets on every start: one slot, so that two
# loads never write raw_meta_ads under one pipeline of dlt, or refresh the token, at the same time.
LOAD_POOL = "meta_ads_load"


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
    # A run of the daily load, scheduled or of the history, holds the pool of the loads for as
    # long as it takes: the next cycle comes after it.
    active = context["ti"].get_dr_count(dag_id=DAILY, states=["queued", "running"])
    if active:
        log.info(
            "%d runs of %s are queued or running: the next cycle comes after them", active, DAILY
        )
        return False
    return not waited_too_long()


@task(task_id="load_meta_ads_intraday", outlets=[RAW_META_ADS_INTRADAY], pool=LOAD_POOL)
def load_meta_ads_intraday() -> None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, meta_ads

    # Should anything write a secret into the log, Airflow shows ***. A token that a refresh gives,
    # and the proof of the App Secret, the load masks with the same function.
    mask_secret(loads.credentials()["password"])
    for name in (meta_ads.TOKEN, meta_ads.APP_SECRET):
        if os.environ.get(name, "").strip():
            mask_secret(os.environ[name].strip())
    # A skipped task updates no asset, so the transform does not run for nothing.
    if waited_too_long():
        raise AirflowSkipException("the run waited longer than the period")
    try:
        # The moment of the read, which gives the current day of each account.
        counts = meta_ads.load_intraday(datetime.now(UTC), variables=Variable, mask=mask_secret)
    except meta_ads.AccountsFailed as failed:
        # The load took the other accounts, so a retry would repeat it for nothing, and the next
        # cycle takes the failed accounts again. The line lists them (ADR-0004).
        raise AirflowFailException(str(failed)) from None
    if counts is None:
        raise AirflowSkipException("no account of meta_ads has had a whole load yet")


@dag(
    dag_id="agoranomos_meta_ads_intraday",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run ends within an hour, its two tries of up to 20 minutes and the wait between them
    # included, so that a load never lives longer than that: the cleanup of the rows of a load that
    # did not end is to wait at least as long (ADR-0046).
    dagrun_timeout=timedelta(hours=1),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # and of throttling are in agoranomos_pipelines/meta_ads.py, and these retry the task once,
        # as after a failed refresh of the token or a lock timeout of the loader role (ADR-0046);
        # the next cycle comes anyway.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # Two calls for each account take seconds, a report run up to meta_ads.INTRADAY_ENDS. The
        # timeout ends a try that runs too long in Python, as in the waits of throttling; a
        # statement that waits on a lock is bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["agoranomos", "meta_ads"],
    doc_md=__doc__,
)
def agoranomos_meta_ads_intraday():
    due() >> load_meta_ads_intraday()


agoranomos_meta_ads_intraday()
