"""The current day of the Google Ads accounts of the deployment, every few hours (ADR-0051).

agoranomos_google_ads_intraday runs every AGORANOMOS_INTRADAY_HOURS hours of .env, 3 by default, at
27 minutes past the hour in UTC, at hours that keep at least an hour from the start of the daily
backup, BACKUP_TIME in BACKUP_TZ (ADR-0020), from 01:27 with the defaults, and loads with dlt, as
the loader role, into raw_google_ads, for each account of google_ads that
extensions/model/clients.toml names and that a whole load has taken, its current day in its time
zone, and the day before while no whole load covers it: the impressions, clicks and cost of its
campaigns, without conversions, as a load of the kind intraday (agoranomos_pipelines/google_ads.py,
load_intraday). That is two requests to the Google Ads API for each account, its customer and the
statistics of its campaigns, against twelve for a daily load. The load updates an asset of its own,
and the transform takes it like any load: the rows of a day that only such a load took are partial
in the core (is_partial_day), until a load of agoranomos_google_ads_daily takes the day and replaces
them whole. An account that no whole load has taken yet waits for its first daily load, and a run
with no such account loads nothing. With 0, the DAG has no schedule; a value that is neither 0 nor a
divisor of 24 stops the parse of this file with a line that says so
(agoranomos_pipelines/intraday.py), as ./agoranomos stops before up.

The loads of Google Ads run one at a time in the pool google_ads_load, which airflow-init sets with
one slot on every start: the daily load, its runs of the history and this one. The first task,
due, skips the run while a run of agoranomos_google_ads_daily is queued or running, as its load can
hold the pool for hours, and a run that waited longer than the period, for the pool or for a slot
of the executor, ends without a load, as the next one comes. A run ends within an hour, so that a
load never lives longer than the cleanup of the rows of a load that did not end waits (ADR-0046).

The DAG starts paused: a deployment unpauses it with agoranomos_google_ads_daily, once its key and
its accounts are in place ([Connect Google Ads](docs/how-to/connect-google-ads.md)). An error of
the API on one account does not stop the others: the load takes them, and the task fails at the
end, without a retry, with one line that lists the accounts and their errors; the next cycle takes
them again.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import intraday
from agoranomos_pipelines.assets import RAW_GOOGLE_ADS_INTRADAY
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

START = datetime(2026, 1, 1, tzinfo=UTC)
# The hours between two loads, from AGORANOMOS_INTRADAY_HOURS, which compose.yaml gives the DAG
# processor and the tasks, with BACKUP_TIME and BACKUP_TZ, which the hours of the runs keep away
# from: a value that is not allowed stops the parse of this file. 0 gives no schedule.
HOURS = intraday.period()
CRON = intraday.cron(HOURS)
SCHEDULE = None if CRON is None else CronTriggerTimetable(CRON, timezone="UTC")
# The DAG of the daily load of Google Ads and of its runs of the history.
DAILY = "agoranomos_google_ads_daily"
# The pool of the loads of Google Ads, which airflow-init sets on every start: one slot, so that two
# loads never write raw_google_ads under one pipeline of dlt at the same time.
LOAD_POOL = "google_ads_load"


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


@task(task_id="load_google_ads_intraday", outlets=[RAW_GOOGLE_ADS_INTRADAY], pool=LOAD_POOL)
def load_google_ads_intraday() -> None:
    # dlt and google-auth are imported here and not at the top: the DAG processor parses this file
    # often.
    from agoranomos_pipelines import google_ads, loads

    # Should anything write a secret into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(google_ads.key_info()["private_key"])
    # A skipped task updates no asset, so the transform does not run for nothing.
    if waited_too_long():
        raise AirflowSkipException("the run waited longer than the period")
    try:
        # The moment of the read, which gives the current day of each account.
        counts = google_ads.load_intraday(datetime.now(UTC))
    except google_ads.AccountsFailed as failed:
        # The load took the other accounts, so a retry would repeat it for nothing, and the next
        # cycle takes the failed accounts again. The line lists them (ADR-0004).
        raise AirflowFailException(str(failed)) from None
    if counts is None:
        raise AirflowSkipException("no account of google_ads has had a whole load yet")


@dag(
    dag_id="agoranomos_google_ads_intraday",
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
        # are in agoranomos_pipelines/google_ads.py, and these retry the task once, also after a
        # new column of the API waited past the lock timeout of the loader role for a run of the
        # transform that reads the table (ADR-0046); the next cycle comes anyway.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # Two requests for each account take minutes. The timeout ends a try that runs too long in
        # Python; a statement that waits on a lock is bounded by the lock timeout of the loader
        # role.
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["agoranomos", "google_ads"],
    doc_md=__doc__,
)
def agoranomos_google_ads_intraday():
    due() >> load_google_ads_intraday()


agoranomos_google_ads_intraday()
