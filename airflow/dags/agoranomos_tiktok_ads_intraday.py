"""The current day of the TikTok advertisers of the deployment, every few hours (ADR-0051).

agoranomos_tiktok_ads_intraday runs every AGORANOMOS_INTRADAY_HOURS hours of .env, 3 by default, at
27 minutes past the hour in UTC, at hours that keep at least an hour from the start of the daily
backup, BACKUP_TIME in BACKUP_TZ (ADR-0020), from 01:27 with the defaults, at the same moments as
the short cycles of Google Ads and of Meta, so that the loads of one cycle start the transform once
or twice and not once each, and loads with dlt, as the loader role, into raw_tiktok_ads, for each
advertiser of tiktok_ads that extensions/model/clients.toml names and that a whole load has taken,
its current day in its time zone, and the day before while no whole load covers it: the impressions,
clicks and spend of its campaigns, without conversions, as a load of the kind intraday
(agoranomos_pipelines/tiktok_ads.py, load_intraday). That is two calls to the TikTok API for each
advertiser, the advertiser and the report of its campaigns. The load updates an asset of its own,
and the transform takes it like any load: the rows of a day that only such a load took are partial
in the core (is_partial_day), until a load of agoranomos_tiktok_ads_daily takes the day and replaces
them whole. An advertiser that no whole load has taken yet waits for its first daily load, and a run
with no such advertiser loads nothing. With 0, the DAG has no schedule; a value that is neither 0
nor a divisor of 24 stops the parse of this file with a line that says so
(agoranomos_pipelines/intraday.py), as ./agoranomos stops before up.

The loads of TikTok run one at a time in the pool tiktok_ads_load, which airflow-init sets with one
slot on every start: the daily load, its runs of the history and this one. The first task, due,
skips the run while a run of agoranomos_tiktok_ads_daily is queued or running, as its load can
hold the pool for hours, and a run that waited longer than the period, for the pool or for a slot
of the executor, ends without a load, as the next one comes. A run ends within an hour, so that a
load never lives longer than the cleanup of the rows of a load that did not end waits (ADR-0046).

The DAG starts paused: a deployment unpauses it with agoranomos_tiktok_ads_daily, once its token
and its advertisers are in place ([Connect TikTok](docs/how-to/connect-tiktok.md)). An error of
the API on one advertiser does not stop the others: the load takes them, and its task succeeds, so
that its asset starts the transform for them, while the last task, check_accounts, fails the run,
without a retry, with one line that lists the advertisers and their errors (outcome.py of the
package); when no advertiser loaded, the task of the load fails with that line. The failed
advertiser leaves no row in the journal, and the next cycle takes it again.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import intraday, outcome
from agoranomos_pipelines.assets import RAW_TIKTOK_ADS_INTRADAY
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

START = datetime(2026, 1, 1, tzinfo=UTC)
# The hours between two loads, from AGORANOMOS_INTRADAY_HOURS, which compose.yaml gives the DAG
# processor and the tasks, with BACKUP_TIME and BACKUP_TZ, which the hours of the runs keep away
# from, as those of the short cycle of Google Ads: a value that is not allowed stops the parse of
# this file. 0 gives no schedule.
HOURS = intraday.period()
CRON = intraday.cron(HOURS)
SCHEDULE = None if CRON is None else CronTriggerTimetable(CRON, timezone="UTC")
# The DAG of the daily load of TikTok and of its runs of the history.
DAILY = "agoranomos_tiktok_ads_daily"
# The pool of the loads of TikTok, which airflow-init sets on every start: one slot, so that two
# loads never write raw_tiktok_ads under one pipeline of dlt at the same time.
LOAD_POOL = "tiktok_ads_load"


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


@task(task_id="load_tiktok_ads_intraday", outlets=[RAW_TIKTOK_ADS_INTRADAY], pool=LOAD_POOL)
def load_tiktok_ads_intraday() -> str | None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, tiktok_ads

    # Should anything write a secret into the log, Airflow shows ***. The load masks the token with
    # the same function.
    mask_secret(loads.credentials()["password"])
    # A skipped task updates no asset, so the transform does not run for nothing.
    if waited_too_long():
        raise AirflowSkipException("the run waited longer than the period")
    try:
        # The moment of the read, which gives the current day of each advertiser.
        counts = tiktok_ads.load_intraday(datetime.now(UTC), mask=mask_secret)
    except tiktok_ads.AdvertisersFailed as failed:
        # A retry would repeat the load for nothing, and the next cycle takes the failed advertisers
        # again. The line lists them (ADR-0004): check_accounts fails the run with it, after the
        # transform has started for the other advertisers, if any.
        return outcome.line_of(failed)
    if counts is None:
        raise AirflowSkipException("no advertiser of tiktok_ads has had a whole load yet")
    return None


@dag(
    dag_id="agoranomos_tiktok_ads_intraday",
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
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a call and
        # of throttling are in agoranomos_pipelines/tiktok_ads.py, and these retry the task once,
        # as after a lock timeout of the loader role (ADR-0046); the next cycle comes anyway.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # Two calls for each advertiser take seconds; a wait of throttling takes five minutes. The
        # timeout ends a try that runs too long in Python; a statement that waits on a lock is
        # bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["agoranomos", "tiktok_ads"],
    doc_md=__doc__,
)
def agoranomos_tiktok_ads_intraday():
    # A run that due skips skips the two tasks after it, check_accounts too, whatever its trigger
    # rule, and ends green; a load that skips leaves check_accounts to run and pass.
    loaded = load_tiktok_ads_intraday()
    due() >> loaded
    outcome.check_accounts(loaded)


agoranomos_tiktok_ads_intraday()
