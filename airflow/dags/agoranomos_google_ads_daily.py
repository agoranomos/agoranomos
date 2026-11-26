"""The daily load of the Google Ads accounts of the deployment (ADR-0032).

agoranomos_google_ads_daily runs at 10:37 UTC, after midnight in every time zone in use from
UTC-10 east, and loads with dlt, as the loader role, the accounts of google_ads that
extensions/model/clients.toml names into raw_google_ads: for each account, the days from the day
after the last that the core accepted, or from 90 days back when that is earlier, to yesterday in
its time zone, the last 90 days on its first load too (agoranomos_pipelines/google_ads.py), with
the totals of each account by day, against which the transform checks its campaigns (G0a of
ADR-0035). It signs in with the key of the service account in GOOGLE_ADS_SERVICE_ACCOUNT_KEY. Each
load updates the asset of its statistics, and a scheduled one keeps the watermark of each account
in the asset state store of that asset, as a cache of the warehouse; the transform runs after it.

A run with the parameters window_from and window_to, and accounts, loads the history (ADR-0038,
"The first load"): the days from window_from to window_to of those accounts, or of every account
when accounts is empty, with their totals, as a load of the kind history, which leaves the
entities and the watermarks as they were. agoranomos_google_ads_history starts such runs, a
quarter at a time, and a person may start one from the form of the run to load a period again.

The DAG starts paused: a deployment unpauses it once its key and its accounts are in place, and
the DAG of the rates of the ECB with it ([Connect Google Ads](docs/how-to/connect-google-ads.md)).
A load without a key, or without an account of google_ads in clients.toml, fails with a line that
says so. An error of the API on one account, or an answer that breaks off, does not stop the
others: the load takes them and records the failed account in its journal (ADR-0035). The task of
the load then succeeds, so that its asset starts the transform for the accounts that it took, and
the last task, check_accounts, fails the run, without a retry, with one line that lists the
accounts and their errors, so that the alert of a failed run goes (outcome.py of the package);
when no account loaded, the task of the load fails with that line and starts nothing.
The next load takes the days that the failed accounts missed. A retry of the task, after any other
error, loads every account again.
A repeat is safe: each load replaces the days that it covers.

Its load runs in the pool google_ads_load of one slot, which airflow-init sets on every start, with
that of agoranomos_google_ads_intraday, which loads the current day of each account every few hours
as a load of the kind intraday (ADR-0051): two loads never write raw_google_ads at the same time,
and a load of this DAG, whole days, replaces the partial days of the short cycle in the core.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import outcome
from agoranomos_pipelines.assets import RAW_GOOGLE_ADS
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight from UTC-10 east, and after the rates of the ECB of the day.
SCHEDULE = CronTriggerTimetable("37 10 * * *", timezone="UTC")
# The parameters of a run of the history: a scheduled run has their defaults, and takes the usual
# window of each account. Airflow checks them against these schemas when a run is triggered, and
# the load checks them again (google_ads.portion_of).
PARAMS = {
    "accounts": Param(
        [],
        type="array",
        items={"type": "string", "pattern": "^[0-9]{10}$"},
        description=(
            "The Google Ads accounts of a run of the history, ten digits each, which "
            "extensions/model/clients.toml names; empty for every account."
        ),
    ),
    "window_from": Param(
        None,
        type=["null", "string"],
        format="date",
        description="The first day of a run of the history, such as 2026-01-01; empty for none.",
    ),
    "window_to": Param(
        None,
        type=["null", "string"],
        format="date",
        description="The last day of a run of the history, such as 2026-03-31; empty for none.",
    ),
}
# The pool of the loads of Google Ads, which airflow-init sets on every start: one slot, so that
# this load and that of the short cycle, agoranomos_google_ads_intraday, never write raw_google_ads
# under one pipeline of dlt at the same time (ADR-0051).
LOAD_POOL = "google_ads_load"


@task(task_id="load_google_ads", outlets=[RAW_GOOGLE_ADS], pool=LOAD_POOL)
def load_google_ads() -> str | None:
    # dlt and google-auth are imported here and not at the top: the DAG processor parses this file
    # often.
    from agoranomos_pipelines import google_ads, loads

    # Should anything write a secret into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(google_ads.key_info()["private_key"])
    context = get_current_context()
    params = context["params"]
    # None for a scheduled run; a run of the history names its accounts and days.
    portion = google_ads.portion_of(params["accounts"], params["window_from"], params["window_to"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    try:
        google_ads.load(
            context["dag_run"].run_after,
            cache=context["asset_state_store"][RAW_GOOGLE_ADS],
            portion=portion,
        )
    except google_ads.AccountsFailed as failed:
        # A retry would repeat the load for nothing, and the next load takes the days that the
        # failed accounts missed. The line lists them (ADR-0004): check_accounts fails the run
        # with it, after the transform has started for the other accounts, if any.
        return outcome.line_of(failed)
    return None


@dag(
    dag_id="agoranomos_google_ads_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    # A run of the history and a scheduled run wait for each other, and the short cycle for both,
    # in the pool of the loads.
    max_active_runs=1,
    is_paused_upon_creation=True,
    params=PARAMS,
    # A run ends within seven hours, its three tries of up to two hours and the waits between them
    # included (ADR-0039), so that a load never lives longer than that: the cleanup of the rows of
    # a load that did not end is to wait at least as long (ADR-0046).
    dagrun_timeout=timedelta(hours=7),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/google_ads.py, and these retry the task, also after a new
        # column of the API waited past the lock timeout of the loader role for a run of the
        # transform that reads the table (ADR-0046). The wait doubles with each retry, from 10 to
        # 20 minutes and then from 20 to 40, which max_retry_delay cuts to 20: Airflow 3.3.2 takes
        # a multiplier here, and True would be 1, no backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=20),
        # A load of many accounts, or the window after a long stop, takes long. The timeout ends a
        # try that runs too long in Python; a statement that waits on a lock is bounded by the
        # lock timeout of the loader role.
        "execution_timeout": timedelta(hours=2),
    },
    tags=["agoranomos", "google_ads"],
    doc_md=__doc__,
)
def agoranomos_google_ads_daily():
    outcome.check_accounts(load_google_ads())


agoranomos_google_ads_daily()
