"""The daily load of the Meta ad accounts of the deployment (ADR-0050).

agoranomos_meta_ads_daily runs at 10:07 UTC, after midnight in every time zone in use from UTC-10
east, half an hour before the load of Google Ads, so that the run of the transform that follows it
ends before that load starts, and loads with dlt, as the loader role, the accounts of meta_ads that
extensions/model/clients.toml names into raw_meta_ads: for each account, the account, its
campaigns, ad sets and ads, and the days from the day after the last that the core accepted, or
from 28 days back when that is earlier, to yesterday in its time zone, the last 28 days on its
first load too (agoranomos_pipelines/meta_ads.py), with the totals of each account by day for G0a
of ADR-0035. It signs in with the token of a system user in META_ADS_ACCESS_TOKEN, and, for a token
that expires, refreshes it with META_ADS_APP_ID and META_ADS_APP_SECRET and keeps the newest one in
an Airflow Variable. Each load updates the asset of its insights, and a scheduled one keeps the
watermark of each account in the asset state store of that asset, as a cache of the warehouse. The
transform after a load takes it through dbt/models/staging/meta_ads/ into the core and the marts.

A run with the parameters window_from and window_to, and accounts, loads a portion of the history
(ADR-0038, "The first load"): the days from window_from to window_to of those accounts, or of
every account when accounts is empty, through report runs, as a load of the kind history, which
leaves the watermarks as they were, and leaves to the next portion the accounts that it has not
reached within an hour, so that a portion ends within the timeout of its task.
agoranomos_meta_ads_history starts such runs, a quarter at a time, and a person may start one from
the form of the run to load a period again.

The DAG starts paused: a deployment unpauses it once its token and its accounts are in place
([Connect Meta](docs/how-to/connect-meta.md)). A load without a token, or without an account of
meta_ads in clients.toml, fails with a line that says so. An error of the API on one account does
not stop the others: the load takes them and records the failed account in its journal
(ADR-0035). The task then fails with one line that lists the accounts and their errors, and
Airflow retries the whole load, as after any other error, so that an outage that clears within the
retries leaves no account behind. On the last try, the task of the load succeeds instead when it
took an account, so that its asset starts the transform for the accounts that it took, and the
last task, check_accounts, fails the run with the line, so that the alert of a failed run goes
(outcome.py of the package); when no account loaded, the task of the load fails with that line and
starts nothing. A repeat is safe: each load replaces the days that it covers.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import outcome
from agoranomos_pipelines.assets import RAW_META_ADS
from airflow.sdk import CronTriggerTimetable, Param, Variable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight from UTC-10 east, after the rates of the ECB of the day, and half an hour before
# the load of Google Ads at 10:37, as both start the one transform.
SCHEDULE = CronTriggerTimetable("7 10 * * *", timezone="UTC")
# The parameters of a run of the history: a scheduled run has their defaults, and takes the usual
# window of each account. Airflow checks them against these schemas when a run is triggered, and
# the load checks them again (meta_ads.portion_of).
PARAMS = {
    "accounts": Param(
        [],
        type="array",
        items={"type": "string", "pattern": "^[0-9]{1,20}$"},
        description=(
            "The Meta ad accounts of a run of the history, the digits of each without act_, which "
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


@task(task_id="load_meta_ads", outlets=[RAW_META_ADS])
def load_meta_ads() -> str | None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, meta_ads

    # Should anything write a secret into the log, Airflow shows ***. A token that a refresh gives,
    # and the proof of the App Secret, the load masks with the same function.
    mask_secret(loads.credentials()["password"])
    for name in (meta_ads.TOKEN, meta_ads.APP_SECRET):
        if os.environ.get(name, "").strip():
            mask_secret(os.environ[name].strip())
    context = get_current_context()
    params = context["params"]
    # None for a scheduled run; a run of the history names its accounts and days.
    portion = meta_ads.portion_of(params["accounts"], params["window_from"], params["window_to"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    try:
        meta_ads.load(
            context["dag_run"].run_after,
            cache=context["asset_state_store"][RAW_META_ADS],
            portion=portion,
            variables=Variable,
            mask=mask_secret,
        )
    except meta_ads.AccountsFailed as failed:
        # A try that Airflow will retry raises the failure again, and the retry loads every
        # one of the accounts again. The last try returns the line, which lists the failed
        # accounts (ADR-0004), when the load took others: check_accounts fails the run with
        # it, after the transform has started for them. The next load takes the days that the
        # failed accounts missed.
        return outcome.line_of(failed, context["ti"])
    return None


@dag(
    dag_id="agoranomos_meta_ads_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    # A run of the history and a scheduled run wait for each other.
    max_active_runs=1,
    is_paused_upon_creation=True,
    params=PARAMS,
    # A run ends within seven hours, its three tries of up to two hours and the waits between them
    # included, as for Google Ads: the cleanup of the rows of a load that did not end waits longer
    # (ADR-0046).
    dagrun_timeout=timedelta(hours=7),
    default_args={
        # Retries are explicit (ADR-0004): those of a request and of throttling are in
        # agoranomos_pipelines/meta_ads.py, and these retry the task, as after a failed refresh of
        # the token or a lock timeout of the loader role (ADR-0046). The wait doubles with each
        # retry, from 10 to 20 minutes, which max_retry_delay keeps: Airflow 3.3.2 takes a
        # multiplier here, and True would be 1, no backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=20),
        # The waits of throttling and the report runs, up to an hour each, take long.
        "execution_timeout": timedelta(hours=2),
    },
    tags=["agoranomos", "meta_ads"],
    doc_md=__doc__,
)
def agoranomos_meta_ads_daily():
    outcome.check_accounts(load_meta_ads())


agoranomos_meta_ads_daily()
