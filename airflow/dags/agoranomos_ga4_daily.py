"""The daily load of the GA4 properties of the deployment (ADR-0054).

agoranomos_ga4_daily runs at 14:07 UTC and loads with dlt, as the loader role, the properties of
ga4 that extensions/model/clients.toml names into raw_ga4: for each property, the property, from
the Admin API, and its sessions by day, source, medium, campaign and default channel group, from
one report of the Data API over the days from the day after the last that the core accepted, or
from 14 days back when that is earlier, to yesterday in its time zone, the last 14 days on its
first load too (agoranomos_pipelines/ga4.py), with the four flags of its answers in the journal.
GA4 gives the daily data of a standard property about 12 hours after the day ends in the time zone
of the property ("[GA4] Data freshness"), so at 14:07 UTC yesterday is whole for the properties
from UTC-2 to UTC+9, Europe and most of Asia among them; further west, yesterday may still be the
data of the day in progress, which the next daily load takes again with its window. It signs in
with the key of the service account in GA4_SERVICE_ACCOUNT_KEY. Each load updates the asset of its
report, and the transform after it takes the load through dbt/models/staging/ga4/ into
core.web_sessions_daily and the mart web_sessions_daily.

A run with the parameters window_from and window_to, and accounts, the properties, loads a portion
of the history (ADR-0038, "The first load"): the days from window_from to window_to of those
properties, or of every property when accounts is empty, as a load of the kind history.
agoranomos_ga4_history starts such runs, a quarter at a time, and a person may start one from the
form of the run to load a period again.

The DAG starts paused: a deployment unpauses it once its key and its properties are in place
([Connect GA4](docs/how-to/connect-ga4.md)). A load without a key, or without a property of ga4 in
clients.toml, fails with a line that says so. An error of the API on one property does not stop
the others: the load takes them and records the failed property in its journal (ADR-0035). The
task then fails with one line that lists the properties and their errors, and Airflow retries the
whole load, as after any other error, so that an outage that clears within the retries leaves no
property behind. On the last try, the task of the load succeeds instead when it took a property,
so that its asset starts the transform for the properties that it took, and the last task,
check_accounts, fails the run with the line, so that the alert of a failed run goes (outcome.py of
the package); when no property loaded, the task of the load fails with that line and starts
nothing. A repeat is safe: each load replaces the days that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import outcome
from agoranomos_pipelines.assets import RAW_GA4
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# Twelve hours or more after midnight from UTC-2 to UTC+9, after the loads of the ad platforms in
# the morning in UTC and their runs of the transform.
SCHEDULE = CronTriggerTimetable("7 14 * * *", timezone="UTC")
# The parameters of a run of the history: a scheduled run has their defaults, and takes the usual
# window of each property. The planner of the history, which the ad platforms share, names them
# accounts. Airflow checks them against these schemas when a run is triggered, and the load checks
# them again (ga4.portion_of).
PARAMS = {
    "accounts": Param(
        [],
        type="array",
        items={"type": "string", "pattern": "^[0-9]{1,20}$"},
        description=(
            "The GA4 properties of a run of the history, the digits of each without properties/, "
            "which extensions/model/clients.toml names; empty for every property."
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


@task(task_id="load_ga4", outlets=[RAW_GA4])
def load_ga4() -> str | None:
    # dlt and google-auth are imported here and not at the top: the DAG processor parses this file
    # often.
    from agoranomos_pipelines import ga4, loads

    # Should anything write a secret into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(ga4.key_info()["private_key"])
    context = get_current_context()
    params = context["params"]
    # None for a scheduled run; a run of the history names its properties and days.
    portion = ga4.portion_of(params["accounts"], params["window_from"], params["window_to"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    try:
        ga4.load(context["dag_run"].run_after, portion=portion)
    except ga4.PropertiesFailed as failed:
        # A try that Airflow will retry raises the failure again, and the retry loads every
        # one of the properties again. The last try returns the line, which lists the failed
        # properties (ADR-0004), when the load took others: check_accounts fails the run with
        # it, after the transform has started for them. The next load takes the days that the
        # failed properties missed.
        return outcome.line_of(failed, context["ti"])
    return None


@dag(
    dag_id="agoranomos_ga4_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    # A run of the history and a scheduled run wait for each other.
    max_active_runs=1,
    is_paused_upon_creation=True,
    params=PARAMS,
    # A run ends within seven hours, its three tries of up to two hours and the waits between them
    # included, as for Google Ads and Meta: the cleanup of the rows of a load that did not end
    # waits longer (ADR-0046).
    dagrun_timeout=timedelta(hours=7),
    default_args={
        # Retries are explicit (ADR-0004): those of a request, of a server error and of the quota
        # are in agoranomos_pipelines/ga4.py, and these retry the task, as after a lock timeout of
        # the loader role (ADR-0046). The wait doubles with each retry, from 10 to 20 minutes,
        # which max_retry_delay keeps: Airflow 3.3.2 takes a multiplier here, and True would be 1,
        # no backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=20),
        # A wait for the hourly quota of a property takes up to 70 minutes of a load.
        "execution_timeout": timedelta(hours=2),
    },
    tags=["agoranomos", "ga4"],
    doc_md=__doc__,
)
def agoranomos_ga4_daily():
    outcome.check_accounts(load_ga4())


agoranomos_ga4_daily()
