"""The daily load of the TikTok advertisers of the deployment (ADR-0053).

agoranomos_tiktok_ads_daily runs at 12:17 UTC, after the data fix that TikTok runs every day at
12:00 UTC, "so you are recommended to pull data after 12:00 (UTC+0)" (Data latency for reports,
v1.3, as of November 2026), after midnight in every time zone, and after the loads of Meta at 10:07
and of Google Ads at 10:37, so that the runs of the transform after them have ended. It loads with
dlt, as the loader role, the advertisers of tiktok_ads that extensions/model/clients.toml names into
raw_tiktok_ads: for each advertiser, the advertiser, its campaigns, ad groups and ads, and the days
from the day after the last that the core accepted, or from 28 days back when that is earlier, to
yesterday in its time zone, the last 28 days on its first load too
(agoranomos_pipelines/tiktok_ads.py), with the totals of each advertiser by day for G0a of ADR-0035.
It signs in with the long-term token in TIKTOK_ADS_ACCESS_TOKEN, which ./agoranomos tiktok connect
writes. Each load updates the asset of its reports, and a scheduled one keeps the watermark of each
advertiser in the asset state store of that asset, as a cache of the warehouse. No staging model
reads raw_tiktok_ads yet: the core and the marts of TikTok are C5d of #225.

A run with the parameters window_from and window_to, and accounts, loads a portion of the history
(ADR-0038, "The first load"): the days from window_from to window_to of those advertisers, or of
every advertiser when accounts is empty, as a load of the kind history, which leaves the watermarks
as they were. agoranomos_tiktok_ads_history starts such runs, a quarter at a time, and a person may
start one from the form of the run to load a period again.

The DAG starts paused: a deployment unpauses it once its token and its advertisers are in place
([Connect TikTok](docs/how-to/connect-tiktok.md)). A load without a token, or without an advertiser
of tiktok_ads in clients.toml, fails with a line that says so. An error of the API on one
advertiser does not stop the others: the load takes them, records the failed advertiser in its
journal, and the task fails at the end, without a retry, with one line that lists the advertisers
and their errors (ADR-0035). A repeat is safe: each load replaces the days that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_TIKTOK_ADS
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After the data fix of TikTok at 12:00 UTC, and after the loads of Meta and of Google Ads and the
# runs of the transform that follow them.
SCHEDULE = CronTriggerTimetable("17 12 * * *", timezone="UTC")
# The parameters of a run of the history: a scheduled run has their defaults, and takes the usual
# window of each advertiser. Airflow checks them against these schemas when a run is triggered,
# and the load checks them again (tiktok_ads.portion_of).
PARAMS = {
    "accounts": Param(
        [],
        type="array",
        items={"type": "string", "pattern": "^[0-9]{1,20}$"},
        description=(
            "The TikTok advertisers of a run of the history, the digits of each advertiser ID, "
            "which extensions/model/clients.toml names; empty for every advertiser."
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
        description="The last day of a run of the history, such as 2026-01-30; empty for none.",
    ),
}


@task(task_id="load_tiktok_ads", outlets=[RAW_TIKTOK_ADS])
def load_tiktok_ads() -> None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, tiktok_ads

    # Should anything write a secret into the log, Airflow shows ***. The load masks the token with
    # the same function.
    mask_secret(loads.credentials()["password"])
    context = get_current_context()
    params = context["params"]
    # None for a scheduled run; a run of the history names its advertisers and days.
    portion = tiktok_ads.portion_of(params["accounts"], params["window_from"], params["window_to"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    try:
        tiktok_ads.load(
            context["dag_run"].run_after,
            cache=context["asset_state_store"][RAW_TIKTOK_ADS],
            portion=portion,
            mask=mask_secret,
        )
    except tiktok_ads.AdvertisersFailed as failed:
        # The load took the other advertisers, so a retry would repeat it for nothing, and the next
        # load takes the days that the failed advertisers missed. The line lists them (ADR-0004).
        raise AirflowFailException(str(failed)) from None


@dag(
    dag_id="agoranomos_tiktok_ads_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    # A run of the history and a scheduled run wait for each other.
    max_active_runs=1,
    is_paused_upon_creation=True,
    params=PARAMS,
    # A run ends within seven hours, its three tries of up to two hours and the waits between them
    # included, as for Google Ads and Meta: the cleanup of the rows of a load that did not end waits
    # longer (ADR-0046).
    dagrun_timeout=timedelta(hours=7),
    default_args={
        # Retries are explicit (ADR-0004): those of a request and of throttling are in
        # agoranomos_pipelines/tiktok_ads.py, and these retry the task, as after a lock timeout of
        # the loader role (ADR-0046). The wait doubles with each retry, from 10 to 20 minutes, which
        # max_retry_delay keeps: Airflow 3.3.2 takes a multiplier here, and True would be 1, no
        # backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=20),
        # The pace of the calls and the waits of throttling, of five minutes each, take long.
        "execution_timeout": timedelta(hours=2),
    },
    tags=["agoranomos", "tiktok_ads"],
    doc_md=__doc__,
)
def agoranomos_tiktok_ads_daily():
    load_tiktok_ads()


agoranomos_tiktok_ads_daily()
