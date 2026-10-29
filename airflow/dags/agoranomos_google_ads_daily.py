"""The daily load of the Google Ads accounts of the deployment (ADR-0032).

agoranomos_google_ads_daily runs at 10:37 UTC, after midnight in every time zone in use from
UTC-10 east, and loads with dlt, as the loader role, the accounts of google_ads that
extensions/model/clients.toml names into raw_google_ads: for each account, the days from the day
after the last that a load took, or from 90 days back when that is earlier, to yesterday in its
time zone, and the last 36 months on its first load (agoranomos_pipelines/google_ads.py). It signs
in with the key of the service account in GOOGLE_ADS_SERVICE_ACCOUNT_KEY. Each load updates the
asset of its statistics, and the transform runs after it.

The DAG starts paused: a deployment unpauses it once its key and its accounts are in place, and
the DAG of the rates of the ECB with it ([Connect Google Ads](docs/how-to/connect-google-ads.md)).
A load without a key, or without an account of google_ads in clients.toml, fails with a line that
says so. A repeat is safe: each load replaces the days that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_GOOGLE_ADS
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight from UTC-10 east, and after the rates of the ECB of the day.
SCHEDULE = CronTriggerTimetable("37 10 * * *", timezone="UTC")


@task(task_id="load_google_ads", outlets=[RAW_GOOGLE_ADS])
def load_google_ads() -> None:
    # dlt and google-auth are imported here and not at the top: the DAG processor parses this file
    # often.
    from agoranomos_pipelines import google_ads, loads

    # Should anything write a secret into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(google_ads.key_info()["private_key"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    google_ads.load(get_current_context()["dag_run"].run_after)


@dag(
    dag_id="agoranomos_google_ads_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/google_ads.py, and these retry the task.
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        # The first load of many accounts takes their history. The timeout ends a try that runs
        # too long in Python; a statement that waits on a lock is bounded by the lock timeout of
        # the loader role.
        "execution_timeout": timedelta(hours=2),
    },
    tags=["agoranomos", "google_ads"],
    doc_md=__doc__,
)
def agoranomos_google_ads_daily():
    load_google_ads()


agoranomos_google_ads_daily()
