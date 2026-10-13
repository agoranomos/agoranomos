"""The history of the demo of Agoranomos: 18 months of a fictional business on four fictional
platforms and a source of rates.

agoranomos_demo_history loads, as the loader role, the demo data of a business with two clients:
their ad accounts on demo_search, demo_social and demo_video, their properties on demo_web, and the
rates of demo_fx, in the shapes of the Google Ads API v25, the Meta Marketing API v26.0, the TikTok
API for Business v1.3 and the GA4 Data API v1beta, up to yesterday in the time zone of each
account, into raw_demo_<platform>. The first run of each platform also replays the names that its
campaigns had. A repeat is safe: each load replaces the days of each account that it covers. The
five loads run one after another, so that one load runs at a time, and each updates the asset of
its main table. The DAG has no schedule and starts paused: with the demo profile, the one-shot
demo-init job unpauses it and triggers it once, on the first start.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_DEMO_PLATFORMS
from agoranomos_pipelines.demo.platforms import LOAD_ORDER
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)


def load_task(code: str):
    @task(task_id=f"load_{code}", outlets=[RAW_DEMO_PLATFORMS[code]])
    def load() -> None:
        # dlt is imported here and not at the top: the DAG processor parses this file often.
        from agoranomos_pipelines.demo import sources

        # Should anything write the password of the loader into the log, Airflow shows ***.
        mask_secret(sources.credentials()["password"])
        # The moment of the load is the run's, so a retry loads as of the same moment.
        sources.load_history(code, get_current_context()["dag_run"].run_after)

    return load()


@dag(
    dag_id="agoranomos_demo_history",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004).
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        # A load takes seconds to a minute. The timeout ends a try that runs too long in Python;
        # a statement that waits on a lock is bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_history():
    previous = None
    for code in LOAD_ORDER:
        current = load_task(code)
        if previous is not None:
            previous >> current
        previous = current


agoranomos_demo_history()
