"""The demo path of Agoranomos: demo ad statistics go into raw_demo.

agoranomos_demo_load loads eight weeks of one fictional ad account, up to yesterday (UTC), into
raw_demo with dlt, as the loader role. Its success updates the asset of the raw table. A repeat is
safe: dlt merges on the natural key. Run it again to add the days since the last load.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, task

# The asset of the raw table, in the form that Airflow 3 requires for PostgreSQL:
# postgres://<host>:<port>/<database>/<schema>/<table>.
RAW_DEMO = Asset("postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily")
START = datetime(2026, 1, 1, tzinfo=UTC)


@dag(
    dag_id="agoranomos_demo_load",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    # Retries are explicit, as the contract of the sources asks (ADR-0004).
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_load():
    @task(outlets=[RAW_DEMO])
    def load() -> None:
        # dlt is imported here and not at the top: the DAG processor parses this file often.
        from agoranomos_pipelines import demo_source

        # Data is final up to yesterday.
        demo_source.load(datetime.now(UTC).date() - timedelta(days=1))

    load()


agoranomos_demo_load()
