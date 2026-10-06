"""The demo path of Agoranomos: demo ad statistics go into raw_demo, and dbt builds marts from them.

agoranomos_demo_load loads eight weeks of one fictional ad account, up to yesterday (UTC), into
raw_demo with dlt, as the loader role. Its success updates the asset of the raw table. A repeat is
safe: dlt merges on the natural key. Run it again to add the days since the last load, up to eight
weeks back. The DAG has no schedule and starts paused: unpause and trigger it in the Airflow UI.

agoranomos_demo_transform runs the dbt project through Cosmos, as the transformer role. It builds
the staging model stg_demo__ad_stats_daily and then the mart marts.ad_spend_daily, each followed by
its tests, and a failed test of the staging model keeps the mart as it was. The asset of the raw
table starts it, and a repeat is safe: dbt rebuilds the models. It starts paused too, and a load
starts it only while it is unpaused: unpause it in the Airflow UI before you trigger the load.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, dag, task
from airflow.sdk.log import mask_secret
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)

# The asset of the raw table, in the form that Airflow 3 requires for PostgreSQL:
# postgres://<host>:<port>/<database>/<schema>/<table>. The dbt profile uses the same host, port
# and database, so the assets that Cosmos emits for the models have the same form.
RAW_DEMO = Asset("postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily")
DBT_PROJECT = Path("/opt/agoranomos/dbt")
# dbt has a virtual environment of its own in the image (airflow/Dockerfile).
DBT_EXECUTABLE = "/opt/airflow/dbt_venv/bin/dbt"
START = datetime(2026, 1, 1, tzinfo=UTC)


@dag(
    dag_id="agoranomos_demo_load",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004).
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        # A load takes seconds. The timeout ends a try that runs too long in Python, which is then
        # tried again, but it cannot interrupt a statement that waits in PostgreSQL: a statement
        # that waits on a lock is bounded by the lock timeout of the loader role (warehouse.sql).
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_load():
    @task(outlets=[RAW_DEMO])
    def load() -> None:
        # dlt is imported here and not at the top: the DAG processor parses this file often.
        from agoranomos_pipelines import demo_source

        # Should anything write the password of the loader into the log, Airflow shows *** instead.
        # Only the task reads the password: the DAG processor, which parses this file, has none.
        mask_secret(demo_source.credentials()["password"])
        # Data is final up to yesterday.
        demo_source.load(datetime.now(UTC).date() - timedelta(days=1))

    load()


@dag(
    dag_id="agoranomos_demo_transform",
    schedule=[RAW_DEMO],
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={
        # A failed dbt test does not pass on a retry: it found data to fix.
        "retries": 0,
        # A dbt task takes seconds. The timeout ends one that hangs: Cosmos interrupts its dbt
        # process, and dbt then ends its queries in PostgreSQL. A statement that waits on a lock
        # fails sooner, at the lock timeout of the transformer role (warehouse.sql).
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_transform():
    DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT, install_dbt_deps=False),
        profile_config=ProfileConfig(
            profile_name="agoranomos",
            target_name="prod",
            profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
            # The tests of a model run right after it; a failed one stops the models that read it.
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )


agoranomos_demo_load()
agoranomos_demo_transform()
