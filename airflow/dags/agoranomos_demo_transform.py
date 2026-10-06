"""The demo transform of Agoranomos: dbt builds marts from the demo data in raw_demo.

agoranomos_demo_transform runs the dbt project through Cosmos, as the transformer role. It builds
the staging model stg_demo__ad_stats_daily and then the mart marts.ad_spend_daily, each followed by
its tests, and a failed test of the staging model keeps the mart as it was. The asset of the raw
table starts it, which each successful run of agoranomos_demo_load updates, and a repeat is safe:
dbt rebuilds the models. The DAG starts paused, and a load starts it only while it is unpaused:
unpause it in the Airflow UI before you trigger the load, or trigger it there yourself.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from agoranomos_pipelines.assets import RAW_DEMO
from airflow.sdk import dag
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

DBT_PROJECT = Path("/opt/agoranomos/dbt")
# dbt has a virtual environment of its own in the image (airflow/Dockerfile).
DBT_EXECUTABLE = "/opt/airflow/dbt_venv/bin/dbt"
START = datetime(2026, 1, 1, tzinfo=UTC)


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
    # Cosmos runs dbt ls each time that this file is parsed, by the DAG processor and by each task
    # of this DAG when it starts, since its cache is off (airflow/Dockerfile). So the DAG has a file
    # of its own: the task of the load DAG does not wait for dbt ls, and a dbt project on which dbt
    # ls fails cannot break the import of the load DAG.
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


agoranomos_demo_transform()
