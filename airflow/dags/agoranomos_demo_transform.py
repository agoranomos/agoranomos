"""The demo transform of Agoranomos: dbt builds marts from the demo data in raw_demo.

agoranomos_demo_transform runs the dbt project through Cosmos, as the transformer role. It builds
the staging model stg_demo__ad_stats_daily and then the mart marts.ad_spend_daily, each followed by
its tests, and a failed test of the staging model keeps the mart as it was. Each successful run of
agoranomos_demo_load updates the asset of the raw table, and that starts this DAG. A repeat is
safe: dbt rebuilds the models. The DAG starts paused, and a load starts it only while it is
unpaused. With the demo profile, the one-shot demo-init job unpauses both demo DAGs on the first
start and triggers the load; without it, unpause this DAG in the Airflow UI before you trigger the
load, or trigger it there yourself.

Cosmos renders its tasks from the manifest that metadata-export writes on every start (ADR-0027),
so a change of the dbt project reaches the DAG at the next ./agoranomos up; its tasks of dbt run at
most two at a time, in the pool dbt.
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
    TestIndirectSelection,
)

DBT_PROJECT = Path("/opt/agoranomos/dbt")
# dbt has a virtual environment of its own in the image (airflow/Dockerfile).
DBT_EXECUTABLE = "/opt/airflow/dbt_venv/bin/dbt"
# The manifest of the dbt project, which metadata-export writes on every start into a volume that
# the Airflow services mount read-only (ADR-0027): Cosmos renders the tasks from it, without dbt ls.
DBT_MANIFEST = Path("/opt/agoranomos/manifest/manifest.json")
# The pool of the tasks of dbt, which airflow-init sets on every start: at most two processes of dbt
# share the memory of the container that runs the tasks (ADR-0027).
DBT_POOL = "dbt"
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
    # Cosmos renders the tasks from the manifest of the project, without dbt ls; the DAG still has a
    # file of its own, so that a missing or broken manifest, which fails the import of this file,
    # cannot break the import of the other DAGs.
    DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT, manifest_path=DBT_MANIFEST, install_dbt_deps=False
        ),
        profile_config=ProfileConfig(
            profile_name="agoranomos",
            target_name="prod",
            profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
            # A test of two nodes, such as a relationships test of a model against a seed, runs
            # only in the test task of the node that depends on the other, so after both, and its
            # failure stops the models below that node. By default (eager), the test task of either
            # node would run it, on a first run also before the other exists. The dbt project names
            # such a dependency where no ref makes it, and tests/unit/test_dbt_project.py checks it.
            test_indirect_selection=TestIndirectSelection.BUILDABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # The tests of a model run right after it; a failed one stops the models that read it.
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        # Only the tasks of dbt go into its pool.
        default_args={"pool": DBT_POOL},
    )


agoranomos_demo_transform()
