"""The transform of agoranomos: the settings of the model, and then dbt builds the model of
advertising statistics.

agoranomos_demo_transform first loads the settings of the model of the deployment, the files of
extensions/model, and the clients of the demo into raw_settings, as the loader role (ADR-0024): a
mistake in a file fails that task with one line that names the file and the entry, nothing is
written, and no task of dbt runs. Then it runs the dbt project through Cosmos, as the transformer
role: the seeds, the staging models of each platform, the steps that the platforms share, and the
marts, each built with its tests in one task, dbt build (ADR-0025, ADR-0028), and the views of the
Ops collection. A failed critical test stops the models that read it, and the marts keep their data.
Before them, one task builds the tables of elementary in the schema ops, into which the hooks of
elementary in every task of dbt write the results of its models and tests (ADR-0029). The DAG runs
when each of the five platforms of the demo has loaded since its last run, each load updating the
asset of its main table, after each load of the rates of the ECB, and after each load of Google Ads
(ADR-0032, ADR-0039), one run at a time: a run takes the events of the loads that ended while the
run before it ran. A repeat is safe: the settings are loaded again, and dbt rebuilds the models. The
DAG starts paused. With the demo
profile, the one-shot demo-init job unpauses it and runs agoranomos_demo_history on the first
start; without it, unpause this DAG in the Airflow UI before you trigger a load. The DAG keeps the
word demo in its id for now, as the Ops collection and the demo name it; a change of its own is to
rename it.

Cosmos renders its tasks from the manifest that metadata-export writes on every start (ADR-0027),
so a change of the dbt project reaches the DAG at the next ./agoranomos up; its tasks of dbt run at
most two at a time, in the pool dbt.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from agoranomos_pipelines.assets import LOADED
from airflow.sdk import dag, task
from airflow.sdk.log import mask_secret
from cosmos import (
    DbtRunLocalOperator,
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
# The models of the package elementary (ADR-0029): one task builds them, before the tasks of dbt,
# and the task group leaves them out.
ELEMENTARY = ["package:elementary"]
# The DAG and the run of Airflow, which elementary records with each run of dbt, from these names
# of its own, so that the Ops collection can group the runs of dbt of one run of the transform.
JOB_ENV = {"DBT_JOB_ID": "{{ dag.dag_id }}", "DBT_JOB_RUN_ID": "{{ run_id }}"}
START = datetime(2026, 1, 1, tzinfo=UTC)
PROFILE = ProfileConfig(
    profile_name="agoranomos",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
)


@dag(
    dag_id="agoranomos_demo_transform",
    schedule=LOADED,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={
        # A failed dbt test does not pass on a retry: it found data to fix. Nor does a mistake in
        # a settings file.
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
    @task(task_id="load_settings")
    def load_settings() -> None:
        # dlt is imported here and not at the top: the DAG processor parses this file often.
        from agoranomos_pipelines.settings import source

        # Should anything write the password of the loader into the log, Airflow shows ***.
        mask_secret(source.credentials()["password"])
        # The settings of the deployment, extensions/model/*.toml, and the clients of the demo;
        # a mistake in a file fails this task, and the tasks of dbt after it do not run.
        source.load()

    # The tables of elementary: its incremental ones keep their rows, and the post-hooks of those of
    # the artifacts of the project write its models and tests once a run. Built before the other
    # tasks of dbt, so that the hooks of the first run find them; until they exist, the hooks write
    # nothing. The image holds the packages (airflow/Dockerfile): no dbt deps.
    elementary = DbtRunLocalOperator(
        task_id="elementary",
        project_dir=DBT_PROJECT,
        profile_config=PROFILE,
        dbt_executable_path=DBT_EXECUTABLE,
        invocation_mode=InvocationMode.SUBPROCESS,
        install_deps=False,
        select=ELEMENTARY,
        env=JOB_ENV,
        # Its tables are no data of the model: no asset event, which Airflow would log as one of
        # an asset that no DAG declares.
        emit_datasets=False,
        pool=DBT_POOL,
    )

    # Cosmos renders the tasks from the manifest of the project, without dbt ls; the DAG still has a
    # file of its own, so that a missing or broken manifest, which fails the import of this file,
    # cannot break the import of the other DAGs.
    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT, manifest_path=DBT_MANIFEST, install_dbt_deps=False
        ),
        profile_config=PROFILE,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
            # A test of two nodes, such as a relationships test of a model against a seed, runs
            # only in the task of the node that depends on the other, so after both, and its
            # failure stops the models below that node. By default (eager), the task of either
            # node would run it, on a first run also before the other exists. The dbt project names
            # such a dependency where no ref makes it, and tests/unit/test_dbt_project.py checks it.
            test_indirect_selection=TestIndirectSelection.BUILDABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # Each model and each seed is one task, dbt build, which builds it and then runs its
            # tests: a failed one fails the task and stops the models that read it. One process of
            # dbt for each node, instead of one for the node and one for its tests, halves the
            # processes of a run (ADR-0028).
            test_behavior=TestBehavior.BUILD,
            exclude=ELEMENTARY,
        ),
        operator_args={"env": JOB_ENV},
        # Only the tasks of dbt go into its pool.
        default_args={"pool": DBT_POOL},
    )
    # Every task of dbt comes after the settings and the tables of elementary.
    load_settings() >> elementary >> dbt


agoranomos_demo_transform()
