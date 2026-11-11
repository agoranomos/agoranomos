"""The maintenance of agoranomos: the upkeep that the stack and the warehouse need, once a day, with
no operator (ADR-0039).

The retention of ADR-0044:

- clean_airflow_logs deletes the logs of Airflow older than 30 days from the volume of the logs, the
  logs of the tasks and of the DAG processor, and the folders that the files leave empty a day
  later. A run of Airflow stays 90 days, so a run between 30 and 90 days old has no log any more.
- clean_ops deletes the results of the runs of dbt older than 90 days from the tables of elementary
  in the schema ops, as the transformer role. The Ops collection reads the last 30 days.
- clean_dlt_loads deletes, as the loader role, the rows of _dlt_loads of each raw schema older than
  90 days, but those that the next load, dlt or the next pin still reads.
- clean_core_loads deletes, as transformer, the accepted rows of the kind window of core.loads
  older than 13 months, but those that a run or a load still reads, and none whose row _dlt_loads
  still holds; a row of the kind history stays.
- drop_old_journal_months drops the months of the journals of restatements older than 13 months,
  through core.drop_partition, and leaves a month that is in use to the next run.
- clean_cosmos_variables deletes the Variables cosmos_xcom_backup__* of the runs older than 90 days,
  which the watcher mode of Cosmos leaves behind after a failed run.

The upkeep of the core, as transformer:

- make_partitions makes the partitions of the current month and the two after it, in UTC, of each
  fact and journal of the core, through core.ensure_partition.
- analyze_the_facts runs ANALYZE ONLY on the parents of the facts, which autovacuum does not
  analyze, and fails when one was not analyzed.
- freeze_old_months runs VACUUM (FREEZE) on each month of a fact that has left the window of
  restatements and changed since it was last frozen, and fails when one was not vacuumed.

What Ops shows, as transformer:

- record_the_disk records the used and the free disk, the size of the warehouse and of its core,
  and the partitions of the window of restatements with their free space, in ops.ops_disk.
- compare_with_platform_totals compares the core with core.platform_totals_daily over the last 90
  days once a month, into ops.ops_core_totals.

The steps on the core record what they did and the seconds that each took in
ops.ops_maintenance_steps.

agoranomos_maintenance runs at 04:23 UTC, away from the default time of the daily backup and before
the loads of the day. It is unpaused when Airflow first sees it, unlike the other DAGs of the base
package, so that it runs by itself, and at once, for the last time of its schedule; a pause that a
person makes stays. On the first start of a stack that run can come before the API server of
Airflow answers, and its first task then fails and is tried again five minutes later, and before
the first run of the transform, which makes the tables of ops that the tasks record into: the
tasks do their work and record nothing. Its tasks do not wait for each other and run one at a time,
so that two steps never take the locks of one table at once: a task that fails leaves the others
to run and the run failed, and a repeat is safe. The database of Airflow is not cleaned here, as a
task has no access to it: airflow-init cleans it on each start (compose.yaml). The code of the
tasks is in agoranomos_pipelines/maintenance.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import maintenance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronTriggerTimetable, Variable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# Away from 03:00 UTC, the default time of the daily backup (BACKUP_TIME), and before the first load
# of the day, that of the rates of the ECB at 06:47 UTC.
SCHEDULE = CronTriggerTimetable("23 4 * * *", timezone="UTC")


def run_id() -> str:
    """The ID of the run of the DAG, which the tables of ops record with each step."""
    mask_secret(maintenance.transformer_credentials()["password"])
    return get_current_context()["run_id"]


@task(task_id="clean_ops")
def clean_ops() -> None:
    # Should anything write the password of transformer into the log, Airflow shows ***.
    mask_secret(maintenance.transformer_credentials()["password"])
    maintenance.clean_ops()


@task(task_id="clean_dlt_loads")
def clean_dlt_loads() -> None:
    mask_secret(maintenance.loader_credentials()["password"])
    maintenance.clean_dlt_loads(run_id())


@task(task_id="clean_core_loads")
def clean_core_loads() -> None:
    maintenance.clean_core_loads(run_id())


@task(task_id="drop_old_journal_months")
def drop_old_journal_months() -> None:
    maintenance.drop_old_journal_months(run_id())


@task(task_id="clean_cosmos_variables")
def clean_cosmos_variables() -> None:
    # The Task SDK lists the names of the Variables in the database of Airflow, and deletes one.
    maintenance.clean_cosmos_variables(
        Variable.keys(prefix=maintenance.VARIABLE_PREFIX), Variable.delete, datetime.now(UTC)
    )


@task(task_id="make_partitions")
def make_partitions() -> None:
    maintenance.make_partitions(run_id())


@task(task_id="analyze_the_facts")
def analyze_the_facts() -> None:
    maintenance.analyze_the_facts(run_id())


# The first freeze of a long history can take most of the budget of the step, 20 minutes.
@task(task_id="freeze_old_months", execution_timeout=timedelta(minutes=30))
def freeze_old_months() -> None:
    maintenance.freeze_old_months(run_id())


@task(task_id="record_the_disk")
def record_the_disk() -> None:
    maintenance.record_the_disk(run_id())


@task(task_id="compare_with_platform_totals")
def compare_with_platform_totals() -> None:
    maintenance.compare_with_platform_totals(run_id())


@dag(
    dag_id="agoranomos_maintenance",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    # One task at a time, so that the upkeep takes little of the memory of the tasks and of the
    # warehouse, and two steps never lock one table at once (#189); the tasks do not depend on
    # each other.
    max_active_tasks=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # Each task takes seconds, but the freeze of the old months.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "maintenance"],
    doc_md=__doc__,
)
def agoranomos_maintenance():
    # find prints nothing, and the task keeps no XCom of it.
    BashOperator(
        task_id="clean_airflow_logs",
        bash_command=maintenance.clean_logs_command(),
        do_xcom_push=False,
    )
    clean_ops()
    clean_dlt_loads()
    clean_core_loads()
    drop_old_journal_months()
    clean_cosmos_variables()
    make_partitions()
    analyze_the_facts()
    freeze_old_months()
    record_the_disk()
    compare_with_platform_totals()


agoranomos_maintenance()
