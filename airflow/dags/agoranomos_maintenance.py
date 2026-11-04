"""The maintenance of agoranomos: the upkeep that the stack and the warehouse need, once a day, with
no operator (ADR-0039). Today it runs the retention of ADR-0044:

- clean_airflow_logs deletes the logs of Airflow older than 30 days from the volume of the logs, the
  logs of the tasks and of the DAG processor, and the folders that the files leave empty a day
  later. A run of Airflow stays 90 days, so a run between 30 and 90 days old has no log any more.
- clean_ops deletes the results of the runs of dbt older than 90 days from the tables of elementary
  in the schema ops, as the transformer role. The Ops collection reads the last 30 days.

agoranomos_maintenance runs at 04:23 UTC, away from the default time of the daily backup and before
the loads of the day. It is unpaused when Airflow first sees it, unlike the other DAGs of the base
package, so that it runs by itself, and at once, for the last time of its schedule; a pause that a
person makes stays. Its tasks do not wait for each other and run one at a time: a task that fails
leaves the others to run and the run failed, and a repeat is safe. The database of Airflow is not
cleaned here, as a task has no access to it: airflow-init cleans it on each start (compose.yaml).
The code of the tasks is in agoranomos_pipelines/maintenance.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import maintenance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronTriggerTimetable, dag, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# Away from 03:00 UTC, the default time of the daily backup (BACKUP_TIME), and before the first load
# of the day, that of the rates of the ECB at 06:47 UTC.
SCHEDULE = CronTriggerTimetable("23 4 * * *", timezone="UTC")


@task(task_id="clean_ops")
def clean_ops() -> None:
    # Should anything write the password of transformer into the log, Airflow shows ***.
    mask_secret(maintenance.transformer_credentials()["password"])
    maintenance.clean_ops()


@dag(
    dag_id="agoranomos_maintenance",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    # One task at a time, so that the upkeep takes little of the memory of the tasks and of the
    # warehouse; the tasks do not depend on each other.
    max_active_tasks=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # Each task takes seconds.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "maintenance"],
    doc_md=__doc__,
)
def agoranomos_maintenance():
    BashOperator(task_id="clean_airflow_logs", bash_command=maintenance.clean_logs_command())
    clean_ops()


agoranomos_maintenance()
