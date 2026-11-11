"""The watcher of agoranomos (ADR-0045): every 15 minutes it checks what no failed task shows, the
accounts in quarantine, the accounts whose data is late, the backups and the free disk, and sends an
alert through Apprise to the addresses of AGORANOMOS_ALERT_URLS when something changes, once a day
while a problem remains, and when it is gone. A failed run of any DAG is the plugin's to report
(airflow/plugins/agoranomos_alerts.py).

agoranomos_watcher runs at 4, 19, 34 and 49 minutes past each hour in UTC, away from the times of
the other DAGs of the base package. It reads, and writes the rows of its own table only, so it
neither waits for a backup, whose dump holds a shared lock on each table, nor holds one back: no
time of the day is closed to it (ADR-0020). It is unpaused when Airflow first sees it, like the DAG
of maintenance, so that it runs with no one to start it; a pause that a person makes stays, and
stops the alerts of the watcher, not those of failed runs. Its one task connects as transformer and
takes seconds; a run that fails is retried once, two minutes later. The code of the task is in
agoranomos_pipelines/watcher.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines import maintenance, watcher
from airflow.sdk import CronTriggerTimetable, dag, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
SCHEDULE = CronTriggerTimetable("4,19,34,49 * * * *", timezone="UTC")


@task(task_id="watch")
def watch() -> None:
    # The plugin of the image, which Airflow puts on sys.path: it sends the alerts, and has Airflow
    # mask the addresses in the log of the task, as the password of transformer.
    import agoranomos_alerts

    agoranomos_alerts.mask_addresses()
    mask_secret(maintenance.transformer_credentials()["password"])
    watcher.watch()


@dag(
    dag_id="agoranomos_watcher",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=10),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["agoranomos", "alerts"],
    doc_md=__doc__,
)
def agoranomos_watcher():
    watch()


agoranomos_watcher()
