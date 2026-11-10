"""A DAG that the stack tests of the alerts write into the extensions folder, under an ID of their
own each time, and remove (tests/stack/test_alerts.py): its one task kills its own process with
SIGKILL, as the kernel does at a memory limit, so that the run fails without any callback of the
task, and the listener of the plugin agoranomos_alerts in the scheduler sends the alert of the run
(ADR-0045).
"""

import os
import signal

import pendulum
from airflow.sdk import dag, task

DAG_ID = "ci_alert_check"


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},
    tags=["ci"],
    doc_md=__doc__,
)
def ci_alert_check():
    @task
    def killed() -> None:
        os.kill(os.getpid(), signal.SIGKILL)

    killed()


ci_alert_check()
