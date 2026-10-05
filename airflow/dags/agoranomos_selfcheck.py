"""A self-check of the platform: one task that runs through the executor of the stack.

Unpause and trigger it in the Airflow UI to check that the scheduler, the executor and the metadata
database work together. With the Celery variant of the stack the task runs on a worker, so the check
covers the broker and the worker as well. The stack tests in CI run it too.
"""

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="agoranomos_selfcheck",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["agoranomos"],
    doc_md=__doc__,
)
def agoranomos_selfcheck():
    @task
    def ping() -> str:
        return "ok"

    ping()


agoranomos_selfcheck()
