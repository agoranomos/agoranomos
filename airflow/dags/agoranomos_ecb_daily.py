"""The daily load of the reference rates of the euro of the European Central Bank (ADR-0032).

agoranomos_ecb_daily runs at 06:47 UTC, after midnight in Frankfurt, and loads with dlt, as the
loader role, the rates of the ECB into raw_ecb: on the first run, those of the last 37 months, and
then the days from the day after the last that a load took, or from 7 days back when that is
earlier, to yesterday in Frankfurt (agoranomos_pipelines/ecb.py). The first task, due, skips the
run when a load has taken the day already. The transform takes the rates at its next run; where the
ECB and the demo both have a rate for a day, it takes the rate of the ECB.

The DAG starts paused: the demo keeps its own rates and asks the ECB nothing, so a deployment with
the demo alone, and the stack tests, need no network. A deployment with a real source unpauses it,
which the setup of the first real source is to do. A repeat is safe: each load replaces the days
that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_ECB
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight in Frankfurt, whether it keeps CET or CEST, and before the daily load of the demo.
SCHEDULE = CronTriggerTimetable("47 6 * * *", timezone="UTC")


@task.short_circuit(task_id="due")
def due() -> bool:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import ecb, loads

    # Should anything write the password of the loader into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    return ecb.due(get_current_context()["dag_run"].run_after)


@task(task_id="load_ecb", outlets=[RAW_ECB])
def load_ecb() -> None:
    from agoranomos_pipelines import ecb, loads

    mask_secret(loads.credentials()["password"])
    # The moment of the load is the run's, so a retry loads as of the same moment.
    ecb.load(get_current_context()["dag_run"].run_after)


@dag(
    dag_id="agoranomos_ecb_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/ecb.py, and these retry the task.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # A load takes seconds. The timeout ends a try that runs too long in Python; a statement
        # that waits on a lock is bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "rates"],
    doc_md=__doc__,
)
def agoranomos_ecb_daily():
    due() >> load_ecb()


agoranomos_ecb_daily()
