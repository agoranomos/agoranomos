"""The daily load of the demo of Agoranomos: each day, the days of the fictional platforms and of
the source of rates that are new since the last load, and the recent days whose numbers still
change.

agoranomos_demo_daily runs at 09:17 UTC, after midnight in the time zone of every account of the
demo, and loads the five platforms of agoranomos_demo_history, as the loader role, one after
another: for each account, the days from the day after the last that a load took, or from the first
day of the reload window of the platform if that is earlier, to yesterday in the time zone of the
account (ADR-0025). The window is 90 days on demo_search, 28 on demo_social and demo_video, 14 on
demo_web and 7 on demo_fx. A platform that holds no load yet gets its history. The first task, due,
skips the run when every account has a load of its day already, as another load on the same day
would bring the same numbers: so does the run that the DAG makes at once, for the last time of its
schedule, when it is unpaused after the history. Each load updates the asset of its main table, so
the transform runs after the five. A repeat is safe: each load replaces the days of each account
that it covers. The loads of this DAG and of the history run one at a time, in the pool demo_load.
The DAG starts paused: with the demo profile, the one-shot demo-init job unpauses it after the
history, on the first start that finds it without a run, so a pause that a person made stays.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_DEMO_PLATFORMS
from agoranomos_pipelines.demo.platforms import LOAD_ORDER
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight in the time zone of every account of the demo: the day of the westernmost,
# Etc/GMT+8, starts at 08:00 UTC.
SCHEDULE = CronTriggerTimetable("17 9 * * *", timezone="UTC")
# The pool of the loads of the demo, which airflow-init sets on every start: one slot, so that a
# load of this DAG and one of the history never replace the days of a platform at the same time.
LOAD_POOL = "demo_load"


@task.short_circuit(task_id="due")
def due() -> bool:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines.demo import sources

    # Should anything write the password of the loader into the log, Airflow shows ***.
    mask_secret(sources.credentials()["password"])
    return sources.due(get_current_context()["dag_run"].run_after)


def load_task(code: str):
    @task(task_id=f"load_{code}", outlets=[RAW_DEMO_PLATFORMS[code]], pool=LOAD_POOL)
    def load() -> None:
        from agoranomos_pipelines.demo import sources

        mask_secret(sources.credentials()["password"])
        # The moment of the load is the run's, so a retry loads as of the same moment.
        sources.load_daily(code, get_current_context()["dag_run"].run_after)

    return load()


@dag(
    dag_id="agoranomos_demo_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004).
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        # A daily load takes seconds. The timeout ends a try that runs too long in Python; a
        # statement that waits on a lock is bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "demo"],
    doc_md=__doc__,
)
def agoranomos_demo_daily():
    previous = due()
    for code in LOAD_ORDER:
        current = load_task(code)
        previous >> current
        previous = current


agoranomos_demo_daily()
