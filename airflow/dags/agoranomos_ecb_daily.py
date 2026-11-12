"""The daily load of the reference rates of the euro of the European Central Bank (ADR-0032).

agoranomos_ecb_daily runs at 06:47 UTC, after midnight in Frankfurt, and loads with dlt, as the
loader role, the rates of the ECB into raw_ecb: on the first run, those of the last 37 months, and
then the days from the day after the last that a load took, or from 14 days back when that is
earlier, to yesterday in Frankfurt (agoranomos_pipelines/ecb.py, ADR-0042). When the core lacks
rates into the reporting currency on days whose loads it has accepted, as after a change of
AGORANOMOS_REPORTING_CURRENCY, which ops.ops_rates names, a load takes the history of the rates
again from those days, with the kind history, after reading the core as the transformer role
(agoranomos_pipelines/rates.py). The first task, due, skips the run when a load has taken the day
already and the core needs no history. Each load updates the asset of the rates, and the transform
runs after it (ADR-0039); where the ECB and the demo both have a rate for a day, it takes the rate
of the ECB.

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
    from agoranomos_pipelines import ecb, loads, rates

    # Should anything write the passwords of the loader, or of the transformer, which reads what
    # the core lacks, into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    return ecb.due(get_current_context()["dag_run"].run_after)


@task(task_id="load_ecb", outlets=[RAW_ECB])
def load_ecb() -> None:
    from agoranomos_pipelines import ecb, loads, rates

    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    # The moment of the load is the run's, so a retry loads as of the same moment.
    ecb.load(get_current_context()["dag_run"].run_after)


@dag(
    dag_id="agoranomos_ecb_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run ends within an hour, retries and their waits included (ADR-0039), so that a load never
    # lives longer than that: the cleanup of the rows of a load that did not end is to wait at least
    # as long (ADR-0046).
    dagrun_timeout=timedelta(hours=1),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/ecb.py, and these retry the task, also after a new column of
        # the API waited past the lock timeout of the loader role for a run of the transform that
        # reads the table (ADR-0046). The wait doubles with each retry, from 5 to 10 minutes and
        # then from 10 to 20, which max_retry_delay cuts to 10: Airflow 3.3.2 takes a multiplier
        # here, and True would be 1, no backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=10),
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
