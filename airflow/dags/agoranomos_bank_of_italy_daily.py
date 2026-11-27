"""The daily load of the exchange rates of the Bank of Italy against the euro (ADR-0042).

agoranomos_bank_of_italy_daily runs at 07:17 UTC, after midnight in Rome, and loads with dlt, as
the loader role, the rates of the Bank of Italy into raw_bank_of_italy, one request for each
weekday, at most one a second: on the first run, those of the last 37 months, and then the days
from the day after the last that a load took, or from 14 days back when that is earlier, to
yesterday in Rome (agoranomos_pipelines/bank_of_italy.py), and, when the core lacks rates into the
reporting currency, the history again, as the load of the ECB does (agoranomos_pipelines/rates.py).
The first task, due, skips the run when a load has taken the day already and the core needs no
history. Each load updates the asset of the rates, and the transform runs after it (ADR-0039),
pins the load, takes its rates into core.fx_rates_daily in the order that AGORANOMOS_RATES_SOURCE
gives, first unless it names nbk or ecb (dbt/macros/rates.sql), accepts it and cleans it from the
raw layer. With ecb alone, a load takes its usual window and never the history again.

The DAG starts paused: the demo keeps its own rates and asks the bank nothing, so a deployment with
the demo alone, and the stack tests, need no network. A deployment with a real source unpauses it,
with agoranomos_nbk_daily and agoranomos_ecb_daily, as its guide of the setup says. A repeat is
safe: each load replaces the days that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_BANK_OF_ITALY
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After midnight in Rome, whether it keeps CET or CEST, after the load of the ECB and before the
# daily load of the demo.
SCHEDULE = CronTriggerTimetable("17 7 * * *", timezone="UTC")


@task.short_circuit(task_id="due")
def due() -> bool:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import bank_of_italy, loads, rates

    # Should anything write the passwords of the loader, or of the transformer, which reads what
    # the core lacks, into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    return bank_of_italy.due(get_current_context()["dag_run"].run_after)


@task(task_id="load_bank_of_italy", outlets=[RAW_BANK_OF_ITALY])
def load_bank_of_italy() -> None:
    from agoranomos_pipelines import bank_of_italy, loads, rates

    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    # The moment of the load is the run's, so a retry loads as of the same moment.
    bank_of_italy.load(get_current_context()["dag_run"].run_after)


@dag(
    dag_id="agoranomos_bank_of_italy_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run ends within two hours, retries and their waits included (ADR-0039), so that a load
    # never lives longer than that: the cleanup of the rows of a load that did not end is to wait
    # at least as long (ADR-0046).
    dagrun_timeout=timedelta(hours=2),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/bank_of_italy.py, and these retry the task, also after a new
        # column of the API waited past the lock timeout of the loader role for a run of the
        # transform that reads the table (ADR-0046). The wait doubles with each retry, from 5 to 10
        # minutes and then from 10 to 20, which max_retry_delay cuts to 10: Airflow 3.4.0 takes a
        # multiplier here, and reads True as 2.0.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=10),
        # A daily load takes about ten requests, and the history about 800, at one a second: some
        # 15 minutes. The timeout ends a try that runs too long in Python; a statement that waits
        # on a lock is bounded by the lock timeout of the loader role. Three tries of 30 minutes
        # and their waits of 5 and 10 fit in the two hours of a run.
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["agoranomos", "rates"],
    doc_md=__doc__,
)
def agoranomos_bank_of_italy_daily():
    due() >> load_bank_of_italy()


agoranomos_bank_of_italy_daily()
