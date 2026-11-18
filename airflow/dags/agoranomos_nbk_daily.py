"""The daily load of the official rates of the tenge of the National Bank of Kazakhstan (ADR-0042).

agoranomos_nbk_daily runs at 11:13 UTC, 16:13 in Astana, after the NBK has fixed the rates of the
day at 15:30 and 16:00 Astana time, and loads with dlt, as the loader role, the rates of the NBK
into raw_nbk, one request for each calendar day, at least a second apart: on the first run, those
of the last 37 months, and then the days from the day after the last that a load took, or from 14
days back when that is earlier, to today in Astana (agoranomos_pipelines/nbk.py). When the core
lacks rates into the reporting currency on days whose loads it has accepted, a load takes the
history again from those days, after reading the core as the transformer role
(agoranomos_pipelines/rates.py). The first task, due, skips the run when a load has taken the day
already and the core needs no history. Each load updates the asset of the rates of the NBK, and
the transform runs after it (ADR-0039), which pins the load, accepts it and removes its rows from
raw_nbk as for any other source. core.fx_rates_daily takes these rates in the order that
AGORANOMOS_RATES_SOURCE gives (dbt/macros/rates.sql): first for a reporting currency of KZT, and
else for a currency that the Bank of Italy lacks, such as RUB and BYN, through the tenge. With ecb
alone, a load takes its usual window and never the history again.

The DAG starts paused: the demo keeps its own rates and asks the NBK nothing, so a deployment with
the demo alone, and the stack tests, need no network. A deployment with a real source unpauses it,
with agoranomos_bank_of_italy_daily and agoranomos_ecb_daily, as its guide of the setup says. A
repeat is safe: each load replaces the days that it covers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import RAW_NBK
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.log import mask_secret

START = datetime(2026, 1, 1, tzinfo=UTC)
# After 16:00 in Astana, UTC+5, when the NBK has fixed the rates of the next business day, and the
# set of today is there.
SCHEDULE = CronTriggerTimetable("13 11 * * *", timezone="UTC")


@task.short_circuit(task_id="due")
def due() -> bool:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, nbk, rates

    # Should anything write the passwords of the loader, or of the transformer, which reads what
    # the core lacks, into the log, Airflow shows ***.
    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    return nbk.due(get_current_context()["dag_run"].run_after)


@task(task_id="load_nbk", outlets=[RAW_NBK])
def load_nbk() -> None:
    from agoranomos_pipelines import loads, nbk, rates

    mask_secret(loads.credentials()["password"])
    mask_secret(rates.transformer_password())
    # The moment of the load is the run's, so a retry loads as of the same moment.
    nbk.load(get_current_context()["dag_run"].run_after)


@dag(
    dag_id="agoranomos_nbk_daily",
    schedule=SCHEDULE,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run ends within four hours, its three tries and the waits between them included, so that a
    # load never lives longer than that: the cleanup of the rows of a load that did not end is to
    # wait at least as long (ADR-0046).
    dagrun_timeout=timedelta(hours=4),
    default_args={
        # Retries are explicit, as the contract of the sources asks (ADR-0004): those of a request
        # are in agoranomos_pipelines/nbk.py, and these retry the task, also after a new column of
        # the API waited past the lock timeout of the loader role for a run of the transform that
        # reads the table (ADR-0046). The wait doubles with each retry, from 5 to 10 minutes and
        # then from 10 to 20, which max_retry_delay cuts to 10: Airflow 3.3.2 takes a multiplier
        # here, and True would be 1, no backoff.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(minutes=10),
        # A daily load takes 14 requests, and the history about 1,130, a second apart, about 20
        # minutes. The timeout ends a try that runs too long in Python; a statement that waits on
        # a lock is bounded by the lock timeout of the loader role.
        "execution_timeout": timedelta(hours=1),
    },
    tags=["agoranomos", "rates"],
    doc_md=__doc__,
)
def agoranomos_nbk_daily():
    due() >> load_nbk()


agoranomos_nbk_daily()
