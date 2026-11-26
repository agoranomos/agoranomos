"""The history of the GA4 properties of the deployment, in the background (ADR-0038, "The first
load"; ADR-0054, "The window and the history").

The first load of a property takes its usual window, the last 14 days, so that a dashboard fills
in minutes, and agoranomos_ga4_history brings the days before it, back to the first day of the
month 36 months before yesterday (ADR-0034); no page that Google publishes gives the oldest day
that the Data API returns, so no oldest day bounds it, and a quarter before the property had data
costs one request and loads no row. It runs after each run of the transform that succeeded, on
the asset TRANSFORMED, one run at a time, and starts at most one portion of the history: the
newest calendar quarter that a property lacks days of, for every property that lacks days of it,
as a run of agoranomos_ga4_daily with the properties and the days as its parameters
(agoranomos_pipelines/ga4.py, plan, and agoranomos_pipelines/history.py, the planner that the
histories of Google Ads, of Meta and of TikTok share). That load updates the asset of GA4, and the
next portion comes once the transform has pinned it, so that each portion goes from raw to the
core whole before the next one, from the new quarters to the old ones. The plan starts nothing
while a run of the load is queued or running, while a whole load waits for the run of the
transform that pins it, or once every property that has had its usual window holds its history;
it reads the journal of the loads and core.loads as the loader role, and asks GA4 nothing.

The history of GA4 plans apart from those of the ad platforms, and its portion may run at the same
time as theirs: the APIs and their quotas are apart.

The DAG starts paused: a deployment unpauses it with agoranomos_ga4_daily, once its key and its
properties are in place ([Connect GA4](docs/how-to/connect-ga4.md)). A run of the load of a portion
that fails for some of its properties fails after its retries, and its asset starts the transform
for the others, whose end starts the next plan; one that fails for every property fails its task,
which updates no asset, so the next plan comes after the next run of the transform that succeeds,
which another load starts. The failed property then holds up no other
(agoranomos_pipelines/history.py, failures).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from agoranomos_pipelines.assets import TRANSFORMED
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

START = datetime(2026, 1, 1, tzinfo=UTC)
# The DAG of the load of GA4, which takes a portion from the parameters of its run.
LOAD = "agoranomos_ga4_daily"


@task.short_circuit(task_id="plan")
def plan() -> dict | None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import ga4, loads

    # Should anything write the password of the loader into the log, Airflow shows ***. The plan
    # reads no key of GA4.
    mask_secret(loads.credentials()["password"])
    context = get_current_context()
    # A run of the load that is queued or running, of the history or the scheduled one, ends
    # first, and the transform that takes it starts the plan again.
    active = context["ti"].get_dr_count(dag_id=LOAD, states=["queued", "running"])
    if active:
        log.info("%d runs of %s are queued or running: the history waits for them", active, LOAD)
        return None
    portion = ga4.plan(context["dag_run"].run_after)
    # The parameters of the run of the load, or nothing to start, which skips the next task.
    return None if portion is None else portion.conf()


@dag(
    dag_id="agoranomos_ga4_history",
    schedule=TRANSFORMED,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # A run reads two tables and starts another run: it ends within minutes.
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        # The plan reads the warehouse, which a retry reads again; the next run of the transform
        # that succeeds starts the plan anyway.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["agoranomos", "ga4"],
    doc_md=__doc__,
)
def agoranomos_ga4_history():
    # The run of the load of the portion, without waiting for it: its asset starts the transform,
    # and the end of the transform the next plan. A paused DAG of the load fails the run, rather
    # than keep a run of the history queued until it is unpaused. The parameters of the run are
    # those of the portion and no other: no information of OpenLineage goes into them.
    TriggerDagRunOperator(
        task_id="load_portion",
        trigger_dag_id=LOAD,
        conf=plan(),
        fail_when_dag_is_paused=True,
        openlineage_inject_parent_info=False,
    )


agoranomos_ga4_history()
