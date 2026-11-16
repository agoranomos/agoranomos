"""The history of the Meta ad accounts of the deployment, in the background (ADR-0038, "The first
load"; ADR-0050, "The windows and the history").

The first load of an account takes its usual window, the last 28 days, so that a dashboard fills in
minutes, and agoranomos_meta_ads_history brings the days before it, back to the first day of the
month 36 months before yesterday (ADR-0034), and never before the 37 months past which Meta refuses
a start date with error 3018. It runs after each run of the transform that succeeded, on the asset
TRANSFORMED, one run at a time, and starts at most one portion of the history: the newest calendar
quarter that an account lacks days of, for every account that lacks days of it, as a run of
agoranomos_meta_ads_daily with the accounts and the days as its parameters, which takes the
insights of the portion through asynchronous report runs (agoranomos_pipelines/meta_ads.py, plan,
and agoranomos_pipelines/history.py, the planner that the history of Google Ads shares). That load
updates the asset of Meta, the transform takes it, pins it into core.loads with the kind history
and creates the partitions of its months before it writes the core, and its end starts the next
portion, so that each portion goes from raw to the core whole before the next one, from the new
quarters to the old ones. A portion takes its accounts for at most an hour, and leaves those that
it has not reached to the next portion, so that it ends within the timeout of its task. The plan
starts nothing while a run of the load is queued or running, while a whole load waits for the run
of the transform that pins it, or once every account that has had its usual window holds its
history; it reads the journal of the loads and core.loads as the loader role, and asks Meta
nothing.

The history of Meta and that of Google Ads plan apart, and may each have a portion running at the
same time: their APIs and their quotas are apart, a load writes what it reads to disk a page at a
time, and the one transform takes the loads that have ended one run at a time.

The DAG starts paused: a deployment unpauses it with agoranomos_meta_ads_daily, once its token and
its accounts are in place ([Connect Meta](docs/how-to/connect-meta.md)). A run of the load of a
portion that fails for an account, also one whose report run ran out of the time of the portion,
fails its task, which updates no asset, so the next plan comes after the next run of the
transform that succeeds, which another load starts. The account then holds up no other: the
portions go on without it, and its quarter comes again once the other accounts hold their
history and a load of its usual window has loaded it since (agoranomos_pipelines/history.py,
failures). Ops shows the progress (ops.ops_history).
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
# The DAG of the load of Meta, which takes a portion from the parameters of its run.
LOAD = "agoranomos_meta_ads_daily"


@task.short_circuit(task_id="plan")
def plan() -> dict | None:
    # dlt is imported here and not at the top: the DAG processor parses this file often.
    from agoranomos_pipelines import loads, meta_ads

    # Should anything write the password of the loader into the log, Airflow shows ***. The plan
    # reads no token of Meta.
    mask_secret(loads.credentials()["password"])
    context = get_current_context()
    # A run of the load that is queued or running, of the history or the scheduled one, ends
    # first, and the transform that takes it starts the plan again.
    active = context["ti"].get_dr_count(dag_id=LOAD, states=["queued", "running"])
    if active:
        log.info("%d runs of %s are queued or running: the history waits for them", active, LOAD)
        return None
    portion = meta_ads.plan(context["dag_run"].run_after)
    # The parameters of the run of the load, or nothing to start, which skips the next task.
    return None if portion is None else portion.conf()


@dag(
    dag_id="agoranomos_meta_ads_history",
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
    tags=["agoranomos", "meta_ads"],
    doc_md=__doc__,
)
def agoranomos_meta_ads_history():
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


agoranomos_meta_ads_history()
