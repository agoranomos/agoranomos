"""The demo transform, agoranomos_demo_transform: the asset of the raw table starts it, dbt builds
marts from raw_demo, and a failed test of the staging model keeps the mart as it was.

The assertions print counts, sums, states and names, never a secret.
"""

import json

import pytest
from airflow_api import (
    asset_events,
    downstream_task_ids,
    is_busy,
    run_dag,
    task_instances,
    task_log,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from services import DEMO
from stack_helpers import leaked, secrets, totals, wait_for, warehouse_output

pytestmark = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)

LOAD = "agoranomos_demo_load"
TRANSFORM = "agoranomos_demo_transform"
RAW_TABLE = "raw_demo.ad_stats_daily"
MART_TABLE = "marts.ad_spend_daily"
RAW_ASSET = "postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily"
MART_ASSET = "postgres://warehouse-db:5432/warehouse/marts/ad_spend_daily"
# The tasks that Cosmos renders in the task group dbt: for each model a run and then its tests.
STAGING_RUN = "dbt.stg_demo__ad_stats_daily.run"
STAGING_TEST = "dbt.stg_demo__ad_stats_daily.test"
MART_RUN = "dbt.ad_spend_daily.run"
MART_TEST = "dbt.ad_spend_daily.test"
DBT_TASKS = {STAGING_RUN, STAGING_TEST, MART_RUN, MART_TEST}
# The row that breaks a test of the staging model: a negative spend, on a day outside the window.
BROKEN_ROW = (
    "INSERT INTO raw_demo.ad_stats_daily (date, account_id, campaign_id, campaign_name, "
    "impressions, clicks, spend, conversions, currency, _dlt_load_id, _dlt_id) VALUES "
    "(DATE '2026-01-01', 'demo-account-001', 'cmp-stack-test', 'Broken by a test', 10, 1, -1.00, "
    "0, 'USD', 'stack-test', 'stack-test-broken-row')"
)
REMOVE_BROKEN_ROW = "DELETE FROM raw_demo.ad_stats_daily WHERE _dlt_id = 'stack-test-broken-row'"
# The test of the staging model that the broken row fails.
BROKEN_TEST = "non_negative_stg_demo__ad_stats_daily_spend"


def said(entries, text):
    """Whether an entry of a log has the text in its event: the lines that dbt prints are events."""
    return any(text in json.loads(entry).get("event", "") for entry in entries)


def assert_no_secret_in_the_logs(env, airflow_url, token, run):
    """Every try of every dbt task of the run, each entry whole: names are printed, never values.
    The log is read where it is used, so that no frame of a failed test holds it."""
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    instances = task_instances(airflow_url, token, run)
    assert {i["task_id"] for i in instances} == DBT_TASKS
    for instance in instances:
        found = leaked(values, task_log(airflow_url, token, instance))
        assert not found, f"the log of {instance['task_id']} holds the values of {found}"


@pytest.fixture(scope="module")
def transform_run(env, airflow_url, token):
    """A demo load, and the run of the transform DAG that the asset event of that load started."""
    for dag_id in (LOAD, TRANSFORM):
        wait_for_dag(airflow_url, token, dag_id)
    warehouse_output(env, REMOVE_BROKEN_ROW)  # left behind by a test that was stopped
    # A load starts the transform only while the transform is unpaused.
    unpause(airflow_url, token, TRANSFORM)
    # Once the transform is unpaused, the loads of earlier tests start runs of it too. While one is
    # queued or running, Airflow 3.3.2 defers the next run, and that run takes only the events up
    # to the first one that waited: a later event starts no run of its own. So the load of this
    # fixture waits until the transform is idle, and its event then starts a run.
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM),
        600,
        interval=5,
        message="the runs of the transform DAG that earlier loads started did not end",
    )
    load = run_dag(airflow_url, token, LOAD)
    assert load["state"] == "success", load["state"]
    # The tests of the mart compare it with raw_demo, and two empty tables would be equal.
    assert totals(env, RAW_TABLE)["rows"] > 0, "the demo load wrote no rows"

    def started_by_the_load():
        # The event of this load lists the runs that it started.
        [event] = asset_events(
            airflow_url, token, RAW_ASSET, source_dag_id=LOAD, source_run_id=load["dag_run_id"]
        )
        return next((r for r in event["created_dagruns"] if r["dag_id"] == TRANSFORM), None)

    started = wait_for(
        started_by_the_load,
        180,
        interval=5,
        message="the asset of the raw table started no run of the transform DAG",
    )
    return wait_for_run(airflow_url, token, TRANSFORM, started["run_id"], timeout=600)


def test_the_asset_of_the_raw_table_starts_the_transform(transform_run):
    assert transform_run["run_type"] == "asset_triggered"
    assert transform_run["state"] == "success", transform_run["state"]


def test_cosmos_runs_each_model_and_then_its_tests(airflow_url, token, transform_run):
    # One chain: each model, then its tests, and the mart only after the tests of the staging model.
    assert downstream_task_ids(airflow_url, token, TRANSFORM) == {
        STAGING_RUN: [STAGING_TEST],
        STAGING_TEST: [MART_RUN],
        MART_RUN: [MART_TEST],
        MART_TEST: [],
    }
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, transform_run)}
    assert states == dict.fromkeys(DBT_TASKS, "success")


def test_the_mart_holds_what_raw_demo_holds(env, transform_run):
    assert totals(env, MART_TABLE) == totals(env, RAW_TABLE)


def test_cosmos_emits_the_asset_of_the_mart(airflow_url, token, transform_run):
    # The event of this run: Airflow keeps the events of every run before it.
    run_id = transform_run["dag_run_id"]
    events = asset_events(
        airflow_url, token, MART_ASSET, source_dag_id=TRANSFORM, source_run_id=run_id
    )
    assert events, f"the run {run_id} did not update the asset of the mart"
    # The API ignores a filter that it does not know, so the events must be those of this run.
    sources = {(e["source_dag_id"], e["source_run_id"], e["source_task_id"]) for e in events}
    assert sources == {(TRANSFORM, run_id, MART_RUN)}


def test_metabase_reader_reads_the_mart_that_dbt_built(env, transform_run):
    # dbt creates the table as transformer, and the default privileges of transformer give
    # metabase_reader the new tables of marts.
    rows = warehouse_output(env, "SELECT count(*) FROM marts.ad_spend_daily", "metabase_reader")
    assert int(rows) == totals(env, RAW_TABLE)["rows"]


def test_the_logs_of_dbt_hold_no_secret(env, airflow_url, token, transform_run):
    # The line that dbt prints first shows that the scan reads the logs of dbt.
    instances = task_instances(airflow_url, token, transform_run)
    assert {i["task_id"] for i in instances} == DBT_TASKS
    for instance in instances:
        ran_dbt = said(task_log(airflow_url, token, instance), "Running with dbt=")
        assert ran_dbt, f"the log of {instance['task_id']} does not show a run of dbt"
    assert_no_secret_in_the_logs(env, airflow_url, token, transform_run)


def test_a_failed_test_of_the_staging_model_keeps_the_mart(env, airflow_url, token, transform_run):
    # A negative spend breaks the test non_negative of stg_demo__ad_stats_daily. The run of the mart
    # then does not start, and the mart keeps the rows of the last good run.
    before = totals(env, MART_TABLE)
    warehouse_output(env, REMOVE_BROKEN_ROW)  # left behind by a test that was stopped
    try:
        warehouse_output(env, BROKEN_ROW)
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=600)
        assert run["state"] == "failed", run["state"]
        instances = task_instances(airflow_url, token, run)
        states = {i["task_id"]: i["state"] for i in instances}
        assert states == {
            STAGING_RUN: "success",
            STAGING_TEST: "failed",
            MART_RUN: "upstream_failed",
            MART_TEST: "upstream_failed",
        }
        assert totals(env, MART_TABLE) == before
        # The log of the failed task names the test that failed, with its one row, as dbt 1.12
        # prints it. No log of the run holds a secret: a failure prints more than a success does.
        [failed] = [i for i in instances if i["task_id"] == STAGING_TEST]
        named = said(task_log(airflow_url, token, failed), f"FAIL 1 {BROKEN_TEST}")
        assert named, f"the log of {STAGING_TEST} does not name the test {BROKEN_TEST}"
        assert_no_secret_in_the_logs(env, airflow_url, token, run)
    finally:
        warehouse_output(env, REMOVE_BROKEN_ROW)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=600)
    assert run["state"] == "success", run["state"]
    assert totals(env, MART_TABLE) == before
