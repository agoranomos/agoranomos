"""The history of the loads (ADR-0038, "The first load"): the kind of each load in core.loads, which
the pin copies from the journal of the load, the view of the progress in the schema ops, the asset
that the last task of a run of the transform that succeeded updates, and the history of Google Ads,
which runs on that asset, in a running stack.

No load reaches Google here: the DAGs of Google Ads stay paused but for one run of the history,
whose plan reads the warehouse and asks the API nothing. That test names an account of google_ads
of its own in extensions/model/clients.toml, with an ID of the placeholder of the guides, which no
load ever took, so the plan finds no usual window and starts nothing; it takes the file out again
and pauses the DAG in its finally. If it is killed before that, delete
extensions/model/clients.toml and pause agoranomos_google_ads_history by hand. The demo takes its
history in one load of each platform, of the kind history, and a daily load of the demo its usual
window.

The assertions print IDs, days, counts, kinds and states, never a secret.
"""

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import (
    api,
    asset_events,
    dag_runs,
    downstream_task_ids,
    is_busy,
    is_paused,
    pause,
    task_instances,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from stack_helpers import REPO_ROOT, wait_for, warehouse, warehouse_output

TRANSFORM = "agoranomos_demo_transform"
LOAD = "agoranomos_google_ads_daily"
HISTORY = "agoranomos_google_ads_history"
# The asset of a run of the transform in which every task succeeded (assets.py).
TRANSFORMED = "agoranomos_transformed"
# The rows of each whole load of a schema of raw, by load and account, with the kind that its
# journal gives, as the pin reads it: a row without one is of the usual window.
JOURNAL = (
    "SELECT j._dlt_load_id, j.account_id, coalesce(j.kind, 'window') "
    "FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0"
)
# An account of google_ads of the test, with the ten digits that the guides show.
CLIENTS = """\
[clients.history-test]
name = "History of the stack test"

[[accounts]]
platform = "google_ads"
account_id = "1234567890"
client = "history-test"
"""


@pytest.fixture(scope="module")
def transform_run(airflow_url, token):
    """The newest successful run of the transform: the first start made one, and the tests of the
    transform before these made more."""
    runs = dag_runs(airflow_url, token, TRANSFORM, limit=50)
    successful = [run for run in runs if run["state"] == "success"]
    assert successful, f"{TRANSFORM} has no successful run among its last 50"
    return successful[0]


def rows_of(env, sql):
    return {tuple(line.split("|")) for line in warehouse_output(env, sql).splitlines()}


@pytest.mark.demo
@pytest.mark.usefixtures("daily_load_paused")
def test_the_pin_copies_the_kind_of_each_load_of_the_demo(env, transform_run):
    # Each whole load of each platform of the demo that a run pinned is in core.loads with the
    # kind of its journal; the history of each platform is of the kind history, for every account
    # of the platform. A load that a test made after the last pin waits for the next one.
    for code, platform in platforms.PLATFORMS.items():
        journal = {
            (load, account): kind
            for load, account, kind in rows_of(env, JOURNAL.format(dataset=platform.dataset))
        }
        pinned = {
            (load, account): kind
            for load, account, kind in rows_of(
                env,
                "SELECT load_id, account_id, kind FROM core.loads "  # noqa: S608 (a code of the demo)
                f"WHERE source = '{code}'",
            )
        }
        both = journal.keys() & pinned.keys()
        assert both, code
        differ = sorted(key for key in both if journal[key] != pinned[key])
        assert not differ, (code, differ[:3])
        history = {account for (_, account), kind in pinned.items() if kind == "history"}
        assert history == {a.account_id for a in platform.module.accounts()}, code
        assert set(pinned.values()) <= {"window", "history"}, code


@pytest.mark.demo
@pytest.mark.usefixtures("daily_load_paused")
def test_ops_shows_how_far_back_the_loads_of_each_account_reach(env, transform_run):
    # One row for each account that a pin has taken: the first day of its whole loads, and its
    # loads of history, the history of the demo among them. metabase_reader reads the view.
    shown = {
        (platform, account): (first, int(portions), last)
        for platform, account, first, portions, last in rows_of(
            env,
            "SELECT platform, account_id, first_day_loaded, portions, last_portion_at "
            "FROM ops.ops_history",
        )
    }
    expected = rows_of(
        env,
        "SELECT source, account_id, min(window_from), "
        "count(*) FILTER (WHERE kind = 'history') "
        "FROM core.loads WHERE outcome = 'loaded' GROUP BY source, account_id",
    )
    assert {(s, a): (f, int(n)) for s, a, f, n in expected} == {
        key: value[:2] for key, value in shown.items()
    }
    for code, platform in platforms.PLATFORMS.items():
        for account in platform.module.accounts():
            _, portions, last = shown[(code, account.account_id)]
            assert portions >= 1, (code, account.account_id)
            assert last, (code, account.account_id)
    result = warehouse(env, "SELECT count(*) FROM ops.ops_history", "metabase_reader")
    assert result.returncode == 0, result.stderr


@pytest.mark.demo
@pytest.mark.usefixtures("daily_load_paused")
def test_a_run_of_the_transform_that_succeeded_ends_by_updating_its_asset(
    airflow_url, token, transform_run
):
    # The task transformed comes after every other task of the run, and its event is that of the
    # run, on which the history of Google Ads runs.
    downstream = downstream_task_ids(airflow_url, token, TRANSFORM)
    assert downstream["transformed"] == []
    pointing = {task for task, following in downstream.items() if "transformed" in following}
    leaves = {
        task for task, following in downstream.items() if task != "transformed" and not following
    }
    assert not leaves, sorted(leaves)[:5]
    assert pointing, "no task comes before transformed"
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, transform_run)}
    assert states["transformed"] == "success"
    events = asset_events(
        airflow_url,
        token,
        TRANSFORMED,
        source_dag_id=TRANSFORM,
        source_run_id=transform_run["dag_run_id"],
    )
    assert len(events) == 1, len(events)


def test_the_load_of_google_ads_takes_the_parameters_of_a_run_of_the_history(airflow_url, token):
    # A scheduled run has the defaults, and takes the usual window; the history DAG runs on the
    # asset of the transform. Both start paused.
    for dag_id in (LOAD, HISTORY):
        wait_for_dag(airflow_url, token, dag_id)
    status, details = api(airflow_url, token, "GET", f"/api/v2/dags/{LOAD}/details")
    assert status == 200, details
    params = {name: param["value"] for name, param in details["params"].items()}
    assert params == {"accounts": [], "window_from": None, "window_to": None}
    status, details = api(airflow_url, token, "GET", f"/api/v2/dags/{HISTORY}/details")
    assert status == 200, details
    assert TRANSFORMED in str(details["asset_expression"]), details["asset_expression"]
    assert details["max_active_runs"] == 1


def test_the_history_of_google_ads_starts_nothing_before_the_usual_window(env, airflow_url, token):
    # An account that no load has taken waits for its usual window, which the daily load takes:
    # the plan reads the journal and core.loads as loader, finds no window, and the task that
    # would start the load of a portion is skipped. No run of the load starts.
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    path = (REPO_ROOT / folder).resolve() / "model" / "clients.toml"
    assert not path.exists(), "extensions/model/clients.toml is in the way of this test"
    wait_for_dag(airflow_url, token, HISTORY)
    assert is_paused(airflow_url, token, HISTORY), f"{HISTORY} is to start paused"
    loads_before = len(dag_runs(airflow_url, token, LOAD, limit=50))
    path.write_text(CLIENTS, encoding="utf-8")
    try:
        unpause(airflow_url, token, HISTORY)
        run = trigger(airflow_url, token, HISTORY)
        run = wait_for_run(airflow_url, token, HISTORY, run["dag_run_id"], timeout=300)
        states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, run)}
        assert run["state"] == "success", states
        assert states == {"plan": "success", "load_portion": "skipped"}
        assert len(dag_runs(airflow_url, token, LOAD, limit=50)) == loads_before
    finally:
        # A run that an event of the asset queued while the DAG was paused runs once it is
        # unpaused, and ends before the DAG is paused again and the file goes.
        wait_for(
            lambda: not is_busy(airflow_url, token, HISTORY),
            300,
            interval=5,
            message=f"the runs of {HISTORY} did not end",
        )
        pause(airflow_url, token, HISTORY)
        path.unlink(missing_ok=True)
