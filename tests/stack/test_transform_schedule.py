"""What starts the transform, agoranomos_demo_transform (ADR-0039): an event of the asset of the
rates of the ECB, which a load of the rates makes, starts a run of it on its own, as the five assets
of the demo do together.

The ECB is not asked: the DAG of the rates starts paused, and the stack tests need no network
(ADR-0032). So the test makes the event of the asset through the API of Airflow, as the task
load_ecb makes it with its outlet, and reads the events that Airflow lists for the run that
followed. The assertions print ids, states and URIs, never a secret.
"""

from urllib.parse import quote

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import (
    api,
    dag_runs,
    is_busy,
    is_paused,
    pause,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from services import DEMO
from stack_helpers import wait_for

pytestmark = [
    pytest.mark.skipif(
        not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
    ),
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
# The asset of the rates of the ECB, as agoranomos_pipelines/assets.py declares it;
# tests/unit/test_dag_files.py checks the line.
RATES = "postgres://warehouse-db:5432/warehouse/raw_ecb/rates_daily"
# The assets of the demo, which are one condition of the schedule of the transform: the events of
# some of them start no run.
DEMO_ASSETS = len(platforms.PLATFORMS)


def asset_id(airflow_url, token, uri):
    """The id of the asset with this URI, among every asset that Airflow knows, active or not."""
    path = f"/api/v2/assets?uri_pattern={quote(uri)}&only_active=false"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    [found] = [asset["id"] for asset in body["assets"] if asset["uri"] == uri]
    return found


def consumed_events(airflow_url, token, run):
    """The asset events that started a run, as Airflow lists them."""
    path = f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}/upstreamAssetEvents"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    return body["asset_events"]


@pytest.fixture(scope="module")
def idle_transform(airflow_url, token):
    """The transform unpaused, with no run queued or running and none that Airflow is about to
    create, so that the run of the test is the one that its event starts; paused again at the end
    when it was."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    was_paused = is_paused(airflow_url, token, TRANSFORM)
    unpause(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=DEMO_ASSETS),
        900,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    yield
    if was_paused:
        pause(airflow_url, token, TRANSFORM)


def test_a_load_of_the_rates_starts_the_transform(airflow_url, token, idle_transform):
    before = {run["dag_run_id"] for run in dag_runs(airflow_url, token, TRANSFORM)}
    status, event = api(
        airflow_url,
        token,
        "POST",
        "/api/v2/assets/events",
        {"asset_id": asset_id(airflow_url, token, RATES), "extra": {"source": "tests/stack"}},
    )
    assert status in (200, 201), event
    assert event["uri"] == RATES, event

    def started():
        # The scheduler creates the run at its next loop, queued, and only then lists it.
        for run in dag_runs(airflow_url, token, TRANSFORM, limit=5):
            if run["dag_run_id"] not in before and run["run_type"] == "asset_triggered":
                return run
        return None

    run = wait_for(
        started, 120, interval=2, message="no run of the transform followed the event of the rates"
    )
    # The run took the event, and no event of the demo, whose loads were paused, with it; the
    # events of an earlier load that started no run, as a history that failed half way leaves them,
    # would come along, so the test asks for its own event and not for the only one.
    consumed = {(e["id"], e["uri"]) for e in consumed_events(airflow_url, token, run)}
    assert (event["id"], RATES) in consumed, sorted(consumed)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=900)
    assert run["state"] == "success", run["state"]
