"""Calls to the REST API of Airflow, for the stack tests. The token fixture of conftest.py signs
them in; the token is a Secret, so a failed test does not print it."""

import json
import time
from urllib.parse import quote

import pytest
from stack_helpers import LogEntries, Secret, SecretHeaders, http_json, wait_for

FINISHED = ("success", "failed")
# The most events that the API of Airflow gives on one page ([api] maximum_page_limit, 100 by
# default in Airflow 3.3.2).
EVENTS_PER_PAGE = 100


def api(airflow_url, token, method, path, body=None, headers=None):
    headers = SecretHeaders({"Authorization": Secret(f"Bearer {token}"), **(headers or {})})
    return http_json(method, f"{airflow_url}{path}", body, headers)


def wait_for_dag(airflow_url, token, dag_id, timeout=180):
    def fetch():
        status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}")
        return body if status == 200 else None

    return wait_for(fetch, timeout, interval=5, message=f"Airflow did not load the DAG {dag_id}")


def unpause(airflow_url, token, dag_id):
    status, body = api(airflow_url, token, "PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    assert status == 200, body


def trigger(airflow_url, token, dag_id):
    """Start a manual run of a DAG and return it, without waiting for it."""
    status, run = api(
        airflow_url, token, "POST", f"/api/v2/dags/{dag_id}/dagRuns", {"logical_date": None}
    )
    assert status in (200, 201), run
    return run


def wait_for_run(airflow_url, token, dag_id, run_id, timeout=300):
    """Wait for the end of a run of a DAG: returns the finished run."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, run = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        assert status == 200, run
        if run["state"] in FINISHED:
            return run
        time.sleep(2)
    pytest.fail(f"the DAG run {run_id} of {dag_id} did not finish in {timeout} seconds")


def run_dag(airflow_url, token, dag_id, timeout=300):
    """Unpause a DAG, run it once and wait for the end of the run: returns the finished run."""
    unpause(airflow_url, token, dag_id)
    run = trigger(airflow_url, token, dag_id)
    return wait_for_run(airflow_url, token, dag_id, run["dag_run_id"], timeout)


def dag_runs(airflow_url, token, dag_id, limit=20):
    """The latest runs of a DAG, the newest first."""
    path = f"/api/v2/dags/{dag_id}/dagRuns?order_by=-run_after&limit={limit}"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    return body["dag_runs"]


def task_instances(airflow_url, token, run):
    path = f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}/taskInstances"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    return body["task_instances"]


def task_log(airflow_url, token, instance):
    """The log of a task instance, every try of it, oldest first, one text per entry.

    An entry of the structured log holds its event and other fields, such as the detail of an
    error, and a search for a value has to see all of them: so each text is the whole entry, as
    JSON, with the letters that are not ASCII as they are. The list shows only the number of its
    entries, as stack_helpers.leaked() looks in them for secrets. A task that never started has no
    try, and no log.
    """
    entries = LogEntries()
    for try_number in range(1, instance["try_number"] + 1):
        path = (
            f"/api/v2/dags/{instance['dag_id']}/dagRuns/{instance['dag_run_id']}"
            f"/taskInstances/{instance['task_id']}/logs/{try_number}"
        )
        status, body = api(airflow_url, token, "GET", path, headers={"Accept": "application/json"})
        assert status == 200, body
        entries.extend(json.dumps(entry, ensure_ascii=False) for entry in body["content"])
    return entries


def asset_events(airflow_url, token, uri, **filters):
    """The events of the asset with this URI, the newest first. filters are fields of the events
    API, such as source_dag_id and source_run_id.

    Airflow keeps the events of every run, also after ./agoranomos down, and its API sorts them by
    timestamp, the oldest first, and gives 50 unless it is told otherwise: the event of a new run
    would be missing once there are more. So the helper asks for the newest first, as many as one
    page can hold, and fails when total_entries says that there are more than the page holds,
    rather than return a part of them. Ask for the event of one run with source_run_id.
    """
    status, body = api(airflow_url, token, "GET", f"/api/v2/assets?uri_pattern={quote(uri)}")
    assert status == 200, body
    matching = [asset for asset in body["assets"] if asset["uri"] == uri]
    assert matching, f"Airflow has no asset {uri}"
    query = "&".join(
        f"{key}={quote(str(value))}"
        for key, value in {
            "asset_id": matching[0]["id"],
            **filters,
            "order_by": "-timestamp",
            "limit": EVENTS_PER_PAGE,
        }.items()
    )
    status, body = api(airflow_url, token, "GET", f"/api/v2/assets/events?{query}")
    assert status == 200, body
    events = body["asset_events"]
    # pytest rewrites the assertions of test modules only, so this one carries its own message.
    assert body["total_entries"] <= len(events), (
        f"Airflow has {body['total_entries']} events of {uri} for {sorted(filters)}, and one page "
        f"holds {len(events)}: narrow the filters, for example with source_run_id"
    )
    return events
