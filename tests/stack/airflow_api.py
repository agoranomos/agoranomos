"""Calls to the REST API of Airflow, for the stack tests. The token fixture of conftest.py signs
them in; the token is a Secret, so a failed test does not print it."""

import time

import pytest
from stack_helpers import Secret, SecretHeaders, http_json, wait_for


def api(airflow_url, token, method, path, body=None, headers=None):
    headers = SecretHeaders({"Authorization": Secret(f"Bearer {token}"), **(headers or {})})
    return http_json(method, f"{airflow_url}{path}", body, headers)


def wait_for_dag(airflow_url, token, dag_id, timeout=180):
    def fetch():
        status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}")
        return body if status == 200 else None

    return wait_for(fetch, timeout, interval=5, message=f"Airflow did not load the DAG {dag_id}")


def run_dag(airflow_url, token, dag_id, timeout=300):
    """Unpause a DAG, run it once and wait for the end of the run: returns the finished run."""
    status, body = api(airflow_url, token, "PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    assert status == 200, body
    status, run = api(
        airflow_url, token, "POST", f"/api/v2/dags/{dag_id}/dagRuns", {"logical_date": None}
    )
    assert status in (200, 201), run
    run_id = run["dag_run_id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, run = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        if run["state"] in ("success", "failed"):
            return run
        time.sleep(2)
    pytest.fail(f"the DAG run {run_id} of {dag_id} did not finish in {timeout} seconds")


def task_instances(airflow_url, token, run):
    path = f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}/taskInstances"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    return body["task_instances"]


def task_log(airflow_url, token, instance):
    """The events of the log of a task instance, as text."""
    path = (
        f"/api/v2/dags/{instance['dag_id']}/dagRuns/{instance['dag_run_id']}"
        f"/taskInstances/{instance['task_id']}/logs/{instance['try_number']}"
    )
    status, body = api(airflow_url, token, "GET", path, headers={"Accept": "application/json"})
    assert status == 200, body
    return [entry.get("event", "") for entry in body["content"]]
