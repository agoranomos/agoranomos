"""Calls to the REST API of Airflow, for the stack tests. The token fixture of conftest.py signs
them in; the token is a Secret, so a failed test does not print it."""

import json
import time
from urllib.parse import quote

import pytest
from stack_helpers import LogEntries, Secret, SecretHeaders, http_json, wait_for

FINISHED = ("success", "failed")
# The most items that the API of Airflow gives on one page ([api] maximum_page_limit, 100 by
# default in Airflow 3.3.2).
PER_PAGE = 100


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


def pause(airflow_url, token, dag_id):
    status, body = api(airflow_url, token, "PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": True})
    assert status == 200, body


def is_paused(airflow_url, token, dag_id):
    status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}")
    assert status == 200, body
    return body["is_paused"]


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


def run_count(airflow_url, token, dag_id, states=(), run_types=()):
    """How many runs a DAG has in the states and of the run types (any, when none are given):
    total_entries, which counts them all. Airflow keeps every run, also after ./agoranomos down,
    so the runs can fill a page, and its length would count no more than the page holds."""
    query = "&".join(
        [
            *(f"state={state}" for state in states),
            *(f"run_type={run_type}" for run_type in run_types),
            "order_by=-run_after",
            "limit=1",
        ]
    )
    status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}/dagRuns?{query}")
    assert status == 200, body
    return body["total_entries"]


def is_busy(airflow_url, token, dag_id, assets=1):
    """Whether a DAG has a run queued or running, or a run that Airflow is about to create: an
    event queued for each of the assets of its schedule, as many as assets says. The events of some
    of its assets, as a history that failed half way leaves them, start no run until the others
    come, so they do not count. The list of queued events has no pages: it holds one event at most
    for each asset."""
    if run_count(airflow_url, token, dag_id, states=("queued", "running")):
        return True
    status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}/assets/queuedEvents")
    assert status == 200, body
    return body["total_entries"] >= assets


def downstream_task_ids(airflow_url, token, dag_id):
    """The tasks of the latest version of a DAG, each with the sorted ids of the tasks that run
    after it: the order of the tasks, as the DAG declares it."""
    status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}/tasks")
    assert status == 200, body
    return {task["task_id"]: sorted(task["downstream_task_ids"] or []) for task in body["tasks"]}


def task_instances(airflow_url, token, run):
    """Every task instance of a run. The API gives 50 to a page unless it is told otherwise, and
    the transform has more tasks than that: so the helper asks for pages as large as the API allows
    and reads them until it has as many instances as total_entries counts, each once."""
    path = f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}/taskInstances"
    found = {}
    while True:
        status, body = api(
            airflow_url, token, "GET", f"{path}?limit={PER_PAGE}&offset={len(found)}"
        )
        assert status == 200, body
        page = body["task_instances"]
        found.update(((i["task_id"], i.get("map_index", -1)), i) for i in page)
        if not page or len(found) >= body["total_entries"]:
            break
    # pytest rewrites the assertions of test modules only, so this one carries its own message.
    assert len(found) == body["total_entries"], (
        f"the run {run['dag_run_id']} has {body['total_entries']} task instances, and the pages "
        f"gave {len(found)} different ones"
    )
    return list(found.values())


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

    The API lists only the active assets unless it is told otherwise. An asset that a task adds to
    an alias as it runs, as Cosmos does, becomes active only at the next check of the scheduler,
    up to [scheduler] parsing_cleanup_interval (60 seconds by default) after its first event. So
    the helper asks for every asset, and fails unless exactly one has the URI.
    """
    path = f"/api/v2/assets?uri_pattern={quote(uri)}&only_active=false"
    status, body = api(airflow_url, token, "GET", path)
    assert status == 200, body
    matching = [asset["id"] for asset in body["assets"] if asset["uri"] == uri]
    assert len(matching) == 1, f"Airflow has {len(matching)} assets with the URI {uri}"
    query = "&".join(
        f"{key}={quote(str(value))}"
        for key, value in {
            "asset_id": matching[0],
            **filters,
            "order_by": "-timestamp",
            "limit": PER_PAGE,
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


def variable_names(airflow_url, token, prefix):
    """The names of the Airflow Variables whose names start with prefix, sorted: names, never
    values, as a Variable can hold a secret.

    The API matches the prefix by a range of keys, after it strips the characters at its end that
    are not letters or digits, so cosmos_cache__ also matches cosmos_cache_other: the names are
    checked again here. Like asset_events, the helper fails when total_entries says that there are
    more Variables than one page holds. The answer, with the values, is dropped before any check.
    """
    query = f"variable_key_prefix_pattern={quote(prefix)}&order_by=key&limit={PER_PAGE}"
    status, body = api(airflow_url, token, "GET", f"/api/v2/variables?{query}")
    names, total = (
        ([item["key"] for item in body["variables"]], body["total_entries"])
        if status == 200
        else (None, None)
    )
    del body
    # pytest rewrites the assertions of test modules only, so these carry their own messages.
    assert status == 200, f"GET /api/v2/variables answered HTTP {status}"
    assert total <= len(names), (
        f"Airflow has {total} Variables that start with {prefix!r}, and one page holds {len(names)}"
    )
    return sorted(name for name in names if name.startswith(prefix))
