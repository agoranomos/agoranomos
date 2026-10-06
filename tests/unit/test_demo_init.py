"""Tests of metabase/demo_init.py against stand-ins for the REST APIs of Airflow and Metabase.

The stand-ins answer as Airflow 3.3.2 and Metabase 0.63 do where demo-init depends on it, and they
answer a failure by echoing the request with its credentials: the worst case for secrets. A fake
clock runs the waits of demo-init, so a pause takes no time, and the scheduler of the Airflow
stand-in runs on the same clock.
"""

from __future__ import annotations

import ast
import copy
import json
import math
import socket
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import demo_init as di
import pytest

ENV = {
    # A quote and a backslash, which JSON escapes: the scan for secrets must find both forms.
    "AIRFLOW_ADMIN_PASSWORD": 'Airflow-"password"-1',
    "METABASE_ADMIN_EMAIL": "admin@example.com",
    "METABASE_ADMIN_PASSWORD": "Metabase-pass\\word-1",
    "WAREHOUSE_HOST": "warehouse-db",
    "WAREHOUSE_PORT": "5432",
    "WAREHOUSE_DB": "warehouse",
    "WAREHOUSE_USER": "metabase_reader",
}
# What demo-init must never print: the passwords that the tests pass in, and the tokens that the
# stand-ins hand out. The assertions name them by label.
SECRETS = {
    "the Airflow password": ENV["AIRFLOW_ADMIN_PASSWORD"],
    "the Metabase password": ENV["METABASE_ADMIN_PASSWORD"],
    "the Airflow token": "airflow-token-1",
    "the Metabase session": "metabase-session-1",
}
WAREHOUSE = {
    "id": 2,
    "name": "Agoranomos warehouse",
    "engine": "postgres",
    "details": {
        "host": "warehouse-db",
        "port": 5432,
        "dbname": "warehouse",
        "user": "metabase_reader",
        "password": "**MetabasePass**",
    },
}
SAMPLE = {"id": 1, "name": "Sample Database", "engine": "sqlite", "details": {"db": "sample"}}
FINISHED = ("success", "failed")
# The moment that 0 on the fake clock stands for, in the times that Airflow gives.
START = datetime(2026, 10, 5, 6, 0, tzinfo=UTC)


def leaked(text: str) -> list[str]:
    """The labels of the secrets that are in the text, as they are or as JSON writes them inside a
    string, where a quote and a backslash are escaped: a stand-in echoes a request as JSON."""
    return [
        label
        for label, value in SECRETS.items()
        if value in text or json.dumps(value)[1:-1] in text
    ]


def stamp(seconds: float) -> str:
    """A moment on the fake clock as Airflow writes it: in UTC, with microseconds and a Z."""
    return (START + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FakeClock:
    """The clock of the waits of demo-init: a pause moves it on at once."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeAirflow:
    """What demo-init can see of Airflow 3.3.2: two DAGs, their runs, and the asset between them.

    A scheduler runs on the fake clock, one step a second. A queued run of an unpaused DAG starts
    when no other run of its DAG is running (max_active_runs=1), and a running one ends after the
    duration of its DAG, with its result. A load that succeeds updates the asset of the raw table a
    second before its run ends: while the transform is unpaused, that queues an event for it, unless
    one is queued already, in which case it is dropped (ON CONFLICT DO NOTHING). A queued event
    becomes a run of the transform, whose run_after is the time of the event, once the transform
    has no run queued or running.

    The list of runs answers as the API does: the oldest first unless order_by says otherwise, 50
    to a page unless limit says otherwise and at most 100, and total_entries counts every run that
    matches the filters.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.url = ""
        self.loaded_at = 0.0  # when the DAG processor has loaded the DAGs
        self.paused = {di.LOAD_DAG: True, di.TRANSFORM_DAG: True}
        self.durations = {di.LOAD_DAG: 10.0, di.TRANSFORM_DAG: 30.0}
        self.results = {di.LOAD_DAG: "success", di.TRANSFORM_DAG: "success"}
        self.runs: list[dict[str, Any]] = []
        self.queued_event: float | None = None
        self.scheduled = 0.0
        self.failures: dict[tuple[str, str], int] = {}
        self.calls: list[tuple[str, str]] = []
        self.listings: list[dict[str, list[str]]] = []

    def add_run(
        self,
        dag_id: str,
        state: str,
        run_type: str = "manual",
        at: float | None = None,
        result: str | None = None,
    ) -> dict[str, Any]:
        """A run of a DAG, in a state, made at a time on the clock (now unless at says otherwise);
        result is how it ends, else as the results of its DAG say."""
        moment = self.clock.now if at is None else at
        run = {
            "id": len(self.runs) + 1,  # the order of the default listing, never shown
            "dag_run_id": f"{run_type}__{len(self.runs) + 1:04d}",
            "dag_id": dag_id,
            "run_type": run_type,
            "state": state,
            "run_after": stamp(moment),
            "start_date": None if state == "queued" else stamp(moment),
            "end_date": stamp(moment) if state in FINISHED else None,
            "ends_at": moment + self.durations[dag_id],
            "result": result,
            "emitted": False,
        }
        self.runs.append(run)
        return run

    def runs_of(self, dag_id: str) -> list[dict[str, Any]]:
        return [run for run in self.runs if run["dag_id"] == dag_id]

    def active(self, dag_id: str) -> bool:
        return any(run["state"] in ("queued", "running") for run in self.runs_of(dag_id))

    def step(self, moment: float) -> None:
        for run in self.runs:
            if run["state"] != "running":
                continue
            result = run["result"] or self.results[run["dag_id"]]
            # The task of a load updates the asset when it ends, a second before the scheduler
            # marks its run as done: as on the stand, the event is older than the end of the run.
            emits = run["dag_id"] == di.LOAD_DAG and result == "success" and not run["emitted"]
            if emits and moment >= run["ends_at"] - 1:
                run["emitted"] = True
                self.update_raw_asset(moment)
            if moment >= run["ends_at"]:
                run["state"] = result
                run["end_date"] = stamp(moment)
        transform = di.TRANSFORM_DAG
        waiting = self.queued_event is not None and not self.paused[transform]
        if waiting and not self.active(transform):
            self.add_run(transform, "queued", "asset_triggered", at=self.queued_event)
            self.queued_event = None
        for dag_id, paused in self.paused.items():
            runs = self.runs_of(dag_id)
            if paused or any(run["state"] == "running" for run in runs):
                continue
            queued = [run for run in runs if run["state"] == "queued"]
            if queued:
                queued[0].update(
                    state="running",
                    start_date=stamp(moment),
                    ends_at=moment + self.durations[dag_id],
                )

    def update_raw_asset(self, moment: float) -> None:
        if not self.paused[di.TRANSFORM_DAG] and self.queued_event is None:
            self.queued_event = moment

    def catch_up(self) -> None:
        """Run the scheduler up to the time of the clock."""
        while self.scheduled + 1 <= self.clock.now:
            self.scheduled += 1
            self.step(self.scheduled)

    def listing(self, dag_id: str, query: dict[str, list[str]]) -> tuple[int, Any]:
        self.listings.append(query)
        runs = self.runs_of(dag_id)
        if "state" in query:
            runs = [run for run in runs if run["state"] in query["state"]]
        if "run_after_gte" in query:
            since = datetime.fromisoformat(query["run_after_gte"][0])
            runs = [run for run in runs if datetime.fromisoformat(run["run_after"]) >= since]
        [order] = query.get("order_by", ["id"])
        if order.removeprefix("-") not in ("id", "run_after"):
            return 400, {"detail": f"Ordering with '{order}' is not supported by this stand-in"}
        runs.sort(key=lambda run: run[order.removeprefix("-")], reverse=order.startswith("-"))
        page = runs[: min(int(query.get("limit", ["50"])[0]), 100)]
        return 200, {"dag_runs": [public(run) for run in page], "total_entries": len(runs)}

    def handle(self, method: str, url: str, body: Any, auth: str | None) -> tuple[int, Any]:
        parts = urlsplit(url)
        path, query = parts.path, parse_qs(parts.query)
        self.calls.append((method, path))
        if (method, path) in self.failures:
            echo = {"body": body, "authorization": auth, "token": "airflow-token-1"}
            return self.failures[(method, path)], echo
        if (method, path) == ("POST", "/auth/token"):
            if body == {"username": "admin", "password": ENV["AIRFLOW_ADMIN_PASSWORD"]}:
                return 201, {"access_token": "airflow-token-1"}
            return 401, {"detail": "Invalid credentials"}
        if auth != "Bearer airflow-token-1":
            return 401, {"detail": "Not authenticated"}
        self.catch_up()
        segments = path.split("/")  # "", "api", "v2", "dags", <dag_id>, ...
        if segments[:4] != ["", "api", "v2", "dags"] or len(segments) < 5:
            return 404, {"detail": "Not Found"}
        dag_id, rest = segments[4], segments[5:]
        if dag_id not in self.paused or self.clock.now < self.loaded_at:
            return 404, {"detail": f"The Dag with ID: `{dag_id}` was not found"}
        if not rest and method == "GET":
            return 200, {"dag_id": dag_id, "is_paused": self.paused[dag_id]}
        if not rest and method == "PATCH":
            self.paused[dag_id] = body["is_paused"]
            return 200, {"dag_id": dag_id, "is_paused": self.paused[dag_id]}
        if rest == ["dagRuns"] and method == "POST":
            return 200, public(self.add_run(dag_id, "queued"))
        if rest == ["dagRuns"] and method == "GET":
            return self.listing(dag_id, query)
        if len(rest) == 2 and rest[0] == "dagRuns" and method == "GET":
            for run in self.runs_of(dag_id):
                if run["dag_run_id"] == unquote(rest[1]):
                    return 200, public(run)
        if rest == ["assets", "queuedEvents"] and method == "GET":
            waiting = dag_id == di.TRANSFORM_DAG and self.queued_event is not None
            events = [{"dag_id": dag_id, "asset_id": 1, "created_at": "..."}] if waiting else []
            return 200, {"queued_events": events, "total_entries": len(events)}
        return 404, {"detail": "Not Found"}


def public(run: dict[str, Any]) -> dict[str, Any]:
    """A run as the API shows it, without what only the stand-in keeps."""
    hidden = ("id", "ends_at", "result", "emitted")
    return {key: value for key, value in run.items() if key not in hidden}


class FakeMetabase:
    """What demo-init can see of Metabase 0.63: the warehouse, its metadata and the content.

    A sync of the schema finds the mart sync_seconds later on the fake clock. The list of
    collections holds the root collection and a personal one, and leaves out the archived ones, as
    Metabase does; so do the items of a collection.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.url = ""
        self.databases = [copy.deepcopy(SAMPLE), copy.deepcopy(WAREHOUSE)]
        self.mart_known = False
        self.sync_seconds = 10.0
        self.synced_at: float | None = None
        self.items_as_list = False
        self.collections: list[dict[str, Any]] = [
            {"id": "root", "name": "Our analytics", "can_write": True},
            {
                "id": 3,
                "name": "Agoranomos Admin's Personal Collection",
                "location": "/",
                "personal_owner_id": 1,
                "archived": False,
            },
        ]
        self.cards: dict[int, dict[str, Any]] = {}
        self.dashboards: dict[int, dict[str, Any]] = {}
        self.failures: dict[tuple[str, str], int] = {}
        self.calls: list[tuple[str, str]] = []
        self.next_id = 10

    def new_id(self) -> int:
        self.next_id += 1
        return self.next_id

    def tables(self) -> list[dict[str, Any]]:
        if self.synced_at is not None and self.clock.now >= self.synced_at:
            self.mart_known = True
        return [{"schema": "marts", "name": "ad_spend_daily"}] if self.mart_known else []

    def items(self, collection_id: int, model: str) -> Any:
        source = self.cards if model == "card" else self.dashboards
        items = [
            {"id": item_id, "name": item["name"], "model": model}
            for item_id, item in source.items()
            if item["collection_id"] == collection_id and not item.get("archived")
        ]
        return items if self.items_as_list else {"data": items, "total": len(items)}

    def handle(self, method: str, url: str, body: Any, session: str | None) -> tuple[int, Any]:
        path = urlsplit(url).path
        query = parse_qs(urlsplit(url).query)
        self.calls.append((method, path))
        for (failing_method, failing_path), status in self.failures.items():
            # A failing path that ends with a slash stands for every path under it.
            prefix = failing_path.endswith("/") and path.startswith(failing_path)
            if method == failing_method and (path == failing_path or prefix):
                echo = {"body": body, "session": session, "token": "metabase-session-1"}
                return status, echo
        if (method, path) == ("POST", "/api/session"):
            expected = {
                "username": ENV["METABASE_ADMIN_EMAIL"],
                "password": ENV["METABASE_ADMIN_PASSWORD"],
            }
            if body == expected:
                return 200, {"id": "metabase-session-1"}
            return 401, {"errors": {"password": "did not match stored password"}}
        if session != "metabase-session-1":
            return 401, "Unauthenticated"
        warehouse_id = WAREHOUSE["id"]
        if (method, path) == ("GET", "/api/database"):
            return 200, {"data": self.databases, "total": len(self.databases)}
        if (method, path) == ("GET", f"/api/database/{warehouse_id}/metadata"):
            return 200, {"id": warehouse_id, "tables": self.tables()}
        if (method, path) == ("POST", f"/api/database/{warehouse_id}/sync_schema"):
            if self.synced_at is None:
                self.synced_at = self.clock.now + self.sync_seconds
            return 200, {"status": "ok"}
        if (method, path) == ("GET", "/api/collection"):
            return 200, [c for c in self.collections if not c.get("archived")]
        if (method, path) == ("POST", "/api/collection"):
            collection = {
                "id": self.new_id(),
                "location": "/",
                "personal_owner_id": None,
                "archived": False,
                **body,
            }
            self.collections.append(collection)
            return 200, collection
        if method == "GET" and path.startswith("/api/collection/") and path.endswith("/items"):
            [model] = query["models"]
            return 200, self.items(int(path.split("/")[3]), model)
        if (method, path) == ("POST", "/api/card"):
            card_id = self.new_id()
            self.cards[card_id] = copy.deepcopy(body)
            return 200, {"id": card_id, **body}
        if (method, path) == ("POST", "/api/dashboard"):
            dashboard_id = self.new_id()
            self.dashboards[dashboard_id] = {**copy.deepcopy(body), "dashcards": []}
            return 200, {"id": dashboard_id, **body}
        if path.startswith("/api/dashboard/"):
            dashboard_id = int(path.split("/")[3])
            if dashboard_id in self.dashboards:
                dashboard = self.dashboards[dashboard_id]
                if method == "GET":
                    return 200, {"id": dashboard_id, **dashboard}
                if method == "PUT":
                    # Metabase gives each new dashcard (a negative id) an id of its own.
                    dashboard["dashcards"] = [
                        {**dashcard, "id": self.new_id()} if dashcard["id"] < 0 else dashcard
                        for dashcard in body["dashcards"]
                    ]
                    return 200, {"id": dashboard_id, **dashboard}
        return 404, "Not found"


def serve(state: Any, header: str) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            status, payload = state.handle(self.command, self.path, body, self.headers.get(header))
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = do_PATCH = serve

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    return server


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(di, "clock", fake.monotonic)
    monkeypatch.setattr(di, "pause", fake.sleep)
    return fake


@pytest.fixture
def airflow(clock):
    state = FakeAirflow(clock)
    server = serve(state, "Authorization")
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def metabase(clock):
    state = FakeMetabase(clock)
    server = serve(state, "X-Metabase-Session")
    yield state
    server.shutdown()
    server.server_close()


def env_for(airflow: FakeAirflow, metabase: FakeMetabase, **overrides: str) -> dict[str, str]:
    return {**ENV, "AIRFLOW_URL": airflow.url, "METABASE_URL": metabase.url, **overrides}


def one_line(capsys: pytest.CaptureFixture[str]) -> str:
    """The line that demo-init prints when it fails: one line on standard error, which names the
    step, with no traceback and no secret."""
    output = capsys.readouterr()
    assert output.out == ""
    assert "Traceback" not in output.err
    assert not leaked(output.err), leaked(output.err)
    [line] = output.err.splitlines()
    assert line.startswith("demo-init: ")
    return line


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_the_scan_finds_a_secret_in_the_json_of_an_echo():
    # A stand-in echoes the request as JSON, which escapes the quotes of the Airflow password: the
    # value as it is would not be found.
    echo = json.dumps({"body": {"password": ENV["AIRFLOW_ADMIN_PASSWORD"]}})
    assert ENV["AIRFLOW_ADMIN_PASSWORD"] not in echo
    assert leaked(echo) == ["the Airflow password"]


def test_a_first_start_runs_the_pipeline_and_creates_the_demo(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: False, di.TRANSFORM_DAG: False}
    assert airflow.calls.count(("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns")) == 1
    # The load starts the transform through the asset of the raw table: demo-init does not.
    assert ("POST", f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns") not in airflow.calls
    [transform] = airflow.runs_of(di.TRANSFORM_DAG)
    assert (transform["run_type"], transform["state"]) == ("asset_triggered", "success")
    assert metabase.calls.count(("POST", "/api/database/2/sync_schema")) == 1
    [collection] = [c for c in metabase.collections if c.get("name") == di.COLLECTION]
    assert (collection["location"], collection["personal_owner_id"]) == ("/", None)
    assert sorted(card["name"] for card in metabase.cards.values()) == sorted(
        question["name"] for question in di.QUESTIONS
    )
    for card in metabase.cards.values():
        assert card["collection_id"] == collection["id"]
        assert card["dataset_query"]["database"] == 2
        assert card["dataset_query"]["type"] == "native"
        assert "marts.ad_spend_daily" in card["dataset_query"]["native"]["query"]
    [dashboard] = metabase.dashboards.values()
    assert (dashboard["name"], dashboard["collection_id"]) == (di.DASHBOARD, collection["id"])
    assert sorted(dashcard["card_id"] for dashcard in dashboard["dashcards"]) == sorted(
        metabase.cards
    )
    out = capsys.readouterr()
    assert out.err == ""
    [line] = out.out.splitlines()
    assert line.startswith("demo-init: the demo pipeline ran now")
    assert not leaked(out.out + out.err)


def test_a_second_start_changes_nothing(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    calls_before = len(airflow.calls), len(metabase.calls)
    runs_before = copy.deepcopy(airflow.runs)
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    # It signs in and reads: no PATCH of the pause state, no new run, nothing new in Metabase.
    for call in airflow.calls[calls_before[0] :]:
        assert call[0] == "GET" or call == ("POST", "/auth/token"), call
    for call in metabase.calls[calls_before[1] :]:
        assert call[0] == "GET" or call == ("POST", "/api/session"), call
    assert airflow.runs == runs_before
    assert len([c for c in metabase.collections if c.get("name") == di.COLLECTION]) == 1
    assert len(metabase.cards) == len(di.QUESTIONS)
    assert len(metabase.dashboards) == 1
    assert capsys.readouterr().out.startswith("demo-init: the demo pipeline ran before")


def test_a_pause_made_after_the_first_start_stays(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    airflow.paused = {di.LOAD_DAG: True, di.TRANSFORM_DAG: True}  # paused in the Airflow UI
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True}


def test_a_success_long_ago_counts_whatever_ran_since(airflow, metabase):
    # Airflow keeps every run, also after ./agoranomos down. Here the one success is the oldest of
    # 121 runs of the transform, so a page of the newest runs would not show it: the count of the
    # successful runs does.
    airflow.add_run(di.TRANSFORM_DAG, "success", at=-10_000)
    for n in range(120):
        airflow.add_run(di.TRANSFORM_DAG, "failed", at=-9_000 + n)
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True}
    assert [call for call in airflow.calls if call[0] in ("POST", "PATCH")] == [
        ("POST", "/auth/token")
    ]


def test_the_newest_of_many_loads_is_awaited_and_not_started_again(airflow, metabase):
    # 120 loads that ended long ago, and the newest is still running: the oldest 50, which the API
    # gives unless it is told otherwise, would show only the old ones.
    for n in range(120):
        airflow.add_run(di.LOAD_DAG, "failed", at=-9_000 + n)
    running = airflow.add_run(di.LOAD_DAG, "running")
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls
    assert running["state"] == "success"
    [transform] = airflow.runs_of(di.TRANSFORM_DAG)
    assert transform["state"] == "success"


def test_every_listing_of_runs_names_its_order_and_its_limit(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.listings
    for query in airflow.listings:
        assert query.get("order_by") == ["-run_after"], query
        assert len(query.get("limit", [])) == 1, query


def test_the_load_starts_only_once_the_transform_is_idle(airflow, metabase):
    # An earlier start stopped half-way: a run of the transform is running, and an event waits for
    # the next one. With max_active_runs=1, the event of a load that ended now would be dropped and
    # would start no run, so demo-init waits until the transform is idle before it loads.
    airflow.paused = {di.LOAD_DAG: False, di.TRANSFORM_DAG: False}
    airflow.add_run(di.TRANSFORM_DAG, "running", at=-10, result="failed")
    airflow.queued_event = -5.0
    assert di.main(env_for(airflow, metabase)) == 0
    [load] = airflow.runs_of(di.LOAD_DAG)
    earlier = [run for run in airflow.runs_of(di.TRANSFORM_DAG) if run["run_after"] < stamp(0)]
    assert len(earlier) == 2  # the one that ran, and the one that the waiting event started
    assert load["run_after"] >= max(run["end_date"] for run in earlier)
    [after_the_load] = [
        run for run in airflow.runs_of(di.TRANSFORM_DAG) if run["run_after"] >= load["start_date"]
    ]
    assert (after_the_load["run_type"], after_the_load["state"]) == ("asset_triggered", "success")


def test_a_load_that_is_running_is_awaited_and_not_started_again(airflow, metabase):
    airflow.add_run(di.LOAD_DAG, "running")
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls


def test_a_failed_load_stops_with_the_dag_named(airflow, metabase, capsys):
    airflow.results[di.LOAD_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"the DAG {di.LOAD_DAG} failed" in line
    assert metabase.calls == []


def test_a_failed_transform_names_the_transform_dag_and_dbt(airflow, metabase, capsys):
    airflow.results[di.TRANSFORM_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"the DAG {di.TRANSFORM_DAG} failed after the demo load" in line
    assert "dbt test" in line
    assert metabase.calls == []


# A step that runs out of time: how to make it hang, and what its line says.
HANGING_STEPS = [
    pytest.param(
        lambda airflow, metabase: setattr(airflow, "loaded_at", math.inf),
        f"Airflow did not load the DAG {di.LOAD_DAG}",
        "./agoranomos logs airflow-dag-processor",
        id="dags",
    ),
    pytest.param(
        lambda airflow, metabase: airflow.add_run(
            di.TRANSFORM_DAG, "running", at=0, result="failed"
        ).update(ends_at=math.inf),
        f"the DAG {di.TRANSFORM_DAG} did not finish its earlier runs",
        "see it in Airflow",
        id="transform-busy",
    ),
    pytest.param(
        lambda airflow, metabase: airflow.durations.update({di.LOAD_DAG: math.inf}),
        f"the DAG {di.LOAD_DAG} did not finish",
        "see it in Airflow",
        id="load",
    ),
    pytest.param(
        lambda airflow, metabase: airflow.durations.update({di.TRANSFORM_DAG: math.inf}),
        f"the DAG {di.TRANSFORM_DAG} did not succeed after the demo load",
        "see it in Airflow",
        id="transform",
    ),
    pytest.param(
        lambda airflow, metabase: setattr(metabase, "sync_seconds", math.inf),
        "Metabase did not find marts.ad_spend_daily after a sync of the warehouse",
        "./agoranomos logs metabase",
        id="sync",
    ),
]


@pytest.mark.parametrize(("hang", "step", "hint"), HANGING_STEPS)
def test_a_step_that_runs_out_of_time_names_itself(
    airflow, metabase, clock, capsys, hang, step, hint
):
    hang(airflow, metabase)
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert step in line
    assert f"within {di.DEADLINE:g} seconds of the start of demo-init" in line
    assert hint in line
    # It gives up at the deadline, and not before: the waits share it.
    assert di.DEADLINE <= clock.now < di.DEADLINE + di.POLL_INTERVAL


def test_the_whole_job_has_one_deadline(airflow, metabase, clock, capsys):
    # The DAGs take 595 of the 600 seconds to load, and a load takes 10: with one deadline for the
    # whole job, the load runs out of time, five seconds after it starts.
    airflow.loaded_at = 595
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"the DAG {di.LOAD_DAG} did not finish" in line
    assert clock.now == di.DEADLINE
    [load] = airflow.runs_of(di.LOAD_DAG)
    assert load["state"] == "running"
    assert metabase.calls == []


def test_a_refusal_while_waiting_for_the_dags_stops_at_once(airflow, metabase, clock, capsys):
    # A missing DAG (404) is waited for; any other refusal is not.
    airflow.failures[("GET", f"/api/v2/dags/{di.LOAD_DAG}")] = 403
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"GET /api/v2/dags/{di.LOAD_DAG} failed with HTTP 403" in line
    assert clock.now == 0


def test_a_wrong_airflow_password_names_the_setting(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase, AIRFLOW_ADMIN_PASSWORD="not-the-password")) == 1
    line = one_line(capsys)
    assert "AIRFLOW_ADMIN_PASSWORD" in line
    assert "not-the-password" not in line


def test_a_wrong_metabase_password_names_the_settings(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase, METABASE_ADMIN_PASSWORD="not-the-password")) == 1
    line = one_line(capsys)
    assert "AGORANOMOS_ADMIN_EMAIL" in line
    assert "METABASE_ADMIN_PASSWORD" in line
    assert "not-the-password" not in line


def test_an_airflow_that_cannot_be_reached_is_named_with_the_class_of_the_error(metabase, capsys):
    env = {**ENV, "AIRFLOW_URL": f"http://127.0.0.1:{free_port()}", "METABASE_URL": metabase.url}
    assert di.main(env) == 1
    line = one_line(capsys)
    assert "POST /auth/token failed: URLError (ConnectionRefusedError)" in line
    assert "./agoranomos logs airflow-apiserver" in line


@pytest.mark.parametrize(
    ("side", "call"),
    [
        # The answer echoes the request, with the password of the admin of Airflow in it.
        ("airflow", ("POST", "/auth/token")),
        ("airflow", ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns")),
        ("airflow", ("PATCH", f"/api/v2/dags/{di.LOAD_DAG}")),
        ("airflow", ("GET", f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns")),
        ("airflow", ("GET", f"/api/v2/dags/{di.TRANSFORM_DAG}/assets/queuedEvents")),
        ("metabase", ("POST", "/api/session")),
        ("metabase", ("POST", "/api/database/2/sync_schema")),
        ("metabase", ("POST", "/api/card")),
        ("metabase", ("PUT", "/api/dashboard/")),
        ("metabase", ("POST", "/api/collection")),
    ],
)
def test_a_failed_call_names_the_step_and_prints_no_secret(airflow, metabase, capsys, side, call):
    # The stand-ins answer a failure by echoing the request with its credentials, the worst case.
    (airflow if side == "airflow" else metabase).failures[call] = 500
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"{call[0]} {call[1]}" in line
    assert "HTTP 500" in line


@pytest.mark.parametrize(
    ("side", "call", "server"),
    [
        ("airflow", ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns"), "Airflow"),
        ("airflow", ("GET", f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns"), "Airflow"),
        ("metabase", ("POST", "/api/session"), "Metabase"),
        ("metabase", ("POST", "/api/card"), "Metabase"),
    ],
)
def test_an_answer_of_an_unexpected_shape_ends_in_one_line(
    airflow, metabase, capsys, side, call, server
):
    # A success status with an echo of the request, credentials included: the job looks for what
    # is not there. The line names the class of the error and where to look, never the answer.
    (airflow if side == "airflow" else metabase).failures[call] = 200
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"an answer of {server} had an unexpected shape (KeyError)" in line
    hint = "airflow-apiserver" if server == "Airflow" else "metabase"
    assert line.endswith(f"See ./agoranomos logs {hint}.")


def test_a_renamed_warehouse_connection_is_found_by_its_address(airflow, metabase):
    metabase.databases[1]["name"] = "Marketing data"  # renamed in the Metabase UI
    assert di.main(env_for(airflow, metabase)) == 0
    assert metabase.calls.count(("POST", "/api/database/2/sync_schema")) == 1
    assert {card["dataset_query"]["database"] for card in metabase.cards.values()} == {2}


def test_without_a_warehouse_connection_the_line_points_at_metabase_init(airflow, metabase, capsys):
    metabase.databases = [copy.deepcopy(SAMPLE)]
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert "./agoranomos logs metabase-init" in line


def test_a_mart_that_metabase_knows_is_not_synced_again(airflow, metabase):
    metabase.mart_known = True
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls


def test_a_collection_of_the_same_name_elsewhere_is_not_the_demo_collection(airflow, metabase):
    # One in a personal collection, one inside another collection, and one in the trash.
    others = [
        {"id": 5, "name": di.COLLECTION, "location": "/3/", "personal_owner_id": None},
        {"id": 6, "name": di.COLLECTION, "location": "/", "personal_owner_id": 1},
        {"id": 7, "name": di.COLLECTION, "location": "/", "archived": True},
    ]
    metabase.collections += others
    assert di.main(env_for(airflow, metabase)) == 0
    [created] = [
        c for c in metabase.collections if c.get("name") == di.COLLECTION and c not in others
    ]
    assert {card["collection_id"] for card in metabase.cards.values()} == {created["id"]}


def test_items_that_come_as_a_bare_list_are_understood(airflow, metabase):
    metabase.items_as_list = True
    assert di.main(env_for(airflow, metabase)) == 0
    assert di.main(env_for(airflow, metabase)) == 0
    assert len(metabase.cards) == len(di.QUESTIONS)
    assert len(metabase.dashboards) == 1


def test_an_empty_dashboard_gets_its_questions(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    [dashboard] = metabase.dashboards.values()
    dashboard["dashcards"] = []  # as after a crash between the two requests
    assert di.main(env_for(airflow, metabase)) == 0
    assert len(dashboard["dashcards"]) == len(di.QUESTIONS)
    assert len(metabase.cards) == len(di.QUESTIONS)


def test_a_deleted_question_comes_back_and_the_rest_stays(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    [dashboard] = metabase.dashboards.values()
    placed = list(dashboard["dashcards"])
    deleted = next(iter(metabase.cards))
    metabase.cards[deleted]["archived"] = True  # moved to the trash
    assert di.main(env_for(airflow, metabase)) == 0
    assert len([c for c in metabase.cards.values() if not c.get("archived")]) == len(di.QUESTIONS)
    assert dashboard["dashcards"] == placed  # a dashboard with questions is left as it is


def test_an_edited_question_keeps_its_edit(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    card_id, card = next(iter(metabase.cards.items()))
    card["description"] = "Changed in Metabase"
    card["dataset_query"]["native"]["query"] = "SELECT 1 AS spend"
    assert di.main(env_for(airflow, metabase)) == 0
    assert metabase.cards[card_id]["description"] == "Changed in Metabase"
    assert metabase.cards[card_id]["dataset_query"]["native"]["query"] == "SELECT 1 AS spend"
    assert len(metabase.cards) == len(di.QUESTIONS)


def test_a_renamed_question_is_made_again_and_the_renamed_one_stays(airflow, metabase):
    # demo-init finds its questions by name, so a renamed one counts as missing.
    assert di.main(env_for(airflow, metabase)) == 0
    card_id, card = next(iter(metabase.cards.items()))
    original = card["name"]
    card["name"] = "My spend"
    assert di.main(env_for(airflow, metabase)) == 0
    names = [c["name"] for c in metabase.cards.values()]
    assert names.count(original) == 1
    assert metabase.cards[card_id]["name"] == "My spend"
    assert len(names) == len(di.QUESTIONS) + 1


def test_missing_settings_are_named():
    with pytest.raises(di.SetupError, match="AIRFLOW_URL"):
        di.Config.from_env({**ENV, "METABASE_URL": "http://metabase:3000"})


def test_the_urls_must_be_http():
    env = {**ENV, "AIRFLOW_URL": "file:///etc/passwd", "METABASE_URL": "http://metabase:3000"}
    with pytest.raises(di.SetupError, match="AIRFLOW_URL"):
        di.Config.from_env(env)


@pytest.mark.parametrize("port", ["five", "5432.0", "0", "65536"])
def test_a_warehouse_port_that_is_not_a_port_number_is_named(port):
    # The rule of metabase-init: one rule for every job.
    env = {
        **ENV,
        "AIRFLOW_URL": "http://airflow-apiserver:8080",
        "METABASE_URL": "http://metabase:3000",
        "WAREHOUSE_PORT": port,
    }
    with pytest.raises(di.SetupError, match="WAREHOUSE_PORT must be a port number"):
        di.Config.from_env(env)


def test_the_script_runs_as_compose_runs_it():
    # compose.yaml runs the file with the python of a plain image, from the folder of metabase_init.
    script = Path(di.__file__)
    result = subprocess.run(
        [sys.executable, str(script)], env={}, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 1
    assert "missing environment variables" in result.stderr
    assert "Traceback" not in result.stderr


def test_the_module_imports_only_the_standard_library_and_metabase_init():
    tree = ast.parse(Path(di.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    allowed = set(sys.stdlib_module_names) | {"metabase_init"}
    assert imported <= allowed, sorted(imported - allowed)
