"""Tests of metabase/demo_init.py against stand-ins for the REST APIs of Airflow and Metabase.

The stand-ins answer as Airflow 3.3.2 and Metabase 0.63 and 0.64 do where demo-init depends on it,
and they answer a failure by echoing the request with its credentials: the worst case for secrets.
A fake clock runs the waits of demo-init, so a pause takes no time, and the scheduler of the Airflow
stand-in runs on the same clock.
"""

from __future__ import annotations

import ast
import copy
import json
import math
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import demo_init as di
import metadata as md
import pytest
from metabase_stand_in import (
    EXPORT,
    SAMPLE,
    FakeClock,
    FakeMetabase,
    free_port,
    metadata_env,
    serve,
)
from metabase_stand_in import leaked as leaked_in

ENV = {
    # A quote and a backslash, which JSON escapes: the scan for secrets must find both forms.
    "AIRFLOW_ADMIN_PASSWORD": 'Airflow-"password"-1',
    "METABASE_ADMIN_EMAIL": "admin@example.com",
    "METABASE_ADMIN_PASSWORD": "Metabase-pass\\word-1",
    "WAREHOUSE_HOST": "warehouse-db",
    "WAREHOUSE_PORT": "5432",
    "WAREHOUSE_DB": "warehouse",
    "WAREHOUSE_USER": "metabase_reader",
    # The files of the metadata: env_for points these at files that the fixture writes.
    "METADATA_EXPORT": "/nonexistent/metadata.json",
    "METADATA_OVERRIDES": "/nonexistent/overrides.toml",
}
# What demo-init must never print: the passwords that the tests pass in, and the tokens that the
# stand-ins hand out. The assertions name them by label.
SECRETS = {
    "the Airflow password": ENV["AIRFLOW_ADMIN_PASSWORD"],
    "the Metabase password": ENV["METABASE_ADMIN_PASSWORD"],
    "the Airflow token": "airflow-token-1",
    "the Metabase session": "session-from-login",
}
FINISHED = ("success", "failed")
# The moment that 0 on the fake clock stands for, in the times that Airflow gives.
START = datetime(2026, 10, 5, 6, 0, tzinfo=UTC)


def leaked(text: str) -> list[str]:
    return leaked_in(text, SECRETS)


def stamp(seconds: float) -> str:
    """A moment on the fake clock as Airflow writes it: in UTC, with microseconds and a Z."""
    return (START + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FakeAirflow:
    """What demo-init can see of Airflow 3.3.2: the three DAGs of the demo, their runs, and the
    asset between the load and the transform. The history is a DAG like any other here: its runs
    update no asset that schedules a DAG.

    A scheduler runs on the fake clock, one loop a second, in the order of the loop of Airflow:
    first a run for an event that waits, then the queued runs start, then the running ones end.
    A queued run of an unpaused DAG starts when no other run of its DAG is running
    (max_active_runs=1), and a running one ends after the duration of its DAG, with its result.
    The task of a load that succeeds updates the asset of the raw table a loop before its run
    ends: while the transform is unpaused, that queues an event for it, unless one is queued
    already, in which case it is dropped (ON CONFLICT DO NOTHING). The event becomes a run of the
    transform, whose run_after is the time of the event, on a later loop, once the transform has
    no run queued or running. triggers are the runs that a person triggers in the Airflow UI, at a
    time.

    The list of runs answers as the API does: the oldest first unless order_by says otherwise, 50
    to a page unless limit says otherwise and at most 100, and total_entries counts every run that
    matches the filters.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.url = ""
        self.loaded_at = 0.0  # when the DAG processor has loaded the DAGs
        self.paused = {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.HISTORY_DAG: True}
        self.durations = {di.LOAD_DAG: 10.0, di.TRANSFORM_DAG: 30.0, di.HISTORY_DAG: 60.0}
        self.results = {
            di.LOAD_DAG: "success",
            di.TRANSFORM_DAG: "success",
            di.HISTORY_DAG: "success",
        }
        self.runs: list[dict[str, Any]] = []
        self.queued_event: float | None = None
        self.triggers: list[tuple[float, str, str]] = []  # (time, DAG, result)
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
        """One loop of the scheduler. In Airflow, the end of a task writes the queued row of its
        event, and a later loop makes the run: a loop makes the runs for the events that wait,
        then starts the queued runs, then updates the running ones."""
        for at, dag_id, result in self.triggers:
            if at == moment:
                self.add_run(dag_id, "queued", at=moment, result=result)
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
        for run in self.runs:
            if run["state"] != "running":
                continue
            result = run["result"] or self.results[run["dag_id"]]
            # The task of a load updates the asset when it ends, a loop before the scheduler marks
            # its run as done: as on a running stack, the event is older than the end of the run.
            emits = run["dag_id"] == di.LOAD_DAG and result == "success" and not run["emitted"]
            if emits and moment >= run["ends_at"] - 1:
                run["emitted"] = True
                self.update_raw_asset(moment)
            if moment >= run["ends_at"]:
                run["state"] = result
                run["end_date"] = stamp(moment)

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
def metabase(clock, tmp_path):
    # A Metabase that metabase-init has set up, and a warehouse in which the demo pipeline has
    # built the mart, which Metabase knows after a sync.
    state = FakeMetabase(clock)
    state.set_up(ENV["METABASE_ADMIN_EMAIL"], ENV["METABASE_ADMIN_PASSWORD"])
    state.mart_in_warehouse = True
    state.files = metadata_env(tmp_path)
    server = serve(state, "X-Metabase-Session")
    yield state
    server.shutdown()
    server.server_close()


def env_for(airflow: FakeAirflow, metabase: FakeMetabase, **overrides: str) -> dict[str, str]:
    return {
        **ENV,
        "AIRFLOW_URL": airflow.url,
        "METABASE_URL": metabase.url,
        **metabase.files,
        **overrides,
    }


# The lines that demo-init prints for the metadata, each as soon as its write is done.
METADATA_LINES = tuple(f"demo-init: {kind}: " for kind in (md.FIRST, md.UPDATE, md.REVERT))


# The calls that demo-init makes after it applied the metadata of the marts: a failure of one of
# them comes after the lines of the changes that apply() made.
AFTER_APPLY = {
    ("PUT", "/api/field/"),
    ("POST", "/api/card"),
    ("PUT", "/api/dashboard/"),
    ("POST", "/api/collection"),
}


def one_line(capsys: pytest.CaptureFixture[str], *, after_apply: bool = False) -> str:
    """The line that demo-init prints when it fails: one line on standard error, which names the
    step, with no traceback and no secret. Standard output is empty, or, for a failure after the
    metadata was applied, holds no more than the lines of the changes that it made."""
    output = capsys.readouterr()
    if after_apply:
        assert all(line.startswith(METADATA_LINES) for line in output.out.splitlines()), output.out
    else:
        assert output.out == "", output.out
    assert "Traceback" not in output.err
    assert not leaked(output.out + output.err), leaked(output.out + output.err)
    [line] = output.err.splitlines()
    assert line.startswith("demo-init: ")
    return line


def test_the_scan_finds_a_secret_in_the_json_of_an_echo():
    # A stand-in echoes the request as JSON, which escapes the quotes of the Airflow password: the
    # value as it is would not be found.
    echo = json.dumps({"body": {"password": ENV["AIRFLOW_ADMIN_PASSWORD"]}})
    assert ENV["AIRFLOW_ADMIN_PASSWORD"] not in echo
    assert leaked(echo) == ["the Airflow password"]


def test_a_first_start_runs_the_pipeline_and_creates_the_demo(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: False, di.TRANSFORM_DAG: False, di.HISTORY_DAG: False}
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
    *metadata_lines, line = out.out.splitlines()
    assert line.startswith("demo-init: the demo pipeline ran now")
    # The metadata of the mart, applied after the sync: one line for each change.
    assert "demo-init: first fill: marts.ad_spend_daily display_name: " in metadata_lines[0]
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
    assert capsys.readouterr().out.startswith(
        "demo-init: the demo pipeline ran before, the history of the demo ran before, and the "
        "dashboard "
    )


def test_a_pause_made_after_the_first_start_stays(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    airflow.paused = dict.fromkeys(airflow.paused, True)  # paused in the Airflow UI
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.HISTORY_DAG: True}


def test_a_success_long_ago_counts_whatever_ran_since(airflow, metabase):
    # Airflow keeps every run, also after ./agoranomos down. Here the one success is the oldest of
    # 121 runs of the transform, and of the history, so a page of the newest runs would not show
    # it: the count of the successful runs does.
    for dag_id in (di.TRANSFORM_DAG, di.HISTORY_DAG):
        airflow.add_run(dag_id, "success", at=-10_000)
        for n in range(120):
            airflow.add_run(dag_id, "failed", at=-9_000 + n)
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.HISTORY_DAG: True}
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
    airflow.paused.update({di.LOAD_DAG: False, di.TRANSFORM_DAG: False})
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


def test_an_event_that_waits_makes_the_transform_busy(airflow, metabase):
    # An event of the raw asset waits, and the scheduler has not made its run yet: no run is
    # queued or running. The transform is busy all the same, and the load waits until the run of
    # that event has ended, since its own event would find the waiting one and be dropped.
    airflow.paused.update({di.LOAD_DAG: False, di.TRANSFORM_DAG: False})
    airflow.queued_event = 0.0
    client = di.Airflow(airflow.url)
    di.sign_in_to_airflow(client, ENV["AIRFLOW_ADMIN_PASSWORD"])
    assert di.count_runs(client, di.TRANSFORM_DAG, "queued", "running") == 0
    assert di.transform_is_busy(client) is True
    assert di.main(env_for(airflow, metabase)) == 0
    [waiting] = [run for run in airflow.runs_of(di.TRANSFORM_DAG) if run["run_after"] == stamp(0)]
    [load] = airflow.runs_of(di.LOAD_DAG)
    assert load["run_after"] >= waiting["end_date"]


def test_a_failed_run_of_a_person_does_not_stop_the_job_while_the_run_of_the_load_waits(
    airflow, metabase, capsys, monkeypatch
):
    # A person triggers the transform in the Airflow UI while the load runs, and that run fails,
    # with a run_after after the start of the load. The event of the load waits for that run to
    # end and becomes a run a loop later. The failed run ends at 35 s, when demo-init looks: the
    # event still waits then, so the transform is busy, and the job waits for the run of the load.
    # The look at 35 s needs a poll every 5 s: with another interval the job would never look in
    # that one loop, and the test would pass without the waiting event counting as busy.
    monkeypatch.setattr(di, "POLL_INTERVAL", 5.0)
    airflow.triggers = [(5.0, di.TRANSFORM_DAG, "failed")]
    assert di.main(env_for(airflow, metabase)) == 0
    [load] = airflow.runs_of(di.LOAD_DAG)
    person, of_the_load = sorted(airflow.runs_of(di.TRANSFORM_DAG), key=lambda run: run["id"])
    assert (person["run_type"], person["state"]) == ("manual", "failed")
    assert person["run_after"] > load["start_date"]
    assert person["end_date"] == stamp(35)
    assert (of_the_load["run_type"], of_the_load["state"]) == ("asset_triggered", "success")
    assert "the demo pipeline ran now" in capsys.readouterr().out


def test_a_run_of_the_load_that_succeeds_while_the_job_looks_at_a_failed_one_counts(
    airflow, metabase, capsys
):
    # A failed run of a person, as above, and the run of the event of the load ends with success
    # between the count of the successes and the look at whether the transform is busy: the
    # transform is idle then, and the run that the job waited for has succeeded.
    airflow.triggers = [(5.0, di.TRANSFORM_DAG, "failed")]
    handle = airflow.handle
    ended = []

    def racing(method, url, body, auth):
        parts = urlsplit(url)
        busy_check = parse_qs(parts.query).get("state") == ["queued", "running"]
        if busy_check and parts.path == f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns":
            airflow.catch_up()
            for run in airflow.runs_of(di.TRANSFORM_DAG):
                if (run["run_type"], run["state"]) == ("asset_triggered", "running"):
                    run.update(state="success", end_date=stamp(airflow.clock.now))
                    ended.append(run)
        return handle(method, url, body, auth)

    airflow.handle = racing
    assert di.main(env_for(airflow, metabase)) == 0
    assert ended, "the run of the load never ended between the two looks"
    assert "the demo pipeline ran now" in capsys.readouterr().out


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


def test_a_first_start_runs_the_history_once(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    history = airflow.runs_of(di.HISTORY_DAG)
    assert [run["state"] for run in history] == ["success"]
    assert not airflow.paused[di.HISTORY_DAG]
    out = capsys.readouterr()
    assert out.err == ""
    assert out.out.splitlines()[-1].startswith(
        "demo-init: the demo pipeline ran now, the history of the demo ran now, and the dashboard "
    )


def test_a_history_that_succeeded_before_is_not_run_again(airflow, metabase):
    airflow.add_run(di.HISTORY_DAG, "success", at=-3600)
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3500)
    assert di.main(env_for(airflow, metabase)) == 0
    assert [run["state"] for run in airflow.runs_of(di.HISTORY_DAG)] == ["success"]
    assert airflow.paused[di.HISTORY_DAG]  # a start that finds a success leaves the pause as it is


def test_a_stack_whose_pipeline_ran_before_runs_the_history_on_its_next_start(
    airflow, metabase, capsys
):
    # A stack of v0.1, whose transform succeeded: its first start with the history runs the
    # history and nothing else in Airflow, and the line says which of the two ran now.
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3500)
    assert di.main(env_for(airflow, metabase)) == 0
    assert [run["state"] for run in airflow.runs_of(di.HISTORY_DAG)] == ["success"]
    assert airflow.runs_of(di.LOAD_DAG) == []
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.HISTORY_DAG: False}
    assert (
        capsys.readouterr()
        .out.splitlines()[-1]
        .startswith(
            "demo-init: the demo pipeline ran before, the history of the demo ran now, and the "
            "dashboard "
        )
    )


def test_a_failed_history_stops_with_the_dag_named(airflow, metabase, capsys):
    airflow.results[di.HISTORY_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    assert one_line(capsys) == f"demo-init: the DAG {di.HISTORY_DAG} failed: see it in Airflow"


def test_a_history_that_is_running_is_awaited_and_not_started_again(airflow, metabase):
    airflow.paused[di.HISTORY_DAG] = False
    airflow.add_run(di.HISTORY_DAG, "running")
    assert di.main(env_for(airflow, metabase)) == 0
    assert len(airflow.runs_of(di.HISTORY_DAG)) == 1
    assert ("POST", f"/api/v2/dags/{di.HISTORY_DAG}/dagRuns") not in airflow.calls


def test_a_history_that_runs_now_leaves_a_missing_mart_to_the_pipeline_that_ran_before(
    airflow, metabase, capsys
):
    # A stack of v0.1, whose transform succeeded, gets the history on its first start with it.
    # When its warehouse has lost the mart since, the line points at the load, as after any reset
    # of the warehouse volume: the transform did not run now.
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3500)
    metabase.mart_in_warehouse = False
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line.startswith(
        "demo-init: Metabase finds no marts.ad_spend_daily in the warehouse: the demo pipeline "
        "ran before"
    )
    assert [run["state"] for run in airflow.runs_of(di.HISTORY_DAG)] == ["success"]


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
        # The file of the history does not load, as with an import error: Airflow answers 404.
        lambda airflow, metabase: airflow.paused.pop(di.HISTORY_DAG),
        f"Airflow did not load the DAG {di.HISTORY_DAG}",
        "./agoranomos logs airflow-dag-processor",
        id="history-dag",
    ),
    pytest.param(
        lambda airflow, metabase: airflow.durations.update({di.HISTORY_DAG: math.inf}),
        f"the DAG {di.HISTORY_DAG} did not finish",
        "see it in Airflow",
        id="history",
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
    env = {
        **ENV,
        "AIRFLOW_URL": f"http://127.0.0.1:{free_port()}",
        "METABASE_URL": metabase.url,
        **metabase.files,
    }
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
        ("metabase", ("PUT", "/api/field/")),
        ("metabase", ("POST", "/api/card")),
        ("metabase", ("PUT", "/api/dashboard/")),
        ("metabase", ("POST", "/api/collection")),
    ],
)
def test_a_failed_call_names_the_step_and_prints_no_secret(airflow, metabase, capsys, side, call):
    # The stand-ins answer a failure by echoing the request with its credentials, the worst case.
    (airflow if side == "airflow" else metabase).failures[call] = 500
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys, after_apply=call in AFTER_APPLY)
    assert f"{call[0]} {call[1]}" in line
    assert "HTTP 500" in line


@pytest.mark.parametrize(
    ("side", "call", "server", "function"),
    [
        (
            "airflow",
            ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns"),
            "Airflow",
            "run_demo_pipeline",
        ),
        ("airflow", ("GET", f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns"), "Airflow", "count_runs"),
        ("metabase", ("POST", "/api/session"), "Metabase", "sign_in"),
        ("metabase", ("POST", "/api/card"), "Metabase", "ensure_questions"),
    ],
)
def test_an_answer_of_an_unexpected_shape_ends_in_one_line(
    airflow, metabase, capsys, side, call, server, function
):
    # A success status with an echo of the request, credentials included: the job looks for what
    # is not there. The line names the class of the error, the function that read the answer and
    # where to look, never the answer.
    (airflow if side == "airflow" else metabase).failures[call] = 200
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys, after_apply=call in AFTER_APPLY)
    assert f"an answer of {server} had an unexpected shape (KeyError in {function})" in line
    hint = "airflow-apiserver" if server == "Airflow" else "metabase"
    assert line.endswith(f"See ./agoranomos logs {hint}.")


def test_a_page_with_a_success_status_ends_in_one_line(airflow, metabase, capsys):
    # A proxy in front of Airflow can answer with a page of its own and a success status: the job
    # gets a text where it reads an object, a TypeError, and stops with one line.
    handle = airflow.handle

    def page(method, url, body, auth):
        if (method, urlsplit(url).path) == ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns"):
            return 200, "<html><body>Sign in to the proxy</body></html>"
        return handle(method, url, body, auth)

    airflow.handle = page
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert "an answer of Airflow had an unexpected shape (TypeError in run_demo_pipeline)" in line
    assert line.endswith("See ./agoranomos logs airflow-apiserver.")


# A page in Latin-1, which is not UTF-8, as a proxy in front of Airflow or Metabase can send.
LATIN_1_PAGE = "<html><body>Passerelle indisponible, réessayez</body></html>".encode("latin-1")


@pytest.mark.parametrize(
    ("side", "call", "hint"),
    [
        ("airflow", ("POST", "/auth/token"), "airflow-apiserver"),
        ("metabase", ("GET", "/api/database"), "metabase"),
    ],
)
def test_a_page_that_is_not_utf_8_ends_in_one_line(airflow, metabase, capsys, side, call, hint):
    state = airflow if side == "airflow" else metabase
    handle = state.handle

    def page(method, url, body, header):
        if (method, urlsplit(url).path) == call:
            return 502, LATIN_1_PAGE
        return handle(method, url, body, header)

    state.handle = page
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        f"demo-init: {call[0]} {call[1]} failed with HTTP 502. See ./agoranomos logs {hint}."
    )


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


@pytest.mark.parametrize("left", ["no-table", "an-inactive-table"])
def test_a_mart_that_is_gone_after_an_earlier_run_names_the_cause_and_the_load(
    airflow, metabase, clock, capsys, left
):
    # A reset of the warehouse volume alone drops the mart, and Airflow keeps its runs: the start
    # finds the successful transform and runs no pipeline. Metabase shows no table, or, after its
    # next sync, the old one as inactive: neither counts. The warehouse has none, so the start
    # stops at once, with no sync and no wait, and its line says what to do.
    assert di.main(env_for(airflow, metabase)) == 0
    metabase.mart_in_warehouse = False
    if left == "no-table":
        metabase.mart_known = False
    else:
        metabase.mart_table()["active"] = False
    calls, started = len(metabase.calls), clock.now
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line.startswith(
        "demo-init: Metabase finds no marts.ad_spend_daily in the warehouse: the demo pipeline "
        "ran before"
    )
    assert f"trigger {di.LOAD_DAG} in Airflow" in line
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls[calls:]
    assert clock.now == started


def test_a_transform_that_ran_now_and_left_no_mart_names_the_transform(airflow, metabase, capsys):
    # The pipeline runs now and succeeds, and the warehouse still has no mart, as when the model
    # of the mart was left out of the run: the line points at the run of the transform, not at a
    # reset of the warehouse, and the job asks the warehouse once and syncs nothing.
    metabase.mart_in_warehouse = False
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        "demo-init: Metabase finds no marts.ad_spend_daily in the warehouse: see the last run of "
        f"{di.TRANSFORM_DAG} in Airflow"
    )
    assert airflow.calls.count(("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns")) == 1
    assert metabase.calls.count(("POST", "/api/dataset")) == 1
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls


def test_an_export_without_the_mart_of_the_demo_stops_before_anything_is_written(
    airflow, metabase, tmp_path, capsys
):
    # The dbt project of a release that renamed or dropped the mart of the demo: demo-init reads
    # the export first, so it stops before it signs in anywhere, and its line names the job that
    # exported the metadata.
    export = {**EXPORT, "models": [m for m in EXPORT["models"] if m["schema"] != "marts"]}
    metabase.files = metadata_env(tmp_path, export=export)
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        "demo-init: the dbt project declares no marts.ad_spend_daily: see ./agoranomos logs "
        "metadata-export"
    )
    assert airflow.calls == []
    assert metabase.calls == []


def test_a_first_start_waits_for_the_fields_of_the_mart_and_not_its_table_alone(
    airflow, metabase, clock, capsys
):
    # A sync of Metabase makes the row of a new table before its fields: the start waits for the
    # fields, so that apply() sets the metadata of every column, and none is a note.
    metabase.fields_seconds = 15.0
    assert di.main(env_for(airflow, metabase)) == 0
    assert clock.now >= metabase.synced_at + metabase.fields_seconds
    assert "does not show this column" not in capsys.readouterr().out
    assert metabase.field("spend")["semantic_type"] == "type/Currency"


def test_a_mart_that_a_person_hid_in_metabase_counts_as_synced(airflow, metabase, clock, capsys):
    # Hiding the table in Admin > Table Metadata is a normal edit, and the dashboard keeps working:
    # its questions are SQL. The next start finds the table and waits for nothing.
    assert di.main(env_for(airflow, metabase)) == 0
    metabase.mart_hidden = True
    calls, started = len(metabase.calls), clock.now
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls[calls:]
    assert clock.now == started
    assert capsys.readouterr().out.startswith("demo-init: the demo pipeline ran before")


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


def test_the_module_imports_only_the_standard_library_and_the_other_jobs():
    tree = ast.parse(Path(di.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    allowed = set(sys.stdlib_module_names) | {"metabase_init", "metadata"}
    assert imported <= allowed, sorted(imported - allowed)


def settings_of(metabase: FakeMetabase, name: str) -> dict[str, Any]:
    [card] = [card for card in metabase.cards.values() if card["name"] == name]
    return card["visualization_settings"]


def test_new_questions_get_the_titles_of_their_columns_and_axes(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    by_campaign = settings_of(metabase, "Spend by campaign, last 7 days")
    assert by_campaign["column_settings"] == {
        '["name","campaign_name"]': {"column_title": "Campaign Name"},
        '["name","spend"]': {"column_title": "Spend"},
    }
    assert by_campaign["graph.x_axis.title_text"] == "Campaign Name"
    assert by_campaign["graph.y_axis.title_text"] == "Spend"
    assert settings_of(metabase, "Spend by day")["graph.x_axis.title_text"] == "Report Date"
    # A number has no axes.
    assert settings_of(metabase, "Spend, last 7 days") == {
        "column_settings": {'["name","spend"]': {"column_title": "Spend"}}
    }


def test_the_titles_come_from_the_overrides_of_the_deployment(airflow, metabase, tmp_path):
    overrides = "[marts.ad_spend_daily.columns.spend]\ndisplay_name = 'Cost'\n"
    metabase.files = metadata_env(tmp_path, overrides=overrides)
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, "Spend by day")["graph.y_axis.title_text"] == "Cost"
    assert metabase.field("spend")["display_name"] == "Cost"


def test_a_question_that_exists_gets_only_the_titles_that_it_lacks(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by day")
    settings["graph.x_axis.title_text"] = "Day"  # a title of a person
    del settings["graph.y_axis.title_text"]  # as on a question made before the titles
    spend = settings["column_settings"]['["name","spend"]']
    del spend["column_title"]
    spend["decimals"] = 0  # a format of a person
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by day")
    assert settings["graph.x_axis.title_text"] == "Day"
    assert settings["graph.y_axis.title_text"] == "Spend"
    assert settings["column_settings"]['["name","spend"]'] == {
        "decimals": 0,
        "column_title": "Spend",
    }
    assert settings["graph.dimensions"] == ["report_date"]


CHARTS = ("Spend by campaign, last 7 days", "Spend by day")


def test_the_charts_are_drawn_in_the_first_color_of_the_project(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    for name in CHARTS:
        # The series of a chart with the one metric spend is keyed by the name of its column.
        assert settings_of(metabase, name)["series_settings"] == {"spend": {"color": "#A86400"}}
    # A number has no series.
    assert "series_settings" not in settings_of(metabase, "Spend, last 7 days")


@pytest.mark.parametrize("name", CHARTS)
def test_a_chart_that_exists_gets_the_color_that_it_lacks(airflow, metabase, name):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, name)
    # As on a chart made before the color, with a title of a person on its series.
    settings["series_settings"] = {"spend": {"title": "Cost"}}
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, name)
    assert settings["series_settings"] == {"spend": {"title": "Cost", "color": "#A86400"}}
    assert settings["graph.metrics"] == ["spend"]
    del settings["series_settings"]
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {"spend": {"color": "#A86400"}}


@pytest.mark.parametrize("name", CHARTS)
@pytest.mark.parametrize("color", ["#4F6382", "#a86400", "red", ""])
def test_a_color_that_a_person_set_stays(airflow, metabase, name, color):
    assert di.main(env_for(airflow, metabase)) == 0
    settings_of(metabase, name)["series_settings"]["spend"] = {"color": color, "color_name": "x"}
    calls_before = len(metabase.calls)
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {
        "spend": {"color": color, "color_name": "x"}
    }
    # Nothing was written: a question that lacks nothing is not saved again.
    assert all(
        call[0] == "GET" or call == ("POST", "/api/session")
        for call in metabase.calls[calls_before:]
    )


def test_the_series_of_a_person_stay_when_the_color_is_added(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by day")
    settings["series_settings"] = {"other": {"color": "#000000"}, "spend": {"title": "Cost"}}
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, "Spend by day")["series_settings"] == {
        "other": {"color": "#000000"},
        "spend": {"title": "Cost", "color": "#A86400"},
    }


@pytest.mark.parametrize("name", CHARTS)
@pytest.mark.parametrize("others", [{}, {"other": {"color": "#000000"}}])
def test_a_chart_whose_series_settings_lack_spend_gets_the_color(airflow, metabase, name, others):
    assert di.main(env_for(airflow, metabase)) == 0
    settings_of(metabase, name)["series_settings"] = dict(others)
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {
        **others,
        "spend": {"color": "#A86400"},
    }


def test_a_mistake_in_the_overrides_stops_demo_init_before_it_writes(
    airflow, metabase, tmp_path, capsys
):
    metabase.files = metadata_env(tmp_path, overrides="[marts.ad_spend_daily]\ndisplay_name = 7\n")
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        "demo-init: overrides.toml: marts.ad_spend_daily.display_name: a text in quotes is "
        "expected, not int"
    )
    assert airflow.calls == []
    assert metabase.calls == []


def test_demo_init_applies_the_metadata_after_its_sync(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    sync = metabase.calls.index(("POST", "/api/database/2/sync_schema"))
    field_puts = [
        i for i, call in enumerate(metabase.calls) if call[0] == "PUT" and "/api/field/" in call[1]
    ]
    assert field_puts
    assert min(field_puts) > sync
    assert metabase.field("conversions")["display_name"] == "Conversions"


def test_a_later_step_that_fails_leaves_the_lines_of_the_metadata_in_the_log(
    airflow, metabase, capsys
):
    # demo-init applies the metadata before it makes the demo content. The next start finds nothing
    # to change in the metadata, so the lines of what it applied, with the values that they
    # replaced, must be in the log when a later step fails.
    metabase.failures[("POST", "/api/collection")] = 500
    assert di.main(env_for(airflow, metabase)) == 1
    output = capsys.readouterr()
    applied = output.out.splitlines()
    assert applied[0] == (
        'demo-init: first fill: marts.ad_spend_daily display_name: "Ad Spend Daily" -> '
        '"Ad Spend by Day"'
    )
    assert all(line.startswith("demo-init: first fill: ") for line in applied), applied
    [line] = output.err.splitlines()
    assert line.startswith("demo-init: POST /api/collection failed with HTTP 500")
    assert metabase.mart_table()["display_name"] == "Ad Spend by Day"
    assert not leaked(output.out + output.err)


def test_the_lines_of_the_metadata_reach_the_log_of_a_process_before_its_error(airflow, metabase):
    # As a container runs it, with its standard output a pipe that Python buffers: each line is
    # flushed as it is printed, so the log has the lines of the metadata before the line of the
    # error. The pipeline and the history ran before and Metabase knows the mart, so the process
    # waits for nothing.
    airflow.add_run(di.TRANSFORM_DAG, "success", at=-10)
    airflow.add_run(di.HISTORY_DAG, "success", at=-10)
    metabase.mart_known = True
    metabase.failures[("POST", "/api/collection")] = 500
    result = subprocess.run(
        [sys.executable, str(Path(di.__file__))],
        env=env_for(airflow, metabase),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )
    assert result.returncode == 1
    *applied, error = result.stdout.splitlines()
    assert applied, "no line of the metadata before the error"
    assert all(line.startswith("demo-init: first fill: ") for line in applied), applied
    assert error.startswith("demo-init: POST /api/collection failed with HTTP 500")
    assert not leaked(result.stdout)
