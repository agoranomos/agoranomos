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
import re
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import content
import demo_init as di
import metadata as md
import pytest
from metabase_stand_in import (
    DEMO_EXPORT,
    SAMPLE,
    ContentState,
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
    # The files of the metadata, and the registry of the content: env_for points these at files in
    # the folder of the test.
    "METADATA_EXPORT": "/nonexistent/metadata.json",
    "METADATA_OVERRIDES": "/nonexistent/overrides.toml",
    "CONTENT_REGISTRY": "/nonexistent/content.json",
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
# The display names of the columns of each mart in the export of the stand-in, by mart and name,
# with the titles of the ratios, which are no column of a mart.
TITLES = {
    model["name"]: {
        **{c["name"]: c["meta"]["metabase"]["display_name"] for c in model["columns"]},
        **di.RATIO_TITLES,
    }
    for model in DEMO_EXPORT["models"]
    if model["schema"] == "marts"
}
# The marts that the questions of the demo read, as the lines of demo-init list them.
MARTS = (
    "marts.ad_conversions_campaign_daily, marts.ad_stats_campaign_daily and "
    "marts.web_sessions_daily"
)
# The names of the two dashboards of the demo, and the questions of each, by key.
ADVERTISING, WEB = (di.DASHBOARDS[key][0] for key in (di.ADVERTISING, di.WEB))
QUESTIONS = {q["key"]: q for q in di.QUESTIONS}


def leaked(text: str) -> list[str]:
    return leaked_in(text, SECRETS)


def stamp(seconds: float) -> str:
    """A moment on the fake clock as Airflow writes it: in UTC, with microseconds and a Z."""
    return (START + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FakeAirflow:
    """What demo-init can see of Airflow 3.3.2: the three DAGs of the demo, the history, the
    transform and the daily load, their runs, and the five assets between the history and the
    transform.

    A scheduler runs on the fake clock, one loop a second, in the order of the loop of Airflow:
    first a run for an event that waits, then the queued runs start, then the running ones end.
    A queued run of an unpaused DAG starts when no other run of its DAG is running
    (max_active_runs=1), and a running one ends after the duration of its DAG, with its result.
    The last of the five loads of a history that succeeds updates its asset a loop before the run
    of the history ends: while the transform is unpaused, that completes the events of the five
    assets, which the fake keeps as one event, unless one is queued already, in which case it is
    dropped (ON CONFLICT DO NOTHING). The event becomes a run of the transform, whose run_after is
    the time of the event, on a later loop, once the transform has no run queued or running.
    partial_events are the events of some of the five assets, as a history that failed half way
    leaves them: they start no run, and a run of the transform that the one event starts consumes
    them too. The list of queued events counts all five while the fake keeps its event, else the
    partial ones. triggers are the runs that a person triggers in the Airflow UI, at a time. The
    daily load, unpaused without a run, makes a scheduled run at once, for the last time of its
    schedule: after the history it finds the day of every account loaded, and updates no asset.

    The list of runs answers as the API does: the oldest first unless order_by says otherwise, 50
    to a page unless limit says otherwise and at most 100, and total_entries counts every run that
    matches the filters.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.url = ""
        self.loaded_at = 0.0  # when the DAG processor has loaded the DAGs
        self.paused = {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.DAILY_DAG: True}
        self.durations = {di.LOAD_DAG: 10.0, di.TRANSFORM_DAG: 30.0, di.DAILY_DAG: 5.0}
        self.results = dict.fromkeys(self.paused, "success")
        self.runs: list[dict[str, Any]] = []
        self.queued_event: float | None = None
        self.partial_events = 0
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
            self.partial_events = 0
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
            # The last load of the history updates its asset when it ends, a loop before the
            # scheduler marks the run as done: as on a running stack, the event is older than the
            # end of the run.
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
            if dag_id == di.DAILY_DAG and not body["is_paused"] and not self.runs_of(dag_id):
                self.add_run(dag_id, "queued", "scheduled")
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
            count = 0
            if dag_id == di.TRANSFORM_DAG:
                count = (
                    di.TRANSFORM_ASSETS if self.queued_event is not None else self.partial_events
                )
            events = [
                {"dag_id": dag_id, "asset_id": n + 1, "created_at": "..."} for n in range(count)
            ]
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
    state.demo_marts = True
    state.files = {
        **metadata_env(tmp_path, export=DEMO_EXPORT),
        "CONTENT_REGISTRY": str(tmp_path / "content.json"),
    }
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


# The lines that demo-init prints for the metadata and for the content, each as soon as its write
# is done.
METADATA_LINES = tuple(f"demo-init: {kind}: " for kind in (md.FIRST, md.UPDATE, md.REVERT))
CONTENT_LINES = tuple(
    f"demo-init: {kind}: "
    for kind in (
        "created",
        "created again",
        "restored from the trash",
        "retired",
        "lowered",
        "moved",
        "moved out",
    )
)


# The line that demo-init prints when it has unpaused the daily load, after the pipeline.
DAILY_LINE = f"demo-init: unpaused {di.DAILY_DAG}, which loads the new days of the demo every day"

# The calls that demo-init makes after it applied the metadata of the marts: a failure of one of
# them comes after the lines of the changes that apply() made, and of those that it made in the
# content before.
AFTER_APPLY = {
    ("PUT", "/api/field/"),
    ("POST", "/api/card"),
    ("PUT", "/api/dashboard/"),
    ("POST", "/api/collection"),
}


def one_line(capsys: pytest.CaptureFixture[str], *, after_apply: bool = False) -> str:
    """The line that demo-init prints when it fails: one line on standard error, which names the
    step, with no traceback and no secret. Standard output holds no more than the line of the
    daily load that demo-init unpaused, and, for a failure after the metadata was applied, the
    lines of the changes that it made."""
    output = capsys.readouterr()
    lines = output.out.splitlines()
    if lines[:1] == [DAILY_LINE]:
        lines = lines[1:]
    if after_apply:
        assert all(line.startswith(METADATA_LINES + CONTENT_LINES) for line in lines), output.out
    else:
        assert lines == [], output.out
    assert "Traceback" not in output.err
    assert not leaked(output.out + output.err), leaked(output.out + output.err)
    [line] = output.err.splitlines()
    assert line.startswith("demo-init: ")
    return line


def registry_of(metabase: FakeMetabase) -> dict[str, dict[str, str]]:
    """The entries of the registry of the content that the jobs keep, by key."""
    path = Path(metabase.files["CONTENT_REGISTRY"])
    return json.loads(path.read_text(encoding="utf-8"))["items"]


def registry_keys(metabase: FakeMetabase) -> list[str]:
    """The keys of the registry of the content, none while the jobs have written no registry."""
    path = Path(metabase.files["CONTENT_REGISTRY"])
    return list(registry_of(metabase)) if path.exists() else []


def registered(metabase: FakeMetabase, key: str) -> dict[str, Any]:
    """The item of a key of the registry, as Metabase keeps it."""
    entry = registry_of(metabase)[key]
    content = metabase.content_state
    table = {"collection": content.collections, "card": content.cards}.get(
        entry["model"], content.dashboards
    )
    [item] = [item for item in table.values() if item["entity_id"] == entry["entity_id"]]
    return item


def demo_items(
    metabase: FakeMetabase,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """The collection Agoranomos and its sub-collection Demo, as the registry finds them, and the
    questions and the dashboards of Demo by name, those in the trash left out."""
    content = metabase.content_state
    root, demo = registered(metabase, "agoranomos"), registered(metabase, "demo")
    cards = {
        c["name"]: c
        for c in content.cards.values()
        if c["collection_id"] == demo["id"] and not c["archived"]
    }
    boards = {
        d["name"]: d
        for d in content.dashboards.values()
        if d["collection_id"] == demo["id"] and not d["archived"]
    }
    return root, demo, cards, boards


def card_of(metabase: FakeMetabase, name: str) -> dict[str, Any]:
    """The question of the demo with a name."""
    return demo_items(metabase)[2][name]


def board_of(metabase: FakeMetabase, name: str = ADVERTISING) -> dict[str, Any]:
    """The dashboard of the demo with a name."""
    return demo_items(metabase)[3][name]


def done_line(metabase: FakeMetabase, ran: str = "ran now") -> str:
    """The last line of a start that succeeds, with the IDs of the two dashboards."""
    named = " and ".join(
        f"{name!r} ({board_of(metabase, name)['id']})" for name in (ADVERTISING, WEB)
    )
    return (
        f"demo-init: the demo pipeline {ran}, and the dashboards {named} are in the collection "
        "Agoranomos / Demo"
    )


# The questions of v0.1 by name, as its demo-init made them: the description, the query on the
# mart of v0.1, the column and the series of its charts, spend, its display and its place on the
# grid of its dashboard.
V01_LAST_7_DAYS = "report_date > (SELECT max(report_date) FROM marts.ad_spend_daily) - 7"
V01_QUESTIONS = {
    "Spend, last 7 days": (
        "The spend of the last seven days that have data.",
        f"SELECT sum(spend) AS spend\nFROM marts.ad_spend_daily\nWHERE {V01_LAST_7_DAYS}",
        {"column_settings": {'["name","spend"]': {"column_title": "Spend"}}},
        "scalar",
        {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    ),
    "Spend by campaign, last 7 days": (
        "The spend of each campaign in the last seven days that have data.",
        "SELECT campaign_name, sum(spend) AS spend\nFROM marts.ad_spend_daily\n"
        f"WHERE {V01_LAST_7_DAYS}\nGROUP BY campaign_name\nORDER BY spend DESC",
        {
            "graph.dimensions": ["campaign_name"],
            "graph.metrics": ["spend"],
            "series_settings": {"spend": {"color": di.SERIES_COLOR}},
        },
        "row",
        {"row": 0, "col": 6, "size_x": 18, "size_y": 4},
    ),
    "Spend by day": (
        "The spend of every day that has data.",
        "SELECT report_date, sum(spend) AS spend\nFROM marts.ad_spend_daily\n"
        "GROUP BY report_date\nORDER BY report_date",
        {
            "graph.dimensions": ["report_date"],
            "graph.metrics": ["spend"],
            "series_settings": {"spend": {"color": di.SERIES_COLOR}},
        },
        "line",
        {"row": 4, "col": 0, "size_x": 24, "size_y": 6},
    ),
}


def an_earlier_demo(metabase: FakeMetabase) -> tuple[dict, dict[str, dict], dict]:
    """The content of v0.1 in Metabase: the collection Agoranomos demo at the root, with its three
    questions and its dashboard, which shows them, all with the queries and settings of v0.1."""
    content = metabase.content_state
    old = content.add_collection(di.COLLECTION_V01, "The demo of Agoranomos: one ad account.")
    cards = {}
    for name, (description, sql, settings, display, _) in V01_QUESTIONS.items():
        query = {"database": 2, "type": "native", "native": {"query": sql}}
        cards[name] = content.add_card(
            {
                "name": name,
                "description": description,
                "display": display,
                "collection_id": old["id"],
                "dataset_query": query,
                "visualization_settings": copy.deepcopy(settings),
            }
        )
    board = content.add_dashboard(
        {
            "name": di.DASHBOARD_V01,
            "description": "Ad spend of the demo account, from marts.ad_spend_daily.",
            "collection_id": old["id"],
        }
    )
    board["dashcards"] = [
        {"id": 900 + index, "card_id": cards[name]["id"], **question[4]}
        for index, (name, question) in enumerate(V01_QUESTIONS.items())
    ]
    return old, cards, board


def test_the_scan_finds_a_secret_in_the_json_of_an_echo():
    # A stand-in echoes the request as JSON, which escapes the quotes of the Airflow password: the
    # value as it is would not be found.
    echo = json.dumps({"body": {"password": ENV["AIRFLOW_ADMIN_PASSWORD"]}})
    assert ENV["AIRFLOW_ADMIN_PASSWORD"] not in echo
    assert leaked(echo) == ["the Airflow password"]


def test_a_first_start_runs_the_pipeline_and_creates_the_demo(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: False, di.TRANSFORM_DAG: False, di.DAILY_DAG: False}
    assert airflow.calls.count(("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns")) == 1
    # The history starts the transform through the assets of its five loads: demo-init does not.
    assert ("POST", f"/api/v2/dags/{di.TRANSFORM_DAG}/dagRuns") not in airflow.calls
    [transform] = airflow.runs_of(di.TRANSFORM_DAG)
    assert (transform["run_type"], transform["state"]) == ("asset_triggered", "success")
    assert metabase.calls.count(("POST", "/api/database/2/sync_schema")) == 1
    _, demo, cards, boards = demo_items(metabase)
    assert sorted(cards) == sorted(question["name"] for question in di.QUESTIONS)
    assert len(metabase.cards) == len(di.QUESTIONS)
    for question in di.QUESTIONS:
        card = cards[question["name"]]
        assert card["dataset_query"]["database"] == 2
        assert card["dataset_query"]["type"] == "native"
        assert f"marts.{question['mart']}" in card["dataset_query"]["native"]["query"]
    assert sorted(boards) == sorted([ADVERTISING, WEB])
    for key, (name, _) in di.DASHBOARDS.items():
        assert boards[name]["collection_id"] == demo["id"]
        shown = sorted(dashcard["card_id"] for dashcard in boards[name]["dashcards"])
        wanted = [cards[q["name"]]["id"] for q in di.QUESTIONS if q["dashboard"] == key]
        assert shown == sorted(wanted)
    out = capsys.readouterr()
    assert out.err == ""
    *lines, line = out.out.splitlines()
    assert line == done_line(metabase)
    # The metadata of the marts, applied after the sync: one line for each change, and then one
    # line for each item of the content that it created.
    assert lines[0] == DAILY_LINE
    assert "demo-init: first fill: marts.ad_conversions_campaign_daily display_name: " in lines[1]
    assert 'demo-init: created: collection agoranomos "Agoranomos"' in lines
    assert f'demo-init: created: dashboard demo/advertising "{ADVERTISING}"' in lines
    assert f'demo-init: created: dashboard demo/web-analytics "{WEB}"' in lines
    assert not leaked(out.out + out.err)


def test_a_first_start_makes_the_demo_in_agoranomos(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    root, demo, cards, boards = demo_items(metabase)
    assert (root["name"], root["location"]) == ("Agoranomos", "/")
    assert (demo["name"], demo["location"]) == ("Demo", f"/{root['id']}/")
    assert set(cards) == {q["name"] for q in di.QUESTIONS}
    assert set(boards) == {ADVERTISING, WEB}
    # All Users may only view the demo, and the Administrators keep Curate.
    assert metabase.content_state.graph["1"][str(demo["id"])] == "read"
    assert metabase.content_state.graph["1"][str(root["id"])] == "read"
    assert metabase.content_state.graph["2"][str(demo["id"])] == "write"
    # The registry knows each item by its key, and by the entity ID that Metabase gave it.
    keys = {f"demo/{q['key']}" for q in di.QUESTIONS} | {f"demo/{key}" for key in di.DASHBOARDS}
    assert set(registry_of(metabase)) == keys | {"agoranomos", "demo"}
    assert registry_of(metabase)["demo/spend-by-day"] == {
        "model": "card",
        "entity_id": cards["Spend by day and platform"]["entity_id"],
    }
    assert registry_of(metabase)["demo/web-analytics"] == {
        "model": "dashboard",
        "entity_id": boards[WEB]["entity_id"],
    }


def test_a_second_start_changes_nothing(airflow, metabase, clock, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    # The run that the daily load made when demo-init unpaused it ends before the next start.
    clock.sleep(60)
    airflow.catch_up()
    assert [run["state"] for run in airflow.runs_of(di.DAILY_DAG)] == ["success"]
    calls_before = len(airflow.calls), len(metabase.calls)
    runs_before = copy.deepcopy(airflow.runs)
    content_before = copy.deepcopy(
        (metabase.collections, metabase.cards, metabase.dashboards, metabase.content_state.graph)
    )
    registry = Path(metabase.files["CONTENT_REGISTRY"])
    saved = registry.read_text(encoding="utf-8")
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    # It signs in and reads: no PATCH of the pause state, no new run, nothing new in Metabase.
    for call in airflow.calls[calls_before[0] :]:
        assert call[0] == "GET" or call == ("POST", "/auth/token"), call
    for call in metabase.calls[calls_before[1] :]:
        assert call[0] == "GET" or call == ("POST", "/api/session"), call
    assert airflow.runs == runs_before
    assert content_before == (
        metabase.collections,
        metabase.cards,
        metabase.dashboards,
        metabase.content_state.graph,
    )
    assert registry.read_text(encoding="utf-8") == saved
    # One line, which says that nothing ran: no line of a change.
    [line] = capsys.readouterr().out.splitlines()
    assert line == done_line(metabase, "ran before")


def test_a_pause_made_after_the_first_start_stays(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    airflow.paused = dict.fromkeys(airflow.paused, True)  # paused in the Airflow UI
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.DAILY_DAG: True}


def test_the_daily_load_is_unpaused_after_the_history_and_its_first_run_ends(airflow, metabase):
    # The daily load would bring the history itself on a run before it: demo-init unpauses it
    # once the transform after the history has succeeded.
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.calls.count(("PATCH", f"/api/v2/dags/{di.DAILY_DAG}")) == 1
    [history] = airflow.runs_of(di.LOAD_DAG)
    [transform] = airflow.runs_of(di.TRANSFORM_DAG)
    # The stand-in makes the run of the daily load when it is unpaused.
    [daily] = airflow.runs_of(di.DAILY_DAG)
    assert daily["run_type"] == "scheduled"
    assert daily["run_after"] >= transform["end_date"] > history["end_date"]


def test_an_update_that_brings_the_daily_load_unpauses_it_without_the_pipeline(
    airflow, metabase, capsys
):
    # A deployment whose pipeline ran before the update: the daily load, which has never run,
    # brings the days since, and the history does not run again.
    airflow.add_run(di.LOAD_DAG, "success", at=-3_600)
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3_500)
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.DAILY_DAG: False}
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == DAILY_LINE
    assert "the demo pipeline ran before" in lines[-1]


def test_a_daily_load_that_has_run_keeps_its_pause_state(airflow, metabase):
    airflow.add_run(di.LOAD_DAG, "success", at=-3_600)
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3_500)
    airflow.add_run(di.DAILY_DAG, "success", "scheduled", at=-3_400)
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("PATCH", f"/api/v2/dags/{di.DAILY_DAG}") not in airflow.calls
    assert airflow.paused[di.DAILY_DAG] is True


def test_a_failed_history_leaves_the_daily_load_paused(airflow, metabase):
    airflow.results[di.LOAD_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    assert airflow.paused[di.DAILY_DAG] is True
    assert airflow.runs_of(di.DAILY_DAG) == []


def test_demo_init_waits_for_the_daily_load_to_be_loaded(airflow, metabase, capsys):
    # The DAG processor may load the daily load last: until it has, demo-init cannot unpause it.
    del airflow.paused[di.DAILY_DAG]
    assert di.main(env_for(airflow, metabase)) == 1
    assert f"Airflow did not load the DAG {di.DAILY_DAG}" in one_line(capsys)
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls


def test_a_success_long_ago_counts_whatever_ran_since(airflow, metabase):
    # Airflow keeps every run, also after ./agoranomos down. Here the one success of the history,
    # and the one of the transform after it, are the oldest of 121 runs of each, so a page of the
    # newest runs would not show them: the newest success and the count of the successful runs
    # since it do.
    for dag_id, at in ((di.LOAD_DAG, -10_000), (di.TRANSFORM_DAG, -9_990)):
        airflow.add_run(dag_id, "success", at=at)
        for n in range(120):
            airflow.add_run(dag_id, "failed", at=-9_000 + n)
    airflow.add_run(di.DAILY_DAG, "success", "scheduled", at=-9_980)
    assert di.main(env_for(airflow, metabase)) == 0
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.DAILY_DAG: True}
    assert [call for call in airflow.calls if call[0] in ("POST", "PATCH")] == [
        ("POST", "/auth/token")
    ]


def test_the_newest_of_many_histories_is_awaited_and_not_started_again(airflow, metabase):
    # 120 histories that ended long ago, and the newest is still running: the oldest 50, which the
    # API gives unless it is told otherwise, would show only the old ones.
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


def test_the_history_starts_only_once_the_transform_is_idle(airflow, metabase):
    # An earlier start stopped half-way: a run of the transform is running, and an event waits for
    # the next one. With max_active_runs=1, the event of a history that ended now would be dropped
    # and would start no run, so demo-init waits until the transform is idle before the history.
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
    # The events of the five assets wait, and the scheduler has not made their run yet: no run is
    # queued or running. The transform is busy all the same, and the history waits until the run
    # of those events has ended, since its own events would find the waiting ones and be dropped.
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


def test_a_failed_run_of_a_person_does_not_stop_the_job_while_the_run_of_the_history_waits(
    airflow, metabase, capsys, monkeypatch
):
    # A person triggers the transform in the Airflow UI while the history runs, and that run fails,
    # with a run_after after the start of the history. The event of the history waits for that run
    # to end and becomes a run a loop later. The failed run ends at 35 s, when demo-init looks: the
    # event still waits then, so the transform is busy, and the job waits for the run of the
    # history.
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


def test_a_run_of_the_history_that_succeeds_while_the_job_looks_at_a_failed_one_counts(
    airflow, metabase, capsys
):
    # A failed run of a person, as above, and the run of the event of the history ends with success
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
    assert ended, "the run of the history never ended between the two looks"
    assert "the demo pipeline ran now" in capsys.readouterr().out


def test_a_history_that_is_running_is_awaited_and_not_started_again(airflow, metabase):
    airflow.add_run(di.LOAD_DAG, "running")
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls


def test_a_failed_history_stops_with_the_dag_named(airflow, metabase, capsys):
    airflow.results[di.LOAD_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    assert one_line(capsys) == f"demo-init: the DAG {di.LOAD_DAG} failed: see it in Airflow"
    assert metabase.calls == []


def test_a_failed_transform_names_the_transform_dag_and_dbt(airflow, metabase, capsys):
    airflow.results[di.TRANSFORM_DAG] = "failed"
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert f"the DAG {di.TRANSFORM_DAG} failed after the demo history" in line
    assert "dbt test" in line
    assert metabase.calls == []


def last_line(capsys: pytest.CaptureFixture[str]) -> str:
    """The last line that demo-init printed on a success: nothing on standard error."""
    output = capsys.readouterr()
    assert output.err == "", output.err
    return output.out.splitlines()[-1]


def test_the_transform_waits_for_one_asset_of_each_platform():
    from agoranomos_pipelines.demo import platforms

    assert di.LOAD_DAG == "agoranomos_demo_history"
    assert di.TRANSFORM_ASSETS == len(platforms.PLATFORMS) == 5


def test_a_deployment_of_v0_1_runs_the_history_and_a_transform_after_it(airflow, metabase, capsys):
    # v0.1 left a transform that succeeded and no run of the history.
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-86_400)
    assert di.main(env_for(airflow, metabase)) == 0
    assert [run["state"] for run in airflow.runs_of(di.LOAD_DAG)] == ["success"]
    assert [run["state"] for run in airflow.runs_of(di.TRANSFORM_DAG)] == ["success", "success"]
    assert "the demo pipeline ran now" in last_line(capsys)


def test_a_history_without_a_good_transform_after_it_runs_again(airflow, metabase):
    # A deployment of v0.1 whose first transform after the history failed, as on a critical test:
    # the success of v0.1 came before the history, and a repeat replaces the same days.
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-86_400)
    airflow.add_run(di.LOAD_DAG, "success", at=-3_600)
    airflow.add_run(di.TRANSFORM_DAG, "failed", run_type="asset_triggered", at=-3_500)
    assert di.main(env_for(airflow, metabase)) == 0
    assert [run["state"] for run in airflow.runs_of(di.LOAD_DAG)] == ["success", "success"]
    assert airflow.runs_of(di.TRANSFORM_DAG)[-1]["state"] == "success"


def test_a_history_with_a_good_transform_after_it_is_not_run_again(airflow, metabase, capsys):
    airflow.add_run(di.LOAD_DAG, "success", at=-3_600)
    airflow.add_run(di.TRANSFORM_DAG, "success", run_type="asset_triggered", at=-3_500)
    airflow.add_run(di.DAILY_DAG, "success", "scheduled", at=-3_400)
    assert di.main(env_for(airflow, metabase)) == 0
    assert ("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns") not in airflow.calls
    assert airflow.paused == {di.LOAD_DAG: True, di.TRANSFORM_DAG: True, di.DAILY_DAG: True}
    assert "the demo pipeline ran before" in last_line(capsys)


@pytest.mark.parametrize(("result", "histories"), [("success", 1), ("failed", 2)])
def test_a_transform_that_an_earlier_start_left_running_after_the_history_is_awaited(
    airflow, metabase, capsys, result, histories
):
    # An earlier start ran out of time while the transform that the history started was running.
    # The next start waits for that run: when it succeeds, the history does not run again, so a
    # slow machine gets past the deadline in two starts; when it fails, the history runs again.
    airflow.paused.update({di.LOAD_DAG: False, di.TRANSFORM_DAG: False})
    airflow.add_run(di.LOAD_DAG, "success", at=-60)
    airflow.add_run(di.TRANSFORM_DAG, "running", "asset_triggered", at=-10, result=result)
    assert di.main(env_for(airflow, metabase)) == 0
    assert len(airflow.runs_of(di.LOAD_DAG)) == histories
    assert airflow.runs_of(di.TRANSFORM_DAG)[-1]["state"] == "success"
    assert "the demo pipeline ran now" in last_line(capsys)


def test_the_events_of_a_history_that_failed_half_way_do_not_hold_up_the_start(airflow, metabase):
    # Two of the five platforms loaded before a history failed: their events wait for the other
    # three and start no run, so the transform is not busy, and the next history completes them.
    airflow.add_run(di.LOAD_DAG, "failed", at=-600)
    airflow.partial_events = 2
    assert di.main(env_for(airflow, metabase)) == 0
    assert [run["state"] for run in airflow.runs_of(di.TRANSFORM_DAG)] == ["success"]
    assert airflow.partial_events == 0


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
        id="history",
    ),
    pytest.param(
        lambda airflow, metabase: airflow.durations.update({di.TRANSFORM_DAG: math.inf}),
        f"the DAG {di.TRANSFORM_DAG} did not succeed after the demo history",
        "see it in Airflow",
        id="transform",
    ),
    pytest.param(
        lambda airflow, metabase: setattr(metabase, "sync_seconds", math.inf),
        f"Metabase did not find {MARTS} after a sync of the warehouse",
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
    # The DAGs take 595 of the 600 seconds to load, and a history takes 10: with one deadline for
    # the whole job, the history runs out of time, five seconds after it starts.
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
        ("metabase", ("POST", "/api/card"), "Metabase", "ensure_question"),
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
    # Metabase still shows the other marts, active, when it only marked the mart inactive.
    gone = MARTS if left == "no-table" else "marts.ad_stats_campaign_daily"
    assert line.startswith(
        f"demo-init: Metabase finds no {gone} in the warehouse: the demo pipeline ran before"
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
        f"demo-init: Metabase finds no {MARTS} in the warehouse: see the last run of "
        f"{di.TRANSFORM_DAG} in Airflow"
    )
    assert airflow.calls.count(("POST", f"/api/v2/dags/{di.LOAD_DAG}/dagRuns")) == 1
    assert metabase.calls.count(("POST", "/api/dataset")) == 1
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls


def test_an_export_without_the_mart_of_the_demo_stops_before_anything_is_written(
    airflow, metabase, tmp_path, capsys
):
    # The dbt project of a release that renamed or dropped a mart of the demo: demo-init reads
    # the export first, so it stops before it signs in anywhere, and its line names the job that
    # exported the metadata.
    models = [m for m in DEMO_EXPORT["models"] if m["name"] != "ad_stats_campaign_daily"]
    export = {**DEMO_EXPORT, "models": models}
    metabase.files.update(metadata_env(tmp_path, export=export))
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        "demo-init: the dbt project declares no marts.ad_stats_campaign_daily: see ./agoranomos "
        "logs metadata-export"
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


def test_a_collection_of_the_old_name_that_is_not_at_the_root_does_not_move(airflow, metabase):
    # One inside another collection, one that is personal, and one in the trash: none is the demo
    # of an earlier release, and none changes. Nor does a collection named Agoranomos or Demo that
    # a person made: the jobs never take a collection by its name for theirs.
    content = metabase.content_state
    marketing = content.add_collection("Marketing")
    others = [
        content.add_collection(di.COLLECTION_V01, parent_id=marketing["id"]),
        content.add_collection(di.COLLECTION_V01),
        content.add_collection(di.COLLECTION_V01),
        content.add_collection("Agoranomos"),
        content.add_collection("Demo"),
    ]
    others[1]["personal_owner_id"] = 1
    others[2]["archived"] = True
    before = copy.deepcopy(others)
    assert di.main(env_for(airflow, metabase)) == 0
    assert others == before
    root, demo, _, _ = demo_items(metabase)
    assert {root["id"], demo["id"]}.isdisjoint(other["id"] for other in others)
    assert {card["collection_id"] for card in metabase.cards.values()} == {demo["id"]}
    assert content.graph["1"][str(others[3]["id"])] == "write", "the decoy keeps its permissions"


def test_items_that_come_as_a_bare_list_are_understood(airflow, metabase):
    # The items of the collection of an earlier release, which the move reads, as a bare list.
    metabase.items_as_list = True
    old, cards, board = an_earlier_demo(metabase)
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, now_cards, boards = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert {c["id"] for c in cards.values()} <= {c["id"] for c in now_cards.values()}
    assert boards[ADVERTISING]["id"] == board["id"]


def test_a_dashboard_whose_cards_were_changed_gets_those_of_the_code_back(
    airflow, metabase, capsys
):
    assert di.main(env_for(airflow, metabase)) == 0
    dashboard = board_of(metabase)

    def grid():
        return sorted(
            (d["card_id"], d["row"], d["col"], d["size_x"], d["size_y"])
            for d in dashboard["dashcards"]
        )

    placed = grid()
    # A person removed the questions, added a text card and a filter.
    dashboard["dashcards"] = [
        {"id": 990, "card_id": None, "row": 10, "col": 0, "size_x": 4, "size_y": 2}
    ]
    dashboard["parameters"] = [{"id": "f1", "name": "Campaign", "type": "string/="}]
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    assert grid() == placed
    filters = [di.DASHBOARD_FILTERS[name] for name in di.AD_FILTERS]
    assert dashboard["parameters"] == [content.filter_shape(f) for f in filters]
    assert len(metabase.cards) == len(di.QUESTIONS)
    assert (
        "demo-init: revert of an edit: dashboard demo/advertising cards: 1 card (1 not as the code "
        "says) and 1 filter (1 not as the code says) -> 9 cards and 3 filters"
    ) in capsys.readouterr().out.splitlines()


def test_a_question_in_the_trash_comes_back_and_one_deleted_for_good_is_made_again(
    airflow, metabase, capsys
):
    assert di.main(env_for(airflow, metabase)) == 0
    _, _, cards, boards = demo_items(metabase)
    dashboard = boards[ADVERTISING]
    trashed, deleted = cards["Spend"], cards["Spend by day and platform"]
    trashed.update(archived=True, archived_directly=True)  # moved to the trash in Metabase
    # Deleted for good from the trash, which takes its card off the dashboard.
    del metabase.cards[deleted["id"]]
    dashboard["dashcards"] = [d for d in dashboard["dashcards"] if d["card_id"] != deleted["id"]]
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    _, _, cards, boards = demo_items(metabase)
    assert boards[ADVERTISING] is dashboard
    assert cards["Spend"] is trashed
    assert cards["Spend by day and platform"]["id"] != deleted["id"]
    shown = [cards[q["name"]]["id"] for q in di.QUESTIONS if q["dashboard"] == di.ADVERTISING]
    assert sorted(d["card_id"] for d in dashboard["dashcards"]) == sorted(shown)
    out = capsys.readouterr().out.splitlines()
    assert "demo-init: restored from the trash: question demo/spend" in out
    assert 'demo-init: created again: question demo/spend-by-day "Spend by day and platform"' in out


def test_an_edited_question_is_put_back_by_the_next_start(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    card = card_of(metabase, "Spend by client")
    card["name"] = "My spend"
    card["description"] = "Changed in Metabase"
    card["dataset_query"]["native"]["query"] = "SELECT 1 AS spend_reporting"
    card["visualization_settings"]["series_settings"]["spend_reporting"]["color"] = "#000000"
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    question = QUESTIONS["by-client"]
    assert card["name"] == "Spend by client"
    assert card["description"] == question["description"]
    assert card["dataset_query"]["native"]["query"] == question["query"]
    series = card["visualization_settings"]["series_settings"]
    assert series["spend_reporting"]["color"] == di.SERIES_COLOR
    assert len(metabase.cards) == len(di.QUESTIONS)
    out = capsys.readouterr().out.splitlines()
    revert = "demo-init: revert of an edit: question demo/by-client"
    assert f'{revert} name: "My spend" -> "Spend by client"' in out
    assert f'{revert} description: "Changed in Metabase" -> "{question["description"]}"' in out
    assert f"{revert} query" in out
    assert f"{revert} visualization_settings" in out


def test_a_renamed_question_gets_its_name_back_and_no_copy_is_made(airflow, metabase):
    # demo-init finds its questions by the entity IDs that Metabase gave them, so a renamed one is
    # still the question of the demo.
    assert di.main(env_for(airflow, metabase)) == 0
    card = card_of(metabase, "Spend by day and platform")
    card["name"] = "My spend"
    assert di.main(env_for(airflow, metabase)) == 0
    assert card["name"] == "Spend by day and platform"
    assert len(metabase.cards) == len(di.QUESTIONS)


def test_an_edit_of_the_content_is_reverted_by_the_next_start_and_logged(airflow, metabase, capsys):
    # As an admin, or the AI client of one, could do: rename a question, put the dashboard in the
    # trash, and let All Users curate the demo.
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, cards, boards = demo_items(metabase)
    board = boards[WEB]
    cards["Sessions"]["name"] = "Edited by a test"
    board.update(archived=True, archived_directly=True)
    metabase.content_state.graph["1"][str(demo["id"])] = "write"
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    assert cards["Sessions"]["name"] == "Sessions"
    assert (board["archived"], board["collection_id"]) == (False, demo["id"])
    assert metabase.content_state.graph["1"][str(demo["id"])] == "read"
    out = capsys.readouterr().out.splitlines()
    assert (
        'demo-init: revert of an edit: question demo/sessions name: "Edited by a test" -> '
        '"Sessions"'
    ) in out
    assert "demo-init: restored from the trash: dashboard demo/web-analytics" in out
    assert 'demo-init: lowered: group "All Users" on collection "Demo": curate -> view' in out


def test_the_collection_of_v0_1_moves_into_agoranomos(airflow, metabase, capsys):
    # A Metabase of v0.1: the collection at the root with its questions and its dashboard, which
    # All Users may curate, all made by the account of demo-init; a question that a person added to
    # it; and a copy of a question of the demo that a person made in a collection of their own.
    content = metabase.content_state
    old, cards, board = an_earlier_demo(metabase)
    native = {"database": 2, "type": "native", "native": {"query": "SELECT 1"}}
    own = content.add_collection("Mine")
    added = content.add_card(
        {"name": "My question", "collection_id": old["id"], "dataset_query": native}
    )
    copied = content.add_card(
        {"name": "Spend by day", "collection_id": own["id"], "dataset_query": native}
    )
    before = copy.deepcopy(copied)
    assert di.main(env_for(airflow, metabase)) == 0
    root, demo, now_cards, boards = demo_items(metabase)
    assert (demo["id"], demo["name"], demo["location"]) == (old["id"], "Demo", f"/{root['id']}/")
    now_board = boards[ADVERTISING]
    assert now_board["id"] == board["id"], "the dashboard of v0.1 keeps its ID, and its links work"
    # The questions of v0.1 are those of the keys that the names of v0.1 map to.
    for name, key in di.V01_KEYS.items():
        assert now_cards[QUESTIONS[key]["name"]]["id"] == cards[name]["id"]
    # The question that the demo does not ship went to the root before the move; the copy in a
    # collection of a person stays as it is.
    assert added["collection_id"] is None
    assert copied == before
    assert len(metabase.cards) == len(di.QUESTIONS) + 2
    assert content.graph["1"][str(demo["id"])] == "read"
    out = capsys.readouterr().out.splitlines()
    assert (
        f"demo-init: moved out: question {added['id']} of the collection of v0.1 to the root, as "
        "the demo does not ship it"
    ) in out
    assert (
        f"demo-init: moved: collection {di.COLLECTION_V01!r} of an earlier release into "
        "Agoranomos as Demo"
    ) in out
    assert (
        'demo-init: lowered: group "All Users" on collection "Agoranomos demo": curate -> view'
    ) in out
    # The move is one line: no revert of the name or the place of the collection after it. The
    # questions of v0.1 read the mart of v0.1, and they are put back as the code of this release
    # says, in place; the code makes the questions that v0.1 did not have, and the second
    # dashboard.
    assert not [
        line
        for line in out
        if line.startswith("demo-init: revert of an edit: collection")
        and " description: " not in line
    ]
    for key in di.V01_KEYS.values():
        assert f"demo-init: revert of an edit: question demo/{key} query" in out
    created = [line for line in out if line.startswith("demo-init: created") and "demo/" in line]
    assert sorted(created) == sorted(
        [
            f'demo-init: created: question demo/{q["key"]} "{q["name"]}"'
            for q in di.QUESTIONS
            if q["key"] not in di.V01_KEYS.values()
        ]
        + [f'demo-init: created: dashboard demo/web-analytics "{WEB}"']
    )
    for question in di.QUESTIONS:
        query = now_cards[question["name"]]["dataset_query"]["native"]["query"]
        assert f"marts.{question['mart']}" in query
    assert registry_of(metabase)["demo/advertising"]["entity_id"] == board["entity_id"]


def test_a_decoy_of_the_old_name_that_another_account_made_is_not_moved(airflow, metabase, capsys):
    # On a Metabase of v0.1 any account may curate the root: one made a collection of the old name,
    # with a dashboard and a question of the names of v0.1 in it. Its dashboard is not one that the
    # account of demo-init made, so it is no demo of v0.1: it stays as it is, and the demo is new.
    content = metabase.content_state
    decoy = content.add_collection(di.COLLECTION_V01)
    board = content.add_dashboard(
        {"name": di.DASHBOARD_V01, "collection_id": decoy["id"], "creator_id": 7}
    )
    card = content.add_card({"name": "Spend by day", "collection_id": decoy["id"], "creator_id": 7})
    before = copy.deepcopy((decoy, board, card))
    assert di.main(env_for(airflow, metabase)) == 0
    assert (decoy, board, card) == before
    _, demo, cards, boards = demo_items(metabase)
    assert (demo["id"], boards[ADVERTISING]["id"]) != (decoy["id"], board["id"])
    assert card["id"] not in {c["id"] for c in cards.values()}
    out = capsys.readouterr().out.splitlines()
    assert (
        f"demo-init: not moved: collection {decoy['id']} named {di.COLLECTION_V01!r} at the root "
        f"holds no dashboard {di.DASHBOARD_V01!r} that the account of demo-init made, so it is not "
        "the demo of v0.1, and it stays where it is"
    ) in out
    assert not [line for line in out if line.startswith("demo-init: moved")]


def test_of_several_collections_of_v0_1_the_one_with_the_lowest_id_moves(airflow, metabase):
    # A decoy that another account made first, then the demo of v0.1, then a second one that the
    # account of demo-init made: the lowest of the two that qualify is the demo.
    content = metabase.content_state
    decoy = content.add_collection(di.COLLECTION_V01)
    content.add_dashboard({"name": di.DASHBOARD_V01, "collection_id": decoy["id"], "creator_id": 7})
    old, _, board = an_earlier_demo(metabase)
    later, _, _ = an_earlier_demo(metabase)
    # Metabase lists the collections in an order of its own: here the newest first.
    listed = list(content.collections.items())
    content.collections.clear()
    content.collections.update(reversed(listed))
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, _, boards = demo_items(metabase)
    assert (demo["id"], boards[ADVERTISING]["id"]) == (old["id"], board["id"])
    for other in (decoy, later):
        assert (other["name"], other["location"]) == (di.COLLECTION_V01, "/")


def test_what_the_demo_does_not_ship_leaves_the_collection_of_v0_1_for_the_root(
    airflow, metabase, capsys
):
    # Before the move, every item of the collection of v0.1 that the demo does not adopt goes to the
    # root: what another account made, a question of a name of v0.1 included, a model, a dashboard,
    # a sub-collection, and a question saved into the dashboard of v0.1. A question of a dashboard
    # that goes moves with it. Only the content of the package enters Agoranomos.
    content = metabase.content_state
    old, cards, board = an_earlier_demo(metabase)
    theirs_board = content.add_dashboard(
        {"name": "Their board", "collection_id": old["id"], "creator_id": 7}
    )
    theirs = {
        "card": content.add_card(
            {"name": "Spend by day", "collection_id": old["id"], "creator_id": 7}
        ),
        "dataset": content.add_card(
            {"name": "A model", "collection_id": old["id"], "type": "model"}
        ),
        "dashboard": theirs_board,
        "dashboard of the old name": content.add_dashboard(
            {"name": di.DASHBOARD_V01, "collection_id": old["id"], "creator_id": 7}
        ),
        "on the dashboard of v0.1": content.add_card(
            {"name": "Extra", "collection_id": old["id"], "dashboard_id": board["id"]}
        ),
    }
    notes = content.add_collection("Notes", parent_id=old["id"])
    inside = content.add_card(
        {"name": "Inside", "collection_id": old["id"], "dashboard_id": theirs_board["id"]}
    )
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, now_cards, boards = demo_items(metabase)
    assert boards[ADVERTISING]["id"] == board["id"]
    assert {c["id"] for c in cards.values()} <= {c["id"] for c in now_cards.values()}
    for item in theirs.values():
        assert item["collection_id"] is None, item["name"]
    assert theirs["on the dashboard of v0.1"]["dashboard_id"] is None
    assert (inside["collection_id"], inside["dashboard_id"]) == (None, theirs_board["id"])
    assert notes["location"] == "/"
    inside_demo = [c for c in metabase.cards.values() if c["collection_id"] == demo["id"]]
    assert sorted(c["id"] for c in inside_demo) == sorted(c["id"] for c in now_cards.values())
    out = capsys.readouterr().out.splitlines()
    # One line for each, by its model and its ID; the question of their dashboard moved with it.
    taken_out = [
        ("question", theirs["card"]),
        ("model", theirs["dataset"]),
        ("dashboard", theirs_board),
        ("dashboard", theirs["dashboard of the old name"]),
        ("question", theirs["on the dashboard of v0.1"]),
        ("collection", notes),
    ]
    assert sorted(line for line in out if line.startswith("demo-init: moved out: ")) == sorted(
        f"demo-init: moved out: {model} {item['id']} of the collection of v0.1 to the root, as "
        "the demo does not ship it"
        for model, item in taken_out
    )
    # No name that another account chose reaches the log.
    for name in ("Their board", "A model", "Extra", "Notes", "Inside"):
        assert not [line for line in out if name in line], name


def test_what_is_in_the_trash_of_the_collection_of_v0_1_goes_to_the_root_too(
    airflow, metabase, capsys
):
    # A question and a dashboard that a person put in the trash from the collection of v0.1, and a
    # sub-collection in the trash with a question in it: restored later, they would land in Demo.
    # Each goes to the root and stays in the trash; the question goes with its sub-collection.
    content = metabase.content_state
    old, _, _ = an_earlier_demo(metabase)
    trashed = content.add_card({"name": "Mine", "collection_id": old["id"], "creator_id": 7})
    board = content.add_dashboard({"name": "Theirs", "collection_id": old["id"], "creator_id": 7})
    for item in (trashed, board):
        item.update(archived=True, archived_directly=True)
    notes = content.add_collection("Notes", parent_id=old["id"])
    inside = content.add_card({"name": "Inside", "collection_id": notes["id"]})
    notes["archived"] = True
    inside["archived"] = True  # went to the trash with its collection
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, _, _ = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert (trashed["collection_id"], trashed["archived"]) == (None, True)
    assert (board["collection_id"], board["archived"]) == (None, True)
    assert (notes["location"], notes["archived"]) == ("/", True)
    assert inside["collection_id"] == notes["id"]
    out = capsys.readouterr().out.splitlines()
    for model, item in (("question", trashed), ("dashboard", board), ("collection", notes)):
        assert (
            f"demo-init: moved out: {model} {item['id']} of the collection of v0.1, in the trash, "
            "to the root, as the demo does not ship it"
        ) in out


def test_a_question_of_v0_1_in_the_trash_is_not_adopted(airflow, metabase):
    # A question of v0.1 that a person put in the trash goes to the root with the rest, and stays in
    # the trash there; the code makes its own question.
    _, cards, _ = an_earlier_demo(metabase)
    trashed = cards["Spend by day"]
    trashed.update(archived=True, archived_directly=True)
    assert di.main(env_for(airflow, metabase)) == 0
    assert registered(metabase, "demo/spend-by-day")["id"] != trashed["id"]
    assert (trashed["collection_id"], trashed["archived"]) == (None, True)


def lock_of(content: ContentState, collection_id: int, plant) -> dict:
    """Run plant() once, just before Metabase applies the first PUT of the graph that names a
    collection: what an account that may still curate it could add before the lock lands."""
    handle = content.handle
    done: dict = {}

    def racing(method, path, query, body):
        locking = (method, path) == ("PUT", "/api/collection/graph") and any(
            str(collection_id) in perms for perms in (body or {}).get("groups", {}).values()
        )
        if locking and not done:
            done.update(plant())
        return handle(method, path, query, body)

    content.handle = racing
    return done


# What an account of All Users can add to the collection of v0.1 before demo-init locks it, of
# each kind: a question, one that it puts in the trash at once, a sub-collection, a timeline and a
# dashboard.
def strays(content: ContentState, old: dict) -> dict[str, Callable[[], dict]]:
    def trashed() -> dict:
        card = content.add_card({"name": "Gone", "collection_id": old["id"]})
        card.update(archived=True, archived_directly=True)
        return card

    return {
        "question": lambda: content.add_card({"name": "Late", "collection_id": old["id"]}),
        "question in the trash": trashed,
        "collection": lambda: content.add_collection("Late notes", parent_id=old["id"]),
        "timeline": lambda: content.add_timeline(old["id"]),
        "dashboard": lambda: content.add_dashboard({"name": "Late", "collection_id": old["id"]}),
    }


@pytest.mark.parametrize(
    "kind", ["question", "question in the trash", "collection", "timeline", "dashboard"]
)
def test_what_reaches_the_collection_before_its_lock_stops_the_job_and_keeps_it_at_the_root(
    airflow, metabase, capsys, kind
):
    # Another account adds an item to the collection of v0.1 after demo-init listed it and before
    # its lock lands. The listing after the lock finds it: the job stops in one line, and the
    # collection stays at the root, view-only, unknown to the registry. The next start takes the
    # item out and moves the collection.
    content = metabase.content_state
    old, cards, _ = an_earlier_demo(metabase)
    late = lock_of(content, old["id"], lambda: {"item": strays(content, old)[kind]()})
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys, after_apply=True)
    word = kind.split()[0]
    assert line == (
        f"demo-init: the collection of v0.1 got a {word} {late['item']['id']} before demo-init "
        "could lock it, which the demo does not ship: it stays at the root, view-only, and the "
        "next start takes that out and moves it"
    )
    assert (old["name"], old["location"]) == (di.COLLECTION_V01, "/")
    assert content.graph["1"][str(old["id"])] == "read"
    assert not [k for k in registry_keys(metabase) if k == "demo" or k.startswith("demo/")]
    del content.handle
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, now_cards, _ = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert {c["id"] for c in cards.values()} <= {c["id"] for c in now_cards.values()}
    item = late["item"]
    assert item.get("collection_id", "no collection") is None or item.get("location") == "/"


def test_a_failure_after_the_lock_leaves_the_collection_of_v0_1_to_be_checked_again(
    airflow, metabase, capsys
):
    # An item reaches the collection before its lock, and the listing after the lock fails, as
    # with a failure of Metabase or a job that stops: the registry does not know the demo yet, so
    # the next start lists the collection again, finds the item and takes it out.
    content = metabase.content_state
    old, _, _ = an_earlier_demo(metabase)
    late = lock_of(content, old["id"], lambda: {"item": strays(content, old)["question"]()})
    handle = content.handle
    calls = {"after the lock": 0}

    def failing(method, path, query, body):
        if late and (method, path) == ("GET", "/api/card") and not calls["after the lock"]:
            calls["after the lock"] += 1
            return 500, {"message": "a failure of Metabase"}
        return handle(method, path, query, body)

    content.handle = failing
    assert di.main(env_for(airflow, metabase)) == 1
    assert "GET /api/card?f=archived failed with HTTP 500" in one_line(capsys, after_apply=True)
    assert (old["name"], old["location"]) == (di.COLLECTION_V01, "/")
    assert not [k for k in registry_keys(metabase) if k == "demo" or k.startswith("demo/")]
    del content.handle
    assert di.main(env_for(airflow, metabase)) == 0
    assert late["item"]["collection_id"] is None
    _, demo, _, _ = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert (
        f"demo-init: moved out: question {late['item']['id']} of the collection of v0.1 to the "
        "root, as the demo does not ship it"
    ) in capsys.readouterr().out.splitlines()


def test_what_reaches_a_new_demo_before_its_lock_stops_the_job_and_goes_to_the_trash(
    airflow, metabase, capsys
):
    # A first start, as when the demo is turned on after people exist: an account of All Users
    # saves a question into the new Demo right after Metabase made it, before its lock. The job
    # stops in one line, the new Demo goes to the trash with the question, and the next start
    # makes it again.
    content = metabase.content_state
    handle = content.handle
    planted: dict = {}

    def watching(method, path, query, body):
        answer = handle(method, path, query, body)
        if (method, path) == ("POST", "/api/collection") and body.get("name") == "Demo":
            planted.setdefault(
                "card", content.add_card({"name": "Mine", "collection_id": answer[1]["id"]})
            )
        return answer

    content.handle = watching
    assert di.main(env_for(airflow, metabase)) == 1
    assert one_line(capsys, after_apply=True) == (
        f"demo-init: the new collection demo got a question {planted['card']['id']} before its "
        "lock, which the package does not ship: both are in the trash now, and the next start "
        "makes the collection again"
    )
    [first] = [c for c in content.collections.values() if c["name"] == "Demo"]
    assert first["archived"]
    del content.handle
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, cards, _ = demo_items(metabase)
    assert demo["id"] != first["id"]
    assert planted["card"]["id"] not in {c["id"] for c in cards.values()}


def test_a_collection_planted_in_a_new_agoranomos_during_the_move_stops_the_job(
    airflow, metabase, capsys
):
    # The upgrade from v0.1: an account of All Users makes a collection inside Agoranomos right
    # after Metabase made it, before its lock. The job stops in one line; the demo of v0.1, which
    # the registry knows by then, goes into the Agoranomos of the next start.
    content = metabase.content_state
    old, _, _ = an_earlier_demo(metabase)
    handle = content.handle
    planted: dict = {}

    def watching(method, path, query, body):
        answer = handle(method, path, query, body)
        if (method, path) == ("POST", "/api/collection") and body.get("name") == "Agoranomos":
            planted.setdefault(
                "notes", content.add_collection("Official notes", parent_id=answer[1]["id"])
            )
        return answer

    content.handle = watching
    assert di.main(env_for(airflow, metabase)) == 1
    assert one_line(capsys, after_apply=True) == (
        f"demo-init: the new collection agoranomos got a collection {planted['notes']['id']} "
        "before its lock, which the package does not ship: both are in the trash now, and the "
        "next start makes the collection again"
    )
    del content.handle
    assert di.main(env_for(airflow, metabase)) == 0
    root, demo, _, _ = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert not planted["notes"]["location"].startswith(f"/{root['id']}/")
    assert "registry" not in planted


def test_the_timelines_of_the_collection_of_v0_1_go_to_the_root_with_their_events(
    airflow, metabase, capsys
):
    # The events of a timeline show on the charts of its collection: a timeline goes to the root
    # with its events, also one in the trash, which stays there.
    content = metabase.content_state
    old, _, _ = an_earlier_demo(metabase)
    live = content.add_timeline(old["id"])
    trashed = content.add_timeline(old["id"], archived=True)
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, _, _ = demo_items(metabase)
    assert demo["id"] == old["id"]
    assert (live["collection_id"], live["events"]) == (None, [{"name": "An event"}])
    assert (trashed["collection_id"], trashed["archived"]) == (None, True)
    out = capsys.readouterr().out.splitlines()
    assert (
        f"demo-init: moved out: timeline {live['id']} of the collection of v0.1 to the root, as "
        "the demo does not ship it"
    ) in out
    assert (
        f"demo-init: moved out: timeline {trashed['id']} of the collection of v0.1, in the trash, "
        "to the root, as the demo does not ship it"
    ) in out


# What demo-init cannot take out of the collection of v0.1, as each comes to it: a document, one
# in the trash, which Metabase lists in the trash only, and a pulse.
def a_pulse(content: ContentState, collection_id: int) -> dict[str, Any]:
    pulse = {"id": 900, "model": "pulse", "name": "Weekly", "collection_id": collection_id}
    content.others.append(pulse)
    return pulse


KEPT_BY = {
    "document": lambda content, old: content.add_document(old["id"]),
    "document in the trash": lambda content, old: content.add_document(
        old["id"], archived=True, archived_directly=True
    ),
    "pulse": lambda content, old: a_pulse(content, old["id"]),
}


@pytest.mark.parametrize("kind", KEPT_BY)
def test_an_item_that_demo_init_cannot_take_out_keeps_the_collection_of_v0_1_where_it_is(
    airflow, metabase, capsys, kind
):
    content = metabase.content_state
    old, cards, _ = an_earlier_demo(metabase)
    item = KEPT_BY[kind](content, old)
    before = copy.deepcopy((old, cards))
    assert di.main(env_for(airflow, metabase)) == 0
    assert (old, cards) == before
    _, demo, _, _ = demo_items(metabase)
    assert demo["id"] != old["id"]
    model = kind.split()[0]
    out = capsys.readouterr().out.splitlines()
    assert (
        f"demo-init: not moved: collection {old['id']} named {di.COLLECTION_V01!r} at the root "
        f"holds a {model} {item['id']} that demo-init cannot take out first, and it stays where "
        "it is"
    ) in out
    assert not [line for line in out if "A document" in line or "Weekly" in line]


def test_of_two_questions_of_one_name_in_the_collection_of_v0_1_the_older_is_the_demos(
    airflow, metabase, monkeypatch
):
    # A question of v0.1 and a newer one of the same name next to it, listed newest first: the
    # older one, which the dashboard of v0.1 shows, is the question of the demo, and the newer one
    # goes to the root.
    content = metabase.content_state
    old, cards, board = an_earlier_demo(metabase)
    newer = content.add_card({"name": "Spend by day", "collection_id": old["id"]})
    listed = content.items

    def newest_first(*arguments):
        return {"data": listed(*arguments)["data"][::-1]}

    monkeypatch.setattr(content, "items", newest_first)
    assert di.main(env_for(airflow, metabase)) == 0
    assert registered(metabase, "demo/spend-by-day") is cards["Spend by day"]
    assert (newer["name"], newer["collection_id"]) == ("Spend by day", None)
    assert newer["id"] not in {d["card_id"] for d in board["dashcards"]}


def test_a_question_of_v0_1_that_a_person_renamed_goes_to_the_root_and_the_code_makes_its_own(
    airflow, metabase, capsys
):
    _, cards, _ = an_earlier_demo(metabase)
    renamed = cards["Spend by day"]
    renamed["name"] = "Daily spend"
    assert di.main(env_for(airflow, metabase)) == 0
    _, _, now_cards, boards = demo_items(metabase)
    assert (renamed["name"], renamed["collection_id"]) == ("Daily spend", None)
    assert now_cards["Spend by day and platform"]["id"] != renamed["id"]
    assert renamed["id"] not in {d["card_id"] for d in boards[ADVERTISING]["dashcards"]}
    assert 'demo-init: created: question demo/spend-by-day "Spend by day and platform"' in (
        capsys.readouterr().out.splitlines()
    )


# The types that Metabase 0.64.1 gave the columns of the answers of the questions of the demo on
# the stand, in October 2026: the base type, the type of the database, and the semantic type that
# it gives by the name of a column, which it leaves out of the metadata that it computes when it
# gives none. Every other column of the answers is a sum or a ratio.
STAND_TYPES = {
    "report_date": ("type/Date", "date", None),
    "is_primary": ("type/Boolean", "bool", None),
    "click_through_rate": ("type/Decimal", "numeric", "type/Share"),
    "engagement_rate": ("type/Decimal", "numeric", "type/Share"),
    "cost_per_conversion": ("type/Decimal", "numeric", "type/Cost"),
    "session_source": ("type/Text", "text", "type/Source"),
    "session_default_channel_group": ("type/Text", "text", "type/Source"),
    **{
        name: ("type/Text", "text", None)
        for name in (
            "platform_name",
            "client_name",
            "campaign_name",
            "conversion_action_name",
            "conversion_category",
            "session_medium",
            "session_campaign_name",
        )
    },
}
STAND_NUMBER = ("type/Decimal", "numeric", None)
# The statistics of a run, by the base type of a column.
FINGERPRINTS = {
    "type/Date": {"type/DateTime": {"earliest": "2026-08-18", "latest": "2026-10-12"}},
    "type/Text": {"type/Text": {"average-length": 12.5, "percent-json": 0.0}},
    "type/Decimal": {"type/Number": {"min": 6664.71, "q1": 6664.71, "skewness": None}},
}


def metadata_on_the_stand(name: str, after_a_run: bool) -> dict[str, Any]:
    """The metadata of a column of an answer as Metabase 0.64.1 made it on the stand: computed from
    the query, or recorded after a run of the question, with the keys and statistics of a run."""
    base_type, database_type, semantic_type = STAND_TYPES.get(name, STAND_NUMBER)
    column = {
        "name": name,
        "database_type": database_type,
        "base_type": base_type,
        "display_name": name,
        "field_ref": ["field", name, {"base-type": base_type}],
    }
    if semantic_type is not None:
        column["semantic_type"] = semantic_type
    if after_a_run:
        aliases = (
            "deduplicated-name",
            "original-name",
            "source-column-alias",
            "desired-column-alias",
        )
        column |= {f"lib/{alias}": name for alias in aliases}
        column |= {"effective_type": base_type, "semantic_type": semantic_type, "source": "native"}
        column |= {"lib/source": "source/native"}
        column["fingerprint"] = {"global": {"distinct-count": 56, "nil%": 0.0}}
        if base_type in FINGERPRINTS:
            column["fingerprint"]["type"] = FINGERPRINTS[base_type]
    return column


@pytest.mark.parametrize("after_a_run", [False, True], ids=["computed", "recorded after a run"])
def test_the_metadata_that_metabase_makes_for_the_demo_is_left_as_it_is(
    airflow, metabase, after_a_run
):
    # A start that finds the metadata of the columns of the questions as Metabase made it changes
    # nothing, also after a view of the dashboard has run them.
    assert di.main(env_for(airflow, metabase)) == 0
    for question in di.QUESTIONS:
        card_of(metabase, question["name"])["result_metadata"] = [
            metadata_on_the_stand(name, after_a_run) for name in question["columns"]
        ]
    writes = len(metabase.bodies)
    assert di.main(env_for(airflow, metabase)) == 0
    assert [b for b in metabase.bodies[writes:] if b[1] != "/api/session"] == []


def test_a_click_behavior_added_to_a_card_of_the_demo_dashboard_is_reverted(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    board = board_of(metabase)
    template = "https://example.com/{{spend_reporting}}"
    link = {"type": "link", "linkType": "url", "linkTemplate": template}
    board["dashcards"][0]["visualization_settings"] = {"click_behavior": link}
    assert di.main(env_for(airflow, metabase)) == 0
    assert [d["visualization_settings"] for d in board["dashcards"]] == [{}] * len(
        board["dashcards"]
    )
    assert len(board["dashcards"]) == len(
        [q for q in di.QUESTIONS if q["dashboard"] == di.ADVERTISING]
    )


def test_an_extra_key_in_the_settings_of_a_question_of_the_demo_is_reverted(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    settings_of(metabase, "Spend by client")["graph.show_values"] = True
    assert di.main(env_for(airflow, metabase)) == 0
    wanted = di.visualization_settings(QUESTIONS["by-client"], TITLES[di.AD_STATS], "USD")
    assert settings_of(metabase, "Spend by client") == wanted


def test_a_second_root_collection_of_the_old_name_is_left_alone_once_the_demo_is_found(
    airflow, metabase
):
    assert di.main(env_for(airflow, metabase)) == 0
    other = metabase.content_state.add_collection(di.COLLECTION_V01)
    assert di.main(env_for(airflow, metabase)) == 0
    assert (other["location"], other["name"]) == ("/", di.COLLECTION_V01)


def test_a_demo_of_v0_1_that_a_restore_brings_back_moves_again(airflow, metabase, capsys):
    # A start of this release, then the way back from the upgrade: the Metabase of v0.1 restored
    # from its backup, while the registry of the content, which no backup holds, stays. The entity
    # IDs of the registry match nothing there, so the next start moves the collection of v0.1.
    assert di.main(env_for(airflow, metabase)) == 0
    metabase.content_state = ContentState()
    # Metabase makes entity IDs at random; the stand-in makes them from its serial numbers, which
    # move on here, so that the IDs of the restored Metabase are not those of the registry.
    metabase.content_state.serial = 500
    old, cards, board = an_earlier_demo(metabase)
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    _, demo, now_cards, boards = demo_items(metabase)
    assert (demo["id"], boards[ADVERTISING]["id"]) == (old["id"], board["id"])
    assert {c["id"] for c in cards.values()} <= {c["id"] for c in now_cards.values()}
    out = capsys.readouterr().out.splitlines()
    assert any(line.startswith("demo-init: moved: collection ") for line in out)
    assert 'demo-init: created again: collection agoranomos "Agoranomos"' in out


def test_a_registry_that_is_no_registry_stops_demo_init_before_it_writes(airflow, metabase, capsys):
    path = Path(metabase.files["CONTENT_REGISTRY"])
    path.write_text("{", encoding="utf-8")
    assert di.main(env_for(airflow, metabase)) == 1
    assert one_line(capsys) == (
        f"demo-init: {path} is not a registry of the content of format 1: move it away and run "
        "./agoranomos up, which creates the content again next to the old one"
    )
    assert airflow.calls == []
    assert metabase.calls == []


def test_a_registry_that_cannot_be_written_stops_demo_init_in_one_line(
    airflow, metabase, tmp_path, capsys
):
    path = tmp_path / "no-such-folder" / "content.json"
    assert di.main(env_for(airflow, metabase, CONTENT_REGISTRY=str(path))) == 1
    assert one_line(capsys, after_apply=True) == (
        f"demo-init: cannot write {path}: FileNotFoundError. The volume metabase-content must be "
        "writable by the user 65534 of the job"
    )


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
    allowed = set(sys.stdlib_module_names) | {"content", "metabase_init", "metadata"}
    assert "content" in imported
    assert imported <= allowed, sorted(imported - allowed)


def settings_of(metabase: FakeMetabase, name: str) -> dict[str, Any]:
    [card] = [card for card in metabase.cards.values() if card["name"] == name]
    return card["visualization_settings"]


def test_new_questions_get_the_titles_of_their_columns_and_axes(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    by_client = settings_of(metabase, "Spend by client")
    assert by_client["column_settings"] == {
        '["name","client_name"]': {"column_title": "Client"},
        '["name","spend_reporting"]': {
            "column_title": "Spend in Reporting Currency",
            "number_style": "currency",
            "currency": "USD",
        },
    }
    assert by_client["graph.x_axis.title_text"] == "Client"
    assert by_client["graph.y_axis.title_text"] == "Spend in Reporting Currency"
    by_day = settings_of(metabase, "Sessions by day and channel group")
    assert by_day["graph.x_axis.title_text"] == "Report Date"
    assert by_day["graph.y_axis.title_text"] == "Sessions"
    # The ratios, which are no column of a mart, get the titles of the code.
    by_platform = settings_of(metabase, "Results by platform")["column_settings"]
    assert by_platform['["name","click_through_rate"]'] == {
        "column_title": "CTR",
        "number_style": "percent",
        "decimals": 2,
    }
    assert by_platform['["name","roas"]'] == {"column_title": "ROAS", "decimals": 2}
    # A number has no axes.
    assert settings_of(metabase, "Spend") == {
        "column_settings": {
            '["name","spend_reporting"]': {
                "column_title": "Spend in Reporting Currency",
                "number_style": "currency",
                "currency": "USD",
            }
        }
    }


def test_the_money_is_in_the_reporting_currency_of_the_metadata(airflow, metabase, tmp_path):
    # metadata-export renders the reporting currency of the deployment into the currency of the
    # money of the marts, and the questions show their money in it.
    export = copy.deepcopy(DEMO_EXPORT)
    for model in export["models"]:
        for column in model["columns"]:
            meta = column["meta"].get("metabase", {})
            if meta.get("currency") == "USD":
                meta["currency"] = "EUR"
    metabase.files.update(metadata_env(tmp_path, export=export))
    assert di.main(env_for(airflow, metabase)) == 0
    for question in di.QUESTIONS:
        settings = settings_of(metabase, question["name"])["column_settings"]
        for name in question["columns"]:
            shown = settings.get(f'["name","{name}"]', {})
            assert shown.get("currency") == ("EUR" if name in di.MONEY else None), name


def test_the_titles_come_from_the_overrides_of_the_deployment(airflow, metabase, tmp_path):
    overrides = "[marts.ad_stats_campaign_daily.columns.spend_reporting]\ndisplay_name = 'Cost'\n"
    metabase.files.update(metadata_env(tmp_path, export=DEMO_EXPORT, overrides=overrides))
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, "Spend by client")["graph.y_axis.title_text"] == "Cost"
    assert metabase.field("spend_reporting")["display_name"] == "Cost"


def test_the_titles_of_a_question_are_put_back_as_the_code_says(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by client")
    settings["graph.x_axis.title_text"] = "Brand"  # a title of a person
    del settings["graph.y_axis.title_text"]
    spend = settings["column_settings"]['["name","spend_reporting"]']
    del spend["column_title"]
    spend["decimals"] = 0  # a format of a person, in the settings of a column that the code sets
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    wanted = di.visualization_settings(QUESTIONS["by-client"], TITLES[di.AD_STATS], "USD")
    assert settings_of(metabase, "Spend by client") == wanted
    assert wanted["graph.x_axis.title_text"] == "Client"
    assert capsys.readouterr().out.splitlines() == [
        "demo-init: revert of an edit: question demo/by-client visualization_settings",
        done_line(metabase, "ran before"),
    ]


def test_a_display_name_that_changes_reaches_the_titles_at_the_next_start(
    airflow, metabase, tmp_path
):
    # The titles come from the effective metadata on every start, so a new display name in the
    # overrides of the deployment reaches the questions too, and not only those made after it.
    assert di.main(env_for(airflow, metabase)) == 0
    overrides = "[marts.ad_stats_campaign_daily.columns.spend_reporting]\ndisplay_name = 'Cost'\n"
    metabase.files.update(metadata_env(tmp_path, export=DEMO_EXPORT, overrides=overrides))
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by client")
    assert settings["graph.y_axis.title_text"] == "Cost"
    assert settings["column_settings"]['["name","spend_reporting"]'] == {
        "column_title": "Cost",
        "number_style": "currency",
        "currency": "USD",
    }


# The charts with one series, which the color of the project draws, and those with a series for each
# value of their breakout, which keep the colors of Metabase.
CHARTS = ("Spend by client",)
BREAKOUTS = ("Spend by day and platform", "Sessions by day and channel group")


def test_the_charts_are_drawn_in_the_first_color_of_the_project(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    for name in CHARTS:
        # The series of a chart with the one metric spend_reporting is keyed by the name of its
        # column.
        assert settings_of(metabase, name)["series_settings"] == {
            "spend_reporting": {"color": "#A86400"}
        }
    # A chart with a breakout, and a number, have no series settings.
    for name in (*BREAKOUTS, "Spend"):
        assert "series_settings" not in settings_of(metabase, name), name


@pytest.mark.parametrize("name", CHARTS)
def test_a_chart_without_the_color_gets_it_back(airflow, metabase, name):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, name)
    # A title of a person on the series, in place of the color.
    settings["series_settings"] = {"spend_reporting": {"title": "Cost"}}
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, name)
    assert settings["series_settings"] == {"spend_reporting": {"color": "#A86400"}}
    assert settings["graph.metrics"] == ["spend_reporting"]
    del settings["series_settings"]
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {
        "spend_reporting": {"color": "#A86400"}
    }


@pytest.mark.parametrize("name", CHARTS)
@pytest.mark.parametrize("color", ["#4F6382", "#a86400", "red", ""])
def test_a_color_that_a_person_set_is_put_back(airflow, metabase, capsys, name, color):
    assert di.main(env_for(airflow, metabase)) == 0
    series = settings_of(metabase, name)["series_settings"]
    series["spend_reporting"] = {"color": color, "color_name": "x"}
    writes_before = len(metabase.bodies)
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {
        "spend_reporting": {"color": "#A86400"}
    }
    # One write: the settings of the question, and its line.
    [(method, path, _)] = [b for b in metabase.bodies[writes_before:] if b[1] != "/api/session"]
    assert (method, path) == ("PUT", f"/api/card/{card_of(metabase, name)['id']}")
    key = next(q["key"] for q in di.QUESTIONS if q["name"] == name)
    assert (
        f"demo-init: revert of an edit: question demo/{key} visualization_settings"
        in capsys.readouterr().out.splitlines()
    )


def test_the_series_settings_of_a_person_give_way_to_those_of_the_code(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    settings = settings_of(metabase, "Spend by client")
    settings["series_settings"] = {
        "other": {"color": "#000000"},
        "spend_reporting": {"title": "Cost"},
    }
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, "Spend by client")["series_settings"] == {
        "spend_reporting": {"color": "#A86400"}
    }


@pytest.mark.parametrize("name", CHARTS)
@pytest.mark.parametrize("others", [{}, {"other": {"color": "#000000"}}])
def test_a_chart_whose_series_settings_lack_its_metric_gets_those_of_the_code(
    airflow, metabase, name, others
):
    assert di.main(env_for(airflow, metabase)) == 0
    settings_of(metabase, name)["series_settings"] = dict(others)
    assert di.main(env_for(airflow, metabase)) == 0
    assert settings_of(metabase, name)["series_settings"] == {
        "spend_reporting": {"color": "#A86400"}
    }


def test_each_question_filters_on_the_fields_of_the_mart_that_it_reads(airflow, metabase):
    # Each question has a field filter for each filter of its dashboard, on the field of the column
    # of the mart that it reads, with the base type that Metabase gives that field.
    assert di.main(env_for(airflow, metabase)) == 0
    fields = {
        (table["name"], f["name"]): f for table in metabase.all_tables() for f in table["fields"]
    }
    for question in di.QUESTIONS:
        native = card_of(metabase, question["name"])["dataset_query"]["native"]
        names = di.QUESTION_FILTERS[question["dashboard"]]
        assert list(native["template-tags"]) == list(names), question["key"]
        for name in names:
            display_name, widget_type, column = di.FILTERS[name]
            field = fields[(question["mart"], column)]
            assert native["template-tags"][name] == {
                "id": content.tag_id("demo", question["key"], name),
                "name": name,
                "display-name": display_name,
                "type": "dimension",
                "dimension": ["field", field["id"], {"base-type": field["base_type"]}],
                "widget-type": widget_type,
            }
            assert f"{{{{{name}}}}}" in native["query"], (question["key"], name)
        # The parameters stay empty: Metabase shows the filters from the template tags.
        assert card_of(metabase, question["name"])["parameters"] == []


def test_each_dashboard_sets_the_filters_of_each_of_its_cards(airflow, metabase):
    assert di.main(env_for(airflow, metabase)) == 0
    for key, (name, _) in di.DASHBOARDS.items():
        board = board_of(metabase, name)
        slugs = [f["slug"] for f in board["parameters"]]
        assert slugs == list(di.QUESTION_FILTERS[key])
        period = next(f for f in board["parameters"] if f["slug"] == "period")
        assert (period["type"], period["default"]) == ("date/all-options", "past30days")
        for dashcard in board["dashcards"]:
            mappings = dashcard["parameter_mappings"]
            assert [m["parameter_id"] for m in mappings] == slugs
            for mapping in mappings:
                assert mapping["card_id"] == dashcard["card_id"]
                assert mapping["target"] == ["dimension", ["template-tag", mapping["parameter_id"]]]


def test_a_filter_that_a_person_changed_on_a_dashboard_is_put_back(airflow, metabase, capsys):
    # A person changes the default of the period, drops the filter of the platform, and unhooks a
    # card from the filter of the client.
    assert di.main(env_for(airflow, metabase)) == 0
    board = board_of(metabase)
    board["parameters"][0]["default"] = "past7days"
    board["parameters"] = [f for f in board["parameters"] if f["slug"] != "platform"]
    board["dashcards"][0]["parameter_mappings"] = []
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    filters = [content.filter_shape(di.DASHBOARD_FILTERS[f]) for f in di.AD_FILTERS]
    assert board["parameters"] == filters
    assert all(len(d["parameter_mappings"]) == len(filters) for d in board["dashcards"])
    assert (
        "demo-init: revert of an edit: dashboard demo/advertising cards: 9 cards (1 not as the "
        "code says) and 2 filters (1 not as the code says) -> 9 cards and 3 filters"
    ) in capsys.readouterr().out.splitlines()


def test_a_template_tag_that_a_person_changed_is_put_back(airflow, metabase, capsys):
    assert di.main(env_for(airflow, metabase)) == 0
    card = card_of(metabase, "Sessions")
    card["dataset_query"]["native"]["template-tags"]["client"]["widget-type"] = "string/contains"
    capsys.readouterr()
    assert di.main(env_for(airflow, metabase)) == 0
    tag = card["dataset_query"]["native"]["template-tags"]["client"]
    assert tag["widget-type"] == "string/="
    assert "demo-init: revert of an edit: question demo/sessions query" in (
        capsys.readouterr().out.splitlines()
    )


def test_metabase_gives_the_columns_of_the_answers_the_semantic_types_of_the_code():
    # Metabase gives a column of an answer a semantic type by its name each time that it computes
    # the metadata of the question: the code expects that type, or a start would take the metadata
    # that Metabase made for an edit, and put it back on every start.
    from test_dbt_project import METABASE_KINDS, METABASE_NAME_TYPES

    kinds_of = {**METABASE_KINDS, "bool": METABASE_KINDS["boolean"]}
    for question in di.QUESTIONS:
        for column in di.answer_columns(question):
            kinds = kinds_of[column.database_type]
            named = next(
                (
                    given
                    for pattern, kind, given in METABASE_NAME_TYPES
                    if kind in kinds and re.search(pattern, column.name)
                ),
                None,
            )
            assert column.semantic_type == named, (question["key"], column.name)


def test_a_mistake_in_the_overrides_stops_demo_init_before_it_writes(
    airflow, metabase, tmp_path, capsys
):
    overrides = "[marts.ad_stats_campaign_daily]\ndisplay_name = 7\n"
    metabase.files.update(metadata_env(tmp_path, overrides=overrides))
    assert di.main(env_for(airflow, metabase)) == 1
    line = one_line(capsys)
    assert line == (
        "demo-init: overrides.toml: marts.ad_stats_campaign_daily.display_name: a text in quotes "
        "is expected, not int"
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
    daily, *applied = output.out.splitlines()
    assert daily == DAILY_LINE
    assert (
        'demo-init: first fill: marts.ad_stats_campaign_daily display_name: "Ad Stats Campaign '
        'Daily" -> "Ad Statistics by Campaign and Day"'
    ) in applied
    assert all(line.startswith("demo-init: first fill: ") for line in applied), applied
    [line] = output.err.splitlines()
    assert line.startswith("demo-init: POST /api/collection failed with HTTP 500")
    assert metabase.mart_table()["display_name"] == "Ad Statistics by Campaign and Day"
    assert not leaked(output.out + output.err)


def test_the_lines_of_the_metadata_reach_the_log_of_a_process_before_its_error(airflow, metabase):
    # As a container runs it, with its standard output a pipe that Python buffers: each line is
    # flushed as it is printed, so the log has the lines of the metadata before the line of the
    # error. The pipeline and the daily load ran before and Metabase knows the mart, so the
    # process waits for nothing.
    airflow.add_run(di.LOAD_DAG, "success", at=-20)
    airflow.add_run(di.TRANSFORM_DAG, "success", at=-10)
    airflow.add_run(di.DAILY_DAG, "success", "scheduled", at=-5)
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
