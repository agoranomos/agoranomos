"""The demo of Agoranomos, made ready by the one-shot demo-init service on every start of the demo
profile.

It runs the demo pipeline once: agoranomos_demo_load, and then agoranomos_demo_transform, which the
asset of the raw table starts. It also runs agoranomos_demo_history once, which loads the history
of the fictional platforms of the demo generator into raw_demo_<platform>. Then it makes the demo
visible in Metabase: it syncs the schema of the warehouse, applies the metadata of the marts
(metadata.py), as metabase-init does, and puts the demo, its questions and its dashboard, in the
collection Agoranomos / Demo as the code says (content.py, ADR-0018): an edit is reverted and
logged, an item in the trash comes back, and every group but Administrators gets at most View on
them. The questions get the titles of their columns and axes from the display names of the
metadata, and their charts get the color of the project. The demo collection of v0.1, the one
whose dashboard the account of demo-init made, moves into Agoranomos as Demo once, after every item
in it that the demo does not ship has gone to the root.

Each step looks at the current state first. A start that finds a successful run of the transform
does not run the pipeline again and leaves the pause state of both of its DAGs as it is, and one
that finds a successful run of the history does the same with the history. The content of
Metabase is found by the entity IDs that Metabase gave it, in the registry of the content, never by
name, but for the one move of the collection of an earlier release.

The whole job has one deadline, DEADLINE seconds from its start, which every wait shares, and a
step that runs out of time names itself; ./agoranomos up waits longer than that for the stack.

It runs in a plain Python image and uses the client of metabase_init.py. Like metabase-init, it
takes every secret from the environment, sends it only in the body of a request, and ends a failure
with one line that names a step and an HTTP status, or the class of an error, never what a server
answered.
"""

from __future__ import annotations

import copy
import json
import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

import content
import metadata
from metabase_init import (
    SHAPE_ERRORS,
    ApiClient,
    Metabase,
    SetupError,
    find_warehouse,
    items_of,
    list_databases,
    logged,
    read_settings,
    sign_in,
    unexpected_shape,
)

LOAD_DAG = "agoranomos_demo_load"
TRANSFORM_DAG = "agoranomos_demo_transform"
HISTORY_DAG = "agoranomos_demo_history"
# The demo as a package of the content (content.py): its key in the registry, and its name, the
# name of its collection in Agoranomos.
DEMO_KEY = "demo"
DEMO_NAME = "Demo"
DEMO_DESCRIPTION = (
    "The demo of Agoranomos: a fictional business, as the demo data of this release shows it."
)
DASHBOARD = "Ad spend (demo)"
DASHBOARD_DESCRIPTION = "Ad spend of the demo account, from marts.ad_spend_daily."
# The names of the demo of v0.1, which the jobs look for once, to move it into Agoranomos: its
# collection at the root, its dashboard, and its questions, with the key of each in the package.
# Written out, so that they stay as v0.1 named them when the questions of the code change.
COLLECTION_V01 = "Agoranomos demo"
DASHBOARD_V01 = "Ad spend (demo)"
V01_KEYS = {
    "Spend, last 7 days": "spend-last-7-days",
    "Spend by campaign, last 7 days": "spend-by-campaign-last-7-days",
    "Spend by day": "spend-by-day",
}
MART_SCHEMA = "marts"
MART_TABLE = "ad_spend_daily"
# The one deadline of the job, in seconds from its start, shared by the waits for the DAGs, the
# load, the transform and the sync. ./agoranomos up waits 900 seconds for the whole stack.
DEADLINE = 600.0
# How long to wait between two looks at Airflow or Metabase, in seconds.
POLL_INTERVAL = 5.0
# The clock and the pause of the waits; the tests replace both with a fake clock.
clock = time.monotonic
pause = time.sleep
FINISHED = ("success", "failed")
LAST_7_DAYS = "report_date > (SELECT max(report_date) FROM marts.ad_spend_daily) - 7"
AIRFLOW_ADMIN_HINT = (
    "Check AIRFLOW_ADMIN_PASSWORD in .env. If you changed the password of admin in Airflow, put "
    "the same value in .env."
)
# Where to look when Metabase does not show the table of the mart. When the warehouse has it and
# no sync shows it, Metabase is the suspect. When the warehouse does not have it and the pipeline
# ran before and not now, Airflow has kept its runs, and the warehouse has lost the table since, as
# after a reset of the warehouse volume alone. When the pipeline ran now, its transform is.
SYNC_HINT = "see ./agoranomos logs metabase"
RAN_BEFORE_HINT = (
    "the demo pipeline ran before, and the table is gone from the warehouse since, as after a "
    f"reset of its volume: trigger {LOAD_DAG} in Airflow, and the transform that follows it builds "
    "the table again"
)
RAN_NOW_HINT = f"see the last run of {TRANSFORM_DAG} in Airflow"

# The settings of a chart that hold the titles of its axes.
AXIS_TITLES = ("graph.x_axis.title_text", "graph.y_axis.title_text")
# The color of the series of the demo charts: the first color of the chart palette of the project,
# which is made for a light background (the default of a Metabase dashboard). Metabase keeps the
# color of a series in series_settings, under the series key, which for a chart with one metric and
# no breakout is the name of the metric column. A color with no color_name is drawn as it is.
SERIES_COLOR = "#A86400"

# The questions of the demo dashboard, in the order of the dashboard, with their place on its grid
# of 24 columns. key is the key of a question in the package, which the registry of the content
# keeps; columns are the columns of the answer, which are columns of the mart, so that the titles
# come from their display names.
QUESTIONS: tuple[dict[str, Any], ...] = (
    {
        "key": "spend-last-7-days",
        "name": "Spend, last 7 days",
        "description": "The spend of the last seven days that have data.",
        "display": "scalar",
        "columns": ("spend",),
        "visualization_settings": {},
        "query": f"SELECT sum(spend) AS spend\nFROM marts.ad_spend_daily\nWHERE {LAST_7_DAYS}",
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    },
    {
        "key": "spend-by-campaign-last-7-days",
        "name": "Spend by campaign, last 7 days",
        "description": "The spend of each campaign in the last seven days that have data.",
        "display": "row",
        "columns": ("campaign_name", "spend"),
        "visualization_settings": {
            "graph.dimensions": ["campaign_name"],
            "graph.metrics": ["spend"],
            "series_settings": {"spend": {"color": SERIES_COLOR}},
        },
        "query": (
            "SELECT campaign_name, sum(spend) AS spend\nFROM marts.ad_spend_daily\n"
            f"WHERE {LAST_7_DAYS}\nGROUP BY campaign_name\nORDER BY spend DESC"
        ),
        "layout": {"row": 0, "col": 6, "size_x": 18, "size_y": 4},
    },
    {
        "key": "spend-by-day",
        "name": "Spend by day",
        "description": "The spend of every day that has data.",
        "display": "line",
        "columns": ("report_date", "spend"),
        "visualization_settings": {
            "graph.dimensions": ["report_date"],
            "graph.metrics": ["spend"],
            "series_settings": {"spend": {"color": SERIES_COLOR}},
        },
        "query": (
            "SELECT report_date, sum(spend) AS spend\nFROM marts.ad_spend_daily\n"
            "GROUP BY report_date\nORDER BY report_date"
        ),
        "layout": {"row": 4, "col": 0, "size_x": 24, "size_y": 6},
    },
)

ENV_NAMES = {
    "airflow_url": "AIRFLOW_URL",
    "airflow_password": "AIRFLOW_ADMIN_PASSWORD",
    "metabase_url": "METABASE_URL",
    "admin_email": "METABASE_ADMIN_EMAIL",
    "admin_password": "METABASE_ADMIN_PASSWORD",
    "warehouse_host": "WAREHOUSE_HOST",
    "warehouse_port": "WAREHOUSE_PORT",
    "warehouse_db": "WAREHOUSE_DB",
    "warehouse_user": "WAREHOUSE_USER",
    "metadata_export": "METADATA_EXPORT",
    "metadata_overrides": "METADATA_OVERRIDES",
    "content_registry": "CONTENT_REGISTRY",
}


@dataclass(frozen=True)
class Config:
    airflow_url: str
    airflow_password: str
    metabase_url: str
    admin_email: str
    admin_password: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    metadata_export: str
    metadata_overrides: str
    content_registry: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        return cls(**read_settings(env, ENV_NAMES, urls=("airflow_url", "metabase_url")))


class Deadline:
    """The one deadline of the job: its length in seconds, and when on clock() it is over."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.at = clock() + seconds

    def passed(self) -> bool:
        return clock() >= self.at


def wait_until[T](check: Callable[[], T], deadline: Deadline, what: str, hint: str) -> T:
    """Look with check every POLL_INTERVAL seconds until it gives a true result, and return it. A
    step that is not done by the deadline stops the job with a line that names the step (what)
    and says where to look (hint)."""
    while True:
        result = check()
        if result:
            return result
        if deadline.passed():
            raise SetupError(
                f"{what} within {deadline.seconds:g} seconds of the start of demo-init: {hint}"
            )
        pause(POLL_INTERVAL)


class Airflow(ApiClient):
    """The REST API of Airflow, signed in with a bearer token."""

    logs_hint = "See ./agoranomos logs airflow-apiserver."

    def __init__(self, url: str, timeout: float = 30.0) -> None:
        super().__init__(url, timeout)
        self.token: str | None = None

    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}


def sign_in_to_airflow(airflow: Airflow, password: str) -> None:
    """Sign in as the admin that airflow-init creates (user name admin)."""
    payload = airflow.call(
        "POST",
        "/auth/token",
        {"username": "admin", "password": password},
        hints={401: AIRFLOW_ADMIN_HINT, 403: AIRFLOW_ADMIN_HINT},
    )
    if not isinstance(payload, dict) or not payload.get("access_token"):
        raise SetupError(f"POST /auth/token gave no token. {airflow.logs_hint}")
    airflow.token = payload["access_token"]


def runs_path(dag_id: str, states: tuple[str, ...], since: str | None) -> str:
    """The query of the runs of a DAG in the states (any state when none is given) whose run_after
    is not before since, the newest first, one to a page. Airflow keeps every run, also after
    ./agoranomos down, and its API sorts the oldest first and gives 50 unless it is told otherwise:
    so the query says both, and a count comes from total_entries, never from a page."""
    query = [("order_by", "-run_after"), ("limit", "1"), *(("state", state) for state in states)]
    if since is not None:
        query.append(("run_after_gte", since))
    return f"/api/v2/dags/{dag_id}/dagRuns?{urlencode(query)}"


def count_runs(airflow: Airflow, dag_id: str, *states: str, since: str | None = None) -> int:
    return airflow.call("GET", runs_path(dag_id, states, since))["total_entries"]


def newest_run(airflow: Airflow, dag_id: str, *states: str) -> dict[str, Any] | None:
    page = airflow.call("GET", runs_path(dag_id, states, None))["dag_runs"]
    return page[0] if page else None


def transform_is_busy(airflow: Airflow) -> bool:
    """Whether the transform has a run queued or running, or an asset event that waits for a run.
    The list of queued events has no pages: it holds one event at most for each asset that
    schedules the DAG."""
    if count_runs(airflow, TRANSFORM_DAG, "queued", "running"):
        return True
    queued = airflow.call("GET", f"/api/v2/dags/{TRANSFORM_DAG}/assets/queuedEvents")
    return queued["total_entries"] > 0


def wait_for_dags(airflow: Airflow, deadline: Deadline) -> None:
    """Wait until the DAG processor has loaded the demo DAGs; until then Airflow answers 404."""
    for dag_id in (LOAD_DAG, TRANSFORM_DAG, HISTORY_DAG):
        wait_until(
            lambda dag_id=dag_id: airflow.call("GET", f"/api/v2/dags/{dag_id}", missing_ok=True),
            deadline,
            f"Airflow did not load the DAG {dag_id}",
            "see ./agoranomos logs airflow-dag-processor",
        )


def wait_for_run(airflow: Airflow, dag_id: str, run_id: str, deadline: Deadline) -> dict[str, Any]:
    """Wait until a run of a DAG has finished, and return it."""

    def finished() -> dict[str, Any] | None:
        run = airflow.call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id)}")
        return run if run["state"] in FINISHED else None

    return wait_until(finished, deadline, f"the DAG {dag_id} did not finish", "see it in Airflow")


def wait_for_transform(airflow: Airflow, since: str, deadline: Deadline) -> None:
    """Wait until a run of the transform that the load started has succeeded: a run whose
    run_after, the time of the asset event that started it, is not before since, the start of
    the load."""

    def succeeded() -> bool:
        if count_runs(airflow, TRANSFORM_DAG, "success", since=since):
            return True
        failed = count_runs(airflow, TRANSFORM_DAG, "failed", since=since)
        if failed and not transform_is_busy(airflow):
            # The run that the load started can succeed between the first count and the look at
            # whether the transform is busy, while a failed run of a person counts: so the
            # successes are counted once more before the job gives up.
            if count_runs(airflow, TRANSFORM_DAG, "success", since=since):
                return True
            raise SetupError(
                f"the DAG {TRANSFORM_DAG} failed after the demo load: see it in Airflow; a failed "
                "dbt test there means that dbt found broken data"
            )
        return False

    wait_until(
        succeeded,
        deadline,
        f"the DAG {TRANSFORM_DAG} did not succeed after the demo load",
        "see it in Airflow",
    )


def run_demo_pipeline(airflow: Airflow, deadline: Deadline) -> bool:
    """Run the demo pipeline, unless a run of the transform has succeeded before; True if it ran.

    A start that finds such a run changes nothing in Airflow, so a pause that a person made stays.
    Otherwise it unpauses both DAGs and waits until the transform is idle before the load: with
    max_active_runs=1, Airflow defers the run that an asset event starts while another run of the
    transform is queued or running, the deferred run consumes only the events up to the one that
    queued it, and an event that comes in between starts no run of its own. A load that is queued
    or running already is waited for, and not started again.
    """
    if count_runs(airflow, TRANSFORM_DAG, "success"):
        return False
    for dag_id in (LOAD_DAG, TRANSFORM_DAG):
        airflow.call("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    wait_until(
        lambda: not transform_is_busy(airflow),
        deadline,
        f"the DAG {TRANSFORM_DAG} did not finish its earlier runs",
        "see it in Airflow",
    )
    load = newest_run(airflow, LOAD_DAG, "queued", "running")
    if load is None:
        load = airflow.call("POST", f"/api/v2/dags/{LOAD_DAG}/dagRuns", {"logical_date": None})
    load = wait_for_run(airflow, LOAD_DAG, load["dag_run_id"], deadline)
    if load["state"] != "success":
        raise SetupError(f"the DAG {LOAD_DAG} failed: see it in Airflow")
    wait_for_transform(airflow, load["start_date"], deadline)
    return True


def run_demo_history(airflow: Airflow, deadline: Deadline) -> bool:
    """Run the history of the fictional platforms once, unless a run of it has succeeded before;
    True if it ran. A start that finds such a run changes nothing in Airflow. Otherwise it unpauses
    the DAG and triggers it, or waits for a run that is queued or running, until it ends."""
    if count_runs(airflow, HISTORY_DAG, "success"):
        return False
    airflow.call("PATCH", f"/api/v2/dags/{HISTORY_DAG}", {"is_paused": False})
    run = newest_run(airflow, HISTORY_DAG, "queued", "running")
    if run is None:
        run = airflow.call("POST", f"/api/v2/dags/{HISTORY_DAG}/dagRuns", {"logical_date": None})
    run = wait_for_run(airflow, HISTORY_DAG, run["dag_run_id"], deadline)
    if run["state"] != "success":
        raise SetupError(f"the DAG {HISTORY_DAG} failed: see it in Airflow")
    return True


def mart_is_synced(
    metabase: Metabase, database_id: int, table: metadata.Table, columns: Iterable[str]
) -> bool:
    """Whether Metabase shows the table of the mart, active, with the fields of these columns, by
    the rule of metadata.shows(). A person may have hidden the table in Admin > Table Metadata,
    and the metadata of a database leaves a hidden table out unless the request passes
    include_hidden=true (Metabase 0.63.19 and 0.64.1); a sync does not show it again. A table that a
    sync marked inactive, as after a reset of the warehouse, does not count."""
    return metadata.shows(metadata.metadata_of(metabase, database_id), table, columns)


def ensure_synced(
    metabase: Metabase, database_id: int, table: metadata.Table, deadline: Deadline, ran: bool
) -> bool:
    """Make Metabase show the table of the mart with the fields of its columns; True if a sync was
    needed. When Metabase does not show it so, the warehouse is asked through the connection of
    Metabase: a warehouse without the table stops the job at once, with what to do (ran says
    whether the pipeline ran now), and a table that it has gets a sync, waited for until Metabase
    shows its fields, which come after the table."""
    key = (table.schema, table.name)
    shown = metadata.metadata_of(metabase, database_id)
    sync = metadata.sync_needed(metabase, database_id, metadata.Metadata({key: table}), shown)
    if sync.absent:
        cause = RAN_NOW_HINT if ran else RAN_BEFORE_HINT
        raise SetupError(f"Metabase finds no {MART_SCHEMA}.{MART_TABLE} in the warehouse: {cause}")
    if not sync.tables:
        return False
    metabase.call("POST", f"/api/database/{database_id}/sync_schema")
    wait_until(
        lambda: mart_is_synced(metabase, database_id, table, sync.tables[key]),
        deadline,
        f"Metabase did not find {MART_SCHEMA}.{MART_TABLE} after a sync of the warehouse",
        SYNC_HINT,
    )
    return True


def demo_mart(marts: metadata.Metadata) -> metadata.Table:
    """The table of the mart of the demo in the effective metadata: the dbt project declares it."""
    table = marts.effective.get((MART_SCHEMA, MART_TABLE))
    if table is None:
        raise SetupError(
            f"the dbt project declares no {MART_SCHEMA}.{MART_TABLE}: see ./agoranomos logs "
            "metadata-export"
        )
    return table


def column_titles(marts: metadata.Metadata) -> dict[str, str]:
    """The display names of the columns of the mart, by name, in the effective metadata."""
    table = marts.effective.get((MART_SCHEMA, MART_TABLE))
    columns = table.columns if table is not None else ()
    return {column.name: column.display_name for column in columns if column.display_name}


def column_key(name: str) -> str:
    """The key of a column in the column settings of a card, as the frontend of Metabase writes it:
    JSON.stringify(["name", <column name>]), with no blank."""
    return json.dumps(["name", name], separators=(",", ":"))


def visualization_settings(question: Mapping[str, Any], titles: Mapping[str, str]) -> dict:
    """The settings of a question, with the titles of its columns and, for a chart, of its axes,
    and the series color that its definition holds. A native question takes no title from the
    fields of the mart, so the titles are its own."""
    settings = copy.deepcopy(question["visualization_settings"])
    columns = {
        column_key(name): {"column_title": titles[name]}
        for name in question["columns"]
        if name in titles
    }
    if columns:
        settings["column_settings"] = columns
    dimensions = settings.get("graph.dimensions") or [None]
    metrics = settings.get("graph.metrics") or [None]
    for key, name in zip(AXIS_TITLES, (dimensions[0], metrics[0]), strict=True):
        if name in titles:
            settings[key] = titles[name]
    return settings


def demo_package(titles: Mapping[str, str]) -> content.Package:
    """The demo as a package of the content: its questions, with the titles of their columns and
    the series color, and its dashboard."""
    return content.Package(
        key=DEMO_KEY,
        name=DEMO_NAME,
        description=DEMO_DESCRIPTION,
        questions=tuple(
            content.Question(
                q["key"],
                q["name"],
                q["description"],
                q["display"],
                q["query"],
                visualization_settings(q, titles),
            )
            for q in QUESTIONS
        ),
        dashboards=(
            content.Dashboard(
                "dashboard",
                DASHBOARD,
                DASHBOARD_DESCRIPTION,
                tuple(content.Placement(q["key"], **q["layout"]) for q in QUESTIONS),
            ),
        ),
    )


# How each model of the items of a collection leaves the collection of v0.1 for the root, and its
# word in the log. A dashboard takes its own questions along (Metabase 0.64.1); a question of the
# dashboard of v0.1 leaves that dashboard too. An item of any other model, such as a timeline or a
# document, keeps the collection of v0.1 where it is.
TAKE_OUT = {
    "card": ("question", "card", {"collection_id": None}),
    "dataset": ("model", "card", {"collection_id": None}),
    "metric": ("metric", "card", {"collection_id": None}),
    "dashboard": ("dashboard", "dashboard", {"collection_id": None}),
    "collection": ("collection", "collection", {"parent_id": None}),
}


# The model of a question in the items of a collection, by its type (collections/children.clj).
ITEM_MODEL = {"question": "card", "model": "dataset", "metric": "metric"}


def everything_in(metabase: Metabase, collection_id: int) -> list[dict[str, Any]]:
    """Every item of a collection, those in the trash included, each once: the items out of the
    trash, with the questions of its dashboards; with archived=true, its sub-collections in the
    trash and what went there with them; and the questions and the dashboards that a person put
    in the trash from it. Metabase 0.64.1 lists those in the trash only, as items of the trash, but
    its lists of the questions and the dashboards in the trash keep their own collection (read in
    collections/children.clj and on the stand, October 2026)."""
    found: dict[tuple[str, int], dict[str, Any]] = {}
    for archived in ("", "&archived=true"):
        path = f"/api/collection/{collection_id}/items?show_dashboard_questions=true{archived}"
        for item in items_of(metabase.call("GET", path)):
            found[(item.get("model"), item["id"])] = item
    for kind in ("card", "dashboard"):
        for item in items_of(metabase.call("GET", f"/api/{kind}?f=archived")):
            if item.get("collection_id") == collection_id:
                model = kind if kind == "dashboard" else ITEM_MODEL.get(item.get("type"), "card")
                found[(model, item["id"])] = {**item, "model": model, "archived": True}
    return sorted(found.values(), key=lambda item: (str(item.get("model")), item["id"]))


def take_out(
    metabase: Metabase, item: Mapping[str, Any], board_id: int, where: str, log: content.Log
) -> None:
    """Move an item that the demo does not ship to the root, where it stays in the trash if it was
    there, with one line by its model and its ID, never its name. A question of another dashboard
    moves with its dashboard; a question of the dashboard of the demo leaves it."""
    word, model, body = TAKE_OUT[item["model"]]
    if item.get("dashboard_id") and item["dashboard_id"] != board_id:
        return
    if item.get("dashboard_id"):
        body = {**body, "dashboard_id": None}
    if model == "collection" and item.get("archived"):
        body = {**body, "archived": True}  # a PUT of a collection without it restores it
    metabase.call("PUT", f"/api/{model}/{item['id']}", body)
    trash = ", in the trash," if item.get("archived") else ""
    log(
        f"moved out: {word} {item['id']} of {where}{trash} to the root, as the demo does not "
        "ship it"
    )


def made_by(metabase: Metabase, model: str, item_id: int, account_id: int) -> bool:
    """Whether the account made the question or the dashboard of an ID."""
    return metabase.call("GET", f"/api/{model}/{item_id}").get("creator_id") == account_id


def demo_of_v01(
    metabase: Metabase, items: list[dict[str, Any]], account_id: int
) -> dict[str, dict[str, Any]]:
    """The items of a collection of the old name that are the demo of v0.1, by their keys in the
    package: of each name of v0.1, the oldest question, and the oldest dashboard, that the account
    of demo-init made. A question of the demo is one of the collection, not of a dashboard."""
    found: dict[str, dict[str, Any]] = {}
    names = {**V01_KEYS, DASHBOARD_V01: "dashboard"}
    for item in sorted(items, key=lambda item: item["id"]):
        model = "dashboard" if names.get(item.get("name")) == "dashboard" else "card"
        key = names.get(item.get("name"))
        if (
            key is not None
            and key not in found
            and item.get("model") == model
            and not item.get("archived")
            and not item.get("dashboard_id")
            and made_by(metabase, model, item["id"], account_id)
        ):
            found[key] = item
    return found


def collection_line(old: Mapping[str, Any]) -> str:
    return f"collection {old['id']} named {COLLECTION_V01!r} at the root"


def adopt_earlier_demo(metabase: Metabase, registry: content.Registry, log: content.Log) -> bool:
    """Move the demo collection of v0.1 into Agoranomos as Demo, once, and record it with its
    dashboard and questions, so that the next steps put them back as the code says (ADR-0018).

    Nothing is done when Metabase has the demo of the registry, also in the trash: a registry whose
    demo Metabase does not have, as after a restore of the Metabase of an earlier release, lets the
    collection of that release move again. The name of a collection proves nothing, as on a
    Metabase of v0.1 every account may curate the root: the demo of v0.1 is the collection named
    Agoranomos demo at the root, the one with the lowest ID of those, that holds the dashboard Ad
    spend (demo) that the account of demo-init made, the account that made the demo of v0.1. Of
    its questions, only those of the names of v0.1 that this account made are the demo's. When no
    collection is the demo, or one holds an item that the job cannot take out, nothing moves: one
    line says so, and the package makes its demo anew.

    Before the move, every other item of the collection, whoever made it, goes to the root, also
    from the trash, where it stays, with one line for each by its model and its ID, so that only
    the content of the package enters Agoranomos. After the move, no group but Administrators may
    curate Demo any more, and what reached the collection in between and the registry does not
    name goes to the root as well. True if it moved one."""
    entry = registry.get(DEMO_KEY)
    if entry and content.find(metabase, "collection", entry["entity_id"]) is not None:
        return False
    candidates = sorted(
        (
            c
            for c in items_of(metabase.call("GET", "/api/collection"))
            if c.get("name") == COLLECTION_V01
            and c.get("location") == "/"
            and not c.get("personal_owner_id")
            and not c.get("archived")
        ),
        key=lambda c: c["id"],
    )
    if not candidates:
        return False
    account_id = metabase.call("GET", "/api/user/current")["id"]
    for old in candidates:
        items = everything_in(metabase, old["id"])
        adopted = demo_of_v01(metabase, items, account_id)
        if "dashboard" in adopted:
            break
    else:
        ids = ", ".join(str(c["id"]) for c in candidates)
        one = len(candidates) == 1
        log(
            f"not moved: {'collection' if one else 'collections'} {ids} named {COLLECTION_V01!r} "
            f"at the root {'holds' if one else 'hold'} no dashboard {DASHBOARD_V01!r} that the "
            "account of demo-init made, "
            + (
                "so it is not the demo of v0.1, and it stays where it is"
                if one
                else "so none is the demo of v0.1, and they stay where they are"
            )
        )
        return False
    kept = {item["id"] for item in adopted.values() if item["model"] == "card"}
    board_id = adopted["dashboard"]["id"]
    others = [
        item
        for item in items
        if not (item.get("model") == "dashboard" and item["id"] == board_id)
        and not (item.get("model") == "card" and item["id"] in kept)
    ]
    unknown = next((item for item in others if item.get("model") not in TAKE_OUT), None)
    if unknown is not None:
        log(
            f"not moved: {collection_line(old)} holds a {unknown.get('model')} {unknown['id']} "
            "that demo-init cannot take out first, and it stays where it is"
        )
        return False
    for item in others:
        take_out(metabase, item, board_id, "the collection of v0.1", log)
    root = content.ensure_collection(
        metabase,
        registry,
        content.ROOT_KEY,
        content.ROOT_NAME,
        content.ROOT_DESCRIPTION,
        None,
        log,
    )
    moved = {"name": DEMO_NAME, "parent_id": root["id"]}
    metabase.call("PUT", f"/api/collection/{old['id']}", moved)
    registry.set(DEMO_KEY, "collection", old["entity_id"])
    log(
        f"moved: collection {COLLECTION_V01!r} of an earlier release into {content.ROOT_NAME} as "
        f"{DEMO_NAME}"
    )
    for key, item in adopted.items():
        model = "dashboard" if key == "dashboard" else "card"
        registry.set(f"{DEMO_KEY}/{key}", model, item["entity_id"])
    # Every account could curate the collection until now: it may have got an item during the
    # move, which no account but an admin can add once it is locked.
    content.lock_down(metabase, registry, log)
    named = {registry.get(key)["entity_id"] for key in registry.keys_of(DEMO_KEY)}
    for item in everything_in(metabase, old["id"]):
        if item.get("entity_id") in named:
            continue
        if item.get("model") in TAKE_OUT:
            take_out(metabase, item, board_id, DEMO_NAME, log)
        else:
            log(f"left: {item.get('model')} {item['id']} in Demo, which demo-init cannot take out")
    return True


def when(ran: bool) -> str:
    return "ran now" if ran else "ran before"


def main(env: Mapping[str, str] | None = None) -> int:
    # The server whose answers the job reads, for the line of an answer of an unexpected shape.
    server, hint = "Airflow", Airflow.logs_hint
    try:
        deadline = Deadline(DEADLINE)
        config = Config.from_env(os.environ if env is None else env)
        # The metadata and the registry of the content are read and checked first: a mistake in
        # the overrides of the deployment, or a registry that is none, stops the job before it
        # writes anything, in Airflow or in Metabase.
        marts = metadata.load(config.metadata_export, config.metadata_overrides)
        mart = demo_mart(marts)
        registry = content.Registry(config.content_registry)
        log = logged("demo-init")
        airflow = Airflow(config.airflow_url)
        sign_in_to_airflow(airflow, config.airflow_password)
        wait_for_dags(airflow, deadline)
        ran_pipeline = run_demo_pipeline(airflow, deadline)
        ran_history = run_demo_history(airflow, deadline)
        server, hint = "Metabase", Metabase.logs_hint
        metabase = Metabase(config.metabase_url)
        sign_in(metabase, config.admin_email, config.admin_password)
        # The connection that metabase-init keeps up to date; its log names it when several match.
        database = find_warehouse(list_databases(metabase), config, warn=False)
        if database is None:
            raise SetupError(
                "Metabase has no connection to the warehouse: see ./agoranomos logs metabase-init"
            )
        # The history builds no mart: what a missing mart points at depends on whether the
        # pipeline ran now.
        ensure_synced(metabase, database["id"], mart, deadline, ran_pipeline)
        metadata.apply(
            metabase,
            database["id"],
            marts,
            lambda check, what: wait_until(check, deadline, what, SYNC_HINT),
            log=log,
        )
        adopt_earlier_demo(metabase, registry, log)
        package = demo_package(column_titles(marts))
        ids = content.ensure_package(metabase, registry, package, database["id"], log)
        dashboard_id = ids["dashboard"]
    except (SetupError, metadata.MetadataError, content.ContentError) as error:
        print(f"demo-init: {error}", file=sys.stderr)
        return 1
    except SHAPE_ERRORS as error:
        print(f"demo-init: {unexpected_shape(server, error, hint)}", file=sys.stderr)
        return 1
    print(
        f"demo-init: the demo pipeline {when(ran_pipeline)}, the history of the demo "
        f"{when(ran_history)}, and the dashboard "
        f"{DASHBOARD!r} ({dashboard_id}) is in the collection {content.ROOT_NAME} / {DEMO_NAME}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
