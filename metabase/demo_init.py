"""The demo of Agoranomos, made ready by the one-shot demo-init service on every start of the demo
profile.

It runs the demo pipeline once: agoranomos_demo_load, and then agoranomos_demo_transform, which the
asset of the raw table starts. Then it makes the demo visible in Metabase: it syncs the schema of
the warehouse and creates the demo collection, its questions and its dashboard.

Each step looks at the current state first. A start that finds a successful run of the transform
does not run the pipeline again and leaves the pause state of both DAGs as it is. The content of
Metabase is found by name: what is there stays as it is, edits included, and what is missing is
created again, so a deleted or renamed question, dashboard or collection comes back.

The whole job has one deadline, DEADLINE seconds from its start, which every wait shares, and a
step that runs out of time names itself; ./agoranomos up waits longer than that for the stack.

It runs in a plain Python image and uses the client of metabase_init.py. Like metabase-init, it
takes every secret from the environment, sends it only in the body of a request, and its messages
name a step and an HTTP status, never what a server answered.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

from metabase_init import (
    ApiClient,
    Metabase,
    SetupError,
    find_warehouse,
    items_of,
    list_databases,
    read_settings,
    sign_in,
)

LOAD_DAG = "agoranomos_demo_load"
TRANSFORM_DAG = "agoranomos_demo_transform"
COLLECTION = "Agoranomos demo"
DASHBOARD = "Ad spend (demo)"
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

# The questions of the demo dashboard, in the order of the dashboard, with their place on its grid
# of 24 columns.
QUESTIONS: tuple[dict[str, Any], ...] = (
    {
        "name": "Spend, last 7 days",
        "description": "The spend of the last seven days that have data.",
        "display": "scalar",
        "visualization_settings": {},
        "query": f"SELECT sum(spend) AS spend\nFROM marts.ad_spend_daily\nWHERE {LAST_7_DAYS}",
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    },
    {
        "name": "Spend by campaign, last 7 days",
        "description": "The spend of each campaign in the last seven days that have data.",
        "display": "row",
        "visualization_settings": {
            "graph.dimensions": ["campaign_name"],
            "graph.metrics": ["spend"],
        },
        "query": (
            "SELECT campaign_name, sum(spend) AS spend\nFROM marts.ad_spend_daily\n"
            f"WHERE {LAST_7_DAYS}\nGROUP BY campaign_name\nORDER BY spend DESC"
        ),
        "layout": {"row": 0, "col": 6, "size_x": 18, "size_y": 4},
    },
    {
        "name": "Spend by day",
        "description": "The spend of every day that has data.",
        "display": "line",
        "visualization_settings": {"graph.dimensions": ["report_date"], "graph.metrics": ["spend"]},
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
    """Wait until the DAG processor has loaded both demo DAGs; until then Airflow answers 404."""
    for dag_id in (LOAD_DAG, TRANSFORM_DAG):
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


def mart_is_synced(metabase: Metabase, database_id: int) -> bool:
    metadata = metabase.call("GET", f"/api/database/{database_id}/metadata?skip_fields=true")
    return any(
        table.get("schema") == MART_SCHEMA and table.get("name") == MART_TABLE
        for table in metadata.get("tables", [])
    )


def ensure_synced(metabase: Metabase, database_id: int, deadline: Deadline) -> bool:
    """Make Metabase know the table of the mart; True if a sync was needed."""
    if mart_is_synced(metabase, database_id):
        return False
    metabase.call("POST", f"/api/database/{database_id}/sync_schema")
    wait_until(
        lambda: mart_is_synced(metabase, database_id),
        deadline,
        f"Metabase did not find {MART_SCHEMA}.{MART_TABLE} after a sync of the warehouse",
        "see ./agoranomos logs metabase",
    )
    return True


def ensure_collection(metabase: Metabase) -> int:
    """The demo collection, at the top level of Metabase, created when there is none."""
    for collection in items_of(metabase.call("GET", "/api/collection")):
        if (
            collection.get("name") == COLLECTION
            and collection.get("location") == "/"
            and not collection.get("personal_owner_id")
            and not collection.get("archived")
        ):
            return collection["id"]
    created = metabase.call(
        "POST",
        "/api/collection",
        {"name": COLLECTION, "description": "The demo of Agoranomos: one fictional ad account."},
    )
    return created["id"]


def collection_items(metabase: Metabase, collection_id: int, model: str) -> dict[str, int]:
    """The ids of the items of a model in a collection, by name."""
    response = metabase.call("GET", f"/api/collection/{collection_id}/items?models={model}")
    return {item["name"]: item["id"] for item in items_of(response) if item.get("model") == model}


def ensure_questions(metabase: Metabase, collection_id: int, database_id: int) -> dict[str, int]:
    """Create the questions that the collection lacks; return the id of every question by name."""
    cards = collection_items(metabase, collection_id, "card")
    for question in QUESTIONS:
        if question["name"] in cards:
            continue
        card = metabase.call(
            "POST",
            "/api/card",
            {
                "name": question["name"],
                "description": question["description"],
                "display": question["display"],
                "visualization_settings": question["visualization_settings"],
                "collection_id": collection_id,
                "dataset_query": {
                    "database": database_id,
                    "type": "native",
                    "native": {"query": question["query"]},
                },
            },
        )
        cards[question["name"]] = card["id"]
    return cards


def ensure_dashboard(metabase: Metabase, collection_id: int, cards: Mapping[str, int]) -> int:
    """Create the dashboard if the collection lacks it, and place the questions on an empty one."""
    dashboard_id = collection_items(metabase, collection_id, "dashboard").get(DASHBOARD)
    if dashboard_id is None:
        dashboard_id = metabase.call(
            "POST",
            "/api/dashboard",
            {
                "name": DASHBOARD,
                "description": "Ad spend of the demo account, from marts.ad_spend_daily.",
                "collection_id": collection_id,
            },
        )["id"]
    if not metabase.call("GET", f"/api/dashboard/{dashboard_id}").get("dashcards"):
        dashcards = [
            {"id": -(index + 1), "card_id": cards[question["name"]], **question["layout"]}
            for index, question in enumerate(QUESTIONS)
        ]
        metabase.call("PUT", f"/api/dashboard/{dashboard_id}", {"dashcards": dashcards})
    return dashboard_id


def main(env: Mapping[str, str] | None = None) -> int:
    try:
        deadline = Deadline(DEADLINE)
        config = Config.from_env(os.environ if env is None else env)
        airflow = Airflow(config.airflow_url)
        sign_in_to_airflow(airflow, config.airflow_password)
        wait_for_dags(airflow, deadline)
        ran = run_demo_pipeline(airflow, deadline)
        metabase = Metabase(config.metabase_url)
        sign_in(metabase, config.admin_email, config.admin_password)
        # The connection that metabase-init keeps up to date; its log names it when several match.
        database = find_warehouse(list_databases(metabase), config, warn=False)
        if database is None:
            raise SetupError(
                "Metabase has no connection to the warehouse: see ./agoranomos logs metabase-init"
            )
        ensure_synced(metabase, database["id"], deadline)
        collection_id = ensure_collection(metabase)
        cards = ensure_questions(metabase, collection_id, database["id"])
        dashboard_id = ensure_dashboard(metabase, collection_id, cards)
    except SetupError as error:
        print(f"demo-init: {error}", file=sys.stderr)
        return 1
    print(
        f"demo-init: the demo pipeline {'ran now' if ran else 'ran before'}, and the dashboard "
        f"{DASHBOARD!r} ({dashboard_id}) is in the collection {COLLECTION!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
