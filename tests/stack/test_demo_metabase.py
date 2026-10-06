"""The demo in Metabase, made by demo-init: the collection, its questions and its dashboard, each
once, with the numbers that the generator made; and demo-init runs again without a change.

Answers of Metabase and the output of demo-init could hold secrets: the assertions print names,
counts, sums and states only.
"""

from datetime import date
from decimal import Decimal

import pytest
from agoranomos_pipelines.demo_data import rows
from airflow_api import is_busy, run_count
from services import DEMO
from stack_helpers import compose, http_json, leaked, secrets, wait_for, warehouse_output

pytestmark = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)

COLLECTION = "Agoranomos demo"
DASHBOARD = "Ad spend (demo)"
QUESTIONS = {"Spend, last 7 days", "Spend by campaign, last 7 days", "Spend by day"}
WAREHOUSE_NAME = "Agoranomos warehouse"
LOAD = "agoranomos_demo_load"
TRANSFORM = "agoranomos_demo_transform"
DAGS = (LOAD, TRANSFORM)


@pytest.fixture(scope="module", autouse=True)
def idle_demo_dags(airflow_url, token):
    """The tests of the demo load run before these, and each of their loads starts a run of the
    transform. These tests wait until such runs have ended, so that none starts or rebuilds the
    mart while they count runs or read the mart."""
    wait_for(
        lambda: not any(is_busy(airflow_url, token, dag_id) for dag_id in DAGS),
        600,
        interval=5,
        message="the runs of the demo DAGs that earlier tests started did not end",
    )


def get(metabase_url, session, path):
    status, body = http_json("GET", f"{metabase_url}{path}", headers=session)
    assert status == 200, f"GET {path} answered HTTP {status}"
    return body


def demo_content(metabase_url, session):
    """The demo collection, and the names and ids of its questions and dashboards."""
    collections = [
        c
        for c in get(metabase_url, session, "/api/collection")
        if c.get("name") == COLLECTION and c.get("location") == "/" and not c.get("archived")
    ]
    assert len(collections) == 1, f"{len(collections)} collections named {COLLECTION}"
    [collection] = collections
    content = {}
    for model in ("card", "dashboard"):
        items = get(
            metabase_url, session, f"/api/collection/{collection['id']}/items?models={model}"
        )
        content[model] = sorted((item["name"], item["id"]) for item in items["data"])
    return collection, content


def run_demo_init_again(values):
    """Run demo-init once more, as every start does: its exit code, whether it found that the
    demo pipeline ran before, and the names of the secrets in its output. The output stays in
    this frame, which has ended before the test checks anything."""
    result = compose("run", "--rm", "--no-deps", "-T", "demo-init", check=False)
    return (
        result.returncode,
        "ran before" in result.stdout,
        leaked(values, [result.stdout, result.stderr]),
    )


def test_the_collection_holds_the_questions_and_the_dashboard_once(metabase_url, session):
    _, content = demo_content(metabase_url, session)
    assert sorted(name for name, _ in content["card"]) == sorted(QUESTIONS)
    assert [name for name, _ in content["dashboard"]] == [DASHBOARD]


def test_the_dashboard_shows_the_three_questions(metabase_url, session):
    _, content = demo_content(metabase_url, session)
    [(_, dashboard_id)] = content["dashboard"]
    dashboard = get(metabase_url, session, f"/api/dashboard/{dashboard_id}")
    shown = sorted(dashcard["card_id"] for dashcard in dashboard["dashcards"])
    assert len(shown) == len(QUESTIONS)
    assert shown == sorted(card_id for _, card_id in content["card"])


def test_the_spend_of_the_last_seven_days_is_what_the_generator_made(env, metabase_url, session):
    _, content = demo_content(metabase_url, session)
    card_id = dict(content["card"])["Spend, last 7 days"]
    status, result = http_json("POST", f"{metabase_url}/api/card/{card_id}/query", headers=session)
    # Metabase 0.63.19 answers 202, with the rows in data.rows.
    state = result.get("status") if isinstance(result, dict) else None
    assert (status, state) in {(200, "completed"), (202, "completed")}, (status, state)
    [[spend]] = result["data"]["rows"]
    last = date.fromisoformat(
        warehouse_output(env, "SELECT max(report_date) FROM marts.ad_spend_daily")
    )
    expected = sum(row["spend"] for row in rows(last, 7))  # the seven days up to the last
    assert Decimal(str(spend)).quantize(Decimal("0.01")) == expected


def test_metabase_knows_the_mart(metabase_url, session):
    databases = get(metabase_url, session, "/api/database")
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    [warehouse] = [d for d in databases if d.get("name") == WAREHOUSE_NAME]
    # As demo-init asks: a table that a person hid in Metabase counts, and Metabase 0.63.19 leaves
    # it out unless the request passes include_hidden=true.
    metadata = get(
        metabase_url,
        session,
        f"/api/database/{warehouse['id']}/metadata?skip_fields=true&include_hidden=true",
    )
    tables = {(table["schema"], table["name"]) for table in metadata["tables"]}
    assert ("marts", "ad_spend_daily") in tables


def test_the_first_start_ran_the_demo_pipeline(airflow_url, token):
    # What the first start did, whatever a person did since, such as a pause, which demo-init
    # keeps: the load ran, and its asset started a run of the transform that succeeded. Counted by
    # total_entries, as Airflow keeps every run.
    assert run_count(airflow_url, token, LOAD) > 0, f"{LOAD} has no run"
    succeeded = run_count(
        airflow_url, token, TRANSFORM, states=("success",), run_types=("asset_triggered",)
    )
    assert succeeded > 0, f"{TRANSFORM} has no successful run that the asset of the load started"


def test_demo_init_runs_again_without_a_change(env, airflow_url, token, metabase_url, session):
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    # Counted by total_entries: Airflow keeps every run, and they can fill a page.
    runs = {dag_id: run_count(airflow_url, token, dag_id) for dag_id in DAGS}
    before = demo_content(metabase_url, session)
    code, ran_before, found = run_demo_init_again(values)
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert ran_before, "demo-init did not find that the demo pipeline ran before"
    assert {dag_id: run_count(airflow_url, token, dag_id) for dag_id in DAGS} == runs
    assert demo_content(metabase_url, session) == before
