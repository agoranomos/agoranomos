"""The demo in Metabase, made by demo-init: the collection Agoranomos / Demo, its questions and its
dashboard, found through the registry of the content, each once, with the numbers that the
generator made, view-only for every group but Administrators (ADR-0018); demo-init runs again
without a change, puts an edit back, never takes a collection of the same name for the package,
and moves the demo collection of v0.1.

The tests change the content as an admin could, and the next run of demo-init puts it back. The
test of the move runs last: it rebuilds the content. Answers of Metabase and the output of
demo-init could hold secrets: the assertions print names, counts, sums and states only.
"""

from datetime import date
from decimal import Decimal

import pytest
from agoranomos_pipelines.demo_data import rows
from airflow_api import is_busy, run_count
from metabase_content import (
    REGISTRY,
    delete,
    get,
    group_ids,
    in_metabase_init,
    item,
    post,
    put,
    registry,
    run_job,
    warehouse_database_id,
)
from services import DEMO
from stack_helpers import http_json, secrets, wait_for, warehouse_output

pytestmark = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)

DASHBOARD = "Ad spend (demo)"
# The questions of the demo by their keys in the package.
QUESTIONS = {
    "spend-last-7-days": "Spend, last 7 days",
    "spend-by-campaign-last-7-days": "Spend by campaign, last 7 days",
    "spend-by-day": "Spend by day",
}
LOAD = "agoranomos_demo_load"
TRANSFORM = "agoranomos_demo_transform"
HISTORY = "agoranomos_demo_history"
DAGS = (LOAD, TRANSFORM, HISTORY)
# The keys of the content of the demo in the registry.
KEYS = ("agoranomos", "demo", "demo/dashboard", *(f"demo/{key}" for key in QUESTIONS))


@pytest.fixture(scope="module", autouse=True)
def idle_demo_dags(airflow_url, token):
    """The tests of the demo load run before these, and each of their loads starts a run of the
    transform, and they run the history too. These tests wait until such runs have ended, so that
    none starts or rebuilds the mart while they count runs or read the mart."""
    wait_for(
        lambda: not any(is_busy(airflow_url, token, dag_id) for dag_id in DAGS),
        600,
        interval=5,
        message="the runs of the demo DAGs that earlier tests started did not end",
    )


def demo_content(metabase_url, session):
    """The registry of the demo, and what Metabase has for each of its items: the name, the place,
    whether it is in the trash, and for a question its query and settings of display, for the
    dashboard its cards."""
    entries = registry()
    content = {}
    for key in KEYS:
        found = item(metabase_url, session, key, entries)
        content[key] = (
            found["name"],
            found.get("location") or found.get("collection_id"),
            found.get("archived"),
            (found.get("dataset_query") or {}).get("native"),
            found.get("visualization_settings"),
            sorted(
                (d.get("card_id"), d["row"], d["col"], d["size_x"], d["size_y"])
                for d in found.get("dashcards") or []
            ),
        )
    return {key: entries[key] for key in KEYS}, content


def test_the_demo_is_in_agoranomos_as_demo(metabase_url, session):
    entries = registry()
    root = item(metabase_url, session, "agoranomos", entries)
    demo = item(metabase_url, session, "demo", entries)
    assert (root["name"], root["location"]) == ("Agoranomos", "/")
    assert (demo["name"], demo["location"]) == ("Demo", f"/{root['id']}/")
    questions = {key: item(metabase_url, session, f"demo/{key}", entries) for key in QUESTIONS}
    assert {key: q["name"] for key, q in questions.items()} == QUESTIONS
    assert {q["collection_id"] for q in questions.values()} == {demo["id"]}
    board = item(metabase_url, session, "demo/dashboard", entries)
    assert (board["name"], board["collection_id"]) == (DASHBOARD, demo["id"])
    shown = sorted(dashcard["card_id"] for dashcard in board["dashcards"])
    assert shown == sorted(q["id"] for q in questions.values())


def test_every_group_but_administrators_has_at_most_view(metabase_url, session):
    admins, all_users = group_ids(metabase_url, session)
    graph = get(metabase_url, session, "/api/collection/graph")
    entries = registry()
    for key in ("agoranomos", "demo"):
        cid = str(item(metabase_url, session, key, entries)["id"])
        for group, perms in graph["groups"].items():
            if group not in admins:
                assert perms.get(cid, "none") in ("read", "none"), (group, key)
    demo_id = str(item(metabase_url, session, "demo", entries)["id"])
    assert graph["groups"][all_users][demo_id] == "read"


def test_the_spend_of_the_last_seven_days_is_what_the_generator_made(env, metabase_url, session):
    card_id = item(metabase_url, session, "demo/spend-last-7-days")["id"]
    status, result = http_json("POST", f"{metabase_url}/api/card/{card_id}/query", headers=session)
    # Metabase 0.63.19 and 0.64.1 answer 202, with the rows in data.rows.
    state = result.get("status") if isinstance(result, dict) else None
    assert (status, state) in {(200, "completed"), (202, "completed")}, (status, state)
    [[spend]] = result["data"]["rows"]
    last = date.fromisoformat(
        warehouse_output(env, "SELECT max(report_date) FROM marts.ad_spend_daily")
    )
    expected = sum(row["spend"] for row in rows(last, 7))  # the seven days up to the last
    assert Decimal(str(spend)).quantize(Decimal("0.01")) == expected


def test_metabase_knows_the_mart(metabase_url, session):
    # As demo-init asks: a table that a person hid in Metabase counts, and Metabase 0.63.19 and
    # 0.64.1 leave it out unless the request passes include_hidden=true.
    database_id = warehouse_database_id(metabase_url, session)
    metadata = get(
        metabase_url,
        session,
        f"/api/database/{database_id}/metadata?skip_fields=true&include_hidden=true",
    )
    tables = {(table["schema"], table["name"]) for table in metadata["tables"]}
    assert ("marts", "ad_spend_daily") in tables


def test_the_first_start_ran_the_demo_pipeline(airflow_url, token):
    # What the first start did, whatever a person did since, such as a pause, which demo-init
    # keeps: the load ran, and its asset started a run of the transform that succeeded, and the
    # history succeeded. Counted by total_entries, as Airflow keeps every run.
    assert run_count(airflow_url, token, LOAD) > 0, f"{LOAD} has no run"
    succeeded = run_count(
        airflow_url, token, TRANSFORM, states=("success",), run_types=("asset_triggered",)
    )
    assert succeeded > 0, f"{TRANSFORM} has no successful run that the asset of the load started"
    assert run_count(airflow_url, token, HISTORY, states=("success",)) > 0, f"{HISTORY} never ran"


def test_demo_init_runs_again_without_a_change(env, airflow_url, token, metabase_url, session):
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    # Counted by total_entries: Airflow keeps every run, and they can fill a page.
    runs = {dag_id: run_count(airflow_url, token, dag_id) for dag_id in DAGS}
    before = demo_content(metabase_url, session)
    code, changes, found, last = run_job("demo-init", values)
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert "ran before" in last, "demo-init did not find that the demo pipeline ran before"
    assert changes == [], "demo-init changed the content of a start that it had made itself"
    assert {dag_id: run_count(airflow_url, token, dag_id) for dag_id in DAGS} == runs
    assert demo_content(metabase_url, session) == before


def test_an_edit_of_the_content_is_reverted_by_the_next_start_and_logged(
    env, metabase_url, session
):
    entries = registry()
    question = item(metabase_url, session, "demo/spend-by-day", entries)
    board = item(metabase_url, session, "demo/dashboard", entries)
    demo_id = str(item(metabase_url, session, "demo", entries)["id"])
    put(metabase_url, session, f"/api/card/{question['id']}", {"name": "Edited by a test"})
    put(metabase_url, session, f"/api/dashboard/{board['id']}", {"archived": True})
    graph = get(metabase_url, session, "/api/collection/graph")
    _, all_users = group_ids(metabase_url, session)
    body = {"revision": graph["revision"], "groups": {all_users: {demo_id: "write"}}}
    put(metabase_url, session, "/api/collection/graph", body)
    code, changes, found, _ = run_job("demo-init", secrets(env))
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert item(metabase_url, session, "demo/spend-by-day", entries)["name"] == "Spend by day"
    board = item(metabase_url, session, "demo/dashboard", entries)
    assert (board["archived"], board["collection_id"]) == (False, int(demo_id))
    assert len(board["dashcards"]) == len(QUESTIONS)
    graph = get(metabase_url, session, "/api/collection/graph")
    assert graph["groups"][all_users][demo_id] == "read"
    assert changes == [
        'demo-init: revert of an edit: question demo/spend-by-day name: "Edited by a test" -> '
        '"Spend by day"',
        "demo-init: restored from the trash: dashboard demo/dashboard",
        "demo-init: lowered: group 'All Users' on collection Demo: curate -> view",
    ], changes


def test_a_collection_with_the_same_name_is_not_taken_for_the_package(env, metabase_url, session):
    decoy = post(metabase_url, session, "/api/collection", {"name": "Agoranomos"})
    try:
        code, changes, found, _ = run_job("demo-init", secrets(env))
        assert not found, f"the output of demo-init holds the values of {found}"
        assert code == 0, f"demo-init exited with {code}"
        assert changes == [], changes
        assert item(metabase_url, session, "agoranomos")["id"] != decoy["id"]
        kept = get(metabase_url, session, f"/api/collection/{decoy['id']}")
        assert (kept["name"], kept["location"], kept["archived"]) == ("Agoranomos", "/", False)
    finally:
        put(metabase_url, session, f"/api/collection/{decoy['id']}", {"archived": True})


def test_the_collection_of_v0_1_moves_into_agoranomos(env, metabase_url, session):
    # A Metabase of v0.1, made on this stand: the content of the package goes to the trash, is
    # deleted from it, and leaves the registry, and a collection of the old name at the root, with
    # the questions and the dashboard of v0.1 in it, takes its place.
    root = item(metabase_url, session, "agoranomos")
    put(metabase_url, session, f"/api/collection/{root['id']}", {"archived": True})
    delete(metabase_url, session, f"/api/collection/{root['id']}")
    removed = in_metabase_init("rm", "-f", REGISTRY)
    assert removed.returncode == 0, f"the registry was not removed: exit code {removed.returncode}"
    old = post(metabase_url, session, "/api/collection", {"name": "Agoranomos demo"})
    database_id = warehouse_database_id(metabase_url, session)
    native = {"database": database_id, "type": "native", "native": {"query": "SELECT 1"}}
    cards = {
        name: post(
            metabase_url,
            session,
            "/api/card",
            {
                "name": name,
                "collection_id": old["id"],
                "display": "table",
                "visualization_settings": {},
                "dataset_query": native,
            },
        )
        for name in QUESTIONS.values()
    }
    board = post(
        metabase_url, session, "/api/dashboard", {"name": DASHBOARD, "collection_id": old["id"]}
    )
    code, changes, found, _ = run_job("demo-init", secrets(env))
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert (
        "demo-init: moved: collection 'Agoranomos demo' of an earlier release into Agoranomos as "
        "Demo"
    ) in changes, changes
    entries = registry()
    demo = item(metabase_url, session, "demo", entries)
    root = item(metabase_url, session, "agoranomos", entries)
    assert (demo["id"], demo["name"], demo["location"]) == (old["id"], "Demo", f"/{root['id']}/")
    assert item(metabase_url, session, "demo/dashboard", entries)["id"] == board["id"]
    adopted = {key: item(metabase_url, session, f"demo/{key}", entries) for key in QUESTIONS}
    assert {q["id"] for q in adopted.values()} == {c["id"] for c in cards.values()}
    assert adopted["spend-by-day"]["dataset_query"]["native"]["query"] != "SELECT 1"
    # The questions of v0.1 were adopted and put right, not replaced: none is in the trash.
    assert [q["archived"] for q in adopted.values()] == [False] * len(QUESTIONS)
