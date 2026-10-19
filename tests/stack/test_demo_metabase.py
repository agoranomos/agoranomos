"""The demo in Metabase, made by demo-init: the collection Agoranomos / Demo, its questions and its
dashboard, found through the registry of the content, each once, with the numbers of the mart,
view-only for every group but Administrators (ADR-0018); demo-init runs again
without a change, puts an edit back, never takes a collection of the same name for the package,
and moves the demo collection of v0.1.

The tests change the content as an admin could, and the next run of demo-init puts it back. The
test of the move runs last: it rebuilds the content. Answers of Metabase and the output of
demo-init could hold secrets: the assertions print names, counts, sums and states only.
"""

from decimal import Decimal

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import is_busy, run_count
from metabase_content import (
    REGISTRY,
    another_account,
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
LOAD = "agoranomos_demo_history"
TRANSFORM = "agoranomos_demo_transform"
DAGS = (LOAD, TRANSFORM)
# The spend of the last seven days of the mart, in the reporting currency, as the question of the
# demo adds it up.
SPEND_LAST_7_DAYS = (
    "SELECT sum(spend_reporting) FROM marts.ad_stats_campaign_daily WHERE report_date > "
    "(SELECT max(report_date) FROM marts.ad_stats_campaign_daily) - 7"
)
# The keys of the content of the demo in the registry.
KEYS = ("agoranomos", "demo", "demo/dashboard", *(f"demo/{key}" for key in QUESTIONS))


@pytest.fixture(scope="module", autouse=True)
def idle_demo_dags(airflow_url, token):
    """The tests of the history run before these, and each of their histories starts a run of the
    transform. These tests wait until such runs have ended, so that none starts or rebuilds the
    mart while they count runs or read the mart."""
    wait_for(
        lambda: (
            not (
                is_busy(airflow_url, token, LOAD)
                or is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS))
            )
        ),
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


def test_the_spend_of_the_last_seven_days_is_that_of_the_mart(env, metabase_url, session):
    card_id = item(metabase_url, session, "demo/spend-last-7-days")["id"]
    status, result = http_json("POST", f"{metabase_url}/api/card/{card_id}/query", headers=session)
    # Metabase 0.63.19 and 0.64.1 answer 202, with the rows in data.rows.
    state = result.get("status") if isinstance(result, dict) else None
    assert (status, state) in {(200, "completed"), (202, "completed")}, (status, state)
    [[spend]] = result["data"]["rows"]
    expected = Decimal(warehouse_output(env, SPEND_LAST_7_DAYS))
    assert expected > 0, "the mart has no spend in its last seven days"
    # Metabase answers with a number in JSON, which may round the last of the six places.
    assert abs(Decimal(str(spend)) - expected) < Decimal("0.0001"), (spend, expected)


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
    assert ("marts", "ad_stats_campaign_daily") in tables


def test_the_first_start_ran_the_demo_pipeline(airflow_url, token):
    # What the first start did, whatever a person did since, such as a pause, which demo-init
    # keeps: the history succeeded, and its assets started a run of the transform that succeeded.
    # Counted by total_entries, as Airflow keeps every run.
    assert run_count(airflow_url, token, LOAD, states=("success",)) > 0, f"{LOAD} never ran"
    succeeded = run_count(
        airflow_url, token, TRANSFORM, states=("success",), run_types=("asset_triggered",)
    )
    assert succeeded > 0, f"{TRANSFORM} has no successful run that the history started"


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
        'demo-init: lowered: group "All Users" on collection "Demo": curate -> view',
    ], changes


def test_a_click_behavior_and_an_extra_setting_are_reverted_by_the_next_start(
    env, metabase_url, session
):
    # An admin gives a card of the dashboard a click behavior, which could send whoever clicks it
    # to any address, and a question a setting that the code does not set.
    entries = registry()
    board = item(metabase_url, session, "demo/dashboard", entries)
    question = item(metabase_url, session, "demo/spend-by-day", entries)
    template = "https://example.com/{{spend_reporting}}"
    link = {"type": "link", "linkType": "url", "linkTemplate": template}
    dashcards = [
        {
            **{key: d[key] for key in ("id", "card_id", "row", "col", "size_x", "size_y")},
            "visualization_settings": {"click_behavior": link} if n == 0 else {},
        }
        for n, d in enumerate(board["dashcards"])
    ]
    put(metabase_url, session, f"/api/dashboard/{board['id']}", {"dashcards": dashcards})
    settings = {**question["visualization_settings"], "graph.show_values": True}
    put(metabase_url, session, f"/api/card/{question['id']}", {"visualization_settings": settings})
    code, changes, found, _ = run_job("demo-init", secrets(env))
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    board = item(metabase_url, session, "demo/dashboard", entries)
    assert [d["visualization_settings"] for d in board["dashcards"]] == [{}] * len(QUESTIONS)
    settings = item(metabase_url, session, "demo/spend-by-day", entries)["visualization_settings"]
    assert "graph.show_values" not in settings
    assert changes == [
        "demo-init: revert of an edit: question demo/spend-by-day visualization_settings",
        "demo-init: revert of an edit: dashboard demo/dashboard cards: 3 cards (1 not as the "
        "code says) and no filter -> 3 cards and no filter",
    ], changes


def test_a_description_written_into_the_columns_of_a_question_is_reverted(
    env, metabase_url, session
):
    # The metadata of the columns of a question is what people and AI clients read as their display
    # names and descriptions; Metabase 0.64.1 keeps what a request sends there with the query
    # unchanged. The next start drops it, and Metabase computes it again from the query.
    question = item(metabase_url, session, "demo/spend-by-day")
    computed = question["result_metadata"]
    assert computed, "Metabase computed no metadata for the question"
    written = [{**column, "description": "Written by a stack test"} for column in computed]
    put(metabase_url, session, f"/api/card/{question['id']}", {"result_metadata": written})
    code, changes, found, _ = run_job("demo-init", secrets(env))
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert changes == [
        "demo-init: revert of an edit: question demo/spend-by-day result_metadata"
    ], changes
    metadata = wait_for(
        lambda: item(metabase_url, session, "demo/spend-by-day")["result_metadata"],
        60,
        interval=2,
        message="Metabase did not compute the metadata of the question again",
    )
    assert metadata == computed


def test_another_account_cannot_put_anything_into_the_package(metabase_url, session):
    # Agoranomos and Demo are view-only for every group but Administrators: a second account, in
    # All Users only, can make neither a collection in Agoranomos nor a question in Demo.
    entries = registry()
    root = item(metabase_url, session, "agoranomos", entries)
    demo = item(metabase_url, session, "demo", entries)
    database_id = warehouse_database_id(metabase_url, session)
    native = {"database": database_id, "type": "native", "native": {"query": "SELECT 1"}}
    question = {"name": "Mine", "display": "table", "visualization_settings": {}}
    with another_account(metabase_url, session) as theirs:
        tries = {
            "a collection in Agoranomos": (
                "/api/collection",
                {"name": "Official notes", "parent_id": root["id"]},
            ),
            "a question in Demo": (
                "/api/card",
                {**question, "collection_id": demo["id"], "dataset_query": native},
            ),
        }
        answers = {
            what: http_json("POST", f"{metabase_url}{path}", body, headers=theirs)[0]
            for what, (path, body) in tries.items()
        }
    assert answers == dict.fromkeys(tries, 403), answers


def test_a_collection_that_an_admin_opens_inside_agoranomos_is_locked_by_the_next_start(
    env, metabase_url, session
):
    # An admin makes a collection inside Agoranomos and lets All Users curate it: the next run of
    # metabase-init, which every start runs, lowers it to View and says so.
    root = item(metabase_url, session, "agoranomos")
    inner = post(
        metabase_url, session, "/api/collection", {"name": "Inner", "parent_id": root["id"]}
    )
    try:
        _, all_users = group_ids(metabase_url, session)
        graph = get(metabase_url, session, "/api/collection/graph")
        body = {"revision": graph["revision"], "groups": {all_users: {str(inner["id"]): "write"}}}
        put(metabase_url, session, "/api/collection/graph", body)
        code, changes, found, _ = run_job("metabase-init", secrets(env))
        assert not found, f"the output of metabase-init holds the values of {found}"
        assert code == 0, f"metabase-init exited with {code}"
        graph = get(metabase_url, session, "/api/collection/graph")
        assert graph["groups"][all_users][str(inner["id"])] == "read"
        assert changes == [
            'metabase-init: lowered: group "All Users" on collection "Inner": curate -> view'
        ], changes
    finally:
        put(metabase_url, session, f"/api/collection/{inner['id']}", {"archived": True})


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
    # deleted from it, and leaves the registry. A second account, which may curate the root as on
    # v0.1, makes a decoy of the old name first, with a dashboard of the old name; the admin then
    # makes the collection of v0.1 with its questions and its dashboard; and the second account
    # adds a question of a name of v0.1 to it.
    root = item(metabase_url, session, "agoranomos")
    put(metabase_url, session, f"/api/collection/{root['id']}", {"archived": True})
    delete(metabase_url, session, f"/api/collection/{root['id']}")
    removed = in_metabase_init("rm", "-f", REGISTRY)
    assert removed.returncode == 0, f"the registry was not removed: exit code {removed.returncode}"
    database_id = warehouse_database_id(metabase_url, session)
    native = {"database": database_id, "type": "native", "native": {"query": "SELECT 1"}}

    def question(headers, collection_id, name):
        body = {
            "name": name,
            "collection_id": collection_id,
            "display": "table",
            "visualization_settings": {},
            "dataset_query": native,
        }
        return post(metabase_url, headers, "/api/card", body)

    with another_account(metabase_url, session) as theirs:
        decoy = post(metabase_url, theirs, "/api/collection", {"name": "Agoranomos demo"})
        decoy_board = post(
            metabase_url,
            theirs,
            "/api/dashboard",
            {"name": DASHBOARD, "collection_id": decoy["id"]},
        )
        old = post(metabase_url, session, "/api/collection", {"name": "Agoranomos demo"})
        cards = {name: question(session, old["id"], name) for name in QUESTIONS.values()}
        board = post(
            metabase_url, session, "/api/dashboard", {"name": DASHBOARD, "collection_id": old["id"]}
        )
        added = question(theirs, old["id"], "Spend by day")
        trashed = question(theirs, old["id"], "Mine")
        put(metabase_url, theirs, f"/api/card/{trashed['id']}", {"archived": True})
    try:
        code, changes, found, _ = run_job("demo-init", secrets(env))
        assert not found, f"the output of demo-init holds the values of {found}"
        assert code == 0, f"demo-init exited with {code}"
        assert (
            "demo-init: moved: collection 'Agoranomos demo' of an earlier release into Agoranomos "
            "as Demo"
        ) in changes, changes
        assert (
            f"demo-init: moved out: question {added['id']} of the collection of v0.1 to the root, "
            "as the demo does not ship it"
        ) in changes, changes
        entries = registry()
        demo = item(metabase_url, session, "demo", entries)
        root = item(metabase_url, session, "agoranomos", entries)
        assert (demo["id"], demo["name"], demo["location"]) == (
            old["id"],
            "Demo",
            f"/{root['id']}/",
        )
        assert item(metabase_url, session, "demo/dashboard", entries)["id"] == board["id"]
        adopted = {key: item(metabase_url, session, f"demo/{key}", entries) for key in QUESTIONS}
        assert {q["id"] for q in adopted.values()} == {c["id"] for c in cards.values()}
        assert adopted["spend-by-day"]["dataset_query"]["native"]["query"] != "SELECT 1"
        # The questions of v0.1 were adopted and put right, not replaced: none is in the trash.
        assert [q["archived"] for q in adopted.values()] == [False] * len(QUESTIONS)
        # The question of the second account is at the root, and the decoy is as it was.
        assert get(metabase_url, session, f"/api/card/{added['id']}")["collection_id"] is None
        # The one in the trash went to the root too, and stays in the trash: restored, it is not
        # in Demo. The list of the trash keeps the collection of each question.
        in_trash = {c["id"]: c for c in get(metabase_url, session, "/api/card?f=archived")}
        assert in_trash[trashed["id"]]["collection_id"] is None
        kept = get(metabase_url, session, f"/api/collection/{decoy['id']}")
        assert (kept["name"], kept["location"], kept["archived"]) == ("Agoranomos demo", "/", False)
        kept_board = get(metabase_url, session, f"/api/dashboard/{decoy_board['id']}")
        assert kept_board["collection_id"] == decoy["id"]
    finally:
        put(metabase_url, session, f"/api/collection/{decoy['id']}", {"archived": True})
        put(metabase_url, session, f"/api/card/{added['id']}", {"archived": True})
        put(metabase_url, session, f"/api/card/{trashed['id']}", {"archived": True})
