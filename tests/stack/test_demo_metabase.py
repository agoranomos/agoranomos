"""The demo in Metabase, made by demo-init: the collection Agoranomos / Demo, its questions and its
two dashboards, found through the registry of the content, each once, with the numbers of the marts
for the filters of the dashboards, view-only for every group but Administrators (ADR-0018);
demo-init runs again without a change, puts an edit back, never takes a collection of the same name
for the package, and moves the demo collection of v0.1.

The tests change the content as an admin could, and the next run of demo-init puts it back. The
test of the move runs last: it rebuilds the content. Answers of Metabase and the output of
demo-init could hold secrets: the assertions print names, counts, sums and states only.
"""

from datetime import date, timedelta
from decimal import Decimal

import demo_init
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

# The dashboards of the demo and its questions, by their keys in the package, with their names.
DASHBOARDS = {"advertising": "Advertising overview (demo)", "web-analytics": "Web analytics (demo)"}
QUESTIONS = {question["key"]: question["name"] for question in demo_init.QUESTIONS}
# The dashboard of each question, by its key.
BOARD_OF = {question["key"]: question["dashboard"] for question in demo_init.QUESTIONS}
# The questions and the dashboard of v0.1, by the keys of this release that they become.
V01_QUESTIONS = {
    "spend": "Spend, last 7 days",
    "top-campaigns": "Spend by campaign, last 7 days",
    "spend-by-day": "Spend by day",
}
V01_DASHBOARD = "Ad spend (demo)"
LOAD = "agoranomos_demo_history"
TRANSFORM = "agoranomos_demo_transform"
DAILY = "agoranomos_demo_daily"
DAGS = (LOAD, TRANSFORM, DAILY)
# The last day of the mart, and the sums of a client in the 30 days that end on it, in the
# reporting currency, as the questions of the demo add them up for the period and the client of
# their filters.
LAST_DAY = "SELECT max(report_date) FROM marts.ad_stats_campaign_daily"
CLIENT = "SELECT min(client_name) FROM marts.ad_stats_campaign_daily"
SPEND = (
    "SELECT sum(spend_reporting) FROM marts.ad_stats_campaign_daily "
    "WHERE report_date BETWEEN '{start}' AND '{end}' AND client_name = '{client}'"
)
SESSIONS = (
    "SELECT sum(sessions) FROM marts.web_sessions_daily "
    "WHERE report_date BETWEEN '{start}' AND '{end}' AND client_name = '{client}'"
)
# The keys of the content of the demo in the registry.
KEYS = (
    "agoranomos",
    "demo",
    *(f"demo/{key}" for key in DASHBOARDS),
    *(f"demo/{key}" for key in QUESTIONS),
)


@pytest.fixture(scope="module", autouse=True)
def idle_demo_dags(airflow_url, token):
    """The tests of the history run before these, and a run of the transform follows them. These
    tests wait until such runs have ended, so that none starts or rebuilds the mart while they
    count runs or read the mart."""
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
    whether it is in the trash, and for a question its query and settings of display, for a
    dashboard its cards with their filter mappings, and its filters."""
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
            sorted(
                (d.get("card_id"), str(d.get("parameter_mappings")))
                for d in found.get("dashcards") or []
            ),
            found.get("parameters"),
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
    for key, name in DASHBOARDS.items():
        board = item(metabase_url, session, f"demo/{key}", entries)
        assert (board["name"], board["collection_id"]) == (name, demo["id"])
        shown = sorted(dashcard["card_id"] for dashcard in board["dashcards"])
        wanted = [q["id"] for k, q in questions.items() if BOARD_OF[k] == key]
        assert shown == sorted(wanted)


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


def run_card(metabase_url, session, entries, board_key, card_key, values):
    """The answer of a card of a dashboard of the demo, as a view of the dashboard runs it, with
    values for the filters of the dashboard by their slugs; a filter without a value is left out,
    as Metabase leaves it when a person clears it. entries is the registry, read once by a test."""
    board = item(metabase_url, session, f"demo/{board_key}", entries)
    card = item(metabase_url, session, f"demo/{card_key}", entries)
    [dashcard] = [d for d in board["dashcards"] if d["card_id"] == card["id"]]
    filters = {f["id"]: f for f in board["parameters"]}
    parameters = [
        {
            "id": mapping["parameter_id"],
            "type": filters[mapping["parameter_id"]]["type"],
            "value": values[filters[mapping["parameter_id"]]["slug"]],
            "target": mapping["target"],
        }
        for mapping in dashcard["parameter_mappings"]
        if values.get(filters[mapping["parameter_id"]]["slug"]) is not None
    ]
    path = f"/api/dashboard/{board['id']}/dashcard/{dashcard['id']}/card/{card['id']}/query"
    status, result = http_json(
        "POST", f"{metabase_url}{path}", {"parameters": parameters}, headers=session
    )
    # Metabase 0.63.19 and 0.64.1 answer 202, with the rows in data.rows.
    state = result.get("status") if isinstance(result, dict) else None
    assert (status, state) in {(200, "completed"), (202, "completed")}, (status, state)
    return result["data"]["rows"]


@pytest.mark.parametrize(
    ("board", "card", "query"),
    [("advertising", "spend", SPEND), ("web-analytics", "sessions", SESSIONS)],
)
def test_a_number_of_a_dashboard_is_that_of_the_mart_for_its_filters(
    env, metabase_url, session, board, card, query
):
    # The 30 days that end on the last day of the mart, and one client: the numbers that the
    # filters of the dashboard give are those of the mart.
    last = date.fromisoformat(warehouse_output(env, LAST_DAY))
    start = last - timedelta(days=29)
    client = warehouse_output(env, CLIENT)
    values = {"period": f"{start}~{last}", "client": [client]}
    [[number]] = run_card(metabase_url, session, registry(), board, card, values)
    quoted = client.replace("'", "''")
    expected = Decimal(warehouse_output(env, query.format(start=start, end=last, client=quoted)))
    assert expected > 0, f"the mart has nothing for the client in the 30 days up to {last}"
    # Metabase answers with a number in JSON, which may round the last of the six places.
    assert abs(Decimal(str(number)) - expected) < Decimal("0.0001"), (number, expected)


def test_every_card_of_the_dashboards_answers_for_the_defaults_of_their_filters(
    metabase_url, session
):
    # The default period, the past 30 days, ends yesterday, and the demo has data up to then.
    entries = registry()
    for key, board in BOARD_OF.items():
        rows = run_card(metabase_url, session, entries, board, key, {"period": "past30days"})
        assert rows, f"the question {key} has no rows for the past 30 days"


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
    # Then it unpaused the daily load, which made a run at once, for the last time of its
    # schedule.
    scheduled = run_count(airflow_url, token, DAILY, run_types=("scheduled",))
    assert scheduled > 0, f"{DAILY} has no scheduled run: the first start did not unpause it"


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
    board = item(metabase_url, session, "demo/web-analytics", entries)
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
    assert (
        item(metabase_url, session, "demo/spend-by-day", entries)["name"]
        == QUESTIONS["spend-by-day"]
    )
    board = item(metabase_url, session, "demo/web-analytics", entries)
    assert (board["archived"], board["collection_id"]) == (False, int(demo_id))
    assert len(board["dashcards"]) == list(BOARD_OF.values()).count("web-analytics")
    graph = get(metabase_url, session, "/api/collection/graph")
    assert graph["groups"][all_users][demo_id] == "read"
    assert changes == [
        'demo-init: revert of an edit: question demo/spend-by-day name: "Edited by a test" -> '
        f'"{QUESTIONS["spend-by-day"]}"',
        "demo-init: restored from the trash: dashboard demo/web-analytics",
        'demo-init: lowered: group "All Users" on collection "Demo": curate -> view',
    ], changes


def test_a_click_behavior_and_an_extra_setting_are_reverted_by_the_next_start(
    env, metabase_url, session
):
    # An admin gives a card of the dashboard a click behavior, which could send whoever clicks it
    # to any address, and a question a setting that the code does not set.
    entries = registry()
    board = item(metabase_url, session, "demo/advertising", entries)
    question = item(metabase_url, session, "demo/by-client", entries)
    template = "https://example.com/{{spend_reporting}}"
    link = {"type": "link", "linkType": "url", "linkTemplate": template}
    kept = ("id", "card_id", "row", "col", "size_x", "size_y", "parameter_mappings")
    dashcards = [
        {
            **{key: d[key] for key in kept},
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
    board = item(metabase_url, session, "demo/advertising", entries)
    cards = len(board["dashcards"])
    assert [d["visualization_settings"] for d in board["dashcards"]] == [{}] * cards
    settings = item(metabase_url, session, "demo/by-client", entries)["visualization_settings"]
    assert "graph.show_values" not in settings
    assert changes == [
        "demo-init: revert of an edit: question demo/by-client visualization_settings",
        f"demo-init: revert of an edit: dashboard demo/advertising cards: {cards} cards (1 not as "
        f"the code says) and 3 filters -> {cards} cards and 3 filters",
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
            {"name": V01_DASHBOARD, "collection_id": decoy["id"]},
        )
        old = post(metabase_url, session, "/api/collection", {"name": "Agoranomos demo"})
        cards = {name: question(session, old["id"], name) for name in V01_QUESTIONS.values()}
        board = post(
            metabase_url,
            session,
            "/api/dashboard",
            {"name": V01_DASHBOARD, "collection_id": old["id"]},
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
        assert item(metabase_url, session, "demo/advertising", entries)["id"] == board["id"]
        adopted = {
            key: item(metabase_url, session, f"demo/{key}", entries) for key in V01_QUESTIONS
        }
        assert {q["id"] for q in adopted.values()} == {c["id"] for c in cards.values()}
        assert adopted["spend-by-day"]["dataset_query"]["native"]["query"] != "SELECT 1"
        # The questions of v0.1 were adopted and put right, not replaced: none is in the trash.
        assert [q["archived"] for q in adopted.values()] == [False] * len(V01_QUESTIONS)
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
