"""Tests of metabase/content.py, the content of the package in Metabase as code (ADR-0018),
against the stand-in of Metabase."""

import ast
import json
import sys
from pathlib import Path

import content as ct
import metabase_init as mi
import pytest
from metabase_stand_in import FakeClock, FakeMetabase, serve

QUESTION = ct.Question(
    key="spend",
    name="Spend",
    description="The spend.",
    display="scalar",
    query="SELECT 1 AS spend",
    visualization_settings={"column_settings": {'["name","spend"]': {"column_title": "Spend"}}},
)
PACKAGE = ct.Package(
    key="demo",
    name="Demo",
    description="The demo.",
    questions=(QUESTION,),
    dashboards=(
        ct.Dashboard("dashboard", "Board", "A board.", (ct.Placement("spend", 0, 0, 6, 4),)),
    ),
)


@pytest.fixture
def metabase():
    state = FakeMetabase(FakeClock())
    state.set_up("admin@example.com", "Secret-password-1")
    server = serve(state, "X-Metabase-Session")
    client = mi.Metabase(state.url)
    client.session = "session-from-login"
    yield state, client
    server.shutdown()
    server.server_close()


@pytest.fixture
def registry(tmp_path):
    return ct.Registry(tmp_path / "content.json")


def lines():
    found = []
    return found, found.append


def test_a_first_start_creates_the_content_and_records_it(metabase, registry):
    state, client = metabase
    log, say = lines()
    ids = ct.ensure_package(client, registry, PACKAGE, 2, say)
    content = state.content_state
    [root] = [c for c in content.collections.values() if c["location"] == "/"]
    [demo] = [c for c in content.collections.values() if c["location"] == f"/{root['id']}/"]
    assert (root["name"], demo["name"]) == ("Agoranomos", "Demo")
    assert content.cards[ids["spend"]]["collection_id"] == demo["id"]
    dashcards = content.dashboards[ids["dashboard"]]["dashcards"]
    assert [d["card_id"] for d in dashcards] == [ids["spend"]]
    saved = json.loads(registry.path.read_text(encoding="utf-8"))
    assert saved["format"] == 1
    assert set(saved["items"]) == {"agoranomos", "demo", "demo/spend", "demo/dashboard"}
    assert saved["items"]["demo/spend"] == {
        "model": "card",
        "entity_id": content.cards[ids["spend"]]["entity_id"],
    }
    assert any(line.startswith("created: collection agoranomos") for line in log)


def test_a_second_start_changes_nothing_and_logs_nothing(metabase, registry):
    state, client = metabase
    ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    saved = registry.path.read_text(encoding="utf-8")
    log, say = lines()
    puts = len(state.bodies)
    ct.ensure_package(client, registry, PACKAGE, 2, say)
    assert log == []
    assert [b for b in state.bodies[puts:] if b[0] in ("PUT", "POST")] == []
    assert registry.path.read_text(encoding="utf-8") == saved


def test_an_edit_is_reverted_and_each_change_is_one_line(metabase, registry):
    state, client = metabase
    ids = ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    card = state.content_state.cards[ids["spend"]]
    card.update(
        name="Renamed",
        dataset_query={"database": 2, "type": "native", "native": {"query": "SELECT 2"}},
    )
    card["visualization_settings"] = {}
    board = state.content_state.dashboards[ids["dashboard"]]
    board["dashcards"].append(
        {"id": 999, "card_id": None, "row": 4, "col": 0, "size_x": 4, "size_y": 2}
    )
    board["archived"] = True
    log, say = lines()
    ct.ensure_package(client, registry, PACKAGE, 2, say)
    assert card["name"] == "Spend"
    assert card["dataset_query"]["native"]["query"] == "SELECT 1 AS spend"
    assert card["visualization_settings"] == dict(QUESTION.visualization_settings)
    assert not board["archived"]
    assert [d["card_id"] for d in board["dashcards"]] == [ids["spend"]]
    assert 'revert of an edit: question demo/spend name: "Renamed" -> "Spend"' in log
    assert "restored from the trash: dashboard demo/dashboard" in log
    assert any(line.startswith("revert of an edit: dashboard demo/dashboard cards") for line in log)


def test_an_item_that_a_person_put_in_the_trash_comes_back_to_its_place_in_one_line(
    metabase, registry
):
    # Metabase shows a question that a person put in the trash in the collection of the trash:
    # that is no edit of its place, and the one line says where it was.
    state, client = metabase
    ids = ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    client.call("PUT", f"/api/card/{ids['spend']}", {"archived": True})
    log, say = lines()
    ct.ensure_package(client, registry, PACKAGE, 2, say)
    card = state.content_state.cards[ids["spend"]]
    demo = ct.find(client, "collection", registry.get("demo")["entity_id"])
    assert (card["archived"], card["collection_id"]) == (False, demo["id"])
    assert log == ["restored from the trash: question demo/spend"]


def test_a_dashboard_whose_cards_a_person_removed_gets_them_back_out_loud(metabase, registry):
    state, client = metabase
    ids = ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    board = state.content_state.dashboards[ids["dashboard"]]
    board["dashcards"] = []
    log, say = lines()
    ct.ensure_package(client, registry, PACKAGE, 2, say)
    assert [d["card_id"] for d in board["dashcards"]] == [ids["spend"]]
    assert log == [
        "revert of an edit: dashboard demo/dashboard cards: 0 cards and no filter -> 1 card "
        "and no filter"
    ]


def test_an_item_that_metabase_lost_is_created_again(metabase, registry):
    state, client = metabase
    ids = ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    del state.content_state.cards[ids["spend"]]
    log, say = lines()
    again = ct.ensure_package(client, registry, PACKAGE, 2, say)
    assert again["spend"] != ids["spend"]
    assert 'created again: question demo/spend "Spend"' in log


def test_a_collection_with_the_same_name_is_not_taken_for_the_package(metabase, registry):
    state, client = metabase
    decoy = state.content_state.add_collection("Agoranomos")
    ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    roots = [c for c in state.content_state.collections.values() if c["name"] == "Agoranomos"]
    assert len(roots) == 2
    assert registry.get("agoranomos")["entity_id"] != decoy["entity_id"]
    assert state.content_state.graph["1"][str(decoy["id"])] == "write", (
        "the decoy keeps its permissions"
    )


def test_every_group_but_administrators_gets_at_most_view(metabase, registry):
    state, client = metabase
    content = state.content_state
    content.groups.append({"id": 7, "name": "Analysts", "magic_group_type": None})
    content.graph["7"] = {"root": "none"}
    log, say = lines()
    ct.ensure_package(client, registry, PACKAGE, 2, say)
    for key in ("agoranomos", "demo"):
        collection = ct.find(client, "collection", registry.get(key)["entity_id"])
        assert content.graph["1"][str(collection["id"])] == "read"
        assert content.graph["2"][str(collection["id"])] == "write"
        assert content.graph["7"][str(collection["id"])] == "none"
    assert "lowered: group 'All Users' on collection Agoranomos: curate -> view" in log


def test_a_graph_that_changed_meanwhile_is_read_again(metabase, registry):
    state, client = metabase
    state.content_state.conflicts = 1
    ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    assert len(state.content_state.graph_puts) == 1


def test_an_item_that_the_code_no_longer_ships_goes_to_the_trash(metabase, registry):
    state, client = metabase
    ids = ct.ensure_package(client, registry, PACKAGE, 2, lambda line: None)
    smaller = ct.Package("demo", "Demo", "The demo.", (), ())
    log, say = lines()
    ct.ensure_package(client, registry, smaller, 2, say)
    assert state.content_state.cards[ids["spend"]]["archived"]
    assert registry.get("demo/spend") is None
    assert "retired: question demo/spend: moved to the trash, as the code no longer ships it" in log


def test_a_registry_that_is_not_json_stops_the_job_with_what_to_do(tmp_path):
    path = tmp_path / "content.json"
    path.write_text("{", encoding="utf-8")
    with pytest.raises(ct.ContentError, match="move it away"):
        ct.Registry(path)


def test_an_entry_of_the_registry_that_is_no_entity_id_is_left_out(tmp_path):
    # The registry names the paths of the requests of the jobs, which sign in as the admin.
    path = tmp_path / "content.json"
    items = {
        "demo": {"model": "collection", "entity_id": "../../api/user/1?x=y"},
        "demo/spend": {"model": "card", "entity_id": "B" * 21},
    }
    path.write_text(json.dumps({"format": 1, "items": items}), encoding="utf-8")
    registry = ct.Registry(path)
    assert registry.get("demo") is None
    assert registry.get("demo/spend") == {"model": "card", "entity_id": "B" * 21}


def test_the_registry_is_written_in_one_step(registry):
    registry.set("demo", "collection", "A" * 21)
    saved = json.loads(registry.path.read_text(encoding="utf-8"))
    assert saved["items"]["demo"]["entity_id"] == "A" * 21
    assert not list(registry.path.parent.glob(".*.tmp"))


def test_the_module_imports_only_the_standard_library_and_metadata():
    tree = ast.parse(Path(ct.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    allowed = set(sys.stdlib_module_names) | {"metadata"}
    assert imported <= allowed, sorted(imported - allowed)
