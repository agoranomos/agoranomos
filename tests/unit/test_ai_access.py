"""Tests of metabase/ai_access.py: the levels of AI access in Metabase (ADR-0017), which
./agoranomos checks before Compose starts and metabase-init checks again."""

from __future__ import annotations

import re
from pathlib import Path

import ai_access
import content
import metabase_init as mi
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = (REPO_ROOT / "agoranomos").read_text(encoding="utf-8")

# The settings of a .env that ./agoranomos init made from .env.example.
FRESH = {
    "METABASE_AI_LEVEL": "off",
    "METABASE_AI_DEFAULT_LEVEL": "explore",
    "METABASE_MCP": "false",
    "METABASE_MCP_SQL": "false",
}


def script_list(name: str) -> tuple[str, ...]:
    match = re.search(rf'^{name}="([^"]*)"$', SCRIPT, flags=re.MULTILINE)
    assert match, f"{name} is not in ./agoranomos"
    return tuple(match.group(1).split())


def test_the_script_and_the_job_know_the_same_levels():
    # ./agoranomos checks before Compose starts and metabase-init after: the two must agree.
    assert script_list("AI_LEVELS") == ai_access.LEVELS
    assert script_list("AI_LEVELS_OFFERED") == ai_access.OFFERED


def test_the_offered_levels_are_the_lowest_ones():
    # A default no higher than an offered level is then offered too.
    assert ai_access.LEVELS[: len(ai_access.OFFERED)] == ai_access.OFFERED


def test_compose_gives_a_setting_that_is_not_set_the_default_of_the_job():
    # The job reads the values that compose.yaml hands it, and Metabase gets the switches from the
    # same settings with the same defaults.
    compose = (REPO_ROOT / "compose.yaml").read_text(encoding="utf-8")
    for name, default in ai_access.DEFAULTS.items():
        assert f"{name}: ${{{name}:-{default}}}" in compose, name
    for switch in ("MB_AI_FEATURES_ENABLED", "MB_MCP_ENABLED"):
        assert f"{switch}: ${{METABASE_MCP:-false}}" in compose, switch
    assert "MB_MCP_EXECUTE_SQL_ENABLED: ${METABASE_MCP_SQL:-false}" in compose


def test_env_example_has_no_problem():
    example = dict(
        line.split("=", 1)
        for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    assert set(ai_access.DEFAULTS) <= set(example)
    assert ai_access.problem(example) is None


@pytest.mark.parametrize(
    ("level", "switches"),
    [
        ("off", ("false", "false")),
        ("answers", ("true", "false")),
        ("explore", ("true", "false")),
        ("build", ("true", "false")),
        ("build-sql", ("true", "true")),
    ],
)
def test_the_switches_follow_the_level(level, switches):
    assert ai_access.switches(level) == switches


def test_a_fresh_env_has_no_problem():
    assert ai_access.problem(FRESH) is None


def test_empty_or_missing_settings_have_their_defaults():
    assert ai_access.problem(dict.fromkeys(FRESH, "")) is None
    assert ai_access.problem({}) is None


@pytest.mark.parametrize(
    ("changes", "words"),
    [
        ({"METABASE_AI_LEVEL": "everything"}, "is not a level of AI access in Metabase"),
        ({"METABASE_AI_LEVEL": "build-sql"}, "a level that this release does not offer"),
        ({"METABASE_AI_LEVEL": "build-sql"}, "MCP Toolbox runs with the role ai_reader"),
        ({"METABASE_AI_LEVEL": "build"}, "do not follow METABASE_AI_LEVEL=build"),
        ({"METABASE_AI_DEFAULT_LEVEL": "off"}, "not a level that every account can get"),
        ({"METABASE_AI_DEFAULT_LEVEL": "most"}, "not a level that every account can get"),
        ({"METABASE_MCP": "true"}, "do not follow METABASE_AI_LEVEL=off"),
        ({"METABASE_MCP_SQL": "true"}, "do not follow METABASE_AI_LEVEL=off"),
        ({"METABASE_MCP": "yes"}, "do not follow METABASE_AI_LEVEL=off"),
    ],
)
def test_a_problem_names_the_setting_and_the_command(changes, words):
    message = ai_access.problem({**FRESH, **changes})
    assert message is not None
    assert words in message
    assert "./agoranomos ai set" in message


def test_a_default_higher_than_the_level_is_a_problem():
    settings = {**FRESH, "METABASE_AI_LEVEL": "answers", "METABASE_MCP": "true"}
    assert ai_access.problem({**settings, "METABASE_AI_DEFAULT_LEVEL": "answers"}) is None
    message = ai_access.problem({**settings, "METABASE_AI_DEFAULT_LEVEL": "explore"})
    assert message is not None
    assert "higher than METABASE_AI_LEVEL=answers" in message


def test_metabase_init_stops_on_a_problem_before_it_reaches_metabase(capsys):
    # A port that nothing listens on: the job would wait for Metabase there for ten minutes, so a
    # quick answer shows that it stopped before.
    env = {**FRESH, "METABASE_URL": "http://127.0.0.1:9", "METABASE_MCP": "true"}
    assert mi.main(env) == 1
    error = capsys.readouterr().err
    assert error.startswith("metabase-init: METABASE_MCP and METABASE_MCP_SQL in .env")


# The permissions of the levels.

WAREHOUSE, OTHER = "1", "2"
DATABASES = {WAREHOUSE: "Agoranomos warehouse", OTHER: "Other database"}
ALL_USERS, ADMINS, BUILDERS, EXPLORERS, MARKETING = "1", "2", "5", "6", "7"
NAMES = {
    ALL_USERS: "All Users",
    ADMINS: "Administrators",
    BUILDERS: "Agoranomos: Builders",
    EXPLORERS: "Agoranomos: Explorers",
    MARKETING: "Marketing",
}


@pytest.mark.parametrize(
    ("group_level", "level", "default", "expected"),
    [
        ("build", "build", "explore", "build"),
        ("explore", "build", "explore", "explore"),
        # A group at or below the default gets the default.
        ("explore", "build", "build", "build"),
        # A group above the ceiling gets the ceiling.
        ("build", "explore", "explore", "explore"),
        ("build-sql", "build", "answers", "build"),
    ],
)
def test_a_group_of_the_setup_stays_between_the_default_and_the_ceiling(
    group_level, level, default, expected
):
    assert ai_access.effective(group_level, level, default) == expected


@pytest.mark.parametrize(
    ("value", "limit", "expected"),
    [
        ("query-builder-and-native", "query-builder", "query-builder"),
        ("query-builder", "query-builder", "query-builder"),
        ("no", "query-builder", "no"),
        ("query-builder", "no", "no"),
        # Granular: each schema and table comes down, and none goes up.
        (
            {"public": "query-builder-and-native", "staging": {"7": "no", "8": "query-builder"}},
            "query-builder",
            {"public": "query-builder", "staging": {"7": "no", "8": "query-builder"}},
        ),
        # A value that is not known counts as the highest.
        ("everything", "no", "no"),
    ],
)
def test_create_queries_come_down_to_a_limit_and_never_go_up(value, limit, expected):
    assert ai_access.capped_queries(value, limit) == expected


def test_all_users_get_the_default_on_the_warehouse_and_at_most_it_elsewhere():
    graph = {
        ALL_USERS: {
            WAREHOUSE: {"view-data": "unrestricted", "create-queries": "query-builder-and-native"},
            OTHER: {"view-data": "unrestricted", "create-queries": "query-builder-and-native"},
        }
    }
    changes, lines = ai_access.data_plan(
        graph, {ALL_USERS: "answers"}, DATABASES, WAREHOUSE, "answers", NAMES
    )
    assert changes == {
        ALL_USERS: {WAREHOUSE: {"create-queries": "no"}, OTHER: {"create-queries": "no"}}
    }
    assert lines == [
        'set: group "All Users" on database "Agoranomos warehouse": create queries query builder '
        "and native -> no",
        'set: group "All Users" on database "Other database": create queries query builder and '
        "native -> no",
    ]


def test_a_group_of_the_setup_gets_its_level_on_the_warehouse_and_the_default_elsewhere():
    # A group that is not in the graph, or has no Create queries, has none, as Metabase reads it.
    graph = {BUILDERS: {OTHER: {"create-queries": "query-builder-and-native"}}}
    changes, _ = ai_access.data_plan(
        graph, {BUILDERS: "build"}, DATABASES, WAREHOUSE, "explore", NAMES
    )
    assert changes == {
        BUILDERS: {
            WAREHOUSE: {"create-queries": "query-builder"},
            OTHER: {"create-queries": "query-builder"},
        }
    }


def test_the_data_plan_is_empty_when_the_graph_has_what_the_levels_say():
    graph = {ALL_USERS: {WAREHOUSE: {"create-queries": "query-builder"}, OTHER: {}}}
    plan = ai_access.data_plan(
        graph, {ALL_USERS: "explore"}, DATABASES, WAREHOUSE, "explore", NAMES
    )
    assert plan == ({}, [])


ROOT, AGORANOMOS, TEAM, WORKSPACE, DRAFTS, REPORTS = "root", "3", "6", "10", "11", "12"
COLLECTIONS = {
    ROOT: "Our analytics",
    AGORANOMOS: "Agoranomos",
    TEAM: "Team stuff",
    WORKSPACE: "Workspace",
    DRAFTS: "Drafts",
    REPORTS: "Reports",
}


def plan_collections(graph, roles, default="explore"):
    return ai_access.collection_plan(
        graph,
        COLLECTIONS,
        frozenset({WORKSPACE, DRAFTS}),
        frozenset({REPORTS}),
        REPORTS,
        roles,
        default,
        NAMES,
    )


def test_all_users_view_the_root_and_reports_and_at_most_view_the_rest():
    # A new Metabase gives All Users Curate on the root and on what is made there.
    graph = {ALL_USERS: {ROOT: "write", TEAM: "write", WORKSPACE: "write", DRAFTS: "write"}}
    changes, lines = plan_collections(graph, {ALL_USERS: "all"})
    assert changes == {
        ALL_USERS: {
            ROOT: "read",
            TEAM: "read",
            WORKSPACE: "none",
            DRAFTS: "none",
            REPORTS: "read",
        }
    }
    assert 'lowered: group "All Users" on collection "Team stuff": curate -> view' in lines
    assert 'raised: group "All Users" on collection "Reports": no access -> view' in lines
    assert 'lowered: group "All Users" on collection "Workspace": curate -> no access' in lines


def test_all_users_curate_workspace_when_the_default_is_build():
    changes, _ = plan_collections({ALL_USERS: {ROOT: "read"}}, {ALL_USERS: "all"}, "build")
    assert changes[ALL_USERS][WORKSPACE] == changes[ALL_USERS][DRAFTS] == "write"


def test_a_group_of_the_setup_curates_workspace_from_build_up_and_views_the_rest():
    graph = {BUILDERS: {TEAM: "write", REPORTS: "write"}, EXPLORERS: {WORKSPACE: "write"}}
    changes, _ = plan_collections(graph, {BUILDERS: "build", EXPLORERS: "explore"})
    assert changes == {
        BUILDERS: {TEAM: "read", WORKSPACE: "write", DRAFTS: "write", REPORTS: "read"},
        EXPLORERS: {WORKSPACE: "none"},
    }


def test_a_group_of_the_deployment_keeps_its_permissions_but_curate_on_reports():
    graph = {MARKETING: {TEAM: "write", WORKSPACE: "write", REPORTS: "write"}}
    changes, lines = plan_collections(graph, {MARKETING: None})
    assert changes == {MARKETING: {REPORTS: "read"}}
    assert lines == ['lowered: group "Marketing" on collection "Reports": curate -> view']


class FakeMetabase:
    """What ensure_groups, ensure_spaces and put_graph ask of the client of metabase_init.py: the
    answers of the calls by method and path, and a record of every call."""

    def __init__(self, answers=None, statuses=()):
        self.answers = answers or {}
        self.statuses = list(statuses)
        self.calls = []

    def call(self, method, path, body=None, missing_ok=False):
        self.calls.append((method, path, body))
        answer = self.answers.get((method, path))
        if callable(answer):
            return answer(body)
        if answer is None and missing_ok:
            return None
        return answer

    def send(self, method, path, body=None):
        self.calls.append((method, path, body))
        return (self.statuses.pop(0) if self.statuses else 200), None


def registry(tmp_path):
    return content.Registry(tmp_path / "registry.json")


GROUPS = [
    {"id": 1, "name": "All Users", "magic_group_type": "all-internal-users", "entity_id": "a" * 21},
    {"id": 2, "name": "Administrators", "magic_group_type": "admin", "entity_id": "b" * 21},
]


def test_the_groups_above_the_default_up_to_the_ceiling_are_created(tmp_path):
    made = []

    def create(body):
        made.append(body["name"])
        return {"id": 10 + len(made), "name": body["name"], "entity_id": f"{len(made)}" * 21}

    metabase = FakeMetabase(
        {
            ("GET", "/api/permissions/group"): GROUPS,
            ("POST", "/api/permissions/group"): create,
        }
    )
    log = []
    found = ai_access.ensure_groups(metabase, registry(tmp_path), "build", "answers", log.append)
    assert made == ["Agoranomos: Explorers", "Agoranomos: Builders"]
    assert found == {
        "11": ("explore", "Agoranomos: Explorers"),
        "12": ("build", "Agoranomos: Builders"),
    }
    assert log == [
        'created: group "Agoranomos: Explorers"',
        'created: group "Agoranomos: Builders"',
    ]
    # The registry knows them now: a second run creates nothing.
    groups = [
        *GROUPS,
        {"id": 11, "name": "Agoranomos: Explorers", "entity_id": "1" * 21},
        {"id": 12, "name": "Agoranomos: Builders", "entity_id": "2" * 21},
    ]
    again = FakeMetabase({("GET", "/api/permissions/group"): groups})
    loaded = content.Registry(tmp_path / "registry.json")
    assert ai_access.ensure_groups(again, loaded, "build", "answers", log.append) == found
    assert [c for c in again.calls if c[0] == "POST"] == []


def test_a_group_with_the_name_of_a_group_of_the_setup_is_adopted(tmp_path):
    # Metabase allows a name once, in any case.
    groups = [*GROUPS, {"id": 9, "name": "agoranomos: builders", "entity_id": "c" * 21}]
    metabase = FakeMetabase({("GET", "/api/permissions/group"): groups})
    log = []
    store = registry(tmp_path)
    found = ai_access.ensure_groups(metabase, store, "build", "explore", log.append)
    assert found == {"9": ("build", "agoranomos: builders")}
    assert log == [
        'adopted: group "agoranomos: builders", which has the name of a group of the setup'
    ]
    assert store.get("access/group/build") == {"model": "group", "entity_id": "c" * 21}


def test_a_group_of_the_setup_that_is_no_longer_needed_stays_with_its_level(tmp_path):
    store = registry(tmp_path)
    store.set("access/group/build", "group", "c" * 21)
    groups = [*GROUPS, {"id": 9, "name": "Agoranomos: Builders", "entity_id": "c" * 21}]
    metabase = FakeMetabase({("GET", "/api/permissions/group"): groups})
    found = ai_access.ensure_groups(metabase, store, "explore", "explore", [].append)
    assert found == {"9": ("build", "Agoranomos: Builders")}


def test_the_registry_forgets_a_group_that_is_gone_and_not_needed(tmp_path):
    store = registry(tmp_path)
    store.set("access/group/build-sql", "group", "d" * 21)
    metabase = FakeMetabase({("GET", "/api/permissions/group"): GROUPS})
    assert ai_access.ensure_groups(metabase, store, "build", "build", [].append) == {}
    assert store.get("access/group/build-sql") is None


def test_the_collections_of_ai_access_are_not_content_of_the_package(tmp_path):
    # The lock-down of the package gives at most View on the collections of the registry: not on
    # Workspace, which builders curate.
    store = registry(tmp_path)
    store.set("agoranomos", "collection", "e" * 21)
    store.set("access/workspace", "collection", "f" * 21)
    store.set("access/group/build", "group", "c" * 21)
    assert store.collection_keys() == ["agoranomos"]
    loaded = content.Registry(tmp_path / "registry.json")
    assert loaded.get("access/group/build") == {"model": "group", "entity_id": "c" * 21}


def test_workspace_and_reports_are_created_and_brought_back_from_the_trash(tmp_path):
    store = registry(tmp_path)
    made = []

    def create(body):
        made.append(body["name"])
        return {"id": 20 + len(made), "name": body["name"], "entity_id": f"{len(made)}" * 21}

    metabase = FakeMetabase({("POST", "/api/collection"): create})
    log = []
    spaces = ai_access.ensure_spaces(metabase, store, log.append)
    assert made == ["Workspace", "Reports"]
    assert {key: space["id"] for key, space in spaces.items()} == {"workspace": 21, "reports": 22}
    assert log == [
        'created: collection access/workspace "Workspace"',
        'created: collection access/reports "Reports"',
    ]
    # A person put Reports in the trash, and renamed Workspace: the name stays.
    found = {
        ("GET", f"/api/collection/{'1' * 21}"): {"id": 21, "name": "Drafts", "archived": False},
        ("GET", f"/api/collection/{'2' * 21}"): {"id": 22, "name": "Reports", "archived": True},
        ("PUT", "/api/collection/22"): {"id": 22, "name": "Reports", "archived": False},
    }
    again = FakeMetabase(found)
    log.clear()
    spaces = ai_access.ensure_spaces(again, store, log.append)
    assert spaces["workspace"]["name"] == "Drafts"
    assert log == ["restored from the trash: collection access/reports"]


def test_a_graph_that_changed_in_between_is_read_and_planned_again():
    graph = {"revision": 4, "groups": {}}
    metabase = FakeMetabase({("GET", "/api/collection/graph"): graph}, statuses=[409, 200])
    log = []
    ai_access.put_graph(
        metabase,
        "/api/collection/graph",
        lambda g: ({"1": {"root": "read"}}, ["a line"]),
        log.append,
    )
    puts = [c for c in metabase.calls if c[0] == "PUT"]
    assert len(puts) == 2
    assert log == ["a line"]


def test_a_second_409_stops_the_job():
    graph = {"revision": 4, "groups": {}}
    metabase = FakeMetabase({("GET", "/api/collection/graph"): graph}, statuses=[409, 409])
    with pytest.raises(ai_access.AccessError, match="refused twice with HTTP 409"):
        ai_access.put_graph(
            metabase, "/api/collection/graph", lambda g: ({"1": {"root": "read"}}, []), print
        )


def test_off_changes_no_permission():
    metabase = FakeMetabase()
    ai_access.apply(metabase, None, 1, {"METABASE_AI_LEVEL": "off"}, print)
    assert metabase.calls == []
