"""The permissions of the level of AI access in Metabase on a running stack (ADR-0017).

The tests run when .env sets a level above off, as a new installation does: metabase-init has set
the permissions of the level on its start. They read the groups, the data graph and the collection
graph, and run metabase-init once more, which must change nothing then. They print names, levels
and permissions, never a value of .env.
"""

import ai_access
import pytest
from metabase_content import get, group_ids, run_job, warehouse_database_id
from services import AI_LEVEL, AI_ON
from stack_helpers import secrets

pytestmark = pytest.mark.skipif(not AI_ON, reason="AI access in Metabase is off in .env")


@pytest.fixture(scope="module")
def default(env):
    """The default level of every account in .env, or the default of compose.yaml."""
    return ai_access.settings(env)["METABASE_AI_DEFAULT_LEVEL"]


@pytest.fixture(scope="module")
def groups(metabase_url, session):
    """The groups of Metabase by name, and their IDs as the graphs name them."""
    return {g["name"]: str(g["id"]) for g in get(metabase_url, session, "/api/permissions/group")}


@pytest.fixture(scope="module")
def spaces(metabase_url, session):
    """Workspace and Reports, the collections of AI access, by name: the stack of the tests has
    no other collection of these names."""
    collections = get(metabase_url, session, "/api/collection")
    return {
        c["name"]: str(c["id"])
        for c in collections
        if c.get("name") in ("Workspace", "Reports") and not c.get("is_personal")
    }


def setup_levels(default):
    """The levels that have a group of the setup: above the default, up to the level of .env."""
    return [
        level
        for level in ai_access.GROUPS
        if ai_access.rank(default) < ai_access.rank(level) <= ai_access.rank(AI_LEVEL)
    ]


def test_each_level_above_the_default_has_a_group_of_the_setup(groups, default):
    names = {ai_access.GROUPS[level] for level in setup_levels(default)}
    assert names <= set(groups), sorted(names - set(groups))


def test_workspace_and_reports_exist(spaces):
    assert set(spaces) == {"Workspace", "Reports"}


def test_create_queries_on_the_warehouse_follow_the_levels(metabase_url, session, groups, default):
    graph = get(metabase_url, session, "/api/permissions/graph")["groups"]
    warehouse = str(warehouse_database_id(metabase_url, session))
    _, all_users = group_ids(metabase_url, session)
    expected = {"All Users": ai_access.CREATE_QUERIES[default]}
    expected |= {
        ai_access.GROUPS[level]: ai_access.CREATE_QUERIES[level] for level in setup_levels(default)
    }
    found = {
        name: (graph.get(groups[name] if name != "All Users" else all_users) or {})
        .get(warehouse, {})
        .get("create-queries", "no")
        for name in expected
    }
    assert found == expected


def test_all_users_view_the_root_and_reports_and_workspace_follows_the_default(
    metabase_url, session, spaces, default
):
    graph = get(metabase_url, session, "/api/collection/graph")["groups"]
    _, all_users = group_ids(metabase_url, session)
    held = graph.get(all_users) or {}
    curates = ai_access.rank(default) >= ai_access.rank(ai_access.CURATES_WORKSPACE)
    found = {
        "root": held.get("root", "none"),
        "Reports": held.get(spaces["Reports"], "none"),
        "Workspace": held.get(spaces["Workspace"], "none"),
    }
    assert found == {"root": "read", "Reports": "read", "Workspace": "write" if curates else "none"}


def test_the_groups_of_the_setup_curate_workspace_from_build_up(
    metabase_url, session, groups, spaces, default
):
    graph = get(metabase_url, session, "/api/collection/graph")["groups"]
    found = {
        ai_access.GROUPS[level]: (graph.get(groups[ai_access.GROUPS[level]]) or {}).get(
            spaces["Workspace"], "none"
        )
        for level in setup_levels(default)
    }
    expected = {
        ai_access.GROUPS[level]: "write"
        if ai_access.rank(level) >= ai_access.rank(ai_access.CURATES_WORKSPACE)
        else "none"
        for level in setup_levels(default)
    }
    assert found == expected


def test_metabase_init_changes_no_permission_when_they_are_set(env):
    # Every start runs it: once the permissions are as the level says, it prints no change.
    code, changes, found, _ = run_job("metabase-init", secrets(env))
    assert not found, f"the output of metabase-init holds the values of {found}"
    assert code == 0, f"metabase-init exited with {code}"
    assert changes == [], changes
