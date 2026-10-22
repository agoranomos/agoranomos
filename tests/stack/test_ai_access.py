"""The permissions of the level of AI access in Metabase on a running stack (ADR-0017).

The tests run when .env sets a level above off, as a new installation does: metabase-init has set
the permissions of the level on its start. They read the groups, the data graph and the collection
graph, and run metabase-init once more, and the dry run of ./agoranomos ai diff, which must find
nothing to change then. They print names, levels and permissions, never a value of .env.
"""

import base64
import hashlib
import html
import json
import re
import secrets
import subprocess
import urllib.error
import urllib.parse
import urllib.request

import ai_access
import pytest
from metabase_content import another_account, get, group_ids, run_job, warehouse_database_id
from services import AI_LEVEL, AI_ON, SERVER
from stack_helpers import REPO_ROOT, Secret, SecretHeaders, leaked
from stack_helpers import secrets as env_secrets

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
    code, changes, found, _ = run_job("metabase-init", env_secrets(env))
    assert not found, f"the output of metabase-init holds the values of {found}"
    assert code == 0, f"metabase-init exited with {code}"
    assert changes == [], changes


def agoranomos(env, *args):
    """A command of ./agoranomos, whose output must hold no value of .env."""
    result = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), *args],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    found = leaked(env_secrets(env), [result.stdout, result.stderr])
    assert not found, f"the output of ./agoranomos {' '.join(args)} holds the values of {found}"
    return result


def test_the_dry_run_finds_nothing_to_change_after_a_start(env):
    result = agoranomos(env, "ai", "diff")
    assert result.returncode == 0, f"ai diff exited with {result.returncode}: {result.stderr}"
    last = result.stdout.splitlines()[-1]
    assert last == "Metabase has the permissions of the level of .env: nothing to change"


def test_ai_shows_the_groups_of_the_setup_and_their_members(env, default):
    result = agoranomos(env, "ai")
    assert result.returncode == 0, f"ai exited with {result.returncode}: {result.stderr}"
    line = result.stdout.splitlines()[-1]
    assert line.startswith("Groups of the setup: "), line
    for level in setup_levels(default):
        assert f'"{ai_access.GROUPS[level]}" ' in line, line


# The OAuth clients of the MCP server (ADR-0031): a client that ai connect registers reaches the
# MCP server with the token of an account, until ai revoke deletes it from the tables of Metabase.


class KeepRedirects(urllib.request.HTTPRedirectHandler):
    """Hands a redirect back as the answer: the code of OAuth comes in the address of one."""

    def redirect_request(self, *args, **kwargs):
        return None


OAUTH = urllib.request.build_opener(urllib.request.ProxyHandler({}), KeepRedirects())


def oauth_send(method, url, form=None, headers=None, body=None):
    """One request of the flow of OAuth, with a form or a JSON body: the status, the headers and
    the text of the answer."""
    request = urllib.request.Request(url, headers=dict(headers or {}), method=method)
    if form is not None:
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
        request.data = urllib.parse.urlencode(form).encode()
    elif body is not None:
        request.add_header("Content-Type", "application/json")
        request.data = json.dumps(body).encode()
    try:
        with OAUTH.open(request, timeout=30) as response:
            return response.status, response.headers, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read().decode()


def oauth_token(metabase_url, person, client_id, redirect):
    """The access token of the account that person signs in, for the client, by the flow of a
    public client with PKCE: the request to authorize, the approval on the consent page, which
    Metabase guards with a cookie, and the exchange of the code."""
    _, _, text = oauth_send("GET", f"{metabase_url}/.well-known/oauth-protected-resource/api/mcp")
    resource = json.loads(text)
    verifier = Secret(secrets.token_urlsafe(48))
    digest = hashlib.sha256(verifier.encode()).digest()
    query = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect,
            "code_challenge": base64.urlsafe_b64encode(digest).rstrip(b"=").decode(),
            "code_challenge_method": "S256",
            "state": "stack-test",
            "scope": " ".join(resource["scopes_supported"]),
            "resource": resource["resource"],
        }
    )
    status, headers, page = oauth_send(
        "GET", f"{metabase_url}/oauth/authorize?{query}", None, person
    )
    assert status == 200, f"the request to authorize got HTTP {status}"
    form = {
        name: html.unescape(value)
        for name, value in re.findall(
            r'<input name="([a-z_]+)" type="hidden" value="([^"]*)"', page
        )
    }
    cookies = "; ".join(c.split(";", 1)[0] for c in headers.get_all("Set-Cookie") or [])
    status, headers, _ = oauth_send(
        "POST",
        f"{metabase_url}/oauth/authorize/decision",
        {**form, "approved": "true"},
        SecretHeaders({**person, "Cookie": Secret(cookies)}),
    )
    location = urllib.parse.urlparse(headers.get("Location") or "")
    assert status == 302, f"the approval got HTTP {status}"
    assert f"{location.scheme}://{location.netloc}{location.path}" == redirect
    code = Secret(urllib.parse.parse_qs(location.query)["code"][0])
    status, _, text = oauth_send(
        "POST",
        f"{metabase_url}/oauth/token",
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect,
            "client_id": client_id,
            "code_verifier": verifier,
            "resource": resource["resource"],
        },
    )
    assert status == 200, f"the exchange of the code got HTTP {status}"
    return Secret(json.loads(text)["access_token"])


# The first call of a session of MCP.
INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "agoranomos-stack-test", "version": "1"},
    },
}


def mcp_status(metabase_url, token):
    """The status that the MCP server of Metabase answers to an initialize with the token."""
    headers = SecretHeaders(
        {
            "Accept": "application/json, text/event-stream",
            "Authorization": Secret("Bearer " + token),
        }
    )
    status, _, _ = oauth_send("POST", f"{metabase_url}/api/mcp", headers=headers, body=INITIALIZE)
    return status


def client_id_of(result):
    """The client ID that ai connect printed."""
    found = re.search(r"Its client ID: (\S+)", result.stdout)
    assert found, result.stdout
    return found[1]


def test_a_client_of_ai_connect_reaches_the_mcp_server_until_ai_revoke_deletes_it(
    env, metabase_url, session
):
    port = "47999"
    result = agoranomos(env, "ai", "connect", "claude-code", "--port", port)
    assert result.returncode == 0, f"ai connect exited with {result.returncode}: {result.stderr}"
    client_id = client_id_of(result)
    revoked = False
    try:
        with another_account(metabase_url, session) as person:
            token = oauth_token(
                metabase_url, person, client_id, f"http://localhost:{port}/callback"
            )
            assert mcp_status(metabase_url, token) == 200
            listed = agoranomos(env, "ai", "clients").stdout.splitlines()
            [at] = [i for i, line in enumerate(listed) if line.strip().startswith(client_id)]
            assert '"Claude Code", returns to 127.0.0.1:47999 localhost:47999' in listed[at]
            assert listed[at + 1].strip().startswith("connected now: stack-test-")
            result = agoranomos(env, "ai", "revoke", client_id)
            revoked = result.returncode == 0
            assert revoked, f"ai revoke exited with {result.returncode}: {result.stderr}"
            # Metabase refuses the token of a deleted client at once.
            assert mcp_status(metabase_url, token) == 401
    finally:
        if not revoked:
            agoranomos(env, "ai", "revoke", client_id)
    assert client_id not in agoranomos(env, "ai", "clients").stdout


def test_ai_connect_claude_registers_the_claude_apps_in_server_mode_only(env):
    result = agoranomos(env, "ai", "connect", "claude")
    if not SERVER:
        assert result.returncode == 1
        assert "only server mode allows" in result.stderr
        return
    assert result.returncode == 0, f"ai connect exited with {result.returncode}: {result.stderr}"
    site = (env.get("AGORANOMOS_SITE_URL") or "").rstrip("/")
    assert f"  URL: {site}/api/mcp" in result.stdout.splitlines()
    revoked = agoranomos(env, "ai", "revoke", client_id_of(result))
    assert revoked.returncode == 0, revoked.stderr
