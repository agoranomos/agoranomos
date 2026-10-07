"""Metabase and its first-start setup, on a running stack.

The answers of Metabase and the environment of its containers hold secrets, and a failed assertion
prints what it compares: these tests print names, flags and status codes, never an answer. They
change no setting of Metabase. They read, they run one SELECT through it, they run metabase-init
again, which the stack does on every start anyway, and they try three changes of the LLM providers
that Metabase must refuse, and that would change nothing if it took them.
"""

import pytest
from ai_switches import readings, switches_on
from services import DEMO
from stack_helpers import compose, http_json, leaked, psql, secrets

WAREHOUSE_NAME = "Agoranomos warehouse"

# The AI entry points of Metabase 0.63 and 0.64, which are on by default and which compose.yaml
# switches off: each one is a setting, and the environment variable that sets it.
# ai-features-enabled? is the master switch that the other two also require, so the three are pinned
# one by one.
AI_SETTINGS = {
    "ai-features-enabled?": "MB_AI_FEATURES_ENABLED",
    "mcp-enabled?": "MB_MCP_ENABLED",
    "agent-api-enabled?": "MB_AGENT_API_ENABLED",
}
# What else compose.yaml fixes: nothing is reported to the makers of Metabase.
REPORTING_SETTINGS = {
    "anon-tracking-enabled": "MB_ANON_TRACKING_ENABLED",
    "check-for-updates": "MB_CHECK_FOR_UPDATES",
}
# The endpoint of the MCP server, and its legacy alias.
MCP_PATHS = ("/api/metabase-mcp", "/api/mcp")
INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "stack-test", "version": "0"},
    },
}
# The Anthropic key and the provider of Metabot: /api/llm does not read ai-features-enabled?, it is
# closed only while no key is saved, and a key saved in the admin UI would open it. In Metabase 0.64
# the keys are connections in the list of LLM providers, which MB_LLM_PROVIDERS keeps empty.
LLM_SETTINGS = ("llm-anthropic-api-key-configured?", "llm-metabot-configured?")
# Writes to the list of LLM providers that would change nothing even if Metabase took them: a
# provider type and a connection that do not exist. While MB_LLM_PROVIDERS sets the list, Metabase
# refuses each of them with 400 and a message that names the variable, before it looks at the
# request; without it, it answers 400 for the unknown type and 404 for the missing connection.
PROVIDER_WRITES = {
    "create": ("POST", "/api/llm/providers", {"type": "agoranomos-stack-test"}),
    "update": ("PUT", "/api/llm/providers/agoranomos-stack-test", {}),
    "delete": ("DELETE", "/api/llm/providers/agoranomos-stack-test", None),
}


@pytest.fixture(scope="module")
def properties(metabase_url):
    """What Metabase tells everyone, without a session: its public settings."""
    status, body = http_json("GET", f"{metabase_url}/api/session/properties")
    assert status == 200, f"GET /api/session/properties answered HTTP {status}"
    return body


def databases(metabase_url, session):
    status, response = http_json("GET", f"{metabase_url}/api/database", headers=session)
    assert status == 200, f"GET /api/database answered HTTP {status}"
    return response["data"]


def test_metabase_is_healthy(metabase_url):
    assert http_json("GET", f"{metabase_url}/api/health") == (200, {"status": "ok"})


def test_metabase_is_set_up(properties):
    assert properties["has-user-setup"] is True


def test_the_ai_settings_read_false(properties):
    # They are public settings, so an anonymous request shows what Metabase reads. mcp-enabled? and
    # agent-api-enabled? read false whenever ai-features-enabled? does, so they cannot show a
    # missing variable of their own: the next test does.
    states = {name: properties.get(name) for name in AI_SETTINGS}
    assert states == dict.fromkeys(AI_SETTINGS, False), states


def test_every_ai_switch_of_metabase_reads_false(metabase_url, session, properties):
    # compose.yaml fixes the three switches that Metabase 0.63 and 0.64 have. A later release can
    # add one that is on by default: this finds every setting that is named like an AI switch (see
    # ai_switches), so that a new one turns this test red. The names are printed, and never a value.
    status, admin_settings = http_json("GET", f"{metabase_url}/api/setting", headers=session)
    assert status == 200, f"GET /api/setting answered HTTP {status}"
    found = readings(properties, admin_settings)
    # Not vacuous: the sweep must see the switches that exist.
    known = {"ai-features-enabled?", "mcp-enabled?", "agent-api-enabled?"}
    assert known <= set(found), f"the sweep does not find {sorted(known - set(found))}"
    on = switches_on(found)
    assert not on, f"AI switches that read true: {on}"


def test_no_llm_key_is_saved(properties):
    # Metabase answers /api/llm when a key is saved, whatever ai-features-enabled? says, so a key
    # saved in the admin UI makes an entry point of its own. Public settings, so no session.
    states = {name: properties.get(name) for name in LLM_SETTINGS}
    assert states == dict.fromkeys(LLM_SETTINGS, False), states


def test_no_llm_provider_is_saved(metabase_url, session):
    # Metabase 0.64 keeps the keys of its LLM providers in a list of connections, which only this
    # admin endpoint shows: the public settings above tell of the Anthropic key and of the provider
    # of Metabot, and of no other. The answer masks the keys; the test prints the provider types.
    status, providers = http_json("GET", f"{metabase_url}/api/llm/providers", headers=session)
    assert status == 200, f"GET /api/llm/providers answered HTTP {status}"
    types = [p.get("type") for p in providers] if isinstance(providers, list) else providers
    assert types == [], f"LLM provider connections are saved: {types}"


@pytest.mark.parametrize("write", PROVIDER_WRITES)
def test_the_llm_providers_cannot_be_changed_through_the_api(metabase_url, session, write):
    # compose.yaml sets MB_LLM_PROVIDERS to an empty list, and an admin cannot add a provider to it,
    # in Admin > AI or through the API. The answer holds a message, which is not a secret.
    method, path, body = PROVIDER_WRITES[write]
    status, answer = http_json(method, f"{metabase_url}{path}", body, headers=session)
    message = answer.get("message") if isinstance(answer, dict) else answer
    assert status == 400, f"{method} {path} answered HTTP {status}"
    assert "MB_LLM_PROVIDERS" in str(message), f"{method} {path} was refused: {message}"


def test_the_settings_that_compose_fixes_come_from_the_environment(metabase_url, session):
    # A value from the environment wins over a value saved in Metabase, including one changed in the
    # Metabase UI, which does not lock these switches (0.63.19 and 0.64.1). This sees each variable
    # apart from the others: with ai-features-enabled? off, the other two AI settings read false
    # without theirs.
    expected = {**AI_SETTINGS, **REPORTING_SETTINGS}
    status, settings = http_json("GET", f"{metabase_url}/api/setting", headers=session)
    assert status == 200, f"GET /api/setting answered HTTP {status}"
    found = {
        s["key"]: (s["is_env_setting"], s["env_name"]) for s in settings if s["key"] in expected
    }
    assert found == {name: (True, variable) for name, variable in expected.items()}, found


@pytest.mark.parametrize("path", MCP_PATHS)
def test_the_mcp_endpoint_answers_403_to_everyone(metabase_url, session, path):
    # With the server on, an anonymous request gets 401 and a WWW-Authenticate challenge that starts
    # the OAuth flow, and a request with the session of the admin is served. 403 comes before both.
    url = f"{metabase_url}{path}"
    answers = {
        "anonymous": http_json("POST", url, INITIALIZE)[0],
        "the admin": http_json("POST", url, INITIALIZE, headers=session)[0],
    }
    assert answers == {"anonymous": 403, "the admin": 403}, f"POST {path}: {answers}"


def test_no_oauth_client_can_register(metabase_url):
    # The read-only check comes first, and stops the test when the registration is open: the
    # discovery document that MCP clients read offers the registration then.
    _, metadata = http_json("GET", f"{metabase_url}/.well-known/oauth-authorization-server")
    offered = isinstance(metadata, dict) and "registration_endpoint" in metadata
    assert not offered, "the discovery document offers the registration of clients"
    # Only then the POST, with a body that is not a JSON object. An endpoint that is open refuses
    # it as invalid and makes no client, so even a mistake here leaves nothing in the instance. A
    # closed one answers 403 before it looks at the body.
    status, _ = http_json("POST", f"{metabase_url}/oauth/register", [])
    assert status == 403, f"POST /oauth/register answered HTTP {status}"


def test_the_agent_api_is_refused(metabase_url, session):
    # The ping needs no permission of its own, so the switch is all that can refuse it, and it
    # refuses before it asks who is calling.
    url = f"{metabase_url}/api/agent/v1/ping"
    answers = {
        "anonymous": http_json("GET", url)[0],
        "the admin": http_json("GET", url, headers=session)[0],
    }
    assert answers == {"anonymous": 403, "the admin": 403}, f"GET /api/agent/v1/ping: {answers}"


def test_anonymous_tracking_is_off(properties):
    # GET /api/setting/<key> answers 204, with no value, for a setting that comes from the
    # environment, so the public properties are where its value shows.
    assert properties.get("anon-tracking-enabled") is False


def test_the_warehouse_connection_is_unique_and_read_only(metabase_url, session):
    everything = databases(metabase_url, session)
    matches = [d for d in everything if d["name"] == WAREHOUSE_NAME]
    assert len(matches) == 1, [d["name"] for d in everything]
    [warehouse] = matches
    details = warehouse["details"]
    assert warehouse["engine"] == "postgres"
    assert details["user"] == "metabase_reader"
    assert details.get("schema-filters-patterns") == "marts"


def test_metabase_reads_the_warehouse_as_the_read_only_role(metabase_url, session):
    # Metabase makes the connection from its own container, over the data network, so this also
    # shows that it can reach the warehouse.
    [warehouse] = [d for d in databases(metabase_url, session) if d["name"] == WAREHOUSE_NAME]
    url = f"{metabase_url}/api/database/{warehouse['id']}/healthcheck"
    assert http_json("GET", url, headers=session) == (200, {"status": "ok"})
    sql = "SELECT current_user, current_setting('default_transaction_read_only')"
    query = {"database": warehouse["id"], "type": "native", "native": {"query": sql}}
    status, result = http_json("POST", f"{metabase_url}/api/dataset", query, headers=session)
    state = result.get("status") if isinstance(result, dict) else None
    assert (status, state) in {(200, "completed"), (202, "completed")}, (status, state)
    assert result["data"]["rows"] == [["metabase_reader", "on"]]


def test_metabase_init_can_run_again(metabase_url, session):
    # The second run signs in instead of creating the admin, and it updates the connection instead
    # of making another one. The assertions hold flags: what the job prints is not echoed here.
    result = compose("run", "--rm", "--no-deps", "-T", "metabase-init", check=False)
    assert result.returncode == 0, f"metabase-init exited with {result.returncode}"
    found_the_admin = "the admin account exists" in result.stdout
    assert found_the_admin, "metabase-init did not find the admin that exists"
    updated_the_connection = "is up to date" in result.stdout
    assert updated_the_connection, "metabase-init did not update the warehouse connection"
    names = [d["name"] for d in databases(metabase_url, session)]
    assert names.count(WAREHOUSE_NAME) == 1, names


def stored_details(env, in_clear):
    """Read what the application database of Metabase keeps of the warehouse connection: the exit
    code of psql, whether it keeps anything, and the labels of the values of in_clear that it keeps
    in clear. The details stay in this frame, which has ended before the test checks anything."""
    result = psql(
        "platform-db",
        "metabase",
        env["METABASE_DB_PASSWORD"],
        "metabase",
        "SELECT details FROM metabase_database WHERE name = 'Agoranomos warehouse'",
    )
    return result.returncode, bool(result.stdout.strip()), leaked(in_clear, result.stdout)


def test_the_connection_details_are_encrypted_in_the_application_database(env):
    # METABASE_ENCRYPTION_KEY reaches Metabase as MB_ENCRYPTION_SECRET_KEY. Without that variable
    # (a misspelled name does as much) Metabase saves what it knows of a connection in clear.
    in_clear = {
        "the user": "metabase_reader",
        "the host": "warehouse-db",
        "the password": env["METABASE_READER_PASSWORD"],
    }
    code, stored, found = stored_details(env, in_clear)
    assert code == 0, f"psql exited with {code}"
    assert stored, "the application database holds no details of the warehouse"
    assert not found, f"the details of the connection are saved in clear: {found}"


@pytest.mark.parametrize(
    "service",
    [
        "metabase",
        "metadata-export",
        "metabase-init",
        pytest.param(
            "demo-init",
            marks=pytest.mark.skipif(
                not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
            ),
        ),
    ],
)
def test_no_secret_is_in_the_logs(env, service):
    # The log of a container is kept, and shown by ./agoranomos logs. The names of the settings are
    # printed, never the values. The log is read where it is used, so that no frame of a failed
    # test holds it.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    has_log = bool(compose("logs", "--no-color", service).stdout.strip())
    assert has_log, f"{service} has no log, so there is nothing to look in"
    found = leaked(values, compose("logs", "--no-color", service).stdout)
    assert not found, f"the log of {service} holds the values of {found}"
