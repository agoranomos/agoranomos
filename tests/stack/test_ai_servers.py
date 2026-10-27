"""The MCP servers of the warehouse and of Airflow in the container ai, on a running stack
(ADR-0030).

The tests run when .env lists the ai profile, as a new installation in local mode does. Each starts
a server as an AI client does, with ./agoranomos mcp <server>, speaks MCP to it over stdio, and
checks the two limits of each server: its switch, and the account behind it, ai_reader in the
warehouse and the Airflow user ai with the role Viewer. They print tool names, statuses and errors,
never a value of .env.
"""

import json
import subprocess
from typing import Any

import pytest
from services import AI_SERVERS, DEMO
from stack_helpers import REPO_ROOT, SecretHeaders, compose, http_json, warehouse_output

pytestmark = pytest.mark.skipif(not AI_SERVERS, reason=".env does not list the ai profile")

# The tools of ai/tools.yaml.
WAREHOUSE_TOOLS = {"list_tables", "describe_table", "execute_sql"}
# The mart that the demo fills, which ai_reader reads.
MART = "marts.ad_stats_campaign_daily"


class Session:
    """One session of an MCP client with a server of the container ai, over stdio: newline-delimited
    JSON-RPC, as the stdio transport of MCP sends it."""

    def __init__(self, server: str) -> None:
        self.process = subprocess.Popen(
            [str(REPO_ROOT / "agoranomos"), "mcp", server],
            cwd=REPO_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.next_id = 0
        self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "agoranomos-stack-tests", "version": "0"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def send(self, message: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a request and return the result of its answer; the notifications and the requests
        of the server that come before it are skipped."""
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params})
        assert self.process.stdout is not None
        for line in self.process.stdout:
            message = json.loads(line)
            if message.get("id") == self.next_id and "method" not in message:
                assert "error" not in message, f"{method}: {message['error']}"
                return message["result"]
        self.process.wait(timeout=30)
        assert self.process.stderr is not None
        pytest.fail(f"the server ended before it answered {method}: {self.process.stderr.read()}")

    def tools(self) -> set[str]:
        return {tool["name"] for tool in self.request("tools/list", {})["tools"]}

    def call(self, tool: str, arguments: dict[str, Any]) -> tuple[bool, str]:
        """Call a tool: whether the server reports an error, and the text of its answer, one line
        for each item of its content."""
        result = self.request("tools/call", {"name": tool, "arguments": arguments})
        text = "\n".join(c.get("text", "") for c in result.get("content", []))
        return bool(result.get("isError")), text

    def rows(self, tool: str, arguments: dict[str, Any]) -> list[dict[str, Any]]:
        """The rows that a tool of Toolbox returns: Toolbox 1.15.0 sends each row as an item of
        the content of its answer, a JSON object."""
        result = self.request("tools/call", {"name": tool, "arguments": arguments})
        texts = [c.get("text", "") for c in result.get("content", [])]
        assert not result.get("isError"), "\n".join(texts)
        return [json.loads(text) for text in texts]

    def close(self) -> None:
        assert self.process.stdin is not None
        self.process.stdin.close()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


@pytest.fixture(scope="module")
def warehouse_server():
    session = Session("warehouse")
    yield session
    session.close()


@pytest.fixture(scope="module")
def airflow_server():
    session = Session("airflow")
    yield session
    session.close()


def test_the_warehouse_server_offers_the_tools_of_the_repository(warehouse_server):
    assert warehouse_server.tools() == WAREHOUSE_TOOLS


def test_the_warehouse_server_lists_marts_and_staging_and_nothing_else(warehouse_server):
    schemas = {row["schema_name"] for row in warehouse_server.rows("list_tables", {})}
    assert schemas == {"marts", "staging"}


@pytest.mark.skipif(not DEMO, reason="the demo profile is off: the marts are empty")
def test_the_warehouse_server_reads_a_mart_and_describes_it(warehouse_server, env):
    count = f"SELECT count(*) AS n FROM {MART}"  # noqa: S608 (a constant of this file)
    [row] = warehouse_server.rows("execute_sql", {"sql": count})
    assert row["n"] == int(warehouse_output(env, count))
    schema, table = MART.split(".")
    columns = warehouse_server.rows("describe_table", {"schema_name": schema, "table_name": table})
    assert columns
    assert all(column["description"] for column in columns)


@pytest.mark.parametrize(
    "sql",
    [
        # A write: the session is read-only, and the role has no grant to write.
        "CREATE TABLE marts.ai_was_here (n integer)",
        f"DELETE FROM {MART}",  # noqa: S608 (a constant of this file)
        # The schemas that the role does not read: a raw schema, and ops, which records the runs.
        "SELECT count(*) FROM raw_demo_search._dlt_loads",
        "SELECT count(*) FROM ops.dbt_run_results",
    ],
)
def test_the_warehouse_server_refuses_what_ai_reader_may_not_do(warehouse_server, sql):
    failed, text = warehouse_server.call("execute_sql", {"sql": sql})
    assert failed, f"{sql} passed"
    assert "read-only transaction" in text or "permission denied" in text, text


def test_ai_reader_keeps_the_limits_of_the_decision(env):
    found = warehouse_output(
        env,
        "SELECT rolcanlogin, rolconnlimit, array_to_string(rolconfig, ',')"
        " FROM pg_roles WHERE rolname = 'ai_reader'",
    )
    can_login, connections, config = found.split("|")
    assert (can_login, connections) == ("t", "3")
    assert set(config.split(",")) == {
        "default_transaction_read_only=on",
        "statement_timeout=1min",
        "idle_in_transaction_session_timeout=1min",
    }


def test_the_airflow_server_reads_the_dags(airflow_server):
    tools = airflow_server.tools()
    assert {"list_dags", "get_task_logs", "trigger_dag"} <= tools
    failed, text = airflow_server.call("list_dags", {})
    assert not failed, text
    assert "agoranomos_demo_transform" in text


@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("trigger_dag", {"dag_id": "agoranomos_demo_transform"}),
        ("pause_dag", {"dag_id": "agoranomos_demo_transform"}),
    ],
)
def test_the_airflow_server_refuses_every_tool_that_writes(airflow_server, tool, arguments):
    # astro-airflow-mcp 0.9.1 answers a refused call with an error object as its result, and not
    # with an error of MCP: the call never reaches Airflow, whose role Viewer would refuse it too.
    failed, text = airflow_server.call(tool, arguments)
    answer = json.loads(text)
    assert failed or answer.get("error_type") == "ReadOnlyError", f"{tool} passed: {text}"
    assert "read-only mode" in answer["error"], text


def test_the_airflow_user_ai_is_a_viewer(env, airflow_url):
    status, body = http_json(
        "POST",
        f"{airflow_url}/auth/token",
        {"username": "ai", "password": env["AIRFLOW_AI_PASSWORD"]},
    )
    assert status in (200, 201), status
    # pytest prints the headers of a failed call by their names only: see SecretHeaders.
    headers = SecretHeaders({"Authorization": f"Bearer {body['access_token']}"})
    assert http_json("GET", f"{airflow_url}/api/v2/dags?limit=1", headers=headers)[0] == 200
    for path in ("/api/v2/connections", "/api/v2/variables", "/api/v2/config"):
        assert http_json("GET", f"{airflow_url}{path}", headers=headers)[0] == 403, path


@pytest.mark.parametrize("server", ["warehouse", "airflow"])
def test_a_server_whose_switch_is_off_does_not_start(server):
    switch = {"warehouse": "WAREHOUSE_MCP", "airflow": "AIRFLOW_MCP"}[server]
    result = compose(
        "exec",
        "-T",
        "-e",
        f"{switch}=false",
        "ai",
        "/opt/agoranomos/ai/mcp.sh",
        server,
        check=False,
    )
    assert result.returncode == 1
    assert f"./agoranomos ai {server} on" in result.stderr
