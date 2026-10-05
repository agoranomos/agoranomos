"""Tests of the helpers themselves: they need neither Docker nor a running stack."""

import http.client
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import ai_switches
import metabase_init
import pytest
import services
import stack_helpers

PASSWORD = "a-password-that-must-stay-out-of-the-arguments"


def test_the_psql_helpers_keep_the_password_out_of_the_arguments(monkeypatch):
    # The arguments of a process can be read by the other users of the host (ps, /proc). The
    # environment of the docker process is not, and -e PGPASSWORD, with no value, hands the variable
    # on to the container.
    calls = []

    def run(args, **options):
        calls.append((args, options))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(stack_helpers.subprocess, "run", run)
    stack_helpers.psql("warehouse-db", "metabase_reader", PASSWORD, "warehouse", "SELECT 1")
    stack_helpers.psql_over_network(
        "warehouse-db", "metabase_reader", PASSWORD, "warehouse", "SELECT 1"
    )
    assert len(calls) == 2
    for args, options in calls:
        assert not any(PASSWORD in argument for argument in args), args
        assert args[args.index("-e") + 1] == "PGPASSWORD"
        assert options["env"]["PGPASSWORD"] == PASSWORD


def test_secret_headers_keep_their_values_out_of_what_pytest_prints():
    # pytest prints the arguments of a failed test, and headers can hold a session.
    value = "a-session-that-must-stay-out-of-the-report"
    headers = stack_helpers.SecretHeaders({"X-Metabase-Session": value})
    assert "X-Metabase-Session" in repr(headers)
    assert value not in repr(headers) + str(headers)
    # They are still headers: http_json copies them into the request.
    assert {"Content-Type": "application/json", **headers}["X-Metabase-Session"] == value


def test_a_secret_is_a_str_that_does_not_show_its_value():
    value = "a-token-that-must-stay-out-of-the-report"
    secret = stack_helpers.Secret(value)
    assert value not in repr(secret)
    assert "Secret" in repr(secret)
    # Everywhere a request needs the value, it is the value: the headers, the body and the text.
    assert secret == value
    assert str(secret) == value
    assert f"Bearer {secret}" == f"Bearer {value}"
    assert json.dumps({"password": secret}) == json.dumps({"password": value})
    assert value in f"text that holds {secret} in the middle"
    request = urllib.request.Request("http://127.0.0.1:9", headers={"X-Secret": secret})
    assert request.get_header("X-secret") == value
    # Inside a container, it still shows nothing: pytest prints the arguments of a failed test.
    assert value not in repr({"password": secret, "again": [secret]})


def test_env_values_print_their_names_and_never_their_values():
    value = "a-password-that-must-stay-out-of-the-report"
    env = stack_helpers.EnvValues({"DB_PASSWORD": value, "PORT": "8080"})
    assert "DB_PASSWORD" in repr(env)
    assert "PORT" in repr(env)
    assert value not in repr(env) + str(env)
    assert "8080" not in repr(env)
    # A dict like any other for the tests that read it.
    assert env["DB_PASSWORD"] == value
    assert env.get("PORT") == "8080"
    assert dict(env) == {"DB_PASSWORD": value, "PORT": "8080"}


def test_load_env_makes_secrets_of_the_settings_that_the_example_leaves_empty(
    tmp_path, monkeypatch
):
    (tmp_path / ".env.example").write_text("PORT=8080\nDB_PASSWORD=\nKEY=\n", encoding="utf-8")
    (tmp_path / ".env").write_text("PORT=9090\nDB_PASSWORD=one\nKEY=\n", encoding="utf-8")
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    env = stack_helpers.load_env(tmp_path / ".env")
    assert isinstance(env, stack_helpers.EnvValues)
    assert dict(env) == {"PORT": "9090", "DB_PASSWORD": "one", "KEY": ""}
    # A secret that has a value is a Secret, wherever it goes after that. What is not a secret, and
    # a secret that is empty, stays a plain str: a port in a message helps, and hides nothing.
    assert isinstance(env["DB_PASSWORD"], stack_helpers.Secret)
    assert type(env["PORT"]) is str
    assert type(env["KEY"]) is str
    # The helper that finds the secrets of a stack still finds them, as Secret values.
    assert stack_helpers.secrets(env) == {"DB_PASSWORD": "one"}


def test_the_airflow_helper_keeps_the_token_out_of_the_headers_that_pytest_prints(monkeypatch):
    # api() builds the headers of every Airflow request, and its own arguments are printed when
    # a request fails inside it.
    import test_airflow

    value = "a-token-that-must-stay-out-of-the-report"
    seen = {}

    def fake_http_json(method, url, body=None, headers=None, timeout=30):
        seen["headers"] = headers
        return 200, {}

    monkeypatch.setattr(test_airflow, "http_json", fake_http_json)
    test_airflow.api("http://127.0.0.1:8080", stack_helpers.Secret(value), "GET", "/api/v2/dags")
    headers = seen["headers"]
    assert value not in repr(headers) + repr(list(headers.values()))
    assert headers["Authorization"] == f"Bearer {value}"  # the request still carries it


def fake_proc(tmp_path, entries):
    """A folder that looks like /proc: one folder per process ID, with its name and its status."""
    for pid, (name, uid) in entries.items():
        folder = tmp_path / "proc" / str(pid)
        folder.mkdir(parents=True)
        (folder / "comm").write_text(f"{name}\n", encoding="utf-8")
        (folder / "status").write_text(f"Name:\t{name}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n")
    return str(tmp_path / "proc")


def list_uids(name, proc):
    return subprocess.run(
        ["sh", "-c", stack_helpers.process_uids(name, proc)],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_the_process_listing_succeeds_whatever_process_comes_last(tmp_path):
    # The glob sorts the IDs as strings: 1, 133, 8, 91. The shell that docker compose exec starts
    # is the last of them here, it is not the server, and the loop used to end in its status, 1.
    # How early in the life of a container this happens depends on the process IDs.
    proc = fake_proc(
        tmp_path,
        {1: ("tini", 999), 133: ("sh", 999), 8: ("valkey-server", 999), 91: ("sh", 999)},
    )
    result = list_uids("valkey-server", proc)
    assert result.returncode == 0
    assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"]


def test_the_process_listing_prints_nothing_for_a_process_that_is_not_there(tmp_path):
    # It succeeds, and the test that uses it compares the output, so a missing server fails there,
    # and says what it saw.
    proc = fake_proc(tmp_path, {1: ("tini", 999), 8: ("sh", 999)})
    result = list_uids("valkey-server", proc)
    assert result.returncode == 0
    assert result.stdout == ""


def test_the_process_listing_runs_in_the_shell_of_the_image(tmp_path):
    # The images ship dash or busybox sh: the command must not need more than POSIX sh.
    proc = fake_proc(tmp_path, {1: ("tini", 0), 8: ("valkey-server", 999), 9: ("sh", 0)})
    for shell in ("/bin/dash", "/bin/sh"):
        if not Path(shell).exists():
            continue
        result = subprocess.run(
            [shell, "-c", stack_helpers.process_uids("valkey-server", proc)],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert result.returncode == 0, shell
        assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"], shell


def test_a_failed_job_is_reported_by_its_exit_code_and_never_by_its_output():
    # A failed ALTER ROLE shows its statement, password included, in the output of db-init.
    result = subprocess.CompletedProcess(
        ["docker"], 3, stdout="", stderr="ERROR: ALTER ROLE airflow WITH LOGIN PASSWORD 'hunter2'"
    )
    message = stack_helpers.job_failed("db-init", result)
    assert "hunter2" not in message
    assert "db-init" in message
    assert "3" in message


@pytest.mark.parametrize(
    ("name", "is_switch"),
    [
        ("ai-features-enabled?", True),
        ("mcp-enabled?", True),
        ("agent-api-enabled?", True),
        ("embedded-metabot-enabled?", True),
        ("metabot-enabled?", True),
        ("mcp-execute-sql-enabled", True),
        ("llm-something-new-enabled?", True),
        ("ai-sql-fixer-enabled", True),
        # "ai" is a word part, not a few letters of a longer word.
        ("airgap-enabled", False),
        ("email-configured?", False),
        ("maintenance-enabled?", False),
        # It has to end in enabled.
        ("enable-ai-controls?", False),
        ("llm-metabot-configured?", False),
        ("show-metabot", False),
        ("ai-features-enabled-at", False),
        # It has to have an AI word.
        ("embedding-enabled?", False),
        ("enabled", False),
    ],
)
def test_the_sweep_knows_an_ai_switch_by_its_name(name, is_switch):
    assert ai_switches.is_ai_switch(name) is is_switch


def admin_entry(name, value=None, default=None, env=False):
    """An entry of GET /api/setting: an environment variable hides the value and the default."""
    return {
        "key": name,
        "value": None if env else value,
        "default": "Using value of env var" if env else default,
        "is_env_setting": env,
    }


def test_the_sweep_reads_the_public_value_and_the_admin_list_for_what_is_not_public():
    properties = {"ai-features-enabled?": False, "mcp-enabled?": True, "show-metabot": True}
    admin = [
        admin_entry("ai-features-enabled?", env=True),  # public: the public value counts
        admin_entry("ai-sql-fixer-enabled?", default=True),  # not saved, on by default
        admin_entry("ai-chat-enabled?", value=False, default=True),
        admin_entry("agent-thing-enabled?", env=True),  # fixed by the environment
        admin_entry("llm-unset-enabled?"),  # no value and no default: unset
        admin_entry("site-name", value="x"),  # not an AI switch
    ]
    readings = ai_switches.readings(properties, admin)
    assert readings == {
        "ai-features-enabled?": False,
        "mcp-enabled?": True,
        "ai-sql-fixer-enabled?": True,
        "ai-chat-enabled?": False,
        "agent-thing-enabled?": ai_switches.LOCKED,
        "llm-unset-enabled?": None,
    }


def test_the_sweep_finds_a_switch_that_is_on_and_that_nothing_allows():
    readings = {
        "ai-features-enabled?": False,
        "mcp-enabled?": True,
        "ai-sql-fixer-enabled?": True,
        "agent-thing-enabled?": ai_switches.LOCKED,
        "llm-unset-enabled?": None,
        "mcp-execute-sql-enabled": True,
    }
    on = ai_switches.switches_on(readings, allowed={"mcp-execute-sql-enabled": "a reason"})
    assert on == ["ai-sql-fixer-enabled?", "mcp-enabled?"]
    # Without the allowance the same reading is red, so the allowance is what turns it green.
    assert "mcp-execute-sql-enabled" in ai_switches.switches_on(readings, allowed={})


def test_every_allowed_switch_has_a_reason_and_is_a_switch():
    assert ai_switches.ALLOWED_ON, "the list starts with the SQL tool of the MCP server"
    for name, reason in ai_switches.ALLOWED_ON.items():
        assert ai_switches.is_ai_switch(name), name
        assert len(reason.split()) >= 4, f"{name} needs a reason in plain words"


# What a pytest that a test starts reads from the environment of the person who runs the tests: the
# colors and the width of its report, more options and plugins. And the proxy of urllib, which
# would send what the nested tests request somewhere else. A test of a report must not depend on
# any of them, so the nested run does not get them.
NESTED_PYTEST_IGNORES = frozenset(
    {
        "COLUMNS",
        "LINES",
        "FORCE_COLOR",
        "NO_COLOR",
        "PY_COLORS",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "PYTEST_THEME",
        "PYTEST_THEME_MODE",
    }
)


def nested_pytest(directory: Path, *options: str, **variables: str) -> str:
    """Run pytest in a process of its own, in directory, and return its report.

    The report is the same on every machine: the process gets the environment of this one without
    the variables above and without any *_PROXY, it gets --color=no, and it gets exactly the options
    that the test names. It has no -q: with -q, -v would be quieter than it says, and CI runs -v.
    The variables of the test go into its environment, and its modules can import stack_helpers.
    """
    environment = {
        name: value
        for name, value in os.environ.items()
        if name.upper() not in NESTED_PYTEST_IGNORES and not name.upper().endswith("_PROXY")
    }
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--color=no", *options],
        cwd=directory,
        env={
            **environment,
            **variables,
            "PYTHONPATH": str(Path(stack_helpers.__file__).parent),
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return result.stdout + result.stderr


def shown_in(value: str, report: str) -> bool:
    """Whether a report shows the value, also when pytest has wrapped it over several lines.

    With -vv, --showlocals prints a local with pprint, which cuts a long bytes value into literals
    that follow each other: (b'..."password": "a-password-that-must-stay-out-of-the-repo' on one
    line and b'rt-4"}') on the next. A plain `value in report` misses that, so the literals are
    joined first.
    """
    joined = re.sub(r"""(['"])\s*\n\s*b?\1""", "", report)
    return value in report or value in joined


@pytest.mark.parametrize(
    ("report", "shown"),
    [
        pytest.param("a log line with a-secret-value-12345 in it", True, id="plain"),
        pytest.param(
            "data = (b'{\"p\": \"a-secret-va'\n        b'lue-12345\"}')", True, id="bytes"
        ),
        pytest.param("data = ('a-secret-va'\n        'lue-12345')", True, id="text"),
        pytest.param('data = ("a-secret-va"\n        "lue-12345")', True, id="double-quotes"),
        pytest.param("nothing of it here", False, id="absent"),
        pytest.param("a-secret-va\nlue-12345", False, id="a-line-break-that-is-no-join"),
        pytest.param("(b'a-secret-va'\n b'other-12345')", False, id="another-value"),
    ],
)
def test_shown_in_finds_a_value_that_pytest_wrapped_over_lines(report, shown):
    assert shown_in("a-secret-value-12345", report) is shown


# A test module that fails on purpose, run by a pytest of its own, so that the output is the real
# one. The values are made up. The env fixture and the comparison hook are those of the real
# conftest.py, which the test copies next to this module; the token and the headers are what
# test_airflow.py builds. The control takes a plain str, and the report must show it: a check that
# cannot see a leak would pass too.
FAILING_TESTS = """
from pathlib import Path

import pytest
import stack_helpers

stack_helpers.REPO_ROOT = Path(__ROOT__)


@pytest.fixture
def token():
    return stack_helpers.Secret("__TOKEN__")


@pytest.fixture
def headers(token):
    return stack_helpers.SecretHeaders({"Authorization": stack_helpers.Secret(f"Bearer {token}")})


@pytest.fixture
def plain():
    return "__PLAIN__"


def helper(password, body, headers):
    raise RuntimeError("the helper failed")


def test_a_helper_fails(env, token, headers):
    body = {"username": "admin", "password": env["API_PASSWORD"], "token": token}
    helper(env["API_PASSWORD"], body, headers)


def test_comparisons_fail(env, token, headers):
    assert env["API_PASSWORD"] == "something else", "deliberate failure"


def test_a_comparison_of_containers_fails(env, token, headers):
    assert {"password": env["API_PASSWORD"], "token": token} == {"password": "x", "token": "y"}


def test_a_comparison_of_lists_fails(env, token, headers):
    assert [env["API_PASSWORD"], token] == ["x", "y"]


def test_a_text_that_holds_a_secret_fails(env, token, headers):
    assert env["API_PASSWORD"] not in f"some log text with {env['API_PASSWORD']} in the middle"


def test_the_control_fails(plain):
    assert plain == "something else", "deliberate failure"
"""


@pytest.mark.parametrize(
    "options",
    [
        pytest.param([], id="default"),
        pytest.param(["--tb=long"], id="long"),
        pytest.param(["-vv", "--showlocals"], id="verbose-with-locals"),
    ],
)
def test_pytest_does_not_print_the_secrets_of_a_failing_test(tmp_path, options):
    values = {
        "__TOKEN__": "a-token-that-must-stay-out-of-the-report-1",
        "__PLAIN__": "a-plain-value-that-the-report-does-show-2",
    }
    password = "a-password-that-must-stay-out-of-the-report-3"
    (tmp_path / ".env.example").write_text("PORT=8080\nAPI_PASSWORD=\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"PORT=9090\nAPI_PASSWORD={password}\n", encoding="utf-8")
    source = FAILING_TESTS.replace("__ROOT__", repr(str(tmp_path)))
    for placeholder, value in values.items():
        source = source.replace(placeholder, value)
    (tmp_path / "test_fails_on_purpose.py").write_text(source, encoding="utf-8")
    shutil.copy(Path(stack_helpers.__file__).parent / "conftest.py", tmp_path / "conftest.py")
    report = nested_pytest(tmp_path, *options)
    assert "6 failed" in report, "the tests did not fail as they should"
    # The arguments are printed, and show no value.
    assert "Secret('<hidden>')" in report
    assert "EnvValues(" in report
    hidden = {"the token": values["__TOKEN__"], "the password": password}
    assert [label for label, value in hidden.items() if shown_in(value, report)] == []
    assert not shown_in(f"Bearer {values['__TOKEN__']}", report)
    # The control shows that this check can see a leak.
    assert shown_in(values["__PLAIN__"], report)


def unused_port() -> int:
    """A port on loopback where nothing listens: a socket binds it and lets it go."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def http_server():
    """A local server for http_json: each path answers as its entry in ANSWERS says, and the
    requests that it got are kept in server.requests as (method, path, headers, body)."""
    answers = {
        "/ok": (200, "application/json", b'{"answer": [1, 2]}'),
        "/empty": (204, "application/json", b""),
        "/text": (200, "text/plain", b"not json"),
        "/denied": (401, "application/json", b'{"detail": "Not authenticated"}'),
        "/missing": (404, "text/plain", b"nothing here"),
    }

    class Handler(BaseHTTPRequestHandler):
        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            server.requests.append((self.command, self.path, dict(self.headers), body))
            path = self.path.split("?")[0]
            if path == "/hangs-up":
                self.close_connection = True
                return
            if path == "/sleeps":
                self.close_connection = True
                time.sleep(0.6)  # longer than the timeout of the test, and it writes nothing after
                return
            if path == "/cut-off":
                self.close_connection = True
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"part": ')
                return
            status, content_type, data = answers[path]
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = serve

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.requests = []
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", server
    server.shutdown()
    server.server_close()


def test_http_json_sends_the_body_and_the_headers_and_reads_the_json(http_server):
    url, server = http_server
    secret = stack_helpers.Secret("a-session-that-must-stay-out-of-the-report")
    headers = stack_helpers.SecretHeaders({"X-Session": secret})
    body = {"username": "admin", "password": stack_helpers.Secret("a-password-for-the-wire")}
    assert stack_helpers.http_json("POST", f"{url}/ok", body, headers) == (200, {"answer": [1, 2]})
    [(method, path, sent, data)] = server.requests
    assert (method, path) == ("POST", "/ok")
    assert json.loads(data) == {"username": "admin", "password": "a-password-for-the-wire"}
    assert sent["Content-Type"] == "application/json"
    assert sent["X-Session"] == "a-session-that-must-stay-out-of-the-report"


def test_http_json_sends_no_body_when_it_has_none(http_server):
    url, server = http_server
    assert stack_helpers.http_json("GET", f"{url}/ok") == (200, {"answer": [1, 2]})
    [(method, _, sent, data)] = server.requests
    assert method == "GET"
    assert data is None
    assert "Content-Length" not in sent


@pytest.mark.parametrize(
    ("path", "answer"),
    [
        pytest.param("/empty", (204, None), id="no-body"),
        pytest.param("/text", (200, "not json"), id="not-json"),
        pytest.param("/denied", (401, {"detail": "Not authenticated"}), id="a-status-of-401"),
        pytest.param("/missing", (404, "nothing here"), id="a-status-of-404-that-is-text"),
    ],
)
def test_http_json_returns_every_answer_of_the_server_as_a_status_and_a_body(
    http_server, path, answer
):
    # An error status is an answer to look at, and not an exception: tests assert on it.
    url, _ = http_server
    assert stack_helpers.http_json("GET", f"{url}{path}") == answer


@pytest.mark.parametrize(
    ("path", "failure"),
    [
        pytest.param("/hangs-up", "RemoteDisconnected", id="hangs-up"),
        pytest.param("/cut-off", "IncompleteRead", id="cut-off"),
    ],
)
def test_http_json_names_the_call_and_the_class_of_a_failure_and_nothing_else(
    http_server, path, failure
):
    url, _ = http_server
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json(
            "PUT", f"{url}{path}?token=in-the-query", {"password": "in-the-body"}
        )
    assert str(raised.value) == f"PUT {path} failed: {failure}"
    # The original error is not kept to be printed: its frames hold the request.
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True


def test_http_json_sends_no_request_through_a_proxy(http_server, monkeypatch):
    # A developer machine can export HTTP_PROXY, and urlopen would send every request there, with
    # its token or password, loopback too unless NO_PROXY says no. The stack tests talk to the
    # services that the stack publishes on 127.0.0.1 and to nothing else.
    url, server = http_server
    dead = f"http://127.0.0.1:{unused_port()}"
    for name in ("HTTP_PROXY", "http_proxy"):
        monkeypatch.setenv(name, dead)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    # urlopen builds its opener at its first use in the process and keeps it. A test that ran
    # before this one may have built it without the variables above, and then a request through
    # urlopen would reach the server and hide the problem: so the opener is unset here, and the
    # order of the tests cannot matter.
    monkeypatch.setattr(urllib.request, "_opener", None, raising=False)
    # The control: a plain opener, built now, reads the variables and goes to the proxy, which is
    # dead, so this check can fail.
    with pytest.raises(urllib.error.URLError):
        urllib.request.build_opener().open(f"{url}/ok", timeout=2)
    assert stack_helpers.http_json("GET", f"{url}/ok") == (200, {"answer": [1, 2]})
    assert [(method, path) for method, path, _, _ in server.requests] == [("GET", "/ok")]


def test_http_json_tells_a_timeout_from_a_refused_connection(http_server):
    # In a CI log, "URLError" alone would be the same for a refused connection, a name that does not
    # resolve and a server that does not answer in time.
    url, _ = http_server
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json("GET", f"{url}/sleeps", timeout=0.2)
    assert str(raised.value) == "GET /sleeps failed: TimeoutError"


@pytest.mark.parametrize(
    ("error", "name"),
    [
        pytest.param(
            urllib.error.URLError(ConnectionRefusedError(61, "Connection refused")),
            "URLError (ConnectionRefusedError)",
            id="a-refused-connection",
        ),
        pytest.param(
            urllib.error.URLError(socket.gaierror(8, "nodename nor servname provided")),
            "URLError (gaierror)",
            id="a-name-that-does-not-resolve",
        ),
        pytest.param(
            urllib.error.URLError("unknown url type: x"), "URLError", id="a-reason-that-is-text"
        ),
        pytest.param(http.client.RemoteDisconnected("closed"), "RemoteDisconnected", id="hang-up"),
        pytest.param(http.client.IncompleteRead(b"part", 10), "IncompleteRead", id="cut-off"),
        pytest.param(TimeoutError("timed out"), "TimeoutError", id="timeout"),
        pytest.param(OSError(5, "Input/output error"), "OSError", id="plain-os-error"),
    ],
)
def test_the_stack_helpers_name_an_error_by_its_classes_as_metabase_init_does(error, name):
    # The name is the class of the error and, when urllib wrapped one, the class of its reason:
    # never the text, which can hold an address or a header.
    assert stack_helpers.error_classes(error) == name
    assert metabase_init.error_classes(error) == name


def test_http_json_names_the_path_of_a_url_that_has_none():
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json("GET", f"http://127.0.0.1:{unused_port()}")
    assert str(raised.value) == "GET / failed: URLError (ConnectionRefusedError)"


# A request with a password in its body that cannot be made, in three ways: to a port where nothing
# listens, so that the transport fails; to a URL that urllib takes for another kind of URL, so that
# it fails when it opens it (in 127.0.0.1:1/api/session it reads 127.0.0.1 as the scheme); and to a
# URL that it cannot parse at all, so that Request() itself raises. The values reach the test
# through the environment, not as literals: pytest prints the source of the failing test, and a
# literal would be in the report as text. Each case has a control, which makes the same request
# with plain urllib in a helper that keeps the body in a local and in an argument: pytest prints
# those under --tb=long and --showlocals, and the check must see it there, or it could not see a
# leak at all.
FAILING_REQUESTS = """
import json
import os
import urllib.request

import stack_helpers

PORT = os.environ["TEST_PORT"]
REFUSED = "http://127.0.0.1:" + PORT + "/api/session?marker=visible-in-the-query"
HOST_AS_A_SCHEME = "127.0.0.1:" + PORT + "/api/session"
CANNOT_BE_PARSED = "localhost/api/session"


def leaky_helper(url, password):
    data = json.dumps({"password": password}).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    urllib.request.urlopen(request, timeout=5)


def credentials():
    secret = stack_helpers.Secret
    token = secret("Bearer " + os.environ["TEST_TOKEN"])
    body = {"username": "admin", "password": secret(os.environ["TEST_PASSWORD"])}
    return stack_helpers.SecretHeaders({"Authorization": token}), body


def test_http_json_cannot_connect():
    headers, body = credentials()
    stack_helpers.http_json("POST", REFUSED, body, headers)


def test_the_control_cannot_connect():
    leaky_helper(REFUSED, os.environ["TEST_CONTROL"])


def test_http_json_gets_a_host_as_a_scheme():
    headers, body = credentials()
    stack_helpers.http_json("POST", HOST_AS_A_SCHEME, body, headers)


def test_the_control_gets_a_host_as_a_scheme():
    leaky_helper(HOST_AS_A_SCHEME, os.environ["TEST_CONTROL"])


def test_http_json_gets_a_url_that_cannot_be_parsed():
    headers, body = credentials()
    stack_helpers.http_json("POST", CANNOT_BE_PARSED, body, headers)


def test_the_control_gets_a_url_that_cannot_be_parsed():
    leaky_helper(CANNOT_BE_PARSED, os.environ["TEST_CONTROL"])
"""


def error_lines(report: str, test: str) -> list[str]:
    """The lines of the traceback of one failed test that start with E, without the E."""
    parts = re.split(r"^_+ (test_\w+) _+$", report, flags=re.MULTILINE)
    section = dict(zip(parts[1::2], parts[2::2], strict=True))[test]
    return [
        line.removeprefix("E").strip() for line in section.splitlines() if line.startswith("E ")
    ]


@pytest.mark.parametrize(
    ("options", "control_leaks"),
    [
        pytest.param([], False, id="default"),
        pytest.param(["-v"], False, id="verbose"),
        pytest.param(["-vv"], False, id="very-verbose"),
        pytest.param(["--tb=long"], True, id="long"),
        pytest.param(["-vv", "--showlocals"], True, id="verbose-with-locals"),
        pytest.param(["--full-trace", "-l"], True, id="full-trace-with-locals"),
    ],
)
@pytest.mark.parametrize("case", ["cannot_connect", "host_as_a_scheme", "cannot_be_parsed"])
def test_a_failed_request_prints_no_part_of_its_body_under_any_traceback_mode(
    tmp_path, options, control_leaks, case
):
    # CI runs with -v, and the other modes are what a person may ask for when a test fails.
    values = {
        "TEST_PASSWORD": "a-password-that-must-stay-out-of-the-report-4",
        "TEST_TOKEN": "a-token-that-must-stay-out-of-the-report-5",
        "TEST_CONTROL": "a-control-value-that-the-report-does-show-6",
    }
    port = unused_port()
    (tmp_path / "test_fails_to_connect.py").write_text(FAILING_REQUESTS, encoding="utf-8")
    report = nested_pytest(tmp_path, *options, "-k", case, **values, TEST_PORT=str(port))
    assert "2 failed" in report, "the tests did not fail as they should"
    hidden = {"the password": values["TEST_PASSWORD"], "the token": values["TEST_TOKEN"]}
    assert [label for label, value in hidden.items() if shown_in(value, report)] == []
    # The error of http_json, as the traceback of the test shows it. A failure of the transport, and
    # a URL that urllib fails to open, give a ConnectionError that names the method and the path
    # and nothing of the query. A URL that Request() cannot parse gives the ValueError of urllib,
    # which is raised before the body is encoded. The lines that start with E are read, as the
    # summary line is cut to the width of the terminal.
    subject, expected = {
        "cannot_connect": (
            "test_http_json_cannot_connect",
            "ConnectionError: POST /api/session failed: URLError (ConnectionRefusedError)",
        ),
        "host_as_a_scheme": (
            "test_http_json_gets_a_host_as_a_scheme",
            f"ConnectionError: POST 127.0.0.1:{port}/api/session failed: URLError",
        ),
        "cannot_be_parsed": (
            "test_http_json_gets_a_url_that_cannot_be_parsed",
            "ValueError: unknown url type: 'localhost/api/session'",
        ),
    }[case]
    assert error_lines(report, subject) == [expected]
    # The control shows that this check can see a leak where there is one.
    assert shown_in(values["TEST_CONTROL"], report) is control_leaks


def write_env(tmp_path, monkeypatch, text):
    """Make stack_helpers read a .env with this text, in a checkout of its own."""
    (tmp_path / ".env").write_text(text, encoding="utf-8")
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)


@pytest.mark.parametrize(
    ("setting", "variant"),
    [
        ("COMPOSE_FILE=compose.yaml", "local"),
        ("COMPOSE_FILE=compose.yaml:compose.celery.yaml", "celery"),
        ('COMPOSE_FILE="compose.yaml:compose.celery.yaml"', "celery"),
        ("COMPOSE_FILE='compose.yaml:compose.celery.yaml'", "celery"),
        ("COMPOSE_FILE=compose.celery.yaml:compose.yaml", "celery"),
        ("COMPOSE_FILE=/srv/stack/compose.yaml:/srv/stack/compose.celery.yaml", "celery"),
        ("COMPOSE_FILE=compose.yaml:compose.celery.yaml.bak", "local"),
        ("COMPOSE_FILE=compose.yaml:not-compose.celery.yaml", "local"),
        ("COMPOSE_FILE=", "local"),
        ("COMPOSE_PROFILES=core", "local"),
    ],
)
def test_active_variant_follows_compose_file_in_env(tmp_path, monkeypatch, setting, variant):
    write_env(tmp_path, monkeypatch, f"COMPOSE_PROJECT_NAME=agoranomos\n{setting}\n")
    assert stack_helpers.active_variant() == variant


def test_active_variant_is_local_without_an_env_file(tmp_path, monkeypatch):
    # The env fixture reports a missing .env with a message of its own, so importing the modules
    # that call active_variant must not fail first.
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    assert stack_helpers.active_variant() == "local"


def test_active_variant_takes_the_last_setting_as_docker_compose_does(tmp_path, monkeypatch):
    write_env(
        tmp_path,
        monkeypatch,
        "COMPOSE_FILE=compose.yaml:compose.celery.yaml\nCOMPOSE_FILE=compose.yaml\n",
    )
    assert stack_helpers.active_variant() == "local"


def test_the_celery_variant_adds_the_broker_and_the_worker_to_the_long_running_services():
    # services.py takes the variant of the .env in the checkout when it is imported; this tests the
    # function that it calls, so the result does not depend on that file.
    local = services.long_running("local")
    celery = services.long_running("celery")
    assert {"broker", "airflow-worker"}.isdisjoint(local)
    assert celery == local | {"broker", "airflow-worker"}
    assert {
        "warehouse-db",
        "platform-db",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
        "metabase",
    } <= local
    assert {"db-init", "airflow-init", "metabase-init"} == services.ONE_SHOT
    assert services.LONG_RUNNING in (local, celery)


def test_secrets_are_the_settings_that_the_example_leaves_empty(tmp_path, monkeypatch):
    (tmp_path / ".env.example").write_text(
        "# a comment\nPORT=8080\nVALKEY_PASSWORD=\nAIRFLOW_FERNET_KEY=\nNOT_SET_YET=\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    env = {"PORT": "9090", "VALKEY_PASSWORD": "a-secret", "AIRFLOW_FERNET_KEY": "another", "X": "y"}
    # Only the settings that the example leaves empty, and only when .env gives them a value.
    assert stack_helpers.secrets(env) == {
        "VALKEY_PASSWORD": "a-secret",
        "AIRFLOW_FERNET_KEY": "another",
    }
