"""Tests of metabase/metabase_init.py against a stand-in for the Metabase REST API."""

from __future__ import annotations

import ast
import copy
import json
import socket
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import metabase_init as mi
import pytest

ENV = {
    "METABASE_SITE_NAME": "Agoranomos",
    "METABASE_ADMIN_EMAIL": "admin@example.com",
    "METABASE_ADMIN_PASSWORD": "Secret-password-1",
    "WAREHOUSE_HOST": "warehouse-db",
    "WAREHOUSE_PORT": "5432",
    "WAREHOUSE_DB": "warehouse",
    "WAREHOUSE_USER": "metabase_reader",
    "WAREHOUSE_PASSWORD": "reader-password",
}

# What metabase-init must never print: the values that these tests pass in, and the tokens that the
# stand-in hands out. The assertions name them by label, as the stack tests do.
SECRETS = {
    "the admin password": ENV["METABASE_ADMIN_PASSWORD"],
    "the warehouse password": ENV["WAREHOUSE_PASSWORD"],
    "the setup token": "setup-token-1",
    "the session of the setup": "session-from-setup",
    "the session of a sign-in": "session-from-login",
}


def leaked(text: str, secrets: dict[str, str] = SECRETS) -> list[str]:
    """The labels of the secrets that are in the text, as they are or as JSON writes them inside a
    string, where a quote and a backslash are escaped: the stand-in echoes a request as JSON."""
    return [
        label
        for label, value in secrets.items()
        if value in text or json.dumps(value)[1:-1] in text
    ]


def test_the_scan_finds_a_secret_in_the_json_of_an_echo():
    # JSON escapes a quote and a backslash, so a password with one would hide from a scan of the
    # value as it is in what the stand-in echoes.
    secrets = {"a password with a quote": 'pass"word', "one with a backslash": "pass\\word"}
    echo = json.dumps({"body": {"password": 'pass"word', "other": "pass\\word"}})
    assert not any(value in echo for value in secrets.values())
    assert leaked(echo, secrets) == ["a password with a quote", "one with a backslash"]


class FakeMetabase:
    """What metabase-init can see of a Metabase instance."""

    def __init__(self) -> None:
        self.url = ""
        self.unhealthy_polls = 0
        self.raw_health: bytes | None = None
        self.has_user_setup = False
        self.gives_setup_token = True
        self.databases_as_list = False
        self.failures: dict[tuple[str, str], int] = {}
        # Calls that get no HTTP answer: "close" (the connection ends), "truncate" (it ends in
        # the middle of the body) or "garbage" (the answer is not HTTP).
        self.faults: dict[tuple[str, str], str] = {}
        # Calls that get a page of their own, as (status, body in bytes), such as a proxy sends.
        self.pages: dict[tuple[str, str], tuple[int, bytes]] = {}
        self.health_faults = 0
        self.admin: dict[str, str] | None = None
        self.databases: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []

    def listed(self) -> list[dict[str, Any]]:
        """The databases as GET /api/database shows them: Metabase masks the password."""
        shown = copy.deepcopy(self.databases)
        for database in shown:
            if "password" in database.get("details", {}):
                database["details"]["password"] = "**MetabasePass**"
        return shown

    def handle(self, method: str, path: str, body: Any, session: str | None) -> tuple[int, Any]:
        self.calls.append((method, path))
        if (method, path) in self.faults:
            return 0, self.faults[(method, path)]
        if (method, path) in self.pages:
            return self.pages[(method, path)]
        if (method, path) == ("GET", "/api/health") and self.health_faults > 0:
            self.health_faults -= 1
            return 0, "close"
        if (method, path) in self.failures:
            # The worst case for secrets: a server that explains its failure by echoing the request
            # back, with the tokens that it knows.
            echo = {"body": body, "session": session, "setup-token": "setup-token-1"}
            return self.failures[(method, path)], echo
        if (method, path) == ("GET", "/api/health"):
            if self.raw_health is not None:
                return 200, self.raw_health
            if self.unhealthy_polls > 0:
                self.unhealthy_polls -= 1
                return 503, {"status": "initializing"}
            return 200, {"status": "ok"}
        if (method, path) == ("GET", "/api/session/properties"):
            token = "setup-token-1" if self.gives_setup_token and not self.has_user_setup else None
            return 200, {"has-user-setup": self.has_user_setup, "setup-token": token}
        if (method, path) == ("POST", "/api/setup"):
            if self.has_user_setup or body.get("token") != "setup-token-1":
                return 403, "The setup token is not valid"
            if not body.get("prefs", {}).get("site_name"):
                return 400, "site_name is required"
            self.has_user_setup = True
            self.admin = {"email": body["user"]["email"], "password": body["user"]["password"]}
            return 200, {"id": "session-from-setup"}
        if (method, path) == ("POST", "/api/session"):
            if self.admin and body == {
                "username": self.admin["email"],
                "password": self.admin["password"],
            }:
                return 200, {"id": "session-from-login"}
            return 401, {"errors": {"password": "did not match stored password"}}
        if session not in ("session-from-setup", "session-from-login"):
            return 401, "Unauthenticated"
        if (method, path) == ("GET", "/api/database"):
            if self.databases_as_list:
                return 200, self.listed()
            return 200, {"data": self.listed(), "total": len(self.databases)}
        if (method, path) == ("POST", "/api/database"):
            database = {"id": len(self.databases) + 1, **body}
            self.databases.append(database)
            return 200, database
        if method == "PUT" and path.startswith("/api/database/"):
            database_id = int(path.rsplit("/", 1)[1])
            for database in self.databases:
                if database["id"] == database_id:
                    database.update(body)
                    return 200, database
        return 404, "Not found"


@pytest.fixture
def metabase():
    state = FakeMetabase()

    class Handler(BaseHTTPRequestHandler):
        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            status, payload = state.handle(
                self.command, self.path, body, self.headers.get("X-Metabase-Session")
            )
            if status == 0:
                self.break_connection(payload)
                return
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def break_connection(self, fault: str) -> None:
            """End the connection as a Metabase that crashes or restarts does."""
            self.close_connection = True
            if fault == "truncate":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"data": [')
            elif fault == "garbage":
                self.wfile.write(b"this is not an HTTP answer\r\n\r\n")

        do_GET = do_POST = do_PUT = serve

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    # A short poll interval, so that shutdown() does not wait half a second for every test.
    serving = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    serving.start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield state
    server.shutdown()
    server.server_close()


def env_for(metabase: FakeMetabase, **overrides: str) -> dict[str, str]:
    return {**ENV, "METABASE_URL": metabase.url, **overrides}


def test_first_start_creates_the_admin_and_the_warehouse_connection(metabase):
    assert mi.main(env_for(metabase)) == 0
    assert metabase.admin == {"email": "admin@example.com", "password": "Secret-password-1"}
    [warehouse] = metabase.databases
    assert warehouse["name"] == "Agoranomos warehouse"
    assert warehouse["engine"] == "postgres"
    assert warehouse["details"]["user"] == "metabase_reader"
    assert warehouse["details"]["schema-filters-type"] == "inclusion"
    assert warehouse["details"]["schema-filters-patterns"] == "marts"


def test_next_start_updates_the_connection_and_creates_nothing(metabase):
    assert mi.main(env_for(metabase)) == 0
    assert mi.main(env_for(metabase, WAREHOUSE_PASSWORD="rotated-password")) == 0
    assert metabase.calls.count(("POST", "/api/setup")) == 1
    assert len(metabase.databases) == 1
    assert metabase.databases[0]["details"]["password"] == "rotated-password"


def test_the_result_is_reported_without_a_secret(metabase, capsys):
    assert mi.main(env_for(metabase)) == 0
    first = capsys.readouterr()
    assert "the admin account is created" in first.out
    assert "warehouse connection 1 is created" in first.out
    assert mi.main(env_for(metabase)) == 0
    second = capsys.readouterr()
    assert "the admin account exists" in second.out
    assert "warehouse connection 1 is up to date" in second.out
    assert first.err == second.err == ""
    assert leaked(first.out + second.out) == []


def test_no_secret_is_sent_in_a_path(metabase):
    # Secrets travel in request bodies (and the session in its header), never in a URL, where
    # servers and proxies log them.
    assert mi.main(env_for(metabase)) == 0
    assert mi.main(env_for(metabase)) == 0
    paths = " ".join(path for _, path in metabase.calls)
    assert leaked(paths) == []


def test_waits_until_metabase_is_healthy(metabase):
    metabase.unhealthy_polls = 2
    mi.wait_until_healthy(mi.Metabase(metabase.url), timeout=5, interval=0.01)
    assert metabase.calls.count(("GET", "/api/health")) == 3


def test_gives_up_when_metabase_stays_unhealthy(metabase):
    metabase.unhealthy_polls = 1_000_000
    with pytest.raises(mi.SetupError, match="did not become healthy"):
        mi.wait_until_healthy(mi.Metabase(metabase.url), timeout=0.2, interval=0.01)


def test_an_answer_that_is_not_the_json_of_metabase_is_not_healthy(metabase):
    # Something else may answer on the port, such as a proxy with an error page.
    metabase.raw_health = b"<html>a proxy, not Metabase</html>"
    with pytest.raises(mi.SetupError, match="did not become healthy"):
        mi.wait_until_healthy(mi.Metabase(metabase.url), timeout=0.2, interval=0.01)


def test_gives_up_when_nothing_listens():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with pytest.raises(mi.SetupError, match="did not become healthy"):
        mi.wait_until_healthy(
            mi.Metabase(f"http://127.0.0.1:{port}", timeout=0.2), timeout=0.3, interval=0.05
        )


def test_a_wrong_admin_password_is_explained(metabase, capsys):
    assert mi.main(env_for(metabase)) == 0
    capsys.readouterr()
    assert mi.main(env_for(metabase, METABASE_ADMIN_PASSWORD="changed-in-env-1")) == 1
    error = capsys.readouterr().err
    assert "METABASE_ADMIN_PASSWORD" in error
    assert "HTTP 401" in error
    assert leaked(error, {**SECRETS, "the wrong password": "changed-in-env-1"}) == []


def test_a_new_metabase_that_gives_no_setup_token_is_explained(metabase, capsys):
    metabase.gives_setup_token = False
    assert mi.main(env_for(metabase)) == 1
    assert "no setup token" in capsys.readouterr().err
    assert metabase.admin is None


def test_an_array_of_databases_is_understood_as_well_as_an_object(metabase):
    # GET /api/database answers {"data": [...]} today; an array must not make a second connection.
    assert mi.main(env_for(metabase)) == 0
    metabase.databases_as_list = True
    assert mi.main(env_for(metabase)) == 0
    assert len(metabase.databases) == 1


# A call that fails: the step to fail, whether the first start has to happen before it, and how
# the message names the step.
FAILING_STEPS = [
    pytest.param(
        "GET",
        "/api/session/properties",
        False,
        "GET /api/session/properties failed",
        id="properties",
    ),
    pytest.param("POST", "/api/setup", False, "POST /api/setup failed", id="setup"),
    pytest.param("POST", "/api/session", True, "POST /api/session failed", id="sign-in"),
    pytest.param("GET", "/api/database", False, "GET /api/database failed", id="list-databases"),
    pytest.param("POST", "/api/database", False, "POST /api/database failed", id="connect"),
    pytest.param("PUT", "/api/database/1", True, "PUT /api/database/1 failed", id="update"),
]


@pytest.mark.parametrize(("method", "path", "after_first_start", "step"), FAILING_STEPS)
def test_a_failing_call_names_the_step_and_the_status_and_prints_no_secret(
    metabase, capsys, method, path, after_first_start, step
):
    # The stand-in answers the failing step with 500 and echoes the request, the session and the
    # setup token back. The message names the step and the status, and nothing of the answer.
    if after_first_start:
        assert mi.main(env_for(metabase)) == 0
        capsys.readouterr()
    metabase.failures[(method, path)] = 500
    assert mi.main(env_for(metabase)) == 1
    output = capsys.readouterr()
    assert step in output.err
    assert "HTTP 500" in output.err
    assert "./agoranomos logs metabase" in output.err
    assert leaked(output.out + output.err) == []


@pytest.mark.parametrize(
    ("method", "path", "after_first_start", "function"),
    [
        pytest.param("POST", "/api/setup", False, "ensure_admin", id="setup"),
        pytest.param("POST", "/api/session", True, "sign_in", id="sign-in"),
        pytest.param("POST", "/api/database", False, "ensure_warehouse", id="connect"),
    ],
)
def test_an_answer_of_an_unexpected_shape_ends_in_one_line(
    metabase, capsys, method, path, after_first_start, function
):
    # A success status with an echo of the request, the setup token and a password in it: the job
    # looks for an id that is not there. The line names the class of the error and the function
    # that read the answer, never the answer.
    if after_first_start:
        assert mi.main(env_for(metabase)) == 0
        capsys.readouterr()
    metabase.failures[(method, path)] = 200
    assert mi.main(env_for(metabase)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Traceback" not in output.err
    assert output.err.splitlines() == [
        f"metabase-init: an answer of Metabase had an unexpected shape (KeyError in {function}). "
        "See ./agoranomos logs metabase."
    ]
    assert leaked(output.err) == []


def test_the_line_of_an_unexpected_shape_names_the_function_that_raised():
    # The name of a function is code, never a value: a nested one keeps the name of the function
    # around it, and an error that was never raised has no function to name.
    def read_the_id(answer):
        return answer["id"]

    with pytest.raises(KeyError) as raised:
        read_the_id({})
    assert mi.unexpected_shape("Metabase", raised.value, "A hint.") == (
        "an answer of Metabase had an unexpected shape (KeyError in "
        "test_the_line_of_an_unexpected_shape_names_the_function_that_raised.read_the_id). "
        "A hint."
    )
    assert mi.unexpected_shape("Metabase", KeyError("id"), "A hint.") == (
        "an answer of Metabase had an unexpected shape (KeyError). A hint."
    )


# A page in Latin-1, which is not UTF-8, as a proxy in front of Metabase can send.
LATIN_1_PAGE = "<html><body>Passerelle indisponible, réessayez</body></html>".encode("latin-1")


@pytest.mark.parametrize(
    ("status", "line"),
    [
        pytest.param(
            502,
            "metabase-init: GET /api/session/properties failed with HTTP 502. "
            "See ./agoranomos logs metabase.",
            id="an-error-status",
        ),
        pytest.param(
            200,
            "metabase-init: an answer of Metabase had an unexpected shape (AttributeError in "
            "ensure_admin). See ./agoranomos logs metabase.",
            id="a-success-status",
        ),
    ],
)
def test_a_page_that_is_not_utf_8_ends_in_one_line(metabase, capsys, status, line):
    metabase.pages[("GET", "/api/session/properties")] = (status, LATIN_1_PAGE)
    assert mi.main(env_for(metabase)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err.splitlines() == [line]


def test_an_answer_that_is_not_utf_8_is_a_text_with_a_status(metabase):
    # The client gives the status and the page as text, with a replacement for what is not UTF-8,
    # whatever the status: the caller judges the status, and the shape of what it reads.
    metabase.pages[("GET", "/api/session/properties")] = (502, LATIN_1_PAGE)
    page = LATIN_1_PAGE.decode(errors="replace")
    assert "�" in page
    assert mi.Metabase(metabase.url).request("GET", "/api/session/properties") == (502, page)


def test_a_failing_call_is_an_error_with_the_step_the_status_and_a_fixed_hint_only(metabase):
    metabase.failures[("GET", "/api/database")] = 503
    client = mi.Metabase(metabase.url)
    with pytest.raises(mi.SetupError) as raised:
        client.call("GET", "/api/database")
    assert str(raised.value) == (
        "GET /api/database failed with HTTP 503. See ./agoranomos logs metabase."
    )


def test_a_call_that_may_find_nothing_gives_none_for_a_404_only(metabase):
    # demo-init waits for the DAGs of Airflow this way: a 404 means not there yet.
    client = mi.Metabase(metabase.url)
    client.session = "session-from-login"
    assert client.call("GET", "/api/not-there", missing_ok=True) is None
    metabase.failures[("GET", "/api/not-there")] = 403
    with pytest.raises(mi.SetupError, match=r"^GET /api/not-there failed with HTTP 403\. "):
        client.call("GET", "/api/not-there", missing_ok=True)


def test_a_client_of_another_api_ends_a_failure_with_its_own_hint(metabase):
    class Other(mi.ApiClient):
        logs_hint = "See ./agoranomos logs another-service."

    metabase.failures[("GET", "/api/database")] = 500
    with pytest.raises(mi.SetupError) as raised:
        Other(metabase.url).call("GET", "/api/database")
    assert str(raised.value) == (
        "GET /api/database failed with HTTP 500. See ./agoranomos logs another-service."
    )
    with pytest.raises(mi.SetupError) as raised:
        Other(f"http://127.0.0.1:{free_port()}", timeout=1).send("GET", "/api/database")
    assert str(raised.value) == (
        "GET /api/database failed: URLError (ConnectionRefusedError). "
        "See ./agoranomos logs another-service."
    )


@pytest.mark.parametrize(
    ("response", "items"),
    [
        pytest.param({"data": [{"id": 1}], "total": 1}, [{"id": 1}], id="object"),
        pytest.param([{"id": 1}], [{"id": 1}], id="bare"),
        pytest.param({"total": 0}, [], id="object-without-data"),
    ],
)
def test_a_list_comes_bare_or_as_the_data_of_an_object(response, items):
    assert mi.items_of(response) == items


def test_the_warehouse_can_be_found_without_a_warning(capsys):
    # demo-init uses the connection that metabase-init keeps up to date, whose log names it.
    config = mi.Config.from_env({**ENV, "METABASE_URL": "http://metabase:3000"})
    databases = [connection(7, "Copy"), connection(3, "Mine")]
    assert mi.find_warehouse(databases, config, warn=False)["id"] == 3
    assert capsys.readouterr().err == ""
    assert mi.find_warehouse(databases, config)["id"] == 3
    assert "ids 3, 7" in capsys.readouterr().err


WAREHOUSE_HINT = (
    "Check that warehouse-db is running and that METABASE_READER_PASSWORD in .env matches the "
    "warehouse (./agoranomos up updates it)."
)


@pytest.mark.parametrize(
    ("method", "path", "after_first_start"),
    [
        pytest.param("POST", "/api/database", False, id="connect"),
        pytest.param("PUT", "/api/database/1", True, id="update"),
    ],
)
def test_a_400_for_the_warehouse_says_what_to_check(
    metabase, capsys, method, path, after_first_start
):
    # Metabase answers 400 when it cannot connect with the details it was given.
    if after_first_start:
        assert mi.main(env_for(metabase)) == 0
        capsys.readouterr()
    metabase.failures[(method, path)] = 400
    assert mi.main(env_for(metabase)) == 1
    error = capsys.readouterr().err
    assert f"{method} {path} failed with HTTP 400" in error
    assert WAREHOUSE_HINT in error
    assert leaked(error) == []


@pytest.mark.parametrize("status", [401, 403, 404, 500, 502])
def test_the_hint_about_the_warehouse_is_for_a_400_only(metabase, capsys, status):
    metabase.failures[("POST", "/api/database")] = status
    assert mi.main(env_for(metabase)) == 1
    error = capsys.readouterr().err
    assert f"HTTP {status}" in error
    assert "METABASE_READER_PASSWORD" not in error
    assert "./agoranomos logs metabase" in error


def test_a_400_from_a_step_that_has_nothing_to_do_with_the_warehouse_does_not_blame_it(
    metabase, capsys
):
    metabase.failures[("GET", "/api/database")] = 400
    assert mi.main(env_for(metabase)) == 1
    error = capsys.readouterr().err
    assert "GET /api/database failed with HTTP 400" in error
    assert "METABASE_READER_PASSWORD" not in error


@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_sign_in_points_at_the_admin_settings(metabase, capsys, status):
    assert mi.main(env_for(metabase)) == 0
    capsys.readouterr()
    metabase.failures[("POST", "/api/session")] = status
    assert mi.main(env_for(metabase)) == 1
    error = capsys.readouterr().err
    assert f"HTTP {status}" in error
    assert "AGORANOMOS_ADMIN_EMAIL" in error
    assert "METABASE_ADMIN_PASSWORD" in error
    assert leaked(error) == []


@pytest.mark.parametrize("status", [400, 404, 429, 500, 502, 503])
def test_another_status_at_the_sign_in_is_not_blamed_on_the_password(metabase, capsys, status):
    assert mi.main(env_for(metabase)) == 0
    capsys.readouterr()
    metabase.failures[("POST", "/api/session")] = status
    assert mi.main(env_for(metabase)) == 1
    error = capsys.readouterr().err
    assert f"POST /api/session failed with HTTP {status}" in error
    assert "./agoranomos logs metabase" in error
    assert "METABASE_ADMIN_PASSWORD" not in error
    assert leaked(error) == []


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


# What a broken connection looks like to the client, and the class that the message names.
FAULTS = [
    pytest.param("close", "RemoteDisconnected", id="closed"),
    pytest.param("truncate", "IncompleteRead", id="cut-off"),
    pytest.param("garbage", "BadStatusLine", id="not-http"),
]


@pytest.mark.parametrize(("fault", "error_class"), FAULTS)
@pytest.mark.parametrize(("method", "path", "after_first_start", "step"), FAILING_STEPS)
def test_a_dropped_connection_is_one_line_with_the_step_and_the_class_of_the_error(
    metabase, capsys, method, path, after_first_start, step, fault, error_class
):
    if after_first_start:
        assert mi.main(env_for(metabase)) == 0
        capsys.readouterr()
    metabase.faults[(method, path)] = fault
    assert mi.main(env_for(metabase)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    [line] = output.err.splitlines()
    assert line.startswith(f"metabase-init: {step}")
    assert error_class in line
    assert "./agoranomos logs metabase" in line
    assert "Traceback" not in output.err
    assert leaked(output.err) == []


def test_a_call_that_cannot_connect_names_the_step_and_the_classes_of_the_error():
    client = mi.Metabase(f"http://127.0.0.1:{free_port()}", timeout=1)
    with pytest.raises(mi.SetupError) as raised:
        client.call("GET", "/api/database")
    assert str(raised.value) == (
        "GET /api/database failed: URLError (ConnectionRefusedError). "
        "See ./agoranomos logs metabase."
    )


def test_the_health_poll_keeps_retrying_after_a_dropped_connection(metabase):
    metabase.health_faults = 2
    mi.wait_until_healthy(mi.Metabase(metabase.url), timeout=5, interval=0.01)
    assert metabase.calls.count(("GET", "/api/health")) == 3


@pytest.mark.parametrize(("fault", "error_class"), FAULTS)
def test_a_broken_health_answer_is_not_healthy_and_not_a_crash(metabase, fault, error_class):
    metabase.faults[("GET", "/api/health")] = fault
    with pytest.raises(mi.SetupError, match="did not become healthy"):
        mi.wait_until_healthy(mi.Metabase(metabase.url), timeout=0.3, interval=0.01)


def test_proxy_variables_do_not_send_the_requests_elsewhere(metabase, monkeypatch):
    # Docker can put HTTP_PROXY into every container (the proxy settings of its client), and
    # urllib sends requests there, loopback and Compose names included, unless NO_PROXY says no.
    dead = f"http://127.0.0.1:{free_port()}"
    for name in (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        monkeypatch.setenv(name, dead)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    # The control: a plain opener, built now, reads the variables above and goes to the proxy, which
    # is dead, so this check can fail. urlopen would not do: it builds its opener once, at its first
    # use in the process, with the variables of that moment, and keeps it for every later caller.
    with pytest.raises(urllib.error.URLError):
        urllib.request.build_opener().open(f"{metabase.url}/api/health", timeout=2)
    client = mi.Metabase(metabase.url, timeout=5)
    assert client.request("GET", "/api/health") == (200, {"status": "ok"})
    assert mi.main(env_for(metabase)) == 0


@pytest.mark.parametrize("port", ["five", "54 32", "5432.0", "", "-1", "0", "65536", "99999"])
def test_a_warehouse_port_that_is_not_a_port_number_is_named(capsys, port):
    env = {**ENV, "METABASE_URL": "http://metabase:3000", "WAREHOUSE_PORT": port}
    if port:
        with pytest.raises(mi.SetupError, match="WAREHOUSE_PORT"):
            mi.Config.from_env(env)
    assert mi.main(env) == 1
    error = capsys.readouterr().err
    [line] = error.splitlines()
    assert line.startswith("metabase-init: ")
    assert "WAREHOUSE_PORT" in line
    assert "Traceback" not in error


def connection(database_id: int, name: str, **details: Any) -> dict[str, Any]:
    """A Postgres connection as the stand-in keeps it: at the address of the warehouse, unless the
    details say otherwise."""
    return {
        "id": database_id,
        "name": name,
        "engine": "postgres",
        "details": {
            "host": "warehouse-db",
            "port": 5432,
            "dbname": "warehouse",
            "user": "metabase_reader",
            "password": "old-password",
            **details,
        },
    }


def test_a_renamed_connection_is_updated_and_not_created_again(metabase):
    assert mi.main(env_for(metabase)) == 0
    metabase.databases[0]["name"] = "Marketing data"  # renamed in the Metabase UI
    assert mi.main(env_for(metabase, WAREHOUSE_PASSWORD="rotated-password")) == 0
    [warehouse] = metabase.databases  # no second connection
    assert warehouse["name"] == "Marketing data"  # the name that the person gave it stays
    assert warehouse["details"]["password"] == "rotated-password"
    assert metabase.calls.count(("POST", "/api/database")) == 1  # the first start's


def test_the_job_updates_the_details_it_owns_and_keeps_the_others(metabase):
    assert mi.main(env_for(metabase)) == 0
    details = metabase.databases[0]["details"]
    details["additional-options"] = "connectTimeout=5"  # set in the Metabase UI
    details["ssl"] = True  # the job owns this one
    assert mi.main(env_for(metabase, WAREHOUSE_PASSWORD="rotated-password")) == 0
    [warehouse] = metabase.databases
    assert warehouse["details"]["additional-options"] == "connectTimeout=5"
    assert warehouse["details"]["ssl"] is False
    # The list shows the password masked: what the job saves is its own, never the mask.
    assert warehouse["details"]["password"] == "rotated-password"
    assert "**MetabasePass**" not in json.dumps(warehouse)


def test_an_address_with_the_port_as_text_is_the_same_address(metabase):
    metabase.databases = [connection(5, "Mine", port="5432")]
    assert mi.main(env_for(metabase)) == 0
    [warehouse] = metabase.databases
    assert warehouse["id"] == 5
    assert warehouse["details"]["password"] == "reader-password"


@pytest.mark.parametrize(
    "other",
    [
        pytest.param({"host": "another-host"}, id="host"),
        pytest.param({"port": 5433}, id="port"),
        pytest.param({"dbname": "another"}, id="database"),
        pytest.param({"user": "another_user"}, id="user"),
    ],
)
def test_a_connection_with_another_address_is_not_the_warehouse(metabase, other):
    metabase.databases = [connection(5, "Mine", **other)]
    assert mi.main(env_for(metabase)) == 0
    assert [d["id"] for d in metabase.databases] == [5, 2]
    assert metabase.databases[0]["details"]["password"] == "old-password"  # not touched
    assert metabase.databases[1]["name"] == "Agoranomos warehouse"


def test_connections_of_other_engines_and_without_details_are_left_alone(metabase):
    sample = {"id": 1, "name": "Sample Database", "engine": "h2", "details": {"db": "file:x"}}
    odd = {"id": 10, "name": "No details", "engine": "postgres"}
    mysql = {**connection(11, "A MySQL"), "engine": "mysql"}
    metabase.databases = [sample, odd, mysql]
    assert mi.main(env_for(metabase)) == 0
    assert [d["id"] for d in metabase.databases] == [1, 10, 11, 4]
    assert metabase.databases[3]["name"] == "Agoranomos warehouse"
    assert metabase.databases[2]["details"]["password"] == "old-password"


def test_the_name_comes_before_the_address(metabase, capsys):
    # The connection with the name is the job's, even when it points elsewhere, and a connection
    # of the person that points at the warehouse is not taken for it.
    metabase.databases = [
        connection(3, "Agoranomos warehouse", host="an-old-host"),
        connection(2, "Mine"),
    ]
    assert mi.main(env_for(metabase)) == 0
    by_id = {d["id"]: d for d in metabase.databases}
    assert by_id[3]["details"]["host"] == "warehouse-db"
    assert by_id[3]["details"]["password"] == "reader-password"
    assert by_id[2]["details"]["password"] == "old-password"
    assert "warehouse connection 3 is up to date" in capsys.readouterr().out


def test_two_connections_at_the_address_use_the_lowest_id_and_say_so(metabase, capsys):
    assert mi.main(env_for(metabase)) == 0
    capsys.readouterr()
    metabase.databases[0]["name"] = "Marketing data"
    # The person made another connection to the same warehouse; the list shows its id first.
    metabase.databases.insert(0, connection(7, "Copy of the warehouse"))
    assert mi.main(env_for(metabase, WAREHOUSE_PASSWORD="rotated-password")) == 0
    output = capsys.readouterr()
    by_id = {d["id"]: d for d in metabase.databases}
    assert by_id[1]["details"]["password"] == "rotated-password"
    assert by_id[7]["details"]["password"] == "old-password"
    assert "warehouse connection 1 is up to date" in output.out
    [line] = output.err.splitlines()
    assert "ids 1, 7" in line
    assert "updating 1" in line
    # The ids, and no detail of any connection.
    for value in ("warehouse-db", "metabase_reader", "rotated-password", "old-password"):
        assert value not in line


def test_two_connections_with_the_name_use_the_lowest_id_too(metabase, capsys):
    metabase.databases = [
        connection(4, "Agoranomos warehouse"),
        connection(2, "Agoranomos warehouse"),
    ]
    assert mi.main(env_for(metabase)) == 0
    output = capsys.readouterr()
    by_id = {d["id"]: d for d in metabase.databases}
    assert by_id[2]["details"]["password"] == "reader-password"
    assert by_id[4]["details"]["password"] == "old-password"
    [line] = output.err.splitlines()
    assert "ids 2, 4" in line
    assert "updating 2" in line


def test_one_matching_connection_makes_no_noise(metabase, capsys):
    assert mi.main(env_for(metabase)) == 0
    metabase.databases[0]["name"] = "Marketing data"
    assert mi.main(env_for(metabase)) == 0
    assert capsys.readouterr().err == ""


def test_missing_settings_are_named():
    env = {**ENV, "METABASE_URL": "http://metabase:3000"}
    del env["WAREHOUSE_PASSWORD"]
    with pytest.raises(mi.SetupError, match="WAREHOUSE_PASSWORD"):
        mi.Config.from_env(env)


def test_main_reports_missing_settings_and_stops(capsys):
    assert mi.main({}) == 1
    error = capsys.readouterr().err
    assert error.startswith("metabase-init: missing environment variables: METABASE_URL")
    assert "WAREHOUSE_PASSWORD" in error


def test_the_url_must_be_http():
    with pytest.raises(mi.SetupError, match="METABASE_URL"):
        mi.Config.from_env({**ENV, "METABASE_URL": "file:///etc/passwd"})


def test_a_trailing_slash_of_the_url_is_dropped():
    config = mi.Config.from_env({**ENV, "METABASE_URL": "http://metabase:3000/"})
    assert config.url == "http://metabase:3000"
    assert config.warehouse_port == 5432


def test_the_script_runs_as_compose_runs_it():
    # compose.yaml runs the file with the python of the image, in an environment of its own.
    script = Path(mi.__file__)
    result = subprocess.run(
        [sys.executable, str(script)], env={}, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 1
    assert "missing environment variables" in result.stderr
    assert "Traceback" not in result.stderr


def test_the_module_imports_only_the_standard_library():
    # metabase-init runs in a plain python image: it has the standard library and nothing else.
    tree = ast.parse(Path(mi.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    assert imported
    assert imported <= set(sys.stdlib_module_names), sorted(imported - set(sys.stdlib_module_names))
