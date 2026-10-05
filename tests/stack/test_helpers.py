"""Tests of the helpers themselves: they need neither Docker nor a running stack."""

import json
import os
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

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


def test_a_failed_job_is_reported_by_its_exit_code_and_never_by_its_output():
    # A failed ALTER ROLE shows its statement, password included, in the output of db-init.
    result = subprocess.CompletedProcess(
        ["docker"], 3, stdout="", stderr="ERROR: ALTER ROLE airflow WITH LOGIN PASSWORD 'hunter2'"
    )
    message = stack_helpers.job_failed("db-init", result)
    assert "hunter2" not in message
    assert "db-init" in message
    assert "3" in message


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
    stack_tests = Path(stack_helpers.__file__).parent
    shutil.copy(stack_tests / "conftest.py", tmp_path / "conftest.py")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", *options],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(stack_tests)},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    report = result.stdout + result.stderr
    assert "6 failed" in report, "the tests did not fail as they should"
    # The arguments are printed, and show no value.
    assert "Secret('<hidden>')" in report
    assert "EnvValues(" in report
    hidden = {"the token": values["__TOKEN__"], "the password": password}
    assert [label for label, value in hidden.items() if value in report] == []
    assert f"Bearer {values['__TOKEN__']}" not in report
    # The control shows that this check can see a leak.
    assert values["__PLAIN__"] in report


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
