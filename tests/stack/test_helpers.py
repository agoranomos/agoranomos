"""Tests of the helpers themselves: they need neither Docker nor a running stack."""

import subprocess

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
