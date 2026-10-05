"""What only the Celery variant has: the broker and the worker (skipped in the local variant)."""

import pytest
from stack_helpers import active_variant, compose, process_uids

pytestmark = pytest.mark.skipif(
    active_variant() != "celery",
    reason="the broker and the worker need COMPOSE_FILE=compose.yaml:compose.celery.yaml",
)


def valkey_cli(env, *args, password=True):
    """Run valkey-cli in the broker. The password goes in the environment of the process, which
    -e VALKEYCLI_AUTH hands on to the container: never in an argument, where ps could show it."""
    options = ["-e", "VALKEYCLI_AUTH"] if password else []
    return compose(
        "exec",
        "-T",
        *options,
        "broker",
        "valkey-cli",
        *args,
        check=False,
        env={"VALKEYCLI_AUTH": env["VALKEY_PASSWORD"]},
    )


def test_the_broker_asks_for_the_password(env):
    result = valkey_cli(env, "ping", password=False)
    assert "NOAUTH" in result.stdout + result.stderr
    result = valkey_cli(env, "ping")
    assert result.stdout.strip() == "PONG", result.stderr


def test_the_broker_keeps_nothing_on_disk(env):
    result = valkey_cli(env, "CONFIG", "GET", "save")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split("\n")[:2] == ["save", ""], result.stdout
    result = valkey_cli(env, "CONFIG", "GET", "appendonly")
    assert result.stdout.split() == ["appendonly", "no"], result.stdout


def test_the_broker_server_does_not_run_as_root():
    # The image drops privileges only when its command is valkey-server, and the command here is a
    # shell, so compose.celery.yaml sets the user. The image has no ps: the helper reads the
    # process files. It always succeeds, so a server that is not there is an empty answer, and
    # fails here.
    result = compose("exec", "-T", "broker", "sh", "-c", process_uids("valkey-server"))
    assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"], result.stdout


def test_the_worker_does_not_run_as_root():
    result = compose("exec", "-T", "airflow-worker", "id", "-u")
    assert result.stdout.strip() == "50000"
