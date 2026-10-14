"""The health check of the Celery worker (x-airflow-worker-check in compose.celery.yaml), read from
compose.celery.yaml as text and run against a stand-in for Celery, so that no Docker and no broker
are needed. tests/stack/test_variants.py checks what Compose makes of it, and
tests/stack/test_celery.py runs it in the worker of a running stack."""

import ast
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_CELERY = REPO_ROOT / "compose.celery.yaml"
ANCHOR = "x-airflow-worker-check: &airflow-worker-check |"
# A made-up broker URL in the form that compose.celery.yaml gives Airflow.
BROKER = "redis://:made-up-password@broker:6379/0"
HOST = "0123456789ab"
NODE = f"celery@{HOST}"

# Stands in for Celery: Celery() records how the check made its app, and control.ping() reads what
# to do from FAKE_SCENARIO. What the check did is written as JSON to FAKE_RECORD.
FAKE_CELERY = """
import json
import os

RECORD = {}


def _write():
    with open(os.environ["FAKE_RECORD"], "w", encoding="utf-8") as handle:
        json.dump(RECORD, handle)


class _Control:
    def ping(self, destination=None, timeout=1.0, **kwargs):
        RECORD.update(destination=destination, timeout=timeout, kwargs=kwargs)
        _write()
        scenario = os.environ["FAKE_SCENARIO"]
        if scenario == "down":
            raise OSError("Error 111 connecting to broker:6379. Connection refused.")
        if scenario == "alive":
            return [{node: {"ok": "pong"}} for node in destination]
        if scenario == "another node":
            return [{"celery@another-container": {"ok": "pong"}}]
        return []


class Celery:
    def __init__(self, main=None, **kwargs):
        RECORD.update(main=main, app_kwargs=kwargs)
        self.control = _Control()
"""


def worker_check() -> str:
    """The Python of the check: the block scalar of the anchor, without its indent of two spaces."""
    lines = COMPOSE_CELERY.read_text(encoding="utf-8").splitlines()
    assert ANCHOR in lines, f"compose.celery.yaml has no line {ANCHOR!r}"
    code = []
    for line in lines[lines.index(ANCHOR) + 1 :]:
        if line and not line.startswith("  "):
            break
        code.append(line[2:])
    return "\n".join(code).strip() + "\n"


def run_check(tmp_path: Path, scenario: str):
    """Run the check as the health check of Compose does, python -c, against the stand-in: its
    exit code and what it did."""
    package = tmp_path / "celery"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text(FAKE_CELERY, encoding="utf-8")
    record = tmp_path / "record.json"
    result = subprocess.run(
        [sys.executable, "-c", worker_check()],
        env={
            "PYTHONPATH": str(tmp_path),
            "PYTHONDONTWRITEBYTECODE": "1",
            "AIRFLOW__CELERY__BROKER_URL": BROKER,
            "HOSTNAME": HOST,
            "FAKE_SCENARIO": scenario,
            "FAKE_RECORD": str(record),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.returncode, json.loads(record.read_text(encoding="utf-8"))


def test_the_check_imports_no_airflow():
    # celery --app airflow.providers.celery.executors.celery_executor.app inspect ping imports
    # Airflow, which costs more than a second of CPU and 120 MiB of the memory limit of the worker
    # at every check: this check needs Celery and the standard library only.
    imported = set()
    for node in ast.walk(ast.parse(worker_check())):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported == {"celery", "os"}


def test_the_check_holds_no_secret_and_no_dollar_sign():
    # The URL of the broker, with its password, comes from the environment of the container, so
    # that no command line shows it. Compose would interpolate a dollar sign in the check.
    code = worker_check()
    assert 'os.environ["AIRFLOW__CELERY__BROKER_URL"]' in code
    assert "redis://" not in code
    assert "$" not in code


def test_the_check_passes_when_the_worker_of_this_container_answers(tmp_path):
    code, record = run_check(tmp_path, "alive")
    assert code == 0
    # An app of Celery alone, with the broker of Airflow, and nothing else of its configuration:
    # a ping needs no result backend.
    assert record["app_kwargs"] == {"broker": BROKER}
    # The node of this container only, as celery inspect ping -d celery@$HOSTNAME asks for it:
    # Celery names a worker celery@ and its host name. With a destination, Celery returns as soon
    # as that node has answered, and waits 5 seconds at most.
    assert record["destination"] == [NODE]
    assert record["timeout"] == 5.0
    assert record["kwargs"] == {}


def test_the_check_fails_when_no_worker_answers(tmp_path):
    code, _ = run_check(tmp_path, "none")
    assert code == 1


def test_the_check_fails_when_another_node_answers(tmp_path):
    code, _ = run_check(tmp_path, "another node")
    assert code == 1


def test_the_check_fails_when_the_broker_cannot_be_reached(tmp_path):
    code, _ = run_check(tmp_path, "down")
    assert code != 0


def test_compose_runs_the_check_for_the_worker():
    # Read as text: the anchor and the one check that uses it. No check of the file runs celery
    # inspect with the app of Airflow any more.
    text = COMPOSE_CELERY.read_text(encoding="utf-8")
    assert text.count('test: ["CMD", "python", "-c", *airflow-worker-check]') == 1
    checks = [line for line in text.splitlines() if line.strip().startswith("test:")]
    assert not [line for line in checks if "inspect" in line or "celery_executor" in line]
