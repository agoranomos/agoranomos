"""The health check of the DAG processor and of the triggerer (x-airflow-job-check in compose.yaml),
read from compose.yaml as text and run against a stand-in for psycopg2, so that no Docker and no
database are needed. tests/stack/test_variants.py checks what Compose makes of it, and
tests/stack/test_airflow.py runs it in the containers of a running stack."""

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = REPO_ROOT / "compose.yaml"
ANCHOR = "x-airflow-job-check: &airflow-job-check |"
# A made-up connection string in the form that compose.yaml gives Airflow.
CONNECTION = "postgresql+psycopg2://airflow:made-up-password@platform-db:5432/airflow"
HOST = "0123456789ab"

# Stands in for psycopg2: connect() reads what to do from FAKE_SCENARIO, and what the check did is
# written as JSON to FAKE_RECORD when it closes the connection, or when connect() fails.
FAKE_PSYCOPG2 = """
import json
import os

RECORD = {}


def _write():
    with open(os.environ["FAKE_RECORD"], "w", encoding="utf-8") as handle:
        json.dump(RECORD, handle)


class OperationalError(Exception):
    pass


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        RECORD["cursor_closed"] = True
        return False

    def execute(self, query, params):
        RECORD["query"] = query
        RECORD["params"] = list(params)

    def fetchone(self):
        return (1,) if os.environ["FAKE_SCENARIO"] == "alive" else None


class _Connection:
    def cursor(self):
        return _Cursor()

    def close(self):
        RECORD["closed"] = True
        _write()


def connect(dsn, **kwargs):
    RECORD.update(dsn=dsn, kwargs=kwargs)
    if os.environ["FAKE_SCENARIO"] == "down":
        _write()
        raise OperationalError("connection refused")
    return _Connection()
"""


def job_check() -> str:
    """The Python of the check: the block scalar of the anchor, without its indent of two spaces."""
    lines = COMPOSE.read_text(encoding="utf-8").splitlines()
    assert ANCHOR in lines, f"compose.yaml has no line {ANCHOR!r}"
    code = []
    for line in lines[lines.index(ANCHOR) + 1 :]:
        if line and not line.startswith("  "):
            break
        code.append(line[2:])
    return "\n".join(code).strip() + "\n"


def run_check(tmp_path: Path, scenario: str, job_type: str = "DagProcessorJob"):
    """Run the check as the health check of Compose does, python -c with the job type as its
    argument, against the stand-in: its exit code and what it did."""
    package = tmp_path / "psycopg2"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text(FAKE_PSYCOPG2, encoding="utf-8")
    record = tmp_path / "record.json"
    result = subprocess.run(
        [sys.executable, "-c", job_check(), job_type],
        env={
            "PYTHONPATH": str(tmp_path),
            "PYTHONDONTWRITEBYTECODE": "1",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": CONNECTION,
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
    # airflow jobs check imports Airflow, which costs more than a second of CPU and about 150 MiB
    # at every check: this check needs psycopg2 and the standard library only.
    imported = set()
    for node in ast.walk(ast.parse(job_check())):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported == {"contextlib", "os", "psycopg2", "sys"}


@pytest.mark.parametrize("job_type", ["DagProcessorJob", "TriggererJob"])
def test_the_check_passes_while_the_job_of_this_container_sends_heartbeats(tmp_path, job_type):
    code, record = run_check(tmp_path, "alive", job_type)
    assert code == 0
    # libpq takes the URL of SQLAlchemy without the name of the driver.
    assert record["dsn"] == CONNECTION.replace("postgresql+psycopg2://", "postgresql://")
    assert record["kwargs"] == {"connect_timeout": 5}
    # The job of the type that the check gets, on the host name of the container, as airflow jobs
    # check --job-type ... --hostname ... asks for it.
    assert record["params"] == [job_type, HOST]
    query = " ".join(record["query"].split())
    assert "FROM job WHERE job_type = %s AND hostname = %s" in query
    assert "state = 'running'" in query
    # [dag_processor] health_check_threshold and [triggerer] triggerer_health_check_threshold:
    # 30 seconds by default in Airflow 3.3.2.
    assert "latest_heartbeat > now() - interval '30 seconds'" in query
    assert record["closed"] is True


def test_the_check_fails_when_no_running_job_has_sent_a_heartbeat(tmp_path):
    code, record = run_check(tmp_path, "none")
    assert code == 1
    assert record["closed"] is True


def test_the_check_fails_when_the_database_cannot_be_reached(tmp_path):
    code, _ = run_check(tmp_path, "down")
    assert code != 0


def test_compose_runs_the_check_for_the_dag_processor_and_the_triggerer():
    # Read as text: the anchor and both checks that use it, each with the type of its job.
    text = COMPOSE.read_text(encoding="utf-8")
    for job_type in ("DagProcessorJob", "TriggererJob"):
        line = f'test: ["CMD", "python", "-c", *airflow-job-check, "{job_type}"]'
        assert text.count(line) == 1, job_type
    assert "jobs check --job-type" not in text


def test_compose_parses_an_unchanged_dag_file_every_five_minutes():
    # In the environment that every Airflow service shares (x-airflow).
    lines = COMPOSE.read_text(encoding="utf-8").splitlines()
    start = lines.index("  environment: &airflow-environment")
    end = next(i for i in range(start + 1, len(lines)) if not lines[i].startswith("    "))
    environment = lines[start + 1 : end]
    assert '    AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL: "300"' in environment
    assert '    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "60"' in environment
