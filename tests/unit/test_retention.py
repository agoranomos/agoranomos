"""The retention of the service data (ADR-0044), without Docker: the cleanup of the database of
Airflow at the end of the command of airflow-init, read from compose.yaml as text and run against
stand-ins for Airflow and pendulum; the retention of the log of the queries of Metabase; the DAG of
maintenance, read as source; its script of the logs, run against a folder of this test; and its
cleanup of ops, run against a stand-in for psycopg2. tests/stack/test_retention.py checks all of
them on a running stack."""

import ast
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import maintenance

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = REPO_ROOT / "compose.yaml"
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "agoranomos_maintenance.py"
MAINTENANCE = REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines" / "maintenance.py"

# The tables that R4 (#190) checked with airflow db clean on Airflow 3.3.2: every table that the
# command takes but dag.
CLEANED = {
    "asset_event",
    "callback",
    "connection_test_request",
    "dag_run",
    "dag_version",
    "deadline",
    "import_error",
    "job",
    "log",
    "partitioned_asset_key_log",
    "revoked_token",
    "session",
    "task_instance",
    "task_instance_history",
    "task_reschedule",
    "task_state_store",
    "trigger",
    "xcom",
}
# The tables that Airflow 3.3.2 filters by a column dag_id that they lack, and those with rows of no
# DAG: a call with --exclude-dag-ids leaves them as they are.
UNFILTERED = {"asset_event", "deadline", "job", "log", "task_reschedule"}

# Stands in for airflow.__main__: main() writes each command line to FAKE_RECORD, one JSON list a
# line, and makes a db clean fail as FAKE_FAIL says: integrity raises an error of its own class
# named IntegrityError, as SQLAlchemy does, and exit raises SystemExit, as db clean does when it
# has no table to clean.
FAKE_MAIN = """
import json
import os
import sys


class IntegrityError(Exception):
    pass


def main():
    with open(os.environ["FAKE_RECORD"], "a", encoding="utf-8") as handle:
        handle.write(json.dumps(sys.argv[1:]) + "\\n")
    if sys.argv[1:3] == ["db", "clean"]:
        fail = os.environ.get("FAKE_FAIL", "")
        if fail == "integrity":
            message = "(psycopg2.errors.IntegrityConstraintViolation) refused\\nDETAIL: a key"
            raise IntegrityError(message)
        if fail == "exit":
            raise SystemExit("No tables selected for db cleanup. Please choose valid table names.")
"""
# Stands in for the security manager of the FAB provider: no user ai, so a switch that is off
# changes nothing.
FAKE_FAB_UTILS = """
import contextlib
import types


@contextlib.contextmanager
def get_application_builder():
    yield types.SimpleNamespace(sm=types.SimpleNamespace(find_user=lambda username: None))
"""
# Stands in for pendulum: now("UTC").subtract(days=n).to_iso8601_string().
FAKE_PENDULUM = """
from datetime import UTC, datetime, timedelta


class _Moment:
    def __init__(self, moment):
        self.moment = moment

    def subtract(self, days):
        return _Moment(self.moment - timedelta(days=days))

    def to_iso8601_string(self):
        return self.moment.isoformat()


def now(tz):
    assert tz == "UTC"
    return _Moment(datetime.now(UTC))
"""


def init_command() -> str:
    """The Python of the command of airflow-init: the block scalar after its command, without its
    indent of eight spaces."""
    lines = COMPOSE.read_text(encoding="utf-8").splitlines()
    start = lines.index("  airflow-init:")
    first = next(n for n in range(start, len(lines)) if lines[n] == "      - |") + 1
    code = []
    for line in lines[first:]:
        if line and not line.startswith("        "):
            break
        code.append(line[8:])
    return "\n".join(code).strip() + "\n"


def run_init(tmp_path: Path, fail: str = "") -> tuple[subprocess.CompletedProcess[str], list]:
    """Run the command of airflow-init against the stand-ins: the process and the command lines that
    it gave Airflow, in order."""
    files = {
        "airflow/__init__.py": "",
        "airflow/__main__.py": FAKE_MAIN,
        "airflow/providers/__init__.py": "",
        "airflow/providers/fab/__init__.py": "",
        "airflow/providers/fab/auth_manager/__init__.py": "",
        "airflow/providers/fab/auth_manager/cli_commands/__init__.py": "",
        "airflow/providers/fab/auth_manager/cli_commands/utils.py": FAKE_FAB_UTILS,
        "pendulum/__init__.py": FAKE_PENDULUM,
    }
    for name, text in files.items():
        path = tmp_path / "stand-ins" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    record = tmp_path / "record.jsonl"
    result = subprocess.run(
        [sys.executable, "-c", init_command()],
        env={
            "PYTHONPATH": str(tmp_path / "stand-ins"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "AGORANOMOS_ADMIN_EMAIL": "admin@example.com",
            "AIRFLOW_ADMIN_PASSWORD": "made-up-password",
            "AIRFLOW_MCP": "false",
            "AIRFLOW_AI_PASSWORD": "made-up-password",
            "FAKE_RECORD": str(record),
            "FAKE_FAIL": fail,
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    calls = [json.loads(line) for line in record.read_text(encoding="utf-8").splitlines()]
    return result, calls


def option(call: list[str], name: str) -> str:
    return call[call.index(name) + 1]


def test_airflow_init_cleans_its_database_last_in_two_calls(tmp_path):
    result, calls = run_init(tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    # After the setup of the database, the admin, the pools and the user ai.
    assert [call[:2] for call in calls] == [
        ["db", "check-migrations"],
        ["users", "create"],
        ["pools", "set"],
        ["pools", "set"],
        ["db", "clean"],
        ["db", "clean"],
    ]
    first, second = calls[-2:]
    tables = [set(option(call, "--tables").split(",")) for call in (first, second)]
    # Every table that R4 checked, each in one call, and never dag, which a stop of more than 90
    # days would empty before the DAG processor runs, with the pauses of the DAGs.
    assert tables[0] | tables[1] == CLEANED
    assert not tables[0] & tables[1]
    assert "dag" not in tables[0] | tables[1]
    # The first call keeps the manual run of the demo history; the second takes the tables that
    # such a filter misses.
    assert option(first, "--exclude-dag-ids") == "agoranomos_demo_history"
    assert "--exclude-dag-ids" not in second
    assert tables[1] == UNFILTERED
    for call in (first, second):
        assert call[-2:] == ["--skip-archive", "--yes"]
        # A failure of a table warns and does not stop the job.
        assert "--error-on-cleanup-failure" not in call
        assert "--dag-ids" not in call
        limit = datetime.fromisoformat(option(call, "--clean-before-timestamp"))
        assert abs(limit - (datetime.now(UTC) - timedelta(days=90))) < timedelta(minutes=1)


@pytest.mark.parametrize(
    ("fail", "error"),
    [
        ("integrity", "IntegrityError: (psycopg2.errors.IntegrityConstraintViolation) refused"),
        (
            "exit",
            "SystemExit: No tables selected for db cleanup. Please choose valid table names.",
        ),
    ],
)
def test_a_failed_cleanup_warns_in_one_line_and_the_job_succeeds(tmp_path, fail, error):
    # db clean swallows the operational and programming errors of a table itself; any other error,
    # such as one of integrity, would end the job and stop ./agoranomos up.
    result, calls = run_init(tmp_path, fail)
    assert result.returncode == 0, result.stderr
    # Both calls ran, and each says in one line that its tables keep their rows, without the rest
    # of the error.
    assert [call[:2] for call in calls[-2:]] == [["db", "clean"], ["db", "clean"]]
    lines = result.stderr.splitlines()
    assert len(lines) == 2, lines
    for line, call in zip(lines, calls[-2:], strict=True):
        tables = option(call, "--tables")
        assert line == (
            f"airflow-init: the database of Airflow keeps its old rows of {tables} until the next"
            f" start: the cleanup failed with {error}"
        ), line


def test_metabase_keeps_the_log_of_its_queries_90_days():
    # In the environment of the service metabase; Metabase 0.64.1 does not show it in its API.
    lines = COMPOSE.read_text(encoding="utf-8").splitlines()
    start = lines.index("  metabase:")
    # The next service is the next line with an indent of two spaces and a name.
    end = next(
        n
        for n in range(start + 1, len(lines))
        if lines[n].startswith("  ") and lines[n][2:3].isalpha()
    )
    assert '      MB_AUDIT_MAX_RETENTION_DAYS: "90"' in lines[start:end]


def dag_keywords(path: Path) -> dict[str, str]:
    """The keyword arguments of the decorator dag(...) of the file, as source, by name."""
    [decorator] = [
        decorator
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.FunctionDef)
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call) and ast.unparse(decorator.func) == "dag"
    ]
    return {keyword.arg: ast.unparse(keyword.value) for keyword in decorator.keywords}


def test_the_maintenance_runs_once_a_day_by_itself_one_task_at_a_time():
    found = dag_keywords(DAG_FILE)
    assert found["dag_id"] == "'agoranomos_maintenance'"
    assert found["schedule"] == "SCHEDULE"
    assert found["catchup"] == "False"
    assert found["max_active_runs"] == "1"
    assert found["max_active_tasks"] == "1"
    # Unlike the other DAGs of the base package, which start paused: nobody has to unpause it.
    assert found["is_paused_upon_creation"] == "False"
    text = DAG_FILE.read_text(encoding="utf-8")
    assert 'SCHEDULE = CronTriggerTimetable("23 4 * * *", timezone="UTC")' in text


def test_the_tasks_of_the_maintenance_do_not_wait_for_each_other():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    [function] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "agoranomos_maintenance"
    ]
    assert [ast.unparse(statement) for statement in function.body] == [
        "BashOperator(task_id='clean_airflow_logs', "
        "bash_command=maintenance.clean_logs_command(), do_xcom_push=False)",
        "clean_ops()",
    ]


def test_psycopg2_is_imported_in_the_task_only():
    # The DAG processor parses the file of the DAG often, and imports what it imports.
    for path in (DAG_FILE, MAINTENANCE):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        top = [
            alias.name if isinstance(node, ast.Import) else node.module
            for node in tree.body
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        ]
        assert "psycopg2" not in top, path.name
    calls = [ast.unparse(node) for node in ast.walk(ast.parse(DAG_FILE.read_text("utf-8")))]
    assert "mask_secret(maintenance.transformer_credentials()['password'])" in calls


def make_file(path: Path, age: timedelta) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a line of a log\n", encoding="utf-8")
    moment = time.time() - age.total_seconds()
    os.utime(path, (moment, moment))
    return path


def make_folder(path: Path, age: timedelta) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    moment = time.time() - age.total_seconds()
    os.utime(path, (moment, moment))
    return path


def test_the_script_of_the_logs_deletes_what_is_older_than_30_days(tmp_path):
    logs = tmp_path / "logs with a space"
    run = logs / "dag_id=agoranomos_demo_daily" / "run_id=scheduled__2026-01-01"
    old = make_file(run / "task_id=due" / "attempt=1.log", timedelta(days=31))
    young = make_file(run / "task_id=load" / "attempt=1.log", timedelta(days=29))
    # A new attempt in an old run: the folders keep the time of their first entries.
    retried = make_file(run / "task_id=again" / "attempt=2.log", timedelta(minutes=5))
    first = make_file(run / "task_id=again" / "attempt=1.log", timedelta(days=200))
    make_folder(run / "task_id=again", timedelta(days=200))
    # A trigger writes a log of its own beside the attempt.
    trigger = make_file(run / "task_id=due" / "attempt=1.log.trigger.7.log", timedelta(days=45))
    # Empty folders of two days, one in another, which go in one pass, and an empty folder that a
    # task has just made.
    gone = logs / "dag_id=gone" / "run_id=old" / "task_id=t"
    for folder in (gone, gone.parent, gone.parent.parent):
        make_folder(folder, timedelta(days=2))
    fresh = make_folder(logs / "dag_id=new" / "run_id=new" / "task_id=t", timedelta(0))
    day = logs / "dag_processor" / "2026-01-01" / "agoranomos"
    parsed = make_file(day / "agoranomos_selfcheck.py.log", timedelta(days=40))
    today = make_file(logs / "dag_processor" / "today" / "agoranomos" / "a.py.log", timedelta(0))
    (logs / "dag_processor" / "latest").symlink_to("today")
    result = subprocess.run(
        ["bash", "-c", maintenance.clean_logs_command(str(logs))],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    for path in (old, first, trigger, parsed, gone.parent.parent):
        assert not path.exists(), path.relative_to(logs)
    for path in (young, retried, fresh, today, run / "task_id=again"):
        assert path.exists(), path.relative_to(logs)
    # The folder that the file of the day left empty goes at a later run, a day after.
    assert old.parent.is_dir()
    assert (logs / "dag_processor" / "latest").is_symlink()
    assert (logs / "dag_processor" / "latest" / "agoranomos" / "a.py.log").exists()


def test_the_script_of_the_logs_needs_a_folder(tmp_path):
    # find fails on a folder that is not there, and so does the task, rather than succeed with
    # nothing cleaned.
    command = maintenance.clean_logs_command(str(tmp_path / "missing"))
    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=60)
    assert result.returncode != 0


class StandInCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        self.connection.statements.append((" ".join(query.split()), params))
        if query.startswith("SELECT to_regclass"):
            self.row = (params[0] not in self.connection.missing,)
        else:
            self.rowcount = self.connection.counts[query.split()[2]]

    def fetchone(self):
        return self.row


class StandInConnection:
    def __init__(self, missing, counts):
        self.missing = missing
        self.counts = counts
        self.statements = []
        self.commits = 0
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, kind, *exc):
        # As psycopg2 does: the block commits, or rolls back on an error.
        if kind is None:
            self.commits += 1
        return False

    def cursor(self):
        return StandInCursor(self)

    def close(self):
        self.closed = True


def test_the_cleanup_of_ops_deletes_by_age_as_transformer_table_by_table(monkeypatch):
    monkeypatch.setenv("DBT_ENV_SECRET_WAREHOUSE_PASSWORD", "made-up-password")
    monkeypatch.delenv("WAREHOUSE_HOST", raising=False)
    monkeypatch.delenv("WAREHOUSE_PORT", raising=False)
    connection = StandInConnection(
        missing={"ops.dbt_source_freshness_results"},
        counts={f"ops.{table}": number for number, table in enumerate(maintenance.OPS_TABLES)},
    )
    seen = {}

    def connect(**kwargs):
        seen.update(kwargs)
        return connection

    monkeypatch.setitem(sys.modules, "psycopg2", type(sys)("psycopg2"))
    monkeypatch.setattr(sys.modules["psycopg2"], "connect", connect, raising=False)
    deleted = maintenance.clean_ops()
    assert {key: value for key, value in seen.items() if key != "password"} == {
        "host": "warehouse-db",
        "port": 5432,
        "dbname": "warehouse",
        "user": "transformer",
        "connect_timeout": 10,
        "application_name": "agoranomos_maintenance",
    }
    assert seen["password"] == "made-up-password"
    # Each table in a transaction of its own, the one that does not exist yet passed over.
    present = [table for table in maintenance.OPS_TABLES if table != "dbt_source_freshness_results"]
    assert deleted == {table: connection.counts[f"ops.{table}"] for table in present}
    deletes = [statement for statement in connection.statements if statement[0].startswith("DEL")]
    condition = "WHERE created_at < localtimestamp - make_interval(days => %s)"
    expected = [(f"DELETE FROM ops.{table} {condition}", (90,)) for table in present]  # noqa: S608
    assert deletes == expected
    assert connection.commits == len(maintenance.OPS_TABLES)
    assert connection.closed


def test_ops_keeps_more_than_the_ops_collection_reads():
    # The questions of the Ops collection read the last 30 days (metabase/ops.py).
    import ops

    windows = {
        query.split("interval '")[1].split("'")[0]
        for query in (question["query"] for question in ops.QUESTIONS)
        if "interval '" in query
    }
    assert windows == {"30 days"}
    assert maintenance.OPS_RETENTION_DAYS > 30
    assert maintenance.LOG_RETENTION_DAYS == 30
    assert set(maintenance.OPS_TABLES.values()) == {"created_at"}
    assert not set(maintenance.OPS_TABLES) & set(maintenance.OPS_TABLES_LEFT_OUT)
