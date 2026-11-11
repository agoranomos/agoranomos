"""Tests of the one-shot job marts-init (metabase/marts_init.py, ADR-0039, ADR-0044): what it builds
for each state of the warehouse, the lock that it shares with the transform, and how it fails,
with a fake connection and a fake dbt."""

import json
import re
import subprocess
from pathlib import Path

import marts_init as mi
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT = REPO_ROOT / "dbt"
COMPOSE = REPO_ROOT / "compose.yaml"
ENVIRON = {mi.PASSWORD: "made-up-password", mi.CURRENCY: "EUR"}


class Cursor:
    """A cursor that records what it runs and answers from a list of rows."""

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, parameters: tuple = ()) -> None:
        self.connection.executed.append((sql, parameters))
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("canceling statement due to lock timeout\nCONTEXT: more")

    def fetchone(self) -> tuple:
        return self.connection.rows.pop(0)


class Connection:
    def __init__(self, rows: list[tuple], fail_on: str = "") -> None:
        self.rows = rows
        self.fail_on = fail_on
        self.executed: list[tuple[str, tuple]] = []
        self.autocommit = False
        self.closed = False

    def cursor(self) -> Cursor:
        return Cursor(self)

    def close(self) -> None:
        self.closed = True


class Dbt:
    """A fake subprocess.run of dbt: it records the call and the connection that holds the lock."""

    def __init__(self, connection: Connection | None, returncode: int = 0) -> None:
        self.connection = connection
        self.returncode = returncode
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str], **options: object) -> subprocess.CompletedProcess:
        # dbt runs while the job holds the lock, on its open connection.
        assert self.connection is None or not self.connection.closed
        assert options == {"timeout": mi.DBT_TIMEOUT, "check": False}
        self.calls.append(argv)
        return subprocess.CompletedProcess(argv, self.returncode)


def run_job(rows, returncode=0, fail_on="", environ=ENVIRON):
    connection = Connection(rows, fail_on)
    dbt = Dbt(connection, returncode)
    code = mi.main([], environ, lambda env: connection, dbt)
    return code, connection, dbt


def selected(argv: list[str]) -> tuple[str, list[str], dict]:
    """The command of dbt, what it selects and its vars."""
    start = argv.index("--select") + 1
    end = argv.index("--vars")
    return argv[2], argv[start:end], json.loads(argv[end + 1])


@pytest.mark.parametrize(
    ("snapshots", "other", "verb", "nodes", "variables"),
    [
        (True, False, "run", ["marts"], {"rebuild_views": True}),
        (False, False, "build", ["+marts"], {"rebuild_views": True}),
        (
            True,
            True,
            "run",
            ["marts", "core_fx_rates_daily"],
            {"rebuild_views": True, "recompute_rates": True},
        ),
        (False, True, "build", ["+marts"], {"rebuild_views": True, "recompute_rates": True}),
    ],
)
def test_the_job_builds_what_the_state_of_the_warehouse_asks(
    capsys, snapshots, other, verb, nodes, variables
):
    code, connection, dbt = run_job([(True,), (snapshots, other)])
    assert code == 0
    [argv] = dbt.calls
    assert argv[0] == mi.DBT
    assert selected(argv) == (verb, nodes, variables)
    # The project is read-only: dbt writes its target and logs on the tmpfs.
    assert argv[argv.index("--target-path") + 1] == f"{mi.WORK}/target"
    assert argv[argv.index("--log-path") + 1] == f"{mi.WORK}/logs"
    assert argv[argv.index("--project-dir") + 1] == mi.PROJECT
    assert argv[argv.index("--profiles-dir") + 1] == mi.PROJECT
    # The lock first, then the state, then dbt, and the lock goes with the connection.
    assert connection.autocommit is True
    assert connection.executed[0] == ("SELECT pg_try_advisory_lock(%s)", (mi.LOCK,))
    assert connection.executed[1] == (mi.STATE, (list(mi.SNAPSHOTS), len(mi.SNAPSHOTS), "EUR"))
    assert connection.closed
    out = capsys.readouterr().out
    assert out.endswith("marts-init: done\n")
    assert ("in EUR, the new reporting currency" in out) == other


def test_the_job_waits_for_the_nodes_of_the_transform_that_write(capsys):
    code, connection, dbt = run_job([(False,), (True, False)])
    assert code == 0
    assert [sql for sql, _ in connection.executed[:4]] == [
        "SELECT pg_try_advisory_lock(%s)",
        "SET lock_timeout = '11min'",
        "SELECT pg_advisory_lock(%s)",
        "RESET lock_timeout",
    ]
    assert "waiting for the run of the transform that writes now" in capsys.readouterr().out
    assert len(dbt.calls) == 1


def test_a_lock_that_does_not_come_fails_the_job_before_dbt(capsys):
    code, connection, dbt = run_job([(False,)], fail_on="pg_advisory_lock(")
    assert code == 1
    assert dbt.calls == []
    assert connection.closed
    err = capsys.readouterr().err
    assert err == (
        "marts-init: cannot take the lock of the views or read the core (see ./agoranomos logs "
        "marts-init): RuntimeError: canceling statement due to lock timeout\n"
    )


def test_a_failed_dbt_fails_the_job_with_its_exit_code(capsys):
    code, connection, _ = run_job([(True,), (True, False)], returncode=2)
    assert code == 2
    assert connection.closed
    assert capsys.readouterr().err == "marts-init: dbt exited with 2: the lines above say why\n"


def test_a_dbt_that_hangs_or_is_missing_fails_the_job(capsys):
    connection = Connection([(True,), (True, False)])

    def hangs(argv, **options):
        raise subprocess.TimeoutExpired(argv, mi.DBT_TIMEOUT)

    assert mi.main([], ENVIRON, lambda env: connection, hangs) == 1
    assert "dbt did not end within 1800 seconds" in capsys.readouterr().err
    connection = Connection([(True,), (True, False)])

    def missing(argv, **options):
        raise FileNotFoundError(argv[0])

    assert mi.main([], ENVIRON, lambda env: connection, missing) == 1
    assert capsys.readouterr().err == (
        f"marts-init: cannot run dbt ({mi.DBT}): FileNotFoundError\n"
    )


def test_a_warehouse_out_of_reach_or_no_password_fails_the_job_in_one_line(capsys):
    def refuses(environ):
        raise RuntimeError('connection to server at "warehouse-db" failed\nIs the server running?')

    assert mi.main([], ENVIRON, refuses, Dbt(None)) == 1
    assert capsys.readouterr().err == (
        "marts-init: cannot connect to the warehouse as transformer: RuntimeError: connection to "
        'server at "warehouse-db" failed\n'
    )
    assert mi.main([], {}, mi.connect_to_warehouse, Dbt(None)) == 1
    assert capsys.readouterr().err == (
        "marts-init: DBT_ENV_SECRET_WAREHOUSE_PASSWORD is empty: run ./agoranomos init\n"
    )
    assert mi.main(["extra"], ENVIRON, refuses, Dbt(None)) == 2


def test_the_reporting_currency_is_usd_by_default():
    _, connection, _ = run_job([(True,), (True, False)], environ={mi.PASSWORD: "x"})
    assert connection.executed[1][1][2] == "USD"


def test_the_key_of_the_lock_is_that_of_the_dbt_project():
    macro = (DBT / "macros" / "marts_init.sql").read_text(encoding="utf-8")
    [key] = re.findall(r"return\((\d+)\)", macro)
    assert int(key) == mi.LOCK
    # Each node of a run of the transform that writes takes it, shared, in its transaction; dbt by
    # hand, and marts-init, which set no DBT_JOB_RUN_ID, do not.
    assert "pg_advisory_xact_lock_shared({{ marts_init_lock() }})" in macro
    assert "env_var('DBT_JOB_RUN_ID', '') != ''" in macro
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    assert project.count('+pre-hook: "{{ wait_for_marts_init() }}"') == 3
    for section in ("models:\n  agoranomos:", "seeds:\n  agoranomos:", "snapshots:\n  agoranomos:"):
        block = project.split(section, 1)[1].split("\n\n", 1)[0]
        assert "wait_for_marts_init" in block, section
    # The pin holds it, shared, through all of its transactions, and lets go at the end.
    pin = (DBT / "macros" / "pin_loads.sql").read_text(encoding="utf-8")
    assert pin.count("pg_advisory_lock_shared({{ marts_init_lock() }})") == 1
    assert pin.count("pg_advisory_unlock_shared({{ marts_init_lock() }})") == 1


def test_the_snapshots_are_those_that_the_views_of_marts_read():
    # A build of +marts builds these and no other snapshot, so the next start finds them.
    snapshots = {path.stem for path in (DBT / "snapshots").glob("*.sql")}
    read = {
        name
        for path in (DBT / "models" / "marts").glob("*.sql")
        for name in re.findall(r"ref\('(\w+)'\)", path.read_text(encoding="utf-8"))
        if name in snapshots
    }
    assert tuple(sorted(read)) == mi.SNAPSHOTS


def test_the_warehouse_is_the_one_of_the_profile_of_dbt():
    profile = (DBT / "profiles.yml").read_text(encoding="utf-8")
    assert f"host: {mi.HOST}\n" in profile
    assert f"port: {mi.PORT}\n" in profile
    assert f"user: {mi.USER}\n" in profile
    assert f"dbname: {mi.DATABASE}\n" in profile
    assert f"env_var('{mi.PASSWORD}', '')" in profile


def test_the_service_runs_the_job_with_the_python_of_dbt():
    text = COMPOSE.read_text(encoding="utf-8")
    block = text.split("\n  marts-init:\n", 1)[1].split("\n\n", 1)[0]
    assert (
        'entrypoint: ["/opt/airflow/dbt_venv/bin/python", "/opt/agoranomos/metabase/marts_init.py"]'
        in block
    )
    assert "DBT_JOB_RUN_ID" not in block
    assert f"{mi.PASSWORD}: ${{WAREHOUSE_TRANSFORMER_PASSWORD:?run ./agoranomos init}}" in block
    # metabase-init waits for the views of the release.
    init = text.split("\n  metabase-init:\n", 1)[1].split("\n\n", 1)[0]
    assert "      marts-init:\n        condition: service_completed_successfully" in init
