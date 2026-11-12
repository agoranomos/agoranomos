"""Tests of the one-shot job marts-init (metabase/marts_init.py, ADR-0039, ADR-0044): what it builds
for each state of the warehouse, the lock that it shares with the transform, the relations of
earlier releases that it drops, and how it fails, with a fake connection and a fake dbt."""

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

    def fetchall(self) -> list[tuple]:
        # Only the query of the relations that are left fetches all.
        assert self.connection.executed[-1][0] == mi.KINDS
        return list(self.connection.kinds)


class Connection:
    def __init__(
        self, rows: list[tuple], fail_on: str = "", kinds: list[tuple] | None = None
    ) -> None:
        self.rows = rows
        self.fail_on = fail_on
        # The relkind of each name of LEFTOVERS, None for one that the warehouse lacks.
        self.kinds = kinds if kinds is not None else [(name, None) for name, _ in mi.LEFTOVERS]
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
        if self.connection is not None:
            self.connection.executed.append(("dbt", ()))
        return subprocess.CompletedProcess(argv, self.returncode)


def run_job(rows, returncode=0, fail_on="", environ=ENVIRON, kinds=None):
    connection = Connection(rows, fail_on, kinds)
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
            ["marts", "core_fx_rates_daily", "ops_rates"],
            {"rebuild_views": True, "recompute_rates": True},
        ),
        (
            False,
            True,
            "build",
            ["+marts", "ops_rates"],
            {"rebuild_views": True, "recompute_rates": True},
        ),
    ],
)
def test_the_job_builds_what_the_state_of_the_warehouse_asks(
    capsys, snapshots, other, verb, nodes, variables
):
    # After a change of the reporting currency, the job reads the days without a rate: none here.
    code, connection, dbt = run_job([(True,), (snapshots, other)] + [(None, 0)] * other)
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


def test_a_new_reporting_currency_without_older_rates_is_a_warning_and_no_failure(capsys):
    # The core cannot give the rates of the days whose loads it accepted in the new currency:
    # ops.ops_rates names them, and the job says so, in a line that ./agoranomos up shows.
    code, connection, _ = run_job([(True,), (True, True), ("2025-05-14", 548)])
    assert code == 0
    assert connection.executed[-1] == (mi.GAPS, ())
    out = capsys.readouterr().out
    assert (
        "marts-init: warning: the core holds no rate into EUR on 548 days from 2025-05-14 on, of "
        "the currencies of the accounts, as their loads of the rates are accepted and have left "
        "the raw layer: the next load of the rates takes their history again, "
        "agoranomos_ecb_daily or with the demo alone agoranomos_demo_daily, and until the "
        "transform reads it, the amounts of those days in EUR are empty (ADR-0042)\n"
    ) in out
    assert out.endswith("marts-init: done\n")
    # A view that cannot be read is a warning too.
    code, _, _ = run_job([(True,), (True, True)], fail_on=mi.GAPS)
    assert code == 0
    assert (
        "marts-init: warning: cannot read ops.ops_rates: RuntimeError: canceling statement due to "
        "lock timeout\n"
    ) in capsys.readouterr().out
    # The same start without a change of the currency reads nothing more.
    _, connection, _ = run_job([(True,), (True, False)])
    assert mi.GAPS not in [sql for sql, _ in connection.executed]


def test_the_view_of_the_rates_has_the_columns_that_the_job_reads():
    view = (DBT / "models" / "ops" / f"{mi.OPS_RATES}.sql").read_text(encoding="utf-8")
    for column in ("first_day_without_rate", "days_without_rate"):
        assert re.search(rf" as {column}\b", view), column
        assert column in mi.GAPS


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
    # Nothing is dropped after a failed build, which may not have built the views of staging again.
    assert mi.KINDS not in [sql for sql, _ in connection.executed]


# The three relations of v0.4 that the upgrade from v0.4.5 left on 11 November 2026, with the kind
# in which v0.4.5 built each (tests/stack/test_upgrade.py). v0.4.0 to v0.4.4 built the same.
OF_V0_4 = (
    ("staging.int_ad_stats_ad__unioned", "TABLE"),
    ("staging.int_ad_stats_ad_group__unioned", "TABLE"),
    ("staging.int_ad_platforms__unioned", "VIEW"),
)


def test_the_relations_left_of_earlier_releases_are_named_one_by_one():
    assert mi.LEFTOVERS == OF_V0_4
    assert [mi.drop_statement(name, kind) for name, kind in mi.LEFTOVERS] == [
        "DROP TABLE IF EXISTS staging.int_ad_stats_ad__unioned",
        "DROP TABLE IF EXISTS staging.int_ad_stats_ad_group__unioned",
        "DROP VIEW IF EXISTS staging.int_ad_platforms__unioned",
    ]
    # A plain schema.name each, never a pattern, and a kind that the catalog tells apart.
    for name, kind in mi.LEFTOVERS:
        assert re.fullmatch(r"[a-z_]+\.[a-z0-9_]+", name), name
        assert kind in mi.RELKINDS, kind
    assert mi.RELKINDS == {"TABLE": "r", "VIEW": "v"}
    assert "CASCADE" not in mi.drop_statement("staging.x", "TABLE").upper()


def test_no_model_seed_or_snapshot_of_the_dbt_project_has_the_name_of_a_leftover():
    # The names under which dbt builds a relation: the file of each model and seed, the block of
    # each snapshot, and each alias that a model sets.
    names = {path.stem for path in (DBT / "models").rglob("*.sql")}
    names |= {path.stem for path in (DBT / "seeds").rglob("*.csv")}
    for path in (DBT / "snapshots").glob("*.sql"):
        names |= set(re.findall(r"\{%\s*snapshot\s+(\w+)\s*%\}", path.read_text(encoding="utf-8")))
    for path in (DBT / "models").rglob("*.sql"):
        names |= set(re.findall(r"\balias\s*=\s*'(\w+)'", path.read_text(encoding="utf-8")))
    assert "dim_campaign_versions" in names
    assert "seed_ad_platforms" in names
    assert "ad_stats_ad_daily" in names  # an alias
    assert [name for name, _ in mi.LEFTOVERS if name.split(".", 1)[1] in names] == []
    # No file of the project names them either, so nothing reads them.
    files = [DBT / "dbt_project.yml"]
    for folder in ("models", "seeds", "snapshots", "macros", "tests"):
        files += sorted(p for p in (DBT / folder).rglob("*") if p.suffix in (".sql", ".yml", ".md"))
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert [n for n, _ in mi.LEFTOVERS if n.split(".", 1)[1] in text] == [], path


def test_the_job_drops_after_dbt_what_is_left_with_its_kind_and_names_each(capsys):
    kinds = [
        ("staging.int_ad_stats_ad__unioned", "r"),
        ("staging.int_ad_stats_ad_group__unioned", None),
        ("staging.int_ad_platforms__unioned", "v"),
    ]
    code, connection, _ = run_job([(True,), (False, False)], kinds=kinds)
    assert code == 0
    statements = [sql for sql, _ in connection.executed]
    after = statements[statements.index("dbt") + 1 :]
    assert after == [
        mi.KINDS,
        "DROP TABLE IF EXISTS staging.int_ad_stats_ad__unioned",
        "DROP VIEW IF EXISTS staging.int_ad_platforms__unioned",
    ]
    assert connection.executed[statements.index(mi.KINDS)][1] == (
        [name for name, _ in mi.LEFTOVERS],
    )
    lines = capsys.readouterr().out.splitlines()
    assert lines[1:] == [
        "marts-init: dropped the table staging.int_ad_stats_ad__unioned of an earlier release, "
        "which no model builds or reads",
        "marts-init: dropped the view staging.int_ad_platforms__unioned of an earlier release, "
        "which no model builds or reads",
        "marts-init: done",
    ]
    assert connection.closed


def test_the_job_says_nothing_of_leftovers_when_there_are_none(capsys):
    code, connection, _ = run_job([(True,), (True, False)])
    assert code == 0
    statements = [sql for sql, _ in connection.executed]
    assert statements[statements.index("dbt") + 1 :] == [mi.KINDS]
    assert capsys.readouterr().out == (
        "marts-init: building the views of marts again\nmarts-init: done\n"
    )


def test_a_relation_of_the_name_and_of_another_kind_stays():
    # A view of the name of a table of v0.4, or a table of the name of its view, is not one of
    # them: a person made it.
    kinds = [
        ("staging.int_ad_stats_ad__unioned", "v"),
        ("staging.int_ad_stats_ad_group__unioned", "m"),
        ("staging.int_ad_platforms__unioned", "r"),
    ]
    code, connection, _ = run_job([(True,), (True, False)], kinds=kinds)
    assert code == 0
    assert not any(sql.startswith("DROP") for sql, _ in connection.executed)


def test_a_drop_that_fails_is_a_warning_and_the_others_go_on(capsys):
    kinds = [(name, mi.RELKINDS[kind]) for name, kind in mi.LEFTOVERS]
    code, connection, _ = run_job(
        [(True,), (True, False)],
        fail_on="DROP TABLE IF EXISTS staging.int_ad_stats_ad_group__unioned",
        kinds=kinds,
    )
    assert code == 0
    assert [sql for sql, _ in connection.executed if sql.startswith("DROP")] == [
        mi.drop_statement(name, kind) for name, kind in mi.LEFTOVERS
    ]
    out = capsys.readouterr().out
    assert (
        "marts-init: warning: cannot drop the table staging.int_ad_stats_ad_group__unioned of an "
        "earlier release, which no model builds or reads, so it stays: RuntimeError: canceling "
        "statement due to lock timeout\n"
    ) in out
    assert "dropped the table staging.int_ad_stats_ad__unioned" in out
    assert "dropped the view staging.int_ad_platforms__unioned" in out
    assert out.endswith("marts-init: done\n")


def test_a_catalog_that_cannot_be_read_after_dbt_is_a_warning(capsys):
    code, connection, _ = run_job([(True,), (True, False)], fail_on="FROM unnest(")
    assert code == 0
    assert connection.closed
    out = capsys.readouterr().out
    assert out.endswith(
        "marts-init: warning: cannot read which relations of earlier releases are left: "
        "RuntimeError: canceling statement due to lock timeout\nmarts-init: done\n"
    )


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
