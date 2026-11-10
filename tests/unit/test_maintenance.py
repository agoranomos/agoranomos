"""The upkeep of the core in the DAG of maintenance (ADR-0039, ADR-0044), without Docker: its
constants against the migrations of the core and the models of ops that it writes into, the SQL of
its cleanups of _dlt_loads and core.loads, which rows of which tables they keep, its choice of the
months to freeze, and its cleanup of the Variables of Cosmos, against names that Cosmos 1.16.0
makes. tests/stack/test_maintenance.py runs the DAG on a stack."""

import ast
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import maintenance

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = REPO_ROOT / "postgres" / "migrations"
OPS_MODELS = REPO_ROOT / "dbt" / "models" / "ops"
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "agoranomos_maintenance.py"


def migration(prefix: str) -> str:
    [path] = MIGRATIONS.glob(f"{prefix}_*.sql")
    return path.read_text(encoding="utf-8")


def partitioned_tables(text: str) -> list[str]:
    """The tables of the core that a migration partitions by range, in their order."""
    return re.findall(
        r"^CREATE TABLE core\.(\w+) \((?:(?!^\);).)*?^\) PARTITION BY RANGE",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )


def test_the_maintenance_knows_every_partitioned_table_of_the_core():
    facts = partitioned_tables(migration("0005"))
    journals = partitioned_tables(migration("0006"))
    assert list(maintenance.FACTS) == facts
    assert list(maintenance.JOURNALS) == journals
    # Every other migration partitions nothing.
    for path in sorted(MIGRATIONS.glob("*.sql")):
        if not path.name.startswith(("0005", "0006")):
            assert "PARTITION BY" not in path.read_text(encoding="utf-8"), path.name


def test_the_months_ahead_are_those_that_ensure_partition_allows():
    # core.ensure_partition refuses a month more than two after the current one, in UTC.
    text = migration("0002")
    assert (
        "v_from > date_trunc('month', now()) + interval "
        f"'{maintenance.MONTHS_AHEAD} months'" in text
    )
    assert "SET TimeZone = 'UTC'" in text


def test_the_freeze_takes_the_months_that_the_trigger_freezes():
    text = migration("0005")
    bounds = set(
        re.findall(r"OLD\.report_date < date_trunc\('month', current_date - (\d+)\)", text)
    )
    assert bounds == {str(maintenance.FROZEN_DAYS)}


def test_the_journals_keep_13_months_as_their_comments_say():
    text = migration("0006")
    for journal in maintenance.JOURNALS:
        comment = re.search(rf"COMMENT ON TABLE core\.{journal} IS\n'([^']*)'", text)[1]
        assert comment.endswith(f"kept {maintenance.JOURNAL_MONTHS} months."), journal
    assert maintenance.LOADS_MONTHS == 13
    assert maintenance.DLT_LOADS_DAYS == 90


@pytest.mark.parametrize(
    ("table", "columns"),
    [
        (maintenance.STEPS, maintenance.STEP_COLUMNS),
        (maintenance.DISK, maintenance.DISK_COLUMNS),
        (maintenance.TOTALS, maintenance.TOTALS_COLUMNS),
    ],
)
def test_the_maintenance_writes_the_columns_of_the_models_of_ops(table, columns):
    # dbt makes each table, empty, with the columns of its model, and adds a column that a later
    # release declares; the maintenance inserts into them by name.
    schema, name = table.split(".")
    assert schema == "ops"
    text = (OPS_MODELS / f"{name}.sql").read_text(encoding="utf-8")
    assert tuple(re.findall(r"^    cast\(null as .+\) as (\w+),?$", text, re.MULTILINE)) == columns
    assert text.rstrip().endswith("where false")
    for setting in (
        "materialized='incremental'",
        "incremental_strategy='append'",
        "full_refresh=false",
        "on_schema_change='append_new_columns'",
    ):
        assert setting in text, (name, setting)
    column, kept = maintenance.OWN_TABLES[table]
    assert column == columns[0]
    assert kept in ("90 days", "13 months")


def test_the_dag_runs_every_step_as_a_task_of_its_own():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    [function] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "agoranomos_maintenance"
    ]
    called = [ast.unparse(statement.value.func) for statement in function.body[1:]]
    tasks = [
        "clean_ops",
        "clean_dlt_loads",
        "clean_core_loads",
        "drop_old_journal_months",
        "clean_cosmos_variables",
        "make_partitions",
        "analyze_the_facts",
        "freeze_old_months",
        "record_the_disk",
        "compare_with_platform_totals",
    ]
    assert called == tasks
    # The doc of the DAG names each task, and each task its function of maintenance.
    doc = ast.get_docstring(tree)
    for name in ["clean_airflow_logs", *tasks]:
        assert f"- {name} " in doc, name
    text = DAG_FILE.read_text(encoding="utf-8")
    for name in tasks:
        assert f'@task(task_id="{name}"' in text, name
        assert f"maintenance.{name}(" in text, name


def test_the_loader_and_transformer_passwords_are_masked():
    calls = [ast.unparse(node) for node in ast.walk(ast.parse(DAG_FILE.read_text("utf-8")))]
    assert "mask_secret(maintenance.transformer_credentials()['password'])" in calls
    assert "mask_secret(maintenance.loader_credentials()['password'])" in calls


def test_the_cleanup_of_dlt_loads_keeps_what_a_load_dlt_and_the_pin_read():
    query = maintenance.dlt_loads_delete("raw_demo_search", has_state=True, has_journal=True)
    flat = " ".join(query.split())
    # Old rows only, by the time of the row.
    assert 'DELETE FROM "raw_demo_search"._dlt_loads AS d WHERE d.inserted_at < now() - ' in flat
    assert "make_interval(days => %(days)s)" in flat
    # The newest whole load, for loads.has_loads().
    assert (
        '(SELECT load_id FROM "raw_demo_search"._dlt_loads WHERE status = 0'
        " ORDER BY load_id DESC LIMIT 1)" in flat
    )
    # The newest state of each pipeline, as dlt 1.32.0 restores it: joined with the whole loads,
    # the largest load_id as text.
    assert (
        "SELECT DISTINCT ON (s.pipeline_name) l.load_id FROM"
        ' "raw_demo_search"._dlt_pipeline_state AS s JOIN "raw_demo_search"._dlt_loads AS l'
        " ON l.load_id = s._dlt_load_id AND l.status = 0 ORDER BY s.pipeline_name, l.load_id DESC"
        in flat
    )
    # The newest day and the last day loaded of each account, as loads.recorded() reads them.
    assert "max(as_of) AS as_of, max(window_to) AS window_to" in flat
    assert "(w.as_of = b.as_of OR w.window_to = b.window_to)" in flat
    assert "WHERE j.status = 'loaded'" in flat
    # The loads that no run has pinned, by source, load and account.
    assert (
        "WHERE NOT EXISTS (SELECT FROM core.loads AS p WHERE p.source = 'demo_search'"
        " AND p.load_id = j._dlt_load_id AND p.account_id = j.account_id)" in flat
    )
    assert "NOT EXISTS (SELECT FROM kept WHERE kept.load_id = d.load_id)" in flat


def test_the_cleanup_of_dlt_loads_of_a_schema_without_a_journal_keeps_the_newest_and_the_state():
    query = maintenance.dlt_loads_delete("raw_settings", has_state=True, has_journal=False)
    assert "_agoranomos_loads" not in query
    assert "core.loads" not in query
    assert "_dlt_pipeline_state" in query
    query = maintenance.dlt_loads_delete("raw_settings", has_state=False, has_journal=False)
    assert query.count("UNION") == 0
    assert "status = 0 ORDER BY load_id DESC LIMIT 1" in query


@pytest.mark.parametrize("schema", ["raw_x'; drop table y; --", "public", 'raw_"x"', "raw_X"])
def test_the_cleanup_of_dlt_loads_takes_only_the_names_of_raw_schemas(schema):
    with pytest.raises(ValueError, match="is not the name of a raw schema"):
        maintenance.dlt_loads_delete(schema, has_state=True, has_journal=True)


def test_the_cleanup_of_core_loads_takes_old_accepted_rows_that_nothing_reads():
    query = " ".join(maintenance.core_loads_delete('"raw_demo_search"._dlt_loads').split())
    assert query.startswith("DELETE FROM core.loads AS l WHERE l.source = %(source)s")
    # Never a pinned row: no run has accepted it, and staging reads it.
    assert "AND l.status = 'accepted'" in query
    assert "l.finished_at < now() - make_interval(months => %(months)s)" in query
    # Not the newest accepted load of its account with a window.
    assert (
        "AND EXISTS (SELECT FROM core.loads AS n WHERE n.source = l.source AND n.account_id ="
        " l.account_id AND n.status = 'accepted' AND n.outcome = 'loaded'"
        " AND n.load_id::numeric > l.load_id::numeric)" in query
    )
    # Not one newer than the oldest pinned load of its account, which staging still reads.
    assert (
        "AND NOT EXISTS (SELECT FROM core.loads AS p WHERE p.source = l.source AND p.account_id ="
        " l.account_id AND p.status = 'pinned' AND p.outcome = 'loaded'"
        " AND p.load_id::numeric < l.load_id::numeric)" in query
    )
    # Not one whose row of _dlt_loads raw still holds, which the next pin would take again.
    assert query.endswith(
        'AND NOT EXISTS (SELECT FROM "raw_demo_search"._dlt_loads AS d WHERE d.load_id = l.load_id)'
    )
    assert "_dlt_loads" not in maintenance.core_loads_delete(None)


def stats(**changed):
    found = {
        "last_vacuum": datetime(2026, 11, 1, tzinfo=UTC),
        "last_autovacuum": None,
        "last_analyze": None,
        "inserted": 0,
        "dead": 0,
    }
    return {**found, **changed}


@pytest.mark.parametrize(
    ("found", "expected"),
    [
        (stats(), False),
        (stats(last_vacuum=None), True),
        (stats(inserted=3), True),
        (stats(dead=1), True),
        (stats(last_autovacuum=datetime(2026, 11, 2, tzinfo=UTC)), True),
        (stats(last_autovacuum=datetime(2026, 10, 2, tzinfo=UTC)), False),
    ],
)
def test_a_frozen_month_is_frozen_again_only_after_a_change(found, expected):
    assert maintenance.needs_freeze(found) is expected


def cosmos_key(dag_id: str, task_group_id: str | None, run_id: str) -> str:
    """The name of the Variable of the backup of a producer, as Cosmos 1.16.0 makes it
    (cosmos/operators/_watcher/xcom.py, _xcom_backup_variable_key)."""

    def clean(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_-]", "_", value)

    parts = ["cosmos_xcom_backup", clean(dag_id.replace(".", "___"))]
    if task_group_id:
        parts.append(clean(task_group_id.replace(".", "__")))
    parts.append(clean(run_id.replace(".", "_")))
    return "__".join(parts)


@pytest.mark.parametrize(
    ("run_id", "moment"),
    [
        ("scheduled__2026-05-04T10:15:00+00:00", datetime(2026, 5, 4, 10, 15, tzinfo=UTC)),
        (
            "asset_triggered__2026-11-10T06:47:12.345678+00:00",
            datetime(2026, 11, 10, 6, 47, 12, tzinfo=UTC),
        ),
        ("manual__2026-01-02T03:04:05.6+00:00_aBcD1234", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("my own run", None),
    ],
)
def test_the_moment_of_a_variable_of_cosmos_is_that_of_its_run(run_id, moment):
    # The ID of the run comes last, so its moment wins over one in the ID of the DAG.
    key = cosmos_key("dag_of_2025-01-01T00:00:00", "dbt", run_id)
    assert key.startswith(maintenance.VARIABLE_PREFIX)
    assert maintenance.variable_moment(key) == moment


def test_the_variables_of_cosmos_of_runs_older_than_90_days_go():
    now = datetime(2026, 11, 10, 4, 23, tzinfo=UTC)
    old = cosmos_key("a_dag", "dbt", f"asset_triggered__{(now - timedelta(days=91)).isoformat()}")
    young = cosmos_key("a_dag", None, f"manual__{(now - timedelta(days=89)).isoformat()}")
    undated = cosmos_key("a_dag", None, "a run of my own")
    other = "cosmos_seed_checksum__a_dag"
    deleted = []
    gone, kept = maintenance.clean_cosmos_variables(
        [old, young, undated, other], deleted.append, now
    )
    assert gone == deleted == [old]
    assert kept == [undated]


def test_the_steps_on_the_core_connect_as_transformer_and_the_cleanup_of_raw_as_loader():
    source = (REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines" / "maintenance.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    functions = {
        node.name: ast.unparse(node) for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    assert "loader_credentials()" in functions["clean_dlt_loads"]
    for name in (
        "make_partitions",
        "analyze_the_facts",
        "freeze_old_months",
        "drop_old_journal_months",
        "clean_core_loads",
        "record_the_disk",
        "compare_with_platform_totals",
    ):
        assert "transformer_credentials()" in functions[name], name
        assert "loader_credentials()" not in functions[name], name
    # The partitions come and go through the functions of migrator only, and the tables of ops
    # through dbt.
    for statement in ("DROP TABLE", "CREATE TABLE", "ALTER TABLE", "GRANT", "REVOKE"):
        assert statement not in source, statement
    assert "core.ensure_partition(%s::regclass, %s)" in source
    assert "core.drop_partition(%s::regclass, %s)" in source
