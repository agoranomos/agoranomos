"""The retention of the service data (ADR-0044) on a running stack: airflow-init deletes the rows of
the database of Airflow older than 90 days on every start, and a failure of that cleanup does not
stop the start; the DAG agoranomos_maintenance deletes the logs of Airflow older than 30 days and
the rows of the runs of dbt in ops older than 90 days; Metabase keeps the log of its queries for 90
days.

The history is accrued by the tests themselves: they age a run of the self-check, the run of the
demo history and rows and files of their own, all named with a tag of the test, and put back or
remove at the end what they aged or made. They run airflow-init twice, first with a trigger of
their own that refuses the deletion of one aged row of the log of Airflow, which makes the cleanup
fail with an error of integrity, as db clean swallows the operational errors itself, and then
without it. No migration and no table of the stack changes.

The assertions print names, counts and statuses, never a secret: of the output of a job, the
fixtures keep the lines that its cleanup prints, which name tables and errors.
"""

import json
import secrets
from types import SimpleNamespace

import pytest
from agoranomos_pipelines import maintenance
from airflow_api import dag_runs, is_paused, run_dag, wait_for_dag, wait_for_run
from metabase_content import run_job
from services import DEMO
from stack_helpers import compose, psql, warehouse_output
from stack_helpers import secrets as env_secrets

SELFCHECK = "agoranomos_selfcheck"
HISTORY = "agoranomos_demo_history"
MAINTENANCE = "agoranomos_maintenance"
LOGS = maintenance.LOGS
# Older than the 90 days of the database of Airflow and of ops, and younger.
AGED = "100 days"
# What airflow-init prints for a call of db clean that failed, before the tables of the call.
FAILED = "airflow-init: the database of Airflow keeps its old rows of "
# The tables of the second call of db clean in airflow-init, which has no filter by DAG.
UNFILTERED = "asset_event,deadline,job,log,task_reschedule"
# The column of each table of elementary in ops that names a row, for the rows of the tests.
OPS_KEYS = {
    "dbt_invocations": "invocation_id",
    "dbt_run_results": "model_execution_id",
    "dbt_source_freshness_results": "source_freshness_execution_id",
    "elementary_test_results": "id",
    "test_result_rows": "elementary_test_results_id",
}


def airflow_sql(env, sql):
    """The output of SQL in the database of Airflow as its owner, the role airflow, which must
    succeed. The SQL holds no secret, so a failure may print the error of psql."""
    result = psql("platform-db", "airflow", env["AIRFLOW_DB_PASSWORD"], "airflow", sql)
    assert result.returncode == 0, f"psql exited with {result.returncode}: {result.stderr.strip()}"
    return result.stdout.strip()


def quoted(text):
    return "'" + text.replace("'", "''") + "'"


def shift(dag_id, run_id, interval):
    """SQL that moves a run of a DAG and its task instances and XComs in time by an interval, such
    as '- interval ...': each column by which db clean finds old rows, and those beside it."""
    where = f"WHERE dag_id = {quoted(dag_id)} AND run_id = {quoted(run_id)}"
    return (
        f"UPDATE dag_run SET start_date = start_date {interval}, end_date = end_date {interval}, "  # noqa: S608 (constants and IDs of Airflow, quoted)
        f"queued_at = queued_at {interval}, run_after = run_after {interval} {where}; "
        f"UPDATE task_instance SET start_date = start_date {interval}, "
        f"end_date = end_date {interval}, queued_dttm = queued_dttm {interval}, "
        f"scheduled_dttm = scheduled_dttm {interval} {where}; "
        f'UPDATE xcom SET "timestamp" = "timestamp" {interval} {where};'
    )


def run_airflow_init():
    """airflow-init once more, as every start runs it: its exit code, the lines in which it says
    that a call of db clean failed, and the warnings of db clean about the tables that it did not
    clean. The rest of the output stays in this frame."""
    result = compose("run", "--rm", "--no-deps", "-T", "airflow-init", check=False)
    lines = (result.stdout + result.stderr).splitlines()
    return SimpleNamespace(
        code=result.returncode,
        failed=[line for line in lines if line.startswith(FAILED)],
        not_cleaned=[line for line in lines if "were not cleaned due to errors" in line],
    )


def count(env, sql):
    return int(airflow_sql(env, f"SELECT count(*) FROM {sql}"))  # noqa: S608 (SQL of this file)


@pytest.fixture(scope="module")
def cleanups(env, airflow_url, token):
    """Two starts of airflow-init on an accrued history: the first with a cleanup that fails, the
    second without. What the tests check is recorded in between and after."""
    tag = f"b22_{secrets.token_hex(4)}"
    wait_for_dag(airflow_url, token, SELFCHECK)
    old_run = run_dag(airflow_url, token, SELFCHECK)
    new_run = run_dag(airflow_url, token, SELFCHECK)
    assert (old_run["state"], new_run["state"]) == ("success", "success")
    history = None
    if DEMO:
        succeeded = [
            run for run in dag_runs(airflow_url, token, HISTORY) if run["state"] == "success"
        ]
        assert succeeded, f"{HISTORY} has no successful run"
        history = succeeded[0]["dag_run_id"]
    refuse = f"{tag}_refuse"
    probe_dag = f"{tag}_dag"
    pauses = "SELECT dag_id || '|' || is_paused FROM dag WHERE dag_id NOT LIKE 'b22\\_%' ORDER BY 1"
    try:
        airflow_sql(
            env,
            shift(SELFCHECK, old_run["dag_run_id"], f"- interval '{AGED}'")  # noqa: S608 (constants, IDs of Airflow and a tag of the test)
            + f" UPDATE log SET dttm = dttm - interval '{AGED}' WHERE dag_id = {quoted(SELFCHECK)}"  # noqa: S608 (the same)
            f" AND run_id = {quoted(old_run['dag_run_id'])};"
            # Rows of no DAG, which a filter by DAG never selects, and an asset event.
            f" INSERT INTO log (dttm, event) VALUES (now() - interval '{AGED}', '{tag}_old'),"
            f" (now() - interval '89 days', '{tag}_kept'),"
            f" (now() - interval '{AGED}', '{tag}_refused');"
            f' INSERT INTO asset_event (asset_id, extra, "timestamp")'
            f" SELECT min(id), json_build_object('probe', '{tag}_old'), now() - interval '{AGED}'"
            f" FROM asset UNION ALL"
            f" SELECT min(id), json_build_object('probe', '{tag}_kept'), now() - interval '89 days'"
            f" FROM asset;"
            # A DAG that the DAG processor has not parsed for 100 days, unpaused, as every DAG
            # looks to airflow-init after a stop of that long.
            f" CREATE TEMPORARY TABLE probe AS SELECT * FROM dag WHERE dag_id = '{SELFCHECK}';"
            f" UPDATE probe SET dag_id = '{probe_dag}', is_paused = false,"
            f" last_parsed_time = now() - interval '{AGED}';"
            " INSERT INTO dag SELECT * FROM probe;"
            + (shift(HISTORY, history, f"- interval '{AGED}'") if history else ""),
        )
        before = airflow_sql(env, pauses)
        airflow_sql(
            env,
            f"CREATE FUNCTION {refuse}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
            " RAISE EXCEPTION 'a stack test refuses the delete' USING ERRCODE = '23000';"
            f" END $$; CREATE TRIGGER {refuse} BEFORE DELETE ON log FOR EACH ROW"
            f" WHEN (OLD.event = '{tag}_refused') EXECUTE FUNCTION {refuse}();",
        )
        try:
            failed = run_airflow_init()
            refused_after_failed = count(env, f"log WHERE event = '{tag}_refused'")
        finally:
            airflow_sql(
                env, f"DROP TRIGGER IF EXISTS {refuse} ON log; DROP FUNCTION IF EXISTS {refuse}();"
            )
        cleaned = run_airflow_init()
        yield SimpleNamespace(
            tag=tag,
            old_run=old_run["dag_run_id"],
            new_run=new_run["dag_run_id"],
            history=history,
            probe_dag=probe_dag,
            failed=failed,
            refused_after_failed=refused_after_failed,
            cleaned=cleaned,
            pauses_before=before,
            pauses_after=airflow_sql(env, pauses),
        )
    finally:
        airflow_sql(
            env,
            f"DELETE FROM log WHERE event LIKE '{tag}\\_%';"  # noqa: S608 (a tag of the test)
            f" DELETE FROM asset_event WHERE extra->>'probe' LIKE '{tag}\\_%';"
            f" DELETE FROM dag WHERE dag_id = '{probe_dag}';"
            + (shift(HISTORY, history, f"+ interval '{AGED}'") if history else ""),
        )


def test_a_failed_cleanup_says_so_in_one_line_and_the_start_goes_on(cleanups):
    # The trigger fails the call of the tables without a filter by DAG, which takes log, with an
    # error of integrity, which db clean does not swallow: the job still exits 0, so ./agoranomos
    # up goes on, and the refused row waits for the next start.
    assert cleanups.failed.code == 0
    [line] = cleanups.failed.failed
    assert line.startswith(
        f"{FAILED}{UNFILTERED} until the next start: the cleanup failed with IntegrityError: "
    ), line
    assert "a stack test refuses the delete" in line, line
    assert cleanups.refused_after_failed == 1


def test_the_next_start_cleans_what_a_failed_one_left(env, cleanups):
    assert cleanups.cleaned.code == 0
    assert cleanups.cleaned.failed == []
    assert count(env, f"log WHERE event = '{cleanups.tag}_refused'") == 0


def test_db_clean_misses_no_table_but_deadline_in_its_first_call(cleanups):
    # Airflow 3.3.2 filters deadline by a column dag_id that it lacks, and dag_run brings it into
    # the call that keeps the rows of the demo history: that call warns, and the next cleans it.
    # A release of Airflow that fixes this fails the test, and the comment of compose.yaml goes.
    [line] = cleanups.cleaned.not_cleaned
    assert "['deadline']" in line, line


def test_the_rows_older_than_90_days_go_and_the_younger_stay(env, cleanups):
    tag = cleanups.tag
    for table in ("dag_run", "task_instance", "xcom"):
        where = f"WHERE dag_id = '{SELFCHECK}' AND run_id = {quoted(cleanups.old_run)}"
        assert count(env, f"{table} {where}") == 0, table
        where = f"WHERE dag_id = '{SELFCHECK}' AND run_id = {quoted(cleanups.new_run)}"
        assert count(env, f"{table} {where}") >= 1, table
    assert count(env, f"log WHERE event = '{tag}_old'") == 0
    assert count(env, f"log WHERE event = '{tag}_kept'") == 1
    assert count(env, f"asset_event WHERE extra->>'probe' = '{tag}_old'") == 0
    assert count(env, f"asset_event WHERE extra->>'probe' = '{tag}_kept'") == 1


def test_the_cleanup_leaves_no_table_of_archive(env, cleanups):
    assert count(env, "pg_tables WHERE tablename LIKE '\\_airflow\\_deleted\\_\\_%'") == 0


def test_a_dag_unparsed_for_100_days_keeps_its_row_and_its_pause(env, cleanups):
    # The row of dag that a cleanup of that table would delete, as it would every row after a stop
    # of more than 90 days: the DAG would come back paused at its next parse.
    probe = airflow_sql(env, f"SELECT is_paused FROM dag WHERE dag_id = '{cleanups.probe_dag}'")  # noqa: S608 (a tag of the test)
    assert probe == "f"
    assert cleanups.pauses_after == cleanups.pauses_before


@pytest.mark.demo
def test_the_manual_run_of_the_demo_history_stays_and_demo_init_finds_it(env, cleanups):
    # db clean keeps the last run of a DAG unless it is manual, and that of the history is.
    where = f"WHERE dag_id = '{HISTORY}' AND run_id = {quoted(cleanups.history)}"
    assert count(env, f"dag_run {where}") == 1
    assert count(env, f"task_instance {where}") > 0
    code, _, found, last = run_job("demo-init", env_secrets(env))
    assert not found, f"the output of demo-init holds the values of {found}"
    assert code == 0, f"demo-init exited with {code}"
    assert "ran before" in last, "demo-init ran the demo pipeline again"


def test_metabase_keeps_the_log_of_its_queries_90_days():
    # Metabase 0.64.1 does not show the setting in its API.
    result = compose(
        "exec", "-T", "metabase", "sh", "-c", 'echo "$MB_AUDIT_MAX_RETENTION_DAYS"', check=False
    )
    assert (result.returncode, result.stdout.strip()) == (0, "90")


def in_logs(script):
    """Run a shell script in the volume of the logs of Airflow, as the user of the tasks, in the
    scheduler, which mounts it in both variants: its exit code and output."""
    result = compose("exec", "-T", "airflow-scheduler", "sh", "-c", script, check=False)
    return result.returncode, result.stdout.strip()


def ops_tables_exist(env):
    return warehouse_output(env, "SELECT to_regclass('ops.dbt_run_results') IS NOT NULL") == "t"


@pytest.fixture(scope="module")
def maintenance_run(env, airflow_url, token):
    """A run of the DAG of maintenance on files and rows of ops of the test, some old and some
    young; the files and the rows go at the end."""
    tag = f"b22_{secrets.token_hex(4)}"
    probe = f"{LOGS}/dag_id={tag}"
    code, _ = in_logs(
        "set -eu; "
        f"p='{probe}'; "
        'mkdir -p "$p/run_id=old/task_id=t" "$p/run_id=young/task_id=t" '
        '"$p/run_id=empty/task_id=t" "$p/run_id=fresh/task_id=t"; '
        'echo old > "$p/run_id=old/task_id=t/attempt=1.log"; '
        'echo young > "$p/run_id=young/task_id=t/attempt=1.log"; '
        "touch -d '31 days ago' \"$p/run_id=old/task_id=t/attempt=1.log\"; "
        "touch -d '29 days ago' \"$p/run_id=young/task_id=t/attempt=1.log\"; "
        'touch -d \'2 days ago\' "$p/run_id=empty/task_id=t" "$p/run_id=empty"'
    )
    assert code == 0, "the files of the test could not be made in the volume of the logs"
    rows = ops_tables_exist(env)
    try:
        if rows:
            warehouse_output(
                env,
                " ".join(
                    f"INSERT INTO ops.{table} ({key}, created_at) VALUES"  # noqa: S608 (constants and a tag of the test)
                    f" ('{tag}_old', localtimestamp - interval '91 days'),"
                    f" ('{tag}_kept', localtimestamp - interval '89 days');"
                    for table, key in OPS_KEYS.items()
                ),
            )
        wait_for_dag(airflow_url, token, MAINTENANCE)
        run = run_dag(airflow_url, token, MAINTENANCE, timeout=900)
        yield SimpleNamespace(tag=tag, probe=probe, run=run, rows=rows)
    finally:
        in_logs(f"rm -rf '{probe}'")
        if rows:
            warehouse_output(
                env,
                " ".join(
                    f"DELETE FROM ops.{table} WHERE {key} LIKE '{tag}\\_%';"  # noqa: S608 (constants and a tag of the test)
                    for table, key in OPS_KEYS.items()
                ),
            )


def test_the_maintenance_is_unpaused_and_runs_by_itself(airflow_url, token):
    # Unlike the other DAGs of the base package: the run that Airflow makes at once, for the last
    # time of its schedule, when it first sees the DAG unpaused.
    wait_for_dag(airflow_url, token, MAINTENANCE)
    assert is_paused(airflow_url, token, MAINTENANCE) is False
    runs = dag_runs(airflow_url, token, MAINTENANCE, limit=100)
    scheduled = [run for run in runs if run["run_type"] == "scheduled"]
    assert scheduled, f"{MAINTENANCE} has no scheduled run"
    run = wait_for_run(airflow_url, token, MAINTENANCE, scheduled[0]["dag_run_id"], timeout=900)
    assert run["state"] == "success"


def test_the_maintenance_succeeds(maintenance_run):
    assert maintenance_run.run["state"] == "success"


def test_a_log_older_than_30_days_goes_and_a_younger_one_stays(maintenance_run):
    probe = maintenance_run.probe
    checks = {
        "the log of 31 days is gone": f"! test -e '{probe}/run_id=old/task_id=t/attempt=1.log'",
        "the log of 29 days stays": f"test -f '{probe}/run_id=young/task_id=t/attempt=1.log'",
        # The deletion of its file gave the folder a new time: it goes a day later.
        "the folder that the old log left stays": f"test -d '{probe}/run_id=old/task_id=t'",
        "the empty folders of two days are gone": f"! test -e '{probe}/run_id=empty'",
        "the empty folder of now stays": f"test -d '{probe}/run_id=fresh/task_id=t'",
        "the link of the DAG processor stands": (
            f"test -L '{LOGS}/dag_processor/latest' && test -d '{LOGS}/dag_processor/latest/'"
        ),
    }
    failed = [name for name, script in checks.items() if in_logs(script)[0] != 0]
    assert failed == []


def test_the_rows_of_ops_older_than_90_days_go_and_younger_ones_stay(env, maintenance_run):
    if not maintenance_run.rows:
        pytest.skip("ops has no tables of elementary yet: no run of the transform has built them")
    tag = maintenance_run.tag
    left = {
        table: warehouse_output(
            env,
            f"SELECT string_agg({key}, ',' ORDER BY {key}) FROM ops.{table}"  # noqa: S608 (constants and a tag of the test)
            f" WHERE {key} LIKE '{tag}\\_%'",
        )
        for table, key in OPS_KEYS.items()
    }
    assert left == {table: f"{tag}_kept" for table in OPS_KEYS}


def test_the_cleanup_of_ops_covers_every_table_of_elementary_that_grows():
    # elementary declares the column of the age of a row in the config of each table that grows
    # with the runs (meta.timestamp_column): each of them is cleaned by that column, or left out on
    # purpose. A newer elementary with another such table fails this test.
    result = compose(
        "exec", "-T", "airflow-scheduler", "cat", "/opt/agoranomos/manifest/manifest.json"
    )
    nodes = json.loads(result.stdout)["nodes"].values()
    growing = {
        node["name"]: (node["config"].get("meta") or node.get("meta") or {})["timestamp_column"]
        for node in nodes
        if node["package_name"] == "elementary"
        and node["resource_type"] == "model"
        and "timestamp_column" in (node["config"].get("meta") or node.get("meta") or {})
    }
    assert set(growing) == set(maintenance.OPS_TABLES) | set(maintenance.OPS_TABLES_LEFT_OUT)
    assert {table: growing[table] for table in maintenance.OPS_TABLES} == maintenance.OPS_TABLES
