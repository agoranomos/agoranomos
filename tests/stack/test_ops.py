"""The Ops collection (ADR-0029): the hooks of elementary record each node of a run of the transform
in the schema ops, with the run of Airflow, and no row that a test found; the views of ops show the
loads of every account; metabase_reader reads those views and no table of elementary; and every
question of the dashboard of the Ops collection answers in Metabase.

The assertions print names, counts and statuses, never a secret.
"""

import ops
import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import dag_runs, task_instances
from metabase_content import item, registry
from stack_helpers import http_json, warehouse, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
]

TRANSFORM = "agoranomos_demo_transform"
# The views of ops that metabase_reader may read, and a table of elementary that it may not.
VIEWS = ("ops_loads", "ops_transform_nodes", "ops_transform_tests", "ops_warehouse_tables")
TABLE_OF_ELEMENTARY = "ops.dbt_run_results"


@pytest.fixture(scope="module")
def transform_run(airflow_url, token):
    """The newest successful run of the transform: the first start made one, and the tests of the
    transform before these made more."""
    runs = dag_runs(airflow_url, token, TRANSFORM, limit=50)
    successful = [run for run in runs if run["state"] == "success"]
    assert successful, f"{TRANSFORM} has no successful run among its last 50"
    return successful[0]


def test_elementary_records_each_model_and_seed_of_a_run(env, airflow_url, token, transform_run):
    # Each task of dbt is one node and its tests, and elementary records each node with the run of
    # Airflow that the task gives dbt.
    suffixes = ("_model_build", "_seed_build")
    built = {
        instance["task_id"].removeprefix("dbt.").removesuffix(suffix)
        for instance in task_instances(airflow_url, token, transform_run)
        for suffix in suffixes
        if instance["task_id"].startswith("dbt.") and instance["task_id"].endswith(suffix)
    }
    assert built, "the run has no task of dbt"
    run_id = transform_run["dag_run_id"].replace("'", "''")
    text = warehouse_output(
        env,
        "SELECT node_name || '|' || status FROM ops.ops_transform_nodes "  # noqa: S608 (an ID of Airflow, quoted)
        f"WHERE airflow_run_id = '{run_id}' AND resource_type IN ('model', 'seed')",
    )
    recorded = dict(line.split("|") for line in text.splitlines())
    assert set(recorded) == built, sorted(set(recorded) ^ built)[:5]
    assert set(recorded.values()) == {"success"}, recorded
    tests = warehouse_output(
        env,
        "SELECT DISTINCT status FROM ops.ops_transform_tests "  # noqa: S608 (an ID of Airflow, quoted)
        f"WHERE airflow_run_id = '{run_id}'",
    )
    # The demo leaves one day of one account out, which a warning test finds.
    assert set(tests.split()) == {"pass", "warn"}, tests


def test_the_tests_record_no_row_that_they_found(env, transform_run):
    # test_sample_row_count is 0 in dbt_project.yml: a result is a status and a number of rows.
    assert int(warehouse_output(env, "SELECT count(*) FROM ops.test_result_rows")) == 0
    kept = "SELECT count(*) FROM ops.elementary_test_results WHERE result_rows IS NOT NULL"
    assert int(warehouse_output(env, kept)) == 0


@pytest.mark.parametrize("view", VIEWS)
def test_metabase_reader_reads_each_view_of_ops(env, transform_run, view):
    rows = warehouse_output(env, f"SELECT count(*) FROM ops.{view}", "metabase_reader")  # noqa: S608 (constants of this file)
    assert int(rows) > 0, view


def test_metabase_reader_reads_no_table_of_elementary(env, transform_run):
    result = warehouse(env, f"SELECT 1 FROM {TABLE_OF_ELEMENTARY} LIMIT 1", "metabase_reader")  # noqa: S608 (a constant of this file)
    assert result.returncode != 0
    assert "permission denied" in result.stderr


def test_the_loads_of_every_account_are_in_ops(env, transform_run):
    text = warehouse_output(env, "SELECT platform || '|' || account_id FROM ops.ops_loads")
    shown = {tuple(line.split("|")) for line in text.splitlines()}
    accounts = {
        (code, account.account_id)
        for code, platform in platforms.PLATFORMS.items()
        for account in platform.module.accounts()
    }
    assert shown == accounts


def test_every_question_of_the_ops_dashboard_answers(metabase_url, session, transform_run):
    entries = registry()
    board = item(metabase_url, session, "ops/pipeline", entries)
    assert board["name"] == ops.PIPELINE_NAME
    assert len(board["dashcards"]) == len(ops.QUESTIONS)
    for question in ops.QUESTIONS:
        card = item(metabase_url, session, f"ops/{question['key']}", entries)
        status, answer = http_json(
            "POST", f"{metabase_url}/api/card/{card['id']}/query", {}, headers=session
        )
        assert status in (200, 202), (question["key"], status)
        assert answer.get("status") == "completed", (question["key"], answer.get("status"))
