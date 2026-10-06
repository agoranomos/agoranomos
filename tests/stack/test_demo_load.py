"""The demo load, agoranomos_demo_load: raw_demo holds exactly the rows that the generator makes,
and a repeated load changes no row.

The generator is deterministic and a row depends only on its campaign and day, so the test computes
what the warehouse must hold for the days that it holds. The assertions print counts, dates and
sums, never a secret.
"""

import json
from datetime import date, datetime, timedelta

import pytest
from agoranomos_pipelines.demo_data import CAMPAIGNS, DAYS, rows
from airflow_api import asset_events, run_dag, task_instances, task_log, wait_for_dag
from stack_helpers import leaked, secrets, totals, warehouse_output

LOAD = "agoranomos_demo_load"
RAW_ASSET = "postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily"
RAW_TABLE = "raw_demo.ad_stats_daily"


def totals_generated(days):
    generated = [row for day in days for row in rows(day, 1)]
    return {
        "rows": len(generated),
        "impressions": sum(row["impressions"] for row in generated),
        "clicks": sum(row["clicks"] for row in generated),
        "spend": sum(row["spend"] for row in generated),
        "conversions": sum(row["conversions"] for row in generated),
    }


def days_in_warehouse(env):
    text = warehouse_output(env, "SELECT DISTINCT date FROM raw_demo.ad_stats_daily ORDER BY 1")
    return [date.fromisoformat(day) for day in text.split()]


def loaded(end):
    """The line that the load writes into its log when it has loaded the eight weeks up to end."""
    first = end - timedelta(days=DAYS - 1)
    return f"Loaded {DAYS * len(CAMPAIGNS)} rows of {first} to {end} into {RAW_TABLE}"


@pytest.fixture(scope="module")
def load_run(airflow_url, token):
    wait_for_dag(airflow_url, token, LOAD)
    return run_dag(airflow_url, token, LOAD)


def test_the_load_succeeds(load_run):
    assert load_run["state"] == "success", load_run["state"]


def test_raw_demo_holds_exactly_the_generated_rows(env, load_run):
    days = days_in_warehouse(env)
    assert days, "raw_demo holds no day"
    yesterday = datetime.fromisoformat(load_run["start_date"]).date() - timedelta(days=1)
    # A run that starts just before midnight (UTC) and loads just after it ends one day later.
    assert days[-1] in (yesterday, yesterday + timedelta(days=1)), "the load does not end yesterday"
    # The window is the eight weeks that end on the last day of the load.
    end = days[-1]
    window = {end - timedelta(days=offset) for offset in range(DAYS)}
    assert window <= set(days), "the eight weeks up to the last day are not all in raw_demo"
    assert totals(env, RAW_TABLE) == totals_generated(days)


def test_a_repeated_load_changes_no_row(env, airflow_url, token, load_run):
    last = days_in_warehouse(env)[-1]
    before = totals(env, RAW_TABLE, until=last)
    loads = int(warehouse_output(env, "SELECT count(*) FROM raw_demo._dlt_loads"))
    run = run_dag(airflow_url, token, LOAD)
    assert run["state"] == "success", run["state"]
    assert totals(env, RAW_TABLE, until=last) == before
    assert int(warehouse_output(env, "SELECT count(*) FROM raw_demo._dlt_loads")) == loads + 1


def test_the_columns_have_the_types_of_the_source(env, load_run):
    text = warehouse_output(
        env,
        "SELECT column_name || ':' || data_type || coalesce(':' || numeric_precision || ','"
        " || numeric_scale, '') FROM information_schema.columns WHERE table_schema = 'raw_demo'"
        " AND table_name = 'ad_stats_daily' AND column_name IN ('date', 'spend', 'conversions',"
        " 'clicks') ORDER BY 1",
    )
    assert text.split() == [
        "clicks:bigint:64,0",
        "conversions:numeric:12,2",
        "date:date",
        "spend:numeric:12,2",
    ]


def test_the_loader_role_owns_the_tables_of_the_load(env, load_run):
    # dlt signs in as loader, so loader creates the tables: the data and the state of dlt in
    # raw_demo, and the tables of the merge in raw_demo_staging. transformer reads raw_demo
    # through the default privileges of loader.
    text = warehouse_output(
        env,
        "SELECT DISTINCT schemaname || ':' || tableowner FROM pg_tables "
        "WHERE schemaname IN ('raw_demo', 'raw_demo_staging') ORDER BY 1",
    )
    assert text.split() == ["raw_demo:loader", "raw_demo_staging:loader"]


def test_a_successful_load_updates_the_asset_of_the_raw_table(airflow_url, token, load_run):
    # The event of this run: Airflow keeps the events of every run before it, also after
    # ./agoranomos down.
    run_id = load_run["dag_run_id"]
    events = asset_events(airflow_url, token, RAW_ASSET, source_dag_id=LOAD, source_run_id=run_id)
    assert events, f"the run {run_id} did not update the asset of the raw table"
    # The API ignores a filter that it does not know, so the events must be those of this run.
    assert {(e["source_dag_id"], e["source_run_id"]) for e in events} == {(LOAD, run_id)}


def test_the_log_of_the_load_holds_no_secret(env, airflow_url, token, load_run):
    # Every try of the task, each entry whole: names are printed, never values. The log is read
    # where it is used, so that no frame of a failed test holds it, and the line that only the
    # load writes shows that the scan reads the log of a load.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    [instance] = task_instances(airflow_url, token, load_run)
    # The task loads up to the day before the one it starts on, which can follow the start of the
    # run by a day.
    yesterday = datetime.fromisoformat(load_run["start_date"]).date() - timedelta(days=1)
    lines = {loaded(yesterday), loaded(yesterday + timedelta(days=1))}
    said = any(
        json.loads(entry).get("event") in lines for entry in task_log(airflow_url, token, instance)
    )
    assert said, "the log of the load does not say what it loaded"
    found = leaked(values, task_log(airflow_url, token, instance))
    assert not found, f"the log of the load holds the values of {found}"
