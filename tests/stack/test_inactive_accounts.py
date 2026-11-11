"""An account that leaves the settings of the model (ADR-0044): the core keeps its data, the views
of the marts go on showing it with the client unassigned and is_account_active false, the test
assert_every_account_with_data_is_in_the_settings warns, and an account that the settings name
again is active again, with its client.

The demo names its accounts in a file of the base package, which a deployment cannot take an
account out of. So the test checks that the newest run of the transform ran the new test, which
found no row, and then takes one account of demo_search and one property of demo_web out of
raw_settings.account_clients as loader, the owner of raw, and builds dim_account with its tests by
hand, as the task of the transform builds it. In its finally it puts the two rows back as they were
and builds dim_account again. If it is killed before that, run the transform: its first task loads
the settings from the files again, and dim_account takes them.

The assertions print IDs, counts and states, never a secret.
"""

import json

import pytest
from airflow_api import is_busy, wait_for_dag
from stack_helpers import active_variant, compose, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
TEST = "assert_every_account_with_data_is_in_the_settings"
# The account of an ad platform and the property of web analytics that leave the settings, both of
# the client brindlemoor of the demo, with the view of the marts that holds the data of each, its
# column of the ID and a measure.
LEAVING = {
    ("demo_search", "4170000012"): ("ad_stats_campaign_daily", "account_id", "spend"),
    ("demo_web", "390000002"): ("web_sessions_daily", "property_id", "sessions"),
}
CLIENT = "brindlemoor"
PAIRS = ", ".join(f"('{platform}', '{account}')" for platform, account in sorted(LEAVING))
# The rows of the settings of the two, whole, as JSON, which puts them back as they were.
SETTINGS = (
    "SELECT coalesce(json_agg(s ORDER BY s.platform, s.account_id), '[]') "  # noqa: S608 (IDs of the demo)
    f"FROM raw_settings.account_clients AS s WHERE (s.platform, s.account_id) IN ({PAIRS})"
)
REMOVE = f"DELETE FROM raw_settings.account_clients WHERE (platform, account_id) IN ({PAIRS})"  # noqa: S608 (IDs of the demo)
PUT_BACK = (
    "INSERT INTO raw_settings.account_clients "
    "SELECT * FROM json_populate_recordset(NULL::raw_settings.account_clients, '{rows}')"
)
INACTIVE = "SELECT platform || ' ' || account_id FROM core.dim_account WHERE NOT is_active"
# dbt by hand, in the container that runs the tasks of Airflow, which has the password of
# transformer, with a target and a log folder outside the project, which is mounted read-only
# (CONTRIBUTING.md), and the tests of each node selected as the transform selects them.
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = "/tmp/stack-test-inactive-accounts"  # noqa: S108 (a folder in the container, not on this host)


def task_service() -> str:
    """The container that runs the tasks of Airflow, with dbt and the password of transformer."""
    return "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"


def build_the_registry():
    """Build dim_account with its tests, and return the status of the test of the accounts with
    data out of the settings and the number of rows that it found, from the results of dbt."""
    built = compose(
        "exec",
        "-T",
        task_service(),
        DBT,
        "--no-use-colors",
        "build",
        "--select",
        "dim_account",
        "--indirect-selection",
        "buildable",
        "--project-dir",
        PROJECT,
        "--profiles-dir",
        PROJECT,
        "--target-path",
        f"{WORK}/target",
        "--log-path",
        f"{WORK}/logs",
        check=False,
    )
    assert built.returncode == 0, built.stdout[-3000:]
    results = compose(
        "exec", "-T", task_service(), "cat", f"{WORK}/target/run_results.json", check=False
    )
    assert results.returncode == 0, results.stderr[-500:]
    [result] = [
        r for r in json.loads(results.stdout)["results"] if r["unique_id"].endswith(f".{TEST}")
    ]
    return result["status"], result["failures"]


def shown(env, view, column, measure, platform, account):
    """What a view of the marts shows of an account: its rows, the sum of a measure, its clients
    and whether it is active, as rows|sum|clients|activities."""
    return warehouse_output(
        env,
        f"SELECT count(*), sum({measure}), string_agg(DISTINCT client_id, ','), "  # noqa: S608 (constants of this file)
        "string_agg(DISTINCT is_account_active::text, ',') "
        f"FROM marts.{view} WHERE platform = '{platform}' AND {column} = '{account}'",
        "metabase_reader",
    )


def listed(env, account):
    """The client of an ad account in the view of the accounts, and whether it is active."""
    return warehouse_output(
        env,
        "SELECT client_id || '|' || is_account_active FROM marts.ad_accounts "  # noqa: S608 (IDs of the demo)
        f"WHERE platform = 'demo_search' AND account_id = '{account}'",
        "metabase_reader",
    )


def test_an_account_that_leaves_the_settings_keeps_its_data_and_is_no_longer_active(
    env, airflow_url, token
):
    # No run of the transform is queued or running, which would load the settings again.
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    # The transform runs the test, in the task of dim_account: its newest run recorded it, with no
    # row, as the settings of the demo name every account of the demo.
    newest = warehouse_output(
        env,
        "SELECT status || '|' || failures FROM ops.ops_transform_tests "  # noqa: S608 (constants of this file)
        f"WHERE airflow_dag_id = '{TRANSFORM}' AND test_type = '{TEST}' "
        "ORDER BY detected_at DESC LIMIT 1",
    )
    assert newest == "pass|0", newest
    before = {key: shown(env, *where, *key).split("|") for key, where in LEAVING.items()}
    for key, (rows, _, clients, active) in before.items():
        assert int(rows) > 0, key
        assert (clients, active) == (CLIENT, "true"), key
    inactive = set(warehouse_output(env, INACTIVE).splitlines())
    settings = warehouse_output(env, SETTINGS, "loader")
    found = len(json.loads(settings))
    assert found == len(LEAVING), f"the settings hold {found} rows of the two, not one of each"
    try:
        warehouse_output(env, REMOVE, "loader")
        assert build_the_registry() == ("warn", len(LEAVING))
        # The registry keeps both, no longer active, with the client unassigned, and the views show
        # every row that they showed, which follows the client of the account.
        left = {f"{platform} {account}" for platform, account in LEAVING}
        assert set(warehouse_output(env, INACTIVE).splitlines()) == inactive | left
        for key, where in LEAVING.items():
            rows, total, *_ = before[key]
            assert shown(env, *where, *key).split("|") == [rows, total, "unassigned", "false"]
        assert listed(env, "4170000012") == "unassigned|false"
        assert listed(env, "4170000011") == "tarnwick|true"
    finally:
        warehouse_output(env, PUT_BACK.format(rows=settings.replace("'", "''")), "loader")
        back = build_the_registry()
    # Named again, each is active again, with its client and all its data.
    assert back == ("pass", 0)
    assert set(warehouse_output(env, INACTIVE).splitlines()) == inactive
    for key, where in LEAVING.items():
        assert shown(env, *where, *key).split("|") == before[key]
    assert listed(env, "4170000012") == f"{CLIENT}|true"
