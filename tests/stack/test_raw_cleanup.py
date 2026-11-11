"""The acceptance of the loads and the cleanup of the raw layer (ADR-0035, ADR-0046): a run of the
transform that succeeds marks the loads that it read accepted in core.loads, by load and account,
and then deletes their rows from raw, but the journal and the tables of dlt; an account in
quarantine keeps its rows and its pinned load; a run after a cleanup keeps every entity, fact and
day of the core; a repeat of either step finds nothing to do; a load in progress loses no row; and
the rows of a load that did not end go once it is older than the cut
(airflow/dags/agoranomos_pipelines/acceptance.py).

The tests make loads of their own of demo_search and demo_social as loader, in the container of the
tasks, as of the moment of the newest load of each, so that they hold the rows that the demo has
for those days; they break the list of one account of demo_search and put it back, and write rows
of their own into raw_demo_search.campaign_stats, whose _dlt_id starts with stack-test-cleanup,
which they delete. Each runs the transform again in its finally, so the demo is as before. If one
is killed before that, delete those rows as loader, UPDATE the currency of 4170000012 in
raw_demo_search.customer_clients to USD, and run the transform.

The assertions print codes, IDs, counts and sums, never a secret.
"""

import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest
from agoranomos_pipelines import acceptance
from agoranomos_pipelines.demo import platforms, world
from airflow_api import is_busy, task_instances, trigger, unpause, wait_for_dag, wait_for_run
from raw_loads import last_moment, load_platform, run_in_tasks, transform_keeping_raw
from stack_helpers import REPO_ROOT, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
MARK = "stack-test-cleanup"
ACCOUNTS = {account.account_id: account for account in world.ACCOUNTS}
BAD, GOOD = ACCOUNTS["4170000012"], ACCOUNTS["4170000011"]
DEMO = {platform.dataset: code for code, platform in platforms.PLATFORMS.items()}
# The steps by hand, in the container of the tasks, as the tasks run them.
STEP = """
import json
import sys

from agoranomos_pipelines import acceptance

print(json.dumps(getattr(acceptance, sys.argv[1])()))
"""
# Each table of the core that a run writes, as its rows and their sums, and the entities of each
# dimension, listed or not, by platform.
CORE = {
    "facts": (
        "SELECT a.platform, count(*), count(DISTINCT (f.account_sk, f.report_date)), "
        "sum(f.impressions), sum(f.clicks), sum(f.spend) FROM core.ad_stats_{level}_daily AS f "
        "JOIN core.dim_account AS a USING (account_sk) GROUP BY 1 ORDER BY 1"
    ),
    "entities": (
        "SELECT a.platform, count(*), count(*) FILTER (WHERE d.is_listed) FROM core.dim_{level} "
        "AS d JOIN core.dim_account AS a USING (account_sk) GROUP BY 1 ORDER BY 1"
    ),
}
LEVELS = {"facts": ("campaign", "ad_group", "ad"), "entities": ("campaign", "ad_group", "ad")}
SESSIONS = (
    "SELECT count(*), count(DISTINCT (account_sk, report_date)), sum(sessions) "
    "FROM core.web_sessions_daily"
)
ACCOUNTS_LISTED = (
    "SELECT platform, count(*), count(*) FILTER (WHERE is_active), count(*) FILTER (WHERE is_stub) "
    "FROM core.dim_account GROUP BY 1 ORDER BY 1"
)
# The rows that the tests write into raw_demo_search.campaign_stats, by their _dlt_id.
REMOVE_OWN = f"DELETE FROM raw_demo_search.campaign_stats WHERE _dlt_id LIKE '{MARK}%'"  # noqa: S608


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    return run


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def step(env, name):
    """Run a step of acceptance.py by hand, as its task does, and return what it returned."""
    return json.loads(run_in_tasks(env, STEP, name).stdout.strip().splitlines()[-1])


def core(env):
    """What the core holds: the facts of ads and sessions, and the entities, by platform."""
    held = {
        f"{kind}.{level}": rows(env, CORE[kind].format(level=level))
        for kind, levels in LEVELS.items()
        for level in levels
    }
    held["sessions"] = rows(env, SESSIONS)
    held["accounts"] = rows(env, ACCOUNTS_LISTED)
    return held


def kept_in_raw(env, schema, table, where="true"):
    """The rows of a table of raw that belong to an accepted load, by load and account where the
    cleanup names the account of the table: none should be left."""
    account = acceptance.ACCOUNTS[schema][table]
    source = schema.removeprefix("raw_")
    match = f"l.account_id = ({account})::text" if account is not None else "true"
    return int(
        warehouse_output(
            env,
            f"SELECT count(*) FROM {schema}.{table} AS r JOIN core.loads AS l "  # noqa: S608 (names of the demo)
            f"ON l.source = '{source}' AND l.load_id = r._dlt_load_id AND {match} "
            f"AND l.status = 'accepted' WHERE {where}",
        )
    )


@pytest.fixture(scope="module")
def cleaned(env, airflow_url, token):
    """A run of the transform that succeeded, after the runs that earlier tests started: the run
    that accepted and cleaned what they left."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1800,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    warehouse_output(env, REMOVE_OWN, "loader")  # left behind by a test that was stopped
    return transform(airflow_url, token)


def test_a_run_accepts_what_it_read_and_raw_keeps_no_row_of_it(env, airflow_url, token, cleaned):
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, cleaned)}
    assert (states["accept"], states["clean_raw"], states["transformed"]) == ("success",) * 3
    # No account of the demo is in quarantine, so every load of the demo is accepted.
    pinned = rows(
        env,
        "SELECT source, count(*) FROM core.loads WHERE status = 'pinned' "
        "AND source LIKE 'demo\\_%' GROUP BY 1",
    )
    assert pinned == []
    unaccepted = "SELECT count(*) FROM core.loads WHERE status = 'accepted' AND accepted_at IS NULL"
    assert warehouse_output(env, unaccepted) == "0"
    for schema, tables in acceptance.ACCOUNTS.items():
        if schema not in DEMO:
            continue
        for table in tables:
            assert kept_in_raw(env, schema, table) == 0, f"{schema}.{table}"
    # Nothing of the demo is left in raw: the child tables of Meta went with their rows.
    for table in ("insights_campaign__actions", "insights_ad__action_values"):
        assert warehouse_output(env, f"SELECT count(*) FROM raw_demo_social.{table}") == "0"  # noqa: S608
    # The journal and the tables of dlt stay whole: the next load starts its window from them.
    for dataset in DEMO:
        for table in ("_agoranomos_loads", "_dlt_loads", "_dlt_version"):
            found = warehouse_output(env, f"SELECT count(*) > 0 FROM {dataset}.{table}")  # noqa: S608
            assert found == "t", f"{dataset}.{table}"


def test_a_repeat_of_the_acceptance_and_of_the_cleanup_finds_nothing(env, cleaned):
    assert step(env, "accept") == {}
    gone = step(env, "clean_raw")
    assert {schema for schema in gone} >= set(DEMO), sorted(gone)
    assert set(gone.values()) == {0}, gone


def test_a_load_in_progress_loses_no_row_and_a_load_that_did_not_end_goes_after_a_day(env, cleaned):
    # A table that holds rows of an accepted load only, as the cleanup would TRUNCATE, while a load
    # writes into it in a transaction that is still open: the cleanup waits for the lock, sees the
    # rows of the load, and deletes only the accepted ones (#188). A row of a load that did not end,
    # older than the cut, goes; a younger one stays.
    [accepted] = rows(
        env,
        "SELECT load_id FROM core.loads WHERE source = 'demo_search' AND status = 'accepted' "  # noqa: S608 (an ID of the demo)
        f"AND account_id = '{GOOD.account_id}' ORDER BY load_id::numeric DESC LIMIT 1",
    )
    now = datetime.now(UTC)
    young = f"{now.timestamp():.6f}"
    old = f"{(now - acceptance.UNFINISHED_AFTER - timedelta(hours=1)).timestamp():.6f}"
    insert = (
        "INSERT INTO raw_demo_search.campaign_stats (customer__id, _dlt_load_id, _dlt_id) "  # noqa: S608 (values of this test)
        f"VALUES ('{GOOD.account_id}', '{{load}}', '{MARK}-{{name}}')"
    )
    count = (
        "SELECT count(*) FROM raw_demo_search.campaign_stats "  # noqa: S608 (a constant)
        f"WHERE _dlt_id = '{MARK}-{{name}}'"
    )
    try:
        warehouse_output(env, insert.format(load=accepted, name="accepted"), "loader")
        warehouse_output(env, insert.format(load=old, name="old"), "loader")
        progress = subprocess.Popen(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "-e",
                f"PGAPPNAME={MARK}",
                "warehouse-db",
                "psql",
                "--no-psqlrc",
                "-h",
                "127.0.0.1",
                "-U",
                "loader",
                "-d",
                "warehouse",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"BEGIN; {insert.format(load=young, name='progress')}; SELECT pg_sleep(8); COMMIT",
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        wait_for(
            lambda: (
                warehouse_output(
                    env,
                    "SELECT count(*) FROM pg_stat_activity "  # noqa: S608 (a constant)
                    f"WHERE application_name = '{MARK}' AND wait_event = 'PgSleep'",
                )
                == "1"
            ),
            60,
            message="the load of the test did not begin",
        )
        gone = step(env, "clean_raw")
        assert progress.wait(timeout=60) == 0
        assert gone["raw_demo_search"] == 2, gone
        assert warehouse_output(env, count.format(name="accepted")) == "0"
        assert warehouse_output(env, count.format(name="old")) == "0"
        assert warehouse_output(env, count.format(name="progress")) == "1"
    finally:
        warehouse_output(env, REMOVE_OWN, "loader")


def test_a_run_after_the_cleanup_keeps_every_entity_fact_and_day(env, airflow_url, token, cleaned):
    # B9-rest: staging reads the lists of an account only from its newest load when that load is
    # pinned, so after a cleanup the views keep what the core holds. A load of the last days of
    # demo_social, as of the moment of its newest load, holds the same rows as the history did for
    # them: the next run reads it, writes the same days, lists the same entities, and cleans it.
    before = core(env)
    now = last_moment(env, "demo_social")
    load = load_platform(env, "demo_social", now, (now - timedelta(days=4)).date())
    transform(airflow_url, token)
    assert core(env) == before
    states = rows(
        env,
        "SELECT DISTINCT status FROM core.loads "  # noqa: S608 (an ID of the warehouse)
        f"WHERE source = 'demo_social' AND load_id = '{load}'",
    )
    assert states == ["accepted"]
    for table in acceptance.ACCOUNTS["raw_demo_social"]:
        assert kept_in_raw(env, "raw_demo_social", table) == 0, table
        found = warehouse_output(
            env,
            f"SELECT count(*) FROM raw_demo_social.{table} WHERE _dlt_load_id = '{load}'",  # noqa: S608
        )
        assert found == "0", table


def test_a_failure_between_the_acceptance_and_the_cleanup_loses_nothing(
    env, airflow_url, token, cleaned
):
    # ADR-0046: the acceptance comes first, so a run whose cleanup did not happen leaves accepted
    # loads with their rows in raw, which no run reads any more; the next run writes nothing of
    # them and deletes their rows.
    before = core(env)
    now = last_moment(env, "demo_social")
    load = load_platform(env, "demo_social", now, (now - timedelta(days=4)).date())
    left = (
        "SELECT count(*) FROM raw_demo_social.insights_campaign "  # noqa: S608 (an ID of the warehouse)
        f"WHERE _dlt_load_id = '{load}'"
    )
    states = (
        "SELECT string_agg(DISTINCT status, ',') FROM core.loads "  # noqa: S608 (an ID of the warehouse)
        f"WHERE source = 'demo_social' AND load_id = '{load}'"
    )
    transform_keeping_raw(airflow_url, token, skipped="clean_raw")
    assert warehouse_output(env, states) == "accepted"
    assert int(warehouse_output(env, left)) > 0
    assert core(env) == before
    transform(airflow_url, token)
    assert warehouse_output(env, left) == "0"
    assert core(env) == before


def test_an_account_in_quarantine_keeps_its_rows_and_the_other_loses_them(
    env, airflow_url, token, cleaned
):
    # One load can be accepted for one account and wait in quarantine for another, so the cleanup
    # goes by load and account (ADR-0035): a currency outside ISO 4217 in the list of one account of
    # a new load of demo_search holds that account, whose rows of the load stay in raw and whose row
    # of core.loads stays pinned, while the other account of the load is accepted and loses its
    # rows. Once the list is right again, the next run accepts the account and cleans its rows.
    now = last_moment(env, "demo_search")
    load = load_platform(env, "demo_search", now, (now - timedelta(days=4)).date())
    currency = (
        "UPDATE raw_demo_search.customer_clients SET customer_client__currency_code = '{code}' "  # noqa: S608
        f"WHERE _dlt_load_id = '{load}' AND customer_client__id = '{BAD.account_id}'"
    )
    status = (
        "SELECT status FROM core.loads WHERE source = 'demo_search' "  # noqa: S608
        f"AND load_id = '{load}' AND account_id = '{{account}}'"
    )
    held = (
        "SELECT count(*) FROM raw_demo_search.{table} "  # noqa: S608 (names of the demo)
        f"WHERE _dlt_load_id = '{load}' AND {{column}} = '{{account}}'"
    )
    try:
        warehouse_output(env, currency.format(code="ZZZ"), "loader")
        transform(airflow_url, token)
        quarantined = rows(
            env,
            "SELECT DISTINCT account_id || '|' || reason FROM core.account_quarantine "  # noqa: S608
            f"WHERE source = 'demo_search' AND load_id = '{load}'",
        )
        assert quarantined == [f"{BAD.account_id}|unknown_currency"]
        assert warehouse_output(env, status.format(account=BAD.account_id)) == "pinned"
        assert warehouse_output(env, status.format(account=GOOD.account_id)) == "accepted"
        for table, column in (
            ("campaign_stats", "customer__id"),
            ("campaigns", "customer__id"),
            ("customer_clients", "customer_client__id"),
        ):
            found = held.format(table=table, column=column, account=BAD.account_id)
            assert int(warehouse_output(env, found)) > 0, table
            found = held.format(table=table, column=column, account=GOOD.account_id)
            assert warehouse_output(env, found) == "0", table
    finally:
        warehouse_output(env, currency.format(code=BAD.currency), "loader")
        transform(airflow_url, token)
    assert warehouse_output(env, status.format(account=BAD.account_id)) == "accepted"
    for table, column in (
        ("campaign_stats", "customer__id"),
        ("customer_clients", "customer_client__id"),
    ):
        found = held.format(table=table, column=column, account=BAD.account_id)
        assert warehouse_output(env, found) == "0", table
