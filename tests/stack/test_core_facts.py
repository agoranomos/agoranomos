"""The facts of the core (ADR-0036, ADR-0038): each run of the transform replaces the days of the
windows that it pinned whole, by account and day, keeps the rows that a load changed or removed in
the journals of restatements, and does it in one transaction with the write, or not at all.

The test of a changed day makes a load of its own in raw_demo_search as loader, the owner of raw:
the rows of five days of one account, copied from the load that staging reads, with one campaign
changed and one removed on the first day, and none on the last. It deletes that load from raw, and
its row from core.loads as the superuser, and runs the transform again in its finally, so that the
demo is as before; the journals keep their rows, as they keep every restatement. If it is killed
before that, delete by hand, as its finally does: the rows of the six fact tables of
raw_demo_search, of _dlt_loads and of _agoranomos_loads whose load is that of the test, the one
journal row whose _dlt_id is stack-test-facts, and that load from core.loads, then run the
transform; and end the session of PostgreSQL whose application_name is stack-test-facts-lock.

The assertions print IDs, days, counts and sums, never a secret.
"""

import subprocess
import time
from decimal import Decimal

import pytest
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from stack_helpers import REPO_ROOT, active_variant, compose, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
SOURCE = "demo_search"
ACCOUNT = "4170000011"
# The fact tables of raw_demo_search, which each load of the source writes, all of them under its
# own _dlt_load_id (ADR-0038).
TABLES = (
    "campaign_stats",
    "campaign_conversions",
    "ad_group_stats",
    "ad_group_conversions",
    "ad_stats",
    "ad_conversions",
)
# The facts of the core, by the table of raw whose rows each takes from demo_search.
FACTS = {
    "ad_stats_campaign_daily": "campaign_stats",
    "ad_stats_ad_group_daily": "ad_group_stats",
    "ad_stats_ad_daily": "ad_stats",
    "ad_conversions_campaign_daily": "campaign_conversions",
}
MARK = "stack-test-facts"
LOCK = "stack-test-facts-lock"
# dbt by hand, in the container that runs the tasks, which has the password of transformer, with a
# target and a log folder outside the project, which is mounted read-only (CONTRIBUTING.md).
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = f"/tmp/{MARK}"  # noqa: S108 (a folder in the container, not on this host)
# The days of the account that staging reads, with the load of each, and its surrogate.
DAYS = (
    "SELECT d.day, d.load_id FROM staging.stg_loads__newest_days AS d "  # noqa: S608 (constants)
    f"WHERE d.source = '{SOURCE}' AND d.account_id = '{ACCOUNT}' ORDER BY d.day"
)
SK = (
    "SELECT account_sk FROM core.dim_account "  # noqa: S608 (constants of this file)
    f"WHERE platform = '{SOURCE}' AND account_id = '{ACCOUNT}'"
)
# The rows of the campaigns of the account on some days in the core, as campaign, day, impressions,
# clicks, spend and load.
CAMPAIGNS = (
    "SELECT campaign_id, report_date, impressions, clicks, spend, load_sk "
    "FROM core.ad_stats_campaign_daily WHERE account_sk = {sk} "
    "AND report_date BETWEEN DATE '{first}' AND DATE '{last}' ORDER BY 2, 1"
)
# The rows that a load wrote into a journal, as campaign or key, day, clicks and whether vanished.
JOURNAL = (
    "SELECT {key}, report_date, clicks, is_vanished FROM core.{fact}_restatements "
    "WHERE new_load_sk = {load} ORDER BY 2, 1"
)


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    return wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1200)


def rows(env, sql, role="postgres"):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql, role).splitlines()]


def task_service() -> str:
    """The container that runs the tasks of Airflow, with dbt and the password of transformer."""
    return "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"


def dbt(*args: str):
    return compose(
        "exec",
        "-T",
        task_service(),
        DBT,
        "--no-use-colors",
        *args,
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


def copy_load(env, load, days):
    """The rows of the load that staging reads for the account on days, in each fact table of
    raw_demo_search, under the load of the test, as loader, which may not read staging: the days
    and their loads go in as values."""
    read = dict(rows(env, DAYS))
    listed = ", ".join(f"(DATE '{day}', '{read[day]}')" for day in days)
    for table in TABLES:
        columns = warehouse_output(
            env,
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) "  # noqa: S608 (names of the demo)
            "FROM information_schema.columns WHERE table_schema = 'raw_demo_search' "
            f"AND table_name = '{table}' AND column_name NOT IN ('_dlt_load_id', '_dlt_id')",
            "loader",
        )
        warehouse_output(
            env,
            f"INSERT INTO raw_demo_search.{table} ({columns}, _dlt_load_id, _dlt_id) "  # noqa: S608 (names of the demo)
            f"SELECT {columns}, '{load}', '{MARK}-' || md5(random()::text) "
            f"FROM raw_demo_search.{table} AS r JOIN (VALUES {listed}) AS d (day, load_id) "
            "ON d.load_id = r._dlt_load_id AND d.day = r.segments__date "
            f"WHERE r.customer__id = '{ACCOUNT}'",
            "loader",
        )


def remove_load(env, load):
    """The load of the test out of raw and out of core.loads."""
    for table in (*TABLES, "_agoranomos_loads"):
        warehouse_output(
            env,
            f"DELETE FROM raw_demo_search.{table} WHERE _dlt_load_id = '{load}'",  # noqa: S608
            "loader",
        )
    warehouse_output(
        env,
        f"DELETE FROM raw_demo_search._dlt_loads WHERE load_id = '{load}'",  # noqa: S608
        "loader",
    )
    warehouse_output(
        env,
        f"DELETE FROM core.loads WHERE source = '{SOURCE}' AND load_id = '{load}'",  # noqa: S608
    )
    warehouse_output(
        env,
        f"DELETE FROM core.account_quarantine WHERE load_id = '{load}'",  # noqa: S608 (a test load)
    )


def end_the_lock(env):
    warehouse_output(
        env,
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "  # noqa: S608 (a constant)
        f"WHERE application_name = '{LOCK}'",
    )


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, and one has written the facts."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    end_the_lock(env)  # left behind by a test that was stopped
    found = warehouse_output(env, "SELECT to_regclass('staging.int_window_days') IS NOT NULL")
    assert found == "t", "staging.int_window_days does not exist: no transform has run here"


@pytest.mark.parametrize(
    ("fact", "measures"),
    [
        ("ad_stats_campaign_daily", "impressions, clicks, spend, conversions, conversion_value"),
        ("ad_stats_ad_group_daily", "impressions, clicks, spend, conversions, conversion_value"),
        ("ad_stats_ad_daily", "impressions, clicks, spend, conversions, conversion_value"),
        (
            "ad_conversions_campaign_daily",
            "conversions, all_conversions, conversion_value, all_conversion_value",
        ),
        ("web_sessions_daily", "sessions, engaged_sessions, key_events, total_revenue"),
    ],
)
def test_each_fact_holds_rows_with_a_measure_from_the_load_of_their_day(env, idle, fact, measures):
    # A row whose measures are all zero stays out (ADR-0036), and each row carries the load that
    # wrote it, a load of its account in core.loads whose window covers its day.
    total = int(warehouse_output(env, f"SELECT count(*) FROM core.{fact}"))  # noqa: S608 (a fact of this file)
    assert total > 0, f"core.{fact} holds no row of the demo"
    zeros = " AND ".join(f"coalesce({measure}, 0) = 0" for measure in measures.split(", "))
    empty = warehouse_output(env, f"SELECT count(*) FROM core.{fact} WHERE {zeros}")  # noqa: S608
    assert int(empty) == 0, f"core.{fact} holds rows whose measures are all zero"
    unloaded = warehouse_output(
        env,
        f"SELECT count(*) FROM core.{fact} AS f "  # noqa: S608 (a fact of this file)
        "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
        "LEFT JOIN core.loads AS l ON l.load_sk = f.load_sk AND l.source = a.platform "
        "AND l.account_id = a.account_id AND f.report_date BETWEEN l.window_from AND l.window_to "
        "WHERE l.load_sk IS NULL",
    )
    assert int(unloaded) == 0, f"rows of core.{fact} carry no load of their account and day"


def test_a_changed_day_goes_into_the_journal_and_the_fact_whole_or_not_at_all(
    env, airflow_url, token, idle
):
    # ADR-0036 and ADR-0038: a load of five days of one account, newer than the one that staging
    # reads, with one campaign changed and one gone on its first day and no row on its last. The
    # run replaces the five days, and the journal of each level holds the rows that changed or
    # vanished, with their old values; a row that came back the same goes nowhere.
    days = [day for day, _ in rows(env, DAYS)]
    assert len(days) >= 20, "the account has too few days in staging for the test"
    first, *middle, last = days[-15:-10]
    sk = warehouse_output(env, SK)
    before = rows(env, CAMPAIGNS.format(sk=sk, first=first, last=last))
    on_first = sorted((r for r in before if r[1] == first), key=lambda r: Decimal(r[4]))
    assert len(on_first) >= 2, "the account has fewer than two campaigns on the first day"
    gone, changed = on_first[0], on_first[-1]
    spent = sum(Decimal(r[4]) for r in before)
    kept = spent - Decimal(gone[4]) - sum(Decimal(r[4]) for r in before if r[1] == last)
    # G1a would hold a load that lost more than half of the spend of the core (ADR-0035).
    assert kept > spent / 2, "the test would put the account in quarantine"
    load = f"{time.time():.6f}"
    try:
        copy_load(env, load, [first, *middle])
        warehouse_output(
            env,
            "UPDATE raw_demo_search.campaign_stats SET "  # noqa: S608 (values of this test)
            "metrics__impressions = (metrics__impressions::bigint + 70)::text, "
            "metrics__clicks = (metrics__clicks::bigint + 7)::text, "
            "metrics__cost_micros = (metrics__cost_micros::bigint + 1000000)::text "
            f"WHERE _dlt_load_id = '{load}' AND campaign__id = '{changed[0]}' "
            f"AND segments__date = DATE '{first}'",
            "loader",
        )
        warehouse_output(
            env,
            "DELETE FROM raw_demo_search.campaign_stats "  # noqa: S608 (values of this test)
            f"WHERE _dlt_load_id = '{load}' AND campaign__id = '{gone[0]}' "
            f"AND segments__date = DATE '{first}'",
            "loader",
        )
        warehouse_output(
            env,
            "INSERT INTO raw_demo_search._dlt_loads "  # noqa: S608 (values of this test)
            "(load_id, schema_name, status, inserted_at, schema_version_hash) "
            f"SELECT '{load}', schema_name, 0, now(), schema_version_hash "
            "FROM raw_demo_search._dlt_loads WHERE status = 0 ORDER BY inserted_at DESC LIMIT 1",
            "loader",
        )
        warehouse_output(
            env,
            "INSERT INTO raw_demo_search._agoranomos_loads (account_id, as_of, window_from, "  # noqa: S608 (values of this test)
            "window_to, now, loaded_at, status, _dlt_load_id, _dlt_id) "
            f"VALUES ('{ACCOUNT}', DATE '{last}', DATE '{first}', DATE '{last}', now(), now(), "
            f"'loaded', '{load}', '{MARK}')",
            "loader",
        )
        run = transform(airflow_url, token)
        assert run["state"] == "success", run["state"]
        assert rows(env, "SELECT reason FROM ops.ops_quarantine", "metabase_reader") == []
        [(new,)] = rows(
            env,
            f"SELECT load_sk FROM core.loads WHERE source = '{SOURCE}' AND load_id = '{load}' "  # noqa: S608 (values of this test)
            f"AND account_id = '{ACCOUNT}' AND status = 'pinned'",
        )
        # The fact: the new values on the first day, the campaign gone, the last day empty, and
        # every row of the five days written by the load of the test.
        after = rows(env, CAMPAIGNS.format(sk=sk, first=first, last=last))
        expected = [
            (*r[:2], str(int(r[2]) + 70), str(int(r[3]) + 7), str(Decimal(r[4]) + 1), new)
            if r == changed
            else (*r[:5], new)
            for r in before
            if r != gone and r[1] != last
        ]
        assert after == expected
        # The journal of the campaigns: the changed row, the one gone, and those of the last day.
        journal = rows(
            env, JOURNAL.format(key="campaign_id", fact="ad_stats_campaign_daily", load=new)
        )
        assert journal == sorted(
            [
                (changed[0], first, changed[3], "f"),
                (gone[0], first, gone[3], "t"),
                *((r[0], last, r[3], "t") for r in before if r[1] == last),
            ],
            key=lambda r: (r[1], int(r[0])),
        )
        # The journals of the ad groups and the ads: the rows of the last day, which vanished.
        for fact, key in (
            ("ad_stats_ad_group_daily", "ad_group_id"),
            ("ad_stats_ad_daily", "ad_id"),
        ):
            held = warehouse_output(
                env,
                f"SELECT count(*) FROM core.{fact} WHERE account_sk = {sk} "  # noqa: S608 (values of this test)
                f"AND report_date = DATE '{last}'",
            )
            assert held == "0", fact
            journal = rows(env, JOURNAL.format(key=key, fact=fact, load=new))
            assert journal, fact
            assert {(r[1], r[3]) for r in journal} == {(last, "t")}, fact
        conversions = warehouse_output(
            env,
            "SELECT count(*) FROM core.ad_conversions_campaign_daily "  # noqa: S608 (values of this test)
            f"WHERE account_sk = {sk} AND report_date = DATE '{last}'",
        )
        assert conversions == "0"
        assert_one_transaction(env, sk, load, new, middle[0])
    finally:
        end_the_lock(env)
        remove_load(env, load)
        # The next run, which the tests after this one need, whatever this one found.
        again = transform(airflow_url, token)
    # It writes the five days from the load that staging reads again, and the core holds what the
    # marts hold.
    assert again["state"] == "success", again["state"]
    restored = rows(env, CAMPAIGNS.format(sk=sk, first=first, last=last))
    assert [r[:5] for r in restored] == [r[:5] for r in before]
    assert_the_core_holds_what_the_marts_hold(env)


def assert_one_transaction(env, sk, load, new, day):
    """A change of a row of the load of the test that the core holds, written by dbt by hand while
    another session holds the row, so that the delete of the write waits past the lock timeout of
    transformer and fails: the journal of the hook before it keeps no row, and the fact keeps its
    values, as the hooks and the write of a model are one transaction (#191). Then again without
    the lock, which writes both."""
    [(campaign, clicks)] = rows(
        env,
        "SELECT campaign_id, clicks FROM core.ad_stats_campaign_daily "  # noqa: S608 (values of this test)
        f"WHERE account_sk = {sk} AND report_date = DATE '{day}' ORDER BY spend DESC LIMIT 1",
    )
    warehouse_output(
        env,
        "UPDATE raw_demo_search.campaign_stats "  # noqa: S608 (values of this test)
        "SET metrics__clicks = (metrics__clicks::bigint + 3)::text "
        f"WHERE _dlt_load_id = '{load}' AND campaign__id = '{campaign}' "
        f"AND segments__date = DATE '{day}'",
        "loader",
    )
    held = (
        f"SELECT clicks FROM core.ad_stats_campaign_daily WHERE account_sk = {sk} "  # noqa: S608
        f"AND campaign_id = {campaign} AND report_date = DATE '{day}'"
    )
    journal = JOURNAL.format(key="campaign_id", fact="ad_stats_campaign_daily", load=new)
    journaled = rows(env, journal)
    lock = subprocess.Popen(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            f"PGAPPNAME={LOCK}",
            "warehouse-db",
            "psql",
            "--no-psqlrc",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
            "-d",
            "warehouse",
            "-c",
            f"BEGIN; SELECT 1 FROM core.ad_stats_campaign_daily WHERE account_sk = {sk} "  # noqa: S608
            f"AND campaign_id = {campaign} AND report_date = DATE '{day}' FOR UPDATE; "
            "SELECT pg_sleep(600); COMMIT",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(
            lambda: (
                warehouse_output(
                    env,
                    "SELECT count(*) FROM pg_stat_activity "  # noqa: S608 (a constant)
                    f"WHERE application_name = '{LOCK}' AND wait_event = 'PgSleep'",
                )
                == "1"
            ),
            60,
            message="the session of the test did not take the row",
        )
        failed = dbt("build", "--select", "core_ad_stats_campaign_daily")
        assert failed.returncode != 0, "the write did not wait for the row of the test"
        assert "canceling statement due to lock timeout" in failed.stdout, failed.stdout[-2000:]
        assert rows(env, journal) == journaled
        assert warehouse_output(env, held) == clicks
    finally:
        end_the_lock(env)
        lock.wait(timeout=60)
    written = dbt("build", "--select", "core_ad_stats_campaign_daily")
    assert written.returncode == 0, written.stdout[-2000:]
    assert warehouse_output(env, held) == str(int(clicks) + 3)
    assert set(rows(env, journal)) - set(journaled) == {(campaign, day, clicks, "f")}


def assert_the_core_holds_what_the_marts_hold(env):
    """The facts of the core and the marts, which staging builds alike, add up to the same by
    platform and account: no day of an account was lost or doubled."""
    for mart, measures in (
        ("ad_stats_campaign_daily", "impressions, clicks, spend"),
        ("ad_stats_ad_group_daily", "impressions, clicks, spend"),
        ("ad_stats_ad_daily", "impressions, clicks, spend"),
    ):
        sums = ", ".join(f"sum({measure})" for measure in measures.split(", "))
        core = rows(
            env,
            f"SELECT a.platform, a.account_id, {sums} FROM core.{mart} AS f "  # noqa: S608 (names of this file)
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk GROUP BY 1, 2 "
            f"HAVING {' + '.join(sums.split(', '))} > 0 ORDER BY 1, 2",
        )
        marts = rows(
            env,
            f"SELECT platform, account_id, {sums} FROM marts.{mart} "  # noqa: S608 (names of this file)
            f"GROUP BY 1, 2 HAVING {' + '.join(sums.split(', '))} > 0 ORDER BY 1, 2",
        )
        assert core == marts, mart
    core = rows(
        env,
        "SELECT a.account_id, sum(f.sessions) FROM core.web_sessions_daily AS f "
        "JOIN core.dim_account AS a ON a.account_sk = f.account_sk GROUP BY 1 ORDER BY 1",
    )
    marts = rows(
        env, "SELECT property_id, sum(sessions) FROM marts.web_sessions_daily GROUP BY 1 ORDER BY 1"
    )
    assert core == marts
