"""The history of the fictional platforms, agoranomos_demo_history, and their daily load,
agoranomos_demo_daily: each raw_demo_<platform> holds what the demo generator makes for the days of
each account that a load replaces, a repeated history and a row that a platform no longer sends
leave no stale row, each load records the window of each account that it replaced, and the daily
load takes the days after the last load, or nothing when the day of every account is loaded. The
transform is paused while these tests run the loads, which would start a run of it each time, and
runs once after the last of them.

The generator is deterministic, so the tests compute what the warehouse must hold for the days that
it holds. The assertions print counts, dates and sums, never a secret.
"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise

import pytest
from agoranomos_pipelines.demo import common, platforms
from airflow_api import (
    asset_events,
    dag_runs,
    is_busy,
    is_paused,
    pause,
    run_dag,
    task_instances,
    task_log,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from services import DEMO
from stack_helpers import leaked, secrets, wait_for, warehouse_output

pytestmark = [
    pytest.mark.skipif(
        not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
    ),
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

HISTORY = "agoranomos_demo_history"
TRANSFORM = "agoranomos_demo_transform"
DAILY = "agoranomos_demo_daily"
# What each main table holds, as SQL over its columns and as the same from a record of the
# generator: the account, the day, and the count and the sums that the comparison takes.
MAIN = {
    "demo_search": (
        "customer__id",
        "segments__date",
        "count(*), sum(metrics__cost_micros::numeric), sum(metrics__impressions::bigint), "
        "sum(metrics__clicks::bigint)",
        lambda r: (
            r["customer"]["id"],
            r["segments"]["date"],
            (
                int(r["metrics"]["costMicros"]),
                int(r["metrics"]["impressions"]),
                int(r["metrics"]["clicks"]),
            ),
        ),
    ),
    "demo_social": (
        "account_id",
        "date_start",
        "count(*), sum(spend::numeric), sum(impressions::bigint), sum(inline_link_clicks::bigint)",
        lambda r: (
            r["account_id"],
            r["date_start"],
            (Decimal(r["spend"]), int(r["impressions"]), int(r["inline_link_clicks"])),
        ),
    ),
    "demo_video": (
        "advertiser_id",
        "left(dimensions__stat_time_day::text, 10)",
        "count(*), sum(metrics__spend::numeric), sum(metrics__impressions::bigint), "
        "sum(metrics__clicks::bigint)",
        lambda r: (
            r["advertiser_id"],
            r["dimensions"]["stat_time_day"][:10],
            (
                Decimal(r["metrics"]["spend"]),
                int(r["metrics"]["impressions"]),
                int(r["metrics"]["clicks"]),
            ),
        ),
    ),
    "demo_web": (
        "split_part(property, '/', 2)",
        "to_char(to_date(date, 'YYYYMMDD'), 'YYYY-MM-DD')",
        "count(*), sum(sessions::bigint), sum(engaged_sessions::bigint), "
        "sum(total_revenue::numeric)",
        lambda r: (
            r["property"].split("/")[1],
            f"{r['date'][:4]}-{r['date'][4:6]}-{r['date'][6:]}",
            (int(r["sessions"]), int(r["engagedSessions"]), Decimal(r["totalRevenue"])),
        ),
    ),
    "demo_fx": (
        "base",
        "rate_date",
        "count(*), sum(rate::numeric), 0, 0",
        lambda r: (r["base"], r["rate_date"], (Decimal(r["rate"]), 0, 0)),
    ),
}


@pytest.fixture(scope="module", autouse=True)
def transform_paused(airflow_url, token):
    """The tests below run the history three times. While the transform is unpaused, each history
    starts or queues a run of it, which takes minutes on a runner of CI and which none of them
    needs; Airflow queues no event for a paused DAG. So the transform is paused while they run, and
    runs once after the last history, as the first start leaves the demo: demo-init, and the tests
    of the demo in Metabase after these, find a good transform after the newest history."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    was_paused = is_paused(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        900,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    pause(airflow_url, token, TRANSFORM)
    yield
    if was_paused:
        return
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=900)
    assert run["state"] == "success", run["state"]


@pytest.fixture(scope="module")
def history_run(airflow_url, token):
    """A run of the history now: its moment, run_after, is the moment of every load in it."""
    wait_for_dag(airflow_url, token, HISTORY)
    run = run_dag(airflow_url, token, HISTORY, timeout=900)
    assert run["state"] == "success", run["state"]
    return run


def moment(run):
    return datetime.fromisoformat(run["run_after"].replace("Z", "+00:00"))


def expected(code, now, history=None):
    """The count and the sums of the main table of a platform, from the generator, as the history
    as of now loads it: the days from the first of the history to yesterday of each account,
    without the day that the history as of history, by default now, left out."""
    platform = platforms.PLATFORMS[code]
    _, _, _, of = MAIN[code]
    skip = platforms.gap(now if history is None else history)
    records = platform.module.records(now, skip=skip)[platform.main_table]
    found = [of(record) for record in records]
    sums = [sum(values[i] for _, _, values in found) for i in range(3)]
    return [len(found), *sums]


def in_warehouse(env, code, now):
    """The same, from the warehouse, for the days that a load as of now replaces, its windows:
    earlier loads may hold days before them."""
    platform = platforms.PLATFORMS[code]
    account, day, sums, _ = MAIN[code]
    windows = " OR ".join(
        f"({account} = '{account_id}' AND ({day})::date "
        f"BETWEEN DATE '{first.isoformat()}' AND DATE '{last.isoformat()}')"
        for account_id, (first, last) in platform.module.windows(now).items()
    )
    # The names and the values come from the code of the demo.
    sql = f"SELECT {sums} FROM {platform.dataset}.{platform.main_table} WHERE {windows}"  # noqa: S608
    text = warehouse_output(env, sql)
    return [
        Decimal(value or 0) if "." in (value or "") else int(value or 0)
        for value in text.split("|")
    ]


def test_the_history_loads_the_platforms_one_after_another(airflow_url, token, history_run):
    instances = sorted(
        task_instances(airflow_url, token, history_run), key=lambda i: i["start_date"]
    )
    assert [i["task_id"] for i in instances] == [f"load_{code}" for code in platforms.LOAD_ORDER]
    assert all(i["state"] == "success" for i in instances)
    for earlier, later in pairwise(instances):
        assert earlier["end_date"] <= later["start_date"], "two loads of the history overlapped"


@pytest.mark.parametrize("code", platforms.LOAD_ORDER)
def test_each_main_table_holds_what_the_generator_made(env, history_run, code):
    now = moment(history_run)
    assert in_warehouse(env, code, now) == expected(code, now)


@pytest.mark.parametrize("code", platforms.LOAD_ORDER)
def test_every_load_of_the_history_is_complete(env, history_run, code):
    dataset = platforms.PLATFORMS[code].dataset
    text = warehouse_output(
        env,
        f"SELECT count(*), count(*) FILTER (WHERE status <> 0) FROM {dataset}._dlt_loads",  # noqa: S608
    )
    loads, failed = map(int, text.split("|"))
    assert loads > 0
    assert failed == 0


def test_a_repeated_history_changes_no_row(env, airflow_url, token, history_run):
    # A second load as of a later moment replaces the days of each account that it covers, and
    # the days of the earlier history before them stay: the main tables hold what the generator
    # makes as of the later moment, without a duplicate.
    run = run_dag(airflow_url, token, HISTORY, timeout=900)
    assert run["state"] == "success", run["state"]
    later = moment(run)
    for code in platforms.LOAD_ORDER:
        assert in_warehouse(env, code, later) == expected(code, later), code


def test_the_names_of_campaigns_have_their_versions(env, history_run):
    # The first history replayed the names of the campaigns into the empty dataset, as versions of
    # scd2; the later runs of the history replay none.
    text = warehouse_output(
        env,
        "SELECT campaign__name || '|' || _dlt_valid_from::date || '|' "
        "|| coalesce(_dlt_valid_to::date::text, '') FROM raw_demo_search.campaigns "
        "WHERE campaign__id = '21000000102' ORDER BY _dlt_valid_from",
    )
    versions = [line.split("|") for line in text.splitlines()]
    assert len(versions) >= 3, "the half-yearly renames of the history are not versions"
    assert versions[-1][2] == "", "the newest version is not the current one"
    assert len({valid_from for _, valid_from, _ in versions}) == len(versions), "a replayed version"
    names = {name for name, _, _ in versions}
    assert names == {"Prospecting - Spring Range", "Prospecting - Autumn Range"}


# The columns of raw that staging reads, with the types that the hints of the resources give and
# that dlt 1.31.0 writes: the day of each fact table, the money of Google as a string of micros, and
# the windows of the actions of Meta in its child tables, as dlt names them. dlt writes its type
# text as varchar, which PostgreSQL reports as character varying. The unit tests load with a
# stand-in of dlt, so only a load with dlt itself shows that each hint reaches its column.
RAW_COLUMNS = (
    "raw_demo_fx.rates_daily.rate_date:date",
    "raw_demo_search.campaign_conversions.segments__date:date",
    "raw_demo_search.campaign_stats.metrics__cost_micros:character varying",
    "raw_demo_search.campaign_stats.segments__date:date",
    "raw_demo_social.insights_campaign.date_start:date",
    "raw_demo_social.insights_campaign__action_values._7d_click:character varying",
    "raw_demo_social.insights_campaign__actions._1d_click:character varying",
    "raw_demo_social.insights_campaign__actions._1d_view:character varying",
    "raw_demo_social.insights_campaign__actions._7d_click:character varying",
    "raw_demo_social.insights_campaign__actions._dlt_root_id:character varying",
    "raw_demo_video.report_campaign_daily.dimensions__stat_time_day:character varying",
    "raw_demo_web.report_sessions_daily.date:character varying",
)


def test_the_raw_tables_have_the_columns_that_staging_reads(env, history_run):
    names = ", ".join(f"'{column.split(':')[0]}'" for column in RAW_COLUMNS)
    text = warehouse_output(
        env,
        "SELECT table_schema || '.' || table_name || '.' || column_name || ':' || data_type "  # noqa: S608 (constants of this file)
        "FROM information_schema.columns "
        f"WHERE table_schema || '.' || table_name || '.' || column_name IN ({names})",
    )
    assert sorted(text.splitlines()) == sorted(RAW_COLUMNS)


def test_a_row_that_the_platform_no_longer_sends_is_gone_after_the_next_load(
    env, airflow_url, token, history_run
):
    # A row inside the window of an account that the next load does not bring, as a campaign that
    # a platform stops reporting after a correction: the scope of the load deletes it, which a
    # merge on the keys would not.
    [(account, last)] = [
        (a.account_id, common.last_final_day(moment(history_run), a.time_zone))
        for a in platforms.PLATFORMS["demo_search"].module.accounts()
        if a.account_id == "4170000011"
    ]
    stale = (
        "INSERT INTO raw_demo_search.campaign_stats (customer__id, campaign__id, "  # noqa: S608 (values of this test)
        "campaign__name, campaign__resource_name, metrics__impressions, metrics__clicks, "
        "metrics__cost_micros, metrics__conversions, metrics__conversions_value, segments__date, "
        "_dlt_load_id, _dlt_id) "
        f"VALUES ('{account}', '21099999999', 'Stale by a test', "
        f"'customers/{account}/campaigns/21099999999', '10', '1', '1000000', 0, 0, "
        f"'{last.isoformat()}', 'stack-test', 'stack-test-stale-row')"
    )
    warehouse_output(env, stale)
    run = run_dag(airflow_url, token, HISTORY, timeout=900)
    assert run["state"] == "success", run["state"]
    left = warehouse_output(
        env,
        "SELECT count(*) FROM raw_demo_search.campaign_stats "
        "WHERE _dlt_id = 'stack-test-stale-row'",
    )
    assert int(left) == 0, "the load kept a row that it no longer brings"


def test_the_history_leaves_out_the_day_that_the_demo_misses(env, history_run):
    [(account, day)] = platforms.gap(moment(history_run))
    count = warehouse_output(
        env,
        "SELECT count(*) FROM raw_demo_social.insights_campaign "  # noqa: S608 (values of the demo)
        f"WHERE account_id = '{account}' AND date_start = DATE '{day.isoformat()}'",
    )
    assert int(count) == 0


def test_loader_owns_the_tables_of_the_history(env, history_run):
    schemas = ", ".join(
        f"'{p.dataset}', '{p.dataset}_staging'" for p in platforms.PLATFORMS.values()
    )
    text = warehouse_output(
        env,
        f"SELECT DISTINCT tableowner FROM pg_tables WHERE schemaname IN ({schemas})",  # noqa: S608
    )
    assert text.split() == ["loader"]


# The columns of _agoranomos_reloads and the types that PostgreSQL reports for them: dlt writes
# its type text as varchar and its timestamp with a time zone. The hints of the journal, which every
# source shares, make the columns of the status and of the error also where no row fills them.
RELOAD_COLUMNS = {
    "account_id": "character varying",
    "as_of": "date",
    "window_start": "date",
    "window_end": "date",
    "now": "timestamp with time zone",
    "loaded_at": "timestamp with time zone",
    "status": "character varying",
    "error": "character varying",
}


def test_each_load_of_the_history_records_the_window_of_each_account(env, history_run):
    # What a later load reads to know how far each account has been loaded: the window of each
    # account, and the day of the load in the time zone of the account, with the status loaded: a
    # load of the demo fails whole or not at all.
    now = moment(history_run)
    for code in platforms.LOAD_ORDER:
        platform = platforms.PLATFORMS[code]
        windows = platform.module.windows(now)
        text = warehouse_output(
            env,
            "SELECT account_id, as_of, window_start, window_end, status "  # noqa: S608 (names of the demo)
            f"FROM {platform.dataset}._agoranomos_reloads "
            f"WHERE now = TIMESTAMPTZ '{now.isoformat()}'",
        )
        wanted = [
            f"{a.account_id}|{common.local_today(now, a.time_zone)}|"
            f"{windows[a.account_id][0]}|{windows[a.account_id][1]}|loaded"
            for a in sorted(platform.module.accounts(), key=lambda a: a.account_id)
        ]
        # Sorted here, not by the collation of the warehouse.
        assert sorted(text.splitlines()) == sorted(wanted), code
        kinds = warehouse_output(
            env,
            "SELECT column_name || ':' || data_type FROM information_schema.columns "  # noqa: S608
            f"WHERE table_schema = '{platform.dataset}' AND table_name = '_agoranomos_reloads' "
            "AND column_name NOT LIKE '\\_dlt\\_%'",
        )
        # Without the columns of dlt, _dlt_load_id and _dlt_id.
        found = dict(line.split(":") for line in kinds.splitlines())
        assert found == RELOAD_COLUMNS, code


def test_each_load_of_the_history_updates_its_asset_and_logs_no_secret(
    env, airflow_url, token, history_run
):
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    run_id = history_run["dag_run_id"]
    for code in platforms.LOAD_ORDER:
        uri = platforms.asset_uri(platforms.PLATFORMS[code])
        events = asset_events(airflow_url, token, uri, source_dag_id=HISTORY, source_run_id=run_id)
        assert events, f"the load of {code} did not update {uri}"
    for instance in task_instances(airflow_url, token, history_run):
        # The line that only the load writes, for each table: "Loaded <n> rows into <dataset>...".
        dataset = platforms.PLATFORMS[instance["task_id"].removeprefix("load_")].dataset
        said = any(
            f" rows into {dataset}." in (json.loads(entry).get("event") or "")
            for entry in task_log(airflow_url, token, instance)
        )
        assert said, f"the log of {instance['task_id']} does not say what it loaded"
        found = leaked(values, task_log(airflow_url, token, instance))
        assert not found, f"the log of {instance['task_id']} holds the values of {found}"


def run_daily(airflow_url, token):
    """A run of the daily load now, which the tests keep paused otherwise (conftest.py), as the
    history: its moment, run_after, is the moment of every load in it. A run that its schedule
    makes when it is unpaused, for a time that passed while it was paused, ends too, before it is
    paused again."""
    wait_for_dag(airflow_url, token, DAILY)
    unpause(airflow_url, token, DAILY)
    try:
        run = trigger(airflow_url, token, DAILY)
        run = wait_for_run(airflow_url, token, DAILY, run["dag_run_id"], timeout=900)
        wait_for(
            lambda: not is_busy(airflow_url, token, DAILY),
            900,
            interval=5,
            message=f"the runs of {DAILY} did not end",
        )
    finally:
        pause(airflow_url, token, DAILY)
    return run


def newest_loads(env):
    """The day of the newest load of each account of each platform, as _agoranomos_reloads records
    it."""
    found = {}
    for code in platforms.LOAD_ORDER:
        dataset = platforms.PLATFORMS[code].dataset
        text = warehouse_output(
            env,
            f"SELECT account_id, max(as_of) FROM {dataset}._agoranomos_reloads GROUP BY account_id",  # noqa: S608
        )
        for line in text.splitlines():
            account_id, as_of = line.split("|")
            found[(code, account_id)] = datetime.fromisoformat(as_of).date()
    return found


def states_of(airflow_url, token, run):
    return {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, run)}


def test_a_daily_load_after_the_history_skips_its_loads(env, airflow_url, token, history_run):
    # The history took the day of every account, so the task due skips the five loads, which
    # update no asset. Should a day of an account begin in between, the loads run.
    loaded = newest_loads(env)
    run = run_daily(airflow_url, token)
    assert run["state"] == "success", run["state"]
    now = moment(run)
    due = any(
        loaded[(code, a.account_id)] < common.local_today(now, a.time_zone)
        for code in platforms.LOAD_ORDER
        for a in platforms.PLATFORMS[code].module.accounts()
    )
    loads = "success" if due else "skipped"
    wanted = {"due": "success", **{f"load_{code}": loads for code in platforms.LOAD_ORDER}}
    assert states_of(airflow_url, token, run) == wanted
    if not due:
        for code in platforms.LOAD_ORDER:
            uri = platforms.asset_uri(platforms.PLATFORMS[code])
            events = asset_events(
                airflow_url, token, uri, source_dag_id=DAILY, source_run_id=run["dag_run_id"]
            )
            assert not events, f"a skipped load updated {uri}"


# How many days demo_web lags in the test below: longer than its window of 14 days.
LAG_DAYS = 40


def test_a_daily_load_brings_every_day_after_the_last_load(env, airflow_url, token, history_run):
    # As after a stand that was off for 40 days: the rows of demo_web, and the record of its loads,
    # end 40 days before yesterday. The daily load brings every day after the last of each account
    # and the window of each other platform, in the pool of the loads of the demo, and the main
    # tables hold what the generator makes, without a hole or a duplicate.
    web = platforms.PLATFORMS["demo_web"]
    ends = dict(
        line.split("|")
        for line in warehouse_output(
            env,
            "SELECT account_id, max(window_end) FROM raw_demo_web._agoranomos_reloads "
            "GROUP BY account_id",
        ).splitlines()
    )
    lagged = {
        account_id: datetime.fromisoformat(end).date() - timedelta(days=LAG_DAYS)
        for account_id, end in ends.items()
    }
    for account_id, end in lagged.items():
        warehouse_output(
            env,
            "DELETE FROM raw_demo_web.report_sessions_daily "  # noqa: S608 (values of the demo)
            f"WHERE property = 'properties/{account_id}' "
            f"AND to_date(date, 'YYYYMMDD') > DATE '{end.isoformat()}'",
        )
    warehouse_output(
        env,
        "UPDATE raw_demo_web._agoranomos_reloads "  # noqa: S608 (a constant of this file)
        f"SET as_of = as_of - {LAG_DAYS}, window_end = window_end - {LAG_DAYS}",
    )
    run = run_daily(airflow_url, token)
    assert run["state"] == "success", run["state"]
    wanted = {"due": "success", **{f"load_{code}": "success" for code in platforms.LOAD_ORDER}}
    assert states_of(airflow_url, token, run) == wanted
    pools = {i["task_id"]: i["pool"] for i in task_instances(airflow_url, token, run)}
    assert pools == {
        "due": "default_pool",
        **{f"load_{c}": "demo_load" for c in platforms.LOAD_ORDER},
    }
    later = moment(run)
    # The day that the newest history left out stays out: a daily load never reaches it.
    [history] = dag_runs(airflow_url, token, HISTORY, limit=1)
    for code in platforms.LOAD_ORDER:
        platform = platforms.PLATFORMS[code]
        text = warehouse_output(
            env,
            "SELECT account_id, window_start, window_end "  # noqa: S608 (names of the demo)
            f"FROM {platform.dataset}._agoranomos_reloads "
            f"WHERE now = TIMESTAMPTZ '{later.isoformat()}'",
        )
        windows = {
            account_id: (datetime.fromisoformat(start).date(), datetime.fromisoformat(end).date())
            for account_id, start, end in (line.split("|") for line in text.splitlines())
        }
        # Each window ends on yesterday of its account, as the history's does.
        yesterdays = {a: end for a, (_, end) in platform.module.windows(later).items()}
        assert {a: end for a, (_, end) in windows.items()} == yesterdays, code
        for account_id, (start, end) in windows.items():
            if platform is web:
                assert start == lagged[account_id] + timedelta(days=1), account_id
            else:
                assert (end - start).days + 1 == platform.window_days, (code, account_id)
        assert in_warehouse(env, code, later) == expected(code, later, moment(history)), code
        uri = platforms.asset_uri(platform)
        events = asset_events(
            airflow_url, token, uri, source_dag_id=DAILY, source_run_id=run["dag_run_id"]
        )
        assert events, f"the load of {code} did not update {uri}"
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    for instance in task_instances(airflow_url, token, run):
        found = leaked(values, task_log(airflow_url, token, instance))
        assert not found, f"the log of {instance['task_id']} holds the values of {found}"
