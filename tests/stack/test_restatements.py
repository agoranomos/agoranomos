"""The view of restatements, ops.ops_restatements (ADR-0036, ADR-0038): how much the loads that the
core accepted changed the days that they replaced, by platform, kind of the load and age of the
day, from the journal of restatements of the campaigns, and the card of the Ops collection that
shows it.

The test compares the view before and after a run of its own, so a load that an earlier test left
for the core to accept goes in with a run of the transform first. It then makes a daily load of
demo_search and of demo_social as of the moment of their newest load, in the container of the tasks
and outside Airflow, as raw_loads.py does: their usual windows, 90 and 28 days up to yesterday of
each account, which the history took, with the numbers that the core holds, as a load of the demo as
of the same moment gives the same rows. Before the run of the transform that reads them, it changes
by hand, in the core, the spend and the conversions of one campaign of one account of demo_search on
the first and on the last day of its window, as the superuser, so that the load changes those rows
back and the journal keeps the rows as the test changed them. Each is a row that no journal holds
yet: the view takes the new value of a row of the journal from the next row of the same campaign and
day, which a change by hand breaks, so a second change of the same row would cancel the change of
the load before it, as the second run of the tests on the same stack does on a push to main and
every week. The run accepts the loads, as a daily run does, and the core is as before; the journal
keeps its rows, as it keeps every restatement. If the test is killed between the change and the run,
run agoranomos_demo_transform by hand: it reads the loads and writes the rows back.

The assertions print platforms, ages, days, counts and sums, never a secret.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import ops
import pytest
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from raw_loads import NEWEST_LOAD, last_moment, run_in_tasks
from stack_helpers import wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
# The platforms of the test and the days of their usual windows (agoranomos_demo_daily.py).
WINDOWS = {"demo_search": 90, "demo_social": 28}
# The account of demo_search whose campaigns the test changes by hand, in USD.
SOURCE = "demo_search"
ACCOUNT = "4170000011"
# A daily load of a platform of the demo as of a moment, as agoranomos_demo_daily makes it: for
# each account, its usual window up to yesterday, as the last load took yesterday already.
DAILY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

sources.load_daily(sys.argv[2], datetime.fromisoformat(sys.argv[1]))
"""
# The rows of the view of a platform for the loads of the usual window, by age.
VIEW = (
    "SELECT age_days, replaced_days, changed_days, changed_rows, vanished_rows, spend_shift, "
    "conversions_shift, spend_change_reporting FROM ops.ops_restatements "
    "WHERE platform = '{platform}' AND load_kind = 'window' ORDER BY age_days"
)
# The campaign with the most spend of the account on a day whose row no journal holds, and the
# currency of the row. Three campaigns of the account run every day, so a second run of the tests
# finds one.
TOP_CAMPAIGN = (
    "SELECT f.campaign_id, f.currency FROM core.ad_stats_campaign_daily AS f "  # noqa: S608 (constants)
    "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
    f"WHERE a.platform = '{SOURCE}' AND a.account_id = '{ACCOUNT}' "
    "AND f.report_date = DATE '{day}' AND NOT EXISTS ("
    "SELECT FROM core.ad_stats_campaign_daily_restatements AS j "
    "WHERE j.account_sk = f.account_sk AND j.campaign_id = f.campaign_id "
    "AND j.report_date = f.report_date) "
    "ORDER BY f.spend DESC, f.campaign_id LIMIT 1"
)
CHANGE = (
    "UPDATE core.ad_stats_campaign_daily AS f "  # noqa: S608 (constants of this file)
    "SET spend = f.spend + 1, conversions = coalesce(f.conversions, 0) + 2 "
    "FROM core.dim_account AS a WHERE a.account_sk = f.account_sk "
    f"AND a.platform = '{SOURCE}' AND a.account_id = '{ACCOUNT}' "
    "AND f.campaign_id = {campaign} AND f.report_date = DATE '{day}'"
)
# The window of each account in the journal of a load in raw, as loader, the owner of raw.
WINDOW = (
    "SELECT account_id, window_from, window_to FROM {dataset}._agoranomos_loads "
    "WHERE _dlt_load_id = '{load}' AND status = 'loaded' ORDER BY account_id"
)
# When each account of a load ended, in UTC, as the pin copied it into core.loads, with the time
# zone of the account and the status of the load.
ENDED = (
    "SELECT l.account_id, "
    "to_char(l.finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS'), "
    "coalesce(a.time_zone, 'UTC'), l.status, l.kind FROM core.loads AS l "
    "JOIN core.dim_account AS a ON a.platform = l.source AND a.account_id = l.account_id "
    "WHERE l.source = '{source}' AND l.load_id = '{load}' ORDER BY l.account_id"
)


def rows(env, sql, role="postgres"):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql, role).splitlines()]


def view(env, platform):
    """The rows of the view of a platform for the loads of the usual window, by age."""
    return {int(r[0]): r[1:] for r in rows(env, VIEW.format(platform=platform), "metabase_reader")}


def unaccepted(env, platform) -> bool:
    """Whether the core has not accepted the newest whole load of a platform, or holds a load of
    it that is still pinned."""
    newest = warehouse_output(env, NEWEST_LOAD.format(dataset=f"raw_{platform}"))
    found = warehouse_output(
        env,
        "SELECT count(*) FILTER (WHERE status = 'pinned'), "  # noqa: S608 (values of this test)
        f"count(*) FILTER (WHERE load_id = '{newest}' AND status = 'accepted') "
        f"FROM core.loads WHERE source = '{platform}'",
    )
    pinned, accepted = map(int, found.split("|"))
    return pinned > 0 or (bool(newest) and accepted == 0)


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    return wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)


@pytest.fixture(scope="module")
def replaced(env, airflow_url, token):
    """A daily load of each platform of the test as of its newest load, two rows of the core
    changed by hand, and the run of the transform that read the loads and accepted them: the
    view of each platform before and after, the windows of the loads, when each account of them
    ended, and the days, campaigns and currencies of the rows changed by hand."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    # A load that no run has accepted yet would be accepted by the run of this test and count in
    # the view after it: a run first, if an earlier test left one.
    if any(unaccepted(env, platform) for platform in WINDOWS):
        run = transform(airflow_url, token)
        assert run["state"] == "success", run["state"]
        assert not any(unaccepted(env, platform) for platform in WINDOWS)
    before = {platform: view(env, platform) for platform in WINDOWS}
    loads = {}
    windows = {}
    for platform in WINDOWS:
        now = last_moment(env, platform)
        run_in_tasks(env, DAILY, now.isoformat(), platform)
        dataset = f"raw_{platform}"
        loads[platform] = warehouse_output(env, NEWEST_LOAD.format(dataset=dataset))
        windows[platform] = {
            account: (date.fromisoformat(first), date.fromisoformat(last))
            for account, first, last in rows(
                env, WINDOW.format(dataset=dataset, load=loads[platform]), "loader"
            )
        }
    first, last = windows[SOURCE][ACCOUNT]
    changed = {}
    done = False
    try:
        for day in (first, last):
            [(campaign, currency)] = rows(env, TOP_CAMPAIGN.format(day=day))
            warehouse_output(env, CHANGE.format(campaign=campaign, day=day))
            changed[day] = (campaign, currency.strip())
        run = transform(airflow_url, token)
        done = True
    finally:
        if changed and not done:
            transform(airflow_url, token)
    assert run["state"] == "success", run["state"]
    after = {platform: view(env, platform) for platform in WINDOWS}
    ended = {
        platform: rows(env, ENDED.format(source=platform, load=loads[platform]))
        for platform in WINDOWS
    }
    return before, after, windows, ended, changed


def local_day(moment: str, time_zone: str) -> date:
    return datetime.fromisoformat(moment).replace(tzinfo=UTC).astimezone(ZoneInfo(time_zone)).date()


def ages(windows, ended, platform):
    """The age of each day of the window of each account of the load of a platform: the day on
    which the load ended, in the time zone of the account, minus the day; how many days of an
    account have each age."""
    found: dict[int, int] = {}
    for account, moment, time_zone, _, _ in ended[platform]:
        first, last = windows[platform][account]
        load_day = local_day(moment, time_zone)
        for age in range((load_day - last).days, (load_day - first).days + 1):
            found[age] = found.get(age, 0) + 1
    return found


def test_the_loads_are_daily_loads_that_the_core_accepted(replaced):
    _, _, windows, ended, _ = replaced
    for platform, days in WINDOWS.items():
        assert ended[platform], platform
        assert {(r[3], r[4]) for r in ended[platform]} == {("accepted", "window")}, platform
        for account, (first, last) in windows[platform].items():
            assert (last - first).days + 1 == days, (platform, account)


@pytest.mark.parametrize("platform", WINDOWS)
def test_the_view_counts_each_day_of_the_window_as_replaced_at_its_age(replaced, platform):
    # The history took every day of the usual window, so each is a replaced day of the load of
    # the test, at its age: 1 to 90 on demo_search and 1 to 28 on demo_social, a day older for
    # each midnight of the account between the moment as of which the test made the load and the
    # end of the load, as at 23:00 UTC in winter for an account in Europe/Berlin.
    before, after, windows, ended, _ = replaced
    expected = ages(windows, ended, platform)
    assert min(expected) >= 1, sorted(expected)
    assert set(expected) <= set(after[platform]), sorted(set(expected) - set(after[platform]))
    for age, accounts in expected.items():
        was = int(before[platform][age][0]) if age in before[platform] else 0
        assert int(after[platform][age][0]) - was == accounts, (platform, age)


@pytest.mark.parametrize("platform", WINDOWS)
def test_a_day_that_the_load_brought_back_the_same_counts_as_unchanged(replaced, platform):
    # As of the moment of the newest load, the demo gives the numbers that the core holds, so the
    # load changes only the rows that the test changed by hand.
    before, after, windows, ended, changed = replaced
    hand = (
        {
            age
            for account, moment, time_zone, _, _ in ended[platform]
            if account == ACCOUNT
            for age in ((local_day(moment, time_zone) - day).days for day in changed)
        }
        if platform == SOURCE
        else set()
    )
    for age in ages(windows, ended, platform):
        was = before[platform].get(age, ("0", "0", "0", "0"))
        now = after[platform][age]
        moved = [int(now[i]) - int(was[i]) for i in (1, 2, 3)]
        assert moved == ([1, 1, 0] if age in hand else [0, 0, 0]), (platform, age)


def test_the_shift_of_a_row_changed_by_hand_is_its_change(env, replaced):
    # The load wrote back the spend and the conversions that the test had raised by 1 and 2: the
    # journal keeps the raised row, so the change, new minus old, is -1 of spend, in the reporting
    # currency at the rate of its day, and the shifts of its age are no longer 0.
    before, after, _, ended, changed = replaced
    [(moment, time_zone)] = [(r[1], r[2]) for r in ended[SOURCE] if r[0] == ACCOUNT]
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    for day, (_, currency) in changed.items():
        age = (local_day(moment, time_zone) - day).days
        rate = Decimal(
            warehouse_output(
                env,
                "SELECT rate_into_reporting_currency FROM core.fx_rates_daily "  # noqa: S608 (values of this test)
                f"WHERE rate_date = DATE '{day}' AND currency = '{currency}' "
                f"AND reporting_currency = '{reporting}'",
            )
        )
        was = before[SOURCE].get(age, ("0",) * 7)
        now = after[SOURCE][age]
        moved = Decimal(now[6]) - Decimal(was[6])
        # Each sum is rounded to six places once, so the difference of two can be off by one in
        # the last place.
        assert abs(moved + rate) <= Decimal("0.000002"), (age, moved, -rate)
        assert Decimal(now[4]) > 0, (age, "spend_shift")
        assert Decimal(now[5]) > 0, (age, "conversions_shift")


def test_the_core_holds_the_rows_of_the_load_again(env, replaced):
    # The load wrote back the rows that the test had changed, and the journal of the campaigns
    # holds them as the test left them, with the load that replaced them.
    _, _, _, _, changed = replaced
    for day, (campaign, _) in changed.items():
        [(spend, held)] = rows(
            env,
            "SELECT f.spend, j.spend FROM core.ad_stats_campaign_daily AS f "  # noqa: S608 (values of this test)
            "JOIN core.ad_stats_campaign_daily_restatements AS j "
            "ON j.account_sk = f.account_sk AND j.campaign_id = f.campaign_id "
            "AND j.report_date = f.report_date AND j.new_load_sk = f.load_sk "
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = '{SOURCE}' AND a.account_id = '{ACCOUNT}' "
            f"AND f.campaign_id = {campaign} AND f.report_date = DATE '{day}' "
            "AND NOT j.is_vanished",
        )
        assert Decimal(held) - Decimal(spend) == 1, (day, campaign)


def test_the_card_of_the_restatements_answers_in_under_2_seconds(env, replaced):
    # As the cards of ADR-0037: the query of the card, as metabase_reader, on the demo.
    [question] = [q for q in ops.QUESTIONS if q["key"] == "restatements"]
    plan = json.loads(
        warehouse_output(
            env, f"EXPLAIN (ANALYZE, FORMAT JSON) {question['query']}", "metabase_reader"
        )
    )
    took = plan[0]["Planning Time"] + plan[0]["Execution Time"]
    assert took < 2000, f"{took:.0f} ms"
    answer = rows(env, question["query"], "metabase_reader")
    assert {r[1].split(": ")[0] for r in answer} >= set(WINDOWS), "a platform has no line"
