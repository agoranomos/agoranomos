"""A missed daily load leaves no day not final (windows.daily_window of
airflow/dags/agoranomos_pipelines/windows.py): the window of the load after it starts a day earlier,
at the oldest day of the window of the missed load, which it reads at the age of the window or more,
and the view marks that day final, is_final, from that load.

The test makes demo_social, with its window of 28 days, look as if its daily loads had missed one
day: as loader, the owner of raw, it moves the last day of every whole load in the journal
raw_demo_social._agoranomos_loads two days back, which is what the daily load reads to count its
window (loads.recorded), runs a daily load of the platform as of the moment of its newest load, in
the container of the tasks and outside Airflow, as tests/stack/test_restatements.py does, and puts
the journal back at once, before any pin reads it. As of that moment the load gives the numbers
that the core holds, so the core is as before but for the load of the rows of its window. For each
account whose newest whole load ended its window on Y, the journal then says Y - 2: the load of
the day after Y - 2 took place, and that of the day after Y - 1, whose window was Y - 28 to Y - 1,
was missed. The window of the new load, to Y, then starts on Y - 28, the oldest day of the window
of the missed load, one day before the usual window, and the run of the transform that follows
accepts it: in marts.ad_stats_campaign_daily, every row of each account on that day comes from
that load, which ended at least 28 days after it, and is final. With the window of ADR-0038 before
24 November 2026, the load started on Y - 27, and the day kept the load that read it at 27 days,
not final for good.

If the test is killed between the move of the journal and its return, add the two days back as
loader: UPDATE raw_demo_social._agoranomos_loads SET window_to = window_to + 2 on the rows whose
window_to is two days before the last that their account had.

The assertions print accounts, days, load IDs and counts, never a secret.
"""

from datetime import date, timedelta

import pytest
from agoranomos_pipelines.windows import restatement_window_days
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
SOURCE = "demo_social"
DATASET = f"raw_{SOURCE}"
# The days of the journal that the missed loads move back: one missed load, as a load of the day
# before would have taken the window to a day before the newest.
MISSED = 1
# A daily load of the platform as of a moment, as agoranomos_demo_daily makes it.
DAILY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

sources.load_daily(sys.argv[2], datetime.fromisoformat(sys.argv[1]))
"""
# The last day of the whole loads of each account in the journal, as loads.recorded() reads it.
LAST_DAYS = (
    "SELECT j.account_id, max(j.window_to) FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "WHERE j.status = 'loaded' AND j.kind IS DISTINCT FROM 'intraday' "
    "GROUP BY 1 ORDER BY 1"
)
# The loads of the journal, which the test moves back and then forward again.
JOURNAL_LOADS = (
    "SELECT string_agg(DISTINCT quote_literal(_dlt_load_id), ',') FROM {dataset}._agoranomos_loads"
)
MOVE = (
    "UPDATE {dataset}._agoranomos_loads SET window_to = window_to {sign} {days} "
    "WHERE _dlt_load_id IN ({loads})"
)
# The window of each account in the journal of a load.
WINDOW = (
    "SELECT account_id, window_from, window_to FROM {dataset}._agoranomos_loads "
    "WHERE _dlt_load_id = '{load}' AND status = 'loaded' ORDER BY account_id"
)
# The rows of the view of the campaigns of an account on a day: how many, how many final, and how
# many from the load, which ended at least the window after the day, by the time zone of the
# account.
DAY = (
    "SELECT count(*), count(*) FILTER (WHERE v.is_final), "
    "count(*) FILTER (WHERE v.loaded_at = l.finished_at "
    "AND (l.finished_at AT TIME ZONE v.time_zone)::date - v.report_date >= {window}) "
    "FROM marts.ad_stats_campaign_daily AS v "
    "JOIN core.loads AS l ON l.source = v.platform AND l.account_id = v.account_id "
    "AND l.load_id = '{load}' "
    "WHERE v.platform = '{source}' AND v.account_id = '{account}' AND v.report_date = DATE '{day}'"
)
PINNED = "SELECT count(*) FROM core.loads WHERE status = 'pinned'"


def rows(env, sql, role="postgres"):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql, role).splitlines()]


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    return run


@pytest.fixture(scope="module")
def missed(env, airflow_url, token):
    """A daily load of demo_social after a missed one, and the run of the transform that accepted
    it: the window of restatements, the last day of each account before the load, the load, and
    the window of each account in its journal."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    # Every whole load in core.loads, so that no pin reads the journal while it is moved.
    if warehouse_output(env, PINNED) != "0":
        transform(airflow_url, token)
    assert warehouse_output(env, PINNED) == "0", "loads stay pinned after a run of the transform"
    width = restatement_window_days(SOURCE)
    last = {
        account: date.fromisoformat(day)
        for account, day in rows(env, LAST_DAYS.format(dataset=DATASET), "loader")
    }
    assert last, f"{DATASET} has no whole load"
    loads = warehouse_output(env, JOURNAL_LOADS.format(dataset=DATASET), "loader")
    now = last_moment(env, SOURCE)
    move = {"dataset": DATASET, "days": MISSED + 1, "loads": loads}
    warehouse_output(env, MOVE.format(sign="-", **move), "loader")
    try:
        run_in_tasks(env, DAILY, now.isoformat(), SOURCE)
    finally:
        warehouse_output(env, MOVE.format(sign="+", **move), "loader")
    assert rows(env, LAST_DAYS.format(dataset=DATASET), "loader") == [
        (account, day.isoformat()) for account, day in last.items()
    ], "the journal is not as it was"
    load = warehouse_output(env, NEWEST_LOAD.format(dataset=DATASET))
    windows = {
        account: (date.fromisoformat(first), date.fromisoformat(end))
        for account, first, end in rows(env, WINDOW.format(dataset=DATASET, load=load), "loader")
    }
    transform(airflow_url, token)
    assert warehouse_output(env, PINNED) == "0", "the run of the transform accepted no load"
    return width, last, load, windows


def test_the_load_after_a_missed_one_starts_a_day_earlier(missed):
    width, last, _, windows = missed
    assert windows.keys() == last.keys()
    for account, day in last.items():
        first, end = windows[account]
        # The journal said day - MISSED - 1, the last day of the load before the missed one.
        last_end = day - timedelta(days=MISSED + 1)
        usual = end - timedelta(days=width - 1)
        assert first == min(usual, last_end + timedelta(days=2 - width)), (account, first, end)
        assert first < usual, (account, first, usual)


def test_the_oldest_day_of_the_missed_window_is_final(env, missed):
    width, _, load, windows = missed
    for account, (oldest_of_the_missed, _) in windows.items():
        found, final, from_the_load = map(
            int,
            warehouse_output(
                env,
                DAY.format(
                    window=width,
                    load=load,
                    source=SOURCE,
                    account=account,
                    day=oldest_of_the_missed,
                ),
            ).split("|"),
        )
        assert found > 0, f"{account} has no campaign on {oldest_of_the_missed}"
        assert final == from_the_load == found, (account, oldest_of_the_missed, load, found)
