"""The short cycle of the current day of the demo (ADR-0051), whole: a run of the DAG
agoranomos_demo_intraday loads the current day of each account of demo_search so far, and the day
before while no whole load covers it, as a load of the kind intraday; its asset starts the
transform, which writes those days into core.ad_stats_campaign_daily as partial days, as the demo
generator gives them for the moment of the run, and into no other fact; the daily load of the
demo, the next morning of the day before, replaces that day whole, with no row of the journal of
restatements, and leaves today partial. With AGORANOMOS_INTRADAY_HOURS=0 the DAG has no schedule.

The stack of the tests has loaded the demo up to yesterday of each account, so the day before is
whole already. To have it partial, as between midnight in the account and the daily load, the test
first makes the core forget yesterday of the two accounts of demo_search, as the superuser: the
rows of yesterday in the four facts of the ad platforms go, and the whole loads whose windows end on
yesterday end the day before, in core.loads and in the journal of raw_demo_search. The run of the
short cycle then takes yesterday and today, and the daily load of demo_search, which the test runs
in the container of the tasks beside the run of the transform of the short cycle, outside Airflow,
as raw_loads.py does, takes yesterday back whole; it ends during that run, so it waits for the next
one, which the test runs by hand. The finally deletes the loads of the short cycle of the test from
raw_demo_search, _dlt_loads, the journal and core.loads, and today from the fact of campaigns,
gives the dimensions back the last day of statistics that they held, or the last whole day of the
core when its daily load took a later one, as across a midnight of an account, and the whole loads
their windows; if the daily load did not replace yesterday, it loads and transforms once more, so
that the demo is as before. If the test is killed before that, run agoranomos_demo_daily and the
transform by hand, and delete what the finally deletes: the rows of the kind intraday of
raw_demo_search._agoranomos_loads, of _dlt_loads and of campaign_stats whose load ID is newer than
the start of the test, those loads in core.loads, and the rows of today of the two accounts in
core.ad_stats_campaign_daily.

The test measures the short cycle on the runner (row 13 of the register of the audit): the time of
the run of the transform that holds only the load of the short cycle, the peak of memory of each
container while it runs with the daily load beside it (scripts/memory_report.py), and the bytes
that elementary adds to the schema ops in that run and in the run of the daily load. It prints
them, and appends them to $GITHUB_STEP_SUMMARY in CI.

The assertions print IDs, days, counts and sums, never a secret.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import memory_report
import pytest
from agoranomos_pipelines import intraday
from agoranomos_pipelines.demo import common, platforms, search
from airflow_api import (
    asset_events,
    dag_runs,
    is_busy,
    is_paused,
    pause,
    task_instances,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from raw_loads import DAGS, run_in_tasks
from stack_helpers import compose, ops_growth, ops_sizes, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

INTRADAY = "agoranomos_demo_intraday"
TRANSFORM = "agoranomos_demo_transform"
ASSET = "agoranomos_demo_intraday"
SOURCE = "demo_search"
ACCOUNTS = tuple(account.account_id for account in search.accounts())
ZONES = {account.account_id: account.time_zone for account in search.accounts()}
LISTED = ", ".join(f"'{account_id}'" for account_id in ACCOUNTS)
# The events of the five platforms of the demo, which start a run of the transform together.
ASSETS = 5
SKS = (
    "SELECT account_id, account_sk FROM core.dim_account "  # noqa: S608 (constants of the demo)
    f"WHERE platform = '{SOURCE}' AND account_id IN ({LISTED})"
)
# The last day of the whole loads of each account, which the core accepted.
LAST_DAYS = (
    "SELECT account_id, max(window_to) FROM core.loads "  # noqa: S608 (constants of the demo)
    f"WHERE source = '{SOURCE}' AND account_id IN ({LISTED}) AND outcome = 'loaded' "
    "AND kind <> 'intraday' GROUP BY account_id"
)
# The rows of the campaigns of an account on a day: campaign, impressions, clicks, spend, whether
# partial, the load and its kind, and whether the row has conversions.
CAMPAIGNS = (
    "SELECT f.campaign_id, f.impressions, f.clicks, f.spend, f.is_partial_day, l.load_id, l.kind, "
    "f.conversions IS NOT NULL OR f.conversion_value IS NOT NULL OR f.all_conversions IS NOT NULL "
    "FROM core.ad_stats_campaign_daily AS f JOIN core.loads AS l ON l.load_sk = f.load_sk "
    "WHERE f.account_sk = {sk} AND f.report_date = DATE '{day}' ORDER BY 1"
)
# The facts of the ad platforms but that of campaigns.
OTHER_FACTS = (
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_conversions_campaign_daily",
)
FACTS = ("ad_stats_campaign_daily", *OTHER_FACTS)
# The loads of the kind intraday of raw_demo_search newer than a moment, as a time in seconds: the
# account, the window, the moment of the load and the load ID.
INTRADAY_LOADS = (
    "SELECT j.account_id, j.window_from, j.window_to, "
    "to_char(j.now AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"'), j._dlt_load_id "
    "FROM raw_demo_search._agoranomos_loads AS j "
    "JOIN raw_demo_search._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "WHERE j.kind = 'intraday' AND j._dlt_load_id::numeric >= {since} "
    "ORDER BY j._dlt_load_id::numeric, j.account_id"
)
# The newest whole load of raw_demo_search newer than a moment, and its window of each account.
WHOLE_LOAD = (
    "SELECT j.account_id, j.window_from, j.window_to, j._dlt_load_id "
    "FROM raw_demo_search._agoranomos_loads AS j "
    "JOIN raw_demo_search._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "WHERE j.kind IS DISTINCT FROM 'intraday' AND j._dlt_load_id::numeric >= {since} "
    "ORDER BY j._dlt_load_id::numeric, j.account_id"
)
# The whole loads of the platforms of the demo that the core has not accepted: those that the next
# run of the transform reads.
WAITING = "SELECT " + " + ".join(  # noqa: S608 (names of the demo)
    f"(SELECT count(*) FROM {platform.dataset}._agoranomos_loads AS j "  # noqa: S608
    f"JOIN {platform.dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "WHERE j.status = 'loaded' AND NOT EXISTS (SELECT FROM core.loads AS p "
    f"WHERE p.source = '{code}' AND p.load_id = j._dlt_load_id "
    "AND p.account_id = j.account_id AND p.status = 'accepted'))"
    for code, platform in platforms.PLATFORMS.items()
)
# The sources and kinds of the loads that a run of the transform accepted.
ACCEPTED = (
    "SELECT DISTINCT source, kind FROM core.loads "
    "WHERE accepted_at BETWEEN TIMESTAMPTZ '{start}' AND TIMESTAMPTZ '{end}' ORDER BY 1, 2"
)
# The daily load of demo_search as of a moment, as agoranomos_demo_daily makes it.
DAILY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

sources.load_daily(sys.argv[1], datetime.fromisoformat(sys.argv[2]))
"""
# The schedule of the DAG of the short cycle as its file gives it for the setting of the
# environment, parsed in the container of the DAG processor: the cron of its period, or None, and
# the timetable of the DAG.
SCHEDULE = """
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("demo_intraday_check", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
dag = module.agoranomos_demo_intraday()
print(module.CRON, type(dag.timetable).__name__, sep="|")
"""


def rows(env, sql, role="postgres"):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql, role).splitlines()]


def moment(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def seconds(run) -> float:
    return (moment(run["end_date"]) - moment(run["start_date"])).total_seconds()


def report(capsys, title: str, lines: list[str]) -> None:
    """A table of a measurement, printed in the log of the tests and appended to the summary of
    the job of CI."""
    text = "\n".join([f"### {title}", "", *lines, ""]) + "\n"
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as summary:
            summary.write(text)
    with capsys.disabled():
        print(f"\n{text}")


def campaigns(env, sk, day):
    """The rows of the campaigns of an account on a day, by campaign: impressions, clicks, spend,
    and the rest as CAMPAIGNS gives it."""
    return {
        r[0]: ((int(r[1]), int(r[2]), Decimal(r[3])), r[4:])
        for r in rows(env, CAMPAIGNS.format(sk=sk, day=day))
    }


def generated(records, account_id, day):
    """The impressions, clicks and spend of each campaign of an account on a day in records of
    campaign_stats of the generator."""
    return {
        r["campaign"]["id"]: (
            int(r["metrics"]["impressions"]),
            int(r["metrics"]["clicks"]),
            Decimal(r["metrics"]["costMicros"]) / 1_000_000,
        )
        for r in records
        if r["customer"]["id"] == account_id and r["segments"]["date"] == day.isoformat()
    }


def other_rows(env, sk, day) -> dict[str, int]:
    return {
        fact: int(
            warehouse_output(
                env,
                f"SELECT count(*) FROM core.{fact} WHERE account_sk = {sk} "  # noqa: S608 (names of this test)
                f"AND report_date = DATE '{day}'",
            )
        )
        for fact in OTHER_FACTS
    }


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    return run


def date_or_null(day: str) -> str:
    return f"DATE '{day}'" if day else "NULL"


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, and no load of the demo waits for one, so
    that the run of the short cycle reads its load alone: a run takes what waits first."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, "SELECT to_regclass('staging.int_window_days') IS NOT NULL")
    assert found == "t", "staging.int_window_days does not exist: no transform has run here"
    if warehouse_output(env, WAITING) != "0":
        transform(airflow_url, token)
    assert warehouse_output(env, WAITING) == "0", "loads of the demo wait after a run"


def test_the_setting_gives_the_short_cycle_its_schedule_or_none(env, airflow_url, token):
    # The DAG in Airflow has the schedule of the setting in .env, none at 0, as CI runs the stack;
    # parsed in the container of the DAG processor, its file gives the schedule of the other value
    # too (agoranomos_pipelines/intraday.py).
    hours = intraday.period(env.get("AGORANOMOS_INTRADAY_HOURS") or "")
    body = wait_for_dag(airflow_url, token, INTRADAY)
    summary = body.get("timetable_summary")
    if hours == 0:
        assert body.get("next_dagrun_run_after") is None, body.get("next_dagrun_run_after")
        assert (summary or "None") == "None", summary
    else:
        assert summary == intraday.cron(hours), summary
    for value, wanted in (("0", "None|NullTimetable"), ("3", "27 */3 * * *|CronTriggerTimetable")):
        result = compose(
            "exec",
            "-T",
            "-e",
            f"PYTHONPATH={DAGS}",
            "-e",
            f"AGORANOMOS_INTRADAY_HOURS={value}",
            "airflow-dag-processor",
            "python",
            "-c",
            SCHEDULE,
            f"{DAGS}/agoranomos_demo_intraday.py",
            check=False,
        )
        assert result.returncode == 0, result.stderr.strip().splitlines()[-3:]
        assert result.stdout.strip().splitlines()[-1] == wanted, value


def forget_yesterday(env, sks, last, now, rewound):
    """Make the core forget yesterday of each account whose whole loads end on it, as if the daily
    load of today had not come yet: its rows in the facts of the ad platforms go, and the windows of
    those loads end the day before, in core.loads and in the journal. rewound gets what the windows
    were, as (account, load, day), before they change. An account whose loads end before
    yesterday, as when a midnight of the account came since the history, lacks yesterday
    already."""
    days = {a: day for a, day in last.items() if day == common.last_final_day(now, ZONES[a])}
    for account_id, day in days.items():
        alone = warehouse_output(
            env,
            "SELECT count(*) FROM core.loads "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND account_id = '{account_id}' AND outcome = 'loaded' "
            f"AND kind <> 'intraday' AND window_from >= DATE '{day}'",
        )
        assert alone == "0", f"a whole load of {account_id} takes {day} alone"
    for account_id, day in days.items():
        rewound += [
            (account_id, load_id, day)
            for (load_id,) in rows(
                env,
                "SELECT load_id FROM core.loads "  # noqa: S608 (values of this test)
                f"WHERE source = '{SOURCE}' AND account_id = '{account_id}' "
                f"AND outcome = 'loaded' AND kind <> 'intraday' AND window_to = DATE '{day}'",
            )
        ]
        for fact in FACTS:
            warehouse_output(
                env,
                f"DELETE FROM core.{fact} WHERE account_sk = {sks[account_id]} "  # noqa: S608 (names of this test)
                f"AND report_date = DATE '{day}'",
            )
        warehouse_output(
            env,
            "UPDATE core.loads SET window_to = window_to - 1 "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND account_id = '{account_id}' AND outcome = 'loaded' "
            f"AND kind <> 'intraday' AND window_to = DATE '{day}'",
        )
        warehouse_output(
            env,
            "UPDATE raw_demo_search._agoranomos_loads SET window_to = window_to - 1 "  # noqa: S608 (values of this test)
            f"WHERE account_id = '{account_id}' AND status = 'loaded' "
            f"AND kind IS DISTINCT FROM 'intraday' AND window_to = DATE '{day}'",
        )


def pinned(airflow_url, token, run) -> bool:
    """Whether the pin of a run of the transform has ended: the run reads only what it pinned."""
    states = [i["state"] for i in task_instances(airflow_url, token, run) if i["task_id"] == "pin"]
    return bool(states) and states[0] in ("success", "failed", "upstream_failed", "skipped")


def run_of_the_cycle(airflow_url, token, cycle):
    """The run of the transform that the event of the load of a run of the short cycle started."""
    since = moment(cycle["start_date"])
    for run in dag_runs(airflow_url, token, TRANSFORM, limit=5):
        if run["run_type"] == "asset_triggered" and moment(run["run_after"]) >= since:
            return run
    return None


def test_the_short_cycle_writes_partial_days_that_the_daily_load_replaces(
    env, airflow_url, token, idle, capsys
):
    if intraday.period(env.get("AGORANOMOS_INTRADAY_HOURS") or "") != 0:
        pytest.skip(
            "the short cycle of the demo runs on its schedule here, and unpaused it would load at "
            "once: the test runs it by hand with AGORANOMOS_INTRADAY_HOURS=0, as CI does"
        )
    start = time.time()
    sks = dict(rows(env, SKS))
    assert set(sks) == set(ACCOUNTS), sks
    last = {account_id: date.fromisoformat(day) for account_id, day in rows(env, LAST_DAYS)}
    assert set(last) == set(ACCOUNTS), last
    now = datetime.now(UTC)
    for account_id, day in last.items():
        today = common.local_today(now, ZONES[account_id])
        assert day < today, (account_id, day, today)
        assert campaigns(env, sks[account_id], today) == {}, f"the core holds today of {account_id}"
    reported = {
        name: rows(
            env,
            f"SELECT account_sk, {key}, last_report_date FROM core.{name} "  # noqa: S608 (names of this test)
            f"WHERE account_sk IN ({', '.join(sks.values())})",
        )
        for name, key in (("dim_account", "account_sk"), ("dim_campaign", "campaign_id"))
    }
    was_paused = is_paused(airflow_url, token, INTRADAY)
    rewound = []
    replaced = False
    try:
        forget_yesterday(env, sks, last, now, rewound)

        # 1. A run of the short cycle, and the run of the transform that its asset starts, with
        # the daily load of demo_search beside it once it has pinned what it reads, so that the
        # daily load waits for the next run.
        unpause(airflow_url, token, TRANSFORM)
        unpause(airflow_url, token, INTRADAY)
        before = ops_sizes(env)
        cycle = trigger(airflow_url, token, INTRADAY)
        cycle = wait_for_run(airflow_url, token, INTRADAY, cycle["dag_run_id"], timeout=900)
        assert cycle["state"] == "success", cycle["state"]
        states = {
            i["task_id"]: (i["state"], i["pool"]) for i in task_instances(airflow_url, token, cycle)
        }
        assert states == {
            "due": ("success", "default_pool"),
            "load_demo_search_intraday": ("success", "demo_load"),
        }, states
        events = asset_events(
            airflow_url, token, ASSET, source_dag_id=INTRADAY, source_run_id=cycle["dag_run_id"]
        )
        assert len(events) == 1, events
        first = wait_for(
            lambda: run_of_the_cycle(airflow_url, token, cycle),
            300,
            interval=2,
            message="the load of the short cycle started no run of the transform",
        )
        sampler = memory_report.Sampler()
        try:
            wait_for(
                lambda: pinned(airflow_url, token, first),
                900,
                interval=2,
                message="the run of the short cycle did not pin its loads",
            )
            daily_now = datetime.now(UTC)
            run_in_tasks(env, DAILY, SOURCE, daily_now.isoformat())
            first = wait_for_run(airflow_url, token, TRANSFORM, first["dag_run_id"], timeout=1800)
        finally:
            peaks = sampler.stop()
        assert first["state"] == "success", first["state"]
        after = ops_sizes(env)
        accepted = rows(env, ACCEPTED.format(start=first["start_date"], end=first["end_date"]))
        assert accepted == [(SOURCE, "intraday")], accepted
        table = memory_report.render(
            "The short cycle of the demo: a run of the transform of its load alone, with the "
            "daily load of demo_search beside it",
            memory_report.docker("info", "--format", "{{.Architecture}}").strip(),
            memory_report.collect(peaks),
            sampler.skipped,
        )
        report(capsys, table.splitlines()[0].removeprefix("### "), table.splitlines()[2:])

        # Yesterday and today partial, as the generator gives them at the moment of the run, in
        # the fact of campaigns alone, with no conversions.
        found = rows(env, INTRADAY_LOADS.format(since=int(start)))
        loads = sorted({load for *_, load in found})
        assert len(loads) == 1, loads
        windows = {a: (date.fromisoformat(f), date.fromisoformat(t)) for a, f, t, _, _ in found}
        [cycle_now] = {moment(at) for *_, at, _ in found}
        assert cycle_now == moment(cycle["run_after"]), (cycle_now, cycle["run_after"])
        assert set(windows) == set(ACCOUNTS), windows
        records = list(search.intraday_stats(cycle_now, windows))
        kind = rows(
            env,
            "SELECT DISTINCT kind, status FROM core.loads "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND load_id = '{loads[0]}'",
        )
        assert kind == [("intraday", "accepted")], kind
        partial = {}
        for account_id, (first_day, last_day) in windows.items():
            today = common.local_today(cycle_now, ZONES[account_id])
            assert (first_day, last_day) == (today - timedelta(days=1), today), account_id
            for day in common.days(first_day, last_day):
                held = campaigns(env, sks[account_id], day)
                wanted = generated(records, account_id, day)
                # Seconds after midnight in the account, today may have no row yet.
                assert wanted or day == today, (account_id, day)
                assert {c: m for c, (m, _) in held.items()} == wanted, (account_id, day)
                assert {rest for _, rest in held.values()} == {("t", loads[0], "intraday", "f")}
                assert other_rows(env, sks[account_id], day) == dict.fromkeys(OTHER_FACTS, 0)
                partial[(account_id, day)] = wanted
        cycle_tasks = len(task_instances(airflow_url, token, first))
        cycle_growth = ops_growth(before, after)

        # 2. The daily load in the next run: yesterday whole, as the generator gives it at the
        # moment of the daily load, with conversions and the other facts, and no row of the
        # journal of restatements for it; today stays partial.
        [daily_load] = sorted({load for *_, load in rows(env, WHOLE_LOAD.format(since=int(start)))})
        before = ops_sizes(env)
        second = transform(airflow_url, token)
        after = ops_sizes(env)
        replaced = True
        daily = list(search.stats(daily_now, first=lambda a: windows[a.account_id][0]))
        for account_id, (yesterday, today) in windows.items():
            sk = sks[account_id]
            assert yesterday == common.last_final_day(daily_now, ZONES[account_id]), account_id
            held = campaigns(env, sk, yesterday)
            wanted = generated(daily, account_id, yesterday)
            assert {c: m for c, (m, _) in held.items()} == wanted, account_id
            assert {rest for _, rest in held.values()} == {("f", daily_load, "window", "t")}
            if common.age(daily_now, ZONES[account_id], yesterday) == common.age(
                cycle_now, ZONES[account_id], yesterday
            ):
                # The same day of the account: the short cycle showed what the daily load took.
                assert wanted == partial[(account_id, yesterday)], account_id
            assert other_rows(env, sk, yesterday)["ad_stats_ad_group_daily"] > 0, account_id
            journaled = warehouse_output(
                env,
                "SELECT count(*) FROM core.ad_stats_campaign_daily_restatements AS j "  # noqa: S608 (values of this test)
                f"JOIN core.loads AS l ON l.load_sk = j.new_load_sk WHERE l.source = '{SOURCE}' "
                f"AND l.load_id = '{daily_load}' AND j.account_sk = {sk} "
                f"AND j.report_date = DATE '{yesterday}'",
            )
            assert journaled == "0", account_id
            held = campaigns(env, sk, today)
            assert {c: m for c, (m, _) in held.items()} == partial[(account_id, today)]
            assert {rest for _, rest in held.values()} == {("t", loads[0], "intraday", "f")}
        daily_growth = ops_growth(before, after)
        report(
            capsys,
            "The short cycle of the demo, the runs of the transform",
            [
                "| Run of the transform | Duration | Tasks | Bytes of the tables of elementary | "
                "Bytes of ops |",
                "|---|---:|---:|---:|---:|",
                f"| The load of the short cycle alone, the daily load beside it | "
                f"{seconds(first):.0f} s | {cycle_tasks} | {cycle_growth[0]:,} | "
                f"{cycle_growth[1]:,} |",
                f"| The daily load of demo_search, a window of 90 days | {seconds(second):.0f} s | "
                f"{len(task_instances(airflow_url, token, second))} | {daily_growth[0]:,} | "
                f"{daily_growth[1]:,} |",
            ],
        )
    finally:
        if was_paused:
            pause(airflow_url, token, INTRADAY)
        if not replaced:
            # The daily load brings yesterday back whole, with the run that reads it.
            wait_for(
                lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
                1800,
                interval=5,
                message="the run of the transform of the short cycle did not end",
            )
            if not rows(env, WHOLE_LOAD.format(since=int(start))):
                run_in_tasks(env, DAILY, SOURCE, datetime.now(UTC).isoformat())
            transform(airflow_url, token)
        loads = sorted({load for *_, load in rows(env, INTRADAY_LOADS.format(since=int(start)))})
        listed = ", ".join(f"'{load}'" for load in loads) or "NULL"
        warehouse_output(
            env,
            "DELETE FROM core.ad_stats_campaign_daily WHERE load_sk IN "  # noqa: S608 (values of this test)
            f"(SELECT load_sk FROM core.loads WHERE source = '{SOURCE}' AND load_id IN ({listed}))",
        )
        for table in ("campaign_stats", "_agoranomos_loads"):
            warehouse_output(
                env,
                f"DELETE FROM raw_demo_search.{table} WHERE _dlt_load_id IN ({listed})",  # noqa: S608
                "loader",
            )
        warehouse_output(
            env,
            f"DELETE FROM raw_demo_search._dlt_loads WHERE load_id IN ({listed})",  # noqa: S608
            "loader",
        )
        warehouse_output(
            env,
            f"DELETE FROM core.account_quarantine WHERE load_id IN ({listed})",  # noqa: S608
        )
        warehouse_output(
            env,
            f"DELETE FROM core.loads WHERE source = '{SOURCE}' AND load_id IN ({listed})",  # noqa: S608
        )
        for account_id, load_id, day in rewound:
            warehouse_output(
                env,
                f"UPDATE core.loads SET window_to = DATE '{day}' "  # noqa: S608 (values of this test)
                f"WHERE source = '{SOURCE}' AND account_id = '{account_id}' "
                f"AND load_id = '{load_id}'",
            )
            warehouse_output(
                env,
                "UPDATE raw_demo_search._agoranomos_loads "  # noqa: S608 (values of this test)
                f"SET window_to = DATE '{day}' WHERE account_id = '{account_id}' "
                f"AND _dlt_load_id = '{load_id}'",
            )
        # The last day of statistics that each dimension held, or the last whole day that the core
        # holds now when that is later: across a midnight of the account between the start of the
        # stack and this test, the daily load of the test takes a day whole that the core did not
        # hold, and a dimension that went back to the day before would take it again in the next
        # run of the transform, as a change of its row.
        for key, _, day in reported["dim_account"]:
            warehouse_output(
                env,
                "UPDATE core.dim_account SET last_report_date = greatest("  # noqa: S608 (values of the core)
                f"{date_or_null(day)}, (SELECT max(report_date) FROM core.ad_stats_campaign_daily "
                f"WHERE account_sk = {key} AND NOT is_partial_day)) WHERE account_sk = {key}",
            )
        for key, campaign, day in reported["dim_campaign"]:
            warehouse_output(
                env,
                "UPDATE core.dim_campaign SET last_report_date = greatest("  # noqa: S608 (values of the core)
                f"{date_or_null(day)}, (SELECT max(report_date) FROM core.ad_stats_campaign_daily "
                f"WHERE account_sk = {key} AND campaign_id = {campaign} AND NOT is_partial_day)) "
                f"WHERE account_sk = {key} AND campaign_id = {campaign}",
            )
    for account_id, day in last.items():
        restored = warehouse_output(
            env,
            "SELECT max(window_to) FROM core.loads "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND account_id = '{account_id}' AND outcome = 'loaded' "
            "AND kind <> 'intraday'",
        )
        assert date.fromisoformat(restored) >= day, account_id
        held = rows(env, CAMPAIGNS.format(sk=sks[account_id], day=day))
        assert held, account_id
        assert {r[4] for r in held} == {"f"}, account_id
    assert warehouse_output(env, "SELECT count(*) FROM core.loads WHERE status = 'pinned'") == "0"
