"""Tests of the short cycle of the current day (ADR-0051): its period, the hours of its runs, which
keep an hour from the daily backup (ADR-0020), and its days
(airflow/dags/agoranomos_pipelines/intraday.py), and the DAGs of Google Ads, of Meta, of TikTok
and of the demo that run it (airflow/dags/agoranomos_google_ads_intraday.py,
agoranomos_meta_ads_intraday.py, agoranomos_tiktok_ads_intraday.py, agoranomos_demo_intraday.py).

Airflow is not in the development environment, so the DAG file is imported with stand-ins of the
parts of airflow.sdk that it and the modules of the package use (airflow_stand_in.py): the
decorators keep the function of each task, so that a test calls it with a context of its own."""

import importlib
import itertools
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import intraday
from airflow_stand_in import WITH_AIRFLOW, forget, import_dag

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 29, 12, 27, tzinfo=UTC)


# The minute of the day in UTC of the default backup, 03:00 in UTC (.env.example).
DEFAULT_BACKUP = (3 * 60,)


@pytest.fixture(autouse=True)
def no_backup_settings(monkeypatch):
    """No BACKUP_TIME or BACKUP_TZ from the environment of the tests: their defaults, as
    compose.yaml gives them."""
    monkeypatch.delenv(intraday.BACKUP_TIME_SETTING, raising=False)
    monkeypatch.delenv(intraday.BACKUP_TZ_SETTING, raising=False)


@pytest.mark.parametrize(
    ("value", "cron"),
    [
        # Every hour but the two whose runs, at 02:27 and 03:27, fall within the hour of 03:00.
        ("1", "27 0,1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *"),
        # No move keeps every run of a period of 2 hours an hour away: 02:27 is left out.
        ("2", "27 0,4,6,8,10,12,14,16,18,20,22 * * *"),
        # 03:27 would be 27 minutes after the backup: the runs move one hour later, from 01:27.
        ("3", "27 1,4,7,10,13,16,19,22 * * *"),
        # From midnight, every run is an hour away already: 04:27 is 87 minutes after 03:00.
        ("4", "27 */4 * * *"),
        ("6", "27 */6 * * *"),
        ("8", "27 */8 * * *"),
        ("12", "27 */12 * * *"),
        ("24", "27 0 * * *"),
    ],
)
def test_each_divisor_of_24_hours_is_a_schedule_at_a_fixed_minute_away_from_the_backup(value, cron):
    # The loads fall at the same hours every day, in UTC, at least an hour from the backup at the
    # defaults of BACKUP_TIME and BACKUP_TZ.
    hours = intraday.period(value)
    assert hours == int(value)
    assert intraday.backup_minutes() == DEFAULT_BACKUP
    assert intraday.cron(hours) == cron
    assert intraday.cron(hours, DEFAULT_BACKUP) == cron
    for hour in intraday.run_hours(hours):
        assert intraday.distance(hour * 60 + intraday.MINUTE, 3 * 60) >= 60, (value, hour)


def test_the_runs_of_the_defaults_moved_off_0327():
    # Before, every 3 hours from 00:27, with a run at 03:27, 27 minutes after the backup began.
    old = tuple(range(0, 24, 3))
    new = intraday.run_hours(3)
    assert new == (1, 4, 7, 10, 13, 16, 19, 22)
    assert 3 in old
    assert 3 not in new
    # As many runs a day as before, still every 3 hours.
    assert len(new) == len(old) == 8
    assert {b - a for a, b in itertools.pairwise(new)} == {3}


@pytest.mark.parametrize("hours", [1, 2, 3, 4, 6, 8, 12, 24])
def test_every_run_keeps_an_hour_from_a_backup_at_any_minute_of_the_day(hours):
    # In UTC, for each minute of the day as BACKUP_TIME: every run that is kept is at least an hour
    # from it, the hours are those of one move of the schedule from midnight, and no smaller move
    # keeps as many runs.
    for start in range(intraday.DAY):
        kept = intraday.run_hours(hours, (start,))
        assert kept, (hours, start)
        for hour in kept:
            assert intraday.distance(hour * 60 + intraday.MINUTE, start) >= 60, (hours, start)
        shift = kept[0] % hours
        assert all(hour % hours == shift for hour in kept), (hours, start)
        if hours >= 3:
            # The gap of 3 hours or more around the backup always holds an hour on each side at
            # some whole hour: no run is left out.
            assert len(kept) == 24 // hours, (hours, start)
        elif hours == 2:
            # 12 runs only when the backup starts at 27 past an hour, with the runs an hour
            # before and after it.
            assert len(kept) == (12 if start % 60 == intraday.MINUTE else 11), start
        else:
            assert len(kept) == (23 if start % 60 == intraday.MINUTE else 22), start
        for smaller in range(shift):
            fewer = [
                hour
                for hour in range(smaller, 24, hours)
                if intraday.distance(hour * 60 + intraday.MINUTE, start) >= 60
            ]
            assert len(fewer) < len(kept), (hours, start, smaller)


@pytest.mark.parametrize(
    ("backup_time", "cron_of_3"),
    [
        # 00:27 would be 27 minutes after it: one hour later, 22:27 and 01:27 keep away.
        ("00:00", "27 1,4,7,10,13,16,19,22 * * *"),
        # 00:27 and 01:27 are both closer than an hour: two hours later, 23:27 and 02:27.
        ("01:00", "27 2,5,8,11,14,17,20,23 * * *"),
        # 00:27 and 03:27, 63 and 117 minutes away: the runs stay from midnight.
        ("01:30", "27 */3 * * *"),
        ("09:17", "27 1,4,7,10,13,16,19,22 * * *"),
        ("11:00", "27 */3 * * *"),
        ("12:00", "27 1,4,7,10,13,16,19,22 * * *"),
        ("22:45", "27 */3 * * *"),
        ("23:59", "27 1,4,7,10,13,16,19,22 * * *"),
    ],
)
def test_a_backup_at_another_hour_moves_the_runs_only_when_one_falls_within_the_hour(
    monkeypatch, backup_time, cron_of_3
):
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, backup_time)
    hour, minute = (int(part) for part in backup_time.split(":"))
    assert intraday.backup_minutes() == (hour * 60 + minute,)
    assert intraday.cron(3) == cron_of_3


def test_a_time_zone_with_summer_time_keeps_the_hour_at_both_offsets(monkeypatch):
    # 03:00 in Berlin is 02:00 UTC in winter and 01:00 UTC in summer: the hours of the runs stay in
    # UTC, the same every day, and keep away from both.
    today = date(2026, 11, 26)
    assert intraday.backup_minutes("03:00", "Europe/Berlin", today) == (60, 120)
    # In June too, whatever the day of the year that the parse comes on.
    assert intraday.backup_minutes("03:00", "Europe/Berlin", date(2026, 6, 1)) == (60, 120)
    # A zone without summer time gives one minute, half past an hour for India.
    assert intraday.backup_minutes("03:00", "Asia/Kolkata", today) == (21 * 60 + 30,)
    assert intraday.backup_minutes("03:00", "UTC", today) == DEFAULT_BACKUP
    berlin = intraday.backup_minutes("03:00", "Europe/Berlin", today)
    for hours in (1, 2, 3, 4, 6, 8, 12, 24):
        for hour in intraday.run_hours(hours, berlin):
            for start in berlin:
                assert intraday.distance(hour * 60 + intraday.MINUTE, start) >= 60, (hours, hour)
    # No move of a period of 3 hours keeps every run an hour from both 01:00 and 02:00 UTC: the
    # runs stay from midnight, and that of 00:27 is left out, seven runs of the eight.
    assert intraday.cron(3, berlin) == "27 3,6,9,12,15,18,21 * * *"
    # From 4 hours on, a move keeps every run away from both.
    assert intraday.cron(4, berlin) == "27 3,7,11,15,19,23 * * *"
    assert intraday.cron(24, berlin) == "27 3 * * *"
    assert len(intraday.run_hours(1, berlin)) == 21
    assert len(intraday.run_hours(2, berlin)) == 11
    # From the environment, as compose.yaml gives it.
    monkeypatch.setenv(intraday.BACKUP_TZ_SETTING, "Europe/Berlin")
    assert intraday.backup_minutes(today=today) == (60, 120)


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("BACKUP_TIME", "3am", r"BACKUP_TIME in \.env must be a time of day as HH:MM"),
        ("BACKUP_TIME", "24:00", r"BACKUP_TIME in \.env must be a time of day as HH:MM"),
        ("BACKUP_TIME", "3:00", r"BACKUP_TIME in \.env must be a time of day as HH:MM"),
        ("BACKUP_TZ", "Mars/Olympus", r"BACKUP_TZ in \.env must be a time zone"),
        ("BACKUP_TZ", "../etc", r"BACKUP_TZ in \.env must be a time zone"),
    ],
)
def test_a_backup_setting_that_is_not_a_time_or_a_zone_is_refused(
    monkeypatch, setting, value, message
):
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError, match=message):
        intraday.backup_minutes()
    with pytest.raises(ValueError, match=message):
        intraday.cron(3)


def test_empty_backup_settings_take_the_defaults_of_compose(monkeypatch):
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, " ")
    monkeypatch.setenv(intraday.BACKUP_TZ_SETTING, "")
    assert intraday.backup_minutes() == DEFAULT_BACKUP
    assert intraday.run_hours(0) == ()


def test_0_is_no_schedule_and_empty_is_every_3_hours(monkeypatch):
    assert intraday.period("0") == 0
    assert intraday.cron(0) is None
    assert intraday.period("") == intraday.period(" ") == 3
    # Without a value, the setting of the environment, as compose.yaml gives it.
    monkeypatch.delenv(intraday.SETTING, raising=False)
    assert intraday.period() == 3
    monkeypatch.setenv(intraday.SETTING, "6")
    assert intraday.period() == 6


@pytest.mark.parametrize("value", ["5", "7", "48", "-3", "03", "3.0", "three", "3h", "every 3"])
def test_a_period_that_does_not_divide_24_hours_or_a_word_is_refused(value):
    with pytest.raises(ValueError, match=r"AGORANOMOS_INTRADAY_HOURS in \.env must be 1, 2, 3"):
        intraday.period(value)
    with pytest.raises(ValueError, match="is not a period"):
        intraday.cron(5)
    with pytest.raises(ValueError, match="is not a period"):
        intraday.run_hours(5)


def test_a_run_that_waited_longer_than_the_period_ends_without_a_load():
    run_after = NOW - timedelta(hours=3)
    assert not intraday.waited_too_long(run_after, NOW, 3)
    assert intraday.waited_too_long(run_after - timedelta(seconds=1), NOW, 3)
    assert not intraday.waited_too_long(NOW - timedelta(minutes=50), NOW, 1)
    assert intraday.waited_too_long(NOW - timedelta(minutes=61), NOW, 1)
    # A run by hand of a DAG without a schedule never waits too long.
    assert not intraday.waited_too_long(NOW - timedelta(days=2), NOW, 0)


def test_a_load_takes_the_current_day_and_the_day_before_while_no_whole_load_covers_it():
    today = date(2026, 10, 29)
    yesterday = date(2026, 10, 28)
    # The daily load has taken yesterday: today alone.
    assert intraday.days(today, yesterday) == (today, today)
    # After midnight, before the daily load: the day before too, and no day before it, however
    # long ago the last whole day was.
    assert intraday.days(today, yesterday - timedelta(days=1)) == (yesterday, today)
    assert intraday.days(today, yesterday - timedelta(days=120)) == (yesterday, today)


def test_the_last_whole_day_is_the_later_of_the_core_and_the_journal_or_none():
    # Google Ads, Meta and TikTok share it: an account that no whole load has taken waits for its
    # first daily load, as the check of the accounts knows an account from the list of a whole one.
    core = {"a": date(2026, 10, 27)}
    journal = {
        "a": (date(2026, 10, 27), date(2026, 10, 26)),
        "b": (date(2026, 10, 29), date(2026, 10, 28)),
    }
    assert intraday.whole_end("a", core, journal) == date(2026, 10, 27)
    assert intraday.whole_end("a", {}, journal) == date(2026, 10, 26)
    assert intraday.whole_end("b", core, journal) == date(2026, 10, 28)
    assert intraday.whole_end("c", core, journal) is None


def test_the_minute_keeps_away_from_the_daily_loads_and_the_watcher():
    # Meta at 10:07, Google Ads at 10:37 and TikTok at 12:17 UTC, the watcher at 4, 19, 34 and 49
    # past each hour.
    taken = set()
    for name in (
        "agoranomos_meta_ads_daily.py",
        "agoranomos_google_ads_daily.py",
        "agoranomos_tiktok_ads_daily.py",
    ):
        text = (ROOT / "airflow" / "dags" / name).read_text(encoding="utf-8")
        [line] = [line for line in text.splitlines() if line.startswith("SCHEDULE = ")]
        taken.add(int(line.split('("')[1].split()[0]))
    assert taken == {7, 17, 37}
    assert intraday.MINUTE not in taken | {4, 19, 34, 49}


@pytest.fixture
def dag_file(monkeypatch):
    """A function that imports a DAG file of the short cycle, that of Google Ads unless dag_name
    says otherwise, with a period, and what the import made: the arguments of the DAG, its tasks,
    the secrets that it masked, the context of its tasks, whose runs of another DAG and whose
    moments a test sets, and the DAGs whose runs a task asked for."""
    names = (
        "agoranomos_google_ads_intraday",
        "agoranomos_meta_ads_intraday",
        "agoranomos_tiktok_ads_intraday",
        "agoranomos_demo_intraday",
        *WITH_AIRFLOW,
    )

    def imported(hours: str | None, dag_name: str = "agoranomos_google_ads_intraday"):
        made = {"tasks": {}, "masked": [], "dag": None}
        context = {
            "dag_run": types.SimpleNamespace(run_after=datetime.now(UTC)),
            "ti": types.SimpleNamespace(
                get_dr_count=lambda dag_id, states: (
                    made["asked"].append((dag_id, tuple(states))) or made["active"]
                )
            ),
        }
        made["active"], made["context"], made["asked"] = 0, context, []
        if hours is None:
            monkeypatch.delenv(intraday.SETTING, raising=False)
        else:
            monkeypatch.setenv(intraday.SETTING, hours)
        forget(monkeypatch, names)
        return import_dag(monkeypatch, dag_name, made, context), made

    yield imported
    for name in names:
        sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("hours", "cron"),
    [
        (None, "27 1,4,7,10,13,16,19,22 * * *"),
        ("1", "27 0,1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *"),
    ],
)
def test_the_dag_runs_on_the_schedule_of_the_setting_one_run_at_a_time_and_starts_paused(
    dag_file, hours, cron
):
    module, made = dag_file(hours)
    assert ("cron", cron, "UTC") == module.SCHEDULE
    arguments = made["dag"]
    assert arguments["schedule"] == module.SCHEDULE
    assert arguments["max_active_runs"] == 1
    assert arguments["is_paused_upon_creation"] is True
    assert arguments["catchup"] is False
    # A load never lives longer than an hour, which the cleanup of raw waits longer than.
    assert arguments["dagrun_timeout"] <= timedelta(hours=1)
    defaults = arguments["default_args"]
    retries = defaults["retries"] + 1
    longest = (
        retries * defaults["execution_timeout"] + defaults["retries"] * defaults["retry_delay"]
    )
    assert longest <= arguments["dagrun_timeout"]


def test_the_dag_has_no_schedule_at_0(dag_file):
    module, made = dag_file("0")
    assert module.SCHEDULE is None
    assert made["dag"]["schedule"] is None


@pytest.mark.parametrize("hours", ["5", "three"])
def test_the_dag_refuses_a_period_that_does_not_divide_24_hours_or_a_word(dag_file, hours):
    # The parse of the file stops, and Airflow shows the line as the error of the import.
    with pytest.raises(ValueError, match=r"AGORANOMOS_INTRADAY_HOURS in \.env must be"):
        dag_file(hours)


@pytest.mark.parametrize(
    "dag_name",
    [
        "agoranomos_google_ads_intraday",
        "agoranomos_meta_ads_intraday",
        "agoranomos_tiktok_ads_intraday",
        "agoranomos_demo_intraday",
    ],
)
def test_the_dag_keeps_its_runs_away_from_the_backup_of_the_environment(
    dag_file, monkeypatch, dag_name
):
    # BACKUP_TIME and BACKUP_TZ come from the environment that compose.yaml gives the DAG processor.
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, "12:00")
    module, _ = dag_file("3", dag_name)
    assert module.SCHEDULE == ("cron", "27 1,4,7,10,13,16,19,22 * * *", "UTC")
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, "11:00")
    module, _ = dag_file("3", dag_name)
    assert module.SCHEDULE == ("cron", "27 */3 * * *", "UTC")
    # 03:00 in Berlin, 01:00 or 02:00 UTC: seven runs of the eight.
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, "03:00")
    monkeypatch.setenv(intraday.BACKUP_TZ_SETTING, "Europe/Berlin")
    module, _ = dag_file("3", dag_name)
    assert module.SCHEDULE == ("cron", "27 3,6,9,12,15,18,21 * * *", "UTC")
    # A value that is no time of day stops the parse of the file, as a wrong period does.
    monkeypatch.setenv(intraday.BACKUP_TIME_SETTING, "9am")
    with pytest.raises(ValueError, match=r"BACKUP_TIME in \.env must be"):
        dag_file("3", dag_name)


def test_the_load_runs_in_the_pool_of_google_ads_and_updates_an_asset_of_its_own(dag_file):
    module, made = dag_file(None)
    assert module.LOAD_POOL == "google_ads_load"
    load = made["tasks"]["load_google_ads_intraday"]
    assert load.arguments["pool"] == "google_ads_load"
    [asset] = load.arguments["outlets"]
    assert asset.uri == "agoranomos_google_ads_intraday"
    assets = sys.modules["agoranomos_pipelines.assets"]
    assert asset is assets.RAW_GOOGLE_ADS_INTRADAY
    assert asset in assets.LOADED
    assert asset is not assets.RAW_GOOGLE_ADS
    # The task of the daily load is in the same pool.
    daily = (ROOT / "airflow" / "dags" / "agoranomos_google_ads_daily.py").read_text("utf-8")
    assert 'LOAD_POOL = "google_ads_load"' in daily
    assert '@task(task_id="load_google_ads", outlets=[RAW_GOOGLE_ADS], pool=LOAD_POOL)' in daily
    assert made["tasks"]["due"].kind == "short_circuit"
    assert "pool" not in made["tasks"]["due"].arguments


def test_the_last_task_checks_the_accounts_after_the_load_and_passes_after_a_skip(dag_file):
    # A run that due skips skips both tasks after it, and a load that skips leaves the check to run
    # and pass: none_failed, unlike all_success, does not skip after a skip, and fails, as
    # upstream_failed, after a load that failed (agoranomos_pipelines/outcome.py).
    _, made = dag_file("3")
    tasks = made["tasks"]
    assert tasks["load_google_ads_intraday"].upstream == ["due"]
    check = tasks["check_accounts"]
    assert check.upstream == ["load_google_ads_intraday"]
    assert check.arguments["trigger_rule"] == "none_failed"
    assert "outlets" not in check.arguments
    assert "pool" not in check.arguments


def test_a_run_skips_while_the_daily_load_runs_or_once_it_waited_longer_than_the_period(dag_file):
    _, made = dag_file("3")
    due = made["tasks"]["due"].function
    assert due() is True
    made["active"] = 1
    assert due() is False
    made["active"] = 0
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=3, minutes=1)
    assert due() is False


def test_the_load_skips_without_an_account_to_load_or_after_a_long_wait(dag_file, monkeypatch):
    # A skipped task updates no asset, so the transform does not run for nothing.
    _, made = dag_file("3")
    google_ads = types.ModuleType("agoranomos_pipelines.google_ads")
    loads = types.ModuleType("agoranomos_pipelines.loads")
    loads.credentials = lambda: {"password": "loader-password"}
    google_ads.key_info = lambda: {"private_key": "stand-in key"}

    class AccountsFailed(Exception):
        def __init__(self, line, loaded):
            super().__init__(line)
            self.loaded = loaded

    google_ads.AccountsFailed = AccountsFailed
    answers = []
    google_ads.load_intraday = lambda now: answers.pop(0)
    package = importlib.import_module("agoranomos_pipelines")
    monkeypatch.setitem(sys.modules, "agoranomos_pipelines.google_ads", google_ads)
    monkeypatch.setitem(sys.modules, "agoranomos_pipelines.loads", loads)
    monkeypatch.setattr(package, "google_ads", google_ads, raising=False)
    monkeypatch.setattr(package, "loads", loads, raising=False)
    exceptions = sys.modules["airflow.sdk.exceptions"]
    load = made["tasks"]["load_google_ads_intraday"].function
    check = made["tasks"]["check_accounts"].function
    answers.append({"campaign_stats": 3})
    assert load() is None
    assert check(None) is None
    assert made["masked"] == ["loader-password", "stand-in key"]
    answers.append(None)
    with pytest.raises(exceptions.AirflowSkipException, match="no account of google_ads"):
        load()

    def failing(loaded):
        def load_intraday(now):
            raise AccountsFailed(f"1 of {1 + loaded} Google Ads accounts failed", loaded)

        return load_intraday

    # One account loaded: the task succeeds, so that its asset starts the transform, and the last
    # task fails the run with the line.
    google_ads.load_intraday = failing(1)
    line = load()
    assert line == "1 of 2 Google Ads accounts failed"
    with pytest.raises(exceptions.AirflowFailException, match="1 of 2"):
        check(line)
    # None loaded: the task fails, and starts nothing.
    google_ads.load_intraday = failing(0)
    with pytest.raises(exceptions.AirflowFailException, match="1 of 1"):
        load()
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=4)
    with pytest.raises(exceptions.AirflowSkipException, match="waited longer than the period"):
        load()


DEMO = "agoranomos_demo_intraday"


@pytest.mark.parametrize(
    ("hours", "cron"), [(None, "27 1,4,7,10,13,16,19,22 * * *"), ("6", "27 */6 * * *")]
)
def test_the_dag_of_the_demo_runs_on_the_same_schedule_one_run_at_a_time_and_starts_paused(
    dag_file, hours, cron
):
    module, made = dag_file(hours, DEMO)
    assert ("cron", cron, "UTC") == module.SCHEDULE
    arguments = made["dag"]
    assert arguments["dag_id"] == DEMO
    assert arguments["schedule"] == module.SCHEDULE
    assert arguments["max_active_runs"] == 1
    assert arguments["is_paused_upon_creation"] is True
    assert arguments["catchup"] is False
    assert arguments["dagrun_timeout"] <= timedelta(hours=1)
    defaults = arguments["default_args"]
    longest = (defaults["retries"] + 1) * defaults["execution_timeout"] + defaults[
        "retries"
    ] * defaults["retry_delay"]
    assert longest <= arguments["dagrun_timeout"]


def test_the_dag_of_the_demo_has_no_schedule_at_0_and_refuses_another_period(dag_file):
    module, made = dag_file("0", DEMO)
    assert module.SCHEDULE is None
    assert made["dag"]["schedule"] is None
    with pytest.raises(ValueError, match=r"AGORANOMOS_INTRADAY_HOURS in \.env must be"):
        dag_file("5", DEMO)


def test_the_load_of_the_demo_runs_in_the_pool_of_the_demo_and_updates_an_asset_of_its_own(
    dag_file,
):
    module, made = dag_file(None, DEMO)
    assert module.LOAD_POOL == "demo_load"
    load = made["tasks"]["load_demo_search_intraday"]
    assert load.arguments["pool"] == "demo_load"
    [asset] = load.arguments["outlets"]
    assert asset.uri == "agoranomos_demo_intraday"
    assets = sys.modules["agoranomos_pipelines.assets"]
    assert asset is assets.RAW_DEMO_INTRADAY
    assert asset in assets.LOADED
    assert asset not in assets.RAW_DEMO_PLATFORMS.values()
    # The history and the daily load of the demo are in the same pool.
    for name in ("agoranomos_demo_history.py", "agoranomos_demo_daily.py"):
        text = (ROOT / "airflow" / "dags" / name).read_text("utf-8")
        assert 'LOAD_POOL = "demo_load"' in text, name
    assert made["tasks"]["due"].kind == "short_circuit"
    assert "pool" not in made["tasks"]["due"].arguments


def test_a_run_of_the_demo_skips_while_the_history_runs_or_once_it_waited_too_long(dag_file):
    _, made = dag_file("3", DEMO)
    due = made["tasks"]["due"].function
    assert due() is True
    assert made["asked"] == [("agoranomos_demo_history", ("queued", "running"))]
    made["active"] = 1
    assert due() is False
    made["active"] = 0
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=3, minutes=1)
    assert due() is False
    # Without a schedule, a run by hand never waits too long.
    _, made = dag_file("0", DEMO)
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(days=2)
    assert made["tasks"]["due"].function() is True


def test_the_load_of_the_demo_takes_the_moment_of_the_run_and_skips_without_a_whole_load(
    dag_file, monkeypatch
):
    # A skipped task updates no asset, so the transform does not run for nothing.
    _, made = dag_file("3", DEMO)
    sources = types.ModuleType("agoranomos_pipelines.demo.sources")
    sources.credentials = lambda: {"password": "loader-password"}
    moments, answers = [], []
    sources.load_intraday = lambda now: moments.append(now) or answers.pop(0)
    package = importlib.import_module("agoranomos_pipelines.demo")
    monkeypatch.setitem(sys.modules, "agoranomos_pipelines.demo.sources", sources)
    monkeypatch.setattr(package, "sources", sources, raising=False)
    exceptions = sys.modules["airflow.sdk.exceptions"]
    load = made["tasks"]["load_demo_search_intraday"].function
    run_after = datetime.now(UTC) - timedelta(minutes=20)
    made["context"]["dag_run"].run_after = run_after
    answers.append({"campaign_stats": 7})
    assert load() is None
    assert moments == [run_after]
    assert made["masked"] == ["loader-password"]
    answers.append(None)
    with pytest.raises(exceptions.AirflowSkipException, match="no account of demo_search"):
        load()
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=4)
    with pytest.raises(exceptions.AirflowSkipException, match="waited longer than the period"):
        load()
    assert len(moments) == 2


def test_compose_sets_the_pools_and_gives_the_setting_to_every_airflow_service():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    for pool in ("google_ads_load", "meta_ads_load", "tiktok_ads_load"):
        assert f'"{pool}",\n            "1",' in compose, pool
    # In the environment that every Airflow service shares, the DAG processor among them.
    shared = compose.split("environment: &airflow-environment\n")[1].split("\n  volumes:")[0]
    assert "    AGORANOMOS_INTRADAY_HOURS: ${AGORANOMOS_INTRADAY_HOURS:-3}\n" in shared
    # The settings of the backup, with the defaults that the backup service has.
    assert "    BACKUP_TIME: ${BACKUP_TIME:-03:00}\n" in shared
    assert "    BACKUP_TZ: ${BACKUP_TZ:-UTC}\n" in shared
    backup = compose.split("\n  backup:\n")[1].split("\n    volumes:")[0]
    assert "      BACKUP_TIME: ${BACKUP_TIME:-03:00}\n" in backup
    assert "      TZ: ${BACKUP_TZ:-UTC}\n" in backup
    assert intraday.DEFAULT_BACKUP_TIME == "03:00"
    assert intraday.DEFAULT_BACKUP_TZ == "UTC"
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "\nAGORANOMOS_INTRADAY_HOURS=3\n" in example
    assert "\nBACKUP_TIME=03:00\nBACKUP_TZ=UTC\n" in example
    # The profile of an agency turns it off until a cycle is measured on it (ADR-0051).
    agency = example.split("# Agency profile (ADR-0040)")[1].split("\n\n")[0]
    assert "\n#   AGORANOMOS_INTRADAY_HOURS=0" in agency


# The short cycles of Meta and of TikTok (#412), as that of Google Ads: the DAG, the code of its
# platform, its pool, the task of its load and that of the daily load, and its asset.
PLATFORMS = {
    "agoranomos_meta_ads_intraday": (
        "meta_ads",
        "meta_ads_load",
        "load_meta_ads_intraday",
        "agoranomos_meta_ads_daily",
        "load_meta_ads",
        "RAW_META_ADS_INTRADAY",
        "RAW_META_ADS",
    ),
    "agoranomos_tiktok_ads_intraday": (
        "tiktok_ads",
        "tiktok_ads_load",
        "load_tiktok_ads_intraday",
        "agoranomos_tiktok_ads_daily",
        "load_tiktok_ads",
        "RAW_TIKTOK_ADS_INTRADAY",
        "RAW_TIKTOK_ADS",
    ),
}


@pytest.mark.parametrize("dag_name", sorted(PLATFORMS))
@pytest.mark.parametrize(
    ("hours", "cron"), [(None, "27 1,4,7,10,13,16,19,22 * * *"), ("6", "27 */6 * * *")]
)
def test_the_dags_of_meta_and_tiktok_run_as_that_of_google_ads_and_start_paused(
    dag_file, dag_name, hours, cron
):
    # The same moments as the short cycle of Google Ads, so that the loads of one cycle start the
    # transform once or twice and not once each.
    module, made = dag_file(hours, dag_name)
    assert ("cron", cron, "UTC") == module.SCHEDULE
    arguments = made["dag"]
    assert arguments["dag_id"] == dag_name
    assert arguments["schedule"] == module.SCHEDULE
    assert arguments["max_active_runs"] == 1
    assert arguments["is_paused_upon_creation"] is True
    assert arguments["catchup"] is False
    assert arguments["dagrun_timeout"] <= timedelta(hours=1)
    defaults = arguments["default_args"]
    longest = (defaults["retries"] + 1) * defaults["execution_timeout"] + defaults[
        "retries"
    ] * defaults["retry_delay"]
    assert longest <= arguments["dagrun_timeout"]
    google, _ = dag_file(hours)
    assert module.SCHEDULE == google.SCHEDULE


@pytest.mark.parametrize("dag_name", sorted(PLATFORMS))
def test_the_dags_of_meta_and_tiktok_have_no_schedule_at_0_and_refuse_another_period(
    dag_file, dag_name
):
    module, made = dag_file("0", dag_name)
    assert module.SCHEDULE is None
    assert made["dag"]["schedule"] is None
    with pytest.raises(ValueError, match=r"AGORANOMOS_INTRADAY_HOURS in \.env must be"):
        dag_file("5", dag_name)


@pytest.mark.parametrize("dag_name", sorted(PLATFORMS))
def test_the_load_runs_in_the_pool_of_its_platform_with_the_daily_load_and_its_own_asset(
    dag_file, dag_name
):
    _, pool, task_id, daily, daily_task, asset_name, daily_asset = PLATFORMS[dag_name]
    module, made = dag_file(None, dag_name)
    assert pool == module.LOAD_POOL
    assert daily == module.DAILY
    load = made["tasks"][task_id]
    assert load.arguments["pool"] == pool
    [asset] = load.arguments["outlets"]
    assert asset.uri == dag_name
    assets = sys.modules["agoranomos_pipelines.assets"]
    assert asset is getattr(assets, asset_name)
    assert asset in assets.LOADED
    assert asset is not getattr(assets, daily_asset)
    # The task of the daily load, and so its runs of the history, is in the same pool.
    text = (ROOT / "airflow" / "dags" / f"{daily}.py").read_text("utf-8")
    assert f'LOAD_POOL = "{pool}"' in text
    assert f'@task(task_id="{daily_task}", outlets=[{daily_asset}], pool=LOAD_POOL)' in text
    assert made["tasks"]["due"].kind == "short_circuit"
    assert "pool" not in made["tasks"]["due"].arguments


@pytest.mark.parametrize("dag_name", sorted(PLATFORMS))
def test_the_dags_of_meta_and_tiktok_end_with_the_check_of_their_accounts(dag_file, dag_name):
    # As for Google Ads: the check is the one task after the load, with none_failed, so that it
    # fails the run after a load that failed for some accounts and passes after one that skipped
    # (agoranomos_pipelines/outcome.py).
    task_id = PLATFORMS[dag_name][2]
    _, made = dag_file("3", dag_name)
    tasks = made["tasks"]
    assert tasks[task_id].upstream == ["due"]
    check = tasks["check_accounts"]
    assert check.upstream == [task_id]
    assert check.arguments["trigger_rule"] == "none_failed"
    assert "outlets" not in check.arguments
    assert "pool" not in check.arguments


@pytest.mark.parametrize("dag_name", sorted(PLATFORMS))
def test_a_run_of_meta_or_tiktok_skips_while_its_daily_load_runs_or_once_it_waited_too_long(
    dag_file, dag_name
):
    daily = PLATFORMS[dag_name][3]
    _, made = dag_file("3", dag_name)
    due = made["tasks"]["due"].function
    assert due() is True
    assert made["asked"] == [(daily, ("queued", "running"))]
    made["active"] = 1
    assert due() is False
    made["active"] = 0
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=3, minutes=1)
    assert due() is False


def stand_in_connector(monkeypatch, code: str, failure: str):
    """A stand-in of the module of a connector, agoranomos_pipelines.<code>, and of loads, whose
    load_intraday gives the answers that a test puts into the list it returns, after it records
    its moment and its arguments."""
    connector = types.ModuleType(f"agoranomos_pipelines.{code}")
    loads = types.ModuleType("agoranomos_pipelines.loads")
    loads.credentials = lambda: {"password": "loader-password"}
    connector.TOKEN, connector.APP_SECRET = "META_ADS_ACCESS_TOKEN", "META_ADS_APP_SECRET"

    class Failed(Exception):
        def __init__(self, line, loaded):
            super().__init__(line)
            self.loaded = loaded

    setattr(connector, failure, Failed)
    answers, calls = [], []

    def load_intraday(now, **arguments):
        calls.append((now, arguments))
        found = answers.pop(0)
        if isinstance(found, Exception):
            raise found
        return found

    connector.load_intraday = load_intraday
    package = importlib.import_module("agoranomos_pipelines")
    monkeypatch.setitem(sys.modules, f"agoranomos_pipelines.{code}", connector)
    monkeypatch.setitem(sys.modules, "agoranomos_pipelines.loads", loads)
    monkeypatch.setattr(package, code, connector, raising=False)
    monkeypatch.setattr(package, "loads", loads, raising=False)
    return answers, calls, Failed


@pytest.mark.parametrize(
    ("dag_name", "failure", "line"),
    [
        ("agoranomos_meta_ads_intraday", "AccountsFailed", "1 of 2 Meta ad accounts failed"),
        ("agoranomos_tiktok_ads_intraday", "AdvertisersFailed", "1 of 2 TikTok advertisers failed"),
    ],
)
def test_the_load_of_meta_or_tiktok_skips_without_a_whole_load_and_fails_with_the_line(
    dag_file, monkeypatch, dag_name, failure, line
):
    # A skipped task updates no asset, so the transform does not run for nothing; a load that
    # failed for some accounts returns the line of the connector, which check_accounts fails the
    # run with, and one that took none fails, as for Google Ads.
    code, _, task_id, *_ = PLATFORMS[dag_name]
    _, made = dag_file("3", dag_name)
    monkeypatch.setenv("META_ADS_ACCESS_TOKEN", "stand-in-token")
    monkeypatch.delenv("META_ADS_APP_SECRET", raising=False)
    answers, calls, failed = stand_in_connector(monkeypatch, code, failure)
    exceptions = sys.modules["airflow.sdk.exceptions"]
    load = made["tasks"][task_id].function
    answers.append({"campaigns": 3})
    before = datetime.now(UTC)
    assert load() is None
    [(now, arguments)] = calls
    # The moment of the read, which gives the current day of each account.
    assert before <= now <= datetime.now(UTC)
    assert arguments["mask"] == made["masked"].append
    if code == "meta_ads":
        # The Variables of Airflow keep a token that expires, as for the daily load.
        assert arguments["variables"] is sys.modules["airflow.sdk"].Variable
        assert made["masked"] == ["loader-password", "stand-in-token"]
    else:
        assert made["masked"] == ["loader-password"]
    answers.append(None)
    with pytest.raises(exceptions.AirflowSkipException, match=f"of {code} has had a whole load"):
        load()
    check = made["tasks"]["check_accounts"].function
    assert check(None) is None
    # One account loaded: the task succeeds, so that its asset starts the transform, and the last
    # task fails the run with the line.
    answers.append(failed(line, 1))
    assert load() == line
    with pytest.raises(exceptions.AirflowFailException, match=line):
        check(line)
    # None loaded: the task fails, and starts nothing.
    answers.append(failed(line, 0))
    with pytest.raises(exceptions.AirflowFailException, match=line):
        load()
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=4)
    with pytest.raises(exceptions.AirflowSkipException, match="waited longer than the period"):
        load()
    assert len(calls) == 4
