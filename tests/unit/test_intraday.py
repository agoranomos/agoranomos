"""Tests of the short cycle of the current day (ADR-0051): its period and its days
(airflow/dags/agoranomos_pipelines/intraday.py), and the DAGs of Google Ads and of the demo that
run it (airflow/dags/agoranomos_google_ads_intraday.py, agoranomos_demo_intraday.py).

Airflow is not in the development environment, so the DAG file is imported with stand-ins of the
parts of airflow.sdk that it and the modules of the package use (airflow_stand_in.py): the
decorators keep the function of each task, so that a test calls it with a context of its own."""

import importlib
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import intraday
from airflow_stand_in import WITH_AIRFLOW, forget, import_dag

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 29, 12, 27, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "cron"),
    [
        ("1", "27 * * * *"),
        ("2", "27 */2 * * *"),
        ("3", "27 */3 * * *"),
        ("4", "27 */4 * * *"),
        ("6", "27 */6 * * *"),
        ("8", "27 */8 * * *"),
        ("12", "27 */12 * * *"),
        ("24", "27 0 * * *"),
    ],
)
def test_each_divisor_of_24_hours_is_a_schedule_at_a_fixed_minute_in_utc(value, cron):
    # The loads fall at the same hours every day, from midnight in UTC.
    hours = intraday.period(value)
    assert hours == int(value)
    assert intraday.cron(hours) == cron


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


def test_the_minute_keeps_away_from_the_daily_loads_and_the_watcher():
    # Meta at 10:07 and Google Ads at 10:37 UTC, the watcher at 4, 19, 34 and 49 past each hour.
    taken = set()
    for name in ("agoranomos_meta_ads_daily.py", "agoranomos_google_ads_daily.py"):
        text = (ROOT / "airflow" / "dags" / name).read_text(encoding="utf-8")
        [line] = [line for line in text.splitlines() if line.startswith("SCHEDULE = ")]
        taken.add(int(line.split('("')[1].split()[0]))
    assert taken == {7, 37}
    assert intraday.MINUTE not in taken | {4, 19, 34, 49}


@pytest.fixture
def dag_file(monkeypatch):
    """A function that imports a DAG file of the short cycle, that of Google Ads unless dag_name
    says otherwise, with a period, and what the import made: the arguments of the DAG, its tasks,
    the secrets that it masked, the context of its tasks, whose runs of another DAG and whose
    moments a test sets, and the DAGs whose runs a task asked for."""
    names = (
        "agoranomos_google_ads_intraday",
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
                ),
                # The second and last try of a task with retries=1, as the load of the cycle.
                try_number=2,
                max_tries=1,
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


@pytest.mark.parametrize(("hours", "cron"), [(None, "27 */3 * * *"), ("1", "27 * * * *")])
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

    # On the first try, which Airflow retries five minutes later, the failure is raised for the
    # retry, partial or whole.
    ti = made["context"]["ti"]
    ti.try_number = 1
    for loaded in (1, 0):
        google_ads.load_intraday = failing(loaded)
        with pytest.raises(AccountsFailed):
            load()
    # On the last try, with one account loaded, the task succeeds, so that its asset starts the
    # transform, and the last task fails the run with the line.
    ti.try_number = 2
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


@pytest.mark.parametrize(("hours", "cron"), [(None, "27 */3 * * *"), ("6", "27 */6 * * *")])
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


def test_compose_sets_the_pool_and_gives_the_setting_to_every_airflow_service():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert '"google_ads_load",\n            "1",' in compose
    # In the environment that every Airflow service shares, the DAG processor among them.
    shared = compose.split("environment: &airflow-environment\n")[1].split("\n  volumes:")[0]
    assert "    AGORANOMOS_INTRADAY_HOURS: ${AGORANOMOS_INTRADAY_HOURS:-3}\n" in shared
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "\nAGORANOMOS_INTRADAY_HOURS=3\n" in example
    # The profile of an agency turns it off until a cycle is measured on it (ADR-0051).
    agency = example.split("# Agency profile (ADR-0040)")[1].split("\n\n")[0]
    assert "\n#   AGORANOMOS_INTRADAY_HOURS=0" in agency
