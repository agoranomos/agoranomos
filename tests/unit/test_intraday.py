"""Tests of the short cycle of the current day (ADR-0051): its period and its days
(airflow/dags/agoranomos_pipelines/intraday.py), and the DAGs of Google Ads, of Meta, of TikTok
and of the demo that run it (airflow/dags/agoranomos_google_ads_intraday.py,
agoranomos_meta_ads_intraday.py, agoranomos_tiktok_ads_intraday.py, agoranomos_demo_intraday.py).

Airflow is not in the development environment, so the DAG file is imported with stand-ins of the
parts of airflow.sdk that it and the module of the assets use: the decorators keep the function of
each task, so that a test calls it with a context of its own."""

import importlib
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import intraday

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


class Node:
    """A task in the stand-in of a DAG, which >> chains."""

    def __rshift__(self, other):
        return other


class StandInTask:
    """A task of the stand-in of airflow.sdk: the decorated function, with its arguments."""

    def __init__(self, function, kind, arguments):
        self.function, self.kind, self.arguments = function, kind, arguments

    def __call__(self):
        return Node()


def stand_in_sdk(made: dict, context: dict) -> dict[str, types.ModuleType]:
    """The parts of airflow.sdk that the DAG file and the module of the assets import."""
    sdk = types.ModuleType("airflow.sdk")

    class Asset:
        def __init__(self, uri):
            self.uri = uri

    sdk.Asset = Asset
    sdk.AssetAll = sdk.AssetAny = lambda *assets: assets

    def task(**arguments):
        return lambda function: made["tasks"].setdefault(
            arguments["task_id"], StandInTask(function, "task", arguments)
        )

    def short_circuit(**arguments):
        return lambda function: made["tasks"].setdefault(
            arguments["task_id"], StandInTask(function, "short_circuit", arguments)
        )

    task.short_circuit = short_circuit
    sdk.task = task

    def dag(**arguments):
        def decorate(function):
            def build():
                made["dag"] = arguments
                function()

            return build

        return decorate

    sdk.dag = dag
    # The Variables of Airflow, which the short cycle of Meta passes to its load.
    sdk.Variable = types.SimpleNamespace(name="Variable")
    sdk.CronTriggerTimetable = lambda cron, timezone: ("cron", cron, timezone)
    sdk.get_current_context = lambda: context
    exceptions = types.ModuleType("airflow.sdk.exceptions")

    class AirflowFailException(Exception):
        pass

    class AirflowSkipException(Exception):
        pass

    exceptions.AirflowFailException = AirflowFailException
    exceptions.AirflowSkipException = AirflowSkipException
    log = types.ModuleType("airflow.sdk.log")
    log.mask_secret = made["masked"].append
    return {
        "airflow": types.ModuleType("airflow"),
        "airflow.sdk": sdk,
        "airflow.sdk.exceptions": exceptions,
        "airflow.sdk.log": log,
    }


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
        "agoranomos_pipelines.assets",
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
        for name, module in stand_in_sdk(made, context).items():
            monkeypatch.setitem(sys.modules, name, module)
        if hours is None:
            monkeypatch.delenv(intraday.SETTING, raising=False)
        else:
            monkeypatch.setenv(intraday.SETTING, hours)
        for name in names:
            sys.modules.pop(name, None)
        return importlib.import_module(dag_name), made

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
        pass

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
    answers.append({"campaign_stats": 3})
    assert load() is None
    assert made["masked"] == ["loader-password", "stand-in key"]
    answers.append(None)
    with pytest.raises(exceptions.AirflowSkipException, match="no account of google_ads"):
        load()

    def failing(now):
        raise AccountsFailed("1 of 2 Google Ads accounts failed")

    google_ads.load_intraday = failing
    with pytest.raises(exceptions.AirflowFailException, match="1 of 2"):
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


def test_compose_sets_the_pools_and_gives_the_setting_to_every_airflow_service():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    for pool in ("google_ads_load", "meta_ads_load", "tiktok_ads_load"):
        assert f'"{pool}",\n            "1",' in compose, pool
    # In the environment that every Airflow service shares, the DAG processor among them.
    shared = compose.split("environment: &airflow-environment\n")[1].split("\n  volumes:")[0]
    assert "    AGORANOMOS_INTRADAY_HOURS: ${AGORANOMOS_INTRADAY_HOURS:-3}\n" in shared
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "\nAGORANOMOS_INTRADAY_HOURS=3\n" in example
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
@pytest.mark.parametrize(("hours", "cron"), [(None, "27 */3 * * *"), ("6", "27 */6 * * *")])
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
        pass

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
    # failed for some accounts fails without a retry, with the line of the connector, as the daily
    # load does.
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
    answers.append(failed(line))
    with pytest.raises(exceptions.AirflowFailException, match=line):
        load()
    made["context"]["dag_run"].run_after = datetime.now(UTC) - timedelta(hours=4)
    with pytest.raises(exceptions.AirflowSkipException, match="waited longer than the period"):
        load()
    assert len(calls) == 3
