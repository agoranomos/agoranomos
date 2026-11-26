"""Tests of how a run of the daily load of Google Ads, of Meta, of TikTok or of GA4 ends when some of
its accounts failed (airflow/dags/agoranomos_pipelines/outcome.py, ADR-0035): the task of the load
succeeds when it took an account, so that its asset starts the transform for it, and returns the
line of the failed accounts, with which the last task, check_accounts, fails the run; a load that
took no account fails its task.

Airflow is not in the development environment, so each DAG file is imported with stand-ins of the
parts of airflow.sdk that it and the modules of the package use (airflow_stand_in.py), and with a
stand-in of its connector, whose load ends as each test says."""

import importlib
import sys
import types
from datetime import UTC, datetime

import pytest
from airflow_stand_in import WITH_AIRFLOW, forget, import_dag

RUN_AFTER = datetime(2026, 11, 26, 10, 37, tzinfo=UTC)
# Each DAG of a daily load of accounts: its task, its connector and the failure of the connector.
LOADS = {
    "agoranomos_google_ads_daily": ("load_google_ads", "google_ads", "AccountsFailed"),
    "agoranomos_meta_ads_daily": ("load_meta_ads", "meta_ads", "AccountsFailed"),
    "agoranomos_tiktok_ads_daily": ("load_tiktok_ads", "tiktok_ads", "AdvertisersFailed"),
    "agoranomos_ga4_daily": ("load_ga4", "ga4", "PropertiesFailed"),
}


class Failed(Exception):
    """The failure of a connector, as each connector raises it: the line, and how many accounts the
    load took."""

    def __init__(self, line: str, loaded: int) -> None:
        super().__init__(line)
        self.loaded = loaded


class StateStore:
    """The asset state store of the context of a task: the store of each asset by its URI."""

    def __getitem__(self, asset):
        return ("store of", asset.uri)


@pytest.fixture(params=sorted(LOADS))
def daily(request, monkeypatch):
    """The DAG of a daily load imported with stand-ins: what the import made, its two tasks, the
    end of the load of its connector in ends[0], None for counts or an exception to raise, and the
    calls of that load."""
    dag_id = request.param
    task_id, connector_name, failure = LOADS[dag_id]
    made = {"tasks": {}, "masked": [], "dag": None}
    context = {
        "dag_run": types.SimpleNamespace(run_after=RUN_AFTER),
        "params": {"accounts": [], "window_from": None, "window_to": None},
        "asset_state_store": StateStore(),
    }
    ends: list[Exception | None] = [None]
    calls: list[tuple] = []

    def load(now, **arguments):
        calls.append((now, arguments))
        if ends[0] is not None:
            raise ends[0]
        return {"rows": 3}

    connector = types.ModuleType(f"agoranomos_pipelines.{connector_name}")
    setattr(connector, failure, Failed)
    connector.load = load
    connector.portion_of = lambda accounts, first, last: None
    connector.key_info = lambda: {"private_key": "stand-in key"}
    connector.TOKEN, connector.APP_SECRET = "META_ADS_ACCESS_TOKEN", "META_ADS_APP_SECRET"
    loads = types.ModuleType("agoranomos_pipelines.loads")
    loads.credentials = lambda: {"password": "loader-password"}
    package = importlib.import_module("agoranomos_pipelines")
    for name, module in ((connector_name, connector), ("loads", loads)):
        monkeypatch.setitem(sys.modules, f"agoranomos_pipelines.{name}", module)
        monkeypatch.setattr(package, name, module, raising=False)
    imported = import_dag(monkeypatch, dag_id, made, context)
    yield types.SimpleNamespace(
        module=imported,
        made=made,
        load=made["tasks"][task_id],
        check=made["tasks"]["check_accounts"],
        ends=ends,
        calls=calls,
        exceptions=sys.modules["airflow.sdk.exceptions"],
    )
    forget(monkeypatch, (dag_id, *WITH_AIRFLOW))


def test_the_last_task_checks_the_accounts_after_the_load_and_is_the_one_leaf(daily):
    # none_failed: after a load that failed, Airflow marks the check upstream_failed, which fails
    # the run, as a run takes the state of its leaves alone (Airflow 3.3.2); all_done would let it
    # pass, and the run would end green.
    tasks = daily.made["tasks"]
    assert set(tasks) == {daily.load.arguments["task_id"], "check_accounts"}
    assert daily.load.upstream == []
    assert daily.check.upstream == [daily.load.arguments["task_id"]]
    assert daily.check.arguments == {"task_id": "check_accounts", "trigger_rule": "none_failed"}
    # The load updates its asset, and the check updates none and waits in no pool.
    [asset] = daily.load.arguments["outlets"]
    assert asset.uri.startswith("postgres://warehouse-db:5432/warehouse/raw_")


def test_a_load_whose_every_account_loaded_passes_the_check(daily):
    assert daily.load.function() is None
    [(now, arguments)] = daily.calls
    assert now == RUN_AFTER
    assert arguments["portion"] is None
    assert daily.check.function(None) is None
    assert "loader-password" in daily.made["masked"]


def test_a_load_that_took_some_accounts_succeeds_and_the_check_fails_the_run(daily):
    # The task succeeds, so that Airflow emits the event of its asset and the transform takes the
    # account that loaded; the line goes to the check, which fails the run, without a retry.
    line = "1 of 2 accounts failed, the other 1 is loaded: 1111111111: 403 PERMISSION_DENIED"
    daily.ends[0] = Failed(line, 1)
    assert daily.load.function() == line
    with pytest.raises(daily.exceptions.AirflowFailException) as raised:
        daily.check.function(line)
    assert str(raised.value) == line


def test_a_load_that_took_no_account_fails_its_task(daily):
    # Nothing for the transform: the task fails with the line, without a retry, and emits no event
    # of its asset; Airflow then marks the check upstream_failed.
    line = "2 of 2 accounts failed, none is loaded: 1111111111: 403; 2222222222: 403"
    daily.ends[0] = Failed(line, 0)
    with pytest.raises(daily.exceptions.AirflowFailException) as raised:
        daily.load.function()
    assert str(raised.value) == line
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__


def test_another_error_of_the_load_fails_its_task_for_a_retry(daily):
    # An error that is no failure of accounts, such as a lock timeout of the loader role, leaves
    # the task to Airflow as it is, not as AirflowFailException, so that Airflow retries it: the
    # retry loads every account again.
    daily.ends[0] = RuntimeError("canceling statement due to lock timeout")
    with pytest.raises(RuntimeError, match="lock timeout"):
        daily.load.function()
