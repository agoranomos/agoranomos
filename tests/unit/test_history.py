"""Tests of the planner of the history that Google Ads and Meta share
(airflow/dags/agoranomos_pipelines/history.py; ADR-0038, "The first load").

dlt is in the Airflow image and not in the development environment, so the module is imported with
a stand-in of dlt that loads nothing, and a stand-in pipeline that answers the queries of the plan
from what a test says the dataset holds. The plan of each source, with its accounts and its first
day, is tested with its own load, in test_google_ads_source.py and test_meta_ads_source.py."""

import importlib
import logging
import sys
import types
from datetime import date

import pytest

GOOGLE = ("google_ads", "raw_google_ads", ["1111111111", "2222222222"])
META = ("meta_ads", "raw_meta_ads", ["1111111111111111", "2222222222222222"])
SOURCES = [GOOGLE, META]
LIMIT = date(2023, 10, 1)


class UndefinedRelation(Exception):
    pass


class StandInClient:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def make_qualified_table_name(self, name):
        return f'"{self.pipeline.dataset_name}"."{name}"'

    def execute_sql(self, sql):
        self.pipeline.queries.append(sql)
        state = self.pipeline.state
        if sql.startswith("SELECT count(*) FROM ") and '"_dlt_loads"' in sql:
            if state["loads"] is None:
                raise UndefinedRelation(sql)
            return [(state["loads"],)]
        if sql.startswith("SELECT account_id, window_from, window_to FROM core.loads "):
            return state["core_windows"]
        if sql.startswith("SELECT count(DISTINCT j._dlt_load_id) "):
            return [(state["waiting"],)]
        if sql.startswith("SELECT j.account_id, j.window_from, j.window_to "):
            return state["windows"]
        raise AssertionError(f"an unexpected query: {sql}")


class StandInPipeline:
    def __init__(self, dataset_name, **state):
        self.dataset_name, self.queries = dataset_name, []
        self.state = {"loads": 1, "core_windows": [], "waiting": 0, "windows": [], **state}

    def sql_client(self):
        return StandInClient(self)


@pytest.fixture
def history(monkeypatch):
    modules = {
        "dlt": types.ModuleType("dlt"),
        "dlt.destinations": types.ModuleType("dlt.destinations"),
        "dlt.destinations.exceptions": types.ModuleType("dlt.destinations.exceptions"),
    }
    modules["dlt.destinations.exceptions"].DatabaseUndefinedRelation = UndefinedRelation
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    names = ("agoranomos_pipelines.history", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    yield importlib.import_module("agoranomos_pipelines.history")
    for name in names:
        sys.modules.pop(name, None)


def test_the_history_starts_on_the_first_day_of_the_month_36_months_before(history):
    # ADR-0034: the last 36 months, from the first day of their month.
    assert history.HISTORY_MONTHS == 36
    assert history.history_start(date(2026, 10, 28)) == date(2023, 10, 1)
    assert history.history_start(date(2026, 1, 1)) == date(2023, 1, 1)
    assert history.history_start(date(2026, 12, 31)) == date(2023, 12, 1)
    assert history.history_start(date(2026, 3, 31), 37) == date(2023, 2, 1)
    # The same day of a month before, or the last day of a shorter month.
    assert history.months_back(date(2026, 10, 29), 37) == date(2023, 9, 29)
    assert history.months_back(date(2026, 3, 31), 37) == date(2023, 2, 28)
    assert history.months_back(date(2027, 5, 31), 39) == date(2024, 2, 29)
    assert history.months_back(date(2026, 1, 15), 1) == date(2025, 12, 15)


def test_a_quarter_runs_from_its_first_to_its_last_day(history):
    assert history.quarter(date(2026, 2, 28)) == (date(2026, 1, 1), date(2026, 3, 31))
    assert history.quarter(date(2026, 4, 1)) == (date(2026, 4, 1), date(2026, 6, 30))
    assert history.quarter(date(2026, 12, 31)) == (date(2026, 10, 1), date(2026, 12, 31))


@pytest.mark.parametrize(
    ("usual", "count"),
    [
        # The usual window of Google Ads, 90 days, and of Meta, 28 days, both ending on 28 October
        # 2026 in one account and on the 27th in the other, in another time zone.
        (90, 12),
        (28, 12),
    ],
)
def test_the_history_goes_by_quarters_from_the_newest_back_to_the_limit(history, usual, count):
    # ADR-0038: a portion is one quarter across all the accounts that lack it, from the new
    # quarters to the old ones, until every account holds its days back to the limit; an account
    # without a load waits for its usual window.
    known = {
        "1": [(date.fromordinal(date(2026, 10, 27).toordinal() - usual + 1), date(2026, 10, 27))],
        "2": [(date.fromordinal(date(2026, 10, 28).toordinal() - usual + 1), date(2026, 10, 28))],
        "3": [],
    }
    portions = []
    while (portion := history.next_portion(known, LIMIT)) is not None:
        portions.append(portion)
        for account_id in portion.accounts:
            known[account_id].append((portion.first, portion.last))
        assert len(portions) < 20, "the history never ends"
    assert len(portions) == count
    assert [p.first for p in portions] == sorted((p.first for p in portions), reverse=True)
    for portion in portions:
        assert history.quarter(portion.first) == history.quarter(portion.last)
        assert portion.accounts == ("1", "2")
    assert (portions[-1].first, portions[-1].last) == (LIMIT, date(2023, 12, 31))
    assert history.missing(known["1"], LIMIT) == history.missing(known["2"], LIMIT) == []


def test_the_parameters_of_a_run_name_a_portion_with_the_form_of_the_platform(history):
    class PlatformError(RuntimeError):
        pass

    def portion(*args):
        return history.portion_from(
            *args, form=history.ACCOUNT_ID, shape="digits", error=PlatformError
        )

    assert portion(None, None, None) is None
    assert portion(["2", "1", "2"], "2026-01-01", "2026-03-31") == history.Portion(
        ("1", "2"), date(2026, 1, 1), date(2026, 3, 31)
    )
    assert history.Portion((), date(2026, 1, 1), date(2026, 3, 31)).conf() == {
        "accounts": [],
        "window_from": "2026-01-01",
        "window_to": "2026-03-31",
    }
    for args, message in (
        ((["1"], None, None), "names the days of their history too"),
        (([], "2026-01-01", None), "both window_from and window_to"),
        (([], "2026-02-30", "2026-03-31"), "are dates"),
        (([], "2026-04-01", "2026-03-31"), "is after window_to"),
        ((["act_1"], "2026-01-01", "2026-03-31"), "an account of a run is digits, not 'act_1'"),
    ):
        with pytest.raises(PlatformError, match=message):
            portion(*args)


@pytest.mark.parametrize(("source", "dataset", "accounts"), SOURCES)
def test_the_plan_of_a_source_reads_its_own_journal_and_its_rows_of_core_loads(
    history, caplog, source, dataset, accounts
):
    one, two = accounts
    pipeline = StandInPipeline(
        dataset,
        windows=[
            (one, date(2026, 7, 30), date(2026, 10, 27)),
            (one, None, None),
            (two, date(2026, 7, 31), date(2026, 10, 28)),
        ],
        core_windows=[(two, date(2026, 4, 1), date(2026, 7, 30))],
    )
    with caplog.at_level(logging.INFO):
        portion = history.planned(pipeline, source, "The platform", [two, one], LIMIT)
    assert portion == history.Portion((one,), date(2026, 7, 1), date(2026, 7, 29))
    assert "of 1 accounts (" + one + ") of The platform" in caplog.text
    # The queries as the plan sends them, with the names of the source and of its dataset.
    queries = [
        'SELECT count(*) FROM "DATASET"."_dlt_loads" WHERE status = 0',
        "SELECT count(DISTINCT j._dlt_load_id) "
        'FROM "DATASET"."_agoranomos_loads" AS j '
        'JOIN "DATASET"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE NOT EXISTS (SELECT FROM core.loads AS p "
        "WHERE p.source = 'SOURCE' AND p.load_id = j._dlt_load_id "
        "AND p.account_id = j.account_id)",
        'SELECT count(*) FROM "DATASET"."_dlt_loads" WHERE status = 0',
        "SELECT account_id, window_from, window_to FROM core.loads "
        "WHERE source = 'SOURCE' AND outcome = 'loaded' AND account_id IN ('ONE', 'TWO')",
        "SELECT j.account_id, j.window_from, j.window_to "
        'FROM "DATASET"."_agoranomos_loads" AS j '
        'JOIN "DATASET"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' AND j.account_id IN ('ONE', 'TWO')",
    ]
    names = {"DATASET": dataset, "SOURCE": source, "ONE": one, "TWO": two}
    for name, value in names.items():
        queries = [query.replace(name, value) for query in queries]
    assert pipeline.queries == queries


@pytest.mark.parametrize(("source", "dataset", "accounts"), SOURCES)
def test_the_plan_of_a_source_waits_for_its_own_loads_that_no_run_pinned(
    history, caplog, source, dataset, accounts
):
    pipeline = StandInPipeline(
        dataset, waiting=2, windows=[(accounts[0], date(2026, 7, 30), date(2026, 10, 27))]
    )
    with caplog.at_level(logging.INFO):
        assert history.planned(pipeline, source, "The platform", accounts, LIMIT) is None
    assert "2 whole loads of The platform wait for the run of the transform" in caplog.text
    # It reads the windows no more.
    assert not [q for q in pipeline.queries if "window_from" in q]


def test_the_plan_starts_nothing_before_a_first_load_or_once_the_history_is_whole(history, caplog):
    source, dataset, (one, two) = META
    # A dataset that no load wrote yet: no journal, and core.loads holds nothing of the source.
    empty = StandInPipeline(dataset, loads=None)
    assert history.waiting(empty, source) == 0
    assert history.known_windows(empty, source, [one]) == {one: []}
    assert history.planned(empty, source, "Meta", [one, two], LIMIT) is None
    whole = StandInPipeline(
        dataset,
        windows=[(one, LIMIT, date(2026, 10, 27)), (two, date(2023, 9, 1), date(2026, 10, 28))],
    )
    with caplog.at_level(logging.INFO):
        assert history.planned(whole, source, "Meta", [one, two], LIMIT) is None
    said = "Every account of Meta that has had its usual window holds its days back to 2023-10-01"
    assert said in caplog.text


def test_the_code_and_the_accounts_of_a_query_are_checked(history):
    pipeline = StandInPipeline("raw_meta_ads")
    for source, accounts in (
        ("meta_ads'; --", ["1"]),
        ("Meta", ["1"]),
        ("meta_ads", []),
        ("meta_ads", ["act_1"]),
        ("meta_ads", ["1' OR true --"]),
    ):
        with pytest.raises(ValueError, match=r"the code of a source|one account ID of digits"):
            history.known_windows(pipeline, source, accounts)
    with pytest.raises(ValueError, match="the code of a source is lower case letters"):
        history.waiting(pipeline, "meta ads")
    assert pipeline.queries == []
    assert history.checked("google_ads", ["2", "1"]) == ["1", "2"]
