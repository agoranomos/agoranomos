"""Tests of the load of the reference rates of the ECB (airflow/dags/agoranomos_pipelines/ecb.py).

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing and asks the ECB nothing: its client answers with the
CSV of a test and records what it was asked, and its pipeline records the resources of each run
and answers from what the test says the dataset holds."""

import importlib
import sys
import types
from datetime import UTC, date, datetime

import pytest

# A Thursday, at 03:00 in Frankfurt (CET).
NOW = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
CSV = (
    "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-27,1.1121\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-28,1.1133\n"
)


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
        return f'"{self.pipeline.options["dataset_name"]}"."{name}"'

    def execute_sql(self, sql):
        self.pipeline.queries.append(sql)
        if self.pipeline.state["loads"] is None:
            raise UndefinedRelation(sql)
        if '"_agoranomos_loads"' in sql:
            return self.pipeline.state["reloads"]
        return [(self.pipeline.state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None
        self.formats, self.contracts, self.queries = [], [], []

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources, loader_file_format=None, schema_contract=None):
        self.formats.append(loader_file_format)
        self.contracts.append(schema_contract)
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        self.last_trace = types.SimpleNamespace(
            last_extract_info=types.SimpleNamespace(
                metrics={
                    "1": [
                        {
                            "table_metrics": {
                                name: types.SimpleNamespace(items_count=rows)
                                for name, rows in counts.items()
                            }
                        }
                    ]
                }
            )
        )


class Resource:
    def __init__(self, data, name, **hints):
        self.data, self.name, self.hints = data, name, hints


def resource(data, name, **hints):
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into the dict of its
    hint, in place."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


@pytest.fixture
def ecb(monkeypatch):
    """The module with a stand-in for dlt, and what it made and was asked: the pipelines, the
    clients with their settings, and the requests. state["loads"] is how many complete loads the
    dataset holds (None: no table _dlt_loads yet), state["reloads"] the rows that the journal of
    the whole loads gives, and state["csv"] what the ECB answers."""
    state = {"loads": None, "reloads": [], "csv": CSV}
    made = {"pipelines": [], "clients": [], "requests": []}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def get(self, url, params):
            made["requests"].append((url, params))
            return types.SimpleNamespace(text=state["csv"])

    dlt = types.ModuleType("dlt")
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        made["pipelines"].append(StandInPipeline(options, state)) or made["pipelines"][-1]
    )
    dlt.destinations = types.SimpleNamespace(
        postgres=lambda credentials, create_indexes: ("postgres", credentials, create_indexes)
    )
    exceptions = types.ModuleType("dlt.destinations.exceptions")
    exceptions.DatabaseUndefinedRelation = UndefinedRelation
    requests = types.ModuleType("dlt.sources.helpers.requests")
    requests.Client = Client
    for name, module in {
        "dlt": dlt,
        "dlt.destinations": types.ModuleType("dlt.destinations"),
        "dlt.destinations.exceptions": exceptions,
        "dlt.sources": types.ModuleType("dlt.sources"),
        "dlt.sources.helpers": types.ModuleType("dlt.sources.helpers"),
        "dlt.sources.helpers.requests": requests,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    # The settings of dlt that a load sets go when the test ends.
    for name in (
        "LOAD__TRUNCATE_STAGING_DATASET",
        "LOAD__WORKERS",
    ):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    names = ("agoranomos_pipelines.ecb", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.ecb")
    yield module, state, made
    for name in names:
        sys.modules.pop(name, None)


def test_a_load_ends_yesterday_in_frankfurt(ecb):
    module, _, _ = ecb
    assert module.yesterday(NOW) == date(2026, 10, 28)
    # 23:30 UTC is already the next day in Frankfurt, in CET and in CEST.
    assert module.yesterday(datetime(2026, 10, 28, 23, 30, tzinfo=UTC)) == date(2026, 10, 28)
    assert module.yesterday(datetime(2026, 7, 14, 22, 30, tzinfo=UTC)) == date(2026, 7, 14)


def test_the_first_load_takes_37_months_one_before_the_history_of_google_ads(ecb):
    module, _, _ = ecb
    assert module.window(NOW, None) == (date(2023, 9, 1), date(2026, 10, 28))
    assert module.history_start(datetime(2026, 1, 15, 12, tzinfo=UTC)) == date(2022, 12, 1)
    assert module.history_start(datetime(2026, 12, 31, 12, tzinfo=UTC)) == date(2023, 11, 1)


def test_a_later_load_takes_the_new_days_and_at_least_the_last_seven(ecb):
    module, _, _ = ecb
    # The day after the last loaded, when that is earlier than the last seven days.
    assert module.window(NOW, date(2026, 10, 10)) == (date(2026, 10, 11), date(2026, 10, 28))
    # The last seven days, when a load took them already, so that a corrected rate comes again.
    assert module.window(NOW, date(2026, 10, 28)) == (date(2026, 10, 22), date(2026, 10, 28))


def test_a_load_asks_the_ecb_for_the_data_of_its_window_with_explicit_retries(ecb):
    module, state, made = ecb
    state.update(loads=1, reloads=[("EUR", date(2026, 10, 28), date(2026, 10, 27))])
    module.load(NOW)
    assert made["requests"] == [
        (
            "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A",
            {
                "format": "csvdata",
                "detail": "dataonly",
                "startPeriod": "2026-10-22",
                "endPeriod": "2026-10-28",
            },
        )
    ]
    assert made["clients"] == [
        {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}
    ]


def test_a_load_appends_the_rows_as_the_csv_names_them(ecb):
    module, _, made = ecb
    assert module.load(NOW) == 2
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_ecb"
    assert pipeline.options["pipeline_name"] == "agoranomos_ecb"
    [(rows, hints)] = pipeline.runs
    assert pipeline.formats == ["csv"]
    assert pipeline.contracts == [{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}]
    # dlt makes no index on _dlt_id of a new table (ADR-0035).
    assert pipeline.options["destination"][2] is False
    assert rows["rates_daily"][0] == {
        "KEY": "EXR.D.USD.EUR.SP00.A",
        "FREQ": "D",
        "CURRENCY": "USD",
        "CURRENCY_DENOM": "EUR",
        "EXR_TYPE": "SP00",
        "EXR_SUFFIX": "A",
        "TIME_PERIOD": "2026-10-27",
        "OBS_VALUE": "1.1121",
    }
    # Each load appends its rows, and staging reads the newest whole load of each day (ADR-0038).
    assert hints["rates_daily"]["write_disposition"] == "append"
    # Every column stays text, as the ECB writes it, and each hint has a dict of its own.
    columns = hints["rates_daily"]["columns"]
    assert {name: hint["data_type"] for name, hint in columns.items()} == dict.fromkeys(
        ("key", "freq", "currency", "currency_denom", "exr_type", "exr_suffix", "time_period"),
        "text",
    ) | {"obs_value": "text"}
    assert all(hint["name"] == name for name, hint in columns.items())


def test_a_load_records_its_window_in_the_same_run(ecb):
    module, _, made = ecb
    module.load(NOW)
    [(rows, hints)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert {key: record[key] for key in ("account_id", "as_of", "window_from", "window_to")} == {
        "account_id": "EUR",
        "as_of": date(2026, 10, 29),
        "window_from": date(2023, 9, 1),
        "window_to": date(2026, 10, 28),
    }
    assert record["now"] == NOW
    assert record["status"] == "loaded"
    assert hints["_agoranomos_loads"]["write_disposition"] == "append"
    assert set(hints["_agoranomos_loads"]["columns"]) == {*record, "error"}


def test_a_window_without_a_rate_still_replaces_its_days(ecb):
    # The ECB answers a weekend with an empty body: the marker makes dlt make the table, and the
    # journal holds the window, whose days staging then reads as empty.
    module, state, made = ecb
    state.update(csv="", loads=1, reloads=[("EUR", date(2026, 10, 25), date(2026, 10, 24))])
    assert module.load(datetime(2026, 10, 26, 6, 47, tzinfo=UTC)) == 0
    [(rows, _)] = made["pipelines"][0].runs
    assert rows["rates_daily"] == [MARKER]
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["window_to"]) == (date(2026, 10, 19), date(2026, 10, 25))


def test_a_window_starts_after_the_last_whole_load(ecb):
    # The journal counts the loads that dlt finished, with their row in _dlt_loads: the days of a
    # load that stopped half way are taken again.
    module, state, made = ecb
    state.update(loads=1, reloads=[("EUR", date(2026, 10, 28), date(2026, 10, 27))])
    module.load(NOW)
    [query] = [q for q in made["pipelines"][0].queries if "_agoranomos_loads" in q]
    assert query == (
        "SELECT j.account_id, max(j.as_of), max(j.window_to) "
        'FROM "raw_ecb"."_agoranomos_loads" AS j JOIN "raw_ecb"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' GROUP BY j.account_id"
    )


def test_a_dataset_of_a_release_before_v0_5_takes_the_history_again(ecb):
    # Its loads have no row in _agoranomos_loads, which a release before v0.5 did not write: the
    # load takes the history, and the rows of the earlier loads stay unread by staging.
    module, state, made = ecb
    state.update(loads=4, reloads=[])
    module.load(NOW)
    [(rows, _)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert record["window_from"] == date(2023, 9, 1)


def test_a_load_is_due_until_one_has_taken_the_day_in_frankfurt(ecb):
    module, state, _ = ecb
    assert module.due(NOW)
    state.update(loads=1, reloads=[("EUR", date(2026, 10, 28), date(2026, 10, 27))])
    assert module.due(NOW)
    state.update(reloads=[("EUR", date(2026, 10, 29), date(2026, 10, 28))])
    assert not module.due(NOW)
