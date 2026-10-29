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
        if self.pipeline.state["loads"] is None:
            raise UndefinedRelation(sql)
        if '"_agoranomos_reloads"' in sql:
            return self.pipeline.state["reloads"]
        return [(self.pipeline.state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources):
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        self.last_trace = types.SimpleNamespace(
            last_normalize_info=types.SimpleNamespace(row_counts=counts)
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
    dataset holds (None: no table _dlt_loads yet), state["reloads"] the rows that the record of
    the loads gives, and state["csv"] what the ECB answers."""
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
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: ("postgres", credentials))
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
    monkeypatch.setenv("LOAD__TRUNCATE_STAGING_DATASET", "unset")
    monkeypatch.delenv("LOAD__TRUNCATE_STAGING_DATASET")
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


def test_the_first_load_takes_the_months_of_two_years_back(ecb):
    module, _, _ = ecb
    assert module.window(NOW, None) == (date(2024, 10, 1), date(2026, 10, 28))
    assert module.history_start(datetime(2026, 1, 15, 12, tzinfo=UTC)) == date(2024, 1, 1)
    assert module.history_start(datetime(2026, 12, 31, 12, tzinfo=UTC)) == date(2024, 12, 1)


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


def test_a_load_keeps_the_rows_as_the_csv_names_them_and_replaces_its_days(ecb):
    module, state, made = ecb
    assert module.load(NOW) == 2
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_ecb"
    assert pipeline.options["pipeline_name"] == "agoranomos_ecb"
    [(rows, hints)] = pipeline.runs
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
    assert hints["rates_daily"]["write_disposition"] == {
        "disposition": "merge",
        "strategy": "delete-insert",
        "destination_scope": "time_period BETWEEN '2024-10-01' AND '2026-10-28'",
    }
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
    [record] = rows["_agoranomos_reloads"]
    assert {key: record[key] for key in ("account_id", "as_of", "window_start", "window_end")} == {
        "account_id": "EUR",
        "as_of": date(2026, 10, 29),
        "window_start": date(2024, 10, 1),
        "window_end": date(2026, 10, 28),
    }
    assert record["now"] == NOW
    assert hints["_agoranomos_reloads"]["write_disposition"] == "append"


def test_a_window_without_a_rate_still_replaces_its_days(ecb):
    # The ECB answers a weekend with an empty body: the marker makes dlt delete the days anyway.
    module, state, made = ecb
    state.update(csv="", loads=1, reloads=[("EUR", date(2026, 10, 25), date(2026, 10, 24))])
    assert module.load(datetime(2026, 10, 26, 6, 47, tzinfo=UTC)) == 0
    [(rows, _)] = made["pipelines"][0].runs
    assert rows["rates_daily"] == [MARKER]


def test_a_load_is_due_until_one_has_taken_the_day_in_frankfurt(ecb):
    module, state, _ = ecb
    assert module.due(NOW)
    state.update(loads=1, reloads=[("EUR", date(2026, 10, 28), date(2026, 10, 27))])
    assert module.due(NOW)
    state.update(reloads=[("EUR", date(2026, 10, 29), date(2026, 10, 28))])
    assert not module.due(NOW)
