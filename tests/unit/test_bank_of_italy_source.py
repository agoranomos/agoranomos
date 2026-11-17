"""Tests of the load of the exchange rates of the Bank of Italy
(airflow/dags/agoranomos_pipelines/bank_of_italy.py), and of its shape in dbt.

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing and asks the bank nothing: its client answers each day
from the answers that fixtures/bank_of_italy/ recorded, as the bank wrote them on 16 November 2026
for 12 November 2026, a Thursday, and 8 November 2026, a Sunday, with the notice of the bank kept
as data, and records what it was asked; its pipeline records the resources of each run and
answers from what the test says the dataset holds."""

import copy
import importlib
import json
import re
import sys
import types
from collections import Counter
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bank_of_italy"
THURSDAY = json.loads((FIXTURES / "daily_rates_2026-11-12.json").read_text(encoding="utf-8"))
SUNDAY = json.loads((FIXTURES / "daily_rates_2026-11-08.json").read_text(encoding="utf-8"))
SHAPE = REPO_ROOT / "dbt" / "macros" / "shapes" / "bank_of_italy_rates.sql"
STAGING = REPO_ROOT / "dbt" / "models" / "staging" / "bank_of_italy"

# A Friday, at 08:17 in Rome (CET).
NOW = datetime(2026, 11, 13, 7, 17, tzinfo=UTC)
MARKER = "<materialize_table_schema>"


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
        if "to_jsonb(j)" in sql or sql.startswith("SELECT count(*) FROM core.loads"):
            # How many loads of the history wait for the core (rates.waiting_sql).
            return [(self.pipeline.state["waiting"],)]
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
    """As dlt.resource of dlt 1.32.0, which writes the name of each column into the dict of its
    hint, in place."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


@pytest.fixture
def bank(monkeypatch):
    """The module with a stand-in for dlt, and what it made and was asked: the pipelines, the
    clients with their settings, the requests with their headers, and the waits between them.
    state["loads"] is how many complete loads the dataset holds (None: no table _dlt_loads yet),
    state["reloads"] the rows that the journal of the whole loads gives, and state["answers"] what
    the bank answers for each day, by default the Thursday for a weekday that it does not name.
    state["core"] is what the core says of its rates, as rates.read_core reads it as transformer
    (None: no view yet), and state["waiting"] how many loads of the history wait for the core."""
    state = {"loads": None, "reloads": [], "answers": {}, "core": None, "waiting": 0}
    made = {"pipelines": [], "clients": [], "requests": [], "waits": [], "clock": [100.0]}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def get(self, url, params, headers):
            made["requests"].append((url, params, headers, made["clock"][0]))
            # Each answer takes a quarter of a second.
            made["clock"][0] += 0.25
            day = date.fromisoformat(params["referenceDate"])
            answer = copy.deepcopy(state["answers"].get(day, THURSDAY))
            return types.SimpleNamespace(json=lambda: answer)

    def sleep(seconds):
        made["waits"].append(seconds)
        made["clock"][0] += seconds

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
    for name in ("LOAD__TRUNCATE_STAGING_DATASET", "LOAD__WORKERS"):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    names = ("agoranomos_pipelines.bank_of_italy", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.bank_of_italy")
    monkeypatch.setattr(module.rates, "read_core", lambda: state["core"])
    monkeypatch.setattr(module, "sleep", sleep)
    monkeypatch.setattr(module, "monotonic", lambda: made["clock"][0])
    monkeypatch.setenv("AGORANOMOS_REPORTING_CURRENCY", "GBP")
    yield module, state, made
    for name in names:
        sys.modules.pop(name, None)


def test_the_recorded_answers_have_the_quirks_that_the_load_and_staging_handle():
    # As the bank answered on 16 November 2026: the brief of #222 read the same on 13 November.
    info = THURSDAY["resultsInfo"]
    assert info["totalRecords"] == len(THURSDAY["rates"]) == 171
    assert info["timezoneReference"] == "Dates refer to the Central European Time Zone"
    # The notice of the bank, kept as data: the rates are indicative.
    assert (
        "are only indicative and are not intended to be used as a financial benchmark"
        in (info["notice"])
    )
    assert SUNDAY["resultsInfo"]["notice"] == info["notice"]
    codes = Counter(item["isoCode"] for item in THURSDAY["rates"])
    assert len(codes) == 154
    assert {code: n for code, n in codes.items() if n > 1} == {"XAF": 6, "XCD": 6, "XOF": 8}
    assert sorted(i["isoCode"] for i in THURSDAY["rates"] if i["avgRate"] == "N.A.") == [
        "BYN",
        "IRR",
        "KPW",
        "RUB",
    ]
    assert {i["exchangeConventionCode"] for i in THURSDAY["rates"]} == {"C"}
    assert {i["exchangeConvention"] for i in THURSDAY["rates"]} == {
        "Foreign currency amount for 1 Euro."
    }
    assert {i["referenceDate"] for i in THURSDAY["rates"]} == {"2026-11-12"}
    # A day without quotations has no item.
    assert SUNDAY["resultsInfo"]["totalRecords"] == 0
    assert SUNDAY["rates"] == []


def test_a_load_ends_yesterday_in_rome(bank):
    module, _, _ = bank
    assert module.yesterday(NOW) == date(2026, 11, 12)
    # 23:30 UTC is already the next day in Rome, in CET and in CEST.
    assert module.yesterday(datetime(2026, 11, 12, 23, 30, tzinfo=UTC)) == date(2026, 11, 12)
    assert module.yesterday(datetime(2026, 7, 14, 22, 30, tzinfo=UTC)) == date(2026, 7, 14)


def test_the_window_and_the_history_are_those_of_the_ecb(bank):
    module, _, _ = bank
    from agoranomos_pipelines import ecb, windows

    assert module.HISTORY_MONTHS == ecb.HISTORY_MONTHS == 37
    # The window of restatements of each, in the seed of the platforms (windows.py).
    assert windows.restatement_window_days(module.CODE) == 14
    assert windows.restatement_window_days(ecb.CODE) == 14
    # The first load takes the history, from the first day of the month 37 months back.
    assert module.window(NOW, None) == (date(2023, 10, 1), date(2026, 11, 12), "history")
    # A later one, the day after the last loaded, or the last fourteen days when that is earlier.
    assert module.window(NOW, date(2026, 10, 20)) == (
        date(2026, 10, 21),
        date(2026, 11, 12),
        "window",
    )
    assert module.window(NOW, date(2026, 11, 12)) == (
        date(2026, 10, 30),
        date(2026, 11, 12),
        "window",
    )
    # And the history that the core needs (rates.history_from), when it is earlier.
    last = date(2026, 11, 12)
    assert module.window(NOW, last, date(2024, 1, 1)) == (date(2024, 1, 1), last, "history")
    assert module.window(NOW, last, date(2026, 11, 5)) == (date(2026, 10, 30), last, "window")
    assert module.window(NOW, None, date(2022, 1, 1)) == (date(2022, 1, 1), last, "history")


def test_the_history_asks_for_the_weekdays_of_37_months(bank):
    module, _, _ = bank
    first, last, _ = module.window(NOW, None)
    days = module.weekdays(first, last)
    # About 800 requests: 814 weekdays from 1 October 2023 to 12 November 2026.
    assert len(days) == 814
    assert all(day.weekday() < 5 for day in days)
    assert module.weekdays(date(2026, 11, 7), date(2026, 11, 9)) == [date(2026, 11, 9)]


def test_a_load_asks_for_each_weekday_of_its_window_in_json_against_the_euro(bank):
    module, state, made = bank
    state.update(loads=1, reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))])
    module.load(NOW)
    # Fourteen days from Friday 30 October to Thursday 12 November: ten weekdays.
    asked = [params["referenceDate"] for _, params, _, _ in made["requests"]]
    assert asked == [
        "2026-10-30",
        "2026-11-02",
        "2026-11-03",
        "2026-11-04",
        "2026-11-05",
        "2026-11-06",
        "2026-11-09",
        "2026-11-10",
        "2026-11-11",
        "2026-11-12",
    ]
    url, params, headers, _ = made["requests"][0]
    assert url == ("https://tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0/dailyRates")
    assert params == {"referenceDate": "2026-10-30", "currencyIsoCode": "EUR", "lang": "en"}
    assert headers == {"Accept": "application/json"}
    # One client for the load, with explicit retries.
    assert made["clients"] == [
        {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}
    ]


def test_a_load_asks_at_most_once_a_second(bank):
    module, state, made = bank
    state.update(loads=1, reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))])
    module.load(NOW)
    moments = [moment for *_, moment in made["requests"]]
    assert all(b - a >= module.PACE_SECONDS for a, b in pairwise(moments))
    # An answer of a quarter of a second leaves three quarters to wait, and the first request
    # waits for nothing.
    assert made["waits"] == [0.75] * 9


def test_a_slow_answer_needs_no_wait(bank):
    module, state, made = bank
    state.update(loads=1, reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))])
    original = module.fetch

    def slow(client, day):
        made["clock"][0] += 2
        return original(client, day)

    module.fetch = slow
    module.load(NOW)
    assert made["waits"] == []


def test_a_load_appends_each_item_as_the_bank_names_its_fields_all_text(bank):
    module, state, made = bank
    # Every weekday of the window but 12 November answers with no item, and that one with its 171.
    empty = module.weekdays(date(2026, 10, 30), date(2026, 11, 11))
    state.update(
        loads=1,
        reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))],
        answers=dict.fromkeys(empty, SUNDAY),
    )
    assert module.load(NOW) == 171
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_bank_of_italy"
    assert pipeline.options["pipeline_name"] == "agoranomos_bank_of_italy"
    assert pipeline.formats == ["csv"]
    assert pipeline.contracts == [{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}]
    # dlt makes no index on _dlt_id of a new table (ADR-0035).
    assert pipeline.options["destination"][2] is False
    [(rows, hints)] = pipeline.runs
    assert rows["rates_daily"][0] == {
        "country": "AFGHANISTAN (Islamic State of)",
        "currency": "Afghani",
        "isoCode": "AFN",
        "uicCode": "115",
        "avgRate": "72.3811",
        "exchangeConvention": "Foreign currency amount for 1 Euro.",
        "exchangeConventionCode": "C",
        "referenceDate": "2026-11-12",
    }
    # Every item, N.A. and the countries of one currency too: staging reads them.
    assert len(rows["rates_daily"]) == 171
    [rub] = [row for row in rows["rates_daily"] if row["isoCode"] == "RUB"]
    assert rub["avgRate"] == "N.A."
    assert hints["rates_daily"]["write_disposition"] == "append"
    columns = hints["rates_daily"]["columns"]
    assert {name: hint["data_type"] for name, hint in columns.items()} == dict.fromkeys(
        (
            "country",
            "currency",
            "iso_code",
            "uic_code",
            "avg_rate",
            "exchange_convention",
            "exchange_convention_code",
            "reference_date",
        ),
        "text",
    )
    assert all(hint["name"] == name for name, hint in columns.items())


def test_a_value_that_is_not_text_is_written_as_text(bank):
    module, state, _ = bank
    item = {**THURSDAY["rates"][0], "avgRate": 72.3811, "uicCode": None}
    state.update(answers={date(2026, 11, 12): {"resultsInfo": {}, "rates": [item]}})
    rows = list(module.rows(date(2026, 11, 12), date(2026, 11, 12)))
    assert rows[0]["avgRate"] == "72.3811"
    assert rows[0]["uicCode"] is None


def test_an_answer_without_its_list_of_rates_fails_the_load(bank):
    module, state, _ = bank
    state.update(answers={date(2026, 11, 12): {"resultsInfo": {"totalRecords": 0}}})
    with pytest.raises(KeyError):
        list(module.rows(date(2026, 11, 12), date(2026, 11, 12)))


def test_a_load_records_its_window_in_the_same_run(bank):
    module, _, made = bank
    module.load(NOW)
    [(rows, hints)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert {key: record[key] for key in ("account_id", "as_of", "window_from", "window_to")} == {
        "account_id": "EUR",
        "as_of": date(2026, 11, 13),
        "window_from": date(2023, 10, 1),
        "window_to": date(2026, 11, 12),
    }
    assert record["now"] == NOW
    assert record["status"] == "loaded"
    # The first load is a load of the history, and the pin copies its kind (ADR-0038).
    assert record["kind"] == "history"
    assert hints["_agoranomos_loads"]["write_disposition"] == "append"
    assert set(hints["_agoranomos_loads"]["columns"]) == {*record, "error"}
    assert len(made["requests"]) == 814


def test_a_window_without_quotations_still_replaces_its_days(bank):
    # A weekend has no request, and a day without quotations no item: the marker makes dlt make the
    # table, and the journal holds the window, whose days staging then reads as empty.
    module, state, made = bank
    state.update(
        loads=1,
        reloads=[("EUR", date(2026, 11, 8), date(2026, 11, 7))],
        answers=dict.fromkeys(module.weekdays(date(2026, 10, 26), date(2026, 11, 8)), SUNDAY),
    )
    assert module.load(datetime(2026, 11, 9, 7, 17, tzinfo=UTC)) == 0
    [(rows, _)] = made["pipelines"][0].runs
    assert rows["rates_daily"] == [MARKER]
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["window_to"]) == (date(2026, 10, 26), date(2026, 11, 8))


def test_a_window_starts_after_the_last_whole_load(bank):
    # The journal counts the loads that dlt finished, with their row in _dlt_loads.
    module, state, made = bank
    state.update(loads=1, reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))])
    module.load(NOW)
    [query] = [q for q in made["pipelines"][0].queries if "_agoranomos_loads" in q]
    assert query == (
        "SELECT j.account_id, max(j.as_of), max(j.window_to) "
        'FROM "raw_bank_of_italy"."_agoranomos_loads" AS j '
        'JOIN "raw_bank_of_italy"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' GROUP BY j.account_id"
    )


def test_a_load_is_due_until_one_has_taken_the_day_in_rome(bank):
    module, state, _ = bank
    assert module.due(NOW)
    state.update(loads=1, reloads=[("EUR", date(2026, 11, 12), date(2026, 11, 11))])
    assert module.due(NOW)
    state.update(reloads=[("EUR", date(2026, 11, 13), date(2026, 11, 12))])
    assert not module.due(NOW)


# What the core says after a change of the reporting currency to GBP, which no account has, before
# the ECB has a rate in the core (rates.read_core).
CHANGED_TO_GBP = {
    "view_currency": "GBP",
    "first_without": date(2025, 5, 14),
    "days_without": 548,
    "first_day": date(2023, 10, 1),
    "holds_preferred": False,
}


def test_a_load_asks_the_core_for_the_history_in_its_window_and_when_it_is_due(bank):
    module, state, made = bank
    state.update(
        loads=1,
        reloads=[("EUR", date(2026, 11, 13), date(2026, 11, 12))],
        core=module.rates.CoreRates(**CHANGED_TO_GBP),
    )
    # A load of the day already taken is still due while the core needs the history.
    assert module.due(NOW)
    module.load(NOW)
    [(rows, _)] = made["pipelines"][-1].runs
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["window_to"], record["kind"]) == (
        date(2023, 10, 1),
        date(2026, 11, 12),
        "history",
    )
    # Not while a load of the history waits for the core.
    state.update(waiting=1)
    assert not module.due(NOW)


def test_the_core_takes_the_rates_of_the_ecb_so_the_bank_takes_no_history(bank):
    # Until C2c of #222 the model takes the rates of the ECB alone once it has one
    # (rates.PREFERRED): a load of the bank then keeps its usual window.
    module, state, made = bank
    state.update(
        loads=1,
        reloads=[("EUR", date(2026, 11, 13), date(2026, 11, 12))],
        core=module.rates.CoreRates(**{**CHANGED_TO_GBP, "holds_preferred": True}),
    )
    assert module.rates.PREFERRED == "ecb"
    assert not module.due(NOW)
    module.load(NOW)
    [(rows, _)] = made["pipelines"][-1].runs
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["kind"]) == (date(2026, 10, 30), "window")


def test_the_source_asks_the_core_in_its_window_and_in_whether_it_is_due():
    text = (REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines" / "bank_of_italy.py").read_text(
        encoding="utf-8"
    )
    assert text.count("rates.history_from(pipeline, CODE)") == 2


def kept_by_the_shape(items):
    """The rows that the macro bank_of_italy_rates keeps of the items of an answer, by its own
    pattern of a number and its own convention, in its order of the duplicates: the rate of each
    day and ISO code, of the first country by name."""
    shape = SHAPE.read_text(encoding="utf-8")
    [pattern] = re.findall(r"where avg_rate ~ '([^']+)'", shape)
    [convention] = re.findall(r"like '([^']+)%'", shape)
    kept = {}
    for item in sorted(items, key=lambda i: (i["referenceDate"], i["isoCode"], i["country"])):
        if not re.fullmatch(pattern, item["avgRate"]):
            continue
        if not item["exchangeConvention"].strip().lower().startswith(convention):
            continue
        kept.setdefault((item["referenceDate"], item["isoCode"]), item["avgRate"])
    return kept


def test_staging_keeps_one_rate_of_each_currency_and_never_casts_n_a():
    kept = kept_by_the_shape(THURSDAY["rates"])
    # 154 ISO codes, less the four without a rate.
    assert len(kept) == 150
    assert kept[("2026-11-12", "XOF")] == "655.957"
    assert kept[("2026-11-12", "XCD")] == "2.9919"
    assert kept[("2026-11-12", "USD")] == "1.1081"
    for code in ("BYN", "IRR", "KPW", "RUB"):
        assert ("2026-11-12", code) not in kept
    assert kept_by_the_shape(SUNDAY["rates"]) == {}
    # A rate in another convention, as the Operating Instructions show for the US dollar, is not
    # read as an amount for one euro.
    other = {**THURSDAY["rates"][0], "exchangeConvention": "Euro amount for 1 unit"}
    assert kept_by_the_shape([other]) == {}


def test_the_shape_gives_the_columns_of_the_rates_of_the_euro():
    shape = SHAPE.read_text(encoding="utf-8")
    assert "{% macro bank_of_italy_rates(relation, platform) %}" in shape
    for column in ("rate_date", "base_currency", "quote_currency", "rate", "rate_source"):
        assert f" as {column}" in shape, column
    assert "'EUR' as base_currency" in shape
    assert "cast(avg_rate as numeric(18,8)) as rate" in shape
    assert "select distinct on (cast(reference_date as date), iso_code)" in shape
    assert "order by cast(reference_date as date), iso_code, country" in shape
    # The cast is in the select over the rows that the pattern of a number has kept, so that no
    # condition of a query over the view goes before the pattern: PostgreSQL pushes none into a
    # subquery of distinct on through a column outside its keys.
    outer, inner = shape.split("from (\n", 1)
    assert "cast(avg_rate as numeric(18,8)) as rate" in outer
    assert "where avg_rate ~ '^[0-9]+(\\.[0-9]+)?$'" in inner
    assert "cast(avg_rate" not in inner


def test_the_staging_of_the_bank_reads_its_table_through_its_shape_even_before_a_load():
    # The DAG starts paused, so a deployment with the demo alone never has the table: the model
    # reads an empty query of the same columns until a load writes it.
    text = (STAGING / "stg_bank_of_italy__rates_daily.sql").read_text(encoding="utf-8")
    assert text.startswith("-- stg_bank_of_italy__rates_daily.sql: ")
    columns = dict(re.findall(r"'(\w+)': '([\w ]+)'", text))
    from_load = REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines" / "bank_of_italy.py"
    fields = re.search(r"^FIELDS = \((.*?)\)$", from_load.read_text(encoding="utf-8"), re.M | re.S)
    names = re.findall(r'"(\w+)"', fields.group(1))
    assert columns == dict.fromkeys((*names, "_dlt_load_id"), "text")
    call = (
        "{{ bank_of_italy_rates(newest_load(relation_or_empty(source('raw_bank_of_italy', "
        "'rates_daily'), {"
    )
    assert text.count(call) == 1
    assert text.rstrip().endswith(
        "}), 'raw_bank_of_italy', ref('stg_loads__newest_days')), 'bank_of_italy') }}"
    )


def test_the_sources_of_the_bank_are_the_tables_that_its_load_writes():
    text = (STAGING / "_bank_of_italy__sources.yml").read_text(encoding="utf-8")
    [source] = re.findall(r"^  - name: (\w+)\n    description:", text, flags=re.MULTILINE)
    assert source == "raw_bank_of_italy"
    assert re.search(r"^    schema: raw_bank_of_italy$", text, flags=re.MULTILINE)
    pattern = r"^      - name: (\w+)\n        description:"
    assert set(re.findall(pattern, text, flags=re.MULTILINE)) == {
        "rates_daily",
        "_agoranomos_loads",
        "_dlt_loads",
    }


def test_the_staging_of_the_bank_has_the_tests_of_the_rates_of_the_ecb():
    # The models of the rates give the same columns, and the same tests guard them.
    bank = (STAGING / "_bank_of_italy__models.yml").read_text(encoding="utf-8")
    ecb = (STAGING.parent / "ecb" / "_ecb__models.yml").read_text(encoding="utf-8")
    assert bank.split("    data_tests:", 1)[1] == ecb.split("    data_tests:", 1)[1]
    assert "severity" not in bank
