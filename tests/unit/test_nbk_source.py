"""Tests of the load of the official rates of the National Bank of Kazakhstan
(airflow/dags/agoranomos_pipelines/nbk.py).

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing and asks the NBK nothing: its client answers each day
with a recorded answer of the feed, and records what it was asked, and its pipeline records the
resources of each run and answers from what the test says the dataset holds. The clock of the
pacing is a stand-in too, so no test waits.

The answers in tests/unit/fixtures/nbk/ are those of https://nationalbank.kz/rss/get_rates.cfm,
as the NBK sent them on 16 November 2026 between 02:43 and 02:50 UTC, each file named by the day
asked for: a Thursday (12 November 2026, 48 currencies), a Friday, the Saturday and Sunday after
it and the Monday (6 to 9 November 2026), a day whose set was not published yet (17 November
2026), a holiday (1 January 2026, 39 currencies), a Sunday of 2023 (1 October 2023, 39 currencies)
and the JSON of a date in the form YYYY-MM-DD (invalid-date.json, with a line break added at its
end)."""

import importlib
import itertools
import re
import sys
import types
from datetime import UTC, date, datetime
from pathlib import Path
from xml.etree import ElementTree

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "nbk"
# A Thursday, at 16:13 in Astana (UTC+5), when the DAG runs.
NOW = datetime(2026, 11, 12, 11, 13, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
FIELDS = {"date", "fullname", "title", "description", "quant", "index", "change"}


def recorded(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def answer(fdate: str) -> bytes:
    """The recorded answer of the feed for a day asked as DD.MM.YYYY, or, for a day without one,
    the answer of the day without a set with its date."""
    day, month, year = fdate.split(".")
    path = FIXTURES / f"{year}-{month}-{day}.xml"
    if path.exists():
        return path.read_bytes()
    return recorded("2026-11-17.xml").replace(b"17.11.2026", fdate.encode())


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
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into the dict of its
    hint, in place."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


class Clock:
    """The monotonic clock and the sleep of the pacing: each request takes `request` seconds, and
    a sleep moves the clock on."""

    def __init__(self, request=0.25):
        self.now, self.request, self.slept, self.starts = 1000.0, request, [], []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def nbk(monkeypatch):
    """The module with a stand-in for dlt, and what it made and was asked: the pipelines, the
    clients with their settings, and the requests. state["loads"] is how many complete loads the
    dataset holds (None: no table _dlt_loads yet), state["reloads"] the rows that the journal of
    the whole loads gives, and state["answer"] what the NBK answers for a day as DD.MM.YYYY.
    state["core"] is what the core says of its rates, as rates.read_core reads it as transformer
    (None: no view yet), and state["waiting"] how many loads of the history wait for the core."""
    state = {"loads": None, "reloads": [], "answer": answer, "core": None, "waiting": 0}
    clock = Clock()
    made = {"pipelines": [], "clients": [], "requests": [], "clock": clock}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def get(self, url, params):
            made["requests"].append((url, params))
            clock.starts.append(clock.now)
            clock.now += clock.request
            return types.SimpleNamespace(content=state["answer"](params["fdate"]))

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
    for name in ("LOAD__TRUNCATE_STAGING_DATASET", "LOAD__WORKERS"):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    names = ("agoranomos_pipelines.nbk", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.nbk")
    monkeypatch.setattr(module.rates, "read_core", lambda code: state["core"])
    monkeypatch.setattr(
        module, "time", types.SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep)
    )
    monkeypatch.setenv("AGORANOMOS_REPORTING_CURRENCY", "KZT")
    yield module, state, made
    for name in names:
        sys.modules.pop(name, None)


def items_of(module, day: date) -> list[dict[str, str | None]]:
    return module.items(day, answer(day.strftime("%d.%m.%Y")))


def test_astana_time_is_utc_plus_five():
    # One time zone for all of Kazakhstan since 1 March 2024, UTC+5, by the decree of the
    # Government of 19 January 2024 (primeminister.kz/ru/decisions/19012024-20, read on 16
    # November 2026), which the time zone database has for Asia/Almaty since 2024a.
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    zone = ZoneInfo("Asia/Almaty")
    assert datetime(2026, 11, 12, 12, tzinfo=zone).utcoffset() == timedelta(hours=5)
    assert datetime(2026, 7, 1, 12, tzinfo=zone).utcoffset() == timedelta(hours=5)
    assert datetime(2023, 10, 1, 12, tzinfo=zone).utcoffset() == timedelta(hours=6)


def test_a_load_ends_today_in_astana(nbk):
    module, _, _ = nbk
    assert str(module.TIME_ZONE) == "Asia/Almaty"
    assert module.today(NOW) == date(2026, 11, 12)
    # 19:00 UTC is already midnight of the next day in Astana.
    assert module.today(datetime(2026, 11, 12, 18, 59, tzinfo=UTC)) == date(2026, 11, 12)
    assert module.today(datetime(2026, 11, 12, 19, 0, tzinfo=UTC)) == date(2026, 11, 13)


def test_the_first_load_takes_37_months_as_the_load_of_the_ecb(nbk):
    module, _, _ = nbk
    assert module.HISTORY_MONTHS == 37
    assert module.window(NOW, None) == (date(2023, 10, 1), date(2026, 11, 12), "history")
    assert module.history_start(datetime(2026, 1, 15, 12, tzinfo=UTC)) == date(2022, 12, 1)
    assert module.history_start(datetime(2026, 12, 31, 12, tzinfo=UTC)) == date(2023, 11, 1)
    # Every calendar day: about 1,130 requests.
    assert len(list(module.days(date(2023, 10, 1), date(2026, 11, 12)))) == 1139


def test_a_later_load_takes_the_new_days_and_at_least_the_last_fourteen(nbk):
    module, _, _ = nbk
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


def test_a_load_takes_the_history_again_from_the_day_that_the_core_needs(nbk):
    module, _, _ = nbk
    last = date(2026, 11, 12)
    assert module.window(NOW, last, date(2023, 9, 1)) == (date(2023, 9, 1), last, "history")
    assert module.window(NOW, last, date(2026, 11, 5)) == (date(2026, 10, 30), last, "window")
    assert module.window(NOW, None, date(2022, 1, 1)) == (date(2022, 1, 1), last, "history")
    assert module.window(NOW, None, date(2024, 1, 1)) == (date(2023, 10, 1), last, "history")


def test_a_weekday_gives_every_item_as_the_feed_names_it(nbk):
    module, _, _ = nbk
    found = items_of(module, date(2026, 11, 12))
    assert len(found) == 48
    assert all(set(row) == FIELDS for row in found)
    usd = next(row for row in found if row["title"] == "USD")
    assert usd == {
        "date": "12.11.2026",
        "fullname": "ДОЛЛАР США",
        "title": "USD",
        "description": "444.76",
        "quant": "1",
        "index": usd["index"],
        "change": usd["change"],
    }
    # Every value is text, as the NBK writes it, with one or two decimal places.
    assert all(isinstance(value, str) for row in found for value in row.values())
    assert all(re.fullmatch(r"\d+(\.\d{1,2})?", row["description"]) for row in found)
    codes = {row["title"] for row in found}
    assert len(codes) == 48
    # The currencies for which ADR-0042 takes the NBK.
    assert "KZT" not in codes
    assert {"RUB", "BYN", "UZS", "AED", "SAR", "VND", "USD", "EUR"} <= codes


def test_quant_goes_from_1_to_10000_and_changes_over_time(nbk):
    module, _, _ = nbk
    quants = {
        day: {row["title"]: row["quant"] for row in items_of(module, day) if row["quant"] != "1"}
        for day in (date(2026, 11, 12), date(2026, 1, 1), date(2023, 10, 1))
    }
    assert quants[date(2026, 11, 12)] == {
        "AMD": "10",
        "HUF": "10",
        "MNT": "100",
        "UZS": "100",
        "KRW": "100",
        "VND": "1000",
        "IDR": "1000",
        "IRR": "10000",
    }
    # IRR went from 1000 to 10000 units during 2026.
    assert (
        quants[date(2026, 1, 1)]
        == quants[date(2023, 10, 1)]
        == {
            "AMD": "10",
            "HUF": "10",
            "UZS": "100",
            "KRW": "100",
            "IRR": "1000",
        }
    )


def test_the_list_of_the_currencies_changes_over_time(nbk):
    module, _, _ = nbk
    november = {row["title"] for row in items_of(module, date(2026, 11, 12))}
    january = {row["title"] for row in items_of(module, date(2026, 1, 1))}
    october_2023 = {row["title"] for row in items_of(module, date(2023, 10, 1))}
    assert len(january) == len(october_2023) == 39
    assert january == october_2023
    assert november - january == {"EGP", "IDR", "MNT", "OMR", "PKR", "QAR", "VND", "ILS", "RON"}
    assert january <= november


def test_every_calendar_day_has_a_set_and_a_weekend_carries_the_rate_of_the_monday(nbk):
    # The rate fixed on Friday 6 November 2026 is set for the next business day, Monday 9, and for
    # the Saturday and the Sunday before it; that of Friday 6 itself was fixed on Thursday 5.
    module, _, _ = nbk
    usd = {
        day: next(r["description"] for r in items_of(module, day) if r["title"] == "USD")
        for day in (date(2026, 11, 6), date(2026, 11, 7), date(2026, 11, 8), date(2026, 11, 9))
    }
    assert usd == {
        date(2026, 11, 6): "451.87",
        date(2026, 11, 7): "447.14",
        date(2026, 11, 8): "447.14",
        date(2026, 11, 9): "447.14",
    }
    # A holiday has its set too.
    assert len(items_of(module, date(2026, 1, 1))) == 39


def test_a_day_whose_set_is_not_published_yet_gives_no_row(nbk):
    module, _, _ = nbk
    content = recorded("2026-11-17.xml")
    assert "на выбранную дату информации нет".encode() in content
    assert module.items(date(2026, 11, 17), content) == []


def test_an_answer_that_is_not_the_feed_of_the_day_stops_the_load(nbk):
    module, _, _ = nbk
    # A date in another form gets JSON, with the HTTP status 200.
    content = recorded("invalid-date.json")
    assert b'"code":500' in content
    with pytest.raises(ValueError, match="other than XML"):
        module.items(date(2026, 11, 12), content)
    # The feed of another day.
    with pytest.raises(ValueError, match=r"'13\.11\.2026'"):
        module.items(date(2026, 11, 12), answer("13.11.2026"))
    # Another document.
    with pytest.raises(ValueError, match="<html>"):
        module.items(date(2026, 11, 12), b"<html><date>12.11.2026</date></html>")
    # A document type, which the feed never declares.
    with pytest.raises(ValueError, match="document type"):
        module.items(
            date(2026, 11, 12),
            b'<?xml version="1.0"?><!DOCTYPE rates [<!ENTITY a "b">]><rates>&a;</rates>',
        )


def test_the_feed_takes_the_day_as_dd_mm_yyyy(nbk):
    # The feed also takes DD/MM/YYYY, with the same answer, and the load asks with dots, the form
    # that it writes in <date>.
    module, _, _ = nbk
    assert module.DAY_FORMAT == "%d.%m.%Y"
    assert date(2026, 11, 12).strftime(module.DAY_FORMAT) == "12.11.2026"
    root = ElementTree.fromstring(recorded("2026-11-12.xml"))  # noqa: S314 (a recorded fixture)
    assert root.findtext("date") == "12.11.2026"
    assert "fdate=DD.MM.YYYY" in module.__doc__
    assert "DD/MM/YYYY" in module.__doc__


def test_a_load_asks_for_each_day_of_its_window_a_second_apart(nbk):
    module, state, made = nbk
    state.update(loads=1, reloads=[("KZT", date(2026, 11, 11), date(2026, 11, 11))])
    module.load(NOW)
    assert made["clients"] == [
        {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}
    ]
    urls = {url for url, _ in made["requests"]}
    assert urls == {"https://nationalbank.kz/rss/get_rates.cfm"}
    asked = [params for _, params in made["requests"]]
    assert asked[0] == {"fdate": "30.10.2026"}
    assert asked[-1] == {"fdate": "12.11.2026"}
    assert len(asked) == 14
    # Each request starts at least a second after the one before, and none waits longer.
    starts = made["clock"].starts
    gaps = [later - earlier for earlier, later in itertools.pairwise(starts)]
    assert gaps == [pytest.approx(1.0)] * 13
    assert made["clock"].slept == [pytest.approx(0.75)] * 13


def test_a_slow_answer_needs_no_wait(nbk):
    module, state, made = nbk
    made["clock"].request = 1.5
    state.update(loads=1, reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    module.load(NOW)
    assert len(made["requests"]) == 14
    assert made["clock"].slept == []


def test_a_load_appends_the_items_as_text_with_the_day_of_the_feed(nbk):
    module, state, made = nbk
    state.update(loads=1, reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    # The fourteen days to 12 November, of which 6 to 9 and 12 November have a recorded answer.
    assert module.load(NOW) == 48 * 5
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_nbk"
    assert pipeline.options["pipeline_name"] == "agoranomos_nbk"
    assert pipeline.formats == ["csv"]
    assert pipeline.contracts == [{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}]
    assert pipeline.options["destination"][2] is False
    [(rows, hints)] = pipeline.runs
    assert {row["date"] for row in rows["rates_daily"]} == {
        "06.11.2026",
        "07.11.2026",
        "08.11.2026",
        "09.11.2026",
        "12.11.2026",
    }
    assert hints["rates_daily"]["write_disposition"] == "append"
    columns = hints["rates_daily"]["columns"]
    assert {name: hint["data_type"] for name, hint in columns.items()} == dict.fromkeys(
        FIELDS, "text"
    )
    assert all(hint["name"] == name for name, hint in columns.items())


def test_a_load_records_its_window_in_the_same_run(nbk):
    module, _, made = nbk
    module.load(NOW)
    [(rows, hints)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert {key: record[key] for key in ("account_id", "as_of", "window_from", "window_to")} == {
        "account_id": "KZT",
        "as_of": date(2026, 11, 12),
        "window_from": date(2023, 10, 1),
        "window_to": date(2026, 11, 12),
    }
    assert record["now"] == NOW
    assert record["status"] == "loaded"
    assert record["kind"] == "history"
    assert hints["_agoranomos_loads"]["write_disposition"] == "append"
    assert set(hints["_agoranomos_loads"]["columns"]) == {*record, "error"}
    # One request for each of the 1,139 days, of which 2023-10-01, 2026-01-01, 2026-11-06 to
    # 2026-11-09 and 2026-11-12 have a recorded answer.
    assert len(made["requests"]) == 1139
    assert len(rows["rates_daily"]) == 39 * 2 + 48 * 5


def test_a_window_without_a_set_still_replaces_its_days(nbk):
    # The marker makes dlt make the table, and the journal holds the window, whose days staging
    # then reads as empty.
    module, state, made = nbk
    state.update(
        answer=lambda fdate: answer("17.11.2026").replace(b"17.11.2026", fdate.encode()),
        loads=1,
        reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))],
    )
    assert module.load(NOW) == 0
    [(rows, _)] = made["pipelines"][0].runs
    assert rows["rates_daily"] == [MARKER]
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["window_to"]) == (date(2026, 10, 30), date(2026, 11, 12))


def test_a_window_starts_after_the_last_whole_load(nbk):
    module, state, made = nbk
    state.update(loads=1, reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    module.load(NOW)
    [query] = [q for q in made["pipelines"][0].queries if "_agoranomos_loads" in q]
    assert query == (
        "SELECT j.account_id, max(j.as_of), max(j.window_to) "
        'FROM "raw_nbk"."_agoranomos_loads" AS j JOIN "raw_nbk"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' AND j.kind IS DISTINCT FROM 'intraday' GROUP BY j.account_id"
    )


def test_a_load_is_due_until_one_has_taken_the_day_in_astana(nbk):
    module, state, _ = nbk
    assert module.due(NOW)
    state.update(loads=1, reloads=[("KZT", date(2026, 11, 11), date(2026, 11, 11))])
    assert module.due(NOW)
    state.update(reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    assert not module.due(NOW)


# What the core says of the rates of the NBK after a change of the reporting currency to KZT,
# which no account has, and so to the NBK as the first source of auto (rates.read_core).
CHANGED_TO_KZT = {
    "view_currency": "KZT",
    "first_without": date(2025, 5, 14),
    "days_without": 548,
    "first_day": date(2023, 10, 1),
}


def test_a_load_takes_the_history_again_when_the_core_lacks_the_rates(nbk):
    module, state, made = nbk
    state.update(
        loads=1,
        reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 11))],
        core=module.rates.CoreRates(**CHANGED_TO_KZT),
    )
    module.load(NOW)
    [(rows, _)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["window_to"], record["kind"]) == (
        date(2023, 10, 1),
        date(2026, 11, 12),
        "history",
    )
    assert made["requests"][0][1] == {"fdate": "01.10.2023"}
    state.update(reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    assert module.due(NOW)
    state.update(waiting=1)
    assert not module.due(NOW)


def test_a_load_takes_no_history_when_the_setting_does_not_name_the_nbk(nbk, monkeypatch):
    # With AGORANOMOS_RATES_SOURCE=ecb the model takes the rates of the ECB alone (rates.chain):
    # the NBK, whose DAG may be unpaused, takes its usual window every day.
    module, state, made = nbk
    monkeypatch.setenv("AGORANOMOS_RATES_SOURCE", "ecb")
    state.update(
        loads=1,
        reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 11))],
        core=module.rates.CoreRates(**CHANGED_TO_KZT),
    )
    module.load(NOW)
    [(rows, _)] = made["pipelines"][0].runs
    [record] = rows["_agoranomos_loads"]
    assert (record["window_from"], record["kind"]) == (date(2026, 10, 30), "window")
    state.update(reloads=[("KZT", date(2026, 11, 12), date(2026, 11, 12))])
    assert not module.due(NOW)
