"""Tests of the order of the sources of the rates and of when a load of the rates takes their
history again (airflow/dags/agoranomos_pipelines/rates.py, ADR-0042): from the setting
AGORANOMOS_RATES_SOURCE, from what the core says of the rates of a source, which a test gives in
place of the read as transformer, and from the loads of the history that wait, which a test gives
in place of the read as loader. The loads of the sources that call it are tested in
test_ecb_source.py, test_bank_of_italy_source.py, test_nbk_source.py and test_demo_sources.py, and
the whole way in tests/stack/test_rates.py."""

import logging
import re
from datetime import date
from pathlib import Path

import pytest
from agoranomos_pipelines import rates

REPO_ROOT = Path(__file__).resolve().parents[2]
GBP = {rates.CURRENCY: "GBP"}
# What the core says after a change of the reporting currency to GBP, which no account has: the
# facts begin on 14 May 2025, and the rates of the table on 1 April 2025.
CHANGED = rates.CoreRates("GBP", date(2025, 5, 14), 548, date(2025, 4, 1))


def reads(core):
    """A read of the core that gives core for every source, and records the sources asked."""
    asked = []

    def read(code):
        asked.append(code)
        return core

    read.asked = asked
    return read


@pytest.fixture
def waiting(monkeypatch):
    """The loads of the history of a source that wait for the core, by source, and the sources
    that were asked."""
    found = {"pending": {}, "asked": []}

    def counted(pipeline, code):
        found["asked"].append(code)
        return found["pending"].get(code, 0)

    monkeypatch.setattr(rates, "waiting", counted)
    return found


@pytest.mark.parametrize(
    ("environ", "order"),
    [
        # auto, the default: the Bank of Italy first, and the NBK first for the tenge.
        ({}, ("bank_of_italy", "nbk", "ecb")),
        ({rates.SETTING: "auto", rates.CURRENCY: "EUR"}, ("bank_of_italy", "nbk", "ecb")),
        ({rates.SETTING: "", rates.CURRENCY: "KZT"}, ("nbk", "bank_of_italy", "ecb")),
        ({rates.SETTING: "bank_of_italy", rates.CURRENCY: "KZT"}, ("bank_of_italy", "nbk", "ecb")),
        ({rates.SETTING: "nbk"}, ("nbk", "bank_of_italy", "ecb")),
        # The reference rates of the ECB alone.
        ({rates.SETTING: "ecb", rates.CURRENCY: "KZT"}, ("ecb",)),
    ],
)
def test_the_setting_gives_the_order_of_the_sources(environ, order):
    assert rates.chain(environ) == order
    assert rates.preferred(environ) == order[0]


def test_the_order_is_that_of_the_model():
    # dbt/macros/rates.sql gives the same order to the model of the rates.
    macro = (REPO_ROOT / "dbt" / "macros" / "rates.sql").read_text(encoding="utf-8")
    for setting, order in rates.CHAINS.items():
        assert f"'{setting}': {list(order)!r}," in macro, setting
    assert (
        "{%- set setting = 'nbk' if env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') == 'KZT' "
        "else 'bank_of_italy' -%}"
    ) in macro


def test_another_value_of_the_setting_takes_no_history(waiting, caplog):
    with pytest.raises(ValueError, match="must be auto, bank_of_italy, nbk or ecb, not 'boi'"):
        rates.chain({rates.SETTING: "boi"})
    read = reads(CHANGED)
    environ = {**GBP, rates.SETTING: "boi"}
    assert rates.history_from(None, "bank_of_italy", environ=environ, read=read) is None
    assert read.asked == []
    assert waiting["asked"] == []


def test_a_day_without_a_rate_takes_the_history_from_the_first_day_of_the_table(waiting, caplog):
    caplog.set_level(logging.INFO, logger=rates.__name__)
    read = reads(CHANGED)
    start = rates.history_from(None, "bank_of_italy", environ=GBP, read=read)
    # The table begins before the week before the first day without a rate.
    assert start == date(2025, 4, 1)
    assert read.asked == ["bank_of_italy"]
    assert waiting["asked"] == ["bank_of_italy"]
    assert (
        "The core lacks rates into GBP on 548 days from 2025-05-14, whose loads it has accepted: "
        "the load takes the history of bank_of_italy again from 2025-04-01 (ADR-0042)"
    ) in caplog.messages


def test_the_history_starts_a_week_before_the_first_day_without_a_rate_when_that_is_earlier(
    waiting,
):
    later = rates.CoreRates("GBP", date(2025, 5, 14), 548, date(2025, 5, 20))
    assert rates.history_from(None, "ecb", environ=GBP, read=reads(later)) == date(2025, 5, 7)
    alone = rates.CoreRates("GBP", date(2025, 5, 14), 548, None)
    assert rates.history_from(None, "demo_fx", environ=GBP, read=reads(alone)) == date(2025, 5, 7)
    assert rates.LEAD_DAYS == 7


@pytest.mark.parametrize(
    "core",
    [
        # Before the first run of the transform of this release: no view, or one without the days
        # of a source.
        None,
        # Every day of the facts of the currencies of the source has its rate: a currency of an
        # account, or the one before beside a row of the euro, takes them from the rows of the
        # core, with no load; or the view has no row of the source.
        rates.CoreRates("GBP", None, 0, date(2025, 4, 1)),
    ],
    ids=["no-view", "no-gap"],
)
def test_no_day_without_a_rate_takes_no_history(waiting, core):
    assert rates.history_from(None, "ecb", environ=GBP, read=reads(core)) is None
    assert waiting["asked"] == []


def test_a_view_of_another_reporting_currency_takes_no_history(waiting, caplog):
    # The view was built before the change: the next run of the transform, or marts-init at up,
    # builds it in the reporting currency of the task.
    caplog.set_level(logging.INFO, logger=rates.__name__)
    stale = rates.CoreRates("USD", date(2025, 5, 14), 548, date(2025, 4, 1))
    assert rates.history_from(None, "ecb", environ=GBP, read=reads(stale)) is None
    assert waiting["asked"] == []
    stale_line = "ops.ops_rates is of USD, not of the reporting currency GBP"
    assert any(stale_line in message for message in caplog.messages)


@pytest.mark.parametrize(
    ("setting", "code"),
    [
        # ecb alone: the banks, unpaused or not, never take their history again.
        ("ecb", "bank_of_italy"),
        ("ecb", "nbk"),
    ],
)
def test_a_source_that_the_setting_does_not_name_takes_no_history(waiting, caplog, setting, code):
    caplog.set_level(logging.INFO, logger=rates.__name__)
    read = reads(CHANGED)
    environ = {**GBP, rates.SETTING: setting}
    assert rates.history_from(None, code, environ=environ, read=read) is None
    assert read.asked == []
    assert waiting["asked"] == []
    assert (
        f"AGORANOMOS_RATES_SOURCE={setting} takes the rates from ecb, not from {code}: the load "
        "takes its usual window and no history"
    ) in caplog.messages


def test_each_source_of_the_order_reads_its_own_days(waiting):
    # The view names the days of each source apart, the demo's too (ops_rates.sql): the source
    # asks for its own, and the demo has a row only while the core holds no rate of a real source.
    for code in ("bank_of_italy", "nbk", "ecb", "demo_fx"):
        read = reads(CHANGED)
        assert rates.history_from(None, code, environ=GBP, read=read) == date(2025, 4, 1)
        assert read.asked == [code]


def test_a_history_that_waits_for_the_core_takes_no_other(waiting, caplog):
    caplog.set_level(logging.INFO, logger=rates.__name__)
    waiting["pending"]["ecb"] = 1
    assert rates.history_from(None, "ecb", environ=GBP, read=reads(CHANGED)) is None
    assert any("1 loads of the history of ecb wait" in m for m in caplog.messages)


def test_the_reporting_currency_is_usd_by_default(waiting):
    usd = rates.CoreRates("USD", date(2025, 5, 14), 3, date(2025, 4, 1))
    assert rates.history_from(None, "ecb", environ={}, read=reads(usd)) == date(2025, 4, 1)
    assert rates.history_from(None, "ecb", environ={}, read=reads(CHANGED)) is None


def test_a_core_that_cannot_be_read_costs_the_history_and_not_the_load(waiting, caplog):
    def refuses(code):
        raise RuntimeError('connection to server at "warehouse-db" failed: password secret')

    assert rates.history_from(None, "ecb", environ=GBP, read=refuses) is None
    # The class of the error and no text of it, which could hold a secret.
    assert caplog.messages == [
        "Could not read the rates of the core as transformer (RuntimeError): the load takes its "
        "usual window, and the next one asks again"
    ]

    def stops(pipeline, code):
        raise RuntimeError("canceling statement due to lock timeout")

    rates.waiting = stops
    assert rates.history_from(None, "ecb", environ=GBP, read=reads(CHANGED)) is None
    assert (
        "Could not read the loads of the history of ecb as loader (RuntimeError)"
        in (caplog.messages[-1])
    )


def test_a_history_waits_while_pinned_or_whole_and_not_pinned():
    sql = rates.waiting_sql("ecb", '"raw_ecb"."_agoranomos_loads"', '"raw_ecb"."_dlt_loads"')
    assert sql == (
        "SELECT (SELECT count(*) FROM core.loads WHERE source = 'ecb' AND kind = 'history' "
        "AND status = 'pinned') + (SELECT count(DISTINCT j._dlt_load_id) "
        'FROM "raw_ecb"."_agoranomos_loads" AS j JOIN "raw_ecb"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' AND to_jsonb(j) ->> 'kind' = 'history' "
        "AND NOT EXISTS (SELECT FROM core.loads AS p "
        "WHERE p.source = 'ecb' AND p.load_id = j._dlt_load_id))"
    )
    # A dataset without a whole load has nothing waiting in raw.
    assert rates.waiting_sql("demo_fx", None, None) == (
        "SELECT count(*) FROM core.loads WHERE source = 'demo_fx' AND kind = 'history' "
        "AND status = 'pinned'"
    )


class Cursor:
    """A cursor of psycopg2 that gives the rows of the view and the first day of the table."""

    def __init__(self, rows, first_day):
        self.rows, self.first_day, self.executed = rows, first_day, []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, parameters=None):
        self.executed.append(sql)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return (self.first_day,)


def test_the_core_is_read_from_the_row_of_the_source(monkeypatch):
    # One row for each source (ops_rates.sql): the source takes the days of its own row, none
    # when the view has none of it, and the reporting currency of the view either way. psycopg2 is
    # in the Airflow image and not here: its errors are stand-ins.
    import sys
    import types

    from agoranomos_pipelines import maintenance

    errors = types.SimpleNamespace(UndefinedTable=LookupError, UndefinedColumn=LookupError)
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(errors=errors))

    rows = [
        ("USD", "bank_of_italy", date(2025, 5, 14), 10),
        ("USD", "nbk", date(2025, 6, 1), 3),
        ("USD", "ecb", None, 0),
    ]

    class Connection:
        def set_session(self, readonly):
            assert readonly

        def cursor(self):
            return self.found

    connection = Connection()

    class Connected:
        def __init__(self, credentials, application_name):
            assert application_name == "agoranomos_rates"

        def __enter__(self):
            return connection

        def __exit__(self, *_):
            return False

    monkeypatch.setattr(maintenance, "connected", Connected)
    monkeypatch.setattr(maintenance, "transformer_credentials", dict)
    connection.found = Cursor(rows, date(2025, 4, 1))
    assert rates.read_core("nbk") == rates.CoreRates("USD", date(2025, 6, 1), 3, date(2025, 4, 1))
    assert connection.found.executed == [rates.VIEW, rates.TABLE]
    assert rates.read_core("demo_fx") == rates.CoreRates("USD", None, 0, date(2025, 4, 1))
    connection.found = Cursor([], None)
    assert rates.read_core("nbk") is None


def test_the_core_is_read_from_the_view_of_ops_and_the_table_of_the_rates():
    # The columns that the view declares, and the table of the migration.
    view = (REPO_ROOT / "dbt" / "models" / "ops" / "ops_rates.sql").read_text(encoding="utf-8")
    for column in (
        "reporting_currency",
        "rate_source",
        "first_day_without_rate_of_source",
        "days_without_rate_of_source",
    ):
        assert re.search(rf" as {column}\b", view), column
        assert column in rates.VIEW
    assert rates.VIEW.endswith("FROM ops.ops_rates")
    assert rates.TABLE == "SELECT min(rate_date) FROM core.fx_rates_daily"
    migration = REPO_ROOT / "postgres" / "migrations" / "0007_create_the_rates.sql"
    assert "rate_source text NOT NULL" in migration.read_text(encoding="utf-8")


def test_a_source_of_rates_of_the_base_package_asks_before_it_loads():
    # Each source of the rates asks in its window and in whether it is due (ecb.py,
    # bank_of_italy.py, nbk.py and demo/sources.py).
    pipelines = REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines"
    for name in ("ecb.py", "bank_of_italy.py", "nbk.py"):
        text = (pipelines / name).read_text(encoding="utf-8")
        assert text.count("rates.history_from(pipeline, CODE)") == 2, name
    demo = (pipelines / "demo" / "sources.py").read_text(encoding="utf-8")
    assert demo.count("rates.history_from(pipeline, code)") == 1
    assert "rates_history(RATES)" in demo
