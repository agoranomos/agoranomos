"""Tests of when a load of the rates takes their history again (airflow/dags/agoranomos_pipelines/
rates.py, ADR-0042): from what the core says of its rates, which a test gives in place of the read
as transformer, and from the loads of the history that wait, which a test gives in place of the
read as loader. The loads of the ECB and of the demo that call it are tested in test_ecb_source.py
and test_demo_sources.py, and the whole way in tests/stack/test_rates.py."""

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
CHANGED = rates.CoreRates("GBP", date(2025, 5, 14), 548, date(2025, 4, 1), False)


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


def test_a_day_without_a_rate_takes_the_history_from_the_first_day_of_the_table(waiting, caplog):
    caplog.set_level(logging.INFO, logger=rates.__name__)
    start = rates.history_from(None, "ecb", environ=GBP, read=lambda: CHANGED)
    # The table begins before the week before the first day without a rate.
    assert start == date(2025, 4, 1)
    assert waiting["asked"] == ["ecb"]
    assert (
        "The core lacks rates into GBP on 548 days from 2025-05-14, whose loads it has accepted: "
        "the load takes the history of ecb again from 2025-04-01 (ADR-0042)"
    ) in caplog.messages


def test_the_history_starts_a_week_before_the_first_day_without_a_rate_when_that_is_earlier(
    waiting,
):
    later = rates.CoreRates("GBP", date(2025, 5, 14), 548, date(2025, 5, 20), False)
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: later) == date(2025, 5, 7)
    alone = rates.CoreRates("GBP", date(2025, 5, 14), 548, None, False)
    assert rates.history_from(None, "demo_fx", environ=GBP, read=lambda: alone) == date(2025, 5, 7)
    assert rates.LEAD_DAYS == 7


@pytest.mark.parametrize(
    "core",
    [
        # Before the first run of the transform of this release: no view, or one without the days.
        None,
        # Every day of the facts has its rate: a currency of an account, or the one before beside
        # a row of the euro, takes them from the rows of the core, with no load.
        rates.CoreRates("GBP", None, 0, date(2025, 4, 1), False),
    ],
    ids=["no-view", "no-gap"],
)
def test_no_day_without_a_rate_takes_no_history(waiting, core):
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: core) is None
    assert waiting["asked"] == []


def test_a_view_of_another_reporting_currency_takes_no_history(waiting, caplog):
    # The view was built before the change: the next run of the transform, or marts-init at up,
    # builds it in the reporting currency of the task.
    caplog.set_level(logging.INFO, logger=rates.__name__)
    stale = rates.CoreRates("USD", date(2025, 5, 14), 548, date(2025, 4, 1), False)
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: stale) is None
    assert waiting["asked"] == []
    stale_line = "ops.ops_rates is of USD, not of the reporting currency GBP"
    assert any(stale_line in message for message in caplog.messages)


def test_the_demo_takes_no_history_once_the_core_holds_a_rate_of_the_ecb(waiting):
    # The model takes the rates of the ECB alone then (rates_source).
    with_ecb = rates.CoreRates("GBP", date(2025, 5, 14), 548, date(2025, 4, 1), True)
    assert rates.history_from(None, "demo_fx", environ=GBP, read=lambda: with_ecb) is None
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: with_ecb) == date(2025, 4, 1)
    assert rates.PREFERRED == "ecb"


def test_a_history_that_waits_for_the_core_takes_no_other(waiting, caplog):
    caplog.set_level(logging.INFO, logger=rates.__name__)
    waiting["pending"]["ecb"] = 1
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: CHANGED) is None
    assert any("1 loads of the history of ecb wait" in m for m in caplog.messages)


def test_the_reporting_currency_is_usd_by_default(waiting):
    usd = rates.CoreRates("USD", date(2025, 5, 14), 3, date(2025, 4, 1), False)
    assert rates.history_from(None, "ecb", environ={}, read=lambda: usd) == date(2025, 4, 1)
    assert rates.history_from(None, "ecb", environ={}, read=lambda: CHANGED) is None


def test_a_core_that_cannot_be_read_costs_the_history_and_not_the_load(waiting, caplog):
    def refuses():
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
    assert rates.history_from(None, "ecb", environ=GBP, read=lambda: CHANGED) is None
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


def test_the_core_is_read_from_the_view_of_ops_and_the_table_of_the_rates():
    # The columns that the view declares, and the table of the migration.
    view = (REPO_ROOT / "dbt" / "models" / "ops" / "ops_rates.sql").read_text(encoding="utf-8")
    for column in ("reporting_currency", "first_day_without_rate", "days_without_rate"):
        assert re.search(rf" as {column}\b", view), column
        assert column in rates.VIEW
    assert rates.VIEW.endswith("FROM ops.ops_rates")
    assert "FROM core.fx_rates_daily" in rates.TABLE
    migration = REPO_ROOT / "postgres" / "migrations" / "0007_create_the_rates.sql"
    assert "rate_source text NOT NULL" in migration.read_text(encoding="utf-8")


def test_a_source_of_rates_of_the_base_package_asks_before_it_loads():
    # Each source of the rates asks in its window and in whether it is due (ecb.py and
    # demo/sources.py); a source of v0.6 is to do the same.
    pipelines = REPO_ROOT / "airflow" / "dags" / "agoranomos_pipelines"
    ecb = (pipelines / "ecb.py").read_text(encoding="utf-8")
    assert ecb.count("rates.history_from(pipeline, CODE)") == 2
    demo = (pipelines / "demo" / "sources.py").read_text(encoding="utf-8")
    assert demo.count("rates.history_from(pipeline, code)") == 1
    assert "rates_history(RATES)" in demo
