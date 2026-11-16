"""bank_of_italy: the daily exchange rates of the Bank of Italy against the euro as a dlt source,
loaded into raw_bank_of_italy as the loader role (ADR-0004, ADR-0042).

The REST API of the portal of the exchange rates of the Bank of Italy serves, without a key, the
rates of about 150 currencies against the euro on each day of quotation, as JSON when the request
asks for it in its Accept header (Operating Instructions, version 1.0, as of November 2026). A load
asks dailyRates for each weekday of its window, one day for each request, at most one request a
second, and keeps each item of the answer as the bank names its fields, which dlt writes in
snake_case into raw_bank_of_italy.rates_daily, all as text, as the bank writes them: the macro
bank_of_italy_rates of dbt/macros/shapes/ reads them. A day without quotations, a weekend or a
closing day of TARGET, comes back with no item, and a weekend is not asked. The answers call the
rates indicative and no financial benchmark: the notice of each answer stays out of the table.

The window, the journal and the history are those of the rates of the ECB (ecb.py): each load
appends its rows under its own _dlt_load_id and records its window in _agoranomos_loads in the same
run, with the account EUR, the base of the rates, and the staging model reads, for each day, the
rates of the newest load whose window covers it, of those that the run of the transform pinned
(ADR-0038, ADR-0046). The first load takes the rates from the first day of the month 37 months
back, about 800 requests; a later one, the days from the day after the last that a whole load took,
or from 14 days back when that is earlier, so that a rate that the bank corrects, or that an
earlier load missed, comes again, and, when the core needs them (rates.history_from), from the
first day without a rate. All end yesterday in Rome, where the bank publishes. A source that the
setting AGORANOMOS_RATES_SOURCE does not name, as with ecb alone, takes no history but its first
(rates.chain). core.fx_rates_daily takes these rates first, unless the setting names nbk or ecb
(dbt/macros/rates.sql)."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from time import monotonic, sleep
from zoneinfo import ZoneInfo

import dlt
from dlt.sources.helpers.requests import Client

from agoranomos_pipelines import rates
from agoranomos_pipelines.loads import (
    APPEND,
    DLT_SETTINGS,
    HISTORY,
    LOAD_HINTS,
    LOADED,
    LOADS,
    TEXT,
    WINDOW,
    destination,
    has_loads,
    or_marker,
    own,
    recorded,
    row_counts,
    run,
)

log = logging.getLogger(__name__)

CODE = "bank_of_italy"
PIPELINE = "agoranomos_bank_of_italy"
DATASET = "raw_bank_of_italy"
TABLE = "rates_daily"
URL = "https://tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0/dailyRates"
# The account of the record of the loads: the base of the rates, as for the ECB.
ACCOUNT = "EUR"
# The dates of the answers refer to Central European Time.
TIME_ZONE = ZoneInfo("Europe/Rome")
# As for the ECB (ecb.py): the history starts one month before that of Google Ads (ADR-0034).
WINDOW_DAYS = 14
HISTORY_MONTHS = 37
# At most one request a second: the history of 37 months is about 800 of them, and the policy of
# the statistics of the ESCB lets the bank restrict a user who acts against the interests of the
# others.
PACE_SECONDS = 1.0
# The fields of an item of the answer of dailyRates, by the names that dlt gives them: each load
# appends its rows. Every column stays text, as the bank writes it, also avgRate, which is N.A. for
# a currency without a rate on the day, and staging casts it.
FIELDS = (
    "country",
    "currency",
    "iso_code",
    "uic_code",
    "avg_rate",
    "exchange_convention",
    "exchange_convention_code",
    "reference_date",
)
HINTS = {"write_disposition": APPEND, "columns": {name: TEXT for name in FIELDS}}
# The retries of a request are explicit, as the contract of the sources asks (ADR-0004): five
# tries over about a minute on a timeout, a broken connection, 429 or a 5xx.
CLIENT = {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}


def yesterday(now: datetime) -> date:
    """The last day whose rates a load at now takes: yesterday in Rome."""
    return now.astimezone(TIME_ZONE).date() - timedelta(days=1)


def history_start(now: datetime) -> date:
    """The first day of the first load at now: the first day of the month HISTORY_MONTHS before
    yesterday."""
    last = yesterday(now)
    months = last.year * 12 + last.month - 1 - HISTORY_MONTHS
    return date(months // 12, months % 12 + 1, 1)


def window(
    now: datetime, last_end: date | None, history: date | None = None
) -> tuple[date, date, str]:
    """The days of a load at now and its kind: the history when no load has taken a day yet, from
    history when the core needs it earlier (rates.history_from), else the days from the day after
    the last loaded, or from the first of the last WINDOW_DAYS when that is earlier, to yesterday,
    and those from history, as a load of the history, when it is earlier still."""
    end = yesterday(now)
    if last_end is None:
        first = history_start(now)
        return (first if history is None else min(first, history)), end, HISTORY
    usual = min(last_end + timedelta(days=1), end - timedelta(days=WINDOW_DAYS - 1))
    if history is not None and history < usual:
        return history, end, HISTORY
    return usual, end, WINDOW


def weekdays(first: date, last: date) -> list[date]:
    """The days from first to last that the bank may quote: Monday to Friday. It quotes none on a
    weekend, so a load does not ask for one."""
    days = (first + timedelta(days=n) for n in range((last - first).days + 1))
    return [day for day in days if day.weekday() < 5]


def fetch(client, day: date) -> list[dict[str, object]]:
    """The items of the answer of dailyRates for one day, against the euro, in English: empty on a
    day without quotations."""
    response = client.get(
        URL,
        params={"referenceDate": day.isoformat(), "currencyIsoCode": ACCOUNT, "lang": "en"},
        headers={"Accept": "application/json"},
    )
    # An answer without the list of the rates fails the load: it is not one of dailyRates.
    return response.json()["rates"]


def rows(first: date, last: date) -> Iterator[dict[str, str | None]]:
    """The items of the weekdays from first to last, each as the bank names its fields, with each
    value as text, asked one day after another, at most one request every PACE_SECONDS. dlt reads
    them as they come, so the history is never held in memory whole."""
    client = Client(**CLIENT)
    asked = None
    for day in weekdays(first, last):
        if asked is not None:
            wait = PACE_SECONDS - (monotonic() - asked)
            if wait > 0:
                sleep(wait)
        asked = monotonic()
        for item in fetch(client, day):
            yield {key: None if value is None else str(value) for key, value in item.items()}


def journal(now: datetime, first: date, last: date, kind: str) -> list[dict[str, object]]:
    """The row of _agoranomos_loads of a load at now, with its kind: a load of the rates fails
    whole or not at all, so the row is always one of a loaded account."""
    return [
        {
            "account_id": ACCOUNT,
            "as_of": now.astimezone(TIME_ZONE).date(),
            "window_from": first,
            "window_to": last,
            "now": now,
            "loaded_at": datetime.now(UTC),
            "status": LOADED,
            "kind": kind,
        }
    ]


@contextmanager
def open_pipeline() -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of the rates, as the loader role, in a temporary working folder: dlt keeps
    the state of each pipeline in its dataset."""
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        yield dlt.pipeline(
            pipeline_name=PIPELINE,
            pipelines_dir=workdir,
            destination=destination(),
            dataset_name=DATASET,
        )


def last_end(pipeline) -> date | None:
    """The last day that a whole load of the rates took, or None before the first one."""
    if not has_loads(pipeline):
        return None
    found = recorded(pipeline).get(ACCOUNT)
    return found[1] if found else None


def due(now: datetime) -> bool:
    """Whether a load at now brings anything: no load has taken the day of now in Rome yet, or the
    core needs the history of the rates again (rates.history_from). Another load on the same day
    asks for the same days."""
    today = now.astimezone(TIME_ZONE).date()
    with open_pipeline() as pipeline:
        found = recorded(pipeline).get(ACCOUNT) if has_loads(pipeline) else None
        if not found or found[0] < today:
            return True
        if rates.history_from(pipeline, CODE) is not None:
            return True
    log.info("The rates have a load of %s already: nothing to load", today.isoformat())
    return False


def load(now: datetime) -> int:
    """Load the rates of the window of now (window()), with the history that the core needs
    (rates.history_from), record it in _agoranomos_loads with its kind, and return the rows of
    rates_daily that dlt reports."""
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline:
        first, last, kind = window(now, last_end(pipeline), rates.history_from(pipeline, CODE))
        log.info(
            "Asking the Bank of Italy for the rates of %d weekdays from %s to %s",
            len(weekdays(first, last)),
            first.isoformat(),
            last.isoformat(),
        )
        resources = [
            dlt.resource(or_marker(rows(first, last)), name=TABLE, **own(HINTS)),
            # In the same run as the rates, so under the same _dlt_load_id: a load that does not
            # end whole has no row in _dlt_loads, and its window is read by nobody.
            dlt.resource(journal(now, first, last, kind), name=LOADS, **own(LOAD_HINTS)),
        ]
        run(pipeline, resources)
        loaded = row_counts(pipeline).get(TABLE, 0)
    log.info(
        "Loaded %d rows into %s.%s, the days from %s to %s, as a load of the kind %s",
        loaded,
        DATASET,
        TABLE,
        first.isoformat(),
        last.isoformat(),
        kind,
    )
    return loaded
