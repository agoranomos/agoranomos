"""ecb: the daily reference rates of the euro of the European Central Bank as a dlt source, loaded
into raw_ecb as the loader role (ADR-0004, ADR-0032).

The data API of the ECB serves the series EXR/D..EUR.SP00.A, the rate of each currency for one euro
on each day of publication, as CSV and without a key; the ECB publishes at about 16:00 CET on each
TARGET working day. A load asks for the data of its window only (detail=dataonly) and keeps each
row as the CSV names its columns, which dlt writes in snake_case into raw_ecb.rates_daily, all as
text, as the ECB writes them: the macro ecb_exr of dbt/macros/shapes/ reads them. Each load
appends its rows under its own _dlt_load_id and records its window in _agoranomos_loads in the same
run, with the account EUR, the base of the rates, as the loads of the demo do, and the staging
model reads, for each day, the rates of the newest load whose window covers it, of those that the
run of the transform pinned (ADR-0038, ADR-0046): a window without a rate, such as a weekend,
comes back empty and still replaces its days. A
dataset that a release before v0.5 loaded has no _agoranomos_loads, so its next load takes the
history again, and the rows of the earlier loads stay unread. The first load takes the rates from
the first day of the month 37 months back, one month before the history of Google Ads (ADR-0034),
so that its first day has a rate; a later one, the days from the day after the last that a whole
load took, or from 14 days back when that is earlier, so that a rate that the ECB corrects, or that
an earlier load missed, comes again and replaces a carried one in core.fx_rates_daily (ADR-0042).
When the core lacks rates into the reporting currency on days whose loads it has accepted, as
after a change of AGORANOMOS_REPORTING_CURRENCY, a load takes the history again from the first of
those days, or earlier (agoranomos_pipelines/rates.py). All end yesterday in Frankfurt, where the
ECB publishes. The journal gives the first load and a load of the history the kind history, and a
later load the kind window (ADR-0038)."""

from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
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

CODE = "ecb"
PIPELINE = "agoranomos_ecb"
DATASET = "raw_ecb"
TABLE = "rates_daily"
URL = "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A"
# The account of the record of the loads: the base of the rates, as demo_fx has it.
ACCOUNT = "EUR"
TIME_ZONE = ZoneInfo("Europe/Berlin")
WINDOW_DAYS = 14
HISTORY_MONTHS = 37
# The hints of the table, by the names that dlt gives the columns of the CSV: each load appends its
# rows. Every column stays text, as the ECB writes it, and staging casts it.
HINTS = {
    "write_disposition": APPEND,
    "columns": {
        name: TEXT
        for name in (
            "key",
            "freq",
            "currency",
            "currency_denom",
            "exr_type",
            "exr_suffix",
            "time_period",
            "obs_value",
        )
    },
}
# The retries of a request are explicit, as the contract of the sources asks (ADR-0004): five
# tries over about a minute on a timeout, a broken connection, 429 or a 5xx.
CLIENT = {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}


def yesterday(now: datetime) -> date:
    """The last day whose rates a load at now takes: yesterday in Frankfurt."""
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


def fetch(first: date, last: date) -> str:
    """The CSV of the rates of the days from first to last, as the data API of the ECB writes it:
    empty when the days have no rate."""
    response = Client(**CLIENT).get(
        URL,
        params={
            "format": "csvdata",
            "detail": "dataonly",
            "startPeriod": first.isoformat(),
            "endPeriod": last.isoformat(),
        },
    )
    return response.text


def rows(text: str) -> Iterator[dict[str, str]]:
    """The rows of the CSV of the ECB, each as the CSV names its columns."""
    yield from csv.DictReader(io.StringIO(text))


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
    """Whether a load at now brings anything: no load has taken the day of now in Frankfurt yet,
    or the core needs the history of the rates again (rates.history_from). Another load on the
    same day asks for the same days."""
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
        resources = [
            dlt.resource(or_marker(rows(fetch(first, last))), name=TABLE, **own(HINTS)),
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
