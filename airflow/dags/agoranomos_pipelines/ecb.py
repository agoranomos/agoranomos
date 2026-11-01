"""ecb: the daily reference rates of the euro of the European Central Bank as a dlt source, loaded
into raw_ecb as the loader role (ADR-0004, ADR-0032).

The data API of the ECB serves the series EXR/D..EUR.SP00.A, the rate of each currency for one euro
on each day of publication, as CSV and without a key; the ECB publishes at about 16:00 CET on each
TARGET working day. A load asks for the data of its window only (detail=dataonly) and keeps each
row as the CSV names its columns, which dlt writes in snake_case into raw_ecb.rates_daily, all as
text, as the ECB writes them: the macro ecb_exr of dbt/macros/shapes/ reads them. A window without
a rate, such as a weekend, comes back empty and still replaces its days. Each load records its
window in _agoranomos_reloads in the same run, with the account EUR, the base of the rates, as the
loads of the demo do. The first load takes the rates from the first day of the month 37 months
back, one month before the history of Google Ads (ADR-0034), so that its first day has a rate; a
later one, the days from the day after the last that a load took, or from 7 days back when that is
earlier, so that a rate that the ECB corrects comes again. Both end yesterday in Frankfurt,
where the ECB publishes."""

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

from agoranomos_pipelines.loads import (
    DLT_SETTINGS,
    LOADED,
    MERGE,
    RELOAD_HINTS,
    RELOADS,
    TEXT,
    credentials,
    has_loads,
    or_marker,
    own,
    recorded,
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
WINDOW_DAYS = 7
HISTORY_MONTHS = 37
# The hints of the table, by the names that dlt gives the columns of the CSV. Every column stays
# text, as the ECB writes it, and staging casts it.
HINTS = {
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


def window(now: datetime, last_end: date | None) -> tuple[date, date]:
    """The days of a load at now: the history when no load has taken a day yet, else the days
    from the day after the last loaded, or from the first of the last WINDOW_DAYS when that is
    earlier, to yesterday."""
    end = yesterday(now)
    if last_end is None:
        return history_start(now), end
    return min(last_end + timedelta(days=1), end - timedelta(days=WINDOW_DAYS - 1)), end


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


def scope(first: date, last: date) -> str:
    """The rows that a load of the days from first to last replaces, as the SQL condition of dlt's
    destination scope: the dates are those of the code, never text from outside."""
    return f"time_period BETWEEN '{first.isoformat()}' AND '{last.isoformat()}'"


def reloads(now: datetime, first: date, last: date) -> list[dict[str, object]]:
    """The row of _agoranomos_reloads of a load at now: a load of the rates fails whole or not at
    all, so the row is always one of a loaded account."""
    return [
        {
            "account_id": ACCOUNT,
            "as_of": now.astimezone(TIME_ZONE).date(),
            "window_start": first,
            "window_end": last,
            "now": now,
            "loaded_at": datetime.now(UTC),
            "status": LOADED,
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
            destination=dlt.destinations.postgres(credentials=credentials()),
            dataset_name=DATASET,
        )


def last_end(pipeline) -> date | None:
    """The last day that a load of the rates took, or None before the first complete load."""
    if not has_loads(pipeline):
        return None
    found = recorded(pipeline).get(ACCOUNT)
    return found[1] if found else None


def due(now: datetime) -> bool:
    """Whether a load at now brings anything: no load has taken the day of now in Frankfurt yet.
    Another load on the same day asks for the same days."""
    with open_pipeline() as pipeline:
        found = recorded(pipeline).get(ACCOUNT) if has_loads(pipeline) else None
    today = now.astimezone(TIME_ZONE).date()
    if found and found[0] >= today:
        log.info("The rates have a load of %s already: nothing to load", today.isoformat())
        return False
    return True


def load(now: datetime) -> int:
    """Load the rates of the window of now (window()), record it in _agoranomos_reloads, and return
    the rows of rates_daily that dlt reports."""
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline:
        first, last = window(now, last_end(pipeline))
        hints = own(HINTS)
        hints["write_disposition"] = {**MERGE, "destination_scope": scope(first, last)}
        resources = [
            dlt.resource(or_marker(rows(fetch(first, last))), name=TABLE, **hints),
            # In the same run as the rates, so that a load that fails records no window.
            dlt.resource(reloads(now, first, last), name=RELOADS, **own(RELOAD_HINTS)),
        ]
        pipeline.run(resources)
        loaded = pipeline.last_trace.last_normalize_info.row_counts.get(TABLE, 0)
    log.info(
        "Loaded %d rows into %s.%s, the days from %s to %s",
        loaded,
        DATASET,
        TABLE,
        first.isoformat(),
        last.isoformat(),
    )
    return loaded
