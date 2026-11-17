"""nbk: the official rates of the tenge of the National Bank of Kazakhstan as a dlt source, loaded
into raw_nbk as the loader role (ADR-0004, ADR-0042).

The feed of the NBK, rss/get_rates.cfm?fdate=DD.MM.YYYY, serves the rates of one day as XML and
without a key (as of November 2026): a root <rates> with the day in <date>, and one <item> for each
currency, with its name in Russian (fullname), its ISO code (title), the price in tenge of quant
units of it, to two decimal places at most (description), quant, and the direction and size of the
change from the day before (index, change). A day whose set the NBK has not published yet, as a day
after today, has no <item> but an <info> that says so, and a date in another form than DD.MM.YYYY
or DD/MM/YYYY gets an answer in JSON, with the HTTP status 200. Every calendar day has a set,
weekends and holidays too, and the set of a day is published before the day: the official rate
against the US dollar is the weighted average of the exchange at 15:30 Astana time, set for the
next business day, and the other currencies come from the cross rates of 16:00, so the sets of a
Saturday, a Sunday and a Monday are those of the trading of the Friday before. The list of the
currencies changes over time: 48 on 12 November 2026, 39 on 1 January 2026 and on 1 October 2023.

A load asks for each calendar day of its window, one request at a time, at least PACE seconds
apart, and keeps each item as the feed names its elements, every one text, with the day of the
feed in the column date, in raw_nbk.rates_daily: the macro nbk_rates of dbt/macros/shapes/ reads
them. A day without a set gives no row. Each load appends its rows under its own _dlt_load_id and
records its window in _agoranomos_loads in the same run, with the account KZT, the base of the
rates, as the load of the ECB does with EUR, and the staging model reads, for each day, the rates
of the newest load whose window covers it, of those that the run of the transform pinned (ADR-0038,
ADR-0046). The first load takes the rates from the first day of the month 37 months back, as the
load of the ECB does, about 1,130 requests; a later one, the days from the day after the last that
a whole load took, or from 14 days back when that is earlier, so that a set that the NBK corrects,
or that an earlier load missed, comes again. When the core lacks rates into the reporting currency
on days whose loads it has accepted, a load takes the history again from the first of those days,
or earlier (agoranomos_pipelines/rates.py). All end today in Astana, as the set of a day is known
before it. The journal gives the first load and a load of the history the kind history, and a
later load the kind window (ADR-0038).

The model does not read these rates yet: core.fx_rates_daily takes those of the ECB, or of the
demo (dbt/macros/rates.sql)."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from xml.etree import ElementTree
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
from agoranomos_pipelines.windows import restatement_window_days

log = logging.getLogger(__name__)

CODE = "nbk"
PIPELINE = "agoranomos_nbk"
DATASET = "raw_nbk"
TABLE = "rates_daily"
URL = "https://nationalbank.kz/rss/get_rates.cfm"
# The form of the day that the feed takes in fdate and writes in <date>.
DAY_FORMAT = "%d.%m.%Y"
# The account of the record of the loads: the base of the rates, as EUR is for the ECB.
ACCOUNT = "KZT"
# Astana time, in which the NBK fixes and publishes its rates: UTC+5 in all of Kazakhstan since
# 1 March 2024, by the decree of the Government of 19 January 2024, as the time zone database has it
# for Asia/Almaty since its release 2024a.
TIME_ZONE = ZoneInfo("Asia/Almaty")
HISTORY_MONTHS = 37
# The least time between the starts of two requests, in seconds: the history takes about 1,130.
PACE = 1.0
# The column of the day of the feed, and the elements of each item, which every load writes, all
# as text, as the NBK writes them; a new element of the feed reaches raw as a column of its own.
DAY_COLUMN = "date"
FIELDS = ("fullname", "title", "description", "quant", "index", "change")
HINTS = {
    "write_disposition": APPEND,
    "columns": {name: TEXT for name in (DAY_COLUMN, *FIELDS)},
}
# The retries of a request are explicit, as the contract of the sources asks (ADR-0004): five
# tries over about a minute on a timeout, a broken connection, 429 or a 5xx.
CLIENT = {"request_timeout": 60, "request_max_attempts": 5, "request_backoff_factor": 2}


def today(now: datetime) -> date:
    """The last day whose rates a load at now takes: today in Astana, whose set the NBK published
    the business day before."""
    return now.astimezone(TIME_ZONE).date()


def history_start(now: datetime) -> date:
    """The first day of the first load at now: the first day of the month HISTORY_MONTHS before
    today in Astana."""
    last = today(now)
    months = last.year * 12 + last.month - 1 - HISTORY_MONTHS
    return date(months // 12, months % 12 + 1, 1)


def window(
    now: datetime, last_end: date | None, history: date | None = None
) -> tuple[date, date, str]:
    """The days of a load at now and its kind: the history when no load has taken a day yet, from
    history when the core needs it earlier (rates.history_from), else the days from the day after
    the last loaded, or from the first of the last days of its window of restatements (windows.py)
    when that is earlier, to today in Astana, and those from history, as a load of the history,
    when it is earlier still."""
    end = today(now)
    if last_end is None:
        first = history_start(now)
        return (first if history is None else min(first, history)), end, HISTORY
    width = restatement_window_days(CODE)
    usual = min(last_end + timedelta(days=1), end - timedelta(days=width - 1))
    if history is not None and history < usual:
        return history, end, HISTORY
    return usual, end, WINDOW


def days(first: date, last: date) -> Iterator[date]:
    """Every calendar day from first to last."""
    day = first
    while day <= last:
        yield day
        day += timedelta(days=1)


def fetch(client, day: date) -> bytes:
    """The answer of the feed for one day, as the NBK sends it."""
    return client.get(URL, params={"fdate": day.strftime(DAY_FORMAT)}).content


def items(day: date, content: bytes) -> list[dict[str, str | None]]:
    """The items of the answer of the feed for day, each as the feed names its elements, with the
    day of the feed: none when the NBK has published no set for the day. An answer that is not the
    XML of the rates of that day, such as the JSON of a date in another form, stops the load."""
    # The XML of a bank, which declares no document type: ElementTree with the expat of Python
    # 3.13 expands no external entity, and one that declares a type is not the feed.
    if b"<!DOCTYPE" in content[:1024].upper():
        raise ValueError(f"The NBK answered {day.isoformat()} with a document type: not its feed")
    try:
        root = ElementTree.fromstring(content)  # noqa: S314 (no document type, as checked above)
    except ElementTree.ParseError as error:
        raise ValueError(
            f"The NBK answered {day.isoformat()} with something other than XML: {content[:200]!r}"
        ) from error
    stated = root.findtext(DAY_COLUMN)
    if root.tag != "rates" or stated != day.strftime(DAY_FORMAT):
        raise ValueError(
            f"The NBK answered {day.isoformat()} with <{root.tag}> of the day {stated!r}, not with "
            "the rates of that day"
        )
    found = []
    for item in root.findall("item"):
        row: dict[str, str | None] = {DAY_COLUMN: stated}
        for element in item:
            if element.tag != DAY_COLUMN:
                row[element.tag] = element.text.strip() if element.text is not None else None
        found.append(row)
    return found


def rows(first: date, last: date) -> Iterator[dict[str, str | None]]:
    """The items of each day from first to last, one request a day, each starting at least PACE
    seconds after the one before."""
    client = Client(**CLIENT)
    started = None
    empty = []
    for day in days(first, last):
        if started is not None:
            wait = PACE - (time.monotonic() - started)
            if wait > 0:
                time.sleep(wait)
        started = time.monotonic()
        found = items(day, fetch(client, day))
        if not found:
            empty.append(day)
        yield from found
    if empty:
        log.info(
            "The NBK has published no set for %d days of the window, the first %s and the last %s: "
            "they have no row",
            len(empty),
            empty[0].isoformat(),
            empty[-1].isoformat(),
        )


def journal(now: datetime, first: date, last: date, kind: str) -> list[dict[str, object]]:
    """The row of _agoranomos_loads of a load at now, with its kind: a load of the rates fails
    whole or not at all, so the row is always one of a loaded account."""
    return [
        {
            "account_id": ACCOUNT,
            "as_of": today(now),
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
    """Whether a load at now brings anything: no load has taken the day of now in Astana yet, or
    the core needs the history of the rates again (rates.history_from). Another load on the same
    day asks for the same days."""
    day = today(now)
    with open_pipeline() as pipeline:
        found = recorded(pipeline).get(ACCOUNT) if has_loads(pipeline) else None
        if not found or found[0] < day:
            return True
        if rates.history_from(pipeline, CODE) is not None:
            return True
    log.info("The rates of the NBK have a load of %s already: nothing to load", day.isoformat())
    return False


def load(now: datetime) -> int:
    """Load the rates of the window of now (window()), with the history that the core needs
    (rates.history_from), record it in _agoranomos_loads with its kind, and return the rows of
    rates_daily that dlt reports."""
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline:
        first, last, kind = window(now, last_end(pipeline), rates.history_from(pipeline, CODE))
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
