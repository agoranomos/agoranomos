"""ga4: the GA4 properties of the deployment as a dlt source, loaded into raw_ga4 as the loader role
(ADR-0004, ADR-0054).

The source loads the properties of the platform ga4 that extensions/model/clients.toml names, by
their numeric ID without properties/, and no other, so that a service account that is a user of
the properties of several businesses brings in none of another (ADR-0024). It signs in as a
service account of the Google Cloud project of the deployment, whose key is
GA4_SERVICE_ACCOUNT_KEY, in the forms that GOOGLE_ADS_SERVICE_ACCOUNT_KEY takes, JSON or base64 of
the JSON, which may hold the same key, with the scope analytics.readonly alone; the service account
is a user of each property with the role Viewer. It calls the REST endpoints of version API_VERSION
of the Google Analytics Admin API and the Google Analytics Data API with the HTTP client of dlt and
an access token of google-auth, sent as Authorization: Bearer, never in a URL.

For each property, one after another and one request at a time, a load reads properties.get of the
Admin API, for its name, display name, time zone and currency, into properties, and then one
runReport of the Data API over its window in one date range, with the dimensions and the metrics
of the shape of demo_web, which the macros of dbt/macros/shapes/ga4_v1beta.sql read, into
report_sessions_daily, with property added to each row as properties/<id>. Every value stays text,
as the API writes a metric as a string. A report comes in pages of up to 250,000 rows, the most
that a request returns, each next page from its offset, in an order on the six dimensions that
keeps the pages of one report apart; each page goes to a file on disk a row at a time as it
arrives, so that a task holds a row and not a page, and the rows that the pages give must be the
rowCount of the report, or the property fails. A row whose measures are all zero does not come
(keepEmptyRows false), as the shape drops it anyway.

Four fields of the metadata of each answer say that its numbers may not be the whole truth:
subjectToThresholding, dataLossFromOtherRow, samplingMetadatas and dataTruncationReasons. The load
writes them for each property and load into four boolean columns of the journal
_agoranomos_loads, is_subject_to_thresholding, has_data_loss_from_other_row, is_sampled and
is_truncated, each true when any page of the property in the load had it; the pin copies them
into core.loads (loads.FLAGS). A flag says "may": a report subject to thresholding may have lost
nothing.

The window of a daily load of a property is [min(yesterday + 1 - W, last accepted day + 1),
yesterday] in the time zone of the property, from properties.get, where W is the window of
restatements of ga4 in the seed of the platforms, 14 days (ADR-0038, windows.py), and the last
accepted day the end of the newest accepted load of the property in core.loads, or, before the core
has accepted one, of its newest whole load in the journal. The first load takes the window, and the
history follows in the background by quarters, from new to old, through history.py (plan): the DAG
agoranomos_ga4_history starts a run of the load with the properties and the days of a portion as
its parameters, which takes those days with the kind history. No page that Google publishes gives
the oldest day that runReport returns, so no oldest day bounds the plan: a quarter before the
property had data costs one request and writes its window and no row. GA4 takes no load of the kind
intraday (ADR-0054, "No current day").

The quota and the errors (ADR-0054): each answer of a report carries propertyQuota, which the load
logs, consumed and remaining, of each bucket. A request is asked again after a 500 or a 503 at most
RETRIES times, after a wait that doubles, and a property fails at its fourth server error in one
load, as a project may have ten of them an hour for a property. A 429 RESOURCE_EXHAUSTED fails the
property at once when the last answer of the property left no token of the day, as the daily
tokens come back only at midnight Pacific time, and otherwise waits for the hourly buckets,
QUOTA_WAIT at a time and at most QUOTA_PATIENCE in one load, then fails the property. Any other
error of the API, such as 400, 403 or 404, fails the property at once. A timeout or a broken
connection before an answer starts is asked again by dlt; one in the middle of a page is asked
again here, after the file is cut back to where the page began, up to ATTEMPTS times. A property
that fails leaves nothing in the load, is recorded in the journal as failed with the error, the
others load, and the load fails at the end with one line that lists the properties and their
errors (ADR-0035), which Airflow retries; on the last try, the task of the load succeeds when it
took another property, so that the transform takes it, and the run fails after it (outcome.py).
What the API returns is data: a campaign name can carry any text, and nothing here acts on it."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import TextIO
from zoneinfo import ZoneInfoNotFoundError

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client, RequestException
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from agoranomos_pipelines.google_ads import (
    SPACE,
    Answer,
    BrokenAnswer,
    pieces,
    service_account_info,
    today,
    yesterday,
)
from agoranomos_pipelines.history import Portion, history_start, planned, portion_from
from agoranomos_pipelines.loads import (
    APPEND,
    DLT_SETTINGS,
    FAILED,
    FLAGS,
    HISTORY,
    INTRADAY,
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
from agoranomos_pipelines.settings.files import load_settings
from agoranomos_pipelines.windows import restatement_window_days

log = logging.getLogger(__name__)

CODE = "ga4"
PIPELINE = "agoranomos_ga4"
DATASET = "raw_ga4"
# The version of both APIs, in this one place (ADR-0054). As of November 2026 the Data API and the
# Admin API have v1alpha and v1beta and no v1, Google says of Beta that "No breaking changes are
# expected in this channel", and no page gives a date of deprecation for either v1beta, so no test
# of a sunset comes until Google publishes one.
API_VERSION = "v1beta"
REPORT_URL = "https://analyticsdata.googleapis.com/{version}/properties/{property}:runReport"
PROPERTY_URL = "https://analyticsadmin.googleapis.com/{version}/properties/{property}"
# The one scope that both requests take, and the least: properties.get and runReport each accept
# analytics.readonly (their references, read on 22 November 2026).
SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
# The folder of the settings of the deployment, which compose.yaml mounts read-only.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
KEY = "GA4_SERVICE_ACCOUNT_KEY"
# The ID of a property: its digits, without properties/. It goes into the URL of each request and
# the SQL of the journal, so nothing else may.
PROPERTY_ID = re.compile(r"[0-9]{1,20}")
# The dimensions and the metrics of the report, in the order of the request, which the headers of
# each answer must keep: those of the shape ga4_v1beta, six of the nine dimensions that a request
# may have (the basics of the Data API, read on 22 November 2026).
DIMENSIONS = (
    "date",
    "sessionSource",
    "sessionMedium",
    "sessionCampaignName",
    "sessionCampaignId",
    "sessionDefaultChannelGroup",
)
METRICS = ("sessions", "engagedSessions", "keyEvents", "totalRevenue")
# "The API returns a maximum of 250,000 rows per request, no matter how many you ask for" (the
# reference of runReport, read on 22 November 2026).
LIMIT = 250_000
# The client of dlt asks a request again after a timeout or a connection that fails before an
# answer starts, up to three tries, and never on a status: the load decides on each status itself.
CLIENT = {
    "request_timeout": 300,
    "request_max_attempts": 3,
    "request_backoff_factor": 2,
    "raise_for_status": False,
    "status_codes": (),
}
# Server errors, as the quotas of the Data API count them: "Server Errors are 500 and 503 codes",
# ten an hour for a project and a standard property (read on 22 November 2026). A request is asked
# again after one at most RETRIES times, after BACKOFF seconds and then twice as long, and a
# property fails at its fourth in one load, so that a daily load and a portion of history in the
# same hour stay below ten.
SERVER_ERRORS = frozenset({500, 503})
RETRIES = 2
SERVER_ERRORS_PER_PROPERTY = 3
BACKOFF = 15
# A 429 RESOURCE_EXHAUSTED, "If any quota bucket reaches 0 remaining": the hourly buckets "are
# refreshed within an hour but not necessarily on the whole hour boundaries", so a load waits
# QUOTA_WAIT seconds at a time, at most QUOTA_PATIENCE seconds in all, within the timeout of its
# task of two hours.
QUOTA_WAIT = 5 * 60
QUOTA_PATIENCE = 70 * 60
# An answer whose connection breaks in the middle of a page is asked again, up to ATTEMPTS
# requests in all.
ATTEMPTS = 3
# The buckets of propertyQuota, as the reference of PropertyQuota names them.
QUOTAS = (
    "tokensPerDay",
    "tokensPerHour",
    "tokensPerProjectPerHour",
    "concurrentRequests",
    "serverErrorsPerProjectPerHour",
    "potentiallyThresholdedRequestsPerHour",
)
# The fields of properties.get that the shape reads, and no other.
PROPERTY_FIELDS = ("name", "displayName", "timeZone", "currencyCode")
# The flags of the answers of a property in a load are FLAGS of loads.py, as columns of the
# journal (ADR-0054).
BOOL = {"data_type": "bool"}


def snake(name: str) -> str:
    """The name that dlt gives a field of the API: sessionSource as session_source."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# The hints of each table, by the names that dlt gives the fields: every field as text, as the API
# writes them, so that dlt makes each column even when no row of a load has it. Both tables append:
# the property of each load and the rows of its window, so staging reads the newest load of each
# property (ADR-0038).
HINTS = {
    "properties": {
        "write_disposition": APPEND,
        "columns": {snake(name): TEXT for name in PROPERTY_FIELDS},
    },
    "report_sessions_daily": {
        "write_disposition": APPEND,
        "columns": {snake(name): TEXT for name in ("property", *DIMENSIONS, *METRICS)},
    },
}
# The journal of the loads of GA4: that of every source, with the four flags.
JOURNAL_HINTS = {
    **LOAD_HINTS,
    "columns": {**LOAD_HINTS["columns"], **{flag: BOOL for flag in FLAGS}},
}


class Ga4Error(RuntimeError):
    """A mistake in the settings of the source, which stops the load before any request, or an
    answer of the API that stops the load of one property: one line that names the property and
    gives the error of the API."""


class AnswerBrokeOff(Ga4Error):
    """A page whose connection broke after it had started, which read_report asks again."""


class PropertiesFailed(Ga4Error):
    """The load took the other properties, if any, and these failed: one line that counts them and
    lists each property with the error of the API (ADR-0004), and loaded, how many properties the
    load took. Airflow retries the task, as after any other error, and the retry loads every
    property again; on the last try, the task of the load succeeds when it took one, so that the
    transform takes them, and the last task of the DAG fails the run with the line, and with none
    the task fails (outcome.py). The next load takes the days that the failed properties missed."""

    def __init__(self, line: str, loaded: int) -> None:
        super().__init__(line)
        self.loaded = loaded


def properties(extensions: Path = EXTENSIONS) -> list[str]:
    """The IDs of the GA4 properties that clients.toml of the deployment names, checked."""
    found = [
        entry["account_id"]
        for entry in load_settings(extensions, None).account_clients
        if entry["platform"] == CODE
    ]
    if not found:
        raise Ga4Error("extensions/model/clients.toml names no property of the platform ga4")
    wrong = [property_id for property_id in found if not PROPERTY_ID.fullmatch(property_id)]
    if wrong:
        raise Ga4Error(
            "clients.toml: an account_id of ga4 is the digits of the property without "
            f"properties/, such as 123456789, not {wrong[0]!r}"
        )
    return sorted(found)


def checked(property_ids: Iterable[str]) -> list[str]:
    """The IDs of the properties of a scope or a query, sorted: at least one, each digits. They go
    into URLs and SQL as they are, so nothing else may."""
    found = sorted(property_ids)
    if not found or not all(PROPERTY_ID.fullmatch(property_id) for property_id in found):
        raise ValueError("a scope of ga4 takes one property ID of digits or more")
    return found


def key_info() -> dict:
    """The key of the service account, from GA4_SERVICE_ACCOUNT_KEY. A message never shows it."""
    return service_account_info(KEY, Ga4Error)


def sign_in() -> service_account.Credentials:
    """The credentials of the service account, with an access token for the scope
    analytics.readonly."""
    signed = service_account.Credentials.from_service_account_info(key_info(), scopes=[SCOPE])
    signed.refresh(Request())
    return signed


def window(last: date, last_end: date | None) -> tuple[date, date]:
    """The days of a load of a property whose last day is last, as ADR-0038 gives them,
    [min(last + 1 - W, last_end + 1), last], where W is the window of restatements of ga4 in the
    seed of the platforms (windows.py), read now, and last_end the last day that the core accepted
    for the property: the last W days when none is known yet, else the days from the day after it,
    or from the first of the last W days when that is earlier, so that a stop of any length closes
    by itself."""
    usual = last - timedelta(days=restatement_window_days(CODE) - 1)
    if last_end is None:
        return usual, last
    return min(last_end + timedelta(days=1), usual), last


def accepted(pipeline, property_ids: Iterable[str]) -> dict[str, date]:
    """The last day that the core accepted for each property: the end of the window of its newest
    accepted whole load in core.loads, which loader may read (ADR-0038); none for a property of
    which the core accepted no load, as before the transform reads raw_ga4."""
    # IDs of checked digits, and constants of this module.
    wanted = ", ".join(f"'{property_id}'" for property_id in checked(property_ids))
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                "SELECT account_id, max(window_to) FROM core.loads "  # noqa: S608
                f"WHERE source = '{CODE}' AND status = 'accepted' AND outcome = '{LOADED}' "
                f"AND kind <> '{INTRADAY}' AND account_id IN ({wanted}) GROUP BY account_id"
            )
    except DatabaseUndefinedRelation:
        return {}
    return {property_id: end for property_id, end in rows or () if end is not None}


def last_day(
    property_id: str, core: dict[str, date], journal: dict[str, tuple[date, date]]
) -> date | None:
    """The day after which the window of a property starts: the last that the core accepted for
    it, or, while the core has accepted no load of it, the last day of its newest whole load in the
    journal; none when neither knows it, which then takes its usual window."""
    if property_id in core:
        return core[property_id]
    if property_id in journal:
        return journal[property_id][1]
    return None


def portion_of(
    property_ids: Iterable[str] | None, first: str | None, last: str | None
) -> Portion | None:
    """The portion that the parameters of a run of the DAG of the load name: properties, as their
    digits, or none for every property, and the first and last day, as ISO dates; none when the run
    names no days, as a scheduled run does, which takes the usual window of each property."""
    return portion_from(
        property_ids,
        first,
        last,
        form=PROPERTY_ID,
        shape="the digits of a property without properties/, such as 123456789",
        error=Ga4Error,
    )


def plan(now: datetime, extensions: Path = EXTENSIONS) -> Portion | None:
    """The next portion of the history of the properties of the deployment as of now, back to the
    first day of the month 36 months before yesterday in UTC, or none: when every property holds
    its history, when no property has had its usual window yet, or while a whole load waits for
    the run of the transform that pins it (history.py)."""
    wanted = checked(properties(extensions))
    limit = history_start(now.astimezone(UTC).date() - timedelta(days=1))
    with open_pipeline() as pipeline:
        return planned(pipeline, CODE, "GA4", wanted, limit)


def report_body(days: tuple[date, date], offset: int) -> dict:
    """The body of runReport for the days of a window and the page that starts at offset."""
    first, last = days
    body: dict = {
        "dateRanges": [{"startDate": first.isoformat(), "endDate": last.isoformat()}],
        "dimensions": [{"name": name} for name in DIMENSIONS],
        "metrics": [{"name": name} for name in METRICS],
        "limit": str(LIMIT),
        # One order of the rows of a report, so that its pages follow each other; whether GA4
        # needs it for that, no page that Google publishes says (ADR-0054).
        "orderBys": [{"dimension": {"dimensionName": name}} for name in DIMENSIONS],
        "keepEmptyRows": False,
        "returnPropertyQuota": True,
    }
    if offset:
        body["offset"] = str(offset)
    return body


class Report(Answer):
    """An answer of runReport as it arrives, an object read a member at a time: the rows of its
    member rows one at a time, and its other members, the headers, rowCount, metadata and
    propertyQuota, kept in members as they come."""

    def __init__(self, pieces: Iterable[str]):
        super().__init__(pieces)
        self.members: dict[str, object] = {}

    def rows(self) -> Iterator[dict]:
        """The rows of the answer as they come, and then nothing to its end but white space."""
        self.take("{")
        if self.char() == "}":
            self.at += 1
        else:
            while True:
                name = self.value()
                if not isinstance(name, str):
                    raise BrokenAnswer("the answer has a name of a member that is not a string")
                self.take(":")
                if name == "rows":
                    yield from self.results()
                else:
                    self.members[name] = self.value()
                if self.take(",}") == "}":
                    break
        while True:
            self.at = SPACE.match(self.text, self.at).end()
            if self.at < len(self.text):
                raise BrokenAnswer("the answer goes on after its object")
            piece = next(self.pieces, None)
            if piece is None:
                return
            self.text, self.at = piece, 0


@dataclass
class Asked:
    """What a load has asked of a property: its server errors, and the propertyQuota of its last
    answer."""

    server_errors: int = 0
    quota: dict = field(default_factory=dict)


def daily_spent(quota: dict) -> bool:
    """Whether the last answer of a property left it no token of the day."""
    remaining = (quota.get("tokensPerDay") or {}).get("remaining")
    return isinstance(remaining, int) and remaining <= 0


class Api:
    """The REST endpoints of the Admin API and the Data API, as the service account, with the
    retries of ADR-0054."""

    def __init__(self, signed: service_account.Credentials):
        self.signed = signed
        self.client = Client(**CLIENT)
        self.requests = 0
        self.waited = 0

    def headers(self) -> dict[str, str]:
        if not self.signed.valid:
            self.signed.refresh(Request())
        return {"Authorization": f"Bearer {self.signed.token}"}

    def answer(self, property_id: str, asked: Asked, method: str, url: str, body=None):
        """The answer of a request about a property, once its status is 200, as it starts to
        arrive. A 500 or a 503 is asked again (SERVER_ERRORS), a 429 waits while the property may
        have tokens of the day (QUOTA_WAIT), and every other error raises Ga4Error with the status
        and the start of the body of the answer, which names the error."""
        retries = 0
        while True:
            self.requests += 1
            try:
                response = self.client.request(
                    method, url, json=body, headers=self.headers(), stream=True
                )
            except RequestException as error:
                raise Ga4Error(f"GA4 property {property_id}: no answer: {error}") from None
            status = response.status_code
            if status == 200:
                return response
            try:
                text = response.text[:2000]
            finally:
                response.close()
            error = f"GA4 property {property_id}: {status} {text}"
            if status in SERVER_ERRORS:
                asked.server_errors += 1
                if asked.server_errors > SERVER_ERRORS_PER_PROPERTY:
                    raise Ga4Error(f"{error} (server error {asked.server_errors} of this load)")
                if retries >= RETRIES:
                    raise Ga4Error(f"{error} (after {RETRIES} retries)")
                retries += 1
                wait = BACKOFF * 2 ** (retries - 1)
                log.warning(
                    "%s; asking again in %d s, retry %d of %d", error, wait, retries, RETRIES
                )
                sleep(wait)
                continue
            if status == 429:
                if daily_spent(asked.quota):
                    raise Ga4Error(f"{error} (no token of the day is left)")
                if self.waited + QUOTA_WAIT > QUOTA_PATIENCE:
                    raise Ga4Error(f"{error} (after {self.waited // 60} minutes of waiting)")
                log.warning("%s; waiting %d s for the hourly quota", error, QUOTA_WAIT)
                sleep(QUOTA_WAIT)
                self.waited += QUOTA_WAIT
                continue
            raise Ga4Error(error)

    def details(self, property_id: str, asked: Asked) -> dict:
        """properties.get of a property: its name, display name, time zone and currency."""
        url = PROPERTY_URL.format(version=API_VERSION, property=property_id)
        response = self.answer(property_id, asked, "GET", url)
        try:
            found = json.loads(response.text)
        except (ValueError, RequestException) as error:
            raise Ga4Error(f"GA4 property {property_id}: a broken answer: {error}") from None
        finally:
            response.close()
        if not isinstance(found, dict) or found.get("name") != f"properties/{property_id}":
            raise Ga4Error(f"GA4 property {property_id}: the Admin API returned another property")
        return {name: found.get(name) for name in PROPERTY_FIELDS}

    def page(
        self, property_id: str, asked: Asked, days: tuple[date, date], offset: int, file: TextIO
    ) -> tuple[int, dict]:
        """Write the rows of the page of the report of a property over days that starts at
        offset to file, a line each as they come, the values of the dimensions and then of the
        metrics, and return how many came and the other members of the answer. A page whose
        connection breaks raises AnswerBrokeOff, and one that is not whole JSON Ga4Error."""
        url = REPORT_URL.format(version=API_VERSION, property=property_id)
        response = self.answer(property_id, asked, "POST", url, report_body(days, offset))
        report = Report(pieces(response))
        count = 0
        try:
            for row in report.rows():
                file.write(json.dumps(values(row)) + "\n")
                count += 1
        except (BrokenAnswer, UnicodeDecodeError) as error:
            raise Ga4Error(f"GA4 property {property_id}: a broken answer: {error}") from None
        except RequestException as error:
            raise AnswerBrokeOff(
                f"GA4 property {property_id}: the answer broke off: {error}"
            ) from None
        finally:
            response.close()
        return count, report.members


def values(row: object) -> list[str | None]:
    """The values of a row of a report, its dimensions and then its metrics, as text."""
    if not isinstance(row, dict):
        raise BrokenAnswer("a row of the answer is not an object")
    found = []
    for name in ("dimensionValues", "metricValues"):
        cells = row.get(name) or []
        if not isinstance(cells, list):
            raise BrokenAnswer(f"{name} of a row of the answer is not an array")
        found += [cell.get("value") if isinstance(cell, dict) else None for cell in cells]
    if len(found) != len(DIMENSIONS) + len(METRICS):
        raise BrokenAnswer("a row of the answer has another number of values than the request")
    return found


def headers_of(members: dict, count: int) -> None:
    """Check that the headers of a page name the dimensions and the metrics of the request in its
    order, which the values of its rows follow. A page without rows may have none."""
    names = (
        [h.get("name") for h in members.get("dimensionHeaders") or [] if isinstance(h, dict)],
        [h.get("name") for h in members.get("metricHeaders") or [] if isinstance(h, dict)],
    )
    if count == 0 and names == ([], []):
        return
    if names != (list(DIMENSIONS), list(METRICS)):
        raise BrokenAnswer("the headers of the answer differ from the request")


def flags_of(members: dict) -> dict[str, bool]:
    """The four flags of the metadata of an answer (ADR-0054)."""
    meta = members.get("metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    return {
        "is_subject_to_thresholding": meta.get("subjectToThresholding") is True,
        "has_data_loss_from_other_row": meta.get("dataLossFromOtherRow") is True,
        "is_sampled": bool(meta.get("samplingMetadatas")),
        "is_truncated": bool(meta.get("dataTruncationReasons")),
    }


def log_quota(property_id: str, members: dict) -> dict:
    """Log propertyQuota of an answer, consumed and remaining of each bucket, and return it."""
    quota = members.get("propertyQuota")
    if not isinstance(quota, dict):
        return {}
    parts = []
    for name in QUOTAS:
        bucket = quota.get(name)
        if isinstance(bucket, dict):
            parts.append(f"{name} {bucket.get('consumed', 0)}/{bucket.get('remaining', '?')}")
    log.info("GA4 property %s, quota consumed/remaining: %s", property_id, ", ".join(parts))
    return quota


def warn_of_restrictions(property_id: str, members: dict) -> None:
    """A warning when GA4 says that it restricted a metric of the report for the service account,
    as a data restriction such as No Revenue Metrics shows 0 for totalRevenue."""
    meta = members.get("metadata") or {}
    restrictions = (meta.get("schemaRestrictionResponse") or {}) if isinstance(meta, dict) else {}
    restricted = [
        str(found.get("metricName"))
        for found in restrictions.get("activeMetricRestrictions") or []
        if isinstance(found, dict)
    ]
    if restricted:
        log.warning(
            "GA4 property %s restricts %s for the service account: give it the role Viewer with "
            "no data restriction",
            property_id,
            ", ".join(sorted(set(restricted))),
        )


@dataclass
class Read:
    """Everything that a load read of one property, before dlt sees any of it: its row of
    properties, the window of its report, the file of its rows, and the four flags."""

    prop: dict
    window: tuple[date, date]
    file: Path
    flags: dict[str, bool]


def read_report(
    api: Api, property_id: str, asked: Asked, days: tuple[date, date], path: Path
) -> dict[str, bool]:
    """Write the rows of the report of a property over days to the file path, page by page, and
    return its flags. The rows of the pages must be the rowCount of the report: a page lost or
    given twice fails the property. A page whose connection breaks is asked again, after the file
    is cut back to where it began, up to ATTEMPTS times."""
    flags = dict.fromkeys(FLAGS, False)
    total, expected = 0, None
    with path.open("w", encoding="utf-8") as file:
        while True:
            start = file.tell()
            for attempt in range(1, ATTEMPTS + 1):
                try:
                    count, members = api.page(property_id, asked, days, total, file)
                    break
                except AnswerBrokeOff as error:
                    if attempt == ATTEMPTS:
                        raise AnswerBrokeOff(f"{error} (attempt {attempt} of {ATTEMPTS})") from None
                    log.warning("%s; asking again, attempt %d of %d", error, attempt + 1, ATTEMPTS)
                    file.seek(start)
                    file.truncate()
            if "error" in members:
                raise Ga4Error(f"GA4 property {property_id}: an error in the answer")
            try:
                headers_of(members, count)
            except BrokenAnswer as error:
                raise Ga4Error(f"GA4 property {property_id}: {error}") from None
            asked.quota = log_quota(property_id, members) or asked.quota
            warn_of_restrictions(property_id, members)
            for name, value in flags_of(members).items():
                flags[name] = flags[name] or value
            # proto3 JSON leaves out a count of zero.
            row_count = members.get("rowCount", 0)
            if not isinstance(row_count, int):
                raise Ga4Error(
                    f"GA4 property {property_id}: the rowCount of the answer is no number"
                )
            if expected is None:
                expected = row_count
            elif row_count != expected:
                raise Ga4Error(
                    f"GA4 property {property_id}: the report counted {expected} rows on its first "
                    f"page and {row_count} on the page at offset {total}"
                )
            total += count
            if total >= expected or count == 0:
                break
    if total != expected:
        raise Ga4Error(
            f"GA4 property {property_id}: the pages gave {total} rows, the report counts {expected}"
        )
    return flags


def read_property(
    api: Api,
    property_id: str,
    now: datetime,
    last_end: date | None,
    staging: Path,
    days: tuple[date, date] | None = None,
) -> Read:
    """Read a property whole: properties.get, which gives its time zone and so its window, then
    its report over the window into a file in the folder of the property under staging, on disk.
    With days, a portion of the history, the window is those days, up to yesterday of the
    property. An error raises Ga4Error and leaves nothing of the property."""
    asked = Asked()
    prop = api.details(property_id, asked)
    try:
        last_day_of_property = yesterday(now, str(prop["timeZone"]))
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        raise Ga4Error(
            f"GA4 property {property_id}: the time zone of the property is not one of IANA: "
            f"{str(prop['timeZone'])[:100]!r}"
        ) from None
    if days is None:
        first, last = window(last_day_of_property, last_end)
    else:
        first, last = days[0], min(days[1], last_day_of_property)
        if first > last:
            raise Ga4Error(
                f"GA4 property {property_id}: the days of the history start on "
                f"{first.isoformat()}, after its yesterday, {last_day_of_property.isoformat()}"
            )
    # The ID is checked digits.
    folder = staging / property_id
    folder.mkdir()
    path = folder / "report_sessions_daily.jsonl"
    try:
        flags = read_report(api, property_id, asked, (first, last), path)
    except Ga4Error:
        shutil.rmtree(folder, ignore_errors=True)
        raise
    return Read(prop, (first, last), path, flags)


def staged_rows(read: dict[str, Read]) -> Iterator[dict]:
    """The rows of the report of each property read, from its file, as dictionaries of the names
    of the API, with the property."""
    names = (*DIMENSIONS, *METRICS)
    for property_id, prop in read.items():
        with prop.file.open(encoding="utf-8") as file:
            for line in file:
                yield {
                    "property": f"properties/{property_id}",
                    **dict(zip(names, json.loads(line), strict=True)),
                }


def journal_rows(
    read: dict[str, Read], failed: dict[str, Ga4Error], now: datetime, kind: str
) -> list[dict]:
    """The rows of the journal of a load of the kind kind at now, by property: each property read
    with its window and its flags, and each failed property with its error and no flag."""
    loaded_at = datetime.now(UTC)
    rows = [
        {
            "account_id": property_id,
            # The day of the load in the time zone of the property.
            "as_of": today(now, str(prop.prop["timeZone"])),
            "window_from": prop.window[0],
            "window_to": prop.window[1],
            "now": now,
            "loaded_at": loaded_at,
            "status": LOADED,
            "error": None,
            "kind": kind,
            **prop.flags,
        }
        for property_id, prop in read.items()
    ] + [
        {
            "account_id": property_id,
            "as_of": None,
            "window_from": None,
            "window_to": None,
            "now": now,
            "loaded_at": None,
            "status": FAILED,
            "error": str(error),
            "kind": kind,
            **dict.fromkeys(FLAGS),
        }
        for property_id, error in failed.items()
    ]
    return sorted(rows, key=lambda row: row["account_id"])


def failure_line(failed: dict[str, Ga4Error], loaded: int) -> str:
    """The line with which the task fails after a load with failed properties (ADR-0004)."""
    if loaded:
        rest = f"the other {loaded} {'is' if loaded == 1 else 'are'} loaded"
    else:
        rest = "none is loaded"
    errors = "; ".join(str(error) for error in failed.values())
    return f"{len(failed)} of {len(failed) + loaded} GA4 properties failed, {rest}: {errors}"


@contextmanager
def open_pipeline() -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of GA4, as the loader role, in a temporary working folder."""
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        yield dlt.pipeline(
            pipeline_name=PIPELINE,
            pipelines_dir=workdir,
            destination=destination(),
            dataset_name=DATASET,
        )


def load(
    now: datetime, extensions: Path = EXTENSIONS, portion: Portion | None = None
) -> dict[str, int]:
    """Load the properties of the deployment as of now, each read whole before dlt sees any of it,
    record each property in _agoranomos_loads, loaded with its window and its flags or failed with
    its error, and return the rows of each table that dlt reports. With a portion, the load takes
    the days of the portion of its properties, which clients.toml names, or of every property,
    with the kind history. When a property failed, the load takes the others and raises
    PropertiesFailed at the end; when every property failed, it writes the journal alone."""
    named = properties(extensions)
    wanted = named
    if portion is not None and portion.accounts:
        unnamed = sorted(set(portion.accounts) - set(named))
        if unnamed:
            raise Ga4Error(
                "extensions/model/clients.toml names no property of ga4 "
                + ", ".join(unnamed)
                + ": a run of the history takes the properties of the deployment only"
            )
        wanted = list(portion.accounts)
    kind = WINDOW if portion is None else HISTORY
    api = Api(sign_in())
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline, tempfile.TemporaryDirectory(prefix="ga4-") as staging:
        journal = recorded(pipeline) if has_loads(pipeline) else {}
        core = accepted(pipeline, wanted)
        days = None if portion is None else (portion.first, portion.last)
        read: dict[str, Read] = {}
        failed: dict[str, Ga4Error] = {}
        for property_id in wanted:
            last_end = last_day(property_id, core, journal)
            try:
                read[property_id] = read_property(
                    api, property_id, now, last_end, Path(staging), days
                )
            except Ga4Error as error:
                log.error("%s; the load goes on with the other properties", error)
                failed[property_id] = error
        resources = []
        if read:
            rows = [prop.prop for prop in read.values()]
            resources.append(dlt.resource(rows, name="properties", **own(HINTS["properties"])))
            resources.append(
                dlt.resource(
                    or_marker(staged_rows(read)),
                    name="report_sessions_daily",
                    **own(HINTS["report_sessions_daily"]),
                )
            )
        # In the same run as the rows, so under the same _dlt_load_id.
        record = journal_rows(read, failed, now, kind)
        resources.append(dlt.resource(record, name=LOADS, **own(JOURNAL_HINTS)))
        run(pipeline, resources)
        counts = row_counts(pipeline)
    for table, rows in sorted(counts.items()):
        log.info("Loaded %d rows into %s.%s, as of %s", rows, DATASET, table, now.isoformat())
    log.info("%d requests to the GA4 APIs for %d properties", api.requests, len(wanted))
    if failed:
        raise PropertiesFailed(failure_line(failed, len(read)), len(read))
    return counts
