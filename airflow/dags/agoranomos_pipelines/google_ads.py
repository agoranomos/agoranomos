"""google_ads: the Google Ads accounts of the deployment as a dlt source, loaded into raw_google_ads
as the loader role (ADR-0004, ADR-0032).

The source loads the accounts of the platform google_ads that extensions/model/clients.toml names,
and no other, so that a manager account of an agency brings in no account of another business
(ADR-0024). It signs in as a service account of the Google Cloud project of the deployment, whose
key is GOOGLE_ADS_SERVICE_ACCOUNT_KEY, as JSON or as base64 of the JSON, and names the manager
account in GOOGLE_ADS_LOGIN_CUSTOMER_ID when the service account reaches the accounts through one.
It asks googleAds:searchStream of the REST interface of the Google Ads API, version API_VERSION, for
each account, and keeps each result as the API gives it, which dlt writes in snake_case with nested
objects joined by __: the shape that demo_search mimics, which the macros of
dbt/macros/shapes/google_ads_v25.sql read (none of their fields changed in v26).

A load first reads each account (customer), then its campaigns, its ad groups, its ads and its
conversion actions, whose versions dlt keeps (scd2), and then the statistics and the conversions by
conversion action of each of the three levels, by day, in a request of their own, as
segments.conversion_action cannot be selected with metrics.cost_micros (ADR-0034). A Performance
Max campaign has no ad groups. A conversion action carries include_in_conversions_metric, whether
Google counts it in metrics.conversions, which the model takes for whether the action is primary
where extensions/model/conversion_actions.toml does not name it, and primary_for_goal, which the
guide of conversion goals of Google names in its place, so that a real account can show whether
the two differ. The days of each account run from the day after the last that a load took, or from
90 days back when that is earlier, as conversions keep coming for up to 90 days after a click; the
first load of an account takes the last 36 months, as Google Ads keeps data by day for 37 only
(ADR-0034), a calendar month in each request. All end yesterday in the time zone of the account. A
request counts as one operation of the daily quota, whatever its rows, so a daily load costs eleven
operations for each account, and the first about 230. The days of each account are replaced, empty
or not, and the load records the window of each account in _agoranomos_reloads in the same run.

A load reads each account whole before dlt sees any of it, into a file for each table on disk, and
reads each answer of the API as it arrives, a row at a time, so that a task holds a row and a piece
of an answer and not a month of rows (ADR-0038). An error of the API on one account, such as the
403 of an account that the service account may not read, or an answer that is not whole JSON,
is caught at the account, which leaves nothing in the load, as a window replaces whole days and a
part of one would lose the rest; an answer whose connection breaks after it has started is asked
again first, up to three times in all. The load takes the other accounts, records the failed
account in _agoranomos_reloads with the status failed and the text of the error, keeps its row of
customers as the last load saw it, and fails at the end with one line that lists the accounts and
their errors (ADR-0004, ADR-0035). What the API returns is data: a campaign name can carry any
text, and nothing here acts on it."""

from __future__ import annotations

import base64
import binascii
import codecs
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import TextIO
from zoneinfo import ZoneInfo

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client, HTTPError, RequestException
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from agoranomos_pipelines.loads import (
    DATE,
    DLT_SETTINGS,
    FAILED,
    FILE_FORMAT,
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
    row_counts,
)
from agoranomos_pipelines.settings.files import load_settings

log = logging.getLogger(__name__)

CODE = "google_ads"
PIPELINE = "agoranomos_google_ads"
DATASET = "raw_google_ads"
# The latest major version as of October 2026, which Google is to sunset in November 2027
# (tentatively, https://developers.google.com/google-ads/api/docs/sunset-dates). A unit test fails
# from 90 days before SUNSET, and each load warns from then on (ADR-0032).
API_VERSION = "v26"
SUNSET = date(2027, 11, 1)
WARN_DAYS = 90
URL = "https://googleads.googleapis.com/{version}/customers/{account}/googleAds:searchStream"
SCOPE = "https://www.googleapis.com/auth/adwords"
# The folder of the settings of the deployment, which compose.yaml mounts read-only.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
KEY = "GOOGLE_ADS_SERVICE_ACCOUNT_KEY"
LOGIN = "GOOGLE_ADS_LOGIN_CUSTOMER_ID"
# A customer ID of Google Ads: ten digits, without the hyphens that the UI shows. The ID goes into
# the URL of each request and the SQL of each scope, so nothing else may.
ACCOUNT_ID = re.compile(r"\d{10}")
WINDOW_DAYS = 90
HISTORY_MONTHS = 36
# A window longer than this goes in a request for each calendar month.
ONE_REQUEST_DAYS = 92
# The retries of a request are explicit, as the contract of the sources asks (ADR-0004): five
# tries over about a minute on a timeout, a broken connection, 429 or a 5xx, after the time that a
# Retry-After asks for.
CLIENT = {"request_timeout": 120, "request_max_attempts": 5, "request_backoff_factor": 2}
# dlt asks a request again until its answer starts. An answer whose connection breaks after that,
# in the middle of its rows, read_account asks again itself, once it has cut the file of its table
# back to where the answer began, so that no row comes twice: up to ATTEMPTS requests in all, after
# a wait that doubles from the backoff factor of CLIENT, 2 and then 4 seconds, as dlt waits (dlt
# 1.32.0, wait_exponential of tenacity 9.1.4).
ATTEMPTS = 3
# An answer of searchStream over REST is a JSON array of batches, each a
# SearchGoogleAdsStreamResponse: its rows in results, with fieldMask and requestId, and
# queryResourceConsumption in the last batch (Search and SearchStream in the REST guide,
# https://developers.google.com/google-ads/api/rest/common/search, and the reference of
# SearchGoogleAdsStreamResponse of v26, read on 3 November 2026). Google gives no size of a batch,
# so a load reads an answer as it arrives, CHUNK bytes at a time, and parses it a row at a time.
# A value of an answer longer than LONGEST characters, which no row comes near, is taken for a
# broken answer, so that not even a broken one makes a task hold the rest of it.
CHUNK = 64 * 1024
LONGEST = 8 * 1024 * 1024
# The reader of an answer is the json module of the standard library, which parses one value at
# a time, and these patterns, which find where a value ends when a piece cuts it: white space; a
# string; a number, true, false or null, up to the next delimiter; and the text up to the next
# bracket outside a string, which is characters other than quotes and brackets, and whole
# strings. Each quantifier is possessive, so that a scan never steps back.
DECODER = json.JSONDecoder()
SPACE = re.compile(r"[ \t\n\r]*+")
STRING = re.compile(r'"(?:[^"\\]++|\\.)*+"', re.DOTALL)
SCALAR = re.compile(r'[^ \t\n\r,:\[\]{}"]*+')
PLAIN = re.compile(r'(?:[^"\[\]{}]++|"(?:[^"\\]++|\\.)*+")*+', re.DOTALL)

# The queries of each table, in GAQL. The fields are those of the shape of demo_search, so that
# the same macros read both.
FIELDS = {
    "customers": (
        "customer.resource_name, customer.id, customer.descriptive_name, customer.currency_code, "
        "customer.time_zone, customer.manager, customer.status"
    ),
    "campaigns": (
        "customer.id, campaign.resource_name, campaign.id, campaign.name, campaign.status, "
        "campaign.advertising_channel_type"
    ),
    "campaign_stats": (
        "customer.id, campaign.resource_name, campaign.id, campaign.name, metrics.impressions, "
        "metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value, "
        "segments.date"
    ),
    "campaign_conversions": (
        "customer.id, campaign.resource_name, campaign.id, campaign.name, segments.date, "
        "segments.conversion_action, segments.conversion_action_name, "
        "segments.conversion_action_category, metrics.conversions, metrics.conversions_value, "
        "metrics.all_conversions, metrics.all_conversions_value"
    ),
    "ad_groups": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group.name, ad_group.status, ad_group.type"
    ),
    "ad_group_stats": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group.name, metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, segments.date"
    ),
    "ad_group_conversions": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group.name, segments.date, segments.conversion_action, "
        "segments.conversion_action_name, segments.conversion_action_category, "
        "metrics.conversions, metrics.conversions_value, metrics.all_conversions, "
        "metrics.all_conversions_value"
    ),
    "ads": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group_ad.resource_name, ad_group_ad.status, ad_group_ad.ad.resource_name, "
        "ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type"
    ),
    "ad_stats": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group_ad.resource_name, ad_group_ad.ad.resource_name, ad_group_ad.ad.id, "
        "ad_group_ad.ad.name, metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, segments.date"
    ),
    "ad_conversions": (
        "customer.id, campaign.resource_name, campaign.id, ad_group.resource_name, ad_group.id, "
        "ad_group_ad.resource_name, ad_group_ad.ad.resource_name, ad_group_ad.ad.id, "
        "ad_group_ad.ad.name, segments.date, segments.conversion_action, "
        "segments.conversion_action_name, segments.conversion_action_category, "
        "metrics.conversions, metrics.conversions_value, metrics.all_conversions, "
        "metrics.all_conversions_value"
    ),
    "conversion_actions": (
        "customer.id, conversion_action.resource_name, conversion_action.id, "
        "conversion_action.name, conversion_action.status, conversion_action.category, "
        "conversion_action.include_in_conversions_metric, conversion_action.primary_for_goal"
    ),
}
RESOURCES = {
    "customers": "customer",
    "campaigns": "campaign",
    "campaign_stats": "campaign",
    "campaign_conversions": "campaign",
    "ad_groups": "ad_group",
    "ad_group_stats": "ad_group",
    "ad_group_conversions": "ad_group",
    "ads": "ad_group_ad",
    "ad_stats": "ad_group_ad",
    "ad_conversions": "ad_group_ad",
    "conversion_actions": "conversion_action",
}
# The tables whose versions dlt keeps, and the fact tables, whose days each load replaces.
ENTITIES = ("campaigns", "ad_groups", "ads", "conversion_actions")
FACTS = (
    "campaign_stats",
    "campaign_conversions",
    "ad_group_stats",
    "ad_group_conversions",
    "ad_stats",
    "ad_conversions",
)
DOUBLE = {"data_type": "double"}
BOOL = {"data_type": "bool"}
SCD2 = {"disposition": "merge", "strategy": "scd2"}
# The columns of a field of each query: the names that dlt gives the fields, with their types.
COLUMNS = {
    "metrics.conversions": DOUBLE,
    "metrics.all_conversions": DOUBLE,
    "segments.date": DATE,
    "customer.manager": BOOL,
    "conversion_action.include_in_conversions_metric": BOOL,
    "conversion_action.primary_for_goal": BOOL,
}


def columns(table: str) -> dict[str, dict]:
    """The column of each field of the query of a table, as dlt names it: text, unless COLUMNS
    gives it another type."""
    return {
        field.replace(".", "__"): COLUMNS.get(field, TEXT)
        for field in (part.strip() for part in FIELDS[table].split(","))
    }


# The hints of each table, by the names that dlt gives the fields: every field of its query, so
# that dlt makes each column even when no row of a load has it, with the INT64 numbers as text, as
# the REST interface writes them, the money as text too, and the other numbers as the API gives
# them.
HINTS = {
    "customers": {"write_disposition": "replace", "columns": columns("customers")},
    **{table: {"write_disposition": SCD2, "columns": columns(table)} for table in ENTITIES},
    **{table: {"columns": columns(table)} for table in FACTS},
}


class GoogleAdsError(RuntimeError):
    """A mistake in the settings of the source, which stops the load before any request, or an
    answer of the API that stops the load of one account: one line that names the account and
    gives the error of the API."""


class AnswerBrokeOff(GoogleAdsError):
    """An answer whose connection broke after it had started, as requests reports it while the
    rows come: the one error that read_account asks again, as the next answer may well come whole.
    An answer that the API refuses, which dlt has asked again already, an error in an answer and an
    answer that is not whole JSON fail the account at once."""


class AccountsFailed(GoogleAdsError):
    """The load took the other accounts, and these failed: one line that counts them and lists each
    account with the error of the API, as the contract of the sources asks (ADR-0004). A retry of
    the task would repeat the whole load and gain nothing: the next load takes the days that the
    failed accounts missed."""


def accounts(extensions: Path = EXTENSIONS) -> list[str]:
    """The IDs of the Google Ads accounts that clients.toml of the deployment names, checked."""
    found = [
        entry["account_id"]
        for entry in load_settings(extensions, None).account_clients
        if entry["platform"] == CODE
    ]
    if not found:
        raise GoogleAdsError(
            "extensions/model/clients.toml names no account of the platform google_ads"
        )
    wrong = [account_id for account_id in found if not ACCOUNT_ID.fullmatch(account_id)]
    if wrong:
        raise GoogleAdsError(
            "clients.toml: an account_id of google_ads is ten digits without hyphens, "
            f"such as 1234567890, not {wrong[0]!r}"
        )
    return sorted(found)


def login_customer_id() -> str | None:
    """The manager account through which the service account reaches the accounts, if any."""
    value = os.environ.get(LOGIN, "").strip()
    if value and not ACCOUNT_ID.fullmatch(value):
        raise GoogleAdsError(f"{LOGIN} in .env is ten digits without hyphens, or empty")
    return value or None


def key_info() -> dict:
    """The key of the service account, from GOOGLE_ADS_SERVICE_ACCOUNT_KEY: the JSON file that
    Google Cloud makes, as it is or as base64. A message never shows the key."""
    value = os.environ.get(KEY, "").strip()
    if not value:
        raise GoogleAdsError(f"{KEY} in .env is empty: put the key of the service account there")
    if not value.startswith("{"):
        try:
            value = base64.b64decode(value, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            raise GoogleAdsError(f"{KEY} in .env is neither JSON nor base64 of JSON") from None
    try:
        info = json.loads(value)
    except json.JSONDecodeError:
        raise GoogleAdsError(f"{KEY} in .env is not the JSON of a key") from None
    if not isinstance(info, dict) or info.get("type") != "service_account":
        raise GoogleAdsError(f"{KEY} in .env is not the key of a service account")
    return info


def sign_in() -> service_account.Credentials:
    """The credentials of the service account, with an access token for the Google Ads API."""
    signed = service_account.Credentials.from_service_account_info(key_info(), scopes=[SCOPE])
    signed.refresh(Request())
    return signed


def yesterday(now: datetime, time_zone: str) -> date:
    """The last day that a load at now takes of an account: yesterday in its time zone."""
    return now.astimezone(ZoneInfo(time_zone)).date() - timedelta(days=1)


def history_start(last: date) -> date:
    """The first day of the first load of an account whose last day is last: the first day of
    the month HISTORY_MONTHS before it."""
    months = last.year * 12 + last.month - 1 - HISTORY_MONTHS
    return date(months // 12, months % 12 + 1, 1)


def window(last: date, last_end: date | None) -> tuple[date, date]:
    """The days of a load of an account whose last day is last: the history when no load has
    taken a day of it yet, else the days from the day after the last loaded, or from the first of
    the last WINDOW_DAYS when that is earlier."""
    if last_end is None:
        return history_start(last), last
    return min(last_end + timedelta(days=1), last - timedelta(days=WINDOW_DAYS - 1)), last


def chunks(first: date, last: date) -> list[tuple[date, date]]:
    """The days from first to last in one request, or in one for each calendar month when they
    are more than ONE_REQUEST_DAYS."""
    if (last - first).days < ONE_REQUEST_DAYS:
        return [(first, last)]
    parts, start = [], first
    while start <= last:
        following = date(start.year + start.month // 12, start.month % 12 + 1, 1)
        end = min(following - timedelta(days=1), last)
        parts.append((start, end))
        start = end + timedelta(days=1)
    return parts


def query(table: str, days: tuple[date, date] | None = None) -> str:
    """The GAQL of a table, over the days of a fact table."""
    # GAQL of the fields and resources above and of dates, never text from outside.
    text = f"SELECT {FIELDS[table]} FROM {RESOURCES[table]}"  # noqa: S608
    if days is not None:
        first, last = days
        text += f" WHERE segments.date BETWEEN '{first.isoformat()}' AND '{last.isoformat()}'"
    return text


class BrokenAnswer(ValueError):
    """An answer of searchStream that is not a whole JSON array of batches: cut short, not JSON,
    or with more after its end."""


class ErrorInAnswer(Exception):
    """An element of an answer of searchStream with an error, as Google writes an error over HTTP
    and JSON, {"error": {"code": ..., "message": ..., "status": ..., "details": [...]}} (AIP-193).
    Google does not document how an error that comes after the first rows shows in an answer over
    REST, so the reader stops on such an element wherever it comes."""


class Answer:
    """An answer of searchStream as it arrives, in pieces of text, read a row at a time. The text
    holds what is left of the last piece from at, where the value being read starts."""

    def __init__(self, pieces: Iterable[str]):
        self.pieces = iter(pieces)
        self.text, self.at = "", 0

    def more(self) -> None:
        """Take the next piece of the answer after what is left unread."""
        piece = next(self.pieces, None)
        if piece is None:
            raise BrokenAnswer("the answer ends before its array does")
        self.text, self.at = self.text[self.at :] + piece, 0
        if len(self.text) > LONGEST:
            raise BrokenAnswer(f"a value of the answer is longer than {LONGEST:,} characters")

    def char(self) -> str:
        """The next character of the answer that is not white space, left unread."""
        while True:
            self.at = SPACE.match(self.text, self.at).end()
            if self.at < len(self.text):
                return self.text[self.at]
            self.more()

    def take(self, wanted: str) -> str:
        """Read the next character that is not white space, which must be one of wanted."""
        char = self.char()
        if char not in wanted:
            raise BrokenAnswer(f"the answer has {char!r} where JSON has one of {' '.join(wanted)}")
        self.at += 1
        return char

    def until(self, pattern: re.Pattern) -> int:
        """The end of what pattern matches at the next character, once a character follows it."""
        while True:
            found = pattern.match(self.text, self.at)
            if found and found.end() < len(self.text):
                return found.end()
            self.more()

    def bracketed(self) -> int:
        """The end of the object or array that starts at the next character: the bracket that
        closes it, counted outside strings."""
        depth, offset = 0, 0
        while True:
            end = PLAIN.match(self.text, self.at + offset).end()
            offset = end - self.at
            if end == len(self.text) or self.text[end] == '"':
                # The piece ends, or ends a string short: read on from there.
                self.more()
                continue
            depth += 1 if self.text[end] in "[{" else -1
            offset += 1
            if depth == 0:
                return self.at + offset

    def value(self) -> object:
        """Read the next JSON value of the answer. An object or an array that the piece holds
        whole is parsed where it lies, and one that the end of the piece cuts, or that is not
        JSON, is found by its brackets first."""
        char = self.char()
        if char in "[{":
            try:
                found, self.at = DECODER.raw_decode(self.text, self.at)
            except json.JSONDecodeError:
                pass
            else:
                return found
        end = self.bracketed() if char in "[{" else self.until(STRING if char == '"' else SCALAR)
        text, self.at = self.text[self.at : end], end
        try:
            return json.loads(text)
        except json.JSONDecodeError as error:
            raise BrokenAnswer(f"the answer is not JSON: {error.msg}") from None

    def rows(self) -> Iterator[dict]:
        """The rows of each batch of the answer as they come, and then nothing to its end but
        white space."""
        self.take("[")
        if self.char() == "]":
            self.at += 1
        else:
            while True:
                yield from self.batch()
                if self.take(",]") == "]":
                    break
        while True:
            self.at = SPACE.match(self.text, self.at).end()
            if self.at < len(self.text):
                raise BrokenAnswer("the answer goes on after its array")
            piece = next(self.pieces, None)
            if piece is None:
                return
            self.text, self.at = piece, 0

    def batch(self) -> Iterator[dict]:
        """The rows of the batch that starts at the next character: an object whose results are
        the rows, and whose other members, such as fieldMask and requestId, are passed over."""
        self.take("{")
        if self.char() == "}":
            self.at += 1
            return
        while True:
            name = self.value()
            if not isinstance(name, str):
                raise BrokenAnswer("the answer has a name of a member that is not a string")
            self.take(":")
            if name == "results":
                yield from self.results()
            elif name == "error":
                raise ErrorInAnswer(json.dumps({"error": self.value()}))
            else:
                self.value()
            if self.take(",}") == "}":
                return

    def results(self) -> Iterator[dict]:
        """The rows of the array of results that starts at the next character."""
        self.take("[")
        if self.char() == "]":
            self.at += 1
            return
        while True:
            row = self.value()
            if not isinstance(row, dict):
                raise BrokenAnswer("a row of the answer is not an object")
            yield row
            if self.take(",]") == "]":
                return


def pieces(response) -> Iterator[str]:
    """The text of an answer as it arrives, CHUNK bytes at a time, as UTF-8, which JSON between
    systems is (RFC 8259)."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    for chunk in response.iter_content(CHUNK):
        yield decoder.decode(chunk)
    yield decoder.decode(b"", final=True)


class Api:
    """The REST interface of the Google Ads API, as the service account, with explicit retries."""

    def __init__(self, signed: service_account.Credentials, login: str | None):
        self.signed, self.login = signed, login
        self.client = Client(**CLIENT)
        self.requests = 0

    def search(self, account_id: str, gaql: str) -> Iterator[dict]:
        """The rows of a query of an account, as the API gives them, one at a time as the answer
        arrives. dlt retries a request until its answer starts. An answer that the API refuses, an
        error in the middle of one, and an answer that is not whole JSON raise GoogleAdsError; a
        connection that breaks in an answer raises AnswerBrokeOff, which read_account asks again.
        Each call is a request, and counts as an operation of the daily quota."""
        if not self.signed.valid:
            self.signed.refresh(Request())
        headers = {"Authorization": f"Bearer {self.signed.token}"}
        if self.login:
            headers["login-customer-id"] = self.login
        self.requests += 1
        try:
            response = self.client.post(
                URL.format(version=API_VERSION, account=account_id),
                json={"query": gaql},
                headers=headers,
                stream=True,
            )
        except HTTPError as error:
            # The original error of the API, as the contract of the sources asks (ADR-0004): its
            # status and the start of its body, which names the error and the request.
            answer = error.response
            status, body = (
                (answer.status_code, answer.text[:2000]) if answer is not None else ("", "")
            )
            raise GoogleAdsError(f"Google Ads account {account_id}: {status} {body}") from None
        try:
            yield from Answer(pieces(response)).rows()
        except ErrorInAnswer as error:
            raise GoogleAdsError(
                f"Google Ads account {account_id}: an error in the answer: {str(error)[:2000]}"
            ) from None
        except (BrokenAnswer, UnicodeDecodeError) as error:
            raise GoogleAdsError(
                f"Google Ads account {account_id}: a broken answer: {error}"
            ) from None
        except RequestException as error:
            raise AnswerBrokeOff(
                f"Google Ads account {account_id}: the answer broke off: {error}"
            ) from None
        finally:
            response.close()


def checked(account_ids: Iterable[str]) -> list[str]:
    """The IDs of the accounts of a scope, sorted: at least one, each ten digits. A scope goes into
    the SQL of dlt as it is, so nothing else may."""
    found = sorted(account_ids)
    if not found or not all(ACCOUNT_ID.fullmatch(account_id) for account_id in found):
        raise ValueError("a scope of google_ads takes one account ID of ten digits or more")
    return found


def scope(windows: dict[str, tuple[date, date]]) -> str:
    """The rows of a fact table that a load replaces, as the SQL condition of dlt's destination
    scope: those of each account that the load read, in its window. The days are dates."""
    return " OR ".join(
        f"(customer__id = '{account_id}' AND segments__date BETWEEN "
        f"'{windows[account_id][0].isoformat()}' AND '{windows[account_id][1].isoformat()}')"
        for account_id in checked(windows)
    )


def accounts_scope(account_ids: Iterable[str]) -> str:
    """The rows of a table of entities whose versions a load may retire, as the SQL condition of
    dlt's destination scope: those of the accounts that the load read, so that the entities of an
    account whose load failed keep their versions (dlt 1.31.0: the scope bounds what scd2
    retires)."""
    return "customer__id IN (" + ", ".join(f"'{a}'" for a in checked(account_ids)) + ")"


@dataclass
class Read:
    """Everything that a load read of one account, before dlt sees any of it: its row of
    customers, the window of its facts, and the rows of its campaigns, ad groups, ads and
    conversion actions and of its facts, in a file for each table."""

    customer: dict
    window: tuple[date, date]
    files: dict[str, Path]


def retried[T](ask: Callable[[], T], undo: Callable[[], None] = lambda: None) -> T:
    """What ask returns, asked again when its answer breaks off, up to ATTEMPTS times in all, each
    time after undo has taken back what the broken answer gave and after a wait that doubles. The
    error of the last attempt is that of the account (ADR-0004)."""
    for attempt in range(1, ATTEMPTS):
        try:
            return ask()
        except AnswerBrokeOff as error:
            wait = CLIENT["request_backoff_factor"] * 2 ** (attempt - 1)
            log.warning(
                "%s; asking again in %d s, attempt %d of %d", error, wait, attempt + 1, ATTEMPTS
            )
            undo()
            sleep(wait)
    try:
        return ask()
    except AnswerBrokeOff as error:
        raise AnswerBrokeOff(f"{error} (attempt {ATTEMPTS} of {ATTEMPTS})") from None


def written(api: Api, account_id: str, gaql: str, file: TextIO) -> None:
    """Write the rows of a query of an account to file, a line each, as they come. An answer that
    breaks off is asked again, after the file is cut back to where the answer began, so that no
    row comes twice."""
    start = file.tell()

    def write() -> None:
        for row in api.search(account_id, gaql):
            file.write(json.dumps(row) + "\n")

    def undo() -> None:
        file.seek(start)
        file.truncate()

    retried(write, undo)


def read_account(
    api: Api, account_id: str, now: datetime, last_end: date | None, staging: Path
) -> Read:
    """Read an account whole: its customer, which gives its time zone and so its window, then its
    campaigns, ad groups, ads and conversion actions and the facts of its window, each table into a
    file in the folder of the account under staging, on disk, a row at a time as each answer
    arrives, so that the load holds a row of an answer in memory and not the answer (ADR-0038).
    An answer that breaks off is asked again (retried). An error of the API on any request, an
    answer that is not whole JSON, or one that breaks off at each attempt, raises GoogleAdsError
    and leaves nothing of the account: a window replaces whole days, and a part of one would lose
    the rest (ADR-0035)."""
    rows = retried(
        lambda: [
            row
            for row in api.search(account_id, query("customers"))
            if row["customer"]["id"] == account_id
        ]
    )
    if not rows:
        raise GoogleAdsError(f"Google Ads account {account_id}: the API returned no customer")
    customer = rows[0]
    first, last = window(yesterday(now, customer["customer"]["timeZone"]), last_end)
    # The ID is checked digits.
    folder = staging / account_id
    folder.mkdir()
    files = {}
    try:
        for table in (*ENTITIES, *FACTS):
            path = folder / f"{table}.jsonl"
            with path.open("w", encoding="utf-8") as file:
                for days in chunks(first, last) if table in FACTS else [None]:
                    written(api, account_id, query(table, days), file)
            files[table] = path
    except GoogleAdsError:
        shutil.rmtree(folder, ignore_errors=True)
        raise
    return Read(customer, (first, last), files)


def staged_rows(read: dict[str, Read], table: str) -> Iterator[dict]:
    """The rows of a table of each account read, from its file, as the API gave them."""
    for account in read.values():
        with account.files[table].open(encoding="utf-8") as file:
            for line in file:
                yield json.loads(line)


def kept_customers(pipeline, account_ids: list[str]) -> list[dict]:
    """The rows of customers of the accounts that this load could not read, as the last load that
    could left them, so that each load replaces the table with every account that a load has seen:
    an account whose load failed keeps its name, currency and time zone in the model, which its
    facts join. None before the first load. dlt names the column of a key with __ as it names a
    nested path (dlt 1.31.0), so a row goes back as the table holds it."""
    if not account_ids:
        return []
    names = list(columns("customers"))
    # Names of the code, a name that dlt quotes, and IDs of checked digits.
    wanted = ", ".join(f"'{account_id}'" for account_id in checked(account_ids))
    try:
        with pipeline.sql_client() as client:
            table = client.make_qualified_table_name("customers")
            rows = client.execute_sql(
                f"SELECT {', '.join(names)} FROM {table} WHERE customer__id IN ({wanted})"  # noqa: S608
            )
    except DatabaseUndefinedRelation:
        return []
    return [dict(zip(names, row, strict=True)) for row in rows or ()]


def failure_line(failed: dict[str, GoogleAdsError], loaded: int) -> str:
    """The line with which the task fails after a load with failed accounts: how many failed and
    how many loaded, and the original error of each account (ADR-0004)."""
    if loaded:
        rest = f"the other {loaded} {'is' if loaded == 1 else 'are'} loaded"
    else:
        rest = "none is loaded"
    errors = "; ".join(str(error) for error in failed.values())
    return f"{len(failed)} of {len(failed) + loaded} Google Ads accounts failed, {rest}: {errors}"


@contextmanager
def open_pipeline() -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of Google Ads, as the loader role, in a temporary working folder: dlt keeps
    the state of each pipeline in its dataset."""
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        yield dlt.pipeline(
            pipeline_name=PIPELINE,
            pipelines_dir=workdir,
            destination=dlt.destinations.postgres(credentials=credentials()),
            dataset_name=DATASET,
        )


def warn_of_the_sunset(now: datetime) -> None:
    """A warning in the log from WARN_DAYS before the sunset of the version of the API."""
    if now.date() >= SUNSET - timedelta(days=WARN_DAYS):
        log.warning(
            "Google is to sunset the Google Ads API %s in %s: update agoranomos before then, or "
            "the loads of google_ads stop",
            API_VERSION,
            SUNSET.strftime("%B %Y"),
        )


def load(now: datetime, extensions: Path = EXTENSIONS) -> dict[str, int]:
    """Load the accounts of the deployment as of now, each read whole before dlt sees any of it,
    record each account in _agoranomos_reloads, loaded with its window or failed with its error,
    and return the rows of each table that dlt reports. When an account failed, the load takes the
    other accounts and raises AccountsFailed at the end; when every account failed, it writes the
    journal alone."""
    warn_of_the_sunset(now)
    wanted = accounts(extensions)
    api = Api(sign_in(), login_customer_id())
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline, tempfile.TemporaryDirectory(prefix="google-ads-") as staging:
        last = recorded(pipeline) if has_loads(pipeline) else {}
        read: dict[str, Read] = {}
        failed: dict[str, GoogleAdsError] = {}
        for account_id in wanted:
            last_end = last[account_id][1] if account_id in last else None
            try:
                read[account_id] = read_account(api, account_id, now, last_end, Path(staging))
            except GoogleAdsError as error:
                log.error("%s; the load goes on with the other accounts", error)
                failed[account_id] = error
        resources = []
        if read:
            customers = [account.customer for account in read.values()]
            customers += kept_customers(pipeline, sorted(failed))
            resources.append(dlt.resource(customers, name="customers", **own(HINTS["customers"])))
            for table in ENTITIES:
                hints = own(HINTS[table])
                hints["write_disposition"] = {
                    **hints["write_disposition"],
                    "boundary_timestamp": now.isoformat(),
                    "destination_scope": accounts_scope(read),
                }
                resources.append(dlt.resource(staged_rows(read, table), name=table, **hints))
            windows = {account_id: account.window for account_id, account in read.items()}
            for table in FACTS:
                hints = own(HINTS[table])
                hints["write_disposition"] = {**MERGE, "destination_scope": scope(windows)}
                resources.append(
                    dlt.resource(or_marker(staged_rows(read, table)), name=table, **hints)
                )
        loaded_at = datetime.now(UTC)
        record = [
            {
                "account_id": account_id,
                "as_of": account.window[1] + timedelta(days=1),
                "window_start": account.window[0],
                "window_end": account.window[1],
                "now": now,
                "loaded_at": loaded_at,
                "status": LOADED,
                "error": None,
            }
            for account_id, account in read.items()
        ] + [
            # Nothing was loaded: no day and no loaded_at, so that recorded() and ops_loads pass
            # the row over, as they do for an account that no load took; now keeps the attempt.
            {
                "account_id": account_id,
                "as_of": None,
                "window_start": None,
                "window_end": None,
                "now": now,
                "loaded_at": None,
                "status": FAILED,
                "error": str(error),
            }
            for account_id, error in failed.items()
        ]
        record.sort(key=lambda row: row["account_id"])
        # In the same run as the facts, so that a load that fails records no window.
        resources.append(dlt.resource(record, name=RELOADS, **own(RELOAD_HINTS)))
        pipeline.run(resources, loader_file_format=FILE_FORMAT)
        counts = row_counts(pipeline)
    for table, rows in sorted(counts.items()):
        log.info("Loaded %d rows into %s.%s, as of %s", rows, DATASET, table, now.isoformat())
    log.info("%d requests to the Google Ads API for %d accounts", api.requests, len(wanted))
    if failed:
        raise AccountsFailed(failure_line(failed, len(read)))
    return counts
