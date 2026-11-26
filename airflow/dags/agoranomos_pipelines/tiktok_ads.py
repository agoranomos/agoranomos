"""tiktok_ads: the TikTok advertisers of the deployment as a dlt source, loaded into raw_tiktok_ads
as the loader role (ADR-0004, ADR-0053).

The source loads the advertisers of the platform tiktok_ads that extensions/model/clients.toml
names, by their numeric ID, and no other, so that a token of a member of an agency, which reaches
the advertisers of its other clients too, brings in nothing that the file does not name
(ADR-0024). It signs in with the long-term token of the person who authorized the app of the
deployment, TIKTOK_ADS_ACCESS_TOKEN, which ./agoranomos tiktok connect writes into .env
(tiktok_api.py) and which never expires, sent in the header Access-Token of each call, never in a
URL. TikTok has no system user: the token carries the rights of that person, so an advertiser that
the person no longer reaches fails with the error of permission. No token reaches a log or an
error: each text that the source writes from an error goes through Secrets.scrub, and the task
masks the value too.

The source calls the TikTok API for Business version API_VERSION (tiktok_api.py) through the HTTP
client of dlt, and keeps each row as the API gives it, which dlt writes in snake_case with nested
objects joined by __: the shape that demo_video mimics, which the macros of
dbt/macros/shapes/tiktok_v1_3.sql read. For each advertiser, in this order, every page of each
answer at a page_size of PAGE_SIZE: the advertiser (advertisers, from /advertiser/info/), the full
lists of its campaigns, ad groups and ads (campaigns, adgroups, ads, from /campaign/get/,
/adgroup/get/ and /ad/get/), filtered on the primary_status STATUS_ALL, which TikTok defines as
"All statuses (including "Deleted")", and on both campaign_system_origins, so that the campaigns
made in the TikTok app come too, with operation_status and secondary_status; then the synchronous
report BASIC by stat_time_day (/report/integrated/get/) at the level AUCTION_ADVERTISER, the
totals of the advertiser by day, against which the transform checks the sum of its campaigns
(G0a of ADR-0035), right before the levels AUCTION_CAMPAIGN, AUCTION_ADGROUP and AUCTION_AD
(report_advertiser_daily, report_campaign_daily, report_adgroup_daily, report_ad_daily), each level
below the advertiser filtered on the status of its level at STATUS_ALL, as each defaults to
STATUS_NOT_DELETE and a filter of another level is refused ("Supported filters" of the basic
reports, as of November 2026). Each report takes one advertiser through advertiser_id, never
advertiser_ids, with which TikTok presents money "without a decimal point", in ranges of at most
RANGE_DAYS, the longest that a report by day allows. Each row of a report gets the advertiser_id of
its request, as the demo has it. A report that TikTok cuts at 20,000 ads, which it says in the
header X-Tt-Ads-Throttle, goes again by groups of at most IDS_PER_FILTER IDs of its level from the
list; a report that times out is not asked again as it was, as TikTok asks, but split, first into
halves of its days, then, below the advertiser, by groups of at most CAMPAIGNS_PER_FILTER
campaigns. No asynchronous report: it "is currently an allowlist-only feature".

The calls keep below the limits of an app at the level Basic, 10 a second and 600 a minute for
each endpoint (Pace); a call throttled with code 40100 or 40133 waits THROTTLE_WAIT, five minutes,
as TikTok says of its limit by minute, and asks again; an error of the server, a 5xx or a code from
50000, waits SERVER_WAIT and asks again, as TikTok advises, up to ATTEMPTS calls in all; and an
answer whose code is not 0 is an error even with HTTP 200 (Api.call).

The days of each advertiser run from the day after the last day that the core accepted for it, or
from W days back when that is earlier, W being the window of restatements of tiktok_ads in the seed
of the platforms (windows.py), 28 days, as TikTok dates a conversion by the day of the click and
allows a click window of up to 28 days (ADR-0053), to yesterday in the time zone of the advertiser,
its field timezone; the first load takes these W days. A run with a portion, which the parameters of
the DAG name, takes the days of the portion of its advertisers as a load of the kind history, in
ranges of at most RANGE_DAYS: a quarter is three or four ranges, so up to sixteen reports for each
advertiser and their pages. The DAG agoranomos_tiktok_ads_history plans the portions after each run
of the transform that succeeded, by quarters from the newest, back to the first day of the month 36
months before yesterday (plan, history_limit, and agoranomos_pipelines/history.py, which the loads
of Google Ads and of Meta share). TikTok keeps report data "forever", so no window has an oldest
day. Each load appends the lists and the facts of each advertiser under its own _dlt_load_id and
records the window of each advertiser in _agoranomos_loads in the same run. The transform after it
pins the loads that have ended and takes them through dbt/models/staging/tiktok_ads/ into the core
and the marts, and the plan of the history waits for the pin of each whole load.

The last day that the core accepted for an advertiser is the end of its newest accepted load in
core.loads, which loader may read; while the core has accepted none, it is the end of its newest
whole load in _agoranomos_loads; with neither, the load takes its usual window. A load keeps the
watermark of each advertiser in the asset state store of Airflow 3.3 as a cache of the warehouse,
which never moves a window (watermarks).

A load reads each advertiser whole before dlt sees any of it, into a file for each table on disk, a
page at a time. An error of the API on one advertiser is caught at the advertiser, which leaves
nothing in the load; the load takes the other advertisers, records the failed one in
_agoranomos_loads with the status failed and the text of the error, and fails at the end with one
line that lists the advertisers and their errors (ADR-0004, ADR-0035), which Airflow retries; on
the last try, the task of the load succeeds when it took another advertiser, so that the transform
takes it, and the run fails after it (outcome.py). What the API returns is data: a campaign name
can carry any text, and nothing here acts on it."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import TextIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client, RequestException, Timeout

from agoranomos_pipelines.history import Portion, history_start, planned, portion_from
from agoranomos_pipelines.loads import (
    APPEND,
    DLT_SETTINGS,
    FAILED,
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
from agoranomos_pipelines.settings.files import load_settings
from agoranomos_pipelines.tiktok_api import (
    ADVERTISER_ID,
    API_VERSION,
    BASE,
    SUNSET,
    TOKEN,
    TOKEN_FORM,
    WARN_DAYS,
)
from agoranomos_pipelines.windows import restatement_window_days

log = logging.getLogger(__name__)

CODE = "tiktok_ads"
PIPELINE = "agoranomos_tiktok_ads"
DATASET = "raw_tiktok_ads"
# The folder of the settings of the deployment, which compose.yaml mounts read-only.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
# The rows of a page, the most that each list and report takes: "Value range: 1-1,000".
PAGE_SIZE = 1000
# The days of a report by stat_time_day: "the query time range cannot exceed 30 days".
RANGE_DAYS = 30
# The IDs of a filter campaign_ids, adgroup_ids or ad_ids: "you can pass in up to 100 IDs", and
# at most 30 campaign IDs in a report at the level of ads against a timeout (Best practices to
# reduce timeouts in synchronous reports, v1.3, as of November 2026).
IDS_PER_FILTER = 100
CAMPAIGNS_PER_FILTER = 30
# The header with which TikTok says that it cut a report at 20,000 ads.
CUT = "X-Tt-Ads-Throttle"
# The pace of the calls of each endpoint, below the 10 a second and 600 a minute of an app at the
# level Basic, "All apps are set to Basic level by default" (Rate limits, v1.3, as of November
# 2026), which leaves a margin for a person who tries the app by hand at the same time.
PER_SECOND = 8
PER_MINUTE = 480
# The retries of a call are explicit, as the contract of the sources asks (ADR-0004), and are made
# here, not by dlt: TikTok asks that a request that timed out not be sent again as it was. The
# client raises no error on a status, as TikTok writes its errors into the body. A call is asked up
# to ATTEMPTS times in all: after a throttling, code 40100 of the app or 40133 of an advertiser,
# THROTTLE_WAIT, as "For QPM limit, you need to wait 5 minutes"; after an error of the server, a
# 5xx or a code from 50000, SERVER_WAIT, as TikTok says "Wait for about 5 minutes and retry"; and
# after a broken connection, CONNECTION_WAIT.
CLIENT = {
    "request_timeout": 120,
    "request_max_attempts": 1,
    "request_backoff_factor": 2,
    "raise_for_status": False,
}
ATTEMPTS = 4
THROTTLED = frozenset({40100, 40133})
THROTTLE_WAIT = 300
SERVER_WAIT = 300
CONNECTION_WAIT = 60

# The statuses that every list and report asks for, and the origins of the campaigns, as each list
# defaults to TT_ADS_PLATFORM and leaves out the Promote campaigns of the TikTok app.
STATUS_ALL = "STATUS_ALL"
ORIGINS = ("PROMOTE", "TT_ADS_PLATFORM")

# The fields of each list: those of the shape of demo_video, so that the same macros read both,
# with secondary_status, where a deletion shows (ADR-0053, "The common state").
FIELDS = {
    "advertisers": ("advertiser_id", "name", "currency", "timezone", "display_timezone", "status"),
    "campaigns": (
        "advertiser_id",
        "campaign_id",
        "campaign_name",
        "objective_type",
        "operation_status",
        "secondary_status",
    ),
    "adgroups": (
        "advertiser_id",
        "campaign_id",
        "adgroup_id",
        "adgroup_name",
        "optimization_goal",
        "operation_status",
        "secondary_status",
    ),
    "ads": (
        "advertiser_id",
        "campaign_id",
        "adgroup_id",
        "ad_id",
        "ad_name",
        "ad_format",
        "operation_status",
        "secondary_status",
    ),
}
# The path of each list.
EDGES = {"campaigns": "campaign/get/", "adgroups": "adgroup/get/", "ads": "ad/get/"}


@dataclass(frozen=True)
class Level:
    """A level of the report: its data_level, its ID dimension, the status filter of the level,
    the filter of its IDs and the list that holds them, and its metrics."""

    data_level: str
    dimension: str
    status: str | None
    ids: str | None
    entities: str | None
    metrics: tuple[str, ...]


# The metrics of every level: the currency, as the demo has it, the money, the impressions and the
# clicks of the destination; below the advertiser, the conversions of the optimization event and the
# complete payments with their value, and the names and the IDs of the levels above, which the
# macros read.
TOTALS = ("currency", "spend", "impressions", "clicks")
CONVERSIONS = ("conversion", "complete_payment", "total_complete_payment_rate")
LEVELS = {
    "report_advertiser_daily": Level(
        "AUCTION_ADVERTISER", "advertiser_id", None, None, None, TOTALS
    ),
    "report_campaign_daily": Level(
        "AUCTION_CAMPAIGN",
        "campaign_id",
        "campaign_status",
        "campaign_ids",
        "campaigns",
        ("campaign_name", *TOTALS, *CONVERSIONS),
    ),
    "report_adgroup_daily": Level(
        "AUCTION_ADGROUP",
        "adgroup_id",
        "adgroup_status",
        "adgroup_ids",
        "adgroups",
        ("campaign_id", "adgroup_name", *TOTALS, *CONVERSIONS),
    ),
    "report_ad_daily": Level(
        "AUCTION_AD",
        "ad_id",
        "ad_status",
        "ad_ids",
        "ads",
        ("campaign_id", "adgroup_id", "ad_name", *TOTALS, *CONVERSIONS),
    ),
}
# The lists and the facts of each advertiser, in the order of their requests: the totals of the
# advertiser right before the report of its campaigns, which G0a checks against them.
ENTITIES = tuple(EDGES)
FACTS = tuple(LEVELS)


def report_columns(level: Level) -> dict[str, dict]:
    columns = {
        "advertiser_id": TEXT,
        f"dimensions__{level.dimension}": TEXT,
        "dimensions__stat_time_day": TEXT,
    }
    return columns | {f"metrics__{name}": TEXT for name in level.metrics}


# The hints of each table: every field, so that dlt makes each column even when no row of a load
# has it, as text, numbers and money as the API writes them, which is as strings, "-" for one that
# means nothing, never the type decimal of dlt (ADR-0038). Every table appends, the lists as the
# facts (ADR-0038).
HINTS = {
    **{
        table: {"write_disposition": APPEND, "columns": dict.fromkeys(fields, TEXT)}
        for table, fields in FIELDS.items()
    },
    **{
        table: {"write_disposition": APPEND, "columns": report_columns(level)}
        for table, level in LEVELS.items()
    },
}


class TikTokAdsError(RuntimeError):
    """A mistake in the settings of the source, which stops the load before any request, or an
    answer of the API that stops the load of one advertiser: one line that names the advertiser
    and gives the error of the API, with no secret."""


class Slow(TikTokAdsError):
    """A request that timed out: a report goes again split, never as it was."""


class AdvertisersFailed(TikTokAdsError):
    """The load took the other advertisers, if any, and these failed: one line that counts them and
    lists each advertiser with the error of the API (ADR-0004), and loaded, how many advertisers the
    load took. Airflow retries the task, as after any other error, and the retry loads every
    advertiser again; on the last try, the task of the load succeeds when it took one, so that the
    transform takes them, and the last task of the DAG fails the run with the line, and with none
    the task fails (outcome.py). The next load takes the days that the failed advertisers missed."""

    def __init__(self, line: str, loaded: int) -> None:
        super().__init__(line)
        self.loaded = loaded


@dataclass
class Secrets:
    """The secrets of the calls, which no text that the source writes may show."""

    values: set[str] = field(default_factory=set)

    def add(self, value: str | None) -> None:
        if value:
            self.values.add(value)

    def scrub(self, text: str) -> str:
        for value in sorted(self.values, key=len, reverse=True):
            text = text.replace(value, "***")
        return text


def advertisers(extensions: Path = EXTENSIONS) -> list[str]:
    """The IDs of the TikTok advertisers that clients.toml of the deployment names, checked."""
    found = [
        entry["account_id"]
        for entry in load_settings(extensions, None).account_clients
        if entry["platform"] == CODE
    ]
    if not found:
        raise TikTokAdsError(
            "extensions/model/clients.toml names no account of the platform tiktok_ads"
        )
    wrong = [advertiser_id for advertiser_id in found if not ADVERTISER_ID.fullmatch(advertiser_id)]
    if wrong:
        raise TikTokAdsError(
            "clients.toml: an account_id of tiktok_ads is the digits of the advertiser ID, such as "
            f"7012345678901234567, not {wrong[0]!r}"
        )
    return sorted(found)


def token() -> str:
    """The token of .env, checked for its form only. No message shows it."""
    value = os.environ.get(TOKEN, "").strip()
    if not value:
        raise TikTokAdsError(
            f"{TOKEN} in .env is empty: ./agoranomos tiktok connect puts the token there"
        )
    if not TOKEN_FORM.fullmatch(value):
        raise TikTokAdsError(
            f"{TOKEN} in .env is not one word without spaces, quotes, $, # or backslashes"
        )
    return value


def yesterday(now: datetime, time_zone: str) -> date:
    """The last day that a load at now takes of an advertiser: yesterday in its time zone."""
    return now.astimezone(ZoneInfo(time_zone)).date() - timedelta(days=1)


def window(last: date, last_end: date | None) -> tuple[date, date]:
    """The days of a load of an advertiser whose last day is last, as ADR-0038 gives them,
    [min(last + 1 - W, last_end + 1), last], where W is the window of restatements of TikTok in
    the seed of the platforms (windows.py), read now, and last_end the last day that the core
    accepted for the advertiser: the last W days when none is known yet, else the days from the
    day after it, or from the first of the last W days when that is earlier."""
    usual = last - timedelta(days=restatement_window_days(CODE) - 1)
    if last_end is None:
        return usual, last
    return min(last_end + timedelta(days=1), usual), last


def ranges(first: date, last: date, days: int = RANGE_DAYS) -> list[tuple[date, date]]:
    """The days from first to last in ranges of at most days, from the oldest."""
    parts, start = [], first
    while start <= last:
        end = min(start + timedelta(days=days - 1), last)
        parts.append((start, end))
        start = end + timedelta(days=1)
    return parts


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[n : n + size] for n in range(0, len(values), size)]


def checked(advertiser_ids: Iterable[str]) -> list[str]:
    """The IDs of the advertisers of a query, sorted: at least one, each digits. They go into SQL
    as they are, so nothing else may."""
    found = sorted(advertiser_ids)
    if not found or not all(ADVERTISER_ID.fullmatch(item) for item in found):
        raise ValueError("a query of tiktok_ads takes one advertiser ID of digits or more")
    return found


def accepted(pipeline, advertiser_ids: Iterable[str]) -> dict[str, date]:
    """The last day that the core accepted for each advertiser: the end of the window of its newest
    accepted load in core.loads, which loader may read (ADR-0038)."""
    # IDs of checked digits, and constants of this module.
    wanted = ", ".join(f"'{advertiser_id}'" for advertiser_id in checked(advertiser_ids))
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                "SELECT account_id, max(window_to) FROM core.loads "  # noqa: S608
                f"WHERE source = '{CODE}' AND status = 'accepted' AND outcome = '{LOADED}' "
                f"AND account_id IN ({wanted}) GROUP BY account_id"
            )
    except DatabaseUndefinedRelation:
        return {}
    return {advertiser_id: end for advertiser_id, end in rows or () if end is not None}


def last_day(
    advertiser_id: str, core: dict[str, date], journal: dict[str, tuple[date, date]]
) -> date | None:
    """The day after which the window of an advertiser starts: the last that the core accepted
    for it, or, while the core has accepted no load of it, the last day of its newest whole load
    in the journal; none when neither knows the advertiser."""
    if advertiser_id in core:
        return core[advertiser_id]
    if advertiser_id in journal:
        return journal[advertiser_id][1]
    return None


# The key of the watermarks of the advertisers in the asset state store of the asset of the load.
WATERMARKS = "watermarks"


def watermarks(cache) -> dict[str, date]:
    """The watermark of each advertiser that the asset state store holds, cache: a cache of the
    warehouse, as a JSON object of advertiser IDs and ISO dates. None without a cache."""
    found = cache.get(WATERMARKS, {}) if cache is not None else {}
    marks = {}
    for advertiser_id, value in found.items() if isinstance(found, dict) else ():
        try:
            marks[advertiser_id] = date.fromisoformat(value)
        except (TypeError, ValueError):
            log.warning("The watermark of TikTok advertiser %s is not a date", advertiser_id)
    return marks


def check_watermark(
    advertiser_id: str,
    cached: dict[str, date],
    core: dict[str, date],
    journal: dict[str, tuple[date, date]],
) -> None:
    """A warning when the watermark of the cache of an advertiser differs from the last day of its
    newest load that the warehouse knows, which the window follows."""
    known = [
        day for day in (core.get(advertiser_id), journal.get(advertiser_id, (None, None))[1]) if day
    ]
    held = max(known) if known else None
    if advertiser_id in cached and cached[advertiser_id] != held:
        log.warning(
            "TikTok advertiser %s: Airflow keeps %s as the last day of its newest load, the "
            "warehouse %s; the window follows the warehouse",
            advertiser_id,
            cached[advertiser_id].isoformat(),
            held.isoformat() if held is not None else "none",
        )


def remember(cache, cached: dict[str, date], read: dict) -> None:
    """Keep in the cache the watermark of each advertiser that the load took, and those of the
    others as they were, once the load is whole."""
    if cache is None or not read:
        return
    marks = {**cached, **{advertiser_id: item.window[1] for advertiser_id, item in read.items()}}
    cache.set(
        WATERMARKS, {advertiser_id: day.isoformat() for advertiser_id, day in sorted(marks.items())}
    )


def portion_of(
    advertiser_ids: Iterable[str] | None, first: str | None, last: str | None
) -> Portion | None:
    """The portion that the parameters of a run name: advertisers, as digits each, or none for
    every advertiser, and the first and last day, as ISO dates; none when the run names no days,
    as a scheduled run does, which takes the usual window of each advertiser."""
    return portion_from(
        advertiser_ids,
        first,
        last,
        form=ADVERTISER_ID,
        shape="the digits of the advertiser ID",
        error=TikTokAdsError,
    )


def history_limit(now: datetime) -> date:
    """The first day of the history of the advertisers at now: the first day of the month
    HISTORY_MONTHS before yesterday in UTC (ADR-0034). TikTok keeps report data "forever", "there
    is no query lookback range restriction" (Reporting FAQs, v1.3, as of November 2026), so no
    oldest day of the API comes later (ADR-0053)."""
    return history_start(now.astimezone(UTC).date() - timedelta(days=1))


def plan(now: datetime, extensions: Path = EXTENSIONS) -> Portion | None:
    """The next portion of the history of the advertisers of the deployment as of now, back to
    history_limit, or none: when every advertiser holds its history, when no advertiser has had
    its usual window yet, or while a whole load of tiktok_ads waits for the run of the transform
    that pins it, so that each portion goes from raw to the core before the next one starts
    (ADR-0038, agoranomos_pipelines/history.py). It reads the warehouse as loader, and needs no
    token."""
    wanted = checked(advertisers(extensions))
    with open_pipeline() as pipeline:
        return planned(pipeline, CODE, "TikTok", wanted, history_limit(now))


class Pace:
    """The calls of each endpoint in the last minute, so that the next one waits while it would be
    the PER_SECOND-th in a second or the PER_MINUTE-th in a minute."""

    def __init__(self, per_second: int = PER_SECOND, per_minute: int = PER_MINUTE):
        self.per_second, self.per_minute = per_second, per_minute
        self.calls: dict[str, deque[float]] = {}

    def wait(self, endpoint: str) -> None:
        calls = self.calls.setdefault(endpoint, deque())
        now = monotonic()
        while calls and now - calls[0] >= 60:
            calls.popleft()
        waits = []
        if len(calls) >= self.per_minute:
            waits.append(calls[-self.per_minute] + 60 - now)
        if len(calls) >= self.per_second:
            waits.append(calls[-self.per_second] + 1 - now)
        wait = max(waits, default=0.0)
        if wait > 0:
            sleep(wait)
        calls.append(monotonic())


@dataclass(frozen=True)
class Answer:
    """The body of an answer whose code is 0, and its headers."""

    body: dict
    headers: dict


class Api:
    """The TikTok API for Business, with the token of the deployment, a pace under the limits of
    the app and explicit retries."""

    def __init__(self, secrets: Secrets, access_token: str):
        self.secrets = secrets
        self.secrets.add(access_token)
        self.headers = {"Access-Token": access_token}
        self.client = Client(**CLIENT)
        self.pace = Pace()
        self.requests = 0
        # Whether an answer since the last reset carried the header of a cut at 20,000 ads.
        self.cut = False

    def fail(self, subject: str, text: str, kind: type[TikTokAdsError] = TikTokAdsError):
        return kind(self.secrets.scrub(f"{subject}: {text}"))

    def wait(self, subject: str, said: str, seconds: int, attempt: int) -> None:
        log.warning(
            "%s; waiting %d s and asking again, attempt %d of %d",
            self.secrets.scrub(f"{subject}: {said}"),
            seconds,
            attempt + 1,
            ATTEMPTS,
        )
        sleep(seconds)

    def call(self, subject: str, path: str, params: dict[str, str]) -> Answer:
        """The answer of GET path with params, whose code is 0, after the waits of the pace, of
        throttling and of the errors of the server. A timeout raises Slow at once, and any other
        error TikTokAdsError with the error of the API and no secret."""
        for attempt in range(1, ATTEMPTS + 1):
            self.pace.wait(path)
            self.requests += 1
            try:
                response = self.client.get(f"{BASE}/{path}", params=params, headers=self.headers)
            except Timeout as error:
                raise self.fail(subject, f"{path} timed out: {error}", Slow) from None
            except RequestException as error:
                if attempt < ATTEMPTS:
                    self.wait(subject, f"{path} failed: {error}", CONNECTION_WAIT, attempt)
                    continue
                raise self.fail(subject, f"{path} failed: {error}") from None
            headers = dict(response.headers or {})
            if response.status_code >= 500:
                said = f"{path} answered {response.status_code}"
                if attempt < ATTEMPTS:
                    self.wait(subject, said, SERVER_WAIT, attempt)
                    continue
                raise self.fail(subject, said)
            try:
                body = response.json()
            except ValueError:
                raise self.fail(
                    subject, f"{path} answered {response.status_code} with no JSON"
                ) from None
            if not isinstance(body, dict):
                raise self.fail(subject, f"{path} answered with something that is not an object")
            code = body.get("code")
            if code == 0 and response.status_code < 400:
                if CUT in headers:
                    self.cut = True
                return Answer(body, headers)
            message = str(body.get("message", ""))[:500]
            said = (
                f"{path} answered {response.status_code} {message} (code {code}, request "
                f"{body.get('request_id')})"
            )
            server = isinstance(code, int) and 50000 <= code < 60000
            if (code in THROTTLED or server) and attempt < ATTEMPTS:
                self.wait(
                    subject, said, THROTTLE_WAIT if code in THROTTLED else SERVER_WAIT, attempt
                )
                continue
            raise self.fail(subject, said)
        raise AssertionError("unreachable")  # pragma: no cover

    def pages(self, subject: str, path: str, params: dict[str, str]) -> Iterator[dict]:
        """The rows of every page of an answer, data.list, from page 1 to data.page_info.total_page
        (the responses of the lists and of the report, v1.3, as of November 2026)."""
        page = 1
        while True:
            answer = self.call(
                subject, path, {**params, "page": str(page), "page_size": str(PAGE_SIZE)}
            )
            data = answer.body.get("data")
            if not isinstance(data, dict):
                raise self.fail(subject, f"{path} answered without data")
            rows = data.get("list", [])
            if not isinstance(rows, list):
                raise self.fail(subject, f"{path} answered with a list that is not a list")
            for row in rows:
                if not isinstance(row, dict):
                    raise self.fail(subject, f"a row of {path} is not an object")
                yield row
            info = data.get("page_info")
            if info is not None and not isinstance(info, dict):
                raise self.fail(subject, f"{path} answered with a page_info that is not an object")
            total = (info or {}).get("total_page")
            if not isinstance(total, int) or page >= total:
                return
            page += 1


def write_rows(rows: Iterable[dict], file: TextIO) -> None:
    for row in rows:
        file.write(json.dumps(row) + "\n")


def report_params(
    advertiser_id: str,
    level: Level,
    days: tuple[date, date],
    ids: tuple[str, list[str]] | None = None,
) -> dict[str, str]:
    """The parameters of the synchronous report of a level of an advertiser over days, a row a
    day: report_type BASIC, the data_level and the ID dimension of the level with stat_time_day,
    its metrics, the days in the time zone of the advertiser, and the filter of its status at
    STATUS_ALL, and of some IDs, as JSON, as the reference of /report/integrated/get/ writes them
    (Run a synchronous report, v1.3, as of November 2026)."""
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": level.data_level,
        "dimensions": json.dumps([level.dimension, "stat_time_day"]),
        "metrics": json.dumps(list(level.metrics)),
        "start_date": days[0].isoformat(),
        "end_date": days[1].isoformat(),
    }
    filtering = []
    if level.status is not None:
        filtering.append(
            {
                "field_name": level.status,
                "filter_type": "IN",
                "filter_value": json.dumps([STATUS_ALL]),
            }
        )
    if ids is not None:
        filtering.append(
            {"field_name": ids[0], "filter_type": "IN", "filter_value": json.dumps(ids[1])}
        )
    if filtering:
        params["filtering"] = json.dumps(filtering)
    return params


def list_ids(path: Path, name: str) -> list[str]:
    """The IDs of a list that a load wrote, sorted, without repeats."""
    found = set()
    with path.open(encoding="utf-8") as file:
        for line in file:
            value = json.loads(line).get(name)
            if value is not None:
                found.add(str(value))
    return sorted(found)


def written_report(
    api: Api,
    advertiser_id: str,
    table: str,
    days: tuple[date, date],
    file: TextIO,
    lists: dict[str, Path],
    ids: tuple[str, list[str]] | None = None,
) -> None:
    """Write the report of a level of an advertiser over days, at most RANGE_DAYS, to file, each
    row with the advertiser_id of the request. A report that TikTok cuts at 20,000 ads goes again
    by groups of IDS_PER_FILTER IDs of its level from its list; one that times out goes again
    split, after the file is cut back to where its rows began: by halves of its days, and, for one
    day below the advertiser, by groups of CAMPAIGNS_PER_FILTER campaigns. A day that still times
    out fails the advertiser."""
    subject = f"TikTok advertiser {advertiser_id}"
    level = LEVELS[table]
    start = file.tell()
    api.cut = False
    try:
        for row in api.pages(
            subject, "report/integrated/get/", report_params(advertiser_id, level, days, ids)
        ):
            file.write(json.dumps({"advertiser_id": advertiser_id, **row}) + "\n")
    except Slow as error:
        file.seek(start)
        file.truncate()
        if days[1] > days[0]:
            middle = days[0] + (days[1] - days[0]) // 2
            log.warning("%s; asking again by halves of its days", error)
            for part in ((days[0], middle), (middle + timedelta(days=1), days[1])):
                written_report(api, advertiser_id, table, part, file, lists, ids)
            return
        if level.ids is None or ids is not None:
            raise
        campaigns = list_ids(lists["campaigns"], "campaign_id")
        if not campaigns:
            raise
        log.warning("%s; asking again by groups of %d campaigns", error, CAMPAIGNS_PER_FILTER)
        for group in chunks(campaigns, CAMPAIGNS_PER_FILTER):
            written_report(api, advertiser_id, table, days, file, lists, ("campaign_ids", group))
        return
    if api.cut and ids is None and level.ids is not None:
        file.seek(start)
        file.truncate()
        found = list_ids(lists[level.entities], level.dimension)
        log.warning(
            "%s: TikTok cut the report of %s at 20,000 ads; asking again by groups of %d of its %d "
            "IDs",
            subject,
            level.data_level,
            IDS_PER_FILTER,
            len(found),
        )
        for group in chunks(found, IDS_PER_FILTER):
            written_report(api, advertiser_id, table, days, file, lists, (level.ids, group))


@dataclass
class Read:
    """Everything that a load read of one advertiser, before dlt sees any of it: its row of
    advertisers, the window of its facts, and a file for each table of its lists and facts."""

    advertiser: dict
    window: tuple[date, date]
    files: dict[str, Path]


def advertiser_info(api: Api, advertiser_id: str) -> dict:
    """The advertiser, from /advertiser/info/ with its fields (Get ad account details, v1.3, as of
    November 2026), asked for each advertiser apart, so that one that the token does not reach
    fails alone (ADR-0035)."""
    subject = f"TikTok advertiser {advertiser_id}"
    answer = api.call(
        subject,
        "advertiser/info/",
        {
            "advertiser_ids": json.dumps([advertiser_id]),
            "fields": json.dumps(list(FIELDS["advertisers"])),
        },
    )
    data = answer.body.get("data")
    rows = data.get("list") if isinstance(data, dict) else None
    found = [
        row
        for row in rows or ()
        if isinstance(row, dict) and str(row.get("advertiser_id")) == advertiser_id
    ]
    if len(found) != 1:
        raise api.fail(subject, "the API returned another advertiser or none")
    return found[0]


def read_advertiser(
    api: Api,
    advertiser_id: str,
    now: datetime,
    last_end: date | None,
    staging: Path,
    days: tuple[date, date] | None = None,
) -> Read:
    """Read an advertiser whole: the advertiser, whose time zone gives its window, then its lists
    and the reports of its window, each table into a file in the folder of the advertiser under
    staging. With days, a portion of the history, the window is those days, up to yesterday of the
    advertiser; the lists come too, as staging reads the lists of the newest load of an
    advertiser. An error raises TikTokAdsError and leaves nothing of the advertiser: a window
    replaces whole days, and a part of one would lose the rest (ADR-0035)."""
    subject = f"TikTok advertiser {advertiser_id}"
    advertiser = advertiser_info(api, advertiser_id)
    try:
        last_day_of_advertiser = yesterday(now, str(advertiser.get("timezone")))
    except (ZoneInfoNotFoundError, ValueError):
        raise api.fail(subject, "the advertiser has no time zone that agoranomos knows") from None
    if days is None:
        first, last = window(last_day_of_advertiser, last_end)
    else:
        first, last = days[0], min(days[1], last_day_of_advertiser)
        if first > last:
            raise api.fail(
                subject,
                f"the days of the history start on {first.isoformat()}, after its yesterday, "
                f"{last_day_of_advertiser.isoformat()}",
            )
    # The ID is checked digits.
    folder = staging / advertiser_id
    folder.mkdir()
    files: dict[str, Path] = {}
    try:
        for table in ENTITIES:
            path = folder / f"{table}.jsonl"
            params = {
                "advertiser_id": advertiser_id,
                "fields": json.dumps(list(FIELDS[table])),
                "filtering": json.dumps(
                    {"primary_status": STATUS_ALL, "campaign_system_origins": list(ORIGINS)}
                ),
            }
            with path.open("w", encoding="utf-8") as file:
                for row in api.pages(subject, EDGES[table], params):
                    file.write(json.dumps({"advertiser_id": advertiser_id, **row}) + "\n")
            files[table] = path
        for table in FACTS:
            path = folder / f"{table}.jsonl"
            with path.open("w", encoding="utf-8") as file:
                for part in ranges(first, last):
                    written_report(api, advertiser_id, table, part, file, files)
            files[table] = path
    except TikTokAdsError:
        shutil.rmtree(folder, ignore_errors=True)
        raise
    return Read(advertiser, (first, last), files)


def staged_rows(read: dict[str, Read], table: str) -> Iterator[dict]:
    """The rows of a table of each advertiser read, from its file, as the API gave them."""
    for item in read.values():
        with item.files[table].open(encoding="utf-8") as file:
            for line in file:
                yield json.loads(line)


def failure_line(failed: dict[str, TikTokAdsError], loaded: int) -> str:
    """The line with which the task fails after a load with failed advertisers (ADR-0004)."""
    if loaded:
        rest = f"the other {loaded} {'is' if loaded == 1 else 'are'} loaded"
    else:
        rest = "none is loaded"
    errors = "; ".join(str(error) for error in failed.values())
    return f"{len(failed)} of {len(failed) + loaded} TikTok advertisers failed, {rest}: {errors}"


@contextmanager
def open_pipeline() -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of TikTok, as the loader role, in a temporary working folder: dlt keeps the
    state of each pipeline in its dataset."""
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        yield dlt.pipeline(
            pipeline_name=PIPELINE,
            pipelines_dir=workdir,
            destination=destination(),
            dataset_name=DATASET,
        )


def warn_of_the_sunset(now: datetime) -> None:
    """A warning in the log from WARN_DAYS before the sunset of the version of the API."""
    if now.date() >= SUNSET - timedelta(days=WARN_DAYS):
        log.warning(
            "TikTok may sunset the reports of its API %s around %s, an estimate while TikTok "
            "gives no date: update agoranomos before then, or the loads of tiktok_ads stop",
            API_VERSION,
            SUNSET.strftime("%B %Y"),
        )


def load(
    now: datetime,
    extensions: Path = EXTENSIONS,
    cache=None,
    portion: Portion | None = None,
    mask: Callable[[str], None] = lambda value: None,
) -> dict[str, int]:
    """Load the advertisers of the deployment as of now, each read whole before dlt sees any of
    it, record each advertiser in _agoranomos_loads, loaded with its window or failed with its
    error, and return the rows of each table that dlt reports. cache is the asset state store of
    the asset of the load, or None; mask hides the token in the logs of Airflow. With a portion,
    the load takes the days of the portion of its advertisers, or of every advertiser, with the
    kind history, and leaves the watermarks as they were. When an advertiser failed, the load
    takes the others and raises AdvertisersFailed at the end; when every advertiser failed, it
    writes the journal alone."""
    warn_of_the_sunset(now)
    named = advertisers(extensions)
    wanted = named
    if portion is not None and portion.accounts:
        unnamed = sorted(set(portion.accounts) - set(named))
        if unnamed:
            raise TikTokAdsError(
                "extensions/model/clients.toml names no account of tiktok_ads "
                + ", ".join(unnamed)
                + ": a run of the history takes the advertisers of the deployment only"
            )
        wanted = list(portion.accounts)
    kind = WINDOW if portion is None else HISTORY
    access_token = token()
    mask(access_token)
    api = Api(Secrets(), access_token)
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline, tempfile.TemporaryDirectory(prefix="tiktok-ads-") as staging:
        journal = recorded(pipeline) if has_loads(pipeline) else {}
        core = accepted(pipeline, wanted)
        cached = watermarks(cache)
        days = None if portion is None else (portion.first, portion.last)
        read: dict[str, Read] = {}
        failed: dict[str, TikTokAdsError] = {}
        for advertiser_id in wanted:
            if portion is None:
                check_watermark(advertiser_id, cached, core, journal)
            last_end = last_day(advertiser_id, core, journal)
            try:
                read[advertiser_id] = read_advertiser(
                    api, advertiser_id, now, last_end, Path(staging), days
                )
            except TikTokAdsError as error:
                log.error("%s; the load goes on with the other advertisers", error)
                failed[advertiser_id] = error
        resources = []
        if read:
            rows = [item.advertiser for item in read.values()]
            resources.append(dlt.resource(rows, name="advertisers", **own(HINTS["advertisers"])))
            for table in ENTITIES:
                resources.append(
                    dlt.resource(staged_rows(read, table), name=table, **own(HINTS[table]))
                )
            for table in FACTS:
                resources.append(
                    dlt.resource(
                        or_marker(staged_rows(read, table)), name=table, **own(HINTS[table])
                    )
                )
        loaded_at = datetime.now(UTC)
        record = [
            {
                "account_id": advertiser_id,
                "as_of": yesterday(now, str(item.advertiser["timezone"])) + timedelta(days=1),
                "window_from": item.window[0],
                "window_to": item.window[1],
                "now": now,
                "loaded_at": loaded_at,
                "status": LOADED,
                "error": None,
                "kind": kind,
            }
            for advertiser_id, item in read.items()
        ] + [
            {
                "account_id": advertiser_id,
                "as_of": None,
                "window_from": None,
                "window_to": None,
                "now": now,
                "loaded_at": None,
                "status": FAILED,
                "error": str(error),
                "kind": kind,
            }
            for advertiser_id, error in failed.items()
        ]
        record.sort(key=lambda row: row["account_id"])
        # In the same run as the facts, so under the same _dlt_load_id.
        resources.append(dlt.resource(record, name=LOADS, **own(LOAD_HINTS)))
        run(pipeline, resources)
        counts = row_counts(pipeline)
        if portion is None:
            remember(cache, cached, read)
    for table, rows in sorted(counts.items()):
        log.info("Loaded %d rows into %s.%s, as of %s", rows, DATASET, table, now.isoformat())
    log.info("%d calls to the TikTok API for %d advertisers", api.requests, len(wanted))
    if failed:
        raise AdvertisersFailed(failure_line(failed, len(read)), len(read))
    return counts
