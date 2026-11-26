"""meta_ads: the Meta ad accounts of the deployment as a dlt source, loaded into raw_meta_ads as
the loader role (ADR-0004, ADR-0050).

The source loads the accounts of the platform meta_ads that extensions/model/clients.toml names,
and no other, so that the accounts that other businesses share with the business portfolio of the
deployment bring in nothing that the file does not name (ADR-0024). An account is its numeric ID,
without the prefix act_ that the paths of the API add. It signs in with the token of a system user
of the portfolio, META_ADS_ACCESS_TOKEN, sent in the header Authorization: Bearer of each call,
never in a URL or a body, but for the refresh below (Api.sign_in). A token that never expires is
all that .env holds. A token that expires in 60 days comes with META_ADS_APP_ID and
META_ADS_APP_SECRET: the load then refreshes it through GET oauth/access_token with
fb_exchange_token before it is 30 days old, and keeps the newest token in the Airflow Variable
VARIABLE, with a hash of the token of .env that it came from, so that a new token in .env replaces
it; a refresh that fails fails the load (current_token). Whenever the secret is set, every other
call carries the parameter appsecret_proof, the HMAC-SHA256 of the token keyed with the secret. No
token, secret or proof reaches a log or an error: each text that the source writes from an error
goes through Secrets.scrub, and the task masks the values too.

The source calls the Graph API version API_VERSION through the HTTP client of dlt, and keeps each
row as the API gives it, which dlt writes in snake_case with nested objects joined by __ and the
lists actions and action_values as the child tables <table>__actions and <table>__action_values,
with the windows of attribution as the columns _1d_click, _7d_click and _1d_view: the shape that
demo_social mimics, which the macros of dbt/macros/shapes/meta_v26.sql read (no field that they
read changed in v27.0). For each account, in this order, every page of each answer through
paging.next: the account (ad_accounts), the full lists of its campaigns, ad sets and ads
(campaigns, adsets, ads), filtered on every value of effective_status but DELETED, which the
lists refuse, so that archived objects come too; then, by day, the totals of the account
from the insights at the level account (insights_account), against which the transform is to check
the sum of its campaigns (G0a of ADR-0035), right before the insights of the campaigns, and the
insights of the levels campaign, adset and ad (insights_campaign, insights_adset, insights_ad),
each filtered on the effective_status of its level over every value (ADR-0036), with actions,
action_values and attribution_setting, and the windows 1d_click, 7d_click and 1d_view. A daily
load asks the insights synchronously; a request that fails with too much data (code 100, subcode
1487534), a timeout, an error of the server, or one of the errors of SYNC_TIMEOUTS, with no
synchronous retry, goes again as a report run (report_run), as each portion of the history does:
POST act_<id>/insights, polled until it completes, and then <report_run_id>/insights page by
page; a report run that has too much data, refused at its POST or failed with Report Too Large,
goes by calendar months, and one that Meta skips or that fails with an error to retry later is
asked again. After each call, the load reads the headers of throttling, X-Ad-Account-Usage,
X-FB-Ads-Insights-Throttle and X-Business-Use-Case-Usage, logs them, and waits when a use passes
PAUSE_AT percent, and an error of throttling waits and asks again (Api.call).

The days of each account run from the day after the last day that the core accepted for it, or
from 28 days back when that is earlier, as insights "do not change after 28 days of being
reported", to yesterday in the time zone of the account (ADR-0038); the first load takes these 28
days. A run with a portion, which the parameters of the DAG name, takes the days of the portion of
its accounts as a load of the kind history, through report runs, for at most the time of a
portion (PORTION_STARTS), after which the accounts that it has not reached wait for the next
portion. The DAG agoranomos_meta_ads_history plans the portions after each run of the transform
that succeeded, by quarters from the newest, back to the first day of the month 36 months before
yesterday, and never before the 37 months that Meta gives (plan, history_limit, and
agoranomos_pipelines/history.py, which the load of Google Ads shares). No window starts before
those 37 months (oldest). Each load appends the lists and the facts of each account under its
own _dlt_load_id and records the window of each account in _agoranomos_loads in the same run. The
transform after it pins the loads that have ended and takes them through
dbt/models/staging/meta_ads/ into the core and the marts; the plan of the history waits for the
pin of each whole load.

The last day that the core accepted for an account is the end of its newest accepted load in
core.loads, which loader may read; while the core has accepted none, it is the end of its newest
whole load in _agoranomos_loads; with neither, the load takes its usual window. A load keeps the
watermark of each account in the asset state store of Airflow 3.3 as a cache of the warehouse,
which never moves a window (watermarks).

A load reads each account whole before dlt sees any of it, into a file for each table on disk, a
page at a time. An error of the API on one account is caught at the account, which leaves nothing
in the load; the load takes the other accounts, records the failed account in _agoranomos_loads
with the status failed and the text of the error, and fails at the end with one line that lists
the accounts and their errors (ADR-0004, ADR-0035). What the API returns is data: a campaign name
can carry any text, and nothing here acts on it."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import TextIO
from urllib.parse import parse_qsl, urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client, RequestException, Timeout

from agoranomos_pipelines.history import (
    Portion,
    history_start,
    months_back,
    planned,
    portion_from,
)
from agoranomos_pipelines.loads import (
    APPEND,
    DATE,
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
from agoranomos_pipelines.windows import restatement_window_days

log = logging.getLogger(__name__)

CODE = "meta_ads"
PIPELINE = "agoranomos_meta_ads"
DATASET = "raw_meta_ads"
# The latest version of the Marketing API as of November 2026, released on 4 November 2026 with
# the expiry "TBD" (https://developers.facebook.com/documentation/ads-commerce/marketing-api/
# marketing-api-changelog/versions, read on 15 November 2026). While Meta gives no date, SUNSET is
# the estimate of ADR-0050: the release plus the shortest life of the last ten versions, 336 days,
# taken to the start of its month; a release replaces it with the date of Meta once Meta publishes
# one. A unit test fails from 90 days before SUNSET, and each load warns from then on (ADR-0032).
API_VERSION = "v27.0"
SUNSET = date(2027, 10, 1)
WARN_DAYS = 90
GRAPH = "graph.facebook.com"
BASE = f"https://{GRAPH}/{API_VERSION}"
# The folder of the settings of the deployment, which compose.yaml mounts read-only.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
TOKEN = "META_ADS_ACCESS_TOKEN"  # noqa: S105 (the name of a variable)
APP_ID = "META_ADS_APP_ID"
APP_SECRET = "META_ADS_APP_SECRET"  # noqa: S105 (the name of a variable)
# The Airflow Variable of the newest token that a refresh gave. Airflow encrypts a Variable with
# the Fernet key of the deployment, and hides the value of one whose key names a token in its UI
# and logs (the sensitive names of Airflow 3.3.2).
VARIABLE = "agoranomos_meta_ads_token"
# A token that expires is valid for 60 days from the day it was made or refreshed (System users,
# https://developers.facebook.com/docs/business-management-apis/system-users/
# install-apps-and-generate-tokens, read on 15 November 2026): the load refreshes it once it is
# this old, so that a refresh that fails leaves 30 days to make a new one.
REFRESH_AFTER = timedelta(days=30)
# An ad account of Meta: a numeric string (the reference of AdAccount, read on 15 November 2026).
# The ID goes into the path of each request and the SQL of the window, so nothing else may.
ACCOUNT_ID = re.compile(r"[0-9]{1,20}")
# A token, an App Secret: one word of printable characters, which .env cannot carry with a space or
# a quote, and an App ID: digits. agoranomos preflight checks the same form, and never the value.
SECRET_FORM = re.compile(r"[!#-&(-;=?-~]+")
APP_ID_FORM = re.compile(r"[0-9]+")
# Meta refuses a start date "beyond 37 months from the current date", error 3018 (the reference of
# the insights of an ad account, read on 15 November 2026; ADR-0050).
OLDEST_MONTHS = 37
# The rows of a page that a request asks for. Meta gives no maximum, and fewer pages are fewer
# calls of the score of the account; the answers of a page are read whole.
LIMIT = 500
# The retries of a request are explicit, as the contract of the sources asks (ADR-0004): dlt asks
# again five times over about a minute on a timeout, a broken connection, 429 or a 5xx, after the
# time that a Retry-After asks for. The client raises no error on a status: Meta writes its errors
# into the body of a 4xx, which Api.call reads.
CLIENT = {
    "request_timeout": 120,
    "request_max_attempts": 5,
    "request_backoff_factor": 2,
    "raise_for_status": False,
}
# The errors of throttling and the temporary errors, which wait and ask again up to
# THROTTLE_ATTEMPTS times in all: 4, 17 and 613, with any subcode, such as 17 with 2446079, 613
# with 1487742 and 4 with 1504022 or 1504039, and 80000, 80003, 80004 and 80014 of the
# business-use-case limits (Rate limiting of the Marketing API, https://developers.facebook.com/
# documentation/ads-commerce/marketing-api/overview/rate-limiting, and of the Graph API,
# https://developers.facebook.com/docs/graph-api/overview/rate-limiting/); and 1 and 2, for which
# the Graph API says to wait and retry (https://developers.facebook.com/docs/graph-api/guides/
# error-handling/), all read on 15 November 2026.
THROTTLED = frozenset({4, 17, 613, 80000, 80003, 80004, 80014})
TEMPORARY = frozenset({1, 2})
THROTTLE_ATTEMPTS = 5
# Too much data per call: code 100 with subcode 1487534, on a synchronous or an asynchronous
# request (Limits and best practices of the Insights API, https://developers.facebook.com/
# documentation/ads-commerce/marketing-api/insights/best-practices, read on 15 November 2026).
TOO_MUCH_DATA = (100, 1487534)
# The use of the account, in percent, past which the load waits: under Limited access an account
# reaches its maximum score at 60 points, a read scoring 1 and the score decaying over 300 seconds,
# and is then blocked for 300 seconds (ADR-0050). The load waits for the reset time that a header
# gives, or THROTTLE_WAIT when none does, and never longer than MAX_WAIT at a time.
PAUSE_AT = 75.0
THROTTLE_WAIT = 60
MAX_WAIT = 900
# A report run is polled after 5 seconds, then after twice the wait before, up to 60 seconds, for
# up to an hour, as "it can take up to an hour to complete a request including retry attempts"; a
# run that Meta skips is asked again, up to REPORT_ATTEMPTS runs in all ("Please resubmit your job
# and try again").
POLL_FIRST, POLL_LONGEST = 5, 60
REPORT_TIMEOUT = 3600
REPORT_ATTEMPTS = 3
COMPLETED, FAILED_JOB, SKIPPED = "Job Completed", "Job Failed", "Job Skipped"
# A report run that fails carries its error since v25.0, "error_code, error_message, error_subcode,
# error_user_title, and error_user_msg fields returned by default" (Limits and best practices of the
# Insights API, read on 15 November 2026). Of the codes of the Ads Insights Error Codes
# (https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/error-codes,
# read on 15 November 2026), -3 with 1504045, "Report Too Large", is too much data per call, like
# 100 with 1487534, and goes by calendar months; -2 with 2490547, "Report Failed", and 2 with
# 1504043, an intermittent error, say to retry later, and are asked again as a skipped run is. Any
# other failure fails the account.
TOO_LARGE = frozenset({TOO_MUCH_DATA, (-3, 1504045)})
RETRY_LATER = frozenset({(-2, 2490547), (2, 1504043)})
# The errors of a synchronous request of insights that go again as a report run at once, with no
# synchronous retry (written_insights). The Ads Insights Error Codes, read on 25 November 2026,
# give two synchronous timeouts, 100 with 1504018 and 2 with 1504038, "Request Timed Out", for
# which they say "Try a smaller date range, fetch less data, or use async jobs."; and 2 with
# 1504044, a synchronous "Unknown Error Occurred", for which they say "Please refresh the page or
# try again. If the issue persists, contact Meta Support." On 25 November 2026 the stand S6 (#220)
# saw 2 with 1504044, HTTP 400, after about 30 seconds, like a timeout, on the synchronous
# insights of four accounts, again on each of the five calls of three of them; on one of these,
# by hand, the level adset failed the same way again, while a report run of the same request
# completed in 7 seconds. Elsewhere code 2 stays a temporary error, and 100 with 1504018 fails the
# account, as any other error does.
SYNC_TIMEOUTS = frozenset({(100, 1504018), (2, 1504038), (2, 1504044)})
# The time of a portion of the history, which one task of agoranomos_meta_ads_daily takes, within
# its execution_timeout of two hours: a portion starts no account PORTION_STARTS seconds after the
# load began, and leaves the accounts that it has not reached to the next portion, which the plan
# gives them; a report run of a portion that has not completed PORTION_ENDS seconds after the load
# began fails its account; the half hour after it is for dlt to write what the load read. A daily
# load has no such time: its 28 days go synchronously.
PORTION_STARTS = 60 * 60
PORTION_ENDS = 90 * 60
# The values of effective_status, every value that the references of the edges and the objects
# list as of November 2026: the edges campaigns and adsets of an ad account, and the field
# effective_status of an ad, list twelve; the field of a campaign six, and that of an ad set seven,
# which the filter of each level of insights takes. The lists take the twelve but DELETED: the
# stand S6 showed in November 2026 that the edges campaigns, adsets and ads of an account refuse an
# effective_status that holds DELETED, alone or with others (code 100, subcode 1815001, "Cannot
# request deleted objects"), while the other eleven pass and archived objects come. The insights
# take DELETED, so the statistics of a deleted object still come, and a deleted object reaches the
# core only through them, as a stub of ADR-0036 named from its statistics.
EVERY_STATUS = (
    "ACTIVE",
    "PAUSED",
    "DELETED",
    "PENDING_REVIEW",
    "DISAPPROVED",
    "PREAPPROVED",
    "PENDING_BILLING_INFO",
    "CAMPAIGN_PAUSED",
    "ARCHIVED",
    "ADSET_PAUSED",
    "IN_PROCESS",
    "WITH_ISSUES",
)
LIST_STATUSES = tuple(status for status in EVERY_STATUS if status != "DELETED")
LEVEL_STATUSES = {
    "campaign": ("ACTIVE", "PAUSED", "DELETED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"),
    "adset": (
        "ACTIVE",
        "PAUSED",
        "DELETED",
        "CAMPAIGN_PAUSED",
        "ARCHIVED",
        "IN_PROCESS",
        "WITH_ISSUES",
    ),
    "ad": EVERY_STATUS,
}
# The windows of attribution of the actions, as the demo shape has them; 7d_view and 28d_view
# return no data since 12 January 2026 (ADR-0050). No use_unified_attribution_setting and no
# action_report_time: Meta disregards both since 10 June 2025.
WINDOWS = ("1d_click", "7d_click", "1d_view")

# The fields of each table: those of the shape of demo_social, so that the same macros read both,
# with the statuses of ADR-0036 on the lists. A level of insights is that of its table. The
# account has no business: that field needs the permission business_management, which a token of
# ads_read lacks, and Meta refuses the whole request with error 100, as the stand S6 showed in
# November 2026.
STATS = ("date_start", "date_stop", "impressions", "clicks", "inline_link_clicks", "spend")
ACTIONS = ("attribution_setting", "actions", "action_values")
ACCOUNT = ("account_id", "account_name", "account_currency")
FIELDS = {
    "ad_accounts": (
        "id",
        "account_id",
        "name",
        "currency",
        "timezone_name",
        "account_status",
    ),
    "campaigns": (
        "id",
        "account_id",
        "name",
        "status",
        "configured_status",
        "effective_status",
        "objective",
    ),
    "adsets": (
        "id",
        "account_id",
        "campaign_id",
        "name",
        "status",
        "configured_status",
        "effective_status",
        "optimization_goal",
    ),
    "ads": (
        "id",
        "account_id",
        "campaign_id",
        "adset_id",
        "name",
        "status",
        "configured_status",
        "effective_status",
    ),
    "insights_account": (
        "account_id",
        "date_start",
        "date_stop",
        "impressions",
        "inline_link_clicks",
        "spend",
    ),
    "insights_campaign": (*ACCOUNT, "campaign_id", "campaign_name", *STATS, *ACTIONS),
    "insights_adset": (*ACCOUNT, "campaign_id", "adset_id", "adset_name", *STATS, *ACTIONS),
    "insights_ad": (*ACCOUNT, "campaign_id", "adset_id", "ad_id", "ad_name", *STATS, *ACTIONS),
}
# The edge of each list, and the level of each table of insights.
EDGES = {"campaigns": "campaigns", "adsets": "adsets", "ads": "ads"}
LEVELS = {
    "insights_account": "account",
    "insights_campaign": "campaign",
    "insights_adset": "adset",
    "insights_ad": "ad",
}
# The lists and the facts of each account, in the order of their requests: the totals of the
# account right before the insights of its campaigns, which G0a is to check against them.
ENTITIES = tuple(EDGES)
FACTS = tuple(LEVELS)


def columns(table: str) -> dict[str, dict]:
    """The columns of the fields of a table, as dlt names them: text, the dates as dates, and none
    for the lists of actions, which become child tables of text."""
    found: dict[str, dict] = {}
    for name in FIELDS[table]:
        if name in ("actions", "action_values"):
            continue
        found[name] = DATE if name in ("date_start", "date_stop") else TEXT
    return found


# The hints of each table: every field, so that dlt makes each column even when no row of a load
# has it, as text, numbers and money as the API writes them, which is as strings, never the type
# decimal of dlt (ADR-0038). Every table appends, the lists as the facts (ADR-0038).
HINTS = {
    table: {"write_disposition": APPEND, "columns": columns(table)}
    for table in ("ad_accounts", *ENTITIES, *FACTS)
}


class MetaAdsError(RuntimeError):
    """A mistake in the settings of the source, which stops the load before any request, or an
    answer of the API that stops the load of one account: one line that names the account and
    gives the error of the API, with no secret."""


class Slow(MetaAdsError):
    """A request that timed out, or that the server failed, after the retries of dlt, or a
    synchronous request of insights that Meta answered with an error of SYNC_TIMEOUTS: a
    synchronous request of insights goes again as a report run."""


class TooMuchData(Slow):
    """Too much data per call, code 100 with subcode 1487534: a synchronous request goes again as
    a report run, and a report run by calendar months."""


class AccountsFailed(MetaAdsError):
    """The load took the other accounts, and these failed: one line that counts them and lists each
    account with the error of the API (ADR-0004). A retry of the task would repeat the whole load
    and gain nothing: the next load takes the days that the failed accounts missed."""


@dataclass
class Secrets:
    """The secrets of the calls, which no text that the source writes may show."""

    values: set[str] = field(default_factory=set)

    # A parameter of a URL that carries a secret, whatever its value.
    PARAMETER = re.compile(
        r"(access_token|appsecret_proof|client_secret|fb_exchange_token)=[^&\s]*"
    )
    # The credentials of a header Authorization, whatever the token.
    BEARER = re.compile(r"(Bearer )[^\s'\",}]+")

    def add(self, value: str | None) -> None:
        if value:
            self.values.add(value)

    def scrub(self, text: str) -> str:
        """The text with each parameter of a URL that carries a secret, the credentials of each
        header Authorization, and then each secret, as ***: the parameters and the header first, so
        that a secret inside the name of one cannot hide it."""
        text = self.PARAMETER.sub(r"\1=***", text)
        text = self.BEARER.sub(r"\1***", text)
        for value in sorted(self.values, key=len, reverse=True):
            text = text.replace(value, "***")
        return text


def accounts(extensions: Path = EXTENSIONS) -> list[str]:
    """The IDs of the Meta ad accounts that clients.toml of the deployment names, checked."""
    found = [
        entry["account_id"]
        for entry in load_settings(extensions, None).account_clients
        if entry["platform"] == CODE
    ]
    if not found:
        raise MetaAdsError(
            "extensions/model/clients.toml names no account of the platform meta_ads"
        )
    wrong = [account_id for account_id in found if not ACCOUNT_ID.fullmatch(account_id)]
    if wrong:
        raise MetaAdsError(
            "clients.toml: an account_id of meta_ads is the digits of the ad account without act_, "
            f"such as 1234567890123456, not {wrong[0]!r}"
        )
    return sorted(found)


@dataclass(frozen=True)
class Settings:
    """The token of .env, and the App ID and App Secret of a token that expires, or none."""

    token: str
    app_id: str | None
    app_secret: str | None

    @property
    def expiring(self) -> bool:
        return self.app_secret is not None


def settings() -> Settings:
    """The settings of the sign-in from the environment, checked for their form only. No message
    shows a value."""
    token = os.environ.get(TOKEN, "").strip()
    app_id = os.environ.get(APP_ID, "").strip() or None
    secret = os.environ.get(APP_SECRET, "").strip() or None
    if not token:
        raise MetaAdsError(f"{TOKEN} in .env is empty: put the token of the system user there")
    if not SECRET_FORM.fullmatch(token):
        raise MetaAdsError(f"{TOKEN} in .env is not one word without spaces or quotes")
    if (app_id is None) != (secret is None):
        raise MetaAdsError(
            f"{APP_ID} and {APP_SECRET} in .env go together, for a token that expires, or both "
            "stay empty"
        )
    if app_id is not None and not APP_ID_FORM.fullmatch(app_id):
        raise MetaAdsError(f"{APP_ID} in .env is the digits of the App ID")
    if secret is not None and not SECRET_FORM.fullmatch(secret):
        raise MetaAdsError(f"{APP_SECRET} in .env is not one word without spaces or quotes")
    return Settings(token, app_id, secret)


def proof(token: str, secret: str) -> str:
    """appsecret_proof: "a sha256 hash of your access token, using your app secret as the key", as
    hash_hmac('sha256', $access_token, $app_secret) of PHP gives it, in hexadecimal (Securing
    requests, https://developers.facebook.com/docs/graph-api/securing-requests/, read on 15
    November 2026)."""
    return hmac.new(secret.encode(), token.encode(), hashlib.sha256).hexdigest()


def fingerprint(token: str) -> str:
    """The SHA-256 of a token, by which the Variable knows the token of .env that it came from."""
    return hashlib.sha256(token.encode()).hexdigest()


def yesterday(now: datetime, time_zone: str) -> date:
    """The last day that a load at now takes of an account: yesterday in its time zone."""
    return now.astimezone(ZoneInfo(time_zone)).date() - timedelta(days=1)


def window(last: date, last_end: date | None) -> tuple[date, date]:
    """The days of a load of an account whose last day is last, as ADR-0038 gives them,
    [min(last + 1 - W, last_end + 1), last], where W is the window of restatements of Meta in the
    seed of the platforms (windows.py), read now, and last_end the last day that the core accepted
    for the account: the last W days when none is known yet, else the days from the day after it,
    or from the first of the last W days when that is earlier."""
    usual = last - timedelta(days=restatement_window_days(CODE) - 1)
    if last_end is None:
        return usual, last
    return min(last_end + timedelta(days=1), usual), last


def months(first: date, last: date) -> list[tuple[date, date]]:
    """The days from first to last, by calendar month."""
    parts, start = [], first
    while start <= last:
        following = date(start.year + start.month // 12, start.month % 12 + 1, 1)
        end = min(following - timedelta(days=1), last)
        parts.append((start, end))
        start = end + timedelta(days=1)
    return parts


def checked(account_ids: Iterable[str]) -> list[str]:
    """The IDs of the accounts of a query, sorted: at least one, each digits. They go into SQL as
    they are, so nothing else may."""
    found = sorted(account_ids)
    if not found or not all(ACCOUNT_ID.fullmatch(account_id) for account_id in found):
        raise ValueError("a query of meta_ads takes one account ID of digits or more")
    return found


def accepted(pipeline, account_ids: Iterable[str]) -> dict[str, date]:
    """The last day that the core accepted for each account: the end of the window of its newest
    accepted load in core.loads, which loader may read (ADR-0038)."""
    # IDs of checked digits, and constants of this module.
    wanted = ", ".join(f"'{account_id}'" for account_id in checked(account_ids))
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                "SELECT account_id, max(window_to) FROM core.loads "  # noqa: S608
                f"WHERE source = '{CODE}' AND status = 'accepted' AND outcome = '{LOADED}' "
                f"AND account_id IN ({wanted}) GROUP BY account_id"
            )
    except DatabaseUndefinedRelation:
        return {}
    return {account_id: end for account_id, end in rows or () if end is not None}


def last_day(
    account_id: str, core: dict[str, date], journal: dict[str, tuple[date, date]]
) -> date | None:
    """The day after which the window of an account starts: the last that the core accepted for
    it, or, while the core has accepted no load of it, the last day of its newest whole load in
    the journal; none when neither knows the account."""
    if account_id in core:
        return core[account_id]
    if account_id in journal:
        return journal[account_id][1]
    return None


# The key of the watermarks of the accounts in the asset state store of the asset of the load.
WATERMARKS = "watermarks"


def watermarks(cache) -> dict[str, date]:
    """The watermark of each account that the asset state store holds, cache: a cache of the
    warehouse, as a JSON object of account IDs and ISO dates. None without a cache."""
    found = cache.get(WATERMARKS, {}) if cache is not None else {}
    marks = {}
    for account_id, value in found.items() if isinstance(found, dict) else ():
        try:
            marks[account_id] = date.fromisoformat(value)
        except (TypeError, ValueError):
            log.warning("The watermark of Meta ad account %s is not a date", account_id)
    return marks


def check_watermark(
    account_id: str,
    cached: dict[str, date],
    core: dict[str, date],
    journal: dict[str, tuple[date, date]],
) -> None:
    """A warning when the watermark of the cache of an account differs from the last day of its
    newest load that the warehouse knows, which the window follows."""
    known = [day for day in (core.get(account_id), journal.get(account_id, (None, None))[1]) if day]
    held = max(known) if known else None
    if account_id in cached and cached[account_id] != held:
        log.warning(
            "Meta ad account %s: Airflow keeps %s as the last day of its newest load, the "
            "warehouse %s; the window follows the warehouse",
            account_id,
            cached[account_id].isoformat(),
            held.isoformat() if held is not None else "none",
        )


def remember(cache, cached: dict[str, date], read: dict) -> None:
    """Keep in the cache the watermark of each account that the load took, and those of the others
    as they were, once the load is whole."""
    if cache is None or not read:
        return
    marks = {**cached, **{account_id: account.window[1] for account_id, account in read.items()}}
    cache.set(
        WATERMARKS, {account_id: day.isoformat() for account_id, day in sorted(marks.items())}
    )


def portion_of(
    account_ids: Iterable[str] | None, first: str | None, last: str | None
) -> Portion | None:
    """The portion that the parameters of a run name: accounts, as digits each, or none for every
    account, and the first and last day, as ISO dates; none when the run names no days, as a
    scheduled run does, which takes the usual window of each account."""
    return portion_from(
        account_ids,
        first,
        last,
        form=ACCOUNT_ID,
        shape="the digits of the ad account without act_",
        error=MetaAdsError,
    )


def oldest(now: datetime) -> date:
    """The first day that Meta still gives at now: "The start date of the time range cannot be
    beyond 37 months from the current date" (error 3018), counted from the newest date anywhere on
    Earth, at UTC+14, as Meta does not say in which time zone, and a day later still, so that a
    load never asks for a day that Meta has just dropped."""
    newest = (now.astimezone(UTC) + timedelta(hours=14)).date()
    return months_back(newest, OLDEST_MONTHS) + timedelta(days=1)


def history_limit(now: datetime) -> date:
    """The first day of the history of the accounts at now: the first day of the month
    HISTORY_MONTHS before yesterday in UTC (ADR-0034), or the oldest day that Meta still gives,
    when that is later, as it can be for a day or two at the end of a month (ADR-0050)."""
    return max(history_start(now.astimezone(UTC).date() - timedelta(days=1)), oldest(now))


def plan(now: datetime, extensions: Path = EXTENSIONS) -> Portion | None:
    """The next portion of the history of the accounts of the deployment as of now, back to
    history_limit, or none: when every account holds its history, when no account has had its
    usual window yet, or while a whole load of meta_ads waits for the run of the transform that
    pins it, so that each portion goes from raw to the core before the next one starts (ADR-0038,
    agoranomos_pipelines/history.py). It reads the warehouse as loader, and needs no token."""
    wanted = checked(accounts(extensions))
    with open_pipeline() as pipeline:
        return planned(pipeline, CODE, "Meta", wanted, history_limit(now))


def number(value: object) -> float | None:
    """A number of a header, or None."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def code_of(value: object) -> int | None:
    """A code of an error, which Meta writes as a number or a numeric string, or None."""
    try:
        return int(str(value))
    except ValueError:
        return None


@dataclass(frozen=True)
class Usage:
    """What the headers of throttling of an answer say: the highest use in percent of the account,
    of the app or of a business-use-case limit, and the longest time in seconds until the use
    resets, where a header gives one; with the text to log."""

    percent: float | None
    reset: float | None
    text: str


def usage(headers) -> Usage:
    """The use that the headers of throttling of an answer give, as the rate limiting of the Graph
    API documents them (read on 15 November 2026): X-Ad-Account-Usage with acc_id_util_pct and
    reset_time_duration, in seconds; X-FB-Ads-Insights-Throttle with acc_id_util_pct and
    app_id_util_pct; and X-Business-Use-Case-Usage, an object of a list for each business ID, each
    with call_count, total_cputime and total_time in percent and estimated_time_to_regain_access in
    minutes. The Marketing API names the last X-Business-Use-Case, so the load reads both names. A
    header that is not JSON is logged and passed over."""
    percents: list[float] = []
    resets: list[float] = []
    said: list[str] = []
    for name in (
        "X-Ad-Account-Usage",
        "X-FB-Ads-Insights-Throttle",
        "X-Business-Use-Case-Usage",
        "X-Business-Use-Case",
    ):
        raw = headers.get(name)
        if raw is None:
            continue
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            said.append(f"{name} is not JSON")
            continue
        said.append(f"{name} {json.dumps(value, sort_keys=True)}")
        entries = []
        if name.startswith("X-Business-Use-Case") and isinstance(value, dict):
            for found in value.values():
                if isinstance(found, list):
                    entries.extend(item for item in found if isinstance(item, dict))
        elif isinstance(value, dict):
            entries.append(value)
        for entry in entries:
            for key in (
                "acc_id_util_pct",
                "app_id_util_pct",
                "call_count",
                "total_cputime",
                "total_time",
            ):
                if (found := number(entry.get(key))) is not None:
                    percents.append(found)
            if (found := number(entry.get("reset_time_duration"))) is not None and found > 0:
                resets.append(found)
            minutes = number(entry.get("estimated_time_to_regain_access"))
            if minutes is not None and minutes > 0:
                resets.append(minutes * 60)
    return Usage(
        max(percents) if percents else None,
        max(resets) if resets else None,
        "; ".join(said) if said else "no header of throttling",
    )


def for_a_person(error: dict, scrub: Callable[[str], str]) -> str:
    """What Meta writes about an error for a person, error_user_title and error_user_msg, as
    "; <title>: <message>", or "" when it writes neither: "The title of the dialog, if shown" and
    "The message to display to the user", in the language of "the locale of the API request"
    (Handling errors, read on 25 November 2026), which is that of the token. Each is scrubbed and
    then cut to 500 characters, so that a secret across the cut leaves none of its characters.
    They are data to show, and nothing acts on them."""
    said = [
        scrub(str(error[key]))[:500]
        for key in ("error_user_title", "error_user_msg")
        if error.get(key) not in (None, "")
    ]
    return "; " + ": ".join(said) if said else ""


def graph_error(response, scrub: Callable[[str], str]) -> tuple[int | None, int | None, str]:
    """The code, the subcode and the message of an error of the Graph API, with what for_a_person
    gives, which it writes as {"error": {"message": ..., "type": ..., "code": ..., "error_subcode":
    ..., "error_user_title": ..., "error_user_msg": ..., "fbtrace_id": ...}} (Handling errors,
    https://developers.facebook.com/docs/graph-api/guides/error-handling/, read on 15 November 2026,
    and for error_user_title and error_user_msg on 25 November 2026), or the start of a body that
    is not one. Each text is scrubbed and then cut to 500 characters, so that a secret across the
    cut leaves none of its characters."""
    try:
        body = response.json()
    except ValueError:
        return None, None, scrub(response.text or "")[:500]
    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return None, None, scrub(json.dumps(body))[:500]
    code, subcode = error.get("code"), error.get("error_subcode")
    return (
        code if isinstance(code, int) else None,
        subcode if isinstance(subcode, int) else None,
        scrub(str(error.get("message", "")))[:500] + for_a_person(error, scrub),
    )


class Api:
    """The Graph API, as the system user, with explicit retries and the waits of throttling."""

    def __init__(self, secrets: Secrets, token: str | None = None, secret: str | None = None):
        self.secrets = secrets
        self.client = Client(**CLIENT)
        self.requests = 0
        # The moment, by monotonic(), by which a report run of a portion completes, or None.
        self.deadline: float | None = None
        self.headers: dict[str, str] = {}
        self.auth: dict[str, str] = {}
        if token is not None:
            self.sign_in(token, secret)

    def sign_in(self, token: str, secret: str | None) -> None:
        """The sign-in that each signed call carries: the token in the header Authorization, and the
        parameter appsecret_proof, its proof, whenever the App Secret is set. The reference of the
        Marketing API shows the token as the parameter access_token, which a URL would carry; the
        WhatsApp Cloud API on the same graph.facebook.com says "include your token in an
        authorization request header, preceded by Bearer" (Access tokens,
        https://developers.facebook.com/documentation/business-messaging/whatsapp/access-tokens/,
        read on 25 November 2026), and the Graph API v27.0 took the header for the ad accounts, the
        lists and the insights of each level, their pages included, on the stand S6 (#220) on 25
        November 2026, which tried no report run. The refresh of current_token is the one call that
        is not signed: it carries its secrets in its query, as Meta documents it. Of the versions of
        the image as of November 2026, dlt 1.32.0 passes the headers of a call to requests and logs
        none, urllib3 2.7.0 logs the URL of a request and never its headers, and requests 2.34.2
        writes the value of a header into an error only when it is malformed, which SECRET_FORM
        rules out; Secrets.scrub masks the header in any text all the same."""
        self.headers = {"Authorization": f"Bearer {token}"}
        self.auth = {}
        self.secrets.add(token)
        if secret is not None:
            self.auth["appsecret_proof"] = proof(token, secret)
            self.secrets.add(self.auth["appsecret_proof"])

    def fail(self, subject: str, text: str, kind: type[MetaAdsError] = MetaAdsError):
        return kind(self.secrets.scrub(f"{subject}: {text}"))

    def pause(self, subject: str, found: Usage, wait: float | None = None) -> None:
        """Log the use that an answer gives, and wait when it passes PAUSE_AT percent, or for wait,
        as after an error of throttling: for the reset time that a header gives, or THROTTLE_WAIT,
        never longer than MAX_WAIT."""
        log.info("%s: %s", subject, found.text)
        if wait is None and (found.percent is None or found.percent <= PAUSE_AT):
            return
        error = wait is not None
        wait = min(wait if error else found.reset or THROTTLE_WAIT, MAX_WAIT)
        percent = "?" if found.percent is None else f"{found.percent:g}"
        if error:
            log.warning(
                "%s: waiting %d s because of the error, not the use of the account (at %s%%)",
                subject,
                wait,
                percent,
            )
        else:
            log.warning(
                "%s: the use of the account is at %s%%; waiting %d s", subject, percent, wait
            )
        sleep(wait)

    def call(
        self,
        subject: str,
        method: str,
        url: str,
        params: dict[str, str],
        sign: bool = True,
        slow: frozenset[tuple[int, int]] = frozenset(),
    ) -> dict:
        """The answer of a call as JSON, after the waits of throttling. An error of throttling, or
        a temporary one, waits and asks again, up to THROTTLE_ATTEMPTS calls; too much data per
        call raises TooMuchData, a timeout or an error of the server after the retries of dlt
        raises Slow, and so does an error whose code and subcode are in slow, at once, which only
        the synchronous request of insights passes, with SYNC_TIMEOUTS; any other error raises
        MetaAdsError, with the error of the API and no secret.
        Each call counts in the score of the account."""
        for attempt in range(1, THROTTLE_ATTEMPTS + 1):
            sent = {**params, **(self.auth if sign else {})}
            headers = self.headers if sign else {}
            self.requests += 1
            try:
                if method == "POST":
                    response = self.client.post(url, data=sent, headers=headers)
                else:
                    response = self.client.get(url, params=sent, headers=headers)
            except Timeout as error:
                raise self.fail(subject, f"the request timed out: {error}", Slow) from None
            except RequestException as error:
                raise self.fail(subject, f"the request failed: {error}") from None
            found = usage(response.headers)
            if response.status_code < 400:
                self.pause(subject, found)
                try:
                    body = response.json()
                except ValueError:
                    raise self.fail(subject, "an answer that is not JSON") from None
                if not isinstance(body, dict):
                    raise self.fail(subject, "an answer that is not a JSON object")
                return body
            code, subcode, message = graph_error(response, self.secrets.scrub)
            said = f"{response.status_code} {message} (code {code}, subcode {subcode})"
            if (code, subcode) == TOO_MUCH_DATA:
                raise self.fail(subject, said, TooMuchData)
            if response.status_code >= 500:
                raise self.fail(subject, said, Slow)
            if (code, subcode) in slow:
                raise self.fail(subject, said, Slow)
            if code in THROTTLED | TEMPORARY and attempt < THROTTLE_ATTEMPTS:
                wait = found.reset or THROTTLE_WAIT * 2 ** (attempt - 1)
                log.warning(
                    "%s; asking again, attempt %d of %d",
                    self.secrets.scrub(f"{subject}: {said}"),
                    attempt + 1,
                    THROTTLE_ATTEMPTS,
                )
                self.pause(subject, found, wait)
                continue
            raise self.fail(subject, said)
        raise AssertionError("unreachable")  # pragma: no cover

    def pages(
        self,
        subject: str,
        path: str,
        params: dict[str, str],
        slow: frozenset[tuple[int, int]] = frozenset(),
    ) -> Iterator[dict]:
        """The rows of every page of an answer, data, through paging.next until a page has none
        (Paginated results of the Graph API, read on 15 November 2026). A next page goes to the
        Graph API alone, with the sign-in of this load, never a token or a proof that the link
        carries."""
        url, query = f"{BASE}/{path}", params
        while True:
            body = self.call(subject, "GET", url, query, slow=slow)
            rows = body.get("data")
            if not isinstance(rows, list):
                raise self.fail(subject, "an answer without a list of data")
            for row in rows:
                if not isinstance(row, dict):
                    raise self.fail(subject, "a row of the answer is not an object")
                yield row
            paging = body.get("paging")
            if paging is not None and not isinstance(paging, dict):
                raise self.fail(subject, "an answer whose paging is not an object")
            link = (paging or {}).get("next")
            if not link:
                return
            parts = urlsplit(str(link))
            if parts.scheme != "https" or parts.hostname != GRAPH:
                raise self.fail(subject, "a next page that is not on the Graph API")
            url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
            query = {
                key: value
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
                if key not in ("access_token", "appsecret_proof")
            }


def current_token(
    api: Api, config: Settings, now: datetime, variables, mask: Callable[[str], None]
) -> str:
    """The token that the load signs in with: the token of .env when it never expires; and for a
    token that expires, the newest that a refresh gave, from the Variable, while it came from the
    token of .env and is younger than REFRESH_AFTER, or else a new one, which a refresh of it
    gives and the Variable then keeps. A token of .env whose age the Variable does not know is
    refreshed at once. A refresh that fails fails the load."""
    if not config.expiring:
        return config.token
    if variables is None:
        raise MetaAdsError("a token that expires needs the Variables of Airflow, to keep it")
    held = variables.get(VARIABLE, default=None, deserialize_json=True)
    source = fingerprint(config.token)
    token = config.token
    if isinstance(held, dict) and held.get("from") == source and held.get("token"):
        token = str(held["token"])
        mask(token)
        api.secrets.add(token)
        try:
            refreshed = datetime.fromisoformat(str(held.get("refreshed_at")))
        except ValueError:
            refreshed = None
        if refreshed is not None and now - refreshed < REFRESH_AFTER:
            return token
    elif held is not None:
        log.info("The token of Meta in .env is new: the load refreshes it and keeps the new one")
    # GET oauth/access_token with grant_type=fb_exchange_token, as the guide of system users shows
    # it, which answers {"access_token": ..., "token_type": "bearer", "expires_in": ...}.
    api.secrets.add(config.app_secret)
    body = api.call(
        "The refresh of the token of Meta",
        "GET",
        f"{BASE}/oauth/access_token",
        {
            "grant_type": "fb_exchange_token",
            "client_id": str(config.app_id),
            "client_secret": str(config.app_secret),
            "set_token_expires_in_60_days": "true",
            "fb_exchange_token": token,
        },
        sign=False,
    )
    new = body.get("access_token")
    if not isinstance(new, str) or not SECRET_FORM.fullmatch(new):
        raise MetaAdsError("The refresh of the token of Meta returned no token")
    mask(new)
    api.secrets.add(new)
    variables.set(
        VARIABLE,
        {"token": new, "from": source, "refreshed_at": now.isoformat()},
        description="The newest token of Meta that the load refreshed (ADR-0050)",
        serialize_json=True,
    )
    log.info(
        "Refreshed the token of Meta; it expires in %s s", body.get("expires_in", "an unknown")
    )
    return new


def insights_params(level: str, days: tuple[date, date]) -> dict[str, str]:
    """The parameters of the insights of a level over days, a row a day: the fields of its table,
    and below the account the filter on the effective_status of the level over every value and the
    windows of attribution (the reference of the insights of an ad account, read on 15 November
    2026)."""
    table = f"insights_{level}"
    params = {
        "level": level,
        "fields": ",".join(FIELDS[table]),
        "time_range": json.dumps({"since": days[0].isoformat(), "until": days[1].isoformat()}),
        "time_increment": "1",
        "limit": str(LIMIT),
    }
    if level != "account":
        params["filtering"] = json.dumps(
            [
                {
                    "field": f"{level}.effective_status",
                    "operator": "IN",
                    "value": list(LEVEL_STATUSES[level]),
                }
            ]
        )
        params["action_attribution_windows"] = json.dumps(list(WINDOWS))
    return params


def write_rows(rows: Iterable[dict], file: TextIO) -> None:
    for row in rows:
        file.write(json.dumps(row) + "\n")


def report_run(api: Api, subject: str, account_id: str, params: dict[str, str], file: TextIO):
    """An asynchronous report run of insights (Limits and best practices of the Insights API, read
    on 15 November 2026): POST act_<id>/insights, which returns a report_run_id; a poll of
    <report_run_id> with a backoff from POLL_FIRST to POLL_LONGEST seconds until async_status is
    Job Completed and async_percent_completion 100; then every page of <report_run_id>/insights,
    written to file. A run that Meta skips, or that fails with an error of RETRY_LATER, is asked
    again, up to REPORT_ATTEMPTS runs; one that fails with too much data, of TOO_LARGE, raises
    TooMuchData, which written_insights takes by months; one that fails otherwise, or that has not
    completed after REPORT_TIMEOUT seconds, fails the account. The portions of the history take
    their insights through this function."""
    for attempt in range(1, REPORT_ATTEMPTS + 1):
        body = api.call(subject, "POST", f"{BASE}/act_{account_id}/insights", params)
        run_id = str(body.get("report_run_id", ""))
        if not ACCOUNT_ID.fullmatch(run_id):
            raise api.fail(subject, "the report run has no report_run_id")
        ended = poll(api, subject, run_id)
        status = ended.get("async_status")
        if status == COMPLETED:
            write_rows(api.pages(subject, f"{run_id}/insights", {"limit": str(LIMIT)}), file)
            return
        found = (code_of(ended.get("error_code")), code_of(ended.get("error_subcode")))
        said = f"report run {run_id} ended with {status}"
        if status != FAILED_JOB:
            found = None
        elif found[0] is not None:
            # Scrubbed before the cut, as graph_error does, and scrubbed again with the rest.
            message = api.secrets.scrub(str(ended.get("error_message") or "no message"))[:500]
            person = for_a_person(ended, api.secrets.scrub)
            said += f": {message}{person} (code {found[0]}, subcode {found[1]})"
        if found in TOO_LARGE:
            raise api.fail(subject, said, TooMuchData)
        if (status == SKIPPED or found in RETRY_LATER) and attempt < REPORT_ATTEMPTS:
            log.warning("%s; asking again", api.secrets.scrub(f"{subject}: {said}"))
            continue
        raise api.fail(subject, said)


def poll(api: Api, subject: str, run_id: str) -> dict:
    """The answer of the report run at which it ends: Job Completed at 100 percent, Job Skipped,
    or Job Failed, which is read once more without fields, so that it carries the fields of its
    error, which Meta returns by default. A run that has not ended after REPORT_TIMEOUT seconds of
    waits, or that would not end before api.deadline, the time of a portion, fails the account."""
    waited, wait = 0, POLL_FIRST
    while waited < REPORT_TIMEOUT:
        if api.deadline is not None and monotonic() + wait > api.deadline:
            raise api.fail(
                subject,
                f"report run {run_id} had not completed when the time of the portion ran out, "
                f"{PORTION_ENDS // 60} minutes after the load began; a later portion asks again",
            )
        sleep(wait)
        waited += wait
        body = api.call(
            subject, "GET", f"{BASE}/{run_id}", {"fields": "async_status,async_percent_completion"}
        )
        status = body.get("async_status")
        if status == COMPLETED and number(body.get("async_percent_completion")) == 100:
            return body
        if status == FAILED_JOB:
            return {**api.call(subject, "GET", f"{BASE}/{run_id}", {}), "async_status": status}
        if status == SKIPPED:
            return body
        log.info(
            "%s: report run %s is at %s%% (%s)",
            subject,
            run_id,
            body.get("async_percent_completion"),
            status,
        )
        wait = min(wait * 2, POLL_LONGEST)
    raise api.fail(subject, f"report run {run_id} did not complete within {REPORT_TIMEOUT} s")


def written_insights(
    api: Api, account_id: str, level: str, days: tuple[date, date], file: TextIO, by_report: bool
) -> None:
    """Write the insights of a level of an account over days to file: synchronously, and as a
    report run when the synchronous request is too large, times out, fails on the server or is
    answered with an error of SYNC_TIMEOUTS on any of its pages, with no synchronous retry of
    these, after the file is cut back to where its rows began; or as a report run at once,
    by_report. A report run with too much data, refused at its POST or failed with an error of
    TOO_LARGE, goes by calendar months (ADR-0050), and a month with too much data fails the
    account."""
    subject = f"Meta ad account {account_id}"
    params = insights_params(level, days)
    if not by_report:
        start = file.tell()
        try:
            found = api.pages(subject, f"act_{account_id}/insights", params, slow=SYNC_TIMEOUTS)
            write_rows(found, file)
            return
        except Slow as error:
            log.warning("%s; asking again as a report run", error)
            file.seek(start)
            file.truncate()
    start = file.tell()
    try:
        report_run(api, subject, account_id, params, file)
    except TooMuchData:
        parts = months(*days)
        if len(parts) == 1:
            raise
        file.seek(start)
        file.truncate()
        log.warning("%s: too much data for one report run of %s; by months", subject, level)
        for part in parts:
            written_insights(api, account_id, level, part, file, by_report=True)


@dataclass
class Read:
    """Everything that a load read of one account, before dlt sees any of it: its row of
    ad_accounts, the window of its facts, and a file for each table of its lists and facts."""

    account: dict
    window: tuple[date, date]
    files: dict[str, Path]


def read_account(
    api: Api,
    account_id: str,
    now: datetime,
    last_end: date | None,
    staging: Path,
    days: tuple[date, date] | None = None,
) -> Read:
    """Read an account whole: the account, whose time zone gives its window, then its lists and the
    insights of its window, each table into a file in the folder of the account under staging. With
    days, a portion of the history, the window is those days, up to yesterday of the account, and
    the insights go as report runs; the lists come too, as staging is to read the lists of the
    newest load of an account. No window starts before the oldest day that Meta still gives
    (oldest): a daily window after a stop of more than 37 months starts there, and says that the
    days before it are lost. An error raises MetaAdsError and leaves nothing of the account: a
    window replaces whole days, and a part of one would lose the rest (ADR-0035)."""
    subject = f"Meta ad account {account_id}"
    account = api.call(
        subject, "GET", f"{BASE}/act_{account_id}", {"fields": ",".join(FIELDS["ad_accounts"])}
    )
    if str(account.get("account_id")) != account_id:
        raise api.fail(subject, "the API returned another account or none")
    try:
        last_day_of_account = yesterday(now, str(account.get("timezone_name")))
    except (ZoneInfoNotFoundError, ValueError):
        raise api.fail(subject, "the account has no time zone that agoranomos knows") from None
    earliest = oldest(now)
    if days is None:
        first, last = window(last_day_of_account, last_end)
        if first < earliest:
            log.warning(
                "%s: the window would start on %s, but Meta gives nothing older than 37 months; it "
                "starts on %s, and the days before are lost",
                subject,
                first.isoformat(),
                earliest.isoformat(),
            )
            first = earliest
    else:
        first, last = max(days[0], earliest), min(days[1], last_day_of_account)
        if days[1] < earliest:
            raise api.fail(
                subject,
                f"the days of the history end on {days[1].isoformat()}, before "
                f"{earliest.isoformat()}, the oldest day that Meta still gives (error 3018)",
            )
        if first > last:
            raise api.fail(
                subject,
                f"the days of the history start on {first.isoformat()}, after its yesterday, "
                f"{last_day_of_account.isoformat()}",
            )
    # The ID is checked digits.
    folder = staging / account_id
    folder.mkdir()
    files = {}
    try:
        for table in ENTITIES:
            path = folder / f"{table}.jsonl"
            params = {
                "fields": ",".join(FIELDS[table]),
                "effective_status": json.dumps(list(LIST_STATUSES)),
                "limit": str(LIMIT),
            }
            with path.open("w", encoding="utf-8") as file:
                write_rows(api.pages(subject, f"act_{account_id}/{EDGES[table]}", params), file)
            files[table] = path
        for table in FACTS:
            path = folder / f"{table}.jsonl"
            with path.open("w", encoding="utf-8") as file:
                written_insights(
                    api, account_id, LEVELS[table], (first, last), file, by_report=days is not None
                )
            files[table] = path
    except MetaAdsError:
        shutil.rmtree(folder, ignore_errors=True)
        raise
    return Read(account, (first, last), files)


def staged_rows(read: dict[str, Read], table: str) -> Iterator[dict]:
    """The rows of a table of each account read, from its file, as the API gave them."""
    for account in read.values():
        with account.files[table].open(encoding="utf-8") as file:
            for line in file:
                yield json.loads(line)


def failure_line(failed: dict[str, MetaAdsError], loaded: int) -> str:
    """The line with which the task fails after a load with failed accounts (ADR-0004)."""
    if loaded:
        rest = f"the other {loaded} {'is' if loaded == 1 else 'are'} loaded"
    else:
        rest = "none is loaded"
    errors = "; ".join(str(error) for error in failed.values())
    return f"{len(failed)} of {len(failed) + loaded} Meta ad accounts failed, {rest}: {errors}"


@contextmanager
def open_pipeline() -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of Meta, as the loader role, in a temporary working folder: dlt keeps the
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
            "Meta is to sunset the Marketing API %s around %s, an estimate while Meta gives no "
            "date: update agoranomos before then, or the loads of meta_ads stop",
            API_VERSION,
            SUNSET.strftime("%B %Y"),
        )


def load(
    now: datetime,
    extensions: Path = EXTENSIONS,
    cache=None,
    portion: Portion | None = None,
    variables=None,
    mask: Callable[[str], None] = lambda value: None,
) -> dict[str, int]:
    """Load the accounts of the deployment as of now, each read whole before dlt sees any of it,
    record each account in _agoranomos_loads, loaded with its window or failed with its error,
    and return the rows of each table that dlt reports. cache is the asset state store of the
    asset of the load, or None; variables the Variables of Airflow, which keep a token that
    expires; mask hides a token that a refresh gives in the logs of Airflow. With a portion, the
    load takes the days of the portion of its accounts, or of every account, with the kind history,
    and leaves the watermarks as they were; it starts no account PORTION_STARTS seconds after it
    began, and leaves those to the next portion, and a report run that has not completed
    PORTION_ENDS seconds after it began fails its account. When an account failed, the load takes
    the other accounts and raises AccountsFailed at the end; when every account failed, it writes
    the journal alone."""
    warn_of_the_sunset(now)
    named = accounts(extensions)
    wanted = named
    if portion is not None and portion.accounts:
        unnamed = sorted(set(portion.accounts) - set(named))
        if unnamed:
            raise MetaAdsError(
                "extensions/model/clients.toml names no account of meta_ads "
                + ", ".join(unnamed)
                + ": a run of the history takes the accounts of the deployment only"
            )
        wanted = list(portion.accounts)
    kind = WINDOW if portion is None else HISTORY
    config = settings()
    secrets = Secrets()
    secrets.add(config.token)
    secrets.add(config.app_secret)
    api = Api(secrets)
    api.sign_in(current_token(api, config, now, variables, mask), config.app_secret)
    if "appsecret_proof" in api.auth:
        mask(api.auth["appsecret_proof"])
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline, tempfile.TemporaryDirectory(prefix="meta-ads-") as staging:
        journal = recorded(pipeline) if has_loads(pipeline) else {}
        core = accepted(pipeline, wanted)
        cached = watermarks(cache)
        days = None if portion is None else (portion.first, portion.last)
        read: dict[str, Read] = {}
        failed: dict[str, MetaAdsError] = {}
        left: list[str] = []
        began = monotonic()
        if portion is not None:
            api.deadline = began + PORTION_ENDS
        for account_id in wanted:
            if portion is not None and (read or failed) and monotonic() - began >= PORTION_STARTS:
                # Not read and not in the journal: the plan gives it to the next portion.
                left.append(account_id)
                continue
            if portion is None:
                check_watermark(account_id, cached, core, journal)
            last_end = last_day(account_id, core, journal)
            try:
                read[account_id] = read_account(api, account_id, now, last_end, Path(staging), days)
            except MetaAdsError as error:
                log.error("%s; the load goes on with the other accounts", error)
                failed[account_id] = error
        resources = []
        if read:
            rows = [account.account for account in read.values()]
            resources.append(dlt.resource(rows, name="ad_accounts", **own(HINTS["ad_accounts"])))
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
                "account_id": account_id,
                "as_of": yesterday(now, account.account["timezone_name"]) + timedelta(days=1),
                "window_from": account.window[0],
                "window_to": account.window[1],
                "now": now,
                "loaded_at": loaded_at,
                "status": LOADED,
                "error": None,
                "kind": kind,
            }
            for account_id, account in read.items()
        ] + [
            {
                "account_id": account_id,
                "as_of": None,
                "window_from": None,
                "window_to": None,
                "now": now,
                "loaded_at": None,
                "status": FAILED,
                "error": str(error),
                "kind": kind,
            }
            for account_id, error in failed.items()
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
    log.info("%d calls to the Graph API for %d accounts", api.requests, len(wanted) - len(left))
    if left:
        log.warning(
            "The portion ran out of its time, %d minutes, after %d accounts: the next portion "
            "takes %s",
            PORTION_STARTS // 60,
            len(read) + len(failed),
            ", ".join(left),
        )
    if failed:
        raise AccountsFailed(failure_line(failed, len(read)))
    return counts
