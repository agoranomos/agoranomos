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

A load first reads each account (customer), then its campaigns, its ad groups and its ads, whose
versions dlt keeps (scd2), and then the statistics and the conversions by conversion action of each
of the three levels, by day, in a request of their own, as segments.conversion_action cannot be
selected with metrics.cost_micros (ADR-0034). A Performance Max campaign has no ad groups. The days
of each account run from the day after the last that a load took, or from 90 days back when that is
earlier, as conversions keep coming for up to 90 days after a click; the first load of an account
takes the last 36 months, as Google Ads keeps data by day for 37 only (ADR-0034), a calendar month
in each request. All end yesterday in the time zone of the account. A request counts as one
operation of the daily quota, whatever its rows, so a daily load costs ten operations for each
account, and the first about 230. The days of each account are replaced, empty or not, and the load
records the window of each account in _agoranomos_reloads in the same run.

A load reads each account whole before dlt sees any of it: an error of the API on one account, such
as the 403 of an account that the service account may not read, is caught at the account, which
leaves nothing in the load, as a window replaces whole days and a part of one would lose the rest.
The load takes the other accounts, records the failed account in _agoranomos_reloads with the status
failed and the text of the error, keeps its row of customers as the last load saw it, and fails at
the end with one line that lists the accounts and their errors (ADR-0004, ADR-0035). What the API
returns is data: a campaign name can carry any text, and nothing here acts on it."""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client, HTTPError
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from agoranomos_pipelines.loads import (
    DATE,
    DLT_SETTINGS,
    FAILED,
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
}
# The tables whose versions dlt keeps, and the fact tables, whose days each load replaces.
ENTITIES = ("campaigns", "ad_groups", "ads")
FACTS = (
    "campaign_stats",
    "campaign_conversions",
    "ad_group_stats",
    "ad_group_conversions",
    "ad_stats",
    "ad_conversions",
)
DOUBLE = {"data_type": "double"}
SCD2 = {"disposition": "merge", "strategy": "scd2"}
# The columns of a field of each query: the names that dlt gives the fields, with their types.
COLUMNS = {
    "metrics.conversions": DOUBLE,
    "metrics.conversions_value": DOUBLE,
    "metrics.all_conversions": DOUBLE,
    "metrics.all_conversions_value": DOUBLE,
    "segments.date": DATE,
    "customer.manager": {"data_type": "bool"},
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
# the REST interface writes them, and the other numbers as the API gives them.
HINTS = {
    "customers": {"write_disposition": "replace", "columns": columns("customers")},
    **{table: {"write_disposition": SCD2, "columns": columns(table)} for table in ENTITIES},
    **{table: {"columns": columns(table)} for table in FACTS},
}


class GoogleAdsError(RuntimeError):
    """A mistake in the settings of the source, which stops the load before any request, or an
    answer of the API that stops the load of one account: one line that names the account and
    gives the error of the API."""


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


class Api:
    """The REST interface of the Google Ads API, as the service account, with explicit retries."""

    def __init__(self, signed: service_account.Credentials, login: str | None):
        self.signed, self.login = signed, login
        self.client = Client(**CLIENT)
        self.requests = 0

    def search(self, account_id: str, gaql: str) -> Iterator[dict]:
        """The results of a query of an account, as the API gives them."""
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
            )
        except HTTPError as error:
            # The original error of the API, as the contract of the sources asks (ADR-0004): its
            # status and the start of its body, which names the error and the request.
            answer = error.response
            status, body = (
                (answer.status_code, answer.text[:2000]) if answer is not None else ("", "")
            )
            raise GoogleAdsError(f"Google Ads account {account_id}: {status} {body}") from None
        for batch in response.json():
            yield from batch.get("results", ())


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
    customers, the window of its facts, and the rows of its campaigns, ad groups and ads and of
    its facts, in a file for each table."""

    customer: dict
    window: tuple[date, date]
    files: dict[str, Path]


def read_account(
    api: Api, account_id: str, now: datetime, last_end: date | None, staging: Path
) -> Read:
    """Read an account whole: its customer, which gives its time zone and so its window, then its
    campaigns, ad groups and ads and the facts of its window, each table into a file in the folder
    of the account under staging, on disk, so that the load holds one answer in memory at a time,
    as a load that fed dlt did (ADR-0038). An error of the API on any request raises GoogleAdsError
    and leaves nothing of the account: a window replaces whole days, and a part of one would lose
    the rest (ADR-0035)."""
    rows = [
        row
        for row in api.search(account_id, query("customers"))
        if row["customer"]["id"] == account_id
    ]
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
                    for row in api.search(account_id, query(table, days)):
                        file.write(json.dumps(row) + "\n")
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
        pipeline.run(resources)
        counts = {
            table: rows
            for table, rows in pipeline.last_trace.last_normalize_info.row_counts.items()
            if not table.startswith("_")
        }
    for table, rows in sorted(counts.items()):
        log.info("Loaded %d rows into %s.%s, as of %s", rows, DATASET, table, now.isoformat())
    log.info("%d requests to the Google Ads API for %d accounts", api.requests, len(wanted))
    if failed:
        raise AccountsFailed(failure_line(failed, len(read)))
    return counts
