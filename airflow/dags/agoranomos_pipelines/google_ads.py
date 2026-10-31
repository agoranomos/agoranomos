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
records the window of each account in _agoranomos_reloads in the same run. What the API returns is
data: a campaign name can carry any text, and nothing here acts on it."""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dlt
from dlt.sources.helpers.requests import Client, HTTPError
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from agoranomos_pipelines.loads import (
    DATE,
    DLT_SETTINGS,
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
    """A mistake in the settings of the source, or an answer of the API that stops the load: one
    line that names the account and gives the error of the API."""


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


def scope(windows: dict[str, tuple[date, date]]) -> str:
    """The rows of a fact table that a load replaces, as the SQL condition of dlt's destination
    scope: those of each account in its window. The IDs are checked digits, the days dates."""
    if not all(ACCOUNT_ID.fullmatch(account_id) for account_id in windows):
        raise ValueError("the scope of google_ads takes account IDs of ten digits only")
    return " OR ".join(
        f"(customer__id = '{account_id}' AND segments__date BETWEEN "
        f"'{first.isoformat()}' AND '{last.isoformat()}')"
        for account_id, (first, last) in sorted(windows.items())
    )


def entities(api: Api, table: str, wanted: list[str]) -> Iterator[dict]:
    """The rows of a table of entities, campaigns, ad groups or ads, of each account."""
    for account_id in wanted:
        yield from api.search(account_id, query(table))


def facts(api: Api, table: str, windows: dict[str, tuple[date, date]]) -> Iterator[dict]:
    """The rows of a fact table of each account in its window."""
    for account_id, (first, last) in sorted(windows.items()):
        for days in chunks(first, last):
            yield from api.search(account_id, query(table, days))


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
    """Load the accounts of the deployment as of now, record the window of each account in
    _agoranomos_reloads, and return the rows of each table that dlt reports."""
    warn_of_the_sunset(now)
    wanted = accounts(extensions)
    api = Api(sign_in(), login_customer_id())
    customers = [row for account_id in wanted for row in api.search(account_id, query("customers"))]
    zones = {row["customer"]["id"]: row["customer"]["timeZone"] for row in customers}
    missing = [account_id for account_id in wanted if account_id not in zones]
    if missing:
        raise GoogleAdsError(f"Google Ads account {missing[0]}: the API returned no customer")
    os.environ.update(DLT_SETTINGS)
    with open_pipeline() as pipeline:
        last = recorded(pipeline) if has_loads(pipeline) else {}
        windows = {
            account_id: window(
                yesterday(now, zones[account_id]),
                last[account_id][1] if account_id in last else None,
            )
            for account_id in wanted
        }
        resources = [dlt.resource(customers, name="customers", **own(HINTS["customers"]))]
        for table in ENTITIES:
            hints = own(HINTS[table])
            hints["write_disposition"] = {
                **hints["write_disposition"],
                "boundary_timestamp": now.isoformat(),
            }
            resources.append(dlt.resource(entities(api, table, wanted), name=table, **hints))
        for table in FACTS:
            hints = own(HINTS[table])
            hints["write_disposition"] = {**MERGE, "destination_scope": scope(windows)}
            resources.append(
                dlt.resource(or_marker(facts(api, table, windows)), name=table, **hints)
            )
        record = [
            {
                "account_id": account_id,
                "as_of": yesterday(now, zones[account_id]) + timedelta(days=1),
                "window_start": first,
                "window_end": end,
                "now": now,
                "loaded_at": datetime.now(UTC),
            }
            for account_id, (first, end) in sorted(windows.items())
        ]
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
    return counts
