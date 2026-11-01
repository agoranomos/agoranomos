"""The fictional platforms of the demo as dlt sources, loaded into raw_demo_<platform> as the
loader role (ADR-0004, ADR-0025).

Each table of a platform is a resource with the hints of TABLE_HINTS: the entities as they are
(replace), the campaigns, the ad groups and the ads with their history (scd2, from the moment of
each load), and the facts,
whose load replaces the days of each account that it covers (delete-insert with a destination
scope that every run sets, as a run without one would reuse the last scope stored in the schema of
dlt), so a repeated load creates no duplicates and a row that a platform no longer sends goes. No
resource has a primary key: with nested tables, as the actions of Meta are, dlt would need to
create a temporary table, which the loader role may not. The keys are checked in dbt. A window
that comes back without a row still replaces the window: its resource yields the marker with which
dlt deletes the scope and inserts nothing. Each load also records, in the same run, the window of
each account that it replaced and the day of the load in the time zone of the account
(_agoranomos_reloads), so that a later load can read how far each account has been loaded. dlt
keeps the state of each pipeline in its dataset, so its local working folder is a temporary one,
and it empties the staging dataset after each load, which keeps only the rows of the last one
otherwise. The first load of a platform, the history, also replays the names that its campaigns
had. The daily load reads that record: it takes, for each account, the days after the last that a
load took, and at least the reload window of the platform."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta

import dlt

from agoranomos_pipelines.demo.common import local_today
from agoranomos_pipelines.demo.platforms import LOAD_ORDER, PLATFORMS, gap
from agoranomos_pipelines.demo.world import Account
from agoranomos_pipelines.loads import (
    DATE,
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

SCD2 = {"disposition": "merge", "strategy": "scd2"}
# The IDs of the tables of the ad groups and of the ads of demo_search.
SEARCH_AD_GROUP = {"customer__id": TEXT, "campaign__id": TEXT, "ad_group__id": TEXT}
SEARCH_AD = {**SEARCH_AD_GROUP, "ad_group_ad__ad__id": TEXT, "ad_group_ad__ad__name": TEXT}

# The hints of each table, by the names that dlt gives the columns: the fields of the API in
# snake_case, nested objects joined by __ (dlt 1.31.0, the naming convention snake_case).
TABLE_HINTS: dict[tuple[str, str], dict] = {
    ("demo_search", "customer_clients"): {
        "write_disposition": "replace",
        "columns": {"customer_client__id": TEXT},
    },
    ("demo_search", "campaigns"): {
        "write_disposition": SCD2,
        "columns": {"customer__id": TEXT, "campaign__id": TEXT},
    },
    ("demo_search", "campaign_stats"): {
        "write_disposition": MERGE,
        "columns": {"customer__id": TEXT, "campaign__id": TEXT, "segments__date": DATE},
    },
    ("demo_search", "campaign_conversions"): {
        "write_disposition": MERGE,
        "columns": {"customer__id": TEXT, "campaign__id": TEXT, "segments__date": DATE},
    },
    ("demo_search", "ad_groups"): {
        "write_disposition": SCD2,
        "columns": {"customer__id": TEXT, "campaign__id": TEXT, "ad_group__id": TEXT},
    },
    ("demo_search", "ad_group_stats"): {
        "write_disposition": MERGE,
        "columns": {**SEARCH_AD_GROUP, "segments__date": DATE},
    },
    ("demo_search", "ad_group_conversions"): {
        "write_disposition": MERGE,
        "columns": {**SEARCH_AD_GROUP, "segments__date": DATE},
    },
    # A type of ad that Google gives no name has no field name, so its column is hinted.
    ("demo_search", "ads"): {"write_disposition": SCD2, "columns": SEARCH_AD},
    ("demo_search", "ad_stats"): {
        "write_disposition": MERGE,
        "columns": {**SEARCH_AD, "segments__date": DATE},
    },
    ("demo_search", "ad_conversions"): {
        "write_disposition": MERGE,
        "columns": {**SEARCH_AD, "segments__date": DATE},
    },
    ("demo_social", "ad_accounts"): {
        "write_disposition": "replace",
        "columns": {"account_id": TEXT},
    },
    ("demo_social", "campaigns"): {
        "write_disposition": SCD2,
        "columns": {"id": TEXT, "account_id": TEXT},
    },
    ("demo_social", "insights_campaign"): {
        "write_disposition": MERGE,
        "columns": {"account_id": TEXT, "campaign_id": TEXT, "date_start": DATE, "date_stop": DATE},
    },
    ("demo_social", "adsets"): {
        "write_disposition": SCD2,
        "columns": {"id": TEXT, "account_id": TEXT, "campaign_id": TEXT},
    },
    ("demo_social", "insights_adset"): {
        "write_disposition": MERGE,
        "columns": {
            "account_id": TEXT,
            "campaign_id": TEXT,
            "adset_id": TEXT,
            "date_start": DATE,
            "date_stop": DATE,
        },
    },
    ("demo_social", "ads"): {
        "write_disposition": SCD2,
        "columns": {"id": TEXT, "account_id": TEXT, "campaign_id": TEXT, "adset_id": TEXT},
    },
    ("demo_social", "insights_ad"): {
        "write_disposition": MERGE,
        "columns": {
            "account_id": TEXT,
            "campaign_id": TEXT,
            "adset_id": TEXT,
            "ad_id": TEXT,
            "date_start": DATE,
            "date_stop": DATE,
        },
    },
    ("demo_video", "advertisers"): {
        "write_disposition": "replace",
        "columns": {"advertiser_id": TEXT},
    },
    ("demo_video", "campaigns"): {
        "write_disposition": SCD2,
        "columns": {"advertiser_id": TEXT, "campaign_id": TEXT},
    },
    ("demo_video", "report_campaign_daily"): {
        "write_disposition": MERGE,
        # Text, as TikTok writes it: staging takes the date from it.
        "columns": {
            "advertiser_id": TEXT,
            "dimensions__campaign_id": TEXT,
            "dimensions__stat_time_day": TEXT,
        },
    },
    ("demo_video", "adgroups"): {
        "write_disposition": SCD2,
        "columns": {"advertiser_id": TEXT, "campaign_id": TEXT, "adgroup_id": TEXT},
    },
    ("demo_video", "report_adgroup_daily"): {
        "write_disposition": MERGE,
        "columns": {
            "advertiser_id": TEXT,
            "dimensions__adgroup_id": TEXT,
            "dimensions__stat_time_day": TEXT,
            "metrics__campaign_id": TEXT,
        },
    },
    ("demo_video", "ads"): {
        "write_disposition": SCD2,
        "columns": {
            "advertiser_id": TEXT,
            "campaign_id": TEXT,
            "adgroup_id": TEXT,
            "ad_id": TEXT,
        },
    },
    ("demo_video", "report_ad_daily"): {
        "write_disposition": MERGE,
        "columns": {
            "advertiser_id": TEXT,
            "dimensions__ad_id": TEXT,
            "dimensions__stat_time_day": TEXT,
            "metrics__campaign_id": TEXT,
            "metrics__adgroup_id": TEXT,
        },
    },
    ("demo_web", "properties"): {"write_disposition": "replace", "columns": {"name": TEXT}},
    ("demo_web", "report_sessions_daily"): {
        "write_disposition": MERGE,
        "columns": {"property": TEXT, "date": TEXT},
    },
    ("demo_fx", "rates_daily"): {
        "write_disposition": MERGE,
        "columns": {"rate_date": DATE},
    },
}

# The account IDs that a scope writes into its SQL, which dlt then formats with str.format: ASCII
# letters, digits, _ and -, so that an ID from outside the code of the demo cannot change either.
ACCOUNT_ID = re.compile(r"[A-Za-z0-9_-]+")


def scope(code: str, table: str, windows: dict[str, tuple[date, date]]) -> str:
    """The rows of a fact table that a load replaces, as the SQL condition of dlt's destination
    scope: those of each account in its window, by the column of the account and the day of the
    fact table (platforms.FactTable). The days are dates, and an account ID that is not of
    ACCOUNT_ID fails, with a line that names no ID. The natural keys, which the critical tests of
    staging check, are in the reference of the model: these tables have none in dlt."""
    [fact] = [f for f in PLATFORMS[code].facts if f.table == table]
    if not all(ACCOUNT_ID.fullmatch(account_id) for account_id in windows):
        raise ValueError(
            f"the scope of {code}.{table} takes account IDs of ASCII letters, digits, _ and - only"
        )
    parts = []
    for account_id, (first, last) in sorted(windows.items()):
        between = (
            f"{fact.day_sql} BETWEEN '{first.strftime(fact.day_text)}' "
            f"AND '{last.strftime(fact.day_text)}'"
        )
        if fact.account_sql is None:
            parts.append(between)
        else:
            account = fact.account_text.format(account_id)
            parts.append(f"({fact.account_sql} = '{account}' AND {between})")
    return " OR ".join(parts)


def reloads(
    code: str, now: datetime, first: Callable[[Account], date] | None = None
) -> list[dict[str, object]]:
    """The rows of _agoranomos_reloads of a load at now: the window of each account, and the day
    of now in the time zone of the account. A load of the demo fails whole or not at all, so every
    row is one of a loaded account."""
    platform = PLATFORMS[code]
    zones = {account.account_id: account.time_zone for account in platform.module.accounts()}
    loaded_at = datetime.now(UTC)
    return [
        {
            "account_id": account_id,
            "as_of": local_today(now, zones[account_id]),
            "window_start": start,
            "window_end": end,
            "now": now,
            "loaded_at": loaded_at,
            "status": LOADED,
        }
        for account_id, (start, end) in sorted(platform.module.windows(now, first).items())
    ]


@contextmanager
def open_pipeline(code: str) -> Iterator[dlt.Pipeline]:
    """The dlt pipeline of a platform, as the loader role, in a temporary working folder: dlt keeps
    the state of each pipeline in its dataset. A caller can read through it what earlier loads left
    in the dataset before it loads."""
    platform = PLATFORMS[code]
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        yield dlt.pipeline(
            pipeline_name=platform.pipeline,
            pipelines_dir=workdir,
            destination=dlt.destinations.postgres(credentials=credentials()),
            dataset_name=platform.dataset,
        )


def build(
    code: str,
    now: datetime,
    *,
    first: Callable[[Account], date] | None = None,
    skip: frozenset[tuple[str, date]] = frozenset(),
    entities_only: bool = False,
    boundary: datetime | None = None,
) -> list:
    """The resources of a platform as of now: every table, or the campaigns alone, whose versions
    start at boundary, by default now. A fact table replaces the windows of its accounts, empty or
    not. Like the scope, the boundary is set on every run: a run of scd2 without one would reuse
    the boundary that dlt stored in the schema at the last load."""
    platform = PLATFORMS[code]
    boundary = now if boundary is None else boundary
    facts = {fact.table for fact in platform.facts}
    resources = []
    for table, rows in platform.module.records(now, first, skip).items():
        if entities_only and table != "campaigns":
            continue
        hints = own(TABLE_HINTS[(code, table)])
        if hints["write_disposition"] == SCD2:
            hints["write_disposition"] = {**SCD2, "boundary_timestamp": boundary.isoformat()}
        if table in facts:
            covered = scope(code, table, platform.module.windows(now, first))
            hints["write_disposition"] = {**MERGE, "destination_scope": covered}
            rows = or_marker(rows)
        resources.append(dlt.resource(rows, name=table, **hints))
    return resources


def last_loads(code: str) -> dict[str, tuple[date, date]] | None:
    """What the loads of a platform recorded of each account (recorded()), or None when its dataset
    holds no complete load."""
    with open_pipeline(code) as pipeline:
        return recorded(pipeline) if has_loads(pipeline) else None


def behind(code: str, now: datetime) -> list[str]:
    """The accounts of a platform that no load has taken on their day of now, in the time zone of
    each: every account before the first load of the platform."""
    last = last_loads(code) or {}
    return [
        account.account_id
        for account in PLATFORMS[code].module.accounts()
        if account.account_id not in last
        or last[account.account_id][0] < local_today(now, account.time_zone)
    ]


def due(now: datetime) -> bool:
    """Whether a daily load at now brings anything: an account of a platform that no load has
    taken on its day of now. Another load on the same day brings the same numbers, which change
    with the age of their day only (common.matured)."""
    late = {code: behind(code, now) for code in LOAD_ORDER}
    for code, accounts in late.items():
        if accounts:
            log.info(
                "%s: %d of %d accounts have no load of their day at %s",
                code,
                len(accounts),
                len(PLATFORMS[code].module.accounts()),
                now.isoformat(),
            )
    if not any(late.values()):
        log.info("Every account has a load of its day at %s: nothing to load", now.isoformat())
    return any(late.values())


def counted(counts: dict[str, int], pipeline) -> None:
    """Add the rows of the tables of the platform that the last run loaded: not those of dlt, nor
    the record of the windows."""
    for table, rows in pipeline.last_trace.last_normalize_info.row_counts.items():
        if not table.startswith("_"):
            counts[table] = counts.get(table, 0) + rows


def load(
    code: str,
    now: datetime,
    *,
    first: Callable[[Account], date] | None = None,
    skip: frozenset[tuple[str, date]] = frozenset(),
    replay_names: bool = False,
) -> dict[str, int]:
    """Load a platform as of now, from first(account) to yesterday of each account, record the
    window of each account in _agoranomos_reloads, and return the rows of each table of the
    platform that dlt reports. With replay_names, a dataset that holds no load yet first gets the
    names of the campaigns at each of the platform's name_versions(now)."""
    platform = PLATFORMS[code]
    os.environ.update(DLT_SETTINGS)
    counts: dict[str, int] = {}
    with open_pipeline(code) as pipeline:
        if replay_names and not has_loads(pipeline):
            for moment in platform.module.name_versions(now):
                pipeline.run(build(code, moment, entities_only=True, boundary=moment))
                counted(counts, pipeline)
        resources = build(code, now, first=first, skip=skip, boundary=now)
        # In the same run as the facts, so that a load that fails records no window.
        resources.append(dlt.resource(reloads(code, now, first), name=RELOADS, **own(RELOAD_HINTS)))
        pipeline.run(resources)
        counted(counts, pipeline)
    for table, rows in sorted(counts.items()):
        log.info(
            "Loaded %d rows into %s.%s, as of %s", rows, platform.dataset, table, now.isoformat()
        )
    return counts


def load_history(code: str, now: datetime) -> dict[str, int]:
    """The first load of a platform: its history up to yesterday, with the versions of the names of
    its campaigns, and without the day that the demo leaves out."""
    return load(code, now, skip=gap(now), replay_names=True)


def load_daily(code: str, now: datetime) -> dict[str, int]:
    """The daily load of a platform (ADR-0025): for each account, the days from the day after the
    last that a load took, or from the first day of the reload window of the platform when that is
    earlier, to yesterday of the account. A platform whose dataset holds no load yet gets its
    history, and an account that no load has recorded, the days of the history."""
    platform = PLATFORMS[code]
    last = last_loads(code)
    if last is None:
        return load_history(code, now)
    history = platform.module.windows(now)
    reload_days = timedelta(days=platform.window_days - 1)

    def first(account: Account) -> date:
        start, end = history[account.account_id]
        if account.account_id not in last:
            return start
        return min(last[account.account_id][1] + timedelta(days=1), end - reload_days)

    return load(code, now, first=first)
