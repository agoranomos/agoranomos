"""The fictional platforms of the demo as dlt sources, loaded into raw_demo_<platform> as the
loader role (ADR-0004, ADR-0025, ADR-0038).

Each table of a platform is a resource with the hints of TABLE_HINTS: the entities as they are
(replace), the campaigns, the ad groups and the ads with their history (scd2, from the moment of
each load), and the facts, which each load appends under its own _dlt_load_id (append). Each load
also records, in the same run, the window of each account that it took and the day of the load in
the time zone of the account (_agoranomos_loads): the staging models read, for each account and day,
the rows of the newest load whose window covers the day, of those that the run of the transform
pinned (ADR-0046), so a load replaces the days of its window whole, a repeated load creates no
duplicate in staging, and a row that a platform no longer sends goes. No resource has a primary key:
with nested tables, as the actions of Meta are, dlt would need to create a temporary table, which
the loader role may not. The keys are checked in dbt. dlt keeps the state of each pipeline in its
dataset, so its local working folder is a temporary one, and it empties the staging dataset, through
which the versions go, after each load. The first load of a platform, the history, also replays the
names that its campaigns had. The daily load reads the journal: it takes, for each account, the days
after the last that a whole load took, and at least the reload window of the platform. A dataset
that a release before v0.5 loaded, with its merges and its journal _agoranomos_reloads, is dropped
by the next load, which loads the history again (start_over)."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta

import dlt

from agoranomos_pipelines.demo.common import local_today
from agoranomos_pipelines.demo.platforms import LOAD_ORDER, PLATFORMS, gap
from agoranomos_pipelines.demo.world import Account
from agoranomos_pipelines.loads import (
    APPEND,
    DATE,
    DLT_SETTINGS,
    HISTORY,
    LOAD_HINTS,
    LOADED,
    LOADS,
    OLD_JOURNAL,
    TEXT,
    WINDOW,
    credentials,  # noqa: F401 (the DAGs of the demo mask the password of loader through it)
    destination,
    has_loads,
    or_marker,
    own,
    recorded,
    row_counts,
    run,
)

log = logging.getLogger(__name__)

SCD2 = {"disposition": "merge", "strategy": "scd2"}
# The IDs of the tables of the ad groups and of the ads of demo_search.
SEARCH_AD_GROUP = {"customer__id": TEXT, "campaign__id": TEXT, "ad_group__id": TEXT}
SEARCH_AD = {**SEARCH_AD_GROUP, "ad_group_ad__ad__id": TEXT, "ad_group_ad__ad__name": TEXT}
# The money of the reports of demo_search, which the API writes as a JSON number: text, as in the
# table of the connector of Google Ads.
SEARCH_STATS_MONEY = {"metrics__conversions_value": TEXT}
SEARCH_ACTIONS_MONEY = {**SEARCH_STATS_MONEY, "metrics__all_conversions_value": TEXT}

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
        "write_disposition": APPEND,
        "columns": {
            "customer__id": TEXT,
            "campaign__id": TEXT,
            "segments__date": DATE,
            **SEARCH_STATS_MONEY,
        },
    },
    ("demo_search", "campaign_conversions"): {
        "write_disposition": APPEND,
        "columns": {
            "customer__id": TEXT,
            "campaign__id": TEXT,
            "segments__date": DATE,
            **SEARCH_ACTIONS_MONEY,
        },
    },
    ("demo_search", "ad_groups"): {
        "write_disposition": SCD2,
        "columns": {"customer__id": TEXT, "campaign__id": TEXT, "ad_group__id": TEXT},
    },
    ("demo_search", "ad_group_stats"): {
        "write_disposition": APPEND,
        "columns": {**SEARCH_AD_GROUP, "segments__date": DATE, **SEARCH_STATS_MONEY},
    },
    ("demo_search", "ad_group_conversions"): {
        "write_disposition": APPEND,
        "columns": {**SEARCH_AD_GROUP, "segments__date": DATE, **SEARCH_ACTIONS_MONEY},
    },
    # A type of ad that Google gives no name has no field name, so its column is hinted.
    ("demo_search", "ads"): {"write_disposition": SCD2, "columns": SEARCH_AD},
    ("demo_search", "ad_stats"): {
        "write_disposition": APPEND,
        "columns": {**SEARCH_AD, "segments__date": DATE, **SEARCH_STATS_MONEY},
    },
    ("demo_search", "ad_conversions"): {
        "write_disposition": APPEND,
        "columns": {**SEARCH_AD, "segments__date": DATE, **SEARCH_ACTIONS_MONEY},
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
        "write_disposition": APPEND,
        "columns": {"account_id": TEXT, "campaign_id": TEXT, "date_start": DATE, "date_stop": DATE},
    },
    ("demo_social", "adsets"): {
        "write_disposition": SCD2,
        "columns": {"id": TEXT, "account_id": TEXT, "campaign_id": TEXT},
    },
    ("demo_social", "insights_adset"): {
        "write_disposition": APPEND,
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
        "write_disposition": APPEND,
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
        "write_disposition": APPEND,
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
        "write_disposition": APPEND,
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
        "write_disposition": APPEND,
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
        "write_disposition": APPEND,
        "columns": {"property": TEXT, "date": TEXT},
    },
    ("demo_fx", "rates_daily"): {
        "write_disposition": APPEND,
        "columns": {"rate_date": DATE},
    },
}


def journal(
    code: str,
    now: datetime,
    first: Callable[[Account], date] | None = None,
    history: frozenset[str] | None = None,
) -> list[dict[str, object]]:
    """The rows of _agoranomos_loads of a load at now: the window of each account, the day of now
    in the time zone of the account, and the kind of the load of the account: history for an
    account that takes its history, every account without first, or those of history with it,
    and window for the others (ADR-0038). A load of the demo fails whole or not at all, so every
    row is one of a loaded account."""
    platform = PLATFORMS[code]
    zones = {account.account_id: account.time_zone for account in platform.module.accounts()}
    loaded_at = datetime.now(UTC)
    return [
        {
            "account_id": account_id,
            "as_of": local_today(now, zones[account_id]),
            "window_from": start,
            "window_to": end,
            "now": now,
            "loaded_at": loaded_at,
            "status": LOADED,
            "kind": HISTORY if first is None or account_id in (history or ()) else WINDOW,
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
            destination=destination(),
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
    start at boundary, by default now. A fact table takes the rows of the windows of its accounts,
    and is made with its columns when they hold none. The boundary is set on every run: a run of
    scd2 without one would reuse the boundary that dlt stored in the schema at the last load."""
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
            rows = or_marker(rows)
        resources.append(dlt.resource(rows, name=table, **hints))
    return resources


def in_earlier_shape(pipeline) -> bool:
    """Whether a release before v0.5 loaded the dataset of a pipeline: it holds the journal of
    those releases, _agoranomos_reloads, which every load of the demo wrote from v0.2.0 on, and its
    facts were merged in place, so a row of a fact carries the id of the load that last wrote it
    and not of the newest load that covers its day, which the staging models read now."""
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s",
            pipeline.dataset_name,
            OLD_JOURNAL,
        )
    return bool(rows and rows[0][0])


def start_over(pipeline) -> None:
    """Drop every table of the dataset of a platform and of its staging dataset, in one
    transaction, as their owner, the loader role: those of dlt with them, so the next run of the
    pipeline starts as on a new warehouse. CASCADE takes the views of staging over the tables,
    which the next run of the transform builds again; the marts keep their data until then."""
    schemas = (pipeline.dataset_name, f"{pipeline.dataset_name}_staging")
    with pipeline.sql_client() as client:
        # PostgreSQL quotes the names (%I); psycopg2 reads %% as % when it is given values.
        statements = client.execute_sql(
            "SELECT format('DROP TABLE IF EXISTS %%I.%%I CASCADE', schemaname, tablename) "
            "FROM pg_catalog.pg_tables WHERE schemaname IN (%s, %s) ORDER BY 1",
            *schemas,
        )
        with client.begin_transaction():
            for (statement,) in statements or ():
                client.execute_sql(statement)
    log.info(
        "%s was loaded by a release before v0.5: dropped its %d tables, to load its history again",
        pipeline.dataset_name,
        len(statements or ()),
    )


def last_loads(code: str) -> dict[str, tuple[date, date]] | None:
    """What the whole loads of a platform recorded of each account (recorded()), or None when its
    dataset holds no complete load, or was loaded by a release before v0.5, whose next load starts
    over."""
    with open_pipeline(code) as pipeline:
        if not has_loads(pipeline) or in_earlier_shape(pipeline):
            return None
        return recorded(pipeline)


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
    the journal."""
    for table, rows in row_counts(pipeline).items():
        counts[table] = counts.get(table, 0) + rows


def load(
    code: str,
    now: datetime,
    *,
    first: Callable[[Account], date] | None = None,
    history: frozenset[str] | None = None,
    skip: frozenset[tuple[str, date]] = frozenset(),
    replay_names: bool = False,
) -> dict[str, int]:
    """Load a platform as of now, from first(account) to yesterday of each account, record the
    window of each account in _agoranomos_loads, with the kind history for the accounts of
    history, or every account without first (journal), and return the rows of each table of the
    platform that dlt reports. A dataset that a release before v0.5 loaded is dropped first
    (start_over). With replay_names, a dataset that holds no load yet first gets the names of the
    campaigns at each of the platform's name_versions(now)."""
    platform = PLATFORMS[code]
    os.environ.update(DLT_SETTINGS)
    counts: dict[str, int] = {}
    with open_pipeline(code) as pipeline:
        if in_earlier_shape(pipeline):
            start_over(pipeline)
        if replay_names and not has_loads(pipeline):
            for moment in platform.module.name_versions(now):
                run(pipeline, build(code, moment, entities_only=True, boundary=moment))
                counted(counts, pipeline)
        resources = build(code, now, first=first, skip=skip, boundary=now)
        # In the same run as the facts, so under the same _dlt_load_id: a load that does not end
        # whole has no row in _dlt_loads, and its window is read by nobody.
        resources.append(
            dlt.resource(journal(code, now, first, history), name=LOADS, **own(LOAD_HINTS))
        )
        run(pipeline, resources)
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
    last that a whole load took, or from the first day of the reload window of the platform when
    that is earlier, to yesterday of the account. A platform whose dataset holds no load yet, or was
    loaded by a release before v0.5, gets its history, and an account that no load has recorded,
    the days of the history."""
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

    # An account that no load has recorded takes the days of the history, as a load of history.
    new = frozenset(account_id for account_id in history if account_id not in last)
    return load(code, now, first=first, history=new)
