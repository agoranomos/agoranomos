"""The fictional platforms of the demo in the order that the history loads them: where dlt loads
each, the main table whose asset its load updates, how its fact tables hold an account and a day,
and how many recent days its daily load is to reload (ADR-0025). The history leaves one day of one
account out (gap): a failed day that no reload window reaches, which the warning test gap of dbt
finds. Nothing here imports dlt, so that code without it can read the fact tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import ModuleType

from agoranomos_pipelines.demo import fx, search, social, video, web
from agoranomos_pipelines.demo.common import last_final_day


@dataclass(frozen=True)
class FactTable:
    """A fact table of a platform in raw, by the names that dlt gives its columns, and how it holds
    the account and the day of a row: a load replaces the days of each account in its window
    through these, and a reader finds the days of each account through them."""

    table: str
    account_sql: str | None  # the column of the account; None for the rates
    account_text: str  # how an account ID is written in it, a format such as properties/{} of GA4
    day_sql: str  # the day as stored
    day_text: str  # the strftime of a stored day: ISO or COMPACT
    day_date_sql: str  # the day as a date, for reads


@dataclass(frozen=True)
class Platform:
    code: str
    mimics: str
    dataset: str
    pipeline: str
    main_table: str
    window_days: int
    module: ModuleType
    facts: tuple[FactTable, ...]  # the main table first


def _platform(module: ModuleType, main_table: str, window_days: int, *facts: FactTable) -> Platform:
    code = module.PLATFORM
    return Platform(
        code,
        module.API,
        f"raw_{code}",
        f"agoranomos_{code}",
        main_table,
        window_days,
        module,
        facts,
    )


ISO, COMPACT = "%Y-%m-%d", "%Y%m%d"
# dlt types the flattened day of Google as a date, as its hint says (dlt 1.31.0); the cast of the
# day for reads stays, so that it reads a day stored as text as well.
SEARCH_DAY = ('"segments__date"', ISO, '"segments__date"::date')
# TikTok writes the day as "YYYY-MM-DD 00:00:00".
VIDEO_DAY = 'left("dimensions__stat_time_day"::text, 10)'

PLATFORMS = {
    platform.code: platform
    for platform in (
        _platform(
            fx,
            "rates_daily",
            7,
            FactTable("rates_daily", None, "{}", '"rate_date"', ISO, '"rate_date"'),
        ),
        _platform(
            search,
            "campaign_stats",
            90,
            FactTable("campaign_stats", '"customer__id"', "{}", *SEARCH_DAY),
            FactTable("campaign_conversions", '"customer__id"', "{}", *SEARCH_DAY),
        ),
        _platform(
            social,
            "insights_campaign",
            28,
            # Its child tables of actions follow it through _dlt_root_id.
            FactTable(
                "insights_campaign", '"account_id"', "{}", '"date_start"', ISO, '"date_start"'
            ),
        ),
        _platform(
            video,
            "report_campaign_daily",
            28,
            FactTable(
                "report_campaign_daily",
                '"advertiser_id"',
                "{}",
                VIDEO_DAY,
                ISO,
                f"{VIDEO_DAY}::date",
            ),
        ),
        _platform(
            web,
            "report_sessions_daily",
            14,
            FactTable(
                "report_sessions_daily",
                '"property"',
                "properties/{}",
                '"date"',
                COMPACT,
                "to_date(\"date\", 'YYYYMMDD')",
            ),
        ),
    )
}
LOAD_ORDER = tuple(PLATFORMS)
GAP_ACCOUNT = "802000000000001"
GAP_DAYS_AGO = 120


def asset_uri(platform: Platform) -> str:
    """The asset of the main table, in the form that Airflow 3 requires for PostgreSQL."""
    return f"postgres://warehouse-db:5432/warehouse/{platform.dataset}/{platform.main_table}"


def gap(now: datetime) -> frozenset[tuple[str, date]]:
    """The (account, day) that the history leaves out."""
    account = next(a for a in social.accounts() if a.account_id == GAP_ACCOUNT)
    return frozenset(
        {(GAP_ACCOUNT, last_final_day(now, account.time_zone) - timedelta(days=GAP_DAYS_AGO))}
    )
