"""demo_web: a fictional platform of web analytics, in the shape of the GA4 Data API v1beta: the
sessions of each property by day, source, medium, campaign and default channel group, with every
value as a string, the day as YYYYMMDD in the time zone of the property, and no row without
sessions. The links of the ads of the demo carry the ID of their campaign in utm_id, so the
sessions that an ad brings name its campaign; the name is the one of the day of the session, as
GA4 keeps it (ADR-0025)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import (
    age,
    days,
    demand_pct,
    history_first_day,
    last_final_day,
    major,
    matured,
    pct,
)

PLATFORM = "demo_web"
API = "GA4 Data API v1beta"
SESSIONS = ((1, 970), (2, 1000))
# GA4 changes the attribution of key events for up to 12 days.
KEY_EVENTS = ((1, 800), (3, 930), (7, 980), (12, 1000))
# The source, medium and default channel group of the sessions that the ads of each platform
# bring, as the utm parameters of their links say.
AD_TRAFFIC = {
    "demo_search": ("demo-search", "cpc", "Paid Search"),
    "demo_social": ("demo-social", "paid", "Paid Social"),
    "demo_video": ("demo-video", "paid", "Paid Video"),
}
# The sessions that come without ads, on an average day of a property: source, medium, campaign,
# default channel group, sessions.
OTHER_TRAFFIC = (
    ("(direct)", "(none)", "(direct)", "Direct", 300),
    ("demo-search", "organic", "(organic)", "Organic Search", 900),
    ("other-search.example", "organic", "(organic)", "Organic Search", 80),
    ("newsletter", "email", "Newsletter {month} {year}", "Email", 120),
    ("blog.example", "referral", "(referral)", "Referral", 60),
    ("(not set)", "(not set)", "(not set)", "Unassigned", 20),
)
MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def accounts() -> tuple[world.Account, ...]:
    return world.accounts_of(PLATFORM)


def properties(now: datetime) -> Iterator[dict[str, object]]:
    for account in accounts():
        yield {
            "name": f"properties/{account.account_id}",
            "displayName": account.name,
            "timeZone": account.time_zone,
            "currencyCode": account.currency,
        }


def row(
    prop,
    day,
    source,
    medium,
    campaign_name,
    campaign_id,
    channel,
    sessions,
    engaged,
    key_events,
    revenue,
):
    return {
        "property": f"properties/{prop.account_id}",
        "date": day.strftime("%Y%m%d"),
        "sessionSource": source,
        "sessionMedium": medium,
        "sessionCampaignName": campaign_name,
        "sessionCampaignId": campaign_id,
        "sessionDefaultChannelGroup": channel,
        "sessions": str(sessions),
        "engagedSessions": str(engaged),
        "keyEvents": str(key_events),
        "totalRevenue": major(revenue),
    }


def report_sessions_daily(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    for prop in accounts():
        start = first(prop) if first else history_first_day(now, prop.time_zone)
        last = last_final_day(now, prop.time_zone)
        ads = [a for a in world.ACCOUNTS if a.client == prop.client and a.platform in AD_TRAFFIC]
        for day in days(start, last):
            if (prop.account_id, day) in skip:
                continue
            days_old = age(now, prop.time_zone, day)
            for account in ads:
                source, medium, channel = AD_TRAFFIC[account.platform]
                for campaign in world.campaigns_of(account.account_id, day, day):
                    stats = world.final(campaign, day)
                    if stats is None:
                        continue
                    key = (PLATFORM, prop.account_id, campaign.campaign_id, day.isoformat())
                    sessions = matured(
                        stats.clicks * pct(85, 95, *key, "s") // 100, days_old, SESSIONS
                    )
                    if sessions == 0:
                        continue
                    yield row(
                        prop,
                        day,
                        source,
                        medium,
                        campaign.name_at(day),
                        campaign.campaign_id,
                        channel,
                        sessions,
                        sessions * pct(60, 70, *key, "e") // 100,
                        matured(
                            stats.conversions_hundredths // 100 * pct(80, 95, *key, "k") // 100,
                            days_old,
                            KEY_EVENTS,
                        ),
                        matured(
                            stats.value_minor * pct(90, 100, *key, "r") // 100, days_old, KEY_EVENTS
                        ),
                    )
            for source, medium, name, channel, base in OTHER_TRAFFIC:
                key = (PLATFORM, prop.account_id, source, medium, day.isoformat())
                sessions = matured(
                    base * demand_pct(day) // 100 * pct(90, 110, *key, "s") // 100,
                    days_old,
                    SESSIONS,
                )
                if sessions == 0:
                    continue
                key_events = matured(sessions * 2 // 100, days_old, KEY_EVENTS)
                yield row(
                    prop,
                    day,
                    source,
                    medium,
                    name.format(month=MONTHS[day.month - 1], year=day.year),
                    "(not set)",
                    channel,
                    sessions,
                    sessions * pct(45, 55, *key, "e") // 100,
                    key_events,
                    key_events * 6_000,
                )


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "properties": properties(now),
        "report_sessions_daily": report_sessions_daily(now, first, skip),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return []
