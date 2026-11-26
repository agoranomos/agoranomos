"""demo_web: a fictional platform of web analytics, in the shape of the GA4 Data API v1beta: the
sessions of each property by day, source, medium, campaign and default channel group, with every
value as a string, the day as YYYYMMDD in the time zone of the property, and no row without
sessions. The links of the ads of the demo carry the ID of their campaign in utm_id, so the
sessions that an ad brings name its campaign; the name is the one of the day of the session, as
GA4 keeps it (ADR-0025). The sessions of a campaign of Performance Max of demo_search take the
channel group Cross-network, and on about half the days GA4 splits them between Cross-network and
Display, as it does for such a campaign of Google Ads, so that one source, medium and campaign of a
day has two rows that differ only in the group (ADR-0057). The journal of each load carries the
four flags of the answers of GA4 (ADR-0054): one of them on the loads of one property whose window
holds one day (flagged), none on the others."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta

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
# The channel groups of the sessions of a campaign of Google whose channel type runs across the
# networks, and the share in percent that GA4 gives the second group on a day that it splits them,
# with the share of the days that it splits.
CROSS_NETWORK = {"PERFORMANCE_MAX": ("Cross-network", "Display")}
SPLIT_SHARE = (20, 40)
SPLIT_DAYS_PCT = 50
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
# The property and the day of the flag of the demo: each load of the property whose window holds the
# day that is 30 days before its yesterday has an answer with rows rolled into "(other)", which GA4
# says in dataLossFromOtherRow when one of its tables has more rows than its limit (ADR-0054), and
# every other load has no flag. In a stack that loads every day only the history takes that day,
# so the rows that the history wrote of the property carry the flag and those of the daily loads do
# not. The name is that of the column of the journal (FLAGS of agoranomos_pipelines/loads.py).
FLAGGED_PROPERTY = "390000002"
FLAGGED_DAYS_AGO = 30
FLAGGED = "has_data_loss_from_other_row"
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
                    measures = (
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
                    for group, values in channel_groups(campaign.kind, channel, key, measures):
                        yield row(
                            prop,
                            day,
                            source,
                            medium,
                            campaign.name_at(day),
                            campaign.campaign_id,
                            group,
                            *values,
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


def channel_groups(kind, channel, key, measures):
    """The default channel groups of the sessions of a campaign on a day, each with its sessions,
    engaged sessions, key events and revenue: the channel of its platform, or, for a kind of
    CROSS_NETWORK, its first group, and on a day that GA4 splits, a share of each measure in its
    second group, which adds up to the same measures. The share and the days depend on the key
    alone, so a day changes only as its measures mature."""
    if kind not in CROSS_NETWORK:
        return [(channel, measures)]
    first, second = CROSS_NETWORK[kind]
    if pct(0, 99, *key, "split") >= SPLIT_DAYS_PCT:
        return [(first, measures)]
    share = pct(*SPLIT_SHARE, *key, "share")
    split = tuple(value * share // 100 for value in measures)
    if split[0] == 0:
        return [(first, measures)]
    kept = tuple(value - part for value, part in zip(measures, split, strict=True))
    return [(first, kept), (second, split)]


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "properties": properties(now),
        "report_sessions_daily": report_sessions_daily(now, first, skip),
    }


def flagged(now: datetime, account_id: str, window: tuple[date, date]) -> bool:
    """Whether a load at now of the window of a property has the flag FLAGGED: the property is
    FLAGGED_PROPERTY and the window holds the day FLAGGED_DAYS_AGO before its yesterday."""
    if account_id != FLAGGED_PROPERTY:
        return False
    prop = next(a for a in accounts() if a.account_id == account_id)
    day = last_final_day(now, prop.time_zone) - timedelta(days=FLAGGED_DAYS_AGO)
    return window[0] <= day <= window[1]


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)
