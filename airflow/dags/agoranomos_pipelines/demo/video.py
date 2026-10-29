"""demo_video: a fictional platform of video ads, in the shape of the TikTok API for Business v1.3:
the integrated report of type BASIC at the level AUCTION_CAMPAIGN, by campaign_id and stat_time_day,
with every metric as a string and "-" for one that means nothing, such as the cost of a conversion
on a day without one. A removed campaign is left out of the list of campaigns and of the reports,
as the default filter STATUS_NOT_DELETE does (ADR-0025). The reports at the levels AUCTION_ADGROUP
and AUCTION_AD, by adgroup_id or ad_id and stat_time_day, have the same metrics, with the IDs of
the levels above as metrics, and the ad groups and the ads are listed like the campaigns
(ADR-0034)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import DELIVERY, major, matured

PLATFORM = "demo_video"
API = "TikTok API for Business v1.3"
# The window after a click reaches 28 days.
CONVERSIONS = ((1, 650), (7, 880), (14, 960), (28, 1000))


def accounts() -> tuple[world.Account, ...]:
    return world.accounts_of(PLATFORM)


def as_of(now: datetime) -> date:
    return now.astimezone(UTC).date()


def advertisers(now: datetime) -> Iterator[dict[str, object]]:
    for account in accounts():
        yield {
            "advertiser_id": account.account_id,
            "name": account.name,
            "currency": account.currency,
            "timezone": account.time_zone,
            "display_timezone": account.display_time_zone,
            "status": "STATUS_ENABLE",
        }


def campaigns(day: date) -> Iterator[dict[str, object]]:
    for campaign in world.listed_campaigns(PLATFORM, day):
        yield {
            "advertiser_id": campaign.account_id,
            "campaign_id": campaign.campaign_id,
            "campaign_name": campaign.name_at(day),
            "objective_type": campaign.kind,
            "operation_status": "ENABLE" if campaign.active(day) else "DISABLE",
        }


def adgroups(day: date) -> Iterator[dict[str, object]]:
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        yield {
            "advertiser_id": group.account_id,
            "campaign_id": campaign.campaign_id,
            "adgroup_id": group.ad_group_id,
            "adgroup_name": group.name,
            "optimization_goal": group.kind,
            "operation_status": "ENABLE",
        }


def ads(day: date) -> Iterator[dict[str, object]]:
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        for ad in group.ads:
            yield {
                "advertiser_id": group.account_id,
                "campaign_id": campaign.campaign_id,
                "adgroup_id": group.ad_group_id,
                "ad_id": ad.ad_id,
                "ad_name": ad.name,
                "ad_format": ad.kind,
                "operation_status": "ENABLE",
            }


CONVERTING = frozenset({"payments", "real_time", "value"})


def measured(final: world.Final, days_old: int) -> dict[str, int]:
    """The numbers of a campaign on a day as a load sees them at an age, money in cents."""
    return {
        "impressions": matured(final.impressions, days_old, DELIVERY),
        "clicks": matured(final.clicks, days_old, DELIVERY),
        "spend": matured(final.spend_micros, days_old, DELIVERY) // 10_000,
        "payments": matured(final.conversions_hundredths // 100, days_old, CONVERSIONS),
        "real_time": matured(
            final.conversions_hundredths // 100 * 95 // 100, days_old, CONVERSIONS
        ),
        "value": matured(final.value_minor, days_old, CONVERSIONS),
    }


def report(now, first=None, skip=frozenset(), level="campaign") -> Iterator[dict[str, object]]:
    """The report of a level, campaign, adgroup or ad, by day. The numbers of an ad group are its
    share of those of its campaign, and those of an ad its share of those of its ad group."""
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        if campaign.removed(name_day):
            continue
        numbers = measured(final, days_old)
        found = [
            (
                {"campaign_id": campaign.campaign_id},
                {"campaign_name": campaign.name_at(name_day)},
                numbers,
            )
        ]
        if level != "campaign":
            groups = world.ad_groups_of(campaign)
            found = [
                (
                    {"adgroup_id": group.ad_group_id},
                    {"campaign_id": campaign.campaign_id, "adgroup_name": group.name},
                    in_group,
                    group,
                )
                for group, in_group in zip(
                    groups, world.divided(numbers, CONVERTING, groups, day), strict=True
                )
            ]
        if level == "ad":
            found = [
                (
                    {"ad_id": ad.ad_id},
                    {
                        "campaign_id": campaign.campaign_id,
                        "adgroup_id": group.ad_group_id,
                        "ad_name": ad.name,
                    },
                    in_ad,
                )
                for _, _, in_group, group in found
                for ad, in_ad in zip(
                    group.ads, world.divided(in_group, CONVERTING, group.ads, day), strict=True
                )
            ]
        for dimensions, attributes, part, *_ in found:
            spend, payments = part["spend"], part["payments"]
            yield {
                "advertiser_id": account.account_id,
                "dimensions": {**dimensions, "stat_time_day": f"{day.isoformat()} 00:00:00"},
                "metrics": {
                    **attributes,
                    "currency": account.currency,
                    "spend": major(spend),
                    "impressions": str(part["impressions"]),
                    "clicks": str(part["clicks"]),
                    # The event that the campaigns optimize for: a complete payment.
                    "conversion": str(payments),
                    "cost_per_conversion": major(spend // payments) if payments else "-",
                    "real_time_conversion": str(part["real_time"]),
                    "complete_payment": str(payments),
                    # Despite its name, the value of the purchases on the site.
                    "total_complete_payment_rate": major(part["value"]),
                },
            }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "advertisers": advertisers(now),
        "campaigns": campaigns(as_of(now)),
        "report_campaign_daily": report(now, first, skip),
        "adgroups": adgroups(as_of(now)),
        "report_adgroup_daily": report(now, first, skip, "adgroup"),
        "ads": ads(as_of(now)),
        "report_ad_daily": report(now, first, skip, "ad"),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
