"""demo_video: a fictional platform of video ads, in the shape of the TikTok API for Business v1.3:
the integrated report of type BASIC at the level AUCTION_CAMPAIGN, by campaign_id and stat_time_day,
with every metric as a string and "-" for one that means nothing, such as the cost of a conversion
on a day without one. A removed campaign is left out of the list of campaigns and of the reports,
as the default filter STATUS_NOT_DELETE does (ADR-0025)."""

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


def report_campaign_daily(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        if campaign.removed(name_day):
            continue
        spend = matured(final.spend_micros, days_old, DELIVERY) // 10_000
        payments = matured(final.conversions_hundredths // 100, days_old, CONVERSIONS)
        yield {
            "advertiser_id": account.account_id,
            "dimensions": {
                "campaign_id": campaign.campaign_id,
                "stat_time_day": f"{day.isoformat()} 00:00:00",
            },
            "metrics": {
                "campaign_name": campaign.name_at(name_day),
                "currency": account.currency,
                "spend": major(spend),
                "impressions": str(matured(final.impressions, days_old, DELIVERY)),
                "clicks": str(matured(final.clicks, days_old, DELIVERY)),
                # The event that the campaigns optimize for: a complete payment.
                "conversion": str(payments),
                "cost_per_conversion": major(spend // payments) if payments else "-",
                "real_time_conversion": str(
                    matured(final.conversions_hundredths // 100 * 95 // 100, days_old, CONVERSIONS)
                ),
                "complete_payment": str(payments),
                # Despite its name, the value of the purchases on the site.
                "total_complete_payment_rate": major(
                    matured(final.value_minor, days_old, CONVERSIONS)
                ),
            },
        }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "advertisers": advertisers(now),
        "campaigns": campaigns(as_of(now)),
        "report_campaign_daily": report_campaign_daily(now, first, skip),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
