"""demo_search: a fictional platform of search ads, in the shape of the Google Ads API v25 as its
REST interface answers searchStream: lowerCamelCase keys, INT64 numbers as strings and DOUBLE
numbers as JSON numbers. Every row has the name of its campaign as of the load, a day without
metrics has no row, and the conversions by action come in a report of their own, as
segments.conversion_action cannot be selected with the metrics of cost (ADR-0025)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import DELIVERY, matured

PLATFORM = "demo_search"
API = "Google Ads API v25"
# Conversions are dated by the click, and the windows after a click reach 90 days.
CONVERSIONS = ((1, 600), (2, 700), (7, 850), (14, 920), (30, 970), (60, 990), (90, 1000))
# The conversion actions of each account: ID, name, category, whether it counts in
# metrics.conversions (include_in_conversions_metric), and its share in per mille: of the
# conversions of a campaign for an action that counts, of its clicks for one that does not.
ACTIONS = {
    "4170000011": (
        ("9001", "Purchase", "PURCHASE", True, 800),
        ("9002", "Lead form", "SUBMIT_LEAD_FORM", True, 200),
        ("9003", "Calls from ads", "PHONE_CALL_LEAD", False, 20),
    ),
    "4170000012": (
        ("9101", "Kauf", "PURCHASE", True, 1000),
        ("9103", "Anrufe", "PHONE_CALL_LEAD", False, 15),
    ),
}
# What a lead of the lead form is worth, in cents of the currency of the account.
LEAD_VALUE_MINOR = 2_000


def accounts() -> tuple[world.Account, ...]:
    return world.accounts_of(PLATFORM)


def as_of(now: datetime) -> date:
    """The day whose state of the campaigns a load reads: the day of now in UTC."""
    return now.astimezone(UTC).date()


def status(campaign: world.Campaign, day: date) -> str:
    if campaign.removed(day):
        return "REMOVED"
    return "ENABLED" if campaign.active(day) else "PAUSED"


def campaign_part(campaign: world.Campaign, day: date) -> dict[str, object]:
    return {
        "resourceName": f"customers/{campaign.account_id}/campaigns/{campaign.campaign_id}",
        "id": campaign.campaign_id,
        "name": campaign.name_at(day),
    }


def customer_clients(now: datetime) -> Iterator[dict[str, object]]:
    """The hierarchy under the manager account, as customer_client lists it: the manager itself at
    level 0, and its client accounts at level 1."""
    manager = world.MANAGER
    for account, level in ((manager, "0"), *((account, "1") for account in accounts())):
        link = f"{manager.account_id}/customerClients/{account.account_id}"
        yield {
            "customerClient": {
                "resourceName": f"customers/{link}",
                "clientCustomer": f"customers/{account.account_id}",
                "id": account.account_id,
                "descriptiveName": account.name,
                "currencyCode": account.currency,
                "timeZone": account.time_zone,
                "manager": account is manager,
                "level": level,
                "status": "ENABLED",
            }
        }


def campaigns(day: date) -> Iterator[dict[str, object]]:
    """The campaigns as a query FROM campaign lists them on a day, the removed ones too."""
    for campaign in world.listed_campaigns(PLATFORM, day):
        yield {
            "customer": {"id": campaign.account_id},
            "campaign": {
                **campaign_part(campaign, day),
                "status": status(campaign, day),
                "advertisingChannelType": campaign.kind,
            },
        }


def action_finals(account_id: str, final: world.Final) -> Iterator[tuple[tuple, int, int]]:
    """Each conversion action of an account with its final conversions of a campaign and day, in
    hundredths, and their final value, in cents."""
    for action in ACTIONS[account_id]:
        _, _, category, counts, share = action
        conversions = (
            final.conversions_hundredths * share // 1000
            if counts
            else final.clicks * share * 100 // 1000
        )
        if category == "PURCHASE":
            value = final.value_minor * share // 1000
        elif category == "SUBMIT_LEAD_FORM":
            value = conversions * LEAD_VALUE_MINOR // 100
        else:
            value = 0
        yield action, conversions, value


def number(hundredths: int) -> float:
    """A DOUBLE of the API, from hundredths."""
    return hundredths / 100


def campaign_stats(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        counted = [
            (matured(c, days_old, CONVERSIONS), matured(v, days_old, CONVERSIONS))
            for (_, _, _, counts, _), c, v in action_finals(account.account_id, final)
            if counts
        ]
        yield {
            "customer": {"id": account.account_id},
            "campaign": campaign_part(campaign, name_day),
            "metrics": {
                "impressions": str(matured(final.impressions, days_old, DELIVERY)),
                "clicks": str(matured(final.clicks, days_old, DELIVERY)),
                "costMicros": str(matured(final.spend_micros, days_old, DELIVERY)),
                "conversions": number(sum(c for c, _ in counted)),
                "conversionsValue": number(sum(v for _, v in counted)),
            },
            "segments": {"date": day.isoformat()},
        }


def campaign_conversions(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        for (action_id, name, category, counts, _), c, v in action_finals(
            account.account_id, final
        ):
            conversions, value = (
                matured(c, days_old, CONVERSIONS),
                matured(v, days_old, CONVERSIONS),
            )
            if conversions == 0:
                continue  # rows with zero conversions are excluded
            resource = f"customers/{account.account_id}/conversionActions/{action_id}"
            yield {
                "customer": {"id": account.account_id},
                "campaign": campaign_part(campaign, name_day),
                "segments": {
                    "date": day.isoformat(),
                    "conversionAction": resource,
                    "conversionActionName": name,
                    "conversionActionCategory": category,
                },
                "metrics": {
                    "conversions": number(conversions if counts else 0),
                    "conversionsValue": number(value if counts else 0),
                    "allConversions": number(conversions),
                    "allConversionsValue": number(value),
                },
            }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "customer_clients": customer_clients(now),
        "campaigns": campaigns(as_of(now)),
        "campaign_stats": campaign_stats(now, first, skip),
        "campaign_conversions": campaign_conversions(now, first, skip),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
