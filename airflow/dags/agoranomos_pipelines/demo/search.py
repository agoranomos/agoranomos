"""demo_search: a fictional platform of search ads, in the shape of the Google Ads API v25 as its
REST interface answers searchStream: lowerCamelCase keys, INT64 numbers as strings and DOUBLE
numbers as JSON numbers. Every row has the name of its campaign as of the load, a day without
metrics has no row, and the conversions by action come in a report of their own, as
segments.conversion_action cannot be selected with the metrics of cost (ADR-0025). The ad groups
(FROM ad_group) and the ads (FROM ad_group_ad) have the same two reports, and a Performance Max
campaign has neither (ADR-0034)."""

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


def child_status(campaign: world.Campaign, day: date) -> str:
    """The status of an ad group or an ad: removed with its campaign, enabled until then."""
    return "REMOVED" if campaign.removed(day) else "ENABLED"


def campaign_part(campaign: world.Campaign, day: date) -> dict[str, object]:
    return {
        "resourceName": f"customers/{campaign.account_id}/campaigns/{campaign.campaign_id}",
        "id": campaign.campaign_id,
        "name": campaign.name_at(day),
    }


def campaign_ref(campaign: world.Campaign) -> dict[str, object]:
    """The campaign of a row of a lower level, by its ID, which comes with its resource name."""
    return {
        "resourceName": f"customers/{campaign.account_id}/campaigns/{campaign.campaign_id}",
        "id": campaign.campaign_id,
    }


def ad_group_ref(group: world.AdGroup) -> dict[str, object]:
    return {
        "resourceName": f"customers/{group.account_id}/adGroups/{group.ad_group_id}",
        "id": group.ad_group_id,
    }


def ad_group_part(group: world.AdGroup) -> dict[str, object]:
    return {**ad_group_ref(group), "name": group.name}


def ad_part(group: world.AdGroup, ad: world.Ad) -> dict[str, object]:
    """The ad_group_ad of an ad, with its ad: a type of ad that has no name has no field name, as
    the REST interface leaves out a field without a value."""
    named = {} if ad.name is None else {"name": ad.name}
    return {
        "resourceName": (f"customers/{group.account_id}/adGroupAds/{group.ad_group_id}~{ad.ad_id}"),
        "ad": {
            "resourceName": f"customers/{group.account_id}/ads/{ad.ad_id}",
            "id": ad.ad_id,
            **named,
        },
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


def ad_groups(day: date) -> Iterator[dict[str, object]]:
    """The ad groups as a query FROM ad_group lists them on a day."""
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        yield {
            "customer": {"id": group.account_id},
            "campaign": campaign_ref(campaign),
            "adGroup": {
                **ad_group_part(group),
                "status": child_status(campaign, day),
                "type": group.kind,
            },
        }


def ads(day: date) -> Iterator[dict[str, object]]:
    """The ads as a query FROM ad_group_ad lists them on a day."""
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        for ad in group.ads:
            part = ad_part(group, ad)
            yield {
                "customer": {"id": group.account_id},
                "campaign": campaign_ref(campaign),
                "adGroup": ad_group_ref(group),
                "adGroupAd": {
                    **part,
                    "status": child_status(campaign, day),
                    "ad": {**part["ad"], "type": ad.kind},
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


Numbers = dict[tuple[str, str], int]


def measured(
    account_id: str, final: world.Final, days_old: int
) -> tuple[Numbers, frozenset[tuple[str, str]]]:
    """The numbers of a campaign on a day as a load sees them at an age: impressions, clicks and
    cost, and the conversions and their value of each conversion action; and those that are
    conversions, which the ad groups and the ads share by their conversions."""
    numbers = {
        ("impressions", ""): matured(final.impressions, days_old, DELIVERY),
        ("clicks", ""): matured(final.clicks, days_old, DELIVERY),
        ("cost", ""): matured(final.spend_micros, days_old, DELIVERY),
    }
    for (action_id, *_), c, v in action_finals(account_id, final):
        numbers[("conversions", action_id)] = matured(c, days_old, CONVERSIONS)
        numbers[("value", action_id)] = matured(v, days_old, CONVERSIONS)
    return numbers, frozenset(key for key in numbers if key[0] in ("conversions", "value"))


Level = tuple[dict[str, dict[str, object]], Numbers]


def levels(now, first, skip, level: str) -> Iterator[tuple[world.Account, date, Level]]:
    """Each row of a report at a level, campaign, ad_group or ad, before its metrics: the account,
    the day, the objects that name the row, and its numbers. The numbers of an ad group are its
    share of those of its campaign, and those of an ad its share of those of its ad group."""
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        numbers, converting = measured(account.account_id, final, days_old)
        named = {"customer": {"id": account.account_id}}
        if level == "campaign":
            yield account, day, ({**named, "campaign": campaign_part(campaign, name_day)}, numbers)
            continue
        named["campaign"] = campaign_ref(campaign)
        groups = world.ad_groups_of(campaign)
        for group, in_group in zip(
            groups, world.divided(numbers, converting, groups, day), strict=True
        ):
            if level == "ad_group":
                yield account, day, ({**named, "adGroup": ad_group_part(group)}, in_group)
                continue
            for ad, in_ad in zip(
                group.ads, world.divided(in_group, converting, group.ads, day), strict=True
            ):
                parts = {**named, "adGroup": ad_group_ref(group)}
                yield account, day, ({**parts, "adGroupAd": ad_part(group, ad)}, in_ad)


def counted(account_id: str, numbers: Numbers) -> tuple[int, int]:
    """The conversions, in hundredths, and their value of the actions that count in
    metrics.conversions."""
    actions = [action_id for action_id, _, _, counts, _ in ACTIONS[account_id] if counts]
    return (
        sum(numbers[("conversions", action_id)] for action_id in actions),
        sum(numbers[("value", action_id)] for action_id in actions),
    )


def stats(now, first=None, skip=frozenset(), level="campaign") -> Iterator[dict[str, object]]:
    for account, day, (named, numbers) in levels(now, first, skip, level):
        if not any(numbers.values()):
            continue  # a day without metrics has no row
        conversions, value = counted(account.account_id, numbers)
        yield {
            **named,
            "metrics": {
                "impressions": str(numbers[("impressions", "")]),
                "clicks": str(numbers[("clicks", "")]),
                "costMicros": str(numbers[("cost", "")]),
                "conversions": number(conversions),
                "conversionsValue": number(value),
            },
            "segments": {"date": day.isoformat()},
        }


def conversions(now, first=None, skip=frozenset(), level="campaign") -> Iterator[dict[str, object]]:
    for account, day, (named, numbers) in levels(now, first, skip, level):
        for action_id, name, category, counts, _ in ACTIONS[account.account_id]:
            converted, value = numbers[("conversions", action_id)], numbers[("value", action_id)]
            if converted == 0:
                continue  # rows with zero conversions are excluded
            resource = f"customers/{account.account_id}/conversionActions/{action_id}"
            yield {
                **named,
                "segments": {
                    "date": day.isoformat(),
                    "conversionAction": resource,
                    "conversionActionName": name,
                    "conversionActionCategory": category,
                },
                "metrics": {
                    "conversions": number(converted if counts else 0),
                    "conversionsValue": number(value if counts else 0),
                    "allConversions": number(converted),
                    "allConversionsValue": number(value),
                },
            }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "customer_clients": customer_clients(now),
        "campaigns": campaigns(as_of(now)),
        "campaign_stats": stats(now, first, skip),
        "campaign_conversions": conversions(now, first, skip),
        "ad_groups": ad_groups(as_of(now)),
        "ad_group_stats": stats(now, first, skip, "ad_group"),
        "ad_group_conversions": conversions(now, first, skip, "ad_group"),
        "ads": ads(as_of(now)),
        "ad_stats": stats(now, first, skip, "ad"),
        "ad_conversions": conversions(now, first, skip, "ad"),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
