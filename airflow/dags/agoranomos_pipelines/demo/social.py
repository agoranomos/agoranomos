"""demo_social: a fictional platform of social ads, in the shape of the Meta Marketing API v26.0:
the insights of each ad account at the level of campaigns, a row a day (time_increment=1), numbers
as strings, money in major units, and the actions and their values as lists of AdsActionStats with
the windows 1d_click, 7d_click and 1d_view, which dlt loads as child tables. Meta gives no 7d_view
or 28d_view since 12 January 2026, so the records have none (ADR-0025). The insights at the levels
of ad sets and of ads have the same shape, with the IDs and the name of their level, and the ad
sets and the ads are listed like the campaigns (ADR-0034)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import DELIVERY, Curve, matured, trimmed

PLATFORM = "demo_social"
API = "Meta Marketing API v26.0"
# Meta revises conversions for 28 days, and its estimate of the first week runs above the final
# count, so the numbers of a day go down again in its second week.
CONVERSIONS: Curve = ((1, 700), (3, 900), (7, 1040), (14, 1010), (28, 1000))
# The attribution setting of every ad set of the demo: seven days after a click and one after a
# view [unverified: the exact text that Meta writes in this field].
ATTRIBUTION = "7d_click_1d_view"


def accounts() -> tuple[world.Account, ...]:
    return world.accounts_of(PLATFORM)


def as_of(now: datetime) -> date:
    return now.astimezone(UTC).date()


def status(campaign: world.Campaign, day: date) -> str:
    if campaign.removed(day):
        return "DELETED"
    return "ACTIVE" if campaign.active(day) else "PAUSED"


def child_status(campaign: world.Campaign, day: date) -> str:
    """The status of an ad set or an ad: deleted with its campaign, active until then."""
    return "DELETED" if campaign.removed(day) else "ACTIVE"


def ad_accounts(now: datetime) -> Iterator[dict[str, object]]:
    for account in accounts():
        offset = now.astimezone(ZoneInfo(account.time_zone)).utcoffset()
        yield {
            "id": f"act_{account.account_id}",
            "account_id": account.account_id,
            "name": account.name,
            "currency": account.currency,
            "timezone_name": account.time_zone,
            "timezone_offset_hours_utc": offset.total_seconds() / 3600,
            "account_status": 1,  # ACTIVE
        }


def campaigns(day: date) -> Iterator[dict[str, object]]:
    for campaign in world.listed_campaigns(PLATFORM, day):
        yield {
            "id": campaign.campaign_id,
            "account_id": campaign.account_id,
            "name": campaign.name_at(day),
            "status": status(campaign, day),
            "objective": campaign.kind,
        }


def adsets(day: date) -> Iterator[dict[str, object]]:
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        yield {
            "id": group.ad_group_id,
            "account_id": group.account_id,
            "campaign_id": campaign.campaign_id,
            "name": group.name,
            "status": child_status(campaign, day),
            "optimization_goal": group.kind,
        }


def ads(day: date) -> Iterator[dict[str, object]]:
    for campaign, group in world.listed_ad_groups(PLATFORM, day):
        for ad in group.ads:
            yield {
                "id": ad.ad_id,
                "account_id": group.account_id,
                "campaign_id": campaign.campaign_id,
                "adset_id": group.ad_group_id,
                "name": ad.name,
                "status": child_status(campaign, day),
            }


def action_finals(
    campaign: world.Campaign, final: world.Final
) -> list[tuple[str, int, int, Curve]]:
    """The actions of a campaign on a day: their type, final count, final value in cents, and the
    curve by which they mature. A lead campaign has leads, any other purchases, counted twice: by
    the pixel, and across the surfaces of Meta as omni_purchase."""
    link, purchases = final.clicks, final.conversions_hundredths // 100
    found = [
        ("link_click", link, 0, DELIVERY),
        ("landing_page_view", link * 80 // 100, 0, DELIVERY),
    ]
    if campaign.kind == "OUTCOME_LEADS":
        found.append(("offsite_conversion.fb_pixel_lead", purchases, 0, CONVERSIONS))
    else:
        found += [
            ("offsite_conversion.fb_pixel_add_to_cart", purchases * 3, 0, CONVERSIONS),
            ("offsite_conversion.fb_pixel_purchase", purchases, final.value_minor, CONVERSIONS),
            ("omni_purchase", purchases, final.value_minor, CONVERSIONS),
        ]
    return found


WINDOWS = ("value", "1d_click", "7d_click", "1d_view")


def attributed(total: int, days_old: int, curve: Curve) -> dict[str, int]:
    """The windows of an AdsActionStats of a final total: value, the default attribution of seven
    days after a click and one after a view, and its windows, each matured by the curve."""
    seven = total * 85 // 100
    view = total - seven
    one = seven * 60 // 100
    return {
        "value": matured(seven + view, days_old, curve),
        "1d_click": matured(one, days_old, curve),
        "7d_click": matured(seven, days_old, curve),
        "1d_view": matured(view, days_old, curve),
    }


Numbers = dict[tuple[str, str, str], int]


def measured(
    campaign: world.Campaign, final: world.Final, days_old: int
) -> tuple[Numbers, frozenset[tuple[str, str, str]]]:
    """The numbers of a campaign on a day as a load sees them at an age: impressions, clicks,
    spend in cents, and the count and the value of each action in each window; and those of the
    actions that mature as conversions, which the ad sets and the ads share by their conversions."""
    numbers = {
        ("impressions", "", ""): matured(final.impressions, days_old, DELIVERY),
        # Every click, and those to the site, which the model counts (ADR-0025).
        ("clicks", "", ""): matured(final.clicks * 125 // 100, days_old, DELIVERY),
        ("inline_link_clicks", "", ""): matured(final.clicks, days_old, DELIVERY),
        ("spend", "", ""): matured(final.spend_micros, days_old, DELIVERY) // 10_000,
    }
    converting = set()
    for action_type, count, value, curve in action_finals(campaign, final):
        for kind, total in (("count", count), ("value", value)):
            for window, number in attributed(total, days_old, curve).items():
                numbers[(kind, action_type, window)] = number
                if curve is CONVERSIONS:
                    converting.add((kind, action_type, window))
    return numbers, frozenset(converting)


def stats_list(numbers: Numbers, kind: str, text) -> list[dict[str, str]]:
    """The AdsActionStats of the actions of a kind, count or value, that have a value."""
    found = []
    for action_type in dict.fromkeys(a for k, a, _ in numbers if k == kind):
        if numbers[(kind, action_type, "value")]:
            windows = {w: text(numbers[(kind, action_type, w)]) for w in WINDOWS}
            found.append({"action_type": action_type, **windows})
    return found


def insights(now, first=None, skip=frozenset(), level="campaign") -> Iterator[dict[str, object]]:
    """The insights of a level, campaign, adset or ad, one row a day. The numbers of an ad set are
    its share of those of its campaign, and those of an ad its share of those of its ad set."""
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        numbers, converting = measured(campaign, final, days_old)
        found = [({"campaign_name": campaign.name_at(name_day)}, numbers)]
        if level != "campaign":
            groups = world.ad_groups_of(campaign)
            found = [
                ({"adset_id": group.ad_group_id, "adset_name": group.name}, in_group, group)
                for group, in_group in zip(
                    groups, world.divided(numbers, converting, groups, day), strict=True
                )
            ]
        if level == "ad":
            found = [
                ({"adset_id": named["adset_id"], "ad_id": ad.ad_id, "ad_name": ad.name}, in_ad)
                for named, in_group, group in found
                for ad, in_ad in zip(
                    group.ads, world.divided(in_group, converting, group.ads, day), strict=True
                )
            ]
        for named, part, *_ in found:
            yield {
                "account_id": account.account_id,
                "account_name": account.name,
                "account_currency": account.currency,
                "campaign_id": campaign.campaign_id,
                **named,
                "date_start": day.isoformat(),
                "date_stop": day.isoformat(),
                "impressions": str(part[("impressions", "", "")]),
                "clicks": str(part[("clicks", "", "")]),
                "inline_link_clicks": str(part[("inline_link_clicks", "", "")]),
                "spend": trimmed(part[("spend", "", "")]),
                "attribution_setting": ATTRIBUTION,
                "actions": stats_list(part, "count", str),
                "action_values": stats_list(part, "value", trimmed),
            }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "ad_accounts": ad_accounts(now),
        "campaigns": campaigns(as_of(now)),
        "insights_campaign": insights(now, first, skip),
        "adsets": adsets(as_of(now)),
        "insights_adset": insights(now, first, skip, "adset"),
        "ads": ads(as_of(now)),
        "insights_ad": insights(now, first, skip, "ad"),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
