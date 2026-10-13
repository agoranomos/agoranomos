"""demo_social: a fictional platform of social ads, in the shape of the Meta Marketing API v26.0:
the insights of each ad account at the level of campaigns, a row a day (time_increment=1), numbers
as strings, money in major units, and the actions and their values as lists of AdsActionStats with
the windows 1d_click, 7d_click and 1d_view, which dlt loads as child tables. Meta gives no 7d_view
or 28d_view since 12 January 2026, so the records have none (ADR-0025)."""

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


def attributed(total: int, days_old: int, curve: Curve, text) -> dict[str, str]:
    """An AdsActionStats of a final total: value, the default attribution of seven days after a
    click and one after a view, and its windows, each matured by the curve and written by text."""
    seven = total * 85 // 100
    view = total - seven
    one = seven * 60 // 100
    return {
        "value": text(matured(seven + view, days_old, curve)),
        "1d_click": text(matured(one, days_old, curve)),
        "7d_click": text(matured(seven, days_old, curve)),
        "1d_view": text(matured(view, days_old, curve)),
    }


def insights_campaign(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    name_day = as_of(now)
    for account, campaign, day, final, days_old in world.observed(PLATFORM, now, first, skip):
        actions, values = [], []
        for action_type, count, value, curve in action_finals(campaign, final):
            counted = attributed(count, days_old, curve, str)
            if counted["value"] != "0":
                actions.append({"action_type": action_type, **counted})
            if value:
                valued = attributed(value, days_old, curve, trimmed)
                if valued["value"] != "0":
                    values.append({"action_type": action_type, **valued})
        yield {
            "account_id": account.account_id,
            "account_name": account.name,
            "account_currency": account.currency,
            "campaign_id": campaign.campaign_id,
            "campaign_name": campaign.name_at(name_day),
            "date_start": day.isoformat(),
            "date_stop": day.isoformat(),
            "impressions": str(matured(final.impressions, days_old, DELIVERY)),
            # Every click, and those to the site, which the model counts (ADR-0025).
            "clicks": str(matured(final.clicks * 125 // 100, days_old, DELIVERY)),
            "inline_link_clicks": str(matured(final.clicks, days_old, DELIVERY)),
            "spend": trimmed(matured(final.spend_micros, days_old, DELIVERY) // 10_000),
            "attribution_setting": ATTRIBUTION,
            "actions": actions,
            "action_values": values,
        }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {
        "ad_accounts": ad_accounts(now),
        "campaigns": campaigns(as_of(now)),
        "insights_campaign": insights_campaign(now, first, skip),
    }


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    return world.windows(PLATFORM, now, first)


def name_versions(now: datetime) -> list[datetime]:
    return world.name_versions(PLATFORM, now)
