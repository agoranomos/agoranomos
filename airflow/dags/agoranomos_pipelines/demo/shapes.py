"""The shape of each record that a fictional platform of the demo yields: the fields of the JSON
of the API that it mimics, by path, with the type of the JSON value of each field, and the version
of that API. A list holds records of the fields of its own shape, and a path that ends in ? is a
field that a record may leave out. One place says what each platform sends, so that a test checks
every record against it (ADR-0025)."""

from __future__ import annotations

from typing import Any

STR, INT, FLOAT, BOOL = "string", "integer", "number", "boolean"

API = {
    "demo_search": "Google Ads API v25",
    "demo_social": "Meta Marketing API v26.0",
    "demo_video": "TikTok API for Business v1.3",
    "demo_web": "GA4 Data API v1beta",
    "demo_fx": "a source of euro reference rates",
}
# An AdsActionStats of Meta with the windows that the demo asks for: Meta gives no 7d_view or
# 28d_view since 12 January 2026.
ACTION = {"action_type": STR, "value": STR, "1d_click": STR, "7d_click": STR, "1d_view": STR}

# The parts of the rows of Google that name a campaign, an ad group and an ad by their IDs, and
# their metrics: a type of ad that Google gives no name has no field name.
SEARCH_CAMPAIGN = {"customer.id": STR, "campaign.resourceName": STR, "campaign.id": STR}
SEARCH_AD_GROUP = {**SEARCH_CAMPAIGN, "adGroup.resourceName": STR, "adGroup.id": STR}
SEARCH_AD = {
    **SEARCH_AD_GROUP,
    "adGroupAd.resourceName": STR,
    "adGroupAd.ad.resourceName": STR,
    "adGroupAd.ad.id": STR,
    "adGroupAd.ad.name?": STR,
}
SEARCH_STATS = {
    "metrics.impressions": STR,
    "metrics.clicks": STR,
    "metrics.costMicros": STR,
    "metrics.conversions": FLOAT,
    "metrics.conversionsValue": FLOAT,
    "segments.date": STR,
}
SEARCH_CONVERSIONS = {
    "segments.date": STR,
    "segments.conversionAction": STR,
    "segments.conversionActionName": STR,
    "segments.conversionActionCategory": STR,
    "metrics.conversions": FLOAT,
    "metrics.conversionsValue": FLOAT,
    "metrics.allConversions": FLOAT,
    "metrics.allConversionsValue": FLOAT,
}
# The fields of the insights of Meta at every level, after those that name the level.
SOCIAL_INSIGHTS = {
    "date_start": STR,
    "date_stop": STR,
    "impressions": STR,
    "clicks": STR,
    "inline_link_clicks": STR,
    "spend": STR,
    "attribution_setting": STR,
    "actions": [ACTION],
    "action_values": [ACTION],
}
SOCIAL_ACCOUNT = {"account_id": STR, "account_name": STR, "account_currency": STR}
# The metrics of the reports of TikTok at every level, after the attributes of the level.
VIDEO_METRICS = {
    "metrics.currency": STR,
    "metrics.spend": STR,
    "metrics.impressions": STR,
    "metrics.clicks": STR,
    "metrics.conversion": STR,
    "metrics.cost_per_conversion": STR,
    "metrics.real_time_conversion": STR,
    "metrics.complete_payment": STR,
    "metrics.total_complete_payment_rate": STR,
}

SHAPES: dict[tuple[str, str], dict[str, Any]] = {
    ("demo_search", "customer_clients"): {
        "customerClient.resourceName": STR,
        "customerClient.clientCustomer": STR,
        "customerClient.id": STR,
        "customerClient.descriptiveName": STR,
        "customerClient.currencyCode": STR,
        "customerClient.timeZone": STR,
        "customerClient.manager": BOOL,
        "customerClient.level": STR,
        "customerClient.status": STR,
    },
    ("demo_search", "campaigns"): {
        "customer.id": STR,
        "campaign.resourceName": STR,
        "campaign.id": STR,
        "campaign.name": STR,
        "campaign.status": STR,
        "campaign.advertisingChannelType": STR,
    },
    ("demo_search", "campaign_stats"): {**SEARCH_CAMPAIGN, "campaign.name": STR, **SEARCH_STATS},
    ("demo_search", "campaign_conversions"): {
        **SEARCH_CAMPAIGN,
        "campaign.name": STR,
        **SEARCH_CONVERSIONS,
    },
    ("demo_search", "ad_groups"): {
        **SEARCH_AD_GROUP,
        "adGroup.name": STR,
        "adGroup.status": STR,
        "adGroup.type": STR,
    },
    ("demo_search", "ad_group_stats"): {**SEARCH_AD_GROUP, "adGroup.name": STR, **SEARCH_STATS},
    ("demo_search", "ad_group_conversions"): {
        **SEARCH_AD_GROUP,
        "adGroup.name": STR,
        **SEARCH_CONVERSIONS,
    },
    ("demo_search", "ads"): {**SEARCH_AD, "adGroupAd.status": STR, "adGroupAd.ad.type": STR},
    ("demo_search", "ad_stats"): {**SEARCH_AD, **SEARCH_STATS},
    ("demo_search", "ad_conversions"): {**SEARCH_AD, **SEARCH_CONVERSIONS},
    ("demo_social", "ad_accounts"): {
        "id": STR,
        "account_id": STR,
        "name": STR,
        "currency": STR,
        "timezone_name": STR,
        "timezone_offset_hours_utc": FLOAT,
        "account_status": INT,
    },
    ("demo_social", "campaigns"): {
        "id": STR,
        "account_id": STR,
        "name": STR,
        "status": STR,
        "objective": STR,
    },
    ("demo_social", "insights_campaign"): {
        **SOCIAL_ACCOUNT,
        "campaign_id": STR,
        "campaign_name": STR,
        **SOCIAL_INSIGHTS,
    },
    ("demo_social", "adsets"): {
        "id": STR,
        "account_id": STR,
        "campaign_id": STR,
        "name": STR,
        "status": STR,
        "optimization_goal": STR,
    },
    ("demo_social", "insights_adset"): {
        **SOCIAL_ACCOUNT,
        "campaign_id": STR,
        "adset_id": STR,
        "adset_name": STR,
        **SOCIAL_INSIGHTS,
    },
    ("demo_social", "ads"): {
        "id": STR,
        "account_id": STR,
        "campaign_id": STR,
        "adset_id": STR,
        "name": STR,
        "status": STR,
    },
    ("demo_social", "insights_ad"): {
        **SOCIAL_ACCOUNT,
        "campaign_id": STR,
        "adset_id": STR,
        "ad_id": STR,
        "ad_name": STR,
        **SOCIAL_INSIGHTS,
    },
    ("demo_video", "advertisers"): {
        "advertiser_id": STR,
        "name": STR,
        "currency": STR,
        "timezone": STR,
        "display_timezone": STR,
        "status": STR,
    },
    # The names of the fields of /campaign/get/ v1.3, as the documentation of TikTok gives them.
    ("demo_video", "campaigns"): {
        "advertiser_id": STR,
        "campaign_id": STR,
        "campaign_name": STR,
        "objective_type": STR,
        "operation_status": STR,
    },
    ("demo_video", "report_campaign_daily"): {
        "advertiser_id": STR,
        "dimensions.campaign_id": STR,
        "dimensions.stat_time_day": STR,
        "metrics.campaign_name": STR,
        **VIDEO_METRICS,
    },
    ("demo_video", "adgroups"): {
        "advertiser_id": STR,
        "campaign_id": STR,
        "adgroup_id": STR,
        "adgroup_name": STR,
        "optimization_goal": STR,
        "operation_status": STR,
    },
    ("demo_video", "report_adgroup_daily"): {
        "advertiser_id": STR,
        "dimensions.adgroup_id": STR,
        "dimensions.stat_time_day": STR,
        "metrics.campaign_id": STR,
        "metrics.adgroup_name": STR,
        **VIDEO_METRICS,
    },
    ("demo_video", "ads"): {
        "advertiser_id": STR,
        "campaign_id": STR,
        "adgroup_id": STR,
        "ad_id": STR,
        "ad_name": STR,
        "ad_format": STR,
        "operation_status": STR,
    },
    ("demo_video", "report_ad_daily"): {
        "advertiser_id": STR,
        "dimensions.ad_id": STR,
        "dimensions.stat_time_day": STR,
        "metrics.campaign_id": STR,
        "metrics.adgroup_id": STR,
        "metrics.ad_name": STR,
        **VIDEO_METRICS,
    },
    # A property as the GA4 Admin API v1beta gives it.
    ("demo_web", "properties"): {
        "name": STR,
        "displayName": STR,
        "timeZone": STR,
        "currencyCode": STR,
    },
    # A row of runReport as a dictionary of the names of its dimensions and metrics, as a
    # connector makes it, with the property that the connector asked for.
    ("demo_web", "report_sessions_daily"): {
        "property": STR,
        "date": STR,
        "sessionSource": STR,
        "sessionMedium": STR,
        "sessionCampaignName": STR,
        "sessionCampaignId": STR,
        "sessionDefaultChannelGroup": STR,
        "sessions": STR,
        "engagedSessions": STR,
        "keyEvents": STR,
        "totalRevenue": STR,
    },
    ("demo_fx", "rates_daily"): {
        "rate_date": STR,
        "base": STR,
        "quote": STR,
        "rate": STR,
    },
}


def json_type(value: Any) -> str:
    if isinstance(value, bool):
        return BOOL
    if isinstance(value, int):
        return INT
    if isinstance(value, float):
        return FLOAT
    if isinstance(value, str):
        return STR
    return type(value).__name__


def flatten(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """The fields of a record by path; a list stays a value of its path."""
    found = {}
    for key, value in record.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            found |= flatten(value, f"{path}.")
        else:
            found[path] = value
    return found


def problems(platform: str, table: str, record: dict[str, Any]) -> list[str]:
    """What a record lacks or has besides the shape of its table, and the fields of another type."""
    optional = {path.removesuffix("?") for path in SHAPES[(platform, table)] if path.endswith("?")}
    shape = {path.removesuffix("?"): kind for path, kind in SHAPES[(platform, table)].items()}
    fields = flatten(record)
    found = [f"missing {path}" for path in shape if path not in fields and path not in optional]
    found += [f"extra {path}" for path in fields if path not in shape]
    for path, kind in shape.items():
        if path not in fields:
            continue
        value = fields[path]
        if isinstance(kind, list):
            if not isinstance(value, list):
                found.append(f"{path} is not a list")
                continue
            for index, item in enumerate(value):
                found += [f"{path}[{index}]: {p}" for p in item_problems(kind[0], item)]
        elif json_type(value) != kind:
            found.append(f"{path} is {json_type(value)}, not {kind}")
    return found


def item_problems(shape: dict[str, str], item: dict[str, Any]) -> list[str]:
    found = [f"missing {k}" for k in shape if k not in item]
    found += [f"extra {k}" for k in item if k not in shape]
    found += [
        f"{k} is {json_type(v)}" for k, v in item.items() if k in shape and json_type(v) != shape[k]
    ]
    return found
