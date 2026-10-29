"""The dashboard Advertising of Agoranomos: one dashboard with the tabs Overview, Platforms,
Campaigns, Ad groups and ads, and Conversions, which metabase-init keeps in the collection
Agoranomos / Advertising as the code says, on every start, with or without the demo, and demo-init
after the first run of the transform (content.py, ADR-0018, ADR-0034).

Its questions are native SQL on the marts, each with the filters of the dashboard that its mart
has: the period, the time grouping of a chart over time, the client, and, on the marts of the ad
platforms, the platform, the account and the campaign, which the client, the platform and the
account limit as linked filters. The time grouping changes how a question groups its days, as a
template tag of the type temporal-unit, and filters nothing. A KPI is the ratio of sums that the
marts declare for the metrics of Metabase (metrics.py), written into the SQL from the same
declaration, so every card computes it as the metric does. The overview leads with the KPIs,
each compared with the period before by the grouping of time, then the trend, the platforms, the
clients and the top campaigns; a click on a campaign opens the tab of ad groups and ads with the
filter of the campaign set. Its card of the loads reads the view ops.ops_loads, as the Ops
collection does (ops.py).

The questions need the fields of the marts for their filters: until Metabase shows the marts,
before the first run of the transform, the dashboard waits, and a later start makes it.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

import content
import metadata
import metrics
import ops

ADVERTISING_KEY = "advertising"
ADVERTISING_NAME = "Advertising"
ADVERTISING_DESCRIPTION = (
    "The advertising of the business on every ad platform: the dashboard Advertising, with its "
    "KPIs, platforms, campaigns, ad groups and ads, and conversions."
)
DASHBOARD = "advertising"
DASHBOARD_NAME = "Advertising"
DASHBOARD_DESCRIPTION = (
    "The spend, the results and the KPIs of the ad platforms in the reporting currency: an "
    "overview, then the platforms, the campaigns, the ad groups and ads, and the conversions by "
    "action, for the period, the client, the platform, the account and the campaign of its filters."
)
OVERVIEW, PLATFORMS, CAMPAIGNS, AD_GROUPS_AND_ADS, CONVERSIONS = TABS = (
    "Overview",
    "Platforms",
    "Campaigns",
    "Ad groups and ads",
    "Conversions",
)

MART_SCHEMA = metrics.MART_SCHEMA
AD_STATS = "ad_stats_campaign_daily"
AD_GROUP_STATS = "ad_stats_ad_group_daily"
AD_STATS_OF_ADS = "ad_stats_ad_daily"
AD_CONVERSIONS = "ad_conversions_campaign_daily"
WEB_SESSIONS = "web_sessions_daily"
# The mart whose metrics give the KPIs, and their slugs: the marts of ad groups and of ads declare
# the same ones.
KPI_MART = AD_STATS
KPIS = ("ctr", "cpc", "cpm", "cost_per_conversion", "conversion_rate", "roas")

# The filters of the questions, by the names of their template tags: their display names, the type
# of their widget, and the column of the mart that each is on. A dashboard filter of the same slug
# sets each of them. The time grouping is no filter of rows: it groups the days of report_date.
FILTERS = {
    "period": ("Period", "date/all-options", "report_date"),
    "unit": ("Time grouping", content.TEMPORAL_UNIT, "report_date"),
    "client": ("Client", "string/=", "client_name"),
    "platform": ("Platform", "string/=", "platform_name"),
    "account": ("Account", "string/=", "account_name"),
    "campaign": ("Campaign", "string/=", "campaign_name"),
}
# The filters of the dashboard. The period starts as the 12 weeks before this one and the grouping
# as the week, which starts on Monday (metabase_init.WEEK_START): each KPI then compares the last
# whole week with the one before. The account offers the accounts of the client and the platform
# that the filters name, and the campaign those of the account too.
DASHBOARD_FILTERS = (
    content.DashboardFilter("period", "Period", "date/all-options", "date", "past12weeks"),
    content.DashboardFilter(
        "unit", "Time grouping", content.TEMPORAL_UNIT, "temporal-unit", "week"
    ),
    content.DashboardFilter("client", "Client", "string/=", "string"),
    content.DashboardFilter("platform", "Platform", "string/=", "string"),
    content.DashboardFilter(
        "account", "Account", "string/=", "string", filtering=("client", "platform")
    ),
    content.DashboardFilter(
        "campaign", "Campaign", "string/=", "string", filtering=("client", "platform", "account")
    ),
)
# The filters that the rows of each mart can take. The sessions of web analytics have a client,
# but no platform, account or campaign of an ad platform of their own.
AD_FILTERS = ("period", "client", "platform", "account", "campaign")
MART_FILTERS = {
    AD_STATS: AD_FILTERS,
    AD_GROUP_STATS: AD_FILTERS,
    AD_STATS_OF_ADS: AD_FILTERS,
    AD_CONVERSIONS: AD_FILTERS,
    WEB_SESSIONS: ("period", "client"),
}

# The types that Metabase 0.64.1 gives the columns of the answers, each time that it computes their
# metadata (read on the stand in October 2026): the contracts of the marts fix the types of the
# columns that the questions return as they are, the day stays a date when no grouping of time is
# set, and every sum or ratio is numeric in PostgreSQL.
TEXT = ("type/Text", "text")
NUMBER = ("type/Decimal", "numeric")
COLUMN_TYPES = {
    "report_date": ("type/Date", "date"),
    "is_primary": ("type/Boolean", "bool"),
    **dict.fromkeys(
        (
            "platform_name",
            "client_name",
            "account_name",
            "campaign_name",
            "ad_group_name",
            "ad_name",
            "conversion_action_name",
            "conversion_category",
            "attribution_setting",
        ),
        TEXT,
    ),
    **{name: ops.COLUMN_TYPES[name] for name in ("platform", "account_id")},
    **{name: ops.COLUMN_TYPES[name] for name in ("last_day_loaded", "last_loaded_at")},
}
# The semantic types that Metabase 0.64.1 gives the columns of the answers by their names
# (src/metabase/analyze/classifiers/name.clj at the tag v0.64.1): a decimal whose name ends in rate
# is a share, and one whose name has cost is a cost.
SEMANTIC_TYPES = {"conversion_rate": "type/Share", "cost_per_conversion": "type/Cost"}
# The titles of the columns that no mart has: those of the loads, as the Ops collection gives them.
OTHER_TITLES = {
    name: ops.TITLES[name] for name in ("account_id", "last_day_loaded", "last_loaded_at")
}
OTHER_TITLES["platform"] = "Platform Code"
# Money in the reporting currency, and the decimals of the other numbers of the marts.
MONEY = frozenset({"spend_reporting", "conversion_value_reporting"})
DECIMALS = {"conversions": 0, "key_events": 0}

# The texts of the cards of text, as facts for people and AI clients.
ABOUT = (
    "**About these numbers.** Money is in the reporting currency. The platforms still change the "
    "statistics of the last days, above all the conversions, which they date by the click, so a "
    "recent day can rise after it first shows. The conversions are those of the primary conversion "
    "actions, as each platform counts them, so a sum across platforms can count one conversion "
    "more than once. The key events come from web analytics, which dates each by its event and "
    "credits it to one source, so they differ from the conversions of the platforms."
)
WITHOUT_PERFORMANCE_MAX = (
    "The ad groups and the ads come from reports of their own of each platform. Performance Max "
    "campaigns of Google have neither, so the totals of this tab leave them out and can be lower "
    "than those of the campaigns."
)
ATTRIBUTION = (
    "**How the conversions are attributed.** Each platform credits a conversion to its own ads, "
    "by the attribution setting of the conversion action, and dates it by the click or the view "
    "that led to it. The totals of the dashboard count the primary actions only; this table shows "
    "every action, primary or not."
)


def sql(*lines: str) -> str:
    return "\n".join(line for line in lines if line)


def where(*filters: str) -> str:
    """The WHERE clause of a question: the template tag of each filter, which Metabase replaces."""
    return "WHERE " + " AND ".join(f"{{{{{name}}}}}" for name in filters)


def totals(*columns: str) -> list[str]:
    """The sum of each column, under its own name."""
    return [f"sum({column}) AS {column}" for column in columns]


def ratio(metric: metrics.Declared) -> str:
    """The SQL of a KPI, as its metric computes it: the sum of its numerator divided by the sum of
    its denominator, times its scale, with no value where the sum of the denominator is 0."""
    scale = f" * {metric.scale}" if metric.scale != 1 else ""
    return f"sum({metric.numerator}) / nullif(sum({metric.denominator}), 0){scale} AS {metric.slug}"


def over_time(mart: str, *selected: str, by: str = "") -> str:
    """A question of a mart over time: its columns by the time grouping, and by a column."""
    group = ", ".join(["{{unit}}", *([by] if by else [])])
    return sql(
        "SELECT " + ", ".join(["{{unit}} AS report_date", *([by] if by else []), *selected]),
        f"FROM {MART_SCHEMA}.{mart}",
        where(*MART_FILTERS[mart]),
        f"GROUP BY {group}",
        f"ORDER BY {group}",
    )


def grouped(
    mart: str, keys: tuple[str, ...], selected: list[str], order: str, limit: int = 0
) -> str:
    """A question of a mart by some of its columns, in an order."""
    return sql(
        "SELECT " + ", ".join([*keys, *selected]),
        f"FROM {MART_SCHEMA}.{mart}",
        where(*MART_FILTERS[mart]),
        f"GROUP BY {', '.join(keys)}",
        f"ORDER BY {order}",
        f"LIMIT {limit}" if limit else "",
    )


def questions(kpis: Mapping[str, metrics.Declared]) -> tuple[dict[str, Any], ...]:
    """The questions of the dashboard, by tab and in the order of its cards, with their place on
    its grid of 24 columns, from the KPIs that the marts declare, by their slugs. key is the key of
    a question in the package, which the registry of the content keeps; mart is the mart that it
    reads, whose display names give the titles of its columns, and whose filters it takes; and
    columns are the columns of the answer, in their order. A card of text has a text and no key.
    The descriptions say what each counts, as facts for people and AI clients."""
    ctr, cpc, cpm = kpis["ctr"], kpis["cpc"], kpis["cpm"]
    cpa, rate, roas = kpis["cost_per_conversion"], kpis["conversion_rate"], kpis["roas"]
    results = [*totals("spend_reporting", "impressions", "clicks", "conversions")]
    kpi_columns = ("ctr", "cpc", "cost_per_conversion", "roas")
    kpi_sql = [ratio(k) for k in (ctr, cpc, cpa, roas)]
    measures = ("spend_reporting", "impressions", "clicks", "conversions", *kpi_columns)

    def trend(key: str, name: str, description: str, mart: str, column: str, select: str, at: int):
        return {
            "tab": OVERVIEW,
            "key": key,
            "name": name,
            "description": description + " Each period of the time grouping, the last compared "
            "with the one before.",
            "display": "smartscalar",
            "mart": mart,
            "columns": ("report_date", column),
            "query": over_time(mart, select),
            "layout": {"row": 3 * (at // 4), "col": 6 * (at % 4), "size_x": 6, "size_y": 3},
        }

    return (
        trend(
            "spend",
            "Spend",
            "The spend of the ad accounts, in the reporting currency.",
            AD_STATS,
            "spend_reporting",
            "sum(spend_reporting) AS spend_reporting",
            0,
        ),
        trend(
            "conversions",
            "Conversions",
            "The conversions of the primary conversion actions, as the platforms count them.",
            AD_STATS,
            "conversions",
            "sum(conversions) AS conversions",
            1,
        ),
        trend(
            "cost-per-conversion",
            "Cost per conversion",
            cpa.description,
            AD_STATS,
            cpa.slug,
            ratio(cpa),
            2,
        ),
        trend("roas", "ROAS", roas.description, AD_STATS, roas.slug, ratio(roas), 3),
        trend(
            "clicks",
            "Clicks",
            "The clicks on the ads that led to the site.",
            AD_STATS,
            "clicks",
            "sum(clicks) AS clicks",
            4,
        ),
        trend("ctr", "CTR", ctr.description, AD_STATS, ctr.slug, ratio(ctr), 5),
        trend("cpc", "CPC", cpc.description, AD_STATS, cpc.slug, ratio(cpc), 6),
        trend(
            "key-events",
            "Key events of web analytics",
            "The key events of the sessions of web analytics, which no ad platform counts: none "
            "where the deployment has no source of web analytics.",
            WEB_SESSIONS,
            "key_events",
            "sum(key_events) AS key_events",
            7,
        ),
        {
            "tab": OVERVIEW,
            "key": "trend",
            "name": "Spend and conversions over time",
            "description": (
                "The spend, in the reporting currency, and the conversions of the primary "
                "conversion actions, by the time grouping."
            ),
            "display": "combo",
            "mart": AD_STATS,
            "columns": ("report_date", "spend_reporting", "conversions"),
            "visualization_settings": {
                "graph.dimensions": ["report_date"],
                "graph.metrics": ["spend_reporting", "conversions"],
                "series_settings": {
                    "spend_reporting": {"display": "bar", "color": content.SERIES_COLOR},
                    "conversions": {"display": "line", "axis": "right"},
                },
            },
            "query": over_time(AD_STATS, *totals("spend_reporting", "conversions")),
            "layout": {"row": 6, "col": 0, "size_x": 16, "size_y": 6},
        },
        {"tab": OVERVIEW, "text": ABOUT, "layout": {"row": 6, "col": 16, "size_x": 8, "size_y": 6}},
        {
            "tab": OVERVIEW,
            "key": "platforms",
            "name": "Platforms",
            "description": "The spend, the conversions and their KPIs on each platform.",
            "display": "table",
            "mart": AD_STATS,
            "columns": ("platform_name", "spend_reporting", "conversions", cpa.slug, roas.slug),
            "query": grouped(
                AD_STATS,
                ("platform_name",),
                [*totals("spend_reporting", "conversions"), ratio(cpa), ratio(roas)],
                "spend_reporting DESC",
            ),
            "layout": {"row": 12, "col": 0, "size_x": 12, "size_y": 6},
        },
        {
            "tab": OVERVIEW,
            "key": "clients",
            "name": "Spend by client",
            "description": (
                "The spend of the ad accounts of each client, in the reporting currency."
            ),
            "display": "row",
            "mart": AD_STATS,
            "columns": ("client_name", "spend_reporting"),
            "visualization_settings": {
                "graph.dimensions": ["client_name"],
                "graph.metrics": ["spend_reporting"],
            },
            "query": grouped(
                AD_STATS, ("client_name",), totals("spend_reporting"), "spend_reporting DESC"
            ),
            "layout": {"row": 12, "col": 12, "size_x": 12, "size_y": 6},
        },
        {
            "tab": OVERVIEW,
            "key": "top-campaigns",
            "name": "Top campaigns by spend",
            "description": (
                "The ten campaigns with the most spend, with their conversions and KPIs. A click "
                "on a campaign opens its ad groups and ads."
            ),
            "display": "table",
            "mart": AD_STATS,
            "columns": (
                "campaign_name",
                "platform_name",
                "spend_reporting",
                "conversions",
                cpa.slug,
                roas.slug,
            ),
            "query": grouped(
                AD_STATS,
                ("campaign_name", "platform_name"),
                [*totals("spend_reporting", "conversions"), ratio(cpa), ratio(roas)],
                "spend_reporting DESC",
                limit=10,
            ),
            "click": content.Click("campaign_name", AD_GROUPS_AND_ADS, "campaign"),
            "layout": {"row": 18, "col": 0, "size_x": 16, "size_y": 7},
        },
        {
            "tab": OVERVIEW,
            "key": "loads",
            "name": "Last loads",
            "description": (
                "The last day that a load took for each account of each source, the rates of "
                "currencies among them, in the time zone of the account, and when that load ran."
            ),
            "display": "table",
            "mart": None,
            "columns": ("platform", "account_id", "last_day_loaded", "last_loaded_at"),
            "query": sql(
                "SELECT platform, account_id, last_day_loaded, last_loaded_at",
                "FROM ops.ops_loads",
                "ORDER BY last_loaded_at, platform, account_id",
            ),
            "layout": {"row": 18, "col": 16, "size_x": 8, "size_y": 7},
        },
        {
            "tab": PLATFORMS,
            "key": "by-platform",
            "name": "Results by platform",
            "description": "The statistics and every KPI of each platform.",
            "display": "table",
            "mart": AD_STATS,
            "columns": (
                "platform_name",
                "spend_reporting",
                "impressions",
                "clicks",
                "conversions",
                "conversion_value_reporting",
                *(k.slug for k in (ctr, cpc, cpm, cpa, rate, roas)),
            ),
            "query": grouped(
                AD_STATS,
                ("platform_name",),
                [
                    *results,
                    *totals("conversion_value_reporting"),
                    *(ratio(k) for k in (ctr, cpc, cpm, cpa, rate, roas)),
                ],
                "spend_reporting DESC",
            ),
            "layout": {"row": 0, "col": 0, "size_x": 24, "size_y": 5},
        },
        {
            "tab": PLATFORMS,
            "key": "spend-by-platform",
            "name": "Spend by platform over time",
            "description": (
                "The spend of each platform, in the reporting currency, by the time grouping."
            ),
            "display": "area",
            "mart": AD_STATS,
            "columns": ("report_date", "platform_name", "spend_reporting"),
            "visualization_settings": {
                "graph.dimensions": ["report_date", "platform_name"],
                "graph.metrics": ["spend_reporting"],
                "stackable.stack_type": "stacked",
            },
            "query": over_time(AD_STATS, *totals("spend_reporting"), by="platform_name"),
            "layout": {"row": 5, "col": 0, "size_x": 12, "size_y": 6},
        },
        {
            "tab": PLATFORMS,
            "key": "conversions-by-platform",
            "name": "Conversions by platform over time",
            "description": (
                "The conversions of the primary conversion actions of each platform, by the time "
                "grouping."
            ),
            "display": "line",
            "mart": AD_STATS,
            "columns": ("report_date", "platform_name", "conversions"),
            "visualization_settings": {
                "graph.dimensions": ["report_date", "platform_name"],
                "graph.metrics": ["conversions"],
            },
            "query": over_time(AD_STATS, *totals("conversions"), by="platform_name"),
            "layout": {"row": 5, "col": 12, "size_x": 12, "size_y": 6},
        },
        {
            "tab": CAMPAIGNS,
            "key": "campaigns",
            "name": "Campaigns",
            "description": (
                "Every campaign with its statistics and KPIs, by platform and account. A click on "
                "a campaign opens its ad groups and ads."
            ),
            "display": "table",
            "mart": AD_STATS,
            "columns": ("platform_name", "account_name", "campaign_name", *measures),
            "query": grouped(
                AD_STATS,
                ("platform_name", "account_name", "campaign_name"),
                [*results, *kpi_sql],
                "spend_reporting DESC",
            ),
            "click": content.Click("campaign_name", AD_GROUPS_AND_ADS, "campaign"),
            "layout": {"row": 0, "col": 0, "size_x": 24, "size_y": 12},
        },
        {
            "tab": AD_GROUPS_AND_ADS,
            "text": WITHOUT_PERFORMANCE_MAX,
            "layout": {"row": 0, "col": 0, "size_x": 24, "size_y": 2},
        },
        {
            "tab": AD_GROUPS_AND_ADS,
            "key": "ad-groups",
            "name": "Ad groups",
            "description": "Every ad group with its statistics and KPIs, by platform and campaign.",
            "display": "table",
            "mart": AD_GROUP_STATS,
            "columns": ("platform_name", "campaign_name", "ad_group_name", *measures),
            "query": grouped(
                AD_GROUP_STATS,
                ("platform_name", "campaign_name", "ad_group_name"),
                [*results, *kpi_sql],
                "spend_reporting DESC",
            ),
            "layout": {"row": 2, "col": 0, "size_x": 24, "size_y": 8},
        },
        {
            "tab": AD_GROUPS_AND_ADS,
            "key": "ads",
            "name": "Ads",
            "description": "Every ad with its statistics and KPIs, by campaign and ad group.",
            "display": "table",
            "mart": AD_STATS_OF_ADS,
            "columns": ("campaign_name", "ad_group_name", "ad_name", *measures),
            "query": grouped(
                AD_STATS_OF_ADS,
                ("campaign_name", "ad_group_name", "ad_name"),
                [*results, *kpi_sql],
                "spend_reporting DESC",
            ),
            "layout": {"row": 10, "col": 0, "size_x": 24, "size_y": 8},
        },
        {
            "tab": CONVERSIONS,
            "key": "by-action",
            "name": "Conversions by action",
            "description": (
                "The conversions and the conversion value of each conversion action, primary or "
                "not, with its category and the attribution setting of its platform."
            ),
            "display": "table",
            "mart": AD_CONVERSIONS,
            "columns": (
                "platform_name",
                "conversion_action_name",
                "conversion_category",
                "is_primary",
                "attribution_setting",
                "conversions",
                "conversion_value_reporting",
            ),
            "query": grouped(
                AD_CONVERSIONS,
                (
                    "platform_name",
                    "conversion_action_name",
                    "conversion_category",
                    "is_primary",
                    "attribution_setting",
                ),
                totals("conversions", "conversion_value_reporting"),
                "conversions DESC",
            ),
            "layout": {"row": 0, "col": 0, "size_x": 16, "size_y": 8},
        },
        {
            "tab": CONVERSIONS,
            "text": ATTRIBUTION,
            "layout": {"row": 0, "col": 16, "size_x": 8, "size_y": 8},
        },
        {
            "tab": CONVERSIONS,
            "key": "by-category",
            "name": "Conversions by category over time",
            "description": (
                "The conversions of every conversion action, primary or not, by its category and "
                "the time grouping."
            ),
            "display": "bar",
            "mart": AD_CONVERSIONS,
            "columns": ("report_date", "conversion_category", "conversions"),
            "visualization_settings": {
                "graph.dimensions": ["report_date", "conversion_category"],
                "graph.metrics": ["conversions"],
                "stackable.stack_type": "stacked",
            },
            "query": over_time(AD_CONVERSIONS, *totals("conversions"), by="conversion_category"),
            "layout": {"row": 8, "col": 0, "size_x": 12, "size_y": 6},
        },
        {
            "tab": CONVERSIONS,
            "key": "key-events-over-time",
            "name": "Key events of web analytics over time",
            "description": (
                "The key events of the sessions of web analytics by the time grouping, beside the "
                "conversions of the platforms: none where the deployment has no source of web "
                "analytics."
            ),
            "display": "line",
            "mart": WEB_SESSIONS,
            "columns": ("report_date", "key_events"),
            "visualization_settings": {
                "graph.dimensions": ["report_date"],
                "graph.metrics": ["key_events"],
            },
            "query": over_time(WEB_SESSIONS, *totals("key_events")),
            "layout": {"row": 8, "col": 12, "size_x": 12, "size_y": 6},
        },
    )


def filters_of(question: Mapping[str, Any]) -> tuple[str, ...]:
    """The filters of a question: those of its mart, and the time grouping when it groups by it."""
    mart = question.get("mart")
    names = MART_FILTERS.get(mart, ())
    if "{{unit}}" in question["query"]:
        names = ("period", "unit", *names[1:])
    return names


def column_titles(
    marts: metadata.Metadata, mart: str | None, kpis: Mapping[str, metrics.Declared]
) -> dict[str, str]:
    """The titles of the columns of a question: the display names of the columns of its mart in
    the effective metadata, the KPIs by name, and the columns of the loads."""
    table = marts.effective.get((MART_SCHEMA, mart)) if mart else None
    columns = table.columns if table is not None else ()
    titles = {column.name: column.display_name for column in columns if column.display_name}
    return {**OTHER_TITLES, **titles, **{k.slug: k.kpi for k in kpis.values()}}


def column_format(
    name: str, kpis: Mapping[str, metrics.Declared], currency: str | None
) -> dict[str, Any]:
    """How a column shows its numbers: a KPI as its metric shows it, money in the reporting
    currency, and a count with the decimals of DECIMALS. Metabase formats the column of a native
    question only from the settings of the question, as it is linked to no field."""
    if name in kpis:
        return dict(metrics.settings_of(kpis[name])["column_settings"][content.column_key(name)])
    if name in MONEY and currency:
        return {"number_style": "currency", "currency": currency}
    if name in DECIMALS:
        return {"decimals": DECIMALS[name]}
    return {}


def visualization_settings(
    question: Mapping[str, Any],
    titles: Mapping[str, str],
    kpis: Mapping[str, metrics.Declared],
    currency: str | None,
) -> dict[str, Any]:
    """The settings of a question: the title and the format of each of its columns, and for a chart
    the titles of its axes and the color of its series when it has one."""
    settings = copy.deepcopy(question.get("visualization_settings", {}))
    columns = {}
    for name in question["columns"]:
        shown = column_format(name, kpis, currency)
        if name in titles:
            shown = {"column_title": titles[name], **shown}
        if shown:
            columns[content.column_key(name)] = shown
    if columns:
        settings["column_settings"] = columns
    dimensions = settings.get("graph.dimensions") or [None]
    chart_metrics = settings.get("graph.metrics") or [None]
    for key, name in zip(content.AXIS_TITLES, (dimensions[0], chart_metrics[0]), strict=True):
        if name in titles:
            settings[key] = titles[name]
    if len(dimensions) == 1 and len(chart_metrics) == 1 and chart_metrics[0] is not None:
        settings["series_settings"] = {chart_metrics[0]: {"color": content.SERIES_COLOR}}
    return settings


def answer_columns(question: Mapping[str, Any]) -> tuple[content.Column, ...]:
    """The columns of the answer of a question, with the types that Metabase gives them."""
    return tuple(
        content.Column(c, *COLUMN_TYPES.get(c, NUMBER), SEMANTIC_TYPES.get(c))
        for c in question["columns"]
    )


def question_filters(
    question: Mapping[str, Any], fields: metrics.Fields
) -> tuple[content.Filter, ...]:
    """The filters of a question, each on the field of its column in the mart that the question
    reads (waiting() has checked that Metabase shows it)."""
    filters = []
    for name in filters_of(question):
        display_name, widget_type, column = FILTERS[name]
        field = fields[(question["mart"], column)]
        filters.append(content.Filter(name, display_name, widget_type, *field))
    return tuple(filters)


def waiting(fields: metrics.Fields) -> list[str]:
    """The marts whose fields for the filters Metabase does not show yet, before the first run of
    the transform."""
    return [
        f"{MART_SCHEMA}.{mart}"
        for mart, names in MART_FILTERS.items()
        if any((mart, FILTERS[name][2]) not in fields for name in names)
    ]


def kpis_of(document: Mapping[str, Any]) -> dict[str, metrics.Declared]:
    """The KPIs that the mart of the campaigns declares, by their slugs, each one that the
    dashboard shows."""
    found = {m.slug: m for m in metrics.declared(document) if m.table == KPI_MART}
    absent = [slug for slug in KPIS if slug not in found]
    if absent:
        raise metadata.MetadataError(
            f"{MART_SCHEMA}.{KPI_MART} declares no metric {', '.join(absent)}, which the "
            "dashboard Advertising shows: see the metrics of docs/reference/describe-the-marts.md"
        )
    return found


def reporting_currency(marts: metadata.Metadata) -> str | None:
    """The reporting currency of the marts, as the metadata gives it to the money of the column
    spend_reporting: metadata-export renders it from AGORANOMOS_REPORTING_CURRENCY."""
    table = marts.effective.get((MART_SCHEMA, AD_STATS))
    columns = {c.name: c for c in table.columns} if table is not None else {}
    column = columns.get("spend_reporting")
    return column.currency if column is not None else None


def advertising_package(
    marts: metadata.Metadata,
    kpis: Mapping[str, metrics.Declared],
    fields: metrics.Fields,
) -> content.Package:
    """The dashboard Advertising as a package of the content."""
    currency = reporting_currency(marts)
    shipped, placements = [], []
    for q in questions(kpis):
        if "text" in q:
            placements.append(content.Placement(None, **q["layout"], tab=q["tab"], text=q["text"]))
            continue
        shipped.append(
            content.Question(
                key=q["key"],
                name=q["name"],
                description=q["description"],
                display=q["display"],
                query=q["query"],
                columns=answer_columns(q),
                visualization_settings=visualization_settings(
                    q, column_titles(marts, q["mart"], kpis), kpis, currency
                ),
                filters=question_filters(q, fields),
            )
        )
        placements.append(
            content.Placement(q["key"], **q["layout"], tab=q["tab"], click=q.get("click"))
        )
    return content.Package(
        key=ADVERTISING_KEY,
        name=ADVERTISING_NAME,
        description=ADVERTISING_DESCRIPTION,
        questions=tuple(shipped),
        dashboards=(
            content.Dashboard(
                DASHBOARD,
                DASHBOARD_NAME,
                DASHBOARD_DESCRIPTION,
                tuple(placements),
                DASHBOARD_FILTERS,
                TABS,
            ),
        ),
    )


def ensure(
    metabase: content.Client,
    registry: content.Registry,
    marts: metadata.Metadata,
    export_path: str,
    database_id: int,
    log: content.Log,
) -> dict[str, int]:
    """The dashboard Advertising in Agoranomos / Advertising, as the code says; the IDs of its
    questions and its dashboard by key, none while it waits for the marts."""
    _, fields = metrics.shown_marts(metadata.metadata_of(metabase, database_id))
    absent = waiting(fields)
    if absent:
        log(
            f"waiting: the dashboard Advertising, whose filters need {', '.join(absent)}, which "
            "Metabase does not show yet, comes at a start after the first run of the transform"
        )
        return {}
    kpis = kpis_of(metadata.read_export(export_path))
    return content.ensure_package(
        metabase, registry, advertising_package(marts, kpis, fields), database_id, log
    )
