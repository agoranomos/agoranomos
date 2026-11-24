"""The dashboard Web analytics of agoranomos: one dashboard of the sessions, the engaged sessions,
the key events and the revenue of marts.web_sessions_daily by source, medium, default channel group
and campaign, which metabase-init keeps in the collection agoranomos / Web analytics as the code
says, on every start, with or without the demo, and demo-init after the first run of the transform
(content.py, ADR-0018, ADR-0054). Every deployment has it, so it is empty until a source of web
analytics, GA4 or the demo, loads, and its description says so.

It follows the dashboard Advertising (advertising.py): its questions are native SQL on the mart of
the sessions, each with the filters of the dashboard, the period, the time grouping of a chart over
time, the client and the property, which the client limits as a linked filter on the fields of the
same mart, so that Metabase finds its values with no join. The time grouping changes how a question
groups its days, as a template tag of the type temporal-unit, and filters nothing. It leads with
the sessions, the engaged sessions, the key events and the revenue, each compared with the period
before by the grouping of time, then the trend, the channel groups, the sources and mediums, and
the campaigns that web analytics names. The engagement rate is the ratio of the sums of the engaged
sessions and the sessions, over the rows that the filters select. The mart has no partial day: web
analytics takes no load of the current day.

The questions need the fields of the mart for their filters: until Metabase shows the marts,
before the first run of the transform, the dashboard waits, and a later start makes it.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

import content
import metadata
import metrics

PACKAGE_KEY = "web-analytics"
PACKAGE_NAME = "Web analytics"
PACKAGE_DESCRIPTION = (
    "The web analytics of the business: the dashboard Web analytics, with the sessions, engaged "
    "sessions, key events and revenue of its properties."
)
DASHBOARD = "web-analytics"
DASHBOARD_NAME = "Web analytics"
DASHBOARD_DESCRIPTION = (
    "The sessions, engaged sessions, key events and revenue of the properties of web analytics, "
    "revenue in the reporting currency, by source, medium, default channel group and campaign, "
    "for the period, the client and the property of its filters. It is empty until a source of "
    "web analytics loads."
)

MART_SCHEMA = metrics.MART_SCHEMA
WEB_SESSIONS = "web_sessions_daily"

# The filters of the questions, by the names of their template tags: their display names, the type
# of their widget, and the column of the mart that each is on. A dashboard filter of the same slug
# sets each of them. The time grouping is no filter of rows: it groups the days of report_date.
FILTERS = {
    "period": ("Period", "date/all-options", "report_date"),
    "unit": ("Time grouping", content.TEMPORAL_UNIT, "report_date"),
    "client": ("Client", "string/=", "client_name"),
    "property": ("Property", "string/=", "property_name"),
}
# The filters of the dashboard, as those of the dashboard Advertising: the period starts as the 12
# weeks before this one and the grouping as the week, which starts on Monday
# (metabase_init.WEEK_START), so that each KPI compares the last whole week with the one before. The
# property offers the properties of the client that the filter names.
DASHBOARD_FILTERS = (
    content.DashboardFilter("period", "Period", "date/all-options", "date", "past12weeks"),
    content.DashboardFilter(
        "unit", "Time grouping", content.TEMPORAL_UNIT, "temporal-unit", "week"
    ),
    content.DashboardFilter("client", "Client", "string/=", "string"),
    content.DashboardFilter("property", "Property", "string/=", "string", filtering=("client",)),
)
# The filters of the rows of every question.
ROW_FILTERS = ("period", "client", "property")

# The types that Metabase 0.64.1 gives the columns of the answers, as for the dashboard Advertising
# and the questions of the demo (read on the stand in October 2026): the contract of the mart fixes
# the types of the columns that the questions return as they are, and every sum or ratio is
# numeric in PostgreSQL.
TEXT = ("type/Text", "text")
NUMBER = ("type/Decimal", "numeric")
COLUMN_TYPES = {
    "report_date": ("type/Date", "date"),
    **dict.fromkeys(
        (
            "session_default_channel_group",
            "session_source",
            "session_medium",
            "session_campaign_name",
        ),
        TEXT,
    ),
}
# The semantic types that Metabase 0.64.1 gives the columns of the answers by their names
# (src/metabase/analyze/classifiers/name.clj at the tag v0.64.1; seen on the stand in October 2026):
# a decimal whose name ends in rate is a share, and a name with source or channel is a source.
SEMANTIC_TYPES = {
    "engagement_rate": "type/Share",
    "session_source": "type/Source",
    "session_default_channel_group": "type/Source",
}
# The titles of the columns that are no column of the mart: the ratio.
RATIO_TITLES = {"engagement_rate": "Engagement Rate"}
# Money in the reporting currency, a share as a percentage, and the decimals of a count.
MONEY = frozenset({"total_revenue_reporting"})
PERCENT = frozenset({"engagement_rate"})
DECIMALS = {"key_events": 0}

# The engagement rate, as SQL over the sums of the rows of the answer, null where they have no
# session.
ENGAGEMENT = "sum(engaged_sessions)::numeric / nullif(sum(sessions), 0) AS engagement_rate"
# The measures of the tables, in their order.
MEASURES = (
    "sessions",
    "engaged_sessions",
    "engagement_rate",
    "key_events",
    "total_revenue_reporting",
)

# The text of the card of text, as facts for people and AI clients.
ABOUT = (
    "**About these numbers.** The sessions come from the properties of web analytics, each day in "
    "the time zone of its property, and revenue is in the reporting currency. Web analytics "
    "credits each session to one source, medium and campaign, and dates each key event by its "
    "event, so the key events differ from the conversions of the ad platforms on the dashboard "
    "Advertising. A total of sessions adds up the rows of each day, source, medium and campaign: "
    "GA4 estimates its sessions, so the total that it shows can differ. Each daily load takes the "
    "last 14 days again, so a recent day can still change. The dashboard is empty until a source "
    "of web analytics loads."
)


def sql(*lines: str) -> str:
    return "\n".join(line for line in lines if line)


def where() -> str:
    """The WHERE clause of a question: the template tag of each filter of its rows, which Metabase
    replaces."""
    return "WHERE " + " AND ".join(f"{{{{{name}}}}}" for name in ROW_FILTERS)


def totals(*columns: str) -> list[str]:
    """The sum of each column, under its own name."""
    return [f"sum({column}) AS {column}" for column in columns]


# The SQL of each measure of the tables.
MEASURES_SQL = [*totals("sessions", "engaged_sessions"), ENGAGEMENT]
MEASURES_SQL += totals("key_events", "total_revenue_reporting")


def over_time(*selected: str, by: str = "") -> str:
    """A question over time: its columns by the time grouping, and by a column."""
    group = ", ".join(["{{unit}}", *([by] if by else [])])
    return sql(
        "SELECT " + ", ".join(["{{unit}} AS report_date", *([by] if by else []), *selected]),
        f"FROM {MART_SCHEMA}.{WEB_SESSIONS}",
        where(),
        f"GROUP BY {group}",
        f"ORDER BY {group}",
    )


def grouped(keys: tuple[str, ...], group: tuple[str, ...] = (), limit: int = 0) -> str:
    """A question by some columns, with every measure, the most sessions first: grouped by the
    keys, and by the columns of group too, which it does not show."""
    return sql(
        "SELECT " + ", ".join([*keys, *MEASURES_SQL]),
        f"FROM {MART_SCHEMA}.{WEB_SESSIONS}",
        where(),
        f"GROUP BY {', '.join([*group, *keys])}",
        "ORDER BY sessions DESC",
        f"LIMIT {limit}" if limit else "",
    )


def questions() -> tuple[dict[str, Any], ...]:
    """The questions of the dashboard, in the order of its cards, with their place on its grid of
    24 columns. key is the key of a question in the package, which the registry of the content
    keeps, and columns are the columns of the answer, in their order. A card of text has a text and
    no key. The descriptions say what each counts, as facts for people and AI clients."""

    def trend(key: str, name: str, description: str, column: str, at: int) -> dict[str, Any]:
        return {
            "key": key,
            "name": name,
            "description": description + " Each period of the time grouping, the last compared "
            "with the one before.",
            "display": "smartscalar",
            "columns": ("report_date", column),
            "query": over_time(f"sum({column}) AS {column}"),
            "layout": {"row": 0, "col": 6 * at, "size_x": 6, "size_y": 3},
        }

    return (
        trend("sessions", "Sessions", "The sessions of the properties.", "sessions", 0),
        trend(
            "engaged-sessions",
            "Engaged sessions",
            "The engaged sessions of the properties.",
            "engaged_sessions",
            1,
        ),
        trend("key-events", "Key events", "The key events of the sessions.", "key_events", 2),
        trend(
            "revenue",
            "Revenue",
            "The revenue of the sessions, in the reporting currency.",
            "total_revenue_reporting",
            3,
        ),
        {
            "key": "trend",
            "name": "Sessions and key events over time",
            "description": "The sessions and their key events, by the time grouping.",
            "display": "combo",
            "columns": ("report_date", "sessions", "key_events"),
            "visualization_settings": {
                "graph.dimensions": ["report_date"],
                "graph.metrics": ["sessions", "key_events"],
                "series_settings": {
                    "sessions": {"display": "bar", "color": content.SERIES_COLOR},
                    "key_events": {"display": "line", "axis": "right"},
                },
            },
            "query": over_time(*totals("sessions", "key_events")),
            "layout": {"row": 3, "col": 0, "size_x": 16, "size_y": 6},
        },
        {"text": ABOUT, "layout": {"row": 3, "col": 16, "size_x": 8, "size_y": 6}},
        {
            "key": "channel-groups-over-time",
            "name": "Sessions by channel group over time",
            "description": (
                "The sessions of each default channel group of web analytics, by the time grouping."
            ),
            "display": "area",
            "columns": ("report_date", "session_default_channel_group", "sessions"),
            "visualization_settings": {
                "graph.dimensions": ["report_date", "session_default_channel_group"],
                "graph.metrics": ["sessions"],
                "stackable.stack_type": "stacked",
            },
            "query": over_time(*totals("sessions"), by="session_default_channel_group"),
            "layout": {"row": 9, "col": 0, "size_x": 24, "size_y": 6},
        },
        {
            "key": "by-channel-group",
            "name": "Results by channel group",
            "description": (
                "The sessions, engaged sessions, engagement rate, key events and revenue of each "
                "default channel group; revenue in the reporting currency."
            ),
            "display": "table",
            "columns": ("session_default_channel_group", *MEASURES),
            "query": grouped(("session_default_channel_group",)),
            "layout": {"row": 15, "col": 0, "size_x": 24, "size_y": 5},
        },
        {
            "key": "by-source",
            "name": "Top sources and mediums",
            "description": (
                "The 20 pairs of a source and a medium with the most sessions, with their engaged "
                "sessions, engagement rate, key events and revenue, in the reporting currency."
            ),
            "display": "table",
            "columns": ("session_source", "session_medium", *MEASURES),
            "query": grouped(("session_source", "session_medium"), limit=20),
            "layout": {"row": 20, "col": 0, "size_x": 24, "size_y": 7},
        },
        {
            "key": "campaigns",
            "name": "Campaigns",
            "description": (
                "Every campaign that web analytics names, with its source and medium, its "
                "sessions, engaged sessions, engagement rate, key events and revenue, in the "
                "reporting currency, the most sessions first."
            ),
            "display": "table",
            "columns": ("session_campaign_name", "session_source", "session_medium", *MEASURES),
            "query": grouped(
                ("session_campaign_name", "session_source", "session_medium"),
                group=("session_campaign_id",),
            ),
            "layout": {"row": 27, "col": 0, "size_x": 24, "size_y": 8},
        },
    )


def filters_of(question: Mapping[str, Any]) -> tuple[str, ...]:
    """The filters of a question: those of its rows, and the time grouping when it groups by it."""
    if "{{unit}}" in question["query"]:
        return ("period", "unit", *ROW_FILTERS[1:])
    return ROW_FILTERS


def column_titles(marts: metadata.Metadata) -> dict[str, str]:
    """The titles of the columns of the questions: the display names of the columns of the mart in
    the effective metadata, and the title of the ratio."""
    table = marts.effective.get((MART_SCHEMA, WEB_SESSIONS))
    columns = table.columns if table is not None else ()
    titles = {column.name: column.display_name for column in columns if column.display_name}
    return {**titles, **RATIO_TITLES}


def column_format(name: str, currency: str | None) -> dict[str, Any]:
    """How a column shows its numbers: money in the reporting currency, a share as a percentage,
    and a count with the decimals of DECIMALS. Metabase formats the column of a native question
    only from the settings of the question, as it is linked to no field."""
    if name in MONEY and currency:
        return {"number_style": "currency", "currency": currency}
    if name in PERCENT:
        return {"number_style": "percent", "decimals": 2}
    if name in DECIMALS:
        return {"decimals": DECIMALS[name]}
    return {}


def visualization_settings(
    question: Mapping[str, Any], titles: Mapping[str, str], currency: str | None
) -> dict[str, Any]:
    """The settings of a question: the title and the format of each of its columns, and for a chart
    the titles of its axes and the color of its series when it has one. A chart of several metrics
    names them in its legend and gives its y axis no title, as on the dashboard Advertising."""
    settings = copy.deepcopy(question.get("visualization_settings", {}))
    columns = {}
    for name in question["columns"]:
        shown = column_format(name, currency)
        if name in titles:
            shown = {"column_title": titles[name], **shown}
        if shown:
            columns[content.column_key(name)] = shown
    if columns:
        settings["column_settings"] = columns
    dimensions = settings.get("graph.dimensions") or [None]
    chart_metrics = settings.get("graph.metrics") or [None]
    several = len(chart_metrics) > 1
    named = (dimensions[0], None if several else chart_metrics[0])
    for key, name in zip(content.AXIS_TITLES, named, strict=True):
        if name in titles:
            settings[key] = titles[name]
    if several:
        series = settings.setdefault("series_settings", {})
        for name in chart_metrics:
            if name in titles:
                series.setdefault(name, {})["title"] = titles[name]
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
    """The filters of a question, each on the field of its column in the mart of the sessions
    (waiting() has checked that Metabase shows the field)."""
    filters = []
    for name in filters_of(question):
        display_name, widget_type, column = FILTERS[name]
        filters.append(
            content.Filter(name, display_name, widget_type, *fields[(WEB_SESSIONS, column)])
        )
    return tuple(filters)


def waiting(fields: metrics.Fields) -> list[str]:
    """The mart, when Metabase does not show yet the fields of its filters, before the first run
    of the transform."""
    needed = {column for _, _, column in FILTERS.values()}
    if all((WEB_SESSIONS, column) in fields for column in needed):
        return []
    return [f"{MART_SCHEMA}.{WEB_SESSIONS}"]


def reporting_currency(marts: metadata.Metadata) -> str | None:
    """The reporting currency of the marts, as the metadata gives it to the money of the column
    total_revenue_reporting: metadata-export renders it from AGORANOMOS_REPORTING_CURRENCY."""
    table = marts.effective.get((MART_SCHEMA, WEB_SESSIONS))
    columns = {c.name: c for c in table.columns} if table is not None else {}
    column = columns.get("total_revenue_reporting")
    return column.currency if column is not None else None


def web_analytics_package(marts: metadata.Metadata, fields: metrics.Fields) -> content.Package:
    """The dashboard Web analytics as a package of the content."""
    currency = reporting_currency(marts)
    titles = column_titles(marts)
    shipped, placements = [], []
    for q in questions():
        if "text" in q:
            placements.append(content.Placement(None, **q["layout"], text=q["text"]))
            continue
        shipped.append(
            content.Question(
                key=q["key"],
                name=q["name"],
                description=q["description"],
                display=q["display"],
                query=q["query"],
                columns=answer_columns(q),
                visualization_settings=visualization_settings(q, titles, currency),
                filters=question_filters(q, fields),
            )
        )
        placements.append(content.Placement(q["key"], **q["layout"]))
    return content.Package(
        key=PACKAGE_KEY,
        name=PACKAGE_NAME,
        description=PACKAGE_DESCRIPTION,
        questions=tuple(shipped),
        dashboards=(
            content.Dashboard(
                DASHBOARD,
                DASHBOARD_NAME,
                DASHBOARD_DESCRIPTION,
                tuple(placements),
                DASHBOARD_FILTERS,
            ),
        ),
    )


def ensure(
    metabase: content.Client,
    registry: content.Registry,
    marts: metadata.Metadata,
    database_id: int,
    log: content.Log,
) -> dict[str, int]:
    """The dashboard Web analytics in agoranomos / Web analytics, as the code says; the IDs of its
    questions and its dashboard by key, none while it waits for the mart."""
    _, fields = metrics.shown_marts(metadata.metadata_of(metabase, database_id))
    absent = waiting(fields)
    if absent:
        log(
            f"waiting: the dashboard Web analytics, whose filters need {', '.join(absent)}, which "
            "Metabase does not show yet, comes at a start after the first run of the transform"
        )
        return {}
    return content.ensure_package(
        metabase, registry, web_analytics_package(marts, fields), database_id, log
    )
